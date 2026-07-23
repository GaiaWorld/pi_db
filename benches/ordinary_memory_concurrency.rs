#![feature(test)]
//! 普通持久化 Memory 表的 writer 扩展与确定性冲突率真实环境基准。
//!
//! 全 target 共享一套 8-worker 数据库 runtime 和一套 8-worker 调用 runtime，避免 runtime worker
//! 存活到进程退出时随 case 累积；每个 case 仍独占真实事务管理器、CommitLogger、根 WAL、数据库
//! 和临时目录。数据库启动及 DDL 位于采样区间外；每轮采样包含根事务创建、query/upsert、跨
//! runtime 并发 prepare、冲突 rollback、成功 commit，以及 manager、WAL 和权威最终值硬断言。
//!
//! 所有事务先完成动作阶段再进入并发 prepare，所以冲突数只由 Disjoint/Paired/Hot Key 分组
//! 决定，不依赖 sleep 或随机调度。结果是带正确性门禁的一轮端到端延迟，不是裸锁、裸 Map 或
//! 裸 WAL 微基准；只能在同一机器、工具链和依赖图下比较。完整口径见
//! `docs/ORDINARY_MEMORY_CONCURRENCY_BENCHMARK.md#ordinary-memory-concurrency-benchmark-index`。

extern crate test;

use std::{
    collections::BTreeSet,
    env,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex, OnceLock,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use async_channel::bounded as async_bounded;
use crossbeam_channel::bounded as sync_bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime, AsyncRuntimeExt,
};
use pi_async_transaction::{
    manager_2pc::{Transaction2PcManager, Transaction2PcStatus},
    AsyncCommitLog, ErrorLevel, UnitTransaction,
};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    Binary, KVDBTableType, KVTableMeta, KVTableTrError,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};
use test::{black_box, Bencher};

type BenchResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type RealManager = Transaction2PcManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const TABLE_NAME: &str = "bench_ordinary_concurrency_memory";
const FIRST_KEY: usize = 0x7100_0000;

macro_rules! concurrency_benchmark {
    ($name:ident, $writers:expr, $profile:expr) => {
        #[doc = concat!(
            "测量 ",
            stringify!($writers),
            " 个普通 Memory writer 在 ",
            stringify!($profile),
            " Key 分组下的一轮完整并发事务延迟。"
        )]
        #[bench]
        fn $name(b: &mut Bencher) {
            let _time_loop = startup_global_time_loop(10);
            let _case_guard = BENCH_CASE_LOCK
                .lock()
                .expect("ordinary concurrency benchmark case lock must not be poisoned");
            let fixture = Fixture::new(stringify!($name));
            b.iter(|| black_box(fixture.run_sample($writers, $profile)));
        }
    };
}

concurrency_benchmark!(bench_1_writer_disjoint, 1, ConflictProfile::Disjoint);
concurrency_benchmark!(bench_2_writers_disjoint, 2, ConflictProfile::Disjoint);
concurrency_benchmark!(bench_2_writers_hot, 2, ConflictProfile::Hot);
concurrency_benchmark!(bench_4_writers_disjoint, 4, ConflictProfile::Disjoint);
concurrency_benchmark!(bench_4_writers_paired, 4, ConflictProfile::Paired);
concurrency_benchmark!(bench_4_writers_hot, 4, ConflictProfile::Hot);
concurrency_benchmark!(bench_8_writers_disjoint, 8, ConflictProfile::Disjoint);
concurrency_benchmark!(bench_8_writers_paired, 8, ConflictProfile::Paired);
concurrency_benchmark!(bench_8_writers_hot, 8, ConflictProfile::Hot);

/// libtest benchmark 通常串行执行；显式 case 锁同时防止非默认 harness 参数引入资源竞争。
static BENCH_CASE_LOCK: Mutex<()> = Mutex::new(());
static DB_RUNTIME: OnceLock<MultiTaskRuntime<()>> = OnceLock::new();
static CALLER_RUNTIME: OnceLock<MultiTaskRuntime<()>> = OnceLock::new();

fn shared_db_runtime() -> MultiTaskRuntime<()> {
    DB_RUNTIME
        .get_or_init(|| {
            MultiTaskRuntimeBuilder::default()
                .init_worker_size(8)
                .build()
        })
        .clone()
}

fn shared_caller_runtime() -> MultiTaskRuntime<()> {
    CALLER_RUNTIME
        .get_or_init(|| {
            MultiTaskRuntimeBuilder::default()
                .init_worker_size(8)
                .build()
        })
        .clone()
}

/// 一个 benchmark case 的独占真实数据库装配。
struct Fixture {
    db: RealDb,
    manager: RealManager,
    logger: CommitLogger,
    db_rt: MultiTaskRuntime<()>,
    caller_rt: MultiTaskRuntime<()>,
    table: Atom,
    next_key: AtomicUsize,
    _root: TempRoot,
}

impl Fixture {
    /// 在采样区间外启动数据库并完成持久化 Memory 表 DDL。
    fn new(label: &str) -> Self {
        let root = TempRoot::new(label);
        let root_path = root.path().to_path_buf();
        let db_rt = shared_db_runtime();
        let caller_rt = shared_caller_runtime();
        let setup_rt = db_rt.clone();
        let table = Atom::from(TABLE_NAME);
        let setup_table = table.clone();
        let (sender, receiver) = sync_bounded(1);

        assert_eq!(usize::BITS, 64, "recorded BM-CONC-001 baseline requires 64-bit usize");
        assert_eq!(encode_usize(FIRST_KEY).as_ref().len(), 5);
        assert_eq!(
            encode_usize(FIRST_KEY.wrapping_mul(17)).as_ref().len(),
            7,
        );

        db_rt
            .block_on(async move {
                let logger = CommitLoggerBuilder::new(setup_rt.clone(), root_path.join("root-wal"))
                    .log_file_limit(512 * 1024 * 1024)
                    .collect_interval(5 * 60 * 1000)
                    .build()
                    .await
                    .expect("ordinary concurrency benchmark CommitLogger must start");
                let manager = Transaction2PcManager::new(
                    setup_rt.clone(),
                    GuidGen::new(0, std::process::id() as u16),
                    logger.clone(),
                );
                let db = KVDBManagerBuilder::new(
                    setup_rt.clone(),
                    manager.clone(),
                    root_path.join("database"),
                )
                .startup(false)
                .await
                .expect("ordinary concurrency benchmark database must start");

                let transaction = db
                    .transaction(
                        Atom::from("ordinary concurrency benchmark DDL"),
                        true,
                        10_000,
                        10_000,
                    )
                    .expect("ordinary concurrency benchmark DDL transaction must start");
                transaction
                    .create_table(
                        setup_table,
                        KVTableMeta::new(
                            KVDBTableType::MemOrdTab,
                            true,
                            EnumType::Usize,
                            EnumType::Usize,
                        ),
                        false,
                    )
                    .await
                    .expect("ordinary concurrency benchmark Memory table must be created");
                let token = transaction
                    .prepare_modified_conflicts()
                    .await
                    .expect("ordinary concurrency benchmark DDL prepare must succeed");
                assert!(!token.is_empty(), "initial DDL must produce root WAL bytes");
                transaction
                    .commit_modified(token)
                    .await
                    .expect("ordinary concurrency benchmark DDL commit must succeed");
                assert_eq!(transaction.get_status(), Transaction2PcStatus::Commited);
                assert_eq!(manager.transaction_len(), 0);
                assert_eq!(logger.append_total_count(), 1);

                sender
                    .send((db, manager, logger))
                    .expect("ordinary concurrency benchmark fixture receiver must remain alive");
            })
            .expect("ordinary concurrency benchmark setup runtime must complete");

        let (db, manager, logger) = receiver
            .recv()
            .expect("ordinary concurrency benchmark fixture must be returned");
        Self {
            db,
            manager,
            logger,
            db_rt,
            caller_rt,
            table,
            next_key: AtomicUsize::new(FIRST_KEY),
            _root: root,
        }
    }

    /// 执行一轮完整事务并在返回前核对 outcome、manager、WAL 和权威值。
    fn run_sample(&self, writers: usize, profile: ConflictProfile) -> usize {
        assert!(matches!(writers, 1 | 2 | 4 | 8));
        let expected_successes = profile.unique_groups(writers);
        let first_key = self
            .next_key
            .fetch_add(expected_successes, Ordering::Relaxed);
        let produced_before = self.manager.produced_transaction_total();
        let consumed_before = self.manager.consumed_transaction_total();
        let append_before = self.logger.append_total_count();
        let db = self.db.clone();
        let caller_rt = self.caller_rt.clone();
        let table = self.table.clone();

        let execution = self
            .db_rt
            .block_on(async move {
                WaveExecution(Some(
                    run_wave(&caller_rt, &db, table, writers, profile, first_key).await,
                ))
            })
            .expect("ordinary concurrency benchmark runtime must execute sample");
        let outcome = execution
            .0
            .expect("ordinary concurrency benchmark future must return an outcome")
            .unwrap_or_else(|error| panic!("ordinary concurrency benchmark sample failed: {error}"));

        assert_eq!(outcome.attempts, writers);
        assert_eq!(outcome.successes, expected_successes);
        assert_eq!(outcome.conflicts, writers - expected_successes);
        assert_eq!(
            self.manager.produced_transaction_total() - produced_before,
            writers,
            "every attempted root must be registered exactly once",
        );
        assert_eq!(
            self.manager.consumed_transaction_total() - consumed_before,
            writers,
            "every committed or rolled-back root must be consumed exactly once",
        );
        assert_eq!(self.manager.transaction_len(), 0, "sample must not leak active roots");
        assert_eq!(
            self.logger.append_total_count() - append_before,
            expected_successes,
            "only successful roots may append WAL",
        );
        outcome.attempts
    }
}

/// 执行动作、并发 prepare、rollback/commit 和最终值核验。
async fn run_wave(
    caller_rt: &MultiTaskRuntime<()>,
    db: &RealDb,
    table: Atom,
    writers: usize,
    profile: ConflictProfile,
    first_key: usize,
) -> BenchResult<WaveOutcome> {
    let actioned = collect_actioned(
        caller_rt,
        db,
        table.clone(),
        writers,
        profile,
        first_key,
    )
    .await?;
    let prepared = collect_prepared(caller_rt, actioned).await?;
    let (winners, conflicts) = classify_prepared(
        prepared,
        table.as_str(),
        writers,
        profile,
        first_key,
    )?;
    let conflict_count = conflicts.len();
    rollback_conflicts(caller_rt, conflicts).await?;
    let success_count = winners.len();
    commit_winners(caller_rt, winners).await?;
    verify_final_values(db, table, profile, writers, first_key).await?;
    Ok(WaveOutcome {
        attempts: writers,
        successes: success_count,
        conflicts: conflict_count,
    })
}

/// 并发构造私有事务前缀；全部任务完成后才允许任何 prepare，形成确定性屏障。
async fn collect_actioned(
    caller_rt: &MultiTaskRuntime<()>,
    db: &RealDb,
    table: Atom,
    writers: usize,
    profile: ConflictProfile,
    first_key: usize,
) -> BenchResult<Vec<ActionedAttempt>> {
    let (sender, receiver) = async_bounded(writers);
    for index in 0..writers {
        let task_db = db.clone();
        let task_table = table.clone();
        let task_sender = sender.clone();
        let group = profile.group(index);
        let key_number = first_key + group;
        caller_rt
            .spawn(async move {
                let result = build_actioned_attempt(
                    &task_db,
                    task_table,
                    index,
                    group,
                    key_number,
                )
                .await;
                let _ = task_sender.send((index, result)).await;
            })
            .map_err(|error| format!("spawning action writer {index} failed: {error:?}"))?;
    }
    drop(sender);

    let mut ordered = (0..writers)
        .map(|_| None)
        .collect::<Vec<Option<ActionedAttempt>>>();
    for _ in 0..writers {
        let (index, result) = receiver
            .recv()
            .await
            .map_err(|error| format!("action result missing: {error}"))?;
        if ordered[index].is_some() {
            return Err(format!("duplicate action result for writer {index}"));
        }
        ordered[index] = Some(result?);
    }
    ordered
        .into_iter()
        .enumerate()
        .map(|(index, attempt)| {
            attempt.ok_or_else(|| format!("action result {index} was not populated"))
        })
        .collect()
}

async fn build_actioned_attempt(
    db: &RealDb,
    table: Atom,
    index: usize,
    group: usize,
    key_number: usize,
) -> BenchResult<ActionedAttempt> {
    let key = encode_usize(key_number);
    let value = encode_usize(key_number.wrapping_mul(17));
    let source = format!("ordinary concurrency benchmark writer {index}");
    let transaction = db
        .transaction(Atom::from(source.as_str()), true, 10_000, 10_000)
        .ok_or_else(|| format!("writer {index} could not create a root transaction"))?;
    let observed = transaction
        .query(vec![TableKV::new(table.clone(), key.clone(), None)])
        .await;
    if observed.len() != 1 || observed[0].is_some() {
        return Err(format!(
            "writer {index} expected a new Key, observed {observed:?}",
        ));
    }
    transaction
        .upsert(vec![TableKV::new(table, key.clone(), Some(value))])
        .await
        .map_err(|error| format!("writer {index} upsert failed: {error:?}"))?;
    Ok(ActionedAttempt {
        group,
        key,
        transaction,
    })
}

/// 在动作屏障后并发 prepare，并保留每个事务的 owned 句柄直到唯一 finish 阶段。
async fn collect_prepared(
    caller_rt: &MultiTaskRuntime<()>,
    actioned: Vec<ActionedAttempt>,
) -> BenchResult<Vec<PreparedAttempt>> {
    let count = actioned.len();
    let (sender, receiver) = async_bounded(count);
    for attempt in actioned {
        let task_sender = sender.clone();
        caller_rt
            .spawn(async move {
                let prepare = attempt.transaction.prepare_modified_conflicts().await;
                let _ = task_sender
                    .send(PreparedAttempt {
                        group: attempt.group,
                        key: attempt.key,
                        transaction: attempt.transaction,
                        prepare,
                    })
                    .await;
            })
            .map_err(|error| format!("spawning prepare task failed: {error:?}"))?;
    }
    drop(sender);

    let mut results = Vec::with_capacity(count);
    for _ in 0..count {
        results.push(
            receiver
                .recv()
                .await
                .map_err(|error| format!("prepare result missing: {error}"))?,
        );
    }
    Ok(results)
}

fn classify_prepared(
    prepared: Vec<PreparedAttempt>,
    expected_table: &str,
    writers: usize,
    profile: ConflictProfile,
    first_key: usize,
) -> BenchResult<(Vec<PreparedWinner>, Vec<RealTransaction>)> {
    let expected_successes = profile.unique_groups(writers);
    let expected_groups = (0..expected_successes).collect::<BTreeSet<_>>();
    let mut winner_groups = BTreeSet::new();
    let mut winners = Vec::with_capacity(expected_successes);
    let mut conflicts = Vec::with_capacity(writers - expected_successes);

    for result in prepared {
        match result.prepare {
            Ok(token) => {
                if token.is_empty() {
                    return Err(format!("group {} returned an empty prepare token", result.group));
                }
                if result.transaction.get_status() != Transaction2PcStatus::Prepared {
                    return Err(format!(
                        "group {} successful root is not Prepared: {:?}",
                        result.group,
                        result.transaction.get_status(),
                    ));
                }
                if !winner_groups.insert(result.group) {
                    return Err(format!(
                        "group {} produced more than one prepare winner",
                        result.group,
                    ));
                }
                winners.push(PreparedWinner {
                    transaction: result.transaction,
                    token,
                });
            },
            Err(error) => {
                validate_conflict(
                    &error,
                    expected_table,
                    &result.key,
                    result.group,
                    first_key,
                )?;
                if result.transaction.get_status() != Transaction2PcStatus::PrepareFailed {
                    return Err(format!(
                        "group {} conflict root is not PrepareFailed: {:?}",
                        result.group,
                        result.transaction.get_status(),
                    ));
                }
                conflicts.push(result.transaction);
            },
        }
    }

    if winner_groups != expected_groups
        || winners.len() != expected_successes
        || conflicts.len() != writers - expected_successes {
        return Err(format!(
            "prepare outcome mismatch: winners={winner_groups:?}/{expected_groups:?}, success={}, conflicts={}, expected={expected_successes}/{}",
            winners.len(),
            conflicts.len(),
            writers - expected_successes,
        ));
    }
    Ok((winners, conflicts))
}

fn validate_conflict(
    error: &KVTableTrError,
    expected_table: &str,
    expected_key: &Binary,
    group: usize,
    first_key: usize,
) -> BenchResult<()> {
    if !matches!(error.level(), ErrorLevel::Normal)
        || !error.is_conflicts()
        || error.is_all_conflicts() {
        return Err(format!(
            "group {group} expected ordinary Conflicts(Normal), observed {error:?}",
        ));
    }
    let (table, key) = error
        .conflicts()
        .ok_or_else(|| format!("group {group} conflict accessor returned None"))?;
    if table.as_str() != expected_table || key.as_ref() != expected_key.as_ref() {
        return Err(format!(
            "group {group} conflict location mismatch: table={table:?}, key={key:?}, expected_key_number={}",
            first_key + group,
        ));
    }
    Ok(())
}

/// 冲突事务先全部 rollback，避免把未经独立验证的 commit/rollback 同时交错引入测量变量。
async fn rollback_conflicts(
    caller_rt: &MultiTaskRuntime<()>,
    conflicts: Vec<RealTransaction>,
) -> BenchResult<()> {
    let count = conflicts.len();
    let (sender, receiver) = async_bounded(count.max(1));
    for transaction in conflicts {
        let task_sender = sender.clone();
        caller_rt
            .spawn(async move {
                let result = transaction.rollback_modified().await;
                let status = transaction.get_status();
                let _ = task_sender.send((status, result)).await;
            })
            .map_err(|error| format!("spawning rollback task failed: {error:?}"))?;
    }
    drop(sender);

    for _ in 0..count {
        let (status, result) = receiver
            .recv()
            .await
            .map_err(|error| format!("rollback result missing: {error}"))?;
        result.map_err(|error| format!("conflict rollback failed: {error:?}"))?;
        if status != Transaction2PcStatus::Rollbacked {
            return Err(format!("rollback root is not Rollbacked: {status:?}"));
        }
    }
    Ok(())
}

async fn commit_winners(
    caller_rt: &MultiTaskRuntime<()>,
    winners: Vec<PreparedWinner>,
) -> BenchResult<()> {
    let count = winners.len();
    let (sender, receiver) = async_bounded(count.max(1));
    for winner in winners {
        let task_sender = sender.clone();
        caller_rt
            .spawn(async move {
                let result = winner.transaction.commit_modified(winner.token).await;
                let status = winner.transaction.get_status();
                let _ = task_sender.send((status, result)).await;
            })
            .map_err(|error| format!("spawning commit task failed: {error:?}"))?;
    }
    drop(sender);

    for _ in 0..count {
        let (status, result) = receiver
            .recv()
            .await
            .map_err(|error| format!("commit result missing: {error}"))?;
        result.map_err(|error| format!("winner commit failed: {error:?}"))?;
        if status != Transaction2PcStatus::Commited {
            return Err(format!("committed root is not Commited: {status:?}"));
        }
    }
    Ok(())
}

/// 新只读根只验证当前 Memory 权威根，不进入 manager 或 WAL。
async fn verify_final_values(
    db: &RealDb,
    table: Atom,
    profile: ConflictProfile,
    writers: usize,
    first_key: usize,
) -> BenchResult<()> {
    let group_count = profile.unique_groups(writers);
    let transaction = db
        .transaction(
            Atom::from("ordinary concurrency benchmark verifier"),
            false,
            10_000,
            10_000,
        )
        .ok_or_else(|| "verifier could not create a read-only root".to_string())?;
    let queries = (0..group_count)
        .map(|group| {
            TableKV::new(
                table.clone(),
                encode_usize(first_key + group),
                None,
            )
        })
        .collect::<Vec<_>>();
    let observed = transaction.query(queries).await;
    if observed.len() != group_count {
        return Err(format!(
            "verifier returned {} values for {group_count} groups",
            observed.len(),
        ));
    }
    for (group, value) in observed.iter().enumerate() {
        let expected = encode_usize((first_key + group).wrapping_mul(17));
        if value.as_ref() != Some(&expected) {
            return Err(format!(
                "group {group} final value mismatch: observed={value:?}, expected={expected:?}",
            ));
        }
    }
    Ok(())
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

#[derive(Clone, Copy, Debug)]
enum ConflictProfile {
    Disjoint,
    Paired,
    Hot,
}

impl ConflictProfile {
    fn group(self, writer: usize) -> usize {
        match self {
            Self::Disjoint => writer,
            Self::Paired => writer / 2,
            Self::Hot => 0,
        }
    }

    fn unique_groups(self, writers: usize) -> usize {
        match self {
            Self::Disjoint => writers,
            Self::Paired => (writers + 1) / 2,
            Self::Hot => 1,
        }
    }
}

struct ActionedAttempt {
    group: usize,
    key: Binary,
    transaction: RealTransaction,
}

struct PreparedAttempt {
    group: usize,
    key: Binary,
    transaction: RealTransaction,
    prepare: Result<Vec<u8>, KVTableTrError>,
}

struct PreparedWinner {
    transaction: RealTransaction,
    token: Vec<u8>,
}

struct WaveOutcome {
    attempts: usize,
    successes: usize,
    conflicts: usize,
}

/// `AsyncRuntimeExt::block_on` 要求输出可默认构造；`None` 只表示 runtime 未运行完目标 future。
#[derive(Default)]
struct WaveExecution(Option<BenchResult<WaveOutcome>>);

/// 临时目录最后析构，保证其前面的数据库、logger 和 runtime owner 已先释放。
struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(label: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("ordinary concurrency benchmark clock must be after UNIX_EPOCH")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "pi_db_ordinary_concurrency_bench_{label}_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path)
            .expect("ordinary concurrency benchmark temporary root must be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
