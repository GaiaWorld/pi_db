//! 普通根事务四个公开 2PC API 的有界分阶段真实环境基准。
//!
//! 普通事务对象依法只能 prepare/commit/rollback 一次，而 nightly libtest `Bencher` 只提供
//! 会把每轮 fixture 一并计时的 `iter`。本 target 因此使用 `harness = false`：每个样本先在
//! 计时区外创建真实事务并登记动作或制造冲突，再只测量目标公开 API 及其 runtime 调度，最后
//! 在计时区外严格核对状态、manager、WAL 和最终值。输出 min/p50/p90/p99/mean/max，仅用于
//! 同机、同工具链和同依赖图的相对比较。
//!
//! 非持久化 Memory 样本隔离事务状态机与表级开销；额外的一项持久化单 Key commit 样本包含
//! 根 WAL append/flush 和 Memory commit，但不包含异步提交确认等待。完整口径见
//! `docs/ROOT_ORDINARY_2PC_CONTRACT.md#root-ordinary-2pc-benchmark`。

use std::{
    fs,
    future::Future,
    hint::black_box,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntimeExt,
};
use pi_async_transaction::{
    manager_2pc::{Transaction2PcManager, Transaction2PcStatus},
    AsyncCommitLog, ErrorLevel, Transaction2Pc, TransactionTree, UnitTransaction,
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

type RealDb = KVDBManager<usize, CommitLogger>;
type RealManager = Transaction2PcManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const VOLATILE_TABLE: &str = "bench_ordinary_2pc_volatile";
const WAL_TABLE: &str = "bench_ordinary_2pc_wal";
const FIRST_KEY: usize = 0x7a00_0000;
const ACTION_COUNTS: [usize; 4] = [0, 1, 16, 256];
const WARMUP_SAMPLES: usize = 2;
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(10);

fn main() {
    let _time_loop = startup_global_time_loop(10);
    let fixture = Fixture::new();

    println!(
        "ordinary_2pc_phases: workers=4, warmup={}, volatile Memory, setup/checks excluded",
        WARMUP_SAMPLES,
    );
    println!(
        "{:<34} {:>7} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "case", "samples", "min(ns)", "p50(ns)", "p90(ns)", "p99(ns)", "mean(ns)", "max(ns)",
    );

    for actions in ACTION_COUNTS {
        run_case(
            &fixture,
            &format!("prepare_generic_{actions}"),
            actions,
            |fixture, count| fixture.measure_prepare(count, PrepareVariant::Generic),
        );
    }
    for actions in ACTION_COUNTS {
        run_case(
            &fixture,
            &format!("prepare_first_conflict_{actions}"),
            actions,
            |fixture, count| fixture.measure_prepare(count, PrepareVariant::FirstConflict),
        );
    }
    for actions in ACTION_COUNTS {
        run_case(
            &fixture,
            &format!("commit_prepared_{actions}"),
            actions,
            |fixture, count| fixture.measure_commit(count),
        );
    }
    for actions in [1usize, 16, 256] {
        run_case(
            &fixture,
            &format!("rollback_prefailed_{actions}"),
            actions,
            |fixture, count| fixture.measure_rollback(count),
        );
    }
    run_case_with_samples(
        &fixture,
        "commit_prepared_wal_1",
        1,
        12,
        |fixture, _| fixture.measure_persistent_commit(),
    );

    assert_eq!(fixture.manager.transaction_len(), 0);
    assert_eq!(
        fixture.manager.produced_transaction_total(),
        fixture.manager.consumed_transaction_total(),
        "all benchmark roots must be consumed",
    );
}

fn run_case<F>(fixture: &Fixture, label: &str, actions: usize, sample: F)
where
    F: FnMut(&Fixture, usize) -> Duration,
{
    run_case_with_samples(fixture, label, actions, sample_count(actions), sample);
}

fn run_case_with_samples<F>(
    fixture: &Fixture,
    label: &str,
    actions: usize,
    samples: usize,
    mut sample: F,
)
where
    F: FnMut(&Fixture, usize) -> Duration,
{
    for _ in 0..WARMUP_SAMPLES {
        black_box(sample(fixture, actions));
    }

    let mut observed = Vec::with_capacity(samples);
    for _ in 0..samples {
        observed.push(sample(fixture, actions));
    }
    let stats = SampleStats::from_samples(&mut observed);
    println!(
        "{:<34} {:>7} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
        label,
        samples,
        stats.min,
        stats.p50,
        stats.p90,
        stats.p99,
        stats.mean,
        stats.max,
    );
}

const fn sample_count(actions: usize) -> usize {
    match actions {
        0 | 1 => 24,
        16 => 16,
        256 => 8,
        _ => panic!("unsupported ordinary 2PC benchmark action count"),
    }
}

#[derive(Clone, Copy)]
enum PrepareVariant {
    Generic,
    FirstConflict,
}

struct Fixture {
    db: RealDb,
    manager: RealManager,
    logger: CommitLogger,
    rt: MultiTaskRuntime<()>,
    next_key: AtomicUsize,
    _root: TempRoot,
}

impl Fixture {
    fn new() -> Self {
        let root = TempRoot::new();
        let root_path = root.path().to_path_buf();
        let rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(4)
            .build();
        let setup_rt = rt.clone();
        let (sender, receiver) = bounded(1);

        rt.block_on(async move {
            let logger = CommitLoggerBuilder::new(
                setup_rt.clone(),
                root_path.join("root-wal"),
            )
            .log_file_limit(64 * 1024 * 1024)
            .collect_interval(5 * 60 * 1000)
            .build()
            .await
            .expect("ordinary 2PC benchmark CommitLogger must start");
            let manager = Transaction2PcManager::new(
                setup_rt.clone(),
                GuidGen::new(0, std::process::id() as u16),
                logger.clone(),
            );
            let db = KVDBManagerBuilder::new(
                setup_rt,
                manager.clone(),
                root_path.join("database"),
            )
            .key_version_ttl(Duration::ZERO)
            .key_version_ttl_poll_interval(Duration::ZERO)
            .startup(false)
            .await
            .expect("ordinary 2PC benchmark database must start");
            create_tables(&db).await;
            assert_eq!(manager.transaction_len(), 0);
            assert_eq!(logger.append_total_count(), 1);
            sender
                .send((db, manager, logger))
                .expect("ordinary 2PC benchmark fixture receiver must remain alive");
        })
        .expect("ordinary 2PC benchmark setup runtime must complete");

        let (db, manager, logger) = receiver
            .recv()
            .expect("ordinary 2PC benchmark fixture must be returned");
        Self {
            db,
            manager,
            logger,
            rt,
            next_key: AtomicUsize::new(FIRST_KEY),
            _root: root,
        }
    }

    fn measure_prepare(&self, actions: usize, variant: PrepareVariant) -> Duration {
        let produced_before = self.manager.produced_transaction_total();
        let consumed_before = self.manager.consumed_transaction_total();
        let append_before = self.logger.append_total_count();
        let (transaction, expected) = self.create_write(actions, VOLATILE_TABLE, 0x1100);
        let phase_transaction = transaction.clone();

        let started = Instant::now();
        let prepared = self
            .run_operation(async move {
                match variant {
                    PrepareVariant::Generic => phase_transaction.prepare_modified().await,
                    PrepareVariant::FirstConflict => {
                        phase_transaction.prepare_modified_conflicts().await
                    },
                }
            });
        let elapsed = started.elapsed();
        let token = prepared.expect("ordinary 2PC prepare benchmark must succeed");

        assert!(token.is_empty(), "volatile Memory prepare must not emit WAL");
        assert_eq!(transaction.get_status(), Transaction2PcStatus::Prepared);
        assert_eq!(transaction.children_len(), usize::from(actions > 0));
        let cleanup_transaction = transaction.clone();
        self.run_operation(async move { cleanup_transaction.commit_modified(token).await })
            .expect("ordinary 2PC prepare cleanup commit must succeed");
        assert_eq!(transaction.get_status(), Transaction2PcStatus::Commited);
        self.assert_closed(
            produced_before,
            consumed_before,
            1,
            append_before,
            0,
        );
        self.assert_values(VOLATILE_TABLE, &expected);
        elapsed
    }

    fn measure_commit(&self, actions: usize) -> Duration {
        let produced_before = self.manager.produced_transaction_total();
        let consumed_before = self.manager.consumed_transaction_total();
        let append_before = self.logger.append_total_count();
        let (transaction, expected) = self.create_write(actions, VOLATILE_TABLE, 0x2200);
        let prepare_transaction = transaction.clone();
        let token = self
            .run_operation(async move { prepare_transaction.prepare_modified_conflicts().await })
            .expect("ordinary 2PC commit setup prepare must succeed");
        assert!(token.is_empty(), "volatile Memory prepare must not emit WAL");
        assert_eq!(transaction.get_status(), Transaction2PcStatus::Prepared);
        let phase_transaction = transaction.clone();

        let started = Instant::now();
        self.run_operation(async move { phase_transaction.commit_modified(token).await })
            .expect("ordinary 2PC commit benchmark must succeed");
        let elapsed = started.elapsed();

        assert_eq!(transaction.get_status(), Transaction2PcStatus::Commited);
        self.assert_closed(
            produced_before,
            consumed_before,
            1,
            append_before,
            0,
        );
        self.assert_values(VOLATILE_TABLE, &expected);
        elapsed
    }

    fn measure_persistent_commit(&self) -> Duration {
        let produced_before = self.manager.produced_transaction_total();
        let consumed_before = self.manager.consumed_transaction_total();
        let append_before = self.logger.append_total_count();
        let confirm_before = self.logger.confirm_total_count();
        let waiting_before = self
            .rt
            .block_on({
                let logger = self.logger.clone();
                async move { logger.waiting_confirm_count().await }
            })
            .expect("ordinary 2PC WAL benchmark waiting count must be readable");
        let (transaction, expected) = self.create_write(1, WAL_TABLE, 0x3300);
        let prepare_transaction = transaction.clone();
        let token = self
            .run_operation(async move { prepare_transaction.prepare_modified_conflicts().await })
            .expect("ordinary 2PC WAL commit setup prepare must succeed");
        assert!(token.len() > 16, "persistent Memory prepare must emit table WAL");
        let commit_uid = transaction
            .get_commit_uid()
            .expect("persistent benchmark root must have a commit UID");
        let phase_transaction = transaction.clone();

        let started = Instant::now();
        self.run_operation(async move { phase_transaction.commit_modified(token).await })
            .expect("ordinary 2PC WAL commit benchmark must succeed");
        let elapsed = started.elapsed();

        assert_eq!(transaction.get_status(), Transaction2PcStatus::Commited);
        self.assert_closed(
            produced_before,
            consumed_before,
            1,
            append_before,
            1,
        );
        self.assert_values(WAL_TABLE, &expected);
        self.wait_for_confirmation(commit_uid, confirm_before, waiting_before);
        elapsed
    }

    fn measure_rollback(&self, actions: usize) -> Duration {
        assert!(actions > 0, "a legal conflict rollback needs at least one action");
        let produced_before = self.manager.produced_transaction_total();
        let consumed_before = self.manager.consumed_transaction_total();
        let append_before = self.logger.append_total_count();
        let (stale, stale_expected) = self.create_write(actions, VOLATILE_TABLE, 0x4400);
        let winner_values: Vec<(usize, Binary)> = stale_expected
            .iter()
            .map(|(key, _)| (*key, encode_usize(key ^ 0x5500)))
            .collect();
        let winner = self.writable_transaction("ordinary 2PC rollback winner");
        let winner_input = winner_values
            .iter()
            .map(|(key, value)| kv(VOLATILE_TABLE, *key, Some(value.clone())))
            .collect();
        let winner_action = winner.clone();
        self.run_operation(async move { winner_action.upsert(winner_input).await })
            .expect("ordinary 2PC rollback winner action must succeed");
        let winner_prepare = winner.clone();
        let winner_token = self
            .run_operation(async move { winner_prepare.prepare_modified_conflicts().await })
            .expect("ordinary 2PC rollback winner prepare must succeed");
        assert!(winner_token.is_empty());
        let winner_commit = winner.clone();
        self.run_operation(async move { winner_commit.commit_modified(winner_token).await })
            .expect("ordinary 2PC rollback winner commit must succeed");

        let stale_prepare = stale.clone();
        let error = self
            .run_operation(async move { stale_prepare.prepare_modified_conflicts().await })
            .expect_err("ordinary 2PC rollback stale prepare must conflict");
        assert!(error.is_conflicts());
        assert!(matches!(error.level(), ErrorLevel::Normal));
        assert_eq!(stale.get_status(), Transaction2PcStatus::PrepareFailed);
        let phase_transaction = stale.clone();

        let started = Instant::now();
        self.run_operation(async move { phase_transaction.rollback_modified().await })
            .expect("ordinary 2PC rollback benchmark must succeed");
        let elapsed = started.elapsed();

        assert_eq!(stale.get_status(), Transaction2PcStatus::Rollbacked);
        self.assert_closed(
            produced_before,
            consumed_before,
            2,
            append_before,
            0,
        );
        self.assert_values(VOLATILE_TABLE, &winner_values);
        elapsed
    }

    fn create_write(
        &self,
        actions: usize,
        table: &str,
        value_mask: usize,
    ) -> (RealTransaction, Vec<(usize, Binary)>) {
        let transaction = self.writable_transaction("ordinary 2PC benchmark sample");
        if actions == 0 {
            return (transaction, Vec::new());
        }

        let first_key = self
            .next_key
            .fetch_add(actions, Ordering::Relaxed);
        let expected: Vec<(usize, Binary)> = (0..actions)
            .map(|offset| {
                let key = first_key + offset;
                (key, encode_usize(key ^ value_mask))
            })
            .collect();
        let input = expected
            .iter()
            .map(|(key, value)| kv(table, *key, Some(value.clone())))
            .collect();
        let action_transaction = transaction.clone();
        self.run_operation(async move { action_transaction.upsert(input).await })
            .expect("ordinary 2PC benchmark action must succeed");
        (transaction, expected)
    }

    fn assert_values(&self, table: &str, expected: &[(usize, Binary)]) {
        if expected.is_empty() {
            return;
        }
        let query = expected
            .iter()
            .map(|(key, _)| kv(table, *key, None))
            .collect();
        let transaction = self
            .db
            .transaction(
                Atom::from("ordinary 2PC benchmark verification"),
                false,
                10_000,
                10_000,
            )
            .expect("ordinary 2PC benchmark verification root must start");
        let query_transaction = transaction.clone();
        let values = self
            .rt
            .block_on(async move { query_transaction.query(query).await })
            .expect("ordinary 2PC benchmark verification runtime must complete");
        assert_eq!(values.len(), expected.len());
        for (index, (actual, (_, expected_value))) in
            values.iter().zip(expected).enumerate()
        {
            assert_eq!(
                actual.as_ref().map(AsRef::<[u8]>::as_ref),
                Some(expected_value.as_ref()),
                "ordinary 2PC benchmark value mismatch at index {index}",
            );
        }
    }

    fn assert_closed(
        &self,
        produced_before: usize,
        consumed_before: usize,
        roots: usize,
        append_before: usize,
        expected_appends: usize,
    ) {
        assert_eq!(self.manager.transaction_len(), 0);
        assert_eq!(
            self.manager.produced_transaction_total() - produced_before,
            roots,
        );
        assert_eq!(
            self.manager.consumed_transaction_total() - consumed_before,
            roots,
        );
        assert_eq!(
            self.logger.append_total_count() - append_before,
            expected_appends,
        );
    }

    fn wait_for_confirmation(
        &self,
        commit_uid: pi_guid::Guid,
        confirm_before: usize,
        waiting_before: usize,
    ) {
        let deadline = Instant::now() + CONFIRM_TIMEOUT;
        let expected_confirmed = confirm_before
            .checked_add(1)
            .expect("ordinary 2PC benchmark confirmation counter must not overflow");
        loop {
            let (checkpoint, waiting) = self
                .rt
                .block_on({
                    let logger = self.logger.clone();
                    let commit_uid = commit_uid.clone();
                    async move {
                        (
                            logger.check_point_of(commit_uid).await,
                            logger.waiting_confirm_count().await,
                        )
                    }
                })
                .expect("ordinary 2PC WAL confirmation state must be readable");
            if checkpoint.is_none()
                && self.logger.confirm_total_count() == expected_confirmed
                && waiting == waiting_before
            {
                return;
            }
            assert!(
                self.logger.confirm_total_count() <= expected_confirmed,
                "ordinary 2PC benchmark observed unrelated confirmation progress",
            );
            assert!(
                Instant::now() < deadline,
                "ordinary 2PC WAL confirmation exceeded {CONFIRM_TIMEOUT:?}: checkpoint={checkpoint:?}, waiting={waiting}, confirm_before={confirm_before}, confirm_now={}",
                self.logger.confirm_total_count(),
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn writable_transaction(&self, source: &str) -> RealTransaction {
        self.db
            .transaction(Atom::from(source), true, 10_000, 10_000)
            .expect("ordinary 2PC benchmark writable root must start")
    }

    fn run_operation<T, F>(&self, future: F) -> Result<T, KVTableTrError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, KVTableTrError>> + Send + 'static,
    {
        self.rt
            .block_on(async move { CapturedOperation(Some(future.await)) })
            .expect("ordinary 2PC benchmark runtime must complete")
            .0
            .expect("ordinary 2PC benchmark runtime must preserve operation output")
    }
}

async fn create_tables(db: &RealDb) {
    let transaction = db
        .transaction(
            Atom::from("ordinary 2PC benchmark DDL"),
            true,
            10_000,
            10_000,
        )
        .expect("ordinary 2PC benchmark DDL root must start");
    transaction
        .create_table(
            Atom::from(VOLATILE_TABLE),
            memory_meta(false),
            false,
        )
        .await
        .expect("ordinary 2PC benchmark volatile table must be created");
    transaction
        .create_table(
            Atom::from(WAL_TABLE),
            memory_meta(true),
            false,
        )
        .await
        .expect("ordinary 2PC benchmark WAL table must be created");
    let token = transaction
        .prepare_modified_conflicts()
        .await
        .expect("ordinary 2PC benchmark DDL prepare must succeed");
    assert!(token.len() > 16, "DDL must emit Meta WAL");
    transaction
        .commit_modified(token)
        .await
        .expect("ordinary 2PC benchmark DDL commit must succeed");
    assert_eq!(transaction.get_status(), Transaction2PcStatus::Commited);
    assert_eq!(db.table_size().await, 3);
}

fn kv(table: &str, key: usize, value: Option<Binary>) -> TableKV {
    TableKV::new(Atom::from(table), encode_usize(key), value)
}

fn memory_meta(persistence: bool) -> KVTableMeta {
    KVTableMeta::new(
        KVDBTableType::MemOrdTab,
        persistence,
        EnumType::Usize,
        EnumType::Usize,
    )
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

struct SampleStats {
    min: u128,
    p50: u128,
    p90: u128,
    p99: u128,
    mean: u128,
    max: u128,
}

impl SampleStats {
    fn from_samples(samples: &mut [Duration]) -> Self {
        assert!(!samples.is_empty());
        samples.sort_unstable();
        let nanos: Vec<u128> = samples.iter().map(Duration::as_nanos).collect();
        let total: u128 = nanos.iter().sum();
        Self {
            min: nanos[0],
            p50: percentile(&nanos, 50),
            p90: percentile(&nanos, 90),
            p99: percentile(&nanos, 99),
            mean: total / nanos.len() as u128,
            max: nanos[nanos.len() - 1],
        }
    }
}

fn percentile(sorted: &[u128], percentile: usize) -> u128 {
    let index = (sorted.len() - 1) * percentile / 100;
    sorted[index]
}

struct CapturedOperation<T>(Option<Result<T, KVTableTrError>>);

impl<T> Default for CapturedOperation<T> {
    fn default() -> Self {
        Self(None)
    }
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must follow UNIX_EPOCH")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "pi_db_ordinary_2pc_bench_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path)
            .expect("ordinary 2PC benchmark temporary root must be created");
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
