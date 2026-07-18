//! `BUG-KV-TTL-001` 的真实生产装配专项红测。
//!
//! 本 target 使用真实 4-worker runtime、`Transaction2PcManager`、根 `CommitLogger`、公开
//! `KVDBManagerBuilder`、真实文件系统和公开 DDL 创建的 Memory 表。测试反复读取一个不存在
//! 的 Key，让 `query_with_version` 生成 `Delete(Guid)` 首次观察版本，并要求后台 TTL loop
//! 确实完成固定轮数的淘汰。
//!
//! TTL 最小单位为 1ms，配置中不足 1ms 的小数按契约忽略。本测试每轮寿命从可能创建该版本
//! 的公开调用之前开始，到观察到下一版本的公开调用之后结束；任何调度、锁等待和查询开销只会
//! 扩大测量值。因此测得值小于量化后的有效 TTL 是严格的提前淘汰证据，不可能由测试线程晚
//! 观察或 runtime 调度抖动制造。

use std::{
    fs,
    future::Future,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::{
    AsyncCommitLog, ErrorLevel, UnitTransaction,
    manager_2pc::{Transaction2PcManager, Transaction2PcStatus},
};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    Binary, KVDBTableType, KVTableMeta, TableKeyVersion, Version,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const TABLE_NAME: &str = "key_version_ttl_memory";
const CONFIGURED_TTL: Duration = Duration::from_micros(20_999);
const EFFECTIVE_TTL: Duration = Duration::from_millis(20);
const POLL_INTERVAL: Duration = Duration::from_millis(1);
const REQUIRED_EXPIRATIONS: usize = 64;
const PER_EXPIRATION_TIMEOUT: Duration = Duration::from_secs(2);
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

#[test]
fn test_key_version_ttl_never_expires_before_effective_duration() {
    let root = TempRoot::new().expect("creating TTL test root must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt, &root_path).await?;
        create_memory_table(&fixture.db).await?;
        require(
            fixture.logger.append_total_count() > 0,
            "real Memory DDL did not append through the root CommitLogger",
        )?;
        verify_repeated_ttl_lifetimes(&rt, &fixture.db).await?;
        verify_expired_read_version_is_a_complete_conflict(&rt, &fixture).await
    })
    .unwrap_or_else(|error| panic!("key-version TTL lower-bound contract failed: {error}"));
}

/// 当外部 read-set 携带的版本已经被 TTL 淘汰时，当前实现没有可比较的值基线；它必须在
/// manager prepare 中保守返回完整冲突，而不能把缺席当成匹配、重新生成版本或继续写 WAL。
async fn verify_expired_read_version_is_a_complete_conflict(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
) -> TestResult<()> {
    let table = Atom::from(TABLE_NAME);
    let key = encode_usize(0x5454_4c02);
    let (value, expired_version) = fixture
        .db
        .query_with_version(table.clone(), key.clone())
        .await
        .map_err(|error| format!("creating read-set version for TTL conflict failed: {error:?}"))?;
    require(value.is_none(), "TTL conflict key unexpectedly had a value")?;
    require_delete_version(&expired_version, "TTL conflict initial observation")?;

    // 前一矩阵已经证明 64 个真实 TTL 周期能够推进。这里等待 25 个有效 TTL，使目标记录在
    // 没有任何读写刷新、没有活跃 snapshot lease 的条件下确定进入淘汰窗口。
    rt.timeout(500).await;

    let produced_before = fixture.manager.produced_transaction_total();
    let consumed_before = fixture.manager.consumed_transaction_total();
    let appended_before = fixture.logger.append_total_count();
    let transaction = transaction(&fixture.db, "expired read-set version")?;
    let error = transaction
        .prepare_with_version(
            vec![TableKeyVersion {
                table: table.clone(),
                key: key.clone(),
                version: expired_version.clone(),
            }],
            Vec::new(),
        )
        .await
        .expect_err("an expired read-set version must not prepare successfully");
    if !error.is_all_conflicts() || !matches!(error.level(), ErrorLevel::Normal) {
        return Err(format!(
            "expired read-set version expected AllConflicts(Normal), observed {error:?}",
        ));
    }
    let conflicts = error
        .all_conflicts()
        .ok_or_else(|| "expired read-set conflict omitted its complete set".to_owned())?;
    require(conflicts.len() == 1, &format!(
        "expired read-set expected one conflict, observed {conflicts:?}",
    ))?;
    require(conflicts[0].table == table,
            "expired read-set conflict reported the wrong table")?;
    require(conflicts[0].key.as_ref() == key.as_ref(),
            "expired read-set conflict reported the wrong key")?;
    require(transaction.get_status() == Transaction2PcStatus::PrepareFailed,
            "expired read-set transaction did not enter PrepareFailed")?;
    require(fixture.logger.append_total_count() == appended_before,
            "expired read-set conflict unexpectedly appended root WAL")?;

    transaction
        .rollback_modified()
        .await
        .map_err(|error| format!("rolling back expired read-set transaction failed: {error:?}"))?;
    require(transaction.get_status() == Transaction2PcStatus::Rollbacked,
            "expired read-set transaction did not close as Rollbacked")?;
    require(fixture.manager.transaction_len() == 0,
            "expired read-set rollback left an active manager entry")?;
    require(fixture.manager.produced_transaction_total() - produced_before == 1,
            "expired read-set manager did not produce exactly one transaction")?;
    require(fixture.manager.consumed_transaction_total() - consumed_before == 1,
            "expired read-set manager did not consume exactly one transaction")?;

    let (replacement_value, replacement_version) = fixture
        .db
        .query_with_version(table, key)
        .await
        .map_err(|error| format!("refreshing expired read-set version failed: {error:?}"))?;
    require(replacement_value.is_none(), "refreshed TTL conflict key unexpectedly had a value")?;
    require_delete_version(&replacement_version, "TTL conflict replacement observation")?;
    require(replacement_version != expired_version,
            "query_with_version reused the version that TTL had removed")
}

async fn verify_repeated_ttl_lifetimes(
    rt: &MultiTaskRuntime<()>,
    db: &RealDb,
) -> TestResult<()> {
    let table = Atom::from(TABLE_NAME);
    let key = encode_usize(0x5454_4c01);
    let mut generation_started = Instant::now();
    let (value, mut current_version) = db
        .query_with_version(table.clone(), key.clone())
        .await
        .map_err(|error| format!("creating initial observed version failed: {error:?}"))?;
    require(value.is_none(), "missing Memory key unexpectedly had a value")?;
    require_delete_version(&current_version, "initial observation")?;

    let mut early_lifetimes = Vec::new();
    let mut observed_expirations = 0usize;
    while observed_expirations < REQUIRED_EXPIRATIONS {
        let wait_started = Instant::now();
        loop {
            if wait_started.elapsed() >= PER_EXPIRATION_TIMEOUT {
                return Err(format!(
                    "TTL loop did not replace version {} within {:?}; observed {} of {} required expirations",
                    observed_expirations + 1,
                    PER_EXPIRATION_TIMEOUT,
                    observed_expirations,
                    REQUIRED_EXPIRATIONS,
                ));
            }

            let query_started = Instant::now();
            let (value, observed_version) = db
                .query_with_version(table.clone(), key.clone())
                .await
                .map_err(|error| {
                    format!(
                        "querying observed version {} failed: {error:?}",
                        observed_expirations + 1,
                    )
                })?;
            let query_finished = Instant::now();
            require(value.is_none(), "missing Memory key unexpectedly acquired a value")?;
            require_delete_version(&observed_version, "replacement observation")?;

            if observed_version != current_version {
                let measured_lifetime = query_finished.duration_since(generation_started);
                observed_expirations += 1;
                if measured_lifetime < EFFECTIVE_TTL {
                    early_lifetimes.push((observed_expirations, measured_lifetime));
                }
                current_version = observed_version;
                // 此次 query 在旧记录已被淘汰时创建新版本；调用开始点是其严格下界。
                generation_started = query_started;
                break;
            }

            rt.timeout(0).await;
        }
    }

    if early_lifetimes.is_empty() {
        Ok(())
    } else {
        let earliest = early_lifetimes
            .iter()
            .map(|(_, lifetime)| *lifetime)
            .min()
            .unwrap();
        Err(format!(
            "observed {} premature expirations in {} completed real TTL cycles: configured={:?}, effective={:?}, earliest={:?}, samples={:?}",
            early_lifetimes.len(),
            observed_expirations,
            CONFIGURED_TTL,
            EFFECTIVE_TTL,
            earliest,
            early_lifetimes,
        ))
    }
}

fn require_delete_version(version: &Version, phase: &str) -> TestResult<()> {
    if matches!(version, Version::Delete(_)) {
        Ok(())
    } else {
        Err(format!(
            "{phase} for a missing Memory key returned a non-delete version: {version:?}"
        ))
    }
}

async fn create_memory_table(db: &RealDb) -> TestResult<()> {
    let transaction = transaction(db, "key-version TTL DDL")?;
    transaction
        .create_table(
            Atom::from(TABLE_NAME),
            KVTableMeta::new(
                KVDBTableType::MemOrdTab,
                false,
                EnumType::Usize,
                EnumType::Usize,
            ),
            false,
        )
        .await
        .map_err(|error| format!("creating Memory table failed: {error}"))?;
    commit_transaction(&transaction, "key-version TTL DDL").await
}

fn transaction(db: &RealDb, source: &str) -> TestResult<RealTransaction> {
    db.transaction(Atom::from(source), true, 10_000, 10_000)
        .ok_or_else(|| format!("database rejected transaction {source}"))
}

async fn commit_transaction(transaction: &RealTransaction, label: &str) -> TestResult<()> {
    let prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing {label} failed: {error:?}"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing {label} failed: {error:?}"))
}

async fn build_database(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<Fixture> {
    fs::create_dir_all(root)
        .map_err(|error| format!("creating fixture root {root:?} failed: {error}"))?;
    let wal_path = root.join("root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building CommitLogger at {wal_path:?} failed: {error}"))?;
    let manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger.clone(),
    );
    let db_path = root.join("database");
    let db = KVDBManagerBuilder::new(rt.clone(), manager.clone(), &db_path)
        .key_version_ttl(CONFIGURED_TTL)
        .key_version_ttl_poll_interval(POLL_INTERVAL)
        .startup(false)
        .await
        .map_err(|error| format!("starting database at {db_path:?} failed: {error}"))?;
    Ok(Fixture { db, logger, manager })
}

fn run_on_runtime<T, F, Fut>(timeout: Duration, build: F) -> TestResult<T>
where
    T: Send + 'static,
    F: FnOnce(MultiTaskRuntime<()>) -> Fut,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    let _time_loop = startup_global_time_loop(1);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(4)
        .build();
    let future = build(rt.clone());
    let (result_tx, result_rx) = bounded(1);
    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    })
    .map_err(|error| format!("spawning TTL test future failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("TTL test future exceeded {timeout:?}: {error}"))?
}

fn require(condition: bool, message: &str) -> TestResult<()> {
    if condition {
        Ok(())
    } else {
        Err(message.to_owned())
    }
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

struct Fixture {
    db: RealDb,
    logger: CommitLogger,
    manager: Transaction2PcManager<usize, CommitLogger>,
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new() -> TestResult<Self> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system time is before UNIX_EPOCH: {error}"))?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "pi_db_key_version_ttl_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path)
            .map_err(|error| format!("creating temporary root {path:?} failed: {error}"))?;
        Ok(Self { path })
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
