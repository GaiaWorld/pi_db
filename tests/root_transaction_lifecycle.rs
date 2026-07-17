//! 根事务公开生命周期与两阶段提交当前实现的真实契约矩阵。
//!
//! 本 target 不引用或运行旧测试。它通过公开
//! `KVDBManagerBuilder -> KVDBManager -> KVDBTransaction` 链路，使用真实 4-worker runtime、
//! `Transaction2PcManager`、`CommitLogger`、Meta/Memory 表和真实文件系统，验证：
//!
//! - 根事务创建时尚未注册，prepare/commit 后 manager 的 active/produced/consumed 精确闭环；
//! - 空只读和空可写事务都返回空 prepare 输出，不追加根 WAL；
//! - `prepare_timeout=0/commit_timeout=0` 当前只被保存，不阻止一次正常空事务完成；
//! - 非持久 Memory 写在 commit 前只对本事务可见，commit 后原观察事务仍保持旧快照；
//! - Memory `persistence=true` 合法、仍无数据文件，但 prepare 生成根 WAL payload，commit
//!   先追加/flush WAL，再由 Memory 成功信号异步完成该事务确认；
//! - 只读根事务当前会接受 Memory 写动作，但 prepare 快路跳过子树，commit 后数据不变；
//! - 两个真实事务写同一 Memory Key 时，后 prepare 者返回精确 `Conflicts`，非 Fatal rollback
//!   后不污染已提交根、WAL 或 manager registry。
//!
//! 只读写静默丢弃和 timeout 不执行分别是 `FIND-TR-001`、`FIND-TIMEOUT-001` 的当前事实，
//! 不是最终或最佳 API。测试不篡改 prepare token、不执行越序 commit/panic，也不把这些非法
//! 调用固化为正常契约。最终持久化失败、崩溃恢复和多表确认由已有独立专项负责。
//!
//! 被测入口：`KVDBManager::transaction`、`KVDBTransaction::{upsert,query,
//! prepare_modified,prepare_modified_conflicts,commit_modified,rollback_modified}`。
//! 文档入口：`docs/SEMANTIC_CONTRACTS.md#contract-transaction`。

use std::{
    fmt::Debug,
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
    manager_2pc::{Transaction2PcManager, Transaction2PcStatus},
    AsyncCommitLog, AsyncTransaction, ErrorLevel, Transaction2Pc, UnitTransaction,
};
use pi_atom::Atom;
use pi_bon::{Decode, Encode, ReadBuffer, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    Binary, KVDBTableType, KVTableMeta,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;
type RealTrManager = Transaction2PcManager<usize, CommitLogger>;

const MEMORY_VOLATILE: &str = "lifecycle_memory_volatile";
const MEMORY_WAL: &str = "lifecycle_memory_wal";
const TEST_TIMEOUT: Duration = Duration::from_secs(60);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(10);

/// 在一个真实数据库实例上执行完整根事务生命周期矩阵。
#[test]
fn test_root_transaction_lifecycle_current_contract() {
    let root = TempRoot::new("matrix").expect("creating lifecycle test root must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt, &root_path).await?;
        create_memory_tables(&fixture).await?;
        verify_empty_transactions(&fixture).await?;
        verify_nonpersistent_memory_visibility(&fixture).await?;
        verify_persistent_memory_wal(&rt, &fixture).await?;
        verify_read_only_write_is_discarded(&fixture).await?;
        verify_conflict_and_rollback(&fixture).await?;
        Ok(())
    })
    .unwrap_or_else(|error| panic!("root transaction lifecycle matrix failed: {error}"));
}

/// 创建一个普通 Memory 表和一个只写根 WAL、没有数据文件的 Memory 表。
async fn create_memory_tables(fixture: &Fixture) -> TestResult<()> {
    let transaction = transaction(&fixture.db, "lifecycle DDL", true, 10_000, 10_000)?;
    transaction
        .create_table(Atom::from(MEMORY_VOLATILE), table_meta(false), false)
        .await
        .map_err(|error| format!("creating volatile Memory failed: {error}"))?;
    transaction
        .create_table(Atom::from(MEMORY_WAL), table_meta(true), false)
        .await
        .map_err(|error| format!("creating WAL Memory failed: {error}"))?;

    let append_before = fixture.logger.append_total_count();
    commit_transaction(&transaction, "lifecycle DDL").await?;
    expect_eq(
        "DDL root WAL append count",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;
    expect_eq(
        "transaction registry after DDL commit",
        &fixture.tr_manager.transaction_len(),
        &0,
    )?;
    expect_eq("table count after DDL", &fixture.db.table_size().await, &3)?;
    expect_eq(
        "WAL Memory data path",
        &fixture.db.table_path(&Atom::from(MEMORY_WAL)).await,
        &None,
    )?;
    require(
        !fixture.db.tables_path().join(MEMORY_WAL).exists(),
        "persistence=true Memory unexpectedly created a storage-engine directory",
    )
}

/// 验证空只读/可写事务和零 timeout 的当前状态机与计数。
async fn verify_empty_transactions(fixture: &Fixture) -> TestResult<()> {
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let waiting_before = fixture.logger.waiting_confirm_count().await;

    for (source, writable) in [("empty read only", false), ("empty writable", true)] {
        let transaction = transaction(&fixture.db, source, writable, 0, 0)?;
        expect_eq(
            &format!("{source} initial status"),
            &transaction.get_status(),
            &Transaction2PcStatus::Start,
        )?;
        expect_eq(
            &format!("{source} writable flag"),
            &transaction.is_writable(),
            &writable,
        )?;
        expect_eq(
            &format!("{source} prepare timeout"),
            &transaction.get_prepare_timeout(),
            &0,
        )?;
        expect_eq(
            &format!("{source} commit timeout"),
            &transaction.get_commit_timeout(),
            &0,
        )?;
        expect_eq(
            &format!("{source} registry before prepare"),
            &fixture.tr_manager.transaction_len(),
            &0,
        )?;

        let prepare = transaction
            .prepare_modified()
            .await
            .map_err(|error| format!("preparing {source} failed: {error:?}"))?;
        expect_eq(
            &format!("{source} empty prepare output"),
            &prepare.is_empty(),
            &true,
        )?;
        expect_eq(
            &format!("{source} prepared status"),
            &transaction.get_status(),
            &Transaction2PcStatus::Prepared,
        )?;
        expect_eq(
            &format!("{source} registry after prepare"),
            &fixture.tr_manager.transaction_len(),
            &1,
        )?;

        transaction
            .commit_modified(prepare)
            .await
            .map_err(|error| format!("committing {source} failed: {error:?}"))?;
        expect_eq(
            &format!("{source} committed status"),
            &transaction.get_status(),
            &Transaction2PcStatus::Commited,
        )?;
        expect_eq(
            &format!("{source} registry after commit"),
            &fixture.tr_manager.transaction_len(),
            &0,
        )?;
    }

    expect_eq(
        "empty lifecycle produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 2),
    )?;
    expect_eq(
        "empty lifecycle consumed count",
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 2),
    )?;
    expect_eq(
        "empty lifecycle WAL append count",
        &fixture.logger.append_total_count(),
        &append_before,
    )?;
    expect_eq(
        "empty lifecycle waiting count",
        &fixture.logger.waiting_confirm_count().await,
        &waiting_before,
    )
}

/// 验证非持久 Memory 的事务私有可见性、发布和旧事务快照。
async fn verify_nonpersistent_memory_visibility(fixture: &Fixture) -> TestResult<()> {
    let key = 10usize;
    let writer = transaction(&fixture.db, "volatile writer", true, 10_000, 10_000)?;
    writer
        .upsert(vec![kv(MEMORY_VOLATILE, key, Some(110))])
        .await
        .map_err(|error| format!("volatile writer upsert failed: {error:?}"))?;
    expect_eq(
        "volatile root persistence flag",
        &writer.is_require_persistence(),
        &false,
    )?;
    expect_eq(
        "writer private value",
        &query_one(&writer, MEMORY_VOLATILE, key).await?,
        &Some(110),
    )?;

    let stale_reader = transaction(&fixture.db, "stale reader", false, 10_000, 10_000)?;
    expect_eq(
        "uncommitted value from another transaction",
        &query_one(&stale_reader, MEMORY_VOLATILE, key).await?,
        &None,
    )?;
    expect_eq(
        "manager count before volatile commit",
        &fixture
            .db
            .table_record_size(&Atom::from(MEMORY_VOLATILE))
            .await,
        &Some(0),
    )?;

    let append_before = fixture.logger.append_total_count();
    let prepare = writer
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing volatile writer failed: {error:?}"))?;
    expect_eq("volatile prepare output", &prepare.is_empty(), &true)?;
    writer
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing volatile writer failed: {error:?}"))?;
    expect_eq(
        "volatile commit WAL append count",
        &fixture.logger.append_total_count(),
        &append_before,
    )?;
    expect_eq(
        "manager count after volatile commit",
        &fixture
            .db
            .table_record_size(&Atom::from(MEMORY_VOLATILE))
            .await,
        &Some(1),
    )?;
    expect_eq(
        "stale transaction snapshot after external commit",
        &query_one(&stale_reader, MEMORY_VOLATILE, key).await?,
        &None,
    )?;

    let fresh_reader = transaction(&fixture.db, "fresh reader", false, 10_000, 10_000)?;
    expect_eq(
        "fresh transaction after volatile commit",
        &query_one(&fresh_reader, MEMORY_VOLATILE, key).await?,
        &Some(110),
    )
}

/// 验证 persistence=true Memory 的根 WAL 和目标 commit UID 最终确认。
async fn verify_persistent_memory_wal(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
) -> TestResult<()> {
    let key = 20usize;
    let writer = transaction(&fixture.db, "WAL Memory writer", true, 10_000, 10_000)?;
    writer
        .upsert(vec![kv(MEMORY_WAL, key, Some(220))])
        .await
        .map_err(|error| format!("WAL Memory upsert failed: {error:?}"))?;
    expect_eq(
        "WAL Memory root persistence flag",
        &writer.is_require_persistence(),
        &true,
    )?;

    let append_before = fixture.logger.append_total_count();
    let confirm_before = fixture.logger.confirm_total_count();
    let waiting_before = fixture.logger.waiting_confirm_count().await;
    let prepare = writer
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing WAL Memory writer failed: {error:?}"))?;
    require(
        prepare.len() > 16,
        &format!(
            "WAL Memory prepare output did not contain a child payload: {} bytes",
            prepare.len()
        ),
    )?;
    expect_eq(
        "WAL append before commit",
        &fixture.logger.append_total_count(),
        &append_before,
    )?;
    let commit_uid = writer
        .get_commit_uid()
        .ok_or_else(|| "persistent root did not receive a commit UID during prepare".to_owned())?;

    writer
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing WAL Memory writer failed: {error:?}"))?;
    expect_eq(
        "WAL Memory committed status",
        &writer.get_status(),
        &Transaction2PcStatus::Commited,
    )?;
    expect_eq(
        "WAL Memory append count",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;
    expect_eq(
        "WAL Memory committed value",
        &query_fresh(&fixture.db, MEMORY_WAL, key).await?,
        &Some(220),
    )?;

    wait_for_commit_confirmation(
        rt,
        &fixture.logger,
        commit_uid,
        confirm_before,
        waiting_before,
        CONFIRM_TIMEOUT,
    )
    .await
}

/// 验证只读根当前接受写动作但在 prepare 快路中丢弃；该行为不作为目标设计背书。
async fn verify_read_only_write_is_discarded(fixture: &Fixture) -> TestResult<()> {
    let key = 30usize;
    let transaction = transaction(&fixture.db, "read-only writer", false, 10_000, 10_000)?;
    let append_before = fixture.logger.append_total_count();
    transaction
        .upsert(vec![kv(MEMORY_VOLATILE, key, Some(330))])
        .await
        .map_err(|error| format!("current read-only write path returned an error: {error:?}"))?;
    expect_eq(
        "read-only transaction private action",
        &query_one(&transaction, MEMORY_VOLATILE, key).await?,
        &Some(330),
    )?;

    let prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing read-only writer failed: {error:?}"))?;
    expect_eq("read-only write prepare output", &prepare.is_empty(), &true)?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing read-only writer failed: {error:?}"))?;
    expect_eq(
        "read-only write WAL count",
        &fixture.logger.append_total_count(),
        &append_before,
    )?;
    expect_eq(
        "read-only write committed visibility",
        &query_fresh(&fixture.db, MEMORY_VOLATILE, key).await?,
        &None,
    )
}

/// 构造确定性的 same-key COW 根冲突，并验证非 Fatal rollback 完整闭环。
async fn verify_conflict_and_rollback(fixture: &Fixture) -> TestResult<()> {
    let key = 40usize;
    let first = transaction(&fixture.db, "conflict winner", true, 10_000, 10_000)?;
    let second = transaction(&fixture.db, "conflict loser", true, 10_000, 10_000)?;
    first
        .upsert(vec![kv(MEMORY_VOLATILE, key, Some(440))])
        .await
        .map_err(|error| format!("winner upsert failed: {error:?}"))?;
    second
        .upsert(vec![kv(MEMORY_VOLATILE, key, Some(441))])
        .await
        .map_err(|error| format!("loser upsert failed: {error:?}"))?;

    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    commit_transaction(&first, "conflict winner").await?;
    expect_eq(
        "winner committed value",
        &query_fresh(&fixture.db, MEMORY_VOLATILE, key).await?,
        &Some(440),
    )?;

    let error = second
        .prepare_modified_conflicts()
        .await
        .expect_err("the stale same-key transaction must conflict");
    require(
        matches!(error.level(), ErrorLevel::Normal),
        &format!("conflict returned a non-Normal level: {:?}", error.level()),
    )?;
    require(
        error.is_conflicts(),
        &format!("same-key prepare returned a non-conflict error: {error:?}"),
    )?;
    let (table, conflict_key) = error
        .conflicts()
        .ok_or_else(|| "conflict error did not expose table/key".to_owned())?;
    expect_eq("conflict table", &table.as_str(), &MEMORY_VOLATILE)?;
    expect_eq(
        "conflict key bytes",
        &conflict_key.as_ref(),
        &encode_usize(key).as_ref(),
    )?;
    expect_eq(
        "loser prepare-failed status",
        &second.get_status(),
        &Transaction2PcStatus::PrepareFailed,
    )?;
    expect_eq(
        "registry while conflict awaits rollback",
        &fixture.tr_manager.transaction_len(),
        &1,
    )?;
    expect_eq(
        "conflict path WAL count before rollback",
        &fixture.logger.append_total_count(),
        &append_before,
    )?;

    second
        .rollback_modified()
        .await
        .map_err(|error| format!("rolling back conflict failed: {error:?}"))?;
    expect_eq(
        "loser rollback status",
        &second.get_status(),
        &Transaction2PcStatus::Rollbacked,
    )?;
    expect_eq(
        "registry after conflict rollback",
        &fixture.tr_manager.transaction_len(),
        &0,
    )?;
    expect_eq(
        "conflict lifecycle produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 2),
    )?;
    expect_eq(
        "conflict lifecycle consumed count",
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 2),
    )?;
    expect_eq(
        "conflict rollback preserved winner",
        &query_fresh(&fixture.db, MEMORY_VOLATILE, key).await?,
        &Some(440),
    )?;
    expect_eq(
        "conflict path final WAL count",
        &fixture.logger.append_total_count(),
        &append_before,
    )
}

/// 以 commit UID 为目标条件等待 Memory 成功信号完成根 WAL 确认。
async fn wait_for_commit_confirmation(
    rt: &MultiTaskRuntime<()>,
    logger: &CommitLogger,
    commit_uid: pi_guid::Guid,
    confirm_before: usize,
    waiting_before: usize,
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let checkpoint = logger.check_point_of(commit_uid.clone()).await;
        let confirmed = logger.confirm_total_count();
        let waiting = logger.waiting_confirm_count().await;
        if checkpoint.is_none() && confirmed >= confirm_before + 1 && waiting <= waiting_before {
            return Ok(());
        }
        if confirmed > confirm_before + 2 {
            return Err(format!(
                "unexpected confirmations while waiting for {commit_uid:?}: before={confirm_before}, observed={confirmed}"
            ));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "Memory WAL confirmation exceeded {timeout:?}: commit_uid={commit_uid:?}, checkpoint={checkpoint:?}, confirmed={confirmed}, waiting={waiting}, baseline_waiting={waiting_before}"
            ));
        }
        rt.timeout(1).await;
    }
}

async fn query_fresh(db: &RealDb, table: &str, key: usize) -> TestResult<Option<usize>> {
    let transaction = transaction(db, "fresh query", false, 10_000, 10_000)?;
    query_one(&transaction, table, key).await
}

async fn query_one(
    transaction: &RealTransaction,
    table: &str,
    key: usize,
) -> TestResult<Option<usize>> {
    let mut output = transaction.query(vec![kv(table, key, None)]).await;
    if output.len() != 1 {
        return Err(format!(
            "query {table}/{key} returned {} slots instead of one",
            output.len()
        ));
    }
    match output.pop().expect("length checked above") {
        Some(value) => decode_usize(&value).map(Some),
        None => Ok(None),
    }
}

fn table_meta(persistence: bool) -> KVTableMeta {
    KVTableMeta::new(
        KVDBTableType::MemOrdTab,
        persistence,
        EnumType::Usize,
        EnumType::Usize,
    )
}

fn kv(table: &str, key: usize, value: Option<usize>) -> TableKV {
    TableKV::new(
        Atom::from(table),
        encode_usize(key),
        value.map(encode_usize),
    )
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn decode_usize(value: &Binary) -> TestResult<usize> {
    let mut buffer = ReadBuffer::new(value.as_ref(), 0);
    usize::decode(&mut buffer)
        .map_err(|error| format!("decoding BON usize from query output failed: {error:?}"))
}

fn transaction(
    db: &RealDb,
    source: &str,
    writable: bool,
    prepare_timeout: u64,
    commit_timeout: u64,
) -> TestResult<RealTransaction> {
    db.transaction(
        Atom::from(source),
        writable,
        prepare_timeout,
        commit_timeout,
    )
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
        .map_err(|error| format!("creating lifecycle fixture root {root:?} failed: {error}"))?;
    let wal_path = root.join("root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building CommitLogger at {wal_path:?} failed: {error}"))?;
    let tr_manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger.clone(),
    );
    let db_path = root.join("database");
    let db = KVDBManagerBuilder::new(rt.clone(), tr_manager.clone(), &db_path)
        .startup(false)
        .await
        .map_err(|error| format!("starting lifecycle database at {db_path:?} failed: {error}"))?;
    Ok(Fixture {
        db,
        tr_manager,
        logger,
    })
}

fn run_on_runtime<T, F, Fut>(timeout: Duration, build: F) -> TestResult<T>
where
    T: Send + 'static,
    F: FnOnce(MultiTaskRuntime<()>) -> Fut,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    let _time_loop = startup_global_time_loop(10);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(4)
        .build();
    let future = build(rt.clone());
    let (result_tx, result_rx) = bounded(1);

    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    })
    .map_err(|error| format!("spawning lifecycle future failed: {error:?}"))?;

    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("lifecycle future exceeded {timeout:?}: {error}"))?
}

fn expect_eq<T: Debug + PartialEq>(label: &str, actual: &T, expected: &T) -> TestResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{label}: expected {expected:?}, observed {actual:?}"
        ))
    }
}

fn require(condition: bool, message: &str) -> TestResult<()> {
    if condition {
        Ok(())
    } else {
        Err(message.to_owned())
    }
}

struct Fixture {
    db: RealDb,
    tr_manager: RealTrManager,
    logger: CommitLogger,
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(label: &str) -> TestResult<Self> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system time is before UNIX_EPOCH: {error}"))?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "pi_db_root_lifecycle_{label}_{}_{}",
            std::process::id(),
            nanos
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
