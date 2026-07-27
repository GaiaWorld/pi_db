//! 根事务构造、共享句柄、子节点继承和单外根登记的真实公开契约。
//!
//! 本 target 不引用或运行旧测试。它使用真实 4-worker runtime、`Transaction2PcManager`、
//! `CommitLogger`、Memory/LogOrdered 表和真实文件系统，严格验证：
//!
//! - `KVDBManager::transaction` 返回尚未分配 TID/CID、尚未登记 manager 且没有 WAL 副作用的根；
//! - source、可写属性及两个原始 `u64` timeout 被完整保存并传给子节点，source 不参与 UID；
//! - clone 是同一逻辑根的共享句柄，不创建第二个事务或第二份状态；
//! - `KVDBChildTrList` 按首次触表顺序保存每表唯一 owner，旧 iterator 是节点集合快照；
//! - prepare 只登记一个外层根，并把同一 TID/CID 发布给两个直接子节点；
//! - commit 后 manager 来源计数、累计计数、根 WAL、异步确认和最终权威数据严格闭合；
//! - 未 prepare 根及其 clone 的最终释放不改变 manager 或 WAL 计数。
//!
//! 本测试只证明来源标签的保存、继承和串行生命周期计数，不证明上游同 source 并发限流具有
//! 严格线性化；该独立风险记录为 `FIND-TR-SOURCE-001`。只读写、越序生命周期、token 篡改、
//! 崩溃恢复及并发冲突由其它专项负责。

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
    AsyncCommitLog, AsyncTransaction, SequenceTransaction, Transaction2Pc, TransactionTree,
    UnitTransaction,
};
use pi_atom::Atom;
use pi_bon::{Decode, Encode, ReadBuffer, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    utils::CreateTableOptions,
    Binary, KVDBTableType, KVTableMeta, TableTrQos,
};
use pi_guid::{Guid, GuidGen};
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;
type RealTrManager = Transaction2PcManager<usize, CommitLogger>;

const MEMORY_TABLE: &str = "construction_memory";
const LOG_ORDERED_TABLE: &str = "construction_log_ordered";
const UNPREPARED_SOURCE: &str = "root construction unprepared";
const SHARED_SOURCE: &str = "root construction shared";
const TEST_TIMEOUT: Duration = Duration::from_secs(120);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(85);

#[test]
fn test_root_transaction_construction_and_registration_contract() {
    let root = TempRoot::new().expect("creating root construction test directory must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt, &root_path).await?;
        verify_unprepared_root_has_zero_manager_and_wal_side_effects(&fixture)?;
        let ddl_commit_uid = create_tables(&fixture).await?;
        verify_shared_root_tree_registration_and_commit(&rt, &fixture, ddl_commit_uid).await
    })
    .unwrap_or_else(|error| panic!("root transaction construction contract failed: {error}"));
}

async fn create_tables(fixture: &Fixture) -> TestResult<Guid> {
    let ddl = transaction(&fixture.db, "root construction DDL", true, 10_000, 10_000)?;
    ddl
        .create_table(
            Atom::from(MEMORY_TABLE),
            table_meta(KVDBTableType::MemOrdTab),
            false,
        )
        .await
        .map_err(|error| format!("creating construction Memory table failed: {error}"))?;
    ddl
        .create_table_with_options(
            Atom::from(LOG_ORDERED_TABLE),
            table_meta(KVDBTableType::LogOrdTab),
            CreateTableOptions::LogOrdTab(
                64 * 1024 * 1024,
                1024 * 1024,
                1024 * 1024,
            ),
            false,
        )
        .await
        .map_err(|error| format!("creating construction LogOrdered table failed: {error}"))?;

    let append_before = fixture.logger.append_total_count();
    let commit_uid = prepare_and_commit(&ddl, "construction DDL").await?;
    expect_eq(
        "construction DDL root WAL count",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;
    expect_eq("registered table count", &fixture.db.table_size().await, &3usize)?;
    Ok(commit_uid)
}

fn verify_unprepared_root_has_zero_manager_and_wal_side_effects(
    fixture: &Fixture,
) -> TestResult<()> {
    let source = Atom::from(UNPREPARED_SOURCE);
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let confirm_before = fixture.logger.confirm_total_count();

    require(
        fixture.tr_manager.source_len(&source).is_none(),
        "unprepared source unexpectedly existed before root construction",
    )?;
    let transaction = fixture
        .db
        .transaction(source.clone(), false, 0, u64::MAX)
        .ok_or_else(|| "database rejected an unprepared construction root".to_owned())?;
    assert_fresh_root_contract(&transaction, UNPREPARED_SOURCE, false, 0, u64::MAX)?;
    let shared = transaction.clone();
    drop(transaction);
    expect_eq(
        "shared root status after original owner drop",
        &shared.get_status(),
        &Transaction2PcStatus::Start,
    )?;
    require(
        shared.get_transaction_uid().is_none(),
        "dropping one owner unexpectedly started the shared root",
    )?;
    drop(shared);

    expect_eq(
        "unprepared root manager registration count",
        &fixture.tr_manager.transaction_len(),
        &0usize,
    )?;
    expect_eq(
        "unprepared root produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &produced_before,
    )?;
    expect_eq(
        "unprepared root consumed count",
        &fixture.tr_manager.consumed_transaction_total(),
        &consumed_before,
    )?;
    require(
        fixture.tr_manager.source_len(&source).is_none(),
        "unprepared root unexpectedly created a source counter",
    )?;
    expect_eq(
        "unprepared root WAL append count",
        &fixture.logger.append_total_count(),
        &append_before,
    )?;
    expect_eq(
        "unprepared root WAL confirmation count",
        &fixture.logger.confirm_total_count(),
        &confirm_before,
    )
}

async fn verify_shared_root_tree_registration_and_commit(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
    ddl_commit_uid: Guid,
) -> TestResult<()> {
    let source = Atom::from(SHARED_SOURCE);
    let transaction = fixture
        .db
        .transaction(source.clone(), true, 1_234, u64::MAX)
        .ok_or_else(|| "database rejected the shared construction root".to_owned())?;
    assert_fresh_root_contract(&transaction, SHARED_SOURCE, true, 1_234, u64::MAX)?;
    let shared = transaction.clone();

    shared
        .upsert(vec![kv(MEMORY_TABLE, 1, 11)])
        .await
        .map_err(|error| format!("first Memory upsert failed: {error:?}"))?;
    expect_eq(
        "child count after first table",
        &transaction.children_len(),
        &1usize,
    )?;
    let first_snapshot = transaction.to_children();

    transaction
        .upsert(vec![
            kv(LOG_ORDERED_TABLE, 2, 22),
            kv(MEMORY_TABLE, 1, 12),
        ])
        .await
        .map_err(|error| format!("second-table and repeated-table upsert failed: {error:?}"))?;
    expect_eq(
        "child count after second and repeated tables",
        &shared.children_len(),
        &2usize,
    )?;
    expect_eq(
        "Memory private final value through original owner",
        &query_one(&transaction, MEMORY_TABLE, 1).await?,
        &Some(12usize),
    )?;
    expect_eq(
        "LogOrdered private value through clone owner",
        &query_one(&shared, LOG_ORDERED_TABLE, 2).await?,
        &Some(22usize),
    )?;

    let snapshot_children: Vec<RealTransaction> = first_snapshot.collect();
    expect_eq(
        "old child iterator snapshot length",
        &snapshot_children.len(),
        &1usize,
    )?;
    require(
        matches!(snapshot_children.first(), Some(KVDBTransaction::MemOrdTabTr(_))),
        "old child iterator snapshot did not preserve the first Memory owner",
    )?;

    let children: Vec<RealTransaction> = transaction.to_children().collect();
    assert_child_order_and_inheritance(&children)?;
    expect_eq(
        "root persistence after persistent-table writes",
        &transaction.is_require_persistence(),
        &true,
    )?;

    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    require(
        fixture.tr_manager.source_len(&source).is_none(),
        "shared source unexpectedly existed before prepare",
    )?;

    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing shared construction root failed: {error:?}"))?;
    require(
        prepare.len() > 16,
        &format!(
            "persistent two-table prepare output did not contain child payloads: {} bytes",
            prepare.len()
        ),
    )?;
    let transaction_uid = transaction
        .get_transaction_uid()
        .ok_or_else(|| "prepare did not allocate the shared root TID".to_owned())?;
    let commit_uid = transaction
        .get_commit_uid()
        .ok_or_else(|| "prepare did not allocate the shared root CID".to_owned())?;
    expect_eq(
        "clone observes prepared root status",
        &shared.get_status(),
        &Transaction2PcStatus::Prepared,
    )?;
    assert_prepared_child_identity(&children, &transaction_uid, &commit_uid)?;
    expect_eq(
        "manager registers only one outer root",
        &fixture.tr_manager.transaction_len(),
        &1usize,
    )?;
    expect_eq(
        "manager source count after prepare",
        &fixture.tr_manager.source_len(&source),
        &Some(1usize),
    )?;
    expect_eq(
        "manager produced count after prepare",
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        "manager consumed count before commit",
        &fixture.tr_manager.consumed_transaction_total(),
        &consumed_before,
    )?;
    expect_eq(
        "manager lookup observes prepared root",
        &fixture
            .tr_manager
            .get_transaction_status::<RealTransaction>(&transaction_uid),
        &Some(Transaction2PcStatus::Prepared),
    )?;
    expect_eq(
        "prepare active count after await",
        &fixture.tr_manager.prepare_len(),
        &0usize,
    )?;

    shared
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing shared construction root failed: {error:?}"))?;
    expect_eq(
        "original owner observes committed root status",
        &transaction.get_status(),
        &Transaction2PcStatus::Commited,
    )?;
    for (index, child) in children.iter().enumerate() {
        expect_eq(
            &format!("committed child {index} status"),
            &child.get_status(),
            &Transaction2PcStatus::Commited,
        )?;
    }
    expect_eq(
        "manager registry after commit",
        &fixture.tr_manager.transaction_len(),
        &0usize,
    )?;
    expect_eq(
        "manager source count after commit",
        &fixture.tr_manager.source_len(&source),
        &Some(0usize),
    )?;
    expect_eq(
        "manager produced count after commit",
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        "manager consumed count after commit",
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 1),
    )?;
    expect_eq(
        "manager lookup after finish",
        &fixture
            .tr_manager
            .get_transaction_status::<RealTransaction>(&transaction_uid),
        &None,
    )?;
    expect_eq(
        "commit active count after await",
        &fixture.tr_manager.commit_len(),
        &0usize,
    )?;
    expect_eq(
        "shared root WAL append count",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;

    wait_for_all_confirmations(
        rt,
        &fixture.logger,
        &[ddl_commit_uid, commit_uid],
        append_before + 1,
        CONFIRM_TIMEOUT,
        "construction DDL and shared construction root",
    )
    .await?;
    expect_eq(
        "committed Memory value",
        &query_fresh(&fixture.db, MEMORY_TABLE, 1).await?,
        &Some(12usize),
    )?;
    expect_eq(
        "committed LogOrdered value",
        &query_fresh(&fixture.db, LOG_ORDERED_TABLE, 2).await?,
        &Some(22usize),
    )
}

fn assert_fresh_root_contract(
    transaction: &RealTransaction,
    source: &str,
    writable: bool,
    prepare_timeout: u64,
    commit_timeout: u64,
) -> TestResult<()> {
    require(
        matches!(transaction, KVDBTransaction::RootTr(_)),
        "KVDBManager::transaction returned a non-root variant",
    )?;
    expect_eq("fresh root source", &transaction.get_source().as_str(), &source)?;
    expect_eq("fresh root writable flag", &transaction.is_writable(), &writable)?;
    expect_eq(
        "fresh root prepare timeout",
        &transaction.get_prepare_timeout(),
        &prepare_timeout,
    )?;
    expect_eq(
        "fresh root commit timeout",
        &transaction.get_commit_timeout(),
        &commit_timeout,
    )?;
    expect_eq(
        "fresh root status",
        &transaction.get_status(),
        &Transaction2PcStatus::Start,
    )?;
    expect_eq("fresh root QoS", &transaction.qos(), &TableTrQos::Safe)?;
    expect_eq("fresh root is tree", &transaction.is_tree(), &true)?;
    expect_eq("fresh root is unit", &transaction.is_unit(), &false)?;
    expect_eq("fresh root is sequence", &transaction.is_sequence(), &false)?;
    require(
        transaction.prev_item().is_none() && transaction.next_item().is_none(),
        "fresh root unexpectedly exposed sequence neighbors",
    )?;
    expect_eq(
        "fresh root concurrent prepare",
        &transaction.is_concurrent_prepare(),
        &false,
    )?;
    expect_eq(
        "fresh root concurrent commit",
        &transaction.is_concurrent_commit(),
        &false,
    )?;
    expect_eq(
        "fresh root concurrent rollback",
        &transaction.is_concurrent_rollback(),
        &false,
    )?;
    expect_eq(
        "fresh root UID inheritance",
        &transaction.is_enable_inherit_uid(),
        &true,
    )?;
    expect_eq(
        "fresh root persistence",
        &transaction.is_require_persistence(),
        &false,
    )?;
    require(
        transaction.get_transaction_uid().is_none()
            && transaction.get_prepare_uid().is_none()
            && transaction.get_commit_uid().is_none(),
        "fresh root unexpectedly had a TID, PID or CID",
    )?;
    expect_eq("fresh root child count", &transaction.children_len(), &0usize)?;
    require(
        transaction.to_children().next().is_none(),
        "fresh root unexpectedly contained a child transaction",
    )
}

fn assert_child_order_and_inheritance(children: &[RealTransaction]) -> TestResult<()> {
    expect_eq("direct child count", &children.len(), &2usize)?;
    require(
        matches!(children.first(), Some(KVDBTransaction::MemOrdTabTr(_))),
        "first direct child was not the first-touched Memory table",
    )?;
    require(
        matches!(children.get(1), Some(KVDBTransaction::LogOrdTabTr(_))),
        "second direct child was not the second-touched LogOrdered table",
    )?;
    for (index, child) in children.iter().enumerate() {
        expect_eq(
            &format!("child {index} source"),
            &child.get_source().as_str(),
            &SHARED_SOURCE,
        )?;
        expect_eq(
            &format!("child {index} writable flag"),
            &child.is_writable(),
            &true,
        )?;
        expect_eq(
            &format!("child {index} prepare timeout"),
            &child.get_prepare_timeout(),
            &1_234u64,
        )?;
        expect_eq(
            &format!("child {index} commit timeout"),
            &child.get_commit_timeout(),
            &u64::MAX,
        )?;
        expect_eq(
            &format!("child {index} initial status"),
            &child.get_status(),
            &Transaction2PcStatus::Start,
        )?;
        expect_eq(
            &format!("child {index} is unit"),
            &child.is_unit(),
            &true,
        )?;
        expect_eq(
            &format!("child {index} is tree"),
            &child.is_tree(),
            &false,
        )?;
        expect_eq(
            &format!("child {index} persistence"),
            &child.is_require_persistence(),
            &true,
        )?;
        require(
            child.get_transaction_uid().is_none()
                && child.get_prepare_uid().is_none()
                && child.get_commit_uid().is_none(),
            &format!("child {index} unexpectedly had an identity before prepare"),
        )?;
    }
    Ok(())
}

fn assert_prepared_child_identity(
    children: &[RealTransaction],
    transaction_uid: &Guid,
    commit_uid: &Guid,
) -> TestResult<()> {
    for (index, child) in children.iter().enumerate() {
        expect_eq(
            &format!("prepared child {index} TID"),
            &child.get_transaction_uid(),
            &Some(transaction_uid.clone()),
        )?;
        expect_eq(
            &format!("prepared child {index} CID"),
            &child.get_commit_uid(),
            &Some(commit_uid.clone()),
        )?;
        expect_eq(
            &format!("prepared child {index} PID"),
            &child.get_prepare_uid(),
            &None,
        )?;
        expect_eq(
            &format!("prepared child {index} status"),
            &child.get_status(),
            &Transaction2PcStatus::Prepared,
        )?;
    }
    Ok(())
}

async fn prepare_and_commit(
    transaction: &RealTransaction,
    label: &str,
) -> TestResult<Guid> {
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing {label} failed: {error:?}"))?;
    let commit_uid = transaction
        .get_commit_uid()
        .ok_or_else(|| format!("{label} prepare did not allocate a commit UID"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing {label} failed: {error:?}"))?;
    Ok(commit_uid)
}

async fn wait_for_all_confirmations(
    rt: &MultiTaskRuntime<()>,
    logger: &CommitLogger,
    commit_uids: &[Guid],
    expected_total: usize,
    timeout: Duration,
    label: &str,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut pending = Vec::new();
        for commit_uid in commit_uids {
            if let Some(checkpoint) = logger.check_point_of(commit_uid.clone()).await {
                pending.push((commit_uid.clone(), checkpoint));
            }
        }
        let appended = logger.append_total_count();
        let confirmed = logger.confirm_total_count();
        let waiting = logger.waiting_confirm_count().await;
        if pending.is_empty()
            && appended == expected_total
            && confirmed == expected_total
            && waiting == 0 {
            return Ok(());
        }
        if appended > expected_total || confirmed > expected_total {
            return Err(format!(
                "{label} advanced beyond the exact WAL total: expected={expected_total}, appended={appended}, confirmed={confirmed}, waiting={waiting}, pending={pending:?}"
            ));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{label} confirmation exceeded {timeout:?}: expected={expected_total}, appended={appended}, confirmed={confirmed}, waiting={waiting}, pending={pending:?}"
            ));
        }
        rt.timeout(1).await;
    }
}

async fn query_fresh(db: &RealDb, table: &str, key: usize) -> TestResult<Option<usize>> {
    let transaction = transaction(db, "root construction verifier", false, 10_000, 10_000)?;
    query_one(&transaction, table, key).await
}

async fn query_one(
    transaction: &RealTransaction,
    table: &str,
    key: usize,
) -> TestResult<Option<usize>> {
    let mut values = transaction
        .query(vec![TableKV::new(
            Atom::from(table),
            encode_usize(key),
            None,
        )])
        .await;
    if values.len() != 1 {
        return Err(format!(
            "query {table}/{key} returned {} slots instead of one",
            values.len()
        ));
    }
    match values.pop().expect("query length checked above") {
        Some(value) => decode_usize(&value).map(Some),
        None => Ok(None),
    }
}

fn transaction(
    db: &RealDb,
    source: &str,
    writable: bool,
    prepare_timeout: u64,
    commit_timeout: u64,
) -> TestResult<RealTransaction> {
    db
        .transaction(
            Atom::from(source),
            writable,
            prepare_timeout,
            commit_timeout,
        )
        .ok_or_else(|| format!("database rejected transaction {source}"))
}

fn table_meta(table_type: KVDBTableType) -> KVTableMeta {
    KVTableMeta::new(table_type, true, EnumType::Usize, EnumType::Usize)
}

fn kv(table: &str, key: usize, value: usize) -> TableKV {
    TableKV::new(
        Atom::from(table),
        encode_usize(key),
        Some(encode_usize(value)),
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
        .map_err(|error| format!("decoding BON usize failed: {error:?}"))
}

async fn build_database(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<Fixture> {
    fs::create_dir_all(root)
        .map_err(|error| format!("creating construction root {root:?} failed: {error}"))?;
    let wal_path = root.join("root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
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
        .map_err(|error| format!("starting construction database at {db_path:?} failed: {error}"))?;
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

    rt
        .spawn(async move {
            let _ = result_tx.send(future.await);
        })
        .map_err(|error| format!("spawning construction future failed: {error:?}"))?;

    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("construction future exceeded {timeout:?}: {error}"))?
}

fn expect_eq<T: Debug + PartialEq>(
    label: &str,
    actual: &T,
    expected: &T,
) -> TestResult<()> {
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
    fn new() -> TestResult<Self> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system time is before UNIX_EPOCH: {error}"))?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "pi_db_root_transaction_construction_{}_{}",
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
