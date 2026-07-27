//! `KVDBManagerBuilder` 与 `KVDBManager` 的真实当前实现契约矩阵。
//!
//! 本 target 不引用或运行旧测试。它使用真实 4-worker runtime、
//! `Transaction2PcManager`、`CommitLogger`、Meta/Memory/LogOrdered/LogWrite/Btree 和真实临时
//! 文件系统，验证：
//!
//! - 启动路径、内部 Meta 注册和五类用户表的 registry/路径/属性/空表统计；
//! - Memory `persistence=false/true` 都无数据目录，后者仅表示动作可以进入根 WAL；
//! - 缺表、非法长度名称、已删表以及 Closing/Closed 状态下的管理查询边界；
//! - 删表动作立即移除 registry，但不删除原物理目录，提交仍进入根 WAL 和确认闭环；
//! - 缺表 maintenance 当前的 `Ok(())` 边界；
//! - `append_new_commit_log` 真实轮换 checkpoint，但不增加业务 WAL 计数；
//! - listener 通道实际执行同步批量回调，回调清空事件后不会保留旧批次；
//! - 无 listener 时报告请求返回 `ConnectionAborted`；
//! - clone 共享软关闭状态，新事务立即被拒绝，但 close 前已创建或已注册的普通提交、可恢复
//!   冲突 rollback 和版本提交当前仍可完成，且持久化 Memory 的根 WAL 继续进入确认闭环。
//!
//! 最后一项只描述 `Q-CLOSE-001` / `FIND-CLOSE-001` 的当前实现，不是最终或最佳 shutdown
//! 契约。Btree 非空 overlay 的长度风险由 `FIND-TABLE-002` 和后续表专项负责，本 target 只对
//! 空 Btree 断言 `0`，不把错误折叠为 `0` 认可为正确设计。
//!
//! 被测入口：`pi_db::db::{KVDBManagerBuilder, KVDBManager}`。
//! 文档入口：`docs/TEST_AND_BENCHMARK_STRATEGY.md#test-integration`。

use std::{
    collections::BTreeSet,
    fmt::Debug,
    fs,
    future::Future,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::{
    manager_2pc::Transaction2PcManager, AsyncCommitLog, ErrorLevel, Transaction2Pc,
};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    utils::{CreateTableOptions, KVDBEvent},
    Binary, KVDBTableType, KVTableMeta, TableKeyVersion, Version, MAX_TABLE_NAME_BYTES,
};
use pi_guid::{Guid, GuidGen};
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;
type RealTrManager = Transaction2PcManager<usize, CommitLogger>;

const META_TABLE: &str = ".tables_meta";
const MEMORY_VOLATILE: &str = "manager_memory_volatile";
const MEMORY_WAL: &str = "manager_memory_wal";
const LOG_ORDERED: &str = "manager_log_ordered";
const LOG_WRITE: &str = "manager_log_write";
const BTREE: &str = "manager_btree";
const MISSING: &str = "manager_missing";
const TEST_TIMEOUT: Duration = Duration::from_secs(60);
const OBSERVATION_TIMEOUT: Duration = Duration::from_secs(10);

/// 在真实生产装配上验证管理器当前契约，外层同步看门狗防止 runtime/锁异常永久挂起。
#[test]
fn test_manager_current_contract_matrix() {
    let root = TempRoot::new("matrix").expect("creating manager test root must succeed");
    let root_path = root.path().to_path_buf();
    let (event_tx, event_rx) = unbounded();

    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database_with_listener(&rt, &root_path, event_tx).await?;
        verify_startup_and_initial_registry(&fixture, &root_path).await?;
        verify_listener_delivery(&rt, &fixture, &event_rx).await?;
        create_user_tables(&fixture).await?;
        verify_user_table_registry(&fixture).await?;
        verify_missing_and_maintenance_contract(&fixture).await?;
        verify_removed_table_query_contract(&fixture).await?;
        verify_checkpoint_rotation(&fixture).await?;
        verify_no_listener_error(&rt, &root_path).await?;
        verify_soft_close_contract(&fixture).await?;
        Ok(())
    })
    .unwrap_or_else(|error| panic!("manager current contract matrix failed: {error}"));
}

/// 验证启动创建的三个路径和只含内部 Meta 的初始 registry。
async fn verify_startup_and_initial_registry(fixture: &Fixture, root: &Path) -> TestResult<()> {
    let db_path = root.join("database");
    expect_eq("db_path", &fixture.db.db_path(), &&*db_path)?;
    expect_eq(
        "tables_meta_path",
        &fixture.db.tables_meta_path(),
        &&*db_path.join(META_TABLE),
    )?;
    expect_eq(
        "tables_path",
        &fixture.db.tables_path(),
        &&*db_path.join(".tables"),
    )?;

    require(
        fixture.db.tables_meta_path().is_dir(),
        "startup did not create the Meta directory",
    )?;
    require(
        fixture.db.tables_path().is_dir(),
        "startup did not create the user-table directory",
    )?;
    expect_eq("initial table_size", &fixture.db.table_size().await, &1)?;
    expect_table_names(&fixture.db, &[META_TABLE]).await?;

    let meta = Atom::from(META_TABLE);
    expect_eq("Meta existence", &fixture.db.is_exist(&meta).await, &true)?;
    expect_eq(
        "Meta path",
        &fixture.db.table_path(&meta).await,
        &Some(fixture.db.tables_meta_path().to_path_buf()),
    )?;
    expect_eq(
        "Meta persistence",
        &fixture.db.is_persistent_table(&meta).await,
        &Some(true),
    )?;
    expect_eq(
        "Meta ordering",
        &fixture.db.is_ordered_table(&meta).await,
        &Some(true),
    )?;
    expect_eq(
        "initial Meta record count",
        &fixture.db.table_record_size(&meta).await,
        &Some(0),
    )?;
    expect_eq(
        "initial Meta cache bytes",
        &fixture.db.table_cache_size(&meta).await,
        &Some(0),
    )?;
    expect_eq(
        "initial transaction registry",
        &fixture.tr_manager.transaction_len(),
        &0,
    )?;
    Ok(())
}

/// 发送两个有明确确认点的报告请求，验证事件实际到达且每批由回调 drain。
async fn verify_listener_delivery(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
    event_rx: &Receiver<EventObservation>,
) -> TestResult<()> {
    for sequence in 1..=2 {
        fixture
            .db
            .report_transaction_info()
            .await
            .map_err(|error| format!("report request {sequence} was rejected: {error}"))?;
        let observed = wait_for_report(rt, event_rx, OBSERVATION_TIMEOUT).await?;
        expect_eq(
            &format!("report batch {sequence} total event count"),
            &observed.total,
            &1,
        )?;
        expect_eq(
            &format!("report batch {sequence} report count"),
            &observed.reports,
            &1,
        )?;
        expect_eq(
            &format!("report batch {sequence} commit-failed count"),
            &observed.commit_failed,
            &0,
        )?;
        expect_eq(
            &format!("report batch {sequence} confirmed count"),
            &observed.confirmed,
            &0,
        )?;
        expect_eq(
            &format!("report batch {sequence} transaction count"),
            &observed.transaction_len,
            &0,
        )?;
        expect_eq(
            &format!("report batch {sequence} manager path"),
            &observed.db_path,
            &fixture.db.db_path().to_path_buf(),
        )?;
    }
    Ok(())
}

/// 在一个真实 DDL 根事务中创建两种 Memory 配置和三种持久化表。
async fn create_user_tables(fixture: &Fixture) -> TestResult<()> {
    let transaction = transaction(&fixture.db, "manager DDL", true, 10_000, 10_000)?;
    let tables = [
        (
            MEMORY_VOLATILE,
            table_meta(KVDBTableType::MemOrdTab, false),
            CreateTableOptions::Empty,
        ),
        (
            MEMORY_WAL,
            table_meta(KVDBTableType::MemOrdTab, true),
            CreateTableOptions::Empty,
        ),
        (
            LOG_ORDERED,
            table_meta(KVDBTableType::LogOrdTab, true),
            CreateTableOptions::LogOrdTab(64 * 1024 * 1024, 1024 * 1024, 1024 * 1024),
        ),
        (
            LOG_WRITE,
            table_meta(KVDBTableType::LogWTab, true),
            CreateTableOptions::Empty,
        ),
        (
            BTREE,
            table_meta(KVDBTableType::BtreeOrdTab, true),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
        ),
    ];

    for (name, meta, options) in tables {
        transaction
            .create_table_with_options(Atom::from(name), meta, options, false)
            .await
            .map_err(|error| format!("creating {name} failed: {error}"))?;
    }

    let append_before = fixture.logger.append_total_count();
    let confirmation_before = confirmation_accounting_snapshot(&fixture.logger).await?;
    commit_transaction(&transaction, "manager DDL").await?;
    expect_eq(
        "DDL root WAL append count",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;
    expect_eq(
        "DDL confirmation accounting",
        &confirmation_accounting_snapshot(&fixture.logger).await?,
        &(confirmation_before + 1),
    )
}

/// 验证 hash registry、逐表属性、目录和空表统计的精确矩阵。
async fn verify_user_table_registry(fixture: &Fixture) -> TestResult<()> {
    let expected = [
        META_TABLE,
        MEMORY_VOLATILE,
        MEMORY_WAL,
        LOG_ORDERED,
        LOG_WRITE,
        BTREE,
    ];
    expect_eq("table_size after DDL", &fixture.db.table_size().await, &6)?;
    expect_table_names(&fixture.db, &expected).await?;

    let expected_properties = [
        (
            META_TABLE,
            true,
            true,
            Some(fixture.db.tables_meta_path().to_path_buf()),
        ),
        (MEMORY_VOLATILE, false, true, None),
        (MEMORY_WAL, true, true, None),
        (
            LOG_ORDERED,
            true,
            true,
            Some(fixture.db.tables_path().join(LOG_ORDERED)),
        ),
        (
            LOG_WRITE,
            true,
            true,
            Some(fixture.db.tables_path().join(LOG_WRITE)),
        ),
        (
            BTREE,
            true,
            true,
            Some(fixture.db.tables_path().join(BTREE).join("table.dat")),
        ),
    ];

    for (name, persistent, ordered, path) in expected_properties {
        let atom = Atom::from(name);
        expect_eq(
            &format!("{name} existence"),
            &fixture.db.is_exist(&atom).await,
            &true,
        )?;
        expect_eq(
            &format!("{name} persistence"),
            &fixture.db.is_persistent_table(&atom).await,
            &Some(persistent),
        )?;
        expect_eq(
            &format!("{name} ordering"),
            &fixture.db.is_ordered_table(&atom).await,
            &Some(ordered),
        )?;
        expect_eq(
            &format!("{name} path"),
            &fixture.db.table_path(&atom).await,
            &path,
        )?;
    }

    require(
        !fixture.db.tables_path().join(MEMORY_VOLATILE).exists(),
        "volatile Memory unexpectedly created a data directory",
    )?;
    require(
        !fixture.db.tables_path().join(MEMORY_WAL).exists(),
        "persistence=true Memory unexpectedly created a data directory",
    )?;
    for name in [LOG_ORDERED, LOG_WRITE, BTREE] {
        require(
            fixture.db.tables_path().join(name).is_dir(),
            &format!("persistent table directory for {name} is missing"),
        )?;
    }

    expect_eq(
        "Meta record count after five creates",
        &fixture.db.table_record_size(&Atom::from(META_TABLE)).await,
        &Some(5),
    )?;
    let meta_cache = fixture
        .db
        .table_cache_size(&Atom::from(META_TABLE))
        .await
        .ok_or_else(|| "Meta cache size unexpectedly missing".to_owned())?;
    require(meta_cache > 0, "Meta cache stayed empty after five creates")?;

    for name in [MEMORY_VOLATILE, MEMORY_WAL, LOG_ORDERED, LOG_WRITE, BTREE] {
        let atom = Atom::from(name);
        expect_eq(
            &format!("empty {name} record count"),
            &fixture.db.table_record_size(&atom).await,
            &Some(0),
        )?;
        expect_eq(
            &format!("empty {name} cache bytes"),
            &fixture.db.table_cache_size(&atom).await,
            &Some(0),
        )?;
    }
    Ok(())
}

/// 验证缺表的所有查询结果，以及缺表/Memory maintenance 的当前无操作语义。
async fn verify_missing_and_maintenance_contract(fixture: &Fixture) -> TestResult<()> {
    let missing = Atom::from(MISSING);
    expect_eq(
        "missing existence",
        &fixture.db.is_exist(&missing).await,
        &false,
    )?;
    expect_eq(
        "missing path",
        &fixture.db.table_path(&missing).await,
        &None,
    )?;
    expect_eq(
        "missing persistence",
        &fixture.db.is_persistent_table(&missing).await,
        &None,
    )?;
    expect_eq(
        "missing ordering",
        &fixture.db.is_ordered_table(&missing).await,
        &None,
    )?;
    expect_eq(
        "missing record count",
        &fixture.db.table_record_size(&missing).await,
        &None,
    )?;
    expect_eq(
        "missing cache bytes",
        &fixture.db.table_cache_size(&missing).await,
        &None,
    )?;
    assert_absent_table_queries(fixture, &Atom::from(""), "empty table name", 6).await?;
    assert_absent_table_queries(
        fixture,
        &Atom::from("x".repeat(MAX_TABLE_NAME_BYTES + 1)),
        "over-limit table name",
        6,
    )
    .await?;
    fixture
        .db
        .ready_collect_table(&missing)
        .await
        .map_err(|error| format!("missing ready_collect was not a no-op: {error}"))?;
    fixture
        .db
        .collect_table(&missing)
        .await
        .map_err(|error| format!("missing collect was not a no-op: {error}"))?;

    let memory = Atom::from(MEMORY_VOLATILE);
    fixture
        .db
        .ready_collect_table(&memory)
        .await
        .map_err(|error| format!("Memory ready_collect no-op failed: {error}"))?;
    fixture
        .db
        .collect_table(&memory)
        .await
        .map_err(|error| format!("Memory collect no-op failed: {error}"))?;
    expect_eq(
        "Memory record count after maintenance",
        &fixture.db.table_record_size(&memory).await,
        &Some(0),
    )
}

/// 验证删表的 registry 可见性、查询返回、物理目录非目标及根 WAL 确认闭环。
async fn verify_removed_table_query_contract(fixture: &Fixture) -> TestResult<()> {
    let table = Atom::from(LOG_ORDERED);
    let physical_path = fixture.db.tables_path().join(LOG_ORDERED);
    require(
        physical_path.is_dir(),
        "LogOrdered physical directory was missing before removal",
    )?;

    let transaction = transaction(&fixture.db, "manager remove table", true, 10_000, 10_000)?;
    let append_before = fixture.logger.append_total_count();
    let confirmation_before = confirmation_accounting_snapshot(&fixture.logger).await?;
    transaction
        .remove_table(table.clone())
        .await
        .map_err(|error| format!("staging manager table removal failed: {error}"))?;

    assert_absent_table_queries(fixture, &table, "staged removed table", 5).await?;
    expect_eq(
        "remove action WAL append count",
        &fixture.logger.append_total_count(),
        &append_before,
    )?;
    require(
        physical_path.is_dir(),
        "remove action unexpectedly deleted the LogOrdered physical directory",
    )?;

    commit_transaction(&transaction, "manager remove table").await?;
    assert_absent_table_queries(fixture, &table, "committed removed table", 5).await?;
    expect_eq(
        "remove commit WAL append count",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;
    expect_eq(
        "remove commit confirmation accounting",
        &confirmation_accounting_snapshot(&fixture.logger).await?,
        &(confirmation_before + 1),
    )?;
    require(
        physical_path.is_dir(),
        "committed removal unexpectedly deleted the LogOrdered physical directory",
    )
}

/// 对任意未注册名称验证所有逐表管理查询都使用同一“缺表”结果。
async fn assert_absent_table_queries(
    fixture: &Fixture,
    table: &Atom,
    label: &str,
    expected_table_size: usize,
) -> TestResult<()> {
    expect_eq(
        &format!("{label} existence"),
        &fixture.db.is_exist(table).await,
        &false,
    )?;
    expect_eq(
        &format!("{label} path"),
        &fixture.db.table_path(table).await,
        &None,
    )?;
    expect_eq(
        &format!("{label} persistence"),
        &fixture.db.is_persistent_table(table).await,
        &None,
    )?;
    expect_eq(
        &format!("{label} ordering"),
        &fixture.db.is_ordered_table(table).await,
        &None,
    )?;
    expect_eq(
        &format!("{label} record count"),
        &fixture.db.table_record_size(table).await,
        &None,
    )?;
    expect_eq(
        &format!("{label} cache bytes"),
        &fixture.db.table_cache_size(table).await,
        &None,
    )?;
    expect_eq(
        &format!("{label} table count"),
        &fixture.db.table_size().await,
        &expected_table_size,
    )?;
    require(
        !fixture.db.tables().await.iter().any(|name| name == table),
        &format!("{label} unexpectedly appeared in tables()"),
    )
}

/// 验证 manager checkpoint API 与 logger 可观测量严格一致。
async fn verify_checkpoint_rotation(fixture: &Fixture) -> TestResult<()> {
    let before_index = fixture.logger.current_check_point().await;
    let append_count = fixture.logger.append_total_count();
    let confirm_count = fixture.logger.confirm_total_count();
    let waiting_count = fixture.logger.waiting_confirm_count().await;

    let first = fixture
        .db
        .append_new_commit_log()
        .await
        .map_err(|error| format!("first checkpoint rotation failed: {error}"))?;
    expect_eq(
        "first current checkpoint",
        &fixture.logger.current_check_point().await,
        &(first + 1),
    )?;
    expect_eq("first checkpoint allocation", &first, &before_index)?;

    let second = fixture
        .db
        .append_new_commit_log()
        .await
        .map_err(|error| format!("second checkpoint rotation failed: {error}"))?;
    expect_eq(
        "second current checkpoint",
        &fixture.logger.current_check_point().await,
        &(second + 1),
    )?;
    expect_eq("second checkpoint increment", &second, &(first + 1))?;
    expect_eq(
        "checkpoint rotation business append count",
        &fixture.logger.append_total_count(),
        &append_count,
    )?;
    expect_eq(
        "checkpoint rotation confirm count",
        &fixture.logger.confirm_total_count(),
        &confirm_count,
    )?;
    expect_eq(
        "checkpoint rotation waiting count",
        &fixture.logger.waiting_confirm_count().await,
        &waiting_count,
    )
}

/// 使用第二个真实数据库验证未配置 listener 的错误分类。
async fn verify_no_listener_error(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<()> {
    let no_listener_root = root.join("no-listener");
    let fixture = build_database(rt, &no_listener_root).await?;
    let error = fixture
        .db
        .report_transaction_info()
        .await
        .expect_err("reporting without a listener must fail");
    expect_eq(
        "no-listener report error kind",
        &error.kind(),
        &ErrorKind::ConnectionAborted,
    )?;
    fixture.db.close();
    Ok(())
}

/// 验证 close 的可观察软关闭行为，不把它扩大成完整 shutdown 保证。
async fn verify_soft_close_contract(fixture: &Fixture) -> TestResult<()> {
    let ordinary_key = encode_usize(51_001);
    let ordinary_value = encode_usize(61_001);
    let rejected_value = encode_usize(61_002);
    let version_key = encode_usize(51_002);
    let version_value = encode_usize(61_003);

    let precreated = transaction(&fixture.db, "created before close", true, 0, 0)?;
    let observer = transaction(&fixture.db, "observer created before close", false, 0, 0)?;
    let active = transaction(&fixture.db, "ordinary registered before close", true, 0, 0)?;
    let rollback = transaction(&fixture.db, "rollback registered before close", true, 0, 0)?;
    let version = transaction(&fixture.db, "version registered before close", true, 0, 0)?;
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let confirmation_before = confirmation_accounting_snapshot(&fixture.logger).await?;

    active
        .upsert(vec![TableKV::new(
            Atom::from(MEMORY_VOLATILE),
            ordinary_key.clone(),
            Some(ordinary_value.clone()),
        )])
        .await
        .map_err(|error| format!("staging ordinary close write failed: {error:?}"))?;
    let active_prepare = active
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing active close transaction failed: {error:?}"))?;

    rollback
        .upsert(vec![TableKV::new(
            Atom::from(MEMORY_VOLATILE),
            ordinary_key.clone(),
            Some(rejected_value),
        )])
        .await
        .map_err(|error| format!("staging rollback close write failed: {error:?}"))?;
    let rollback_error = rollback
        .prepare_modified_conflicts()
        .await
        .expect_err("the competing ordinary transaction must fail before close");
    require(
        rollback_error.is_conflicts()
            && !rollback_error.is_all_conflicts()
            && matches!(rollback_error.level(), ErrorLevel::Normal),
        &format!(
            "competing ordinary transaction returned an invalid error: {rollback_error:?}"
        ),
    )?;
    let rollback_conflict = rollback_error
        .conflicts()
        .ok_or_else(|| "ordinary conflict did not expose its table and key".to_owned())?;
    expect_eq(
        "ordinary close conflict table",
        &rollback_conflict.0.as_str(),
        &MEMORY_VOLATILE,
    )?;
    expect_eq(
        "ordinary close conflict key",
        rollback_conflict.1,
        &ordinary_key,
    )?;

    let (initial_version_value, initial_version) = fixture
        .db
        .query_with_version(Atom::from(MEMORY_WAL), version_key.clone())
        .await
        .map_err(|error| format!("loading close version baseline failed: {error:?}"))?;
    expect_eq(
        "close version baseline value",
        &initial_version_value,
        &None,
    )?;
    require(
        matches!(&initial_version, Version::Delete(_)),
        &format!(
            "absent close version baseline was not Delete: {initial_version:?}"
        ),
    )?;
    let version_prepare = version
        .prepare_with_version(
            vec![TableKeyVersion {
                table: Atom::from(MEMORY_WAL),
                key: version_key.clone(),
                version: initial_version,
            }],
            vec![TableKV::new(
                Atom::from(MEMORY_WAL),
                version_key.clone(),
                Some(version_value.clone()),
            )],
        )
        .await
        .map_err(|error| format!("preparing version close transaction failed: {error:?}"))?;
    let version_uid = version
        .get_transaction_uid()
        .ok_or_else(|| "version close prepare did not allocate a transaction UID".to_owned())?;

    expect_eq(
        "active transaction registry before close",
        &fixture.tr_manager.transaction_len(),
        &3,
    )?;

    fixture.db.clone().close();
    verify_management_queries_after_close(fixture, "Closing").await?;
    require(
        fixture
            .db
            .transaction(Atom::from("created after close"), true, 1, 1)
            .is_none(),
        "close did not reject a new transaction",
    )?;
    fixture.db.close();

    active
        .commit_modified(active_prepare)
        .await
        .map_err(|error| format!("active transaction could not finish after close: {error:?}"))?;
    rollback
        .rollback_modified()
        .await
        .map_err(|error| format!("failed transaction could not rollback after close: {error:?}"))?;
    let receipt = version
        .commit_with_version(version_prepare)
        .await
        .map_err(|error| format!("version transaction could not finish after close: {error:?}"))?;
    expect_eq("close version receipt count", &receipt.len(), &1usize)?;
    expect_eq(
        "close version receipt table",
        &receipt[0].table.as_str(),
        &MEMORY_WAL,
    )?;
    expect_eq(
        "close version receipt key",
        &receipt[0].key,
        &version_key,
    )?;
    expect_eq(
        "close version receipt version",
        &receipt[0].version,
        &Version::Upsert(version_uid),
    )?;
    expect_eq(
        "active transaction registry after close completions",
        &fixture.tr_manager.transaction_len(),
        &0,
    )?;

    let precreated_prepare = precreated.prepare_modified().await.map_err(|error| {
        format!("pre-created transaction could not prepare after close: {error:?}")
    })?;
    precreated
        .commit_modified(precreated_prepare)
        .await
        .map_err(|error| {
            format!("pre-created transaction could not commit after close: {error:?}")
        })?;

    let observed = observer
        .query(vec![
            TableKV::new(
                Atom::from(MEMORY_VOLATILE),
                ordinary_key,
                None,
            ),
            TableKV::new(
                Atom::from(MEMORY_WAL),
                version_key,
                None,
            ),
        ])
        .await;
    expect_eq("close observer result count", &observed.len(), &2usize)?;
    expect_eq(
        "ordinary committed value after close",
        &observed[0],
        &Some(ordinary_value),
    )?;
    expect_eq(
        "version committed value after close",
        &observed[1],
        &Some(version_value),
    )?;

    expect_eq(
        "close lifecycle produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 4),
    )?;
    expect_eq(
        "close lifecycle consumed count",
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 4),
    )?;
    expect_eq(
        "close lifecycle final transaction registry",
        &fixture.tr_manager.transaction_len(),
        &0,
    )?;
    expect_eq(
        "close lifecycle WAL append count",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;
    expect_eq(
        "close lifecycle confirmation accounting",
        &confirmation_accounting_snapshot(&fixture.logger).await?,
        &(confirmation_before + 1),
    )?;

    fixture.db.close();
    verify_management_queries_after_close(fixture, "Closed").await?;
    require(
        fixture
            .db
            .transaction(Atom::from("created after repeated close"), false, 0, 0)
            .is_none(),
        "repeated close unexpectedly reopened the database",
    )
}

/// 管理查询不读取软关闭状态；Closing 和 Closed 都继续暴露同一 registry 当前值。
async fn verify_management_queries_after_close(
    fixture: &Fixture,
    phase: &str,
) -> TestResult<()> {
    expect_eq(
        &format!("{phase} Meta path derivation"),
        &fixture.db.tables_meta_path(),
        &&*fixture.db.db_path().join(META_TABLE),
    )?;
    expect_eq(
        &format!("{phase} user-table path derivation"),
        &fixture.db.tables_path(),
        &&*fixture.db.db_path().join(".tables"),
    )?;
    expect_eq(
        &format!("{phase} table count"),
        &fixture.db.table_size().await,
        &5usize,
    )?;
    expect_table_names(
        &fixture.db,
        &[META_TABLE, MEMORY_VOLATILE, MEMORY_WAL, LOG_WRITE, BTREE],
    )
    .await?;

    expect_eq(
        &format!("{phase} existing table"),
        &fixture.db.is_exist(&Atom::from(MEMORY_WAL)).await,
        &true,
    )?;
    expect_eq(
        &format!("{phase} Btree path"),
        &fixture.db.table_path(&Atom::from(BTREE)).await,
        &Some(fixture.db.tables_path().join(BTREE).join("table.dat")),
    )?;
    expect_eq(
        &format!("{phase} volatile Memory persistence"),
        &fixture
            .db
            .is_persistent_table(&Atom::from(MEMORY_VOLATILE))
            .await,
        &Some(false),
    )?;
    expect_eq(
        &format!("{phase} Btree ordering"),
        &fixture.db.is_ordered_table(&Atom::from(BTREE)).await,
        &Some(true),
    )?;
    expect_eq(
        &format!("{phase} Meta record count"),
        &fixture.db.table_record_size(&Atom::from(META_TABLE)).await,
        &Some(4),
    )?;
    require(
        fixture
            .db
            .table_cache_size(&Atom::from(META_TABLE))
            .await
            .is_some_and(|size| size > 0),
        &format!("{phase} Meta cache bytes were absent or zero"),
    )?;
    assert_absent_table_queries(fixture, &Atom::from(LOG_ORDERED), phase, 5).await
}

/// 等待 listener 回调实际交付一个含报告请求的批次。
async fn wait_for_report(
    rt: &MultiTaskRuntime<()>,
    event_rx: &Receiver<EventObservation>,
    timeout: Duration,
) -> TestResult<EventObservation> {
    let deadline = Instant::now() + timeout;
    loop {
        match event_rx.try_recv() {
            Ok(observation) if observation.reports > 0 => return Ok(observation),
            Ok(observation) => {
                return Err(format!(
                    "listener delivered a batch without the requested report: {observation:?}"
                ));
            }
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                return Err("listener observation channel disconnected".to_owned());
            }
            Err(crossbeam_channel::TryRecvError::Empty) => {}
        }
        if Instant::now() >= deadline {
            return Err(format!("listener did not run within {timeout:?}"));
        }
        rt.timeout(1).await;
    }
}

/// 比较 registry 名称集合，明确忽略 hash map 的不稳定迭代顺序。
async fn expect_table_names(db: &RealDb, expected: &[&str]) -> TestResult<()> {
    let actual = db
        .tables()
        .await
        .into_iter()
        .map(|name| name.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    let expected = expected
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    expect_eq("table name set", &actual, &expected)
}

fn table_meta(table_type: KVDBTableType, persistence: bool) -> KVTableMeta {
    KVTableMeta::new(table_type, persistence, EnumType::Usize, EnumType::Usize)
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

/// `CommitLogger` 在同一检查点锁内把一个事务从 waiting 转移到 confirmed。
///
/// 指标 API 分开暴露两个计数，直接各读一次可能撞上转移中间态。这里仅在确认累计值前后
/// 一致时接受夹在中间的 waiting 读数，从而得到某一稳定瞬间的守恒总量；不等待队列清空，
/// 也不把 Manager 的软关闭语义扩大成 graceful shutdown。
async fn confirmation_accounting_snapshot(logger: &CommitLogger) -> TestResult<usize> {
    let deadline = Instant::now() + OBSERVATION_TIMEOUT;
    loop {
        let confirmed_before = logger.confirm_total_count();
        let waiting = logger.waiting_confirm_count().await;
        let confirmed_after = logger.confirm_total_count();
        if confirmed_before == confirmed_after {
            return confirmed_after
                .checked_add(waiting)
                .ok_or_else(|| "CommitLogger confirmation accounting overflowed usize".to_owned());
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "CommitLogger confirmation accounting did not reach a stable observation within {:?}",
                OBSERVATION_TIMEOUT
            ));
        }
    }
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

/// 构建带真实 listener 的数据库，并把每次 drain 后的批次摘要发送给测试线程。
async fn build_database_with_listener(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
    event_tx: Sender<EventObservation>,
) -> TestResult<Fixture> {
    let (tr_manager, logger) = build_transaction_manager(rt, root).await?;
    let db_path = root.join("database");
    let db = KVDBManagerBuilder::new(rt.clone(), tr_manager.clone(), &db_path)
        .startup_with_listener(
            false,
            Some(
                move |db: &RealDb, manager: &RealTrManager, events: &mut Vec<KVDBEvent<Guid>>| {
                    let mut observation = EventObservation {
                        total: events.len(),
                        reports: 0,
                        commit_failed: 0,
                        confirmed: 0,
                        transaction_len: manager.transaction_len(),
                        db_path: db.db_path().to_path_buf(),
                    };
                    for event in events.drain(..) {
                        if event.is_report_transaction_info() {
                            observation.reports += 1;
                        } else if event.is_commit_failed() {
                            observation.commit_failed += 1;
                        } else if event.is_confirm_commited() {
                            observation.confirmed += 1;
                        }
                    }
                    let _ = event_tx.send(observation);
                },
            ),
        )
        .await
        .map_err(|error| format!("starting listener database at {db_path:?} failed: {error}"))?;
    Ok(Fixture {
        db,
        tr_manager,
        logger,
    })
}

/// 构建无 listener 的对照数据库。
async fn build_database(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<Fixture> {
    let (tr_manager, logger) = build_transaction_manager(rt, root).await?;
    let db_path = root.join("database");
    let db = KVDBManagerBuilder::new(rt.clone(), tr_manager.clone(), &db_path)
        .startup(false)
        .await
        .map_err(|error| format!("starting database at {db_path:?} failed: {error}"))?;
    Ok(Fixture {
        db,
        tr_manager,
        logger,
    })
}

async fn build_transaction_manager(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
) -> TestResult<(RealTrManager, CommitLogger)> {
    fs::create_dir_all(root)
        .map_err(|error| format!("creating manager fixture root {root:?} failed: {error}"))?;
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
    Ok((manager, logger))
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
    .map_err(|error| format!("spawning manager contract future failed: {error:?}"))?;

    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("manager contract future exceeded {timeout:?}: {error}"))?
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

#[derive(Debug)]
struct EventObservation {
    total: usize,
    reports: usize,
    commit_failed: usize,
    confirmed: usize,
    transaction_len: usize,
    db_path: PathBuf,
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
            "pi_db_manager_contract_{label}_{}_{}",
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
