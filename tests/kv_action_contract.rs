//! `KVAction` 与根事务动作路由的真实当前实现契约矩阵。
//!
//! 本 target 不引用或运行旧测试。它通过公开
//! `KVDBManagerBuilder -> KVDBManager -> KVDBTransaction` 链路，使用真实多线程 runtime、
//! `Transaction2PcManager`、`CommitLogger`、Meta/Memory/LogOrdered/LogWrite/Btree 表和真实
//! 文件系统。测试覆盖：
//!
//! - 四类用户表的普通/dirty upsert、query 和 delete 的事务私有可见性与返回值顺序；两套
//!   非空动作严格使用不同根事务，避免用协议禁止的混用路径证明公开语义；
//! - delete 返回值的逐表差异：Memory/LogOrdered 不返回旧值，Btree 返回可取得的旧值，
//!   LogWrite 不执行删除；
//! - LogWrite 当前只写边界；
//! - 缺表在普通/dirty query、upsert、delete 上互不相同的返回语义；普通 upsert 返回 Fatal 后
//!   立即丢弃该根，另用独立误用根验证 rollback 被拒绝，绝不把 Fatal 后继续使用当成合法证据；
//! - `TableKV::value=None` 传给 upsert 时当前是 no-op，而不是 delete；
//! - 五类表的 `lock_key/unlock_key` 当前不互斥、不校验 owner，缺表也成功；
//! - `CreateTableOptions` 对 Memory/LogWrite 当前被忽略，对 LogOrdered/Btree 必须匹配 variant。
//!
//! `FIND-DIRTY-001`、`FIND-LOCK-001` 和配置校验差异仍是待设计/已归档边界。这里的断言只
//! 客观描述当前生产实现，并明确不是最终或最佳事务/锁/配置设计；若相关设计解冻，必须同步
//! 更新实现、注释、正式契约与本 target。测试不提交用户数据，因此不把异步数据文件确认
//! 混入点操作语义；DDL 事务仍走真实 prepare/根 WAL/commit 路径。
//!
//! 被测入口：`pi_db::KVAction`、`KVDBTransaction::{query,dirty_query,upsert,dirty_upsert,
//! delete,dirty_delete,lock_key,unlock_key,create_table_with_options}`。
//! 文档入口：`docs/SEMANTIC_CONTRACTS.md#contract-action-001`。

use std::{
    fs,
    future::Future,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::{
    manager_2pc::{Transaction2PcManager, Transaction2PcStatus},
    AsyncCommitLog, ErrorLevel, UnitTransaction,
};
use pi_atom::Atom;
use pi_bon::{Decode, Encode, ReadBuffer, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    utils::CreateTableOptions,
    Binary, KVDBTableType, KVTableMeta, KVTableTrError,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type RealManager = Transaction2PcManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const MEMORY_TABLE: &str = "action_memory";
const LOG_ORDERED_TABLE: &str = "action_log_ordered";
const LOG_WRITE_TABLE: &str = "action_log_write";
const BTREE_TABLE: &str = "action_btree";
const MISSING_TABLE: &str = "action_missing";
const INVALID_LOG_OPTIONS_TABLE: &str = "action_invalid_log_options";
const INVALID_BTREE_OPTIONS_TABLE: &str = "action_invalid_btree_options";
const TEST_TIMEOUT: Duration = Duration::from_secs(45);

/// 在真实多线程 runtime 上运行完整矩阵，并用同步通道施加硬截止。
#[test]
fn test_kv_action_current_contract_matrix() {
    let root = TempRoot::new("matrix").expect("creating the KVAction test root must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let (db, tr_manager, logger) = build_database(&rt, &root_path).await?;
        create_tables(&db).await?;
        exercise_point_action_matrix(&db).await?;
        exercise_missing_table_matrix(&db, &tr_manager, &logger).await?;
        exercise_noop_lock_matrix(&db).await?;
        exercise_invalid_option_matrix(&db).await?;
        Ok(())
    })
    .unwrap_or_else(|error| panic!("KVAction current contract matrix failed: {error}"));
}

/// 创建四类用户表；故意给 Memory/LogWrite 传入不相干 variant，以验证当前忽略语义。
async fn create_tables(db: &RealDb) -> TestResult<()> {
    let transaction = transaction(db, "KVAction DDL", true)?;

    transaction
        .create_table_with_options(
            Atom::from(MEMORY_TABLE),
            table_meta(KVDBTableType::MemOrdTab, false),
            CreateTableOptions::BtreeOrdTab(usize::MAX, true),
            false,
        )
        .await
        .map_err(|error| format!("Memory must currently ignore unrelated options: {error}"))?;

    transaction
        .create_table_with_options(
            Atom::from(LOG_ORDERED_TABLE),
            table_meta(KVDBTableType::LogOrdTab, true),
            CreateTableOptions::LogOrdTab(64 * 1024 * 1024, 1024 * 1024, 1024 * 1024),
            false,
        )
        .await
        .map_err(|error| format!("creating LogOrdered table failed: {error}"))?;

    transaction
        .create_table_with_options(
            Atom::from(LOG_WRITE_TABLE),
            table_meta(KVDBTableType::LogWTab, true),
            CreateTableOptions::LogOrdTab(1, 2, 3),
            false,
        )
        .await
        .map_err(|error| format!("LogWrite must currently ignore unrelated options: {error}"))?;

    transaction
        .create_table_with_options(
            Atom::from(BTREE_TABLE),
            table_meta(KVDBTableType::BtreeOrdTab, true),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .map_err(|error| format!("creating Btree table failed: {error}"))?;

    commit_transaction(&transaction, "KVAction DDL").await
}

/// 分别验证普通族和 dirty 族的事务私有点操作、输入顺序、旧值和 LogWrite 当前只写边界。
///
/// 两个非空操作族必须使用不同根事务。空批次另用第三个根验证其在协议选择前直接短路；该
/// 特例不能推广为允许混用非空动作。
async fn exercise_point_action_matrix(db: &RealDb) -> TestResult<()> {
    let ordered_tables = [MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE];
    let ordinary = transaction(db, "KVAction ordinary point matrix", true)?;

    ordinary
        .upsert(
            ordered_tables
                .iter()
                .enumerate()
                .map(|(index, table)| kv(table, 10 + index, Some(110 + index)))
                .chain(std::iter::once(kv(LOG_WRITE_TABLE, 13, Some(113))))
                .collect(),
        )
        .await
        .map_err(|error| format!("ordinary upsert matrix failed: {error:?}"))?;

    let query_input = vec![
        kv(MISSING_TABLE, 99, Some(9_999)),
        kv(MEMORY_TABLE, 10, Some(9_999)),
        kv(LOG_ORDERED_TABLE, 11, Some(9_999)),
        kv(BTREE_TABLE, 12, Some(9_999)),
        kv(LOG_WRITE_TABLE, 13, Some(9_999)),
    ];
    assert_values(
        "ordinary query order",
        ordinary.query(query_input).await,
        &[None, Some(110), Some(111), Some(112), None],
    )?;

    // `None` in an upsert input is ignored; it does not reuse delete semantics.
    ordinary
        .upsert(vec![kv(MEMORY_TABLE, 10, None)])
        .await
        .map_err(|error| format!("None ordinary upsert returned an error: {error:?}"))?;
    assert_values(
        "None ordinary upsert must currently preserve the existing value",
        ordinary.query(vec![kv(MEMORY_TABLE, 10, None)]).await,
        &[Some(110)],
    )?;

    let deleted = ordinary
        .delete(vec![
            kv(MISSING_TABLE, 99, None),
            kv(MEMORY_TABLE, 10, None),
            kv(LOG_ORDERED_TABLE, 11, None),
            kv(BTREE_TABLE, 12, None),
            kv(LOG_WRITE_TABLE, 13, None),
        ])
        .await
        .map_err(|error| format!("ordinary delete matrix failed: {error:?}"))?;
    // Memory/LogOrdered 会完成删除但有意不复制旧值；只有 Btree 返回可取得的旧值。
    // 因而前三个 None 分别表示缺表、Memory 命中和 LogOrdered 命中，不能按统一语义解释。
    assert_values(
        "ordinary delete table-specific return values",
        deleted,
        &[None, None, None, Some(112), None],
    )?;
    assert_values(
        "ordinary delete post-state",
        ordinary
            .query(vec![
                kv(MEMORY_TABLE, 10, None),
                kv(LOG_ORDERED_TABLE, 11, None),
                kv(BTREE_TABLE, 12, None),
                kv(LOG_WRITE_TABLE, 13, None),
            ])
            .await,
        &[None, None, None, None],
    )?;
    drop(ordinary);

    let dirty = transaction(db, "KVAction dirty point matrix", true)?;
    dirty
        .dirty_upsert(
            ordered_tables
                .iter()
                .enumerate()
                .map(|(index, table)| kv(table, 20 + index, Some(220 + index)))
                .chain(std::iter::once(kv(LOG_WRITE_TABLE, 23, Some(223))))
                .collect(),
        )
        .await
        .map_err(|error| format!("dirty upsert matrix failed: {error:?}"))?;
    let dirty_query_input = vec![
        kv(MISSING_TABLE, 199, Some(19_999)),
        kv(MEMORY_TABLE, 20, Some(19_999)),
        kv(LOG_ORDERED_TABLE, 21, Some(19_999)),
        kv(BTREE_TABLE, 22, Some(19_999)),
        kv(LOG_WRITE_TABLE, 23, Some(19_999)),
    ];
    assert_values(
        "dirty query order",
        dirty.dirty_query(dirty_query_input).await,
        &[None, Some(220), Some(221), Some(222), None],
    )?;
    dirty
        .dirty_upsert(vec![kv(MEMORY_TABLE, 20, None)])
        .await
        .map_err(|error| format!("None dirty upsert returned an error: {error:?}"))?;
    assert_values(
        "None dirty upsert must currently preserve the existing value",
        dirty.dirty_query(vec![kv(MEMORY_TABLE, 20, None)]).await,
        &[Some(220)],
    )?;

    let dirty_deleted = dirty
        .dirty_delete(vec![
            kv(MEMORY_TABLE, 20, None),
            kv(LOG_ORDERED_TABLE, 21, None),
            kv(BTREE_TABLE, 22, None),
            kv(LOG_WRITE_TABLE, 23, None),
        ])
        .await
        .map_err(|error| format!("dirty delete matrix failed: {error:?}"))?;
    assert_values(
        "dirty delete table-specific return values",
        dirty_deleted,
        &[None, None, Some(222), None],
    )?;
    assert_values(
        "dirty delete post-state",
        dirty
            .dirty_query(vec![
                kv(MEMORY_TABLE, 20, None),
                kv(LOG_ORDERED_TABLE, 21, None),
                kv(BTREE_TABLE, 22, None),
                kv(LOG_WRITE_TABLE, 23, None),
            ])
            .await,
        &[None, None, None, None],
    )?;
    drop(dirty);

    // Empty batches return before protocol selection, so this neutral root may exercise both names.
    let empty = transaction(db, "KVAction protocol-neutral empty batches", true)?;
    assert!(empty.query(Vec::new()).await.is_empty());
    assert!(empty.dirty_query(Vec::new()).await.is_empty());
    empty
        .upsert(Vec::new())
        .await
        .map_err(|error| format!("empty upsert failed: {error:?}"))?;
    empty
        .dirty_upsert(Vec::new())
        .await
        .map_err(|error| format!("empty dirty upsert failed: {error:?}"))?;
    assert!(empty
        .delete(Vec::new())
        .await
        .map_err(|error| format!("empty delete failed: {error:?}"))?
        .is_empty());
    assert!(empty
        .dirty_delete(Vec::new())
        .await
        .map_err(|error| format!("empty dirty delete failed: {error:?}"))?
        .is_empty());

    // 六个空点操作必须在协议选择前短路。随后能够合法选择并完成空版本 2PC，才是对该
    // 中立性的公开可观察证明；仅断言普通/dirty 空调用都成功并不能排除它们选择了 Ordinary。
    let version_prepare = empty
        .prepare_with_version(Vec::new(), Vec::new())
        .await
        .map_err(|error| format!("empty point actions unexpectedly selected a protocol: {error:?}"))?;
    let version_receipt = empty
        .commit_with_version(version_prepare)
        .await
        .map_err(|error| format!("committing protocol-neutral empty action proof failed: {error:?}"))?;
    if !version_receipt.is_empty() {
        return Err(format!(
            "protocol-neutral empty action proof returned unexpected version receipts: {version_receipt:?}"
        ));
    }
    drop(empty);
    Ok(())
}

/// 验证缺表的普通写为 Fatal，而 dirty 写静默成功；读/删除均保留一个 `None` 槽位。
///
/// 普通读/删、Fatal 写和 Fatal 后 rollback 防御分别使用不同根。Fatal 根只允许被立即 drop；
/// rollback probe 是独立的非法调用防御测试，观察一次拒绝后也立即 drop，不能解释为可恢复。
async fn exercise_missing_table_matrix(db: &RealDb,
                                       tr_manager: &RealManager,
                                       logger: &CommitLogger) -> TestResult<()> {
    let produced_before = tr_manager.produced_transaction_total();
    let consumed_before = tr_manager.consumed_transaction_total();
    let append_before = logger.append_total_count();
    if tr_manager.transaction_len() != 0 {
        return Err(format!(
            "missing-table matrix started with {} registered transactions",
            tr_manager.transaction_len()
        ));
    }

    let dirty = transaction(db, "KVAction missing dirty", true)?;
    dirty
        .dirty_upsert(vec![kv(MISSING_TABLE, 1, Some(2))])
        .await
        .map_err(|error| format!("dirty upsert currently skips a missing table: {error:?}"))?;
    assert_values(
        "missing dirty query",
        dirty.dirty_query(vec![kv(MISSING_TABLE, 1, None)]).await,
        &[None],
    )?;
    assert_values(
        "missing dirty delete",
        dirty
            .dirty_delete(vec![kv(MISSING_TABLE, 1, None)])
            .await
            .map_err(|error| format!("missing dirty delete failed: {error:?}"))?,
        &[None],
    )?;
    drop(dirty);

    let ordinary_read_delete = transaction(db, "KVAction missing ordinary read delete", true)?;
    assert_values(
        "missing ordinary query",
        ordinary_read_delete
            .query(vec![kv(MISSING_TABLE, 1, None)])
            .await,
        &[None],
    )?;
    assert_values(
        "missing ordinary delete",
        ordinary_read_delete
            .delete(vec![kv(MISSING_TABLE, 1, None)])
            .await
            .map_err(|error| format!("missing ordinary delete failed: {error:?}"))?,
        &[None],
    )?;
    drop(ordinary_read_delete);

    // 前三项只写入事务私有 COW/overlay，末项缺表返回 Fatal。Fatal 后不得再调用该根；drop 后
    // 新根逐表权威查询必须仍为空，从外部证明批次前缀没有发布，也没有进入根 WAL。
    let ordinary_fatal = transaction(db, "KVAction missing ordinary Fatal", true)?;
    let error = ordinary_fatal
        .upsert(vec![
            kv(MEMORY_TABLE, 31, Some(331)),
            kv(LOG_ORDERED_TABLE, 32, Some(332)),
            kv(BTREE_TABLE, 33, Some(333)),
            kv(MISSING_TABLE, 34, Some(334)),
        ])
        .await
        .expect_err("ordinary upsert of a missing table must currently return an error");
    if !matches!(error.level(), ErrorLevel::Fatal) {
        return Err(format!(
            "ordinary missing-table upsert returned non-Fatal error: {error:?}"
        ));
    }
    drop(ordinary_fatal);

    let verifier = transaction(db, "KVAction missing Fatal verifier", false)?;
    assert_values(
        "Fatal batch prefix must remain unpublished",
        verifier
            .query(vec![
                kv(MEMORY_TABLE, 31, None),
                kv(LOG_ORDERED_TABLE, 32, None),
                kv(BTREE_TABLE, 33, None),
            ])
            .await,
        &[None, None, None],
    )?;
    drop(verifier);

    // 该根只用于验证状态机对“Fatal 后 rollback”这一非法调用的防御结果。返回 Normal 表示
    // rollback 因根仍处于 Start 而被拒绝，不表示原 Fatal 可恢复；拒绝后不再使用该根。
    let rollback_probe = transaction(db, "KVAction missing Fatal rollback probe", true)?;
    let fatal = rollback_probe
        .upsert(vec![kv(MISSING_TABLE, 41, Some(441))])
        .await
        .expect_err("rollback probe must first produce the missing-table Fatal");
    if !matches!(fatal.level(), ErrorLevel::Fatal) {
        return Err(format!(
            "rollback probe missing-table upsert returned non-Fatal error: {fatal:?}"
        ));
    }
    let rollback_error = rollback_probe
        .rollback_modified()
        .await
        .expect_err("rollback after Fatal must be rejected");
    if !matches!(&rollback_error, KVTableTrError::Common(ErrorLevel::Normal, _)) {
        return Err(format!(
            "rollback rejection must be Common(Normal), actual: {rollback_error:?}"
        ));
    }
    if rollback_probe.get_status() != Transaction2PcStatus::RollbackFailed {
        return Err(format!(
            "rollback rejection must leave the misuse root in RollbackFailed, actual: {:?}",
            rollback_probe.get_status()
        ));
    }
    drop(rollback_probe);

    if tr_manager.produced_transaction_total() != produced_before
        || tr_manager.consumed_transaction_total() != consumed_before
        || tr_manager.transaction_len() != 0
        || logger.append_total_count() != append_before {
        return Err(format!(
            "missing-table actions unexpectedly entered 2PC or WAL, produced={}->{}, consumed={}->{}, active={}, appended={}->{}",
            produced_before,
            tr_manager.produced_transaction_total(),
            consumed_before,
            tr_manager.consumed_transaction_total(),
            tr_manager.transaction_len(),
            append_before,
            logger.append_total_count()
        ));
    }
    Ok(())
}

/// 两个事务在首个 owner 未解锁前都取得同一 Key，并验证无 owner 解锁和缺表调用也成功。
async fn exercise_noop_lock_matrix(db: &RealDb) -> TestResult<()> {
    let owner = transaction(db, "KVAction lock owner", true)?;
    let contender = transaction(db, "KVAction lock contender", true)?;
    let key = encode_usize(7);

    owner
        .lock_key(Atom::from(MEMORY_TABLE), key.clone())
        .await
        .map_err(|error| format!("owner lock hook failed: {error:?}"))?;
    contender
        .lock_key(Atom::from(MEMORY_TABLE), key.clone())
        .await
        .map_err(|error| format!("contender lock hook unexpectedly blocked/failed: {error:?}"))?;
    contender
        .unlock_key(Atom::from(MEMORY_TABLE), key.clone())
        .await
        .map_err(|error| format!("non-owner unlock hook failed: {error:?}"))?;
    owner
        .unlock_key(Atom::from(MEMORY_TABLE), key.clone())
        .await
        .map_err(|error| format!("owner unlock hook failed: {error:?}"))?;

    owner
        .lock_key(Atom::from(MISSING_TABLE), key.clone())
        .await
        .map_err(|error| format!("missing-table lock hook failed: {error:?}"))?;
    owner
        .unlock_key(Atom::from(MISSING_TABLE), key)
        .await
        .map_err(|error| format!("missing-table unlock hook failed: {error:?}"))?;

    drop(contender);
    drop(owner);
    Ok(())
}

/// 验证 LogOrdered/Btree 拒绝不匹配 options，且失败调用没有注册对应表。
async fn exercise_invalid_option_matrix(db: &RealDb) -> TestResult<()> {
    let invalid_transaction = transaction(db, "KVAction invalid options", true)?;

    let log_error = invalid_transaction
        .create_table_with_options(
            Atom::from(INVALID_LOG_OPTIONS_TABLE),
            table_meta(KVDBTableType::LogOrdTab, true),
            CreateTableOptions::Empty,
            false,
        )
        .await
        .expect_err("LogOrdered must reject Empty options");
    if log_error.kind() != ErrorKind::Other {
        return Err(format!(
            "invalid LogOrdered options returned {:?} instead of Other",
            log_error.kind()
        ));
    }

    let btree_error = invalid_transaction
        .create_table_with_options(
            Atom::from(INVALID_BTREE_OPTIONS_TABLE),
            table_meta(KVDBTableType::BtreeOrdTab, true),
            CreateTableOptions::LogOrdTab(64 * 1024 * 1024, 1024 * 1024, 1024 * 1024),
            false,
        )
        .await
        .expect_err("Btree must reject LogOrdTab options");
    if btree_error.kind() != ErrorKind::Other {
        return Err(format!(
            "invalid Btree options returned {:?} instead of Other",
            btree_error.kind()
        ));
    }
    drop(invalid_transaction);

    let verifier = transaction(db, "KVAction invalid options verifier", false)?;
    if verifier
        .keys(Atom::from(INVALID_LOG_OPTIONS_TABLE), None, false)
        .await
        .is_some()
    {
        return Err("invalid LogOrdered options still registered a table".to_owned());
    }
    if verifier
        .keys(Atom::from(INVALID_BTREE_OPTIONS_TABLE), None, false)
        .await
        .is_some()
    {
        return Err("invalid Btree options still registered a table".to_owned());
    }
    drop(verifier);
    Ok(())
}

fn table_meta(table_type: KVDBTableType, persistence: bool) -> KVTableMeta {
    KVTableMeta::new(table_type, persistence, EnumType::Usize, EnumType::Usize)
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
        .map_err(|error| format!("decoding BON usize from KVAction output failed: {error:?}"))
}

fn assert_values(
    label: &str,
    actual: Vec<Option<Binary>>,
    expected: &[Option<usize>],
) -> TestResult<()> {
    if actual.len() != expected.len() {
        return Err(format!(
            "{label}: expected {} result slots, observed {}",
            expected.len(),
            actual.len()
        ));
    }

    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        match (actual, expected) {
            (None, None) => {}
            (Some(actual), Some(expected)) => {
                let decoded = decode_usize(actual)?;
                if decoded != *expected {
                    return Err(format!(
                        "{label}: slot {index} expected {expected}, observed {decoded}"
                    ));
                }
            }
            (actual, expected) => {
                return Err(format!(
                    "{label}: slot {index} presence mismatch, expected={expected:?}, actual={actual:?}"
                ));
            }
        }
    }
    Ok(())
}

fn transaction(db: &RealDb, source: &str, writable: bool) -> TestResult<RealTransaction> {
    db.transaction(Atom::from(source), writable, 10_000, 10_000)
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

async fn build_database(rt: &MultiTaskRuntime<()>,
                        root: &Path) -> TestResult<(RealDb, RealManager, CommitLogger)> {
    fs::create_dir_all(root)
        .map_err(|error| format!("creating KVAction root {root:?} failed: {error}"))?;
    let wal_path = root.join("root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .build()
        .await
        .map_err(|error| format!("building CommitLogger at {wal_path:?} failed: {error}"))?;
    let manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger.clone(),
    );
    let db = KVDBManagerBuilder::new(rt.clone(), manager.clone(), root.join("database"))
        .startup(false)
        .await
        .map_err(|error| format!("starting KVAction database failed: {error}"))?;
    Ok((db, manager, logger))
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
    .map_err(|error| format!("spawning KVAction future failed: {error:?}"))?;

    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("KVAction future exceeded {timeout:?}: {error}"))?
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
            "pi_db_kv_action_{label}_{}_{}",
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
