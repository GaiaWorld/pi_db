//! `KVAction` 与根事务动作路由的真实当前实现契约矩阵。
//!
//! 本 target 不引用或运行旧测试。它通过公开
//! `KVDBManagerBuilder -> KVDBManager -> KVDBTransaction` 链路，使用真实多线程 runtime、
//! `Transaction2PcManager`、`CommitLogger`、Meta/Memory/LogOrdered/LogWrite/Btree 表和真实
//! 文件系统。测试覆盖：
//!
//! - 四类用户表的普通/dirty upsert、query 和 delete 的事务私有可见性与返回值顺序；
//! - delete 返回值的逐表差异：Memory/LogOrdered 不返回旧值，Btree 返回可取得的旧值，
//!   LogWrite 不执行删除；
//! - LogWrite 当前只写边界；
//! - 缺表在普通/dirty query、upsert、delete 上互不相同的返回语义；
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
use pi_async_transaction::{manager_2pc::Transaction2PcManager, ErrorLevel};
use pi_atom::Atom;
use pi_bon::{Decode, Encode, ReadBuffer, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    utils::CreateTableOptions,
    Binary, KVDBTableType, KVTableMeta,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
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
        let db = build_database(&rt, &root_path).await?;
        create_tables(&db).await?;
        exercise_point_action_matrix(&db).await?;
        exercise_missing_table_matrix(&db).await?;
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

/// 验证事务私有点操作、输入顺序、旧值和 LogWrite 当前只写边界。
async fn exercise_point_action_matrix(db: &RealDb) -> TestResult<()> {
    let transaction = transaction(db, "KVAction point matrix", true)?;
    let ordered_tables = [MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE];

    transaction
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
        transaction.query(query_input.clone()).await,
        &[None, Some(110), Some(111), Some(112), None],
    )?;
    assert_values(
        "dirty query order",
        transaction.dirty_query(query_input).await,
        &[None, Some(110), Some(111), Some(112), None],
    )?;

    // `None` in an upsert input is ignored; it does not reuse delete semantics.
    transaction
        .upsert(vec![kv(MEMORY_TABLE, 10, None)])
        .await
        .map_err(|error| format!("None ordinary upsert returned an error: {error:?}"))?;
    transaction
        .dirty_upsert(vec![kv(MEMORY_TABLE, 10, None)])
        .await
        .map_err(|error| format!("None dirty upsert returned an error: {error:?}"))?;
    assert_values(
        "None upsert must currently preserve the existing value",
        transaction.query(vec![kv(MEMORY_TABLE, 10, None)]).await,
        &[Some(110)],
    )?;

    let deleted = transaction
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
        transaction
            .query(vec![
                kv(MEMORY_TABLE, 10, None),
                kv(LOG_ORDERED_TABLE, 11, None),
                kv(BTREE_TABLE, 12, None),
                kv(LOG_WRITE_TABLE, 13, None),
            ])
            .await,
        &[None, None, None, None],
    )?;

    transaction
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
    let dirty_deleted = transaction
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

    // Empty batches are exact no-ops and preserve result cardinality.
    assert!(transaction.query(Vec::new()).await.is_empty());
    assert!(transaction.dirty_query(Vec::new()).await.is_empty());
    assert!(transaction
        .delete(Vec::new())
        .await
        .map_err(|error| format!("empty delete failed: {error:?}"))?
        .is_empty());
    assert!(transaction
        .dirty_delete(Vec::new())
        .await
        .map_err(|error| format!("empty dirty delete failed: {error:?}"))?
        .is_empty());

    drop(transaction);
    Ok(())
}

/// 验证缺表的普通写为 Fatal，而 dirty 写静默成功；读/删除均保留一个 `None` 槽位。
async fn exercise_missing_table_matrix(db: &RealDb) -> TestResult<()> {
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

    let ordinary = transaction(db, "KVAction missing ordinary", true)?;
    let error = ordinary
        .upsert(vec![kv(MISSING_TABLE, 1, Some(2))])
        .await
        .expect_err("ordinary upsert of a missing table must currently return an error");
    if !matches!(error.level(), ErrorLevel::Fatal) {
        return Err(format!(
            "ordinary missing-table upsert returned non-Fatal error: {error:?}"
        ));
    }
    assert_values(
        "missing ordinary query",
        ordinary.query(vec![kv(MISSING_TABLE, 1, None)]).await,
        &[None],
    )?;
    assert_values(
        "missing ordinary delete",
        ordinary
            .delete(vec![kv(MISSING_TABLE, 1, None)])
            .await
            .map_err(|error| format!("missing ordinary delete failed: {error:?}"))?,
        &[None],
    )?;
    drop(ordinary);
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

async fn build_database(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<RealDb> {
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
        logger,
    );
    KVDBManagerBuilder::new(rt.clone(), manager, root.join("database"))
        .startup(false)
        .await
        .map_err(|error| format!("starting KVAction database failed: {error}"))
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
