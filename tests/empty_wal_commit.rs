//! 可写事务只有读动作时的空 WAL 提交闭环专项。
//!
//! 本 target 使用真实 4-worker runtime、`Transaction2PcManager`、`CommitLogger`、五类表和真实
//! 文件系统。DDL 矩阵严格复现三个互相独立的根事务：首次建表、同元信息幂等建表、最终删表。
//! 第二个根的 prepare output 必须为空，但它仍是可写事务，commit 必须关闭 Meta 读预留且不得
//! 追加、刷新或确认 WAL；第三个根的 prepare 成功是预留确实释放的硬门禁。另一矩阵直接验证
//! Memory、LogOrdered、Btree 的可写 query-only 与 dirty_query-only 事务及后续同 Key 写入。
//! 两类点操作严格放在不同根事务中；外部协议禁止在同一事务混用 dirty 与普通点操作。
//!
//! 显式只读事务由 `is_writable() == false` 判定，仍不需要 commit；空 input 本身不能用来推断
//! 事务只读。框架任意深度和显式只读正对照由
//! `../pi_async_transaction/tests/all_conflicts.rs` 永久验证。

use std::{
    fmt::Debug,
    fs,
    future::Future,
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
    AsyncCommitLog, UnitTransaction,
};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
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

const TEST_TIMEOUT: Duration = Duration::from_secs(60);

#[test]
fn test_writable_read_only_actions_commit_without_wal() {
    let root = TempRoot::new().expect("creating empty-WAL test root must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let cases = [
            ("Memory", "empty_wal_commit_memory", KVDBTableType::MemOrdTab),
            ("LogOrdered", "empty_wal_commit_log_ordered", KVDBTableType::LogOrdTab),
            ("LogWrite", "empty_wal_commit_log_write", KVDBTableType::LogWTab),
            ("Btree", "empty_wal_commit_btree", KVDBTableType::BtreeOrdTab),
        ];
        for (label, table_name, table_type) in cases {
            let fixture = build_database(&rt, &root_path.join(table_name)).await?;
            verify_idempotent_create_empty_commit(&fixture, label, table_name, table_type).await?;
        }
        Ok(())
    })
    .unwrap_or_else(|error| panic!("empty WAL commit contract failed: {error}"));
}

async fn verify_idempotent_create_empty_commit(
    fixture: &Fixture,
    label: &str,
    table: &str,
    table_type: KVDBTableType,
) -> TestResult<()> {
    let table_name = Atom::from(table);
    let meta = KVTableMeta::new(table_type, true, EnumType::Usize, EnumType::Usize);

    let create = transaction(&fixture.db, &format!("{label} empty WAL create"), true)?;
    create
        .create_table(table_name.clone(), meta.clone(), false)
        .await
        .map_err(|error| format!("creating {label} probe table failed: {error}"))?;
    commit_transaction(&create, &format!("{label} initial create")).await?;
    require(
        fixture.db.is_exist(&table_name).await,
        &format!("created {label} table is absent"),
    )?;

    let append_before = fixture.logger.append_total_count();
    let confirm_before = fixture.logger.confirm_total_count();
    let waiting_before = fixture.logger.waiting_confirm_count().await;
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();

    let no_op = transaction(
        &fixture.db,
        &format!("{label} empty WAL idempotent create"),
        true,
    )?;
    no_op
        .create_table(table_name.clone(), meta.clone(), false)
        .await
        .map_err(|error| format!("{label} idempotent create failed: {error}"))?;
    let prepare = no_op
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing {label} idempotent create failed: {error:?}"))?;
    expect_eq(&format!("{label} idempotent prepare bytes"), &prepare.len(), &0)?;
    no_op
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing {label} idempotent create failed: {error:?}"))?;
    let no_op_status = no_op.get_status();

    expect_eq(
        &format!("{label} idempotent manager registry"),
        &fixture.tr_manager.transaction_len(),
        &0,
    )?;
    expect_eq(
        &format!("{label} idempotent produced transaction count"),
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        &format!("{label} idempotent consumed transaction count"),
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 1),
    )?;
    expect_eq(
        &format!("{label} idempotent WAL append count"),
        &fixture.logger.append_total_count(),
        &append_before,
    )?;
    expect_eq(
        &format!("{label} idempotent WAL confirm count"),
        &fixture.logger.confirm_total_count(),
        &confirm_before,
    )?;
    expect_eq(
        &format!("{label} idempotent waiting WAL count"),
        &fixture.logger.waiting_confirm_count().await,
        &waiting_before,
    )?;
    require(
        fixture.db.is_exist(&table_name).await,
        &format!("{label} idempotent create changed the table registry"),
    )?;
    let observed_meta = query_table_meta(&fixture.db, table_name.clone()).await?;
    expect_eq(&format!("{label} idempotent Meta value"), &observed_meta, &Some(meta))?;

    let remove = transaction(&fixture.db, &format!("{label} empty WAL remove"), true)?;
    remove
        .remove_table(table_name.clone())
        .await
        .map_err(|error| format!("removing {label} probe table failed: {error}"))?;
    let remove_prepare = remove
        .prepare_modified_conflicts()
        .await
        .map_err(|error| {
            format!(
                "{label} empty-WAL commit leaked a Meta prepare reservation, idempotent_root_status={no_op_status:?}: {error:?}"
            )
        })?;
    expect_eq(
        &format!("{label} idempotent root status"),
        &no_op_status,
        &Transaction2PcStatus::Commited,
    )?;
    remove
        .commit_modified(remove_prepare)
        .await
        .map_err(|error| format!("committing {label} probe removal failed: {error:?}"))?;

    require(
        !fixture.db.is_exist(&table_name).await,
        &format!("removed {label} table remains in the registry"),
    )?;
    expect_eq(
        &format!("removed {label} Meta value"),
        &query_table_meta(&fixture.db, table_name).await?,
        &None,
    )?;
    expect_eq(
        &format!("{label} final manager registry"),
        &fixture.tr_manager.transaction_len(),
        &0,
    )
}

#[test]
fn test_writable_query_only_transaction_closes_table_read_reservations() {
    let root = TempRoot::new().expect("creating query-only test root must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt, &root_path).await?;
        let cases = [
            ("Memory", "query_only_memory", KVDBTableType::MemOrdTab),
            ("LogOrdered", "query_only_log_ordered", KVDBTableType::LogOrdTab),
            ("Btree", "query_only_btree", KVDBTableType::BtreeOrdTab),
        ];

        let ddl = transaction(&fixture.db, "query-only table DDL", true)?;
        for (_, table, table_type) in &cases {
            ddl.create_table(
                Atom::from(*table),
                KVTableMeta::new(table_type.clone(), true, EnumType::Usize, EnumType::Usize),
                false,
            )
            .await
            .map_err(|error| format!("creating query-only {table} failed: {error}"))?;
        }
        commit_transaction(&ddl, "query-only table DDL").await?;

        for (index, (label, table, _)) in cases.iter().enumerate() {
            let key = encode_usize(0x40 + index);
            let value = encode_usize(0x70 + index);
            let reader = transaction(
                &fixture.db,
                &format!("{label} writable query-only transaction"),
                true,
            )?;
            let query = vec![TableKV::new(Atom::from(*table), key.clone(), None)];
            expect_eq(
                &format!("{label} ordinary missing-key query"),
                &reader.query(query).await,
                &vec![None],
            )?;
            let append_before = fixture.logger.append_total_count();
            let confirm_before = fixture.logger.confirm_total_count();
            let waiting_before = fixture.logger.waiting_confirm_count().await;
            let prepare = reader
                .prepare_modified_conflicts()
                .await
                .map_err(|error| format!("preparing {label} query-only transaction failed: {error:?}"))?;
            expect_eq(&format!("{label} query-only prepare bytes"), &prepare.len(), &0)?;
            reader
                .commit_modified(prepare)
                .await
                .map_err(|error| format!("committing {label} query-only transaction failed: {error:?}"))?;
            expect_eq(
                &format!("{label} query-only root status"),
                &reader.get_status(),
                &Transaction2PcStatus::Commited,
            )?;
            expect_eq(
                &format!("{label} query-only WAL append count"),
                &fixture.logger.append_total_count(),
                &append_before,
            )?;
            expect_eq(
                &format!("{label} query-only WAL confirm count"),
                &fixture.logger.confirm_total_count(),
                &confirm_before,
            )?;
            expect_eq(
                &format!("{label} query-only waiting WAL count"),
                &fixture.logger.waiting_confirm_count().await,
                &waiting_before,
            )?;

            let writer = transaction(
                &fixture.db,
                &format!("{label} post-query writer"),
                true,
            )?;
            writer
                .upsert(vec![TableKV::new(
                    Atom::from(*table),
                    key.clone(),
                    Some(value.clone()),
                )])
                .await
                .map_err(|error| format!("{label} post-query upsert failed: {error:?}"))?;
            let writer_prepare = writer
                .prepare_modified_conflicts()
                .await
                .map_err(|error| {
                    format!("{label} query-only commit leaked a table read reservation: {error:?}")
                })?;
            writer
                .commit_modified(writer_prepare)
                .await
                .map_err(|error| format!("committing {label} post-query writer failed: {error:?}"))?;

            let observer = transaction(&fixture.db, &format!("{label} query-only observer"), false)?;
            let observed = observer
                .query(vec![TableKV::new(Atom::from(*table), key, None)])
                .await;
            expect_eq(
                &format!("{label} post-query committed value"),
                &observed,
                &vec![Some(value)],
            )?;

            let dirty_key = encode_usize(0x140 + index);
            let dirty_value = encode_usize(0x170 + index);
            let dirty_reader = transaction(
                &fixture.db,
                &format!("{label} writable dirty-query-only transaction"),
                true,
            )?;
            expect_eq(
                &format!("{label} dirty-query-only missing-key query"),
                &dirty_reader
                    .dirty_query(vec![TableKV::new(
                        Atom::from(*table),
                        dirty_key.clone(),
                        None,
                    )])
                    .await,
                &vec![None],
            )?;

            let dirty_append_before = fixture.logger.append_total_count();
            let dirty_confirm_before = fixture.logger.confirm_total_count();
            let dirty_waiting_before = fixture.logger.waiting_confirm_count().await;
            let dirty_prepare = dirty_reader
                .prepare_modified_conflicts()
                .await
                .map_err(|error| {
                    format!("preparing {label} dirty-query-only transaction failed: {error:?}")
                })?;
            expect_eq(
                &format!("{label} dirty-query-only prepare bytes"),
                &dirty_prepare.len(),
                &0,
            )?;
            dirty_reader
                .commit_modified(dirty_prepare)
                .await
                .map_err(|error| {
                    format!("committing {label} dirty-query-only transaction failed: {error:?}")
                })?;
            expect_eq(
                &format!("{label} dirty-query-only root status"),
                &dirty_reader.get_status(),
                &Transaction2PcStatus::Commited,
            )?;
            expect_eq(
                &format!("{label} dirty-query-only WAL append count"),
                &fixture.logger.append_total_count(),
                &dirty_append_before,
            )?;
            expect_eq(
                &format!("{label} dirty-query-only WAL confirm count"),
                &fixture.logger.confirm_total_count(),
                &dirty_confirm_before,
            )?;
            expect_eq(
                &format!("{label} dirty-query-only waiting WAL count"),
                &fixture.logger.waiting_confirm_count().await,
                &dirty_waiting_before,
            )?;

            let dirty_writer = transaction(
                &fixture.db,
                &format!("{label} post-dirty-query writer"),
                true,
            )?;
            dirty_writer
                .upsert(vec![TableKV::new(
                    Atom::from(*table),
                    dirty_key.clone(),
                    Some(dirty_value.clone()),
                )])
                .await
                .map_err(|error| format!("{label} post-dirty-query upsert failed: {error:?}"))?;
            let dirty_writer_prepare = dirty_writer
                .prepare_modified_conflicts()
                .await
                .map_err(|error| {
                    format!(
                        "{label} dirty-query-only commit left child state or a read reservation: {error:?}"
                    )
                })?;
            dirty_writer
                .commit_modified(dirty_writer_prepare)
                .await
                .map_err(|error| {
                    format!("committing {label} post-dirty-query writer failed: {error:?}")
                })?;

            let dirty_observer = transaction(
                &fixture.db,
                &format!("{label} dirty-query-only observer"),
                false,
            )?;
            let dirty_observed = dirty_observer
                .query(vec![TableKV::new(Atom::from(*table), dirty_key, None)])
                .await;
            expect_eq(
                &format!("{label} post-dirty-query committed value"),
                &dirty_observed,
                &vec![Some(dirty_value)],
            )?;
            expect_eq(
                &format!("{label} manager registry after separated query-family flows"),
                &fixture.tr_manager.transaction_len(),
                &0,
            )?;
        }

        Ok(())
    })
    .unwrap_or_else(|error| panic!("query-only transaction contract failed: {error}"));
}

async fn query_table_meta(db: &RealDb, table: Atom) -> TestResult<Option<KVTableMeta>> {
    let transaction = transaction(db, "empty WAL Meta observer", false)?;
    Ok(transaction.table_meta(table).await)
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

fn transaction(db: &RealDb, source: &str, writable: bool) -> TestResult<RealTransaction> {
    db.transaction(Atom::from(source), writable, 10_000, 10_000)
        .ok_or_else(|| format!("database rejected transaction {source}"))
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
    let tr_manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger.clone(),
    );
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
    .map_err(|error| format!("spawning empty-WAL future failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("empty-WAL future exceeded {timeout:?}: {error}"))?
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

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
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
            "pi_db_empty_wal_commit_{}_{}",
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
