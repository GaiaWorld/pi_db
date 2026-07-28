//! 根事务 `query/dirty_query` 的真实公开契约、快照、冲突和生命周期专项。
//!
//! 本 target 不引用或运行旧测试。完整矩阵使用真实 4-worker runtime、事务管理器、根
//! `CommitLogger`、Memory/LogOrdered/Btree、真实文件系统、WAL、表 collector 和独立冷启动
//! 进程。它严格区分：
//!
//! - 显式只读根查询后直接释放，不进入 2PC；
//! - 可写纯读根即使 prepare 输出为空也必须 commit；
//! - Memory/LogOrdered 在首次触表时固定 COW 根；
//! - Btree 只固定 overlay，overlay 缺席时每次读取 redb，但首次读冲突基线不会刷新；
//! - Memory/LogOrdered dirty_query 不登记 Read，Btree dirty_query 仍复用普通 query；
//! - 拒绝事务零 WAL、成功 writer 精确 WAL、manager 守恒和 data-only 最终状态。
//!
//! 第二个测试是聚焦 TSan 的最小真实装配。它使用非持久 Memory 及始终持久的
//! LogOrdered/Btree，但只写小值且不等待 collector；三个独立只读根与 24 个三表 writer 根
//! 真实并发，所有读者必须持续持有首次触表快照，最终 writer 状态和 manager/WAL 计数必须
//! 精确闭合。
//!
//! 正式契约见 `docs/ROOT_QUERY_CONTRACT.md#root-query-contract-index`。

use std::{
    env,
    fs,
    future::Future,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop,
    AsyncRuntime,
};
use pi_async_transaction::{
    manager_2pc::{Transaction2PcManager, Transaction2PcStatus},
    AsyncCommitLog,
    ErrorLevel,
    Transaction2Pc,
    TransactionTree,
    UnitTransaction,
};
use pi_atom::Atom;
use pi_bon::WriteBuffer;
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    utils::CreateTableOptions,
    Binary,
    KVDBTableType,
    KVTableMeta,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type RealManager = Transaction2PcManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const TEST_NAME: &str = "test_root_query_contract_matrix";
const PHASE_ENV: &str = "PI_DB_ROOT_QUERY_PHASE";
const ROOT_ENV: &str = "PI_DB_ROOT_QUERY_ROOT";
const ARCHIVED_WAL_DIR: &str = "confirmed-root-wal";

const MEMORY_TABLE: &str = "root_query_memory";
const LOG_ORDERED_TABLE: &str = "root_query_log_ordered";
const BTREE_TABLE: &str = "root_query_btree";
const MISSING_TABLE: &str = "root_query_missing";

const PROCESS_TIMEOUT: Duration = Duration::from_secs(180);
const LIVE_TIMEOUT: Duration = Duration::from_secs(150);
const DATA_ONLY_TIMEOUT: Duration = Duration::from_secs(30);
const CONCURRENCY_TIMEOUT: Duration = Duration::from_secs(45);
const OBSERVATION_TIMEOUT: Duration = Duration::from_secs(90);
const BTREE_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

const FILLER_COUNT: usize = 272;
const FILLER_VALUE_BYTES: usize = 4 * 1024;
const CONCURRENT_KEYS: usize = 24;
const CONCURRENT_READERS: usize = 3;
const CONCURRENT_READ_LOOPS: usize = 96;

const TABLE_CASES: [TableCase; 3] = [
    TableCase::new("Memory", MEMORY_TABLE, 0),
    TableCase::new("LogOrdered", LOG_ORDERED_TABLE, 1),
    TableCase::new("Btree", BTREE_TABLE, 2),
];

#[test]
fn test_root_query_contract_matrix() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV)
                .expect("root-query child phase must receive its root path"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("root-query phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root("matrix");
    fs::create_dir_all(&root).expect("creating root-query matrix root must succeed");
    for phase in ["live", "data-only", "data-only-again"] {
        if let Err(error) = run_phase_process(&root, phase, PROCESS_TIMEOUT) {
            panic!(
                "root-query contract failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root,
            );
        }
    }
    fs::remove_dir_all(&root).expect("cleaning root-query matrix root must succeed");
}

#[test]
fn test_root_query_concurrency_safety() {
    let root = TempRoot::new("concurrency")
        .expect("creating root-query concurrency root must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(CONCURRENCY_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt, &root_path).await?;
        create_tables(&fixture, false).await?;
        seed_concurrency_values(&fixture).await?;

        let produced_before = fixture.manager.produced_transaction_total();
        let consumed_before = fixture.manager.consumed_transaction_total();
        let append_before = fixture.logger.append_total_count();
        let ready = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(AtomicBool::new(false));
        let active = Arc::new(AtomicUsize::new(0));
        let (result_tx, result_rx) = bounded(CONCURRENT_READERS);

        for reader_index in 0..CONCURRENT_READERS {
            let db = fixture.db.clone();
            let reader_rt = rt.clone();
            let reader_ready = ready.clone();
            let reader_start = start.clone();
            let reader_active = active.clone();
            let reader_result = result_tx.clone();
            rt.spawn(async move {
                let result = run_concurrent_reader(
                    &db,
                    &reader_rt,
                    reader_index,
                    &reader_ready,
                    &reader_start,
                    &reader_active,
                )
                .await;
                let _ = reader_result.send(result);
            })
            .map_err(|error| format!("spawning root-query reader {reader_index} failed: {error:?}"))?;
        }
        drop(result_tx);

        let ready_deadline = Instant::now() + Duration::from_secs(10);
        while ready.load(Ordering::SeqCst) != CONCURRENT_READERS {
            if Instant::now() >= ready_deadline {
                return Err(format!(
                    "only {} of {} root-query readers reached the initial snapshot gate",
                    ready.load(Ordering::SeqCst),
                    CONCURRENT_READERS,
                ));
            }
            rt.timeout(1).await;
        }
        expect_eq(
            "active readers before concurrent writers",
            &active.load(Ordering::SeqCst),
            &CONCURRENT_READERS,
        )?;
        start.store(true, Ordering::SeqCst);

        let mut overlap_observed = false;
        for key_index in 0..CONCURRENT_KEYS {
            overlap_observed |= active.load(Ordering::SeqCst) > 0;
            let writer = writable_transaction(
                &fixture.db,
                &format!("root-query concurrent writer {key_index}"),
            )?;
            writer
                .upsert(
                    TABLE_CASES
                        .iter()
                        .map(|table| {
                            TableKV::new(
                                Atom::from(table.name),
                                concurrent_key(key_index),
                                Some(concurrent_updated_value(table.index, key_index)),
                            )
                        })
                        .collect(),
                )
                .await
                .map_err(|error| {
                    format!("concurrent writer {key_index} upsert failed: {error:?}")
                })?;
            commit_ordinary(&writer, &format!("concurrent writer {key_index}")).await?;
        }

        for reader_index in 0..CONCURRENT_READERS {
            result_rx
                .recv_timeout(Duration::from_secs(20))
                .map_err(|error| {
                    format!("joining root-query reader {reader_index} failed: {error}")
                })??;
        }
        require(
            overlap_observed,
            "concurrent writers did not overlap any active root-query reader",
        )?;
        expect_eq(
            "active readers after joins",
            &active.load(Ordering::SeqCst),
            &0usize,
        )?;
        expect_eq(
            "concurrent writer produced count",
            &fixture.manager.produced_transaction_total(),
            &(produced_before + CONCURRENT_KEYS),
        )?;
        expect_eq(
            "concurrent writer consumed count",
            &fixture.manager.consumed_transaction_total(),
            &(consumed_before + CONCURRENT_KEYS),
        )?;
        expect_eq(
            "concurrent active transaction registry",
            &fixture.manager.transaction_len(),
            &0usize,
        )?;
        expect_eq(
            "concurrent successful writer WAL count",
            &fixture.logger.append_total_count(),
            &(append_before + CONCURRENT_KEYS),
        )?;
        verify_concurrent_final_values(&fixture.db).await
    })
    .unwrap_or_else(|error| panic!("root-query concurrency safety failed: {error}"));
}

fn run_child_phase(phase: &str, root: &Path) -> TestResult<()> {
    match phase {
        "live" => {
            let root = root.to_path_buf();
            run_on_runtime(LIVE_TIMEOUT, move |rt| async move {
                phase_live(rt, root).await
            })
        },
        "data-only" => {
            archive_root_wal(root)?;
            let root = root.to_path_buf();
            run_on_runtime(DATA_ONLY_TIMEOUT, move |rt| async move {
                phase_data_only(rt, root, "first data-only").await
            })
        },
        "data-only-again" => {
            let root = root.to_path_buf();
            run_on_runtime(DATA_ONLY_TIMEOUT, move |rt| async move {
                phase_data_only(rt, root, "second data-only").await
            })
        },
        other => Err(format!("unknown root-query phase: {other}")),
    }
}

async fn phase_live(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let fixture = build_database(&rt, &root).await?;
    create_tables(&fixture, true).await?;
    seed_persistent_values(&fixture).await?;
    wait_for_btree_cache_zero(&rt, &fixture.db, "initial seed").await?;

    verify_batch_and_read_only_contract(&fixture).await?;
    verify_empty_batch_protocol_neutrality(&fixture).await?;
    verify_writable_read_only_actions_close(&fixture).await?;
    verify_lazy_table_snapshots(&fixture).await?;
    verify_ordinary_read_conflicts(&fixture).await?;
    verify_dirty_query_differences(&fixture).await?;
    verify_btree_redb_refresh(&rt, &fixture).await?;
    verify_live_final_values(&fixture.db, "live final").await?;

    expect_eq(
        "complete live root WAL append count",
        &fixture.logger.append_total_count(),
        &10usize,
    )?;
    expect_eq(
        "complete live manager produced count",
        &fixture.manager.produced_transaction_total(),
        &19usize,
    )?;
    expect_eq(
        "complete live manager consumed count",
        &fixture.manager.consumed_transaction_total(),
        &19usize,
    )?;
    expect_eq(
        "complete live manager active count",
        &fixture.manager.transaction_len(),
        &0usize,
    )?;

    wait_for_all_confirmations(&rt, &fixture.logger).await?;
    wait_for_btree_cache_zero(&rt, &fixture.db, "final confirmation").await?;
    verify_live_final_values(&fixture.db, "confirmed live final").await?;
    require(
        nonempty_bak_count(&root.join("root-wal"))? > 0,
        "confirmed live root WAL did not produce any nonempty .bak file",
    )
}

async fn phase_data_only(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
    label: &str,
) -> TestResult<()> {
    let fixture = build_database(&rt, &root).await?;
    expect_eq(
        &format!("{label} registered table count"),
        &fixture.db.table_size().await,
        &4usize,
    )?;
    expect_eq(
        &format!("{label} repair append count"),
        &fixture.logger.append_total_count(),
        &0usize,
    )?;
    expect_eq(
        &format!("{label} manager produced count"),
        &fixture.manager.produced_transaction_total(),
        &0usize,
    )?;
    expect_eq(
        &format!("{label} manager consumed count"),
        &fixture.manager.consumed_transaction_total(),
        &0usize,
    )?;
    expect_eq(
        &format!("{label} Btree overlay"),
        &fixture
            .db
            .table_cache_size(&Atom::from(BTREE_TABLE))
            .await,
        &Some(0u64),
    )?;
    verify_data_only_values(&fixture.db, label).await
}

async fn verify_batch_and_read_only_contract(fixture: &Fixture) -> TestResult<()> {
    let produced_before = fixture.manager.produced_transaction_total();
    let consumed_before = fixture.manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let max_key = maximum_key();
    let ignored = Some(value(99_999));
    let input = vec![
        TableKV::new(Atom::from(MISSING_TABLE), key("missing"), ignored.clone()),
        TableKV::new(Atom::from(MEMORY_TABLE), minimum_key(), ignored.clone()),
        TableKV::new(Atom::from(LOG_ORDERED_TABLE), max_key.clone(), ignored.clone()),
        TableKV::new(Atom::from(BTREE_TABLE), key("snapshot"), ignored.clone()),
        TableKV::new(Atom::from(MEMORY_TABLE), minimum_key(), ignored.clone()),
        TableKV::new(Atom::from(BTREE_TABLE), max_key, ignored),
    ];
    let expected = vec![
        None,
        Some(baseline_value(0, 0)),
        Some(baseline_value(1, 1)),
        Some(baseline_value(2, 2)),
        Some(baseline_value(0, 0)),
        Some(baseline_value(2, 1)),
    ];

    let ordinary = read_only_transaction(&fixture.db, "root-query read-only ordinary batch")?;
    assert_values("ordinary batch order and boundaries",
                  ordinary.query(input.clone()).await,
                  &expected)?;
    drop(ordinary);

    let dirty = read_only_transaction(&fixture.db, "root-query read-only dirty batch")?;
    assert_values("dirty batch order and boundaries",
                  dirty.dirty_query(input).await,
                  &expected)?;
    drop(dirty);

    expect_eq(
        "read-only queries produced transactions",
        &fixture.manager.produced_transaction_total(),
        &produced_before,
    )?;
    expect_eq(
        "read-only queries consumed transactions",
        &fixture.manager.consumed_transaction_total(),
        &consumed_before,
    )?;
    expect_eq(
        "read-only queries active transactions",
        &fixture.manager.transaction_len(),
        &0usize,
    )?;
    expect_eq(
        "read-only queries root WAL",
        &fixture.logger.append_total_count(),
        &append_before,
    )
}

async fn verify_empty_batch_protocol_neutrality(fixture: &Fixture) -> TestResult<()> {
    let produced_before = fixture.manager.produced_transaction_total();
    let consumed_before = fixture.manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let transaction = writable_transaction(&fixture.db, "root-query empty protocol-neutral")?;
    require(transaction.query(Vec::new()).await.is_empty(),
            "empty ordinary query did not return an empty Vec")?;
    require(transaction.dirty_query(Vec::new()).await.is_empty(),
            "empty dirty query did not return an empty Vec")?;
    expect_eq("empty query child count", &transaction.children_len(), &0usize)?;

    let prepare = transaction
        .prepare_with_version(Vec::new(), Vec::new())
        .await
        .map_err(|error| {
            format!("empty queries unexpectedly selected Ordinary: {error:?}")
        })?;
    require(prepare.is_empty(), "empty version prepare output was not empty")?;
    let receipt = transaction
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("empty version commit failed: {error:?}"))?;
    require(receipt.is_empty(), "empty version commit returned receipts")?;

    expect_eq(
        "empty query produced count",
        &fixture.manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        "empty query consumed count",
        &fixture.manager.consumed_transaction_total(),
        &(consumed_before + 1),
    )?;
    expect_eq(
        "empty query WAL count",
        &fixture.logger.append_total_count(),
        &append_before,
    )
}

async fn verify_writable_read_only_actions_close(fixture: &Fixture) -> TestResult<()> {
    let produced_before = fixture.manager.produced_transaction_total();
    let consumed_before = fixture.manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let transaction = writable_transaction(&fixture.db, "root-query writable pure read")?;
    let input: Vec<TableKV> = TABLE_CASES
        .iter()
        .map(|table| {
            TableKV::new(
                Atom::from(table.name),
                key("snapshot"),
                None,
            )
        })
        .collect();
    let expected: Vec<Option<Binary>> = TABLE_CASES
        .iter()
        .map(|table| Some(baseline_value(table.index, 2)))
        .collect();
    assert_values("writable pure read values",
                  transaction.query(input).await,
                  &expected)?;
    expect_eq("writable pure read child count", &transaction.children_len(), &3usize)?;
    expect_eq(
        "writable pure read persistence",
        &transaction.is_require_persistence(),
        &false,
    )?;
    let children: Vec<RealTransaction> = transaction.to_children().collect();
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing writable pure read failed: {error:?}"))?;
    require(prepare.is_empty(), "writable pure read unexpectedly produced WAL bytes")?;
    require(transaction.get_transaction_uid().is_some(),
            "writable pure read did not allocate a transaction UID")?;
    expect_eq(
        "writable pure read commit UID",
        &transaction.get_commit_uid(),
        &None,
    )?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing writable pure read failed: {error:?}"))?;
    expect_eq(
        "writable pure read root status",
        &transaction.get_status(),
        &Transaction2PcStatus::Commited,
    )?;
    for (index, child) in children.iter().enumerate() {
        expect_eq(
            &format!("writable pure read child {index} status"),
            &child.get_status(),
            &Transaction2PcStatus::Commited,
        )?;
    }
    expect_eq(
        "writable pure read produced count",
        &fixture.manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        "writable pure read consumed count",
        &fixture.manager.consumed_transaction_total(),
        &(consumed_before + 1),
    )?;
    expect_eq(
        "writable pure read WAL count",
        &fixture.logger.append_total_count(),
        &append_before,
    )
}

async fn verify_lazy_table_snapshots(fixture: &Fixture) -> TestResult<()> {
    let reader = read_only_transaction(&fixture.db, "root-query lazy table snapshot")?;
    assert_values(
        "lazy snapshot initial Memory",
        reader
            .query(vec![TableKV::new(
                Atom::from(MEMORY_TABLE),
                key("snapshot"),
                None,
            )])
            .await,
        &[Some(baseline_value(0, 2))],
    )?;

    let writer = writable_transaction(&fixture.db, "root-query lazy snapshot writer")?;
    writer
        .upsert(
            TABLE_CASES
                .iter()
                .map(|table| {
                    TableKV::new(
                        Atom::from(table.name),
                        key("snapshot"),
                        Some(snapshot_updated_value(table.index)),
                    )
                })
                .collect(),
        )
        .await
        .map_err(|error| format!("lazy snapshot writer upsert failed: {error:?}"))?;
    commit_ordinary(&writer, "lazy snapshot writer").await?;

    assert_values(
        "lazy snapshot per-table visibility",
        reader
            .query(vec![
                TableKV::new(Atom::from(MEMORY_TABLE), key("snapshot"), None),
                TableKV::new(Atom::from(LOG_ORDERED_TABLE), key("snapshot"), None),
                TableKV::new(Atom::from(BTREE_TABLE), key("snapshot"), None),
            ])
            .await,
        &[
            Some(baseline_value(0, 2)),
            Some(snapshot_updated_value(1)),
            Some(snapshot_updated_value(2)),
        ],
    )?;
    drop(reader);
    Ok(())
}

async fn verify_ordinary_read_conflicts(fixture: &Fixture) -> TestResult<()> {
    for table in TABLE_CASES {
        let label = format!("{} ordinary read conflict", table.label);
        let reader = writable_transaction(&fixture.db, &format!("{label} reader"))?;
        assert_values(
            &format!("{label} baseline"),
            reader
                .query(vec![TableKV::new(
                    Atom::from(table.name),
                    key("conflict"),
                    None,
                )])
                .await,
            &[Some(baseline_value(table.index, 3))],
        )?;
        let produced_before = fixture.manager.produced_transaction_total();
        let consumed_before = fixture.manager.consumed_transaction_total();
        let append_before = fixture.logger.append_total_count();

        let writer = writable_transaction(&fixture.db, &format!("{label} writer"))?;
        writer
            .upsert(vec![TableKV::new(
                Atom::from(table.name),
                key("conflict"),
                Some(conflict_updated_value(table.index)),
            )])
            .await
            .map_err(|error| format!("{label} writer upsert failed: {error:?}"))?;
        commit_ordinary(&writer, &format!("{label} writer")).await?;

        assert_read_conflict(&reader, table, &key("conflict"), &label).await?;
        reader
            .rollback_modified()
            .await
            .map_err(|error| format!("{label} rollback failed: {error:?}"))?;
        expect_eq(
            &format!("{label} rollback status"),
            &reader.get_status(),
            &Transaction2PcStatus::Rollbacked,
        )?;
        expect_eq(
            &format!("{label} produced count"),
            &fixture.manager.produced_transaction_total(),
            &(produced_before + 2),
        )?;
        expect_eq(
            &format!("{label} consumed count"),
            &fixture.manager.consumed_transaction_total(),
            &(consumed_before + 2),
        )?;
        expect_eq(
            &format!("{label} WAL count"),
            &fixture.logger.append_total_count(),
            &(append_before + 1),
        )?;
        expect_single_value(
            &fixture.db,
            table.name,
            key("conflict"),
            Some(&conflict_updated_value(table.index)),
            &format!("{label} final"),
        )
        .await?;
    }
    Ok(())
}

async fn verify_dirty_query_differences(fixture: &Fixture) -> TestResult<()> {
    for table in [TABLE_CASES[0], TABLE_CASES[1]] {
        let label = format!("{} dirty query without Read", table.label);
        let reader = writable_transaction(&fixture.db, &format!("{label} reader"))?;
        assert_values(
            &format!("{label} baseline"),
            reader
                .dirty_query(vec![TableKV::new(
                    Atom::from(table.name),
                    key("dirty"),
                    None,
                )])
                .await,
            &[Some(baseline_value(table.index, 4))],
        )?;
        let produced_before = fixture.manager.produced_transaction_total();
        let consumed_before = fixture.manager.consumed_transaction_total();
        let append_before = fixture.logger.append_total_count();
        let writer = writable_transaction(&fixture.db, &format!("{label} writer"))?;
        writer
            .upsert(vec![TableKV::new(
                Atom::from(table.name),
                key("dirty"),
                Some(dirty_updated_value(table.index)),
            )])
            .await
            .map_err(|error| format!("{label} writer upsert failed: {error:?}"))?;
        commit_ordinary(&writer, &format!("{label} writer")).await?;

        let prepare = reader
            .prepare_modified_conflicts()
            .await
            .map_err(|error| {
                format!("{label} unexpectedly established a Read conflict: {error:?}")
            })?;
        require(prepare.is_empty(), &format!("{label} produced WAL bytes"))?;
        reader
            .commit_modified(prepare)
            .await
            .map_err(|error| format!("{label} commit failed: {error:?}"))?;
        expect_eq(
            &format!("{label} produced count"),
            &fixture.manager.produced_transaction_total(),
            &(produced_before + 2),
        )?;
        expect_eq(
            &format!("{label} consumed count"),
            &fixture.manager.consumed_transaction_total(),
            &(consumed_before + 2),
        )?;
        expect_eq(
            &format!("{label} WAL count"),
            &fixture.logger.append_total_count(),
            &(append_before + 1),
        )?;
        expect_single_value(
            &fixture.db,
            table.name,
            key("dirty"),
            Some(&dirty_updated_value(table.index)),
            &format!("{label} final"),
        )
        .await?;
    }

    let table = TABLE_CASES[2];
    let label = "Btree dirty query retains ordinary Read";
    let reader = writable_transaction(&fixture.db, "Btree dirty query reader")?;
    assert_values(
        "Btree dirty baseline",
        reader
            .dirty_query(vec![TableKV::new(
                Atom::from(table.name),
                key("dirty"),
                None,
            )])
            .await,
        &[Some(baseline_value(table.index, 4))],
    )?;
    let produced_before = fixture.manager.produced_transaction_total();
    let consumed_before = fixture.manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let writer = writable_transaction(&fixture.db, "Btree dirty query writer")?;
    writer
        .upsert(vec![TableKV::new(
            Atom::from(table.name),
            key("dirty"),
            Some(dirty_updated_value(table.index)),
        )])
        .await
        .map_err(|error| format!("{label} writer upsert failed: {error:?}"))?;
    commit_ordinary(&writer, "Btree dirty query writer").await?;
    assert_read_conflict(&reader, table, &key("dirty"), label).await?;
    reader
        .rollback_modified()
        .await
        .map_err(|error| format!("{label} rollback failed: {error:?}"))?;
    expect_eq(
        "Btree dirty produced count",
        &fixture.manager.produced_transaction_total(),
        &(produced_before + 2),
    )?;
    expect_eq(
        "Btree dirty consumed count",
        &fixture.manager.consumed_transaction_total(),
        &(consumed_before + 2),
    )?;
    expect_eq(
        "Btree dirty WAL count",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;
    expect_single_value(
        &fixture.db,
        table.name,
        key("dirty"),
        Some(&dirty_updated_value(table.index)),
        "Btree dirty final",
    )
    .await
}

async fn verify_btree_redb_refresh(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
) -> TestResult<()> {
    let table = TABLE_CASES[2];
    let reader = writable_transaction(&fixture.db, "Btree redb refresh reader")?;
    assert_values(
        "Btree initial redb fallback",
        reader
            .query(vec![TableKV::new(
                Atom::from(table.name),
                key("redb"),
                None,
            )])
            .await,
        &[Some(baseline_value(table.index, 5))],
    )?;
    let produced_before = fixture.manager.produced_transaction_total();
    let consumed_before = fixture.manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();

    let writer = writable_transaction(&fixture.db, "Btree redb refresh writer")?;
    let mut actions = vec![TableKV::new(
        Atom::from(table.name),
        key("redb"),
        Some(redb_updated_value()),
    )];
    actions.extend(filler_entries(BTREE_TABLE, "redb-refresh", 0xD5));
    writer
        .upsert(actions)
        .await
        .map_err(|error| format!("Btree redb refresh upsert failed: {error:?}"))?;
    commit_ordinary(&writer, "Btree redb refresh writer").await?;
    wait_for_btree_cache_zero(rt, &fixture.db, "redb refresh").await?;

    assert_values(
        "Btree repeated redb fallback return value",
        reader
            .query(vec![TableKV::new(
                Atom::from(table.name),
                key("redb"),
                None,
            )])
            .await,
        &[Some(redb_updated_value())],
    )?;
    assert_read_conflict(&reader, table, &key("redb"), "Btree redb refresh").await?;
    reader
        .rollback_modified()
        .await
        .map_err(|error| format!("Btree redb refresh rollback failed: {error:?}"))?;
    expect_eq(
        "Btree redb refresh produced count",
        &fixture.manager.produced_transaction_total(),
        &(produced_before + 2),
    )?;
    expect_eq(
        "Btree redb refresh consumed count",
        &fixture.manager.consumed_transaction_total(),
        &(consumed_before + 2),
    )?;
    expect_eq(
        "Btree redb refresh WAL count",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )
}

async fn assert_read_conflict(
    transaction: &RealTransaction,
    table: TableCase,
    expected_key: &Binary,
    label: &str,
) -> TestResult<()> {
    let error = transaction
        .prepare_modified_conflicts()
        .await
        .expect_err("query reader must conflict after a committed same-key update");
    require(
        matches!(error.level(), ErrorLevel::Normal),
        &format!("{label}: read conflict was not Normal: {error:?}"),
    )?;
    let Some((actual_table, actual_key)) = error.conflicts() else {
        return Err(format!("{label}: read conflict did not expose table/key: {error:?}"));
    };
    expect_eq(
        &format!("{label}: conflict table"),
        &actual_table.as_str(),
        &table.name,
    )?;
    expect_binary(
        &format!("{label}: conflict key"),
        Some(actual_key),
        Some(expected_key),
    )?;
    expect_eq(
        &format!("{label}: failed status"),
        &transaction.get_status(),
        &Transaction2PcStatus::PrepareFailed,
    )
}

async fn seed_persistent_values(fixture: &Fixture) -> TestResult<()> {
    let transaction = writable_transaction(&fixture.db, "root-query persistent seed")?;
    let mut actions = Vec::new();
    for table in TABLE_CASES {
        actions.extend([
            TableKV::new(
                Atom::from(table.name),
                minimum_key(),
                Some(baseline_value(table.index, 0)),
            ),
            TableKV::new(
                Atom::from(table.name),
                maximum_key(),
                Some(baseline_value(table.index, 1)),
            ),
            TableKV::new(
                Atom::from(table.name),
                key("snapshot"),
                Some(baseline_value(table.index, 2)),
            ),
            TableKV::new(
                Atom::from(table.name),
                key("conflict"),
                Some(baseline_value(table.index, 3)),
            ),
            TableKV::new(
                Atom::from(table.name),
                key("dirty"),
                Some(baseline_value(table.index, 4)),
            ),
        ]);
    }
    actions.push(TableKV::new(
        Atom::from(BTREE_TABLE),
        key("redb"),
        Some(baseline_value(2, 5)),
    ));
    actions.extend(filler_entries(LOG_ORDERED_TABLE, "seed-log", 0xA1));
    actions.extend(filler_entries(BTREE_TABLE, "seed-btree", 0xB2));
    transaction
        .upsert(actions)
        .await
        .map_err(|error| format!("root-query persistent seed upsert failed: {error:?}"))?;
    commit_ordinary(&transaction, "root-query persistent seed").await
}

async fn verify_live_final_values(db: &RealDb, label: &str) -> TestResult<()> {
    for table in TABLE_CASES {
        let expected = vec![
            Some(baseline_value(table.index, 0)),
            Some(baseline_value(table.index, 1)),
            Some(snapshot_updated_value(table.index)),
            Some(conflict_updated_value(table.index)),
            Some(dirty_updated_value(table.index)),
        ];
        assert_values(
            &format!("{label} {}", table.label),
            query_values(
                db,
                table.name,
                vec![
                    minimum_key(),
                    maximum_key(),
                    key("snapshot"),
                    key("conflict"),
                    key("dirty"),
                ],
                &format!("{label} {} verifier", table.label),
            )
            .await?,
            &expected,
        )?;
    }
    expect_single_value(
        db,
        BTREE_TABLE,
        key("redb"),
        Some(&redb_updated_value()),
        &format!("{label} Btree redb"),
    )
    .await?;
    expect_single_value(
        db,
        LOG_ORDERED_TABLE,
        filler_key("seed-log", FILLER_COUNT - 1),
        Some(&filler_value(0xA1, FILLER_COUNT - 1)),
        &format!("{label} LogOrdered filler"),
    )
    .await?;
    expect_single_value(
        db,
        BTREE_TABLE,
        filler_key("redb-refresh", FILLER_COUNT - 1),
        Some(&filler_value(0xD5, FILLER_COUNT - 1)),
        &format!("{label} Btree refresh filler"),
    )
    .await
}

async fn verify_data_only_values(db: &RealDb, label: &str) -> TestResult<()> {
    let memory_values = query_values(
        db,
        MEMORY_TABLE,
        vec![
            minimum_key(),
            maximum_key(),
            key("snapshot"),
            key("conflict"),
            key("dirty"),
        ],
        &format!("{label} Memory verifier"),
    )
    .await?;
    assert_values(
        &format!("{label} volatile Memory"),
        memory_values,
        &[None, None, None, None, None],
    )?;

    for table in [TABLE_CASES[1], TABLE_CASES[2]] {
        assert_values(
            &format!("{label} persisted {}", table.label),
            query_values(
                db,
                table.name,
                vec![
                    minimum_key(),
                    maximum_key(),
                    key("snapshot"),
                    key("conflict"),
                    key("dirty"),
                ],
                &format!("{label} {} verifier", table.label),
            )
            .await?,
            &[
                Some(baseline_value(table.index, 0)),
                Some(baseline_value(table.index, 1)),
                Some(snapshot_updated_value(table.index)),
                Some(conflict_updated_value(table.index)),
                Some(dirty_updated_value(table.index)),
            ],
        )?;
    }
    expect_single_value(
        db,
        BTREE_TABLE,
        key("redb"),
        Some(&redb_updated_value()),
        &format!("{label} Btree redb"),
    )
    .await?;
    expect_single_value(
        db,
        LOG_ORDERED_TABLE,
        filler_key("seed-log", 0),
        Some(&filler_value(0xA1, 0)),
        &format!("{label} first LogOrdered filler"),
    )
    .await?;
    expect_single_value(
        db,
        LOG_ORDERED_TABLE,
        filler_key("seed-log", FILLER_COUNT - 1),
        Some(&filler_value(0xA1, FILLER_COUNT - 1)),
        &format!("{label} last LogOrdered filler"),
    )
    .await?;
    expect_single_value(
        db,
        BTREE_TABLE,
        filler_key("seed-btree", FILLER_COUNT - 1),
        Some(&filler_value(0xB2, FILLER_COUNT - 1)),
        &format!("{label} seed Btree filler"),
    )
    .await?;
    expect_single_value(
        db,
        BTREE_TABLE,
        filler_key("redb-refresh", FILLER_COUNT - 1),
        Some(&filler_value(0xD5, FILLER_COUNT - 1)),
        &format!("{label} refresh Btree filler"),
    )
    .await
}

async fn seed_concurrency_values(fixture: &Fixture) -> TestResult<()> {
    let transaction = writable_transaction(&fixture.db, "root-query concurrency seed")?;
    let actions = TABLE_CASES
        .iter()
        .flat_map(|table| {
            (0..CONCURRENT_KEYS).map(move |key_index| {
                TableKV::new(
                    Atom::from(table.name),
                    concurrent_key(key_index),
                    Some(concurrent_initial_value(table.index, key_index)),
                )
            })
        })
        .collect();
    transaction
        .upsert(actions)
        .await
        .map_err(|error| format!("root-query concurrency seed failed: {error:?}"))?;
    commit_ordinary(&transaction, "root-query concurrency seed").await
}

async fn run_concurrent_reader(
    db: &RealDb,
    rt: &MultiTaskRuntime<()>,
    reader_index: usize,
    ready: &AtomicUsize,
    start: &AtomicBool,
    active: &AtomicUsize,
) -> TestResult<()> {
    let transaction = read_only_transaction(
        db,
        &format!("root-query concurrent reader {reader_index}"),
    )?;
    let input = concurrent_query_input();
    let expected = concurrent_initial_results();
    assert_values(
        &format!("concurrent reader {reader_index} initial snapshot"),
        transaction.query(input.clone()).await,
        &expected,
    )?;
    active.fetch_add(1, Ordering::SeqCst);
    ready.fetch_add(1, Ordering::SeqCst);
    while !start.load(Ordering::SeqCst) {
        rt.timeout(0).await;
    }

    for iteration in 0..CONCURRENT_READ_LOOPS {
        assert_values(
            &format!("concurrent reader {reader_index} iteration {iteration}"),
            transaction.query(input.clone()).await,
            &expected,
        )?;
        if iteration % 8 == 0 {
            rt.timeout(0).await;
        }
    }
    active.fetch_sub(1, Ordering::SeqCst);
    drop(transaction);
    Ok(())
}

async fn verify_concurrent_final_values(db: &RealDb) -> TestResult<()> {
    let values = query_mixed_values(
        db,
        concurrent_query_input(),
        "root-query concurrency final verifier",
    )
    .await?;
    let expected = TABLE_CASES
        .iter()
        .flat_map(|table| {
            (0..CONCURRENT_KEYS)
                .map(move |key_index| Some(concurrent_updated_value(table.index, key_index)))
        })
        .collect::<Vec<_>>();
    assert_values("root-query concurrency final values", values, &expected)
}

fn concurrent_query_input() -> Vec<TableKV> {
    TABLE_CASES
        .iter()
        .flat_map(|table| {
            (0..CONCURRENT_KEYS).map(move |key_index| {
                TableKV::new(
                    Atom::from(table.name),
                    concurrent_key(key_index),
                    Some(value(0xFFFF)),
                )
            })
        })
        .collect()
}

fn concurrent_initial_results() -> Vec<Option<Binary>> {
    TABLE_CASES
        .iter()
        .flat_map(|table| {
            (0..CONCURRENT_KEYS)
                .map(move |key_index| Some(concurrent_initial_value(table.index, key_index)))
        })
        .collect()
}

async fn create_tables(fixture: &Fixture, memory_persistence: bool) -> TestResult<()> {
    let transaction = writable_transaction(&fixture.db, "root-query table DDL")?;
    transaction
        .create_table(
            Atom::from(MEMORY_TABLE),
            table_meta(KVDBTableType::MemOrdTab, memory_persistence),
            false,
        )
        .await
        .map_err(|error| format!("creating root-query Memory failed: {error}"))?;
    transaction
        .create_table_with_options(
            Atom::from(LOG_ORDERED_TABLE),
            table_meta(KVDBTableType::LogOrdTab, true),
            CreateTableOptions::LogOrdTab(
                64 * 1024 * 1024,
                1024 * 1024,
                1024 * 1024,
            ),
            false,
        )
        .await
        .map_err(|error| format!("creating root-query LogOrdered failed: {error}"))?;
    transaction
        .create_table_with_options(
            Atom::from(BTREE_TABLE),
            table_meta(KVDBTableType::BtreeOrdTab, true),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .map_err(|error| format!("creating root-query Btree failed: {error}"))?;
    commit_ordinary(&transaction, "root-query table DDL").await?;
    expect_eq(
        "root-query registered table count",
        &fixture.db.table_size().await,
        &4usize,
    )
}

async fn build_database(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<Fixture> {
    fs::create_dir_all(root)
        .map_err(|error| format!("creating root-query fixture root {root:?} failed: {error}"))?;
    let wal_path = root.join("root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building root-query logger at {wal_path:?} failed: {error}"))?;
    let manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger.clone(),
    );
    let db = KVDBManagerBuilder::new(rt.clone(), manager.clone(), root.join("database"))
        .startup(false)
        .await
        .map_err(|error| format!("starting root-query database failed: {error}"))?;
    Ok(Fixture {
        db,
        manager,
        logger,
    })
}

fn writable_transaction(db: &RealDb, source: &str) -> TestResult<RealTransaction> {
    db.transaction(Atom::from(source), true, 10_000, 10_000)
        .ok_or_else(|| format!("database rejected writable transaction {source}"))
}

fn read_only_transaction(db: &RealDb, source: &str) -> TestResult<RealTransaction> {
    db.transaction(Atom::from(source), false, 10_000, 10_000)
        .ok_or_else(|| format!("database rejected read-only transaction {source}"))
}

async fn commit_ordinary(transaction: &RealTransaction, label: &str) -> TestResult<()> {
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing {label} failed: {error:?}"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing {label} failed: {error:?}"))
}

async fn query_values(
    db: &RealDb,
    table: &str,
    keys: Vec<Binary>,
    label: &str,
) -> TestResult<Vec<Option<Binary>>> {
    query_mixed_values(
        db,
        keys.into_iter()
            .map(|key| TableKV::new(Atom::from(table), key, None))
            .collect(),
        label,
    )
    .await
}

async fn query_mixed_values(
    db: &RealDb,
    input: Vec<TableKV>,
    label: &str,
) -> TestResult<Vec<Option<Binary>>> {
    let transaction = read_only_transaction(db, label)?;
    let values = transaction.query(input).await;
    drop(transaction);
    Ok(values)
}

async fn expect_single_value(
    db: &RealDb,
    table: &str,
    key: Binary,
    expected: Option<&Binary>,
    label: &str,
) -> TestResult<()> {
    let mut values = query_values(db, table, vec![key], label).await?;
    if values.len() != 1 {
        return Err(format!(
            "{label}: expected one query slot, observed {}",
            values.len(),
        ));
    }
    expect_binary(label,
                  values.pop().expect("query length was checked").as_ref(),
                  expected)
}

async fn wait_for_btree_cache_zero(
    rt: &MultiTaskRuntime<()>,
    db: &RealDb,
    phase: &str,
) -> TestResult<()> {
    let deadline = Instant::now() + BTREE_DRAIN_TIMEOUT;
    loop {
        let current = db
            .table_cache_size(&Atom::from(BTREE_TABLE))
            .await
            .ok_or_else(|| format!("{phase}: Btree table disappeared"))?;
        if current == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{phase}: Btree overlay did not drain within {:?}, remaining={current}",
                BTREE_DRAIN_TIMEOUT,
            ));
        }
        rt.timeout(10).await;
    }
}

async fn wait_for_all_confirmations(
    rt: &MultiTaskRuntime<()>,
    logger: &CommitLogger,
) -> TestResult<()> {
    let expected = logger.append_total_count();
    let deadline = Instant::now() + OBSERVATION_TIMEOUT;
    loop {
        let confirmed = logger.confirm_total_count();
        let waiting = logger.waiting_confirm_count().await;
        if confirmed == expected && waiting == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "root-query confirmations did not close within {:?}: appended={expected}, confirmed={confirmed}, waiting={waiting}",
                OBSERVATION_TIMEOUT,
            ));
        }
        rt.timeout(10).await;
    }
}

fn filler_entries(table: &str, prefix: &str, marker: u8) -> Vec<TableKV> {
    (0..FILLER_COUNT)
        .map(|index| {
            TableKV::new(
                Atom::from(table),
                filler_key(prefix, index),
                Some(filler_value(marker, index)),
            )
        })
        .collect()
}

fn filler_key(prefix: &str, index: usize) -> Binary {
    encode_bin(format!("{prefix}-{index:04}").as_bytes())
}

fn filler_value(marker: u8, index: usize) -> Binary {
    let mut bytes = vec![marker; FILLER_VALUE_BYTES];
    bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
    encode_bin(&bytes)
}

fn minimum_key() -> Binary {
    // 空 payload 的 BON Bin 仍有 1 字节类型/长度头，因此它是最短的合法编码 Key，
    // 不属于数据库禁止的 `Binary::len() == 0` 空 Key。
    let key = encode_bin(&[]);
    assert_eq!(key.as_ref().len(), 1);
    key
}

fn maximum_key() -> Binary {
    // BON Bin 对 0x0100..=0xffff 字节 payload 使用 3 字节头；这里验证的是
    // `TableKV` 中完整编码 Key 的 u16::MAX 长度边界，而不是 payload 长度。
    let key = encode_bin(&vec![0xA5; u16::MAX as usize - 3]);
    assert_eq!(key.as_ref().len(), u16::MAX as usize);
    key
}

fn key(name: &str) -> Binary {
    encode_bin(name.as_bytes())
}

fn value(number: u64) -> Binary {
    encode_bin(&number.to_le_bytes())
}

fn baseline_value(table_index: usize, value_index: usize) -> Binary {
    value(1_000 + table_index as u64 * 100 + value_index as u64)
}

fn snapshot_updated_value(table_index: usize) -> Binary {
    value(2_000 + table_index as u64)
}

fn conflict_updated_value(table_index: usize) -> Binary {
    value(3_000 + table_index as u64)
}

fn dirty_updated_value(table_index: usize) -> Binary {
    value(4_000 + table_index as u64)
}

fn redb_updated_value() -> Binary {
    value(5_000)
}

fn concurrent_key(index: usize) -> Binary {
    encode_bin(format!("concurrent-{index:02}").as_bytes())
}

fn concurrent_initial_value(table_index: usize, key_index: usize) -> Binary {
    value(10_000 + table_index as u64 * 1_000 + key_index as u64)
}

fn concurrent_updated_value(table_index: usize, key_index: usize) -> Binary {
    value(20_000 + table_index as u64 * 1_000 + key_index as u64)
}

fn table_meta(table_type: KVDBTableType, persistence: bool) -> KVTableMeta {
    KVTableMeta::new(table_type, persistence, EnumType::Bin, EnumType::Bin)
}

fn encode_bin(bytes: &[u8]) -> Binary {
    let mut buffer = WriteBuffer::new();
    buffer.write_bin(bytes, 0..bytes.len());
    Binary::new(buffer.bytes)
}

fn assert_values(
    label: &str,
    actual: Vec<Option<Binary>>,
    expected: &[Option<Binary>],
) -> TestResult<()> {
    if actual.len() != expected.len() {
        return Err(format!(
            "{label}: expected {} slots, observed {}",
            expected.len(),
            actual.len(),
        ));
    }
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        expect_binary(
            &format!("{label} slot {index}"),
            actual.as_ref(),
            expected.as_ref(),
        )?;
    }
    Ok(())
}

fn expect_binary(
    label: &str,
    actual: Option<&Binary>,
    expected: Option<&Binary>,
) -> TestResult<()> {
    let equal = match (actual, expected) {
        (None, None) => true,
        (Some(actual), Some(expected)) => actual.as_ref() == expected.as_ref(),
        _ => false,
    };
    if equal {
        Ok(())
    } else {
        Err(format!(
            "{label}: expected {:?}, observed {:?}",
            expected.map(AsRef::<[u8]>::as_ref),
            actual.map(AsRef::<[u8]>::as_ref),
        ))
    }
}

fn expect_eq<T: std::fmt::Debug + PartialEq>(
    label: &str,
    actual: &T,
    expected: &T,
) -> TestResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{label}: expected {expected:?}, observed {actual:?}"))
    }
}

fn require(condition: bool, message: &str) -> TestResult<()> {
    if condition {
        Ok(())
    } else {
        Err(message.to_owned())
    }
}

fn nonempty_bak_count(path: &Path) -> TestResult<usize> {
    let mut count = 0usize;
    for entry in fs::read_dir(path)
        .map_err(|error| format!("reading root WAL directory {path:?} failed: {error}"))?
    {
        let entry = entry.map_err(|error| format!("reading root WAL entry failed: {error}"))?;
        if entry.path().extension().and_then(|value| value.to_str()) == Some("bak")
            && entry
                .metadata()
                .map_err(|error| format!("reading {:?} metadata failed: {error}", entry.path()))?
                .len()
                > 0
        {
            count += 1;
        }
    }
    Ok(count)
}

fn archive_root_wal(root: &Path) -> TestResult<()> {
    let wal = root.join("root-wal");
    let archive = root.join(ARCHIVED_WAL_DIR);
    require(wal.is_dir(), "root WAL directory was absent before data-only phase")?;
    require(!archive.exists(), "archived root WAL already existed")?;
    fs::rename(&wal, &archive)
        .map_err(|error| format!("archiving confirmed root WAL failed: {error}"))?;
    fs::create_dir_all(&wal)
        .map_err(|error| format!("creating empty data-only root WAL failed: {error}"))
}

fn run_phase_process(root: &Path, phase: &str, timeout: Duration) -> TestResult<ExitStatus> {
    let executable = env::current_exe()
        .map_err(|error| format!("resolving root-query test executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase)
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| format!("spawning root-query phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(status)
    } else {
        Err(format!("root-query phase {phase} exited with {status}"))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("polling root-query child failed: {error}"))?
        {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("root-query child exceeded {timeout:?}"));
        }
        thread::sleep(Duration::from_millis(20));
    }
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
    .map_err(|error| format!("spawning root-query future failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("root-query future exceeded {timeout:?}: {error}"))?
}

fn unique_temp_root(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after UNIX_EPOCH")
        .as_nanos();
    env::temp_dir().join(format!(
        "pi_db_root_query_{label}_{}_{}",
        std::process::id(),
        nanos,
    ))
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(label: &str) -> TestResult<Self> {
        let path = unique_temp_root(label);
        fs::create_dir_all(&path)
            .map_err(|error| format!("creating root-query temporary root failed: {error}"))?;
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

struct Fixture {
    db: RealDb,
    manager: RealManager,
    logger: CommitLogger,
}

#[derive(Clone, Copy)]
struct TableCase {
    label: &'static str,
    name: &'static str,
    index: usize,
}

impl TableCase {
    const fn new(label: &'static str, name: &'static str, index: usize) -> Self {
        Self { label, name, index }
    }
}
