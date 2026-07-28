//! 根事务 `upsert/dirty_upsert` 的真实批次、冲突、WAL、确认和恢复契约专项。
//!
//! 本 target 使用真实 4-worker runtime、`Transaction2PcManager`、`CommitLogger`、
//! Memory/LogOrdered/Btree、redb、表日志和文件系统。恢复矩阵在独立进程中依次完成 setup、
//! 生产 `try_repair`、移走已确认根 WAL 后的两次 data-only 冷启动；最终数据状态与 `.bak`
//! 同时作为硬门禁。冲突矩阵使用互相独立的根事务，严格验证当前 dirty 写的逐表差异。
//!
//! 测试只使用可写根、规范非空 BON Key 和非空 Value；普通与 dirty 操作族从不在同一非空
//! 事务中混用。正式契约见
//! `docs/ROOT_UPSERT_CONTRACT.md#root-upsert-contract-index`。

mod key_version_support;

use std::{
    env,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use pi_async_rt::rt::{
    multi_thread::MultiTaskRuntime,
    AsyncRuntime,
};
use pi_async_transaction::{
    manager_2pc::Transaction2PcStatus,
    AsyncCommitLog,
    ErrorLevel,
    Transaction2Pc,
    TransactionTree,
    UnitTransaction,
};
use pi_atom::Atom;
use pi_bon::WriteBuffer;
use pi_db::{
    db::KVDBTransaction,
    tables::{
        mem_ord_table::MemoryOrderedTable,
        KVTable,
        TableKV,
    },
    utils::CreateTableOptions,
    Binary,
    KVDBTableType,
    KVTableMeta,
    KVTableTrError,
};
use pi_sinfo::EnumType;
use pi_store::commit_logger::CommitLogger;

use key_version_support::{
    Fixture,
    RealTransaction,
    TestResult,
    build_database,
    expect_binary,
    expect_eq,
    query_ordinary,
    read_only_transaction,
    run_on_runtime,
    writable_transaction,
};

type CodecTable = MemoryOrderedTable<usize, CommitLogger>;

const TEST_NAME: &str = "test_root_upsert_contract_recovery";
const PHASE_ENV: &str = "PI_DB_ROOT_UPSERT_PHASE";
const ROOT_ENV: &str = "PI_DB_ROOT_UPSERT_ROOT";
const ARCHIVED_WAL_DIR: &str = "confirmed-root-wal";

const MEMORY_TABLE: &str = "root_upsert_memory";
const VOLATILE_MEMORY_TABLE: &str = "root_upsert_memory_volatile";
const LOG_ORDERED_TABLE: &str = "root_upsert_log_ordered";
const BTREE_TABLE: &str = "root_upsert_btree";

const ROOT_TID_BYTES: usize = 16;
const PROCESS_TIMEOUT: Duration = Duration::from_secs(150);
const SETUP_TIMEOUT: Duration = Duration::from_secs(45);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(120);
const DATA_ONLY_TIMEOUT: Duration = Duration::from_secs(30);
const CONFLICT_TIMEOUT: Duration = Duration::from_secs(60);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(90);

#[test]
fn test_root_upsert_contract_recovery() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV)
                .expect("root-upsert child phase must receive its root path"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("root-upsert phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root("recovery");
    fs::create_dir_all(&root).expect("creating root-upsert recovery root must succeed");
    for phase in ["setup", "recover", "data-only", "data-only-again"] {
        if let Err(error) = run_phase_process(&root, phase, PROCESS_TIMEOUT) {
            panic!(
                "root-upsert recovery failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root,
            );
        }
    }
    fs::remove_dir_all(&root).expect("cleaning root-upsert recovery root must succeed");
}

#[test]
fn test_root_dirty_upsert_conflict_matrix() {
    let root = TempRoot::new("dirty-conflicts")
        .expect("creating dirty-upsert conflict root must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(CONFLICT_TIMEOUT, move |rt| async move {
        let fixture = build_database(
            &rt,
            &root_path,
            Duration::ZERO,
            Duration::ZERO,
        ).await?;
        create_tables(&fixture).await?;

        for case in [
            DirtyConflictCase::new("persistent Memory", MEMORY_TABLE, false, true),
            DirtyConflictCase::new("volatile Memory", VOLATILE_MEMORY_TABLE, true, false),
            DirtyConflictCase::new("LogOrdered", LOG_ORDERED_TABLE, false, true),
            DirtyConflictCase::new("Btree", BTREE_TABLE, true, true),
        ] {
            verify_dirty_conflict_case(&fixture, case).await?;
        }

        expect_eq(
            "dirty conflict matrix manager balance",
            &fixture.tr_manager.produced_transaction_total(),
            &fixture.tr_manager.consumed_transaction_total(),
        )?;
        expect_eq(
            "dirty conflict matrix active roots",
            &fixture.tr_manager.transaction_len(),
            &0usize,
        )
    })
    .unwrap_or_else(|error| panic!("root dirty-upsert conflict matrix failed: {error}"));
}

fn run_child_phase(phase: &str, root: &Path) -> TestResult<()> {
    match phase {
        "setup" => {
            let root = root.to_path_buf();
            run_on_runtime(SETUP_TIMEOUT, move |rt| async move {
                phase_setup(rt, root).await
            })
        },
        "recover" => {
            let root = root.to_path_buf();
            run_on_runtime(RECOVERY_TIMEOUT, move |rt| async move {
                phase_recover(rt, root).await
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
        other => Err(format!("unknown root-upsert phase: {other}")),
    }
}

async fn phase_setup(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let fixture = build_database(
        &rt,
        &root,
        Duration::ZERO,
        Duration::ZERO,
    ).await?;
    create_tables(&fixture).await?;
    assert_table_definitions(&fixture, "setup").await?;
    expect_eq(
        "setup DDL WAL append count",
        &fixture.logger.append_total_count(),
        &1usize,
    )?;

    verify_empty_and_none_batches(&fixture).await?;

    let ordinary = ordinary_plan();
    execute_committed_plan(&fixture, &ordinary, ActionMode::Ordinary).await?;
    let dirty = dirty_plan();
    execute_committed_plan(&fixture, &dirty, ActionMode::Dirty).await?;

    let expected = combined_final_entries();
    assert_live_values(&fixture, &expected, "setup committed").await?;
    expect_eq(
        "setup complete WAL append count",
        &fixture.logger.append_total_count(),
        &3usize,
    )?;
    expect_eq(
        "setup complete WAL confirm count",
        &fixture.logger.confirm_total_count(),
        &0usize,
    )?;
    expect_eq(
        "setup complete WAL waiting count",
        &fixture.logger.waiting_confirm_count().await,
        &3usize,
    )?;
    expect_eq(
        "setup complete manager produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &6usize,
    )?;
    expect_eq(
        "setup complete manager consumed count",
        &fixture.tr_manager.consumed_transaction_total(),
        &6usize,
    )?;
    expect_eq(
        "setup complete manager active roots",
        &fixture.tr_manager.transaction_len(),
        &0usize,
    )?;
    expect_eq(
        "setup nonempty .bak count",
        &nonempty_bak_count(&root.join("root-wal"))?,
        &0usize,
    )
}

async fn phase_recover(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let wal_path = root.join("root-wal");
    let bak_before = nonempty_bak_count(&wal_path)?;
    let fixture = build_database(
        &rt,
        &root,
        Duration::ZERO,
        Duration::ZERO,
    ).await?;
    assert_table_definitions(&fixture, "recovered").await?;
    let expected = combined_final_entries();
    assert_live_values(&fixture, &expected, "replayed before confirmation").await?;

    wait_for_wal_state(
        &rt,
        &fixture,
        3,
        3,
        0,
        CONFIRM_TIMEOUT,
        "recovery",
    ).await?;
    assert_live_values(&fixture, &expected, "replayed after confirmation").await?;
    expect_eq(
        "recovery manager produced/consumed balance",
        &fixture.tr_manager.produced_transaction_total(),
        &fixture.tr_manager.consumed_transaction_total(),
    )?;
    expect_eq(
        "recovery manager active roots",
        &fixture.tr_manager.transaction_len(),
        &0usize,
    )?;
    expect_eq(
        "recovery Btree overlay drained",
        &fixture.db.table_cache_size(&Atom::from(BTREE_TABLE)).await,
        &Some(0u64),
    )?;

    let bak_after = nonempty_bak_count(&wal_path)?;
    if bak_after <= bak_before {
        return Err(format!(
            "root-upsert recovery did not add a confirmed .bak checkpoint: before={bak_before}, after={bak_after}",
        ));
    }
    let active = active_file_sizes(&wal_path)?;
    if active.iter().any(|(_, len)| *len > 0) {
        return Err(format!(
            "nonempty active root WAL remains after root-upsert recovery: {active:?}",
        ));
    }
    Ok(())
}

async fn phase_data_only(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
    label: &str,
) -> TestResult<()> {
    let archived_wal = root.join(ARCHIVED_WAL_DIR);
    if !archived_wal.exists() || nonempty_bak_count(&archived_wal)? == 0 {
        return Err(format!(
            "{label}: archived root WAL has no nonempty confirmed .bak checkpoint",
        ));
    }
    if label == "second data-only" {
        let active = active_file_sizes(&root.join("root-wal"))?;
        if active.iter().any(|(_, len)| *len > 0) {
            return Err(format!(
                "{label}: first data-only start produced nonempty root WAL: {active:?}",
            ));
        }
    }

    let fixture = build_database(
        &rt,
        &root,
        Duration::ZERO,
        Duration::ZERO,
    ).await?;
    assert_table_definitions(&fixture, label).await?;
    for entry in combined_final_entries() {
        let actual = query_ordinary(
            &fixture.db,
            entry.table,
            entry.key.clone(),
            &format!("{label} query for {}", entry.table),
        ).await?;
        if entry.table == MEMORY_TABLE || entry.table == VOLATILE_MEMORY_TABLE {
            expect_binary(
                &format!("{label} volatile data for {}", entry.table),
                actual.as_ref(),
                None,
            )?;
        } else {
            expect_binary(
                &format!("{label} persisted data for {}", entry.table),
                actual.as_ref(),
                Some(&entry.value),
            )?;
        }
    }
    for (table, key) in none_only_keys() {
        let actual = query_ordinary(
            &fixture.db,
            table,
            key,
            &format!("{label} None-only query for {table}"),
        ).await?;
        expect_binary(
            &format!("{label} None-only value for {table}"),
            actual.as_ref(),
            None,
        )?;
    }
    expect_eq(
        &format!("{label} WAL append count"),
        &fixture.logger.append_total_count(),
        &0usize,
    )?;
    expect_eq(
        &format!("{label} WAL confirm count"),
        &fixture.logger.confirm_total_count(),
        &0usize,
    )?;
    expect_eq(
        &format!("{label} WAL waiting count"),
        &fixture.logger.waiting_confirm_count().await,
        &0usize,
    )?;
    expect_eq(
        &format!("{label} manager produced count"),
        &fixture.tr_manager.produced_transaction_total(),
        &0usize,
    )?;
    expect_eq(
        &format!("{label} manager consumed count"),
        &fixture.tr_manager.consumed_transaction_total(),
        &0usize,
    )?;
    expect_eq(
        &format!("{label} Btree overlay"),
        &fixture.db.table_cache_size(&Atom::from(BTREE_TABLE)).await,
        &Some(0u64),
    )
}

async fn create_tables(fixture: &Fixture) -> TestResult<()> {
    let transaction = writable_transaction(&fixture.db, "root-upsert table DDL")?;
    transaction
        .create_table(
            Atom::from(MEMORY_TABLE),
            table_meta(KVDBTableType::MemOrdTab, true),
            false,
        )
        .await
        .map_err(|error| format!("creating persistent Memory table failed: {error}"))?;
    transaction
        .create_table(
            Atom::from(VOLATILE_MEMORY_TABLE),
            table_meta(KVDBTableType::MemOrdTab, false),
            false,
        )
        .await
        .map_err(|error| format!("creating volatile Memory table failed: {error}"))?;
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
        .map_err(|error| format!("creating LogOrdered table failed: {error}"))?;
    transaction
        .create_table_with_options(
            Atom::from(BTREE_TABLE),
            table_meta(KVDBTableType::BtreeOrdTab, true),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .map_err(|error| format!("creating Btree table failed: {error}"))?;
    commit_ordinary(&transaction, "root-upsert table DDL").await
}

async fn verify_empty_and_none_batches(fixture: &Fixture) -> TestResult<()> {
    let append_before = fixture.logger.append_total_count();
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();

    let empty = writable_transaction(&fixture.db, "root-upsert empty protocol proof")?;
    empty
        .upsert(Vec::new())
        .await
        .map_err(|error| format!("empty ordinary upsert failed: {error:?}"))?;
    empty
        .dirty_upsert(Vec::new())
        .await
        .map_err(|error| format!("empty dirty upsert failed: {error:?}"))?;
    expect_eq("empty root child count", &empty.children_len(), &0usize)?;
    expect_eq(
        "empty root persistence",
        &empty.is_require_persistence(),
        &false,
    )?;
    let version_prepare = empty
        .prepare_with_version(Vec::new(), Vec::new())
        .await
        .map_err(|error| {
            format!("empty upsert batches unexpectedly selected Ordinary: {error:?}")
        })?;
    expect_eq(
        "empty root version prepare bytes",
        &version_prepare.len(),
        &0usize,
    )?;
    let receipt = empty
        .commit_with_version(version_prepare)
        .await
        .map_err(|error| format!("committing empty version proof failed: {error:?}"))?;
    expect_eq("empty root version receipt", &receipt.len(), &0usize)?;

    for mode in [ActionMode::Ordinary, ActionMode::Dirty] {
        let (table, key) = match mode {
            ActionMode::Ordinary => (MEMORY_TABLE, key("ordinary-none-only")),
            ActionMode::Dirty => (LOG_ORDERED_TABLE, key("dirty-none-only")),
        };
        let transaction = writable_transaction(
            &fixture.db,
            &format!("root-upsert {} None-only root", mode.label()),
        )?;
        let input = vec![TableKV::new(Atom::from(table), key.clone(), None)];
        mode.upsert(&transaction, input).await?;
        expect_eq(
            &format!("{} None-only child count", mode.label()),
            &transaction.children_len(),
            &1usize,
        )?;
        expect_eq(
            &format!("{} None-only persistence", mode.label()),
            &transaction.is_require_persistence(),
            &true,
        )?;
        let prepare = transaction
            .prepare_modified_conflicts()
            .await
            .map_err(|error| {
                format!("preparing {} None-only root failed: {error:?}", mode.label())
            })?;
        expect_eq(
            &format!("{} None-only prepare bytes", mode.label()),
            &prepare.len(),
            &0usize,
        )?;
        transaction
            .commit_modified(prepare)
            .await
            .map_err(|error| {
                format!("committing {} None-only root failed: {error:?}", mode.label())
            })?;
        expect_eq(
            &format!("{} None-only root status", mode.label()),
            &transaction.get_status(),
            &Transaction2PcStatus::Commited,
        )?;
        let observed = query_ordinary(
            &fixture.db,
            table,
            key,
            &format!("{} None-only observer", mode.label()),
        ).await?;
        expect_binary(
            &format!("{} None-only user value", mode.label()),
            observed.as_ref(),
            None,
        )?;
    }

    expect_eq(
        "empty/None roots produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 3),
    )?;
    expect_eq(
        "empty/None roots consumed count",
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 3),
    )?;
    expect_eq(
        "empty/None roots active count",
        &fixture.tr_manager.transaction_len(),
        &0usize,
    )?;
    expect_eq(
        "empty/None roots WAL append count",
        &fixture.logger.append_total_count(),
        &append_before,
    )
}

async fn execute_committed_plan(
    fixture: &Fixture,
    plan: &ActionPlan,
    mode: ActionMode,
) -> TestResult<()> {
    let transaction = writable_transaction(
        &fixture.db,
        &format!("root-upsert {} business root", mode.label()),
    )?;
    expect_eq(
        &format!("{} initial status", mode.label()),
        &transaction.get_status(),
        &Transaction2PcStatus::Start,
    )?;
    expect_eq(
        &format!("{} initial transaction UID", mode.label()),
        &transaction.get_transaction_uid(),
        &None,
    )?;

    mode.upsert(&transaction, plan.input.clone()).await?;
    expect_eq(
        &format!("{} root persistence", mode.label()),
        &transaction.is_require_persistence(),
        &true,
    )?;
    expect_eq(
        &format!("{} direct child count", mode.label()),
        &transaction.children_len(),
        &plan.child_order.len(),
    )?;
    let children: Vec<RealTransaction> = transaction.to_children().collect();
    assert_child_order(&children, &plan.child_order, mode.label())?;

    let private = mode.query(
        &transaction,
        plan.final_entries
            .iter()
            .map(|entry| {
                TableKV::new(
                    Atom::from(entry.table),
                    entry.key.clone(),
                    None,
                )
            })
            .collect(),
    ).await;
    assert_query_values(
        &format!("{} private final values", mode.label()),
        &private,
        &plan.final_entries,
    )?;

    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing {} business root failed: {error:?}", mode.label()))?;
    let transaction_uid = transaction
        .get_transaction_uid()
        .ok_or_else(|| format!("{} prepare did not allocate TID", mode.label()))?;
    let commit_uid = transaction
        .get_commit_uid()
        .ok_or_else(|| format!("{} prepare did not allocate CID", mode.label()))?;
    expect_eq(
        &format!("{} prepared root status", mode.label()),
        &transaction.get_status(),
        &Transaction2PcStatus::Prepared,
    )?;
    assert_shared_child_identity(
        &children,
        &transaction_uid,
        &commit_uid,
        mode.label(),
        Transaction2PcStatus::Prepared,
    )?;
    assert_prepare_output(
        &prepare,
        &transaction_uid,
        &plan.child_order,
        &plan.final_entries,
        mode.label(),
    )?;

    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing {} business root failed: {error:?}", mode.label()))?;
    expect_eq(
        &format!("{} committed root status", mode.label()),
        &transaction.get_status(),
        &Transaction2PcStatus::Commited,
    )?;
    assert_shared_child_identity(
        &children,
        &transaction_uid,
        &commit_uid,
        mode.label(),
        Transaction2PcStatus::Commited,
    )?;
    expect_eq(
        &format!("{} manager produced increment", mode.label()),
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        &format!("{} manager consumed increment", mode.label()),
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 1),
    )?;
    expect_eq(
        &format!("{} manager active roots", mode.label()),
        &fixture.tr_manager.transaction_len(),
        &0usize,
    )?;
    expect_eq(
        &format!("{} root WAL append increment", mode.label()),
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;
    assert_live_values(fixture, &plan.final_entries, mode.label()).await
}

async fn verify_dirty_conflict_case(
    fixture: &Fixture,
    case: DirtyConflictCase,
) -> TestResult<()> {
    let key = key(&format!("dirty-conflict-{}", case.table));
    let seed = value(10);
    let winner_value = value(20);
    let stale_value = value(30);
    let retry_value = value(40);

    let seed_transaction = writable_transaction(
        &fixture.db,
        &format!("{} dirty conflict seed", case.label),
    )?;
    seed_transaction
        .upsert(vec![TableKV::new(
            Atom::from(case.table),
            key.clone(),
            Some(seed),
        )])
        .await
        .map_err(|error| format!("{} seed upsert failed: {error:?}", case.label))?;
    commit_ordinary(&seed_transaction, &format!("{} seed", case.label)).await?;

    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();

    let stale = writable_transaction(
        &fixture.db,
        &format!("{} stale dirty writer", case.label),
    )?;
    stale
        .dirty_upsert(vec![TableKV::new(
            Atom::from(case.table),
            key.clone(),
            Some(stale_value.clone()),
        )])
        .await
        .map_err(|error| format!("{} stale dirty upsert failed: {error:?}", case.label))?;
    let stale_children: Vec<RealTransaction> = stale.to_children().collect();
    expect_eq(
        &format!("{} stale child count", case.label),
        &stale_children.len(),
        &1usize,
    )?;

    let winner = writable_transaction(
        &fixture.db,
        &format!("{} concurrent winner", case.label),
    )?;
    winner
        .upsert(vec![TableKV::new(
            Atom::from(case.table),
            key.clone(),
            Some(winner_value.clone()),
        )])
        .await
        .map_err(|error| format!("{} winner upsert failed: {error:?}", case.label))?;
    commit_ordinary(&winner, &format!("{} winner", case.label)).await?;

    if case.must_conflict {
        let error = stale
            .prepare_modified_conflicts()
            .await
            .expect_err("stale dirty writer must return the frozen conflict result");
        assert_conflict(&error, case.table, &key, case.label)?;
        expect_eq(
            &format!("{} stale prepare-failed status", case.label),
            &stale.get_status(),
            &Transaction2PcStatus::PrepareFailed,
        )?;
        expect_eq(
            &format!("{} rejected stale root active before rollback", case.label),
            &fixture.tr_manager.transaction_len(),
            &1usize,
        )?;
        expect_eq(
            &format!("{} rejected stale root wrote no WAL", case.label),
            &fixture.logger.append_total_count(),
            &(append_before + usize::from(case.persistent)),
        )?;
        stale
            .rollback_modified()
            .await
            .map_err(|error| format!("{} stale rollback failed: {error:?}", case.label))?;
        expect_eq(
            &format!("{} stale rollback status", case.label),
            &stale.get_status(),
            &Transaction2PcStatus::Rollbacked,
        )?;
        expect_binary(
            &format!("{} winner value after rollback", case.label),
            query_ordinary(
                &fixture.db,
                case.table,
                key.clone(),
                &format!("{} rollback observer", case.label),
            ).await?.as_ref(),
            Some(&winner_value),
        )?;

        let retry = writable_transaction(
            &fixture.db,
            &format!("{} post-rollback retry", case.label),
        )?;
        retry
            .upsert(vec![TableKV::new(
                Atom::from(case.table),
                key.clone(),
                Some(retry_value.clone()),
            )])
            .await
            .map_err(|error| format!("{} retry upsert failed: {error:?}", case.label))?;
        commit_ordinary(&retry, &format!("{} retry", case.label)).await?;
        expect_binary(
            &format!("{} retry final value", case.label),
            query_ordinary(
                &fixture.db,
                case.table,
                key,
                &format!("{} retry observer", case.label),
            ).await?.as_ref(),
            Some(&retry_value),
        )?;
        expect_eq(
            &format!("{} produced transaction count", case.label),
            &fixture.tr_manager.produced_transaction_total(),
            &(produced_before + 3),
        )?;
        expect_eq(
            &format!("{} consumed transaction count", case.label),
            &fixture.tr_manager.consumed_transaction_total(),
            &(consumed_before + 3),
        )?;
        expect_eq(
            &format!("{} final WAL append count", case.label),
            &fixture.logger.append_total_count(),
            &(append_before + 2 * usize::from(case.persistent)),
        )?;
    } else {
        let prepare = stale
            .prepare_modified_conflicts()
            .await
            .map_err(|error| {
                format!("{} stale dirty prepare unexpectedly conflicted: {error:?}", case.label)
            })?;
        if case.persistent && prepare.len() <= ROOT_TID_BYTES {
            return Err(format!(
                "{} stale dirty prepare omitted its persistent write payload",
                case.label,
            ));
        }
        stale
            .commit_modified(prepare)
            .await
            .map_err(|error| format!("{} stale dirty commit failed: {error:?}", case.label))?;
        expect_binary(
            &format!("{} stale dirty final value", case.label),
            query_ordinary(
                &fixture.db,
                case.table,
                key,
                &format!("{} stale dirty observer", case.label),
            ).await?.as_ref(),
            Some(&stale_value),
        )?;
        expect_eq(
            &format!("{} produced transaction count", case.label),
            &fixture.tr_manager.produced_transaction_total(),
            &(produced_before + 2),
        )?;
        expect_eq(
            &format!("{} consumed transaction count", case.label),
            &fixture.tr_manager.consumed_transaction_total(),
            &(consumed_before + 2),
        )?;
        expect_eq(
            &format!("{} final WAL append count", case.label),
            &fixture.logger.append_total_count(),
            &(append_before + 2 * usize::from(case.persistent)),
        )?;
    }

    expect_eq(
        &format!("{} final active roots", case.label),
        &fixture.tr_manager.transaction_len(),
        &0usize,
    )
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

fn assert_conflict(
    error: &KVTableTrError,
    expected_table: &str,
    expected_key: &Binary,
    label: &str,
) -> TestResult<()> {
    if !matches!(error.level(), ErrorLevel::Normal) {
        return Err(format!(
            "{label}: conflict returned non-Normal level {:?}",
            error.level(),
        ));
    }
    if !error.is_conflicts() {
        return Err(format!("{label}: expected conflict error, observed {error:?}"));
    }
    let (table, key) = error
        .conflicts()
        .ok_or_else(|| format!("{label}: conflict error omitted table/key"))?;
    expect_eq(
        &format!("{label} conflict table"),
        &table.as_str(),
        &expected_table,
    )?;
    if key.as_ref() != expected_key.as_ref() {
        return Err(format!(
            "{label}: conflict key mismatch, expected_len={}, observed_len={}",
            expected_key.len(),
            key.len(),
        ));
    }
    Ok(())
}

fn assert_child_order(
    children: &[RealTransaction],
    expected: &[&str],
    label: &str,
) -> TestResult<()> {
    let observed = children
        .iter()
        .map(child_table_name)
        .collect::<Vec<_>>();
    expect_eq(
        &format!("{label} first-touch child order"),
        &observed,
        &expected.to_vec(),
    )?;
    for (index, child) in children.iter().enumerate() {
        expect_eq(
            &format!("{label} child {index} is unit"),
            &child.is_unit(),
            &true,
        )?;
        expect_eq(
            &format!("{label} child {index} is not tree"),
            &child.is_tree(),
            &false,
        )?;
    }
    Ok(())
}

fn child_table_name(child: &RealTransaction) -> &'static str {
    match child {
        KVDBTransaction::MemOrdTabTr(_) => MEMORY_TABLE,
        KVDBTransaction::LogOrdTabTr(_) => LOG_ORDERED_TABLE,
        KVDBTransaction::BtreeOrdTabTr(_) => BTREE_TABLE,
        KVDBTransaction::MetaTabTr(_) => ".tables_meta",
        KVDBTransaction::LogWTabTr(_) => "LogWrite",
        KVDBTransaction::RootTr(_) => "Root",
    }
}

fn assert_shared_child_identity(
    children: &[RealTransaction],
    transaction_uid: &pi_guid::Guid,
    commit_uid: &pi_guid::Guid,
    label: &str,
    status: Transaction2PcStatus,
) -> TestResult<()> {
    for (index, child) in children.iter().enumerate() {
        expect_eq(
            &format!("{label} child {index} TID"),
            &child.get_transaction_uid(),
            &Some(transaction_uid.clone()),
        )?;
        expect_eq(
            &format!("{label} child {index} CID"),
            &child.get_commit_uid(),
            &Some(commit_uid.clone()),
        )?;
        expect_eq(
            &format!("{label} child {index} status"),
            &child.get_status(),
            &status,
        )?;
    }
    Ok(())
}

fn assert_prepare_output(
    output: &Vec<u8>,
    transaction_uid: &pi_guid::Guid,
    expected_order: &[&str],
    expected_entries: &[ExpectedEntry],
    label: &str,
) -> TestResult<()> {
    if output.len() <= ROOT_TID_BYTES {
        return Err(format!(
            "{label}: prepare output contains no table action, len={}",
            output.len(),
        ));
    }
    expect_eq(
        &format!("{label} WAL TID bytes"),
        &&output[..ROOT_TID_BYTES],
        &&transaction_uid.0.to_le_bytes()[..],
    )?;

    let mut offset = ROOT_TID_BYTES;
    let mut segments = Vec::new();
    while offset < output.len() {
        let (table, count, actions_offset) =
            <CodecTable as KVTable>::get_init_table_prepare_output(output, offset);
        let (actions, next_offset) =
            <CodecTable as KVTable>::get_all_key_value_from_table_prepare_output(
                output,
                &table,
                count,
                actions_offset,
            );
        if next_offset <= offset || next_offset > output.len() {
            return Err(format!(
                "{label}: invalid decoded WAL offset {next_offset} from {offset}",
            ));
        }
        segments.push((table, actions));
        offset = next_offset;
    }
    expect_eq(
        &format!("{label} final WAL offset"),
        &offset,
        &output.len(),
    )?;
    let observed_order = segments
        .iter()
        .map(|(table, _)| table.as_str())
        .collect::<Vec<_>>();
    expect_eq(
        &format!("{label} WAL table order"),
        &observed_order,
        &expected_order.to_vec(),
    )?;

    let expected_total = expected_entries.len();
    let observed_total = segments
        .iter()
        .map(|(_, actions)| actions.len())
        .sum::<usize>();
    expect_eq(
        &format!("{label} WAL final action count"),
        &observed_total,
        &expected_total,
    )?;
    for (table, actions) in segments {
        let expected = expected_entries
            .iter()
            .filter(|entry| entry.table == table.as_str())
            .collect::<Vec<_>>();
        expect_eq(
            &format!("{label} {} action count", table.as_str()),
            &actions.len(),
            &expected.len(),
        )?;
        for entry in expected {
            let matches = actions
                .iter()
                .filter(|action| {
                    action.key.as_ref() == entry.key.as_ref()
                        && action.value.as_ref().map(AsRef::<[u8]>::as_ref)
                            == Some(entry.value.as_ref())
                })
                .count();
            expect_eq(
                &format!(
                    "{label} exact final action {} key_len={}",
                    table.as_str(),
                    entry.key.len(),
                ),
                &matches,
                &1usize,
            )?;
        }
    }
    Ok(())
}

async fn assert_table_definitions(fixture: &Fixture, label: &str) -> TestResult<()> {
    expect_eq(
        &format!("{label} registered table count"),
        &fixture.db.table_size().await,
        &5usize,
    )?;
    let verifier = read_only_transaction(
        &fixture.db,
        &format!("{label} table definition observer"),
    )?;
    for (name, table_type, persistence) in [
        (MEMORY_TABLE, KVDBTableType::MemOrdTab, true),
        (VOLATILE_MEMORY_TABLE, KVDBTableType::MemOrdTab, false),
        (LOG_ORDERED_TABLE, KVDBTableType::LogOrdTab, true),
        (BTREE_TABLE, KVDBTableType::BtreeOrdTab, true),
    ] {
        expect_eq(
            &format!("{label} table definition for {name}"),
            &verifier.table_meta(Atom::from(name)).await,
            &Some(table_meta(table_type, persistence)),
        )?;
    }
    Ok(())
}

async fn assert_live_values(
    fixture: &Fixture,
    expected: &[ExpectedEntry],
    label: &str,
) -> TestResult<()> {
    let transaction = read_only_transaction(
        &fixture.db,
        &format!("{label} root-upsert observer"),
    )?;
    let observed = transaction
        .query(
            expected
                .iter()
                .map(|entry| {
                    TableKV::new(
                        Atom::from(entry.table),
                        entry.key.clone(),
                        None,
                    )
                })
                .collect(),
        )
        .await;
    assert_query_values(label, &observed, expected)
}

fn assert_query_values(
    label: &str,
    actual: &[Option<Binary>],
    expected: &[ExpectedEntry],
) -> TestResult<()> {
    expect_eq(
        &format!("{label} result count"),
        &actual.len(),
        &expected.len(),
    )?;
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        if actual.as_ref().map(AsRef::<[u8]>::as_ref) != Some(expected.value.as_ref()) {
            return Err(format!(
                "{label}: value mismatch at {index}, table={}, key_len={}, expected_value_len={}, observed_value_len={:?}",
                expected.table,
                expected.key.len(),
                expected.value.len(),
                actual.as_ref().map(Binary::len),
            ));
        }
    }
    Ok(())
}

async fn wait_for_wal_state(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
    expected_appended: usize,
    expected_confirmed: usize,
    expected_waiting: usize,
    timeout: Duration,
    label: &str,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let appended = fixture.logger.append_total_count();
        let confirmed = fixture.logger.confirm_total_count();
        let waiting = fixture.logger.waiting_confirm_count().await;
        if appended == expected_appended
            && confirmed == expected_confirmed
            && waiting == expected_waiting {
            return Ok(());
        }
        if appended > expected_appended || confirmed > expected_confirmed {
            return Err(format!(
                "{label}: WAL advanced beyond expected state, appended={appended}, confirmed={confirmed}, waiting={waiting}",
            ));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{label}: WAL did not reach appended={expected_appended}, confirmed={expected_confirmed}, waiting={expected_waiting} before {timeout:?}; observed appended={appended}, confirmed={confirmed}, waiting={waiting}",
            ));
        }
        rt.timeout(25).await;
    }
}

fn archive_root_wal(root: &Path) -> TestResult<()> {
    let wal_path = root.join("root-wal");
    let archived = root.join(ARCHIVED_WAL_DIR);
    if archived.exists() {
        return Err(format!("archived root WAL unexpectedly exists: {archived:?}"));
    }
    if nonempty_bak_count(&wal_path)? == 0 {
        return Err("root WAL cannot be archived before a nonempty .bak exists".to_owned());
    }
    fs::rename(&wal_path, &archived)
        .map_err(|error| format!("archiving confirmed root WAL failed: {error}"))
}

fn regular_file_sizes(path: &Path) -> TestResult<Vec<(PathBuf, u64)>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(path)
        .map_err(|error| format!("reading WAL directory {path:?} failed: {error}"))? {
        let entry = entry
            .map_err(|error| format!("reading WAL entry in {path:?} failed: {error}"))?;
        let metadata = entry
            .metadata()
            .map_err(|error| format!("reading metadata for {:?} failed: {error}", entry.path()))?;
        if metadata.is_file() {
            files.push((entry.path(), metadata.len()));
        }
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(files)
}

fn nonempty_bak_count(path: &Path) -> TestResult<usize> {
    Ok(regular_file_sizes(path)?
        .into_iter()
        .filter(|(file, len)| {
            *len > 0
                && file.extension().and_then(|extension| extension.to_str()) == Some("bak")
        })
        .count())
}

fn active_file_sizes(path: &Path) -> TestResult<Vec<(PathBuf, u64)>> {
    Ok(regular_file_sizes(path)?
        .into_iter()
        .filter(|(file, _)| {
            file.extension().and_then(|extension| extension.to_str()) != Some("bak")
        })
        .collect())
}

fn run_phase_process(root: &Path, phase: &str, timeout: Duration) -> TestResult<()> {
    let executable = env::current_exe()
        .map_err(|error| format!("locating root-upsert test executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase)
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| format!("spawning root-upsert phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("root-upsert phase {phase} exited with {status}"))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking root-upsert child status failed: {error}"))? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("root-upsert child exceeded {timeout:?}"));
        }
        thread::sleep(Duration::from_millis(25));
    }
}

#[derive(Clone, Copy)]
enum ActionMode {
    Ordinary,
    Dirty,
}

impl ActionMode {
    const fn label(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::Dirty => "dirty",
        }
    }

    async fn upsert(
        self,
        transaction: &RealTransaction,
        input: Vec<TableKV>,
    ) -> TestResult<()> {
        match self {
            Self::Ordinary => transaction.upsert(input).await,
            Self::Dirty => transaction.dirty_upsert(input).await,
        }
        .map_err(|error| format!("{} root upsert failed: {error:?}", self.label()))
    }

    async fn query(
        self,
        transaction: &RealTransaction,
        input: Vec<TableKV>,
    ) -> Vec<Option<Binary>> {
        match self {
            Self::Ordinary => transaction.query(input).await,
            Self::Dirty => transaction.dirty_query(input).await,
        }
    }
}

struct ActionPlan {
    input: Vec<TableKV>,
    child_order: Vec<&'static str>,
    final_entries: Vec<ExpectedEntry>,
}

struct ExpectedEntry {
    table: &'static str,
    key: Binary,
    value: Binary,
}

fn ordinary_plan() -> ActionPlan {
    let btree_max = maximum_key();
    let btree_second = key("ordinary-btree-second");
    let memory_min = minimum_key();
    let memory_repeated = key("ordinary-memory-repeated");
    let log_repeated = key("ordinary-log-repeated");
    let memory_min_value = value(101);
    let memory_final_value = value(106);
    let log_final_value = value(105);
    let btree_max_value = value(103);
    let btree_second_value = minimum_value();

    ActionPlan {
        input: vec![
            TableKV::new(Atom::from(BTREE_TABLE), btree_max.clone(), None),
            TableKV::new(
                Atom::from(MEMORY_TABLE),
                memory_min.clone(),
                Some(memory_min_value.clone()),
            ),
            TableKV::new(
                Atom::from(LOG_ORDERED_TABLE),
                log_repeated.clone(),
                Some(value(102)),
            ),
            TableKV::new(
                Atom::from(BTREE_TABLE),
                btree_max.clone(),
                Some(btree_max_value.clone()),
            ),
            TableKV::new(
                Atom::from(MEMORY_TABLE),
                memory_repeated.clone(),
                Some(value(104)),
            ),
            TableKV::new(
                Atom::from(LOG_ORDERED_TABLE),
                log_repeated.clone(),
                Some(log_final_value.clone()),
            ),
            TableKV::new(
                Atom::from(MEMORY_TABLE),
                memory_repeated.clone(),
                Some(memory_final_value.clone()),
            ),
            TableKV::new(
                Atom::from(BTREE_TABLE),
                btree_second.clone(),
                Some(btree_second_value.clone()),
            ),
            TableKV::new(
                Atom::from(LOG_ORDERED_TABLE),
                key("ordinary-log-none"),
                None,
            ),
        ],
        child_order: vec![BTREE_TABLE, MEMORY_TABLE, LOG_ORDERED_TABLE],
        final_entries: vec![
            ExpectedEntry {
                table: BTREE_TABLE,
                key: btree_max,
                value: btree_max_value,
            },
            ExpectedEntry {
                table: BTREE_TABLE,
                key: btree_second,
                value: btree_second_value,
            },
            ExpectedEntry {
                table: MEMORY_TABLE,
                key: memory_min,
                value: memory_min_value,
            },
            ExpectedEntry {
                table: MEMORY_TABLE,
                key: memory_repeated,
                value: memory_final_value,
            },
            ExpectedEntry {
                table: LOG_ORDERED_TABLE,
                key: log_repeated,
                value: log_final_value,
            },
        ],
    }
}

fn dirty_plan() -> ActionPlan {
    let log_first = key("dirty-log-first");
    let log_second = key("dirty-log-second");
    let btree_repeated = key("dirty-btree-repeated");
    let memory_repeated = key("dirty-memory-repeated");
    let log_first_value = value(205);
    let log_second_value = minimum_value();
    let btree_final_value = value(204);
    let memory_final_value = value(206);

    ActionPlan {
        input: vec![
            TableKV::new(
                Atom::from(LOG_ORDERED_TABLE),
                key("dirty-log-none"),
                None,
            ),
            TableKV::new(
                Atom::from(BTREE_TABLE),
                btree_repeated.clone(),
                Some(value(201)),
            ),
            TableKV::new(
                Atom::from(MEMORY_TABLE),
                memory_repeated.clone(),
                Some(value(202)),
            ),
            TableKV::new(
                Atom::from(LOG_ORDERED_TABLE),
                log_first.clone(),
                Some(value(203)),
            ),
            TableKV::new(
                Atom::from(BTREE_TABLE),
                btree_repeated.clone(),
                Some(btree_final_value.clone()),
            ),
            TableKV::new(
                Atom::from(LOG_ORDERED_TABLE),
                log_first.clone(),
                Some(log_first_value.clone()),
            ),
            TableKV::new(
                Atom::from(MEMORY_TABLE),
                memory_repeated.clone(),
                Some(memory_final_value.clone()),
            ),
            TableKV::new(
                Atom::from(LOG_ORDERED_TABLE),
                log_second.clone(),
                Some(log_second_value.clone()),
            ),
            TableKV::new(
                Atom::from(BTREE_TABLE),
                key("dirty-btree-none"),
                None,
            ),
        ],
        child_order: vec![LOG_ORDERED_TABLE, BTREE_TABLE, MEMORY_TABLE],
        final_entries: vec![
            ExpectedEntry {
                table: LOG_ORDERED_TABLE,
                key: log_first,
                value: log_first_value,
            },
            ExpectedEntry {
                table: LOG_ORDERED_TABLE,
                key: log_second,
                value: log_second_value,
            },
            ExpectedEntry {
                table: BTREE_TABLE,
                key: btree_repeated,
                value: btree_final_value,
            },
            ExpectedEntry {
                table: MEMORY_TABLE,
                key: memory_repeated,
                value: memory_final_value,
            },
        ],
    }
}

fn combined_final_entries() -> Vec<ExpectedEntry> {
    let mut combined = ordinary_plan().final_entries;
    combined.extend(dirty_plan().final_entries);
    combined
}

fn none_only_keys() -> Vec<(&'static str, Binary)> {
    vec![
        (MEMORY_TABLE, key("ordinary-none-only")),
        (LOG_ORDERED_TABLE, key("dirty-none-only")),
    ]
}

#[derive(Clone, Copy)]
struct DirtyConflictCase {
    label: &'static str,
    table: &'static str,
    must_conflict: bool,
    persistent: bool,
}

impl DirtyConflictCase {
    const fn new(
        label: &'static str,
        table: &'static str,
        must_conflict: bool,
        persistent: bool,
    ) -> Self {
        Self {
            label,
            table,
            must_conflict,
            persistent,
        }
    }
}

fn table_meta(table_type: KVDBTableType, persistence: bool) -> KVTableMeta {
    KVTableMeta::new(table_type, persistence, EnumType::Bin, EnumType::Bin)
}

fn minimum_key() -> Binary {
    let key = encode_bin(&[]);
    assert_eq!(key.len(), 1);
    key
}

fn maximum_key() -> Binary {
    let key = encode_bin(&vec![0xA5; u16::MAX as usize - 3]);
    assert_eq!(key.len(), u16::MAX as usize);
    key
}

fn key(name: &str) -> Binary {
    encode_bin(name.as_bytes())
}

fn value(number: u64) -> Binary {
    encode_bin(&number.to_le_bytes())
}

fn minimum_value() -> Binary {
    let value = encode_bin(&[]);
    assert_eq!(value.len(), 1);
    value
}

fn encode_bin(bytes: &[u8]) -> Binary {
    let mut buffer = WriteBuffer::new();
    buffer.write_bin(bytes, 0..bytes.len());
    Binary::new(buffer.bytes)
}

fn unique_temp_root(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must not precede UNIX_EPOCH")
        .as_nanos();
    env::temp_dir().join(format!(
        "pi_db_root_upsert_{label}_{}_{}",
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
