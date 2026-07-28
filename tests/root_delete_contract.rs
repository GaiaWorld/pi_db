//! 根事务 `delete/dirty_delete` 的真实批次、冲突、WAL、确认和恢复契约专项。
//!
//! 本 target 使用真实 4-worker runtime、`Transaction2PcManager`、`CommitLogger`、
//! Memory/LogOrdered/Btree、redb、表日志和文件系统。恢复矩阵先确认种子值已经进入表数据文件，
//! 再移走种子根 WAL，制造删除 WAL 已提交但表删除尚未确认的窗口；生产 repair、`.bak` 和两次
//! data-only 冷启动共同作为硬门禁。普通与 dirty 非空动作始终使用不同根。
//!
//! 正式契约见 `docs/ROOT_DELETE_CONTRACT.md#root-delete-contract-index`。

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
    build_database,
    expect_binary,
    expect_eq,
    query_ordinary,
    read_only_transaction,
    run_on_runtime,
    writable_transaction,
    Fixture,
    RealTransaction,
    TestResult,
};

type CodecTable = MemoryOrderedTable<usize, CommitLogger>;

const TEST_NAME: &str = "test_root_delete_contract_recovery";
const PHASE_ENV: &str = "PI_DB_ROOT_DELETE_PHASE";
const ROOT_ENV: &str = "PI_DB_ROOT_DELETE_ROOT";
const SEED_WAL_DIR: &str = "confirmed-seed-root-wal";
const DELETE_WAL_DIR: &str = "confirmed-delete-root-wal";

const MEMORY_TABLE: &str = "root_delete_memory";
const VOLATILE_MEMORY_TABLE: &str = "root_delete_memory_volatile";
const LOG_ORDERED_TABLE: &str = "root_delete_log_ordered";
const BTREE_TABLE: &str = "root_delete_btree";

const ROOT_TID_BYTES: usize = 16;
const PROCESS_TIMEOUT: Duration = Duration::from_secs(180);
const SEED_TIMEOUT: Duration = Duration::from_secs(130);
const DELETE_TIMEOUT: Duration = Duration::from_secs(45);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(130);
const DATA_ONLY_TIMEOUT: Duration = Duration::from_secs(30);
const CONFLICT_TIMEOUT: Duration = Duration::from_secs(60);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(100);

#[test]
fn test_root_delete_contract_recovery() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV)
                .expect("root-delete child phase must receive its root path"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("root-delete phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root("recovery");
    fs::create_dir_all(&root).expect("creating root-delete recovery root must succeed");
    let result = (|| -> TestResult<()> {
        run_phase_process(&root, "seed", PROCESS_TIMEOUT)?;
        archive_root_wal(&root, SEED_WAL_DIR)?;
        run_phase_process(&root, "delete", PROCESS_TIMEOUT)?;
        run_phase_process(&root, "recover", PROCESS_TIMEOUT)?;
        archive_root_wal(&root, DELETE_WAL_DIR)?;
        run_phase_process(&root, "data-only", PROCESS_TIMEOUT)?;
        run_phase_process(&root, "data-only-again", PROCESS_TIMEOUT)
    })();
    if let Err(error) = result {
        panic!(
            "root-delete recovery failed; evidence is preserved at {:?}: {error}",
            root,
        );
    }
    fs::remove_dir_all(&root).expect("cleaning root-delete recovery root must succeed");
}

#[test]
fn test_root_dirty_delete_conflict_matrix() {
    let root = TempRoot::new("dirty-conflicts")
        .expect("creating dirty-delete conflict root must succeed");
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
            "dirty-delete conflict manager balance",
            &fixture.tr_manager.produced_transaction_total(),
            &fixture.tr_manager.consumed_transaction_total(),
        )?;
        expect_eq(
            "dirty-delete conflict active roots",
            &fixture.tr_manager.transaction_len(),
            &0usize,
        )?;
        let appended = fixture.logger.append_total_count();
        let confirmed = fixture.logger.confirm_total_count();
        let waiting = fixture.logger.waiting_confirm_count().await;
        expect_eq(
            "dirty-delete conflict WAL conservation",
            &appended,
            &(confirmed + waiting),
        )
    })
    .unwrap_or_else(|error| panic!("root dirty-delete conflict matrix failed: {error}"));
}

fn run_child_phase(phase: &str, root: &Path) -> TestResult<()> {
    match phase {
        "seed" => {
            let root = root.to_path_buf();
            run_on_runtime(SEED_TIMEOUT, move |rt| async move {
                phase_seed(rt, root).await
            })
        },
        "delete" => {
            let root = root.to_path_buf();
            run_on_runtime(DELETE_TIMEOUT, move |rt| async move {
                phase_delete(rt, root).await
            })
        },
        "recover" => {
            let root = root.to_path_buf();
            run_on_runtime(RECOVERY_TIMEOUT, move |rt| async move {
                phase_recover(rt, root).await
            })
        },
        "data-only" => {
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
        other => Err(format!("unknown root-delete phase: {other}")),
    }
}

async fn phase_seed(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let fixture = build_database(
        &rt,
        &root,
        Duration::ZERO,
        Duration::ZERO,
    ).await?;
    create_tables(&fixture).await?;
    assert_table_definitions(&fixture, "seed").await?;

    let seed = writable_transaction(&fixture.db, "root-delete persisted seed")?;
    seed
        .upsert(
            baseline_entries()
                .into_iter()
                .map(ExpectedAction::into_table_kv)
                .collect(),
        )
        .await
        .map_err(|error| format!("writing root-delete seed failed: {error:?}"))?;
    commit_ordinary(&seed, "root-delete persisted seed").await?;
    assert_baseline_values(&fixture, "seed visible before confirmation").await?;

    wait_for_wal_state(
        &rt,
        &fixture,
        2,
        2,
        0,
        CONFIRM_TIMEOUT,
        "seed persistence",
    ).await?;
    assert_baseline_values(&fixture, "seed visible after confirmation").await?;
    expect_eq(
        "seed manager produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &2usize,
    )?;
    expect_eq(
        "seed manager consumed count",
        &fixture.tr_manager.consumed_transaction_total(),
        &2usize,
    )?;
    expect_eq(
        "seed manager active roots",
        &fixture.tr_manager.transaction_len(),
        &0usize,
    )?;
    expect_eq(
        "seed Btree overlay drained",
        &fixture.db.table_cache_size(&Atom::from(BTREE_TABLE)).await,
        &Some(0u64),
    )?;
    if nonempty_bak_count(&root.join("root-wal"))? == 0 {
        return Err("seed confirmation produced no nonempty .bak checkpoint".to_owned());
    }
    let active = active_file_sizes(&root.join("root-wal"))?;
    if active.iter().any(|(_, len)| *len > 0) {
        return Err(format!(
            "seed confirmation left a nonempty active root WAL: {active:?}",
        ));
    }
    Ok(())
}

async fn phase_delete(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    if !root.join(SEED_WAL_DIR).exists() {
        return Err("delete phase did not find archived confirmed seed WAL".to_owned());
    }
    let fixture = build_database(
        &rt,
        &root,
        Duration::ZERO,
        Duration::ZERO,
    ).await?;
    assert_table_definitions(&fixture, "delete").await?;
    assert_data_file_baseline(&fixture, "data-file baseline before delete").await?;
    expect_eq(
        "delete startup WAL append count",
        &fixture.logger.append_total_count(),
        &0usize,
    )?;
    expect_eq(
        "delete startup manager produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &0usize,
    )?;

    verify_empty_and_missing_batches(&fixture).await?;
    commit_overlay_seed(&fixture).await?;
    execute_delete_plan(&fixture, &ordinary_plan()).await?;
    execute_delete_plan(&fixture, &dirty_plan()).await?;
    assert_final_state(&fixture, "delete committed view", true).await?;

    expect_eq(
        "delete phase WAL append count",
        &fixture.logger.append_total_count(),
        &3usize,
    )?;
    expect_eq(
        "delete phase WAL confirm count",
        &fixture.logger.confirm_total_count(),
        &0usize,
    )?;
    expect_eq(
        "delete phase WAL waiting count",
        &fixture.logger.waiting_confirm_count().await,
        &3usize,
    )?;
    expect_eq(
        "delete phase manager produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &4usize,
    )?;
    expect_eq(
        "delete phase manager consumed count",
        &fixture.tr_manager.consumed_transaction_total(),
        &4usize,
    )?;
    expect_eq(
        "delete phase manager active roots",
        &fixture.tr_manager.transaction_len(),
        &0usize,
    )?;
    expect_eq(
        "delete phase nonempty .bak count",
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
    assert_final_state(&fixture, "replayed before confirmation", true).await?;

    wait_for_wal_state(
        &rt,
        &fixture,
        3,
        3,
        0,
        CONFIRM_TIMEOUT,
        "delete recovery",
    ).await?;
    assert_final_state(&fixture, "replayed after confirmation", true).await?;
    expect_eq(
        "recovery manager produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &3usize,
    )?;
    expect_eq(
        "recovery manager consumed count",
        &fixture.tr_manager.consumed_transaction_total(),
        &3usize,
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
            "delete recovery did not add a confirmed .bak checkpoint: before={bak_before}, after={bak_after}",
        ));
    }
    let active = active_file_sizes(&wal_path)?;
    if active.iter().any(|(_, len)| *len > 0) {
        return Err(format!(
            "nonempty active root WAL remains after delete recovery: {active:?}",
        ));
    }
    Ok(())
}

async fn phase_data_only(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
    label: &str,
) -> TestResult<()> {
    let archived = root.join(DELETE_WAL_DIR);
    if !archived.exists() || nonempty_bak_count(&archived)? == 0 {
        return Err(format!(
            "{label}: archived delete WAL has no nonempty confirmed .bak checkpoint",
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
    assert_final_state(&fixture, label, false).await?;
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
    let transaction = writable_transaction(&fixture.db, "root-delete table DDL")?;
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
    commit_ordinary(&transaction, "root-delete table DDL").await
}

async fn verify_empty_and_missing_batches(fixture: &Fixture) -> TestResult<()> {
    let empty = writable_transaction(&fixture.db, "root-delete empty protocol proof")?;
    let ordinary = empty
        .delete(Vec::new())
        .await
        .map_err(|error| format!("empty ordinary delete failed: {error:?}"))?;
    let dirty = empty
        .dirty_delete(Vec::new())
        .await
        .map_err(|error| format!("empty dirty delete failed: {error:?}"))?;
    expect_eq("empty ordinary result", &ordinary.len(), &0usize)?;
    expect_eq("empty dirty result", &dirty.len(), &0usize)?;
    expect_eq("empty root child count", &empty.children_len(), &0usize)?;
    expect_eq(
        "empty root persistence",
        &empty.is_require_persistence(),
        &false,
    )?;
    drop(empty);

    let missing = writable_transaction(&fixture.db, "root-delete missing-only root")?;
    let result = missing
        .delete(vec![
            delete_item("missing-delete-first", key("missing-key-first"), None),
            delete_item(
                "missing-delete-middle",
                key("missing-key-middle"),
                Some(value(9_001)),
            ),
            delete_item("missing-delete-last", key("missing-key-last"), None),
        ])
        .await
        .map_err(|error| format!("missing-only delete failed: {error:?}"))?;
    assert_delete_results(
        "missing-only delete",
        &result,
        &[None, None, None],
    )?;
    expect_eq(
        "missing-only child count",
        &missing.children_len(),
        &0usize,
    )?;
    expect_eq(
        "missing-only persistence",
        &missing.is_require_persistence(),
        &false,
    )?;
    let prepare = missing
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing missing-only delete failed: {error:?}"))?;
    expect_eq("missing-only prepare bytes", &prepare.len(), &0usize)?;
    missing
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing missing-only delete failed: {error:?}"))?;
    expect_eq(
        "missing-only committed status",
        &missing.get_status(),
        &Transaction2PcStatus::Commited,
    )?;
    expect_eq(
        "empty/missing WAL append count",
        &fixture.logger.append_total_count(),
        &0usize,
    )
}

async fn commit_overlay_seed(fixture: &Fixture) -> TestResult<()> {
    let transaction = writable_transaction(&fixture.db, "root-delete overlay seed")?;
    let btree = ExpectedAction::upsert(
        BTREE_TABLE,
        key("ordinary-btree-shared"),
        value(106),
    );
    let mut expected = vec![btree.clone()];
    expected.extend(
        baseline_entries()
            .into_iter()
            .filter(|action| action.table == MEMORY_TABLE),
    );
    transaction
        .upsert(
            expected
                .iter()
                .cloned()
                .map(ExpectedAction::into_table_kv)
                .collect(),
        )
        .await
        .map_err(|error| format!("writing root-delete overlay seed failed: {error:?}"))?;
    let children: Vec<RealTransaction> = transaction.to_children().collect();
    assert_child_order(
        &children,
        &[BTREE_TABLE, MEMORY_TABLE],
        "root-delete overlay seed",
    )?;
    let prepare = prepare_and_assert(
        &transaction,
        &children,
        &[BTREE_TABLE, MEMORY_TABLE],
        &expected,
        "root-delete overlay seed",
    ).await?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing root-delete overlay seed failed: {error:?}"))?;
    expect_binary(
        "shared Btree seed visible",
        query_ordinary(
            &fixture.db,
            BTREE_TABLE,
            btree.key,
            "shared Btree seed observer",
        ).await?.as_ref(),
        btree.value.as_ref(),
    )?;
    for action in expected
        .into_iter()
        .filter(|action| action.table == MEMORY_TABLE) {
        expect_binary(
            "Memory overlay seed visible",
            query_ordinary(
                &fixture.db,
                MEMORY_TABLE,
                action.key,
                "Memory overlay seed observer",
            ).await?.as_ref(),
            action.value.as_ref(),
        )?;
    }
    Ok(())
}

async fn execute_delete_plan(fixture: &Fixture, plan: &DeletePlan) -> TestResult<()> {
    let transaction = writable_transaction(
        &fixture.db,
        &format!("root-delete {} business root", plan.mode.label()),
    )?;
    expect_eq(
        &format!("{} initial status", plan.mode.label()),
        &transaction.get_status(),
        &Transaction2PcStatus::Start,
    )?;
    expect_eq(
        &format!("{} initial TID", plan.mode.label()),
        &transaction.get_transaction_uid(),
        &None,
    )?;

    plan.mode
        .upsert(&transaction, plan.pre_upserts.clone())
        .await?;
    let results = plan.mode
        .delete(&transaction, plan.delete_input.clone())
        .await?;
    assert_delete_results(
        &format!("{} delete results", plan.mode.label()),
        &results,
        &plan.expected_returns,
    )?;
    plan.mode
        .upsert(&transaction, plan.post_upserts.clone())
        .await?;

    expect_eq(
        &format!("{} root persistence", plan.mode.label()),
        &transaction.is_require_persistence(),
        &true,
    )?;
    expect_eq(
        &format!("{} child count", plan.mode.label()),
        &transaction.children_len(),
        &plan.child_order.len(),
    )?;
    let children: Vec<RealTransaction> = transaction.to_children().collect();
    assert_child_order(&children, &plan.child_order, plan.mode.label())?;

    let private = plan.mode
        .query(
            &transaction,
            plan.final_actions
                .iter()
                .cloned()
                .map(ExpectedAction::into_query)
                .collect(),
        )
        .await;
    assert_action_values(
        &format!("{} private final state", plan.mode.label()),
        &private,
        &plan.final_actions,
    )?;

    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let prepare = prepare_and_assert(
        &transaction,
        &children,
        &plan.child_order,
        &plan.final_actions,
        plan.mode.label(),
    ).await?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| {
            format!("committing {} business root failed: {error:?}", plan.mode.label())
        })?;
    expect_eq(
        &format!("{} committed status", plan.mode.label()),
        &transaction.get_status(),
        &Transaction2PcStatus::Commited,
    )?;
    expect_eq(
        &format!("{} manager produced increment", plan.mode.label()),
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        &format!("{} manager consumed increment", plan.mode.label()),
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 1),
    )?;
    expect_eq(
        &format!("{} manager active roots", plan.mode.label()),
        &fixture.tr_manager.transaction_len(),
        &0usize,
    )?;
    expect_eq(
        &format!("{} WAL append increment", plan.mode.label()),
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )
}

async fn prepare_and_assert(
    transaction: &RealTransaction,
    children: &[RealTransaction],
    child_order: &[&str],
    expected_actions: &[ExpectedAction],
    label: &str,
) -> TestResult<Vec<u8>> {
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing {label} failed: {error:?}"))?;
    let transaction_uid = transaction
        .get_transaction_uid()
        .ok_or_else(|| format!("{label}: prepare did not allocate TID"))?;
    let commit_uid = transaction
        .get_commit_uid()
        .ok_or_else(|| format!("{label}: prepare did not allocate CID"))?;
    expect_eq(
        &format!("{label} prepared status"),
        &transaction.get_status(),
        &Transaction2PcStatus::Prepared,
    )?;
    assert_shared_child_identity(
        children,
        &transaction_uid,
        &commit_uid,
        label,
        Transaction2PcStatus::Prepared,
    )?;
    assert_prepare_output(
        &prepare,
        &transaction_uid,
        child_order,
        expected_actions,
        label,
    )?;
    Ok(prepare)
}

async fn verify_dirty_conflict_case(
    fixture: &Fixture,
    case: DirtyConflictCase,
) -> TestResult<()> {
    let target = key(&format!("dirty-delete-conflict-{}", case.table));
    let seed_value = value(10);
    let winner_value = value(20);

    let seed = writable_transaction(
        &fixture.db,
        &format!("{} dirty-delete seed", case.label),
    )?;
    seed
        .upsert(vec![TableKV::new(
            Atom::from(case.table),
            target.clone(),
            Some(seed_value.clone()),
        )])
        .await
        .map_err(|error| format!("{} seed upsert failed: {error:?}", case.label))?;
    commit_ordinary(&seed, &format!("{} seed", case.label)).await?;

    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();

    let stale = writable_transaction(
        &fixture.db,
        &format!("{} stale dirty deleter", case.label),
    )?;
    let stale_result = stale
        .dirty_delete(vec![TableKV::new(
            Atom::from(case.table),
            target.clone(),
            Some(value(30)),
        )])
        .await
        .map_err(|error| format!("{} stale dirty delete failed: {error:?}", case.label))?;
    let expected_stale = if case.table == BTREE_TABLE {
        vec![Some(seed_value)]
    } else {
        vec![None]
    };
    assert_delete_results(
        &format!("{} stale dirty return", case.label),
        &stale_result,
        &expected_stale,
    )?;

    let winner = writable_transaction(
        &fixture.db,
        &format!("{} concurrent winner", case.label),
    )?;
    winner
        .upsert(vec![TableKV::new(
            Atom::from(case.table),
            target.clone(),
            Some(winner_value.clone()),
        )])
        .await
        .map_err(|error| format!("{} winner upsert failed: {error:?}", case.label))?;
    commit_ordinary(&winner, &format!("{} winner", case.label)).await?;

    if case.must_conflict {
        let error = stale
            .prepare_modified_conflicts()
            .await
            .expect_err("stale dirty delete must return the frozen conflict result");
        assert_conflict(&error, case.table, &target, case.label)?;
        expect_eq(
            &format!("{} stale prepare-failed status", case.label),
            &stale.get_status(),
            &Transaction2PcStatus::PrepareFailed,
        )?;
        expect_eq(
            &format!("{} rejected root active before rollback", case.label),
            &fixture.tr_manager.transaction_len(),
            &1usize,
        )?;
        expect_eq(
            &format!("{} rejected root wrote no WAL", case.label),
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
            &format!("{} winner after rollback", case.label),
            query_ordinary(
                &fixture.db,
                case.table,
                target.clone(),
                &format!("{} rollback observer", case.label),
            ).await?.as_ref(),
            Some(&winner_value),
        )?;

        let retry = writable_transaction(
            &fixture.db,
            &format!("{} post-rollback retry", case.label),
        )?;
        let retry_result = retry
            .delete(vec![TableKV::new(
                Atom::from(case.table),
                target.clone(),
                None,
            )])
            .await
            .map_err(|error| format!("{} retry delete failed: {error:?}", case.label))?;
        let expected_retry = if case.table == BTREE_TABLE {
            vec![Some(winner_value)]
        } else {
            vec![None]
        };
        assert_delete_results(
            &format!("{} retry delete return", case.label),
            &retry_result,
            &expected_retry,
        )?;
        commit_ordinary(&retry, &format!("{} retry", case.label)).await?;
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
    } else {
        let prepare = stale
            .prepare_modified_conflicts()
            .await
            .map_err(|error| {
                format!("{} stale dirty prepare unexpectedly conflicted: {error:?}", case.label)
            })?;
        if case.persistent && prepare.len() <= ROOT_TID_BYTES {
            return Err(format!(
                "{} stale dirty delete omitted persistent WAL payload",
                case.label,
            ));
        }
        stale
            .commit_modified(prepare)
            .await
            .map_err(|error| format!("{} stale dirty commit failed: {error:?}", case.label))?;
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
    }

    expect_binary(
        &format!("{} final deleted value", case.label),
        query_ordinary(
            &fixture.db,
            case.table,
            target,
            &format!("{} final observer", case.label),
        ).await?.as_ref(),
        None,
    )?;
    expect_eq(
        &format!("{} final WAL append count", case.label),
        &fixture.logger.append_total_count(),
        &(append_before + 2 * usize::from(case.persistent)),
    )?;
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
    expected_actions: &[ExpectedAction],
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
    let observed_total = segments
        .iter()
        .map(|(_, actions)| actions.len())
        .sum::<usize>();
    expect_eq(
        &format!("{label} WAL final action count"),
        &observed_total,
        &expected_actions.len(),
    )?;

    for (table, actions) in segments {
        let expected = expected_actions
            .iter()
            .filter(|action| action.table == table.as_str())
            .collect::<Vec<_>>();
        expect_eq(
            &format!("{label} {} action count", table.as_str()),
            &actions.len(),
            &expected.len(),
        )?;
        for expected_action in expected {
            let matches = actions
                .iter()
                .filter(|action| {
                    action.key.as_ref() == expected_action.key.as_ref()
                        && action.value.as_ref().map(AsRef::<[u8]>::as_ref)
                            == expected_action.value.as_ref().map(AsRef::<[u8]>::as_ref)
                })
                .count();
            expect_eq(
                &format!(
                    "{label} exact final action {} key_len={}",
                    table.as_str(),
                    expected_action.key.len(),
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

async fn assert_baseline_values(fixture: &Fixture, label: &str) -> TestResult<()> {
    let expected = baseline_entries();
    let transaction = read_only_transaction(
        &fixture.db,
        &format!("{label} baseline observer"),
    )?;
    let observed = transaction
        .query(
            expected
                .iter()
                .cloned()
                .map(ExpectedAction::into_query)
                .collect(),
        )
        .await;
    assert_action_values(label, &observed, &expected)
}

async fn assert_data_file_baseline(fixture: &Fixture, label: &str) -> TestResult<()> {
    for action in baseline_entries() {
        let actual = query_ordinary(
            &fixture.db,
            action.table,
            action.key,
            &format!("{label} observer for {}", action.table),
        ).await?;
        let expected = if action.table == MEMORY_TABLE {
            None
        } else {
            action.value.as_ref()
        };
        expect_binary(
            &format!("{label} value in {}", action.table),
            actual.as_ref(),
            expected,
        )?;
    }
    Ok(())
}

async fn assert_final_state(
    fixture: &Fixture,
    label: &str,
    memory_is_live: bool,
) -> TestResult<()> {
    for (table, target) in deleted_keys() {
        let actual = query_ordinary(
            &fixture.db,
            table,
            target,
            &format!("{label} deleted-key observer for {table}"),
        ).await?;
        expect_binary(
            &format!("{label} deleted key in {table}"),
            actual.as_ref(),
            None,
        )?;
    }
    for action in restored_entries() {
        let actual = query_ordinary(
            &fixture.db,
            action.table,
            action.key.clone(),
            &format!("{label} restored-key observer for {}", action.table),
        ).await?;
        expect_binary(
            &format!("{label} restored key in {}", action.table),
            actual.as_ref(),
            action.value.as_ref(),
        )?;
    }
    for action in control_entries() {
        let actual = query_ordinary(
            &fixture.db,
            action.table,
            action.key.clone(),
            &format!("{label} control observer for {}", action.table),
        ).await?;
        let expected = if action.table == MEMORY_TABLE && !memory_is_live {
            None
        } else {
            action.value.as_ref()
        };
        expect_binary(
            &format!("{label} control value in {}", action.table),
            actual.as_ref(),
            expected,
        )?;
    }
    if !memory_is_live {
        for action in baseline_entries()
            .into_iter()
            .filter(|action| action.table == MEMORY_TABLE) {
            let actual = query_ordinary(
                &fixture.db,
                MEMORY_TABLE,
                action.key,
                &format!("{label} Memory data-only observer"),
            ).await?;
            expect_binary(
                &format!("{label} Memory has no independent data file"),
                actual.as_ref(),
                None,
            )?;
        }
    }
    Ok(())
}

fn assert_action_values(
    label: &str,
    actual: &[Option<Binary>],
    expected: &[ExpectedAction],
) -> TestResult<()> {
    expect_eq(
        &format!("{label} result count"),
        &actual.len(),
        &expected.len(),
    )?;
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        if actual.as_ref().map(AsRef::<[u8]>::as_ref)
            != expected.value.as_ref().map(AsRef::<[u8]>::as_ref) {
            return Err(format!(
                "{label}: value mismatch at {index}, table={}, key_len={}, expected_value_len={:?}, observed_value_len={:?}",
                expected.table,
                expected.key.len(),
                expected.value.as_ref().map(Binary::len),
                actual.as_ref().map(Binary::len),
            ));
        }
    }
    Ok(())
}

fn assert_delete_results(
    label: &str,
    actual: &[Option<Binary>],
    expected: &[Option<Binary>],
) -> TestResult<()> {
    expect_eq(
        &format!("{label} result count"),
        &actual.len(),
        &expected.len(),
    )?;
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        if actual.as_ref().map(AsRef::<[u8]>::as_ref)
            != expected.as_ref().map(AsRef::<[u8]>::as_ref) {
            return Err(format!(
                "{label}: return mismatch at {index}, expected_len={:?}, observed_len={:?}",
                expected.as_ref().map(Binary::len),
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

fn archive_root_wal(root: &Path, archive_name: &str) -> TestResult<()> {
    let wal_path = root.join("root-wal");
    let archived = root.join(archive_name);
    if archived.exists() {
        return Err(format!("archived root WAL unexpectedly exists: {archived:?}"));
    }
    if nonempty_bak_count(&wal_path)? == 0 {
        return Err(format!(
            "root WAL cannot be archived to {archive_name} before a nonempty .bak exists",
        ));
    }
    fs::rename(&wal_path, &archived)
        .map_err(|error| format!("archiving root WAL to {archive_name} failed: {error}"))
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
        .map_err(|error| format!("locating root-delete test executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase)
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| format!("spawning root-delete phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if !status.success() {
        return Err(format!(
            "root-delete phase {phase} exited unsuccessfully: {status}",
        ));
    }
    Ok(())
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("polling root-delete child failed: {error}"))? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("root-delete child exceeded {timeout:?}"));
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

    async fn delete(
        self,
        transaction: &RealTransaction,
        input: Vec<TableKV>,
    ) -> TestResult<Vec<Option<Binary>>> {
        match self {
            Self::Ordinary => transaction.delete(input).await,
            Self::Dirty => transaction.dirty_delete(input).await,
        }
        .map_err(|error| format!("{} root delete failed: {error:?}", self.label()))
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

struct DeletePlan {
    mode: ActionMode,
    pre_upserts: Vec<TableKV>,
    delete_input: Vec<TableKV>,
    expected_returns: Vec<Option<Binary>>,
    post_upserts: Vec<TableKV>,
    child_order: Vec<&'static str>,
    final_actions: Vec<ExpectedAction>,
}

#[derive(Clone)]
struct ExpectedAction {
    table: &'static str,
    key: Binary,
    value: Option<Binary>,
}

impl ExpectedAction {
    fn upsert(table: &'static str, key: Binary, value: Binary) -> Self {
        Self {
            table,
            key,
            value: Some(value),
        }
    }

    fn delete(table: &'static str, key: Binary) -> Self {
        Self {
            table,
            key,
            value: None,
        }
    }

    fn into_table_kv(self) -> TableKV {
        TableKV::new(Atom::from(self.table), self.key, self.value)
    }

    fn into_query(self) -> TableKV {
        TableKV::new(Atom::from(self.table), self.key, None)
    }
}

fn ordinary_plan() -> DeletePlan {
    let btree_max = maximum_key();
    let memory_key = key("ordinary-memory");
    let memory_repeat = key("ordinary-memory-repeat");
    let log_key = key("ordinary-log");
    let log_restore = key("ordinary-log-restore");
    let btree_shared = key("ordinary-btree-shared");
    let btree_private = key("ordinary-btree-private");
    let private_value = value(107);

    DeletePlan {
        mode: ActionMode::Ordinary,
        pre_upserts: vec![TableKV::new(
            Atom::from(BTREE_TABLE),
            btree_private.clone(),
            Some(private_value.clone()),
        )],
        delete_input: vec![
            delete_item("missing-ordinary-first", key("missing-ordinary-first"), None),
            TableKV::new(Atom::from(BTREE_TABLE), btree_max.clone(), None),
            TableKV::new(Atom::from(MEMORY_TABLE), memory_key.clone(), None),
            delete_item("missing-ordinary-middle", key("missing-ordinary-middle"), None),
            TableKV::new(
                Atom::from(LOG_ORDERED_TABLE),
                log_key.clone(),
                Some(value(9_101)),
            ),
            TableKV::new(Atom::from(BTREE_TABLE), btree_shared.clone(), None),
            TableKV::new(Atom::from(BTREE_TABLE), btree_private.clone(), None),
            TableKV::new(Atom::from(BTREE_TABLE), btree_max.clone(), None),
            TableKV::new(Atom::from(MEMORY_TABLE), memory_repeat.clone(), None),
            TableKV::new(Atom::from(MEMORY_TABLE), memory_repeat.clone(), None),
            TableKV::new(Atom::from(LOG_ORDERED_TABLE), log_restore.clone(), None),
            delete_item("missing-ordinary-last", key("missing-ordinary-last"), None),
        ],
        expected_returns: vec![
            None,
            Some(value(105)),
            None,
            None,
            None,
            Some(value(106)),
            Some(private_value),
            None,
            None,
            None,
            None,
            None,
        ],
        post_upserts: vec![TableKV::new(
            Atom::from(LOG_ORDERED_TABLE),
            log_restore.clone(),
            Some(value(1_004)),
        )],
        child_order: vec![BTREE_TABLE, MEMORY_TABLE, LOG_ORDERED_TABLE],
        final_actions: vec![
            ExpectedAction::delete(BTREE_TABLE, btree_max),
            ExpectedAction::delete(BTREE_TABLE, btree_shared),
            ExpectedAction::delete(BTREE_TABLE, btree_private),
            ExpectedAction::delete(MEMORY_TABLE, memory_key),
            ExpectedAction::delete(MEMORY_TABLE, memory_repeat),
            ExpectedAction::delete(LOG_ORDERED_TABLE, log_key),
            ExpectedAction::upsert(LOG_ORDERED_TABLE, log_restore, value(1_004)),
        ],
    }
}

fn dirty_plan() -> DeletePlan {
    let memory_key = key("dirty-memory");
    let memory_private = key("dirty-memory-private");
    let log_key = key("dirty-log");
    let log_restore = key("dirty-log-restore");
    let btree_key = key("dirty-btree");

    DeletePlan {
        mode: ActionMode::Dirty,
        pre_upserts: vec![TableKV::new(
            Atom::from(MEMORY_TABLE),
            memory_private.clone(),
            Some(value(207)),
        )],
        delete_input: vec![
            delete_item("missing-dirty-first", key("missing-dirty-first"), None),
            TableKV::new(Atom::from(LOG_ORDERED_TABLE), log_key.clone(), None),
            TableKV::new(Atom::from(MEMORY_TABLE), memory_key.clone(), None),
            TableKV::new(Atom::from(BTREE_TABLE), btree_key.clone(), None),
            delete_item("missing-dirty-middle", key("missing-dirty-middle"), None),
            TableKV::new(Atom::from(MEMORY_TABLE), memory_private.clone(), None),
            TableKV::new(Atom::from(BTREE_TABLE), btree_key.clone(), None),
            TableKV::new(
                Atom::from(LOG_ORDERED_TABLE),
                log_restore.clone(),
                Some(value(9_201)),
            ),
            delete_item("missing-dirty-last", key("missing-dirty-last"), None),
        ],
        expected_returns: vec![
            None,
            None,
            None,
            Some(value(204)),
            None,
            None,
            None,
            None,
            None,
        ],
        post_upserts: vec![TableKV::new(
            Atom::from(LOG_ORDERED_TABLE),
            log_restore.clone(),
            Some(value(1_203)),
        )],
        child_order: vec![MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE],
        final_actions: vec![
            ExpectedAction::delete(MEMORY_TABLE, memory_key),
            ExpectedAction::delete(MEMORY_TABLE, memory_private),
            ExpectedAction::delete(LOG_ORDERED_TABLE, log_key),
            ExpectedAction::upsert(LOG_ORDERED_TABLE, log_restore, value(1_203)),
            ExpectedAction::delete(BTREE_TABLE, btree_key),
        ],
    }
}

fn baseline_entries() -> Vec<ExpectedAction> {
    vec![
        ExpectedAction::upsert(MEMORY_TABLE, key("ordinary-memory"), value(101)),
        ExpectedAction::upsert(MEMORY_TABLE, key("ordinary-memory-repeat"), value(102)),
        ExpectedAction::upsert(MEMORY_TABLE, key("dirty-memory"), value(201)),
        ExpectedAction::upsert(MEMORY_TABLE, key("memory-control"), value(190)),
        ExpectedAction::upsert(LOG_ORDERED_TABLE, key("ordinary-log"), value(103)),
        ExpectedAction::upsert(
            LOG_ORDERED_TABLE,
            key("ordinary-log-restore"),
            value(104),
        ),
        ExpectedAction::upsert(LOG_ORDERED_TABLE, key("dirty-log"), value(202)),
        ExpectedAction::upsert(
            LOG_ORDERED_TABLE,
            key("dirty-log-restore"),
            value(203),
        ),
        ExpectedAction::upsert(LOG_ORDERED_TABLE, key("log-control"), value(290)),
        ExpectedAction::upsert(BTREE_TABLE, maximum_key(), value(105)),
        ExpectedAction::upsert(BTREE_TABLE, key("dirty-btree"), value(204)),
        ExpectedAction::upsert(BTREE_TABLE, key("btree-control"), value(390)),
    ]
}

fn restored_entries() -> Vec<ExpectedAction> {
    vec![
        ExpectedAction::upsert(
            LOG_ORDERED_TABLE,
            key("ordinary-log-restore"),
            value(1_004),
        ),
        ExpectedAction::upsert(
            LOG_ORDERED_TABLE,
            key("dirty-log-restore"),
            value(1_203),
        ),
    ]
}

fn control_entries() -> Vec<ExpectedAction> {
    vec![
        ExpectedAction::upsert(MEMORY_TABLE, key("memory-control"), value(190)),
        ExpectedAction::upsert(LOG_ORDERED_TABLE, key("log-control"), value(290)),
        ExpectedAction::upsert(BTREE_TABLE, key("btree-control"), value(390)),
    ]
}

fn deleted_keys() -> Vec<(&'static str, Binary)> {
    vec![
        (MEMORY_TABLE, key("ordinary-memory")),
        (MEMORY_TABLE, key("ordinary-memory-repeat")),
        (MEMORY_TABLE, key("dirty-memory")),
        (MEMORY_TABLE, key("dirty-memory-private")),
        (LOG_ORDERED_TABLE, key("ordinary-log")),
        (LOG_ORDERED_TABLE, key("dirty-log")),
        (BTREE_TABLE, maximum_key()),
        (BTREE_TABLE, key("ordinary-btree-shared")),
        (BTREE_TABLE, key("ordinary-btree-private")),
        (BTREE_TABLE, key("dirty-btree")),
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

fn delete_item(table: &str, key: Binary, value: Option<Binary>) -> TableKV {
    TableKV::new(Atom::from(table), key, value)
}

fn table_meta(table_type: KVDBTableType, persistence: bool) -> KVTableMeta {
    KVTableMeta::new(table_type, persistence, EnumType::Bin, EnumType::Bin)
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
        "pi_db_root_delete_{label}_{}_{}",
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
