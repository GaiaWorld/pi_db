//! 普通单层多表事务在首个、中间和最后叶子冲突后的真实 rollback 闭环专项。
//!
//! 本 target 使用真实 4-worker runtime、`Transaction2PcManager`、`CommitLogger`、Memory、
//! LogOrdered、Btree 和文件系统。每轮 stale 根都按相同首次触表顺序写三表，并分别让 Memory、
//! LogOrdered、Btree 成为唯一冲突叶子。测试严格区分普通协议的首个 `Conflicts` 与版本协议的
//! `AllConflicts`，并检查失败前各叶子的 Prepared/PrepareFailed/Inited 状态。
//!
//! 每表使用 evidence/probe 两个原先不存在的 Key：evidence 保留 rollback 未发布、未落盘证据；
//! probe 由 rollback 后的新根复用并成功提交，证明前序 Prepared 预留已经释放。`recover` 等待七个
//! 合法根 WAL 全部确认；两个 data-only 进程移走原 WAL 后以最终数据状态作为硬门禁。Memory 没有
//! 独立数据文件，LogOrdered/Btree 必须恢复 winner evidence 和 retry probe 的精确值。
//!
//! 冻结方案、门禁和实际证据见
//! `docs/ORDINARY_MULTI_TABLE_CONFLICT_ROLLBACK_ACCEPTANCE.md#ordinary-multi-table-conflict-rollback-index`。

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
    AsyncCommitLog, ErrorLevel, Transaction2Pc, TransactionTree, UnitTransaction,
};
use pi_atom::Atom;
use pi_db::{
    db::KVDBTransaction,
    tables::TableKV,
    Binary, KVDBTableType, KVTableTrError,
};

use key_version_support::{
    BTREE_TABLE, Fixture, LOG_ORDERED_TABLE, MEMORY_TABLE, RealTransaction, TestResult,
    build_database, create_active_tables, encode_usize, expect_binary, expect_eq,
    query_ordinary, read_only_transaction, run_on_runtime, table_meta, writable_transaction,
};

const TEST_NAME: &str = "test_ordinary_multi_table_conflict_rollback";
const PHASE_ENV: &str = "PI_DB_ORDINARY_MULTI_TABLE_CONFLICT_PHASE";
const ROOT_ENV: &str = "PI_DB_ORDINARY_MULTI_TABLE_CONFLICT_ROOT";
const ARCHIVED_WAL_DIR: &str = "confirmed-root-wal";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(120);
const SETUP_TIMEOUT: Duration = Duration::from_secs(30);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(100);
const DATA_ONLY_TIMEOUT: Duration = Duration::from_secs(30);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(80);
const EXPECTED_WAL_COUNT: usize = 7;
const EXPECTED_SETUP_CONFIRMED: usize = 1;

#[test]
fn test_ordinary_multi_table_conflict_rollback() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV)
                .expect("ordinary multi-table conflict child phase must receive its root path"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("ordinary multi-table conflict phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root();
    fs::create_dir_all(&root)
        .expect("creating ordinary multi-table conflict root must succeed");
    for phase in ["setup", "recover", "inspect-data-only", "inspect-data-only-again"] {
        if let Err(error) = run_phase_process(&root, phase, PROCESS_TIMEOUT) {
            panic!(
                "ordinary multi-table conflict failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root,
            );
        }
    }
    fs::remove_dir_all(&root)
        .expect("cleaning ordinary multi-table conflict root must succeed");
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
        "inspect-data-only" => {
            let root = root.to_path_buf();
            run_on_runtime(DATA_ONLY_TIMEOUT, move |rt| async move {
                phase_inspect_data_only(rt, root, true).await
            })
        },
        "inspect-data-only-again" => {
            let root = root.to_path_buf();
            run_on_runtime(DATA_ONLY_TIMEOUT, move |rt| async move {
                phase_inspect_data_only(rt, root, false).await
            })
        },
        other => Err(format!("unknown ordinary multi-table conflict phase: {other}")),
    }
}

async fn phase_setup(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    create_active_tables(&fixture).await?;
    assert_table_definitions(&fixture, "setup").await?;
    expect_eq("setup DDL WAL append count",
              &fixture.logger.append_total_count(),
              &1usize)?;

    let scenarios = conflict_scenarios();
    for scenario in &scenarios {
        run_conflict_scenario(&fixture, scenario).await?;
    }
    assert_live_values(&fixture, &scenarios, "setup final").await?;
    expect_eq("setup manager produced/consumed balance",
              &fixture.tr_manager.produced_transaction_total(),
              &fixture.tr_manager.consumed_transaction_total())?;
    expect_eq("setup manager active roots",
              &fixture.tr_manager.transaction_len(),
              &0usize)?;

    // 只有 Memory-only winner 能立即完成根确认；其余合法根等待真实 60 秒表 collector。
    wait_for_wal_state(
        &rt,
        &fixture,
        EXPECTED_WAL_COUNT,
        EXPECTED_SETUP_CONFIRMED,
        EXPECTED_WAL_COUNT - EXPECTED_SETUP_CONFIRMED,
        Duration::from_secs(3),
        "setup",
    ).await?;
    expect_eq("setup nonempty .bak count",
              &nonempty_bak_count(&root.join("root-wal"))?,
              &0usize)
}

async fn run_conflict_scenario(
    fixture: &Fixture,
    scenario: &ConflictScenario,
) -> TestResult<()> {
    assert_scenario_values(fixture,
                           scenario,
                           false,
                           false,
                           &format!("{} initial", scenario.label)).await?;
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();

    let stale = writable_transaction(&fixture.db, &format!("{} stale", scenario.label))?;
    stale
        .upsert(stale_actions(scenario))
        .await
        .map_err(|error| format!("{} stale actions failed: {error:?}", scenario.label))?;
    expect_eq(&format!("{} stale direct child count", scenario.label),
              &stale.children_len(),
              &3usize)?;
    let stale_children: Vec<RealTransaction> = stale.to_children().collect();
    assert_direct_leaf_order(&stale_children, &format!("{} stale", scenario.label))?;

    let target = &scenario.tables[scenario.conflict_index];
    let winner = writable_transaction(&fixture.db, &format!("{} winner", scenario.label))?;
    winner
        .upsert(vec![TableKV::new(
            Atom::from(target.table),
            target.evidence_key.clone(),
            Some(target.winner_value.clone()),
        )])
        .await
        .map_err(|error| format!("{} winner action failed: {error:?}", scenario.label))?;
    let winner_prepare = winner
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("{} winner prepare failed: {error:?}", scenario.label))?;
    winner
        .commit_modified(winner_prepare)
        .await
        .map_err(|error| format!("{} winner commit failed: {error:?}", scenario.label))?;
    expect_eq(&format!("{} winner committed status", scenario.label),
              &winner.get_status(),
              &Transaction2PcStatus::Commited)?;
    expect_eq(&format!("{} winner-only WAL increment", scenario.label),
              &fixture.logger.append_total_count(),
              &(append_before + 1))?;

    let conflict = stale
        .prepare_modified_conflicts()
        .await
        .expect_err("ordinary stale multi-table transaction must conflict");
    assert_first_conflict(&conflict,
                          target.table,
                          &target.evidence_key,
                          scenario.label)?;
    expect_eq(&format!("{} stale root prepare-failed status", scenario.label),
              &stale.get_status(),
              &Transaction2PcStatus::PrepareFailed)?;
    let stale_tid = stale
        .get_transaction_uid()
        .ok_or_else(|| format!("{} stale root has no TID", scenario.label))?;
    let stale_cid = stale
        .get_commit_uid()
        .ok_or_else(|| format!("{} stale root has no CID", scenario.label))?;
    assert_failed_child_states(scenario,
                               &stale_children,
                               &stale_tid,
                               &stale_cid)?;
    expect_eq(&format!("{} produced before rollback", scenario.label),
              &fixture.tr_manager.produced_transaction_total(),
              &(produced_before + 2))?;
    expect_eq(&format!("{} consumed before rollback", scenario.label),
              &fixture.tr_manager.consumed_transaction_total(),
              &(consumed_before + 1))?;
    expect_eq(&format!("{} active stale root", scenario.label),
              &fixture.tr_manager.transaction_len(),
              &1usize)?;
    expect_eq(&format!("{} rejected stale appended no WAL", scenario.label),
              &fixture.logger.append_total_count(),
              &(append_before + 1))?;
    assert_scenario_values(fixture,
                           scenario,
                           true,
                           false,
                           &format!("{} before rollback", scenario.label)).await?;

    stale
        .rollback_modified()
        .await
        .map_err(|error| format!("{} stale rollback failed: {error:?}", scenario.label))?;
    expect_eq(&format!("{} stale root rollback status", scenario.label),
              &stale.get_status(),
              &Transaction2PcStatus::Rollbacked)?;
    for (index, child) in stale_children.iter().enumerate() {
        expect_eq(&format!("{} stale child {index} rollback status", scenario.label),
                  &child.get_status(),
                  &Transaction2PcStatus::Rollbacked)?;
    }
    expect_eq(&format!("{} produced after rollback", scenario.label),
              &fixture.tr_manager.produced_transaction_total(),
              &(produced_before + 2))?;
    expect_eq(&format!("{} consumed after rollback", scenario.label),
              &fixture.tr_manager.consumed_transaction_total(),
              &(consumed_before + 2))?;
    expect_eq(&format!("{} active roots after rollback", scenario.label),
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq(&format!("{} rollback appended no WAL", scenario.label),
              &fixture.logger.append_total_count(),
              &(append_before + 1))?;
    assert_scenario_values(fixture,
                           scenario,
                           true,
                           false,
                           &format!("{} after rollback", scenario.label)).await?;

    // 保持旧根及全部叶子 Arc 存活时创建新根，证明 cleanup 不依赖失败事务先析构。
    let retry = writable_transaction(&fixture.db, &format!("{} retry", scenario.label))?;
    retry
        .upsert(retry_actions(scenario))
        .await
        .map_err(|error| format!("{} retry actions failed: {error:?}", scenario.label))?;
    let retry_children: Vec<RealTransaction> = retry.to_children().collect();
    assert_direct_leaf_order(&retry_children, &format!("{} retry", scenario.label))?;
    let retry_prepare = retry
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("{} retry prepare failed: {error:?}", scenario.label))?;
    if retry_prepare.len() <= 16 {
        return Err(format!(
            "{} retry prepare must contain three table WAL actions, observed {} bytes",
            scenario.label,
            retry_prepare.len(),
        ));
    }
    let retry_tid = retry
        .get_transaction_uid()
        .ok_or_else(|| format!("{} retry root has no TID", scenario.label))?;
    let retry_cid = retry
        .get_commit_uid()
        .ok_or_else(|| format!("{} retry root has no CID", scenario.label))?;
    if retry_tid == stale_tid || retry_cid == stale_cid {
        return Err(format!(
            "{} retry must use fresh TID/CID: stale=({stale_tid:?},{stale_cid:?}), retry=({retry_tid:?},{retry_cid:?})",
            scenario.label,
        ));
    }
    assert_prepared_children(&retry_children,
                             &retry_tid,
                             &retry_cid,
                             &format!("{} retry", scenario.label))?;
    retry
        .commit_modified(retry_prepare)
        .await
        .map_err(|error| format!("{} retry commit failed: {error:?}", scenario.label))?;
    expect_eq(&format!("{} retry root committed", scenario.label),
              &retry.get_status(),
              &Transaction2PcStatus::Commited)?;
    for (index, child) in retry_children.iter().enumerate() {
        expect_eq(&format!("{} retry child {index} committed", scenario.label),
                  &child.get_status(),
                  &Transaction2PcStatus::Commited)?;
    }
    expect_eq(&format!("{} final produced count", scenario.label),
              &fixture.tr_manager.produced_transaction_total(),
              &(produced_before + 3))?;
    expect_eq(&format!("{} final consumed count", scenario.label),
              &fixture.tr_manager.consumed_transaction_total(),
              &(consumed_before + 3))?;
    expect_eq(&format!("{} final active roots", scenario.label),
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq(&format!("{} final WAL increment", scenario.label),
              &fixture.logger.append_total_count(),
              &(append_before + 2))?;
    assert_scenario_values(fixture,
                           scenario,
                           true,
                           true,
                           &format!("{} after retry", scenario.label)).await?;

    drop(retry_children);
    drop(retry);
    drop(stale_children);
    drop(stale);
    drop(winner);
    Ok(())
}

async fn phase_recover(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let wal_path = root.join("root-wal");
    let bak_before = nonempty_bak_count(&wal_path)?;
    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    assert_table_definitions(&fixture, "recovered").await?;
    let scenarios = conflict_scenarios();
    assert_live_values(&fixture, &scenarios, "replayed before confirm").await?;

    wait_for_wal_state(&rt,
                       &fixture,
                       EXPECTED_WAL_COUNT,
                       EXPECTED_WAL_COUNT,
                       0,
                       CONFIRM_TIMEOUT,
                       "recovery").await?;
    assert_live_values(&fixture, &scenarios, "replayed after confirm").await?;
    expect_eq("recovery manager produced/consumed balance",
              &fixture.tr_manager.produced_transaction_total(),
              &fixture.tr_manager.consumed_transaction_total())?;
    expect_eq("recovery manager active roots",
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq("recovery Btree overlay drained",
              &fixture.db.table_cache_size(&Atom::from(BTREE_TABLE)).await,
              &Some(0u64))?;

    let bak_after = nonempty_bak_count(&wal_path)?;
    if bak_after <= bak_before {
        return Err(format!(
            "ordinary conflict recovery did not add a confirmed .bak checkpoint: before={bak_before}, after={bak_after}",
        ));
    }
    let active = active_file_sizes(&wal_path)?;
    if active.iter().any(|(_, len)| *len > 0) {
        return Err(format!("nonempty active WAL remains after conflict recovery: {active:?}"));
    }
    Ok(())
}

async fn phase_inspect_data_only(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
    archive_wal: bool,
) -> TestResult<()> {
    let wal_path = root.join("root-wal");
    let archived_wal = root.join(ARCHIVED_WAL_DIR);
    if archive_wal {
        if archived_wal.exists() {
            return Err(format!("archived WAL path unexpectedly exists: {archived_wal:?}"));
        }
        fs::rename(&wal_path, &archived_wal)
            .map_err(|error| format!("archiving conflict recovery WAL failed: {error}"))?;
    } else {
        if !archived_wal.exists() {
            return Err("second conflict data-only start cannot find archived WAL".to_owned());
        }
        let active = active_file_sizes(&wal_path)?;
        if active.iter().any(|(_, len)| *len > 0) {
            return Err(format!("first conflict data-only start produced nonempty WAL: {active:?}"));
        }
    }
    if nonempty_bak_count(&archived_wal)? == 0 {
        return Err("archived conflict WAL contains no nonempty .bak checkpoint".to_owned());
    }

    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    let label = if archive_wal { "data-only" } else { "second data-only" };
    assert_table_definitions(&fixture, label).await?;
    assert_data_only_values(&fixture, &conflict_scenarios(), label).await?;
    expect_eq(&format!("{label} WAL append count"),
              &fixture.logger.append_total_count(),
              &0usize)?;
    expect_eq(&format!("{label} WAL confirm count"),
              &fixture.logger.confirm_total_count(),
              &0usize)?;
    expect_eq(&format!("{label} WAL waiting count"),
              &fixture.logger.waiting_confirm_count().await,
              &0usize)?;
    expect_eq(&format!("{label} manager produced/consumed balance"),
              &fixture.tr_manager.produced_transaction_total(),
              &fixture.tr_manager.consumed_transaction_total())?;
    expect_eq(&format!("{label} manager active roots"),
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq(&format!("{label} Btree overlay size"),
              &fixture.db.table_cache_size(&Atom::from(BTREE_TABLE)).await,
              &Some(0u64))
}

#[derive(Clone)]
struct ConflictTableCase {
    table: &'static str,
    evidence_key: Binary,
    probe_key: Binary,
    stale_evidence_value: Binary,
    stale_probe_value: Binary,
    winner_value: Binary,
    retry_value: Binary,
}

struct ConflictScenario {
    label: &'static str,
    conflict_index: usize,
    tables: [ConflictTableCase; 3],
}

fn conflict_scenarios() -> Vec<ConflictScenario> {
    vec![
        build_scenario("first Memory conflict", 0, 10_000),
        build_scenario("middle LogOrdered conflict", 1, 20_000),
        build_scenario("last Btree conflict", 2, 30_000),
    ]
}

fn build_scenario(
    label: &'static str,
    conflict_index: usize,
    base: usize,
) -> ConflictScenario {
    ConflictScenario {
        label,
        conflict_index,
        tables: [
            build_table_case(MEMORY_TABLE, base),
            build_table_case(LOG_ORDERED_TABLE, base + 100),
            build_table_case(BTREE_TABLE, base + 200),
        ],
    }
}

fn build_table_case(table: &'static str, base: usize) -> ConflictTableCase {
    ConflictTableCase {
        table,
        evidence_key: encode_usize(base + 1),
        probe_key: encode_usize(base + 2),
        stale_evidence_value: encode_usize(base + 11),
        stale_probe_value: encode_usize(base + 12),
        winner_value: encode_usize(base + 21),
        retry_value: encode_usize(base + 22),
    }
}

fn stale_actions(scenario: &ConflictScenario) -> Vec<TableKV> {
    let mut actions = Vec::with_capacity(6);
    for table in &scenario.tables {
        actions.push(TableKV::new(
            Atom::from(table.table),
            table.evidence_key.clone(),
            Some(table.stale_evidence_value.clone()),
        ));
        actions.push(TableKV::new(
            Atom::from(table.table),
            table.probe_key.clone(),
            Some(table.stale_probe_value.clone()),
        ));
    }
    actions
}

fn retry_actions(scenario: &ConflictScenario) -> Vec<TableKV> {
    scenario
        .tables
        .iter()
        .map(|table| {
            TableKV::new(
                Atom::from(table.table),
                table.probe_key.clone(),
                Some(table.retry_value.clone()),
            )
        })
        .collect()
}

fn assert_first_conflict(
    error: &KVTableTrError,
    table: &str,
    key: &Binary,
    label: &str,
) -> TestResult<()> {
    if error.is_all_conflicts()
        || !error.is_conflicts()
        || !matches!(error.level(), ErrorLevel::Normal) {
        return Err(format!(
            "{label}: expected ordinary Conflicts(Normal), observed {error:?}",
        ));
    }
    let actual = error
        .conflicts()
        .ok_or_else(|| format!("{label}: conflict accessor returned None"))?;
    expect_eq(&format!("{label} conflict table"), &actual.0.as_str(), &table)?;
    expect_eq(&format!("{label} conflict key"), actual.1, key)?;
    expect_eq(&format!("{label} all-conflicts accessor"),
              &error.all_conflicts().is_none(),
              &true)
}

fn assert_failed_child_states(
    scenario: &ConflictScenario,
    children: &[RealTransaction],
    transaction_uid: &pi_guid::Guid,
    commit_uid: &pi_guid::Guid,
) -> TestResult<()> {
    for (index, child) in children.iter().enumerate() {
        expect_eq(&format!("{} child {index} TID", scenario.label),
                  &child.get_transaction_uid(),
                  &Some(transaction_uid.clone()))?;
        expect_eq(&format!("{} child {index} CID", scenario.label),
                  &child.get_commit_uid(),
                  &Some(commit_uid.clone()))?;
        let expected = if index < scenario.conflict_index {
            Transaction2PcStatus::Prepared
        } else if index == scenario.conflict_index {
            Transaction2PcStatus::PrepareFailed
        } else {
            Transaction2PcStatus::Inited
        };
        expect_eq(&format!("{} child {index} failed-prepare state", scenario.label),
                  &child.get_status(),
                  &expected)?;
    }
    Ok(())
}

fn assert_prepared_children(
    children: &[RealTransaction],
    transaction_uid: &pi_guid::Guid,
    commit_uid: &pi_guid::Guid,
    label: &str,
) -> TestResult<()> {
    for (index, child) in children.iter().enumerate() {
        expect_eq(&format!("{label} child {index} TID"),
                  &child.get_transaction_uid(),
                  &Some(transaction_uid.clone()))?;
        expect_eq(&format!("{label} child {index} CID"),
                  &child.get_commit_uid(),
                  &Some(commit_uid.clone()))?;
        expect_eq(&format!("{label} child {index} prepared"),
                  &child.get_status(),
                  &Transaction2PcStatus::Prepared)?;
    }
    Ok(())
}

fn assert_direct_leaf_order(children: &[RealTransaction], label: &str) -> TestResult<()> {
    expect_eq(&format!("{label} direct leaf count"), &children.len(), &3usize)?;
    let observed = children
        .iter()
        .map(|child| {
            match child {
                KVDBTransaction::MemOrdTabTr(_) => "Memory",
                KVDBTransaction::LogOrdTabTr(_) => "LogOrdered",
                KVDBTransaction::BtreeOrdTabTr(_) => "Btree",
                KVDBTransaction::MetaTabTr(_) => "Meta",
                KVDBTransaction::LogWTabTr(_) => "LogWrite",
                KVDBTransaction::RootTr(_) => "Root",
            }
        })
        .collect::<Vec<_>>();
    expect_eq(&format!("{label} direct leaf order"),
              &observed,
              &vec!["Memory", "LogOrdered", "Btree"])?;
    for (index, child) in children.iter().enumerate() {
        expect_eq(&format!("{label} child {index} is unit"),
                  &child.is_unit(),
                  &true)?;
        expect_eq(&format!("{label} child {index} is not tree"),
                  &child.is_tree(),
                  &false)?;
    }
    Ok(())
}

async fn assert_scenario_values(
    fixture: &Fixture,
    scenario: &ConflictScenario,
    winner_committed: bool,
    probes_committed: bool,
    label: &str,
) -> TestResult<()> {
    for (index, table) in scenario.tables.iter().enumerate() {
        let evidence = query_ordinary(
            &fixture.db,
            table.table,
            table.evidence_key.clone(),
            &format!("{label} evidence query for {}", table.table),
        ).await?;
        let expected_evidence = if winner_committed && index == scenario.conflict_index {
            Some(&table.winner_value)
        } else {
            None
        };
        expect_binary(&format!("{label} evidence value for {}", table.table),
                      evidence.as_ref(),
                      expected_evidence)?;

        let probe = query_ordinary(
            &fixture.db,
            table.table,
            table.probe_key.clone(),
            &format!("{label} probe query for {}", table.table),
        ).await?;
        let expected_probe = if probes_committed {
            Some(&table.retry_value)
        } else {
            None
        };
        expect_binary(&format!("{label} probe value for {}", table.table),
                      probe.as_ref(),
                      expected_probe)?;
    }
    Ok(())
}

async fn assert_live_values(
    fixture: &Fixture,
    scenarios: &[ConflictScenario],
    label: &str,
) -> TestResult<()> {
    for scenario in scenarios {
        assert_scenario_values(fixture,
                               scenario,
                               true,
                               true,
                               &format!("{label} {}", scenario.label)).await?;
    }
    Ok(())
}

async fn assert_data_only_values(
    fixture: &Fixture,
    scenarios: &[ConflictScenario],
    label: &str,
) -> TestResult<()> {
    for scenario in scenarios {
        for (index, table) in scenario.tables.iter().enumerate() {
            let evidence = query_ordinary(
                &fixture.db,
                table.table,
                table.evidence_key.clone(),
                &format!("{label} evidence query for {}", table.table),
            ).await?;
            let expected_evidence = if table.table == MEMORY_TABLE {
                None
            } else if index == scenario.conflict_index {
                Some(&table.winner_value)
            } else {
                None
            };
            expect_binary(
                &format!("{label} evidence value for {}", table.table),
                evidence.as_ref(),
                expected_evidence,
            )?;

            let probe = query_ordinary(
                &fixture.db,
                table.table,
                table.probe_key.clone(),
                &format!("{label} probe query for {}", table.table),
            ).await?;
            let expected_probe = if table.table == MEMORY_TABLE {
                None
            } else {
                Some(&table.retry_value)
            };
            expect_binary(&format!("{label} probe value for {}", table.table),
                          probe.as_ref(),
                          expected_probe)?;
        }
    }
    Ok(())
}

async fn assert_table_definitions(fixture: &Fixture, label: &str) -> TestResult<()> {
    expect_eq(&format!("{label} registered table count"),
              &fixture.db.table_size().await,
              &4usize)?;
    let verifier = read_only_transaction(&fixture.db, &format!("{label} table definitions"))?;
    for (name, table_type) in [
        (MEMORY_TABLE, KVDBTableType::MemOrdTab),
        (LOG_ORDERED_TABLE, KVDBTableType::LogOrdTab),
        (BTREE_TABLE, KVDBTableType::BtreeOrdTab),
    ] {
        expect_eq(&format!("{label} table definition for {name}"),
                  &verifier.table_meta(Atom::from(name)).await,
                  &Some(table_meta(table_type, true)))?;
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
                "{label} WAL advanced beyond expected state: appended={appended}, confirmed={confirmed}, waiting={waiting}",
            ));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{label} WAL did not reach appended={expected_appended}, confirmed={expected_confirmed}, waiting={expected_waiting} before {timeout:?}; observed appended={appended}, confirmed={confirmed}, waiting={waiting}",
            ));
        }
        rt.timeout(25).await;
    }
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
            *len > 0 && file.extension().and_then(|extension| extension.to_str()) == Some("bak")
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
        .map_err(|error| format!("locating ordinary conflict test executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase)
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| format!("spawning ordinary conflict phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("ordinary conflict phase {phase} exited with {status}"))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking ordinary conflict child status failed: {error}"))? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("ordinary conflict child exceeded {timeout:?}"));
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn unique_temp_root() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must not precede UNIX_EPOCH")
        .as_nanos();
    env::temp_dir().join(format!(
        "pi_db_ordinary_multi_table_conflict_{}_{}",
        std::process::id(),
        nanos,
    ))
}
