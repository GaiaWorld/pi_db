//! 普通 Memory/LogOrdered/Btree 六种首次触表顺序与交叉预提交真实专项。
//!
//! 本 target 先逐一提交三表的全部六种排列，再让六个排列事务对同一组三表新 Key 并发
//! prepare。交叉预留允许零或一个 winner；其余结果必须是普通冲突并完整 rollback。随后由全新
//! 根写入确定 final 值，证明预留释放。setup 在确认前退出，recover 使用生产 `try_repair`，最后
//! 两次移走根 WAL 的 data-only 冷启动以最终数据作为硬门禁。
//!
//! 冻结方案、允许的调度集合和非目标见
//! `docs/ORDINARY_MULTI_TABLE_ORDERING_ACCEPTANCE.md#ordinary-multi-table-ordering-index`。

mod key_version_support;

use std::{
    env,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
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
use pi_guid::Guid;

use key_version_support::{
    BTREE_TABLE, Fixture, LOG_ORDERED_TABLE, MEMORY_TABLE, RealTransaction, TestResult,
    build_database, create_active_tables, encode_usize, expect_binary, expect_eq,
    read_only_transaction, run_on_runtime, table_meta, writable_transaction,
};

const TEST_NAME: &str = "test_ordinary_multi_table_ordering";
const PHASE_ENV: &str = "PI_DB_ORDINARY_MULTI_TABLE_ORDERING_PHASE";
const ROOT_ENV: &str = "PI_DB_ORDINARY_MULTI_TABLE_ORDERING_ROOT";
const ARCHIVED_WAL_DIR: &str = "confirmed-root-wal";
const OUTCOME_MANIFEST: &str = "ordering-winner-count.bin";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(180);
const SETUP_TIMEOUT: Duration = Duration::from_secs(60);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(130);
const DATA_ONLY_TIMEOUT: Duration = Duration::from_secs(30);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(100);

const TABLES: [&str; 3] = [MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE];
const PERMUTATIONS: [[usize; 3]; 6] = [
    [0, 1, 2],
    [0, 2, 1],
    [1, 0, 2],
    [1, 2, 0],
    [2, 0, 1],
    [2, 1, 0],
];
const BASE_WAL_COUNT: usize = 8;
const PERMUTATION_KEY_BASE: usize = 0x7100_0000;
const PERMUTATION_VALUE_BASE: usize = 0x7110_0000;
const CONTENDED_KEY_BASE: usize = 0x7200_0000;
const CONTENDER_VALUE_BASE: usize = 0x7300_0000;
const FINAL_VALUE_BASE: usize = 0x7400_0000;

#[test]
fn test_ordinary_multi_table_ordering() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV)
                .expect("ordinary multi-table ordering child phase must receive its root path"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("ordinary multi-table ordering phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root();
    fs::create_dir_all(&root)
        .expect("creating ordinary multi-table ordering root must succeed");
    for phase in ["setup", "recover", "inspect-data-only", "inspect-data-only-again"] {
        if let Err(error) = run_phase_process(&root, phase, PROCESS_TIMEOUT) {
            panic!(
                "ordinary multi-table ordering failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root,
            );
        }
    }
    fs::remove_dir_all(&root)
        .expect("cleaning ordinary multi-table ordering root must succeed");
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
        other => Err(format!("unknown ordinary multi-table ordering phase: {other}")),
    }
}

async fn phase_setup(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let caller_rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(2)
        .build();
    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    create_active_tables(&fixture).await?;
    assert_table_definitions(&fixture, "setup").await?;
    expect_eq("setup DDL WAL append count",
              &fixture.logger.append_total_count(),
              &1usize)?;

    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let mut identities = Vec::new();
    for (index, order) in PERMUTATIONS.iter().enumerate() {
        let identity = commit_independent_permutation(&fixture, index, *order).await?;
        record_unique_identity(&mut identities,
                               identity,
                               &format!("independent permutation {index}"))?;
    }
    expect_eq("six independent roots produced",
              &(fixture.tr_manager.produced_transaction_total() - produced_before),
              &PERMUTATIONS.len())?;
    expect_eq("six independent roots consumed",
              &(fixture.tr_manager.consumed_transaction_total() - consumed_before),
              &PERMUTATIONS.len())?;
    expect_eq("six independent roots WAL count",
              &fixture.logger.append_total_count(),
              &7usize)?;
    assert_permutation_values(&fixture, true, "setup independent permutations").await?;
    assert_contended_values(&fixture,
                            true,
                            ContendedExpectation::Missing,
                            "setup before contention").await?;

    let cross = run_cross_order_contention(&rt,
                                           &caller_rt,
                                           &fixture).await?;
    println!("ordinary cross-order prepare outcome: winners={}, conflicts={}",
             cross.winner_count,
             PERMUTATIONS.len() - cross.winner_count);
    for identity in cross.identities {
        record_unique_identity(&mut identities, identity, "cross-order contender")?;
    }

    let final_identity = commit_final_retry(&fixture).await?;
    record_unique_identity(&mut identities, final_identity, "final retry")?;
    assert_permutation_values(&fixture, true, "setup final permutations").await?;
    assert_contended_values(&fixture,
                            true,
                            ContendedExpectation::Final,
                            "setup final contention values").await?;

    let expected_wal = BASE_WAL_COUNT + cross.winner_count;
    write_winner_manifest(&root, cross.winner_count)?;
    expect_eq("setup total produced roots",
              &(fixture.tr_manager.produced_transaction_total() - produced_before),
              &(PERMUTATIONS.len() * 2 + 1))?;
    expect_eq("setup total consumed roots",
              &(fixture.tr_manager.consumed_transaction_total() - consumed_before),
              &(PERMUTATIONS.len() * 2 + 1))?;
    expect_eq("setup manager active roots",
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    wait_for_wal_state(&rt,
                       &fixture,
                       expected_wal,
                       0,
                       expected_wal,
                       Duration::from_secs(3),
                       "setup").await?;
    expect_eq("setup nonempty .bak count",
              &nonempty_bak_count(&root.join("root-wal"))?,
              &0usize)
}

async fn commit_independent_permutation(
    fixture: &Fixture,
    index: usize,
    order: [usize; 3],
) -> TestResult<Identity> {
    let label = format!("independent permutation {index}");
    let transaction = writable_transaction(&fixture.db, &label)?;
    transaction
        .upsert(permutation_actions(index, order))
        .await
        .map_err(|error| format!("{label} actions failed: {error:?}"))?;
    let children = assert_direct_leaf_order(&transaction, order, &label)?;
    expect_eq(&format!("{label} persistence aggregation"),
              &transaction.is_require_persistence(),
              &true)?;

    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let token = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("{label} prepare failed: {error:?}"))?;
    if token.len() <= 16 {
        return Err(format!(
            "{label} prepare token must contain three table actions, observed {} bytes",
            token.len(),
        ));
    }
    expect_eq(&format!("{label} prepared root state"),
              &transaction.get_status(),
              &Transaction2PcStatus::Prepared)?;
    let identity = transaction_identity(&transaction, &label)?;
    assert_children_identity_and_state(&children,
                                       &identity,
                                       Transaction2PcStatus::Prepared,
                                       &label)?;
    assert_single_permutation_value(fixture,
                                    index,
                                    false,
                                    &format!("{label} before commit")).await?;
    expect_eq(&format!("{label} prepare appended no WAL"),
              &fixture.logger.append_total_count(),
              &append_before)?;

    transaction
        .commit_modified(token)
        .await
        .map_err(|error| format!("{label} commit failed: {error:?}"))?;
    expect_eq(&format!("{label} committed root state"),
              &transaction.get_status(),
              &Transaction2PcStatus::Commited)?;
    assert_children_identity_and_state(&children,
                                       &identity,
                                       Transaction2PcStatus::Commited,
                                       &label)?;
    expect_eq(&format!("{label} produced increment"),
              &fixture.tr_manager.produced_transaction_total(),
              &(produced_before + 1))?;
    expect_eq(&format!("{label} consumed increment"),
              &fixture.tr_manager.consumed_transaction_total(),
              &(consumed_before + 1))?;
    expect_eq(&format!("{label} active roots"),
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq(&format!("{label} WAL increment"),
              &fixture.logger.append_total_count(),
              &(append_before + 1))?;
    assert_single_permutation_value(fixture,
                                    index,
                                    true,
                                    &format!("{label} after commit")).await?;
    Ok(identity)
}

async fn run_cross_order_contention(
    db_rt: &MultiTaskRuntime<()>,
    caller_rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
) -> TestResult<CrossOutcome> {
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let mut attempts = Vec::with_capacity(PERMUTATIONS.len());
    for (index, order) in PERMUTATIONS.iter().enumerate() {
        let label = format!("cross-order contender {index}");
        let transaction = writable_transaction(&fixture.db, &label)?;
        transaction
            .upsert(contended_actions(index, *order))
            .await
            .map_err(|error| format!("{label} actions failed: {error:?}"))?;
        assert_direct_leaf_order(&transaction, *order, &label)?;
        attempts.push(ActionedAttempt {
            index,
            order: *order,
            transaction,
        });
    }
    assert_contended_values(fixture,
                            true,
                            ContendedExpectation::Missing,
                            "cross-order actioned values").await?;

    let results = collect_prepare_results(db_rt, caller_rt, attempts).await?;
    let mut winners = Vec::new();
    let mut losers = Vec::new();
    let mut identities = Vec::with_capacity(PERMUTATIONS.len());
    for result in results {
        let label = format!("cross-order contender {}", result.index);
        let identity = transaction_identity(&result.transaction, &label)?;
        record_unique_identity(&mut identities, identity.clone(), &label)?;
        match result.prepare {
            Ok(token) => {
                if token.len() <= 16 {
                    return Err(format!(
                        "{label} prepare token must contain three table actions, observed {} bytes",
                        token.len(),
                    ));
                }
                expect_eq(&format!("{label} prepared root state"),
                          &result.transaction.get_status(),
                          &Transaction2PcStatus::Prepared)?;
                let children = result.transaction.to_children().collect::<Vec<_>>();
                assert_children_identity_and_state(&children,
                                                   &identity,
                                                   Transaction2PcStatus::Prepared,
                                                   &label)?;
                winners.push(PreparedWinner {
                    index: result.index,
                    transaction: result.transaction,
                    token,
                });
            },
            Err(error) => {
                assert_cross_order_conflict(&error,
                                            result.order,
                                            &result.transaction,
                                            &identity,
                                            &label)?;
                losers.push((result.index, result.transaction));
            },
        }
    }

    if winners.len() > 1 {
        return Err(format!(
            "cross-order prepare produced {} winners for fully overlapping keys",
            winners.len(),
        ));
    }
    expect_eq("cross-order classified attempt count",
              &(winners.len() + losers.len()),
              &PERMUTATIONS.len())?;
    expect_eq("cross-order prepare produced roots",
              &(fixture.tr_manager.produced_transaction_total() - produced_before),
              &PERMUTATIONS.len())?;
    expect_eq("cross-order prepare consumed no roots",
              &fixture.tr_manager.consumed_transaction_total(),
              &consumed_before)?;
    expect_eq("cross-order prepared/failed roots remain registered",
              &fixture.tr_manager.transaction_len(),
              &PERMUTATIONS.len())?;
    expect_eq("cross-order prepare appended no WAL",
              &fixture.logger.append_total_count(),
              &append_before)?;
    assert_contended_values(fixture,
                            true,
                            ContendedExpectation::Missing,
                            "cross-order before finish").await?;

    for (index, transaction) in losers {
        let children = transaction.to_children().collect::<Vec<_>>();
        transaction
            .rollback_modified()
            .await
            .map_err(|error| format!("cross-order contender {index} rollback failed: {error:?}"))?;
        expect_eq(&format!("cross-order contender {index} rollback root state"),
                  &transaction.get_status(),
                  &Transaction2PcStatus::Rollbacked)?;
        for (child_index, child) in children.iter().enumerate() {
            expect_eq(&format!("cross-order contender {index} rollback child {child_index}"),
                      &child.get_status(),
                      &Transaction2PcStatus::Rollbacked)?;
        }
    }
    assert_contended_values(fixture,
                            true,
                            ContendedExpectation::Missing,
                            "cross-order after loser rollback").await?;

    let winner_count = winners.len();
    if let Some(winner) = winners.pop() {
        let children = winner.transaction.to_children().collect::<Vec<_>>();
        winner
            .transaction
            .commit_modified(winner.token)
            .await
            .map_err(|error| format!("cross-order winner {} commit failed: {error:?}", winner.index))?;
        expect_eq("cross-order winner root state",
                  &winner.transaction.get_status(),
                  &Transaction2PcStatus::Commited)?;
        for (child_index, child) in children.iter().enumerate() {
            expect_eq(&format!("cross-order winner child {child_index}"),
                      &child.get_status(),
                      &Transaction2PcStatus::Commited)?;
        }
        assert_contended_values(fixture,
                                true,
                                ContendedExpectation::Contender(winner.index),
                                "cross-order winner values").await?;
    } else {
        assert_contended_values(fixture,
                                true,
                                ContendedExpectation::Missing,
                                "cross-order all-conflict values").await?;
    }

    expect_eq("cross-order final produced roots",
              &(fixture.tr_manager.produced_transaction_total() - produced_before),
              &PERMUTATIONS.len())?;
    expect_eq("cross-order final consumed roots",
              &(fixture.tr_manager.consumed_transaction_total() - consumed_before),
              &PERMUTATIONS.len())?;
    expect_eq("cross-order final active roots",
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq("cross-order committed winners are the only WAL writes",
              &(fixture.logger.append_total_count() - append_before),
              &winner_count)?;
    Ok(CrossOutcome {
        winner_count,
        identities,
    })
}

async fn collect_prepare_results(
    db_rt: &MultiTaskRuntime<()>,
    caller_rt: &MultiTaskRuntime<()>,
    attempts: Vec<ActionedAttempt>,
) -> TestResult<Vec<PrepareResult>> {
    let count = attempts.len();
    let (ready_sender, ready_receiver) = bounded(count);
    let (start_sender, start_receiver) = bounded(count);
    let (result_sender, result_receiver) = bounded(count);
    for attempt in attempts {
        let executor = if attempt.index % 2 == 0 { db_rt.clone() } else { caller_rt.clone() };
        let task_ready = ready_sender.clone();
        let task_start = start_receiver.clone();
        let task_result = result_sender.clone();
        executor.spawn(async move {
            let _ = task_ready.send(attempt.index).await;
            if task_start.recv().await.is_err() {
                return;
            }
            let prepare = attempt.transaction.prepare_modified_conflicts().await;
            let _ = task_result.send(PrepareResult {
                index: attempt.index,
                order: attempt.order,
                transaction: attempt.transaction,
                prepare,
            }).await;
        }).map_err(|error| format!("spawning cross-order contender failed: {error:?}"))?;
    }
    drop(ready_sender);
    drop(result_sender);

    let mut ready = vec![false; count];
    for _ in 0..count {
        let index = ready_receiver
            .recv()
            .await
            .map_err(|error| format!("cross-order ready signal missing: {error}"))?;
        if index >= count || ready[index] {
            return Err(format!("invalid or duplicate cross-order ready index: {index}"));
        }
        ready[index] = true;
    }
    for _ in 0..count {
        start_sender
            .send(())
            .await
            .map_err(|error| format!("releasing cross-order prepare barrier failed: {error}"))?;
    }
    drop(start_sender);

    let mut results = Vec::with_capacity(count);
    for _ in 0..count {
        results.push(result_receiver
            .recv()
            .await
            .map_err(|error| format!("cross-order prepare result missing: {error}"))?);
    }
    results.sort_by_key(|result| result.index);
    Ok(results)
}

async fn commit_final_retry(fixture: &Fixture) -> TestResult<Identity> {
    let label = "cross-order final retry";
    let order = [2, 1, 0];
    let transaction = writable_transaction(&fixture.db, label)?;
    transaction
        .upsert(final_actions(order))
        .await
        .map_err(|error| format!("{label} actions failed: {error:?}"))?;
    let children = assert_direct_leaf_order(&transaction, order, label)?;
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let token = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("{label} prepare failed after all prior roots finished: {error:?}"))?;
    if token.len() <= 16 {
        return Err(format!(
            "{label} prepare token must contain three table actions, observed {} bytes",
            token.len(),
        ));
    }
    let identity = transaction_identity(&transaction, label)?;
    assert_children_identity_and_state(&children,
                                       &identity,
                                       Transaction2PcStatus::Prepared,
                                       label)?;
    transaction
        .commit_modified(token)
        .await
        .map_err(|error| format!("{label} commit failed: {error:?}"))?;
    expect_eq("final retry root state",
              &transaction.get_status(),
              &Transaction2PcStatus::Commited)?;
    assert_children_identity_and_state(&children,
                                       &identity,
                                       Transaction2PcStatus::Commited,
                                       label)?;
    expect_eq("final retry produced increment",
              &fixture.tr_manager.produced_transaction_total(),
              &(produced_before + 1))?;
    expect_eq("final retry consumed increment",
              &fixture.tr_manager.consumed_transaction_total(),
              &(consumed_before + 1))?;
    expect_eq("final retry WAL increment",
              &fixture.logger.append_total_count(),
              &(append_before + 1))?;
    expect_eq("final retry active roots",
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    assert_contended_values(fixture,
                            true,
                            ContendedExpectation::Final,
                            "final retry committed values").await?;
    Ok(identity)
}

fn assert_cross_order_conflict(
    error: &KVTableTrError,
    order: [usize; 3],
    transaction: &RealTransaction,
    identity: &Identity,
    label: &str,
) -> TestResult<()> {
    if error.is_all_conflicts()
        || !error.is_conflicts()
        || !matches!(error.level(), ErrorLevel::Normal) {
        return Err(format!("{label}: expected ordinary Conflicts(Normal), observed {error:?}"));
    }
    expect_eq(&format!("{label} failed root state"),
              &transaction.get_status(),
              &Transaction2PcStatus::PrepareFailed)?;
    let (table, key) = error
        .conflicts()
        .ok_or_else(|| format!("{label}: conflict accessor returned None"))?;
    let table_index = TABLES
        .iter()
        .position(|candidate| *candidate == table.as_str())
        .ok_or_else(|| format!("{label}: conflict referenced unexpected table {table:?}"))?;
    expect_eq(&format!("{label} conflict key"),
              key,
              &contended_key(table_index))?;
    expect_eq(&format!("{label} complete conflict accessor"),
              &error.all_conflicts().is_none(),
              &true)?;
    let conflict_position = order
        .iter()
        .position(|candidate| *candidate == table_index)
        .ok_or_else(|| format!("{label}: conflict table is absent from first-touch order"))?;
    let children = transaction.to_children().collect::<Vec<_>>();
    expect_eq(&format!("{label} child count"), &children.len(), &3usize)?;
    for (position, child) in children.iter().enumerate() {
        expect_eq(&format!("{label} child {position} TID"),
                  &child.get_transaction_uid(),
                  &Some(identity.tid.clone()))?;
        expect_eq(&format!("{label} child {position} CID"),
                  &child.get_commit_uid(),
                  &Some(identity.cid.clone()))?;
        let expected = if position < conflict_position {
            Transaction2PcStatus::Prepared
        } else if position == conflict_position {
            Transaction2PcStatus::PrepareFailed
        } else {
            Transaction2PcStatus::Inited
        };
        expect_eq(&format!("{label} child {position} failed-prepare state"),
                  &child.get_status(),
                  &expected)?;
    }
    Ok(())
}

fn assert_direct_leaf_order(
    transaction: &RealTransaction,
    order: [usize; 3],
    label: &str,
) -> TestResult<Vec<RealTransaction>> {
    expect_eq(&format!("{label} direct child count"),
              &transaction.children_len(),
              &3usize)?;
    let children = transaction.to_children().collect::<Vec<_>>();
    let observed = children
        .iter()
        .map(child_table_index)
        .collect::<TestResult<Vec<_>>>()?;
    expect_eq(&format!("{label} direct child first-touch order"),
              &observed,
              &order.to_vec())?;
    for (index, child) in children.iter().enumerate() {
        expect_eq(&format!("{label} child {index} is unit"), &child.is_unit(), &true)?;
        expect_eq(&format!("{label} child {index} is not tree"), &child.is_tree(), &false)?;
    }
    Ok(children)
}

fn child_table_index(child: &RealTransaction) -> TestResult<usize> {
    match child {
        KVDBTransaction::MemOrdTabTr(_) => Ok(0),
        KVDBTransaction::LogOrdTabTr(_) => Ok(1),
        KVDBTransaction::BtreeOrdTabTr(_) => Ok(2),
        KVDBTransaction::MetaTabTr(_) => Err("unexpected Meta child in ordinary ordering tree".to_owned()),
        KVDBTransaction::LogWTabTr(_) => Err("unexpected LogWrite child in ordinary ordering tree".to_owned()),
        KVDBTransaction::RootTr(_) => Err("unexpected Root child in ordinary ordering tree".to_owned()),
    }
}

fn transaction_identity(transaction: &RealTransaction, label: &str) -> TestResult<Identity> {
    let tid = transaction
        .get_transaction_uid()
        .ok_or_else(|| format!("{label} has no transaction UID after prepare"))?;
    let cid = transaction
        .get_commit_uid()
        .ok_or_else(|| format!("{label} has no commit UID after persistent prepare"))?;
    Ok(Identity { tid, cid })
}

fn assert_children_identity_and_state(
    children: &[RealTransaction],
    identity: &Identity,
    status: Transaction2PcStatus,
    label: &str,
) -> TestResult<()> {
    expect_eq(&format!("{label} identity child count"), &children.len(), &3usize)?;
    for (index, child) in children.iter().enumerate() {
        expect_eq(&format!("{label} child {index} TID"),
                  &child.get_transaction_uid(),
                  &Some(identity.tid.clone()))?;
        expect_eq(&format!("{label} child {index} CID"),
                  &child.get_commit_uid(),
                  &Some(identity.cid.clone()))?;
        expect_eq(&format!("{label} child {index} status"),
                  &child.get_status(),
                  &status)?;
    }
    Ok(())
}

fn record_unique_identity(
    identities: &mut Vec<Identity>,
    identity: Identity,
    label: &str,
) -> TestResult<()> {
    if identities.iter().any(|previous| previous.tid == identity.tid) {
        return Err(format!("{label} reused transaction UID {:?}", identity.tid));
    }
    if identities.iter().any(|previous| previous.cid == identity.cid) {
        return Err(format!("{label} reused commit UID {:?}", identity.cid));
    }
    identities.push(identity);
    Ok(())
}

fn permutation_actions(index: usize, order: [usize; 3]) -> Vec<TableKV> {
    order
        .iter()
        .map(|table_index| {
            TableKV::new(
                Atom::from(TABLES[*table_index]),
                permutation_key(index),
                Some(permutation_value(index, *table_index)),
            )
        })
        .collect()
}

fn contended_actions(index: usize, order: [usize; 3]) -> Vec<TableKV> {
    order
        .iter()
        .map(|table_index| {
            TableKV::new(
                Atom::from(TABLES[*table_index]),
                contended_key(*table_index),
                Some(contender_value(index, *table_index)),
            )
        })
        .collect()
}

fn final_actions(order: [usize; 3]) -> Vec<TableKV> {
    order
        .iter()
        .map(|table_index| {
            TableKV::new(
                Atom::from(TABLES[*table_index]),
                contended_key(*table_index),
                Some(final_value(*table_index)),
            )
        })
        .collect()
}

fn permutation_key(index: usize) -> Binary {
    encode_usize(PERMUTATION_KEY_BASE + index)
}

fn permutation_value(index: usize, table_index: usize) -> Binary {
    encode_usize(PERMUTATION_VALUE_BASE + index * TABLES.len() + table_index)
}

fn contended_key(table_index: usize) -> Binary {
    encode_usize(CONTENDED_KEY_BASE + table_index)
}

fn contender_value(index: usize, table_index: usize) -> Binary {
    encode_usize(CONTENDER_VALUE_BASE + index * TABLES.len() + table_index)
}

fn final_value(table_index: usize) -> Binary {
    encode_usize(FINAL_VALUE_BASE + table_index)
}

async fn assert_single_permutation_value(
    fixture: &Fixture,
    permutation_index: usize,
    visible: bool,
    label: &str,
) -> TestResult<()> {
    let transaction = read_only_transaction(&fixture.db, label)?;
    let actions = TABLES
        .iter()
        .map(|table| TableKV::new(Atom::from(*table), permutation_key(permutation_index), None))
        .collect();
    let observed = transaction.query(actions).await;
    expect_eq(&format!("{label} result count"), &observed.len(), &TABLES.len())?;
    for (table_index, actual) in observed.iter().enumerate() {
        let expected = if visible {
            Some(permutation_value(permutation_index, table_index))
        } else {
            None
        };
        expect_binary(&format!("{label} table {table_index}"),
                      actual.as_ref(),
                      expected.as_ref())?;
    }
    Ok(())
}

async fn assert_permutation_values(
    fixture: &Fixture,
    include_memory: bool,
    label: &str,
) -> TestResult<()> {
    let transaction = read_only_transaction(&fixture.db, label)?;
    let mut actions = Vec::with_capacity(TABLES.len() * PERMUTATIONS.len());
    let mut expected = Vec::with_capacity(TABLES.len() * PERMUTATIONS.len());
    for (table_index, table) in TABLES.iter().enumerate() {
        for permutation_index in 0..PERMUTATIONS.len() {
            actions.push(TableKV::new(Atom::from(*table), permutation_key(permutation_index), None));
            if table_index == 0 && !include_memory {
                expected.push(None);
            } else {
                expected.push(Some(permutation_value(permutation_index, table_index)));
            }
        }
    }
    let observed = transaction.query(actions).await;
    expect_eq(&format!("{label} result count"), &observed.len(), &expected.len())?;
    for (index, (actual, expected)) in observed.iter().zip(expected.iter()).enumerate() {
        expect_binary(&format!("{label} slot {index}"),
                      actual.as_ref(),
                      expected.as_ref())?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum ContendedExpectation {
    Missing,
    Contender(usize),
    Final,
}

async fn assert_contended_values(
    fixture: &Fixture,
    include_memory: bool,
    expectation: ContendedExpectation,
    label: &str,
) -> TestResult<()> {
    let transaction = read_only_transaction(&fixture.db, label)?;
    let actions = TABLES
        .iter()
        .enumerate()
        .map(|(table_index, table)| {
            TableKV::new(Atom::from(*table), contended_key(table_index), None)
        })
        .collect();
    let observed = transaction.query(actions).await;
    expect_eq(&format!("{label} result count"), &observed.len(), &TABLES.len())?;
    for (table_index, actual) in observed.iter().enumerate() {
        let expected = if table_index == 0 && !include_memory {
            None
        } else {
            match expectation {
                ContendedExpectation::Missing => None,
                ContendedExpectation::Contender(index) => {
                    Some(contender_value(index, table_index))
                },
                ContendedExpectation::Final => Some(final_value(table_index)),
            }
        };
        expect_binary(&format!("{label} table {table_index}"),
                      actual.as_ref(),
                      expected.as_ref())?;
    }
    Ok(())
}

async fn phase_recover(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let winner_count = read_winner_manifest(&root)?;
    let expected_wal = BASE_WAL_COUNT + winner_count;
    let wal_path = root.join("root-wal");
    let bak_before = nonempty_bak_count(&wal_path)?;
    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    assert_table_definitions(&fixture, "recovered").await?;
    assert_permutation_values(&fixture, true, "replayed before confirm").await?;
    assert_contended_values(&fixture,
                            true,
                            ContendedExpectation::Final,
                            "replayed before confirm final values").await?;

    wait_for_wal_state(&rt,
                       &fixture,
                       expected_wal,
                       expected_wal,
                       0,
                       CONFIRM_TIMEOUT,
                       "recovery").await?;
    assert_permutation_values(&fixture, true, "replayed after confirm").await?;
    assert_contended_values(&fixture,
                            true,
                            ContendedExpectation::Final,
                            "replayed after confirm final values").await?;
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
            "ordinary ordering recovery did not add a confirmed .bak checkpoint: before={bak_before}, after={bak_after}",
        ));
    }
    let active = active_file_sizes(&wal_path)?;
    if active.iter().any(|(_, len)| *len > 0) {
        return Err(format!("nonempty active WAL remains after ordering recovery: {active:?}"));
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
            .map_err(|error| format!("archiving ordering recovery WAL failed: {error}"))?;
    } else {
        if !archived_wal.exists() {
            return Err("second ordering data-only start cannot find archived WAL".to_owned());
        }
        let active = active_file_sizes(&wal_path)?;
        if active.iter().any(|(_, len)| *len > 0) {
            return Err(format!("first ordering data-only start produced nonempty WAL: {active:?}"));
        }
    }
    if nonempty_bak_count(&archived_wal)? == 0 {
        return Err("archived ordering WAL contains no nonempty .bak checkpoint".to_owned());
    }

    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    let label = if archive_wal { "data-only" } else { "second data-only" };
    assert_table_definitions(&fixture, label).await?;
    assert_permutation_values(&fixture, false, label).await?;
    assert_contended_values(&fixture,
                            false,
                            ContendedExpectation::Final,
                            &format!("{label} final values")).await?;
    expect_eq(&format!("{label} WAL append count"),
              &fixture.logger.append_total_count(),
              &0usize)?;
    expect_eq(&format!("{label} WAL confirm count"),
              &fixture.logger.confirm_total_count(),
              &0usize)?;
    expect_eq(&format!("{label} WAL waiting count"),
              &fixture.logger.waiting_confirm_count().await,
              &0usize)?;
    expect_eq(&format!("{label} manager produced roots"),
              &fixture.tr_manager.produced_transaction_total(),
              &0usize)?;
    expect_eq(&format!("{label} manager consumed roots"),
              &fixture.tr_manager.consumed_transaction_total(),
              &0usize)?;
    expect_eq(&format!("{label} manager active roots"),
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq(&format!("{label} Btree overlay size"),
              &fixture.db.table_cache_size(&Atom::from(BTREE_TABLE)).await,
              &Some(0u64))
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

#[derive(Clone)]
struct Identity {
    tid: Guid,
    cid: Guid,
}

struct ActionedAttempt {
    index: usize,
    order: [usize; 3],
    transaction: RealTransaction,
}

struct PrepareResult {
    index: usize,
    order: [usize; 3],
    transaction: RealTransaction,
    prepare: Result<Vec<u8>, KVTableTrError>,
}

struct PreparedWinner {
    index: usize,
    transaction: RealTransaction,
    token: Vec<u8>,
}

struct CrossOutcome {
    winner_count: usize,
    identities: Vec<Identity>,
}

fn write_winner_manifest(root: &Path, winner_count: usize) -> TestResult<()> {
    if winner_count > 1 {
        return Err(format!("cannot persist invalid cross-order winner count {winner_count}"));
    }
    fs::write(root.join(OUTCOME_MANIFEST), [winner_count as u8])
        .map_err(|error| format!("writing ordering outcome manifest failed: {error}"))
}

fn read_winner_manifest(root: &Path) -> TestResult<usize> {
    let bytes = fs::read(root.join(OUTCOME_MANIFEST))
        .map_err(|error| format!("reading ordering outcome manifest failed: {error}"))?;
    if bytes.len() != 1 || bytes[0] > 1 {
        return Err(format!("invalid ordering outcome manifest bytes: {bytes:?}"));
    }
    Ok(bytes[0] as usize)
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
        .map_err(|error| format!("locating ordinary ordering test executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase)
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| format!("spawning ordinary ordering phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("ordinary ordering phase {phase} exited with {status}"))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking ordinary ordering child status failed: {error}"))? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("ordinary ordering child exceeded {timeout:?}"));
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
        "pi_db_ordinary_multi_table_ordering_{}_{}",
        std::process::id(),
        nanos,
    ))
}
