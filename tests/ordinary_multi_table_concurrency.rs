//! 普通 Memory/LogOrdered/Btree 多表多 Key 分层并发冲突、回滚和恢复专项。
//!
//! 本 target 使用真实 4-worker 数据库 runtime、独立 2-worker 调用 runtime、事务管理器、根
//! CommitLogger、三类生产表和文件系统。每个事务先在三表读取 counter/marker 两个 Key，再同时
//! 更新 counter 并交替 upsert/delete marker。每波在全部动作完成后才并发 prepare，因而高、中、
//! 低三种冲突率的成功数和冲突数由 Key 分组唯一决定，不依赖随机调度或 sleep。
//!
//! 测试以独立参考模型、精确 outcome 计数、manager 配平和成功事务对应的 WAL 数量共同约束
//! 运行期结果；随后使用原 WAL 启动并等待确认，再移走全部 WAL 连续两次 data-only 冷启动。
//! Memory 没有独立数据文件，LogOrdered/Btree 必须精确等于参考模型。
//!
//! 冻结方案、证据和结论见
//! `docs/ORDINARY_MULTI_TABLE_CONCURRENCY_ACCEPTANCE.md#ordinary-multi-table-concurrency-index`。

mod key_version_support;

use std::{
    collections::BTreeSet,
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
    AsyncCommitLog, ErrorLevel, TransactionTree, UnitTransaction,
};
use pi_atom::Atom;
use pi_db::{
    db::KVDBTransaction,
    tables::TableKV,
    Binary, KVDBTableType, KVTableTrError,
};

use key_version_support::{
    BTREE_TABLE, Fixture, LOG_ORDERED_TABLE, MEMORY_TABLE, RealTransaction, TestResult,
    build_database, commit_ordinary, create_active_tables, encode_usize, expect_binary, expect_eq,
    read_only_transaction, run_on_runtime, table_meta, writable_transaction,
};

const TEST_NAME: &str = "test_ordinary_multi_table_concurrency";
const PHASE_ENV: &str = "PI_DB_ORDINARY_MULTI_TABLE_CONCURRENCY_PHASE";
const ROOT_ENV: &str = "PI_DB_ORDINARY_MULTI_TABLE_CONCURRENCY_ROOT";
const ARCHIVED_WAL_DIR: &str = "confirmed-root-wal";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(180);
const SETUP_TIMEOUT: Duration = Duration::from_secs(100);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(130);
const DATA_ONLY_TIMEOUT: Duration = Duration::from_secs(30);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(100);

const CONTENDERS: usize = 8;
const SHARD_COUNT: usize = 8;
const HOT_ROUNDS: usize = 16;
const STRIPED_ROUNDS: usize = 12;
const SPARSE_ROUNDS: usize = 8;
const EXPECTED_ATTEMPTS: usize = 288;
const EXPECTED_SUCCESSES: usize = 120;
const EXPECTED_CONFLICTS: usize = 168;
const EXPECTED_WAL_COUNT: usize = 2 + EXPECTED_SUCCESSES;
const COUNTER_KEY_BASE: usize = 0x6100_0000;
const MARKER_KEY_BASE: usize = 0x6200_0000;
const MARKER_VALUE_BASE: usize = 0x6300_0000;

const TABLES: [&str; 3] = [MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE];

#[test]
fn test_ordinary_multi_table_concurrency() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV)
                .expect("ordinary multi-table concurrency child phase must receive its root path"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("ordinary multi-table concurrency phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root();
    fs::create_dir_all(&root)
        .expect("creating ordinary multi-table concurrency root must succeed");
    for phase in ["setup", "recover", "inspect-data-only", "inspect-data-only-again"] {
        if let Err(error) = run_phase_process(&root, phase, PROCESS_TIMEOUT) {
            panic!(
                "ordinary multi-table concurrency failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root,
            );
        }
    }
    fs::remove_dir_all(&root)
        .expect("cleaning ordinary multi-table concurrency root must succeed");
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
        other => Err(format!("unknown ordinary multi-table concurrency phase: {other}")),
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
    seed_counters(&fixture).await?;
    expect_eq("setup seed WAL append count",
              &fixture.logger.append_total_count(),
              &2usize)?;

    let waves = build_waves();
    assert_workload_shape(&waves)?;
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let mut model = ReferenceModel::new();
    let mut totals = OutcomeTotals::default();

    for (index, wave) in waves.iter().enumerate() {
        run_wave(&rt,
                 &caller_rt,
                 &fixture,
                 wave,
                 &mut model,
                 &mut totals).await?;
        if (index + 1) % 6 == 0 || index + 1 == waves.len() {
            assert_model(&fixture,
                         &model,
                         true,
                         &format!("setup after wave {}", index + 1)).await?;
        }
        if wave.profile == ConflictProfile::Sparse {
            rt.timeout(3).await;
        } else {
            rt.timeout(0).await;
        }
    }

    assert_outcome_totals(&totals)?;
    expect_eq("setup workload produced roots",
              &(fixture.tr_manager.produced_transaction_total() - produced_before),
              &EXPECTED_ATTEMPTS)?;
    expect_eq("setup workload consumed roots",
              &(fixture.tr_manager.consumed_transaction_total() - consumed_before),
              &EXPECTED_ATTEMPTS)?;
    expect_eq("setup manager active roots",
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq("setup successful roots are the only appended WAL transactions",
              &(fixture.logger.append_total_count() - append_before),
              &EXPECTED_SUCCESSES)?;
    expect_eq("setup total WAL append count",
              &fixture.logger.append_total_count(),
              &EXPECTED_WAL_COUNT)?;
    assert_model(&fixture, &model, true, "setup final model").await?;
    assert_wal_accounting(&fixture, EXPECTED_WAL_COUNT, "setup").await
}

async fn seed_counters(fixture: &Fixture) -> TestResult<()> {
    let transaction = writable_transaction(&fixture.db, "ordinary concurrency seed")?;
    let mut actions = Vec::with_capacity(TABLES.len() * SHARD_COUNT);
    for table in TABLES {
        for shard in 0..SHARD_COUNT {
            actions.push(TableKV::new(
                Atom::from(table),
                counter_key(shard),
                Some(encode_usize(0)),
            ));
        }
    }
    transaction
        .upsert(actions)
        .await
        .map_err(|error| format!("seeding ordinary concurrency counters failed: {error:?}"))?;
    commit_ordinary(&transaction, "ordinary concurrency seed").await
}

async fn run_wave(
    db_rt: &MultiTaskRuntime<()>,
    caller_rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
    wave: &WaveSpec,
    model: &mut ReferenceModel,
    totals: &mut OutcomeTotals,
) -> TestResult<()> {
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let actioned = collect_actioned_attempts(db_rt,
                                             caller_rt,
                                             &fixture.db,
                                             wave,
                                             model).await?;
    let prepared = collect_prepare_results(db_rt, caller_rt, actioned).await?;
    let expected_winners = wave.distinct_shards().len();
    let expected_conflicts = CONTENDERS - expected_winners;
    let mut winner_shards = BTreeSet::new();
    let mut winners = Vec::with_capacity(expected_winners);
    let mut conflicts = Vec::with_capacity(expected_conflicts);

    for result in prepared {
        match result.prepare {
            Ok(token) => {
                expect_eq(&format!("{} contender {} prepared root state", wave.label, result.index),
                          &result.transaction.get_status(),
                          &Transaction2PcStatus::Prepared)?;
                if token.len() <= 16 {
                    return Err(format!(
                        "{} contender {} prepare token must contain three-table actions, observed {} bytes",
                        wave.label,
                        result.index,
                        token.len(),
                    ));
                }
                if !winner_shards.insert(result.shard) {
                    return Err(format!(
                        "{} prepared more than one winner for shard {}",
                        wave.label,
                        result.shard,
                    ));
                }
                assert_child_states(&result.transaction,
                                    Transaction2PcStatus::Prepared,
                                    &format!("{} contender {} prepared", wave.label, result.index))?;
                winners.push(PreparedWinner {
                    index: result.index,
                    transaction: result.transaction,
                    token,
                });
            },
            Err(error) => {
                if matches!(error.level(), ErrorLevel::Fatal) {
                    totals.fatal += 1;
                    return Err(format!(
                        "{} contender {} returned unexpected Fatal prepare error: {error:?}",
                        wave.label,
                        result.index,
                    ));
                }
                if !error.is_conflicts() || error.is_all_conflicts() {
                    totals.other_normal += 1;
                    return Err(format!(
                        "{} contender {} returned unexpected non-conflict Normal error: {error:?}",
                        wave.label,
                        result.index,
                    ));
                }
                assert_conflict_location(&error,
                                         result.shard,
                                         &format!("{} contender {}", wave.label, result.index))?;
                expect_eq(&format!("{} contender {} conflict root state", wave.label, result.index),
                          &result.transaction.get_status(),
                          &Transaction2PcStatus::PrepareFailed)?;
                conflicts.push((result.index, result.transaction));
            },
        }
    }

    expect_eq(&format!("{} prepared winner count", wave.label),
              &winners.len(),
              &expected_winners)?;
    expect_eq(&format!("{} conflict count", wave.label),
              &conflicts.len(),
              &expected_conflicts)?;
    expect_eq(&format!("{} winner shard set", wave.label),
              &winner_shards,
              &wave.distinct_shards())?;

    let rollback_outcome = rollback_conflicts(db_rt, caller_rt, conflicts, wave).await?;
    let commit_outcome = commit_winners(db_rt, caller_rt, winners, wave).await?;

    totals.attempts += CONTENDERS;
    totals.successes += expected_winners;
    totals.conflicts += expected_conflicts;
    totals.rollback_successes += rollback_outcome.successes;
    totals.rollback_failures += rollback_outcome.failures;
    totals.commit_successes += commit_outcome.successes;
    totals.commit_failures += commit_outcome.failures;
    if !rollback_outcome.errors.is_empty() || !commit_outcome.errors.is_empty() {
        return Err(format!(
            "{} finish failures: rollback={:?}, commit={:?}",
            wave.label,
            rollback_outcome.errors,
            commit_outcome.errors,
        ));
    }
    model.apply_wave(wave);

    expect_eq(&format!("{} manager produced roots", wave.label),
              &(fixture.tr_manager.produced_transaction_total() - produced_before),
              &CONTENDERS)?;
    expect_eq(&format!("{} manager consumed roots", wave.label),
              &(fixture.tr_manager.consumed_transaction_total() - consumed_before),
              &CONTENDERS)?;
    expect_eq(&format!("{} manager active roots", wave.label),
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq(&format!("{} WAL increment", wave.label),
              &(fixture.logger.append_total_count() - append_before),
              &expected_winners)
}

async fn collect_actioned_attempts(
    db_rt: &MultiTaskRuntime<()>,
    caller_rt: &MultiTaskRuntime<()>,
    db: &key_version_support::RealDb,
    wave: &WaveSpec,
    model: &ReferenceModel,
) -> TestResult<Vec<ActionedAttempt>> {
    let (sender, receiver) = bounded(CONTENDERS);
    for (index, shard) in wave.assignments.iter().copied().enumerate() {
        let executor = if index % 2 == 0 { db_rt.clone() } else { caller_rt.clone() };
        let task_db = db.clone();
        let task_wave = wave.clone();
        let task_model = model.clone();
        let task_sender = sender.clone();
        executor.spawn(async move {
            let result = build_actioned_attempt(&task_db,
                                                &task_wave,
                                                &task_model,
                                                index,
                                                shard).await;
            let _ = task_sender.send((index, result)).await;
        }).map_err(|error| format!("spawning {} action contender {index} failed: {error:?}", wave.label))?;
    }
    drop(sender);

    let mut ordered = (0..CONTENDERS)
        .map(|_| None)
        .collect::<Vec<Option<ActionedAttempt>>>();
    for _ in 0..CONTENDERS {
        let (index, result) = receiver
            .recv()
            .await
            .map_err(|error| format!("{} action result missing: {error}", wave.label))?;
        if ordered[index].is_some() {
            return Err(format!("{} received duplicate action result for contender {index}", wave.label));
        }
        ordered[index] = Some(result?);
    }
    ordered
        .into_iter()
        .enumerate()
        .map(|(index, attempt)| {
            attempt.ok_or_else(|| format!("{} action result {index} was not populated", wave.label))
        })
        .collect()
}

async fn build_actioned_attempt(
    db: &key_version_support::RealDb,
    wave: &WaveSpec,
    model: &ReferenceModel,
    index: usize,
    shard: usize,
) -> TestResult<ActionedAttempt> {
    let transaction = writable_transaction(
        db,
        &format!("{} contender {index} shard {shard}", wave.label),
    )?;
    let observed = transaction.query(query_actions(shard)).await;
    assert_action_snapshot(&observed,
                           model.counters[shard],
                           model.markers[shard],
                           &format!("{} contender {index}", wave.label))?;

    transaction
        .upsert(counter_upserts(shard, model.counters[shard] + 1))
        .await
        .map_err(|error| format!("{} contender {index} counter upsert failed: {error:?}", wave.label))?;
    match wave.marker {
        Some(value) => {
            transaction
                .upsert(marker_upserts(shard, value))
                .await
                .map_err(|error| format!("{} contender {index} marker upsert failed: {error:?}", wave.label))?;
        },
        None => {
            let deleted = transaction
                .delete(marker_actions(shard))
                .await
                .map_err(|error| format!("{} contender {index} marker delete failed: {error:?}", wave.label))?;
            assert_delete_results(&deleted,
                                  model.markers[shard],
                                  &format!("{} contender {index}", wave.label))?;
        },
    }
    assert_direct_leaf_order(&transaction,
                             &format!("{} contender {index} actioned", wave.label))?;
    Ok(ActionedAttempt {
        index,
        shard,
        transaction,
    })
}

async fn collect_prepare_results(
    db_rt: &MultiTaskRuntime<()>,
    caller_rt: &MultiTaskRuntime<()>,
    actioned: Vec<ActionedAttempt>,
) -> TestResult<Vec<PrepareResult>> {
    let result_count = actioned.len();
    let (sender, receiver) = bounded(result_count);
    for attempt in actioned {
        let executor = if attempt.index % 2 == 0 { db_rt.clone() } else { caller_rt.clone() };
        let task_sender = sender.clone();
        executor.spawn(async move {
            let prepare = attempt.transaction.prepare_modified_conflicts().await;
            let _ = task_sender.send(PrepareResult {
                index: attempt.index,
                shard: attempt.shard,
                transaction: attempt.transaction,
                prepare,
            }).await;
        }).map_err(|error| format!("spawning prepare contender failed: {error:?}"))?;
    }
    drop(sender);

    let mut results = Vec::with_capacity(result_count);
    for _ in 0..result_count {
        results.push(receiver
            .recv()
            .await
            .map_err(|error| format!("prepare result missing: {error}"))?);
    }
    Ok(results)
}

async fn rollback_conflicts(
    db_rt: &MultiTaskRuntime<()>,
    caller_rt: &MultiTaskRuntime<()>,
    conflicts: Vec<(usize, RealTransaction)>,
    wave: &WaveSpec,
) -> TestResult<OperationOutcome> {
    let count = conflicts.len();
    let (sender, receiver) = bounded(count.max(1));
    for (index, transaction) in conflicts {
        let executor = if index % 2 == 0 { db_rt.clone() } else { caller_rt.clone() };
        let task_sender = sender.clone();
        executor.spawn(async move {
            let rollback = transaction.rollback_modified().await;
            let _ = task_sender.send((index, transaction, rollback)).await;
        }).map_err(|error| format!("spawning {} rollback contender {index} failed: {error:?}", wave.label))?;
    }
    drop(sender);

    let mut outcome = OperationOutcome::default();
    for _ in 0..count {
        let (index, transaction, rollback) = receiver
            .recv()
            .await
            .map_err(|error| format!("{} rollback result missing: {error}", wave.label))?;
        let result = match rollback {
            Err(error) => Err(format!(
                "{} contender {index} rollback failed: {error:?}",
                wave.label,
            )),
            Ok(()) => {
                expect_eq(&format!("{} contender {index} rollback root state", wave.label),
                          &transaction.get_status(),
                          &Transaction2PcStatus::Rollbacked)
                    .and_then(|_| {
                        assert_child_states(&transaction,
                                            Transaction2PcStatus::Rollbacked,
                                            &format!("{} contender {index} rollback", wave.label))
                    })
            },
        };
        match result {
            Ok(()) => outcome.successes += 1,
            Err(error) => {
                outcome.failures += 1;
                outcome.errors.push(error);
            },
        }
    }
    Ok(outcome)
}

async fn commit_winners(
    db_rt: &MultiTaskRuntime<()>,
    caller_rt: &MultiTaskRuntime<()>,
    winners: Vec<PreparedWinner>,
    wave: &WaveSpec,
) -> TestResult<OperationOutcome> {
    let count = winners.len();
    let (sender, receiver) = bounded(count.max(1));
    for winner in winners {
        let executor = if winner.index % 2 == 0 { db_rt.clone() } else { caller_rt.clone() };
        let task_sender = sender.clone();
        executor.spawn(async move {
            let commit = winner.transaction.commit_modified(winner.token).await;
            let _ = task_sender.send((winner.index, winner.transaction, commit)).await;
        }).map_err(|error| format!("spawning {} commit contender {} failed: {error:?}", wave.label, winner.index))?;
    }
    drop(sender);

    let mut outcome = OperationOutcome::default();
    for _ in 0..count {
        let (index, transaction, commit) = receiver
            .recv()
            .await
            .map_err(|error| format!("{} commit result missing: {error}", wave.label))?;
        let result = match commit {
            Err(error) => Err(format!(
                "{} contender {index} commit failed: {error:?}",
                wave.label,
            )),
            Ok(()) => {
                expect_eq(&format!("{} contender {index} committed root state", wave.label),
                          &transaction.get_status(),
                          &Transaction2PcStatus::Commited)
                    .and_then(|_| {
                        assert_child_states(&transaction,
                                            Transaction2PcStatus::Commited,
                                            &format!("{} contender {index} committed", wave.label))
                    })
            },
        };
        match result {
            Ok(()) => outcome.successes += 1,
            Err(error) => {
                outcome.failures += 1;
                outcome.errors.push(error);
            },
        }
    }
    Ok(outcome)
}

fn assert_conflict_location(error: &KVTableTrError,
                            shard: usize,
                            label: &str) -> TestResult<()> {
    if error.is_all_conflicts()
        || !error.is_conflicts()
        || !matches!(error.level(), ErrorLevel::Normal) {
        return Err(format!("{label}: expected ordinary Conflicts(Normal), observed {error:?}"));
    }
    let (table, key) = error
        .conflicts()
        .ok_or_else(|| format!("{label}: conflict accessor returned None"))?;
    expect_eq(&format!("{label} conflict table"),
              &table.as_str(),
              &MEMORY_TABLE)?;
    let counter = counter_key(shard);
    let marker = marker_key(shard);
    if key.as_ref() != counter.as_ref() && key.as_ref() != marker.as_ref() {
        return Err(format!(
            "{label}: conflict key is neither shard {shard} counter nor marker: {key:?}",
        ));
    }
    expect_eq(&format!("{label} has no complete conflict set"),
              &error.all_conflicts().is_none(),
              &true)
}

fn assert_direct_leaf_order(transaction: &RealTransaction, label: &str) -> TestResult<()> {
    expect_eq(&format!("{label} direct leaf count"),
              &transaction.children_len(),
              &3usize)?;
    let children = transaction.to_children().collect::<Vec<_>>();
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
        expect_eq(&format!("{label} child {index} is unit"), &child.is_unit(), &true)?;
        expect_eq(&format!("{label} child {index} is not tree"), &child.is_tree(), &false)?;
    }
    Ok(())
}

fn assert_child_states(transaction: &RealTransaction,
                       expected: Transaction2PcStatus,
                       label: &str) -> TestResult<()> {
    let children = transaction.to_children().collect::<Vec<_>>();
    expect_eq(&format!("{label} direct child count"), &children.len(), &3usize)?;
    for (index, child) in children.iter().enumerate() {
        expect_eq(&format!("{label} child {index} state"),
                  &child.get_status(),
                  &expected)?;
    }
    Ok(())
}

fn query_actions(shard: usize) -> Vec<TableKV> {
    let mut actions = Vec::with_capacity(TABLES.len() * 2);
    for table in TABLES {
        actions.push(TableKV::new(Atom::from(table), counter_key(shard), None));
        actions.push(TableKV::new(Atom::from(table), marker_key(shard), None));
    }
    actions
}

fn counter_upserts(shard: usize, value: usize) -> Vec<TableKV> {
    TABLES
        .iter()
        .map(|table| {
            TableKV::new(Atom::from(*table),
                         counter_key(shard),
                         Some(encode_usize(value)))
        })
        .collect()
}

fn marker_upserts(shard: usize, value: usize) -> Vec<TableKV> {
    TABLES
        .iter()
        .map(|table| {
            TableKV::new(Atom::from(*table),
                         marker_key(shard),
                         Some(encode_usize(value)))
        })
        .collect()
}

fn marker_actions(shard: usize) -> Vec<TableKV> {
    TABLES
        .iter()
        .map(|table| TableKV::new(Atom::from(*table), marker_key(shard), None))
        .collect()
}

fn assert_action_snapshot(observed: &[Option<Binary>],
                          counter: usize,
                          marker: Option<usize>,
                          label: &str) -> TestResult<()> {
    expect_eq(&format!("{label} query result length"), &observed.len(), &6usize)?;
    let expected_counter = encode_usize(counter);
    let expected_marker = marker.map(encode_usize);
    for table_index in 0..TABLES.len() {
        expect_binary(&format!("{label} {} counter snapshot", TABLES[table_index]),
                      observed[table_index * 2].as_ref(),
                      Some(&expected_counter))?;
        expect_binary(&format!("{label} {} marker snapshot", TABLES[table_index]),
                      observed[table_index * 2 + 1].as_ref(),
                      expected_marker.as_ref())?;
    }
    Ok(())
}

fn assert_delete_results(observed: &[Option<Binary>],
                         marker: Option<usize>,
                         label: &str) -> TestResult<()> {
    expect_eq(&format!("{label} delete result length"), &observed.len(), &3usize)?;
    expect_binary(&format!("{label} Memory delete old value"),
                  observed[0].as_ref(),
                  None)?;
    expect_binary(&format!("{label} LogOrdered delete old value"),
                  observed[1].as_ref(),
                  None)?;
    let expected_btree = marker.map(encode_usize);
    expect_binary(&format!("{label} Btree delete old value"),
                  observed[2].as_ref(),
                  expected_btree.as_ref())
}

fn counter_key(shard: usize) -> Binary {
    encode_usize(COUNTER_KEY_BASE + shard)
}

fn marker_key(shard: usize) -> Binary {
    encode_usize(MARKER_KEY_BASE + shard)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConflictProfile {
    Hot,
    Striped,
    Sparse,
}

#[derive(Clone)]
struct WaveSpec {
    label: String,
    profile: ConflictProfile,
    assignments: Vec<usize>,
    marker: Option<usize>,
}

impl WaveSpec {
    fn distinct_shards(&self) -> BTreeSet<usize> {
        self.assignments.iter().copied().collect()
    }
}

fn build_waves() -> Vec<WaveSpec> {
    let mut waves = Vec::with_capacity(HOT_ROUNDS + STRIPED_ROUNDS + SPARSE_ROUNDS);
    let mut wave_id = 0usize;
    for round in 0..HOT_ROUNDS {
        waves.push(build_wave(
            ConflictProfile::Hot,
            round,
            vec![round % SHARD_COUNT; CONTENDERS],
            wave_id,
        ));
        wave_id += 1;
    }
    for round in 0..STRIPED_ROUNDS {
        let assignments = (0..CONTENDERS)
            .map(|index| (round + index / 2) % SHARD_COUNT)
            .collect();
        waves.push(build_wave(ConflictProfile::Striped,
                              round,
                              assignments,
                              wave_id));
        wave_id += 1;
    }
    for round in 0..SPARSE_ROUNDS {
        let pair = round % SHARD_COUNT;
        let mut assignments = Vec::with_capacity(CONTENDERS);
        assignments.push(pair);
        assignments.push(pair);
        for offset in 1..=6 {
            assignments.push((pair + offset) % SHARD_COUNT);
        }
        waves.push(build_wave(ConflictProfile::Sparse,
                              round,
                              assignments,
                              wave_id));
        wave_id += 1;
    }
    waves
}

fn build_wave(profile: ConflictProfile,
              round: usize,
              assignments: Vec<usize>,
              wave_id: usize) -> WaveSpec {
    let marker = if wave_id % 3 == 0 {
        None
    } else {
        Some(MARKER_VALUE_BASE + wave_id)
    };
    WaveSpec {
        label: format!("{:?} wave {round}", profile),
        profile,
        assignments,
        marker,
    }
}

fn assert_workload_shape(waves: &[WaveSpec]) -> TestResult<()> {
    let attempts = waves.len() * CONTENDERS;
    let successes = waves
        .iter()
        .map(|wave| wave.distinct_shards().len())
        .sum::<usize>();
    let conflicts = attempts - successes;
    expect_eq("ordinary concurrency wave count",
              &waves.len(),
              &(HOT_ROUNDS + STRIPED_ROUNDS + SPARSE_ROUNDS))?;
    expect_eq("ordinary concurrency attempt count", &attempts, &EXPECTED_ATTEMPTS)?;
    expect_eq("ordinary concurrency expected successes", &successes, &EXPECTED_SUCCESSES)?;
    expect_eq("ordinary concurrency expected conflicts", &conflicts, &EXPECTED_CONFLICTS)
}

#[derive(Clone)]
struct ReferenceModel {
    counters: [usize; SHARD_COUNT],
    markers: [Option<usize>; SHARD_COUNT],
}

impl ReferenceModel {
    fn new() -> Self {
        Self {
            counters: [0; SHARD_COUNT],
            markers: [None; SHARD_COUNT],
        }
    }

    fn apply_wave(&mut self, wave: &WaveSpec) {
        for shard in wave.distinct_shards() {
            self.counters[shard] += 1;
            self.markers[shard] = wave.marker;
        }
    }
}

fn expected_model() -> ReferenceModel {
    let mut model = ReferenceModel::new();
    for wave in build_waves() {
        model.apply_wave(&wave);
    }
    model
}

#[derive(Default)]
struct OutcomeTotals {
    attempts: usize,
    successes: usize,
    conflicts: usize,
    other_normal: usize,
    fatal: usize,
    commit_successes: usize,
    commit_failures: usize,
    rollback_successes: usize,
    rollback_failures: usize,
    timeouts: usize,
}

#[derive(Default)]
struct OperationOutcome {
    successes: usize,
    failures: usize,
    errors: Vec<String>,
}

fn assert_outcome_totals(totals: &OutcomeTotals) -> TestResult<()> {
    expect_eq("ordinary concurrency attempts", &totals.attempts, &EXPECTED_ATTEMPTS)?;
    expect_eq("ordinary concurrency successes", &totals.successes, &EXPECTED_SUCCESSES)?;
    expect_eq("ordinary concurrency conflicts", &totals.conflicts, &EXPECTED_CONFLICTS)?;
    expect_eq("ordinary concurrency other Normal errors", &totals.other_normal, &0usize)?;
    expect_eq("ordinary concurrency Fatal errors", &totals.fatal, &0usize)?;
    expect_eq("ordinary concurrency commit successes",
              &totals.commit_successes,
              &EXPECTED_SUCCESSES)?;
    expect_eq("ordinary concurrency commit failures", &totals.commit_failures, &0usize)?;
    expect_eq("ordinary concurrency rollback successes",
              &totals.rollback_successes,
              &EXPECTED_CONFLICTS)?;
    expect_eq("ordinary concurrency rollback failures", &totals.rollback_failures, &0usize)?;
    expect_eq("ordinary concurrency timeouts", &totals.timeouts, &0usize)?;
    expect_eq("ordinary concurrency total outcome conservation",
              &(totals.successes + totals.conflicts + totals.other_normal + totals.fatal),
              &totals.attempts)
}

struct ActionedAttempt {
    index: usize,
    shard: usize,
    transaction: RealTransaction,
}

struct PrepareResult {
    index: usize,
    shard: usize,
    transaction: RealTransaction,
    prepare: Result<Vec<u8>, KVTableTrError>,
}

struct PreparedWinner {
    index: usize,
    transaction: RealTransaction,
    token: Vec<u8>,
}

async fn phase_recover(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let wal_path = root.join("root-wal");
    let bak_before = nonempty_bak_count(&wal_path)?;
    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    assert_table_definitions(&fixture, "recovered").await?;
    let model = expected_model();
    assert_model(&fixture, &model, true, "replayed before confirm").await?;

    wait_for_wal_state(&rt,
                       &fixture,
                       EXPECTED_WAL_COUNT,
                       EXPECTED_WAL_COUNT,
                       0,
                       CONFIRM_TIMEOUT,
                       "recovery").await?;
    assert_model(&fixture, &model, true, "replayed after confirm").await?;
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
    if bak_after == 0 || bak_after < bak_before {
        return Err(format!(
            "ordinary concurrency recovery has invalid .bak state: before={bak_before}, after={bak_after}",
        ));
    }
    let active = active_file_sizes(&wal_path)?;
    if active.iter().any(|(_, len)| *len > 0) {
        return Err(format!("nonempty active WAL remains after concurrency recovery: {active:?}"));
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
            .map_err(|error| format!("archiving concurrency recovery WAL failed: {error}"))?;
    } else {
        if !archived_wal.exists() {
            return Err("second concurrency data-only start cannot find archived WAL".to_owned());
        }
        let active = active_file_sizes(&wal_path)?;
        if active.iter().any(|(_, len)| *len > 0) {
            return Err(format!("first concurrency data-only start produced nonempty WAL: {active:?}"));
        }
    }
    if nonempty_bak_count(&archived_wal)? == 0 {
        return Err("archived concurrency WAL contains no nonempty .bak checkpoint".to_owned());
    }

    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    let label = if archive_wal { "data-only" } else { "second data-only" };
    assert_table_definitions(&fixture, label).await?;
    assert_model(&fixture, &expected_model(), false, label).await?;
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

async fn assert_model(fixture: &Fixture,
                      model: &ReferenceModel,
                      include_memory: bool,
                      label: &str) -> TestResult<()> {
    let transaction = read_only_transaction(&fixture.db, &format!("{label} reference model"))?;
    let mut actions = Vec::with_capacity(TABLES.len() * SHARD_COUNT * 2);
    let mut expected = Vec::with_capacity(TABLES.len() * SHARD_COUNT * 2);
    for table in TABLES {
        for shard in 0..SHARD_COUNT {
            actions.push(TableKV::new(Atom::from(table), counter_key(shard), None));
            actions.push(TableKV::new(Atom::from(table), marker_key(shard), None));
            if table == MEMORY_TABLE && !include_memory {
                expected.push(None);
                expected.push(None);
            } else {
                expected.push(Some(encode_usize(model.counters[shard])));
                expected.push(model.markers[shard].map(encode_usize));
            }
        }
    }
    let observed = transaction.query(actions).await;
    expect_eq(&format!("{label} reference result length"),
              &observed.len(),
              &expected.len())?;
    for (index, (actual, expected)) in observed.iter().zip(expected.iter()).enumerate() {
        expect_binary(&format!("{label} reference slot {index}"),
                      actual.as_ref(),
                      expected.as_ref())?;
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

async fn assert_wal_accounting(fixture: &Fixture,
                               expected_appended: usize,
                               label: &str) -> TestResult<()> {
    let appended = fixture.logger.append_total_count();
    let confirmed = fixture.logger.confirm_total_count();
    let waiting = fixture.logger.waiting_confirm_count().await;
    expect_eq(&format!("{label} WAL appended"), &appended, &expected_appended)?;
    expect_eq(&format!("{label} WAL confirmed plus waiting"),
              &(confirmed + waiting),
              &appended)
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
        .map_err(|error| format!("locating ordinary concurrency test executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase)
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| format!("spawning ordinary concurrency phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("ordinary concurrency phase {phase} exited with {status}"))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking ordinary concurrency child status failed: {error}"))? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("ordinary concurrency child exceeded {timeout:?}"));
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
        "pi_db_ordinary_multi_table_concurrency_{}_{}",
        std::process::id(),
        nanos,
    ))
}
