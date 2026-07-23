//! Key 版本 publication 与 prepare 的真实跨线程、跨运行时并发专项。
//!
//! 本 target 使用真实 4-worker 数据库 runtime、独立 2-worker 调用 runtime、事务管理器、根
//! CommitLogger、Memory 表和文件系统。它精确验证同 Key 并发首次观察只有一个版本、qwv 与连续
//! commit 交错时值/版本不撕裂，以及同一版本基线的多个写事务恰好一个 prepare 成功。TTL 的并发
//! 索引和到期路径由独立 `key_version_ttl_index` target 承担，避免把 TSan 负载无意义放大。

mod key_version_support;

use std::{collections::HashMap,
          sync::{Arc, Mutex},
          time::Duration};

use async_channel::bounded;
use pi_async_rt::rt::{AsyncRuntime,
                      multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder}};
use pi_async_transaction::{Transaction2Pc, UnitTransaction,
                           manager_2pc::Transaction2PcStatus};
use pi_atom::Atom;
use pi_db::{Binary, TableKeyConflict, TableKeyVersion, Version, VersionConflictKind,
            tables::TableKV};

use key_version_support::{MEMORY_TABLE, RealDb, RealTransaction, TestResult, TempRoot,
                          build_database, create_active_tables, encode_usize, expect_binary,
                          expect_eq, run_on_runtime, writable_transaction};

const TEST_TIMEOUT: Duration = Duration::from_secs(90);
const FIRST_OBSERVERS: usize = 24;
const READER_TASKS: usize = 4;
const READER_ROUNDS: usize = 128;
const WRITER_ROUNDS: usize = 12;
const CONTENDERS: usize = 8;

#[test]
fn test_key_version_concurrency_safety() {
    let root = TempRoot::new("concurrency_safety")
        .expect("creating key-version concurrency root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |db_rt| async move {
        let caller_rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(2)
            .build();
        let fixture = build_database(&db_rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        create_active_tables(&fixture).await?;
        let produced_before = fixture.tr_manager.produced_transaction_total();
        let consumed_before = fixture.tr_manager.consumed_transaction_total();

        verify_concurrent_first_observation(&db_rt, &caller_rt, &fixture.db).await?;
        verify_query_commit_pairs_never_tear(&db_rt, &caller_rt, &fixture.db).await?;
        verify_exactly_one_same_key_prepare(&db_rt, &caller_rt, &fixture.db).await?;

        expect_eq("concurrency active transaction registry",
                  &fixture.tr_manager.transaction_len(),
                  &0usize)?;
        expect_eq("concurrency produced/consumed balance",
                  &(fixture.tr_manager.produced_transaction_total() - produced_before),
                  &(fixture.tr_manager.consumed_transaction_total() - consumed_before))
    })
    .unwrap_or_else(|error| panic!("key-version concurrency safety failed: {error}"));
}

async fn verify_concurrent_first_observation(
    db_rt: &MultiTaskRuntime<()>,
    caller_rt: &MultiTaskRuntime<()>,
    db: &RealDb,
) -> TestResult<()> {
    let key = encode_usize(0x5100_0001);
    let (sender, receiver) = bounded(FIRST_OBSERVERS);
    for index in 0..FIRST_OBSERVERS {
        let executor = if index % 2 == 0 { db_rt.clone() } else { caller_rt.clone() };
        let task_db = db.clone();
        let task_key = key.clone();
        let task_sender = sender.clone();
        executor.spawn(async move {
            let result = task_db
                .query_with_version(Atom::from(MEMORY_TABLE), task_key)
                .await
                .map_err(|error| format!("first observer {index} failed: {error:?}"));
            let _ = task_sender.send(result).await;
        }).map_err(|error| format!("spawning first observer {index} failed: {error:?}"))?;
    }
    drop(sender);

    let mut baseline = None;
    for index in 0..FIRST_OBSERVERS {
        let (value, version) = receiver
            .recv()
            .await
            .map_err(|error| format!("first observer result {index} missing: {error}"))??;
        expect_binary(&format!("first observer {index} value"), value.as_ref(), None)?;
        if !matches!(version, Version::Delete(_)) {
            return Err(format!("first observer {index} returned non-delete version {version:?}"));
        }
        if let Some(expected) = baseline.as_ref() {
            expect_eq(&format!("first observer {index} unique version"), &version, expected)?;
        } else {
            baseline = Some(version);
        }
    }
    Ok(())
}

async fn verify_query_commit_pairs_never_tear(
    db_rt: &MultiTaskRuntime<()>,
    caller_rt: &MultiTaskRuntime<()>,
    db: &RealDb,
) -> TestResult<()> {
    let table = Atom::from(MEMORY_TABLE);
    let key = encode_usize(0x5100_0002);
    let initial = db
        .query_with_version(table.clone(), key.clone())
        .await
        .map_err(|error| format!("loading pair baseline failed: {error:?}"))?;
    expect_binary("pair baseline value", initial.0.as_ref(), None)?;

    let allowed = Arc::new(Mutex::new(HashMap::<Version, Option<Binary>>::new()));
    allowed.lock().unwrap().insert(initial.1, None);
    let (sender, receiver) = bounded(READER_TASKS);
    for reader in 0..READER_TASKS {
        let executor = if reader % 2 == 0 { db_rt.clone() } else { caller_rt.clone() };
        let task_rt = executor.clone();
        let task_db = db.clone();
        let task_table = table.clone();
        let task_key = key.clone();
        let task_allowed = allowed.clone();
        let task_sender = sender.clone();
        executor.spawn(async move {
            let mut result = Ok(());
            for round in 0..READER_ROUNDS {
                let observed = task_db
                    .query_with_version(task_table.clone(), task_key.clone())
                    .await
                    .map_err(|error| format!("reader {reader} round {round} qwv failed: {error:?}"));
                match observed {
                    Err(error) => {
                        result = Err(error);
                        break;
                    },
                    Ok((value, version)) => {
                        let expected = task_allowed.lock().unwrap().get(&version).cloned();
                        match expected {
                            None => {
                                result = Err(format!("reader {reader} round {round} observed unknown version {version:?}"));
                                break;
                            },
                            Some(expected) => {
                                if let Err(error) = expect_binary(
                                    &format!("reader {reader} round {round} value/version pair"),
                                    value.as_ref(),
                                    expected.as_ref()) {
                                    result = Err(error);
                                    break;
                                }
                            },
                        }
                    },
                }
                task_rt.timeout(1).await;
            }
            let _ = task_sender.send(result).await;
        }).map_err(|error| format!("spawning pair reader {reader} failed: {error:?}"))?;
    }
    drop(sender);

    let mut last_value = None;
    for round in 0..WRITER_ROUNDS {
        let (value, version) = db
            .query_with_version(table.clone(), key.clone())
            .await
            .map_err(|error| format!("writer round {round} baseline failed: {error:?}"))?;
        expect_binary(&format!("writer round {round} baseline value"),
                      value.as_ref(),
                      last_value.as_ref())?;
        let next_value = encode_usize(0x5200_0000 + round);
        let transaction = writable_transaction(db, &format!("pair writer {round}"))?;
        let prepare = transaction
            .prepare_with_version(
                vec![TableKeyVersion {
                    table: table.clone(),
                    key: key.clone(),
                    version,
                }],
                vec![TableKV::new(table.clone(), key.clone(), Some(next_value.clone()))],
            )
            .await
            .map_err(|error| format!("writer round {round} prepare failed: {error:?}"))?;
        let uid = transaction
            .get_transaction_uid()
            .ok_or_else(|| format!("writer round {round} has no transaction UID"))?;
        let published = Version::Upsert(uid.clone());
        // 在 commit 前登记允许映射；publication 保证读者只能在数据真正发布后观察该版本。
        allowed.lock().unwrap().insert(published.clone(), Some(next_value.clone()));
        let receipt = transaction
            .commit_with_version(prepare)
            .await
            .map_err(|error| format!("writer round {round} commit failed: {error:?}"))?;
        assert_single_receipt(&receipt, &table, &key, &published,
                              &format!("writer round {round}"))?;
        last_value = Some(next_value);
    }

    for reader in 0..READER_TASKS {
        receiver
            .recv()
            .await
            .map_err(|error| format!("pair reader {reader} result missing: {error}"))??;
    }
    let final_pair = db
        .query_with_version(table, key)
        .await
        .map_err(|error| format!("final pair query failed: {error:?}"))?;
    expect_binary("final pair value", final_pair.0.as_ref(), last_value.as_ref())?;
    let expected = allowed.lock().unwrap().get(&final_pair.1).cloned();
    expect_eq("final pair version is known", &expected.is_some(), &true)?;
    expect_binary("final pair exact mapping",
                  final_pair.0.as_ref(),
                  expected.flatten().as_ref())
}

async fn verify_exactly_one_same_key_prepare(
    db_rt: &MultiTaskRuntime<()>,
    caller_rt: &MultiTaskRuntime<()>,
    db: &RealDb,
) -> TestResult<()> {
    let table = Atom::from(MEMORY_TABLE);
    let key = encode_usize(0x5100_0003);
    let (_, baseline) = db
        .query_with_version(table.clone(), key.clone())
        .await
        .map_err(|error| format!("loading contention baseline failed: {error:?}"))?;
    let (sender, receiver) = bounded(CONTENDERS);
    for index in 0..CONTENDERS {
        let executor = if index % 2 == 0 { db_rt.clone() } else { caller_rt.clone() };
        let task_db = db.clone();
        let task_table = table.clone();
        let task_key = key.clone();
        let task_version = baseline.clone();
        let task_sender = sender.clone();
        executor.spawn(async move {
            let transaction = writable_transaction(&task_db, &format!("contention writer {index}"));
            let result = match transaction {
                Err(error) => Err(error),
                Ok(transaction) => {
                    let prepare = transaction
                        .prepare_with_version(
                            vec![TableKeyVersion {
                                table: task_table.clone(),
                                key: task_key.clone(),
                                version: task_version,
                            }],
                            vec![TableKV::new(task_table,
                                             task_key,
                                             Some(encode_usize(0x5300_0000 + index)))],
                        )
                        .await;
                    Ok((transaction, prepare))
                },
            };
            let _ = task_sender.send((index, result)).await;
        }).map_err(|error| format!("spawning contention writer {index} failed: {error:?}"))?;
    }
    drop(sender);

    let mut success = 0usize;
    let mut conflicts = 0usize;
    let mut winner: Option<(RealTransaction, Vec<u8>)> = None;
    for _ in 0..CONTENDERS {
        let (index, result) = receiver
            .recv()
            .await
            .map_err(|error| format!("contention result missing: {error}"))?;
        match result {
            Err(error) => {
                return Err(format!("contention writer {index} setup failed: {error}"));
            },
            Ok((transaction, Ok(prepare))) => {
                success += 1;
                if winner.replace((transaction, prepare)).is_some() {
                    return Err("more than one same-key version transaction prepared".to_owned());
                }
            },
            Ok((transaction, Err(error))) => {
                let expected = [TableKeyConflict {
                    table: table.clone(),
                    key: key.clone(),
                    kind: VersionConflictKind::TransactionConflict,
                }];
                if error.all_conflicts() != Some(&expected[..]) {
                    return Err(format!("contention writer {index} returned unexpected error {error:?}"));
                }
                conflicts += 1;
                expect_eq(&format!("contention writer {index} failed status"),
                          &transaction.get_status(),
                          &Transaction2PcStatus::PrepareFailed)?;
                transaction
                    .rollback_modified()
                    .await
                    .map_err(|rollback| format!("contention writer {index} rollback failed: {rollback:?}"))?;
                expect_eq(&format!("contention writer {index} rollback status"),
                          &transaction.get_status(),
                          &Transaction2PcStatus::Rollbacked)?;
            },
        }
    }
    expect_eq("same-key prepare success count", &success, &1usize)?;
    expect_eq("same-key prepare conflict count", &conflicts, &(CONTENDERS - 1))?;

    let (winner, prepare) = winner.ok_or_else(|| "same-key contention has no winner".to_owned())?;
    let uid = winner
        .get_transaction_uid()
        .ok_or_else(|| "same-key winner has no transaction UID".to_owned())?;
    let receipt = winner
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("same-key winner commit failed: {error:?}"))?;
    assert_single_receipt(&receipt,
                          &table,
                          &key,
                          &Version::Upsert(uid),
                          "same-key winner")
}

fn assert_single_receipt(
    receipt: &[TableKeyVersion],
    table: &Atom,
    key: &Binary,
    version: &Version,
    label: &str,
) -> TestResult<()> {
    expect_eq(&format!("{label} receipt length"), &receipt.len(), &1usize)?;
    expect_eq(&format!("{label} receipt table"), &receipt[0].table, table)?;
    expect_eq(&format!("{label} receipt key"), &receipt[0].key, key)?;
    expect_eq(&format!("{label} receipt version"), &receipt[0].version, version)
}
