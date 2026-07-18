//! 迭代器与版本事务树隔离的真实生产专项。
//!
//! 目标契约和修复边界见
//! `docs/KEY_VERSION_ITERATOR_PROTOCOL_ISOLATION_BUG.md#bug-kv-protocol-isolation-001-index`。
//! 本 target 使用真实 runtime、事务管理器、CommitLogger、四个在用表和文件系统；修复前必须
//! 因同表 iterator Ordinary 子节点与 Versioned 子节点碰撞而失败，修复后保持同一断言转绿。

mod key_version_support;

use std::time::{Duration, Instant};

use futures::StreamExt;
use pi_async_rt::rt::AsyncRuntime;
use pi_async_transaction::{AsyncCommitLog, ErrorLevel, Transaction2Pc};
use pi_atom::Atom;
use pi_db::{Binary, TableKeyVersion, Version, tables::TableKV};

use key_version_support::{
    BTREE_TABLE, LOG_ORDERED_TABLE, MEMORY_TABLE, META_TABLE, Fixture, TempRoot, TestResult,
    build_database, commit_ordinary, create_active_tables, encode_atom, encode_usize,
    expect_binary, expect_eq, run_on_runtime, writable_transaction,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(90);
// 生产表的定时 collector 固定每 60 秒 drain 一次小批量提交；本专项不以大对象人为触发
// 容量门限，故给真实定时路径保留 15 秒调度余量，同时仍受 90 秒 target 硬截止约束。
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(75);

#[test]
fn test_iterator_before_same_table_version_commit_active_table_matrix() {
    let root = TempRoot::new("iterator_protocol_isolation")
        .expect("creating iterator protocol-isolation root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt, &root_path, Duration::ZERO, Duration::ZERO).await?;
        create_active_tables(&fixture).await?;
        let produced_before = fixture.tr_manager.produced_transaction_total();
        let consumed_before = fixture.tr_manager.consumed_transaction_total();
        verify_active_table_matrix(&fixture).await?;
        verify_iterator_lifecycle_matrix(&fixture).await?;
        verify_delete_snapshot_matrix(&fixture).await?;
        verify_single_layer_multi_table_version_tree(&fixture).await?;
        verify_protocol_selection_boundaries(&fixture).await?;
        verify_iterator_is_not_an_implicit_read_set(&fixture).await?;
        wait_for_all_confirmed(&rt, &fixture).await?;
        verify_final_persisted_state(&fixture).await?;
        expect_eq("iterator isolation active transaction count",
                  &fixture.tr_manager.transaction_len(),
                  &0usize)?;
        expect_eq("iterator isolation produced/consumed balance",
                  &(fixture.tr_manager.produced_transaction_total() - produced_before),
                  &(fixture.tr_manager.consumed_transaction_total() - consumed_before))
    })
    .unwrap_or_else(|error| panic!("iterator protocol-isolation matrix failed: {error}"));
}

async fn verify_active_table_matrix(fixture: &Fixture) -> TestResult<()> {
    for (index, table) in [MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE]
        .into_iter()
        .enumerate()
    {
        let key = encode_usize(100 + index);
        let old_value = encode_usize(200 + index);
        let new_value = encode_usize(300 + index);
        let baseline = writable_transaction(&fixture.db, "iterator isolation baseline")?;
        baseline
            .upsert(vec![TableKV::new(
                Atom::from(table),
                key.clone(),
                Some(old_value),
            )])
            .await
            .map_err(|error| format!("writing {table} baseline failed: {error:?}"))?;
        commit_ordinary(&baseline, &format!("{table} iterator isolation baseline")).await?;
        verify_one_table(fixture, table, key, new_value).await?;
    }

    let meta_key = encode_atom(MEMORY_TABLE);
    let (meta_value, _) = fixture
        .db
        .query_with_version(Atom::from(META_TABLE), meta_key.clone())
        .await
        .map_err(|error| format!("querying Meta baseline failed: {error:?}"))?;
    let meta_value = meta_value.ok_or_else(|| "Meta baseline is absent".to_owned())?;
    verify_one_table(fixture, META_TABLE, meta_key, meta_value).await
}

async fn verify_one_table(
    fixture: &Fixture,
    table: &str,
    key: pi_db::Binary,
    new_value: pi_db::Binary,
) -> TestResult<()> {
    let (_, baseline_version) = fixture
        .db
        .query_with_version(Atom::from(table), key.clone())
        .await
        .map_err(|error| format!("querying {table} baseline version failed: {error:?}"))?;
    let writer = writable_transaction(&fixture.db, "iterator then version writer")?;

    // 流保持存活但不 poll，证明仅创建 iterator 就不得把普通子事务装入版本 2PC 树。
    let stream = writer
        .values(Atom::from(table), None, false)
        .await
        .ok_or_else(|| format!("creating {table} values iterator returned None"))?;
    let prepare = writer
        .prepare_with_version(
            vec![TableKeyVersion {
                table: Atom::from(table),
                key: key.clone(),
                version: baseline_version,
            }],
            vec![TableKV::new(
                Atom::from(table),
                key.clone(),
                Some(new_value.clone()),
            )],
        )
        .await
        .map_err(|error| format!("preparing {table} version writer failed: {error:?}"))?;
    let transaction_uid = writer
        .get_transaction_uid()
        .ok_or_else(|| format!("{table} version prepare did not allocate a TID"))?;
    let receipt = writer
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("committing {table} version writer failed: {error:?}"))?;
    let (authoritative_value, authoritative_version) = fixture
        .db
        .query_with_version(Atom::from(table), key.clone())
        .await
        .map_err(|error| format!("querying {table} committed state failed: {error:?}"))?;
    drop(stream);

    expect_eq(&format!("{table} receipt length"), &receipt.len(), &1usize)?;
    expect_eq(&format!("{table} receipt table"), &receipt[0].table, &Atom::from(table))?;
    expect_binary(&format!("{table} receipt key"), Some(&receipt[0].key), Some(&key))?;
    expect_eq(
        &format!("{table} receipt version"),
        &receipt[0].version,
        &Version::Upsert(transaction_uid.clone()),
    )?;
    expect_binary(
        &format!("{table} authoritative value"),
        authoritative_value.as_ref(),
        Some(&new_value),
    )?;
    expect_eq(
        &format!("{table} authoritative version"),
        &authoritative_version,
        &Version::Upsert(transaction_uid),
    )
}

#[derive(Clone, Copy)]
enum IteratorKind {
    Keys,
    Values,
}

#[derive(Clone, Copy)]
enum IteratorState {
    Unpolled,
    Partial,
    Exhausted,
    Dropped,
}

async fn verify_iterator_lifecycle_matrix(fixture: &Fixture) -> TestResult<()> {
    let cases = [
        (MEMORY_TABLE, IteratorKind::Keys, IteratorState::Partial),
        (LOG_ORDERED_TABLE, IteratorKind::Values, IteratorState::Exhausted),
        (BTREE_TABLE, IteratorKind::Keys, IteratorState::Dropped),
        (META_TABLE, IteratorKind::Values, IteratorState::Unpolled),
    ];
    for (index, (table, kind, state)) in cases.into_iter().enumerate() {
        let (key, old_value) = if table == META_TABLE {
            let key = encode_atom(MEMORY_TABLE);
            let (value, _) = fixture
                .db
                .query_with_version(Atom::from(table), key.clone())
                .await
                .map_err(|error| format!("querying lifecycle Meta value failed: {error:?}"))?;
            (key, value.ok_or_else(|| "lifecycle Meta value is absent".to_owned())?)
        } else {
            let key = encode_usize(1_000 + index);
            let value = encode_usize(1_100 + index);
            let seed = writable_transaction(&fixture.db, "iterator lifecycle seed")?;
            seed.upsert(vec![TableKV::new(
                Atom::from(table),
                key.clone(),
                Some(value.clone()),
            )])
            .await
            .map_err(|error| format!("seeding {table} lifecycle value failed: {error:?}"))?;
            commit_ordinary(&seed, &format!("{table} iterator lifecycle seed")).await?;
            (key, value)
        };
        let new_value = if table == META_TABLE {
            old_value.clone()
        } else {
            encode_usize(1_200 + index)
        };
        verify_iterator_lifecycle_case(fixture,
                                       table,
                                       key,
                                       old_value,
                                       new_value,
                                       kind,
                                       state).await?;
    }
    Ok(())
}

async fn verify_iterator_lifecycle_case(
    fixture: &Fixture,
    table: &str,
    key: Binary,
    old_value: Binary,
    new_value: Binary,
    kind: IteratorKind,
    state: IteratorState,
) -> TestResult<()> {
    let (_, baseline_version) = fixture
        .db
        .query_with_version(Atom::from(table), key.clone())
        .await
        .map_err(|error| format!("querying {table} lifecycle version failed: {error:?}"))?;
    let writer = writable_transaction(&fixture.db, "iterator lifecycle version writer")?;
    let mut held_keys = None;
    let mut held_values = None;
    match kind {
        IteratorKind::Keys => {
            let mut stream = writer
                .keys(Atom::from(table), Some(key.clone()), false)
                .await
                .ok_or_else(|| format!("creating {table} lifecycle keys iterator returned None"))?;
            match state {
                IteratorState::Unpolled => held_keys = Some(stream),
                IteratorState::Partial => {
                    let first = stream.next().await;
                    expect_binary(&format!("{table} partial iterator key"), first.as_ref(), Some(&key))?;
                    held_keys = Some(stream);
                },
                IteratorState::Exhausted => {
                    let mut observed = false;
                    while let Some(item) = stream.next().await {
                        if item.as_ref() == key.as_ref() {
                            observed = true;
                        }
                    }
                    expect_eq(&format!("{table} exhausted iterator observed target"), &observed, &true)?;
                    held_keys = Some(stream);
                },
                IteratorState::Dropped => drop(stream),
            }
        },
        IteratorKind::Values => {
            let mut stream = writer
                .values(Atom::from(table), Some(key.clone()), false)
                .await
                .ok_or_else(|| format!("creating {table} lifecycle values iterator returned None"))?;
            match state {
                IteratorState::Unpolled => held_values = Some(stream),
                IteratorState::Partial => {
                    let first = stream.next().await
                        .ok_or_else(|| format!("{table} partial values iterator ended early"))?;
                    expect_binary(&format!("{table} partial iterator value key"), Some(&first.0), Some(&key))?;
                    expect_binary(&format!("{table} partial iterator value"), Some(&first.1), Some(&old_value))?;
                    held_values = Some(stream);
                },
                IteratorState::Exhausted => {
                    let mut observed = false;
                    while let Some((item_key, item_value)) = stream.next().await {
                        if item_key.as_ref() == key.as_ref() {
                            expect_binary(&format!("{table} exhausted iterator value"),
                                          Some(&item_value),
                                          Some(&old_value))?;
                            observed = true;
                        }
                    }
                    expect_eq(&format!("{table} exhausted values observed target"), &observed, &true)?;
                    held_values = Some(stream);
                },
                IteratorState::Dropped => drop(stream),
            }
        },
    }

    let prepare = writer
        .prepare_with_version(
            vec![TableKeyVersion {
                table: Atom::from(table),
                key: key.clone(),
                version: baseline_version,
            }],
            vec![TableKV::new(Atom::from(table), key.clone(), Some(new_value.clone()))],
        )
        .await
        .map_err(|error| format!("preparing {table} lifecycle writer failed: {error:?}"))?;
    let transaction_uid = writer
        .get_transaction_uid()
        .ok_or_else(|| format!("{table} lifecycle writer has no TID"))?;
    let receipt = writer
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("committing {table} lifecycle writer failed: {error:?}"))?;
    verify_receipt_item(table, &key, &receipt, &Version::Upsert(transaction_uid.clone()))?;
    let (value, version) = fixture
        .db
        .query_with_version(Atom::from(table), key.clone())
        .await
        .map_err(|error| format!("querying {table} lifecycle result failed: {error:?}"))?;
    expect_binary(&format!("{table} lifecycle result"), value.as_ref(), Some(&new_value))?;
    expect_eq(&format!("{table} lifecycle version"),
              &version,
              &Version::Upsert(transaction_uid))?;
    drop(held_keys);
    drop(held_values);
    Ok(())
}

async fn verify_delete_snapshot_matrix(fixture: &Fixture) -> TestResult<()> {
    for (index, table) in [MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE]
        .into_iter()
        .enumerate()
    {
        let key = encode_usize(2_000 + index);
        let old_value = encode_usize(2_100 + index);
        let seed = writable_transaction(&fixture.db, "iterator delete seed")?;
        seed.upsert(vec![TableKV::new(
            Atom::from(table),
            key.clone(),
            Some(old_value.clone()),
        )])
        .await
        .map_err(|error| format!("seeding {table} delete value failed: {error:?}"))?;
        commit_ordinary(&seed, &format!("{table} iterator delete seed")).await?;
        let (_, baseline_version) = fixture
            .db
            .query_with_version(Atom::from(table), key.clone())
            .await
            .map_err(|error| format!("querying {table} delete baseline failed: {error:?}"))?;
        let writer = writable_transaction(&fixture.db, "iterator delete version writer")?;

        if index % 2 == 0 {
            let mut stream = writer
                .keys(Atom::from(table), Some(key.clone()), false)
                .await
                .ok_or_else(|| format!("creating {table} delete keys iterator returned None"))?;
            commit_version_delete(fixture, &writer, table, &key, baseline_version).await?;
            let snapshot_key = stream.next().await;
            expect_binary(&format!("{table} delete keys snapshot"),
                          snapshot_key.as_ref(),
                          Some(&key))?;
        } else {
            let mut stream = writer
                .values(Atom::from(table), Some(key.clone()), false)
                .await
                .ok_or_else(|| format!("creating {table} delete values iterator returned None"))?;
            commit_version_delete(fixture, &writer, table, &key, baseline_version).await?;
            let snapshot = stream.next().await
                .ok_or_else(|| format!("{table} delete values snapshot ended early"))?;
            expect_binary(&format!("{table} delete snapshot key"), Some(&snapshot.0), Some(&key))?;
            expect_binary(&format!("{table} delete snapshot value"),
                          Some(&snapshot.1),
                          Some(&old_value))?;
        }
    }
    Ok(())
}

async fn commit_version_delete(
    fixture: &Fixture,
    writer: &key_version_support::RealTransaction,
    table: &str,
    key: &Binary,
    baseline_version: Version,
) -> TestResult<()> {
    let prepare = writer
        .prepare_with_version(
            vec![TableKeyVersion {
                table: Atom::from(table),
                key: key.clone(),
                version: baseline_version,
            }],
            vec![TableKV::new(Atom::from(table), key.clone(), None)],
        )
        .await
        .map_err(|error| format!("preparing {table} delete failed: {error:?}"))?;
    let transaction_uid = writer
        .get_transaction_uid()
        .ok_or_else(|| format!("{table} delete writer has no TID"))?;
    let receipt = writer
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("committing {table} delete failed: {error:?}"))?;
    verify_receipt_item(table, key, &receipt, &Version::Delete(transaction_uid.clone()))?;
    let (value, version) = fixture
        .db
        .query_with_version(Atom::from(table), key.clone())
        .await
        .map_err(|error| format!("querying {table} delete result failed: {error:?}"))?;
    expect_binary(&format!("{table} delete result"), value.as_ref(), None)?;
    expect_eq(&format!("{table} delete version"),
              &version,
              &Version::Delete(transaction_uid))
}

async fn verify_single_layer_multi_table_version_tree(fixture: &Fixture) -> TestResult<()> {
    let mut read_set = Vec::new();
    let mut write_set = Vec::new();
    let mut expected = Vec::new();
    for (index, table) in [MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE, META_TABLE]
        .into_iter()
        .enumerate()
    {
        let key = if table == META_TABLE {
            encode_atom(MEMORY_TABLE)
        } else {
            encode_usize(3_000 + index)
        };
        let (value, version) = fixture
            .db
            .query_with_version(Atom::from(table), key.clone())
            .await
            .map_err(|error| format!("querying {table} multi-table baseline failed: {error:?}"))?;
        let new_value = if table == META_TABLE {
            value.ok_or_else(|| "multi-table Meta value is absent".to_owned())?
        } else {
            encode_usize(3_100 + index)
        };
        read_set.push(TableKeyVersion {
            table: Atom::from(table),
            key: key.clone(),
            version,
        });
        write_set.push(TableKV::new(Atom::from(table), key.clone(), Some(new_value.clone())));
        expected.push((Atom::from(table), key, new_value));
    }

    let writer = writable_transaction(&fixture.db, "single-layer multi-table version writer")?;
    let stream = writer
        .values(Atom::from(MEMORY_TABLE), None, false)
        .await
        .ok_or_else(|| "creating multi-table control iterator returned None".to_owned())?;
    let prepare = writer
        .prepare_with_version(read_set, write_set)
        .await
        .map_err(|error| format!("preparing single-layer multi-table version tree failed: {error:?}"))?;
    let transaction_uid = writer
        .get_transaction_uid()
        .ok_or_else(|| "multi-table version writer has no TID".to_owned())?;
    let receipt = writer
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("committing single-layer multi-table version tree failed: {error:?}"))?;
    drop(stream);
    expect_eq("multi-table receipt length", &receipt.len(), &expected.len())?;
    for (table, key, value) in expected {
        let item = receipt
            .iter()
            .find(|item| item.table == table && item.key.as_ref() == key.as_ref())
            .ok_or_else(|| format!("multi-table receipt is missing {:?}/{key:?}", table.as_str()))?;
        expect_eq(&format!("multi-table {:?} receipt version", table.as_str()),
                  &item.version,
                  &Version::Upsert(transaction_uid.clone()))?;
        let (actual_value, actual_version) = fixture
            .db
            .query_with_version(table.clone(), key.clone())
            .await
            .map_err(|error| format!("querying multi-table {:?} result failed: {error:?}", table.as_str()))?;
        expect_binary(&format!("multi-table {:?} value", table.as_str()),
                      actual_value.as_ref(),
                      Some(&value))?;
        expect_eq(&format!("multi-table {:?} version", table.as_str()),
                  &actual_version,
                  &Version::Upsert(transaction_uid.clone()))?;
    }
    Ok(())
}

async fn verify_protocol_selection_boundaries(fixture: &Fixture) -> TestResult<()> {
    let table = Atom::from(MEMORY_TABLE);
    let key = encode_usize(4_000);
    let value = encode_usize(4_100);
    let (_, version) = fixture
        .db
        .query_with_version(table.clone(), key.clone())
        .await
        .map_err(|error| format!("querying protocol selection baseline failed: {error:?}"))?;
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let ordinary = writable_transaction(&fixture.db, "ordinary child before version prepare")?;
    ordinary
        .upsert(vec![TableKV::new(table.clone(), key.clone(), Some(value.clone()))])
        .await
        .map_err(|error| format!("creating ordinary child before version prepare failed: {error:?}"))?;
    let error = ordinary
        .prepare_with_version(
            vec![TableKeyVersion {
                table: table.clone(),
                key: key.clone(),
                version,
            }],
            vec![TableKV::new(table.clone(), key.clone(), Some(value.clone()))],
        )
        .await
        .expect_err("version prepare must reject a root containing an ordinary child");
    if !matches!(error.level(), ErrorLevel::Normal) {
        return Err(format!("ordinary/version isolation expected Normal error, observed {error:?}"));
    }
    expect_eq("ordinary/version isolation TID", &ordinary.get_transaction_uid(), &None)?;
    expect_eq("ordinary/version isolation produced count",
              &fixture.tr_manager.produced_transaction_total(),
              &produced_before)?;
    expect_eq("ordinary/version isolation consumed count",
              &fixture.tr_manager.consumed_transaction_total(),
              &consumed_before)?;
    expect_eq("ordinary/version isolation WAL append count",
              &fixture.logger.append_total_count(),
              &append_before)?;
    drop(ordinary);
    let (authoritative, _) = fixture
        .db
        .query_with_version(table.clone(), key.clone())
        .await
        .map_err(|error| format!("querying rejected ordinary/version state failed: {error:?}"))?;
    expect_binary("rejected ordinary/version value", authoritative.as_ref(), None)?;

    // 隔离门禁作用于整棵根事务树，不只防止同表两个节点共享 TID。表 A 的普通只读子节点
    // 也必须阻止表 B 的版本子树安装，且拒绝发生在 manager/WAL 之前。
    let cross_ordinary_table = Atom::from(MEMORY_TABLE);
    let cross_ordinary_key = encode_usize(4_001);
    let cross_version_table = Atom::from(LOG_ORDERED_TABLE);
    let cross_version_key = encode_usize(4_002);
    let cross_version_value = encode_usize(4_102);
    let (_, cross_version) = fixture
        .db
        .query_with_version(cross_version_table.clone(), cross_version_key.clone())
        .await
        .map_err(|error| format!("querying cross-table version baseline failed: {error:?}"))?;
    let cross_produced_before = fixture.tr_manager.produced_transaction_total();
    let cross_consumed_before = fixture.tr_manager.consumed_transaction_total();
    let cross_append_before = fixture.logger.append_total_count();
    let cross = writable_transaction(&fixture.db, "cross-table ordinary before version prepare")?;
    let ordinary_values = cross
        .query(vec![TableKV::new(
            cross_ordinary_table.clone(),
            cross_ordinary_key.clone(),
            None,
        )])
        .await;
    expect_eq("cross-table ordinary query result count", &ordinary_values.len(), &1usize)?;
    expect_binary("cross-table ordinary query result", ordinary_values[0].as_ref(), None)?;
    let cross_error = cross
        .prepare_with_version(
            vec![TableKeyVersion {
                table: cross_version_table.clone(),
                key: cross_version_key.clone(),
                version: cross_version,
            }],
            vec![TableKV::new(
                cross_version_table.clone(),
                cross_version_key.clone(),
                Some(cross_version_value),
            )],
        )
        .await
        .expect_err("a cross-table ordinary/version tree must be rejected");
    if !matches!(cross_error.level(), ErrorLevel::Normal) {
        return Err(format!(
            "cross-table ordinary/version isolation expected Normal error, observed {cross_error:?}",
        ));
    }
    expect_eq("cross-table ordinary/version TID", &cross.get_transaction_uid(), &None)?;
    expect_eq("cross-table ordinary/version produced count",
              &fixture.tr_manager.produced_transaction_total(),
              &cross_produced_before)?;
    expect_eq("cross-table ordinary/version consumed count",
              &fixture.tr_manager.consumed_transaction_total(),
              &cross_consumed_before)?;
    expect_eq("cross-table ordinary/version WAL append count",
              &fixture.logger.append_total_count(),
              &cross_append_before)?;
    drop(cross);
    let (cross_authoritative, _) = fixture
        .db
        .query_with_version(cross_version_table, cross_version_key)
        .await
        .map_err(|error| format!("querying rejected cross-table state failed: {error:?}"))?;
    expect_binary("rejected cross-table version value", cross_authoritative.as_ref(), None)?;

    let empty = writable_transaction(&fixture.db, "empty version transaction after iterator")?;
    let stream = empty
        .keys(table, None, false)
        .await
        .ok_or_else(|| "creating empty-version iterator returned None".to_owned())?;
    let empty_append_before = fixture.logger.append_total_count();
    let prepare = empty
        .prepare_with_version(Vec::new(), Vec::new())
        .await
        .map_err(|error| format!("preparing empty version transaction failed: {error:?}"))?;
    let receipt = empty
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("committing empty version transaction failed: {error:?}"))?;
    drop(stream);
    expect_eq("empty version receipt", &receipt.len(), &0usize)?;
    expect_eq("empty version WAL append count",
              &fixture.logger.append_total_count(),
              &empty_append_before)
}

async fn verify_iterator_is_not_an_implicit_read_set(fixture: &Fixture) -> TestResult<()> {
    let table = Atom::from(MEMORY_TABLE);
    let observed_key = encode_usize(5_000);
    let observed_old = encode_usize(5_100);
    let observed_new = encode_usize(5_101);
    let write_key = encode_usize(5_001);
    let write_value = encode_usize(5_200);
    let seed = writable_transaction(&fixture.db, "implicit read-set seed")?;
    seed.upsert(vec![TableKV::new(
        table.clone(),
        observed_key.clone(),
        Some(observed_old.clone()),
    )])
    .await
    .map_err(|error| format!("seeding implicit read-set value failed: {error:?}"))?;
    commit_ordinary(&seed, "implicit read-set seed").await?;

    let writer = writable_transaction(&fixture.db, "iterator without implicit read-set")?;
    let mut stream = writer
        .values(table.clone(), Some(observed_key.clone()), false)
        .await
        .ok_or_else(|| "creating implicit read-set iterator returned None".to_owned())?;
    let concurrent = writable_transaction(&fixture.db, "post-iterator concurrent writer")?;
    concurrent
        .upsert(vec![TableKV::new(
            table.clone(),
            observed_key.clone(),
            Some(observed_new.clone()),
        )])
        .await
        .map_err(|error| format!("updating post-iterator value failed: {error:?}"))?;
    commit_ordinary(&concurrent, "post-iterator concurrent writer").await?;

    // read_set 为空是本用例的硬前提：iterator 只保留自己的快照，不得隐式参与版本冲突判断。
    let prepare = writer
        .prepare_with_version(
            Vec::new(),
            vec![TableKV::new(table.clone(), write_key.clone(), Some(write_value.clone()))],
        )
        .await
        .map_err(|error| format!("preparing iterator without implicit read-set failed: {error:?}"))?;
    let transaction_uid = writer
        .get_transaction_uid()
        .ok_or_else(|| "iterator without implicit read-set has no TID".to_owned())?;
    let receipt = writer
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("committing iterator without implicit read-set failed: {error:?}"))?;
    verify_receipt_item(MEMORY_TABLE,
                        &write_key,
                        &receipt,
                        &Version::Upsert(transaction_uid.clone()))?;
    let snapshot = stream.next().await
        .ok_or_else(|| "iterator snapshot ended before observed key".to_owned())?;
    expect_binary("implicit read-set snapshot key", Some(&snapshot.0), Some(&observed_key))?;
    expect_binary("implicit read-set snapshot old value", Some(&snapshot.1), Some(&observed_old))?;
    let (observed_value, _) = fixture
        .db
        .query_with_version(table.clone(), observed_key)
        .await
        .map_err(|error| format!("querying post-iterator authoritative value failed: {error:?}"))?;
    expect_binary("post-iterator authoritative value", observed_value.as_ref(), Some(&observed_new))?;
    let (written_value, written_version) = fixture
        .db
        .query_with_version(table, write_key)
        .await
        .map_err(|error| format!("querying iterator version write failed: {error:?}"))?;
    expect_binary("iterator version write value", written_value.as_ref(), Some(&write_value))?;
    expect_eq("iterator version write version",
              &written_version,
              &Version::Upsert(transaction_uid))
}

fn verify_receipt_item(
    table: &str,
    key: &Binary,
    receipt: &[TableKeyVersion],
    expected_version: &Version,
) -> TestResult<()> {
    expect_eq(&format!("{table} receipt length"), &receipt.len(), &1usize)?;
    expect_eq(&format!("{table} receipt table"), &receipt[0].table, &Atom::from(table))?;
    expect_binary(&format!("{table} receipt key"), Some(&receipt[0].key), Some(key))?;
    expect_eq(&format!("{table} receipt version"), &receipt[0].version, expected_version)
}

async fn wait_for_all_confirmed(
    rt: &pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
    fixture: &Fixture,
) -> TestResult<()> {
    let started = Instant::now();
    loop {
        let waiting = fixture.logger.waiting_confirm_count().await;
        let appended = fixture.logger.append_total_count();
        let confirmed = fixture.logger.confirm_total_count();
        let btree_cache = fixture.db.table_cache_size(&Atom::from(BTREE_TABLE)).await;
        if waiting == 0 && appended == confirmed && btree_cache == Some(0) {
            return Ok(());
        }
        if started.elapsed() >= CONFIRM_TIMEOUT {
            return Err(format!("iterator isolation persistence did not close before {CONFIRM_TIMEOUT:?}: waiting={waiting}, appended={appended}, confirmed={confirmed}, btree_cache={btree_cache:?}"));
        }
        rt.timeout(10).await;
    }
}

async fn verify_final_persisted_state(fixture: &Fixture) -> TestResult<()> {
    for (index, table) in [MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE]
        .into_iter()
        .enumerate()
    {
        let key = encode_usize(3_000 + index);
        let expected = encode_usize(3_100 + index);
        let (value, version) = fixture
            .db
            .query_with_version(Atom::from(table), key)
            .await
            .map_err(|error| format!("querying final persisted {table} value failed: {error:?}"))?;
        expect_binary(&format!("final persisted {table} value"), value.as_ref(), Some(&expected))?;
        if !matches!(version, Version::Upsert(_)) {
            return Err(format!("final persisted {table} version is not Upsert: {version:?}"));
        }
    }
    for (index, table) in [MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE]
        .into_iter()
        .enumerate()
    {
        let (value, version) = fixture
            .db
            .query_with_version(Atom::from(table), encode_usize(2_000 + index))
            .await
            .map_err(|error| format!("querying final deleted {table} value failed: {error:?}"))?;
        expect_binary(&format!("final deleted {table} value"), value.as_ref(), None)?;
        if !matches!(version, Version::Delete(_)) {
            return Err(format!("final deleted {table} version is not Delete: {version:?}"));
        }
    }
    Ok(())
}
