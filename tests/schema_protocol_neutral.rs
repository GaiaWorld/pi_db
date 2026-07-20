//! 元信息检查/建表前导阶段与普通、版本协议选择隔离的真实专项。
//!
//! 目标契约见
//! `docs/SCHEMA_PROTOCOL_NEUTRAL_DESIGN.md#schema-protocol-neutral-design-index`。本 target
//! 使用真实 4-worker runtime、事务管理器、CommitLogger、持久化 Memory 表和文件系统：先用
//! 无 schema prelude 的版本 2PC 证明生产装配基线可用，再要求 `table_meta/create_table*` 在同一
//! 根事务内保持协议中立。修复前后使用完全相同的业务断言；冻结前基线曾在两个 prelude
//! 场景的 `prepare_with_version` 处稳定失败，当前实现必须使原断言直接转绿，不能把失败改写成
//! 仅检查错误分支的测试。
//!
//! 本 target 聚焦根子节点装配、公开业务回执和第一阶段提交。异步数据文件确认、WAL crash/
//! replay 和冷启动最终数据由 SI-033 的独立恢复专项承担，不能由这里的进程内查询替代。

mod key_version_support;

use std::{
    io::ErrorKind,
    time::Duration,
};

use futures::StreamExt;
use pi_async_transaction::{
    manager_2pc::Transaction2PcStatus,
    AsyncCommitLog, ErrorLevel, Transaction2Pc, UnitTransaction,
};
use pi_atom::Atom;
use pi_db::{
    tables::TableKV,
    utils::CreateTableOptions,
    Binary, KVDBTableType, KVTableMeta, TableKeyVersion, Version,
};

use key_version_support::{
    BTREE_TABLE, Fixture, LOG_ORDERED_TABLE, MEMORY_TABLE, TempRoot, TestResult, build_database,
    commit_ordinary, create_active_tables, encode_usize, expect_binary, expect_eq, query_ordinary,
    read_only_transaction, run_on_runtime, table_meta, writable_transaction,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(60);
const TABLE_NAME: &str = "schema_protocol_memory";
const ORDINARY_CREATED_TABLE: &str = "schema_protocol_ordinary_created";
const EMPTY_VERSION_CREATED_TABLE: &str = "schema_protocol_empty_version_created";
const REJECTED_AFTER_ORDINARY: &str = "schema_protocol_rejected_after_ordinary";
const REJECTED_AFTER_VERSION: &str = "schema_protocol_rejected_after_version";
const CREATE_REMOVE_TABLE: &str = "schema_protocol_create_remove";
const ROLLED_BACK_SCHEMA_TABLE: &str = "schema_protocol_rolled_back_schema";

#[test]
fn test_version_commit_without_schema_prelude_control() {
    let root = TempRoot::new("schema_protocol_control")
        .expect("creating schema-protocol control root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        create_committed_memory_table(&fixture).await?;
        commit_one_version_write(&fixture,
                                 10_001,
                                 20_001,
                                 "schema-protocol no-prelude control").await
    })
    .unwrap_or_else(|error| panic!("schema-protocol control failed: {error}"));
}

#[test]
fn test_table_meta_only_before_version_commit() {
    let root = TempRoot::new("schema_protocol_table_meta_only")
        .expect("creating table-meta-only schema-protocol root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        let expected_meta = create_committed_memory_table(&fixture).await?;
        let key = encode_usize(10_004);
        let value = encode_usize(20_004);
        let (_, baseline_version) = fixture
            .db
            .query_with_version(Atom::from(TABLE_NAME), key.clone())
            .await
            .map_err(|error| format!("loading table-meta-only baseline failed: {error:?}"))?;
        assert_missing_version(&baseline_version, "table-meta-only baseline")?;

        let produced_before = fixture.tr_manager.produced_transaction_total();
        let consumed_before = fixture.tr_manager.consumed_transaction_total();
        let append_before = fixture.logger.append_total_count();
        let transaction = writable_transaction(&fixture.db, "table_meta only then version commit")?;
        expect_eq(
            "table_meta-only committed definition",
            &transaction.table_meta(Atom::from(TABLE_NAME)).await,
            &Some(expected_meta),
        )?;
        commit_version_transaction(
            &fixture,
            &transaction,
            key,
            value,
            baseline_version,
            (produced_before, consumed_before, append_before),
            "table_meta-only schema prelude",
        ).await
    })
    .unwrap_or_else(|error| panic!("table-meta-only schema prelude failed: {error}"));
}

#[test]
fn test_existing_table_meta_and_idempotent_create_before_version_commit() {
    let root = TempRoot::new("schema_protocol_existing")
        .expect("creating existing-table schema-protocol root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        let expected_meta = create_committed_memory_table(&fixture).await?;
        let key = encode_usize(10_002);
        let value = encode_usize(20_002);
        let (_, baseline_version) = fixture
            .db
            .query_with_version(Atom::from(TABLE_NAME), key.clone())
            .await
            .map_err(|error| format!("loading existing-table version baseline failed: {error:?}"))?;
        assert_missing_version(&baseline_version, "existing-table version baseline")?;

        let produced_before = fixture.tr_manager.produced_transaction_total();
        let consumed_before = fixture.tr_manager.consumed_transaction_total();
        let append_before = fixture.logger.append_total_count();
        let transaction = writable_transaction(
            &fixture.db,
            "existing table schema prelude then version commit",
        )?;
        let observed_meta = transaction
            .table_meta(Atom::from(TABLE_NAME))
            .await
            .ok_or_else(|| "table_meta did not observe the committed Memory definition".to_owned())?;
        expect_eq("existing table_meta result", &observed_meta, &expected_meta)?;
        transaction
            .create_table(Atom::from(TABLE_NAME), expected_meta, false)
            .await
            .map_err(|error| format!("idempotent create before version prepare failed: {error}"))?;

        commit_version_transaction(
            &fixture,
            &transaction,
            key,
            value,
            baseline_version,
            (produced_before, consumed_before, append_before),
            "existing table schema prelude",
        ).await
    })
    .unwrap_or_else(|error| panic!("existing-table schema prelude failed: {error}"));
}

#[test]
fn test_create_missing_table_before_version_commit() {
    let root = TempRoot::new("schema_protocol_create")
        .expect("creating missing-table schema-protocol root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        let expected_meta = memory_meta();
        let produced_before = fixture.tr_manager.produced_transaction_total();
        let consumed_before = fixture.tr_manager.consumed_transaction_total();
        let append_before = fixture.logger.append_total_count();
        let transaction = writable_transaction(
            &fixture.db,
            "create missing table schema prelude then version commit",
        )?;
        expect_eq(
            "missing table_meta result",
            &transaction.table_meta(Atom::from(TABLE_NAME)).await,
            &None,
        )?;
        transaction
            .create_table(Atom::from(TABLE_NAME), expected_meta.clone(), false)
            .await
            .map_err(|error| format!("creating Memory table before version prepare failed: {error}"))?;
        expect_eq(
            "new table registration before prepare",
            &fixture.db.is_exist(&Atom::from(TABLE_NAME)).await,
            &true,
        )?;

        let key = encode_usize(10_003);
        let value = encode_usize(20_003);
        let (_, baseline_version) = fixture
            .db
            .query_with_version(Atom::from(TABLE_NAME), key.clone())
            .await
            .map_err(|error| format!("loading newly-created table version baseline failed: {error:?}"))?;
        assert_missing_version(&baseline_version, "newly-created table version baseline")?;
        commit_version_transaction(
            &fixture,
            &transaction,
            key,
            value,
            baseline_version,
            (produced_before, consumed_before, append_before),
            "new table schema prelude",
        ).await?;

        let verifier = read_only_transaction(&fixture.db, "verify committed schema definition")?;
        expect_eq(
            "committed schema definition",
            &verifier.table_meta(Atom::from(TABLE_NAME)).await,
            &Some(expected_meta),
        )
    })
    .unwrap_or_else(|error| panic!("missing-table schema prelude failed: {error}"));
}

#[test]
fn test_schema_prelude_multi_table_version_delete() {
    let root = TempRoot::new("schema_protocol_multi_table_delete")
        .expect("creating multi-table delete schema-protocol root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        create_active_tables(&fixture).await?;

        let entries = [
            (MEMORY_TABLE, 10_031usize, 20_031usize),
            (LOG_ORDERED_TABLE, 10_032usize, 20_032usize),
            (BTREE_TABLE, 10_033usize, 20_033usize),
        ];
        let seed = writable_transaction(&fixture.db, "schema multi-table delete seed")?;
        seed.upsert(entries
            .iter()
            .map(|(table, key, value)| {
                TableKV::new(
                    Atom::from(*table),
                    encode_usize(*key),
                    Some(encode_usize(*value)),
                )
            })
            .collect())
            .await
            .map_err(|error| format!("seeding schema multi-table delete values failed: {error:?}"))?;
        commit_ordinary(&seed, "schema multi-table delete seed").await?;

        let mut read_set = Vec::with_capacity(entries.len());
        for (table, key, value) in &entries {
            let key = encode_usize(*key);
            let expected = encode_usize(*value);
            let (actual, version) = fixture
                .db
                .query_with_version(Atom::from(*table), key.clone())
                .await
                .map_err(|error| format!("loading {table} delete baseline failed: {error:?}"))?;
            expect_binary(
                &format!("{table} delete baseline value"),
                actual.as_ref(),
                Some(&expected),
            )?;
            read_set.push(TableKeyVersion {
                table: Atom::from(*table),
                key,
                version,
            });
        }

        let produced_before = fixture.tr_manager.produced_transaction_total();
        let consumed_before = fixture.tr_manager.consumed_transaction_total();
        let append_before = fixture.logger.append_total_count();
        let transaction = writable_transaction(&fixture.db, "schema multi-table version delete")?;
        transaction
            .create_table(
                Atom::from(MEMORY_TABLE),
                table_meta(KVDBTableType::MemOrdTab, true),
                false,
            )
            .await
            .map_err(|error| format!("idempotent Memory schema prelude failed: {error}"))?;
        transaction
            .create_table_with_options(
                Atom::from(LOG_ORDERED_TABLE),
                table_meta(KVDBTableType::LogOrdTab, true),
                CreateTableOptions::LogOrdTab(64 * 1024 * 1024, 1024 * 1024, 1024 * 1024),
                false,
            )
            .await
            .map_err(|error| format!("idempotent LogOrdered schema prelude failed: {error}"))?;
        transaction
            .create_table_with_options(
                Atom::from(BTREE_TABLE),
                table_meta(KVDBTableType::BtreeOrdTab, true),
                CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
                false,
            )
            .await
            .map_err(|error| format!("idempotent Btree schema prelude failed: {error}"))?;

        let write_set: Vec<_> = read_set
            .iter()
            .map(|item| TableKV::new(item.table.clone(), item.key.clone(), None))
            .collect();
        let prepare = transaction
            .prepare_with_version(read_set.clone(), write_set)
            .await
            .map_err(|error| format!("preparing schema multi-table delete failed: {error:?}"))?;
        let transaction_uid = transaction
            .get_transaction_uid()
            .ok_or_else(|| "schema multi-table delete prepare did not allocate a TID".to_owned())?;
        let receipt = transaction
            .commit_with_version(prepare)
            .await
            .map_err(|error| format!("committing schema multi-table delete failed: {error:?}"))?;

        expect_eq("schema multi-table delete receipt length", &receipt.len(), &entries.len())?;
        if receipt.iter().any(|item| item.table.as_str() == ".tables_meta") {
            return Err("Schema Meta write leaked into multi-table delete receipt".to_owned());
        }
        for expected in &read_set {
            let matches: Vec<_> = receipt
                .iter()
                .filter(|item| item.table == expected.table && item.key == expected.key)
                .collect();
            expect_eq(
                &format!("{} delete receipt multiplicity", expected.table),
                &matches.len(),
                &1usize,
            )?;
            expect_eq(
                &format!("{} delete receipt version", expected.table),
                &matches[0].version,
                &Version::Delete(transaction_uid.clone()),
            )?;
            let (value, version) = fixture
                .db
                .query_with_version(expected.table.clone(), expected.key.clone())
                .await
                .map_err(|error| format!("querying {} delete result failed: {error:?}", expected.table))?;
            expect_binary(
                &format!("{} authoritative delete value", expected.table),
                value.as_ref(),
                None,
            )?;
            expect_eq(
                &format!("{} authoritative delete version", expected.table),
                &version,
                &Version::Delete(transaction_uid.clone()),
            )?;
        }
        expect_eq(
            "schema multi-table delete produced transaction increment",
            &fixture.tr_manager.produced_transaction_total(),
            &(produced_before + 1),
        )?;
        expect_eq(
            "schema multi-table delete consumed transaction increment",
            &fixture.tr_manager.consumed_transaction_total(),
            &(consumed_before + 1),
        )?;
        expect_eq(
            "schema multi-table delete active transaction count",
            &fixture.tr_manager.transaction_len(),
            &0usize,
        )?;
        expect_eq(
            "schema multi-table delete root WAL append increment",
            &fixture.logger.append_total_count(),
            &(append_before + 1),
        )
    })
    .unwrap_or_else(|error| panic!("schema multi-table version delete failed: {error}"));
}

#[test]
fn test_empty_ordinary_actions_remain_protocol_neutral() {
    let root = TempRoot::new("schema_protocol_empty_actions")
        .expect("creating empty-action schema-protocol root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        let expected_meta = create_committed_memory_table(&fixture).await?;
        let key = encode_usize(10_005);
        let value = encode_usize(20_005);
        let (_, baseline_version) = fixture
            .db
            .query_with_version(Atom::from(TABLE_NAME), key.clone())
            .await
            .map_err(|error| format!("loading empty-action baseline failed: {error:?}"))?;
        assert_missing_version(&baseline_version, "empty-action baseline")?;

        let produced_before = fixture.tr_manager.produced_transaction_total();
        let consumed_before = fixture.tr_manager.consumed_transaction_total();
        let append_before = fixture.logger.append_total_count();
        let transaction = writable_transaction(&fixture.db, "empty ordinary actions then version")?;
        expect_eq(
            "empty dirty query",
            &transaction.dirty_query(Vec::new()).await,
            &Vec::<Option<Binary>>::new(),
        )?;
        expect_eq(
            "empty transaction-safe query",
            &transaction.query(Vec::new()).await,
            &Vec::<Option<Binary>>::new(),
        )?;
        transaction
            .dirty_upsert(Vec::new())
            .await
            .map_err(|error| format!("empty dirty upsert failed: {error:?}"))?;
        transaction
            .upsert(Vec::new())
            .await
            .map_err(|error| format!("empty transaction-safe upsert failed: {error:?}"))?;
        expect_eq(
            "empty dirty delete",
            &transaction.dirty_delete(Vec::new()).await
                .map_err(|error| format!("empty dirty delete failed: {error:?}"))?,
            &Vec::<Option<Binary>>::new(),
        )?;
        expect_eq(
            "empty transaction-safe delete",
            &transaction.delete(Vec::new()).await
                .map_err(|error| format!("empty transaction-safe delete failed: {error:?}"))?,
            &Vec::<Option<Binary>>::new(),
        )?;
        transaction
            .create_table(Atom::from(TABLE_NAME), expected_meta, false)
            .await
            .map_err(|error| format!("idempotent create after empty actions failed: {error}"))?;

        commit_version_transaction(
            &fixture,
            &transaction,
            key,
            value,
            baseline_version,
            (produced_before, consumed_before, append_before),
            "empty ordinary actions",
        ).await
    })
    .unwrap_or_else(|error| panic!("empty-action protocol neutrality failed: {error}"));
}

#[test]
fn test_create_missing_table_before_ordinary_commit() {
    let root = TempRoot::new("schema_protocol_ordinary_create")
        .expect("creating ordinary schema-protocol root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        let expected_meta = memory_meta();
        let key = encode_usize(10_006);
        let value = encode_usize(20_006);
        let produced_before = fixture.tr_manager.produced_transaction_total();
        let consumed_before = fixture.tr_manager.consumed_transaction_total();
        let append_before = fixture.logger.append_total_count();
        let transaction = writable_transaction(&fixture.db, "create then ordinary 2PC")?;
        transaction
            .create_table(Atom::from(ORDINARY_CREATED_TABLE), expected_meta.clone(), false)
            .await
            .map_err(|error| format!("creating table before ordinary action failed: {error}"))?;
        transaction
            .upsert(vec![TableKV::new(
                Atom::from(ORDINARY_CREATED_TABLE),
                key.clone(),
                Some(value.clone()),
            )])
            .await
            .map_err(|error| format!("ordinary write after create failed: {error:?}"))?;
        let prepare = transaction
            .prepare_modified_conflicts()
            .await
            .map_err(|error| format!("ordinary prepare after create failed: {error:?}"))?;
        let transaction_uid = transaction
            .get_transaction_uid()
            .ok_or_else(|| "ordinary create/write prepare did not allocate a TID".to_owned())?;
        transaction
            .commit_modified(prepare)
            .await
            .map_err(|error| format!("ordinary commit after create failed: {error:?}"))?;

        expect_binary(
            "ordinary create/write committed value",
            query_ordinary_table(&fixture, ORDINARY_CREATED_TABLE, key.clone()).await?.as_ref(),
            Some(&value),
        )?;
        let (versioned_value, version) = fixture
            .db
            .query_with_version(Atom::from(ORDINARY_CREATED_TABLE), key)
            .await
            .map_err(|error| format!("ordinary create/write version query failed: {error:?}"))?;
        expect_binary(
            "ordinary create/write versioned value",
            versioned_value.as_ref(),
            Some(&value),
        )?;
        expect_eq(
            "ordinary create/write published version",
            &version,
            &Version::Upsert(transaction_uid),
        )?;
        let verifier = read_only_transaction(&fixture.db, "verify ordinary-created table meta")?;
        expect_eq(
            "ordinary-created committed schema",
            &verifier.table_meta(Atom::from(ORDINARY_CREATED_TABLE)).await,
            &Some(expected_meta),
        )?;
        assert_manager_and_wal_closed(
            &fixture,
            produced_before,
            consumed_before,
            append_before,
            1,
            "ordinary create/write",
        )
    })
    .unwrap_or_else(|error| panic!("ordinary create/write protocol failed: {error}"));
}

#[test]
fn test_create_only_before_empty_version_commit() {
    let root = TempRoot::new("schema_protocol_empty_version")
        .expect("creating empty-version schema-protocol root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        let expected_meta = memory_meta();
        let produced_before = fixture.tr_manager.produced_transaction_total();
        let consumed_before = fixture.tr_manager.consumed_transaction_total();
        let append_before = fixture.logger.append_total_count();
        let transaction = writable_transaction(&fixture.db, "create only then empty version 2PC")?;
        transaction
            .create_table(Atom::from(EMPTY_VERSION_CREATED_TABLE), expected_meta.clone(), false)
            .await
            .map_err(|error| format!("creating table before empty version prepare failed: {error}"))?;
        let prepare = transaction
            .prepare_with_version(Vec::new(), Vec::new())
            .await
            .map_err(|error| format!("empty version prepare after create failed: {error:?}"))?;
        if transaction.get_transaction_uid().is_none() {
            return Err("empty version prepare after create did not allocate a TID".to_owned());
        }
        let receipt = transaction
            .commit_with_version(prepare)
            .await
            .map_err(|error| format!("empty version commit after create failed: {error:?}"))?;
        expect_eq("schema-only public version receipt", &receipt.len(), &0usize)?;
        let verifier = read_only_transaction(&fixture.db, "verify empty-version-created table meta")?;
        expect_eq(
            "empty-version-created committed schema",
            &verifier.table_meta(Atom::from(EMPTY_VERSION_CREATED_TABLE)).await,
            &Some(expected_meta),
        )?;
        expect_eq(
            "empty-version-created live registration",
            &fixture.db.is_exist(&Atom::from(EMPTY_VERSION_CREATED_TABLE)).await,
            &true,
        )?;
        assert_manager_and_wal_closed(
            &fixture,
            produced_before,
            consumed_before,
            append_before,
            1,
            "schema-only empty version commit",
        )
    })
    .unwrap_or_else(|error| panic!("schema-only empty version commit failed: {error}"));
}

#[test]
fn test_idempotent_create_empty_version_commit_releases_schema_read() {
    let root = TempRoot::new("schema_protocol_idempotent_empty_version")
        .expect("creating idempotent empty-version root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        let expected_meta = create_committed_memory_table(&fixture).await?;
        let produced_before = fixture.tr_manager.produced_transaction_total();
        let consumed_before = fixture.tr_manager.consumed_transaction_total();
        let append_before = fixture.logger.append_total_count();
        let transaction = writable_transaction(
            &fixture.db,
            "idempotent create then empty version 2PC",
        )?;
        transaction
            .create_table(Atom::from(TABLE_NAME), expected_meta, false)
            .await
            .map_err(|error| format!("idempotent create before empty version failed: {error}"))?;
        let prepare = transaction
            .prepare_with_version(Vec::new(), Vec::new())
            .await
            .map_err(|error| format!("idempotent empty version prepare failed: {error:?}"))?;
        let receipt = transaction
            .commit_with_version(prepare)
            .await
            .map_err(|error| format!("idempotent empty version commit failed: {error:?}"))?;
        expect_eq("idempotent empty version receipt", &receipt.len(), &0usize)?;
        assert_manager_and_wal_closed(
            &fixture,
            produced_before,
            consumed_before,
            append_before,
            0,
            "idempotent empty version commit",
        )?;

        // 后续 Meta 写与遗留的 SchemaCreate Read 严格冲突；删表能够 prepare，反向证明空 WAL
        // commit 仍提交了整棵 Prepared 树并清除了表级预留，而不是只关闭根事务计数。
        let remove = writable_transaction(&fixture.db, "remove after idempotent empty version")?;
        remove
            .remove_table(Atom::from(TABLE_NAME))
            .await
            .map_err(|error| format!("removing table after empty version failed: {error}"))?;
        expect_eq(
            "same-root table_meta observes pending remove",
            &remove.table_meta(Atom::from(TABLE_NAME)).await,
            &None,
        )?;
        commit_ordinary(&remove, "remove after idempotent empty version").await?;
        expect_eq(
            "removed table registration",
            &fixture.db.is_exist(&Atom::from(TABLE_NAME)).await,
            &false,
        )?;
        expect_eq(
            "removed committed table metadata",
            &read_only_transaction(&fixture.db, "verify removed metadata")?
                .table_meta(Atom::from(TABLE_NAME)).await,
            &None,
        )?;
        expect_eq(
            "remove after empty version WAL increment",
            &fixture.logger.append_total_count(),
            &(append_before + 1),
        )
    })
    .unwrap_or_else(|error| panic!("idempotent empty-version schema cleanup failed: {error}"));
}

#[test]
fn test_schema_prelude_iterator_lifecycle_matrix_before_version_commit() {
    let root = TempRoot::new("schema_protocol_iterator_lifecycle")
        .expect("creating schema iterator-lifecycle root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        let expected_meta = create_committed_memory_table(&fixture).await?;
        let key = encode_usize(10_020);
        let initial_value = encode_usize(20_020);
        let seed = writable_transaction(&fixture.db, "schema iterator lifecycle seed")?;
        seed.upsert(vec![TableKV::new(
            Atom::from(TABLE_NAME),
            key.clone(),
            Some(initial_value),
        )])
        .await
        .map_err(|error| format!("seeding schema iterator lifecycle failed: {error:?}"))?;
        commit_ordinary(&seed, "schema iterator lifecycle seed").await?;

        for (index, state) in [
            SchemaIteratorState::Unpolled,
            SchemaIteratorState::Partial,
            SchemaIteratorState::Exhausted,
            SchemaIteratorState::Dropped,
        ].into_iter().enumerate() {
            verify_schema_iterator_state(&fixture,
                                         &expected_meta,
                                         key.clone(),
                                         20_021 + index,
                                         state).await?;
        }
        Ok(())
    })
    .unwrap_or_else(|error| panic!("schema iterator-lifecycle matrix failed: {error}"));
}

#[test]
fn test_selected_protocol_rejects_late_create_without_side_effects() {
    let root = TempRoot::new("schema_protocol_late_create")
        .expect("creating late-create schema-protocol root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        create_committed_memory_table(&fixture).await?;
        verify_ordinary_rejects_late_create(&fixture).await?;
        verify_version_rejects_late_create_and_ordinary_action(&fixture).await
    })
    .unwrap_or_else(|error| panic!("late-create protocol isolation failed: {error}"));
}

#[test]
fn test_schema_create_rejects_remove_before_any_remove_side_effect() {
    let root = TempRoot::new("schema_protocol_create_remove")
        .expect("creating create/remove schema-protocol root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        let expected_meta = memory_meta();
        let append_before = fixture.logger.append_total_count();
        let transaction = writable_transaction(&fixture.db, "create then rejected remove")?;
        transaction
            .create_table(Atom::from(CREATE_REMOVE_TABLE), expected_meta.clone(), false)
            .await
            .map_err(|error| format!("creating table for remove rejection failed: {error}"))?;
        let error = transaction
            .remove_table(Atom::from(CREATE_REMOVE_TABLE))
            .await
            .expect_err("remove after schema create must be rejected");
        assert_invalid_input(&error, "remove after schema create")?;
        expect_eq(
            "create/remove rejection preserves live registration",
            &fixture.db.is_exist(&Atom::from(CREATE_REMOVE_TABLE)).await,
            &true,
        )?;
        expect_eq(
            "create/remove rejection preserves private schema",
            &transaction.table_meta(Atom::from(CREATE_REMOVE_TABLE)).await,
            &Some(expected_meta.clone()),
        )?;
        commit_ordinary(&transaction, "create after rejected remove").await?;
        expect_eq(
            "create/remove rejection commits original create",
            &read_only_transaction(&fixture.db, "verify create/remove schema")?
                .table_meta(Atom::from(CREATE_REMOVE_TABLE)).await,
            &Some(expected_meta),
        )?;
        expect_eq(
            "create/remove rejection WAL increment",
            &fixture.logger.append_total_count(),
            &(append_before + 1),
        )
    })
    .unwrap_or_else(|error| panic!("create/remove protocol isolation failed: {error}"));
}

#[test]
fn test_schema_create_version_conflict_rolls_back_transactional_state() {
    let root = TempRoot::new("schema_protocol_conflict_rollback")
        .expect("creating schema conflict rollback root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        create_committed_memory_table(&fixture).await?;
        let key = encode_usize(10_010);
        let winner_value = encode_usize(20_010);
        let rejected_value = encode_usize(20_011);
        let followup_value = encode_usize(20_012);
        let (_, stale_version) = fixture
            .db
            .query_with_version(Atom::from(TABLE_NAME), key.clone())
            .await
            .map_err(|error| format!("loading schema conflict baseline failed: {error:?}"))?;
        assert_missing_version(&stale_version, "schema conflict baseline")?;

        let winner = writable_transaction(&fixture.db, "schema conflict winner")?;
        winner
            .upsert(vec![TableKV::new(
                Atom::from(TABLE_NAME),
                key.clone(),
                Some(winner_value.clone()),
            )])
            .await
            .map_err(|error| format!("schema conflict winner action failed: {error:?}"))?;
        commit_ordinary(&winner, "schema conflict winner").await?;
        let (observed_winner_value, winner_version) = fixture
            .db
            .query_with_version(Atom::from(TABLE_NAME), key.clone())
            .await
            .map_err(|error| format!("loading schema conflict winner failed: {error:?}"))?;
        expect_binary(
            "schema conflict winner value",
            observed_winner_value.as_ref(),
            Some(&winner_value),
        )?;

        let produced_before = fixture.tr_manager.produced_transaction_total();
        let consumed_before = fixture.tr_manager.consumed_transaction_total();
        let append_before = fixture.logger.append_total_count();
        let transaction = writable_transaction(&fixture.db, "schema create then version conflict")?;
        transaction
            .create_table(Atom::from(ROLLED_BACK_SCHEMA_TABLE), memory_meta(), false)
            .await
            .map_err(|error| format!("creating rollback schema table failed: {error}"))?;
        let error = transaction
            .prepare_with_version(
                vec![TableKeyVersion {
                    table: Atom::from(TABLE_NAME),
                    key: key.clone(),
                    version: stale_version,
                }],
                vec![TableKV::new(
                    Atom::from(TABLE_NAME),
                    key.clone(),
                    Some(rejected_value),
                )],
            )
            .await
            .expect_err("stale version with SchemaCreate must return a conflict");
        if !error.is_all_conflicts() || !matches!(error.level(), ErrorLevel::Normal) {
            return Err(format!(
                "schema/version stale prepare expected AllConflicts(Normal), observed {error:?}",
            ));
        }
        let conflicts = error
            .all_conflicts()
            .ok_or_else(|| "schema/version conflict did not expose its complete set".to_owned())?;
        expect_eq("schema/version conflict count", &conflicts.len(), &1usize)?;
        expect_eq("schema/version conflict table", &conflicts[0].table, &Atom::from(TABLE_NAME))?;
        expect_binary("schema/version conflict key", Some(&conflicts[0].key), Some(&key))?;
        expect_eq(
            "schema/version conflict status",
            &transaction.get_status(),
            &Transaction2PcStatus::PrepareFailed,
        )?;
        transaction
            .rollback_modified()
            .await
            .map_err(|error| format!("rolling back schema/version conflict failed: {error:?}"))?;
        expect_eq(
            "schema/version rollback status",
            &transaction.get_status(),
            &Transaction2PcStatus::Rollbacked,
        )?;

        // 当前 DDL 注册项不可 rollback，但 Meta 定义、业务根、版本和 WAL 都必须保持未发布。
        expect_eq(
            "rolled-back schema physical registration boundary",
            &fixture.db.is_exist(&Atom::from(ROLLED_BACK_SCHEMA_TABLE)).await,
            &true,
        )?;
        expect_eq(
            "rolled-back schema committed Meta absence",
            &read_only_transaction(&fixture.db, "verify rolled-back schema Meta")?
                .table_meta(Atom::from(ROLLED_BACK_SCHEMA_TABLE)).await,
            &None,
        )?;
        let (value_after_rollback, version_after_rollback) = fixture
            .db
            .query_with_version(Atom::from(TABLE_NAME), key.clone())
            .await
            .map_err(|error| format!("querying winner after schema rollback failed: {error:?}"))?;
        expect_binary(
            "schema rollback preserves winner value",
            value_after_rollback.as_ref(),
            Some(&winner_value),
        )?;
        expect_eq(
            "schema rollback preserves winner version",
            &version_after_rollback,
            &winner_version,
        )?;
        assert_manager_and_wal_closed(
            &fixture,
            produced_before,
            consumed_before,
            append_before,
            0,
            "schema/version conflict rollback",
        )?;

        // 一个全新事务必须能立即取得相同 Key 的版本快照并提交，证明失败树未残留 prepare
        // 预留、版本 lease 或 manager 节点。
        let followup_produced = fixture.tr_manager.produced_transaction_total();
        let followup_consumed = fixture.tr_manager.consumed_transaction_total();
        let followup_append = fixture.logger.append_total_count();
        let followup = writable_transaction(&fixture.db, "schema rollback follow-up version write")?;
        commit_version_transaction(
            &fixture,
            &followup,
            key,
            followup_value,
            winner_version,
            (followup_produced, followup_consumed, followup_append),
            "schema rollback follow-up",
        ).await
    })
    .unwrap_or_else(|error| panic!("schema/version conflict rollback failed: {error}"));
}

async fn verify_ordinary_rejects_late_create(fixture: &Fixture) -> TestResult<()> {
    let key = encode_usize(10_007);
    let value = encode_usize(20_007);
    let append_before = fixture.logger.append_total_count();
    let transaction = writable_transaction(&fixture.db, "ordinary then rejected create")?;
    transaction
        .upsert(vec![TableKV::new(
            Atom::from(TABLE_NAME),
            key.clone(),
            Some(value.clone()),
        )])
        .await
        .map_err(|error| format!("selecting ordinary protocol failed: {error:?}"))?;
    let error = transaction
        .create_table(Atom::from(REJECTED_AFTER_ORDINARY), memory_meta(), false)
        .await
        .expect_err("create after ordinary action must be rejected");
    assert_invalid_input(&error, "create after ordinary action")?;
    expect_eq(
        "ordinary late-create rejection leaves table absent",
        &fixture.db.is_exist(&Atom::from(REJECTED_AFTER_ORDINARY)).await,
        &false,
    )?;
    expect_eq(
        "ordinary late-create rejection leaves Meta absent",
        &transaction.table_meta(Atom::from(REJECTED_AFTER_ORDINARY)).await,
        &None,
    )?;
    commit_ordinary(&transaction, "ordinary after rejected create").await?;
    expect_binary(
        "ordinary action survives rejected create",
        query_ordinary_table(fixture, TABLE_NAME, key).await?.as_ref(),
        Some(&value),
    )?;
    expect_eq(
        "ordinary late-create rejection WAL increment",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )
}

async fn verify_version_rejects_late_create_and_ordinary_action(
    fixture: &Fixture,
) -> TestResult<()> {
    // 本段刻意进入“prepare 成功后不得再追加动作”的非法调用域，只验证库在触碰表/Schema
    // 前 fail-fast，且被拒绝的调用没有改变已经冻结的 prepare token。它不是合法生产调用顺序，
    // 也不能替代上方 create -> prepare -> commit 的正向契约证据。
    let key = encode_usize(10_008);
    let value = encode_usize(20_008);
    let rejected_key = encode_usize(10_009);
    let rejected_value = encode_usize(20_009);
    let (_, baseline_version) = fixture
        .db
        .query_with_version(Atom::from(TABLE_NAME), key.clone())
        .await
        .map_err(|error| format!("loading late-version baseline failed: {error:?}"))?;
    assert_missing_version(&baseline_version, "late-version baseline")?;
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let transaction = writable_transaction(&fixture.db, "version then rejected ordinary/create")?;
    let prepare = transaction
        .prepare_with_version(
            vec![TableKeyVersion {
                table: Atom::from(TABLE_NAME),
                key: key.clone(),
                version: baseline_version,
            }],
            vec![TableKV::new(
                Atom::from(TABLE_NAME),
                key.clone(),
                Some(value.clone()),
            )],
        )
        .await
        .map_err(|error| format!("preparing version protocol for isolation failed: {error:?}"))?;
    let transaction_uid = transaction
        .get_transaction_uid()
        .ok_or_else(|| "version isolation prepare did not allocate a TID".to_owned())?;

    let create_error = transaction
        .create_table(Atom::from(REJECTED_AFTER_VERSION), memory_meta(), false)
        .await
        .expect_err("create after version prepare must be rejected");
    assert_invalid_input(&create_error, "create after version prepare")?;
    expect_eq(
        "version late-create rejection leaves table absent",
        &fixture.db.is_exist(&Atom::from(REJECTED_AFTER_VERSION)).await,
        &false,
    )?;
    let ordinary_error = transaction
        .upsert(vec![TableKV::new(
            Atom::from(TABLE_NAME),
            rejected_key.clone(),
            Some(rejected_value),
        )])
        .await
        .expect_err("ordinary upsert after version prepare must be rejected");
    if !ordinary_error.is_common() || !matches!(ordinary_error.level(), ErrorLevel::Normal) {
        return Err(format!(
            "ordinary action after version prepare expected Common(Normal), observed {ordinary_error:?}",
        ));
    }

    let receipt = transaction
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("version commit after rejected mixed actions failed: {error:?}"))?;
    expect_eq("isolated version receipt length", &receipt.len(), &1usize)?;
    expect_eq("isolated version receipt table", &receipt[0].table, &Atom::from(TABLE_NAME))?;
    expect_binary("isolated version receipt key", Some(&receipt[0].key), Some(&key))?;
    expect_eq(
        "isolated version receipt version",
        &receipt[0].version,
        &Version::Upsert(transaction_uid.clone()),
    )?;
    let (committed_value, committed_version) = fixture
        .db
        .query_with_version(Atom::from(TABLE_NAME), key)
        .await
        .map_err(|error| format!("querying isolated version result failed: {error:?}"))?;
    expect_binary("isolated version committed value", committed_value.as_ref(), Some(&value))?;
    expect_eq(
        "isolated version committed version",
        &committed_version,
        &Version::Upsert(transaction_uid),
    )?;
    expect_binary(
        "rejected ordinary action did not publish",
        query_ordinary_table(fixture, TABLE_NAME, rejected_key).await?.as_ref(),
        None,
    )?;
    assert_manager_and_wal_closed(
        fixture,
        produced_before,
        consumed_before,
        append_before,
        1,
        "version protocol isolation",
    )
}

async fn query_ordinary_table(
    fixture: &Fixture,
    table: &str,
    key: Binary,
) -> TestResult<Option<Binary>> {
    let label = format!("schema-protocol ordinary query for {table}");
    query_ordinary(&fixture.db, table, key, &label).await
}

#[derive(Clone, Copy)]
enum SchemaIteratorState {
    Unpolled,
    Partial,
    Exhausted,
    Dropped,
}

async fn verify_schema_iterator_state(
    fixture: &Fixture,
    expected_meta: &KVTableMeta,
    key: Binary,
    value_number: usize,
    state: SchemaIteratorState,
) -> TestResult<()> {
    let (old_value, baseline_version) = fixture
        .db
        .query_with_version(Atom::from(TABLE_NAME), key.clone())
        .await
        .map_err(|error| format!("loading schema iterator baseline failed: {error:?}"))?;
    let old_value = old_value.ok_or_else(|| "schema iterator baseline value is absent".to_owned())?;
    let transaction = writable_transaction(&fixture.db, "schema prelude iterator version writer")?;
    transaction
        .create_table(Atom::from(TABLE_NAME), expected_meta.clone(), false)
        .await
        .map_err(|error| format!("idempotent create before iterator failed: {error}"))?;

    let mut keys = None;
    let mut values = None;
    match state {
        SchemaIteratorState::Unpolled | SchemaIteratorState::Exhausted => {
            let mut stream = transaction
                .keys(Atom::from(TABLE_NAME), Some(key.clone()), false)
                .await
                .ok_or_else(|| "creating schema keys iterator returned None".to_owned())?;
            if matches!(state, SchemaIteratorState::Exhausted) {
                expect_binary("exhausted schema iterator first key",
                              stream.next().await.as_ref(),
                              Some(&key))?;
                expect_eq("exhausted schema iterator end", &stream.next().await, &None)?;
            }
            keys = Some(stream);
        },
        SchemaIteratorState::Partial | SchemaIteratorState::Dropped => {
            let mut stream = transaction
                .values(Atom::from(TABLE_NAME), Some(key.clone()), false)
                .await
                .ok_or_else(|| "creating schema values iterator returned None".to_owned())?;
            if matches!(state, SchemaIteratorState::Partial) {
                let (observed_key, observed_value) = stream
                    .next()
                    .await
                    .ok_or_else(|| "partial schema values iterator ended early".to_owned())?;
                expect_binary("partial schema iterator key", Some(&observed_key), Some(&key))?;
                expect_binary("partial schema iterator value",
                              Some(&observed_value),
                              Some(&old_value))?;
                values = Some(stream);
            } else {
                drop(stream);
            }
        },
    }

    let new_value = encode_usize(value_number);
    let prepare = transaction
        .prepare_with_version(
            vec![TableKeyVersion {
                table: Atom::from(TABLE_NAME),
                key: key.clone(),
                version: baseline_version,
            }],
            vec![TableKV::new(
                Atom::from(TABLE_NAME),
                key.clone(),
                Some(new_value.clone()),
            )],
        )
        .await
        .map_err(|error| format!("preparing schema iterator version write failed: {error:?}"))?;
    let transaction_uid = transaction
        .get_transaction_uid()
        .ok_or_else(|| "schema iterator version prepare did not allocate a TID".to_owned())?;
    let receipt = transaction
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("committing schema iterator version write failed: {error:?}"))?;
    expect_eq("schema iterator version receipt length", &receipt.len(), &1usize)?;
    expect_eq("schema iterator version receipt table",
              &receipt[0].table,
              &Atom::from(TABLE_NAME))?;
    expect_binary("schema iterator version receipt key", Some(&receipt[0].key), Some(&key))?;
    expect_eq("schema iterator version receipt version",
              &receipt[0].version,
              &Version::Upsert(transaction_uid.clone()))?;

    if matches!(state, SchemaIteratorState::Unpolled) {
        let mut stream = keys.take().expect("unpolled state must retain its keys stream");
        expect_binary("unpolled schema iterator keeps old snapshot",
                      stream.next().await.as_ref(),
                      Some(&key))?;
    }
    drop(keys);
    drop(values);
    let (committed_value, committed_version) = fixture
        .db
        .query_with_version(Atom::from(TABLE_NAME), key)
        .await
        .map_err(|error| format!("querying schema iterator committed state failed: {error:?}"))?;
    expect_binary("schema iterator committed value", committed_value.as_ref(), Some(&new_value))?;
    expect_eq("schema iterator committed version",
              &committed_version,
              &Version::Upsert(transaction_uid))
}

fn assert_invalid_input(error: &std::io::Error, label: &str) -> TestResult<()> {
    if error.kind() == ErrorKind::InvalidInput {
        Ok(())
    } else {
        Err(format!(
            "{label}: expected InvalidInput, observed {:?}: {error}",
            error.kind(),
        ))
    }
}

fn assert_manager_and_wal_closed(
    fixture: &Fixture,
    produced_before: usize,
    consumed_before: usize,
    append_before: usize,
    append_increment: usize,
    label: &str,
) -> TestResult<()> {
    expect_eq(
        &format!("{label} produced transaction increment"),
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        &format!("{label} consumed transaction increment"),
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 1),
    )?;
    expect_eq(
        &format!("{label} active transaction count"),
        &fixture.tr_manager.transaction_len(),
        &0usize,
    )?;
    expect_eq(
        &format!("{label} root WAL append increment"),
        &fixture.logger.append_total_count(),
        &(append_before + append_increment),
    )
}

async fn create_committed_memory_table(fixture: &Fixture) -> TestResult<KVTableMeta> {
    let meta = memory_meta();
    let transaction = writable_transaction(&fixture.db, "schema-protocol DDL fixture")?;
    transaction
        .create_table(Atom::from(TABLE_NAME), meta.clone(), false)
        .await
        .map_err(|error| format!("creating schema-protocol Memory fixture failed: {error}"))?;
    commit_ordinary(&transaction, "schema-protocol Memory fixture").await?;
    Ok(meta)
}

async fn commit_one_version_write(
    fixture: &Fixture,
    key_value: usize,
    value_value: usize,
    label: &str,
) -> TestResult<()> {
    let key = encode_usize(key_value);
    let value = encode_usize(value_value);
    let (_, baseline_version) = fixture
        .db
        .query_with_version(Atom::from(TABLE_NAME), key.clone())
        .await
        .map_err(|error| format!("{label}: loading version baseline failed: {error:?}"))?;
    assert_missing_version(&baseline_version, label)?;
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let transaction = writable_transaction(&fixture.db, label)?;
    commit_version_transaction(
        fixture,
        &transaction,
        key,
        value,
        baseline_version,
        (produced_before, consumed_before, append_before),
        label,
    ).await
}

async fn commit_version_transaction(
    fixture: &Fixture,
    transaction: &key_version_support::RealTransaction,
    key: Binary,
    value: Binary,
    baseline_version: Version,
    counters_before: (usize, usize, usize),
    label: &str,
) -> TestResult<()> {
    let (produced_before, consumed_before, append_before) = counters_before;
    let prepare_result = transaction
        .prepare_with_version(
            vec![TableKeyVersion {
                table: Atom::from(TABLE_NAME),
                key: key.clone(),
                version: baseline_version,
            }],
            vec![TableKV::new(
                Atom::from(TABLE_NAME),
                key.clone(),
                Some(value.clone()),
            )],
        )
        .await;
    let prepare = prepare_result.map_err(|error| format!(
        "{label}: version prepare failed: {error:?}; tid={:?}, produced_delta={}, consumed_delta={}, active={}, wal_append_delta={}",
        transaction.get_transaction_uid(),
        fixture.tr_manager.produced_transaction_total() - produced_before,
        fixture.tr_manager.consumed_transaction_total() - consumed_before,
        fixture.tr_manager.transaction_len(),
        fixture.logger.append_total_count() - append_before,
    ))?;
    let transaction_uid = transaction
        .get_transaction_uid()
        .ok_or_else(|| format!("{label}: successful prepare did not allocate a transaction TID"))?;
    let receipt = transaction
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("{label}: version commit failed: {error:?}"))?;

    expect_eq(&format!("{label} receipt length"), &receipt.len(), &1usize)?;
    expect_eq(
        &format!("{label} receipt table"),
        &receipt[0].table,
        &Atom::from(TABLE_NAME),
    )?;
    expect_binary(
        &format!("{label} receipt key"),
        Some(&receipt[0].key),
        Some(&key),
    )?;
    expect_eq(
        &format!("{label} receipt version"),
        &receipt[0].version,
        &Version::Upsert(transaction_uid.clone()),
    )?;

    let (authoritative_value, authoritative_version) = fixture
        .db
        .query_with_version(Atom::from(TABLE_NAME), key)
        .await
        .map_err(|error| format!("{label}: authoritative query failed: {error:?}"))?;
    expect_binary(
        &format!("{label} authoritative value"),
        authoritative_value.as_ref(),
        Some(&value),
    )?;
    expect_eq(
        &format!("{label} authoritative version"),
        &authoritative_version,
        &Version::Upsert(transaction_uid),
    )?;
    expect_eq(
        &format!("{label} produced transaction increment"),
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        &format!("{label} consumed transaction increment"),
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 1),
    )?;
    expect_eq(
        &format!("{label} active transaction count"),
        &fixture.tr_manager.transaction_len(),
        &0usize,
    )?;
    expect_eq(
        &format!("{label} root WAL append increment"),
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )
}

fn assert_missing_version(version: &Version, label: &str) -> TestResult<()> {
    if matches!(version, Version::Delete(_)) {
        Ok(())
    } else {
        Err(format!(
            "{label}: missing key must have a Delete baseline version, observed {version:?}",
        ))
    }
}

fn memory_meta() -> KVTableMeta {
    table_meta(KVDBTableType::MemOrdTab, true)
}
