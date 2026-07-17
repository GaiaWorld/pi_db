//! Key 版本公开 API 的真实参数、首次观察和空事务契约。
//!
//! 本 target 使用真实 4-worker runtime、事务管理器、根 CommitLogger、Meta/Memory/
//! LogOrdered/Btree 与文件系统。它精确验证公开载荷类型、qwv 首次 Some/None、重复读取稳定、
//! 普通提交推进版本、输入拒绝零事务/WAL 副作用，以及可写空版本事务的 prepare/commit 闭环。
//! LogWrite 行为测试按 HC-059 暂停，不由本 target 绕过。

mod key_version_support;

use std::{collections::HashSet, time::Duration};

use pi_async_transaction::{
    AsyncCommitLog, ErrorLevel, Transaction2Pc,
};
use pi_atom::Atom;
use pi_db::{
    tables::TableKV,
    Binary, KVTableTrError, TableKey, TableKeyVersion, Version,
};

use key_version_support::{
    BTREE_TABLE, LOG_ORDERED_TABLE, MEMORY_TABLE, META_TABLE, TestResult, TempRoot,
    build_database, commit_ordinary, create_active_tables, encode_atom, encode_usize, expect_binary,
    expect_eq, read_only_transaction, run_on_runtime, writable_transaction,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(60);

#[test]
fn test_key_version_public_api_contract() {
    assert_send_sync::<Version>();
    assert_send_sync::<TableKeyVersion>();
    assert_send_sync::<TableKey>();

    let root = TempRoot::new("api_contract")
        .expect("creating key-version API root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        create_active_tables(&fixture).await?;
        verify_query_with_version(&fixture).await?;
        verify_invalid_inputs_have_no_transaction_or_wal_side_effects(&fixture).await?;
        verify_empty_writable_version_transaction(&fixture).await
    })
    .unwrap_or_else(|error| panic!("key-version API contract failed: {error}"));
}

async fn verify_query_with_version(
    fixture: &key_version_support::Fixture,
) -> TestResult<()> {
    let meta_key = encode_atom(MEMORY_TABLE);
    let (meta_value, meta_version) = fixture
        .db
        .query_with_version(Atom::from(META_TABLE), meta_key.clone())
        .await
        .map_err(|error| format!("querying real Meta entry with version failed: {error:?}"))?;
    if meta_value.is_none() || !matches!(meta_version, Version::Upsert(_)) {
        return Err(format!(
            "real Meta entry must return Some + Upsert, value_present={}, version={meta_version:?}",
            meta_value.is_some(),
        ));
    }
    let repeated_meta = fixture
        .db
        .query_with_version(Atom::from(META_TABLE), meta_key)
        .await
        .map_err(|error| format!("repeating real Meta version query failed: {error:?}"))?;
    expect_binary("repeated Meta value", repeated_meta.0.as_ref(), meta_value.as_ref())?;
    expect_eq("repeated Meta version", &repeated_meta.1, &meta_version)?;

    let key = encode_usize(10_001);
    let value = encode_usize(20_001);
    let (missing, missing_version) = fixture
        .db
        .query_with_version(Atom::from(MEMORY_TABLE), key.clone())
        .await
        .map_err(|error| format!("querying missing Memory key with version failed: {error:?}"))?;
    expect_binary("missing Memory value", missing.as_ref(), None)?;
    if !matches!(missing_version, Version::Delete(_)) {
        return Err(format!(
            "missing Memory key must return Delete version, observed {missing_version:?}",
        ));
    }
    let repeated_missing = fixture
        .db
        .query_with_version(Atom::from(MEMORY_TABLE), key.clone())
        .await
        .map_err(|error| format!("repeating missing Memory version query failed: {error:?}"))?;
    expect_binary("repeated missing Memory value", repeated_missing.0.as_ref(), None)?;
    expect_eq("repeated missing Memory version",
              &repeated_missing.1,
              &missing_version)?;

    let write = writable_transaction(&fixture.db, "API ordinary version advance")?;
    write
        .upsert(vec![TableKV::new(
            Atom::from(MEMORY_TABLE),
            key.clone(),
            Some(value.clone()),
        )])
        .await
        .map_err(|error| format!("ordinary Memory upsert failed: {error:?}"))?;
    commit_ordinary(&write, "API ordinary version advance").await?;

    let (committed, committed_version) = fixture
        .db
        .query_with_version(Atom::from(MEMORY_TABLE), key.clone())
        .await
        .map_err(|error| format!("querying committed Memory key with version failed: {error:?}"))?;
    expect_binary("committed Memory value", committed.as_ref(), Some(&value))?;
    if !matches!(committed_version, Version::Upsert(_)) {
        return Err(format!(
            "committed Memory key must return Upsert version, observed {committed_version:?}",
        ));
    }
    if committed_version == missing_version {
        return Err("ordinary commit did not advance the Memory key version".to_owned());
    }

    let mut payloads = HashSet::new();
    payloads.insert(TableKeyVersion {
        table: Atom::from(MEMORY_TABLE),
        key: key.clone(),
        version: committed_version.clone(),
    });
    payloads.insert(TableKeyVersion {
        table: Atom::from(MEMORY_TABLE),
        key,
        version: committed_version,
    });
    expect_eq("TableKeyVersion Eq/Hash duplicate collapse", &payloads.len(), &1usize)?;

    // 三类用户表都必须已由真实 DDL 注册，避免夹具只覆盖 Memory 的伪装配。
    for table in [LOG_ORDERED_TABLE, BTREE_TABLE] {
        expect_eq(&format!("{table} registration"),
                  &fixture.db.is_exist(&Atom::from(table)).await,
                  &true)?;
    }
    Ok(())
}

async fn verify_invalid_inputs_have_no_transaction_or_wal_side_effects(
    fixture: &key_version_support::Fixture,
) -> TestResult<()> {
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();

    assert_common_normal(
        fixture
            .db
            .query_with_version(Atom::from(""), encode_usize(1))
            .await
            .expect_err("empty table name must be rejected"),
        "empty qwv table",
    )?;
    assert_common_normal(
        fixture
            .db
            .query_with_version(Atom::from(MEMORY_TABLE), Binary::new(Vec::new()))
            .await
            .expect_err("empty qwv key must be rejected"),
        "empty qwv key",
    )?;
    assert_common_normal(
        fixture
            .db
            .query_with_version(
                Atom::from(MEMORY_TABLE),
                Binary::new(vec![7u8; u16::MAX as usize + 1]),
            )
            .await
            .expect_err("oversized qwv key must be rejected"),
        "oversized qwv key",
    )?;
    assert_common_normal(
        fixture
            .db
            .query_with_version(Atom::from("x".repeat(4097)), encode_usize(1))
            .await
            .expect_err("oversized qwv table must be rejected"),
        "oversized qwv table",
    )?;

    let read_only = read_only_transaction(&fixture.db, "read-only version prepare")?;
    assert_common_normal(
        read_only
            .prepare_with_version(Vec::new(), Vec::new())
            .await
            .expect_err("read-only root must reject version prepare"),
        "read-only version prepare",
    )?;
    expect_eq("read-only rejected transaction UID",
              &read_only.get_transaction_uid(),
              &None)?;

    let observed_key = encode_usize(10_001);
    let observed = fixture
        .db
        .query_with_version(Atom::from(MEMORY_TABLE), observed_key.clone())
        .await
        .map_err(|error| format!("loading duplicate-input baseline failed: {error:?}"))?;
    let expected = TableKeyVersion {
        table: Atom::from(MEMORY_TABLE),
        key: observed_key.clone(),
        version: observed.1,
    };

    let duplicate_read = writable_transaction(&fixture.db, "duplicate version read set")?;
    assert_common_normal(
        duplicate_read
            .prepare_with_version(vec![expected.clone(), expected], Vec::new())
            .await
            .expect_err("duplicate version read set must be rejected"),
        "duplicate version read set",
    )?;
    expect_eq("duplicate-read transaction UID",
              &duplicate_read.get_transaction_uid(),
              &None)?;

    let duplicate_write = writable_transaction(&fixture.db, "duplicate version write set")?;
    let write = TableKV::new(
        Atom::from(MEMORY_TABLE),
        encode_usize(30_001),
        Some(encode_usize(40_001)),
    );
    assert_common_normal(
        duplicate_write
            .prepare_with_version(Vec::new(), vec![write.clone(), write])
            .await
            .expect_err("duplicate version write set must be rejected"),
        "duplicate version write set",
    )?;
    expect_eq("duplicate-write transaction UID",
              &duplicate_write.get_transaction_uid(),
              &None)?;

    let empty_value = writable_transaction(&fixture.db, "empty version value")?;
    assert_common_normal(
        empty_value
            .prepare_with_version(
                Vec::new(),
                vec![TableKV::new(
                    Atom::from(MEMORY_TABLE),
                    encode_usize(30_002),
                    Some(Binary::new(Vec::new())),
                )],
            )
            .await
            .expect_err("empty persisted value must be rejected"),
        "empty version value",
    )?;
    expect_eq("empty-value transaction UID",
              &empty_value.get_transaction_uid(),
              &None)?;

    expect_eq("invalid-input produced transactions",
              &fixture.tr_manager.produced_transaction_total(),
              &produced_before)?;
    expect_eq("invalid-input consumed transactions",
              &fixture.tr_manager.consumed_transaction_total(),
              &consumed_before)?;
    expect_eq("invalid-input active transactions",
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq("invalid-input WAL appends",
              &fixture.logger.append_total_count(),
              &append_before)
}

async fn verify_empty_writable_version_transaction(
    fixture: &key_version_support::Fixture,
) -> TestResult<()> {
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let transaction = writable_transaction(&fixture.db, "empty writable version transaction")?;
    let prepare = transaction
        .prepare_with_version(Vec::new(), Vec::new())
        .await
        .map_err(|error| format!("empty writable version prepare failed: {error:?}"))?;
    expect_eq("empty version prepare output", &prepare, &Vec::<u8>::new())?;
    if transaction.get_transaction_uid().is_none() {
        return Err("empty writable version prepare did not allocate a transaction UID".to_owned());
    }
    let receipt = transaction
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("empty writable version commit failed: {error:?}"))?;
    expect_eq("empty version receipt", &receipt, &Vec::<TableKeyVersion>::new())?;
    expect_eq("empty version produced increment",
              &fixture.tr_manager.produced_transaction_total(),
              &(produced_before + 1))?;
    expect_eq("empty version consumed increment",
              &fixture.tr_manager.consumed_transaction_total(),
              &(consumed_before + 1))?;
    expect_eq("empty version active transactions",
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq("empty version WAL appends",
              &fixture.logger.append_total_count(),
              &append_before)
}

fn assert_common_normal(error: KVTableTrError, label: &str) -> TestResult<()> {
    if !error.is_common() || !matches!(error.level(), ErrorLevel::Normal) {
        Err(format!(
            "{label}: expected Common(Normal), observed {error:?}",
        ))
    } else {
        Ok(())
    }
}

fn assert_send_sync<T: Send + Sync>() {}
