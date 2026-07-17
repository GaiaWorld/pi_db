//! Key 版本发布和回执的真实生产专项。
//!
//! 正常矩阵使用真实 4-worker runtime、事务管理器、CommitLogger、Meta/Memory/LogOrdered/
//! Btree 和文件系统，验证版本协议与普通协议都在根 WAL 成功后发布数据及 Key 版本。
//!
//! 本 target 不验证异步数据文件失败后的 repair；该完整链路由
//! `commit_confirmation_real_environment` 的 WAL/最终数据双硬门禁负责。这里聚焦“根 WAL 成功
//! 是数据/版本 publication 的前置条件”和“回执只描述本事务最终写入”。
//!
//! 根 WAL 自身因磁盘、文件系统、设备、runtime 或文件大小限制导致的 append/flush 失败不在
//! 当前事务安全保证内，也不进入本 target。此前真实 `EFBIG` 零字节诊断及当前实现边界归档于
//! `LIMIT-ROOT-WAL-IO-001`；它不是空 WAL，且不得作为“应安全 rollback”的新回归断言。

mod key_version_support;

use std::time::{Duration, Instant};

use pi_async_rt::rt::AsyncRuntime;
use pi_async_transaction::{AsyncCommitLog, Transaction2Pc};
use pi_atom::Atom;
use pi_db::{
    Binary, TableKeyVersion, Version,
    tables::TableKV,
};
use pi_guid::Guid;

use key_version_support::{
    BTREE_TABLE, LOG_ORDERED_TABLE, MEMORY_TABLE, META_TABLE, Fixture, TempRoot, TestResult,
    build_database, create_active_tables, encode_atom, encode_usize, expect_binary, expect_eq,
    run_on_runtime, writable_transaction,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(90);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(75);

#[test]
fn test_key_version_publication_real() {
    let root = TempRoot::new("publication_real")
        .expect("creating key-version publication root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        create_active_tables(&fixture).await?;
        verify_successful_publication(&rt, &fixture).await
    })
    .unwrap_or_else(|error| panic!("key-version publication matrix failed: {error}"));
}

async fn verify_successful_publication(
    rt: &pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
    fixture: &Fixture,
) -> TestResult<()> {
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();

    let meta_key = encode_atom(MEMORY_TABLE);
    let (meta_value, meta_version) = fixture
        .db
        .query_with_version(Atom::from(META_TABLE), meta_key.clone())
        .await
        .map_err(|error| format!("loading Meta publication baseline failed: {error:?}"))?;
    let meta_value = meta_value
        .ok_or_else(|| "the real Memory table Meta entry is absent".to_owned())?;

    let user_keys = [
        (MEMORY_TABLE, encode_usize(11_001), encode_usize(21_001), encode_usize(31_001)),
        (LOG_ORDERED_TABLE, encode_usize(11_002), encode_usize(21_002), encode_usize(31_002)),
        (BTREE_TABLE, encode_usize(11_003), encode_usize(21_003), encode_usize(31_003)),
    ];
    let mut read_set = vec![TableKeyVersion {
        table: Atom::from(META_TABLE),
        key: meta_key.clone(),
        version: meta_version,
    }];
    for (table, key, _, _) in &user_keys {
        let (value, version) = fixture
            .db
            .query_with_version(Atom::from(*table), key.clone())
            .await
            .map_err(|error| format!("loading {table} publication baseline failed: {error:?}"))?;
        expect_binary(&format!("{table} initial publication value"), value.as_ref(), None)?;
        if !matches!(version, Version::Delete(_)) {
            return Err(format!(
                "{table} missing publication baseline must be Delete, observed {version:?}",
            ));
        }
        read_set.push(TableKeyVersion {
            table: Atom::from(*table),
            key: key.clone(),
            version,
        });
    }

    let append_before = fixture.logger.append_total_count();
    let version_writer = writable_transaction(&fixture.db, "version publication writer")?;
    let mut first_writes = vec![TableKV::new(
        Atom::from(META_TABLE),
        meta_key.clone(),
        Some(meta_value.clone()),
    )];
    first_writes.extend(user_keys.iter().map(|(table, key, first, _)| {
        TableKV::new(Atom::from(*table), key.clone(), Some(first.clone()))
    }));
    let prepare = version_writer
        .prepare_with_version(read_set, first_writes)
        .await
        .map_err(|error| format!("preparing multi-table version write failed: {error:?}"))?;
    let version_uid = version_writer
        .get_transaction_uid()
        .ok_or_else(|| "version prepare did not allocate a transaction UID".to_owned())?;
    let receipt = version_writer
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("committing multi-table version write failed: {error:?}"))?;
    let mut first_expected = vec![(META_TABLE, meta_key.clone(), true, version_uid.clone())];
    first_expected.extend(user_keys.iter().map(|(table, key, _, _)| {
        (*table, key.clone(), true, version_uid.clone())
    }));
    assert_receipts(&receipt, &first_expected, "version multi-table receipt")?;
    expect_eq("version multi-table root WAL append",
              &fixture.logger.append_total_count(),
              &(append_before + 1))?;

    assert_query_version(&fixture,
                         META_TABLE,
                         meta_key.clone(),
                         Some(&meta_value),
                         &Version::Upsert(version_uid.clone()),
                         "version Meta publication").await?;
    for (table, key, first, _) in &user_keys {
        assert_query_version(&fixture,
                             table,
                             key.clone(),
                             Some(first),
                             &Version::Upsert(version_uid.clone()),
                             &format!("version {table} publication")).await?;
    }

    // 普通 commit 不返回回执，但必须发布同一个根事务 UID，供之后的 qwv 精确观察。
    let ordinary_append_before = fixture.logger.append_total_count();
    let ordinary = writable_transaction(&fixture.db, "ordinary publication writer")?;
    let mut second_writes = vec![TableKV::new(
        Atom::from(META_TABLE),
        meta_key.clone(),
        Some(meta_value.clone()),
    )];
    second_writes.extend(user_keys.iter().map(|(table, key, _, second)| {
        TableKV::new(Atom::from(*table), key.clone(), Some(second.clone()))
    }));
    ordinary
        .upsert(second_writes)
        .await
        .map_err(|error| format!("ordinary multi-table upsert failed: {error:?}"))?;
    let ordinary_prepare = ordinary
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("ordinary multi-table prepare failed: {error:?}"))?;
    let ordinary_uid = ordinary
        .get_transaction_uid()
        .ok_or_else(|| "ordinary prepare did not allocate a transaction UID".to_owned())?;
    ordinary
        .commit_modified(ordinary_prepare)
        .await
        .map_err(|error| format!("ordinary multi-table commit failed: {error:?}"))?;
    expect_eq("ordinary multi-table root WAL append",
              &fixture.logger.append_total_count(),
              &(ordinary_append_before + 1))?;

    let ordinary_meta_version = Version::Upsert(ordinary_uid.clone());
    assert_query_version(&fixture,
                         META_TABLE,
                         meta_key.clone(),
                         Some(&meta_value),
                         &ordinary_meta_version,
                         "ordinary Meta publication").await?;
    for (table, key, _, second) in &user_keys {
        assert_query_version(&fixture,
                             table,
                             key.clone(),
                             Some(second),
                             &Version::Upsert(ordinary_uid.clone()),
                             &format!("ordinary {table} publication")).await?;
    }

    let mut delete_reads = Vec::new();
    for (table, key, _, second) in &user_keys {
        let (value, version) = fixture
            .db
            .query_with_version(Atom::from(*table), key.clone())
            .await
            .map_err(|error| format!("loading {table} delete baseline failed: {error:?}"))?;
        expect_binary(&format!("{table} delete baseline"), value.as_ref(), Some(second))?;
        delete_reads.push(TableKeyVersion {
            table: Atom::from(*table),
            key: key.clone(),
            version,
        });
    }
    let delete_append_before = fixture.logger.append_total_count();
    let deleter = writable_transaction(&fixture.db, "version publication deleter")?;
    let delete_prepare = deleter
        .prepare_with_version(
            delete_reads,
            user_keys
                .iter()
                .map(|(table, key, _, _)| {
                    TableKV::new(Atom::from(*table), key.clone(), None)
                })
                .collect(),
        )
        .await
        .map_err(|error| format!("preparing multi-table version delete failed: {error:?}"))?;
    let delete_uid = deleter
        .get_transaction_uid()
        .ok_or_else(|| "version delete prepare did not allocate a transaction UID".to_owned())?;
    let delete_receipt = deleter
        .commit_with_version(delete_prepare)
        .await
        .map_err(|error| format!("committing multi-table version delete failed: {error:?}"))?;
    let delete_expected: Vec<_> = user_keys
        .iter()
        .map(|(table, key, _, _)| (*table, key.clone(), false, delete_uid.clone()))
        .collect();
    assert_receipts(&delete_receipt, &delete_expected, "version delete receipt")?;
    expect_eq("version delete root WAL append",
              &fixture.logger.append_total_count(),
              &(delete_append_before + 1))?;
    for (table, key, _, _) in &user_keys {
        assert_query_version(&fixture,
                             table,
                             key.clone(),
                             None,
                             &Version::Delete(delete_uid.clone()),
                             &format!("version {table} delete publication")).await?;
    }
    assert_query_version(&fixture,
                         META_TABLE,
                         meta_key,
                         Some(&meta_value),
                         &ordinary_meta_version,
                         "unmodified Meta publication").await?;

    wait_for_all_confirmed(rt, fixture, CONFIRM_TIMEOUT, "publication writes").await?;
    expect_eq("publication active transaction registry",
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq("publication produced/consumed balance",
              &(fixture.tr_manager.produced_transaction_total() - produced_before),
              &(fixture.tr_manager.consumed_transaction_total() - consumed_before))
}

async fn assert_query_version(
    fixture: &Fixture,
    table: &str,
    key: Binary,
    expected_value: Option<&Binary>,
    expected_version: &Version,
    label: &str,
) -> TestResult<()> {
    let (value, version) = fixture
        .db
        .query_with_version(Atom::from(table), key)
        .await
        .map_err(|error| format!("{label} qwv failed: {error:?}"))?;
    expect_binary(&format!("{label} value"), value.as_ref(), expected_value)?;
    expect_eq(&format!("{label} version"), &version, expected_version)
}

fn assert_receipts(
    receipts: &[TableKeyVersion],
    expected: &[(&str, Binary, bool, Guid)],
    label: &str,
) -> TestResult<()> {
    expect_eq(&format!("{label} length"), &receipts.len(), &expected.len())?;
    for (table, key, upsert, uid) in expected {
        let matching: Vec<_> = receipts
            .iter()
            .filter(|receipt| {
                receipt.table.as_str() == *table && receipt.key.as_ref() == key.as_ref()
            })
            .collect();
        if matching.len() != 1 {
            return Err(format!(
                "{label}: expected one receipt for table={table:?}, key={:?}, observed {}",
                key.as_ref(),
                matching.len(),
            ));
        }
        let valid = match (&matching[0].version, *upsert) {
            (Version::Upsert(actual), true) | (Version::Delete(actual), false) => actual == uid,
            _ => false,
        };
        if !valid {
            return Err(format!(
                "{label}: receipt for table={table:?}, key={:?} has wrong operation/UID: {:?}",
                key.as_ref(),
                matching[0].version,
            ));
        }
    }
    Ok(())
}

async fn wait_for_all_confirmed(
    rt: &pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
    fixture: &Fixture,
    timeout: Duration,
    label: &str,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let waiting = fixture.logger.waiting_confirm_count().await;
        let appended = fixture.logger.append_total_count();
        let confirmed = fixture.logger.confirm_total_count();
        if waiting == 0 && appended == confirmed {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{label} did not confirm before {timeout:?}: waiting={waiting}, appended={appended}, confirmed={confirmed}",
            ));
        }
        rt.timeout(5).await;
    }
}
