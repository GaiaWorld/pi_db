//! Key 版本事务两阶段完整冲突的真实生产专项。
//!
//! 本 target 在真实 Meta/Memory/LogOrdered/Btree 装配中分别固定：版本阶段必须优先并返回
//! 确定性完整集合；版本匹配后标准 prepare 预留仍返回完整集合；版本 Read 预留会阻止普通和
//! DirtyWrite；写表身份错误的 Common 优先于可合并冲突。每个非 Fatal 失败都精确 rollback，
//! 并检查状态、manager 计数、WAL 增量和最终值。LogWrite 行为按 HC-059 不在本 target 执行。

mod key_version_support;

use std::time::Duration;

use pi_async_transaction::{
    manager_2pc::Transaction2PcStatus,
    AsyncCommitLog, ErrorLevel, UnitTransaction,
};
use pi_atom::Atom;
use pi_db::{
    tables::TableKV,
    Binary, KVTableTrError, TableKeyConflict, TableKeyVersion, VersionConflictKind,
};

use key_version_support::{
    BTREE_TABLE, LOG_ORDERED_TABLE, MEMORY_TABLE, TestResult, TempRoot, build_database,
    commit_ordinary, create_active_tables, encode_usize, expect_binary, expect_eq,
    query_ordinary, run_on_runtime, writable_transaction,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(90);

#[test]
fn test_key_version_complete_conflict_protocol() {
    let root = TempRoot::new("conflict_real")
        .expect("creating key-version conflict root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        create_active_tables(&fixture).await?;
        let produced_before = fixture.tr_manager.produced_transaction_total();
        let consumed_before = fixture.tr_manager.consumed_transaction_total();

        verify_version_phase_complete_set(&fixture).await?;
        verify_standard_phase_complete_set(&fixture).await?;
        verify_read_reservation_blocks_safe_and_dirty_writes(&fixture).await?;
        verify_common_write_identity_error_has_priority(&fixture).await?;

        expect_eq("conflict target produced/consumed balance",
                  &(fixture.tr_manager.produced_transaction_total() - produced_before),
                  &(fixture.tr_manager.consumed_transaction_total() - consumed_before))?;
        expect_eq("conflict target active transaction registry",
                  &fixture.tr_manager.transaction_len(),
                  &0usize)
    })
    .unwrap_or_else(|error| panic!("key-version conflict protocol failed: {error}"));
}

async fn verify_version_phase_complete_set(
    fixture: &key_version_support::Fixture,
) -> TestResult<()> {
    let cases = [
        (MEMORY_TABLE, encode_usize(101), encode_usize(1_101)),
        (LOG_ORDERED_TABLE, encode_usize(102), encode_usize(1_102)),
        (BTREE_TABLE, encode_usize(103), encode_usize(1_103)),
    ];
    let mut stale_reads = Vec::new();
    for (table, key, _) in &cases {
        let (value, version) = fixture
            .db
            .query_with_version(Atom::from(*table), key.clone())
            .await
            .map_err(|error| format!("loading {table} stale baseline failed: {error:?}"))?;
        expect_binary(&format!("{table} initial missing value"), value.as_ref(), None)?;
        stale_reads.push(TableKeyVersion {
            table: Atom::from(*table),
            key: key.clone(),
            version,
        });
    }

    let owner = writable_transaction(&fixture.db, "version-phase owner")?;
    owner
        .upsert(cases
            .iter()
            .map(|(table, key, value)| {
                TableKV::new(Atom::from(*table), key.clone(), Some(value.clone()))
            })
            .collect())
        .await
        .map_err(|error| format!("version-phase owner upsert failed: {error:?}"))?;
    commit_ordinary(&owner, "version-phase owner").await?;

    let append_before = fixture.logger.append_total_count();
    let stale = writable_transaction(&fixture.db, "version-phase stale")?;
    let error = stale
        .prepare_with_version(stale_reads.clone(), Vec::new())
        .await
        .expect_err("all stale versions must fail in phase one");
    assert_all_conflicts(
        &error,
        stale_reads
            .iter()
            .map(|item| TableKeyConflict {
                table: item.table.clone(),
                key: item.key.clone(),
                kind: VersionConflictKind::ReadSetVersionMismatch,
            })
            .collect(),
        "version phase complete set",
    )?;
    assert_prepare_failed_then_rollback(&stale, "version-phase stale").await?;
    expect_eq("version-phase rejected WAL",
              &fixture.logger.append_total_count(),
              &append_before)?;
    for (table, key, value) in &cases {
        let actual = query_ordinary(&fixture.db,
                                    table,
                                    key.clone(),
                                    "version-phase final query").await?;
        expect_binary(&format!("{table} version-phase final value"),
                      actual.as_ref(),
                      Some(value))?;
    }
    Ok(())
}

async fn verify_standard_phase_complete_set(
    fixture: &key_version_support::Fixture,
) -> TestResult<()> {
    let cases = [
        (MEMORY_TABLE, encode_usize(201), encode_usize(1_201)),
        (BTREE_TABLE, encode_usize(202), encode_usize(1_202)),
    ];
    let mut reads = Vec::new();
    for (table, key, _) in &cases {
        let (_, version) = fixture
            .db
            .query_with_version(Atom::from(*table), key.clone())
            .await
            .map_err(|error| format!("loading {table} reservation baseline failed: {error:?}"))?;
        reads.push(TableKeyVersion {
            table: Atom::from(*table),
            key: key.clone(),
            version,
        });
    }

    let owner = writable_transaction(&fixture.db, "standard-phase reservation owner")?;
    owner
        .upsert(cases
            .iter()
            .map(|(table, key, value)| {
                TableKV::new(Atom::from(*table), key.clone(), Some(value.clone()))
            })
            .collect())
        .await
        .map_err(|error| format!("reservation owner upsert failed: {error:?}"))?;
    let owner_prepare = owner
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("reservation owner prepare failed: {error:?}"))?;
    expect_eq("reservation owner status",
              &owner.get_status(),
              &Transaction2PcStatus::Prepared)?;

    let append_before = fixture.logger.append_total_count();
    let contender = writable_transaction(&fixture.db, "standard-phase version contender")?;
    let error = contender
        .prepare_with_version(
            reads,
            cases
                .iter()
                .map(|(table, key, value)| {
                    TableKV::new(Atom::from(*table), key.clone(), Some(value.clone()))
                })
                .collect(),
        )
        .await
        .expect_err("matching versions must still honor all prepared reservations");
    assert_all_conflicts(
        &error,
        cases
            .iter()
            .map(|(table, key, _)| TableKeyConflict {
                table: Atom::from(*table),
                key: key.clone(),
                kind: VersionConflictKind::TransactionConflict,
            })
            .collect(),
        "standard phase complete set",
    )?;
    assert_prepare_failed_then_rollback(&contender, "standard-phase contender").await?;
    expect_eq("standard-phase rejected WAL",
              &fixture.logger.append_total_count(),
              &append_before)?;

    owner
        .commit_modified(owner_prepare)
        .await
        .map_err(|error| format!("committing reservation owner failed: {error:?}"))?;
    for (table, key, value) in &cases {
        let actual = query_ordinary(&fixture.db,
                                    table,
                                    key.clone(),
                                    "standard-phase final query").await?;
        expect_binary(&format!("{table} standard-phase final value"),
                      actual.as_ref(),
                      Some(value))?;
    }
    Ok(())
}

async fn verify_read_reservation_blocks_safe_and_dirty_writes(
    fixture: &key_version_support::Fixture,
) -> TestResult<()> {
    let key = encode_usize(301);
    let (_, version) = fixture
        .db
        .query_with_version(Atom::from(MEMORY_TABLE), key.clone())
        .await
        .map_err(|error| format!("loading read-reservation baseline failed: {error:?}"))?;
    let read = TableKeyVersion {
        table: Atom::from(MEMORY_TABLE),
        key: key.clone(),
        version,
    };

    let reader = writable_transaction(&fixture.db, "version read reservation")?;
    let reader_prepare = reader
        .prepare_with_version(vec![read.clone()], Vec::new())
        .await
        .map_err(|error| format!("version read-only prepare failed: {error:?}"))?;
    expect_eq("version reader prepared status",
              &reader.get_status(),
              &Transaction2PcStatus::Prepared)?;

    for dirty in [false, true] {
        let label = if dirty { "dirty writer" } else { "safe writer" };
        let writer = writable_transaction(&fixture.db, label)?;
        let write = vec![TableKV::new(
            Atom::from(MEMORY_TABLE),
            key.clone(),
            Some(encode_usize(if dirty { 3_102 } else { 3_101 })),
        )];
        if dirty {
            writer
                .dirty_upsert(write)
                .await
                .map_err(|error| format!("{label} action failed: {error:?}"))?;
        } else {
            writer
                .upsert(write)
                .await
                .map_err(|error| format!("{label} action failed: {error:?}"))?;
        }
        let error = writer
            .prepare_modified_conflicts()
            .await
            .expect_err("version Read reservation must block safe and dirty writes");
        assert_first_conflict(&error, MEMORY_TABLE, &key, label)?;
        assert_prepare_failed_then_rollback(&writer, label).await?;
    }

    let receipt = reader
        .commit_with_version(reader_prepare)
        .await
        .map_err(|error| format!("committing version reader failed: {error:?}"))?;
    expect_eq("version reader empty receipt",
              &receipt,
              &Vec::<TableKeyVersion>::new())?;

    // 反向边界：普通 DirtyWrite 已登记时，Versioned Read 同样必须在标准阶段冲突。
    let dirty_owner = writable_transaction(&fixture.db, "ordinary dirty reservation")?;
    dirty_owner
        .dirty_upsert(vec![TableKV::new(
            Atom::from(MEMORY_TABLE),
            key.clone(),
            Some(encode_usize(3_103)),
        )])
        .await
        .map_err(|error| format!("ordinary dirty reservation action failed: {error:?}"))?;
    let dirty_prepare = dirty_owner
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("ordinary dirty reservation prepare failed: {error:?}"))?;

    let version_reader = writable_transaction(&fixture.db, "version reader against dirty")?;
    let error = version_reader
        .prepare_with_version(vec![read], Vec::new())
        .await
        .expect_err("version Read must conflict with existing ordinary DirtyWrite");
    assert_all_conflicts(
        &error,
        vec![TableKeyConflict {
            table: Atom::from(MEMORY_TABLE),
            key: key.clone(),
            kind: VersionConflictKind::TransactionConflict,
        }],
        "version reader against dirty",
    )?;
    assert_prepare_failed_then_rollback(&version_reader,
                                        "version reader against dirty").await?;
    dirty_owner
        .commit_modified(dirty_prepare)
        .await
        .map_err(|error| format!("committing ordinary dirty reservation failed: {error:?}"))?;

    let final_value = query_ordinary(&fixture.db,
                                     MEMORY_TABLE,
                                     key,
                                     "read-reservation final query").await?;
    expect_binary("read-reservation final value",
                  final_value.as_ref(),
                  Some(&encode_usize(3_103)))
}

async fn verify_common_write_identity_error_has_priority(
    fixture: &key_version_support::Fixture,
) -> TestResult<()> {
    let key = encode_usize(401);
    let (_, stale_version) = fixture
        .db
        .query_with_version(Atom::from(MEMORY_TABLE), key.clone())
        .await
        .map_err(|error| format!("loading Common-priority baseline failed: {error:?}"))?;
    let owner = writable_transaction(&fixture.db, "Common-priority owner")?;
    owner
        .upsert(vec![TableKV::new(
            Atom::from(MEMORY_TABLE),
            key.clone(),
            Some(encode_usize(4_101)),
        )])
        .await
        .map_err(|error| format!("Common-priority owner action failed: {error:?}"))?;
    commit_ordinary(&owner, "Common-priority owner").await?;

    let append_before = fixture.logger.append_total_count();
    let transaction = writable_transaction(&fixture.db, "Common-priority contender")?;
    let error = transaction
        .prepare_with_version(
            vec![TableKeyVersion {
                table: Atom::from(MEMORY_TABLE),
                key,
                version: stale_version,
            }],
            vec![TableKV::new(
                Atom::from("missing_version_write_table"),
                encode_usize(402),
                Some(encode_usize(4_102)),
            )],
        )
        .await
        .expect_err("missing write table must return Common before version conflicts");
    if !error.is_common() || !matches!(error.level(), ErrorLevel::Normal) {
        return Err(format!(
            "Common-priority path expected Common(Normal), observed {error:?}",
        ));
    }
    assert_prepare_failed_then_rollback(&transaction, "Common-priority contender").await?;
    expect_eq("Common-priority rejected WAL",
              &fixture.logger.append_total_count(),
              &append_before)
}

fn assert_all_conflicts(
    error: &KVTableTrError,
    mut expected: Vec<TableKeyConflict>,
    label: &str,
) -> TestResult<()> {
    if !error.is_all_conflicts() || !matches!(error.level(), ErrorLevel::Normal) {
        return Err(format!(
            "{label}: expected AllConflicts(Normal), observed {error:?}",
        ));
    }
    expected.sort_by(|left, right| {
        left.table
            .as_str()
            .as_bytes()
            .cmp(right.table.as_str().as_bytes())
            .then_with(|| left.key.as_ref().cmp(right.key.as_ref()))
    });
    let actual = error
        .all_conflicts()
        .ok_or_else(|| format!("{label}: AllConflicts accessor returned None"))?;
    expect_eq(&format!("{label} exact set"), &actual, &&expected[..])?;
    let first = error
        .conflicts()
        .ok_or_else(|| format!("{label}: compatibility first-conflict accessor returned None"))?;
    expect_eq(&format!("{label} first table"), first.0, &expected[0].table)?;
    expect_eq(&format!("{label} first key"), first.1, &expected[0].key)
}

fn assert_first_conflict(
    error: &KVTableTrError,
    table: &str,
    key: &Binary,
    label: &str,
) -> TestResult<()> {
    if error.is_all_conflicts() || !error.is_conflicts()
        || !matches!(error.level(), ErrorLevel::Normal) {
        return Err(format!(
            "{label}: expected ordinary first Conflicts(Normal), observed {error:?}",
        ));
    }
    let actual = error
        .conflicts()
        .ok_or_else(|| format!("{label}: first conflict accessor returned None"))?;
    expect_eq(&format!("{label} conflict table"),
              &actual.0.as_str(),
              &table)?;
    expect_eq(&format!("{label} conflict key"), actual.1, key)
}

async fn assert_prepare_failed_then_rollback(
    transaction: &key_version_support::RealTransaction,
    label: &str,
) -> TestResult<()> {
    expect_eq(&format!("{label} failed status"),
              &transaction.get_status(),
              &Transaction2PcStatus::PrepareFailed)?;
    transaction
        .rollback_modified()
        .await
        .map_err(|error| format!("{label} rollback failed: {error:?}"))?;
    expect_eq(&format!("{label} rollback status"),
              &transaction.get_status(),
              &Transaction2PcStatus::Rollbacked)
}
