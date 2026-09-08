//! Schema 前置事务与多表版本提交、根 WAL 修复和冷启动数据状态的真实专项。
//!
//! 本 target 只使用公开数据库 API，并通过四个独立进程验证冻结契约：同一根先创建 Memory、
//! LogOrdered、Btree，再以一个版本 2PC 写三表；Schema Meta 必须先进入同一根 WAL，但不得进入
//! 公开回执。`setup` 在表 collector 确认前退出，`inspect-wal` 离线逐项核对真实未确认 WAL 的
//! TID/CID、Meta/业务段顺序和 Key/Value 内容，`recover` 再使用同一 WAL 执行生产
//! `try_repair`，`inspect-data-only` 移走已确认 WAL 后仅从数据目录冷启动。修复是否成功同时以
//! WAL 实际内容、`.bak` 和最终数据为硬门禁，不能只根据进程内 COW 根、logger 计数或 API
//! 返回 Ok 下结论。
//!
//! Memory 的 `persistence=true` 只表示写根 WAL，不提供独立数据文件；因此 WAL 已确认并移走后，
//! data-only 冷启动应恢复表定义但不恢复 Memory 值。LogOrdered/Btree 必须从各自数据文件恢复值。
//! 本 target 等待生产 60 秒 collector，属于串行慢速恢复专项，不进入 TSan 最小负载 target。

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
use pi_async_transaction::{AsyncCommitLog, Transaction2Pc};
use pi_atom::Atom;
use pi_db::{
    inspector::CommitLogInspector,
    tables::TableKV,
    utils::CreateTableOptions,
    Binary, KVDBTableType, KVTableMeta, TableKeyVersion, Version,
};
use pi_store::commit_logger::CommitLoggerBuilder;

use key_version_support::{
    Fixture, TestResult, build_database, encode_usize, expect_binary, expect_eq,
    read_only_transaction, run_on_runtime, table_meta, writable_transaction,
};

const TEST_NAME: &str = "test_schema_protocol_multi_table_recovery";
const PHASE_ENV: &str = "PI_DB_SCHEMA_PROTOCOL_RECOVERY_PHASE";
const ROOT_ENV: &str = "PI_DB_SCHEMA_PROTOCOL_RECOVERY_ROOT";
const MEMORY_TABLE: &str = "schema_recovery_memory";
const LOG_ORDERED_TABLE: &str = "schema_recovery_log_ordered";
const BTREE_TABLE: &str = "schema_recovery_btree";
const UID_FILE: &str = "schema-recovery-transaction-uid";
const COMMIT_UID_FILE: &str = "schema-recovery-commit-uid";
const ARCHIVED_WAL_DIR: &str = "confirmed-root-wal";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(110);
const SETUP_TIMEOUT: Duration = Duration::from_secs(30);
const WAL_INSPECT_TIMEOUT: Duration = Duration::from_secs(30);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(90);
const DATA_ONLY_TIMEOUT: Duration = Duration::from_secs(30);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(80);

#[test]
fn test_schema_protocol_multi_table_recovery() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV)
                .expect("schema recovery child phase must receive its root path"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("schema recovery phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root();
    fs::create_dir_all(&root).expect("creating schema recovery root must succeed");
    for phase in ["setup", "inspect-wal", "recover", "inspect-data-only"] {
        if let Err(error) = run_phase_process(&root, phase, PROCESS_TIMEOUT) {
            panic!(
                "schema recovery failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root,
            );
        }
    }
    fs::remove_dir_all(&root).expect("cleaning schema recovery root must succeed");
}

fn run_child_phase(phase: &str, root: &Path) -> TestResult<()> {
    match phase {
        "setup" => {
            let root = root.to_path_buf();
            run_on_runtime(SETUP_TIMEOUT, move |rt| async move {
                phase_setup(rt, root).await
            })
        },
        "inspect-wal" => {
            let root = root.to_path_buf();
            run_on_runtime(WAL_INSPECT_TIMEOUT, move |rt| async move {
                phase_inspect_wal(rt, root).await
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
                phase_inspect_data_only(rt, root).await
            })
        },
        other => Err(format!("unknown schema recovery phase: {other}")),
    }
}

async fn phase_setup(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    let metas = active_table_metas();
    let transaction = writable_transaction(&fixture.db, "schema recovery create and version write")?;
    transaction
        .create_table(Atom::from(MEMORY_TABLE), metas[0].1.clone(), false)
        .await
        .map_err(|error| format!("creating schema recovery Memory failed: {error}"))?;
    transaction
        .create_table_with_options(
            Atom::from(LOG_ORDERED_TABLE),
            metas[1].1.clone(),
            CreateTableOptions::LogOrdTab(64 * 1024 * 1024, 1024 * 1024, 1024 * 1024),
            false,
        )
        .await
        .map_err(|error| format!("creating schema recovery LogOrdered failed: {error}"))?;
    transaction
        .create_table_with_options(
            Atom::from(BTREE_TABLE),
            metas[2].1.clone(),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .map_err(|error| format!("creating schema recovery Btree failed: {error}"))?;

    for (name, meta) in &metas {
        expect_eq(
            &format!("setup private schema for {name}"),
            &transaction.table_meta(Atom::from(*name)).await,
            &Some(meta.clone()),
        )?;
    }
    expect_eq("setup live table count", &fixture.db.table_size().await, &4usize)?;

    let entries = active_entries();
    let mut read_set = Vec::with_capacity(entries.len());
    let mut write_set = Vec::with_capacity(entries.len());
    for entry in &entries {
        let (value, version) = fixture
            .db
            .query_with_version(Atom::from(entry.table), entry.key.clone())
            .await
            .map_err(|error| format!("loading setup baseline for {} failed: {error:?}", entry.table))?;
        expect_binary(&format!("setup missing value for {}", entry.table), value.as_ref(), None)?;
        if !matches!(version, Version::Delete(_)) {
            return Err(format!(
                "setup missing key for {} must have Delete baseline, observed {version:?}",
                entry.table,
            ));
        }
        read_set.push(TableKeyVersion {
            table: Atom::from(entry.table),
            key: entry.key.clone(),
            version,
        });
        write_set.push(TableKV::new(
            Atom::from(entry.table),
            entry.key.clone(),
            entry.value.clone(),
        ));
    }

    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let prepare = transaction
        .prepare_with_version(read_set, write_set)
        .await
        .map_err(|error| format!("preparing schema recovery version transaction failed: {error:?}"))?;
    let transaction_uid = transaction
        .get_transaction_uid()
        .ok_or_else(|| "schema recovery prepare did not allocate a transaction TID".to_owned())?;
    let receipt = transaction
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("committing schema recovery version transaction failed: {error:?}"))?;
    let commit_uid = transaction
        .get_commit_uid()
        .ok_or_else(|| "schema recovery commit did not allocate a root WAL CID".to_owned())?;
    assert_receipt(&receipt, &entries, &transaction_uid)?;
    assert_live_values(&fixture, &entries, Some(&transaction_uid), "setup committed").await?;
    expect_eq(
        "setup produced transaction increment",
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        "setup consumed transaction increment",
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 1),
    )?;
    expect_eq("setup active transaction count", &fixture.tr_manager.transaction_len(), &0usize)?;
    expect_eq(
        "setup root WAL append increment",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;

    wait_for_unconfirmed_wal(&rt, &fixture, Duration::from_secs(3)).await?;
    expect_eq(
        "setup nonempty .bak count",
        &nonempty_bak_count(&root.join("root-wal"))?,
        &0usize,
    )?;
    fs::write(root.join(UID_FILE), transaction_uid.0.to_le_bytes())
        .map_err(|error| format!("writing schema recovery TID evidence failed: {error}"))?;
    fs::write(root.join(COMMIT_UID_FILE), commit_uid.0.to_le_bytes())
        .map_err(|error| format!("writing schema recovery CID evidence failed: {error}"))
}

async fn phase_inspect_wal(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let transaction_uid = read_guid(&root.join(UID_FILE), "TID")?;
    let commit_uid = read_guid(&root.join(COMMIT_UID_FILE), "CID")?;
    let wal_path = root.join("root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("opening schema recovery WAL for inspection failed: {error}"))?;
    let inspector = CommitLogInspector::new(rt, logger.clone());
    if !inspector.begin() {
        return Err("schema recovery WAL inspector rejected its first begin".to_owned());
    }

    let mut records = Vec::new();
    while let Some(record) = inspector.next() {
        records.push(record);
    }
    assert_wal_records(&records, &transaction_uid, &commit_uid)?;
    expect_eq(
        "WAL inspection did not create a nonempty .bak",
        &nonempty_bak_count(&wal_path)?,
        &0usize,
    )
}

async fn phase_recover(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let transaction_uid = read_transaction_uid(&root)?;
    let wal_path = root.join("root-wal");
    let bak_before = nonempty_bak_count(&wal_path)?;
    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    let metas = active_table_metas();
    expect_eq("recovered live table count", &fixture.db.table_size().await, &4usize)?;
    let verifier = read_only_transaction(&fixture.db, "verify replayed schema definitions")?;
    for (name, meta) in &metas {
        expect_eq(
            &format!("replayed schema for {name}"),
            &verifier.table_meta(Atom::from(*name)).await,
            &Some(meta.clone()),
        )?;
    }

    let entries = active_entries();
    // repair commit 会临时按原 TID 发布，但 Builder 在 DB_INITED 前按冻结契约清空所有恢复期
    // 版本。下面第一次公开读取必须重新生成版本，不能把原 TID 暴露给外部缓存。
    let recovered_versions = observe_recovered_first_versions(
        &fixture,
        &entries,
        &transaction_uid,
        "replayed first observation",
    ).await?;
    wait_for_confirmed_wal(&rt, &fixture, CONFIRM_TIMEOUT).await?;
    // 修复结论必须再次以最终业务值为硬门禁，不能只依赖 logger/.bak 状态。
    assert_live_values_and_versions(
        &fixture,
        &entries,
        &recovered_versions,
        "replayed after confirm",
    ).await?;
    expect_eq("recovery active transaction count", &fixture.tr_manager.transaction_len(), &0usize)?;
    expect_eq(
        "recovery Btree overlay drained",
        &fixture.db.table_cache_size(&Atom::from(BTREE_TABLE)).await,
        &Some(0u64),
    )?;
    let bak_after = nonempty_bak_count(&wal_path)?;
    if bak_after <= bak_before {
        return Err(format!(
            "recovery did not add a confirmed .bak checkpoint: before={bak_before}, after={bak_after}",
        ));
    }
    let active = active_file_sizes(&wal_path)?;
    if active.iter().any(|(_, len)| *len > 0) {
        return Err(format!("nonempty active WAL remains after schema recovery: {active:?}"));
    }
    Ok(())
}

async fn phase_inspect_data_only(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let transaction_uid = read_transaction_uid(&root)?;
    let wal_path = root.join("root-wal");
    let archived_wal = root.join(ARCHIVED_WAL_DIR);
    if archived_wal.exists() {
        return Err(format!("archived WAL path unexpectedly exists: {archived_wal:?}"));
    }
    fs::rename(&wal_path, &archived_wal)
        .map_err(|error| format!("archiving confirmed root WAL failed: {error}"))?;
    if nonempty_bak_count(&archived_wal)? == 0 {
        return Err("archived recovery WAL contains no nonempty .bak checkpoint".to_owned());
    }

    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    expect_eq("data-only live table count", &fixture.db.table_size().await, &4usize)?;
    let metas = active_table_metas();
    let verifier = read_only_transaction(&fixture.db, "verify data-only schema definitions")?;
    for (name, meta) in &metas {
        expect_eq(
            &format!("data-only schema for {name}"),
            &verifier.table_meta(Atom::from(*name)).await,
            &Some(meta.clone()),
        )?;
    }

    let entries = active_entries();
    for entry in &entries {
        let (value, version) = fixture
            .db
            .query_with_version(Atom::from(entry.table), entry.key.clone())
            .await
            .map_err(|error| format!("data-only query for {} failed: {error:?}", entry.table))?;
        if entry.table == MEMORY_TABLE {
            expect_binary("data-only Memory value", value.as_ref(), None)?;
            if !matches!(version, Version::Delete(_)) {
                return Err(format!(
                    "data-only Memory absence must produce Delete first-observation version, observed {version:?}",
                ));
            }
        } else {
            expect_binary(
                &format!("data-only persisted value for {}", entry.table),
                value.as_ref(),
                entry.value.as_ref(),
            )?;
            match (&entry.value, version) {
                (Some(_), Version::Upsert(uid)) if uid != transaction_uid => {},
                (None, Version::Delete(uid)) if uid != transaction_uid => {},
                other => {
                    return Err(format!(
                        "data-only {} must rebuild a fresh version matching persisted existence, observed {other:?}",
                        entry.table,
                    ));
                },
            }
        }
    }
    expect_eq("data-only WAL append count", &fixture.logger.append_total_count(), &0usize)?;
    expect_eq("data-only WAL confirm count", &fixture.logger.confirm_total_count(), &0usize)?;
    expect_eq(
        "data-only WAL waiting count",
        &fixture.logger.waiting_confirm_count().await,
        &0usize,
    )?;
    expect_eq("data-only active transaction count", &fixture.tr_manager.transaction_len(), &0usize)?;
    expect_eq(
        "data-only Btree overlay size",
        &fixture.db.table_cache_size(&Atom::from(BTREE_TABLE)).await,
        &Some(0u64),
    )
}

struct ActiveEntry {
    table: &'static str,
    key: Binary,
    value: Option<Binary>,
}

fn active_entries() -> Vec<ActiveEntry> {
    vec![
        ActiveEntry {
            table: MEMORY_TABLE,
            key: encode_usize(101),
            value: Some(encode_usize(1_101)),
        },
        ActiveEntry {
            table: MEMORY_TABLE,
            key: encode_usize(201),
            value: None,
        },
        ActiveEntry {
            table: LOG_ORDERED_TABLE,
            key: encode_usize(102),
            value: Some(encode_usize(1_102)),
        },
        ActiveEntry {
            table: LOG_ORDERED_TABLE,
            key: encode_usize(202),
            value: None,
        },
        ActiveEntry {
            table: BTREE_TABLE,
            key: encode_usize(103),
            value: Some(encode_usize(1_103)),
        },
        ActiveEntry {
            table: BTREE_TABLE,
            key: encode_usize(203),
            value: None,
        },
    ]
}

fn active_table_metas() -> Vec<(&'static str, KVTableMeta)> {
    vec![
        (MEMORY_TABLE, table_meta(KVDBTableType::MemOrdTab, true)),
        (LOG_ORDERED_TABLE, table_meta(KVDBTableType::LogOrdTab, true)),
        (BTREE_TABLE, table_meta(KVDBTableType::BtreeOrdTab, true)),
    ]
}

fn assert_wal_records(
    records: &[(String, String, String, bool, Vec<u8>, Vec<u8>)],
    transaction_uid: &pi_guid::Guid,
    commit_uid: &pi_guid::Guid,
) -> TestResult<()> {
    let metas = active_table_metas();
    let entries = active_entries();
    expect_eq(
        "inspected WAL action count",
        &records.len(),
        &(metas.len() + entries.len()),
    )?;

    let expected_tid = transaction_uid.0.to_string();
    let expected_cid = commit_uid.0.to_string();
    for (index, record) in records.iter().enumerate() {
        expect_eq(&format!("WAL action {index} TID"), &record.0, &expected_tid)?;
        expect_eq(&format!("WAL action {index} CID"), &record.1, &expected_cid)?;
    }

    // SchemaCreate 是根列表中的第一个子节点，因此所有 Meta 动作必须形成完整前缀；Meta
    // 子事务内部使用动作 Map，当前不承诺不同表名之间的稳定迭代顺序，测试只比较这个前缀的
    // 精确全集和唯一性，不能把某次哈希迭代顺序误写成公开 WAL 契约。
    let mut unmatched_meta: Vec<(Vec<u8>, Vec<u8>)> = metas
        .iter()
        .map(|(table, meta)| {
            (
                table.as_bytes().to_vec(),
                format!("{:?}", meta).into_bytes(),
            )
        })
        .collect();
    for (index, record) in records[..metas.len()].iter().enumerate() {
        expect_eq(
            &format!("WAL Meta action {index} table"),
            &record.2.as_str(),
            &".tables_meta",
        )?;
        if !record.3 {
            return Err(format!("WAL Meta action {index} was encoded as delete"));
        }
        let position = unmatched_meta
            .iter()
            .position(|(key, value)| key == &record.4 && value == &record.5)
            .ok_or_else(|| {
                format!(
                    "WAL Meta action {index} has unexpected or duplicate content: key={:?}, value={:?}",
                    record.4,
                    record.5,
                )
            })?;
        unmatched_meta.remove(position);
    }
    if !unmatched_meta.is_empty() {
        return Err(format!("WAL Meta prefix omitted expected actions: {unmatched_meta:?}"));
    }

    // 版本输入首次出现表的顺序是 Memory -> LogOrdered -> Btree，根按同一子节点顺序拼接
    // prepare 输出，因此三个表段的位置必须精确稳定。每个表的动作来自 XHashMap，段内 Key
    // 顺序不是公开契约；这里对每个连续表段检查精确全集、唯一性、upsert 内容以及 delete 的
    // 诊断占位 `[0]`。原始 WAL 的 delete `value_len=0` 由生产 decoder 还原为 None，inspector
    // 再按其公开格式输出 `[0]`；原始字节布局另由 table_wal_codec_contract 逐偏移验证。
    let mut record_index = metas.len();
    for (table, _) in &metas {
        let expected: Vec<&ActiveEntry> = entries
            .iter()
            .filter(|entry| entry.table == *table)
            .collect();
        let end = record_index + expected.len();
        let mut unmatched = expected;
        for (offset, record) in records[record_index..end].iter().enumerate() {
            let index = record_index + offset;
            expect_eq(
                &format!("WAL business action {index} table"),
                &record.2.as_str(),
                table,
            )?;
            let position = unmatched
                .iter()
                .position(|entry| {
                    let expected_upsert = entry.value.is_some();
                    let expected_value = entry
                        .value
                        .as_ref()
                        .map(Binary::as_ref)
                        .unwrap_or(&[0]);
                    record.3 == expected_upsert
                        && record.4.as_slice() == entry.key.as_ref()
                        && record.5.as_slice() == expected_value
                })
                .ok_or_else(|| {
                    format!(
                        "WAL business action {index} has unexpected or duplicate content: upsert={}, key={:?}, value={:?}",
                        record.3,
                        record.4,
                        record.5,
                    )
                })?;
            unmatched.remove(position);
        }
        if !unmatched.is_empty() {
            return Err(format!(
                "WAL business table segment {table:?} omitted {} expected action(s)",
                unmatched.len(),
            ));
        }
        record_index = end;
    }
    Ok(())
}

fn assert_receipt(
    receipt: &[TableKeyVersion],
    entries: &[ActiveEntry],
    transaction_uid: &pi_guid::Guid,
) -> TestResult<()> {
    expect_eq("multi-table version receipt length", &receipt.len(), &entries.len())?;
    for entry in entries {
        let matches: Vec<&TableKeyVersion> = receipt
            .iter()
            .filter(|item| item.table.as_str() == entry.table && item.key == entry.key)
            .collect();
        expect_eq(
            &format!("receipt multiplicity for {}", entry.table),
            &matches.len(),
            &1usize,
        )?;
        let expected_version = if entry.value.is_some() {
            Version::Upsert(transaction_uid.clone())
        } else {
            Version::Delete(transaction_uid.clone())
        };
        expect_eq(
            &format!("receipt version for {}", entry.table),
            &matches[0].version,
            &expected_version,
        )?;
    }
    if receipt.iter().any(|item| item.table.as_str() == ".tables_meta") {
        return Err("Schema Meta write leaked into the public version receipt".to_owned());
    }
    Ok(())
}

async fn assert_live_values(
    fixture: &Fixture,
    entries: &[ActiveEntry],
    expected_uid: Option<&pi_guid::Guid>,
    label: &str,
) -> TestResult<()> {
    for entry in entries {
        let (value, version) = fixture
            .db
            .query_with_version(Atom::from(entry.table), entry.key.clone())
            .await
            .map_err(|error| format!("{label} query for {} failed: {error:?}", entry.table))?;
        expect_binary(
            &format!("{label} value for {}", entry.table),
            value.as_ref(),
            entry.value.as_ref(),
        )?;
        if let Some(uid) = expected_uid {
            let expected_version = if entry.value.is_some() {
                Version::Upsert(uid.clone())
            } else {
                Version::Delete(uid.clone())
            };
            expect_eq(
                &format!("{label} version for {}", entry.table),
                &version,
                &expected_version,
            )?;
        }
    }
    Ok(())
}

async fn observe_recovered_first_versions(
    fixture: &Fixture,
    entries: &[ActiveEntry],
    original_uid: &pi_guid::Guid,
    label: &str,
) -> TestResult<Vec<Version>> {
    let mut versions = Vec::with_capacity(entries.len());
    for entry in entries {
        let (value, version) = fixture
            .db
            .query_with_version(Atom::from(entry.table), entry.key.clone())
            .await
            .map_err(|error| format!("{label} query for {} failed: {error:?}", entry.table))?;
        expect_binary(
            &format!("{label} value for {}", entry.table),
            value.as_ref(),
            entry.value.as_ref(),
        )?;
        match (&entry.value, &version) {
            (Some(_), Version::Upsert(uid)) if uid != original_uid => {},
            (None, Version::Delete(uid)) if uid != original_uid => {},
            other => {
                return Err(format!(
                    "{label} for {} must be a fresh version matching logical existence and distinct from repair TID {:?}, observed {other:?}",
                    entry.table,
                    original_uid,
                ));
            },
        }
        versions.push(version);
    }
    Ok(versions)
}

async fn assert_live_values_and_versions(
    fixture: &Fixture,
    entries: &[ActiveEntry],
    expected_versions: &[Version],
    label: &str,
) -> TestResult<()> {
    expect_eq(
        &format!("{label} expected version count"),
        &expected_versions.len(),
        &entries.len(),
    )?;
    for (entry, expected_version) in entries.iter().zip(expected_versions) {
        let (value, version) = fixture
            .db
            .query_with_version(Atom::from(entry.table), entry.key.clone())
            .await
            .map_err(|error| format!("{label} query for {} failed: {error:?}", entry.table))?;
        expect_binary(
            &format!("{label} value for {}", entry.table),
            value.as_ref(),
            entry.value.as_ref(),
        )?;
        expect_eq(
            &format!("{label} stable version for {}", entry.table),
            &version,
            expected_version,
        )?;
    }
    Ok(())
}

async fn wait_for_unconfirmed_wal(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let waiting = fixture.logger.waiting_confirm_count().await;
        let appended = fixture.logger.append_total_count();
        let confirmed = fixture.logger.confirm_total_count();
        if waiting == 1 && appended == 1 && confirmed == 0 {
            return Ok(());
        }
        if confirmed > 0 {
            return Err(format!(
                "setup WAL confirmed before crash boundary: waiting={waiting}, appended={appended}, confirmed={confirmed}",
            ));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "setup did not expose one unconfirmed WAL before {timeout:?}: waiting={waiting}, appended={appended}, confirmed={confirmed}",
            ));
        }
        rt.timeout(10).await;
    }
}

async fn wait_for_confirmed_wal(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let waiting = fixture.logger.waiting_confirm_count().await;
        let appended = fixture.logger.append_total_count();
        let confirmed = fixture.logger.confirm_total_count();
        if waiting == 0 && appended >= 1 && appended == confirmed {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "schema recovery did not fully confirm before {timeout:?}: waiting={waiting}, appended={appended}, confirmed={confirmed}",
            ));
        }
        rt.timeout(25).await;
    }
}

fn read_transaction_uid(root: &Path) -> TestResult<pi_guid::Guid> {
    read_guid(&root.join(UID_FILE), "TID")
}

fn read_guid(path: &Path, label: &str) -> TestResult<pi_guid::Guid> {
    let bytes = fs::read(path)
        .map_err(|error| format!("reading schema recovery {label} evidence failed: {error}"))?;
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| format!("schema recovery {label} has {} bytes", bytes.len()))?;
    Ok(pi_guid::Guid(u128::from_le_bytes(bytes)))
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
        .map_err(|error| format!("locating schema recovery test executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase)
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| format!("spawning schema recovery phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("schema recovery phase {phase} exited with {status}"))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking schema recovery child status failed: {error}"))? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("schema recovery child exceeded {timeout:?}"));
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
        "pi_db_schema_protocol_recovery_{}_{}",
        std::process::id(),
        nanos,
    ))
}
