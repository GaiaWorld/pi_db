//! `KVDBManager` 非空表整理与 data-only 冷启动的真实专项。
//!
//! 本 target 使用真实 4-worker runtime、事务管理器、CommitLogger、根 WAL、Meta、Memory、
//! LogOrdered、Btree 和文件系统。setup 通过三个独立普通根完成 DDL、3x24 Key 写入和每表 8 Key
//! 删除，等待全部根确认和 Btree overlay 清空后，按 Meta、Memory、LogOrdered、Btree 顺序执行
//! `ready_collect_table -> collect_table`。每个阶段都精确复核逻辑值、统计、manager、根 WAL
//! 计数和文件快照；随后两个独立 data-only 进程移走根 WAL 并验证表数据恢复。
//!
//! 当前 manager 会让 registry 读 guard 跨越 maintenance future，本 target 只验证无并发 DDL
//! 的成功路径。Btree compact 的有界重试和真实失败恢复由同文件局部专项独立验证；本 target
//! 负责证明修复后完整 manager/WAL/data-only 成功闭环不退化。完整范围和非目标见
//! `docs/MANAGER_TABLE_MAINTENANCE_ACCEPTANCE.md`。

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
    AsyncCommitLog, Transaction2Pc, TransactionTree, UnitTransaction,
};
use pi_atom::Atom;
use pi_db::{
    tables::TableKV,
    Binary, KVDBTableType,
};

use key_version_support::{
    BTREE_TABLE, Fixture, LOG_ORDERED_TABLE, MEMORY_TABLE, META_TABLE, RealTransaction,
    TestResult, build_database, create_active_tables, encode_usize, expect_binary, expect_eq,
    read_only_transaction, run_on_runtime, table_meta, writable_transaction,
};

const TEST_NAME: &str = "test_manager_nonempty_table_maintenance_and_cold_restart";
const PHASE_ENV: &str = "PI_DB_MANAGER_TABLE_MAINTENANCE_PHASE";
const ROOT_ENV: &str = "PI_DB_MANAGER_TABLE_MAINTENANCE_ROOT";
const ARCHIVED_WAL_DIR: &str = "confirmed-root-wal";
const TABLES: [&str; 3] = [MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE];
const KEY_COUNT: usize = 24;
const LIVE_KEY_COUNT: usize = 16;
const EXPECTED_ROOT_WAL_COUNT: usize = 3;
const PROCESS_TIMEOUT: Duration = Duration::from_secs(120);
const SETUP_TIMEOUT: Duration = Duration::from_secs(100);
const DATA_ONLY_TIMEOUT: Duration = Duration::from_secs(30);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(85);

#[test]
fn test_manager_nonempty_table_maintenance_and_cold_restart() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV)
                .expect("manager maintenance child phase must receive its root path"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("manager maintenance phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root();
    fs::create_dir_all(&root).expect("creating manager maintenance root must succeed");
    for phase in ["setup-and-maintain", "inspect-data-only", "inspect-data-only-again"] {
        if let Err(error) = run_phase_process(&root, phase, PROCESS_TIMEOUT) {
            panic!(
                "manager maintenance failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root,
            );
        }
    }
    fs::remove_dir_all(&root).expect("cleaning manager maintenance root must succeed");
}

fn run_child_phase(phase: &str, root: &Path) -> TestResult<()> {
    match phase {
        "setup-and-maintain" => {
            let root = root.to_path_buf();
            run_on_runtime(SETUP_TIMEOUT, move |rt| async move {
                phase_setup_and_maintain(rt, root).await
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
        other => Err(format!("unknown manager maintenance phase: {other}")),
    }
}

async fn phase_setup_and_maintain(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
) -> TestResult<()> {
    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    create_active_tables(&fixture).await?;
    expect_eq("DDL manager produced", &fixture.tr_manager.produced_transaction_total(), &1usize)?;
    expect_eq("DDL manager consumed", &fixture.tr_manager.consumed_transaction_total(), &1usize)?;
    expect_eq("DDL manager active", &fixture.tr_manager.transaction_len(), &0usize)?;
    expect_eq("DDL root WAL append", &fixture.logger.append_total_count(), &1usize)?;

    let seed = writable_transaction(&fixture.db, "manager maintenance seed")?;
    seed
        .upsert(seed_actions())
        .await
        .map_err(|error| format!("manager maintenance seed upsert failed: {error:?}"))?;
    expect_eq("seed direct child count", &seed.children_len(), &3usize)?;
    expect_eq("seed persistence aggregation", &seed.is_require_persistence(), &true)?;
    commit_checked(&fixture, &seed, "seed").await?;

    let delete = writable_transaction(&fixture.db, "manager maintenance delete")?;
    let deleted = delete
        .delete(delete_actions())
        .await
        .map_err(|error| format!("manager maintenance delete failed: {error:?}"))?;
    assert_delete_results(&deleted)?;
    expect_eq("delete direct child count", &delete.children_len(), &3usize)?;
    expect_eq("delete persistence aggregation", &delete.is_require_persistence(), &true)?;
    commit_checked(&fixture, &delete, "delete").await?;

    expect_eq("three roots produced", &fixture.tr_manager.produced_transaction_total(), &3usize)?;
    expect_eq("three roots consumed", &fixture.tr_manager.consumed_transaction_total(), &3usize)?;
    expect_eq("three roots active", &fixture.tr_manager.transaction_len(), &0usize)?;
    expect_eq(
        "three roots appended",
        &fixture.logger.append_total_count(),
        &EXPECTED_ROOT_WAL_COUNT,
    )?;

    wait_for_confirmed_wal(&rt, &fixture, &root, CONFIRM_TIMEOUT).await?;
    assert_table_definitions(&fixture, "before maintenance").await?;
    assert_reference_model(&fixture, true, "before maintenance").await?;
    let baseline = capture_maintenance_baseline(&fixture, &root).await?;
    assert_expected_live_stats(&baseline.table_stats, "before maintenance")?;

    for table in [META_TABLE, MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE] {
        maintain_table(&fixture, &root, &baseline, table).await?;
    }

    assert_maintenance_invariants(&fixture,
                                  &root,
                                  &baseline,
                                  "after all maintenance").await?;
    Ok(())
}

async fn commit_checked(
    fixture: &Fixture,
    transaction: &RealTransaction,
    label: &str,
) -> TestResult<()> {
    expect_eq(&format!("{label} initial status"),
              &transaction.get_status(),
              &Transaction2PcStatus::Start)?;
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let token = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing manager maintenance {label} failed: {error:?}"))?;
    if token.len() <= 16 {
        return Err(format!(
            "manager maintenance {label} prepare token must contain table actions, observed {} bytes",
            token.len(),
        ));
    }
    expect_eq(&format!("{label} prepared status"),
              &transaction.get_status(),
              &Transaction2PcStatus::Prepared)?;
    if transaction.get_transaction_uid().is_none() || transaction.get_commit_uid().is_none() {
        return Err(format!("manager maintenance {label} prepare did not allocate TID/CID"));
    }

    transaction
        .commit_modified(token)
        .await
        .map_err(|error| format!("committing manager maintenance {label} failed: {error:?}"))?;
    expect_eq(&format!("{label} committed status"),
              &transaction.get_status(),
              &Transaction2PcStatus::Commited)?;
    expect_eq(&format!("{label} produced increment"),
              &fixture.tr_manager.produced_transaction_total(),
              &(produced_before + 1))?;
    expect_eq(&format!("{label} consumed increment"),
              &fixture.tr_manager.consumed_transaction_total(),
              &(consumed_before + 1))?;
    expect_eq(&format!("{label} active roots"),
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq(&format!("{label} root WAL append increment"),
              &fixture.logger.append_total_count(),
              &(append_before + 1))
}

fn seed_actions() -> Vec<TableKV> {
    let mut actions = Vec::with_capacity(TABLES.len() * KEY_COUNT);
    for (table_index, table) in TABLES.iter().enumerate() {
        for key_index in 0..KEY_COUNT {
            actions.push(TableKV::new(
                Atom::from(*table),
                model_key(key_index),
                Some(model_value(table_index, key_index)),
            ));
        }
    }
    actions
}

fn delete_actions() -> Vec<TableKV> {
    let mut actions = Vec::with_capacity(TABLES.len() * (KEY_COUNT - LIVE_KEY_COUNT));
    for table in TABLES {
        for key_index in LIVE_KEY_COUNT..KEY_COUNT {
            actions.push(TableKV::new(Atom::from(table), model_key(key_index), None));
        }
    }
    actions
}

fn assert_delete_results(results: &[Option<Binary>]) -> TestResult<()> {
    let expected_len = TABLES.len() * (KEY_COUNT - LIVE_KEY_COUNT);
    expect_eq("delete result length", &results.len(), &expected_len)?;
    let per_table = KEY_COUNT - LIVE_KEY_COUNT;
    for (index, actual) in results.iter().enumerate() {
        let table_index = index / per_table;
        let key_index = LIVE_KEY_COUNT + index % per_table;
        let expected = if TABLES[table_index] == BTREE_TABLE {
            Some(model_value(table_index, key_index))
        } else {
            None
        };
        expect_binary(
            &format!("delete result table {table_index} key {key_index}"),
            actual.as_ref(),
            expected.as_ref(),
        )?;
    }
    Ok(())
}

fn model_key(index: usize) -> Binary {
    encode_usize(10_000 + index)
}

fn model_value(table_index: usize, key_index: usize) -> Binary {
    encode_usize((table_index + 1) * 100_000 + key_index)
}

async fn wait_for_confirmed_wal(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
    root: &Path,
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let appended = fixture.logger.append_total_count();
        let confirmed = fixture.logger.confirm_total_count();
        let waiting = fixture.logger.waiting_confirm_count().await;
        let bak_count = nonempty_bak_count(&root.join("root-wal"))?;
        let active = active_file_sizes(&root.join("root-wal"))?;
        let active_nonempty = active.iter().any(|(_, len)| *len > 0);
        let btree_cache = fixture.db.table_cache_size(&Atom::from(BTREE_TABLE)).await;
        if appended == EXPECTED_ROOT_WAL_COUNT
            && confirmed == EXPECTED_ROOT_WAL_COUNT
            && waiting == 0
            && bak_count > 0
            && !active_nonempty
            && btree_cache == Some(0) {
            rt.timeout(50).await;
            expect_eq("stable WAL append count",
                      &fixture.logger.append_total_count(),
                      &EXPECTED_ROOT_WAL_COUNT)?;
            expect_eq("stable WAL confirm count",
                      &fixture.logger.confirm_total_count(),
                      &EXPECTED_ROOT_WAL_COUNT)?;
            expect_eq("stable WAL waiting count",
                      &fixture.logger.waiting_confirm_count().await,
                      &0usize)?;
            expect_eq("stable Btree overlay",
                      &fixture.db.table_cache_size(&Atom::from(BTREE_TABLE)).await,
                      &Some(0u64))?;
            if active_file_sizes(&root.join("root-wal"))?
                .iter()
                .any(|(_, len)| *len > 0) {
                return Err("root WAL became nonempty after reaching confirmed state".to_owned());
            }
            return Ok(());
        }
        if appended > EXPECTED_ROOT_WAL_COUNT || confirmed > EXPECTED_ROOT_WAL_COUNT {
            return Err(format!(
                "root WAL advanced beyond expected maintenance baseline: appended={appended}, confirmed={confirmed}, waiting={waiting}",
            ));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "root WAL did not fully confirm before {timeout:?}: appended={appended}, confirmed={confirmed}, waiting={waiting}, bak={bak_count}, active={active:?}, btree_cache={btree_cache:?}",
            ));
        }
        rt.timeout(25).await;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TableStats {
    table: &'static str,
    records: usize,
    cache_bytes: u64,
}

#[derive(Clone, Debug)]
struct MaintenanceBaseline {
    table_stats: Vec<TableStats>,
    wal_files: Vec<(PathBuf, u64)>,
    produced: usize,
    consumed: usize,
}

async fn capture_maintenance_baseline(
    fixture: &Fixture,
    root: &Path,
) -> TestResult<MaintenanceBaseline> {
    Ok(MaintenanceBaseline {
        table_stats: capture_table_stats(fixture).await?,
        wal_files: regular_file_sizes(&root.join("root-wal"))?,
        produced: fixture.tr_manager.produced_transaction_total(),
        consumed: fixture.tr_manager.consumed_transaction_total(),
    })
}

async fn capture_table_stats(fixture: &Fixture) -> TestResult<Vec<TableStats>> {
    let mut stats = Vec::with_capacity(4);
    for table in [META_TABLE, MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE] {
        let atom = Atom::from(table);
        let records = fixture
            .db
            .table_record_size(&atom)
            .await
            .ok_or_else(|| format!("table {table} disappeared while reading record count"))?;
        let cache_bytes = fixture
            .db
            .table_cache_size(&atom)
            .await
            .ok_or_else(|| format!("table {table} disappeared while reading cache bytes"))?;
        stats.push(TableStats {
            table,
            records,
            cache_bytes,
        });
    }
    Ok(stats)
}

fn assert_expected_live_stats(stats: &[TableStats], label: &str) -> TestResult<()> {
    expect_eq(&format!("{label} table stat count"), &stats.len(), &4usize)?;
    let expected_records = [3usize, LIVE_KEY_COUNT, LIVE_KEY_COUNT, LIVE_KEY_COUNT];
    for (index, stat) in stats.iter().enumerate() {
        expect_eq(&format!("{label} {} records", stat.table),
                  &stat.records,
                  &expected_records[index])?;
        if stat.table == BTREE_TABLE {
            expect_eq(&format!("{label} Btree cache"), &stat.cache_bytes, &0u64)?;
        } else if stat.cache_bytes == 0 {
            return Err(format!("{label} {} cache unexpectedly stayed empty", stat.table));
        }
    }
    Ok(())
}

async fn maintain_table(
    fixture: &Fixture,
    root: &Path,
    baseline: &MaintenanceBaseline,
    table: &'static str,
) -> TestResult<()> {
    let atom = Atom::from(table);
    let table_path = fixture.db.table_path(&atom).await;
    let files_before_ready = match table_path.as_ref() {
        Some(path) => Some(regular_file_sizes(path)?),
        None => None,
    };

    fixture
        .db
        .ready_collect_table(&atom)
        .await
        .map_err(|error| format!("ready_collect_table failed for {table}: {error}"))?;

    let files_after_ready = match table_path.as_ref() {
        Some(path) => Some(regular_file_sizes(path)?),
        None => None,
    };
    match table {
        META_TABLE | LOG_ORDERED_TABLE => {
            if files_before_ready == files_after_ready {
                return Err(format!(
                    "ready_collect_table for {table} did not create a new physical log file",
                ));
            }
        },
        MEMORY_TABLE => {
            expect_eq("Memory maintenance path", &table_path, &None)?;
            expect_eq("Memory ready file snapshot", &files_after_ready, &None)?;
        },
        BTREE_TABLE => {
            expect_eq("Btree ready no-op file snapshot",
                      &files_after_ready,
                      &files_before_ready)?;
        },
        _ => return Err(format!("unexpected maintenance table {table}")),
    }
    assert_maintenance_invariants(
        fixture,
        root,
        baseline,
        &format!("after {table} ready_collect"),
    ).await?;

    fixture
        .db
        .collect_table(&atom)
        .await
        .map_err(|error| format!("collect_table failed for {table}: {error}"))?;
    assert_maintenance_invariants(
        fixture,
        root,
        baseline,
        &format!("after {table} collect"),
    ).await
}

async fn assert_maintenance_invariants(
    fixture: &Fixture,
    root: &Path,
    baseline: &MaintenanceBaseline,
    label: &str,
) -> TestResult<()> {
    assert_table_definitions(fixture, label).await?;
    assert_reference_model(fixture, true, label).await?;
    expect_eq(&format!("{label} table stats"),
              &capture_table_stats(fixture).await?,
              &baseline.table_stats)?;
    expect_eq(&format!("{label} manager produced"),
              &fixture.tr_manager.produced_transaction_total(),
              &baseline.produced)?;
    expect_eq(&format!("{label} manager consumed"),
              &fixture.tr_manager.consumed_transaction_total(),
              &baseline.consumed)?;
    expect_eq(&format!("{label} manager active"),
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq(&format!("{label} WAL appended"),
              &fixture.logger.append_total_count(),
              &EXPECTED_ROOT_WAL_COUNT)?;
    expect_eq(&format!("{label} WAL confirmed"),
              &fixture.logger.confirm_total_count(),
              &EXPECTED_ROOT_WAL_COUNT)?;
    expect_eq(&format!("{label} WAL waiting"),
              &fixture.logger.waiting_confirm_count().await,
              &0usize)?;
    expect_eq(&format!("{label} root WAL file snapshot"),
              &regular_file_sizes(&root.join("root-wal"))?,
              &baseline.wal_files)
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
        let atom = Atom::from(name);
        expect_eq(&format!("{label} table meta {name}"),
                  &verifier.table_meta(atom.clone()).await,
                  &Some(table_meta(table_type, true)))?;
        expect_eq(&format!("{label} persistence {name}"),
                  &fixture.db.is_persistent_table(&atom).await,
                  &Some(true))?;
        expect_eq(&format!("{label} ordering {name}"),
                  &fixture.db.is_ordered_table(&atom).await,
                  &Some(true))?;
    }
    Ok(())
}

async fn assert_reference_model(
    fixture: &Fixture,
    include_memory: bool,
    label: &str,
) -> TestResult<()> {
    let transaction = read_only_transaction(&fixture.db, &format!("{label} reference model"))?;
    let mut actions = Vec::with_capacity(TABLES.len() * KEY_COUNT);
    let mut expected = Vec::with_capacity(TABLES.len() * KEY_COUNT);
    for (table_index, table) in TABLES.iter().enumerate() {
        for key_index in 0..KEY_COUNT {
            actions.push(TableKV::new(Atom::from(*table), model_key(key_index), None));
            if (!include_memory && *table == MEMORY_TABLE) || key_index >= LIVE_KEY_COUNT {
                expected.push(None);
            } else {
                expected.push(Some(model_value(table_index, key_index)));
            }
        }
    }
    let observed = transaction.query(actions).await;
    expect_eq(&format!("{label} result length"), &observed.len(), &expected.len())?;
    for (index, (actual, expected)) in observed.iter().zip(expected.iter()).enumerate() {
        expect_binary(&format!("{label} result slot {index}"),
                      actual.as_ref(),
                      expected.as_ref())?;
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
            return Err(format!("archived maintenance WAL unexpectedly exists: {archived_wal:?}"));
        }
        fs::rename(&wal_path, &archived_wal)
            .map_err(|error| format!("archiving confirmed maintenance WAL failed: {error}"))?;
    } else {
        if !archived_wal.exists() {
            return Err("second data-only phase cannot find archived maintenance WAL".to_owned());
        }
        if active_file_sizes(&wal_path)?
            .iter()
            .any(|(_, len)| *len > 0) {
            return Err("first data-only phase produced a nonempty root WAL".to_owned());
        }
    }
    if nonempty_bak_count(&archived_wal)? == 0 {
        return Err("archived maintenance WAL contains no nonempty .bak checkpoint".to_owned());
    }

    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    let label = if archive_wal { "data-only" } else { "second data-only" };
    assert_table_definitions(&fixture, label).await?;
    assert_reference_model(&fixture, false, label).await?;
    let stats = capture_table_stats(&fixture).await?;
    assert_expected_data_only_stats(&stats, label)?;
    expect_eq(&format!("{label} manager produced"),
              &fixture.tr_manager.produced_transaction_total(),
              &0usize)?;
    expect_eq(&format!("{label} manager consumed"),
              &fixture.tr_manager.consumed_transaction_total(),
              &0usize)?;
    expect_eq(&format!("{label} manager active"),
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq(&format!("{label} WAL appended"),
              &fixture.logger.append_total_count(),
              &0usize)?;
    expect_eq(&format!("{label} WAL confirmed"),
              &fixture.logger.confirm_total_count(),
              &0usize)?;
    expect_eq(&format!("{label} WAL waiting"),
              &fixture.logger.waiting_confirm_count().await,
              &0usize)
}

fn assert_expected_data_only_stats(stats: &[TableStats], label: &str) -> TestResult<()> {
    expect_eq(&format!("{label} table stat count"), &stats.len(), &4usize)?;
    let expected_records = [3usize, 0usize, LIVE_KEY_COUNT, LIVE_KEY_COUNT];
    for (index, stat) in stats.iter().enumerate() {
        expect_eq(&format!("{label} {} records", stat.table),
                  &stat.records,
                  &expected_records[index])?;
        match stat.table {
            META_TABLE | LOG_ORDERED_TABLE => {
                if stat.cache_bytes == 0 {
                    return Err(format!("{label} {} cache unexpectedly stayed empty", stat.table));
                }
            },
            MEMORY_TABLE | BTREE_TABLE => {
                expect_eq(&format!("{label} {} cache", stat.table),
                          &stat.cache_bytes,
                          &0u64)?;
            },
            _ => return Err(format!("unexpected data-only table {}", stat.table)),
        }
    }
    Ok(())
}

fn regular_file_sizes(path: &Path) -> TestResult<Vec<(PathBuf, u64)>> {
    let path_metadata = fs::metadata(path)
        .map_err(|error| format!("reading metadata for {path:?} failed: {error}"))?;
    if path_metadata.is_file() {
        return Ok(vec![(path.to_path_buf(), path_metadata.len())]);
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(path)
        .map_err(|error| format!("reading directory {path:?} failed: {error}"))? {
        let entry = entry
            .map_err(|error| format!("reading entry in {path:?} failed: {error}"))?;
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
        .map_err(|error| format!("locating manager maintenance executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase)
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| format!("spawning manager maintenance phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("manager maintenance phase {phase} exited with {status}"))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking manager maintenance child failed: {error}"))? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("manager maintenance child exceeded {timeout:?}"));
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
        "pi_db_manager_table_maintenance_{}_{}",
        std::process::id(),
        nanos,
    ))
}
