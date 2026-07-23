//! 普通单层多表事务从根 WAL 到数据文件冷启动的真实闭环专项。
//!
//! 本 target 使用真实 4-worker runtime、`Transaction2PcManager`、`CommitLogger`、Memory、
//! LogOrdered、Btree 和文件系统。`setup` 先用独立普通 DDL 根创建三张表，再用一个普通业务根
//! 按首次触表顺序安装三个直接叶子并写入三表；业务根提交后、60 秒 collector 运行前退出。
//! `recover` 从原未确认 WAL 执行生产 `try_repair`，等待全部表持久化和根确认；随后两个独立
//! data-only 进程移走原 WAL 并重复冷启动，以最终数据状态和 `.bak` 同时作为硬门禁。
//!
//! Memory 的 `persistence=true` 只表示动作进入根 WAL，没有独立数据文件。因此移走已确认 WAL
//! 后，Memory 表定义仍由 Meta 恢复，但其业务值必须不存在；LogOrdered/Btree 必须分别从表日志
//! 和 redb 数据文件恢复精确值。本 target 是串行慢速恢复专项，不属于 TSan 最小负载目标。

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
    db::KVDBTransaction,
    tables::TableKV,
    Binary, KVDBTableType,
};

use key_version_support::{
    BTREE_TABLE, Fixture, LOG_ORDERED_TABLE, MEMORY_TABLE, RealTransaction, TestResult,
    build_database, create_active_tables, encode_usize, expect_binary, expect_eq,
    query_ordinary, read_only_transaction, run_on_runtime, table_meta, writable_transaction,
};

const TEST_NAME: &str = "test_ordinary_multi_table_transaction_recovery";
const PHASE_ENV: &str = "PI_DB_ORDINARY_MULTI_TABLE_RECOVERY_PHASE";
const ROOT_ENV: &str = "PI_DB_ORDINARY_MULTI_TABLE_RECOVERY_ROOT";
const ARCHIVED_WAL_DIR: &str = "confirmed-root-wal";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(120);
const SETUP_TIMEOUT: Duration = Duration::from_secs(30);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(100);
const DATA_ONLY_TIMEOUT: Duration = Duration::from_secs(30);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(80);

#[test]
fn test_ordinary_multi_table_transaction_recovery() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV)
                .expect("ordinary multi-table recovery child phase must receive its root path"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("ordinary multi-table phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root();
    fs::create_dir_all(&root).expect("creating ordinary multi-table recovery root must succeed");
    for phase in ["setup", "recover", "inspect-data-only", "inspect-data-only-again"] {
        if let Err(error) = run_phase_process(&root, phase, PROCESS_TIMEOUT) {
            panic!(
                "ordinary multi-table recovery failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root,
            );
        }
    }
    fs::remove_dir_all(&root)
        .expect("cleaning ordinary multi-table recovery root must succeed");
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
        other => Err(format!("unknown ordinary multi-table recovery phase: {other}")),
    }
}

async fn phase_setup(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    create_active_tables(&fixture).await?;
    assert_table_definitions(&fixture, "setup").await?;
    expect_eq("setup DDL root WAL append count", &fixture.logger.append_total_count(), &1usize)?;
    expect_eq("setup DDL root WAL confirm count", &fixture.logger.confirm_total_count(), &0usize)?;
    expect_eq(
        "setup DDL root WAL waiting count",
        &fixture.logger.waiting_confirm_count().await,
        &1usize,
    )?;

    let entries = active_entries();
    let transaction = writable_transaction(&fixture.db, "ordinary multi-table business root")?;
    expect_eq("business root initial status",
              &transaction.get_status(),
              &Transaction2PcStatus::Start)?;
    expect_eq("business root initial transaction UID",
              &transaction.get_transaction_uid(),
              &None)?;
    expect_eq("business root is a tree", &transaction.is_tree(), &true)?;
    expect_eq("business root is not a unit", &transaction.is_unit(), &false)?;

    transaction
        .upsert(entries.iter().map(|entry| {
            TableKV::new(
                Atom::from(entry.table),
                entry.key.clone(),
                Some(entry.value.clone()),
            )
        }).collect())
        .await
        .map_err(|error| format!("ordinary multi-table upsert failed: {error:?}"))?;
    expect_eq("business root persistence aggregation",
              &transaction.is_require_persistence(),
              &true)?;
    expect_eq("business root direct child count", &transaction.children_len(), &3usize)?;
    let children: Vec<RealTransaction> = transaction.to_children().collect();
    assert_direct_leaf_order(&children)?;

    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing ordinary multi-table root failed: {error:?}"))?;
    if prepare.len() <= 16 {
        return Err(format!(
            "ordinary three-table prepare output must contain child WAL actions, observed {} bytes",
            prepare.len(),
        ));
    }
    let transaction_uid = transaction
        .get_transaction_uid()
        .ok_or_else(|| "ordinary multi-table prepare did not allocate a transaction UID".to_owned())?;
    let commit_uid = transaction
        .get_commit_uid()
        .ok_or_else(|| "ordinary multi-table prepare did not allocate a commit UID".to_owned())?;
    expect_eq("business root prepared status",
              &transaction.get_status(),
              &Transaction2PcStatus::Prepared)?;
    assert_shared_child_identity(&children, &transaction_uid, &commit_uid)?;

    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing ordinary multi-table root failed: {error:?}"))?;
    expect_eq("business root committed status",
              &transaction.get_status(),
              &Transaction2PcStatus::Commited)?;
    for (index, child) in children.iter().enumerate() {
        expect_eq(
            &format!("business child {index} committed status"),
            &child.get_status(),
            &Transaction2PcStatus::Commited,
        )?;
    }
    expect_eq(
        "business manager produced increment",
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        "business manager consumed increment",
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 1),
    )?;
    expect_eq("business manager active roots", &fixture.tr_manager.transaction_len(), &0usize)?;
    expect_eq(
        "business root WAL append increment",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;
    assert_live_values(&fixture, &entries, "setup committed").await?;
    wait_for_wal_state(&rt, &fixture, 2, 0, 2, Duration::from_secs(3), "setup").await?;
    expect_eq(
        "setup nonempty .bak count",
        &nonempty_bak_count(&root.join("root-wal"))?,
        &0usize,
    )?;

    drop(children);
    drop(transaction);
    Ok(())
}

async fn phase_recover(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let wal_path = root.join("root-wal");
    let bak_before = nonempty_bak_count(&wal_path)?;
    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    assert_table_definitions(&fixture, "recovered").await?;
    let entries = active_entries();
    assert_live_values(&fixture, &entries, "replayed before confirm").await?;

    wait_for_wal_state(&rt, &fixture, 2, 2, 0, CONFIRM_TIMEOUT, "recovery").await?;
    // logger 收口不能替代业务数据结论；确认后再次从当前表状态逐项复核。
    assert_live_values(&fixture, &entries, "replayed after confirm").await?;
    expect_eq("recovery manager produced/consumed balance",
              &fixture.tr_manager.produced_transaction_total(),
              &fixture.tr_manager.consumed_transaction_total())?;
    expect_eq("recovery manager active roots", &fixture.tr_manager.transaction_len(), &0usize)?;
    expect_eq(
        "recovery Btree overlay drained",
        &fixture.db.table_cache_size(&Atom::from(BTREE_TABLE)).await,
        &Some(0u64),
    )?;

    let bak_after = nonempty_bak_count(&wal_path)?;
    if bak_after <= bak_before {
        return Err(format!(
            "ordinary recovery did not add a confirmed .bak checkpoint: before={bak_before}, after={bak_after}",
        ));
    }
    let active = active_file_sizes(&wal_path)?;
    if active.iter().any(|(_, len)| *len > 0) {
        return Err(format!("nonempty active WAL remains after ordinary recovery: {active:?}"));
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
            .map_err(|error| format!("archiving confirmed root WAL failed: {error}"))?;
    } else {
        if !archived_wal.exists() {
            return Err("second data-only start cannot find archived confirmed WAL".to_owned());
        }
        let active = active_file_sizes(&wal_path)?;
        if active.iter().any(|(_, len)| *len > 0) {
            return Err(format!("first data-only start produced a nonempty root WAL: {active:?}"));
        }
    }
    if nonempty_bak_count(&archived_wal)? == 0 {
        return Err("archived ordinary recovery WAL contains no nonempty .bak checkpoint".to_owned());
    }

    let fixture = build_database(&rt,
                                 &root,
                                 Duration::ZERO,
                                 Duration::ZERO).await?;
    let label = if archive_wal { "data-only" } else { "second data-only" };
    assert_table_definitions(&fixture, label).await?;
    let entries = active_entries();
    for entry in &entries {
        let value = query_ordinary(
            &fixture.db,
            entry.table,
            entry.key.clone(),
            &format!("{label} query for {}", entry.table),
        ).await?;
        if entry.table == MEMORY_TABLE {
            expect_binary(&format!("{label} Memory value"), value.as_ref(), None)?;
        } else {
            expect_binary(
                &format!("{label} persisted value for {}", entry.table),
                value.as_ref(),
                Some(&entry.value),
            )?;
        }
    }
    expect_eq(&format!("{label} WAL append count"),
              &fixture.logger.append_total_count(),
              &0usize)?;
    expect_eq(&format!("{label} WAL confirm count"),
              &fixture.logger.confirm_total_count(),
              &0usize)?;
    expect_eq(
        &format!("{label} WAL waiting count"),
        &fixture.logger.waiting_confirm_count().await,
        &0usize,
    )?;
    expect_eq(&format!("{label} manager produced/consumed balance"),
              &fixture.tr_manager.produced_transaction_total(),
              &fixture.tr_manager.consumed_transaction_total())?;
    expect_eq(&format!("{label} manager active roots"),
              &fixture.tr_manager.transaction_len(),
              &0usize)?;
    expect_eq(
        &format!("{label} Btree overlay size"),
        &fixture.db.table_cache_size(&Atom::from(BTREE_TABLE)).await,
        &Some(0u64),
    )
}

struct ActiveEntry {
    table: &'static str,
    key: Binary,
    value: Binary,
}

fn active_entries() -> Vec<ActiveEntry> {
    vec![
        ActiveEntry {
            table: MEMORY_TABLE,
            key: encode_usize(201),
            value: encode_usize(1_201),
        },
        ActiveEntry {
            table: LOG_ORDERED_TABLE,
            key: encode_usize(202),
            value: encode_usize(1_202),
        },
        ActiveEntry {
            table: BTREE_TABLE,
            key: encode_usize(203),
            value: encode_usize(1_203),
        },
    ]
}

fn assert_direct_leaf_order(children: &[RealTransaction]) -> TestResult<()> {
    expect_eq("direct leaf vector length", &children.len(), &3usize)?;
    let observed = children.iter().map(|child| {
        match child {
            KVDBTransaction::MemOrdTabTr(_) => "Memory",
            KVDBTransaction::LogOrdTabTr(_) => "LogOrdered",
            KVDBTransaction::BtreeOrdTabTr(_) => "Btree",
            KVDBTransaction::MetaTabTr(_) => "Meta",
            KVDBTransaction::LogWTabTr(_) => "LogWrite",
            KVDBTransaction::RootTr(_) => "Root",
        }
    }).collect::<Vec<_>>();
    expect_eq(
        "direct leaf first-touch order",
        &observed,
        &vec!["Memory", "LogOrdered", "Btree"],
    )?;
    for (index, child) in children.iter().enumerate() {
        expect_eq(&format!("direct child {index} is unit"), &child.is_unit(), &true)?;
        expect_eq(&format!("direct child {index} is not tree"), &child.is_tree(), &false)?;
    }
    Ok(())
}

fn assert_shared_child_identity(
    children: &[RealTransaction],
    transaction_uid: &pi_guid::Guid,
    commit_uid: &pi_guid::Guid,
) -> TestResult<()> {
    for (index, child) in children.iter().enumerate() {
        expect_eq(
            &format!("direct child {index} transaction UID"),
            &child.get_transaction_uid(),
            &Some(transaction_uid.clone()),
        )?;
        expect_eq(
            &format!("direct child {index} commit UID"),
            &child.get_commit_uid(),
            &Some(commit_uid.clone()),
        )?;
        expect_eq(
            &format!("direct child {index} prepared status"),
            &child.get_status(),
            &Transaction2PcStatus::Prepared,
        )?;
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
        expect_eq(
            &format!("{label} table definition for {name}"),
            &verifier.table_meta(Atom::from(name)).await,
            &Some(table_meta(table_type, true)),
        )?;
    }
    Ok(())
}

async fn assert_live_values(
    fixture: &Fixture,
    entries: &[ActiveEntry],
    label: &str,
) -> TestResult<()> {
    for entry in entries {
        let value = query_ordinary(
            &fixture.db,
            entry.table,
            entry.key.clone(),
            &format!("{label} query for {}", entry.table),
        ).await?;
        expect_binary(
            &format!("{label} value for {}", entry.table),
            value.as_ref(),
            Some(&entry.value),
        )?;
    }
    Ok(())
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
                "{label} WAL advanced beyond the expected state: appended={appended}, confirmed={confirmed}, waiting={waiting}",
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
        .map_err(|error| format!("locating ordinary recovery test executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase)
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| format!("spawning ordinary recovery phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("ordinary recovery phase {phase} exited with {status}"))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking ordinary recovery child status failed: {error}"))? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("ordinary recovery child exceeded {timeout:?}"));
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
        "pi_db_ordinary_multi_table_recovery_{}_{}",
        std::process::id(),
        nanos,
    ))
}
