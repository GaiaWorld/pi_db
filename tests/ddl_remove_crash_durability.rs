//! 独立删表事务在提交成功后立即崩溃时的真实耐久性专项。
//!
//! 本 target 不引用或运行旧测试。它通过公开
//! `KVDBManagerBuilder -> KVDBManager -> KVDBTransaction` 链路，使用真实多线程/单线程
//! runtime、`Transaction2PcManager`、`CommitLogger`、Meta 表日志和文件系统，验证删表是否
//! 遵守“预提交输出先写入根 WAL，提交成功后才异步持久化数据文件”的数据库事务顺序。
//!
//! 测试分为四个隔离进程：
//!
//! - `setup` 创建 Memory 表，并等待创建事务的根 WAL 与 Meta 数据日志均最终确认；
//! - `remove-crash` 在单 worker runtime 中用全新根事务删除全部测试表，验证预提交输出与
//!   根 WAL 计数，
//!   `commit_modified` 返回成功后立即 `_exit`，不允许 Meta 后台任务获得下一次 poll；
//! - `recover-with-original-wal` 使用原 WAL 正常启动并执行 `try_repair`，检查恢复后的表定义，
//!   并等待删除 tombstone 最终写入 Meta 数据文件、根 WAL 完成确认；
//! - `inspect-data-only` 使用独立空 WAL 再次启动，只从 Meta 数据文件验证所有表保持删除。
//!
//! setup 使用约 4096 个长度合法的 Memory 表名，使 Meta 表待写总量超过固定 16 MiB 阈值，
//! 无需依赖 60 秒定时整理；每个名称均满足 `MAX_TABLE_NAME_BYTES`，Memory 引擎也不会创建
//! 用户表数据文件。测试不声称 DDL 当前具有完整事务 rollback 原子性，它只验证已经返回
//! “提交成功”的删除在随后的进程崩溃中拥有根 WAL 保护，并验证恢复后最终数据文件状态。
//! 第一次恢复必须使用原 WAL；只有确认恢复写入完成后，才能用空 WAL 检查数据文件。
//! 被测入口：`KVDBTransaction::{remove_table,prepare_modified,commit_modified}`、
//! `RootTransaction::is_require_persistence`、Meta 子事务 prepare/commit 和根提交日志。
//! 归档入口：`docs/SEMANTIC_CONTRACTS.md#contract-ddl-remove-crash-durability`。

#![cfg(target_os = "linux")]

use std::{
    env, fs,
    future::Future,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::{manager_2pc::Transaction2PcManager, AsyncCommitLog};
use pi_atom::Atom;
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder},
    KVDBTableType, KVTableMeta, MAX_TABLE_NAME_BYTES,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;

const TEST_NAME: &str = "test_remove_table_commit_survives_immediate_process_crash";
const PHASE_ENV: &str = "PI_DB_DDL_REMOVE_CRASH_PHASE";
const ROOT_ENV: &str = "PI_DB_DDL_REMOVE_CRASH_ROOT";
const TABLE_NAME: &str = "ddl_remove_crash_target";
const FILLER_NAME_PREFIX: &str = "ddl_remove_crash_meta_filler_";
const FILLER_TABLE_COUNT: usize = 4_096;
const META_WAIT_THRESHOLD: usize = 16 * 1024 * 1024;
const PROCESS_TIMEOUT: Duration = Duration::from_secs(90);
const SETUP_CONFIRM_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LoggerState {
    waiting_confirm: usize,
    append_total: usize,
    confirm_total: usize,
}

/// 验收契约：删表提交成功后立即崩溃，原 WAL 必须恢复删除并最终写入 Meta 数据文件。
#[test]
fn test_remove_table_commit_survives_immediate_process_crash() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV).expect("DDL remove child phase must receive its root path"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("DDL remove crash phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root();
    fs::create_dir_all(&root).expect("creating DDL remove crash root must succeed");

    for phase in [
        "setup",
        "remove-crash",
        "recover-with-original-wal",
        "inspect-data-only",
    ] {
        if let Err(error) = run_phase_process(&root, phase, PROCESS_TIMEOUT) {
            panic!(
                "DDL remove crash durability failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root
            );
        }
    }

    fs::remove_dir_all(&root).expect("cleaning DDL remove crash root must succeed");
}

fn run_child_phase(phase: &str, root: &Path) -> TestResult<()> {
    match phase {
        "setup" => {
            let root = root.to_path_buf();
            run_on_runtime(4, PROCESS_TIMEOUT, move |rt| async move {
                phase_setup(rt, root).await
            })
        }
        "remove-crash" => {
            let root = root.to_path_buf();
            run_on_runtime(1, PROCESS_TIMEOUT, move |rt| async move {
                phase_remove_and_crash(rt, root).await
            })
        }
        "recover-with-original-wal" => {
            let root = root.to_path_buf();
            run_on_runtime(4, PROCESS_TIMEOUT, move |rt| async move {
                phase_recover_with_original_wal(rt, root).await
            })
        }
        "inspect-data-only" => {
            let root = root.to_path_buf();
            run_on_runtime(2, PROCESS_TIMEOUT, move |rt| async move {
                phase_inspect_data_only(rt, root).await
            })
        }
        other => Err(format!("unknown DDL remove crash phase: {other}")),
    }
}

async fn phase_setup(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let (db, logger) = build_database(&rt, &root.join("root-wal"), &root.join("database")).await?;
    let table_name = Atom::from(TABLE_NAME);
    let transaction = db
        .transaction(Atom::from("DDL remove crash setup"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected setup transaction".to_owned())?;
    transaction
        .create_table(
            table_name.clone(),
            KVTableMeta::new(
                KVDBTableType::MemOrdTab,
                false,
                EnumType::Usize,
                EnumType::Usize,
            ),
            false,
        )
        .await
        .map_err(|error| format!("creating setup table failed: {error}"))?;
    for index in 0..FILLER_TABLE_COUNT {
        transaction
            .create_table(filler_table_name(index), memory_meta(), false)
            .await
            .map_err(|error| format!("creating setup filler {index} failed: {error}"))?;
    }
    let prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing setup DDL failed: {error:?}"))?;
    if prepare.len() <= META_WAIT_THRESHOLD {
        return Err(format!(
            "setup root WAL payload did not exceed the Meta immediate-write threshold: {}",
            prepare.len()
        ));
    }
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing setup DDL failed: {error:?}"))?;

    let state =
        wait_for_logger_confirmation(&rt, &logger, SETUP_CONFIRM_TIMEOUT, "setup table creation")
            .await?;
    if state.append_total == 0 {
        return Err(format!(
            "setup did not append a root WAL transaction: {state:?}"
        ));
    }
    if !db.is_exist(&table_name).await {
        return Err("setup table disappeared before the crash phase".to_owned());
    }

    // Meta 已经落盘且根 WAL 已确认，后续阶段的基线不依赖进程析构行为。
    unsafe { libc::_exit(0) }
}

async fn phase_remove_and_crash(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let (db, logger) = build_database(&rt, &root.join("root-wal"), &root.join("database")).await?;
    let table_name = Atom::from(TABLE_NAME);
    if !db.is_exist(&table_name).await {
        return Err("setup table is absent before the remove transaction".to_owned());
    }

    let append_before = logger.append_total_count();
    let confirm_before = logger.confirm_total_count();
    let transaction = db
        .transaction(
            Atom::from("DDL remove immediate crash"),
            true,
            10_000,
            10_000,
        )
        .ok_or_else(|| "database rejected remove transaction".to_owned())?;
    transaction
        .remove_table(table_name.clone())
        .await
        .map_err(|error| format!("remove_table returned an error: {error}"))?;
    for index in 0..FILLER_TABLE_COUNT {
        transaction
            .remove_table(filler_table_name(index))
            .await
            .map_err(|error| format!("removing filler {index} failed: {error}"))?;
    }
    if db.is_exist(&table_name).await {
        return Err("remove_table did not remove the table from the live registry".to_owned());
    }
    if db.table_size().await != 1 {
        return Err(format!(
            "remove transaction left {} registry entries instead of only Meta",
            db.table_size().await
        ));
    }

    let prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing remove transaction failed: {error:?}"))?;
    if prepare.len() <= META_WAIT_THRESHOLD {
        return Err(format!(
            "remove transaction did not produce a complete root WAL payload: {} bytes",
            prepare.len()
        ));
    }
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing remove transaction failed: {error:?}"))?;

    // 这些计数读取不 await。单 worker 上，Meta commit 刚投递的后台写任务在 `_exit` 前
    // 不可能获得下一次 poll，因而这里精确模拟 API 返回成功后的立即进程崩溃。
    if logger.append_total_count() != append_before + 1
        || logger.confirm_total_count() != confirm_before
    {
        eprintln!(
            "remove root WAL counters mismatch: append {} -> {}, confirm {} -> {}",
            append_before,
            logger.append_total_count(),
            confirm_before,
            logger.confirm_total_count()
        );
        unsafe { libc::_exit(1) }
    }

    unsafe { libc::_exit(0) }
}

async fn phase_recover_with_original_wal(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
) -> TestResult<()> {
    // 正常启动必须同时加载 Meta 数据文件并重放原 WAL；这是提交成功后崩溃的合法恢复路径。
    let (db, logger) = build_database(&rt, &root.join("root-wal"), &root.join("database")).await?;
    let table_name = Atom::from(TABLE_NAME);
    if db.is_exist(&table_name).await {
        return Err(
            "remove_table returned commit success, but the table reappeared after an immediate crash and original-WAL recovery"
                .to_owned(),
        );
    }
    if db.is_exist(&filler_table_name(0)).await
        || db
            .is_exist(&filler_table_name(FILLER_TABLE_COUNT - 1))
            .await
        || db.table_size().await != 1
    {
        return Err(format!(
            "original-WAL recovery did not remove the complete table set; registry size: {}",
            db.table_size().await
        ));
    }
    wait_for_logger_confirmation(
        &rt,
        &logger,
        SETUP_CONFIRM_TIMEOUT,
        "recovered table removal",
    )
    .await?;
    Ok(())
}

async fn phase_inspect_data_only(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let (db, logger) = build_database(
        &rt,
        &root.join("inspection-empty-wal"),
        &root.join("database"),
    )
    .await?;
    if db.is_exist(&Atom::from(TABLE_NAME)).await
        || db.is_exist(&filler_table_name(0)).await
        || db
            .is_exist(&filler_table_name(FILLER_TABLE_COUNT - 1))
            .await
        || db.table_size().await != 1
    {
        return Err(format!(
            "data-only restart restored a removed table; registry size: {}",
            db.table_size().await
        ));
    }
    let state = LoggerState {
        waiting_confirm: logger.waiting_confirm_count().await,
        append_total: logger.append_total_count(),
        confirm_total: logger.confirm_total_count(),
    };
    if state
        != (LoggerState {
            waiting_confirm: 0,
            append_total: 0,
            confirm_total: 0,
        })
    {
        return Err(format!(
            "data-only inspection unexpectedly used its empty WAL: {state:?}"
        ));
    }
    Ok(())
}

async fn build_database(
    rt: &MultiTaskRuntime<()>,
    wal_path: &Path,
    db_path: &Path,
) -> TestResult<(RealDb, CommitLogger)> {
    fs::create_dir_all(wal_path)
        .map_err(|error| format!("creating WAL path {wal_path:?} failed: {error}"))?;
    let logger = CommitLoggerBuilder::new(rt.clone(), wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .build()
        .await
        .map_err(|error| format!("building CommitLogger at {wal_path:?} failed: {error}"))?;
    let manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger.clone(),
    );
    let db = KVDBManagerBuilder::new(rt.clone(), manager, db_path)
        .startup(false)
        .await
        .map_err(|error| format!("starting database at {db_path:?} failed: {error}"))?;
    Ok((db, logger))
}

async fn wait_for_logger_confirmation(
    rt: &MultiTaskRuntime<()>,
    logger: &CommitLogger,
    timeout: Duration,
    description: &str,
) -> TestResult<LoggerState> {
    let deadline = Instant::now() + timeout;
    loop {
        let state = LoggerState {
            waiting_confirm: logger.waiting_confirm_count().await,
            append_total: logger.append_total_count(),
            confirm_total: logger.confirm_total_count(),
        };
        if state.waiting_confirm == 0
            && state.append_total > 0
            && state.append_total == state.confirm_total
        {
            return Ok(state);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{description} was not confirmed within {timeout:?}; last state: {state:?}"
            ));
        }
        rt.timeout(25).await;
    }
}

fn memory_meta() -> KVTableMeta {
    KVTableMeta::new(
        KVDBTableType::MemOrdTab,
        false,
        EnumType::Usize,
        EnumType::Usize,
    )
}

fn filler_table_name(index: usize) -> Atom {
    let prefix = format!("{FILLER_NAME_PREFIX}{index:04}_");
    let mut name = String::with_capacity(MAX_TABLE_NAME_BYTES);
    name.push_str(&prefix);
    name.extend(std::iter::repeat('x').take(MAX_TABLE_NAME_BYTES - prefix.len()));
    Atom::from(name)
}

fn run_on_runtime<T, F, Fut>(workers: usize, timeout: Duration, build: F) -> TestResult<T>
where
    T: Send + 'static,
    F: FnOnce(MultiTaskRuntime<()>) -> Fut,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    let _time_loop = startup_global_time_loop(10);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(workers)
        .build();
    let future = build(rt.clone());
    let (result_tx, result_rx) = bounded(1);
    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    })
    .map_err(|error| format!("spawning DDL remove crash future failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("DDL remove crash future exceeded {timeout:?}: {error}"))?
}

fn run_phase_process(root: &Path, phase: &str, timeout: Duration) -> TestResult<()> {
    let executable = env::current_exe()
        .map_err(|error| format!("locating current test executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase)
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| format!("spawning phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("phase {phase} exited with {status}"))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking child status failed: {error}"))?
        {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("child exceeded {timeout:?}"));
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
        "pi_db_ddl_remove_crash_{}_{}",
        std::process::id(),
        nanos
    ))
}
