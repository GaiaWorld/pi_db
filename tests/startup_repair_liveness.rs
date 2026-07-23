//! 单 worker runtime 上启动恢复执行上下文的真实公开装配专项。
//!
//! 空 WAL、4-worker 未确认 WAL、外部线程驱动的 1-worker 未确认 WAL，以及同 runtime worker
//! 驱动的 1-worker 同构未确认 WAL 分别运行在独立子进程中。外部线程分支严格模拟
//! `pi-launcher` 使用 `futures::executor::block_on` 驱动 `pi_db_server` 初始化的上下文；只有最后
//! 一个分支把 startup future 投递给数据库 runtime 自身。setup 通过公开 Schema prelude +
//! 普通事务创建 LogOrdered 表并写入非空值，在表 collector 确认前退出；recover 只调用公开
//! `KVDBManagerBuilder::startup(false)`。子进程内同步截止和父进程强制截止共同保证当前死锁实现
//! 能够形成有限、可重复的红证据，而不会挂住测试进程。
//!
//! 归档结论、执行上下文和证据边界见：
//! `docs/STARTUP_REPAIR_RUNTIME_LIVENESS_BUG.md#bug-startup-repair-liveness-index`。

mod key_version_support;

use std::{
    env,
    fs,
    future::Future,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::{bounded, RecvTimeoutError};
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::AsyncCommitLog;
use pi_atom::Atom;
use pi_db::{
    tables::TableKV,
    utils::CreateTableOptions,
    KVDBTableType,
};

use key_version_support::{
    TestResult, build_database, commit_ordinary, encode_usize, expect_binary, expect_eq,
    query_ordinary, table_meta, writable_transaction,
};

const TEST_NAME: &str = "test_startup_repair_runtime_liveness";
const PHASE_ENV: &str = "PI_DB_STARTUP_REPAIR_LIVENESS_PHASE";
const ROOT_ENV: &str = "PI_DB_STARTUP_REPAIR_LIVENESS_ROOT";
const TABLE_NAME: &str = "startup_repair_liveness_log_ordered";
const EMPTY_PHASE_TIMEOUT: Duration = Duration::from_secs(8);
const SETUP_PHASE_TIMEOUT: Duration = Duration::from_secs(8);
const MULTI_RECOVERY_TIMEOUT: Duration = Duration::from_secs(8);
const IN_RUNTIME_SINGLE_RECOVERY_TIMEOUT: Duration = Duration::from_secs(6);
const CHILD_PROCESS_TIMEOUT: Duration = Duration::from_secs(15);

#[test]
fn test_startup_repair_runtime_liveness() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV)
                .expect("startup repair liveness child phase must receive its root path"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("startup repair liveness phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root();
    fs::create_dir_all(&root).expect("creating startup repair liveness root must succeed");
    let phases = [
        ("empty-single", root.join("empty-single")),
        ("setup-multi", root.join("multi-worker")),
        ("recover-multi", root.join("multi-worker")),
        ("setup-external-single", root.join("external-single-worker")),
        ("recover-external-single", root.join("external-single-worker")),
        ("setup-single", root.join("single-worker")),
        ("recover-single-redline", root.join("single-worker")),
    ];

    for (phase, phase_root) in phases {
        if let Err(error) = run_phase_process(&phase_root, phase, CHILD_PROCESS_TIMEOUT) {
            panic!(
                "startup repair liveness failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root,
            );
        }
        if phase == "setup-multi"
            || phase == "setup-external-single"
            || phase == "setup-single" {
            assert_pending_wal(&phase_root)
                .unwrap_or_else(|error| panic!("{phase} produced invalid WAL evidence: {error}"));
        }
    }

    fs::remove_dir_all(&root)
        .expect("cleaning startup repair liveness root must succeed");
}

fn run_child_phase(phase: &str, root: &Path) -> TestResult<()> {
    match phase {
        "empty-single" => {
            let root = root.to_path_buf();
            run_on_runtime(1, EMPTY_PHASE_TIMEOUT, move |rt| async move {
                phase_empty_single(rt, root).await
            })
        },
        "setup-multi" | "setup-external-single" | "setup-single" => {
            let root = root.to_path_buf();
            run_on_runtime(4, SETUP_PHASE_TIMEOUT, move |rt| async move {
                phase_setup(rt, root).await
            })
        },
        "recover-multi" => {
            let root = root.to_path_buf();
            run_on_runtime(4, MULTI_RECOVERY_TIMEOUT, move |rt| async move {
                phase_recover(rt, root).await
            })
        },
        "recover-external-single" => {
            let root = root.to_path_buf();
            run_on_caller_thread(1, move |rt| async move {
                phase_recover(rt, root).await
            })
        },
        "recover-single-redline" => {
            let root = root.to_path_buf();
            expect_same_runtime_single_worker_redline(root)
        },
        other => Err(format!("unknown startup repair liveness phase: {other}")),
    }
}

async fn phase_empty_single(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let fixture = build_database(&rt, &root, Duration::ZERO, Duration::ZERO).await?;
    expect_eq("empty single-worker table count", &fixture.db.table_size().await, &1usize)?;
    expect_eq("empty single-worker WAL append count", &fixture.logger.append_total_count(), &0usize)?;
    expect_eq("empty single-worker WAL confirm count", &fixture.logger.confirm_total_count(), &0usize)?;
    expect_eq(
        "empty single-worker WAL waiting count",
        &fixture.logger.waiting_confirm_count().await,
        &0usize,
    )?;
    expect_eq(
        "empty single-worker manager produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &0usize,
    )?;
    expect_eq(
        "empty single-worker manager consumed count",
        &fixture.tr_manager.consumed_transaction_total(),
        &0usize,
    )?;
    expect_eq(
        "empty single-worker manager active count",
        &fixture.tr_manager.transaction_len(),
        &0usize,
    )
}

async fn phase_setup(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let fixture = build_database(&rt, &root, Duration::ZERO, Duration::ZERO).await?;
    let transaction = writable_transaction(&fixture.db, "startup repair liveness setup")?;
    transaction
        .create_table_with_options(
            Atom::from(TABLE_NAME),
            table_meta(KVDBTableType::LogOrdTab, true),
            CreateTableOptions::LogOrdTab(64 * 1024 * 1024, 1024 * 1024, 1024 * 1024),
            false,
        )
        .await
        .map_err(|error| format!("creating liveness LogOrdered table failed: {error}"))?;

    let key = test_key();
    let value = test_value();
    transaction
        .upsert(vec![TableKV::new(
            Atom::from(TABLE_NAME),
            key.clone(),
            Some(value.clone()),
        )])
        .await
        .map_err(|error| format!("writing liveness LogOrdered value failed: {error:?}"))?;
    commit_ordinary(&transaction, "startup repair liveness setup").await?;

    expect_eq("setup registered table count", &fixture.db.table_size().await, &2usize)?;
    expect_eq("setup WAL append count", &fixture.logger.append_total_count(), &1usize)?;
    expect_eq("setup WAL confirm count", &fixture.logger.confirm_total_count(), &0usize)?;
    expect_eq(
        "setup WAL waiting count",
        &fixture.logger.waiting_confirm_count().await,
        &1usize,
    )?;
    expect_eq(
        "setup manager produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &1usize,
    )?;
    expect_eq(
        "setup manager consumed count",
        &fixture.tr_manager.consumed_transaction_total(),
        &1usize,
    )?;
    expect_eq("setup manager active count", &fixture.tr_manager.transaction_len(), &0usize)?;

    let observed = query_ordinary(
        &fixture.db,
        TABLE_NAME,
        key,
        "startup repair liveness setup query",
    ).await?;
    expect_binary("setup authoritative value", observed.as_ref(), Some(&value))
}

async fn phase_recover(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    // 当前缺陷只会让“由同一 runtime 唯一 worker 驱动”的分支停在 build_database 内部 startup；
    // 外部调用线程驱动的单 worker 分支必须正常返回。后续断言只在 startup 返回后执行，因此
    // 超时证据不会与 query、collector 或确认等待混淆。
    let fixture = build_database(&rt, &root, Duration::ZERO, Duration::ZERO).await?;
    expect_eq("recovered registered table count", &fixture.db.table_size().await, &2usize)?;
    expect_eq("recovered WAL append count", &fixture.logger.append_total_count(), &1usize)?;
    expect_eq(
        "recovered manager produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &1usize,
    )?;
    expect_eq(
        "recovered manager consumed count",
        &fixture.tr_manager.consumed_transaction_total(),
        &1usize,
    )?;
    expect_eq("recovered manager active count", &fixture.tr_manager.transaction_len(), &0usize)?;

    let key = test_key();
    let value = test_value();
    let observed = query_ordinary(
        &fixture.db,
        TABLE_NAME,
        key,
        "startup repair liveness recovered query",
    ).await?;
    expect_binary("recovered authoritative value", observed.as_ref(), Some(&value))
}

fn run_on_runtime<T, F, Fut>(
    worker_count: usize,
    timeout: Duration,
    build: F,
) -> TestResult<T>
where
    T: Send + 'static,
    F: FnOnce(MultiTaskRuntime<()>) -> Fut,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    let _time_loop = startup_global_time_loop(1);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(worker_count)
        .set_worker_limit(worker_count, worker_count)
        .build();
    let future = build(rt.clone());
    let (result_tx, result_rx) = bounded(1);
    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    })
    .map_err(|error| format!("spawning {worker_count}-worker liveness future failed: {error:?}"))?;

    result_rx
        .recv_timeout(timeout)
        .map_err(|error| {
            format!(
                "{worker_count}-worker startup repair future exceeded {timeout:?}: {error}",
            )
        })?
}

/// 特征化当前已归档的执行上下文红线，而不是把它当作生产启动失败用例：startup 已经在
/// 数据库 runtime 的唯一 worker 内运行时，非空 WAL callback 会同步等待刚投递到同一 runtime
/// 的 repair task。只有精确等待超时才符合当前已确认限制；startup 提前返回错误、channel 断开、
/// panic 或意外成功都会使本测试失败，要求重新审查实现、文档和限制状态。
fn expect_same_runtime_single_worker_redline(root: PathBuf) -> TestResult<()> {
    let _time_loop = startup_global_time_loop(1);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(1)
        .set_worker_limit(1, 1)
        .build();
    let future_rt = rt.clone();
    let (result_tx, result_rx) = bounded(1);
    rt.spawn(async move {
        let _ = result_tx.send(phase_recover(future_rt, root).await);
    })
    .map_err(|error| format!("spawning single-worker redline future failed: {error:?}"))?;

    match result_rx.recv_timeout(IN_RUNTIME_SINGLE_RECOVERY_TIMEOUT) {
        Err(RecvTimeoutError::Timeout) => Ok(()),
        Err(RecvTimeoutError::Disconnected) => Err(
            "single-worker redline result channel disconnected before the expected wait timeout"
                .to_owned(),
        ),
        Ok(Err(error)) => Err(format!(
            "same-runtime single-worker startup returned an error instead of reaching the documented wait redline: {error}",
        )),
        Ok(Ok(())) => Err(
            "same-runtime single-worker startup unexpectedly completed; re-audit the repair execution context and update the archived limitation before changing this redline"
                .to_owned(),
        ),
    }
}

/// 与 pi-launcher 的数据库进程启动方式一致：startup future 由调用线程驱动，传给数据库的
/// runtime 只执行文件、repair_commit 和提交确认任务。父进程硬截止负责处理意外停滞。
fn run_on_caller_thread<T, F, Fut>(worker_count: usize, build: F) -> TestResult<T>
where
    F: FnOnce(MultiTaskRuntime<()>) -> Fut,
    Fut: Future<Output = TestResult<T>>,
{
    let _time_loop = startup_global_time_loop(1);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(worker_count)
        .set_worker_limit(worker_count, worker_count)
        .build();
    futures::executor::block_on(build(rt))
}

fn assert_pending_wal(root: &Path) -> TestResult<()> {
    let wal_path = root.join("root-wal");
    let mut nonempty_active = Vec::new();
    let mut nonempty_backup = Vec::new();
    for entry in fs::read_dir(&wal_path)
        .map_err(|error| format!("reading WAL directory {wal_path:?} failed: {error}"))? {
        let entry = entry
            .map_err(|error| format!("reading WAL entry in {wal_path:?} failed: {error}"))?;
        let metadata = entry
            .metadata()
            .map_err(|error| format!("reading WAL metadata for {:?} failed: {error}", entry.path()))?;
        if !metadata.is_file() || metadata.len() == 0 {
            continue;
        }
        if entry.path().extension().and_then(|extension| extension.to_str()) == Some("bak") {
            nonempty_backup.push((entry.path(), metadata.len()));
        } else {
            nonempty_active.push((entry.path(), metadata.len()));
        }
    }
    if !nonempty_backup.is_empty() {
        return Err(format!(
            "setup WAL was already confirmed instead of remaining pending: {nonempty_backup:?}",
        ));
    }
    if nonempty_active.is_empty() {
        return Err("setup produced no nonempty active root WAL".to_owned());
    }
    Ok(())
}

fn run_phase_process(root: &Path, phase: &str, timeout: Duration) -> TestResult<()> {
    let executable = env::current_exe()
        .map_err(|error| format!("locating startup repair liveness executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase)
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| format!("spawning startup repair liveness phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("startup repair liveness phase {phase} exited with {status}"))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking liveness child status failed: {error}"))? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("startup repair liveness child exceeded {timeout:?}"));
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn test_key() -> pi_db::Binary {
    encode_usize(11)
}

fn test_value() -> pi_db::Binary {
    encode_usize(29)
}

fn unique_temp_root() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must not precede UNIX_EPOCH")
        .as_nanos();
    env::temp_dir().join(format!(
        "pi_db_startup_repair_liveness_{}_{}",
        std::process::id(),
        nanos,
    ))
}
