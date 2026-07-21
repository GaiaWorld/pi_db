//! 启动加载超过 8192 条持久化表元信息时的真实批次边界专项。
//!
//! 本 target 不引用或运行旧测试。它通过公开
//! `KVDBManagerBuilder -> KVDBManager -> KVDBTransaction` 链路，使用真实 4-worker runtime、
//! `Transaction2PcManager`、`CommitLogger`、Meta 表日志、文件系统和两个隔离进程验证
//! `FIND-START-001`：
//!
//! - `setup` 在一个 DDL 根事务中创建 8193 张 Memory 表，严格确认 live registry、Meta 记录、
//!   根 WAL append/confirm/waiting 和非空 `.bak`；
//! - `inspect-data-only` 使用独立空 WAL 打开同一数据目录，精确比较权威 Meta 记录数、完整表名
//!   集合和 live registry，禁止根 WAL repair 掩盖启动批次装配错误。
//!
//! 表名长度固定为 2100 字节，使 Meta prepare 输出稳定超过 16 MiB 立即整理阈值，避免依赖
//! 60 秒周期任务。Memory 表没有独立数据文件，测试负载集中在 Meta 持久化与启动注册流程。
//! 正确 ABI ASan 可显式启用 64 字节短名夹具：表数、批次边界和全部持久化/集合断言不变，
//! setup 通过真实 60 秒 collector 完成确认，只移除与启动边界内存安全无关的长字符串开销。
//! 修复前本 target 以相同正确目标断言稳定失败；修复通过专项、安全、性能和完整新回归门禁后，
//! 已加入 `scripts/new-regression-targets.txt`，用于永久保护该生产边界。
//! 问题归档：`docs/STARTUP_TABLE_BATCH_BOUNDARY_BUG.md#startup-table-batch-boundary-index`。

#![cfg(target_os = "linux")]

use std::{
    collections::BTreeSet,
    env, fs,
    future::Future,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    sync::OnceLock,
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
    KVDBTableType, KVTableMeta,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;

const TEST_NAME: &str = "test_startup_loads_every_table_across_8192_boundary";
const PHASE_ENV: &str = "PI_DB_STARTUP_BATCH_BOUNDARY_PHASE";
const ROOT_ENV: &str = "PI_DB_STARTUP_BATCH_BOUNDARY_ROOT";
const SHORT_NAMES_ENV: &str = "PI_DB_STARTUP_BATCH_BOUNDARY_SHORT_NAMES";
const META_TABLE: &str = ".tables_meta";
const TABLE_NAME_PREFIX: &str = "startup_batch_table_";
const TABLE_COUNT: usize = 8_193;
const TABLE_NAME_BYTES: usize = 2_100;
const SHORT_TABLE_NAME_BYTES: usize = 64;
const META_WAIT_THRESHOLD: usize = 16 * 1024 * 1024;
const PROCESS_TIMEOUT: Duration = Duration::from_secs(180);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(90);
static EFFECTIVE_TABLE_NAME_BYTES: OnceLock<usize> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LoggerState {
    waiting_confirm: usize,
    append_total: usize,
    confirm_total: usize,
}

/// 正确目标：空 WAL 冷启动必须从 8193 条权威 Meta 记录装配出全部 8193 张用户表。
#[test]
fn test_startup_loads_every_table_across_8192_boundary() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV).expect("startup batch child phase must receive its root path"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("startup batch phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root();
    fs::create_dir_all(&root).expect("creating startup batch root must succeed");

    for phase in ["setup", "inspect-data-only"] {
        if let Err(error) = run_phase_process(&root, phase, PROCESS_TIMEOUT) {
            panic!(
                "startup table batch boundary failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root
            );
        }
    }

    fs::remove_dir_all(&root).expect("cleaning startup batch root must succeed");
}

fn run_child_phase(phase: &str, root: &Path) -> TestResult<()> {
    match phase {
        "setup" => {
            let root = root.to_path_buf();
            run_on_runtime(PROCESS_TIMEOUT, move |rt| async move {
                phase_setup(rt, root).await
            })
        }
        "inspect-data-only" => {
            let root = root.to_path_buf();
            run_on_runtime(PROCESS_TIMEOUT, move |rt| async move {
                phase_inspect_data_only(rt, root).await
            })
        }
        other => Err(format!("unknown startup batch phase: {other}")),
    }
}

async fn phase_setup(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let wal_path = root.join("root-wal");
    let db_path = root.join("database");
    let (db, logger) = build_database(&rt, &wal_path, &db_path).await?;
    let transaction = db
        .transaction(Atom::from("startup batch boundary setup"), true, 30_000, 30_000)
        .ok_or_else(|| "database rejected startup batch setup transaction".to_owned())?;

    for index in 0..TABLE_COUNT {
        transaction
            .create_table(table_name(index), memory_meta(), false)
            .await
            .map_err(|error| format!("creating startup batch table {index} failed: {error}"))?;
    }

    expect_eq(
        "setup live registry size before prepare",
        &db.table_size().await,
        &(TABLE_COUNT + 1),
    )?;
    let prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing startup batch DDL failed: {error:?}"))?;
    if prepare.is_empty() {
        return Err("startup batch prepare unexpectedly produced an empty root WAL".to_owned());
    }
    if table_name_bytes() == TABLE_NAME_BYTES && prepare.len() <= META_WAIT_THRESHOLD {
        return Err(format!(
            "startup batch prepare did not exceed the Meta immediate-write threshold: {}",
            prepare.len()
        ));
    }
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing startup batch DDL failed: {error:?}"))?;

    let state = wait_for_logger_confirmation(
        &rt,
        &logger,
        CONFIRM_TIMEOUT,
        "startup batch setup",
    )
    .await?;
    if state.append_total != 1 || state.confirm_total != 1 {
        return Err(format!(
            "setup root WAL must contain exactly one confirmed transaction: {state:?}"
        ));
    }
    if nonempty_bak_count(&wal_path)? == 0 {
        return Err("setup produced no nonempty confirmed .bak checkpoint".to_owned());
    }
    expect_eq(
        "setup committed Meta record count",
        &db.table_record_size(&Atom::from(META_TABLE)).await,
        &Some(TABLE_COUNT),
    )?;
    verify_registry(&db, "setup live registry").await?;

    // Meta 和根 WAL 均已完成确认；立即退出可保证下一阶段只消费已落地数据。
    // SAFETY: 当前是隔离的测试子进程，所有持久化硬门禁都已通过；不再运行用户代码，也不依赖
    // Rust 析构完成其它数据写入。跳过 runtime 后台线程析构正是模拟进程立即退出所需的行为。
    unsafe { libc::_exit(0) }
}

async fn phase_inspect_data_only(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let wal_path = root.join("inspection-empty-wal");
    let db_path = root.join("database");
    let (db, logger) = build_database(&rt, &wal_path, &db_path).await?;

    expect_eq(
        "data-only authoritative Meta record count",
        &db.table_record_size(&Atom::from(META_TABLE)).await,
        &Some(TABLE_COUNT),
    )?;
    let state = LoggerState {
        waiting_confirm: logger.waiting_confirm_count().await,
        append_total: logger.append_total_count(),
        confirm_total: logger.confirm_total_count(),
    };
    expect_eq(
        "data-only empty WAL state",
        &state,
        &LoggerState {
            waiting_confirm: 0,
            append_total: 0,
            confirm_total: 0,
        },
    )?;
    verify_registry(&db, "data-only live registry").await
}

async fn verify_registry(db: &RealDb, label: &str) -> TestResult<()> {
    let actual = db
        .tables()
        .await
        .into_iter()
        .map(|name| name.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    let mut expected = BTreeSet::new();
    expected.insert(META_TABLE.to_owned());
    for index in 0..TABLE_COUNT {
        expected.insert(table_name(index).as_str().to_owned());
    }

    if actual != expected {
        let missing_count = expected.difference(&actual).count();
        let unexpected_count = actual.difference(&expected).count();
        let missing = summarize_names(expected.difference(&actual));
        let unexpected = summarize_names(actual.difference(&expected));
        return Err(format!(
            "{label} differs from authoritative Meta: expected={}, actual={}, missing_count={}, unexpected_count={}, first_missing={missing:?}, first_unexpected={unexpected:?}",
            expected.len(),
            actual.len(),
            missing_count,
            unexpected_count,
        ));
    }
    expect_eq(
        &format!("{label} table count"),
        &db.table_size().await,
        &(TABLE_COUNT + 1),
    )
}

fn summarize_names<'a>(names: impl Iterator<Item = &'a String>) -> Vec<String> {
    names
        .take(8)
        .map(|name| {
            let prefix = name
                .chars()
                .take(TABLE_NAME_PREFIX.len() + 6)
                .collect::<String>();
            format!("{prefix}...(len={})", name.len())
        })
        .collect()
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

fn table_name(index: usize) -> Atom {
    let prefix = format!("{TABLE_NAME_PREFIX}{index:05}_");
    let table_name_bytes = table_name_bytes();
    let mut name = String::with_capacity(table_name_bytes);
    name.push_str(&prefix);
    name.extend(std::iter::repeat_n('x', table_name_bytes - prefix.len()));
    Atom::from(name)
}

fn table_name_bytes() -> usize {
    *EFFECTIVE_TABLE_NAME_BYTES.get_or_init(|| {
        if env::var_os(SHORT_NAMES_ENV).is_some() {
            SHORT_TABLE_NAME_BYTES
        } else {
            TABLE_NAME_BYTES
        }
    })
}

fn nonempty_bak_count(path: &Path) -> TestResult<usize> {
    let mut count = 0usize;
    for entry in fs::read_dir(path)
        .map_err(|error| format!("reading WAL directory {path:?} failed: {error}"))?
    {
        let entry = entry.map_err(|error| format!("reading WAL entry failed: {error}"))?;
        let entry_path = entry.path();
        if entry_path.extension().and_then(|value| value.to_str()) == Some("bak")
            && entry
                .metadata()
                .map_err(|error| format!("reading WAL metadata {entry_path:?} failed: {error}"))?
                .len()
                > 0
        {
            count += 1;
        }
    }
    Ok(count)
}

fn run_on_runtime<T, F, Fut>(timeout: Duration, build: F) -> TestResult<T>
where
    T: Send + 'static,
    F: FnOnce(MultiTaskRuntime<()>) -> Fut,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    let _time_loop = startup_global_time_loop(10);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(4)
        .build();
    let future = build(rt.clone());
    let (result_tx, result_rx) = bounded(1);
    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    })
    .map_err(|error| format!("spawning startup batch future failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("startup batch future exceeded {timeout:?}: {error}"))?
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
        .map_err(|error| format!("spawning startup batch phase {phase} failed: {error}"))?;
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
        "pi_db_startup_batch_boundary_{}_{}",
        std::process::id(),
        nanos
    ))
}

fn expect_eq<T: std::fmt::Debug + PartialEq>(
    label: &str,
    actual: &T,
    expected: &T,
) -> TestResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{label}: expected {expected:?}, observed {actual:?}"
        ))
    }
}
