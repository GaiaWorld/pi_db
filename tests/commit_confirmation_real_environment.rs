//! LogOrdered 最终持久化成功信号、WAL 保留与恢复的真实生产环境专项。
//!
//! 本 target 不运行或引用任何旧测试。它使用完整的
//! `KVDBManager + Transaction2PcManager + CommitLogger + LogOrderedTable` 调用链，并在 Linux
//! 隔离子进程中用 `RLIMIT_FSIZE` 令表日志执行真实的零字节写失败。测试验证当前确认协议：
//!
//! - 表日志持久化失败时，LogOrdered 不调用 `KVDBCommitConfirm`，根 WAL 保持未确认；
//! - 失败值未进入表数据文件，不能用内存可见性伪装成持久化成功；
//! - 使用原根 WAL 重启后，`try_repair` 重放事务、补齐表日志并最终确认；
//! - 再次绕开原 WAL 时，修复值仍能仅从表数据文件读取。
//!
//! 该测试验证的是合法生产路径中的“失败不发送成功信号”，不会直接构造确认器或向通用
//! `Result` 参数注入内置协议不会产生的 `Err`。
//!
//! 故障场景中，根 WAL 的当前文件小于限制，因此必须正常落地；表日志已经达到限制，因此
//! 写入必须失败。
//!
//! 完整链路验证分四个进程阶段：
//!
//! - `fault`：创建真实数据库和基线，注入表日志写失败，核对 WAL 待确认状态；
//! - `inspect-before`：使用独立空 WAL 只从表数据文件加载，证明故障值尚未持久化；
//! - `recover`：使用原 WAL 正常启动，等待 `try_repair` 完成持久化确认；
//! - `inspect-after`：再次绕开原 WAL，只从表数据文件加载，证明故障值已真实修复。
//!
//! 每个阶段均由当前测试二进制的独立子进程执行。文件大小限制、信号处理、runtime 后台任务
//! 和打开的文件句柄不会泄漏到测试驱动进程。所有等待都有截止时间；生产 `error!` 日志、
//! 物理文件长度、真实确认计数、`.bak` 文件和重启查询共同构成故障确实生效的证据。`.bak` 与
//! 确认计数只是日志状态门禁；`inspect-after` 冷态读取到精确最终 Key/Value 是独立且不可替代的
//! 数据硬门禁，任一门禁失败都必须判定恢复失败。
//!
//! 关联生产入口：`pi_db::KVDBCommitConfirm`、`pi_db::db::KVDBManagerBuilder`、
//! `pi_db::db::KVDBTransaction::commit_modified`、`pi_db::tables::log_ord_table`、
//! `pi_store::commit_logger::CommitLogger`。
//!
//! 本地双向入口：`docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only`、
//! `docs/PI_DB_ARCHITECTURE.md#arch-confirm-success-only`、
//! `docs/TEST_AND_BENCHMARK_STRATEGY.md#test-bug-001-reclassification`、
//! `docs/BUG_001_FIX_PLAN.md#bug-001-closure-index`。

#![cfg(target_os = "linux")]

use std::{
    env, fs,
    future::Future,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    sync::Mutex,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use log::{Level, LevelFilter, Log, Metadata, Record};
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::{manager_2pc::Transaction2PcManager, AsyncCommitLog};
use pi_atom::Atom;
use pi_bon::{Decode, Encode, ReadBuffer, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    utils::CreateTableOptions,
    Binary, KVDBTableType, KVTableMeta,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const FULL_CHAIN_TEST_NAME: &str =
    "test_log_ordered_persistence_failure_preserves_wal_and_recovers";
const PHASE_ENV: &str = "PI_DB_CONFIRMATION_REAL_PHASE";
const ROOT_ENV: &str = "PI_DB_CONFIRMATION_REAL_ROOT";

const TABLE_NAME: &str = "confirmation_log_table";
const BASELINE_KEY: u8 = 1;
const FAILED_KEY: u8 = 2;
const BASELINE_FILL: u8 = b'b';
const FAILED_FILL: u8 = b'f';

// 两个值都超过 LogOrderedTable 固定的 16 MiB waits_limit，提交后会立即进入 collect_waits。
const BASELINE_PAYLOAD_LEN: usize = 24 * 1024 * 1024;
const FAILED_PAYLOAD_LEN: usize = 16 * 1024 * 1024 + 1024;

const DDL_CONFIRM_TIMEOUT: Duration = Duration::from_secs(75);
const TABLE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(25);
const STORAGE_FAILURE_TIMEOUT: Duration = Duration::from_secs(25);

/// 对真实 `CommitLogger` 的可观察计数快照。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LoggerState {
    waiting_confirm: usize,
    append_total: usize,
    confirm_total: usize,
}

/// 只捕获生产错误日志的进程级观察器。
///
/// 它不改变任何生产返回值或调度，只用于证明 `collect_waits` 确实执行到真实文件写失败分支。
struct ErrorCaptureLogger;

static ERROR_CAPTURE_LOGGER: ErrorCaptureLogger = ErrorCaptureLogger;
static CAPTURED_ERRORS: Mutex<Vec<String>> = Mutex::new(Vec::new());

impl Log for ErrorCaptureLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= Level::Error
    }

    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            CAPTURED_ERRORS
                .lock()
                .expect("error capture mutex must not be poisoned")
                .push(format!("{}", record.args()));
        }
    }

    fn flush(&self) {}
}

/// 临时降低当前子进程的最大文件长度，并在离开作用域时恢复原值和信号处理器。
///
/// 安全边界：测试只在隔离 Linux 子进程内调用；限制值来自已存在表日志的真实长度，
/// `Drop` 恢复原始 `rlimit`。`SIGXFSZ` 临时设为忽略，使写调用返回 `EFBIG` 而不是终止进程。
struct FileSizeLimitGuard {
    original_limit: libc::rlimit,
    original_sigxfsz: libc::sighandler_t,
}

impl FileSizeLimitGuard {
    fn install(max_file_len: u64) -> TestResult<Self> {
        let mut original_limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };

        // SAFETY: `original_limit` points to valid writable storage for one `libc::rlimit` value.
        if unsafe { libc::getrlimit(libc::RLIMIT_FSIZE, &mut original_limit) } != 0 {
            return Err(format!(
                "getrlimit(RLIMIT_FSIZE) failed: {}",
                std::io::Error::last_os_error()
            ));
        }

        if original_limit.rlim_max != libc::RLIM_INFINITY && max_file_len > original_limit.rlim_max
        {
            return Err(format!(
                "requested RLIMIT_FSIZE {max_file_len} exceeds hard limit {}",
                original_limit.rlim_max
            ));
        }

        // SAFETY: `signal` installs a process-wide disposition in this isolated child process.
        let original_sigxfsz = unsafe { libc::signal(libc::SIGXFSZ, libc::SIG_IGN) };
        if original_sigxfsz == libc::SIG_ERR {
            return Err(format!(
                "ignoring SIGXFSZ failed: {}",
                std::io::Error::last_os_error()
            ));
        }

        let limited = libc::rlimit {
            rlim_cur: max_file_len as libc::rlim_t,
            rlim_max: original_limit.rlim_max,
        };
        // SAFETY: `limited` is initialized and preserves the process hard limit.
        if unsafe { libc::setrlimit(libc::RLIMIT_FSIZE, &limited) } != 0 {
            // SAFETY: restoring the previously returned signal handler is valid in this child.
            unsafe {
                libc::signal(libc::SIGXFSZ, original_sigxfsz);
            }
            return Err(format!(
                "setrlimit(RLIMIT_FSIZE={max_file_len}) failed: {}",
                std::io::Error::last_os_error()
            ));
        }

        Ok(Self {
            original_limit,
            original_sigxfsz,
        })
    }
}

impl Drop for FileSizeLimitGuard {
    fn drop(&mut self) {
        // SAFETY: both values were obtained from successful libc calls in `install`.
        unsafe {
            let _ = libc::setrlimit(libc::RLIMIT_FSIZE, &self.original_limit);
            libc::signal(libc::SIGXFSZ, self.original_sigxfsz);
        }
    }
}

/// 在真实多线程 runtime 上执行一个 future，并以同步截止时间返回结果。
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
    .map_err(|error| format!("spawning test future failed: {error:?}"))?;

    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("test future exceeded {timeout:?}: {error}"))?
}

/// 安装错误观察器；每个故障阶段都在全新子进程中，因此不存在测试间 logger 复用。
fn install_error_capture() -> TestResult<()> {
    CAPTURED_ERRORS
        .lock()
        .map_err(|_| "error capture mutex is poisoned".to_owned())?
        .clear();
    log::set_logger(&ERROR_CAPTURE_LOGGER)
        .map_err(|_| "installing the process error logger failed".to_owned())?;
    log::set_max_level(LevelFilter::Error);
    Ok(())
}

fn captured_errors() -> TestResult<Vec<String>> {
    CAPTURED_ERRORS
        .lock()
        .map(|messages| messages.clone())
        .map_err(|_| "error capture mutex is poisoned".to_owned())
}

/// 等待生产日志明确报告目标表的真实整理/写入失败。
async fn wait_for_storage_failure_log(rt: &MultiTaskRuntime<()>) -> TestResult<Vec<String>> {
    let deadline = Instant::now() + STORAGE_FAILURE_TIMEOUT;
    loop {
        let messages = captured_errors()?;
        if messages.iter().any(|message| {
            message.contains("Collect log ordered table failed") && message.contains(TABLE_NAME)
        }) {
            return Ok(messages);
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "no production LogOrderedTable storage failure was observed within {:?}; captured errors: {:?}",
                STORAGE_FAILURE_TIMEOUT, messages
            ));
        }
        rt.timeout(25).await;
    }
}

async fn logger_state(logger: &CommitLogger) -> LoggerState {
    LoggerState {
        waiting_confirm: logger.waiting_confirm_count().await,
        append_total: logger.append_total_count(),
        confirm_total: logger.confirm_total_count(),
    }
}

/// 轮询真实 logger 状态，避免以固定 sleep 代替完成条件。
async fn wait_for_logger_state<P>(
    rt: &MultiTaskRuntime<()>,
    logger: &CommitLogger,
    timeout: Duration,
    description: &str,
    mut predicate: P,
) -> TestResult<LoggerState>
where
    P: FnMut(&LoggerState) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        let state = logger_state(logger).await;
        if predicate(&state) {
            return Ok(state);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for {description} after {timeout:?}; last logger state: {state:?}"
            ));
        }
        rt.timeout(25).await;
    }
}

fn encode_u8(value: u8) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

/// 生成类型元数据为 `Str` 时合法的 BON 字符串值。
fn encode_string(payload_len: usize, fill: u8) -> Binary {
    let value =
        String::from_utf8(vec![fill; payload_len]).expect("the test only uses an ASCII fill byte");
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

/// 对重启后读取的值执行类型级验证，不只检查 `Some`。
fn validate_string(value: &Binary, expected_len: usize, expected_fill: u8) -> TestResult<()> {
    let mut buffer = ReadBuffer::new(value.as_ref(), 0);
    let decoded = String::decode(&mut buffer)
        .map_err(|error| format!("decoding persisted BON string failed: {error:?}"))?;
    if decoded.len() != expected_len {
        return Err(format!(
            "persisted string length mismatch: expected {expected_len}, observed {}",
            decoded.len()
        ));
    }
    if decoded.as_bytes().iter().any(|byte| *byte != expected_fill) {
        return Err(format!(
            "persisted string payload contains bytes other than {expected_fill}"
        ));
    }
    Ok(())
}

async fn build_database(
    rt: &MultiTaskRuntime<()>,
    db_path: &Path,
    wal_path: &Path,
) -> TestResult<(RealDb, CommitLogger)> {
    let logger = CommitLoggerBuilder::new(rt.clone(), wal_path)
        .log_file_limit(256 * 1024 * 1024)
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

async fn create_log_table(db: &RealDb) -> TestResult<()> {
    let table = Atom::from(TABLE_NAME);
    let transaction = db
        .transaction(Atom::from("confirmation real DDL"), true, 5_000, 5_000)
        .ok_or_else(|| "database rejected the DDL transaction".to_owned())?;
    transaction
        .create_table_with_options(
            table,
            KVTableMeta::new(KVDBTableType::LogOrdTab, true, EnumType::U8, EnumType::Str),
            CreateTableOptions::LogOrdTab(256 * 1024 * 1024, 2 * 1024 * 1024, 2 * 1024 * 1024),
            false,
        )
        .await
        .map_err(|error| format!("creating {TABLE_NAME} failed: {error}"))?;
    let prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing {TABLE_NAME} creation failed: {error:?}"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing {TABLE_NAME} creation failed: {error:?}"))
}

async fn prepare_upsert(
    db: &RealDb,
    source: &'static str,
    key: u8,
    value: Binary,
) -> TestResult<(RealTransaction, Vec<u8>)> {
    let transaction = db
        .transaction(Atom::from(source), true, 10_000, 10_000)
        .ok_or_else(|| format!("database rejected transaction {source}"))?;
    transaction
        .upsert(vec![TableKV::new(
            Atom::from(TABLE_NAME),
            encode_u8(key),
            Some(value),
        )])
        .await
        .map_err(|error| format!("upsert in {source} failed: {error:?}"))?;
    let prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("prepare in {source} failed: {error:?}"))?;
    Ok((transaction, prepare))
}

/// 查询一个值并正常完成只读根事务，避免把未 finish 的事务留作隐式测试条件。
async fn query_value(db: &RealDb, source: &'static str, key: u8) -> TestResult<Option<Binary>> {
    let transaction = db
        .transaction(Atom::from(source), false, 5_000, 5_000)
        .ok_or_else(|| format!("database rejected read transaction {source}"))?;
    let mut values = transaction
        .query(vec![TableKV::new(
            Atom::from(TABLE_NAME),
            encode_u8(key),
            None,
        )])
        .await;
    if values.len() != 1 {
        return Err(format!(
            "query in {source} returned {} slots instead of one",
            values.len()
        ));
    }
    let value = values.remove(0);
    let prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("finishing read prepare in {source} failed: {error:?}"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("finishing read commit in {source} failed: {error:?}"))?;
    Ok(value)
}

fn regular_file_sizes(path: &Path) -> TestResult<Vec<(PathBuf, u64)>> {
    let mut files = Vec::new();
    for entry in
        fs::read_dir(path).map_err(|error| format!("reading directory {path:?} failed: {error}"))?
    {
        let entry =
            entry.map_err(|error| format!("reading an entry in {path:?} failed: {error}"))?;
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

fn only_table_log_file(path: &Path) -> TestResult<(PathBuf, u64)> {
    let files = active_file_sizes(path)?;
    if files.len() != 1 {
        return Err(format!(
            "expected exactly one active table log file in {path:?}, observed {files:?}"
        ));
    }
    Ok(files[0].clone())
}

fn root_paths(root: &Path) -> (PathBuf, PathBuf) {
    (root.join("database"), root.join("root-wal"))
}

/// 使用完整内置生产链路验证真实 LogOrdered 数据文件失败、WAL 保留和重启恢复。
///
/// 该测试只在无 `PHASE_ENV` 时充当父进程；子进程通过同一个精确测试入口执行单一阶段。
/// 故障阶段必须证明真实文件写失败且没有成功确认；恢复阶段必须证明原 WAL 能补齐表文件并
/// 最终确认。该测试同时检查 logger 计数、`.bak`、物理文件长度和绕开 WAL 的数据可见性。
#[test]
fn test_log_ordered_persistence_failure_preserves_wal_and_recovers() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV)
                .expect("the child phase must receive PI_DB_CONFIRMATION_REAL_ROOT"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("confirmation real phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root("full-chain");
    fs::create_dir_all(&root).expect("creating the full-chain test directory must succeed");

    let phases = [
        ("fault", Duration::from_secs(150)),
        ("inspect-before", Duration::from_secs(50)),
        ("recover", Duration::from_secs(75)),
        ("inspect-after", Duration::from_secs(50)),
    ];
    for (phase, timeout) in phases {
        if let Err(error) = run_phase_process(&root, phase, timeout) {
            panic!(
                "confirmation production-chain phase failed; temporary evidence is preserved at {:?}: {}",
                root, error
            );
        }
    }

    fs::remove_dir_all(&root).expect("cleaning the full-chain test directory must succeed");
}

fn run_child_phase(phase: &str, root: &Path) -> TestResult<()> {
    match phase {
        "fault" => {
            install_error_capture()?;
            let root = root.to_path_buf();
            run_on_runtime(Duration::from_secs(140), move |rt| async move {
                phase_fault(rt, root).await
            })
        }
        "inspect-before" => {
            let root = root.to_path_buf();
            run_on_runtime(Duration::from_secs(40), move |rt| async move {
                phase_inspect(rt, root, false).await
            })
        }
        "recover" => {
            let root = root.to_path_buf();
            run_on_runtime(Duration::from_secs(65), move |rt| async move {
                phase_recover(rt, root).await
            })
        }
        "inspect-after" => {
            let root = root.to_path_buf();
            run_on_runtime(Duration::from_secs(40), move |rt| async move {
                phase_inspect(rt, root, true).await
            })
        }
        other => Err(format!("unknown child phase {other}")),
    }
}

async fn phase_fault(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let (db_path, wal_path) = root_paths(&root);
    let (db, logger) = build_database(&rt, &db_path, &wal_path).await?;

    create_log_table(&db).await?;
    let ddl_state = wait_for_logger_state(
        &rt,
        &logger,
        DDL_CONFIRM_TIMEOUT,
        "the metadata table to persist and confirm DDL",
        |state| state.waiting_confirm == 0 && state.append_total == state.confirm_total,
    )
    .await?;

    let (baseline_transaction, baseline_prepare) = prepare_upsert(
        &db,
        "confirmation real baseline",
        BASELINE_KEY,
        encode_string(BASELINE_PAYLOAD_LEN, BASELINE_FILL),
    )
    .await?;
    baseline_transaction
        .commit_modified(baseline_prepare)
        .await
        .map_err(|error| format!("committing the baseline transaction failed: {error:?}"))?;
    let baseline_state = wait_for_logger_state(
        &rt,
        &logger,
        TABLE_CONFIRM_TIMEOUT,
        "the oversized baseline table write to confirm",
        |state| {
            state.waiting_confirm == 0
                && state.append_total == ddl_state.append_total + 1
                && state.confirm_total == ddl_state.confirm_total + 1
        },
    )
    .await?;

    let table_path = db
        .table_path(&Atom::from(TABLE_NAME))
        .await
        .ok_or_else(|| format!("{TABLE_NAME} has no physical table path"))?;
    let (table_file, table_len_before) = only_table_log_file(&table_path)?;
    if table_len_before < BASELINE_PAYLOAD_LEN as u64 {
        return Err(format!(
            "baseline table file is unexpectedly short: file={table_file:?}, len={table_len_before}"
        ));
    }

    let active_wal_files = active_file_sizes(&wal_path)?;
    if active_wal_files.len() != 1 || active_wal_files[0].1 != 0 {
        return Err(format!(
            "root WAL must have exactly one empty active checkpoint before fault injection; observed {active_wal_files:?}"
        ));
    }
    let bak_before = nonempty_bak_count(&wal_path)?;

    let (failed_transaction, failed_prepare) = prepare_upsert(
        &db,
        "confirmation real storage failure",
        FAILED_KEY,
        encode_string(FAILED_PAYLOAD_LEN, FAILED_FILL),
    )
    .await?;
    if failed_prepare.len() as u64 + 128 * 1024 >= table_len_before {
        return Err(format!(
            "fault-injection separation is invalid: root WAL payload {} plus safety margin is not smaller than table file limit {table_len_before}",
            failed_prepare.len()
        ));
    }

    // 根 WAL 从长度 0 开始且可以完整写入限制内；表日志从 table_len_before 开始写，
    // 因此 Linux 必须在写入任何字节前以 EFBIG 拒绝。
    let file_limit = FileSizeLimitGuard::install(table_len_before)?;
    failed_transaction
        .commit_modified(failed_prepare)
        .await
        .map_err(|error| {
            format!(
                "root commit failed under separated RLIMIT_FSIZE; the WAL should fit below {table_len_before}: {error:?}"
            )
        })?;

    let storage_errors = wait_for_storage_failure_log(&rt).await?;
    let table_len_after_failure = fs::metadata(&table_file)
        .map_err(|error| format!("reading failed table log metadata failed: {error}"))?
        .len();
    let failed_state = logger_state(&logger).await;
    let bak_after = nonempty_bak_count(&wal_path)?;
    drop(file_limit);

    if table_len_after_failure != table_len_before {
        return Err(format!(
            "RLIMIT_FSIZE did not produce a zero-byte table write: before={table_len_before}, after={table_len_after_failure}, file={table_file:?}"
        ));
    }
    if failed_state.waiting_confirm != 1
        || failed_state.append_total != baseline_state.append_total + 1
        || failed_state.confirm_total != baseline_state.confirm_total
    {
        return Err(format!(
            "root WAL was not preserved after the real table write failure: baseline={baseline_state:?}, after={failed_state:?}"
        ));
    }
    if bak_after != bak_before {
        return Err(format!(
            "the failed transaction changed nonempty .bak count from {bak_before} to {bak_after}; its checkpoint must remain active"
        ));
    }

    let in_memory = query_value(&db, "confirmation in-memory publication check", FAILED_KEY)
        .await?
        .ok_or_else(|| {
            "committed failed-write value is missing from the published in-memory root".to_owned()
        })?;
    validate_string(&in_memory, FAILED_PAYLOAD_LEN, FAILED_FILL)?;

    let active_wal_after = active_file_sizes(&wal_path)?;
    if !active_wal_after.iter().any(|(_, len)| *len > 0) {
        return Err(format!(
            "no nonempty active WAL contains the failed transaction: {active_wal_after:?}"
        ));
    }

    println!(
        "confirmation fault evidence: table={table_file:?}, table_len={table_len_before}, logger={failed_state:?}, bak_count={bak_after}, captured_errors={storage_errors:?}"
    );
    Ok(())
}

/// 使用独立空提交日志启动同一个数据库，只观察真实表数据文件。
async fn phase_inspect(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
    expect_repaired_value: bool,
) -> TestResult<()> {
    let (db_path, _) = root_paths(&root);
    let inspection_wal = if expect_repaired_value {
        root.join("inspection-wal-after")
    } else {
        root.join("inspection-wal-before")
    };
    let (db, logger) = build_database(&rt, &db_path, &inspection_wal).await?;
    let state = logger_state(&logger).await;
    if state
        != (LoggerState {
            waiting_confirm: 0,
            append_total: 0,
            confirm_total: 0,
        })
    {
        return Err(format!(
            "the independent inspection WAL must stay empty, observed {state:?}"
        ));
    }

    let baseline = query_value(&db, "confirmation disk-only baseline query", BASELINE_KEY)
        .await?
        .ok_or_else(|| "baseline value is absent from the real table data file".to_owned())?;
    validate_string(&baseline, BASELINE_PAYLOAD_LEN, BASELINE_FILL)?;

    let failed = query_value(&db, "confirmation disk-only failed query", FAILED_KEY).await?;
    match (expect_repaired_value, failed) {
        (false, None) => {}
        (false, Some(value)) => {
            return Err(format!(
                "failed value unexpectedly exists in table data before WAL repair; encoded len={} ",
                value.len()
            ));
        }
        (true, Some(value)) => validate_string(&value, FAILED_PAYLOAD_LEN, FAILED_FILL)?,
        (true, None) => {
            return Err("failed value is still absent from table data after WAL repair".to_owned());
        }
    }

    println!(
        "confirmation disk-only inspection: expect_repaired={expect_repaired_value}, baseline_present=true, failed_present={expect_repaired_value}"
    );
    Ok(())
}

/// 使用原提交日志正常启动，验证 `try_repair` 最终持久化并确认故障事务。
async fn phase_recover(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let (db_path, wal_path) = root_paths(&root);
    let bak_before = nonempty_bak_count(&wal_path)?;
    let (db, logger) = build_database(&rt, &db_path, &wal_path).await?;

    let recovered_state = wait_for_logger_state(
        &rt,
        &logger,
        TABLE_CONFIRM_TIMEOUT,
        "replayed table data to persist and the original WAL to confirm",
        |state| {
            state.waiting_confirm == 0
                && state.append_total >= 1
                && state.confirm_total >= 1
                && state.append_total == state.confirm_total
        },
    )
    .await?;

    let recovered = query_value(&db, "confirmation recovered in-memory query", FAILED_KEY)
        .await?
        .ok_or_else(|| "try_repair did not restore the failed value in memory".to_owned())?;
    validate_string(&recovered, FAILED_PAYLOAD_LEN, FAILED_FILL)?;

    let bak_after = nonempty_bak_count(&wal_path)?;
    if bak_after <= bak_before {
        return Err(format!(
            "recovery confirmed no additional WAL checkpoint: before={bak_before}, after={bak_after}, state={recovered_state:?}"
        ));
    }
    let active = active_file_sizes(&wal_path)?;
    if active.iter().any(|(_, len)| *len > 0) {
        return Err(format!(
            "nonempty active WAL remains after successful replay confirmation: {active:?}"
        ));
    }

    println!(
        "confirmation recovery evidence: logger={recovered_state:?}, bak_before={bak_before}, bak_after={bak_after}"
    );
    Ok(())
}

fn unique_temp_root(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must be after UNIX_EPOCH")
        .as_nanos();
    env::temp_dir().join(format!(
        "pi_db_confirmation_{label}_{}_{}",
        std::process::id(),
        nanos
    ))
}

fn run_phase_process(root: &Path, phase: &str, timeout: Duration) -> TestResult<()> {
    let executable = env::current_exe()
        .map_err(|error| format!("locating current test executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(FULL_CHAIN_TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase)
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| format!("spawning child phase {phase} failed: {error}"))?;

    let status = wait_for_child(&mut child, timeout)
        .map_err(|error| format!("child phase {phase}: {error}"))?;
    if !status.success() {
        return Err(format!("child phase {phase} exited with {status}"));
    }
    Ok(())
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        match child
            .try_wait()
            .map_err(|error| format!("polling child process failed: {error}"))?
        {
            Some(status) => return Ok(status),
            None if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
            None => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("timed out after {timeout:?} and was terminated"));
            }
        }
    }
}
