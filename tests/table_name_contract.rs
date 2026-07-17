//! 表名 `1..=4096` UTF-8 字节边界、拒绝无副作用和根 WAL 恢复专项。
//!
//! 本 target 不引用或运行旧测试。它通过真实
//! `KVDBManagerBuilder -> KVDBManager -> KVDBTransaction`、多线程 runtime、
//! `Transaction2PcManager`、`CommitLogger`、Meta 表和文件系统验证：
//!
//! - [`pi_db::MAX_TABLE_NAME_BYTES`] 固定为 4096；
//! - 空名称和 4097 字节名称在任何注册表、Meta、根持久化标记或 WAL 副作用前返回
//!   `io::ErrorKind::InvalidInput`；
//! - 内部 `.tables_meta` 名称不能通过公开删表入口移除，并且拒绝路径同样没有事务/WAL副作用；
//! - 4096 字节名称能够完整进入 Meta 预提交输出，提交成功后立即终止进程，下一进程仍能
//!   通过原根 WAL 恢复精确名称；
//! - 长度按 UTF-8 字节而非 Unicode 标量值计数。
//!
//! 测试冻结名称长度契约及 `.tables_meta` 的删除专用例外；它不冻结公开 create 对该名称、
//! 其它内部名称、相对路径、`..` 或平台路径组件的语义，也不把 Memory 表的无数据文件语义
//! 扩展到其它引擎。创建阶段在根 WAL flush 成功后使用 `_exit`，故恢复不能依赖正常析构；
//! 恢复阶段精确比较完整 4096 字节名称，避免只检查表数。
//!
//! 被测入口：`MAX_TABLE_NAME_BYTES`、`KVDBTransaction::{create_table,
//! create_table_with_options,remove_table,prepare_modified,commit_modified}`、
//! `KVDBManagerBuilder::startup`。正式契约入口：
//! `docs/SEMANTIC_CONTRACTS.md#contract-table-name-001`。

#![cfg(target_os = "linux")]

use std::{
    env, fs,
    future::Future,
    io::ErrorKind,
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
    utils::CreateTableOptions,
    KVDBTableType, KVTableMeta, MAX_TABLE_NAME_BYTES,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;

const TEST_NAME: &str = "test_table_name_utf8_byte_limit_and_wal_recovery";
const PHASE_ENV: &str = "PI_DB_TABLE_NAME_PHASE";
const ROOT_ENV: &str = "PI_DB_TABLE_NAME_ROOT";
const META_TABLE_NAME: &str = ".tables_meta";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(60);

/// 4096 字节名称必须可恢复；空名和 4097 字节名称必须无副作用地拒绝。
#[test]
fn test_table_name_utf8_byte_limit_and_wal_recovery() {
    assert_eq!(MAX_TABLE_NAME_BYTES, 4096);

    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV).expect("table-name child phase must receive its root path"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("table-name phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root();
    fs::create_dir_all(&root).expect("creating table-name test root must succeed");
    for phase in ["create-and-crash", "recover"] {
        if let Err(error) = run_phase_process(&root, phase, PROCESS_TIMEOUT) {
            panic!(
                "table-name contract failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root
            );
        }
    }
    fs::remove_dir_all(&root).expect("cleaning table-name test root must succeed");
}

fn run_child_phase(phase: &str, root: &Path) -> TestResult<()> {
    let root = root.to_path_buf();
    match phase {
        "create-and-crash" => run_on_runtime(PROCESS_TIMEOUT, move |rt| async move {
            phase_create_and_crash(rt, root).await
        }),
        "recover" => run_on_runtime(PROCESS_TIMEOUT, move |rt| async move {
            phase_recover(rt, root).await
        }),
        other => Err(format!("unknown table-name phase: {other}")),
    }
}

async fn phase_create_and_crash(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let (db, logger) = build_database(&rt, &root).await?;
    let initial_table_size = db.table_size().await;
    let append_before_invalid = logger.append_total_count();
    let confirm_before_invalid = logger.confirm_total_count();
    if !db.is_exist(&Atom::from(META_TABLE_NAME)).await {
        return Err("internal Meta table is absent before rejection checks".to_owned());
    }

    let invalid_transaction = db
        .transaction(Atom::from("invalid table names"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected invalid-name transaction".to_owned())?;
    assert_invalid_input(
        invalid_transaction
            .create_table(Atom::from(""), memory_meta(), false)
            .await,
        "empty create_table",
    )?;
    assert_invalid_input(
        invalid_transaction
            .create_table_with_options(
                over_limit_name(),
                memory_meta(),
                CreateTableOptions::Empty,
                false,
            )
            .await,
        "4097-byte create_table_with_options",
    )?;
    assert_invalid_input(
        invalid_transaction.remove_table(Atom::from("")).await,
        "empty remove_table",
    )?;
    assert_invalid_input(
        invalid_transaction.remove_table(over_limit_name()).await,
        "4097-byte remove_table",
    )?;
    assert_invalid_input(
        invalid_transaction
            .remove_table(Atom::from(META_TABLE_NAME))
            .await,
        "reserved Meta remove_table",
    )?;

    if db.table_size().await != initial_table_size {
        return Err("invalid names changed the manager table registry".to_owned());
    }
    if !db.is_exist(&Atom::from(META_TABLE_NAME)).await {
        return Err("reserved-name rejection removed the internal Meta table".to_owned());
    }
    if db.is_exist(&over_limit_name()).await {
        return Err("4097-byte table name was registered despite InvalidInput".to_owned());
    }
    let invalid_prepare = invalid_transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing rejected-name transaction failed: {error:?}"))?;
    if !invalid_prepare.is_empty() {
        return Err(format!(
            "rejected names unexpectedly produced {} WAL bytes",
            invalid_prepare.len()
        ));
    }
    invalid_transaction
        .commit_modified(invalid_prepare)
        .await
        .map_err(|error| format!("finishing rejected-name transaction failed: {error:?}"))?;
    if logger.append_total_count() != append_before_invalid
        || logger.confirm_total_count() != confirm_before_invalid
    {
        return Err("rejected names changed root WAL counters".to_owned());
    }

    let valid_name = exact_limit_name();
    if valid_name.as_str().as_bytes().len() != MAX_TABLE_NAME_BYTES {
        return Err("exact-limit fixture is not exactly 4096 UTF-8 bytes".to_owned());
    }
    if valid_name.as_str().chars().count() >= MAX_TABLE_NAME_BYTES {
        return Err(
            "exact-limit fixture does not distinguish UTF-8 bytes from characters".to_owned(),
        );
    }
    let transaction = db
        .transaction(Atom::from("exact table name limit"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected exact-limit transaction".to_owned())?;
    transaction
        .create_table(valid_name.clone(), memory_meta(), false)
        .await
        .map_err(|error| format!("4096-byte table name was rejected: {error}"))?;
    if !db.is_exist(&valid_name).await {
        return Err("4096-byte table was not registered before commit".to_owned());
    }
    let prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing 4096-byte table failed: {error:?}"))?;
    if prepare.len() <= MAX_TABLE_NAME_BYTES {
        return Err(format!(
            "4096-byte table name was not completely represented in WAL output: {} bytes",
            prepare.len()
        ));
    }
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing 4096-byte table failed: {error:?}"))?;
    if logger.append_total_count() != append_before_invalid + 1 {
        return Err("exact-limit DDL did not append exactly one root WAL transaction".to_owned());
    }

    // 根 WAL 已由 commit_modified flush；立即退出，禁止把正常析构当作恢复前置条件。
    unsafe { libc::_exit(0) }
}

async fn phase_recover(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let (db, _) = build_database(&rt, &root).await?;
    let expected = exact_limit_name();
    if !db.is_exist(&expected).await {
        return Err("4096-byte table name was not restored from the flushed root WAL".to_owned());
    }
    let matching: Vec<_> = db
        .tables()
        .await
        .into_iter()
        .filter(|name| name == &expected)
        .collect();
    if matching.len() != 1 || matching[0].as_str().as_bytes().len() != MAX_TABLE_NAME_BYTES {
        return Err("recovered registry did not contain one byte-exact 4096-byte name".to_owned());
    }
    let transaction = db
        .transaction(Atom::from("recovered name metadata"), false, 10_000, 10_000)
        .ok_or_else(|| "database rejected recovered-name query transaction".to_owned())?;
    let meta = transaction
        .table_meta(expected)
        .await
        .ok_or_else(|| "recovered 4096-byte table has no Meta definition".to_owned())?;
    if meta != memory_meta() {
        return Err(format!("recovered table metadata mismatch: {meta:?}"));
    }
    let prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("finishing recovered-name query failed: {error:?}"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing recovered-name query failed: {error:?}"))
}

fn assert_invalid_input(result: std::io::Result<()>, label: &str) -> TestResult<()> {
    match result {
        Err(error) if error.kind() == ErrorKind::InvalidInput => Ok(()),
        Err(error) => Err(format!(
            "{label} returned {:?} instead of InvalidInput: {error}",
            error.kind()
        )),
        Ok(()) => Err(format!("{label} unexpectedly succeeded")),
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

fn exact_limit_name() -> Atom {
    // `界` 是 3 个 UTF-8 字节，使字符数量严格小于字节数量。
    let mut name = String::with_capacity(MAX_TABLE_NAME_BYTES);
    name.push('界');
    name.extend(std::iter::repeat('v').take(MAX_TABLE_NAME_BYTES - '界'.len_utf8()));
    Atom::from(name)
}

fn over_limit_name() -> Atom {
    let mut name = String::with_capacity(MAX_TABLE_NAME_BYTES + 1);
    name.push('界');
    name.extend(std::iter::repeat('x').take(MAX_TABLE_NAME_BYTES + 1 - '界'.len_utf8()));
    Atom::from(name)
}

async fn build_database(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
) -> TestResult<(RealDb, CommitLogger)> {
    let wal_path = root.join("root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .build()
        .await
        .map_err(|error| format!("building CommitLogger at {wal_path:?} failed: {error}"))?;
    let manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger.clone(),
    );
    let db = KVDBManagerBuilder::new(rt.clone(), manager, root.join("database"))
        .startup(false)
        .await
        .map_err(|error| format!("starting table-name database failed: {error}"))?;
    Ok((db, logger))
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
    .map_err(|error| format!("spawning table-name future failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("table-name future exceeded {timeout:?}: {error}"))?
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
        .map_err(|error| format!("spawning table-name phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("table-name phase {phase} exited with {status}"))
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
            return Err(format!("table-name child exceeded {timeout:?}"));
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
        "pi_db_table_name_contract_{}_{}",
        std::process::id(),
        nanos
    ))
}
