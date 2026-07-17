//! 内部 Meta 表公开删表拒绝与非法删表 WAL 恢复专项。
//!
//! 本 target 不引用或运行旧测试。它使用真实 `MultiTaskRuntime`、公开
//! `KVDBManagerBuilder -> KVDBManager -> KVDBTransaction`、真实
//! `Transaction2PcManager`、`CommitLogger`、Meta 表和文件系统，验证两条删除专用边界：
//!
//! - 公开 `remove_table(".tables_meta")` 必须在根持久化标记、注册表、Meta 动作和根 WAL
//!   之前返回 `InvalidInput`；内部 Meta 表仍存在，prepare 为空，logger 计数完全不变；
//! - 已落盘根 WAL 若包含删除 `.tables_meta` 的 Meta tombstone，真实 startup/try_repair 必须
//!   返回 `InvalidData`，不能移除内部表、静默忽略、panic 或把错误降级为 `Other`；
//! - Meta tombstone Key 的 BON Atom 类型标记损坏时，真实 startup/try_repair 同样必须返回
//!   `InvalidData`，并且失败恢复不能污染独立空 WAL 下的数据文件启动状态。
//!
//! 恢复夹具不是 mock：第一进程先调用公开 `remove_table` 为同长度普通名称生成真实根 prepare
//! payload，只把该 payload 中唯一的普通名称 UTF-8 字节等长替换为 `.tables_meta`，保留真实
//! 事务 UID、Meta 表段、动作数量、Key BON 长度和 tombstone 编码，再通过真实
//! `CommitLogger::append/flush` 落盘。第二进程使用同一 WAL 和数据库目录执行正常 startup。
//! 第三进程使用独立空 WAL 检查数据库仍可正常启动且注册表只有内部 Meta 表。后三个进程在
//! 独立数据库目录重复真实 payload 生成，只破坏 Key 内部 Atom 类型标记并保持外层段长度、
//! Key 长度和 tombstone 编码不变，然后分别验证拒绝恢复和空 WAL 数据状态。
//!
//! 该测试只冻结删表边界，不修改或断言公开建表对保留名称的语义，也不声称 DDL rollback、
//! future 取消、物理文件删除或 collector 释放已经具备完整原子性。正式契约见
//! `docs/SEMANTIC_CONTRACTS.md#contract-ddl-remove-crash-durability`，修复归档见
//! `docs/DDL_REMOVE_DURABILITY_FIX.md#ddl-remove-fix-index`。

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
use pi_db::db::{KVDBManager, KVDBManagerBuilder};
use pi_guid::{Guid, GuidGen};
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;

const TEST_NAME: &str = "test_reserved_meta_remove_and_recovery_are_rejected";
const PHASE_ENV: &str = "PI_DB_RESERVED_META_PHASE";
const ROOT_ENV: &str = "PI_DB_RESERVED_META_ROOT";
const META_TABLE_NAME: &str = ".tables_meta";
const SURROGATE_TABLE_NAME: &str = "user_table_x";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(90);

/// 公开删除、保留目标 WAL 和畸形 Key WAL 必须按各自契约无副作用拒绝。
#[test]
fn test_reserved_meta_remove_and_recovery_are_rejected() {
    assert_eq!(
        META_TABLE_NAME.len(),
        SURROGATE_TABLE_NAME.len(),
        "WAL mutation fixture must preserve the encoded Atom byte length"
    );

    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV).expect("reserved-Meta child phase must receive its root path"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("reserved-Meta phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root();
    fs::create_dir_all(&root).expect("creating reserved-Meta test root must succeed");
    for phase in [
        "reject-and-write-invalid-wal",
        "reject-invalid-recovery",
        "inspect-data-only",
        "write-malformed-key-wal",
        "reject-malformed-key-recovery",
        "inspect-malformed-data-only",
    ] {
        if let Err(error) = run_phase_process(&root, phase, PROCESS_TIMEOUT) {
            panic!(
                "reserved-Meta safety failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root
            );
        }
    }
    fs::remove_dir_all(&root).expect("cleaning reserved-Meta test root must succeed");
}

/// 将隔离进程阶段投递到真实多线程 runtime，并给整个阶段设置诊断截止。
fn run_child_phase(phase: &str, root: &Path) -> TestResult<()> {
    let root = root.to_path_buf();
    match phase {
        "reject-and-write-invalid-wal" => run_on_runtime(PROCESS_TIMEOUT, move |rt| async move {
            phase_reject_and_write_invalid_wal(rt, root).await
        }),
        "reject-invalid-recovery" => run_on_runtime(PROCESS_TIMEOUT, move |rt| async move {
            phase_reject_invalid_recovery(rt, root).await
        }),
        "inspect-data-only" => run_on_runtime(PROCESS_TIMEOUT, move |rt| async move {
            phase_inspect_data_only(rt, root).await
        }),
        "write-malformed-key-wal" => run_on_runtime(PROCESS_TIMEOUT, move |rt| async move {
            phase_write_malformed_key_wal(rt, root).await
        }),
        "reject-malformed-key-recovery" => run_on_runtime(PROCESS_TIMEOUT, move |rt| async move {
            phase_reject_malformed_key_recovery(rt, root).await
        }),
        "inspect-malformed-data-only" => run_on_runtime(PROCESS_TIMEOUT, move |rt| async move {
            phase_inspect_malformed_data_only(rt, root).await
        }),
        other => Err(format!("unknown reserved-Meta phase: {other}")),
    }
}

/// 验证公开拒绝无副作用，再落地一条真实格式但非法目标的删除 WAL。
async fn phase_reject_and_write_invalid_wal(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
) -> TestResult<()> {
    let (db, logger) = build_database(&rt, &root.join("root-wal"), &root.join("database")).await?;
    let meta_name = Atom::from(META_TABLE_NAME);
    if db.table_size().await != 1 || !db.is_exist(&meta_name).await {
        return Err(format!(
            "fresh database must contain only Meta; size: {}",
            db.table_size().await
        ));
    }

    let append_before = logger.append_total_count();
    let confirm_before = logger.confirm_total_count();
    let transaction = db
        .transaction(
            Atom::from("reject reserved Meta removal"),
            true,
            10_000,
            10_000,
        )
        .ok_or_else(|| "database rejected reserved-name test transaction".to_owned())?;
    match transaction.remove_table(meta_name.clone()).await {
        Err(error) if error.kind() == ErrorKind::InvalidInput => {}
        Err(error) => {
            return Err(format!(
                "reserved Meta removal returned {:?} instead of InvalidInput: {error}",
                error.kind()
            ));
        }
        Ok(()) => return Err("reserved Meta removal unexpectedly succeeded".to_owned()),
    }
    if db.table_size().await != 1 || !db.is_exist(&meta_name).await {
        return Err("reserved Meta rejection changed the live registry".to_owned());
    }
    let rejected_prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing rejected Meta removal failed: {error:?}"))?;
    if !rejected_prepare.is_empty() {
        return Err(format!(
            "reserved Meta rejection produced {} root WAL bytes",
            rejected_prepare.len()
        ));
    }
    transaction
        .commit_modified(rejected_prepare)
        .await
        .map_err(|error| format!("finishing rejected Meta removal failed: {error:?}"))?;
    if logger.append_total_count() != append_before
        || logger.confirm_total_count() != confirm_before
        || logger.waiting_confirm_count().await != 0
    {
        return Err(format!(
            "reserved Meta rejection changed logger state: append={}, confirm={}, waiting={}",
            logger.append_total_count(),
            logger.confirm_total_count(),
            logger.waiting_confirm_count().await
        ));
    }

    // 通过受支持的普通删表入口生成真实 Meta tombstone payload；该普通表可以不存在。
    let wal_transaction = db
        .transaction(Atom::from("build invalid Meta WAL"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected invalid-WAL fixture transaction".to_owned())?;
    wal_transaction
        .remove_table(Atom::from(SURROGATE_TABLE_NAME))
        .await
        .map_err(|error| format!("building surrogate removal failed: {error}"))?;
    let mut payload = wal_transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing surrogate removal failed: {error:?}"))?;
    if payload.len() <= 16 {
        return Err(format!(
            "surrogate removal did not produce a Meta WAL segment: {} bytes",
            payload.len()
        ));
    }
    replace_unique_equal_length_name(
        &mut payload,
        SURROGATE_TABLE_NAME.as_bytes(),
        META_TABLE_NAME.as_bytes(),
    )?;

    let invalid_commit_uid = Guid(0x4444_4c52_4553_4552_5645_444d_4554_4101);
    let handle = logger
        .append(invalid_commit_uid, payload)
        .await
        .map_err(|error| format!("appending invalid Meta-removal WAL failed: {error}"))?;
    logger
        .flush(handle)
        .await
        .map_err(|error| format!("flushing invalid Meta-removal WAL failed: {error}"))?;
    if logger.append_total_count() != append_before + 1
        || logger.confirm_total_count() != confirm_before
        || logger.waiting_confirm_count().await != 1
    {
        return Err(format!(
            "invalid WAL was not left as one flushed unconfirmed transaction: append={}, confirm={}, waiting={}",
            logger.append_total_count(),
            logger.confirm_total_count(),
            logger.waiting_confirm_count().await
        ));
    }

    // 根 WAL 已真实 flush；立即退出，确保下一阶段只通过生产 startup/replay 消费它。
    unsafe { libc::_exit(0) }
}

/// 使用正常 startup 消费原 WAL，并严格检查 InvalidData 及可诊断原因。
async fn phase_reject_invalid_recovery(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let wal_path = root.join("root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .build()
        .await
        .map_err(|error| format!("reopening invalid WAL at {wal_path:?} failed: {error}"))?;
    let manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger,
    );
    match KVDBManagerBuilder::new(rt, manager, root.join("database"))
        .startup(false)
        .await
    {
        Err(error) if error.kind() == ErrorKind::InvalidData => {
            let message = error.to_string();
            if !message.contains("reserved internal meta table cannot be removed")
                || !message.contains("Repair tables meta failed")
            {
                return Err(format!(
                    "InvalidData lacked reserved-Meta recovery context: {message}"
                ));
            }
        }
        Err(error) => {
            return Err(format!(
                "invalid Meta-removal WAL returned {:?} instead of InvalidData: {error}",
                error.kind()
            ));
        }
        Ok(_) => {
            return Err("startup accepted a WAL that deletes the internal Meta table".to_owned())
        }
    }

    // 不运行析构或后台清理来掩盖错误状态；下一阶段使用空 WAL 检查数据目录。
    unsafe { libc::_exit(0) }
}

/// 使用独立空 WAL 验证失败恢复没有破坏数据库的最小可启动状态。
async fn phase_inspect_data_only(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let (db, logger) = build_database(
        &rt,
        &root.join("inspection-empty-wal"),
        &root.join("database"),
    )
    .await?;
    let meta_name = Atom::from(META_TABLE_NAME);
    if db.table_size().await != 1 || !db.is_exist(&meta_name).await {
        return Err(format!(
            "data-only startup lost the internal Meta table; size: {}",
            db.table_size().await
        ));
    }
    if logger.append_total_count() != 0
        || logger.confirm_total_count() != 0
        || logger.waiting_confirm_count().await != 0
    {
        return Err("data-only inspection unexpectedly used its empty WAL".to_owned());
    }
    Ok(())
}

/// 由公开删表生成真实 payload，只破坏 Meta tombstone Key 内部的 BON Atom 类型标记。
async fn phase_write_malformed_key_wal(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let (db, logger) = build_database(
        &rt,
        &root.join("malformed-root-wal"),
        &root.join("malformed-database"),
    )
    .await?;
    let append_before = logger.append_total_count();
    let confirm_before = logger.confirm_total_count();
    let transaction = db
        .transaction(
            Atom::from("build malformed remove WAL"),
            true,
            10_000,
            10_000,
        )
        .ok_or_else(|| "database rejected malformed-WAL fixture transaction".to_owned())?;
    transaction
        .remove_table(Atom::from(SURROGATE_TABLE_NAME))
        .await
        .map_err(|error| format!("building malformed surrogate removal failed: {error}"))?;
    let mut payload = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing malformed surrogate removal failed: {error:?}"))?;
    if payload.len() <= 16 {
        return Err(format!(
            "malformed surrogate removal did not produce a Meta WAL segment: {} bytes",
            payload.len()
        ));
    }
    corrupt_unique_atom_type_tag(&mut payload, SURROGATE_TABLE_NAME.as_bytes())?;

    let malformed_commit_uid = Guid(0x4444_4c52_4d41_4c46_4f52_4d45_444b_4559);
    let handle = logger
        .append(malformed_commit_uid, payload)
        .await
        .map_err(|error| format!("appending malformed remove WAL failed: {error}"))?;
    logger
        .flush(handle)
        .await
        .map_err(|error| format!("flushing malformed remove WAL failed: {error}"))?;
    if logger.append_total_count() != append_before + 1
        || logger.confirm_total_count() != confirm_before
        || logger.waiting_confirm_count().await != 1
    {
        return Err(format!(
            "malformed WAL was not left as one flushed unconfirmed transaction: append={}, confirm={}, waiting={}",
            logger.append_total_count(),
            logger.confirm_total_count(),
            logger.waiting_confirm_count().await
        ));
    }

    unsafe { libc::_exit(0) }
}

/// 正常 startup 必须把无法解码的 Meta tombstone Key 分类为持久化数据损坏。
async fn phase_reject_malformed_key_recovery(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
) -> TestResult<()> {
    let wal_path = root.join("malformed-root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .build()
        .await
        .map_err(|error| format!("reopening malformed WAL at {wal_path:?} failed: {error}"))?;
    let manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger,
    );
    match KVDBManagerBuilder::new(rt, manager, root.join("malformed-database"))
        .startup(false)
        .await
    {
        Err(error) if error.kind() == ErrorKind::InvalidData => {
            let message = error.to_string();
            if !message.contains("decode table name failed")
                || !message.contains("Repair removed table failed")
            {
                return Err(format!(
                    "malformed-Key InvalidData lacked decoder context: {message}"
                ));
            }
        }
        Err(error) => {
            return Err(format!(
                "malformed Meta tombstone returned {:?} instead of InvalidData: {error}",
                error.kind()
            ));
        }
        Ok(_) => return Err("startup accepted a malformed Meta tombstone Key".to_owned()),
    }

    unsafe { libc::_exit(0) }
}

/// 空 WAL 启动证明畸形 Key 恢复失败没有修改独立数据目录中的内部 Meta 基线。
async fn phase_inspect_malformed_data_only(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
) -> TestResult<()> {
    let (db, logger) = build_database(
        &rt,
        &root.join("malformed-inspection-empty-wal"),
        &root.join("malformed-database"),
    )
    .await?;
    let meta_name = Atom::from(META_TABLE_NAME);
    if db.table_size().await != 1 || !db.is_exist(&meta_name).await {
        return Err(format!(
            "malformed-Key failure changed the data-only Meta baseline; size: {}",
            db.table_size().await
        ));
    }
    if logger.append_total_count() != 0
        || logger.confirm_total_count() != 0
        || logger.waiting_confirm_count().await != 0
    {
        return Err("malformed data-only inspection unexpectedly used its empty WAL".to_owned());
    }
    Ok(())
}

/// 只允许一次等长替换，确保真实 WAL 的所有长度和段边界保持不变。
fn replace_unique_equal_length_name(
    payload: &mut [u8],
    source: &[u8],
    replacement: &[u8],
) -> TestResult<()> {
    if source.len() != replacement.len() || source.is_empty() {
        return Err("WAL name replacement must be nonempty and equal-length".to_owned());
    }
    let positions: Vec<_> = payload
        .windows(source.len())
        .enumerate()
        .filter_map(|(index, window)| (window == source).then_some(index))
        .collect();
    if positions.len() != 1 {
        return Err(format!(
            "surrogate table name occurred {} times in real WAL instead of exactly once",
            positions.len()
        ));
    }
    let start = positions[0];
    payload[start..start + replacement.len()].copy_from_slice(replacement);
    if payload.windows(source.len()).any(|window| window == source) {
        return Err("surrogate table name remained after WAL mutation".to_owned());
    }
    Ok(())
}

/// 破坏 Key 内部的短字符串类型标记，不改变外层 Binary 长度或后续 payload 边界。
fn corrupt_unique_atom_type_tag(payload: &mut [u8], encoded_name: &[u8]) -> TestResult<()> {
    if encoded_name.is_empty() || encoded_name.len() > 64 {
        return Err("malformed Atom fixture requires a 1..=64 byte name".to_owned());
    }
    let positions: Vec<_> = payload
        .windows(encoded_name.len())
        .enumerate()
        .filter_map(|(index, window)| (window == encoded_name).then_some(index))
        .collect();
    if positions.len() != 1 || positions[0] == 0 {
        return Err(format!(
            "encoded malformed fixture name occurred at invalid positions: {positions:?}"
        ));
    }
    let tag_index = positions[0] - 1;
    let expected_tag = 42 + encoded_name.len() as u8;
    if payload[tag_index] != expected_tag {
        return Err(format!(
            "Atom tag before fixture name was {}, expected {}",
            payload[tag_index], expected_tag
        ));
    }
    payload[tag_index] = 0;
    if payload[tag_index] == expected_tag {
        return Err("malformed Atom tag mutation did not take effect".to_owned());
    }
    Ok(())
}

/// 构建真实数据库、事务管理器和 root CommitLogger。
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

/// 在真实 4 worker runtime 中运行一个阶段，并通过外部同步通道施加硬截止。
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
    .map_err(|error| format!("spawning reserved-Meta future failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("reserved-Meta future exceeded {timeout:?}: {error}"))?
}

/// 启动当前测试二进制的单一精确用例，并等待指定隔离阶段退出。
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
        .map_err(|error| format!("spawning reserved-Meta phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("phase {phase} exited with {status}"))
    }
}

/// 轮询子进程状态；超时后终止并回收，避免挂起污染后续回归。
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

/// 为本次父进程创建唯一临时根目录，失败时保留路径用于取证。
fn unique_temp_root() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must not precede UNIX_EPOCH")
        .as_nanos();
    env::temp_dir().join(format!(
        "pi_db_reserved_meta_safety_{}_{}",
        std::process::id(),
        nanos
    ))
}
