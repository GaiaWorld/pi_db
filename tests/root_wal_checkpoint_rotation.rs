//! 根 WAL `append -> flush` 与 checkpoint 并发轮换的真实红线专项。
//!
//! 本 target 使用三层互补证据：
//!
//! - 真实 `CommitLogger/LogFile` 按确定顺序构造 T1 尚未 flush 时轮换，再让 T2 先确认；独立
//!   进程重开 logger 后必须仍能 replay T1/T2；
//! - 真实 `KVDBManager/KVDBTransaction` 在根 WAL append 计数门控后调用公开
//!   `append_new_commit_log`，事务登记 checkpoint 对应文件必须实际包含该事务 WAL。
//! - 真实 Btree 数据库构造同一窗口，先证明待恢复值尚未进入数据文件，再通过生产
//!   `try_repair` 恢复；确认闭环后移走全部根 WAL，连续两次冷启动仍必须得到精确最终值。
//!
//! 修复前第三层会稳定丢失待恢复值；修复验收后完整 target 进入永久新回归入口。红线和最终
//! E5 口径分别见 `docs/ROOT_WAL_CHECKPOINT_ROTATION_BUG.md#bug-root-wal-checkpoint-redline`
//! 与 `#bug-root-wal-checkpoint-acceptance`。

use std::{
    env,
    fs,
    future::Future,
    io::{Error as IoError, ErrorKind, Result as IoResult},
    panic,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
        Mutex,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_channel::{
    bounded as async_bounded,
    Receiver as AsyncReceiver,
    Sender as AsyncSender,
};
use bytes::BufMut;
use crossbeam_channel::bounded;
use futures::{future::BoxFuture, FutureExt};
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::{
    manager_2pc::Transaction2PcManager, AsyncCommitLog, Transaction2Pc,
};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    utils::CreateTableOptions,
    Binary,
    KVDBTableType,
    KVTableMeta,
};
use pi_guid::{Guid, GuidGen};
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type GatedDb = KVDBManager<usize, GatedCommitLogger>;

const REPLAY_TEST_NAME: &str =
    "test_commit_logger_rotation_keeps_unconfirmed_wal_replayable";
const REPAIR_TEST_NAME: &str =
    "test_checkpoint_rotation_preserves_try_repair_final_data";
const PHASE_ENV: &str = "PI_DB_ROOT_WAL_ROTATION_PHASE";
const ROOT_ENV: &str = "PI_DB_ROOT_WAL_ROTATION_ROOT";
const MANIFEST_FILE: &str = "checkpoint-manifest";
const REPAIR_MANIFEST_FILE: &str = "repair-checkpoint-manifest";
const ARCHIVED_WAL_DIR: &str = "confirmed-root-wal";
const PRE_REPAIR_COPY_DIR: &str = "pre-repair-data-copy";
const MEMORY_TABLE: &str = "root_wal_rotation_memory";
const REPAIR_PENDING_TABLE: &str = "root_wal_rotation_pending";
const REPAIR_CONFIRMING_TABLE: &str = "root_wal_rotation_confirming";
const T1: Guid = Guid(0x1010_1010_1010_1010_1010_1010_1010_1010);
const T2: Guid = Guid(0x2020_2020_2020_2020_2020_2020_2020_2020);
const T1_PAYLOAD_BYTE: u8 = 0x11;
const T2_PAYLOAD_BYTE: u8 = 0x22;
const DIRECT_PAYLOAD_LEN: usize = 1024;
const REPAIR_PENDING_KEY: usize = 71;
const REPAIR_CONFIRMING_KEY: usize = 72;
const REPAIR_TRIGGER_KEY: usize = 73;
const REPAIR_PENDING_VALUE_BYTE: u8 = 0x71;
const REPAIR_CONFIRMING_VALUE_BYTE: u8 = 0x72;
const REPAIR_TRIGGER_VALUE_BYTE: u8 = 0x73;
const REPAIR_PENDING_VALUE_LEN: usize = 1024 * 1024 - 64;
const REPAIR_CONFIRMING_VALUE_LEN: usize = 1024 * 1024;
const REPAIR_TRIGGER_VALUE_LEN: usize = 128;
const PROCESS_TIMEOUT: Duration = Duration::from_secs(90);
const REPAIR_PROCESS_TIMEOUT: Duration = Duration::from_secs(150);
const RUNTIME_TIMEOUT: Duration = Duration::from_secs(30);
const REPAIR_RUNTIME_TIMEOUT: Duration = Duration::from_secs(135);
const OBSERVATION_TIMEOUT: Duration = Duration::from_secs(10);
const DDL_CONFIRM_TIMEOUT: Duration = Duration::from_secs(75);

/// 只在本专项的 T1 根 WAL flush 处建立确定性调度门；所有 WAL 字节、checkpoint、replay 和
/// confirm 仍由真实 `CommitLogger` 完成。
#[derive(Clone)]
struct GatedCommitLogger {
    inner: CommitLogger,
    gate: Arc<FlushGate>,
}

struct FlushGate {
    armed: AtomicBool,
    pending_uid: Mutex<Option<Guid>>,
    entered_tx: AsyncSender<(Guid, usize)>,
    release_rx: AsyncReceiver<()>,
}

struct FlushGateControl {
    gate: Arc<FlushGate>,
    entered_rx: AsyncReceiver<(Guid, usize)>,
    release_tx: AsyncSender<()>,
}

impl FlushGateControl {
    fn arm(&self) -> TestResult<()> {
        let pending = self
            .gate
            .pending_uid
            .lock()
            .map_err(|_| "flush gate pending UID mutex was poisoned".to_owned())?;
        if pending.is_some() {
            return Err("flush gate retained a pending UID before arm".to_owned());
        }
        if self
            .gate
            .armed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err("flush gate was already armed".to_owned());
        }
        drop(pending);
        Ok(())
    }

    async fn wait_until_entered(&self) -> TestResult<(Guid, usize)> {
        self
            .entered_rx
            .recv()
            .await
            .map_err(|error| format!("receiving gated flush observation failed: {error}"))
    }

    async fn release(&self) -> TestResult<()> {
        self
            .release_tx
            .send(())
            .await
            .map_err(|error| format!("releasing gated flush failed: {error}"))
    }
}

fn gated_commit_logger(inner: CommitLogger) -> (GatedCommitLogger, FlushGateControl) {
    let (entered_tx, entered_rx) = async_bounded(1);
    let (release_tx, release_rx) = async_bounded(1);
    let gate = Arc::new(FlushGate {
        armed: AtomicBool::new(false),
        pending_uid: Mutex::new(None),
        entered_tx,
        release_rx,
    });
    (
        GatedCommitLogger {
            inner,
            gate: gate.clone(),
        },
        FlushGateControl {
            gate,
            entered_rx,
            release_tx,
        },
    )
}

impl AsyncCommitLog for GatedCommitLogger {
    type C = usize;
    type Cid = Guid;

    fn append<B>(&self, commit_uid: Self::Cid, log: B) -> BoxFuture<'static, IoResult<Self::C>>
    where
        B: BufMut + AsRef<[u8]> + Send + Sized + 'static,
    {
        let logger = self.clone();
        async move {
            let handle = logger.inner.append(commit_uid.clone(), log).await?;
            if logger.gate.armed.load(Ordering::Acquire) {
                let mut pending = logger
                    .gate
                    .pending_uid
                    .lock()
                    .map_err(|_| IoError::new(ErrorKind::Other, "flush gate pending UID mutex was poisoned"))?;
                if pending.is_some() {
                    return Err(IoError::new(
                        ErrorKind::Other,
                        "flush gate observed more than one append while armed",
                    ));
                }
                *pending = Some(commit_uid);
            }
            Ok(handle)
        }
        .boxed()
    }

    fn flush(&self, log_handle: Self::C) -> BoxFuture<'static, IoResult<()>> {
        let logger = self.clone();
        async move {
            if logger
                .gate
                .armed
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let commit_uid = logger
                    .gate
                    .pending_uid
                    .lock()
                    .map_err(|_| IoError::new(ErrorKind::Other, "flush gate pending UID mutex was poisoned"))?
                    .take()
                    .ok_or_else(|| {
                        IoError::new(
                            ErrorKind::Other,
                            "flush gate entered without a preceding nonempty append",
                        )
                    })?;
                logger
                    .gate
                    .entered_tx
                    .send((commit_uid, log_handle))
                    .await
                    .map_err(|error| {
                        IoError::new(
                            ErrorKind::BrokenPipe,
                            format!("sending gated flush observation failed: {error}"),
                        )
                    })?;
                logger.gate.release_rx.recv().await.map_err(|error| {
                    IoError::new(
                        ErrorKind::BrokenPipe,
                        format!("waiting for gated flush release failed: {error}"),
                    )
                })?;
            }
            logger.inner.flush(log_handle).await
        }
        .boxed()
    }

    fn confirm(&self, commit_uid: Self::Cid) -> BoxFuture<'static, IoResult<()>> {
        self.inner.confirm(commit_uid)
    }

    fn start_replay<B, F>(
        &self,
        callback: Arc<F>,
    ) -> BoxFuture<'static, IoResult<(usize, usize)>>
    where
        B: BufMut + AsRef<[u8]> + From<Vec<u8>> + Send + Sized + 'static,
        F: Fn(Self::Cid, B) -> IoResult<()> + Send + Sync + 'static,
    {
        self.inner.start_replay(callback)
    }

    fn append_replay<B>(
        &self,
        commit_uid: Self::Cid,
        log: B,
    ) -> BoxFuture<'static, IoResult<Self::C>>
    where
        B: BufMut + AsRef<[u8]> + Send + Sized + 'static,
    {
        self.inner.append_replay(commit_uid, log)
    }

    fn flush_replay(&self, log_handle: Self::C) -> BoxFuture<'static, IoResult<()>> {
        self.inner.flush_replay(log_handle)
    }

    fn confirm_replay(&self, commit_uid: Self::Cid) -> BoxFuture<'static, IoResult<()>> {
        self.inner.confirm_replay(commit_uid)
    }

    fn finish_replay(&self) -> BoxFuture<'static, IoResult<()>> {
        self.inner.finish_replay()
    }

    fn check_point_of(&self, commit_uid: Self::Cid) -> BoxFuture<'static, Option<usize>> {
        self.inner.check_point_of(commit_uid)
    }

    fn current_check_point(&self) -> BoxFuture<'static, usize> {
        self.inner.current_check_point()
    }

    fn append_check_point(&self) -> BoxFuture<'static, IoResult<usize>> {
        self.inner.append_check_point()
    }

    fn waiting_confirm_count(&self) -> BoxFuture<'static, usize> {
        self.inner.waiting_confirm_count()
    }

    fn append_total_count(&self) -> usize {
        self.inner.append_total_count()
    }

    fn confirm_total_count(&self) -> usize {
        self.inner.confirm_total_count()
    }
}

/// 确定性红线：未确认 T1 的实际 WAL 不得因错配 checkpoint 进入 `.bak` 并被 replay 忽略。
#[test]
fn test_commit_logger_rotation_keeps_unconfirmed_wal_replayable() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        install_abort_on_any_panic();
        let root = PathBuf::from(
            env::var_os(ROOT_ENV)
                .expect("root WAL rotation child phase must receive its root path"),
        );
        run_replay_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("root WAL rotation phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root("replay-redline");
    fs::create_dir_all(&root).expect("creating root WAL replay redline root must succeed");
    for phase in ["setup-unconfirmed", "reopen-replay"] {
        if let Err(error) = run_phase_process(&root, phase, PROCESS_TIMEOUT) {
            panic!(
                "root WAL checkpoint replay redline failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root,
            );
        }
    }
    fs::remove_dir_all(&root).expect("cleaning root WAL replay redline root must succeed");
}

/// 生产可达性红线：公开轮换不能让登记 checkpoint 保持为空而把事务 WAL 写入新文件。
#[test]
fn test_manager_rotation_writes_transaction_wal_to_registered_checkpoint() {
    let root = unique_temp_root("manager-redline");
    fs::create_dir_all(&root).expect("creating manager WAL rotation root must succeed");
    let root_for_test = root.clone();

    let result = run_on_runtime(RUNTIME_TIMEOUT, move |rt| async move {
        verify_manager_rotation_production_path(rt, root_for_test).await
    });
    if let Err(error) = result {
        panic!(
            "manager checkpoint rotation redline failed; evidence is preserved at {:?}: {error}",
            root,
        );
    }
    fs::remove_dir_all(&root).expect("cleaning manager WAL rotation root must succeed");
}

/// 最终数据红线：竞争窗口后的未确认写必须由生产 `try_repair` 恢复并最终进入数据文件。
#[test]
fn test_checkpoint_rotation_preserves_try_repair_final_data() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        install_abort_on_any_panic();
        let root = PathBuf::from(
            env::var_os(ROOT_ENV)
                .expect("checkpoint repair child phase must receive its root path"),
        );
        run_repair_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("checkpoint repair phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root("try-repair-final-data");
    fs::create_dir_all(&root).expect("creating checkpoint repair root must succeed");
    for phase in [
        "repair-setup",
        "repair-pre-repair-data",
        "repair-recover",
        "repair-data-only",
        "repair-data-only-again",
    ] {
        if let Err(error) =
            run_repair_phase_process(&root, phase, REPAIR_PROCESS_TIMEOUT)
        {
            panic!(
                "checkpoint repair final-data redline failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root,
            );
        }
    }
    fs::remove_dir_all(&root).expect("cleaning checkpoint repair root must succeed");
}

/// runtime worker panic 默认不会使测试主线程失败；所有 child phase 必须将任意线程 panic
/// 转换成非零进程退出，避免后台 I/O/collector 异常被阶段业务返回值遮蔽。
fn install_abort_on_any_panic() {
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        default_hook(info);
        std::process::abort();
    }));
}

fn run_repair_child_phase(phase: &str, root: &Path) -> TestResult<()> {
    match phase {
        "repair-setup" => {
            let root = root.to_path_buf();
            run_on_runtime(REPAIR_RUNTIME_TIMEOUT, move |rt| async move {
                phase_repair_setup(rt, root).await
            })
        },
        "repair-pre-repair-data" => {
            let root = root.to_path_buf();
            run_on_runtime(REPAIR_RUNTIME_TIMEOUT, move |rt| async move {
                phase_pre_repair_data(rt, root).await
            })
        },
        "repair-recover" => {
            let root = root.to_path_buf();
            run_on_runtime(REPAIR_RUNTIME_TIMEOUT, move |rt| async move {
                phase_repair_recover(rt, root).await
            })
        },
        "repair-data-only" => {
            let root = root.to_path_buf();
            run_on_runtime(REPAIR_RUNTIME_TIMEOUT, move |rt| async move {
                phase_repair_data_only(rt, root, true).await
            })
        },
        "repair-data-only-again" => {
            let root = root.to_path_buf();
            run_on_runtime(REPAIR_RUNTIME_TIMEOUT, move |rt| async move {
                phase_repair_data_only(rt, root, false).await
            })
        },
        other => Err(format!("unknown checkpoint repair phase: {other}")),
    }
}

/// 先让 DDL 独立完成 Meta 持久化，再建立 T1 append 完成但 flush 被门控的唯一竞争窗口。
///
/// DDL 必须先确认，否则它会作为更早的未确认 checkpoint 阻止后续 `.bak` 推进，从而遮蔽
/// 本 Bug。Meta 的生产 collector 周期固定为 60 秒，本阶段真实等待该路径，不能用表维护
/// API 代替，因为 `ready_collect_table/collect_table` 不消费待确认事务队列。
async fn phase_repair_setup(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
) -> TestResult<()> {
    let wal_path = root.join("root-wal");
    let (db, tr_manager, logger, gate) = build_gated_database(&rt, &root).await?;
    let ddl_commit_uid = create_repair_tables(&db).await?;
    wait_for_commit_confirmation(
        &rt,
        &logger.inner,
        ddl_commit_uid,
        DDL_CONFIRM_TIMEOUT,
        "repair schema DDL confirmation",
    )
    .await?;
    let (ddl_appended, ddl_confirmed, ddl_waiting) =
        stable_logger_accounting(&rt, &logger.inner, OBSERVATION_TIMEOUT).await?;
    expect_eq("repair DDL append total", &ddl_appended, &1usize)?;
    expect_eq("repair DDL confirm total", &ddl_confirmed, &1usize)?;
    expect_eq("repair DDL waiting total", &ddl_waiting, &0usize)?;
    if nonempty_bak_count(&wal_path)? == 0 {
        return Err("confirmed repair DDL did not produce a nonempty .bak checkpoint".to_owned());
    }

    let pending_key = encode_usize(REPAIR_PENDING_KEY);
    let pending_value =
        Binary::new(vec![REPAIR_PENDING_VALUE_BYTE; REPAIR_PENDING_VALUE_LEN]);
    let pending_transaction =
        writable_transaction_for(&db, "checkpoint pending writer")?;
    pending_transaction
        .upsert(vec![TableKV::new(
            Atom::from(REPAIR_PENDING_TABLE),
            pending_key.clone(),
            Some(pending_value.clone()),
        )])
        .await
        .map_err(|error| format!("checkpoint pending upsert failed: {error:?}"))?;
    let pending_prepare = pending_transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("checkpoint pending prepare failed: {error:?}"))?;
    let pending_commit_uid = pending_transaction
        .get_commit_uid()
        .ok_or_else(|| "checkpoint pending prepare did not allocate a CID".to_owned())?;

    gate.arm()?;
    let (commit_tx, commit_rx) = async_bounded(1);
    rt.spawn(async move {
        let result = pending_transaction.commit_modified(pending_prepare).await;
        let _ = commit_tx.send(result).await;
    })
    .map_err(|error| format!("spawning checkpoint pending commit failed: {error:?}"))?;

    let (gated_commit_uid, _gated_handle) = gate.wait_until_entered().await?;
    let registered_checkpoint = logger
        .check_point_of(pending_commit_uid.clone())
        .await
        .ok_or_else(|| "checkpoint pending CID was not registered before flush".to_owned())?;
    let rotation_result = db.append_new_commit_log().await;
    let release_result = gate.release().await;
    let confirming_checkpoint = rotation_result
        .map_err(|error| format!("rotating at gated root WAL flush failed: {error}"))?;
    release_result?;
    expect_eq(
        "gated flush CID",
        &gated_commit_uid,
        &pending_commit_uid,
    )?;
    commit_rx
        .recv()
        .await
        .map_err(|error| format!("receiving checkpoint pending commit failed: {error}"))?
        .map_err(|error| format!("checkpoint pending commit failed: {error:?}"))?;

    expect_query_value(
        &db,
        REPAIR_PENDING_TABLE,
        pending_key,
        Some(&pending_value),
        "setup pending live value",
    )
    .await?;
    expect_eq(
        "pending CID remains unconfirmed",
        &logger.check_point_of(pending_commit_uid).await,
        &Some(registered_checkpoint),
    )?;

    let confirming_key = encode_usize(REPAIR_CONFIRMING_KEY);
    let confirming_value =
        Binary::new(vec![REPAIR_CONFIRMING_VALUE_BYTE; REPAIR_CONFIRMING_VALUE_LEN]);
    let confirming_commit_uid = commit_upsert(
        &db,
        REPAIR_CONFIRMING_TABLE,
        confirming_key.clone(),
        confirming_value.clone(),
        "checkpoint confirming writer",
    )
    .await?;
    wait_for_commit_confirmation(
        &rt,
        &logger.inner,
        confirming_commit_uid,
        OBSERVATION_TIMEOUT,
        "checkpoint confirming transaction",
    )
    .await?;
    expect_query_value(
        &db,
        REPAIR_CONFIRMING_TABLE,
        confirming_key,
        Some(&confirming_value),
        "setup confirming persisted value",
    )
    .await?;

    let (appended, confirmed, waiting) =
        stable_logger_accounting(&rt, &logger.inner, OBSERVATION_TIMEOUT).await?;
    expect_eq("repair setup append total", &appended, &3usize)?;
    expect_eq("repair setup confirm total", &confirmed, &2usize)?;
    expect_eq("repair setup waiting total", &waiting, &1usize)?;
    expect_eq(
        "repair setup manager produced/consumed",
        &tr_manager.produced_transaction_total(),
        &tr_manager.consumed_transaction_total(),
    )?;
    expect_eq(
        "repair setup manager active roots",
        &tr_manager.transaction_len(),
        &0usize,
    )?;

    let manifest = format!("{registered_checkpoint}\n{confirming_checkpoint}\n");
    fs::write(root.join(REPAIR_MANIFEST_FILE), manifest)
        .map_err(|error| format!("writing repair checkpoint manifest failed: {error}"))
}

/// 只复制数据目录、不复制任何根 WAL；证明 T1 在 repair 前确实尚未进入 redb，而 T2 已落地。
async fn phase_pre_repair_data(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
) -> TestResult<()> {
    let copy_root = root.join(PRE_REPAIR_COPY_DIR);
    if copy_root.exists() {
        return Err(format!("pre-repair copy root unexpectedly exists: {copy_root:?}"));
    }
    copy_dir_tree(&root.join("database"), &copy_root.join("database"))?;
    let (db, tr_manager, logger) = build_real_database(&rt, &copy_root).await?;
    let pending_key = encode_usize(REPAIR_PENDING_KEY);
    expect_query_value(
        &db,
        REPAIR_PENDING_TABLE,
        pending_key,
        None,
        "pre-repair pending data-file value",
    )
    .await?;
    let confirming_key = encode_usize(REPAIR_CONFIRMING_KEY);
    let confirming_value =
        Binary::new(vec![REPAIR_CONFIRMING_VALUE_BYTE; REPAIR_CONFIRMING_VALUE_LEN]);
    expect_query_value(
        &db,
        REPAIR_CONFIRMING_TABLE,
        confirming_key,
        Some(&confirming_value),
        "pre-repair confirming data-file value",
    )
    .await?;
    expect_eq("pre-repair copy append total", &logger.append_total_count(), &0usize)?;
    expect_eq("pre-repair copy confirm total", &logger.confirm_total_count(), &0usize)?;
    expect_eq(
        "pre-repair copy waiting total",
        &logger.waiting_confirm_count().await,
        &0usize,
    )?;
    expect_eq(
        "pre-repair copy manager produced/consumed",
        &tr_manager.produced_transaction_total(),
        &tr_manager.consumed_transaction_total(),
    )?;
    expect_eq(
        "pre-repair copy manager active roots",
        &tr_manager.transaction_len(),
        &0usize,
    )
}

/// 在全新进程执行生产 startup/try_repair，并用最终 redb 状态和根 WAL 收口共同验收。
async fn phase_repair_recover(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
) -> TestResult<()> {
    let (pending_checkpoint, confirming_checkpoint) = read_repair_manifest(&root)?;
    let wal_path = root.join("root-wal");
    let bak_before = nonempty_bak_count(&wal_path)?;
    let (db, tr_manager, logger) = build_real_database(&rt, &root).await?;

    let pending_key = encode_usize(REPAIR_PENDING_KEY);
    let pending_value =
        Binary::new(vec![REPAIR_PENDING_VALUE_BYTE; REPAIR_PENDING_VALUE_LEN]);
    expect_query_value(
        &db,
        REPAIR_PENDING_TABLE,
        pending_key,
        Some(&pending_value),
        "try_repair pending value",
    )
    .await?;
    let confirming_key = encode_usize(REPAIR_CONFIRMING_KEY);
    let confirming_value =
        Binary::new(vec![REPAIR_CONFIRMING_VALUE_BYTE; REPAIR_CONFIRMING_VALUE_LEN]);
    expect_query_value(
        &db,
        REPAIR_CONFIRMING_TABLE,
        confirming_key,
        Some(&confirming_value),
        "try_repair confirming value",
    )
    .await?;

    // T1 恢复动作略低于 Btree 1 MiB collector 阈值；追加一个 128-byte 真实事务只负责触发
    // 同表 collector，使 T1 的异步数据持久化与根确认在本阶段内闭环，而不等待 60 秒定时器。
    let trigger_key = encode_usize(REPAIR_TRIGGER_KEY);
    let trigger_value =
        Binary::new(vec![REPAIR_TRIGGER_VALUE_BYTE; REPAIR_TRIGGER_VALUE_LEN]);
    let _trigger_commit_uid = commit_upsert(
        &db,
        REPAIR_PENDING_TABLE,
        trigger_key.clone(),
        trigger_value.clone(),
        "checkpoint repair collector trigger",
    )
    .await?;
    wait_for_logger_closed(&rt, &logger, Duration::from_secs(30), "checkpoint repair").await?;

    expect_query_value(
        &db,
        REPAIR_PENDING_TABLE,
        encode_usize(REPAIR_PENDING_KEY),
        Some(&pending_value),
        "repaired pending value after confirmation",
    )
    .await?;
    expect_query_value(
        &db,
        REPAIR_CONFIRMING_TABLE,
        encode_usize(REPAIR_CONFIRMING_KEY),
        Some(&confirming_value),
        "repaired confirming value after confirmation",
    )
    .await?;
    expect_query_value(
        &db,
        REPAIR_PENDING_TABLE,
        trigger_key,
        Some(&trigger_value),
        "repair collector trigger value",
    )
    .await?;
    expect_eq(
        "repaired pending Btree overlay drained",
        &db.table_cache_size(&Atom::from(REPAIR_PENDING_TABLE)).await,
        &Some(0u64),
    )?;
    expect_eq(
        "repaired confirming Btree overlay drained",
        &db.table_cache_size(&Atom::from(REPAIR_CONFIRMING_TABLE)).await,
        &Some(0u64),
    )?;
    expect_eq(
        "repair manager produced/consumed",
        &tr_manager.produced_transaction_total(),
        &tr_manager.consumed_transaction_total(),
    )?;
    expect_eq(
        "repair manager active roots",
        &tr_manager.transaction_len(),
        &0usize,
    )?;

    let bak_after = nonempty_bak_count(&wal_path)?;
    if bak_after <= bak_before {
        return Err(format!(
            "try_repair did not add a confirmed nonempty .bak checkpoint: before={bak_before}, after={bak_after}",
        ));
    }
    for (label, checkpoint) in [
        ("pending checkpoint", pending_checkpoint),
        ("confirming checkpoint", confirming_checkpoint),
    ] {
        match checkpoint_file_state(&wal_path, checkpoint)? {
            CheckpointFileState::Backup(len) if len > 0 => {},
            observed => {
                return Err(format!(
                    "{label} must be a nonempty .bak after repair confirmation, observed {observed:?}",
                ));
            },
        }
    }
    let active = active_file_sizes(&wal_path)?;
    if active.iter().any(|(_, len)| *len > 0) {
        return Err(format!("nonempty active WAL remains after try_repair: {active:?}"));
    }
    Ok(())
}

/// 移走全部已确认根 WAL 后连续两次冷启动，证明最终值来自数据文件而不是残留 replay 输入。
async fn phase_repair_data_only(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
    archive_wal: bool,
) -> TestResult<()> {
    let (pending_checkpoint, confirming_checkpoint) = read_repair_manifest(&root)?;
    let wal_path = root.join("root-wal");
    let archived_wal = root.join(ARCHIVED_WAL_DIR);
    if archive_wal {
        if archived_wal.exists() {
            return Err(format!("archived repair WAL unexpectedly exists: {archived_wal:?}"));
        }
        fs::rename(&wal_path, &archived_wal)
            .map_err(|error| format!("archiving repaired root WAL failed: {error}"))?;
    } else {
        if !archived_wal.exists() {
            return Err("second repair data-only start cannot find archived WAL".to_owned());
        }
        let active = active_file_sizes(&wal_path)?;
        if active.iter().any(|(_, len)| *len > 0) {
            return Err(format!(
                "first repair data-only start produced a nonempty root WAL: {active:?}",
            ));
        }
    }
    for (label, checkpoint) in [
        ("archived pending checkpoint", pending_checkpoint),
        ("archived confirming checkpoint", confirming_checkpoint),
    ] {
        match checkpoint_file_state(&archived_wal, checkpoint)? {
            CheckpointFileState::Backup(len) if len > 0 => {},
            observed => {
                return Err(format!(
                    "{label} must remain a nonempty .bak, observed {observed:?}",
                ));
            },
        }
    }

    let (db, tr_manager, logger) = build_real_database(&rt, &root).await?;
    let label = if archive_wal {
        "repair data-only"
    } else {
        "second repair data-only"
    };
    let pending_value =
        Binary::new(vec![REPAIR_PENDING_VALUE_BYTE; REPAIR_PENDING_VALUE_LEN]);
    let confirming_value =
        Binary::new(vec![REPAIR_CONFIRMING_VALUE_BYTE; REPAIR_CONFIRMING_VALUE_LEN]);
    let trigger_value =
        Binary::new(vec![REPAIR_TRIGGER_VALUE_BYTE; REPAIR_TRIGGER_VALUE_LEN]);
    expect_query_value(
        &db,
        REPAIR_PENDING_TABLE,
        encode_usize(REPAIR_PENDING_KEY),
        Some(&pending_value),
        &format!("{label} pending value"),
    )
    .await?;
    expect_query_value(
        &db,
        REPAIR_CONFIRMING_TABLE,
        encode_usize(REPAIR_CONFIRMING_KEY),
        Some(&confirming_value),
        &format!("{label} confirming value"),
    )
    .await?;
    expect_query_value(
        &db,
        REPAIR_PENDING_TABLE,
        encode_usize(REPAIR_TRIGGER_KEY),
        Some(&trigger_value),
        &format!("{label} trigger value"),
    )
    .await?;
    expect_eq(&format!("{label} append total"), &logger.append_total_count(), &0usize)?;
    expect_eq(&format!("{label} confirm total"), &logger.confirm_total_count(), &0usize)?;
    expect_eq(
        &format!("{label} waiting total"),
        &logger.waiting_confirm_count().await,
        &0usize,
    )?;
    expect_eq(
        &format!("{label} manager produced/consumed"),
        &tr_manager.produced_transaction_total(),
        &tr_manager.consumed_transaction_total(),
    )?;
    expect_eq(
        &format!("{label} manager active roots"),
        &tr_manager.transaction_len(),
        &0usize,
    )
}

fn run_replay_child_phase(phase: &str, root: &Path) -> TestResult<()> {
    match phase {
        "setup-unconfirmed" => {
            let root = root.to_path_buf();
            run_on_runtime(RUNTIME_TIMEOUT, move |rt| async move {
                phase_setup_unconfirmed(rt, root).await
            })
        },
        "reopen-replay" => {
            let root = root.to_path_buf();
            run_on_runtime(RUNTIME_TIMEOUT, move |rt| async move {
                phase_reopen_replay(rt, root).await
            })
        },
        other => Err(format!("unknown root WAL rotation phase: {other}")),
    }
}

/// 用真实 logger 确定性建立“T1 登记在旧 checkpoint、T2 先确认”的崩溃前状态。
async fn phase_setup_unconfirmed(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
) -> TestResult<()> {
    let wal_path = root.join("root-wal");
    let logger = build_logger(&rt, &wal_path).await?;
    let next_checkpoint = logger.current_check_point().await;
    let t1_checkpoint = next_checkpoint
        .checked_sub(1)
        .ok_or_else(|| "initial checkpoint index underflowed".to_owned())?;

    let t1_payload = vec![T1_PAYLOAD_BYTE; DIRECT_PAYLOAD_LEN];
    let t1_handle = logger
        .append(T1, t1_payload)
        .await
        .map_err(|error| format!("appending T1 failed: {error}"))?;
    expect_eq(
        "T1 registered checkpoint before rotation",
        &logger.check_point_of(T1).await,
        &Some(t1_checkpoint),
    )?;

    let t2_checkpoint = logger
        .append_check_point()
        .await
        .map_err(|error| format!("rotating between T1 append and flush failed: {error}"))?;
    expect_eq(
        "explicit rotation allocation",
        &t2_checkpoint,
        &next_checkpoint,
    )?;

    logger
        .flush(t1_handle)
        .await
        .map_err(|error| format!("flushing T1 after rotation failed: {error}"))?;

    let t2_payload = vec![T2_PAYLOAD_BYTE; DIRECT_PAYLOAD_LEN];
    let t2_handle = logger
        .append(T2, t2_payload)
        .await
        .map_err(|error| format!("appending T2 failed: {error}"))?;
    logger
        .flush(t2_handle)
        .await
        .map_err(|error| format!("flushing T2 failed: {error}"))?;
    expect_eq(
        "T2 registered checkpoint",
        &logger.check_point_of(T2).await,
        &Some(t2_checkpoint),
    )?;

    logger
        .confirm(T2)
        .await
        .map_err(|error| format!("confirming T2 failed: {error}"))?;
    expect_eq("direct append total", &logger.append_total_count(), &2usize)?;
    expect_eq("direct confirm total", &logger.confirm_total_count(), &1usize)?;
    expect_eq(
        "direct waiting total",
        &logger.waiting_confirm_count().await,
        &1usize,
    )?;
    expect_eq(
        "T1 remains registered after T2 confirmation",
        &logger.check_point_of(T1).await,
        &Some(t1_checkpoint),
    )?;

    let manifest = format!("{t1_checkpoint}\n{t2_checkpoint}\n");
    fs::write(root.join(MANIFEST_FILE), manifest)
        .map_err(|error| format!("writing checkpoint manifest failed: {error}"))
}

/// 在全新进程中同时验证 `.bak` 拓扑和 replay 的精确事务集合。
async fn phase_reopen_replay(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
) -> TestResult<()> {
    let (t1_checkpoint, t2_checkpoint) = read_manifest(&root)?;
    let wal_path = root.join("root-wal");
    let mut failures = Vec::new();
    for (label, checkpoint) in [
        ("T1 registered checkpoint", t1_checkpoint),
        ("T2 physical checkpoint", t2_checkpoint),
    ] {
        match checkpoint_file_state(&wal_path, checkpoint)? {
            CheckpointFileState::Active(len) if len > 0 => {},
            observed => failures.push(format!(
                "{label} must remain active and nonempty before T1 confirmation, observed {observed:?}",
            )),
        }
    }

    let logger = build_logger(&rt, &wal_path).await?;
    let observed = Arc::new(Mutex::new(Vec::<(u128, Vec<u8>)>::new()));
    let observed_copy = observed.clone();
    let replay_result = logger
        .start_replay::<Vec<u8>, _>(Arc::new(move |commit_uid: Guid, payload: Vec<u8>| {
            observed_copy
                .lock()
                .expect("replay observation mutex must not be poisoned")
                .push((commit_uid.0, payload));
            Ok(())
        }))
        .await;
    let finish_result = logger.finish_replay().await;

    match replay_result {
        Ok((count, bytes)) => {
            if count != 2 {
                failures.push(format!("replay count must be 2, observed {count}"));
            }
            let expected_bytes = 2 * (16 + DIRECT_PAYLOAD_LEN);
            if bytes != expected_bytes {
                failures.push(format!(
                    "replay bytes must be {expected_bytes}, observed {bytes}",
                ));
            }
        },
        Err(error) => failures.push(format!("reopening replay failed: {error}")),
    }
    if let Err(error) = finish_result {
        failures.push(format!("finishing reopening replay failed: {error}"));
    }

    let mut observed = observed
        .lock()
        .map_err(|_| "replay observation mutex was poisoned".to_owned())?
        .clone();
    observed.sort_by_key(|(commit_uid, _)| *commit_uid);
    let expected = vec![
        (T1.0, vec![T1_PAYLOAD_BYTE; DIRECT_PAYLOAD_LEN]),
        (T2.0, vec![T2_PAYLOAD_BYTE; DIRECT_PAYLOAD_LEN]),
    ];
    if observed != expected {
        failures.push(format!(
            "replay payload set mismatch: expected IDs [{:#x}, {:#x}], observed {:?}",
            T1.0,
            T2.0,
            observed
                .iter()
                .map(|(commit_uid, payload)| (*commit_uid, payload.len()))
                .collect::<Vec<_>>(),
        ));
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

/// 用完整 `pi_db` 生产路径证明显式轮换可与事务 append/flush 窗口相交。
async fn verify_manager_rotation_production_path(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
) -> TestResult<()> {
    let wal_path = root.join("root-wal");
    let logger = build_logger(&rt, &wal_path).await?;
    let tr_manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger.clone(),
    );
    let db_path = root.join("database");
    let db = KVDBManagerBuilder::new(rt.clone(), tr_manager.clone(), &db_path)
        .startup(false)
        .await
        .map_err(|error| format!("starting production redline database failed: {error}"))?;

    create_memory_table(&db).await?;
    // DDL 的 Meta 子事务已经完成根 WAL append/flush，但其表日志确认由独立 collector
    // 异步完成，不能把 collector 的分钟级周期误设为本红线的十秒前置条件。这里改用公开
    // checkpoint API 把已经落盘的 DDL WAL 轮换为只读文件，并建立一个可直接观测为空的新
    // checkpoint；后续只等待被测 Memory 事务自身的即时确认。
    let expected_fresh_checkpoint = logger.current_check_point().await;
    let registered_checkpoint = db
        .append_new_commit_log()
        .await
        .map_err(|error| format!("creating fresh production checkpoint failed: {error}"))?;
    expect_eq(
        "fresh production checkpoint allocation",
        &registered_checkpoint,
        &expected_fresh_checkpoint,
    )?;
    expect_eq(
        "fresh production checkpoint state",
        &checkpoint_file_state(&wal_path, registered_checkpoint)?,
        &CheckpointFileState::Active(0),
    )?;
    let next_checkpoint = logger.current_check_point().await;

    let key = encode_usize(1);
    let value = Binary::new(vec![0x5a; 64 * 1024]);
    let transaction = writable_transaction(&db, "checkpoint production writer")?;
    transaction
        .upsert(vec![TableKV::new(
            Atom::from(MEMORY_TABLE),
            key.clone(),
            Some(value.clone()),
        )])
        .await
        .map_err(|error| format!("production redline upsert failed: {error:?}"))?;
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("production redline prepare failed: {error:?}"))?;
    let commit_uid = transaction
        .get_commit_uid()
        .ok_or_else(|| "production redline prepare did not allocate CID".to_owned())?;
    let watcher_commit_uid = commit_uid.clone();
    let append_before = logger.append_total_count();
    let target_append_total = append_before + 1;

    let (rotation_tx, rotation_rx) = async_bounded(1);
    let watcher_db = db.clone();
    let watcher_logger = logger.clone();
    rt.spawn(async move {
        let deadline = Instant::now() + OBSERVATION_TIMEOUT;
        loop {
            let appended = watcher_logger.append_total_count();
            if appended == target_append_total {
                let checkpoint = watcher_logger.check_point_of(watcher_commit_uid).await;
                let rotation = watcher_db.append_new_commit_log().await;
                let _ = rotation_tx.send((checkpoint, rotation)).await;
                break;
            }
            if appended > target_append_total || Instant::now() >= deadline {
                let _ = rotation_tx
                    .send((
                        None,
                        Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!(
                                "append watcher missed target {target_append_total}, observed {appended}",
                            ),
                        )),
                    ))
                    .await;
                break;
            }
            std::hint::spin_loop();
        }
    })
    .map_err(|error| format!("spawning checkpoint watcher failed: {error:?}"))?;

    let (commit_tx, commit_rx) = async_bounded(1);
    rt.spawn(async move {
        let result = transaction.commit_modified(prepare).await;
        let _ = commit_tx.send(result).await;
    })
    .map_err(|error| format!("spawning production commit failed: {error:?}"))?;

    let (observed_checkpoint, rotation_result) = rotation_rx
        .recv()
        .await
        .map_err(|error| format!("receiving checkpoint watcher result failed: {error}"))?;
    expect_eq(
        "production CID registered checkpoint",
        &observed_checkpoint,
        &Some(registered_checkpoint),
    )?;
    let rotation = rotation_result
        .map_err(|error| format!("public append_new_commit_log failed: {error}"))?;
    expect_eq(
        "public rotation allocated checkpoint",
        &rotation,
        &next_checkpoint,
    )?;

    commit_rx
        .recv()
        .await
        .map_err(|error| format!("receiving production commit result failed: {error}"))?
        .map_err(|error| format!("production transaction commit failed: {error:?}"))?;

    let registered_state = checkpoint_file_state(&wal_path, registered_checkpoint)?;
    match registered_state {
        CheckpointFileState::Active(len) | CheckpointFileState::Backup(len) if len > 0 => {},
        observed => {
            return Err(format!(
                "registered checkpoint must contain the transaction WAL after commit, observed {observed:?}",
            ));
        },
    }

    let (queried, _) = db
        .query_with_version(Atom::from(MEMORY_TABLE), key)
        .await
        .map_err(|error| format!("authoritative production query failed: {error:?}"))?;
    if queried.as_ref().map(AsRef::<[u8]>::as_ref) != Some(value.as_ref()) {
        return Err(format!(
            "authoritative value mismatch after checkpoint race: expected_len={}, observed_len={:?}",
            value.len(),
            queried.as_ref().map(Binary::len),
        ));
    }

    wait_for_commit_confirmation(
        &rt,
        &logger,
        commit_uid,
        OBSERVATION_TIMEOUT,
        "production transaction confirmation",
    )
    .await?;
    let (appended, confirmed, waiting) =
        stable_logger_accounting(&rt, &logger, OBSERVATION_TIMEOUT).await?;
    expect_eq(
        "production append total",
        &appended,
        &target_append_total,
    )?;
    expect_eq(
        "production append/confirm conservation",
        &(confirmed + waiting),
        &appended,
    )?;
    expect_eq(
        "production manager produced/consumed",
        &tr_manager.produced_transaction_total(),
        &tr_manager.consumed_transaction_total(),
    )?;
    expect_eq(
        "production manager active roots",
        &tr_manager.transaction_len(),
        &0usize,
    )
}

async fn build_gated_database(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
) -> TestResult<(
    GatedDb,
    Transaction2PcManager<usize, GatedCommitLogger>,
    GatedCommitLogger,
    FlushGateControl,
)> {
    let inner = build_logger(rt, &root.join("root-wal")).await?;
    let (logger, gate) = gated_commit_logger(inner);
    let tr_manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger.clone(),
    );
    let db = KVDBManagerBuilder::new(
        rt.clone(),
        tr_manager.clone(),
        root.join("database"),
    )
    .startup(false)
    .await
    .map_err(|error| format!("starting gated checkpoint database failed: {error}"))?;
    Ok((db, tr_manager, logger, gate))
}

async fn build_real_database(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
) -> TestResult<(
    RealDb,
    Transaction2PcManager<usize, CommitLogger>,
    CommitLogger,
)> {
    let logger = build_logger(rt, &root.join("root-wal")).await?;
    let tr_manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger.clone(),
    );
    let db = KVDBManagerBuilder::new(
        rt.clone(),
        tr_manager.clone(),
        root.join("database"),
    )
    .startup(false)
    .await
    .map_err(|error| format!("starting checkpoint repair database failed: {error}"))?;
    Ok((db, tr_manager, logger))
}

async fn create_repair_tables(db: &GatedDb) -> TestResult<Guid> {
    let transaction = writable_transaction_for(db, "checkpoint repair DDL")?;
    for name in [REPAIR_PENDING_TABLE, REPAIR_CONFIRMING_TABLE] {
        transaction
            .create_table_with_options(
                Atom::from(name),
                KVTableMeta::new(
                    KVDBTableType::BtreeOrdTab,
                    true,
                    EnumType::Usize,
                    EnumType::Usize,
                ),
                CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
                false,
            )
            .await
            .map_err(|error| format!("creating repair Btree table {name} failed: {error}"))?;
    }
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing checkpoint repair DDL failed: {error:?}"))?;
    let commit_uid = transaction
        .get_commit_uid()
        .ok_or_else(|| "checkpoint repair DDL did not allocate a CID".to_owned())?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing checkpoint repair DDL failed: {error:?}"))?;
    Ok(commit_uid)
}

fn writable_transaction_for<Log>(
    db: &KVDBManager<usize, Log>,
    source: &str,
) -> TestResult<KVDBTransaction<usize, Log>>
where
    Log: AsyncCommitLog<C = usize, Cid = Guid>,
{
    db.transaction(Atom::from(source), true, 10_000, 10_000)
        .ok_or_else(|| format!("database rejected writable transaction {source}"))
}

async fn commit_upsert<Log>(
    db: &KVDBManager<usize, Log>,
    table: &str,
    key: Binary,
    value: Binary,
    source: &str,
) -> TestResult<Guid>
where
    Log: AsyncCommitLog<C = usize, Cid = Guid>,
{
    let transaction = writable_transaction_for(db, source)?;
    transaction
        .upsert(vec![TableKV::new(
            Atom::from(table),
            key,
            Some(value),
        )])
        .await
        .map_err(|error| format!("{source} upsert failed: {error:?}"))?;
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("{source} prepare failed: {error:?}"))?;
    let commit_uid = transaction
        .get_commit_uid()
        .ok_or_else(|| format!("{source} prepare did not allocate a CID"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("{source} commit failed: {error:?}"))?;
    Ok(commit_uid)
}

async fn expect_query_value<Log>(
    db: &KVDBManager<usize, Log>,
    table: &str,
    key: Binary,
    expected: Option<&Binary>,
    label: &str,
) -> TestResult<()>
where
    Log: AsyncCommitLog<C = usize, Cid = Guid>,
{
    let (actual, _) = db
        .query_with_version(Atom::from(table), key)
        .await
        .map_err(|error| format!("{label}: query_with_version failed: {error:?}"))?;
    let equal = match (actual.as_ref(), expected) {
        (None, None) => true,
        (Some(actual), Some(expected)) => actual.as_ref() == expected.as_ref(),
        _ => false,
    };
    if equal {
        Ok(())
    } else {
        Err(format!(
            "{label}: expected_len={:?}, observed_len={:?}",
            expected.map(Binary::len),
            actual.as_ref().map(Binary::len),
        ))
    }
}

async fn create_memory_table(db: &RealDb) -> TestResult<()> {
    let transaction = writable_transaction(db, "checkpoint production DDL")?;
    transaction
        .create_table(
            Atom::from(MEMORY_TABLE),
            KVTableMeta::new(
                KVDBTableType::MemOrdTab,
                true,
                EnumType::Usize,
                EnumType::Usize,
            ),
            false,
        )
        .await
        .map_err(|error| format!("creating production Memory table failed: {error}"))?;
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing production DDL failed: {error:?}"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing production DDL failed: {error:?}"))
}

fn writable_transaction(
    db: &RealDb,
    source: &str,
) -> TestResult<KVDBTransaction<usize, CommitLogger>> {
    writable_transaction_for(db, source)
}

async fn build_logger(
    rt: &MultiTaskRuntime<()>,
    wal_path: &Path,
) -> TestResult<CommitLogger> {
    fs::create_dir_all(wal_path)
        .map_err(|error| format!("creating WAL path {wal_path:?} failed: {error}"))?;
    CommitLoggerBuilder::new(rt.clone(), wal_path)
        .delay_timeout(10)
        .log_file_limit(64 * 1024 * 1024)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building CommitLogger at {wal_path:?} failed: {error}"))
}

async fn wait_for_commit_confirmation(
    rt: &MultiTaskRuntime<()>,
    logger: &CommitLogger,
    commit_uid: Guid,
    timeout: Duration,
    label: &str,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if logger.check_point_of(commit_uid.clone()).await.is_none() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{label}: CID {commit_uid:?} remained registered beyond {timeout:?}",
            ));
        }
        rt.timeout(1).await;
    }
}

/// 在两个原子确认计数读数一致时读取 checkpoint map 长度，避免把并发确认中的瞬态组合
/// 当成守恒失败。调用方已经停止产生新事务，因此 append 总数也必须在同一轮保持稳定。
async fn stable_logger_accounting(
    rt: &MultiTaskRuntime<()>,
    logger: &CommitLogger,
    timeout: Duration,
) -> TestResult<(usize, usize, usize)> {
    let deadline = Instant::now() + timeout;
    loop {
        let appended_before = logger.append_total_count();
        let confirmed_before = logger.confirm_total_count();
        let waiting = logger.waiting_confirm_count().await;
        let confirmed_after = logger.confirm_total_count();
        let appended_after = logger.append_total_count();
        if appended_before == appended_after && confirmed_before == confirmed_after {
            return Ok((appended_after, confirmed_after, waiting));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "logger accounting did not stabilize before {timeout:?}: appended={appended_before}->{appended_after}, confirmed={confirmed_before}->{confirmed_after}, waiting={waiting}",
            ));
        }
        rt.timeout(1).await;
    }
}

async fn wait_for_logger_closed(
    rt: &MultiTaskRuntime<()>,
    logger: &CommitLogger,
    timeout: Duration,
    label: &str,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let (appended, confirmed, waiting) =
            stable_logger_accounting(rt, logger, Duration::from_secs(1)).await?;
        if appended > 0 && confirmed == appended && waiting == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{label}: root WAL did not close before {timeout:?}; appended={appended}, confirmed={confirmed}, waiting={waiting}",
            ));
        }
        rt.timeout(5).await;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CheckpointFileState {
    Active(u64),
    Backup(u64),
    Missing,
}

fn checkpoint_file_state(
    wal_path: &Path,
    checkpoint: usize,
) -> TestResult<CheckpointFileState> {
    let active = wal_path.join(format!("{checkpoint:09}"));
    let backup = active.with_extension("bak");
    let active_exists = active.is_file();
    let backup_exists = backup.is_file();
    if active_exists && backup_exists {
        return Err(format!(
            "checkpoint {checkpoint} exists as both active and backup files",
        ));
    }
    if active_exists {
        return fs::metadata(&active)
            .map(|metadata| CheckpointFileState::Active(metadata.len()))
            .map_err(|error| format!("reading active checkpoint {active:?} failed: {error}"));
    }
    if backup_exists {
        return fs::metadata(&backup)
            .map(|metadata| CheckpointFileState::Backup(metadata.len()))
            .map_err(|error| format!("reading backup checkpoint {backup:?} failed: {error}"));
    }
    Ok(CheckpointFileState::Missing)
}

fn regular_file_sizes(path: &Path) -> TestResult<Vec<(PathBuf, u64)>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(path)
        .map_err(|error| format!("reading WAL directory {path:?} failed: {error}"))?
    {
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
            *len > 0
                && file.extension().and_then(|extension| extension.to_str()) == Some("bak")
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

fn copy_dir_tree(source: &Path, destination: &Path) -> TestResult<()> {
    fs::create_dir_all(destination)
        .map_err(|error| format!("creating copy directory {destination:?} failed: {error}"))?;
    for entry in fs::read_dir(source)
        .map_err(|error| format!("reading copy source {source:?} failed: {error}"))?
    {
        let entry =
            entry.map_err(|error| format!("reading copy entry in {source:?} failed: {error}"))?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry
            .file_type()
            .map_err(|error| format!("reading file type for {source_path:?} failed: {error}"))?;
        if file_type.is_dir() {
            copy_dir_tree(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            fs::copy(&source_path, &destination_path).map_err(|error| {
                format!(
                    "copying database file {source_path:?} to {destination_path:?} failed: {error}",
                )
            })?;
        } else {
            return Err(format!(
                "database copy encountered unsupported entry {source_path:?}",
            ));
        }
    }
    Ok(())
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn read_manifest(root: &Path) -> TestResult<(usize, usize)> {
    let text = fs::read_to_string(root.join(MANIFEST_FILE))
        .map_err(|error| format!("reading checkpoint manifest failed: {error}"))?;
    let mut lines = text.lines();
    let t1 = lines
        .next()
        .ok_or_else(|| "checkpoint manifest omitted T1 index".to_owned())?
        .parse::<usize>()
        .map_err(|error| format!("parsing T1 checkpoint failed: {error}"))?;
    let t2 = lines
        .next()
        .ok_or_else(|| "checkpoint manifest omitted T2 index".to_owned())?
        .parse::<usize>()
        .map_err(|error| format!("parsing T2 checkpoint failed: {error}"))?;
    if lines.next().is_some() {
        return Err("checkpoint manifest contains unexpected extra lines".to_owned());
    }
    Ok((t1, t2))
}

fn read_repair_manifest(root: &Path) -> TestResult<(usize, usize)> {
    let text = fs::read_to_string(root.join(REPAIR_MANIFEST_FILE))
        .map_err(|error| format!("reading repair checkpoint manifest failed: {error}"))?;
    let mut lines = text.lines();
    let pending = lines
        .next()
        .ok_or_else(|| "repair manifest omitted pending checkpoint".to_owned())?
        .parse::<usize>()
        .map_err(|error| format!("parsing pending checkpoint failed: {error}"))?;
    let confirming = lines
        .next()
        .ok_or_else(|| "repair manifest omitted confirming checkpoint".to_owned())?
        .parse::<usize>()
        .map_err(|error| format!("parsing confirming checkpoint failed: {error}"))?;
    if lines.next().is_some() {
        return Err("repair checkpoint manifest contains unexpected extra lines".to_owned());
    }
    Ok((pending, confirming))
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
            "{label}: expected {expected:?}, observed {actual:?}",
        ))
    }
}

fn run_on_runtime<T, F, Fut>(timeout: Duration, build: F) -> TestResult<T>
where
    T: Send + 'static,
    F: FnOnce(MultiTaskRuntime<()>) -> Fut,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    let _time_loop = startup_global_time_loop(1);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(4)
        .build();
    let future = build(rt.clone());
    let (result_tx, result_rx) = bounded(1);
    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    })
    .map_err(|error| format!("spawning root WAL rotation target failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("root WAL rotation target exceeded {timeout:?}: {error}"))?
}

fn run_phase_process(root: &Path, phase: &str, timeout: Duration) -> TestResult<()> {
    let executable = env::current_exe()
        .map_err(|error| format!("locating root WAL rotation executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(REPLAY_TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase)
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| format!("spawning root WAL rotation phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("root WAL rotation phase {phase} exited with {status}"))
    }
}

fn run_repair_phase_process(
    root: &Path,
    phase: &str,
    timeout: Duration,
) -> TestResult<()> {
    let executable = env::current_exe()
        .map_err(|error| format!("locating checkpoint repair executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(REPAIR_TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase)
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| format!("spawning checkpoint repair phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("checkpoint repair phase {phase} exited with {status}"))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking root WAL rotation child failed: {error}"))? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("root WAL rotation child exceeded {timeout:?}"));
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn unique_temp_root(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must follow UNIX_EPOCH")
        .as_nanos();
    env::temp_dir().join(format!(
        "pi_db_root_wal_rotation_{label}_{}_{}",
        std::process::id(),
        nanos,
    ))
}
