//! `KVDBCommitConfirm` 合法成功信号协议测试。
//!
//! 被测生产入口是 `pi_db::KVDBCommitConfirm` 及其
//! `Fn(Guid, Guid, Result<(), KVTableTrError>)` 实现。当前 `pi_db` 内置表只在最终持久化
//! 成功后传入 `Ok(())`；数据文件失败通过“不调用确认器”保留根 WAL。因此本 target 只验证：
//!
//! - 前 N-1 个合法成功信号不得提前确认，最后一个必须恰好确认一次；
//! - 多线程并发发送不同子表的成功信号仍只能确认一次；
//! - transaction/commit UID 不匹配必须被拒绝，且不能消费合法成功计数。
//!
//! 本 target 使用确定性的 `AsyncCommitLog` fake 观察最终 logger 调用。fake 只隔离真实磁盘
//! I/O，不补全生产状态机，也不用于推断表持久化失败的生产行为。真实 LogOrdered 文件失败、
//! WAL 保留和重启修复由 `tests/commit_confirmation_real_environment.rs` 独立验证。
//!
//! `Result` 能表达的 `Err` 不属于当前内置合法调用域，本 target 不把手工 `Err` 注入包装成
//! 生产正确性断言。对应契约、架构和历史误报关闭记录：
//!
//! - `docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only`；
//! - `docs/PI_DB_ARCHITECTURE.md#arch-confirm-success-only`；
//! - `docs/BUG_001_FIX_PLAN.md#bug-001-closure-index`；
//! - `docs/TEST_AND_BENCHMARK_STRATEGY.md#test-fault-injection`。

use std::{
    io::{Error as IoError, ErrorKind, Result as IoResult},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier,
    },
    thread,
    time::Duration,
};

use bytes::BufMut;
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};
use futures::future::BoxFuture;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    AsyncRuntime,
};
use pi_async_transaction::AsyncCommitLog;
use pi_db::KVDBCommitConfirm;
use pi_guid::Guid;

const TASK_DEADLINE: Duration = Duration::from_secs(5);
const NO_CONFIRM_OBSERVATION: Duration = Duration::from_millis(300);

/// 只记录根 WAL 最终确认调用的提交日志 fake。
///
/// `confirm` 在 future 被轮询时同时记录次数和 commit UID。其它 trait 方法返回确定性的空
/// 成功值，因为本 target 只验证确认器的合法成功计数，不声称覆盖 append/flush/replay。
/// 真实 logger 和文件系统语义由 `commit_confirmation_real_environment` 覆盖。
#[derive(Clone)]
struct RecordingCommitLog {
    confirmed_tx: Sender<Guid>,
    confirm_count: Arc<AtomicUsize>,
}

impl RecordingCommitLog {
    fn new() -> (Self, Receiver<Guid>) {
        let (confirmed_tx, confirmed_rx) = bounded(16);
        (
            Self {
                confirmed_tx,
                confirm_count: Arc::new(AtomicUsize::new(0)),
            },
            confirmed_rx,
        )
    }

    fn confirm_count(&self) -> usize {
        self.confirm_count.load(Ordering::SeqCst)
    }
}

impl AsyncCommitLog for RecordingCommitLog {
    type C = ();
    type Cid = Guid;

    fn append<B>(&self, _commit_uid: Self::Cid, _log: B) -> BoxFuture<'static, IoResult<Self::C>>
    where
        B: BufMut + AsRef<[u8]> + Send + Sized + 'static,
    {
        Box::pin(async { Ok(()) })
    }

    fn flush(&self, _log_handle: Self::C) -> BoxFuture<'static, IoResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn confirm(&self, commit_uid: Self::Cid) -> BoxFuture<'static, IoResult<()>> {
        let confirmed_tx = self.confirmed_tx.clone();
        let confirm_count = self.confirm_count.clone();
        Box::pin(async move {
            confirm_count.fetch_add(1, Ordering::SeqCst);
            confirmed_tx.send(commit_uid).map_err(|error| {
                IoError::new(
                    ErrorKind::BrokenPipe,
                    format!("recording confirm receiver closed: {error}"),
                )
            })
        })
    }

    fn start_replay<B, F>(&self, _callback: Arc<F>) -> BoxFuture<'static, IoResult<(usize, usize)>>
    where
        B: BufMut + AsRef<[u8]> + From<Vec<u8>> + Send + Sized + 'static,
        F: Fn(Self::Cid, B) -> IoResult<()> + Send + Sync + 'static,
    {
        Box::pin(async { Ok((0, 0)) })
    }

    fn append_replay<B>(
        &self,
        _commit_uid: Self::Cid,
        _log: B,
    ) -> BoxFuture<'static, IoResult<Self::C>>
    where
        B: BufMut + AsRef<[u8]> + Send + Sized + 'static,
    {
        Box::pin(async { Ok(()) })
    }

    fn flush_replay(&self, _log_handle: Self::C) -> BoxFuture<'static, IoResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn confirm_replay(&self, _commit_uid: Self::Cid) -> BoxFuture<'static, IoResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn finish_replay(&self) -> BoxFuture<'static, IoResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn check_point_of(&self, _commit_uid: Self::Cid) -> BoxFuture<'static, Option<usize>> {
        Box::pin(async { None })
    }

    fn current_check_point(&self) -> BoxFuture<'static, usize> {
        Box::pin(async { 0 })
    }

    fn append_check_point(&self) -> BoxFuture<'static, IoResult<usize>> {
        Box::pin(async { Ok(0) })
    }

    fn waiting_confirm_count(&self) -> BoxFuture<'static, usize> {
        Box::pin(async { 0 })
    }

    fn append_total_count(&self) -> usize {
        0
    }

    fn confirm_total_count(&self) -> usize {
        self.confirm_count()
    }
}

/// 为每个用例提供独立单 worker runtime、记录器和事务身份。
///
/// 单 worker 使“栅栏任务已执行”成为稳定观察点；需要验证调用方并发时，测试线程并发调用
/// 同一个 confirmer，而最终 logger 任务仍由该 runtime 执行。各用例不共享全局可变状态。
struct ConfirmHarness {
    rt: MultiTaskRuntime<()>,
    log: RecordingCommitLog,
    confirmed_rx: Receiver<Guid>,
    transaction_uid: Guid,
    commit_uid: Guid,
}

impl ConfirmHarness {
    fn new() -> Self {
        let rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(1)
            .build();
        let (log, confirmed_rx) = RecordingCommitLog::new();
        Self {
            rt,
            log,
            confirmed_rx,
            transaction_uid: Guid(0x1001),
            commit_uid: Guid(0x2001),
        }
    }

    fn confirmer(
        &self,
        persistent_child_count: usize,
    ) -> KVDBCommitConfirm<(), RecordingCommitLog> {
        KVDBCommitConfirm::new(
            self.rt.clone(),
            self.log.clone(),
            self.transaction_uid.clone(),
            Some(self.commit_uid.clone()),
            persistent_child_count,
        )
    }

    /// 先等待 runtime 栅栏，再使用有界观察期证明 logger 没有被提前调用。
    fn assert_not_confirmed(&self, scenario: &str) {
        let (fence_tx, fence_rx) = bounded(1);
        self.rt
            .spawn(async move {
                let _ = fence_tx.send(());
            })
            .expect("the runtime must accept the fence task");
        fence_rx
            .recv_timeout(TASK_DEADLINE)
            .expect("the runtime must execute the fence task before the deadline");

        match self.confirmed_rx.recv_timeout(NO_CONFIRM_OBSERVATION) {
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                panic!("{scenario}: recording commit log disconnected unexpectedly")
            }
            Ok(commit_uid) => {
                panic!("{scenario}: root WAL was confirmed early with {commit_uid:?}")
            }
        }
        assert_eq!(
            self.log.confirm_count(),
            0,
            "{scenario}: AsyncCommitLog::confirm must not be called"
        );
    }

    fn assert_confirmed_once(&self, scenario: &str) {
        let confirmed_uid = self
            .confirmed_rx
            .recv_timeout(TASK_DEADLINE)
            .unwrap_or_else(|error| panic!("{scenario}: confirmation deadline exceeded: {error}"));
        assert_eq!(confirmed_uid, self.commit_uid, "{scenario}: commit UID");
        assert_eq!(self.log.confirm_count(), 1, "{scenario}: confirm count");
        assert_eq!(
            self.confirmed_rx.recv_timeout(NO_CONFIRM_OBSERVATION),
            Err(RecvTimeoutError::Timeout),
            "{scenario}: the root WAL must be confirmed exactly once"
        );
    }
}

/// 验证确认器只在全部合法成功信号到齐后调用一次根 logger。
///
/// 覆盖计数边界 1..=3 的中间状态和最终状态；不使用固定 sleep 作为完成条件。该测试保护
/// CONTRACT-CFM-001 的“成功信号计数”语义，不覆盖真实表文件 I/O。
#[test]
fn test_commit_confirm_waits_for_all_successful_children() {
    let harness = ConfirmHarness::new();
    let confirmer = harness.confirmer(3);

    for _ in 0..2 {
        assert!(confirmer(
            harness.transaction_uid.clone(),
            harness.commit_uid.clone(),
            Ok(())
        )
        .is_ok());
    }
    harness.assert_not_confirmed("first two of three successful children");

    assert!(confirmer(
        harness.transaction_uid.clone(),
        harness.commit_uid.clone(),
        Ok(())
    )
    .is_ok());
    harness.assert_confirmed_once("all three successful children");
}

/// 验证并发成功信号不会丢计数或重复确认。
///
/// 16 个线程通过 barrier 同时调用同一共享确认器，所有调用都必须成功，最终 logger 必须只
/// 收到同一个 commit UID 一次。该测试覆盖 `Arc + AtomicUsize(SeqCst)` 的并发聚合路径。
#[test]
fn test_commit_confirm_concurrent_success_callbacks_confirm_once() {
    const PERSISTENT_CHILD_COUNT: usize = 16;

    let harness = ConfirmHarness::new();
    let confirmer = harness.confirmer(PERSISTENT_CHILD_COUNT);
    let start = Arc::new(Barrier::new(PERSISTENT_CHILD_COUNT));
    let mut handles = Vec::with_capacity(PERSISTENT_CHILD_COUNT);

    for _ in 0..PERSISTENT_CHILD_COUNT {
        let confirmer = confirmer.clone();
        let start = start.clone();
        let transaction_uid = harness.transaction_uid.clone();
        let commit_uid = harness.commit_uid.clone();
        handles.push(thread::spawn(move || {
            start.wait();
            confirmer(transaction_uid, commit_uid, Ok(()))
        }));
    }

    for handle in handles {
        assert!(
            handle
                .join()
                .expect("successful callback thread must not panic")
                .is_ok(),
            "every valid success signal must be accepted"
        );
    }
    harness.assert_confirmed_once("concurrent successful children");
}

/// 验证错误事务身份在修改成功计数之前被拒绝。
///
/// 先分别发送错误 transaction UID 和错误 commit UID，确认均返回错误且 logger 未被调用；
/// 随后发送两个合法成功信号，仍必须在第二个信号后恰好确认一次。这证明身份拒绝没有偷偷
/// 消费计数。错误 UID 是公开 API 明确处理的边界，不等同于向结果参数伪造持久化 `Err`。
#[test]
fn test_commit_confirm_rejects_mismatched_ids_without_consuming_count() {
    let harness = ConfirmHarness::new();
    let confirmer = harness.confirmer(2);

    assert!(
        confirmer(Guid(0xdead), harness.commit_uid.clone(), Ok(())).is_err(),
        "a mismatched transaction UID must be rejected"
    );
    assert!(
        confirmer(harness.transaction_uid.clone(), Guid(0xbeef), Ok(())).is_err(),
        "a mismatched commit UID must be rejected"
    );
    harness.assert_not_confirmed("mismatched transaction identities");

    for _ in 0..2 {
        assert!(confirmer(
            harness.transaction_uid.clone(),
            harness.commit_uid.clone(),
            Ok(())
        )
        .is_ok());
    }
    harness.assert_confirmed_once("valid callbacks after rejected identities");
}
