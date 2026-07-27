//! 离线根 WAL Inspector 的 replay 生命周期专项。
//!
//! 本 target 使用真实多 worker runtime、真实 `CommitLogger`、真实 WAL 文件和生产表 WAL
//! codec，不使用 fake logger 或私有状态探针。它验证：
//!
//! - 空 WAL 的 pull 检查也必须闭合 `start_replay -> finish_replay`，同一 logger 随后能够正常
//!   append/confirm，并可由新 Inspector 再次检查；
//! - callback 检查期间到达的 confirm 会先被真实 logger 缓冲；检查结束通知只能在
//!   `finish_replay` 已清空缓冲并恢复 logger 后发出。
//!
//! Inspector 是专用、离线、单消费者、单实例一次性使用的诊断工具。底层 replay 为建立稳定
//! 遍历边界可以分裂或整理 WAL；本测试不把它描述为物理文件只读，也不允许与在线数据库共享
//! logger。`LogTableInspector` 不进入 CommitLogger replay 状态，因此不属于本生命周期配对。

use std::{
    fmt::Debug,
    fs,
    future::Future,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::{bounded, Receiver};
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::AsyncCommitLog;
use pi_atom::Atom;
use pi_db::{
    inspector::{CommitLogInspector, LogTableInspector},
    tables::{mem_ord_table::MemoryOrderedTable, KVTable},
    Binary,
};
use pi_guid::Guid;
use pi_store::{
    commit_logger::{CommitLogger, CommitLoggerBuilder},
    log_store::log_file::{LogFile, LogMethod},
};

type TestResult<T = ()> = Result<T, String>;
type CodecTable = MemoryOrderedTable<usize, CommitLogger>;

const TEST_TIMEOUT: Duration = Duration::from_secs(30);
const TABLE_NAME: &str = "inspector_lifecycle_memory";
const KEY_BYTES: &[u8] = b"inspector-key";
const VALUE_BYTES: &[u8] = b"inspector-value";
const REMOVED_KEY_BYTES: &[u8] = b"removed-key";

#[test]
fn test_commit_log_inspector_closes_replay_for_pull_and_callback() {
    let root = TempRoot::new().expect("creating Inspector lifecycle root must succeed");
    let _time_loop = startup_global_time_loop(10);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(4)
        .build();

    verify_empty_pull_can_be_repeated(&rt, &root.path().join("pull"))
        .unwrap_or_else(|error| panic!("pull Inspector lifecycle failed: {error}"));
    verify_callback_drains_replay_confirms_before_completion(&rt, &root.path().join("callback"))
        .unwrap_or_else(|error| panic!("callback Inspector lifecycle failed: {error}"));
    verify_log_table_can_be_read_repeatedly(&rt, &root.path().join("log-table"))
        .unwrap_or_else(|error| panic!("LogTableInspector lifecycle failed: {error}"));
}

fn verify_empty_pull_can_be_repeated(
    rt: &MultiTaskRuntime<()>,
    wal_path: &Path,
) -> TestResult<()> {
    let logger = build_logger(rt, wal_path)?;

    for round in 0..2u128 {
        let inspector = CommitLogInspector::new(rt.clone(), logger.clone());
        require(
            inspector.begin(),
            &format!("pull round {round} did not start"),
        )?;
        require(
            inspector.next().is_none(),
            &format!("empty pull round {round} returned a WAL action"),
        )?;

        let commit_uid = Guid(0x7100 + round);
        append_flush_confirm(
            rt,
            &logger,
            commit_uid,
            vec![0x80 + round as u8],
            &format!("pull round {round} post-inspection probe"),
        )?;
        expect_eq(
            &format!("pull round {round} confirmed count"),
            &logger.confirm_total_count(),
            &((round + 1) as usize),
        )?;
        expect_eq(
            &format!("pull round {round} waiting count"),
            &run_async(rt, {
                let logger = logger.clone();
                async move { Ok(logger.waiting_confirm_count().await) }
            })?,
            &0usize,
        )?;
    }

    Ok(())
}

fn verify_callback_drains_replay_confirms_before_completion(
    rt: &MultiTaskRuntime<()>,
    wal_path: &Path,
) -> TestResult<()> {
    let logger = build_logger(rt, wal_path)?;
    let transaction_uid = Guid(0x7201);
    let seed_commit_uid = Guid(0x7202);
    append_and_flush(
        rt,
        &logger,
        seed_commit_uid.clone(),
        valid_prepare_output(transaction_uid.clone()),
        "callback seed WAL",
    )?;

    let (record_tx, record_rx) = bounded(1);
    let (release_tx, release_rx) = bounded(1);
    let (completed_tx, completed_rx) = bounded(1);
    let inspector = CommitLogInspector::new(rt.clone(), logger.clone());
    require(
        inspector.begin_with_callback(move |event| {
            if let Some(record) = event {
                record_tx
                    .send(record)
                    .expect("record observer must remain connected");
                release_rx
                    .recv_timeout(TEST_TIMEOUT)
                    .expect("callback release must arrive before the deadline");
            } else {
                completed_tx
                    .send(())
                    .expect("completion observer must remain connected");
            }
        }),
        "callback Inspector did not start",
    )?;

    let record = recv(&record_rx, "callback WAL record")?;
    expect_eq("callback transaction UID", &record.0, &transaction_uid)?;
    expect_eq("callback commit UID", &record.1, &seed_commit_uid)?;
    expect_eq("callback table", &record.2, &TABLE_NAME.to_owned())?;
    require(
        matches!(record.3, LogMethod::PlainAppend),
        "callback method is not append",
    )?;
    expect_eq("callback key", &record.5, &KEY_BYTES.to_vec())?;
    expect_eq("callback value", &record.6, &VALUE_BYTES.to_vec())?;

    let probe_commit_uid = Guid(0x7203);
    append_flush_confirm(
        rt,
        &logger,
        probe_commit_uid,
        vec![0x91],
        "callback replay confirm probe",
    )?;
    expect_eq(
        "confirm must remain buffered while callback blocks replay",
        &logger.confirm_total_count(),
        &0usize,
    )?;

    release_tx
        .send(())
        .map_err(|error| format!("releasing callback failed: {error}"))?;
    recv(&completed_rx, "callback completion")?;

    expect_eq(
        "completion must follow finish_replay confirm drain",
        &logger.confirm_total_count(),
        &1usize,
    )?;
    expect_eq(
        "only the unconfirmed seed may remain after callback completion",
        &run_async(rt, {
            let logger = logger.clone();
            async move { Ok(logger.waiting_confirm_count().await) }
        })?,
        &1usize,
    )?;

    Ok(())
}

fn verify_log_table_can_be_read_repeatedly(
    rt: &MultiTaskRuntime<()>,
    table_path: &Path,
) -> TestResult<()> {
    let rt_for_log = rt.clone();
    let table_path_for_log = table_path.to_path_buf();
    run_async(rt, async move {
        let log_file = LogFile::open(
            rt_for_log,
            &table_path_for_log,
            8 * 1024,
            2 * 1024 * 1024,
            None,
        )
        .await
        .map_err(|error| {
            format!(
                "opening real table log at {table_path_for_log:?} failed: {error}"
            )
        })?;
        let append_uid = log_file.append(LogMethod::PlainAppend, KEY_BYTES, VALUE_BYTES);
        let remove_uid = log_file.append(LogMethod::Remove, REMOVED_KEY_BYTES, b"ignored");
        log_file
            .delay_commit(append_uid.max(remove_uid), false, 1)
            .await
            .map_err(|error| {
                format!("committing real table log at {table_path_for_log:?} failed: {error}")
            })
    })?;

    let mut expected = vec![
        (true, KEY_BYTES.to_vec(), VALUE_BYTES.to_vec()),
        (false, REMOVED_KEY_BYTES.to_vec(), vec![0]),
    ];
    expected.sort_by(|left, right| left.1.cmp(&right.1));

    for round in 0..2 {
        let inspector = LogTableInspector::new(rt.clone(), table_path.to_path_buf())
            .map_err(|error| {
                format!("constructing LogTableInspector round {round} failed: {error}")
            })?;
        require(
            inspector.begin(),
            &format!("LogTableInspector round {round} did not start"),
        )?;
        let mut observed = Vec::new();
        while let Some((_file, is_upsert, key, value)) = inspector.next() {
            observed.push((is_upsert, key, value));
        }
        observed.sort_by(|left, right| left.1.cmp(&right.1));
        expect_eq(
            &format!("LogTableInspector round {round} actions"),
            &observed,
            &expected,
        )?;
    }

    Ok(())
}

fn valid_prepare_output(transaction_uid: Guid) -> Vec<u8> {
    let table = CodecTable::new(Atom::from(TABLE_NAME), true);
    let key = Binary::new(KEY_BYTES.to_vec());
    let value = Binary::new(VALUE_BYTES.to_vec());
    let mut output = transaction_uid.0.to_le_bytes().to_vec();
    table.init_table_prepare_output(&mut output, 1);
    table.append_key_value_to_table_prepare_output(&mut output, &key, Some(&value));
    output
}

fn build_logger(rt: &MultiTaskRuntime<()>, wal_path: &Path) -> TestResult<CommitLogger> {
    let rt_for_build = rt.clone();
    let wal_path = wal_path.to_path_buf();
    run_async(rt, async move {
        CommitLoggerBuilder::new(rt_for_build, &wal_path)
            .log_file_limit(64 * 1024 * 1024)
            .collect_interval(5 * 60 * 1000)
            .build()
            .await
            .map_err(|error| format!("building CommitLogger at {wal_path:?} failed: {error}"))
    })
}

fn append_and_flush(
    rt: &MultiTaskRuntime<()>,
    logger: &CommitLogger,
    commit_uid: Guid,
    payload: Vec<u8>,
    label: &str,
) -> TestResult<()> {
    let logger = logger.clone();
    let label = label.to_owned();
    run_async(rt, async move {
        let handle = logger
            .append(commit_uid, payload)
            .await
            .map_err(|error| format!("appending {label} failed: {error}"))?;
        logger
            .flush(handle)
            .await
            .map_err(|error| format!("flushing {label} failed: {error}"))
    })
}

fn append_flush_confirm(
    rt: &MultiTaskRuntime<()>,
    logger: &CommitLogger,
    commit_uid: Guid,
    payload: Vec<u8>,
    label: &str,
) -> TestResult<()> {
    let logger = logger.clone();
    let label = label.to_owned();
    run_async(rt, async move {
        let handle = logger
            .append(commit_uid.clone(), payload)
            .await
            .map_err(|error| format!("appending {label} failed: {error}"))?;
        logger
            .flush(handle)
            .await
            .map_err(|error| format!("flushing {label} failed: {error}"))?;
        logger
            .confirm(commit_uid)
            .await
            .map_err(|error| format!("confirming {label} failed: {error}"))
    })
}

fn run_async<T, Fut>(rt: &MultiTaskRuntime<()>, future: Fut) -> TestResult<T>
where
    T: Send + 'static,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    let (result_tx, result_rx) = bounded(1);
    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    })
    .map_err(|error| format!("spawning Inspector lifecycle task failed: {error:?}"))?;
    result_rx
        .recv_timeout(TEST_TIMEOUT)
        .map_err(|error| format!("Inspector lifecycle task exceeded {TEST_TIMEOUT:?}: {error}"))?
}

fn recv<T>(receiver: &Receiver<T>, label: &str) -> TestResult<T> {
    receiver
        .recv_timeout(TEST_TIMEOUT)
        .map_err(|error| format!("waiting for {label} exceeded {TEST_TIMEOUT:?}: {error}"))
}

fn expect_eq<T: Debug + PartialEq>(label: &str, actual: &T, expected: &T) -> TestResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{label}: expected {expected:?}, observed {actual:?}"
        ))
    }
}

fn require(condition: bool, message: &str) -> TestResult<()> {
    if condition {
        Ok(())
    } else {
        Err(message.to_owned())
    }
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new() -> TestResult<Self> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system time is before UNIX_EPOCH: {error}"))?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "pi_db_inspector_replay_lifecycle_{}_{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&path)
            .map_err(|error| format!("creating temporary root {path:?} failed: {error}"))?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
