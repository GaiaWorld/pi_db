//! `KVDBManagerBuilder` 的 Key 版本 TTL 启动配置专项。
//!
//! 本 target 使用真实 4-worker runtime、`Transaction2PcManager`、`CommitLogger`、文件系统、
//! 内部 Meta 表和公开 `query_with_version`，验证两个合法公开边界：
//!
//! - TTL 开启且轮询间隔为 ZERO 时，startup 返回 `InvalidInput`，并且数据库目录、表、任务、
//!   manager 和根 WAL 均没有副作用；
//! - TTL 为 ZERO 时完全关闭淘汰，轮询间隔包括 ZERO 在内均被忽略，startup 正常完成，同一
//!   缺失 Meta Key 的首次观察版本在等待后保持不变。
//!
//! 本测试不注入损坏路径、Meta 或 WAL，不对 startup 的其它错误/panic 策略作结论。正式证据见
//! `docs/MANAGER_STARTUP_CONFIGURATION_ACCEPTANCE.md#manager-startup-config-index`。

use std::{
    fs,
    future::Future,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::{manager_2pc::Transaction2PcManager, AsyncCommitLog};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{db::KVDBManagerBuilder, Binary};
use pi_guid::GuidGen;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;

const META_TABLE: &str = ".tables_meta";
const TEST_TIMEOUT: Duration = Duration::from_secs(30);
const NO_TTL_OBSERVATION: Duration = Duration::from_millis(25);

#[test]
fn test_manager_startup_key_version_ttl_configuration_contract() {
    let root = TempRoot::new().expect("creating startup configuration root must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let (manager, logger) = build_transaction_manager(&rt, &root_path).await?;
        let db_path = root_path.join("database");
        require(!db_path.exists(), "database path existed before startup")?;

        let invalid = KVDBManagerBuilder::new(rt.clone(), manager.clone(), &db_path)
            .key_version_ttl(Duration::from_secs(1))
            .key_version_ttl_poll_interval(Duration::ZERO)
            .startup(false)
            .await;
        let error = match invalid {
            Ok(_) => return Err("enabled TTL with a zero poll interval unexpectedly started".to_owned()),
            Err(error) => error,
        };
        require(
            error.kind() == ErrorKind::InvalidInput,
            &format!("invalid TTL configuration returned {:?}: {error}", error.kind()),
        )?;
        require(!db_path.exists(), "invalid TTL configuration created the database root")?;
        require(
            !db_path.join(META_TABLE).exists(),
            "invalid TTL configuration created the Meta directory",
        )?;
        require(
            !db_path.join(".tables").exists(),
            "invalid TTL configuration created the user-table directory",
        )?;
        assert_no_transaction_or_wal_side_effects(&manager, &logger).await?;

        let db = KVDBManagerBuilder::new(rt.clone(), manager.clone(), &db_path)
            .key_version_ttl(Duration::ZERO)
            .key_version_ttl_poll_interval(Duration::ZERO)
            .startup(false)
            .await
            .map_err(|error| format!("disabled TTL with zero poll interval failed: {error}"))?;
        require(db.db_path() == db_path, "successful startup changed the lexical database path")?;
        require(db.tables_meta_path().is_dir(), "successful startup omitted the Meta directory")?;
        require(db.tables_path().is_dir(), "successful startup omitted the user-table directory")?;

        let key = encode_atom("manager_startup_ttl_disabled_missing_key");
        let (initial_value, initial_version) = db
            .query_with_version(Atom::from(META_TABLE), key.clone())
            .await
            .map_err(|error| format!("initial disabled-TTL query failed: {error:?}"))?;
        require(initial_value.is_none(), "missing Meta Key unexpectedly had a value")?;
        rt.timeout(NO_TTL_OBSERVATION.as_millis() as usize).await;
        let (later_value, later_version) = db
            .query_with_version(Atom::from(META_TABLE), key)
            .await
            .map_err(|error| format!("repeated disabled-TTL query failed: {error:?}"))?;
        require(later_value.is_none(), "missing Meta Key appeared during disabled TTL")?;
        require(
            later_version == initial_version,
            "disabled TTL replaced a cached first-observation version",
        )?;
        assert_no_transaction_or_wal_side_effects(&manager, &logger).await?;
        db.close();
        Ok(())
    })
    .unwrap_or_else(|error| panic!("manager startup configuration contract failed: {error}"));
}

async fn assert_no_transaction_or_wal_side_effects(
    manager: &Transaction2PcManager<usize, CommitLogger>,
    logger: &CommitLogger,
) -> TestResult<()> {
    require(manager.transaction_len() == 0, "startup configuration registered a transaction")?;
    require(
        manager.produced_transaction_total() == 0,
        "startup configuration produced a managed transaction",
    )?;
    require(
        manager.consumed_transaction_total() == 0,
        "startup configuration consumed an unexpected managed transaction",
    )?;
    require(logger.append_total_count() == 0, "startup configuration appended root WAL")?;
    require(logger.confirm_total_count() == 0, "startup configuration confirmed root WAL")?;
    require(
        logger.waiting_confirm_count().await == 0,
        "startup configuration left root WAL waiting for confirmation",
    )
}

async fn build_transaction_manager(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
) -> TestResult<(Transaction2PcManager<usize, CommitLogger>, CommitLogger)> {
    let wal_path = root.join("root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building CommitLogger at {wal_path:?} failed: {error}"))?;
    let manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger.clone(),
    );
    Ok((manager, logger))
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
    .map_err(|error| format!("spawning startup configuration future failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("startup configuration exceeded {timeout:?}: {error}"))?
}

fn encode_atom(value: &str) -> Binary {
    let mut buffer = WriteBuffer::new();
    Atom::from(value).encode(&mut buffer);
    Binary::new(buffer.bytes)
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
    fn new() -> std::io::Result<Self> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "pi_db_manager_startup_configuration_{}_{}",
            std::process::id(),
            nonce,
        ));
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.path) {
            eprintln!("failed to remove startup configuration root {:?}: {error}", self.path);
        }
    }
}
