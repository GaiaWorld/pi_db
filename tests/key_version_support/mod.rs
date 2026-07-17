//! Key 版本协议专项共用的真实生产装配夹具。
//!
//! 本模块只减少各独立 target 的 runtime、事务管理器、根 WAL、DDL 和临时目录重复代码。
//! 它不替换数据库、事务、表存储或文件系统，也不暴露生产内部状态；各 target 仍须通过公开
//! API 和独立参考值完成业务断言。

#![allow(dead_code)]

use std::{
    fs,
    future::Future,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::{
    manager_2pc::Transaction2PcManager,
};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    utils::CreateTableOptions,
    Binary, KVDBTableType, KVTableMeta,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

pub type TestResult<T = ()> = Result<T, String>;
pub type RealDb = KVDBManager<usize, CommitLogger>;
pub type RealTransaction = KVDBTransaction<usize, CommitLogger>;
pub type RealTrManager = Transaction2PcManager<usize, CommitLogger>;

pub const META_TABLE: &str = ".tables_meta";
pub const MEMORY_TABLE: &str = "key_version_memory";
pub const LOG_ORDERED_TABLE: &str = "key_version_log_ordered";
pub const BTREE_TABLE: &str = "key_version_btree";

/// 保持数据库、事务管理器和 logger 的同一真实装配。
pub struct Fixture {
    pub db: RealDb,
    pub tr_manager: RealTrManager,
    pub logger: CommitLogger,
}

/// 在独立 4-worker runtime 中执行一个完整 target，并施加同步硬截止。
pub fn run_on_runtime<T, F, Fut>(timeout: Duration, build: F) -> TestResult<T>
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
    .map_err(|error| format!("spawning key-version target failed: {error:?}"))?;

    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("key-version target exceeded {timeout:?}: {error}"))?
}

/// 启动真实数据库；TTL 参数由目标显式给出，避免不同专项共享隐式计时条件。
pub async fn build_database(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
    ttl: Duration,
    poll_interval: Duration,
) -> TestResult<Fixture> {
    fs::create_dir_all(root)
        .map_err(|error| format!("creating key-version fixture root {root:?} failed: {error}"))?;
    let wal_path = root.join("root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building CommitLogger at {wal_path:?} failed: {error}"))?;
    let tr_manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger.clone(),
    );
    let db_path = root.join("database");
    let db = KVDBManagerBuilder::new(rt.clone(), tr_manager.clone(), &db_path)
        .key_version_ttl(ttl)
        .key_version_ttl_poll_interval(poll_interval)
        .startup(false)
        .await
        .map_err(|error| format!("starting key-version database at {db_path:?} failed: {error}"))?;

    Ok(Fixture {
        db,
        tr_manager,
        logger,
    })
}

/// 通过一个真实 DDL 根事务创建当前允许测试的 Memory、LogOrdered 和 Btree 表。
pub async fn create_active_tables(fixture: &Fixture) -> TestResult<()> {
    let transaction = writable_transaction(&fixture.db, "key-version active-table DDL")?;
    transaction
        .create_table(
            Atom::from(MEMORY_TABLE),
            table_meta(KVDBTableType::MemOrdTab, true),
            false,
        )
        .await
        .map_err(|error| format!("creating key-version Memory table failed: {error}"))?;
    transaction
        .create_table_with_options(
            Atom::from(LOG_ORDERED_TABLE),
            table_meta(KVDBTableType::LogOrdTab, true),
            CreateTableOptions::LogOrdTab(64 * 1024 * 1024, 1024 * 1024, 1024 * 1024),
            false,
        )
        .await
        .map_err(|error| format!("creating key-version LogOrdered table failed: {error}"))?;
    transaction
        .create_table_with_options(
            Atom::from(BTREE_TABLE),
            table_meta(KVDBTableType::BtreeOrdTab, true),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .map_err(|error| format!("creating key-version Btree table failed: {error}"))?;
    commit_ordinary(&transaction, "key-version active-table DDL").await?;

    expect_eq("registered table count", &fixture.db.table_size().await, &4usize)
}

pub fn writable_transaction(db: &RealDb, source: &str) -> TestResult<RealTransaction> {
    db.transaction(Atom::from(source), true, 10_000, 10_000)
        .ok_or_else(|| format!("database rejected writable transaction {source}"))
}

pub fn read_only_transaction(db: &RealDb, source: &str) -> TestResult<RealTransaction> {
    db.transaction(Atom::from(source), false, 10_000, 10_000)
        .ok_or_else(|| format!("database rejected read-only transaction {source}"))
}

pub async fn commit_ordinary(transaction: &RealTransaction, label: &str) -> TestResult<()> {
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing {label} failed: {error:?}"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing {label} failed: {error:?}"))
}

pub async fn query_ordinary(
    db: &RealDb,
    table: &str,
    key: Binary,
    label: &str,
) -> TestResult<Option<Binary>> {
    let transaction = read_only_transaction(db, label)?;
    let mut values = transaction
        .query(vec![TableKV::new(Atom::from(table), key, None)])
        .await;
    if values.len() != 1 {
        return Err(format!(
            "{label}: expected one ordinary query slot, observed {}",
            values.len(),
        ));
    }
    Ok(values.pop().expect("ordinary query result length was checked"))
}

pub fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

pub fn encode_atom(value: &str) -> Binary {
    let mut buffer = WriteBuffer::new();
    Atom::from(value).encode(&mut buffer);
    Binary::new(buffer.bytes)
}

pub fn table_meta(table_type: KVDBTableType, persistence: bool) -> KVTableMeta {
    KVTableMeta::new(table_type, persistence, EnumType::Usize, EnumType::Usize)
}

pub fn expect_eq<T: std::fmt::Debug + PartialEq>(
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

pub fn expect_binary(
    label: &str,
    actual: Option<&Binary>,
    expected: Option<&Binary>,
) -> TestResult<()> {
    let equal = match (actual, expected) {
        (None, None) => true,
        (Some(actual), Some(expected)) => actual.as_ref() == expected.as_ref(),
        _ => false,
    };
    if equal {
        Ok(())
    } else {
        Err(format!(
            "{label}: expected {:?}, observed {:?}",
            expected.map(AsRef::<[u8]>::as_ref),
            actual.map(AsRef::<[u8]>::as_ref),
        ))
    }
}

/// 每个 target 使用独立真实目录；Drop 失败不遮蔽业务断言，资源专项另行严格检查。
pub struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    pub fn new(label: &str) -> TestResult<Self> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system time is before UNIX_EPOCH: {error}"))?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "pi_db_key_version_{label}_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path)
            .map_err(|error| format!("creating temporary root {path:?} failed: {error}"))?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
