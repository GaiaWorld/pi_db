//! Key 版本 TTL FIFO 索引的真实生产装配专项测试。
//!
//! 本 target 使用真实 4-worker 数据库 runtime、独立 2-worker 调用 runtime、
//! `Transaction2PcManager`、根 `CommitLogger`、公开 `KVDBManagerBuilder`、真实文件系统和公开
//! DDL 创建的 Memory 表。首批 Key 数量超过两个 scanner 批次；其中一部分通过独立版本事务
//! 更新并刷新 deadline，第二批 Key 则在首批临近到期时由两个 runtime 的 worker 分步插入。
//! 最终逐 Key 验证版本全部更替且数据库值未被 TTL 改变。
//!
//! 测试只通过公开 API 观察结果，不读取内部队列或版本 Map。它证明真实装配下没有 token 丢失、
//! 同轮重复消费导致的误删、已有记录 publication 丢失 deadline 刷新，或 TTL 把版本删除误当成
//! 表数据删除；内部 FIFO 的精确批次和所有权不变量由 `src/key_version.rs` 单元测试固定。方案、
//! Review 与完整证据见 `docs/KEY_VERSION_TTL_FIFO_ACCEPTANCE.md#kv-ttl-fifo-evidence`。

use std::{
    collections::HashMap,
    fs,
    future::Future,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_channel::bounded as async_bounded;
use crossbeam_channel::bounded as sync_bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::{AsyncCommitLog, manager_2pc::Transaction2PcManager};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    Binary, KVDBTableType, KVTableMeta, TableKeyVersion, Version,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const TABLE_NAME: &str = "key_version_ttl_index_memory";
const KEY_VERSION_TTL: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(2);
const INITIAL_KEYS: usize = 640;
const UPDATED_KEYS: usize = 64;
const CONCURRENT_KEYS: usize = 320;
const PRODUCERS: usize = 4;
const UPDATE_DELAY: Duration = Duration::from_millis(800);
const CONCURRENT_WAVE_DELAY: Duration = Duration::from_millis(900);
const PRODUCER_PACE_MILLIS: usize = 1;
const EXPIRY_TIMEOUT: Duration = Duration::from_secs(12);
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

#[test]
fn test_key_version_ttl_fifo_index_under_concurrent_publication() {
    let root = TempRoot::new().expect("creating TTL index test root must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let caller_rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(2)
            .build();
        let fixture = build_database(&rt, &root_path).await?;
        create_memory_table(&fixture.db).await?;
        require(
            fixture.logger.append_total_count() > 0,
            "real Memory DDL did not append through the root CommitLogger",
        )?;
        verify_fifo_index_interleaving(&rt, &caller_rt, &fixture.db).await
    })
    .unwrap_or_else(|error| panic!("key-version TTL FIFO index contract failed: {error}"));
}

async fn verify_fifo_index_interleaving(
    db_rt: &MultiTaskRuntime<()>,
    caller_rt: &MultiTaskRuntime<()>,
    db: &RealDb,
) -> TestResult<()> {
    let table = Atom::from(TABLE_NAME);
    let initial = observe_missing_keys_concurrently(db_rt,
                                                    caller_rt,
                                                    db,
                                                    &table,
                                                    0,
                                                    INITIAL_KEYS,
                                                    false).await?;
    require(initial.len() == INITIAL_KEYS,
            "initial concurrent observation returned an incomplete result set")?;

    db_rt.timeout(UPDATE_DELAY.as_millis() as usize).await;
    let published = publish_existing_keys(db, &table, &initial[..UPDATED_KEYS]).await?;
    require(published.len() == UPDATED_KEYS,
            "version commit returned an incomplete publication receipt")?;

    let mut expectations = Vec::with_capacity(INITIAL_KEYS + CONCURRENT_KEYS);
    let published_by_key: HashMap<Vec<u8>, Version> = published
        .into_iter()
        .map(|item| (item.key.as_ref().to_vec(), item.version))
        .collect();
    require(published_by_key.len() == UPDATED_KEYS,
            "version commit receipt contained duplicate or unexpected keys")?;
    for (index, observed) in initial.into_iter().enumerate() {
        let expected_value = if index < UPDATED_KEYS {
            Some(encode_usize(value_for_key(index)))
        } else {
            None
        };
        let baseline = if index < UPDATED_KEYS {
            published_by_key
                .get(observed.key.as_ref())
                .cloned()
                .ok_or_else(|| format!("version commit receipt omitted updated key {index}"))?
        } else {
            observed.version
        };
        expectations.push(ExpectedEntry {
            key: observed.key,
            baseline,
            expected_value,
        });
    }

    db_rt.timeout(CONCURRENT_WAVE_DELAY.as_millis() as usize).await;
    let concurrent = observe_missing_keys_concurrently(db_rt,
                                                       caller_rt,
                                                       db,
                                                       &table,
                                                       INITIAL_KEYS,
                                                       CONCURRENT_KEYS,
                                                       true).await?;
    require(concurrent.len() == CONCURRENT_KEYS,
            "concurrent near-expiry observation returned an incomplete result set")?;
    expectations.extend(concurrent.into_iter().map(|observed| ExpectedEntry {
        key: observed.key,
        baseline: observed.version,
        expected_value: None,
    }));

    wait_for_all_versions_to_expire(db_rt, db, &table, expectations).await
}

async fn observe_missing_keys_concurrently(
    db_rt: &MultiTaskRuntime<()>,
    caller_rt: &MultiTaskRuntime<()>,
    db: &RealDb,
    table: &Atom,
    start: usize,
    count: usize,
    paced: bool,
) -> TestResult<Vec<ObservedEntry>> {
    let (sender, receiver) = async_bounded(count);
    for producer in 0..PRODUCERS {
        let producer_rt = if producer % 2 == 0 {
            db_rt.clone()
        } else {
            caller_rt.clone()
        };
        let task_rt = producer_rt.clone();
        let task_db = db.clone();
        let task_table = table.clone();
        let task_sender = sender.clone();
        producer_rt.spawn(async move {
            for offset in (producer..count).step_by(PRODUCERS) {
                let index = start + offset;
                let key = encode_usize(index);
                let result = task_db
                    .query_with_version(task_table.clone(), key.clone())
                    .await
                    .map_err(|error| {
                        format!("query_with_version failed for key {index}: {error:?}")
                    })
                    .and_then(|(value, version)| {
                        if value.is_some() {
                            Err(format!("missing key {index} unexpectedly had a value"))
                        } else if !matches!(version, Version::Delete(_)) {
                            Err(format!("missing key {index} returned non-delete version {version:?}"))
                        } else {
                            Ok(ObservedEntry { index, key, version })
                        }
                    });
                if task_sender.send(result).await.is_err() {
                    return;
                }
                if paced {
                    task_rt.timeout(PRODUCER_PACE_MILLIS).await;
                }
            }
        })
        .map_err(|error| format!("spawning observation producer {producer} failed: {error:?}"))?;
    }
    drop(sender);

    let mut observed = Vec::with_capacity(count);
    for _ in 0..count {
        let item = receiver
            .recv()
            .await
            .map_err(|error| format!("observation producer channel closed early: {error}"))??;
        observed.push(item);
    }
    observed.sort_by_key(|item| item.index);
    for (offset, item) in observed.iter().enumerate() {
        require(item.index == start + offset,
                "concurrent observation result set contained a duplicate or missing key")?;
    }
    Ok(observed)
}

async fn publish_existing_keys(
    db: &RealDb,
    table: &Atom,
    observed: &[ObservedEntry],
) -> TestResult<Vec<TableKeyVersion>> {
    let transaction = transaction(db, "TTL index version publication")?;
    let read_set: Vec<TableKeyVersion> = observed
        .iter()
        .map(|item| TableKeyVersion {
            table: table.clone(),
            key: item.key.clone(),
            version: item.version.clone(),
        })
        .collect();
    let write_set: Vec<TableKV> = observed
        .iter()
        .map(|item| TableKV::new(table.clone(),
                                 item.key.clone(),
                                 Some(encode_usize(value_for_key(item.index)))))
        .collect();
    let prepare = transaction
        .prepare_with_version(read_set, write_set)
        .await
        .map_err(|error| format!("preparing TTL index version update failed: {error:?}"))?;
    let published = transaction
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("committing TTL index version update failed: {error:?}"))?;

    for item in &published {
        require(item.table == *table,
                "version commit receipt returned an unexpected table")?;
        require(matches!(item.version, Version::Upsert(_)),
                "version commit receipt returned a non-upsert version")?;
    }
    Ok(published)
}

async fn wait_for_all_versions_to_expire(
    rt: &MultiTaskRuntime<()>,
    db: &RealDb,
    table: &Atom,
    expectations: Vec<ExpectedEntry>,
) -> TestResult<()> {
    let started = Instant::now();
    let mut pending = vec![true; expectations.len()];
    let mut pending_count = expectations.len();

    while pending_count > 0 {
        if started.elapsed() >= EXPIRY_TIMEOUT {
            let pending_keys: Vec<Vec<u8>> = expectations
                .iter()
                .zip(&pending)
                .filter_map(|(item, is_pending)| {
                    if *is_pending {
                        Some(item.key.as_ref().to_vec())
                    } else {
                        None
                    }
                })
                .take(16)
                .collect();
            return Err(format!(
                "{} of {} versions did not expire within {:?}; first pending encoded keys: {:?}",
                pending_count,
                expectations.len(),
                EXPIRY_TIMEOUT,
                pending_keys,
            ));
        }

        for (index, expectation) in expectations.iter().enumerate() {
            if !pending[index] {
                continue;
            }
            let (value, version) = db
                .query_with_version(table.clone(), expectation.key.clone())
                .await
                .map_err(|error| {
                    format!("observing TTL replacement for key {:?} failed: {error:?}",
                            expectation.key.as_ref())
                })?;
            require_binary_option_eq(value.as_ref(), expectation.expected_value.as_ref(),
                                     expectation.key.as_ref())?;
            if version != expectation.baseline {
                if expectation.expected_value.is_some() {
                    require(matches!(version, Version::Upsert(_)),
                            "existing Memory value acquired a non-upsert replacement version")?;
                } else {
                    require(matches!(version, Version::Delete(_)),
                            "missing Memory value acquired a non-delete replacement version")?;
                }
                pending[index] = false;
                pending_count -= 1;
            }
        }
        rt.timeout(1).await;
    }

    Ok(())
}

fn require_binary_option_eq(
    actual: Option<&Binary>,
    expected: Option<&Binary>,
    key: &[u8],
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
            "TTL changed Memory data for encoded key {:?}: actual={:?}, expected={:?}",
            key,
            actual.map(AsRef::<[u8]>::as_ref),
            expected.map(AsRef::<[u8]>::as_ref),
        ))
    }
}

async fn create_memory_table(db: &RealDb) -> TestResult<()> {
    let transaction = transaction(db, "TTL index DDL")?;
    transaction
        .create_table(
            Atom::from(TABLE_NAME),
            KVTableMeta::new(
                KVDBTableType::MemOrdTab,
                false,
                EnumType::Usize,
                EnumType::Usize,
            ),
            false,
        )
        .await
        .map_err(|error| format!("creating Memory table failed: {error}"))?;
    commit_transaction(&transaction, "TTL index DDL").await
}

fn transaction(db: &RealDb, source: &str) -> TestResult<RealTransaction> {
    db.transaction(Atom::from(source), true, 10_000, 10_000)
        .ok_or_else(|| format!("database rejected transaction {source}"))
}

async fn commit_transaction(transaction: &RealTransaction, label: &str) -> TestResult<()> {
    let prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing {label} failed: {error:?}"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing {label} failed: {error:?}"))
}

async fn build_database(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<Fixture> {
    fs::create_dir_all(root)
        .map_err(|error| format!("creating fixture root {root:?} failed: {error}"))?;
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
    let db_path = root.join("database");
    let db = KVDBManagerBuilder::new(rt.clone(), manager, &db_path)
        .key_version_ttl(KEY_VERSION_TTL)
        .key_version_ttl_poll_interval(POLL_INTERVAL)
        .startup(false)
        .await
        .map_err(|error| format!("starting database at {db_path:?} failed: {error}"))?;
    Ok(Fixture { db, logger })
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
    let (result_tx, result_rx) = sync_bounded(1);
    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    })
    .map_err(|error| format!("spawning TTL index test future failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("TTL index test future exceeded {timeout:?}: {error}"))?
}

fn value_for_key(index: usize) -> usize {
    0x5454_0000usize + index
}

fn require(condition: bool, message: &str) -> TestResult<()> {
    if condition {
        Ok(())
    } else {
        Err(message.to_owned())
    }
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

struct ObservedEntry {
    index: usize,
    key: Binary,
    version: Version,
}

struct ExpectedEntry {
    key: Binary,
    baseline: Version,
    expected_value: Option<Binary>,
}

struct Fixture {
    db: RealDb,
    logger: CommitLogger,
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
            "pi_db_key_version_ttl_index_{}_{}",
            std::process::id(),
            nanos,
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
