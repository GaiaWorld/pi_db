//! Btree Key 版本在 overlay、redb collector 与冷启动之间的一致性专项。
//!
//! 本 target 只使用公开数据库 API，并拆成三个独立进程：`setup` 以
//! `enable_accelerated_repair=false` 提交一个版本化写事务，再用合法 `Str -> Usize` 大批次
//! 触发生产 1 MiB Btree collector 阈值；`accelerated-reopen-write` 以 `true` 重开同一 Btree、
//! 验证基线并再次完成版本写和 collector；`inspect-data-only` 最后以 `false` 和全新空 WAL
//! 打开同一数据目录。前两个进程均在数据和根 WAL 确认闭合后直接 `_exit`，不依赖正常析构。
//! 测试据此同时验证：
//!
//! - collector 前 overlay 中的值与 `commit_with_version` 回执严格匹配；
//! - collector 把值写入 redb、清理 overlay 后，不得改变该 Key 的已发布版本；
//! - `false -> true -> false` 三次启动与两次非正常进程退出不改变 Btree 逻辑值；
//! - 冷启动只从 Meta/Btree 数据文件恢复值，Key 版本缓存不会跨进程持久化；
//! - 冷启动后的首次 `query_with_version` 为已存在值生成新的 `Upsert(Guid)`，重复读取稳定；
//! - data-only 检查使用的空 WAL 保持零 append、零 confirm、零 waiting。
//!
//! `enable_accelerated_repair` 是否到达 redb `set_quick_repair` 由生产调用链静态审查证明；本功能
//! target 只证明模式切换兼容性，不把运行时间解释为稳定的恢复或提交性能收益。
//!
//! 根 WAL 自身的磁盘空间、配额、只读文件系统、设备 I/O、runtime 拒绝或文件大小限制失败属于
//! `LIMIT-ROOT-WAL-IO-001`，不在当前事务安全保证内，本 target 不注入也不扩大该边界。

mod key_version_support;

use std::{env, fs,
          future::Future,
          path::{Path, PathBuf},
          process::{Child, Command, ExitStatus},
          thread,
          time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{AsyncRuntime,
                      multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
                      startup_global_time_loop};
use pi_async_transaction::{AsyncCommitLog, Transaction2Pc,
                           manager_2pc::Transaction2PcManager};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{Binary, KVDBTableType, KVTableMeta, TableKeyVersion, Version,
            db::{KVDBManager, KVDBManagerBuilder},
            tables::TableKV,
            utils::CreateTableOptions};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

use key_version_support::{expect_binary, expect_eq};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;

const TEST_NAME: &str = "test_key_version_btree_restart_consistency";
const PHASE_ENV: &str = "PI_DB_KEY_VERSION_BTREE_RESTART_PHASE";
const ROOT_ENV: &str = "PI_DB_KEY_VERSION_BTREE_RESTART_ROOT";
const TABLE_NAME: &str = "key_version_btree_restart";
const UID_FILE: &str = "committed-version-uid";
const ACCELERATED_UID_FILE: &str = "accelerated-committed-version-uid";
const TARGET_INDEX: usize = 137;
const FILLER_KEY_BASE: usize = 10_000;
const ACCELERATED_FILLER_KEY_BASE: usize = 20_000;
const FILLER_KEYS: usize = 320;
const PERSISTED_KEY_BYTES: usize = 4 * 1024;
const BTREE_WAIT_THRESHOLD: usize = 1024 * 1024;
const PROCESS_TIMEOUT: Duration = Duration::from_secs(110);
const SETUP_TIMEOUT: Duration = Duration::from_secs(90);
const ACCELERATED_TIMEOUT: Duration = Duration::from_secs(45);
const INSPECTION_TIMEOUT: Duration = Duration::from_secs(30);
const PERSISTENCE_TIMEOUT: Duration = Duration::from_secs(75);

#[test]
fn test_key_version_btree_restart_consistency() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV)
                .expect("Btree restart child phase must receive its root path"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("Btree restart phase {phase} failed: {error}"));
        return;
    }

    let root = unique_temp_root();
    fs::create_dir_all(&root).expect("creating Btree restart root must succeed");
    for phase in ["setup", "accelerated-reopen-write", "inspect-data-only"] {
        if let Err(error) = run_phase_process(&root, phase, PROCESS_TIMEOUT) {
            panic!(
                "Btree restart consistency failed in phase {phase}; evidence is preserved at {:?}: {error}",
                root,
            );
        }
    }
    fs::remove_dir_all(&root).expect("cleaning Btree restart root must succeed");
}

fn run_child_phase(phase: &str, root: &Path) -> TestResult<()> {
    match phase {
        "setup" => {
            let root = root.to_path_buf();
            run_on_runtime(4, SETUP_TIMEOUT, move |rt| async move {
                phase_setup(rt, root).await
            })
        },
        "accelerated-reopen-write" => {
            let root = root.to_path_buf();
            run_on_runtime(4, ACCELERATED_TIMEOUT, move |rt| async move {
                phase_accelerated_reopen_and_write(rt, root).await
            })
        },
        "inspect-data-only" => {
            let root = root.to_path_buf();
            run_on_runtime(2, INSPECTION_TIMEOUT, move |rt| async move {
                phase_inspect_data_only(rt, root).await
            })
        },
        other => Err(format!("unknown Btree restart phase: {other}")),
    }
}

async fn phase_setup(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let (db, manager, logger) = build_database(&rt,
                                                &root.join("database"),
                                                &root.join("root-wal"),
                                                false).await?;
    create_btree_table(&db).await?;

    let table = Atom::from(TABLE_NAME);
    let target_key = encode_string(persisted_key(TARGET_INDEX));
    let target_value = encode_usize(0x5a17_0137);
    let (initial_value, initial_version) = db
        .query_with_version(table.clone(), target_key.clone())
        .await
        .map_err(|error| format!("loading initial Btree version failed: {error:?}"))?;
    expect_binary("initial Btree value", initial_value.as_ref(), None)?;
    if !matches!(initial_version, Version::Delete(_)) {
        return Err(format!(
            "missing Btree key must have a Delete first-observation version, observed {initial_version:?}",
        ));
    }

    let transaction = db
        .transaction(Atom::from("Btree restart version writer"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected Btree version writer".to_owned())?;
    let prepare = transaction
        .prepare_with_version(
            vec![TableKeyVersion {
                table: table.clone(),
                key: target_key.clone(),
                version: initial_version,
            }],
            vec![TableKV::new(table.clone(),
                              target_key.clone(),
                              Some(target_value.clone()))],
        )
        .await
        .map_err(|error| format!("preparing Btree version writer failed: {error:?}"))?;
    let transaction_uid = transaction
        .get_transaction_uid()
        .ok_or_else(|| "Btree version prepare did not allocate a transaction UID".to_owned())?;
    let receipt = transaction
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("committing Btree version writer failed: {error:?}"))?;
    assert_single_receipt(&receipt, &table, &target_key, &transaction_uid)?;

    let overlay_size = db
        .table_cache_size(&table)
        .await
        .ok_or_else(|| "Btree table disappeared before overlay inspection".to_owned())?;
    if overlay_size == 0 {
        return Err("small Btree version write was not observable in overlay before collector".to_owned());
    }
    assert_query_version(&db,
                         &table,
                         target_key.clone(),
                         &target_value,
                         &Version::Upsert(transaction_uid.clone()),
                         "pre-collector overlay").await?;

    // 小事务的后台任务只需把自身加入 waits；让出一次调度后，大批次会在同一生产队列中触发
    // collector。这里不依赖 60 秒定时器，也不调用表内部构造或私有刷新入口。
    rt.timeout(10).await;
    let filler_bytes = FILLER_KEYS * PERSISTED_KEY_BYTES;
    if filler_bytes <= BTREE_WAIT_THRESHOLD {
        return Err(format!(
            "Btree filler does not exceed the production collector threshold: {filler_bytes}",
        ));
    }
    let filler = db
        .transaction(Atom::from("Btree restart collector trigger"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected Btree collector trigger".to_owned())?;
    filler
        .upsert(
            (0..FILLER_KEYS)
                .map(|index| {
                    TableKV::new(
                        table.clone(),
                        encode_string(persisted_key(FILLER_KEY_BASE + index)),
                        Some(encode_usize(index + 1)),
                    )
                })
                .collect(),
        )
        .await
        .map_err(|error| format!("writing Btree collector filler failed: {error:?}"))?;
    commit_ordinary(&filler, "Btree collector filler").await?;

    // Btree 大批次会立即超过自身 1 MiB waits 阈值；DDL 的 Meta 记录仍按生产 60 秒定时器
    // 持久化。统一等待必须同时看到 overlay 清空和全部根 WAL 确认，才能进入 data-only 冷启动。
    wait_for_btree_persistence(&rt,
                               &db,
                               &logger,
                               &table,
                               PERSISTENCE_TIMEOUT).await?;
    assert_query_version(&db,
                         &table,
                         target_key.clone(),
                         &target_value,
                         &Version::Upsert(transaction_uid.clone()),
                         "post-collector redb").await?;
    assert_manager_totals(&manager, 3, "Btree setup")?;
    expect_eq("Btree setup WAL append count", &logger.append_total_count(), &3usize)?;
    expect_eq("Btree setup WAL confirm count", &logger.confirm_total_count(), &3usize)?;
    expect_eq("Btree setup WAL waiting count", &logger.waiting_confirm_count().await, &0usize)?;

    fs::write(root.join(UID_FILE), transaction_uid.0.to_le_bytes())
        .map_err(|error| format!("writing committed version UID evidence failed: {error}"))?;
    // 所有数据与根 WAL 已确认后直接结束进程，下一阶段不得依赖 redb、runtime 或 manager 析构。
    // SAFETY: 这是父测试创建的专用子进程；证据文件已关闭，业务数据和三个根 WAL 均已确认，
    // 不存在必须由 Rust Drop 才能完成的测试断言，且 `_exit` 正是本专项要模拟的非正常终止边界。
    unsafe { libc::_exit(0) }
}

async fn phase_accelerated_reopen_and_write(
    rt: MultiTaskRuntime<()>,
    root: PathBuf,
) -> TestResult<()> {
    let original_uid = read_uid(&root.join(UID_FILE), "original committed version")?;
    let (db, manager, logger) = build_database(&rt,
                                                &root.join("database"),
                                                &root.join("accelerated-root-wal"),
                                                true).await?;
    expect_eq("accelerated reopen table count", &db.table_size().await, &2usize)?;

    let table = Atom::from(TABLE_NAME);
    let target_key = encode_string(persisted_key(TARGET_INDEX));
    let baseline_value = encode_usize(0x5a17_0137);
    let (loaded_value, loaded_version) = db
        .query_with_version(table.clone(), target_key.clone())
        .await
        .map_err(|error| format!("loading accelerated-reopen Btree version failed: {error:?}"))?;
    expect_binary("accelerated-reopen baseline value",
                  loaded_value.as_ref(),
                  Some(&baseline_value))?;
    let loaded_uid = match &loaded_version {
        Version::Upsert(uid) => uid,
        other => {
            return Err(format!(
                "accelerated reopen of an existing value must create Upsert evidence, observed {other:?}",
            ));
        },
    };
    if loaded_uid.0 == original_uid {
        return Err("accelerated reopen unexpectedly restored the old in-memory version".to_owned());
    }
    expect_eq("accelerated pre-write WAL append count", &logger.append_total_count(), &0usize)?;
    expect_eq("accelerated pre-write WAL confirm count", &logger.confirm_total_count(), &0usize)?;
    expect_eq("accelerated pre-write WAL waiting count",
              &logger.waiting_confirm_count().await,
              &0usize)?;

    let updated_value = encode_usize(0xa551_0137);
    let transaction = db
        .transaction(Atom::from("Btree accelerated version writer"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected accelerated Btree version writer".to_owned())?;
    let prepare = transaction
        .prepare_with_version(
            vec![TableKeyVersion {
                table: table.clone(),
                key: target_key.clone(),
                version: loaded_version,
            }],
            vec![TableKV::new(table.clone(),
                              target_key.clone(),
                              Some(updated_value.clone()))],
        )
        .await
        .map_err(|error| format!("preparing accelerated Btree version writer failed: {error:?}"))?;
    let transaction_uid = transaction
        .get_transaction_uid()
        .ok_or_else(|| "accelerated Btree prepare did not allocate a transaction UID".to_owned())?;
    let receipt = transaction
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("committing accelerated Btree version writer failed: {error:?}"))?;
    assert_single_receipt(&receipt, &table, &target_key, &transaction_uid)?;
    assert_query_version(&db,
                         &table,
                         target_key.clone(),
                         &updated_value,
                         &Version::Upsert(transaction_uid.clone()),
                         "accelerated pre-collector overlay").await?;

    rt.timeout(10).await;
    let filler = db
        .transaction(Atom::from("Btree accelerated collector trigger"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected accelerated Btree collector trigger".to_owned())?;
    filler
        .upsert(
            (0..FILLER_KEYS)
                .map(|index| {
                    TableKV::new(
                        table.clone(),
                        encode_string(persisted_key(ACCELERATED_FILLER_KEY_BASE + index)),
                        Some(encode_usize(index + 1)),
                    )
                })
                .collect(),
        )
        .await
        .map_err(|error| format!("writing accelerated Btree collector filler failed: {error:?}"))?;
    commit_ordinary(&filler, "accelerated Btree collector filler").await?;
    wait_for_btree_persistence(&rt,
                               &db,
                               &logger,
                               &table,
                               PERSISTENCE_TIMEOUT).await?;
    assert_query_version(&db,
                         &table,
                         target_key,
                         &updated_value,
                         &Version::Upsert(transaction_uid.clone()),
                         "accelerated post-collector redb").await?;
    expect_eq("accelerated WAL append count", &logger.append_total_count(), &2usize)?;
    expect_eq("accelerated WAL confirm count", &logger.confirm_total_count(), &2usize)?;
    expect_eq("accelerated WAL waiting count",
              &logger.waiting_confirm_count().await,
              &0usize)?;
    assert_manager_totals(&manager, 2, "accelerated Btree write")?;

    fs::write(root.join(ACCELERATED_UID_FILE), transaction_uid.0.to_le_bytes())
        .map_err(|error| format!("writing accelerated committed UID evidence failed: {error}"))?;
    // 与 setup 相同，数据确认后直接退出，最终 false 模式冷启动必须独立恢复文件状态。
    // SAFETY: 当前专用子进程的两个根事务已消费且 WAL 已确认，证据文件已关闭；故意跳过 Drop
    // 才能验证下一进程不依赖 runtime、manager 或 redb 的正常析构，进程中无其它测试共享状态。
    unsafe { libc::_exit(0) }
}

async fn phase_inspect_data_only(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let committed_uid = read_uid(&root.join(ACCELERATED_UID_FILE),
                                 "accelerated committed version")?;

    let (db, manager, logger) = build_database(&rt,
                                                &root.join("database"),
                                                &root.join("inspection-empty-wal"),
                                                false).await?;
    expect_eq("data-only table count", &db.table_size().await, &2usize)?;
    let table = Atom::from(TABLE_NAME);
    let key = encode_string(persisted_key(TARGET_INDEX));
    let expected_value = encode_usize(0xa551_0137);
    let (value, version) = db
        .query_with_version(table.clone(), key.clone())
        .await
        .map_err(|error| format!("data-only Btree qwv failed: {error:?}"))?;
    expect_binary("data-only Btree value", value.as_ref(), Some(&expected_value))?;
    let fresh_uid = match &version {
        Version::Upsert(uid) => uid,
        other => {
            return Err(format!(
                "existing redb value must rebuild an Upsert first-observation version, observed {other:?}",
            ));
        },
    };
    if fresh_uid.0 == committed_uid {
        return Err("cold startup unexpectedly restored the old in-memory commit version".to_owned());
    }

    let (repeat_value, repeat_version) = db
        .query_with_version(table.clone(), key)
        .await
        .map_err(|error| format!("repeating data-only Btree qwv failed: {error:?}"))?;
    expect_binary("repeated data-only Btree value",
                  repeat_value.as_ref(),
                  Some(&expected_value))?;
    expect_eq("repeated data-only Btree version", &repeat_version, &version)?;
    expect_eq("data-only Btree overlay size",
              &db.table_cache_size(&table).await,
              &Some(0u64))?;
    expect_eq("data-only WAL append count", &logger.append_total_count(), &0usize)?;
    expect_eq("data-only WAL confirm count", &logger.confirm_total_count(), &0usize)?;
    expect_eq("data-only WAL waiting count", &logger.waiting_confirm_count().await, &0usize)?;
    assert_manager_totals(&manager, 0, "data-only Btree inspection")
}

async fn create_btree_table(db: &RealDb) -> TestResult<()> {
    let transaction = db
        .transaction(Atom::from("Btree restart DDL"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected Btree restart DDL".to_owned())?;
    transaction
        .create_table_with_options(
            Atom::from(TABLE_NAME),
            KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                             true,
                             EnumType::Str,
                             EnumType::Usize),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .map_err(|error| format!("creating Btree restart table failed: {error}"))?;
    commit_ordinary(&transaction, "Btree restart DDL").await
}

async fn commit_ordinary(
    transaction: &pi_db::db::KVDBTransaction<usize, CommitLogger>,
    label: &str,
) -> TestResult<()> {
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing {label} failed: {error:?}"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing {label} failed: {error:?}"))
}

async fn assert_query_version(
    db: &RealDb,
    table: &Atom,
    key: Binary,
    expected_value: &Binary,
    expected_version: &Version,
    label: &str,
) -> TestResult<()> {
    let (value, version) = db
        .query_with_version(table.clone(), key)
        .await
        .map_err(|error| format!("{label} qwv failed: {error:?}"))?;
    expect_binary(&format!("{label} value"), value.as_ref(), Some(expected_value))?;
    expect_eq(&format!("{label} version"), &version, expected_version)
}

fn assert_single_receipt(
    receipt: &[TableKeyVersion],
    table: &Atom,
    key: &Binary,
    transaction_uid: &pi_guid::Guid,
) -> TestResult<()> {
    expect_eq("Btree version receipt length", &receipt.len(), &1usize)?;
    expect_eq("Btree version receipt table", &receipt[0].table, table)?;
    if receipt[0].key.as_ref() != key.as_ref() {
        return Err("Btree version receipt key does not match the committed key".to_owned());
    }
    expect_eq("Btree version receipt version",
              &receipt[0].version,
              &Version::Upsert(transaction_uid.clone()))
}

async fn wait_for_btree_persistence(
    rt: &MultiTaskRuntime<()>,
    db: &RealDb,
    logger: &CommitLogger,
    table: &Atom,
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let cache_size = db.table_cache_size(table).await;
        let waiting = logger.waiting_confirm_count().await;
        let appended = logger.append_total_count();
        let confirmed = logger.confirm_total_count();
        if cache_size == Some(0) && waiting == 0 && appended == confirmed {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "Btree data did not persist before {timeout:?}: cache={cache_size:?}, waiting={waiting}, appended={appended}, confirmed={confirmed}",
            ));
        }
        rt.timeout(10).await;
    }
}

async fn build_database(
    rt: &MultiTaskRuntime<()>,
    db_path: &Path,
    wal_path: &Path,
    enable_accelerated_repair: bool,
) -> TestResult<(RealDb, Transaction2PcManager<usize, CommitLogger>, CommitLogger)> {
    fs::create_dir_all(wal_path)
        .map_err(|error| format!("creating WAL path {wal_path:?} failed: {error}"))?;
    let logger = CommitLoggerBuilder::new(rt.clone(), wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .build()
        .await
        .map_err(|error| format!("building CommitLogger at {wal_path:?} failed: {error}"))?;
    let manager = Transaction2PcManager::new(rt.clone(),
                                               GuidGen::new(0, std::process::id() as u16),
                                               logger.clone());
    let db = KVDBManagerBuilder::new(rt.clone(), manager.clone(), db_path)
        .key_version_ttl(Duration::ZERO)
        .key_version_ttl_poll_interval(Duration::ZERO)
        .startup(enable_accelerated_repair)
        .await
        .map_err(|error| format!("starting database at {db_path:?} failed: {error}"))?;
    Ok((db, manager, logger))
}

fn read_uid(path: &Path, label: &str) -> TestResult<u128> {
    let uid_bytes = fs::read(path)
        .map_err(|error| format!("reading {label} evidence at {path:?} failed: {error}"))?;
    let uid_bytes: [u8; 16] = uid_bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| format!("{label} evidence has {} bytes", bytes.len()))?;
    Ok(u128::from_le_bytes(uid_bytes))
}

fn assert_manager_totals(
    manager: &Transaction2PcManager<usize, CommitLogger>,
    expected: usize,
    label: &str,
) -> TestResult<()> {
    expect_eq(&format!("{label} active transaction count"),
              &manager.transaction_len(),
              &0usize)?;
    expect_eq(&format!("{label} produced transaction count"),
              &manager.produced_transaction_total(),
              &expected)?;
    expect_eq(&format!("{label} consumed transaction count"),
              &manager.consumed_transaction_total(),
              &expected)
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn encode_string(value: String) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn persisted_key(index: usize) -> String {
    let prefix = format!("key={index:020};");
    assert!(prefix.len() <= PERSISTED_KEY_BYTES);
    let fill = char::from(b'a' + (index % 26) as u8);
    let mut key = String::with_capacity(PERSISTED_KEY_BYTES);
    key.push_str(&prefix);
    for _ in prefix.len()..PERSISTED_KEY_BYTES {
        key.push(fill);
    }
    key
}

fn run_on_runtime<T, F, Fut>(
    workers: usize,
    timeout: Duration,
    build: F,
) -> TestResult<T>
where
    T: Send + 'static,
    F: FnOnce(MultiTaskRuntime<()>) -> Fut,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    let _time_loop = startup_global_time_loop(1);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(workers)
        .build();
    let future = build(rt.clone());
    let (result_tx, result_rx) = bounded(1);
    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    })
    .map_err(|error| format!("spawning Btree restart future failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("Btree restart future exceeded {timeout:?}: {error}"))?
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
        .map_err(|error| format!("spawning Btree restart phase {phase} failed: {error}"))?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("Btree restart phase {phase} exited with {status}"))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking Btree restart child status failed: {error}"))?
        {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("Btree restart child exceeded {timeout:?}"));
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
        "pi_db_key_version_btree_restart_{}_{}",
        std::process::id(),
        nanos,
    ))
}
