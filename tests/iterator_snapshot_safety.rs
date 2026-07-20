//! 有序流在创建事务内继续写入时的稳定快照与内存安全专项。
//!
//! 本 target 不运行或引用旧测试。它只通过公开 `KVDBManager -> KVDBTransaction` 生产调用
//! 链访问真实 Memory、Meta、LogOrdered、LogWrite 与 Btree 表，并使用真实
//! `MultiTaskRuntime`、`Transaction2PcManager`、`CommitLogger`、redb、日志文件和文件系统。
//!
//! 冻结的合法调用域如下：
//!
//! - 创建流的根事务必须存活到流迭代结束；
//! - 该事务存活期间允许它自身继续 `upsert/delete`；
//! - 流必须严格观察创建瞬间的有序快照，但不承诺可串行化、提交/回滚原子绑定或其它
//!   事务安全语义；
//! - 事务释放后继续使用流是非法输入，本测试刻意不构造该场景。
//!
//! 父测试在当前测试二进制的独立子进程中运行每个场景，并施加硬截止。矩阵覆盖：原红
//! Memory `keys/values`、空表/单项/方向/包含边界、LogOrdered、Btree 纯 overlay 与已落入
//! redb 的基线、首次 poll 前修改、其它事务提交、注册表移除、确定性跨任务交错、0/1/中段
//! 取消、三类 COW 表的 `Weak` 精确释放、跨线程/跨运行时消费与取消、公开 DDL 驱动的
//! Meta 修改、LogWrite 现状非回归，以及 debug/release/sanitizer 可复用的重复压力。断言
//! 覆盖数量、顺序、键、值、实际状态变化、旧根强引用消失和资源释放后的可继续使用；任何
//! crash、abort、timeout、解码错误、集合漂移、资源未释放或测试安排未真正发生都构成失败。
//!
//! Btree 磁盘场景使用类型合法且超过 1 MiB 生产刷新阈值的 `Usize -> Str` 提交，在提交后
//! 等待公开缓存字节数归零，并由新事务精确反查全部值，以证明基线来自 redb 而不是
//! overlay；它不依赖 60 秒定时刷新或私有测试注入。矩阵不构造事务释放后继续 poll，也不
//! 把 LogWrite 空哨兵认可为最终设计。若任一合法场景复现问题，应按 FIND-ITER-001 门禁
//! 停止并回到根因/方案。
//!
//! 关联生产入口：`pi_db::db::KVDBTransaction::{keys,values,upsert,delete}`、
//! `pi_db::tables::mem_ord_table::MemOrdTabTr`、`pi_ordmap::IterTree`。
//!
//! 本地双向入口：`docs/SEMANTIC_CONTRACTS.md#contract-iter-001`、
//! `docs/REVIEW_FINDINGS.md#find-iter-001-next-experiment`、
//! `docs/TEST_AND_BENCHMARK_STRATEGY.md#test-iterator-special`。

use std::{
    collections::BTreeMap,
    env, fs,
    future::Future,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    sync::{Arc, Weak},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use futures::StreamExt;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::manager_2pc::Transaction2PcManager;
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

const TEST_NAME: &str = "test_iterator_snapshot_safety_matrix";
const PHASE_ENV: &str = "PI_DB_ITERATOR_SNAPSHOT_PHASE";
const ROOT_ENV: &str = "PI_DB_ITERATOR_SNAPSHOT_ROOT";

const TABLE_NAME: &str = "iterator_snapshot_memory";
const BOUNDARY_TABLE_NAME: &str = "iterator_snapshot_boundaries";
const LOG_TABLE_NAME: &str = "iterator_snapshot_log";
const BTREE_TABLE_NAME: &str = "iterator_snapshot_btree";
const OTHER_TRANSACTION_TABLE_NAME: &str = "iterator_snapshot_other_transaction";
const RELEASE_TABLE_NAME: &str = "iterator_snapshot_release";
const CONCURRENT_TABLE_NAME: &str = "iterator_snapshot_concurrent";
const CANCELLATION_MEMORY_TABLE_NAME: &str = "iterator_snapshot_cancel_memory";
const CANCELLATION_BTREE_TABLE_NAME: &str = "iterator_snapshot_cancel_btree";
const LOG_WRITE_TABLE_NAME: &str = "iterator_snapshot_log_write";
const META_TABLE_NAME: &str = ".tables_meta";
const META_SNAPSHOT_TABLE_NAME: &str = "iterator_snapshot_meta_before";
const META_REPLACEMENT_TABLE_NAME: &str = "iterator_snapshot_meta_after";
const STRESS_TABLE_NAME: &str = "iterator_snapshot_stress";
const LIFECYCLE_MEMORY_TABLE_NAME: &str = "iterator_lifecycle_memory";
const LIFECYCLE_LOG_TABLE_NAME: &str = "iterator_lifecycle_log";
const LIFECYCLE_BTREE_TABLE_NAME: &str = "iterator_lifecycle_btree";
const CROSS_RUNTIME_MEMORY_TABLE_NAME: &str = "iterator_cross_runtime_memory";
const CROSS_RUNTIME_BTREE_TABLE_NAME: &str = "iterator_cross_runtime_btree";
const SNAPSHOT_LEN: usize = 2_048;
const MATRIX_LEN: usize = 512;
const PERSISTED_LEN: usize = 320;
const PERSISTED_VALUE_BYTES: usize = 4 * 1024;
const MUTATION_ROUNDS: usize = 4;
const REPLACEMENT_STRIDE: usize = 100_000;
const CHILD_TIMEOUT: Duration = Duration::from_secs(45);
const RUNTIME_TIMEOUT: Duration = Duration::from_secs(35);

#[derive(Clone, Copy, Debug)]
enum StreamKind {
    Keys,
    Values,
}

#[derive(Clone, Copy, Debug)]
enum ExitPoint {
    ZeroPoll,
    OneItem,
    MidStream,
    Exhausted,
}

#[derive(Clone, Copy, Debug)]
struct Scenario {
    kind: StreamKind,
    mutate_after_creation: bool,
}

impl Scenario {
    fn parse(phase: &str) -> TestResult<Self> {
        match phase {
            "keys-control" => Ok(Self {
                kind: StreamKind::Keys,
                mutate_after_creation: false,
            }),
            "values-control" => Ok(Self {
                kind: StreamKind::Values,
                mutate_after_creation: false,
            }),
            "keys-same-transaction-mutation" => Ok(Self {
                kind: StreamKind::Keys,
                mutate_after_creation: true,
            }),
            "values-same-transaction-mutation" => Ok(Self {
                kind: StreamKind::Values,
                mutate_after_creation: true,
            }),
            _ => Err(format!("unknown iterator snapshot phase: {phase}")),
        }
    }
}

/// 删除当前私有根中的全部数据，再反复插入和删除互不相同的数据集。
///
/// 这不是随机压力：每一轮都确定性地让 `root_mut` 离开前一棵 COW 根，最终数据集也与流
/// 的创建时快照完全不相交。返回最终数据集的 key offset，供流结束后的事务存活正证明使用。
async fn replace_private_root(transaction: &RealTransaction) -> TestResult<usize> {
    replace_private_root_for(transaction, TABLE_NAME, SNAPSHOT_LEN, MUTATION_ROUNDS).await
}

/// 对指定表执行确定性的整根替换压力，并返回最终数据集 offset。
async fn replace_private_root_for(
    transaction: &RealTransaction,
    table_name: &str,
    len: usize,
    mutation_rounds: usize,
) -> TestResult<usize> {
    let mut current_offset = 0;

    for round in 0..mutation_rounds {
        let deletes = (0..len)
            .map(|index| {
                TableKV::new(
                    Atom::from(table_name),
                    encode_usize(current_offset + index),
                    None,
                )
            })
            .collect();
        let deleted = transaction
            .delete(deletes)
            .await
            .map_err(|error| format!("mutation round {round} delete failed: {error:?}"))?;
        if deleted.len() != len {
            return Err(format!(
                "mutation round {round} returned {} delete slots instead of {len}",
                deleted.len()
            ));
        }

        current_offset = (round + 1) * REPLACEMENT_STRIDE;
        transaction
            .upsert(dataset_for(table_name, current_offset, len))
            .await
            .map_err(|error| format!("mutation round {round} upsert failed: {error:?}"))?;
    }

    Ok(current_offset)
}

/// 在真实公开事务路径上运行一个正对照或同事务改写场景。
async fn exercise_scenario(db: &RealDb, scenario: Scenario) -> TestResult<()> {
    let transaction = db
        .transaction(
            Atom::from("iterator snapshot scenario"),
            true,
            10_000,
            10_000,
        )
        .ok_or_else(|| "database rejected the iterator snapshot transaction".to_owned())?;

    transaction
        .upsert(dataset(0))
        .await
        .map_err(|error| format!("creating the transaction-private snapshot failed: {error:?}"))?;

    let final_offset = match scenario.kind {
        StreamKind::Keys => {
            let mut stream = transaction
                .keys(Atom::from(TABLE_NAME), None, false)
                .await
                .ok_or_else(|| "keys returned no stream for the Memory table".to_owned())?;

            let final_offset = if scenario.mutate_after_creation {
                replace_private_root(&transaction).await?
            } else {
                0
            };

            let mut observed = Vec::with_capacity(SNAPSHOT_LEN);
            while let Some(key) = stream.next().await {
                observed.push(decode_usize(&key)?);
            }
            drop(stream);

            let expected: Vec<_> = (0..SNAPSHOT_LEN).collect();
            assert_exact_sequence("keys", &expected, &observed)?;
            final_offset
        }
        StreamKind::Values => {
            let mut stream = transaction
                .values(Atom::from(TABLE_NAME), None, false)
                .await
                .ok_or_else(|| "values returned no stream for the Memory table".to_owned())?;

            let final_offset = if scenario.mutate_after_creation {
                replace_private_root(&transaction).await?
            } else {
                0
            };

            let mut observed = Vec::with_capacity(SNAPSHOT_LEN);
            while let Some((key, value)) = stream.next().await {
                observed.push((decode_usize(&key)?, decode_usize(&value)?));
            }
            drop(stream);

            let expected: Vec<_> = (0..SNAPSHOT_LEN)
                .map(|key| (key, value_for_key(key)))
                .collect();
            assert_exact_sequence("values", &expected, &observed)?;
            final_offset
        }
    };

    // 在流已经耗尽并释放后再次使用同一事务，客观证明 owner 覆盖了整个迭代过程。
    let mut probe = transaction
        .query(vec![TableKV::new(
            Atom::from(TABLE_NAME),
            encode_usize(final_offset),
            None,
        )])
        .await;
    if probe.len() != 1 {
        return Err(format!(
            "owner-liveness probe returned {} slots instead of one",
            probe.len()
        ));
    }
    let observed_probe = probe.remove(0).ok_or_else(|| {
        "owner-liveness probe did not find the transaction's current root".to_owned()
    })?;
    let expected_probe = value_for_key(final_offset);
    let observed_probe = decode_usize(&observed_probe)?;
    if observed_probe != expected_probe {
        return Err(format!(
            "owner-liveness probe mismatch: expected {expected_probe}, observed {observed_probe}"
        ));
    }

    let prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing iterator scenario failed: {error:?}"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing iterator scenario failed: {error:?}"))?;

    println!(
        "iterator snapshot evidence: scenario={scenario:?}, snapshot_len={SNAPSHOT_LEN}, final_offset={final_offset}"
    );
    Ok(())
}

fn dataset(offset: usize) -> Vec<TableKV> {
    dataset_for(TABLE_NAME, offset, SNAPSHOT_LEN)
}

fn dataset_for(table_name: &str, offset: usize, len: usize) -> Vec<TableKV> {
    (0..len)
        .map(|index| {
            let key = offset + index;
            TableKV::new(
                Atom::from(table_name),
                encode_usize(key),
                Some(encode_usize(value_for_key(key))),
            )
        })
        .collect()
}

/// 构造可由测试调用方通过 `Weak` 精确观察释放时点的合法 BON usize 值。
fn tracked_dataset(
    table_name: &str,
    offset: usize,
    len: usize,
) -> (Vec<TableKV>, Vec<Weak<Vec<u8>>>) {
    let mut dataset = Vec::with_capacity(len);
    let mut probes = Vec::with_capacity(len);
    for index in 0..len {
        let key = offset + index;
        let shared: Arc<Vec<u8>> = encode_usize(value_for_key(key)).to_shared();
        probes.push(Arc::downgrade(&shared));
        dataset.push(TableKV::new(
            Atom::from(table_name),
            encode_usize(key),
            Some(Binary::from_shared(shared)),
        ));
    }
    (dataset, probes)
}

fn assert_tracked_values_alive(label: &str, probes: &[Weak<Vec<u8>>]) -> TestResult<()> {
    if let Some(index) = probes.iter().position(|probe| probe.upgrade().is_none()) {
        return Err(format!(
            "{label}: snapshot released tracked value {index} before stream exit"
        ));
    }
    Ok(())
}

fn assert_tracked_values_released(label: &str, probes: &[Weak<Vec<u8>>]) -> TestResult<()> {
    let retained: Vec<_> = probes
        .iter()
        .enumerate()
        .filter_map(|(index, probe)| probe.upgrade().is_some().then_some(index))
        .collect();
    if !retained.is_empty() {
        return Err(format!(
            "{label}: {} tracked values remain strongly referenced after stream exit; first retained indexes: {:?}",
            retained.len(),
            &retained[..retained.len().min(8)]
        ));
    }
    Ok(())
}

fn value_for_key(key: usize) -> usize {
    key.checked_mul(3)
        .and_then(|value| value.checked_add(7))
        .expect("test keys must remain within usize")
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn decode_usize(value: &Binary) -> TestResult<usize> {
    let mut buffer = ReadBuffer::new(value.as_ref(), 0);
    usize::decode(&mut buffer)
        .map_err(|error| format!("decoding BON usize from iterator output failed: {error:?}"))
}

fn encode_string(value: String) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn decode_string(value: &Binary) -> TestResult<String> {
    let mut buffer = ReadBuffer::new(value.as_ref(), 0);
    String::decode(&mut buffer)
        .map_err(|error| format!("decoding BON string from iterator output failed: {error:?}"))
}

/// 生成固定长度且与 key 绑定的合法字符串，避免只用重复值弱化值流断言。
fn persisted_value_for_key(key: usize) -> String {
    let prefix = format!("key={key:020};");
    assert!(prefix.len() <= PERSISTED_VALUE_BYTES);
    let fill = char::from(b'a' + (key % 26) as u8);
    let mut value = String::with_capacity(PERSISTED_VALUE_BYTES);
    value.push_str(&prefix);
    for _ in prefix.len()..PERSISTED_VALUE_BYTES {
        value.push(fill);
    }
    value
}

fn persisted_dataset(offset: usize, len: usize) -> Vec<TableKV> {
    (0..len)
        .map(|index| {
            let key = offset + index;
            TableKV::new(
                Atom::from(BTREE_TABLE_NAME),
                encode_usize(key),
                Some(encode_string(persisted_value_for_key(key))),
            )
        })
        .collect()
}

/// 比较完整序列并报告首个差异；数量、顺序、缺项和新增项由同一断言覆盖。
fn assert_exact_sequence<T>(label: &str, expected: &[T], observed: &[T]) -> TestResult<()>
where
    T: std::fmt::Debug + PartialEq,
{
    if expected == observed {
        return Ok(());
    }

    let first_difference = expected
        .iter()
        .zip(observed.iter())
        .position(|(expected, observed)| expected != observed)
        .or_else(|| {
            (expected.len() != observed.len()).then_some(expected.len().min(observed.len()))
        });
    Err(format!(
        "{label} snapshot mismatch: expected_len={}, observed_len={}, first_difference={first_difference:?}, expected_at_difference={:?}, observed_at_difference={:?}",
        expected.len(),
        observed.len(),
        first_difference.and_then(|index| expected.get(index)),
        first_difference.and_then(|index| observed.get(index)),
    ))
}

async fn build_database(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<RealDb> {
    fs::create_dir_all(root)
        .map_err(|error| format!("creating child root {root:?} failed: {error}"))?;
    let wal_path = root.join("root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .build()
        .await
        .map_err(|error| format!("building CommitLogger at {wal_path:?} failed: {error}"))?;
    let manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger,
    );
    KVDBManagerBuilder::new(rt.clone(), manager, root.join("database"))
        .startup(false)
        .await
        .map_err(|error| format!("starting iterator snapshot database failed: {error}"))
}

async fn create_memory_table(db: &RealDb) -> TestResult<()> {
    let transaction = db
        .transaction(Atom::from("iterator snapshot DDL"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected the iterator snapshot DDL transaction".to_owned())?;
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
    let prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing Memory table creation failed: {error:?}"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing Memory table creation failed: {error:?}"))
}

/// 通过公开 DDL 创建矩阵所需表，并正常完成元信息事务。
async fn create_test_table(
    db: &RealDb,
    table_name: &str,
    table_type: KVDBTableType,
    persistence: bool,
) -> TestResult<()> {
    create_typed_test_table(
        db,
        table_name,
        table_type,
        persistence,
        EnumType::Usize,
        EnumType::Usize,
    )
    .await
}

/// 通过公开 DDL 创建带显式键值类型的测试表。
async fn create_typed_test_table(
    db: &RealDb,
    table_name: &str,
    table_type: KVDBTableType,
    persistence: bool,
    key_type: EnumType,
    value_type: EnumType,
) -> TestResult<()> {
    let transaction = db
        .transaction(
            Atom::from("iterator snapshot matrix DDL"),
            true,
            10_000,
            10_000,
        )
        .ok_or_else(|| format!("database rejected DDL for {table_name}"))?;
    let meta = KVTableMeta::new(table_type.clone(), persistence, key_type, value_type);
    let create_result = match table_type {
        KVDBTableType::LogOrdTab => {
            transaction
                .create_table_with_options(
                    Atom::from(table_name),
                    meta,
                    CreateTableOptions::LogOrdTab(64 * 1024 * 1024, 1024 * 1024, 1024 * 1024),
                    false,
                )
                .await
        }
        KVDBTableType::BtreeOrdTab => {
            transaction
                .create_table_with_options(
                    Atom::from(table_name),
                    meta,
                    CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
                    false,
                )
                .await
        }
        _ => {
            transaction
                .create_table(Atom::from(table_name), meta, false)
                .await
        }
    };
    create_result.map_err(|error| format!("creating {table_name} failed: {error}"))?;
    let prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing DDL for {table_name} failed: {error:?}"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing DDL for {table_name} failed: {error:?}"))
}

async fn collect_keys(
    transaction: &RealTransaction,
    table_name: &str,
    start: Option<usize>,
    descending: bool,
) -> TestResult<Vec<usize>> {
    let mut stream = transaction
        .keys(Atom::from(table_name), start.map(encode_usize), descending)
        .await
        .ok_or_else(|| format!("keys returned no stream for {table_name}"))?;
    let mut observed = Vec::new();
    while let Some(key) = stream.next().await {
        observed.push(decode_usize(&key)?);
    }
    Ok(observed)
}

async fn collect_values(
    transaction: &RealTransaction,
    table_name: &str,
    start: Option<usize>,
    descending: bool,
) -> TestResult<Vec<(usize, usize)>> {
    let mut stream = transaction
        .values(Atom::from(table_name), start.map(encode_usize), descending)
        .await
        .ok_or_else(|| format!("values returned no stream for {table_name}"))?;
    let mut observed = Vec::new();
    while let Some((key, value)) = stream.next().await {
        observed.push((decode_usize(&key)?, decode_usize(&value)?));
    }
    Ok(observed)
}

async fn collect_string_values(
    transaction: &RealTransaction,
    table_name: &str,
    start: Option<usize>,
    descending: bool,
) -> TestResult<Vec<(usize, String)>> {
    let mut stream = transaction
        .values(Atom::from(table_name), start.map(encode_usize), descending)
        .await
        .ok_or_else(|| format!("values returned no stream for {table_name}"))?;
    let mut observed = Vec::new();
    while let Some((key, value)) = stream.next().await {
        observed.push((decode_usize(&key)?, decode_string(&value)?));
    }
    Ok(observed)
}

/// 放弃仍处于 `Start` 的事务。
///
/// `rollback_modified` 只处理 Action/Prepare/LogCommit 失败后的恢复；普通未预提交事务尚未
/// 进入 2PC 管理器，其合法取消方式是释放 owner，让私有 COW 根和子事务一并析构。
fn discard_start_transaction(transaction: RealTransaction, _label: &str) {
    drop(transaction);
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

/// Btree 只有在 redb 提交成功并清理 overlay 后缓存字节数才归零。
async fn wait_for_empty_cache(
    rt: &MultiTaskRuntime<()>,
    db: &RealDb,
    table_name: &str,
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        match db.table_cache_size(&Atom::from(table_name)).await {
            Some(0) => return Ok(()),
            Some(size) if Instant::now() < deadline => {
                let _ = size;
                rt.timeout(10).await;
            }
            Some(size) => {
                return Err(format!(
                    "{table_name} cache remained at {size} bytes after {timeout:?}"
                ));
            }
            None => {
                return Err(format!(
                    "{table_name} disappeared while waiting for persistence"
                ))
            }
        }
    }
}

/// 覆盖空表、单项、方向以及存在/不存在起始 key 的包含边界。
async fn exercise_memory_boundaries(db: &RealDb) -> TestResult<()> {
    create_test_table(db, BOUNDARY_TABLE_NAME, KVDBTableType::MemOrdTab, false).await?;
    let transaction = db
        .transaction(Atom::from("iterator boundary matrix"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected boundary transaction".to_owned())?;

    for descending in [false, true] {
        assert_exact_sequence(
            "empty keys",
            &Vec::<usize>::new(),
            &collect_keys(&transaction, BOUNDARY_TABLE_NAME, None, descending).await?,
        )?;
        assert_exact_sequence(
            "empty values",
            &Vec::<(usize, usize)>::new(),
            &collect_values(&transaction, BOUNDARY_TABLE_NAME, None, descending).await?,
        )?;
    }

    transaction
        .upsert(dataset_for(BOUNDARY_TABLE_NAME, 20, 1))
        .await
        .map_err(|error| format!("inserting boundary singleton failed: {error:?}"))?;
    assert_exact_sequence(
        "singleton ascending keys",
        &[20],
        &collect_keys(&transaction, BOUNDARY_TABLE_NAME, None, false).await?,
    )?;
    assert_exact_sequence(
        "singleton descending values",
        &[(20, value_for_key(20))],
        &collect_values(&transaction, BOUNDARY_TABLE_NAME, None, true).await?,
    )?;

    transaction
        .upsert(
            [10usize, 30, 40]
                .into_iter()
                .map(|key| {
                    TableKV::new(
                        Atom::from(BOUNDARY_TABLE_NAME),
                        encode_usize(key),
                        Some(encode_usize(value_for_key(key))),
                    )
                })
                .collect(),
        )
        .await
        .map_err(|error| format!("inserting boundary matrix failed: {error:?}"))?;

    let cases = [
        (None, false, vec![10, 20, 30, 40]),
        (None, true, vec![40, 30, 20, 10]),
        (Some(20), false, vec![20, 30, 40]),
        (Some(20), true, vec![20, 10]),
        (Some(25), false, vec![30, 40]),
        (Some(25), true, vec![20, 10]),
        (Some(5), false, vec![10, 20, 30, 40]),
        (Some(5), true, vec![]),
        (Some(50), false, vec![]),
        (Some(50), true, vec![40, 30, 20, 10]),
    ];
    for (start, descending, expected_keys) in cases {
        let label = format!("boundary keys start={start:?} descending={descending}");
        let observed_keys =
            collect_keys(&transaction, BOUNDARY_TABLE_NAME, start, descending).await?;
        assert_exact_sequence(&label, &expected_keys, &observed_keys)?;

        let expected_values: Vec<_> = expected_keys
            .iter()
            .map(|key| (*key, value_for_key(*key)))
            .collect();
        let observed_values =
            collect_values(&transaction, BOUNDARY_TABLE_NAME, start, descending).await?;
        assert_exact_sequence(
            &format!("boundary values start={start:?} descending={descending}"),
            &expected_values,
            &observed_values,
        )?;
    }

    discard_start_transaction(transaction, "boundary transaction");
    Ok(())
}

/// 在指定有序表的纯事务私有根上复用原崩溃模型。
async fn exercise_ordered_table_mutation(
    db: &RealDb,
    table_name: &str,
    kind: StreamKind,
) -> TestResult<()> {
    let transaction = db
        .transaction(
            Atom::from("iterator ordered table mutation"),
            true,
            10_000,
            10_000,
        )
        .ok_or_else(|| format!("database rejected mutation transaction for {table_name}"))?;
    transaction
        .upsert(dataset_for(table_name, 0, MATRIX_LEN))
        .await
        .map_err(|error| format!("creating {table_name} private root failed: {error:?}"))?;

    match kind {
        StreamKind::Keys => {
            let mut stream = transaction
                .keys(Atom::from(table_name), None, false)
                .await
                .ok_or_else(|| format!("keys returned no stream for {table_name}"))?;
            replace_private_root_for(&transaction, table_name, MATRIX_LEN, MUTATION_ROUNDS).await?;
            let mut observed = Vec::with_capacity(MATRIX_LEN);
            while let Some(key) = stream.next().await {
                observed.push(decode_usize(&key)?);
            }
            let expected: Vec<_> = (0..MATRIX_LEN).collect();
            assert_exact_sequence(&format!("{table_name} keys"), &expected, &observed)?;
        }
        StreamKind::Values => {
            let mut stream = transaction
                .values(Atom::from(table_name), None, false)
                .await
                .ok_or_else(|| format!("values returned no stream for {table_name}"))?;
            replace_private_root_for(&transaction, table_name, MATRIX_LEN, MUTATION_ROUNDS).await?;
            let mut observed = Vec::with_capacity(MATRIX_LEN);
            while let Some((key, value)) = stream.next().await {
                observed.push((decode_usize(&key)?, decode_usize(&value)?));
            }
            let expected: Vec<_> = (0..MATRIX_LEN)
                .map(|key| (key, value_for_key(key)))
                .collect();
            assert_exact_sequence(&format!("{table_name} values"), &expected, &observed)?;
        }
    }

    discard_start_transaction(transaction, &format!("{table_name} mutation transaction"));
    Ok(())
}

async fn exercise_log_ordered_snapshot(db: &RealDb, kind: StreamKind) -> TestResult<()> {
    create_test_table(db, LOG_TABLE_NAME, KVDBTableType::LogOrdTab, true).await?;
    exercise_ordered_table_mutation(db, LOG_TABLE_NAME, kind).await
}

async fn exercise_btree_overlay_snapshot(db: &RealDb, kind: StreamKind) -> TestResult<()> {
    create_test_table(db, BTREE_TABLE_NAME, KVDBTableType::BtreeOrdTab, true).await?;
    exercise_ordered_table_mutation(db, BTREE_TABLE_NAME, kind).await
}

/// 构造已确认进入 redb 的基线，再叠加更新、tombstone 和新增项。
async fn exercise_btree_persisted_snapshot(
    rt: &MultiTaskRuntime<()>,
    db: &RealDb,
    kind: StreamKind,
) -> TestResult<()> {
    create_typed_test_table(
        db,
        BTREE_TABLE_NAME,
        KVDBTableType::BtreeOrdTab,
        true,
        EnumType::Usize,
        EnumType::Str,
    )
    .await?;
    let baseline = db
        .transaction(Atom::from("btree persisted baseline"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected Btree baseline transaction".to_owned())?;
    baseline
        .upsert(persisted_dataset(0, PERSISTED_LEN))
        .await
        .map_err(|error| format!("writing Btree baseline failed: {error:?}"))?;
    commit_transaction(&baseline, "Btree persisted baseline").await?;
    drop(baseline);
    wait_for_empty_cache(rt, db, BTREE_TABLE_NAME, Duration::from_secs(15)).await?;

    // 新事务在 overlay 已清空时仍必须从 redb 精确读回全部基线。
    let disk_probe = db
        .transaction(Atom::from("btree disk probe"), false, 10_000, 10_000)
        .ok_or_else(|| "database rejected Btree disk probe".to_owned())?;
    let expected_disk: Vec<_> = (0..PERSISTED_LEN)
        .map(|key| (key, persisted_value_for_key(key)))
        .collect();
    let observed_disk = collect_string_values(&disk_probe, BTREE_TABLE_NAME, None, false).await?;
    assert_exact_sequence(
        "Btree persisted redb baseline",
        &expected_disk,
        &observed_disk,
    )?;
    discard_start_transaction(disk_probe, "Btree disk probe");

    let transaction = db
        .transaction(Atom::from("btree persisted overlay"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected Btree overlay transaction".to_owned())?;
    let mut model: BTreeMap<usize, String> = (0..PERSISTED_LEN)
        .map(|key| (key, persisted_value_for_key(key)))
        .collect();

    let deleted = transaction
        .delete(
            (0..32)
                .map(|key| TableKV::new(Atom::from(BTREE_TABLE_NAME), encode_usize(key), None))
                .collect(),
        )
        .await
        .map_err(|error| format!("creating Btree tombstones failed: {error:?}"))?;
    let observed_deleted: TestResult<Vec<Option<String>>> = deleted
        .iter()
        .map(|value| value.as_ref().map(decode_string).transpose())
        .collect();
    let expected_deleted: Vec<_> = (0..32)
        .map(|key| Some(persisted_value_for_key(key)))
        .collect();
    assert_exact_sequence(
        "Btree persisted delete old values",
        &expected_deleted,
        &observed_deleted?,
    )?;
    let deleted_probe = transaction
        .query(
            (0..32)
                .map(|key| TableKV::new(Atom::from(BTREE_TABLE_NAME), encode_usize(key), None))
                .collect(),
        )
        .await;
    if deleted_probe.len() != 32 || deleted_probe.iter().any(Option::is_some) {
        return Err(format!(
            "Btree tombstones did not hide all 32 persisted values: {deleted_probe:?}"
        ));
    }
    for key in 0..32 {
        model.remove(&key);
    }

    let overlay_writes: Vec<_> = (32..64)
        .map(|key| (key, persisted_value_for_key(key + REPLACEMENT_STRIDE)))
        .chain((1_000..1_032).map(|key| (key, persisted_value_for_key(key + REPLACEMENT_STRIDE))))
        .collect();
    transaction
        .upsert(
            overlay_writes
                .iter()
                .map(|(key, value)| {
                    TableKV::new(
                        Atom::from(BTREE_TABLE_NAME),
                        encode_usize(*key),
                        Some(encode_string(value.clone())),
                    )
                })
                .collect(),
        )
        .await
        .map_err(|error| format!("writing Btree overlay failed: {error:?}"))?;
    for (key, value) in overlay_writes {
        model.insert(key, value);
    }

    let expected_keys: Vec<_> = model.keys().copied().collect();
    let expected_values: Vec<_> = model
        .iter()
        .map(|(key, value)| (*key, value.clone()))
        .collect();
    match kind {
        StreamKind::Keys => {
            let mut stream = transaction
                .keys(Atom::from(BTREE_TABLE_NAME), None, false)
                .await
                .ok_or_else(|| "Btree persisted keys returned no stream".to_owned())?;
            replace_btree_overlay_after_snapshot(&transaction, model.keys().copied()).await?;
            let mut observed = Vec::new();
            while let Some(key) = stream.next().await {
                observed.push(decode_usize(&key)?);
            }
            assert_exact_sequence("Btree persisted keys snapshot", &expected_keys, &observed)?;
        }
        StreamKind::Values => {
            let mut stream = transaction
                .values(Atom::from(BTREE_TABLE_NAME), None, false)
                .await
                .ok_or_else(|| "Btree persisted values returned no stream".to_owned())?;
            replace_btree_overlay_after_snapshot(&transaction, model.keys().copied()).await?;
            let mut observed = Vec::new();
            while let Some((key, value)) = stream.next().await {
                observed.push((decode_usize(&key)?, decode_string(&value)?));
            }
            assert_exact_sequence(
                "Btree persisted values snapshot",
                &expected_values,
                &observed,
            )?;
        }
    }

    discard_start_transaction(transaction, "Btree persisted overlay");
    Ok(())
}

async fn replace_btree_overlay_after_snapshot<I>(
    transaction: &RealTransaction,
    current_keys: I,
) -> TestResult<()>
where
    I: IntoIterator<Item = usize>,
{
    let deletes: Vec<_> = current_keys
        .into_iter()
        .map(|key| TableKV::new(Atom::from(BTREE_TABLE_NAME), encode_usize(key), None))
        .collect();
    let expected_len = deletes.len();
    let deleted = transaction
        .delete(deletes)
        .await
        .map_err(|error| format!("replacing Btree overlay delete failed: {error:?}"))?;
    if deleted.len() != expected_len {
        return Err(format!(
            "replacing Btree overlay returned {} slots instead of {expected_len}",
            deleted.len()
        ));
    }
    transaction
        .upsert(persisted_dataset(500_000, MATRIX_LEN))
        .await
        .map_err(|error| format!("replacing Btree overlay upsert failed: {error:?}"))
}

/// 证明其它根事务发布新根后，创建事务持有的旧流仍保持原快照。
async fn exercise_other_transaction_commit(db: &RealDb) -> TestResult<()> {
    create_test_table(
        db,
        OTHER_TRANSACTION_TABLE_NAME,
        KVDBTableType::MemOrdTab,
        false,
    )
    .await?;
    let baseline = db
        .transaction(
            Atom::from("other transaction baseline"),
            true,
            10_000,
            10_000,
        )
        .ok_or_else(|| "database rejected other-transaction baseline".to_owned())?;
    baseline
        .upsert(dataset_for(OTHER_TRANSACTION_TABLE_NAME, 0, MATRIX_LEN))
        .await
        .map_err(|error| format!("writing other-transaction baseline failed: {error:?}"))?;
    commit_transaction(&baseline, "other-transaction baseline").await?;

    let creator = db
        .transaction(
            Atom::from("other transaction snapshot owner"),
            false,
            10_000,
            10_000,
        )
        .ok_or_else(|| "database rejected snapshot owner".to_owned())?;
    let mut keys = creator
        .keys(Atom::from(OTHER_TRANSACTION_TABLE_NAME), None, false)
        .await
        .ok_or_else(|| "other-transaction keys returned no stream".to_owned())?;
    let mut values = creator
        .values(Atom::from(OTHER_TRANSACTION_TABLE_NAME), None, false)
        .await
        .ok_or_else(|| "other-transaction values returned no stream".to_owned())?;

    let writer = db
        .transaction(Atom::from("other transaction writer"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected other transaction writer".to_owned())?;
    writer
        .delete(
            (0..MATRIX_LEN / 2)
                .map(|key| {
                    TableKV::new(
                        Atom::from(OTHER_TRANSACTION_TABLE_NAME),
                        encode_usize(key),
                        None,
                    )
                })
                .collect(),
        )
        .await
        .map_err(|error| format!("other transaction delete failed: {error:?}"))?;
    writer
        .upsert(dataset_for(
            OTHER_TRANSACTION_TABLE_NAME,
            REPLACEMENT_STRIDE,
            MATRIX_LEN,
        ))
        .await
        .map_err(|error| format!("other transaction upsert failed: {error:?}"))?;
    commit_transaction(&writer, "other transaction writer").await?;

    let mut observed_keys = Vec::new();
    while let Some(key) = keys.next().await {
        observed_keys.push(decode_usize(&key)?);
    }
    let mut observed_values = Vec::new();
    while let Some((key, value)) = values.next().await {
        observed_values.push((decode_usize(&key)?, decode_usize(&value)?));
    }
    let expected_keys: Vec<_> = (0..MATRIX_LEN).collect();
    let expected_values: Vec<_> = (0..MATRIX_LEN)
        .map(|key| (key, value_for_key(key)))
        .collect();
    assert_exact_sequence("other transaction keys", &expected_keys, &observed_keys)?;
    assert_exact_sequence(
        "other transaction values",
        &expected_values,
        &observed_values,
    )?;
    drop(keys);
    drop(values);

    let verifier = db
        .transaction(
            Atom::from("other transaction verifier"),
            false,
            10_000,
            10_000,
        )
        .ok_or_else(|| "database rejected other transaction verifier".to_owned())?;
    let current = verifier
        .query(vec![
            TableKV::new(
                Atom::from(OTHER_TRANSACTION_TABLE_NAME),
                encode_usize(0),
                None,
            ),
            TableKV::new(
                Atom::from(OTHER_TRANSACTION_TABLE_NAME),
                encode_usize(REPLACEMENT_STRIDE),
                None,
            ),
        ])
        .await;
    if current.len() != 2 || current[0].is_some() {
        return Err(format!(
            "other transaction did not publish its new root: {current:?}"
        ));
    }
    let replacement = current[1]
        .as_ref()
        .ok_or_else(|| "other transaction replacement key is absent".to_owned())?;
    if decode_usize(replacement)? != value_for_key(REPLACEMENT_STRIDE) {
        return Err("other transaction replacement value mismatch".to_owned());
    }
    discard_start_transaction(verifier, "other transaction verifier");
    discard_start_transaction(creator, "other transaction snapshot owner");
    Ok(())
}

/// 从管理器注册表移除表后，既有创建事务和流仍持有必要共享引用。
async fn exercise_table_release(db: &RealDb) -> TestResult<()> {
    create_test_table(db, RELEASE_TABLE_NAME, KVDBTableType::MemOrdTab, false).await?;
    let baseline = db
        .transaction(Atom::from("release baseline"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected release baseline".to_owned())?;
    baseline
        .upsert(dataset_for(RELEASE_TABLE_NAME, 0, MATRIX_LEN))
        .await
        .map_err(|error| format!("writing release baseline failed: {error:?}"))?;
    commit_transaction(&baseline, "release baseline").await?;

    let creator = db
        .transaction(Atom::from("release snapshot owner"), false, 10_000, 10_000)
        .ok_or_else(|| "database rejected release snapshot owner".to_owned())?;
    let mut stream = creator
        .values(Atom::from(RELEASE_TABLE_NAME), None, false)
        .await
        .ok_or_else(|| "release values returned no stream".to_owned())?;

    let remover = db
        .transaction(Atom::from("release table remover"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected release table remover".to_owned())?;
    remover
        .remove_table(Atom::from(RELEASE_TABLE_NAME))
        .await
        .map_err(|error| format!("removing table from registry failed: {error}"))?;
    if db.is_exist(&Atom::from(RELEASE_TABLE_NAME)).await {
        return Err("removed table remains in manager registry".to_owned());
    }
    commit_transaction(&remover, "release table remover").await?;

    let mut observed = Vec::new();
    while let Some((key, value)) = stream.next().await {
        observed.push((decode_usize(&key)?, decode_usize(&value)?));
    }
    let expected: Vec<_> = (0..MATRIX_LEN)
        .map(|key| (key, value_for_key(key)))
        .collect();
    assert_exact_sequence("released table snapshot", &expected, &observed)?;
    drop(stream);
    discard_start_transaction(creator, "released table snapshot owner");
    Ok(())
}

/// 用两个异步任务和双向屏障在两个 yield 区间内确定性替换同一事务私有根。
async fn exercise_concurrent_interleaving(
    rt: &MultiTaskRuntime<()>,
    db: &RealDb,
) -> TestResult<()> {
    create_test_table(db, CONCURRENT_TABLE_NAME, KVDBTableType::MemOrdTab, false).await?;
    let transaction = db
        .transaction(
            Atom::from("concurrent iterator owner"),
            true,
            10_000,
            10_000,
        )
        .ok_or_else(|| "database rejected concurrent iterator owner".to_owned())?;
    transaction
        .upsert(dataset_for(CONCURRENT_TABLE_NAME, 0, MATRIX_LEN))
        .await
        .map_err(|error| format!("writing concurrent baseline failed: {error:?}"))?;
    let mut stream = transaction
        .values(Atom::from(CONCURRENT_TABLE_NAME), None, false)
        .await
        .ok_or_else(|| "concurrent values returned no stream".to_owned())?;

    let (root_replaced_tx, root_replaced_rx) = async_channel::bounded(1);
    let (continue_tx, continue_rx) = async_channel::bounded(1);
    let (result_tx, result_rx) = async_channel::bounded(1);
    let writer = transaction.clone();
    rt.spawn(async move {
        let result: TestResult<()> = async {
            let deleted = writer
                .delete(
                    (0..MATRIX_LEN)
                        .map(|key| {
                            TableKV::new(Atom::from(CONCURRENT_TABLE_NAME), encode_usize(key), None)
                        })
                        .collect(),
                )
                .await
                .map_err(|error| format!("concurrent delete failed: {error:?}"))?;
            if deleted.len() != MATRIX_LEN {
                return Err(format!(
                    "concurrent delete returned {} slots instead of {MATRIX_LEN}",
                    deleted.len()
                ));
            }
            root_replaced_tx
                .send(())
                .await
                .map_err(|error| format!("signalling root replacement failed: {error}"))?;
            continue_rx
                .recv()
                .await
                .map_err(|error| format!("waiting for reader continuation failed: {error}"))?;
            writer
                .upsert(dataset_for(
                    CONCURRENT_TABLE_NAME,
                    REPLACEMENT_STRIDE,
                    MATRIX_LEN,
                ))
                .await
                .map_err(|error| format!("concurrent replacement upsert failed: {error:?}"))
        }
        .await;
        let _ = result_tx.send(result).await;
    })
    .map_err(|error| format!("spawning concurrent writer failed: {error:?}"))?;

    let mut observed = Vec::with_capacity(MATRIX_LEN);
    for _ in 0..64 {
        let (key, value) = stream
            .next()
            .await
            .ok_or_else(|| "concurrent stream ended before first barrier".to_owned())?;
        observed.push((decode_usize(&key)?, decode_usize(&value)?));
    }
    root_replaced_rx
        .recv()
        .await
        .map_err(|error| format!("receiving root replacement signal failed: {error}"))?;
    for _ in 0..64 {
        let (key, value) = stream
            .next()
            .await
            .ok_or_else(|| "concurrent stream ended before second barrier".to_owned())?;
        observed.push((decode_usize(&key)?, decode_usize(&value)?));
    }
    continue_tx
        .send(())
        .await
        .map_err(|error| format!("releasing concurrent writer failed: {error}"))?;
    result_rx
        .recv()
        .await
        .map_err(|error| format!("receiving concurrent writer result failed: {error}"))??;
    while let Some((key, value)) = stream.next().await {
        observed.push((decode_usize(&key)?, decode_usize(&value)?));
    }

    let expected: Vec<_> = (0..MATRIX_LEN)
        .map(|key| (key, value_for_key(key)))
        .collect();
    assert_exact_sequence("concurrent interleaving snapshot", &expected, &observed)?;
    drop(stream);
    discard_start_transaction(transaction, "concurrent iterator owner");
    Ok(())
}

/// 在 0 poll、1 item 和中段三个位置释放 Memory/Btree 流并继续使用事务。
async fn exercise_cancellation(db: &RealDb) -> TestResult<()> {
    create_test_table(
        db,
        CANCELLATION_MEMORY_TABLE_NAME,
        KVDBTableType::MemOrdTab,
        false,
    )
    .await?;
    create_test_table(
        db,
        CANCELLATION_BTREE_TABLE_NAME,
        KVDBTableType::BtreeOrdTab,
        true,
    )
    .await?;

    for table_name in [
        CANCELLATION_MEMORY_TABLE_NAME,
        CANCELLATION_BTREE_TABLE_NAME,
    ] {
        let transaction = db
            .transaction(Atom::from("iterator cancellation"), true, 10_000, 10_000)
            .ok_or_else(|| format!("database rejected cancellation for {table_name}"))?;
        transaction
            .upsert(dataset_for(table_name, 0, MATRIX_LEN))
            .await
            .map_err(|error| format!("writing cancellation dataset failed: {error:?}"))?;

        let stream = transaction
            .keys(Atom::from(table_name), None, false)
            .await
            .ok_or_else(|| format!("0-poll keys returned no stream for {table_name}"))?;
        drop(stream);

        let mut stream = transaction
            .values(Atom::from(table_name), None, false)
            .await
            .ok_or_else(|| format!("1-item values returned no stream for {table_name}"))?;
        let first = stream
            .next()
            .await
            .ok_or_else(|| format!("1-item stream was empty for {table_name}"))?;
        if (decode_usize(&first.0)?, decode_usize(&first.1)?) != (0, value_for_key(0)) {
            return Err(format!(
                "1-item cancellation prefix mismatch for {table_name}"
            ));
        }
        drop(stream);

        let mut stream = transaction
            .keys(Atom::from(table_name), None, false)
            .await
            .ok_or_else(|| format!("mid-stream keys returned no stream for {table_name}"))?;
        for expected in 0..MATRIX_LEN / 2 {
            let key = stream
                .next()
                .await
                .ok_or_else(|| format!("mid-stream ended at {expected} for {table_name}"))?;
            if decode_usize(&key)? != expected {
                return Err(format!(
                    "mid-stream prefix mismatch for {table_name}: expected {expected}"
                ));
            }
        }
        drop(stream);

        let probe_key = REPLACEMENT_STRIDE;
        transaction
            .upsert(vec![TableKV::new(
                Atom::from(table_name),
                encode_usize(probe_key),
                Some(encode_usize(value_for_key(probe_key))),
            )])
            .await
            .map_err(|error| format!("post-cancel upsert failed for {table_name}: {error:?}"))?;
        let probe = transaction
            .query(vec![TableKV::new(
                Atom::from(table_name),
                encode_usize(probe_key),
                None,
            )])
            .await;
        if probe.len() != 1
            || probe[0].as_ref().map(decode_usize).transpose()? != Some(value_for_key(probe_key))
        {
            return Err(format!(
                "post-cancel transaction probe failed for {table_name}"
            ));
        }
        discard_start_transaction(transaction, &format!("cancellation {table_name}"));
    }
    Ok(())
}

/// 在完整公开事务路径上直接观察快照 COW owner 的保活和释放。
async fn exercise_lifecycle_release_matrix(db: &RealDb) -> TestResult<()> {
    for (table_name, table_type, persistence) in [
        (LIFECYCLE_MEMORY_TABLE_NAME, KVDBTableType::MemOrdTab, false),
        (LIFECYCLE_LOG_TABLE_NAME, KVDBTableType::LogOrdTab, true),
        (LIFECYCLE_BTREE_TABLE_NAME, KVDBTableType::BtreeOrdTab, true),
    ] {
        create_test_table(db, table_name, table_type, persistence).await?;

        for exit_point in [
            ExitPoint::ZeroPoll,
            ExitPoint::OneItem,
            ExitPoint::MidStream,
            ExitPoint::Exhausted,
        ] {
            let label = format!("{table_name} {exit_point:?}");
            let transaction = db
                .transaction(Atom::from("iterator lifecycle owner"), true, 10_000, 10_000)
                .ok_or_else(|| format!("database rejected lifecycle transaction: {label}"))?;
            let (dataset, probes) = tracked_dataset(table_name, 0, MATRIX_LEN);
            transaction
                .upsert(dataset)
                .await
                .map_err(|error| format!("{label}: tracked setup failed: {error:?}"))?;

            let mut stream = transaction
                .keys(Atom::from(table_name), None, false)
                .await
                .ok_or_else(|| format!("{label}: keys returned no stream"))?;
            let replacement_offset =
                replace_private_root_for(&transaction, table_name, MATRIX_LEN, 1).await?;
            assert_tracked_values_alive(&label, &probes)?;

            match exit_point {
                ExitPoint::ZeroPoll => drop(stream),
                ExitPoint::OneItem => {
                    let key = stream
                        .next()
                        .await
                        .ok_or_else(|| format!("{label}: stream ended before one item"))?;
                    if decode_usize(&key)? != 0 {
                        return Err(format!("{label}: first key was not zero"));
                    }
                    drop(key);
                    drop(stream);
                }
                ExitPoint::MidStream => {
                    for expected in 0..MATRIX_LEN / 2 {
                        let key = stream.next().await.ok_or_else(|| {
                            format!("{label}: stream ended at prefix index {expected}")
                        })?;
                        if decode_usize(&key)? != expected {
                            return Err(format!(
                                "{label}: expected prefix key {expected}, observed {:?}",
                                decode_usize(&key)
                            ));
                        }
                    }
                    drop(stream);
                }
                ExitPoint::Exhausted => {
                    let mut observed = Vec::with_capacity(MATRIX_LEN);
                    while let Some(key) = stream.next().await {
                        observed.push(decode_usize(&key)?);
                    }
                    let expected: Vec<_> = (0..MATRIX_LEN).collect();
                    assert_exact_sequence(&format!("{label} snapshot"), &expected, &observed)?;
                    assert_tracked_values_released(&format!("{label} after Ready(None)"), &probes)?;
                    drop(stream);
                }
            }
            assert_tracked_values_released(&label, &probes)?;

            let probe = transaction
                .query(vec![TableKV::new(
                    Atom::from(table_name),
                    encode_usize(replacement_offset),
                    None,
                )])
                .await;
            if probe.len() != 1
                || probe[0].as_ref().map(decode_usize).transpose()?
                    != Some(value_for_key(replacement_offset))
            {
                return Err(format!(
                    "{label}: creator transaction was unusable after stream exit"
                ));
            }
            discard_start_transaction(transaction, &label);
        }
    }
    Ok(())
}

/// 在运行时 A 创建流，在独立运行时 B 消费或取消，并在 A 保持根事务存活。
async fn exercise_cross_runtime_lifecycle(
    _rt_a: &MultiTaskRuntime<()>,
    db: &RealDb,
) -> TestResult<()> {
    for (table_name, table_type, consume_all) in [
        (
            CROSS_RUNTIME_MEMORY_TABLE_NAME,
            KVDBTableType::MemOrdTab,
            true,
        ),
        (
            CROSS_RUNTIME_BTREE_TABLE_NAME,
            KVDBTableType::BtreeOrdTab,
            false,
        ),
    ] {
        create_test_table(db, table_name, table_type, true).await?;
        let transaction = db
            .transaction(
                Atom::from("cross-runtime snapshot owner"),
                true,
                10_000,
                10_000,
            )
            .ok_or_else(|| format!("database rejected cross-runtime transaction: {table_name}"))?;
        let (dataset, probes) = tracked_dataset(table_name, 0, MATRIX_LEN);
        transaction
            .upsert(dataset)
            .await
            .map_err(|error| format!("{table_name}: cross-runtime setup failed: {error:?}"))?;
        let mut stream = transaction
            .keys(Atom::from(table_name), None, false)
            .await
            .ok_or_else(|| format!("{table_name}: cross-runtime keys returned no stream"))?;
        let replacement_offset =
            replace_private_root_for(&transaction, table_name, MATRIX_LEN, 1).await?;
        assert_tracked_values_alive(table_name, &probes)?;

        let rt_b = MultiTaskRuntimeBuilder::default()
            .init_worker_size(2)
            .build();
        let (result_tx, result_rx) = async_channel::bounded(1);
        rt_b.spawn(async move {
            let result: TestResult<Vec<usize>> = async {
                let limit = if consume_all {
                    MATRIX_LEN
                } else {
                    MATRIX_LEN / 2
                };
                let mut observed = Vec::with_capacity(limit);
                for expected in 0..limit {
                    let key = stream
                        .next()
                        .await
                        .ok_or_else(|| format!("cross-runtime stream ended at index {expected}"))?;
                    observed.push(decode_usize(&key)?);
                }
                if consume_all && stream.next().await.is_some() {
                    return Err("cross-runtime full stream yielded excess items".to_owned());
                }
                drop(stream);
                Ok(observed)
            }
            .await;
            let _ = result_tx.send(result).await;
        })
        .map_err(|error| format!("spawning independent runtime consumer failed: {error:?}"))?;
        let observed = result_rx
            .recv()
            .await
            .map_err(|error| format!("receiving independent runtime result failed: {error}"))??;
        let expected_len = if consume_all {
            MATRIX_LEN
        } else {
            MATRIX_LEN / 2
        };
        let expected: Vec<_> = (0..expected_len).collect();
        assert_exact_sequence(
            &format!("{table_name} cross-runtime snapshot"),
            &expected,
            &observed,
        )?;
        assert_tracked_values_released(table_name, &probes)?;

        let current = transaction
            .query(vec![TableKV::new(
                Atom::from(table_name),
                encode_usize(replacement_offset),
                None,
            )])
            .await;
        if current.len() != 1
            || current[0].as_ref().map(decode_usize).transpose()?
                != Some(value_for_key(replacement_offset))
        {
            return Err(format!(
                "{table_name}: creator transaction failed after cross-runtime stream exit"
            ));
        }
        discard_start_transaction(transaction, table_name);
        drop(rt_b);
    }
    Ok(())
}

/// 通过公开 DDL 修改内部 Meta 表，并验证旧目录流保持创建时状态。
async fn exercise_meta_ddl_snapshot(db: &RealDb) -> TestResult<()> {
    let expected_meta = KVTableMeta::new(
        KVDBTableType::MemOrdTab,
        false,
        EnumType::Usize,
        EnumType::Usize,
    );
    create_typed_test_table(
        db,
        META_SNAPSHOT_TABLE_NAME,
        KVDBTableType::MemOrdTab,
        false,
        EnumType::Usize,
        EnumType::Usize,
    )
    .await?;

    let snapshot = db
        .transaction(Atom::from("meta iterator snapshot"), false, 10_000, 10_000)
        .ok_or_else(|| "database rejected Meta snapshot transaction".to_owned())?;
    let mut keys = snapshot
        .keys(Atom::from(META_TABLE_NAME), None, false)
        .await
        .ok_or_else(|| "Meta keys returned no stream".to_owned())?;
    let mut values = snapshot
        .values(Atom::from(META_TABLE_NAME), None, false)
        .await
        .ok_or_else(|| "Meta values returned no stream".to_owned())?;

    // 建表与删表不能在同一根事务中混用；快照 owner、删表和替换建表必须各自独立。
    let remover = db
        .transaction(Atom::from("meta iterator remover"), true, 10_000, 10_000)
        .ok_or_else(|| "database rejected Meta remove transaction".to_owned())?;
    remover
        .remove_table(Atom::from(META_SNAPSHOT_TABLE_NAME))
        .await
        .map_err(|error| format!("removing first Meta entry failed: {error}"))?;
    commit_transaction(&remover, "Meta snapshot table remover").await?;
    create_typed_test_table(
        db,
        META_REPLACEMENT_TABLE_NAME,
        KVDBTableType::MemOrdTab,
        false,
        EnumType::Usize,
        EnumType::Usize,
    )
    .await?;

    let mut observed_keys = Vec::new();
    while let Some(key) = keys.next().await {
        observed_keys.push(decode_atom(&key)?.as_str().to_owned());
    }
    assert_exact_sequence(
        "Meta DDL keys snapshot",
        &[META_SNAPSHOT_TABLE_NAME.to_owned()],
        &observed_keys,
    )?;

    let mut observed_values = Vec::new();
    while let Some((key, value)) = values.next().await {
        observed_values.push((
            decode_atom(&key)?.as_str().to_owned(),
            KVTableMeta::from(value),
        ));
    }
    assert_exact_sequence(
        "Meta DDL values snapshot",
        &[(META_SNAPSHOT_TABLE_NAME.to_owned(), expected_meta)],
        &observed_values,
    )?;
    drop(keys);
    drop(values);
    if db.is_exist(&Atom::from(META_SNAPSHOT_TABLE_NAME)).await
        || !db.is_exist(&Atom::from(META_REPLACEMENT_TABLE_NAME)).await
    {
        return Err("Meta DDL did not alter the live registry as arranged".to_owned());
    }
    discard_start_transaction(snapshot, "Meta DDL snapshot");
    Ok(())
}

fn decode_atom(value: &Binary) -> TestResult<Atom> {
    let mut buffer = ReadBuffer::new(value.as_ref(), 0);
    Atom::decode(&mut buffer).map_err(|error| format!("decoding Meta table name failed: {error:?}"))
}

/// 固定 LogWrite 当前唯一空哨兵行为，确保本修复没有顺带改动该表。
async fn exercise_log_write_non_regression(db: &RealDb) -> TestResult<()> {
    create_test_table(db, LOG_WRITE_TABLE_NAME, KVDBTableType::LogWTab, true).await?;
    let transaction = db
        .transaction(
            Atom::from("LogWrite iterator non-regression"),
            false,
            10_000,
            10_000,
        )
        .ok_or_else(|| "database rejected LogWrite iterator transaction".to_owned())?;
    let mut keys = transaction
        .keys(
            Atom::from(LOG_WRITE_TABLE_NAME),
            Some(encode_usize(42)),
            true,
        )
        .await
        .ok_or_else(|| "LogWrite keys returned no stream".to_owned())?;
    let key = keys
        .next()
        .await
        .ok_or_else(|| "LogWrite keys omitted its current sentinel".to_owned())?;
    if !key.as_ref().is_empty() || keys.next().await.is_some() {
        return Err("LogWrite keys no longer yields exactly one empty sentinel".to_owned());
    }

    let mut values = transaction
        .values(
            Atom::from(LOG_WRITE_TABLE_NAME),
            Some(encode_usize(42)),
            true,
        )
        .await
        .ok_or_else(|| "LogWrite values returned no stream".to_owned())?;
    let (key, value) = values
        .next()
        .await
        .ok_or_else(|| "LogWrite values omitted its current sentinel".to_owned())?;
    if !key.as_ref().is_empty() || !value.as_ref().is_empty() || values.next().await.is_some() {
        return Err("LogWrite values no longer yields exactly one empty sentinel pair".to_owned());
    }
    drop(keys);
    drop(values);
    discard_start_transaction(transaction, "LogWrite non-regression");
    Ok(())
}

/// 重复创建、整根替换、耗尽和释放快照，供 debug/release 与 sanitizer 复用。
async fn exercise_repeated_stress(db: &RealDb) -> TestResult<()> {
    create_test_table(db, STRESS_TABLE_NAME, KVDBTableType::MemOrdTab, false).await?;
    for iteration in 0..16usize {
        let transaction = db
            .transaction(Atom::from("iterator repeated stress"), true, 10_000, 10_000)
            .ok_or_else(|| format!("database rejected stress iteration {iteration}"))?;
        transaction
            .upsert(dataset_for(STRESS_TABLE_NAME, 0, MATRIX_LEN))
            .await
            .map_err(|error| format!("stress iteration {iteration} setup failed: {error:?}"))?;
        let descending = iteration % 2 == 1;
        if iteration % 2 == 0 {
            let mut stream = transaction
                .keys(Atom::from(STRESS_TABLE_NAME), None, descending)
                .await
                .ok_or_else(|| format!("stress keys iteration {iteration} returned no stream"))?;
            replace_private_root_for(&transaction, STRESS_TABLE_NAME, MATRIX_LEN, 3).await?;
            let mut observed = Vec::new();
            while let Some(key) = stream.next().await {
                observed.push(decode_usize(&key)?);
            }
            let mut expected: Vec<_> = (0..MATRIX_LEN).collect();
            if descending {
                expected.reverse();
            }
            assert_exact_sequence(
                &format!("stress keys iteration {iteration}"),
                &expected,
                &observed,
            )?;
        } else {
            let mut stream = transaction
                .values(Atom::from(STRESS_TABLE_NAME), None, descending)
                .await
                .ok_or_else(|| format!("stress values iteration {iteration} returned no stream"))?;
            replace_private_root_for(&transaction, STRESS_TABLE_NAME, MATRIX_LEN, 3).await?;
            let mut observed = Vec::new();
            while let Some((key, value)) = stream.next().await {
                observed.push((decode_usize(&key)?, decode_usize(&value)?));
            }
            let mut expected: Vec<_> = (0..MATRIX_LEN)
                .map(|key| (key, value_for_key(key)))
                .collect();
            if descending {
                expected.reverse();
            }
            assert_exact_sequence(
                &format!("stress values iteration {iteration}"),
                &expected,
                &observed,
            )?;
        }
        discard_start_transaction(transaction, &format!("stress iteration {iteration}"));
    }
    Ok(())
}

fn run_child_phase(phase: &str, root: &Path) -> TestResult<()> {
    let phase = phase.to_owned();
    let phase_root = root.join(&phase);
    run_on_runtime(RUNTIME_TIMEOUT, move |rt| async move {
        let db = build_database(&rt, &phase_root).await?;
        match phase.as_str() {
            "keys-control"
            | "values-control"
            | "keys-same-transaction-mutation"
            | "values-same-transaction-mutation" => {
                create_memory_table(&db).await?;
                exercise_scenario(&db, Scenario::parse(&phase)?).await
            }
            "memory-boundaries" => exercise_memory_boundaries(&db).await,
            "logordered-keys-mutation" => {
                exercise_log_ordered_snapshot(&db, StreamKind::Keys).await
            }
            "logordered-values-mutation" => {
                exercise_log_ordered_snapshot(&db, StreamKind::Values).await
            }
            "btree-overlay-keys-mutation" => {
                exercise_btree_overlay_snapshot(&db, StreamKind::Keys).await
            }
            "btree-overlay-values-mutation" => {
                exercise_btree_overlay_snapshot(&db, StreamKind::Values).await
            }
            "btree-persisted-keys-mutation" => {
                exercise_btree_persisted_snapshot(&rt, &db, StreamKind::Keys).await
            }
            "btree-persisted-values-mutation" => {
                exercise_btree_persisted_snapshot(&rt, &db, StreamKind::Values).await
            }
            "other-transaction-commit" => exercise_other_transaction_commit(&db).await,
            "table-release" => exercise_table_release(&db).await,
            "concurrent-interleaving" => exercise_concurrent_interleaving(&rt, &db).await,
            "cancellation" => exercise_cancellation(&db).await,
            "lifecycle-release-matrix" => exercise_lifecycle_release_matrix(&db).await,
            "cross-runtime-lifecycle" => exercise_cross_runtime_lifecycle(&rt, &db).await,
            "meta-ddl-snapshot" => exercise_meta_ddl_snapshot(&db).await,
            "logwrite-non-regression" => exercise_log_write_non_regression(&db).await,
            "repeated-stress" => exercise_repeated_stress(&db).await,
            _ => Err(format!("unknown iterator snapshot phase: {phase}")),
        }
    })
}

/// 在真实多线程 runtime 上执行 future，并用同步通道提供确定硬截止。
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
    .map_err(|error| format!("spawning iterator snapshot future failed: {error:?}"))?;

    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("iterator snapshot future exceeded {timeout:?}: {error}"))?
}

/// 运行全部隔离场景；子进程退出状态和硬超时也是验收断言的一部分。
#[test]
fn test_iterator_snapshot_safety_matrix() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        let root = PathBuf::from(
            env::var_os(ROOT_ENV)
                .expect("the child phase must receive PI_DB_ITERATOR_SNAPSHOT_ROOT"),
        );
        run_child_phase(&phase, &root)
            .unwrap_or_else(|error| panic!("iterator snapshot phase {phase} failed: {error}"));
        return;
    }

    let temp_root = TempRoot::new("memory-same-transaction")
        .expect("creating the iterator snapshot parent directory must succeed");
    for phase in [
        "keys-control",
        "values-control",
        "keys-same-transaction-mutation",
        "values-same-transaction-mutation",
        "memory-boundaries",
        "logordered-keys-mutation",
        "logordered-values-mutation",
        "btree-overlay-keys-mutation",
        "btree-overlay-values-mutation",
        "btree-persisted-keys-mutation",
        "btree-persisted-values-mutation",
        "other-transaction-commit",
        "table-release",
        "concurrent-interleaving",
        "cancellation",
        "lifecycle-release-matrix",
        "cross-runtime-lifecycle",
        "meta-ddl-snapshot",
        "logwrite-non-regression",
        "repeated-stress",
    ] {
        run_phase_process(temp_root.path(), phase, CHILD_TIMEOUT)
            .unwrap_or_else(|error| panic!("iterator snapshot child {phase} failed: {error}"));
    }
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(label: &str) -> TestResult<Self> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system time is before UNIX_EPOCH: {error}"))?
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "pi_db_iterator_snapshot_{label}_{}_{}",
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
