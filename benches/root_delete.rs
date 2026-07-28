#![feature(test)]
//! 根事务 `delete/dirty_delete` 动作阶段的独立真实环境基准。
//!
//! 十四个场景使用真实 4-worker runtime、事务管理器、根 `CommitLogger`、
//! Memory/LogOrdered/Btree/redb 和临时文件系统，分别测量三类表单项、Btree cache/redb
//! 两种旧值来源，以及三表 1/16/256 项混合批次的普通/dirty 删除。
//!
//! 每个正式样本创建全新可写根、clone 预构造输入、调用公开根 API，并释放尚未 prepare 的
//! 私有根。数据库启动、DDL、种子提交、Btree 持久化等待、cache=0 门禁和严格返回/私有状态
//! preflight 位于计时外。结果只表示 action-stage 相对成本，不包含 prepare、根 WAL、commit、
//! 异步物理删除、确认或 repair。正式口径见
//! `docs/ROOT_DELETE_CONTRACT.md#root-delete-benchmark`。

extern crate test;

use std::{
    env,
    fs,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop,
    AsyncRuntime,
    AsyncRuntimeExt,
};
use pi_async_transaction::{
    manager_2pc::Transaction2PcManager,
    TransactionTree,
};
use pi_atom::Atom;
use pi_bon::WriteBuffer;
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    utils::CreateTableOptions,
    Binary,
    KVDBTableType,
    KVTableMeta,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};
use test::{black_box, Bencher};

type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const MEMORY_TABLE: &str = "bench_root_delete_memory";
const LOG_ORDERED_TABLE: &str = "bench_root_delete_log_ordered";
const BTREE_CACHE_TABLE: &str = "bench_root_delete_btree_cache";
const BTREE_REDB_TABLE: &str = "bench_root_delete_btree_redb";

const BASELINE_KEYS: usize = 256;
const REDB_FILLER_COUNT: usize = 3;
const REDB_FILLER_BYTES: usize = 400 * 1024;
const REDB_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

static DB_RUNTIME: OnceLock<MultiTaskRuntime<()>> = OnceLock::new();

macro_rules! root_delete_benchmark {
    ($name:ident, $case:expr, $mode:expr, $description:expr) => {
        #[doc = $description]
        #[bench]
        fn $name(b: &mut Bencher) {
            let _time_loop = startup_global_time_loop(10);
            let fixture = Fixture::new(stringify!($name));
            fixture.benchmark(b, $case, $mode);
        }
    };
}

root_delete_benchmark!(
    bench_memory_delete_single,
    DeleteCase::MemorySingle,
    ActionMode::Ordinary,
    "测量 Memory 单项普通 delete 动作阶段。"
);
root_delete_benchmark!(
    bench_memory_dirty_delete_single,
    DeleteCase::MemorySingle,
    ActionMode::Dirty,
    "测量 Memory 单项 dirty_delete 动作阶段。"
);
root_delete_benchmark!(
    bench_log_ordered_delete_single,
    DeleteCase::LogOrderedSingle,
    ActionMode::Ordinary,
    "测量 LogOrdered 单项普通 delete 动作阶段。"
);
root_delete_benchmark!(
    bench_log_ordered_dirty_delete_single,
    DeleteCase::LogOrderedSingle,
    ActionMode::Dirty,
    "测量 LogOrdered 单项 dirty_delete 动作阶段。"
);
root_delete_benchmark!(
    bench_btree_cache_delete_single,
    DeleteCase::BtreeCacheSingle,
    ActionMode::Ordinary,
    "测量 Btree cache-hit 单项普通 delete 动作阶段。"
);
root_delete_benchmark!(
    bench_btree_cache_dirty_delete_single,
    DeleteCase::BtreeCacheSingle,
    ActionMode::Dirty,
    "测量 Btree cache-hit 单项 dirty_delete 动作阶段。"
);
root_delete_benchmark!(
    bench_btree_redb_delete_single,
    DeleteCase::BtreeRedbSingle,
    ActionMode::Ordinary,
    "测量 Btree redb-only 单项普通 delete 动作阶段。"
);
root_delete_benchmark!(
    bench_btree_redb_dirty_delete_single,
    DeleteCase::BtreeRedbSingle,
    ActionMode::Dirty,
    "测量 Btree redb-only 单项 dirty_delete 动作阶段。"
);
root_delete_benchmark!(
    bench_mixed_1_delete,
    DeleteCase::Mixed1,
    ActionMode::Ordinary,
    "测量三表轮转 1 项普通 delete 动作阶段。"
);
root_delete_benchmark!(
    bench_mixed_1_dirty_delete,
    DeleteCase::Mixed1,
    ActionMode::Dirty,
    "测量三表轮转 1 项 dirty_delete 动作阶段。"
);
root_delete_benchmark!(
    bench_mixed_16_delete,
    DeleteCase::Mixed16,
    ActionMode::Ordinary,
    "测量三表轮转 16 项普通 delete 动作阶段。"
);
root_delete_benchmark!(
    bench_mixed_16_dirty_delete,
    DeleteCase::Mixed16,
    ActionMode::Dirty,
    "测量三表轮转 16 项 dirty_delete 动作阶段。"
);
root_delete_benchmark!(
    bench_mixed_256_delete,
    DeleteCase::Mixed256,
    ActionMode::Ordinary,
    "测量三表轮转 256 项普通 delete 动作阶段。"
);
root_delete_benchmark!(
    bench_mixed_256_dirty_delete,
    DeleteCase::Mixed256,
    ActionMode::Dirty,
    "测量三表轮转 256 项 dirty_delete 动作阶段。"
);

#[derive(Clone, Copy)]
enum ActionMode {
    Ordinary,
    Dirty,
}

impl ActionMode {
    const fn label(self) -> &'static str {
        match self {
            Self::Ordinary => "delete",
            Self::Dirty => "dirty_delete",
        }
    }
}

#[derive(Clone, Copy)]
enum DeleteCase {
    MemorySingle,
    LogOrderedSingle,
    BtreeCacheSingle,
    BtreeRedbSingle,
    Mixed1,
    Mixed16,
    Mixed256,
}

struct DeletePlan {
    input: Vec<TableKV>,
    expected: Vec<Option<Binary>>,
    expected_children: usize,
}

struct Fixture {
    db: RealDb,
    rt: MultiTaskRuntime<()>,
    _root: TempRoot,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let root = TempRoot::new(label);
        let root_path = root.path().to_path_buf();
        let rt = shared_runtime();
        let setup_rt = rt.clone();
        let (sender, receiver) = bounded(1);

        rt.block_on(async move {
            let logger = CommitLoggerBuilder::new(
                setup_rt.clone(),
                root_path.join("root-wal"),
            )
            .log_file_limit(64 * 1024 * 1024)
            .collect_interval(5 * 60 * 1000)
            .build()
            .await
            .expect("root-delete benchmark CommitLogger must start");
            let manager = Transaction2PcManager::new(
                setup_rt.clone(),
                GuidGen::new(0, std::process::id() as u16),
                logger,
            );
            let db = KVDBManagerBuilder::new(
                setup_rt.clone(),
                manager,
                root_path.join("database"),
            )
            .key_version_ttl(Duration::ZERO)
            .key_version_ttl_poll_interval(Duration::ZERO)
            .startup(false)
            .await
            .expect("root-delete benchmark database must start");
            create_tables(&db).await;
            seed_cow_and_cache_tables(&db).await;
            seed_redb_table(&setup_rt, &db).await;
            sender
                .send(db)
                .expect("root-delete benchmark fixture receiver must remain alive");
        })
        .expect("root-delete benchmark setup runtime must complete");

        Self {
            db: receiver
                .recv()
                .expect("root-delete benchmark fixture must be returned"),
            rt,
            _root: root,
        }
    }

    fn benchmark(&self, b: &mut Bencher, case: DeleteCase, mode: ActionMode) {
        let plan = delete_plan(case);
        self.assert_source_state(case);
        self.preflight(&plan, mode);

        b.iter(|| {
            let transaction = writable_transaction(
                &self.db,
                &format!("root-delete benchmark {} sample", mode.label()),
            );
            let transaction_for_action = transaction.clone();
            let input = plan.input.clone();
            let (result, children) = self
                .rt
                .block_on(async move {
                    let result = match mode {
                        ActionMode::Ordinary => {
                            transaction_for_action.delete(input).await
                        },
                        ActionMode::Dirty => {
                            transaction_for_action.dirty_delete(input).await
                        },
                    }
                    .expect("root-delete benchmark action must succeed");
                    (result, transaction_for_action.children_len())
                })
                .expect("root-delete benchmark runtime must execute one sample");
            assert_eq!(result.len(), plan.input.len());
            assert_eq!(children, plan.expected_children);
            black_box(result);
            black_box(children);
            drop(transaction);
        });
    }

    fn assert_source_state(&self, case: DeleteCase) {
        let db = self.db.clone();
        let (cache_size, redb_size) = self
            .rt
            .block_on(async move {
                (
                    db.table_cache_size(&Atom::from(BTREE_CACHE_TABLE)).await,
                    db.table_cache_size(&Atom::from(BTREE_REDB_TABLE)).await,
                )
            })
            .expect("root-delete benchmark must read source-table cache sizes");
        let cache_size = cache_size
            .expect("root-delete benchmark cache table must exist");
        assert!(
            cache_size > 0,
            "cache-hit benchmark source unexpectedly has an empty overlay",
        );
        let redb_size = redb_size
            .expect("root-delete benchmark redb table must exist");
        assert_eq!(
            redb_size,
            0,
            "redb-only benchmark source must have an empty overlay",
        );
        black_box(case);
    }

    fn preflight(&self, plan: &DeletePlan, mode: ActionMode) {
        let transaction = writable_transaction(
            &self.db,
            &format!("root-delete benchmark {} preflight", mode.label()),
        );
        let transaction_for_action = transaction.clone();
        let query_before = plan
            .input
            .iter()
            .map(|item| TableKV::new(item.table.clone(), item.key.clone(), None))
            .collect();
        let query_after = plan
            .input
            .iter()
            .map(|item| TableKV::new(item.table.clone(), item.key.clone(), None))
            .collect();
        let input = plan.input.clone();
        let (before, deleted, after) = self
            .rt
            .block_on(async move {
                match mode {
                    ActionMode::Ordinary => {
                        let before = transaction_for_action.query(query_before).await;
                        let deleted = transaction_for_action
                            .delete(input)
                            .await
                            .expect("ordinary root-delete preflight must succeed");
                        let after = transaction_for_action.query(query_after).await;
                        (before, deleted, after)
                    },
                    ActionMode::Dirty => {
                        let before = transaction_for_action.dirty_query(query_before).await;
                        let deleted = transaction_for_action
                            .dirty_delete(input)
                            .await
                            .expect("dirty root-delete preflight must succeed");
                        let after = transaction_for_action.dirty_query(query_after).await;
                        (before, deleted, after)
                    },
                }
            })
            .expect("root-delete benchmark preflight runtime must complete");
        assert_eq!(transaction.children_len(), plan.expected_children);
        assert_eq!(before.len(), plan.expected.len());
        assert_eq!(deleted.len(), plan.expected.len());
        assert_eq!(after.len(), plan.expected.len());
        for (index, ((before, deleted), expected)) in before
            .iter()
            .zip(&deleted)
            .zip(&plan.expected)
            .enumerate() {
            let expected_value = baseline_value_for_item(&plan.input[index]);
            assert_eq!(
                before.as_ref().map(AsRef::<[u8]>::as_ref),
                Some(expected_value.as_ref()),
                "root-delete benchmark preflight baseline mismatch at {index}",
            );
            assert_eq!(
                deleted.as_ref().map(AsRef::<[u8]>::as_ref),
                expected.as_ref().map(AsRef::<[u8]>::as_ref),
                "root-delete benchmark preflight return mismatch at {index}",
            );
            assert!(
                after[index].is_none(),
                "root-delete benchmark preflight tombstone missing at {index}",
            );
        }
        drop(transaction);
    }
}

fn shared_runtime() -> MultiTaskRuntime<()> {
    DB_RUNTIME
        .get_or_init(|| {
            MultiTaskRuntimeBuilder::default()
                .init_worker_size(4)
                .build()
        })
        .clone()
}

async fn create_tables(db: &RealDb) {
    let transaction = writable_transaction(db, "root-delete benchmark DDL");
    transaction
        .create_table(
            Atom::from(MEMORY_TABLE),
            table_meta(KVDBTableType::MemOrdTab, true),
            false,
        )
        .await
        .expect("root-delete benchmark Memory table must be created");
    transaction
        .create_table_with_options(
            Atom::from(LOG_ORDERED_TABLE),
            table_meta(KVDBTableType::LogOrdTab, true),
            CreateTableOptions::LogOrdTab(
                64 * 1024 * 1024,
                1024 * 1024,
                1024 * 1024,
            ),
            false,
        )
        .await
        .expect("root-delete benchmark LogOrdered table must be created");
    transaction
        .create_table_with_options(
            Atom::from(BTREE_CACHE_TABLE),
            table_meta(KVDBTableType::BtreeOrdTab, false),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .expect("root-delete benchmark cache Btree table must be created");
    transaction
        .create_table_with_options(
            Atom::from(BTREE_REDB_TABLE),
            table_meta(KVDBTableType::BtreeOrdTab, true),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .expect("root-delete benchmark redb Btree table must be created");
    commit(&transaction, "DDL").await;
}

async fn seed_cow_and_cache_tables(db: &RealDb) {
    let transaction = writable_transaction(db, "root-delete benchmark COW/cache seed");
    let mut input = Vec::with_capacity(BASELINE_KEYS * 2 + 1);
    for index in 0..BASELINE_KEYS {
        input.push(baseline_item(MEMORY_TABLE, index));
        input.push(baseline_item(LOG_ORDERED_TABLE, index));
    }
    input.push(TableKV::new(
        Atom::from(BTREE_CACHE_TABLE),
        benchmark_key(BTREE_CACHE_TABLE, 0),
        Some(benchmark_value(BTREE_CACHE_TABLE, 0)),
    ));
    transaction
        .upsert(input)
        .await
        .expect("root-delete benchmark COW/cache seed must write");
    commit(&transaction, "COW/cache seed").await;
}

async fn seed_redb_table(rt: &MultiTaskRuntime<()>, db: &RealDb) {
    let transaction = writable_transaction(db, "root-delete benchmark redb seed");
    let mut input = Vec::with_capacity(BASELINE_KEYS + REDB_FILLER_COUNT);
    for index in 0..BASELINE_KEYS {
        input.push(baseline_item(BTREE_REDB_TABLE, index));
    }
    for index in 0..REDB_FILLER_COUNT {
        input.push(TableKV::new(
            Atom::from(BTREE_REDB_TABLE),
            encode_bin(format!("redb-filler-{index}").as_bytes()),
            Some(filler_value(index)),
        ));
    }
    transaction
        .upsert(input)
        .await
        .expect("root-delete benchmark redb seed must write");
    commit(&transaction, "redb seed").await;
    wait_for_empty_cache(rt, db, BTREE_REDB_TABLE).await;
}

async fn commit(transaction: &RealTransaction, label: &str) {
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .unwrap_or_else(|error| panic!("root-delete benchmark {label} prepare failed: {error:?}"));
    transaction
        .commit_modified(prepare)
        .await
        .unwrap_or_else(|error| panic!("root-delete benchmark {label} commit failed: {error:?}"));
}

async fn wait_for_empty_cache(
    rt: &MultiTaskRuntime<()>,
    db: &RealDb,
    table: &str,
) {
    let deadline = Instant::now() + REDB_DRAIN_TIMEOUT;
    loop {
        match db.table_cache_size(&Atom::from(table)).await {
            Some(0) => return,
            Some(size) if Instant::now() >= deadline => {
                panic!(
                    "root-delete benchmark {table} cache remained at {size} bytes after {REDB_DRAIN_TIMEOUT:?}",
                );
            },
            Some(_) => rt.timeout(10).await,
            None => panic!("root-delete benchmark {table} disappeared while draining cache"),
        }
    }
}

fn delete_plan(case: DeleteCase) -> DeletePlan {
    match case {
        DeleteCase::MemorySingle => single_table_plan(MEMORY_TABLE, false),
        DeleteCase::LogOrderedSingle => single_table_plan(LOG_ORDERED_TABLE, false),
        DeleteCase::BtreeCacheSingle => single_table_plan(BTREE_CACHE_TABLE, true),
        DeleteCase::BtreeRedbSingle => single_table_plan(BTREE_REDB_TABLE, true),
        DeleteCase::Mixed1 => mixed_plan(1),
        DeleteCase::Mixed16 => mixed_plan(16),
        DeleteCase::Mixed256 => mixed_plan(256),
    }
}

fn single_table_plan(table: &'static str, returns_old_value: bool) -> DeletePlan {
    let value = benchmark_value(table, 0);
    DeletePlan {
        input: vec![TableKV::new(
            Atom::from(table),
            benchmark_key(table, 0),
            None,
        )],
        expected: vec![returns_old_value.then_some(value)],
        expected_children: 1,
    }
}

fn mixed_plan(count: usize) -> DeletePlan {
    let tables = [MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_REDB_TABLE];
    let mut input = Vec::with_capacity(count);
    let mut expected = Vec::with_capacity(count);
    for index in 0..count {
        let table = tables[index % tables.len()];
        input.push(TableKV::new(
            Atom::from(table),
            benchmark_key(table, index),
            None,
        ));
        expected.push(
            (table == BTREE_REDB_TABLE)
                .then(|| benchmark_value(table, index)),
        );
    }
    DeletePlan {
        input,
        expected,
        expected_children: count.min(tables.len()),
    }
}

fn baseline_item(table: &'static str, index: usize) -> TableKV {
    TableKV::new(
        Atom::from(table),
        benchmark_key(table, index),
        Some(benchmark_value(table, index)),
    )
}

fn baseline_value_for_item(item: &TableKV) -> Binary {
    let table = item.table.as_str();
    let key = item.key.as_ref();
    for index in 0..BASELINE_KEYS {
        if key == benchmark_key(table, index).as_ref() {
            return benchmark_value(table, index);
        }
    }
    panic!(
        "root-delete benchmark input key was not present in the frozen baseline: table={table}, key_len={}",
        item.key.len(),
    );
}

fn benchmark_key(table: &str, index: usize) -> Binary {
    encode_bin(format!("{table}-key-{index:04}").as_bytes())
}

fn benchmark_value(table: &str, index: usize) -> Binary {
    let marker = match table {
        MEMORY_TABLE => 0x11,
        LOG_ORDERED_TABLE => 0x22,
        BTREE_CACHE_TABLE => 0x33,
        BTREE_REDB_TABLE => 0x44,
        _ => panic!("unknown root-delete benchmark table {table}"),
    };
    let mut bytes = vec![marker; 64];
    bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
    encode_bin(&bytes)
}

fn filler_value(index: usize) -> Binary {
    let mut bytes = vec![0x80 + index as u8; REDB_FILLER_BYTES];
    bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
    encode_bin(&bytes)
}

fn table_meta(table_type: KVDBTableType, persistence: bool) -> KVTableMeta {
    KVTableMeta::new(table_type, persistence, EnumType::Bin, EnumType::Bin)
}

fn writable_transaction(db: &RealDb, source: &str) -> RealTransaction {
    db.transaction(Atom::from(source), true, 10_000, 10_000)
        .expect("root-delete benchmark writable transaction must start")
}

fn encode_bin(bytes: &[u8]) -> Binary {
    let mut buffer = WriteBuffer::new();
    buffer.write_bin(bytes, 0..bytes.len());
    Binary::new(buffer.bytes)
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(label: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must not precede UNIX_EPOCH")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "pi_db_bench_root_delete_{label}_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path)
            .expect("creating root-delete benchmark root must succeed");
        Self { path }
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
