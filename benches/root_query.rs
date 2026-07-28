#![feature(test)]
//! 根事务 `query/dirty_query` 稳态点读的独立真实环境基准。
//!
//! 十个场景使用真实 4-worker runtime、事务管理器、根 `CommitLogger`、Memory、
//! LogOrdered、Btree/redb 和临时文件系统，分别测量三表单 Key、Btree overlay/redb 两种
//! 来源及 48 项三表混合批次的普通/dirty 点读。每个 benchmark case 在计时外独立完成建表、
//! 初始写入、Btree collector 清空、overlay 二次写入、只读根创建、首次查询及完整值校验。
//!
//! 正式样本复用同一个显式只读根，包含输入 Vec/Binary clone、runtime `block_on`、公开根 API
//! 分派、逐项表注册表读锁、表子事务查询和返回 Vec 构造；不包含数据库启动、DDL、写事务、
//! collector 等待、prepare 或 commit。普通模式的 Read 基线已由预热建立，因此结果表示稳态
//! 重复点读，不表示首次触表或首次 Key 基线成本。Btree overlay 样本按设计保留尚未被
//! collector 搬入 redb 的已提交小批次；redb 样本则由 cache=0 硬门禁确认。
//!
//! 结果只用于相同机器、工具链和依赖图下的相对回归，不是跨硬件 SLA。契约与口径见
//! `docs/ROOT_QUERY_CONTRACT.md#root-query-benchmark`。

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
use pi_async_transaction::manager_2pc::Transaction2PcManager;
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
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};
use test::{black_box, Bencher};

type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const MEMORY_TABLE: &str = "bench_root_query_memory";
const LOG_ORDERED_TABLE: &str = "bench_root_query_log_ordered";
const BTREE_TABLE: &str = "bench_root_query_btree";

const MIXED_KEYS_PER_TABLE: usize = 16;
const MEMORY_KEY_BASE: usize = 1_000;
const LOG_ORDERED_KEY_BASE: usize = 2_000;
const BTREE_REDB_KEY_BASE: usize = 3_000;
const BTREE_OVERLAY_KEY_BASE: usize = 4_000;
const BTREE_FILLER_KEY_BASE: usize = 10_000;
const BTREE_FILLER_COUNT: usize = 3;
const BTREE_FILLER_BYTES: usize = 400 * 1024;
const BTREE_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

static DB_RUNTIME: OnceLock<MultiTaskRuntime<()>> = OnceLock::new();

macro_rules! root_query_benchmark {
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

root_query_benchmark!(
    bench_memory_query_single,
    QueryCase::MemorySingle,
    QueryMode::Ordinary,
    "测量 Memory 单 Key 普通稳态点读。"
);
root_query_benchmark!(
    bench_memory_dirty_query_single,
    QueryCase::MemorySingle,
    QueryMode::Dirty,
    "测量 Memory 单 Key dirty 稳态点读。"
);
root_query_benchmark!(
    bench_log_ordered_query_single,
    QueryCase::LogOrderedSingle,
    QueryMode::Ordinary,
    "测量 LogOrdered 单 Key 普通稳态点读。"
);
root_query_benchmark!(
    bench_log_ordered_dirty_query_single,
    QueryCase::LogOrderedSingle,
    QueryMode::Dirty,
    "测量 LogOrdered 单 Key dirty 稳态点读。"
);
root_query_benchmark!(
    bench_btree_overlay_query_single,
    QueryCase::BtreeOverlaySingle,
    QueryMode::Ordinary,
    "测量 Btree overlay 命中单 Key 普通稳态点读。"
);
root_query_benchmark!(
    bench_btree_overlay_dirty_query_single,
    QueryCase::BtreeOverlaySingle,
    QueryMode::Dirty,
    "测量 Btree overlay 命中单 Key dirty 稳态点读。"
);
root_query_benchmark!(
    bench_btree_redb_query_single,
    QueryCase::BtreeRedbSingle,
    QueryMode::Ordinary,
    "测量 Btree redb fallback 单 Key 普通稳态点读。"
);
root_query_benchmark!(
    bench_btree_redb_dirty_query_single,
    QueryCase::BtreeRedbSingle,
    QueryMode::Dirty,
    "测量 Btree redb fallback 单 Key dirty 稳态点读。"
);
root_query_benchmark!(
    bench_mixed_48_query,
    QueryCase::Mixed,
    QueryMode::Ordinary,
    "测量 Memory/LogOrdered/Btree 固定 48 项混合批次普通稳态点读。"
);
root_query_benchmark!(
    bench_mixed_48_dirty_query,
    QueryCase::Mixed,
    QueryMode::Dirty,
    "测量 Memory/LogOrdered/Btree 固定 48 项混合批次 dirty 稳态点读。"
);

#[derive(Clone, Copy)]
enum QueryMode {
    Ordinary,
    Dirty,
}

impl QueryMode {
    const fn label(self) -> &'static str {
        match self {
            Self::Ordinary => "query",
            Self::Dirty => "dirty_query",
        }
    }
}

#[derive(Clone, Copy)]
enum QueryCase {
    MemorySingle,
    LogOrderedSingle,
    BtreeOverlaySingle,
    BtreeRedbSingle,
    Mixed,
}

struct QueryPlan {
    input: Vec<TableKV>,
    expected: Vec<Binary>,
}

/// 一个 benchmark case 的独占数据库、存储和临时目录。
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
            let logger = CommitLoggerBuilder::new(setup_rt.clone(), root_path.join("root-wal"))
                .log_file_limit(128 * 1024 * 1024)
                .collect_interval(5 * 60 * 1000)
                .build()
                .await
                .expect("root-query benchmark CommitLogger must start");
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
            .startup(false)
            .await
            .expect("root-query benchmark database must start");

            create_tables(&db).await;
            seed_common_and_redb_values(&db).await;
            wait_for_empty_btree_cache(&setup_rt, &db).await;
            seed_btree_overlay_values(&db).await;
            let overlay_size = db
                .table_cache_size(&Atom::from(BTREE_TABLE))
                .await
                .expect("root-query benchmark Btree must remain registered");
            assert!(
                overlay_size > 0,
                "root-query benchmark overlay keys must remain in Btree cache",
            );

            sender
                .send(db)
                .expect("root-query benchmark fixture receiver must remain alive");
        })
        .expect("root-query benchmark setup runtime must complete");

        Self {
            db: receiver
                .recv()
                .expect("root-query benchmark fixture must be returned"),
            rt,
            _root: root,
        }
    }

    fn benchmark(&self, b: &mut Bencher, case: QueryCase, mode: QueryMode) {
        let transaction = self
            .db
            .transaction(
                Atom::from(format!("root-query benchmark {} reader", mode.label())),
                false,
                10_000,
                10_000,
            )
            .expect("root-query benchmark read-only transaction must start");
        let plan = query_plan(case);
        let prewarmed = self.query_once(&transaction, mode, plan.input.clone());
        assert_exact_values(&prewarmed, &plan.expected, "root-query benchmark prewarm");

        b.iter(|| {
            let observed = self.query_once(&transaction, mode, plan.input.clone());
            assert_eq!(observed.len(), plan.expected.len());
            assert!(
                observed.iter().all(Option::is_some),
                "root-query benchmark sample must not lose a seeded value",
            );
            black_box(observed)
        });
    }

    fn query_once(
        &self,
        transaction: &RealTransaction,
        mode: QueryMode,
        input: Vec<TableKV>,
    ) -> Vec<Option<Binary>> {
        let transaction = transaction.clone();
        self.rt
            .block_on(async move {
                match mode {
                    QueryMode::Ordinary => transaction.query(input).await,
                    QueryMode::Dirty => transaction.dirty_query(input).await,
                }
            })
            .expect("root-query benchmark runtime must execute one sample")
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
    let transaction = writable_transaction(db, "root-query benchmark DDL");
    transaction
        .create_table(
            Atom::from(MEMORY_TABLE),
            KVTableMeta::new(
                KVDBTableType::MemOrdTab,
                false,
                EnumType::Usize,
                EnumType::Str,
            ),
            false,
        )
        .await
        .expect("root-query benchmark Memory table must be created");
    transaction
        .create_table_with_options(
            Atom::from(LOG_ORDERED_TABLE),
            KVTableMeta::new(
                KVDBTableType::LogOrdTab,
                true,
                EnumType::Usize,
                EnumType::Str,
            ),
            CreateTableOptions::LogOrdTab(
                64 * 1024 * 1024,
                1024 * 1024,
                1024 * 1024,
            ),
            false,
        )
        .await
        .expect("root-query benchmark LogOrdered table must be created");
    transaction
        .create_table_with_options(
            Atom::from(BTREE_TABLE),
            KVTableMeta::new(
                KVDBTableType::BtreeOrdTab,
                true,
                EnumType::Usize,
                EnumType::Str,
            ),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .expect("root-query benchmark Btree table must be created");
    commit(&transaction, "DDL").await;
}

async fn seed_common_and_redb_values(db: &RealDb) {
    let transaction = writable_transaction(db, "root-query benchmark redb seed");
    let mut actions = Vec::with_capacity(
        MIXED_KEYS_PER_TABLE * 3 + BTREE_FILLER_COUNT,
    );
    for index in 0..MIXED_KEYS_PER_TABLE {
        actions.push(table_kv(
            MEMORY_TABLE,
            MEMORY_KEY_BASE + index,
            normal_value("memory", index),
        ));
        actions.push(table_kv(
            LOG_ORDERED_TABLE,
            LOG_ORDERED_KEY_BASE + index,
            normal_value("log-ordered", index),
        ));
        actions.push(table_kv(
            BTREE_TABLE,
            BTREE_REDB_KEY_BASE + index,
            normal_value("btree-redb", index),
        ));
    }
    for index in 0..BTREE_FILLER_COUNT {
        actions.push(table_kv(
            BTREE_TABLE,
            BTREE_FILLER_KEY_BASE + index,
            fixed_string_value("btree-filler", index, BTREE_FILLER_BYTES),
        ));
    }
    transaction
        .upsert(actions)
        .await
        .expect("root-query benchmark redb seed must succeed");
    commit(&transaction, "redb seed").await;
}

async fn seed_btree_overlay_values(db: &RealDb) {
    let transaction = writable_transaction(db, "root-query benchmark overlay seed");
    let actions = (0..MIXED_KEYS_PER_TABLE)
        .map(|index| {
            table_kv(
                BTREE_TABLE,
                BTREE_OVERLAY_KEY_BASE + index,
                normal_value("btree-overlay", index),
            )
        })
        .collect();
    transaction
        .upsert(actions)
        .await
        .expect("root-query benchmark overlay seed must succeed");
    commit(&transaction, "overlay seed").await;
}

async fn wait_for_empty_btree_cache(rt: &MultiTaskRuntime<()>, db: &RealDb) {
    let deadline = Instant::now() + BTREE_DRAIN_TIMEOUT;
    loop {
        match db.table_cache_size(&Atom::from(BTREE_TABLE)).await {
            Some(0) => return,
            Some(_) if Instant::now() < deadline => rt.timeout(10).await,
            Some(size) => panic!(
                "root-query benchmark Btree cache remained at {size} bytes after {BTREE_DRAIN_TIMEOUT:?}",
            ),
            None => panic!("root-query benchmark Btree disappeared during collector wait"),
        }
    }
}

fn writable_transaction(db: &RealDb, source: &str) -> RealTransaction {
    db.transaction(Atom::from(source), true, 10_000, 10_000)
        .expect("root-query benchmark writable transaction must start")
}

async fn commit(transaction: &RealTransaction, label: &str) {
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .unwrap_or_else(|error| {
            panic!("root-query benchmark {label} prepare failed: {error:?}")
        });
    transaction
        .commit_modified(prepare)
        .await
        .unwrap_or_else(|error| {
            panic!("root-query benchmark {label} commit failed: {error:?}")
        });
}

fn query_plan(case: QueryCase) -> QueryPlan {
    match case {
        QueryCase::MemorySingle => single_plan(
            MEMORY_TABLE,
            MEMORY_KEY_BASE,
            normal_value("memory", 0),
        ),
        QueryCase::LogOrderedSingle => single_plan(
            LOG_ORDERED_TABLE,
            LOG_ORDERED_KEY_BASE,
            normal_value("log-ordered", 0),
        ),
        QueryCase::BtreeOverlaySingle => single_plan(
            BTREE_TABLE,
            BTREE_OVERLAY_KEY_BASE,
            normal_value("btree-overlay", 0),
        ),
        QueryCase::BtreeRedbSingle => single_plan(
            BTREE_TABLE,
            BTREE_REDB_KEY_BASE,
            normal_value("btree-redb", 0),
        ),
        QueryCase::Mixed => {
            let mut input = Vec::with_capacity(MIXED_KEYS_PER_TABLE * 3);
            let mut expected = Vec::with_capacity(MIXED_KEYS_PER_TABLE * 3);
            for index in 0..MIXED_KEYS_PER_TABLE {
                input.push(query_kv(MEMORY_TABLE, MEMORY_KEY_BASE + index));
                expected.push(normal_value("memory", index));
                input.push(query_kv(
                    LOG_ORDERED_TABLE,
                    LOG_ORDERED_KEY_BASE + index,
                ));
                expected.push(normal_value("log-ordered", index));
                if index % 2 == 0 {
                    input.push(query_kv(
                        BTREE_TABLE,
                        BTREE_REDB_KEY_BASE + index,
                    ));
                    expected.push(normal_value("btree-redb", index));
                } else {
                    input.push(query_kv(
                        BTREE_TABLE,
                        BTREE_OVERLAY_KEY_BASE + index,
                    ));
                    expected.push(normal_value("btree-overlay", index));
                }
            }
            QueryPlan { input, expected }
        },
    }
}

fn single_plan(table: &str, key: usize, expected: Binary) -> QueryPlan {
    QueryPlan {
        input: vec![query_kv(table, key)],
        expected: vec![expected],
    }
}

fn table_kv(table: &str, key: usize, value: Binary) -> TableKV {
    TableKV::new(Atom::from(table), encode_usize(key), Some(value))
}

fn query_kv(table: &str, key: usize) -> TableKV {
    TableKV::new(Atom::from(table), encode_usize(key), None)
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn normal_value(table: &str, index: usize) -> Binary {
    encode_string(format!("{table}-value-{index:04}"))
}

fn fixed_string_value(
    prefix: &str,
    index: usize,
    payload_bytes: usize,
) -> Binary {
    let head = format!("{prefix}-{index:04};");
    assert!(head.len() <= payload_bytes);
    let mut value = String::with_capacity(payload_bytes);
    value.push_str(&head);
    value.extend(std::iter::repeat('x').take(payload_bytes - head.len()));
    encode_string(value)
}

fn encode_string(value: String) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn assert_exact_values(
    observed: &[Option<Binary>],
    expected: &[Binary],
    label: &str,
) {
    assert_eq!(
        observed.len(),
        expected.len(),
        "{label} returned the wrong slot count",
    );
    for (index, (observed, expected)) in observed.iter().zip(expected).enumerate() {
        let observed = observed
            .as_ref()
            .unwrap_or_else(|| panic!("{label} slot {index} unexpectedly returned None"));
        assert_eq!(
            observed.as_ref(),
            expected.as_ref(),
            "{label} slot {index} returned different bytes",
        );
    }
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(label: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("root-query benchmark clock must be after UNIX_EPOCH")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "pi_db_root_query_bench_{label}_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path)
            .expect("root-query benchmark temporary root must be created");
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
