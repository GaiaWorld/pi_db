#![feature(test)]
//! 根事务 `upsert/dirty_upsert` 动作阶段的独立真实环境基准。
//!
//! 十二个场景使用真实 4-worker runtime、事务管理器、根 `CommitLogger`、
//! Memory/LogOrdered/Btree/redb 和临时文件系统，分别测量三类表单项及三表 1/16/256 项
//! 混合批次的普通/dirty 动作登记。
//!
//! 每个正式样本创建一个全新可写根、clone 预构造输入、调用公开根 API，并直接释放尚未
//! prepare 的私有根。数据库启动、DDL、输入构造和严格私有值 preflight 位于计时外。结果只
//! 表示 action-stage 相对成本，不包含 prepare、根 WAL、commit、异步持久化、确认或 repair，
//! 不能作为完整事务延迟。正式口径见
//! `docs/ROOT_UPSERT_CONTRACT.md#root-upsert-benchmark`。

extern crate test;

use std::{
    env,
    fs,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop,
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

const MEMORY_TABLE: &str = "bench_root_upsert_memory";
const LOG_ORDERED_TABLE: &str = "bench_root_upsert_log_ordered";
const BTREE_TABLE: &str = "bench_root_upsert_btree";

static DB_RUNTIME: OnceLock<MultiTaskRuntime<()>> = OnceLock::new();

macro_rules! root_upsert_benchmark {
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

root_upsert_benchmark!(
    bench_memory_upsert_single,
    UpsertCase::MemorySingle,
    ActionMode::Ordinary,
    "测量 Memory 单项普通 upsert 动作阶段。"
);
root_upsert_benchmark!(
    bench_memory_dirty_upsert_single,
    UpsertCase::MemorySingle,
    ActionMode::Dirty,
    "测量 Memory 单项 dirty_upsert 动作阶段。"
);
root_upsert_benchmark!(
    bench_log_ordered_upsert_single,
    UpsertCase::LogOrderedSingle,
    ActionMode::Ordinary,
    "测量 LogOrdered 单项普通 upsert 动作阶段。"
);
root_upsert_benchmark!(
    bench_log_ordered_dirty_upsert_single,
    UpsertCase::LogOrderedSingle,
    ActionMode::Dirty,
    "测量 LogOrdered 单项 dirty_upsert 动作阶段。"
);
root_upsert_benchmark!(
    bench_btree_upsert_single,
    UpsertCase::BtreeSingle,
    ActionMode::Ordinary,
    "测量 Btree 单项普通 upsert 动作阶段。"
);
root_upsert_benchmark!(
    bench_btree_dirty_upsert_single,
    UpsertCase::BtreeSingle,
    ActionMode::Dirty,
    "测量 Btree 单项 dirty_upsert 动作阶段。"
);
root_upsert_benchmark!(
    bench_mixed_1_upsert,
    UpsertCase::Mixed1,
    ActionMode::Ordinary,
    "测量三表轮转 1 项普通 upsert 动作阶段。"
);
root_upsert_benchmark!(
    bench_mixed_1_dirty_upsert,
    UpsertCase::Mixed1,
    ActionMode::Dirty,
    "测量三表轮转 1 项 dirty_upsert 动作阶段。"
);
root_upsert_benchmark!(
    bench_mixed_16_upsert,
    UpsertCase::Mixed16,
    ActionMode::Ordinary,
    "测量三表轮转 16 项普通 upsert 动作阶段。"
);
root_upsert_benchmark!(
    bench_mixed_16_dirty_upsert,
    UpsertCase::Mixed16,
    ActionMode::Dirty,
    "测量三表轮转 16 项 dirty_upsert 动作阶段。"
);
root_upsert_benchmark!(
    bench_mixed_256_upsert,
    UpsertCase::Mixed256,
    ActionMode::Ordinary,
    "测量三表轮转 256 项普通 upsert 动作阶段。"
);
root_upsert_benchmark!(
    bench_mixed_256_dirty_upsert,
    UpsertCase::Mixed256,
    ActionMode::Dirty,
    "测量三表轮转 256 项 dirty_upsert 动作阶段。"
);

#[derive(Clone, Copy)]
enum ActionMode {
    Ordinary,
    Dirty,
}

impl ActionMode {
    const fn label(self) -> &'static str {
        match self {
            Self::Ordinary => "upsert",
            Self::Dirty => "dirty_upsert",
        }
    }
}

#[derive(Clone, Copy)]
enum UpsertCase {
    MemorySingle,
    LogOrderedSingle,
    BtreeSingle,
    Mixed1,
    Mixed16,
    Mixed256,
}

struct UpsertPlan {
    input: Vec<TableKV>,
    expected: Vec<Binary>,
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
            .expect("root-upsert benchmark CommitLogger must start");
            let manager = Transaction2PcManager::new(
                setup_rt.clone(),
                GuidGen::new(0, std::process::id() as u16),
                logger,
            );
            let db = KVDBManagerBuilder::new(
                setup_rt,
                manager,
                root_path.join("database"),
            )
            .key_version_ttl(Duration::ZERO)
            .key_version_ttl_poll_interval(Duration::ZERO)
            .startup(false)
            .await
            .expect("root-upsert benchmark database must start");
            create_tables(&db).await;
            sender
                .send(db)
                .expect("root-upsert benchmark fixture receiver must remain alive");
        })
        .expect("root-upsert benchmark setup runtime must complete");

        Self {
            db: receiver
                .recv()
                .expect("root-upsert benchmark fixture must be returned"),
            rt,
            _root: root,
        }
    }

    fn benchmark(&self, b: &mut Bencher, case: UpsertCase, mode: ActionMode) {
        let plan = upsert_plan(case);
        self.preflight(&plan, mode);

        b.iter(|| {
            let transaction = writable_transaction(
                &self.db,
                &format!("root-upsert benchmark {} sample", mode.label()),
            );
            let transaction_for_action = transaction.clone();
            let input = plan.input.clone();
            let observed_children = self
                .rt
                .block_on(async move {
                    match mode {
                        ActionMode::Ordinary => {
                            transaction_for_action.upsert(input).await
                        },
                        ActionMode::Dirty => {
                            transaction_for_action.dirty_upsert(input).await
                        },
                    }
                    .expect("root-upsert benchmark action must succeed");
                    transaction_for_action.children_len()
                })
                .expect("root-upsert benchmark runtime must execute one sample");
            assert_eq!(observed_children, plan.expected_children);
            black_box(observed_children);
            drop(transaction);
        });
    }

    fn preflight(&self, plan: &UpsertPlan, mode: ActionMode) {
        let transaction = writable_transaction(
            &self.db,
            &format!("root-upsert benchmark {} preflight", mode.label()),
        );
        let transaction_for_action = transaction.clone();
        let input = plan.input.clone();
        let query = plan
            .input
            .iter()
            .map(|item| {
                TableKV::new(
                    item.table.clone(),
                    item.key.clone(),
                    None,
                )
            })
            .collect();
        let observed = self
            .rt
            .block_on(async move {
                match mode {
                    ActionMode::Ordinary => {
                        transaction_for_action
                            .upsert(input)
                            .await
                            .expect("ordinary root-upsert preflight action must succeed");
                        transaction_for_action.query(query).await
                    },
                    ActionMode::Dirty => {
                        transaction_for_action
                            .dirty_upsert(input)
                            .await
                            .expect("dirty root-upsert preflight action must succeed");
                        transaction_for_action.dirty_query(query).await
                    },
                }
            })
            .expect("root-upsert benchmark preflight runtime must complete");
        assert_eq!(transaction.children_len(), plan.expected_children);
        assert_eq!(observed.len(), plan.expected.len());
        for (index, (actual, expected)) in observed.iter().zip(&plan.expected).enumerate() {
            assert_eq!(
                actual.as_ref().map(AsRef::<[u8]>::as_ref),
                Some(expected.as_ref()),
                "root-upsert benchmark preflight value mismatch at {index}",
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
    let transaction = writable_transaction(db, "root-upsert benchmark DDL");
    transaction
        .create_table(
            Atom::from(MEMORY_TABLE),
            table_meta(KVDBTableType::MemOrdTab),
            false,
        )
        .await
        .expect("root-upsert benchmark Memory table must be created");
    transaction
        .create_table_with_options(
            Atom::from(LOG_ORDERED_TABLE),
            table_meta(KVDBTableType::LogOrdTab),
            CreateTableOptions::LogOrdTab(
                64 * 1024 * 1024,
                1024 * 1024,
                1024 * 1024,
            ),
            false,
        )
        .await
        .expect("root-upsert benchmark LogOrdered table must be created");
    transaction
        .create_table_with_options(
            Atom::from(BTREE_TABLE),
            table_meta(KVDBTableType::BtreeOrdTab),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .expect("root-upsert benchmark Btree table must be created");
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .expect("root-upsert benchmark DDL must prepare");
    transaction
        .commit_modified(prepare)
        .await
        .expect("root-upsert benchmark DDL must commit");
}

fn upsert_plan(case: UpsertCase) -> UpsertPlan {
    match case {
        UpsertCase::MemorySingle => single_table_plan(MEMORY_TABLE),
        UpsertCase::LogOrderedSingle => single_table_plan(LOG_ORDERED_TABLE),
        UpsertCase::BtreeSingle => single_table_plan(BTREE_TABLE),
        UpsertCase::Mixed1 => mixed_plan(1),
        UpsertCase::Mixed16 => mixed_plan(16),
        UpsertCase::Mixed256 => mixed_plan(256),
    }
}

fn single_table_plan(table: &'static str) -> UpsertPlan {
    let key = encode_bin(format!("{table}-single-key").as_bytes());
    let value = benchmark_value(0, table.as_bytes()[0]);
    UpsertPlan {
        input: vec![TableKV::new(
            Atom::from(table),
            key,
            Some(value.clone()),
        )],
        expected: vec![value],
        expected_children: 1,
    }
}

fn mixed_plan(count: usize) -> UpsertPlan {
    let tables = [MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE];
    let mut input = Vec::with_capacity(count);
    let mut expected = Vec::with_capacity(count);
    for index in 0..count {
        let table = tables[index % tables.len()];
        let key = encode_bin(format!("mixed-{index:04}").as_bytes());
        let value = benchmark_value(index, 0x40 + (index % tables.len()) as u8);
        input.push(TableKV::new(
            Atom::from(table),
            key,
            Some(value.clone()),
        ));
        expected.push(value);
    }
    UpsertPlan {
        input,
        expected,
        expected_children: count.min(tables.len()),
    }
}

fn benchmark_value(index: usize, marker: u8) -> Binary {
    let mut bytes = vec![marker; 64];
    bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
    encode_bin(&bytes)
}

fn table_meta(table_type: KVDBTableType) -> KVTableMeta {
    KVTableMeta::new(table_type, true, EnumType::Bin, EnumType::Bin)
}

fn writable_transaction(db: &RealDb, source: &str) -> RealTransaction {
    db.transaction(Atom::from(source), true, 10_000, 10_000)
        .expect("root-upsert benchmark writable transaction must start")
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
            "pi_db_bench_root_upsert_{label}_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path)
            .expect("creating root-upsert benchmark root must succeed");
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
