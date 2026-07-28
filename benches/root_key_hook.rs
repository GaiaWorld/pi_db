#![feature(test)]
//! 根事务 `lock_key/unlock_key` 当前兼容钩子的独立真实环境基准。
//!
//! 六个场景使用真实 4-worker runtime、事务管理器、根 `CommitLogger`、Memory 表和临时
//! 文件系统，分别测量 lock/unlock 的首次命中已有表、复用已有表子事务和缺表路径。
//!
//! 首次命中和缺表样本包含根事务构造、Atom/Binary clone、runtime `block_on`、协议选择和
//! registry 查询；首次命中还包含 managed 子事务及 boxed future 分配。复用样本在计时外创建
//! 根并首次触表，计时内只测同一根再次调用。所有样本均不执行 prepare/commit，不产生业务
//! WAL；DDL、数据库启动和 fixture 构造不计时。
//!
//! 当前 hook 不提供锁语义，结果只能作为相同机器、工具链和依赖图下的内部成本基线，不能解释
//! 为真实 Key 锁吞吐或跨硬件 SLA。契约与口径见
//! `docs/ROOT_KEY_HOOK_CONTRACT.md#root-key-hook-benchmark`。

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

const MEMORY_TABLE: &str = "bench_root_key_hook_memory";
const MISSING_TABLE: &str = "bench_root_key_hook_missing";

static DB_RUNTIME: OnceLock<MultiTaskRuntime<()>> = OnceLock::new();

macro_rules! root_key_hook_benchmark {
    ($name:ident, $action:expr, $path:expr, $description:expr) => {
        #[doc = $description]
        #[bench]
        fn $name(b: &mut Bencher) {
            let _time_loop = startup_global_time_loop(10);
            let fixture = Fixture::new(stringify!($name));
            fixture.benchmark(b, $action, $path);
        }
    };
}

root_key_hook_benchmark!(
    bench_lock_key_first_existing_table,
    HookAction::Lock,
    HookPath::FirstExisting,
    "测量 fresh 根首次 lock_key 命中已有 Memory 表并创建 managed 子事务。"
);
root_key_hook_benchmark!(
    bench_unlock_key_first_existing_table,
    HookAction::Unlock,
    HookPath::FirstExisting,
    "测量 fresh 根首次 unlock_key 命中已有 Memory 表并创建 managed 子事务。"
);
root_key_hook_benchmark!(
    bench_lock_key_reused_existing_table,
    HookAction::Lock,
    HookPath::ReusedExisting,
    "测量已持有 Memory 子事务的根重复调用 lock_key。"
);
root_key_hook_benchmark!(
    bench_unlock_key_reused_existing_table,
    HookAction::Unlock,
    HookPath::ReusedExisting,
    "测量已持有 Memory 子事务的根重复调用 unlock_key。"
);
root_key_hook_benchmark!(
    bench_lock_key_missing_table,
    HookAction::Lock,
    HookPath::Missing,
    "测量 fresh 根调用 lock_key 的缺表成功路径。"
);
root_key_hook_benchmark!(
    bench_unlock_key_missing_table,
    HookAction::Unlock,
    HookPath::Missing,
    "测量 fresh 根调用 unlock_key 的缺表成功路径。"
);

#[derive(Clone, Copy)]
enum HookAction {
    Lock,
    Unlock,
}

impl HookAction {
    const fn label(self) -> &'static str {
        match self {
            Self::Lock => "lock",
            Self::Unlock => "unlock",
        }
    }
}

#[derive(Clone, Copy)]
enum HookPath {
    FirstExisting,
    ReusedExisting,
    Missing,
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
            .expect("root-key-hook benchmark CommitLogger must start");
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
            .startup(false)
            .await
            .expect("root-key-hook benchmark database must start");
            create_memory_table(&db).await;
            sender
                .send(db)
                .expect("root-key-hook benchmark fixture receiver must remain alive");
        })
        .expect("root-key-hook benchmark setup runtime must complete");

        Self {
            db: receiver
                .recv()
                .expect("root-key-hook benchmark fixture must be returned"),
            rt,
            _root: root,
        }
    }

    fn benchmark(&self, b: &mut Bencher, action: HookAction, path: HookPath) {
        let table = match path {
            HookPath::FirstExisting | HookPath::ReusedExisting => Atom::from(MEMORY_TABLE),
            HookPath::Missing => Atom::from(MISSING_TABLE),
        };
        let key = encode_bin(b"benchmark-key");
        let source = Atom::from(format!("root-key-hook benchmark {}", action.label()));

        match path {
            HookPath::ReusedExisting => {
                let transaction = self
                    .db
                    .transaction(source, true, 10_000, 10_000)
                    .expect("root-key-hook reused transaction must start");
                self.invoke(&transaction, action, table.clone(), key.clone());
                assert_eq!(transaction.children_len(), 1);
                b.iter(|| {
                    self.invoke(&transaction, action, table.clone(), key.clone());
                    assert_eq!(transaction.children_len(), 1);
                    black_box(())
                });
            },
            HookPath::FirstExisting | HookPath::Missing => {
                let expected_children = match path {
                    HookPath::FirstExisting => 1,
                    HookPath::Missing => 0,
                    HookPath::ReusedExisting => unreachable!(),
                };
                b.iter(|| {
                    let transaction = self
                        .db
                        .transaction(source.clone(), true, 10_000, 10_000)
                        .expect("root-key-hook fresh transaction must start");
                    self.invoke(&transaction, action, table.clone(), key.clone());
                    assert_eq!(transaction.children_len(), expected_children);
                    black_box(transaction)
                });
            },
        }
    }

    fn invoke(
        &self,
        transaction: &RealTransaction,
        action: HookAction,
        table: Atom,
        key: Binary,
    ) {
        let transaction = transaction.clone();
        self.rt
            .block_on(async move {
                let result = match action {
                    HookAction::Lock => transaction.lock_key(table, key).await,
                    HookAction::Unlock => transaction.unlock_key(table, key).await,
                };
                result.expect("root-key-hook benchmark hook must succeed");
            })
            .expect("root-key-hook benchmark runtime must execute one sample");
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

async fn create_memory_table(db: &RealDb) {
    let transaction = db
        .transaction(
            Atom::from("root-key-hook benchmark DDL"),
            true,
            10_000,
            10_000,
        )
        .expect("root-key-hook benchmark DDL transaction must start");
    transaction
        .create_table(
            Atom::from(MEMORY_TABLE),
            KVTableMeta::new(
                KVDBTableType::MemOrdTab,
                false,
                EnumType::Bin,
                EnumType::Bin,
            ),
            false,
        )
        .await
        .expect("root-key-hook benchmark Memory table must be created");
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .expect("root-key-hook benchmark DDL prepare must succeed");
    transaction
        .commit_modified(prepare)
        .await
        .expect("root-key-hook benchmark DDL commit must succeed");
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
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must follow Unix epoch")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "pi_db_root_key_hook_bench_{}_{}_{}",
            std::process::id(),
            label,
            unique,
        ));
        fs::create_dir_all(&path)
            .expect("root-key-hook benchmark temporary root must be created");
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
