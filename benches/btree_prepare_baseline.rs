#![feature(test)]
//! Btree 普通事务冲突基线的独立真实环境性能基准。
//!
//! 本 target 不依赖历史 `benches/bench.rs`。四个场景使用真实 4-worker runtime、
//! `KVDBManager`、事务管理器、根 `CommitLogger`、Btree/redb 和临时文件系统，分别测量
//! 4 KiB 与 256 KiB value 的两条合法路径：
//!
//! - overlay 中仍有共享 `Binary` 时，prepare 走 allocation 身份 O(1) 快路径；
//! - collector 已把相同逻辑值写入 redb 并清空 overlay 时，query 与 prepare 分别解码独立
//!   allocation，prepare 走原始 bytes 回退。
//!
//! 每个样本都创建可写根事务，执行普通 query、`prepare_modified_conflicts` 和完整 commit。
//! 建表、初始写入和 redb collector 清空位于计时外；计时前会完整比较一次返回 bytes，样本内
//! 继续严格检查值长度和空 prepare 输出。结果仍包含事务创建、future 分派、两阶段管理器、
//! Btree 查询和空 WAL commit 成本，不是纯判等或 redb 裸读性能；只能在同机、同工具链和同
//! 依赖图下比较相对结果。语义与修复边界见
//! `docs/KEY_VERSION_BTREE_REDB_BASELINE_BUG.md#bug-kv-btree-redb-baseline-001-index`。

extern crate test;

use std::{
    env, fs,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime, AsyncRuntimeExt,
};
use pi_async_transaction::manager_2pc::Transaction2PcManager;
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
use test::{black_box, Bencher};

type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const TABLE: &str = "bench_btree_prepare_baseline";
const TARGET_KEY: usize = 1;
const SMALL_VALUE_BYTES: usize = 4 * 1024;
const LARGE_VALUE_BYTES: usize = 256 * 1024;
const REDB_FILLER_COUNT: usize = 3;
const REDB_FILLER_BYTES: usize = 400 * 1024;
const PERSISTENCE_DEADLINE: Duration = Duration::from_secs(30);

macro_rules! baseline_benchmark {
    ($name:ident, $storage:expr, $value_bytes:expr, $description:expr) => {
        #[doc = $description]
        #[bench]
        fn $name(b: &mut Bencher) {
            let _time_loop = startup_global_time_loop(10);
            let fixture = Fixture::new($storage, $value_bytes);
            fixture.verify_exact_value();
            let expected_len = fixture.expected.len();

            b.iter(|| {
                let observed = fixture.read_prepare_commit_once();
                assert_eq!(observed.len(), expected_len);
                black_box(observed)
            });
        }
    };
}

baseline_benchmark!(
    bench_btree_prepare_overlay_identity_4k,
    BaselineStorage::Overlay,
    SMALL_VALUE_BYTES,
    "测量 4 KiB overlay 共享 allocation 基线的普通 query/prepare/commit。"
);
baseline_benchmark!(
    bench_btree_prepare_redb_bytes_4k,
    BaselineStorage::Redb,
    SMALL_VALUE_BYTES,
    "测量 4 KiB redb 独立 allocation 基线的普通 query/prepare/commit。"
);
baseline_benchmark!(
    bench_btree_prepare_overlay_identity_256k,
    BaselineStorage::Overlay,
    LARGE_VALUE_BYTES,
    "测量 256 KiB overlay 共享 allocation 基线的普通 query/prepare/commit。"
);
baseline_benchmark!(
    bench_btree_prepare_redb_bytes_256k,
    BaselineStorage::Redb,
    LARGE_VALUE_BYTES,
    "测量 256 KiB redb 独立 allocation 基线的普通 query/prepare/commit。"
);

#[derive(Clone, Copy)]
enum BaselineStorage {
    Overlay,
    Redb,
}

impl BaselineStorage {
    const fn label(self) -> &'static str {
        match self {
            Self::Overlay => "overlay",
            Self::Redb => "redb",
        }
    }
}

/// 保持数据库、runtime、预期值和临时目录在一次基准的全部采样期间存活。
struct Fixture {
    db: RealDb,
    rt: MultiTaskRuntime<()>,
    expected: Binary,
    _root: TempRoot,
}

impl Fixture {
    fn new(storage: BaselineStorage, value_bytes: usize) -> Self {
        let root = TempRoot::new(storage.label(), value_bytes);
        let root_path = root.path().to_path_buf();
        let expected = encode_string_payload("target", value_bytes);
        let setup_expected = expected.clone();
        let rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(4)
            .build();
        let setup_rt = rt.clone();
        let (sender, receiver) = bounded(1);

        rt.block_on(async move {
            let logger = CommitLoggerBuilder::new(setup_rt.clone(), root_path.join("root-wal"))
                .log_file_limit(128 * 1024 * 1024)
                .build()
                .await
                .expect("Btree prepare benchmark CommitLogger must start");
            let manager = Transaction2PcManager::new(
                setup_rt.clone(),
                GuidGen::new(0, std::process::id() as u16),
                logger,
            );
            let db = KVDBManagerBuilder::new(setup_rt.clone(), manager, root_path.join("database"))
                .startup(false)
                .await
                .expect("Btree prepare benchmark database must start");

            create_table(&db).await;
            let writer = db
                .transaction(
                    Atom::from("Btree prepare benchmark initial writer"),
                    true,
                    10_000,
                    10_000,
                )
                .expect("Btree prepare benchmark writer must start");
            let mut writes = vec![table_kv(TARGET_KEY, Some(setup_expected))];
            if matches!(storage, BaselineStorage::Redb) {
                for index in 0..REDB_FILLER_COUNT {
                    writes.push(table_kv(
                        10 + index,
                        Some(encode_string_payload("filler", REDB_FILLER_BYTES)),
                    ));
                }
            }
            writer
                .upsert(writes)
                .await
                .expect("Btree prepare benchmark baseline write must succeed");
            commit(&writer, "initial writer").await;

            match storage {
                BaselineStorage::Overlay => {
                    let size = db
                        .table_cache_size(&Atom::from(TABLE))
                        .await
                        .expect("overlay benchmark table must remain registered");
                    assert!(size > 0, "overlay benchmark baseline must remain in cache");
                },
                BaselineStorage::Redb => {
                    wait_for_empty_cache(&setup_rt, &db).await;
                },
            }

            sender
                .send(db)
                .expect("Btree prepare benchmark fixture receiver must remain alive");
        })
        .expect("Btree prepare benchmark setup runtime must complete");

        Self {
            db: receiver
                .recv()
                .expect("Btree prepare benchmark fixture must be returned"),
            rt,
            expected,
            _root: root,
        }
    }

    /// 计时前完整验证一次 bytes，避免每轮 O(n) 断言掩盖被测判等成本。
    fn verify_exact_value(&self) {
        let observed = self.read_prepare_commit_once();
        assert_eq!(observed.as_ref(), self.expected.as_ref());
    }

    /// 执行一次合法的可写只读事务闭环，并返回 query 取得的 owned value。
    fn read_prepare_commit_once(&self) -> Binary {
        let transaction = self
            .db
            .transaction(
                Atom::from("Btree prepare benchmark sample"),
                true,
                10_000,
                10_000,
            )
            .expect("Btree prepare benchmark transaction must start");
        let input = table_kv(TARGET_KEY, None);

        self.rt
            .block_on(async move {
                let mut values = transaction.query(vec![input]).await;
                assert_eq!(values.len(), 1);
                let value = values
                    .pop()
                    .flatten()
                    .expect("Btree prepare benchmark query must return the baseline value");
                let prepare = transaction
                    .prepare_modified_conflicts()
                    .await
                    .expect("Btree prepare benchmark prepare must succeed");
                assert!(prepare.is_empty(), "read-only action set must not produce WAL bytes");
                transaction
                    .commit_modified(prepare)
                    .await
                    .expect("Btree prepare benchmark commit must succeed");
                value
            })
            .expect("Btree prepare benchmark runtime must execute the sample")
    }
}

async fn create_table(db: &RealDb) {
    let transaction = db
        .transaction(Atom::from("Btree prepare benchmark DDL"), true, 10_000, 10_000)
        .expect("Btree prepare benchmark DDL transaction must start");
    transaction
        .create_table_with_options(
            Atom::from(TABLE),
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
        .expect("Btree prepare benchmark table must be created");
    commit(&transaction, "DDL").await;
}

async fn commit(transaction: &RealTransaction, label: &str) {
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .unwrap_or_else(|error| panic!("Btree prepare benchmark {label} prepare failed: {error:?}"));
    transaction
        .commit_modified(prepare)
        .await
        .unwrap_or_else(|error| panic!("Btree prepare benchmark {label} commit failed: {error:?}"));
}

async fn wait_for_empty_cache(rt: &MultiTaskRuntime<()>, db: &RealDb) {
    let deadline = Instant::now() + PERSISTENCE_DEADLINE;
    loop {
        match db.table_cache_size(&Atom::from(TABLE)).await {
            Some(0) => return,
            Some(_) if Instant::now() < deadline => rt.timeout(10).await,
            Some(size) => panic!(
                "Btree prepare benchmark cache remained at {size} bytes after {PERSISTENCE_DEADLINE:?}"
            ),
            None => panic!("Btree prepare benchmark table disappeared during persistence"),
        }
    }
}

fn table_kv(key: usize, value: Option<Binary>) -> TableKV {
    TableKV::new(Atom::from(TABLE), encode_usize(key), value)
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn encode_string_payload(label: &str, payload_bytes: usize) -> Binary {
    assert!(label.len() <= payload_bytes);
    let mut value = String::with_capacity(payload_bytes);
    value.push_str(label);
    value.extend(std::iter::repeat('x').take(payload_bytes - label.len()));
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(storage: &str, value_bytes: usize) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Btree prepare benchmark clock must be after UNIX_EPOCH")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "pi_db_btree_prepare_bench_{storage}_{value_bytes}_{}_{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&path)
            .expect("Btree prepare benchmark temporary root must be created");
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
