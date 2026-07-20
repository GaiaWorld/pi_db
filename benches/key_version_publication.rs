#![feature(test)]
//! Key 版本发布协议的独立真实环境性能基准。
//!
//! 本文件成对测量 Meta、持久化 Memory 和 Btree：普通协议以 1/16/256 个 Key 执行
//! `upsert -> prepare_modified_conflicts -> commit_modified`；版本协议对同规模 Key 执行
//! `query_with_version -> prepare_with_version -> commit_with_version`。Memory 另测量幂等建表前导
//! 后的版本提交，以量化协议中立 Schema 子节点的固定成本；三类表另有稳定 cache-hit qwv。
//! 数据库、4-worker runtime、事务管理器、根 `CommitLogger`、redb 和临时文件系统均为真实组件；
//! 数据库启动、首次 DDL 和热读基线写入位于采样区间之外。
//!
//! 结果包含 Binary/BON 构造、版本读取、事务树创建、冲突检查、回执校验、WAL append/flush
//! 及表提交成本，不是底层 Map、锁或文件写的裸性能。Meta 使用真实存在的表定义记录，Memory
//! 与 Btree 批量提交使用唯一且初始不存在的 Usize Key。Btree 热读命中当前写缓存，不代表
//! redb-only 冷读。数值只能用于同一机器、同一工具链和同一依赖图下的相对比较，不能外推为
//! 跨机器吞吐承诺。设计与验收口径见
//! `docs/KEY_VERSION_PUBLICATION_IMPLEMENTATION_PLAN.md#kv-impl-acceptance`。

extern crate test;

use std::{
    collections::HashSet,
    env, fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntimeExt,
};
use pi_async_transaction::{Transaction2Pc, manager_2pc::Transaction2PcManager};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    utils::CreateTableOptions,
    Binary, KVDBTableType, KVTableMeta, TableKeyVersion, Version,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};
use test::{black_box, Bencher};

type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const META_TABLE: &str = ".tables_meta";
const MEMORY_TABLE: &str = "bench_key_version_memory";
const BTREE_TABLE: &str = "bench_key_version_btree";

macro_rules! ordinary_benchmark {
    ($name:ident, $table:expr, $keys:expr) => {
        #[doc = concat!(
                    "测量普通事务在真实 ",
                    stringify!($table),
                    " 表中提交 ",
                    stringify!($keys),
                    " 个唯一 Key 的端到端延迟。"
                )]
        #[bench]
        fn $name(b: &mut Bencher) {
            let _time_loop = startup_global_time_loop(10);
            let fixture = Fixture::new($table, $keys);
            b.iter(|| black_box(fixture.commit_ordinary_batch($keys)));
        }
    };
}

macro_rules! version_benchmark {
    ($name:ident, $table:expr, $keys:expr) => {
        #[doc = concat!(
                    "测量版本事务在真实 ",
                    stringify!($table),
                    " 表中读取版本并提交 ",
                    stringify!($keys),
                    " 个 Key 的端到端延迟。"
                )]
        #[bench]
        fn $name(b: &mut Bencher) {
            let _time_loop = startup_global_time_loop(10);
            let fixture = Fixture::new($table, $keys);
            b.iter(|| black_box(fixture.commit_version_batch($keys)));
        }
    };
}

macro_rules! schema_version_benchmark {
    ($name:ident, $keys:expr) => {
        #[doc = concat!(
                    "测量真实 Memory 表先执行幂等建表前导，再以版本事务提交 ",
                    stringify!($keys),
                    " 个唯一 Key 的端到端延迟。"
                )]
        #[bench]
        fn $name(b: &mut Bencher) {
            let _time_loop = startup_global_time_loop(10);
            let fixture = Fixture::new(BenchTable::Memory, $keys);
            b.iter(|| black_box(fixture.commit_version_batch_after_schema($keys)));
        }
    };
}

macro_rules! query_benchmark {
    ($name:ident, $table:expr) => {
        #[doc = concat!("测量真实 ", stringify!($table), " 表稳定 Key 的 qwv cache-hit 延迟。")]
        #[bench]
        fn $name(b: &mut Bencher) {
            let _time_loop = startup_global_time_loop(10);
            let fixture = Fixture::new($table, 1);
            let (key, value, version) = fixture.create_cached_query_target();
            b.iter(|| black_box(fixture.query_cached(&key, &value, &version)));
        }
    };
}

ordinary_benchmark!(bench_ordinary_meta_1_key, BenchTable::Meta, 1);
ordinary_benchmark!(bench_ordinary_meta_16_keys, BenchTable::Meta, 16);
ordinary_benchmark!(bench_ordinary_meta_256_keys, BenchTable::Meta, 256);
ordinary_benchmark!(bench_ordinary_memory_1_key, BenchTable::Memory, 1);
ordinary_benchmark!(bench_ordinary_memory_16_keys, BenchTable::Memory, 16);
ordinary_benchmark!(bench_ordinary_memory_256_keys, BenchTable::Memory, 256);
ordinary_benchmark!(bench_ordinary_btree_1_key, BenchTable::Btree, 1);
ordinary_benchmark!(bench_ordinary_btree_16_keys, BenchTable::Btree, 16);
ordinary_benchmark!(bench_ordinary_btree_256_keys, BenchTable::Btree, 256);
version_benchmark!(bench_version_meta_1_key, BenchTable::Meta, 1);
version_benchmark!(bench_version_meta_16_keys, BenchTable::Meta, 16);
version_benchmark!(bench_version_meta_256_keys, BenchTable::Meta, 256);
version_benchmark!(bench_version_memory_1_key, BenchTable::Memory, 1);
version_benchmark!(bench_version_memory_16_keys, BenchTable::Memory, 16);
version_benchmark!(bench_version_memory_256_keys, BenchTable::Memory, 256);
version_benchmark!(bench_version_btree_1_key, BenchTable::Btree, 1);
version_benchmark!(bench_version_btree_16_keys, BenchTable::Btree, 16);
version_benchmark!(bench_version_btree_256_keys, BenchTable::Btree, 256);
schema_version_benchmark!(bench_schema_version_memory_1_key, 1);
schema_version_benchmark!(bench_schema_version_memory_16_keys, 16);
query_benchmark!(bench_qwv_meta_cached, BenchTable::Meta);
query_benchmark!(bench_qwv_memory_cached, BenchTable::Memory);
query_benchmark!(bench_qwv_btree_overlay_cached, BenchTable::Btree);

/// 保持数据库、runtime 和临时目录在一次基准的全部采样期间存活。
struct Fixture {
    db: RealDb,
    rt: MultiTaskRuntime<()>,
    table_kind: BenchTable,
    table: Atom,
    meta_rows: Vec<(Binary, Binary)>,
    source: Atom,
    next_key: AtomicUsize,
    _root: TempRoot,
}

impl Fixture {
    /// 创建真实数据库，并在采样开始前完成目标用户表的 DDL 提交。
    fn new(table_kind: BenchTable, key_capacity: usize) -> Self {
        let root = TempRoot::new(table_kind.label());
        let root_path = root.path().to_path_buf();
        let rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(4)
            .build();
        let setup_rt = rt.clone();
        let (sender, receiver) = bounded(1);

        rt.block_on(async move {
            let logger = CommitLoggerBuilder::new(setup_rt.clone(), root_path.join("root-wal"))
                .log_file_limit(512 * 1024 * 1024)
                .collect_interval(5 * 60 * 1000)
                .build()
                .await
                .expect("key-version benchmark CommitLogger must start");
            let manager = Transaction2PcManager::new(
                setup_rt.clone(),
                GuidGen::new(0, std::process::id() as u16),
                logger,
            );
            let db = KVDBManagerBuilder::new(setup_rt.clone(), manager, root_path.join("database"))
                .startup(false)
                .await
                .expect("key-version benchmark database must start");

            let meta_rows = create_target_table(&db, table_kind, key_capacity).await;
            sender
                .send((db, meta_rows))
                .expect("key-version benchmark fixture receiver must remain alive");
        })
        .expect("key-version benchmark setup runtime must complete");

        let (db, meta_rows) = receiver
                .recv()
                .expect("key-version benchmark fixture must be returned");
        Self {
            db,
            rt,
            table_kind,
            table: Atom::from(table_kind.table_name()),
            meta_rows,
            source: Atom::from(table_kind.source()),
            // 避开启动过程写入 Meta 表的短 key；每个样本随后取得互不重叠的区间。
            next_key: AtomicUsize::new(1_000_000),
            _root: root,
        }
    }

    /// 提交一批真实类型的普通表动作；成功返回批量大小，任何错误都会使基准立即失败。
    fn commit_ordinary_batch(&self, key_count: usize) -> usize {
        let writes = self.build_writes(key_count);
        let transaction = self
            .db
            .transaction(self.source.clone(), true, 10_000, 10_000)
            .expect("key-version benchmark transaction must start");

        self.rt
            .block_on(async move {
                transaction
                    .upsert(writes)
                    .await
                    .expect("key-version benchmark upsert must succeed");
                commit(&transaction, "ordinary sample").await;
                key_count
            })
            .expect("key-version benchmark runtime must execute the sample")
    }

    /// 读取同批 Key 的版本后，以独立版本协议提交并严格校验回执。
    fn commit_version_batch(&self, key_count: usize) -> usize {
        let writes = self.build_writes(key_count);
        let db = self.db.clone();
        let table = self.table.clone();
        let source = self.source.clone();
        let table_kind = self.table_kind;
        self.rt
            .block_on(async move {
                let mut reads = Vec::with_capacity(key_count);
                for write in &writes {
                    let (value, version) = db
                        .query_with_version(table.clone(), write.key.clone())
                        .await
                        .expect("key-version benchmark qwv baseline must succeed");
                    match table_kind {
                        BenchTable::Meta => assert_eq!(
                            value.as_ref(),
                            write.value.as_ref(),
                            "Meta benchmark must read the real table definition",
                        ),
                        BenchTable::Memory | BenchTable::Btree => assert!(
                            value.is_none(),
                            "unique benchmark Key must be absent before version commit",
                        ),
                    }
                    reads.push(TableKeyVersion {
                        table: table.clone(),
                        key: write.key.clone(),
                        version,
                    });
                }

                let transaction = db
                    .transaction(source, true, 10_000, 10_000)
                    .expect("key-version benchmark version transaction must start");
                let prepare = transaction
                    .prepare_with_version(reads, writes.clone())
                    .await
                    .expect("key-version benchmark version prepare must succeed");
                let transaction_uid = transaction
                    .get_transaction_uid()
                    .expect("version prepare must allocate the root transaction UID");
                let receipt = transaction
                    .commit_with_version(prepare)
                    .await
                    .expect("key-version benchmark version commit must succeed");
                assert_eq!(receipt.len(), key_count, "version receipt length must match writes");
                let receipt_keys: HashSet<_> = receipt
                    .iter()
                    .map(|item| {
                        assert_eq!(item.table, table, "version receipt table must match");
                        assert_eq!(
                            item.version,
                            Version::Upsert(transaction_uid.clone()),
                            "version receipt must contain this transaction's UID",
                        );
                        item.key.clone()
                    })
                    .collect();
                assert_eq!(receipt_keys.len(), key_count, "version receipt keys must be unique");
                for write in &writes {
                    assert!(
                        receipt_keys.contains(&write.key),
                        "version receipt must contain every written Key",
                    );
                }
                key_count
            })
            .expect("key-version benchmark runtime must execute the version sample")
    }

    /// 在同一根事务先执行幂等建表前导，再完成版本提交并严格校验公开回执。
    ///
    /// 已存在且定义相同的表不会重复产生 Meta 写；该样本专门量化协议中立 Schema 子节点、
    /// 根协议选择和版本业务节点共同装配的固定成本，不代表首次物理建表或目录创建成本。
    fn commit_version_batch_after_schema(&self, key_count: usize) -> usize {
        assert!(matches!(self.table_kind, BenchTable::Memory));
        let writes = self.build_writes(key_count);
        let db = self.db.clone();
        let table = self.table.clone();
        let source = self.source.clone();
        self.rt
            .block_on(async move {
                let transaction = db
                    .transaction(source, true, 10_000, 10_000)
                    .expect("schema benchmark version transaction must start");
                transaction
                    .create_table(
                        table.clone(),
                        KVTableMeta::new(
                            KVDBTableType::MemOrdTab,
                            true,
                            EnumType::Usize,
                            EnumType::Usize,
                        ),
                        false,
                    )
                    .await
                    .expect("schema benchmark idempotent table prelude must succeed");

                let mut reads = Vec::with_capacity(key_count);
                for write in &writes {
                    let (value, version) = db
                        .query_with_version(table.clone(), write.key.clone())
                        .await
                        .expect("schema benchmark qwv baseline must succeed");
                    assert!(value.is_none(), "unique schema benchmark Key must be absent");
                    reads.push(TableKeyVersion {
                        table: table.clone(),
                        key: write.key.clone(),
                        version,
                    });
                }

                let prepare = transaction
                    .prepare_with_version(reads, writes.clone())
                    .await
                    .expect("schema benchmark version prepare must succeed");
                let transaction_uid = transaction
                    .get_transaction_uid()
                    .expect("schema benchmark prepare must allocate the root transaction UID");
                let receipt = transaction
                    .commit_with_version(prepare)
                    .await
                    .expect("schema benchmark version commit must succeed");
                assert_eq!(receipt.len(), key_count, "schema benchmark receipt length must match");
                let receipt_keys: HashSet<_> = receipt
                    .iter()
                    .map(|item| {
                        assert_eq!(item.table, table, "schema benchmark receipt table must match");
                        assert_eq!(
                            item.version,
                            Version::Upsert(transaction_uid.clone()),
                            "schema benchmark receipt must contain this transaction's UID",
                        );
                        item.key.clone()
                    })
                    .collect();
                assert_eq!(receipt_keys.len(), key_count, "schema benchmark keys must be unique");
                for write in &writes {
                    assert!(
                        receipt_keys.contains(&write.key),
                        "schema benchmark receipt must contain every written Key",
                    );
                }
                key_count
            })
            .expect("key-version benchmark runtime must execute the schema sample")
    }

    /// 为 cache-hit qwv 创建一个真实现存值，并返回稳定的值/版本基线。
    fn create_cached_query_target(&self) -> (Binary, Binary, Version) {
        let (key, expected) = match self.table_kind {
            BenchTable::Meta => self.meta_rows[0].clone(),
            BenchTable::Memory | BenchTable::Btree => {
                let raw_key = self.next_key.fetch_add(1, Ordering::Relaxed);
                let key = encode_usize(raw_key);
                let value = encode_usize(raw_key.wrapping_mul(17));
                let transaction = self
                    .db
                    .transaction(self.source.clone(), true, 10_000, 10_000)
                    .expect("qwv benchmark baseline transaction must start");
                let write = TableKV::new(self.table.clone(), key.clone(), Some(value.clone()));
                self.rt
                    .block_on(async move {
                        transaction
                            .upsert(vec![write])
                            .await
                            .expect("qwv benchmark baseline upsert must succeed");
                        commit(&transaction, "qwv baseline").await;
                    })
                    .expect("qwv benchmark runtime must create the baseline");
                (key, value)
            },
        };
        let db = self.db.clone();
        let table = self.table.clone();
        let query_key = key.clone();
        let expected_value = expected.clone();
        let version = self.rt
            .block_on(async move {
                let (value, version) = db
                    .query_with_version(table, query_key)
                    .await
                    .expect("qwv benchmark cache baseline must succeed");
                assert_eq!(value.as_ref(), Some(&expected_value));
                Some(version)
            })
            .expect("qwv benchmark runtime must observe the baseline")
            .expect("qwv benchmark baseline must return a version");
        (key, expected, version)
    }

    /// 重复读取同一 Key，保证 value 与当前版本始终成对返回。
    fn query_cached(&self, key: &Binary, expected: &Binary, version: &Version) -> Version {
        let db = self.db.clone();
        let table = self.table.clone();
        let query_key = key.clone();
        let expected_value = expected.clone();
        let expected_version = version.clone();
        self.rt
            .block_on(async move {
                let (value, version) = db
                    .query_with_version(table, query_key)
                    .await
                    .expect("qwv benchmark cache hit must succeed");
                assert_eq!(value.as_ref(), Some(&expected_value));
                assert_eq!(version, expected_version);
                Some(version)
            })
            .expect("qwv benchmark runtime must execute cache hit")
            .expect("qwv benchmark cache hit must return a version")
    }

    /// Meta 复用真实 DDL 记录，其它表为每个样本分配一段唯一 Key。
    fn build_writes(&self, key_count: usize) -> Vec<TableKV> {
        match self.table_kind {
            BenchTable::Meta => {
                assert!(key_count <= self.meta_rows.len());
                self.meta_rows
                    .iter()
                    .take(key_count)
                    .map(|(key, value)| {
                        TableKV::new(self.table.clone(), key.clone(), Some(value.clone()))
                    })
                    .collect()
            },
            BenchTable::Memory | BenchTable::Btree => {
                let first_key = self.next_key.fetch_add(key_count, Ordering::Relaxed);
                (0..key_count)
                    .map(|offset| {
                        let key = first_key + offset;
                        TableKV::new(
                            self.table.clone(),
                            encode_usize(key),
                            Some(encode_usize(key.wrapping_mul(17))),
                        )
                    })
                    .collect()
            },
        }
    }
}

/// 在真实根事务中建立当前基准所需的用户表；Meta 使用数据库启动时已有的表。
async fn create_target_table(db: &RealDb,
                             table_kind: BenchTable,
                             key_capacity: usize) -> Vec<(Binary, Binary)> {
    if matches!(table_kind, BenchTable::Meta) {
        return create_meta_benchmark_rows(db, key_capacity).await;
    }
    let (table_name, meta, options) = match table_kind {
        BenchTable::Meta => unreachable!(),
        BenchTable::Memory => (
            MEMORY_TABLE,
            KVTableMeta::new(
                KVDBTableType::MemOrdTab,
                true,
                EnumType::Usize,
                EnumType::Usize,
            ),
            None,
        ),
        BenchTable::Btree => (
            BTREE_TABLE,
            KVTableMeta::new(
                KVDBTableType::BtreeOrdTab,
                true,
                EnumType::Usize,
                EnumType::Usize,
            ),
            Some(CreateTableOptions::BtreeOrdTab(64 * 1024 * 1024, false)),
        ),
    };
    let transaction = db
        .transaction(
            Atom::from("key-version benchmark DDL"),
            true,
            10_000,
            10_000,
        )
        .expect("key-version benchmark DDL transaction must start");

    match options {
        Some(options) => transaction
            .create_table_with_options(Atom::from(table_name), meta, options, false)
            .await
            .expect("key-version benchmark table must be created"),
        None => transaction
            .create_table(Atom::from(table_name), meta, false)
            .await
            .expect("key-version benchmark table must be created"),
    }
    commit(&transaction, "DDL").await;
    Vec::new()
}

/// 创建真实 Memory 表并返回其 Meta 编码，避免用畸形 Usize bytes 污染 Meta 性能样本。
async fn create_meta_benchmark_rows(db: &RealDb,
                                    key_capacity: usize) -> Vec<(Binary, Binary)> {
    let transaction = db
        .transaction(
            Atom::from("key-version benchmark Meta DDL"),
            true,
            10_000,
            10_000,
        )
        .expect("key-version benchmark Meta DDL transaction must start");
    let meta = KVTableMeta::new(
        KVDBTableType::MemOrdTab,
        true,
        EnumType::Usize,
        EnumType::Usize,
    );
    let mut rows = Vec::with_capacity(key_capacity);
    for index in 0..key_capacity {
        let table_name = format!("bench_key_version_meta_{index:04}");
        transaction
            .create_table(Atom::from(table_name.as_str()), meta.clone(), false)
            .await
            .expect("key-version benchmark Meta row table must be created");
        rows.push((encode_atom(table_name.as_str()), Binary::from(meta.clone())));
    }
    commit(&transaction, "Meta DDL batch").await;
    rows
}

/// 执行普通协议的 prepare/commit；该 helper 同时用于 DDL 和采样事务。
async fn commit(transaction: &RealTransaction, label: &str) {
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .unwrap_or_else(|error| panic!("key-version benchmark {label} prepare failed: {error:?}"));
    transaction
        .commit_modified(prepare)
        .await
        .unwrap_or_else(|error| panic!("key-version benchmark {label} commit failed: {error:?}"));
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn encode_atom(value: &str) -> Binary {
    let mut buffer = WriteBuffer::new();
    Atom::from(value).encode(&mut buffer);
    Binary::new(buffer.bytes)
}

#[derive(Clone, Copy)]
enum BenchTable {
    Meta,
    Memory,
    Btree,
}

impl BenchTable {
    const fn label(self) -> &'static str {
        match self {
            Self::Meta => "meta",
            Self::Memory => "memory",
            Self::Btree => "btree",
        }
    }

    const fn table_name(self) -> &'static str {
        match self {
            Self::Meta => META_TABLE,
            Self::Memory => MEMORY_TABLE,
            Self::Btree => BTREE_TABLE,
        }
    }

    const fn source(self) -> &'static str {
        match self {
            Self::Meta => "key-version benchmark Meta sample",
            Self::Memory => "key-version benchmark Memory sample",
            Self::Btree => "key-version benchmark Btree sample",
        }
    }
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(label: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("key-version benchmark clock must be after UNIX_EPOCH")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "pi_db_key_version_bench_{label}_{}_{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&path).expect("key-version benchmark temporary root must be created");
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
