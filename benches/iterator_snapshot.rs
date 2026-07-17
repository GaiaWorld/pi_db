#![feature(test)]
//! 创建时快照的独立公开 API 性能基准。
//!
//! 本 target 不依赖也不运行历史 `benches/bench.rs`。每个场景使用真实多线程 runtime、
//! `KVDBManager`、根事务、Memory 表、事务管理器和临时文件系统，分别测量 1K 与 64K 表：
//!
//! - 创建 `keys` 快照后立即 drop，用于观察创建成本是否随表长呈 O(n) 物化特征；
//! - 完整消费 `keys` 快照并严格核对项数，用于记录正常 O(n) 遍历成本。
//!
//! 基准在采样前预热并复用同一个创建事务，确保该事务始终活到流结束，也避免把首次子事务
//! 注册混入每轮。测量仍包含 runtime `block_on`、公开 API 分派和 stream Box/drop 成本；只能
//! 用相同环境下 1K/64K 的结构性比例判断，不应把单次墙钟值解释为跨机器吞吐保证。
//!
//! 冻结要求见 `docs/ITERATOR_SNAPSHOT_FIX_PLAN.md#8-专项与回归矩阵`，安全审查见
//! `docs/ITERATOR_SNAPSHOT_FIX_REVIEW.md#iter-fix-review-index`。

extern crate test;

use std::{
    env, fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use futures::StreamExt;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntimeExt,
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

const SMALL_LEN: usize = 1_024;
const LARGE_LEN: usize = 65_536;

/// 测量 1K Memory 表创建并立即释放快照流的公开 API 成本。
#[bench]
fn bench_memory_snapshot_create_1k(b: &mut Bencher) {
    let _time_loop = startup_global_time_loop(10);
    let fixture = Fixture::new(SMALL_LEN);
    b.iter(|| black_box(fixture.create_and_drop()));
}

/// 测量 64K Memory 表创建并立即释放快照流的公开 API 成本。
#[bench]
fn bench_memory_snapshot_create_64k(b: &mut Bencher) {
    let _time_loop = startup_global_time_loop(10);
    let fixture = Fixture::new(LARGE_LEN);
    b.iter(|| black_box(fixture.create_and_drop()));
}

/// 测量 1K Memory 表完整遍历创建时快照的端到端成本。
#[bench]
fn bench_memory_snapshot_traverse_1k(b: &mut Bencher) {
    let _time_loop = startup_global_time_loop(10);
    let fixture = Fixture::new(SMALL_LEN);
    b.iter(|| black_box(fixture.traverse_all()));
}

/// 测量 64K Memory 表完整遍历创建时快照的端到端成本。
#[bench]
fn bench_memory_snapshot_traverse_64k(b: &mut Bencher) {
    let _time_loop = startup_global_time_loop(10);
    let fixture = Fixture::new(LARGE_LEN);
    b.iter(|| black_box(fixture.traverse_all()));
}

/// 保持数据库、创建事务、运行时和临时目录在全部采样期间存活。
struct Fixture {
    _db: RealDb,
    transaction: RealTransaction,
    table: Atom,
    len: usize,
    rt: MultiTaskRuntime<()>,
    _root: TempRoot,
}

impl Fixture {
    fn new(len: usize) -> Self {
        let root = TempRoot::new();
        let root_path = root.path().to_path_buf();
        let table = Atom::from(format!("iterator_snapshot_bench_{len}"));
        let rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(4)
            .build();
        let setup_rt = rt.clone();
        let setup_table = table.clone();
        let (sender, receiver) = bounded(1);

        rt.block_on(async move {
            let logger = CommitLoggerBuilder::new(setup_rt.clone(), root_path.join("root-wal"))
                .log_file_limit(64 * 1024 * 1024)
                .build()
                .await
                .expect("iterator benchmark CommitLogger must start");
            let manager = Transaction2PcManager::new(
                setup_rt.clone(),
                GuidGen::new(0, std::process::id() as u16),
                logger,
            );
            let db = KVDBManagerBuilder::new(setup_rt.clone(), manager, root_path.join("database"))
                .startup(false)
                .await
                .expect("iterator benchmark database must start");

            create_memory_table(&db, setup_table.clone()).await;
            populate_memory_table(&db, setup_table.clone(), len).await;

            let transaction = db
                .transaction(
                    Atom::from("iterator benchmark snapshot owner"),
                    false,
                    10_000,
                    10_000,
                )
                .expect("iterator benchmark snapshot transaction must start");

            // 预热子事务注册并验证已提交基线；每次正式采样仍创建独立快照 owner。
            let mut stream = transaction
                .keys(setup_table.clone(), None, false)
                .await
                .expect("iterator benchmark table must remain registered");
            let mut observed = 0usize;
            while stream.next().await.is_some() {
                observed += 1;
            }
            assert_eq!(observed, len, "iterator benchmark baseline must be visible");
            drop(stream);

            sender
                .send((db, transaction))
                .expect("iterator benchmark fixture receiver must remain alive");
        })
        .expect("iterator benchmark setup runtime must complete");

        let (db, transaction) = receiver
            .recv()
            .expect("iterator benchmark fixture must be returned");
        Self {
            _db: db,
            transaction,
            table,
            len,
            rt,
            _root: root,
        }
    }

    fn create_and_drop(&self) -> usize {
        let transaction = self.transaction.clone();
        let table = self.table.clone();
        self.rt
            .block_on(async move {
                let stream = transaction
                    .keys(table, None, false)
                    .await
                    .expect("iterator benchmark table must remain registered");
                drop(stream);
                1usize
            })
            .expect("iterator benchmark runtime must create the stream")
    }

    fn traverse_all(&self) -> usize {
        let transaction = self.transaction.clone();
        let table = self.table.clone();
        let expected = self.len;
        let observed = self
            .rt
            .block_on(async move {
                let mut stream = transaction
                    .keys(table, None, false)
                    .await
                    .expect("iterator benchmark table must remain registered");
                let mut count = 0usize;
                while stream.next().await.is_some() {
                    count += 1;
                }
                count
            })
            .expect("iterator benchmark runtime must consume the stream");
        assert_eq!(observed, expected);
        observed
    }
}

async fn create_memory_table(db: &RealDb, table: Atom) {
    let transaction = db
        .transaction(Atom::from("iterator benchmark DDL"), true, 10_000, 10_000)
        .expect("iterator benchmark DDL transaction must start");
    transaction
        .create_table_with_options(
            table,
            KVTableMeta::new(
                KVDBTableType::MemOrdTab,
                false,
                EnumType::Usize,
                EnumType::Usize,
            ),
            CreateTableOptions::Empty,
            false,
        )
        .await
        .expect("iterator benchmark Memory table must be created");
    commit(&transaction, "DDL").await;
}

async fn populate_memory_table(db: &RealDb, table: Atom, len: usize) {
    // 第二个参数是可写性；只读根事务的预提交不会发布临时根。
    let transaction = db
        .transaction(
            Atom::from("iterator benchmark population"),
            true,
            10_000,
            10_000,
        )
        .expect("iterator benchmark population transaction must start");
    let entries = (0..len)
        .map(|index| {
            TableKV::new(
                table.clone(),
                encode_usize(index),
                Some(encode_usize(index)),
            )
        })
        .collect();
    transaction
        .upsert(entries)
        .await
        .expect("iterator benchmark baseline must be inserted");
    commit(&transaction, "population").await;
}

async fn commit(transaction: &RealTransaction, label: &str) {
    let prepare = transaction
        .prepare_modified()
        .await
        .unwrap_or_else(|error| panic!("iterator benchmark {label} prepare failed: {error:?}"));
    transaction
        .commit_modified(prepare)
        .await
        .unwrap_or_else(|error| panic!("iterator benchmark {label} commit failed: {error:?}"));
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("iterator benchmark clock must be after UNIX_EPOCH")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "pi_db_iterator_snapshot_bench_{}_{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&path).expect("iterator benchmark temporary root must be created");
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
