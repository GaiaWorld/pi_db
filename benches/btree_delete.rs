#![feature(test)]
//! Btree 删除旧值来源的独立公开 API 性能基准。
//!
//! 本 target 不依赖也不运行历史 `benches/bench.rs`。两个场景均使用真实多线程 runtime、
//! `KVDBManager`、根事务、Btree 表、事务管理器、根 WAL、redb 和临时文件系统，并在每次
//! 采样中创建一个事务、删除一个 4 KiB Key、校验精确旧值后放弃该未提交事务：
//!
//! - `bench_btree_shared_cache_delete` 从已提交共享只写缓存读取旧值；
//! - `bench_btree_redb_delete` 先让超过 1 MiB 的数据集完成持久化和缓存清理，再从 redb
//!   点读同一 Key 的旧值。
//!
//! 两个场景具有相同的公开 API、Key 大小、值类型、事务创建和释放边界，因此其差值主要
//! 反映缓存 O(log m) 查找与同步 `begin_read/open_table/get` 的差异。事务不提交是刻意的：
//! 它使每轮都从相同来源读取同一旧值，避免把 WAL、异步 collector 和物理删除混入单次
//! delete 延迟。结果仍包含 future 分派、参数构造和事务创建成本，不能解释为 redb 单次
//! `get` 的裸成本，也不能用单机一次墙钟值宣称跨机器性能结论。
//!
//! 语义与复杂度证据见 `docs/BTREE_DELETE_FIX_REVIEW.md#btree-delete-fix-review`。

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
use pi_bon::{Decode, Encode, ReadBuffer, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder},
    tables::TableKV,
    utils::CreateTableOptions,
    Binary, KVDBTableType, KVTableMeta,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};
use test::{black_box, Bencher};

type RealDb = KVDBManager<usize, CommitLogger>;

const SHARED_CACHE_TABLE: &str = "bench_btree_shared_cache_delete";
const REDB_TABLE: &str = "bench_btree_redb_delete";
const PERSISTED_KEYS: usize = 320;
const KEY_BYTES: usize = 4 * 1024;
const EXPECTED_VALUE: usize = 10_000;
const PERSISTENCE_DEADLINE: Duration = Duration::from_secs(30);

/// 测量从已提交共享只写缓存返回删除旧值的公开 API 端到端延迟。
#[bench]
fn bench_btree_shared_cache_delete(b: &mut Bencher) {
    let _time_loop = startup_global_time_loop(10);
    let fixture = Fixture::new();
    let key = persisted_key(0);

    b.iter(|| black_box(fixture.delete_once(SHARED_CACHE_TABLE, &key, EXPECTED_VALUE)));
}

/// 测量缓存完全缺席时同步点读 redb 并返回删除旧值的公开 API 端到端延迟。
#[bench]
fn bench_btree_redb_delete(b: &mut Bencher) {
    let _time_loop = startup_global_time_loop(10);
    let fixture = Fixture::new();
    let key = persisted_key(0);

    b.iter(|| black_box(fixture.delete_once(REDB_TABLE, &key, EXPECTED_VALUE)));
}

/// 保持数据库、运行时和临时目录在整个采样期间存活。
struct Fixture {
    db: RealDb,
    rt: MultiTaskRuntime<()>,
    _root: TempRoot,
}

impl Fixture {
    fn new() -> Self {
        let root = TempRoot::new();
        let root_path = root.path().to_path_buf();
        let rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(4)
            .build();
        let setup_rt = rt.clone();
        let (sender, receiver) = bounded(1);
        rt.block_on(async move {
            let logger = CommitLoggerBuilder::new(setup_rt.clone(), root_path.join("root-wal"))
                .log_file_limit(64 * 1024 * 1024)
                .build()
                .await
                .expect("benchmark CommitLogger must start");
            let manager = Transaction2PcManager::new(
                setup_rt.clone(),
                GuidGen::new(0, std::process::id() as u16),
                logger,
            );
            let db = KVDBManagerBuilder::new(setup_rt.clone(), manager, root_path.join("database"))
                .startup(false)
                .await
                .expect("benchmark database must start");

            create_table(&db, SHARED_CACHE_TABLE, false).await;
            create_table(&db, REDB_TABLE, true).await;

            let cache_writer = db
                .transaction(
                    Atom::from("benchmark shared-cache writer"),
                    true,
                    10_000,
                    10_000,
                )
                .expect("benchmark shared-cache transaction must start");
            cache_writer
                .upsert(vec![table_kv(
                    SHARED_CACHE_TABLE,
                    persisted_key(0),
                    Some(EXPECTED_VALUE),
                )])
                .await
                .expect("benchmark shared-cache baseline must be inserted");
            commit(&cache_writer, "shared-cache baseline").await;

            let redb_writer = db
                .transaction(Atom::from("benchmark redb writer"), true, 10_000, 10_000)
                .expect("benchmark redb transaction must start");
            redb_writer
                .upsert(
                    (0..PERSISTED_KEYS)
                        .map(|index| {
                            table_kv(
                                REDB_TABLE,
                                persisted_key(index),
                                Some(EXPECTED_VALUE + index),
                            )
                        })
                        .collect(),
                )
                .await
                .expect("benchmark redb baseline must be inserted");
            commit(&redb_writer, "redb baseline").await;
            wait_for_empty_cache(&setup_rt, &db, REDB_TABLE).await;

            sender
                .send(db)
                .expect("benchmark fixture receiver must remain alive");
        })
        .expect("benchmark setup runtime must complete");
        let db = receiver.recv().expect("benchmark fixture must be returned");

        Self {
            db,
            rt,
            _root: root,
        }
    }

    fn delete_once(&self, table: &str, key: &str, expected: usize) -> usize {
        let transaction = self
            .db
            .transaction(Atom::from("benchmark delete sample"), true, 10_000, 10_000)
            .expect("benchmark delete transaction must start");
        let input = table_kv(table, key.to_owned(), None);
        let observed = self
            .rt
            .block_on(async move {
                let deleted = transaction
                    .delete(vec![input])
                    .await
                    .expect("benchmark delete must succeed");
                let old = deleted
                    .into_iter()
                    .next()
                    .flatten()
                    .expect("benchmark delete must return its old value");
                decode_usize(&old)
            })
            .expect("benchmark runtime must execute delete");
        assert_eq!(observed, expected);
        observed
    }
}

async fn create_table(db: &RealDb, table: &str, persistence: bool) {
    let transaction = db
        .transaction(Atom::from("benchmark DDL"), true, 10_000, 10_000)
        .expect("benchmark DDL transaction must start");
    transaction
        .create_table_with_options(
            Atom::from(table),
            KVTableMeta::new(
                KVDBTableType::BtreeOrdTab,
                persistence,
                EnumType::Str,
                EnumType::Usize,
            ),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .expect("benchmark Btree table must be created");
    commit(&transaction, "DDL").await;
}

async fn commit(transaction: &pi_db::db::KVDBTransaction<usize, CommitLogger>, label: &str) {
    let prepare = transaction
        .prepare_modified()
        .await
        .unwrap_or_else(|error| panic!("benchmark {label} prepare failed: {error:?}"));
    transaction
        .commit_modified(prepare)
        .await
        .unwrap_or_else(|error| panic!("benchmark {label} commit failed: {error:?}"));
}

async fn wait_for_empty_cache(rt: &MultiTaskRuntime<()>, db: &RealDb, table: &str) {
    let deadline = Instant::now() + PERSISTENCE_DEADLINE;
    loop {
        match db.table_cache_size(&Atom::from(table)).await {
            Some(0) => return,
            Some(_) if Instant::now() < deadline => rt.timeout(10).await,
            Some(size) => panic!(
                "benchmark {table} cache remained at {size} bytes after {PERSISTENCE_DEADLINE:?}"
            ),
            None => panic!("benchmark table {table} disappeared during persistence"),
        }
    }
}

fn table_kv(table: &str, key: String, value: Option<usize>) -> TableKV {
    TableKV::new(
        Atom::from(table),
        encode_string(key),
        value.map(encode_usize),
    )
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn decode_usize(value: &Binary) -> usize {
    let mut buffer = ReadBuffer::new(value.as_ref(), 0);
    usize::decode(&mut buffer).expect("benchmark old value must be valid BON usize")
}

fn encode_string(value: String) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn persisted_key(index: usize) -> String {
    let prefix = format!("key={index:020};");
    assert!(prefix.len() <= KEY_BYTES);
    let fill = char::from(b'a' + (index % 26) as u8);
    let mut key = String::with_capacity(KEY_BYTES);
    key.push_str(&prefix);
    for _ in prefix.len()..KEY_BYTES {
        key.push(fill);
    }
    key
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("benchmark clock must be after UNIX_EPOCH")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "pi_db_btree_delete_bench_{}_{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&path).expect("benchmark temporary root must be created");
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
