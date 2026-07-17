#![feature(test)]
//! Key 版本 TTL FIFO 索引的独立真实环境基准。
//!
//! 本 target 使用真实 4-worker runtime、`Transaction2PcManager`、根 `CommitLogger`、Memory 表
//! DDL 和文件系统。前三项比较首次观察在 TTL 关闭/开启时的公开热路径及 TTL cache hit；最后
//! 一项测量 4096 个首次观察版本从登记、分批扫描、淘汰到公开重新观察的完整闭环。
//!
//! 结果只适合同一机器、工具链和依赖图下的相对比较。到期闭环包含配置 TTL 等待、Guid 分配和
//! 公开查询成本，不是 FIFO/channel 的裸吞吐；它用于识别数量级退化、永久 token 或 scanner
//! 停滞，不能被解释为跨硬件延迟承诺。该 target 不进入普通回归入口。当前五对交错结果和
//! 解释边界见 `docs/KEY_VERSION_TTL_FIFO_ACCEPTANCE.md#kv-ttl-fifo-performance`。

extern crate test;

use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
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
    Binary, KVDBTableType, KVTableMeta, Version,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};
use test::{black_box, Bencher};

type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const TABLE_NAME: &str = "bench_key_version_ttl_memory";
const EXPIRY_BATCH_SIZE: usize = 4096;
const EXPIRY_TTL: Duration = Duration::from_millis(50);
const EXPIRY_TIMEOUT: Duration = Duration::from_secs(5);

#[bench]
fn bench_query_with_version_missing_ttl_disabled(b: &mut Bencher) {
    let _time_loop = startup_global_time_loop(10);
    let fixture = Fixture::new(TtlMode::Disabled);
    b.iter(|| black_box(fixture.observe_unique_missing()));
}

#[bench]
fn bench_query_with_version_missing_ttl_enabled(b: &mut Bencher) {
    let _time_loop = startup_global_time_loop(10);
    let fixture = Fixture::new(TtlMode::LongLived);
    b.iter(|| black_box(fixture.observe_unique_missing()));
}

#[bench]
fn bench_query_with_version_hit_ttl_enabled(b: &mut Bencher) {
    let _time_loop = startup_global_time_loop(10);
    let fixture = Fixture::new(TtlMode::LongLived);
    let (key, version) = fixture.create_cached_observation();
    b.iter(|| black_box(fixture.observe_cached(&key, &version)));
}

#[bench]
fn bench_ttl_expire_4096_first_observations(b: &mut Bencher) {
    let _time_loop = startup_global_time_loop(10);
    let fixture = Fixture::new(TtlMode::Expiring);
    b.iter(|| black_box(fixture.expire_unique_batch(EXPIRY_BATCH_SIZE)));
}

struct Fixture {
    db: RealDb,
    rt: MultiTaskRuntime<()>,
    table: Atom,
    next_key: AtomicUsize,
    _root: TempRoot,
}

impl Fixture {
    fn new(mode: TtlMode) -> Self {
        let root = TempRoot::new(mode.label());
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
                .expect("TTL index benchmark CommitLogger must start");
            let manager = Transaction2PcManager::new(
                setup_rt.clone(),
                GuidGen::new(0, std::process::id() as u16),
                logger,
            );
            let db = KVDBManagerBuilder::new(setup_rt.clone(),
                                             manager,
                                             root_path.join("database"))
                .key_version_ttl(mode.ttl())
                .key_version_ttl_poll_interval(mode.poll_interval())
                .startup(false)
                .await
                .expect("TTL index benchmark database must start");
            create_memory_table(&db).await;
            sender
                .send(db)
                .expect("TTL index benchmark fixture receiver must remain alive");
        })
        .expect("TTL index benchmark setup runtime must complete");

        Self {
            db: receiver
                .recv()
                .expect("TTL index benchmark fixture must be returned"),
            rt,
            table: Atom::from(TABLE_NAME),
            next_key: AtomicUsize::new(1_000_000),
            _root: root,
        }
    }

    fn observe_unique_missing(&self) -> Version {
        let key = self.next_key.fetch_add(1, Ordering::Relaxed);
        let db = self.db.clone();
        let table = self.table.clone();
        self.rt
            .block_on(async move {
                let (value, version) = db
                    .query_with_version(table, encode_usize(key))
                    .await
                    .expect("TTL index benchmark first observation must succeed");
                assert!(value.is_none(), "benchmark key must remain absent");
                Some(version)
            })
            .expect("TTL index benchmark runtime must execute first observation")
            .expect("TTL index benchmark first observation must return a version")
    }

    fn create_cached_observation(&self) -> (Binary, Version) {
        let key = encode_usize(self.next_key.fetch_add(1, Ordering::Relaxed));
        let db = self.db.clone();
        let table = self.table.clone();
        let query_key = key.clone();
        let version = self.rt
            .block_on(async move {
                let (value, version) = db
                    .query_with_version(table, query_key)
                    .await
                    .expect("TTL index benchmark cached baseline must succeed");
                assert!(value.is_none(), "benchmark cached key must remain absent");
                Some(version)
            })
            .expect("TTL index benchmark runtime must create cached baseline")
            .expect("TTL index benchmark cached baseline must return a version");
        (key, version)
    }

    fn observe_cached(&self, key: &Binary, expected: &Version) -> Version {
        let db = self.db.clone();
        let table = self.table.clone();
        let query_key = key.clone();
        let expected = expected.clone();
        self.rt
            .block_on(async move {
                let (value, version) = db
                    .query_with_version(table, query_key)
                    .await
                    .expect("TTL index benchmark cache hit must succeed");
                assert!(value.is_none(), "benchmark cached key must remain absent");
                assert_eq!(version, expected, "cache hit must not refresh the version");
                Some(version)
            })
            .expect("TTL index benchmark runtime must execute cache hit")
            .expect("TTL index benchmark cache hit must return a version")
    }

    fn expire_unique_batch(&self, count: usize) -> usize {
        let first = self.next_key.fetch_add(count, Ordering::Relaxed);
        let db = self.db.clone();
        let table = self.table.clone();
        let task_rt = self.rt.clone();
        self.rt
            .block_on(async move {
                let mut baselines = Vec::with_capacity(count);
                for offset in 0..count {
                    let key = encode_usize(first + offset);
                    let (value, version) = db
                        .query_with_version(table.clone(), key.clone())
                        .await
                        .expect("TTL index benchmark batch observation must succeed");
                    assert!(value.is_none(), "benchmark batch key must remain absent");
                    baselines.push((key, version));
                }

                let started = Instant::now();
                loop {
                    let mut changed = 0;
                    for (key, baseline) in &baselines {
                        let (value, version) = db
                            .query_with_version(table.clone(), key.clone())
                            .await
                            .expect("TTL index benchmark expiry observation must succeed");
                        assert!(value.is_none(), "TTL must not create or delete table data");
                        if version != *baseline {
                            changed += 1;
                        }
                    }
                    if changed == count {
                        return count;
                    }
                    assert!(started.elapsed() < EXPIRY_TIMEOUT,
                            "TTL index benchmark scanner did not retire the full batch");
                    task_rt.timeout(1).await;
                }
            })
            .expect("TTL index benchmark runtime must execute expiry batch")
    }
}

async fn create_memory_table(db: &RealDb) {
    let transaction = db
        .transaction(Atom::from("TTL index benchmark DDL"), true, 10_000, 10_000)
        .expect("TTL index benchmark DDL transaction must start");
    transaction
        .create_table(
            Atom::from(TABLE_NAME),
            KVTableMeta::new(KVDBTableType::MemOrdTab,
                             true,
                             EnumType::Usize,
                             EnumType::Usize),
            false,
        )
        .await
        .expect("TTL index benchmark Memory table must be created");
    commit(&transaction, "DDL").await;
}

async fn commit(transaction: &RealTransaction, label: &str) {
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .unwrap_or_else(|error| panic!("TTL index benchmark {label} prepare failed: {error:?}"));
    transaction
        .commit_modified(prepare)
        .await
        .unwrap_or_else(|error| panic!("TTL index benchmark {label} commit failed: {error:?}"));
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

#[derive(Clone, Copy)]
enum TtlMode {
    Disabled,
    LongLived,
    Expiring,
}

impl TtlMode {
    const fn label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::LongLived => "long-lived",
            Self::Expiring => "expiring",
        }
    }

    const fn ttl(self) -> Duration {
        match self {
            Self::Disabled => Duration::ZERO,
            Self::LongLived => Duration::from_secs(60 * 60),
            Self::Expiring => EXPIRY_TTL,
        }
    }

    const fn poll_interval(self) -> Duration {
        match self {
            Self::Disabled => Duration::ZERO,
            Self::LongLived => Duration::from_secs(60 * 3),
            Self::Expiring => Duration::from_millis(1),
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
            .expect("TTL index benchmark clock must be after UNIX_EPOCH")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "pi_db_key_version_ttl_bench_{label}_{}_{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&path)
            .expect("TTL index benchmark temporary root must be created");
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
