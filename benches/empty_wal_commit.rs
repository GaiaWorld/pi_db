#![feature(test)]
//! 可写事务空 WAL 提交闭环的独立真实环境基准。
//!
//! 本 target 不依赖历史 `benches/bench.rs`。两个场景共享真实 4-worker runtime、
//! `Transaction2PcManager`、`CommitLogger`、Meta/Memory 表和临时文件系统：
//!
//! - 持久化可写事务对已存在 Memory 表执行同元信息幂等建表，prepare output 为空，但仍
//!   必须提交事务树；这是 `BUG-PI-ASYNC-TRANSACTION-004` 修复后的真实生产路径。
//! - 非持久化可写事务通过 `table_meta` 普通读取同一 Meta Key，再提交空 output；它提供既有
//!   空事务树提交路径的结构性参照。
//!
//! 两者都测量公开 API 分派、事务/子事务创建、prepare、runtime `block_on` 和 commit。幂等
//! 建表还包含注册表写锁、元信息比较和持久化标记，因此二者差值不是纯 manager 分支开销，
//! 也不能外推为跨机器固定延迟。每轮都严格断言 output 为空、根状态完成、manager 无残留且
//! WAL append 数不变。

extern crate test;

use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntimeExt,
};
use pi_async_transaction::{
    manager_2pc::{Transaction2PcManager, Transaction2PcStatus},
    AsyncCommitLog, UnitTransaction,
};
use pi_atom::Atom;
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder},
    KVDBTableType, KVTableMeta,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};
use test::{black_box, Bencher};

type RealDb = KVDBManager<usize, CommitLogger>;
type RealTrManager = Transaction2PcManager<usize, CommitLogger>;

#[bench]
fn bench_persistent_writable_idempotent_create_empty_wal(b: &mut Bencher) {
    let _time_loop = startup_global_time_loop(10);
    let fixture = Fixture::new();
    b.iter(|| black_box(fixture.idempotent_create_empty_wal()));
}

#[bench]
fn bench_nonpersistent_writable_meta_query_empty_wal(b: &mut Bencher) {
    let _time_loop = startup_global_time_loop(10);
    let fixture = Fixture::new();
    b.iter(|| black_box(fixture.meta_query_empty_wal()));
}

struct Fixture {
    db: RealDb,
    manager: RealTrManager,
    logger: CommitLogger,
    table: Atom,
    meta: KVTableMeta,
    rt: MultiTaskRuntime<()>,
    _root: TempRoot,
}

impl Fixture {
    fn new() -> Self {
        let root = TempRoot::new();
        let root_path = root.path().to_path_buf();
        let table = Atom::from("empty_wal_commit_bench_memory");
        let meta = KVTableMeta::new(
            KVDBTableType::MemOrdTab,
            true,
            EnumType::Usize,
            EnumType::Usize,
        );
        let rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(4)
            .build();
        let setup_rt = rt.clone();
        let setup_table = table.clone();
        let setup_meta = meta.clone();
        let (sender, receiver) = bounded(1);

        rt.block_on(async move {
            let logger = CommitLoggerBuilder::new(setup_rt.clone(), root_path.join("root-wal"))
                .log_file_limit(64 * 1024 * 1024)
                .build()
                .await
                .expect("empty-WAL benchmark CommitLogger must start");
            let manager = Transaction2PcManager::new(
                setup_rt.clone(),
                GuidGen::new(0, std::process::id() as u16),
                logger.clone(),
            );
            let db = KVDBManagerBuilder::new(
                setup_rt.clone(),
                manager.clone(),
                root_path.join("database"),
            )
            .startup(false)
            .await
            .expect("empty-WAL benchmark database must start");

            let transaction = db
                .transaction(Atom::from("empty-WAL benchmark initial DDL"), true, 10_000, 10_000)
                .expect("empty-WAL benchmark DDL transaction must start");
            transaction
                .create_table(setup_table, setup_meta, false)
                .await
                .expect("empty-WAL benchmark Memory table must be created");
            let prepare = transaction
                .prepare_modified()
                .await
                .expect("empty-WAL benchmark DDL prepare must succeed");
            assert!(!prepare.is_empty(), "initial DDL must produce WAL bytes");
            transaction
                .commit_modified(prepare)
                .await
                .expect("empty-WAL benchmark DDL commit must succeed");

            sender
                .send((db, manager, logger))
                .expect("empty-WAL benchmark fixture receiver must remain alive");
        })
        .expect("empty-WAL benchmark setup runtime must complete");

        let (db, manager, logger) = receiver
            .recv()
            .expect("empty-WAL benchmark fixture must be returned");
        Self {
            db,
            manager,
            logger,
            table,
            meta,
            rt,
            _root: root,
        }
    }

    fn idempotent_create_empty_wal(&self) -> usize {
        let db = self.db.clone();
        let manager = self.manager.clone();
        let logger = self.logger.clone();
        let table = self.table.clone();
        let meta = self.meta.clone();
        let append_before = logger.append_total_count();
        let result = self
            .rt
            .block_on(async move {
                let transaction = db
                    .transaction(
                        Atom::from("empty-WAL benchmark idempotent DDL"),
                        true,
                        10_000,
                        10_000,
                    )
                    .expect("idempotent DDL transaction must start");
                transaction
                    .create_table(table, meta, false)
                    .await
                    .expect("idempotent DDL action must succeed");
                let prepare = transaction
                    .prepare_modified_conflicts()
                    .await
                    .expect("idempotent DDL prepare must succeed");
                assert!(prepare.is_empty(), "idempotent DDL prepare must remain empty");
                transaction
                    .commit_modified(prepare)
                    .await
                    .expect("idempotent DDL empty commit must succeed");
                assert_eq!(transaction.get_status(), Transaction2PcStatus::Commited);
                assert_eq!(manager.transaction_len(), 0);
                1usize
            })
            .expect("idempotent DDL benchmark runtime must complete");
        assert_eq!(logger.append_total_count(), append_before);
        result
    }

    fn meta_query_empty_wal(&self) -> usize {
        let db = self.db.clone();
        let manager = self.manager.clone();
        let logger = self.logger.clone();
        let table = self.table.clone();
        let meta = self.meta.clone();
        let append_before = logger.append_total_count();
        let result = self
            .rt
            .block_on(async move {
                let transaction = db
                    .transaction(
                        Atom::from("empty-WAL benchmark Meta query"),
                        true,
                        10_000,
                        10_000,
                    )
                    .expect("Meta query transaction must start");
                assert_eq!(transaction.table_meta(table).await, Some(meta));
                let prepare = transaction
                    .prepare_modified_conflicts()
                    .await
                    .expect("Meta query prepare must succeed");
                assert!(prepare.is_empty(), "Meta query prepare must remain empty");
                transaction
                    .commit_modified(prepare)
                    .await
                    .expect("Meta query empty commit must succeed");
                assert_eq!(transaction.get_status(), Transaction2PcStatus::Commited);
                assert_eq!(manager.transaction_len(), 0);
                1usize
            })
            .expect("Meta query benchmark runtime must complete");
        assert_eq!(logger.append_total_count(), append_before);
        result
    }
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after UNIX_EPOCH")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "pi_db_empty_wal_commit_bench_{}_{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&path).expect("empty-WAL benchmark temporary root must be created");
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
