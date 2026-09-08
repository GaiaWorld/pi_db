//! 当前普通/版本事务精确 commit 的 Key 数扩展性基准。
//!
//! 每个规模和读形态使用两张独立、初始为空、非持久化的 Memory 表。普通与版本样本交错执行，
//! prepare、`query/query_with_version`、输入构造和结果校验都在计时区外；计时区只包围
//! `commit_modified` 或 `commit_with_version` 的包含式 wall time。该设计避免 WAL 定时批次掩盖
//! CPU/锁成本，也避免一种协议继承另一种协议已经扩容的 Key 版本 Map。
//!
//! 本基准只回答当前实现的增长曲线和普通/版本增量，不替代 `v0.19.0` 历史源码 A/B，也不代表
//! 持久化或高并发生产延迟。历史对照必须使用相同机器、工具链、依赖图和负载另行串行执行。

use std::{
    collections::HashSet,
    fs,
    future::Future,
    hint::black_box,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntimeExt,
};
use pi_async_transaction::{
    manager_2pc::{Transaction2PcManager, Transaction2PcStatus},
    AsyncCommitLog, Transaction2Pc, UnitTransaction,
};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    Binary, KVDBTableType, KVTableMeta, KVTableTrError, TableKeyVersion, Version,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type RealDb = KVDBManager<usize, CommitLogger>;
type RealManager = Transaction2PcManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const KEY_COUNTS: [usize; 13] = [
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096,
];
const WARMUP_PAIRS: usize = 2;

fn main() {
    let _time_loop = startup_global_time_loop(10);
    let cases = build_cases();
    let fixture = Fixture::new(&cases);

    println!(
        "version_commit_scaling: workers=4, warmup_pairs={}, volatile Memory, exact commit only",
        WARMUP_PAIRS,
    );
    println!(
        "{:<24} {:>6} {:>7} {:>12} {:>12} {:>12} {:>12} {:>10}",
        "shape", "keys", "samples", "ordinary_p50", "ordinary_p99", "version_p50",
        "version_p99", "p50_ratio",
    );

    for case in &cases {
        run_case(&fixture, case);
    }

    assert_eq!(fixture.manager.transaction_len(), 0);
    assert_eq!(
        fixture.manager.produced_transaction_total(),
        fixture.manager.consumed_transaction_total(),
        "all scaling benchmark roots must be consumed",
    );
}

fn build_cases() -> Vec<Case> {
    let mut cases = Vec::with_capacity(KEY_COUNTS.len() * 2);
    for shape in [ReadShape::WriteOnly, ReadShape::ReadBeforeWrite] {
        for key_count in KEY_COUNTS {
            cases.push(Case {
                shape,
                key_count,
                ordinary_table: Atom::from(format!(
                    "bench_scale_{}_{}_ordinary",
                    shape.label(),
                    key_count,
                )),
                version_table: Atom::from(format!(
                    "bench_scale_{}_{}_version",
                    shape.label(),
                    key_count,
                )),
            });
        }
    }
    cases
}

fn run_case(fixture: &Fixture, case: &Case) {
    for pair in 0..WARMUP_PAIRS {
        black_box(run_pair(fixture, case, pair));
    }

    let samples = sample_count(case.key_count);
    let mut ordinary = Vec::with_capacity(samples);
    let mut version = Vec::with_capacity(samples);
    for pair in 0..samples {
        let (ordinary_elapsed, version_elapsed) = run_pair(fixture, case, pair + WARMUP_PAIRS);
        ordinary.push(ordinary_elapsed);
        version.push(version_elapsed);
    }

    let ordinary = SampleStats::from_samples(&mut ordinary);
    let version = SampleStats::from_samples(&mut version);
    let ratio = version.p50 as f64 / ordinary.p50 as f64;
    println!(
        "{:<24} {:>6} {:>7} {:>12} {:>12} {:>12} {:>12} {:>9.3}x",
        case.shape.label(),
        case.key_count,
        samples,
        ordinary.p50,
        ordinary.p99,
        version.p50,
        version.p99,
        ratio,
    );
}

fn run_pair(fixture: &Fixture, case: &Case, pair: usize) -> (Duration, Duration) {
    let produced_before = fixture.manager.produced_transaction_total();
    let consumed_before = fixture.manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let ordinary = fixture.prepare_ordinary(case);
    let version = fixture.prepare_version(case);

    let elapsed = if pair % 2 == 0 {
        let ordinary_elapsed = fixture.commit_ordinary(ordinary);
        let version_elapsed = fixture.commit_version(version);
        (ordinary_elapsed, version_elapsed)
    } else {
        let version_elapsed = fixture.commit_version(version);
        let ordinary_elapsed = fixture.commit_ordinary(ordinary);
        (ordinary_elapsed, version_elapsed)
    };

    assert_eq!(fixture.manager.transaction_len(), 0);
    assert_eq!(
        fixture.manager.produced_transaction_total() - produced_before,
        2,
        "one pair must produce two root transactions",
    );
    assert_eq!(
        fixture.manager.consumed_transaction_total() - consumed_before,
        2,
        "one pair must consume two root transactions",
    );
    assert_eq!(
        fixture.logger.append_total_count(),
        append_before,
        "volatile Memory samples must not append root WAL",
    );
    elapsed
}

const fn sample_count(key_count: usize) -> usize {
    match key_count {
        1..=64 => 24,
        128..=512 => 16,
        1024..=2048 => 8,
        4096 => 5,
        _ => panic!("unsupported scaling benchmark Key count"),
    }
}

#[derive(Clone, Copy)]
enum ReadShape {
    WriteOnly,
    ReadBeforeWrite,
}

impl ReadShape {
    const fn label(self) -> &'static str {
        match self {
            Self::WriteOnly => "write_only",
            Self::ReadBeforeWrite => "read_before_write",
        }
    }
}

struct Case {
    shape: ReadShape,
    key_count: usize,
    ordinary_table: Atom,
    version_table: Atom,
}

struct PreparedOrdinary {
    transaction: RealTransaction,
    token: Vec<u8>,
    table: Atom,
    expected: Vec<(Binary, Binary)>,
}

struct PreparedVersion {
    transaction: RealTransaction,
    token: Vec<u8>,
    transaction_uid: pi_guid::Guid,
    table: Atom,
    expected: Vec<(Binary, Binary)>,
}

struct Fixture {
    db: RealDb,
    manager: RealManager,
    logger: CommitLogger,
    rt: MultiTaskRuntime<()>,
    next_key: AtomicUsize,
    _root: TempRoot,
}

impl Fixture {
    fn new(cases: &[Case]) -> Self {
        let root = TempRoot::new();
        let root_path = root.path().to_path_buf();
        let rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(4)
            .build();
        let setup_rt = rt.clone();
        let tables: Vec<Atom> = cases
            .iter()
            .flat_map(|case| [case.ordinary_table.clone(), case.version_table.clone()])
            .collect();
        let (sender, receiver) = bounded(1);

        rt.block_on(async move {
            let logger = CommitLoggerBuilder::new(
                setup_rt.clone(),
                root_path.join("root-wal"),
            )
            .log_file_limit(512 * 1024 * 1024)
            .collect_interval(5 * 60 * 1000)
            .build()
            .await
            .expect("scaling benchmark CommitLogger must start");
            let manager = Transaction2PcManager::new(
                setup_rt.clone(),
                GuidGen::new(0, std::process::id() as u16),
                logger.clone(),
            );
            let db = KVDBManagerBuilder::new(
                setup_rt,
                manager.clone(),
                root_path.join("database"),
            )
            .key_version_ttl(Duration::ZERO)
            .key_version_ttl_poll_interval(Duration::ZERO)
            .startup(false)
            .await
            .expect("scaling benchmark database must start");
            create_tables(&db, &tables).await;
            sender
                .send((db, manager, logger))
                .expect("scaling benchmark fixture receiver must remain alive");
        })
        .expect("scaling benchmark setup runtime must complete");

        let (db, manager, logger) = receiver
            .recv()
            .expect("scaling benchmark fixture must be returned");
        let fixture = Self {
            db,
            manager,
            logger,
            rt,
            next_key: AtomicUsize::new(0x5a00_0000),
            _root: root,
        };
        assert_eq!(fixture.manager.transaction_len(), 0);
        assert_eq!(fixture.logger.append_total_count(), 1);
        fixture
    }

    fn prepare_ordinary(&self, case: &Case) -> PreparedOrdinary {
        let (expected, input) = self.build_input(&case.ordinary_table, case.key_count, 0x1100);
        let transaction = self.writable_transaction("ordinary scaling sample");
        if matches!(case.shape, ReadShape::ReadBeforeWrite) {
            let queries = expected
                .iter()
                .map(|(key, _)| TableKV::new(case.ordinary_table.clone(), key.clone(), None))
                .collect();
            let query_transaction = transaction.clone();
            let values = self
                .run_operation(async move { Ok(query_transaction.query(queries).await) })
                .expect("ordinary scaling reads must succeed");
            assert_eq!(values, vec![None; case.key_count]);
        }
        let action_transaction = transaction.clone();
        self.run_operation(async move { action_transaction.upsert(input).await })
            .expect("ordinary scaling writes must succeed");
        let prepare_transaction = transaction.clone();
        let token = self
            .run_operation(async move { prepare_transaction.prepare_modified_conflicts().await })
            .expect("ordinary scaling prepare must succeed");
        assert!(token.is_empty());
        PreparedOrdinary {
            transaction,
            token,
            table: case.ordinary_table.clone(),
            expected,
        }
    }

    fn prepare_version(&self, case: &Case) -> PreparedVersion {
        let (expected, write_set) = self.build_input(&case.version_table, case.key_count, 0x2200);
        let read_set = if matches!(case.shape, ReadShape::ReadBeforeWrite) {
            let db = self.db.clone();
            let table = case.version_table.clone();
            let keys: Vec<Binary> = expected.iter().map(|(key, _)| key.clone()).collect();
            self.rt
                .block_on(async move {
                    let mut read_set = Vec::with_capacity(keys.len());
                    for key in keys {
                        let (value, version) = db
                            .query_with_version(table.clone(), key.clone())
                            .await
                            .expect("version scaling baseline read must succeed");
                        assert!(value.is_none(), "version scaling Key must start absent");
                        read_set.push(TableKeyVersion {
                            table: table.clone(),
                            key,
                            version,
                        });
                    }
                    Some(read_set)
                })
                .expect("version scaling baseline runtime must complete")
                .expect("version scaling baseline must preserve its read set")
        } else {
            Vec::new()
        };
        let transaction = self.writable_transaction("version scaling sample");
        let prepare_transaction = transaction.clone();
        let token = self
            .run_operation(async move {
                prepare_transaction
                    .prepare_with_version(read_set, write_set)
                    .await
            })
            .expect("version scaling prepare must succeed");
        assert!(token.is_empty());
        let transaction_uid = transaction
            .get_transaction_uid()
            .expect("version scaling prepare must allocate a transaction UID");
        PreparedVersion {
            transaction,
            token,
            transaction_uid,
            table: case.version_table.clone(),
            expected,
        }
    }

    fn commit_ordinary(&self, prepared: PreparedOrdinary) -> Duration {
        let phase_transaction = prepared.transaction.clone();
        let started = Instant::now();
        self.run_operation(async move {
            phase_transaction.commit_modified(prepared.token).await
        })
        .expect("ordinary scaling commit must succeed");
        let elapsed = started.elapsed();
        assert_eq!(
            prepared.transaction.get_status(),
            Transaction2PcStatus::Commited,
        );
        self.assert_values(&prepared.table, &prepared.expected);
        elapsed
    }

    fn commit_version(&self, prepared: PreparedVersion) -> Duration {
        let phase_transaction = prepared.transaction.clone();
        let started = Instant::now();
        let receipt = self
            .run_operation(async move {
                phase_transaction.commit_with_version(prepared.token).await
            })
            .expect("version scaling commit must succeed");
        let elapsed = started.elapsed();
        assert_eq!(
            prepared.transaction.get_status(),
            Transaction2PcStatus::Commited,
        );
        assert_eq!(receipt.len(), prepared.expected.len());
        let receipt_keys: HashSet<_> = receipt
            .iter()
            .map(|item| {
                assert_eq!(item.table, prepared.table);
                assert_eq!(
                    item.version,
                    Version::Upsert(prepared.transaction_uid.clone()),
                );
                item.key.clone()
            })
            .collect();
        assert_eq!(receipt_keys.len(), prepared.expected.len());
        for (key, _) in &prepared.expected {
            assert!(receipt_keys.contains(key));
        }
        self.assert_values(&prepared.table, &prepared.expected);
        elapsed
    }

    fn build_input(
        &self,
        table: &Atom,
        key_count: usize,
        value_mask: usize,
    ) -> (Vec<(Binary, Binary)>, Vec<TableKV>) {
        let first_key = self.next_key.fetch_add(key_count, Ordering::Relaxed);
        let expected: Vec<_> = (0..key_count)
            .map(|offset| {
                let key = first_key + offset;
                (encode_usize(key), encode_usize(key ^ value_mask))
            })
            .collect();
        let input = expected
            .iter()
            .map(|(key, value)| TableKV::new(table.clone(), key.clone(), Some(value.clone())))
            .collect();
        (expected, input)
    }

    fn assert_values(&self, table: &Atom, expected: &[(Binary, Binary)]) {
        let queries = expected
            .iter()
            .map(|(key, _)| TableKV::new(table.clone(), key.clone(), None))
            .collect();
        let transaction = self
            .db
            .transaction(
                Atom::from("scaling benchmark verification"),
                false,
                10_000,
                10_000,
            )
            .expect("scaling benchmark verification root must start");
        let query_transaction = transaction.clone();
        let values = self
            .rt
            .block_on(async move { Some(query_transaction.query(queries).await) })
            .expect("scaling benchmark verification runtime must complete")
            .expect("scaling benchmark verification values must be preserved");
        assert_eq!(values.len(), expected.len());
        for (index, (actual, (_, expected_value))) in values.iter().zip(expected).enumerate() {
            assert_eq!(
                actual.as_ref().map(AsRef::<[u8]>::as_ref),
                Some(expected_value.as_ref()),
                "scaling benchmark value mismatch at index {index}",
            );
        }
    }

    fn writable_transaction(&self, source: &str) -> RealTransaction {
        self.db
            .transaction(Atom::from(source), true, 10_000, 10_000)
            .expect("scaling benchmark writable root must start")
    }

    fn run_operation<T, F>(&self, future: F) -> Result<T, KVTableTrError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, KVTableTrError>> + Send + 'static,
    {
        self.rt
            .block_on(async move { CapturedOperation(Some(future.await)) })
            .expect("scaling benchmark runtime must complete")
            .0
            .expect("scaling benchmark runtime must preserve operation output")
    }
}

async fn create_tables(db: &RealDb, tables: &[Atom]) {
    let transaction = db
        .transaction(
            Atom::from("scaling benchmark DDL"),
            true,
            10_000,
            10_000,
        )
        .expect("scaling benchmark DDL root must start");
    for table in tables {
        transaction
            .create_table(table.clone(), memory_meta(false), false)
            .await
            .expect("scaling benchmark table must be created");
    }
    let token = transaction
        .prepare_modified_conflicts()
        .await
        .expect("scaling benchmark DDL prepare must succeed");
    assert!(token.len() > 16, "scaling benchmark DDL must emit Meta WAL");
    transaction
        .commit_modified(token)
        .await
        .expect("scaling benchmark DDL commit must succeed");
    assert_eq!(transaction.get_status(), Transaction2PcStatus::Commited);
    assert_eq!(db.table_size().await, tables.len() + 1);
}

fn memory_meta(persistence: bool) -> KVTableMeta {
    KVTableMeta::new(
        KVDBTableType::MemOrdTab,
        persistence,
        EnumType::Usize,
        EnumType::Usize,
    )
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

struct SampleStats {
    p50: u128,
    p99: u128,
}

impl SampleStats {
    fn from_samples(samples: &mut [Duration]) -> Self {
        assert!(!samples.is_empty());
        samples.sort_unstable();
        let nanos: Vec<u128> = samples.iter().map(Duration::as_nanos).collect();
        Self {
            p50: percentile(&nanos, 50),
            p99: percentile(&nanos, 99),
        }
    }
}

fn percentile(sorted: &[u128], percentile: usize) -> u128 {
    let index = (sorted.len() - 1) * percentile / 100;
    sorted[index]
}

struct CapturedOperation<T>(Option<Result<T, KVTableTrError>>);

impl<T> Default for CapturedOperation<T> {
    fn default() -> Self {
        Self(None)
    }
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must follow UNIX_EPOCH")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "pi_db_version_commit_scaling_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path)
            .expect("scaling benchmark temporary root must be created");
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
