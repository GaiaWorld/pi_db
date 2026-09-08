//! 当前普通/版本事务精确 commit 的 WAL 字节数与 Key 数敏感性基准。
//!
//! 每个场景使用两张独立、持久化 Memory 表。Memory 的业务数据只发布到内存根，但其动作会
//! 进入真实根 WAL，因此本基准覆盖真实 runtime、事务管理器、CommitLogger、文件系统、
//! `delay_commit` 和 `sync_data`，同时排除 Btree/LogOrdered 数据文件异步持久化。
//!
//! prepare、输入构造、最终值校验和 WAL 最终确认等待均在计时区外；计时区只包围
//! `commit_modified` 或 `commit_with_version` 的包含式 wall time。固定小 Value 的场景观察
//! Key 数成本，固定约 256 KiB 业务 Value 的场景分离总字节数与 Key 数，单 Key 场景观察
//! WAL payload 增长。普通/版本提交交错执行，且每次提交确认收口后才进入下一次计时。

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
    Binary, KVDBTableType, KVTableMeta, KVTableTrError, Version,
};
use pi_guid::{Guid, GuidGen};
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type RealDb = KVDBManager<usize, CommitLogger>;
type RealManager = Transaction2PcManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const WARMUP_PAIRS: usize = 2;
const SAMPLES: usize = 7;
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(10);
const CASES: [CaseSpec; 10] = [
    CaseSpec::new("small_keys_1", 1, 8),
    CaseSpec::new("small_keys_16", 16, 8),
    CaseSpec::new("small_keys_256", 256, 8),
    CaseSpec::new("small_keys_1024", 1024, 8),
    CaseSpec::new("fixed_256k_keys_1", 1, 256 * 1024),
    CaseSpec::new("fixed_256k_keys_16", 16, 16 * 1024),
    CaseSpec::new("fixed_256k_keys_256", 256, 1024),
    CaseSpec::new("one_key_value_4k", 1, 4 * 1024),
    CaseSpec::new("one_key_value_80k", 1, 80 * 1024),
    CaseSpec::new("one_key_value_1m", 1, 1024 * 1024),
];

fn main() {
    let _time_loop = startup_global_time_loop(10);
    let cases = build_cases();
    let fixture = Fixture::new(&cases);

    println!(
        "version_commit_wal_payload: workers=4, warmup_pairs={}, samples={}, persistent Memory, exact commit only",
        WARMUP_PAIRS,
        SAMPLES,
    );
    println!(
        "{:<24} {:>6} {:>11} {:>12} {:>12} {:>12} {:>12} {:>10}",
        "case", "keys", "value_bytes", "wal_bytes", "ordinary_p50", "version_p50",
        "version_p99", "p50_ratio",
    );

    for case in &cases {
        run_case(&fixture, case);
    }

    assert_eq!(fixture.manager.transaction_len(), 0);
    assert_eq!(
        fixture.manager.produced_transaction_total(),
        fixture.manager.consumed_transaction_total(),
        "all WAL payload benchmark roots must be consumed",
    );
}

fn build_cases() -> Vec<Case> {
    CASES
        .iter()
        .map(|spec| Case {
            spec: *spec,
            ordinary_table: Atom::from(format!("bench_wal_{}_ord", spec.label)),
            version_table: Atom::from(format!("bench_wal_{}_ver", spec.label)),
        })
        .collect()
}

fn run_case(fixture: &Fixture, case: &Case) {
    for pair in 0..WARMUP_PAIRS {
        black_box(run_pair(fixture, case, pair));
    }

    let mut ordinary = Vec::with_capacity(SAMPLES);
    let mut version = Vec::with_capacity(SAMPLES);
    let mut wal_bytes = None;
    for pair in 0..SAMPLES {
        let sample = run_pair(fixture, case, pair + WARMUP_PAIRS);
        ordinary.push(sample.ordinary);
        version.push(sample.version);
        match wal_bytes {
            Some(expected) => assert_eq!(sample.wal_bytes, expected),
            None => wal_bytes = Some(sample.wal_bytes),
        }
    }

    let ordinary = SampleStats::from_samples(&mut ordinary);
    let version = SampleStats::from_samples(&mut version);
    let ratio = version.p50 as f64 / ordinary.p50 as f64;
    println!(
        "{:<24} {:>6} {:>11} {:>12} {:>12} {:>12} {:>12} {:>9.3}x",
        case.spec.label,
        case.spec.key_count,
        case.spec.value_len,
        wal_bytes.expect("measured case must report WAL bytes"),
        ordinary.p50,
        version.p50,
        version.p99,
        ratio,
    );
}

fn run_pair(fixture: &Fixture, case: &Case, pair: usize) -> PairSample {
    let produced_before = fixture.manager.produced_transaction_total();
    let consumed_before = fixture.manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let ordinary = fixture.prepare_ordinary(case);
    let version = fixture.prepare_version(case);
    assert_eq!(
        ordinary.token.len(),
        version.token.len(),
        "equal-shape ordinary/version writes must emit equal-size root WAL",
    );
    let wal_bytes = ordinary.token.len();

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
        fixture.logger.append_total_count() - append_before,
        2,
        "each persistent Memory root must append exactly one WAL record",
    );
    PairSample {
        ordinary: elapsed.0,
        version: elapsed.1,
        wal_bytes,
    }
}

#[derive(Clone, Copy)]
struct CaseSpec {
    label: &'static str,
    key_count: usize,
    value_len: usize,
}

impl CaseSpec {
    const fn new(label: &'static str, key_count: usize, value_len: usize) -> Self {
        Self {
            label,
            key_count,
            value_len,
        }
    }
}

struct Case {
    spec: CaseSpec,
    ordinary_table: Atom,
    version_table: Atom,
}

struct PreparedOrdinary {
    transaction: RealTransaction,
    token: Vec<u8>,
    commit_uid: Guid,
    table: Atom,
    expected: Vec<(Binary, Binary)>,
}

struct PreparedVersion {
    transaction: RealTransaction,
    token: Vec<u8>,
    transaction_uid: Guid,
    commit_uid: Guid,
    table: Atom,
    expected: Vec<(Binary, Binary)>,
}

struct PairSample {
    ordinary: Duration,
    version: Duration,
    wal_bytes: usize,
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
            .expect("WAL payload benchmark CommitLogger must start");
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
            .expect("WAL payload benchmark database must start");
            create_tables(&db, &tables).await;
            sender
                .send((db, manager, logger))
                .expect("WAL payload benchmark fixture receiver must remain alive");
        })
        .expect("WAL payload benchmark setup runtime must complete");

        let (db, manager, logger) = receiver
            .recv()
            .expect("WAL payload benchmark fixture must be returned");
        let fixture = Self {
            db,
            manager,
            logger,
            rt,
            next_key: AtomicUsize::new(0x6b00_0000),
            _root: root,
        };
        assert_eq!(fixture.manager.transaction_len(), 0);
        assert_eq!(fixture.logger.append_total_count(), 1);
        fixture
    }

    fn prepare_ordinary(&self, case: &Case) -> PreparedOrdinary {
        let (expected, input) = self.build_input(case, &case.ordinary_table, 0x31);
        let transaction = self.writable_transaction("ordinary WAL payload sample");
        let action_transaction = transaction.clone();
        self.run_operation(async move { action_transaction.upsert(input).await })
            .expect("ordinary WAL payload writes must succeed");
        let prepare_transaction = transaction.clone();
        let token = self
            .run_operation(async move { prepare_transaction.prepare_modified_conflicts().await })
            .expect("ordinary WAL payload prepare must succeed");
        assert!(token.len() > 16, "persistent Memory prepare must emit WAL");
        PreparedOrdinary {
            commit_uid: transaction
                .get_commit_uid()
                .expect("ordinary WAL payload root must have a commit UID"),
            transaction,
            token,
            table: case.ordinary_table.clone(),
            expected,
        }
    }

    fn prepare_version(&self, case: &Case) -> PreparedVersion {
        let (expected, write_set) = self.build_input(case, &case.version_table, 0x31);
        let transaction = self.writable_transaction("version WAL payload sample");
        let prepare_transaction = transaction.clone();
        let token = self
            .run_operation(async move {
                prepare_transaction
                    .prepare_with_version(Vec::new(), write_set)
                    .await
            })
            .expect("version WAL payload prepare must succeed");
        assert!(token.len() > 16, "persistent Memory prepare must emit WAL");
        PreparedVersion {
            transaction_uid: transaction
                .get_transaction_uid()
                .expect("version WAL payload root must have a transaction UID"),
            commit_uid: transaction
                .get_commit_uid()
                .expect("version WAL payload root must have a commit UID"),
            transaction,
            token,
            table: case.version_table.clone(),
            expected,
        }
    }

    fn commit_ordinary(&self, prepared: PreparedOrdinary) -> Duration {
        let confirm_before = self.logger.confirm_total_count();
        let waiting_before = self.waiting_confirm_count();
        let phase_transaction = prepared.transaction.clone();
        let started = Instant::now();
        self.run_operation(async move {
            phase_transaction.commit_modified(prepared.token).await
        })
        .expect("ordinary WAL payload commit must succeed");
        let elapsed = started.elapsed();
        assert_eq!(
            prepared.transaction.get_status(),
            Transaction2PcStatus::Commited,
        );
        self.assert_values(&prepared.table, &prepared.expected);
        self.wait_for_confirmation(prepared.commit_uid, confirm_before, waiting_before);
        elapsed
    }

    fn commit_version(&self, prepared: PreparedVersion) -> Duration {
        let confirm_before = self.logger.confirm_total_count();
        let waiting_before = self.waiting_confirm_count();
        let phase_transaction = prepared.transaction.clone();
        let started = Instant::now();
        let receipt = self
            .run_operation(async move {
                phase_transaction.commit_with_version(prepared.token).await
            })
            .expect("version WAL payload commit must succeed");
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
        self.wait_for_confirmation(prepared.commit_uid, confirm_before, waiting_before);
        elapsed
    }

    fn build_input(
        &self,
        case: &Case,
        table: &Atom,
        value_seed: u8,
    ) -> (Vec<(Binary, Binary)>, Vec<TableKV>) {
        let first_key = self
            .next_key
            .fetch_add(case.spec.key_count, Ordering::Relaxed);
        let expected: Vec<_> = (0..case.spec.key_count)
            .map(|offset| {
                let key = first_key + offset;
                let mut value = vec![value_seed.wrapping_add(offset as u8); case.spec.value_len];
                if !value.is_empty() {
                    value[0] ^= (key & 0xff) as u8;
                }
                (encode_usize(key), Binary::new(value))
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
                Atom::from("WAL payload benchmark verification"),
                false,
                10_000,
                10_000,
            )
            .expect("WAL payload benchmark verification root must start");
        let query_transaction = transaction.clone();
        let values = self
            .rt
            .block_on(async move { Some(query_transaction.query(queries).await) })
            .expect("WAL payload benchmark verification runtime must complete")
            .expect("WAL payload benchmark verification values must be preserved");
        assert_eq!(values.len(), expected.len());
        for (index, (actual, (_, expected_value))) in values.iter().zip(expected).enumerate() {
            assert_eq!(
                actual.as_ref().map(AsRef::<[u8]>::as_ref),
                Some(expected_value.as_ref()),
                "WAL payload benchmark value mismatch at index {index}",
            );
        }
    }

    fn waiting_confirm_count(&self) -> usize {
        self.rt
            .block_on({
                let logger = self.logger.clone();
                async move { logger.waiting_confirm_count().await }
            })
            .expect("WAL payload benchmark waiting count must be readable")
    }

    fn wait_for_confirmation(
        &self,
        commit_uid: Guid,
        confirm_before: usize,
        waiting_before: usize,
    ) {
        let deadline = Instant::now() + CONFIRM_TIMEOUT;
        let expected_confirmed = confirm_before
            .checked_add(1)
            .expect("WAL payload benchmark confirmation counter must not overflow");
        loop {
            let (checkpoint, waiting) = self
                .rt
                .block_on({
                    let logger = self.logger.clone();
                    let commit_uid = commit_uid.clone();
                    async move {
                        (
                            logger.check_point_of(commit_uid).await,
                            logger.waiting_confirm_count().await,
                        )
                    }
                })
                .expect("WAL payload benchmark confirmation state must be readable");
            if checkpoint.is_none()
                && self.logger.confirm_total_count() == expected_confirmed
                && waiting == waiting_before
            {
                return;
            }
            assert!(
                self.logger.confirm_total_count() <= expected_confirmed,
                "WAL payload benchmark observed unrelated confirmation progress",
            );
            assert!(
                Instant::now() < deadline,
                "WAL payload confirmation exceeded {CONFIRM_TIMEOUT:?}: checkpoint={checkpoint:?}, waiting={waiting}, confirm_before={confirm_before}, confirm_now={}",
                self.logger.confirm_total_count(),
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn writable_transaction(&self, source: &str) -> RealTransaction {
        self.db
            .transaction(Atom::from(source), true, 10_000, 10_000)
            .expect("WAL payload benchmark writable root must start")
    }

    fn run_operation<T, F>(&self, future: F) -> Result<T, KVTableTrError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, KVTableTrError>> + Send + 'static,
    {
        self.rt
            .block_on(async move { CapturedOperation(Some(future.await)) })
            .expect("WAL payload benchmark runtime must complete")
            .0
            .expect("WAL payload benchmark runtime must preserve operation output")
    }
}

async fn create_tables(db: &RealDb, tables: &[Atom]) {
    let transaction = db
        .transaction(
            Atom::from("WAL payload benchmark DDL"),
            true,
            10_000,
            10_000,
        )
        .expect("WAL payload benchmark DDL root must start");
    for table in tables {
        transaction
            .create_table(table.clone(), memory_meta(true), false)
            .await
            .expect("WAL payload benchmark table must be created");
    }
    let token = transaction
        .prepare_modified_conflicts()
        .await
        .expect("WAL payload benchmark DDL prepare must succeed");
    assert!(token.len() > 16, "WAL payload benchmark DDL must emit Meta WAL");
    transaction
        .commit_modified(token)
        .await
        .expect("WAL payload benchmark DDL commit must succeed");
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
            "pi_db_version_commit_wal_payload_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path)
            .expect("WAL payload benchmark temporary root must be created");
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
