//! 当前普通/版本事务在多表并发下的精确 commit 与根 WAL flush 分阶段基准。
//!
//! 每个样本先读目标 Memory 表中的稳定 Key 以及至多 10 张其它 Memory 表，再向目标表写入
//! 唯一 Key。64 个闭环任务在 4 worker runtime 中并发执行，模拟少量热点表上不同 Key 的
//! query、prepare 和 commit 交错。无 WAL 场景隔离表内并发控制；小 WAL 场景额外覆盖真实
//! CommitLogger 延迟批次与 `sync_data`。测试包装器只委托并记录 append/flush wall time，
//! 不改变日志实现。
//!
//! 查询、预提交和提交分别计时；输入构造、确认等待和最终校验不进入任一阶段。最终校验同时
//! 读取表内权威值和版本缓存中的精确 Version，不能用 API 成功返回代替。设置
//! `PI_DB_PUBLICATION_BENCH_ONLY=1` 时只运行非持久化、0 张额外读表、16/64 并发的 A/B 子集；
//! 默认仍运行完整矩阵。所有场景串行独占执行，避免资源争用污染证据。

use std::{
    fs,
    hint::black_box,
    io::Result as IOResult,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::BufMut;
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use futures::future::{BoxFuture, FutureExt};
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime, AsyncRuntimeExt,
};
use pi_async_transaction::{
    manager_2pc::{Transaction2PcManager, Transaction2PcStatus},
    AsyncCommitLog, Transaction2Pc, UnitTransaction,
};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder},
    tables::TableKV,
    Binary, KVDBTableType, KVTableMeta, TableKeyVersion, Version,
};
use pi_guid::{Guid, GuidGen};
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type RealDb = KVDBManager<usize, MeasuredCommitLogger>;
type RealManager = Transaction2PcManager<usize, MeasuredCommitLogger>;

const WORKERS: usize = 4;
const CONCURRENCY: usize = 64;
const CONCURRENCY_COUNTS: [usize; 4] = [1, 4, 16, CONCURRENCY];
const PUBLICATION_CONCURRENCY_COUNTS: [usize; 2] = [16, CONCURRENCY];
const READ_TABLES: usize = 10;
const READ_TABLE_COUNTS: [usize; 3] = [0, 1, READ_TABLES];
const WARMUP_PER_CLIENT: usize = 0;
const SAMPLES_PER_CLIENT: usize = 20;
const PHASE_TIMEOUT: Duration = Duration::from_secs(120);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);

fn main() {
    let _time_loop = startup_global_time_loop(10);
    let fixture = Fixture::new();
    let publication_only = std::env::var_os("PI_DB_PUBLICATION_BENCH_ONLY").is_some();

    println!(
        "version_commit_contention: workers={}, concurrency={}, extra_read_tables={}, warmup/client={}, samples/client={}, publication_only={}",
        WORKERS,
        CONCURRENCY,
        READ_TABLES,
        WARMUP_PER_CLIENT,
        SAMPLES_PER_CLIENT,
        publication_only,
    );
    println!(
        "{:<24} {:>7} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "case", "samples", "query_p50", "query_p99", "prepare_p50", "prepare_p99",
        "commit_p50", "commit_p99", "flush_p50", "flush_p99",
    );

    if publication_only {
        for concurrency in PUBLICATION_CONCURRENCY_COUNTS {
            for protocol in [Protocol::Ordinary, Protocol::Versioned] {
                fixture.run_phase(protocol, false, 0, concurrency);
            }
        }
    } else {
        for concurrency in CONCURRENCY_COUNTS {
            for read_count in READ_TABLE_COUNTS {
                for persistence in [false, true] {
                    for protocol in [Protocol::Ordinary, Protocol::Versioned] {
                        fixture.run_phase(protocol, persistence, read_count, concurrency);
                    }
                }
            }
        }
    }

    assert_eq!(fixture.manager.transaction_len(), 0);
    assert_eq!(
        fixture.manager.produced_transaction_total(),
        fixture.manager.consumed_transaction_total(),
        "all contention benchmark roots must be consumed",
    );
}

#[derive(Clone, Copy)]
enum Protocol {
    Ordinary,
    Versioned,
}

impl Protocol {
    const fn label(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::Versioned => "versioned",
        }
    }
}

#[derive(Clone)]
struct TableSet {
    reads: Vec<Atom>,
    write: Atom,
}

struct TaskResult {
    query_durations: Vec<Duration>,
    prepare_durations: Vec<Duration>,
    commit_durations: Vec<Duration>,
    expected: Vec<(Binary, Binary, Version)>,
}

struct Fixture {
    db: RealDb,
    manager: RealManager,
    logger: MeasuredCommitLogger,
    rt: MultiTaskRuntime<()>,
    volatile_ordinary: TableSet,
    volatile_versioned: TableSet,
    wal_ordinary: TableSet,
    wal_versioned: TableSet,
    next_phase: AtomicUsize,
    _root: TempRoot,
}

impl Fixture {
    fn new() -> Self {
        let volatile_ordinary = table_set("volatile_ord");
        let volatile_versioned = table_set("volatile_ver");
        let wal_ordinary = table_set("wal_ord");
        let wal_versioned = table_set("wal_ver");
        let all_tables = vec![
            (volatile_ordinary.clone(), false),
            (volatile_versioned.clone(), false),
            (wal_ordinary.clone(), true),
            (wal_versioned.clone(), true),
        ];
        let root = TempRoot::new();
        let root_path = root.path().to_path_buf();
        let rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(WORKERS)
            .build();
        let setup_rt = rt.clone();
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
            .expect("contention benchmark CommitLogger must start");
            let logger = MeasuredCommitLogger::new(logger);
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
            .expect("contention benchmark database must start");
            create_tables(&db, &all_tables).await;
            sender
                .send((db, manager, logger))
                .expect("contention benchmark fixture receiver must remain alive");
        })
        .expect("contention benchmark setup runtime must complete");

        let (db, manager, logger) = receiver
            .recv()
            .expect("contention benchmark fixture must be returned");
        let fixture = Self {
            db,
            manager,
            logger,
            rt,
            volatile_ordinary,
            volatile_versioned,
            wal_ordinary,
            wal_versioned,
            next_phase: AtomicUsize::new(1),
            _root: root,
        };
        fixture.prewarm_versions(&fixture.volatile_versioned);
        fixture.prewarm_versions(&fixture.wal_versioned);
        fixture.logger.drain_phase_samples();
        assert_eq!(fixture.manager.transaction_len(), 0);
        assert_eq!(fixture.logger.append_total_count(), 1);
        fixture
    }

    fn run_phase(&self,
                 protocol: Protocol,
                 persistence: bool,
                 read_count: usize,
                 concurrency: usize) {
        let tables = match (protocol, persistence) {
            (Protocol::Ordinary, false) => self.volatile_ordinary.clone(),
            (Protocol::Versioned, false) => self.volatile_versioned.clone(),
            (Protocol::Ordinary, true) => self.wal_ordinary.clone(),
            (Protocol::Versioned, true) => self.wal_versioned.clone(),
        };
        self.logger.drain_phase_samples();
        let produced_before = self.manager.produced_transaction_total();
        let consumed_before = self.manager.consumed_transaction_total();
        let append_before = self.logger.append_total_count();
        let confirm_before = self.logger.confirm_total_count();
        let waiting_before = self.waiting_confirm_count();
        let phase_id = self.next_phase.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = unbounded();

        for client in 0..concurrency {
            let db = self.db.clone();
            let tables = tables.clone();
            let sender = sender.clone();
            self.rt
                .spawn(async move {
                    let result = run_client(db,
                                            protocol,
                                            tables,
                                            read_count,
                                            phase_id,
                                            client).await;
                    let _ = sender.send(result);
                })
                .expect("contention benchmark task must be accepted");
        }
        drop(sender);

        let mut query_durations = Vec::with_capacity(concurrency * SAMPLES_PER_CLIENT);
        let mut prepare_durations = Vec::with_capacity(concurrency * SAMPLES_PER_CLIENT);
        let mut commit_durations = Vec::with_capacity(concurrency * SAMPLES_PER_CLIENT);
        let mut expected = Vec::with_capacity(
            concurrency * (WARMUP_PER_CLIENT + SAMPLES_PER_CLIENT));
        for _ in 0..concurrency {
            let result = receiver
                .recv_timeout(PHASE_TIMEOUT)
                .expect("contention benchmark client must finish before deadline")
                .unwrap_or_else(|error| panic!("contention benchmark client failed: {error}"));
            query_durations.extend(result.query_durations);
            prepare_durations.extend(result.prepare_durations);
            commit_durations.extend(result.commit_durations);
            expected.extend(result.expected);
        }

        let roots = concurrency * (WARMUP_PER_CLIENT + SAMPLES_PER_CLIENT);
        assert_eq!(self.manager.transaction_len(), 0);
        assert_eq!(self.manager.produced_transaction_total() - produced_before, roots);
        assert_eq!(self.manager.consumed_transaction_total() - consumed_before, roots);
        assert_eq!(
            self.logger.append_total_count() - append_before,
            if persistence { roots } else { 0 },
        );
        self.assert_values(&tables.write, &expected);
        if persistence {
            self.wait_for_confirmations(confirm_before, waiting_before, roots);
        } else {
            assert_eq!(self.logger.confirm_total_count(), confirm_before);
            assert_eq!(self.waiting_confirm_count(), waiting_before);
        }

        let phase_samples = self.logger.drain_phase_samples();
        let mut append_durations = Vec::new();
        let mut flush_durations = Vec::new();
        for sample in phase_samples {
            match sample.phase {
                LogPhase::Append => append_durations.push(sample.elapsed),
                LogPhase::Flush => flush_durations.push(sample.elapsed),
            }
        }
        let expected_log_samples = if persistence { roots } else { 0 };
        assert_eq!(append_durations.len(), expected_log_samples);
        assert_eq!(flush_durations.len(), expected_log_samples);
        let queries = SampleStats::from_samples(&mut query_durations);
        let prepares = SampleStats::from_samples(&mut prepare_durations);
        let commits = SampleStats::from_samples(&mut commit_durations);
        let flushes = SampleStats::from_optional_samples(&mut flush_durations);
        let case = format!(
            "{}_{}_r{}_c{}",
            if persistence { "wal" } else { "volatile" },
            protocol.label(),
            read_count,
            concurrency,
        );
        println!(
            "{:<24} {:>7} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
            case,
            commit_durations.len(),
            queries.p50,
            queries.p99,
            prepares.p50,
            prepares.p99,
            commits.p50,
            commits.p99,
            optional_nanos(flushes.as_ref().map(|stats| stats.p50)),
            optional_nanos(flushes.as_ref().map(|stats| stats.p99)),
        );
        black_box(append_durations);
        black_box(queries.max);
        black_box(prepares.max);
        black_box(commits.max);
        black_box(flushes.as_ref().map(|stats| stats.max));
    }

    fn prewarm_versions(&self, tables: &TableSet) {
        let db = self.db.clone();
        let reads = tables.reads.clone();
        let write = tables.write.clone();
        self.rt
            .block_on(async move {
                for table in reads {
                    for client in 0..CONCURRENCY {
                        let (value, _) = db
                            .query_with_version(table.clone(), read_key(client))
                            .await
                            .expect("version baseline prewarm must succeed");
                        assert!(value.is_none());
                    }
                }
                for client in 0..CONCURRENCY {
                    let (value, _) = db
                        .query_with_version(write.clone(), read_key(client))
                        .await
                        .expect("target-table version baseline prewarm must succeed");
                    assert!(value.is_none());
                }
            })
            .expect("version baseline prewarm runtime must complete");
    }

    fn assert_values(&self, table: &Atom, expected: &[(Binary, Binary, Version)]) {
        let queries = expected
            .iter()
            .map(|(key, _, _)| TableKV::new(table.clone(), key.clone(), None))
            .collect();
        let transaction = self
            .db
            .transaction(
                Atom::from("contention benchmark verification"),
                false,
                10_000,
                10_000,
            )
            .expect("contention benchmark verification root must start");
        let values = self
            .rt
            .block_on({
                let transaction = transaction.clone();
                async move { Some(transaction.query(queries).await) }
            })
            .expect("contention benchmark verification runtime must complete")
            .expect("contention benchmark verification values must be preserved");
        assert_eq!(values.len(), expected.len());
        for (index, (actual, (_, expected_value, _))) in values.iter().zip(expected).enumerate() {
            assert_eq!(
                actual.as_ref().map(AsRef::<[u8]>::as_ref),
                Some(expected_value.as_ref()),
                "contention benchmark value mismatch at index {index}",
            );
        }
        for (index, (key, expected_value, expected_version)) in expected.iter().enumerate() {
            let observed = self
                .rt
                .block_on({
                    let db = self.db.clone();
                    let table = table.clone();
                    let key = key.clone();
                    async move { Some(db.query_with_version(table, key).await) }
                })
                .expect("contention benchmark version verification runtime must complete")
                .expect("contention benchmark version verification result must be preserved")
                .expect("contention benchmark version verification must succeed");
            assert_eq!(
                observed.0.as_ref().map(AsRef::<[u8]>::as_ref),
                Some(expected_value.as_ref()),
                "contention benchmark qwv value mismatch at index {index}",
            );
            assert_eq!(
                &observed.1,
                expected_version,
                "contention benchmark cache Version mismatch at index {index}",
            );
        }
    }

    fn waiting_confirm_count(&self) -> usize {
        self.rt
            .block_on({
                let logger = self.logger.clone();
                async move { logger.waiting_confirm_count().await }
            })
            .expect("contention benchmark waiting count must be readable")
    }

    fn wait_for_confirmations(&self,
                              confirm_before: usize,
                              waiting_before: usize,
                              expected: usize) {
        let deadline = Instant::now() + CONFIRM_TIMEOUT;
        let expected_confirmed = confirm_before
            .checked_add(expected)
            .expect("contention benchmark confirmation counter must not overflow");
        loop {
            let confirmed = self.logger.confirm_total_count();
            let waiting = self.waiting_confirm_count();
            if confirmed == expected_confirmed && waiting == waiting_before {
                return;
            }
            assert!(confirmed <= expected_confirmed,
                    "contention benchmark observed unrelated confirmation progress");
            assert!(
                Instant::now() < deadline,
                "contention benchmark confirmation exceeded {CONFIRM_TIMEOUT:?}: expected={expected_confirmed}, actual={confirmed}, waiting_before={waiting_before}, waiting={waiting}",
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

async fn run_client(db: RealDb,
                    protocol: Protocol,
                    tables: TableSet,
                    read_count: usize,
                    phase_id: usize,
                    client: usize)
    -> Result<TaskResult, String> {
    let iterations = WARMUP_PER_CLIENT + SAMPLES_PER_CLIENT;
    let mut query_durations = Vec::with_capacity(SAMPLES_PER_CLIENT);
    let mut prepare_durations = Vec::with_capacity(SAMPLES_PER_CLIENT);
    let mut commit_durations = Vec::with_capacity(SAMPLES_PER_CLIENT);
    let mut expected = Vec::with_capacity(iterations);
    for iteration in 0..iterations {
        let key = write_key(phase_id, client, iteration);
        let value = encode_usize(key ^ 0x5c5c_5c5c);
        let transaction = db
            .transaction(
                Atom::from("contention benchmark sample"),
                true,
                10_000,
                10_000,
            )
            .ok_or_else(|| "start failed: transaction manager rejected root".to_string())?;

        let (query_elapsed, prepare_elapsed, commit_elapsed, expected_version) = match protocol {
            Protocol::Ordinary => {
                let mut queries = vec![TableKV::new(
                    tables.write.clone(),
                    read_key(client),
                    None,
                )];
                queries.extend(tables
                    .reads
                    .iter()
                    .take(read_count)
                    .map(|table| TableKV::new(table.clone(), read_key(client), None))
                    .collect::<Vec<_>>());
                let query_started = Instant::now();
                let values = transaction.query(queries).await;
                let query_elapsed = query_started.elapsed();
                if values != vec![None; read_count + 1] {
                    return Err(format!("ordinary read mismatch: {values:?}"));
                }
                transaction
                    .upsert(vec![TableKV::new(
                        tables.write.clone(),
                        encode_usize(key),
                        Some(value.clone()),
                    )])
                    .await
                    .map_err(|error| format!("ordinary upsert failed: {error:?}"))?;
                let prepare_started = Instant::now();
                let token = transaction
                    .prepare_modified_conflicts()
                    .await
                    .map_err(|error| format!("ordinary prepare failed: {error:?}"))?;
                let prepare_elapsed = prepare_started.elapsed();
                let transaction_uid = transaction
                    .get_transaction_uid()
                    .ok_or_else(|| "ordinary prepare did not allocate TID".to_string())?;
                let started = Instant::now();
                transaction
                    .commit_modified(token)
                    .await
                    .map_err(|error| format!("ordinary commit failed: {error:?}"))?;
                (query_elapsed,
                 prepare_elapsed,
                 started.elapsed(),
                 Version::Upsert(transaction_uid))
            },
            Protocol::Versioned => {
                let mut read_set = Vec::with_capacity(read_count + 1);
                let query_started = Instant::now();
                for table in std::iter::once(&tables.write)
                    .chain(tables.reads.iter().take(read_count)) {
                    let (value, version) = db
                        .query_with_version(table.clone(), read_key(client))
                        .await
                        .map_err(|error| format!("version query failed: {error:?}"))?;
                    if value.is_some() {
                        return Err("version read unexpectedly found a value".to_string());
                    }
                    read_set.push(TableKeyVersion {
                        table: table.clone(),
                        key: read_key(client),
                        version,
                    });
                }
                let query_elapsed = query_started.elapsed();
                let prepare_started = Instant::now();
                let token = transaction
                    .prepare_with_version(
                        read_set,
                        vec![TableKV::new(
                            tables.write.clone(),
                            encode_usize(key),
                            Some(value.clone()),
                        )],
                    )
                    .await
                    .map_err(|error| format!("version prepare failed: {error:?}"))?;
                let prepare_elapsed = prepare_started.elapsed();
                let transaction_uid = transaction
                    .get_transaction_uid()
                    .ok_or_else(|| "version prepare did not allocate TID".to_string())?;
                let started = Instant::now();
                let receipt = transaction
                    .commit_with_version(token)
                    .await
                    .map_err(|error| format!("version commit failed: {error:?}"))?;
                let commit_elapsed = started.elapsed();
                let expected_version = Version::Upsert(transaction_uid.clone());
                if receipt.len() != 1
                    || receipt[0].table != tables.write
                    || receipt[0].key != encode_usize(key)
                    || receipt[0].version != expected_version {
                    return Err(format!("version receipt mismatch: {receipt:?}"));
                }
                (query_elapsed, prepare_elapsed, commit_elapsed, expected_version)
            },
        };
        if transaction.get_status() != Transaction2PcStatus::Commited {
            return Err(format!("unexpected commit status: {:?}", transaction.get_status()));
        }
        if iteration >= WARMUP_PER_CLIENT {
            query_durations.push(query_elapsed);
            prepare_durations.push(prepare_elapsed);
            commit_durations.push(commit_elapsed);
        }
        expected.push((encode_usize(key), value, expected_version));
    }
    Ok(TaskResult {
        query_durations,
        prepare_durations,
        commit_durations,
        expected,
    })
}

fn table_set(prefix: &str) -> TableSet {
    TableSet {
        reads: (0..READ_TABLES)
            .map(|index| Atom::from(format!("bench_contend_{prefix}_read_{index}")))
            .collect(),
        write: Atom::from(format!("bench_contend_{prefix}_write")),
    }
}

async fn create_tables(db: &RealDb, groups: &[(TableSet, bool)]) {
    let transaction = db
        .transaction(
            Atom::from("contention benchmark DDL"),
            true,
            10_000,
            10_000,
        )
        .expect("contention benchmark DDL root must start");
    for (tables, write_persistence) in groups {
        for table in &tables.reads {
            transaction
                .create_table(table.clone(), memory_meta(false), false)
                .await
                .expect("contention benchmark read table must be created");
        }
        transaction
            .create_table(tables.write.clone(), memory_meta(*write_persistence), false)
            .await
            .expect("contention benchmark write table must be created");
    }
    let token = transaction
        .prepare_modified_conflicts()
        .await
        .expect("contention benchmark DDL prepare must succeed");
    assert!(token.len() > 16, "contention benchmark DDL must emit Meta WAL");
    transaction
        .commit_modified(token)
        .await
        .expect("contention benchmark DDL commit must succeed");
    assert_eq!(transaction.get_status(), Transaction2PcStatus::Commited);
    assert_eq!(db.table_size().await, groups.len() * (READ_TABLES + 1) + 1);
}

fn read_key(client: usize) -> Binary {
    encode_usize(0x7d00_0000 + client)
}

fn write_key(phase: usize, client: usize, iteration: usize) -> usize {
    0x7100_0000
        + phase * 1_000_000
        + client * (WARMUP_PER_CLIENT + SAMPLES_PER_CLIENT)
        + iteration
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
    max: u128,
}

impl SampleStats {
    fn from_samples(samples: &mut [Duration]) -> Self {
        assert!(!samples.is_empty());
        samples.sort_unstable();
        let nanos: Vec<u128> = samples.iter().map(Duration::as_nanos).collect();
        Self {
            p50: percentile(&nanos, 50),
            p99: percentile(&nanos, 99),
            max: *nanos.last().expect("sample set must not be empty"),
        }
    }

    fn from_optional_samples(samples: &mut [Duration]) -> Option<Self> {
        if samples.is_empty() {
            None
        } else {
            Some(Self::from_samples(samples))
        }
    }
}

fn percentile(sorted: &[u128], percentile: usize) -> u128 {
    let index = (sorted.len() - 1) * percentile / 100;
    sorted[index]
}

fn optional_nanos(value: Option<u128>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

#[derive(Clone)]
struct MeasuredCommitLogger {
    inner: CommitLogger,
    samples_tx: Sender<LogPhaseSample>,
    samples_rx: Receiver<LogPhaseSample>,
}

impl MeasuredCommitLogger {
    fn new(inner: CommitLogger) -> Self {
        let (samples_tx, samples_rx) = unbounded();
        Self {
            inner,
            samples_tx,
            samples_rx,
        }
    }

    fn drain_phase_samples(&self) -> Vec<LogPhaseSample> {
        self.samples_rx.try_iter().collect()
    }
}

struct LogPhaseSample {
    phase: LogPhase,
    elapsed: Duration,
}

enum LogPhase {
    Append,
    Flush,
}

impl AsyncCommitLog for MeasuredCommitLogger {
    type C = usize;
    type Cid = Guid;

    fn append<B>(&self, commit_uid: Guid, log: B) -> BoxFuture<'static, IOResult<usize>>
    where
        B: BufMut + AsRef<[u8]> + Send + Sized + 'static,
    {
        let inner = self.inner.clone();
        let samples = self.samples_tx.clone();
        async move {
            let started = Instant::now();
            let result = inner.append(commit_uid, log).await;
            let _ = samples.send(LogPhaseSample {
                phase: LogPhase::Append,
                elapsed: started.elapsed(),
            });
            result
        }.boxed()
    }

    fn flush(&self, log_handle: usize) -> BoxFuture<'static, IOResult<()>> {
        let inner = self.inner.clone();
        let samples = self.samples_tx.clone();
        async move {
            let started = Instant::now();
            let result = inner.flush(log_handle).await;
            let _ = samples.send(LogPhaseSample {
                phase: LogPhase::Flush,
                elapsed: started.elapsed(),
            });
            result
        }.boxed()
    }

    fn confirm(&self, commit_uid: Guid) -> BoxFuture<'static, IOResult<()>> {
        self.inner.confirm(commit_uid)
    }

    fn start_replay<B, F>(&self, callback: Arc<F>) -> BoxFuture<'static, IOResult<(usize, usize)>>
    where
        B: BufMut + AsRef<[u8]> + From<Vec<u8>> + Send + Sized + 'static,
        F: Fn(Guid, B) -> IOResult<()> + Send + Sync + 'static,
    {
        self.inner.start_replay(callback)
    }

    fn append_replay<B>(&self,
                        commit_uid: Guid,
                        log: B) -> BoxFuture<'static, IOResult<usize>>
    where
        B: BufMut + AsRef<[u8]> + Send + Sized + 'static,
    {
        self.inner.append_replay(commit_uid, log)
    }

    fn flush_replay(&self, log_handle: usize) -> BoxFuture<'static, IOResult<()>> {
        self.inner.flush_replay(log_handle)
    }

    fn confirm_replay(&self, commit_uid: Guid) -> BoxFuture<'static, IOResult<()>> {
        self.inner.confirm_replay(commit_uid)
    }

    fn finish_replay(&self) -> BoxFuture<'static, IOResult<()>> {
        self.inner.finish_replay()
    }

    fn check_point_of(&self, commit_uid: Guid) -> BoxFuture<'static, Option<usize>> {
        self.inner.check_point_of(commit_uid)
    }

    fn current_check_point(&self) -> BoxFuture<'static, usize> {
        self.inner.current_check_point()
    }

    fn append_check_point(&self) -> BoxFuture<'static, IOResult<usize>> {
        self.inner.append_check_point()
    }

    fn waiting_confirm_count(&self) -> BoxFuture<'static, usize> {
        self.inner.waiting_confirm_count()
    }

    fn append_total_count(&self) -> usize {
        self.inner.append_total_count()
    }

    fn confirm_total_count(&self) -> usize {
        self.inner.confirm_total_count()
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
            "pi_db_version_commit_contention_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path)
            .expect("contention benchmark temporary root must be created");
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
