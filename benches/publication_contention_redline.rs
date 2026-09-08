//! 表级 publication 写门队头阻塞的真实事务专项基准。
//!
//! 每个被测根事务都在计时前完成 prepare，随后经同一个异步关闭门同时进入 commit。业务表
//! 使用非持久化 Memory，以排除根 WAL、表日志和数据文件 I/O；DDL 仍使用真实 CommitLogger
//! 并在测量前等待确认收口。Shared 场景让所有事务写同一张表的不同 Key，Sharded 场景让每个
//! 事务写独立表，二者事务数、每事务 Key 数、协议和断言完全相同。
//!
//! 本 target 同时保留修复前缺陷的定性复现维度和修复后的 A/B 验收维度，不定义跨机器绝对
//! SLA。Shared/Sharded 仍会包含同表数据根串行化的必要成本，特别是 64 Key 场景；因此修复后
//! 不能把“全部 Shared 放大消失”作为门禁，必须用同一二进制夹具交错比较 1 Key 提交队列，
//! 并结合 64 Key、只读和观察者对照区分 publication 门与表数据根的成本。

use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::{bounded, unbounded};
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
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    Binary, KVDBTableType, KVTableMeta, TableKeyVersion, Version,
};
use pi_guid::{Guid, GuidGen};
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type RealDb = KVDBManager<usize, CommitLogger>;
type RealManager = Transaction2PcManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const WORKERS: usize = 4;
const CONCURRENCIES: [usize; 4] = [1, 16, 64, 256];
// 0 表示仅在目标表登记一个 Read；1/64 表示最终写 Key 数。
const WRITE_COUNTS: [usize; 3] = [0, 1, 64];
const ROUNDS: usize = 6;
const INTERFERENCE_CONCURRENCIES: [usize; 2] = [64, 256];
const INTERFERENCE_OBSERVERS: usize = 32;
const INTERFERENCE_ROUNDS: usize = 6;
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const RESULT_TIMEOUT: Duration = Duration::from_secs(30);
const DDL_REGISTRATION_TIMEOUT: Duration = Duration::from_secs(5);

fn main() {
    let _time_loop = startup_global_time_loop(10);
    let profiles = build_profiles();
    let fixture = Fixture::new(&profiles);
    let mut severe_profiles = 0;
    let observer_only = std::env::var_os("PI_DB_PUBLICATION_OBSERVER_ONLY").is_some();

    println!(
        "publication_contention_redline: workers={}, rounds={}, all transactions prepared before common commit gate",
        WORKERS,
        ROUNDS,
    );
    println!(
        "{:<10} {:>6} {:>5} {:<8} {:>7} {:>12} {:>12} {:>12} {:>12}",
        "protocol", "writes", "txs", "topology", "samples", "commit_p50", "commit_p99",
        "release_p99", "batch_p99",
    );

    if !observer_only {
        for protocol in [Protocol::Ordinary, Protocol::Versioned] {
            for write_count in WRITE_COUNTS {
                for concurrency in CONCURRENCIES {
                    let shared = profiles
                        .iter()
                        .find(|profile| {
                            profile.protocol == protocol
                                && profile.write_count == write_count
                                && profile.concurrency == concurrency
                                && profile.topology == Topology::Shared
                        })
                        .expect("shared redline profile must exist");
                    let sharded = profiles
                        .iter()
                        .find(|profile| {
                            profile.protocol == protocol
                                && profile.write_count == write_count
                                && profile.concurrency == concurrency
                                && profile.topology == Topology::Sharded
                        })
                        .expect("sharded redline profile must exist");

                    let sharded_stats = fixture.run_profile(sharded);
                    let shared_stats = fixture.run_profile(shared);
                    print_stats(sharded, &sharded_stats);
                    print_stats(shared, &shared_stats);

                    if concurrency >= 64
                        && shared_stats.release.p99
                            >= sharded_stats.release.p99.saturating_mul(4)
                        && shared_stats.release.p99 >= 1_000_000
                    {
                        severe_profiles += 1;
                    }
                }
            }
        }
    }

    fixture.run_observer_interference();
    assert_eq!(fixture.manager.transaction_len(), 0);
    assert_eq!(
        fixture.manager.produced_transaction_total(),
        fixture.manager.consumed_transaction_total(),
        "all redline transaction roots must be consumed",
    );
    assert_eq!(fixture.logger.append_total_count(), 1);
    assert_eq!(fixture.logger.confirm_total_count(), 0);
    assert_eq!(fixture.waiting_confirm_count(), 1);
    if !observer_only {
        println!("observed_severe_shared_profiles={severe_profiles}");
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Topology {
    Shared,
    Sharded,
}

#[derive(Clone, Copy, Debug)]
enum ObserverKind {
    Query,
    Prepare,
}

impl ObserverKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Prepare => "prepare",
        }
    }
}

impl Topology {
    const fn label(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Sharded => "sharded",
        }
    }
}

struct Profile {
    id: usize,
    protocol: Protocol,
    topology: Topology,
    write_count: usize,
    concurrency: usize,
    tables: Vec<Atom>,
}

fn build_profiles() -> Vec<Profile> {
    let mut profiles = Vec::new();
    let shared_table = Atom::from("publication_redline_shared");
    let sharded_tables: Vec<Atom> = (0..*CONCURRENCIES.last().unwrap())
        .map(|table| Atom::from(format!("publication_redline_shard_{table}")))
        .collect();
    for protocol in [Protocol::Ordinary, Protocol::Versioned] {
        for write_count in WRITE_COUNTS {
            for concurrency in CONCURRENCIES {
                for topology in [Topology::Shared, Topology::Sharded] {
                    let id = profiles.len();
                    let tables = if topology == Topology::Shared {
                        vec![shared_table.clone()]
                    } else {
                        sharded_tables[..concurrency].to_vec()
                    };
                    profiles.push(Profile {
                        id,
                        protocol,
                        topology,
                        write_count,
                        concurrency,
                        tables,
                    });
                }
            }
        }
    }
    profiles
}

struct PreparedCommit {
    transaction: RealTransaction,
    token: Vec<u8>,
    transaction_uid: Guid,
    expected_values: Vec<TableKV>,
    expected_receipt: HashSet<TableKeyVersion>,
}

struct CommitSample {
    commit: Duration,
    from_release: Duration,
}

enum ObserverCompletion {
    Query {
        call: Duration,
        from_release: Duration,
        value: Option<Binary>,
        version: Version,
    },
    Prepared {
        call: Duration,
        from_release: Duration,
        transaction: RealTransaction,
        token: Vec<u8>,
    },
}

struct ProfileStats {
    commit: SampleStats,
    release: SampleStats,
    batch: SampleStats,
}

struct Fixture {
    db: RealDb,
    manager: RealManager,
    logger: CommitLogger,
    rt: MultiTaskRuntime<()>,
    observer_rt: MultiTaskRuntime<()>,
    _root: TempRoot,
}

impl Fixture {
    fn new(profiles: &[Profile]) -> Self {
        let root = TempRoot::new();
        let root_path = root.path().to_path_buf();
        let rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(WORKERS)
            .build();
        let setup_rt = rt.clone();
        let mut unique_tables = HashSet::new();
        let tables: Vec<Atom> = profiles
            .iter()
            .flat_map(|profile| profile.tables.iter())
            .filter_map(|table| {
                if unique_tables.insert(table.clone()) {
                    Some(table.clone())
                } else {
                    None
                }
            })
            .collect();
        let expected_table_count = tables.len() + 1;
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
            .expect("redline CommitLogger must start");
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
            .expect("redline database must start");
            create_tables(&db, tables).await;
            sender
                .send((db, manager, logger))
                .expect("redline fixture receiver must remain alive");
        })
        .expect("redline setup runtime must complete");

        let (db, manager, logger) = receiver.recv().expect("redline fixture must be returned");
        let fixture = Self {
            db,
            manager,
            logger,
            rt,
            observer_rt: MultiTaskRuntimeBuilder::default()
                .init_worker_size(WORKERS)
                .build(),
            _root: root,
        };
        fixture.wait_for_ddl_registration();
        let table_size = fixture
            .rt
            .block_on({
                let db = fixture.db.clone();
                async move { Some(db.table_size().await) }
            })
            .expect("redline table count runtime must complete")
            .expect("redline table count must be preserved");
        assert_eq!(table_size, expected_table_count);
        assert_eq!(fixture.manager.transaction_len(), 0);
        fixture
    }

    fn run_profile(&self, profile: &Profile) -> ProfileStats {
        let produced_before = self.manager.produced_transaction_total();
        let consumed_before = self.manager.consumed_transaction_total();
        let append_before = self.logger.append_total_count();
        let confirm_before = self.logger.confirm_total_count();
        let mut commit_samples = Vec::with_capacity(profile.concurrency * ROUNDS);
        let mut release_samples = Vec::with_capacity(profile.concurrency * ROUNDS);
        let mut batch_samples = Vec::with_capacity(ROUNDS);

        for round in 0..ROUNDS {
            let prepared = self.prepare_batch(profile, round);
            let expected_values: Vec<TableKV> = prepared
                .iter()
                .flat_map(|prepared| prepared.expected_values.iter().cloned())
                .collect();
            let expected_versions: Vec<TableKeyVersion> = prepared
                .iter()
                .flat_map(|prepared| prepared.expected_receipt.iter().cloned())
                .collect();
            let (ready_tx, ready_rx) = unbounded();
            let (result_tx, result_rx) = unbounded();
            let (start_tx, start_rx) = async_channel::bounded::<()>(1);
            let common_start = Arc::new(OnceLock::<Instant>::new());

            for prepared in prepared {
                let ready_tx = ready_tx.clone();
                let result_tx = result_tx.clone();
                let start_rx = start_rx.clone();
                let common_start = common_start.clone();
                let protocol = profile.protocol;
                self.rt
                    .spawn(async move {
                        let _ = ready_tx.send(());
                        if start_rx.recv().await.is_ok() {
                            let _ = result_tx.send(Err(
                                "redline start gate unexpectedly delivered a value".to_string(),
                            ));
                            return;
                        }
                        let local_start = Instant::now();
                        let result = commit_prepared(protocol, prepared).await;
                        let commit = local_start.elapsed();
                        let from_release = common_start
                            .get()
                            .expect("redline common start must be initialized")
                            .elapsed();
                        let _ = result_tx.send(result.map(|_| CommitSample {
                            commit,
                            from_release,
                        }));
                    })
                    .expect("redline commit task must be accepted");
            }
            drop(ready_tx);
            drop(result_tx);

            for _ in 0..profile.concurrency {
                ready_rx
                    .recv_timeout(READY_TIMEOUT)
                    .expect("all redline tasks must reach the commit gate");
            }
            let released = Instant::now();
            common_start
                .set(released)
                .expect("redline common start must only be set once");
            drop(start_tx);

            for _ in 0..profile.concurrency {
                let sample = result_rx
                    .recv_timeout(RESULT_TIMEOUT)
                    .expect("redline commit result must arrive before deadline")
                    .unwrap_or_else(|error| panic!("redline commit failed: {error}"));
                commit_samples.push(sample.commit);
                release_samples.push(sample.from_release);
            }
            batch_samples.push(released.elapsed());
            self.assert_values_and_versions(expected_values, expected_versions);
        }

        let roots = profile.concurrency * ROUNDS;
        assert_eq!(self.manager.transaction_len(), 0);
        assert_eq!(self.manager.produced_transaction_total() - produced_before, roots);
        assert_eq!(self.manager.consumed_transaction_total() - consumed_before, roots);
        assert_eq!(self.logger.append_total_count(), append_before);
        assert_eq!(self.logger.confirm_total_count(), confirm_before);

        ProfileStats {
            commit: SampleStats::from_samples(&mut commit_samples),
            release: SampleStats::from_samples(&mut release_samples),
            batch: SampleStats::from_samples(&mut batch_samples),
        }
    }

    fn prepare_batch(&self, profile: &Profile, round: usize) -> Vec<PreparedCommit> {
        let db = self.db.clone();
        let protocol = profile.protocol;
        let topology = profile.topology;
        let profile_id = profile.id;
        let concurrency = profile.concurrency;
        let write_count = profile.write_count;
        let tables = profile.tables.clone();
        self.rt
            .block_on(async move {
                let mut prepared = Vec::with_capacity(concurrency);
                for client in 0..concurrency {
                    let table = if topology == Topology::Shared {
                        tables[0].clone()
                    } else {
                        tables[client].clone()
                    };
                    let transaction = db
                        .transaction(
                            Atom::from("publication contention redline"),
                            true,
                            1_000_000,
                            1_000_000,
                        )
                        .expect("redline root transaction must start");
                    let mut input = Vec::with_capacity(write_count);
                    let mut expected_values = Vec::with_capacity(write_count);
                    for item in 0..write_count {
                        let key = encode_usize(
                            0x5200_0000
                                + profile_id * 10_000_000
                                + round * 100_000
                                + client * write_count
                                + item,
                        );
                        let value = encode_usize(
                            0x2500_0000
                                + profile_id * 10_000_000
                                + round * 100_000
                                + client * write_count
                                + item,
                        );
                        input.push(TableKV::new(table.clone(), key.clone(), Some(value.clone())));
                        expected_values.push(TableKV::new(table.clone(), key, Some(value)));
                    }

                    let token = if write_count == 0 {
                        let key = encode_usize(
                            0x1200_0000
                                + profile_id * 10_000_000
                                + round * 100_000
                                + client,
                        );
                        match protocol {
                            Protocol::Ordinary => {
                                let values = transaction
                                    .query(vec![TableKV::new(table.clone(), key, None)])
                                    .await;
                                assert_eq!(values, vec![None]);
                                transaction
                                    .prepare_modified_conflicts()
                                    .await
                                    .expect("ordinary read-only redline prepare must succeed")
                            },
                            Protocol::Versioned => {
                                let (value, version) = db
                                    .query_with_version(table.clone(), key.clone())
                                    .await
                                    .expect("versioned redline baseline query must succeed");
                                assert!(value.is_none());
                                transaction
                                    .prepare_with_version(
                                        vec![TableKeyVersion {
                                            table,
                                            key,
                                            version,
                                        }],
                                        Vec::new(),
                                    )
                                    .await
                                    .expect("versioned read-only redline prepare must succeed")
                            },
                        }
                    } else {
                        match protocol {
                            Protocol::Ordinary => {
                                transaction
                                    .upsert(input)
                                    .await
                                    .expect("ordinary redline upsert must succeed");
                                transaction
                                    .prepare_modified_conflicts()
                                    .await
                                    .expect("ordinary redline prepare must succeed")
                            },
                            Protocol::Versioned => {
                                transaction
                                    .prepare_with_version(Vec::new(), input)
                                    .await
                                    .expect("versioned redline prepare must succeed")
                            },
                        }
                    };
                    assert!(token.is_empty(), "volatile Memory must not emit root WAL");
                    let transaction_uid = transaction
                        .get_transaction_uid()
                        .expect("prepared redline transaction must have a TID");
                    let expected_receipt = expected_values
                        .iter()
                        .map(|value| TableKeyVersion {
                            table: value.table.clone(),
                            key: value.key.clone(),
                            version: Version::Upsert(transaction_uid.clone()),
                        })
                        .collect();
                    prepared.push(PreparedCommit {
                        transaction,
                        token,
                        transaction_uid,
                        expected_values,
                        expected_receipt,
                    });
                }
                Some(prepared)
            })
            .expect("redline prepare runtime must complete")
            .expect("redline prepared batch must be preserved")
    }

    fn assert_values_and_versions(&self,
                                  expected: Vec<TableKV>,
                                  expected_versions: Vec<TableKeyVersion>) {
        if expected.is_empty() {
            assert!(expected_versions.is_empty());
            return;
        }
        assert_eq!(expected.len(), expected_versions.len());
        let expected_by_key: HashMap<(Atom, Binary), Binary> = expected
            .iter()
            .map(|item| {
                ((item.table.clone(), item.key.clone()),
                    item.value
                        .clone()
                        .expect("redline write expectation must contain a value"),
                )
            })
            .collect();
        let transaction = self
            .db
            .transaction(
                Atom::from("publication redline verification"),
                false,
                1_000_000,
                1_000_000,
            )
            .expect("redline verification transaction must start");
        let queries = expected
            .iter()
            .map(|value| TableKV::new(value.table.clone(), value.key.clone(), None))
            .collect();
        let values = self
            .rt
            .block_on({
                let transaction = transaction.clone();
                async move { Some(transaction.query(queries).await) }
            })
            .expect("redline verification runtime must complete")
            .expect("redline verification values must be preserved");
        assert_eq!(values.len(), expected.len());
        for (index, (actual, expected)) in values.iter().zip(&expected).enumerate() {
            assert_eq!(
                actual.as_ref().map(AsRef::<[u8]>::as_ref),
                expected.value.as_ref().map(AsRef::<[u8]>::as_ref),
                "redline final value mismatch at index {index}",
            );
        }
        for (index, expected_version) in expected_versions.iter().enumerate() {
            let observed = self
                .rt
                .block_on({
                    let db = self.db.clone();
                    let table = expected_version.table.clone();
                    let key = expected_version.key.clone();
                    async move { Some(db.query_with_version(table, key).await) }
                })
                .expect("redline version verification runtime must complete")
                .expect("redline version verification result must be preserved")
                .expect("redline version verification must succeed");
            let expected_value = expected_by_key
                .get(&(expected_version.table.clone(), expected_version.key.clone()))
                .expect("redline expected version must have one expected value");
            assert_eq!(
                observed.0.as_ref().map(AsRef::<[u8]>::as_ref),
                Some(expected_value.as_ref()),
                "redline qwv value mismatch at index {index}",
            );
            assert_eq!(
                &observed.1,
                &expected_version.version,
                "redline cache Version mismatch at index {index}",
            );
        }
    }

    fn run_observer_interference(&self) {
        println!(
            "publication observer interference: observers={}, rounds={}, writers are versioned one-Key commits",
            INTERFERENCE_OBSERVERS,
            INTERFERENCE_ROUNDS,
        );
        println!(
            "{:<8} {:>7} {:>7} {:>12} {:>12} {:>12} {:>12} {:>12}",
            "observer", "writers", "samples", "call_p50", "call_p99", "release_p99",
            "batch_p99", "min_pending",
        );
        for kind in [ObserverKind::Query, ObserverKind::Prepare] {
            for writers in INTERFERENCE_CONCURRENCIES {
                self.run_observer_profile(kind, writers);
            }
        }
    }

    fn run_observer_profile(&self, kind: ObserverKind, writers: usize) {
        let table = Atom::from("publication_redline_shared");
        let probe_key = encode_usize(
            0x6f00_0000
                + writers * 100
                + match kind {
                    ObserverKind::Query => 1,
                    ObserverKind::Prepare => 2,
                },
        );
        let baseline = self
            .rt
            .block_on({
                let db = self.db.clone();
                let table = table.clone();
                let key = probe_key.clone();
                async move { Some(db.query_with_version(table, key).await) }
            })
            .expect("observer baseline runtime must complete")
            .expect("observer baseline result must be preserved")
            .expect("observer baseline query must succeed");
        assert!(baseline.0.is_none());
        assert!(matches!(&baseline.1, Version::Delete(_)));
        let produced_before = self.manager.produced_transaction_total();
        let consumed_before = self.manager.consumed_transaction_total();
        let append_before = self.logger.append_total_count();
        let confirm_before = self.logger.confirm_total_count();
        let mut calls = Vec::with_capacity(INTERFERENCE_OBSERVERS * INTERFERENCE_ROUNDS);
        let mut releases = Vec::with_capacity(INTERFERENCE_OBSERVERS * INTERFERENCE_ROUNDS);
        let mut batches = Vec::with_capacity(INTERFERENCE_ROUNDS);
        let mut minimum_pending = writers;

        for round in 0..INTERFERENCE_ROUNDS {
            let profile = Profile {
                id: 100 + round + match kind {
                    ObserverKind::Query => 0,
                    ObserverKind::Prepare => INTERFERENCE_ROUNDS,
                },
                protocol: Protocol::Versioned,
                topology: Topology::Shared,
                write_count: 1,
                concurrency: writers,
                tables: vec![table.clone()],
            };
            let prepared = self.prepare_batch(&profile, round);
            let expected_values: Vec<TableKV> = prepared
                .iter()
                .flat_map(|prepared| prepared.expected_values.iter().cloned())
                .collect();
            let expected_versions: Vec<TableKeyVersion> = prepared
                .iter()
                .flat_map(|prepared| prepared.expected_receipt.iter().cloned())
                .collect();
            let (writer_ready_tx, writer_ready_rx) = unbounded();
            let (writer_result_tx, writer_result_rx) = unbounded();
            let (writer_start_tx, writer_start_rx) = async_channel::bounded::<()>(1);
            for prepared in prepared {
                let ready = writer_ready_tx.clone();
                let result = writer_result_tx.clone();
                let start = writer_start_rx.clone();
                self.rt
                    .spawn(async move {
                        let _ = ready.send(());
                        if start.recv().await.is_ok() {
                            let _ = result.send(Err(
                                "observer writer gate unexpectedly delivered a value".to_string(),
                            ));
                            return;
                        }
                        let _ = result.send(commit_prepared(Protocol::Versioned, prepared).await);
                    })
                    .expect("observer writer task must be accepted");
            }
            drop(writer_ready_tx);
            drop(writer_result_tx);

            let (observer_ready_tx, observer_ready_rx) = unbounded();
            let (observer_result_tx, observer_result_rx) = unbounded();
            let (observer_start_tx, observer_start_rx) = async_channel::bounded::<()>(1);
            let observer_release = Arc::new(OnceLock::<Instant>::new());
            for _observer in 0..INTERFERENCE_OBSERVERS {
                let transaction = match kind {
                    ObserverKind::Query => None,
                    ObserverKind::Prepare => Some(self.db
                        .transaction(
                            Atom::from("publication prepare observer"),
                            true,
                            1_000_000,
                            1_000_000,
                        )
                        .expect("prepare observer root must start")),
                };
                let db = self.db.clone();
                let table = table.clone();
                let key = probe_key.clone();
                let version = baseline.1.clone();
                let ready = observer_ready_tx.clone();
                let result = observer_result_tx.clone();
                let start = observer_start_rx.clone();
                let release = observer_release.clone();
                self.observer_rt
                    .spawn(async move {
                        let _ = ready.send(());
                        if start.recv().await.is_ok() {
                            let _ = result.send(Err(
                                "observer gate unexpectedly delivered a value".to_string(),
                            ));
                            return;
                        }
                        let started = Instant::now();
                        let completion = match kind {
                            ObserverKind::Query => db
                                .query_with_version(table, key)
                                .await
                                .map(|(value, version)| ObserverCompletion::Query {
                                    call: started.elapsed(),
                                    from_release: release
                                        .get()
                                        .expect("observer release must be initialized")
                                        .elapsed(),
                                    value,
                                    version,
                                })
                                .map_err(|error| format!("observer query failed: {error:?}")),
                            ObserverKind::Prepare => {
                                let transaction = transaction
                                    .expect("prepare observer must own a transaction");
                                match transaction
                                    .prepare_with_version(
                                        vec![TableKeyVersion { table, key, version }],
                                        Vec::new(),
                                    )
                                    .await {
                                    Ok(token) => Ok(ObserverCompletion::Prepared {
                                        call: started.elapsed(),
                                        from_release: release
                                            .get()
                                            .expect("observer release must be initialized")
                                            .elapsed(),
                                        transaction,
                                        token,
                                    }),
                                    Err(error) => Err(format!(
                                        "observer prepare failed: {error:?}",
                                    )),
                                }
                            },
                        };
                        let _ = result.send(completion);
                    })
                    .expect("observer task must be accepted");
            }
            drop(observer_ready_tx);
            drop(observer_result_tx);

            for _ in 0..writers {
                writer_ready_rx
                    .recv_timeout(READY_TIMEOUT)
                    .expect("all interference writers must reach their gate");
            }
            for _ in 0..INTERFERENCE_OBSERVERS {
                observer_ready_rx
                    .recv_timeout(READY_TIMEOUT)
                    .expect("all interference observers must reach their gate");
            }
            let batch_started = Instant::now();
            drop(writer_start_tx);
            writer_result_rx
                .recv_timeout(RESULT_TIMEOUT)
                .expect("first interference writer must finish")
                .unwrap_or_else(|error| panic!("first interference writer failed: {error}"));
            let pending = writers
                .saturating_sub(1 + writer_result_rx.len());
            minimum_pending = minimum_pending.min(pending);
            assert!(
                pending > 0,
                "observer was released after the entire writer batch completed",
            );
            observer_release
                .set(Instant::now())
                .expect("observer release must only be set once");
            drop(observer_start_tx);

            let mut prepared_observers = Vec::new();
            for _ in 0..INTERFERENCE_OBSERVERS {
                match observer_result_rx
                    .recv_timeout(RESULT_TIMEOUT)
                    .expect("observer result must arrive before deadline")
                    .unwrap_or_else(|error| panic!("publication observer failed: {error}")) {
                    ObserverCompletion::Query {
                        call,
                        from_release,
                        value,
                        version,
                    } => {
                        assert!(matches!(kind, ObserverKind::Query));
                        assert_eq!(value, baseline.0);
                        assert_eq!(version, baseline.1);
                        calls.push(call);
                        releases.push(from_release);
                    },
                    ObserverCompletion::Prepared {
                        call,
                        from_release,
                        transaction,
                        token,
                    } => {
                        assert!(matches!(kind, ObserverKind::Prepare));
                        calls.push(call);
                        releases.push(from_release);
                        prepared_observers.push((transaction, token));
                    },
                }
            }
            for _ in 1..writers {
                writer_result_rx
                    .recv_timeout(RESULT_TIMEOUT)
                    .expect("interference writer result must arrive before deadline")
                    .unwrap_or_else(|error| panic!("interference writer failed: {error}"));
            }
            batches.push(batch_started.elapsed());
            for (transaction, token) in prepared_observers {
                self.rt
                    .block_on(async move {
                        let receipt = transaction
                            .commit_with_version(token)
                            .await
                            .expect("prepared observer commit must succeed");
                        assert!(receipt.is_empty());
                        assert_eq!(transaction.get_status(), Transaction2PcStatus::Commited);
                    })
                    .expect("prepared observer commit runtime must complete");
            }
            self.assert_values_and_versions(expected_values, expected_versions);
        }

        let observer_roots = if matches!(kind, ObserverKind::Prepare) {
            INTERFERENCE_OBSERVERS * INTERFERENCE_ROUNDS
        } else {
            0
        };
        let expected_roots = writers * INTERFERENCE_ROUNDS + observer_roots;
        assert_eq!(self.manager.transaction_len(), 0);
        assert_eq!(self.manager.produced_transaction_total() - produced_before,
                   expected_roots);
        assert_eq!(self.manager.consumed_transaction_total() - consumed_before,
                   expected_roots);
        assert_eq!(self.logger.append_total_count(), append_before);
        assert_eq!(self.logger.confirm_total_count(), confirm_before);
        let call_stats = SampleStats::from_samples(&mut calls);
        let release_stats = SampleStats::from_samples(&mut releases);
        let batch_stats = SampleStats::from_samples(&mut batches);
        println!(
            "{:<8} {:>7} {:>7} {:>12} {:>12} {:>12} {:>12} {:>12}",
            kind.label(),
            writers,
            INTERFERENCE_OBSERVERS * INTERFERENCE_ROUNDS,
            call_stats.p50,
            call_stats.p99,
            release_stats.p99,
            batch_stats.p99,
            minimum_pending,
        );
    }

    fn wait_for_ddl_registration(&self) {
        let deadline = Instant::now() + DDL_REGISTRATION_TIMEOUT;
        loop {
            let waiting = self.waiting_confirm_count();
            if waiting == 1 && self.logger.confirm_total_count() == 0 {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "redline DDL registration exceeded {DDL_REGISTRATION_TIMEOUT:?}: waiting={waiting}, confirmed={}",
                self.logger.confirm_total_count(),
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn waiting_confirm_count(&self) -> usize {
        self.rt
            .block_on({
                let logger = self.logger.clone();
                async move { Some(logger.waiting_confirm_count().await) }
            })
            .expect("redline confirmation query runtime must complete")
            .expect("redline confirmation count must be preserved")
    }
}

async fn commit_prepared(protocol: Protocol,
                         prepared: PreparedCommit) -> Result<(), String> {
    let PreparedCommit {
        transaction,
        token,
        transaction_uid,
        expected_values: _,
        expected_receipt,
    } = prepared;
    match protocol {
        Protocol::Ordinary => {
            transaction
                .commit_modified(token)
                .await
                .map_err(|error| format!("ordinary commit failed: {error:?}"))?;
        },
        Protocol::Versioned => {
            let receipt: HashSet<TableKeyVersion> = transaction
                .commit_with_version(token)
                .await
                .map_err(|error| format!("versioned commit failed: {error:?}"))?
                .into_iter()
                .collect();
            if receipt != expected_receipt {
                return Err(format!(
                    "version receipt mismatch for transaction {transaction_uid:?}: expected={expected_receipt:?}, actual={receipt:?}",
                ));
            }
        },
    }
    if transaction.get_status() != Transaction2PcStatus::Commited {
        return Err(format!(
            "unexpected redline transaction status: {:?}",
            transaction.get_status(),
        ));
    }
    Ok(())
}

async fn create_tables(db: &RealDb, tables: Vec<Atom>) {
    let transaction = db
        .transaction(
            Atom::from("publication redline DDL"),
            true,
            1_000_000,
            1_000_000,
        )
        .expect("redline DDL root must start");
    for table in tables {
        transaction
            .create_table(table, memory_meta(), false)
            .await
            .expect("redline Memory table must be created");
    }
    let token = transaction
        .prepare_modified_conflicts()
        .await
        .expect("redline DDL prepare must succeed");
    assert!(token.len() > 16, "redline DDL must emit Meta WAL");
    transaction
        .commit_modified(token)
        .await
        .expect("redline DDL commit must succeed");
    assert_eq!(transaction.get_status(), Transaction2PcStatus::Commited);
}

fn memory_meta() -> KVTableMeta {
    KVTableMeta::new(
        KVDBTableType::MemOrdTab,
        false,
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
            max: *nanos.last().expect("redline samples must not be empty"),
        }
    }
}

fn percentile(sorted: &[u128], percentile: usize) -> u128 {
    let index = (sorted.len() - 1) * percentile / 100;
    sorted[index]
}

fn print_stats(profile: &Profile, stats: &ProfileStats) {
    println!(
        "{:<10} {:>6} {:>5} {:<8} {:>7} {:>12} {:>12} {:>12} {:>12}",
        profile.protocol.label(),
        profile.write_count,
        profile.concurrency,
        profile.topology.label(),
        profile.concurrency * ROUNDS,
        stats.commit.p50,
        stats.commit.p99,
        stats.release.p99,
        stats.batch.p99,
    );
    assert!(stats.commit.max <= stats.release.max);
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
            "pi_db_publication_contention_redline_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path).expect("redline temporary root must be created");
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
