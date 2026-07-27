//! `KVDBManager` listener 正常生产域的真实批处理与持久化事件契约。
//!
//! 本 target 不引用或运行旧测试。它使用真实 4-worker runtime、`Transaction2PcManager`、
//! `CommitLogger`、Meta/LogOrdered/Btree、异步 collector 和临时文件系统，验证：
//!
//! - 一个受控短回调阻塞期间排队的 3072 个报告事件被精确组成一个满批；
//! - 低于上限的报告事件经空闲窗口交付，回调 drain 后复用的 `Vec` 不保留旧事件；
//! - 真实 DDL 和双表普通事务分别产生 Meta、LogOrdered、Btree 的确认事件；
//! - 事件 source、表名、当前表类型标签、TID/CID 与产生事件的事务严格匹配；
//! - 没有 listener 时，报告请求返回 `ConnectionAborted`；
//! - 最终权威值、manager 计数和根 WAL 确认状态与事件观察结果一致。
//!
//! Meta/LogOrdered 当前都携带 `BtreeOrdTab` 标签，这是 `FIND-EVENT-001` 已归档的非最终实现
//! 事实，本测试只防止事实被误读，不认可该标签是合理的事件模型。callback panic、消费者死亡、
//! 启动末尾状态覆盖、关闭竞态和存储故障均不属于本正常路径 target。

use std::{
    fmt::Debug,
    fs,
    future::Future,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use futures::future::join_all;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::{
    manager_2pc::Transaction2PcManager, AsyncCommitLog, Transaction2Pc,
};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    utils::{CreateTableOptions, KVDBEvent},
    Binary, KVDBTableType, KVTableMeta,
};
use pi_guid::{Guid, GuidGen};
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;
type RealTrManager = Transaction2PcManager<usize, CommitLogger>;

const META_TABLE: &str = ".tables_meta";
const LOG_ORDERED: &str = "listener_log_ordered";
const BTREE: &str = "listener_btree";
const DDL_SOURCE: &str = "listener DDL";
const BUSINESS_SOURCE: &str = "listener business";
const LISTENER_BATCH_LIMIT: usize = 3072;
const TEST_TIMEOUT: Duration = Duration::from_secs(120);
const SHORT_TIMEOUT: Duration = Duration::from_secs(10);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(90);

#[test]
fn test_manager_listener_normal_contract() {
    let root = TempRoot::new("normal").expect("creating listener test root must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let (observation_tx, observation_rx) = unbounded();
        let (gate_enter_tx, gate_enter_rx) = bounded(1);
        let (gate_release_tx, gate_release_rx) = bounded(1);
        let fixture = build_database_with_listener(
            &rt,
            &root_path,
            observation_tx,
            gate_enter_tx,
            gate_release_rx,
        )
        .await?;

        verify_batching_contract(
            &rt,
            &fixture,
            &observation_rx,
            &gate_enter_rx,
            &gate_release_tx,
        )
        .await?;
        verify_real_confirmation_events(&rt, &fixture, &observation_rx).await?;
        verify_no_listener_error(&rt, &root_path).await?;
        fixture.db.close();
        Ok(())
    })
    .unwrap_or_else(|error| panic!("manager listener normal contract failed: {error}"));
}

async fn verify_batching_contract(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
    observation_rx: &Receiver<BatchObservation>,
    gate_enter_rx: &Receiver<()>,
    gate_release_tx: &Sender<()>,
) -> TestResult<()> {
    let producer_db = fixture.db.clone();
    let producer_gate_enter = gate_enter_rx.clone();
    let producer_gate_release = gate_release_tx.clone();
    let producer = thread::spawn(move || -> TestResult<()> {
        producer_gate_enter
            .recv_timeout(SHORT_TIMEOUT)
            .map_err(|error| {
                format!(
                    "listener callback did not enter the controlled gate within {:?}: {error}",
                    SHORT_TIMEOUT,
                )
            })?;
        let results = futures::executor::block_on(join_all(
            (0..LISTENER_BATCH_LIMIT).map(|_| producer_db.report_transaction_info()),
        ));
        let mut send_result = Ok(());
        for (index, result) in results.into_iter().enumerate() {
            if let Err(error) = result {
                send_result = Err(format!(
                    "queueing full batch report {index} failed: {error}"
                ));
                break;
            }
        }
        let release_result = producer_gate_release
            .send(())
            .map_err(|error| format!("releasing listener callback gate failed: {error}"));
        send_result.and(release_result)
    });

    fixture
        .db
        .report_transaction_info()
        .await
        .map_err(|error| format!("sending gate report failed: {error}"))?;
    producer
        .join()
        .map_err(|_| "listener batch producer thread panicked".to_owned())??;

    let gate_batch = wait_for_observation(rt, observation_rx, SHORT_TIMEOUT).await?;
    assert_batch_context(&gate_batch, fixture, "gate batch")?;
    expect_eq("gate batch events", &gate_batch.events, &vec![ObservedEvent::Report])?;
    expect_eq("gate callback timeout", &gate_batch.gate_timed_out, &false)?;

    let full_batch = wait_for_observation(rt, observation_rx, SHORT_TIMEOUT).await?;
    assert_batch_context(&full_batch, fixture, "full batch")?;
    expect_eq("full batch length", &full_batch.events.len(), &LISTENER_BATCH_LIMIT)?;
    require(
        full_batch
            .events
            .iter()
            .all(|event| event == &ObservedEvent::Report),
        "full listener batch contained a non-report event",
    )?;
    expect_eq("full callback timeout", &full_batch.gate_timed_out, &false)?;

    for index in 0..2 {
        fixture
            .db
            .report_transaction_info()
            .await
            .map_err(|error| format!("sending partial batch report {index} failed: {error}"))?;
    }
    let partial_batch = wait_for_observation(rt, observation_rx, SHORT_TIMEOUT).await?;
    assert_batch_context(&partial_batch, fixture, "partial batch")?;
    expect_eq(
        "partial batch events",
        &partial_batch.events,
        &vec![ObservedEvent::Report, ObservedEvent::Report],
    )?;
    expect_eq("partial callback timeout", &partial_batch.gate_timed_out, &false)?;

    rt.timeout(100).await;
    expect_channel_empty(
        observation_rx,
        "listener repeated a drained report batch or produced an unexpected event",
    )
}

async fn verify_real_confirmation_events(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
    observation_rx: &Receiver<BatchObservation>,
) -> TestResult<()> {
    let ddl = writable_transaction(&fixture.db, DDL_SOURCE)?;
    ddl
        .create_table_with_options(
            Atom::from(LOG_ORDERED),
            table_meta(KVDBTableType::LogOrdTab),
            CreateTableOptions::LogOrdTab(
                64 * 1024 * 1024,
                1024 * 1024,
                1024 * 1024,
            ),
            false,
        )
        .await
        .map_err(|error| format!("creating listener LogOrdered table failed: {error}"))?;
    ddl
        .create_table_with_options(
            Atom::from(BTREE),
            table_meta(KVDBTableType::BtreeOrdTab),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .map_err(|error| format!("creating listener Btree table failed: {error}"))?;
    let ddl_identity = commit_transaction(&ddl, "listener DDL").await?;

    let key = encode_usize(71_001);
    let log_value = encode_usize(81_001);
    let btree_value = encode_usize(81_002);
    let business = writable_transaction(&fixture.db, BUSINESS_SOURCE)?;
    business
        .upsert(vec![
            TableKV::new(
                Atom::from(LOG_ORDERED),
                key.clone(),
                Some(log_value.clone()),
            ),
            TableKV::new(
                Atom::from(BTREE),
                key.clone(),
                Some(btree_value.clone()),
            ),
        ])
        .await
        .map_err(|error| format!("writing listener business values failed: {error:?}"))?;
    let business_identity = commit_transaction(&business, "listener business").await?;

    expect_eq(
        "listener committed root count",
        &fixture.logger.append_total_count(),
        &2usize,
    )?;
    expect_eq(
        "listener manager produced roots",
        &fixture.tr_manager.produced_transaction_total(),
        &2usize,
    )?;
    expect_eq(
        "listener manager consumed roots",
        &fixture.tr_manager.consumed_transaction_total(),
        &2usize,
    )?;
    expect_eq(
        "listener manager active roots",
        &fixture.tr_manager.transaction_len(),
        &0usize,
    )?;

    let mut observed = Vec::new();
    let deadline = Instant::now() + CONFIRM_TIMEOUT;
    loop {
        while let Ok(batch) = observation_rx.try_recv() {
            assert_batch_context(&batch, fixture, "confirmation batch")?;
            expect_eq("confirmation callback timeout", &batch.gate_timed_out, &false)?;
            for event in batch.events {
                match event {
                    ObservedEvent::Report => {
                        return Err("unexpected report event during confirmation phase".to_owned());
                    },
                    ObservedEvent::CommitFailed(payload) => {
                        return Err(format!(
                            "real successful transaction produced CommitFailed: {payload:?}"
                        ));
                    },
                    ObservedEvent::Confirmed(payload) => observed.push(payload),
                }
            }
        }

        if observed.len() == 3
            && fixture.logger.confirm_total_count() == 2
            && fixture.logger.waiting_confirm_count().await == 0 {
            break;
        }
        if observed.len() > 3 {
            return Err(format!(
                "listener produced more confirmation events than expected: {observed:?}"
            ));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "listener confirmations did not close before {:?}: events={observed:?}, appended={}, confirmed={}, waiting={}",
                CONFIRM_TIMEOUT,
                fixture.logger.append_total_count(),
                fixture.logger.confirm_total_count(),
                fixture.logger.waiting_confirm_count().await,
            ));
        }
        rt.timeout(10).await;
    }

    observed.sort_by(|left, right| left.table.cmp(&right.table));
    let mut expected = vec![
        EventPayload::new(
            DDL_SOURCE,
            META_TABLE,
            KVDBTableType::BtreeOrdTab,
            ddl_identity.0.clone(),
            ddl_identity.1.clone(),
        ),
        EventPayload::new(
            BUSINESS_SOURCE,
            LOG_ORDERED,
            KVDBTableType::BtreeOrdTab,
            business_identity.0.clone(),
            business_identity.1.clone(),
        ),
        EventPayload::new(
            BUSINESS_SOURCE,
            BTREE,
            KVDBTableType::BtreeOrdTab,
            business_identity.0,
            business_identity.1,
        ),
    ];
    expected.sort_by(|left, right| left.table.cmp(&right.table));
    expect_eq("real confirmation payload set", &observed, &expected)?;
    assert_authoritative_values(fixture, &key, &log_value, &btree_value).await?;

    rt.timeout(100).await;
    expect_channel_empty(
        observation_rx,
        "listener produced a duplicate confirmation after WAL closure",
    )
}

async fn assert_authoritative_values(
    fixture: &Fixture,
    key: &Binary,
    log_value: &Binary,
    btree_value: &Binary,
) -> TestResult<()> {
    let read = fixture
        .db
        .transaction(Atom::from("listener authoritative read"), false, 0, 0)
        .ok_or_else(|| "creating listener authoritative read transaction failed".to_owned())?;
    let values = read
        .query(vec![
            TableKV::new(Atom::from(LOG_ORDERED), key.clone(), None),
            TableKV::new(Atom::from(BTREE), key.clone(), None),
        ])
        .await;
    expect_eq(
        "listener authoritative values",
        &values,
        &vec![Some(log_value.clone()), Some(btree_value.clone())],
    )
}

async fn verify_no_listener_error(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
) -> TestResult<()> {
    let fixture = build_database_without_listener(rt, &root.join("no-listener")).await?;
    let error = fixture
        .db
        .report_transaction_info()
        .await
        .expect_err("reporting without a listener must fail");
    expect_eq(
        "no-listener report error kind",
        &error.kind(),
        &ErrorKind::ConnectionAborted,
    )?;
    fixture.db.close();
    Ok(())
}

async fn commit_transaction(
    transaction: &RealTransaction,
    label: &str,
) -> TestResult<(Guid, Guid)> {
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing {label} failed: {error:?}"))?;
    let transaction_uid = transaction
        .get_transaction_uid()
        .ok_or_else(|| format!("{label} prepare did not allocate a transaction UID"))?;
    let commit_uid = transaction
        .get_commit_uid()
        .ok_or_else(|| format!("{label} prepare did not allocate a commit UID"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing {label} failed: {error:?}"))?;
    Ok((transaction_uid, commit_uid))
}

fn writable_transaction(db: &RealDb, source: &str) -> TestResult<RealTransaction> {
    db.transaction(Atom::from(source), true, 10_000, 10_000)
        .ok_or_else(|| format!("database rejected writable transaction {source}"))
}

fn table_meta(table_type: KVDBTableType) -> KVTableMeta {
    KVTableMeta::new(table_type, true, EnumType::Usize, EnumType::Usize)
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

async fn wait_for_observation(
    rt: &MultiTaskRuntime<()>,
    receiver: &Receiver<BatchObservation>,
    timeout: Duration,
) -> TestResult<BatchObservation> {
    let deadline = Instant::now() + timeout;
    loop {
        match receiver.try_recv() {
            Ok(observation) => return Ok(observation),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                return Err("listener observation channel disconnected".to_owned());
            },
            Err(crossbeam_channel::TryRecvError::Empty) => {},
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "listener did not publish an observation within {timeout:?}"
            ));
        }
        rt.timeout(1).await;
    }
}

fn assert_batch_context(
    batch: &BatchObservation,
    fixture: &Fixture,
    label: &str,
) -> TestResult<()> {
    expect_eq(
        &format!("{label} database path"),
        &batch.db_path,
        &fixture.db.db_path().to_path_buf(),
    )?;
    expect_eq(
        &format!("{label} active transaction count"),
        &batch.transaction_len,
        &0usize,
    )
}

fn expect_channel_empty<T: Debug>(receiver: &Receiver<T>, message: &str) -> TestResult<()> {
    match receiver.try_recv() {
        Ok(value) => Err(format!("{message}: {value:?}")),
        Err(crossbeam_channel::TryRecvError::Disconnected) => {
            Err(format!("{message}: observation channel disconnected"))
        },
        Err(crossbeam_channel::TryRecvError::Empty) => Ok(()),
    }
}

async fn build_database_with_listener(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
    observation_tx: Sender<BatchObservation>,
    gate_enter_tx: Sender<()>,
    gate_release_rx: Receiver<()>,
) -> TestResult<Fixture> {
    let (tr_manager, logger) = build_transaction_manager(rt, root).await?;
    let db_path = root.join("database");
    let callback_count = Arc::new(AtomicUsize::new(0));
    let callback_count_copy = callback_count.clone();
    let db = KVDBManagerBuilder::new(rt.clone(), tr_manager.clone(), &db_path)
        .startup_with_listener(
            false,
            Some(
                move |db: &RealDb, manager: &RealTrManager, events: &mut Vec<KVDBEvent<Guid>>| {
                    let invocation = callback_count_copy.fetch_add(1, Ordering::SeqCst);
                    let observed_events = events.drain(..).map(ObservedEvent::from).collect();
                    let gate_timed_out = if invocation == 0 {
                        let _ = gate_enter_tx.send(());
                        gate_release_rx.recv_timeout(SHORT_TIMEOUT).is_err()
                    } else {
                        false
                    };
                    let _ = observation_tx.send(BatchObservation {
                        events: observed_events,
                        db_path: db.db_path().to_path_buf(),
                        transaction_len: manager.transaction_len(),
                        gate_timed_out,
                    });
                },
            ),
        )
        .await
        .map_err(|error| format!("starting listener database at {db_path:?} failed: {error}"))?;
    Ok(Fixture {
        db,
        tr_manager,
        logger,
    })
}

async fn build_database_without_listener(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
) -> TestResult<Fixture> {
    let (tr_manager, logger) = build_transaction_manager(rt, root).await?;
    let db_path = root.join("database");
    let db = KVDBManagerBuilder::new(rt.clone(), tr_manager.clone(), &db_path)
        .startup(false)
        .await
        .map_err(|error| {
            format!("starting no-listener database at {db_path:?} failed: {error}")
        })?;
    Ok(Fixture {
        db,
        tr_manager,
        logger,
    })
}

async fn build_transaction_manager(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
) -> TestResult<(RealTrManager, CommitLogger)> {
    fs::create_dir_all(root)
        .map_err(|error| format!("creating listener fixture root {root:?} failed: {error}"))?;
    let wal_path = root.join("root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building CommitLogger at {wal_path:?} failed: {error}"))?;
    let manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger.clone(),
    );
    Ok((manager, logger))
}

fn run_on_runtime<T, F, Fut>(timeout: Duration, build: F) -> TestResult<T>
where
    T: Send + 'static,
    F: FnOnce(MultiTaskRuntime<()>) -> Fut,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    let _time_loop = startup_global_time_loop(10);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(4)
        .build();
    let future = build(rt.clone());
    let (result_tx, result_rx) = bounded(1);

    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    })
    .map_err(|error| format!("spawning listener contract future failed: {error:?}"))?;

    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("listener contract future exceeded {timeout:?}: {error}"))?
}

fn expect_eq<T: Debug + PartialEq>(
    label: &str,
    actual: &T,
    expected: &T,
) -> TestResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{label}: expected {expected:?}, observed {actual:?}"
        ))
    }
}

fn require(condition: bool, message: &str) -> TestResult<()> {
    if condition {
        Ok(())
    } else {
        Err(message.to_owned())
    }
}

struct Fixture {
    db: RealDb,
    tr_manager: RealTrManager,
    logger: CommitLogger,
}

#[derive(Debug)]
struct BatchObservation {
    events: Vec<ObservedEvent>,
    db_path: PathBuf,
    transaction_len: usize,
    gate_timed_out: bool,
}

#[derive(Clone, Debug, PartialEq)]
enum ObservedEvent {
    Report,
    CommitFailed(EventPayload),
    Confirmed(EventPayload),
}

impl From<KVDBEvent<Guid>> for ObservedEvent {
    fn from(event: KVDBEvent<Guid>) -> Self {
        match event {
            KVDBEvent::ReportTrInfo => Self::Report,
            KVDBEvent::CommitFailed(source, table, table_type, transaction_uid, commit_uid) => {
                Self::CommitFailed(EventPayload::new(
                    source.as_str(),
                    table.as_str(),
                    table_type,
                    transaction_uid,
                    commit_uid,
                ))
            },
            KVDBEvent::ConfirmCommited(source, table, table_type, transaction_uid, commit_uid) => {
                Self::Confirmed(EventPayload::new(
                    source.as_str(),
                    table.as_str(),
                    table_type,
                    transaction_uid,
                    commit_uid,
                ))
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct EventPayload {
    source: String,
    table: String,
    table_type: KVDBTableType,
    transaction_uid: Guid,
    commit_uid: Guid,
}

impl EventPayload {
    fn new(
        source: &str,
        table: &str,
        table_type: KVDBTableType,
        transaction_uid: Guid,
        commit_uid: Guid,
    ) -> Self {
        Self {
            source: source.to_owned(),
            table: table.to_owned(),
            table_type,
            transaction_uid,
            commit_uid,
        }
    }
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(label: &str) -> TestResult<Self> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system time is before UNIX_EPOCH: {error}"))?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "pi_db_manager_listener_contract_{label}_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path)
            .map_err(|error| format!("creating temporary root {path:?} failed: {error}"))?;
        Ok(Self { path })
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
