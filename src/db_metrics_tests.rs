use std::{collections::{BTreeSet, HashMap},
          fs,
          future::Future,
          path::{Path, PathBuf},
          sync::{Arc, Mutex},
          time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

use crossbeam_channel::bounded;
use opentelemetry::{KeyValue, metrics::MeterProvider as _};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter,
                                SdkMeterProvider,
                                data::{AggregatedMetrics, Metric, MetricData, ResourceMetrics}};
use pi_async_rt::rt::{AsyncRuntime,
                      startup_global_time_loop,
                      multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder}};
use pi_async_transaction::manager_2pc::Transaction2PcManager;
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

use crate::{Binary,
            KVDBTableType,
            KVTableMeta,
            TableKeyVersion,
            key_version::{KeyVersionApiMetricsSnapshot, KeyVersionCacheMetricsSnapshot},
            tables::TableKV,
            utils::CreateTableOptions};

use super::{DatabaseTraceInstruments,
            KEY_VERSION_2PC_CALLS_METRIC,
            KEY_VERSION_ESTIMATED_MEMORY_METRIC,
            KEY_VERSION_QUERY_CALLS_METRIC,
            KEY_VERSION_RECORD_COUNT_METRIC,
            KVDBManager,
            KVDBManagerBuilder,
            KVDBTransaction,
            TABLE_CACHE_SIZE_METRIC,
            TRANSACTION_LIFECYCLE_METRIC,
            TransactionLifecycleMetricsSnapshot,
            DEFAULT_DB_TABLES_META_DIR};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const MEMORY_TABLE: &str = "metrics_memory";
const LOG_ORDERED_TABLE: &str = "metrics_log_ordered";
const BTREE_TABLE: &str = "metrics_btree";
const TEST_TIMEOUT: Duration = Duration::from_secs(45);
static REAL_DB_METRICS_TEST_LOCK: Mutex<()> = Mutex::new(());

/// 直接读取 SDK 聚合结果，固定 instrument 名、低基数 label、Counter 值和删表零 Gauge。
#[test]
fn test_trace_instruments_export_exact_contract() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    let meter = provider.meter("pi_db_metrics_test");
    let instruments = DatabaseTraceInstruments::new(&meter);
    let table = Atom::from(MEMORY_TABLE);

    instruments.record_table(&table, 41, KeyVersionCacheMetricsSnapshot {
        record_count: 3,
        estimated_memory_bytes: 521,
    });
    instruments.record_api_delta(KeyVersionApiMetricsSnapshot {
        query_success: 2,
        query_failure: 3,
        prepare_success: 5,
        prepare_failure: 7,
        commit_success: 11,
        commit_failure: 13,
    });
    instruments.record_transaction_delta(TransactionLifecycleMetricsSnapshot {
        created: 17,
        closed: 19,
    });
    provider.force_flush().expect("forcing first metric export must succeed");

    let first = exporter
        .get_finished_metrics()
        .expect("reading first metric export must succeed");
    let names = metric_names(&first);
    assert_eq!(names, BTreeSet::from([
        TABLE_CACHE_SIZE_METRIC,
        KEY_VERSION_RECORD_COUNT_METRIC,
        KEY_VERSION_ESTIMATED_MEMORY_METRIC,
        KEY_VERSION_QUERY_CALLS_METRIC,
        KEY_VERSION_2PC_CALLS_METRIC,
        TRANSACTION_LIFECYCLE_METRIC,
    ]));
    assert_eq!(gauge_value(&first, TABLE_CACHE_SIZE_METRIC,
                           &[("table", MEMORY_TABLE)]), 41);
    assert_eq!(gauge_value(&first, KEY_VERSION_RECORD_COUNT_METRIC,
                           &[("table", MEMORY_TABLE)]), 3);
    assert_eq!(gauge_value(&first, KEY_VERSION_ESTIMATED_MEMORY_METRIC,
                           &[("table", MEMORY_TABLE)]), 521);
    assert_eq!(counter_value(&first, KEY_VERSION_QUERY_CALLS_METRIC,
                             &[("result", "success")]), 2);
    assert_eq!(counter_value(&first, KEY_VERSION_QUERY_CALLS_METRIC,
                             &[("result", "failure")]), 3);
    assert_eq!(counter_value(&first, KEY_VERSION_2PC_CALLS_METRIC,
                             &[("phase", "prepare"), ("result", "success")]), 5);
    assert_eq!(counter_value(&first, KEY_VERSION_2PC_CALLS_METRIC,
                             &[("phase", "prepare"), ("result", "failure")]), 7);
    assert_eq!(counter_value(&first, KEY_VERSION_2PC_CALLS_METRIC,
                             &[("phase", "commit"), ("result", "success")]), 11);
    assert_eq!(counter_value(&first, KEY_VERSION_2PC_CALLS_METRIC,
                             &[("phase", "commit"), ("result", "failure")]), 13);
    assert_eq!(counter_value(&first, TRANSACTION_LIFECYCLE_METRIC,
                             &[("event", "created")]), 17);
    assert_eq!(counter_value(&first, TRANSACTION_LIFECYCLE_METRIC,
                             &[("event", "closed")]), 19);

    exporter.reset();
    instruments.record_removed_table(&table);
    provider.force_flush().expect("forcing removed-table metric export must succeed");
    let removed = exporter
        .get_finished_metrics()
        .expect("reading removed-table metric export must succeed");
    assert_eq!(gauge_value(&removed, KEY_VERSION_RECORD_COUNT_METRIC,
                           &[("table", MEMORY_TABLE)]), 0);
    assert_eq!(gauge_value(&removed, KEY_VERSION_ESTIMATED_MEMORY_METRIC,
                           &[("table", MEMORY_TABLE)]), 0);
}

/// 真实四表装配下，公开 API 结果、版本记录容量和调用/事务累计值必须严格一致。
#[test]
fn test_trace_metrics_follow_real_database_operations() {
    let _serial = REAL_DB_METRICS_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let root = TempRoot::new("real").expect("creating metrics test root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let db = build_database(&rt, &root_path).await?;
        create_tables(&db).await?;
        verify_real_metrics(&db).await
    }).unwrap_or_else(|error| panic!("real key-version metrics contract failed: {error}"));
}

/// 真实 TTL loop 淘汰全部首次观察后，表内 Map、记录 Gauge 和内存 Gauge 必须共同归零。
#[test]
fn test_trace_cache_metrics_converge_after_real_ttl_expiry() {
    let _serial = REAL_DB_METRICS_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let root = TempRoot::new("ttl").expect("creating TTL metrics test root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let db = build_database_with_ttl(&rt,
                                         &root_path,
                                         Duration::from_secs(1),
                                         Duration::from_millis(5)).await?;
        verify_real_ttl_metrics(&rt, &db).await
    }).unwrap_or_else(|error| panic!("real TTL metrics contract failed: {error}"));
}

async fn verify_real_metrics(db: &RealDb) -> TestResult<()> {
    let tables = [DEFAULT_DB_TABLES_META_DIR,
                  MEMORY_TABLE,
                  LOG_ORDERED_TABLE,
                  BTREE_TABLE];
    let mut baselines = HashMap::new();
    for table in tables {
        let metrics = db
            .table_tracing_metrics(&Atom::from(table))
            .await
            .ok_or_else(|| format!("registered table {table} has no tracing metrics"))?
            .1;
        baselines.insert(table, metrics);
    }
    let api_before = db.0.key_versions.api_metrics_snapshot();
    let lifecycle_before = db.transaction_metrics_snapshot();

    for (index, table) in tables.into_iter().enumerate() {
        let (value, _version) = db
            .query_with_version(Atom::from(table), encode_usize(10_000 + index))
            .await
            .map_err(|error| format!("initial query_with_version for {table} failed: {error:?}"))?;
        require(value.is_none(), &format!("initial query for {table} unexpectedly found a value"))?;
    }
    require(db
        .query_with_version(Atom::from(""), encode_usize(1))
        .await
        .is_err(),
        "invalid query_with_version must fail")?;

    let mut read_set = Vec::new();
    let mut write_set = Vec::new();
    for (index, table) in [MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE]
        .into_iter()
        .enumerate() {
        let key = encode_usize(20_000 + index);
        let (value, version) = db
            .query_with_version(Atom::from(table), key.clone())
            .await
            .map_err(|error| format!("write baseline query for {table} failed: {error:?}"))?;
        require(value.is_none(), &format!("write baseline for {table} unexpectedly exists"))?;
        read_set.push(TableKeyVersion {
            table: Atom::from(table),
            key: key.clone(),
            version,
        });
        write_set.push(TableKV::new(Atom::from(table),
                                    key,
                                    Some(encode_usize(30_000 + index))));
    }
    let transaction = writable_transaction(db, "metrics version success")?;
    let prepare = transaction
        .prepare_with_version(read_set, write_set.clone())
        .await
        .map_err(|error| format!("metrics version prepare failed: {error:?}"))?;
    let receipt = transaction
        .commit_with_version(prepare)
        .await
        .map_err(|error| format!("metrics version commit failed: {error:?}"))?;
    require(receipt.len() == write_set.len(),
            "version commit receipt length does not match write set")?;
    drop(transaction);

    for write in &write_set {
        let (value, version) = db
            .query_with_version(write.table.clone(), write.key.clone())
            .await
            .map_err(|error| format!("authoritative query for {:?} failed: {error:?}",
                                    write.table.as_str()))?;
        require(binary_equal(value.as_ref(), write.value.as_ref()),
                &format!("authoritative value for {:?} does not match committed value",
                         write.table.as_str()))?;
        let committed = receipt
            .iter()
            .find(|item| item.table == write.table && item.key == write.key)
            .ok_or_else(|| format!("receipt is missing {:?}", write.table.as_str()))?;
        require(version == committed.version,
                &format!("authoritative version for {:?} does not match receipt",
                         write.table.as_str()))?;
    }

    let invalid_prepare = writable_transaction(db, "metrics prepare failure")?;
    require(invalid_prepare
        .prepare_with_version(Vec::new(), vec![TableKV::new(
            Atom::from(MEMORY_TABLE),
            Binary::new(Vec::new()),
            Some(encode_usize(1)))])
        .await
        .is_err(),
        "empty Key version prepare must fail")?;
    drop(invalid_prepare);

    let invalid_commit = writable_transaction(db, "metrics commit failure")?;
    require(invalid_commit.commit_with_version(Vec::new()).await.is_err(),
            "commit without version prepare must fail")?;
    drop(invalid_commit);

    let lifecycle_owner = db
        .transaction(Atom::from("metrics final owner"), false, 10_000, 10_000)
        .ok_or_else(|| "database rejected lifecycle transaction".to_owned())?;
    let lifecycle_clone = lifecycle_owner.clone();
    let root_owners_before_iterator = match &lifecycle_owner {
        KVDBTransaction::RootTr(root) => Arc::strong_count(&root.0),
        _ => return Err("manager returned a non-root transaction".to_owned()),
    };
    let lifecycle_iterator = lifecycle_owner
        .keys(Atom::from(DEFAULT_DB_TABLES_META_DIR), None, false)
        .await
        .ok_or_else(|| "Meta keys iterator was not created".to_owned())?;
    let root_owners_after_iterator = match &lifecycle_owner {
        KVDBTransaction::RootTr(root) => Arc::strong_count(&root.0),
        _ => return Err("manager transaction changed variant".to_owned()),
    };
    require(root_owners_after_iterator == root_owners_before_iterator,
            "creating a detached iterator unexpectedly retained the root transaction")?;
    // 外部协议要求流先于根事务结束；该顺序同时证明流不是 closed 指标的 owner。
    drop(lifecycle_iterator);
    let after_create = db.transaction_metrics_snapshot();
    require(after_create.created == lifecycle_before.created + 4,
            "real operations did not create exactly four observed root transactions")?;
    require(after_create.closed == lifecycle_before.closed + 3,
            "root transaction closed before its final clone was dropped")?;
    drop(lifecycle_owner);
    require(db.transaction_metrics_snapshot().closed == lifecycle_before.closed + 3,
            "dropping a non-final root owner incorrectly incremented closed")?;
    drop(lifecycle_clone);
    let lifecycle_after = db.transaction_metrics_snapshot();
    require(lifecycle_after.created == lifecycle_before.created + 4,
            "root created count changed without transaction creation")?;
    require(lifecycle_after.closed == lifecycle_before.closed + 4,
            "final root owner did not increment closed exactly once")?;

    let api_delta = db
        .0
        .key_versions
        .api_metrics_snapshot()
        .delta_since(api_before);
    require(api_delta == KeyVersionApiMetricsSnapshot {
        query_success: 10,
        query_failure: 1,
        prepare_success: 1,
        prepare_failure: 1,
        commit_success: 1,
        commit_failure: 1,
    }, &format!("unexpected real API metric delta: {api_delta:?}"))?;

    for table in tables {
        let current = db
            .table_tracing_metrics(&Atom::from(table))
            .await
            .ok_or_else(|| format!("table {table} disappeared during metric verification"))?
            .1;
        let baseline = baselines.get(table).unwrap();
        let expected_added = if table == DEFAULT_DB_TABLES_META_DIR { 1 } else { 2 };
        require(current.record_count == baseline.record_count + expected_added,
                &format!("table {table} record metric delta is not {expected_added}"))?;
        require(current.estimated_memory_bytes > baseline.estimated_memory_bytes,
                &format!("table {table} memory estimate did not increase"))?;
    }

    let before_rejection = db.transaction_metrics_snapshot();
    db.close();
    require(db.transaction(Atom::from("metrics rejected"), true, 10_000, 10_000).is_none(),
            "closed database accepted a new transaction")?;
    require(db.transaction_metrics_snapshot() == before_rejection,
            "rejected transaction changed lifecycle metrics")
}

async fn verify_real_ttl_metrics(rt: &MultiTaskRuntime<()>, db: &RealDb) -> TestResult<()> {
    const RECORDS: usize = 64;

    let table = Atom::from(DEFAULT_DB_TABLES_META_DIR);
    let baseline = db
        .table_tracing_metrics(&table)
        .await
        .ok_or_else(|| "Meta table has no tracing metrics before TTL test".to_owned())?
        .1;
    require(baseline == KeyVersionCacheMetricsSnapshot::default(),
            &format!("startup Meta version metrics are not empty: {baseline:?}"))?;
    let api_before = db.0.key_versions.api_metrics_snapshot();

    for index in 0..RECORDS {
        let (value, _version) = db
            .query_with_version(table.clone(), encode_usize(50_000 + index))
            .await
            .map_err(|error| format!("TTL baseline query {index} failed: {error:?}"))?;
        require(value.is_none(), &format!("TTL baseline query {index} unexpectedly found data"))?;
    }
    let populated = db
        .table_tracing_metrics(&table)
        .await
        .ok_or_else(|| "Meta table disappeared after TTL observations".to_owned())?
        .1;
    require(populated.record_count == RECORDS as u64,
            &format!("TTL observations produced {} records instead of {RECORDS}",
                     populated.record_count))?;
    require(populated.estimated_memory_bytes > 0,
            "TTL observations did not increase estimated memory")?;

    let deadline = Instant::now() + Duration::from_secs(8);
    let expired = loop {
        let current = db
            .table_tracing_metrics(&table)
            .await
            .ok_or_else(|| "Meta table disappeared during TTL collection".to_owned())?
            .1;
        if current == KeyVersionCacheMetricsSnapshot::default() {
            break current;
        }
        if Instant::now() >= deadline {
            return Err(format!("TTL metrics did not converge before deadline: {current:?}"));
        }
        rt.timeout(5).await;
    };
    require(expired == KeyVersionCacheMetricsSnapshot::default(),
            "expired TTL metrics are not exactly zero")?;
    let map_len = {
        let tables = db.0.tables.read().await;
        tables
            .get(&table)
            .ok_or_else(|| "Meta table disappeared before Map verification".to_owned())?
            .versions
            .len()
    };
    require(map_len == 0, "TTL metrics reached zero while the version Map was non-empty")?;
    let api_delta = db
        .0
        .key_versions
        .api_metrics_snapshot()
        .delta_since(api_before);
    require(api_delta == KeyVersionApiMetricsSnapshot {
        query_success: RECORDS as u64,
        query_failure: 0,
        prepare_success: 0,
        prepare_failure: 0,
        commit_success: 0,
        commit_failure: 0,
    }, &format!("TTL queries produced unexpected API metrics: {api_delta:?}"))
}

async fn build_database(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<RealDb> {
    build_database_with_ttl(rt, root, Duration::ZERO, Duration::ZERO).await
}

async fn build_database_with_ttl(rt: &MultiTaskRuntime<()>,
                                 root: &Path,
                                 ttl: Duration,
                                 poll_interval: Duration) -> TestResult<RealDb> {
    fs::create_dir_all(root)
        .map_err(|error| format!("creating metrics fixture root failed: {error}"))?;
    let wal_path = root.join("root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building metrics CommitLogger failed: {error}"))?;
    let manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger,
    );
    KVDBManagerBuilder::new(rt.clone(), manager, root.join("database"))
        .key_version_ttl(ttl)
        .key_version_ttl_poll_interval(poll_interval)
        .startup(false)
        .await
        .map_err(|error| format!("starting metrics database failed: {error}"))
}

async fn create_tables(db: &RealDb) -> TestResult<()> {
    let transaction = writable_transaction(db, "metrics DDL")?;
    transaction
        .create_table(Atom::from(MEMORY_TABLE),
                      table_meta(KVDBTableType::MemOrdTab),
                      false)
        .await
        .map_err(|error| format!("creating metrics Memory table failed: {error}"))?;
    transaction
        .create_table_with_options(Atom::from(LOG_ORDERED_TABLE),
                                   table_meta(KVDBTableType::LogOrdTab),
                                   CreateTableOptions::LogOrdTab(
                                       64 * 1024 * 1024,
                                       1024 * 1024,
                                       1024 * 1024),
                                   false)
        .await
        .map_err(|error| format!("creating metrics LogOrdered table failed: {error}"))?;
    transaction
        .create_table_with_options(Atom::from(BTREE_TABLE),
                                   table_meta(KVDBTableType::BtreeOrdTab),
                                   CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
                                   false)
        .await
        .map_err(|error| format!("creating metrics Btree table failed: {error}"))?;
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing metrics DDL failed: {error:?}"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing metrics DDL failed: {error:?}"))?;
    drop(transaction);
    require(db.table_size().await == 4, "metrics DDL did not register exactly four tables")
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

fn binary_equal(left: Option<&Binary>, right: Option<&Binary>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.as_ref() == right.as_ref(),
        _ => false,
    }
}

fn metric_names(metrics: &[ResourceMetrics]) -> BTreeSet<&str> {
    metrics
        .iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(|scope| scope.metrics())
        .map(Metric::name)
        .collect()
}

fn find_metric<'a>(metrics: &'a [ResourceMetrics], name: &str) -> &'a Metric {
    metrics
        .iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(|scope| scope.metrics())
        .find(|metric| metric.name() == name)
        .unwrap_or_else(|| panic!("metric {name} was not exported"))
}

fn gauge_value(metrics: &[ResourceMetrics],
               name: &str,
               expected_attributes: &[(&str, &str)]) -> u64 {
    match find_metric(metrics, name).data() {
        AggregatedMetrics::U64(MetricData::Gauge(gauge)) => gauge
            .data_points()
            .find(|point| attributes_match(point.attributes(), expected_attributes))
            .unwrap_or_else(|| panic!("gauge {name} has no matching data point"))
            .value(),
        data => panic!("metric {name} is not a u64 Gauge: {data:?}"),
    }
}

fn counter_value(metrics: &[ResourceMetrics],
                 name: &str,
                 expected_attributes: &[(&str, &str)]) -> u64 {
    match find_metric(metrics, name).data() {
        AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
            .data_points()
            .find(|point| attributes_match(point.attributes(), expected_attributes))
            .unwrap_or_else(|| panic!("counter {name} has no matching data point"))
            .value(),
        data => panic!("metric {name} is not a u64 Sum: {data:?}"),
    }
}

fn attributes_match<'a>(attributes: impl Iterator<Item = &'a KeyValue>,
                        expected: &[(&str, &str)]) -> bool {
    let attributes = attributes.collect::<Vec<_>>();
    attributes.len() == expected.len()
        && expected.iter().all(|(key, value)| {
            attributes.iter().any(|attribute| {
                attribute.key.as_str() == *key && attribute.value.as_str() == *value
            })
        })
}

fn run_on_runtime<T, F, Fut>(timeout: Duration, build: F) -> TestResult<T>
    where T: Send + 'static,
          F: FnOnce(MultiTaskRuntime<()>) -> Fut,
          Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    let _time_loop = startup_global_time_loop(1);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(4)
        .build();
    let future = build(rt.clone());
    let (result_tx, result_rx) = bounded(1);
    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    }).map_err(|error| format!("spawning metrics target failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("metrics target exceeded {timeout:?}: {error}"))?
}

fn require(condition: bool, message: &str) -> TestResult<()> {
    if condition {
        Ok(())
    } else {
        Err(message.to_owned())
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
            "pi_db_key_version_metrics_{label}_{}_{}",
            std::process::id(),
            nanos));
        fs::create_dir_all(&path)
            .map_err(|error| format!("creating metrics temp root failed: {error}"))?;
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
