//! Trace Meter 初始化顺序的真实生产装配专项。
//!
//! 本 target 模拟 `pi_launcher` 的真实顺序：先通过 OpenTelemetry `global` 安装 MeterProvider，
//! 再启动真实 runtime、事务管理器、CommitLogger、文件系统和 `pi_db`。它不调用
//! `pi_logger::opentelemetry::init/is_init`，因为生产使用的是新版 observability 初始化链。
//!
//! 当前 v0.19.6 会永久等待旧 `is_init`，因此修复前本测试必须因六个指标和 loop INFO 均缺失
//! 而失败；修复后它作为 `BUG-TRACE-METER-001` 的永久验收入口。问题与冻结方案见
//! `docs/TRACE_METER_INITIALIZATION_BUG.md#bug-trace-meter-001-index`。

#![cfg(feature = "trace")]

mod key_version_support;

use std::{
    collections::BTreeSet,
    sync::Mutex,
    time::{Duration, Instant},
};

use log::{Level, LevelFilter, Log, Metadata, Record};
use opentelemetry::{global, KeyValue};
use opentelemetry_sdk::metrics::{
    data::{AggregatedMetrics, Metric, MetricData, ResourceMetrics},
    InMemoryMetricExporter, SdkMeterProvider,
};
use pi_async_rt::rt::AsyncRuntime;
use pi_async_transaction::Transaction2Pc;
use pi_atom::Atom;
use pi_db::{
    tables::TableKV, KVDBTableType, TableKeyVersion, Version,
};

use key_version_support::{
    build_database, commit_ordinary, encode_usize, expect_binary, expect_eq, run_on_runtime,
    table_meta, writable_transaction, TempRoot, TestResult, MEMORY_TABLE,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(40);
const TRACE_DEADLINE: Duration = Duration::from_secs(25);
const METER_SCOPE: &str = "pi_db";
const TABLE_CACHE_SIZE_METRIC: &str = "pi_db.db.table_cache_size";
const KEY_VERSION_RECORD_COUNT_METRIC: &str = "pi_db.db.key_version_cache_record_count";
const KEY_VERSION_ESTIMATED_MEMORY_METRIC: &str =
    "pi_db.db.key_version_cache_estimated_memory_bytes";
const KEY_VERSION_QUERY_CALLS_METRIC: &str = "pi_db.db.key_version_query_calls";
const KEY_VERSION_2PC_CALLS_METRIC: &str = "pi_db.db.key_version_2pc_calls";
const TRANSACTION_LIFECYCLE_METRIC: &str = "pi_db.db.transaction_lifecycle";

static CAPTURED_LOGS: Mutex<Vec<String>> = Mutex::new(Vec::new());
static TEST_LOGGER: CapturingLogger = CapturingLogger;

/// 先安装 global Provider，再启动数据库；全部指标和循环日志必须在一个生产周期内出现。
#[test]
fn test_trace_loop_uses_preinitialized_global_meter_provider() {
    log::set_logger(&TEST_LOGGER).expect("trace Meter target must own the process log facade");
    log::set_max_level(LevelFilter::Info);

    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    global::set_meter_provider(provider.clone());

    let root = TempRoot::new("trace_meter_initialization")
        .expect("creating trace Meter temporary root must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        let table = Atom::from(MEMORY_TABLE);
        let ddl = writable_transaction(&fixture.db, "trace Meter DDL")?;
        ddl.create_table(table.clone(),
                         table_meta(KVDBTableType::MemOrdTab, false),
                         false)
            .await
            .map_err(|error| format!("creating trace Meter Memory table failed: {error}"))?;
        commit_ordinary(&ddl, "trace Meter DDL").await?;
        drop(ddl);

        let key = encode_usize(7_001);
        let value = encode_usize(8_001);
        let (baseline_value, baseline_version) = fixture
            .db
            .query_with_version(table.clone(), key.clone())
            .await
            .map_err(|error| format!("loading trace Meter version baseline failed: {error:?}"))?;
        expect_binary("trace Meter missing baseline", baseline_value.as_ref(), None)?;
        if !matches!(baseline_version, Version::Delete(_)) {
            return Err(format!(
                "trace Meter missing baseline must be Delete, observed {baseline_version:?}",
            ));
        }

        let transaction = writable_transaction(&fixture.db, "trace Meter version transaction")?;
        let prepare = transaction
            .prepare_with_version(
                vec![TableKeyVersion {
                    table: table.clone(),
                    key: key.clone(),
                    version: baseline_version,
                }],
                vec![TableKV::new(table.clone(), key.clone(), Some(value.clone()))],
            )
            .await
            .map_err(|error| format!("preparing trace Meter version transaction failed: {error:?}"))?;
        let transaction_uid = transaction
            .get_transaction_uid()
            .ok_or_else(|| "trace Meter prepare did not allocate a transaction UID".to_owned())?;
        let receipt = transaction
            .commit_with_version(prepare)
            .await
            .map_err(|error| format!("committing trace Meter version transaction failed: {error:?}"))?;
        expect_eq("trace Meter receipt count", &receipt.len(), &1usize)?;
        expect_eq("trace Meter receipt table", &receipt[0].table, &table)?;
        expect_eq("trace Meter receipt key", &receipt[0].key, &key)?;
        expect_eq("trace Meter receipt version",
                  &receipt[0].version,
                  &Version::Upsert(transaction_uid))?;
        drop(transaction);

        let (committed_value, committed_version) = fixture
            .db
            .query_with_version(table.clone(), key)
            .await
            .map_err(|error| format!("querying trace Meter committed value failed: {error:?}"))?;
        expect_binary("trace Meter committed value",
                      committed_value.as_ref(),
                      Some(&value))?;
        expect_eq("trace Meter committed version",
                  &committed_version,
                  &receipt[0].version)?;
        let expected_table_cache_size = fixture
            .db
            .table_cache_size(&table)
            .await
            .ok_or_else(|| "trace Meter Memory table disappeared".to_owned())?;
        if expected_table_cache_size == 0 {
            return Err("trace Meter Memory cache unexpectedly reports zero bytes".to_owned());
        }

        wait_for_trace_export(&rt,
                              &provider,
                              &exporter,
                              expected_table_cache_size,
                              TRACE_DEADLINE).await
    })
    .unwrap_or_else(|error| panic!("global Meter initialization contract failed: {error}"));
}

async fn wait_for_trace_export(
    rt: &pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
    provider: &SdkMeterProvider,
    exporter: &InMemoryMetricExporter,
    expected_table_cache_size: u64,
    timeout: Duration,
) -> TestResult<()> {
    let expected_names = BTreeSet::from([
        TABLE_CACHE_SIZE_METRIC,
        KEY_VERSION_RECORD_COUNT_METRIC,
        KEY_VERSION_ESTIMATED_MEMORY_METRIC,
        KEY_VERSION_QUERY_CALLS_METRIC,
        KEY_VERSION_2PC_CALLS_METRIC,
        TRANSACTION_LIFECYCLE_METRIC,
    ]);
    let deadline = Instant::now() + timeout;
    loop {
        provider
            .force_flush()
            .map_err(|error| format!("forcing trace Meter export failed: {error:?}"))?;
        let metrics = exporter
            .get_finished_metrics()
            .map_err(|error| format!("reading trace Meter export failed: {error:?}"))?;
        let names = metric_names_in_scope(&metrics, METER_SCOPE);
        let has_loop_log = captured_loop_log();
        if names == expected_names && has_loop_log {
            expect_eq("exported table cache size",
                      &gauge_value(&metrics,
                                   METER_SCOPE,
                                   TABLE_CACHE_SIZE_METRIC,
                                   &[("table", MEMORY_TABLE)]),
                      &expected_table_cache_size)?;
            expect_eq("exported version record count",
                      &gauge_value(&metrics,
                                   METER_SCOPE,
                                   KEY_VERSION_RECORD_COUNT_METRIC,
                                   &[("table", MEMORY_TABLE)]),
                      &1u64)?;
            if gauge_value(&metrics,
                           METER_SCOPE,
                           KEY_VERSION_ESTIMATED_MEMORY_METRIC,
                           &[("table", MEMORY_TABLE)]) == 0 {
                return Err("exported version memory estimate is zero".to_owned());
            }
            expect_eq("exported query success count",
                      &counter_value(&metrics,
                                     METER_SCOPE,
                                     KEY_VERSION_QUERY_CALLS_METRIC,
                                     &[("result", "success")]),
                      &2u64)?;
            expect_eq("exported prepare success count",
                      &counter_value(&metrics,
                                     METER_SCOPE,
                                     KEY_VERSION_2PC_CALLS_METRIC,
                                     &[("phase", "prepare"), ("result", "success")]),
                      &1u64)?;
            expect_eq("exported commit success count",
                      &counter_value(&metrics,
                                     METER_SCOPE,
                                     KEY_VERSION_2PC_CALLS_METRIC,
                                     &[("phase", "commit"), ("result", "success")]),
                      &1u64)?;
            expect_eq("exported transaction created count",
                      &counter_value(&metrics,
                                     METER_SCOPE,
                                     TRANSACTION_LIFECYCLE_METRIC,
                                     &[("event", "created")]),
                      &3u64)?;
            expect_eq("exported transaction closed count",
                      &counter_value(&metrics,
                                     METER_SCOPE,
                                     TRANSACTION_LIFECYCLE_METRIC,
                                     &[("event", "closed")]),
                      &3u64)?;
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "trace loop did not export the frozen contract within {timeout:?}: scope={METER_SCOPE:?}, expected_names={expected_names:?}, observed_names={names:?}, loop_info={has_loop_log}, logs={:?}",
                captured_logs(),
            ));
        }
        rt.timeout(100).await;
    }
}

fn metric_names_in_scope<'a>(metrics: &'a [ResourceMetrics], scope: &str) -> BTreeSet<&'a str> {
    metrics
        .iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .filter(|scope_metrics| scope_metrics.scope().name() == scope)
        .flat_map(|scope_metrics| scope_metrics.metrics())
        .map(Metric::name)
        .collect()
}

fn find_metric<'a>(metrics: &'a [ResourceMetrics], scope: &str, name: &str) -> &'a Metric {
    metrics
        .iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .filter(|scope_metrics| scope_metrics.scope().name() == scope)
        .flat_map(|scope_metrics| scope_metrics.metrics())
        .find(|metric| metric.name() == name)
        .unwrap_or_else(|| panic!("scope {scope:?} did not export metric {name:?}"))
}

fn gauge_value(
    metrics: &[ResourceMetrics],
    scope: &str,
    name: &str,
    expected_attributes: &[(&str, &str)],
) -> u64 {
    match find_metric(metrics, scope, name).data() {
        AggregatedMetrics::U64(MetricData::Gauge(gauge)) => gauge
            .data_points()
            .find(|point| attributes_match(point.attributes(), expected_attributes))
            .unwrap_or_else(|| panic!("Gauge {name:?} has no matching data point"))
            .value(),
        data => panic!("metric {name:?} is not a u64 Gauge: {data:?}"),
    }
}

fn counter_value(
    metrics: &[ResourceMetrics],
    scope: &str,
    name: &str,
    expected_attributes: &[(&str, &str)],
) -> u64 {
    match find_metric(metrics, scope, name).data() {
        AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
            .data_points()
            .find(|point| attributes_match(point.attributes(), expected_attributes))
            .unwrap_or_else(|| panic!("Counter {name:?} has no matching data point"))
            .value(),
        data => panic!("metric {name:?} is not a u64 Counter: {data:?}"),
    }
}

fn attributes_match<'a>(
    attributes: impl Iterator<Item = &'a KeyValue>,
    expected: &[(&str, &str)],
) -> bool {
    let attributes = attributes.collect::<Vec<_>>();
    attributes.len() == expected.len()
        && expected.iter().all(|(key, value)| {
            attributes.iter().any(|attribute| {
                attribute.key.as_str() == *key && attribute.value.as_str() == *value
            })
        })
}

fn captured_loop_log() -> bool {
    CAPTURED_LOGS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .any(|message| message.contains("Loop tracing succeeded, interval"))
}

fn captured_logs() -> Vec<String> {
    CAPTURED_LOGS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

struct CapturingLogger;

impl Log for CapturingLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            CAPTURED_LOGS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(format!("{} {}", record.target(), record.args()));
        }
    }

    fn flush(&self) {}
}
