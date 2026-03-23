#![feature(stmt_expr_attributes)]
#![feature(ptr_metadata)]

use std::collections::{HashMap, BTreeMap};
use std::fmt::Write as FmtWrite;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::bounded;
use futures::stream::StreamExt;
use pi_async_rt::{prelude::AsyncRuntime, rt::{startup_global_time_loop, multi_thread::MultiTaskRuntimeBuilder}};
use pi_async_transaction::manager_2pc::Transaction2PcManager;
use pi_atom::Atom;
use pi_bon::{Decode, Encode, ReadBonErr, ReadBuffer, WriteBuffer};
use pi_db::{
    db::{DBStartupRepairMode, KVDBManager, KVDBManagerBuilder},
    tables::TableKV,
    Binary, KVDBTableType, KVTableMeta,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::{
    commit_logger::CommitLoggerBuilder,
    log_store::log_file::{LogFile, LogMethod, PairLoader},
};
use pi_time::run_nanos;
use redb::{Builder, ReadableTable, TableDefinition};

const BENCH_LOG_ORD_TABLE: &str = "repair_log_ord";
const BENCH_BTREE_TABLE: &str = "repair_btree";
const BENCH_PAYLOAD_TABLE: &str = "repair_payload";
const BENCH_TMP_ROOT: &str = "./tmp_quick_repair/benchmarks";
const BENCH_CHILD_MODE_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_CHILD_MODE";
const BENCH_CHILD_ROOT_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_CHILD_ROOT";
const BENCH_FILES_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_FILES";
const BENCH_TX_PER_FILE_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_TX_PER_FILE";
const BENCH_KEYS_PER_TX_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_KEYS_PER_TX";
const BENCH_PAYLOAD_BYTES_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_PAYLOAD_BYTES";
const BENCH_LOG_FILE_LIMIT_BYTES_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_LOG_FILE_LIMIT_BYTES";
const BENCH_ITERATIONS_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_ITERATIONS";
const BENCH_DEPTH_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_DEPTH";
const BENCH_POLL_INTERVAL_MS_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_POLL_INTERVAL_MS";
const BENCH_STARTUP_TIMEOUT_SECS_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_STARTUP_TIMEOUT_SECS";
const BENCH_DURABLE_TIMEOUT_SECS_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_DURABLE_TIMEOUT_SECS";
const BENCH_STARTUP_TARGET_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_STARTUP_TARGET";
const BENCH_DURABLE_TARGET_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_DURABLE_TARGET";
const BENCH_ENFORCE_TARGETS_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_ENFORCE_TARGETS";
const BENCH_ENABLE_ACCELERATED_REPAIR_ENV: &str = "PI_DB_QUICK_REPAIR_BENCH_ENABLE_ACCELERATED_REPAIR";
const BENCH_BTREE_DEF: TableDefinition<Binary, Binary> = TableDefinition::new("$default");

static QUICK_REPAIR_BENCH_GUARD: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, PartialEq, Eq)]
struct QuickRepairSnapshot {
    tables:      Vec<String>,
    log_ord:     BTreeMap<usize, usize>,
    btree:       BTreeMap<usize, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QuickRepairDiskState {
    meta_tables: Vec<String>,
    log_ord:     BTreeMap<usize, usize>,
    btree:       BTreeMap<usize, usize>,
}

#[derive(Debug, Clone)]
struct RepairBenchmarkProfile {
    name:                     &'static str,
    commit_log_files:         usize,
    tx_per_file:              usize,
    keys_per_tx:              usize,
    payload_bytes:            usize,
    commit_log_file_limit_bytes: u64,
    iterations:               usize,
    quick_depth:              usize,
    poll_interval_ms:         u64,
    startup_timeout_secs:     u64,
    durable_timeout_secs:     u64,
    startup_speedup_target:   f64,
    durable_speedup_target:   f64,
}

#[derive(Debug, Clone)]
struct CommitLogStats {
    physical_file_count: usize,
    active_file_count:   usize,
    total_bytes:         u64,
    min_bytes:           u64,
    max_bytes:           u64,
    file_sizes:          Vec<u64>,
}

#[derive(Debug, Clone)]
struct BenchmarkRun {
    mode:                 String,
    iteration:            usize,
    startup_elapsed_ms:   u128,
    durable_elapsed_ms:   u128,
    durable_wait_ms:      u128,
    durable_completed:    bool,
    immediate_disk_match: bool,
    startup_snapshot:     QuickRepairSnapshot,
    final_disk_state:     QuickRepairDiskState,
}

#[derive(Debug, Clone)]
enum BenchmarkMode {
    TryRepair,
    TryQuickRepair {
        depth: usize,
    },
}

impl BenchmarkMode {
    fn label(&self) -> String {
        match self {
            BenchmarkMode::TryRepair => "try_repair".to_string(),
            BenchmarkMode::TryQuickRepair { depth } => format!("try_quick_repair_depth_{}", depth),
        }
    }

    fn repair_mode(&self) -> DBStartupRepairMode {
        match self {
            BenchmarkMode::TryRepair => DBStartupRepairMode::TryRepair,
            BenchmarkMode::TryQuickRepair { .. } => DBStartupRepairMode::TryQuickRepair,
        }
    }

    fn quick_depth(&self) -> Option<usize> {
        match self {
            BenchmarkMode::TryRepair => None,
            BenchmarkMode::TryQuickRepair { depth } => Some(*depth),
        }
    }
}

#[derive(Debug, Clone)]
struct BenchmarkSummary {
    mode:               String,
    avg_startup_ms:     f64,
    min_startup_ms:     u128,
    max_startup_ms:     u128,
    avg_durable_ms:     f64,
    min_durable_ms:     u128,
    max_durable_ms:     u128,
    avg_wait_ms:        f64,
    durable_completed_runs: usize,
    durable_timeout_runs:   usize,
    immediate_match_ok: bool,
}

#[test]
#[ignore]
fn prepare_large_multi_file_benchmark_source_fixture() {
    let _guard = QUICK_REPAIR_BENCH_GUARD.lock().unwrap();
    let profile = bench_profile_from_env(default_large_multi_file_profile());

    if let Some(root) = bench_child_root("bench_prepare_large_multi_file_source_fixture") {
        generate_large_multi_file_benchmark_fixture(&root, &profile);
        return;
    }

    let bench_root = bench_tmp_dir("repair_vs_quick_large_multi_file");
    let source_root = bench_root.join("source");
    cleanup_benchmark_workspace(&bench_root);
    spawn_benchmark_fixture("prepare_large_multi_file_benchmark_source_fixture",
                            "bench_prepare_large_multi_file_source_fixture",
                            &source_root,
                            &profile);

    let commit_log_stats = collect_commit_log_stats(&source_root);
    println!("source fixture 准备完成: physical_files={}, active_files={}, total_bytes={}, min_bytes={}, max_bytes={}, file_sizes={:?}",
             commit_log_stats.physical_file_count,
             commit_log_stats.active_file_count,
             commit_log_stats.total_bytes,
             commit_log_stats.min_bytes,
             commit_log_stats.max_bytes,
             commit_log_stats.file_sizes);
}

#[test]
#[ignore]
fn benchmark_try_quick_repair_vs_try_repair_large_multi_file() {
    let _guard = QUICK_REPAIR_BENCH_GUARD.lock().unwrap();
    let profile = bench_profile_from_env(default_large_multi_file_profile());

    if let Some(root) = bench_child_root("bench_large_multi_file_fixture") {
        generate_large_multi_file_benchmark_fixture(&root, &profile);
        return;
    }

    let bench_root = bench_tmp_dir("repair_vs_quick_large_multi_file");
    cleanup_benchmark_workspace(&bench_root);
    let source_root = bench_root.join("source");
    spawn_benchmark_fixture("benchmark_try_quick_repair_vs_try_repair_large_multi_file",
                            "bench_large_multi_file_fixture",
                            &source_root,
                            &profile);

    let commit_log_stats = collect_commit_log_stats(&source_root);
    assert!(commit_log_stats.physical_file_count > 1,
            "benchmark fixture must contain more than one physical commit log file, root: {:?}",
            source_root);

    let reference_root = bench_root.join("reference_quick");
    remove_dir_if_exists(&reference_root);
    copy_dir_all(&source_root, &reference_root);
    let activated_reference_logs = activate_contiguous_commit_logs_for_repair(&reference_root);
    println!("benchmark reference fixture 已激活连续未确认 commit log 文件数={}", activated_reference_logs);
    let (_, reference_snapshot, reference_disk_state) = startup_db_and_collect_state(&reference_root,
                                                                                     DBStartupRepairMode::TryQuickRepair,
                                                                                     Some(profile.quick_depth),
                                                                                     Duration::from_secs(profile.startup_timeout_secs),
                                                                                     profile.commit_log_file_limit_bytes);

    let try_repair_runs = run_repair_benchmark_iterations(&bench_root,
                                                          &source_root,
                                                          &profile,
                                                          BenchmarkMode::TryRepair,
                                                          &reference_snapshot,
                                                          &reference_disk_state);
    let try_quick_repair_runs = run_repair_benchmark_iterations(&bench_root,
                                                                &source_root,
                                                                &profile,
                                                                BenchmarkMode::TryQuickRepair {
                                                                    depth: profile.quick_depth,
                                                                },
                                                                &reference_snapshot,
                                                                &reference_disk_state);

    let try_repair_summary = summarize_runs("try_repair", &try_repair_runs);
    let try_quick_summary = summarize_runs(&format!("try_quick_repair_depth_{}", profile.quick_depth),
                                           &try_quick_repair_runs);
    assert_benchmark_runs_consistent("try_repair",
                                     &try_repair_runs,
                                     &format!("try_quick_repair_depth_{}", profile.quick_depth),
                                     &try_quick_repair_runs);
    let startup_speedup = try_repair_summary.avg_startup_ms / try_quick_summary.avg_startup_ms;
    let durable_speedup = try_repair_summary.avg_durable_ms / try_quick_summary.avg_durable_ms;

    let report = build_repair_vs_quick_report(&profile,
                                              &commit_log_stats,
                                              &try_repair_runs,
                                              &try_quick_repair_runs,
                                              &try_repair_summary,
                                              &try_quick_summary,
                                              startup_speedup,
                                              durable_speedup);
    persist_report(&bench_root.join("reports"),
                   "repair_vs_quick_large_multi_file",
                   &profile,
                   &report,
                   &try_repair_runs,
                   &try_quick_repair_runs);
    cleanup_benchmark_workspace(&bench_root);

    println!("{}", report);

    if read_env_bool(BENCH_ENFORCE_TARGETS_ENV, false) {
        assert!(startup_speedup >= profile.startup_speedup_target,
                "expected startup speedup to reach target, target: {:.3}x, actual: {:.3}x",
                profile.startup_speedup_target,
                startup_speedup);
        assert!(durable_speedup >= profile.durable_speedup_target
                    || (try_quick_summary.durable_timeout_runs == 0 && try_repair_summary.durable_timeout_runs > 0),
                "expected durable speedup to reach target, target: {:.3}x, actual: {:.3}x, try_repair_timeouts: {}, try_quick_timeouts: {}",
                profile.durable_speedup_target,
                durable_speedup,
                try_repair_summary.durable_timeout_runs,
                try_quick_summary.durable_timeout_runs);
    }
}

#[test]
#[ignore]
fn benchmark_try_quick_repair_vs_try_repair_existing_source() {
    let _guard = QUICK_REPAIR_BENCH_GUARD.lock().unwrap();
    let profile = bench_profile_from_env(default_large_multi_file_profile());
    let bench_root = bench_tmp_dir("repair_vs_quick_large_multi_file");
    let source_root = bench_root.join("source");

    assert!(source_root.exists(),
            "benchmark existing source fixture not found, root: {:?}",
            source_root);

    cleanup_benchmark_workspace(&bench_root);

    let commit_log_stats = collect_commit_log_stats(&source_root);
    assert!(commit_log_stats.physical_file_count > 1,
            "benchmark existing source fixture must contain more than one physical commit log file, root: {:?}",
            source_root);

    let reference_root = bench_root.join("reference_quick");
    remove_dir_if_exists(&reference_root);
    copy_dir_all(&source_root, &reference_root);
    let activated_reference_logs = activate_contiguous_commit_logs_for_repair(&reference_root);
    println!("benchmark existing source reference 已激活连续未确认 commit log 文件数={}", activated_reference_logs);
    let (_, reference_snapshot, reference_disk_state) = startup_db_and_collect_state(&reference_root,
                                                                                     DBStartupRepairMode::TryQuickRepair,
                                                                                     Some(profile.quick_depth),
                                                                                     Duration::from_secs(profile.startup_timeout_secs),
                                                                                     profile.commit_log_file_limit_bytes);

    let try_repair_runs = run_repair_benchmark_iterations(&bench_root,
                                                          &source_root,
                                                          &profile,
                                                          BenchmarkMode::TryRepair,
                                                          &reference_snapshot,
                                                          &reference_disk_state);
    let try_quick_repair_runs = run_repair_benchmark_iterations(&bench_root,
                                                                &source_root,
                                                                &profile,
                                                                BenchmarkMode::TryQuickRepair {
                                                                    depth: profile.quick_depth,
                                                                },
                                                                &reference_snapshot,
                                                                &reference_disk_state);

    let try_repair_summary = summarize_runs("try_repair", &try_repair_runs);
    let try_quick_summary = summarize_runs(&format!("try_quick_repair_depth_{}", profile.quick_depth),
                                           &try_quick_repair_runs);
    assert_benchmark_runs_consistent("try_repair",
                                     &try_repair_runs,
                                     &format!("try_quick_repair_depth_{}", profile.quick_depth),
                                     &try_quick_repair_runs);
    let startup_speedup = try_repair_summary.avg_startup_ms / try_quick_summary.avg_startup_ms;
    let durable_speedup = try_repair_summary.avg_durable_ms / try_quick_summary.avg_durable_ms;

    let report = build_repair_vs_quick_report(&profile,
                                              &commit_log_stats,
                                              &try_repair_runs,
                                              &try_quick_repair_runs,
                                              &try_repair_summary,
                                              &try_quick_summary,
                                              startup_speedup,
                                              durable_speedup);
    persist_report(&bench_root.join("reports"),
                   "repair_vs_quick_large_multi_file_existing_source",
                   &profile,
                   &report,
                   &try_repair_runs,
                   &try_quick_repair_runs);

    println!("{}", report);

    if read_env_bool(BENCH_ENFORCE_TARGETS_ENV, false) {
        assert!(startup_speedup >= profile.startup_speedup_target,
                "expected startup speedup to reach target, target: {:.3}x, actual: {:.3}x",
                profile.startup_speedup_target,
                startup_speedup);
        assert!(durable_speedup >= profile.durable_speedup_target
                    || (try_quick_summary.durable_timeout_runs == 0 && try_repair_summary.durable_timeout_runs > 0),
                "expected durable speedup to reach target, target: {:.3}x, actual: {:.3}x, try_repair_timeouts: {}, try_quick_timeouts: {}",
                profile.durable_speedup_target,
                durable_speedup,
                try_repair_summary.durable_timeout_runs,
                try_quick_summary.durable_timeout_runs);
    }
}

#[test]
#[ignore]
fn benchmark_try_quick_repair_depth_two_vs_three_large_multi_file() {
    let _guard = QUICK_REPAIR_BENCH_GUARD.lock().unwrap();
    let profile = bench_profile_from_env(default_large_multi_file_profile());

    if let Some(root) = bench_child_root("bench_large_multi_file_depth_fixture") {
        generate_large_multi_file_benchmark_fixture(&root, &profile);
        return;
    }

    let bench_root = bench_tmp_dir("quick_depth_large_multi_file");
    cleanup_benchmark_workspace(&bench_root);
    let source_root = bench_root.join("source");
    spawn_benchmark_fixture("benchmark_try_quick_repair_depth_two_vs_three_large_multi_file",
                            "bench_large_multi_file_depth_fixture",
                            &source_root,
                            &profile);

    let commit_log_stats = collect_commit_log_stats(&source_root);
    assert!(commit_log_stats.physical_file_count > 1,
            "benchmark fixture must contain more than one physical commit log file, root: {:?}",
            source_root);

    let reference_root = bench_root.join("reference_quick_depth_two");
    remove_dir_if_exists(&reference_root);
    copy_dir_all(&source_root, &reference_root);
    let activated_reference_logs = activate_contiguous_commit_logs_for_repair(&reference_root);
    println!("benchmark depth reference fixture 已激活连续未确认 commit log 文件数={}", activated_reference_logs);
    let (_, reference_snapshot, reference_disk_state) = startup_db_and_collect_state(&reference_root,
                                                                                     DBStartupRepairMode::TryQuickRepair,
                                                                                     Some(2),
                                                                                     Duration::from_secs(profile.startup_timeout_secs),
                                                                                     profile.commit_log_file_limit_bytes);

    let depth_two_runs = run_repair_benchmark_iterations(&bench_root,
                                                         &source_root,
                                                         &profile,
                                                         BenchmarkMode::TryQuickRepair { depth: 2 },
                                                         &reference_snapshot,
                                                         &reference_disk_state);
    let depth_three_runs = run_repair_benchmark_iterations(&bench_root,
                                                           &source_root,
                                                           &profile,
                                                           BenchmarkMode::TryQuickRepair { depth: 3 },
                                                           &reference_snapshot,
                                                           &reference_disk_state);

    let depth_two_summary = summarize_runs("try_quick_repair_depth_2", &depth_two_runs);
    let depth_three_summary = summarize_runs("try_quick_repair_depth_3", &depth_three_runs);
    assert_benchmark_runs_consistent("try_quick_repair_depth_2",
                                     &depth_two_runs,
                                     "try_quick_repair_depth_3",
                                     &depth_three_runs);
    let startup_ratio = depth_two_summary.avg_startup_ms / depth_three_summary.avg_startup_ms;
    let durable_ratio = depth_two_summary.avg_durable_ms / depth_three_summary.avg_durable_ms;

    let report = build_quick_depth_report(&profile,
                                          &commit_log_stats,
                                          &depth_two_runs,
                                          &depth_three_runs,
                                          &depth_two_summary,
                                          &depth_three_summary,
                                          startup_ratio,
                                          durable_ratio);
    persist_report(&bench_root.join("reports"),
                   "quick_depth_large_multi_file",
                   &profile,
                   &report,
                   &depth_two_runs,
                   &depth_three_runs);
    cleanup_benchmark_workspace(&bench_root);

    println!("{}", report);
}

#[test]
#[ignore]
fn debug_try_quick_repair_on_existing_benchmark_source() {
    let _guard = QUICK_REPAIR_BENCH_GUARD.lock().unwrap();
    let profile = bench_profile_from_env(default_large_multi_file_profile());
    let source_root = bench_tmp_dir("repair_vs_quick_large_multi_file").join("source");
    let run_root = bench_tmp_dir("repair_vs_quick_large_multi_file")
        .join("debug_try_quick_repair_existing_source");

    assert!(source_root.exists(),
            "debug quick repair source fixture not found, root: {:?}",
            source_root);

    remove_dir_if_exists(&run_root);
    copy_dir_all(&source_root, &run_root);
    let activated_logs = activate_contiguous_commit_logs_for_repair(&run_root);
    println!("debug quick repair fixture 已激活连续未确认 commit log 文件数={}", activated_logs);

    let (startup_elapsed_ms, snapshot, disk_state) = startup_db_and_collect_state(&run_root,
                                                                                   DBStartupRepairMode::TryQuickRepair,
                                                                                   Some(profile.quick_depth),
                                                                                   Duration::from_secs(profile.startup_timeout_secs),
                                                                                   profile.commit_log_file_limit_bytes);

    println!("debug quick repair existing source finished: startup_elapsed_ms={}, tables={:?}, meta_tables={:?}, log_ord_keys={}, btree_keys={}",
             startup_elapsed_ms,
             snapshot.tables,
             disk_state.meta_tables,
             snapshot.log_ord.len(),
             snapshot.btree.len());
}

#[test]
#[ignore]
fn debug_try_repair_on_existing_benchmark_source() {
    let _guard = QUICK_REPAIR_BENCH_GUARD.lock().unwrap();
    let profile = bench_profile_from_env(default_large_multi_file_profile());
    let source_root = bench_tmp_dir("repair_vs_quick_large_multi_file").join("source");
    let run_root = bench_tmp_dir("repair_vs_quick_large_multi_file")
        .join("debug_try_repair_existing_source");

    assert!(source_root.exists(),
            "debug repair source fixture not found, root: {:?}",
            source_root);

    remove_dir_if_exists(&run_root);
    copy_dir_all(&source_root, &run_root);
    let activated_logs = activate_contiguous_commit_logs_for_repair(&run_root);
    println!("debug repair fixture 已激活连续未确认 commit log 文件数={}", activated_logs);

    let (startup_elapsed_ms, snapshot, disk_state) = startup_db_and_collect_state(&run_root,
                                                                                   DBStartupRepairMode::TryRepair,
                                                                                   None,
                                                                                   Duration::from_secs(profile.startup_timeout_secs),
                                                                                   profile.commit_log_file_limit_bytes);

    println!("debug repair existing source finished: startup_elapsed_ms={}, tables={:?}, meta_tables={:?}, log_ord_keys={}, btree_keys={}",
             startup_elapsed_ms,
             snapshot.tables,
             disk_state.meta_tables,
             snapshot.log_ord.len(),
             snapshot.btree.len());
}

fn default_large_multi_file_profile() -> RepairBenchmarkProfile {
    RepairBenchmarkProfile {
        name: "production_scale_btree_dominant_multi_file_repair",
        commit_log_files: 3,
        tx_per_file: 56_000,
        keys_per_tx: 32,
        payload_bytes: 64,
        commit_log_file_limit_bytes: 64 * 1024 * 1024,
        iterations: 1,
        quick_depth: 2,
        poll_interval_ms: 1_000,
        startup_timeout_secs: 21_600,
        durable_timeout_secs: 21_600,
        startup_speedup_target: 1.10,
        durable_speedup_target: 1.20,
    }
}

fn bench_profile_from_env(mut profile: RepairBenchmarkProfile) -> RepairBenchmarkProfile {
    profile.commit_log_files = read_env_usize(BENCH_FILES_ENV, profile.commit_log_files);
    profile.tx_per_file = read_env_usize(BENCH_TX_PER_FILE_ENV, profile.tx_per_file);
    profile.keys_per_tx = read_env_usize(BENCH_KEYS_PER_TX_ENV, profile.keys_per_tx);
    profile.payload_bytes = read_env_usize(BENCH_PAYLOAD_BYTES_ENV, profile.payload_bytes);
    profile.commit_log_file_limit_bytes = read_env_u64(BENCH_LOG_FILE_LIMIT_BYTES_ENV, profile.commit_log_file_limit_bytes);
    profile.iterations = read_env_usize(BENCH_ITERATIONS_ENV, profile.iterations);
    profile.quick_depth = read_env_usize(BENCH_DEPTH_ENV, profile.quick_depth);
    profile.poll_interval_ms = read_env_u64(BENCH_POLL_INTERVAL_MS_ENV, profile.poll_interval_ms);
    profile.startup_timeout_secs = read_env_u64(BENCH_STARTUP_TIMEOUT_SECS_ENV, profile.startup_timeout_secs);
    profile.durable_timeout_secs = read_env_u64(BENCH_DURABLE_TIMEOUT_SECS_ENV, profile.durable_timeout_secs);
    profile.startup_speedup_target = read_env_f64(BENCH_STARTUP_TARGET_ENV, profile.startup_speedup_target);
    profile.durable_speedup_target = read_env_f64(BENCH_DURABLE_TARGET_ENV, profile.durable_speedup_target);
    profile
}

fn bench_child_root(expected_mode: &str) -> Option<PathBuf> {
    match std::env::var(BENCH_CHILD_MODE_ENV) {
        Err(_) => None,
        Ok(mode) => {
            if mode == expected_mode {
                Some(PathBuf::from(std::env::var(BENCH_CHILD_ROOT_ENV).unwrap()))
            } else {
                None
            }
        },
    }
}

fn bench_tmp_dir(name: &str) -> PathBuf {
    PathBuf::from(BENCH_TMP_ROOT).join(name)
}

fn cleanup_benchmark_workspace(bench_root: &Path) {
    if !bench_root.exists() {
        return;
    }

    let Ok(entries) = std::fs::read_dir(bench_root) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let file_name = path.file_name().and_then(|name| name.to_str());
        if file_name == Some("reports") || file_name == Some("source") {
            continue;
        }

        if path.is_dir() {
            remove_dir_if_exists(&path);
        } else {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn spawn_benchmark_fixture(test_name: &str,
                           mode: &str,
                           root: &Path,
                           profile: &RepairBenchmarkProfile) {
    remove_dir_if_exists(root);
    if let Some(parent) = root.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(test_name)
        .arg("--ignored")
        .arg("--nocapture")
        .env(BENCH_CHILD_MODE_ENV, mode)
        .env(BENCH_CHILD_ROOT_ENV, root.to_str().unwrap())
        .env(BENCH_FILES_ENV, profile.commit_log_files.to_string())
        .env(BENCH_TX_PER_FILE_ENV, profile.tx_per_file.to_string())
        .env(BENCH_KEYS_PER_TX_ENV, profile.keys_per_tx.to_string())
        .env(BENCH_PAYLOAD_BYTES_ENV, profile.payload_bytes.to_string())
        .env(BENCH_LOG_FILE_LIMIT_BYTES_ENV, profile.commit_log_file_limit_bytes.to_string())
        .env(BENCH_ITERATIONS_ENV, profile.iterations.to_string())
        .env(BENCH_DEPTH_ENV, profile.quick_depth.to_string())
        .env(BENCH_POLL_INTERVAL_MS_ENV, profile.poll_interval_ms.to_string())
        .env(BENCH_STARTUP_TIMEOUT_SECS_ENV, profile.startup_timeout_secs.to_string())
        .env(BENCH_DURABLE_TIMEOUT_SECS_ENV, profile.durable_timeout_secs.to_string())
        .env(BENCH_STARTUP_TARGET_ENV, profile.startup_speedup_target.to_string())
        .env(BENCH_DURABLE_TARGET_ENV, profile.durable_speedup_target.to_string())
        .status()
        .unwrap();

    assert!(status.success(),
            "spawn benchmark fixture failed, test: {:?}, mode: {:?}, root: {:?}, status: {:?}",
            test_name,
            mode,
            root,
            status);
}

fn generate_large_multi_file_benchmark_fixture(root: &Path,
                                               profile: &RepairBenchmarkProfile) {
    remove_dir_if_exists(root);
    std::fs::create_dir_all(root).unwrap();

    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();
    let root_copy = root.to_path_buf();
    let profile_copy = profile.clone();
    let (sender, receiver) = bounded(1);

    let _ = rt.spawn(async move {
        println!("开始生成 quick repair benchmark fixture: files={}, tx_per_file={}, keys_per_tx={}, payload_bytes={}, log_file_limit_bytes={}",
                 profile_copy.commit_log_files,
                 profile_copy.tx_per_file,
                 profile_copy.keys_per_tx,
                 profile_copy.payload_bytes,
                 profile_copy.commit_log_file_limit_bytes);
        let guid_gen = GuidGen::new(run_nanos(), 0);
        let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), root_copy.join(".commit_log"));
        let commit_logger = commit_logger_builder
            .log_file_limit(profile_copy.commit_log_file_limit_bytes)
            .build()
            .await
            .unwrap();

        let tr_mgr = Transaction2PcManager::new(rt_copy.clone(),
                                                guid_gen,
                                                commit_logger);

        let builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, root_copy.join("db"));
        let db = builder.startup_by_repair(true, DBStartupRepairMode::TryQuickRepair)
            .await
            .unwrap();

        let btree_table = Atom::from(BENCH_BTREE_TABLE);

        let tr = db.transaction(Atom::from("benchmark fixture create table"), true, 500, 500).unwrap();
        tr.create_table(btree_table.clone(),
                        KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                         true,
                                         EnumType::Usize,
                                         EnumType::Usize),
                        true).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        for file_index in 0..profile_copy.commit_log_files {
            for tx_index in 0..profile_copy.tx_per_file {
                let sequence = file_index * profile_copy.tx_per_file + tx_index;
                let key_base = sequence * profile_copy.keys_per_tx + 1;
                let tr = db.transaction(Atom::from(format!("benchmark fixture file {} tx {}", file_index, tx_index)),
                                        true,
                                        500,
                                        500).unwrap();
                // 这组正式 benchmark 现在刻意对齐生产侧“MetaTab + Btree 主导”的结构：
                // 1. 元信息表天然存在并参与修复；
                // 2. 主数据只落到 BtreeOrdTab；
                // 3. 不再用大型用户 LogOrdTab / LogWTab 扰动端到端结论。
                let mut upserts = Vec::with_capacity(profile_copy.keys_per_tx);
                for offset in 0..profile_copy.keys_per_tx {
                    let key = key_base + offset;
                    upserts.push(TableKV::new(btree_table.clone(),
                                              usize_to_binary(key),
                                              Some(usize_to_binary(2_000_000 + key))));
                }
                tr.upsert(upserts).await.unwrap();
                let output = tr.prepare_modified().await.unwrap();
                tr.commit_modified(output).await.unwrap();
            }

            println!("benchmark fixture 已完成 commit log 文件批次 {}/{}，累计事务数={}",
                     file_index + 1,
                     profile_copy.commit_log_files,
                     (file_index + 1) * profile_copy.tx_per_file);

            if file_index + 1 < profile_copy.commit_log_files {
                // 显式切 checkpoint，稳定得到多个物理 commit log 文件。
                db.append_new_commit_log().await.unwrap();
            }
        }

        println!("quick repair benchmark fixture 生成完成: files={}, total_tx={}",
                 profile_copy.commit_log_files,
                 profile_copy.commit_log_files * profile_copy.tx_per_file);

        let _ = sender.send(());
    });

    // 正式压力样本会生成大量 commit log 文件，不能沿用固定 180s 的短超时；
    // 这里复用 profile 中可配置的 startup_timeout_secs，避免 fixture 生成阶段先于基准本体超时。
    receiver.recv_timeout(Duration::from_secs(profile.startup_timeout_secs)).unwrap();
}

fn run_repair_benchmark_iterations(bench_root: &Path,
                                   source_root: &Path,
                                   profile: &RepairBenchmarkProfile,
                                   mode: BenchmarkMode,
                                   expected_snapshot: &QuickRepairSnapshot,
                                   expected_disk_state: &QuickRepairDiskState) -> Vec<BenchmarkRun> {
    let mut runs = Vec::with_capacity(profile.iterations);

    for iteration in 1..=profile.iterations {
        println!("开始执行 benchmark: mode={}, iteration={}/{}",
                 mode.label(),
                 iteration,
                 profile.iterations);
        let run_root = bench_root.join(format!("{}_iter_{:02}", mode.label(), iteration));
        remove_dir_if_exists(&run_root);
        copy_dir_all(source_root, &run_root);
        let activated_logs = activate_contiguous_commit_logs_for_repair(&run_root);
        println!("benchmark run fixture 已激活连续未确认 commit log 文件数: mode={}, iteration={}/{}, activated_logs={}",
                 mode.label(),
                 iteration,
                 profile.iterations,
                 activated_logs);

        let (startup_elapsed_ms, snapshot, disk_state) = startup_db_and_collect_state(&run_root,
                                                                                       mode.repair_mode(),
                                                                                       mode.quick_depth(),
                                                                                       Duration::from_secs(profile.startup_timeout_secs),
                                                                                       profile.commit_log_file_limit_bytes);

        assert_eq!(&snapshot, expected_snapshot,
                   "repair benchmark snapshot mismatch, mode: {:?}, iteration: {:?}",
                   mode.label(),
                   iteration);

        let immediate_disk_match = &disk_state == expected_disk_state;
        let (durable_wait_ms, durable_completed) = if immediate_disk_match {
            (0, true)
        } else {
            match wait_until_disk_state(&run_root,
                                        expected_disk_state,
                                        Duration::from_secs(profile.durable_timeout_secs),
                                        Duration::from_millis(profile.poll_interval_ms)) {
                Some(wait_ms) => (wait_ms, true),
                None => (Duration::from_secs(profile.durable_timeout_secs).as_millis(), false),
            }
        };
        let final_disk_state = load_benchmark_disk_state(&run_root);

        if durable_completed {
            assert_eq!(&final_disk_state, expected_disk_state,
                       "repair benchmark durable disk state mismatch, mode: {:?}, iteration: {:?}",
                       mode.label(),
                       iteration);
        }

        println!("benchmark 完成: mode={}, iteration={}/{}, startup_elapsed_ms={}, durable_elapsed_ms={}, durable_completed={}, immediate_disk_match={}",
                 mode.label(),
                 iteration,
                 profile.iterations,
                 startup_elapsed_ms,
                 startup_elapsed_ms + durable_wait_ms,
                 durable_completed,
                 immediate_disk_match);

        runs.push(BenchmarkRun {
            mode: mode.label(),
            iteration,
            startup_elapsed_ms,
            durable_elapsed_ms: startup_elapsed_ms + durable_wait_ms,
            durable_wait_ms,
            durable_completed,
            immediate_disk_match,
            startup_snapshot: snapshot,
            final_disk_state,
        });
    }

    runs
}

fn startup_db_and_collect_state(root: &Path,
                                repair_mode: DBStartupRepairMode,
                                quick_repair_file_pipeline_depth: Option<usize>,
                                startup_timeout: Duration,
                                commit_log_file_limit_bytes: u64)
    -> (u128, QuickRepairSnapshot, QuickRepairDiskState) {
    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();
    let root_copy = root.to_path_buf();
    let (sender, receiver) = bounded(1);

    let _ = rt.spawn(async move {
        let startup_begin = Instant::now();
        let db = startup_benchmark_db(rt_copy.clone(),
                                      root_copy.clone(),
                                      repair_mode,
                                      quick_repair_file_pipeline_depth,
                                      commit_log_file_limit_bytes).await;
        let startup_elapsed_ms = startup_begin.elapsed().as_millis();

        let mut tables = db.tables().await
            .into_iter()
            .map(|table| table.as_str().to_string())
            .collect::<Vec<_>>();
        tables.sort();

        let tr = db.transaction(Atom::from("benchmark snapshot"), false, 500, 500).unwrap();
        let snapshot = QuickRepairSnapshot {
            tables,
            log_ord: read_table_values(&tr, Atom::from(BENCH_LOG_ORD_TABLE)).await,
            btree: read_table_values(&tr, Atom::from(BENCH_BTREE_TABLE)).await,
        };

        drop(tr);
        drop(db);
        rt_copy.timeout(0).await;

        let _ = sender.send((startup_elapsed_ms, snapshot));
    });

    let (startup_elapsed_ms, snapshot) = receiver.recv_timeout(startup_timeout).unwrap();
    drop(rt);

    (startup_elapsed_ms, snapshot, load_benchmark_disk_state(root))
}

async fn startup_benchmark_db(rt_copy: pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
                              root_copy: PathBuf,
                              repair_mode: DBStartupRepairMode,
                              quick_repair_file_pipeline_depth: Option<usize>,
                              commit_log_file_limit_bytes: u64)
    -> KVDBManager<usize, pi_store::commit_logger::CommitLogger> {
    let guid_gen = GuidGen::new(run_nanos(), 0);
    let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), root_copy.join(".commit_log"));
    let commit_logger = commit_logger_builder
        .log_file_limit(commit_log_file_limit_bytes)
        .build()
        .await
        .unwrap();

    let tr_mgr = Transaction2PcManager::new(rt_copy.clone(),
                                            guid_gen,
                                            commit_logger);

    let builder = match quick_repair_file_pipeline_depth {
        Some(depth) => {
            KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, root_copy.join("db"))
                .quick_repair_file_pipeline_depth(depth)
        },
        None => {
            KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, root_copy.join("db"))
        },
    };

    let enable_accelerated_repair = read_env_bool(BENCH_ENABLE_ACCELERATED_REPAIR_ENV, true);
    println!("benchmark startup config: repair_mode={:?}, enable_accelerated_repair={}, quick_depth={:?}",
             repair_mode,
             enable_accelerated_repair,
             quick_repair_file_pipeline_depth);

    builder.startup_by_repair(enable_accelerated_repair, repair_mode).await.unwrap()
}

async fn read_table_values<C, Log>(tr: &pi_db::db::KVDBTransaction<C, Log>,
                                   table_name: Atom) -> BTreeMap<usize, usize>
    where C: Clone + Send + 'static,
          Log: pi_async_transaction::AsyncCommitLog<C = C, Cid = pi_guid::Guid>
{
    let mut values_map = BTreeMap::new();

    if let Some(mut values) = tr.values(table_name, None, false).await {
        while let Some((key, value)) = values.next().await {
            values_map.insert(binary_to_usize(&key).unwrap(),
                              binary_to_usize(&value).unwrap());
        }
    }

    values_map
}

fn wait_until_disk_state(root: &Path,
                         expected_disk_state: &QuickRepairDiskState,
                         timeout: Duration,
                         poll_interval: Duration) -> Option<u128> {
    let begin = Instant::now();
    loop {
        let current = load_benchmark_disk_state(root);
        if &current == expected_disk_state {
            return Some(begin.elapsed().as_millis());
        }
        if begin.elapsed() >= timeout {
            return None;
        }
        thread::sleep(poll_interval);
    }
}

fn collect_commit_log_stats(root: &Path) -> CommitLogStats {
    let commit_log_root = root.join(".commit_log");
    let mut active_file_sizes = Vec::new();
    let mut physical_file_sizes = std::fs::read_dir(commit_log_root).unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .filter_map(|path| {
            let file_name = path.file_name()?.to_str()?.to_string();
            let size = path.metadata().ok()?.len();
            if size == 0 {
                return None;
            }
            if !file_name.ends_with(".bak") {
                active_file_sizes.push(size);
            }
            Some(size)
        })
        .collect::<Vec<_>>();
    active_file_sizes.sort_unstable();
    physical_file_sizes.sort_unstable();

    let total_bytes = physical_file_sizes.iter().sum::<u64>();
    let min_bytes = physical_file_sizes.first().copied().unwrap_or(0);
    let max_bytes = physical_file_sizes.last().copied().unwrap_or(0);

    CommitLogStats {
        physical_file_count: physical_file_sizes.len(),
        active_file_count: active_file_sizes.len(),
        total_bytes,
        min_bytes,
        max_bytes,
        file_sizes: physical_file_sizes,
    }
}

fn summarize_runs(mode: &str,
                  runs: &[BenchmarkRun]) -> BenchmarkSummary {
    let startup_total = runs.iter().map(|run| run.startup_elapsed_ms as f64).sum::<f64>();
    let durable_total = runs.iter().map(|run| run.durable_elapsed_ms as f64).sum::<f64>();
    let wait_total = runs.iter().map(|run| run.durable_wait_ms as f64).sum::<f64>();

    BenchmarkSummary {
        mode: mode.to_string(),
        avg_startup_ms: startup_total / runs.len() as f64,
        min_startup_ms: runs.iter().map(|run| run.startup_elapsed_ms).min().unwrap_or(0),
        max_startup_ms: runs.iter().map(|run| run.startup_elapsed_ms).max().unwrap_or(0),
        avg_durable_ms: durable_total / runs.len() as f64,
        min_durable_ms: runs.iter().map(|run| run.durable_elapsed_ms).min().unwrap_or(0),
        max_durable_ms: runs.iter().map(|run| run.durable_elapsed_ms).max().unwrap_or(0),
        avg_wait_ms: wait_total / runs.len() as f64,
        durable_completed_runs: runs.iter().filter(|run| run.durable_completed).count(),
        durable_timeout_runs: runs.iter().filter(|run| !run.durable_completed).count(),
        immediate_match_ok: runs.iter().all(|run| run.immediate_disk_match),
    }
}

fn assert_benchmark_runs_consistent(left_label: &str,
                                    left_runs: &[BenchmarkRun],
                                    right_label: &str,
                                    right_runs: &[BenchmarkRun]) {
    assert_eq!(left_runs.len(), right_runs.len(),
               "benchmark run count mismatch, left: {:?}, right: {:?}",
               left_label,
               right_label);

    for (left, right) in left_runs.iter().zip(right_runs.iter()) {
        assert_eq!(left.iteration, right.iteration,
                   "benchmark iteration mismatch, left: {:?}, right: {:?}",
                   left_label,
                   right_label);
        assert_eq!(left.startup_snapshot, right.startup_snapshot,
                   "benchmark logical result mismatch, left: {:?}, right: {:?}, iteration: {:?}",
                   left_label,
                   right_label,
                   left.iteration);

        if left.durable_completed && right.durable_completed {
            assert_eq!(left.final_disk_state, right.final_disk_state,
                       "benchmark durable disk result mismatch, left: {:?}, right: {:?}, iteration: {:?}",
                       left_label,
                       right_label,
                       left.iteration);
        }
    }
}

fn build_repair_vs_quick_report(profile: &RepairBenchmarkProfile,
                                commit_log_stats: &CommitLogStats,
                                try_repair_runs: &[BenchmarkRun],
                                try_quick_runs: &[BenchmarkRun],
                                try_repair_summary: &BenchmarkSummary,
                                try_quick_summary: &BenchmarkSummary,
                                startup_speedup: f64,
                                durable_speedup: f64) -> String {
    let mut report = String::new();
    let _ = writeln!(&mut report, "# Quick Repair Performance Benchmark");
    let _ = writeln!(&mut report);
    let _ = writeln!(&mut report, "## Profile");
    let _ = writeln!(&mut report, "- name: `{}`", profile.name);
    let _ = writeln!(&mut report, "- commit_log_files: `{}`", profile.commit_log_files);
    let _ = writeln!(&mut report, "- tx_per_file: `{}`", profile.tx_per_file);
    let _ = writeln!(&mut report, "- keys_per_tx: `{}`", profile.keys_per_tx);
    let _ = writeln!(&mut report, "- payload_bytes: `{}`", profile.payload_bytes);
    let _ = writeln!(&mut report, "- commit_log_file_limit_bytes: `{}`", profile.commit_log_file_limit_bytes);
    let _ = writeln!(&mut report, "- iterations: `{}`", profile.iterations);
    let _ = writeln!(&mut report, "- quick_depth: `{}`", profile.quick_depth);
    let _ = writeln!(&mut report, "- startup_timeout_secs: `{}`", profile.startup_timeout_secs);
    let _ = writeln!(&mut report, "- durable_timeout_secs: `{}`", profile.durable_timeout_secs);
    let _ = writeln!(&mut report);
    let _ = writeln!(&mut report, "## Commit Log Stats");
    let _ = writeln!(&mut report, "- physical_file_count: `{}`", commit_log_stats.physical_file_count);
    let _ = writeln!(&mut report, "- active_file_count: `{}`", commit_log_stats.active_file_count);
    let _ = writeln!(&mut report, "- total_bytes: `{}`", commit_log_stats.total_bytes);
    let _ = writeln!(&mut report, "- min_file_bytes: `{}`", commit_log_stats.min_bytes);
    let _ = writeln!(&mut report, "- max_file_bytes: `{}`", commit_log_stats.max_bytes);
    let _ = writeln!(&mut report, "- file_sizes: `{:?}`", commit_log_stats.file_sizes);
    let _ = writeln!(&mut report);
    let _ = writeln!(&mut report, "## Summary");
    let _ = writeln!(&mut report, "| mode | avg_startup_ms | min_startup_ms | max_startup_ms | avg_durable_ms | avg_wait_ms | durable_completed_runs | durable_timeout_runs | immediate_disk_match |");
    let _ = writeln!(&mut report, "| --- | ---: | ---: | ---: | ---: | ---: | --- |");
    let _ = writeln!(&mut report, "| {} | {:.3} | {} | {} | {:.3} | {:.3} | {} | {} | {} |",
                     try_repair_summary.mode,
                     try_repair_summary.avg_startup_ms,
                     try_repair_summary.min_startup_ms,
                     try_repair_summary.max_startup_ms,
                     try_repair_summary.avg_durable_ms,
                     try_repair_summary.avg_wait_ms,
                     try_repair_summary.durable_completed_runs,
                     try_repair_summary.durable_timeout_runs,
                     try_repair_summary.immediate_match_ok);
    let _ = writeln!(&mut report, "| {} | {:.3} | {} | {} | {:.3} | {:.3} | {} | {} | {} |",
                     try_quick_summary.mode,
                     try_quick_summary.avg_startup_ms,
                     try_quick_summary.min_startup_ms,
                     try_quick_summary.max_startup_ms,
                     try_quick_summary.avg_durable_ms,
                     try_quick_summary.avg_wait_ms,
                     try_quick_summary.durable_completed_runs,
                     try_quick_summary.durable_timeout_runs,
                     try_quick_summary.immediate_match_ok);
    let _ = writeln!(&mut report);
    let _ = writeln!(&mut report, "## Evaluation");
    let _ = writeln!(&mut report, "- startup_speedup(try_repair / try_quick_repair): `{:.3}x`", startup_speedup);
    let _ = writeln!(&mut report, "- durable_speedup(try_repair / try_quick_repair): `{:.3}x`", durable_speedup);
    let _ = writeln!(&mut report, "- primary_metric: `avg_durable_ms`，表示从启动修复开始到目标持久化状态达成的端到端总耗时");
    let _ = writeln!(&mut report, "- startup_target: `{:.3}x`", profile.startup_speedup_target);
    let _ = writeln!(&mut report, "- durable_target: `{:.3}x`", profile.durable_speedup_target);
    let _ = writeln!(&mut report, "- startup_target_met: `{}`", startup_speedup >= profile.startup_speedup_target);
    let _ = writeln!(&mut report, "- durable_target_met: `{}`", durable_speedup >= profile.durable_speedup_target);
    let _ = writeln!(&mut report, "- try_repair_durable_timeouts: `{}`", try_repair_summary.durable_timeout_runs);
    let _ = writeln!(&mut report, "- try_quick_repair_durable_timeouts: `{}`", try_quick_summary.durable_timeout_runs);
    let _ = writeln!(&mut report, "- quick_immediate_disk_match_all: `{}`", try_quick_summary.immediate_match_ok);
    let _ = writeln!(&mut report, "- repair_immediate_disk_match_all: `{}`", try_repair_summary.immediate_match_ok);
    let _ = writeln!(&mut report, "- note: `avg_durable_ms` 为观测值；超时样本会按 `durable_timeout_secs` 上限计入，并单独记录 timeout 次数。");
    let _ = writeln!(&mut report);
    append_run_details(&mut report, "TryRepair Runs", try_repair_runs);
    append_run_details(&mut report, "TryQuickRepair Runs", try_quick_runs);
    report
}

fn build_quick_depth_report(profile: &RepairBenchmarkProfile,
                            commit_log_stats: &CommitLogStats,
                            depth_two_runs: &[BenchmarkRun],
                            depth_three_runs: &[BenchmarkRun],
                            depth_two_summary: &BenchmarkSummary,
                            depth_three_summary: &BenchmarkSummary,
                            startup_ratio: f64,
                            durable_ratio: f64) -> String {
    let mut report = String::new();
    let _ = writeln!(&mut report, "# Quick Repair Pipeline Depth Benchmark");
    let _ = writeln!(&mut report);
    let _ = writeln!(&mut report, "## Profile");
    let _ = writeln!(&mut report, "- name: `{}`", profile.name);
    let _ = writeln!(&mut report, "- commit_log_files: `{}`", profile.commit_log_files);
    let _ = writeln!(&mut report, "- tx_per_file: `{}`", profile.tx_per_file);
    let _ = writeln!(&mut report, "- keys_per_tx: `{}`", profile.keys_per_tx);
    let _ = writeln!(&mut report, "- payload_bytes: `{}`", profile.payload_bytes);
    let _ = writeln!(&mut report, "- commit_log_file_limit_bytes: `{}`", profile.commit_log_file_limit_bytes);
    let _ = writeln!(&mut report, "- iterations: `{}`", profile.iterations);
    let _ = writeln!(&mut report, "- startup_timeout_secs: `{}`", profile.startup_timeout_secs);
    let _ = writeln!(&mut report, "- durable_timeout_secs: `{}`", profile.durable_timeout_secs);
    let _ = writeln!(&mut report);
    let _ = writeln!(&mut report, "## Commit Log Stats");
    let _ = writeln!(&mut report, "- physical_file_count: `{}`", commit_log_stats.physical_file_count);
    let _ = writeln!(&mut report, "- active_file_count: `{}`", commit_log_stats.active_file_count);
    let _ = writeln!(&mut report, "- total_bytes: `{}`", commit_log_stats.total_bytes);
    let _ = writeln!(&mut report, "- file_sizes: `{:?}`", commit_log_stats.file_sizes);
    let _ = writeln!(&mut report);
    let _ = writeln!(&mut report, "## Summary");
    let _ = writeln!(&mut report, "| mode | avg_startup_ms | min_startup_ms | max_startup_ms | avg_durable_ms | avg_wait_ms | durable_completed_runs | durable_timeout_runs | immediate_disk_match |");
    let _ = writeln!(&mut report, "| --- | ---: | ---: | ---: | ---: | ---: | --- |");
    let _ = writeln!(&mut report, "| {} | {:.3} | {} | {} | {:.3} | {:.3} | {} | {} | {} |",
                     depth_two_summary.mode,
                     depth_two_summary.avg_startup_ms,
                     depth_two_summary.min_startup_ms,
                     depth_two_summary.max_startup_ms,
                     depth_two_summary.avg_durable_ms,
                     depth_two_summary.avg_wait_ms,
                     depth_two_summary.durable_completed_runs,
                     depth_two_summary.durable_timeout_runs,
                     depth_two_summary.immediate_match_ok);
    let _ = writeln!(&mut report, "| {} | {:.3} | {} | {} | {:.3} | {:.3} | {} | {} | {} |",
                     depth_three_summary.mode,
                     depth_three_summary.avg_startup_ms,
                     depth_three_summary.min_startup_ms,
                     depth_three_summary.max_startup_ms,
                     depth_three_summary.avg_durable_ms,
                     depth_three_summary.avg_wait_ms,
                     depth_three_summary.durable_completed_runs,
                     depth_three_summary.durable_timeout_runs,
                     depth_three_summary.immediate_match_ok);
    let _ = writeln!(&mut report);
    let _ = writeln!(&mut report, "## Evaluation");
    let _ = writeln!(&mut report, "- startup_ratio(depth2 / depth3): `{:.3}x`", startup_ratio);
    let _ = writeln!(&mut report, "- durable_ratio(depth2 / depth3): `{:.3}x`", durable_ratio);
    let _ = writeln!(&mut report, "- primary_metric: `avg_durable_ms`，表示从启动修复开始到目标持久化状态达成的端到端总耗时");
    let _ = writeln!(&mut report, "- note: 本报告只记录 `depth=2/3` 的吞吐差异，不把某一深度固定定义为必须更快。");
    let _ = writeln!(&mut report);
    append_run_details(&mut report, "Quick Depth 2 Runs", depth_two_runs);
    append_run_details(&mut report, "Quick Depth 3 Runs", depth_three_runs);
    report
}

fn append_run_details(report: &mut String,
                      title: &str,
                      runs: &[BenchmarkRun]) {
    let _ = writeln!(report, "## {}", title);
    let _ = writeln!(report, "| mode | iteration | startup_ms | durable_ms | wait_ms | durable_completed | immediate_disk_match |");
    let _ = writeln!(report, "| --- | ---: | ---: | ---: | ---: | --- | --- |");
    for run in runs {
        let _ = writeln!(report, "| {} | {} | {} | {} | {} | {} | {} |",
                         run.mode,
                         run.iteration,
                         run.startup_elapsed_ms,
                         run.durable_elapsed_ms,
                         run.durable_wait_ms,
                         run.durable_completed,
                         run.immediate_disk_match);
    }
    let _ = writeln!(report);
}

fn persist_report(report_root: &Path,
                  report_name: &str,
                  profile: &RepairBenchmarkProfile,
                  markdown: &str,
                  left_runs: &[BenchmarkRun],
                  right_runs: &[BenchmarkRun]) {
    std::fs::create_dir_all(report_root).unwrap();
    let latest_md = report_root.join(format!("{}.md", report_name));
    let latest_csv = report_root.join(format!("{}.csv", report_name));
    let archive_name = format!("{}__{}", report_name, report_archive_suffix(profile));
    let archive_md = report_root.join(format!("{}.md", archive_name));
    let archive_csv = report_root.join(format!("{}.csv", archive_name));
    std::fs::write(&latest_md, markdown).unwrap();
    std::fs::write(&archive_md, markdown).unwrap();

    let mut csv = String::from("mode,iteration,startup_elapsed_ms,durable_elapsed_ms,durable_wait_ms,durable_completed,immediate_disk_match\n");
    for run in left_runs.iter().chain(right_runs.iter()) {
        let _ = writeln!(&mut csv, "{},{},{},{},{},{},{}",
                         run.mode,
                         run.iteration,
                         run.startup_elapsed_ms,
                         run.durable_elapsed_ms,
                         run.durable_wait_ms,
                         run.durable_completed,
                         run.immediate_disk_match);
    }
    std::fs::write(&latest_csv, &csv).unwrap();
    std::fs::write(&archive_csv, csv).unwrap();
}

fn report_archive_suffix(profile: &RepairBenchmarkProfile) -> String {
    format!("{}__f{}__t{}__k{}__p{}__l{}__i{}__d{}",
            profile.name,
            profile.commit_log_files,
            profile.tx_per_file,
            profile.keys_per_tx,
            profile.payload_bytes,
            profile.commit_log_file_limit_bytes,
            profile.iterations,
            profile.quick_depth)
}

fn load_benchmark_disk_state(root: &Path) -> QuickRepairDiskState {
    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    futures::executor::block_on(load_benchmark_disk_state_with_runtime(rt, root))
}

async fn load_benchmark_disk_state_with_runtime(rt: pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
                                                root: &Path)
                                                -> QuickRepairDiskState {
    QuickRepairDiskState {
        meta_tables: load_meta_table_names(rt.clone(), root.join("db/.tables_meta")).await,
        log_ord: load_usize_log_table_state(rt.clone(), root.join("db/.tables").join(BENCH_LOG_ORD_TABLE)).await,
        btree: load_btree_table_state(root.join("db/.tables").join(BENCH_BTREE_TABLE).join("table.dat")),
    }
}

struct LatestLogTableLoader {
    entries: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl LatestLogTableLoader {
    fn new() -> Self {
        LatestLogTableLoader {
            entries: HashMap::new(),
        }
    }
}

impl PairLoader for LatestLogTableLoader {
    fn is_require(&self, _log_file: Option<&PathBuf>, key: &Vec<u8>) -> bool {
        !self.entries.contains_key(key)
    }

    fn load(&mut self,
            _log_file: Option<&PathBuf>,
            _method: LogMethod,
            key: Vec<u8>,
            value: Option<Vec<u8>>) {
        self.entries.insert(key, value);
    }
}

async fn load_latest_log_entries(rt: pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
                                 path: PathBuf) -> HashMap<Vec<u8>, Option<Vec<u8>>> {
    if !path.exists() {
        return HashMap::new();
    }

    let log = LogFile::open(rt,
                            path,
                            2 * 1024 * 1024,
                            512 * 1024 * 1024,
                            None).await.unwrap();
    let mut loader = LatestLogTableLoader::new();
    log.load(&mut loader, None, 2 * 1024 * 1024, true).await.unwrap();
    loader.entries
}

async fn load_usize_log_table_state(rt: pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
                                    path: PathBuf) -> BTreeMap<usize, usize> {
    let entries = load_latest_log_entries(rt, path).await;
    let mut result = BTreeMap::new();

    for (key, value) in entries {
        if let Some(value) = value {
            result.insert(binary_to_usize(&Binary::new(key)).unwrap(),
                          binary_to_usize(&Binary::new(value)).unwrap());
        }
    }

    result
}

async fn load_meta_table_names(rt: pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
                               path: PathBuf) -> Vec<String> {
    let entries = load_latest_log_entries(rt, path).await;
    let mut result = Vec::new();

    for (key, value) in entries {
        if value.is_some() {
            result.push(binary_to_atom(&Binary::new(key)).unwrap().as_str().to_string());
        }
    }
    result.sort();

    result
}

fn load_btree_table_state(path: PathBuf) -> BTreeMap<usize, usize> {
    if !path.exists() {
        return BTreeMap::new();
    }

    let snapshot_path = path.with_extension("snapshot");
    if snapshot_path.exists() {
        let _ = std::fs::remove_file(&snapshot_path);
    }
    std::fs::copy(&path, &snapshot_path).unwrap();

    let db = Builder::new().open(&snapshot_path).unwrap();
    let transaction = db.begin_read().unwrap();
    let table = match transaction.open_table(BENCH_BTREE_DEF) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => {
            drop(transaction);
            drop(db);
            let _ = std::fs::remove_file(snapshot_path);
            return BTreeMap::new();
        },
        Err(error) => panic!("open btree benchmark table failed, path: {:?}, error: {:?}", path, error),
    };
    let iter = table.iter().unwrap();
    let mut result = BTreeMap::new();

    for item in iter {
        let (key, value) = item.unwrap();
        result.insert(binary_to_usize(&key.value()).unwrap(),
                      binary_to_usize(&value.value()).unwrap());
    }

    drop(table);
    drop(transaction);
    drop(db);
    let _ = std::fs::remove_file(snapshot_path);
    result
}

fn remove_dir_if_exists(path: &Path) {
    if path.exists() {
        std::fs::remove_dir_all(path).unwrap();
    }
}

fn copy_dir_all(src: &Path,
                dst: &Path) {
    remove_dir_if_exists(dst);
    std::fs::create_dir_all(dst).unwrap();

    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let file_type = entry.file_type().unwrap();
        let target = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(entry.path().as_path(), target.as_path());
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn activate_contiguous_commit_logs_for_repair(root: &Path) -> usize {
    let commit_log_root = root.join(".commit_log");
    if !commit_log_root.exists() {
        return 0;
    }

    let mut files = std::fs::read_dir(&commit_log_root).unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .filter_map(|path| parse_commit_log_file(&path))
        .collect::<Vec<_>>();
    files.sort_by_key(|file| file.index);

    if files.is_empty() {
        return 0;
    }

    let mut suffix = Vec::new();
    let mut expected = None;
    for file in files.iter().rev() {
        match expected {
            None => {
                suffix.push(file.clone());
                expected = file.index.checked_sub(1);
            },
            Some(index) if file.index == index => {
                suffix.push(file.clone());
                expected = file.index.checked_sub(1);
            },
            _ => {
                break;
            },
        }
    }

    suffix.reverse();

    let mut activated = 0;
    for file in suffix {
        if file.is_bak {
            let active_path = commit_log_root.join(format!("{:09}", file.index));
            std::fs::rename(&file.path, active_path).unwrap();
            activated += 1;
        }
    }

    activated
}

#[derive(Clone)]
struct CommitLogFileEntry {
    index:  usize,
    is_bak: bool,
    path:   PathBuf,
}

fn parse_commit_log_file(path: &Path) -> Option<CommitLogFileEntry> {
    let file_name = path.file_name()?.to_str()?;
    if let Some(stem) = file_name.strip_suffix(".bak") {
        let index = stem.parse::<usize>().ok()?;
        return Some(CommitLogFileEntry {
            index,
            is_bak: true,
            path: path.to_path_buf(),
        });
    }

    let index = file_name.parse::<usize>().ok()?;
    Some(CommitLogFileEntry {
        index,
        is_bak: false,
        path: path.to_path_buf(),
    })
}

fn read_env_usize(name: &str,
                  default: usize) -> usize {
    std::env::var(name).ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn read_env_u64(name: &str,
                default: u64) -> u64 {
    std::env::var(name).ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn read_env_f64(name: &str,
                default: f64) -> f64 {
    std::env::var(name).ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(default)
}

fn read_env_bool(name: &str,
                 default: bool) -> bool {
    std::env::var(name).ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"))
        .unwrap_or(default)
}

fn binary_to_atom(bin: &Binary) -> Result<Atom, ReadBonErr> {
    let mut buffer = ReadBuffer::new(bin, 0);
    Atom::decode(&mut buffer)
}

fn usize_to_binary(number: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    number.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn binary_to_usize(bin: &Binary) -> Result<usize, ReadBonErr> {
    let mut buffer = ReadBuffer::new(bin, 0);
    usize::decode(&mut buffer)
}
