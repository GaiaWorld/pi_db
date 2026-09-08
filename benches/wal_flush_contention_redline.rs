//! 根 WAL flush 全局提交锁排队的真实 CommitLogger 诊断基准。
//!
//! 本 target 绕过表事务，只保留 commit_with_version 的真实 WAL append/flush 依赖链。每个
//! client 使用独立 Guid 和非空 WAL，在同一个四 worker runtime 中闭环执行 append -> flush；
//! 不执行 confirm，避免 checkpoint 轮换把异步确认 I/O 混入 flush 样本。不同 profile 使用独立
//! CommitLogger 和目录，且严格串行运行。
//!
//! default-8k 是生产默认块门限；large-1m 只作为相同实现的批量分支对照，不是配置建议。
//! 输出用于确认并发增长、块门限和 flush 尾延迟的关系，不能单独证明 commit_lock 内部各阶段。

use std::{
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
use pi_async_transaction::AsyncCommitLog;
use pi_guid::Guid;
use pi_store::commit_logger::CommitLoggerBuilder;

const WORKERS: usize = 4;
const WARMUP_PER_CLIENT: usize = 2;
const SAMPLES_PER_CLIENT: usize = 6;
const RESULT_TIMEOUT: Duration = Duration::from_secs(120);
const PROFILES: [Profile; 7] = [
    Profile::new("16k_default_c1", 8 * 1024, 16 * 1024, 1),
    Profile::new("256b_default_c64", 8 * 1024, 256, 64),
    Profile::new("16k_default_c64", 8 * 1024, 16 * 1024, 64),
    Profile::new("16k_default_c256", 8 * 1024, 16 * 1024, 256),
    Profile::new("256b_large_c256", 1024 * 1024, 256, 256),
    Profile::new("16k_large_c64", 1024 * 1024, 16 * 1024, 64),
    Profile::new("16k_large_c256", 1024 * 1024, 16 * 1024, 256),
];

fn main() {
    let _time_loop = startup_global_time_loop(10);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(WORKERS)
        .build();
    let root = TempRoot::new();
    let mut results = Vec::with_capacity(PROFILES.len());

    println!(
        "wal_flush_contention_redline: workers={}, warmup/client={}, samples/client={}",
        WORKERS,
        WARMUP_PER_CLIENT,
        SAMPLES_PER_CLIENT,
    );
    println!(
        "{:<22} {:>7} {:>9} {:>7} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "profile", "clients", "payload", "samples", "append_p99", "flush_p50",
        "flush_p99", "flush_max", "batch",
    );

    for (profile_id, profile) in PROFILES.iter().copied().enumerate() {
        let result = run_profile(&rt, root.path(), profile_id + 1, profile);
        print_result(profile, &result);
        results.push((profile, result));
    }

    let serial = result_of(&results, "16k_default_c1");
    let contended = result_of(&results, "16k_default_c256");
    assert!(
        contended.flush.p50 >= serial.flush.p50.saturating_mul(4)
            && contended.flush.p99 >= serial.flush.p99.saturating_mul(3),
        "default WAL diagnostic did not reproduce the expected relative queueing amplification: serial_p50={}ns, serial_p99={}ns, contended_p50={}ns, contended_p99={}ns",
        serial.flush.p50,
        serial.flush.p99,
        contended.flush.p50,
        contended.flush.p99,
    );
}

#[derive(Clone, Copy)]
struct Profile {
    label: &'static str,
    block_limit: usize,
    payload_len: usize,
    clients: usize,
}

impl Profile {
    const fn new(label: &'static str,
                 block_limit: usize,
                 payload_len: usize,
                 clients: usize) -> Self {
        Self {
            label,
            block_limit,
            payload_len,
            clients,
        }
    }
}

struct ProfileResult {
    append: SampleStats,
    flush: SampleStats,
    batch: Duration,
}

struct ClientResult {
    append: Vec<Duration>,
    flush: Vec<Duration>,
}

fn run_profile(rt: &MultiTaskRuntime<()>,
               root: &Path,
               profile_id: usize,
               profile: Profile) -> ProfileResult {
    let path = root.join(profile.label);
    let setup_rt = rt.clone();
    let (logger_tx, logger_rx) = bounded(1);
    rt.block_on(async move {
        let logger = CommitLoggerBuilder::new(setup_rt, path)
            .log_block_limit(profile.block_limit)
            .delay_timeout(1)
            .log_file_limit(2 * 1024 * 1024 * 1024)
            .collect_interval(5 * 60 * 1000)
            .build()
            .await
            .expect("WAL redline CommitLogger must start");
        logger_tx
            .send(logger)
            .expect("WAL redline logger receiver must remain alive");
    })
    .expect("WAL redline logger setup must complete");
    let logger = logger_rx.recv().expect("WAL redline logger must be returned");
    let (ready_tx, ready_rx) = unbounded();
    let (result_tx, result_rx) = unbounded();
    let (start_tx, start_rx) = async_channel::bounded::<()>(1);
    let common_start = Arc::new(OnceLock::<Instant>::new());
    let iterations = WARMUP_PER_CLIENT + SAMPLES_PER_CLIENT;

    for client in 0..profile.clients {
        let logger = logger.clone();
        let ready_tx = ready_tx.clone();
        let result_tx = result_tx.clone();
        let start_rx = start_rx.clone();
        let common_start = common_start.clone();
        rt.spawn(async move {
            let _ = ready_tx.send(());
            if start_rx.recv().await.is_ok() {
                let _ = result_tx.send(Err(
                    "WAL redline start gate unexpectedly delivered a value".to_string(),
                ));
                return;
            }
            let mut append_samples = Vec::with_capacity(SAMPLES_PER_CLIENT);
            let mut flush_samples = Vec::with_capacity(SAMPLES_PER_CLIENT);
            for iteration in 0..iterations {
                let sequence = client
                    .checked_mul(iterations)
                    .and_then(|base| base.checked_add(iteration))
                    .expect("WAL redline sequence must not overflow");
                let uid = Guid(
                    ((profile_id as u128) << 96)
                        | ((client as u128) << 48)
                        | (iteration as u128 + 1),
                );
                let mut payload = vec![0u8; profile.payload_len];
                payload[..8].copy_from_slice(&(sequence as u64).to_le_bytes());

                let append_started = Instant::now();
                let handle = match logger.append(uid, payload).await {
                    Ok(handle) if handle != 0 => handle,
                    Ok(_) => {
                        let _ = result_tx.send(Err(
                            "non-empty WAL append returned reserved handle 0".to_string(),
                        ));
                        return;
                    },
                    Err(error) => {
                        let _ = result_tx.send(Err(format!("WAL append failed: {error}")));
                        return;
                    },
                };
                let append_elapsed = append_started.elapsed();

                let flush_started = Instant::now();
                if let Err(error) = logger.flush(handle).await {
                    let _ = result_tx.send(Err(format!("WAL flush failed: {error}")));
                    return;
                }
                let flush_elapsed = flush_started.elapsed();
                if iteration >= WARMUP_PER_CLIENT {
                    append_samples.push(append_elapsed);
                    flush_samples.push(flush_elapsed);
                }
            }
            let _ = common_start
                .get()
                .expect("WAL redline common start must be initialized")
                .elapsed();
            let _ = result_tx.send(Ok(ClientResult {
                append: append_samples,
                flush: flush_samples,
            }));
        })
        .expect("WAL redline client task must be accepted");
    }
    drop(ready_tx);
    drop(result_tx);

    for _ in 0..profile.clients {
        ready_rx
            .recv_timeout(RESULT_TIMEOUT)
            .expect("all WAL redline clients must reach the start gate");
    }
    let started = Instant::now();
    common_start
        .set(started)
        .expect("WAL redline common start must only be set once");
    drop(start_tx);

    let mut append = Vec::with_capacity(profile.clients * SAMPLES_PER_CLIENT);
    let mut flush = Vec::with_capacity(profile.clients * SAMPLES_PER_CLIENT);
    for _ in 0..profile.clients {
        let client = result_rx
            .recv_timeout(RESULT_TIMEOUT)
            .expect("WAL redline client must finish before deadline")
            .unwrap_or_else(|error| panic!("WAL redline client failed: {error}"));
        append.extend(client.append);
        flush.extend(client.flush);
    }
    let batch = started.elapsed();
    let expected = profile.clients * iterations;
    assert_eq!(logger.append_total_count(), expected);
    let waiting = rt
        .block_on({
            let logger = logger.clone();
            async move { Some(logger.waiting_confirm_count().await) }
        })
        .expect("WAL redline waiting count query must complete")
        .expect("WAL redline waiting count must be preserved");
    assert_eq!(waiting, expected);
    assert_eq!(append.len(), profile.clients * SAMPLES_PER_CLIENT);
    assert_eq!(flush.len(), profile.clients * SAMPLES_PER_CLIENT);
    assert!(directory_bytes(root.join(profile.label)) > 0,
            "successful WAL flushes must leave non-empty files");

    ProfileResult {
        append: SampleStats::from_samples(&mut append),
        flush: SampleStats::from_samples(&mut flush),
        batch,
    }
}

fn result_of<'a>(results: &'a [(Profile, ProfileResult)], label: &str) -> &'a ProfileResult {
    &results
        .iter()
        .find(|(profile, _)| profile.label == label)
        .expect("WAL redline comparison profile must exist")
        .1
}

fn print_result(profile: Profile, result: &ProfileResult) {
    println!(
        "{:<22} {:>7} {:>9} {:>7} {:>12} {:>12} {:>12} {:>12} {:>12}",
        profile.label,
        profile.clients,
        profile.payload_len,
        profile.clients * SAMPLES_PER_CLIENT,
        result.append.p99,
        result.flush.p50,
        result.flush.p99,
        result.flush.max,
        result.batch.as_nanos(),
    );
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
            max: *nanos.last().expect("WAL redline samples must not be empty"),
        }
    }
}

fn percentile(sorted: &[u128], percentile: usize) -> u128 {
    let index = (sorted.len() - 1) * percentile / 100;
    sorted[index]
}

fn directory_bytes(path: PathBuf) -> u64 {
    fs::read_dir(path)
        .expect("WAL redline directory must be readable")
        .map(|entry| {
            let entry = entry.expect("WAL redline directory entry must be readable");
            let metadata = entry
                .metadata()
                .expect("WAL redline file metadata must be readable");
            if metadata.is_file() {
                metadata.len()
            } else {
                0
            }
        })
        .sum()
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
            "pi_db_wal_flush_contention_redline_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path).expect("WAL redline temporary root must be created");
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
