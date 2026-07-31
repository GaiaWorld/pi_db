//! 根 WAL checkpoint 轮换的修复前后有界真实文件基准。
//!
//! 本 target 使用真实多线程 runtime、`CommitLogger`、`LogFile` 和文件系统，分别测量：
//!
//! - current block 与 writable 文件都为空时的显式轮换；
//! - WAL 已 flush、current block 为空时的轮换；
//! - WAL 已 append 但尚未 flush 时，`append_check_point + flush` 的等价完整工作量。
//!
//! 最后一项把 rotate 和 flush 一起计时，确保修复前后都包含同一次必要 WAL sync，不能通过
//! 修复前错误地遗漏 I/O 得到虚假的性能优势。`PI_DB_BENCH_EXPECT_FIXED=1` 只切换计时外的
//! 正确性 oracle：修复前必须观察到登记文件未增长，修复后必须观察到登记文件增长；采样代码
//! 和计时边界完全相同。结果只允许用于同机、同工具链、同依赖图和固化二进制的相对比较；
//! 最终门禁见
//! `docs/ROOT_WAL_CHECKPOINT_ROTATION_BUG.md#bug-root-wal-checkpoint-acceptance`。

use std::{
    env,
    fs,
    future::Future,
    hint::black_box,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::AsyncCommitLog;
use pi_guid::Guid;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

const EXPECT_FIXED_ENV: &str = "PI_DB_BENCH_EXPECT_FIXED";
const WARMUP_SAMPLES: usize = 4;
const MEASURED_SAMPLES: usize = 40;
const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const PAYLOAD_LEN: usize = 1024;

fn main() {
    let _time_loop = startup_global_time_loop(1);
    let expect_fixed = env::var_os(EXPECT_FIXED_ENV).is_some();
    println!(
        "root_wal_checkpoint_rotation: workers=4, warmup={}, samples={}, expect_fixed={}, setup/checks excluded",
        WARMUP_SAMPLES,
        MEASURED_SAMPLES,
        expect_fixed,
    );
    println!(
        "{:<34} {:>7} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "case", "samples", "min(ns)", "p50(ns)", "p90(ns)", "p99(ns)", "mean(ns)", "max(ns)",
    );

    let empty = Fixture::new("empty");
    run_case("checkpoint_empty", || empty.measure_empty_rotation());

    let flushed = Fixture::new("flushed");
    run_case("checkpoint_after_flush", || {
        flushed.measure_after_flush_rotation()
    });

    let pending = Fixture::new("pending");
    run_case("checkpoint_pending_complete", || {
        pending.measure_pending_rotation(expect_fixed)
    });

    assert_eq!(
        pending.logger.append_total_count(),
        pending.logger.confirm_total_count(),
        "every pending benchmark WAL record must be confirmed outside the timed region",
    );
    assert_eq!(
        pending.run({
            let logger = pending.logger.clone();
            async move { logger.waiting_confirm_count().await }
        }),
        0,
        "pending benchmark must not retain checkpoint registrations",
    );
}

fn run_case<F>(label: &str, mut sample: F)
where
    F: FnMut() -> Duration,
{
    for _ in 0..WARMUP_SAMPLES {
        black_box(sample());
    }

    let mut observed = Vec::with_capacity(MEASURED_SAMPLES);
    for _ in 0..MEASURED_SAMPLES {
        observed.push(sample());
    }
    let stats = SampleStats::from_samples(&mut observed);
    println!(
        "{:<34} {:>7} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
        label,
        MEASURED_SAMPLES,
        stats.min,
        stats.p50,
        stats.p90,
        stats.p99,
        stats.mean,
        stats.max,
    );
}

struct Fixture {
    logger: CommitLogger,
    rt: MultiTaskRuntime<()>,
    wal_path: PathBuf,
    next_guid: std::sync::atomic::AtomicU64,
    _root: TempRoot,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let root = TempRoot::new(label);
        let wal_path = root.path().join("root-wal");
        let rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(4)
            .build();
        let logger = run_on_runtime(
            &rt,
            {
                let build_rt = rt.clone();
                let build_path = wal_path.clone();
                async move {
                    CommitLoggerBuilder::new(build_rt, build_path)
                        .log_file_limit(512 * 1024 * 1024)
                        .collect_interval(5 * 60 * 1000)
                        .build()
                        .await
                        .expect("checkpoint benchmark CommitLogger must start")
                }
            },
        );
        Self {
            logger,
            rt,
            wal_path,
            next_guid: std::sync::atomic::AtomicU64::new(1),
            _root: root,
        }
    }

    fn measure_empty_rotation(&self) -> Duration {
        let next_checkpoint = self.run({
            let logger = self.logger.clone();
            async move { logger.current_check_point().await }
        });

        let started = Instant::now();
        let allocated = self.run({
            let logger = self.logger.clone();
            async move {
                logger
                    .append_check_point()
                    .await
                    .expect("empty checkpoint rotation must succeed")
            }
        });
        let elapsed = started.elapsed();

        assert_eq!(allocated, next_checkpoint);
        assert_eq!(
            active_checkpoint_len(&self.wal_path, allocated),
            0,
            "new empty checkpoint must remain empty",
        );
        elapsed
    }

    fn measure_after_flush_rotation(&self) -> Duration {
        let commit_uid = self.next_commit_uid();
        let (handle, registered) = self.append(commit_uid.clone());
        self.run({
            let logger = self.logger.clone();
            async move {
                logger
                    .flush(handle)
                    .await
                    .expect("checkpoint-after-flush setup must flush WAL")
            }
        });
        let registered_len = active_checkpoint_len(&self.wal_path, registered);
        assert!(registered_len > 0, "flushed checkpoint must be nonempty");

        let started = Instant::now();
        let allocated = self.run({
            let logger = self.logger.clone();
            async move {
                logger
                    .append_check_point()
                    .await
                    .expect("checkpoint-after-flush rotation must succeed")
            }
        });
        let elapsed = started.elapsed();

        assert_eq!(
            active_checkpoint_len(&self.wal_path, registered),
            registered_len,
            "rotating an already-flushed checkpoint must not rewrite it",
        );
        assert_eq!(
            active_checkpoint_len(&self.wal_path, allocated),
            0,
            "new checkpoint after an already-flushed WAL must start empty",
        );
        self.confirm(commit_uid);
        elapsed
    }

    fn measure_pending_rotation(&self, expect_fixed: bool) -> Duration {
        let commit_uid = self.next_commit_uid();
        let (handle, registered) = self.append(commit_uid.clone());
        let registered_len_before = active_checkpoint_len(&self.wal_path, registered);

        let started = Instant::now();
        let allocated = self.run({
            let logger = self.logger.clone();
            async move {
                let checkpoint = logger
                    .append_check_point()
                    .await
                    .expect("pending checkpoint rotation must succeed");
                logger
                    .flush(handle)
                    .await
                    .expect("pending checkpoint completion must flush WAL");
                checkpoint
            }
        });
        let elapsed = started.elapsed();

        let registered_len_after = active_checkpoint_len(&self.wal_path, registered);
        let allocated_len = active_checkpoint_len(&self.wal_path, allocated);
        if expect_fixed {
            assert!(
                registered_len_after > registered_len_before,
                "fixed rotation must write pending WAL to its registered checkpoint: before={registered_len_before}, after={registered_len_after}",
            );
            assert_eq!(
                allocated_len, 0,
                "fixed rotation must not move pre-rotation WAL into the new checkpoint",
            );
        } else {
            assert_eq!(
                registered_len_after, registered_len_before,
                "repair-before baseline must preserve the known registered-file mismatch",
            );
            assert!(
                allocated_len > 0,
                "repair-before baseline must write pending WAL into the new checkpoint",
            );
        }
        self.confirm(commit_uid);
        elapsed
    }

    fn append(&self, commit_uid: Guid) -> (usize, usize) {
        self.run({
            let logger = self.logger.clone();
            async move {
                let handle = logger
                    .append(commit_uid.clone(), vec![0x5a; PAYLOAD_LEN])
                    .await
                    .expect("checkpoint benchmark WAL append must succeed");
                let checkpoint = logger
                    .check_point_of(commit_uid)
                    .await
                    .expect("appended WAL must be registered in a checkpoint");
                (handle, checkpoint)
            }
        })
    }

    fn confirm(&self, commit_uid: Guid) {
        self.run({
            let logger = self.logger.clone();
            async move {
                logger
                    .confirm(commit_uid)
                    .await
                    .expect("checkpoint benchmark confirmation must succeed");
            }
        });
    }

    fn next_commit_uid(&self) -> Guid {
        let sequence = self
            .next_guid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Guid(((std::process::id() as u128) << 64) | sequence as u128)
    }

    fn run<T, F>(&self, future: F) -> T
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        run_on_runtime(&self.rt, future)
    }
}

fn run_on_runtime<T, F>(rt: &MultiTaskRuntime<()>, future: F) -> T
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    let (sender, receiver) = bounded(1);
    rt.spawn(async move {
        let _ = sender.send(future.await);
    })
    .expect("checkpoint benchmark operation must spawn");
    receiver
        .recv_timeout(OPERATION_TIMEOUT)
        .expect("checkpoint benchmark operation exceeded its hard deadline")
}

fn active_checkpoint_len(wal_path: &Path, checkpoint: usize) -> u64 {
    let path = wal_path.join(format!("{checkpoint:09}"));
    fs::metadata(&path)
        .unwrap_or_else(|error| panic!("active checkpoint {path:?} must exist: {error}"))
        .len()
}

struct SampleStats {
    min: u128,
    p50: u128,
    p90: u128,
    p99: u128,
    mean: u128,
    max: u128,
}

impl SampleStats {
    fn from_samples(samples: &mut [Duration]) -> Self {
        assert!(!samples.is_empty());
        samples.sort_unstable();
        let nanos: Vec<u128> = samples.iter().map(Duration::as_nanos).collect();
        let total: u128 = nanos.iter().sum();
        Self {
            min: nanos[0],
            p50: percentile(&nanos, 50),
            p90: percentile(&nanos, 90),
            p99: percentile(&nanos, 99),
            mean: total / nanos.len() as u128,
            max: nanos[nanos.len() - 1],
        }
    }
}

fn percentile(sorted: &[u128], percentile: usize) -> u128 {
    let index = (sorted.len() - 1) * percentile / 100;
    sorted[index]
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(label: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must follow UNIX_EPOCH")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "pi_db_root_wal_checkpoint_bench_{label}_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path)
            .expect("checkpoint benchmark temporary root must be created");
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
