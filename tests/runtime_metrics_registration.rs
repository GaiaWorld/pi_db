//! 本库运行时指标登记的最小真实启动专项。
//!
//! 仅验证 pi_db 启动成功后保存登记 ID，且 manager clone 共享同一回执。
//! 不重复测试上游登记、注销、采集、上报或 SDK 接口；名称及登记位置由调用点审查核对。
//! 数据库后台任务沿用原生命周期，因此在子进程运行真实装配，退出后由父进程清理文件。

#![cfg(all(feature = "trace", not(target_arch = "wasm32")))]

mod key_version_support;

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use pi_async_rt::rt::{multi_thread::MultiTaskRuntimeBuilder, startup_global_time_loop};

use key_version_support::build_database;

const CHILD_ROOT: &str = "PI_DB_RUNTIME_METRICS_REGISTRATION_TEST_ROOT";
const TEST_NAME: &str = "test_database_runtime_metrics_registration";
const DEADLINE: Duration = Duration::from_secs(45);

#[test]
fn test_database_runtime_metrics_registration() {
    if let Some(root) = env::var_os(CHILD_ROOT) {
        verify_database_registration(&PathBuf::from(root));
        return;
    }

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after UNIX_EPOCH")
        .as_nanos();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(format!("runtime-metrics-registration-{}-{}", std::process::id(), suffix));
    let mut child = Command::new(env::current_exe().expect("test executable must be available"))
        .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
        .env(CHILD_ROOT, &root)
        .spawn()
        .expect("starting isolated database registration test must succeed");
    let started = Instant::now();
    let outcome = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) if started.elapsed() < DEADLINE => thread::sleep(Duration::from_millis(10)),
            result => {
                let _ = child.kill();
                let _ = child.wait();
                break Err(format!("database registration child failed or exceeded {DEADLINE:?}: {result:?}"));
            }
        }
    };

    // 必须先确认子进程结束，避免后台文件句柄与 Windows 目录清理相互竞争。
    if root.exists() {
        fs::remove_dir_all(&root).expect("database registration fixture must be fully removed");
    }
    assert!(outcome.expect("database registration test must finish within its deadline").success(),
            "database registration child assertions failed");
}

fn verify_database_registration(root: &Path) {
    let _time_loop = startup_global_time_loop(10);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(2)
        .set_worker_limit(2, 2)
        .build();
    // 在 runtime 外驱动启动；真实文件 I/O 和后台任务仍由上述 runtime 执行。
    let fixture = futures::executor::block_on(build_database(
        &rt,
        root,
        Duration::ZERO,
        Duration::ZERO,
    ))
    .expect("real database startup must succeed");
    let id = fixture.db.runtime_metric_id()
        .expect("successful trace startup must save its runtime registration ID");
    assert_eq!(fixture.db.clone().runtime_metric_id(), Some(id),
               "manager clones must share the saved registration ID");
}
