//! Key 版本 SnapshotLease 的真实事务生命周期与 TTL 保护专项。
//!
//! 本 target 使用短 TTL、真实 4-worker 数据库 runtime、独立 2-worker runtime、真实事务管理器、
//! CommitLogger、Memory 表和文件系统。它证明事务快照之后的同 Key 提交版本在事务存活期间不能
//! 被 TTL 删除，非 Fatal 冲突 rollback 和未启动事务跨运行时 Drop 都会释放租约，随后版本可在
//! 有界轮询内回收，而表数据始终保持不变。

mod key_version_support;

use std::time::{Duration, Instant};

use async_channel::bounded;
use pi_async_rt::rt::{AsyncRuntime,
                      multi_thread::MultiTaskRuntimeBuilder};
use pi_async_transaction::{UnitTransaction,
                           manager_2pc::Transaction2PcStatus};
use pi_atom::Atom;
use pi_db::{Binary, Version, tables::TableKV};

use key_version_support::{MEMORY_TABLE, Fixture, TestResult, TempRoot,
                          build_database, commit_ordinary, create_active_tables, encode_usize,
                          expect_binary, expect_eq, run_on_runtime, writable_transaction};

const VERSION_TTL: Duration = Duration::from_millis(120);
const POLL_INTERVAL: Duration = Duration::from_millis(5);
const BLOCKED_WAIT: Duration = Duration::from_millis(260);
const EXPIRY_TIMEOUT: Duration = Duration::from_secs(3);
const TEST_TIMEOUT: Duration = Duration::from_secs(45);

#[test]
fn test_key_version_snapshot_lifecycle() {
    let root = TempRoot::new("snapshot_lifecycle")
        .expect("creating key-version snapshot lifecycle root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let caller_rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(2)
            .build();
        let fixture = build_database(&rt,
                                     &root_path,
                                     VERSION_TTL,
                                     POLL_INTERVAL).await?;
        create_active_tables(&fixture).await?;
        let produced_before = fixture.tr_manager.produced_transaction_total();
        let consumed_before = fixture.tr_manager.consumed_transaction_total();

        verify_rollback_releases_blocking_lease(&rt, &fixture).await?;
        verify_cross_runtime_drop_releases_blocking_lease(&rt, &caller_rt, &fixture).await?;

        expect_eq("snapshot lifecycle active transactions",
                  &fixture.tr_manager.transaction_len(),
                  &0usize)?;
        expect_eq("snapshot lifecycle produced/consumed balance",
                  &(fixture.tr_manager.produced_transaction_total() - produced_before),
                  &(fixture.tr_manager.consumed_transaction_total() - consumed_before))
    })
    .unwrap_or_else(|error| panic!("key-version snapshot lifecycle failed: {error}"));
}

async fn verify_rollback_releases_blocking_lease(
    rt: &pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
    fixture: &Fixture,
) -> TestResult<()> {
    let table = Atom::from(MEMORY_TABLE);
    let key = encode_usize(0x6100_0001);
    let initial_value = encode_usize(0x6200_0001);
    let committed_value = encode_usize(0x6200_0002);

    let seed = writable_transaction(&fixture.db, "snapshot rollback seed")?;
    seed.upsert(vec![TableKV::new(table.clone(),
                                 key.clone(),
                                 Some(initial_value.clone()))])
        .await
        .map_err(|error| format!("snapshot rollback seed upsert failed: {error:?}"))?;
    commit_ordinary(&seed, "snapshot rollback seed").await?;

    let snapshot = writable_transaction(&fixture.db, "snapshot rollback owner")?;
    let values = snapshot
        .query(vec![TableKV::new(table.clone(), key.clone(), None)])
        .await;
    expect_eq("snapshot rollback query slot count", &values.len(), &1usize)?;
    expect_binary("snapshot rollback captured value",
                  values[0].as_ref(),
                  Some(&initial_value))?;

    let writer = writable_transaction(&fixture.db, "snapshot rollback concurrent writer")?;
    writer.upsert(vec![TableKV::new(table.clone(),
                                   key.clone(),
                                   Some(committed_value.clone()))])
        .await
        .map_err(|error| format!("snapshot rollback writer upsert failed: {error:?}"))?;
    commit_ordinary(&writer, "snapshot rollback concurrent writer").await?;
    let committed = fixture.db
        .query_with_version(table.clone(), key.clone())
        .await
        .map_err(|error| format!("snapshot rollback committed qwv failed: {error:?}"))?;
    expect_binary("snapshot rollback committed value",
                  committed.0.as_ref(),
                  Some(&committed_value))?;
    if !matches!(committed.1, Version::Upsert(_)) {
        return Err(format!("snapshot rollback committed version is not Upsert: {:?}", committed.1));
    }

    rt.timeout(BLOCKED_WAIT.as_millis() as usize).await;
    let blocked = fixture.db
        .query_with_version(table.clone(), key.clone())
        .await
        .map_err(|error| format!("snapshot rollback blocked qwv failed: {error:?}"))?;
    expect_binary("snapshot rollback blocked value",
                  blocked.0.as_ref(),
                  Some(&committed_value))?;
    expect_eq("snapshot rollback lease retained committed version",
              &blocked.1,
              &committed.1)?;

    let error = snapshot
        .prepare_modified_conflicts()
        .await
        .expect_err("snapshot owner must conflict with the post-snapshot write");
    let conflict = error
        .conflicts()
        .ok_or_else(|| format!("snapshot rollback expected conflict, observed {error:?}"))?;
    expect_eq("snapshot rollback conflict table", conflict.0, &table)?;
    expect_eq("snapshot rollback conflict key", conflict.1, &key)?;
    expect_eq("snapshot rollback prepare status",
              &snapshot.get_status(),
              &Transaction2PcStatus::PrepareFailed)?;
    snapshot.rollback_modified()
        .await
        .map_err(|rollback| format!("snapshot rollback failed: {rollback:?}"))?;
    expect_eq("snapshot rollback terminal status",
              &snapshot.get_status(),
              &Transaction2PcStatus::Rollbacked)?;

    wait_for_replacement_version(rt,
                                 fixture,
                                 &table,
                                 &key,
                                 &committed_value,
                                 &committed.1,
                                 "rollback release").await
}

async fn verify_cross_runtime_drop_releases_blocking_lease(
    rt: &pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
    caller_rt: &pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
    fixture: &Fixture,
) -> TestResult<()> {
    let table = Atom::from(MEMORY_TABLE);
    let key = encode_usize(0x6100_0002);
    let committed_value = encode_usize(0x6200_0003);
    let snapshot = writable_transaction(&fixture.db, "snapshot cross-runtime drop owner")?;
    let values = snapshot
        .query(vec![TableKV::new(table.clone(), key.clone(), None)])
        .await;
    expect_eq("snapshot drop query slot count", &values.len(), &1usize)?;
    expect_binary("snapshot drop captured missing value", values[0].as_ref(), None)?;

    let writer = writable_transaction(&fixture.db, "snapshot drop concurrent writer")?;
    writer.upsert(vec![TableKV::new(table.clone(),
                                   key.clone(),
                                   Some(committed_value.clone()))])
        .await
        .map_err(|error| format!("snapshot drop writer upsert failed: {error:?}"))?;
    commit_ordinary(&writer, "snapshot drop concurrent writer").await?;
    let committed = fixture.db
        .query_with_version(table.clone(), key.clone())
        .await
        .map_err(|error| format!("snapshot drop committed qwv failed: {error:?}"))?;
    expect_binary("snapshot drop committed value",
                  committed.0.as_ref(),
                  Some(&committed_value))?;

    rt.timeout(BLOCKED_WAIT.as_millis() as usize).await;
    let blocked = fixture.db
        .query_with_version(table.clone(), key.clone())
        .await
        .map_err(|error| format!("snapshot drop blocked qwv failed: {error:?}"))?;
    expect_eq("snapshot drop lease retained committed version", &blocked.1, &committed.1)?;

    let (sender, receiver) = bounded(1);
    caller_rt.spawn(async move {
        drop(snapshot);
        let _ = sender.send(()).await;
    }).map_err(|error| format!("spawning cross-runtime snapshot drop failed: {error:?}"))?;
    receiver.recv()
        .await
        .map_err(|error| format!("cross-runtime snapshot drop did not complete: {error}"))?;

    wait_for_replacement_version(rt,
                                 fixture,
                                 &table,
                                 &key,
                                 &committed_value,
                                 &committed.1,
                                 "cross-runtime Drop release").await
}

async fn wait_for_replacement_version(
    rt: &pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
    fixture: &Fixture,
    table: &Atom,
    key: &Binary,
    expected_value: &Binary,
    old_version: &Version,
    label: &str,
) -> TestResult<()> {
    let started = Instant::now();
    loop {
        let (value, version) = fixture.db
            .query_with_version(table.clone(), key.clone())
            .await
            .map_err(|error| format!("{label} qwv failed: {error:?}"))?;
        expect_binary(&format!("{label} value"), value.as_ref(), Some(expected_value))?;
        if version != *old_version {
            if !matches!(version, Version::Upsert(_)) {
                return Err(format!("{label} replacement is not Upsert: {version:?}"));
            }
            return Ok(());
        }
        if started.elapsed() >= EXPIRY_TIMEOUT {
            return Err(format!("{label} did not release the committed version within {EXPIRY_TIMEOUT:?}"));
        }
        rt.timeout(POLL_INTERVAL.as_millis() as usize).await;
    }
}
