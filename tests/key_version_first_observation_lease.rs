//! 首次观察租约在真实 TTL、事务、表存储和跨运行时并发下的专项集成测试。
//!
//! 本 target 不读取内部计数，也不把 API 返回 Ok 当作成功。每个成功的版本 read-set prepare
//! 都在预提交占用仍存活时，通过独立普通只读事务读取权威值并逐字节比较；每个写提交同时核验
//! 最终值、版本类型、本事务 TID 回执、事务状态、WAL 确认和 manager 计数。局部原子/RAII/扫描
//! 交错由 `src/key_version.rs` 单元红线承担，本文件证明公开生产装配中的组合语义。真实 WAL
//! 条目顺序和内容、repair、持久表文件冷启动数据及 Memory 无独立文件边界由同一验收矩阵中的
//! `schema_protocol_recovery` 独立进程目标逐项核验；两个目标共同构成 WAL、数据和版本硬门禁。

mod key_version_support;

use std::time::{Duration, Instant};

use async_channel::bounded;
use pi_async_rt::rt::{AsyncRuntime,
                      multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder}};
use pi_async_transaction::{AsyncCommitLog, ErrorLevel, Transaction2Pc, UnitTransaction,
                           manager_2pc::Transaction2PcStatus};
use pi_atom::Atom;
use pi_db::{Binary, KVTableTrError, TableKeyVersion, Version, VersionConflictKind,
            tables::TableKV};

use key_version_support::{BTREE_TABLE, Fixture, LOG_ORDERED_TABLE, MEMORY_TABLE, META_TABLE,
                          RealDb, TestResult, TempRoot, build_database,
                          create_active_tables, encode_atom, encode_usize, expect_binary,
                          expect_eq, expect_single_version_receipt, query_ordinary,
                          run_on_runtime, wait_for_all_confirmed, writable_transaction};

const TEST_TIMEOUT: Duration = Duration::from_secs(150);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(90);
const VERSION_TTL: Duration = Duration::from_millis(12);
const POLL_INTERVAL: Duration = Duration::from_millis(1);
const VERSION_CHANGE_TIMEOUT: Duration = Duration::from_millis(600);
const MAX_WRITE_ATTEMPTS: usize = 128;
// 1 个静态多表根 + 三表各 2 个强制冲突准备根 + 五个并发矩阵各自的初始根和写轮次。
// 冲突重试发生在根 WAL 前，不计入该值；一次多表提交只产生一个根 append。
const EXPECTED_PERSISTENT_WRITE_ROOTS: usize = 1 + 3 * 2 + (1 + 8) + (1 + 16)
    + (1 + 24) + (1 + 8) + (1 + 10);

#[derive(Default)]
struct ObservationCounts {
    prepared: usize,
    conflicted: usize,
}

enum ObservationOutcome {
    Prepared,
    Conflicted,
}

#[test]
fn test_first_observation_lease_real_matrix() {
    let root = TempRoot::new("first_observation_lease")
        .expect("creating first-observation lease root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |db_rt| async move {
        let single_worker_rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(1)
            .build();
        let multi_worker_rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(3)
            .build();
        let fixture = build_database(&db_rt,
                                     &root_path,
                                     VERSION_TTL,
                                     POLL_INTERVAL).await?;
        create_active_tables(&fixture).await?;
        let produced_before = fixture.tr_manager.produced_transaction_total();
        let consumed_before = fixture.tr_manager.consumed_transaction_total();
        let append_before = fixture.logger.append_total_count();

        verify_quiescent_ttl_branches(&db_rt, &fixture).await?;
        for (table_index, table) in
            [MEMORY_TABLE, LOG_ORDERED_TABLE, BTREE_TABLE].into_iter().enumerate() {
            verify_forced_read_set_conflict(&db_rt,
                                            &multi_worker_rt,
                                            &fixture,
                                            table,
                                            0x6050_0000 + table_index).await?;
        }
        verify_contention_case(&db_rt,
                               &single_worker_rt,
                               &fixture,
                               MEMORY_TABLE,
                               0x6100_0000,
                               1,
                               8,
                               12).await?;
        verify_contention_case(&db_rt,
                               &multi_worker_rt,
                               &fixture,
                               MEMORY_TABLE,
                               0x6200_0000,
                               4,
                               16,
                               24).await?;
        verify_contention_case(&db_rt,
                               &multi_worker_rt,
                               &fixture,
                               MEMORY_TABLE,
                               0x6300_0000,
                               12,
                               24,
                               24).await?;
        verify_contention_case(&db_rt,
                               &single_worker_rt,
                               &fixture,
                               LOG_ORDERED_TABLE,
                               0x6400_0000,
                               2,
                               8,
                               12).await?;
        verify_contention_case(&db_rt,
                               &multi_worker_rt,
                               &fixture,
                               BTREE_TABLE,
                               0x6500_0000,
                               4,
                               10,
                               16).await?;

        wait_for_all_confirmed(&db_rt,
                               &fixture,
                               CONFIRM_TIMEOUT,
                               "first-observation lease matrix").await?;
        expect_eq("first-observation lease root WAL append delta",
                  &(fixture.logger.append_total_count() - append_before),
                  &EXPECTED_PERSISTENT_WRITE_ROOTS)?;
        expect_eq("first-observation lease active transaction registry",
                  &fixture.tr_manager.transaction_len(),
                  &0usize)?;
        expect_eq("first-observation lease produced/consumed balance",
                  &(fixture.tr_manager.produced_transaction_total() - produced_before),
                  &(fixture.tr_manager.consumed_transaction_total() - consumed_before))
    })
    .unwrap_or_else(|error| panic!("first-observation lease real matrix failed: {error}"));
}

async fn verify_quiescent_ttl_branches(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
) -> TestResult<()> {
    let entries = [
        (MEMORY_TABLE, encode_usize(0x6000_0001), encode_usize(0x6000_1001)),
        (LOG_ORDERED_TABLE, encode_usize(0x6000_0002), encode_usize(0x6000_1002)),
        (BTREE_TABLE, encode_usize(0x6000_0003), encode_usize(0x6000_1003)),
    ];
    let mut read_set = Vec::with_capacity(entries.len());
    let mut write_set = Vec::with_capacity(entries.len());
    for (table, key, value) in &entries {
        let observed = fixture
            .db
            .query_with_version(Atom::from(*table), key.clone())
            .await
            .map_err(|error| format!("{table} initial missing qwv failed: {error:?}"))?;
        expect_binary(&format!("{table} initial missing value"), observed.0.as_ref(), None)?;
        require_version_kind(&observed.1, false, &format!("{table} initial missing version"))?;
        read_set.push(TableKeyVersion {
            table: Atom::from(*table),
            key: key.clone(),
            version: observed.1,
        });
        write_set.push(TableKV::new(Atom::from(*table),
                                    key.clone(),
                                    Some(value.clone())));
    }

    let writer = writable_transaction(&fixture.db, "lease quiescent multi-table upsert")?;
    let token = writer
        .prepare_with_version(read_set, write_set)
        .await
        .map_err(|error| format!("preparing quiescent multi-table upsert failed: {error:?}"))?;
    let transaction_uid = writer
        .get_transaction_uid()
        .ok_or_else(|| "quiescent multi-table upsert has no transaction UID".to_owned())?;
    let receipts = writer
        .commit_with_version(token)
        .await
        .map_err(|error| format!("committing quiescent multi-table upsert failed: {error:?}"))?;
    expect_eq("quiescent multi-table receipt count", &receipts.len(), &entries.len())?;
    expect_eq("quiescent multi-table committed status",
              &writer.get_status(),
              &Transaction2PcStatus::Commited)?;
    for (table, key, value) in &entries {
        let matching: Vec<_> = receipts
            .iter()
            .filter(|receipt| receipt.table.as_str() == *table
                && receipt.key.as_ref() == key.as_ref())
            .collect();
        expect_eq(&format!("{table} quiescent receipt multiplicity"),
                  &matching.len(),
                  &1usize)?;
        expect_eq(&format!("{table} quiescent receipt version"),
                  &matching[0].version,
                  &Version::Upsert(transaction_uid.clone()))?;
        let observed = fixture
            .db
            .query_with_version(Atom::from(*table), key.clone())
            .await
            .map_err(|error| format!("{table} committed qwv failed: {error:?}"))?;
        expect_binary(&format!("{table} committed value"),
                      observed.0.as_ref(),
                      Some(value))?;
        require_version_kind(&observed.1, true, &format!("{table} committed version"))?;
        verify_observation_can_only_prepare_matching_value(&fixture.db,
                                                           table,
                                                           key,
                                                           observed).await?;
    }

    let meta_key = encode_atom(MEMORY_TABLE);
    let meta = fixture
        .db
        .query_with_version(Atom::from(META_TABLE), meta_key.clone())
        .await
        .map_err(|error| format!("Meta TTL baseline qwv failed: {error:?}"))?;
    let meta_value = meta
        .0
        .clone()
        .ok_or_else(|| "real Memory Meta record is missing".to_owned())?;
    require_version_kind(&meta.1, true, "Meta TTL baseline version")?;
    let replaced_meta = wait_for_version_change(rt,
                                                &fixture.db,
                                                META_TABLE,
                                                &meta_key,
                                                Some(&meta_value),
                                                &meta.1,
                                                "Meta existing first observation").await?;
    verify_observation_can_only_prepare_matching_value(&fixture.db,
                                                       META_TABLE,
                                                       &meta_key,
                                                       replaced_meta).await?;

    for (table, key, value) in &entries {
        let baseline = fixture
            .db
            .query_with_version(Atom::from(*table), key.clone())
            .await
            .map_err(|error| format!("{table} TTL baseline qwv failed: {error:?}"))?;
        expect_binary(&format!("{table} TTL baseline value"), baseline.0.as_ref(), Some(value))?;
        let replaced = wait_for_version_change(rt,
                                               &fixture.db,
                                               table,
                                               key,
                                               Some(value),
                                               &baseline.1,
                                               &format!("{table} committed record expiry")).await?;
        require_version_kind(&replaced.1, true, &format!("{table} replacement version"))?;
        verify_observation_can_only_prepare_matching_value(&fixture.db,
                                                           table,
                                                           key,
                                                           replaced).await?;
    }
    Ok(())
}

async fn verify_contention_case(
    db_rt: &MultiTaskRuntime<()>,
    caller_rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
    table_name: &str,
    key_seed: usize,
    readers: usize,
    writer_rounds: usize,
    reader_rounds: usize,
) -> TestResult<()> {
    let table = Atom::from(table_name);
    let key = encode_usize(key_seed);
    let mut expected = Some(encode_usize(key_seed + 1));
    apply_version_write_with_retry(db_rt,
                                   &fixture.db,
                                   &table,
                                   &key,
                                   expected.clone(),
                                   "contention initial value").await?;

    let (sender, receiver) = bounded(readers);
    for reader in 0..readers {
        let executor = if reader % 2 == 0 {
            db_rt.clone()
        } else {
            caller_rt.clone()
        };
        let task_rt = executor.clone();
        let task_db = fixture.db.clone();
        let task_table = table.clone();
        let task_key = key.clone();
        let task_sender = sender.clone();
        let task_table_name = table_name.to_owned();
        executor.spawn(async move {
            let mut counts = ObservationCounts::default();
            let mut result = Ok(());
            for round in 0..reader_rounds {
                let observed = match task_db
                    .query_with_version(task_table.clone(), task_key.clone())
                    .await {
                    Ok(observed) => observed,
                    Err(error) => {
                        result = Err(format!(
                            "{} reader {reader} round {round} qwv failed: {error:?}",
                            task_table_name,
                        ));
                        break;
                    },
                };
                match verify_observation_can_only_prepare_matching_value(
                    &task_db,
                    &task_table_name,
                    &task_key,
                    observed).await {
                    Ok(ObservationOutcome::Prepared) => counts.prepared += 1,
                    Ok(ObservationOutcome::Conflicted) => counts.conflicted += 1,
                    Err(error) => {
                        result = Err(format!(
                            "{} reader {reader} round {round}: {error}",
                            task_table_name,
                        ));
                        break;
                    },
                }
                task_rt.timeout(0).await;
            }
            let _ = task_sender.send((result, counts)).await;
        }).map_err(|error| format!(
            "spawning {table_name} lease reader {reader} failed: {error:?}",
        ))?;
    }
    drop(sender);

    for round in 0..writer_rounds {
        // 让提交刷新和 TTL 到期反复交替；读任务持续运行，从真实调度中覆盖首次缺失、二次命中、
        // Vacant、Occupied 以及提交/扫描交错，而不是用测试 hook 人工跳过生产路径。
        db_rt.timeout((VERSION_TTL.as_millis() as usize).saturating_add(1)).await;
        expected = if round % 3 == 2 {
            None
        } else {
            Some(encode_usize(key_seed + 10_000 + round))
        };
        if round % 2 == 0 {
            apply_version_write_with_retry(db_rt,
                                           &fixture.db,
                                           &table,
                                           &key,
                                           expected.clone(),
                                           &format!("{table_name} version writer {round}")).await?;
        } else {
            apply_ordinary_write_with_retry(db_rt,
                                            &fixture.db,
                                            &table,
                                            &key,
                                            expected.clone(),
                                            &format!("{table_name} ordinary writer {round}")).await?;
        }
    }

    let mut totals = ObservationCounts::default();
    for reader in 0..readers {
        let (result, counts) = receiver
            .recv()
            .await
            .map_err(|error| format!("{table_name} reader {reader} result missing: {error}"))?;
        result?;
        totals.prepared += counts.prepared;
        totals.conflicted += counts.conflicted;
    }
    expect_eq(&format!("{table_name} observation accounting at concurrency {readers}"),
              &(totals.prepared + totals.conflicted),
              &(readers * reader_rounds))?;
    if totals.prepared == 0 {
        return Err(format!(
            "{table_name} concurrency {readers} produced no successful read-set prepare",
        ));
    }
    // 此矩阵允许调度结果全部线性化为成功或包含若干冲突，不能把概率性冲突当作测试门禁。
    // 紧邻本矩阵执行的 verify_forced_read_set_conflict 使用通道固定提交先后，负责确定性证明
    // read-set 冲突分支；这里负责压力下每次观察、最终数据和最终版本均属于合法结果。

    let authoritative = query_ordinary(&fixture.db,
                                       table_name,
                                       key.clone(),
                                       "contention final authority").await?;
    expect_binary(&format!("{table_name} final authoritative value"),
                  authoritative.as_ref(),
                  expected.as_ref())?;
    let final_observed = fixture
        .db
        .query_with_version(table, key.clone())
        .await
        .map_err(|error| format!("{table_name} final qwv failed: {error:?}"))?;
    expect_binary(&format!("{table_name} final qwv value"),
                  final_observed.0.as_ref(),
                  expected.as_ref())?;
    require_version_kind(&final_observed.1,
                         expected.is_some(),
                         &format!("{table_name} final qwv version"))?;
    let _ = verify_observation_can_only_prepare_matching_value(&fixture.db,
                                                               table_name,
                                                               &key,
                                                               final_observed).await?;
    Ok(())
}

async fn verify_forced_read_set_conflict(
    db_rt: &MultiTaskRuntime<()>,
    caller_rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
    table_name: &'static str,
    key_seed: usize,
) -> TestResult<()> {
    let table = Atom::from(table_name);
    let key = encode_usize(key_seed);
    let initial_value = encode_usize(key_seed + 1);
    apply_version_write_with_retry(db_rt,
                                   &fixture.db,
                                   &table,
                                   &key,
                                   Some(initial_value.clone()),
                                   &format!("{table_name} forced-conflict initial write")).await?;
    let stale_observation = fixture
        .db
        .query_with_version(table.clone(), key.clone())
        .await
        .map_err(|error| format!("{table_name} forced-conflict baseline failed: {error:?}"))?;
    expect_binary(&format!("{table_name} forced-conflict baseline value"),
                  stale_observation.0.as_ref(),
                  Some(&initial_value))?;

    // writer 在独立 runtime 中完整提交后才回执；随后使用旧观察值 prepare，固定形成
    // “观察旧版本 -> 其它事务发布新数据和新版本 -> 旧 read-set 校验”的生产交错。
    let replacement_value = encode_usize(key_seed + 2);
    let task_rt = caller_rt.clone();
    let task_db = fixture.db.clone();
    let task_table = table.clone();
    let task_key = key.clone();
    let task_value = replacement_value.clone();
    let (sender, receiver) = bounded(1);
    caller_rt.spawn(async move {
        let result = apply_version_write_with_retry(
            &task_rt,
            &task_db,
            &task_table,
            &task_key,
            Some(task_value),
            "forced read-set conflict replacement",
        ).await;
        let _ = sender.send(result).await;
    }).map_err(|error| format!(
        "spawning {table_name} forced-conflict writer failed: {error:?}",
    ))?;
    receiver
        .recv()
        .await
        .map_err(|error| format!("{table_name} forced-conflict writer result missing: {error}"))??;

    let stale_reader = writable_transaction(&fixture.db,
                                            &format!("{table_name} forced stale read-set"))?;
    let error = match stale_reader
        .prepare_with_version(
            vec![TableKeyVersion {
                table: table.clone(),
                key: key.clone(),
                version: stale_observation.1,
            }],
            Vec::new(),
        )
        .await {
        Err(error) => error,
        Ok(token) => {
            let receipt = stale_reader
                .commit_with_version(token)
                .await
                .map_err(|error| format!(
                    "{table_name} unexpectedly prepared stale read-set and failed to close: {error:?}",
                ))?;
            return Err(format!(
                "{table_name} stale read-set unexpectedly prepared; commit receipt count={}",
                receipt.len(),
            ));
        },
    };
    if !error.is_all_conflicts() || !matches!(error.level(), ErrorLevel::Normal) {
        return Err(format!(
            "{table_name} stale read-set returned non-recoverable conflict {error:?}",
        ));
    }
    let conflicts = error
        .all_conflicts()
        .ok_or_else(|| format!("{table_name} stale read-set conflict accessor returned None"))?;
    expect_eq(&format!("{table_name} stale read-set conflict count"),
              &conflicts.len(),
              &1usize)?;
    expect_eq(&format!("{table_name} stale read-set conflict table"),
              &conflicts[0].table,
              &table)?;
    expect_binary(&format!("{table_name} stale read-set conflict key"),
                  Some(&conflicts[0].key),
                  Some(&key))?;
    expect_eq(&format!("{table_name} stale read-set conflict kind"),
              &conflicts[0].kind,
              &VersionConflictKind::ReadSetVersionMismatch)?;
    expect_eq(&format!("{table_name} stale read-set failed status"),
              &stale_reader.get_status(),
              &Transaction2PcStatus::PrepareFailed)?;
    stale_reader
        .rollback_modified()
        .await
        .map_err(|error| format!("{table_name} stale read-set rollback failed: {error:?}"))?;
    expect_eq(&format!("{table_name} stale read-set rollback status"),
              &stale_reader.get_status(),
              &Transaction2PcStatus::Rollbacked)?;

    let authoritative = query_ordinary(&fixture.db,
                                       table_name,
                                       key.clone(),
                                       "forced-conflict final authority").await?;
    expect_binary(&format!("{table_name} forced-conflict authoritative value"),
                  authoritative.as_ref(),
                  Some(&replacement_value))?;
    let final_observation = fixture
        .db
        .query_with_version(table, key.clone())
        .await
        .map_err(|error| format!("{table_name} forced-conflict final qwv failed: {error:?}"))?;
    expect_binary(&format!("{table_name} forced-conflict final qwv value"),
                  final_observation.0.as_ref(),
                  Some(&replacement_value))?;
    require_version_kind(&final_observation.1,
                         true,
                         &format!("{table_name} forced-conflict final version"))?;
    let _ = verify_observation_can_only_prepare_matching_value(&fixture.db,
                                                               table_name,
                                                               &key,
                                                               final_observation).await?;
    Ok(())
}

async fn apply_version_write_with_retry(
    rt: &MultiTaskRuntime<()>,
    db: &RealDb,
    table: &Atom,
    key: &Binary,
    value: Option<Binary>,
    label: &str,
) -> TestResult<()> {
    for attempt in 0..MAX_WRITE_ATTEMPTS {
        let baseline = db
            .query_with_version(table.clone(), key.clone())
            .await
            .map_err(|error| format!("{label} attempt {attempt} baseline failed: {error:?}"))?;
        let transaction = writable_transaction(db, &format!("{label} attempt {attempt}"))?;
        match transaction
            .prepare_with_version(
                vec![TableKeyVersion {
                    table: table.clone(),
                    key: key.clone(),
                    version: baseline.1,
                }],
                vec![TableKV::new(table.clone(), key.clone(), value.clone())],
            )
            .await {
            Ok(token) => {
                let uid = transaction
                    .get_transaction_uid()
                    .ok_or_else(|| format!("{label} attempt {attempt} has no TID"))?;
                let receipt = transaction
                    .commit_with_version(token)
                    .await
                    .map_err(|error| format!("{label} attempt {attempt} commit failed: {error:?}"))?;
                expect_single_version_receipt(&receipt,
                                              table,
                                              key,
                                              value.as_ref(),
                                              &uid,
                                              label)?;
                expect_eq(&format!("{label} committed status"),
                          &transaction.get_status(),
                          &Transaction2PcStatus::Commited)?;
                let observed = db
                    .query_with_version(table.clone(), key.clone())
                    .await
                    .map_err(|error| format!("{label} post-commit qwv failed: {error:?}"))?;
                expect_binary(&format!("{label} post-commit value"),
                              observed.0.as_ref(),
                              value.as_ref())?;
                require_version_kind(&observed.1,
                                     value.is_some(),
                                     &format!("{label} post-commit version"))?;
                return Ok(());
            },
            Err(error) => {
                assert_version_conflict(&error, table, key, label)?;
                transaction
                    .rollback_modified()
                    .await
                    .map_err(|rollback| format!(
                        "{label} attempt {attempt} conflict rollback failed: {rollback:?}",
                    ))?;
                rt.timeout(0).await;
            },
        }
    }
    Err(format!("{label} exceeded {MAX_WRITE_ATTEMPTS} conflict retries"))
}

async fn apply_ordinary_write_with_retry(
    rt: &MultiTaskRuntime<()>,
    db: &RealDb,
    table: &Atom,
    key: &Binary,
    value: Option<Binary>,
    label: &str,
) -> TestResult<()> {
    for attempt in 0..MAX_WRITE_ATTEMPTS {
        let transaction = writable_transaction(db, &format!("{label} attempt {attempt}"))?;
        if let Some(value) = value.as_ref() {
            transaction
                .upsert(vec![TableKV::new(table.clone(), key.clone(), Some(value.clone()))])
                .await
                .map_err(|error| format!("{label} attempt {attempt} upsert failed: {error:?}"))?;
        } else {
            let deleted = transaction
                .delete(vec![TableKV::new(table.clone(), key.clone(), None)])
                .await
                .map_err(|error| format!("{label} attempt {attempt} delete failed: {error:?}"))?;
            expect_eq(&format!("{label} delete result count"), &deleted.len(), &1usize)?;
        }
        match transaction.prepare_modified_conflicts().await {
            Ok(token) => {
                let uid = transaction
                    .get_transaction_uid()
                    .ok_or_else(|| format!("{label} attempt {attempt} has no TID"))?;
                transaction
                    .commit_modified(token)
                    .await
                    .map_err(|error| format!("{label} attempt {attempt} commit failed: {error:?}"))?;
                expect_eq(&format!("{label} committed status"),
                          &transaction.get_status(),
                          &Transaction2PcStatus::Commited)?;
                let observed = db
                    .query_with_version(table.clone(), key.clone())
                    .await
                    .map_err(|error| format!("{label} post-commit qwv failed: {error:?}"))?;
                expect_binary(&format!("{label} post-commit value"),
                              observed.0.as_ref(),
                              value.as_ref())?;
                let expected_version = if value.is_some() {
                    Version::Upsert(uid)
                } else {
                    Version::Delete(uid)
                };
                if observed.1 != expected_version {
                    // TTL 可以在 commit 返回后合法淘汰本事务版本；此时新首次观察版本仍必须
                    // 与权威值匹配，不能把“版本已按策略淘汰”误判为提交失败。
                    require_version_kind(&observed.1,
                                         value.is_some(),
                                         &format!("{label} replacement version"))?;
                    let _ = verify_observation_can_only_prepare_matching_value(
                        db,
                        table.as_str(),
                        key,
                        observed).await?;
                }
                return Ok(());
            },
            Err(error) => {
                if !error.is_conflicts()
                    || error.is_all_conflicts()
                    || !matches!(error.level(), ErrorLevel::Normal) {
                    return Err(format!("{label} returned non-ordinary-conflict error {error:?}"));
                }
                let (conflict_table, conflict_key) = error
                    .conflicts()
                    .ok_or_else(|| format!("{label} conflict accessor returned None"))?;
                expect_eq(&format!("{label} conflict table"), conflict_table, table)?;
                expect_binary(&format!("{label} conflict key"),
                              Some(conflict_key),
                              Some(key))?;
                transaction
                    .rollback_modified()
                    .await
                    .map_err(|rollback| format!(
                        "{label} attempt {attempt} conflict rollback failed: {rollback:?}",
                    ))?;
                rt.timeout(0).await;
            },
        }
    }
    Err(format!("{label} exceeded {MAX_WRITE_ATTEMPTS} conflict retries"))
}

async fn verify_observation_can_only_prepare_matching_value(
    db: &RealDb,
    table_name: &str,
    key: &Binary,
    observed: (Option<Binary>, Version),
) -> TestResult<ObservationOutcome> {
    let table = Atom::from(table_name);
    let transaction = writable_transaction(db, "lease observation semantic oracle")?;
    match transaction
        .prepare_with_version(
            vec![TableKeyVersion {
                table: table.clone(),
                key: key.clone(),
                version: observed.1,
            }],
            Vec::new(),
        )
        .await {
        Ok(token) => {
            // 无论权威点读本身是否成功，都先把已经 Prepared 的事务推进到 commit 终态，避免
            // 测试诊断路径把 manager 资源残留误当成生产缺陷。
            let authoritative_result = query_ordinary(db,
                                                      table_name,
                                                      key.clone(),
                                                      "lease observation authority").await;
            let receipt_result = transaction
                .commit_with_version(token)
                .await;
            let authoritative = authoritative_result?;
            let receipt = receipt_result
                .map_err(|error| format!("semantic-oracle read commit failed: {error:?}"))?;
            let value_matches = authoritative
                .as_ref()
                .map(AsRef::<[u8]>::as_ref)
                == observed.0.as_ref().map(AsRef::<[u8]>::as_ref);
            expect_eq("semantic-oracle read receipt", &receipt.len(), &0usize)?;
            expect_eq("semantic-oracle read status",
                      &transaction.get_status(),
                      &Transaction2PcStatus::Commited)?;
            if !value_matches {
                return Err(format!(
                    "forbidden stale value passed version prepare: table={table_name:?}, key={:?}, qwv_value={:?}, authoritative_value={:?}",
                    key.as_ref(),
                    observed.0.as_ref().map(AsRef::<[u8]>::as_ref),
                    authoritative.as_ref().map(AsRef::<[u8]>::as_ref),
                ));
            }
            Ok(ObservationOutcome::Prepared)
        },
        Err(error) => {
            assert_version_conflict(&error, &table, key, "semantic-oracle read prepare")?;
            expect_eq("semantic-oracle conflict status",
                      &transaction.get_status(),
                      &Transaction2PcStatus::PrepareFailed)?;
            transaction
                .rollback_modified()
                .await
                .map_err(|rollback| format!("semantic-oracle rollback failed: {rollback:?}"))?;
            expect_eq("semantic-oracle rollback status",
                      &transaction.get_status(),
                      &Transaction2PcStatus::Rollbacked)?;
            Ok(ObservationOutcome::Conflicted)
        },
    }
}

async fn wait_for_version_change(
    rt: &MultiTaskRuntime<()>,
    db: &RealDb,
    table_name: &str,
    key: &Binary,
    expected_value: Option<&Binary>,
    previous: &Version,
    label: &str,
) -> TestResult<(Option<Binary>, Version)> {
    let started = Instant::now();
    loop {
        let observed = db
            .query_with_version(Atom::from(table_name), key.clone())
            .await
            .map_err(|error| format!("{label} qwv failed: {error:?}"))?;
        expect_binary(&format!("{label} value"), observed.0.as_ref(), expected_value)?;
        if &observed.1 != previous {
            return Ok(observed);
        }
        if started.elapsed() >= VERSION_CHANGE_TIMEOUT {
            return Err(format!(
                "{label} version did not change within {VERSION_CHANGE_TIMEOUT:?}: {previous:?}",
            ));
        }
        rt.timeout(1).await;
    }
}

fn assert_version_conflict(
    error: &KVTableTrError,
    table: &Atom,
    key: &Binary,
    label: &str,
) -> TestResult<()> {
    if !error.is_all_conflicts() || !matches!(error.level(), ErrorLevel::Normal) {
        return Err(format!("{label}: expected AllConflicts(Normal), observed {error:?}"));
    }
    let conflicts = error
        .all_conflicts()
        .ok_or_else(|| format!("{label}: all_conflicts accessor returned None"))?;
    expect_eq(&format!("{label} conflict count"), &conflicts.len(), &1usize)?;
    expect_eq(&format!("{label} conflict table"), &conflicts[0].table, table)?;
    expect_binary(&format!("{label} conflict key"), Some(&conflicts[0].key), Some(key))?;
    if !matches!(conflicts[0].kind,
                 VersionConflictKind::ReadSetVersionMismatch
                 | VersionConflictKind::TransactionConflict) {
        return Err(format!("{label}: unexpected conflict kind {:?}", conflicts[0].kind));
    }
    Ok(())
}

fn require_version_kind(version: &Version, exists: bool, label: &str) -> TestResult<()> {
    let matches = if exists {
        matches!(version, Version::Upsert(_))
    } else {
        matches!(version, Version::Delete(_))
    };
    if matches {
        Ok(())
    } else {
        Err(format!("{label}: existence={exists}, observed {version:?}"))
    }
}
