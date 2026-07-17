//! 五类表普通事务预提交冲突的真实红测矩阵。
//!
//! 本 target 不引用或运行旧测试。它只使用当前公开普通 API，通过真实 4-worker runtime、
//! `Transaction2PcManager`、根 `CommitLogger`、Meta/Memory/LogOrdered/LogWrite/Btree、真实表文件
//! 和临时文件系统，验证以下必须成立的预提交不变量：
//!
//! - 两个事务同时观察不存在的 Key 时，一个事务提交后，另一个事务必须冲突；
//! - 已存在 Key 被另一个事务以同一 `Binary` owner 再次写入时，旧事务仍必须冲突；
//! - Key 从 A 更新到 B、再恢复为旧 A owner 后，旧事务必须识别 ABA 并冲突；
//! - 一个事务仍停留在 Prepared 时，第二个 same-key 事务必须由既有 prepare 预留拒绝。
//!
//! 每个场景精确检查冲突表/Key、错误等级、事务状态、rollback、事务管理器 produced/consumed/
//! active 计数、根 WAL append 数及可公开观察表的最终值。LogWrite 按公开契约不可查询，其最终
//! 写入只通过唯一成功提交、根 WAL 精确增量和零额外提交证明；后续持久化专项再验证数据日志。
//!
//! 修复前五表基线的历史预期是本测试定向失败；失败必须来自“已提交写之后旧事务
//! 预提交成功”，而不能来自夹具、I/O、timeout、错误分类或清理失败。当前 Meta/Memory/
//! LogOrdered/Btree 转绿入口继续使用原断言；LogWrite 历史入口按 HC-059 暂停执行。
//! 设计与实施入口：`docs/KEY_VERSION_PUBLICATION_IMPLEMENTATION_PLAN.md#kv-impl-red-tests`。

use std::{
    fmt::{self, Debug, Write as _},
    fs,
    future::Future,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::{
    manager_2pc::{Transaction2PcManager, Transaction2PcStatus},
    AsyncCommitLog, ErrorLevel, UnitTransaction,
};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    utils::CreateTableOptions,
    Binary, KVDBTableType, KVTableMeta, KVTableTrError,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;
type RealTrManager = Transaction2PcManager<usize, CommitLogger>;

const META_TABLE: &str = ".tables_meta";
const MEMORY_TABLE: &str = "conflict_memory";
const LOG_ORDERED_TABLE: &str = "conflict_log_ordered";
const LOG_WRITE_TABLE: &str = "conflict_log_write";
const BTREE_TABLE: &str = "conflict_btree";
const TEST_TIMEOUT: Duration = Duration::from_secs(120);

const TABLE_CASES: [TableCase; 5] = [
    TableCase::queryable("Meta", META_TABLE),
    TableCase::queryable("Memory", MEMORY_TABLE),
    TableCase::queryable("LogOrdered", LOG_ORDERED_TABLE),
    TableCase::write_only("LogWrite", LOG_WRITE_TABLE),
    TableCase::queryable("Btree", BTREE_TABLE),
];

// LogWrite 当前按 HC-059 暂停行为测试。在用表入口与历史五表红测复用完全相同的
// 断言链，只缩小执行表集；不得由此删除或改写下方五表历史证据入口。
const ACTIVE_TABLE_CASES: [TableCase; 4] = [
    TableCase::queryable("Meta", META_TABLE),
    TableCase::queryable("Memory", MEMORY_TABLE),
    TableCase::queryable("LogOrdered", LOG_ORDERED_TABLE),
    TableCase::queryable("Btree", BTREE_TABLE),
];

/// 在单个真实数据库中先验证 prepared-overlap 正对照，再收集所有已提交写漏冲突红测结果。
#[test]
#[ignore = "LogWrite 行为测试按 HC-059 暂停；保留修复前五表证据和未来解冻入口"]
fn test_prepare_conflict_regression_matrix() {
    run_prepare_conflict_matrix(&TABLE_CASES, "matrix");
}

/// 当前在用表的严格转绿入口；不执行已暂停的 LogWrite 行为场景。
#[test]
fn test_prepare_conflict_active_tables() {
    run_prepare_conflict_matrix(&ACTIVE_TABLE_CASES, "active-tables");
}

fn run_prepare_conflict_matrix(table_cases: &'static [TableCase], root_label: &str) {
    let root = TempRoot::new(root_label).expect("creating conflict test root must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt, &root_path).await?;
        create_user_tables(&fixture).await?;

        for (table_index, table) in table_cases.iter().enumerate() {
            verify_prepared_overlap_control(&fixture, *table, scenario_key(table_index, 0)).await?;
        }

        let mut missing_conflicts = Vec::new();
        for (table_index, table) in table_cases.iter().enumerate() {
            verify_committed_first_insert(
                &fixture,
                *table,
                scenario_key(table_index, 1),
                &mut missing_conflicts,
            )
            .await?;
            verify_committed_same_owner_write(
                &fixture,
                *table,
                scenario_key(table_index, 2),
                &mut missing_conflicts,
            )
            .await?;
            verify_committed_aba(
                &fixture,
                *table,
                scenario_key(table_index, 3),
                &mut missing_conflicts,
            )
            .await?;
        }

        if missing_conflicts.is_empty() {
            Ok(())
        } else {
            let mut report = String::from(
                "ordinary prepare accepted stale transactions after committed same-key writes:\n",
            );
            for failure in &missing_conflicts {
                let _ = writeln!(report, "- {failure}");
            }
            Err(report)
        }
    })
    .unwrap_or_else(|error| panic!("prepare conflict regression matrix failed:\n{error}"));
}

/// 创建四类用户表；Meta 已由数据库启动流程真实加载并注册。
async fn create_user_tables(fixture: &Fixture) -> TestResult<()> {
    let transaction = transaction(&fixture.db, "conflict DDL")?;
    transaction
        .create_table(
            Atom::from(MEMORY_TABLE),
            table_meta(KVDBTableType::MemOrdTab, true),
            false,
        )
        .await
        .map_err(|error| format!("creating persistent Memory table failed: {error}"))?;
    transaction
        .create_table_with_options(
            Atom::from(LOG_ORDERED_TABLE),
            table_meta(KVDBTableType::LogOrdTab, true),
            CreateTableOptions::LogOrdTab(64 * 1024 * 1024, 1024 * 1024, 1024 * 1024),
            false,
        )
        .await
        .map_err(|error| format!("creating LogOrdered table failed: {error}"))?;
    transaction
        .create_table(
            Atom::from(LOG_WRITE_TABLE),
            table_meta(KVDBTableType::LogWTab, true),
            false,
        )
        .await
        .map_err(|error| format!("creating LogWrite table failed: {error}"))?;
    transaction
        .create_table_with_options(
            Atom::from(BTREE_TABLE),
            table_meta(KVDBTableType::BtreeOrdTab, true),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .map_err(|error| format!("creating Btree table failed: {error}"))?;

    commit_transaction(&transaction, "conflict DDL").await?;
    expect_eq(
        "table count after conflict DDL",
        &fixture.db.table_size().await,
        &5,
    )?;
    expect_eq(
        "transaction registry after conflict DDL",
        &fixture.tr_manager.transaction_len(),
        &0,
    )
}

/// 正对照：冲突事务尚在 prepare map 中时，既有预留检查必须精确拒绝第二个事务。
async fn verify_prepared_overlap_control(
    fixture: &Fixture,
    table: TableCase,
    key_id: usize,
) -> TestResult<()> {
    let label = format!("{} prepared-overlap", table.label);
    let key = encode_usize(key_id);
    let owner_value = encode_usize(10_001);
    let first = transaction(&fixture.db, &format!("{label} owner"))?;
    let second = transaction(&fixture.db, &format!("{label} contender"))?;
    first
        .upsert(vec![table_kv(table.name, key.clone(), owner_value.clone())])
        .await
        .map_err(|error| format!("{label}: owner upsert failed: {error:?}"))?;
    second
        .upsert(vec![table_kv(
            table.name,
            key.clone(),
            encode_usize(10_002),
        )])
        .await
        .map_err(|error| format!("{label}: contender upsert failed: {error:?}"))?;

    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();

    let owner_prepare = first
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("{label}: owner prepare failed: {error:?}"))?;
    expect_eq(
        &format!("{label}: owner status"),
        &first.get_status(),
        &Transaction2PcStatus::Prepared,
    )?;
    assert_expected_conflict(&second, table, &key, &label).await?;

    second
        .rollback_modified()
        .await
        .map_err(|error| format!("{label}: contender rollback failed: {error:?}"))?;
    first
        .commit_modified(owner_prepare)
        .await
        .map_err(|error| format!("{label}: owner commit failed: {error:?}"))?;
    expect_eq(
        &format!("{label}: owner committed status"),
        &first.get_status(),
        &Transaction2PcStatus::Commited,
    )?;

    assert_scenario_accounting(
        fixture,
        &label,
        produced_before,
        consumed_before,
        append_before,
        2,
        1,
    )?;
    assert_public_value(fixture, table, &key, Some(&owner_value), &label).await
}

/// 红测一：两个事务都从不存在状态建立快照，先提交者必须使后 prepare 者冲突。
async fn verify_committed_first_insert(
    fixture: &Fixture,
    table: TableCase,
    key_id: usize,
    missing_conflicts: &mut Vec<String>,
) -> TestResult<()> {
    let label = format!("{} committed-first-insert", table.label);
    let key = encode_usize(key_id);
    let winner_value = encode_usize(20_001);
    let stale_value = encode_usize(20_002);
    let winner = transaction(&fixture.db, &format!("{label} winner"))?;
    let stale = transaction(&fixture.db, &format!("{label} stale"))?;
    winner
        .upsert(vec![table_kv(
            table.name,
            key.clone(),
            winner_value.clone(),
        )])
        .await
        .map_err(|error| format!("{label}: winner upsert failed: {error:?}"))?;
    stale
        .upsert(vec![table_kv(table.name, key.clone(), stale_value.clone())])
        .await
        .map_err(|error| format!("{label}: stale upsert failed: {error:?}"))?;

    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    commit_transaction(&winner, &format!("{label} winner")).await?;
    expect_eq(
        &format!("{label}: winner status"),
        &winner.get_status(),
        &Transaction2PcStatus::Commited,
    )?;
    expect_eq(
        &format!("{label}: WAL immediately after winner"),
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;

    let stale_committed =
        observe_required_conflict(fixture, &stale, table, &key, &label, missing_conflicts).await?;
    assert_scenario_accounting(
        fixture,
        &label,
        produced_before,
        consumed_before,
        append_before,
        2,
        1 + usize::from(stale_committed),
    )?;
    let expected = if stale_committed {
        &stale_value
    } else {
        &winner_value
    };
    assert_public_value(fixture, table, &key, Some(expected), &label).await
}

/// 红测二：使用相同 `Binary` allocation 重写同值，证明引用相等快路不能替代写版本判断。
async fn verify_committed_same_owner_write(
    fixture: &Fixture,
    table: TableCase,
    key_id: usize,
    missing_conflicts: &mut Vec<String>,
) -> TestResult<()> {
    let label = format!("{} committed-same-owner-write", table.label);
    let key = encode_usize(key_id);
    let shared_value = encode_usize(30_001);
    let stale_value = encode_usize(30_002);
    seed_value(fixture, table, &key, &shared_value, &label).await?;

    let winner = transaction(&fixture.db, &format!("{label} winner"))?;
    let stale = transaction(&fixture.db, &format!("{label} stale"))?;
    winner
        .upsert(vec![table_kv(
            table.name,
            key.clone(),
            shared_value.clone(),
        )])
        .await
        .map_err(|error| format!("{label}: winner upsert failed: {error:?}"))?;
    stale
        .upsert(vec![table_kv(table.name, key.clone(), stale_value.clone())])
        .await
        .map_err(|error| format!("{label}: stale upsert failed: {error:?}"))?;

    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    commit_transaction(&winner, &format!("{label} winner")).await?;
    expect_eq(
        &format!("{label}: WAL immediately after winner"),
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;

    let stale_committed =
        observe_required_conflict(fixture, &stale, table, &key, &label, missing_conflicts).await?;
    assert_scenario_accounting(
        fixture,
        &label,
        produced_before,
        consumed_before,
        append_before,
        2,
        1 + usize::from(stale_committed),
    )?;
    let expected = if stale_committed {
        &stale_value
    } else {
        &shared_value
    };
    assert_public_value(fixture, table, &key, Some(expected), &label).await
}

/// 红测三：把当前根恢复为事务快照持有的旧 A owner，旧事务仍必须识别中间的 A->B->A。
async fn verify_committed_aba(
    fixture: &Fixture,
    table: TableCase,
    key_id: usize,
    missing_conflicts: &mut Vec<String>,
) -> TestResult<()> {
    let label = format!("{} committed-ABA", table.label);
    let key = encode_usize(key_id);
    let value_a = encode_usize(40_001);
    let value_b = encode_usize(40_002);
    let stale_value = encode_usize(40_003);
    seed_value(fixture, table, &key, &value_a, &label).await?;

    let stale = transaction(&fixture.db, &format!("{label} stale"))?;
    stale
        .upsert(vec![table_kv(table.name, key.clone(), stale_value.clone())])
        .await
        .map_err(|error| format!("{label}: stale upsert failed: {error:?}"))?;

    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();

    let write_b = transaction(&fixture.db, &format!("{label} write B"))?;
    write_b
        .upsert(vec![table_kv(table.name, key.clone(), value_b)])
        .await
        .map_err(|error| format!("{label}: B upsert failed: {error:?}"))?;
    commit_transaction(&write_b, &format!("{label} write B")).await?;

    let restore_a = transaction(&fixture.db, &format!("{label} restore A"))?;
    restore_a
        .upsert(vec![table_kv(table.name, key.clone(), value_a.clone())])
        .await
        .map_err(|error| format!("{label}: A restore upsert failed: {error:?}"))?;
    commit_transaction(&restore_a, &format!("{label} restore A")).await?;
    expect_eq(
        &format!("{label}: WAL after B/A commits"),
        &fixture.logger.append_total_count(),
        &(append_before + 2),
    )?;

    let stale_committed =
        observe_required_conflict(fixture, &stale, table, &key, &label, missing_conflicts).await?;
    assert_scenario_accounting(
        fixture,
        &label,
        produced_before,
        consumed_before,
        append_before,
        3,
        2 + usize::from(stale_committed),
    )?;
    let expected = if stale_committed {
        &stale_value
    } else {
        &value_a
    };
    assert_public_value(fixture, table, &key, Some(expected), &label).await
}

/// 提交场景的初始值，并在开始计数前确认根事务和 manager 已完整结束。
async fn seed_value(
    fixture: &Fixture,
    table: TableCase,
    key: &Binary,
    value: &Binary,
    label: &str,
) -> TestResult<()> {
    let seed = transaction(&fixture.db, &format!("{label} seed"))?;
    seed.upsert(vec![table_kv(table.name, key.clone(), value.clone())])
        .await
        .map_err(|error| format!("{label}: seed upsert failed: {error:?}"))?;
    commit_transaction(&seed, &format!("{label} seed")).await?;
    expect_eq(
        &format!("{label}: seed transaction registry"),
        &fixture.tr_manager.transaction_len(),
        &0,
    )?;
    assert_public_value(fixture, table, key, Some(value), &format!("{label} seed")).await
}

/// 对必须冲突的 stale 事务进行观察；若错误接受，只能合法 commit 清理并返回 `true`。
async fn observe_required_conflict(
    fixture: &Fixture,
    stale: &RealTransaction,
    table: TableCase,
    key: &Binary,
    label: &str,
    missing_conflicts: &mut Vec<String>,
) -> TestResult<bool> {
    let append_before_prepare = fixture.logger.append_total_count();
    match stale.prepare_modified_conflicts().await {
        Ok(prepare) => {
            expect_eq(
                &format!("{label}: unexpectedly prepared status"),
                &stale.get_status(),
                &Transaction2PcStatus::Prepared,
            )?;
            stale.commit_modified(prepare).await.map_err(|error| {
                format!("{label}: committing wrongly accepted stale transaction failed: {error:?}")
            })?;
            expect_eq(
                &format!("{label}: wrongly accepted stale commit status"),
                &stale.get_status(),
                &Transaction2PcStatus::Commited,
            )?;
            expect_eq(
                &format!("{label}: wrongly accepted stale WAL"),
                &fixture.logger.append_total_count(),
                &(append_before_prepare + 1),
            )?;
            missing_conflicts.push(format!(
                "{label}: prepare returned Ok after a committed same-key write; legal cleanup committed the stale value"
            ));
            Ok(true)
        }
        Err(error) => {
            let mismatch = conflict_mismatch(&error, table, key);
            if matches!(error.level(), ErrorLevel::Fatal) {
                return Err(format!(
                    "{label}: stale prepare returned Fatal and cannot rollback: {error:?}"
                ));
            }
            expect_eq(
                &format!("{label}: failed prepare status"),
                &stale.get_status(),
                &Transaction2PcStatus::PrepareFailed,
            )?;
            stale
                .rollback_modified()
                .await
                .map_err(|rollback| format!("{label}: stale rollback failed: {rollback:?}"))?;
            if let Some(mismatch) = mismatch {
                missing_conflicts.push(format!("{label}: {mismatch}; error={error:?}"));
            }
            expect_eq(
                &format!("{label}: stale rollback status"),
                &stale.get_status(),
                &Transaction2PcStatus::Rollbacked,
            )?;
            expect_eq(
                &format!("{label}: rejected stale path appended no WAL"),
                &fixture.logger.append_total_count(),
                &append_before_prepare,
            )?;
            Ok(false)
        }
    }
}

/// 正对照使用的严格冲突检查；任何不匹配立即说明夹具或既有 prepare 预留框架异常。
async fn assert_expected_conflict(
    transaction: &RealTransaction,
    table: TableCase,
    key: &Binary,
    label: &str,
) -> TestResult<()> {
    let error = transaction
        .prepare_modified_conflicts()
        .await
        .expect_err("prepared-overlap contender must conflict");
    if let Some(mismatch) = conflict_mismatch(&error, table, key) {
        return Err(format!("{label}: {mismatch}; error={error:?}"));
    }
    expect_eq(
        &format!("{label}: contender prepare status"),
        &transaction.get_status(),
        &Transaction2PcStatus::PrepareFailed,
    )
}

/// 返回冲突分类、表名或 Key 任一不满足冻结契约时的精确差异。
fn conflict_mismatch(error: &KVTableTrError, table: TableCase, key: &Binary) -> Option<String> {
    if !matches!(error.level(), ErrorLevel::Normal) {
        return Some(format!(
            "expected Normal conflict, observed level {:?}",
            error.level()
        ));
    }
    if !error.is_conflicts() {
        return Some("expected KVTableTrError::Conflicts".to_owned());
    }
    let Some((actual_table, actual_key)) = error.conflicts() else {
        return Some("Conflicts did not expose table/key".to_owned());
    };
    if actual_table.as_str() != table.name {
        return Some(format!(
            "expected conflict table {:?}, observed {:?}",
            table.name,
            actual_table.as_str()
        ));
    }
    if actual_key.as_ref() != key.as_ref() {
        return Some(format!(
            "expected conflict key {:?}, observed {:?}",
            key.as_ref(),
            actual_key.as_ref()
        ));
    }
    None
}

/// 验证每个场景只产生预期事务数、全部被消费、无 active 遗留且根 WAL 只含成功提交。
fn assert_scenario_accounting(
    fixture: &Fixture,
    label: &str,
    produced_before: usize,
    consumed_before: usize,
    append_before: usize,
    transaction_count: usize,
    committed_wal_count: usize,
) -> TestResult<()> {
    expect_eq(
        &format!("{label}: produced transactions"),
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + transaction_count),
    )?;
    expect_eq(
        &format!("{label}: consumed transactions"),
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + transaction_count),
    )?;
    expect_eq(
        &format!("{label}: active transaction registry"),
        &fixture.tr_manager.transaction_len(),
        &0,
    )?;
    expect_eq(
        &format!("{label}: root WAL append count"),
        &fixture.logger.append_total_count(),
        &(append_before + committed_wal_count),
    )
}

/// 对可读表按原始字节断言最终值；LogWrite 则严格断言公开 query 仍返回 `None`。
async fn assert_public_value(
    fixture: &Fixture,
    table: TableCase,
    key: &Binary,
    expected: Option<&Binary>,
    label: &str,
) -> TestResult<()> {
    let query = transaction(&fixture.db, &format!("{label} verifier"))?;
    let mut values = query
        .query(vec![TableKV::new(
            Atom::from(table.name),
            key.clone(),
            None,
        )])
        .await;
    if values.len() != 1 {
        return Err(format!(
            "{label}: expected one query slot, observed {}",
            values.len()
        ));
    }
    let actual = values.pop().expect("query result length was checked");
    drop(query);

    if !table.queryable {
        return expect_eq(
            &format!("{label}: LogWrite public query"),
            &actual.is_none(),
            &true,
        );
    }

    match (actual.as_ref(), expected) {
        (None, None) => Ok(()),
        (Some(actual), Some(expected)) if actual.as_ref() == expected.as_ref() => Ok(()),
        (actual, expected) => Err(format!(
            "{label}: final value mismatch, expected={:?}, actual={:?}",
            expected.map(BinaryBytes),
            actual.map(BinaryBytes)
        )),
    }
}

fn table_meta(table_type: KVDBTableType, persistence: bool) -> KVTableMeta {
    KVTableMeta::new(table_type, persistence, EnumType::Usize, EnumType::Usize)
}

fn table_kv(table: &str, key: Binary, value: Binary) -> TableKV {
    TableKV::new(Atom::from(table), key, Some(value))
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn scenario_key(table_index: usize, scenario_index: usize) -> usize {
    100_000 + table_index * 100 + scenario_index
}

fn transaction(db: &RealDb, source: &str) -> TestResult<RealTransaction> {
    db.transaction(Atom::from(source), true, 10_000, 10_000)
        .ok_or_else(|| format!("database rejected transaction {source}"))
}

async fn commit_transaction(transaction: &RealTransaction, label: &str) -> TestResult<()> {
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing {label} failed: {error:?}"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing {label} failed: {error:?}"))
}

async fn build_database(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<Fixture> {
    fs::create_dir_all(root)
        .map_err(|error| format!("creating conflict fixture root {root:?} failed: {error}"))?;
    let wal_path = root.join("root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building CommitLogger at {wal_path:?} failed: {error}"))?;
    let tr_manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger.clone(),
    );
    let db_path = root.join("database");
    let db = KVDBManagerBuilder::new(rt.clone(), tr_manager.clone(), &db_path)
        .startup(false)
        .await
        .map_err(|error| format!("starting conflict database at {db_path:?} failed: {error}"))?;
    Ok(Fixture {
        db,
        tr_manager,
        logger,
    })
}

/// 在独立真实 runtime 上执行 future，并以同步通道施加可诊断硬截止。
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
    .map_err(|error| format!("spawning conflict future failed: {error:?}"))?;

    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("conflict future exceeded {timeout:?}: {error}"))?
}

fn expect_eq<T: Debug + PartialEq>(label: &str, actual: &T, expected: &T) -> TestResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{label}: expected {expected:?}, observed {actual:?}"
        ))
    }
}

#[derive(Clone, Copy)]
struct TableCase {
    label: &'static str,
    name: &'static str,
    queryable: bool,
}

impl TableCase {
    const fn queryable(label: &'static str, name: &'static str) -> Self {
        Self {
            label,
            name,
            queryable: true,
        }
    }

    const fn write_only(label: &'static str, name: &'static str) -> Self {
        Self {
            label,
            name,
            queryable: false,
        }
    }
}

struct Fixture {
    db: RealDb,
    tr_manager: RealTrManager,
    logger: CommitLogger,
}

struct BinaryBytes<'a>(&'a Binary);

impl Debug for BinaryBytes<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.as_ref().fmt(formatter)
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
            "pi_db_prepare_conflict_{label}_{}_{}",
            std::process::id(),
            nanos
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
