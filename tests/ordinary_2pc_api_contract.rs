//! 普通根事务四个公开 2PC API 的真实环境契约专项。
//!
//! 本 target 使用真实 4-worker runtime、`Transaction2PcManager`、`CommitLogger`、Meta/Memory
//! 表和文件系统，不引用旧测试，也不构造内部假节点。它验证：
//!
//! - 显式只读根的推荐直接释放路径不启动 manager；当前防御性 prepare 路径则必须空 commit；
//! - 非持久化可写事务即使 token 为空也必须提交 Prepared 子树；
//! - 两种 prepare 在健康持久化写上产生等价非空 token/状态/WAL 闭环；
//! - 同一 stale 写在 generic prepare 中返回 `Common(Normal)`，在 first-conflict prepare 中
//!   返回精确 `Conflicts(Table, Key)`；
//! - 两条冲突路径都在根 WAL 前失败，rollback 后根/叶、manager、预留和新根重试完整闭合。
//!
//! 非法 token、重复/并发调用、future 取消、Fatal 合成节点和 LogWrite 不属于本 target。
//! 完整契约见 `docs/ROOT_ORDINARY_2PC_CONTRACT.md#root-ordinary-2pc-contract-index`。

use std::{
    fmt::Debug,
    fs,
    future::Future,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::{
    manager_2pc::{Transaction2PcManager, Transaction2PcStatus},
    AsyncCommitLog, ErrorLevel, Transaction2Pc, TransactionTree, UnitTransaction,
};
use pi_atom::Atom;
use pi_bon::{Decode, Encode, ReadBuffer, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    Binary, KVDBTableType, KVTableMeta,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;
type RealTrManager = Transaction2PcManager<usize, CommitLogger>;

const VOLATILE_TABLE: &str = "ordinary_2pc_volatile";
const WAL_TABLE: &str = "ordinary_2pc_wal";
const TEST_TIMEOUT: Duration = Duration::from_secs(90);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn test_ordinary_2pc_public_api_contract() {
    let root = TempRoot::new("api-contract")
        .expect("creating ordinary 2PC temporary root must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt, &root_path).await?;
        create_tables(&fixture).await?;
        verify_recommended_read_only_drop(&fixture).await?;
        verify_defensive_read_only_prepare_commit(&fixture).await?;
        verify_nonpersistent_empty_token_commit(&fixture).await?;
        verify_successful_prepare_variants(&rt, &fixture).await?;
        verify_conflict_projection_and_rollback(&rt, &fixture).await?;
        assert_manager_balanced(&fixture, "final fixture")?;
        Ok(())
    })
    .unwrap_or_else(|error| panic!("ordinary 2PC public API contract failed: {error}"));
}

async fn create_tables(fixture: &Fixture) -> TestResult<()> {
    let transaction = writable_transaction(&fixture.db, "ordinary 2PC DDL")?;
    transaction
        .create_table(
            Atom::from(VOLATILE_TABLE),
            memory_meta(false),
            false,
        )
        .await
        .map_err(|error| format!("creating volatile Memory table failed: {error:?}"))?;
    transaction
        .create_table(
            Atom::from(WAL_TABLE),
            memory_meta(true),
            false,
        )
        .await
        .map_err(|error| format!("creating WAL Memory table failed: {error:?}"))?;

    let append_before = fixture.logger.append_total_count();
    let token = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing ordinary 2PC DDL failed: {error:?}"))?;
    require(
        token.len() > 16,
        &format!("DDL token must contain Meta WAL actions, observed {} bytes", token.len()),
    )?;
    transaction
        .commit_modified(token)
        .await
        .map_err(|error| format!("committing ordinary 2PC DDL failed: {error:?}"))?;
    expect_eq(
        "DDL WAL append increment",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;
    expect_eq(
        "table count after DDL",
        &fixture.db.table_size().await,
        &3usize,
    )?;
    assert_manager_balanced(fixture, "DDL")
}

/// 推荐的显式只读闭环不调用 prepare，因而不会注册根或分配事务身份。
async fn verify_recommended_read_only_drop(fixture: &Fixture) -> TestResult<()> {
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();

    {
        let transaction = read_only_transaction(&fixture.db, "recommended read only")?;
        expect_eq(
            "recommended read-only initial status",
            &transaction.get_status(),
            &Transaction2PcStatus::Start,
        )?;
        expect_eq(
            "recommended read-only query",
            &query_one(&transaction, VOLATILE_TABLE, 1).await?,
            &None,
        )?;
        expect_eq(
            "recommended read-only child count",
            &transaction.children_len(),
            &1usize,
        )?;
        expect_eq(
            "recommended read-only TID",
            &transaction.get_transaction_uid(),
            &None,
        )?;
        expect_eq(
            "recommended read-only CID",
            &transaction.get_commit_uid(),
            &None,
        )?;
        expect_eq(
            "recommended read-only registry while alive",
            &fixture.tr_manager.transaction_len(),
            &0usize,
        )?;
    }

    expect_eq(
        "recommended read-only produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &produced_before,
    )?;
    expect_eq(
        "recommended read-only consumed count",
        &fixture.tr_manager.consumed_transaction_total(),
        &consumed_before,
    )?;
    expect_eq(
        "recommended read-only WAL count",
        &fixture.logger.append_total_count(),
        &append_before,
    )?;
    assert_manager_balanced(fixture, "recommended read-only drop")
}

/// 当前防御性只读 prepare 会登记外层根；只有空 commit 后 manager 才闭合。
async fn verify_defensive_read_only_prepare_commit(fixture: &Fixture) -> TestResult<()> {
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let transaction = read_only_transaction(&fixture.db, "defensive read only")?;
    expect_eq(
        "defensive read-only query",
        &query_one(&transaction, VOLATILE_TABLE, 2).await?,
        &None,
    )?;
    let children: Vec<RealTransaction> = transaction.to_children().collect();
    expect_eq(
        "defensive read-only child count",
        &children.len(),
        &1usize,
    )?;

    let token = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("defensive read-only prepare failed: {error:?}"))?;
    expect_eq("defensive read-only token", &token.is_empty(), &true)?;
    expect_eq(
        "defensive read-only root prepared",
        &transaction.get_status(),
        &Transaction2PcStatus::Prepared,
    )?;
    expect_eq(
        "defensive read-only child remains initialized",
        &children[0].get_status(),
        &Transaction2PcStatus::Inited,
    )?;
    expect_eq(
        "defensive read-only registry after prepare",
        &fixture.tr_manager.transaction_len(),
        &1usize,
    )?;
    expect_eq(
        "defensive read-only produced after prepare",
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        "defensive read-only consumed before commit",
        &fixture.tr_manager.consumed_transaction_total(),
        &consumed_before,
    )?;

    transaction
        .commit_modified(token)
        .await
        .map_err(|error| format!("defensive read-only commit failed: {error:?}"))?;
    expect_eq(
        "defensive read-only root committed",
        &transaction.get_status(),
        &Transaction2PcStatus::Commited,
    )?;
    expect_eq(
        "defensive read-only child still initialized",
        &children[0].get_status(),
        &Transaction2PcStatus::Inited,
    )?;
    expect_eq(
        "defensive read-only produced final",
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        "defensive read-only consumed final",
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 1),
    )?;
    expect_eq(
        "defensive read-only WAL count",
        &fixture.logger.append_total_count(),
        &append_before,
    )?;
    assert_manager_balanced(fixture, "defensive read-only commit")
}

/// 空 token 只跳过 WAL；非持久化可写 Memory 叶仍必须完整 prepare/commit。
async fn verify_nonpersistent_empty_token_commit(fixture: &Fixture) -> TestResult<()> {
    let key = 10usize;
    let value = 110usize;
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let transaction = writable_transaction(&fixture.db, "nonpersistent empty token")?;
    transaction
        .upsert(vec![kv(VOLATILE_TABLE, key, Some(value))])
        .await
        .map_err(|error| format!("nonpersistent upsert failed: {error:?}"))?;
    let children: Vec<RealTransaction> = transaction.to_children().collect();
    expect_eq("nonpersistent child count", &children.len(), &1usize)?;
    expect_eq(
        "nonpersistent root persistence",
        &transaction.is_require_persistence(),
        &false,
    )?;

    let token = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("nonpersistent prepare failed: {error:?}"))?;
    expect_eq("nonpersistent empty token", &token.is_empty(), &true)?;
    assert_root_and_children_status(
        &transaction,
        &children,
        Transaction2PcStatus::Prepared,
        "nonpersistent prepared",
    )?;
    expect_eq(
        "nonpersistent registry after prepare",
        &fixture.tr_manager.transaction_len(),
        &1usize,
    )?;
    expect_eq(
        "nonpersistent WAL before commit",
        &fixture.logger.append_total_count(),
        &append_before,
    )?;

    transaction
        .commit_modified(token)
        .await
        .map_err(|error| format!("nonpersistent commit failed: {error:?}"))?;
    assert_root_and_children_status(
        &transaction,
        &children,
        Transaction2PcStatus::Commited,
        "nonpersistent committed",
    )?;
    expect_eq(
        "nonpersistent produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        "nonpersistent consumed count",
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 1),
    )?;
    expect_eq(
        "nonpersistent WAL after commit",
        &fixture.logger.append_total_count(),
        &append_before,
    )?;
    expect_eq(
        "nonpersistent final value",
        &query_fresh(&fixture.db, VOLATILE_TABLE, key).await?,
        &Some(value),
    )?;
    assert_manager_balanced(fixture, "nonpersistent empty-token commit")
}

/// 两种 prepare 的健康路径必须形成相同状态、身份、WAL 和最终值闭环。
async fn verify_successful_prepare_variants(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
) -> TestResult<()> {
    commit_persistent_value(
        rt,
        fixture,
        20,
        220,
        PrepareVariant::Generic,
        "generic success",
    )
    .await?;
    commit_persistent_value(
        rt,
        fixture,
        21,
        221,
        PrepareVariant::FirstConflict,
        "first-conflict success",
    )
    .await
}

async fn commit_persistent_value(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
    key: usize,
    value: usize,
    variant: PrepareVariant,
    label: &str,
) -> TestResult<()> {
    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let confirm_before = fixture.logger.confirm_total_count();
    let waiting_before = fixture.logger.waiting_confirm_count().await;
    let transaction = writable_transaction(&fixture.db, label)?;
    transaction
        .upsert(vec![kv(WAL_TABLE, key, Some(value))])
        .await
        .map_err(|error| format!("{label} upsert failed: {error:?}"))?;
    let children: Vec<RealTransaction> = transaction.to_children().collect();
    expect_eq(&format!("{label} child count"), &children.len(), &1usize)?;
    expect_eq(
        &format!("{label} root persistence"),
        &transaction.is_require_persistence(),
        &true,
    )?;

    let token = prepare(&transaction, variant)
        .await
        .map_err(|error| format!("{label} prepare failed: {error:?}"))?;
    require(
        token.len() > 16,
        &format!("{label} token must contain Memory WAL action, observed {} bytes", token.len()),
    )?;
    assert_root_and_children_status(
        &transaction,
        &children,
        Transaction2PcStatus::Prepared,
        &format!("{label} prepared"),
    )?;
    assert_shared_identity(&transaction, &children, label)?;
    expect_eq(
        &format!("{label} WAL before commit"),
        &fixture.logger.append_total_count(),
        &append_before,
    )?;
    let commit_uid = transaction
        .get_commit_uid()
        .ok_or_else(|| format!("{label} persistent root has no commit UID"))?;

    transaction
        .commit_modified(token)
        .await
        .map_err(|error| format!("{label} commit failed: {error:?}"))?;
    assert_root_and_children_status(
        &transaction,
        &children,
        Transaction2PcStatus::Commited,
        &format!("{label} committed"),
    )?;
    expect_eq(
        &format!("{label} produced count"),
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        &format!("{label} consumed count"),
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 1),
    )?;
    expect_eq(
        &format!("{label} WAL increment"),
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;
    expect_eq(
        &format!("{label} final value"),
        &query_fresh(&fixture.db, WAL_TABLE, key).await?,
        &Some(value),
    )?;
    wait_for_confirmation(
        rt,
        &fixture.logger,
        commit_uid,
        confirm_before,
        waiting_before,
        label,
    )
    .await?;
    assert_manager_balanced(fixture, label)
}

/// 对相同 stale 写比较两个 prepare 的唯一公开差异，并验证 rollback 释放表级预留。
async fn verify_conflict_projection_and_rollback(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
) -> TestResult<()> {
    let key = 30usize;
    commit_persistent_value(
        rt,
        fixture,
        key,
        300,
        PrepareVariant::FirstConflict,
        "conflict seed",
    )
    .await?;

    let generic = writable_transaction(&fixture.db, "generic stale loser")?;
    generic
        .upsert(vec![kv(WAL_TABLE, key, Some(301))])
        .await
        .map_err(|error| format!("generic stale upsert failed: {error:?}"))?;
    let generic_children: Vec<RealTransaction> = generic.to_children().collect();

    let first = writable_transaction(&fixture.db, "first stale loser")?;
    first
        .upsert(vec![kv(WAL_TABLE, key, Some(302))])
        .await
        .map_err(|error| format!("first stale upsert failed: {error:?}"))?;
    let first_children: Vec<RealTransaction> = first.to_children().collect();

    let produced_before = fixture.tr_manager.produced_transaction_total();
    let consumed_before = fixture.tr_manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let winner = writable_transaction(&fixture.db, "conflict winner")?;
    winner
        .upsert(vec![kv(WAL_TABLE, key, Some(303))])
        .await
        .map_err(|error| format!("conflict winner upsert failed: {error:?}"))?;
    commit_with_confirmation(
        rt,
        fixture,
        &winner,
        PrepareVariant::FirstConflict,
        "conflict winner",
    )
    .await?;
    expect_eq(
        "conflict winner authoritative value",
        &query_fresh(&fixture.db, WAL_TABLE, key).await?,
        &Some(303),
    )?;

    let generic_error = generic
        .prepare_modified()
        .await
        .expect_err("generic stale prepare must fail");
    require(
        generic_error.is_common(),
        &format!("generic stale prepare must return Common, observed {generic_error:?}"),
    )?;
    require(
        matches!(generic_error.level(), ErrorLevel::Normal),
        &format!("generic stale prepare must be Normal, observed {generic_error:?}"),
    )?;
    expect_eq(
        "generic stale conflict accessor",
        &generic_error.conflicts().is_none(),
        &true,
    )?;
    assert_failed_prepare(
        fixture,
        &generic,
        &generic_children,
        "generic stale prepare",
    )?;
    generic
        .rollback_modified()
        .await
        .map_err(|error| format!("generic stale rollback failed: {error:?}"))?;
    assert_rolled_back(
        fixture,
        &generic,
        &generic_children,
        "generic stale rollback",
    )?;

    let first_error = first
        .prepare_modified_conflicts()
        .await
        .expect_err("first-conflict stale prepare must fail");
    require(
        !first_error.is_common() && first_error.is_conflicts() && !first_error.is_all_conflicts(),
        &format!("first stale prepare must return only Conflicts, observed {first_error:?}"),
    )?;
    require(
        matches!(first_error.level(), ErrorLevel::Normal),
        &format!("first stale prepare must be Normal, observed {first_error:?}"),
    )?;
    let (table, conflict_key) = first_error
        .conflicts()
        .ok_or_else(|| "first stale error did not expose table/key".to_owned())?;
    expect_eq("first stale conflict table", &table.as_str(), &WAL_TABLE)?;
    expect_eq(
        "first stale conflict key",
        &conflict_key.as_ref(),
        &encode_usize(key).as_ref(),
    )?;
    assert_failed_prepare(
        fixture,
        &first,
        &first_children,
        "first stale prepare",
    )?;
    first
        .rollback_modified()
        .await
        .map_err(|error| format!("first stale rollback failed: {error:?}"))?;
    assert_rolled_back(
        fixture,
        &first,
        &first_children,
        "first stale rollback",
    )?;

    expect_eq(
        "conflict paths produced before retry",
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 3),
    )?;
    expect_eq(
        "conflict paths consumed before retry",
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 3),
    )?;
    expect_eq(
        "conflict paths WAL before retry",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;
    expect_eq(
        "conflict rollback preserves winner",
        &query_fresh(&fixture.db, WAL_TABLE, key).await?,
        &Some(303),
    )?;

    // generic/first 失败根及叶子仍保持 Arc 存活时，新根必须取得新身份并成功使用同一 Key。
    let generic_tid = generic
        .get_transaction_uid()
        .ok_or_else(|| "generic stale root has no TID".to_owned())?;
    let first_tid = first
        .get_transaction_uid()
        .ok_or_else(|| "first stale root has no TID".to_owned())?;
    let retry = writable_transaction(&fixture.db, "post-rollback retry")?;
    retry
        .upsert(vec![kv(WAL_TABLE, key, Some(304))])
        .await
        .map_err(|error| format!("post-rollback retry upsert failed: {error:?}"))?;
    let retry_children: Vec<RealTransaction> = retry.to_children().collect();
    let confirm_before = fixture.logger.confirm_total_count();
    let waiting_before = fixture.logger.waiting_confirm_count().await;
    let retry_token = retry
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("post-rollback retry prepare failed: {error:?}"))?;
    let retry_tid = retry
        .get_transaction_uid()
        .ok_or_else(|| "post-rollback retry has no TID".to_owned())?;
    require(
        retry_tid != generic_tid && retry_tid != first_tid,
        "post-rollback retry reused a failed transaction TID",
    )?;
    assert_root_and_children_status(
        &retry,
        &retry_children,
        Transaction2PcStatus::Prepared,
        "post-rollback retry prepared",
    )?;
    let retry_cid = retry
        .get_commit_uid()
        .ok_or_else(|| "post-rollback retry has no CID".to_owned())?;
    retry
        .commit_modified(retry_token)
        .await
        .map_err(|error| format!("post-rollback retry commit failed: {error:?}"))?;
    assert_root_and_children_status(
        &retry,
        &retry_children,
        Transaction2PcStatus::Commited,
        "post-rollback retry committed",
    )?;
    expect_eq(
        "conflict paths final produced count",
        &fixture.tr_manager.produced_transaction_total(),
        &(produced_before + 4),
    )?;
    expect_eq(
        "conflict paths final consumed count",
        &fixture.tr_manager.consumed_transaction_total(),
        &(consumed_before + 4),
    )?;
    expect_eq(
        "conflict paths final WAL increment",
        &fixture.logger.append_total_count(),
        &(append_before + 2),
    )?;
    expect_eq(
        "post-rollback retry final value",
        &query_fresh(&fixture.db, WAL_TABLE, key).await?,
        &Some(304),
    )?;
    wait_for_confirmation(
        rt,
        &fixture.logger,
        retry_cid,
        confirm_before,
        waiting_before,
        "post-rollback retry",
    )
    .await?;
    assert_manager_balanced(fixture, "conflict projection and rollback")
}

async fn commit_with_confirmation(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
    transaction: &RealTransaction,
    variant: PrepareVariant,
    label: &str,
) -> TestResult<()> {
    let append_before = fixture.logger.append_total_count();
    let confirm_before = fixture.logger.confirm_total_count();
    let waiting_before = fixture.logger.waiting_confirm_count().await;
    let token = prepare(transaction, variant)
        .await
        .map_err(|error| format!("{label} prepare failed: {error:?}"))?;
    require(
        token.len() > 16,
        &format!("{label} token must be nonempty, observed {} bytes", token.len()),
    )?;
    let commit_uid = transaction
        .get_commit_uid()
        .ok_or_else(|| format!("{label} has no commit UID"))?;
    transaction
        .commit_modified(token)
        .await
        .map_err(|error| format!("{label} commit failed: {error:?}"))?;
    expect_eq(
        &format!("{label} WAL increment"),
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;
    wait_for_confirmation(
        rt,
        &fixture.logger,
        commit_uid,
        confirm_before,
        waiting_before,
        label,
    )
    .await
}

async fn prepare(
    transaction: &RealTransaction,
    variant: PrepareVariant,
) -> Result<Vec<u8>, pi_db::KVTableTrError> {
    match variant {
        PrepareVariant::Generic => transaction.prepare_modified().await,
        PrepareVariant::FirstConflict => transaction.prepare_modified_conflicts().await,
    }
}

fn assert_failed_prepare(
    fixture: &Fixture,
    transaction: &RealTransaction,
    children: &[RealTransaction],
    label: &str,
) -> TestResult<()> {
    assert_root_and_children_status(
        transaction,
        children,
        Transaction2PcStatus::PrepareFailed,
        label,
    )?;
    expect_eq(
        &format!("{label} active root"),
        &fixture.tr_manager.transaction_len(),
        &1usize,
    )
}

fn assert_rolled_back(
    fixture: &Fixture,
    transaction: &RealTransaction,
    children: &[RealTransaction],
    label: &str,
) -> TestResult<()> {
    assert_root_and_children_status(
        transaction,
        children,
        Transaction2PcStatus::Rollbacked,
        label,
    )?;
    assert_manager_balanced(fixture, label)
}

fn assert_root_and_children_status(
    transaction: &RealTransaction,
    children: &[RealTransaction],
    expected: Transaction2PcStatus,
    label: &str,
) -> TestResult<()> {
    expect_eq(
        &format!("{label} root status"),
        &transaction.get_status(),
        &expected,
    )?;
    for (index, child) in children.iter().enumerate() {
        expect_eq(
            &format!("{label} child {index} status"),
            &child.get_status(),
            &expected,
        )?;
    }
    Ok(())
}

fn assert_shared_identity(
    transaction: &RealTransaction,
    children: &[RealTransaction],
    label: &str,
) -> TestResult<()> {
    let tid = transaction
        .get_transaction_uid()
        .ok_or_else(|| format!("{label} root has no TID"))?;
    let cid = transaction
        .get_commit_uid()
        .ok_or_else(|| format!("{label} root has no CID"))?;
    for (index, child) in children.iter().enumerate() {
        expect_eq(
            &format!("{label} child {index} TID"),
            &child.get_transaction_uid(),
            &Some(tid.clone()),
        )?;
        expect_eq(
            &format!("{label} child {index} CID"),
            &child.get_commit_uid(),
            &Some(cid.clone()),
        )?;
    }
    Ok(())
}

fn assert_manager_balanced(fixture: &Fixture, label: &str) -> TestResult<()> {
    expect_eq(
        &format!("{label} active roots"),
        &fixture.tr_manager.transaction_len(),
        &0usize,
    )?;
    expect_eq(
        &format!("{label} manager totals"),
        &fixture.tr_manager.produced_transaction_total(),
        &fixture.tr_manager.consumed_transaction_total(),
    )
}

async fn wait_for_confirmation(
    rt: &MultiTaskRuntime<()>,
    logger: &CommitLogger,
    commit_uid: pi_guid::Guid,
    confirm_before: usize,
    waiting_before: usize,
    label: &str,
) -> TestResult<()> {
    let deadline = Instant::now() + CONFIRM_TIMEOUT;
    let expected_confirmed = confirm_before
        .checked_add(1)
        .ok_or_else(|| format!("{label} confirmation counter overflow"))?;
    loop {
        let checkpoint = logger.check_point_of(commit_uid.clone()).await;
        let confirmed = logger.confirm_total_count();
        let waiting = logger.waiting_confirm_count().await;
        if checkpoint.is_none()
            && confirmed == expected_confirmed
            && waiting == waiting_before
        {
            return Ok(());
        }
        if confirmed > expected_confirmed {
            return Err(format!(
                "{label} observed unrelated confirmation progress: expected={expected_confirmed}, observed={confirmed}"
            ));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{label} confirmation exceeded {CONFIRM_TIMEOUT:?}: commit_uid={commit_uid:?}, checkpoint={checkpoint:?}, confirmed_before={confirm_before}, confirmed={confirmed}, waiting_before={waiting_before}, waiting={waiting}"
            ));
        }
        rt.timeout(1).await;
    }
}

async fn query_fresh(db: &RealDb, table: &str, key: usize) -> TestResult<Option<usize>> {
    let transaction = read_only_transaction(db, "ordinary 2PC fresh query")?;
    query_one(&transaction, table, key).await
}

async fn query_one(
    transaction: &RealTransaction,
    table: &str,
    key: usize,
) -> TestResult<Option<usize>> {
    let mut values = transaction.query(vec![kv(table, key, None)]).await;
    if values.len() != 1 {
        return Err(format!(
            "query {table}/{key} returned {} slots instead of one",
            values.len(),
        ));
    }
    match values.pop().expect("query length checked above") {
        Some(value) => decode_usize(&value).map(Some),
        None => Ok(None),
    }
}

fn kv(table: &str, key: usize, value: Option<usize>) -> TableKV {
    TableKV::new(
        Atom::from(table),
        encode_usize(key),
        value.map(encode_usize),
    )
}

fn memory_meta(persistence: bool) -> KVTableMeta {
    KVTableMeta::new(
        KVDBTableType::MemOrdTab,
        persistence,
        EnumType::Usize,
        EnumType::Usize,
    )
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn decode_usize(value: &Binary) -> TestResult<usize> {
    let mut buffer = ReadBuffer::new(value.as_ref(), 0);
    usize::decode(&mut buffer)
        .map_err(|error| format!("decoding BON usize failed: {error:?}"))
}

fn writable_transaction(db: &RealDb, source: &str) -> TestResult<RealTransaction> {
    transaction(db, source, true)
}

fn read_only_transaction(db: &RealDb, source: &str) -> TestResult<RealTransaction> {
    transaction(db, source, false)
}

fn transaction(db: &RealDb, source: &str, writable: bool) -> TestResult<RealTransaction> {
    db.transaction(Atom::from(source), writable, 10_000, 10_000)
        .ok_or_else(|| format!("database rejected transaction {source}"))
}

async fn build_database(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
) -> TestResult<Fixture> {
    fs::create_dir_all(root)
        .map_err(|error| format!("creating ordinary 2PC root {root:?} failed: {error}"))?;
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
        .key_version_ttl(Duration::ZERO)
        .startup(false)
        .await
        .map_err(|error| format!("starting ordinary 2PC database at {db_path:?} failed: {error}"))?;
    Ok(Fixture {
        db,
        tr_manager,
        logger,
    })
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
    let (sender, receiver) = bounded(1);
    rt.spawn(async move {
        let _ = sender.send(future.await);
    })
    .map_err(|error| format!("spawning ordinary 2PC future failed: {error:?}"))?;
    receiver
        .recv_timeout(timeout)
        .map_err(|error| format!("ordinary 2PC future exceeded {timeout:?}: {error}"))?
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
            "{label}: expected {expected:?}, observed {actual:?}",
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

#[derive(Clone, Copy)]
enum PrepareVariant {
    Generic,
    FirstConflict,
}

struct Fixture {
    db: RealDb,
    tr_manager: RealTrManager,
    logger: CommitLogger,
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
            "pi_db_ordinary_2pc_{label}_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path)
            .map_err(|error| format!("creating ordinary 2PC temporary root {path:?} failed: {error}"))?;
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
