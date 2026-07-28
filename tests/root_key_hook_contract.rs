//! 根事务 `lock_key/unlock_key` 当前兼容钩子的真实公开契约专项。
//!
//! 本 target 不引用或运行旧测试。它使用真实 4-worker runtime、事务管理器、根
//! `CommitLogger`、Memory/LogOrdered/Btree、真实文件系统和表持久化任务，严格验证：
//!
//! - 当前钩子不建立排他、等待、owner、重入或内存可见性关系；
//! - 缺表成功但仍选择 Ordinary，已存在表创建每表唯一的非持久化 managed 子事务；
//! - 首次触表会固定表数据快照和版本 revision 租约，后续普通写复用该冲突基线；
//! - 纯钩子可写根必须完成空普通 2PC，但不写 WAL、不发布数据或版本；
//! - 空动作提交不能把并发事务已经发布的新根覆盖回旧快照；
//! - 钩子后续普通写会提升 persistence，并保持既有 prepare/commit/rollback 语义；
//! - 最短及 `u16::MAX` 长度的合法 BON Key 都不会被钩子解释；
//! - 多个独立根可在同一 Key 上同时完成“锁定”，从运行时证明该 API 不能用于并发正确性。
//!
//! Meta 只能通过专用 DDL API 操作，LogWrite 当前不允许外部使用；两者的 trivial no-op
//! 实现只作源码和文档核验，不进入本公开动态矩阵。完整契约见
//! `docs/ROOT_KEY_HOOK_CONTRACT.md#root-key-hook-contract-index`。

use std::{
    env,
    fmt::Debug,
    fs,
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::{TryRecvError, bounded};
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop,
    AsyncRuntime,
};
use pi_async_transaction::{
    manager_2pc::{Transaction2PcManager, Transaction2PcStatus},
    AsyncCommitLog,
    AsyncTransaction,
    ErrorLevel,
    Transaction2Pc,
    TransactionTree,
    UnitTransaction,
};
use pi_atom::Atom;
use pi_bon::WriteBuffer;
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    utils::CreateTableOptions,
    Binary,
    KVDBTableType,
    KVTableMeta,
    KVTableTrError,
    Version,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type RealManager = Transaction2PcManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const MEMORY_TABLE: &str = "root_key_hook_memory";
const LOG_ORDERED_TABLE: &str = "root_key_hook_log_ordered";
const BTREE_TABLE: &str = "root_key_hook_btree";
const MISSING_TABLE: &str = "root_key_hook_missing";
const MATRIX_TIMEOUT: Duration = Duration::from_secs(120);
const CONCURRENCY_TIMEOUT: Duration = Duration::from_secs(45);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(85);
const CONCURRENT_ROOTS: usize = 8;

const TABLE_CASES: [TableCase; 3] = [
    TableCase::new("Memory", MEMORY_TABLE, TableKind::Memory),
    TableCase::new("LogOrdered", LOG_ORDERED_TABLE, TableKind::LogOrdered),
    TableCase::new("Btree", BTREE_TABLE, TableKind::Btree),
];

#[derive(Clone, Copy)]
struct TableCase {
    label: &'static str,
    name: &'static str,
    kind: TableKind,
}

impl TableCase {
    const fn new(label: &'static str,
                 name: &'static str,
                 kind: TableKind) -> Self {
        Self { label, name, kind }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TableKind {
    Memory,
    LogOrdered,
    Btree,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Observation {
    value: Option<Binary>,
    version: Version,
}

struct Fixture {
    db: RealDb,
    manager: RealManager,
    logger: CommitLogger,
}

#[test]
fn test_root_key_hook_contract_matrix() {
    let root = TempRoot::new("matrix")
        .expect("creating root-key-hook matrix root must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(MATRIX_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt, &root_path).await?;
        create_tables(&fixture).await?;
        seed_values(&fixture).await?;

        verify_unprepared_and_read_only_hooks(&fixture).await?;
        verify_missing_table_protocol_selection(&fixture).await?;
        verify_lock_only_2pc(&fixture).await?;
        verify_empty_commit_does_not_revert_newer_roots(&fixture).await?;
        verify_pre_touch_conflict_and_rollback(&fixture).await?;
        verify_hook_then_successful_write(&fixture).await?;

        settle_persistent_tables(&rt, &fixture, "final state").await?;
        expect_eq(
            "final manager produced/consumed balance",
            &fixture.manager.produced_transaction_total(),
            &fixture.manager.consumed_transaction_total(),
        )?;
        expect_eq(
            "final manager active roots",
            &fixture.manager.transaction_len(),
            &0usize,
        )?;
        expect_eq(
            "final confirmed WAL count",
            &fixture.logger.confirm_total_count(),
            &fixture.logger.append_total_count(),
        )?;
        expect_eq(
            "final waiting WAL count",
            &fixture.logger.waiting_confirm_count().await,
            &0usize,
        )
    })
    .unwrap_or_else(|error| panic!("root-key-hook contract matrix failed: {error}"));
}

#[test]
fn test_root_key_hook_concurrency_safety() {
    let root = TempRoot::new("concurrency")
        .expect("creating root-key-hook concurrency root must succeed");
    let root_path = root.path().to_path_buf();

    run_on_runtime(CONCURRENCY_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt, &root_path).await?;
        create_memory_table(&fixture).await?;

        let produced_before = fixture.manager.produced_transaction_total();
        let consumed_before = fixture.manager.consumed_transaction_total();
        let append_before = fixture.logger.append_total_count();
        let ready = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let (result_tx, result_rx) = bounded(CONCURRENT_ROOTS);
        let shared_key = key("concurrent-noop");

        for index in 0..CONCURRENT_ROOTS {
            let db = fixture.db.clone();
            let task_rt = rt.clone();
            let task_ready = ready.clone();
            let task_release = release.clone();
            let task_result = result_tx.clone();
            let task_key = shared_key.clone();
            rt.spawn(async move {
                let result: TestResult<()> = async {
                    let transaction = writable_transaction(
                        &db,
                        &format!("root-key-hook concurrent owner {index}"),
                    )?;
                    transaction
                        .lock_key(Atom::from(MEMORY_TABLE), task_key.clone())
                        .await
                        .map_err(|error| {
                            format!("concurrent owner {index} lock failed: {error:?}")
                        })?;
                    expect_eq(
                        &format!("concurrent owner {index} child count"),
                        &transaction.children_len(),
                        &1usize,
                    )?;
                    task_ready.fetch_add(1, Ordering::SeqCst);
                    while !task_release.load(Ordering::SeqCst) {
                        task_rt.timeout(1).await;
                    }
                    transaction
                        .unlock_key(Atom::from(MEMORY_TABLE), task_key)
                        .await
                        .map_err(|error| {
                            format!("concurrent owner {index} unlock failed: {error:?}")
                        })?;
                    drop(transaction);
                    Ok(())
                }.await;
                let _ = task_result.send((index, result));
            })
            .map_err(|error| {
                format!("spawning root-key-hook concurrent owner {index} failed: {error:?}")
            })?;
        }
        drop(result_tx);

        let ready_deadline = Instant::now() + Duration::from_secs(10);
        while ready.load(Ordering::SeqCst) != CONCURRENT_ROOTS {
            if Instant::now() >= ready_deadline {
                release.store(true, Ordering::SeqCst);
                return Err(format!(
                    "only {} of {} independent roots completed lock_key before any unlock",
                    ready.load(Ordering::SeqCst),
                    CONCURRENT_ROOTS,
                ));
            }
            rt.timeout(1).await;
        }
        release.store(true, Ordering::SeqCst);

        let completion_deadline = Instant::now() + Duration::from_secs(10);
        let mut completed = vec![false; CONCURRENT_ROOTS];
        let mut completed_count = 0;
        while completed_count != CONCURRENT_ROOTS {
            match result_rx.try_recv() {
                Ok((index, result)) => {
                    if index >= CONCURRENT_ROOTS {
                        return Err(format!(
                            "concurrent hook returned invalid owner index {index}",
                        ));
                    }
                    if completed[index] {
                        return Err(format!(
                            "concurrent hook returned owner {index} more than once",
                        ));
                    }
                    result?;
                    completed[index] = true;
                    completed_count += 1;
                },
                Err(TryRecvError::Empty) => {
                    if Instant::now() >= completion_deadline {
                        return Err(format!(
                            "only {completed_count} of {CONCURRENT_ROOTS} concurrent owners completed unlock",
                        ));
                    }
                    // 不能在 runtime worker 内使用阻塞式 recv_timeout：spawn 进入当前 worker
                    // 的本地队列时，阻塞该 worker 会让尚未被偷取的 owner 无法继续执行。
                    rt.timeout(1).await;
                },
                Err(TryRecvError::Disconnected) => {
                    return Err(format!(
                        "concurrent result channel disconnected after {completed_count} owners",
                    ));
                },
            }
        }
        expect_eq(
            "unprepared concurrent roots produced count",
            &fixture.manager.produced_transaction_total(),
            &produced_before,
        )?;
        expect_eq(
            "unprepared concurrent roots consumed count",
            &fixture.manager.consumed_transaction_total(),
            &consumed_before,
        )?;
        expect_eq(
            "unprepared concurrent roots active count",
            &fixture.manager.transaction_len(),
            &0usize,
        )?;
        expect_eq(
            "unprepared concurrent roots WAL count",
            &fixture.logger.append_total_count(),
            &append_before,
        )
    })
    .unwrap_or_else(|error| panic!("root-key-hook concurrency safety failed: {error}"));
}

async fn create_tables(fixture: &Fixture) -> TestResult<()> {
    let transaction = writable_transaction(&fixture.db, "root-key-hook table DDL")?;
    transaction
        .create_table(
            Atom::from(MEMORY_TABLE),
            table_meta(KVDBTableType::MemOrdTab, true),
            false,
        )
        .await
        .map_err(|error| format!("creating root-key-hook Memory table failed: {error}"))?;
    transaction
        .create_table_with_options(
            Atom::from(LOG_ORDERED_TABLE),
            table_meta(KVDBTableType::LogOrdTab, true),
            CreateTableOptions::LogOrdTab(
                64 * 1024 * 1024,
                1024 * 1024,
                1024 * 1024,
            ),
            false,
        )
        .await
        .map_err(|error| {
            format!("creating root-key-hook LogOrdered table failed: {error}")
        })?;
    transaction
        .create_table_with_options(
            Atom::from(BTREE_TABLE),
            table_meta(KVDBTableType::BtreeOrdTab, true),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .map_err(|error| format!("creating root-key-hook Btree table failed: {error}"))?;
    commit_ordinary(&transaction, "root-key-hook table DDL").await
}

async fn create_memory_table(fixture: &Fixture) -> TestResult<()> {
    let transaction = writable_transaction(&fixture.db, "root-key-hook concurrency DDL")?;
    transaction
        .create_table(
            Atom::from(MEMORY_TABLE),
            table_meta(KVDBTableType::MemOrdTab, false),
            false,
        )
        .await
        .map_err(|error| {
            format!("creating concurrency Memory table failed: {error}")
        })?;
    commit_ordinary(&transaction, "root-key-hook concurrency DDL").await
}

async fn seed_values(fixture: &Fixture) -> TestResult<()> {
    let transaction = writable_transaction(&fixture.db, "root-key-hook baseline seed")?;
    let mut input = Vec::new();
    for table in TABLE_CASES {
        for name in ["stable", "stale-target", "conflict-target"] {
            input.push(TableKV::new(
                Atom::from(table.name),
                key(name),
                Some(value(&format!("{}-{name}-baseline", table.label))),
            ));
        }
    }
    transaction
        .upsert(input)
        .await
        .map_err(|error| format!("seeding root-key-hook values failed: {error:?}"))?;
    commit_ordinary(&transaction, "root-key-hook baseline seed").await
}

async fn verify_unprepared_and_read_only_hooks(fixture: &Fixture) -> TestResult<()> {
    let produced_before = fixture.manager.produced_transaction_total();
    let consumed_before = fixture.manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let confirm_before = fixture.logger.confirm_total_count();
    let minimum = minimum_key();
    let maximum = maximum_key();

    for table in TABLE_CASES {
        let owner = writable_transaction(
            &fixture.db,
            &format!("{} unprepared hook owner", table.label),
        )?;
        owner
            .lock_key(Atom::from(table.name), minimum.clone())
            .await
            .map_err(|error| format!("{} minimum lock failed: {error:?}", table.label))?;
        owner
            .lock_key(Atom::from(table.name), maximum.clone())
            .await
            .map_err(|error| format!("{} repeated maximum lock failed: {error:?}", table.label))?;
        owner
            .unlock_key(Atom::from(table.name), maximum.clone())
            .await
            .map_err(|error| format!("{} maximum unlock failed: {error:?}", table.label))?;
        owner
            .unlock_key(Atom::from(table.name), minimum.clone())
            .await
            .map_err(|error| format!("{} repeated unlock failed: {error:?}", table.label))?;
        let children = owner.to_children().collect::<Vec<_>>();
        assert_children(
            &children,
            &[table.kind],
            true,
            false,
            Transaction2PcStatus::Start,
            &format!("{} unprepared hook owner", table.label),
        )?;
        drop(owner);
    }

    let reader = read_only_transaction(&fixture.db, "root-key-hook read-only owner")?;
    reader
        .unlock_key(Atom::from(BTREE_TABLE), minimum.clone())
        .await
        .map_err(|error| format!("read-only Btree unlock failed: {error:?}"))?;
    reader
        .lock_key(Atom::from(MEMORY_TABLE), maximum.clone())
        .await
        .map_err(|error| format!("read-only Memory lock failed: {error:?}"))?;
    reader
        .lock_key(Atom::from(LOG_ORDERED_TABLE), minimum.clone())
        .await
        .map_err(|error| format!("read-only LogOrdered lock failed: {error:?}"))?;
    let reader_children = reader.to_children().collect::<Vec<_>>();
    assert_children(
        &reader_children,
        &[TableKind::Btree, TableKind::Memory, TableKind::LogOrdered],
        false,
        false,
        Transaction2PcStatus::Start,
        "read-only hook owner",
    )?;
    drop(reader);

    let max_missing_name = "m".repeat(4096);
    let missing = writable_transaction(&fixture.db, "root-key-hook maximum missing name")?;
    missing
        .lock_key(Atom::from(max_missing_name.as_str()), minimum)
        .await
        .map_err(|error| format!("maximum valid missing table lock failed: {error:?}"))?;
    missing
        .unlock_key(Atom::from(max_missing_name.as_str()), maximum)
        .await
        .map_err(|error| format!("maximum valid missing table unlock failed: {error:?}"))?;
    expect_eq(
        "maximum valid missing table child count",
        &missing.children_len(),
        &0usize,
    )?;
    drop(missing);

    expect_eq(
        "unprepared/read-only produced count",
        &fixture.manager.produced_transaction_total(),
        &produced_before,
    )?;
    expect_eq(
        "unprepared/read-only consumed count",
        &fixture.manager.consumed_transaction_total(),
        &consumed_before,
    )?;
    expect_eq(
        "unprepared/read-only active roots",
        &fixture.manager.transaction_len(),
        &0usize,
    )?;
    expect_eq(
        "unprepared/read-only WAL append count",
        &fixture.logger.append_total_count(),
        &append_before,
    )?;
    expect_eq(
        "unprepared/read-only WAL confirm count",
        &fixture.logger.confirm_total_count(),
        &confirm_before,
    )
}

async fn verify_missing_table_protocol_selection(fixture: &Fixture) -> TestResult<()> {
    let produced_before = fixture.manager.produced_transaction_total();
    let consumed_before = fixture.manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let transaction = writable_transaction(&fixture.db, "root-key-hook missing protocol")?;

    transaction
        .lock_key(Atom::from(MISSING_TABLE), key("missing-lock"))
        .await
        .map_err(|error| format!("missing-table lock failed: {error:?}"))?;
    transaction
        .unlock_key(Atom::from(MISSING_TABLE), key("missing-unlock"))
        .await
        .map_err(|error| format!("missing-table unlock failed: {error:?}"))?;
    expect_eq(
        "missing-table hook child count",
        &transaction.children_len(),
        &0usize,
    )?;
    expect_eq(
        "missing-table hook root persistence",
        &transaction.is_require_persistence(),
        &false,
    )?;

    let error = transaction
        .prepare_with_version(Vec::new(), Vec::new())
        .await
        .expect_err("missing-table hook must select Ordinary before lookup");
    if !error.is_common() || !matches!(error.level(), ErrorLevel::Normal) {
        return Err(format!(
            "missing-table Ordinary/Versioned rejection returned wrong error: {error:?}",
        ));
    }
    expect_eq(
        "missing-table rejected root status",
        &transaction.get_status(),
        &Transaction2PcStatus::Start,
    )?;
    require(
        transaction.get_transaction_uid().is_none(),
        "missing-table protocol rejection unexpectedly registered a TID",
    )?;
    drop(transaction);

    expect_eq(
        "missing-table protocol produced count",
        &fixture.manager.produced_transaction_total(),
        &produced_before,
    )?;
    expect_eq(
        "missing-table protocol consumed count",
        &fixture.manager.consumed_transaction_total(),
        &consumed_before,
    )?;
    expect_eq(
        "missing-table protocol WAL count",
        &fixture.logger.append_total_count(),
        &append_before,
    )
}

async fn verify_lock_only_2pc(fixture: &Fixture) -> TestResult<()> {
    let before = observe_all(&fixture.db, "stable").await?;
    let produced_before = fixture.manager.produced_transaction_total();
    let consumed_before = fixture.manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let confirm_before = fixture.logger.confirm_total_count();
    let waiting_before = fixture.logger.waiting_confirm_count().await;
    let transaction = writable_transaction(&fixture.db, "root-key-hook empty 2PC")?;

    transaction
        .lock_key(Atom::from(LOG_ORDERED_TABLE), minimum_key())
        .await
        .map_err(|error| format!("empty 2PC LogOrdered lock failed: {error:?}"))?;
    transaction
        .unlock_key(Atom::from(BTREE_TABLE), maximum_key())
        .await
        .map_err(|error| format!("empty 2PC Btree unlock failed: {error:?}"))?;
    transaction
        .lock_key(Atom::from(MEMORY_TABLE), key("empty-2pc-memory"))
        .await
        .map_err(|error| format!("empty 2PC Memory lock failed: {error:?}"))?;
    transaction
        .unlock_key(Atom::from(LOG_ORDERED_TABLE), key("unowned"))
        .await
        .map_err(|error| format!("empty 2PC repeated LogOrdered unlock failed: {error:?}"))?;

    let before_prepare = transaction.to_children().collect::<Vec<_>>();
    assert_children(
        &before_prepare,
        &[TableKind::LogOrdered, TableKind::Btree, TableKind::Memory],
        true,
        false,
        Transaction2PcStatus::Start,
        "lock-only before prepare",
    )?;
    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing lock-only root failed: {error:?}"))?;
    expect_eq("lock-only prepare bytes", &prepare.len(), &0usize)?;
    require(
        transaction.get_transaction_uid().is_some(),
        "lock-only prepare omitted the root TID",
    )?;
    require(
        transaction.get_commit_uid().is_none(),
        "nonpersistent lock-only prepare unexpectedly allocated a CID",
    )?;
    let prepared_children = transaction.to_children().collect::<Vec<_>>();
    assert_children(
        &prepared_children,
        &[TableKind::LogOrdered, TableKind::Btree, TableKind::Memory],
        true,
        false,
        Transaction2PcStatus::Prepared,
        "lock-only prepared children",
    )?;
    let root_tid = transaction
        .get_transaction_uid()
        .expect("lock-only prepared root TID checked above");
    for (index, child) in prepared_children.iter().enumerate() {
        expect_eq(
            &format!("lock-only child {index} inherited TID"),
            &child.get_transaction_uid(),
            &Some(root_tid.clone()),
        )?;
        require(
            child.get_commit_uid().is_none(),
            &format!("lock-only child {index} unexpectedly inherited a CID"),
        )?;
    }

    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing lock-only root failed: {error:?}"))?;
    expect_eq(
        "lock-only root status",
        &transaction.get_status(),
        &Transaction2PcStatus::Commited,
    )?;
    let committed_children = transaction.to_children().collect::<Vec<_>>();
    assert_children(
        &committed_children,
        &[TableKind::LogOrdered, TableKind::Btree, TableKind::Memory],
        true,
        false,
        Transaction2PcStatus::Commited,
        "lock-only committed children",
    )?;
    drop(transaction);

    expect_eq(
        "lock-only produced count",
        &fixture.manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        "lock-only consumed count",
        &fixture.manager.consumed_transaction_total(),
        &(consumed_before + 1),
    )?;
    expect_eq(
        "lock-only active roots",
        &fixture.manager.transaction_len(),
        &0usize,
    )?;
    expect_eq(
        "lock-only WAL append count",
        &fixture.logger.append_total_count(),
        &append_before,
    )?;
    expect_eq(
        "lock-only WAL confirm count",
        &fixture.logger.confirm_total_count(),
        &confirm_before,
    )?;
    expect_eq(
        "lock-only waiting WAL count",
        &fixture.logger.waiting_confirm_count().await,
        &waiting_before,
    )?;
    let after = observe_all(&fixture.db, "stable").await?;
    expect_eq("lock-only values and versions", &after, &before)
}

async fn verify_empty_commit_does_not_revert_newer_roots(
    fixture: &Fixture,
) -> TestResult<()> {
    let produced_before = fixture.manager.produced_transaction_total();
    let consumed_before = fixture.manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let stale = writable_transaction(&fixture.db, "root-key-hook stale empty root")?;

    for table in TABLE_CASES {
        stale
            .lock_key(Atom::from(table.name), key("stale-empty-hook"))
            .await
            .map_err(|error| {
                format!("{} stale empty lock failed: {error:?}", table.label)
            })?;
    }
    let stale_children = stale.to_children().collect::<Vec<_>>();
    assert_children(
        &stale_children,
        &[TableKind::Memory, TableKind::LogOrdered, TableKind::Btree],
        true,
        false,
        Transaction2PcStatus::Start,
        "stale empty root before writer",
    )?;

    let writer = writable_transaction(&fixture.db, "root-key-hook newer writer")?;
    writer
        .upsert(
            TABLE_CASES
                .iter()
                .map(|table| {
                    TableKV::new(
                        Atom::from(table.name),
                        key("stale-target"),
                        Some(value(&format!("{}-newer-writer", table.label))),
                    )
                })
                .collect(),
        )
        .await
        .map_err(|error| format!("newer writer upsert failed: {error:?}"))?;
    commit_ordinary(&writer, "root-key-hook newer writer").await?;
    drop(writer);
    let after_writer = observe_all(&fixture.db, "stale-target").await?;
    expect_eq(
        "newer writer WAL count",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;

    let prepare = stale
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing stale empty root failed: {error:?}"))?;
    expect_eq("stale empty prepare bytes", &prepare.len(), &0usize)?;
    stale
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing stale empty root failed: {error:?}"))?;
    drop(stale);

    expect_eq(
        "stale empty produced count",
        &fixture.manager.produced_transaction_total(),
        &(produced_before + 2),
    )?;
    expect_eq(
        "stale empty consumed count",
        &fixture.manager.consumed_transaction_total(),
        &(consumed_before + 2),
    )?;
    expect_eq(
        "stale empty WAL count",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;
    let after_empty_commit = observe_all(&fixture.db, "stale-target").await?;
    expect_eq(
        "stale empty commit retained newer values and versions",
        &after_empty_commit,
        &after_writer,
    )
}

async fn verify_pre_touch_conflict_and_rollback(fixture: &Fixture) -> TestResult<()> {
    for table in TABLE_CASES {
        let produced_before = fixture.manager.produced_transaction_total();
        let consumed_before = fixture.manager.consumed_transaction_total();
        let append_before = fixture.logger.append_total_count();
        let target = key("conflict-target");
        let stale = writable_transaction(
            &fixture.db,
            &format!("{} pre-touch stale root", table.label),
        )?;
        stale
            .lock_key(Atom::from(table.name), key("different-hook-key"))
            .await
            .map_err(|error| format!("{} pre-touch lock failed: {error:?}", table.label))?;
        let before_write_children = stale.to_children().collect::<Vec<_>>();
        assert_children(
            &before_write_children,
            &[table.kind],
            true,
            false,
            Transaction2PcStatus::Start,
            &format!("{} pre-touch before write", table.label),
        )?;

        let winner_value = value(&format!("{}-conflict-winner", table.label));
        let winner = writable_transaction(
            &fixture.db,
            &format!("{} pre-touch conflict winner", table.label),
        )?;
        winner
            .upsert(vec![TableKV::new(
                Atom::from(table.name),
                target.clone(),
                Some(winner_value.clone()),
            )])
            .await
            .map_err(|error| format!("{} winner upsert failed: {error:?}", table.label))?;
        commit_ordinary(
            &winner,
            &format!("{} pre-touch conflict winner", table.label),
        ).await?;
        drop(winner);

        stale
            .upsert(vec![TableKV::new(
                Atom::from(table.name),
                target.clone(),
                Some(value(&format!("{}-stale-proposal", table.label))),
            )])
            .await
            .map_err(|error| format!("{} stale upsert failed: {error:?}", table.label))?;
        expect_eq(
            &format!("{} stale child count after write", table.label),
            &stale.children_len(),
            &1usize,
        )?;
        expect_eq(
            &format!("{} stale root persistence after write", table.label),
            &stale.is_require_persistence(),
            &true,
        )?;
        let promoted_children = stale.to_children().collect::<Vec<_>>();
        assert_children(
            &promoted_children,
            &[table.kind],
            true,
            true,
            Transaction2PcStatus::Start,
            &format!("{} promoted stale child", table.label),
        )?;

        let error = stale
            .prepare_modified_conflicts()
            .await
            .expect_err("pre-touched stale write must conflict");
        assert_conflict(
            &error,
            table.name,
            &target,
            &format!("{} pre-touch conflict", table.label),
        )?;
        expect_eq(
            &format!("{} stale root failed status", table.label),
            &stale.get_status(),
            &Transaction2PcStatus::PrepareFailed,
        )?;
        expect_eq(
            &format!("{} conflict rejection WAL count", table.label),
            &fixture.logger.append_total_count(),
            &(append_before + 1),
        )?;
        stale
            .rollback_modified()
            .await
            .map_err(|error| format!("{} stale rollback failed: {error:?}", table.label))?;
        expect_eq(
            &format!("{} stale root rollback status", table.label),
            &stale.get_status(),
            &Transaction2PcStatus::Rollbacked,
        )?;
        drop(stale);

        expect_eq(
            &format!("{} conflict produced count", table.label),
            &fixture.manager.produced_transaction_total(),
            &(produced_before + 2),
        )?;
        expect_eq(
            &format!("{} conflict consumed count", table.label),
            &fixture.manager.consumed_transaction_total(),
            &(consumed_before + 2),
        )?;
        expect_eq(
            &format!("{} conflict active roots", table.label),
            &fixture.manager.transaction_len(),
            &0usize,
        )?;
        let observed = fixture
            .db
            .query_with_version(Atom::from(table.name), target)
            .await
            .map_err(|error| {
                format!("{} conflict final query failed: {error:?}", table.label)
            })?;
        expect_binary(
            &format!("{} conflict final winner value", table.label),
            observed.0.as_ref(),
            Some(&winner_value),
        )?;
    }
    Ok(())
}

async fn verify_hook_then_successful_write(fixture: &Fixture) -> TestResult<()> {
    let produced_before = fixture.manager.produced_transaction_total();
    let consumed_before = fixture.manager.consumed_transaction_total();
    let append_before = fixture.logger.append_total_count();
    let transaction = writable_transaction(&fixture.db, "root-key-hook promoted writer")?;

    transaction
        .unlock_key(Atom::from(BTREE_TABLE), key("promoted-btree-hook"))
        .await
        .map_err(|error| format!("promoted Btree unlock failed: {error:?}"))?;
    transaction
        .lock_key(Atom::from(MEMORY_TABLE), key("promoted-memory-hook"))
        .await
        .map_err(|error| format!("promoted Memory lock failed: {error:?}"))?;
    transaction
        .lock_key(Atom::from(LOG_ORDERED_TABLE), key("promoted-log-hook"))
        .await
        .map_err(|error| format!("promoted LogOrdered lock failed: {error:?}"))?;
    let initial_children = transaction.to_children().collect::<Vec<_>>();
    assert_children(
        &initial_children,
        &[TableKind::Btree, TableKind::Memory, TableKind::LogOrdered],
        true,
        false,
        Transaction2PcStatus::Start,
        "promoted writer before actions",
    )?;

    transaction
        .upsert(
            TABLE_CASES
                .iter()
                .map(|table| {
                    TableKV::new(
                        Atom::from(table.name),
                        key("promoted-target"),
                        Some(value(&format!("{}-promoted-value", table.label))),
                    )
                })
                .collect(),
        )
        .await
        .map_err(|error| format!("promoted writer upsert failed: {error:?}"))?;
    expect_eq(
        "promoted writer child count",
        &transaction.children_len(),
        &3usize,
    )?;
    expect_eq(
        "promoted writer root persistence",
        &transaction.is_require_persistence(),
        &true,
    )?;
    let promoted_children = transaction.to_children().collect::<Vec<_>>();
    assert_children(
        &promoted_children,
        &[TableKind::Btree, TableKind::Memory, TableKind::LogOrdered],
        true,
        true,
        Transaction2PcStatus::Start,
        "promoted writer after actions",
    )?;

    let prepare = transaction
        .prepare_modified_conflicts()
        .await
        .map_err(|error| format!("preparing promoted writer failed: {error:?}"))?;
    require(
        prepare.len() > 16,
        &format!(
            "promoted writer prepare omitted persistent table actions, len={}",
            prepare.len(),
        ),
    )?;
    require(
        transaction.get_commit_uid().is_some(),
        "promoted writer did not allocate a CID",
    )?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing promoted writer failed: {error:?}"))?;
    let committed_children = transaction.to_children().collect::<Vec<_>>();
    assert_children(
        &committed_children,
        &[TableKind::Btree, TableKind::Memory, TableKind::LogOrdered],
        true,
        true,
        Transaction2PcStatus::Commited,
        "promoted writer committed children",
    )?;
    drop(transaction);

    expect_eq(
        "promoted writer produced count",
        &fixture.manager.produced_transaction_total(),
        &(produced_before + 1),
    )?;
    expect_eq(
        "promoted writer consumed count",
        &fixture.manager.consumed_transaction_total(),
        &(consumed_before + 1),
    )?;
    expect_eq(
        "promoted writer WAL count",
        &fixture.logger.append_total_count(),
        &(append_before + 1),
    )?;
    for table in TABLE_CASES {
        let expected = value(&format!("{}-promoted-value", table.label));
        let observed = fixture
            .db
            .query_with_version(Atom::from(table.name), key("promoted-target"))
            .await
            .map_err(|error| {
                format!("{} promoted final query failed: {error:?}", table.label)
            })?;
        expect_binary(
            &format!("{} promoted final value", table.label),
            observed.0.as_ref(),
            Some(&expected),
        )?;
    }
    Ok(())
}

async fn observe_all(db: &RealDb, key_name: &str) -> TestResult<Vec<Observation>> {
    let mut observations = Vec::with_capacity(TABLE_CASES.len());
    for table in TABLE_CASES {
        let (value, version) = db
            .query_with_version(Atom::from(table.name), key(key_name))
            .await
            .map_err(|error| {
                format!("{} observing {key_name} failed: {error:?}", table.label)
            })?;
        observations.push(Observation { value, version });
    }
    Ok(observations)
}

fn assert_children(
    children: &[RealTransaction],
    expected_order: &[TableKind],
    writable: bool,
    persistent: bool,
    status: Transaction2PcStatus,
    label: &str,
) -> TestResult<()> {
    let observed_order = children
        .iter()
        .map(child_kind)
        .collect::<TestResult<Vec<_>>>()?;
    expect_eq(
        &format!("{label} child order"),
        &observed_order,
        &expected_order.to_vec(),
    )?;
    for (index, child) in children.iter().enumerate() {
        expect_eq(
            &format!("{label} child {index} writable"),
            &child.is_writable(),
            &writable,
        )?;
        expect_eq(
            &format!("{label} child {index} persistence"),
            &child.is_require_persistence(),
            &persistent,
        )?;
        expect_eq(
            &format!("{label} child {index} status"),
            &child.get_status(),
            &status,
        )?;
        expect_eq(
            &format!("{label} child {index} is unit"),
            &child.is_unit(),
            &true,
        )?;
        expect_eq(
            &format!("{label} child {index} is tree"),
            &child.is_tree(),
            &false,
        )?;
    }
    Ok(())
}

fn child_kind(child: &RealTransaction) -> TestResult<TableKind> {
    match child {
        KVDBTransaction::MemOrdTabTr(_) => Ok(TableKind::Memory),
        KVDBTransaction::LogOrdTabTr(_) => Ok(TableKind::LogOrdered),
        KVDBTransaction::BtreeOrdTabTr(_) => Ok(TableKind::Btree),
        KVDBTransaction::MetaTabTr(_) => {
            Err("public key-hook matrix unexpectedly created a Meta child".to_owned())
        },
        KVDBTransaction::LogWTabTr(_) => {
            Err("public key-hook matrix unexpectedly created a LogWrite child".to_owned())
        },
        KVDBTransaction::RootTr(_) => {
            Err("root transaction appeared in its own child list".to_owned())
        },
    }
}

fn assert_conflict(
    error: &KVTableTrError,
    expected_table: &str,
    expected_key: &Binary,
    label: &str,
) -> TestResult<()> {
    if !matches!(error.level(), ErrorLevel::Normal) {
        return Err(format!(
            "{label}: conflict returned non-Normal level {:?}",
            error.level(),
        ));
    }
    if !error.is_conflicts() {
        return Err(format!("{label}: expected conflict error, observed {error:?}"));
    }
    let (table, key) = error
        .conflicts()
        .ok_or_else(|| format!("{label}: conflict omitted table/key"))?;
    expect_eq(
        &format!("{label} table"),
        &table.as_str(),
        &expected_table,
    )?;
    if key.as_ref() != expected_key.as_ref() {
        return Err(format!(
            "{label}: key mismatch, expected_len={}, observed_len={}",
            expected_key.len(),
            key.len(),
        ));
    }
    Ok(())
}

async fn settle_persistent_tables(
    rt: &MultiTaskRuntime<()>,
    fixture: &Fixture,
    label: &str,
) -> TestResult<()> {
    // ready_collect_table/collect_table 只执行表日志切分或数据文件维护，不消费事务确认 FIFO。
    // 这里必须等待构造表时启动的真实 60 秒 collector，避免测试伪造生产确认时序。
    let deadline = Instant::now() + CONFIRM_TIMEOUT;
    loop {
        let appended = fixture.logger.append_total_count();
        let confirmed = fixture.logger.confirm_total_count();
        let waiting = fixture.logger.waiting_confirm_count().await;
        let btree_cache = fixture
            .db
            .table_cache_size(&Atom::from(BTREE_TABLE))
            .await;
        if confirmed == appended && waiting == 0 && btree_cache == Some(0) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{label}: persistent state did not settle within {:?}: appended={appended}, confirmed={confirmed}, waiting={waiting}, btree_cache={btree_cache:?}",
                CONFIRM_TIMEOUT,
            ));
        }
        rt.timeout(10).await;
    }
}

async fn commit_ordinary(transaction: &RealTransaction, label: &str) -> TestResult<()> {
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
        .map_err(|error| format!("creating root-key-hook fixture root failed: {error}"))?;
    let logger = CommitLoggerBuilder::new(rt.clone(), root.join("root-wal"))
        .log_file_limit(64 * 1024 * 1024)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("building root-key-hook CommitLogger failed: {error}"))?;
    let manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger.clone(),
    );
    let db = KVDBManagerBuilder::new(rt.clone(), manager.clone(), root.join("database"))
        .key_version_ttl(Duration::ZERO)
        .key_version_ttl_poll_interval(Duration::ZERO)
        .startup(false)
        .await
        .map_err(|error| format!("starting root-key-hook database failed: {error}"))?;
    Ok(Fixture {
        db,
        manager,
        logger,
    })
}

fn writable_transaction(db: &RealDb, source: &str) -> TestResult<RealTransaction> {
    db.transaction(Atom::from(source), true, 10_000, 10_000)
        .ok_or_else(|| format!("database rejected writable transaction {source}"))
}

fn read_only_transaction(db: &RealDb, source: &str) -> TestResult<RealTransaction> {
    db.transaction(Atom::from(source), false, 10_000, 10_000)
        .ok_or_else(|| format!("database rejected read-only transaction {source}"))
}

fn table_meta(table_type: KVDBTableType, persistence: bool) -> KVTableMeta {
    KVTableMeta::new(table_type, persistence, EnumType::Bin, EnumType::Bin)
}

fn minimum_key() -> Binary {
    let key = encode_bin(&[]);
    assert_eq!(key.len(), 1);
    key
}

fn maximum_key() -> Binary {
    let key = encode_bin(&vec![0xA5; u16::MAX as usize - 3]);
    assert_eq!(key.len(), u16::MAX as usize);
    key
}

fn key(name: &str) -> Binary {
    encode_bin(name.as_bytes())
}

fn value(name: &str) -> Binary {
    encode_bin(name.as_bytes())
}

fn encode_bin(bytes: &[u8]) -> Binary {
    let mut buffer = WriteBuffer::new();
    buffer.write_bin(bytes, 0..bytes.len());
    Binary::new(buffer.bytes)
}

fn expect_binary(
    label: &str,
    actual: Option<&Binary>,
    expected: Option<&Binary>,
) -> TestResult<()> {
    if actual.map(Binary::as_ref) != expected.map(Binary::as_ref) {
        return Err(format!(
            "{label}: binary mismatch, actual_len={:?}, expected_len={:?}",
            actual.map(Binary::len),
            expected.map(Binary::len),
        ));
    }
    Ok(())
}

fn expect_eq<T>(label: &str, actual: &T, expected: &T) -> TestResult<()>
where
    T: Debug + PartialEq,
{
    if actual != expected {
        return Err(format!(
            "{label}: expected {expected:?}, observed {actual:?}",
        ));
    }
    Ok(())
}

fn require(condition: bool, message: &str) -> TestResult<()> {
    if condition {
        Ok(())
    } else {
        Err(message.to_owned())
    }
}

fn run_on_runtime<T, F, Fut>(timeout: Duration, build: F) -> TestResult<T>
where
    T: Send + 'static,
    F: FnOnce(MultiTaskRuntime<()>) -> Fut + Send + 'static,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    let _time_loop = startup_global_time_loop(10);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(4)
        .build();
    let (result_tx, result_rx) = bounded(1);
    let task_rt = rt.clone();
    rt.spawn(async move {
        let _ = result_tx.send(build(task_rt).await);
    })
    .map_err(|error| format!("spawning root-key-hook runtime task failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("root-key-hook runtime exceeded {timeout:?}: {error}"))?
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(label: &str) -> TestResult<Self> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("reading system time failed: {error}"))?
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "pi_db_root_key_hook_{label}_{}_{}",
            std::process::id(),
            nanos,
        ));
        fs::create_dir_all(&path)
            .map_err(|error| format!("creating temp root {path:?} failed: {error}"))?;
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
