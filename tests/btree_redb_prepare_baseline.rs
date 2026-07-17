//! Btree 跨 overlay/redb 表示的普通事务冲突基线专项。
//!
//! 本 target 使用真实 4-worker runtime、事务管理器、根 WAL、Btree/redb 和文件系统。
//! 少量合法大 `Str` 值用于越过生产 1 MiB collector 阈值，避免为 sanitizer 重复数百次与
//! 判等风险无关的 Key 操作。它严格验证：
//!
//! - redb-only Key 未被并发修改时，普通 delete 可以 prepare/commit；
//! - redb 每次解码产生不同 allocation，不得因此误报冲突；
//! - 同值并发写和 ABA 即使最终字节相同，仍由 Key revision 判定为冲突；
//! - overlay 基线被 collector 原样搬到 redb 后仍可合法 prepare；
//! - redb 删除返回值、事务基线及事务闭环后的强引用数严格为 `2 -> 1 -> 0`。
//!
//! 修复前真实环境在无并发 redb-only 删除 prepare 处确定性返回伪冲突。问题、冻结方案和
//! 验收边界见 `docs/KEY_VERSION_BTREE_REDB_BASELINE_BUG.md#bug-kv-btree-redb-baseline-001-index`。

mod key_version_support;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use pi_async_rt::rt::AsyncRuntime;
use pi_async_transaction::{
    ErrorLevel, UnitTransaction,
    manager_2pc::Transaction2PcStatus,
};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{
    Binary, KVDBTableType, KVTableMeta,
    tables::TableKV,
    utils::CreateTableOptions,
};
use pi_sinfo::EnumType;

use key_version_support::{
    RealDb, RealTransaction, TempRoot, TestResult, build_database, commit_ordinary, encode_usize,
    expect_binary, expect_eq, run_on_runtime, writable_transaction,
};

const TABLE: &str = "btree_redb_prepare_baseline";
const TEST_TIMEOUT: Duration = Duration::from_secs(90);
const CACHE_DEADLINE: Duration = Duration::from_secs(30);
const LARGE_VALUE_BYTES: usize = 400 * 1024;
const TRIGGER_VALUE_BYTES: usize = 300 * 1024;

#[test]
fn test_btree_redb_prepare_baseline_and_revision_conflicts() {
    let root = TempRoot::new("btree_redb_prepare_baseline")
        .expect("creating Btree redb-baseline root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        create_table(&fixture.db).await?;

        let value_a = encode_string_payload("value-a", LARGE_VALUE_BYTES);
        let value_b = encode_string_payload("value-b", LARGE_VALUE_BYTES);
        let value_c = encode_string_payload("value-c", LARGE_VALUE_BYTES);
        let initial = writable_transaction(&fixture.db, "Btree redb baseline initial writer")?;
        initial
            .upsert(vec![
                table_kv(1, Some(value_a.clone())),
                table_kv(2, Some(value_b.clone())),
                table_kv(3, Some(value_c.clone())),
            ])
            .await
            .map_err(|error| format!("writing initial redb baseline failed: {error:?}"))?;
        commit_ordinary(&initial, "Btree redb baseline initial writer").await?;
        wait_for_empty_cache(&rt, &fixture.db, CACHE_DEADLINE).await?;

        // 无并发修改的 redb-only 删除是本 Bug 的最小生产红/绿路径。
        let deleter = writable_transaction(&fixture.db, "Btree redb baseline deleter")?;
        let mut deleted = deleter
            .delete(vec![table_kv(1, None)])
            .await
            .map_err(|error| format!("deleting redb-only baseline failed: {error:?}"))?;
        expect_eq("redb-only delete result count", &deleted.len(), &1usize)?;
        let old = deleted
            .pop()
            .flatten()
            .ok_or_else(|| "redb-only delete did not return the old value".to_owned())?;
        expect_binary("redb-only delete old value", Some(&old), Some(&value_a))?;
        let shared = old.to_shared();
        let weak = Arc::downgrade(&shared);
        drop(shared);
        expect_eq("redb old value owners before caller drop", &weak.strong_count(), &2usize)?;
        let old_len = std::thread::spawn(move || old.len())
            .join()
            .map_err(|_| "redb old-value consumer thread panicked".to_owned())?;
        expect_eq("redb old value cross-thread length", &old_len, &value_a.len())?;
        expect_eq("redb baseline owner after caller drop", &weak.strong_count(), &1usize)?;
        commit_ordinary(&deleter, "Btree redb baseline deleter").await?;
        if weak.upgrade().is_some() {
            return Err("redb baseline remained owned after delete transaction commit".to_owned());
        }

        // 最终字节相同也不能掩盖同值并发提交；revision 是该事实的权威证据。
        let same_value_stale = writable_transaction(&fixture.db, "Btree same-value stale reader")?;
        let same_value_read = query_one(&same_value_stale, 2).await?;
        expect_binary("same-value stale baseline", same_value_read.as_ref(), Some(&value_b))?;
        drop(same_value_read);
        let same_value_writer = writable_transaction(&fixture.db, "Btree same-value writer")?;
        same_value_writer
            .upsert(vec![table_kv(2, Some(value_b.clone()))])
            .await
            .map_err(|error| format!("writing same Btree value failed: {error:?}"))?;
        commit_ordinary(&same_value_writer, "Btree same-value writer").await?;
        expect_conflict_then_rollback(&same_value_stale, 2, "same-value stale reader").await?;

        // A -> B -> A 的最终内容与基线相同，但两次提交都必须使旧事务冲突。
        let aba_stale = writable_transaction(&fixture.db, "Btree ABA stale reader")?;
        let aba_read = query_one(&aba_stale, 3).await?;
        expect_binary("ABA stale baseline", aba_read.as_ref(), Some(&value_c))?;
        drop(aba_read);
        let changed = encode_string_payload("value-c-mutated", LARGE_VALUE_BYTES);
        let aba_changed = writable_transaction(&fixture.db, "Btree ABA changed writer")?;
        aba_changed
            .upsert(vec![table_kv(3, Some(changed))])
            .await
            .map_err(|error| format!("writing Btree ABA middle value failed: {error:?}"))?;
        commit_ordinary(&aba_changed, "Btree ABA changed writer").await?;
        let aba_restored = writable_transaction(&fixture.db, "Btree ABA restored writer")?;
        aba_restored
            .upsert(vec![table_kv(3, Some(value_c.clone()))])
            .await
            .map_err(|error| format!("restoring Btree ABA value failed: {error:?}"))?;
        commit_ordinary(&aba_restored, "Btree ABA restored writer").await?;
        expect_conflict_then_rollback(&aba_stale, 3, "ABA stale reader").await?;

        // 两个值保持在 overlay 中，事务取得基线后再用无关 Key 触发 collector。表示从
        // overlay 迁移到 redb 不能被误认为逻辑值发生变化。
        let value_d = encode_string_payload("value-d", LARGE_VALUE_BYTES);
        let value_e = encode_string_payload("value-e", LARGE_VALUE_BYTES);
        let overlay_writer = writable_transaction(&fixture.db, "Btree overlay baseline writer")?;
        overlay_writer
            .upsert(vec![
                table_kv(4, Some(value_d.clone())),
                table_kv(5, Some(value_e)),
            ])
            .await
            .map_err(|error| format!("writing overlay relocation baseline failed: {error:?}"))?;
        commit_ordinary(&overlay_writer, "Btree overlay baseline writer").await?;
        rt.timeout(20).await;
        let cache_size = fixture
            .db
            .table_cache_size(&Atom::from(TABLE))
            .await
            .ok_or_else(|| "Btree table disappeared before relocation test".to_owned())?;
        if cache_size == 0 {
            return Err("overlay relocation baseline was unexpectedly collected early".to_owned());
        }

        let relocation = writable_transaction(&fixture.db, "Btree collector relocation reader")?;
        let relocation_read = query_one(&relocation, 4).await?;
        expect_binary("collector relocation baseline", relocation_read.as_ref(), Some(&value_d))?;
        drop(relocation_read);
        let trigger_value = encode_string_payload("collector-trigger", TRIGGER_VALUE_BYTES);
        let trigger = writable_transaction(&fixture.db, "Btree collector trigger writer")?;
        trigger
            .upsert(vec![table_kv(6, Some(trigger_value.clone()))])
            .await
            .map_err(|error| format!("writing collector trigger failed: {error:?}"))?;
        commit_ordinary(&trigger, "Btree collector trigger writer").await?;
        wait_for_empty_cache(&rt, &fixture.db, CACHE_DEADLINE).await?;
        commit_ordinary(&relocation, "Btree collector relocation reader").await?;

        expect_binary("final deleted key", query_fresh(&fixture.db, 1).await?.as_ref(), None)?;
        expect_binary("final same-value key", query_fresh(&fixture.db, 2).await?.as_ref(), Some(&value_b))?;
        expect_binary("final ABA key", query_fresh(&fixture.db, 3).await?.as_ref(), Some(&value_c))?;
        expect_binary("final relocated key", query_fresh(&fixture.db, 4).await?.as_ref(), Some(&value_d))?;
        expect_binary("final trigger key", query_fresh(&fixture.db, 6).await?.as_ref(), Some(&trigger_value))?;
        expect_eq("Btree redb-baseline active transactions",
                  &fixture.tr_manager.transaction_len(),
                  &0usize)
    })
    .unwrap_or_else(|error| panic!("Btree redb prepare-baseline contract failed: {error}"));
}

async fn create_table(db: &RealDb) -> TestResult<()> {
    let transaction = writable_transaction(db, "Btree redb baseline DDL")?;
    transaction
        .create_table_with_options(
            Atom::from(TABLE),
            KVTableMeta::new(
                KVDBTableType::BtreeOrdTab,
                true,
                EnumType::Usize,
                EnumType::Str,
            ),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .map_err(|error| format!("creating Btree redb-baseline table failed: {error}"))?;
    commit_ordinary(&transaction, "Btree redb baseline DDL").await
}

async fn query_one(transaction: &RealTransaction, key: usize) -> TestResult<Option<Binary>> {
    let mut values = transaction.query(vec![table_kv(key, None)]).await;
    if values.len() != 1 {
        return Err(format!(
            "querying Btree key {key} returned {} slots instead of one",
            values.len(),
        ));
    }
    Ok(values.pop().expect("Btree query result length was checked"))
}

async fn query_fresh(db: &RealDb, key: usize) -> TestResult<Option<Binary>> {
    let transaction = db
        .transaction(Atom::from("Btree redb-baseline final query"), false, 10_000, 10_000)
        .ok_or_else(|| "database rejected final Btree query transaction".to_owned())?;
    query_one(&transaction, key).await
}

async fn expect_conflict_then_rollback(
    transaction: &RealTransaction,
    key: usize,
    label: &str,
) -> TestResult<()> {
    let expected_key = encode_usize(key);
    let error = transaction
        .prepare_modified_conflicts()
        .await
        .expect_err("stale Btree transaction must conflict");
    if !error.is_conflicts() || !matches!(error.level(), ErrorLevel::Normal) {
        return Err(format!(
            "{label} expected Conflicts(Normal), observed {error:?}",
        ));
    }
    let (actual_table, actual_key) = error
        .conflicts()
        .ok_or_else(|| format!("{label} conflict did not expose table/key"))?;
    if actual_table.as_str() != TABLE {
        return Err(format!(
            "{label} expected conflict table {TABLE:?}, observed {:?}",
            actual_table.as_str(),
        ));
    }
    expect_binary(&format!("{label} conflict key"), Some(actual_key), Some(&expected_key))?;
    expect_eq(&format!("{label} prepare-failed status"),
              &transaction.get_status(),
              &Transaction2PcStatus::PrepareFailed)?;
    transaction
        .rollback_modified()
        .await
        .map_err(|error| format!("rolling back {label} failed: {error:?}"))?;
    expect_eq(&format!("{label} rollback status"),
              &transaction.get_status(),
              &Transaction2PcStatus::Rollbacked)
}

async fn wait_for_empty_cache(
    rt: &pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
    db: &RealDb,
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        match db.table_cache_size(&Atom::from(TABLE)).await {
            Some(0) => return Ok(()),
            Some(_) if Instant::now() < deadline => rt.timeout(5).await,
            Some(size) => {
                return Err(format!(
                    "Btree redb-baseline cache remained at {size} bytes after {timeout:?}",
                ));
            },
            None => return Err("Btree redb-baseline table disappeared".to_owned()),
        }
    }
}

fn table_kv(key: usize, value: Option<Binary>) -> TableKV {
    TableKV::new(Atom::from(TABLE), encode_usize(key), value)
}

fn encode_string_payload(prefix: &str, target_bytes: usize) -> Binary {
    assert!(prefix.len() <= target_bytes);
    let mut value = String::with_capacity(target_bytes);
    value.push_str(prefix);
    while value.len() < target_bytes {
        value.push('x');
    }
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}
