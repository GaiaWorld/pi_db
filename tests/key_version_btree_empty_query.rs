//! 新建空 Btree 表首次版本读取的真实生产专项与回归验收。
//!
//! 合法路径严格分离 DDL 与业务访问：先以独立根事务创建并提交 Btree 表，再调用
//! `query_with_version` 读取尚不存在的 Key。redb 数据库文件此时存在，但内部 `$default` 表要
//! 到首次物理写入时才可能创建；该表示层状态必须被解释为逻辑空表，不能伪装成存储故障。
//! 随后通过同一版本执行一次真实版本事务，证明首次 Delete 版本可以进入 prepare/commit，且
//! 返回的 Upsert 回执和最终值严格一致。
//!
//! 修复前本 target 在首次 qwv 处确定性得到 `TableDoesNotExist("$default")` 包装的
//! `Common(Normal)`；修复后作为 BUG-KV-BTREE-QWV-001 的长期验收入口。详细证据见
//! `docs/KEY_VERSION_BTREE_EMPTY_QUERY_BUG.md#bug-kv-btree-qwv-001-index`。

mod key_version_support;

use std::time::Duration;

use pi_async_transaction::{
    AsyncCommitLog, Transaction2Pc,
};
use pi_atom::Atom;
use pi_db::{
    tables::TableKV,
    utils::CreateTableOptions,
    KVDBTableType, TableKeyVersion, Version,
};

use key_version_support::{
    BTREE_TABLE, TempRoot, build_database, commit_ordinary, encode_usize,
    expect_binary, expect_eq, query_ordinary, run_on_runtime, table_meta, writable_transaction,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(60);

#[test]
fn test_new_empty_btree_query_with_version_is_logically_missing() {
    let root = TempRoot::new("btree_empty_query")
        .expect("creating empty-Btree qwv root must succeed");
    let root_path = root.path().to_path_buf();
    run_on_runtime(TEST_TIMEOUT, move |rt| async move {
        let fixture = build_database(&rt,
                                     &root_path,
                                     Duration::ZERO,
                                     Duration::ZERO).await?;
        let append_before = fixture.logger.append_total_count();
        let ddl = writable_transaction(&fixture.db, "empty-Btree qwv DDL")?;
        ddl.create_table_with_options(
            Atom::from(BTREE_TABLE),
            table_meta(KVDBTableType::BtreeOrdTab, true),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .map_err(|error| format!("creating empty Btree table failed: {error}"))?;
        commit_ordinary(&ddl, "empty-Btree qwv DDL").await?;
        expect_eq("empty-Btree DDL WAL append",
                  &fixture.logger.append_total_count(),
                  &(append_before + 1))?;
        expect_eq("empty-Btree registration",
                  &fixture.db.is_exist(&Atom::from(BTREE_TABLE)).await,
                  &true)?;

        let key = encode_usize(70_001);
        let ordinary = query_ordinary(&fixture.db,
                                      BTREE_TABLE,
                                      key.clone(),
                                      "empty-Btree ordinary control").await?;
        expect_binary("empty-Btree ordinary control value", ordinary.as_ref(), None)?;

        let (missing, first_version) = fixture
            .db
            .query_with_version(Atom::from(BTREE_TABLE), key.clone())
            .await
            .map_err(|error| format!(
                "new empty Btree qwv must return logical absence, observed {error:?}",
            ))?;
        expect_binary("new empty-Btree qwv value", missing.as_ref(), None)?;
        if !matches!(first_version, Version::Delete(_)) {
            return Err(format!(
                "new empty-Btree qwv must return Delete version, observed {first_version:?}",
            ));
        }
        let repeated = fixture
            .db
            .query_with_version(Atom::from(BTREE_TABLE), key.clone())
            .await
            .map_err(|error| format!("repeating empty-Btree qwv failed: {error:?}"))?;
        expect_binary("repeated empty-Btree qwv value", repeated.0.as_ref(), None)?;
        expect_eq("repeated empty-Btree qwv version", &repeated.1, &first_version)?;

        let value = encode_usize(80_001);
        let transaction = writable_transaction(&fixture.db, "empty-Btree version write")?;
        let prepare = transaction
            .prepare_with_version(
                vec![TableKeyVersion {
                    table: Atom::from(BTREE_TABLE),
                    key: key.clone(),
                    version: first_version,
                }],
                vec![TableKV::new(
                    Atom::from(BTREE_TABLE),
                    key.clone(),
                    Some(value.clone()),
                )],
            )
            .await
            .map_err(|error| format!("preparing empty-Btree version write failed: {error:?}"))?;
        let transaction_uid = transaction
            .get_transaction_uid()
            .ok_or_else(|| "version prepare did not allocate transaction UID".to_owned())?;
        let receipt = transaction
            .commit_with_version(prepare)
            .await
            .map_err(|error| format!("committing empty-Btree version write failed: {error:?}"))?;
        expect_eq("empty-Btree version receipt length", &receipt.len(), &1usize)?;
        expect_eq("empty-Btree version receipt table", &receipt[0].table, &Atom::from(BTREE_TABLE))?;
        expect_eq("empty-Btree version receipt key", &receipt[0].key, &key)?;
        match &receipt[0].version {
            Version::Upsert(uid) if *uid == transaction_uid => (),
            version => {
                return Err(format!(
                    "empty-Btree receipt must contain this transaction UID, observed {version:?}",
                ));
            },
        }

        let final_read = fixture
            .db
            .query_with_version(Atom::from(BTREE_TABLE), key)
            .await
            .map_err(|error| format!("querying committed empty-Btree write failed: {error:?}"))?;
        expect_binary("empty-Btree committed value", final_read.0.as_ref(), Some(&value))?;
        expect_eq("empty-Btree committed version", &final_read.1, &receipt[0].version)?;
        expect_eq("empty-Btree active transactions",
                  &fixture.tr_manager.transaction_len(),
                  &0usize)
    })
    .unwrap_or_else(|error| panic!("empty-Btree qwv contract failed: {error}"));
}
