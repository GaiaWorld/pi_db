//! Btree 删除旧值与三态只写缓存语义的真实环境专项测试。
//!
//! 本 target 只通过公开 `KVDBManager -> KVDBTransaction::{upsert,dirty_upsert,delete,
//! dirty_delete,query,dirty_query}`
//! 生产调用链访问真实 Btree、redb、根 WAL、事务管理器、运行时和文件系统，不使用 mock、
//! 私有构造或旧测试。它保护 FIND-BTREE-001 的冻结边界：
//!
//! - 事务私有或已提交共享只写缓存中的 `Some(value)` 必须作为删除旧值返回；
//! - 缓存 tombstone、重复删除和真正不存在的 Key 必须返回 `None`；
//! - 缓存完全缺席时必须中立读取 `delete` 执行时的 redb 快照并返回精确旧值，但不得把
//!   该值写入 `cache_ref`；已有 tombstone 和重复删除禁止回读 redb；
//! - `begin_read/open_table/get` 错误必须记录详细 error 日志并降级为 `Ok(None)`，删除
//!   tombstone 仍须保留且事务仍可提交；真实环境使用新建空表稳定覆盖 `open_table` 错误；
//! - `dirty_delete` 当前复用普通 Btree 删除路径，本测试只冻结相同的旧值返回边界，不把
//!   dirty 冲突行为认定为最终设计。
//! - 普通与 dirty 非空动作始终位于不同根事务；测试不会用协议禁止的混用路径证明公开语义。
//! - 返回旧 `Binary` 必须可以跨线程读取；redb-only 普通删除还必须在事务存活期间保留同一
//!   payload 的首次读取基线，以供 prepare 做值冲突比较。调用方释放返回值后只允许该基线
//!   一个 owner，事务提交消费动作后必须归零，禁止形成超出事务生命周期的隐藏引用。
//!
//! 持久化场景保留 320 个 4 KiB Key，并增加 17 个 62 KiB 的合法 `Str -> Usize` Key；等待
//! 公开缓存大小归零后再删除，以客观证明 Key 只存在于 redb。普通根和 dirty 根各自的删除
//! Key 总大小都独立超过生产 1 MiB 刷新阈值，并在两次提交之间等待缓存归零；测试不依赖
//! detached 入队任务的调度顺序，也无需等待 60 秒定时器。整个测试由同步通道施加硬截止。
//!
//! 双向文档入口：`docs/REVIEW_FINDINGS.md#find-btree-001`、
//! `docs/SEMANTIC_CONTRACTS.md#contract-btree-delete-old-value`、
//! `docs/BTREE_DELETE_FIX_PLAN.md#btree-delete-fix-index`。

use std::{
    env, fs,
    future::Future,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::bounded;
use log::{Level, LevelFilter, Log, Metadata, Record};
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::manager_2pc::Transaction2PcManager;
use pi_atom::Atom;
use pi_bon::{Decode, Encode, ReadBuffer, WriteBuffer};
use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder, KVDBTransaction},
    tables::TableKV,
    utils::CreateTableOptions,
    Binary, KVDBTableType, KVTableMeta,
};
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};

type TestResult<T = ()> = Result<T, String>;
type RealDb = KVDBManager<usize, CommitLogger>;
type RealTransaction = KVDBTransaction<usize, CommitLogger>;

const PRIVATE_TABLE: &str = "btree_delete_private_cache";
const DIRTY_TABLE: &str = "btree_dirty_delete_private_cache";
const SHARED_TABLE: &str = "btree_delete_shared_cache";
const LIFECYCLE_TABLE: &str = "btree_delete_return_lifecycle";
const REDB_TABLE: &str = "btree_delete_redb_only";
const REDB_ERROR_TABLE: &str = "btree_delete_redb_open_error";
const RUNTIME_DEADLINE: Duration = Duration::from_secs(120);
const PERSISTENCE_DEADLINE: Duration = Duration::from_secs(30);
const PERSISTED_KEYS: usize = 320;
const PERSISTED_KEY_BYTES: usize = 4 * 1024;
const ORDINARY_COLLECTOR_KEYS: usize = 17;
const ORDINARY_COLLECTOR_KEY_BYTES: usize = 62 * 1024;
const BTREE_WAITS_LIMIT_BYTES: usize = 1024 * 1024;
const REDB_READ_ERROR_MARKER: &str = "Btree delete redb old-value read failed";

static CAPTURE_LOGGER: CaptureLogger = CaptureLogger {
    records: Mutex::new(Vec::new()),
};
static LOGGER_INIT: OnceLock<Result<(), String>> = OnceLock::new();

/// 仅捕获 error 记录，用于严格验证生产错误降级路径的可观察证据。
struct CaptureLogger {
    records: Mutex<Vec<String>>,
}

impl CaptureLogger {
    fn clear(&self) -> TestResult<()> {
        self.records
            .lock()
            .map_err(|_| "capture logger mutex was poisoned while clearing".to_string())?
            .clear();
        Ok(())
    }

    fn matching(&self, marker: &str) -> TestResult<Vec<String>> {
        Ok(self
            .records
            .lock()
            .map_err(|_| "capture logger mutex was poisoned while reading".to_string())?
            .iter()
            .filter(|record| record.contains(marker))
            .cloned()
            .collect())
    }
}

impl Log for CaptureLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= Level::Error
    }

    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            if let Ok(mut records) = self.records.lock() {
                records.push(format!(
                    "level={} target={} {}",
                    record.level(),
                    record.target(),
                    record.args()
                ));
            }
        }
    }

    fn flush(&self) {}
}

fn init_capture_logger() -> TestResult<()> {
    LOGGER_INIT
        .get_or_init(|| {
            log::set_logger(&CAPTURE_LOGGER)
                .map(|()| log::set_max_level(LevelFilter::Error))
                .map_err(|error| format!("installing capture logger failed: {error}"))
        })
        .clone()
}

/// 事务私有缓存必须返回删除前旧值，并区分不存在与重复删除。
async fn exercise_private_cache(db: &RealDb) -> TestResult<()> {
    create_btree_table(db, PRIVATE_TABLE, false, EnumType::Usize, EnumType::Usize).await?;
    let transaction = writable_transaction(db, "private cache delete")?;
    transaction
        .upsert(vec![
            table_kv(PRIVATE_TABLE, 1, Some(101)),
            table_kv(PRIVATE_TABLE, 2, Some(202)),
        ])
        .await
        .map_err(|error| format!("upserting private cache baseline failed: {error:?}"))?;

    let first = transaction
        .delete(vec![
            table_kv(PRIVATE_TABLE, 1, None),
            table_kv(PRIVATE_TABLE, 99, None),
            table_kv(PRIVATE_TABLE, 1, None),
        ])
        .await
        .map_err(|error| format!("deleting private cache values failed: {error:?}"))?;
    assert_optional_usizes(
        "private cache delete results",
        &[Some(101), None, None],
        &first,
    )?;
    assert_optional_usizes(
        "private cache visibility after delete",
        &[None, Some(202)],
        &transaction
            .query(vec![
                table_kv(PRIVATE_TABLE, 1, None),
                table_kv(PRIVATE_TABLE, 2, None),
            ])
            .await,
    )
}

/// `dirty_delete` 当前与普通删除共享旧值和 tombstone 行为。
async fn exercise_dirty_private_cache(db: &RealDb) -> TestResult<()> {
    create_btree_table(db, DIRTY_TABLE, false, EnumType::Usize, EnumType::Usize).await?;
    let transaction = writable_transaction(db, "dirty private cache delete")?;
    transaction
        .dirty_upsert(vec![table_kv(DIRTY_TABLE, 7, Some(707))])
        .await
        .map_err(|error| format!("dirty-upserting dirty-delete baseline failed: {error:?}"))?;

    let first = transaction
        .dirty_delete(vec![table_kv(DIRTY_TABLE, 7, None)])
        .await
        .map_err(|error| format!("dirty-deleting private cache value failed: {error:?}"))?;
    assert_optional_usizes("dirty private cache first delete", &[Some(707)], &first)?;
    let repeated = transaction
        .dirty_delete(vec![table_kv(DIRTY_TABLE, 7, None)])
        .await
        .map_err(|error| format!("repeating dirty delete failed: {error:?}"))?;
    assert_optional_usizes("dirty private cache repeated delete", &[None], &repeated)
}

/// 非持久 Btree 提交后，下一事务必须从共享只写缓存取得旧值。
async fn exercise_shared_cache(db: &RealDb) -> TestResult<()> {
    create_btree_table(db, SHARED_TABLE, false, EnumType::Usize, EnumType::Usize).await?;
    let writer = writable_transaction(db, "shared cache writer")?;
    writer
        .upsert(vec![table_kv(SHARED_TABLE, 11, Some(1_111))])
        .await
        .map_err(|error| format!("upserting shared cache baseline failed: {error:?}"))?;
    commit_transaction(&writer, "shared cache writer").await?;

    let deleter = writable_transaction(db, "shared cache deleter")?;
    let deleted = deleter
        .delete(vec![table_kv(SHARED_TABLE, 11, None)])
        .await
        .map_err(|error| format!("deleting shared cache value failed: {error:?}"))?;
    assert_optional_usizes("shared cache delete result", &[Some(1_111)], &deleted)?;
    assert_optional_usizes(
        "shared cache tombstone visibility",
        &[None],
        &deleter.query(vec![table_kv(SHARED_TABLE, 11, None)]).await,
    )?;
    commit_transaction(&deleter, "shared cache deleter").await?;

    let probe = writable_transaction(db, "shared cache tombstone probe")?;
    assert_optional_usizes(
        "committed shared tombstone visibility",
        &[None],
        &probe.query(vec![table_kv(SHARED_TABLE, 11, None)]).await,
    )
}

/// 返回旧值可以跨线程拥有；释放后缓存、动作日志和 COW 根不得继续持有其 payload。
async fn exercise_returned_value_lifecycle(db: &RealDb) -> TestResult<()> {
    create_btree_table(db, LIFECYCLE_TABLE, false, EnumType::Usize, EnumType::Usize).await?;
    let transaction = writable_transaction(db, "returned value lifecycle")?;
    let encoded = encode_usize(31_337);
    let shared = encoded.to_shared();
    let weak = Arc::downgrade(&shared);
    drop(encoded);
    transaction
        .upsert(vec![TableKV::new(
            Atom::from(LIFECYCLE_TABLE),
            encode_usize(1),
            Some(Binary::from_shared(shared)),
        )])
        .await
        .map_err(|error| format!("upserting lifecycle baseline failed: {error:?}"))?;

    let mut deleted = transaction
        .delete(vec![table_kv(LIFECYCLE_TABLE, 1, None)])
        .await
        .map_err(|error| format!("deleting lifecycle value failed: {error:?}"))?;
    if deleted.len() != 1 {
        return Err(format!(
            "lifecycle delete result count mismatch: expected=1, observed={}",
            deleted.len()
        ));
    }
    let old = deleted
        .pop()
        .flatten()
        .ok_or_else(|| "lifecycle delete did not return the cached old value".to_string())?;
    if weak.strong_count() != 1 {
        return Err(format!(
            "old payload has hidden owners after delete: expected=1, observed={}",
            weak.strong_count()
        ));
    }

    let decoded = std::thread::spawn(move || decode_usize(&old))
        .join()
        .map_err(|_| "old-value consumer thread panicked".to_string())??;
    if decoded != 31_337 {
        return Err(format!(
            "cross-thread old value mismatch: expected=31337, observed={decoded}"
        ));
    }
    if weak.upgrade().is_some() {
        return Err(
            "old payload remained strongly referenced after returned value was dropped".to_string(),
        );
    }

    assert_optional_usizes(
        "lifecycle tombstone visibility",
        &[None],
        &transaction
            .query(vec![table_kv(LIFECYCLE_TABLE, 1, None)])
            .await,
    )
}

/// redb-only Key 必须返回删除调用时的精确旧值，重复删除不得穿透 tombstone。
async fn exercise_redb_only_boundary(rt: &MultiTaskRuntime<()>, db: &RealDb) -> TestResult<()> {
    create_btree_table(db, REDB_TABLE, true, EnumType::Str, EnumType::Usize).await?;
    let keys: Vec<_> = (0..PERSISTED_KEYS).map(persisted_key).collect();
    let ordinary_collector_keys: Vec<_> = (0..ORDINARY_COLLECTOR_KEYS)
        .map(ordinary_collector_key)
        .collect();
    let writer = writable_transaction(db, "redb-only writer")?;
    let mut baseline_input: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(index, key)| {
            TableKV::new(
                Atom::from(REDB_TABLE),
                encode_string(key.clone()),
                Some(encode_usize(index + 10_000)),
            )
        })
        .collect();
    baseline_input.extend(
        ordinary_collector_keys
            .iter()
            .enumerate()
            .map(|(index, key)| {
                TableKV::new(
                    Atom::from(REDB_TABLE),
                    encode_string(key.clone()),
                    Some(encode_usize(index + 20_000)),
                )
            }),
    );
    writer
        .upsert(baseline_input)
        .await
        .map_err(|error| format!("upserting redb-only baseline failed: {error:?}"))?;
    commit_transaction(&writer, "redb-only writer").await?;
    wait_for_empty_cache(rt, db, REDB_TABLE, PERSISTENCE_DEADLINE).await?;

    let baseline_probe = writable_transaction(db, "redb-only baseline probe")?;
    let baseline = baseline_probe
        .query(
            keys.iter()
                .map(|key| TableKV::new(Atom::from(REDB_TABLE), encode_string(key.clone()), None))
                .collect(),
        )
        .await;
    let expected_baseline: Vec<_> = (0..PERSISTED_KEYS)
        .map(|index| Some(index + 10_000))
        .collect();
    assert_optional_usizes(
        "redb-only persisted baseline",
        &expected_baseline,
        &baseline,
    )?;
    let ordinary_collector_baseline = baseline_probe
        .query(
            ordinary_collector_keys
                .iter()
                .map(|key| {
                    TableKV::new(
                        Atom::from(REDB_TABLE),
                        encode_string(key.clone()),
                        None,
                    )
                })
                .collect(),
        )
        .await;
    assert_optional_usizes(
        "redb-only ordinary collector baseline",
        &(0..ORDINARY_COLLECTOR_KEYS)
            .map(|index| Some(index + 20_000))
            .collect::<Vec<_>>(),
        &ordinary_collector_baseline,
    )?;
    drop(baseline_probe);

    let mut delete_input: Vec<_> = keys
        .iter()
        .map(|key| TableKV::new(Atom::from(REDB_TABLE), encode_string(key.clone()), None))
        .collect();
    delete_input.extend(ordinary_collector_keys.iter().map(|key| {
        TableKV::new(Atom::from(REDB_TABLE), encode_string(key.clone()), None)
    }));
    let mut normal_delete_input = vec![TableKV::new(
        Atom::from(REDB_TABLE),
        encode_string(keys[0].clone()),
        None,
    )];
    normal_delete_input.extend(ordinary_collector_keys.iter().map(|key| {
        TableKV::new(Atom::from(REDB_TABLE), encode_string(key.clone()), None)
    }));
    if let Some(invalid) = normal_delete_input
        .iter()
        .find(|item| item.key.len() == 0 || item.key.len() > u16::MAX as usize) {
        return Err(format!(
            "ordinary collector fixture exceeds the WAL key boundary: key_bytes={}, valid_range=1..={}",
            invalid.key.len(),
            u16::MAX
        ));
    }
    let normal_delete_bytes: usize = normal_delete_input.iter().map(|item| item.key.len()).sum();
    if normal_delete_bytes <= BTREE_WAITS_LIMIT_BYTES {
        return Err(format!(
            "ordinary delete batch cannot independently trigger collector: bytes={normal_delete_bytes}, threshold={BTREE_WAITS_LIMIT_BYTES}"
        ));
    }
    let dirty_delete_input: Vec<_> = keys
        .iter()
        .skip(1)
        .map(|key| TableKV::new(Atom::from(REDB_TABLE), encode_string(key.clone()), None))
        .collect();
    if let Some(invalid) = dirty_delete_input
        .iter()
        .find(|item| item.key.len() == 0 || item.key.len() > u16::MAX as usize) {
        return Err(format!(
            "dirty collector fixture exceeds the WAL key boundary: key_bytes={}, valid_range=1..={}",
            invalid.key.len(),
            u16::MAX
        ));
    }
    let dirty_delete_bytes: usize = dirty_delete_input.iter().map(|item| item.key.len()).sum();
    if dirty_delete_bytes <= BTREE_WAITS_LIMIT_BYTES {
        return Err(format!(
            "dirty delete batch cannot independently trigger collector: bytes={dirty_delete_bytes}, threshold={BTREE_WAITS_LIMIT_BYTES}"
        ));
    }

    // 普通根处理第一个目标 Key 和 17 个合法大 Key。该批次自身超过 collector 阈值，提交后
    // 可以先严格等待持久化闭环，再启动 dirty 根；测试不依赖两个 detached 入队任务的顺序。
    let deleter = writable_transaction(db, "redb-only ordinary deleter")?;
    let mut deleted = deleter
        .delete(normal_delete_input.clone())
        .await
        .map_err(|error| format!("deleting redb-only values failed: {error:?}"))?;
    assert_optional_usizes(
        "redb-only normal delete returns persisted old values",
        &std::iter::once(Some(10_000))
            .chain((0..ORDINARY_COLLECTOR_KEYS).map(|index| Some(index + 20_000)))
            .collect::<Vec<_>>(),
        &deleted,
    )?;

    let redb_owner = deleted
        .get_mut(0)
        .and_then(Option::take)
        .ok_or_else(|| "redb-only delete did not return its first old value".to_string())?;
    let shared = redb_owner.to_shared();
    let weak = Arc::downgrade(&shared);
    drop(shared);
    // redb-only 普通删除必须同时保留调用方返回值和事务首次读取基线。后者不是泄漏：
    // 无版本缓存时 prepare 依赖它做值冲突比较，并应在事务闭环后释放。
    if weak.strong_count() != 2 {
        return Err(format!(
            "redb old value owner count before caller drop mismatch: expected=2, observed={}",
            weak.strong_count()
        ));
    }
    let decoded = std::thread::spawn(move || decode_usize(&redb_owner))
        .join()
        .map_err(|_| "redb old-value consumer thread panicked".to_string())??;
    if decoded != 10_000 {
        return Err(format!(
            "cross-thread redb old value mismatch: expected=10000, observed={decoded}"
        ));
    }
    if weak.strong_count() != 1 {
        return Err(format!(
            "redb old value baseline owner count after caller drop mismatch: expected=1, observed={}",
            weak.strong_count()
        ));
    }

    let repeated = deleter
        .delete(normal_delete_input.clone())
        .await
        .map_err(|error| format!("repeating normal redb-only delete failed: {error:?}"))?;
    assert_optional_usizes(
        "redb-only repeated normal delete stops at tombstone",
        &vec![None; 1 + ORDINARY_COLLECTOR_KEYS],
        &repeated,
    )?;
    assert_optional_usizes(
        "redb-only normal tombstone hides persisted value",
        &vec![None; 1 + ORDINARY_COLLECTOR_KEYS],
        &deleter.query(normal_delete_input).await,
    )?;
    commit_transaction(&deleter, "redb-only ordinary deleter").await?;
    if weak.upgrade().is_some() {
        return Err(
            "redb old value baseline remained owned after ordinary transaction commit".to_string(),
        );
    }
    wait_for_empty_cache(rt, db, REDB_TABLE, PERSISTENCE_DEADLINE).await?;

    // 其余 319 个 4 KiB Key 使用独立 dirty 根，且该批次自身超过 1 MiB collector 门槛。
    let dirty_deleter = writable_transaction(db, "redb-only dirty deleter")?;
    let dirty_deleted = dirty_deleter
        .dirty_delete(dirty_delete_input.clone())
        .await
        .map_err(|error| format!("dirty-deleting redb-only values failed: {error:?}"))?;
    assert_optional_usizes(
        "redb-only dirty delete returns persisted old values",
        &(1..PERSISTED_KEYS)
            .map(|index| Some(index + 10_000))
            .collect::<Vec<_>>(),
        &dirty_deleted,
    )?;

    let repeated_dirty = dirty_deleter
        .dirty_delete(dirty_delete_input.clone())
        .await
        .map_err(|error| format!("repeating dirty redb-only delete failed: {error:?}"))?;
    assert_optional_usizes(
        "redb-only repeated dirty delete stops at tombstones",
        &vec![None; PERSISTED_KEYS - 1],
        &repeated_dirty,
    )?;
    assert_optional_usizes(
        "redb-only dirty tombstones hide persisted values",
        &vec![None; PERSISTED_KEYS - 1],
        &dirty_deleter.dirty_query(dirty_delete_input).await,
    )?;
    commit_transaction(&dirty_deleter, "redb-only dirty deleter").await?;
    wait_for_empty_cache(rt, db, REDB_TABLE, PERSISTENCE_DEADLINE).await?;

    let final_probe = writable_transaction(db, "redb-only final probe")?;
    assert_optional_usizes(
        "redb-only values removed after persistence",
        &vec![None; PERSISTED_KEYS + ORDINARY_COLLECTOR_KEYS],
        &final_probe.query(delete_input).await,
    )
}

/// 新建空 Btree 的真实缺表错误必须详细记录并降级，不能撤销 tombstone 或阻止提交。
async fn exercise_redb_open_table_error(rt: &MultiTaskRuntime<()>, db: &RealDb) -> TestResult<()> {
    create_btree_table(db, REDB_ERROR_TABLE, true, EnumType::Str, EnumType::Usize).await?;
    CAPTURE_LOGGER.clear()?;

    let keys: Vec<_> = (0..PERSISTED_KEYS).map(persisted_key).collect();
    let delete_input: Vec<_> = keys
        .iter()
        .map(|key| {
            TableKV::new(
                Atom::from(REDB_ERROR_TABLE),
                encode_string(key.clone()),
                None,
            )
        })
        .collect();
    let deleter = writable_transaction(db, "redb open-table error deleter")?;
    let deleted = deleter
        .delete(delete_input.clone())
        .await
        .map_err(|error| format!("empty-table delete unexpectedly failed: {error:?}"))?;
    assert_optional_usizes(
        "redb open-table errors degrade to None",
        &vec![None; PERSISTED_KEYS],
        &deleted,
    )?;
    assert_optional_usizes(
        "redb open-table errors retain tombstones",
        &vec![None; PERSISTED_KEYS],
        &deleter.query(delete_input.clone()).await,
    )?;

    let error_records = CAPTURE_LOGGER.matching(REDB_READ_ERROR_MARKER)?;
    if error_records.len() != PERSISTED_KEYS {
        return Err(format!(
            "redb open-table log count mismatch: expected={}, observed={}, records={:?}",
            PERSISTED_KEYS,
            error_records.len(),
            error_records.iter().take(3).collect::<Vec<_>>()
        ));
    }
    let sample = &error_records[0];
    for required in [
        "stage=open_table",
        REDB_ERROR_TABLE,
        "table_path=",
        "key=",
        "key_len=",
        "source=",
        "transaction_uid=None",
        "TableDoesNotExist",
        "old_value=None",
        "tombstone=retained",
    ] {
        if !sample.contains(required) {
            return Err(format!(
                "redb error log is missing {required:?}: record={sample}"
            ));
        }
    }

    commit_transaction(&deleter, "redb open-table error deleter").await?;
    wait_for_empty_cache(rt, db, REDB_ERROR_TABLE, PERSISTENCE_DEADLINE).await?;
    let probe = writable_transaction(db, "redb open-table error final probe")?;
    assert_optional_usizes(
        "redb open-table degraded delete commits successfully",
        &vec![None; PERSISTED_KEYS],
        &probe.query(delete_input).await,
    )
}

/// 运行全部独立场景并聚合失败，避免首个旧值错误掩盖其它边界证据。
async fn exercise_matrix(rt: MultiTaskRuntime<()>, root: PathBuf) -> TestResult<()> {
    let db = build_database(&rt, &root).await?;
    let mut failures = Vec::new();

    if let Err(error) = exercise_private_cache(&db).await {
        failures.push(format!("private-cache: {error}"));
    }
    if let Err(error) = exercise_dirty_private_cache(&db).await {
        failures.push(format!("dirty-private-cache: {error}"));
    }
    if let Err(error) = exercise_shared_cache(&db).await {
        failures.push(format!("shared-cache: {error}"));
    }
    if let Err(error) = exercise_returned_value_lifecycle(&db).await {
        failures.push(format!("returned-value-lifecycle: {error}"));
    }
    if let Err(error) = exercise_redb_only_boundary(&rt, &db).await {
        failures.push(format!("redb-only: {error}"));
    }
    if let Err(error) = exercise_redb_open_table_error(&rt, &db).await {
        failures.push(format!("redb-open-table-error: {error}"));
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "Btree delete old-value matrix failed in {} scenario(s):\n{}",
            failures.len(),
            failures.join("\n")
        ))
    }
}

async fn build_database(rt: &MultiTaskRuntime<()>, root: &Path) -> TestResult<RealDb> {
    fs::create_dir_all(root)
        .map_err(|error| format!("creating test root {root:?} failed: {error}"))?;
    let wal_path = root.join("root-wal");
    let logger = CommitLoggerBuilder::new(rt.clone(), &wal_path)
        .log_file_limit(64 * 1024 * 1024)
        .build()
        .await
        .map_err(|error| format!("building CommitLogger at {wal_path:?} failed: {error}"))?;
    let manager = Transaction2PcManager::new(
        rt.clone(),
        GuidGen::new(0, std::process::id() as u16),
        logger,
    );
    KVDBManagerBuilder::new(rt.clone(), manager, root.join("database"))
        .startup(false)
        .await
        .map_err(|error| format!("starting Btree delete test database failed: {error}"))
}

async fn create_btree_table(
    db: &RealDb,
    table_name: &str,
    persistence: bool,
    key_type: EnumType,
    value_type: EnumType,
) -> TestResult<()> {
    let transaction = writable_transaction(db, &format!("DDL {table_name}"))?;
    transaction
        .create_table_with_options(
            Atom::from(table_name),
            KVTableMeta::new(
                KVDBTableType::BtreeOrdTab,
                persistence,
                key_type,
                value_type,
            ),
            CreateTableOptions::BtreeOrdTab(4 * 1024 * 1024, false),
            false,
        )
        .await
        .map_err(|error| format!("creating Btree table {table_name} failed: {error}"))?;
    commit_transaction(&transaction, &format!("DDL {table_name}")).await
}

fn writable_transaction(db: &RealDb, source: &str) -> TestResult<RealTransaction> {
    db.transaction(Atom::from(source), true, 10_000, 10_000)
        .ok_or_else(|| format!("database rejected transaction {source}"))
}

async fn commit_transaction(transaction: &RealTransaction, label: &str) -> TestResult<()> {
    let prepare = transaction
        .prepare_modified()
        .await
        .map_err(|error| format!("preparing {label} failed: {error:?}"))?;
    transaction
        .commit_modified(prepare)
        .await
        .map_err(|error| format!("committing {label} failed: {error:?}"))
}

async fn wait_for_empty_cache(
    rt: &MultiTaskRuntime<()>,
    db: &RealDb,
    table_name: &str,
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        match db.table_cache_size(&Atom::from(table_name)).await {
            Some(0) => return Ok(()),
            Some(_size) if Instant::now() < deadline => rt.timeout(10).await,
            Some(size) => {
                return Err(format!(
                    "{table_name} cache remained at {size} bytes after {timeout:?}"
                ))
            }
            None => {
                return Err(format!(
                    "{table_name} disappeared while waiting for persistence"
                ))
            }
        }
    }
}

fn table_kv(table: &str, key: usize, value: Option<usize>) -> TableKV {
    TableKV::new(
        Atom::from(table),
        encode_usize(key),
        value.map(encode_usize),
    )
}

fn encode_usize(value: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn decode_usize(value: &Binary) -> TestResult<usize> {
    let mut buffer = ReadBuffer::new(value.as_ref(), 0);
    usize::decode(&mut buffer).map_err(|error| format!("decoding BON usize failed: {error:?}"))
}

fn encode_string(value: String) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

fn persisted_key(index: usize) -> String {
    let prefix = format!("key={index:020};");
    assert!(prefix.len() <= PERSISTED_KEY_BYTES);
    let fill = char::from(b'a' + (index % 26) as u8);
    let mut key = String::with_capacity(PERSISTED_KEY_BYTES);
    key.push_str(&prefix);
    for _ in prefix.len()..PERSISTED_KEY_BYTES {
        key.push(fill);
    }
    key
}

fn ordinary_collector_key(index: usize) -> String {
    let prefix = format!("ordinary-collector-key={index:020};");
    assert!(prefix.len() <= ORDINARY_COLLECTOR_KEY_BYTES);
    let fill = char::from(b'A' + (index % 26) as u8);
    let mut key = String::with_capacity(ORDINARY_COLLECTOR_KEY_BYTES);
    key.push_str(&prefix);
    for _ in prefix.len()..ORDINARY_COLLECTOR_KEY_BYTES {
        key.push(fill);
    }
    key
}

fn assert_optional_usizes(
    label: &str,
    expected: &[Option<usize>],
    observed: &[Option<Binary>],
) -> TestResult<()> {
    let decoded: TestResult<Vec<_>> = observed
        .iter()
        .map(|value| value.as_ref().map(decode_usize).transpose())
        .collect();
    let decoded = decoded?;
    if decoded == expected {
        Ok(())
    } else {
        Err(format!(
            "{label} mismatch: expected={expected:?}, observed={decoded:?}"
        ))
    }
}

/// 在真实多线程运行时执行完整矩阵，并以同步截止防止异步挂起被误判为通过。
fn run_on_runtime<F, Fut>(timeout: Duration, build: F) -> TestResult<()>
where
    F: FnOnce(MultiTaskRuntime<()>) -> Fut,
    Fut: Future<Output = TestResult<()>> + Send + 'static,
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
    .map_err(|error| format!("spawning Btree delete matrix failed: {error:?}"))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("Btree delete matrix exceeded {timeout:?}: {error}"))?
}

#[test]
fn test_btree_delete_old_value_matrix() {
    init_capture_logger().expect("installing Btree delete capture logger must succeed");
    let temp_root = TempRoot::new("matrix").expect("creating Btree delete temp root must succeed");
    let root = temp_root.path().to_path_buf();
    run_on_runtime(RUNTIME_DEADLINE, move |rt| exercise_matrix(rt, root))
        .unwrap_or_else(|error| panic!("{error}"));
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
        let path = env::temp_dir().join(format!(
            "pi_db_btree_delete_{label}_{}_{}",
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
