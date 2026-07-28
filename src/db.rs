//! 数据库启动、表注册、管理入口与根事务树装配。
//!
//! 本模块位于公开数据库 API 与 `pi_async_transaction`/`pi_store` 之间：
//!
//! - [`crate::db::KVDBManagerBuilder`] 创建目录、加载内部 Meta 表和用户表，然后始终通过内部
//!   `try_repair` 重放未确认的根前导日志；
//! - [`crate::db::KVDBManager`] 共享 runtime、两阶段事务管理器、根 WAL logger、表注册表和事件通道；
//! - [`crate::db::KVDBTransaction`] 是公开事务句柄，应用层只能把
//!   [`crate::db::KVDBTransaction::RootTr`] 当作
//!   动作和生命周期入口，其余 variant 是事务树内部子节点；
//! - [`crate::db::RootTransaction`] 按首次触表顺序持有子事务，并把 prepare、根 WAL、子表发布和最终
//!   确认串成两阶段提交闭环。
//!
//! 当前 `close`、只读事务写入、timeout 和 prepare token 仍按实现事实记录，并不是最终或
//! 最佳设计。稳定事务/确认协议见 `docs/SEMANTIC_CONTRACTS.md#contract-transaction`，管理器
//! 当前契约由 `tests/manager_contract.rs` 验证，根生命周期由
//! `tests/root_transaction_lifecycle.rs` 验证。

use std::mem::swap;
use std::time::{Duration, Instant};
use std::convert::TryInto;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::collections::{VecDeque, HashMap, BTreeMap};
#[cfg(feature = "trace")]
use std::collections::HashSet;
use std::io::{Error, Result as IOResult, ErrorKind};
use std::sync::{Arc,
                OnceLock,
                atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering}};

use futures::{future::{FutureExt, BoxFuture}, stream::BoxStream, StreamExt};
use crossbeam_channel::bounded;
use async_lock::{Mutex, RwLock};
use async_channel::{Sender, Receiver, unbounded};
use dashmap::DashMap;
use lazy_static::lazy_static;
use bytes::BufMut;
use log::{info, error};
#[cfg(target_os = "linux")]
use libc::malloc_trim;
#[cfg(feature = "trace")]
use opentelemetry::{global,
                    metrics::{Counter, Gauge, Meter},
                    KeyValue};
use pi_atom::Atom;
use pi_bon::{WriteBuffer, ReadBuffer, Encode, Decode, ReadBonErr};
use pi_guid::Guid;
use pi_async_rt::{lock::spin_lock::SpinLock,
                  rt::{AsyncRuntime, AsyncValue,
                       multi_thread::MultiTaskRuntime}};
use pi_async_transaction::{AsyncTransaction, Transaction2Pc, Transaction2PcAllConflicts, UnitTransaction, SequenceTransaction, TransactionTree, AsyncCommitLog, ErrorLevel, TransactionError,
                           manager_2pc::{Transaction2PcStatus, Transaction2PcManager}};
use pi_async_file::file::create_dir;
use pi_hash::XHashMap;

use crate::{Binary,
            KVAction,
            KVDBTableType,
            KVTableMeta,
            TableTrQos,
            KVDBCommitConfirm,
            KVTableTrError,
            MAX_TABLE_NAME_BYTES,
            TableKey,
            TableKeyConflict,
            TableKeyVersion,
            Version,
            VersionConflictKind,
            key_version::{KeyVersionConfig,
                          KeyVersionRegistry,
                          KeyVersions,
                          PrepareMode,
                          TableVersionContext,
            VersionReceipt},
            tables::{KVTable,
                     TableKV,
                     meta_table::{MetaTable,
                                  MetaTabTr},
                     mem_ord_table::{MemoryOrderedTable,
                                     MemOrdTabTr},
                     log_ord_table::{LogOrderedTable,
                                     LogOrdTabTr},
                     log_write_table::{LogWriteTable,
                                       LogWTabTr},
                     b_tree_ord_table::{DEFAULT_CACHE_SIZE, BtreeOrderedTable,
                                        BtreeOrdTabTr}},
            utils::{CreateTableOptions, KVDBEvent}};
#[cfg(feature = "trace")]
use crate::key_version::{KeyVersionApiMetricsSnapshot,
                         KeyVersionApiOperation,
                         KeyVersionCacheMetricsSnapshot};

///
/// 默认的数据库表元信息目录名
///
pub(crate) const DEFAULT_DB_TABLES_META_DIR: &str = ".tables_meta";

///
/// 默认的数据库表所在目录名
///
const DEFAULT_DB_TABLES_DIR: &str = ".tables";

///
/// 数据库未启动状态
///
const DB_UNSTARTUP_STATUS: u64 = 0;

///
/// 数据库正在初始化状态
///
const DB_INITING_STATUS: u64 = 1;

///
/// 数据库已初始化状态
///
const DB_INITED_STATUS: u64 = 2;

/// 根事务当前选择的业务协议。
///
/// Schema Meta 子节点不构成第四种协议；它可在 Unselected 阶段预先建立，并随之后唯一选择的
/// Ordinary 或 Versioned 协议进入同一棵 2PC 树。该状态只保护树装配，不替代 2PC 生命周期。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum RootTransactionProtocol {
    Unselected = 0,
    Ordinary = 1,
    Versioned = 2,
}

impl RootTransactionProtocol {
    #[inline]
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Unselected,
            1 => Self::Ordinary,
            2 => Self::Versioned,
            _ => unreachable!("invalid root transaction protocol: {value}"),
        }
    }
}

///
/// 数据库正在关闭状态
///
const DB_CLOSEING_STATUS: u64 = 3;

///
/// 数据库已关闭状态
///
const DB_CLOSED_STATUS: u64 = 4;

///
/// 启动数据库的源
///
const STARTUP_DB_SOURCE: &str = "Startup db";

/// 启动时从 Meta 表分批装配用户表的单批上限。
///
/// 该值只约束启动阶段的临时内存和单次异步装配规模，不是公开表数量限制，也不改变 Meta、
/// WAL、repair 或表存储格式。边界设计和真实 8193 表证据见
/// `docs/STARTUP_TABLE_BATCH_BOUNDARY_BUG.md#startup-table-batch-boundary-index`。
const STARTUP_TABLE_META_BATCH_LIMIT: usize = 8192;

/// 将本轮 Meta 项放入启动缓冲，并在旧缓冲已满时返回完整旧批次。
///
/// 满批次必须先从 `buffer` 中换出，随后 `current` 无条件进入已清空的新缓冲；否则第 8193、
/// 16385 等边界项会既不属于旧批次也不属于最终剩余批次。返回的旧批次由调用方按原流程
/// 异步装配。`current` 在装配 await 前只存在于 startup future 的私有局部 `Vec` 中，旧批次
/// 失败时会随返回路径释放，不会提前修改 registry、Meta、WAL 或后台任务。
#[inline]
fn stage_startup_table_meta<T>(buffer: &mut Vec<T>, current: T) -> Option<Vec<T>> {
    let full_batch = if buffer.len() >= STARTUP_TABLE_META_BATCH_LIMIT {
        let mut batch = Vec::with_capacity(buffer.len());
        swap(buffer, &mut batch);
        Some(batch)
    } else {
        None
    };
    buffer.push(current);
    full_batch
}

///
/// 修复数据库时的源
///
const REPAIR_DB_SOURCE: &str = "Repair db";

const DEFAULT_KEY_VERSION_TTL: Duration = Duration::from_secs(60 * 60);
const DEFAULT_KEY_VERSION_TTL_POLL_INTERVAL: Duration = Duration::from_secs(3 * 60);

// 整个进程内的 pi_db 实例共享同一个数据库指标 Meter，避免各指标组使用含义不同的 scope。
#[cfg(feature = "trace")]
static DATABASE_METER: OnceLock<Meter> = OnceLock::new();

#[cfg(feature = "trace")]
const DATABASE_METER_SCOPE: &str = "pi_db";

#[cfg(feature = "trace")]
const TABLE_CACHE_SIZE_METRIC: &str = "pi_db.db.table_cache_size";
#[cfg(feature = "trace")]
const KEY_VERSION_RECORD_COUNT_METRIC: &str = "pi_db.db.key_version_cache_record_count";
#[cfg(feature = "trace")]
const KEY_VERSION_ESTIMATED_MEMORY_METRIC: &str = "pi_db.db.key_version_cache_estimated_memory_bytes";
#[cfg(feature = "trace")]
const KEY_VERSION_QUERY_CALLS_METRIC: &str = "pi_db.db.key_version_query_calls";
#[cfg(feature = "trace")]
const KEY_VERSION_2PC_CALLS_METRIC: &str = "pi_db.db.key_version_2pc_calls";
#[cfg(feature = "trace")]
const TRANSACTION_LIFECYCLE_METRIC: &str = "pi_db.db.transaction_lifecycle";

/// 获取进程级共享的数据库指标 Meter。
///
/// 调用方必须在启动数据库前通过 OpenTelemetry `global` 安装 MeterProvider；该顺序由
/// `pi_launcher` 的启动流程保证。这里不能依赖 `pi_logger::opentelemetry::is_init()`：新版
/// observability 初始化会直接设置全局 Provider，却不会设置旧兼容模块的初始化标记。
/// 如果外部没有启用指标 Provider，OpenTelemetry 会按其标准语义返回 no-op Meter；本函数
/// 不等待、不轮询，也不会阻塞数据库 trace loop。
#[cfg(feature = "trace")]
pub(crate) fn get_database_meter() -> &'static Meter {
    DATABASE_METER.get_or_init(|| global::meter(DATABASE_METER_SCOPE))
}

/// 复用进程级数据库 Meter 的 trace-only 指标集合；数据库热路径只写内部原子，不直接调用 Meter。
#[cfg(feature = "trace")]
struct DatabaseTraceInstruments {
    table_cache: Gauge<u64>,
    key_version_records: Gauge<u64>,
    key_version_memory: Gauge<u64>,
    key_version_query_calls: Counter<u64>,
    key_version_2pc_calls: Counter<u64>,
    transaction_lifecycle: Counter<u64>,
    query_success: [KeyValue; 1],
    query_failure: [KeyValue; 1],
    prepare_success: [KeyValue; 2],
    prepare_failure: [KeyValue; 2],
    commit_success: [KeyValue; 2],
    commit_failure: [KeyValue; 2],
    transaction_created: [KeyValue; 1],
    transaction_closed: [KeyValue; 1],
}

#[cfg(feature = "trace")]
impl DatabaseTraceInstruments {
    fn new(meter: &Meter) -> Self {
        Self {
            table_cache: meter.u64_gauge(TABLE_CACHE_SIZE_METRIC).build(),
            key_version_records: meter
                .u64_gauge(KEY_VERSION_RECORD_COUNT_METRIC)
                .build(),
            key_version_memory: meter
                .u64_gauge(KEY_VERSION_ESTIMATED_MEMORY_METRIC)
                .build(),
            key_version_query_calls: meter
                .u64_counter(KEY_VERSION_QUERY_CALLS_METRIC)
                .build(),
            key_version_2pc_calls: meter
                .u64_counter(KEY_VERSION_2PC_CALLS_METRIC)
                .build(),
            transaction_lifecycle: meter
                .u64_counter(TRANSACTION_LIFECYCLE_METRIC)
                .build(),
            query_success: [KeyValue::new("result", "success")],
            query_failure: [KeyValue::new("result", "failure")],
            prepare_success: [KeyValue::new("phase", "prepare"),
                              KeyValue::new("result", "success")],
            prepare_failure: [KeyValue::new("phase", "prepare"),
                              KeyValue::new("result", "failure")],
            commit_success: [KeyValue::new("phase", "commit"),
                             KeyValue::new("result", "success")],
            commit_failure: [KeyValue::new("phase", "commit"),
                             KeyValue::new("result", "failure")],
            transaction_created: [KeyValue::new("event", "created")],
            transaction_closed: [KeyValue::new("event", "closed")],
        }
    }

    fn record_table(&self,
                    table: &Atom,
                    table_cache_size: u64,
                    version_metrics: KeyVersionCacheMetricsSnapshot) {
        let attributes = [KeyValue::new("table", table.as_str().to_string())];
        self.table_cache.record(table_cache_size, &attributes);
        self.key_version_records.record(version_metrics.record_count, &attributes);
        self.key_version_memory
            .record(version_metrics.estimated_memory_bytes, &attributes);
    }

    fn record_removed_table(&self, table: &Atom) {
        let attributes = [KeyValue::new("table", table.as_str().to_string())];
        self.key_version_records.record(0, &attributes);
        self.key_version_memory.record(0, &attributes);
    }

    fn record_api_delta(&self, delta: KeyVersionApiMetricsSnapshot) {
        if delta.query_success > 0 {
            self.key_version_query_calls.add(delta.query_success, &self.query_success);
        }
        if delta.query_failure > 0 {
            self.key_version_query_calls.add(delta.query_failure, &self.query_failure);
        }
        if delta.prepare_success > 0 {
            self.key_version_2pc_calls.add(delta.prepare_success, &self.prepare_success);
        }
        if delta.prepare_failure > 0 {
            self.key_version_2pc_calls.add(delta.prepare_failure, &self.prepare_failure);
        }
        if delta.commit_success > 0 {
            self.key_version_2pc_calls.add(delta.commit_success, &self.commit_success);
        }
        if delta.commit_failure > 0 {
            self.key_version_2pc_calls.add(delta.commit_failure, &self.commit_failure);
        }
    }

    fn record_transaction_delta(&self, delta: TransactionLifecycleMetricsSnapshot) {
        if delta.created > 0 {
            self.transaction_lifecycle.add(delta.created, &self.transaction_created);
        }
        if delta.closed > 0 {
            self.transaction_lifecycle.add(delta.closed, &self.transaction_closed);
        }
    }
}

/// tracing loop 一次读取的根事务对象生命周期累计计数。
#[cfg(feature = "trace")]
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
struct TransactionLifecycleMetricsSnapshot {
    created: u64,
    closed: u64,
}

#[cfg(feature = "trace")]
impl TransactionLifecycleMetricsSnapshot {
    fn delta_since(self, previous: Self) -> Self {
        Self {
            created: self.created.wrapping_sub(previous.created),
            closed: self.closed.wrapping_sub(previous.closed),
        }
    }
}

/// 只在 trace 构建中存在；不参与事务 manager 的注册、状态或资源释放判断。
#[cfg(feature = "trace")]
#[derive(Default)]
struct TransactionLifecycleMetrics {
    created: AtomicU64,
    closed: AtomicU64,
}

/// 键值对数据库管理器的一次性构建器。
///
/// 构建器拥有多线程 runtime、[`Transaction2PcManager`] 和三个词法路径；[`Self::new`] 不做
/// I/O，真正的目录创建、表加载、根 WAL 修复和后台任务启动发生在 [`Self::startup`] 或
/// [`Self::startup_with_listener`]。启动方法消费 `self`，同一构建器不能重复启动。
///
/// 该类型本身不提供配置持久化：LogOrdered/Btree 首次创建时的运行参数不会写入 Meta，重启
/// 使用固定默认值。启动不是事务原子操作，失败前已经创建的目录、文件或后台资源可能保留。
/// 真实入口和边界由 `tests/manager_contract.rs` 及恢复专项验证。
pub struct KVDBManagerBuilder<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    /// 执行启动 I/O、表 collector、repair、listener 和版本 TTL 的数据库 runtime。
    rt:                 MultiTaskRuntime<()>,
    /// 分配根事务 ID、登记 2PC 树并持有根 CommitLogger 的共享事务管理器。
    tr_mgr:             Transaction2PcManager<C, Log>,
    /// 调用方提供的数据库根路径词法副本；不等于根 WAL 路径。
    db_path:            PathBuf,
    /// 固定由 `db_path/.tables_meta` 派生的内部 Meta 表日志目录。
    tables_meta_path:   PathBuf,
    /// 固定由 `db_path/.tables` 派生的持久化用户表父目录。
    tables_path:        PathBuf,
    /// Key 版本记录的配置寿命；ZERO 完全关闭 TTL。
    key_version_ttl:    Duration,
    /// TTL 开启时的固定扫描间隔；TTL 关闭时包括 ZERO 在内均被忽略。
    key_version_ttl_poll_interval: Duration,
}

/*
* 键值对数据库管理器构建器同步方法
*/
impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVDBManagerBuilder<C, Log> {
    /// 保存 runtime、事务管理器和数据库根路径，构造数据库启动计划。
    ///
    /// `rt` 必须是可继续接收任务的多线程 runtime；启动、表 collector、事件监听和最终 WAL
    /// 确认都会克隆并长期使用它。`tr_mgr` 的 commit logger 决定根 WAL 的真实位置，它可以与
    /// `path` 分离。`path` 只按 [`AsRef<Path>`] 复制，不会 canonicalize、创建、校验权限或
    /// 检查路径逃逸；相对路径仍相对于后续进程工作目录解释。
    ///
    /// 本函数为 O(p) 时间和空间，p 为路径长度；除分配和所有权转移外无副作用，不持锁、
    /// 不执行 I/O，也不会启动任务。
    pub fn new<P: AsRef<Path>>(rt: MultiTaskRuntime<()>,
                               tr_mgr: Transaction2PcManager<C, Log>,
                               path: P) -> Self {
        let db_path = path.as_ref().to_path_buf();
        let tables_meta_path = db_path.join(DEFAULT_DB_TABLES_META_DIR);
        let tables_path = db_path.join(DEFAULT_DB_TABLES_DIR);

        KVDBManagerBuilder {
            rt,
            tr_mgr,
            db_path,
            tables_meta_path,
            tables_path,
            key_version_ttl: DEFAULT_KEY_VERSION_TTL,
            key_version_ttl_poll_interval: DEFAULT_KEY_VERSION_TTL_POLL_INTERVAL,
        }
    }

    /// 设置 Key 版本记录的 TTL。
    ///
    /// 默认一小时；`Duration::ZERO` 关闭自动淘汰且不会创建后台 TTL 任务。配置只影响版本
    /// 证据的内存生命周期，不删除表数据，也不改变 WAL 和数据文件持久化语义。
    /// 最小单位是 1ms：非零 sub-ms 值按 1ms 处理，其余不足 1ms 的小数直接忽略。历史
    /// `BUG-KV-TTL-001` 已通过 deadline 基准向上取整修复；版本不得早于量化后的有效 TTL
    /// 淘汰，证据和边界见 `docs/KEY_VERSION_TTL_EARLY_EXPIRY_BUG.md`。
    pub fn key_version_ttl(mut self, ttl: Duration) -> Self {
        self.key_version_ttl = ttl;
        self
    }

    /// 设置 Key 版本 TTL 的固定轮询间隔。
    ///
    /// 默认三分钟。TTL 开启时 ZERO 属于非法启动配置；TTL 关闭时该值不生效。
    /// 间隔同样使用 1ms 最小单位并忽略其余小数；这一量化属于配置语义，不是
    /// `BUG-KV-TTL-001`。
    pub fn key_version_ttl_poll_interval(mut self, interval: Duration) -> Self {
        self.key_version_ttl_poll_interval = interval;
        self
    }
}

/*
* 键值对数据库管理器构建器异步方法
*/
impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVDBManagerBuilder<C, Log> {
    /// 启动不带事件回调的数据库。
    ///
    /// `enable_accelerated_repair` 只传给本次从 Meta 加载以及根 WAL repair-create 的 Btree；
    /// Btree 保存该值并在后续 redb 写事务中调用 `set_quick_repair`。构建器不会把它保存成
    /// manager 的全局建表默认值，启动后显式创建 Btree 仍使用对应建表 API 自己的同名参数。
    /// 无论该值为 `false` 还是 `true`，启动都会执行完整的内部 `try_repair` 根 WAL 扫描，而
    /// 不是切换成另一套 `try_quick_repair` 前导日志协议；Meta、Memory、LogOrdered 和 LogWrite
    /// 不使用该开关。
    ///
    /// 成功返回共享的 [`KVDBManager`]。未安装 listener，因此
    /// [`KVDBManager::report_transaction_info`] 会返回 `ConnectionAborted`。TTL 校验、目录创建、
    /// 表名校验、批量装配的显式错误和根 WAL repair 错误通过 [`IOResult`] 返回；其中部分错误会
    /// 被附加启动上下文。当前并非所有启动失败都能返回 `Err`：Meta/持久表构造及受信元数据解码
    /// 仍有 panic 分支；批量表任务 panic 或 runtime 拒绝任务时，共享完成值可能永远不被设置，
    /// 使启动 future 持续等待。这不是最终或最佳错误模型，调用方不能依赖 panic 文本或无限等待
    /// 作为稳定契约，详见 `FIND-CTOR-001`、`FIND-CODEC-001`。
    ///
    /// 启动不具备 rollback/cancellation 原子性，失败前已产生的目录、文件、已注册表或表 collector
    /// 可能保留；listener、版本 TTL 和 trace 只在表加载及根 WAL repair 成功后启动。
    /// 唯一保证零数据库副作用的配置拒绝是“TTL 非零且轮询间隔为 ZERO”：它在创建数据库目录、
    /// 表、channel 和后台任务前返回 `InvalidInput`。TTL 为 ZERO 时轮询值不生效，ZERO 合法。
    /// 目录创建自身的错误也会在 manager/表/listener 建立前返回，但调用前已经存在的路径和调用方
    /// 已构造的根 logger 不属于本 API 的回滚对象。
    ///
    /// 运行时间为 O(t + w)，t 为 Meta 中用户表数，w 为待扫描/重放 WAL 字节数，并包含真实
    /// 异步文件 I/O、同步锁和表引擎打开成本。存在未确认 WAL 时，当前 replay callback 会同步
    /// 等待投递到数据库 runtime 的 repair task；调用方必须由该 runtime 之外的线程驱动 startup，
    /// 或确保等待期间至少还有一个可执行 worker。单 worker 配置本身合法，现有 pi-launcher 正是
    /// 由外部启动线程驱动；只有 startup 占满同一 runtime 全部 worker 的特定窗口会自阻塞。
    /// 该现状限制、生产排除条件和红线测试见 README 的“启动恢复执行上下文”。
    pub async fn startup(self, enable_accelerated_repair: bool) -> IOResult<KVDBManager<C, Log>> {
        self
            .startup_with_listener::<fn(&KVDBManager<C, Log>, &Transaction2PcManager<C, Log>, &mut Vec<KVDBEvent<Guid>>)>(enable_accelerated_repair, None)
            .await
    }

    /// 启动数据库，并可选安装批量事件回调。
    ///
    /// 启动顺序是：确保 Meta/用户表目录存在，打开内部 Meta 表，按 Meta 快照批量加载用户表，
    /// 调用内部 `try_repair` 修复未确认根 WAL，清空仅供恢复使用的 Key 版本并启动版本 TTL，最后
    /// 启动 listener/trace 任务并把数据库标为可用。各表构造时可能已经启动自己的 collector；
    /// 全局版本 TTL、listener 和 trace 不会先于根 WAL repair 启动。
    /// `enable_accelerated_repair` 的语义与 [`Self::startup`] 相同。
    ///
    /// `db_event_listener=None` 不创建事件通道。传入 `Some` 时，回调被保存到一个长期 runtime
    /// 任务中，并以 `FnMut(&KVDBManager, &Transaction2PcManager, &mut Vec<KVDBEvent>)` 形式同步、
    /// 串行调用。单批上限是 3072：持续有事件时达到上限立即回调；不足上限时在最多五轮 10ms
    /// 空闲等待后回调。这个等待是聚合窗口而不是实时交付 SLA。框架跨轮复用同一个 Vec，回调
    /// 必须在返回前 `drain` 或 `clear` 已处理元素；框架不会清空 Vec。回调阻塞会占用 worker，
    /// panic 会终止监听任务；通道无界，生产速度长期超过消费速度会增长内存。回调可以读取共享
    /// manager，但重入异步维护/DDL 时必须自行避免锁顺序问题。
    ///
    /// 返回、错误、副作用、取消安全和复杂度与 [`Self::startup`] 相同。启动过程会分配表注册表、
    /// channel 和表对象并执行文件 I/O；不是幂等的“探测”操作，也不保证多个进程或 manager
    /// 可以同时打开同一路径。基础管理行为由 `tests/manager_contract.rs` 验证，正常批处理和真实
    /// collector 事件由 `tests/manager_listener_contract.rs` 验证。
    pub async fn startup_with_listener<F>(
        self,
        enable_accelerated_repair: bool,
        db_event_listener: Option<F>
    ) -> IOResult<KVDBManager<C, Log>>
    where F: FnMut(&KVDBManager<C, Log>, &Transaction2PcManager<C, Log>, &mut Vec<KVDBEvent<Guid>>) + Send + Sync + 'static
    {
        // TTL 配置必须先于目录、文件、channel 和表对象创建完成校验，保证 InvalidInput 零副作用。
        let key_version_config = KeyVersionConfig::new(self.key_version_ttl,
                                                       self.key_version_ttl_poll_interval)?;
        if !self.tables_meta_path.exists() {
            //指定路径的元信息表目录不存在，则创建
            let _ = create_dir(self.rt.clone(), self.tables_meta_path.clone()).await?;
        }

        if !self.tables_path.exists() {
            //指定路径的表目录不存在，则创建
            let _ = create_dir(self.rt.clone(), self.tables_path.clone()).await?;
        }

        //创建键值对数据库管理器
        let rt = self.rt;
        let tr_mgr = self.tr_mgr;
        let db_path = self.db_path;
        let tables_meta_path = self.tables_meta_path;
        let tables_path = self.tables_path;
        let tables = Arc::new(RwLock::new(XHashMap::default()));
        let (key_versions, key_version_ttl_receiver) = KeyVersionRegistry::new(key_version_config);
        let status = AtomicU64::new(DB_INITING_STATUS);
        let (notifier, listener) = if db_event_listener.is_some() {
            let (notifier, listener) = unbounded();
            (Some(notifier), Some(listener))
        } else {
            (None, None)
        };
        let inner = InnerKVDBManager {
            rt,
            tr_mgr,
            db_path,
            tables_meta_path,
            tables_path,
            tables,
            key_versions,
            status,
            listener,
            notifier,
            #[cfg(feature = "trace")]
            transaction_metrics: TransactionLifecycleMetrics::default(),
        };
        let db_mgr = KVDBManager(Arc::new(inner));

        // 加载并注册元信息表。MetaTable::new 当前没有 Result 通道，底层日志 open/load 失败会
        // panic，而不是沿 startup 的 IOResult 返回；这是 FIND-CTOR-001 的非最终错误边界。
        let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
        let meta_table: MetaTable<C, Log> =
            MetaTable::new(db_mgr.0.rt.clone(),
                           db_mgr.tables_meta_path().to_path_buf(),
                           meta_table_name.clone(),
                           512 * 1024 * 1024,
                           2 * 1024 * 1024,
                           None,
                           2 * 1024 * 1024,
                           true,
                           16 * 1024 * 1024,
                           60 * 1000,
                           db_mgr.0.notifier.clone()).await;
        let meta_versions = db_mgr.0.key_versions.create_table_versions();
        db_mgr.0.key_versions.install(meta_table_name.clone(), meta_versions.clone());
        db_mgr.0.tables.write().await.insert(meta_table_name.clone(),
                                             RegisteredTable::new(KVDBTable::MetaTab(meta_table),
                                                                  meta_versions));

        //根据元信息表的元信息，加载其它表，加载操作使用的事务，不需要预提交和提交
        let mut tr = db_mgr
            .transaction(Atom::from(STARTUP_DB_SOURCE),
                         true,
                         1000,
                         1000)
            .unwrap();
        let mut meta_iterator = tr
            .values(meta_table_name.clone(),
                    None,
                    false)
            .await
            .unwrap();
        let mut table_metas_buf = Vec::with_capacity(STARTUP_TABLE_META_BATCH_LIMIT);
        let default_log_table_options = CreateTableOptions::LogOrdTab(512 * 1024 * 1024,
                                                                      2 * 1024 * 1024,
                                                                      2 * 1024 * 1024);
        let default_b_tree_table_options = CreateTableOptions::BtreeOrdTab(16 * 1024 * 1024,
                                                                           true);
        let now = Instant::now();
        while let Some((key, value)) = meta_iterator.next().await {
            let table_name = match binary_to_table(&key) {
                Err(e) => {
                    //反序列化表名失败
                    return Err(Error::new(ErrorKind::Other, format!("From binary to table name failed, reason: {:?}", e)));
                },
                Ok(table_name) => {
                    //反序列化表名成功
                    table_name
                }
            };

            validate_table_name(&table_name,
                                ErrorKind::InvalidData,
                                "load table metadata")?;

            if table_name == meta_table_name {
                //忽略元信息表
                continue;
            }
            // Meta value 来自持久化受信编码；当前无错误 decoder 对截断/非法判别值可能 panic。
            // 不得把前面的表名 InvalidData 分支外推成“所有损坏 Meta 都返回 Err”，见
            // FIND-CODEC-001。
            let table_meta = KVTableMeta::from(value);

            let table_options = match table_meta.table_type() {
                KVDBTableType::LogOrdTab => {
                    Some(default_log_table_options.clone())
                },
                KVDBTableType::BtreeOrdTab => {
                    Some(default_b_tree_table_options.clone())
                },
                _ => None,
            };
            // 先从已满缓冲取出旧批次，再无条件暂存当前项。这样恰好 8192 项仍由循环后的
            // 剩余分支加载；第 8193 项则留在下一批，不会因本轮 flush 被跳过。
            let table_metas = match stage_startup_table_meta(
                &mut table_metas_buf,
                (table_name, table_meta, table_options)
            ) {
                None => continue,
                Some(table_metas) => table_metas,
            };

            //异步批量加载表
            if let Err(e) = tr.create_multiple_tables(
                table_metas,
                true,
                enable_accelerated_repair
            ).await {
                //加载指定的表失败，则立即返回错误原因
                db_mgr.0.status.store(DB_UNSTARTUP_STATUS, Ordering::SeqCst);
                return Err(Error::new(ErrorKind::Other,
                                      format!("Load table failed, tables_path: {:?}, reason: {:?}",
                                              db_mgr.tables_path(),
                                              e)));
            }
        }
        if table_metas_buf.len() > 0 {
            //异步批量加载剩余的表
            if let Err(e) = tr.create_multiple_tables(
                table_metas_buf,
                true,
                enable_accelerated_repair
            ).await {
                //加载指定的表失败，则立即返回错误原因
                db_mgr.0.status.store(DB_UNSTARTUP_STATUS, Ordering::SeqCst);
                return Err(Error::new(ErrorKind::Other,
                                      format!("Load table failed, tables_path: {:?}, reason: {:?}",
                                              db_mgr.tables_path(),
                                              e)));
            }
        }
        info!("Load db succeeded, tables: {:?}, time: {:?}",
            db_mgr.table_size().await,
            now.elapsed());
        drop(meta_iterator);
        drop(tr);

        // 如果有未确认的提交日志，则尝试修复数据库表数据。当前 pi_store 在轮询 start_replay
        // 时同步调用下面的 callback，而 callback 会等待投递到 db runtime 的 repair task；因此
        // 驱动 startup 的线程不得同时占满该 runtime 的全部 worker。pi-launcher/pi_db_server 当前
        // 从外部启动线程 block_on 此 future，哪怕数据库只有一个 worker，也仍由空闲 worker 执行
        // repair。这里不能把“单 worker”本身误标为故障条件，也不能在未审计 checkpoint 顺序前
        // 把 callback 改成提前返回。详见 README“启动恢复执行上下文”和 FIND-REPAIR-001。
        let now = Instant::now();
        match db_mgr.try_repair(enable_accelerated_repair).await {
            Err(e) => {
                //有未确认的提交日志，且尝试修复数据库表数据失败，则立即返回错误原因
                return Err(e);
            },
            Ok((repaired_log_len, repaired_bytes_len)) => {
                //尝试修复数据库表数据成功
                if repaired_log_len > 0 {
                    //未确认的提交日志
                    info!("Repair db succeeded, logs: {}, bytes: {}, time: {:?}",
                        repaired_log_len,
                        repaired_bytes_len,
                        now.elapsed());
                }
            }
        }

        // repair 期间产生的版本只服务内部恢复，不得暴露给启动后的外部缓存。
        db_mgr.0.key_versions.clear_records();
        db_mgr.0.key_versions.start_ttl_task(db_mgr.0.rt.clone(), key_version_ttl_receiver);

        if let Some(mut handle) = db_event_listener {
            //指定了数据库事件监听器
            let db_mgr_copy = db_mgr.clone();
            let listener = db_mgr
                .0
                .listener
                .as_ref()
                .unwrap()
                .clone();


            let _ = db_mgr.0.rt.spawn(async move {
                let mut events = Vec::with_capacity(3072);
                loop {
                    let mut try_count = 5usize;
                    while events.len() < 3072 {
                        //未达单次事件处理上限
                        if let Ok(event) = listener.try_recv() {
                            //有事件
                            try_count = 5; //重置重试次数
                            events.push(event);
                        } else {
                            //无事件
                            if try_count > 0 {
                                //未达重试限制，则稍候重试
                                try_count = try_count.checked_sub(1).unwrap_or(0);
                                db_mgr_copy
                                    .0
                                    .rt
                                    .timeout(10)
                                    .await;
                                continue;
                            }

                            //已达重试限制
                            if events.len() > 0 {
                                //有待处理的事件，则立即处理
                                try_count = 5; //重置重试次数
                                handle(&db_mgr_copy, &db_mgr_copy.0.tr_mgr, &mut events);
                                break;
                            } else {
                                //没有待处理的事件，则等待
                                if let Ok(event) = listener.recv().await {
                                    //有事件
                                    try_count = 5; //重置重试次数
                                    events.push(event);
                                }
                            }
                        }
                    }

                    if events.len() > 0 {
                        //有待处理的事件，则立即处理
                        handle(&db_mgr_copy, &db_mgr_copy.0.tr_mgr, &mut events);
                    }
                }
            });
        }

        //启动跟踪系统
        #[cfg(feature = "trace")]
        {
            let rt_copy = db_mgr.0.rt.clone();
            let db_mgr_copy = db_mgr.clone();
            let _ = db_mgr.0.rt.spawn(async move {
                loop_tracing(rt_copy, db_mgr_copy, 15000).await;
            });
        }

        db_mgr.0.status.store(DB_INITED_STATUS, Ordering::SeqCst); //设置数据库状态为已初始化
        info!("Startup db succeeded");

        //在Linux下启动完成后清理一次内存
        #[cfg(target_os = "linux")]
        db_mgr.cleanup_buffer_after_collect_table();

        Ok(db_mgr)
    }
}

/// 已启动数据库的共享管理句柄。
///
/// clone 只增加内部 [`Arc`] 强引用，所有 clone 共享表注册表、关闭状态、事务管理器、根 WAL、
/// listener 通道和 runtime。句柄被 drop 不等于数据库关闭：表 collector、listener 和 runtime
/// 任务可能继续持有 manager/table 引用；[`Self::close`] 也只是立即禁止新事务的软关闭标记，
/// 不等待数据文件、WAL 确认、后台任务或文件句柄释放。完整边界见
/// `docs/SEMANTIC_CONTRACTS.md#q-close-001` 和 `FIND-LIFE-001`。
///
/// 管理器允许跨线程共享；各表注册变更通过异步 `RwLock`，状态通过原子变量，事务和 logger
/// 的并发安全依赖各自公开契约。真实管理 API 矩阵见 `tests/manager_contract.rs`。
pub struct KVDBManager<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerKVDBManager<C, Log>>);

// SAFETY: manager 只移动一个 Arc。Inner 中 registry 使用 async RwLock，状态使用 AtomicU64，
// channel/runtime/Transaction2PcManager/AsyncCommitLog 均由其类型契约提供跨线程同步；路径在
// 构造后只读。C 不以裸值直接存入 manager，而只出现在已要求 Send 的事务依赖类型中。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for KVDBManager<C, Log> {}
// SAFETY: 所有通过共享 manager 访问的可变状态都位于上述同步原语或依赖类型之后；公开路径
// 借用只读，Arc clone/drop 使用原子引用计数。该 impl 不赋予 listener 回调重入安全，调用方
// 仍须遵守 FIND-ASYNC-001 记录的锁顺序边界。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for KVDBManager<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Clone for KVDBManager<C, Log> {
    fn clone(&self) -> Self {
        KVDBManager(self.0.clone())
    }
}

/*
* 键值对数据库管理器同步方法
*/
impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>
> KVDBManager<C, Log> {
    /// 借用构建器保存的数据库根路径。
    ///
    /// 返回值与 `self` 生命周期相同，是构建时路径的词法副本，不保证 canonical、存在、可写，
    /// 也不表示 commit logger 的 WAL 路径。O(1)、纯只读、无分配、无锁和无 I/O；所有 clone
    /// 返回相同路径内容。该借用不读取数据库状态，调用 [`Self::close`] 后只要 manager owner
    /// 仍存活就继续有效。
    pub fn db_path(&self) -> &Path {
        &self.0.db_path
    }

    /// 借用内部 Meta 表目录。
    ///
    /// 路径固定为 `db_path/.tables_meta`，启动成功时应已存在；它是 Meta 表数据目录，不是
    /// 根 WAL 目录。返回借用不转移所有权，O(1)、无锁、无分配和无 I/O；软关闭不改变路径。
    pub fn tables_meta_path(&self) -> &Path {
        &self.0.tables_meta_path
    }

    /// 借用持久化用户表的父目录。
    ///
    /// 路径固定为 `db_path/.tables`。Memory 表即使 `persistence=true` 也没有该目录下的数据
    /// 文件；该标志只使动作进入根 WAL。返回借用为 O(1)、只读、无锁、无分配和无 I/O；
    /// 软关闭不改变路径。
    pub fn tables_path(&self) -> &Path {
        &self.0.tables_path
    }

    /// 创建尚未注册到两阶段管理器的根事务句柄。
    ///
    /// `source` 是事务来源标签，会原样传给全部子事务，并供 `Transaction2PcManager` 的来源
    /// 计数/限流以及表级诊断和事件使用；它不参与 TID/CID 生成，事务身份只在后续 `start`
    /// 中由 `GuidGen` 分配。当前上游来源计数的严格并发线性化尚未验收，见
    /// `FIND-TR-SOURCE-001`，因此不能把本函数保存标签解释为并发限流已经形成强保证。
    ///
    /// `is_writable` 决定 prepare/commit 主路径，但当前动作 API 没有统一拒绝只读写入，只读写
    /// 可能先返回成功、随后在 prepare 快路中被静默丢弃。这是 `FIND-TR-001` 的当前实现事实，
    /// 不是最终或最佳只读契约。`prepare_timeout` 和 `commit_timeout` 仅按原始 `u64` 保存并传给
    /// 子事务，当前没有计时、取消或错误路径，见 `FIND-TIMEOUT-001`；`0` 和 `u64::MAX` 都不会
    /// 被本函数拒绝或归一化。
    ///
    /// 数据库状态为初始化中或已初始化时返回 `Some(RootTr)`；调用 [`Self::close`] 后返回
    /// `None`。创建本身不会分配事务 UID、注册 active transaction 或写 WAL；普通
    /// [`KVDBTransaction::prepare_modified`] / [`KVDBTransaction::prepare_modified_conflicts`]
    /// 或版本 [`KVDBTransaction::prepare_with_version`] 才按各自协议进入首次 start/prepare。
    /// 在 close 前已经创建的句柄当前仍可继续 prepare/commit，这是软关闭边界而不是
    /// graceful shutdown 保证。
    ///
    /// 根事务是否需要持久化由实际触达且需要持久化的写子事务决定。函数为摊销 O(1)，会分配
    /// Arc 和空的子事务 map/list，不执行文件 I/O、不 await；句柄反向持有 manager clone，因而
    /// 会延长数据库对象生命周期，但没有从 manager 指回该事务的环，直到 prepare 注册为止。
    /// 完整构造、共享 owner、子节点继承、登记和性能契约见
    /// `docs/ROOT_TRANSACTION_CONSTRUCTION_ACCEPTANCE.md#root-transaction-construction-index`。
    pub fn transaction(&self,
                       source: Atom,
                       is_writable: bool,
                       prepare_timeout: u64,
                       commit_timeout: u64) -> Option<KVDBTransaction<C, Log>> {
        let status = self.0.status.load(Ordering::Relaxed);
        if status != DB_INITING_STATUS && status != DB_INITED_STATUS {
            //当前数据库状态不允许创建键值对数据库的根事务，则立即返回空
            return None;
        }

        let tid = SpinLock::new(None);
        let cid = SpinLock::new(None);
        let status = SpinLock::new(Transaction2PcStatus::Start);
        let childs_map = SpinLock::new(XHashMap::default());
        let childs = SpinLock::new(KVDBChildTrList::new());
        let db_mgr = self.clone();

        let inner = InnerRootTransaction {
            source,
            tid,
            cid,
            status,
            writable: is_writable,
            protocol: AtomicU8::new(RootTransactionProtocol::Unselected as u8),
            persistence: AtomicBool::new(false), //默认键值对数据库的根事务不持久化
            prepare_timeout,
            commit_timeout,
            childs_map,
            childs,
            db_mgr,
            version_context: SpinLock::new(None),
        };

        let transaction = KVDBTransaction::RootTr(RootTransaction(Arc::new(inner)));
        // 创建指标的线性化点必须位于完整对象构造之后、Some 返回之前。状态拒绝的 None 路径
        // 不计数；关闭由最后一个 InnerRootTransaction owner 的 trace-only Drop 配平。
        #[cfg(feature = "trace")]
        self.0
            .transaction_metrics
            .created
            .fetch_add(1, Ordering::Relaxed);
        Some(transaction)
    }

    /// 请求当前进程的 glibc allocator 归还可释放页。
    ///
    /// 仅 Linux 提供，直接调用 `malloc_trim(0)`；`true` 表示 allocator 报告释放了内存，
    /// `false` 不表示没有可回收对象。该操作作用于整个进程而非单个数据库，可能同步扫描
    /// allocator 并阻塞调用线程，非纯函数、非确定性，也不保证降低 RSS。
    ///
    /// # Safety
    ///
    /// FFI 参数 `0` 是 glibc 允许的 pad 值，调用不传入 Rust 指针。进程所用 allocator 必须
    /// 与链接到的 libc 实现兼容；本 API 不应在时延敏感热路径频繁调用。
    #[cfg(target_os = "linux")]
    pub fn cleanup_buffer_after_collect_table(&self) -> bool {
        match unsafe { malloc_trim(0) } {
            0 => false,
            _ => true,
        }
    }

    /// 设置共享 manager 的软关闭状态并立即禁止创建新事务。
    ///
    /// 若两阶段管理器当前没有已注册事务，状态直接设为 Closed；否则设为 Closing。所有 clone
    /// 立即观察同一关闭结果，重复调用不会重新开放数据库。调用无返回值，不等待活跃事务、
    /// 子表异步持久化、根 WAL 确认、collector/listener 退出或文件句柄释放，也不取消 close 前
    /// 已创建但尚未 prepare 的事务句柄。close 前已经进入 Prepared 的普通/版本事务仍可提交；
    /// close 前已经进入可恢复失败状态的普通事务仍可 rollback；这些终结路径会正常注销事务，
    /// 需要根 WAL 的提交也仍按原确认链推进。最后一个 active transaction 完成后当前没有自动
    /// 把 Closing 推进为 Closed；再次调用本方法才会重写状态。
    ///
    /// 这是 `Q-CLOSE-001` / `FIND-CLOSE-001` 记录的当前实现，不是最终或最佳 shutdown API。
    /// 操作为 O(1)，读取事务 registry 长度并原子写状态，不执行文件 I/O、不 await、不调用
    /// 用户回调。真实边界由 `tests/manager_contract.rs` 验证。
    pub fn close(&self) {
        if self.0.tr_mgr.transaction_len() == 0 {
            //如果当前事务管理器没有任何正在执行的事务，则设置数据库状态为已关闭
            self.0.status.store(DB_CLOSED_STATUS, Ordering::SeqCst);
        } else {
            //如果当前事务管理器还有任何正在执行的事务，则设置数据库状态为正在状态
            self.0.status.store(DB_CLOSEING_STATUS, Ordering::SeqCst);
        }
    }

    /// 无锁读取根事务对象 created/closed 累计值；只供 tracing loop 差量采集。
    #[cfg(feature = "trace")]
    fn transaction_metrics_snapshot(&self) -> TransactionLifecycleMetricsSnapshot {
        TransactionLifecycleMetricsSnapshot {
            created: self.0.transaction_metrics.created.load(Ordering::Relaxed),
            closed: self.0.transaction_metrics.closed.load(Ordering::Relaxed),
        }
    }
}

/*
* 键值对数据库管理器异步方法
*/
impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>
> KVDBManager<C, Log> {
    /// 读取一个 Key 的当前逻辑值及同一 publication 窗口中的公开版本。
    ///
    /// 该入口不创建事务、不获取 prepare 锁，也不写 WAL 或数据文件。版本命中时只读取记录；
    /// 缺席时会分配独立 Guid 并登记首次观察和 TTL，因此它不是纯函数。LogWrite 的逻辑值固定
    /// 为 `None`；Btree 的真实点读错误返回可恢复 `Common(Normal)`，绝不伪装成 Key 不存在。
    ///
    /// 表名长度须为 1..=4096 字节，Key 长度须为 1..=u16::MAX。返回版本必须与值成对缓存，
    /// 并且只能进入 `prepare_with_version -> commit_with_version` 独立协议；禁止与普通事务 API
    /// 混用。完整契约见 `docs/KEY_VERSION_PUBLICATION_DESIGN.md#key-version-public-api` 和
    /// `docs/PI_DB_SERVER_KEY_VERSION_API_HANDOFF.md#pi-db-server-query-with-version`。
    pub async fn query_with_version(&self,
                                    table: Atom,
                                    key: Binary)
        -> Result<(Option<Binary>, Version), KVTableTrError> {
        // 只在 trace 构建中持有两个原子的借用；所有 `?`、取消和 unwind 都由 guard Drop 归入
        // failure，不改变原错误返回、publication 临界区或默认构建热路径。
        #[cfg(feature = "trace")]
        let metric_guard = self
            .0
            .key_versions
            .begin_api_call(KeyVersionApiOperation::Query);
        validate_version_table_key(&table, &key, "query with version")?;
        let registered = {
            self.0
                .tables
                .read()
                .await
                .get(&table)
                .cloned()
        }.ok_or_else(|| {
            KVTableTrError::new_transaction_error(
                ErrorLevel::Normal,
                format!("Query with version failed, table: {:?}, reason: table not found",
                        table.as_str()))
        })?;

        let _publication = registered.versions.publication().read().await;
        let value = match &registered.table {
            KVDBTable::MetaTab(table) => table.query_committed(&key),
            KVDBTable::MemOrdTab(table) => table.query_committed(&key),
            KVDBTable::LogOrdTab(table) => table.query_committed(&key),
            KVDBTable::LogWTab(_) => None,
            KVDBTable::BtreeOrdTab(table) => table
                .query_committed(&key)
                .map_err(|e| KVTableTrError::new_transaction_error(ErrorLevel::Normal, e))?,
        };
        let version = registered
            .versions
            .first_observation(key,
                               value.is_some(),
                               || self.0.tr_mgr.alloc_transaction_uid());

        #[cfg(feature = "trace")]
        metric_guard.finish(true);
        Ok((value, version))
    }

    /// 克隆指定名称的内部表句柄。
    ///
    /// 这是 crate 内模块间接口；`table_name` 只在 await 期间借用。存在时返回共享表 clone，
    /// 不存在或已经从 registry 移除时返回 `None`。返回句柄可继续延长表生命周期，因此 registry
    /// 删除不等于立即释放文件、collector 或内存。平均 O(1)，短暂获取异步 registry 读锁，
    /// 不执行表 I/O。
    pub(crate) async fn get_table(&self, table_name: &Atom) -> Option<KVDBTable<C, Log>> {
        if let Some(table) = self.0.tables.read().await.get(table_name) {
            Some(table.table.clone())
        } else {
            None
        }
    }

    /// 判断表名当前是否注册。
    ///
    /// `table_name` 不会被保存；内部 `.tables_meta` 也计为存在。结果是调用时 registry 状态，
    /// 不是事务快照，返回后可立即因 DDL 改变。查询不执行表名长度/路径校验，任何未注册名称
    /// 都返回 `false`；也不检查 manager 状态，Closing/Closed 后仍可读取 registry。平均 O(1)，
    /// 获取一次异步读锁，无文件 I/O。
    pub async fn is_exist(&self, table_name: &Atom) -> bool {
        self.0.tables.read().await.contains_key(table_name)
    }

    /// 返回当前 registry 条目数。
    ///
    /// 数量包含内部 `.tables_meta`，不只包含用户表；是瞬时值而非跨调用稳定快照。平均 O(1)，
    /// 获取一次异步读锁，无分配和文件 I/O。调用不检查 manager 状态，Closing/Closed 后仍
    /// 返回当前 registry 条目数。
    pub async fn table_size(&self) -> usize {
        self.0.tables.read().await.len()
    }

    /// 克隆当前 registry 中全部表名。
    ///
    /// 返回列表包含内部 `.tables_meta`，顺序来自 hash map，未排序且不稳定。每个 [`Atom`] clone
    /// 保持名称有效，但不保持表仍注册；调用方需要稳定顺序时必须自行排序。O(n) 时间和 O(n)
    /// 新空间，迭代期间持异步 registry 读锁，不执行文件 I/O。一次调用得到的是同一读临界区
    /// 内的完整名称集合；它不检查 manager 状态，Closing/Closed 后仍可调用。
    pub async fn tables(&self) -> Vec<Atom> {
        let mut table_names = Vec::new();
        for key in self.0.tables.read().await.keys() {
            table_names.push(key.clone());
        }

        table_names
    }

    /// 返回指定表实现报告的数据位置词法副本。
    ///
    /// 缺表返回 `None`。Meta、LogOrdered 和 LogWrite 返回各自日志目录；Btree 返回 redb 数据
    /// 文件 `tables_path/<table>/table.dat`，不是其父目录；Memory 无论 `persistence` 为何都返回
    /// `None`，因为它没有存储引擎数据文件。`None` 因此不能单独区分“缺表”和“已存在
    /// Memory 表”，应与 [`Self::is_exist`] 联合判断。调用方不得把不同表 variant 的返回路径
    /// 统一当作目录执行操作。
    ///
    /// 返回路径不保证 canonical、当前存在或可访问。平均 O(1) 查找加 O(p) 路径复制，短暂持
    /// registry 读锁，不访问文件系统。查询不校验名称；未注册名称返回 `None`。它不检查
    /// manager 状态，Closing/Closed 后仍可读取当前 registry。
    pub async fn table_path(&self, table_name: &Atom) -> Option<PathBuf> {
        match self.0.tables.read().await.get(table_name).map(|registered| &registered.table) {
            None => None,
            Some(KVDBTable::MetaTab(table)) => {
                if let Some(path) = table.path() {
                    Some(path.to_path_buf())
                } else {
                    None
                }
            },
            Some(KVDBTable::MemOrdTab(table)) => {
                if let Some(path) = table.path() {
                    Some(path.to_path_buf())
                } else {
                    None
                }
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                if let Some(path) = table.path() {
                    Some(path.to_path_buf())
                } else {
                    None
                }
            },
            Some(KVDBTable::LogWTab(table)) => {
                if let Some(path) = table.path() {
                    Some(path.to_path_buf())
                } else {
                    None
                }
            },
            Some(KVDBTable::BtreeOrdTab(table)) => {
                if let Some(path) = table.path() {
                    Some(path.to_path_buf())
                } else {
                    None
                }
            },
        }
    }

    /// 查询表的持久化标志。
    ///
    /// 缺表返回 `None`；存在时返回 `Some`。Meta、LogOrdered、LogWrite、Btree 当前总为
    /// `Some(true)`；Memory 返回建表元信息中的标志。Memory 的 `true` 仅表示动作进入根 WAL，
    /// 不会创建数据文件。结果是表实例属性，不表示当前事务已有待持久化动作或最终确认完成。
    /// 查询不校验名称或 manager 状态；未注册名称返回 `None`，Closing/Closed 后仍可读取。
    /// 平均 O(1)，短暂持 registry 读锁，无分配和文件 I/O。
    pub async fn is_persistent_table(&self, table_name: &Atom) -> Option<bool> {
        match self.0.tables.read().await.get(table_name).map(|registered| &registered.table) {
            None => None,
            Some(KVDBTable::MetaTab(table)) => {
                Some(table.is_persistent())
            },
            Some(KVDBTable::MemOrdTab(table)) => {
                Some(table.is_persistent())
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                Some(table.is_persistent())
            },
            Some(KVDBTable::LogWTab(table)) => {
                Some(table.is_persistent())
            },
            Some(KVDBTable::BtreeOrdTab(table)) => {
                Some(table.is_persistent())
            },
        }
    }

    /// 查询表是否报告为有序表。
    ///
    /// 缺表返回 `None`。当前五种内部/用户表都返回 `Some(true)`，包括只写语义的 LogWrite；
    /// 该布尔值不能推导某表支持 query/delete/stream 的完整能力。平均 O(1)，短暂持 registry
    /// 读锁，无分配和文件 I/O。查询不校验名称或 manager 状态；未注册名称返回 `None`，
    /// Closing/Closed 后仍可读取。
    pub async fn is_ordered_table(&self, table_name: &Atom) -> Option<bool> {
        match self.0.tables.read().await.get(table_name).map(|registered| &registered.table) {
            None => None,
            Some(KVDBTable::MetaTab(table)) => {
                Some(table.is_ordered())
            },
            Some(KVDBTable::MemOrdTab(table)) => {
                Some(table.is_ordered())
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                Some(table.is_ordered())
            },
            Some(KVDBTable::LogWTab(table)) => {
                Some(table.is_ordered())
            },
            Some(KVDBTable::BtreeOrdTab(table)) => {
                Some(table.is_ordered())
            },
        }
    }

    /// 获取表实现当前报告的记录数。
    ///
    /// 缺表返回 `None`。Meta/Memory/LogOrdered/LogWrite 从当前内存根读取；Btree 同步打开 redb
    /// read transaction，再把只写 cache 粗略叠加到持久基线。Btree 当前会把 begin_read、
    /// open_table 或 `table.len()` 错误折叠为 `Some(0)`，且 tombstone/overlay 计数语义仍有
    /// `FIND-TABLE-002` 风险；因此该值不是可用于诊断存储健康的无损 Result。
    ///
    /// 调用持 registry 异步读锁并进入表内同步锁；Btree 还可能在 runtime worker 上执行同步
    /// 文件支持读取，复杂度约 O(c log d)，c 为 cache key 数、d 为 redb 记录数。其它表通常
    /// 为 O(1)。结果是调用时观察值，不与外部事务建立原子快照。查询不校验名称或 manager
    /// 状态；未注册名称返回 `None`，Closing/Closed 后仍可读取。
    pub async fn table_record_size(&self, table_name: &Atom) -> Option<usize> {
        match self.0.tables.read().await.get(table_name).map(|registered| &registered.table) {
            None => None,
            Some(KVDBTable::MetaTab(table)) => {
                Some(table.len())
            },
            Some(KVDBTable::MemOrdTab(table)) => {
                Some(table.len())
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                Some(table.len())
            },
            Some(KVDBTable::LogWTab(table)) => {
                Some(table.len())
            },
            Some(KVDBTable::BtreeOrdTab(table)) => {
                Some(table.len())
            },
        }
    }

    /// 获取表实现当前报告的内存缓存 payload 字节数。
    ///
    /// 缺表返回 `None`；存在空表通常返回 `Some(0)`。该值由表内 COW root/cache 的
    /// `full_bytes_size` 提供，不包含 Arc、树节点、allocator、文件缓存、redb 页面缓存、WAL
    /// 文件或后台任务开销，不能当作进程 RSS。Btree 只统计只写 cache，不统计 redb 数据。
    ///
    /// 调用短暂持 registry 异步读锁和表内同步锁，通常 O(1)、无文件 I/O；并发写入后返回值
    /// 只代表本次读取时刻，不是事务快照。查询不校验名称或 manager 状态；未注册名称返回
    /// `None`，Closing/Closed 后仍可读取。
    pub async fn table_cache_size(&self, table_name: &Atom) -> Option<u64> {
        match self.0.tables.read().await.get(table_name).map(|registered| &registered.table) {
            None => None,
            Some(KVDBTable::MetaTab(table)) => {
                Some(table.size())
            },
            Some(KVDBTable::MemOrdTab(table)) => {
                Some(table.size())
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                Some(table.size())
            },
            Some(KVDBTable::LogWTab(table)) => {
                Some(table.size())
            },
            Some(KVDBTable::BtreeOrdTab(table)) => {
                Some(table.size())
            },
        }
    }

    /// 在一次现有 table registry 读临界区内取得旧表缓存指标和版本缓存原子快照。
    ///
    /// 该 trace-only helper 不迭代或锁住版本 DashMap；返回后 registry guard 已释放。两个版本
    /// 原子可能来自相邻并发时刻，只用于最终收敛观测，不能作为事务或回收门禁。
    #[cfg(feature = "trace")]
    async fn table_tracing_metrics(&self,
                                   table_name: &Atom)
        -> Option<(u64, KeyVersionCacheMetricsSnapshot)> {
        let tables = self.0.tables.read().await;
        let registered = tables.get(table_name)?;
        let table_cache_size = match &registered.table {
            KVDBTable::MetaTab(table) => table.size(),
            KVDBTable::MemOrdTab(table) => table.size(),
            KVDBTable::LogOrdTab(table) => table.size(),
            KVDBTable::LogWTab(table) => table.size(),
            KVDBTable::BtreeOrdTab(table) => table.size(),
        };
        Some((table_cache_size, registered.versions.metrics_snapshot()))
    }

    /// 强制根 commit logger 轮换到一个新的 checkpoint。
    ///
    /// 成功返回 `pi_store::LogFile::split` 刚创建的可写 WAL 文件索引。依赖中的
    /// `current_check_point()` 读取的是内部 `log_id` 的下一个待分配值，因此本方法成功后该值
    /// 等于“返回索引 + 1”，不能把两者误认为同一个编号。每次调用都会创建/切换可写 WAL
    /// 文件并把前一 checkpoint 加入只读确认队列，即使当前没有事务；因此不是纯函数，也不是
    /// 幂等操作。它不会追加一条业务事务日志，`append_total_count` 不应因此增加。
    ///
    /// 该异步操作获取 logger checkpoint 锁并执行真实文件 I/O；并发 append/confirm/轮换按
    /// `pi_store::CommitLogger` 锁顺序串行。错误原样作为 `io::Error` 返回，已发生的部分文件
    /// 副作用不由本 API rollback。真实副作用由 `tests/manager_contract.rs` 验证。
    pub async fn append_new_commit_log(&self) -> IOResult<usize> {
        let commit_logger = self.0.tr_mgr.commit_logger();
        commit_logger.append_check_point().await
    }

    /// 调用表实现的整理准备阶段。
    ///
    /// `table_name` 不会被保存。缺表当前静默返回 `Ok(())`；Memory/Btree 的准备阶段也是 no-op，
    /// Meta/LogOrdered/LogWrite 会 split 各自日志文件。成功只表示表级准备返回成功，不自动调用
    /// [`Self::collect_table`]，也不建立只能由同一调用者消费的一次性 token。
    ///
    /// 当前实现从取得 registry 读 guard 到表 future 完成期间一直持有该 guard，DDL registry
    /// 写入可能被长 I/O 阻塞，见 `FIND-ASYNC-001`。底层表错误被格式化并统一包装为
    /// `io::ErrorKind::Other`，错误等级信息不会结构化保留。非幂等表可能轮换文件。非空四表当前
    /// 成功路径由 `tests/manager_table_maintenance.rs` 验证；完整范围见
    /// `docs/MANAGER_TABLE_MAINTENANCE_ACCEPTANCE.md#manager-table-maintenance-index`。
    pub async fn ready_collect_table(&self, table_name: &Atom) -> IOResult<()> {
        match self.0.tables.read().await.get(table_name).map(|registered| &registered.table) {
            None => (),
            Some(KVDBTable::MetaTab(table)) => {
                if let Err(e) = table.ready_collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
            Some(KVDBTable::MemOrdTab(table)) => {
                if let Err(e) = table.ready_collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                if let Err(e) = table.ready_collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
            Some(KVDBTable::LogWTab(table)) => {
                if let Err(e) = table.ready_collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
            Some(KVDBTable::BtreeOrdTab(table)) => {
                if let Err(e) = table.ready_collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
        }

        Ok(())
    }

    /// 直接调用表实现的整理/压缩阶段。
    ///
    /// 缺表当前静默返回 `Ok(())`；Memory 为 no-op，日志表执行真实日志 collect，Btree 执行
    /// redb 持久化/compact 路径。API 不检查调用方是否先调用 [`Self::ready_collect_table`]，
    /// 不返回整理后的大小或记录数，也不保证与并发事务形成全库级原子边界。
    ///
    /// 调用可能长时间执行文件 I/O、同步 redb 锁和 runtime timeout；整个 await 期间当前仍持
    /// registry 读 guard，见 `FIND-ASYNC-001`。表错误统一降为 `io::ErrorKind::Other`。Btree
    /// compact 最多总计尝试三次，任意成功立即结束，只有前两次失败各同步等待一秒；第三次
    /// 失败由表层生成可恢复的 Normal 错误，再由本方法包装。FIND-TABLE-001 的修复边界和真实
    /// 失败证据见 `docs/BTREE_COLLECT_RETRY_BUG.md#bug-btree-collect-retry-001-index`。
    /// 该方法有文件副作用且不保证幂等。整理前后逻辑数据、表统计、根 WAL 和 data-only 冷启动
    /// 门禁由 `tests/manager_table_maintenance.rs` 验证。
    pub async fn collect_table(&self, table_name: &Atom) -> IOResult<()> {
        match self.0.tables.read().await.get(table_name).map(|registered| &registered.table) {
            None => (),
            Some(KVDBTable::MetaTab(table)) => {
                if let Err(e) = table.collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
            Some(KVDBTable::MemOrdTab(table)) => {
                if let Err(e) = table.collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                if let Err(e) = table.collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
            Some(KVDBTable::LogWTab(table)) => {
                if let Err(e) = table.collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
            Some(KVDBTable::BtreeOrdTab(table)) => {
                if let Err(e) = table.collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
        }

        Ok(())
    }

    /// 向可选 listener 通道发送一个事务信息报告请求事件。
    ///
    /// 安装 listener 时，本方法把 [`KVDBEvent::ReportTrInfo`] 写入无界通道并返回；`Ok(())`
    /// 只证明 send 接受了事件，不证明 listener 已被调度、已经清空该批事件或成功产出报告。
    /// listener 回调可通过参数中的 [`Transaction2PcManager`] 自行读取当时统计。未安装 listener
    /// 时返回 `io::ErrorKind::ConnectionAborted`。
    ///
    /// 发送为异步安全且允许多线程并发，但通道无背压，持续调用可能增长内存。该 API 不持表
    /// registry 锁、不执行文件 I/O、不等待用户回调，也不保证事件相对 collector 通知的全局
    /// 顺序。基础投递由 `tests/manager_contract.rs` 验证，3072 边界和真实 collector payload
    /// 由 `tests/manager_listener_contract.rs` 验证。
    pub async fn report_transaction_info(&self) -> IOResult<()> {
        if let Some(notifier) = self.0.notifier.as_ref() {
            if let Err(e) = notifier.send(KVDBEvent::ReportTrInfo).await {
                Err(Error::new(ErrorKind::ConnectionAborted,
                               format!("Report transaction info failed, reason: {:?}", e)))
            } else {
                Ok(())
            }
        } else {
            Err(Error::new(ErrorKind::ConnectionAborted,
                           format!("Report transaction info failed, reason: invalid notifier")))
        }
    }

    // 尝试幂等地重播未确认的提交日志，并修复数据库表数据。
    // start_replay 的 callback 必须在本条 WAL 完成 append_replay/commit_repair 后才能返回，存储层
    // 随后才推进对应 checkpoint；因此当前实现把异步 repair 投递到数据库 runtime，再在 callback
    // 所在线程同步等待。只有 callback 占满该 runtime 的全部 worker 时才会自阻塞：外部线程驱动的
    // 单 worker 配置可正常恢复，同 runtime 内仍有空闲 worker 也可推进。当前生产 pi-launcher 属于
    // 前一种安全装配；特定同 runtime 全占用窗口只归档并由 startup_repair_liveness 红线测试约束，
    // 本轮不得为消除此限制而改变 replay/checkpoint/confirm 的执行顺序。
    pub(crate) async fn try_repair(&self, enable_accelerated_repair: bool) -> IOResult<(usize, usize)> {
        //构建重播回调
        let db_mgr = self.clone();
        // pi_store 当前会把 replay callback 的 io::Error 统一包装成 Other。该原子只标记
        // “删表持久化输入非法”这一冻结分支，使 try_repair 能在不修改存储依赖和其它恢复
        // 错误语义的前提下恢复 InvalidData 分类。
        let invalid_remove_data = Arc::new(AtomicBool::new(false));
        let invalid_remove_data_copy = invalid_remove_data.clone();

        let tables = Arc::new(Mutex::new(BTreeMap::new()));
        let tables_copy = tables.clone();
        let replay_callback = move |commit_uid: Guid, prepare_output: Vec<u8>| -> IOResult<()> {
            // callback 由 pi_store 在 start_replay 的轮询线程同步调用。repair 动作本身必须回到
            // 数据库 runtime 执行；同步等待保证 callback 返回前，本条 WAL 已按原 TID/CID 完成
            // prepare_repair/commit_repair，存储层不会提前推进 checkpoint。
            let db_mgr_copy = db_mgr.clone();
            let commit_uid_copy = commit_uid.clone();
            let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
            let (sender, receiver) = bounded(1);

            let tables_clone = tables_copy.clone();
            let invalid_remove_data = invalid_remove_data_copy.clone();
            let boxed = async move {
                let bytes_len = prepare_output.len(); //获取日志缓冲区长度
                let mut offset = 0; //日志缓冲区偏移
                let bytes = prepare_output.as_slice();
                let uid = u128::from_le_bytes(bytes[0..16].try_into().unwrap()); //获取事务唯一id
                let transaciton_uid = Guid(uid);
                offset += 16; //移动缓冲区指针

                if let Some(tr) = db_mgr_copy.transaction(Atom::from(REPAIR_DB_SOURCE),
                                                          true,
                                                          5000,
                                                          5000)
                {
                    //创建数据库事务成功，则迭代日志缓冲区中，本次未确认的提交日志中执行写操作的表和相关键值对
                    //迭代完成后，则可以恢复本次未确认的提交日志对表的内存键值对的修改，并生成对应的键值对操作记录
                    while offset < bytes_len {
                        //获取表名、操作的键值对数量和新的日志缓冲区偏移
                        let (table, kvs_len, new_offset) =
                            <MetaTable<C, Log> as KVTable>::get_init_table_prepare_output(&prepare_output, offset);

                        //获取操作的表键值列表和新的日志缓冲区偏移
                        let (writes, new_offset)
                            = <MetaTable<C, Log> as KVTable>::get_all_key_value_from_table_prepare_output(&prepare_output, &table, kvs_len, new_offset);

                        if table == meta_table_name {
                            //未确认的提交日志操作的表是元信息表，则创建或删除表
                            for write in writes {
                                if let Some(value) = write.value {
                                    //有值，则创建表
                                    let table_name = match binary_to_table(&write.key) {
                                        Err(e) => {
                                            //反序列化表名失败
                                            panic!("From binary to table name failed, reason: {:?}", e);
                                        },
                                        Ok(table_name) => {
                                            //反序列化表名成功
                                            table_name
                                        }
                                    };
                                    let table_meta = KVTableMeta::from(value);

                                    if let Err(e) = tr.repair_create_table(
                                        table_name.clone(),
                                        table_meta.clone(),
                                        enable_accelerated_repair,
                                    ).await {
                                        //重播的创建表失败，则立即返回错误原因
                                        let _ = sender.send(Err(Error::new(ErrorKind::Other, format!("Repair tables meta failed, transaction_uid: {:?}, commit_uid: {:?}, table_name: {:?}, table_meta: {:?}, reason: {:?}", transaciton_uid, commit_uid_copy, table_name, table_meta, e))));
                                        return;
                                    }
                                } else {
                                    //无值，则删除表
                                    // Meta tombstone 的 Key 与建表记录相同，都是 table_to_binary
                                    // 生成的 BON Atom，不能把编码字节直接当 UTF-8 表名。该分支
                                    // 只服务删表 WAL 恢复；解码失败表示持久化日志损坏。
                                    // CONTRACT-DDL-REMOVE-001 / BUG-DDL-REMOVE-001：
                                    // docs/SEMANTIC_CONTRACTS.md#contract-ddl-remove-crash-durability。
                                    let table_name = match binary_to_table(&write.key) {
                                        Err(e) => {
                                            invalid_remove_data.store(true, Ordering::Release);
                                            let _ = sender.send(Err(Error::new(
                                                ErrorKind::InvalidData,
                                                format!("Repair removed table failed, transaction_uid: {:?}, commit_uid: {:?}, table_key_bytes: {}, reason: decode table name failed: {:?}",
                                                        transaciton_uid,
                                                        commit_uid_copy,
                                                        write.key.len(),
                                                        e),
                                            )));
                                            return;
                                        },
                                        Ok(table_name) => table_name,
                                    };

                                    if let Err(e) = tr.repair_remove_table(table_name.clone()).await {
                                        // 重播删表失败时保留底层 ErrorKind：保留 Meta 名称或非法
                                        // 持久化名称必须作为 InvalidData 传播，不能被改写为 Other。
                                        if e.kind() == ErrorKind::InvalidData {
                                            invalid_remove_data.store(true, Ordering::Release);
                                        }
                                        let _ = sender.send(Err(Error::new(
                                            e.kind(),
                                            format!("Repair tables meta failed, transaction_uid: {:?}, commit_uid: {:?}, table_name: {:?}, reason: {:?}",
                                                    transaciton_uid,
                                                    commit_uid_copy,
                                                    table_name,
                                                    e),
                                        )));
                                        return;
                                    }
                                }
                            }
                        } else {
                            //未确认的提交日志操作的表是其它表，则执行本次未确认的提交日志中指定表的键值对写操作
                            tables_clone
                                .lock()
                                .await
                                .insert(table.clone(), ());
                            for write in writes {
                                if write.exist_value() {
                                    //有值，则执行插入或更新操作
                                    if let Err(e) = tr.upsert(vec![write]).await {
                                        //重播的插入或更新操作失败，则立即返回错误原因
                                        let _ = sender.send(Err(Error::new(ErrorKind::Other, format!("Repair db failed, transaction_uid: {:?}, commit_uid: {:?}, reason: {:?}", transaciton_uid, commit_uid_copy, e))));
                                        return;
                                    }
                                } else {
                                    //无值，则执行删除操作
                                    if let Err(e) = tr.delete(vec![write]).await {
                                        //重播的删除操作失败，则立即返回错误原因
                                        let _ = sender.send(Err(Error::new(ErrorKind::Other, format!("Repair db failed, transaction_uid: {:?}, commit_uid: {:?}, reason: {:?}", transaciton_uid, commit_uid_copy, e))));
                                        return;
                                    }
                                }
                            }
                        }

                        //更新日志缓冲区偏移
                        offset = new_offset;
                    }

                    //指定本次重播事务的事务唯一id后执行预提交修复
                    if let Err(e) = tr.prepare_repair(transaciton_uid.clone()).await {
                        //预提交重播事务失败，则立即返回错误原因
                        let _ = sender.send(Err(Error::new(ErrorKind::Other,
                                                           format!("Repair db failed, transaction_uid: {:?}, commit_uid: {:?}, reason: {:?}",
                                                                   transaciton_uid,
                                                                   commit_uid_copy,
                                                                   e))));
                        return;
                    }

                    //指定本次重播事务的事务唯一id和事务提交唯一id后执行提交修复
                    if let Err(e) = tr
                        .commit_repair(transaciton_uid.clone(),
                                       commit_uid_copy.clone(),
                                       prepare_output).await {
                        //提交重播事务失败，则立即返回错误原因
                        let _ = sender.send(Err(Error::new(ErrorKind::Other,
                                                           format!("Repair db failed, transaction_uid: {:?}, commit_uid: {:?}, reason: {:?}",
                                                                   transaciton_uid,
                                                                   commit_uid_copy,
                                                                   e))));
                        return;
                    }

                    //返回成功重播一条未确认的提交日志
                    let _ = sender.send(Ok(()));
                } else {
                    //创建数据库事务失败，则立即返回错误原因
                    let _ = sender.send(Err(Error::new(ErrorKind::Other, format!("Repair db failed, transaction_uid: {:?}, commit_uid: {:?}, reason: get db transaction error", transaciton_uid, commit_uid_copy))));
                }
            }.boxed();
            let _ = db_mgr.0.rt.spawn(boxed);

            // 这里等待的是刚投递到 db runtime 的 repair task。若 callback 本身占用同一 runtime
            // 的最后一个 worker，任务无法首次 poll，形成确定性活性等待；外部线程驱动 startup 时
            // callback 不占数据库 worker，因此单 worker 也能推进。spawn 被拒绝会丢弃 boxed 及其
            // sender，使 recv 返回 channel 错误，并非这个永久等待窗口。完整边界见
            // FIND-REPAIR-001；保持同步等待是当前 checkpoint 顺序的一部分。
            match receiver.recv() {
                Err(e) => {
                    //同步通道异常，则立即返回错误原因
                    Err(Error::new(ErrorKind::Other, format!("Repair db failed, commit_uid: {:?}, reason: {:?}", commit_uid, e)))
                },
                Ok(result) => {
                    //同步阻塞的等待异步重播完成，则立即返回重播结果
                    result
                },
            }
        };

        //异步重播所有未确认的提交日志
        let replay_result = match self.0.tr_mgr.replay_commit_log(replay_callback).await {
            Err(e) if invalid_remove_data.load(Ordering::Acquire) => {
                // CommitLoggerLoader 当前丢失 callback kind，但原错误仍完整保存在其错误文本/源中。
                // 只对上面明确标记的删表持久化输入恢复 InvalidData，禁止按字符串猜测分类。
                return Err(Error::new(ErrorKind::InvalidData, e));
            },
            Err(e) => return Err(e),
            Ok(result) => result,
        };

        //所有未确认的提交日志已完成重播，则立即返回数据库修复成功
        let _ = self.0.tr_mgr.finish_replay().await?; //通知事务管理器，已完成重播

        // for table in tables.lock().await.keys() {
        //     if let Some(KVDBTable::BtreeOrdTab(tab)) = self.get_table(table).await {
        //         //当前表存在且为有序B树表，则立即整理
        //         tab.collect().await;
        //     }
        // }

        return Ok(replay_result);
    }
}

// 内部键值对数据库管理器
struct InnerKVDBManager<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    rt:                 MultiTaskRuntime<()>,                           //异步运行时
    tr_mgr:             Transaction2PcManager<C, Log>,                  //事务管理器
    db_path:            PathBuf,                                        //数据库的表文件所在目录的路径
    tables_meta_path:   PathBuf,                                        //数据库的元信息表文件所在目录的路径
    tables_path:        PathBuf,                                        //数据库表文件所在目录的路径
    tables:             Arc<RwLock<XHashMap<Atom, RegisteredTable<C, Log>>>>, //数据表及其精确版本状态
    key_versions:       KeyVersionRegistry,                             //数据库唯一的全局 Key 版本注册表
    status:             AtomicU64,                                      //数据库状态
    listener:           Option<Receiver<KVDBEvent<Guid>>>,              //数据库事件监听器
    notifier:           Option<Sender<KVDBEvent<Guid>>>,                //数据库事件通知器
    #[cfg(feature = "trace")]
    transaction_metrics: TransactionLifecycleMetrics,                   //根事务对象创建与最终析构计数
}

#[derive(Clone)]
struct RegisteredTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    table: KVDBTable<C, Log>,
    versions: KeyVersions,
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> RegisteredTable<C, Log> {
    fn new(table: KVDBTable<C, Log>, versions: KeyVersions) -> Self {
        Self { table, versions }
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Deref for RegisteredTable<C, Log> {
    type Target = KVDBTable<C, Log>;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

/// 在持有数据库表注册表写锁时安装表及其唯一版本状态。
///
/// 全局版本注册表先指向新实例，再替换表条目；旧事务仍可安全持有旧实例，但后续按身份删除
/// 旧实例时不会误删同名新表的版本状态。调用方必须持续持有 `tables` 的写锁，禁止把该顺序
/// 拆分到多个临界区。
fn install_registered_table<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(tables: &mut XHashMap<Atom, RegisteredTable<C, Log>>,
  key_versions: &KeyVersionRegistry,
  name: Atom,
  table: KVDBTable<C, Log>) {
    let versions = key_versions.create_table_versions();
    key_versions.install(name.clone(), versions.clone());
    if let Some(previous) = tables.insert(name.clone(), RegisteredTable::new(table, versions)) {
        key_versions.remove_exact(&name, &previous.versions);
    }
}

/// 根事务与五种表子事务共用的事务树节点枚举。
///
/// [`KVDBManager::transaction`] 只返回 [`Self::RootTr`]。应用层应始终通过该根 variant 调用
/// DDL、KV、stream、lock 和生命周期 API；其它 variant 由根事务在首次触表时惰性创建，供
/// `pi_async_transaction` 遍历事务树。许多公开 wrapper 在子表 variant 上会 panic，这些
/// variant 公开可构造并不代表它们属于合法应用调用域。
///
/// clone 为共享句柄 clone，不复制事务快照或状态；从任一 clone 创建的子事务、分配的 TID/CID
/// 和状态转换会被其它 clone 观察到。根节点在 trait 视图中是 `Safe` QoS 的事务树，不是 unit
/// 或 sequence；表子节点是由根拥有的 unit。根和内置子节点都选择串行
/// prepare/commit/rollback，顺序由 [`KVDBChildTrList`] 固定。
///
/// 类型可在线程间移动/共享以支持 runtime 调度，但同一逻辑事务上的并发动作、
/// prepare/commit/rollback 越序调用不提供可串行化保证；当前状态防线不足见 `FIND-TR-002`。
/// 真实根生命周期见 `tests/root_transaction_lifecycle.rs`，构造、clone、子节点继承和单根登记
/// 契约见 `tests/root_transaction_construction.rs`。
#[derive(Clone)]
pub enum KVDBTransaction<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    /// 应用层唯一合法的事务入口；持有数据库管理器、子事务树和根版本上下文。
    RootTr(RootTransaction<C, Log>),
    /// 根事务按需创建的 Meta 子事务节点。
    MetaTabTr(MetaTabTr<C, Log>),
    /// 根事务按需创建的 Memory 子事务节点。
    MemOrdTabTr(MemOrdTabTr<C, Log>),
    /// 根事务按需创建的 LogOrdered 子事务节点。
    LogOrdTabTr(LogOrdTabTr<C, Log>),
    /// 根事务按需创建的 LogWrite 子事务节点；当前不允许外部业务使用该表。
    LogWTabTr(LogWTabTr<C, Log>),
    /// 根事务按需创建的 Btree 子事务节点。
    BtreeOrdTabTr(BtreeOrdTabTr<C, Log>),
}

// SAFETY: 每个 variant 都是 Arc/同步原语保护的根或表事务共享句柄；移动枚举只移动该 owner，
// 不搬移自引用数据。各表事务实现负责其 COW root、cache、状态和存储句柄的跨线程同步，Log
// 又受 AsyncCommitLog: Send + Sync 约束。该保证只覆盖内存/线程安全，不扩大合法状态机域。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for KVDBTransaction<C, Log> {}
// SAFETY: 共享调用最终委托给 RootTransaction 或各表事务的同步字段；枚举自身没有额外内部
// 可变性、裸指针或 thread-owner 状态。并发逻辑动作仍须遵守事务状态和冲突协议。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for KVDBTransaction<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> AsyncTransaction for KVDBTransaction<C, Log> {
    type Output = ();
    type Error = KVTableTrError;

    fn is_writable(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_writable()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_writable()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_writable()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_writable()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.is_writable()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.is_writable()
            }
        }
    }

    // 键值对数据库的提交，会把所有子事务的预提交输出合成为一个提交输入，用于写入提交日志，所以也不需要并发
    fn is_concurrent_commit(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_concurrent_commit()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_concurrent_commit()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_concurrent_commit()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_concurrent_commit()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.is_concurrent_commit()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.is_concurrent_commit()
            },
        }
    }

    // 键值对数据库的预提交基本都是内存操作，所以回滚也不需要并发
    fn is_concurrent_rollback(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_concurrent_rollback()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_concurrent_rollback()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_concurrent_rollback()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_concurrent_rollback()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.is_concurrent_rollback()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.is_concurrent_rollback()
            },
        }
    }

    fn get_source(&self) -> Atom {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_source()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.get_source()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_source()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_source()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.get_source()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.get_source()
            },
        }
    }

    fn init(&self)
            -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.init()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.init()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.init()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.init()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.init()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.init()
            },
        }
    }

    fn rollback(&self)
                -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.rollback()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.rollback()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.rollback()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.rollback()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.rollback()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.rollback()
            },
        }
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Transaction2Pc for KVDBTransaction<C, Log> {
    type Tid = Guid;
    type Pid = Guid;
    type Cid = Guid;
    type PrepareOutput = Vec<u8>;
    type PrepareError = KVTableTrError;
    type ConfirmOutput = ();
    type ConfirmError = KVTableTrError;
    type CommitConfirm = KVDBCommitConfirm<C, Log>;

    fn is_require_persistence(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_require_persistence()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_require_persistence()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_require_persistence()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_require_persistence()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.is_require_persistence()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.is_require_persistence()
            },
        }
    }

    fn require_persistence(&self) {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.require_persistence();
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.require_persistence();
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.require_persistence();
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.require_persistence();
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.require_persistence();
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.require_persistence();
            },
        }
    }

    fn is_concurrent_prepare(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_concurrent_prepare()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_concurrent_prepare()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_concurrent_prepare()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_concurrent_prepare()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.is_concurrent_prepare()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.is_concurrent_prepare()
            },
        }
    }

    fn is_enable_inherit_uid(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_enable_inherit_uid()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_enable_inherit_uid()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_enable_inherit_uid()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_enable_inherit_uid()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.is_enable_inherit_uid()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.is_enable_inherit_uid()
            },
        }
    }

    fn get_transaction_uid(&self) -> Option<<Self as Transaction2Pc>::Tid> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_transaction_uid()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.get_transaction_uid()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_transaction_uid()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_transaction_uid()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.get_transaction_uid()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.get_transaction_uid()
            },
        }
    }

    fn set_transaction_uid(&self, uid: <Self as Transaction2Pc>::Tid) {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.set_transaction_uid(uid);
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.set_transaction_uid(uid);
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.set_transaction_uid(uid);
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.set_transaction_uid(uid);
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.set_transaction_uid(uid);
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.set_transaction_uid(uid);
            },
        }
    }

    fn get_prepare_uid(&self) -> Option<<Self as Transaction2Pc>::Pid> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_prepare_uid()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.get_prepare_uid()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_prepare_uid()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_prepare_uid()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.get_prepare_uid()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.get_prepare_uid()
            },
        }
    }

    fn set_prepare_uid(&self, uid: <Self as Transaction2Pc>::Pid) {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.set_prepare_uid(uid);
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.set_prepare_uid(uid);
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.set_prepare_uid(uid);
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.set_prepare_uid(uid);
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.set_prepare_uid(uid);
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.set_prepare_uid(uid);
            },
        }
    }

    fn get_commit_uid(&self) -> Option<<Self as Transaction2Pc>::Cid> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_commit_uid()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.get_commit_uid()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_commit_uid()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_commit_uid()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.get_commit_uid()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.get_commit_uid()
            },
        }
    }

    fn set_commit_uid(&self, uid: <Self as Transaction2Pc>::Cid) {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.set_commit_uid(uid);
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.set_commit_uid(uid);
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.set_commit_uid(uid);
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.set_commit_uid(uid);
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.set_commit_uid(uid);
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.set_commit_uid(uid);
            },
        }
    }

    fn get_prepare_timeout(&self) -> u64 {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_prepare_timeout()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.get_prepare_timeout()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_prepare_timeout()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_prepare_timeout()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.get_prepare_timeout()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.get_prepare_timeout()
            },
        }
    }

    fn get_commit_timeout(&self) -> u64 {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_commit_timeout()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.get_commit_timeout()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_commit_timeout()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_commit_timeout()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.get_commit_timeout()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.get_commit_timeout()
            },
        }
    }

    fn prepare(&self)
               -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        #[cfg(feature = "default")]
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.prepare()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.prepare()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.prepare()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.prepare()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.prepare()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.prepare()
            },
        }
    }

    fn prepare_conflicts(&self)
        -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        #[cfg(feature = "default")]
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.prepare_conflicts()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.prepare_conflicts()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.prepare_conflicts()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.prepare_conflicts()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.prepare_conflicts()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.prepare_conflicts()
            },
        }
    }

    fn commit(&self, confirm: <Self as Transaction2Pc>::CommitConfirm)
              -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        #[cfg(feature = "default")]
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.commit(confirm)
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.commit(confirm)
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.commit(confirm)
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.commit(confirm)
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.commit(confirm)
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.commit(confirm)
            },
        }
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Transaction2PcAllConflicts for KVDBTransaction<C, Log> {
    fn precheck_all_conflicts(&self)
        -> BoxFuture<'_, Result<(), <Self as Transaction2Pc>::PrepareError>> {
        match self {
            KVDBTransaction::RootTr(tr) => tr.precheck_all_conflicts(),
            KVDBTransaction::MetaTabTr(tr) => tr.precheck_all_conflicts(),
            KVDBTransaction::MemOrdTabTr(tr) => tr.precheck_all_conflicts(),
            KVDBTransaction::LogOrdTabTr(tr) => tr.precheck_all_conflicts(),
            KVDBTransaction::LogWTabTr(tr) => tr.precheck_all_conflicts(),
            KVDBTransaction::BtreeOrdTabTr(tr) => tr.precheck_all_conflicts(),
        }
    }

    fn prepare_all_conflicts(&self)
        -> BoxFuture<'_, Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        match self {
            KVDBTransaction::RootTr(tr) => tr.prepare_all_conflicts(),
            KVDBTransaction::MetaTabTr(tr) => tr.prepare_all_conflicts(),
            KVDBTransaction::MemOrdTabTr(tr) => tr.prepare_all_conflicts(),
            KVDBTransaction::LogOrdTabTr(tr) => tr.prepare_all_conflicts(),
            KVDBTransaction::LogWTabTr(tr) => tr.prepare_all_conflicts(),
            KVDBTransaction::BtreeOrdTabTr(tr) => tr.prepare_all_conflicts(),
        }
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> UnitTransaction for KVDBTransaction<C, Log> {
    type Status = Transaction2PcStatus;
    type Qos = TableTrQos;

    fn is_unit(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_unit()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_unit()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_unit()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_unit()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.is_unit()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.is_unit()
            },
        }
    }

    fn get_status(&self) -> <Self as UnitTransaction>::Status {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_status()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.get_status()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_status()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_status()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.get_status()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.get_status()
            },
        }
    }

    fn set_status(&self, status: <Self as UnitTransaction>::Status) {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.set_status(status);
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.set_status(status);
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.set_status(status);
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.set_status(status);
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.set_status(status);
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.set_status(status);
            },
        }
    }

    fn qos(&self) -> <Self as UnitTransaction>::Qos {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.qos()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.qos()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.qos()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.qos()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.qos()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.qos()
            },
        }
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> SequenceTransaction for KVDBTransaction<C, Log> {
    type Item = Self;

    // 键值对数据表事务，一定不是顺序事务
    fn is_sequence(&self) -> bool {
        false
    }

    fn prev_item(&self) -> Option<<Self as SequenceTransaction>::Item> {
        None
    }

    fn next_item(&self) -> Option<<Self as SequenceTransaction>::Item> {
        None
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> TransactionTree for KVDBTransaction<C, Log> {
    type Node = KVDBTransaction<C, Log>; //键值对数据库的根事务的子事务，必须是键值对数据库事务
    type NodeInterator = KVDBChildTrList<C, Log>;

    fn is_tree(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_tree()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_tree()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_tree()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_tree()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.is_tree()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.is_tree()
            },
        }
    }

    fn children_len(&self) -> usize {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.children_len()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.children_len()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.children_len()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.children_len()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.children_len()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.children_len()
            },
        }
    }

    fn to_children(&self) -> Self::NodeInterator {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.to_children()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.to_children()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.to_children()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.to_children()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.to_children()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.to_children()
            },
        }
    }
}

/*
* 键值对数据库事务异步方法
*/
impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVDBTransaction<C, Log> {
    /// 在根事务中观察指定表的元信息，但不选择普通或版本业务协议。
    ///
    /// `name` 按所有权传入；存在定义时返回 owned [`KVTableMeta`]，不存在或内部 Meta 表未注册
    /// 时返回 `None`。当前入口不单独校验空名或 4096 字节上限，调用方仍应遵守表名契约。
    /// 只能对 [`KVDBTransaction::RootTr`] 调用；对子表 variant 调用会 panic。
    ///
    /// 若同一根的专用 DDL 已建立 Meta 子事务，本方法读取该事务的私有 COW 根，因此既可观察
    /// 本根刚创建的表，也可观察普通协议 `remove_table` 尚未提交的删除；否则直接点读当前已
    /// 提交 Meta 根。这不开放调用方直接通过普通/版本 KV API 操作内部 Meta 表。两条路径都不
    /// 登记 Ordinary Read、不加入版本 `read_set`、不创建新的 2PC 子节点、不分配 TID、不修改
    /// 根持久化标志，也不返回 Key 版本。它可位于普通动作或 [`Self::prepare_with_version`] 前；
    /// 结果本身不保证到后续 prepare 期间保持不变。
    ///
    /// 方法会短暂取得根 `childs_map` 同步锁，或等待数据库表注册表异步读锁，再对一个 COW 根
    /// 做 O(log m) 点读；同步 guard 不跨 `.await`，不执行文件 I/O、用户回调或全表扫描。完整
    /// 协议边界见 `docs/SCHEMA_PROTOCOL_NEUTRAL_DESIGN.md#schema-protocol-neutral-table-meta`。
    pub async fn table_meta(&self, name: Atom) -> Option<KVTableMeta> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.table_meta(name).await
            },
            _ => panic!("Get table meta failed, reason: invalid root transaction"),
        }
    }

    /// 以显式存储选项在尚未选择业务协议的根事务中创建或幂等确认一张表。
    ///
    /// `name` 的 UTF-8 长度必须为 `1..=4096` 字节并满足现有表名契约；`meta` 决定表类型、
    /// persistence 和 Key/Value 类型，`options` 只在实际构造 LogOrdered/Btree 时分别接受对应
    /// variant。已存在且元信息相同的表按既有语义直接成功，不重新解释 `options`。
    /// `enable_accelerated_repair` 只传给实际新建的 Btree 存储构造路径，其它表不使用。
    ///
    /// 本方法只允许 RootTr，子表 variant 会 panic。合法调用必须发生在任何非空普通/dirty
    /// KV、lock/unlock、remove、普通 prepare 或版本 prepare 之前；根已经选择 Ordinary 或
    /// Versioned 时，在新增建表副作用前返回 `io::ErrorKind::InvalidInput`。多次建表复用唯一
    /// `SchemaCreate` Meta 子事务，随后既可走普通 2PC，也可走
    /// [`Self::prepare_with_version`] / [`Self::commit_with_version`]。内部 Meta 写参与同一 TID、
    /// 冲突、WAL、发布和最终确认，但永远不进入版本提交的公开业务回执。
    ///
    /// 当前 DDL 仍不具完整事务/取消原子性：表对象和全局注册项在 prepare 前即可见，rollback
    /// 不移除它们，构造 future 中途取消也可能留下部分资源；只有 Meta 定义和后续业务动作受
    /// 2PC/WAL 约束。调用方必须使用可写根并等待明确结果，失败后不得把注册表可见性当作提交
    /// 成功。`remove_table` 不属于该前导阶段，禁止在同一根与 create 混用。
    ///
    /// 方法等待全局表注册表异步写锁，当前仍可能在存储构造期间长期持有它；根
    /// `childs_map -> childs` 只用于 O(1) schema owner 安装且绝不跨 `.await`。Memory 构造为
    /// O(1)，持久化表还包含目录/文件初始化成本。本轮不增加全局协议锁、unsafe 或后台任务。
    /// 完整状态机、WAL 顺序和非目标见
    /// `docs/SCHEMA_PROTOCOL_NEUTRAL_DESIGN.md#schema-protocol-neutral-create`。
    pub async fn create_table_with_options(&self,
                                           name: Atom,
                                           meta: KVTableMeta,
                                           options: CreateTableOptions,
                                           enable_accelerated_repair: bool) -> IOResult<()> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.create_table_with_options(
                    name,
                    meta,
                    options,
                    enable_accelerated_repair
                ).await
            },
            _ => panic!("Create table failed, reason: invalid root transaction"),
        }
    }

    /// 使用当前默认存储选项创建或幂等确认一张表。
    ///
    /// LogOrdered 默认使用 `512MiB/2MiB/2MiB`，Btree 默认使用 `16MiB` cache 并启用 compact，
    /// 其它表使用空选项。协议选择、SchemaCreate、错误、DDL 非原子性、锁和提交语义与
    /// [`Self::create_table_with_options`] 完全相同。
    pub async fn create_table(&self,
                              name: Atom,
                              meta: KVTableMeta,
                              enable_accelerated_repair: bool) -> IOResult<()> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.create_table(
                    name,
                    meta,
                    enable_accelerated_repair
                ).await
            },
            _ => panic!("Create table failed, reason: invalid root transaction"),
        }
    }

    /// 异步批量创建表，只允许在初始化加载表时使用。
    ///
    /// 当前唯一生产调用点保证输入非空、表名来自 Meta 唯一 Key，且用户表尚未注册。该内部
    /// 前置条件不是通用批量 DDL 契约；空输入或重复已注册项的既有健壮性缺陷归档于
    /// `docs/REVIEW_FINDINGS.md#find-start-002`，本轮启动边界修复不改变其行为。
    pub(crate) async fn create_multiple_tables(&self,
                                               table_metas: Vec<(Atom, KVTableMeta, Option<CreateTableOptions>)>,
                                               is_checksum: bool,
                                               enable_accelerated_repair: bool)
        -> IOResult<()>
    {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.create_multiple_tables(
                    table_metas,
                    is_checksum,
                    enable_accelerated_repair
                ).await
            },
            _ => panic!("Create multiple table failed, reason: invalid root transaction"),
        }
    }

    /// 异步修复创建表，需要指定表名和表的元信息
    pub(crate) async fn repair_create_table(&self,
                                            name: Atom,
                                            meta: KVTableMeta,
                                            enable_accelerated_repair: bool) -> IOResult<()> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.repair_create_table(name, meta, enable_accelerated_repair).await
            },
            _ => panic!("Create table failed, reason: invalid root transaction"),
        }
    }

    /// 从当前数据库根事务中移除指定表，并登记持久化的 Meta tombstone。
    ///
    /// `name` 的所有权移入本调用；其 UTF-8 编码长度必须位于
    /// `1..=`[`crate::MAX_TABLE_NAME_BYTES`]。空名或超长名称会在根持久化标记、表注册表和
    /// Meta 子事务发生任何变化前返回 [`std::io::ErrorKind::InvalidInput`]。内部 Meta 表名
    /// `.tables_meta` 同样会被无副作用拒绝，防止删除数据库自身的表目录。当前校验仍不拒绝
    /// 路径分隔符、绝对路径、`.` 或 `..`，调用方须遵守
    /// [CONTRACT-TABLE-NAME-001](../docs/SEMANTIC_CONTRACTS.md#contract-table-name-001) 中记录的
    /// 未决名称/路径边界。
    ///
    /// # 可观察顺序与返回值
    ///
    /// 对合法名称，当前实现按以下顺序执行：
    ///
    /// 1. 将根事务标记为需要持久化；
    /// 2. 获取数据库表注册表的异步写锁，并立即移除名称对应的注册项；
    /// 3. 在同一根事务的 Meta 子事务中写入该名称的删除动作；
    /// 4. 返回 `Ok(())`，由调用方随后执行 [`Self::prepare_modified`] 和
    ///    [`Self::commit_modified`]。
    ///
    /// `Ok(())` 只表示删表动作已经登记，不表示事务已经提交、根 WAL 已落地、Meta 数据文件
    /// 已持久化或表资源已释放。`prepare_modified` 会生成包含 Meta tombstone 的根 WAL 输入；
    /// `commit_modified` 成功才表示该 WAL 已 append/flush，之后 Meta 数据文件异步持久化，
    /// 最终成功信号再确认该 WAL。提交成功后立即崩溃时，启动恢复会从原 WAL 重放删除。
    ///
    /// 表不存在不是错误：调用仍返回 `Ok(())` 并登记 tombstone，因此不能根据返回值判断表
    /// 原先是否存在。重复调用在最终注册状态上是幂等的，但会重复产生事务动作/WAL 副作用，
    /// 不是物理副作用意义上的幂等操作。Meta 动作失败返回 `io::ErrorKind::Other`，此时注册表
    /// 可能已经改变。
    ///
    /// # 事务、取消与生命周期边界
    ///
    /// 本方法只支持 [`KVDBTransaction::RootTr`]；对任一表子事务 variant 调用会 panic。调用方
    /// 必须使用可写根事务；当前实现不在本入口拒绝只读事务，只读误用可能已经改变注册表却在
    /// prepare 时跳过持久化。当前 DDL 不具完整事务原子性：rollback 不会恢复已移除的注册项，
    /// 合法名称通过校验后取消 future 也可能留下根持久化标记或部分副作用。因此本方法不是
    /// rollback-safe 或 cancellation-safe，调用方不能把 `remove_table` 返回前后的中间状态当作
    /// 原子切换。
    ///
    /// 移除注册项不会删除表目录/数据文件，不会停止后台 collector，也不会强制释放已有
    /// `Arc`、表事务、redb/logfile 句柄或迭代流。已有流仍只在其创建事务存活期间按快照契约
    /// 使用；实际资源回收边界见 `FIND-LIFE-001`。这些限制属于当前实现事实，不是最终或最佳
    /// DDL 设计。
    ///
    /// # 并发、性能与安全
    ///
    /// 方法会等待 `async_lock::RwLock` 表注册表写锁，并在持有该 guard 时取得根事务的同步
    /// `childs_map` 锁以及 Meta 事务内部同步锁；当前动作阶段不执行文件 I/O、用户回调、FFI
    /// 或 V8 操作，但竞争会暂停同一注册表上的其它管理操作。并发 DDL 由这些锁和后续 Meta
    /// prepare 冲突检查约束，不提供跨事务串行化或完整原子性保证。本实现没有新增 `unsafe`，
    /// 不自行产生裸指针或跨运行时 owner；其 `Send/Sync` 边界继承根事务、表和 runtime 契约。
    ///
    /// 表注册表删除平均为 O(1)，Meta COW 删除为 O(log m)，名称编码为 O(name_len)；动作阶段
    /// 额外空间为 O(name_len + log m)。真正根 WAL append/flush 的时间和空间由后续 prepare/
    /// commit 的完整事务 payload 决定。与修复前错误跳过 WAL 的行为相比，成功删表提交会增加
    /// 必需的 WAL I/O；修复本身在动作热路径只增加一次 O(1) relaxed 原子存储。
    ///
    /// # 使用顺序
    ///
    /// 以下片段假定 `transaction` 是由 manager 创建的可写根事务：
    ///
    /// ```ignore
    /// transaction.remove_table(Atom::from("users")).await?;
    /// let prepare_output = transaction.prepare_modified().await?;
    /// transaction.commit_modified(prepare_output).await?;
    /// ```
    ///
    /// 正式语义和 BUG 证据见
    /// [CONTRACT-DDL-REMOVE-001](../docs/SEMANTIC_CONTRACTS.md#contract-ddl-remove-crash-durability)
    /// 与 [修复归档](../docs/DDL_REMOVE_DURABILITY_FIX.md#ddl-remove-fix-index)；真实四进程
    /// crash/replay/data-only 验证入口为 `tests/ddl_remove_crash_durability.rs`，名称边界入口为
    /// `tests/table_name_contract.rs`。
    pub async fn remove_table(&self, name: Atom) -> IOResult<()> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.remove_table(name).await
            },
            _ => panic!("Remove table failed, reason: invalid root transaction"),
        }
    }

    /// 在根 WAL 重放期间登记一个删表动作。
    ///
    /// `name` 必须来自已成功解码的 Meta tombstone；空名或超长持久化名称按损坏数据返回
    /// [`std::io::ErrorKind::InvalidData`]。有效输入复用正常删表流程，因此同样只允许根事务，
    /// 同样不删除物理表文件，也继承当前 DDL 非完整原子性。该入口只由 `try_repair` 内部使用，
    /// 不会重新 append 原 WAL；replay commit 负责发布恢复结果和最终确认原事务。
    pub(crate) async fn repair_remove_table(&self, name: Atom) -> IOResult<()> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.repair_remove_table(name).await
            },
            _ => panic!("Remove table failed, reason: invalid root transaction"),
        }
    }

    /// 按输入顺序查询当前根事务中的多个 Table/Key，并采用各表当前的 dirty 读记录策略。
    ///
    /// 返回 Vec 与输入严格等长、同序；缺表和 LogWrite 均占一个 `None` 槽位，输入中的 `value`
    /// 完全不参与查询。空输入在协议选择前直接返回空 Vec。非空输入会选择根的 Ordinary 协议，
    /// 已选择 Versioned 时当前因本签名没有错误通道而 panic；对子表事务 variant 调用也 panic。
    ///
    /// 本方法不代表统一的“脏读”：Meta/Memory/LogOrdered 不登记普通 Read，Btree 当前委托普通
    /// query 并记录 Read，LogWrite 固定返回 `None`。外部协议要求一个事务要么只使用全部
    /// `dirty_*` 点操作，要么只使用普通 `query/upsert/delete`，非空动作不得混用；当前库不以
    /// 独立 guard 强制该约定，违规后的事务安全性没有保证。只读根可以合法查询。
    ///
    /// 根事务不会在创建时固定全库快照。每张表在首次触达时才惰性创建自己的子事务，因此同一
    /// 根首次访问不同表时可以观察不同提交边界。Memory/LogOrdered 后续读取固定的表级 COW
    /// 私有根；Btree 只固定创建时 overlay，overlay 缺席时每次重新读取 redb。Btree 首次读建立
    /// 的冲突基线不会随后续返回值刷新，完整区别见 `ROOT-QUERY-001`。
    ///
    /// 调用按项串行查 registry 并惰性创建/复用非持久化子事务；Btree overlay 缺席时可能同步
    /// 读取 redb 并短暂阻塞 worker。显式只读根可在查询后直接释放；可写根即使只有读动作，
    /// 仍必须完成普通 prepare/commit，以消费 Btree 可能建立的读预留并闭合 manager 生命周期，
    /// 不能用空 prepare 输出推断可跳过 commit。方法不写用户值、根 WAL 或数据文件，但可能
    /// 改变子事务的读/冲突记录。当前无错误返回通道，逐表差异和合法测试入口见
    /// `CONTRACT-ACTION-001`、`Q-DIRTY-001`、`ROOT-QUERY-001` 与
    /// `tests/kv_action_contract.rs`。
    pub async fn dirty_query(&self,
                             table_kv_list: Vec<TableKV>) -> Vec<Option<Binary>> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.dirty_query(table_kv_list).await
            },
            _ => panic!("Query by dirty db failed, reason: invalid root transaction"),
        }
    }

    /// 按输入顺序查询当前根事务中的多个 Table/Key，并登记各表的普通读冲突状态。
    ///
    /// 返回 Vec 与输入严格等长、同序；缺表和 LogWrite 返回 `None`，`TableKV::value` 被忽略。
    /// 空输入在协议选择前直接返回空 Vec；非空输入选择 Ordinary 协议，禁止与版本事务或同一
    /// 根中的任意非空 `dirty_*` 点操作混用。已选择 Versioned 或对子表 variant 调用时当前会
    /// panic。只读根可使用本方法，但只读事务不得随后执行写操作。
    ///
    /// Meta/Memory/LogOrdered/Btree 会登记普通 Read，并可能影响 prepare 冲突；LogWrite 固定
    /// 返回 `None`。根不提供数据库级统一快照：各表在首次触达时分别固定起点；Btree 仅固定
    /// overlay，overlay 缺席时每次 query 都建立独立 redb read transaction，但首次 Key 基线
    /// 继续用于 prepare。调用按项串行取得 registry 读锁和短期子事务索引锁，惰性创建非持久化
    /// 表子事务；Btree 可能同步读取 redb 并阻塞 worker。显式只读根可查询后直接释放；可写
    /// 纯读根必须继续普通 2PC，即使 prepare 输出为空。方法不写根 WAL/数据文件，当前签名也
    /// 无法结构化返回底层读取错误。合法矩阵见 `tests/kv_action_contract.rs` 和
    /// `tests/root_query_contract.rs`，完整边界见 `ROOT-QUERY-001`。
    pub async fn query(&self,
                       table_kv_list: Vec<TableKV>) -> Vec<Option<Binary>> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.query(table_kv_list).await
            },
            _ => panic!("Query db failed, reason: invalid root transaction"),
        }
    }

    /// 按输入顺序在事务私有视图中执行 dirty upsert。
    ///
    /// 空输入在协议选择前返回 `Ok(())`。非空输入选择 Ordinary 根协议，但外部必须让该事务
    /// 只使用 `dirty_*` 点操作；与普通点操作混用不保证事务安全。对子表 variant 调用会 panic，
    /// 已选择 Versioned 返回 Normal 协议错误。当前入口不拒绝只读根；只读事务写入会在 prepare
    /// 快路中被丢弃，属于禁止用法而非受支持语义。
    ///
    /// `Some(value)` 才写入；`None` 不是 delete，不改变用户值，但对已存在表仍会创建/复用子事务
    /// 并可能提升根持久化标志。缺表被静默跳过。批次逐项执行，后项失败不会撤销此前的事务私有
    /// 动作；成功只表示动作已登记，根 WAL、发布、数据文件持久化和确认仍由后续普通 2PC 完成。
    /// 当前未统一拒绝长度为 0 的 Value，调用方必须遵守禁止持久化空值的契约。
    ///
    /// 每项平均包含一次 registry 查找和表内 O(log n) COW/overlay 更新；LogWrite 只登记动作，
    /// Btree dirty 当前复用普通 upsert。同步 guard 不用于文件 I/O，本方法自身不写磁盘。
    /// 根级完整契约、逐表冲突差异和闭环证据见
    /// [ROOT-UPSERT-001](../docs/ROOT_UPSERT_CONTRACT.md#root-upsert-contract-index)。
    pub async fn dirty_upsert(&self,
                              table_kv_list: Vec<TableKV>) -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.dirty_upsert(table_kv_list).await
            },
            _ => panic!("Upsert db failed, reason: invalid root transaction"),
        }
    }

    /// 按输入顺序在事务私有视图中执行普通 upsert。
    ///
    /// 空输入在协议选择前返回 `Ok(())`。非空输入选择 Ordinary 根协议，只能与普通
    /// `query/upsert/delete` 点操作联合使用；禁止与版本事务或非空 `dirty_*` 点操作混用。
    /// 对子表 variant 调用会 panic，已选择 Versioned 返回 Normal 协议错误。当前入口不拒绝
    /// 只读根；只读写入随后会被 prepare 快路静默丢弃，因此调用方必须使用可写根。
    ///
    /// `Some(value)` 才执行写入；`None` 不是 delete，但仍可能创建子事务并提升持久化标志。
    /// 缺表返回 Fatal；由于批次按项执行，缺表前已登记的私有动作不会由本方法撤销，而 Fatal
    /// 又禁止 rollback，所以合法调用必须预先保证全部表存在。表内错误按其原等级返回。成功只
    /// 表示私有动作已登记，后续仍须完整 prepare/commit。长度为 0 的 Value 当前未在入口统一
    /// 拒绝，但不属于可持久化合法域。
    /// 缺表 Fatal 发生在 manager start/TID/CID 和根 WAL 之前；这只说明当前分支没有发布共享
    /// 状态，绝不把 Fatal 降级为可 rollback。调用方必须立即丢弃整个根，不能继续 query/delete/
    /// prepare/commit。真实边界与测试前提校准见
    /// [CONTRACT-TR-004](../docs/SEMANTIC_CONTRACTS.md#contract-tr-004)、
    /// [Fatal/取消边界](../docs/ORDINARY_FATAL_CANCELLATION_BOUNDARY.md#ordinary-fatal-cancellation-boundary-index)
    /// 和 `tests/kv_action_contract.rs`。
    ///
    /// 每项平均包含 registry 查找和 O(log n) COW/overlay 更新；同步 guard 不跨文件 I/O，动作
    /// 阶段不写根 WAL 或数据文件。真实返回、缺表和逐表差异见 `tests/kv_action_contract.rs`；
    /// 根级完整契约和闭环证据见
    /// [ROOT-UPSERT-001](../docs/ROOT_UPSERT_CONTRACT.md#root-upsert-contract-index)。
    pub async fn upsert(&self,
                        table_kv_list: Vec<TableKV>) -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.upsert(table_kv_list).await
            },
            _ => panic!("Upsert db failed, reason: invalid root transaction"),
        }
    }

    /// 在根事务内按输入顺序脏删除多个表和 key，并返回各表实现当前能够取得的旧值。
    ///
    /// Btree 缓存值直接返回；已有 tombstone/重复删除返回 `None`；缓存完全缺席时同步读取
    /// 调用时 redb 快照。redb 读取错误会记录详细 error 日志并降级为 `None`，删除仍写
    /// tombstone 并可提交，因此 `None` 不能无条件解释为持久化存储中原本没有 key。
    /// redb 成功读取的值或逻辑不存在会进入独立 KeyState 冲突基线，但不会写入事务创建时的
    /// `cache_ref`；读取失败只能保留 `OverlayMissing`。prepare 先用 revision 捕获同值写/ABA，
    /// 再按 allocation 身份快路和 bytes 回退比较逻辑状态。每个缓存缺席 Key 的独立读取可能
    /// 短暂阻塞 worker。完整边界见 `CONTRACT-BTREE-DELETE-001`、
    /// `CONTRACT-BTREE-PREPARE-BASELINE-001`、`tests/btree_delete_old_value.rs` 和
    /// `tests/btree_redb_prepare_baseline.rs`。
    ///
    /// 返回 Vec 与输入同序等长；缺表返回一个 `None`，`TableKV::value` 被忽略。空输入不选择
    /// 协议；非空输入选择 Ordinary，但外部必须让该根只使用 `dirty_*` 点操作。当前入口不拒绝
    /// 只读根，且批次后项错误不撤销此前私有删除。只能对根事务调用，否则 panic。根级路由、
    /// 逐表 dirty 差异、WAL/确认/repair 和测试边界见
    /// [ROOT-DELETE-001](../docs/ROOT_DELETE_CONTRACT.md#root-delete-contract-index)。
    pub async fn dirty_delete(&self,
                        table_kv_list: Vec<TableKV>)
                        -> Result<Vec<Option<Binary>>, KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.dirty_delete(table_kv_list).await
            },
            _ => panic!("Delete db failed, reason: invalid root transaction"),
        }
    }

    /// 在根事务内按输入顺序删除多个表和 key，并返回各表实现当前能够取得的旧值。
    ///
    /// Btree 缓存值直接返回；已有 tombstone/重复删除返回 `None`；缓存完全缺席时同步读取
    /// 调用时 redb 快照。redb 读取错误会记录详细 error 日志并降级为 `None`，删除仍写
    /// tombstone 并可提交，因此 `None` 不能无条件解释为持久化存储中原本没有 key。
    /// redb 成功读取的值或逻辑不存在会进入独立 KeyState 冲突基线，但不会写入事务创建时的
    /// `cache_ref`；读取失败只能保留 `OverlayMissing`。prepare 先用 revision 捕获同值写/ABA，
    /// 再按 allocation 身份快路和 bytes 回退比较逻辑状态。每个缓存缺席 Key 的独立读取可能
    /// 短暂阻塞 worker。完整边界见 `CONTRACT-BTREE-DELETE-001`、
    /// `CONTRACT-BTREE-PREPARE-BASELINE-001`、`tests/btree_delete_old_value.rs` 和
    /// `tests/btree_redb_prepare_baseline.rs`。
    ///
    /// 返回 Vec 与输入同序等长；缺表返回一个 `None`，`TableKV::value` 被忽略。空输入不选择
    /// 协议；非空输入选择 Ordinary，只能与普通 `query/upsert/delete` 点操作联合使用。当前
    /// 入口不拒绝只读根，且批次后项错误不撤销此前私有删除。只能对根事务调用，否则 panic。
    /// 根级路由、最终动作、WAL/确认/repair 和测试边界见
    /// [ROOT-DELETE-001](../docs/ROOT_DELETE_CONTRACT.md#root-delete-contract-index)。
    pub async fn delete(&self,
                        table_kv_list: Vec<TableKV>)
                        -> Result<Vec<Option<Binary>>, KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.delete(table_kv_list).await
            },
            _ => panic!("Delete db failed, reason: invalid root transaction"),
        }
    }

    /// 在根事务内创建指定表的关键字快照流。
    ///
    /// `table_name` 必须是当前已注册表；不存在时返回 `None`。`key` 是包含边界，`None`
    /// 表示从升序首项或降序末项开始；`descending=false/true` 分别选择升序/降序。只能对
    /// [`KVDBTransaction::RootTr`] 调用，在子表事务枚举上调用会 panic。
    ///
    /// 若根事务已经因普通动作注册同表子事务，本方法复用它以保留 read-your-own-write；否则
    /// 只创建不加入根 `childs_map/childs` 的只读、非持久化快照事务。因此纯 iterator 不参与
    /// 2PC，也不会与随后由 `prepare_with_version` 安装的同表版本子事务共享 TID/prepare 项。
    /// 每次调用仍返回独立的创建时快照。调用返回后，本事务及其它事务可继续 `upsert/delete`，
    /// 旧流保持不变；流不提供可串行化、实时可见或 commit/rollback 绑定，也不会自动加入
    /// 版本协议 read-set。
    /// 创建本流的根事务必须存活到流耗尽或被 drop，事务释放后继续 poll 属于非法用法。
    ///
    /// 返回项拥有 key；流可在线程间移动但应由单消费者 poll。提前 drop 会释放快照。
    /// Memory/Meta/LogOrdered 创建为 O(1) COW 克隆加 O(log n) 定位；Btree 还会在本方法
    /// 返回前同步建立 redb 读事务，可能短暂阻塞。底层流无错误 item，Btree 读错误当前可能
    /// 表现为空流或提前结束。完整边界见 `CONTRACT-ITER-001` 和
    /// `tests/iterator_snapshot_safety.rs`。
    pub async fn keys<'a>(&self,
                          table_name: Atom,
                          key: Option<Binary>,
                          descending: bool)
                          -> Option<BoxStream<'a, Binary>> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.keys(table_name,
                        key,
                        descending).await
            },
            _ => panic!("Get db keys failed, table: {:?}, key: {:?}, descending: {:?}, reason: invalid root transaction", table_name.as_str(), key, descending),
        }
    }

    /// 在根事务内创建指定表的键值对快照流。
    ///
    /// 表查找、根事务前置条件、包含边界、方向、快照事务装配、创建事务生命周期、
    /// 并发修改、取消、错误和复杂度与 [`KVDBTransaction::keys`] 相同。每个 item 是创建时
    /// 快照中 owned `(key, value)`；随后 value 更新或删除不会改变既有流。
    pub async fn values<'a>(&self,
                            table_name: Atom,
                            key: Option<Binary>,
                            descending: bool) -> Option<BoxStream<'a, (Binary, Binary)>> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.values(table_name,
                          key,
                          descending).await
            },
            _ => panic!("Get db values failed, table: {:?}, key: {:?}, descending: {:?}, reason: invalid root transaction", table_name.as_str(), key, descending)
        }
    }

    /// 调用指定表当前的 Key 锁钩子。
    ///
    /// **当前五类内置表的钩子均为立即成功的 no-op**：不建立排他锁、不等待、不校验 owner 或
    /// 重入，也不提供内存可见性和事务隔离保证；不得用本 API 保护业务临界区。缺表同样返回
    /// `Ok(())`。Key 当前不会被解释，但合法公开调用仍须传入非空、匹配表 Key 类型、长度不超过
    /// `u16::MAX` 的规范 BON 编码；空或畸形 Key 的行为不是兼容契约。
    ///
    /// 本方法在表查找前原子选择 Ordinary，所以缺表成功也会阻止同一根随后选择 Versioned。
    /// 已选择 Versioned 时返回 Normal 协议错误；对子表 variant 调用会 panic。存在表时创建或
    /// 复用该表唯一的非持久化 managed 子事务：首次调用固定当时数据 COW/cache 根和版本
    /// revision 租约，后续同表普通写复用这条较早冲突基线并按表元信息提升 persistence。因此
    /// 调用不是无状态探测，长生命周期根也可能延后版本 TTL 回收。
    ///
    /// 纯 hook 不写值、根 WAL、表日志或数据文件；可写根按协议仍应完成一次空普通 2PC，空动作
    /// commit 不发布旧根或版本。直接释放未 prepare 根只释放快照/租约；只读根无需 commit。
    /// 首次已有表路径会分配 managed 子事务及表 hook boxed future，复用路径仍分配 boxed future；
    /// 缺表路径不创建 child。当前实现持有表 registry 读 guard 和 `childs_map` guard 等待表 hook，
    /// 但五类 hook 首次 poll 都立即完成；若未来实现真正异步等待，必须先重构 guard 边界。
    ///
    /// future 在首次 poll 前被丢弃没有副作用；首次 poll 选择 Ordinary 后可能在等待 registry
    /// 读锁时被取消，从而留下已选择的协议。当前表 hook 自身不会 pending。完整现状、测试和
    /// 基准见 `ROOT-KEY-HOOK-001`、`FIND-LOCK-001`、`tests/root_key_hook_contract.rs` 与
    /// `benches/root_key_hook.rs`。
    pub async fn lock_key(&self,
                          table_name: Atom,
                          key: Binary) -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.lock_key(table_name, key).await
            },
            _ => panic!("Lock table key failed, table: {:?}, key: {:?}, reason: invalid root transaction", table_name.as_str(), key),
        }
    }

    /// 调用指定表当前的 Key 解锁钩子。
    ///
    /// 当前五类实现与 [`Self::lock_key`] 一样都是 no-op：未锁、非 owner、重复解锁和缺表均返回
    /// `Ok(())`，不会释放任何真实同步原语。存在表时仍可能创建非持久化子事务。该方法选择
    /// Ordinary 协议，禁止与版本事务或 dirty-only 根混用；对子表 variant 调用会 panic。
    /// 不得把成功返回解释为临界区所有权已经释放或其它线程已可见。Key 前提、快照/revision
    /// 租约、持久化提升、空 2PC、分配、取消和 guard 边界与 [`Self::lock_key`] 完全相同。
    pub async fn unlock_key(&self,
                            table_name: Atom,
                            key: Binary) -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.unlock_key(table_name, key).await
            },
            _ => panic!("Unlock table key failed, table: {:?}, key: {:?}, reason: invalid root transaction", table_name.as_str(), key),
        }
    }

    /// 初始化并预提交整棵根事务树，返回随后 commit 所需的 opaque 字节。
    ///
    /// 只能对 [`KVDBTransaction::RootTr`] 调用；对子表 variant 调用会 panic。首次调用先向
    /// `Transaction2PcManager` 注册根事务并分配事务 UID，再按首次触表顺序 prepare 子事务。
    /// 成功返回的 `Vec<u8>` 归调用方所有：需要根 WAL 时包含根事务 UID 和持久化子表动作；
    /// 只读、无持久化动作及部分已完成 DDL 快路可返回空 Vec。
    ///
    /// 返回字节必须被视为一次性 opaque token，未经修改原样传给同一事务的
    /// [`Self::commit_modified`]。当前实现尚未校验 token 与事务身份/最近 prepare 输出绑定，
    /// 见 `FIND-TR-003`；篡改、跨事务交换、截断、附加或重复使用都不属于合法调用域。
    ///
    /// 非 Fatal prepare 错误使事务树失败但可调用 [`Self::rollback_modified`]；Fatal 永不可
    /// rollback。timeout 字段当前不执行实际截止。调用会获取多个同步锁并可能执行表级异步
    /// 操作；取消 future 不是已冻结的自动 rollback，调用方不得假设 drop future 会注销事务。
    pub async fn prepare_modified(&self) -> Result<Vec<u8>, KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.prepare_modified().await
            },
            _ => panic!("Prepare modified db failed, reason: invalid root transaction"),
        }
    }

    /// 以可定位冲突的路径初始化并预提交整棵根事务树。
    ///
    /// 调用顺序、RootTr 前置条件、输出所有权、timeout、取消和错误恢复边界与
    /// [`Self::prepare_modified`] 相同。差异是表实现检测到写冲突时返回
    /// [`KVTableTrError::Conflicts`]，其中保存首个冲突表名和 Key，错误等级为 Normal；多个
    /// 冲突不保证全部报告，也不保证跨表诊断顺序独立于首次触表顺序。
    ///
    /// 当前 manager 按首次触表顺序串行 prepare，并在首个错误处停止。因此多叶根失败时，冲突
    /// 之前的叶子可能已经 `Prepared`，冲突叶子为 `PrepareFailed`，尚未访问的叶子仍为
    /// `Inited`；这不是部分提交，三个状态都必须由整树 rollback 收口。首/中/末冲突位置和
    /// 失败节点状态由 `tests/ordinary_multi_table_conflict_rollback.rs` 逐项验证。
    ///
    /// 冲突发生在根 WAL append/flush 和子表根原子发布之前，因此在没有其它 Fatal 节点时
    /// 可以 rollback。真实 same-key 冲突和 manager 计数闭环由
    /// `tests/root_transaction_lifecycle.rs` 验证；普通 Memory/LogOrdered/Btree 三叶根的失败 WAL、
    /// 全树 rollback、同 Key 新根重试和 data-only 最终状态由
    /// `tests/ordinary_multi_table_conflict_rollback.rs` 验证；分层冲突率下 288 次多 Key 尝试的
    /// 成功/冲突守恒、跨 runtime 原子预留和恢复最终值由
    /// `tests/ordinary_multi_table_concurrency.rs` 验证。
    /// 不同外层根采用相反首次触表顺序时，跨表预留可能使全部根冲突，也可能恰有一个根完成
    /// prepare；不承诺固定 winner 或“至少一个成功”。但重叠 Key 绝不能出现两个 winner，全部
    /// loser 必须整树 rollback 后才能用新根重试。六排列、异步 barrier、WAL/repair 和双冷启动
    /// 门禁由 `tests/ordinary_multi_table_ordering.rs` 验证，方案见
    /// `docs/ORDINARY_MULTI_TABLE_ORDERING_ACCEPTANCE.md#ordinary-multi-table-ordering-index`。
    /// 普通持久化 Memory 的 1/2/4/8 writer 与确定性冲突率性能基线由
    /// `benches/ordinary_memory_concurrency.rs` 承载；该基准不外推其它表或版本协议。
    pub async fn prepare_modified_conflicts(&self) -> Result<Vec<u8>, KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.prepare_modified_conflicts().await
            },
            _ => panic!("Prepare conflicts db failed, reason: invalid root transaction"),
        }
    }

    /// 以外部实际读取的版本集合和最终写集合执行完整冲突预提交。
    ///
    /// 只能对尚未选择普通业务协议的可写 RootTr 调用，并且只能与
    /// [`Self::commit_with_version`] 组成独立协议；禁止与普通/dirty KV、lock/unlock、
    /// `remove_table`、普通 prepare/commit 混用。协议选择前调用 [`Self::table_meta`] 及零到多次
    /// [`Self::create_table`] / [`Self::create_table_with_options`] 是唯一 DDL 例外：其内部
    /// SchemaCreate Meta 子节点会原样保留并与版本业务子节点共同提交。纯 `keys/values` 流也
    /// 不选择协议、不会自动加入 `read_set`，因此允许先创建并保持到版本提交完成。`read_set` 和
    /// `write_set` 均可为空；各集合内部不允许重复 `(table, key)`，跨集合重叠合法且最终动作以
    /// write 为准。`Some(value)` 是 upsert，`None` 是 delete；LogWrite 不支持 delete。
    ///
    /// 所有表名、Key、Value 长度及重复项会在 UID、子事务和共享状态副作用前检查。版本缺失、
    /// TTL 淘汰、版本不等及只读表身份失效返回带 `ReadSetVersionMismatch` 的确定性
    /// [`KVTableTrError::AllConflicts`]；revision、值状态或 prepared 预留冲突返回
    /// `TransactionConflict`。冲突项不携带 expected/current Version，同 Key 两类并存时版本失配
    /// 优先，详见
    /// [VERSION-CONFLICT-KIND-001](../docs/VERSION_CONFLICT_KIND_DESIGN.md#version-conflict-kind-design-index)。
    /// 写表缺失/替换和参数错误返回可 rollback 的 Normal Common。成功 token 是一次性 opaque
    /// 数据，只能原样传给同一事务。
    pub async fn prepare_with_version(&self,
                                      read_set: Vec<TableKeyVersion>,
                                      write_set: Vec<TableKV>)
        -> Result<Vec<u8>, KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                #[cfg(feature = "trace")]
                let metric_guard = tr
                    .0
                    .db_mgr
                    .0
                    .key_versions
                    .begin_api_call(KeyVersionApiOperation::Prepare);
                let result = tr.prepare_with_version(read_set, write_set).await;
                #[cfg(feature = "trace")]
                metric_guard.finish(result.is_ok());
                result
            },
            _ => panic!("Prepare with version failed, reason: invalid root transaction"),
        }
    }

    /// 提交一次已经成功 prepare 的根事务。
    ///
    /// `prepare_output` 必须是同一事务最近一次成功 prepare 返回的完整 Vec，并且只能使用
    /// 一次。只能对 RootTr 调用；子表 variant 会 panic。未先 prepare、重复 commit、传入其它
    /// 事务或修改后的 token 都是非法调用；当前部分非法路径可能返回 Normal 错误，另一些路径
    /// 会在内部 UID `unwrap` 处 panic，不能依赖其防御表现。
    ///
    /// 对需要持久化且有有效输出的事务，本方法先 append 并 flush 根 WAL；只有 WAL 落地成功
    /// 后才发布各子表 COW 根并安排最终数据文件持久化。`Ok(())` 表示第一阶段“事务提交成功”，
    /// 不表示所有数据文件已完成，也不表示根 WAL 已确认或改名 `.bak`。第二阶段只有全部持久化
    /// 子表发出成功信号后才由 [`KVDBCommitConfirm`] 异步确认，见 `CONTRACT-TR-002`。
    ///
    /// 在尚未调用根 WAL append/flush 时产生的非 Fatal 逻辑失败可按状态 rollback；WAL 已成功
    /// 后的数据文件失败不能 rollback，WAL 保持未确认并由重启 repair 补齐。方法会执行异步文件
    /// I/O、同步锁和 runtime 任务投递，不保证取消安全；调用方必须等待明确结果并另行观察最终确认。
    /// 普通 Memory/LogOrdered/Btree 三叶事务的共享 TID/CID、单次根 WAL、异步确认、repair 和
    /// 移走 WAL 后最终数据由 `tests/ordinary_multi_table_recovery.rs` 提供真实闭环证据。
    ///
    /// 当前事务安全保证不覆盖根 WAL 自身因磁盘空间/配额、只读或故障文件系统、设备 I/O、
    /// runtime 拒绝任务或文件大小限制导致的 append/flush 失败。依赖层普通 `io::Error` 不保留
    /// 失败阶段和累计写入字节，因此这类错误下的 Normal、`LogCommitFailed` 或 rollback 成功
    /// 都不能证明 WAL 完全未落盘，也不能证明 checkpoint 与磁盘状态已经回到事务前。该限制与
    /// 空 prepare 输出无关，后者直接跳过 WAL I/O。完整现状链、外部处置边界和未来设计入口见
    /// `LIMIT-ROOT-WAL-IO-001`：`docs/ROOT_WAL_IO_FAILURE_BOUNDARY.md`。
    pub async fn commit_modified(&self,
                                 prepare_output: Vec<u8>) -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.commit_modified(prepare_output).await
            },
            _ => panic!("Commit modified db failed, reason: invalid root transaction"),
        }
    }

    /// 提交已经由 [`Self::prepare_with_version`] 成功预提交的根事务，并返回本事务发布的版本。
    ///
    /// token 必须来自同一事务且只能使用一次。返回项只描述 `prepare_with_version` 最终业务
    /// write-set 中的写入；同根 `SchemaCreate` Meta 动作会正常发布版本，但不会进入公开回执。
    /// 本方法不会在末尾重新读取可能已被后续事务推进的全局最新版本，返回顺序也不属于稳定
    /// 契约。Ok 表示根 WAL 已按需落地且表数据/版本已经发布，不表示异步数据文件全部持久化或
    /// WAL 已确认为 `.bak`。
    /// 任一提交错误只返回 Err 并丢弃部分回执；WAL 成功后的错误不可 rollback。
    /// 根 WAL 自身的环境/runtime I/O 失败不属于当前事务安全保证，不能从 Err 或回执为空推断
    /// WAL 未写入；其边界与 [`Self::commit_modified`] 完全相同。
    pub async fn commit_with_version(&self,
                                     prepare_output: Vec<u8>)
        -> Result<Vec<TableKeyVersion>, KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                #[cfg(feature = "trace")]
                let metric_guard = tr
                    .0
                    .db_mgr
                    .0
                    .key_versions
                    .begin_api_call(KeyVersionApiOperation::Commit);
                let result = tr.commit_with_version(prepare_output).await;
                #[cfg(feature = "trace")]
                metric_guard.finish(result.is_ok());
                result
            },
            _ => panic!("Commit with version failed, reason: invalid root transaction"),
        }
    }

    /// 回滚处于可恢复失败状态的整棵事务树。
    ///
    /// 只能对 RootTr 调用；子表 variant 会 panic。本方法不是任意时刻可用的 cancel：当前
    /// `Transaction2PcManager` 只接受 ActionFailed、PrepareFailed 或 LogCommitFailed，且事务树
    /// 中不得存在 Fatal。Start、Prepared、Commited、CommitFailed 等其它状态调用会返回错误，
    /// 并可能把状态推进到 RollbackFailed。
    ///
    /// 成功 rollback 会丢弃未发布的子表 COW 修改并从 manager 注销根事务；因为合法回滚点在
    /// 根 WAL 成功落地和数据文件写入之前，不会撤销已提交数据。Fatal 永不可 rollback。方法
    /// 可能 await 子事务回滚并获取同步锁；成功后返回 `Ok(())`，失败保留错误等级和事务状态，
    /// 不应继续复用严重失败句柄。
    /// 多叶根在首个 prepare 冲突后仍会遍历全部叶子：此前 `Prepared`、冲突
    /// `PrepareFailed` 和尚未 prepare 的 `Inited` 叶子都必须进入 `Rollbacked`。成功回滚后旧
    /// 根及叶子即使仍有外部 `Arc` 也只是已关闭句柄；后续重试必须使用全新根和全新 TID/CID。
    /// 该生命周期由 `tests/ordinary_multi_table_conflict_rollback.rs` 验证；同批大量失败根与
    /// 多个不相交成功根并发收口后的 manager 配平和预留释放由
    /// `tests/ordinary_multi_table_concurrency.rs` 验证。
    ///
    /// 上述安全结论只适用于当前受支持的事务失败域。若 `LogCommitFailed` 来源是根 WAL 的磁盘、
    /// 文件系统、设备、runtime 或文件大小限制错误，当前依赖链无法证明 0/部分/完整落盘状态，
    /// 即使本方法返回 `Ok(())` 也不承诺事务安全或 logger checkpoint 已清理；见
    /// `LIMIT-ROOT-WAL-IO-001`。
    pub async fn rollback_modified(&self) -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.rollback_modified().await
            },
            _ => panic!("Rollback modified db failed, reason: invalid root transaction"),
        }
    }

    /// 在键值对数据库事务的根事务内，异步预提交本次事务对键值对数据库的所有修复修改，不返回预提交的输出
    async fn prepare_repair(&self,
                            transaction_uid: Guid)
                           -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.prepare_repair(transaction_uid).await
            },
            _ => panic!("Repair prepare modified db failed, reason: invalid root transaction"),
        }
    }

    /// 键值对数据库事务的根事务内，异步提交本次事务对键值对数据库的所有修复修改
    async fn commit_repair(&self,
                           transaction_uid: Guid,
                           commit_uid: Guid,
                           prepare_output: Vec<u8>)
        -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.commit_repair(transaction_uid,
                                 commit_uid,
                                 prepare_output).await
            },
            _ => panic!("Repair commit modified db failed, reason: invalid root transaction"),
        }
    }
}

/// 根事务拥有的有序子事务快照。
///
/// 根在每张表首次产生需要参加 2PC 的动作时，把该表唯一子事务追加到列表；顺序是首次触表
/// 顺序，不是表名或表类型顺序。同表后续动作复用既有 owner，不重复追加。公开调用方通常只会
/// 通过 [`TransactionTree::to_children`] 取得本类型，不能直接构造或修改根列表。
///
/// clone 会复制 `VecDeque` 并克隆其中的共享事务句柄，时间和新增空间为 O(n)；所得对象是取得
/// 时的节点集合快照，后续根新增子事务不会出现在旧 clone 中。`Iterator::next` 只从该快照头部
/// O(1) 弹出一个句柄，不会删除根中的节点或改变 2PC 顺序。持有快照会延长其中子事务及其表
/// 快照的生命周期，但不会创建新的逻辑事务或 manager 登记。
#[derive(Clone)]
pub struct KVDBChildTrList<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(VecDeque<KVDBTransaction<C, Log>>);

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Iterator for KVDBChildTrList<C, Log> {
    type Item = KVDBTransaction<C, Log>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_front()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVDBChildTrList<C, Log> {
    /// 构建空的根子事务列表，不分配子节点或登记 manager。
    #[inline]
    pub(crate) fn new() -> Self {
        KVDBChildTrList(VecDeque::default())
    }

    /// 获取当前列表快照中的子事务数量。
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    /// 把共享子事务句柄追加到尾部并返回追加后的数量。
    #[inline]
    pub(crate) fn join(&mut self, tr: KVDBTransaction<C, Log>) -> usize {
        self.0.push_back(tr);
        self.len()
    }
}

/// 一棵数据库事务树的共享根节点。
///
/// 根节点保存 source、事务/提交 UID、2PC 状态、可写/持久化标志、当前未执行的 timeout、按
/// 表名索引的子事务 map、按首次触表顺序排列的子事务 list、原子业务协议三态以及 manager
/// clone。公开建表可先登记唯一 SchemaCreate Meta 节点而不选择业务协议；第一次非空普通动作
/// 或版本 prepare 再原子选择 Ordinary/Versioned。纯 `table_meta/keys/values` 不选择协议，后
/// 两者在没有可复用 Ordinary 节点时使用脱离根容器的快照事务。
///
/// 应用通常不直接构造或匹配本类型，而通过 [`KVDBTransaction::RootTr`] 使用。clone 共享同一
/// 状态机和子事务，不创建独立事务。根持有 manager，prepare 后 manager registry 又持有根；
/// 正常 commit/rollback 的 `finish` 会解除 registry 边，非法/取消流程可能延长生命周期。
///
/// [`KVDBManager::transaction`] 返回时，本节点是 `Start`、没有 TID/CID、没有子节点且尚未向
/// manager 登记；创建本身不执行 WAL、表 I/O 或异步调度。首次 prepare 调用上游 `start` 后，
/// manager 只登记这个外层根，并把同一个 TID 发布给当时整棵 owned tree；根需要 WAL 时再把
/// 同一个 CID 发布给全部节点。根的 `persistence` 只聚合“是否写根 WAL”，不表示根拥有数据
/// 文件。完整证据见
/// `docs/ROOT_TRANSACTION_CONSTRUCTION_ACCEPTANCE.md#root-transaction-construction-index`。
#[derive(Clone)]
pub struct RootTransaction<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerRootTransaction<C, Log>>);

// SAFETY: InnerRootTransaction 的可变 UID/status/子事务容器由 SpinLock 保护，协议和持久化
// 标志分别由 AtomicU8/AtomicBool 保护，其余字段构造后只读；manager 自身满足 Send。移动
// Arc 不改变内部地址或别名。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for RootTransaction<C, Log> {}
// SAFETY: 共享引用只能通过上述锁/原子或线程安全 manager 访问可变状态。该 impl 保证内存
// 安全，不保证对同一根事务并发调用多个动作或生命周期方法具有事务级串行语义。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for RootTransaction<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> AsyncTransaction for RootTransaction<C, Log> {
    type Output = ();
    type Error = KVTableTrError;

    fn is_writable(&self) -> bool {
        self.0.writable
    }

    // 键值对数据库的提交，会把所有子事务的预提交输出合成为一个提交输入，用于写入提交日志，所以也不需要并发
    fn is_concurrent_commit(&self) -> bool {
        false
    }

    // 键值对数据库的预提交基本都是内存操作，所以回滚也不需要并发
    fn is_concurrent_rollback(&self) -> bool {
        false
    }

    fn get_source(&self) -> Atom {
        self.0.source.clone()
    }

    fn init(&self)
            -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        async move {
            Ok(())
        }.boxed()
    }

    fn rollback(&self)
                -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        async move {
            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Transaction2Pc for RootTransaction<C, Log> {
    type Tid = Guid;
    type Pid = Guid;
    type Cid = Guid;
    type PrepareOutput = Vec<u8>;
    type PrepareError = KVTableTrError;
    type ConfirmOutput = ();
    type ConfirmError = KVTableTrError;
    type CommitConfirm = KVDBCommitConfirm<C, Log>;

    // 键值对数据库的根事务
    fn is_require_persistence(&self) -> bool {
        self.0.persistence.load(Ordering::Relaxed)
    }

    fn require_persistence(&self) {
        self.0.persistence.store(true, Ordering::Relaxed);
    }

    // 键值对数据库的预提交基本都是内存操作，不需要并发
    fn is_concurrent_prepare(&self) -> bool {
        false
    }

    // 键值对数据库的根事务是根事务，要求所有子事务的事务相关唯一id与根事务相同
    fn is_enable_inherit_uid(&self) -> bool {
        true
    }

    fn get_transaction_uid(&self) -> Option<<Self as Transaction2Pc>::Tid> {
        self.0.tid.lock().clone()
    }

    fn set_transaction_uid(&self, uid: <Self as Transaction2Pc>::Tid) {
        *self.0.tid.lock() = Some(uid);
    }

    fn get_prepare_uid(&self) -> Option<<Self as Transaction2Pc>::Pid> {
        None
    }

    fn set_prepare_uid(&self, _uid: <Self as Transaction2Pc>::Pid) {}

    fn get_commit_uid(&self) -> Option<<Self as Transaction2Pc>::Cid> {
        self.0.cid.lock().clone()
    }

    fn set_commit_uid(&self, uid: <Self as Transaction2Pc>::Cid) {
        *self.0.cid.lock() = Some(uid);
    }

    fn get_prepare_timeout(&self) -> u64 {
        self.0.prepare_timeout
    }

    fn get_commit_timeout(&self) -> u64 {
        self.0.commit_timeout
    }

    // 预提交键值对数据库的根事务
    fn prepare(&self)
               -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        async move {
            if self.is_require_persistence() {
                //本次键值对数据库的根事务，需要持久化，则写入本次键值对数据库的根事务的事务唯一id的预提交输出缓冲区
                let mut prepare_output_head = Vec::new();
                let transaction_uid: Guid = self.get_transaction_uid().unwrap();
                prepare_output_head.put_u128_le(transaction_uid.0); //写入事务唯一id

                Ok(Some(prepare_output_head))
            } else {
                //本次键值对数据库的根事务，不需要持久化，则立即返回
                Ok(None)
            }
        }.boxed()
    }

    fn prepare_conflicts(&self) -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        self.prepare()
    }

    fn commit(&self, _confirm: <Self as Transaction2Pc>::CommitConfirm)
              -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        // 根节点没有独立数据文件，最终持久化由各子表负责；因此根 commit 不调用确认器，
        // 也不贡献成功计数。事务树完成调度不等于根 WAL 已最终确认。详见 CONTRACT-CFM-001：
        // docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only。
        async move {
            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Transaction2PcAllConflicts for RootTransaction<C, Log> {
    fn precheck_all_conflicts(&self)
        -> BoxFuture<'_, Result<(), <Self as Transaction2Pc>::PrepareError>> {
        let tr = self.clone();
        async move {
            tr.check_version_table_identities().await
        }.boxed()
    }

    fn prepare_all_conflicts(&self)
        -> BoxFuture<'_, Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        let tr = self.clone();
        async move {
            tr.check_version_table_identities().await?;
            tr.prepare().await
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> UnitTransaction for RootTransaction<C, Log> {
    type Status = Transaction2PcStatus;
    type Qos = TableTrQos;

    //键值对数据库的根事务，一定不是单元事务
    fn is_unit(&self) -> bool {
        false
    }

    fn get_status(&self) -> <Self as UnitTransaction>::Status {
        self.0.status.lock().clone()
    }

    fn set_status(&self, status: <Self as UnitTransaction>::Status) {
        *self.0.status.lock() = status;
    }

    fn qos(&self) -> <Self as UnitTransaction>::Qos {
        TableTrQos::Safe
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> SequenceTransaction for RootTransaction<C, Log> {
    type Item = Self;

    // 键值对数据库的根事务，一定不是顺序事务
    fn is_sequence(&self) -> bool {
        false
    }

    fn prev_item(&self) -> Option<<Self as SequenceTransaction>::Item> {
        None
    }

    fn next_item(&self) -> Option<<Self as SequenceTransaction>::Item> {
        None
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> TransactionTree for RootTransaction<C, Log> {
    type Node = KVDBTransaction<C, Log>; //键值对数据库的根事务的子事务，必须是键值对数据库事务
    type NodeInterator = KVDBChildTrList<C, Log>;

    // 键值对数据库事务的根事务，一定是事务树
    fn is_tree(&self) -> bool {
        true
    }

    // 获取键值对数据库事务的子事务数量
    fn children_len(&self) -> usize {
        self.0.childs.lock().len()
    }

    // 获取键值对数据库事务的子事务迭代器
    fn to_children(&self) -> Self::NodeInterator {
        self.0.childs.lock().clone()
    }
}

/*
* 键值对数据库的根事务同步方法
*/
impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> RootTransaction<C, Log> {
    #[inline]
    fn protocol(&self) -> RootTransactionProtocol {
        RootTransactionProtocol::from_u8(self.0.protocol.load(Ordering::Acquire))
    }

    /// 为非空普通动作原子选择 Ordinary；已选 Versioned 时在任何子节点或表副作用前拒绝。
    fn select_ordinary_protocol(&self,
                                operation: &'static str)
        -> Result<(), KVTableTrError> {
        loop {
            match self.protocol() {
                RootTransactionProtocol::Ordinary => return Ok(()),
                RootTransactionProtocol::Versioned => {
                    return Err(KVTableTrError::new_transaction_error(
                        ErrorLevel::Normal,
                        format!("{operation} failed, reason: root transaction already selected version protocol")));
                },
                RootTransactionProtocol::Unselected => {
                    match self.0.protocol.compare_exchange(
                        RootTransactionProtocol::Unselected as u8,
                        RootTransactionProtocol::Ordinary as u8,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return Ok(()),
                        Err(_) => continue,
                    }
                },
            }
        }
    }

    /// 在版本子节点最终安装临界区内选择 Versioned；任何既有选择都表示重复或混用。
    fn select_versioned_protocol(&self) -> Result<(), KVTableTrError> {
        match self.0.protocol.compare_exchange(
            RootTransactionProtocol::Unselected as u8,
            RootTransactionProtocol::Versioned as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(protocol) => Err(KVTableTrError::new_transaction_error(
                ErrorLevel::Normal,
                format!("Prepare with version failed, reason: root transaction already selected {:?} protocol",
                        RootTransactionProtocol::from_u8(protocol)))),
        }
    }

    /// 建表只能发生在协议选择前；调用方还会在持有 childs_map 时复核，封闭并发选择窗口。
    fn ensure_schema_prelude_protocol(&self) -> IOResult<()> {
        match self.protocol() {
            RootTransactionProtocol::Unselected => Ok(()),
            protocol => Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Create table failed, reason: root transaction already selected {:?} protocol",
                        protocol))),
        }
    }

    #[inline]
    fn is_schema_create_child(name: &Atom,
                              child: &KVDBTransaction<C, Log>) -> bool {
        name.as_str() == DEFAULT_DB_TABLES_META_DIR
            && matches!(child,
                        KVDBTransaction::MetaTabTr(tr)
                            if tr.prepare_mode() == PrepareMode::SchemaCreate)
    }

    /// 判断当前根容器是否只包含零个或一个合法 SchemaCreate Meta 节点。
    ///
    /// 调用方必须同时持有 childs_map 和 childs，并先检查两者长度相等。当前构造器保证二者按
    /// 同一临界区同步插入；这里只验证允许版本协议继承的唯一节点，不扫描或修改表状态。
    fn contains_only_schema_create_child(
        childes_map: &XHashMap<Atom, KVDBTransaction<C, Log>>,
        childes_len: usize,
    ) -> bool {
        if childes_map.len() != childes_len || childes_map.len() > 1 {
            return false;
        }
        childes_map
            .iter()
            .all(|(name, child)| Self::is_schema_create_child(name, child))
    }

    // 获取根确认器期待的成功信号数：每个 persistence=true 子事务恰好计一次，根事务不计。
    // 只有读动作的可写事务会得到 0；此时确认器只作为 inert 参数传过事务树，任何节点都不得
    // 调用它，也不存在需要确认的根 WAL。该值的协议边界详见 CONTRACT-CFM-001：
    // docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only。
    fn persistent_children_len(&self) -> usize {
        let mut len = 0;
        for child in self.to_children() {
            if child.is_require_persistence() {
                len += 1;
            }
        }

        len
    }

    /// 创建或复用公开建表前导阶段唯一的 SchemaCreate Meta 子事务。
    ///
    /// 调用方必须持有 tables registry guard 和 childs_map guard；本函数只短暂取得 childs，
    /// 不 await、不执行 I/O。已有节点只有在表名、variant 和 prepare mode 全部匹配时才可复用，
    /// 防止同一根 TID 被两个 Meta owner 共同消费。
    fn schema_meta_transaction(
        &self,
        name: Atom,
        table: &RegisteredTable<C, Log>,
        is_persistent: bool,
        childes_map: &mut XHashMap<Atom, KVDBTransaction<C, Log>>,
    ) -> IOResult<KVDBTransaction<C, Log>> {
        self.ensure_schema_prelude_protocol()?;
        if let Some(existing) = childes_map.get(&name) {
            if Self::is_schema_create_child(&name, existing) {
                if is_persistent {
                    existing.require_persistence();
                }
                return Ok(existing.clone());
            }
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Create table failed, reason: root already contains a non-schema Meta transaction for {:?}",
                        name.as_str()),
            ));
        }
        if !childes_map.is_empty() || name.as_str() != DEFAULT_DB_TABLES_META_DIR {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Create table failed, reason: root already contains non-schema transaction children",
            ));
        }

        let KVDBTable::MetaTab(tab) = &table.table else {
            return Err(Error::new(
                ErrorKind::Other,
                "Create table failed, reason: invalid internal Meta table registration",
            ));
        };
        let tr = MetaTabTr::new_managed(
            self.get_source(),
            self.is_writable(),
            is_persistent,
            self.get_prepare_timeout(),
            self.get_commit_timeout(),
            tab.clone(),
            table.versions.clone(),
            PrepareMode::SchemaCreate,
            XHashMap::default(),
            None,
            XHashMap::default(),
        );
        let table_tr = KVDBTransaction::MetaTabTr(tr);
        childes_map.insert(name, table_tr.clone());
        self.0.childs.lock().join(table_tr.clone());
        Ok(table_tr)
    }

    /// 创建指定名称表的普通子事务，并同时登记每表唯一 owner 与有序事务树节点。
    ///
    /// 调用方已经确认该表尚无 owner 并持续持有 `childs_map` guard。本函数先把 owner 写入 map，
    /// 再在同一 map 临界区内把相同句柄追加到 `childs`；因此 child list 顺序就是普通非空动作的
    /// 首次触表顺序，不是表名或表类型顺序。同表后续动作必须从 map 复用原 owner，不能再次追加
    /// 或改变顺序。事务 manager 按此列表串行 prepare/commit/rollback；多个外层根使用相反顺序
    /// 并发 prepare 时可能形成交叉预留，但不会在这里持有跨表锁或执行 await。
    ///
    /// `is_persistent` 只表示该子事务动作是否进入根 WAL，不表示表拥有独立数据文件。六种三表
    /// 排列、交叉冲突、全树 rollback、重试和 repair 由
    /// `tests/ordinary_multi_table_ordering.rs` 验证，完整边界见 `SI-045`。
    fn table_transaction(&self,
                         name: Atom,
                         table: &RegisteredTable<C, Log>,
                         is_persistent: bool,
                         childes_map: &mut XHashMap<Atom, KVDBTransaction<C, Log>>)
                         -> KVDBTransaction<C, Log> {
        // 调用方已经持有 childs_map；版本最终安装也使用同一锁，并在释放前把协议切为
        // Versioned。普通动作入口先原子选择 Ordinary，因此这里只做不变量防御，不允许通过
        // 直接内部调用把 Ordinary 节点插入 Unselected/Versioned 树。
        assert_eq!(self.protocol(),
                   RootTransactionProtocol::Ordinary,
                   "Create ordinary table transaction failed, table: {:?}, reason: root transaction did not select ordinary protocol",
                   name.as_str());
        match &table.table {
            KVDBTable::MetaTab(tab) => {
                //创建元信息表的表事务，并作为子事务注册到根事务上
                let tr = MetaTabTr::new_managed(self.get_source(),
                                                self.is_writable(),
                                                is_persistent,
                                                self.get_prepare_timeout(),
                                                self.get_commit_timeout(),
                                                tab.clone(),
                                                table.versions.clone(),
                                                PrepareMode::Ordinary,
                                                XHashMap::default(),
                                                None,
                                                XHashMap::default());
                let table_tr = KVDBTransaction::MetaTabTr(tr);

                //注册到键值对数据库的根事务
                childes_map.insert(name, table_tr.clone());
                self.0.childs.lock().join(table_tr.clone());

                table_tr
            },
            KVDBTable::MemOrdTab(tab) => {
                //创建有序内存表的表事务，并作为子事务注册到根事务上
                let tr = MemOrdTabTr::new_managed(self.get_source(),
                                                  self.is_writable(),
                                                  is_persistent,
                                                  self.get_prepare_timeout(),
                                                  self.get_commit_timeout(),
                                                  tab.clone(),
                                                  table.versions.clone(),
                                                  PrepareMode::Ordinary,
                                                  XHashMap::default(),
                                                  None,
                                                  XHashMap::default());
                let table_tr = KVDBTransaction::MemOrdTabTr(tr);

                //注册到键值对数据库的根事务
                childes_map.insert(name, table_tr.clone());
                self.0.childs.lock().join(table_tr.clone());

                table_tr
            },
            KVDBTable::LogOrdTab(tab) => {
                let tr = LogOrdTabTr::new_managed(self.get_source(),
                                                  self.is_writable(),
                                                  is_persistent,
                                                  self.get_prepare_timeout(),
                                                  self.get_commit_timeout(),
                                                  tab.clone(),
                                                  table.versions.clone(),
                                                  PrepareMode::Ordinary,
                                                  XHashMap::default(),
                                                  None,
                                                  XHashMap::default());
                let table_tr = KVDBTransaction::LogOrdTabTr(tr);

                //注册到键值对数据库的根事务
                childes_map.insert(name, table_tr.clone());
                self.0.childs.lock().join(table_tr.clone());

                table_tr
            },
            KVDBTable::LogWTab(tab) => {
                let tr = LogWTabTr::new_managed(self.get_source(),
                                                self.is_writable(),
                                                is_persistent,
                                                self.get_prepare_timeout(),
                                                self.get_commit_timeout(),
                                                tab.clone(),
                                                table.versions.clone(),
                                                PrepareMode::Ordinary,
                                                XHashMap::default(),
                                                None,
                                                XHashMap::default());
                let table_tr = KVDBTransaction::LogWTabTr(tr);

                //注册到键值对数据库的根事务
                childes_map.insert(name, table_tr.clone());
                self.0.childs.lock().join(table_tr.clone());

                table_tr
            },
            KVDBTable::BtreeOrdTab(tab) => {
                let tr = BtreeOrdTabTr::new_managed(self.get_source(),
                                                    self.is_writable(),
                                                    is_persistent,
                                                    self.get_prepare_timeout(),
                                                    self.get_commit_timeout(),
                                                    tab.clone(),
                                                    table.versions.clone(),
                                                    PrepareMode::Ordinary,
                                                    XHashMap::default(),
                                                    None,
                                                    XHashMap::default());
                let table_tr = KVDBTransaction::BtreeOrdTabTr(tr);

                //注册到键值对数据库的根事务
                childes_map.insert(name, table_tr.clone());
                self.0.childs.lock().join(table_tr.clone());

                table_tr
            },
        }
    }

    /// 为纯迭代器创建一个不参与根 2PC 的只读快照事务。
    ///
    /// 返回事务只负责把表的创建时 COW/redb 快照所有权交给流；它不租用 Key 版本、不写入
    /// childs_map/childs，也不会被 manager 分配根 TID。若根中已有普通表事务，调用方应复用
    /// 该事务以保留 read-your-own-write，而不是调用本函数。
    fn detached_iterator_table_transaction(&self,
                                           table: &RegisteredTable<C, Log>)
        -> KVDBTransaction<C, Log> {
        match &table.table {
            KVDBTable::MetaTab(tab) => {
                KVDBTransaction::MetaTabTr(tab.transaction(self.get_source(),
                                                           false,
                                                           false,
                                                           self.get_prepare_timeout(),
                                                           self.get_commit_timeout()))
            },
            KVDBTable::MemOrdTab(tab) => {
                KVDBTransaction::MemOrdTabTr(tab.transaction(self.get_source(),
                                                             false,
                                                             false,
                                                             self.get_prepare_timeout(),
                                                             self.get_commit_timeout()))
            },
            KVDBTable::LogOrdTab(tab) => {
                KVDBTransaction::LogOrdTabTr(tab.transaction(self.get_source(),
                                                             false,
                                                             false,
                                                             self.get_prepare_timeout(),
                                                             self.get_commit_timeout()))
            },
            KVDBTable::LogWTab(tab) => {
                KVDBTransaction::LogWTabTr(tab.transaction(self.get_source(),
                                                           false,
                                                           false,
                                                           self.get_prepare_timeout(),
                                                           self.get_commit_timeout()))
            },
            KVDBTable::BtreeOrdTab(tab) => {
                KVDBTransaction::BtreeOrdTabTr(tab.transaction(self.get_source(),
                                                               false,
                                                               false,
                                                               self.get_prepare_timeout(),
                                                               self.get_commit_timeout()))
            },
        }
    }

    /// 在根锁之外构造一个版本子事务；调用方随后统一、原子地安装整批子节点。
    fn build_versioned_table_transaction(&self,
                                   table: &RegisteredTable<C, Log>,
                                   is_persistent: bool,
                                   expected: XHashMap<Binary, Version>,
                                   receipt: VersionReceipt,
                                   actions: XHashMap<Binary, crate::KVActionLog>)
        -> KVDBTransaction<C, Log> {
        match &table.table {
            KVDBTable::MetaTab(tab) => {
                KVDBTransaction::MetaTabTr(MetaTabTr::new_managed(
                    self.get_source(),
                    self.is_writable(),
                    is_persistent,
                    self.get_prepare_timeout(),
                    self.get_commit_timeout(),
                    tab.clone(),
                    table.versions.clone(),
                    PrepareMode::Versioned,
                    expected,
                    Some(receipt),
                    actions))
            },
            KVDBTable::MemOrdTab(tab) => {
                KVDBTransaction::MemOrdTabTr(MemOrdTabTr::new_managed(
                    self.get_source(),
                    self.is_writable(),
                    is_persistent,
                    self.get_prepare_timeout(),
                    self.get_commit_timeout(),
                    tab.clone(),
                    table.versions.clone(),
                    PrepareMode::Versioned,
                    expected,
                    Some(receipt),
                    actions))
            },
            KVDBTable::LogOrdTab(tab) => {
                KVDBTransaction::LogOrdTabTr(LogOrdTabTr::new_managed(
                    self.get_source(),
                    self.is_writable(),
                    is_persistent,
                    self.get_prepare_timeout(),
                    self.get_commit_timeout(),
                    tab.clone(),
                    table.versions.clone(),
                    PrepareMode::Versioned,
                    expected,
                    Some(receipt),
                    actions))
            },
            KVDBTable::LogWTab(tab) => {
                KVDBTransaction::LogWTabTr(LogWTabTr::new_managed(
                    self.get_source(),
                    self.is_writable(),
                    is_persistent,
                    self.get_prepare_timeout(),
                    self.get_commit_timeout(),
                    tab.clone(),
                    table.versions.clone(),
                    PrepareMode::Versioned,
                    expected,
                    Some(receipt),
                    actions))
            },
            KVDBTable::BtreeOrdTab(tab) => {
                KVDBTransaction::BtreeOrdTabTr(BtreeOrdTabTr::new_managed(
                    self.get_source(),
                    self.is_writable(),
                    is_persistent,
                    self.get_prepare_timeout(),
                    self.get_commit_timeout(),
                    tab.clone(),
                    table.versions.clone(),
                    PrepareMode::Versioned,
                    expected,
                    Some(receipt),
                    actions))
            },
        }
    }
}

/*
* 键值对数据库的根事务异步方法
*/
impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> RootTransaction<C, Log> {
    /// 检查版本协议装配时固定的表身份；只读失效汇总为冲突，任何写表失效优先返回 Common。
    async fn check_version_table_identities(&self) -> Result<(), KVTableTrError> {
        let Some(context) = self.0.version_context.lock().clone() else {
            return Ok(());
        };
        let tables = self.0.db_mgr.0.tables.read().await;
        let mut conflicts = Vec::new();
        let mut invalid_write_table = None;

        for identity in &context.tables {
            let exact = match (&identity.versions, tables.get(&identity.name)) {
                (Some(expected), Some(current)) => expected.ptr_eq(&current.versions),
                _ => false,
            };
            if exact {
                continue;
            }
            if identity.has_write {
                if invalid_write_table.is_none() {
                    invalid_write_table = Some(identity.name.clone());
                }
            } else {
                for key in &identity.read_keys {
                    conflicts.push(TableKeyConflict {
                        table: identity.name.clone(),
                        key: key.clone(),
                        kind: VersionConflictKind::ReadSetVersionMismatch,
                    });
                }
            }
        }
        drop(tables);

        if let Some(table) = invalid_write_table {
            return Err(KVTableTrError::new_transaction_error(
                ErrorLevel::Normal,
                format!("Prepare with version failed, table: {:?}, reason: write table is missing or was replaced after transaction assembly",
                        table.as_str())));
        }
        if conflicts.is_empty() {
            Ok(())
        } else {
            Err(KVTableTrError::new_all_conflicts_error(conflicts))
        }
    }

    /// 异步获取表的元信息
    #[inline]
    async fn table_meta(&self, table: Atom) -> Option<KVTableMeta> {
        let meta_table = Atom::from(DEFAULT_DB_TABLES_META_DIR);
        let key = table_to_binary(&table);

        // 同根专用 DDL 已经建立 Meta 私有 COW 根时，必须观察该根才能读到尚未提交的创建或
        // 删除；clone 后立即释放 childs_map，禁止 SpinLock guard 跨 await。dirty_query 只读
        // root_mut，不把本次专用元信息检查登记成 Ordinary Read 或版本 read-set。外部协议仍
        // 禁止直接通过普通/版本 KV API 操作内部 Meta 表。
        let child = self.0.childs_map.lock().get(&meta_table).cloned();
        if let Some(KVDBTransaction::MetaTabTr(tr)) = child {
            return tr.dirty_query(key).await.map(KVTableMeta::from);
        }

        // 没有同根 Meta 子事务时只克隆已注册 Meta 表句柄；registry guard 在点读 COW 根前释放。
        // 一次 root lock 读取是内存安全且自洽的，但不是跨后续动作保持的数据库级快照。
        let registered = self
            .0
            .db_mgr
            .0
            .tables
            .read()
            .await
            .get(&meta_table)
            .cloned();
        match registered.map(|registered| registered.table) {
            Some(KVDBTable::MetaTab(meta)) => {
                meta.query_committed(&key).map(KVTableMeta::from)
            },
            _ => None,
        }
    }

    /// 异步创建表。
    ///
    /// 当前实现只校验表名 UTF-8 字节长度为 `1..=4096`，不会拒绝绝对路径、`.`、`..`、平台
    /// 分隔符、Windows 特殊名称或 symlink 别名；持久表随后直接执行
    /// `tables_path.join(name)`。调用方必须按可信相对逻辑名称使用，数据库尚不提供路径
    /// containment 保证。该现状不是最终/最佳设计，见
    /// `docs/SEMANTIC_CONTRACTS.md#q-path-001` 和 `FIND-PATH-001`。
    #[inline]
    async fn create_table_with_options(&self,
                                       name: Atom,
                                       meta: KVTableMeta,
                                       options: CreateTableOptions,
                                       enable_accelerated_repair: bool) -> IOResult<()>
    {
        // 必须先于根持久化标记、注册表写锁、Meta 修改及文件/目录创建拒绝非法名称。
        validate_table_name(&name, ErrorKind::InvalidInput, "create table")?;
        // schema prelude 只允许发生在普通/版本协议选择前；该快检没有共享副作用，取得
        // childs_map 后还会再次检查，防止版本最终安装与本调用交错形成混合树。
        self.ensure_schema_prelude_protocol()?;

        //检查待创建的指定名称的表是否存在
        let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
        let mut tables = self.0.db_mgr.0.tables.write().await;
        let meta_table = tables.get(&meta_table_name).cloned().ok_or_else(|| {
            Error::new(ErrorKind::Other,
                       "Create table failed, reason: internal Meta table is not registered")
        })?;
        let mut meta_table_tr = None;

        if tables.contains_key(&name) {
            //指定名称的表已存在
            {
                // 已存在表先创建非持久化 SchemaCreate owner 并读取定义；幂等成功不产生 Meta
                // 写和确认。同步根锁只覆盖节点复用/插入，必须在 query await 前释放。
                let mut childes_map = self.0.childs_map.lock();
                meta_table_tr = Some(self.schema_meta_transaction(meta_table_name.clone(),
                                                                  &meta_table,
                                                                  false,
                                                                  &mut *childes_map)?);
            }
            // schema owner 已在根协议仍为 Unselected 时安装；此后才允许改变根持久化属性。
            self.require_persistence();

            if let Some(KVDBTransaction::MetaTabTr(tr)) = meta_table_tr.as_ref() {
                if let Some(value) = tr.query(table_to_binary(&name)).await {
                    //指定名称的表的元信息存在
                    let table_meta = KVTableMeta::from(value);
                    if table_meta == meta {
                        //待创建表的名称与已存在的表相同，且元信息相同，则立即返回创建成功
                        return Ok(());
                    } else {
                        //待创建表的名称与已存在的表相同，但元信息不同
                        if table_meta.is_persistence() {
                            match tables.get(&name).map(|registered| &registered.table) {
                                Some(KVDBTable::LogOrdTab(tab)) => {
                                    if tab.len() > 0 {
                                        //已存在的同名表是持久化表，且元信息不同，且表中有记录，则表名冲突
                                        return Err(Error::new(ErrorKind::AlreadyExists,
                                                              format!("Create table failed, name: {:?}, meta: {:?}, reason: name conflict", name, meta)));
                                    }
                                },
                                Some(KVDBTable::LogWTab(tab)) => {
                                    if tab.len() > 0 {
                                        //已存在的同名表是持久化表，且元信息不同，且表中有记录，则表名冲突
                                        return Err(Error::new(ErrorKind::AlreadyExists,
                                                              format!("Create table failed, name: {:?}, meta: {:?}, reason: name conflict", name, meta)));
                                    }
                                },
                                Some(KVDBTable::BtreeOrdTab(tab)) => {
                                    if tab.len() > 0 {
                                        //已存在的同名表是持久化表，且元信息不同，且表中有记录，则表名冲突
                                        return Err(Error::new(ErrorKind::AlreadyExists,
                                                              format!("Create table failed, name: {:?}, meta: {:?}, reason: name conflict", name, meta)));
                                    }
                                },
                                _ => (),
                            }
                        }
                    }
                } else {
                    //指定名称的表的元信息不存在，则立即返回错误原因
                    return Err(Error::new(ErrorKind::AlreadyExists,
                                          format!("Create table failed, name: {:?}, meta: {:?}, reason: name conflict and table meta not exist", name, meta)));
                }
            } else {
                //不是元信息表事务，则立即返回错误原因
                return Err(Error::new(ErrorKind::AlreadyExists,
                                      format!("Create table failed, name: {:?}, meta: {:?}, reason: invalid meta table transaction", name, meta)));
            }
        }

        // 只在确实进入物理构造分支时校验类型专用 options，保持“已存在且同 meta”提前成功时
        // 不观察 options 的既有语义。无效 options 仍使用原 ErrorKind/消息，且不会新增 schema
        // 子节点（已存在表为完成幂等定义检查而创建的只读节点除外）。
        let invalid_options = match meta.table_type {
            KVDBTableType::LogOrdTab => {
                !matches!(&options, CreateTableOptions::LogOrdTab(_, _, _))
            },
            KVDBTableType::BtreeOrdTab => {
                !matches!(&options, CreateTableOptions::BtreeOrdTab(_, _))
            },
            _ => false,
        };
        if invalid_options {
            return Err(Error::new(ErrorKind::Other,
                                  format!("Create table failed, name: {:?}, meta: {:?}, options: {:?}, reason: invalid options",
                                          name,
                                          meta,
                                          options)));
        }

        // 缺表时必须在注册新表前安装 SchemaCreate，使 child list 中 Meta WAL 永远先于新表数据。
        // 已存在但需要替换定义时复用上面的 owner，并只在实际 upsert 前提升持久化标志。
        let meta_table_tr = match meta_table_tr {
            Some(table_tr) => {
                table_tr.require_persistence();
                table_tr
            },
            None => {
                let mut childes_map = self.0.childs_map.lock();
                self.schema_meta_transaction(meta_table_name.clone(),
                                             &meta_table,
                                             true,
                                             &mut *childes_map)?
            },
        };
        //创建表的 Meta 写需要进入根 WAL，因此 schema owner 安装成功后聚合根持久化标志。
        self.require_persistence();

        //待创建的指定名称的表不存在，则创建指定名称的表，并将表的元信息注册到元信息表
        match meta.table_type {
            KVDBTableType::MemOrdTab => {
                //创建一个有序内存表
                let table = MemoryOrderedTable::new(name.clone(),
                                                    meta.persistence);

                //注册创建的有序内存表
                install_registered_table(&mut *tables,
                                         &self.0.db_mgr.0.key_versions,
                                         name.clone(),
                                         KVDBTable::MemOrdTab(table));
            },
            KVDBTableType::LogOrdTab => {
                //创建一个有序日志表
                let table_path = self.0.db_mgr.0.tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                if let CreateTableOptions::LogOrdTab(log_file_limit, block_limit, load_buf_len) = options.clone() {
                    //有序日志表的选项
                    let table =
                        LogOrderedTable::new(self.0.db_mgr.0.rt.clone(),
                                             table_path,
                                             name.clone(),
                                             log_file_limit,
                                             block_limit,
                                             None,
                                             load_buf_len as u64,
                                             true,
                                             16 * 1024 * 1024,
                                             60 * 1000,
                                             self.0.db_mgr.0.notifier.clone()).await;

                    //注册创建的有序日志表
                    install_registered_table(&mut *tables,
                                             &self.0.db_mgr.0.key_versions,
                                             name.clone(),
                                             KVDBTable::LogOrdTab(table));
                } else {
                    //没有有序日志表的选项，则立即返回错误原因
                    return Err(Error::new(ErrorKind::Other,
                                          format!("Create table failed, name: {:?}, meta: {:?}, options: {:?}, reason: invalid options",
                                                  name,
                                                  meta,
                                                  options)));
                }
            },
            KVDBTableType::LogWTab => {
                //创建一个只写日志表
                let table_path = self.0.db_mgr.0.tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                let table =
                    LogWriteTable::new(self.0.db_mgr.0.rt.clone(),
                                         table_path,
                                         name.clone(),
                                         512 * 1024 * 1024,
                                         2 * 1024 * 1024,
                                         None,
                                         2 * 1024 * 1024,
                                         true,
                                         16 * 1024 * 1024,
                                         60 * 1000).await;

                //注册创建的只写日志表
                install_registered_table(&mut *tables,
                                         &self.0.db_mgr.0.key_versions,
                                         name.clone(),
                                         KVDBTable::LogWTab(table));
            },
            KVDBTableType::BtreeOrdTab => {
                //创建一个有序B树表
                let table_path = self.0.db_mgr.0.tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                if let CreateTableOptions::BtreeOrdTab(cache_size, enable_compact) = options.clone() {
                    //有序日志表的选项
                    let table =
                        BtreeOrderedTable::new(self.0.db_mgr.0.rt.clone(),
                                               table_path,
                                               name.clone(),
                                               cache_size,
                                               enable_compact,
                                               1024 * 1024,
                                               60 * 1000,
                                               enable_accelerated_repair,
                                               self.0.db_mgr.0.notifier.clone()).await;

                    //注册创建的有序日志表
                    install_registered_table(&mut *tables,
                                             &self.0.db_mgr.0.key_versions,
                                             name.clone(),
                                             KVDBTable::BtreeOrdTab(table));
                } else {
                    //没有有序日志表的选项，则立即返回错误原因
                    return Err(Error::new(ErrorKind::Other,
                                          format!("Create table failed, name: {:?}, meta: {:?}, options: {:?}, reason: invalid options",
                                                  name,
                                                  meta,
                                                  options)));
                }
            },
        }

        // schema owner 已在物理注册前安装；这里只修改其私有 Meta 根，不再持有 childs_map。
        if let KVDBTransaction::MetaTabTr(tr) = &meta_table_tr {
            if let Err(e) = tr.upsert(table_to_binary(&name),
                                      Binary::from(meta.clone())).await {
                //写入表的元信息失败，则立即返回错误原因
                return Err(Error::new(ErrorKind::Other,
                                      format!("Create table failed, name: {:?}, meta: {:?}, reason: {:?}", name, meta, e)));
            }
        } else {
            //不是元信息表事务，则立即返回错误原因
            return Err(Error::new(ErrorKind::Other,
                                  format!("Create table failed, name: {:?}, meta: {:?}, reason: invalid meta table transaction", name, meta)));
        }

        Ok(())
    }

    /// 通过默认参数异步创建表，表名可以是用文件分隔符分隔的路径，但必须是相对路径，且不允许使用".."
    #[inline]
    async fn create_table(&self,
                          name: Atom,
                          meta: KVTableMeta,
                          enable_accelerated_repair: bool) -> IOResult<()>
    {
        match meta.table_type {
            KVDBTableType::LogOrdTab => {
                self.create_table_with_options(name,
                                               meta,
                                               CreateTableOptions::LogOrdTab(512 * 1024 * 1024,
                                                                             2 * 1024 * 1024,
                                                                             2 * 1024 * 1024),
                                               enable_accelerated_repair)
                    .await
            },
            KVDBTableType::BtreeOrdTab => {
                self.create_table_with_options(name,
                                               meta,
                                               CreateTableOptions::BtreeOrdTab(16 * 1024 * 1024,
                                                                               true),
                                               enable_accelerated_repair)
                    .await
            },
            _ => {
                self.create_table_with_options(name,
                                               meta,
                                               CreateTableOptions::Empty,
                                               enable_accelerated_repair)
                    .await
            },
        }

    }

    /// 异步创建指定的多个表，表名可以是用文件分隔符分隔的路径，但必须是相对路径，且不允许使用".."
    async fn create_multiple_tables(&self,
                                    table_metas: Vec<(Atom, KVTableMeta, Option<CreateTableOptions>)>,
                                    is_checksum: bool,
                                    enable_accelerated_repair: bool)
        -> IOResult<()>
    {
        // 启动加载是该内部批量入口的唯一生产调用方，并保证输入非空、Meta Key 唯一且用户表
        // 尚未注册。空输入和重复已注册项的现有行为不是对内契约，详见
        // docs/REVIEW_FINDINGS.md#find-start-002；BUG-STARTUP-BATCH-001 不扩大到这些分支。
        // 先校验完整输入，避免合法项已注册后才在后项发现损坏名称，造成部分加载副作用。
        for (name, _, _) in &table_metas {
            validate_table_name(name,
                                ErrorKind::InvalidData,
                                "load multiple table metadata")?;
        }
        self.select_ordinary_protocol("Load multiple table metadata")
            .map_err(|error| Error::new(ErrorKind::Other, format!("{error:?}")))?;

        //创建表的操作，一定会创建元信息表事务，而元信息表事务是需要持久化的事务，则根事务也设置为需要持久化
        self.require_persistence();

        //检查待创建的指定名称的表是否存在
        let mut require_create_tables = Vec::new();
        let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
        {
            let tables = self
                .0
                .db_mgr
                .0
                .tables
                .read()
                .await;

            for (name, meta, options) in table_metas {
                if tables.contains_key(&name) {
                    //指定名称的表已存在
                    if let Some(meta_table) = tables.get(&meta_table_name) {
                        //元信息表存在，则获取元信息表事务，并查询指定表的元信息
                        let mut childes_map = self.0.childs_map.lock();
                        let meta_table_tr = if let Some(table_tr) = childes_map.get(&meta_table_name) {
                            //元信息表的子事务存在
                            table_tr.clone()
                        } else {
                            //元信息表的子事务不存在，则创建元信息表的事务，因为可能只是查询操作，所以初始化指定表的子事务为非持久化事务
                            self.table_transaction(meta_table_name.clone(), meta_table, false, &mut *childes_map)
                        };

                        if let KVDBTransaction::MetaTabTr(tr) = &meta_table_tr {
                            if let Some(value) = tr.query(table_to_binary(&name)).await {
                                //指定名称的表的元信息存在
                                let table_meta = KVTableMeta::from(value);
                                if table_meta == meta {
                                    //待创建表的名称与已存在的表相同，且元信息相同，则立即返回创建成功
                                    return Ok(());
                                } else {
                                    //待创建表的名称与已存在的表相同，但元信息不同
                                    if table_meta.is_persistence() {
                                        match tables.get(&name).map(|registered| &registered.table) {
                                            Some(KVDBTable::LogOrdTab(tab)) => {
                                                if tab.len() > 0 {
                                                    //已存在的同名表是持久化表，且元信息不同，且表中有记录，则表名冲突
                                                    return Err(Error::new(ErrorKind::AlreadyExists,
                                                                          format!("Create table failed, name: {:?}, meta: {:?}, reason: name conflict", name, meta)));
                                                }
                                            },
                                            Some(KVDBTable::LogWTab(tab)) => {
                                                if tab.len() > 0 {
                                                    //已存在的同名表是持久化表，且元信息不同，且表中有记录，则表名冲突
                                                    return Err(Error::new(ErrorKind::AlreadyExists,
                                                                          format!("Create table failed, name: {:?}, meta: {:?}, reason: name conflict", name, meta)));
                                                }
                                            },
                                            Some(KVDBTable::BtreeOrdTab(tab)) => {
                                                if tab.len() > 0 {
                                                    //已存在的同名表是持久化表，且元信息不同，且表中有记录，则表名冲突
                                                    return Err(Error::new(ErrorKind::AlreadyExists,
                                                                          format!("Create table failed, name: {:?}, meta: {:?}, reason: name conflict", name, meta)));
                                                }
                                            },
                                            _ => (), //忽略内存表元信息的不同
                                        }
                                    }
                                }
                            } else {
                                //指定名称的表的元信息不存在，则立即返回错误原因
                                return Err(Error::new(ErrorKind::AlreadyExists,
                                                      format!("Create table failed, name: {:?}, meta: {:?}, reason: name conflict and table meta not exist", name, meta)));
                            }
                        } else {
                            //不是元信息表事务，则立即返回错误原因
                            return Err(Error::new(ErrorKind::AlreadyExists,
                                                  format!("Create table failed, name: {:?}, meta: {:?}, reason: invalid meta table transaction", name, meta)));
                        }
                    }
                }

                //记录需要创建的表
                require_create_tables.push((name, meta, options));
            }
        }

        // 并发创建待创建的表。当前每个任务只有在正常返回时才递减 count；spawn 返回值被忽略，
        // 表构造 panic 或 runtime 拒绝任务时 result 可能永远不完成，调用方会持续等待。这是
        // FIND-CTOR-001/FIND-SPAWN-001 已归档的非最终启动错误边界，不能误读为 IOResult 已覆盖
        // 所有表加载失败。
        let result = AsyncValue::new();
        let count = Arc::new(AtomicU64::new(require_create_tables.len() as u64));
        for (name, meta, options) in require_create_tables.clone() {
            let db_rt = self.0.db_mgr.0.rt.clone();
            let tables_path = self
                .0
                .db_mgr
                .0
                .tables_path
                .clone();
            let tables = self
                .0
                .db_mgr
                .0
                .tables
                .clone();
            let key_versions = self.0.db_mgr.0.key_versions.clone();
            let result_copy = result.clone();
            let count_copy = count.clone();

            let notifier = self.0.db_mgr.0.notifier.clone();
            let _ = self.0.db_mgr.0.rt.spawn(async move {
                //待创建的指定名称的表不存在，则创建指定名称的表，并将表的元信息注册到元信息表
                match meta.table_type {
                    KVDBTableType::MemOrdTab => {
                        //创建一个有序内存表
                        let table = MemoryOrderedTable::new(name.clone(),
                                                            meta.persistence);

                        //注册创建的有序内存表
                        let mut tables = tables.write().await;
                        install_registered_table(&mut *tables,
                                                 &key_versions,
                                                 name.clone(),
                                                 KVDBTable::MemOrdTab(table));
                    },
                    KVDBTableType::LogOrdTab => {
                        //创建一个有序日志表
                        let table_path = tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                        if let Some(CreateTableOptions::LogOrdTab(log_file_limit, block_limit, load_buf_len)) = options.clone() {
                            //有序日志表的选项
                            let table =
                                LogOrderedTable::new(db_rt,
                                                     table_path,
                                                     name.clone(),
                                                     log_file_limit,
                                                     block_limit,
                                                     None,
                                                     load_buf_len as u64,
                                                     is_checksum,
                                                     16 * 1024 * 1024,
                                                     60 * 1000,
                                                     notifier).await;

                            //注册创建的有序日志表
                            let mut tables = tables.write().await;
                            install_registered_table(&mut *tables,
                                                     &key_versions,
                                                     name.clone(),
                                                     KVDBTable::LogOrdTab(table));
                        } else {
                            //没有有序日志表的选项，则立即通知错误原因
                            result_copy.set(Err(Error::new(ErrorKind::Other,
                                                           format!("Create table failed, name: {:?}, meta: {:?}, options: {:?}, reason: invalid options",
                                                                   name,
                                                                   meta,
                                                                   options))));
                            return;
                        }
                    },
                    KVDBTableType::LogWTab => {
                        //创建一个只写日志表
                        let table_path = tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                        let table =
                            LogWriteTable::new(db_rt,
                                               table_path,
                                               name.clone(),
                                               512 * 1024 * 1024,
                                               2 * 1024 * 1024,
                                               None,
                                               2 * 1024 * 1024,
                                               is_checksum,
                                               16 * 1024 * 1024,
                                               60 * 1000).await;

                        //注册创建的只写日志表
                        let mut tables = tables.write().await;
                        install_registered_table(&mut *tables,
                                                 &key_versions,
                                                 name.clone(),
                                                 KVDBTable::LogWTab(table));
                    },
                    KVDBTableType::BtreeOrdTab => {
                        //创建一个有序B树表
                        let table_path = tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                        if let Some(CreateTableOptions::BtreeOrdTab(cache_size, enable_compact)) = options.clone() {
                            //有序日志表的选项
                            let table =
                                BtreeOrderedTable::new(db_rt,
                                                       table_path,
                                                       name.clone(),
                                                       cache_size,
                                                       enable_compact,
                                                       1024 * 1024,
                                                       60 * 1000,
                                                       enable_accelerated_repair,
                                                       notifier).await;

                            //注册创建的有序日志表
                            let mut tables = tables.write().await;
                            install_registered_table(&mut *tables,
                                                     &key_versions,
                                                     name.clone(),
                                                     KVDBTable::BtreeOrdTab(table));
                        } else {
                            //没有有序日志表的选项，则立即通知错误原因
                            result_copy.set(Err(Error::new(ErrorKind::Other,
                                                           format!("Create table failed, name: {:?}, meta: {:?}, options: {:?}, reason: invalid options",
                                                                   name,
                                                                   meta,
                                                                   options))));
                            return;
                        }
                    },
                }

                if count_copy.fetch_sub(1, Ordering::AcqRel) <= 1 {
                    //本次所有待创建的表都已创建成功，则立即通知创建成功
                    result_copy.set(Ok(()));
                }
            });
        }

        //等待批量创建表全部成功
        result.await?;

        //注册表的元信息
        let mut tables = self
            .0
            .db_mgr
            .0
            .tables
            .write()
            .await;
        for (name, meta, _options) in require_create_tables {
            if let Some(meta_table) = tables.get(&meta_table_name) {
                let mut childes_map = self.0.childs_map.lock();
                let meta_table_tr = if let Some(table_tr) = childes_map.get(&meta_table_name) {
                    //元信息表的子事务存在，则设置子事务为需要持久化
                    table_tr.require_persistence();
                    table_tr.clone()
                } else {
                    //元信息表的子事务不存在，则创建元信息表的事务，因为需要创建表，所以初始化元信息表的子事务为持久化事务
                    self.table_transaction(meta_table_name.clone(),
                                           meta_table,
                                           true,
                                           &mut *childes_map)
                };

                if let KVDBTransaction::MetaTabTr(tr) = &meta_table_tr {
                    if let Err(e) = tr.upsert(table_to_binary(&name),
                                              Binary::from(meta.clone())).await {
                        //写入表的元信息失败，则立即返回错误原因
                        return Err(Error::new(ErrorKind::Other,
                                              format!("Create table failed, name: {:?}, meta: {:?}, reason: {:?}", name, meta, e)));
                    }
                } else {
                    //不是元信息表事务，则立即返回错误原因
                    return Err(Error::new(ErrorKind::Other,
                                          format!("Create table failed, name: {:?}, meta: {:?}, reason: invalid meta table transaction", name, meta)));
                }
            }
        }

        Ok(())
    }

    /// 异步修复创建表，表名可以是用文件分隔符分隔的路径，但必须是相对路径，且不允许使用".."
    #[inline]
    async fn repair_create_table(&self,
                                 name: Atom,
                                 meta: KVTableMeta,
                                 enable_accelerated_repair: bool) -> IOResult<()>
    {
        // 名称来自已落地根 WAL；非法长度表示持久化数据损坏，而不是本次调用参数错误。
        validate_table_name(&name, ErrorKind::InvalidData, "repair table creation")?;
        self.select_ordinary_protocol("Repair table creation")
            .map_err(|error| Error::new(ErrorKind::Other, format!("{error:?}")))?;

        //检查待创建的指定名称的表是否存在
        let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
        let mut tables = self.0.db_mgr.0.tables.write().await;

        self.0.persistence.store(true, Ordering::Relaxed); //创建表的操作，一定会创建元信息表事务，而元信息表事务是需要持久化的事务，则根事务也设置为需要持久化

        //待创建的指定名称的表不存在，则创建指定名称的表，并将表的元信息注册到元信息表
        match meta.table_type {
            KVDBTableType::MemOrdTab => {
                //创建一个有序内存表
                let table = MemoryOrderedTable::new(name.clone(),
                                                    meta.persistence);

                //注册创建的有序内存表
                install_registered_table(&mut *tables,
                                         &self.0.db_mgr.0.key_versions,
                                         name.clone(),
                                         KVDBTable::MemOrdTab(table));
            },
            KVDBTableType::LogOrdTab => {
                //创建一个有序日志表
                let table_path = self.0.db_mgr.0.tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                let table =
                    LogOrderedTable::new(self.0.db_mgr.0.rt.clone(),
                                         table_path,
                                         name.clone(),
                                         512 * 1024 * 1024,
                                         2 * 1024 * 1024,
                                         None,
                                         2 * 1024 * 1024,
                                         true,
                                         16 * 1024 * 1024,
                                         60 * 1000,
                                         self.0.db_mgr.0.notifier.clone()).await;

                //注册创建的有序日志表
                install_registered_table(&mut *tables,
                                         &self.0.db_mgr.0.key_versions,
                                         name.clone(),
                                         KVDBTable::LogOrdTab(table));
            },
            KVDBTableType::LogWTab => {
                //创建一个只写日志表
                let table_path = self.0.db_mgr.0.tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                let table =
                    LogWriteTable::new(self.0.db_mgr.0.rt.clone(),
                                       table_path,
                                       name.clone(),
                                       512 * 1024 * 1024,
                                       2 * 1024 * 1024,
                                       None,
                                       2 * 1024 * 1024,
                                       true,
                                       16 * 1024 * 1024,
                                       60 * 1000).await;

                //注册创建的只写日志表
                install_registered_table(&mut *tables,
                                         &self.0.db_mgr.0.key_versions,
                                         name.clone(),
                                         KVDBTable::LogWTab(table));
            },
            KVDBTableType::BtreeOrdTab => {
                //尝试创建一个有序B树表
                let table_path = self.0.db_mgr.0.tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                if let Some(table) =
                    BtreeOrderedTable::try_new(self.0.db_mgr.0.rt.clone(),
                                               table_path,
                                               name.clone(),
                                               DEFAULT_CACHE_SIZE,
                                               true,
                                               1024 * 1024,
                                               60 * 1000,
                                               enable_accelerated_repair,
                                               self.0.db_mgr.0.notifier.clone()).await {
                    //尝试创建成功，则注册创建的有序日志表
                    install_registered_table(&mut *tables,
                                             &self.0.db_mgr.0.key_versions,
                                             name.clone(),
                                             KVDBTable::BtreeOrdTab(table));
                }
            },
        }

        //注册表的元信息
        if let Some(meta_table) = tables.get(&meta_table_name) {
            let mut childes_map = self.0.childs_map.lock();
            let meta_table_tr = if let Some(table_tr) = childes_map.get(&meta_table_name) {
                //元信息表的子事务存在，则设置子事务为需要持久化
                table_tr.require_persistence();
                table_tr.clone()
            } else {
                //元信息表的子事务不存在，则创建元信息表的事务，因为需要创建表，所以初始化元信息表的子事务为持久化事务
                self.table_transaction(meta_table_name, meta_table, true, &mut *childes_map)
            };

            if let KVDBTransaction::MetaTabTr(tr) = &meta_table_tr {
                if let Err(e) = tr.upsert(table_to_binary(&name),
                                          Binary::from(meta.clone())).await {
                    //写入表的元信息失败，则立即返回错误原因
                    return Err(Error::new(ErrorKind::Other,
                                          format!("Create table failed, name: {:?}, meta: {:?}, reason: {:?}", name, meta, e)));
                }
            } else {
                //不是元信息表事务，则立即返回错误原因
                return Err(Error::new(ErrorKind::Other,
                                      format!("Create table failed, name: {:?}, meta: {:?}, reason: invalid meta table transaction", name, meta)));
            }
        }

        Ok(())
    }

    /// 实现根事务的删表动作；公开契约见 [`KVDBTransaction::remove_table`]。
    ///
    /// 根持久化标记必须先于任何注册表副作用，Meta tombstone 必须继续使用
    /// `table_to_binary` 的 BON Atom 编码。不得把本方法扩展为物理目录删除或 DDL rollback
    /// 补偿；两者均超出 BUG-DDL-REMOVE-001 的冻结修复边界。
    #[inline]
    async fn remove_table(&self, table: Atom) -> IOResult<()> {
        // 必须先于注册表移除和 Meta tombstone 拒绝非法名称，保证 InvalidInput 无副作用。
        validate_removable_table_name(&table, ErrorKind::InvalidInput, "remove table")?;
        // SchemaCreate 与 remove_table 的 Meta tombstone 不能在同一根中混用；必须在移除注册表
        // 和设置持久化标志前拒绝，避免把中立建表节点转义为普通删表 owner。协议 CAS 在同一
        // childs_map 临界区内完成，不能与版本最终安装交错。
        {
            let childes_map = self.0.childs_map.lock();
            if childes_map
                .iter()
                .any(|(name, child)| Self::is_schema_create_child(name, child)) {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Remove table failed, reason: root transaction already contains schema-create actions",
                ));
            }
            self.select_ordinary_protocol("Remove table")
                .map_err(|error| Error::new(ErrorKind::InvalidInput, format!("{error:?}")))?;
        }

        // 删表会持久化 Meta tombstone，因此根事务必须参与 WAL。该标记必须先于注册表移除；
        // 否则 Meta 子事务虽会异步写数据文件，根 prepare 却会丢弃子日志，commit 返回成功后
        // 立即崩溃将无法恢复删除。这里只恢复删表耐久性，不改变 DDL 当前 rollback 非原子性。
        self.require_persistence();

        let mut tables = self.0.db_mgr.0.tables.write().await;

        //移除表
        if let Some(removed) = tables.remove(&table) {
            self.0
                .db_mgr
                .0
                .key_versions
                .remove_exact(&table, &removed.versions);
        }

        //删除表的元信息
        let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
        if let Some(meta_table) = tables.get(&meta_table_name) {
            //元信息表存在，则获取元信息表事务，并查询指定表的元信息
            let mut childes_map = self.0.childs_map.lock();
            let meta_table_tr = if let Some(table_tr) = childes_map.get(&meta_table_name) {
                //元信息表的子事务存在，则设置子事务为需要持久化
                table_tr.require_persistence();
                table_tr.clone()
            } else {
                //元信息表的子事务不存在，则创建元信息表的事务，因为需要移除表，所以初始化元信息表的子事务为持久化事务
                self.table_transaction(meta_table_name, meta_table, true, &mut *childes_map)
            };

            if let KVDBTransaction::MetaTabTr(tr) = &meta_table_tr {
                if let Err(e) = tr.delete(table_to_binary(&table)).await {
                    //删除表的元信息失败，则立即返回错误原因
                    return Err(Error::new(ErrorKind::Other,
                                          format!("Remove table failed, name: {:?}, reason: {:?}", table, e)));
                }
            } else {
                //不是元信息表事务，则立即返回错误原因
                return Err(Error::new(ErrorKind::Other,
                                      format!("Remove table failed, name: {:?}, reason: invalid meta table transaction", table)));
            }
        }

        Ok(())
    }

    /// 重放已提交但未最终确认的删表 WAL；只接受有效持久化名称并复用正常删除动作。
    #[inline]
    async fn repair_remove_table(&self, table: Atom) -> IOResult<()> {
        // 恢复入口接收持久化 WAL 中的名称；先用持久化数据错误语义校验，再复用正常移除。
        validate_removable_table_name(&table, ErrorKind::InvalidData, "repair table removal")?;
        self.remove_table(table).await
    }

    /// 异步查询多个表和键的值的结果集，可能会查询到旧值
    #[inline]
    async fn dirty_query(&self,
                         table_kv_list: Vec<TableKV>) -> Vec<Option<Binary>> {
        if table_kv_list.is_empty() {
            return Vec::new();
        }
        if let Err(error) = self.select_ordinary_protocol("Dirty query") {
            panic!("Dirty query failed before table access: {error:?}");
        }
        let mut result = Vec::new();

        for table_kv in table_kv_list {
            // 每项独立取得 registry read guard；从 get 得到的 table 借用使 guard 覆盖本项
            // 子事务查找/创建和表级 query。当前表级 query future 不产生异步 yield，但 Btree
            // redb fallback 会同步占用 worker，并延长该读临界区。本循环不构成跨表原子快照。
            if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_kv.table) {
                //指定名称的表存在，则获取表事务，并开始查询表的指定关键字的值
                let mut childes_map = self.0.childs_map.lock();
                let table_tr = if let Some(table_tr) = childes_map.get(&table_kv.table) {
                    //指定名称的表的子事务存在
                    table_tr.clone()
                } else {
                    //指定名称的表的子事务不存在，则创建指定表的事务，因为是查询操作，所以初始化指定表的子事务为非持久化事务
                    self.table_transaction(table_kv.table, table, false, &mut *childes_map)
                };

                match &table_tr {
                    KVDBTransaction::RootTr(_tr) => {
                        //忽略键值对数据库的根事务
                        ()
                    },
                    KVDBTransaction::MetaTabTr(tr) => {
                        //查询元信息表的指定关键字的值
                        let value = tr.dirty_query(table_kv.key).await;
                        result.push(value);
                    },
                    KVDBTransaction::MemOrdTabTr(tr) => {
                        //查询有序内存表的指定关键字的值
                        let value = tr.dirty_query(table_kv.key).await;
                        result.push(value);
                    },
                    KVDBTransaction::LogOrdTabTr(tr) => {
                        //查询有序日志表的指定关键字的值
                        let value = tr.dirty_query(table_kv.key).await;
                        result.push(value);
                    },
                    KVDBTransaction::LogWTabTr(tr) => {
                        //查询只写日志表的指定关键字的值
                        let value = tr.dirty_query(table_kv.key).await;
                        result.push(value);
                    },
                    KVDBTransaction::BtreeOrdTabTr(tr) => {
                        //查询有序B树表的指定关键字的值
                        let value = tr.dirty_query(table_kv.key).await;
                        result.push(value);
                    },
                }
            } else {
                //指定名称的表不存在
                result.push(None);
            }
        }

        result
    }

    /// 异步查询多个表和键的值的结果集
    #[inline]
    async fn query(&self,
                   table_kv_list: Vec<TableKV>) -> Vec<Option<Binary>> {
        if table_kv_list.is_empty() {
            return Vec::new();
        }
        if let Err(error) = self.select_ordinary_protocol("Query") {
            panic!("Query failed before table access: {error:?}");
        }
        let mut result = Vec::new();

        for table_kv in table_kv_list {
            // 与 dirty_query 相同，registry guard 只覆盖当前输入项，但持续到该项表级 query
            // 完成。不同表的首次子事务创建时点可以跨越其它根提交，因此结果不是全库统一快照。
            if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_kv.table) {
                //指定名称的表存在，则获取表事务，并开始查询表的指定关键字的值
                let mut childes_map = self.0.childs_map.lock();
                let table_tr = if let Some(table_tr) = childes_map.get(&table_kv.table) {
                    //指定名称的表的子事务存在
                    table_tr.clone()
                } else {
                    //指定名称的表的子事务不存在，则创建指定表的事务，因为是查询操作，所以初始化指定表的子事务为非持久化事务
                    self.table_transaction(table_kv.table, table, false, &mut *childes_map)
                };

                match &table_tr {
                    KVDBTransaction::RootTr(_tr) => {
                        //忽略键值对数据库的根事务
                        ()
                    },
                    KVDBTransaction::MetaTabTr(tr) => {
                        //查询元信息表的指定关键字的值
                        let value = tr.query(table_kv.key).await;
                        result.push(value);
                    },
                    KVDBTransaction::MemOrdTabTr(tr) => {
                        //查询有序内存表的指定关键字的值
                        let value = tr.query(table_kv.key).await;
                        result.push(value);
                    },
                    KVDBTransaction::LogOrdTabTr(tr) => {
                        //查询有序日志表的指定关键字的值
                        let value = tr.query(table_kv.key).await;
                        result.push(value);
                    },
                    KVDBTransaction::LogWTabTr(tr) => {
                        //查询只写日志表的指定关键字的值
                        let value = tr.query(table_kv.key).await;
                        result.push(value);
                    },
                    KVDBTransaction::BtreeOrdTabTr(tr) => {
                        //查询有序B树表的指定关键字的值
                        let value = tr.query(table_kv.key).await;
                        result.push(value);
                    },
                }
            } else {
                //指定名称的表不存在
                result.push(None);
            }
        }

        result
    }

    /// 异步插入或更新指定多个表和键的值，插入或更新可能会被覆蓋
    #[inline]
    async fn dirty_upsert(&self,
                          table_kv_list: Vec<TableKV>) -> Result<(), KVTableTrError> {
        if table_kv_list.is_empty() {
            return Ok(());
        }
        self.select_ordinary_protocol("Dirty upsert")?;
        for table_kv in table_kv_list {
            if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_kv.table) {
                // 每项都先解析表并在首次触达时安装唯一子事务；childs_map 保证同表复用，
                // childs 保留跨表首次触达顺序，后续 prepare/commit 必须沿该顺序推进。
                let mut childes_map = self.0.childs_map.lock();
                let table_tr = if let Some(table_tr) = childes_map.get(&table_kv.table) {
                    //指定名称的表的子事务存在
                    if table.is_persistent() {
                        //指定表需要持久化，且因为插入或更新操作，所以设置子事务为需要持久化
                        table_tr.require_persistence();
                    }
                    table_tr.clone()
                } else {
                    //指定名称的表的子事务不存在，则创建指定表的事务
                    if table.is_persistent() {
                        //指定表需要持久化，且因为插入或更新操作，所以初始化指定表的子事务为持久化事务
                        self.table_transaction(table_kv.table, table, true, &mut *childes_map)
                    } else {
                        //指定表不需要持久化，所以即使插入或更新操作，也初始化指定表的子事务为非持久化事务
                        self.table_transaction(table_kv.table, table, false, &mut *childes_map)
                    }
                };

                if table_tr.is_require_persistence() {
                    //如果任意写操作对应的子事务需要持久化，则根事务也需要持久化
                    self.0.persistence.store(true, Ordering::Relaxed);
                }

                // 子事务创建和持久化提升有意发生在 value 判定之前。因此 None 不是删除，也不
                // 登记表动作，但仍可能改变事务树拓扑；调用方不能把非空的 None 批次视为空批次。
                match &table_tr {
                    KVDBTransaction::RootTr(_tr) => {
                        //忽略键值对数据库的根事务
                        ()
                    },
                    KVDBTransaction::MetaTabTr(tr) => {
                        //插入或更新元信息表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.dirty_upsert(table_kv.key, value).await {
                                //插入或更新元信息表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                    KVDBTransaction::MemOrdTabTr(tr) => {
                        //插入或更新有序内存表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.dirty_upsert(table_kv.key, value).await {
                                //插入或更新有序内存表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                    KVDBTransaction::LogOrdTabTr(tr) => {
                        //插入或更新有序日志表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.dirty_upsert(table_kv.key, value).await {
                                //插入或更新有序日志表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                    KVDBTransaction::LogWTabTr(tr) => {
                        //插入或更新只写日志表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.dirty_upsert(table_kv.key, value).await {
                                //插入或更新只写日志表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                    KVDBTransaction::BtreeOrdTabTr(tr) => {
                        //插入或更新有序B树表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.dirty_upsert(table_kv.key, value).await {
                                //插入或更新有序B树表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                }
            }
        }

        Ok(())
    }

    /// 异步插入或更新指定多个表和键的值
    #[inline]
    async fn upsert(&self,
                    table_kv_list: Vec<TableKV>) -> Result<(), KVTableTrError> {
        if table_kv_list.is_empty() {
            return Ok(());
        }
        self.select_ordinary_protocol("Upsert")?;
        for table_kv in table_kv_list {
            if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_kv.table) {
                // 每项都先解析表并在首次触达时安装唯一子事务；childs_map 保证同表复用，
                // childs 保留跨表首次触达顺序，后续 prepare/commit 必须沿该顺序推进。
                let mut childes_map = self.0.childs_map.lock();
                let table_tr = if let Some(table_tr) = childes_map.get(&table_kv.table) {
                    //指定名称的表的子事务存在
                    if table.is_persistent() {
                        //指定表需要持久化，且因为插入或更新操作，所以设置子事务为需要持久化
                        table_tr.require_persistence();
                    }
                    table_tr.clone()
                } else {
                    //指定名称的表的子事务不存在，则创建指定表的事务
                    if table.is_persistent() {
                        //指定表需要持久化，且因为插入或更新操作，所以初始化指定表的子事务为持久化事务
                        self.table_transaction(table_kv.table, table, true, &mut *childes_map)
                    } else {
                        //指定表不需要持久化，所以即使插入或更新操作，也初始化指定表的子事务为非持久化事务
                        self.table_transaction(table_kv.table, table, false, &mut *childes_map)
                    }
                };

                if table_tr.is_require_persistence() {
                    //如果任意写操作对应的子事务需要持久化，则根事务也需要持久化
                    self.0.persistence.store(true, Ordering::Relaxed);
                }

                // 子事务创建和持久化提升有意发生在 value 判定之前。因此 None 不是删除，也不
                // 登记表动作，但仍可能改变事务树拓扑；调用方不能把非空的 None 批次视为空批次。
                match &table_tr {
                    KVDBTransaction::RootTr(_tr) => {
                        //忽略键值对数据库的根事务
                        ()
                    },
                    KVDBTransaction::MetaTabTr(tr) => {
                        //插入或更新元信息表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.upsert(table_kv.key, value).await {
                                //插入或更新元信息表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                    KVDBTransaction::MemOrdTabTr(tr) => {
                        //插入或更新有序内存表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.upsert(table_kv.key, value).await {
                                //插入或更新有序内存表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                    KVDBTransaction::LogOrdTabTr(tr) => {
                        //插入或更新有序日志表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.upsert(table_kv.key, value).await {
                                //插入或更新有序日志表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                    KVDBTransaction::LogWTabTr(tr) => {
                        //插入或更新只写日志表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.upsert(table_kv.key, value).await {
                                //插入或更新只写日志表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                    KVDBTransaction::BtreeOrdTabTr(tr) => {
                        //插入或更新有序B树表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.upsert(table_kv.key, value).await {
                                //插入或更新有序B树表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                }
            } else {
                //指定名称的表不存在
                error!("Upsert table failed, table: {:?}, reason: table not exist",
                    &table_kv.table.as_str());
                return Err(KVTableTrError::new_transaction_error(ErrorLevel::Fatal,
                                                                 format!("Upsert table failed, table: {:?}, reason: table not exist",
                                                                     &table_kv.table.as_str())));
            }
        }

        Ok(())
    }

    /// 按输入顺序登记 dirty tombstone，并返回与输入同序等长的逐表结果。
    ///
    /// `TableKV::value` 有意忽略；缺表只追加 `None`，不会创建 child。存在表的首次触达顺序
    /// 决定唯一 child 和后续 2PC 顺序，同 Key 后续动作只覆盖最终动作。逐表返回与冲突差异见
    /// `docs/ROOT_DELETE_CONTRACT.md#root-delete-contract-index`。
    #[inline]
    async fn dirty_delete(&self,
                          table_kv_list: Vec<TableKV>) -> Result<Vec<Option<Binary>>, KVTableTrError> {
        if table_kv_list.is_empty() {
            return Ok(Vec::new());
        }
        self.select_ordinary_protocol("Dirty delete")?;
        let mut result = Vec::new();

        for table_kv in table_kv_list {
            if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_kv.table) {
                //指定名称的表存在，则获取表事务，并开始删除表的指定关键字的值
                let mut childes_map = self.0.childs_map.lock();
                let table_tr = if let Some(table_tr) = childes_map.get(&table_kv.table) {
                    //指定名称的表的子事务存在
                    if table.is_persistent() {
                        // 持久表的删除动作需要进入根 WAL；复用只读子事务时必须先提升该标志。
                        table_tr.require_persistence();
                    }
                    table_tr.clone()
                } else {
                    // 首次触达存在表时创建唯一子事务；table_transaction 同时登记 map 和有序
                    // child 列表，因此该分支的执行顺序就是后续 prepare/commit 顺序。
                    if table.is_persistent() {
                        // 持久表的删除动作必须初始化为需要根 WAL 的子事务。
                        self.table_transaction(table_kv.table, table, true, &mut *childes_map)
                    } else {
                        // 非持久表删除不单独要求根 WAL。
                        self.table_transaction(table_kv.table, table, false, &mut *childes_map)
                    }
                };

                if table_tr.is_require_persistence() {
                    //如果任意写操作对应的子事务需要持久化，则根事务也需要持久化
                    self.0.persistence.store(true, Ordering::Relaxed);
                }

                match &table_tr {
                    KVDBTransaction::RootTr(_tr) => {
                        //忽略键值对数据库的根事务
                        ()
                    },
                    KVDBTransaction::MetaTabTr(tr) => {
                        //删除元信息表的指定关键字的值
                        match tr.dirty_delete(table_kv.key).await {
                            Err(e) => {
                                //删除元信息表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除元信息表的指定关键字的值成功
                                result.push(value);
                            },
                        }
                    },
                    KVDBTransaction::MemOrdTabTr(tr) => {
                        //删除有序内存表的指定关键字的值
                        match tr.dirty_delete(table_kv.key).await {
                            Err(e) => {
                                //删除有序内存表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除有序内存表的指定关键字的值成功
                                result.push(value);
                            },
                        }
                    },
                    KVDBTransaction::LogOrdTabTr(tr) => {
                        //删除有序日志表的指定关键字的值
                        match tr.dirty_delete(table_kv.key).await {
                            Err(e) => {
                                //删除有序日志表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除有序日志表的指定关键字的值成功
                                result.push(value);
                            },
                        }
                    },
                    KVDBTransaction::LogWTabTr(tr) => {
                        //删除只写日志表的指定关键字的值
                        match tr.dirty_delete(table_kv.key).await {
                            Err(e) => {
                                //删除只写日志表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除只写日志表的指定关键字的值成功
                                result.push(value);
                            },
                        }
                    },
                    KVDBTransaction::BtreeOrdTabTr(tr) => {
                        //删除有序B树表的指定关键字的值
                        match tr.dirty_delete(table_kv.key).await {
                            Err(e) => {
                                //删除有序B树表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除有序B树表的指定关键字的值成功
                                result.push(value);
                            },
                        }
                    },
                }
            } else {
                // 缺表只占据当前输入对应的返回槽位；不创建 child，也不改变后续项顺序。
                result.push(None);
            }
        }

        Ok(result)
    }

    /// 按输入顺序登记普通 tombstone，并返回与输入同序等长的逐表结果。
    ///
    /// `TableKV::value` 有意忽略；缺表只追加 `None`，不会创建 child。存在表的首次触达顺序
    /// 决定唯一 child 和后续 2PC 顺序，同 Key 后续动作只覆盖最终动作。逐表返回、WAL 与
    /// repair 边界见 `docs/ROOT_DELETE_CONTRACT.md#root-delete-contract-index`。
    #[inline]
    async fn delete(&self,
                    table_kv_list: Vec<TableKV>) -> Result<Vec<Option<Binary>>, KVTableTrError> {
        if table_kv_list.is_empty() {
            return Ok(Vec::new());
        }
        self.select_ordinary_protocol("Delete")?;
        let mut result = Vec::new();

        for table_kv in table_kv_list {
            if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_kv.table) {
                //指定名称的表存在，则获取表事务，并开始删除表的指定关键字的值
                let mut childes_map = self.0.childs_map.lock();
                let table_tr = if let Some(table_tr) = childes_map.get(&table_kv.table) {
                    //指定名称的表的子事务存在
                    if table.is_persistent() {
                        // 持久表的删除动作需要进入根 WAL；复用只读子事务时必须先提升该标志。
                        table_tr.require_persistence();
                    }
                    table_tr.clone()
                } else {
                    // 首次触达存在表时创建唯一子事务；table_transaction 同时登记 map 和有序
                    // child 列表，因此该分支的执行顺序就是后续 prepare/commit 顺序。
                    if table.is_persistent() {
                        // 持久表的删除动作必须初始化为需要根 WAL 的子事务。
                        self.table_transaction(table_kv.table, table, true, &mut *childes_map)
                    } else {
                        // 非持久表删除不单独要求根 WAL。
                        self.table_transaction(table_kv.table, table, false, &mut *childes_map)
                    }
                };

                if table_tr.is_require_persistence() {
                    //如果任意写操作对应的子事务需要持久化，则根事务也需要持久化
                    self.0.persistence.store(true, Ordering::Relaxed);
                }

                match &table_tr {
                    KVDBTransaction::RootTr(_tr) => {
                        //忽略键值对数据库的根事务
                        ()
                    },
                    KVDBTransaction::MetaTabTr(tr) => {
                        //删除元信息表的指定关键字的值
                        match tr.delete(table_kv.key).await {
                            Err(e) => {
                                //删除元信息表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除元信息表的指定关键字的值成功
                                result.push(value);
                            },
                        }
                    },
                    KVDBTransaction::MemOrdTabTr(tr) => {
                        //删除有序内存表的指定关键字的值
                        match tr.delete(table_kv.key).await {
                            Err(e) => {
                                //删除有序内存表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除有序内存表的指定关键字的值成功
                                result.push(value);
                            },
                        }
                    },
                    KVDBTransaction::LogOrdTabTr(tr) => {
                        //删除有序日志表的指定关键字的值
                        match tr.delete(table_kv.key).await {
                            Err(e) => {
                                //删除有序日志表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除有序日志表的指定关键字的值成功
                                result.push(value);
                            },
                        }
                    },
                    KVDBTransaction::LogWTabTr(tr) => {
                        //删除只写日志表的指定关键字的值
                        match tr.delete(table_kv.key).await {
                            Err(e) => {
                                //删除只写日志表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除只写日志表的指定关键字的值成功
                                result.push(value);
                            },
                        }
                    },
                    KVDBTransaction::BtreeOrdTabTr(tr) => {
                        //删除有序B树表的指定关键字的值
                        match tr.delete(table_kv.key).await {
                            Err(e) => {
                                //删除有序B树表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除有序B树表的指定关键字的值成功
                                result.push(value);
                            },
                        }
                    },
                }
            } else {
                // 缺表只占据当前输入对应的返回槽位；不创建 child，也不改变后续项顺序。
                result.push(None);
            }
        }

        Ok(result)
    }

    /// 获取从指定表和关键字开始，从前向后或从后向前的关键字异步流
    #[inline]
    async fn keys<'a>(&self,
                      table_name: Atom,
                      key: Option<Binary>,
                      descending: bool) -> Option<BoxStream<'a, Binary>> {
        let table = self.0.db_mgr.0.tables.read().await.get(&table_name).cloned();
        if let Some(table) = table {
            // 只有根已经选择 Ordinary 才复用同表普通子事务。Unselected 下可能已存在中立
            // SchemaCreate Meta owner，Versioned 下则存在版本子事务；两者都必须使用 detached
            // 快照，保证纯 iterator 不加入 2PC、不选择协议，也不误用其它 prepare mode。
            let ordinary_table_tr = {
                let childes_map = self.0.childs_map.lock();
                if self.protocol() == RootTransactionProtocol::Ordinary {
                    childes_map.get(&table_name).cloned()
                } else {
                    None
                }
            };
            let table_tr = ordinary_table_tr.unwrap_or_else(|| {
                self.detached_iterator_table_transaction(&table)
            });

            match &table_tr {
                KVDBTransaction::RootTr(_tr) => {
                    //忽略键值对数据库的根事务
                    None
                },
                KVDBTransaction::MetaTabTr(tr) => {
                    //获取元信息表的关键字的异步流
                    Some(tr.keys(key, descending))
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    //获取有序内存表的关键字的异步流
                    Some(tr.keys(key, descending))
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    //获取有序日志表的关键字的异步流
                    Some(tr.keys(key, descending))
                },
                KVDBTransaction::LogWTabTr(tr) => {
                    //获取只写日志表的关键字的异步流
                    Some(tr.keys(key, descending))
                },
                KVDBTransaction::BtreeOrdTabTr(tr) => {
                    //获取有序B树表的关键字的异步流
                    Some(tr.keys(key, descending))
                },
            }
        } else {
            //指定名称的表不存在
            None
        }
    }

    /// 获取从指定表和关键字开始，从前向后或从后向前的键值对异步流
    #[inline]
    async fn values<'a>(&self,
                        table_name: Atom,
                        key: Option<Binary>,
                        descending: bool) -> Option<BoxStream<'a, (Binary, Binary)>> {
        let table = self.0.db_mgr.0.tables.read().await.get(&table_name).cloned();
        if let Some(table) = table {
            // 与 keys 使用相同的树隔离：只复用 Ordinary 子事务；中立 schema 和版本树都使用
            // 独立快照事务，不改变根 2PC 子节点数量或协议模式。
            let ordinary_table_tr = {
                let childes_map = self.0.childs_map.lock();
                if self.protocol() == RootTransactionProtocol::Ordinary {
                    childes_map.get(&table_name).cloned()
                } else {
                    None
                }
            };
            let table_tr = ordinary_table_tr.unwrap_or_else(|| {
                self.detached_iterator_table_transaction(&table)
            });

            match &table_tr {
                KVDBTransaction::RootTr(_tr) => {
                    //忽略键值对数据库的根事务
                    None
                },
                KVDBTransaction::MetaTabTr(tr) => {
                    //获取元信息表的键值对异步流
                    Some(tr.values(key, descending))
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    //获取有序内存表的键值对异步流
                    Some(tr.values(key, descending))
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    //获取有序日志表的键值对异步流
                    Some(tr.values(key, descending))
                },
                KVDBTransaction::LogWTabTr(tr) => {
                    //获取只写日志表的键值对异步流
                    Some(tr.values(key, descending))
                },
                KVDBTransaction::BtreeOrdTabTr(tr) => {
                    //获取有序B树表的键值对异步流
                    Some(tr.values(key, descending))
                },
            }
        } else {
            //指定名称的表不存在
            None
        }
    }

    /// 分派当前立即成功的 Key 锁兼容钩子；完整公开契约见 `ROOT-KEY-HOOK-001`。
    #[inline]
    async fn lock_key(&self,
                      table_name: Atom,
                      key: Binary) -> Result<(), KVTableTrError> {
        self.select_ordinary_protocol("Lock table key")?;
        // Ordinary 在 registry await 之前已经选定；取消该 await 会保留协议选择，但尚未创建 child。
        // 当前 if-let 临时值把 registry 读 guard 保持到分支结束，childs_map guard 又保持到表 hook
        // await 完成。五类内置 hook 首次 poll 都立即返回；未来若引入真正等待，必须先缩短这两个
        // guard 的临界区，不能直接把阻塞/重入语义塞进现有分派结构。
        if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_name) {
            //指定名称的表存在，则获取表事务，并开始锁住指定表的指定关键字
            let mut childes_map = self.0.childs_map.lock();
            let table_tr = if let Some(table_tr) = childes_map.get(&table_name) {
                //指定名称的表的子事务存在
                table_tr.clone()
            } else {
                // 首次 hook 创建非持久化 managed owner，同时固定数据根和版本 revision 租约；
                // 后续普通写复用该 owner 并按需提升 persistence。
                self.table_transaction(table_name, table, false, &mut *childes_map)
            };

            match &table_tr {
                KVDBTransaction::RootTr(_tr) => {
                    //忽略键值对数据库的根事务
                    Ok(())
                },
                KVDBTransaction::MetaTabTr(tr) => {
                    //锁住元信息表的指定关键字
                    tr.lock_key(key).await
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    //锁住有序内存表的指定关键字
                    tr.lock_key(key).await
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    //锁住有序日志表的指定关键字
                    tr.lock_key(key).await
                },
                KVDBTransaction::LogWTabTr(tr) => {
                    //锁住只写日志表的指定关键字
                    tr.lock_key(key).await
                },
                KVDBTransaction::BtreeOrdTabTr(tr) => {
                    //锁住有序B树表的指定关键字
                    tr.lock_key(key).await
                },
            }
        } else {
            //指定名称的表不存在
            Ok(())
        }
    }

    /// 分派当前立即成功的 Key 解锁兼容钩子；状态和 guard 边界与 lock_key 相同。
    #[inline]
    async fn unlock_key(&self,
                        table_name: Atom,
                        key: Binary) -> Result<(), KVTableTrError> {
        self.select_ordinary_protocol("Unlock table key")?;
        // 即使缺表，Ordinary 也已在 registry await 前选定。registry/childs_map guard 当前跨
        // hook await；依赖的是五类内置 hook 立即 ready 的实现事实，而不是未来扩展许可。
        if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_name) {
            //指定名称的表存在，则获取表事务，并开始解锁指定表的指定关键字
            let mut childes_map = self.0.childs_map.lock();
            let table_tr = if let Some(table_tr) = childes_map.get(&table_name) {
                //指定名称的表的子事务存在
                table_tr.clone()
            } else {
                // unlock-before-lock 同样创建非持久化 managed owner并固定快照/revision；
                // 它不创建、释放或证明任何真实 Key 锁所有权。
                self.table_transaction(table_name, table, false, &mut *childes_map)
            };

            match &table_tr {
                KVDBTransaction::RootTr(_tr) => {
                    //忽略键值对数据库的根事务
                    Ok(())
                },
                KVDBTransaction::MetaTabTr(tr) => {
                    //解锁元信息表的指定关键字
                    tr.unlock_key(key).await
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    //解锁有序内存表的指定关键字
                    tr.unlock_key(key).await
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    //解锁有序日志表的指定关键字
                    tr.unlock_key(key).await
                },
                KVDBTransaction::LogWTabTr(tr) => {
                    //解锁只写日志表的指定关键字
                    tr.unlock_key(key).await
                },
                KVDBTransaction::BtreeOrdTabTr(tr) => {
                    //解锁有序B树表的指定关键字
                    tr.unlock_key(key).await
                },
            }
        } else {
            //指定名称的表不存在
            Ok(())
        }
    }

    /// 使用外部读缓存版本和最终写集合，一次性装配并预提交独立版本事务。
    async fn prepare_with_version(&self,
                                  read_set: Vec<TableKeyVersion>,
                                  write_set: Vec<TableKV>)
        -> Result<Vec<u8>, KVTableTrError> {
        if !self.is_writable() {
            return Err(KVTableTrError::new_transaction_error(
                ErrorLevel::Normal,
                "Prepare with version failed, reason: root transaction is read-only"));
        }
        validate_prepare_with_version_inputs(&read_set, &write_set)?;

        // 版本协议只能选择 Unselected 根；容器允许为空，或只包含公开 create 建立的唯一
        // SchemaCreate Meta 节点。纯 iterator 不注册，因此不影响选择。这里先做无共享副作用
        // 快检，避免协议误用仍去租用表版本快照；最终安装还会在固定锁序下复检并 CAS。
        {
            let childes_map = self.0.childs_map.lock();
            let childes = self.0.childs.lock();
            if childes_map.len() != childes.len() {
                return Err(KVTableTrError::new_transaction_error(
                    ErrorLevel::Fatal,
                    format!("Prepare with version failed, reason: inconsistent root child containers, map_len: {}, list_len: {}",
                            childes_map.len(), childes.len())));
            }
            if self.protocol() != RootTransactionProtocol::Unselected
                || !Self::contains_only_schema_create_child(&childes_map, childes.len())
                || self.0.version_context.lock().is_some() {
                return Err(KVTableTrError::new_transaction_error(
                    ErrorLevel::Normal,
                    "Prepare with version failed, reason: root transaction already selected a business protocol or contains non-schema 2PC children"));
            }
        }

        // 先在事务私有内存中规范化输入，不触碰表、版本或 manager。分组顺序取两个输入 Vec
        // 第一次出现表的顺序；同一 Table/Key 跨 read/write 重叠时保留 expected 读版本，但最终
        // 动作原子替换为 Write。集合内部重复项已在上方拒绝，避免 HashMap 覆盖掩盖协议错误。
        let mut groups: Vec<VersionPrepareGroup<C, Log>> = Vec::new();
        let mut group_indices: XHashMap<Atom, usize> = XHashMap::default();
        let mut expected_writes = XHashMap::default();
        for item in read_set {
            let index = if let Some(index) = group_indices.get(&item.table) {
                *index
            } else {
                let index = groups.len();
                group_indices.insert(item.table.clone(), index);
                groups.push(VersionPrepareGroup {
                    name: item.table.clone(),
                    registered: None,
                    expected: XHashMap::default(),
                    actions: XHashMap::default(),
                    read_keys: Vec::new(),
                    has_write: false,
                });
                index
            };
            let group = &mut groups[index];
            group.read_keys.push(item.key.clone());
            group.expected.insert(item.key.clone(), item.version);
            group.actions.insert(item.key, crate::KVActionLog::Read);
        }
        for item in write_set {
            let expected_kind = if item.value.is_some() {
                ExpectedVersionKind::Upsert
            } else {
                ExpectedVersionKind::Delete
            };
            expected_writes.insert(TableKey {
                table: item.table.clone(),
                key: item.key.clone(),
            }, expected_kind);
            let index = if let Some(index) = group_indices.get(&item.table) {
                *index
            } else {
                let index = groups.len();
                group_indices.insert(item.table.clone(), index);
                groups.push(VersionPrepareGroup {
                    name: item.table.clone(),
                    registered: None,
                    expected: XHashMap::default(),
                    actions: XHashMap::default(),
                    read_keys: Vec::new(),
                    has_write: false,
                });
                index
            };
            let group = &mut groups[index];
            group.has_write = true;
            group.actions.insert(item.key, crate::KVActionLog::Write(item.value));
        }

        // 这里只克隆注册项并立即释放 registry guard；后续构造、publication await 和 prepare
        // 都不得持有数据库表锁。现存 LogWrite 的 delete 是静态能力错误，必须先于 UID 拒绝。
        {
            let tables = self.0.db_mgr.0.tables.read().await;
            for group in &mut groups {
                group.registered = tables.get(&group.name).cloned();
                if let Some(RegisteredTable {
                    table: KVDBTable::LogWTab(_),
                    ..
                }) = group.registered.as_ref() {
                    if group.actions.values().any(|action| {
                        matches!(action, crate::KVActionLog::Write(None))
                    }) {
                        return Err(KVTableTrError::new_transaction_error(
                            ErrorLevel::Normal,
                            format!("Prepare with version failed, table: {:?}, reason: LogWrite does not support delete",
                                    group.name.as_str())));
                    }
                }
            }
        }

        // 所有子表共享一个只收集“本事务最终写”的回执 owner。它不读取全局最新版本，且只有
        // commit_with_version 会在整棵树提交成功后 take；普通 commit 混用虽被协议禁止，但仍不
        // 能跳过底层版本发布。
        let receipt = VersionReceipt::new();
        let mut identities = Vec::with_capacity(groups.len());
        let mut children = Vec::with_capacity(groups.len());
        let mut require_persistence = false;
        for group in groups {
            let identity_versions = group
                .registered
                .as_ref()
                .map(|registered| registered.versions.clone());
            identities.push(RootVersionTableIdentity {
                name: group.name.clone(),
                versions: identity_versions,
                read_keys: group.read_keys,
                has_write: group.has_write,
            });

            if let Some(registered) = group.registered {
                let is_persistent = group.has_write && registered.is_persistent();
                if is_persistent {
                    require_persistence = true;
                }
                let table_tr = self.build_versioned_table_transaction(&registered,
                                                                       is_persistent,
                                                                       group.expected,
                                                                       receipt.clone(),
                                                                       group.actions);
                children.push((group.name, table_tr));
            }
        }
        let version_context = RootVersionContext {
            tables: identities,
            receipt,
            expected_writes: Arc::new(expected_writes),
        };

        // 子事务构造可能锁各表数据根并租用版本快照，所以必须发生在根锁之外。安装阶段只做
        // HashMap/VecDeque/Option 的内存操作，固定锁序为 childs_map -> childs ->
        // version_context，锁内没有 await、I/O、publication/table 锁或回调。协议 CAS、全部
        // 版本节点和 context 在释放 childs_map 前同时可见，后续普通动作只能 fail-fast。
        {
            let mut childes_map = self.0.childs_map.lock();
            let mut childes = self.0.childs.lock();
            let mut context = self.0.version_context.lock();
            if childes_map.len() != childes.len() {
                return Err(KVTableTrError::new_transaction_error(
                    ErrorLevel::Fatal,
                    format!("Prepare with version failed, reason: inconsistent root child containers during install, map_len: {}, list_len: {}",
                            childes_map.len(), childes.len())));
            }
            if !Self::contains_only_schema_create_child(&childes_map, childes.len())
                || context.is_some() {
                return Err(KVTableTrError::new_transaction_error(
                    ErrorLevel::Normal,
                    "Prepare with version failed, reason: root transaction protocol changed during version child assembly"));
            }
            if let Some((name, _)) = children
                .iter()
                .find(|(name, _)| childes_map.contains_key(name)) {
                return Err(KVTableTrError::new_transaction_error(
                    ErrorLevel::Normal,
                    format!("Prepare with version failed, table: {:?}, reason: version input collides with existing schema transaction owner",
                            name.as_str())));
            }
            self.select_versioned_protocol()?;
            for (name, table_tr) in children {
                childes_map.insert(name, table_tr.clone());
                childes.join(table_tr);
            }
            *context = Some(version_context);
            if require_persistence {
                self.require_persistence();
            }
        }

        self
            .0
            .db_mgr
            .0
            .tr_mgr
            .start(KVDBTransaction::RootTr(self.clone()))
            .await?;
        let prepare_output = self
            .0
            .db_mgr
            .0
            .tr_mgr
            .prepare_all_conflicts(KVDBTransaction::RootTr(self.clone()))
            .await?;

        if self.is_require_persistence() {
            match prepare_output {
                Some(output) if output.len() > 16 => Ok(output),
                _ => Ok(Vec::new()),
            }
        } else {
            Ok(Vec::new())
        }
    }

    /// 异步预提交本次事务对键值对数据库的所有修改，成功返回预提交的输出
    #[inline]
    async fn prepare_modified(&self) -> Result<Vec<u8>, KVTableTrError> {
        self.select_ordinary_protocol("Prepare modified")?;
        if self.get_status() != Transaction2PcStatus::Rollbacked {
            //本次事务的当前状态只要不为回滚成功，则先初始化键值对数据库的根事务
            if let Err(e) = self
                .0
                .db_mgr
                .0
                .tr_mgr
                .start(KVDBTransaction::RootTr(self.clone()))
                .await {
                //初始化键值对数据库的根事务失败，则立即返回错误原因
                return Err(e);
            }
        }

        //预提交键值对数据库的根事务
        match self
            .0
            .db_mgr
            .0
            .tr_mgr
            .prepare(KVDBTransaction::RootTr(self.clone()))
            .await {
            Err(e) => {
                //预提交键值对数据库的根事务失败，则立即返回错误原因
                Err(e)
            },
            Ok(prepare_output) => {
                //预提交键值对数据库的根事务成功
                if self.is_require_persistence() {
                    //本次键值对数据库的根事务，需要持久化
                    if let Some(output) = prepare_output {
                        //键值对数据库的预提交事务，有返回预提交输出
                        if output.len() > 16 {
                            //有效的预提交输出，根事务需要持久化，且至少有一个子事务需要持久化
                            Ok(output)
                        } else {
                            //无效的预提交输出，根事务需要持久化，但所有子事务不需要持久化
                            Ok(vec![])
                        }
                    } else {
                        //预提交键值对数据库的子事务，没有返回预提交输出
                        Ok(vec![])
                    }
                } else {
                    //本次键值对数据库的根事务，不需要持久化
                    Ok(vec![])
                }
            },
        }
    }

    /// 异步预提交本次事务对键值对数据库的所有修改，成功返回预提交的输出，失败返回预提交冲突的首个表名和关键字
    #[inline]
    async fn prepare_modified_conflicts(&self) -> Result<Vec<u8>, KVTableTrError> {
        self.select_ordinary_protocol("Prepare modified conflicts")?;
        if self.get_status() != Transaction2PcStatus::Rollbacked {
            //本次事务的当前状态只要不为回滚成功，则先初始化键值对数据库的根事务
            if let Err(e) = self
                .0
                .db_mgr
                .0
                .tr_mgr
                .start(KVDBTransaction::RootTr(self.clone()))
                .await {
                //初始化键值对数据库的根事务失败，则立即返回错误原因
                return Err(e);
            }
        }

        //预提交键值对数据库的根事务
        match self
            .0
            .db_mgr
            .0
            .tr_mgr
            .prepare_conflicts(KVDBTransaction::RootTr(self.clone()))
            .await {
            Err(e) => {
                //预提交键值对数据库的根事务失败，则立即返回错误原因
                Err(e)
            },
            Ok(prepare_output) => {
                //预提交键值对数据库的根事务成功
                if self.is_require_persistence() {
                    //本次键值对数据库的根事务，需要持久化
                    if let Some(output) = prepare_output {
                        //键值对数据库的预提交事务，有返回预提交输出
                        if output.len() > 16 {
                            //有效的预提交输出，根事务需要持久化，且至少有一个子事务需要持久化
                            Ok(output)
                        } else {
                            //无效的预提交输出，根事务需要持久化，但所有子事务不需要持久化
                            Ok(vec![])
                        }
                    } else {
                        //预提交键值对数据库的子事务，没有返回预提交输出
                        Ok(vec![])
                    }
                } else {
                    //本次键值对数据库的根事务，不需要持久化
                    Ok(vec![])
                }
            },
        }
    }

    /// 异步提交本次事务对键值对数据库的所有修改
    #[inline]
    async fn commit_modified(&self, prepare_output: Vec<u8>) -> Result<(), KVTableTrError> {
        if self.protocol() != RootTransactionProtocol::Ordinary {
            return Err(KVTableTrError::new_transaction_error(
                ErrorLevel::Normal,
                "Commit modified failed, reason: root transaction did not select ordinary protocol"));
        }
        self.commit_core(prepare_output).await
    }

    /// 版本提交与普通提交共享唯一 WAL/事务管理器闭环，差异只在成功后的回执所有权。
    async fn commit_with_version(&self,
                                 prepare_output: Vec<u8>)
        -> Result<Vec<TableKeyVersion>, KVTableTrError> {
        if self.protocol() != RootTransactionProtocol::Versioned {
            return Err(KVTableTrError::new_transaction_error(
                ErrorLevel::Normal,
                "Commit with version failed, reason: root transaction did not select version protocol"));
        }
        let context = self
            .0
            .version_context
            .lock()
            .as_ref()
            .expect("Commit with version failed, reason: transaction was not prepared by version protocol")
            .clone();
        let receipt = context.receipt.clone();
        match self.commit_core(prepare_output).await {
            Ok(()) => {
                let versions = receipt.take();
                self.validate_version_receipt(&context.expected_writes, &versions)?;
                Ok(versions)
            },
            Err(error) => {
                receipt.clear();
                Err(error)
            },
        }
    }

    /// 在整棵树已经提交后验证版本回执与本次最终写集合严格一一对应。
    ///
    /// 该检查不读取全局版本缓存，避免把后续事务的新版本误当成本事务回执。任何不一致都
    /// 说明整棵树已进入并完成不可 rollback 的 commit，却没有形成确定回执；无论该事务是否
    /// 实际写根 WAL，都只能返回 Fatal，不能 rollback。
    fn validate_version_receipt(&self,
                                expected: &XHashMap<TableKey, ExpectedVersionKind>,
                                versions: &[TableKeyVersion])
        -> Result<(), KVTableTrError> {
        let transaction_uid = self.get_transaction_uid().ok_or_else(|| {
            KVTableTrError::new_transaction_error(
                ErrorLevel::Fatal,
                "Validate version receipt failed, reason: committed root transaction has no transaction uid")
        })?;
        if versions.len() != expected.len() {
            return Err(KVTableTrError::new_transaction_error(
                ErrorLevel::Fatal,
                format!("Validate version receipt failed, transaction_uid: {:?}, expected_count: {}, actual_count: {}, reason: committed receipt count does not match final write set",
                        transaction_uid, expected.len(), versions.len())));
        }

        let mut remaining = expected.clone();
        for item in versions {
            let table_key = TableKey {
                table: item.table.clone(),
                key: item.key.clone(),
            };
            let Some(expected_kind) = remaining.remove(&table_key) else {
                return Err(KVTableTrError::new_transaction_error(
                    ErrorLevel::Fatal,
                    format!("Validate version receipt failed, transaction_uid: {:?}, table: {:?}, key: {:?}, reason: duplicate or unexpected receipt item",
                            transaction_uid, item.table.as_str(), item.key)));
            };
            let valid = match (&item.version, expected_kind) {
                (Version::Upsert(uid), ExpectedVersionKind::Upsert) => uid == &transaction_uid,
                (Version::Delete(uid), ExpectedVersionKind::Delete) => uid == &transaction_uid,
                _ => false,
            };
            if !valid {
                return Err(KVTableTrError::new_transaction_error(
                    ErrorLevel::Fatal,
                    format!("Validate version receipt failed, transaction_uid: {:?}, table: {:?}, key: {:?}, expected_kind: {:?}, actual_version: {:?}, reason: receipt action or transaction uid mismatch",
                            transaction_uid,
                            item.table.as_str(),
                            item.key,
                            expected_kind,
                            item.version)));
            }
        }

        if !remaining.is_empty() {
            return Err(KVTableTrError::new_transaction_error(
                ErrorLevel::Fatal,
                format!("Validate version receipt failed, transaction_uid: {:?}, missing_count: {}, reason: final write set contains unacknowledged entries",
                        transaction_uid, remaining.len())));
        }
        Ok(())
    }

    async fn commit_core(&self, prepare_output: Vec<u8>) -> Result<(), KVTableTrError> {
        // 为本次事务创建“持久化成功信号”聚合器。计数只包含需要持久化的子表；
        // 根事务自身和非持久化子表不参与。子表数据文件失败时不会调用该回调，根 WAL
        // 因计数未归零而保持未确认。可写事务只有读动作时 prepare_output 和计数都可以为 0：
        // 空输出只跳过 WAL I/O，仍必须由上游 manager 提交整棵 Prepared 树，以释放表级读预留；
        // 此时确认器不会被调用。详见 CONTRACT-CFM-001 和 CONTRACT-TR-005：
        // docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only。
        // 根 WAL append/flush 的环境/runtime 失败属于 LIMIT-ROOT-WAL-IO-001：上游当前会返回
        // LogCommitFailed，但普通 io::Error 无法证明 WAL 的实际落盘阶段。本层只透传错误，不得
        // 在这里猜测耐久状态、清理 CommitLogger checkpoint 或改变 rollback/Fatal 分类。
        // 当前根 WAL 链最终调用 pi_async_file::AsyncFile::write，而不是未被这四库调用的
        // write_batch；后者的独立实现缺陷归档为 FIND-ASYNC-FILE-BATCH-001，不得据此扩大本边界。
        // 普通三叶根的直接结构、共享 TID/CID、一次 WAL 和 data-only 最终状态由
        // tests/ordinary_multi_table_recovery.rs 验证；该测试不改变本处顺序或计数算法。
        let commit_confirm = KVDBCommitConfirm::new(self.0.db_mgr.0.rt.clone(),
                                                    self.0.db_mgr.0.tr_mgr.commit_logger(),
                                                    self.get_transaction_uid().unwrap(),
                                                    self.get_commit_uid(),
                                                    self.persistent_children_len());

        //提交键值对数据库的根事务
        match self
            .0
            .db_mgr
            .0
            .tr_mgr
            .commit(KVDBTransaction::RootTr(self.clone()),
                    prepare_output,
                    commit_confirm)
            .await {
            Err(e) => Err(e),
            Ok(_) => {
                //提交键值对数据库的根事务成功，则完成本次键值对数据库事务
                self
                    .0
                    .db_mgr
                    .0
                    .tr_mgr
                    .finish(KVDBTransaction::RootTr(self.clone()));
                Ok(())
            }
        }
    }

    ///
    /// 异步回滚本次事务对键值对数据库的所有修改，事务严重错误无法回滚
    ///
    #[inline]
    async fn rollback_modified(&self) -> Result<(), KVTableTrError> {
        //回滚键值对数据库的根事务
        if let Err(e) = self
            .0
            .db_mgr
            .0
            .tr_mgr
            .rollback(KVDBTransaction::RootTr(self.clone()))
            .await {
            //回滚键值对数据库的根事务失败，则立即返回错误原因
            return Err(e);
        }

        //回滚键值对数据库的根事务成功，则完成本次键值对数据库事务
        self
            .0
            .db_mgr
            .0
            .tr_mgr
            .finish(KVDBTransaction::RootTr(self.clone()));
        Ok(())
    }

    /// 异步预提交本次事务对键值对数据库的所有修复修改，不返回预提交的输出
    async fn prepare_repair(&self,
                            transaction_uid: Guid)
                            -> Result<(), KVTableTrError> {
        let mut childs = self.to_children();
        while let Some(child) = childs.next() {
            match &child {
                KVDBTransaction::MetaTabTr(tr) => {
                    tr.prepare_repair(transaction_uid.clone());
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    tr.prepare_repair(transaction_uid.clone());
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    tr.prepare_repair(transaction_uid.clone());
                },
                KVDBTransaction::LogWTabTr(tr) => {
                    tr.prepare_repair(transaction_uid.clone());
                },
                KVDBTransaction::BtreeOrdTabTr(tr) => {
                    tr.prepare_repair(transaction_uid.clone());
                },
                KVDBTransaction::RootTr(_) => {
                    //忽略根事务，并继续执行下一个子事务的预提交修复
                    continue;
                }
            }
        }

        Ok(())
    }

    /// 异步提交本次事务对键值对数据库的所有修复修改
    #[inline]
    async fn commit_repair(&self,
                           transaction_uid: Guid,
                           commit_uid: Guid,
                           prepare_output: Vec<u8>) -> Result<(), KVTableTrError> {
        // 恢复提交沿用正常提交的成功信号协议：只有本次重放涉及的全部持久化子表成功，
        // 原根 WAL 才能确认；任一数据文件失败都不发送成功信号。详见 CONTRACT-CFM-001：
        // docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only。
        let commit_confirm = KVDBCommitConfirm::new(self.0.db_mgr.0.rt.clone(),
                                                    self.0.db_mgr.0.tr_mgr.commit_logger(),
                                                    transaction_uid.clone(),
                                                    Some(commit_uid.clone()),
                                                    self.persistent_children_len());

        //重播提交键值对数据库的根事务
        match self
            .0
            .db_mgr
            .0
            .tr_mgr
            .replay_commit(KVDBTransaction::RootTr(self.clone()),
                           transaction_uid,
                           commit_uid,
                           prepare_output,
                           commit_confirm)
            .await {
            Err(e) => Err(e),
            Ok(_) => {
                //重播提交键值对数据库的根事务成功，则完成本次键值对数据库重播事务
                self
                    .0
                    .db_mgr
                    .0
                    .tr_mgr
                    .finish(KVDBTransaction::RootTr(self.clone()));
                Ok(())
            }
        }
    }
}

/// 版本事务装配时固定的一张表身份和用途。
///
/// `versions` 的 Arc 身份同时代表精确表实例；缺表保存 None。根事务在 Phase 1 和 Phase 2
/// 分别复核一次，防止两阶段之间的 registry 替换被忽略。只读身份失效可枚举 read_keys 为完整
/// 冲突；含写表失效则返回 Common，因为旧表写入不能安全改投到同名新表。
#[derive(Clone)]
struct RootVersionTableIdentity {
    name: Atom,
    versions: Option<KeyVersions>,
    read_keys: Vec<Binary>,
    has_write: bool,
}

/// 根提交回执中一项写入必须具有的动作类型。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpectedVersionKind {
    Upsert,
    Delete,
}

/// 一棵独立版本事务树的根级上下文。
///
/// 表身份按首次触表顺序保存；receipt 与所有已创建子事务共享；expected_writes 固定本次输入
/// 的最终写集合，用于提交后验证每个 `(Table, Key)` 恰好产生同 TID、同动作类型的一项回执。
/// 上下文只属于 `prepare_with_version -> commit_with_version` 协议，不得与普通动作、DDL 或
/// 普通提交入口混用。
#[derive(Clone)]
struct RootVersionContext {
    tables: Vec<RootVersionTableIdentity>,
    receipt: VersionReceipt,
    expected_writes: Arc<XHashMap<TableKey, ExpectedVersionKind>>,
}

/// `prepare_with_version` 在共享状态副作用前构造的单表规范化输入。
///
/// `expected` 与最终 `actions` 是两个不同维度：跨 read/write 重叠的 Key 同时保留外部读版本和
/// 最终 Write。`registered` 只克隆 registry 中的精确 `(table, versions)` 配对，后续 await 不持
/// tables guard。
struct VersionPrepareGroup<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    name: Atom,
    registered: Option<RegisteredTable<C, Log>>,
    expected: XHashMap<Binary, Version>,
    actions: XHashMap<Binary, crate::KVActionLog>,
    read_keys: Vec<Binary>,
    has_write: bool,
}

/// 根事务共享状态。
///
/// 所有字段都随 [`KVDBManager::transaction`] 一次性构造。同步锁只保护短内存状态，不能替代
/// 外部对单次 prepare/commit/rollback 和非并发生命周期调用的协议保证。
struct InnerRootTransaction<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    /// 原样继承到子事务的来源标签；参与来源计数/限流、诊断和事件，不参与 UID 生成。
    source:             Atom,
    /// 上游 `start` 为外层根生成并递归发布给全部 owned 子节点的事务 ID。
    tid:                SpinLock<Option<Guid>>,
    /// 需要根 WAL 时在 prepare 前生成并递归发布的提交确认占位 ID。
    cid:                SpinLock<Option<Guid>>,
    /// 共享 2PC 状态；锁保证内存安全，不允许并发或重复生命周期调用。
    status:             SpinLock<Transaction2PcStatus>,
    /// 创建时固定的读写能力；当前动作入口仍有 `FIND-TR-001` 所述只读写入缺口。
    writable:           bool,
    /// 原子 `Unselected/Ordinary/Versioned` 业务协议；SchemaCreate 不增加第四种根模式。
    protocol:           AtomicU8,
    /// 整棵事务树是否写根 WAL 的权威聚合位，不表示存在根数据文件。
    persistence:        AtomicBool,
    /// 原样传给子节点但当前不执行计时的 prepare timeout。
    prepare_timeout:    u64,
    /// 原样传给子节点但当前不执行计时的 commit timeout。
    commit_timeout:     u64,
    /// 按表名保存每表唯一 2PC owner；与 `childs` 的固定锁序是 map -> list。
    childs_map:         SpinLock<XHashMap<Atom, KVDBTransaction<C, Log>>>,
    /// 按首次触表顺序保存 manager 遍历的 owned 子节点。
    childs:             SpinLock<KVDBChildTrList<C, Log>>,
    /// 反向持有数据库 manager，保证事务使用期间表注册表、runtime 和 2PC manager 存活。
    db_mgr:             KVDBManager<C, Log>,
    /// 仅版本协议安装的表身份、预期写集合和提交回执 owner。
    version_context:    SpinLock<Option<RootVersionContext>>,
}

/// trace 构建以最终 owner 析构作为事务对象关闭的唯一观测点。
///
/// 此时 `db_mgr` 字段仍有效；Drop 只执行一次 Relaxed 原子增量，随后 Rust 继续按原顺序释放全部
/// 字段。它不调用 manager.finish、不改变事务状态；事务 clone 和 manager registry owner 会推迟
/// 本 Drop。迭代器流只持表子事务/快照而不持根 owner，合法协议必须先结束流再释放根事务。
#[cfg(feature = "trace")]
impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Drop for InnerRootTransaction<C, Log> {
    fn drop(&mut self) {
        self.db_mgr
            .0
            .transaction_metrics
            .closed
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// 数据库注册表中五种物理/逻辑表实现的类型擦除句柄。
///
/// clone 只克隆内部表句柄，不复制数据。应用层从 [`KVDBManager`] 的公开方法操作表；直接
/// 构造 variant 会绕过名称、元数据、版本 registry 和目录注册协议，属于禁止调用域。
#[derive(Clone)]
pub enum KVDBTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    /// 记录数据库表定义的 Meta 表。
    MetaTab(MetaTable<C, Log>),
    /// COW 有序 Map 支撑的 Memory 表。
    MemOrdTab(MemoryOrderedTable<C, Log>),
    /// 日志文件支撑的可查询 LogOrdered 表。
    LogOrdTab(LogOrderedTable<C, Log>),
    /// 只写 LogWrite 表；当前不允许外部业务使用。
    LogWTab(LogWriteTable<C, Log>),
    /// 事务写缓存与 redb 数据文件共同支撑的 Btree 表。
    BtreeOrdTab(BtreeOrderedTable<C, Log>),
}

// SAFETY: 所有 variant 的表句柄都由 Arc/线程安全存储和同步原语持有；移动枚举仅移动共享
// owner，不移动自引用对象或暴露可变别名。表级合法调用协议不由该 marker 保证。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for KVDBTable<C, Log> {}
// SAFETY: 各表实现负责其 COW root、缓存、日志/redb 句柄的同步；枚举没有额外内部可变性。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for KVDBTable<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVDBTable<C, Log> {
    /// 是否可持久化的表
    #[inline]
    pub fn is_persistent(&self) -> bool {
        match self {
            Self::MetaTab(tab) => tab.is_persistent(),
            Self::MemOrdTab(tab) => tab.is_persistent(),
            Self::LogOrdTab(tab) => tab.is_persistent(),
            Self::LogWTab(tab) => tab.is_persistent(),
            Self::BtreeOrdTab(tab) => tab.is_persistent(),
        }
    }
}

// 数据库跟踪循环
#[cfg(feature = "trace")]
async fn loop_tracing<R, C, Log>(rt: R,
                                 db_mgr: KVDBManager<C, Log>,
                                 interval: usize)
    where R: AsyncRuntime,
          C: Clone + Send + 'static,
          Log: AsyncCommitLog<C = C, Cid = Guid>,
{
    let meter = get_database_meter();
    let instruments = DatabaseTraceInstruments::new(meter);
    let mut previous_tables = HashSet::new();
    let mut previous_api_metrics = KeyVersionApiMetricsSnapshot::default();
    let mut previous_transaction_metrics = TransactionLifecycleMetricsSnapshot::default();
    loop {
        rt.timeout(interval).await;
        let now = Instant::now();
        let mut current_tables = HashSet::new();
        for table in db_mgr.tables().await {
            if let Some((table_cache_size, version_metrics)) =
                db_mgr.table_tracing_metrics(&table).await {
                instruments.record_table(&table, table_cache_size, version_metrics);
                current_tables.insert(table);
                rt.timeout(0).await;
            }
        }
        // Synchronous Gauge 的后端可能保留最后值；为本轮消失的表记录一次 0，但不改变 DDL、
        // retired KeyVersions 生命周期或既有 table_cache_size 指标的历史行为。
        for table in previous_tables.difference(&current_tables) {
            instruments.record_removed_table(table);
        }
        previous_tables = current_tables;

        // 调用点只写 AtomicU64；这里把累计快照转换为 Counter delta，禁止每轮重复上报累计值。
        let current_api_metrics = db_mgr.0.key_versions.api_metrics_snapshot();
        let api_delta = current_api_metrics.delta_since(previous_api_metrics);
        previous_api_metrics = current_api_metrics;
        instruments.record_api_delta(api_delta);

        let current_transaction_metrics = db_mgr.transaction_metrics_snapshot();
        let transaction_delta = current_transaction_metrics
            .delta_since(previous_transaction_metrics);
        previous_transaction_metrics = current_transaction_metrics;
        instruments.record_transaction_delta(transaction_delta);
        info!("Loop tracing succeeded, interval: {:?}ms, time: {:?}",
            interval,
            now.elapsed());
    }
}

#[cfg(all(test, feature = "trace"))]
#[path = "db_metrics_tests.rs"]
mod metrics_tests;

#[cfg(test)]
#[path = "db_startup_tests.rs"]
mod startup_tests;

// 将表名序列化为二进制数据
pub(crate) fn table_to_binary(table_name: &Atom) -> Binary {
    let mut buffer = WriteBuffer::new();
    table_name.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

// 将二进制数据反序列化为表名
pub(crate) fn binary_to_table(bin: &Binary) -> Result<Atom, ReadBonErr> {
    let mut buffer = ReadBuffer::new(bin, 0);
    Atom::decode(&mut buffer)
}

// 校验完整表名的 UTF-8 字节长度。调用方选择 InvalidInput（公开 DDL 参数）或 InvalidData
//（Meta/WAL 恢复数据）；错误消息只记录长度，避免把最多数 KiB 的名称复制进日志。
#[inline]
fn validate_table_name(table_name: &Atom,
                       error_kind: ErrorKind,
                       operation: &'static str) -> IOResult<()> {
    let bytes_len = table_name.as_str().as_bytes().len();
    if bytes_len == 0 || bytes_len > MAX_TABLE_NAME_BYTES {
        return Err(Error::new(error_kind,
                              format!("{} failed, table_name_bytes: {}, valid_range: 1..={}, reason: invalid table name length",
                                      operation,
                                      bytes_len,
                                      MAX_TABLE_NAME_BYTES)));
    }

    Ok(())
}

/// 校验版本协议的单个 Table/Key，必须在 Guid 分配、版本写入和事务创建前调用。
#[inline]
fn validate_version_table_key(table: &Atom,
                              key: &Binary,
                              operation: &'static str) -> Result<(), KVTableTrError> {
    validate_table_name(table, ErrorKind::InvalidInput, operation).map_err(|e| {
        KVTableTrError::new_transaction_error(ErrorLevel::Normal, e)
    })?;
    if key.len() == 0 || key.len() > u16::MAX as usize {
        return Err(KVTableTrError::new_transaction_error(
            ErrorLevel::Normal,
            format!("{} failed, table: {:?}, key_bytes: {}, valid_range: 1..={}, reason: invalid key length",
                    operation,
                    table.as_str(),
                    key.len(),
                    u16::MAX)));
    }

    Ok(())
}

/// 静态校验版本事务的两个输入集合；本函数无共享状态副作用，必须先于事务 UID 和子事务创建。
fn validate_prepare_with_version_inputs(read_set: &[TableKeyVersion],
                                        write_set: &[TableKV])
    -> Result<(), KVTableTrError> {
    let mut read_seen: XHashMap<Atom, XHashMap<Binary, ()>> = XHashMap::default();
    for item in read_set {
        validate_version_table_key(&item.table, &item.key, "prepare with version read set")?;
        if read_seen
            .entry(item.table.clone())
            .or_default()
            .insert(item.key.clone(), ())
            .is_some() {
            return Err(KVTableTrError::new_transaction_error(
                ErrorLevel::Normal,
                format!("Prepare with version failed, table: {:?}, key_bytes: {}, reason: duplicate key in read set",
                        item.table.as_str(),
                        item.key.len())));
        }
    }

    let mut write_seen: XHashMap<Atom, XHashMap<Binary, ()>> = XHashMap::default();
    for item in write_set {
        validate_version_table_key(&item.table, &item.key, "prepare with version write set")?;
        if let Some(value) = item.value.as_ref() {
            if value.len() == 0 || value.len() > u32::MAX as usize {
                return Err(KVTableTrError::new_transaction_error(
                    ErrorLevel::Normal,
                    format!("Prepare with version failed, table: {:?}, value_bytes: {}, valid_range: 1..={}, reason: invalid value length",
                            item.table.as_str(),
                            value.len(),
                            u32::MAX)));
            }
        }
        if write_seen
            .entry(item.table.clone())
            .or_default()
            .insert(item.key.clone(), ())
            .is_some() {
            return Err(KVTableTrError::new_transaction_error(
                ErrorLevel::Normal,
                format!("Prepare with version failed, table: {:?}, key_bytes: {}, reason: duplicate key in write set",
                        item.table.as_str(),
                        item.key.len())));
        }
    }

    Ok(())
}

// 删表专用校验：除通用长度契约外，内部 Meta 表永远不能成为公开删除或 WAL 删除恢复目标。
// 调用方必须在根持久化标记、注册表锁和 Meta 动作之前调用，以保证拒绝路径无副作用。
#[inline]
fn validate_removable_table_name(table_name: &Atom,
                                 error_kind: ErrorKind,
                                 operation: &'static str) -> IOResult<()> {
    validate_table_name(table_name, error_kind, operation)?;
    if table_name.as_str() == DEFAULT_DB_TABLES_META_DIR {
        return Err(Error::new(error_kind,
                              format!("{} failed, table_name: {:?}, reason: reserved internal meta table cannot be removed",
                                      operation,
                                      DEFAULT_DB_TABLES_META_DIR)));
    }

    Ok(())
}
