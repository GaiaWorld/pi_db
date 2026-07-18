//! Btree 有序持久表的混合存储实现。
//!
//! 逻辑状态由三层组成：事务私有 `cache_mut/key_states`、已经提交但可能尚未写入数据文件的
//! 共享只写 overlay，以及 redb 中的稳定数据。读取必须先解释 overlay 三态：Map 无 Key 才能
//! 回落 redb，`Some(value)` 是最新逻辑值，`None` 是遮蔽 redb 旧值的删除 tombstone。任何代码
//! 都不能把“overlay 无 Key”和“逻辑不存在”合并成同一状态。
//!
//! 根 WAL 成功以后，节点 `commit` 先在版本 publication 写锁内原子发布共享 overlay、每 Key
//! transaction UID 标记、版本 revision 和可选回执；随后释放所有同步锁，把冻结动作交给后台
//! collector。collector 使用一个 redb 写事务批量落盘，成功后才调用提交确认并按 transaction
//! UID 清理仍属于该批事务的 overlay。根 WAL 提交成功与 redb 数据文件确认成功是两个有序但
//! 不等价的阶段。
//!
//! `keys/values` 同时拥有创建流时的 COW overlay 根和 redb `ReadTransaction`，因此在创建事务
//! 仍存活的合法域内保持创建流瞬间的双层快照；流不保证与创建事务后续 upsert/delete 的事务
//! 安全性。`query` 与 `dirty_query` 当前同义，事务安全方法和 `dirty_*` 方法混用不提供保证。
//!
//! 当前 `len` 对活跃 overlay/tombstone 的统计语义尚未冻结，见
//! `docs/REVIEW_FINDINGS.md#find-table-002`；调用方不得把它当作严格逻辑快照计数。

use std::{mem, thread};
use std::path::{Path, PathBuf};
use std::collections::{VecDeque, HashMap, BTreeMap, hash_map::Entry as HashMapEntry};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::sync::{Arc,
                atomic::{AtomicBool, AtomicUsize}};
use std::io::{Error, Result as IOResult, ErrorKind};
use std::ops::Deref;
use pi_async_rt::{rt::{AsyncRuntime,
                       multi_thread::MultiTaskRuntime},
                  lock::spin_lock::SpinLock};
use async_lock::{Mutex as AsyncMutex, RwLock as AsyncRwLock};
use async_channel::{Sender, Receiver, bounded, unbounded};
use pi_async_transaction::{AsyncCommitLog,
                           TransactionError,
                           Transaction2Pc,
                           Transaction2PcAllConflicts,
                           ErrorLevel,
                           AsyncTransaction,
                           UnitTransaction,
                           SequenceTransaction,
                           TransactionTree,
                           manager_2pc::Transaction2PcStatus};
use pi_atom::Atom;
use futures::{future::{FutureExt, BoxFuture},
              stream::{StreamExt, BoxStream}};
use parking_lot::{Mutex, RwLock};
use pi_async_file::file::create_dir;
use redb::{Key, Value, ReadableTableMetadata, ReadableTable, Builder as TableBuilder, Database, TypeName, TableDefinition, ReadTransaction, WriteTransaction, ReadOnlyTable, Table, Range, Durability, DatabaseError, TableError};
use async_stream::stream;
use dashmap::DashMap;
use pi_guid::Guid;
use pi_hash::XHashMap;
use pi_bon::ReadBuffer;
use log::{trace, debug, error, warn, info};
use pi_ordmap::asbtree::Tree;
use pi_ordmap::ordmap::{ImOrdMap, OrdMap};
use pi_store::log_store::log_file::LogMethod;

use crate::{Binary, KVAction, KVActionLog, KVDBCommitConfirm, KVTableTrError, TableKey, TableTrQos, TransactionDebugEvent, transaction_debug_logger, db::{KVDBChildTrList, KVDBTransaction}, key_version::{KeyVersions,
                                                                                                                                                                                                                                                      PrepareMode,
                                                                                                                                                                                                                                                      PreparedActions,
                                                                                                                                                                                                                                                      PreparedCommitError,
                                                                                                                                                                                                                                                      TableVersionContext,
                                                                                                                                                                                                                                                      Version,
                                                                                                                                                                                                                                                      VersionReceipt,
                                                                                                                                                                                                                                                      has_prepared_conflict,
                                                                                                                                                                                                                                                      has_prepared_transaction,
                                                                                                                                                                                                                                                      take_prepared_for_commit}, tables::{KVTable, ordmap_snapshot::OrdMapSnapshot,
                                                                                                                                                                                            log_ord_table::{LogOrderedTable, LogOrdTabTr}}, utils::KVDBEvent, KVDBTableType};

// 默认的表文件名
const DEFAULT_TABLE_FILE_NAME: &str = "table.dat";

// 默认的表名
const DEFAULT_TABLE_NAME: TableDefinition<Binary, Binary> = TableDefinition::new("$default");

// 最小缓存大小
const MIN_CACHE_SIZE: usize = 32 * 1024;

// 默认缓存大小
pub(crate) const DEFAULT_CACHE_SIZE: usize = 2 * 1024 * 1024;

impl Value for Binary {
    type SelfType<'a>
    where
        Self: 'a
    = Binary;
    type AsBytes<'a>
    where
        Self: 'a
    = Binary;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a
    {
        Binary::new(data.to_vec())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b
    {
        value.clone()
    }

    fn type_name() -> TypeName {
        TypeName::new("Binary")
    }
}

impl Key for Binary {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        if let Some(ord) = ReadBuffer::new(data1, 0)
            .partial_cmp(&ReadBuffer::new(data2, 0))
        {
            ord
        } else {
            //pi_bon比较失败，则强制判等
            error!("Compare binary key failed with pi_bon, data1: {:?}, data2: {:?}",
                data1,
                data2);
            std::cmp::Ordering::Equal
        }
    }
}

/// redb 稳定数据与内存只写 overlay 组成的有序持久表共享句柄。
///
/// clone 只增加 `Arc` 引用；后台 collector 也持有 clone，因此表句柄释放不等于后台状态立即
/// 析构。公开点读、事务、迭代和整理入口共享同一 redb 数据库及 overlay 同步边界。
#[derive(Clone)]
pub struct BtreeOrderedTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerBtreeOrderedTable<C, Log>>);

// SAFETY: Database、overlay、prepare、waits 及 collector owner 分别由 RwLock、Mutex、
// AsyncMutex 或原子状态保护；外层句柄只跨线程移动 Arc。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for BtreeOrderedTable<C, Log> {}
// SAFETY: 共享引用无法无同步取得内部可变状态；redb 自身的事务并发规则由 inner RwLock
// 和 redb transaction guard 共同维持。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for BtreeOrderedTable<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVTable for BtreeOrderedTable<C, Log> {
    type Name = Atom;
    type Tr = BtreeOrdTabTr<C, Log>;
    type Error = KVTableTrError;

    fn name(&self) -> <Self as KVTable>::Name {
        self.0.name.clone()
    }

    fn path(&self) -> Option<&Path> {
        Some(self.0.path.as_path())
    }

    #[inline]
    fn is_persistent(&self) -> bool {
        true
    }

    fn is_ordered(&self) -> bool {
        true
    }

    fn len(&self) -> usize {
        // 当前实现以 redb table.len() 为基线，再只补计 redb miss 的 overlay Key；它没有完整
        // 抵消 tombstone 或区分 cache-only tombstone。因此结果不是严格逻辑长度，已归档为
        // FIND-TABLE-002，待语义冻结和真实参考模型专项后再决定是否修改。
        if let Ok(tr) = self.0.inner.read().begin_read() {
            if let Ok(table) = tr.open_table(DEFAULT_TABLE_NAME) {
                let mut table_len = table.len().unwrap_or(0) as usize;
                let cache_copy = self.0.cache.lock().clone();
                let keys = cache_copy.keys(None, false);
                for key in keys {
                    if let Ok(None) = table.get(key) {
                        //记录只在缓存中的关键字
                        table_len += 1;
                    }
                }

                table_len
            } else {
                0
            }
        } else {
            0
        }
    }

    fn size(&self) -> u64 {
        let cache_copy = self.0.cache.lock().clone();
        cache_copy.full_bytes_size()
    }

    fn transaction(&self,
                   source: Atom,
                   is_writable: bool,
                   is_persistent: bool,
                   prepare_timeout: u64,
                   commit_timeout: u64) -> Self::Tr {
        BtreeOrdTabTr::new(source,
                           is_writable,
                           is_persistent,
                           prepare_timeout,
                           commit_timeout,
                           self.clone())
    }

    fn ready_collect(&self) -> BoxFuture<Result<(), Self::Error>> {
        async move {
            //忽略整理准备
            Ok(())
        }.boxed()
    }

    fn collect(&self) -> BoxFuture<Result<(), Self::Error>> {
        let table = self.clone();

        async move {
            //检查是否正在异步整理，如果并未开始异步整理，则设置为正在异步整理，并继续有序B树表的压缩
            loop {
                if let Err(_) = table.0.collecting.compare_exchange(false,
                                                                    true,
                                                                    Ordering::Acquire,
                                                                    Ordering::Relaxed) {
                    //正在异步整理，则稍候重试
                    table.0.rt.timeout(1000).await;
                    continue;
                }

                break;
            }

            //将所有未持久的事务，强制持久化提交
            let mut locked = self.0.inner.write(); //避免外部产生其它事务
            let mut transaction = match locked.begin_write() {
                Err(e) => {
                    //创建写事务失败，则立即返回错误原因
                    table.0.collecting.store(false, Ordering::Release); //设置为已整理结束
                    return Err(KVTableTrError::new_transaction_error(ErrorLevel::Fatal,
                                                                     format!("Compact b-tree ordered table failed, table: {:?}, , reason: {:?}",
                                                                             table.name().as_str(),
                                                                             e)));
                },
                Ok(transaction) => transaction,
            };
            transaction.set_durability(Durability::Immediate);
            transaction.set_quick_repair(table.0.enable_accelerated_repair); //设置redb写事务是否打开快速修复
            if let Err(e) = transaction.commit() {
                //写事务持久化提交失败，则立即返回错误原因
                table.0.collecting.store(false, Ordering::Release); //设置为已整理结束
                return Err(KVTableTrError::new_transaction_error(ErrorLevel::Fatal,
                                                                 format!("Compact b-tree ordered table failed, table: {:?}, , reason: {:?}",
                                                                         table.name().as_str(),
                                                                         e)));
            }

            //开始B树表的压缩
            // 当前 retry_count 未递减且成功后未立即 break，compact 的重试/返回语义存在已归档
            // 高可信缺陷 FIND-TABLE-001。本轮只补注释，不改变维护流程或扩大修复范围。
            let mut retry_count = 3; //可以重试3次
            for _ in 0..3 {
                let now = Instant::now();
                if let Err(e) = locked.compact() {
                    //压缩数据表失败
                    if retry_count == 0 {
                        //重试已达限制，则立即返回错误原因
                        table.0.collecting.store(false, Ordering::Release); //设置为已整理结束
                        return Err(KVTableTrError::new_transaction_error(ErrorLevel::Normal,
                                                                         format!("Compact b-tree ordered table failed, table: {:?}, time: {:?}, reason: {:?}",
                                                                                 table.name().as_str(),
                                                                                 now.elapsed(),
                                                                                 e)));
                    } else {
                        //稍候重试
                        thread::sleep(Duration::from_millis(1000)); //必须同步休眠指定时间
                        continue;
                    }
                }

                info!("Compact b-tree ordered table succeeded, table: {:?}, time: {:?}",
                table.name().as_str(),
                now.elapsed());
            }

            table.0.collecting.store(false, Ordering::Release); //设置为已整理结束
            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> BtreeOrderedTable<C, Log> {
    /// 读取当前已提交的逻辑值，并严格传播 redb 点读错误。
    ///
    /// 调用方必须持有本表 publication read。overlay 的 value/tombstone 是最终逻辑状态；只有
    /// overlay 完全缺席才回落 redb。该方法不创建表事务、不登记 Read，也不修改事务缓存。
    pub(crate) fn query_committed(&self, key: &Binary) -> IOResult<Option<Binary>> {
        let cache = self.0.cache.lock();
        if let Some(value) = cache.get(key) {
            return Ok(value.clone());
        }
        drop(cache);

        let inner = self.0.inner.read();
        let transaction = inner.begin_read().map_err(|e| {
            Error::new(ErrorKind::Other,
                       format!("Query b-tree ordered table with version failed, table: {:?}, key_bytes: {}, stage: begin_read, reason: {:?}",
                               self.0.name.as_str(),
                               key.len(),
                               e))
        })?;
        let table = match transaction.open_table(DEFAULT_TABLE_NAME) {
            Ok(table) => table,
            // 新建且从未物理写入的 redb 数据库尚无 `$default` 表，这是 Btree 逻辑空表的合法
            // 表示，不是点读故障。只允许该精确分支返回 miss；类型不匹配、存储损坏等其它
            // open_table 错误仍必须传播为 Common(Normal)。该例外不创建 redb 写事务，也不
            // 修改 overlay、版本或 WAL。证据见 BUG-KV-BTREE-QWV-001：
            // docs/KEY_VERSION_BTREE_EMPTY_QUERY_BUG.md#bug-kv-btree-qwv-001-index。
            Err(TableError::TableDoesNotExist(name)) if name == "$default" => return Ok(None),
            Err(e) => {
                return Err(Error::new(ErrorKind::Other,
                                      format!("Query b-tree ordered table with version failed, table: {:?}, key_bytes: {}, stage: open_table, reason: {:?}",
                                              self.0.name.as_str(),
                                              key.len(),
                                              e)));
            },
        };
        let value = table.get(key).map_err(|e| {
            Error::new(ErrorKind::Other,
                       format!("Query b-tree ordered table with version failed, table: {:?}, key_bytes: {}, stage: get, reason: {:?}",
                               self.0.name.as_str(),
                               key.len(),
                               e))
        })?;

        Ok(value.map(|value| value.value()))
    }

    /// 打开一个有序 Btree 表；同一路径已经打开或其它打开失败会 panic。
    ///
    /// `cache_size < 32 KiB` 会回退到 2 MiB 默认值；`waits_limit/wait_timeout` 控制 redb
    /// collector 的容量和定时触发。`enable_accelerated_repair` 传给每个 redb 写事务的
    /// `set_quick_repair`，用于降低未来修复成本，但可能降低正常提交吞吐。构造成功会启动一个
    /// 永久 collector task，调用方不能把最后一个外部句柄的 drop 当作显式 shutdown。
    pub async fn new<P: AsRef<Path>>(rt: MultiTaskRuntime<()>,
                                     path: P,
                                     name: Atom,
                                     cache_size: usize,
                                     enable_compact: bool,
                                     waits_limit: usize,
                                     wait_timeout: usize,
                                     enable_accelerated_repair: bool,
                                     notifier: Option<Sender<KVDBEvent<Guid>>>) -> Self
    {
        Self::try_new(rt,
                      path,
                      name.clone(),
                      cache_size,
                      enable_compact,
                      waits_limit,
                      wait_timeout,
                      enable_accelerated_repair,
                      notifier)
            .await
            .expect(format!("Open b-tree ordered table failed, table: {:?}, reason: Attempted to open a table that is already open", name.as_str()).as_str())
    }

    /// 尝试打开 Btree 表；仅 `DatabaseAlreadyOpen` 返回 `None`，其它创建/打开错误仍 panic。
    ///
    /// 路径目录不存在时会通过给定 runtime 异步创建。redb 自身负责启动修复，回调只记录进度；
    /// 本方法不执行根 WAL replay。成功返回前只完成 redb 打开与 collector 启动，overlay 初始为空。
    pub(crate) async fn try_new<P: AsRef<Path>>(rt: MultiTaskRuntime<()>,
                                                path: P,
                                                name: Atom,
                                                mut cache_size: usize,
                                                enable_compact: bool,
                                                waits_limit: usize,
                                                wait_timeout: usize,
                                                enable_accelerated_repair: bool,
                                                notifier: Option<Sender<KVDBEvent<Guid>>>) -> Option<Self>
    {
        let now = Instant::now();
        let cache_size = if cache_size < MIN_CACHE_SIZE {
            DEFAULT_CACHE_SIZE
        } else {
            cache_size
        };

        if !path.as_ref().exists() {
            //指定的路径不存在，则线程安全的创建指定路径
            if let Err(e) = create_dir(rt.clone(), path.as_ref().to_path_buf()).await {
                //创建指定路径的目录失败，则立即返回
                panic!("Create b-tree ordered table dir failed, path: {:?}, {:?}",
                       path.as_ref(),
                       e);
            }
        }

        let path = path
            .as_ref()
            .to_path_buf()
            .join(Path::new(DEFAULT_TABLE_FILE_NAME));
        let mut count = 0;
        let name_copy = name.clone();
        match TableBuilder::new()
            .set_cache_size(cache_size)
            .set_repair_callback(move |session| {
                if count == 0 {
                    //开始修复
                    info!("Repairing inner b-tree ordered table, table: {:?}, cache_size: {:?}, enable_compact: {:?}",
                        name_copy,
                        cache_size,
                        enable_compact);
                }

                let progress = session.progress();
                if progress < 1.0 {
                    //正在修复
                    trace!("Repairing inner b-tree ordered table, table: {:?}, progress: {:?}",
                        name_copy,
                        progress);
                } else {
                    //修复完成
                    info!("Repair inner b-tree ordered table succeeded, table: {:?}, cache_size: {:?}, enable_compact: {:?}",
                        name_copy,
                        cache_size,
                        enable_compact);
                }
            })
            .create(path.clone())
        {
            Err(e) => {
                if let DatabaseError::DatabaseAlreadyOpen = &e {
                    //已打开，则忽略打开指定路径下的有序B树表
                    None
                } else {
                    panic!("Create b-tree ordered table failed, table: {:?}, cache_size: {:?}, enable_compact: {:?}, reason: {:?}",
                           name,
                           cache_size,
                           enable_compact,
                           e);
                }
            },
            Ok(db) => {
                let inner = RwLock::new(db);
                let cache = Mutex::new(OrdMap::new(None));
                let cache_flags = Mutex::new(XHashMap::default());
                let prepare = Mutex::new(XHashMap::default());
                let waits = AsyncMutex::new(VecDeque::new());
                let waits_size = AtomicUsize::new(0);
                let collecting = AtomicBool::new(false);

                let inner = InnerBtreeOrderedTable {
                    name: name.clone(),
                    path: path.clone(),
                    inner,
                    cache,
                    cache_flags,
                    prepare,
                    rt,
                    enable_compact: AtomicBool::new(enable_compact),
                    waits,
                    waits_size,
                    waits_limit,
                    wait_timeout,
                    collecting,
                    notifier,
                    enable_accelerated_repair,
                };
                let table = BtreeOrderedTable(Arc::new(inner));
                info!("Load b-tree ordered table succeeded, table: {:?}, keys: {:?}, cache_size: {:?}, enable_compact: {:?}, time: {:?}",
                    name,
                    table.len(),
                    cache_size,
                    enable_compact,
                    now.elapsed());

                //启动有序B树表的提交待确认事务的定时整理
                let table_copy = table.clone();
                let _ = table.0.rt.spawn(async move {
                    let table_ref = &table_copy;
                    loop {
                        match collect_waits(table_ref,
                                            Some(table_copy.0.wait_timeout))
                            .await
                        {
                            Err((collect_time, statistics)) => {
                                error!("Collect b-tree ordered table failed, table: {:?}, time: {:?}, statistics: {:?}, reason: out of time",
                                    table_copy.name().as_str(),
                                    collect_time,
                                    statistics);
                            },
                            Ok((collect_time, statistics)) => {
                                debug!("Collect b-tree ordered table succeeded, table: {:?}, time: {:?}, statistics: {:?}, reason: out of time",
                                    table_copy.name().as_str(),
                                    collect_time,
                                    statistics);
                            },
                        }
                    }
                });

                Some(table)
            },
        }
    }
}

/// Btree 表的共享状态和各同步域所有权。
///
/// 热路径固定锁序为版本 publication -> `cache_flags` -> `cache`；collector 不持 publication，
/// 只在 redb 成功提交后按 TID 清理 overlay。`inner` 不得与 `cache`/`cache_flags` 长时间交叉
/// 持有，尤其不能在同步 guard 内等待异步任务。
struct InnerBtreeOrderedTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    /// 数据库逻辑表名，也是版本缓存和事件中的表身份。
    name:                       Atom,
    /// redb `table.dat` 的完整路径。
    path:                       PathBuf,
    /// redb 数据库；读锁允许创建 redb 读/写事务，写锁用于 compact 等排他维护。
    inner:                      RwLock<Database>,
    /// 已提交只写 overlay。Map 无 Key表示可回落 redb，`Some(value)` 表示覆盖值，`None`
    /// 表示逻辑删除 tombstone；redb 成功提交且 TID 仍匹配后才允许清理。
    /// 详见 CONTRACT-BTREE-DELETE-001 和 `tests/btree_delete_old_value.rs`。
    cache:                      Mutex<OrdMap<Tree<Binary, Option<Binary>>>>,
    /// 每个 overlay Key 最近一次发布它的事务 ID，用于防止旧 collector 清除并发新值。
    cache_flags:                Mutex<XHashMap<Binary, Guid>>,
    /// TID 到冻结动作集的 prepare 预留；同一锁内完成跨事务 Key 冲突检查与整批登记。
    prepare:                    Mutex<XHashMap<Guid, PreparedActions>>,
    /// collector、容量触发和事件异步发送使用的 runtime。
    rt:                         MultiTaskRuntime<()>,
    /// 是否允许维护入口执行 redb compact；不影响常规 collector 写入。
    enable_compact:             AtomicBool,
    /// 已发布到 overlay、等待 redb 数据文件提交和根 WAL 确认的 FIFO。
    waits:                      AsyncMutex<VecDeque<(BtreeOrdTabTr<C, Log>, XHashMap<Binary, KVActionLog>, <BtreeOrdTabTr<C, Log> as Transaction2Pc>::CommitConfirm)>>,
    /// FIFO 动作的近似累计字节数，用于容量触发，不是精确驻留内存指标。
    waits_size:                 AtomicUsize,
    /// 容量 collector 阈值。
    waits_limit:                usize,
    /// 定时 collector 间隔，单位毫秒。
    wait_timeout:               usize,
    /// 容量、定时和显式整理入口共用的单 collector owner 标记。
    collecting:                 AtomicBool,
    /// 可选提交确认事件通知器；不参与确认成功判定。
    notifier:                   Option<Sender<KVDBEvent<Guid>>>,
    /// 传给 redb 写事务的 quick-repair 开关；以提交性能换取更快恢复准备。
    enable_accelerated_repair:  bool,
}

/// Btree 单元子事务共享句柄。
///
/// 事务创建时固定共享 overlay 的 COW 根；redb-only Key 只有在 query/delete 时才建立完整点读
/// 基线，blind write 保留 `OverlayMissing`。clone 共享同一状态，不创建独立事务或新快照。
#[derive(Clone)]
pub struct BtreeOrdTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerBtreeOrdTabTr<C, Log>>);

#[derive(Clone, Copy)]
enum PrepareConflictKind {
    /// 返回普通可 rollback 错误，只暴露首个 Key 的诊断文本。
    Common,
    /// 返回结构化首冲突。
    First,
    /// 返回去重后的完整结构化冲突集合。
    All,
}

/// Btree 事务创建时能够证明的单 Key 逻辑基线。
#[derive(Clone)]
enum BtreeKeyBaseline {
    /// query/delete 或创建时 overlay 已取得完整逻辑状态；None 是 tombstone/逻辑不存在。
    Known(Option<Binary>),
    /// 创建时只知道 overlay 无 Key，不能把它解释为 redb 中也不存在。
    OverlayMissing,
}

/// 比较 Btree 基线与当前逻辑状态，同时兼容 overlay 和 redb 的所有权表示。
///
/// overlay 未变化或无关 COW 修改会共享同一 Binary allocation，先走 O(1) 身份快路。redb
/// 每次点读都会重新分配 Binary，collector 也可能把同一逻辑值从 overlay 搬到 redb；这两种
/// 情况必须回退到原始字节比较，不能因 Arc 地址不同误报冲突。真正的同值并发写和 ABA 已在
/// 调用本函数前由 `has_committed_after` 的 revision 检查捕获，活跃 snapshot 会阻止对应版本
/// 被 TTL 提前淘汰。本函数不分配、不持锁且不修改全局 COW 表的身份判等语义。
/// 证据见 BUG-KV-BTREE-REDB-BASELINE-001：
/// docs/KEY_VERSION_BTREE_REDB_BASELINE_BUG.md#bug-kv-btree-redb-baseline-001-index。
fn btree_baseline_state_equal(left: Option<&Binary>, right: Option<&Binary>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            Binary::binary_equal(left, right) || left.as_ref() == right.as_ref()
        },
        _ => false,
    }
}

/// 动作和冲突基线必须由同一个事务私有锁原子更新，禁止拆成两个可能撕裂的 Map。
#[derive(Clone)]
struct BtreeKeyState {
    action: KVActionLog,
    baseline: BtreeKeyBaseline,
}

// SAFETY: 外层只移动 Arc；事务状态、COW 根和 KeyState 由 SpinLock 保护，共享表另有同步域。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for BtreeOrdTabTr<C, Log> {}
// SAFETY: 所有共享可变字段均通过内部锁/原子访问，redb guard 不存入可跨线程裸指针。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for BtreeOrdTabTr<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> AsyncTransaction for BtreeOrdTabTr<C, Log> {
    type Output = ();
    type Error = KVTableTrError;

    fn is_writable(&self) -> bool {
        self.0.writable
    }

    fn is_concurrent_commit(&self) -> bool {
        false
    }

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
        let tr = self.clone();

        async move {
            // TID 必须已由事务管理器递归发布。rollback 只移除 prepare 预留并释放版本快照；
            // 事务私有 overlay 随最后一个事务 Arc 析构，不会写入共享 cache 或 redb。
            let transaction_uid = tr.get_transaction_uid().unwrap();
            let _ = tr.0.table.0.prepare.lock().remove(&transaction_uid);
            if let Some(context) = tr.0.version_context.as_ref() {
                context.release_snapshot();
            }

            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Transaction2Pc for BtreeOrdTabTr<C, Log> {
    type Tid = Guid;
    type Pid = Guid;
    type Cid = Guid;
    type PrepareOutput = Vec<u8>;
    type PrepareError = KVTableTrError;
    type ConfirmOutput = ();
    type ConfirmError = KVTableTrError;
    type CommitConfirm = KVDBCommitConfirm<C, Log>;

    fn is_require_persistence(&self) -> bool {
        self.0.persistence.load(Ordering::Relaxed)
    }

    fn require_persistence(&self) {
        self.0.persistence.store(true, Ordering::Relaxed);
    }

    fn is_concurrent_prepare(&self) -> bool {
        false
    }

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

    fn set_prepare_uid(&self, _uid: <Self as Transaction2Pc>::Pid) {

    }

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

    fn prepare(&self)
               -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>>
    {
        let tr = self.clone();

        async move {
            tr.prepare_registered(PrepareConflictKind::Common).await
        }.boxed()
    }

    fn prepare_conflicts(&self) -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        let tr = self.clone();

        async move {
            tr.prepare_registered(PrepareConflictKind::First).await
        }.boxed()
    }

    fn commit(&self, confirm: <Self as Transaction2Pc>::CommitConfirm)
              -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>>
    {
        // 进入节点 commit 表示根 WAL 门禁已经成功。此后任何节点提交失败均由事务框架按 Fatal
        // 处理，不能通过 rollback 撤销已经发布的根 WAL。
        let tr = self.clone();

        async move {
            let transaction_uid = tr.get_transaction_uid().unwrap();
            // publication 写锁把共享 overlay、cache_flags、版本 revision 和回执组成一个对
            // query_with_version 可见的原子发布区。锁内不执行 redb I/O 或用户确认回调。
            let publication = match tr.0.version_context.as_ref() {
                Some(context) => Some(context.versions().publication().write().await),
                None => None,
            };
            // 预提交成功后动作只存在于 prepare 表。按 TID remove 既取得冻结提交输入，也释放
            // 该事务的 Key 预留；同一事务不允许重复 commit。
            // 在线 prepare 和 WAL repair 的 prepare_repair 都必须先登记根 TID；repair 固定使用
            // Ordinary mode，所以 replay 跳过框架标准 prepare 仍不会合法地产生缺项。
            let expected_mode = tr
                .0
                .version_context
                .as_ref()
                .map(TableVersionContext::mode)
                .unwrap_or(PrepareMode::Ordinary);
            let prepared = {
                let mut prepare = tr.0.table.0.prepare.lock();
                take_prepared_for_commit(&mut prepare,
                                         &transaction_uid,
                                         expected_mode,
                                         tr.is_writable())
            };
            let actions = match prepared {
                Ok(Some(prepared)) => prepared.actions,
                Ok(None) => XHashMap::default(),
                Err(PreparedCommitError::ModeMismatch(prepared_mode)) => {
                    drop(publication);
                    if let Some(context) = tr.0.version_context.as_ref() {
                        context.release_snapshot();
                    }
                    return Err(KVTableTrError::new_transaction_error(
                        ErrorLevel::Fatal,
                        format!("Commit b-tree ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, expected_mode: {:?}, prepared_mode: {:?}, reason: prepared action protocol mismatch after entering non-rollbackable commit",
                                tr.0.table.name().as_str(),
                                tr.0.source,
                                transaction_uid,
                                expected_mode,
                                prepared_mode)));
                },
                Err(PreparedCommitError::Missing) => {
                    drop(publication);
                    if let Some(context) = tr.0.version_context.as_ref() {
                        context.release_snapshot();
                    }
                    return Err(KVTableTrError::new_transaction_error(
                        ErrorLevel::Fatal,
                        format!("Commit b-tree ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, expected_mode: {:?}, reason: prepared actions missing after entering non-rollbackable commit",
                                tr.0.table.name().as_str(),
                                tr.0.source,
                                transaction_uid,
                                expected_mode)));
                },
            };
            let has_writes = actions.values().any(|action| {
                matches!(action, KVActionLog::Write(_) | KVActionLog::DirtyWrite(_))
            });

            if has_writes {
                // revision 只为实际写分配，且在 publication 写锁内检查溢出。溢出意味着无法再
                // 维持单调版本顺序，必须在发布任何 Key 前返回 Fatal。
                let revision = match tr.0.version_context.as_ref() {
                    Some(context) => {
                        match context.versions().checked_next_revision() {
                            Some(revision) => Some(revision),
                            None => {
                                drop(publication);
                                context.release_snapshot();
                                return Err(KVTableTrError::new_transaction_error(
                                    ErrorLevel::Fatal,
                                    format!("Commit b-tree ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, reason: key version revision exhausted",
                                            tr.0.table.name().as_str(),
                                            tr.0.source,
                                            transaction_uid)));
                            },
                        }
                    },
                    None => None,
                };

                // 锁序固定为 publication -> cache_flags -> cache；prepare 已在上方取走并释放。
                // cache_flags 和 cache 必须一起发布：前者让旧 collector 识别 Key 是否已被后续
                // 事务覆盖，后者承载 value/tombstone 逻辑状态。collector 不能推进 revision。
                let mut committed_versions = Vec::new();
                let mut cache_flags = tr.0.table.0.cache_flags.lock();
                let mut cache = tr.0.table.0.cache.lock();
                if cache.ptr_eq(&tr.0.cache_ref.lock()) {
                    for (key, action) in &actions {
                        if matches!(action,
                                    KVActionLog::Write(_) | KVActionLog::DirtyWrite(_)) {
                            cache_flags.insert(key.clone(), transaction_uid.clone());
                        }
                    }
                    // prepare 已逐 Key 校验，commit 可保留等价的 COW 整根替换快路径。这里的
                    // ptr_eq 只优化发布成本，绝不是 prepare 冲突判定的替代品。
                    *cache = tr.0.cache_mut.lock().clone();
                } else {
                    for (key, action) in &actions {
                        match action {
                            KVActionLog::Write(value) | KVActionLog::DirtyWrite(value) => {
                                let _ = cache.upsert(key.clone(), value.clone(), false);
                                cache_flags.insert(key.clone(), transaction_uid.clone());
                            },
                            KVActionLog::Read => (),
                        }
                    }
                }

                if let (Some(context), Some(revision)) =
                    (tr.0.version_context.as_ref(), revision) {
                    // 同一事务所有写 Key 使用同一 TID/revision；None 发布 Delete，Some 发布
                    // Upsert。全部 Key 发布后再完成 revision 并追加事务自己的回执，禁止返回
                    // publication 锁释放后别的事务所覆盖的“缓存最新版本”。
                    for (key, action) in &actions {
                        let value = match action {
                            KVActionLog::Write(value) | KVActionLog::DirtyWrite(value) => value,
                            KVActionLog::Read => continue,
                        };
                        committed_versions.push(context.versions().publish(
                            tr.0.table.name(),
                            key.clone(),
                            value.as_ref(),
                            transaction_uid.clone(),
                            revision));
                    }
                    context.versions().complete_revision(revision);
                    if let Some(receipt) = context.receipt() {
                        receipt.append(committed_versions);
                    }
                }
            }

            // 后台 redb 写入、collector 和确认不得持有 publication、cache_flags 或 cache guard。
            drop(publication);
            if let Some(context) = tr.0.version_context.as_ref() {
                context.release_snapshot();
            }

            if tr.is_require_persistence() {
                // commit future 只登记异步 redb 写入并返回；redb write transaction 成功后才
                // 发送 Ok 成功信号。持久化失败不调用确认器，使根 WAL 保持未确认。详见
                // CONTRACT-CFM-001：docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only。
                let table_copy = tr.0.table.clone();
                // 当前实现忽略 runtime spawn 失败；该环境失效边界已归档为
                // LIMIT-ROOT-WAL-IO-001，本轮不扩大到调度接口重构。
                let _ = self.0.table.0.rt.spawn(async move {
                    let mut size = 0;
                    for (key, action) in &actions {
                        match action {
                            KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                                size += key.len() + value.len();
                            },
                            KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                                size += key.len();
                            },
                            KVActionLog::Read => (),
                        }
                    }

                    //注册待确认的已提交事务
                    table_copy
                        .0
                        .waits
                        .lock()
                        .await
                        .push_back((tr, actions, confirm));

                    let last_waits_size = table_copy.0.waits_size.fetch_add(size, Ordering::SeqCst); //更新待确认的已提交事务的大小计数
                    if last_waits_size + size >= table_copy.0.waits_limit {
                        //如果当前已注册的待确认的已提交事务大小已达限制，则立即整理
                        table_copy
                            .0
                            .waits_size
                            .store(0, Ordering::Relaxed); //重置待确认的已提交事务的大小计数

                        match collect_waits(&table_copy,
                                            None).await {
                            Err((collect_time, statistics)) => {
                                error!("Collect b-tree ordered table failed, table: {:?}, time: {:?}, statistics: {:?}, reason: out of size",
                                    table_copy.name().as_str(),
                                    collect_time,
                                    statistics);
                            },
                            Ok((collect_time, statistics)) => {
                                info!("Collect b-tree ordered table succeeded, table: {:?}, time: {:?}, statistics: {:?}, reason: out of size",
                                    table_copy.name().as_str(),
                                    collect_time,
                                    statistics);
                            },
                        }
                    }
                });
            }

            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Transaction2PcAllConflicts for BtreeOrdTabTr<C, Log> {
    fn precheck_all_conflicts(&self)
        -> BoxFuture<'_, Result<(), <Self as Transaction2Pc>::PrepareError>> {
        let tr = self.clone();
        async move {
            tr.precheck_versions().await
        }.boxed()
    }

    fn prepare_all_conflicts(&self)
        -> BoxFuture<'_, Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        let tr = self.clone();
        async move {
            tr.prepare_registered(PrepareConflictKind::All).await
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> UnitTransaction for BtreeOrdTabTr<C, Log> {
    type Status = Transaction2PcStatus;
    type Qos = TableTrQos;

    //有序B树表事务，一定是单元事务
    fn is_unit(&self) -> bool {
        true
    }

    fn get_status(&self) -> <Self as UnitTransaction>::Status {
        self.0.status.lock().clone()
    }

    fn set_status(&self, status: <Self as UnitTransaction>::Status) {
        *self.0.status.lock() = status;
    }

    fn qos(&self) -> <Self as UnitTransaction>::Qos {
        if self.is_require_persistence() {
            TableTrQos::Safe
        } else {
            TableTrQos::ThreadSafe
        }
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> SequenceTransaction for BtreeOrdTabTr<C, Log> {
    type Item = Self;

    //有序B树表事务，一定不是顺序事务
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
> TransactionTree for BtreeOrdTabTr<C, Log> {
    type Node = KVDBTransaction<C, Log>;
    type NodeInterator = KVDBChildTrList<C, Log>;

    //有序B树表事务，一定不是事务树
    fn is_tree(&self) -> bool {
        false
    }

    fn children_len(&self) -> usize {
        0
    }

    fn to_children(&self) -> Self::NodeInterator {
        KVDBChildTrList::new()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVAction for BtreeOrdTabTr<C, Log> {
    type Key = Binary;
    type Value = Binary;
    type Error = KVTableTrError;

    fn dirty_query(&self, key: <Self as KVAction>::Key) -> BoxFuture<Option<<Self as KVAction>::Value>>
    {
        // Btree 的 dirty_query 当前直接复用 query，并非不登记状态的脏读。外部协议要求一个
        // 事务要么只使用 dirty_*，要么只使用事务安全方法；混用不提供事务安全保证。
        self.query(key)
    }

    fn query(&self, key: <Self as KVAction>::Key) -> BoxFuture<Option<<Self as KVAction>::Value>>
    {
        let tr = self.clone();

        async move {
            // 读取优先级固定为事务私有 overlay -> redb。私有 overlay 中的 tombstone 是最终
            // 逻辑不存在，禁止回落；只有 Map 缺席才执行 redb 点读。
            let locked = tr.0.cache_mut.lock();
            let (value, baseline) = match locked.get(&key) {
                Some(Some(value)) => {
                    let value = value.clone();
                    (Some(value.clone()), BtreeKeyBaseline::Known(Some(value)))
                },
                Some(None) => (None, BtreeKeyBaseline::Known(None)),
                None => {
                    drop(locked);
                    // redb 读取结果只进入独立 KeyState 基线，禁止写入 cache_ref；后者必须保持
                    // 事务创建时 overlay 快照，供 commit COW 快路径和 collector 清理使用。
                    // KVAction::query 没有错误通道，因此 begin_read/open_table/get 错误按当前实现
                    // 降级为 None，并把基线保留为 OverlayMissing。严格传播错误的版本协议读取
                    // 走表级 query_committed，由 query_with_version 映射为可恢复 Common 错误。
                    let redb_value = if let Ok(trans) = tr.0.table.0.inner.read().begin_read() {
                        if let Ok(inner_table) = trans.open_table(DEFAULT_TABLE_NAME) {
                            match inner_table.get(&key) {
                                Ok(Some(value)) => Some(Some(value.value())),
                                Ok(None) => Some(None),
                                Err(_) => None,
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    match redb_value {
                        Some(value) => {
                            (value.clone(), BtreeKeyBaseline::Known(value))
                        },
                        _ => (None, BtreeKeyBaseline::OverlayMissing),
                    }
                },
            };
            // 同一 Key 只记录第一次确定的基线；后续读或写不能把事务起点改写为更新状态。
            tr.record_read_if_absent(key, baseline);
            value
        }.boxed()
    }

    fn dirty_upsert(&self,
                    key: <Self as KVAction>::Key,
                    value: <Self as KVAction>::Value) -> BoxFuture<Result<(), <Self as KVAction>::Error>>
    {
        self.upsert(key, value)
    }

    fn upsert(&self,
              key: <Self as KVAction>::Key,
              value: <Self as KVAction>::Value) -> BoxFuture<Result<(), <Self as KVAction>::Error>>
    {
        let tr = self.clone();

        async move {
            // blind upsert 不为 overlay-missing Key 额外读取 redb，避免每次写产生同步 I/O；它只
            // 记录 OverlayMissing，prepare 再用版本 revision、当前 overlay 和预留检查并发变化。
            let baseline = tr.snapshot_overlay_baseline(&key);
            tr.record_action(key.clone(),
                             KVActionLog::Write(Some(value.clone())),
                             baseline);

            //插入或更新指定的键值对
            let _ = tr.0.cache_mut.lock().upsert(key, Some(value), false);

            Ok(())
        }.boxed()
    }

    fn dirty_delete(&self, key: <Self as KVAction>::Key)
                    -> BoxFuture<Result<Option<<Self as KVAction>::Value>, <Self as KVAction>::Error>>
    {
        // 当前 Btree dirty_delete 与 delete 共用动作类型、旧缓存值和 tombstone 语义；
        // dirty 冲突差异不在本轮契约内，见 CONTRACT-BTREE-DELETE-001。
        self.delete(key)
    }

    fn delete(&self, key: <Self as KVAction>::Key)
              -> BoxFuture<Result<Option<<Self as KVAction>::Value>, <Self as KVAction>::Error>>
    {
        let tr = self.clone();

        async move {
            enum CachedDelete {
                Value(Binary),
                Tombstone,
                Missing,
            }

            let cached = {
                let mut locked = tr.0.cache_mut.lock();
                // OrdMap<Binary, Option<Binary>> 的返回值必须保留三态：最内层 Some 是缓存
                // 旧值，最内层 None 是已有 tombstone，最外层 None 才表示 Map 无 Key。
                // copy=true 时 Some(None) 当前不可达；仍按已有覆盖保守处理，禁止回读 redb。
                let cached = match locked.delete(&key, true) {
                    Some(Some(Some(value))) => CachedDelete::Value(value),
                    Some(Some(None)) | Some(None) => CachedDelete::Tombstone,
                    None => CachedDelete::Missing,
                };

                // 无论是否取得旧值都必须写 tombstone，防止查询回落到尚未物理删除的
                // redb 值。根 WAL 成功后 collector 才删除 redb，成功后按 cache_flags 清理。
                let _ = locked.upsert(key.clone(), None, false);
                cached
            };

            let (old_value, baseline) = match cached {
                CachedDelete::Value(value) => {
                    (Some(value.clone()), BtreeKeyBaseline::Known(Some(value)))
                },
                CachedDelete::Tombstone => (None, BtreeKeyBaseline::Known(None)),
                CachedDelete::Missing => {
                    // 缓存完全缺席时，中立读取 delete 执行时的 redb 快照。读取结果不得进入
                    // cache_ref。错误仍按冻结契约记录详细日志并返回 Ok(None)，但冲突基线保持
                    // OverlayMissing，不能把读取失败伪装成已确认不存在。
                    let redb_result: Result<Option<Binary>, (&'static str, String)> = (|| {
                        let trans = {
                            let inner = tr.0.table.0.inner.read();
                            inner.begin_read()
                        }
                            .map_err(|e| ("begin_read", format!("{:?}", e)))?;
                        let inner_table = trans
                            .open_table(DEFAULT_TABLE_NAME)
                            .map_err(|e| ("open_table", format!("{:?}", e)))?;
                        match inner_table.get(&key) {
                            Ok(Some(value)) => Ok(Some(value.value())),
                            Ok(None) => Ok(None),
                            Err(e) => Err(("get", format!("{:?}", e))),
                        }
                    })();

                    match redb_result {
                        Ok(value) => {
                            (value.clone(), BtreeKeyBaseline::Known(value))
                        },
                        Err((stage, reason)) => {
                            error!("Btree delete redb old-value read failed: stage={}, table={:?}, table_path={:?}, key={:?}, key_len={}, source={:?}, transaction_uid={:?}, reason={}, old_value=None, tombstone=retained",
                                   stage,
                                   tr.0.table.name().as_str(),
                                   tr.0.table.0.path,
                                   key,
                                   key.len(),
                                   tr.0.source,
                                   tr.get_transaction_uid(),
                                   reason);
                            (None, BtreeKeyBaseline::OverlayMissing)
                        },
                    }
                },
            };

            tr.record_action(key.clone(), KVActionLog::Write(None), baseline);
            Ok(old_value)
        }.boxed()
    }

    fn keys<'a>(&self,
                key: Option<<Self as KVAction>::Key>,
                descending: bool)
        -> BoxStream<'a, <Self as KVAction>::Key>
    {
        let transaction = self.clone();
        // 先固定事务 overlay 的 O(1) COW 根，再在 API 返回前建立 redb 读事务。两者均随
        // 流存活，因此首次 poll 前以及迭代期间的写入不会进入旧流。捕获过程不同时持有
        // cache_mut 与 redb 内部锁；它不提供跨两种存储的事务级全局线性化。
        // CONTRACT-ITER-001 / tests/iterator_snapshot_safety.rs。
        let cache_root = self.0.cache_mut.lock().clone();
        let mut cache_iterator = OrdMapSnapshot::new(cache_root, key.as_ref(), descending);
        let read_transaction = transaction.0.table.0.inner.read().begin_read();
        let stream = stream! {
            let trans = match read_transaction {
                Err(_e) => {
                    // BoxStream 没有错误通道；保持当前 begin_read 失败即空流的语义。
                    return;
                },
                Ok(trans) => {
                    trans
                },
            };

            let table = if let Ok(table) = trans.open_table(DEFAULT_TABLE_NAME)
            {
                table
            } else {
                //当前表还未创建完成，则只迭代缓存中的关键字
                while let Some((key, opt)) = cache_iterator.next_entry() {
                    //从迭代器获取到下一个关键字
                    if let Some(_value) = opt {
                        //只返回缓存中有值的关键字
                        yield key;
                    }
                }
                return;
            };
            let mut inner_transaction = InnerTransaction::OnlyRead(trans, transaction.0.table.name());
            if let Some(mut iterator) = inner_transaction.values_by_read(&table, key, descending)
            {
                let (min_size, _) = cache_iterator.size_hint();
                let mut ignores = HashMap::with_capacity(min_size);
                let mut cache_b = 2;
                let mut b = 2;
                let mut cache_key_value = None;
                let mut key_value = None;
                loop {
                    //从迭代器获取到关键字
                    cache_key_value = match cache_b {
                        0 => None, //不再获取关键字
                        1 => cache_key_value, //忽略获取关键字
                        _ => cache_iterator.next_entry(), //获取关键字
                    };
                    key_value = match b {
                        0 => None, //不再获取关键字
                        1 => key_value,   //忽略获取关键字
                        _ => {
                            //获取关键字
                            if descending {
                                //倒序
                                iterator.next_back()
                            } else {
                                //顺序
                                iterator.next()
                            }
                        },
                    };

                    match (&cache_key_value, &key_value) {
                        (Some((cache_k, opt)), Some(Ok((key_, _value)))) => {
                            //缓存和文件迭代器都有关键字
                            let k = key_.value();
                            if descending {
                                //倒序
                                if cache_k > &k {
                                    cache_b = 2;
                                    b = 1;

                                    if opt.is_some() {
                                        //只返回缓存中有值的关键字
                                        let (cache_k, _cache_v) = cache_key_value
                                            .take()
                                            .expect("cache entry must exist while it is consumed");
                                        ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的关键字
                                        yield cache_k
                                    }
                                } else if cache_k < &k {
                                    cache_b = 1;
                                    b = 2;

                                    if !ignores.contains_key(&k) {
                                        //在缓存中未迭代过的关键字，则返回
                                        yield k;
                                    }
                                } else {
                                    cache_b = 2;
                                    b = 2;

                                    if opt.is_some() {
                                        //只返回缓存中有值的关键字
                                        let (cache_k, _cache_v) = cache_key_value
                                            .take()
                                            .expect("cache entry must exist while it is consumed");
                                        ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的关键字
                                        yield cache_k
                                    }
                                }
                            } else {
                                //顺序
                                if cache_k < &k {
                                    cache_b = 2;
                                    b = 1;

                                    if opt.is_some() {
                                        //只返回缓存中有值的关键字
                                        let (cache_k, _cache_v) = cache_key_value
                                            .take()
                                            .expect("cache entry must exist while it is consumed");
                                        ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的关键字
                                        yield cache_k
                                    }
                                } else if cache_k > &k {
                                    cache_b = 1;
                                    b = 2;

                                    if !ignores.contains_key(&k) {
                                        //在缓存中未迭代过的关键字，则返回
                                        yield k;
                                    }
                                } else {
                                    cache_b = 2;
                                    b = 2;

                                    if opt.is_some() {
                                        //只返回缓存中有值的关键字
                                        let (cache_k, _cache_v) = cache_key_value
                                            .take()
                                            .expect("cache entry must exist while it is consumed");
                                        ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的关键字
                                        yield cache_k
                                    }
                                }
                            }

                        },
                        (None, Some(Ok((key_, _value)))) => {
                            //只有文件迭代器有关键字
                            cache_b = 0; //关闭缓存迭代器
                            b = 2;
                            let k = key_.value();

                            if !ignores.contains_key(&k) {
                                //在缓存中未迭代过的关键字，则返回
                                yield k;
                            }
                        },
                        (Some((_cache_k, opt)), None) => {
                            //只有缓存迭代器有关键字
                            cache_b = 2;
                            b = 0; //关闭文件迭代器

                            if opt.is_some() {
                                //只返回缓存中有值的关键字
                                let (cache_k, _cache_v) = cache_key_value
                                    .take()
                                    .expect("cache entry must exist while it is consumed");
                                ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的关键字
                                yield cache_k
                            }
                        },
                        _ => {
                            // redb iterator item Err 当前也会落入此分支并静默截断。BoxStream item
                            // 没有错误通道，处置策略尚未冻结，见 FIND-TABLE-003。
                            //迭代已结束
                            break;
                        },
                    }
                }
            }
        };

        stream.boxed()
    }

    fn values<'a>(&self,
                  key: Option<<Self as KVAction>::Key>,
                  descending: bool)
        -> BoxStream<'a, (<Self as KVAction>::Key, <Self as KVAction>::Value)>
    {
        let transaction = self.clone();
        // 与 keys 使用相同的双快照所有权；redb begin_read 仍可能同步短暂阻塞，且失败
        // 仍按既有无错误 item 的 API 表现为空流。
        let cache_root = self.0.cache_mut.lock().clone();
        let mut cache_iterator = OrdMapSnapshot::new(cache_root, key.as_ref(), descending);
        let read_transaction = transaction.0.table.0.inner.read().begin_read();
        let stream = stream! {
            let trans = match read_transaction {
                Err(_e) => {
                    // BoxStream 没有错误通道；保持当前 begin_read 失败即空流的语义。
                    return;
                },
                Ok(trans) => {
                    trans
                },
            };

            let table = if let Ok(table) = trans.open_table(DEFAULT_TABLE_NAME)
            {
                table
            } else {
                //当前表还未创建完成，则只迭代缓存中的键值对
                while let Some((key, opt)) = cache_iterator.next_entry() {
                    if let Some(value) = opt {
                        //只返回缓存中有值的键值对
                        yield (key, value);
                    }
                }
                return;
            };

            let mut inner_transaction = InnerTransaction::OnlyRead(trans, transaction.0.table.name());
            if let Some(mut iterator) = inner_transaction.values_by_read(&table, key, descending)
            {
                let (min_size, _) = cache_iterator.size_hint();
                let mut ignores = HashMap::with_capacity(min_size);
                let mut cache_b = 2;
                let mut b = 2;
                let mut cache_key_value = None;
                let mut key_value = None;
                loop {
                    //从迭代器获取到键值对
                    cache_key_value = match cache_b {
                        0 => None, //不再获取键值对
                        1 => cache_key_value, //忽略获取键值对
                        _ => cache_iterator.next_entry(), //获取键值对
                    };
                    key_value = match b {
                        0 => None, //不再获取键值对
                        1 => key_value,   //忽略获取键值对
                        _ => {
                            //获取键值对
                            if descending {
                                //倒序
                                iterator.next_back()
                            } else {
                                //顺序
                                iterator.next()
                            }
                        },
                    };

                    match (&cache_key_value, &key_value) {
                        (Some((cache_k, opt)), Some(Ok((key_, value_)))) => {
                            //缓存和文件迭代器都有键值对
                            let k = key_.value();
                            if descending {
                                //倒序
                                if cache_k > &k {
                                    cache_b = 2;
                                    b = 1;

                                    if opt.is_some() {
                                        //只返回缓存中有值的键值对
                                        let (cache_k, cache_v) = cache_key_value
                                            .take()
                                            .expect("cache entry must exist while it is consumed");
                                        let cache_v = cache_v
                                            .expect("a consumed cache value must not be a tombstone");
                                        ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的键值对
                                        yield (cache_k, cache_v)
                                    }
                                } else if cache_k < &k {
                                    cache_b = 1;
                                    b = 2;

                                    if !ignores.contains_key(&k) {
                                        //在缓存中未迭代过的键值对，则返回
                                        yield (k, value_.value());
                                    }
                                } else {
                                    cache_b = 2;
                                    b = 2;

                                    if opt.is_some() {
                                        //只返回缓存中有值的键值对
                                        let (cache_k, cache_v) = cache_key_value
                                            .take()
                                            .expect("cache entry must exist while it is consumed");
                                        let cache_v = cache_v
                                            .expect("a consumed cache value must not be a tombstone");
                                        ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的键值对
                                        yield (cache_k, cache_v)
                                    }
                                }
                            } else {
                                //顺序
                                if cache_k < &k {
                                    cache_b = 2;
                                    b = 1;

                                    if opt.is_some() {
                                        //只返回缓存中有值的键值对
                                        let (cache_k, cache_v) = cache_key_value
                                            .take()
                                            .expect("cache entry must exist while it is consumed");
                                        let cache_v = cache_v
                                            .expect("a consumed cache value must not be a tombstone");
                                        ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的键值对
                                        yield (cache_k, cache_v)
                                    }
                                } else if cache_k > &k {
                                    cache_b = 1;
                                    b = 2;

                                    if !ignores.contains_key(&k) {
                                        //在缓存中未迭代过的键值对，则返回
                                        yield (k, value_.value());
                                    }
                                } else {
                                    cache_b = 2;
                                    b = 2;

                                    if opt.is_some() {
                                        //只返回缓存中有值的键值对
                                        let (cache_k, cache_v) = cache_key_value
                                            .take()
                                            .expect("cache entry must exist while it is consumed");
                                        let cache_v = cache_v
                                            .expect("a consumed cache value must not be a tombstone");
                                        ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的键值对
                                        yield (cache_k, cache_v)
                                    }
                                }
                            }
                        },
                        (None, Some(Ok((key_, value_)))) => {
                            //只有文件迭代器有键值对
                            cache_b = 0; //关闭缓存迭代器
                            b = 2;
                            let k = key_.value();

                            if !ignores.contains_key(&k) {
                                //在缓存中未迭代过的键值对，则返回
                                yield (k, value_.value());
                            }
                        },
                        (Some((_cache_k, opt)), None) => {
                            //只有缓存迭代器有键值对
                            cache_b = 2;
                            b = 0; //关闭文件迭代器

                            if opt.is_some() {
                                //只返回缓存中有值的键值对
                                let (cache_k, cache_v) = cache_key_value
                                    .take()
                                    .expect("cache entry must exist while it is consumed");
                                let cache_v = cache_v
                                    .expect("a consumed cache value must not be a tombstone");
                                ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的键值对
                                yield (cache_k, cache_v)
                            }
                        },
                        _ => {
                            // 与 keys 相同，redb item Err 当前会被当作流结束；这不是“完整读取
                            // 成功”的证明，见 FIND-TABLE-003。
                            //迭代已结束
                            break;
                        },
                    }
                }
            }
        };

        stream.boxed()
    }

    fn lock_key(&self, _key: <Self as KVAction>::Key)
                -> BoxFuture<Result<(), <Self as KVAction>::Error>>
    {
        async move {
            Ok(())
        }.boxed()
    }

    fn unlock_key(&self, _key: <Self as KVAction>::Key)
                  -> BoxFuture<Result<(), <Self as KVAction>::Error>>
    {
        async move {
            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> BtreeOrdTabTr<C, Log> {
    /// 构建普通 Btree 子事务。
    ///
    /// 创建成本是一次共享 overlay COW 根 clone，不读取 redb，也不租用版本 revision 快照。
    /// `cache_ref` 与初始 `cache_mut` 共享节点，首次写由 pi_ordmap 写时复制。
    #[inline]
    fn new(source: Atom,
           is_writable: bool,
           is_persistent: bool,
           prepare_timeout: u64,
           commit_timeout: u64,
           table: BtreeOrderedTable<C, Log>) -> Self {
        // 只在 clone 根指针期间持 cache 锁，后续事务动作不占用表级 overlay 锁。
        let cache_ref = table.0.cache.lock().clone();
        let cache_mut = cache_ref.clone();
        let enable_accelerated_repair = table.0.enable_accelerated_repair;
        let inner = InnerBtreeOrdTabTr {
            source,
            tid: SpinLock::new(None),
            cid: SpinLock::new(None),
            status: SpinLock::new(Transaction2PcStatus::default()),
            writable: is_writable,
            persistence: AtomicBool::new(is_persistent),
            prepare_timeout,
            commit_timeout,
            cache_mut: SpinLock::new(cache_mut),
            cache_ref: SpinLock::new(cache_ref),
            table,
            key_states: SpinLock::new(XHashMap::default()),
            version_context: None,
            enable_accelerated_repair,
        };

        BtreeOrdTabTr(Arc::new(inner))
    }

    /// 构建数据库管理器装配的 Btree 事务。
    ///
    /// 全局 overlay guard 同时固定 COW 根和版本 revision。最终动作直接作用于事务私有
    /// `cache_mut`，不会为 blind write 额外读取 redb；逐 Key 基线与动作一起进入 KeyState。
    /// 先在 cache 锁内 clone 根并租用 revision，保证两者代表同一个 publication 边界；随后
    /// 立即释放表锁，再构造批量动作，避免大批次初始化长期阻塞其它 commit/query。
    pub(crate) fn new_managed(source: Atom,
                              is_writable: bool,
                              is_persistent: bool,
                              prepare_timeout: u64,
                              commit_timeout: u64,
                              table: BtreeOrderedTable<C, Log>,
                              versions: KeyVersions,
                              mode: PrepareMode,
                              expected: XHashMap<Binary, Version>,
                              receipt: Option<VersionReceipt>,
                              actions: XHashMap<Binary, KVActionLog>) -> Self {
        let cache_locked = table.0.cache.lock();
        let cache_ref = cache_locked.clone();
        let snapshot = versions.lease_current();
        drop(cache_locked);

        let mut cache_mut = cache_ref.clone();
        let mut key_states = XHashMap::default();
        for (key, action) in actions {
            let baseline = match cache_ref.get(&key) {
                Some(value) => BtreeKeyBaseline::Known(value.clone()),
                None => BtreeKeyBaseline::OverlayMissing,
            };
            match &action {
                KVActionLog::Write(value) | KVActionLog::DirtyWrite(value) => {
                    let _ = cache_mut.upsert(key.clone(), value.clone(), false);
                },
                KVActionLog::Read => (),
            }
            key_states.insert(key, BtreeKeyState {
                action,
                baseline,
            });
        }

        let version_context = TableVersionContext::new(versions,
                                                       snapshot,
                                                       mode,
                                                       expected,
                                                       receipt);
        let enable_accelerated_repair = table.0.enable_accelerated_repair;
        let inner = InnerBtreeOrdTabTr {
            source,
            tid: SpinLock::new(None),
            cid: SpinLock::new(None),
            status: SpinLock::new(Transaction2PcStatus::default()),
            writable: is_writable,
            persistence: AtomicBool::new(is_persistent),
            prepare_timeout,
            commit_timeout,
            cache_mut: SpinLock::new(cache_mut),
            cache_ref: SpinLock::new(cache_ref),
            table,
            key_states: SpinLock::new(key_states),
            version_context: Some(version_context),
            enable_accelerated_repair,
        };

        BtreeOrdTabTr(Arc::new(inner))
    }

    /// 只读取事务创建时的 overlay 根；缺席不代表 redb 不存在。
    fn snapshot_overlay_baseline(&self, key: &Binary) -> BtreeKeyBaseline {
        match self.0.cache_ref.lock().get(key) {
            Some(value) => BtreeKeyBaseline::Known(value.clone()),
            None => BtreeKeyBaseline::OverlayMissing,
        }
    }

    /// 首次读登记基线；同一 Key 已有动作时不能覆盖更早的事务基线。
    fn record_read_if_absent(&self, key: Binary, baseline: BtreeKeyBaseline) {
        let mut key_states = self.0.key_states.lock();
        if let HashMapEntry::Vacant(entry) = key_states.entry(key) {
            entry.insert(BtreeKeyState {
                action: KVActionLog::Read,
                baseline,
            });
        }
    }

    /// 更新最终动作但保留该 Key 在本事务中首次确定的冲突基线。
    fn record_action(&self,
                     key: Binary,
                     action: KVActionLog,
                     baseline: BtreeKeyBaseline) {
        let mut key_states = self.0.key_states.lock();
        match key_states.entry(key) {
            HashMapEntry::Occupied(mut entry) => {
                entry.get_mut().action = action;
            },
            HashMapEntry::Vacant(entry) => {
                entry.insert(BtreeKeyState {
                    action,
                    baseline,
                });
            },
        }
    }

    fn take_actions(&self) -> XHashMap<Binary, KVActionLog> {
        mem::replace(&mut *self.0.key_states.lock(), XHashMap::default())
            .into_iter()
            .map(|(key, state)| (key, state.action))
            .collect()
    }

    async fn precheck_versions(&self) -> Result<(), KVTableTrError> {
        let Some(context) = self.0.version_context.as_ref() else {
            return Ok(());
        };
        if context.mode() != PrepareMode::Versioned {
            return Ok(());
        }

        // 完整冲突模式 Phase 1 只比较外部期望版本，不点读 redb、不登记预留、不移动 KeyState。
        // publication 读锁保证本轮比较不会观察到一个 commit 的部分 Key 发布。
        let _publication = context.versions().publication().read().await;
        let mut conflicts = Vec::new();
        for (key, expected) in context.expected() {
            if context.versions().current_version(key).as_ref() != Some(expected) {
                conflicts.push(TableKey {
                    table: self.0.table.name(),
                    key: key.clone(),
                });
            }
        }
        if conflicts.is_empty() {
            Ok(())
        } else {
            Err(KVTableTrError::new_all_conflicts_error(conflicts))
        }
    }

    async fn prepare_registered(&self,
                                conflict_kind: PrepareConflictKind)
        -> Result<Option<Vec<u8>>, KVTableTrError> {
        if !self.is_writable() {
            return Ok(None);
        }

        // 固定顺序为 publication(read) -> 事务 KeyState clone -> 必要的 cache/redb 点读 ->
        // prepare。所有 redb 读发生在 prepare 锁之前，避免持有全表预留锁跨存储 I/O。
        let _publication = match self.0.version_context.as_ref() {
            Some(context) => Some(context.versions().publication().read().await),
            None => None,
        };
        let key_states = self.0.key_states.lock().clone();
        let actions: XHashMap<Binary, KVActionLog> = key_states
            .iter()
            .map(|(key, state)| (key.clone(), state.action.clone()))
            .collect();
        let mode = self
            .0
            .version_context
            .as_ref()
            .map(TableVersionContext::mode)
            .unwrap_or(PrepareMode::Ordinary);
        let mut conflict_keys = Vec::new();

        if let Some(context) = self.0.version_context.as_ref() {
            if context.mode() == PrepareMode::Versioned {
                for (key, expected) in context.expected() {
                    if context.versions().current_version(key).as_ref() != Some(expected) {
                        conflict_keys.push(key.clone());
                    }
                }
            }
        }

        // 每个可能冲突的 Key 都必须完成版本或值状态判断；COW 根 ptr_eq 不参与此循环，避免
        // “多个事务同时插入原先不存在 Key”被整根快路漏判。
        for (key, state) in &key_states {
            let require_state_check = !self.is_require_persistence()
                || !state.action.is_dirty_writed();
            if !require_state_check {
                continue;
            }

            if let Some(context) = self.0.version_context.as_ref() {
                // expected 匹配本身就是该 Key 的完整外部读基线；不再把 Btree 的
                // OverlayMissing 误解释为 redb 不存在，也不把当前值与另一版本拼接。
                if context.mode() == PrepareMode::Versioned
                    && context.expected().contains_key(key) {
                    continue;
                }
                if context
                    .versions()
                    .has_committed_after(key, context.snapshot_revision()) {
                    conflict_keys.push(key.clone());
                    continue;
                }
            }

            match &state.baseline {
                BtreeKeyBaseline::Known(expected) => {
                    let current = self.0.table.query_committed(key).map_err(|error| {
                        KVTableTrError::new_transaction_error(
                            ErrorLevel::Normal,
                            format!("Prepare b-tree ordered table failed, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, reason: read current logical value failed, detail: {:?}",
                                    self.0.table.name().as_str(),
                                    key,
                                    self.0.source,
                                    self.get_transaction_uid(),
                                    error))
                    })?;
                    if !btree_baseline_state_equal(expected.as_ref(), current.as_ref()) {
                        conflict_keys.push(key.clone());
                    }
                },
                BtreeKeyBaseline::OverlayMissing => {
                    // blind write 只证明创建时 overlay 无项。当前出现 value 或 tombstone 都是
                    // 可证明的并发变化；已被 collector 清走的提交由 revision 证据覆盖。
                    if self.0.table.0.cache.lock().get(key).is_some() {
                        conflict_keys.push(key.clone());
                    }
                },
            }
        }

        // 由不可变动作在锁外生成 WAL payload，缩短 prepare 全表锁临界区。
        let write_buf = self.prepare_output(&actions);
        let mut prepare = self.0.table.0.prepare.lock();
        let transaction_uid = self.get_transaction_uid().unwrap();
        // prepare map 的 TID 是表内冻结动作 owner。重复 TID 表示同表兄弟子节点或重复
        // prepare；在这里覆盖会让错误节点消费动作并静默丢写，因此必须在 WAL 前拒绝。
        if has_prepared_transaction(&prepare, &transaction_uid) {
            return Err(KVTableTrError::new_transaction_error(
                ErrorLevel::Normal,
                format!("Prepare b-tree ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, reason: duplicate prepared transaction uid",
                        self.0.table.name().as_str(),
                        self.0.source,
                        transaction_uid)));
        }
        for (key, action) in &actions {
            if has_prepared_conflict(&prepare, key, mode, action) {
                conflict_keys.push(key.clone());
            }
        }
        if !conflict_keys.is_empty() {
            return Err(self.prepare_conflict_error(conflict_kind, conflict_keys));
        }

        // 在同一个 prepare guard 下完成“检查所有其它预留 -> 登记本事务整批动作”。只有零冲突
        // 才清空事务 KeyState，故失败事务仍可由外部 rollback，并且不能出现部分登记。
        let _ = mem::replace(&mut *self.0.key_states.lock(), XHashMap::default());
        prepare.insert(transaction_uid, PreparedActions {
            mode,
            actions,
        });
        Ok(write_buf)
    }

    fn prepare_output(&self,
                      actions: &XHashMap<Binary, KVActionLog>) -> Option<Vec<u8>> {
        if !self.is_require_persistence() {
            // persistence 只表示是否写根 WAL。可写非持久事务仍需走 prepare/commit 释放预留并
            // 发布内存 overlay，但不会产生本节点的 WAL payload。
            return None;
        }
        let writed_count = actions
            .values()
            .filter(|action| matches!(action,
                                     KVActionLog::Write(_) | KVActionLog::DirtyWrite(_)))
            .count() as u64;
        if writed_count == 0 {
            return None;
        }

        let mut buf = Vec::new();
        self.0.table.init_table_prepare_output(&mut buf, writed_count);
        for (key, action) in actions {
            match action {
                KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                    self.0.table.append_key_value_to_table_prepare_output(&mut buf, key, None);
                },
                KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                    self.0.table.append_key_value_to_table_prepare_output(&mut buf,
                                                                          key,
                                                                          Some(value));
                },
                KVActionLog::Read => (),
            }
        }
        Some(buf)
    }

    fn prepare_conflict_error(&self,
                              conflict_kind: PrepareConflictKind,
                              keys: Vec<Binary>) -> KVTableTrError {
        let key = keys[0].clone();
        match conflict_kind {
            PrepareConflictKind::Common => {
                KVTableTrError::new_transaction_error(
                    ErrorLevel::Normal,
                    format!("Prepare b-tree ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, reason: committed state or prepared reservation changed",
                            self.0.table.name().as_str(),
                            key,
                            self.0.source,
                            self.get_transaction_uid()))
            },
            PrepareConflictKind::First => {
                KVTableTrError::new_conflicts_error(self.0.table.name(), key)
            },
            PrepareConflictKind::All => {
                KVTableTrError::new_all_conflicts_error(keys
                    .into_iter()
                    .map(|key| TableKey {
                        table: self.0.table.name(),
                        key,
                    })
                    .collect())
            },
        }
    }

    /// 在启动 WAL replay 中重建 Btree overlay 和 prepare 动作。
    ///
    /// 该入口信任已经校验的恢复日志，绕过普通版本、值状态和活跃预留冲突检查，并直接把
    /// value/tombstone 写入共享 overlay。它只能在数据库恢复调度保证的独占阶段调用，不能作为
    /// 业务 prepare 快路，也不能与正常事务并发。
    pub(crate) fn prepare_repair(&self, transaction_uid: Guid) {
        //获取事务的当前操作记录，并重置事务的当前操作记录
        let actions = self.take_actions();

        //在事务对应的有序B树表的临时缓存的根节点，执行操作记录中的所有写操作
        for (key, action) in &actions {
            match action {
                KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                    //执行插入或更新指定关键字的值的操作
                    self
                        .0
                        .table
                        .0
                        .cache
                        .lock()
                        .upsert(key.clone(), Some(value.clone()), false);
                },
                KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                    //执行删除指定关键字的值的操作，则标记删除
                    self
                        .0
                        .table
                        .0
                        .cache
                        .lock()
                        .upsert(key.clone(), None, false);
                },
                KVActionLog::Read => (), //忽略读操作
            }
        }

        //将事务的当前操作记录，写入表的预提交表
        self.0.table.0.prepare.lock().insert(transaction_uid, PreparedActions {
            mode: PrepareMode::Ordinary,
            actions,
        });
    }

    /// 清理已由 redb 成功持久化且仍属于对应 TID 的 overlay 项。
    ///
    /// `keys` 中的 TID 来自 collector 批次。只有 `cache_flags[key]` 仍等于该 TID 才删除，若
    /// 后续事务已经覆盖同 Key，则保留新 overlay。锁序是 `cache_flags -> cache`，且无 await。
    pub(crate) fn delete_cache(&self, keys: Vec<(<Self as KVAction>::Key, Option<Guid>)>) {
        //记录需要删除的缓存中的关键字，只用于有序B树表的临时缓存的根节点在当前事务执行过程中已改变
        let mut require_delete_keys = Vec::with_capacity(keys.len());

        //为了减少在锁内阻塞的时间，对需要删除的缓存中的关键字进行预处理
        let mut cache_flags = self
            .0
            .table
            .0
            .cache_flags
            .lock(); //锁住缓存标记
        for (key, transaction_uid) in &keys {
            if let HashMapEntry::Occupied(mut o) = cache_flags.entry(key.clone()) {
                if let Some(tid) = transaction_uid {
                    if o.get() == tid {
                        //如果当前需要删除的缓存中的关键字是由对应事务写入的，则删除
                        let _ = self.0.cache_mut.lock().delete(key, false);
                        require_delete_keys.push(key);
                        let _ = o.remove(); //从缓存标记中移除
                    }
                }
            }
        }

        //更新有序B树表的临时缓存的根节点
        {
            let mut locked = self.0.table.0.cache.lock();
            if !locked.ptr_eq(&self.0.cache_ref.lock()) {
                //有序B树表的临时缓存的根节点在当前事务执行过程中已改变，
                //一般是因为其它事务更新了与当前事务无关的关键字，
                //则将当前事务的修改直接作用在当前有序B树表的临时缓存中
                for key in require_delete_keys {
                    let _ = locked.delete(key, false);
                }
            } else {
                //有序B树表的临时缓存的根节点在当前事务执行过程中未改变，则用本次事务修改并提交成功的根节点替换有序B树表的临时缓存的根节点
                *locked = self.0.cache_mut.lock().clone();
            }
        }
    }
}

/// Btree 子事务的共享可变状态。
///
/// `cache_ref/cache_mut/key_states` 共同描述事务创建基线、候选结果和逐 Key 冲突事实，三者不能
/// 被简化成一个根指针。版本 snapshot 在 commit 或 rollback 恰好释放一次；事务外部 clone
/// 只延长这组状态的生命周期。
struct InnerBtreeOrdTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    /// 事务来源，用于管理器限流、事件和诊断。
    source:                     Atom,
    /// manager 在 start 时为整棵事务树统一发布的 TID。
    tid:                        SpinLock<Option<Guid>>,
    /// 根 WAL 记录及提交确认使用的共享 CID；非持久树可以没有 CID。
    cid:                        SpinLock<Option<Guid>>,
    /// 由事务管理器推进的节点 2PC 状态。
    status:                     SpinLock<Transaction2PcStatus>,
    /// 是否允许写动作；显式只读事务由 manager 在 prepare/commit 入口短路。
    writable:                   bool,
    /// 是否要求写根 WAL，不等价于 redb 数据文件是否存在。
    persistence:                AtomicBool,
    /// manager 观察预提交的超时，单位毫秒。
    prepare_timeout:            u64,
    /// manager 观察提交的超时，单位毫秒；不改变后台 redb 确认时机。
    commit_timeout:             u64,
    /// 在创建时共享 overlay 根上应用事务最终动作后的私有 COW 候选根。
    cache_mut:                  SpinLock<OrdMap<Tree<Binary, Option<Binary>>>>,
    /// 事务创建时的共享 overlay 快照；redb 点读基线绝不能回写此根。
    cache_ref:                  SpinLock<OrdMap<Tree<Binary, Option<Binary>>>>,
    /// 对应共享表句柄，保证事务/流存活时表状态不提前析构。
    table:                      BtreeOrderedTable<C, Log>,
    /// 每 Key 最终动作和首次确定的逻辑基线；同一锁避免动作/基线撕裂。
    key_states:                 SpinLock<XHashMap<Binary, BtreeKeyState>>,
    /// 可选版本 snapshot、外部期望版本、prepare 模式和 commit 回执汇聚器。
    version_context:            Option<TableVersionContext>,
    /// 创建事务时固定的 redb quick-repair 配置，供内部 redb 事务使用。
    enable_accelerated_repair:  bool,
}

/// 对 redb 原生事务的内部状态封装。
///
/// 该枚举服务低层直接 redb 操作，不是 pi_async_transaction 的 2PC 状态。variant 决定允许的
/// 读写/提交动作；其中 `WriteConflict` 和 `Repair` 持有读事务作为特殊流程标记，不能按普通
/// writable transaction 使用。
pub(crate) enum InnerTransaction {
    /// redb 只读快照和来源。
    OnlyRead(ReadTransaction, Atom),
    /// 可提交或回滚的 redb 写事务和来源。
    Writable(WriteTransaction, Atom),
    /// 已判定写冲突后保留的读事务上下文。
    WriteConflict(ReadTransaction, Atom),
    /// 数据修复流程持有的读事务上下文。
    Repair(ReadTransaction, Atom),
}

impl InnerTransaction {
    // 判断是否是只读事务
    pub fn is_only_read(&self) -> bool {
        if let InnerTransaction::OnlyRead(_, _) = self {
            true
        } else {
            false
        }
    }

    // 判断是否是可写事务
    pub fn is_writable(&self) -> bool {
        if let InnerTransaction::Writable(_, _) = self {
            true
        } else {
            false
        }
    }

    // 判断是否是写冲突事务
    pub fn is_write_conflict(&self) -> bool {
        if let InnerTransaction::WriteConflict(_, _) = self {
            true
        } else {
            false
        }
    }

    // 判断是否是修复表事务
    pub fn is_repair(&self) -> bool {
        if let InnerTransaction::Repair(_, _) = self {
            true
        } else {
            false
        }
    }

    // 获取指定关键字的值
    pub fn query(&self, key: &Binary) -> Option<Binary> {
        match self {
            InnerTransaction::OnlyRead(transaction, name) => {
                if let Ok(table) = transaction.open_table(DEFAULT_TABLE_NAME) {
                    match table.get(key) {
                        Err(e) => {
                            error!("Get inner transaction table value failed, table: {:?}, key: {:?}, reason: {:?}",
                                name.as_str(),
                                key,
                                e);
                                None
                        },
                        Ok(value) => {
                            if let Some(val) = value {
                                Some(val.value())
                            } else {
                                None
                            }
                        },
                    }
                } else {
                    None
                }
            },
            InnerTransaction::WriteConflict(transaction, name) => {
                if let Ok(table) = transaction.open_table(DEFAULT_TABLE_NAME) {
                    match table.get(key) {
                        Err(e) => {
                            error!("Get inner transaction table value failed, table: {:?}, key: {:?}, reason: {:?}",
                            name.as_str(),
                            key,
                            e);
                            None
                        },
                        Ok(value) => {
                            if let Some(val) = value {
                                Some(val.value())
                            } else {
                                None
                            }
                        },
                    }
                } else {
                    None
                }
            },
            InnerTransaction::Writable(transaction, name) => {
                if let Ok(table) = transaction.open_table(DEFAULT_TABLE_NAME) {
                    match table.get(key) {
                        Err(e) => {
                            error!("Get inner transaction table value failed, table: {:?}, key: {:?}, reason: {:?}",
                                name.as_str(),
                                key,
                                e);
                            None
                        },
                        Ok(value) => {
                            if let Some(val) = value {
                                Some(val.value())
                            } else {
                                None
                            }
                        },
                    }
                } else {
                    None
                }
            },
            InnerTransaction::Repair(_trans, _name) => {
                //修复时不允许查询
                None
            },
        }
    }

    // 写入指定关键字的值
    pub fn upsert(&mut self, key: Binary, value: Binary) -> IOResult<()> {
        match self {
            InnerTransaction::OnlyRead(_transaction, name) => {
                Err(Error::new(ErrorKind::Other,
                               format!("Upsert inner transaction table failed, table: {:?}, key: {:?}, reason: require write inner transaction",
                                       name.as_str(),
                                       key)))
            },
            InnerTransaction::WriteConflict(_transaction, name) => {
                Err(Error::new(ErrorKind::Other,
                               format!("Upsert inner transaction table failed, table: {:?}, key: {:?}, reason: require write inner transaction",
                                       name.as_str(),
                                       key)))
            },
            InnerTransaction::Writable(transaction, name) => {
                match transaction.open_table(DEFAULT_TABLE_NAME) {
                    Err(e) => {
                        Err(Error::new(ErrorKind::Other, format!("Upsert inner transaction table value failed, table: {:?}, key: {:?}, reason: {:?}",
                                                                 name.as_str(),
                                                                 key,
                                                                 e)))
                    },
                    Ok(mut table) => {
                        match table.insert(key.clone(), value) {
                            Err(e) => {
                                Err(Error::new(ErrorKind::Other, format!("Upsert inner transaction table value failed, table: {:?}, key: {:?}, reason: {:?}",
                                                                         name.as_str(),
                                                                         key,
                                                                         e)))
                            },
                            Ok(_) => {
                                Ok(())
                            },
                        }
                    },
                }
            },
            InnerTransaction::Repair(_trans, _name) => {
                //修复事务不允许直接修改，会被转化为一个可写事务
                Ok(())
            },
        }
    }

    // 删除指定关键字的值
    pub fn delete(&mut self, key: &Binary) -> IOResult<Option<Binary>> {
        match self {
            InnerTransaction::OnlyRead(_transaction, name) => {
                Err(Error::new(ErrorKind::Other,
                               format!("Delete inner transaction failed, table: {:?}, key: {:?}, reason: require write inner transaction",
                                       name.as_str(),
                                       key)))
            },
            InnerTransaction::WriteConflict(_transaction, name) => {
                Err(Error::new(ErrorKind::Other,
                               format!("Delete inner transaction failed, table: {:?}, key: {:?}, reason: require write inner transaction",
                                       name.as_str(), key)))
            },
            InnerTransaction::Writable(transaction, name) => {
                match transaction.open_table(DEFAULT_TABLE_NAME) {
                    Err(e) => {
                        Err(Error::new(ErrorKind::Other, format!("Delete inner transaction failed, table: {:?}, key: {:?}, reason: {:?}",
                                                                 name.as_str(),
                                                                 key,
                                                                 e)))
                    },
                    Ok(mut table) => {
                        match table.remove(key) {
                            Err(e) => {
                                Err(Error::new(ErrorKind::Other, format!("Delete inner transaction failed, table: {:?}, key: {:?}, reason: {:?}",
                                                                         name.as_str(),
                                                                         key,
                                                                         e)))
                            },
                            Ok(value) => {
                                if let Some(val) = value {
                                    Ok(Some(val.value()))
                                } else {
                                    Ok(None)
                                }
                            },
                        }
                    },
                }
            },
            InnerTransaction::Repair(_trans, _name) => {
                //修复事务不允许直接删除，会被转化为一个可写事务
                Ok(None)
            },
        }
    }

    /// 获取从只读事务中指定关键字开始的迭代器
    pub(crate) fn values_by_read<'a>(&'a self,
                                     table: &'a ReadOnlyTable<Binary, Binary>,
                                     key: Option<Binary>,
                                     descending: bool)
        -> Option<Range<'a, Binary, Binary>>
    {
        if let Some(key) = key {
            //指定了关键字
            match self {
                InnerTransaction::OnlyRead(_transaction, name) => {
                    let iterator = match if descending {
                        //倒序
                        table.range(..=key.clone())
                    } else {
                        //顺序
                        table.range(key.clone()..)
                    } {
                        Err(e) => {
                            error!("Take inner transaction table iterator failed, table: {:?}, key: {:?}, descending: {:?}, reason: {:?}",
                                name.as_str(),
                                key,
                                descending,
                                e);
                            return None;
                        },
                        Ok(iterator) => {
                            iterator
                        },
                    };

                    Some(iterator)
                },
                InnerTransaction::WriteConflict(_transaction, name) => {
                    let iterator = match if descending {
                        //倒序
                        table.range(..=key.clone())
                    } else {
                        //顺序
                        table.range(key.clone()..)
                    } {
                        Err(e) => {
                            error!("Take inner transaction table iterator failed, table: {:?}, key: {:?}, descending: {:?}, reason: {:?}",
                                name.as_str(),
                                key,
                                descending,
                                e);
                            return None;
                        },
                        Ok(iterator) => {
                            iterator
                        },
                    };

                    Some(iterator)
                },
                InnerTransaction::Writable(_, _) => {
                    None
                },
                InnerTransaction::Repair(_trans, _name) => {
                    //修复事务不允许迭代
                    None
                },
            }
        } else {
            //未指定关键字
            match self {
                InnerTransaction::OnlyRead(_transaction, name) => {
                    let iterator = match table.iter() {
                        Err(e) => {
                            error!("Take inner transaction table iterator failed, table: {:?}, key: None, reason: {:?}",
                                name.as_str(),
                                e);
                            return None;
                        },
                        Ok(iterator) => {
                            iterator
                        },
                    };

                    Some(iterator)
                },
                InnerTransaction::WriteConflict(_transaction, name) => {
                    let iterator = match table.iter() {
                        Err(e) => {
                            error!("Take inner transaction table iterator failed, table: {:?}, key: None, reason: {:?}",
                                name.as_str(),
                                e);
                            return None;
                        },
                        Ok(iterator) => {
                            iterator
                        },
                    };

                    Some(iterator)
                },
                InnerTransaction::Writable(_, _) => {
                    None
                },
                InnerTransaction::Repair(_trans, _name) => {
                    //修复事务不允许迭代
                    None
                },
            }
        }
    }

    /// 获取从可写事务中的指定关键字开始的迭代器
    pub fn values_by_write<'a>(&'a self,
                               table: &'a Table<'a, Binary, Binary>,
                               key: Option<Binary>,
                               descending: bool)
        -> Option<Range<'a, Binary, Binary>>
    {
        if let Some(key) = key {
            //指定了关键字
            match self {
                InnerTransaction::OnlyRead(_, _) => {
                    None
                },
                InnerTransaction::WriteConflict(_, _) => {
                    None
                },
                InnerTransaction::Writable(_transaction, name) => {
                    match if descending {
                        //倒序
                        table.range(..=key.clone())
                    } else {
                        //顺序
                        table.range(key.clone()..)
                    } {
                        Err(e) => {
                            error!("Get inner transaction table value failed, table: {:?}, kkey: {:?}, descending: {:?}, reason: {:?}",
                                name.as_str(),
                                key,
                                descending,
                                e);
                            None
                        },
                        Ok(iterator) => {
                            Some(iterator)
                        },
                    }
                },
                InnerTransaction::Repair(_trans, _name) => {
                    //修复事务不允许迭代
                    None
                },
            }
        } else {
            //未指定关键字
            match self {
                InnerTransaction::OnlyRead(_, _) => {
                    None
                },
                InnerTransaction::WriteConflict(_, _) => {
                    None
                },
                InnerTransaction::Writable(_transaction, name) => {
                    match table.iter() {
                        Err(e) => {
                            error!("Get inner transaction table value failed, table: {:?}, key: None, reason: {:?}",
                                    name.as_str(),
                                    e);
                            None
                        },
                        Ok(iterator) => {
                            Some(iterator)
                        },
                    }
                },
                InnerTransaction::Repair(_trans, _name) => {
                    //修复事务不允许迭代
                    None
                },
            }
        }
    }

    // 提交可写事务的所有写操作
    pub fn commit(self) -> IOResult<()> {
        if let InnerTransaction::Writable(mut transaction, name) = self {
            //当前是写事务
            if let Err(e) = transaction.commit() {
                Err(Error::new(ErrorKind::Other,
                               format!("Commit inner transaction table failed, table: {:?}, reason: {:?}",
                                       name.as_str(),
                                       e)))
            } else {
                Ok(())
            }
        } else {
            //忽略其它事务的提交
            Ok(())
        }
    }

    // 回滚可写事务的所有写操作
    pub fn rollback(self) -> IOResult<()> {
        if let InnerTransaction::Writable(mut transaction, name) = self {
            //当前是写事务
            if let Err(e) = transaction.abort() {
                Err(Error::new(ErrorKind::Other,
                               format!("Rollback inner transaction failed, table: {:?}, reason: {:?}",
                                       name.as_str(),
                                       e)))
            } else {
                Ok(())
            }
        } else {
            //忽略其它事务的提交
            Ok(())
        }
    }

    // 关闭非可写事务
    pub fn close(self) -> IOResult<()> {
        match self {
            InnerTransaction::OnlyRead(transaction, name) => {
                if let Err(e) = transaction.close() {
                    Err(Error::new(ErrorKind::Other,
                                   format!("Rollback inner transaction failed, table: {:?}, reason: {:?}",
                                           name.as_str(),
                                           e)))
                } else {
                    Ok(())
                }
            },
            InnerTransaction::WriteConflict(transaction, name) => {
                if let Err(e) = transaction.close() {
                    Err(Error::new(ErrorKind::Other,
                                   format!("Rollback inner transaction failed, table: {:?}, reason: {:?}",
                                           name.as_str(),
                                           e)))
                } else {
                    Ok(())
                }
            },
            _ => Ok(()),
        }
    }
}

/// 将已发布到共享 overlay 的事务批量写入 redb，成功后确认根 WAL 并清理旧 overlay。
///
/// `timeout=Some(ms)` 是定时入口，`None` 是容量入口。`collecting` 的 Acquire/Release CAS 保证
/// 同一表同时最多一个 collector owner；竞争失败返回零统计，不代表 FIFO 为空。一个批次只用
/// 一个 redb 写事务，事务数、成功动作数和字节数作为观察统计返回。
///
/// 当前实现持有 `waits` 异步锁完成排队批次的同步 redb 写入和 commit，producer 会异步等待
/// 该锁而不会同步占住 worker；慢磁盘仍会增加入队延迟。redb commit 失败不发送 confirm，已
/// 取出的事务依赖保留的根 WAL 在恢复流程重放。单 Key redb 操作失败后继续并最终可能确认的
/// 偏离已单独归档为 FIND-DUR-002，本轮不在注释任务中修复。
async fn collect_waits<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(table: &BtreeOrderedTable<C, Log>, timeout: Option<usize>)
    -> Result<(Duration, (usize, usize, usize)), (Duration, (usize, usize, usize))>
{
    if let Some(timeout) = timeout {
        //需要等待指定时间后，再开始整理
        table.0.rt.timeout(timeout).await;
    }

    // 与所有成功/错误出口的 Release store 配对，禁止定时和容量入口交叉处理同一 FIFO。
    if let Err(_) = table.0.collecting.compare_exchange(false,
                                                        true,
                                                        Ordering::Acquire,
                                                        Ordering::Relaxed) {
        //正在异步整理，则忽略本次异步整理
        return Ok((Instant::now().elapsed(), (0, 0, 0)));
    }

    //将有序B树表中等待写入redb的事务，写入redb
    let mut waits = VecDeque::new();
    let mut cache_keys = BTreeMap::new();
    let mut trs_len = 0;
    let mut keys_len = 0;
    let mut bytes_len = 0;

    let now = Instant::now();
    {
        // waits guard 冻结本批 FIFO。begin_write/open_table 在 pop 之前失败时队列保持原样；
        // 一旦 pop，后续批次级失败不原地重排，而由未确认根 WAL 承担恢复责任。
        let mut locked = table
            .0
            .waits
            .lock()
            .await;

        match table.0.inner.read().begin_write() {
            Err(e) => {
                //创建redb的写事务失败
                table
                    .0
                    .collecting
                    .store(false, Ordering::Release); //设置为已整理结束
                error!("Collect b-tree ordered table failed, table: {:?}, transactions: {}, keys: {}, bytes: {}, reason: {:?}",
                            table.name().as_str(),
                            trs_len,
                            keys_len,
                            bytes_len,
                            e);

                return Err((now.elapsed(), (trs_len, keys_len, bytes_len)));
            },
            Ok(mut transaction) => {
                //创建redb的写事务成功
                transaction.set_quick_repair(table.0.enable_accelerated_repair); //设置redb写事务是否打开快速修复
                let mut inner_table = match transaction.open_table(DEFAULT_TABLE_NAME) {
                    Err(e) => {
                        table
                            .0
                            .collecting
                            .store(false, Ordering::Release); //设置为已整理结束
                        error!("Collect b-tree ordered table failed, table: {:?}, transactions: {}, keys: {}, bytes: {}, reason: {:?}",
                            table.name().as_str(),
                            trs_len,
                            keys_len,
                            bytes_len,
                            e);

                        return Err((now.elapsed(), (trs_len, keys_len, bytes_len)));
                    },
                    Ok(inner_table) => {
                       inner_table
                    },
                };

                // FIFO 顺序就是同一 redb 写事务内的动作顺序；若同 Key 在本批出现多次，
                // cache_keys 只保留最后一个事务 TID，与 redb 最终逻辑状态一致。
                while let Some((wait_tr, actions, confirm)) = locked.pop_front()
                {
                    let transaction_uid = wait_tr.get_transaction_uid();
                    for (key, actions) in actions.iter() {
                        match actions {
                            KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                                //统计删除了有序B树表中指定关键字的值
                                if let Err(e) = inner_table.remove(key) {
                                    // 当前实现偏离：单条删除失败后仍继续，批次随后可能提交并发送
                                    // Ok 成功信号。这不是最终/最佳语义，且不得被理解为确认器允许
                                    // Err；见 docs/REVIEW_FINDINGS.md#find-dur-002。
                                    error!("Delete key-value pair of redb table failed, table: {:?}, key: {:?}, reason: {:?}",
                                                table.name().as_str(),
                                                key,
                                                e);
                                    continue;
                                }

                                keys_len += 1;
                                bytes_len += key.len();
                            },
                            KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                                //统计插入或更新了有序B树表中指定关键字的值
                                if let Err(e) = inner_table.insert(key, value) {
                                    // 当前实现偏离：单条写入失败后仍继续，批次随后可能提交并发送
                                    // Ok 成功信号。这不是最终/最佳语义，且不得被理解为确认器允许
                                    // Err；见 docs/REVIEW_FINDINGS.md#find-dur-002。
                                    error!("Upsert key-value pair of redb table failed, table: {:?}, key: {:?}, reason: {:?}",
                                                table.name().as_str(),
                                                key,
                                                e);
                                    continue;
                                }

                                keys_len += 1;
                                bytes_len += key.len() + value.len();
                            },
                            KVActionLog::Read => (), //忽略读操作
                        }

                        //记录需要在持久化提交成功后，可能从缓存中清理的关键字
                        cache_keys
                            .insert(key.clone(),
                                    transaction_uid.clone());
                    }

                    trs_len += 1;
                    waits.push_back((wait_tr, confirm));
                }
                drop(inner_table); //在持久化提交前必须关闭redb表

                if let Err(e) = transaction.commit() {
                    // redb 提交失败后有意不调用 confirm；根 WAL 保留，供重试或启动恢复。
                    // 详见 CONTRACT-CFM-001：docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only。
                    table
                        .0
                        .collecting
                        .store(false, Ordering::Release); //设置为已整理结束
                    error!("Collect b-tree ordered table failed, table: {:?}, transactions: {}, keys: {}, bytes: {}, reason: {:?}",
                                table.name().as_str(),
                                trs_len,
                                keys_len,
                                bytes_len,
                                e);

                    return Err((now.elapsed(), (trs_len, keys_len, bytes_len)));
                }
            },
        }
    }

    // 已离开 waits 和 redb transaction 临界区。redb 事务提交成功后才发送 Ok 成功信号；
    // 有/无 notifier 只改变事件报告，不改变协议。事件发送可能异步等待，但此时数据已稳定，
    // 不会回滚 redb；confirm 自身返回错误只记录/上报，不能伪造另一种持久化结果。
    if let Some(notifier) = table.0.notifier.as_ref() {
        //指定了监听器
        for (wait_tr, confirm) in waits {
            if let Err(e) = confirm(wait_tr.get_transaction_uid().unwrap(),
                                    wait_tr.get_commit_uid().unwrap(),
                                    Ok(())) {
                notifier.send(KVDBEvent::CommitFailed(wait_tr.get_source(),
                                                      wait_tr.0.table.name(),
                                                      KVDBTableType::BtreeOrdTab,
                                                      wait_tr.get_transaction_uid().unwrap(),
                                                      wait_tr.get_commit_uid().unwrap()))
                    .await;
                error!("Commit b-tree ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: {:?}",
                    wait_tr.0.table.name().as_str(),
                    wait_tr.0.source,
                    wait_tr.get_transaction_uid(),
                    wait_tr.get_prepare_uid(),
                    e);
            } else {
                notifier.send(KVDBEvent::ConfirmCommited(wait_tr.get_source(),
                                                         wait_tr.0.table.name(),
                                                         KVDBTableType::BtreeOrdTab,
                                                         wait_tr.get_transaction_uid().unwrap(),
                                                         wait_tr.get_commit_uid().unwrap()))
                    .await;
            }
        }
    } else {
        //未指定监听器
        for (wait_tr, confirm) in waits {
            if let Err(e) = confirm(wait_tr.get_transaction_uid().unwrap(),
                                    wait_tr.get_commit_uid().unwrap(),
                                    Ok(())) {
                error!("Commit b-tree ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: {:?}",
                    wait_tr.0.table.name().as_str(),
                    wait_tr.0.source,
                    wait_tr.get_transaction_uid(),
                    wait_tr.get_prepare_uid(),
                    e);
            }
        }
    }
    table.0.collecting.store(false, Ordering::Release); //设置为已整理结束

    // 先释放 collector owner，再清理已经持久化的 overlay。并发新 commit 会改写 cache_flags；
    // delete_cache 只删除 TID 仍匹配的项，因此旧批次不会清掉新 value/tombstone。确认先于清理
    // 不影响逻辑读：两层此时表示相同最终值，overlay 只是暂时多占内存。
    let clean_cache_transaction = table.transaction(Atom::from("Collect_waits_cache"),
                      false,
                      false,
                      5000,
                      5000);
    clean_cache_transaction
        .delete_cache(cache_keys.into_iter().collect());

    Ok((now.elapsed(), (trs_len, keys_len, bytes_len)))
}

#[cfg(test)]
mod iterator_read_transaction_tests {
    //! Btree 流持有和释放 redb `ReadTransaction` 的精确白盒测试。
    //!
    //! redb 在活动读事务存在时明确拒绝 compact。测试使用生产 `BtreeOrdTabTr::keys`
    //! 创建流，以该错误作为资源存活门禁；流取消或耗尽后 compact 必须立即恢复。测试直接
    //! 构造表内部对象，避免启动与本断言无关的永久 collector 任务。

    use std::{
        fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicBool, AtomicUsize},
        time::{SystemTime, UNIX_EPOCH},
    };

    use futures::{executor::block_on, StreamExt};
    use pi_async_rt::rt::multi_thread::MultiTaskRuntimeBuilder;
    use pi_bon::{Encode, WriteBuffer};
    use pi_store::commit_logger::CommitLogger;
    use redb::CompactionError;

    use super::*;

    type TestTable = BtreeOrderedTable<usize, CommitLogger>;

    struct TempRoot(PathBuf);

    impl TempRoot {
        fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time must be after UNIX_EPOCH")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "pi_db_btree_read_guard_{}_{}",
                process::id(),
                nanos
            ));
            fs::create_dir_all(&path).expect("temporary Btree directory must be created");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn bon_usize(value: usize) -> Binary {
        let mut buffer = WriteBuffer::new();
        value.encode(&mut buffer);
        Binary::new(buffer.bytes)
    }

    fn build_table(root: &TempRoot) -> TestTable {
        let path = root.path().join(DEFAULT_TABLE_FILE_NAME);
        let database = TableBuilder::new()
            .create(path.clone())
            .expect("test redb database must open");
        let rt = MultiTaskRuntimeBuilder::default()
            .init_worker_size(2)
            .build();
        BtreeOrderedTable(Arc::new(InnerBtreeOrderedTable {
            name: Atom::from("iterator_read_guard"),
            path,
            inner: RwLock::new(database),
            cache: Mutex::new(OrdMap::new(None)),
            cache_flags: Mutex::new(XHashMap::default()),
            prepare: Mutex::new(XHashMap::default()),
            rt,
            enable_compact: AtomicBool::new(false),
            waits: AsyncMutex::new(VecDeque::new()),
            waits_size: AtomicUsize::new(0),
            waits_limit: 1024 * 1024,
            wait_timeout: 60_000,
            collecting: AtomicBool::new(false),
            notifier: None,
            enable_accelerated_repair: false,
        }))
    }

    fn assert_read_transaction_active(table: &TestTable, label: &str) {
        let error = match table.0.inner.write().compact() {
            Ok(_) => panic!("{label}: compact unexpectedly ignored active read"),
            Err(error) => error,
        };
        assert!(
            matches!(error, CompactionError::TransactionInProgress),
            "{label}: expected TransactionInProgress, observed {error:?}"
        );
    }

    fn assert_read_transaction_released(table: &TestTable, label: &str) {
        table
            .0
            .inner
            .write()
            .compact()
            .unwrap_or_else(|error| panic!("{label}: read transaction remained active: {error:?}"));
    }

    /// 0 poll、部分消费和正常耗尽均必须解除 redb 活动读事务门禁。
    #[test]
    fn test_btree_stream_releases_redb_read_transaction_at_every_exit() {
        let root = TempRoot::new();
        let table = build_table(&root);
        let transaction = table.transaction(Atom::from("read guard owner"), true, false, 5_000, 5_000);
        block_on(transaction.upsert(bon_usize(1), bon_usize(11)))
            .expect("overlay setup must succeed");

        let stream = transaction.keys(None, false);
        assert_read_transaction_active(&table, "zero-poll stream");
        drop(stream);
        assert_read_transaction_released(&table, "zero-poll drop");

        let mut stream = transaction.keys(None, false);
        assert_eq!(block_on(stream.next()), Some(bon_usize(1)));
        assert_read_transaction_active(&table, "partially consumed stream");
        drop(stream);
        assert_read_transaction_released(&table, "partial drop");

        let mut stream = transaction.keys(None, false);
        assert_eq!(block_on(stream.next()), Some(bon_usize(1)));
        assert_eq!(block_on(stream.next()), None);
        assert_read_transaction_released(&table, "normally exhausted stream");
        drop(stream);

        drop(transaction);
        drop(table);
        drop(root);
    }
}
