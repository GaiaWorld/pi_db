//! 数据库内部 Meta 表及其 2PC 子事务实现。
//!
//! Meta 表以 COW `OrdMap` 保存“表名编码 -> `KVTableMeta` 编码”，并以独立 `LogFile` 保存
//! 已提交表定义。DDL 通过根事务访问本模块；外部不得直接构造 Meta 表事务。提交先在
//! publication 写门内发布内存根与 Key 版本，再异步把动作批量写入表日志；只有表日志成功
//! 后才调用确认器，失败时保留根 WAL 供启动修复。

use std::mem;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::collections::{VecDeque, hash_map::Entry as HashMapEntry};
use std::sync::{Arc,
                atomic::{AtomicBool, AtomicUsize, Ordering}};

use parking_lot::Mutex;
use futures::{future::{FutureExt, BoxFuture},
              stream::{StreamExt, BoxStream}};
use async_lock::Mutex as AsyncMutex;
use async_channel::Sender;
use async_stream::stream;
use log::{debug, info, error};
use pi_async_rt::{lock::spin_lock::SpinLock,
                  rt::{AsyncRuntime,
                       multi_thread::MultiTaskRuntime}};
use pi_atom::Atom;
use pi_guid::Guid;
use pi_hash::XHashMap;
use pi_ordmap::{ordmap::OrdMap, asbtree::Tree};
use pi_async_transaction::{AsyncTransaction,
                           Transaction2Pc,
                           Transaction2PcAllConflicts,
                           UnitTransaction,
                           SequenceTransaction,
                           TransactionTree,
                           TransactionError,
                           AsyncCommitLog,
                           ErrorLevel,
                           manager_2pc::Transaction2PcStatus};
use pi_ordmap::ordmap::ImOrdMap;
use pi_store::log_store::log_file::{PairLoader,
                                    LogMethod,
                                    LogFile};

use crate::{Binary, KVAction, TableTrQos, KVActionLog, KVDBCommitConfirm, KVTableTrError,
            TableKey,
            db::{KVDBTransaction, KVDBChildTrList},
            key_version::{KeyVersions,
                          PrepareMode,
                          PreparedActions,
                          TableVersionContext,
                          Version,
                          VersionReceipt,
                          binary_state_equal,
                          has_prepared_conflict},
            tables::{KVTable, ordmap_snapshot::OrdMapSnapshot},
            utils::KVDBEvent,
            KVDBTableType};

///
/// 默认的日志文件延迟提交的超时时长，单位ms
///
const DEFAULT_LOG_FILE_COMMIT_DELAY_TIMEOUT: usize = 1000;

/// 数据库表目录使用的持久化、有序 Meta 表共享句柄。
///
/// `root` 是当前进程内已提交表定义的权威 COW 根，`prepare` 记录已通过冲突检查但尚未 commit
/// 的事务动作。clone 只增加内部 `Arc` 引用。该类型始终具有独立日志文件；DDL 的建表/删表
/// 完整事务原子性是当前已归档限制，不能仅凭内存 Meta 可见就推断目录和全部文件已原子完成。
#[derive(Clone)]
pub struct MetaTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerMetaTable<C, Log>>);

// SAFETY: 内部共享状态由 Mutex/AsyncMutex/原子类型、线程安全 runtime 和 LogFile 保护；外层
// 只移动 Arc owner，不暴露可变引用或裸指针。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for MetaTable<C, Log> {}
// SAFETY: 所有 `&self` 可变访问均经过上述同步原语；DDL/事务调用顺序属于协议约束。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for MetaTable<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVTable for MetaTable<C, Log> {
    type Name = Atom;
    type Tr = MetaTabTr<C, Log>;
    type Error = KVTableTrError;

    fn name(&self) -> <Self as KVTable>::Name {
        self.0.name.clone()
    }

    fn path(&self) -> Option<&Path> {
        Some(self.0.log_file.path())
    }

    #[inline]
    fn is_persistent(&self) -> bool {
        true
    }

    fn is_ordered(&self) -> bool {
        true
    }

    fn len(&self) -> usize {
        self.0.root.lock().size()
    }

    fn size(&self) -> u64 {
        let root_copy = self.0.root.lock().clone();
        root_copy.full_bytes_size()
    }

    fn transaction(&self,
                   source: Atom,
                   is_writable: bool,
                   is_persistent: bool,
                   prepare_timeout: u64,
                   commit_timeout: u64) -> Self::Tr {
        MetaTabTr::new(source,
                       is_writable,
                       is_persistent,
                       prepare_timeout,
                       commit_timeout,
                       self.clone())
    }

    fn ready_collect(&self) -> BoxFuture<Result<(), Self::Error>> {
        let table = self.clone();

        async move {
            let now = Instant::now();
            match table.0.log_file.split().await {
                Err(e) => {
                    //强制创建新的元信息表可写日志文件失败，则立即返回元信息表准备整理错误
                    return Err(KVTableTrError::new_transaction_error(ErrorLevel::Normal,
                                                                     format!("Ready collect meta table failed, path: {:?}, table: {:?}, reason: {:?}",
                                                                             table.0.log_file.path(),
                                                                             table.0.name.as_str(),
                                                                             e)));
                },
                Ok(writed_log_index) => {
                    //强制创建新的元信息表可写日志文件成功
                    info!("Ready collect meta table succeeded, time: {:?}, path: {:?}, table: {:?}, writed_log_index: {}",
                        now.elapsed(),
                        table.0.log_file.path(),
                        table.0.name.as_str(),
                        writed_log_index);
                    Ok(())
                },
            }
        }.boxed()
    }

    fn collect(&self) -> BoxFuture<Result<(), Self::Error>> {
        let table = self.clone();

        async move {
            let now = Instant::now();
            match table.0.log_file.collect(1024 * 1024,
                                           32 * 1024,
                                           false).await {
                Err(e) => {
                    //整理元信息表的只读日志文件失败，则立即返回元信息表整理错误
                    return Err(KVTableTrError::new_transaction_error(ErrorLevel::Normal,
                                                                     format!("Collect meta table failed, path: {:?}, table: {:?}, reason: {:?}",
                                                                        table.0.log_file.path(),
                                                                        table.0.name.as_str(),
                                                                        e)));
                },
                Ok((size, len)) => {
                    //整理元信息表的只读日志文件成功
                    info!("Collect meta table succeeded, time: {:?}, path: {:?}, table: {:?}, file_size: {}, file_len: {}",
                        now.elapsed(),
                        table.0.log_file.path(),
                        table.0.name.as_str(),
                        size,
                        len);
                    Ok(())
                },
            }
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> MetaTable<C, Log> {
    /// 在不创建事务的前提下读取当前已提交 COW 根；调用方负责 publication 同步。
    pub(crate) fn query_committed(&self, key: &Binary) -> Option<Binary> {
        self.0.root.lock().get(key).cloned()
    }

    /// 打开并加载一个 Meta 表，然后启动提交待确认队列的永久整理任务。
    ///
    /// `path` 指向表日志目录；`log_file_limit`/`block_limit` 控制日志文件；`load_buf_len` 和
    /// `is_checksum` 控制启动加载；`waits_limit`/`wait_timeout` 控制已发布事务何时批量写表
    /// 日志。打开或加载失败会 panic，因此该构造器只供已校验的数据库启动路径使用。
    ///
    /// 返回前日志已加载进 COW 根，但后台整理任务没有 shutdown 接口并会持有表 clone。任务
    /// 每轮串行 drain `waits`；它不参与根 WAL append，只有表日志成功后才发送提交确认。
    pub async fn new<P: AsRef<Path>>(rt: MultiTaskRuntime<()>,
                                     path: P,
                                     name: Atom,
                                     log_file_limit: usize,
                                     block_limit: usize,
                                     init_log_file_index: Option<usize>,
                                     load_buf_len: u64,
                                     is_checksum: bool,
                                     waits_limit: usize,
                                     wait_timeout: usize,
                                     notifier: Option<Sender<KVDBEvent<Guid>>>) -> Self {
        let root = Mutex::new(OrdMap::new(None));
        let prepare = Mutex::new(XHashMap::default());

        //打开指定的日志文件，并加载日志文件的内容到元信息表的内存表中
        match LogFile::open(rt.clone(),
                            path.as_ref().to_path_buf(),
                            block_limit,
                            log_file_limit,
                            init_log_file_index).await {
            Err(e) => {
                //打开日志文件失败，则立即抛出异常
                panic!("Open meta table failed, table: {:?}, path: {:?}, reason: {:?}",
                       name.as_str(),
                       path.as_ref(),
                       e);
            },
            Ok(log_file) => {
                //打开日志文件成功
                let waits = AsyncMutex::new(VecDeque::new());
                let waits_size = AtomicUsize::new(0);
                let collecting = AtomicBool::new(false);
                let inner = InnerMetaTable {
                    name: name.clone(),
                    root,
                    prepare,
                    rt,
                    waits,
                    waits_size,
                    waits_limit,
                    wait_timeout,
                    collecting,
                    log_file,
                    notifier,
                };

                let table = MetaTable(Arc::new(inner));

                //加载指定的日志文件的内容到元信息表的内存表
                let now = Instant::now();
                let mut loader = MetaTableLoader::new(table.clone());
                if let Err(e) = table.0.log_file.load(&mut loader,
                                                      None,
                                                      load_buf_len,
                                                      is_checksum).await {
                    //加载指定的日志文件失败，则立即抛出异常
                    panic!("Load meta table failed, table: {:?}, path: {:?}, reason: {:?}",
                           name.as_str(),
                           path.as_ref(),
                           e);
                }
                info!("Load meta table succeeded, table: {:?}, path: {:?}, files: {}, keys: {}, bytes: {}, time: {:?}",
                    name.as_str(),
                    path.as_ref(),
                    loader.log_files_len(),
                    loader.keys_len(),
                    loader.bytes_len(),
                    now.elapsed());

                //启动元信息表的提交待确认事务的定时整理
                let table_copy = table.clone();
                let _ = table.0.rt.spawn(async move {
                    let table_ref = &table_copy;
                    loop {
                        match collect_waits(table_ref,
                                            Some(table_copy.0.wait_timeout)).await {
                            Err((collect_time, statistics)) => {
                                error!("Collect meta table failed, table: {:?}, time: {:?}, statistics: {:?}, reason: out of time",
                                    table_copy.name().as_str(),
                                    collect_time,
                                    statistics);
                            },
                            Ok((collect_time, statistics)) => {
                                debug!("Collect meta table succeeded, table: {:?}, time: {:?}, statistics: {:?}, reason: out of time",
                                    table_copy.name().as_str(),
                                    collect_time,
                                    statistics);
                            },
                        }
                    }
                });

                table
            },
        }
    }
}

/// Meta 表的共享状态及锁所有权。
///
/// 同步锁只保护短内存临界区；`waits` 使用异步锁，因为 collector 会在持有该锁时执行表日志
/// `delay_commit().await`。当前锁顺序是版本 publication（表外）-> `prepare` -> `root`，提交
/// 不反向取得 publication；collector 不访问这三把锁。
struct InnerMetaTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    // 逻辑表名，正常数据库固定为 `.tables_meta`。
    name:           Atom,
    // 当前已提交数据根；clone 是稳定 COW 快照，写操作必须在锁内替换或合并。
    root:           Mutex<OrdMap<Tree<Binary, Binary>>>,
    // TID -> 已预留动作；prepare 原子检查并插入，commit/rollback 按同一 TID 移除。
    prepare:        Mutex<XHashMap<Guid, PreparedActions>>,
    // 驱动表日志打开、定时 collector 和异步确认流程的 runtime。
    rt:             MultiTaskRuntime<()>,
    // 内存根已发布、仍待写表日志并确认的 FIFO；元素同时保活事务和根确认回调。
    waits:          AsyncMutex<VecDeque<(MetaTabTr<C, Log>, XHashMap<Binary, KVActionLog>, <MetaTabTr<C, Log> as Transaction2Pc>::CommitConfirm)>>,
    // 新入队动作的近似累计 bytes，用于触发 size collector；触发时先归零。
    waits_size:     AtomicUsize,
    // `waits_size` 达到该值时立即尝试整理。
    waits_limit:    usize,
    // 定时整理间隔，单位毫秒。
    wait_timeout:   usize,
    // size/timer 两种触发器共享的单 collector owner 标志。
    collecting:     AtomicBool,
    // Meta 的独立数据日志；不等同于根 CommitLogger/WAL。
    log_file:       LogFile,
    // 可选观测事件通道；不参与提交正确性或确认判定。
    notifier:       Option<Sender<KVDBEvent<Guid>>>,
}

// SAFETY: 字段分别由同步原语或其线程安全类型保护，泛型 Log 只经 trait 约束持有在事务/确认器
// 中；没有未同步裸内存。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for InnerMetaTable<C, Log> {}
// SAFETY: 共享引用无法绕过 root/prepare/waits 的锁和原子 collector owner。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for InnerMetaTable<C, Log> {}

/// 一棵根事务树中的 Meta 表单元子事务。
///
/// 创建时 `root_ref` 固定已提交基线，`root_mut` 是事务私有 COW 根，`actions` 记录 Read、
/// Write 或 DirtyWrite。prepare 逐 Key 比较基线、当前根、版本 revision 和其它 prepared
/// 预留；commit 才把私有状态发布到表根。clone 共享同一事务状态，不产生新事务或新快照。
#[derive(Clone)]
pub struct MetaTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerMetaTabTr<C, Log>>);

/// 同一冲突事实面向三种上层 prepare API 的错误投影方式。
#[derive(Clone, Copy)]
enum PrepareConflictKind {
    /// 普通 prepare：返回可恢复 Normal 文本错误。
    Common,
    /// `prepare_conflicts`：只返回首个表/Key。
    First,
    /// `prepare_all_conflicts`：返回当前表收集到的全部 Key。
    All,
}

// SAFETY: 外层只持 Arc；内部所有可变状态由 SpinLock/AtomicBool 或表级同步原语保护。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for MetaTabTr<C, Log> {}
// SAFETY: clone/共享引用不会产生事务私有根的无同步可变别名。并发动作顺序仍由外部协议保证。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for MetaTabTr<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> AsyncTransaction for MetaTabTr<C, Log> {
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
            // rollback 只撤销尚未发布的 prepared 预留并释放版本快照租约；事务私有 COW 根
            // 随最后一个 Arc 释放。commit 已经发布的数据不属于该回滚路径。
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
> Transaction2Pc for MetaTabTr<C, Log> {
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
               -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
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
              -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        let tr = self.clone();

        async move {
            let transaction_uid = tr.get_transaction_uid().unwrap();
            // publication write 使元信息根和全部 Key 版本相对于 query_with_version 一次可见。
            // prepare 锁只用于取走预留，不能延伸到根发布或后续异步 LogFile 路径。
            let publication = match tr.0.version_context.as_ref() {
                Some(context) => Some(context.versions().publication().write().await),
                None => None,
            };
            let actions = tr
                .0
                .table
                .0
                .prepare
                .lock()
                .remove(&transaction_uid)
                .map(|prepared| prepared.actions)
                .unwrap_or_default();
            let has_writes = actions.values().any(|action| {
                matches!(action, KVActionLog::Write(_) | KVActionLog::DirtyWrite(_))
            });

            if has_writes {
                // revision 只为实际写动作分配；纯读事务不会推进版本时钟或产生回执。
                let revision = match tr.0.version_context.as_ref() {
                    Some(context) => {
                        match context.versions().checked_next_revision() {
                            Some(revision) => Some(revision),
                            None => {
                                drop(publication);
                                context.release_snapshot();
                                return Err(KVTableTrError::new_transaction_error(
                                    ErrorLevel::Fatal,
                                    format!("Commit meta table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, reason: key version revision exhausted",
                                            tr.0.table.name().as_str(),
                                            tr.0.source,
                                            transaction_uid)));
                            },
                        }
                    },
                    None => None,
                };

                let mut committed_versions = Vec::new();
                let mut root = tr.0.table.0.root.lock();
                if root.ptr_eq(&tr.0.root_ref) {
                    // 仅 commit 保留 COW 整根替换；prepare 已经逐 Key 完成状态冲突检查。
                    *root = tr.0.root_mut.lock().clone();
                } else {
                    for (key, action) in &actions {
                        match action {
                            KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                                let _ = root.delete(key, false);
                            },
                            KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                                let _ = root.upsert(key.clone(), value.clone(), false);
                            },
                            KVActionLog::Read => (),
                        }
                    }
                }

                if let (Some(context), Some(revision)) =
                    (tr.0.version_context.as_ref(), revision) {
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

            drop(publication);
            if let Some(context) = tr.0.version_context.as_ref() {
                context.release_snapshot();
            }

            if tr.is_require_persistence() {
                // commit future 只登记异步表日志写入并返回；最终 LogFile 成功后才发送 Ok
                // 成功信号。持久化失败不调用确认器，使根 WAL 保持未确认。详见
                // CONTRACT-CFM-001：docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only。
                let table_copy = tr.0.table.clone();
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
                    table_copy.0.waits.lock().await.push_back((tr, actions, confirm)); //注册待确认的已提交事务

                    let last_waits_size = table_copy.0.waits_size.fetch_add(size, Ordering::SeqCst); //更新待确认的已提交事务的大小计数
                    if last_waits_size + size >= table_copy.0.waits_limit {
                        //如果当前已注册的待确认的已提交事务大小已达限制，则立即整理
                        table_copy.0.waits_size.store(0, Ordering::Relaxed); //重置待确认的已提交事务的大小计数

                        match collect_waits(&table_copy,
                                            None).await {
                            Err((collect_time, statistics)) => {
                                error!("Collect meta table failed, table: {:?}, time: {:?}, statistics: {:?}, reason: out of size",
                                    table_copy.name().as_str(),
                                    collect_time,
                                    statistics);
                            },
                            Ok((collect_time, statistics)) => {
                                info!("Collect meta table succeeded, table: {:?}, time: {:?}, statistics: {:?}, reason: out of size",
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
> Transaction2PcAllConflicts for MetaTabTr<C, Log> {
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
> UnitTransaction for MetaTabTr<C, Log> {
    type Status = Transaction2PcStatus;
    type Qos = TableTrQos;

    //元信息表事务，一定是单元事务
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
> SequenceTransaction for MetaTabTr<C, Log> {
    type Item = Self;

    //元信息表事务，一定不是顺序事务
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
> TransactionTree for MetaTabTr<C, Log> {
    type Node = KVDBTransaction<C, Log>;
    type NodeInterator = KVDBChildTrList<C, Log>;

    //元信息表事务，一定不是事务树
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
> KVAction for MetaTabTr<C, Log> {
    type Key = Binary;
    type Value = Binary;
    type Error = KVTableTrError;

    fn dirty_query(&self, key: <Self as KVAction>::Key)
                   -> BoxFuture<Option<<Self as KVAction>::Value>> {
        let tr = self.clone();

        async move {
            if let Some(value) = tr.0.root_mut.lock().get(&key) {
                //指定关键值存在
                return Some(value.clone());
            }

            None
        }.boxed()
    }

    fn query(&self, key: <Self as KVAction>::Key)
             -> BoxFuture<Option<<Self as KVAction>::Value>> {
        let tr = self.clone();

        async move {
            let mut actions_locked = tr.0.actions.lock();

            if let None = actions_locked.get(&key) {
                //在事务内还未未记录指定关键字的操作，则记录对指定关键字的读操作
                let _ = actions_locked.insert(key.clone(), KVActionLog::Read);
            }

            if let Some(value) = tr.0.root_mut.lock().get(&key) {
                //指定关键值存在
                return Some(value.clone());
            }

            None
        }.boxed()
    }

    fn dirty_upsert(&self,
                    key: <Self as KVAction>::Key,
                    value: <Self as KVAction>::Value)
                    -> BoxFuture<Result<(), <Self as KVAction>::Error>> {
        let tr = self.clone();

        async move {
            //记录对指定关键字的最新插入或更新操作
            let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::DirtyWrite(Some(value.clone())));

            //插入或更新指定的键值对
            let _ = tr.0.root_mut.lock().upsert(key, value, false);

            Ok(())
        }.boxed()
    }

    fn upsert(&self,
              key: <Self as KVAction>::Key,
              value: <Self as KVAction>::Value)
              -> BoxFuture<Result<(), <Self as KVAction>::Error>> {
        let tr = self.clone();

        async move {
            //记录对指定关键字的最新插入或更新操作
            let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::Write(Some(value.clone())));

            //插入或更新指定的键值对
            let _ = tr.0.root_mut.lock().upsert(key, value, false);

            Ok(())
        }.boxed()
    }

    fn dirty_delete(&self, key: <Self as KVAction>::Key)
        -> BoxFuture<Result<Option<<Self as KVAction>::Value>, <Self as KVAction>::Error>> {
        let tr = self.clone();

        async move {
            //记录对指定关键字的最新删除操作，并增加写操作计数
            let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::DirtyWrite(None));

            // `copy=false` 是内部 Meta 表的既定语义：删除事务私有根中的表定义，但不返回
            // 旧元数据 Binary。命中与未命中最终都返回 Ok(None)，DDL 结果不能据此判断
            // 定义原先是否存在。见 docs/SEMANTIC_CONTRACTS.md#contract-action-001 与
            // docs/REVIEW_FINDINGS.md#find-ordered-delete-001。
            if let Some(Some(value)) = tr.0.root_mut.lock().delete(&key, false) {
                //指定关键字存在
                return Ok(Some(value));
            }

            Ok(None)
        }.boxed()
    }

    fn delete(&self, key: <Self as KVAction>::Key)
              -> BoxFuture<Result<Option<<Self as KVAction>::Value>, <Self as KVAction>::Error>> {
        let tr = self.clone();

        async move {
            //记录对指定关键字的最新删除操作，并增加写操作计数
            let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::Write(None));

            // `copy=false` 是内部 Meta 表的既定语义：删除事务私有根中的表定义，但不返回
            // 旧元数据 Binary。命中与未命中最终都返回 Ok(None)，DDL 结果不能据此判断
            // 定义原先是否存在。见 docs/SEMANTIC_CONTRACTS.md#contract-action-001 与
            // docs/REVIEW_FINDINGS.md#find-ordered-delete-001。
            if let Some(Some(value)) = tr.0.root_mut.lock().delete(&key, false) {
                //指定关键字存在
                return Ok(Some(value));
            }

            Ok(None)
        }.boxed()
    }

    fn keys<'a>(&self,
                key: Option<<Self as KVAction>::Key>,
                descending: bool)
                -> BoxStream<'a, <Self as KVAction>::Key> {
        // 元信息流固定调用返回前的事务私有 COW 根；后续 DDL 对 root_mut 的修改不会改变
        // 已创建流。这里只修复快照所有权，不改变 DDL 当前非完整原子性边界。
        // CONTRACT-ITER-001 / tests/iterator_snapshot_safety.rs。
        let root = self.0.root_mut.lock().clone();
        let mut iterator = OrdMapSnapshot::new(root, key.as_ref(), descending);

        let stream = stream! {
            while let Some(key) = iterator.next_key() {
                // 从 owner 保活的创建时快照获取下一个 owned key。
                yield key;
            }
        };

        stream.boxed()
    }

    fn values<'a>(&self,
                  key: Option<<Self as KVAction>::Key>,
                  descending: bool)
                  -> BoxStream<'a, (<Self as KVAction>::Key, <Self as KVAction>::Value)> {
        // 与 keys 使用同一 owner 模型；根锁在构造完成后释放，不跨 yield 持有。
        let root = self.0.root_mut.lock().clone();
        let mut iterator = OrdMapSnapshot::new(root, key.as_ref(), descending);

        let stream = stream! {
            while let Some((key, value)) = iterator.next_entry() {
                // 从 owner 保活的创建时快照获取下一个 owned 键值对。
                yield (key, value);
            }
        };

        stream.boxed()
    }

    fn lock_key(&self, _key: <Self as KVAction>::Key)
                -> BoxFuture<Result<(), <Self as KVAction>::Error>> {
        async move {
            Ok(())
        }.boxed()
    }

    fn unlock_key(&self, _key: <Self as KVAction>::Key)
                  -> BoxFuture<Result<(), <Self as KVAction>::Error>> {
        async move {
            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> MetaTabTr<C, Log> {
    // 构建一个元信息表事务
    #[inline]
    fn new(source: Atom,
           is_writable: bool,
           is_persistent: bool,
           prepare_timeout: u64,
           commit_timeout: u64,
           table: MetaTable<C, Log>) -> Self {
        let root_ref = table.0.root.lock().clone();

        let inner = InnerMetaTabTr {
            source,
            tid: SpinLock::new(None),
            cid: SpinLock::new(None),
            status: SpinLock::new(Transaction2PcStatus::default()),
            writable: is_writable,
            persistence: AtomicBool::new(is_persistent),
            prepare_timeout,
            commit_timeout,
            root_mut: SpinLock::new(root_ref.clone()),
            root_ref,
            table,
            actions: SpinLock::new(XHashMap::default()),
            version_context: None,
        };

        MetaTabTr(Arc::new(inner))
    }

    /// 构建由数据库管理器装配的事务，并在同一根 guard 内固定数据快照和版本 revision。
    pub(crate) fn new_managed(source: Atom,
                              is_writable: bool,
                              is_persistent: bool,
                              prepare_timeout: u64,
                              commit_timeout: u64,
                              table: MetaTable<C, Log>,
                              versions: KeyVersions,
                              mode: PrepareMode,
                              expected: XHashMap<Binary, Version>,
                              receipt: Option<VersionReceipt>,
                              actions: XHashMap<Binary, KVActionLog>) -> Self {
        let root_locked = table.0.root.lock();
        let root_ref = root_locked.clone();
        let snapshot = versions.lease_current();
        drop(root_locked);
        let mut root_mut = root_ref.clone();
        for (key, action) in &actions {
            match action {
                KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                    let _ = root_mut.upsert(key.clone(), value.clone(), false);
                },
                KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                    let _ = root_mut.delete(key, false);
                },
                KVActionLog::Read => (),
            }
        }
        let version_context = TableVersionContext::new(versions,
                                                       snapshot,
                                                       mode,
                                                       expected,
                                                       receipt);
        let inner = InnerMetaTabTr {
            source,
            tid: SpinLock::new(None),
            cid: SpinLock::new(None),
            status: SpinLock::new(Transaction2PcStatus::default()),
            writable: is_writable,
            persistence: AtomicBool::new(is_persistent),
            prepare_timeout,
            commit_timeout,
            root_mut: SpinLock::new(root_mut),
            root_ref,
            table,
            actions: SpinLock::new(actions),
            version_context: Some(version_context),
        };

        MetaTabTr(Arc::new(inner))
    }

    async fn precheck_versions(&self) -> Result<(), KVTableTrError> {
        // 根 `prepare_with_version` 的阶段一：只检查外部 read-set，完整收集本表不匹配项。
        // publication 读门保证 value/version 发布期间不会读到中间状态；这里不取得 prepare 锁。
        let Some(context) = self.0.version_context.as_ref() else {
            return Ok(());
        };
        if context.mode() != PrepareMode::Versioned {
            return Ok(());
        }

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
        // 只读表事务没有动作需要预留，也不生成表 WAL 片段。
        if !self.is_writable() {
            return Ok(None);
        }

        // 锁序固定为 publication(read) -> prepare；publication guard 覆盖版本、当前根和预留
        // 三类检查，commit 只能在 guard 释放后取得 publication(write)，避免检查后发布穿插。
        let _publication = match self.0.version_context.as_ref() {
            Some(context) => Some(context.versions().publication().read().await),
            None => None,
        };
        let actions = self.0.actions.lock().clone();
        let mode = self
            .0
            .version_context
            .as_ref()
            .map(TableVersionContext::mode)
            .unwrap_or(PrepareMode::Ordinary);
        let mut conflict_keys = Vec::new();

        if let Some(context) = self.0.version_context.as_ref() {
            if context.mode() == PrepareMode::Versioned {
                // 阶段二仍重复检查 read-set：阶段一到本表 prepare 之间可能已有其它事务提交。
                for (key, expected) in context.expected() {
                    if context.versions().current_version(key).as_ref() != Some(expected) {
                        conflict_keys.push(key.clone());
                    }
                }
            }
        }

        // 不以根指针相同作为跳过条件；每个非 dirty 动作都比较创建时与当前逻辑值状态。
        let current_root = self.0.table.0.root.lock().clone();
        for (key, action) in &actions {
            if action.is_dirty_writed() {
                continue;
            }
            if let Some(context) = self.0.version_context.as_ref() {
                if context
                    .versions()
                    .has_committed_after(key, context.snapshot_revision()) {
                    conflict_keys.push(key.clone());
                    continue;
                }
            }
            if !binary_state_equal(self.0.root_ref.get(key), current_root.get(key)) {
                conflict_keys.push(key.clone());
            }
        }

        let write_buf = self.prepare_output(&actions);
        let mut prepare = self.0.table.0.prepare.lock();
        // prepared-vs-prepared 检查与当前事务预留插入由同一同步锁串行化；不能把循环和 insert
        // 拆到两个临界区，否则两个首次插入相同 Key 的事务可能同时通过。
        for (key, action) in &actions {
            if has_prepared_conflict(&prepare, key, mode, action) {
                conflict_keys.push(key.clone());
            }
        }
        if !conflict_keys.is_empty() {
            return Err(self.prepare_conflict_error(conflict_kind, conflict_keys));
        }

        // 只有全部冲突检查通过才清空事务动作并转移所有权；失败时 actions 保留供 rollback
        // 关闭事务。外部不得在同一事务上再次 prepare。
        let _ = mem::replace(&mut *self.0.actions.lock(), XHashMap::default());
        prepare.insert(self.get_transaction_uid().unwrap(), PreparedActions {
            mode,
            actions,
        });
        Ok(write_buf)
    }

    fn prepare_output(&self,
                      actions: &XHashMap<Binary, KVActionLog>) -> Option<Vec<u8>> {
        // Meta 始终是持久化表，但纯读仍返回 None；None 表示本表没有根 WAL 数据片段，不表示
        // 整棵根事务必然只读或无需由 manager 完成生命周期。
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
        // 调用方只在 keys 非空时进入。All 模式保留本表完整集合；根 manager 最终再跨表归一化。
        let key = keys[0].clone();
        match conflict_kind {
            PrepareConflictKind::Common => {
                KVTableTrError::new_transaction_error(
                    ErrorLevel::Normal,
                    format!("Prepare meta table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, reason: committed state or prepared reservation changed",
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

    /// 为启动 repair 重建已由根 WAL 判定为 committed 的 Meta 动作。
    ///
    /// 该内部入口刻意跳过普通冲突检查：先把 WAL 动作直接作用于当前根，再以指定 TID 放入
    /// `prepare`，使后续 replay commit 沿正常清理/确认结构完成。只能由受信 repair 调用，不能
    /// 用于在线业务事务，也不会创建版本协议上下文。
    pub(crate) fn prepare_repair(&self, transaction_uid: Guid) {
        //获取事务的当前操作记录，并重置事务的当前操作记录
        let actions = mem::replace(&mut *self.0.actions.lock(), XHashMap::default());

        //在事务对应的表的根节点，执行操作记录中的所有写操作
        for (key, action) in &actions {
            match action {
                KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                    //执行插入或更新指定关键字的值的操作
                    self
                        .0
                        .table
                        .0
                        .root
                        .lock()
                        .upsert(key.clone(), value.clone(), false);
                },
                KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                    //执行删除指定关键字的值的操作
                    self.0.table.0.root.lock().delete(key, false);
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
}

/// Meta 子事务的共享状态。
///
/// `root_ref` 与版本 lease 在 managed 构造时同一根锁窗口内取得，保证数据基线和
/// `snapshot_revision` 对应同一观察点。`root_mut`/`actions` 是事务私有逻辑状态；表级
/// `prepare` 才是跨事务可见的预留。
struct InnerMetaTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    source:             Atom,                                                                       // 诊断事件源，不参与冲突身份。
    tid:                SpinLock<Option<Guid>>,                                                     // 根 manager 在 prepare 前分配并传播的事务 ID。
    cid:                SpinLock<Option<Guid>>,                                                     // 根 WAL 提交确认占位/回执使用的 commit ID。
    status:             SpinLock<Transaction2PcStatus>,                                             // 由事务框架推进的 2PC 状态。
    writable:           bool,                                                                       // 创建时固定；false 时 prepare 立即短路。
    persistence:        AtomicBool,                                                                 // Meta 写是否要求进入根 WAL；可由父事务聚合提升。
    prepare_timeout:    u64,                                                                        // 预提交超时，单位毫秒。
    commit_timeout:     u64,                                                                        // 提交超时，单位毫秒。
    root_mut:           SpinLock<OrdMap<Tree<Binary, Binary>>>,                                     // 应用本事务动作后的私有 COW 根。
    root_ref:           OrdMap<Tree<Binary, Binary>>,                                               // 创建事务瞬间的只读冲突基线。
    table:              MetaTable<C, Log>,                                                          // 表共享 owner，保证事务/stream 期间表存活。
    actions:            SpinLock<XHashMap<Binary, KVActionLog>>,                                    // 每 Key 最终动作；prepare 成功后转移到表级预留。
    version_context:    Option<TableVersionContext>,                                                 // managed 模式的 lease/revision/expected/receipt。
}

/// 按“新日志优先”规则把 Meta 日志文件集合恢复为一个内存根的启动 loader。
///
/// `removed` 防止旧文件中的已删除 Key 复活；已在 root 中出现的 Key 也不重复加载。
struct MetaTableLoader<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    statistics:         XHashMap<PathBuf, (u64, u64)>,  //加载统计信息，包括关键字数量和键值对的字节数
    removed:            XHashMap<Vec<u8>, ()>,          //已删除关键字表
    table:              MetaTable<C, Log>,              //元信息表
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> PairLoader for MetaTableLoader<C, Log> {
    fn is_require(&self, _log_file: Option<&PathBuf>, key: &Vec<u8>) -> bool {
        //不在已删除关键字表中且不在元信息表的内存表中的关键字，才允许被加载
        !self
            .removed
            .contains_key(key)
            &&
            self
                .table
                .0
                .root
                .lock()
                .get(&Binary::new(key.clone()))
                .is_none()
    }

    fn load(&mut self,
            log_file: Option<&PathBuf>,
            _method: LogMethod,
            key: Vec<u8>,
            value: Option<Vec<u8>>) {
        if let Some(value) = value {
            //插入或更新指定关键字的值
            if let Some(path) = log_file {
                match self.statistics.entry(path.clone()) {
                    HashMapEntry::Occupied(mut o) => {
                        //指定日志文件的统计信息存在，则继续统计
                        let statistics = o.get_mut();
                        statistics.0 += 1;
                        statistics.1 += (key.len() + value.len()) as u64;
                    },
                    HashMapEntry::Vacant(v) => {
                        //指定日志文件的统计信息不存在，则初始化统计
                        v.insert((1, (key.len() + value.len()) as u64));
                    },
                }
            }

            //加载到元信息表的内存表中
            self.table.0.root.lock().insert(Binary::new(key), Binary::new(value));
        } else {
            //删除指定关键字的值，则不需要加载到元信息表的内存表中，并记录到已删除关键字表中
            self.removed.insert(key, ());
        }
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> MetaTableLoader<C, Log> {
    /// 构建一个元信息表的加载器
    pub fn new(table: MetaTable<C, Log>) -> Self {
        MetaTableLoader {
            statistics: XHashMap::default(),
            removed: XHashMap::default(),
            table,
        }
    }

    /// 获取已加载的文件数量
    pub fn log_files_len(&self) -> usize {
        self.statistics.len()
    }

    /// 获取已加载的关键字数量
    pub fn keys_len(&self) -> u64 {
        let mut len = 0;

        for statistics in self.statistics.values() {
            len += statistics.0;
        }

        len
    }

    /// 获取已加载的字节数
    pub fn bytes_len(&self) -> u64 {
        let mut len = 0;

        for statistics in self.statistics.values() {
            len += statistics.1;
        }

        len
    }
}

/// 批量持久化已经发布到 Meta 内存根、尚未完成表级确认的事务。
///
/// timer 和 size 两个入口通过 `collecting` 竞争唯一 owner；成功 owner 在 `waits` 异步锁内
/// drain 当前 FIFO、append 全部动作并执行一次 `delay_commit`。只有该 I/O 成功后才逐事务调用
/// `confirm(Ok(()))`；失败不确认，根 WAL 因而继续保留供 repair。返回统计为本轮耗时以及
/// `(事务数, Key 数, bytes)`，Err/Ok 分别表示表日志提交失败/成功。
///
/// 当前实现会在表日志 await 期间持有 `waits` 锁，新 commit 只能等待入队；这是现状性能边界。
/// 函数不持有数据根、prepare 或 publication 锁，不与事务冲突临界区交叉。
async fn collect_waits<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(table: &MetaTable<C, Log>,
  timeout: Option<usize>) -> Result<(Duration, (usize, usize, usize)), (Duration, (usize, usize, usize))> {
    //等待指定的时间
    if let Some(timeout) = timeout {
        //需要等待指定时间后，再开始整理
        table.0.rt.timeout(timeout).await;
    }

    //检查是否正在异步整理，如果并未开始异步整理，则设置为正在异步整理，并继续异步整理
    if let Err(_) = table.0.collecting.compare_exchange(false,
                                                        true,
                                                        Ordering::Acquire,
                                                        Ordering::Relaxed) {
        //正在异步整理，则忽略本次异步整理
        return Ok((Instant::now().elapsed(), (0, 0, 0)));
    }

    //将元信息表中等待写入日志文件的事务，写入日志文件
    let mut waits = VecDeque::new();
    let mut log_uid = 0;
    let mut trs_len = 0;
    let mut keys_len = 0;
    let mut bytes_len = 0;

    let now = Instant::now();
    {
        //在锁保护下迭代当前元信息表的等待异步写日志文件的已提交的元信息事务列表
        let mut locked = table
            .0
            .waits
            .lock()
            .await;

        while let Some((wait_tr, actions, confirm)) = locked.pop_front() {
            for (key, actions) in actions.iter() {
                match actions {
                    KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                        //删除了元信息表中指定关键字的值
                        log_uid = table
                            .0
                            .log_file
                            .append(LogMethod::Remove,
                                    key.as_ref(),
                                    &[]);

                        keys_len += 1;
                        bytes_len += key.len();
                    },
                    KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                        //插入或更新了元信息表中指定关键字的值
                        log_uid = table
                            .0
                            .log_file
                            .append(LogMethod::PlainAppend,
                                    key.as_ref(),
                                    value.as_ref());

                        keys_len += 1;
                        bytes_len += key.len() + value.len();
                    },
                    KVActionLog::Read => (), //忽略读操作
                }
            }

            trs_len += 1;
            waits.push_back((wait_tr, confirm));
        }

        if let Err(e) = table
            .0
            .log_file
            .delay_commit(log_uid,
                          false,
                          DEFAULT_LOG_FILE_COMMIT_DELAY_TIMEOUT)
            .await {
            // 持久化失败后有意不调用 confirm；根 WAL 保留，供重试或启动恢复。
            // 详见 CONTRACT-CFM-001：docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only。
            table.0.collecting.store(false, Ordering::Release); //设置为已整理结束
            error!("Collect meta table failed, table: {:?}, transactions: {}, keys: {}, bytes: {}, reason: {:?}",
            table.name().as_str(),
            trs_len,
            keys_len,
            bytes_len,
            e);

            return Err((now.elapsed(), (trs_len, keys_len, bytes_len)));
        }
    }

    // 表日志持久化成功后才发送 Ok 成功信号；有/无 notifier 只改变事件报告，不改变协议。
    if let Some(notifier) = table.0.notifier.as_ref() {
        // 已归档 FIND-EVENT-001：KVDBTableType 当前没有 Meta variant，本分支沿用
        // BtreeOrdTab 标签。它只是现状观测标签，不能解释为真实表引擎类型；本轮不改事件模型。
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
                error!("Collect meta table failed, table: {:?}, transactions: {}, keys: {}, bytes: {}, reason: {:?}",
                    table.name().as_str(),
                    trs_len,
                    keys_len,
                    bytes_len,
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
                error!("Collect meta table failed, table: {:?}, transactions: {}, keys: {}, bytes: {}, reason: {:?}",
                    table.name().as_str(),
                    trs_len,
                    keys_len,
                    bytes_len,
                    e);
            }
        }
    }
    table.0.collecting.store(false, Ordering::Release); //设置为已整理结束

    Ok((now.elapsed(), (trs_len, keys_len, bytes_len)))
}
