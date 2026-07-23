//! 可查询 LogOrdered 表及其 2PC、表日志确认与启动加载实现。
//!
//! LogOrdered 同时维护已提交 COW `OrdMap` 和独立追加日志。commit 在根 WAL 成功之后先发布
//! 内存根/Key 版本，再把动作加入表级待确认 FIFO；collector 批量写表日志成功后才调用根
//! 确认器。表日志失败不会回滚已发布内存根，而是保留未确认根 WAL，重启时由 repair 重新
//! 应用，这正是“提交成功”和“数据持久化确认成功”两个有序阶段的具体实现。
//! 完整结构、锁序、状态机、恢复链与证据边界见
//! `docs/LOG_ORDERED_TABLE_INTERNAL_CONTRACT.md#log-ordered-table-internal-contract-index`。

use std::mem;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::sync::{Arc,
                atomic::{AtomicBool, AtomicUsize, Ordering}};
use std::collections::{VecDeque,
                       hash_map::Entry as HashMapEntry};

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

use crate::{Binary, KVAction, TableTrQos, KVActionLog, KVDBCommitConfirm, KVTableTrError, TableKeyConflict, TransactionDebugEvent, transaction_debug_logger,
            db::{KVDBTransaction, KVDBChildTrList},
            key_version::{KeyVersions,
                          PrepareMode,
                          PreparedActions,
                          PreparedCommitError,
                          TableVersionContext,
                          Version,
                          VersionConflictKind,
                          VersionReceipt,
                          binary_state_equal,
                          has_prepared_conflict,
                          has_prepared_transaction,
                          take_prepared_for_commit},
            tables::{KVTable, ordmap_snapshot::OrdMapSnapshot},
            utils::KVDBEvent,
            KVDBTableType};

///
/// 默认的日志文件延迟提交的超时时长，单位ms
///
const DEFAULT_LOG_FILE_COMMIT_DELAY_TIMEOUT: usize = 1000;

/// 以内存 COW 根提供查询、以独立日志文件提供耐久性的有序表共享句柄。
///
/// `root` 是当前进程可见的已提交状态，`prepare` 保存跨事务预留，`waits` 保存根 WAL 已成功
/// 但表日志尚未确认的事务。clone 只增加内部 `Arc` 引用；`ready_collect/collect` 操作表日志
/// 文件，不等价于根 WAL `.bak` 轮换。
#[derive(Clone)]
pub struct LogOrderedTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerLogOrderedTable<C, Log>>);

// SAFETY: root/prepare/waits/collector 分别由锁或原子 owner 保护，runtime 和 LogFile 可跨线程。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for LogOrderedTable<C, Log> {}
// SAFETY: `&self` 不能绕过同步原语取得共享可变状态；事务顺序是独立协议约束。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for LogOrderedTable<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVTable for LogOrderedTable<C, Log> {
    type Name = Atom;
    type Tr = LogOrdTabTr<C, Log>;
    type Error = KVTableTrError;

    // 返回共享 Atom 的引用计数 clone，不读取表日志或 COW 根。
    fn name(&self) -> <Self as KVTable>::Name {
        self.0.name.clone()
    }

    // 这是独立表日志目录；不是根 CommitLogger/WAL 路径。
    fn path(&self) -> Option<&Path> {
        Some(self.0.log_file.path())
    }

    #[inline]
    // LogOrdered 始终拥有独立数据日志；正常写子事务必须参加根 WAL 和后续确认。
    fn is_persistent(&self) -> bool {
        true
    }

    // COW 根保持按 Binary 字典序排列，并支持带起点的双向稳定快照流。
    fn is_ordered(&self) -> bool {
        true
    }

    // 只统计当前已提交内存根，不扫描磁盘，也不包含事务私有未提交动作。
    fn len(&self) -> usize {
        self.0.root.lock().size()
    }

    // full_bytes_size 统计当前 COW 根的逻辑占用；它不是日志目录实际磁盘大小。
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
        // 直接表事务只固定当前根；数据库生产入口通常改用 new_managed 同时租用版本快照。
        LogOrdTabTr::new(source,
                         is_writable,
                         is_persistent,
                         prepare_timeout,
                         commit_timeout,
                         self.clone())
    }

    fn ready_collect(&self) -> BoxFuture<Result<(), Self::Error>> {
        let table = self.clone();

        async move {
            // split 强制封口当前可写表日志。它不等待或修改根 WAL 的事务确认计数。
            let now = Instant::now();
            match table.0.log_file.split().await {
                Err(e) => {
                    //强制创建新的有序日志表可写日志文件失败，则立即返回有序日志表准备整理错误
                    return Err(KVTableTrError::new_transaction_error(ErrorLevel::Normal,
                                                                     format!("Ready collect log ordered table failed, path: {:?}, table: {:?}, reason: {:?}",
                                                                             table.0.log_file.path(),
                                                                             table.0.name.as_str(),
                                                                             e)));
                },
                Ok(writed_log_index) => {
                    //强制创建新的有序日志表可写日志文件成功
                    info!("Ready collect log ordered table succeeded, time: {:?}, path: {:?}, table: {:?}, writed_log_index: {}",
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
            // collect 仅合并已经封口的只读表日志；当前可写文件和内存根仍由 LogFile/表维护。
            let now = Instant::now();
            match table.0.log_file.collect(1024 * 1024,
                                           32 * 1024,
                                           false).await {
                Err(e) => {
                    //整理有序日志表的只读日志文件失败，则立即返回有序日志表整理错误
                    return Err(KVTableTrError::new_transaction_error(ErrorLevel::Normal,
                                                                     format!("Compact log ordered table failed, path: {:?}, table: {:?}, reason: {:?}",
                                                                             table.0.log_file.path(),
                                                                             table.0.name.as_str(),
                                                                             e)));
                },
                Ok((size, len)) => {
                    //整理有序日志表的只读日志文件成功
                    info!("Compact log ordered table succeeded, time: {:?}, path: {:?}, table: {:?}, file_size: {}, file_len: {}",
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
> LogOrderedTable<C, Log> {
    /// 在不创建事务的前提下读取当前已提交 COW 根；调用方负责 publication 同步。
    pub(crate) fn query_committed(&self, key: &Binary) -> Option<Binary> {
        self.0.root.lock().get(key).cloned()
    }

    /// 打开并恢复 LogOrdered 表，然后启动永久的待确认事务 collector。
    ///
    /// 参数分别控制日志文件上限/块大小、初始文件索引、启动加载缓冲与校验，以及待确认 FIFO
    /// 的 size/time 触发阈值。打开或加载失败会 panic，故仅供数据库启动路径使用。返回前
    /// COW 根已由日志恢复；后台任务没有显式 shutdown，并会持有表和 runtime clone。
    /// `log_file_limit/block_limit/load_buf_len` 分别传给 `pi_store::LogFile` 控制文件轮换、日志
    /// 块和启动读取；`waits_limit/wait_timeout` 只控制本表确认队列的 size/timer 触发。
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

        //打开指定的日志文件，并加载日志文件的内容到有序日志表的内存表中
        match LogFile::open(rt.clone(),
                            path.as_ref().to_path_buf(),
                            block_limit,
                            log_file_limit,
                            init_log_file_index).await {
            Err(e) => {
                //打开日志文件失败，则立即抛出异常
                panic!("Open log ordered table failed, table: {:?}, path: {:?}, reason: {:?}",
                       name.as_str(),
                       path.as_ref(),
                       e);
            },
            Ok(log_file) => {
                //打开日志文件成功
                let waits = AsyncMutex::new(VecDeque::new());
                let waits_size = AtomicUsize::new(0);
                let collecting = AtomicBool::new(false);
                let inner = InnerLogOrderedTable {
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

                let table = LogOrderedTable(Arc::new(inner));

                //加载指定的日志文件的内容到有序日志表的内存表
                let now = Instant::now();
                let mut loader = LogOrderedTableLoader::new(table.clone());
                if let Err(e) = table.0.log_file.load(&mut loader,
                                      None,
                                      load_buf_len,
                                      is_checksum).await {
                    //加载指定的日志文件失败，则立即抛出异常
                    panic!("Load log ordered table failed, table: {:?}, path: {:?}, reason: {:?}",
                           name.as_str(),
                           path.as_ref(),
                           e);
                }
                info!("Load log ordered table succeeded, table: {:?}, path: {:?}, files: {}, keys: {}, bytes: {}, time: {:?}",
                    name.as_str(),
                    path.as_ref(),
                    loader.log_files_len(),
                    loader.keys_len(),
                    loader.bytes_len(),
                    now.elapsed());

                //启动有序日志表的提交待确认事务的定时整理
                let table_copy = table.clone();
                let _ = table.0.rt.spawn(async move {
                    let table_ref = &table_copy;
                    loop {
                        match collect_waits(table_ref,
                                            Some(table_copy.0.wait_timeout)).await {
                            Err((collect_time, statistics)) => {
                                error!("Collect log ordered table failed, table: {:?}, time: {:?}, statistics: {:?}, reason: out of time",
                                    table_copy.name().as_str(),
                                    collect_time,
                                    statistics);
                            },
                            Ok((collect_time, statistics)) => {
                                debug!("Collect log ordered table succeeded, table: {:?}, time: {:?}, statistics: {:?}, reason: out of time",
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

/// LogOrdered 的共享数据、预留和持久化确认状态。
struct InnerLogOrderedTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    // 逻辑表名。
    name:           Atom,
    // 当前已提交 COW 数据根；查询只读该根或事务私有 clone。
    root:           Mutex<OrdMap<Tree<Binary, Binary>>>,
    // TID -> prepared 动作；检查/插入在同一临界区完成。
    prepare:        Mutex<XHashMap<Guid, PreparedActions>>,
    // 驱动 collector 和表日志异步 I/O。
    rt:             MultiTaskRuntime<()>,
    // 内存已发布、仍待表日志成功和根确认的事务 FIFO。
    waits:          AsyncMutex<VecDeque<(LogOrdTabTr<C, Log>, XHashMap<Binary, KVActionLog>, <LogOrdTabTr<C, Log> as Transaction2Pc>::CommitConfirm)>>,
    // 入队动作近似累计 bytes，用于 size 触发。
    waits_size:     AtomicUsize,
    // size collector 阈值。
    waits_limit:    usize,
    // timer collector 间隔，毫秒。
    wait_timeout:   usize,
    // size/timer 触发共享的单 collector owner。
    collecting:     AtomicBool,
    // 本表数据日志，不是根 CommitLogger/WAL。
    log_file:       LogFile,
    // 可选观测事件发送器，不参与正确性。
    notifier:       Option<Sender<KVDBEvent<Guid>>>,
}

// SAFETY: 所有共享可变字段均由同步原语保护，LogFile/runtime 满足线程安全契约。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for InnerLogOrderedTable<C, Log> {}
// SAFETY: 共享引用无法绕过 root/prepare/waits 的锁或 collector 原子 owner。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for InnerLogOrderedTable<C, Log> {}

/// LogOrdered 表的单元子事务共享句柄。
///
/// `root_ref` 是创建时值状态基线，`root_mut` 是事务私有 COW 根；prepare 逐 Key 检查后把
/// `actions` 转移到表级预留，commit 发布内存状态并异步排队表日志。clone 不创建新事务，
/// 同一事务只允许一次 prepare 及一次终结操作。
#[derive(Clone)]
pub struct LogOrdTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerLogOrdTabTr<C, Log>>);

/// 三种 prepare API 对同一冲突集合的错误投影。
#[derive(Clone, Copy)]
enum PrepareConflictKind {
    /// 普通 Normal 错误。
    Common,
    /// 首个结构化冲突。
    First,
    /// 全部结构化冲突。
    All,
}

// SAFETY: 外层为 Arc，内部可变状态由 SpinLock/AtomicBool 和表级同步原语保护。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for LogOrdTabTr<C, Log> {}
// SAFETY: 共享调用不会形成无同步可变别名；状态机顺序仍由事务框架/调用方约束。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for LogOrdTabTr<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> AsyncTransaction for LogOrdTabTr<C, Log> {
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
            // rollback 只撤销 prepared 预留并释放版本 lease；尚未发布的私有根随事务释放。
            // 根 WAL 已成功后不会进入本路径；Fatal commit 错误同样禁止 rollback。
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
> Transaction2Pc for LogOrdTabTr<C, Log> {
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
        // TID 由根 manager 分配并由整棵事务树继承；表级 prepare map 以它作为 owner key。
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
        // CID 只用于根 WAL 提交确认占位与回执，不是 Key 版本号或表日志 UID。
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
            let write_buf = tr.prepare_registered(PrepareConflictKind::Common).await?;
            if tr.is_writable() {
                // 调试事件仍只记录可写事务成功完成的 prepare。
                #[cfg(feature = "log_table_debug")]
                {
                    let output_len = if let Some(buf) = &write_buf {
                        buf.len()
                    } else {
                        0
                    };
                    let event = TransactionDebugEvent::Begin(tr.get_transaction_uid().unwrap(),
                                                             tr.get_status(),
                                                             tr.is_writable(),
                                                             tr.is_require_persistence(),
                                                             output_len);
                    let logger = transaction_debug_logger();
                    logger.log(event);
                }
            }

            Ok(write_buf)
        }.boxed()
    }

    fn prepare_conflicts(&self) -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        let tr = self.clone();

        async move {
            let write_buf = tr.prepare_registered(PrepareConflictKind::First).await?;
            if tr.is_writable() {
                #[cfg(feature = "log_table_debug")]
                {
                    let output_len = if let Some(buf) = &write_buf {
                        buf.len()
                    } else {
                        0
                    };
                    let event = TransactionDebugEvent::Begin(tr.get_transaction_uid().unwrap(),
                                                             tr.get_status(),
                                                             tr.is_writable(),
                                                             tr.is_require_persistence(),
                                                             output_len);
                    let logger = transaction_debug_logger();
                    logger.log(event);
                }
            }

            Ok(write_buf)
        }.boxed()
    }

    fn commit(&self, confirm: <Self as Transaction2Pc>::CommitConfirm)
              -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        let tr = self.clone();

        async move {
            let transaction_uid = tr.get_transaction_uid().unwrap();
            // publication 写门把内存根、每 Key 版本、completed revision 和可选回执组成单表
            // 原子发布区。它不覆盖后续表日志 I/O，避免长时间阻塞 qwv/prepare。
            let publication = match tr.0.version_context.as_ref() {
                Some(context) => Some(context.versions().publication().write().await),
                None => None,
            };
            // 正常 prepare 和启动 repair 都必须先登记根 TID；prepare_repair 使用 Ordinary
            // mode。合法 replay 跳过框架标准 prepare，但不会跳过该表级恢复预置步骤。
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
                        format!("Commit log ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, expected_mode: {:?}, prepared_mode: {:?}, reason: prepared action protocol mismatch after entering non-rollbackable commit",
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
                        format!("Commit log ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, expected_mode: {:?}, reason: prepared actions missing after entering non-rollbackable commit",
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
                // 纯读/空动作不分配 revision；写动作共享本表一次 revision 和根事务 TID。
                let revision = match tr.0.version_context.as_ref() {
                    Some(context) => {
                        match context.versions().checked_next_revision() {
                            Some(revision) => Some(revision),
                            None => {
                                drop(publication);
                                context.release_snapshot();
                                return Err(KVTableTrError::new_transaction_error(
                                    ErrorLevel::Fatal,
                                    format!("Commit log ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, reason: key version revision exhausted",
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
                    // prepare 已逐 Key 校验；commit 可保留等价的 COW 整根替换快路径。
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

            // 不把 publication/root/prepare guard 带入异步日志、调试回调或确认路径。
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
                    //跟踪提交
                    #[cfg(feature = "log_table_debug")]
                    {
                        let current_check_point = confirm
                            .commit_logger()
                            .current_check_point()
                            .await
                            - 1;
                        let event = TransactionDebugEvent::Commit(tr.get_transaction_uid().unwrap(),
                                                                  tr.get_commit_uid().unwrap(),
                                                                  tr.get_status(),
                                                                  tr.0.table.name(),
                                                                  size,
                                                                  current_check_point);
                        let logger = transaction_debug_logger();
                        logger.log(event);
                    }
                    table_copy.0.waits.lock().await.push_back((tr, actions, confirm)); //注册待确认的已提交事务

                    let last_waits_size = table_copy.0.waits_size.fetch_add(size, Ordering::SeqCst); //更新待确认的已提交事务的大小计数
                    if last_waits_size + size >= table_copy.0.waits_limit {
                        //如果当前已注册的待确认的已提交事务大小已达限制，则立即整理
                        table_copy.0.waits_size.store(0, Ordering::Relaxed); //重置待确认的已提交事务的大小计数

                        match collect_waits(&table_copy,
                                            None).await {
                            Err((collect_time, statistics)) => {
                                error!("Collect log ordered table failed, table: {:?}, time: {:?}, statistics: {:?}, reason: out of size",
                                    table_copy.name().as_str(),
                                    collect_time,
                                    statistics);
                            },
                            Ok((collect_time, statistics)) => {
                                info!("Collect log ordered table succeeded, table: {:?}, time: {:?}, statistics: {:?}, reason: out of size",
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
> Transaction2PcAllConflicts for LogOrdTabTr<C, Log> {
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
> UnitTransaction for LogOrdTabTr<C, Log> {
    type Status = Transaction2PcStatus;
    type Qos = TableTrQos;

    //有序日志表事务，一定是单元事务
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
> SequenceTransaction for LogOrdTabTr<C, Log> {
    type Item = Self;

    //有序日志表事务，一定不是顺序事务
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
> TransactionTree for LogOrdTabTr<C, Log> {
    type Node = KVDBTransaction<C, Log>;
    type NodeInterator = KVDBChildTrList<C, Log>;

    //有序日志表事务，一定不是事务树
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
> KVAction for LogOrdTabTr<C, Log> {
    type Key = Binary;
    type Value = Binary;
    type Error = KVTableTrError;

    fn dirty_query(&self, key: <Self as KVAction>::Key)
                   -> BoxFuture<Option<<Self as KVAction>::Value>> {
        let tr = self.clone();

        async move {
            // 当前 dirty 族不登记 Read 动作；它只读创建事务后的私有 COW 根。
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
            // actions 与 root_mut 使用独立短自旋锁；本分支不会跨 await 持有任一 guard。
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
            // 同 Key 的多次动作覆盖为最后一个 DirtyWrite，根只保存最终私有状态。
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
            // 同 Key 的 Read/旧 Write 被最终 Write 覆盖；prepare 只编码最终写动作。
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

            // `copy=false` 是 LogOrdered 表的既定 API 语义：删除事务私有根中的 Key，但不
            // 返回旧 Binary。命中与未命中最终都返回 Ok(None)；WAL 动作和提交语义不受
            // 返回值影响。见 docs/SEMANTIC_CONTRACTS.md#contract-action-001 与
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

            // `copy=false` 是 LogOrdered 表的既定 API 语义：删除事务私有根中的 Key，但不
            // 返回旧 Binary。命中与未命中最终都返回 Ok(None)；WAL 动作和提交语义不受
            // 返回值影响。见 docs/SEMANTIC_CONTRACTS.md#contract-action-001 与
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
        // 在锁内只克隆 O(1) COW 根。流拥有该根，因而同一事务后续替换 root_mut 或表
        // 继续提交都不会释放旧 iterator 的节点。创建事务仍必须活到流结束。
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
        // 与 keys 使用同一 owner 模型；不跨 yield 持有根锁或日志文件资源。
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
        // LogOrdered 当前没有逐 Key 锁；该 trait 入口是成功 no-op，不能当作排他同步原语。
        async move {
            Ok(())
        }.boxed()
    }

    fn unlock_key(&self, _key: <Self as KVAction>::Key)
                  -> BoxFuture<Result<(), <Self as KVAction>::Error>> {
        // 与 lock_key 对称地保持成功 no-op，不维护可重入或 owner 状态。
        async move {
            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> LogOrdTabTr<C, Log> {
    // 构建一个有序日志表事务
    #[inline]
    fn new(source: Atom,
           is_writable: bool,
           is_persistent: bool,
           prepare_timeout: u64,
           commit_timeout: u64,
           table: LogOrderedTable<C, Log>) -> Self {
        // 根锁内只做 O(1) COW clone；root_ref/root_mut 起初指向相同节点集合，后续写时分离。
        let root_ref = table.0.root.lock().clone();

        let inner = InnerLogOrdTabTr {
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

        LogOrdTabTr(Arc::new(inner))
    }

    /// 构建由数据库管理器装配的事务，并在同一根 guard 内固定数据快照和版本 revision。
    pub(crate) fn new_managed(source: Atom,
                              is_writable: bool,
                              is_persistent: bool,
                              prepare_timeout: u64,
                              commit_timeout: u64,
                              table: LogOrderedTable<C, Log>,
                              versions: KeyVersions,
                              mode: PrepareMode,
                              expected: XHashMap<Binary, Version>,
                              receipt: Option<VersionReceipt>,
                              actions: XHashMap<Binary, KVActionLog>) -> Self {
        // 构造期在表根 guard 内创建 lease；lease_current 只短暂获取 active_snapshots mutex，
        // 不获取 publication 门。commit 另按 publication -> 表根发布，三者没有反向嵌套。
        // 根 guard 在应用传入动作前释放，避免扩大共享临界区。
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
        let inner = InnerLogOrdTabTr {
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

        LogOrdTabTr(Arc::new(inner))
    }

    async fn precheck_versions(&self) -> Result<(), KVTableTrError> {
        // 版本协议第一阶段：在 publication 读门内完整核对外部 read-set，不建立 prepared 预留。
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
                conflicts.push(TableKeyConflict {
                    table: self.0.table.name(),
                    key: key.clone(),
                    kind: VersionConflictKind::ReadSetVersionMismatch,
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
        // 只读事务没有表动作和 WAL 片段，立即返回；根生命周期仍由 manager 收口。
        if !self.is_writable() {
            return Ok(None);
        }

        // 固定 publication(read) -> prepare 锁序；commit 仅在 prepare 锁释放后申请 write 门。
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
                // 防止第一阶段检查后、建立预留前发生的合法提交改变版本。
                for (key, expected) in context.expected() {
                    if context.versions().current_version(key).as_ref() != Some(expected) {
                        conflict_keys.push((key.clone(),
                                            VersionConflictKind::ReadSetVersionMismatch));
                    }
                }
            }
        }

        let current_root = self.0.table.0.root.lock().clone();
        for (key, action) in &actions {
            // dirty_* 明确跳过事务值状态冲突；同一事务不得与普通事务安全 API 混用。
            if action.is_dirty_writed() {
                continue;
            }
            if let Some(context) = self.0.version_context.as_ref() {
                if context
                    .versions()
                    .has_committed_after(key, context.snapshot_revision()) {
                    conflict_keys.push((key.clone(),
                                        VersionConflictKind::TransactionConflict));
                    continue;
                }
            }
            if !binary_state_equal(self.0.root_ref.get(key), current_root.get(key)) {
                conflict_keys.push((key.clone(),
                                    VersionConflictKind::TransactionConflict));
            }
        }

        let write_buf = self.prepare_output(&actions);
        let mut prepare = self.0.table.0.prepare.lock();
        let transaction_uid = self.get_transaction_uid().unwrap();
        // prepare map 的 TID 必须唯一归属于一个表子节点；覆盖同 TID 会把冻结动作交给错误
        // 节点提交。该错误发生在根 WAL 前，保留私有动作并返回可恢复错误。
        if has_prepared_transaction(&prepare, &transaction_uid) {
            return Err(KVTableTrError::new_transaction_error(
                ErrorLevel::Normal,
                format!("Prepare log ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, reason: duplicate prepared transaction uid",
                        self.0.table.name().as_str(),
                        self.0.source,
                        transaction_uid)));
        }
        // prepared 检查和当前 TID 插入必须在同一锁临界区，封闭相同新 Key 并发插入窗口。
        for (key, action) in &actions {
            if has_prepared_conflict(&prepare, key, mode, action) {
                conflict_keys.push((key.clone(),
                                    VersionConflictKind::TransactionConflict));
            }
        }
        if !conflict_keys.is_empty() {
            return Err(self.prepare_conflict_error(conflict_kind, conflict_keys));
        }

        // 成功才转移动作；冲突失败保留事务私有状态供 rollback 关闭。
        let _ = mem::replace(&mut *self.0.actions.lock(), XHashMap::default());
        prepare.insert(transaction_uid, PreparedActions {
            mode,
            actions,
        });
        Ok(write_buf)
    }

    fn prepare_output(&self,
                      actions: &XHashMap<Binary, KVActionLog>) -> Option<Vec<u8>> {
        // LogOrdered 写始终生成根 WAL 表片段；纯读返回 None。该 buffer 不是表数据日志，后者
        // 在 commit 发布后由 collect_waits 异步写入。
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
                              keys: Vec<(Binary, VersionConflictKind)>) -> KVTableTrError {
        // keys 已保证非空；All 由根 manager 与其它表冲突合并、排序和去重。
        // 分类与同 Key 优先级见 docs/VERSION_CONFLICT_KIND_DESIGN.md。
        let key = keys[0].0.clone();
        match conflict_kind {
            PrepareConflictKind::Common => {
                KVTableTrError::new_transaction_error(
                    ErrorLevel::Normal,
                    format!("Prepare log ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, reason: committed state or prepared reservation changed",
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
                    .map(|(key, kind)| TableKeyConflict {
                        table: self.0.table.name(),
                        key,
                        kind,
                    })
                    .collect())
            },
        }
    }

    /// 为启动 repair 重建根 WAL 已提交、表日志尚未确认的 LogOrdered 动作。
    ///
    /// repair 已经确定事务应重放，所以本入口跳过在线冲突检查，直接修改当前根并按指定 TID
    /// 建立普通 prepared 记录；随后 replay commit 复用正常表日志/确认流程。不能在线调用。
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

/// LogOrdered 子事务的共享数据快照、动作和版本上下文。
struct InnerLogOrdTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    source:             Atom,                                                                       // 诊断事件源。
    tid:                SpinLock<Option<Guid>>,                                                     // 根 manager 分配的全树事务 ID。
    cid:                SpinLock<Option<Guid>>,                                                     // 根 WAL 提交确认占位/回执 ID。
    status:             SpinLock<Transaction2PcStatus>,                                             // 2PC 状态。
    writable:           bool,                                                                       // 创建时固定写能力。
    persistence:        AtomicBool,                                                                 // 是否要求根 WAL；正常 LogOrdered 写为 true。
    prepare_timeout:    u64,                                                                        // 预提交超时，毫秒。
    commit_timeout:     u64,                                                                        // 提交超时，毫秒。
    root_mut:           SpinLock<OrdMap<Tree<Binary, Binary>>>,                                     // 应用本事务动作后的私有 COW 根。
    root_ref:           OrdMap<Tree<Binary, Binary>>,                                               // 创建时值状态冲突基线。
    table:              LogOrderedTable<C, Log>,                                                    // 表 owner，保活根、日志和 collector。
    actions:            SpinLock<XHashMap<Binary, KVActionLog>>,                                    // 每 Key 最终动作，prepare 成功后转移。
    version_context:    Option<TableVersionContext>,                                                 // managed lease/revision/expected/receipt。
}

/// 按日志新旧优先级恢复当前 LogOrdered COW 根的 loader。
///
/// tombstone 进入 `removed`，防止更旧文件中的同 Key 值复活；同一 Key 首次加载后不再覆盖。
struct LogOrderedTableLoader<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    statistics:         XHashMap<PathBuf, (u64, u64)>,  //加载统计信息，包括关键字数量和键值对的字节数
    removed:            XHashMap<Vec<u8>, ()>,          //已删除关键字表
    table:              LogOrderedTable<C, Log>,        //有序日志表
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> PairLoader for LogOrderedTableLoader<C, Log> {
    fn is_require(&self, _log_file: Option<&PathBuf>, key: &Vec<u8>) -> bool {
        // LogFile::load 从最新记录向更旧记录遍历；root/removed 中任一命中都表示最终状态已定。
        //不在已删除关键字表中且不在有序日志表的内存表中的关键字，才允许被加载
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
        // 调用方已经通过 is_require；PairLoader 在单个启动 future 中串行调用，不存在并发 loader。
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

            //加载到有序日志表的内存表中
            self.table.0.root.lock().insert(Binary::new(key), Binary::new(value));
        } else {
            //删除指定关键字的值，则不需要加载到有序日志表的内存表中，并记录到已删除关键字表中
            self.removed.insert(key, ());
        }
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> LogOrderedTableLoader<C, Log> {
    /// 构建一个有序日志表的加载器
    pub fn new(table: LogOrderedTable<C, Log>) -> Self {
        LogOrderedTableLoader {
            statistics: XHashMap::default(),
            removed: XHashMap::default(),
            table,
        }
    }

    /// 获取已加载的文件数量
    pub fn log_files_len(&self) -> usize {
        // 只统计至少贡献一个最终存活 value 的文件；仅含 tombstone/被遮蔽记录的文件不计入。
        self.statistics.len()
    }

    /// 获取已加载的关键字数量
    pub fn keys_len(&self) -> u64 {
        // 这是实际从日志载入 root 的 value 记录数，不含 tombstone 和被更新记录。
        let mut len = 0;

        for statistics in self.statistics.values() {
            len += statistics.0;
        }

        len
    }

    /// 获取已加载的字节数
    pub fn bytes_len(&self) -> u64 {
        // 统计最终载入 value 的 key+value payload；不代表文件头、块头或目录实际字节数。
        let mut len = 0;

        for statistics in self.statistics.values() {
            len += statistics.1;
        }

        len
    }
}

/// 批量完成 LogOrdered 的“已发布内存状态 -> 表日志 -> 根确认”后半闭环。
///
/// timer/size 触发通过 `collecting` 取得唯一 owner；owner 在 `waits` 锁内 drain 当前 FIFO，
/// append 所有动作并执行一次 `delay_commit`。I/O 成功后才逐事务调用 `confirm(Ok(()))`；失败
/// 不发送确认，使根 WAL 保持可修复。返回统计是 `(事务数, Key 数, bytes)` 与本轮耗时。
///
/// 当前表日志 await 会持有 `waits` 异步锁，因此同期 commit 入队可能等待，但本函数不持有
/// publication、prepare 或 COW root 锁，不形成与在线冲突检查的反向锁序。
async fn collect_waits<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(table: &LogOrderedTable<C, Log>,
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

    //将有序日志表中等待写入日志文件的事务，写入日志文件
    let mut waits = VecDeque::new();
    let mut log_uid = 0;
    let mut trs_len = 0;
    let mut keys_len = 0;
    let mut bytes_len = 0;

    let now = Instant::now();
    {
        //在锁保护下迭代当前有序日志表的等待异步写日志文件的已提交的有序日志事务列表
        let mut locked = table
            .0
            .waits
            .lock()
            .await;

        // 空队列保持 log_uid=0；LogFile 会把已提交 UID 的请求作为幂等成功，不产生空业务值。
        while let Some((wait_tr, actions, confirm)) = locked.pop_front() {
            for (key, actions) in actions.iter() {
                match actions {
                    KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                        //删除了有序日志表中指定关键字的值
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
                        //插入或更新了有序日志表中指定关键字的值
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
            // 持久化失败后有意不调用 confirm；根 WAL 保留，供重试或 try_repair。
            // 真实 EFBIG/重启证据见 tests/commit_confirmation_real_environment.rs；协议见
            // docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only。
            table.0.collecting.store(false, Ordering::Release); //设置为已整理结束
            error!("Collect log ordered table failed, table: {:?}, transactions: {}, keys: {}, bytes: {}, reason: {:?}",
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
        // FIND-EVENT-001 已归档：本分支当前携带 BtreeOrdTab 标签，不能解释为真实
        // LogOrdered 类型；事件模型待单独对齐，本轮只标注而不修改。
        //指定了监听器
        for (wait_tr, confirm) in waits {
            //跟踪提交
            #[cfg(feature = "log_table_debug")]
            {
                let event = TransactionDebugEvent::CommitConfirm(wait_tr.get_transaction_uid().unwrap(),
                                                                 wait_tr.get_commit_uid().unwrap(),
                                                                 wait_tr.0.table.name(),
                                                                 wait_tr.is_writable(),
                                                                 wait_tr.is_require_persistence());
                let logger = transaction_debug_logger();
                logger.log(event);
            }
            if let Err(e) = confirm(wait_tr.get_transaction_uid().unwrap(),
                                    wait_tr.get_commit_uid().unwrap(),
                                    Ok(())) {
                notifier.send(KVDBEvent::CommitFailed(wait_tr.get_source(),
                                                      wait_tr.0.table.name(),
                                                      KVDBTableType::BtreeOrdTab,
                                                      wait_tr.get_transaction_uid().unwrap(),
                                                      wait_tr.get_commit_uid().unwrap()))
                    .await;
                error!("Collect log ordered table failed, table: {:?}, transactions: {}, keys: {}, bytes: {}, reason: {:?}",
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
            //跟踪提交
            #[cfg(feature = "log_table_debug")]
            {
                let event = TransactionDebugEvent::CommitConfirm(wait_tr.get_transaction_uid().unwrap(),
                                                                 wait_tr.get_commit_uid().unwrap(),
                                                                 wait_tr.0.table.name(),
                                                                 wait_tr.is_writable(),
                                                                 wait_tr.is_require_persistence());
                let logger = transaction_debug_logger();
                logger.log(event);
            }
            if let Err(e) = confirm(wait_tr.get_transaction_uid().unwrap(),
                                    wait_tr.get_commit_uid().unwrap(),
                                    Ok(())) {
                error!("Collect log ordered table failed, table: {:?}, transactions: {}, keys: {}, bytes: {}, reason: {:?}",
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
#[cfg(test)]
mod log_ordered_local_contract_tests {
    //! LogOrdered 表内部局部不变量测试。
    //!
    //! 测试夹具使用真实 `LogFile`，但直接构造表内层且不启动永久 collector，只用于观察私有
    //! COW 根、动作、prepared map 和 loader。根 manager、根 WAL、异步确认、版本 publication、
    //! 重启与 repair 的生产可达性仍由独立真实集成测试证明，不能由本模块测试替代。

    use std::{fs,
              path::PathBuf,
              sync::{mpsc::sync_channel,
                     atomic::{AtomicU64, Ordering as AtomicOrdering}},
              time::{SystemTime, UNIX_EPOCH}};

    use futures::executor::block_on;
    use pi_async_rt::{prelude::AsyncRuntimeExt,
                      rt::multi_thread::MultiTaskRuntimeBuilder};
    use pi_bon::{Encode, WriteBuffer};
    use pi_store::commit_logger::CommitLogger;

    use super::*;

    type TestTable = LogOrderedTable<usize, CommitLogger>;

    static NEXT_TEST_ROOT: AtomicU64 = AtomicU64::new(0);

    struct LocalLogOrderedFixture {
        table: Option<TestTable>,
        path: PathBuf,
    }

    impl LocalLogOrderedFixture {
        fn new(label: &str) -> Self {
            let sequence = NEXT_TEST_ROOT.fetch_add(1, AtomicOrdering::Relaxed);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time must be after the Unix epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "pi_db_log_ordered_local_{label}_{}_{}_{}",
                std::process::id(),
                nanos,
                sequence,
            ));
            let rt = MultiTaskRuntimeBuilder::default()
                .init_worker_size(1)
                .build();
            let open_rt = rt.clone();
            let open_path = path.clone();
            let (sender, receiver) = sync_channel(1);
            rt.block_on(async move {
                let result = LogFile::open(open_rt,
                                           open_path,
                                           2 * 1024 * 1024,
                                           64 * 1024 * 1024,
                                           None).await;
                sender.send(result)
                    .expect("LogOrdered local LogFile receiver must remain alive");
            })
                .expect("LogOrdered local runtime must complete LogFile::open");
            let log_file = receiver
                .recv()
                .expect("LogOrdered local LogFile result must be returned")
                .expect("LogOrdered local LogFile must open");
            let table = LogOrderedTable(Arc::new(InnerLogOrderedTable {
                name: Atom::from("log_ordered_local"),
                root: Mutex::new(OrdMap::new(None)),
                prepare: Mutex::new(XHashMap::default()),
                rt,
                waits: AsyncMutex::new(VecDeque::new()),
                waits_size: AtomicUsize::new(0),
                waits_limit: 16 * 1024 * 1024,
                wait_timeout: 60 * 1000,
                collecting: AtomicBool::new(false),
                log_file,
                notifier: None,
            }));

            Self {
                table: Some(table),
                path,
            }
        }

        fn table(&self) -> TestTable {
            self.table.as_ref().expect("fixture table must exist").clone()
        }
    }

    impl Drop for LocalLogOrderedFixture {
        fn drop(&mut self) {
            drop(self.table.take());
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn binary(value: &str) -> Binary {
        assert!(!value.is_empty(), "test Binary must satisfy the non-empty value contract");
        let mut buffer = WriteBuffer::new();
        Atom::from(value).encode(&mut buffer);
        Binary::new(buffer.bytes)
    }

    fn assert_binary(actual: Option<Binary>, expected: Option<&Binary>, label: &str) {
        match (actual, expected) {
            (Some(actual), Some(expected)) => {
                assert_eq!(actual.as_ref(), expected.as_ref(), "{label}: value mismatch");
            },
            (None, None) => (),
            (actual, expected) => {
                panic!("{label}: presence mismatch, actual: {}, expected: {}",
                       actual.is_some(),
                       expected.is_some());
            },
        }
    }

    /// 表属性、叶节点拓扑、根身份字段、状态和 persistence 提升必须保持单义。
    #[test]
    fn test_log_ordered_metadata_leaf_identity_and_qos_contract() {
        let fixture = LocalLogOrderedFixture::new("identity");
        let table = fixture.table();
        assert_eq!(table.name().as_str(), "log_ordered_local");
        assert_eq!(table.path(), Some(fixture.path.as_path()));
        assert!(table.is_persistent());
        assert!(table.is_ordered());
        assert_eq!(table.len(), 0);
        assert_eq!(table.size(), 0);

        let transaction = table.transaction(Atom::from("LogOrdered local identity source"),
                                            true,
                                            false,
                                            1_234,
                                            5_678);
        assert!(transaction.is_writable());
        assert!(!transaction.is_concurrent_prepare());
        assert!(!transaction.is_concurrent_commit());
        assert!(!transaction.is_concurrent_rollback());
        assert!(transaction.is_enable_inherit_uid());
        assert_eq!(transaction.get_source().as_str(), "LogOrdered local identity source");
        assert_eq!(transaction.get_prepare_timeout(), 1_234);
        assert_eq!(transaction.get_commit_timeout(), 5_678);
        assert_eq!(transaction.get_status(), Transaction2PcStatus::Start);
        assert!(transaction.is_unit());
        assert!(!transaction.is_sequence());
        assert!(!transaction.is_tree());
        assert!(transaction.prev_item().is_none());
        assert!(transaction.next_item().is_none());
        assert_eq!(transaction.children_len(), 0);
        assert_eq!(transaction.to_children().count(), 0);
        assert_eq!(transaction.qos(), TableTrQos::ThreadSafe);
        assert!(block_on(transaction.init()).is_ok());

        let tid = Guid(101);
        let cid = Guid(102);
        transaction.set_transaction_uid(tid.clone());
        transaction.set_commit_uid(cid.clone());
        transaction.set_prepare_uid(Guid(103));
        assert_eq!(transaction.get_transaction_uid(), Some(tid));
        assert_eq!(transaction.get_commit_uid(), Some(cid));
        assert!(transaction.get_prepare_uid().is_none());
        transaction.set_status(Transaction2PcStatus::Actioning);
        assert_eq!(transaction.get_status(), Transaction2PcStatus::Actioning);
        transaction.require_persistence();
        transaction.require_persistence();
        assert!(transaction.is_require_persistence());
        assert_eq!(transaction.qos(), TableTrQos::Safe);

        let read_only = table.transaction(Atom::from("LogOrdered local read only"),
                                          false,
                                          true,
                                          7,
                                          9);
        assert!(matches!(block_on(read_only.prepare()), Ok(None)));
        assert!(table.0.prepare.lock().is_empty());
    }

    /// 动作只修改私有 COW 根，同 Key 后写只保留最终动作，流固定创建瞬间的私有根。
    #[test]
    fn test_log_ordered_private_cow_final_action_and_snapshot_contract() {
        let fixture = LocalLogOrderedFixture::new("actions");
        let table = fixture.table();
        let retained_key = binary("log-ordered-local-retained");
        let deleted_key = binary("log-ordered-local-deleted");
        let committed_value = binary("committed-value");
        let first_value = binary("first-private-value");
        let final_value = binary("final-private-value");
        table.0.root.lock().upsert(deleted_key.clone(), committed_value.clone(), false);

        let transaction = table.transaction(Atom::from("LogOrdered local actions source"),
                                            true,
                                            true,
                                            100,
                                            200);
        block_on(transaction.upsert(retained_key.clone(), first_value.clone()))
            .expect("first private LogOrdered upsert must succeed");
        let snapshot = transaction.values(None, false);
        block_on(transaction.upsert(retained_key.clone(), final_value.clone()))
            .expect("final private LogOrdered upsert must succeed");
        let removed = block_on(transaction.delete(deleted_key.clone()))
            .expect("private LogOrdered delete must succeed");
        assert!(removed.is_none(), "LogOrdered delete must not expose the old value");

        assert_binary(block_on(transaction.query(retained_key.clone())),
                      Some(&final_value),
                      "transaction must observe final private upsert");
        assert_binary(table.query_committed(&retained_key),
                      None,
                      "uncommitted LogOrdered upsert must not reach shared root");
        assert_binary(table.query_committed(&deleted_key),
                      Some(&committed_value),
                      "uncommitted LogOrdered delete must not reach shared root");

        let snapshot_entries = block_on(snapshot.collect::<Vec<_>>());
        assert_eq!(snapshot_entries.len(), 2);
        assert!(snapshot_entries.iter().any(|(key, value)| {
            key.as_ref() == retained_key.as_ref() && value.as_ref() == first_value.as_ref()
        }));
        assert!(snapshot_entries.iter().any(|(key, value)| {
            key.as_ref() == deleted_key.as_ref() && value.as_ref() == committed_value.as_ref()
        }));

        let actions = transaction.0.actions.lock();
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions.get(&retained_key),
                         Some(KVActionLog::Write(Some(value)))
                         if value.as_ref() == final_value.as_ref()));
        assert!(matches!(actions.get(&deleted_key), Some(KVActionLog::Write(None))));
    }

    /// prepare 只编码最终写并转移动作；prepared 冲突必须原子拒绝，rollback 不发布私有根。
    #[test]
    fn test_log_ordered_prepare_wal_conflict_ownership_and_rollback_contract() {
        let fixture = LocalLogOrderedFixture::new("prepare");
        let table = fixture.table();
        let upsert_key = binary("log-ordered-local-prepare-upsert");
        let delete_key = binary("log-ordered-local-prepare-delete");
        let read_key = binary("log-ordered-local-prepare-read");
        let old_value = binary("old-committed-value");
        let new_value = binary("new-private-value");
        table.0.root.lock().upsert(delete_key.clone(), old_value.clone(), false);

        let transaction = table.transaction(Atom::from("LogOrdered local prepare source"),
                                            true,
                                            true,
                                            300,
                                            400);
        let tid = Guid(201);
        transaction.set_transaction_uid(tid.clone());
        block_on(transaction.upsert(upsert_key.clone(), new_value.clone()))
            .expect("private LogOrdered upsert before prepare must succeed");
        block_on(transaction.delete(delete_key.clone()))
            .expect("private LogOrdered delete before prepare must succeed");
        assert!(block_on(transaction.query(read_key.clone())).is_none());

        let output = block_on(transaction.prepare_conflicts())
            .expect("LogOrdered prepare must succeed")
            .expect("LogOrdered writes must produce a WAL fragment");
        let (table_name, write_count, offset) =
            <TestTable as KVTable>::get_init_table_prepare_output(&output, 0);
        let (writes, end) =
            <TestTable as KVTable>::get_all_key_value_from_table_prepare_output(
                &output,
                &table_name,
                write_count,
                offset);
        assert_eq!(table_name.as_str(), "log_ordered_local");
        assert_eq!(write_count, 2, "Read must not enter the LogOrdered WAL fragment");
        assert_eq!(writes.len(), 2);
        assert_eq!(end, output.len());
        assert!(writes.iter().any(|entry| {
            entry.key.as_ref() == upsert_key.as_ref()
                && entry.value.as_ref().map(Binary::as_ref) == Some(new_value.as_ref())
        }));
        assert!(writes.iter().any(|entry| {
            entry.key.as_ref() == delete_key.as_ref() && entry.value.is_none()
        }));

        assert!(transaction.0.actions.lock().is_empty());
        {
            let prepared = table.0.prepare.lock();
            let item = prepared
                .get(&tid)
                .expect("LogOrdered prepare map must reserve the root TID");
            assert_eq!(item.mode, PrepareMode::Ordinary);
            assert_eq!(item.actions.len(), 3);
            assert!(matches!(item.actions.get(&read_key), Some(KVActionLog::Read)));
        }

        let contender = table.transaction(Atom::from("LogOrdered local prepared contender"),
                                          true,
                                          true,
                                          500,
                                          600);
        contender.set_transaction_uid(Guid(202));
        block_on(contender.upsert(upsert_key.clone(), binary("contender-value")))
            .expect("LogOrdered contender action must succeed locally");
        let conflict = block_on(contender.prepare_conflicts())
            .expect_err("same-Key prepared LogOrdered contender must conflict");
        assert!(conflict.is_conflicts());
        block_on(contender.rollback()).expect("LogOrdered contender rollback must succeed");

        assert_binary(table.query_committed(&upsert_key),
                      None,
                      "prepare must not publish LogOrdered upsert");
        assert_binary(table.query_committed(&delete_key),
                      Some(&old_value),
                      "prepare must not publish LogOrdered delete");
        block_on(transaction.rollback()).expect("LogOrdered rollback must release prepared state");
        assert!(table.0.prepare.lock().is_empty());
    }

    /// DirtyWrite 当前无条件跳过值状态比较；本测试只固定现状，不把它提升为最终设计。
    #[test]
    fn test_log_ordered_dirty_prepare_current_conflict_branch() {
        let fixture = LocalLogOrderedFixture::new("dirty");
        let table = fixture.table();
        let key = binary("log-ordered-local-dirty");
        let initial_value = binary("initial-value");
        let private_value = binary("dirty-private-value");
        let concurrent_value = binary("concurrent-committed-value");
        table.0.root.lock().upsert(key.clone(), initial_value, false);

        let transaction = table.transaction(Atom::from("LogOrdered local dirty source"),
                                            true,
                                            true,
                                            700,
                                            800);
        let tid = Guid(301);
        transaction.set_transaction_uid(tid.clone());
        block_on(transaction.dirty_upsert(key.clone(), private_value.clone()))
            .expect("LogOrdered dirty upsert must succeed locally");
        assert_binary(block_on(transaction.dirty_query(key.clone())),
                      Some(&private_value),
                      "dirty query must observe the private root");
        table.0.root.lock().upsert(key.clone(), concurrent_value.clone(), false);

        assert!(block_on(transaction.prepare_conflicts())
            .expect("LogOrdered DirtyWrite currently skips committed value comparison")
            .is_some());
        assert!(table.0.prepare.lock().contains_key(&tid));
        assert_binary(table.query_committed(&key),
                      Some(&concurrent_value),
                      "LogOrdered dirty prepare must not publish its private value");
        block_on(transaction.rollback()).expect("LogOrdered dirty rollback must succeed");
        assert!(table.0.prepare.lock().is_empty());
    }

    /// 受信 repair 直接得到最终状态并登记 Ordinary prepared；最终动作可逻辑幂等重放。
    #[test]
    fn test_log_ordered_repair_final_state_and_local_idempotence() {
        let fixture = LocalLogOrderedFixture::new("repair");
        let table = fixture.table();
        let upsert_key = binary("log-ordered-local-repair-upsert");
        let delete_key = binary("log-ordered-local-repair-delete");
        let old_value = binary("old-value");
        let repaired_value = binary("repaired-value");
        table.0.root.lock().upsert(delete_key.clone(), old_value, false);

        let first = table.transaction(Atom::from("LogOrdered local repair first"),
                                      true,
                                      true,
                                      900,
                                      1_000);
        block_on(first.upsert(upsert_key.clone(), repaired_value.clone()))
            .expect("first LogOrdered repair upsert action must be staged");
        block_on(first.delete(delete_key.clone()))
            .expect("first LogOrdered repair delete action must be staged");
        let first_tid = Guid(401);
        first.prepare_repair(first_tid.clone());

        assert_binary(table.query_committed(&upsert_key),
                      Some(&repaired_value),
                      "LogOrdered repair must apply upsert directly");
        assert_binary(table.query_committed(&delete_key),
                      None,
                      "LogOrdered repair must apply delete directly");
        assert!(first.0.actions.lock().is_empty());

        let second = table.transaction(Atom::from("LogOrdered local repair second"),
                                       true,
                                       true,
                                       1_100,
                                       1_200);
        block_on(second.upsert(upsert_key.clone(), repaired_value.clone()))
            .expect("second LogOrdered repair upsert action must be staged");
        block_on(second.delete(delete_key.clone()))
            .expect("second LogOrdered repair delete action must be staged");
        let second_tid = Guid(402);
        second.prepare_repair(second_tid.clone());

        assert_eq!(table.len(), 1);
        assert_binary(table.query_committed(&upsert_key),
                      Some(&repaired_value),
                      "repeated LogOrdered repair must retain final upsert");
        assert_binary(table.query_committed(&delete_key),
                      None,
                      "repeated LogOrdered repair must retain final delete");
        let mut prepared = table.0.prepare.lock();
        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared.remove(&first_tid).map(|item| item.mode),
                   Some(PrepareMode::Ordinary));
        assert_eq!(prepared.remove(&second_tid).map(|item| item.mode),
                   Some(PrepareMode::Ordinary));
        assert!(prepared.is_empty());
    }

    /// loader 必须保持新日志优先，tombstone 必须压制旧值，统计只计算实际载入的 value。
    #[test]
    fn test_log_ordered_loader_newest_tombstone_and_statistics_contract() {
        let fixture = LocalLogOrderedFixture::new("loader");
        let table = fixture.table();
        let newest_key = binary("log-ordered-local-loader-newest");
        let removed_key = binary("log-ordered-local-loader-removed");
        let older_key = binary("log-ordered-local-loader-older");
        let newest_value = binary("newest-value");
        let ignored_older_value = binary("ignored-older-value");
        let older_value = binary("older-value");
        let newer_path = PathBuf::from("log-ordered-newer.log");
        let older_path = PathBuf::from("log-ordered-older.log");
        let mut loader = LogOrderedTableLoader::new(table.clone());

        assert!(loader.is_require(Some(&newer_path), &newest_key.as_ref().to_vec()));
        loader.load(Some(&newer_path),
                    LogMethod::PlainAppend,
                    newest_key.as_ref().to_vec(),
                    Some(newest_value.as_ref().to_vec()));
        assert!(!loader.is_require(Some(&older_path), &newest_key.as_ref().to_vec()));

        assert!(loader.is_require(Some(&newer_path), &removed_key.as_ref().to_vec()));
        loader.load(Some(&newer_path),
                    LogMethod::Remove,
                    removed_key.as_ref().to_vec(),
                    None);
        assert!(!loader.is_require(Some(&older_path), &removed_key.as_ref().to_vec()));

        assert!(loader.is_require(Some(&older_path), &older_key.as_ref().to_vec()));
        loader.load(Some(&older_path),
                    LogMethod::PlainAppend,
                    older_key.as_ref().to_vec(),
                    Some(older_value.as_ref().to_vec()));

        assert_binary(table.query_committed(&newest_key),
                      Some(&newest_value),
                      "newest LogOrdered loader value must win");
        assert_binary(table.query_committed(&removed_key),
                      None,
                      "LogOrdered loader tombstone must suppress older value");
        assert_binary(table.query_committed(&older_key),
                      Some(&older_value),
                      "unshadowed older LogOrdered value must load");
        assert_ne!(table.query_committed(&newest_key), Some(ignored_older_value));
        assert_eq!(loader.log_files_len(), 2);
        assert_eq!(loader.keys_len(), 2);
        assert_eq!(loader.bytes_len(),
                   (newest_key.len() + newest_value.len()
                    + older_key.len() + older_value.len()) as u64);
    }
}
