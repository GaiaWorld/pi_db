//! LogWrite 只写日志表的当前实现。
//!
//! **外部禁止使用此表。** 本轮按 HC-059 暂停 LogWrite 的行为修复、重构和新增行为测试，只
//! 校准注释与文档。当前公开 trait 形状来自五表统一接口，但实际仅支持 upsert：query 始终
//! `None`，delete 是返回 `Ok(None)` 的 no-op，keys/values 各产生一个合成空项。这些行为不是
//! 可依赖的通用表语义，也不是最终设计。内部仍维护 COW 根用于冲突检查/commit，并以独立
//! `LogFile` 持久化 upsert。
//!
//! 提交分为两个有序阶段：根 WAL 成功后，事务 `commit` 在版本 publication 写锁内发布内存根
//! 与版本号，然后把需要持久化的动作移交给表级 collector；collector 成功提交 `LogFile` 后才
//! 调用确认器。表日志失败不会回调伪造失败确认，根 WAL 会继续保留以供恢复。这个流程只是对
//! 当前内部实现的如实说明，不构成允许外部启用 LogWrite 的承诺。

use std::mem;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}};
use std::collections::hash_map::Entry as HashMapEntry;

use parking_lot::Mutex;
use futures::{future::{FutureExt, BoxFuture}, stream::{StreamExt, BoxStream}};
use async_lock::Mutex as AsyncMutex;
use async_stream::stream;
use log::{debug, info, error};

use pi_atom::Atom;
use pi_guid::Guid;
use pi_hash::XHashMap;
use pi_ordmap::{ordmap::{ImOrdMap, Iter, OrdMap},
                asbtree::Tree};
use pi_async_rt::{lock::spin_lock::SpinLock,
               rt::{AsyncRuntime, multi_thread::MultiTaskRuntime}};
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
use pi_store::log_store::log_file::{PairLoader,
                                    LogMethod,
                                    LogFile};

use crate::{Binary,
            KVAction,
            TableTrQos,
            KVActionLog,
            KVDBCommitConfirm,
            KVTableTrError,
            TableKeyConflict,
            db::{KVDBTransaction, KVDBChildTrList},
            key_version::{KeyVersions,
                          PrepareMode,
                          PreparedActions,
                          TableVersionContext,
                          Version,
                          VersionConflictKind,
                          VersionReceipt,
                          binary_state_equal,
                          has_prepared_conflict},
            tables::KVTable};
use std::collections::VecDeque;

///
/// 默认的日志文件延迟提交的超时时长，单位ms
///
const DEFAULT_LOG_FILE_COMMIT_DELAY_TIMEOUT: usize = 1000;

/// 只接受 upsert 的 LogWrite 表共享句柄。
///
/// 该类型当前不允许外部构建或通过数据库业务协议使用。`root` 仅服务内部冲突基线、COW
/// commit 和日志恢复，不能通过 query/iterator 读取；`prepare`/`waits` 分别保存预提交预留和
/// 已发布待表日志确认事务。clone 只增加内部 `Arc` 引用。
#[derive(Clone)]
pub struct LogWriteTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerLogWriteTable<C, Log>>);

// SAFETY: root/prepare/waits 和 collector owner 均由锁或原子类型保护；外层只移动 Arc。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for LogWriteTable<C, Log> {}
// SAFETY: 共享引用不能绕过内部同步原语。公开行为是否合理是已暂停的设计问题。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for LogWriteTable<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVTable for LogWriteTable<C, Log> {
    type Name = Atom;
    type Tr = LogWTabTr<C, Log>;
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
        LogWTabTr::new(source,
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
                    //强制创建新的只写日志表可写日志文件失败，则立即返回只写日志表准备整理错误
                    return Err(KVTableTrError::new_transaction_error(ErrorLevel::Normal, format!("Ready collect only writable table failed, path: {:?}, table: {:?}, reason: {:?}", table.0.log_file.path(), table.0.name.as_str(), e)));
                },
                Ok(writed_log_index) => {
                    //强制创建新的只写日志表可写日志文件成功
                    info!("Ready collect only writable table ok, time: {:?}, path: {:?}, table: {:?}, writed_log_index: {}",
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
                    //整理只写日志表的只读日志文件失败，则立即返回只写日志表整理错误
                    return Err(KVTableTrError::new_transaction_error(ErrorLevel::Normal, format!("Collect only writable table failed, path: {:?}, table: {:?}, reason: {:?}", table.0.log_file.path(), table.0.name.as_str(), e)));
                },
                Ok((size, len)) => {
                    //整理只写日志表的只读日志文件成功
                    info!("Collect only writable table ok, time: {:?}, path: {:?}, table: {:?}, file_size: {}, file_len: {}",
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
> LogWriteTable<C, Log> {
    /// 打开、恢复并启动 LogWrite collector。
    ///
    /// 打开/加载失败会 panic，后台任务永久持有表 clone；参数语义与 LogOrdered 的表日志配置
    /// 相同。此构造器仅供数据库内部兼容加载，协议层禁止外部直接调用或创建新 LogWrite 表。
    pub async fn new<P: AsRef<Path>>(rt: MultiTaskRuntime<()>,
                                     path: P,
                                     name: Atom,
                                     log_file_limit: usize,
                                     block_limit: usize,
                                     init_log_file_index: Option<usize>,
                                     load_buf_len: u64,
                                     is_checksum: bool,
                                     waits_limit: usize,
                                     wait_timeout: usize) -> Self {
        let root = Mutex::new(OrdMap::new(None));
        let prepare = Mutex::new(XHashMap::default());

        //打开指定的日志文件，并加载日志文件的内容到只写日志表的内存表中
        match LogFile::open(rt.clone(),
                            path.as_ref().to_path_buf(),
                            block_limit,
                            log_file_limit,
                            init_log_file_index).await {
            Err(e) => {
                //打开日志文件失败，则立即抛出异常
                panic!("Open only writable table failed, table: {:?}, path: {:?}, reason: {:?}",
                       name.as_str(),
                       path.as_ref(),
                       e);
            },
            Ok(log_file) => {
                //打开日志文件成功
                let waits = AsyncMutex::new(VecDeque::new());
                let waits_size = AtomicUsize::new(0);
                let collecting = AtomicBool::new(false);
                let inner = InnerLogWriteTable {
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
                };

                let table = LogWriteTable(Arc::new(inner));

                //加载指定的日志文件的内容到只写日志表的内存表
                let now = Instant::now();
                let mut loader = LogWriteTableLoader::new(table.clone());
                if let Err(e) = table.0.log_file.load(&mut loader,
                                                      None,
                                                      load_buf_len,
                                                      is_checksum).await {
                    //加载指定的日志文件失败，则立即抛出异常
                    panic!("Load only writable table failed, table: {:?}, path: {:?}, reason: {:?}",
                           name.as_str(),
                           path.as_ref(),
                           e);
                }
                info!("Load only writable table ok, table: {:?}, path: {:?}, files: {}, keys: {}, bytes: {}, time: {:?}",
                    name.as_str(),
                    path.as_ref(),
                    loader.log_files_len(),
                    loader.keys_len(),
                    loader.bytes_len(),
                    now.elapsed());

                //启动只写日志表的提交待确认事务的定时整理
                let table_copy = table.clone();
                let _ = table.0.rt.spawn(async move {
                    let table_ref = &table_copy;
                    loop {
                        match collect_waits(table_ref,
                                            Some(table_copy.0.wait_timeout)).await {
                            Err((collect_time, statistics)) => {
                                error!("Collect only writable table failed, table: {:?}, time: {:?}, statistics: {:?}, reason: out of time",
                                    table_copy.name().as_str(),
                                    collect_time,
                                    statistics);
                            },
                            Ok((collect_time, statistics)) => {
                                debug!("Collect only writable table ok, table: {:?}, time: {:?}, statistics: {:?}, reason: out of time",
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

/// LogWrite 共享状态；结构与 LogOrdered 相似，但不对外提供真实读/删/迭代语义。
struct InnerLogWriteTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    name:           Atom,                                                                                                   // 逻辑表名。
    root:           Mutex<OrdMap<Tree<Binary, Binary>>>,                                                                    // 内部已提交 upsert 根；公开读不暴露。
    prepare:        Mutex<XHashMap<Guid, PreparedActions>>,                                                                 // TID -> 已预留 upsert 动作。
    rt:             MultiTaskRuntime<()>,                                                                                   // collector 与表日志 I/O runtime。
    waits:          AsyncMutex<VecDeque<(LogWTabTr<C, Log>, XHashMap<Binary, KVActionLog>, <LogWTabTr<C, Log> as Transaction2Pc>::CommitConfirm)>>, // 已发布、待表日志和确认 FIFO。
    waits_size:     AtomicUsize,                                                                                            // FIFO 动作近似累计 bytes。
    waits_limit:    usize,                                                                                                  // size collector 阈值。
    wait_timeout:   usize,                                                                                                  // timer collector 间隔，毫秒。
    collecting:     AtomicBool,                                                                                             // size/timer 共用的单 collector owner。
    log_file:       LogFile,                                                                                                // 独立 upsert 数据日志，不是根 WAL。
}

// SAFETY: 共享可变字段均由 Mutex/AsyncMutex/原子类型保护。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for InnerLogWriteTable<C, Log> {}
// SAFETY: 共享引用无法绕过上述同步边界。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for InnerLogWriteTable<C, Log> {}

/// LogWrite 的单元子事务共享句柄。
///
/// 内部 `root_ref/root_mut/actions` 仍实现与其它 COW 表相同的冲突和 commit 结构，但合法动作域
/// 仅有 upsert。该类型当前只为兼容既有数据库装配保留，外部不得依赖其 query/delete/stream
/// 占位行为。clone 共享同一事务状态。
#[derive(Clone)]
pub struct LogWTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerLogWTabTr<C, Log>>);

/// 冲突错误投影；保留与其它表一致的内部 2PC 接口。
#[derive(Clone, Copy)]
enum PrepareConflictKind {
    /// 普通错误。
    Common,
    /// 首个结构化冲突。
    First,
    /// 全部结构化冲突。
    All,
}

// SAFETY: 外层为 Arc，内部字段由 SpinLock/AtomicBool 和表级锁同步。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for LogWTabTr<C, Log> {}
// SAFETY: 共享访问不产生无同步别名；行为协议仍明确禁止外部使用。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for LogWTabTr<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> AsyncTransaction for LogWTabTr<C, Log> {
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
            // manager 在进入 rollback 前已经为整棵事务树发布 TID；这里的 unwrap 属于内部
            // 2PC 协议前置条件。rollback 只撤销 prepare 预留并释放版本快照，不修改已提交根。
            // 一旦根 WAL 已成功落地，manager 会把后续提交错误升级为 Fatal，不会进入这里。
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
> Transaction2Pc for LogWTabTr<C, Log> {
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
            // publication 写锁是“发布内存根 + 发布对应版本号”的线性化边界。持锁期间，
            // query_with_version 不能读取半发布状态，另一个 versioned commit 也不能交错发布。
            // 锁内没有表日志 I/O；后续 collector 的异步落盘发生在释放此锁以后。
            let publication = match tr.0.version_context.as_ref() {
                Some(context) => Some(context.versions().publication().write().await),
                None => None,
            };
            // prepare 表中的动作是预提交成功后冻结的唯一提交输入。remove 同时释放该 TID 的
            // Key 预留；此后即使动作集为空，也不能再次提交同一个事务。
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
                // revision 只在确有写动作时分配，并且必须仍位于 publication 写锁内。耗尽表示
                // 无法再建立单调发布顺序，属于不可恢复 Fatal，不能发布根或回执。
                let revision = match tr.0.version_context.as_ref() {
                    Some(context) => {
                        match context.versions().checked_next_revision() {
                            Some(revision) => Some(revision),
                            None => {
                                drop(publication);
                                context.release_snapshot();
                                return Err(KVTableTrError::new_transaction_error(
                                    ErrorLevel::Fatal,
                                    format!("Commit only writable table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, reason: key version revision exhausted",
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
                // ptr_eq 只是提交时的 COW 快路：根仍等于事务创建快照时，可整根替换；否则按
                // 已冻结动作逐 Key 合并。冲突正确性来自 prepare 的逐 Key 状态/版本/预留检查，
                // 不能把这个指针判等理解为独立的冲突判断。
                if root.ptr_eq(&tr.0.root_ref) {
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
                    // 同一事务的所有 Key 使用相同 TID 和 revision；Version 仍按动作区分 upsert
                    // 与 delete。先发布全部 Key，再 complete_revision，最后追加本次事务回执，
                    // 防止外部拿到尚未完成发布的 Table/Key/Version 集合。
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

            // 先结束内存与版本发布临界区，再释放事务快照，最后才安排可能阻塞的表日志工作。
            drop(publication);
            if let Some(context) = tr.0.version_context.as_ref() {
                context.release_snapshot();
            }

            if tr.is_require_persistence() {
                // 持久化事务先登记后台写入并立即结束本次 commit future；这里的 Ok 只表示
                // 调度路径完成，不表示表日志已落盘或根 WAL 已确认。后台 LogFile 成功后才发送
                // Ok 成功信号；失败时完全不调用确认器并保留根 WAL。详见 CONTRACT-CFM-001：
                // docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only。
                let table_copy = tr.0.table.clone();
                // spawn 失败被当前实现忽略，属于已归档的环境/runtime 失效边界
                // LIMIT-ROOT-WAL-IO-001；本轮只记录，不扩大 LogWrite 修改范围。
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
                    // 先把事务、冻结动作和确认器作为一个不可拆分单元入队，再更新近似字节计数。
                    // waits 使用异步锁，因此争用只挂起 task，不同步阻塞 runtime worker 线程。
                    table_copy.0.waits.lock().await.push_back((tr, actions, confirm)); //注册待确认的已提交事务

                    let last_waits_size = table_copy.0.waits_size.fetch_add(size, Ordering::SeqCst); //更新待确认的已提交事务的大小计数
                    if last_waits_size + size >= table_copy.0.waits_limit {
                        //如果当前已注册的待确认的已提交事务大小已达限制，则立即整理
                        table_copy.0.waits_size.store(0, Ordering::Relaxed); //重置待确认的已提交事务的大小计数

                        match collect_waits(&table_copy,
                                            None).await {
                            Err((collect_time, statistics)) => {
                                error!("Collect only writable table failed, table: {:?}, time: {:?}, statistics: {:?}, reason: out of size",
                                    table_copy.name().as_str(),
                                    collect_time,
                                    statistics);
                            },
                            Ok((collect_time, statistics)) => {
                                info!("Collect only writable table ok, table: {:?}, time: {:?}, statistics: {:?}, reason: out of size",
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
> Transaction2PcAllConflicts for LogWTabTr<C, Log> {
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
> UnitTransaction for LogWTabTr<C, Log> {
    type Status = Transaction2PcStatus;
    type Qos = TableTrQos;

    //只写日志表事务，一定是单元事务
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
> SequenceTransaction for LogWTabTr<C, Log> {
    type Item = Self;

    //只写日志表事务，一定不是顺序事务
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
> TransactionTree for LogWTabTr<C, Log> {
    type Node = KVDBTransaction<C, Log>;
    type NodeInterator = KVDBChildTrList<C, Log>;

    //只写日志表事务，一定不是事务树
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
> KVAction for LogWTabTr<C, Log> {
    type Key = Binary;
    type Value = Binary;
    type Error = KVTableTrError;

    fn dirty_query(&self, _key: <Self as KVAction>::Key)
                   -> BoxFuture<Option<<Self as KVAction>::Value>> {
        async move {
            // HC-059 冻结现状：LogWrite 不公开读取，即使内部 root 存在该 Key 也固定 None。
            None
        }.boxed()
    }

    fn query(&self, _key: <Self as KVAction>::Key)
             -> BoxFuture<Option<<Self as KVAction>::Value>> {
        async move {
            // 不记录 Read，也不建立事务安全读语义；外部禁止使用该占位实现。
            None
        }.boxed()
    }

    fn dirty_upsert(&self,
                    key: <Self as KVAction>::Key,
                    value: <Self as KVAction>::Value)
                    -> BoxFuture<Result<(), <Self as KVAction>::Error>> {
        let tr = self.clone();

        async move {
            // 公开读能力仍固定返回 None，但事务私有根必须同步更新：它是逐 Key 冲突基线，
            // commit 的 COW 整根替换也依赖该根包含本事务的最终写入。
            let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::DirtyWrite(Some(value.clone())));
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
            // 只写表不对外返回值，但内部根仍须记录最终状态，供冲突检查和 commit 发布。
            let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::Write(Some(value.clone())));
            let _ = tr.0.root_mut.lock().upsert(key, value, false);

            Ok(())
        }.boxed()
    }

    fn dirty_delete(&self, _key: <Self as KVAction>::Key)
                    -> BoxFuture<Result<Option<<Self as KVAction>::Value>, <Self as KVAction>::Error>> {
        async move {
            // 当前是明确 no-op：不记录 tombstone、不修改私有根，返回值不能证明 Key 不存在。
            Ok(None)
        }.boxed()
    }

    fn delete(&self, _key: <Self as KVAction>::Key)
              -> BoxFuture<Result<Option<<Self as KVAction>::Value>, <Self as KVAction>::Error>> {
        async move {
            // 与 dirty_delete 相同的 no-op 占位；不是其它四表的 delete 契约。
            Ok(None)
        }.boxed()
    }

    fn keys<'a>(&self,
                key: Option<<Self as KVAction>::Key>,
                descending: bool)
                -> BoxStream<'a, <Self as KVAction>::Key> {
        let stream = stream! {
            // 当前并非空 stream，而是合成一个空 Binary。空 Key 在数据库合法域中被禁止，
            // 因而该项只能视为未完成接口占位，绝不能用于枚举已写 Key。
            yield Binary::new(vec![]);
        };

        stream.boxed()
    }

    fn values<'a>(&self,
                  key: Option<<Self as KVAction>::Key>,
                  descending: bool)
                  -> BoxStream<'a, (<Self as KVAction>::Key, <Self as KVAction>::Value)> {
        let stream = stream! {
            // 同 keys：固定产生一个非法业务空 Key/空 Value 占位，不读取内部 root。
            yield (Binary::new(vec![]), Binary::new(vec![]));
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
> LogWTabTr<C, Log> {
    // 构建一个只写日志表事务
    #[inline]
    fn new(source: Atom,
           is_writable: bool,
           is_persistent: bool,
           prepare_timeout: u64,
           commit_timeout: u64,
           table: LogWriteTable<C, Log>) -> Self {
        let root_ref = table.0.root.lock().clone();

        let inner = InnerLogWTabTr {
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

        LogWTabTr(Arc::new(inner))
    }

    /// 构建由数据库管理器装配的只写表事务；内部根仅作为冲突基线，不改变公开读固定 None。
    pub(crate) fn new_managed(source: Atom,
                              is_writable: bool,
                              is_persistent: bool,
                              prepare_timeout: u64,
                              commit_timeout: u64,
                              table: LogWriteTable<C, Log>,
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
        let inner = InnerLogWTabTr {
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

        LogWTabTr(Arc::new(inner))
    }

    async fn precheck_versions(&self) -> Result<(), KVTableTrError> {
        let Some(context) = self.0.version_context.as_ref() else {
            return Ok(());
        };
        if context.mode() != PrepareMode::Versioned {
            return Ok(());
        }

        // Phase 1 只在 publication 读锁内比较外部提供的期望版本，不登记 prepare 预留、
        // 不移动 actions、不分配 revision，也不改变根。这样整棵事务树可以完整收集冲突后
        // 再决定是否进入有副作用的 Phase 2。
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
        if !self.is_writable() {
            return Ok(None);
        }

        // 固定锁序为 publication(read) -> root(短暂 clone) -> prepare。publication 与同步表锁
        // 不交叉 await；prepare 锁同时覆盖“检查其它事务预留”和“登记本事务全部预留”，因此
        // 同一批 actions 不会只登记一部分，也不会被并发 prepare 穿插。
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
                for (key, expected) in context.expected() {
                    if context.versions().current_version(key).as_ref() != Some(expected) {
                        conflict_keys.push((key.clone(),
                                            VersionConflictKind::ReadSetVersionMismatch));
                    }
                }
            }
        }

        // LogWrite 公开读固定返回 None，但内部根保存真实写值并作为普通冲突的值状态基线。
        let current_root = self.0.table.0.root.lock().clone();
        for (key, action) in &actions {
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

        // WAL 输出可在取得 prepare 锁前由不可变 actions 生成，缩短全表预留锁临界区。
        let write_buf = self.prepare_output(&actions);
        let mut prepare = self.0.table.0.prepare.lock();
        for (key, action) in &actions {
            if has_prepared_conflict(&prepare, key, mode, action) {
                conflict_keys.push((key.clone(),
                                    VersionConflictKind::TransactionConflict));
            }
        }
        if !conflict_keys.is_empty() {
            return Err(self.prepare_conflict_error(conflict_kind, conflict_keys));
        }

        // 冲突检查全部通过后，才原子地把事务私有动作移入表级 prepare 集合。此事务后续
        // commit/rollback 只能通过 TID 从该集合取走一次，禁止重复 prepare/commit 由上层协议保证。
        let _ = mem::replace(&mut *self.0.actions.lock(), XHashMap::default());
        prepare.insert(self.get_transaction_uid().unwrap(), PreparedActions {
            mode,
            actions,
        });
        Ok(write_buf)
    }

    fn prepare_output(&self,
                      actions: &XHashMap<Binary, KVActionLog>) -> Option<Vec<u8>> {
        let writed_count = actions
            .values()
            .filter(|action| matches!(action,
                                     KVActionLog::Write(Some(_))
                                         | KVActionLog::DirtyWrite(Some(_))))
            .count() as u64;
        if writed_count == 0 {
            return None;
        }

        // 当前 LogWrite 合法内部动作只包含 Some(value) upsert；None/delete 和 Read 均不写入
        // 聚合 WAL。返回 None 表示无 WAL payload，不表示可写事务可以跳过节点 commit。
        let mut buf = Vec::new();
        self.0.table.init_table_prepare_output(&mut buf, writed_count);
        for (key, action) in actions {
            match action {
                KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                    self.0.table.append_key_value_to_table_prepare_output(&mut buf,
                                                                          key,
                                                                          Some(value));
                },
                _ => (),
            }
        }
        Some(buf)
    }

    fn prepare_conflict_error(&self,
                              conflict_kind: PrepareConflictKind,
                              keys: Vec<(Binary, VersionConflictKind)>) -> KVTableTrError {
        // LogWrite 当前不开放版本服务，但其公共事务枚举仍必须输出同一分类载荷；完整边界见
        // docs/VERSION_CONFLICT_KIND_DESIGN.md，除此之外不扩大 LogWrite 行为。
        let key = keys[0].0.clone();
        match conflict_kind {
            PrepareConflictKind::Common => {
                KVTableTrError::new_transaction_error(
                    ErrorLevel::Normal,
                    format!("Prepare only writable table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, reason: committed state or prepared reservation changed",
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

    /// 为 WAL 恢复重建 LogWrite 的已提交内存状态和 prepare 输入。
    ///
    /// 该入口信任已经通过根 WAL 校验的恢复动作，故意绕过普通并发冲突检查，并在登记 prepare
    /// 前直接修改当前根。它不是业务事务的快速 prepare，也不得与正常活跃事务并发调用；恢复
    /// 调度器必须保证启动/修复阶段的独占边界。
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

/// LogWrite 子事务的共享可变状态。
///
/// `root_ref` 是创建事务时的不可变 COW 基线，`root_mut` 是叠加本事务动作后的私有候选根；
/// `actions` 在 prepare 成功后被移动到表级 `prepare`，commit/rollback 再按 TID 取走。
struct InnerLogWTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    /// 事务来源，仅用于限流、诊断和错误上下文。
    source:             Atom,
    /// 由事务管理器在 start 时为整棵树统一设置的事务 ID。
    tid:                SpinLock<Option<Guid>>,
    /// 需要根 WAL 时由事务管理器为整棵树统一设置的提交确认占位 ID。
    cid:                SpinLock<Option<Guid>>,
    /// 由事务管理器推进的 2PC 节点状态；SpinLock 只保护短小同步读写。
    status:             SpinLock<Transaction2PcStatus>,
    /// 是否允许动作；只读节点在 prepare/commit 入口由事务框架短路。
    writable:           bool,
    /// 是否要求写根 WAL，不表示是否拥有独立表数据文件。
    persistence:        AtomicBool,
    /// 预提交观察超时，单位毫秒；实际计时由事务管理器负责。
    prepare_timeout:    u64,
    /// 提交观察超时，单位毫秒；不改变 collector 的表日志确认语义。
    commit_timeout:     u64,
    /// 在 `root_ref` 上应用本事务动作后的私有 COW 候选根。
    root_mut:           SpinLock<OrdMap<Tree<Binary, Binary>>>,
    /// 创建事务瞬间的已提交根快照，用于逐 Key 值状态冲突比较。
    root_ref:           OrdMap<Tree<Binary, Binary>>,
    /// 对应表的共享句柄，保证事务存活期间表状态不会提前释放。
    table:              LogWriteTable<C, Log>,
    /// 本事务每个 Key 的最终动作；同 Key 后写覆盖先写。
    actions:            SpinLock<XHashMap<Binary, KVActionLog>>,
    /// 可选版本协议快照、模式、期望版本和 commit 回执汇聚器。
    version_context:    Option<TableVersionContext>,
}

/// 从 LogFile 冷启动重建 LogWrite 内存根的加载器。
///
/// `removed` 记录已观察到的 tombstone，防止继续扫描旧日志时把更早值复活；`root` 中已有 Key
/// 同样会跳过更旧记录。正确性依赖 LogFile 的恢复迭代顺序契约，而不是 HashMap 的遍历顺序。
struct LogWriteTableLoader<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    /// 每个日志文件实际采用的 Key 数和 Key/Value 字节数，仅用于启动日志。
    statistics:         XHashMap<PathBuf, (u64, u64)>,
    /// 已由较新删除记录遮蔽的 Key 集合。
    removed:            XHashMap<Vec<u8>, ()>,
    /// 被重建的表；加载期间由启动流程独占。
    table:              LogWriteTable<C, Log>,
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> PairLoader for LogWriteTableLoader<C, Log> {
    fn is_require(&self, _log_file: Option<&PathBuf>, key: &Vec<u8>) -> bool {
        //不在已删除关键字表中且不在只写日志表的内存表中的关键字，才允许被加载
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

            //加载到只写日志表的内存表中
            self.table.0.root.lock().insert(Binary::new(key), Binary::new(value));
        } else {
            //删除指定关键字的值，则不需要加载到只写日志表的内存表中，并记录到已删除关键字表中
            self.removed.insert(key, ());
        }
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> LogWriteTableLoader<C, Log> {
    /// 构建一个只写日志表的加载器
    pub fn new(table: LogWriteTable<C, Log>) -> Self {
        LogWriteTableLoader {
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

/// 把已经发布到内存根的事务批量写入 LogWrite 的独立 LogFile，并在成功后逐事务确认。
///
/// `timeout` 只控制定时入口开始整理前的等待；容量入口传 None 立即尝试。`collecting` 用 CAS
/// 保证定时入口和容量入口最多一个 owner。返回统计是本次取出的事务数、动作数和字节数；
/// 竞争失败返回零统计，不代表队列为空。
///
/// 当前实现会在持有 `waits` 异步锁时排空 FIFO、append 并等待 `delay_commit`，从而保持本批次
/// 边界稳定；同期 producer 只会异步挂起，不会同步阻塞 worker，但高延迟 I/O 会延长入队等待。
/// 这是已知实现边界，不应被改写为“完全无锁”语义。
async fn collect_waits<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(table: &LogWriteTable<C, Log>,
  timeout: Option<usize>) -> Result<(Duration, (usize, usize, usize)), (Duration, (usize, usize, usize))> {
    //等待指定的时间
    if let Some(timeout) = timeout {
        //需要等待指定时间后，再开始整理
        table.0.rt.timeout(timeout).await;
    }

    // Acquire 与结束路径的 Release 配对，使新 owner 能观察前一轮对队列和日志状态的发布。
    if let Err(_) = table.0.collecting.compare_exchange(false,
                                                        true,
                                                        Ordering::Acquire,
                                                        Ordering::Relaxed) {
        //正在异步整理，则忽略本次异步整理
        return Ok((Instant::now().elapsed(), (0, 0, 0)));
    }

    //将只写日志表中等待写入日志文件的事务，写入日志文件
    let mut waits = VecDeque::new();
    let mut log_uid = 0;
    let mut trs_len = 0;
    let mut keys_len = 0;
    let mut bytes_len = 0;

    let now = Instant::now();
    {
        // 持锁排空当前 FIFO 并冻结批次。被取出的事务不重新入队：若本批表日志失败，不发送
        // confirm，根 WAL 继续保持未确认，后续由数据库恢复流程重放，而不是在活跃进程内盲重试。
        let mut locked = table
            .0
            .waits
            .lock()
            .await;

        while let Some((wait_tr, actions, confirm)) = locked.pop_front() {
            for (key, actions) in actions.iter() {
                match actions {
                    KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                        //删除了只写日志表中指定关键字的值
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
                        //插入或更新了只写日志表中指定关键字的值
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

        // append 只进入 LogFile 的待提交缓冲；delay_commit 成功才是本批独立表日志可确认门禁。
        if let Err(e) = table
            .0
            .log_file
            .delay_commit(log_uid,
                          false,
                          DEFAULT_LOG_FILE_COMMIT_DELAY_TIMEOUT)
            .await {
            // 写入日志文件失败则中止本批次，并且有意不调用任何 confirm：成功计数保持未完成，
            // 根 WAL 留给后续恢复。错误目前只通过日志和 collect 返回值可观察。
            // 详见 CONTRACT-CFM-001：docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only。
            table.0.collecting.store(false, Ordering::Release); //设置为已整理结束
            error!("Collect only writable table failed, table: {:?}, transactions: {}, keys: {}, bytes: {}, reason: {:?}",
            table.name().as_str(),
            trs_len,
            keys_len,
            bytes_len,
            e);

            return Err((now.elapsed(), (trs_len, keys_len, bytes_len)));
        }
    }

    // 日志文件已经成功落盘；释放 waits 锁以后再执行确认器，避免回调间接进入数据库逻辑时
    // 与 producer/collector 队列锁形成重入或锁序环。该回调不是失败结果通道，失败路径已在
    // 上方通过“不调用”表达；单个确认器返回错误只记录日志，不能撤销已经落盘的表日志。
    for (wait_tr, confirm) in waits {
        if let Err(e) = confirm(wait_tr.get_transaction_uid().unwrap(),
                                wait_tr.get_commit_uid().unwrap(),
                                Ok(())) {
            error!("Collect only writable table failed, table: {:?}, transactions: {}, keys: {}, bytes: {}, reason: {:?}",
                    table.name().as_str(),
                    trs_len,
                    keys_len,
                    bytes_len,
                    e);
        }
    }
    table.0.collecting.store(false, Ordering::Release); //设置为已整理结束

    Ok((now.elapsed(), (trs_len, keys_len, bytes_len)))
}
