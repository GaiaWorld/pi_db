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
            TableKeyConflict,
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

/// Meta 独立表日志一次 `delay_commit` 的最大延迟，单位毫秒。
///
/// 该常量只影响已经发布到内存根、正在写入 Meta `LogFile` 的批次，不是根事务 prepare/commit
/// timeout，也不改变根 WAL 的提交成功边界。
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

    /// 返回内部 Meta 表名的共享字符串 owner；正常数据库固定为 `.tables_meta`。
    fn name(&self) -> <Self as KVTable>::Name {
        self.0.name.clone()
    }

    /// 返回 Meta 独立 `LogFile` 的目录，不是数据库根 WAL 目录。
    fn path(&self) -> Option<&Path> {
        Some(self.0.log_file.path())
    }

    /// Meta 定义必须写入根 WAL 和独立表日志，因此表能力恒为持久化。
    #[inline]
    fn is_persistent(&self) -> bool {
        true
    }

    /// Meta 使用有序 COW 根，迭代顺序按编码后的表名 Key 排列。
    fn is_ordered(&self) -> bool {
        true
    }

    /// 返回调用瞬间已提交 Meta 根的记录数；不包含事务私有修改或 prepared 动作。
    fn len(&self) -> usize {
        self.0.root.lock().size()
    }

    /// 返回当前 COW 根维护的逻辑字节估算，不是进程 RSS、文件大小或根 WAL 大小。
    fn size(&self) -> u64 {
        let root_copy = self.0.root.lock().clone();
        root_copy.full_bytes_size()
    }

    /// 创建一个未托管的 Meta 叶事务。
    ///
    /// 生产根事务通常使用 `new_managed` 绑定精确版本表；本入口还用于启动 iterator 的 detached
    /// 快照 owner。`is_persistent` 表示动作是否生成根 WAL 片段，不改变 Meta 表自身持久化能力。
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

    /// 强制切分 Meta 表日志，为后续只读文件整理建立边界。
    ///
    /// 本操作不 flush 根 WAL、不推进事务状态，也不等待当前 `waits` 队列；失败返回可恢复的
    /// Normal maintenance 错误。
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

    /// 整理已经切分出的 Meta 只读日志文件。
    ///
    /// `LogFile::collect` 自行串行化存储状态；本层不重试，也不持有 Meta root/prepare/waits 锁。
    /// 成功只表示表日志整理完成，不代表任何根 WAL 新增确认。
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
    /// 在不创建事务的前提下点读当前已提交 COW 根。
    ///
    /// 单独读取 value 只需本方法内部的 root mutex，可得到调用瞬间旧或新但自洽的值；需要把
    /// value 与 Key 版本原子配对的 `query_with_version` 仍必须由调用方持有 publication read。
    /// 本方法不登记事务动作、不租用 snapshot、不执行 I/O。
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
    /// 每轮串行 drain `waits`；它不参与根 WAL append，只有表日志成功后才发送提交确认。当前
    /// `spawn` 结果被忽略：合法启动契约要求 runtime 仍可接收任务；runtime 拒绝任务不属于可继续
    /// 服务的成功环境。该永久 owner 和关闭边界归档于 `FIND-LIFE-001`，本构造器不提供 Join。
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
/// `delay_commit().await`。版本事务以 publication 作为最外层门，但 `actions`、`root`、`prepare`
/// 都是分别取得并释放的短临界区：prepare 依次观察 actions/root 后才取得 prepare，commit 先
/// 释放 prepare 再取得 root；不存在 `prepare` 与 `root` 的嵌套 guard。collector 不访问上述
/// 数据/版本锁，只使用 `collecting`、`waits` 和 `LogFile`。
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
    // 自上次 size 阈值归零后的入队累计 bytes，不是当前 FIFO 的精确大小。timer drain 不归零，
    // 因而后续小批次可能提前触发一次 size collect；这只影响整理时机，不影响动作或确认内容。
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

    /// 返回根事务创建时固定的可写能力；本层不会在动作 API 中重复执行状态检查。
    fn is_writable(&self) -> bool {
        self.0.writable
    }

    /// Meta 叶提交必须按根 child list 顺序执行，不能与同根其它叶并发发布。
    fn is_concurrent_commit(&self) -> bool {
        false
    }

    /// rollback 仅删除短 prepared 预留，当前不需要并发调度。
    fn is_concurrent_rollback(&self) -> bool {
        false
    }

    /// 返回仅用于诊断和事件的事务来源，不参与事务身份或冲突判定。
    fn get_source(&self) -> Atom {
        self.0.source.clone()
    }

    /// Meta 叶没有额外初始化阶段；实际 COW 快照已在构造时固定。
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

    /// 返回本叶动作是否必须进入根 WAL；Meta 表自身始终拥有独立持久化日志。
    fn is_require_persistence(&self) -> bool {
        self.0.persistence.load(Ordering::Relaxed)
    }

    /// 单向提升根 WAL 需求；重复调用幂等，不会立即执行 I/O。
    fn require_persistence(&self) {
        self.0.persistence.store(true, Ordering::Relaxed);
    }

    /// Meta prepare 依赖同表 prepared map 的确定顺序，当前由 manager 串行执行。
    fn is_concurrent_prepare(&self) -> bool {
        false
    }

    /// 每个 Meta 叶必须继承根 TID/CID，prepared map 只以根 TID 索引。
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
            // prepare 锁只用于取走预留，并在独立块末释放；它不能与 root guard 嵌套，更不能
            // 延伸到后续异步 LogFile 路径。
            let publication = match tr.0.version_context.as_ref() {
                Some(context) => Some(context.versions().publication().write().await),
                None => None,
            };
            // 正常 prepare 和 prepare_repair 都会先以根 TID 登记；后者固定为 Ordinary mode。
            // 因此合法 replay 虽跳过事务框架标准 prepare，也必须在这里取得匹配项。
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
                        format!("Commit meta table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, expected_mode: {:?}, prepared_mode: {:?}, reason: prepared action protocol mismatch after entering non-rollbackable commit",
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
                        format!("Commit meta table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, expected_mode: {:?}, reason: prepared actions missing after entering non-rollbackable commit",
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
                    // root guard 持续覆盖数据、版本和 revision 发布；publication write 又阻止
                    // query_with_version 在两者之间观察。回执只收集本事务的显式 Versioned 写，
                    // SchemaCreate 的 context.receipt 固定为 None。
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
                    // waits guard 只覆盖一次 FIFO 入队；在调用 collect_waits 前已经释放，避免
                    // 同一任务重入异步 mutex。事务 Arc 会一直保活到成功确认或失败批次被丢弃。
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

    /// 读取事务私有根而不登记 Read；它不是绕过事务读取共享最新根。
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

    /// 读取事务私有根，并在该 Key 尚无最终动作时登记普通 Read。
    ///
    /// 当前实现会在持有短 `actions` guard 时取得 `root_mut` guard；模块内不存在
    /// `root_mut -> actions` 反向路径。该历史嵌套不跨 await 或 I/O，不能从本注释推导为通用锁序。
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

    /// 以 DirtyWrite 覆盖该 Key 的最终动作，并修改事务私有 COW 根。
    ///
    /// 外部协议禁止直接操作 `.tables_meta`，该分支只记录内部现状；dirty 与普通动作混用不保证
    /// 事务安全，prepare 的 dirty 冲突边界见 `FIND-DIRTY-001`。
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

    /// 以普通 Write 覆盖该 Key 的最终动作；共享已提交 Meta 根在 commit 前保持不变。
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

    /// 在私有根登记 DirtyWrite tombstone；命中也不承诺返回旧值。
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
            // pi_ordmap 当前 `delete(_, false)` 命中时返回 Some(None)，所以下方旧值分支不会
            // 产生 Some；保留分支不等于允许把 copy 改为 true。
            if let Some(Some(value)) = tr.0.root_mut.lock().delete(&key, false) {
                //指定关键字存在
                return Ok(Some(value));
            }

            Ok(None)
        }.boxed()
    }

    /// 在私有根登记普通 Write tombstone；结果不能用于判断表定义原先是否存在。
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
            // 与 dirty_delete 相同，copy=false 使命中结果也是 None；这是冻结的逐表返回语义。
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
        // 当前兼容钩子忽略 Key；boxed future 首次 poll 立即成功，不触碰 Meta root、prepare、
        // waits 或日志。外部不得绕过专用 DDL API直接操作 Meta，见 ROOT-KEY-HOOK-001。
        async move {
            Ok(())
        }.boxed()
    }

    fn unlock_key(&self, _key: <Self as KVAction>::Key)
                  -> BoxFuture<Result<(), <Self as KVAction>::Error>> {
        // 未持锁、重复调用和任意 Key 都同样成功；只分配立即 ready 的 boxed future。
        async move {
            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> MetaTabTr<C, Log> {
    /// 返回 manager 装配的 prepare 模式，供根事务验证 schema/业务子树结构。
    ///
    /// 非 managed 内部事务没有版本上下文，继续视为 Ordinary。该只读操作不加锁、不修改
    /// snapshot lease，也不暴露给库外调用方。
    pub(crate) fn prepare_mode(&self) -> PrepareMode {
        self
            .0
            .version_context
            .as_ref()
            .map(TableVersionContext::mode)
            .unwrap_or(PrepareMode::Ordinary)
    }

    /// 构建不携带版本上下文的 Meta 叶事务。
    ///
    /// 构造期间只短暂取得共享 root mutex，并 O(1) clone COW owner；`root_ref` 和初始
    /// `root_mut` 指向同一逻辑基线。该入口不登记根 child、不分配 TID/CID、不执行 I/O。
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
    ///
    /// 调用方已经为该表确定唯一根 owner 和 PrepareMode。函数在 root mutex 内 clone 数据基线并
    /// 租用当前 completed revision，随后释放共享锁、只在私有副本上应用最终动作。它不获取
    /// publication 或 prepare，不执行 await/I/O；与 commit 的配对依赖 commit 在相同 root guard
    /// 内先发布数据/版本再推进 revision。
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
        // 只读表事务没有动作需要预留，也不生成表 WAL 片段。
        if !self.is_writable() {
            return Ok(None);
        }

        // publication read 是整个检查阶段的外层门。actions/root 各自 clone 后立即释放，最后才
        // 取得 prepare；三者从不互相嵌套。publication guard 覆盖版本、当前根和预留三类检查，
        // commit 只能在它释放后取得 publication write，避免检查后发布穿插。
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
                        conflict_keys.push((key.clone(),
                                            VersionConflictKind::ReadSetVersionMismatch));
                    }
                }
            }
        }

        // 不以根指针相同作为跳过条件；每个非 dirty 动作都比较创建时与当前逻辑值状态。Meta
        // 的 DirtyWrite 当前无条件跳过值状态比较，这只是现状分支，不是外部可直接使用 Meta KV
        // 的许可，也不是最终 dirty 隔离设计。
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

        let write_buf = self.prepare_output(&actions);
        let mut prepare = self.0.table.0.prepare.lock();
        let transaction_uid = self.get_transaction_uid().unwrap();
        // 同表兄弟节点继承相同根 TID 时，覆盖该项会让先提交的错误节点消费真正写动作。
        // 因此重复 TID 必须在动作所有权转移前作为可恢复的预提交错误拒绝。
        if has_prepared_transaction(&prepare, &transaction_uid) {
            return Err(KVTableTrError::new_transaction_error(
                ErrorLevel::Normal,
                format!("Prepare meta table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, reason: duplicate prepared transaction uid",
                        self.0.table.name().as_str(),
                        self.0.source,
                        transaction_uid)));
        }
        // prepared-vs-prepared 检查与当前事务预留插入由同一同步锁串行化；不能把循环和 insert
        // 拆到两个临界区，否则两个首次插入相同 Key 的事务可能同时通过。
        for (key, action) in &actions {
            if has_prepared_conflict(&prepare, key, mode, action) {
                conflict_keys.push((key.clone(),
                                    VersionConflictKind::TransactionConflict));
            }
        }
        if !conflict_keys.is_empty() {
            return Err(self.prepare_conflict_error(conflict_kind, conflict_keys));
        }

        // 只有全部冲突检查通过才清空事务动作并转移所有权；失败时 actions 保留供 rollback
        // 关闭事务。外部不得在同一事务上再次 prepare。
        let _ = mem::replace(&mut *self.0.actions.lock(), XHashMap::default());
        prepare.insert(transaction_uid, PreparedActions {
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
                              keys: Vec<(Binary, VersionConflictKind)>) -> KVTableTrError {
        // 调用方只在 keys 非空时进入。All 模式保留本表完整集合；根 manager 最终再跨表归一化。
        // 分类与同 Key 优先级见 docs/VERSION_CONFLICT_KIND_DESIGN.md。
        let key = keys[0].0.clone();
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
                    .map(|(key, kind)| TableKeyConflict {
                        table: self.0.table.name(),
                        key,
                        kind,
                    })
                    .collect())
            },
        }
    }

    /// 为启动 repair 重建已由根 WAL 判定为 committed 的 Meta 动作。
    ///
    /// 该内部入口刻意跳过普通冲突检查：先把 WAL 动作直接作用于当前根，再以指定 TID 放入
    /// `prepare`，使后续 replay commit 沿正常清理/确认结构完成。只能由受信 repair 调用，不能
    /// 用于在线业务事务，也不会创建版本协议上下文。动作逐项取得 root mutex，repair 启动期尚未
    /// 对业务开放，因此不需要 publication；启动流程会在全部 replay 完成后清空恢复期版本状态。
    /// 重复应用相同最终 upsert/delete 是逻辑幂等的，但不同 TID 会各自留下 prepared 项，必须由
    /// 对应 `replay_commit` 消费，不能把本方法当作可任意重复调用的公开幂等 API。
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
/// `LogFile::load` 保证从新文件到旧文件遍历，并在调用 `load` 前询问 `is_require`；脱离该调用
/// 顺序直接复用 loader 不具相同语义。Key/Value 解码由上层启动路径完成，本类型不验证 BON。
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
        // 不在已删除集合且尚未被更新日志写入 root 的 Key 才允许读取旧记录。root mutex 只覆盖
        // 一次 O(log n) 点查，不跨文件 I/O；加载器由启动线程串行驱动。
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
        // 当前 LogFile 已把 PlainAppend/Remove 投影为 Some/None；method 不参与最终状态判断。
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
/// 失败批次不会重新放回 `waits`，也不会保留可在线调用的确认器；其根 WAL 只能依赖后续启动
/// repair 收口。当前项目已明确不把存储设备/文件系统/runtime 失败纳入事务安全保证，本函数不得
/// 被解释为提供在线重试或 rollback。该边界不是最终或最佳恢复设计。
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

    // 将当前 FIFO 整批写入同一个 LogFile commit。局部 waits 只保存确认器，不保存 actions；
    // 一旦 drain 后 I/O 失败，本轮不会在内存队列中自动重试。
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
            // 持久化失败后有意不调用 confirm；根 WAL 保留，供后续启动恢复。当前 drained
            // confirm 不会重新入队，因此不存在本进程内的完整确认重试。
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

#[cfg(test)]
mod meta_local_contract_tests {
    //! Meta 表内部局部不变量测试。
    //!
    //! 测试夹具使用真实 `LogFile`，但直接构造表内层而不启动永久 collector，只用于观察私有
    //! COW 根、动作、prepared map 和 loader。根 manager、根 WAL、异步确认、DDL、重启与 repair
    //! 生产可达性仍由独立真实集成测试证明，不能由本模块测试替代。

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

    type TestTable = MetaTable<usize, CommitLogger>;

    static NEXT_TEST_ROOT: AtomicU64 = AtomicU64::new(0);

    struct LocalMetaFixture {
        table: Option<TestTable>,
        path: PathBuf,
    }

    impl LocalMetaFixture {
        fn new(label: &str) -> Self {
            let sequence = NEXT_TEST_ROOT.fetch_add(1, AtomicOrdering::Relaxed);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time must be after the Unix epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "pi_db_meta_local_{label}_{}_{}_{}",
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
                sender.send(result).expect("Meta local LogFile receiver must remain alive");
            })
                .expect("Meta local runtime must complete LogFile::open");
            let log_file = receiver
                .recv()
                .expect("Meta local LogFile result must be returned")
                .expect("Meta local LogFile must open");
            let table = MetaTable(Arc::new(InnerMetaTable {
                name: Atom::from(".tables_meta"),
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

    impl Drop for LocalMetaFixture {
        fn drop(&mut self) {
            drop(self.table.take());
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn bon_usize(value: usize) -> Binary {
        let mut buffer = WriteBuffer::new();
        value.encode(&mut buffer);
        Binary::new(buffer.bytes)
    }

    fn table_key(name: &str) -> Binary {
        crate::db::table_to_binary(&Atom::from(name))
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
    fn test_meta_metadata_leaf_identity_and_qos_contract() {
        let fixture = LocalMetaFixture::new("identity");
        let table = fixture.table();
        assert_eq!(table.name().as_str(), ".tables_meta");
        assert_eq!(table.path(), Some(fixture.path.as_path()));
        assert!(table.is_persistent());
        assert!(table.is_ordered());
        assert_eq!(table.len(), 0);
        assert_eq!(table.size(), 0);

        let transaction = table.transaction(Atom::from("Meta local identity source"),
                                            true,
                                            false,
                                            1_234,
                                            5_678);
        assert!(transaction.is_writable());
        assert!(!transaction.is_concurrent_prepare());
        assert!(!transaction.is_concurrent_commit());
        assert!(!transaction.is_concurrent_rollback());
        assert!(transaction.is_enable_inherit_uid());
        assert_eq!(transaction.get_source().as_str(), "Meta local identity source");
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

        let read_only = table.transaction(Atom::from("Meta local read only"),
                                          false,
                                          true,
                                          7,
                                          9);
        assert!(matches!(block_on(read_only.prepare()), Ok(None)));
        assert!(table.0.prepare.lock().is_empty());
    }

    /// 动作只修改私有 COW 根，同 Key 后写只保留最终动作，流固定创建瞬间的私有根。
    #[test]
    fn test_meta_private_cow_final_action_and_snapshot_contract() {
        let fixture = LocalMetaFixture::new("actions");
        let table = fixture.table();
        let retained_key = table_key("meta_local_retained");
        let deleted_key = table_key("meta_local_deleted");
        let committed_value = bon_usize(10);
        let first_value = bon_usize(11);
        let final_value = bon_usize(12);
        table.0.root.lock().upsert(deleted_key.clone(), committed_value.clone(), false);

        let transaction = table.transaction(Atom::from("Meta local actions source"),
                                            true,
                                            true,
                                            100,
                                            200);
        block_on(transaction.upsert(retained_key.clone(), first_value.clone()))
            .expect("first private Meta upsert must succeed");
        let snapshot = transaction.values(None, false);
        block_on(transaction.upsert(retained_key.clone(), final_value.clone()))
            .expect("final private Meta upsert must succeed");
        let removed = block_on(transaction.delete(deleted_key.clone()))
            .expect("private Meta delete must succeed");
        assert!(removed.is_none(), "Meta delete must not expose the old value");

        assert_binary(block_on(transaction.query(retained_key.clone())),
                      Some(&final_value),
                      "transaction must observe final private upsert");
        assert_binary(table.query_committed(&retained_key),
                      None,
                      "uncommitted Meta upsert must not reach shared root");
        assert_binary(table.query_committed(&deleted_key),
                      Some(&committed_value),
                      "uncommitted Meta delete must not reach shared root");

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
    fn test_meta_prepare_wal_conflict_ownership_and_rollback_contract() {
        let fixture = LocalMetaFixture::new("prepare");
        let table = fixture.table();
        let upsert_key = table_key("meta_local_prepare_upsert");
        let delete_key = table_key("meta_local_prepare_delete");
        let read_key = table_key("meta_local_prepare_read");
        let old_value = bon_usize(210);
        let new_value = bon_usize(211);
        table.0.root.lock().upsert(delete_key.clone(), old_value.clone(), false);

        let transaction = table.transaction(Atom::from("Meta local prepare source"),
                                            true,
                                            true,
                                            300,
                                            400);
        let tid = Guid(201);
        transaction.set_transaction_uid(tid.clone());
        block_on(transaction.upsert(upsert_key.clone(), new_value.clone()))
            .expect("private Meta upsert before prepare must succeed");
        block_on(transaction.delete(delete_key.clone()))
            .expect("private Meta delete before prepare must succeed");
        assert!(block_on(transaction.query(read_key.clone())).is_none());

        let output = block_on(transaction.prepare_conflicts())
            .expect("Meta prepare must succeed")
            .expect("Meta writes must produce a WAL fragment");
        let (table_name, write_count, offset) =
            <TestTable as KVTable>::get_init_table_prepare_output(&output, 0);
        let (writes, end) =
            <TestTable as KVTable>::get_all_key_value_from_table_prepare_output(
                &output,
                &table_name,
                write_count,
                offset);
        assert_eq!(table_name.as_str(), ".tables_meta");
        assert_eq!(write_count, 2, "Read must not enter the Meta WAL fragment");
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
            let item = prepared.get(&tid).expect("Meta prepare map must reserve the root TID");
            assert_eq!(item.mode, PrepareMode::Ordinary);
            assert_eq!(item.actions.len(), 3);
            assert!(matches!(item.actions.get(&read_key), Some(KVActionLog::Read)));
        }

        let contender = table.transaction(Atom::from("Meta local prepared contender"),
                                          true,
                                          true,
                                          500,
                                          600);
        contender.set_transaction_uid(Guid(202));
        block_on(contender.upsert(upsert_key.clone(), bon_usize(212)))
            .expect("Meta contender action must succeed locally");
        let conflict = block_on(contender.prepare_conflicts())
            .expect_err("same-Key prepared Meta contender must conflict");
        assert!(conflict.is_conflicts());
        block_on(contender.rollback()).expect("Meta contender rollback must succeed");

        assert_binary(table.query_committed(&upsert_key),
                      None,
                      "prepare must not publish Meta upsert");
        assert_binary(table.query_committed(&delete_key),
                      Some(&old_value),
                      "prepare must not publish Meta delete");
        block_on(transaction.rollback()).expect("Meta rollback must release prepared state");
        assert!(table.0.prepare.lock().is_empty());
    }

    /// Meta DirtyWrite 当前无条件跳过值状态比较；只记录实现事实，不开放外部 Meta KV 协议。
    #[test]
    fn test_meta_dirty_prepare_current_conflict_branch() {
        let fixture = LocalMetaFixture::new("dirty");
        let table = fixture.table();
        let key = table_key("meta_local_dirty");
        let initial_value = bon_usize(310);
        let private_value = bon_usize(311);
        let concurrent_value = bon_usize(312);
        table.0.root.lock().upsert(key.clone(), initial_value, false);

        let transaction = table.transaction(Atom::from("Meta local dirty source"),
                                            true,
                                            true,
                                            700,
                                            800);
        let tid = Guid(301);
        transaction.set_transaction_uid(tid.clone());
        block_on(transaction.dirty_upsert(key.clone(), private_value))
            .expect("Meta dirty upsert must succeed locally");
        table.0.root.lock().upsert(key.clone(), concurrent_value.clone(), false);

        assert!(block_on(transaction.prepare_conflicts())
            .expect("Meta DirtyWrite currently skips committed value comparison")
            .is_some());
        assert!(table.0.prepare.lock().contains_key(&tid));
        assert_binary(table.query_committed(&key),
                      Some(&concurrent_value),
                      "Meta dirty prepare must not publish its private value");
        block_on(transaction.rollback()).expect("Meta dirty rollback must succeed");
        assert!(table.0.prepare.lock().is_empty());
    }

    /// 受信 repair 直接得到最终 Meta 状态并登记 Ordinary prepared；最终动作可逻辑幂等重放。
    #[test]
    fn test_meta_repair_final_state_and_local_idempotence() {
        let fixture = LocalMetaFixture::new("repair");
        let table = fixture.table();
        let upsert_key = table_key("meta_local_repair_upsert");
        let delete_key = table_key("meta_local_repair_delete");
        let old_value = bon_usize(410);
        let repaired_value = bon_usize(411);
        table.0.root.lock().upsert(delete_key.clone(), old_value, false);

        let first = table.transaction(Atom::from("Meta local repair first"),
                                      true,
                                      true,
                                      900,
                                      1_000);
        block_on(first.upsert(upsert_key.clone(), repaired_value.clone()))
            .expect("first Meta repair upsert action must be staged");
        block_on(first.delete(delete_key.clone()))
            .expect("first Meta repair delete action must be staged");
        let first_tid = Guid(401);
        first.prepare_repair(first_tid.clone());

        assert_binary(table.query_committed(&upsert_key),
                      Some(&repaired_value),
                      "Meta repair must apply upsert directly");
        assert_binary(table.query_committed(&delete_key),
                      None,
                      "Meta repair must apply delete directly");
        assert!(first.0.actions.lock().is_empty());

        let second = table.transaction(Atom::from("Meta local repair second"),
                                       true,
                                       true,
                                       1_100,
                                       1_200);
        block_on(second.upsert(upsert_key.clone(), repaired_value.clone()))
            .expect("second Meta repair upsert action must be staged");
        block_on(second.delete(delete_key.clone()))
            .expect("second Meta repair delete action must be staged");
        let second_tid = Guid(402);
        second.prepare_repair(second_tid.clone());

        assert_eq!(table.len(), 1);
        assert_binary(table.query_committed(&upsert_key),
                      Some(&repaired_value),
                      "repeated Meta repair must retain final upsert");
        assert_binary(table.query_committed(&delete_key),
                      None,
                      "repeated Meta repair must retain final delete");
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
    fn test_meta_loader_newest_tombstone_and_statistics_contract() {
        let fixture = LocalMetaFixture::new("loader");
        let table = fixture.table();
        let newest_key = table_key("meta_local_loader_newest");
        let removed_key = table_key("meta_local_loader_removed");
        let older_key = table_key("meta_local_loader_older");
        let newest_value = bon_usize(510);
        let ignored_older_value = bon_usize(511);
        let older_value = bon_usize(512);
        let newer_path = PathBuf::from("meta-newer.log");
        let older_path = PathBuf::from("meta-older.log");
        let mut loader = MetaTableLoader::new(table.clone());

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
                      "newest Meta loader value must win");
        assert_binary(table.query_committed(&removed_key),
                      None,
                      "Meta loader tombstone must suppress older value");
        assert_binary(table.query_committed(&older_key),
                      Some(&older_value),
                      "unshadowed older Meta value must load");
        assert_ne!(table.query_committed(&newest_key), Some(ignored_older_value));
        assert_eq!(loader.log_files_len(), 2);
        assert_eq!(loader.keys_len(), 2);
        assert_eq!(loader.bytes_len(),
                   (newest_key.len() + newest_value.len()
                    + older_key.len() + older_value.len()) as u64);
    }
}
