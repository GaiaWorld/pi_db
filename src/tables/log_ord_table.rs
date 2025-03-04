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
use pi_ordmap::{ordmap::{Iter, OrdMap, Keys, Entry}, asbtree::Tree};
use pi_async_transaction::{AsyncTransaction,
                           Transaction2Pc,
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

use crate::{Binary, KVAction, TableTrQos, KVActionLog, KVDBCommitConfirm, KVTableTrError, TransactionDebugEvent, transaction_debug_logger,
            db::{KVDBTransaction, KVDBChildTrList},
            tables::KVTable,
            utils::KVDBEvent,
            KVDBTableType};

///
/// 默认的日志文件延迟提交的超时时长，单位ms
///
const DEFAULT_LOG_FILE_COMMIT_DELAY_TIMEOUT: usize = 1000;

///
/// 有序的日志数据表
///
#[derive(Clone)]
pub struct LogOrderedTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerLogOrderedTable<C, Log>>);

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for LogOrderedTable<C, Log> {}
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

    fn transaction(&self,
                   source: Atom,
                   is_writable: bool,
                   is_persistent: bool,
                   prepare_timeout: u64,
                   commit_timeout: u64) -> Self::Tr {
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
    /// 构建一个有序日志表
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

// 内部有序日志数据表
struct InnerLogOrderedTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    //表名
    name:           Atom,
    //有序日志表的根节点
    root:           Mutex<OrdMap<Tree<Binary, Binary>>>,
    //有序日志表的预提交表
    prepare:        Mutex<XHashMap<Guid, XHashMap<Binary, KVActionLog>>>,
    //异步运行时
    rt:             MultiTaskRuntime<()>,
    //等待异步写日志文件的已提交的有序日志事务列表
    waits:          AsyncMutex<VecDeque<(LogOrdTabTr<C, Log>, XHashMap<Binary, KVActionLog>, <LogOrdTabTr<C, Log> as Transaction2Pc>::CommitConfirm)>>,
    //等待异步写日志文件的已提交的有序日志事务的键值对大小
    waits_size:     AtomicUsize,
    //等待异步写日志文件的已提交的有序日志事务大小限制
    waits_limit:    usize,
    //等待异步写日志文件的超时时长，单位毫秒
    wait_timeout:   usize,
    //是否正在整理等待异步写日志文件的已提交的有序日志事务列表
    collecting:     AtomicBool,
    //日志文件
    log_file:       LogFile,
    //表事件通知器
    notifier:       Option<Sender<KVDBEvent<Guid>>>,
}

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for InnerLogOrderedTable<C, Log> {}
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for InnerLogOrderedTable<C, Log> {}

///
/// 有序日志表事务
///
#[derive(Clone)]
pub struct LogOrdTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerLogOrdTabTr<C, Log>>);

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for LogOrdTabTr<C, Log> {}
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
            //移除事务在有序日志表的预提交表中的操作记录
            let transaction_uid = tr.get_transaction_uid().unwrap();
            let _ = tr.0.table.0.prepare.lock().remove(&transaction_uid);

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
            if tr.is_writable() {
                //可写事务预提交
                #[allow(unused_assignments)]
                let mut write_buf = None; //默认的写操作缓冲区

                {
                    //同步锁住有序日志表的预提交表，并进行预提交表的检查和修改
                    let mut prepare_locked = tr.0.table.0.prepare.lock();

                    //将事务的操作记录与表的预提交表进行比较
                    let mut buf = Vec::new();
                    let mut writed_count = 0;
                    for (_key, action) in tr.0.actions.lock().iter() {
                        match action {
                            KVActionLog::Write(_) | KVActionLog::DirtyWrite(_) => {
                                //对指定关键字进行了写操作，则增加本次事务写操作计数
                                writed_count += 1;
                            }
                            KVActionLog::Read => (), //忽略指定关键字的读操作计数
                        }
                    }
                    tr
                        .0
                        .table
                        .init_table_prepare_output(&mut buf,
                                                   writed_count); //初始化本次表事务的预提交输出缓冲区

                    let init_buf_len = buf.len(); //获取初始化本次表事务的预提交输出缓冲区后，缓冲区的长度
                    for (key, action) in tr.0.actions.lock().iter() {
                        if let Err(e) = tr
                            .check_prepare_conflict(&mut prepare_locked,
                                                    key,
                                                    action) {
                            //尝试表的预提交失败，则立即返回错误原因
                            return Err(e);
                        }

                        if !action.is_dirty_writed() {
                            //非脏写操作需要对根节点冲突进行检查
                            if let Err(e) = tr
                                .check_root_conflict(key) {
                                //尝试表的预提交失败，则立即返回错误原因
                                return Err(e);
                            }
                        }

                        //指定关键字的操作预提交成功，则将写操作写入预提交缓冲区
                        match action {
                            KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                                tr.0.table.append_key_value_to_table_prepare_output(&mut buf, key, None);
                            },
                            KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                                tr.0.table.append_key_value_to_table_prepare_output(&mut buf, key, Some(value));
                            },
                            _ => (), //忽略读操作
                        }
                    }

                    if buf.len() <= init_buf_len {
                        //本次事务没有对本地表的写操作，则设置写操作缓冲区为空
                        write_buf = None;
                    } else {
                        //本次事务有对本地表的写操作，则写操作缓冲区为指定的预提交缓冲区
                        write_buf = Some(buf);
                    }

                    //获取事务的当前操作记录，并重置事务的当前操作记录
                    let actions = mem::replace(&mut *tr.0.actions.lock(), XHashMap::default());

                    //将事务的当前操作记录，写入表的预提交表
                    prepare_locked.insert(tr.get_transaction_uid().unwrap(), actions);
                }
                //跟踪预提交
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

                Ok(write_buf)
            } else {
                //只读事务，则不需要同步锁住有序日志表的预提交表，并立即返回
                Ok(None)
            }
        }.boxed()
    }

    fn commit(&self, confirm: <Self as Transaction2Pc>::CommitConfirm)
              -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        let tr = self.clone();

        async move {
            //移除事务在有序日志表的预提交表中的操作记录
            let transaction_uid = tr.get_transaction_uid().unwrap();

            //从有序日志表的预提交表中移除当前事务的操作记录
            let actions = {
                let mut table_prepare = tr
                    .0
                    .table
                    .0
                    .prepare
                    .lock();
                let actions = table_prepare.get(&transaction_uid); //获取有序日志表，本次事务预提交成功的相关操作记录

                //更新有序日志表的根节点
                if let Some(actions) = actions {
                    {
                        let mut locked = tr.0.table.0.root.lock();
                        if !locked.ptr_eq(&tr.0.root_ref) {
                            //有序日志表的根节点在当前事务执行过程中已改变，
                            //一般是因为其它事务更新了与当前事务无关的关键字，
                            //则将当前事务的修改直接作用在当前有序日志表中
                            for (key, action) in actions.iter() {
                                match action {
                                    KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                                        //删除指定关键字
                                        let _ = locked.delete(key, false);
                                    },
                                    KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                                        //插入或更新指定关键字
                                        let _ = locked.upsert(key.clone(), value.clone(), false);
                                    },
                                    KVActionLog::Read => (), //忽略读操作
                                }
                            }
                        } else {
                            //有序日志表的根节点在当前事务执行过程中未改变，则用本次事务修改并提交成功的根节点替换有序日志表的根节点
                            *locked = tr.0.root_mut.lock().clone();
                        }
                    }

                    //有序日志表提交完成后，从有序日志表的预提交表中移除当前事务的操作记录
                    table_prepare.remove(&transaction_uid).unwrap()
                } else {
                    XHashMap::default()
                }
            };

            if tr.is_require_persistence() {
                //持久化的有序日志表事务，则异步将表的修改写入日志文件后，再确认提交成功
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
        let ptr = Box::into_raw(Box::new(self.0.root_mut.lock().keys(key.as_ref(), descending))) as usize;

        let stream = stream! {
            let mut iterator = unsafe {
                Box::from_raw(ptr as *mut Keys<'_, Tree<<Self as KVAction>::Key, <Self as KVAction>::Value>>)
            };

            while let Some(key) = iterator.next() {
                //从迭代器获取到下一个关键字
                yield key.clone();
            }
        };

        stream.boxed()
    }

    fn values<'a>(&self,
                  key: Option<<Self as KVAction>::Key>,
                  descending: bool)
                  -> BoxStream<'a, (<Self as KVAction>::Key, <Self as KVAction>::Value)> {
        let ptr = Box::into_raw(Box::new(self.0.root_mut.lock().iter(key.as_ref(), descending))) as usize;

        let stream = stream! {
            let mut iterator = unsafe {
                Box::from_raw(ptr as *mut <Tree<<Self as KVAction>::Key, <Self as KVAction>::Value> as Iter<'_>>::IterType)
            };

            while let Some(Entry(key, value)) = iterator.next() {
                //从迭代器获取到下一个键值对
                yield (key.clone(), value.clone());
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
> LogOrdTabTr<C, Log> {
    // 构建一个有序日志表事务
    #[inline]
    fn new(source: Atom,
           is_writable: bool,
           is_persistent: bool,
           prepare_timeout: u64,
           commit_timeout: u64,
           table: LogOrderedTable<C, Log>) -> Self {
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
        };

        LogOrdTabTr(Arc::new(inner))
    }

    // 检查有序日志表的预提交表的读写冲突
    fn check_prepare_conflict(&self,
                              prepare: &mut XHashMap<Guid, XHashMap<Binary, KVActionLog>>,
                              key: &Binary,
                              action: &KVActionLog)
                              -> Result<(), KVTableTrError> {
        for (guid, actions) in prepare.iter() {
            match actions.get(key) {
                Some(KVActionLog::Read) => {
                    match action {
                        KVActionLog::Read | KVActionLog::DirtyWrite(_) => {
                            //本地预提交事务对相同的关键字也执行了读操作或脏写操作，则不存在读写冲突，并继续检查预提交表中是否存在读写冲突
                            continue;
                        },
                        KVActionLog::Write(_) => {
                            //本地预提交事务对相同的关键字执行了写操作，则存在读写冲突
                            return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal,
                                                                                                     format!("Prepare log ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, confilicted_transaction_uid: {:?}, reason: require write key but reading now",
                                                                                                             self.0.table.name().as_str(),
                                                                                                             key,
                                                                                                             self.0.source,
                                                                                                             self.get_transaction_uid(),
                                                                                                             self.get_prepare_uid(),
                                                                                                             guid)));
                        },
                    }
                },
                Some(KVActionLog::DirtyWrite(_)) => {
                    //有序日志表的预提交表中的一个预提交事务与本地预提交事务操作了相同的关键字，且是脏写操作，则不存在读写冲突，并继续检查预提交表中是否存在读写冲突
                    continue;
                },
                Some(KVActionLog::Write(_)) => {
                    match action {
                        KVActionLog::DirtyWrite(_) => {
                            //本地预提交事务对相同的关键字也执行了脏写操作，则不存在读写冲突，并继续检查预提交表中是否存在读写冲突
                            continue;
                        },
                        _ => {
                            //有序日志表的预提交表中的一个预提交事务与本地预提交事务操作了相同的关键字，且是写操作，则存在读写冲突
                            return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal,
                                                                                                     format!("Prepare log ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, confilicted_transaction_uid: {:?}, reason: writing now",
                                                                                                             self.0.table.name().as_str(),
                                                                                                             key,
                                                                                                             self.0.source,
                                                                                                             self.get_transaction_uid(),
                                                                                                             self.get_prepare_uid(),
                                                                                                             guid)));
                        },
                    }
                },
                None => {
                    //有序日志表的预提交表中没有任何预提交事务与本地预提交事务操作了相同的关键字，则不存在读写冲突，并继续检查预提交表中是否存在读写冲突
                    continue;
                },
            }
        }

        Ok(())
    }

    // 检查有序日志表的根节点冲突
    fn check_root_conflict(&self, key: &Binary) -> Result<(), KVTableTrError> {
        let b = self.0.table.0.root.lock().ptr_eq(&self.0.root_ref);
        if !b {
            //有序日志表的根节点在当前事务执行过程中已改变
            let key = key.clone();
            match self.0.table.0.root.lock().get(&key) {
                None => {
                    //事务的当前操作记录中的关键字，在当前表中不存在
                    match self.0.root_ref.get(&key) {
                        None => {
                            //事务的当前操作记录中的关键字，在事务创建时的表中也不存在
                            //表示此关键字是在当前事务内新增的，则此关键字的操作记录可以预提交
                            //并继续其它关键字的操作记录的预提交
                            ()
                        },
                        _ => {
                            //事务的当前操作记录中的关键字，在事务创建时的表中已存在
                            //表示此关键字在当前事务执行过程中被删除，则此关键字的操作记录不允许预提交
                            //并立即返回当前事务预提交冲突
                            return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal,
                                                                                                     format!("Prepare log ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: the key is deleted in table while the transaction is running",
                                                                                                             self.0.table.name().as_str(),
                                                                                                             key,
                                                                                                             self.0.source,
                                                                                                             self.get_transaction_uid(),
                                                                                                             self.get_prepare_uid())));
                        },
                    }
                },
                Some(root_value) => {
                    //事务的当前操作记录中的关键字，在当前表中已存在
                    match self.0.root_ref.get(&key) {
                        Some(copy_value) if Binary::binary_equal(root_value, copy_value) => {
                            //事务的当前操作记录中的关键字，在事务创建时的表中也存在，且值引用相同
                            //表示此关键字在当前事务执行过程中未改变，且值也未改变，则此关键字的操作记录允许预提交
                            //并继续其它关键字的操作记录的预提交
                            ()
                        },
                        _ => {
                            //事务的当前操作记录中的关键字，与事务创建时的表中的关键字不匹配
                            //表示此关键字在当前事务执行过程中未改变，但值已改变，则此关键字的操作记录不允许预提交
                            //并立即返回当前事务预提交冲突
                            return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal,
                                                                                                     format!("Prepare log ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: the value is updated in table while the transaction is running",
                                                                                                             self.0.table.name().as_str(),
                                                                                                             key,
                                                                                                             self.0.source,
                                                                                                             self.get_transaction_uid(),
                                                                                                             self.get_prepare_uid())));
                        },
                    }
                },
            }
        }

        Ok(())
    }

    // 预提交所有修复修改
    // 在表的当前根节点上执行键值对操作中的所有写操作
    // 将有序日志表事务的键值对操作记录移动到对应的有序日志表的预提交表，一般只用于修复有序日志表
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
        self.0.table.0.prepare.lock().insert(transaction_uid, actions);
    }
}

// 内部有序日志表事务
struct InnerLogOrdTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    source:             Atom,                                                                       //事件源
    tid:                SpinLock<Option<Guid>>,                                                     //事务唯一id
    cid:                SpinLock<Option<Guid>>,                                                     //事务提交唯一id
    status:             SpinLock<Transaction2PcStatus>,                                             //事务状态
    writable:           bool,                                                                       //事务是否可写
    persistence:        AtomicBool,                                                                 //事务是否持久化
    prepare_timeout:    u64,                                                                        //事务预提交超时时长，单位毫秒
    commit_timeout:     u64,                                                                        //事务提交超时时长，单位毫秒
    root_mut:           SpinLock<OrdMap<Tree<Binary, Binary>>>,                                     //有序日志表的根节点的可写复制
    root_ref:           OrdMap<Tree<Binary, Binary>>,                                               //有序日志表的根节点的只读复制
    table:              LogOrderedTable<C, Log>,                                                    //事务对应的有序日志表
    actions:            SpinLock<XHashMap<Binary, KVActionLog>>,                                    //事务内操作记录
}

// 有序日志表的加载器
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

// 异步整理有序日志表中，等待写入日志文件的事务，
// 返回本次整理消耗的时间，本次写入日志文件成功的事务数、关键字数和字节数，以及本次写入日志文件失败的事务数、关键字数和字节数
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
            //写入日志文件失败，则立即中止本次整理
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

    //写入日志文件成功，则调用指定事务的确认提交回调，并继续写入下一个事务
    if let Some(notifier) = table.0.notifier.as_ref() {
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


