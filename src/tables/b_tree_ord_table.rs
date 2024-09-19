use std::{mem, thread};
use std::path::{Path, PathBuf};
use std::collections::{VecDeque, HashMap, BTreeMap};
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
                           ErrorLevel,
                           AsyncTransaction,
                           UnitTransaction,
                           SequenceTransaction,
                           TransactionTree,
                           manager_2pc::Transaction2PcStatus};
use futures::{future::{FutureExt, BoxFuture},
              stream::{StreamExt, BoxStream}};
use parking_lot::{Mutex, RwLock};
use pi_async_file::file::create_dir;
use redb::{Key, Value, ReadableTableMetadata, ReadableTable, Builder as TableBuilder, Database, TypeName, TableDefinition, ReadTransaction, WriteTransaction, ReadOnlyTable, Table, Range, Durability, DatabaseError};
use async_stream::stream;
use dashmap::DashMap;
use pi_atom::Atom;
use pi_guid::Guid;
use pi_hash::XHashMap;
use pi_bon::ReadBuffer;
use log::{trace, debug, error, warn, info};
use pi_ordmap::asbtree::Tree;
use pi_ordmap::ordmap::{Entry, OrdMap};
use pi_store::log_store::log_file::LogMethod;

use crate::{Binary, KVAction, KVActionLog, KVDBCommitConfirm, KVTableTrError, TableTrQos, TransactionDebugEvent, transaction_debug_logger, db::{KVDBChildTrList, KVDBTransaction}, tables::{KVTable,
                                                                                                                                                                                            log_ord_table::{LogOrderedTable, LogOrdTabTr}}, utils::KVDBEvent, KVDBTableType};

// 默认的表文件名
const DEFAULT_TABLE_FILE_NAME: &str = "table.dat";

// 默认的表名
const DEFAULT_TABLE_NAME: TableDefinition<Binary, Binary> = TableDefinition::new("$default");

// 最小缓存大小
const MIN_CACHE_SIZE: usize = 32 * 1024;

// 默认缓存大小
const DEFAULT_CACHE_SIZE: usize = 16 * 1024 * 1024;

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
        ReadBuffer::new(data1, 0)
            .partial_cmp(&ReadBuffer::new(data2, 0))
            .unwrap()
    }
}

///
/// 有序的B树数据表
///
#[derive(Clone)]
pub struct BtreeOrderedTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerBtreeOrderedTable<C, Log>>);

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for BtreeOrderedTable<C, Log> {}
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
        if let Ok(tr) = self.0.inner.read().begin_read() {
            if let Ok(table) = tr.open_table(DEFAULT_TABLE_NAME) {
                let mut table_len = table.len().unwrap_or(0) as usize;
                let keys = self.0.cache.lock().keys(None, false);
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
            if let Err(e) = transaction.commit() {
                //写事务持久化提交失败，则立即返回错误原因
                table.0.collecting.store(false, Ordering::Release); //设置为已整理结束
                return Err(KVTableTrError::new_transaction_error(ErrorLevel::Fatal,
                                                                 format!("Compact b-tree ordered table failed, table: {:?}, , reason: {:?}",
                                                                         table.name().as_str(),
                                                                         e)));
            }

            //开始B树表的压缩
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
    /// 构建一个有序B树表，同时只允许构建一个同路径下的有序B树表
    pub async fn new<P: AsRef<Path>>(rt: MultiTaskRuntime<()>,
                                     path: P,
                                     name: Atom,
                                     cache_size: usize,
                                     enable_compact: bool,
                                     waits_limit: usize,
                                     wait_timeout: usize,
                                     notifier: Option<Sender<KVDBEvent<Guid>>>) -> Self
    {
        Self::try_new(rt,
                      path,
                      name,
                      cache_size,
                      enable_compact,
                      waits_limit,
                      wait_timeout,
                      notifier)
            .await
            .unwrap()
    }

    /// 尝试构建一个有序B树表，同时只允许构建一个同路径下的有序B树表，如果已经构建则返回空
    pub(crate) async fn try_new<P: AsRef<Path>>(rt: MultiTaskRuntime<()>,
                                                path: P,
                                                name: Atom,
                                                mut cache_size: usize,
                                                enable_compact: bool,
                                                waits_limit: usize,
                                                wait_timeout: usize,
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
                let prepare = Mutex::new(XHashMap::default());
                let waits = AsyncMutex::new(VecDeque::new());
                let waits_size = AtomicUsize::new(0);
                let collecting = AtomicBool::new(false);

                let inner = InnerBtreeOrderedTable {
                    name: name.clone(),
                    path: path.clone(),
                    inner,
                    cache,
                    prepare,
                    rt,
                    enable_compact: AtomicBool::new(enable_compact),
                    waits,
                    waits_size,
                    waits_limit,
                    wait_timeout,
                    collecting,
                    notifier,
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

// 内部有序B树数据表
struct InnerBtreeOrderedTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    //表名
    name:           Atom,
    //有序B树表的实例的磁盘路径
    path:           PathBuf,
    //有序B树表的实例
    inner:          RwLock<Database>,
    //有序B树表的临时缓存，缓存有序B树表两次持久化之间写入的数据，并在有序B树表持久化后清除缓存的数据
    cache:          Mutex<OrdMap<Tree<Binary, Option<Binary>>>>,
    //有序B树表的预提交表
    prepare:        Mutex<XHashMap<Guid, XHashMap<Binary, KVActionLog>>>,
    //异步运行时
    rt:             MultiTaskRuntime<()>,
    //是否允许对有序B树表进行整理压缩
    enable_compact: AtomicBool,
    //等待异步写日志文件的已提交的有序日志事务列表
    waits:          AsyncMutex<VecDeque<(BtreeOrdTabTr<C, Log>, XHashMap<Binary, KVActionLog>, <BtreeOrdTabTr<C, Log> as Transaction2Pc>::CommitConfirm)>>,
    //等待异步写日志文件的已提交的有序日志事务的键值对大小
    waits_size:     AtomicUsize,
    //等待异步写日志文件的已提交的有序日志事务大小限制
    waits_limit:    usize,
    //等待异步写日志文件的超时时长，单位毫秒
    wait_timeout:   usize,
    //是否正在整理等待异步写日志文件的已提交的有序日志事务列表
    collecting:     AtomicBool,
    //表事件通知器
    notifier:       Option<Sender<KVDBEvent<Guid>>>,
}

///
/// 有序B树表事务
///
#[derive(Clone)]
pub struct BtreeOrdTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerBtreeOrdTabTr<C, Log>>);

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for BtreeOrdTabTr<C, Log> {}
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
            let transaction_uid = tr.get_transaction_uid().unwrap();
            let _ = tr.0.table.0.prepare.lock().remove(&transaction_uid);

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
        //开始预提交
        let tr = self.clone();

        async move {
            if tr.is_writable() {
                //可写事务预提交
                #[allow(unused_assignments)]
                let mut write_buf = None; //默认的写操作缓冲区

                {
                    //同步锁住有序B树表的预提交表，并进行预提交表的检查和修改
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

                Ok(write_buf)
            } else {
                //只读事务，则不需要同步锁住有序B树表的预提交表，并立即返回
                Ok(None)
            }
        }.boxed()
    }

    fn commit(&self, confirm: <Self as Transaction2Pc>::CommitConfirm)
              -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>>
    {
        //提交日志已写成功
        let tr = self.clone();

        async move {
            //移除事务在有序B树表的预提交表中的操作记录
            let transaction_uid = tr.get_transaction_uid().unwrap();

            //从有序B树表的预提交表中移除当前事务的操作记录
            let actions = {
                let mut table_prepare = tr
                    .0
                    .table
                    .0
                    .prepare
                    .lock();
                let actions = table_prepare.get(&transaction_uid); //获取有序B树表，本次事务预提交成功的相关操作记录

                //更新有序B树表的临时缓存的根节点
                if let Some(actions) = actions {
                    {
                        let mut locked = tr.0.table.0.cache.lock();
                        if !locked.ptr_eq(&tr.0.cache_ref) {
                            //有序B树表的临时缓存的根节点在当前事务执行过程中已改变，
                            //一般是因为其它事务更新了与当前事务无关的关键字，
                            //则将当前事务的修改直接作用在当前有序B树表的临时缓存中
                            for (key, action) in actions.iter() {
                                match action {
                                    KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                                        //删除指定关键字
                                        if let Some(Some(Some(_))) = locked.delete(key, true) {
                                            //指定关键字存在，则标记删除
                                            let _ = locked.upsert(key.clone(), None, false);
                                        }
                                    },
                                    KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                                        //插入或更新指定关键字
                                        let _ = locked.upsert(key.clone(), Some(value.clone()), false);
                                    },
                                    KVActionLog::Read => (), //忽略读操作
                                }
                            }
                        } else {
                            //有序B树表的临时缓存的根节点在当前事务执行过程中未改变，则用本次事务修改并提交成功的根节点替换有序B树表的临时缓存的根节点
                            *locked = tr.0.cache_mut.lock().clone();
                        }
                    }

                    //有序B树表提交完成后，从有序B树表的预提交表中移除当前事务的操作记录
                    table_prepare.remove(&transaction_uid).unwrap()
                } else {
                    XHashMap::default()
                }
            };

            if tr.is_require_persistence() {
                //持久化的有序B树表事务，则异步将表的修改写入日志文件后，再确认提交成功
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
        self.query(key)
    }

    fn query(&self, key: <Self as KVAction>::Key) -> BoxFuture<Option<<Self as KVAction>::Value>>
    {
        let tr = self.clone();

        async move {
            let mut actions_locked = tr.0.actions.lock();

            if let None = actions_locked.get(&key) {
                //在事务内还未未记录指定关键字的操作，则记录对指定关键字的读操作
                let _ = actions_locked.insert(key.clone(), KVActionLog::Read);
            }

            if let Some(Some(value)) = tr.0.cache_mut.lock().get(&key) {
                //指定关键字在临时缓存中存在
                return Some(value.clone());
            } else {
                //指定关键字在临时缓存中不存在，则直接从内部表中获取
                if let Ok(trans) = tr.0.table.0.inner.read().begin_read() {
                    if let Ok(inner_table) = trans.open_table(DEFAULT_TABLE_NAME) {
                        if let Ok(Some(value)) = inner_table.get(&key) {
                            return Some(value.value());
                        }
                    }
                }
            }

            None
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
            //记录对指定关键字的最新插入或更新操作
            let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::DirtyWrite(Some(value.clone())));

            //插入或更新指定的键值对
            let _ = tr.0.cache_mut.lock().upsert(key, Some(value), false);

            Ok(())
        }.boxed()
    }

    fn dirty_delete(&self, key: <Self as KVAction>::Key)
                    -> BoxFuture<Result<Option<<Self as KVAction>::Value>, <Self as KVAction>::Error>>
    {
        self.delete(key)
    }

    fn delete(&self, key: <Self as KVAction>::Key)
              -> BoxFuture<Result<Option<<Self as KVAction>::Value>, <Self as KVAction>::Error>>
    {
        let tr = self.clone();

        async move {
            //记录对指定关键字的最新删除操作，并增加写操作计数
            let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::Write(None));

            let opt = if let Some(Some(Some(value))) = tr.0.cache_mut.lock().delete(&key, true) {
                //指定关键字存在，则标记删除
                Some(value)
            } else {
                None
            };

            if let Some(value) = opt {
                let _ = tr
                    .0
                    .cache_mut
                    .lock()
                    .upsert(key, None, false);
                Ok(Some(value))
            } else {
                Ok(None)
            }
        }.boxed()
    }

    fn keys<'a>(&self,
                key: Option<<Self as KVAction>::Key>,
                descending: bool)
        -> BoxStream<'a, <Self as KVAction>::Key>
    {
        let transaction = self.clone();
        let ptr = Box::into_raw(Box::new(self.0.table.0.cache.lock().keys(key.as_ref(), descending))) as usize;
        let stream = stream! {
            let trans = match transaction.0.table.0.inner.read().begin_read() {
                Err(e) => {
                    return;
                },
                Ok(trans) => {
                    trans
                },
            };

            let mut cache_iterator = unsafe {
                Box::from_raw(ptr as *mut pi_ordmap::ordmap::Keys<'_, Tree<<Self as KVAction>::Key, <Self as KVAction>::Value>>)
            };

            let table = if let Ok(table) = trans.open_table(DEFAULT_TABLE_NAME)
            {
                table
            } else {
                //当前表还未创建完成，则只迭代缓存中的关键字
                while let Some(key) = cache_iterator.next() {
                    //从迭代器获取到下一个关键字
                    yield key.clone();
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
                let mut cache_key = None;
                let mut key = None;
                loop {
                    //从迭代器获取到关键字
                    cache_key = match cache_b {
                        0 => None, //不再获取关键字
                        1 => cache_key, //忽略获取关键字
                        _ => cache_iterator.next(), //获取关键字
                    };
                    key = match b {
                        0 => None, //不再获取关键字
                        1 => key,   //忽略获取关键字
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

                    match (cache_key, &key) {
                        (Some(cache_k), Some(Ok((key_, _value)))) => {
                            //缓存和文件迭代器都有关键字
                            let k = key_.value();
                            if descending {
                                //倒序
                                if cache_k > &k {
                                    cache_b = 2;
                                    b = 1;
                                    ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的关键字

                                    yield cache_k.clone()
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
                                    ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的关键字

                                    yield cache_k.clone()
                                }
                            } else {
                                //顺序
                                if cache_k < &k {
                                    cache_b = 2;
                                    b = 1;
                                    ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的关键字

                                    yield cache_k.clone()
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
                                    ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的关键字

                                    yield cache_k.clone()
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
                        (Some(cache_k), None) => {
                            //只有缓存迭代器有关键字
                            cache_b = 2;
                            b = 0; //关闭文件迭代器
                            ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的关键字

                            yield cache_k.clone()
                        },
                        _ => {
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
        let ptr = Box::into_raw(Box::new(self.0.table.0.cache.lock().iter(key.as_ref(), descending))) as usize;
        let stream = stream! {
            let trans = match transaction.0.table.0.inner.read().begin_read() {
                Err(e) => {
                    return;
                },
                Ok(trans) => {
                    trans
                },
            };

            let mut cache_iterator = unsafe {
                Box::from_raw(ptr as *mut <Tree<<Self as KVAction>::Key, <Self as KVAction>::Value> as pi_ordmap::ordmap::Iter<'_>>::IterType)
            };

            let table = if let Ok(table) = trans.open_table(DEFAULT_TABLE_NAME)
            {
                table
            } else {
                //当前表还未创建完成，则只迭代缓存中的键值对
                while let Some(Entry(key, value)) = cache_iterator.next() {
                    yield (key.clone(), value.clone());
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
                        _ => cache_iterator.next(), //获取键值对
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

                    match (cache_key_value, &key_value) {
                        (Some(Entry(cache_k, cache_v)), Some(Ok((key_, value_)))) => {
                            //缓存和文件迭代器都有键值对
                            let k = key_.value();
                            if descending {
                                //倒序
                                if cache_k > &k {
                                    cache_b = 2;
                                    b = 1;
                                    ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的键值对

                                    yield (cache_k.clone(), cache_v.clone())
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
                                    ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的键值对

                                    yield (cache_k.clone(), cache_v.clone())
                                }
                            } else {
                                //顺序
                                if cache_k < &k {
                                    cache_b = 2;
                                    b = 1;
                                    ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的键值对

                                    yield (cache_k.clone(), cache_v.clone())
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
                                    ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的键值对

                                    yield (cache_k.clone(), cache_v.clone())
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
                        (Some(Entry(cache_k, cache_v)), None) => {
                            //只有缓存迭代器有键值对
                            cache_b = 2;
                            b = 0; //关闭文件迭代器
                            ignores.insert(cache_k.clone(), ()); //记录在缓存中已迭代过的键值对

                            yield (cache_k.clone(), cache_v.clone())
                        },
                        _ => {
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
    // 构建一个有序B树表事务
    #[inline]
    fn new(source: Atom,
           is_writable: bool,
           is_persistent: bool,
           prepare_timeout: u64,
           commit_timeout: u64,
           table: BtreeOrderedTable<C, Log>) -> Self {
        let cache_ref = table.0.cache.lock().clone();
        let cache_mut = SpinLock::new(cache_ref.clone());
        let inner = InnerBtreeOrdTabTr {
            source,
            tid: SpinLock::new(None),
            cid: SpinLock::new(None),
            status: SpinLock::new(Transaction2PcStatus::default()),
            writable: is_writable,
            persistence: AtomicBool::new(is_persistent),
            prepare_timeout,
            commit_timeout,
            cache_mut,
            cache_ref,
            table,
            actions: SpinLock::new(XHashMap::default()),
        };

        BtreeOrdTabTr(Arc::new(inner))
    }

    // 检查有序B树表的预提交表的读写冲突
    fn check_prepare_conflict(&self,
                              prepare: &mut XHashMap<Guid, XHashMap<Binary, KVActionLog>>,
                              key: &Binary,
                              action: &KVActionLog)
        -> Result<(), KVTableTrError>
    {
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
                                                                                                     format!("Prepare b-tree ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, confilicted_transaction_uid: {:?}, reason: require write key but reading now",
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
                    //有序B树表的预提交表中的一个预提交事务与本地预提交事务操作了相同的关键字，且是脏写操作，则不存在读写冲突，并继续检查预提交表中是否存在读写冲突
                    continue;
                },
                Some(KVActionLog::Write(_)) => {
                    match action {
                        KVActionLog::DirtyWrite(_) => {
                            //本地预提交事务对相同的关键字也执行了脏写操作，则不存在读写冲突，并继续检查预提交表中是否存在读写冲突
                            continue;
                        },
                        _ => {
                            //有序B树表的预提交表中的一个预提交事务与本地预提交事务操作了相同的关键字，且是写操作，则存在读写冲突
                            return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal,
                                                                                                     format!("Prepare b-tree ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, confilicted_transaction_uid: {:?}, reason: writing now",
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
                    //有序B树表的预提交表中没有任何预提交事务与本地预提交事务操作了相同的关键字，则不存在读写冲突，并继续检查预提交表中是否存在读写冲突
                    continue;
                },
            }
        }

        Ok(())
    }

    // 检查有序B树表的临时缓存的根节点冲突
    fn check_root_conflict(&self, key: &Binary) -> Result<(), KVTableTrError> {
        let b = self.0.table.0.cache.lock().ptr_eq(&self.0.cache_ref);
        if !b {
            //有序B树表的临时缓存的根节点在当前事务执行过程中已改变
            let key = key.clone();
            match self.0.table.0.cache.lock().get(&key) {
                None => {
                    //事务的当前操作记录中的关键字，在当前表中不存在
                    match self.0.cache_ref.get(&key) {
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
                                                                                                     format!("Prepare b-tree ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: the key is deleted in table while the transaction is running",
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
                    match self.0.cache_ref.get(&key) {
                        Some(copy_value) => {
                            if root_value.is_some() && copy_value.is_some() {
                                //值都不为空，则比较内容
                                if Binary::binary_equal(root_value.as_ref().unwrap(), copy_value.as_ref().unwrap()) {
                                    //事务的当前操作记录中的关键字，在事务创建时的表中也存在，且值引用相同
                                    //表示此关键字在当前事务执行过程中未改变，且值也未改变，则此关键字的操作记录允许预提交
                                    //并继续其它关键字的操作记录的预提交
                                    ()
                                } else {
                                    //事务的当前操作记录中的关键字，与事务创建时的表中的关键字不匹配
                                    //表示此关键字在当前事务执行过程中未改变，但值已改变，则此关键字的操作记录不允许预提交
                                    //并立即返回当前事务预提交冲突
                                    return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal, format!("Prepare b-tree ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: the value is updated in table while the transaction is running", self.0.table.name().as_str(), key, self.0.source, self.get_transaction_uid(), self.get_prepare_uid())));
                                }
                            } else if root_value.is_none() && copy_value.is_none() {
                                //事务的当前操作记录中的关键字，在事务创建时的表中也存在，且值引用相同
                                //表示此关键字在当前事务执行过程中未改变，且值也未改变，则此关键字的操作记录允许预提交
                                //并继续其它关键字的操作记录的预提交
                                ()
                            } else {
                                //事务的当前操作记录中的关键字，与事务创建时的表中的关键字不匹配
                                //表示此关键字在当前事务执行过程中未改变，但值已改变，则此关键字的操作记录不允许预提交
                                //并立即返回当前事务预提交冲突
                                return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal,
                                                                                                         format!("Prepare b-tree ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: the value is updated in table while the transaction is running",
                                                                                                                 self.0.table.name().as_str(),
                                                                                                                 key,
                                                                                                                 self.0.source,
                                                                                                                 self.get_transaction_uid(),
                                                                                                                 self.get_prepare_uid())));
                            }
                        },
                        _ => {
                            //事务的当前操作记录中的关键字，与事务创建时的表中的关键字不匹配
                            //表示此关键字在当前事务执行过程中未改变，但值已改变，则此关键字的操作记录不允许预提交
                            //并立即返回当前事务预提交冲突
                            return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal,
                                                                                                     format!("Prepare b-tree ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: the value is updated in table while the transaction is running",
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
    // 将有序B树表事务的键值对操作记录移动到对应的有序B树表的预提交表，一般只用于修复有序B树表
    pub(crate) fn prepare_repair(&self, transaction_uid: Guid) {
        //获取事务的当前操作记录，并重置事务的当前操作记录
        let actions = mem::replace(&mut *self.0.actions.lock(), XHashMap::default());

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
        self.0.table.0.prepare.lock().insert(transaction_uid, actions);
    }

    // 立即删除缓存中指定关键字的值，只允许在指定关键字的值被持久化后调用
    pub(crate) fn delete_cache(&self, keys: Vec<<Self as KVAction>::Key>) {
        for key in &keys {
            let _ = self.0.cache_mut.lock().delete(key, false);
        }

        //更新有序B树表的临时缓存的根节点
        {
            let mut locked = self.0.table.0.cache.lock();
            if !locked.ptr_eq(&self.0.cache_ref) {
                //有序B树表的临时缓存的根节点在当前事务执行过程中已改变，
                //一般是因为其它事务更新了与当前事务无关的关键字，
                //则将当前事务的修改直接作用在当前有序B树表的临时缓存中
                for key in &keys {
                    let _ = locked.delete(key, false);
                }
            } else {
                //有序B树表的临时缓存的根节点在当前事务执行过程中未改变，则用本次事务修改并提交成功的根节点替换有序B树表的临时缓存的根节点
                *locked = self.0.cache_mut.lock().clone();
            }
        }
    }
}

// 内部有序B树表事务
struct InnerBtreeOrdTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    //事件源
    source:             Atom,
    //事务唯一id
    tid:                SpinLock<Option<Guid>>,
    //事务提交唯一id
    cid:                SpinLock<Option<Guid>>,
    //事务状态
    status:             SpinLock<Transaction2PcStatus>,
    //事务是否可写
    writable:           bool,
    //事务是否持久化
    persistence:        AtomicBool,
    //事务预提交超时时长，单位毫秒
    prepare_timeout:    u64,
    //事务提交超时时长，单位毫秒
    commit_timeout:     u64,
    //事务的临时缓存的可写引用
    cache_mut:          SpinLock<OrdMap<Tree<Binary, Option<Binary>>>>,
    //事务的临时缓存的只读引用
    cache_ref:          OrdMap<Tree<Binary, Option<Binary>>>,
    //事务对应的有序B树表
    table:              BtreeOrderedTable<C, Log>,
    //事务内操作记录
    actions:            SpinLock<XHashMap<Binary, KVActionLog>>,
}

// 内部事务
pub(crate) enum InnerTransaction {
    OnlyRead(ReadTransaction, Atom),        //只读事务
    Writable(WriteTransaction, Atom),       //可写事务
    WriteConflict(ReadTransaction, Atom),   //写冲突事务
    Repair(ReadTransaction, Atom),          //修复表事务
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

// 异步整理有序B树表中等待写入redb的事务，
// 返回本次整理消耗的时间，本次写入redb成功的事务数、关键字数和字节数，以及本次写入redb失败的事务数、关键字数和字节数
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

    //检查是否正在异步整理，如果并未开始异步整理，则设置为正在异步整理，并继续异步整理
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
        //在锁保护下迭代当前有序B树表的等待异步写日志文件的已提交的有序日志事务列表
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

                while let Some((wait_tr, actions, confirm)) = locked.pop_front()
                {
                    for (key, actions) in actions.iter() {
                        match actions {
                            KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                                //统计删除了有序B树表中指定关键字的值
                                if let Err(e) = inner_table.remove(key) {
                                    //删除指定关键字的值失败，则继续处理下一个操作记录
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
                                    //插入或更新指定关键字的值失败，则继续处理下一个操作记录
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

                        cache_keys.insert(key.clone(), ()); //记录需要在持久化提交成功后，从缓存中清理的关键字
                    }

                    trs_len += 1;
                    waits.push_back((wait_tr, confirm));
                }
                drop(inner_table); //在持久化提交前必须关闭redb表

                if let Err(e) = transaction.commit() {
                    //持久化提交redb失败，则立即中止本次整理
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

    //写入redb成功，则调用指定事务的确认提交回调，并继续写入下一个事务
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

    //清理已经持久化提交后的关键字在缓存中的值
    let clean_cache_transaction = table.transaction(Atom::from("Collect_waits_cache"),
                      false,
                      false,
                      5000,
                      5000);
    clean_cache_transaction
        .delete_cache(cache_keys
            .keys()
            .map(|key| key.clone())
            .collect());

    Ok((now.elapsed(), (trs_len, keys_len, bytes_len)))
}

