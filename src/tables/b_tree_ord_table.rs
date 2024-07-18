use std::{mem, thread};
use std::path::{Path, PathBuf};
use std::collections::VecDeque;
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
use async_channel::{Sender, Receiver, bounded};
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
use pi_store::log_store::log_file::LogMethod;

use crate::{Binary, KVAction, KVActionLog, KVDBCommitConfirm, KVTableTrError, TableTrQos, TransactionDebugEvent, transaction_debug_logger,
            db::{KVDBChildTrList, KVDBTransaction},
            tables::{KVTable,
                     log_ord_table::{LogOrderedTable, LogOrdTabTr}}};

// 默认的表名
const DEFAULT_TABLE_FILE_NAME: &str = "table.dat";

// 默认的表名
const DEFAULT_TABLE_NAME: TableDefinition<Binary, Binary> = TableDefinition::new("$default");

// 初始写锁唯一ID
const INIT_LOCK_WRITE_UID: u64 = 1;

// 未锁住写标记
const UNLOCKED_WRITE_FLAG: u64 = 0;

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
                table.len().unwrap_or(0) as usize
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
            //尝试获取写锁
            let write_lock_uid = loop {
                match table.try_lock_write() {
                    None => {
                        //当前写锁还未释放，则稍候重试
                        table.0.rt.timeout(10).await;
                        continue;
                    },
                    Some(lock_uid) => break lock_uid,
                }
            };

            //将所有未持久的事务，强制持久化提交
            let mut locked = self.0.inner.write(); //避免外部产生新的事务
            let mut transaction = match locked.begin_write() {
                Err(e) => {
                    //创建写事务失败，则立即返回错误原因
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
                return Err(KVTableTrError::new_transaction_error(ErrorLevel::Fatal,
                                                                 format!("Compact b-tree ordered table failed, table: {:?}, , reason: {:?}",
                                                                         table.name().as_str(),
                                                                         e)));
            }
            let _ = table.try_unlock_write(write_lock_uid); //持久化提交成功，则释放写锁

            //开始B树表的压缩
            let mut retry_count = 3; //可以重试3次
            for _ in 0..3 {
                let now = Instant::now();
                if let Err(e) = locked.compact() {
                    //压缩数据表失败
                    if retry_count == 0 {
                        //重试已达限制，则立即返回错误原因
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
                                     write_conflict_limit: usize,
                                     write_conflict_timeout: usize,
                                     waits_limit: usize,
                                     wait_timeout: usize) -> Self
    {
        Self::try_new(rt,
                      path,
                      name,
                      cache_size,
                      enable_compact,
                      write_conflict_limit,
                      write_conflict_timeout,
                      waits_limit,
                      wait_timeout)
            .await
            .unwrap()
    }

    /// 尝试构建一个有序B树表，同时只允许构建一个同路径下的有序B树表，如果已经构建则返回空
    pub(crate) async fn try_new<P: AsRef<Path>>(rt: MultiTaskRuntime<()>,
                                                path: P,
                                                name: Atom,
                                                mut cache_size: usize,
                                                enable_compact: bool,
                                                write_conflict_limit: usize,
                                                write_conflict_timeout: usize,
                                                waits_limit: usize,
                                                wait_timeout: usize) -> Option<Self>
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
                    info!("Repairing b-tree ordered table, table: {:?}, cache_size: {:?}, enable_compact: {:?}",
                        name_copy,
                        cache_size,
                        enable_compact);
                }

                let progress = session.progress();
                if progress < 1.0 {
                    //正在修复
                    trace!("Repairing b-tree ordered table, table: {:?}, progress: {:?}",
                        name_copy,
                        progress);
                } else {
                    //修复完成
                    info!("Repair b-tree ordered table succeeded, table: {:?}, cache_size: {:?}, enable_compact: {:?}",
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
                let prepare = Mutex::new(XHashMap::default());
                let write_lock_allocator = AtomicU64::new(INIT_LOCK_WRITE_UID);
                let write_lock = AtomicU64::new(UNLOCKED_WRITE_FLAG);
                let write_conflict_commits = AsyncRwLock::new(VecDeque::new());
                let waits = AsyncMutex::new(VecDeque::new());
                let waits_size = AtomicUsize::new(0);
                let collecting = AtomicBool::new(false);

                let inner = InnerBtreeOrderedTable {
                    name: name.clone(),
                    path: path.clone(),
                    inner,
                    prepare,
                    rt,
                    write_lock_allocator,
                    write_lock,
                    enable_compact: AtomicBool::new(enable_compact),
                    write_conflict_commits,
                    write_conflict_limit,
                    write_conflict_timeout,
                    waits,
                    waits_size,
                    waits_limit,
                    wait_timeout,
                    collecting,
                };
                let table = BtreeOrderedTable(Arc::new(inner));
                info!("Load b-tree ordered table succeeded, table: {:?}, keys: {:?}, cache_size: {:?}, enable_compact: {:?}, time: {:?}",
                    name,
                    table.len(),
                    cache_size,
                    enable_compact,
                    now.elapsed());

                //启动有序B树表的提交写冲突事务的定时整理
                let table_copy = table.clone();
                let _ = table.0.rt.spawn(async move {
                    let table_ref = &table_copy;
                    loop {
                        collect_write_conflict(table_ref,
                                               Some(table_copy.0.write_conflict_timeout))
                            .await;
                    }
                });

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

    /// 创建修复用事务
    pub(crate) fn transaction_repair(&self,
                                     source: Atom,
                                     is_writable: bool,
                                     is_persistent: bool,
                                     prepare_timeout: u64,
                                     commit_timeout: u64) -> <Self as KVTable>::Tr {
        BtreeOrdTabTr::with_repair(source,
                                   is_writable,
                                   is_persistent,
                                   prepare_timeout,
                                   commit_timeout,
                                   self.clone())
    }

    /// 尝试获取当前B树表的唯一写锁，成功返回写锁的唯一ID，失败返回空
    pub(crate) fn try_lock_write(&self) -> Option<u64> {
        if self.0.write_lock.load(Ordering::Acquire) == UNLOCKED_WRITE_FLAG {
            //当前没有写锁
            let write_lock_uid = self
                .0
                .write_lock_allocator
                .fetch_add(1, Ordering::Release);
            if let Ok(UNLOCKED_WRITE_FLAG) = self.0.write_lock.compare_exchange(UNLOCKED_WRITE_FLAG,
                                                                                write_lock_uid,
                                                                                Ordering::AcqRel,
                                                                                Ordering::Acquire) {
                //写锁成功
                Some(write_lock_uid)
            } else {
                //当前已有写锁
                None
            }
        } else {
            //当前已有写锁
            None
        }
    }

    // 安全的尝试解除表的写锁，成功返回真
    pub(crate) fn try_unlock_write(&self, write_lock_uid: u64) -> bool {
        match self.0.write_lock.compare_exchange(write_lock_uid,
                                                 UNLOCKED_WRITE_FLAG,
                                                 Ordering::AcqRel,
                                                 Ordering::Relaxed) {
            Err(UNLOCKED_WRITE_FLAG) => {
                //当前没有写锁，则忽略解锁
                true
            },
            Err(_) => {
                //解锁失败
                false
            },
            Ok(_) => {
                //解锁成功
                true
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
    name:                   Atom,
    //有序B树表的实例的磁盘路径
    path:                   PathBuf,
    //有序B树表的实例
    inner:                  RwLock<Database>,
    //有序B树表的预提交表
    prepare:                Mutex<XHashMap<Guid, XHashMap<Binary, KVActionLog>>>,
    //异步运行时
    rt:                     MultiTaskRuntime<()>,
    //写锁唯一ID分配器
    write_lock_allocator:   AtomicU64,
    //写锁
    write_lock:             AtomicU64,
    //是否允许对有序B树表进行整理压缩
    enable_compact:         AtomicBool,
    //写冲突事务缓冲
    write_conflict_commits:  AsyncRwLock<VecDeque<(XHashMap<Binary, KVActionLog>, Sender<IOResult<XHashMap<Binary, KVActionLog>>>)>>,
    //等待异步提交的写冲突事务缓冲的大小限制
    write_conflict_limit:   usize,
    //等待提交的写冲突事务缓冲的超时时长，单位毫秒
    write_conflict_timeout: usize,
    //等待异步写日志文件的已提交的有序日志事务列表
    waits:                  AsyncMutex<VecDeque<(BtreeOrdTabTr<C, Log>, XHashMap<Binary, KVActionLog>, <BtreeOrdTabTr<C, Log> as Transaction2Pc>::CommitConfirm)>>,
    //等待异步写日志文件的已提交的有序日志事务的键值对大小
    waits_size:     AtomicUsize,
    //等待异步写日志文件的已提交的有序日志事务大小限制
    waits_limit:    usize,
    //等待异步写日志文件的超时时长，单位毫秒
    wait_timeout:   usize,
    //是否正在整理等待异步写日志文件的已提交的有序日志事务列表
    collecting:     AtomicBool,
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
            //移除事务在有序B树表的预提交表中的操作记录
            let mut result = Ok(());
            if let Some(inner_transaction) = self.0.inner_transaction.lock().take() {
                //有内部事务
                if let Err(e) = inner_transaction.rollback() {
                    //内部事务回滚失败
                    result = Err(KVTableTrError::new_transaction_error(ErrorLevel::Fatal,
                                                                       format!("Rollback b-tree ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: {:?}",
                                                                               self.0.table.name().as_str(),
                                                                               self.0.source,
                                                                               self.get_transaction_uid(),
                                                                               self.get_prepare_uid(),
                                                                               e)));
                }

                if let Some(write_lock_uid) = self.0.write_lock {
                    //当前是唯一的可写事务，则安全的强制解除表的写锁
                    let _ = self
                        .0
                        .table
                        .try_unlock_write(write_lock_uid);
                }
            }

            let transaction_uid = tr.get_transaction_uid().unwrap();
            let _ = tr.0.table.0.prepare.lock().remove(&transaction_uid);

            result
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
                if tr.0.inner_transaction.lock().is_none() {
                    //没有内部事务，则立即返回错误原因
                    return Err(KVTableTrError::new_transaction_error(ErrorLevel::Fatal,
                                                                     format!("Prepare b-tree ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: empty inner transaction",
                                                                             self.0.table.name().as_str(),
                                                                             self.0.source,
                                                                             self.get_transaction_uid(),
                                                                             self.get_prepare_uid())));
                }

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
        //提交日志已写成功，则取出内部事务
        let inner_transaction = if let Some(inner_transaction) = self.0.inner_transaction.lock().take() {
            //有内部事务
            inner_transaction
        } else {
            //无内部事务
            let result = Err(KVTableTrError::new_transaction_error(ErrorLevel::Fatal,
                                                                   format!("Commit b-tree ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: require inner transaction",
                                                                           self.0.table.name().as_str(),
                                                                           self.0.source,
                                                                           self.get_transaction_uid(),
                                                                           self.get_prepare_uid())));
            return async move {
                result
            }.boxed();
        };

        let is_write_conflict = inner_transaction.is_write_conflict()
            || inner_transaction.is_repair(); //写冲突事务或修复表事务都是写冲突
        if inner_transaction.is_writable() {
            //非持久化的提交内部写事务
            if let Err(e) = inner_transaction.commit() {
                //内部事务提交失败
                let result = Err(KVTableTrError::new_transaction_error(ErrorLevel::Fatal,
                                                                       format!("Commit b-tree ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: {:?}",
                                                                               self.0.table.name().as_str(),
                                                                               self.0.source,
                                                                               self.get_transaction_uid(),
                                                                               self.get_prepare_uid(),
                                                                               e)));
                return async move {
                    result
                }.boxed();
            }

            if let Some(write_lock_uid) = self.0.write_lock {
                //当前是唯一的可写事务，则安全的强制解除表的写锁
                let _ = self
                    .0
                    .table
                    .try_unlock_write(write_lock_uid);
            }
        } else {
            //关闭内部非写事务
            if let Err(e) = inner_transaction.close() {
                warn!("Commit b-tree ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: {:?}",
                    self.0.table.name().as_str(),
                    self.0.source,
                    self.get_transaction_uid(),
                    self.get_prepare_uid(),
                    e);
            }
        }

        //根据写缓冲区大小确定是否持久化提交
        let tr = self.clone();
        let table_copy = tr.0.table.clone();
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

                //更新有序B树表的根节点
                if let Some(actions) = table_prepare.remove(&transaction_uid) {
                    //有序B树表提交日志写成功后，从有序B树表的预提交表中移除当前事务的操作记录
                    actions
                } else {
                    XHashMap::default()
                }
            };

            let actions = if is_write_conflict {
                //当前事务是写冲突事务，则将当前缓冲事务加入当前表的写冲突事务缓冲区，并异步等待写冲突事务的修改和非持久化提交完成
                let (sender, receiver) = bounded(1);
                let write_conflict_commits_len = {
                    let mut locked = tr
                        .0
                        .table
                        .0
                        .write_conflict_commits
                        .write()
                        .await;
                    locked.push_back((actions, sender));
                    locked.len()
                };

                if write_conflict_commits_len >= tr.0.table.0.write_conflict_limit {
                    //当前表的写冲突事务缓冲区的数量已达限制，则强制异步整理当前表的写冲突事务缓冲区
                    let table = tr.0.table.clone();
                    tr.0.table.0.rt.spawn(async move {
                        collect_write_conflict(&table, None);
                    });
                }

                match receiver.recv().await {
                    Err(e) => {
                        return Err(KVTableTrError::new_transaction_error(ErrorLevel::Fatal,
                                                                         format!("Conflict commit b-tree ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: {:?}",
                                                                                 self.0.table.name().as_str(),
                                                                                 self.0.source,
                                                                                 self.get_transaction_uid(),
                                                                                 self.get_prepare_uid(),
                                                                                 e)));
                    },
                    Ok(Err(e)) => {
                        return Err(KVTableTrError::new_transaction_error(ErrorLevel::Fatal,
                                                                         format!("Conflict commit b-tree ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: {:?}",
                                                                                 self.0.table.name().as_str(),
                                                                                 self.0.source,
                                                                                 self.get_transaction_uid(),
                                                                                 self.get_prepare_uid(),
                                                                                 e)));
                    },
                    Ok(Ok(actions)) => {
                        actions
                    },
                }
            } else {
                //当前事务是非写冲突事务
                actions
            };

            if tr.is_require_persistence() {
                let rt = tr.0.table.0.rt.clone();
                let _ = rt.spawn(async move {
                    //持久化的有序B树表事务，则异步将表的修改写入redb后，再确认提交成功
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
                        //如果当前已注册的待确认的已提交事务大小已达限制，则立即整理，并重置待确认的已提交事务的大小计数
                        table_copy
                            .0
                            .waits_size
                            .store(0, Ordering::Relaxed);

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

            if let Some(inner_transaction) = &*tr.0.inner_transaction.lock() {
                inner_transaction.query(&key)
            } else {
                None
            }
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
            if let Some(inner_transaction) = &mut *tr.0.inner_transaction.lock() {
                if inner_transaction.is_only_read() {
                    //是只读事务，则中止修改操作，并立即返回
                    return Ok(());
                }

                if inner_transaction.is_writable() {
                    //是可写事务，则直接在内部事务中修改指定关键字
                    if let Err(e) = inner_transaction.upsert(key.clone(), value.clone()) {
                        return Err(KVTableTrError::new_transaction_error(ErrorLevel::Normal, e));
                    }
                }

                //可写事务，写冲突事务或修复表事务需要记录修改操作
                let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::Write(Some(value.clone()))); //记录对指定关键字的最新插入或更新操作
                Ok(())
            } else {
                Err(KVTableTrError::new_transaction_error(ErrorLevel::Normal,
                                                          format!("Upsert b-tree ordered table value failed, table: {:?}, key: {:?}, reason: require inner transaction",
                                                                  self.0.table.name().as_str(),
                                                                  key)))
            }
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
            if let Some(inner_transaction) = &mut *tr.0.inner_transaction.lock() {
                if !inner_transaction.is_only_read() {
                    //不是可写内部事务，则中止删除操作，并立即返回
                    return Ok(None);
                }

                let last_value = if inner_transaction.is_writable() {
                    //是可写事务，则直接在内部事务中删除指定关键字
                    match inner_transaction.delete(&key) {
                        Err(e) => {
                            return Err(KVTableTrError::new_transaction_error(ErrorLevel::Normal, e));
                        },
                        Ok(last_value) => last_value,
                    }
                } else {
                    //是写冲突事务，则暂时忽略删除操作
                    None
                };

                //可写事务，写冲突事务或修复表事务需要记录删除操作
                let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::Write(None)); //记录对指定关键字的最新删除操作
                Ok(last_value)
            } else {
                Err(KVTableTrError::new_transaction_error(ErrorLevel::Normal,
                                                          format!("Delete b-tree ordered table value failed, table: {:?}, key: {:?}, reason: require inner transaction",
                                                                  self.0.table.name().as_str(),
                                                                  key)))
            }
        }.boxed()
    }

    fn keys<'a>(&self,
                key: Option<<Self as KVAction>::Key>,
                descending: bool)
        -> BoxStream<'a, <Self as KVAction>::Key>
    {
        let transaction = self.clone();
        let stream = stream! {
            if let Some(inner_transaction) = &*transaction.0.inner_transaction.lock() {
                match inner_transaction {
                    InnerTransaction::OnlyRead(trans, name) => {
                        let table = if let Ok(table) = trans.open_table(DEFAULT_TABLE_NAME) {
                            table
                        } else {
                            return;
                        };

                        if let Some(mut iterator) = inner_transaction
                            .values_by_read(&table, key, descending)
                        {
                            if descending {
                                //倒序
                                while let Some(Ok((key, _value))) = iterator.next_back() {
                                    //从迭代器获取到上一个关键字
                                    yield key.value();
                                }
                            } else {
                                //顺序
                                while let Some(Ok((key, _value))) = iterator.next() {
                                    //从迭代器获取到下一个关键字
                                    yield key.value();
                                }
                            }
                        }
                    },
                    InnerTransaction::WriteConflict(trans, name) => {
                        let table = if let Ok(table) = trans.open_table(DEFAULT_TABLE_NAME) {
                            table
                        } else {
                            return;
                        };

                        if let Some(mut iterator) = inner_transaction
                            .values_by_read(&table, key, descending)
                        {
                            if descending {
                                //倒序
                                while let Some(Ok((key, _value))) = iterator.next_back() {
                                    //从迭代器获取到上一个关键字
                                    yield key.value();
                                }
                            } else {
                                //顺序
                                while let Some(Ok((key, _value))) = iterator.next() {
                                    //从迭代器获取到下一个关键字
                                    yield key.value();
                                }
                            }
                        }
                    },
                    InnerTransaction::Writable(trans, name) => {
                        let table = if let Ok(table) = trans.open_table(DEFAULT_TABLE_NAME) {
                            table
                        } else {
                            return;
                        };

                        if let Some(mut iterator) = inner_transaction
                            .values_by_write(&table, key, descending)
                        {
                            if descending {
                                //倒序
                                while let Some(Ok((key, _value))) = iterator.next_back() {
                                    //从迭代器获取到上一个关键字
                                    yield key.value();
                                }
                            } else {
                                //顺序
                                while let Some(Ok((key, _value))) = iterator.next() {
                                    //从迭代器获取到下一个关键字
                                    yield key.value();
                                }
                            }
                        }
                    },
                    InnerTransaction::Repair(_trans, _name) => {
                        //修复时不允许迭代
                        return;
                    },
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
        let stream = stream! {
            if let Some(inner_transaction) = &*transaction.0.inner_transaction.lock() {
                match inner_transaction {
                    InnerTransaction::OnlyRead(trans, name) => {
                        let table = if let Ok(table) = trans.open_table(DEFAULT_TABLE_NAME) {
                            table
                        } else {
                            return;
                        };

                        if let Some(mut iterator) = inner_transaction
                            .values_by_read(&table, key, descending)
                        {
                            if descending {
                                //倒序
                                while let Some(Ok((key, value))) = iterator.next_back() {
                                    //从迭代器获取到上一个键值对
                                    yield (key.value(), value.value());
                                }
                            } else {
                                //顺序
                                while let Some(Ok((key, value))) = iterator.next() {
                                    //从迭代器获取到下一个键值对
                                    yield (key.value(), value.value());
                                }
                            }
                        }
                    },
                    InnerTransaction::WriteConflict(trans, name) => {
                        let table = if let Ok(table) = trans.open_table(DEFAULT_TABLE_NAME) {
                            table
                        } else {
                            return;
                        };

                        if let Some(mut iterator) = inner_transaction
                            .values_by_read(&table, key, descending)
                        {
                            if descending {
                                //倒序
                                while let Some(Ok((key, value))) = iterator.next_back() {
                                    //从迭代器获取到上一个键值对
                                    yield (key.value(), value.value());
                                }
                            } else {
                                //顺序
                                while let Some(Ok((key, value))) = iterator.next() {
                                    //从迭代器获取到下一个键值对
                                    yield (key.value(), value.value());
                                }
                            }
                        }
                    },
                    InnerTransaction::Writable(trans, name) => {
                        let table = if let Ok(table) = trans.open_table(DEFAULT_TABLE_NAME) {
                            table
                        } else {
                            return;
                        };

                        if let Some(mut iterator) = inner_transaction
                            .values_by_write(&table, key, descending)
                        {
                            if descending {
                                //倒序
                                while let Some(Ok((key, value))) = iterator.next_back() {
                                    //从迭代器获取到上一个键值对
                                    yield (key.value(), value.value());
                                }
                            } else {
                                //顺序
                                while let Some(Ok((key, value))) = iterator.next() {
                                    //从迭代器获取到下一个键值对
                                    yield (key.value(), value.value());
                                }
                            }
                        }
                    },
                    InnerTransaction::Repair(_trans, _name) => {
                        //修复时不允许迭代
                        return;
                    },
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
        let table_name = table.name();
        let mut write_lock = None;
        let inner_transaction = if is_writable {
            //可写事务
            if let Some(lock) = table.try_lock_write() {
                //当前没有正在执行的写事务，则构建一个可写的内部事务
                match table.0.inner.read().begin_write() {
                    Err(e) => {
                        let _ = table.try_unlock_write(lock);
                        panic!("Create b-tree ordered table inner writable transaction failed, table: {:?}, reason: {:?}",
                               table_name.as_str(),
                               e);
                    },
                    Ok(mut transaction) => {
                        write_lock = Some(lock);
                        transaction.set_durability(Durability::None);
                        InnerTransaction::Writable(transaction, table_name)
                    },
                }
            } else {
                //当前有正在执行的写事务，则构建一个只读的写冲突内部事务，后续有任何写操作将直接返回冲突
                match table.0.inner.read().begin_read() {
                    Err(e) => {
                        panic!("Create b-tree ordered table inner write conflict transaction failed, table: {:?}, reason: {:?}",
                               table_name.as_str(),
                               e);
                    },
                    Ok(transaction) => {
                        InnerTransaction::WriteConflict(transaction, table_name)
                    },
                }
            }
        } else {
            //只读事务
            match table.0.inner.read().begin_read() {
                Err(e) => {
                    panic!("Create b-tree ordered table inner only read transaction failed, table: {:?}, reason: {:?}",
                           table_name.as_str(),
                           e);
                },
                Ok(transaction) => {
                    InnerTransaction::OnlyRead(transaction, table_name)
                },
            }
        };

        let inner = InnerBtreeOrdTabTr {
            source,
            tid: SpinLock::new(None),
            cid: SpinLock::new(None),
            status: SpinLock::new(Transaction2PcStatus::default()),
            writable: is_writable,
            persistence: AtomicBool::new(is_persistent),
            prepare_timeout,
            commit_timeout,
            table,
            actions: SpinLock::new(XHashMap::default()),
            write_lock,
            inner_transaction: SpinLock::new(Some(inner_transaction)),
        };

        BtreeOrdTabTr(Arc::new(inner))
    }

    //构建一个用于修复表的有序B树表事务
    fn with_repair(source: Atom,
                   is_writable: bool,
                   is_persistent: bool,
                   prepare_timeout: u64,
                   commit_timeout: u64,
                   table: BtreeOrderedTable<C, Log>) -> Self {
        let table_name = table.name();
        let mut write_lock = None;
        //当前有正在执行的写事务，则构建一个只读的写冲突内部事务，后续有任何写操作将直接返回冲突
        let inner_transaction = match table.0.inner.read().begin_read() {
            Err(e) => {
                panic!("Create b-tree ordered table inner repair transaction failed, table: {:?}, reason: {:?}",
                       table_name.as_str(),
                       e);
            },
            Ok(transaction) => {
                InnerTransaction::Repair(transaction, table_name)
            },
        };

        let inner = InnerBtreeOrdTabTr {
            source,
            tid: SpinLock::new(None),
            cid: SpinLock::new(None),
            status: SpinLock::new(Transaction2PcStatus::default()),
            writable: is_writable,
            persistence: AtomicBool::new(is_persistent),
            prepare_timeout,
            commit_timeout,
            table,
            actions: SpinLock::new(XHashMap::default()),
            write_lock,
            inner_transaction: SpinLock::new(Some(inner_transaction)),
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
                            return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal, format!("Prepare b-tree ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, confilicted_transaction_uid: {:?}, reason: require write key but reading now", self.0.table.name().as_str(), key, self.0.source, self.get_transaction_uid(), self.get_prepare_uid(), guid)));
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
                            return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal, format!("Prepare b-tree ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, confilicted_transaction_uid: {:?}, reason: writing now", self.0.table.name().as_str(), key, self.0.source, self.get_transaction_uid(), self.get_prepare_uid(), guid)));
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

    // 预提交所有修复修改
    // 在表的当前根节点上执行键值对操作中的所有写操作
    // 将有序B树表事务的键值对操作记录移动到对应的有序B树表的预提交表，一般只用于修复有序B树表
    pub(crate) fn prepare_repair(&self, transaction_uid: Guid) {
        //获取事务的当前操作记录，并重置事务的当前操作记录
        let actions = mem::replace(&mut *self.0.actions.lock(), XHashMap::default());

        if let Some(inner_transaction) = &mut *self.0.inner_transaction.lock() {
            if inner_transaction.is_writable() {
                //当前事务是写事务，则在事务对应的表的根节点，执行操作记录中的所有写操作
                for (key, action) in &actions {
                    match action {
                        KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                            //执行插入或更新指定关键字的值的操作
                            inner_transaction.upsert(key.clone(), value.clone());
                        },
                        KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                            //执行删除指定关键字的值的操作
                            inner_transaction.delete(key);
                        },
                        KVActionLog::Read => (), //忽略读操作
                    }
                }
            }
        }

        //将事务的当前操作记录，写入表的预提交表
        self
            .0
            .table
            .0
            .prepare
            .lock()
            .insert(transaction_uid, actions);
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
    //事务对应的有序B树表
    table:              BtreeOrderedTable<C, Log>,
    //事务内操作记录
    actions:            SpinLock<XHashMap<Binary, KVActionLog>>,
    //写锁唯一ID
    write_lock:         Option<u64>,
    //内部事务
    inner_transaction:  SpinLock<Option<InnerTransaction>>,
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Drop for InnerBtreeOrdTabTr<C, Log> {
    fn drop(&mut self) {
        if let Some(write_lock_uid) = self.write_lock.take() {
            //当前是写事务，则解锁
            let _ = self.table.try_unlock_write(write_lock_uid);
        }

        if let Some(inner_transaction) = self.inner_transaction.lock().take() {
            //当前有内部事务，则立即回滚
            let _ = inner_transaction.rollback();
        }
    }
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

async fn collect_write_conflict<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(table: &BtreeOrderedTable<C, Log>, timeout: Option<usize>) {
    if let Some(timeout) = timeout {
        //需要等待指定时间后，再开始整理
        table.0.rt.timeout(timeout).await;
    }

    //已达当前表的写冲突事务缓冲上限，则非持久化提交当前表的写冲突事务缓冲区中的所有写冲突事务
    let write_confilct_commits: Vec<(XHashMap<Binary, KVActionLog>, Sender<IOResult<XHashMap<Binary, KVActionLog>>>)> = table
        .0
        .write_conflict_commits
        .write()
        .await
        .drain(..)
        .collect();
    let (mut actions_vec, mut senders_vec): (Vec<XHashMap<Binary, KVActionLog>>, Vec<Sender<IOResult<XHashMap<Binary, KVActionLog>>>>) = write_confilct_commits.into_iter().unzip();

    let write_lock_uid = loop {
        if let Some(lock) = table.try_lock_write() {
            //尝试获取锁成功
            break lock;
        } else {
            //尝试获取锁失败，则稍候重试
            table
                .0
                .rt
                .timeout(0)
                .await;
            continue;
        }
    };

    //立即开始内部写事务
    let r = table.0.inner.read().begin_write();
    if r.is_err() {
        if let Err(e) = r {
            //创建写事务失败，则通知所有异步等待非持久化提交成功的写冲突事务
            let _ = table.try_unlock_write(write_lock_uid);

            for sender in senders_vec {
                sender
                    .send(Err(Error::new(ErrorKind::Other,
                                         format!("Conflict commit b-tree ordered table failed, table: {:?}, reason: {:?}",
                                                 table.name().as_str(),
                                                 e))))
                    .await;
            }

            return;
        }
    }
    let mut transaction = r.unwrap();
    transaction.set_durability(Durability::None);
    let mut new_inner_transaction = InnerTransaction::Writable(transaction, table.name());

    //立即在新的内部写事务上执行写冲突事务的修改或删除操作
    for actions in &actions_vec {
        for (key, action) in actions.iter() {
            match action {
                KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                    //删除指定关键字
                    if let Err(e) = new_inner_transaction.delete(key) {
                        //删除失败，则通知所有异步等待非持久化提交成功的写冲突事务
                        for sender in senders_vec {
                            sender
                                .send(Err(Error::new(ErrorKind::Other,
                                                     format!("Conflict delete b-tree ordered table failed, table: {:?}, reason: {:?}",
                                                             table.name().as_str(),
                                                             e))))
                                .await;
                        }

                        return;
                    }
                },
                KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                    //插入或更新指定关键字
                    if let Err(e) = new_inner_transaction.upsert(key.clone(), value.clone()) {
                        //修改失败，则通知所有异步等待非持久化提交成功的写冲突事务
                        for sender in senders_vec {
                            sender
                                .send(Err(Error::new(ErrorKind::Other,
                                                     format!("Conflict upsert b-tree ordered table failed, table: {:?}, reason: {:?}",
                                                             table.name().as_str(),
                                                             e))))
                                .await;
                        }

                        return;
                    }
                },
                KVActionLog::Read => (), //忽略读操作
            }
        }
    }

    //非持久化提交新的内部写事务
    if let Err(e) = new_inner_transaction.commit() {
        //内部事务提交失败，则通知其它异步等待非持久化提交成功的写冲突事务
        for sender in senders_vec {
            sender
                .send(Err(Error::new(ErrorKind::Other,
                                     format!("Conflict commit b-tree ordered table failed, table: {:?}, reason: {:?}",
                                             table.name().as_str(),
                                             e))))
                .await;
        }

        return;
    }

    //安全的强制解除表的写锁
    let _ = table
        .try_unlock_write(write_lock_uid);

    //通知所有异步等待非持久化提交成功的写冲突事务
    for (sender,
        actions) in senders_vec.into_iter().zip(actions_vec.into_iter())
    {
        sender
            .send(Ok(actions))
            .await;
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

        while let Some((wait_tr, actions, confirm)) = locked.pop_front() {
            for (key, actions) in actions.iter() {
                match actions {
                    KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                        //统计删除了有序B树表中指定关键字的值
                        keys_len += 1;
                        bytes_len += key.len();
                    },
                    KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                        //统计插入或更新了有序B树表中指定关键字的值
                        keys_len += 1;
                        bytes_len += key.len() + value.len();
                    },
                    KVActionLog::Read => (), //忽略读操作
                }
            }

            trs_len += 1;
            waits.push_back((wait_tr, confirm));
        }

        loop {
            if let Some(lock) = table.try_lock_write() {
                //当前B树表未写锁
                match table.0.inner.read().begin_write() {
                    Err(e) => {
                        //写入redb失败，则稍候重试
                        let _ = table.try_unlock_write(lock); //立即释放当前B树表的写锁
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
                    },
                    Ok(mut transaction) => {
                        transaction.set_durability(Durability::Immediate);
                        if let Err(e) = transaction.commit() {
                            //写入redb失败，则立即中止本次整理
                            let _ = table.try_unlock_write(lock); //立即释放当前B树表的写锁
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

                        //持久化提交成功，则立即释放当前B树表的写锁
                        let _ = table.try_unlock_write(lock);
                        break;
                    },
                }
            }

            //当前B树表已写锁或获取写事务失败，则稍候重试
            table.0.rt.timeout(1000).await;
        }
    }

    //写入redb成功，则调用指定事务的确认提交回调，并继续写入下一个事务
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
    table.0.collecting.store(false, Ordering::Release); //设置为已整理结束

    Ok((now.elapsed(), (trs_len, keys_len, bytes_len)))
}


