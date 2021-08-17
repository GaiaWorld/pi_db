use std::mem;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::collections::hash_map::Entry as HashMapEntry;

use parking_lot::Mutex;
use futures::{future::{FutureExt, BoxFuture}, stream::{StreamExt, BoxStream}};
use async_stream::stream;
use log::{debug, info, error};

use atom::Atom;
use guid::Guid;
use hash::XHashMap;
use ordmap::{ordmap::{Iter, OrdMap, Keys, Entry}, asbtree::Tree};
use r#async::{lock::{spin_lock::SpinLock,
                     mutex_lock::Mutex as AsyncMutex},
              rt::multi_thread::MultiTaskRuntime};
use async_transaction::{AsyncTransaction,
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

use crate::{Binary,
            KVAction,
            TableTrQos,
            KVActionLog,
            KVDBCommitConfirm,
            KVTableTrError,
            db::{KVDBTransaction, KVDBChildTrList},
            tables::KVTable};
use std::collections::VecDeque;

///
/// 默认的日志文件延迟提交的超时时长，单位ms
///
const DEFAULT_LOG_FILE_COMMIT_DELAY_TIMEOUT: usize = 1000;

///
/// 元信息表
///
#[derive(Clone)]
pub struct MetaTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerMetaTable<C, Log>>);

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for MetaTable<C, Log> {}
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
                   prepare_timeout: u64,
                   commit_timeout: u64) -> Self::Tr {
        MetaTabTr::new(source,
                       is_writable,
                       prepare_timeout,
                       commit_timeout,
                       self.clone())
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> MetaTable<C, Log> {
    /// 构建一个元信息表
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
                let inner = InnerMetaTable {
                    name: name.clone(),
                    root,
                    prepare,
                    rt,
                    waits,
                    waits_size,
                    waits_limit,
                    wait_timeout,
                    log_file,
                };

                let table = MetaTable(Arc::new(inner));

                //加载指定的日志文件的内容到元信息表的内存表
                let now = Instant::now();
                let mut loader = MetaTableLoader::new(table.clone());
                if let Err(e) = table.0.log_file.load(&mut loader,
                                                      Some(path.as_ref().to_path_buf()),
                                                      load_buf_len,
                                                      is_checksum).await {
                    //加载指定的日志文件失败，则立即抛出异常
                    panic!("Load meta table failed, table: {:?}, path: {:?}, reason: {:?}",
                           name.as_str(),
                           path.as_ref(),
                           e);
                }
                info!("Load meta table ok, table: {:?}, path: {:?}, files: {}, keys: {}, bytes: {}, time: {:?}",
                    name.as_str(),
                    path.as_ref(),
                    loader.log_files_len(),
                    loader.keys_len(),
                    loader.bytes_len(),
                    now.elapsed());

                //启动元信息表的提交待确认事务的定时整理
                let table_copy = table.clone();
                let _ = table.0.rt.spawn(table.0.rt.alloc(), async move {
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
                                info!("Collect meta table ok, table: {:?}, time: {:?}, statistics: {:?}, reason: out of time",
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

// 内部元信息表
struct InnerMetaTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    name:           Atom,                                                                                                   //表名
    root:           Mutex<OrdMap<Tree<Binary, Binary>>>,                                                                    //元信息表的根节点
    prepare:        Mutex<XHashMap<Guid, XHashMap<Binary, KVActionLog>>>,                                                   //元信息表的预提交表
    rt:             MultiTaskRuntime<()>,                                                                                   //异步运行时
    waits:          AsyncMutex<VecDeque<(MetaTabTr<C, Log>, XHashMap<Binary, KVActionLog>, <MetaTabTr<C, Log> as Transaction2Pc>::CommitConfirm)>>,                                                                                         //等待异步写日志文件的已提交的元信息事务列表
    waits_size:     AtomicUsize,                                                                                            //等待异步写日志文件的已提交的有序日志事务的键值对大小
    waits_limit:    usize,                                                                                                  //等待异步写日志文件的已提交的元信息事务大小限制
    wait_timeout:   usize,                                                                                                  //等待异步写日志文件的超时时长，单位毫秒
    log_file:       LogFile,                                                                                                //日志文件
}

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for InnerMetaTable<C, Log> {}
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for InnerMetaTable<C, Log> {}

///
/// 元信息表事务
///
#[derive(Clone)]
pub struct MetaTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerMetaTabTr<C, Log>>);

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for MetaTabTr<C, Log> {}
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
            //移除事务在元信息表的预提交表中的操作记录
            let transaction_uid = tr.get_transaction_uid().unwrap();
            let _ = tr.0.table.0.prepare.lock().remove(&transaction_uid);

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
        true
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
                    //同步锁住元信息表的预提交表，并进行预提交表的检查和修改
                    let mut prepare_locked = tr.0.table.0.prepare.lock();

                    //将事务的操作记录与表的预提交表进行比较
                    let mut buf = Vec::new();
                    let mut writed_count = 0;
                    for (_key, action) in tr.0.actions.lock().iter() {
                        match action {
                            KVActionLog::Write(_) => {
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

                    for (key, action) in tr.0.actions.lock().iter() {
                        if let Err(e) = tr
                            .check_prepare_conflict(&mut prepare_locked,
                                                    key,
                                                    action) {
                            //尝试表的预提交失败，则立即返回错误原因
                            return Err(e);
                        }

                        if let Err(e) = tr
                            .check_root_conflict(key) {
                            //尝试表的预提交失败，则立即返回错误原因
                            return Err(e);
                        }

                        //指定关键字的操作预提交成功，则将写操作写入预提交缓冲区
                        match action {
                            KVActionLog::Write(None) => {
                                tr.0.table.append_key_value_to_table_prepare_output(&mut buf, key, None);
                            },
                            KVActionLog::Write(Some(value)) => {
                                tr.0.table.append_key_value_to_table_prepare_output(&mut buf, key, Some(value));
                            },
                            _ => (), //忽略读操作
                        }
                    }
                    write_buf = Some(buf);

                    //获取事务的当前操作记录，并重置事务的当前操作记录
                    let actions = mem::replace(&mut *tr.0.actions.lock(), XHashMap::default());

                    //将事务的当前操作记录，写入表的预提交表
                    prepare_locked.insert(tr.get_transaction_uid().unwrap(), actions);
                }

                Ok(write_buf)
            } else {
                //只读事务，则不需要同步锁住元信息表的预提交表，并立即返回
                Ok(None)
            }
        }.boxed()
    }

    fn commit(&self, confirm: <Self as Transaction2Pc>::CommitConfirm)
              -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        let tr = self.clone();

        async move {
            //移除事务在元信息表的预提交表中的操作记录
            let transaction_uid = tr.get_transaction_uid().unwrap();
            let actions = tr.0.table.0.prepare.lock().remove(&transaction_uid); //获取元信息表，本次事务预提交成功的相关操作记录

            //更新元信息表的根节点
            let actions = actions.unwrap();
            let b = tr.0.table.0.root.lock().ptr_eq(&tr.0.root_ref);
            if !b {
                //元信息表的根节点在当前事务执行过程中已改变
                for (key, action) in actions.iter() {
                    match action {
                        KVActionLog::Write(None) => {
                            //删除指定关键字
                            tr.0.table.0.root.lock().delete(key, false);
                        },
                        KVActionLog::Write(Some(value)) => {
                            //插入或更新指定关键字
                            tr.0.table.0.root.lock().upsert(key.clone(), value.clone(), false);
                        },
                        KVActionLog::Read => (), //忽略读操作
                    }
                }
            } else {
                //元信息表的根节点在当前事务执行过程中未改变，则用本次事务修改并提交成功的根节点替换元信息表的根节点
                *tr.0.table.0.root.lock() = tr.0.root_mut.lock().clone();
            }

            if tr.is_require_persistence() {
                //持久化的元信息表事务，则异步将表的修改写入日志文件后，再确认提交成功
                let table_copy = tr.0.table.clone();
                let _ = self.0.table.0.rt.spawn(self.0.table.0.rt.alloc(), async move {
                    let mut size = 0;
                    for (key, action) in &actions {
                        match action {
                            KVActionLog::Write(Some(value)) => {
                                size += key.len() + value.len();
                            },
                            KVActionLog::Write(None) => {
                                size += key.len();
                            },
                            KVActionLog::Read => (),
                        }
                    }
                    table_copy.0.waits.lock().await.push_back((tr, actions, confirm)); //注册待确认的已提交事务

                    let last_waits_size = table_copy.0.waits_size.fetch_add(size, Ordering::SeqCst); //更新待确认的已提交事务的大小计数
                    if last_waits_size + size >= table_copy.0.waits_limit {
                        //如果当前已注册的待确认的已提交事务大小已达限制，则立即整理
                        table_copy.0.waits_size.store(0, Ordering::SeqCst); //重置待确认的已提交事务的大小计数

                        match collect_waits(&table_copy,
                                            None).await {
                            Err((collect_time, statistics)) => {
                                error!("Collect meta table failed, table: {:?}, time: {:?}, statistics: {:?}, reason: out of size",
                                    table_copy.name().as_str(),
                                    collect_time,
                                    statistics);
                            },
                            Ok((collect_time, statistics)) => {
                                debug!("Collect meta table ok, table: {:?}, time: {:?}, statistics: {:?}, reason: out of size",
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
> MetaTabTr<C, Log> {
    // 构建一个元信息表事务
    #[inline]
    fn new(source: Atom,
           is_writable: bool,
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
            prepare_timeout,
            commit_timeout,
            root_mut: SpinLock::new(root_ref.clone()),
            root_ref,
            table,
            actions: SpinLock::new(XHashMap::default()),
        };

        MetaTabTr(Arc::new(inner))
    }

    // 检查元信息表的预提交表的读写冲突
    fn check_prepare_conflict(&self,
                              prepare: &mut XHashMap<Guid, XHashMap<Binary, KVActionLog>>,
                              key: &Binary,
                              action: &KVActionLog)
                              -> Result<(), KVTableTrError> {
        for (guid, actions) in prepare.iter() {
            match actions.get(key) {
                Some(KVActionLog::Read) => {
                    //元信息表的预提交表中的一个预提交事务与本地预提交事务操作了相同的关键字，且是读操作
                    match action {
                        KVActionLog::Read => {
                            //本地预提交事务对相同的关键字也执行了读操作，则不存在读写冲突，并立即返回检查成功
                            return Ok(());
                        },
                        KVActionLog::Write(_) => {
                            //本地预提交事务对相同的关键字执行了写操作，则存在读写冲突
                            return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal, format!("Prepare meta table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, confilicted_transaction_uid: {:?}, reason: require write key but reading now", self.0.table.name().as_str(), key, self.0.source, self.get_transaction_uid(), self.get_prepare_uid(), guid)));
                        },
                    }
                },
                Some(KVActionLog::Write(_)) => {
                    //元信息表的预提交表中的一个预提交事务与本地预提交事务操作了相同的关键字，且是写操作，则存在读写冲突
                    return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal, format!("Prepare meta table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, confilicted_transaction_uid: {:?}, reason: writing now", self.0.table.name().as_str(), key, self.0.source, self.get_transaction_uid(), self.get_prepare_uid(), guid)));
                },
                None => {
                    //元信息表的预提交表中没有任何预提交事务与本地预提交事务操作了相同的关键字，则不存在读写冲突，并立即返回检查成功
                    return Ok(())
                },
            }
        }

        Ok(())
    }

    // 检查元信息表的根节点冲突
    fn check_root_conflict(&self, key: &Binary)
                           -> Result<(), KVTableTrError> {
        let b = self.0.table.0.root.lock().ptr_eq(&self.0.root_ref);
        if !b {
            //元信息表的根节点在当前事务执行过程中已改变
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
                            return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal, format!("Prepare meta table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: the key is deleted in table while the transaction is running", self.0.table.name().as_str(), key, self.0.source, self.get_transaction_uid(), self.get_prepare_uid())));
                        },
                    }
                },
                Some(root_value) => {
                    //事务的当前操作记录中的关键字，在当前表中已存在
                    match self.0.root_ref.get(&key) {
                        None => {
                            //事务的当前操作记录中的关键字，在事务创建时的表中不存在
                            //表示此关键字是在当前事务内被删除，则此关键字的操作记录允许预提交
                            //并继续其它关键字的操作记录的预提交
                            ()
                        },
                        Some(copy_value) if Binary::binary_equal(&root_value, &copy_value) => {
                            //事务的当前操作记录中的关键字，在事务创建时的表中也存在，且值相同
                            //表示此关键字在当前事务执行过程中未改变，且值也未改变，则此关键字的操作记录允许预提交
                            //并继续其它关键字的操作记录的预提交
                            ()
                        },
                        _ => {
                            //事务的当前操作记录中的关键字，在事务创建时的表中也存在，但值不相同
                            //表示此关键字在当前事务执行过程中未改变，但值已改变，则此关键字的操作记录不允许预提交
                            //并立即返回当前事务预提交冲突
                            return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal, format!("Prepare meta table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: the value is updated in table while the transaction is running", self.0.table.name().as_str(), key, self.0.source, self.get_transaction_uid(), self.get_prepare_uid())));
                        },
                    }
                },
            }
        }

        Ok(())
    }
}

// 内部元信息表事务
struct InnerMetaTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    source:             Atom,                                                                       //事件源
    tid:                SpinLock<Option<Guid>>,                                                     //事务唯一id
    cid:                SpinLock<Option<Guid>>,                                                     //事务提交唯一id
    status:             SpinLock<Transaction2PcStatus>,                                             //事务状态
    writable:           bool,                                                                       //事务是否可写
    prepare_timeout:    u64,                                                                        //事务预提交超时时长，单位毫秒
    commit_timeout:     u64,                                                                        //事务提交超时时长，单位毫秒
    root_mut:           SpinLock<OrdMap<Tree<Binary, Binary>>>,                                     //元信息表的根节点的可写复制
    root_ref:           OrdMap<Tree<Binary, Binary>>,                                               //元信息表的根节点的只读复制
    table:              MetaTable<C, Log>,                                                    //事务对应的元信息表
    actions:            SpinLock<XHashMap<Binary, KVActionLog>>,                                    //事务内操作记录
}

// 元信息表的加载器
struct MetaTableLoader<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    statistics:         XHashMap<PathBuf, (u64, u64)>,  //加载统计信息，包括关键字数量和键值对的字节数
    removed:            XHashMap<Vec<u8>, ()>,          //已删除关键字表
    table:              MetaTable<C, Log>,        //元信息表
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

// 异步整理元信息表中，等待写入日志文件的事务，
// 返回本次整理消耗的时间，本次写入日志文件成功的事务数、关键字数和字节数，以及本次写入日志文件失败的事务数、关键字数和字节数
async fn collect_waits<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(table: &MetaTable<C, Log>,
  timeout: Option<usize>) -> Result<(Duration, (usize, usize, usize)), (Duration, (usize, usize, usize))> {
    //等待指定的时间
    if let Some(timeout) = timeout {
        //需要等待指定时间后，再开始整理
        table.0.rt.wait_timeout(timeout).await;
    }

    //将元信息表中等待写入日志文件的事务，写入日志文件
    let mut waits = VecDeque::new();
    let mut log_uid = 0;
    let mut trs_len = 0;
    let mut keys_len = 0;
    let mut bytes_len = 0;

    let now = Instant::now();
    while let Some((wait_tr, actions, confirm)) = table
        .0
        .waits
        .lock()
        .await
        .pop_front() {

        for (key, actions) in actions.iter() {
            match actions {
                KVActionLog::Write(None) => {
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
                KVActionLog::Write(Some(value)) => {
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
        //写入日志文件失败，则立即中止本次整理
        error!("Collect meta table failed, table: {:?}, transactions: {}, keys: {}, bytes: {}, reason: {:?}",
            table.name().as_str(),
            trs_len,
            keys_len,
            bytes_len,
            e);

        Err((now.elapsed(), (trs_len, keys_len, bytes_len)))
    } else {
        //写入日志文件成功，则调用指定事务的确认提交回调，并继续写入下一个事务
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

        Ok((now.elapsed(), (trs_len, keys_len, bytes_len)))
    }
}
