use std::mem;
use std::sync::Arc;
use std::path::Path;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use futures::{future::{FutureExt, BoxFuture}, stream::{StreamExt, BoxStream}};
use async_stream::stream;

use pi_atom::Atom;
use pi_guid::Guid;
use pi_hash::XHashMap;
use pi_ordmap::{ordmap::{Iter, OrdMap, Keys, Entry}, asbtree::Tree};
use pi_async::lock::spin_lock::SpinLock;
use pi_async_transaction::{AsyncTransaction,
                           Transaction2Pc,
                           UnitTransaction,
                           SequenceTransaction,
                           TransactionTree,
                           TransactionError,
                           AsyncCommitLog,
                           ErrorLevel,
                           manager_2pc::Transaction2PcStatus};

use crate::{Binary,
            KVAction,
            TableTrQos,
            KVActionLog,
            KVDBCommitConfirm,
            KVTableTrError,
            db::{KVDBTransaction, KVDBChildTrList},
            tables::KVTable};

///
/// 有序的内存数据表
///
#[derive(Clone)]
pub struct MemoryOrderedTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerMemoryOrderedTable<C, Log>>);

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for MemoryOrderedTable<C, Log> {}
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for MemoryOrderedTable<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVTable for MemoryOrderedTable<C, Log> {
    type Name = Atom;
    type Tr = MemOrdTabTr<C, Log>;
    type Error = KVTableTrError;

    fn name(&self) -> <Self as KVTable>::Name {
        self.0.name.clone()
    }

    fn path(&self) -> Option<&Path> {
        None
    }

    fn is_persistent(&self) -> bool {
        self.0.persistence
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
        MemOrdTabTr::new(source,
                         is_writable,
                         is_persistent,
                         prepare_timeout,
                         commit_timeout,
                         self.clone())
    }

    fn ready_collect(&self) -> BoxFuture<Result<(), Self::Error>> {
        async move {
            //有序内存表，忽略准备整理
            Ok(())
        }.boxed()
    }

    fn collect(&self) -> BoxFuture<Result<(), Self::Error>> {
        async move {
            //有序内存表，忽略整理
            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> MemoryOrderedTable<C, Log> {
    /// 构建一个有序内存表
    pub fn new(name: Atom,
               is_persistence: bool) -> Self {
        let root = Mutex::new(OrdMap::new(None));
        let prepare = Mutex::new(XHashMap::default());

        let inner = InnerMemoryOrderedTable {
            name,
            persistence: is_persistence,
            root,
            prepare,
            marker: PhantomData,
        };

        MemoryOrderedTable(Arc::new(inner))
    }
}

// 内部有序内存数据表
struct InnerMemoryOrderedTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    name:           Atom,                                                   //表名
    persistence:    bool,                                                   //是否持久化
    root:           Mutex<OrdMap<Tree<Binary, Binary>>>,                    //有序内存表的根节点
    prepare:        Mutex<XHashMap<Guid, XHashMap<Binary, KVActionLog>>>,   //有序内存表的预提交表
    marker:         PhantomData<(C, Log)>,
}

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for InnerMemoryOrderedTable<C, Log> {}
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for InnerMemoryOrderedTable<C, Log> {}

///
/// 有序内存表事务
///
#[derive(Clone)]
pub struct MemOrdTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerMemOrdTabTr<C, Log>>);

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for MemOrdTabTr<C, Log> {}
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for MemOrdTabTr<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> AsyncTransaction for MemOrdTabTr<C, Log> {
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
            //移除事务在有序内存表的预提交表中的操作记录
            let transaction_uid = tr.get_transaction_uid().unwrap();
            let _ = tr.0.table.0.prepare.lock().remove(&transaction_uid);

            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Transaction2Pc for MemOrdTabTr<C, Log> {
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
                let mut write_buf = None; //默认的写操作缓冲区

                {
                    //同步锁住有序内存表的预提交表，并进行预提交表的检查和修改
                    let mut prepare_locked = tr.0.table.0.prepare.lock();

                    //将事务的操作记录与表的预提交表进行比较
                    if tr.is_require_persistence() {
                        //需要持久化的可写事务预提交
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

                        let init_buf_len = buf.len(); //获取初始化本次表事务的预提交输出缓冲区后，缓冲区的长度
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

                        if buf.len() <= init_buf_len {
                            //本次事务没有对本地表的写操作，则设置写操作缓冲区为空
                            write_buf = None;
                        } else {
                            //本次事务有对本地表的写操作，则写操作缓冲区为指定的预提交缓冲区
                            write_buf = Some(buf);
                        }
                    } else {
                        //不需要持久化的可写事务预提交
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
                        }
                    }

                    //获取事务的当前操作记录，并重置事务的当前操作记录
                    let actions = mem::replace(&mut *tr.0.actions.lock(), XHashMap::default());

                    //将事务的当前操作记录，写入表的预提交表
                    prepare_locked.insert(tr.get_transaction_uid().unwrap(), actions);
                }

                Ok(write_buf)
            } else {
                //只读事务，则不需要同步锁住有序内存表的预提交表，并立即返回
                Ok(None)
            }
        }.boxed()
    }

    fn commit(&self, confirm: <Self as Transaction2Pc>::CommitConfirm)
              -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        let tr = self.clone();

        async move {
            //移除事务在有序内存表的预提交表中的操作记录
            let transaction_uid = tr.get_transaction_uid().unwrap();

            {
                let mut table_prepare = tr
                    .0
                    .table
                    .0
                    .prepare
                    .lock();
                let actions = table_prepare.get(&transaction_uid); //获取有序内存表，本次事务预提交成功的相关操作记录

                //更新有序内存表的根节点
                if let Some(actions) = actions {
                    let b = tr.0.table.0.root.lock().ptr_eq(&tr.0.root_ref);
                    if !b {
                        //有序内存表的根节点在当前事务执行过程中已改变
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
                        //有序内存表的根节点在当前事务执行过程中未改变，则用本次事务修改并提交成功的根节点替换有序内存表的根节点
                        *tr.0.table.0.root.lock() = tr.0.root_mut.lock().clone();
                    }

                    //有序内存表提交完成后，从有序内存表的预提交表中移除当前事务的操作记录
                    let _ = table_prepare.remove(&transaction_uid);
                }
            }

            if tr.is_require_persistence() {
                //持久化的有序内存表事务，则立即确认提交成功
                let commit_uid = tr.get_commit_uid().unwrap();
                if let Err(e) = confirm(transaction_uid.clone(), commit_uid, Ok(())) {
                    return Err(e);
                }
            }

            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> UnitTransaction for MemOrdTabTr<C, Log> {
    type Status = Transaction2PcStatus;
    type Qos = TableTrQos;

    //有序内存表事务，一定是单元事务
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
> SequenceTransaction for MemOrdTabTr<C, Log> {
    type Item = Self;

    //有序内存表事务，一定不是顺序事务
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
> TransactionTree for MemOrdTabTr<C, Log> {
    type Node = KVDBTransaction<C, Log>;
    type NodeInterator = KVDBChildTrList<C, Log>;

    //有序内存表事务，一定不是事务树
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
> KVAction for MemOrdTabTr<C, Log> {
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
> MemOrdTabTr<C, Log> {
    // 构建一个有序内存表事务
    #[inline]
    fn new(source: Atom,
           is_writable: bool,
           is_persistence: bool,
           prepare_timeout: u64,
           commit_timeout: u64,
           table: MemoryOrderedTable<C, Log>) -> Self {
        let root_ref = table.0.root.lock().clone();

        let inner = InnerMemOrdTabTr {
            source,
            tid: SpinLock::new(None),
            cid: SpinLock::new(None),
            status: SpinLock::new(Transaction2PcStatus::default()),
            writable: is_writable,
            persistence: AtomicBool::new(is_persistence),
            prepare_timeout,
            commit_timeout,
            root_mut: SpinLock::new(root_ref.clone()),
            root_ref,
            table,
            actions: SpinLock::new(XHashMap::default()),
        };

        MemOrdTabTr(Arc::new(inner))
    }

    // 检查有序内存表的预提交表的读写冲突
    fn check_prepare_conflict(&self,
                              prepare: &mut XHashMap<Guid, XHashMap<Binary, KVActionLog>>,
                              key: &Binary,
                              action: &KVActionLog)
                              -> Result<(), KVTableTrError> {
        for (guid, actions) in prepare.iter() {
            match actions.get(key) {
                Some(KVActionLog::Read) => {
                    //有序内存表的预提交表中的一个预提交事务与本地预提交事务操作了相同的关键字，且是读操作
                    match action {
                        KVActionLog::Read => {
                            //本地预提交事务对相同的关键字也执行了读操作，则不存在读写冲突，并继续检查预提交表中是否存在读写冲突
                            continue;
                        },
                        KVActionLog::Write(_) => {
                            //本地预提交事务对相同的关键字执行了写操作，则存在读写冲突
                            return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal, format!("Prepare memory ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, confilicted_transaction_uid: {:?}, reason: require write key but reading now", self.0.table.name().as_str(), key, self.0.source, self.get_transaction_uid(), self.get_prepare_uid(), guid)));
                        },
                    }
                },
                Some(KVActionLog::Write(_)) => {
                    //有序内存表的预提交表中的一个预提交事务与本地预提交事务操作了相同的关键字，且是写操作，则存在读写冲突
                    return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal, format!("Prepare memory ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, confilicted_transaction_uid: {:?}, reason: writing now", self.0.table.name().as_str(), key, self.0.source, self.get_transaction_uid(), self.get_prepare_uid(), guid)));
                },
                None => {
                    //有序内存表的预提交表中没有任何预提交事务与本地预提交事务操作了相同的关键字，则不存在读写冲突，并继续检查预提交表中是否存在读写冲突
                    continue;
                },
            }
        }

        Ok(())
    }

    // 检查有序内存表的根节点冲突
    fn check_root_conflict(&self, key: &Binary)
                           -> Result<(), KVTableTrError> {
        let b = self.0.table.0.root.lock().ptr_eq(&self.0.root_ref);
        if !b {
            //有序内存表的根节点在当前事务执行过程中已改变
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
                            return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal, format!("Prepare memory ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: the key is deleted in table while the transaction is running", self.0.table.name().as_str(), key, self.0.source, self.get_transaction_uid(), self.get_prepare_uid())));
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
                            return Err(<Self as Transaction2Pc>::PrepareError::new_transaction_error(ErrorLevel::Normal, format!("Prepare memory ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, prepare_uid: {:?}, reason: the value is updated in table while the transaction is running", self.0.table.name().as_str(), key, self.0.source, self.get_transaction_uid(), self.get_prepare_uid())));
                        },
                    }
                },
            }
        }

        Ok(())
    }

    // 预提交所有修复修改
    // 在表的当前根节点上执行键值对操作中的所有写操作
    // 将有序内存表事务的键值对操作记录移动到对应的有序内存表的预提交表，一般只用于修复有序内存表
    pub(crate) fn prepare_repair(&self, transaction_uid: Guid) {
        //获取事务的当前操作记录，并重置事务的当前操作记录
        let actions = mem::replace(&mut *self.0.actions.lock(), XHashMap::default());

        //在事务对应的表的根节点，执行操作记录中的所有写操作
        for (key, action) in &actions {
            match action {
                KVActionLog::Write(Some(value)) => {
                    //执行插入或更新指定关键字的值的操作
                    self
                        .0
                        .table
                        .0
                        .root
                        .lock()
                        .upsert(key.clone(), value.clone(), false);
                },
                KVActionLog::Write(None) => {
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

// 内部有序内存表事务
struct InnerMemOrdTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    source:             Atom,                                       //事件源
    tid:                SpinLock<Option<Guid>>,                     //事务唯一id
    cid:                SpinLock<Option<Guid>>,                     //事务提交唯一id
    status:             SpinLock<Transaction2PcStatus>,             //事务状态
    writable:           bool,                                       //事务是否可写
    persistence:        AtomicBool,                                 //事务是否持久化
    prepare_timeout:    u64,                                        //事务预提交超时时长，单位毫秒
    commit_timeout:     u64,                                        //事务提交超时时长，单位毫秒
    root_mut:           SpinLock<OrdMap<Tree<Binary, Binary>>>,     //有序内存表的根节点的可写复制
    root_ref:           OrdMap<Tree<Binary, Binary>>,               //有序内存表的根节点的只读复制
    table:              MemoryOrderedTable<C, Log>,                 //事务对应的有序内存表
    actions:            SpinLock<XHashMap<Binary, KVActionLog>>,    //事务内操作记录
}