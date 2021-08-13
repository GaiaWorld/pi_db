use std::path::{Path, PathBuf};
use std::collections::VecDeque;
use std::io::{Error, Result as IOResult, ErrorKind};
use std::sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}};

use futures::{future::{FutureExt, BoxFuture},
              stream::{StreamExt, BoxStream}};
use bytes::BufMut;

use atom::Atom;
use guid::{GuidGen, Guid};
use r#async::{lock::{spin_lock::SpinLock,
                     rw_lock::RwLock},
              rt::multi_thread::MultiTaskRuntime};
use async_transaction::{AsyncTransaction,
                        Transaction2Pc,
                        UnitTransaction,
                        SequenceTransaction,
                        TransactionTree,
                        AsyncCommitLog,
                        ErrorLevel,
                        manager_2pc::{Transaction2PcStatus, Transaction2PcManager}};
use async_file::file::create_dir;
use hash::XHashMap;

use crate::{Binary,
            KVAction,
            KVDBTableType,
            KVTableMeta,
            TableTrQos,
            KVDBCommitConfirm,
            KVTableTrError,
            tables::{KVTable,
                     TableKV,
                     mem_ord_table::{MemoryOrderedTable,
                                     MemOrdTabTr},
                     log_ord_table::{LogOrderedTable,
                                     LogOrdTabTr}}};

///
/// 默认的数据库表元信息目录名
///
const DEFAULT_DB_TABLES_META_DIR: &str = ".tables_meta";

///
/// 默认的数据库表所在目录名
///
const DEFAULT_DB_TABLES_DIR: &str = ".tables";

///
/// 键值对数据库管理器构建器
///
pub struct KVDBManagerBuilder<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    rt:                 MultiTaskRuntime<()>,               //异步运行时
    tr_mgr:             Transaction2PcManager<C, Log>,      //事务管理器
    db_path:            PathBuf,                            //数据库的表文件所在目录
    tables_meta_path:   PathBuf,                            //数据库的元信息表文件所在目录
    tables_path:        PathBuf,                            //数据库表文件所在目录
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVDBManagerBuilder<C, Log> {
    /// 构建键值对数据库管理器构建器
    pub fn new<P: AsRef<Path>>(rt: MultiTaskRuntime<()>,
                               tr_mgr: Transaction2PcManager<C, Log>,
                               path: P) -> Self {
        let db_path = path.as_ref().to_path_buf();
        let tables_meta_path = db_path.join(DEFAULT_DB_TABLES_META_DIR);
        let tables_path = db_path.join(DEFAULT_DB_TABLES_DIR);

        KVDBManagerBuilder {
            rt,
            tr_mgr,
            db_path,
            tables_meta_path,
            tables_path,
        }
    }

    /// 异步启动键值对数据库，并返回键值对数据库的管理器
    pub async fn startup(self) -> IOResult<KVDBManager<C, Log>> {
        if !self.tables_meta_path.exists() {
            //指定路径的元信息表目录不存在，则创建
            let _ = create_dir(self.rt.clone(), self.tables_meta_path.clone()).await?;
        }

        if !self.tables_path.exists() {
            //指定路径的表目录不存在，则创建
            let _ = create_dir(self.rt.clone(), self.tables_path.clone()).await?;
        }

        let rt = self.rt;
        let tr_mgr = self.tr_mgr;
        let db_path = self.db_path;
        let tables_meta_path = self.tables_meta_path;
        let tables_path = self.tables_path;
        let tables = Arc::new(RwLock::new(XHashMap::default()));
        let inner = InnerKVDBManager {
            rt,
            tr_mgr,
            db_path,
            tables_meta_path,
            tables_path,
            tables,
        };

        Ok(KVDBManager(Arc::new(inner)))
    }
}

///
/// 键值对数据库管理器
///
#[derive(Clone)]
pub struct KVDBManager<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerKVDBManager<C, Log>>);

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for KVDBManager<C, Log> {}
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for KVDBManager<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVDBManager<C, Log> {

}

/*
* 键值对数据库管理器同步方法
*/
impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>
> KVDBManager<C, Log> {
    /// 获取键值对数据库所在目录的路径
    pub fn db_path(&self) -> &Path {
        &self.0.db_path
    }

    /// 获取键值对数据库的元信息表所在目录的路径
    pub fn tables_meta_path(&self) -> &Path {
        &self.0.tables_meta_path
    }

    /// 获取键值对数据库的表所在目录的路径
    pub fn tables_path(&self) -> &Path {
        &self.0.tables_path
    }

    /// 创建一个键值对数据库的根事务
    /// 根事务是否需要持久化，根据根事务的所有子事务中，是否有执行了写操作且需要持久化的子事务确定，如果有这种子事务存在，则根事务也需要持久化
    pub fn transaction(&self,
                       source: Atom,
                       is_writable: bool,
                       prepare_timeout: u64,
                       commit_timeout: u64) -> KVDBTransaction<C, Log> {
        let tid = SpinLock::new(None);
        let cid = SpinLock::new(None);
        let status = SpinLock::new(Transaction2PcStatus::Start);
        let childs_map = SpinLock::new(XHashMap::default());
        let childs = SpinLock::new(KVDBChildTrList::new());
        let db_mgr = self.clone();

        let inner = InnerRootTransaction {
            source,
            tid,
            cid,
            status,
            writable: is_writable,
            persistence: AtomicBool::new(false), //默认键值对数据库的根事务不持久化
            prepare_timeout,
            commit_timeout,
            childs_map,
            childs,
            db_mgr,
        };

        KVDBTransaction::RootTr(RootTransaction(Arc::new(inner)))
    }
}

/*
* 键值对数据库管理器异步方法
*/
impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>
> KVDBManager<C, Log> {
    /// 异步判断指定名称的表是否存在
    pub async fn is_exist(&self, table_name: &Atom) -> bool {
        self.0.tables.read().await.contains_key(&table_name)
    }

    /// 异步获取键值对数据库的表数量
    pub async fn table_size(&self) -> usize {
        self.0.tables.read().await.len()
    }

    /// 异步获取指定名称的数据表所在目录的路径，返回空表示指定名称的表不存在
    pub async fn table_path(&self, table_name: &Atom) -> Option<PathBuf> {
        match self.0.tables.read().await.get(&table_name) {
            None => None,
            Some(KVDBTable::MemOrdTab(table)) => {
                if let Some(path) = table.path() {
                    Some(path.to_path_buf())
                } else {
                    None
                }
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                if let Some(path) = table.path() {
                    Some(path.to_path_buf())
                } else {
                    None
                }
            },
        }
    }

    /// 异步判断指定名称的数据表是否可持久化，返回空表示指定名称的表不存在
    pub async fn is_persistent_table(&self, table_name: &Atom) -> Option<bool> {
        match self.0.tables.read().await.get(&table_name) {
            None => None,
            Some(KVDBTable::MemOrdTab(table)) => {
                Some(table.is_persistent())
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                Some(table.is_persistent())
            },
        }
    }

    /// 异步判断指定名称的数据表是否有序，返回空表示指定名称的表不存在
    pub async fn is_ordered_table(&self, table_name: &Atom) -> Option<bool> {
        match self.0.tables.read().await.get(&table_name) {
            None => None,
            Some(KVDBTable::MemOrdTab(table)) => {
                Some(table.is_ordered())
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                Some(table.is_ordered())
            },
        }
    }

    /// 异步获取指定名称的数据表的记录数，返回空表示指定名称的表不存在
    pub async fn table_record_size(&self, table_name: &Atom) -> Option<usize> {
        match self.0.tables.read().await.get(&table_name) {
            None => None,
            Some(KVDBTable::MemOrdTab(table)) => {
                Some(table.len())
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                Some(table.len())
            },
        }
    }

    /// 异步创建表，需要指定表类型、表名、是否持久化和表的元信息
    pub async fn create_table(&self,
                              table_type: KVDBTableType,
                              name: Atom,
                              is_persistence: bool,
                              meta: KVTableMeta) -> IOResult<()> {
        let mut tables = self.0.tables.write().await;

        if tables.contains_key(&name) {
            //指定名称的表已存在，则立即返回错误原因
            return Err(Error::new(ErrorKind::AlreadyExists,
                                  format!("Create table failed, type: {:?}, name: {:?}, is_persistence: {:?}, meta: {:?}, reason: name conflict", table_type, name, is_persistence, meta)));
        }

        match table_type {
            KVDBTableType::MemOrdTab => {
                //创建一个有序内存表
                let table = MemoryOrderedTable::new(self.0.rt.clone(),
                                                    name.clone(),
                                                    is_persistence);

                //TODO 设置元信息...

                //注册创建的有序内存表
                tables.insert(name, KVDBTable::MemOrdTab(table));
            },
            KVDBTableType::LogOrdTab => {
                //创建一个有序日志表
                let table_path = self.0.tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                let table =
                    LogOrderedTable::new(self.0.rt.clone(),
                                         table_path,
                                         name.clone(),
                                         512 * 1024 * 1024,
                                         512 * 1024,
                                         None,
                                         512 * 1024 * 1024,
                                         true,
                                         512,
                                         60 * 1000).await;

                //TODO 设置元信息...

                //注册创建的有序内存表
                tables.insert(name, KVDBTable::LogOrdTab(table));
            },
        }

        Ok(())
    }
}

// 内部键值对数据库管理器
struct InnerKVDBManager<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    rt:                 MultiTaskRuntime<()>,                           //异步运行时
    tr_mgr:             Transaction2PcManager<C, Log>,                  //事务管理器
    db_path:            PathBuf,                                        //数据库的表文件所在目录的路径
    tables_meta_path:   PathBuf,                                        //数据库的元信息表文件所在目录的路径
    tables_path:        PathBuf,                                        //数据库表文件所在目录的路径
    tables:             Arc<RwLock<XHashMap<Atom, KVDBTable<C, Log>>>>, //数据表
}

///
/// 键值对数据库事务
///
#[derive(Clone)]
pub enum KVDBTransaction<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    RootTr(RootTransaction<C, Log>),    //键值对数据库的根事务
    MemOrdTabTr(MemOrdTabTr<C, Log>),   //有序内存表事务
    LogOrdTabTr(LogOrdTabTr<C, Log>),   //有序日志表事务
}

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for KVDBTransaction<C, Log> {}
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for KVDBTransaction<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> AsyncTransaction for KVDBTransaction<C, Log> {
    type Output = ();
    type Error = KVTableTrError;

    fn is_writable(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_writable()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_writable()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_writable()
            },
        }
    }

    // 键值对数据库的提交，会把所有子事务的预提交输出合成为一个提交输入，用于写入提交日志，所以也不需要并发
    fn is_concurrent_commit(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_concurrent_commit()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_concurrent_commit()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_concurrent_commit()
            },
        }
    }

    // 键值对数据库的预提交基本都是内存操作，所以回滚也不需要并发
    fn is_concurrent_rollback(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_concurrent_rollback()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_concurrent_rollback()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_concurrent_rollback()
            },
        }
    }

    fn get_source(&self) -> Atom {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_source()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_source()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_source()
            },
        }
    }

    fn init(&self)
            -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.init()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.init()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.init()
            },
        }
    }

    fn rollback(&self)
                -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.rollback()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.rollback()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.rollback()
            },
        }
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Transaction2Pc for KVDBTransaction<C, Log> {
    type Tid = Guid;
    type Pid = Guid;
    type Cid = Guid;
    type PrepareOutput = Vec<u8>;
    type PrepareError = KVTableTrError;
    type ConfirmOutput = ();
    type ConfirmError = KVTableTrError;
    type CommitConfirm = KVDBCommitConfirm<C, Log>;

    fn is_require_persistence(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_require_persistence()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_require_persistence()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_require_persistence()
            },
        }
    }

    fn is_concurrent_prepare(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_concurrent_prepare()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_concurrent_prepare()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_concurrent_prepare()
            },
        }
    }

    fn is_enable_inherit_uid(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_enable_inherit_uid()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_enable_inherit_uid()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_enable_inherit_uid()
            },
        }
    }

    fn get_transaction_uid(&self) -> Option<<Self as Transaction2Pc>::Tid> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_transaction_uid()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_transaction_uid()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_transaction_uid()
            },
        }
    }

    fn set_transaction_uid(&self, uid: <Self as Transaction2Pc>::Tid) {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.set_transaction_uid(uid);
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.set_transaction_uid(uid);
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.set_transaction_uid(uid);
            },
        }
    }

    fn get_prepare_uid(&self) -> Option<<Self as Transaction2Pc>::Pid> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_prepare_uid()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_prepare_uid()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_prepare_uid()
            },
        }
    }

    fn set_prepare_uid(&self, uid: <Self as Transaction2Pc>::Pid) {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.set_prepare_uid(uid);
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.set_prepare_uid(uid);
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.set_prepare_uid(uid);
            },
        }
    }

    fn get_commit_uid(&self) -> Option<<Self as Transaction2Pc>::Cid> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_commit_uid()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_commit_uid()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_commit_uid()
            },
        }
    }

    fn set_commit_uid(&self, uid: <Self as Transaction2Pc>::Cid) {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.set_commit_uid(uid);
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.set_commit_uid(uid);
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.set_commit_uid(uid);
            },
        }
    }

    fn get_prepare_timeout(&self) -> u64 {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_prepare_timeout()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_prepare_timeout()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_prepare_timeout()
            },
        }
    }

    fn get_commit_timeout(&self) -> u64 {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_commit_timeout()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_commit_timeout()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_commit_timeout()
            },
        }
    }

    fn prepare(&self)
               -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.prepare()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.prepare()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.prepare()
            },
        }
    }

    fn commit(&self, confirm: <Self as Transaction2Pc>::CommitConfirm)
              -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.commit(confirm)
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.commit(confirm)
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.commit(confirm)
            },
        }
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> UnitTransaction for KVDBTransaction<C, Log> {
    type Status = Transaction2PcStatus;
    type Qos = TableTrQos;

    fn is_unit(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_unit()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_unit()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_unit()
            },
        }
    }

    fn get_status(&self) -> <Self as UnitTransaction>::Status {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_status()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_status()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_status()
            },
        }
    }

    fn set_status(&self, status: <Self as UnitTransaction>::Status) {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.set_status(status);
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.set_status(status);
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.set_status(status);
            },
        }
    }

    fn qos(&self) -> <Self as UnitTransaction>::Qos {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.qos()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.qos()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.qos()
            },
        }
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> SequenceTransaction for KVDBTransaction<C, Log> {
    type Item = Self;

    // 键值对数据表事务，一定不是顺序事务
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
> TransactionTree for KVDBTransaction<C, Log> {
    type Node = KVDBTransaction<C, Log>; //键值对数据库的根事务的子事务，必须是键值对数据库事务
    type NodeInterator = KVDBChildTrList<C, Log>;

    fn is_tree(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_tree()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_tree()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_tree()
            },
        }
    }

    fn children_len(&self) -> usize {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.children_len()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.children_len()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.children_len()
            },
        }
    }

    fn to_children(&self) -> Self::NodeInterator {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.to_children()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.to_children()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.to_children()
            },
        }
    }
}

/*
* 键值对数据库事务异步方法
*/
impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVDBTransaction<C, Log> {
    /// 在键值对数据库事务的根事务内，异步查询多个表和键的值的结果集
    pub async fn query(&self,
                       table_kv_list: Vec<TableKV>) -> Vec<Option<Binary>> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.query(table_kv_list).await
            },
            _ => panic!("Query db failed, reason: invalid root transaction"),
        }
    }

    /// 在键值对数据库事务的根事务内，异步插入或更新指定多个表和键的值
    pub async fn upsert(&self,
                        table_kv_list: Vec<TableKV>) -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.upsert(table_kv_list).await
            },
            _ => panic!("Upsert db failed, reason: invalid root transaction"),
        }
    }

    /// 在键值对数据库事务的根事务内，异步删除指定多个表和键的值，并返回删除值的结果集
    pub async fn delete(&self,
                        table_kv_list: Vec<TableKV>)
                        -> Result<Vec<Option<Binary>>, KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.delete(table_kv_list).await
            },
            _ => panic!("Delete db failed, reason: invalid root transaction"),
        }
    }

    /// 在键值对数据库事务的根事务内，获取从指定表和关键字开始，从前向后或从后向前的关键字异步流
    pub async fn keys<'a>(&self,
                          table_name: Atom,
                          key: Option<Binary>,
                          descending: bool)
                          -> Option<BoxStream<'a, Binary>> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.keys(table_name,
                        key,
                        descending).await
            },
            _ => panic!("Get db keys failed, table: {:?}, key: {:?}, descending: {:?}, reason: invalid root transaction", table_name.as_str(), key, descending),
        }
    }

    /// 在键值对数据库事务的根事务内，获取从指定表和关键字开始，从前向后或从后向前的键值对异步流
    pub async fn values<'a>(&self,
                            table_name: Atom,
                            key: Option<Binary>,
                            descending: bool) -> Option<BoxStream<'a, (Binary, Binary)>> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.values(table_name,
                          key,
                          descending).await
            },
            _ => panic!("Get db values failed, table: {:?}, key: {:?}, descending: {:?}, reason: invalid root transaction", table_name.as_str(), key, descending)
        }
    }

    /// 在键值对数据库事务的根事务内，锁住指定表的指定关键字
    pub async fn lock_key(&self,
                          table_name: Atom,
                          key: Binary) -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.lock_key(table_name, key).await
            },
            _ => panic!("Lock table key failed, table: {:?}, key: {:?}, reason: invalid root transaction", table_name.as_str(), key),
        }
    }

    /// 在键值对数据库事务的根事务内，解锁指定表的指定关键字
    pub async fn unlock_key(&self,
                            table_name: Atom,
                            key: Binary) -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.unlock_key(table_name, key).await
            },
            _ => panic!("Unlock table key failed, table: {:?}, key: {:?}, reason: invalid root transaction", table_name.as_str(), key),
        }
    }

    /// 在键值对数据库事务的根事务内，异步预提交本次事务对键值对数据库的所有修改，成功返回预提交的输出
    pub async fn prepare_modified(&self) -> Result<Vec<u8>, KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.prepare_modified().await
            },
            _ => panic!("Prepare modified db failed, reason: invalid root transaction"),
        }
    }

    /// 在键值对数据库事务的根事务内，异步提交本次事务对键值对数据库的所有修改
    pub async fn commit_modified(&self, prepare_output: Vec<u8>) -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.commit_modified(prepare_output).await
            },
            _ => panic!("Commit modified db failed, reason: invalid root transaction"),
        }
    }

    /// 在键值对数据库事务的根事务内，异步回滚本次事务对键值对数据库的所有修改，事务严重错误无法回滚
    pub async fn rollback_modified(&self) -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.rollback_modified().await
            },
            _ => panic!("Rollback modified db failed, reason: invalid root transaction"),
        }
    }
}

///
/// 键值对数据库的根事务的子事务列表
///
#[derive(Clone)]
pub struct KVDBChildTrList<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(VecDeque<KVDBTransaction<C, Log>>);

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Iterator for KVDBChildTrList<C, Log> {
    type Item = KVDBTransaction<C, Log>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_front()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVDBChildTrList<C, Log> {
    /// 构建一个键值对数据库的根事务的子事务列表
    #[inline]
    pub(crate) fn new() -> Self {
        KVDBChildTrList(VecDeque::default())
    }

    /// 获取子事务的数量
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    /// 加入一个指定的子事务
    #[inline]
    pub(crate) fn join(&mut self, tr: KVDBTransaction<C, Log>) -> usize {
        self.0.push_back(tr);
        self.len()
    }
}

///
/// 键值对数据库的根事务
///
#[derive(Clone)]
pub struct RootTransaction<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerRootTransaction<C, Log>>);

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for RootTransaction<C, Log> {}
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for RootTransaction<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> AsyncTransaction for RootTransaction<C, Log> {
    type Output = ();
    type Error = KVTableTrError;

    fn is_writable(&self) -> bool {
        self.0.writable
    }

    // 键值对数据库的提交，会把所有子事务的预提交输出合成为一个提交输入，用于写入提交日志，所以也不需要并发
    fn is_concurrent_commit(&self) -> bool {
        false
    }

    // 键值对数据库的预提交基本都是内存操作，所以回滚也不需要并发
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
        async move {
            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Transaction2Pc for RootTransaction<C, Log> {
    type Tid = Guid;
    type Pid = Guid;
    type Cid = Guid;
    type PrepareOutput = Vec<u8>;
    type PrepareError = KVTableTrError;
    type ConfirmOutput = ();
    type ConfirmError = KVTableTrError;
    type CommitConfirm = KVDBCommitConfirm<C, Log>;

    // 键值对数据库的根事务
    fn is_require_persistence(&self) -> bool {
        self.0.persistence.load(Ordering::Relaxed)
    }

    // 键值对数据库的预提交基本都是内存操作，不需要并发
    fn is_concurrent_prepare(&self) -> bool {
        false
    }

    // 键值对数据库的根事务是根事务，要求所有子事务的事务相关唯一id与根事务相同
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

    fn set_prepare_uid(&self, _uid: <Self as Transaction2Pc>::Pid) {}

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

    // 预提交键值对数据库的根事务
    fn prepare(&self)
               -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        async move {
            if self.is_require_persistence() {
                //本次键值对数据库的根事务，需要持久化，则写入本次键值对数据库的根事务的事务唯一id的预提交输出缓冲区
                let mut prepare_output_head = Vec::new();
                let transaction_uid: Guid = self.get_transaction_uid().unwrap();
                prepare_output_head.put_u128_le(transaction_uid.0); //写入事务唯一id

                Ok(Some(prepare_output_head))
            } else {
                //本次键值对数据库的根事务，不需要持久化，则立即返回
                Ok(None)
            }
        }.boxed()
    }

    fn commit(&self, _confirm: <Self as Transaction2Pc>::CommitConfirm)
              -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        async move {
            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> UnitTransaction for RootTransaction<C, Log> {
    type Status = Transaction2PcStatus;
    type Qos = TableTrQos;

    //键值对数据库的根事务，一定不是单元事务
    fn is_unit(&self) -> bool {
        false
    }

    fn get_status(&self) -> <Self as UnitTransaction>::Status {
        self.0.status.lock().clone()
    }

    fn set_status(&self, status: <Self as UnitTransaction>::Status) {
        *self.0.status.lock() = status;
    }

    fn qos(&self) -> <Self as UnitTransaction>::Qos {
        TableTrQos::Safe
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> SequenceTransaction for RootTransaction<C, Log> {
    type Item = Self;

    // 键值对数据库的根事务，一定不是顺序事务
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
> TransactionTree for RootTransaction<C, Log> {
    type Node = KVDBTransaction<C, Log>; //键值对数据库的根事务的子事务，必须是键值对数据库事务
    type NodeInterator = KVDBChildTrList<C, Log>;

    // 键值对数据库事务的根事务，一定是事务树
    fn is_tree(&self) -> bool {
        true
    }

    // 获取键值对数据库事务的子事务数量
    fn children_len(&self) -> usize {
        self.0.childs.lock().len()
    }

    // 获取键值对数据库事务的子事务迭代器
    fn to_children(&self) -> Self::NodeInterator {
        self.0.childs.lock().clone()
    }
}

/*
* 键值对数据库的根事务同步方法
*/
impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> RootTransaction<C, Log> {
    // 获取需要持久化的子事务数量
    fn persistent_children_len(&self) -> usize {
        let mut len = 0;
        for child in self.to_children() {
            if child.is_require_persistence() {
                len += 1;
            }
        }

        len
    }

    // 创建指定名称表的子事务
    fn table_transaction(&self,
                         name: Atom,
                         table: &KVDBTable<C, Log>,
                         childes_map: &mut XHashMap<Atom, KVDBTransaction<C, Log>>)
                         -> KVDBTransaction<C, Log> {
        match table {
            KVDBTable::MemOrdTab(tab) => {
                //创建有序内存表的表事务，并作为子事务注册到根事务上
                let tr = tab.transaction(self.get_source(),
                                         self.is_writable(),
                                         self.get_prepare_timeout(),
                                         self.get_commit_timeout());
                let table_tr = KVDBTransaction::MemOrdTabTr(tr);

                //注册到键值对数据库的根事务
                childes_map.insert(name, table_tr.clone());
                self.0.childs.lock().join(table_tr.clone());

                table_tr
            },
            KVDBTable::LogOrdTab(tab) => {
                let tr = tab.transaction(self.get_source(),
                                         self.is_writable(),
                                         self.get_prepare_timeout(),
                                         self.get_commit_timeout());
                let table_tr = KVDBTransaction::LogOrdTabTr(tr);

                //注册到键值对数据库的根事务
                childes_map.insert(name, table_tr.clone());
                self.0.childs.lock().join(table_tr.clone());

                table_tr
            },
        }
    }
}

/*
* 键值对数据库的根事务异步方法
*/
impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> RootTransaction<C, Log> {
    /// 异步查询多个表和键的值的结果集
    #[inline]
    pub async fn query(&self,
                       table_kv_list: Vec<TableKV>) -> Vec<Option<Binary>> {
        let mut result = Vec::new();

        for table_kv in table_kv_list {
            if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_kv.table) {
                //指定名称的表存在，则获取表事务，并开始查询表的指定关键字的值
                let mut childes_map = self.0.childs_map.lock();
                let table_tr = if let Some(table_tr) = childes_map.get(&table_kv.table) {
                    //指定名称的表的子事务存在
                    table_tr.clone()
                } else {
                    //指定名称的表的子事务不存在，则创建指定表的事务
                    self.table_transaction(table_kv.table, table, &mut *childes_map)
                };

                match table_tr {
                    KVDBTransaction::RootTr(_tr) => {
                        //忽略键值对数据库的根事务
                        ()
                    },
                    KVDBTransaction::MemOrdTabTr(tr) => {
                        //查询有序内存表的指定关键字的值
                        let value = tr.query(table_kv.key).await;
                        result.push(value);
                    },
                    KVDBTransaction::LogOrdTabTr(tr) => {
                        //查询有序日志表的指定关键字的值
                        let value = tr.query(table_kv.key).await;
                        result.push(value);
                    },
                }
            } else {
                //指定名称的表不存在
                result.push(None);
            }
        }

        result
    }

    /// 异步插入或更新指定多个表和键的值
    #[inline]
    pub async fn upsert(&self,
                        table_kv_list: Vec<TableKV>) -> Result<(), KVTableTrError> {
        for table_kv in table_kv_list {
            if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_kv.table) {
                //指定名称的表存在，则获取表事务，并开始插入或更新表的指定关键字的值
                let mut childes_map = self.0.childs_map.lock();
                let table_tr = if let Some(table_tr) = childes_map.get(&table_kv.table) {
                    //指定名称的表的子事务存在
                    table_tr.clone()
                } else {
                    //指定名称的表的子事务不存在，则创建指定表的事务
                    self.table_transaction(table_kv.table, table, &mut *childes_map)
                };

                if table_tr.is_require_persistence() {
                    //如果任意写操作对应的子事务需要持久化，则根事务也需要持久化
                    self.0.persistence.store(true, Ordering::Relaxed);
                }

                match table_tr {
                    KVDBTransaction::RootTr(_tr) => {
                        //忽略键值对数据库的根事务
                        ()
                    },
                    KVDBTransaction::MemOrdTabTr(tr) => {
                        //插入或更新有序内存表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.upsert(table_kv.key, value).await {
                                //插入或更新有序内存表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                    KVDBTransaction::LogOrdTabTr(tr) => {
                        //插入或更新有序日志表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.upsert(table_kv.key, value).await {
                                //插入或更新有序日志表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                }
            }
        }

        Ok(())
    }

    /// 异步删除指定多个表和键的值，并返回删除值的结果集
    #[inline]
    pub async fn delete(&self,
                        table_kv_list: Vec<TableKV>)
                        -> Result<Vec<Option<Binary>>, KVTableTrError> {
        let mut result = Vec::new();

        for table_kv in table_kv_list {
            if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_kv.table) {
                //指定名称的表存在，则获取表事务，并开始删除表的指定关键字的值
                let mut childes_map = self.0.childs_map.lock();
                let table_tr = if let Some(table_tr) = childes_map.get(&table_kv.table) {
                    //指定名称的表的子事务存在
                    table_tr.clone()
                } else {
                    //指定名称的表的子事务不存在，则创建指定表的事务
                    self.table_transaction(table_kv.table, table, &mut *childes_map)
                };

                if table_tr.is_require_persistence() {
                    //如果任意写操作对应的子事务需要持久化，则根事务也需要持久化
                    self.0.persistence.store(true, Ordering::Relaxed);
                }

                match table_tr {
                    KVDBTransaction::RootTr(_tr) => {
                        //忽略键值对数据库的根事务
                        ()
                    },
                    KVDBTransaction::MemOrdTabTr(tr) => {
                        //删除有序内存表的指定关键字的值
                        match tr.delete(table_kv.key).await {
                            Err(e) => {
                                //删除有序内存表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除有序内存表的指定关键字的值成功
                                result.push(value);
                            },
                        }
                    },
                    KVDBTransaction::LogOrdTabTr(tr) => {
                        //删除有序日志表的指定关键字的值
                        match tr.delete(table_kv.key).await {
                            Err(e) => {
                                //删除有序日志表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除有序日志表的指定关键字的值成功
                                result.push(value);
                            },
                        }
                    },
                }
            } else {
                //指定名称的表不存在
                result.push(None);
            }
        }

        Ok(result)
    }

    /// 获取从指定表和关键字开始，从前向后或从后向前的关键字异步流
    #[inline]
    pub async fn keys<'a>(&self,
                          table_name: Atom,
                          key: Option<Binary>,
                          descending: bool)
                          -> Option<BoxStream<'a, Binary>> {
        if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_name) {
            //指定名称的表存在，则获取表事务，并开始获取关键字的异步流
            let mut childes_map = self.0.childs_map.lock();
            let table_tr = if let Some(table_tr) = childes_map.get(&table_name) {
                //指定名称的表的子事务存在
                table_tr.clone()
            } else {
                //指定名称的表的子事务不存在，则创建指定表的事务
                self.table_transaction(table_name, table, &mut *childes_map)
            };

            match table_tr {
                KVDBTransaction::RootTr(_tr) => {
                    //忽略键值对数据库的根事务
                    None
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    //获取有序内存表的关键字的异步流
                    Some(tr.keys(key, descending))
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    //获取有序日志表的关键字的异步流
                    Some(tr.keys(key, descending))
                },
            }
        } else {
            //指定名称的表不存在
            None
        }
    }

    /// 获取从指定表和关键字开始，从前向后或从后向前的键值对异步流
    #[inline]
    pub async fn values<'a>(&self,
                            table_name: Atom,
                            key: Option<Binary>,
                            descending: bool) -> Option<BoxStream<'a, (Binary, Binary)>> {
        if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_name) {
            //指定名称的表存在，则获取表事务，并开始获取键值对异步流
            let mut childes_map = self.0.childs_map.lock();
            let table_tr = if let Some(table_tr) = childes_map.get(&table_name) {
                //指定名称的表的子事务存在
                table_tr.clone()
            } else {
                //指定名称的表的子事务不存在，则创建指定表的事务
                self.table_transaction(table_name, table, &mut *childes_map)
            };

            match table_tr {
                KVDBTransaction::RootTr(_tr) => {
                    //忽略键值对数据库的根事务
                    None
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    //获取有序内存表的键值对异步流
                    Some(tr.values(key, descending))
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    //获取有序日志表的键值对异步流
                    Some(tr.values(key, descending))
                },
            }
        } else {
            //指定名称的表不存在
            None
        }
    }

    /// 锁住指定表的指定关键字
    #[inline]
    pub async fn lock_key(&self,
                          table_name: Atom,
                          key: Binary) -> Result<(), KVTableTrError> {
        if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_name) {
            //指定名称的表存在，则获取表事务，并开始锁住指定表的指定关键字
            let mut childes_map = self.0.childs_map.lock();
            let table_tr = if let Some(table_tr) = childes_map.get(&table_name) {
                //指定名称的表的子事务存在
                table_tr.clone()
            } else {
                //指定名称的表的子事务不存在，则创建指定表的事务
                self.table_transaction(table_name, table, &mut *childes_map)
            };

            match table_tr {
                KVDBTransaction::RootTr(_tr) => {
                    //忽略键值对数据库的根事务
                    Ok(())
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    //锁住有序内存表的指定关键字
                    tr.lock_key(key).await
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    //锁住有序日志表的指定关键字
                    tr.lock_key(key).await
                },
            }
        } else {
            //指定名称的表不存在
            Ok(())
        }
    }

    /// 解锁指定表的指定关键字
    #[inline]
    pub async fn unlock_key(&self,
                            table_name: Atom,
                            key: Binary) -> Result<(), KVTableTrError> {
        if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_name) {
            //指定名称的表存在，则获取表事务，并开始解锁指定表的指定关键字
            let mut childes_map = self.0.childs_map.lock();
            let table_tr = if let Some(table_tr) = childes_map.get(&table_name) {
                //指定名称的表的子事务存在
                table_tr.clone()
            } else {
                //指定名称的表的子事务不存在，则创建指定表的事务
                self.table_transaction(table_name, table, &mut *childes_map)
            };

            match table_tr {
                KVDBTransaction::RootTr(_tr) => {
                    //忽略键值对数据库的根事务
                    Ok(())
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    //解锁有序内存表的指定关键字
                    tr.unlock_key(key).await
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    //解锁有序日志表的指定关键字
                    tr.unlock_key(key).await
                },
            }
        } else {
            //指定名称的表不存在
            Ok(())
        }
    }

    /// 异步预提交本次事务对键值对数据库的所有修改，成功返回预提交的输出
    #[inline]
    pub async fn prepare_modified(&self) -> Result<Vec<u8>, KVTableTrError> {
        if self.get_status() != Transaction2PcStatus::Rollbacked {
            //本次事务的当前状态只要不为回滚成功，则先初始化键值对数据库的根事务
            if let Err(e) = self
                .0
                .db_mgr
                .0
                .tr_mgr
                .start(KVDBTransaction::RootTr(self.clone()))
                .await {
                //初始化键值对数据库的根事务失败，则立即返回错误原因
                return Err(e);
            }
        }

        //预提交键值对数据库的根事务
        match self
            .0
            .db_mgr
            .0
            .tr_mgr
            .prepare(KVDBTransaction::RootTr(self.clone()))
            .await {
            Err(e) => {
                //预提交键值对数据库的根事务失败，则立即返回错误原因
                return Err(e);
            },
            Ok(prepare_output) => {
                //预提交键值对数据库的根事务成功
                if self.is_require_persistence() {
                    //本次键值对数据库的根事务，需要持久化
                    if let Some(output) = prepare_output {
                        //键值对数据库的预提交事务，有返回预提交输出
                        if output.len() > 16 {
                            //有效的预提交输出，根事务需要持久化，且至少有一个子事务需要持久化
                            Ok(output)
                        } else {
                            //无效的预提交输出，根事务需要持久化，但所有子事务不需要持久化
                            Ok(vec![])
                        }
                    } else {
                        //预提交键值对数据库的子事务，没有返回预提交输出
                        Ok(vec![])
                    }
                } else {
                    //本次键值对数据库的根事务，不需要持久化
                    Ok(vec![])
                }
            },
        }
    }

    /// 异步提交本次事务对键值对数据库的所有修改
    #[inline]
    pub async fn commit_modified(&self, prepare_output: Vec<u8>) -> Result<(), KVTableTrError> {
        //为本次事务的异步提交确认，创建提交确认回调
        let commit_confirm = KVDBCommitConfirm::new(self.0.db_mgr.0.rt.clone(),
                                                    self.0.db_mgr.0.tr_mgr.commit_logger(),
                                                    self.get_transaction_uid().unwrap(),
                                                    self.get_commit_uid(),
                                                    self.persistent_children_len());

        //提交键值对数据库的根事务
        match self
            .0
            .db_mgr
            .0
            .tr_mgr
            .commit(KVDBTransaction::RootTr(self.clone()),
                    prepare_output,
                    commit_confirm)
            .await {
            Err(e) => Err(e),
            Ok(_) => {
                //提交键值对数据库的根事务成功，则完成本次键值对数据库事务
                self
                    .0
                    .db_mgr
                    .0
                    .tr_mgr
                    .finish(KVDBTransaction::RootTr(self.clone()));
                Ok(())
            }
        }
    }

    ///
    /// 异步回滚本次事务对键值对数据库的所有修改，事务严重错误无法回滚
    ///
    #[inline]
    pub async fn rollback_modified(&self) -> Result<(), KVTableTrError> {
        //回滚键值对数据库的根事务
        if let Err(e) = self
            .0
            .db_mgr
            .0
            .tr_mgr
            .rollback(KVDBTransaction::RootTr(self.clone()))
            .await {
            //回滚键值对数据库的根事务失败，则立即返回错误原因
            return Err(e);
        }

        Ok(())
    }
}

///
/// 内部键值对数据库的根事务
///
struct InnerRootTransaction<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    source:             Atom,                                               //事件源
    tid:                SpinLock<Option<Guid>>,                             //事务唯一id
    cid:                SpinLock<Option<Guid>>,                             //事务提交唯一id
    status:             SpinLock<Transaction2PcStatus>,                     //事务状态
    writable:           bool,                                               //事务是否可写
    persistence:        AtomicBool,                                         //事务是否持久化
    prepare_timeout:    u64,                                                //事务预提交超时时长，单位毫秒
    commit_timeout:     u64,                                                //事务提交超时时长，单位毫秒
    childs_map:         SpinLock<XHashMap<Atom, KVDBTransaction<C, Log>>>,  //子事务表
    childs:             SpinLock<KVDBChildTrList<C, Log>>,                  //子事务列表
    db_mgr:             KVDBManager<C, Log>,                                //键值对数据库管理器
}

///
/// 键值对数据库的表
///
#[derive(Clone)]
pub enum KVDBTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    MemOrdTab(MemoryOrderedTable<C, Log>),  //有序内存表
    LogOrdTab(LogOrderedTable<C, Log>),     //有序日志表
}

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for KVDBTable<C, Log> {}
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for KVDBTable<C, Log> {}