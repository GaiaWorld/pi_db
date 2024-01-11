use std::time::Instant;
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::collections::{VecDeque, HashMap};
use std::io::{Error, Result as IOResult, ErrorKind};
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};

use futures::{future::{FutureExt, BoxFuture}, stream::BoxStream, StreamExt};
use crossbeam_channel::bounded;
use async_lock::RwLock;
use dashmap::DashMap;
use lazy_static::lazy_static;
use bytes::BufMut;
use log::{info, error};

#[cfg(feature = "trace")]
use tracing::Instrument;
#[cfg(feature = "trace")]
use opentelemetry::{Context,
                    propagation::TextMapPropagator,
                    sdk::propagation::TraceContextPropagator};
#[cfg(feature = "trace")]
use tracing_opentelemetry::OpenTelemetrySpanExt;
#[cfg(feature = "trace")]
use tracing_subscriber::prelude::*;

use pi_atom::Atom;
use pi_bon::{WriteBuffer, ReadBuffer, Encode, Decode, ReadBonErr};
use pi_guid::Guid;
use pi_async_rt::{lock::spin_lock::SpinLock,
                  rt::{AsyncRuntime,
                       multi_thread::MultiTaskRuntime}};
use pi_async_transaction::{AsyncTransaction, Transaction2Pc, UnitTransaction, SequenceTransaction, TransactionTree, AsyncCommitLog, ErrorLevel, TransactionError,
                           manager_2pc::{Transaction2PcStatus, Transaction2PcManager}};
use pi_async_file::file::create_dir;
use pi_hash::XHashMap;

use crate::{Binary,
            KVAction,
            KVDBTableType,
            KVTableMeta,
            TableTrQos,
            KVDBCommitConfirm,
            KVTableTrError,
            tables::{KVTable,
                     TableKV,
                     meta_table::{MetaTable,
                                  MetaTabTr},
                     mem_ord_table::{MemoryOrderedTable,
                                     MemOrdTabTr},
                     log_ord_table::{LogOrderedTable,
                                     LogOrdTabTr},
                     log_write_table::{LogWriteTable,
                                       LogWTabTr}},
            utils::CreateTableOptions};

///
/// 默认的数据库表元信息目录名
///
pub(crate) const DEFAULT_DB_TABLES_META_DIR: &str = ".tables_meta";

///
/// 默认的数据库表所在目录名
///
const DEFAULT_DB_TABLES_DIR: &str = ".tables";

///
/// 数据库未启动状态
///
const DB_UNSTARTUP_STATUS: u64 = 0;

///
/// 数据库正在初始化状态
///
const DB_INITING_STATUS: u64 = 1;

///
/// 数据库已初始化状态
///
const DB_INITED_STATUS: u64 = 2;

///
/// 数据库正在关闭状态
///
const DB_CLOSEING_STATUS: u64 = 3;

///
/// 数据库已关闭状态
///
const DB_CLOSED_STATUS: u64 = 4;

#[cfg(feature = "trace")]
lazy_static! {
    static ref TransactionSpans: DashMap<Guid, Context> = DashMap::default();
}

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

/*
* 键值对数据库管理器构建器同步方法
*/
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
}

/*
* 键值对数据库管理器构建器异步方法
*/
impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVDBManagerBuilder<C, Log> {
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

        //创建键值对数据库管理器
        let rt = self.rt;
        let tr_mgr = self.tr_mgr;
        let db_path = self.db_path;
        let tables_meta_path = self.tables_meta_path;
        let tables_path = self.tables_path;
        let tables = Arc::new(RwLock::new(XHashMap::default()));
        let status = AtomicU64::new(DB_INITING_STATUS);
        let inner = InnerKVDBManager {
            rt,
            tr_mgr,
            db_path,
            tables_meta_path,
            tables_path,
            tables,
            status,
        };
        let db_mgr = KVDBManager(Arc::new(inner));

        //加载并注册元信息表
        let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
        let meta_table: MetaTable<C, Log> =
            MetaTable::new(db_mgr.0.rt.clone(),
                           db_mgr.tables_meta_path().to_path_buf(),
                           meta_table_name.clone(),
                           512 * 1024 * 1024,
                           2 * 1024 * 1024,
                           None,
                           2 * 1024 * 1024,
                           true,
                           16 * 1024 * 1024,
                           60 * 1000).await;
        db_mgr.0.tables.write().await.insert(meta_table_name.clone(), KVDBTable::MetaTab(meta_table));

        //根据元信息表的元信息，加载其它表，加载操作使用的事务，不需要预提交和提交
        let mut tr = db_mgr
            .transaction(Atom::from("Startup db"),
                         true,
                         1000,
                         1000)
            .unwrap();
        let mut meta_iterator = tr
            .values(meta_table_name.clone(),
                    None,
                    false)
            .await
            .unwrap();
        while let Some((key, value)) = meta_iterator.next().await {
            let table_name = match binary_to_table(&key) {
                Err(e) => {
                    //反序列化表名失败
                    return Err(Error::new(ErrorKind::Other, format!("From binary to table name failed, reason: {:?}", e)));
                },
                Ok(table_name) => {
                    //反序列化表名成功
                    table_name
                }
            };

            if table_name == meta_table_name {
                //忽略元信息表
                continue;
            }
            let table_meta = KVTableMeta::from(value);

            if let Err(e) = tr.create_table(table_name.clone(), table_meta.clone()).await {
                //加载指定的表失败，则立即返回错误原因
                db_mgr.0.status.store(DB_UNSTARTUP_STATUS, Ordering::SeqCst);
                return Err(Error::new(ErrorKind::Other, format!("Load table failed, tables_path: {:?}, table: {:?}, meta: {:?}, reason: {:?}", db_mgr.tables_path(), table_name, table_meta, e)));
            }
        }

        //如果有未确认的提交日志，则尝试修复数据库表数据
        let now = Instant::now();
        match db_mgr.try_repair().await {
            Err(e) => {
                //有未确认的提交日志，且尝试修复数据库表数据失败，则立即返回错误原因
                return Err(e);
            },
            Ok((repaired_log_len, repaired_bytes_len)) => {
                //尝试修复数据库表数据成功
                if repaired_log_len > 0 {
                    //未确认的提交日志
                    info!("Repair db ok, logs: {}, bytes: {}, time: {:?}",
                        repaired_log_len,
                        repaired_bytes_len,
                        now.elapsed());
                }
            }
        }

        db_mgr.0.status.store(DB_INITED_STATUS, Ordering::SeqCst); //设置数据库状态为已初始化
        info!("Startup db ok, tables: {}", db_mgr.table_size().await);

        Ok(db_mgr)
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
                       commit_timeout: u64) -> Option<KVDBTransaction<C, Log>> {
        let status = self.0.status.load(Ordering::Relaxed);
        if status != DB_INITING_STATUS && status != DB_INITED_STATUS {
            //当前数据库状态不允许创建键值对数据库的根事务，则立即返回空
            return None;
        }

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

        Some(KVDBTransaction::RootTr(RootTransaction(Arc::new(inner))))
    }

    ///
    /// 关闭数据库，立即禁止创建数据库事务
    ///
    pub fn close(&self) {
        if self.0.tr_mgr.transaction_len() == 0 {
            //如果当前事务管理器没有任何正在执行的事务，则设置数据库状态为已关闭
            self.0.status.store(DB_CLOSED_STATUS, Ordering::SeqCst);
        } else {
            //如果当前事务管理器还有任何正在执行的事务，则设置数据库状态为正在状态
            self.0.status.store(DB_CLOSEING_STATUS, Ordering::SeqCst);
        }
    }
}

/*
* 键值对数据库管理器异步方法
*/
impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>
> KVDBManager<C, Log> {
    /// 获取指定名称的表
    pub(crate) async fn get_table(&self, table_name: &Atom) -> Option<KVDBTable<C, Log>> {
        if let Some(table) = self.0.tables.read().await.get(table_name) {
            Some(table.clone())
        } else {
            None
        }
    }

    /// 异步判断指定名称的表是否存在
    pub async fn is_exist(&self, table_name: &Atom) -> bool {
        self.0.tables.read().await.contains_key(table_name)
    }

    /// 异步获取键值对数据库的表数量
    pub async fn table_size(&self) -> usize {
        self.0.tables.read().await.len()
    }

    /// 异步获取键值对数据库的所有表的名称列表
    pub async fn tables(&self) -> Vec<Atom> {
        let mut table_names = Vec::new();
        for key in self.0.tables.read().await.keys() {
            table_names.push(key.clone());
        }

        table_names
    }

    /// 异步获取指定名称的数据表所在目录的路径，返回空表示指定名称的表不存在
    pub async fn table_path(&self, table_name: &Atom) -> Option<PathBuf> {
        match self.0.tables.read().await.get(table_name) {
            None => None,
            Some(KVDBTable::MetaTab(table)) => {
                if let Some(path) = table.path() {
                    Some(path.to_path_buf())
                } else {
                    None
                }
            },
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
            Some(KVDBTable::LogWTab(table)) => {
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
        match self.0.tables.read().await.get(table_name) {
            None => None,
            Some(KVDBTable::MetaTab(table)) => {
                Some(table.is_persistent())
            },
            Some(KVDBTable::MemOrdTab(table)) => {
                Some(table.is_persistent())
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                Some(table.is_persistent())
            },
            Some(KVDBTable::LogWTab(table)) => {
                Some(table.is_persistent())
            },
        }
    }

    /// 异步判断指定名称的数据表是否有序，返回空表示指定名称的表不存在
    pub async fn is_ordered_table(&self, table_name: &Atom) -> Option<bool> {
        match self.0.tables.read().await.get(table_name) {
            None => None,
            Some(KVDBTable::MetaTab(table)) => {
                Some(table.is_ordered())
            },
            Some(KVDBTable::MemOrdTab(table)) => {
                Some(table.is_ordered())
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                Some(table.is_ordered())
            },
            Some(KVDBTable::LogWTab(table)) => {
                Some(table.is_ordered())
            },
        }
    }

    /// 异步获取指定名称的数据表的记录数，返回空表示指定名称的表不存在
    pub async fn table_record_size(&self, table_name: &Atom) -> Option<usize> {
        match self.0.tables.read().await.get(table_name) {
            None => None,
            Some(KVDBTable::MetaTab(table)) => {
                Some(table.len())
            },
            Some(KVDBTable::MemOrdTab(table)) => {
                Some(table.len())
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                Some(table.len())
            },
            Some(KVDBTable::LogWTab(table)) => {
                Some(table.len())
            },
        }
    }

    /// 追加一个新的提交日志
    pub async fn append_new_commit_log(&self) -> IOResult<usize> {
        let commit_logger = self.0.tr_mgr.commit_logger();
        commit_logger.append_check_point().await
    }

    /// 异步准备整理指定名称的数据表，准备整理成功，才允许开始整理表
    pub async fn ready_collect_table(&self, table_name: &Atom) -> IOResult<()> {
        match self.0.tables.read().await.get(table_name) {
            None => (),
            Some(KVDBTable::MetaTab(table)) => {
                if let Err(e) = table.ready_collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
            Some(KVDBTable::MemOrdTab(table)) => {
                if let Err(e) = table.ready_collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                if let Err(e) = table.ready_collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
            Some(KVDBTable::LogWTab(table)) => {
                if let Err(e) = table.ready_collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
        }

        Ok(())
    }

    /// 异步整理指定名称的数据表
    pub async fn collect_table(&self, table_name: &Atom) -> IOResult<()> {
        match self.0.tables.read().await.get(table_name) {
            None => (),
            Some(KVDBTable::MetaTab(table)) => {
                if let Err(e) = table.collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
            Some(KVDBTable::MemOrdTab(table)) => {
                if let Err(e) = table.collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                if let Err(e) = table.collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
            Some(KVDBTable::LogWTab(table)) => {
                if let Err(e) = table.collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
        }

        Ok(())
    }

    // 尝试幂等的重播未确认的提交日志，并修复数据库表数据
    // 注意如果在只有单个线程的运行时修复或并发修复，则可能会发生阻塞
    pub(crate) async fn try_repair(&self) -> IOResult<(usize, usize)> {
        //构建重播回调
        let db_mgr = self.clone();

        let replay_callback = move |commit_uid: Guid, prepare_output: Vec<u8>| -> IOResult<()> {
            //异步执行重播
            let db_mgr_copy = db_mgr.clone();
            let commit_uid_copy = commit_uid.clone();
            let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
            let (sender, receiver) = bounded(1);

            let boxed = async move {
                let bytes_len = prepare_output.len(); //获取日志缓冲区长度
                let mut offset = 0; //日志缓冲区偏移
                let bytes = prepare_output.as_slice();
                let uid = u128::from_le_bytes(bytes[0..16].try_into().unwrap()); //获取事务唯一id
                let transaciton_uid = Guid(uid);
                offset += 16; //移动缓冲区指针

                if let Some(tr) =
                db_mgr_copy.transaction(Atom::from("Repair db"),
                                        true,
                                        5000,
                                        5000) {
                    //创建数据库事务成功，则迭代日志缓冲区中，本次未确认的提交日志中执行写操作的表和相关键值对
                    //迭代完成后，则可以恢复本次未确认的提交日志对表的内存键值对的修改，并生成对应的键值对操作记录
                    while offset < bytes_len {
                        //获取表名、操作的键值对数量和新的日志缓冲区偏移
                        let (table, kvs_len, new_offset) =
                            <MetaTable<C, Log> as KVTable>::get_init_table_prepare_output(&prepare_output, offset);

                        //获取操作的表键值列表和新的日志缓冲区偏移
                        let (writes, new_offset)
                            = <MetaTable<C, Log> as KVTable>::get_all_key_value_from_table_prepare_output(&prepare_output, &table, kvs_len, new_offset);

                        if table == meta_table_name {
                            //未确认的提交日志操作的表是元信息表，则创建或删除表
                            for write in writes {
                                if let Some(value) = write.value {
                                    //有值，则创建表
                                    let table_name = match binary_to_table(&write.key) {
                                        Err(e) => {
                                            //反序列化表名失败
                                            panic!("From binary to table name failed, reason: {:?}", e);
                                        },
                                        Ok(table_name) => {
                                            //反序列化表名成功
                                            table_name
                                        }
                                    };
                                    let table_meta = KVTableMeta::from(value);

                                    if let Err(e) = tr.repair_create_table(table_name.clone(), table_meta.clone()).await {
                                        //重播的创建表失败，则立即返回错误原因
                                        let _ = sender.send(Err(Error::new(ErrorKind::Other, format!("Repair tables meta failed, transaction_uid: {:?}, commit_uid: {:?}, table_name: {:?}, table_meta: {:?}, reason: {:?}", transaciton_uid, commit_uid_copy, table_name, table_meta, e))));
                                        return;
                                    }
                                } else {
                                    //无值，则删除表
                                    let table_name = Atom::from(write.key.as_ref());

                                    if let Err(e) = tr.repair_remove_table(table_name.clone()).await {
                                        //重播的移除表失败，则立即返回错误原因
                                        let _ = sender.send(Err(Error::new(ErrorKind::Other, format!("Repair tables meta failed, transaction_uid: {:?}, commit_uid: {:?}, table_name: {:?}, reason: {:?}", transaciton_uid, commit_uid_copy, table_name, e))));
                                        return;
                                    }
                                }
                            }
                        } else {
                            //未确认的提交日志操作的表是其它表，则执行本次未确认的提交日志中指定表的键值对写操作
                            for write in writes {
                                if write.exist_value() {
                                    //有值，则执行插入或更新操作
                                    if let Err(e) = tr.upsert(vec![write]).await {
                                        //重播的插入或更新操作失败，则立即返回错误原因
                                        let _ = sender.send(Err(Error::new(ErrorKind::Other, format!("Repair db failed, transaction_uid: {:?}, commit_uid: {:?}, reason: {:?}", transaciton_uid, commit_uid_copy, e))));
                                        return;
                                    }
                                } else {
                                    //无值，则执行删除操作
                                    if let Err(e) = tr.delete(vec![write]).await {
                                        //重播的删除操作失败，则立即返回错误原因
                                        let _ = sender.send(Err(Error::new(ErrorKind::Other, format!("Repair db failed, transaction_uid: {:?}, commit_uid: {:?}, reason: {:?}", transaciton_uid, commit_uid_copy, e))));
                                        return;
                                    }
                                }
                            }
                        }

                        //更新日志缓冲区偏移
                        offset = new_offset;
                    }

                    //指定本次重播事务的事务唯一id后执行预提交修复
                    if let Err(e) = tr.prepare_repair(transaciton_uid.clone()).await {
                        //预提交重播事务失败，则立即返回错误原因
                        let _ = sender.send(Err(Error::new(ErrorKind::Other, format!("Repair db failed, transaction_uid: {:?}, commit_uid: {:?}, reason: {:?}", transaciton_uid, commit_uid_copy, e))));
                        return;
                    }

                    //指定本次重播事务的事务唯一id和事务提交唯一id后执行提交修复
                    if let Err(e) = tr
                        .commit_repair(transaciton_uid.clone(),
                                       commit_uid_copy.clone(),
                                       prepare_output).await {
                        //提交重播事务失败，则立即返回错误原因
                        let _ = sender.send(Err(Error::new(ErrorKind::Other, format!("Repair db failed, transaction_uid: {:?}, commit_uid: {:?}, reason: {:?}", transaciton_uid, commit_uid_copy, e))));
                        return;
                    }

                    //返回成功重播一条未确认的提交日志
                    let _ = sender.send(Ok(()));
                } else {
                    //创建数据库事务失败，则立即返回错误原因
                    let _ = sender.send(Err(Error::new(ErrorKind::Other, format!("Repair db failed, transaction_uid: {:?}, commit_uid: {:?}, reason: get db transaction error", transaciton_uid, commit_uid_copy))));
                }
            }.boxed();
            let _ = db_mgr.0.rt.spawn(boxed);

            //注意单个线程的运行时重播或并发重播，则可能会发生阻塞
            match receiver.recv() {
                Err(e) => {
                    //同步通道异常，则立即返回错误原因
                    Err(Error::new(ErrorKind::Other, format!("Repair db failed, commit_uid: {:?}, reason: {:?}", commit_uid, e)))
                },
                Ok(result) => {
                    //同步阻塞的等待异步重播完成，则立即返回重播结果
                    result
                },
            }
        };

        //异步重播所有未确认的提交日志
        let replay_result = self.0.tr_mgr.replay_commit_log(replay_callback).await?;

        //所有未确认的提交日志已完成重播，则立即返回数据库修复成功
        let _ = self.0.tr_mgr.finish_replay().await?; //通知事务管理器，已完成重播
        return Ok(replay_result);
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
    status:             AtomicU64,                                      //数据库状态
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
    MetaTabTr(MetaTabTr<C, Log>),       //元信息表事务
    MemOrdTabTr(MemOrdTabTr<C, Log>),   //有序内存表事务
    LogOrdTabTr(LogOrdTabTr<C, Log>),   //有序日志表事务
    LogWTabTr(LogWTabTr<C, Log>),       //只写日志表事务
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
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_writable()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_writable()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_writable()
            },
            KVDBTransaction::LogWTabTr(tr) => {
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
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_concurrent_commit()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_concurrent_commit()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_concurrent_commit()
            },
            KVDBTransaction::LogWTabTr(tr) => {
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
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_concurrent_rollback()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_concurrent_rollback()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_concurrent_rollback()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.is_concurrent_rollback()
            },
        }
    }

    fn get_source(&self) -> Atom {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_source()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.get_source()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_source()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_source()
            },
            KVDBTransaction::LogWTabTr(tr) => {
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
            KVDBTransaction::MetaTabTr(tr) => {
                tr.init()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.init()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.init()
            },
            KVDBTransaction::LogWTabTr(tr) => {
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
            KVDBTransaction::MetaTabTr(tr) => {
                tr.rollback()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.rollback()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.rollback()
            },
            KVDBTransaction::LogWTabTr(tr) => {
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
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_require_persistence()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_require_persistence()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_require_persistence()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.is_require_persistence()
            },
        }
    }

    fn require_persistence(&self) {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.require_persistence();
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.require_persistence();
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.require_persistence();
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.require_persistence();
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.require_persistence();
            },
        }
    }

    fn is_concurrent_prepare(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_concurrent_prepare()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_concurrent_prepare()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_concurrent_prepare()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_concurrent_prepare()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.is_concurrent_prepare()
            },
        }
    }

    fn is_enable_inherit_uid(&self) -> bool {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.is_enable_inherit_uid()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_enable_inherit_uid()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_enable_inherit_uid()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_enable_inherit_uid()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.is_enable_inherit_uid()
            },
        }
    }

    fn get_transaction_uid(&self) -> Option<<Self as Transaction2Pc>::Tid> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_transaction_uid()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.get_transaction_uid()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_transaction_uid()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_transaction_uid()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.get_transaction_uid()
            },
        }
    }

    fn set_transaction_uid(&self, uid: <Self as Transaction2Pc>::Tid) {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.set_transaction_uid(uid);
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.set_transaction_uid(uid);
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.set_transaction_uid(uid);
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.set_transaction_uid(uid);
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.set_transaction_uid(uid);
            },
        }
    }

    fn get_prepare_uid(&self) -> Option<<Self as Transaction2Pc>::Pid> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_prepare_uid()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.get_prepare_uid()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_prepare_uid()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_prepare_uid()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.get_prepare_uid()
            },
        }
    }

    fn set_prepare_uid(&self, uid: <Self as Transaction2Pc>::Pid) {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.set_prepare_uid(uid);
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.set_prepare_uid(uid);
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.set_prepare_uid(uid);
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.set_prepare_uid(uid);
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.set_prepare_uid(uid);
            },
        }
    }

    fn get_commit_uid(&self) -> Option<<Self as Transaction2Pc>::Cid> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_commit_uid()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.get_commit_uid()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_commit_uid()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_commit_uid()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.get_commit_uid()
            },
        }
    }

    fn set_commit_uid(&self, uid: <Self as Transaction2Pc>::Cid) {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.set_commit_uid(uid);
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.set_commit_uid(uid);
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.set_commit_uid(uid);
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.set_commit_uid(uid);
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.set_commit_uid(uid);
            },
        }
    }

    fn get_prepare_timeout(&self) -> u64 {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_prepare_timeout()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.get_prepare_timeout()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_prepare_timeout()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_prepare_timeout()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.get_prepare_timeout()
            },
        }
    }

    fn get_commit_timeout(&self) -> u64 {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_commit_timeout()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.get_commit_timeout()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_commit_timeout()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_commit_timeout()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.get_commit_timeout()
            },
        }
    }

    fn prepare(&self)
               -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        #[cfg(feature = "trace")]
        {
            let mut carrier = HashMap::new();
            carrier.insert(
                "traceparent".to_string(),
                self.get_source().as_str().to_string(),
            );
            let propagator = TraceContextPropagator::new();
            let parent_context = propagator.extract(&carrier);
            let cid = self.get_transaction_uid();
            let span = tracing::debug_span!("db_prepare",
                tid = cid.clone().unwrap_or(Guid(0)).0,
                cid = self.get_commit_uid().unwrap_or(Guid(0)).0,
                status = self.get_status() as u8);
            span.set_parent(parent_context);

            if let Some(key) = cid {
                //当前事务的事务id存在，则记录预提交Span的上下文
                let context = span.context();
                TransactionSpans.insert(key, context);
            }

            match self {
                KVDBTransaction::RootTr(tr) => {
                    return tr
                        .prepare()
                        .instrument(span)
                        .boxed();
                },
                KVDBTransaction::MetaTabTr(tr) => {
                    return tr
                        .prepare()
                        .instrument(span)
                        .boxed();
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    return tr
                        .prepare()
                        .instrument(span)
                        .boxed();
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    return tr
                        .prepare()
                        .instrument(span)
                        .boxed();
                },
                KVDBTransaction::LogWTabTr(tr) => {
                    return tr
                        .prepare()
                        .instrument(span)
                        .boxed();
                },
            }
        }

        #[cfg(feature = "default")]
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.prepare()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.prepare()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.prepare()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.prepare()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.prepare()
            },
        }
    }

    fn commit(&self, confirm: <Self as Transaction2Pc>::CommitConfirm)
              -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        #[cfg(feature = "trace")]
        {
            let cid = self.get_transaction_uid().unwrap_or(Guid(0));
            //当前事务的事务id存在
            let db_commit_span = if let Some((_, parent_context)) = TransactionSpans.remove(&cid) {
                //当前事务有预提交Span
                let span = tracing::debug_span!("db_commit",
                        tid = cid.0,
                        cid = self.get_commit_uid().unwrap_or(Guid(0)).0,
                        status = self.get_status() as u8);
                span.set_parent(parent_context);
                span
            } else {
                //当前事务没有预提交Span
                let span = tracing::debug_span!("db_commit",
                        tid = cid.0,
                        cid = self.get_commit_uid().unwrap_or(Guid(0)).0,
                        status = self.get_status() as u8);
                span
            };

            match self {
                KVDBTransaction::RootTr(tr) => {
                    return tr
                        .commit(confirm)
                        .instrument(db_commit_span)
                        .boxed();
                },
                KVDBTransaction::MetaTabTr(tr) => {
                    return tr
                        .commit(confirm)
                        .instrument(db_commit_span)
                        .boxed();
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    return tr
                        .commit(confirm)
                        .instrument(db_commit_span)
                        .boxed();
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    return tr
                        .commit(confirm)
                        .instrument(db_commit_span)
                        .boxed();
                },
                KVDBTransaction::LogWTabTr(tr) => {
                    return tr
                        .commit(confirm)
                        .instrument(db_commit_span)
                        .boxed();
                },
            }
        }

        #[cfg(feature = "default")]
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.commit(confirm)
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.commit(confirm)
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.commit(confirm)
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.commit(confirm)
            },
            KVDBTransaction::LogWTabTr(tr) => {
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
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_unit()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_unit()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_unit()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.is_unit()
            },
        }
    }

    fn get_status(&self) -> <Self as UnitTransaction>::Status {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.get_status()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.get_status()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.get_status()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.get_status()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.get_status()
            },
        }
    }

    fn set_status(&self, status: <Self as UnitTransaction>::Status) {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.set_status(status);
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.set_status(status);
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.set_status(status);
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.set_status(status);
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.set_status(status);
            },
        }
    }

    fn qos(&self) -> <Self as UnitTransaction>::Qos {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.qos()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.qos()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.qos()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.qos()
            },
            KVDBTransaction::LogWTabTr(tr) => {
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
            KVDBTransaction::MetaTabTr(tr) => {
                tr.is_tree()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.is_tree()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.is_tree()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.is_tree()
            },
        }
    }

    fn children_len(&self) -> usize {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.children_len()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.children_len()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.children_len()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.children_len()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.children_len()
            },
        }
    }

    fn to_children(&self) -> Self::NodeInterator {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.to_children()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.to_children()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.to_children()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.to_children()
            },
            KVDBTransaction::LogWTabTr(tr) => {
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
    /// 异步获取表的元信息
    pub async fn table_meta(&self, name: Atom) -> Option<KVTableMeta> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.table_meta(name).await
            },
            _ => panic!("Get table meta failed, reason: invalid root transaction"),
        }
    }

    /// 异步创建表，需要指定表名和表的元信息
    pub async fn create_table_with_options(&self,
                                           name: Atom,
                                           meta: KVTableMeta,
                                           options: CreateTableOptions) -> IOResult<()> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.create_table_with_options(name, meta, options).await
            },
            _ => panic!("Create table failed, reason: invalid root transaction"),
        }
    }

    /// 异步创建表，需要指定表名和表的元信息
    pub async fn create_table(&self,
                              name: Atom,
                              meta: KVTableMeta) -> IOResult<()> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.create_table(name, meta).await
            },
            _ => panic!("Create table failed, reason: invalid root transaction"),
        }
    }

    /// 异步修复创建表，需要指定表名和表的元信息
    pub(crate) async fn repair_create_table(&self,
                                            name: Atom,
                                            meta: KVTableMeta) -> IOResult<()> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.repair_create_table(name, meta).await
            },
            _ => panic!("Create table failed, reason: invalid root transaction"),
        }
    }

    /// 异步移除表
    pub async fn remove_table(&self, name: Atom) -> IOResult<()> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.remove_table(name).await
            },
            _ => panic!("Remove table failed, reason: invalid root transaction"),
        }
    }

    /// 异步修复移除表
    pub(crate) async fn repair_remove_table(&self, name: Atom) -> IOResult<()> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.repair_remove_table(name).await
            },
            _ => panic!("Remove table failed, reason: invalid root transaction"),
        }
    }

    /// 在键值对数据库事务的根事务内，异步查询多个表和键的值的结果集，可能会查询到旧值
    pub async fn dirty_query(&self,
                             table_kv_list: Vec<TableKV>) -> Vec<Option<Binary>> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.dirty_query(table_kv_list).await
            },
            _ => panic!("Query by dirty db failed, reason: invalid root transaction"),
        }
    }

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

    /// 在键值对数据库事务的根事务内，异步插入或更新指定多个表和键的值，插入或更新可能会被覆蓋
    pub async fn dirty_upsert(&self,
                              table_kv_list: Vec<TableKV>) -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.dirty_upsert(table_kv_list).await
            },
            _ => panic!("Upsert db failed, reason: invalid root transaction"),
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

    /// 在键值对数据库事务的根事务内，异步删除指定多个表和键的值，并返回删除值的结果集，删除可能会被覆蓋
    pub async fn dirty_delete(&self,
                        table_kv_list: Vec<TableKV>)
                        -> Result<Vec<Option<Binary>>, KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.dirty_delete(table_kv_list).await
            },
            _ => panic!("Delete db failed, reason: invalid root transaction"),
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
    pub async fn commit_modified(&self,
                                 prepare_output: Vec<u8>) -> Result<(), KVTableTrError> {
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

    /// 在键值对数据库事务的根事务内，异步预提交本次事务对键值对数据库的所有修复修改，不返回预提交的输出
    async fn prepare_repair(&self,
                            transaction_uid: Guid)
                           -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.prepare_repair(transaction_uid).await
            },
            _ => panic!("Repair prepare modified db failed, reason: invalid root transaction"),
        }
    }

    /// 键值对数据库事务的根事务内，异步提交本次事务对键值对数据库的所有修复修改
    async fn commit_repair(&self,
                           transaction_uid: Guid,
                           commit_uid: Guid,
                           prepare_output: Vec<u8>)
        -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.commit_repair(transaction_uid,
                                 commit_uid,
                                 prepare_output).await
            },
            _ => panic!("Repair commit modified db failed, reason: invalid root transaction"),
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

    fn require_persistence(&self) {
        self.0.persistence.store(true, Ordering::Relaxed);
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
    // 注意表事务是否持久化，表示事务是否允许持久化，允许事务持久化表示这个事务的所有写操作会被写入提交日志
    fn table_transaction(&self,
                         name: Atom,
                         table: &KVDBTable<C, Log>,
                         is_persistent: bool,
                         childes_map: &mut XHashMap<Atom, KVDBTransaction<C, Log>>)
                         -> KVDBTransaction<C, Log> {
        match table {
            KVDBTable::MetaTab(tab) => {
                //创建元信息表的表事务，并作为子事务注册到根事务上
                let tr = tab.transaction(self.get_source(),
                                         self.is_writable(),
                                         is_persistent,
                                         self.get_prepare_timeout(),
                                         self.get_commit_timeout());
                let table_tr = KVDBTransaction::MetaTabTr(tr);

                //注册到键值对数据库的根事务
                childes_map.insert(name, table_tr.clone());
                self.0.childs.lock().join(table_tr.clone());

                table_tr
            },
            KVDBTable::MemOrdTab(tab) => {
                //创建有序内存表的表事务，并作为子事务注册到根事务上
                let tr = tab.transaction(self.get_source(),
                                         self.is_writable(),
                                         is_persistent,
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
                                         is_persistent,
                                         self.get_prepare_timeout(),
                                         self.get_commit_timeout());
                let table_tr = KVDBTransaction::LogOrdTabTr(tr);

                //注册到键值对数据库的根事务
                childes_map.insert(name, table_tr.clone());
                self.0.childs.lock().join(table_tr.clone());

                table_tr
            },
            KVDBTable::LogWTab(tab) => {
                let tr = tab.transaction(self.get_source(),
                                         self.is_writable(),
                                         is_persistent,
                                         self.get_prepare_timeout(),
                                         self.get_commit_timeout());
                let table_tr = KVDBTransaction::LogWTabTr(tr);

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
    /// 异步获取表的元信息
    #[inline]
    async fn table_meta(&self, table: Atom) -> Option<KVTableMeta> {
        let meta_table = Atom::from(DEFAULT_DB_TABLES_META_DIR);
        let result = self.query(vec![TableKV::new(meta_table.clone(),
                                                  table_to_binary(&table),
                                                  None)]).await;
        if let Some(binary) = &result[0] {
            //指定名称的表，已注册元信息
            Some(KVTableMeta::from(binary.clone()))
        } else {
            None
        }
    }

    /// 异步创建表，表名可以是用文件分隔符分隔的路径，但必须是相对路径，且不允许使用".."
    #[inline]
    async fn create_table_with_options(&self,
                                       name: Atom,
                                       meta: KVTableMeta,
                                       options: CreateTableOptions) -> IOResult<()> {
        //检查待创建的指定名称的表是否存在
        let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
        let mut tables = self.0.db_mgr.0.tables.write().await;

        self.require_persistence(); //创建表的操作，一定会创建元信息表事务，而元信息表事务是需要持久化的事务，则根事务也设置为需要持久化
        if tables.contains_key(&name) {
            //指定名称的表已存在
            if let Some(meta_table) = tables.get(&meta_table_name) {
                //元信息表存在，则获取元信息表事务，并查询指定表的元信息
                let mut childes_map = self.0.childs_map.lock();
                let meta_table_tr = if let Some(table_tr) = childes_map.get(&meta_table_name) {
                    //元信息表的子事务存在
                    table_tr.clone()
                } else {
                    //元信息表的子事务不存在，则创建元信息表的事务，因为可能只是查询操作，所以初始化指定表的子事务为非持久化事务
                    self.table_transaction(meta_table_name.clone(), meta_table, false, &mut *childes_map)
                };

                if let KVDBTransaction::MetaTabTr(tr) = &meta_table_tr {
                    if let Some(value) = tr.query(table_to_binary(&name)).await {
                        //指定名称的表的元信息存在
                        let table_meta = KVTableMeta::from(value);
                        if table_meta == meta {
                            //待创建表的名称与已存在的表相同，且元信息相同，则立即返回创建成功
                            return Ok(());
                        } else {
                            //待创建表的名称与已存在的表相同，但元信息不同
                            if table_meta.is_persistence() {
                                match tables.get(&name) {
                                    Some(KVDBTable::LogOrdTab(tab)) => {
                                        if tab.len() > 0 {
                                            //已存在的同名表是持久化表，且元信息不同，且表中有记录，则表名冲突
                                            return Err(Error::new(ErrorKind::AlreadyExists,
                                                                  format!("Create table failed, name: {:?}, meta: {:?}, reason: name conflict", name, meta)));
                                        }
                                    },
                                    Some(KVDBTable::LogWTab(tab)) => {
                                        if tab.len() > 0 {
                                            //已存在的同名表是持久化表，且元信息不同，且表中有记录，则表名冲突
                                            return Err(Error::new(ErrorKind::AlreadyExists,
                                                                  format!("Create table failed, name: {:?}, meta: {:?}, reason: name conflict", name, meta)));
                                        }
                                    },
                                    _ => (),
                                }
                            }
                        }
                    } else {
                        //指定名称的表的元信息不存在，则立即返回错误原因
                        return Err(Error::new(ErrorKind::AlreadyExists,
                                              format!("Create table failed, name: {:?}, meta: {:?}, reason: name conflict and table meta not exist", name, meta)));
                    }
                } else {
                    //不是元信息表事务，则立即返回错误原因
                    return Err(Error::new(ErrorKind::AlreadyExists,
                                          format!("Create table failed, name: {:?}, meta: {:?}, reason: invalid meta table transaction", name, meta)));
                }
            }
        }

        //待创建的指定名称的表不存在，则创建指定名称的表，并将表的元信息注册到元信息表
        match meta.table_type {
            KVDBTableType::MemOrdTab => {
                //创建一个有序内存表
                let table = MemoryOrderedTable::new(name.clone(),
                                                    meta.persistence);

                //注册创建的有序内存表
                tables.insert(name.clone(), KVDBTable::MemOrdTab(table));
            },
            KVDBTableType::LogOrdTab => {
                //创建一个有序日志表
                let table_path = self.0.db_mgr.0.tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                if let CreateTableOptions::LogOrdTab(log_file_limit, block_limit, load_buf_len) = options.clone() {
                    //有序日志表的选项
                    let table =
                        LogOrderedTable::new(self.0.db_mgr.0.rt.clone(),
                                             table_path,
                                             name.clone(),
                                             log_file_limit,
                                             block_limit,
                                             None,
                                             load_buf_len as u64,
                                             true,
                                             16 * 1024 * 1024,
                                             60 * 1000).await;

                    //注册创建的有序日志表
                    tables.insert(name.clone(), KVDBTable::LogOrdTab(table));
                } else {
                    //没有有序日志表的选项，则立即返回错误原因
                    return Err(Error::new(ErrorKind::Other,
                                          format!("Create table failed, name: {:?}, meta: {:?}, options: {:?}, reason: invalid options",
                                                  name,
                                                  meta,
                                                  options)));
                }
            },
            KVDBTableType::LogWTab => {
                //创建一个只写日志表
                let table_path = self.0.db_mgr.0.tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                let table =
                    LogWriteTable::new(self.0.db_mgr.0.rt.clone(),
                                         table_path,
                                         name.clone(),
                                         512 * 1024 * 1024,
                                         2 * 1024 * 1024,
                                         None,
                                         2 * 1024 * 1024,
                                         true,
                                         16 * 1024 * 1024,
                                         60 * 1000).await;

                //注册创建的只写日志表
                tables.insert(name.clone(), KVDBTable::LogWTab(table));
            },
        }

        //注册表的元信息
        if let Some(meta_table) = tables.get(&meta_table_name) {
            let mut childes_map = self.0.childs_map.lock();
            let meta_table_tr = if let Some(table_tr) = childes_map.get(&meta_table_name) {
                //元信息表的子事务存在，则设置子事务为需要持久化
                table_tr.require_persistence();
                table_tr.clone()
            } else {
                //元信息表的子事务不存在，则创建元信息表的事务，因为需要创建表，所以初始化元信息表的子事务为持久化事务
                self.table_transaction(meta_table_name, meta_table, true, &mut *childes_map)
            };

            if let KVDBTransaction::MetaTabTr(tr) = &meta_table_tr {
                if let Err(e) = tr.upsert(table_to_binary(&name),
                                               Binary::from(meta.clone())).await {
                    //写入表的元信息失败，则立即返回错误原因
                    return Err(Error::new(ErrorKind::Other,
                                          format!("Create table failed, name: {:?}, meta: {:?}, reason: {:?}", name, meta, e)));
                }
            } else {
                //不是元信息表事务，则立即返回错误原因
                return Err(Error::new(ErrorKind::Other,
                                      format!("Create table failed, name: {:?}, meta: {:?}, reason: invalid meta table transaction", name, meta)));
            }
        }

        Ok(())
    }

    /// 通过默认参数异步创建表，表名可以是用文件分隔符分隔的路径，但必须是相对路径，且不允许使用".."
    #[inline]
    async fn create_table(&self,
                          name: Atom,
                          meta: KVTableMeta) -> IOResult<()> {
        self.create_table_with_options(name,
                                       meta,
                                       CreateTableOptions::LogOrdTab(512 * 1024 * 1024,
                                                                     2 * 1024 * 1024,
                                                                     2 * 1024 * 1024)).await
    }

    /// 异步修复创建表，表名可以是用文件分隔符分隔的路径，但必须是相对路径，且不允许使用".."
    #[inline]
    async fn repair_create_table(&self,
                                 name: Atom,
                                 meta: KVTableMeta) -> IOResult<()> {
        //检查待创建的指定名称的表是否存在
        let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
        let mut tables = self.0.db_mgr.0.tables.write().await;

        self.0.persistence.store(true, Ordering::Relaxed); //创建表的操作，一定会创建元信息表事务，而元信息表事务是需要持久化的事务，则根事务也设置为需要持久化

        //待创建的指定名称的表不存在，则创建指定名称的表，并将表的元信息注册到元信息表
        match meta.table_type {
            KVDBTableType::MemOrdTab => {
                //创建一个有序内存表
                let table = MemoryOrderedTable::new(name.clone(),
                                                    meta.persistence);

                //注册创建的有序内存表
                tables.insert(name.clone(), KVDBTable::MemOrdTab(table));
            },
            KVDBTableType::LogOrdTab => {
                //创建一个有序日志表
                let table_path = self.0.db_mgr.0.tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                let table =
                    LogOrderedTable::new(self.0.db_mgr.0.rt.clone(),
                                         table_path,
                                         name.clone(),
                                         512 * 1024 * 1024,
                                         2 * 1024 * 1024,
                                         None,
                                         2 * 1024 * 1024,
                                         true,
                                         16 * 1024 * 1024,
                                         60 * 1000).await;

                //注册创建的有序日志表
                tables.insert(name.clone(), KVDBTable::LogOrdTab(table));
            },
            KVDBTableType::LogWTab => {
                //创建一个只写日志表
                let table_path = self.0.db_mgr.0.tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                let table =
                    LogWriteTable::new(self.0.db_mgr.0.rt.clone(),
                                       table_path,
                                       name.clone(),
                                       512 * 1024 * 1024,
                                       2 * 1024 * 1024,
                                       None,
                                       2 * 1024 * 1024,
                                       true,
                                       16 * 1024 * 1024,
                                       60 * 1000).await;

                //注册创建的只写日志表
                tables.insert(name.clone(), KVDBTable::LogWTab(table));
            },
        }

        //注册表的元信息
        if let Some(meta_table) = tables.get(&meta_table_name) {
            let mut childes_map = self.0.childs_map.lock();
            let meta_table_tr = if let Some(table_tr) = childes_map.get(&meta_table_name) {
                //元信息表的子事务存在，则设置子事务为需要持久化
                table_tr.require_persistence();
                table_tr.clone()
            } else {
                //元信息表的子事务不存在，则创建元信息表的事务，因为需要创建表，所以初始化元信息表的子事务为持久化事务
                self.table_transaction(meta_table_name, meta_table, true, &mut *childes_map)
            };

            if let KVDBTransaction::MetaTabTr(tr) = &meta_table_tr {
                if let Err(e) = tr.upsert(table_to_binary(&name),
                                          Binary::from(meta.clone())).await {
                    //写入表的元信息失败，则立即返回错误原因
                    return Err(Error::new(ErrorKind::Other,
                                          format!("Create table failed, name: {:?}, meta: {:?}, reason: {:?}", name, meta, e)));
                }
            } else {
                //不是元信息表事务，则立即返回错误原因
                return Err(Error::new(ErrorKind::Other,
                                      format!("Create table failed, name: {:?}, meta: {:?}, reason: invalid meta table transaction", name, meta)));
            }
        }

        Ok(())
    }

    /// 异步移除表
    #[inline]
    async fn remove_table(&self, table: Atom) -> IOResult<()> {
        let mut tables = self.0.db_mgr.0.tables.write().await;

        //移除表
        let _ = tables.remove(&table);

        //删除表的元信息
        let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
        if let Some(meta_table) = tables.get(&meta_table_name) {
            //元信息表存在，则获取元信息表事务，并查询指定表的元信息
            let mut childes_map = self.0.childs_map.lock();
            let meta_table_tr = if let Some(table_tr) = childes_map.get(&meta_table_name) {
                //元信息表的子事务存在，则设置子事务为需要持久化
                table_tr.require_persistence();
                table_tr.clone()
            } else {
                //元信息表的子事务不存在，则创建元信息表的事务，因为需要移除表，所以初始化元信息表的子事务为持久化事务
                self.table_transaction(meta_table_name, meta_table, true, &mut *childes_map)
            };

            if let KVDBTransaction::MetaTabTr(tr) = &meta_table_tr {
                if let Err(e) = tr.delete(table_to_binary(&table)).await {
                    //删除表的元信息失败，则立即返回错误原因
                    return Err(Error::new(ErrorKind::Other,
                                          format!("Remove table failed, name: {:?}, reason: {:?}", table, e)));
                }
            } else {
                //不是元信息表事务，则立即返回错误原因
                return Err(Error::new(ErrorKind::Other,
                                      format!("Remove table failed, name: {:?}, reason: invalid meta table transaction", table)));
            }
        }

        Ok(())
    }

    /// 异步修复移除表
    #[inline]
    async fn repair_remove_table(&self, table: Atom) -> IOResult<()> {
        self.remove_table(table).await
    }

    /// 异步查询多个表和键的值的结果集，可能会查询到旧值
    #[inline]
    async fn dirty_query(&self,
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
                    //指定名称的表的子事务不存在，则创建指定表的事务，因为是查询操作，所以初始化指定表的子事务为非持久化事务
                    self.table_transaction(table_kv.table, table, false, &mut *childes_map)
                };

                match &table_tr {
                    KVDBTransaction::RootTr(_tr) => {
                        //忽略键值对数据库的根事务
                        ()
                    },
                    KVDBTransaction::MetaTabTr(tr) => {
                        //查询元信息表的指定关键字的值
                        let value = tr.dirty_query(table_kv.key).await;
                        result.push(value);
                    },
                    KVDBTransaction::MemOrdTabTr(tr) => {
                        //查询有序内存表的指定关键字的值
                        let value = tr.dirty_query(table_kv.key).await;
                        result.push(value);
                    },
                    KVDBTransaction::LogOrdTabTr(tr) => {
                        //查询有序日志表的指定关键字的值
                        let value = tr.dirty_query(table_kv.key).await;
                        result.push(value);
                    },
                    KVDBTransaction::LogWTabTr(tr) => {
                        //查询只写日志表的指定关键字的值
                        let value = tr.dirty_query(table_kv.key).await;
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

    /// 异步查询多个表和键的值的结果集
    #[inline]
    async fn query(&self,
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
                    //指定名称的表的子事务不存在，则创建指定表的事务，因为是查询操作，所以初始化指定表的子事务为非持久化事务
                    self.table_transaction(table_kv.table, table, false, &mut *childes_map)
                };

                match &table_tr {
                    KVDBTransaction::RootTr(_tr) => {
                        //忽略键值对数据库的根事务
                        ()
                    },
                    KVDBTransaction::MetaTabTr(tr) => {
                        //查询元信息表的指定关键字的值
                        let value = tr.query(table_kv.key).await;
                        result.push(value);
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
                    KVDBTransaction::LogWTabTr(tr) => {
                        //查询只写日志表的指定关键字的值
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

    /// 异步插入或更新指定多个表和键的值，插入或更新可能会被覆蓋
    #[inline]
    async fn dirty_upsert(&self,
                          table_kv_list: Vec<TableKV>) -> Result<(), KVTableTrError> {
        for table_kv in table_kv_list {
            if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_kv.table) {
                //指定名称的表存在，则获取表事务，并开始插入或更新表的指定关键字的值
                let mut childes_map = self.0.childs_map.lock();
                let table_tr = if let Some(table_tr) = childes_map.get(&table_kv.table) {
                    //指定名称的表的子事务存在
                    if table.is_persistent() {
                        //指定表需要持久化，且因为插入或更新操作，所以设置子事务为需要持久化
                        table_tr.require_persistence();
                    }
                    table_tr.clone()
                } else {
                    //指定名称的表的子事务不存在，则创建指定表的事务
                    if table.is_persistent() {
                        //指定表需要持久化，且因为插入或更新操作，所以初始化指定表的子事务为持久化事务
                        self.table_transaction(table_kv.table, table, true, &mut *childes_map)
                    } else {
                        //指定表不需要持久化，所以即使插入或更新操作，也初始化指定表的子事务为非持久化事务
                        self.table_transaction(table_kv.table, table, false, &mut *childes_map)
                    }
                };

                if table_tr.is_require_persistence() {
                    //如果任意写操作对应的子事务需要持久化，则根事务也需要持久化
                    self.0.persistence.store(true, Ordering::Relaxed);
                }

                match &table_tr {
                    KVDBTransaction::RootTr(_tr) => {
                        //忽略键值对数据库的根事务
                        ()
                    },
                    KVDBTransaction::MetaTabTr(tr) => {
                        //插入或更新元信息表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.dirty_upsert(table_kv.key, value).await {
                                //插入或更新元信息表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                    KVDBTransaction::MemOrdTabTr(tr) => {
                        //插入或更新有序内存表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.dirty_upsert(table_kv.key, value).await {
                                //插入或更新有序内存表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                    KVDBTransaction::LogOrdTabTr(tr) => {
                        //插入或更新有序日志表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.dirty_upsert(table_kv.key, value).await {
                                //插入或更新有序日志表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                    KVDBTransaction::LogWTabTr(tr) => {
                        //插入或更新只写日志表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.dirty_upsert(table_kv.key, value).await {
                                //插入或更新只写日志表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                }
            }
        }

        Ok(())
    }

    /// 异步插入或更新指定多个表和键的值
    #[inline]
    async fn upsert(&self,
                    table_kv_list: Vec<TableKV>) -> Result<(), KVTableTrError> {
        for table_kv in table_kv_list {
            if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_kv.table) {
                //指定名称的表存在，则获取表事务，并开始插入或更新表的指定关键字的值
                let mut childes_map = self.0.childs_map.lock();
                let table_tr = if let Some(table_tr) = childes_map.get(&table_kv.table) {
                    //指定名称的表的子事务存在
                    if table.is_persistent() {
                        //指定表需要持久化，且因为插入或更新操作，所以设置子事务为需要持久化
                        table_tr.require_persistence();
                    }
                    table_tr.clone()
                } else {
                    //指定名称的表的子事务不存在，则创建指定表的事务
                    if table.is_persistent() {
                        //指定表需要持久化，且因为插入或更新操作，所以初始化指定表的子事务为持久化事务
                        self.table_transaction(table_kv.table, table, true, &mut *childes_map)
                    } else {
                        //指定表不需要持久化，所以即使插入或更新操作，也初始化指定表的子事务为非持久化事务
                        self.table_transaction(table_kv.table, table, false, &mut *childes_map)
                    }
                };

                if table_tr.is_require_persistence() {
                    //如果任意写操作对应的子事务需要持久化，则根事务也需要持久化
                    self.0.persistence.store(true, Ordering::Relaxed);
                }

                match &table_tr {
                    KVDBTransaction::RootTr(_tr) => {
                        //忽略键值对数据库的根事务
                        ()
                    },
                    KVDBTransaction::MetaTabTr(tr) => {
                        //插入或更新元信息表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.upsert(table_kv.key, value).await {
                                //插入或更新元信息表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
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
                    KVDBTransaction::LogWTabTr(tr) => {
                        //插入或更新只写日志表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.upsert(table_kv.key, value).await {
                                //插入或更新只写日志表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            }
                        }
                    },
                }
            } else {
                //指定名称的表不存在
                error!("Upsert table failed, table: {:?}, reason: table not exist",
                    &table_kv.table.as_str());
                return Err(KVTableTrError::new_transaction_error(ErrorLevel::Fatal,
                                                                 format!("Upsert table failed, table: {:?}, reason: table not exist",
                                                                     &table_kv.table.as_str())));
            }
        }

        Ok(())
    }

    /// 异步删除指定多个表和键的值，并返回删除值的结果集，删除可能会被覆蓋
    #[inline]
    async fn dirty_delete(&self,
                          table_kv_list: Vec<TableKV>) -> Result<Vec<Option<Binary>>, KVTableTrError> {
        let mut result = Vec::new();

        for table_kv in table_kv_list {
            if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_kv.table) {
                //指定名称的表存在，则获取表事务，并开始删除表的指定关键字的值
                let mut childes_map = self.0.childs_map.lock();
                let table_tr = if let Some(table_tr) = childes_map.get(&table_kv.table) {
                    //指定名称的表的子事务存在
                    if table.is_persistent() {
                        //指定表需要持久化，且因为插入或更新操作，所以设置子事务为需要持久化
                        table_tr.require_persistence();
                    }
                    table_tr.clone()
                } else {
                    //指定名称的表的子事务不存在，则创建指定表的事务
                    if table.is_persistent() {
                        //指定表需要持久化，且因为插入或更新操作，所以初始化指定表的子事务为持久化事务
                        self.table_transaction(table_kv.table, table, true, &mut *childes_map)
                    } else {
                        //指定表不需要持久化，所以即使插入或更新操作，也初始化指定表的子事务为非持久化事务
                        self.table_transaction(table_kv.table, table, false, &mut *childes_map)
                    }
                };

                if table_tr.is_require_persistence() {
                    //如果任意写操作对应的子事务需要持久化，则根事务也需要持久化
                    self.0.persistence.store(true, Ordering::Relaxed);
                }

                match &table_tr {
                    KVDBTransaction::RootTr(_tr) => {
                        //忽略键值对数据库的根事务
                        ()
                    },
                    KVDBTransaction::MetaTabTr(tr) => {
                        //删除元信息表的指定关键字的值
                        match tr.dirty_delete(table_kv.key).await {
                            Err(e) => {
                                //删除元信息表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除元信息表的指定关键字的值成功
                                result.push(value);
                            },
                        }
                    },
                    KVDBTransaction::MemOrdTabTr(tr) => {
                        //删除有序内存表的指定关键字的值
                        match tr.dirty_delete(table_kv.key).await {
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
                        match tr.dirty_delete(table_kv.key).await {
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
                    KVDBTransaction::LogWTabTr(tr) => {
                        //删除只写日志表的指定关键字的值
                        match tr.dirty_delete(table_kv.key).await {
                            Err(e) => {
                                //删除只写日志表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除只写日志表的指定关键字的值成功
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

    /// 异步删除指定多个表和键的值，并返回删除值的结果集
    #[inline]
    async fn delete(&self,
                    table_kv_list: Vec<TableKV>) -> Result<Vec<Option<Binary>>, KVTableTrError> {
        let mut result = Vec::new();

        for table_kv in table_kv_list {
            if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_kv.table) {
                //指定名称的表存在，则获取表事务，并开始删除表的指定关键字的值
                let mut childes_map = self.0.childs_map.lock();
                let table_tr = if let Some(table_tr) = childes_map.get(&table_kv.table) {
                    //指定名称的表的子事务存在
                    if table.is_persistent() {
                        //指定表需要持久化，且因为插入或更新操作，所以设置子事务为需要持久化
                        table_tr.require_persistence();
                    }
                    table_tr.clone()
                } else {
                    //指定名称的表的子事务不存在，则创建指定表的事务
                    if table.is_persistent() {
                        //指定表需要持久化，且因为插入或更新操作，所以初始化指定表的子事务为持久化事务
                        self.table_transaction(table_kv.table, table, true, &mut *childes_map)
                    } else {
                        //指定表不需要持久化，所以即使插入或更新操作，也初始化指定表的子事务为非持久化事务
                        self.table_transaction(table_kv.table, table, false, &mut *childes_map)
                    }
                };

                if table_tr.is_require_persistence() {
                    //如果任意写操作对应的子事务需要持久化，则根事务也需要持久化
                    self.0.persistence.store(true, Ordering::Relaxed);
                }

                match &table_tr {
                    KVDBTransaction::RootTr(_tr) => {
                        //忽略键值对数据库的根事务
                        ()
                    },
                    KVDBTransaction::MetaTabTr(tr) => {
                        //删除元信息表的指定关键字的值
                        match tr.delete(table_kv.key).await {
                            Err(e) => {
                                //删除元信息表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除元信息表的指定关键字的值成功
                                result.push(value);
                            },
                        }
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
                    KVDBTransaction::LogWTabTr(tr) => {
                        //删除只写日志表的指定关键字的值
                        match tr.delete(table_kv.key).await {
                            Err(e) => {
                                //删除只写日志表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除只写日志表的指定关键字的值成功
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
    async fn keys<'a>(&self,
                      table_name: Atom,
                      key: Option<Binary>,
                      descending: bool) -> Option<BoxStream<'a, Binary>> {
        if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_name) {
            //指定名称的表存在，则获取表事务，并开始获取关键字的异步流
            let mut childes_map = self.0.childs_map.lock();
            let table_tr = if let Some(table_tr) = childes_map.get(&table_name) {
                //指定名称的表的子事务存在
                table_tr.clone()
            } else {
                //指定名称的表的子事务不存在，则创建指定表的事务，因为是查询操作，所以初始化指定表的子事务为非持久化事务
                self.table_transaction(table_name, table, false, &mut *childes_map)
            };

            match &table_tr {
                KVDBTransaction::RootTr(_tr) => {
                    //忽略键值对数据库的根事务
                    None
                },
                KVDBTransaction::MetaTabTr(tr) => {
                    //获取元信息表的关键字的异步流
                    Some(tr.keys(key, descending))
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    //获取有序内存表的关键字的异步流
                    Some(tr.keys(key, descending))
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    //获取有序日志表的关键字的异步流
                    Some(tr.keys(key, descending))
                },
                KVDBTransaction::LogWTabTr(tr) => {
                    //获取只写日志表的关键字的异步流
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
    async fn values<'a>(&self,
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
                //指定名称的表的子事务不存在，则创建指定表的事务，因为是查询操作，所以初始化指定表的子事务为非持久化事务
                self.table_transaction(table_name, table, false, &mut *childes_map)
            };

            match &table_tr {
                KVDBTransaction::RootTr(_tr) => {
                    //忽略键值对数据库的根事务
                    None
                },
                KVDBTransaction::MetaTabTr(tr) => {
                    //获取元信息表的键值对异步流
                    Some(tr.values(key, descending))
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    //获取有序内存表的键值对异步流
                    Some(tr.values(key, descending))
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    //获取有序日志表的键值对异步流
                    Some(tr.values(key, descending))
                },
                KVDBTransaction::LogWTabTr(tr) => {
                    //获取只写日志表的键值对异步流
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
    async fn lock_key(&self,
                      table_name: Atom,
                      key: Binary) -> Result<(), KVTableTrError> {
        if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_name) {
            //指定名称的表存在，则获取表事务，并开始锁住指定表的指定关键字
            let mut childes_map = self.0.childs_map.lock();
            let table_tr = if let Some(table_tr) = childes_map.get(&table_name) {
                //指定名称的表的子事务存在
                table_tr.clone()
            } else {
                //指定名称的表的子事务不存在，则创建指定表的事务，因为是锁定操作，所以初始化指定表的子事务为非持久化事务
                self.table_transaction(table_name, table, false, &mut *childes_map)
            };

            match &table_tr {
                KVDBTransaction::RootTr(_tr) => {
                    //忽略键值对数据库的根事务
                    Ok(())
                },
                KVDBTransaction::MetaTabTr(tr) => {
                    //锁住元信息表的指定关键字
                    tr.lock_key(key).await
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    //锁住有序内存表的指定关键字
                    tr.lock_key(key).await
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    //锁住有序日志表的指定关键字
                    tr.lock_key(key).await
                },
                KVDBTransaction::LogWTabTr(tr) => {
                    //锁住只写日志表的指定关键字
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
    async fn unlock_key(&self,
                        table_name: Atom,
                        key: Binary) -> Result<(), KVTableTrError> {
        if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_name) {
            //指定名称的表存在，则获取表事务，并开始解锁指定表的指定关键字
            let mut childes_map = self.0.childs_map.lock();
            let table_tr = if let Some(table_tr) = childes_map.get(&table_name) {
                //指定名称的表的子事务存在
                table_tr.clone()
            } else {
                //指定名称的表的子事务不存在，则创建指定表的事务，因为是解锁操作，所以初始化指定表的子事务为非持久化事务
                self.table_transaction(table_name, table, false, &mut *childes_map)
            };

            match &table_tr {
                KVDBTransaction::RootTr(_tr) => {
                    //忽略键值对数据库的根事务
                    Ok(())
                },
                KVDBTransaction::MetaTabTr(tr) => {
                    //解锁元信息表的指定关键字
                    tr.unlock_key(key).await
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    //解锁有序内存表的指定关键字
                    tr.unlock_key(key).await
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    //解锁有序日志表的指定关键字
                    tr.unlock_key(key).await
                },
                KVDBTransaction::LogWTabTr(tr) => {
                    //解锁只写日志表的指定关键字
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
    async fn prepare_modified(&self) -> Result<Vec<u8>, KVTableTrError> {
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
    async fn commit_modified(&self, prepare_output: Vec<u8>) -> Result<(), KVTableTrError> {
        if self.is_writable()
            && self.is_require_persistence()
            && prepare_output.is_empty() {
            //当前事务是可写且需要持久化的事务，但预提交输出为空，则立即完成本次键值对数据库事务
            //一般只出现在事务中只有创建或删除表操作，且表已创建或已删除
            self
                .0
                .db_mgr
                .0
                .tr_mgr
                .finish(KVDBTransaction::RootTr(self.clone()));
            return Ok(());
        }

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
    async fn rollback_modified(&self) -> Result<(), KVTableTrError> {
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

        //回滚键值对数据库的根事务成功，则完成本次键值对数据库事务
        self
            .0
            .db_mgr
            .0
            .tr_mgr
            .finish(KVDBTransaction::RootTr(self.clone()));
        Ok(())
    }

    /// 异步预提交本次事务对键值对数据库的所有修复修改，不返回预提交的输出
    async fn prepare_repair(&self,
                            transaction_uid: Guid)
                            -> Result<(), KVTableTrError> {
        let mut childs = self.to_children();
        while let Some(child) = childs.next() {
            match &child {
                KVDBTransaction::MetaTabTr(tr) => {
                    tr.prepare_repair(transaction_uid.clone());
                },
                KVDBTransaction::MemOrdTabTr(tr) => {
                    tr.prepare_repair(transaction_uid.clone());
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    tr.prepare_repair(transaction_uid.clone());
                },
                KVDBTransaction::LogWTabTr(tr) => {
                    tr.prepare_repair(transaction_uid.clone());
                },
                KVDBTransaction::RootTr(_) => {
                    //忽略根事务，并继续执行下一个子事务的预提交修复
                    continue;
                }
            }
        }

        Ok(())
    }

    /// 异步提交本次事务对键值对数据库的所有修复修改
    #[inline]
    async fn commit_repair(&self,
                           transaction_uid: Guid,
                           commit_uid: Guid,
                           prepare_output: Vec<u8>) -> Result<(), KVTableTrError> {
        //为本次事务的异步提交确认，创建提交确认回调
        let commit_confirm = KVDBCommitConfirm::new(self.0.db_mgr.0.rt.clone(),
                                                    self.0.db_mgr.0.tr_mgr.commit_logger(),
                                                    transaction_uid.clone(),
                                                    Some(commit_uid.clone()),
                                                    self.persistent_children_len());

        //重播提交键值对数据库的根事务
        match self
            .0
            .db_mgr
            .0
            .tr_mgr
            .replay_commit(KVDBTransaction::RootTr(self.clone()),
                           transaction_uid,
                           commit_uid,
                           prepare_output,
                           commit_confirm)
            .await {
            Err(e) => Err(e),
            Ok(_) => {
                //重播提交键值对数据库的根事务成功，则完成本次键值对数据库重播事务
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
    MetaTab(MetaTable<C, Log>),             //元信息表
    MemOrdTab(MemoryOrderedTable<C, Log>),  //有序内存表
    LogOrdTab(LogOrderedTable<C, Log>),     //有序日志表
    LogWTab(LogWriteTable<C, Log>),         //只写日志表
}

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for KVDBTable<C, Log> {}
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for KVDBTable<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVDBTable<C, Log> {
    /// 是否可持久化的表
    #[inline]
    pub fn is_persistent(&self) -> bool {
        match self {
            Self::MetaTab(tab) => tab.is_persistent(),
            Self::MemOrdTab(tab) => tab.is_persistent(),
            Self::LogOrdTab(tab) => tab.is_persistent(),
            Self::LogWTab(tab) => tab.is_persistent(),
        }
    }
}

// 将表名序列化为二进制数据
pub(crate) fn table_to_binary(table_name: &Atom) -> Binary {
    let mut buffer = WriteBuffer::new();
    table_name.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

// 将二进制数据反序列化为表名
pub(crate) fn binary_to_table(bin: &Binary) -> Result<Atom, ReadBonErr> {
    let mut buffer = ReadBuffer::new(bin, 0);
    Atom::decode(&mut buffer)
}