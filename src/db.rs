use std::mem::swap;
use std::time::Instant;
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::collections::{VecDeque, HashMap, BTreeMap};
use std::io::{Error, Result as IOResult, ErrorKind};
use std::sync::{Arc,
                OnceLock,
                atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}};

use futures::{future::{FutureExt, BoxFuture, try_join}, stream::BoxStream, StreamExt};
use crossbeam_channel::bounded;
use async_lock::{Mutex, RwLock};
use async_channel::{Sender, Receiver, bounded as async_bounded, unbounded};
use dashmap::DashMap;
use lazy_static::lazy_static;
use bytes::BufMut;
use log::{info, error};
#[cfg(target_os = "linux")]
use libc::malloc_trim;
#[cfg(feature = "trace")]
use opentelemetry::{global,
                    metrics::Meter,
                    KeyValue};
#[cfg(feature = "trace")]
use pi_logger;
use pi_atom::Atom;
use pi_bon::{WriteBuffer, ReadBuffer, Encode, Decode, ReadBonErr};
use pi_guid::Guid;
use pi_async_rt::{lock::spin_lock::SpinLock,
                  rt::{AsyncRuntime, AsyncValue,
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
                                       LogWTabTr},
                     b_tree_ord_table::{DEFAULT_CACHE_SIZE, BtreeOrderedTable,
                                        BtreeOrdTabTr}},
            utils::{CreateTableOptions, KVDBEvent}};

///
/// 默认的数据库表元信息目录名
///
pub(crate) const DEFAULT_DB_TABLES_META_DIR: &str = ".tables_meta";

///
/// 默认的数据库表所在目录名
///
const DEFAULT_DB_TABLES_DIR: &str = ".tables";

///
/// quick repair 按文件批次流水线处理时的默认深度。
/// 含义是：
/// 1. 一个文件正在 replay/flush；
/// 2. 允许再有一个文件批次处于 producer 侧缓冲/待交付状态。
///
const DEFAULT_QUICK_REPAIR_FILE_PIPELINE_DEPTH: usize = 2;

///
/// quick repair 按文件批次流水线处理时允许的最大深度。
///
const MAX_QUICK_REPAIR_FILE_PIPELINE_DEPTH: usize = 8;

///
/// 开启 quick repair 统计型日志的环境变量。
/// 非 `0`/`false`/空串即视为开启。
///
const QUICK_REPAIR_PROFILE_LOG_ENV: &str = "PI_DB_QUICK_REPAIR_PROFILE_LOG";

///
/// 开启 quick repair 统计型日志后，按记录数输出批次内进度的间隔。
/// `0` 表示关闭批次内进度日志。
///
const QUICK_REPAIR_PROFILE_RECORD_INTERVAL_ENV: &str = "PI_DB_QUICK_REPAIR_PROFILE_RECORD_INTERVAL";

#[inline]
pub(crate) fn quick_repair_profile_log_enabled() -> bool {
    match std::env::var(QUICK_REPAIR_PROFILE_LOG_ENV) {
        Ok(value) => {
            let value = value.trim();
            !value.is_empty() && value != "0" && value.to_ascii_lowercase() != "false"
        },
        Err(_) => false,
    }
}

#[inline]
pub(crate) fn quick_repair_profile_log<S: AsRef<str>>(message: S) {
    if quick_repair_profile_log_enabled() {
        println!("[quick_repair_profile][db] {}", message.as_ref());
    }
}

#[inline]
fn repair_debug_table_names(table_names: &[Atom]) -> String {
    const LIMIT: usize = 8;

    if table_names.is_empty() {
        return "[]".to_string();
    }

    let mut preview = table_names
        .iter()
        .take(LIMIT)
        .map(|name| format!("{:?}", name))
        .collect::<Vec<_>>();
    if table_names.len() > LIMIT {
        preview.push(format!("...(+{} more)", table_names.len() - LIMIT));
    }

    format!("[{}]", preview.join(", "))
}

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

///
/// 启动数据库的源
///
const STARTUP_DB_SOURCE: &str = "Startup db";

///
/// 修复数据库时的源
///
const REPAIR_DB_SOURCE: &str = "Repair db";

///
/// 快速修复数据库时的源。
/// 与旧 try_repair 分开，便于只对 try_quick_repair 打开专用的快速持久化登记路径。
/// 这里故意使用极不自然的内部标记字符串，只用于尽量降低普通事务误撞 quick repair 分支的概率。
/// 注意：这仍然不是绝对隔离机制，后续若要彻底消除误撞风险，应改成内部显式标记而不是 source 字符串判定。
///
pub(crate) const QUICK_REPAIR_DB_SOURCE: &str = "__PiDb__qUiCk_RePaIr__InTeRnAl__DoNoT_uSe__";

// 表缓存大小仪表
#[cfg(feature = "trace")]
static TABLE_CACHE_SIZE_METER: OnceLock<Meter> = OnceLock::new();

// 获取表缓存大小仪表
#[cfg(feature = "trace")]
pub(crate) async fn get_table_cache_size_meter<'a, R>(rt: R) -> &'a Meter
    where R: AsyncRuntime
{
    while !pi_logger::opentelemetry::is_init() {
        //跟踪系统还未初始化，则稍候重试
        rt.timeout(10000).await;
    }

    TABLE_CACHE_SIZE_METER.get_or_init(|| global::meter("table_cache_size"))
}

///
/// 键值对数据库管理器构建器
///
pub struct KVDBManagerBuilder<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    rt:                                 MultiTaskRuntime<()>,   //异步运行时
    tr_mgr:                             Transaction2PcManager<C, Log>, //事务管理器
    db_path:                            PathBuf,                //数据库的表文件所在目录
    tables_meta_path:                   PathBuf,                //数据库的元信息表文件所在目录
    tables_path:                        PathBuf,                //数据库表文件所在目录
    quick_repair_file_pipeline_depth:   usize,                  //quick repair 按文件批次处理时的流水线深度
}

///
/// 数据库启动时的修复模式
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DBStartupRepairMode {
    TryRepair,      //兼容旧修复流程，逐条走普通 upsert/delete，再 prepare/commit repair
    TryQuickRepair, //快速修复流程，只装载持久化表动作，并在 replay 后立即 flush 受影响的持久化表
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
            quick_repair_file_pipeline_depth: DEFAULT_QUICK_REPAIR_FILE_PIPELINE_DEPTH,
        }
    }

    /// 设置 quick repair 按 commit log 文件批次处理时的流水线深度。
    /// 最小值固定为 `2`，最大值固定为 `8`，超出范围时回落到默认值。
    pub fn quick_repair_file_pipeline_depth(mut self, mut depth: usize) -> Self {
        if depth < DEFAULT_QUICK_REPAIR_FILE_PIPELINE_DEPTH || depth > MAX_QUICK_REPAIR_FILE_PIPELINE_DEPTH {
            depth = DEFAULT_QUICK_REPAIR_FILE_PIPELINE_DEPTH;
        }

        self.quick_repair_file_pipeline_depth = depth;
        self
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
    /// 默认使用 `TryQuickRepair`，启动时如果发现未确认的提交日志，会优先走快速修复路径。
    pub async fn startup(self, enable_accelerated_repair: bool) -> IOResult<KVDBManager<C, Log>> {
        self
            .startup_by_repair(enable_accelerated_repair,
                               DBStartupRepairMode::TryQuickRepair)
            .await
    }

    /// 异步按指定修复模式启动键值对数据库，并返回键值对数据库的管理器
    /// 适合在启动修复行为需要与旧流程做结果对比、压测或灰度切换时显式指定。
    pub async fn startup_by_repair(self,
                                   enable_accelerated_repair: bool,
                                   repair_mode: DBStartupRepairMode) -> IOResult<KVDBManager<C, Log>> {
        self
            .startup_with_listener_by_repair::<fn(&KVDBManager<C, Log>, &Transaction2PcManager<C, Log>, &mut Vec<KVDBEvent<Guid>>)>(enable_accelerated_repair,
                                                                                                                                      repair_mode,
                                                                                                                                      None)
            .await
    }

    /// 异步启动指定监听器的键值对数据库，并返回键值对数据库的管理器
    /// 默认使用 `TryQuickRepair`，同时保留数据库事件监听能力。
    pub async fn startup_with_listener<F>(
        self,
        enable_accelerated_repair: bool,
        db_event_listener: Option<F>
    ) -> IOResult<KVDBManager<C, Log>>
    where F: FnMut(&KVDBManager<C, Log>, &Transaction2PcManager<C, Log>, &mut Vec<KVDBEvent<Guid>>) + Send + Sync + 'static
    {
        self
            .startup_with_listener_by_repair(enable_accelerated_repair,
                                             DBStartupRepairMode::TryQuickRepair,
                                             db_event_listener)
            .await
    }

    /// 异步按指定修复模式启动指定监听器的键值对数据库，并返回键值对数据库的管理器
    /// 这是最完整的启动入口，可同时控制修复模式与监听器接入。
    pub async fn startup_with_listener_by_repair<F>(
        self,
        enable_accelerated_repair: bool,
        repair_mode: DBStartupRepairMode,
        db_event_listener: Option<F>
    ) -> IOResult<KVDBManager<C, Log>>
    where F: FnMut(&KVDBManager<C, Log>, &Transaction2PcManager<C, Log>, &mut Vec<KVDBEvent<Guid>>) + Send + Sync + 'static
    {
        let startup_profile_enabled = repair_mode == DBStartupRepairMode::TryQuickRepair
            && quick_repair_profile_log_enabled();
        let startup_begin = Instant::now();
        if startup_profile_enabled {
            quick_repair_profile_log(format!("startup_by_repair begin: repair_mode={:?}, enable_accelerated_repair={}, db_path={:?}",
                                             repair_mode,
                                             enable_accelerated_repair,
                                             self.db_path));
        }

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
        let quick_repair_file_pipeline_depth = self.quick_repair_file_pipeline_depth;
        let tables = Arc::new(RwLock::new(XHashMap::default()));
        let status = AtomicU64::new(DB_INITING_STATUS);
        let (notifier, listener) = if db_event_listener.is_some() {
            let (notifier, listener) = unbounded();
            (Some(notifier), Some(listener))
        } else {
            (None, None)
        };
        let inner = InnerKVDBManager {
            rt,
            tr_mgr,
            db_path,
            tables_meta_path,
            tables_path,
            quick_repair_file_pipeline_depth,
            tables,
            status,
            listener,
            notifier,
        };
        let db_mgr = KVDBManager(Arc::new(inner));

        //加载并注册元信息表
        let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
        let meta_table_load_begin = Instant::now();
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
                           60 * 1000,
                           db_mgr.0.notifier.clone()).await;
        db_mgr.0.tables.write().await.insert(meta_table_name.clone(), KVDBTable::MetaTab(meta_table));
        if startup_profile_enabled {
            quick_repair_profile_log(format!("startup phase meta table loaded: elapsed_ms={}, total_elapsed_ms={}",
                                             meta_table_load_begin.elapsed().as_millis(),
                                             startup_begin.elapsed().as_millis()));
        }

        //根据元信息表的元信息，加载其它表，加载操作使用的事务，不需要预提交和提交
        let table_load_begin = Instant::now();
        let mut tr = db_mgr
            .transaction(Atom::from(STARTUP_DB_SOURCE),
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
        let mut table_metas_buf = Vec::with_capacity(8192);
        let default_log_table_options = CreateTableOptions::LogOrdTab(512 * 1024 * 1024,
                                                                      2 * 1024 * 1024,
                                                                      2 * 1024 * 1024);
        let default_b_tree_table_options = CreateTableOptions::BtreeOrdTab(16 * 1024 * 1024,
                                                                           true);
        let now = Instant::now();
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

            if table_metas_buf.len() < 8192 {
                //填充表元信息，并继续迭代下一个表元信息
                match table_meta.table_type() {
                    KVDBTableType::LogOrdTab => {
                        table_metas_buf.push((table_name, table_meta, Some(default_log_table_options.clone())));
                    },
                    KVDBTableType::BtreeOrdTab => {
                        table_metas_buf.push((table_name, table_meta, Some(default_b_tree_table_options.clone())));
                    },
                    _ => {
                        table_metas_buf.push((table_name, table_meta, None));
                    },
                }
                continue;
            }
            let mut table_metas = Vec::with_capacity(table_metas_buf.len());
            swap(&mut table_metas_buf, &mut table_metas);

            //异步批量加载表
            if let Err(e) = tr.create_multiple_tables(
                table_metas,
                true,
                enable_accelerated_repair
            ).await {
                //加载指定的表失败，则立即返回错误原因
                db_mgr.0.status.store(DB_UNSTARTUP_STATUS, Ordering::SeqCst);
                return Err(Error::new(ErrorKind::Other,
                                      format!("Load table failed, tables_path: {:?}, reason: {:?}",
                                              db_mgr.tables_path(),
                                              e)));
            }
        }
        if table_metas_buf.len() > 0 {
            //异步批量加载剩余的表
            if let Err(e) = tr.create_multiple_tables(
                table_metas_buf,
                true,
                enable_accelerated_repair
            ).await {
                //加载指定的表失败，则立即返回错误原因
                db_mgr.0.status.store(DB_UNSTARTUP_STATUS, Ordering::SeqCst);
                return Err(Error::new(ErrorKind::Other,
                                      format!("Load table failed, tables_path: {:?}, reason: {:?}",
                                              db_mgr.tables_path(),
                                              e)));
            }
        }
        let loaded_tables = db_mgr.table_size().await;
        info!("Load db succeeded, tables: {:?}, time: {:?}",
            loaded_tables,
            now.elapsed());
        if startup_profile_enabled {
            quick_repair_profile_log(format!("startup phase tables loaded: tables={}, elapsed_ms={}, total_elapsed_ms={}",
                                             loaded_tables,
                                             table_load_begin.elapsed().as_millis(),
                                             startup_begin.elapsed().as_millis()));
        }

        //如果有未确认的提交日志，则尝试修复数据库表数据
        let repair_begin = Instant::now();
        info!("Database repair begin, mode: {:?}, tables: {}, accelerated_repair: {}",
              repair_mode,
              loaded_tables,
              enable_accelerated_repair);
        let repair_result = match repair_mode {
            DBStartupRepairMode::TryRepair => {
                db_mgr.try_repair(enable_accelerated_repair).await
            },
            DBStartupRepairMode::TryQuickRepair => {
                db_mgr.try_quick_repair(enable_accelerated_repair).await
            },
        };
        match repair_result {
            Err(e) => {
                //有未确认的提交日志，且尝试修复数据库表数据失败，则立即返回错误原因
                return Err(e);
            },
            Ok((repaired_log_len, repaired_bytes_len)) => {
                //尝试修复数据库表数据成功
                if repaired_log_len > 0 {
                    //未确认的提交日志
                    info!("Repair db succeeded, logs: {}, bytes: {}, time: {:?}",
                        repaired_log_len,
                        repaired_bytes_len,
                        repair_begin.elapsed());
                }
                if startup_profile_enabled {
                    quick_repair_profile_log(format!("startup phase repair finished: repaired_logs={}, repaired_bytes={}, repair_elapsed_ms={}, total_elapsed_ms={}",
                                                     repaired_log_len,
                                                     repaired_bytes_len,
                                                     repair_begin.elapsed().as_millis(),
                                                     startup_begin.elapsed().as_millis()));
                }
                info!("Database repair end, mode: {:?}, repaired_logs: {}, repaired_bytes: {}, elapsed_ms: {}",
                      repair_mode,
                      repaired_log_len,
                      repaired_bytes_len,
                      repair_begin.elapsed().as_millis());
            }
        }

        if let Some(mut handle) = db_event_listener {
            //指定了数据库事件监听器
            let db_mgr_copy = db_mgr.clone();
            let listener = db_mgr
                .0
                .listener
                .as_ref()
                .unwrap()
                .clone();


            let _ = db_mgr.0.rt.spawn(async move {
                let mut events = Vec::with_capacity(3072);
                loop {
                    let mut try_count = 5usize;
                    while events.len() < 3072 {
                        //未达单次事件处理上限
                        if let Ok(event) = listener.try_recv() {
                            //有事件
                            try_count = 5; //重置重试次数
                            events.push(event);
                        } else {
                            //无事件
                            if try_count > 0 {
                                //未达重试限制，则稍候重试
                                try_count = try_count.checked_sub(1).unwrap_or(0);
                                db_mgr_copy
                                    .0
                                    .rt
                                    .timeout(10)
                                    .await;
                                continue;
                            }

                            //已达重试限制
                            if events.len() > 0 {
                                //有待处理的事件，则立即处理
                                try_count = 5; //重置重试次数
                                handle(&db_mgr_copy, &db_mgr_copy.0.tr_mgr, &mut events);
                                break;
                            } else {
                                //没有待处理的事件，则等待
                                if let Ok(event) = listener.recv().await {
                                    //有事件
                                    try_count = 5; //重置重试次数
                                    events.push(event);
                                }
                            }
                        }
                    }

                    if events.len() > 0 {
                        //有待处理的事件，则立即处理
                        handle(&db_mgr_copy, &db_mgr_copy.0.tr_mgr, &mut events);
                    }
                }
            });
        }

        //启动跟踪系统
        #[cfg(feature = "trace")]
        {
            let rt_copy = db_mgr.0.rt.clone();
            let db_mgr_copy = db_mgr.clone();
            let _ = db_mgr.0.rt.spawn(async move {
                loop_tracing(rt_copy, db_mgr_copy, 15000).await;
            });
        }

        db_mgr.0.status.store(DB_INITED_STATUS, Ordering::SeqCst); //设置数据库状态为已初始化
        info!("Startup db succeeded");
        if startup_profile_enabled {
            quick_repair_profile_log(format!("startup_by_repair finished: repair_mode={:?}, total_elapsed_ms={}",
                                             repair_mode,
                                             startup_begin.elapsed().as_millis()));
        }

        //在Linux下启动完成后清理一次内存
        #[cfg(target_os = "linux")]
        db_mgr.cleanup_buffer_after_collect_table();

        Ok(db_mgr)
    }
}

///
/// 键值对数据库管理器
///
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
> Clone for KVDBManager<C, Log> {
    fn clone(&self) -> Self {
        KVDBManager(self.0.clone())
    }
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
    /// 在整理数据表后清理内存缓冲区
    ///
    #[cfg(target_os = "linux")]
    pub fn cleanup_buffer_after_collect_table(&self) -> bool {
        match unsafe { malloc_trim(0) } {
            0 => false,
            _ => true,
        }
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
            Some(KVDBTable::BtreeOrdTab(table)) => {
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
            Some(KVDBTable::BtreeOrdTab(table)) => {
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
            Some(KVDBTable::BtreeOrdTab(table)) => {
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
            Some(KVDBTable::BtreeOrdTab(table)) => {
                Some(table.len())
            },
        }
    }

    /// 异步获取指定名称的数据表的缓存字节大小，返回空表示指定名称的表不存在
    pub async fn table_cache_size(&self, table_name: &Atom) -> Option<u64> {
        match self.0.tables.read().await.get(table_name) {
            None => None,
            Some(KVDBTable::MetaTab(table)) => {
                Some(table.size())
            },
            Some(KVDBTable::MemOrdTab(table)) => {
                Some(table.size())
            },
            Some(KVDBTable::LogOrdTab(table)) => {
                Some(table.size())
            },
            Some(KVDBTable::LogWTab(table)) => {
                Some(table.size())
            },
            Some(KVDBTable::BtreeOrdTab(table)) => {
                Some(table.size())
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
            Some(KVDBTable::BtreeOrdTab(table)) => {
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
            Some(KVDBTable::BtreeOrdTab(table)) => {
                if let Err(e) = table.collect().await {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            },
        }

        Ok(())
    }

    ///
    /// 异步请求数据库的事务信息报告
    ///
    pub async fn report_transaction_info(&self) -> IOResult<()> {
        if let Some(notifier) = self.0.notifier.as_ref() {
            if let Err(e) = notifier.send(KVDBEvent::ReportTrInfo).await {
                Err(Error::new(ErrorKind::ConnectionAborted,
                               format!("Report transaction info failed, reason: {:?}", e)))
            } else {
                Ok(())
            }
        } else {
            Err(Error::new(ErrorKind::ConnectionAborted,
                           format!("Report transaction info failed, reason: invalid notifier")))
        }
    }

    // 尝试幂等的重播未确认的提交日志，并修复数据库表数据
    // 注意如果在只有单个线程的运行时修复或并发修复，则可能会发生阻塞
    pub(crate) async fn try_repair(&self, enable_accelerated_repair: bool) -> IOResult<(usize, usize)> {
        info!("try_repair begin, accelerated_repair: {}", enable_accelerated_repair);
        //构建重播回调
        let db_mgr = self.clone();

        let tables = Arc::new(Mutex::new(BTreeMap::new()));
        let tables_copy = tables.clone();
        let replay_callback = move |commit_uid: Guid, prepare_output: Vec<u8>| -> IOResult<()> {
            //异步执行重播
            let db_mgr_copy = db_mgr.clone();
            let commit_uid_copy = commit_uid.clone();
            let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
            let (sender, receiver) = bounded(1);

            let tables_clone = tables_copy.clone();
            let boxed = async move {
                let bytes_len = prepare_output.len(); //获取日志缓冲区长度
                let mut offset = 0; //日志缓冲区偏移
                let bytes = prepare_output.as_slice();
                let uid = u128::from_le_bytes(bytes[0..16].try_into().unwrap()); //获取事务唯一id
                let transaciton_uid = Guid(uid);
                offset += 16; //移动缓冲区指针

                if let Some(tr) = db_mgr_copy.transaction(Atom::from(REPAIR_DB_SOURCE),
                                                          true,
                                                          5000,
                                                          5000)
                {
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

                                    if let Err(e) = tr.repair_create_table(
                                        table_name.clone(),
                                        table_meta.clone(),
                                        enable_accelerated_repair,
                                    ).await {
                                        //重播的创建表失败，则立即返回错误原因
                                        let _ = sender.send(Err(Error::new(ErrorKind::Other, format!("Repair tables meta failed, transaction_uid: {:?}, commit_uid: {:?}, table_name: {:?}, table_meta: {:?}, reason: {:?}", transaciton_uid, commit_uid_copy, table_name, table_meta, e))));
                                        return;
                                    }
                                } else {
                                    //无值，则删除表。元信息表里的 key 是序列化后的表名，必须先反序列化。
                                    let table_name = match binary_to_table(&write.key) {
                                        Err(e) => {
                                            panic!("From binary to table name failed, reason: {:?}", e);
                                        },
                                        Ok(table_name) => {
                                            table_name
                                        }
                                    };

                                    if let Err(e) = tr.repair_remove_table(table_name.clone()).await {
                                        //重播的移除表失败，则立即返回错误原因
                                        let _ = sender.send(Err(Error::new(ErrorKind::Other, format!("Repair tables meta failed, transaction_uid: {:?}, commit_uid: {:?}, table_name: {:?}, reason: {:?}", transaciton_uid, commit_uid_copy, table_name, e))));
                                        return;
                                    }
                                }
                            }
                        } else {
                            //未确认的提交日志操作的表是其它表，则执行本次未确认的提交日志中指定表的键值对写操作
                            tables_clone
                                .lock()
                                .await
                                .insert(table.clone(), ());
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
                        let _ = sender.send(Err(Error::new(ErrorKind::Other,
                                                           format!("Repair db failed, transaction_uid: {:?}, commit_uid: {:?}, reason: {:?}",
                                                                   transaciton_uid,
                                                                   commit_uid_copy,
                                                                   e))));
                        return;
                    }

                    //指定本次重播事务的事务唯一id和事务提交唯一id后执行提交修复
                    if let Err(e) = tr
                        .commit_repair(transaciton_uid.clone(),
                                       commit_uid_copy.clone(),
                                       prepare_output).await {
                        //提交重播事务失败，则立即返回错误原因
                        let _ = sender.send(Err(Error::new(ErrorKind::Other,
                                                           format!("Repair db failed, transaction_uid: {:?}, commit_uid: {:?}, reason: {:?}",
                                                                   transaciton_uid,
                                                                   commit_uid_copy,
                                                                   e))));
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
        info!("try_repair replay finished, repaired_logs: {}, repaired_bytes: {}",
              replay_result.0,
              replay_result.1);

        //所有未确认的提交日志已完成重播，则立即返回数据库修复成功
        let _ = self.0.tr_mgr.finish_replay().await?; //通知事务管理器，已完成重播
        info!("try_repair finish_replay returned, repaired_logs: {}, repaired_bytes: {}",
              replay_result.0,
              replay_result.1);

        // for table in tables.lock().await.keys() {
        //     if let Some(KVDBTable::BtreeOrdTab(tab)) = self.get_table(table).await {
        //         //当前表存在且为有序B树表，则立即整理
        //         tab.collect().await;
        //     }
        // }

        return Ok(replay_result);
    }

    /// 尝试按 commit log 文件批次顺序重播未确认的提交日志，并使用快速装载路径修复持久化表。
    ///
    /// 关键约束：
    /// 1. 仍然严格按提交日志顺序逐事务重放，不能跨事务乱序。
    /// 2. 元信息表仍走 repair create/remove table，保证表结构恢复语义不变。
    /// 3. 用户表只快速装载持久化表的 `DirtyWrite` 动作，内存表不参与 quick repair。
    /// 4. 预提交阶段仍沿用各表既有的 `prepare_repair`，提交阶段仍沿用 `commit_repair`。
    /// 5. 每个 commit log 文件批次 replay 完成后立即 flush 本文件触达的持久化表。
    /// 6. 文件级 flush 不等于文件级 confirm；重播事务的提交确认仍统一缓冲到 `finish_replay`。
    /// 7. 生产者只做按文件批次缓冲，真实 replay/flush 由单消费者 async worker 顺序执行。
    ///
    /// 流水线语义：
    /// 1. 文件流水线深度默认是 `2`。
    /// 2. 对应含义是“一个文件正在 replay/flush，同时最多允许 producer 再向前缓冲一个文件批次”。
    /// 3. 当流水线深度已满时，背压只作用在文件批次交接点，不在每条记录上阻塞。
    pub(crate) async fn try_quick_repair(&self, enable_accelerated_repair: bool) -> IOResult<(usize, usize)> {
        let profile = QuickRepairProfileState::from_env();
        let pipeline_depth = self.0.quick_repair_file_pipeline_depth;
        let ahead_limit = pipeline_depth.checked_sub(1).unwrap_or(1);
        info!("try_quick_repair begin, accelerated_repair: {}, pipeline_depth: {}, ahead_limit: {}",
              enable_accelerated_repair,
              pipeline_depth,
              ahead_limit);
        profile.log(format!("start try_quick_repair: pipeline_depth={}, ahead_limit={}",
                            pipeline_depth,
                            ahead_limit));
        self.set_quick_repair_timeout_collect_suspended(true).await;
        profile.log("suspend timeout collect loops for quick repair");

        let result = async {
            let (permit_sender, permit_receiver) = bounded(ahead_limit);
            for _ in 0..ahead_limit {
                let _ = permit_sender.send(());
            }

            let (batch_sender, batch_receiver) = async_bounded(ahead_limit);
            let worker_result = AsyncValue::new();

            let worker_db = self.clone();
            let worker_result_copy = worker_result.clone();
            let worker_permit_sender = permit_sender.clone();
            let worker_profile = profile.clone();
            let _ = self.0.rt.spawn(async move {
                let result = worker_db
                    .quick_repair_consume_file_batches(enable_accelerated_repair,
                                                       batch_receiver,
                                                       worker_permit_sender,
                                                       worker_profile)
                    .await;
                worker_result_copy.set(result);
            });
            drop(permit_sender);

        let current_batch = Arc::new(SpinLock::new(None::<QuickRepairFileBatch>));
        let current_batch_copy = current_batch.clone();
        let replay_profile = profile.clone();
        let replay_callback = move |commit_uid: Guid, prepare_output: Vec<u8>| -> IOResult<()> {
            let record = Self::quick_repair_parse_commit_log_record(commit_uid, prepare_output)?;
            let mut current_batch = current_batch_copy.lock();
            if current_batch.is_none() {
                let permit_wait_begin = Instant::now();
                match permit_receiver.recv() {
                        Err(e) => {
                            return Err(Error::new(ErrorKind::Other,
                                                  format!("Quick repair acquire file batch permit failed, reason: {:?}",
                                                          e)));
                        },
                        Ok(()) => {
                            let file_index = replay_profile.next_file_batch_index();
                            *current_batch = Some(QuickRepairFileBatch::with_capacity(file_index,
                                                                                      permit_wait_begin.elapsed().as_millis(),
                                                                                      64));
                        },
                    }
                }

            current_batch
                .as_mut()
                .unwrap()
                .push_record(record);
            Ok(())
        };

            let current_batch_copy = current_batch.clone();
            let batch_sender_copy = batch_sender.clone();
            let file_finished_profile = profile.clone();
            let file_finished = move || -> IOResult<()> {
                let batch = {
                    let mut current_batch = current_batch_copy.lock();
                    match current_batch.take() {
                        None => {
                            return Err(Error::new(ErrorKind::Other,
                                                  "Quick repair file batch finished but current batch is missing"));
                        },
                        Some(batch) => {
                            batch
                        },
                    }
                };

                if batch.is_empty() {
                    return Ok(());
                }

                let file_index = batch.file_index;
                let record_count = batch.record_count();
                let payload_bytes = batch.payload_bytes();
                let permit_wait_ms = batch.permit_wait_ms;
                let send_wait_begin = Instant::now();
                match batch_sender_copy.send_blocking(batch) {
                    Err(e) => {
                        Err(Error::new(ErrorKind::Other,
                                       format!("Quick repair send file batch failed, reason: {:?}", e)))
                    },
                    Ok(()) => {
                        let send_wait_ms = send_wait_begin.elapsed().as_millis();
                        file_finished_profile.on_batch_buffered(file_index,
                                                               record_count,
                                                               payload_bytes,
                                                               permit_wait_ms,
                                                               send_wait_ms);
                        Ok(())
                    },
                }
            };

            let replay_begin = Instant::now();
            let replay_result = self
                .0
                .tr_mgr
                .replay_commit_log_by_file(replay_callback, file_finished)
                .await;
            let replay_loader_elapsed_ms = replay_begin.elapsed().as_millis();
            if let Ok((repaired_logs, repaired_bytes)) = &replay_result {
                info!("try_quick_repair replay loader finished, repaired_logs: {}, repaired_bytes: {}, replay_loader_elapsed_ms: {}",
                      repaired_logs,
                      repaired_bytes,
                      replay_loader_elapsed_ms);
            }

            drop(batch_sender);

            let worker_result = worker_result.await;

            match replay_result {
                Err(e) => {
                    if let Err(worker_error) = worker_result {
                        return Err(worker_error);
                    }

                    Err(e)
                },
                Ok(replay_result) => {
                    if current_batch.lock().is_some() {
                        return Err(Error::new(ErrorKind::Other,
                                              "Quick repair replay finished but current file batch was not delivered"));
                    }

                    if let Err(e) = worker_result {
                        return Err(e);
                    }

                    // 文件级 flush 全部成功后，仍沿用原有 finish_replay 统一执行 replay confirm；
                    // quick repair 不会在文件边界上提前确认提交日志。
                    let finish_replay_begin = Instant::now();
                    let _ = self.0.tr_mgr.finish_replay().await?;
                    let finish_replay_elapsed_ms = finish_replay_begin.elapsed().as_millis();
                    info!("try_quick_repair finish_replay returned, repaired_logs: {}, repaired_bytes: {}, finish_replay_elapsed_ms: {}",
                          replay_result.0,
                          replay_result.1,
                          finish_replay_elapsed_ms);
                    profile.on_finish(replay_loader_elapsed_ms,
                                      finish_replay_elapsed_ms,
                                      replay_result.0,
                                      replay_result.1);
                    Ok(replay_result)
                },
            }
        }.await;

        self.set_quick_repair_timeout_collect_suspended(false).await;
        profile.log("resume timeout collect loops after quick repair");

        result
    }

    fn quick_repair_parse_commit_log_record(commit_uid: Guid,
                                            prepare_output: Vec<u8>) -> IOResult<QuickRepairCommitLogRecord> {
        let bytes_len = prepare_output.len();
        if bytes_len < 16 {
            return Err(Error::new(ErrorKind::Other,
                                  format!("Quick repair parse commit log failed, commit_uid: {:?}, reason: invalid prepare output len {}",
                                          commit_uid,
                                          bytes_len)));
        }

        let mut offset = 0;
        let uid = u128::from_le_bytes(prepare_output[0..16].try_into().unwrap());
        let transaction_uid = Guid(uid);
        offset += 16;

        let mut table_writes = Vec::new();
        while offset < bytes_len {
            let (table, kvs_len, new_offset) =
                <MetaTable<C, Log> as KVTable>::get_init_table_prepare_output(&prepare_output, offset);
            let (writes, new_offset) =
                <MetaTable<C, Log> as KVTable>::get_all_key_value_from_table_prepare_output(&prepare_output,
                                                                                             &table,
                                                                                             kvs_len,
                                                                                             new_offset);
            table_writes.push((table, writes.into_iter().map(|write| (write.key, write.value)).collect()));
            offset = new_offset;
        }

        Ok(QuickRepairCommitLogRecord {
            commit_uid,
            prepare_output,
            transaction_uid,
            payload_bytes: bytes_len,
            table_writes,
        })
    }

    async fn set_quick_repair_timeout_collect_suspended(&self, suspended: bool) {
        let tables = self.0.tables.read().await;
        for table in tables.values() {
            match table {
                KVDBTable::MetaTab(table) => table.set_timeout_collect_suspended(suspended),
                KVDBTable::MemOrdTab(_) => (),
                KVDBTable::LogOrdTab(table) => table.set_timeout_collect_suspended(suspended),
                KVDBTable::LogWTab(table) => table.set_timeout_collect_suspended(suspended),
                KVDBTable::BtreeOrdTab(table) => table.set_timeout_collect_suspended(suspended),
            }
        }
    }

    /// 对 quick repair 当前文件批次触达过的持久化表执行一次立即 flush。
    /// 这里不会设置额外的全局 repair 模式，只是一次性复用表自身的 collect_waits 能力。
    async fn quick_collect_repaired_tables(&self,
                                           file_index: usize,
                                           tables: BTreeMap<Atom, ()>,
                                           profile: &QuickRepairProfileState) -> IOResult<usize> {
        let table_names = tables.keys().cloned().collect::<Vec<_>>();
        let table_count = table_names.len();

        // quick repair 仍以“文件级 barrier”为边界：
        // 1. 继续保持严格串行 flush；
        // 2. 同一文件批次中的触达表，按当前 DB 视图逐张执行 barrier；
        // 3. 全部 flush 完成后才允许进入下一个文件批次。
        // 这里先回退之前的“分表并发 flush”优化，优先保证顺序语义与可靠性，
        // 避免在 remove_table / recreate 等生命周期混合场景里再次引入难以证明的边界行为。
        for table_name in table_names {
            match self.get_table(&table_name).await {
                Some(KVDBTable::MetaTab(table)) => {
                    let flush_begin = Instant::now();
                    if let Err(e) = table.quick_flush_waits().await {
                        return Err(Error::new(ErrorKind::Other,
                                              format!("Quick flush repaired meta table failed, table: {:?}, reason: {:?}",
                                                      table_name,
                                                      e)));
                    }
                    profile.on_table_flush(file_index, "MetaTab", &table_name, flush_begin.elapsed().as_millis());
                },
                Some(KVDBTable::LogOrdTab(table)) => {
                    let flush_begin = Instant::now();
                    if let Err(e) = table.quick_flush_waits().await {
                        return Err(Error::new(ErrorKind::Other,
                                              format!("Quick flush repaired log ordered table failed, table: {:?}, reason: {:?}",
                                                      table_name,
                                                      e)));
                    }
                    profile.on_table_flush(file_index, "LogOrdTab", &table_name, flush_begin.elapsed().as_millis());
                },
                Some(KVDBTable::LogWTab(table)) => {
                    let flush_begin = Instant::now();
                    if let Err(e) = table.quick_flush_waits().await {
                        return Err(Error::new(ErrorKind::Other,
                                              format!("Quick flush repaired only writable table failed, table: {:?}, reason: {:?}",
                                                      table_name,
                                                      e)));
                    }
                    profile.on_table_flush(file_index, "LogWTab", &table_name, flush_begin.elapsed().as_millis());
                },
                Some(KVDBTable::BtreeOrdTab(table)) => {
                    let flush_begin = Instant::now();
                    if let Err(e) = table.quick_flush_waits().await {
                        return Err(Error::new(ErrorKind::Other,
                                              format!("Quick flush repaired b-tree ordered table failed, table: {:?}, reason: {:?}",
                                                      table_name,
                                                      e)));
                    }
                    profile.on_table_flush(file_index, "BtreeOrdTab", &table_name, flush_begin.elapsed().as_millis());
                },
                Some(KVDBTable::MemOrdTab(_)) | None => (),
            }
        }

        Ok(table_count)
    }

    async fn quick_repair_consume_file_batches(&self,
                                               enable_accelerated_repair: bool,
                                               batch_receiver: async_channel::Receiver<QuickRepairFileBatch>,
                                               permit_sender: crossbeam_channel::Sender<()>,
                                               profile: Arc<QuickRepairProfileState>)
        -> IOResult<()> {
        let mut flushed_file_batches = 0usize;

        while let Ok(batch) = batch_receiver.recv().await {
            //当前文件批次已经从等待队列切换到消费者执行路径，
            //可以放行 producer 再向前缓冲一个后继文件批次。
            let _ = permit_sender.send(());

            let stats = self.quick_repair_replay_file_batch(enable_accelerated_repair,
                                                            batch,
                                                            profile.as_ref()).await?;
            profile.on_batch_flushed(&stats);
            flushed_file_batches += 1;
            self.quick_repair_test_abort_after_file_flush(flushed_file_batches);
        }

        Ok(())
    }

    async fn quick_repair_replay_file_batch(&self,
                                            enable_accelerated_repair: bool,
                                            batch: QuickRepairFileBatch,
                                            profile: &QuickRepairProfileState) -> IOResult<QuickRepairFileBatchReplayStats> {
        let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
        let mut tables = BTreeMap::new();
        let file_index = batch.file_index;
        let record_count = batch.record_count();
        let payload_bytes = batch.payload_bytes();
        let replay_begin = Instant::now();
        let mut transaction_elapsed_us = 0;
        let mut load_elapsed_us = 0;
        let mut prepare_elapsed_us = 0;
        let mut prepare_meta_elapsed_us = 0;
        let mut prepare_log_ord_elapsed_us = 0;
        let mut prepare_log_write_elapsed_us = 0;
        let mut prepare_btree_elapsed_us = 0;
        let mut commit_elapsed_us = 0;

        info!("Quick repair file batch replay begin, file_batch: {}, records: {}, payload_bytes: {}",
              file_index,
              record_count,
              payload_bytes);

        for (index, record) in batch.records.into_iter().enumerate() {
            let record_stats = self.quick_repair_replay_record(enable_accelerated_repair,
                                                               &meta_table_name,
                                                               &mut tables,
                                                               file_index,
                                                               index + 1,
                                                               record).await?;
            transaction_elapsed_us += record_stats.transaction_elapsed_us;
            load_elapsed_us += record_stats.load_elapsed_us;
            prepare_elapsed_us += record_stats.prepare_elapsed_us;
            prepare_meta_elapsed_us += record_stats.prepare_meta_elapsed_us;
            prepare_log_ord_elapsed_us += record_stats.prepare_log_ord_elapsed_us;
            prepare_log_write_elapsed_us += record_stats.prepare_log_write_elapsed_us;
            prepare_btree_elapsed_us += record_stats.prepare_btree_elapsed_us;
            commit_elapsed_us += record_stats.commit_elapsed_us;
            profile.on_batch_replay_progress(file_index,
                                             index + 1,
                                             record_count,
                                             replay_begin.elapsed().as_millis());
        }

        let replay_elapsed_ms = replay_begin.elapsed().as_millis();
        let flush_begin = Instant::now();
        let table_names = tables.keys().cloned().collect::<Vec<_>>();
        info!("Quick repair file batch flush begin, file_batch: {}, touched_tables: {}, table_names: {}",
              file_index,
              table_names.len(),
              repair_debug_table_names(&table_names));
        let table_count = self.quick_collect_repaired_tables(file_index, tables, profile).await?;
        let flush_elapsed_ms = flush_begin.elapsed().as_millis();
        info!("Quick repair file batch flush end, file_batch: {}, records: {}, payload_bytes: {}, touched_tables: {}, replay_elapsed_ms: {}, flush_elapsed_ms: {}",
              file_index,
              record_count,
              payload_bytes,
              table_count,
              replay_elapsed_ms,
              flush_elapsed_ms);

        Ok(QuickRepairFileBatchReplayStats {
            file_index,
            record_count,
            payload_bytes,
            table_count,
            replay_elapsed_ms,
            flush_elapsed_ms,
            transaction_elapsed_us,
            load_elapsed_us,
            prepare_elapsed_us,
            prepare_meta_elapsed_us,
            prepare_log_ord_elapsed_us,
            prepare_log_write_elapsed_us,
            prepare_btree_elapsed_us,
            commit_elapsed_us,
        })
    }

    #[cfg(debug_assertions)]
    fn quick_repair_test_abort_after_file_flush(&self,
                                                flushed_file_batches: usize) {
        const QUICK_REPAIR_TEST_ABORT_AFTER_FILE_FLUSHES_ENV: &str = "PI_DB_QUICK_REPAIR_TEST_ABORT_AFTER_FILE_FLUSHES";

        if let Ok(expected) = std::env::var(QUICK_REPAIR_TEST_ABORT_AFTER_FILE_FLUSHES_ENV) {
            if let Ok(expected) = expected.parse::<usize>() {
                if expected > 0 && flushed_file_batches >= expected {
                    // 仅供 quick repair 集成测试使用：
                    // 在“部分文件已 flush，但整体尚未 finish_replay”这一窗口直接退出进程，
                    // 用于稳定复现重启后的幂等恢复场景。
                    std::process::exit(0);
                }
            }
        }
    }

    #[cfg(not(debug_assertions))]
    #[inline(always)]
    fn quick_repair_test_abort_after_file_flush(&self,
                                                _flushed_file_batches: usize) {}

    async fn quick_repair_replay_record(&self,
                                        enable_accelerated_repair: bool,
                                        meta_table_name: &Atom,
                                        tables: &mut BTreeMap<Atom, ()>,
                                        file_index: usize,
                                        record_index: usize,
                                        record: QuickRepairCommitLogRecord) -> IOResult<QuickRepairRecordReplayStats> {
        let mut stats = QuickRepairRecordReplayStats::default();
        let QuickRepairCommitLogRecord {
            commit_uid,
            prepare_output,
            transaction_uid,
            table_writes,
            ..
        } = record;
        let transaciton_uid = transaction_uid;

        let transaction_begin = Instant::now();
        if let Some(tr) = self.transaction(Atom::from(QUICK_REPAIR_DB_SOURCE),
                                           true,
                                           5000,
                                           5000) {
            stats.transaction_elapsed_us = transaction_begin.elapsed().as_micros();
            let load_begin = Instant::now();
            for (table, writes) in table_writes {
                if &table == meta_table_name {
                    tables.insert(meta_table_name.clone(), ());
                    for (key, value) in writes {
                        if let Some(value) = value {
                            let table_name = match binary_to_table(&key) {
                                Err(e) => {
                                    panic!("From binary to table name failed, reason: {:?}", e);
                                },
                                Ok(table_name) => {
                                    table_name
                                }
                            };
                            let table_meta = KVTableMeta::from(value);
                            quick_repair_profile_log(format!("meta create table: file_batch={}, record_index={}, transaction_uid={:?}, commit_uid={:?}, table_name={:?}, table_type={:?}",
                                                             file_index,
                                                             record_index,
                                                             transaciton_uid,
                                                             commit_uid,
                                                             table_name,
                                                             table_meta.table_type));

                            if let Err(e) = tr.repair_create_table(table_name.clone(),
                                                                  table_meta.clone(),
                                                                  enable_accelerated_repair).await {
                                return Err(Error::new(ErrorKind::Other,
                                                      format!("Quick repair tables meta failed, file_batch: {}, record_index: {}, transaction_uid: {:?}, commit_uid: {:?}, table_name: {:?}, table_meta: {:?}, reason: {:?}",
                                                              file_index,
                                                              record_index,
                                                              transaciton_uid,
                                                              commit_uid,
                                                              table_name,
                                                              table_meta,
                                                              e)));
                            }
                        } else {
                            let table_name = match binary_to_table(&key) {
                                Err(e) => {
                                    panic!("From binary to table name failed, reason: {:?}", e);
                                },
                                Ok(table_name) => {
                                    table_name
                                }
                            };
                            quick_repair_profile_log(format!("meta remove table: file_batch={}, record_index={}, transaction_uid={:?}, commit_uid={:?}, table_name={:?}",
                                                             file_index,
                                                             record_index,
                                                             transaciton_uid,
                                                             commit_uid,
                                                             table_name));

                            if let Err(e) = tr.repair_remove_table(table_name.clone()).await {
                                return Err(Error::new(ErrorKind::Other,
                                                      format!("Quick repair tables meta failed, file_batch: {}, record_index: {}, transaction_uid: {:?}, commit_uid: {:?}, table_name: {:?}, reason: {:?}",
                                                              file_index,
                                                              record_index,
                                                              transaciton_uid,
                                                              commit_uid,
                                                              table_name,
                                                              e)));
                            }
                        }
                    }
                } else {
                    if !writes.is_empty() {
                        tables.insert(table.clone(), ());
                        if let Err(e) = tr.quick_repair_writes(table.clone(),
                                                               writes).await {
                            return Err(Error::new(ErrorKind::Other,
                                                  format!("Quick repair db failed, file_batch: {}, record_index: {}, transaction_uid: {:?}, commit_uid: {:?}, table_name: {:?}, reason: {:?}",
                                                          file_index,
                                                          record_index,
                                                          transaciton_uid,
                                                          commit_uid,
                                                          table,
                                                          e)));
                        }
                    }
                }
            }
            stats.load_elapsed_us = load_begin.elapsed().as_micros();

            let prepare_begin = Instant::now();
            let prepare_breakdown = match tr.prepare_quick_repair(transaciton_uid.clone()).await {
                Err(e) => {
                    return Err(Error::new(ErrorKind::Other,
                                          format!("Quick repair db failed, file_batch: {}, record_index: {}, transaction_uid: {:?}, commit_uid: {:?}, reason: {:?}",
                                                  file_index,
                                                  record_index,
                                                  transaciton_uid,
                                                  commit_uid,
                                                  e)));
                },
                Ok(prepare_breakdown) => {
                    prepare_breakdown
                },
            };
            stats.prepare_elapsed_us = prepare_begin.elapsed().as_micros();
            stats.prepare_meta_elapsed_us = prepare_breakdown.meta_elapsed_us;
            stats.prepare_log_ord_elapsed_us = prepare_breakdown.log_ord_elapsed_us;
            stats.prepare_log_write_elapsed_us = prepare_breakdown.log_write_elapsed_us;
            stats.prepare_btree_elapsed_us = prepare_breakdown.btree_elapsed_us;

            let commit_begin = Instant::now();
            if let Err(e) = tr
                .commit_repair(transaciton_uid.clone(),
                               commit_uid.clone(),
                               prepare_output).await {
                return Err(Error::new(ErrorKind::Other,
                                      format!("Quick repair db failed, file_batch: {}, record_index: {}, transaction_uid: {:?}, commit_uid: {:?}, reason: {:?}",
                                              file_index,
                                              record_index,
                                              transaciton_uid,
                                              commit_uid,
                                              e)));
            }
            stats.commit_elapsed_us = commit_begin.elapsed().as_micros();

            Ok(stats)
        } else {
            Err(Error::new(ErrorKind::Other,
                           format!("Quick repair db failed, file_batch: {}, record_index: {}, transaction_uid: {:?}, commit_uid: {:?}, reason: get db transaction error",
                                   file_index,
                                   record_index,
                                   transaciton_uid,
                                   commit_uid)))
        }
    }
}

///
/// quick repair 按文件批次缓冲的一条提交日志记录。
///
struct QuickRepairCommitLogRecord {
    commit_uid:      Guid,
    prepare_output:  Vec<u8>,
    transaction_uid: Guid,
    payload_bytes:   usize,
    table_writes:    Vec<(Atom, Vec<(Binary, Option<Binary>)>)>,
}

///
/// quick repair 统计型日志的共享状态。
/// 只在显式开启环境变量时输出，用于定位批次装载、顺序重放、文件级 flush 和 replay confirm 的耗时。
///
struct QuickRepairProfileState {
    enabled:            bool,
    record_interval:    usize,
    begin:              Instant,
    next_file_index:    AtomicUsize,
    counters:           SpinLock<QuickRepairProfileCounters>,
}

#[derive(Default)]
struct QuickRepairProfileCounters {
    buffered_files:     usize,
    buffered_records:   usize,
    buffered_bytes:     usize,
    replayed_files:     usize,
    replayed_records:   usize,
    replayed_bytes:     usize,
    total_permit_wait_ms: u128,
    total_send_wait_ms: u128,
    total_replay_ms:    u128,
    total_flush_ms:     u128,
    total_tables:       usize,
    total_transaction_us: u128,
    total_load_us:      u128,
    total_prepare_us:   u128,
    total_prepare_meta_us: u128,
    total_prepare_log_ord_us: u128,
    total_prepare_log_write_us: u128,
    total_prepare_btree_us: u128,
    total_commit_us:    u128,
}

impl QuickRepairProfileState {
    fn from_env() -> Arc<Self> {
        let enabled = match std::env::var(QUICK_REPAIR_PROFILE_LOG_ENV) {
            Ok(value) => {
                let value = value.trim();
                !value.is_empty() && value != "0" && value.to_ascii_lowercase() != "false"
            },
            Err(_) => {
                false
            },
        };
        let record_interval = match std::env::var(QUICK_REPAIR_PROFILE_RECORD_INTERVAL_ENV) {
            Ok(value) => value.parse::<usize>().unwrap_or(0),
            Err(_) => 0,
        };

        Arc::new(QuickRepairProfileState {
            enabled,
            record_interval,
            begin: Instant::now(),
            next_file_index: AtomicUsize::new(1),
            counters: SpinLock::new(QuickRepairProfileCounters::default()),
        })
    }

    #[inline(always)]
    fn next_file_batch_index(&self) -> usize {
        self.next_file_index.fetch_add(1, Ordering::Relaxed)
    }

    fn log<S: AsRef<str>>(&self, message: S) {
        if self.enabled {
            println!("[quick_repair_profile][+{}ms] {}",
                     self.begin.elapsed().as_millis(),
                     message.as_ref());
        }
    }

    fn on_batch_buffered(&self,
                         file_index: usize,
                         record_count: usize,
                         payload_bytes: usize,
                         permit_wait_ms: u128,
                         send_wait_ms: u128) {
        if !self.enabled {
            return;
        }

        {
            let mut counters = self.counters.lock();
            counters.buffered_files += 1;
            counters.buffered_records += record_count;
            counters.buffered_bytes += payload_bytes;
            counters.total_permit_wait_ms += permit_wait_ms;
            counters.total_send_wait_ms += send_wait_ms;
        }

        self.log(format!("buffered file batch: file_batch={}, records={}, payload_bytes={}, permit_wait_ms={}, send_wait_ms={}",
                         file_index,
                         record_count,
                         payload_bytes,
                         permit_wait_ms,
                         send_wait_ms));
    }

    fn on_batch_replay_progress(&self,
                                file_index: usize,
                                replayed_records: usize,
                                total_records: usize,
                                elapsed_ms: u128) {
        if !self.enabled || self.record_interval == 0 {
            return;
        }

        if replayed_records % self.record_interval != 0 && replayed_records != total_records {
            return;
        }

        self.log(format!("replay progress: file_batch={}, records={}/{}, replay_elapsed_ms={}",
                         file_index,
                         replayed_records,
                         total_records,
                         elapsed_ms));
    }

    fn on_table_flush(&self,
                      file_index: usize,
                      table_type: &str,
                      table_name: &Atom,
                      flush_elapsed_ms: u128) {
        if !self.enabled {
            return;
        }

        self.log(format!("table flush: file_batch={}, table_type={}, table_name={:?}, flush_elapsed_ms={}",
                         file_index,
                         table_type,
                         table_name,
                         flush_elapsed_ms));
    }

    fn on_batch_flushed(&self,
                        stats: &QuickRepairFileBatchReplayStats) {
        if !self.enabled {
            return;
        }

        {
            let mut counters = self.counters.lock();
            counters.replayed_files += 1;
            counters.replayed_records += stats.record_count;
            counters.replayed_bytes += stats.payload_bytes;
            counters.total_replay_ms += stats.replay_elapsed_ms;
            counters.total_flush_ms += stats.flush_elapsed_ms;
            counters.total_tables += stats.table_count;
            counters.total_transaction_us += stats.transaction_elapsed_us;
            counters.total_load_us += stats.load_elapsed_us;
            counters.total_prepare_us += stats.prepare_elapsed_us;
            counters.total_prepare_meta_us += stats.prepare_meta_elapsed_us;
            counters.total_prepare_log_ord_us += stats.prepare_log_ord_elapsed_us;
            counters.total_prepare_log_write_us += stats.prepare_log_write_elapsed_us;
            counters.total_prepare_btree_us += stats.prepare_btree_elapsed_us;
            counters.total_commit_us += stats.commit_elapsed_us;
        }

        self.log(format!("file batch finished: file_batch={}, records={}, payload_bytes={}, tables={}, replay_elapsed_ms={}, flush_elapsed_ms={}, transaction_ms={:.3}, load_ms={:.3}, prepare_ms={:.3}, prepare_meta_ms={:.3}, prepare_log_ord_ms={:.3}, prepare_log_write_ms={:.3}, prepare_btree_ms={:.3}, commit_ms={:.3}, total_elapsed_ms={}",
                         stats.file_index,
                         stats.record_count,
                         stats.payload_bytes,
                         stats.table_count,
                         stats.replay_elapsed_ms,
                         stats.flush_elapsed_ms,
                         stats.transaction_elapsed_us as f64 / 1000.0,
                         stats.load_elapsed_us as f64 / 1000.0,
                         stats.prepare_elapsed_us as f64 / 1000.0,
                         stats.prepare_meta_elapsed_us as f64 / 1000.0,
                         stats.prepare_log_ord_elapsed_us as f64 / 1000.0,
                         stats.prepare_log_write_elapsed_us as f64 / 1000.0,
                         stats.prepare_btree_elapsed_us as f64 / 1000.0,
                         stats.commit_elapsed_us as f64 / 1000.0,
                         stats.replay_elapsed_ms + stats.flush_elapsed_ms));
    }

    fn on_finish(&self,
                 replay_loader_elapsed_ms: u128,
                 finish_replay_elapsed_ms: u128,
                 repaired_logs: usize,
                 repaired_bytes: usize) {
        if !self.enabled {
            return;
        }

        let counters = self.counters.lock();
        self.log(format!("summary: repaired_logs={}, repaired_log_bytes={}, buffered_files={}, replayed_files={}, buffered_records={}, replayed_records={}, buffered_bytes={}, replayed_bytes={}, total_permit_wait_ms={}, total_send_wait_ms={}, total_replay_ms={}, total_flush_ms={}, total_tables={}, total_transaction_ms={:.3}, total_load_ms={:.3}, total_prepare_ms={:.3}, total_prepare_meta_ms={:.3}, total_prepare_log_ord_ms={:.3}, total_prepare_log_write_ms={:.3}, total_prepare_btree_ms={:.3}, total_commit_ms={:.3}, replay_loader_elapsed_ms={}, finish_replay_elapsed_ms={}, total_elapsed_ms={}",
                         repaired_logs,
                         repaired_bytes,
                         counters.buffered_files,
                         counters.replayed_files,
                         counters.buffered_records,
                         counters.replayed_records,
                         counters.buffered_bytes,
                         counters.replayed_bytes,
                         counters.total_permit_wait_ms,
                         counters.total_send_wait_ms,
                         counters.total_replay_ms,
                         counters.total_flush_ms,
                         counters.total_tables,
                         counters.total_transaction_us as f64 / 1000.0,
                         counters.total_load_us as f64 / 1000.0,
                         counters.total_prepare_us as f64 / 1000.0,
                         counters.total_prepare_meta_us as f64 / 1000.0,
                         counters.total_prepare_log_ord_us as f64 / 1000.0,
                         counters.total_prepare_log_write_us as f64 / 1000.0,
                         counters.total_prepare_btree_us as f64 / 1000.0,
                         counters.total_commit_us as f64 / 1000.0,
                         replay_loader_elapsed_ms,
                         finish_replay_elapsed_ms,
                         self.begin.elapsed().as_millis()));
    }
}

///
/// quick repair 的一个 commit log 文件批次。
/// 一个批次内的所有记录都属于同一个物理 commit log 文件。
///
struct QuickRepairFileBatch {
    file_index:      usize,
    permit_wait_ms:  u128,
    payload_bytes:   usize,
    records:         Vec<QuickRepairCommitLogRecord>,
}

impl QuickRepairFileBatch {
    fn with_capacity(file_index: usize,
                     permit_wait_ms: u128,
                     capacity: usize) -> Self {
        QuickRepairFileBatch {
            file_index,
            permit_wait_ms,
            payload_bytes: 0,
            records: Vec::with_capacity(capacity),
        }
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn push_record(&mut self,
                   record: QuickRepairCommitLogRecord) {
        self.payload_bytes += record.payload_bytes;
        self.records.push(record);
    }

    fn record_count(&self) -> usize {
        self.records.len()
    }

    fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }
}

struct QuickRepairFileBatchReplayStats {
    file_index:          usize,
    record_count:        usize,
    payload_bytes:       usize,
    table_count:         usize,
    replay_elapsed_ms:   u128,
    flush_elapsed_ms:    u128,
    transaction_elapsed_us: u128,
    load_elapsed_us:     u128,
    prepare_elapsed_us:  u128,
    prepare_meta_elapsed_us: u128,
    prepare_log_ord_elapsed_us: u128,
    prepare_log_write_elapsed_us: u128,
    prepare_btree_elapsed_us: u128,
    commit_elapsed_us:   u128,
}

#[derive(Default)]
struct QuickRepairRecordReplayStats {
    transaction_elapsed_us: u128,
    load_elapsed_us:     u128,
    prepare_elapsed_us:  u128,
    prepare_meta_elapsed_us: u128,
    prepare_log_ord_elapsed_us: u128,
    prepare_log_write_elapsed_us: u128,
    prepare_btree_elapsed_us: u128,
    commit_elapsed_us:   u128,
}

#[derive(Default)]
struct QuickRepairPrepareBreakdown {
    meta_elapsed_us:       u128,
    log_ord_elapsed_us:    u128,
    log_write_elapsed_us:  u128,
    btree_elapsed_us:      u128,
}

// 内部键值对数据库管理器
struct InnerKVDBManager<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    rt:                                 MultiTaskRuntime<()>,               //异步运行时
    tr_mgr:                             Transaction2PcManager<C, Log>,      //事务管理器
    db_path:                            PathBuf,                            //数据库的表文件所在目录的路径
    tables_meta_path:                   PathBuf,                            //数据库的元信息表文件所在目录的路径
    tables_path:                        PathBuf,                            //数据库表文件所在目录的路径
    quick_repair_file_pipeline_depth:   usize,                              //quick repair 按文件批次处理时的流水线深度
    tables:                             Arc<RwLock<XHashMap<Atom, KVDBTable<C, Log>>>>, //数据表
    status:                             AtomicU64,                          //数据库状态
    listener:                           Option<Receiver<KVDBEvent<Guid>>>,  //数据库事件监听器
    notifier:                           Option<Sender<KVDBEvent<Guid>>>,    //数据库事件通知器
}

///
/// 键值对数据库事务
///
#[derive(Clone)]
pub enum KVDBTransaction<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    RootTr(RootTransaction<C, Log>),        //键值对数据库的根事务
    MetaTabTr(MetaTabTr<C, Log>),           //元信息表事务
    MemOrdTabTr(MemOrdTabTr<C, Log>),       //有序内存表事务
    LogOrdTabTr(LogOrdTabTr<C, Log>),       //有序日志表事务
    LogWTabTr(LogWTabTr<C, Log>),           //只写日志表事务
    BtreeOrdTabTr(BtreeOrdTabTr<C, Log>),   //有序B树表事务
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.is_writable()
            }
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.get_commit_timeout()
            },
        }
    }

    fn prepare(&self)
               -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.prepare()
            },
        }
    }

    fn prepare_conflicts(&self)
        -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        #[cfg(feature = "default")]
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.prepare_conflicts()
            },
            KVDBTransaction::MetaTabTr(tr) => {
                tr.prepare_conflicts()
            },
            KVDBTransaction::MemOrdTabTr(tr) => {
                tr.prepare_conflicts()
            },
            KVDBTransaction::LogOrdTabTr(tr) => {
                tr.prepare_conflicts()
            },
            KVDBTransaction::LogWTabTr(tr) => {
                tr.prepare_conflicts()
            },
            KVDBTransaction::BtreeOrdTabTr(tr) => {
                tr.prepare_conflicts()
            },
        }
    }

    fn commit(&self, confirm: <Self as Transaction2Pc>::CommitConfirm)
              -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
            KVDBTransaction::BtreeOrdTabTr(tr) => {
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
                                           options: CreateTableOptions,
                                           enable_accelerated_repair: bool) -> IOResult<()> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.create_table_with_options(
                    name,
                    meta,
                    options,
                    enable_accelerated_repair
                ).await
            },
            _ => panic!("Create table failed, reason: invalid root transaction"),
        }
    }

    /// 异步创建表，需要指定表名和表的元信息
    pub async fn create_table(&self,
                              name: Atom,
                              meta: KVTableMeta,
                              enable_accelerated_repair: bool) -> IOResult<()> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.create_table(
                    name,
                    meta,
                    enable_accelerated_repair
                ).await
            },
            _ => panic!("Create table failed, reason: invalid root transaction"),
        }
    }

    /// 异步批量创建表，只允许在初始化加载表时使用
    pub(crate) async fn create_multiple_tables(&self,
                                               table_metas: Vec<(Atom, KVTableMeta, Option<CreateTableOptions>)>,
                                               is_checksum: bool,
                                               enable_accelerated_repair: bool)
        -> IOResult<()>
    {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.create_multiple_tables(
                    table_metas,
                    is_checksum,
                    enable_accelerated_repair
                ).await
            },
            _ => panic!("Create multiple table failed, reason: invalid root transaction"),
        }
    }

    /// 异步修复创建表，需要指定表名和表的元信息
    pub(crate) async fn repair_create_table(&self,
                                            name: Atom,
                                            meta: KVTableMeta,
                                            enable_accelerated_repair: bool) -> IOResult<()> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.repair_create_table(name, meta, enable_accelerated_repair).await
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

    /// 在键值对数据库事务的根事务内，快速装载指定表的修复写操作。
    /// 这个入口只用于 `try_quick_repair`，不会走普通事务的读写路径。
    async fn quick_repair_writes(&self,
                                 table: Atom,
                                 writes: Vec<(Binary, Option<Binary>)>) -> Result<(), KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.quick_repair_writes(table, writes).await
            },
            _ => panic!("Quick repair db failed, reason: invalid root transaction"),
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

    /// 在键值对数据库事务的根事务内，异步预提交本次事务对键值对数据库的所有修改，成功返回预提交的输出，失败返回预提交冲突的首个表名和关键字
    pub async fn prepare_modified_conflicts(&self) -> Result<Vec<u8>, KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.prepare_modified_conflicts().await
            },
            _ => panic!("Prepare conflicts db failed, reason: invalid root transaction"),
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

    /// 在键值对数据库事务的根事务内，异步预提交本次快速修复修改，不返回预提交的输出。
    /// 与旧 repair 的差异是这里会显式跳过内存表，只让持久化表进入 prepare repair。
    async fn prepare_quick_repair(&self,
                                  transaction_uid: Guid)
                                  -> Result<QuickRepairPrepareBreakdown, KVTableTrError> {
        match self {
            KVDBTransaction::RootTr(tr) => {
                tr.prepare_quick_repair(transaction_uid).await
            },
            _ => panic!("Quick repair prepare modified db failed, reason: invalid root transaction"),
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

    fn prepare_conflicts(&self) -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        self.prepare()
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
            KVDBTable::BtreeOrdTab(tab) => {
                let tr = tab.transaction(self.get_source(),
                                         self.is_writable(),
                                         is_persistent,
                                         self.get_prepare_timeout(),
                                         self.get_commit_timeout());
                let table_tr = KVDBTransaction::BtreeOrdTabTr(tr);

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
                                       options: CreateTableOptions,
                                       enable_accelerated_repair: bool) -> IOResult<()>
    {
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
                                    Some(KVDBTable::BtreeOrdTab(tab)) => {
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
                                             60 * 1000,
                                             self.0.db_mgr.0.notifier.clone()).await;

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
            KVDBTableType::BtreeOrdTab => {
                //创建一个有序B树表
                let table_path = self.0.db_mgr.0.tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                if let CreateTableOptions::BtreeOrdTab(cache_size, enable_compact) = options.clone() {
                    //有序日志表的选项
                    let table =
                        BtreeOrderedTable::new(self.0.db_mgr.0.rt.clone(),
                                               table_path,
                                               name.clone(),
                                               cache_size,
                                               enable_compact,
                                               1024 * 1024,
                                               60 * 1000,
                                               enable_accelerated_repair,
                                               self.0.db_mgr.0.notifier.clone()).await;

                    //注册创建的有序日志表
                    tables.insert(name.clone(), KVDBTable::BtreeOrdTab(table));
                } else {
                    //没有有序日志表的选项，则立即返回错误原因
                    return Err(Error::new(ErrorKind::Other,
                                          format!("Create table failed, name: {:?}, meta: {:?}, options: {:?}, reason: invalid options",
                                                  name,
                                                  meta,
                                                  options)));
                }
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
                          meta: KVTableMeta,
                          enable_accelerated_repair: bool) -> IOResult<()>
    {
        match meta.table_type {
            KVDBTableType::LogOrdTab => {
                self.create_table_with_options(name,
                                               meta,
                                               CreateTableOptions::LogOrdTab(512 * 1024 * 1024,
                                                                             2 * 1024 * 1024,
                                                                             2 * 1024 * 1024),
                                               enable_accelerated_repair)
                    .await
            },
            KVDBTableType::BtreeOrdTab => {
                self.create_table_with_options(name,
                                               meta,
                                               CreateTableOptions::BtreeOrdTab(16 * 1024 * 1024,
                                                                               true),
                                               enable_accelerated_repair)
                    .await
            },
            _ => {
                self.create_table_with_options(name,
                                               meta,
                                               CreateTableOptions::Empty,
                                               enable_accelerated_repair)
                    .await
            },
        }

    }

    /// 异步创建指定的多个表，表名可以是用文件分隔符分隔的路径，但必须是相对路径，且不允许使用".."
    async fn create_multiple_tables(&self,
                                    table_metas: Vec<(Atom, KVTableMeta, Option<CreateTableOptions>)>,
                                    is_checksum: bool,
                                    enable_accelerated_repair: bool)
        -> IOResult<()>
    {
        //创建表的操作，一定会创建元信息表事务，而元信息表事务是需要持久化的事务，则根事务也设置为需要持久化
        self.require_persistence();

        //检查待创建的指定名称的表是否存在
        let mut require_create_tables = Vec::new();
        let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
        {
            let tables = self
                .0
                .db_mgr
                .0
                .tables
                .read()
                .await;

            for (name, meta, options) in table_metas {
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
                                            Some(KVDBTable::BtreeOrdTab(tab)) => {
                                                if tab.len() > 0 {
                                                    //已存在的同名表是持久化表，且元信息不同，且表中有记录，则表名冲突
                                                    return Err(Error::new(ErrorKind::AlreadyExists,
                                                                          format!("Create table failed, name: {:?}, meta: {:?}, reason: name conflict", name, meta)));
                                                }
                                            },
                                            _ => (), //忽略内存表元信息的不同
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

                //记录需要创建的表
                require_create_tables.push((name, meta, options));
            }
        }

        //并发创建待创建的表
        let result = AsyncValue::new();
        let count = Arc::new(AtomicU64::new(require_create_tables.len() as u64));
        let startup_table_load_profile = self.get_source().as_str() == STARTUP_DB_SOURCE
            && quick_repair_profile_log_enabled();
        for (name, meta, options) in require_create_tables.clone() {
            let db_rt = self.0.db_mgr.0.rt.clone();
            let tables_path = self
                .0
                .db_mgr
                .0
                .tables_path
                .clone();
            let tables = self
                .0
                .db_mgr
                .0
                .tables
                .clone();
            let result_copy = result.clone();
            let count_copy = count.clone();

            let notifier = self.0.db_mgr.0.notifier.clone();
            let startup_table_load_profile_copy = startup_table_load_profile;
            let _ = self.0.db_mgr.0.rt.spawn(async move {
                //待创建的指定名称的表不存在，则创建指定名称的表，并将表的元信息注册到元信息表
                let table_load_begin = Instant::now();
                let table_type_label = match meta.table_type {
                    KVDBTableType::MemOrdTab => "MemOrdTab",
                    KVDBTableType::LogOrdTab => "LogOrdTab",
                    KVDBTableType::LogWTab => "LogWTab",
                    KVDBTableType::BtreeOrdTab => "BtreeOrdTab",
                };
                match meta.table_type {
                    KVDBTableType::MemOrdTab => {
                        //创建一个有序内存表
                        let table = MemoryOrderedTable::new(name.clone(),
                                                            meta.persistence);

                        //注册创建的有序内存表
                        tables
                            .write()
                            .await
                            .insert(name.clone(),
                                    KVDBTable::MemOrdTab(table));
                    },
                    KVDBTableType::LogOrdTab => {
                        //创建一个有序日志表
                        let table_path = tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                        if let Some(CreateTableOptions::LogOrdTab(log_file_limit, block_limit, load_buf_len)) = options.clone() {
                            //有序日志表的选项
                            let table =
                                LogOrderedTable::new(db_rt,
                                                     table_path,
                                                     name.clone(),
                                                     log_file_limit,
                                                     block_limit,
                                                     None,
                                                     load_buf_len as u64,
                                                     is_checksum,
                                                     16 * 1024 * 1024,
                                                     60 * 1000,
                                                     notifier).await;

                            //注册创建的有序日志表
                            tables
                                .write()
                                .await
                                .insert(name.clone(),
                                        KVDBTable::LogOrdTab(table));
                        } else {
                            //没有有序日志表的选项，则立即通知错误原因
                            result_copy.set(Err(Error::new(ErrorKind::Other,
                                                           format!("Create table failed, name: {:?}, meta: {:?}, options: {:?}, reason: invalid options",
                                                                   name,
                                                                   meta,
                                                                   options))));
                            return;
                        }
                    },
                    KVDBTableType::LogWTab => {
                        //创建一个只写日志表
                        let table_path = tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                        let table =
                            LogWriteTable::new(db_rt,
                                               table_path,
                                               name.clone(),
                                               512 * 1024 * 1024,
                                               2 * 1024 * 1024,
                                               None,
                                               2 * 1024 * 1024,
                                               is_checksum,
                                               16 * 1024 * 1024,
                                               60 * 1000).await;

                        //注册创建的只写日志表
                        tables
                            .write()
                            .await
                            .insert(name.clone(),
                                    KVDBTable::LogWTab(table));
                    },
                    KVDBTableType::BtreeOrdTab => {
                        //创建一个有序B树表
                        let table_path = tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                        if let Some(CreateTableOptions::BtreeOrdTab(cache_size, enable_compact)) = options.clone() {
                            //有序日志表的选项
                            let table =
                                BtreeOrderedTable::new(db_rt,
                                                       table_path,
                                                       name.clone(),
                                                       cache_size,
                                                       enable_compact,
                                                       1024 * 1024,
                                                       60 * 1000,
                                                       enable_accelerated_repair,
                                                       notifier).await;

                            //注册创建的有序日志表
                            tables
                                .write()
                                .await
                                .insert(name.clone(),
                                        KVDBTable::BtreeOrdTab(table));
                        } else {
                            //没有有序日志表的选项，则立即通知错误原因
                            result_copy.set(Err(Error::new(ErrorKind::Other,
                                                           format!("Create table failed, name: {:?}, meta: {:?}, options: {:?}, reason: invalid options",
                                                                   name,
                                                                   meta,
                                                                   options))));
                            return;
                        }
                    },
                }

                if startup_table_load_profile_copy {
                    quick_repair_profile_log(format!("startup table loaded: table_name={:?}, table_type={}, elapsed_ms={}",
                                                     name,
                                                     table_type_label,
                                                     table_load_begin.elapsed().as_millis()));
                }

                if count_copy.fetch_sub(1, Ordering::AcqRel) <= 1 {
                    //本次所有待创建的表都已创建成功，则立即通知创建成功
                    result_copy.set(Ok(()));
                }
            });
        }

        //等待批量创建表全部成功
        result.await?;

        //注册表的元信息
        let mut tables = self
            .0
            .db_mgr
            .0
            .tables
            .write()
            .await;
        for (name, meta, _options) in require_create_tables {
            if let Some(meta_table) = tables.get(&meta_table_name) {
                let mut childes_map = self.0.childs_map.lock();
                let meta_table_tr = if let Some(table_tr) = childes_map.get(&meta_table_name) {
                    //元信息表的子事务存在，则设置子事务为需要持久化
                    table_tr.require_persistence();
                    table_tr.clone()
                } else {
                    //元信息表的子事务不存在，则创建元信息表的事务，因为需要创建表，所以初始化元信息表的子事务为持久化事务
                    self.table_transaction(meta_table_name.clone(),
                                           meta_table,
                                           true,
                                           &mut *childes_map)
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
        }

        Ok(())
    }

    /// 异步修复创建表，表名可以是用文件分隔符分隔的路径，但必须是相对路径，且不允许使用".."
    #[inline]
    async fn repair_create_table(&self,
                                 name: Atom,
                                 meta: KVTableMeta,
                                 enable_accelerated_repair: bool) -> IOResult<()>
    {
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
                                         60 * 1000,
                                         self.0.db_mgr.0.notifier.clone()).await;

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
            KVDBTableType::BtreeOrdTab => {
                //尝试创建一个有序B树表
                let table_path = self.0.db_mgr.0.tables_path.join(name.as_str()); //通过键值对数据库的表所在目录的路径与表名，生成表所在目录的路径
                if let Some(table) =
                    BtreeOrderedTable::try_new(self.0.db_mgr.0.rt.clone(),
                                               table_path,
                                               name.clone(),
                                               DEFAULT_CACHE_SIZE,
                                               true,
                                               1024 * 1024,
                                               60 * 1000,
                                               enable_accelerated_repair,
                                               self.0.db_mgr.0.notifier.clone()).await {
                    //尝试创建成功，则注册创建的有序日志表
                    tables.insert(name.clone(), KVDBTable::BtreeOrdTab(table));
                }
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

    /// 快速装载指定表的修复写操作，只记录持久化表的动作，不参与正常事务路径。
    /// 这里不对内存表建动作，也不提前做 prepare/commit，只把后续 repair 所需的动作挂到对应子事务。
    async fn quick_repair_writes(&self,
                                 table_name: Atom,
                                 writes: Vec<(Binary, Option<Binary>)>) -> Result<(), KVTableTrError> {
        if writes.is_empty() {
            return Ok(());
        }

        if let Some(table) = self.0.db_mgr.0.tables.read().await.get(&table_name) {
            if !table.is_persistent() {
                //内存表不参与快速修复，因为重启后数据本来就不会保留
                return Ok(());
            }

            let mut childes_map = self.0.childs_map.lock();
            let table_tr = if let Some(table_tr) = childes_map.get(&table_name) {
                table_tr.require_persistence();
                table_tr.clone()
            } else {
                self.table_transaction(table_name.clone(), table, true, &mut *childes_map)
            };

            if table_tr.is_require_persistence() {
                self.0.persistence.store(true, Ordering::Relaxed);
            }

            match &table_tr {
                KVDBTransaction::RootTr(_) => (),
                KVDBTransaction::MetaTabTr(tr) => {
                    tr.quick_repair_writes(writes);
                },
                KVDBTransaction::MemOrdTabTr(_) => (),
                KVDBTransaction::LogOrdTabTr(tr) => {
                    tr.quick_repair_writes(writes);
                },
                KVDBTransaction::LogWTabTr(tr) => {
                    tr.quick_repair_writes(writes);
                },
                KVDBTransaction::BtreeOrdTabTr(tr) => {
                    tr.quick_repair_writes(writes);
                },
            }
        } else {
            let existing_tables = {
                let tables = self.0.db_mgr.0.tables.read().await;
                let mut names = tables.keys()
                    .map(|name| name.as_str().to_string())
                    .collect::<Vec<_>>();
                names.sort();
                names
            };
            quick_repair_profile_log(format!("table missing before quick repair writes: target_table={:?}, existing_tables={:?}",
                                             table_name,
                                             existing_tables));
            return Err(KVTableTrError::new_transaction_error(ErrorLevel::Fatal,
                                                             format!("Quick repair db failed, table: {:?}, reason: table not exist",
                                                                     table_name.as_str())));
        }

        Ok(())
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
                    KVDBTransaction::BtreeOrdTabTr(tr) => {
                        //查询有序B树表的指定关键字的值
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
                    KVDBTransaction::BtreeOrdTabTr(tr) => {
                        //查询有序B树表的指定关键字的值
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
                    KVDBTransaction::BtreeOrdTabTr(tr) => {
                        //插入或更新有序B树表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.dirty_upsert(table_kv.key, value).await {
                                //插入或更新有序B树表的指定关键字的值错误，则立即返回错误原因
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
                    KVDBTransaction::BtreeOrdTabTr(tr) => {
                        //插入或更新有序B树表的指定关键字的值
                        if let Some(value) = table_kv.value {
                            //有值则插入或更新
                            if let Err(e) = tr.upsert(table_kv.key, value).await {
                                //插入或更新有序B树表的指定关键字的值错误，则立即返回错误原因
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
                    KVDBTransaction::BtreeOrdTabTr(tr) => {
                        //删除有序B树表的指定关键字的值
                        match tr.dirty_delete(table_kv.key).await {
                            Err(e) => {
                                //删除有序B树表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除有序B树表的指定关键字的值成功
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
                    KVDBTransaction::BtreeOrdTabTr(tr) => {
                        //删除有序B树表的指定关键字的值
                        match tr.delete(table_kv.key).await {
                            Err(e) => {
                                //删除有序B树表的指定关键字的值错误，则立即返回错误原因
                                return Err(e);
                            },
                            Ok(value) => {
                                //删除有序B树表的指定关键字的值成功
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
                KVDBTransaction::BtreeOrdTabTr(tr) => {
                    //获取有序B树表的关键字的异步流
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
                KVDBTransaction::BtreeOrdTabTr(tr) => {
                    //获取有序B树表的键值对异步流
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
                KVDBTransaction::BtreeOrdTabTr(tr) => {
                    //锁住有序B树表的指定关键字
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
                KVDBTransaction::BtreeOrdTabTr(tr) => {
                    //解锁有序B树表的指定关键字
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
                Err(e)
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

    /// 异步预提交本次事务对键值对数据库的所有修改，成功返回预提交的输出，失败返回预提交冲突的首个表名和关键字
    #[inline]
    async fn prepare_modified_conflicts(&self) -> Result<Vec<u8>, KVTableTrError> {
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
            .prepare_conflicts(KVDBTransaction::RootTr(self.clone()))
            .await {
            Err(e) => {
                //预提交键值对数据库的根事务失败，则立即返回错误原因
                Err(e)
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
                KVDBTransaction::BtreeOrdTabTr(tr) => {
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

    /// 异步预提交本次快速修复修改，不返回预提交的输出。
    /// quick repair 只需要持久化表参与预提交，内存表必须跳过，避免恢复出不存在于重启后语义中的状态。
    async fn prepare_quick_repair(&self,
                                  transaction_uid: Guid)
                                  -> Result<QuickRepairPrepareBreakdown, KVTableTrError> {
        let mut breakdown = QuickRepairPrepareBreakdown::default();
        let mut childs = self.to_children();
        while let Some(child) = childs.next() {
            match &child {
                KVDBTransaction::MetaTabTr(tr) => {
                    let begin = Instant::now();
                    tr.prepare_quick_repair(transaction_uid.clone());
                    breakdown.meta_elapsed_us += begin.elapsed().as_micros();
                },
                KVDBTransaction::MemOrdTabTr(_) => {
                    continue;
                },
                KVDBTransaction::LogOrdTabTr(tr) => {
                    let begin = Instant::now();
                    tr.prepare_quick_repair(transaction_uid.clone());
                    breakdown.log_ord_elapsed_us += begin.elapsed().as_micros();
                },
                KVDBTransaction::LogWTabTr(tr) => {
                    let begin = Instant::now();
                    tr.prepare_quick_repair(transaction_uid.clone());
                    breakdown.log_write_elapsed_us += begin.elapsed().as_micros();
                },
                KVDBTransaction::BtreeOrdTabTr(tr) => {
                    let begin = Instant::now();
                    tr.prepare_quick_repair(transaction_uid.clone());
                    breakdown.btree_elapsed_us += begin.elapsed().as_micros();
                },
                KVDBTransaction::RootTr(_) => {
                    continue;
                }
            }
        }

        Ok(breakdown)
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
    BtreeOrdTab(BtreeOrderedTable<C, Log>), //有序B树表
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
            Self::BtreeOrdTab(tab) => tab.is_persistent(),
        }
    }
}

// 数据库跟踪循环
#[cfg(feature = "trace")]
async fn loop_tracing<R, C, Log>(rt: R,
                                 db_mgr: KVDBManager<C, Log>,
                                 interval: usize)
    where R: AsyncRuntime,
          C: Clone + Send + 'static,
          Log: AsyncCommitLog<C = C, Cid = Guid>,
{
    let table_cache_meter = get_table_cache_size_meter(rt.clone())
        .await
        .u64_gauge("pi_db.db.table_cache_size")
        .build();
    loop {
        rt.timeout(interval).await;
        let now = Instant::now();
        for table in db_mgr.tables().await {
            if let Some(size) = db_mgr.table_cache_size(&table).await {
                table_cache_meter.record(size, &[KeyValue::new("table",
                                                               table.as_str().to_string())]);
                rt.timeout(0).await;
            }
        }
        info!("Loop tracing succeeded, interval: {:?}ms, time: {:?}",
            interval,
            now.elapsed());
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
