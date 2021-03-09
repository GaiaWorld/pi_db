use std::collections::{BTreeMap, LinkedList, VecDeque};
use std::env;
use std::fs;
use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::Instant;

use async_file::file::{AsyncFile, AsyncFileOptions};
use atom::Atom;
use guid::Guid;
use hash::{XHashMap, XHashSet};
use ordmap::asbtree::Tree;
use ordmap::ordmap::{Entry, Iter as OIter, Keys, OrdMap};
use pi_store::log_store::log_file::{
    read_log_block, read_log_paths, LogFile, LogMethod, PairLoader,
};
use r#async::lock::mutex_lock::Mutex;
use r#async::lock::rw_lock::RwLock;
use r#async::lock::spin_lock::SpinLock;
use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use r#async::rt::{AsyncRuntime, AsyncValue};

use crate::db::BuildDbType;
use crate::db::{
    Bin, Bon, CommitResult, DBResult, Event, Filter, Iter, IterResult, KeyIterResult, NextResult,
    RwLog, SResult, TabKV, TabMeta, TxState,
};
use crate::fork::{build_fork_chain, TableMetaInfo, ALL_TABLES};
use crate::tabs::TxnType;
use crate::tabs::{Prepare, TabLog, Tabs};
use bon::{Decode, Encode, ReadBuffer, WriteBuffer};

lazy_static! {
    pub static ref STORE_RUNTIME: Arc<RwLock<Option<MultiTaskRuntime<()>>>> =
        Arc::new(RwLock::new(None));
    static ref LOG_FILE_TABS: Arc<RwLock<XHashMap<Atom, LogFileTab>>> =
        Arc::new(RwLock::new(XHashMap::default()));
    pub static ref LOG_FILE_SIZE: AtomicUsize = AtomicUsize::new(200);
    pub static ref LOG_FILE_TOTAL_SIZE: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
}

pub const DB_META_TAB_NAME: &'static str = "tabs_meta";

/**
* 基于file log的数据库
*/
#[derive(Clone)]
pub struct LogFileDB(Tabs);

impl LogFileDB {
    /**
    	* 构建基于file log的数据库
    	* @param db_path 数据库路径
    	* @param db_size 数据库文件最大大小
    	* @returns 返回基于file log的数据库
    	*/
    pub async fn new(db_path: Atom, db_size: usize) -> Self {
        if !Path::new(&db_path.to_string()).exists() {
            let _ = fs::create_dir(db_path.to_string());
        }

        // 从元信息表加载所有表元信息
        let db_path = env::var("DB_PATH").unwrap_or("./".to_string());
        let mut path = PathBuf::new();
        path.push(db_path.clone());
        path.push(DB_META_TAB_NAME);

        let file = match AsyncLogFileStore::open(
            path,
            8000,
            LOG_FILE_SIZE.load(Ordering::Relaxed) * 1024 * 1024,
            None,
        )
        .await
        {
            Err(e) => {
                panic!("!!!!!!open table = {:?} failed, e: {:?}", "tabs_meta", e);
            }
            Ok(store) => store,
        };

        let mut store = AsyncLogFileStore(Arc::new(AsyncLogFileStoreInner {
            removed: SpinLock::new(XHashMap::default()),
            map: SpinLock::new(BTreeMap::new()),
            log_file: file.clone(),
            tmp_map: SpinLock::new(XHashMap::default()),
            writable_path: SpinLock::new(None),
            is_statistics: AtomicBool::new(false),
            is_init: AtomicBool::new(true),
            statistics: SpinLock::new(VecDeque::new()),
        }));

        file.load(&mut store, None, false).await;
        store.0.is_init.store(false, Ordering::SeqCst);

        let mut tabs = Tabs::new();

        let map = store.0.map.lock();
        let rt = STORE_RUNTIME.read().await.as_ref().unwrap().clone();
        let mut async_map = rt.map();
        let start = std::time::Instant::now();
        let mut count = 0;
        for (k, v) in map.iter() {
            let tab_name = Atom::decode(&mut ReadBuffer::new(k, 0)).unwrap();
            let meta = TableMetaInfo::decode(&mut ReadBuffer::new(v.clone().to_vec().as_ref(), 0))
                .unwrap();
            tabs.set_tab_meta(tab_name.clone(), meta.meta.clone()).await;
            ALL_TABLES.lock().await.insert(tab_name.clone(), meta);

            async_map.join(AsyncRuntime::Multi(rt.clone()), async move {
                Ok((tab_name.clone(), LogFileTab::new(&tab_name, &vec![]).await))
            });
        }

        // 等待所有表加载完成
        match async_map.map(AsyncRuntime::Multi(rt.clone())).await {
            Ok(res) => {
                for r in res {
                    count += 1;
                    match r {
                        Ok((tab_name, logfiletab)) => {
                            LOG_FILE_TABS.write().await.insert(tab_name, logfiletab);
                        }
                        Err(e) => {
                            panic!("load tab error {:?}", e);
                        }
                    }
                }
            }
            Err(e) => {
                panic!("load tab erorr: {:?}", e)
            }
        }

        info!(
            "total tabs: {:?}, time: {:?}, {} KB",
            count,
            start.elapsed(),
            format!(
                "{0} {1:.2}",
                "total size",
                LOG_FILE_TOTAL_SIZE.load(Ordering::Relaxed) as f64 / 1024.0
            )
        );

        LogFileDB(tabs)
    }

    pub async fn open(tab: &Atom) -> SResult<LogFileTab> {
        let chains = build_fork_chain(tab.clone()).await;
        match LOG_FILE_TABS.read().await.get(tab) {
            Some(t) => Ok(t.clone()),
            None => Ok(LogFileTab::new(tab, &chains).await),
        }
    }

    pub async fn force_split() -> SResult<()> {
        let meta = LogFileDB::open(&Atom::from(DB_META_TAB_NAME))
            .await
            .unwrap();
        let map = meta.1 .0.map.lock().clone();

        for (key, _) in map.iter() {
            let tab_name = Atom::decode(&mut ReadBuffer::new(key, 0)).unwrap();
            let mut file = LogFileDB::open(&tab_name).await.unwrap();
            file.1 .0.log_file.split().await;
        }

        Ok(())
    }

    //异步整理所有LogFileTab
    pub async fn collect() -> SResult<()> {
        //获取LogFileDB的元信息
        let meta = LogFileDB::open(&Atom::from(DB_META_TAB_NAME))
            .await
            .unwrap();
        let map = meta.1 .0.map.lock();

        //遍历LogFileDB中的所有LogFileTab
        for (key, _) in map.iter() {
            let tab_name = Atom::decode(&mut ReadBuffer::new(key, 0)).unwrap();
            let mut file = LogFileDB::open(&tab_name).await.unwrap();

            //从LogFileTab中，根据文件名从小到大的选择需要整理的只读日志文件
            let mut remove_logs = Vec::new();
            let mut collect_logs = Vec::new();
            let mut collected_logs = XHashMap::default();
            for (log_path, log_len, key_len) in file.1 .0.statistics.lock().iter() {
                if *key_len == 0 {
                    //当前只读日志文件中没有新的关键字，则准备移除当前只读日志文件，并继续选择下一个只读日志文件
                    remove_logs.push(log_path.clone());
                    collected_logs.insert(log_path.clone(), ());
                    continue;
                }

                let f = *log_len as f64 / *key_len as f64;
                if f < 2.0 {
                    //当前只读日志文件的关键字重复率未达限制，则立即停止选择，并准备整理已选择的只读日志文件
                    break; //TODO 后续还要判断分叉的分裂点，除了分裂点为最大的只读日志文件外，其它分裂点将无法选择作为整理的只读日志文件，至到对应分裂点的分叉表被删除...
                }

                //准备整理当前只读日志文件
                collect_logs.push(log_path.clone());
                collected_logs.insert(log_path.clone(), ());
            }

            //整理需要整理的只读日志文件
            if let Err(e) = file
                .1
                 .0
                .log_file
                .collect_logs(remove_logs, collect_logs, 1024 * 1024, false)
                .await
            {
                //整理指定的LogFileTab失败，则立即退出整理
                return Err(format!(
                    "Collect LogFileTab failed, tab: {}, reason: {:?}",
                    tab_name.as_str(),
                    e
                ));
            }

            //从LogFileTab中移除所有的只读日志文件统计信息
            file.1 .0.statistics.lock().clear();

            let collect_start_time = Instant::now();

            //清理加载时的移除缓冲和临时键值缓冲，并设置为不需要统计
            file.1 .0.removed.lock().clear();
            file.1 .0.tmp_map.lock().clear();
            file.1 .0.is_statistics.store(false, Ordering::Relaxed);

            //获取整理后LogFileTab中的所有有效日志文件路径列表
            if let Ok(mut log_paths) = read_log_paths(&file.1 .0.log_file).await {
                //从大到小的分析整理后的日志文件，并更新LogFileTab的统计信息
                let mut offset = None;
                let rt = STORE_RUNTIME.read().await.as_ref().unwrap().clone();
                while let Some(log_path) = log_paths.pop() {
                    let log_file = match AsyncFile::open(
                        rt.clone(),
                        log_path.clone(),
                        AsyncFileOptions::OnlyRead,
                    )
                    .await
                    {
                        Err(e) => {
                            //打开指定日志文件失败，则继续下一个日志文件的分析
                            error!(
                                "Statistic failed after collected, tab: {}, reason: {:?}",
                                tab_name.as_str(),
                                e
                            );
                            continue;
                        }
                        Ok(f) => f,
                    };

                    loop {
                        let mut logs = LinkedList::default();
                        match read_log_block(&rt, &log_path, &log_file, offset, false).await {
                            Err(e) => {
                                error!(
                                    "Statistic failed after collected, tab: {}, reason: {:?}",
                                    tab_name.as_str(),
                                    e
                                );
                            }
                            Ok((next, list)) => {
                                if next == 0 {
                                    //已读到日志文件头，则重置文件偏移
                                    offset = None;
                                } else {
                                    //更新日志文件位置，则返回当前日志文件的下一个日志块
                                    offset = Some(next);
                                }

                                logs = list;
                            }
                        }

                        //分析当前只读日志文件的日志块，并更新当前只读日志文件的统计信息
                        for (method, key, value) in logs {
                            if file.1.is_require(Some(&log_path), &key) {
                                //需要分析的关键字
                                file.1.load(Some(&log_path), method, key, value);
                            }
                        }

                        if offset.is_none() {
                            //继续下一个日志文件的读取
                            break;
                        }
                    }
                }
            }

            file.1 .0.tmp_map.lock().clear(); //清理临时键值缓冲区
            info!(
                "Collect LogFileTab ok, time: {:?}, tab: {}, Statistics: {:?}",
                Instant::now() - collect_start_time,
                tab_name.as_str(),
                &*file.1 .0.statistics.lock()
            );
        }

        return Ok(());
    }

    // 拷贝全部的表
    pub async fn tabs_clone(&self) -> Self {
        LogFileDB(self.0.clone_map())
    }
    // 列出全部的表
    pub async fn list(&self) -> Box<dyn Iterator<Item = Atom>> {
        Box::new(self.0.list())
    }
    // 获取该库对预提交后的处理超时时间, 事务会用最大超时时间来预提交
    pub fn timeout(&self) -> usize {
        TIMEOUT
    }
    // 表的元信息
    pub async fn tab_info(&self, tab_name: &Atom) -> Option<TabMeta> {
        self.0.get(tab_name).await
    }
    // 获取当前表结构快照
    pub async fn snapshot(&self) -> LogFileDBSnapshot {
        LogFileDBSnapshot(
            LogFileDB(self.0.clone_map()),
            Mutex::new(self.0.snapshot().await),
        )
    }
}

// 内存库快照
pub struct LogFileDBSnapshot(LogFileDB, Mutex<TabLog>);

impl LogFileDBSnapshot {
    // 列出全部的表
    pub async fn list(&self) -> Box<dyn Iterator<Item = Atom>> {
        Box::new(self.1.lock().await.list())
    }
    // 表的元信息
    pub async fn tab_info(&self, tab_name: &Atom) -> Option<TabMeta> {
        self.1.lock().await.get(tab_name)
    }
    // 检查该表是否可以创建
    pub fn check(&self, _tab: &Atom, _meta: &Option<TabMeta>) -> DBResult {
        Ok(())
    }
    // 新增 修改 删除 表
    pub async fn alter(&self, tab_name: &Atom, meta: Option<TabMeta>) {
        self.1.lock().await.alter(tab_name, meta)
    }
    // 创建指定表的表事务
    pub async fn tab_txn(&self, tab_name: &Atom, id: &Guid, writable: bool) -> SResult<TxnType> {
        self.1
            .lock()
            .await
            .build(BuildDbType::LogFileDB, tab_name, id, writable)
            .await
    }
    // 创建一个meta事务
    pub fn meta_txn(&self, _id: &Guid) -> LogFileMetaTxn {
        LogFileMetaTxn {
            alters: Arc::new(Mutex::new(XHashMap::default())),
        }
    }
    // 元信息的预提交
    pub async fn prepare(&self, id: &Guid) -> DBResult {
        (self.0).0.prepare(id, &mut *self.1.lock().await).await
    }
    // 元信息的提交
    pub async fn commit(&mut self, id: &Guid) {
        (self.0).0.commit(id).await
    }
    // 回滚
    pub async fn rollback(&self, id: &Guid) {
        (self.0).0.rollback(id).await
    }
    // 库修改通知
    pub fn notify(&self, _event: Event) {}
}

// 内存事务
pub struct FileMemTxn {
    id: Guid,
    writable: bool,
    tab: LogFileTab,
    root: BinMap,
    old: BinMap,
    rwlog: XHashMap<Bin, RwLog>,
    state: TxState,
}

// pub struct RefLogFileTxn(Mutex<FileMemTxn>);
pub struct RefLogFileTxn(usize);

unsafe impl Sync for RefLogFileTxn {}

impl FileMemTxn {
    //开始事务
    pub async fn new(tab: LogFileTab, id: &Guid, writable: bool) -> RefLogFileTxn {
        let root = tab.0.lock().await.root.clone();
        let txn = FileMemTxn {
            id: id.clone(),
            writable,
            root: root.clone(),
            tab,
            old: root,
            rwlog: XHashMap::default(),
            state: TxState::Ok,
        };
        return RefLogFileTxn(Box::into_raw(Box::new(txn)) as usize);
    }
    //获取数据
    pub async fn get(&mut self, key: Bin) -> Option<Bin> {
        match self.root.get(&Bon::new(key.clone())) {
            Some(v) => {
                if self.writable {
                    match self.rwlog.get(&key) {
                        Some(_) => (),
                        None => {
                            &mut self.rwlog.insert(key, RwLog::Read);
                            ()
                        }
                    }
                }

                return Some(v.clone());
            }
            None => return None,
        }
    }
    //插入/修改数据
    pub async fn upsert(&mut self, key: Bin, value: Bin) -> DBResult {
        self.root
            .upsert(Bon::new(key.clone()), value.clone(), false);
        self.rwlog
            .insert(key.clone(), RwLog::Write(Some(value.clone())));

        Ok(())
    }
    //删除
    pub async fn delete(&mut self, key: Bin) -> DBResult {
        self.root.delete(&Bon::new(key.clone()), false);
        self.rwlog.insert(key, RwLog::Write(None));

        Ok(())
    }

    //预提交
    pub async fn prepare_inner(&mut self) -> DBResult {
        let mut tab = self.tab.0.lock().await;
        //遍历事务中的读写日志
        for (key, rw_v) in self.rwlog.iter() {
            //检查预提交是否冲突
            match tab.prepare.try_prepare(key, rw_v) {
                Ok(_) => (),
                Err(s) => return Err(s),
            };
            //检查Tab根节点是否改变
            if tab.root.ptr_eq(&self.old) == false {
                let key = Bon::new(key.clone());
                match tab.root.get(&key) {
                    Some(r1) => match self.old.get(&key) {
                        Some(r2) if (r1 as *const Bin) == (r2 as *const Bin) => (),
                        _ => return Err(String::from("prepare conflicted value diff")),
                    },
                    _ => match self.old.get(&key) {
                        None => (),
                        _ => return Err(String::from("prepare conflicted old not None")),
                    },
                }
            }
        }
        let rwlog = mem::replace(
            &mut self.rwlog,
            XHashMap::with_capacity_and_hasher(0, Default::default()),
        );
        //写入预提交
        tab.prepare.insert(self.id.clone(), rwlog);

        return Ok(());
    }

    // 内部提交方法
    pub async fn commit_inner(&mut self) -> CommitResult {
        let mut lock = self.tab.0.lock().await;
        let logs = lock.prepare.remove(&self.id);
        let logs = match logs {
            Some(rwlog) => {
                let root_if_eq = lock.root.ptr_eq(&self.old);
                //判断根节点是否相等
                if !root_if_eq {
                    for (k, rw_v) in rwlog.iter() {
                        match rw_v {
                            RwLog::Read => (),
                            _ => {
                                let k = Bon::new(k.clone());
                                match rw_v {
                                    RwLog::Write(None) => {
                                        lock.root.delete(&k, false);
                                    }
                                    RwLog::Write(Some(v)) => {
                                        lock.root.upsert(k.clone(), v.clone(), false);
                                    }
                                    _ => (),
                                }
                            }
                        }
                    }
                } else {
                    lock.root = self.root.clone();
                }
                rwlog
            }
            None => return Err(String::from("error prepare null")),
        };

        let async_tab = self.tab.1.clone();

        let mut insert_pairs: Vec<(&[u8], &[u8])> = vec![];
        let mut delete_keys: Vec<&[u8]> = vec![];

        for (k, rw_v) in &logs {
            match rw_v {
                RwLog::Read => {}
                _ => match rw_v {
                    RwLog::Write(None) => {
                        delete_keys.push(k);
                    }
                    RwLog::Write(Some(v)) => {
                        insert_pairs.push((k, v));
                    }
                    _ => {}
                },
            }
        }

        if insert_pairs.len() > 0 {
            async_tab.write_batch(&insert_pairs).await;
        }

        if delete_keys.len() > 0 {
            async_tab.remove_batch(&delete_keys).await;
        }

        Ok(logs)
    }
    //回滚
    pub async fn rollback_inner(&mut self) -> DBResult {
        let mut tab = self.tab.0.lock().await;
        tab.prepare.remove(&self.id);

        Ok(())
    }

    /// 强制产生分裂
    async fn force_fork_inner(&self) -> Result<usize> {
        self.tab.1.clone().force_fork().await
    }

    pub async fn fork_prepare_inner(
        &self,
        ware: Atom,
        tab_name: Atom,
        fork_tab_name: Atom,
        meta: TabMeta,
    ) -> DBResult {
        // 检查元信息表中是否有重复的表名
        if let Some(_) = ALL_TABLES.lock().await.get(&fork_tab_name) {
            return Err("duplicate fork tab name in meta tab".to_string());
        }
        Ok(())
    }

    /// 执行真正的分裂
    pub async fn fork_commit_inner(
        &self,
        ware: Atom,
        tab_name: Atom,
        fork_tab_name: Atom,
        meta: TabMeta,
    ) -> DBResult {
        let index = match self.force_fork_inner().await {
            Ok(idx) => idx,
            Err(e) => return Err(e.to_string()),
        };
        println!("fork_index = {:?}", index);

        let mut tmi = TableMetaInfo::new(fork_tab_name.clone(), meta);
        tmi.parent = Some(tab_name.clone());

        tmi.parent_log_id = Some(index);
        tmi.parent = Some(tab_name.clone());

        let mut wb = WriteBuffer::new();
        tmi.encode(&mut wb);
        let mut wb1 = WriteBuffer::new();
        fork_tab_name.encode(&mut wb1);

        let db_path = env::var("DB_PATH").unwrap_or("./".to_string());

        ALL_TABLES.lock().await.insert(fork_tab_name, tmi);

        let mut path = PathBuf::new();
        path.push(db_path);
        path.push(DB_META_TAB_NAME);

        let file = match AsyncLogFileStore::open(
            path,
            8000,
            LOG_FILE_SIZE.load(Ordering::Relaxed) * 1024 * 1024,
            None,
        )
        .await
        {
            Err(e) => {
                panic!("!!!!!!open table = {:?} failed, e: {:?}", "tabs_meta", e);
            }
            Ok(store) => store,
        };

        let mut store = AsyncLogFileStore(Arc::new(AsyncLogFileStoreInner {
            removed: SpinLock::new(XHashMap::default()),
            map: SpinLock::new(BTreeMap::new()),
            log_file: file.clone(),
            tmp_map: SpinLock::new(XHashMap::default()),
            writable_path: SpinLock::new(None),
            is_statistics: AtomicBool::new(false),
            is_init: AtomicBool::new(true),
            statistics: SpinLock::new(VecDeque::new()),
        }));

        // 找到父表的元信息，将它的引用计数加一
        ALL_TABLES
            .lock()
            .await
            .entry(tab_name.clone())
            .and_modify(|tab| {
                println!("add ref_count tab_name = {:?}", tab_name);
                tab.ref_count += 1;
                let mut b = WriteBuffer::new();
                tab_name.encode(&mut b);

                let mut b2 = WriteBuffer::new();
                tab.encode(&mut b2);
                store.write(b.bytes, b2.bytes);
            });

        // 新创建的分叉表信息写入元信息表中
        // TODO: 错误处理
        store.write(wb1.bytes, wb.bytes).await;

        Ok(())
    }

    pub async fn fork_rollback_inner(&self) -> DBResult {
        // 已经分裂则无法实现回滚
        Ok(())
    }
}

impl RefLogFileTxn {
    fn get(&self) -> Box<FileMemTxn> {
        unsafe { Box::from_raw(self.0 as *mut _) }
    }

    // 获得事务的状态
    pub async fn get_state(&self) -> TxState {
        self.get().state.clone()
    }
    // 预提交一个事务
    pub async fn prepare(&self, _timeout: usize) -> DBResult {
        let mut txn = self.get();
        txn.state = TxState::Preparing;
        match txn.prepare_inner().await {
            Ok(()) => {
                txn.state = TxState::PreparOk;
                mem::forget(txn);
                return Ok(());
            }
            Err(e) => {
                txn.state = TxState::PreparFail;
                mem::forget(txn);
                return Err(e.to_string());
            }
        }
    }
    // 提交一个事务
    pub async fn commit(&self) -> CommitResult {
        let mut txn = self.get();
        txn.state = TxState::Committing;
        match txn.commit_inner().await {
            Ok(log) => {
                txn.state = TxState::Commited;
                // 提交成功，释放 Box<FileMemTxn>
                return Ok(log);
            }
            Err(e) => {
                txn.state = TxState::CommitFail;
                // 提交失败, 回滚
                mem::forget(txn);
                return Err(e.to_string());
            }
        }
    }
    // 回滚一个事务
    pub async fn rollback(&self) -> DBResult {
        let mut txn = self.get();
        txn.state = TxState::Rollbacking;
        match txn.rollback_inner().await {
            Ok(()) => {
                txn.state = TxState::Rollbacked;
                // 回滚成功，释放 Box<FileMemTxn>
                return Ok(());
            }
            Err(e) => {
                txn.state = TxState::RollbackFail;
                // 回滚失败, 同样释放 Box<FileMemTxn>
                return Err(e.to_string());
            }
        }
    }

    /// fork 预提交
    pub async fn fork_prepare(
        &self,
        ware: Atom,
        tab_name: Atom,
        fork_tab_name: Atom,
        meta: TabMeta,
    ) -> DBResult {
        let mut txn = self.get();
        let res = txn
            .fork_prepare_inner(ware, tab_name, fork_tab_name, meta)
            .await;
        mem::forget(txn);
        return res;
    }

    /// fork 提交
    pub async fn fork_commit(
        &self,
        ware: Atom,
        tab_name: Atom,
        fork_tab_name: Atom,
        meta: TabMeta,
    ) -> DBResult {
        let mut txn = self.get();
        match txn
            .fork_commit_inner(ware, tab_name, fork_tab_name, meta)
            .await
        {
            Ok(()) => {
                // 分叉提交成功，释放Box<FileMemTxn>
                return Ok(());
            }
            Err(e) => {
                // 分叉提交失败，回滚
                mem::forget(txn);
                return Err(e.to_string());
            }
        }
    }

    /// fork 回滚
    pub async fn fork_rollback(&self) -> DBResult {
        let mut txn = self.get();
        match txn.fork_rollback_inner().await {
            Ok(()) => {
                // 分叉回滚成功，释放Box<FileMemTxn>
                return Ok(());
            }
            Err(e) => {
                // 分叉回滚失败，同样释放Box<FileMemTxn>
                return Err(e.to_string());
            }
        }
    }

    /// 强制产生分裂
    pub async fn force_fork(&self) -> Result<usize> {
        let mut txn = self.get();
        match txn.force_fork_inner().await {
            Ok(index) => {
                // 分叉成功
                mem::forget(txn);
                return Ok(index);
            }
            Err(e) => {
                // 分叉失败， 释放Box<FileMemTxn>
                return Err(std::io::Error::new(ErrorKind::Other, "fork failed"));
            }
        }
    }

    // 键锁，key可以不存在，根据lock_time的值决定是锁还是解锁
    pub async fn key_lock(
        &self,
        _arr: Arc<Vec<TabKV>>,
        _lock_time: usize,
        _readonly: bool,
    ) -> DBResult {
        Ok(())
    }
    // 查询
    pub async fn query(
        &self,
        arr: Arc<Vec<TabKV>>,
        _lock_time: Option<usize>,
        _readonly: bool,
    ) -> SResult<Vec<TabKV>> {
        let mut txn = self.get();
        let mut value_arr = Vec::new();
        for tabkv in arr.iter() {
            let value = match txn.get(tabkv.key.clone()).await {
                Some(v) => Some(v),
                _ => None,
            };

            value_arr.push(TabKV {
                ware: tabkv.ware.clone(),
                tab: tabkv.tab.clone(),
                key: tabkv.key.clone(),
                index: tabkv.index.clone(),
                value,
            })
        }
        mem::forget(txn);
        Ok(value_arr)
    }
    // 修改，插入、删除及更新
    pub async fn modify(
        &self,
        arr: Arc<Vec<TabKV>>,
        _lock_time: Option<usize>,
        _readonly: bool,
    ) -> DBResult {
        let mut txn = self.get();
        let mut success = true;
        for tabkv in arr.iter() {
            if tabkv.value.is_none() {
                match txn.delete(tabkv.key.clone()).await {
                    Ok(_) => {}
                    Err(e) => {
                        success = false;
                        break;
                    }
                };
            } else {
                match txn
                    .upsert(tabkv.key.clone(), tabkv.value.clone().unwrap())
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        success = false;
                        break;
                    }
                };
            }
        }

        if success {
            mem::forget(txn);
            return Ok(());
        } else {
            // 修改失败，释放Box<FileMemTxn>
            return Err("modify error".to_string());
        }
    }
    // 迭代
    pub async fn iter(
        &self,
        tab: &Atom,
        key: Option<Bin>,
        descending: bool,
        filter: Filter,
    ) -> IterResult {
        let txn = self.get();
        let key = match key {
            Some(k) => Some(Bon::new(k)),
            None => None,
        };
        let key = match &key {
            &Some(ref k) => Some(k),
            None => None,
        };

        let root = txn.root.clone();
        let mem_iter = txn.root.iter(key, descending);
        mem::forget(txn);
        Ok(Box::new(MemIter::new(tab, root, mem_iter, filter)))
    }
    // 迭代
    pub async fn key_iter(
        &self,
        key: Option<Bin>,
        descending: bool,
        filter: Filter,
    ) -> KeyIterResult {
        let txn = self.get();
        let key = match key {
            Some(k) => Some(Bon::new(k)),
            None => None,
        };
        let key = match &key {
            &Some(ref k) => Some(k),
            None => None,
        };
        let root = txn.root.clone();
        let mem_key_iter = txn.root.keys(key, descending);
        let tab = txn.tab.0.lock().await.tab.clone();
        mem::forget(txn);
        Ok(Box::new(MemKeyIter::new(&tab, root, mem_key_iter, filter)))
    }
    // 索引迭代
    pub fn index(
        &self,
        _tab: &Atom,
        _index_key: &Atom,
        _key: Option<Bin>,
        _descending: bool,
        _filter: Filter,
    ) -> IterResult {
        Err("not implemeted".to_string())
    }
    // 表的大小
    pub async fn tab_size(&self) -> SResult<usize> {
        let txn = self.get();
        let size = txn.root.size();
        mem::forget(txn);
        Ok(size)
    }
}

//================================ 内部结构和方法
const TIMEOUT: usize = 100;

type BinMap = OrdMap<Tree<Bon, Bin>>;

// 内存表
struct MemeryTab {
    pub prepare: Prepare,
    pub root: BinMap,
    pub tab: Atom,
}

pub struct MemIter {
    _root: BinMap,
    _filter: Filter,
    point: usize,
}

impl Drop for MemIter {
    fn drop(&mut self) {
        unsafe { Box::from_raw(self.point as *mut <Tree<Bin, Bin> as OIter<'_>>::IterType) };
    }
}

impl MemIter {
    pub fn new<'a>(
        tab: &Atom,
        root: BinMap,
        it: <Tree<Bon, Bin> as OIter<'a>>::IterType,
        filter: Filter,
    ) -> MemIter {
        MemIter {
            _root: root,
            _filter: filter,
            point: Box::into_raw(Box::new(it)) as usize,
        }
    }
}

impl Iter for MemIter {
    type Item = (Bin, Bin);
    fn next(&mut self) -> Option<NextResult<Self::Item>> {
        let mut it =
            unsafe { Box::from_raw(self.point as *mut <Tree<Bin, Bin> as OIter<'_>>::IterType) };
        let r = Some(Ok(match it.next() {
            Some(&Entry(ref k, ref v)) => Some((k.clone(), v.clone())),
            None => None,
        }));
        mem::forget(it);
        r
    }
}

pub struct MemKeyIter {
    _root: BinMap,
    _filter: Filter,
    point: usize,
}

impl Drop for MemKeyIter {
    fn drop(&mut self) {
        unsafe { Box::from_raw(self.point as *mut Keys<'_, Tree<Bin, Bin>>) };
    }
}

impl MemKeyIter {
    pub fn new(
        tab: &Atom,
        root: BinMap,
        keys: Keys<'_, Tree<Bon, Bin>>,
        filter: Filter,
    ) -> MemKeyIter {
        MemKeyIter {
            _root: root,
            _filter: filter,
            point: Box::into_raw(Box::new(keys)) as usize,
        }
    }
}

impl Iter for MemKeyIter {
    type Item = Bin;
    fn next(&mut self) -> Option<NextResult<Self::Item>> {
        let it = unsafe { Box::from_raw(self.point as *mut Keys<'_, Tree<Bin, Bin>>) };
        let r = Some(Ok(
            match unsafe { Box::from_raw(self.point as *mut Keys<'_, Tree<Bin, Bin>>) }.next() {
                Some(k) => Some(k.clone()),
                None => None,
            },
        ));
        mem::forget(it);
        r
    }
}

#[derive(Clone)]
pub struct LogFileMetaTxn {
    alters: Arc<Mutex<XHashMap<Atom, Option<TabMeta>>>>,
}

impl LogFileMetaTxn {
    // 创建表、修改指定表的元数据
    pub async fn alter(&self, tab_name: &Atom, meta: Option<TabMeta>) -> DBResult {
        self.alters.lock().await.insert(tab_name.clone(), meta);
        Ok(())
    }
    // 快照拷贝表
    pub async fn snapshot(&self, _tab: &Atom, _from: &Atom) -> DBResult {
        Ok(())
    }
    // 修改指定表的名字
    pub async fn rename(&self, _tab: &Atom, _new_name: &Atom) -> DBResult {
        Ok(())
    }

    // 获得事务的状态
    pub async fn get_state(&self) -> TxState {
        TxState::Ok
    }
    // 预提交一个事务
    pub async fn prepare(&self, _timeout: usize) -> DBResult {
        Ok(())
    }
    // 提交一个事务
    pub async fn commit(&self) -> CommitResult {
        for (tab_name, meta) in self.alters.lock().await.iter() {
            if let Some(_) = ALL_TABLES.lock().await.get(tab_name) {
                return Err(format!("tab_name: {:?} exist", tab_name));
            }
            let mut kt = WriteBuffer::new();
            tab_name.clone().encode(&mut kt);
            let db_path = env::var("DB_PATH").unwrap_or("./".to_string());
            let mut path = PathBuf::new();
            path.push(db_path.clone());
            path.push(DB_META_TAB_NAME);

            let file = match AsyncLogFileStore::open(
                path,
                8000,
                LOG_FILE_SIZE.load(Ordering::Relaxed) * 1024 * 1024,
                None,
            )
            .await
            {
                Err(e) => {
                    panic!("!!!!!!open table = {:?} failed, e: {:?}", "tabs_meta", e);
                }
                Ok(store) => store,
            };

            let mut store = AsyncLogFileStore(Arc::new(AsyncLogFileStoreInner {
                removed: SpinLock::new(XHashMap::default()),
                map: SpinLock::new(BTreeMap::new()),
                log_file: file.clone(),
                tmp_map: SpinLock::new(XHashMap::default()),
                writable_path: SpinLock::new(None),
                is_statistics: AtomicBool::new(false),
                is_init: AtomicBool::new(true),
                statistics: SpinLock::new(VecDeque::new()),
            }));

            match meta {
                Some(m) => {
                    let mt = TabMeta::new(m.k.clone(), m.v.clone());
                    let tmi = TableMetaInfo::new(tab_name.clone(), mt);
                    let mut vt = WriteBuffer::new();
                    tmi.encode(&mut vt);

                    // 新创建的表加入ALL_TABLES的缓存
                    let meta_name = Atom::from(db_path + &DB_META_TAB_NAME);
                    ALL_TABLES.lock().await.insert(meta_name, tmi);

                    // 新创建表的元信息写入元信息表中
                    store.write(kt.bytes, vt.bytes).await;
                }
                None => {
                    let mut parent = None;
                    match ALL_TABLES.lock().await.get(&tab_name) {
                        Some(tab) => {
                            if tab.ref_count > 0 {
                                return Err(format!(
                                    "delete tab: {:?} failed, ref_count = {:?}",
                                    tab.tab_name, tab.ref_count
                                ));
                            } else {
                                store.remove(kt.bytes).await;
                                parent = tab.parent.clone();
                            }
                        }
                        None => return Err(format!("delete tab: {:?} not found", tab_name)),
                    }
                    ALL_TABLES.lock().await.remove(&tab_name);
                    // 找到他的父表，将父表的引用计数减一
                    let mut wb = WriteBuffer::new();
                    parent.clone().unwrap().encode(&mut wb);
                    ALL_TABLES
                        .lock()
                        .await
                        .entry(parent.clone().unwrap())
                        .and_modify(|t| {
                            t.ref_count -= 1;
                            let mut wb2 = WriteBuffer::new();
                            t.encode(&mut wb2);
                            store.write(wb.bytes, wb2.bytes);
                        });
                }
            }
        }
        Ok(XHashMap::with_capacity_and_hasher(0, Default::default()))
    }
    // 回滚一个事务
    pub async fn rollback(&self) -> DBResult {
        self.alters.lock().await.clear();
        Ok(())
    }
}

#[derive(Clone)]
pub struct AsyncLogFileStore(pub Arc<AsyncLogFileStoreInner>);

pub struct AsyncLogFileStoreInner {
    pub removed: SpinLock<XHashMap<Vec<u8>, ()>>,
    pub map: SpinLock<BTreeMap<Vec<u8>, Arc<[u8]>>>,
    pub log_file: LogFile,
    pub tmp_map: SpinLock<XHashMap<Vec<u8>, ()>>,
    pub writable_path: SpinLock<Option<PathBuf>>,
    pub is_statistics: AtomicBool,
    pub is_init: AtomicBool,
    pub statistics: SpinLock<VecDeque<(PathBuf, u64, u64)>>,
}

unsafe impl Send for AsyncLogFileStore {}
unsafe impl Sync for AsyncLogFileStore {}

impl PairLoader for AsyncLogFileStore {
    fn is_require(&self, log_file: Option<&PathBuf>, key: &Vec<u8>) -> bool {
        let b =
            !self.0.removed.lock().contains_key(key) && !self.0.tmp_map.lock().contains_key(key);

        if self.0.is_statistics.load(Ordering::Relaxed) {
            //需要统计
            let mut init = false;
            if !b {
                //已删除的记录，则不需要加载，但需要统计
                if let Some((path, log_len, key_len)) = self.0.statistics.lock().get_mut(0) {
                    if path.to_str().unwrap() == log_file.as_ref().unwrap().to_str().unwrap() {
                        //指定只读日志文件的统计信息存在，则继续累计
                        *log_len += 1;
                        if !self.0.tmp_map.lock().contains_key(key) {
                            //如果需要加载的关键字不存在，则累计关键字数量
                            *key_len += 1;
                        }
                    } else {
                        //指定只读日志文件的统计信息不存在，则初始化
                        init = true;
                    }
                } else {
                    init = true;
                };
            }

            if init {
                //当前没有任何统计信息，则初始化统计信息
                if !b {
                    //已删除的记录，则不需要加载，但需要统计
                    if self.0.tmp_map.lock().contains_key(key) {
                        //如果不需要加载的关键字已存在，则不累计关键字数量
                        self.0
                            .statistics
                            .lock()
                            .push_front((log_file.cloned().unwrap(), 1, 0));
                    } else {
                        //如果不需要加载的关键字不存在，则累计关键字数量
                        self.0
                            .statistics
                            .lock()
                            .push_front((log_file.cloned().unwrap(), 1, 1));
                    }
                } else {
                    //插入或更新的记录，需要加载，但不需要在判断是否加载时统计
                    self.0
                        .statistics
                        .lock()
                        .push_front((log_file.cloned().unwrap(), 0, 0));
                }
            }
        } else {
            if self.0.writable_path.lock().is_none() {
                //如果当前是可写日志文件，且未记录，则记录，并忽略统计
                *self.0.writable_path.lock() = log_file.cloned();
            } else {
                if self
                    .0
                    .writable_path
                    .lock()
                    .as_ref()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    != log_file.as_ref().unwrap().to_str().unwrap()
                {
                    //当前可写日志文件已记录，且开始加载只读日志文件，则设置为需要统计，并开始初始化统计信息
                    if !b {
                        //已删除的记录，则不需要加载，但需要统计
                        self.0
                            .statistics
                            .lock()
                            .push_front((log_file.cloned().unwrap(), 1, 1));
                    } else {
                        //插入或更新的记录，需要加载，但不需要在判断是否加载时统计
                        self.0
                            .statistics
                            .lock()
                            .push_front((log_file.cloned().unwrap(), 0, 0));
                    }

                    //设置为需要统计
                    self.0.is_statistics.store(true, Ordering::SeqCst);
                }
            }
        }

        b
    }

    fn load(
        &mut self,
        log_file: Option<&PathBuf>,
        method: LogMethod,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) {
        if self.0.is_statistics.load(Ordering::Relaxed) {
            //需要统计
            let mut init = false;
            if let Some((path, log_len, key_len)) = self.0.statistics.lock().get_mut(0) {
                if path.to_str().unwrap() == log_file.as_ref().unwrap().to_str().unwrap() {
                    //指定只读日志文件的统计信息存在，则继续累计
                    *log_len += 1;
                    if !self.0.tmp_map.lock().contains_key(&key) {
                        //如果需要加载的关键字不存在，则累计关键字数量
                        *key_len += 1;
                    }
                } else {
                    //指定只读日志文件的统计信息不存在，则初始化
                    init = true;
                }
            } else {
                init = true;
            };

            if init {
                //当前没有任何统计信息，则初始化统计信息
                if self.0.tmp_map.lock().contains_key(&key) {
                    //如果需要加载的关键字已存在，则不累计关键字数量
                    self.0
                        .statistics
                        .lock()
                        .push_front((log_file.cloned().unwrap(), 1, 0));
                } else {
                    //如果需要加载的关键字不存在，则累计关键字数量
                    self.0
                        .statistics
                        .lock()
                        .push_front((log_file.cloned().unwrap(), 1, 1));
                }
            }
        }

        if let Some(value) = value {
            if self.0.is_init.load(Ordering::Relaxed) {
                //启动初始化，才写入键值缓冲区
                self.0.map.lock().insert(key.clone(), value.into());
            }
            self.0.tmp_map.lock().insert(key, ());
        } else {
            self.0.removed.lock().insert(key, ());
        }
    }
}

impl AsyncLogFileStore {
    pub async fn open<P: AsRef<Path> + std::fmt::Debug>(
        path: P,
        buf_len: usize,
        file_len: usize,
        log_file_index: Option<usize>,
    ) -> Result<LogFile> {
        // println!("AsyncLogFileStore open ====== {:?}, log_index = {:?}", path, log_file_index);
        match LogFile::open(
            STORE_RUNTIME.read().await.as_ref().unwrap().clone(),
            path,
            buf_len,
            file_len,
            log_file_index,
        )
        .await
        {
            Err(e) => panic!("LogFile::open error {:?}", e),
            Ok(file) => Ok(file),
        }
    }

    pub async fn write_batch(&self, pairs: &[(&[u8], &[u8])]) -> Result<()> {
        let mut id = 0;
        for (key, value) in pairs {
            id = self.0.log_file.append(LogMethod::PlainAppend, key, value);
        }
        match self.0.log_file.delay_commit(id, false, 0).await {
            Ok(_) => {
                {
                    let mut map = self.0.map.lock();
                    for (key, value) in pairs {
                        map.insert(key.to_vec(), value.clone().into());
                    }
                }
                Ok(())
            }
            Err(e) => {
                println!("write batch error");
                Err(e)
            }
        }
    }

    pub async fn write(&self, key: Vec<u8>, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let id = self
            .0
            .log_file
            .append(LogMethod::PlainAppend, key.as_ref(), value.as_ref());
        if let Err(e) = self.0.log_file.delay_commit(id, false, 0).await {
            Err(e)
        } else {
            if let Some(value) = self.0.map.lock().insert(key, value.into()) {
                //更新指定key的存储数据，则返回更新前的存储数据
                Ok(Some(value.to_vec()))
            } else {
                Ok(None)
            }
        }
    }

    pub fn read(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        if let Some(value) = self.0.map.lock().get(key) {
            return Some(value.clone());
        }

        None
    }

    pub async fn remove_batch(&self, keys: &[&[u8]]) -> Result<()> {
        let mut id = 0;
        for key in keys {
            id = self.0.log_file.append(LogMethod::Remove, key, &[]);
        }

        match self.0.log_file.delay_commit(id, false, 0).await {
            Ok(_) => {
                for key in keys {
                    self.0.map.lock().remove(key.clone());
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn remove(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let id = self.0.log_file.append(LogMethod::Remove, key.as_ref(), &[]);
        if let Err(e) = self.0.log_file.delay_commit(id, false, 0).await {
            Err(e)
        } else {
            if let Some(value) = self.0.map.lock().remove(&key) {
                Ok(Some(value.to_vec()))
            } else {
                Ok(None)
            }
        }
    }

    pub fn last_key(&self) -> Option<Vec<u8>> {
        self.0.map.lock().iter().last().map(|(k, _)| k.clone())
    }

    /// 强制产生分裂
    pub async fn force_fork(&self) -> Result<usize> {
        self.0.log_file.split().await
    }
}

#[derive(Clone)]
pub struct LogFileTab(Arc<Mutex<MemeryTab>>, pub AsyncLogFileStore);

unsafe impl Send for LogFileTab {}
unsafe impl Sync for LogFileTab {}

impl LogFileTab {
    async fn new(tab: &Atom, chains: &[TableMetaInfo]) -> Self {
        let mut file_mem_tab = MemeryTab {
            prepare: Prepare::new(XHashMap::with_capacity_and_hasher(0, Default::default())),
            root: OrdMap::<Tree<Bon, Bin>>::new(None),
            tab: tab.clone(),
        };

        let mut path = PathBuf::new();
        let db_path = env::var("DB_PATH").unwrap_or(".".to_string());
        path.push(db_path);
        let tab_name = tab.clone();
        let tab_name_clone = tab.clone();
        path.push(tab_name.clone().to_string());

        let mut log_file_id = None;
        // 首先加载叶子节点数据
        let log_file_index = if chains.len() > 0 {
            log_file_id = chains[0].parent_log_id;
            chains[0].parent_log_id
        } else {
            None
        };
        // println!("LogFileTab::new  log_file_index = {:?}, tab = {:?}, chains = {:?}", log_file_index, tab, chains);
        let file = match AsyncLogFileStore::open(
            path.clone(),
            8000,
            LOG_FILE_SIZE.load(Ordering::Relaxed) * 1024 * 1024,
            log_file_index,
        )
        .await
        {
            Err(e) => panic!("!!!!!!open table = {:?} failed, e: {:?}", tab_name, e),
            Ok(file) => file,
        };

        let mut store = AsyncLogFileStore(Arc::new(AsyncLogFileStoreInner {
            removed: SpinLock::new(XHashMap::default()),
            map: SpinLock::new(BTreeMap::new()),
            log_file: file.clone(),
            tmp_map: SpinLock::new(XHashMap::default()),
            writable_path: SpinLock::new(None),
            is_statistics: AtomicBool::new(false),
            is_init: AtomicBool::new(true),
            statistics: SpinLock::new(VecDeque::new()),
        }));

        file.load(&mut store, Some(path), false).await;
        let mut load_size = 0;
        let map = store.0.map.lock();
        for (k, v) in map.iter() {
            load_size += k.len() + v.len();
            file_mem_tab
                .root
                .upsert(Bon::new(Arc::new(k.clone())), Arc::new(v.to_vec()), false);
        }
        store.0.is_init.store(false, Ordering::SeqCst);
        LOG_FILE_TOTAL_SIZE.fetch_add(load_size as u64, Ordering::Relaxed);
        info!(
            "load tab: {} {} KB",
            tab_name_clone.as_str(),
            format!("{0} {1:.2}", "size", load_size as f64 / 1024.0)
        );

        // 再加载分叉路径中的表的数据
        for tm in chains.iter().skip(1) {
            let file = match AsyncLogFileStore::open(
                tm.tab_name.as_ref(),
                8000,
                LOG_FILE_SIZE.load(Ordering::Relaxed) * 1024 * 1024,
                tm.parent_log_id,
            )
            .await
            {
                Err(e) => panic!("!!!!!!open table = {:?} failed, e: {:?}", tm.parent, e),
                Ok(file) => file,
            };
            let mut store = AsyncLogFileStore(Arc::new(AsyncLogFileStoreInner {
                removed: SpinLock::new(XHashMap::default()),
                map: SpinLock::new(BTreeMap::new()),
                log_file: file.clone(),
                tmp_map: SpinLock::new(XHashMap::default()),
                writable_path: SpinLock::new(None),
                is_statistics: AtomicBool::new(false),
                is_init: AtomicBool::new(true),
                statistics: SpinLock::new(VecDeque::new()),
            }));

            let mut path = PathBuf::new();
            path.push(tm.tab_name.clone().as_ref());
            path.push(format!("{:0>width$}", log_file_id.unwrap() - 1, width = 6));
            file.load(&mut store, Some(path), false).await;

            let mut load_size = 0;
            let start_time = Instant::now();
            let map = store.0.map.lock();
            for (k, v) in map.iter() {
                load_size += k.len() + v.len();
                file_mem_tab.root.upsert(
                    Bon::new(Arc::new(k.clone())),
                    Arc::new(v.to_vec()),
                    false,
                );
            }
            log_file_id = tm.parent_log_id;
            store.0.is_init.store(false, Ordering::SeqCst);
            debug!(
                "====> load tab: {:?} size: {:?}byte time elapsed: {:?} <====",
                tm.tab_name,
                load_size,
                start_time.elapsed()
            );
        }

        return LogFileTab(Arc::new(Mutex::new(file_mem_tab)), store);
    }

    pub async fn transaction(&self, id: &Guid, writable: bool) -> RefLogFileTxn {
        FileMemTxn::new(self.clone(), id, writable).await
    }
}
