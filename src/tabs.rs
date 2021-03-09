/**
 * 表管理器，给每个具体的Ware用
 */
use std::mem;
use std::ops::{Deref, DerefMut};
use std::{array::IntoIter, sync::Arc};

use atom::Atom;
use guid::Guid;
use hash::{XHashMap, XHashSet};
use ordmap::asbtree::{new, Tree};
use ordmap::ordmap::{ActionResult, Keys, OrdMap};
use r#async::lock::mutex_lock::Mutex;
use r#async::lock::rw_lock::RwLock;

use crate::log_file_db::LogFileDB;
use crate::log_file_db::{LogFileTab, RefLogFileTxn};
use crate::memery_db::MemDB;
use crate::memery_db::{MTab, RefMemeryTxn};
use crate::{
    db::{Bin, BuildDbType, DBResult, RwLog, SResult, TabMeta},
    log_file_db::MemIter,
};

pub enum TxnType {
    MemTxn(RefMemeryTxn),
    LogFileTxn(RefLogFileTxn),
}
// 表结构及修改日志
pub struct TabLog {
    map: OrdMap<Tree<Atom, TabInfo>>,
    old_map: OrdMap<Tree<Atom, TabInfo>>, // 用于判断mgr中tabs是否修改过
    meta_names: XHashSet<Atom>,           //元信息表的名字
    alter_logs: XHashMap<(Atom, usize), Option<TabMeta>>, // 记录每个被改过元信息的表
    rename_logs: XHashMap<Atom, (Atom, usize)>, // 新名字->(源名字, 版本号)
}
impl TabLog {
    // 列出全部的表
    pub fn list(&self) -> TabIter {
        TabIter::new(self.map.clone(), self.map.keys(None, false))
    }
    // 获取指定的表结构
    pub fn get(&self, tab: &Atom) -> Option<TabMeta> {
        match self.map.get(tab) {
            Some(t) => Some(t.inner.meta.clone()),
            _ => None,
        }
    }
    // 表的元信息
    fn replace(&mut self) -> Self {
        TabLog {
            map: self.map.clone(),
            old_map: self.old_map.clone(),
            meta_names: mem::replace(
                &mut self.meta_names,
                XHashSet::with_capacity_and_hasher(0, Default::default()),
            ),
            alter_logs: mem::replace(
                &mut self.alter_logs,
                XHashMap::with_capacity_and_hasher(0, Default::default()),
            ),
            rename_logs: mem::replace(
                &mut self.rename_logs,
                XHashMap::with_capacity_and_hasher(0, Default::default()),
            ),
        }
    }
    // 表的元信息
    pub fn tab_info(&self, tab_name: &Atom) -> Option<TabMeta> {
        match self.map.get(tab_name) {
            Some(info) => Some(info.inner.meta.clone()),
            _ => None,
        }
    }
    // 新增 修改 删除 表
    pub fn alter(&mut self, tab_name: &Atom, meta: Option<TabMeta>) {
        // 先查找rename_logs，获取该表的源名字及版本，然后修改alter_logs
        let (src_name, ver) = match self.rename_logs.get(tab_name) {
            Some(v) => v.clone(),
            _ => (tab_name.clone(), 0),
        };
        let mut f = |v: Option<&TabInfo>| match v {
            Some(ti) => match &meta {
                Some(si) => ActionResult::Upsert(ti.clone()),
                _ => ActionResult::Delete,
            },
            None => match &meta {
                Some(si) => ActionResult::Upsert(TabInfo::new(si.clone())),
                _ => ActionResult::Ignore,
            },
        };
        self.map.action(&src_name, &mut f);
        self.alter_logs.entry((src_name, ver)).or_insert(meta);
        self.meta_names.insert(tab_name.clone());
    }
    // 创建表事务
    pub async fn build(
        &self,
        ware: BuildDbType,
        tab_name: &Atom,
        id: &Guid,
        writable: bool,
    ) -> SResult<TxnType> {
        match self.map.get(tab_name) {
            Some(ref info) => {
                let tab = {
                    let mut var = info.inner.init.lock().await;
                    match var.wait {
                        Some(ref mut vec) => {
                            // 表尚未build
                            if *vec {
                                // 第一次调用
                                match ware {
                                    BuildDbType::MemoryDB => match MemDB::open(tab_name).await {
                                        Ok(t) => {
                                            var.tab_type = TabType::MemTab(t);
                                            var.wait = None;
                                            var.tab_type.clone()
                                        }

                                        Err(e) => return Err(e),
                                    },
                                    BuildDbType::LogFileDB => {
                                        match LogFileDB::open(tab_name).await {
                                            Ok(t) => {
                                                var.tab_type = TabType::LogFileTab(t);
                                                var.wait = None;
                                                var.tab_type.clone()
                                            }
                                            Err(e) => return Err(e),
                                        }
                                    }
                                }
                            } else {
                                return Err("unreachable branch".to_string());
                            }
                        }
                        _ => var.tab_type.clone(),
                    }
                };
                // 根据结果创建事务或返回错误
                match tab {
                    TabType::MemTab(t) => Ok(TxnType::MemTxn(t.transaction(&id, writable).await)),
                    TabType::LogFileTab(t) => {
                        Ok(TxnType::LogFileTxn(t.transaction(&id, writable).await))
                    }
                    TabType::Unkonwn => Err(String::from("unknown tab type")),
                }
            }
            _ => Err(String::from("TabNotFound: ") + (*tab_name).as_str()),
        }
    }
}

pub struct Prepare(XHashMap<Guid, XHashMap<Bin, RwLog>>);

impl Prepare {
    pub fn new(map: XHashMap<Guid, XHashMap<Bin, RwLog>>) -> Prepare {
        Prepare(map)
    }

    //检查预提交是否冲突（如果预提交表中存在该条目，且其类型为write， 同时，本次预提交类型也为write， 即预提交冲突）
    pub fn try_prepare(&self, key: &Bin, log_type: &RwLog) -> Result<(), String> {
        for o_rwlog in self.0.values() {
            match o_rwlog.get(key) {
                Some(RwLog::Read) => match log_type {
                    RwLog::Read => return Ok(()),
                    RwLog::Write(_) => {
                        debug!("expect read log type, found write log type");
                        return Err(String::from("prepare conflicted rw"));
                    }
                    RwLog::Meta(_) => {
                        debug!("expect read log type, found meta log type");
                        return Err(String::from("unexpected meta log type1"));
                    }
                },
                Some(RwLog::Write(_)) => match log_type {
                    RwLog::Read => {
                        debug!("previous read log exist, key = {:?}", key);
                        return Err(String::from("previous read log exist"));
                    }
                    RwLog::Write(_) => {
                        debug!("previous write log exist, key = {:?}", key);
                        return Err(String::from("previous write log exist"));
                    }
                    RwLog::Meta(_) => {
                        debug!("previous meta log exist, key = {:?}", key);
                        return Err(String::from("previous meta log exist"));
                    }
                },
                Some(RwLog::Meta(_)) => {
                    return Err(String::from("unexpected meta log type2"));
                }
                None => return Ok(()),
            }
        }

        Ok(())
    }
}

impl Deref for Prepare {
    type Target = XHashMap<Guid, XHashMap<Bin, RwLog>>;

    fn deref(&self) -> &XHashMap<Guid, XHashMap<Bin, RwLog>> {
        &self.0
    }
}

impl DerefMut for Prepare {
    fn deref_mut(&mut self) -> &mut XHashMap<Guid, XHashMap<Bin, RwLog>> {
        &mut self.0
    }
}

pub struct TabIter {
    root: OrdMap<Tree<Atom, TabInfo>>,
    point: usize,
}

impl Drop for TabIter {
    fn drop(&mut self) {
        unsafe { Box::from_raw(self.point as *mut Keys<Tree<Atom, TabInfo>>) };
    }
}

impl<'a> TabIter {
    pub fn new(root: OrdMap<Tree<Atom, TabInfo>>, it: Keys<'a, Tree<Atom, TabInfo>>) -> TabIter {
        TabIter {
            root: root.clone(),
            point: Box::into_raw(Box::new(it)) as usize,
        }
    }
}

impl Iterator for TabIter {
    type Item = Atom;
    fn next(&mut self) -> Option<Self::Item> {
        let mut it = unsafe { Box::from_raw(self.point as *mut Keys<Tree<Atom, TabInfo>>) };
        let r = match it.next() {
            Some(k) => Some(k.clone()),
            None => None,
        };
        mem::forget(it);
        r
    }
}

// 表管理器
#[derive(Clone)]
pub struct Tabs {
    //全部的表结构
    map: OrdMap<Tree<Atom, TabInfo>>,
    // 预提交的元信息事务表
    prepare: Arc<Mutex<XHashMap<Guid, TabLog>>>,
}

impl Tabs {
    pub fn new() -> Self {
        Tabs {
            map: OrdMap::new(new()),
            prepare: Arc::new(Mutex::new(XHashMap::with_capacity_and_hasher(
                0,
                Default::default(),
            ))),
        }
    }

    pub fn clone_map(&self) -> Self {
        Tabs {
            map: self.map.clone(),
            prepare: Arc::new(Mutex::new(XHashMap::with_capacity_and_hasher(
                0,
                Default::default(),
            ))),
        }
    }

    // 列出全部的表
    pub fn list(&self) -> TabIter {
        TabIter::new(self.map.clone(), self.map.keys(None, false))
    }
    // 获取指定的表结构
    pub async fn get(&self, tab: &Atom) -> Option<TabMeta> {
        match self.map.get(tab) {
            Some(t) => Some(t.inner.meta.clone()),
            _ => None,
        }
    }
    // 获取当前表结构快照
    pub async fn snapshot(&self) -> TabLog {
        let map = self.map.clone();
        TabLog {
            map: map.clone(),
            old_map: map,
            meta_names: XHashSet::with_capacity_and_hasher(0, Default::default()),
            alter_logs: XHashMap::with_capacity_and_hasher(0, Default::default()),
            rename_logs: XHashMap::with_capacity_and_hasher(0, Default::default()),
        }
    }
    // 获取表的元信息
    pub async fn get_tab_meta(&self, tab: &Atom) -> Option<TabMeta> {
        match self.map.get(tab) {
            Some(info) => Some(info.inner.meta.clone()),
            _ => None,
        }
    }
    // 设置表的元信息
    pub async fn set_tab_meta(&mut self, tab: Atom, meta: TabMeta) -> bool {
        self.map.insert(tab, TabInfo::new(meta))
    }

    // 预提交
    pub async fn prepare(&self, id: &Guid, log: &mut TabLog) -> DBResult {
        // 先检查预提交的交易是否有冲突
        for val in self.prepare.lock().await.values() {
            if !val.meta_names.is_disjoint(&log.meta_names) {
                return Err(String::from("meta prepare conflicting"));
            }
        }
        // 然后检查数据表是否被修改
        if !self.map.ptr_eq(&log.old_map) {
            // 如果被修改，则检查是否有冲突
            // TODO 暂时没有考虑重命名的情况
            for name in log.meta_names.iter() {
                match self.map.get(name) {
                    Some(r1) => match log.old_map.get(name) {
                        Some(r2) if (r1 as *const TabInfo) == (r2 as *const TabInfo) => (),
                        _ => return Err(String::from("meta prepare conflicted")),
                    },
                    _ => match log.old_map.get(name) {
                        None => (),
                        _ => return Err(String::from("meta prepare conflicted")),
                    },
                }
            }
        }
        self.prepare.lock().await.insert(id.clone(), log.replace());
        Ok(())
    }
    // 元信息的提交
    pub async fn commit(&mut self, id: &Guid) {
        match self.prepare.lock().await.remove(id) {
            Some(log) => {
                if self.map.ptr_eq(&log.old_map) {
                    // 检查数据表是否被修改， 如果没有修改，则可以直接替换根节点
                    self.map = log.map;
                } else {
                    // 否则，重新执行一遍修改
                    for r in log.alter_logs {
                        if r.1.is_none() {
                            self.map.delete(&Atom::from((r.0).0.as_ref()), false);
                        } else {
                            let tab_info = TabInfo::new(r.1.unwrap());
                            self.map
                                .upsert(Atom::from((r.0).0.as_ref()), tab_info, false);
                        }
                    }
                }
            }
            _ => (),
        }
    }
    // 回滚
    pub async fn rollback(&self, id: &Guid) {
        self.prepare.lock().await.remove(id);
    }
}
//================================ 内部结构和方法
// 表信息
#[derive(Clone)]
pub struct TabInfo {
    inner: Arc<TabInfoInner>,
}

struct TabInfoInner {
    meta: TabMeta,
    init: Mutex<TabInit>,
}

impl TabInfo {
    fn new(meta: TabMeta) -> Self {
        TabInfo {
            inner: Arc::new(TabInfoInner {
                meta,
                init: Mutex::new(TabInit {
                    tab_type: TabType::Unkonwn,
                    wait: Some(true),
                }),
            }),
        }
    }
}
// 表初始化
struct TabInit {
    tab_type: TabType,
    wait: Option<bool>, // 为None表示tab已经加载
}

#[derive(Clone)]
enum TabType {
    Unkonwn,
    MemTab(MTab),
    LogFileTab(LogFileTab),
}
