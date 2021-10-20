/**
 * 表管理器，给每个具体的Ware用
 */
use std::sync::{Arc};
use std::mem;
use std::ops::{DerefMut, Deref};

use hash::{XHashMap, XHashSet};
use ordmap::ordmap::{OrdMap, ActionResult, Keys};
use ordmap::asbtree::{Tree, new};
use atom::Atom;
use guid::Guid;
use r#async::lock::mutex_lock::Mutex;
use r#async::lock::rw_lock::RwLock;
use time::now_nanosecond;


use crate::{db::{SResult, Bin, RwLog, TabMeta, BuildDbType, DBResult}, log_file_db::MemIter};
use crate::memery_db::MemDB;
use crate::memery_db::{ MTab, RefMemeryTxn };
use crate::log_file_db::{LogFileTab, RefLogFileTxn};
use crate::log_file_db::LogFileDB;

/*
* 表事务类型
*/
pub enum TxnType {
	MemTxn(Arc<RefMemeryTxn>),		//内存表事务
	LogFileTxn(Arc<RefLogFileTxn>)	//日志文件表事务
}

/*
* 元信息表的表修改日志，元信息表一定是日志文件类型的表
*/
pub struct TabLog {
	map: OrdMap<Tree<Atom, TabInfo>>,							//表名和表信息，在创建元信息表的事务的snapshot中初始化，对元信息表进行事务操作时，用于记录表元信息事务中对表元信息的修改
	old_map: OrdMap<Tree<Atom, TabInfo>>, 						//表名和表信息，在创建元信息表的事务的snapshot中初始化，用于在表元信息事务的预提交和提交时的判断
	meta_names: XHashSet<Atom>, 								//所有表(不是元信息表)的名称
	alter_logs: XHashMap<(Atom, usize), Option<Arc<TabMeta>>>, 	//创建表的日志，记录了表(不是元信息表)的名称，版本号(暂时未使用)和表的元信息
	rename_logs: XHashMap<Atom, (Atom, usize)>, 				//重命名表的日志，记录了源表的别名，源表的名称和版本号
}

impl TabLog {
	//获取所有表的表名迭代器
	pub fn list(&self) -> TabIter {
		TabIter::new(Arc::new(RwLock::new(self.map.clone())), self.map.keys(None, false))
	}

	//获取指定表的元信息，没有导出到js层
	//TODO get和tab_info只需要保留一个...
	pub fn get(&self, tab: &Atom) -> Option<Arc<TabMeta>> {
		match self.map.get(tab) {
			Some(t) => Some(t.meta.clone()),
			_ => None
		}
	}

	//替换元信息表操作日志中的所有表名称、创建表的日志和重命名表的日志，以减少内存占用
	fn replace(&mut self) -> Self {
		TabLog {
			map: self.map.clone(),
			old_map: self.old_map.clone(),
			meta_names: mem::replace(&mut self.meta_names, XHashSet::with_capacity_and_hasher(0, Default::default())),
			alter_logs: mem::replace(&mut self.alter_logs, XHashMap::with_capacity_and_hasher(0, Default::default())),
			rename_logs: mem::replace(&mut self.rename_logs, XHashMap::with_capacity_and_hasher(0, Default::default())),
		}
	}

	//获取指定表的元信息，导出到js层
	//TODO get和tab_info只需要保留一个...
	pub fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		match self.map.get(tab_name) {
			Some(info) => Some(info.meta.clone()),
			_ => None,
		}
	}

	//创建、修改或删除指定表，包括表名和表的元信息(主键类型和值类型)，修改和删除操作只影响源名称对应的源表
	pub fn alter(&mut self, tab_name: &Atom, meta: Option<Arc<TabMeta>>) {
		//先查找rename_logs，获取该表的源名称和版本，然后修改alter_logs
		let (src_name, ver) = match self.rename_logs.get(tab_name) {
			Some(v) => v.clone(),
			_ => (tab_name.clone(), 0),
		};
		let mut f = |v: Option<&TabInfo>| {
			match v {
				//指定的表已存在
				Some(ti) => match &meta {
					//修改指定表的元信息
					Some(si) => ActionResult::Upsert(TabInfo {
						meta: si.clone(),
						init: ti.init.clone(),
					}),
					_ => {
						//删除指定表
						ActionResult::Delete
					}
				},
				_ => match &meta {
					Some(si) => {
						//创建指定表的元信息
						ActionResult::Upsert(TabInfo::new(si.clone()))
					},
					_ => ActionResult::Ignore
				}
			}
		};
		self.map.action(&src_name, &mut f); //在Ordmap中执行操作
		self.alter_logs.entry((src_name, ver)).or_insert(meta.clone()); //记录创建表的日志
		self.meta_names.insert(tab_name.clone()); //插入创建、修改或删除表时的表名称，自动去重
	}

	//创建表事务，包括元信息表和其它表的表事务
	pub async fn build(&self, ware: BuildDbType, tab_name: &Atom, id: &Guid, writable: bool) -> SResult<TxnType> {
		match self.map.get(tab_name) {
			Some(ref info) => {
				let tab = {
					let mut var = info.init.lock().await;
					match var.wait {
						Some(ref mut vec) => {// 表尚未build
							if *vec {// 第一次调用
								match ware {
									BuildDbType::MemoryDB => {
										match MemDB::open(tab_name).await {
											Ok(t) => {
												var.tab_type = TabType::MemTab(t);
												var.wait = None;
												var.tab_type.clone()
											}
											
											Err(e) => return Err(e)
										}
									}
									BuildDbType::LogFileDB => {
										match LogFileDB::open(tab_name).await {
											Ok(t) => {
												var.tab_type = TabType::LogFileTab(t);
												var.wait = None;
												var.tab_type.clone()
											}
											Err(e) => return Err(e)
										}
									}
								}
							} else {
								return Err("unreachable branch".to_string())
							}
						},
						_ => {
							var.tab_type.clone()
						}
					}
				};
				// 根据结果创建事务或返回错误
				match tab {
					TabType::MemTab(t) => {
						Ok(TxnType::MemTxn(Arc::new(t.transaction(&id, writable).await)))
					}
					TabType::LogFileTab(t) => {
						Ok(TxnType::LogFileTxn(Arc::new(t.transaction(&id, writable).await)))
					}
					TabType::Unkonwn => Err(String::from("unknown tab type"))
				}
			},
			_ => Err(String::from("TabNotFound: ") + (*tab_name).as_str())
		}
	}
}

/*
* 预提交，Guid是事务id，Bin是主键(元信息表的主键是表名)的二进制，RwLog是事务的操作日志
*/
pub struct Prepare(XHashMap<Guid, XHashMap<Bin, RwLog>>);

impl Prepare{
	//创建一个预提交
	pub fn new(map: XHashMap<Guid, XHashMap<Bin, RwLog>>) -> Prepare{
		Prepare(map)
	}

    //检查预提交是否冲突
	//如果预提交的事务操作日志中存在对应的key，且其类型为Write，同时，本次预提交类型也为Write，则预提交冲突）
	pub fn try_prepare (&mut self, key: &Bin, log_type: &RwLog) -> Result<(), String> {
		let mut timeout_tr = vec![];
		for (guid, o_rwlog) in self.0.iter() {
			// 判断事务是否过期(有效期为1分钟)
			if (now_nanosecond() as u64 - guid.time()) > 60 * 1000 * 1000000 {
				timeout_tr.push((guid.clone(), key.clone(), log_type.clone()));
				continue;
			}
			
			match o_rwlog.get(key) {
				Some(RwLog::Read) => match log_type {
					RwLog::Read => return Ok(()),
					RwLog::Write(_) => {
						debug!("expect read log type, found write log type");
						let key_str = format!("{:?}", key);
						return Err(String::from("prepare conflicted rw") + key_str.as_str());
					}
					RwLog::Meta(_) => {
						debug!("expect read log type, found meta log type");
						let key_str = format!("{:?}", key);
						return Err(String::from("unexpected meta log type1") + key_str.as_str());
					}
				},
				Some(RwLog::Write(_)) => match log_type {
					RwLog::Read => {
						debug!("previous read log exist, key = {:?}", key);
						let key_str = format!("{:?}", key);
						return Err(String::from("previous read log exist") + key_str.as_str());
					}
					RwLog::Write(_) => {
						debug!("previous write log exist, key = {:?}", key);
						let key_str = format!("{:?}", key);
						return Err(String::from("previous write log exist") + key_str.as_str());
					}
					RwLog::Meta(_) => {
						debug!("previous meta log exist, key = {:?}", key);
						let key_str = format!("{:?}", key);
						return Err(String::from("previous meta log exist") + key_str.as_str());
					}
					
				},
				Some(RwLog::Meta(_)) => {
					let key_str = format!("{:?}", key);
					return Err(String::from("unexpected meta log type2") + key_str.as_str());
				}
				None => continue,
			}
		}
		for (guid, key, log_type) in timeout_tr {
			self.0.remove(&guid);
			warn!("overdue transactions guid:{:?}, key:{:?}, log_type:{:?}", guid, key, log_type);
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

/*
* 表名迭代器
*/
pub struct TabIter{
	_root: Arc<RwLock<OrdMap<Tree<Atom, TabInfo>>>>,	//所有表的表信息
	point: usize,										//OrdMap<Tree<Atom, TabInfo>的Keys<Tree<Atom, TabInfo>>指针的数字
}

impl Drop for TabIter {
	fn drop(&mut self) {
        unsafe{Box::from_raw(self.point as *mut Keys<Tree<Atom, TabInfo>>)};
    }
}

impl<'a> TabIter {
	//构建一个表名迭代器
	pub fn new(root: Arc<RwLock<OrdMap<Tree<Atom, TabInfo>>>>, it: Keys<'a, Tree<Atom, TabInfo>>) -> TabIter{
		TabIter{
			_root: root.clone(),
			point: Box::into_raw(Box::new(it)) as usize
		}
	}
}

impl Iterator for TabIter {
	type Item = Atom;
	fn next(&mut self) -> Option<Self::Item>{
		let mut it = unsafe{Box::from_raw(self.point as *mut Keys<Tree<Atom, TabInfo>>)};
		let r = match it.next() {
			Some(k) => Some(k.clone()),
			None => None,
		};
		mem::forget(it);
		r
	}
}


/*
* 表管理器
*/
pub struct Tabs {
	map: Arc<RwLock<OrdMap<Tree<Atom, TabInfo>>>>,	//全部的表结构，包括表名和表的元信息
	prepare: Arc<Mutex<XHashMap<Guid, TabLog>>>,	//记录元信息表的所有预提交事务信息，包括元信息表事务id和元信息表操作日志
}

impl Tabs {
	//构建一个表管理器
	pub fn new() -> Self {
		Tabs {
			map : Arc::new(RwLock::new(OrdMap::new(new()))),
			prepare: Arc::new(Mutex::new(XHashMap::with_capacity_and_hasher(0, Default::default()))),
		}
	}

	//复制表管理器
	pub fn clone_map(&self) -> Self{
		Tabs {
			map : self.map.clone(),
			prepare: Arc::new(Mutex::new(XHashMap::with_capacity_and_hasher(0, Default::default()))),
		}
	}

	//列出全部的表
	pub async fn list(&self) -> TabIter {
		TabIter::new(self.map.clone(), self.map.read().await.keys(None, false))
	}

	//获取指定表的元信息
	//TODO get和get_tab_meta只保留一个...
	pub async fn get(&self, tab: &Atom) -> Option<Arc<TabMeta>> {
		match self.map.read().await.get(tab) {
			Some(t) => Some(t.meta.clone()),
			_ => None
		}
	}

	//获取元信息表的快照
	pub async fn snapshot(&self) -> TabLog {
		let map = self.map.read().await.clone();
		TabLog {
			map: map.clone(),
			old_map: map,
			meta_names: XHashSet::with_capacity_and_hasher(0, Default::default()),
			alter_logs: XHashMap::with_capacity_and_hasher(0, Default::default()),
			rename_logs: XHashMap::with_capacity_and_hasher(0, Default::default()),
		}
	}

	//获取指定表的元信息
	//TODO get和get_tab_meta只保留一个...
	pub async fn get_tab_meta(&self, tab: &Atom) -> Option<Arc<TabMeta>> {
		match self.map.read().await.get(tab) {
			Some(info) => Some(info.meta.clone()),
			_ => None,
		}
	}

	//设置指定表的元信息
	pub async fn set_tab_meta(&mut self, tab: Atom, meta: Arc<TabMeta>) -> bool {
		let r = self.map.write().await.insert(tab, TabInfo::new(meta));
		r
	}

	//元信息表的预提交
	pub async fn prepare(&self, id: &Guid, log: &mut TabLog) -> DBResult {
		// 先检查预提交的交易是否有冲突
		for val in self.prepare.lock().await.values() {
			if !val.meta_names.is_disjoint(&log.meta_names) {
				return Err(String::from("meta prepare conflicting"))
			}
		}
		// 然后检查数据表是否被修改
		if !self.map.read().await.ptr_eq(&log.old_map) {
			// 如果被修改，则检查是否有冲突
			// TODO 暂时没有考虑重命名的情况
			for name in log.meta_names.iter() {
				match self.map.read().await.get(name) {
					Some(r1) => match log.old_map.get(name) {
						Some(r2) if (r1 as *const TabInfo) == (r2 as *const TabInfo) => (),
						_ => return Err(String::from("meta prepare conflicted"))
					}
					_ => match log.old_map.get(name) {
						None => (),
						_ => return Err(String::from("meta prepare conflicted"))
					}
				}
			}
		}
		self.prepare.lock().await.insert(id.clone(), log.replace());
		Ok(())
	}

	//元信息表的提交
	pub async fn commit(&self, id: &Guid) {
		match self.prepare.lock().await.remove(id) {
			Some(log) => if self.map.read().await.ptr_eq(&log.old_map) {
				// 检查数据表是否被修改， 如果没有修改，则可以直接替换根节点
				*self.map.write().await = log.map;
			}else{
				// 否则，重新执行一遍修改
				for r in log.alter_logs {
					if r.1.is_none() {
						self.map.write().await.delete(&Atom::from((r.0).0.as_ref()), false);
					} else {
						let tab_info = TabInfo::new(r.1.unwrap());
						self.map.write().await.upsert(Atom::from((r.0).0.as_ref()), tab_info, false);
					}
				}
			}
			_ => ()
		}
	}

	//元信息表的回滚
	pub async fn rollback(&self, id: &Guid) {
		self.prepare.lock().await.remove(id);
	}

}
//================================ 内部结构和方法
//表信息
#[derive(Clone)]
pub struct TabInfo {
	meta:  Arc<TabMeta>,		//表元信息
	init: Arc<Mutex<TabInit>>,	//表初始化
}
impl TabInfo {
	fn new(meta: Arc<TabMeta>) -> Self {
		TabInfo{
			meta,
			init: Arc::new(Mutex::new(TabInit {
				tab_type: TabType::Unkonwn,
				wait:Some(true),
			})),
		}
	}
}
//表初始化
struct TabInit {
	tab_type: TabType,	//表类型
	wait: Option<bool>, //是否等待初始化，为None表示不需要等待初始化
}

//表类型
#[derive(Clone)]
enum TabType {
	Unkonwn,				//未知
	MemTab(MTab),			//内存表
	LogFileTab(LogFileTab)	//日志文件表
}