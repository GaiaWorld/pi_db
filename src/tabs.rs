/**
 * 表管理器，给每个具体的Ware用
 */
use std::sync::{Arc, Mutex};
use std::mem;

use fnv::{FnvHashMap, FnvHashSet};

use pi_lib::ordmap::{OrdMap, ActionResult, Keys};
use pi_lib::asbtree::{Tree, new};
use pi_lib::atom::Atom;
use pi_lib::sinfo::StructInfo;
use pi_lib::guid::Guid;

use db::{SResult, Tab, TabTxn, OpenTab};

// 表结构及修改日志
pub struct TabLog<T: Clone + Tab> {
	map: OrdMap<Tree<Atom, TabInfo<T>>>,
	old_map: OrdMap<Tree<Atom, TabInfo<T>>>, // 用于判断mgr中tabs是否修改过
	meta_names: FnvHashSet<Atom>, //元信息表的名字
	alter_logs: FnvHashMap<(Atom, usize), Option<Arc<StructInfo>>>, // 记录每个被改过元信息的表
	rename_logs: FnvHashMap<Atom, (Atom, usize)>, // 新名字->(源名字, 版本号)
}
impl<T: Clone + Tab> TabLog<T> {
	// 列出全部的表
	pub fn list(&self) -> TabIter<T> {
		TabIter::new(self.map.clone(), self.map.keys(None, false))
	}
	// 获取指定的表结构
	pub fn get(&self, tab: &Atom) -> Option<Arc<StructInfo>> {
		match self.map.get(tab) {
			Some(t) => Some(t.meta.clone()),
			_ => None
		}
	}
	// 表的元信息
	fn replace(&mut self) -> Self {
		TabLog {
			map: self.map.clone(),
			old_map: self.old_map.clone(),
			meta_names: mem::replace(&mut self.meta_names, FnvHashSet::with_capacity_and_hasher(0, Default::default())),
			alter_logs: mem::replace(&mut self.alter_logs, FnvHashMap::with_capacity_and_hasher(0, Default::default())),
			rename_logs: mem::replace(&mut self.rename_logs, FnvHashMap::with_capacity_and_hasher(0, Default::default())),
		}
	}
	// 表的元信息
	pub fn tab_info(&self, tab_name: &Atom) -> Option<Arc<StructInfo>> {
		match self.map.get(tab_name) {
			Some(info) => Some(info.meta.clone()),
			_ => None,
		}
	}
	// 新增 修改 删除 表
	pub fn alter(&mut self, tab_name: &Atom, meta: Option<Arc<StructInfo>>) {
		// 先查找rename_logs，获取该表的源名字及版本，然后修改alter_logs
		let (src_name, ver) = match self.rename_logs.get(tab_name) {
			Some(v) => v.clone(),
			_ => (tab_name.clone(), 0),
		};
		let mut f = |v: Option<&TabInfo<T>>| {
			match v {
				Some(ti) => match &meta {
					Some(si) => ActionResult::Upsert(TabInfo {
						meta: si.clone(),
						init: ti.init.clone(),
					}),
					_ => ActionResult::Delete
				},
				_ => match &meta {
					Some(si) => ActionResult::Upsert(TabInfo::new(si.clone())),
					_ => ActionResult::Ignore
				}
			}
		};
		self.map.action(&src_name, &mut f);
		self.alter_logs.entry((src_name, ver)).or_insert(meta.clone());
		self.meta_names.insert(tab_name.clone());
	}
	// 创建表事务
	pub fn build<W: OpenTab>(&self, ware: &W, tab_name: &Atom, id: &Guid, writable: bool, cb: Box<Fn(SResult<Arc<TabTxn>>)>) -> Option<SResult<Arc<TabTxn>>> {
		match self.map.get(tab_name) {
			Some(ref info) => {
				let tab = {
					let mut var = info.init.lock().unwrap();
					match var.wait {
						Some(ref mut vec) => {// 表尚未build
							if vec.len() == 0 {// 第一次调用
								let init = info.init.clone();
								match ware.open(tab_name, Box::new(move |tab| {
									// 异步返回，解锁后设置结果，返回等待函数数组
									let vec:Vec<Box<Fn(SResult<T>)>> = {
										let mut var = init.lock().unwrap();
										let v = mem::replace(var.wait.as_mut().unwrap(), Vec::new());
										var.tab = tab.clone();
										var.wait = None;
										v
									};
									// 通知所有的等待函数数组
									for f in vec.into_iter() {
										(*f)(tab.clone())
									}
								})) {
									Some(r) => {// 同步返回，设置结果
										var.tab = r;
										var.wait = None;
										var.tab.clone()
									},
									_ => { //异步的第1次调用，直接返回
										vec.push(handle_fn(id.clone(), writable, cb));
										return None
									}
								}
							}else { // 异步的第n次调用，直接返回
								vec.push(handle_fn(id.clone(), writable, cb));
								return None
							}
						},
						_ => var.tab.clone()
					}
				};
				// 根据结果创建事务或返回错误
				match tab {
					Ok(tab) => Some(Ok(tab.transaction(&id, writable))),
					Err(s) => Some(Err(s))
				}
			},
			_ => Some(Err(String::from("TabNotFound")))
		}
	}
}

pub struct TabIter<T: Clone + Tab>{
	_root: OrdMap<Tree<Atom, TabInfo<T>>>,
	point: usize,
}

impl<'a, T: 'a + Clone + Tab> TabIter<T>{
	pub fn new(root: OrdMap<Tree<Atom, TabInfo<T>>>, it: Keys<'a, Tree<Atom, TabInfo<T>>>) -> TabIter<T>{
		TabIter{
			_root: root.clone(),
			point: Box::into_raw(Box::new(it)) as usize
		}
	}
}

impl<T: Clone + Tab> Iterator for TabIter<T>{
	type Item = Atom;
	fn next(&mut self) -> Option<Self::Item>{
		match unsafe{Box::from_raw(self.point as *mut Keys<'static, Tree<Atom, TabInfo<T>>>)}.next() {
			Some(k) => Some(k.clone()),
			None => None,
		}
	}
}


// 表管理器
pub struct Tabs<T: Clone + Tab> {
	//全部的表结构
	map: OrdMap<Tree<Atom, TabInfo<T>>>,
	// 预提交的元信息事务表
	prepare: FnvHashMap<Guid, TabLog<T>>,
}

impl<T: Clone + Tab> Tabs<T> {
	pub fn new() -> Self {
		Tabs {
			map : OrdMap::new(new()),
			prepare: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
		}
	}
	// 列出全部的表
	pub fn list(&self) -> TabIter<T> {
		TabIter::new(self.map.clone(), self.map.keys(None, false))
	}
	// 获取指定的表结构
	pub fn get(&self, tab: &Atom) -> Option<Arc<StructInfo>> {
		match self.map.get(tab) {
			Some(t) => Some(t.meta.clone()),
			_ => None
		}
	}
	// 获取当前表结构快照
	pub fn snapshot(&self) -> TabLog<T> {
		TabLog {
			map: self.map.clone(),
			old_map: self.map.clone(),
			meta_names: FnvHashSet::with_capacity_and_hasher(0, Default::default()),
			alter_logs: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			rename_logs: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
		}
	}
	// 获取表的元信息
	pub fn get_tab_meta(&self, tab: &Atom) -> Option<Arc<StructInfo>> {
		match self.map.get(tab) {
			Some(info) => Some(info.meta.clone()),
			_ => None,
		}
	}
	// 设置表的元信息
	pub fn set_tab_meta(&mut self, tab: Atom, meta: Arc<StructInfo>) -> bool {
		self.map.insert(tab, TabInfo::new(meta))
	}

	// 元信息的预提交
	pub fn prepare(&mut self, id: &Guid, log: &mut TabLog<T>) -> SResult<()> {
		// 先检查预提交的交易是否有冲突
		for val in self.prepare.values() {
			if val.meta_names.is_disjoint(&log.meta_names) {
				return Err(String::from("meta parpare conflicting"))
			}
		}
		// 然后检查数据表是否被修改
		if !self.map.ptr_eq(&log.old_map) {
			// 如果被修改，则检查是否有冲突
			// TODO 暂时没有考虑重命名的情况
			for name in log.meta_names.iter() {
				match self.map.get(name) {
					Some(r1) => match log.old_map.get(name) {
						Some(r2) if (r1 as *const TabInfo<T>) == (r2 as *const TabInfo<T>) => (),
						_ => return Err(String::from("meta parpare conflicted"))
					}
					_ => match log.old_map.get(name) {
						None => (),
						_ => return Err(String::from("meta parpare conflicted"))
					}
				}
			}
		}
		self.prepare.insert(id.clone(), log.replace());
		Ok(())
	}
	// 元信息的提交
	pub fn commit(&mut self, id: &Guid) {
		match self.prepare.remove(id) {
			Some(log) => if self.map.ptr_eq(&log.old_map) {
				// 检查数据表是否被修改， 如果没有修改，则可以直接替换根节点
				self.map = log.map;
			}else{
				// 否则，重新执行一遍修改， TODO
			}
			_ => ()
		}
	}
	// 回滚
	pub fn rollback(&mut self, id: &Guid) {
		self.prepare.remove(id);
	}

}
//================================ 内部结构和方法
// 表信息
#[derive(Clone)]
pub struct TabInfo<T: Clone + Tab> {
	meta: Arc<StructInfo>,
	init: Arc<Mutex<TabInit<T>>>,
}
impl<T: Clone + Tab> TabInfo<T> {
	fn new(meta: Arc<StructInfo>) -> Self {
		TabInfo{
			meta: meta,
			init: Arc::new(Mutex::new(TabInit {
				tab: Err(String::from("")),
				wait:Some(Vec::new()),
			})),
		}
	}
}
// 表初始化
struct TabInit<T: Clone + Tab> {
	tab: SResult<T>,
	wait: Option<Vec<Box<Fn(SResult<T>)>>>, // 为None表示tab已经加载
}
//================================ 内部静态方法
// 表构建函数的回调函数
fn handle_fn<T: Tab>(id: Guid, writable: bool, cb: Box<Fn(SResult<Arc<TabTxn>>)>) -> Box<Fn(SResult<T>)> {
	Box::new(move |r| {
		match r {
			Ok(tab) => {
				// 创建事务
				cb(Ok(tab.transaction(&id, writable)))
			},
			Err(s) => cb(Err(s))
		}
	})
}
