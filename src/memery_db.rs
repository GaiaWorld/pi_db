
use pi_lib::ordmap::{OrdMap, Entry};
use pi_lib::asbtree::{Tree};
use pi_lib::atom::{Atom};
use pi_lib::guid::{Guid, GuidGen};
use pi_lib::sinfo::StructInfo;
use pi_lib::time::now_nanos;
use pi_lib::bon::{BonBuffer, Encode};

// use fnv::HashMap;

use db::{Txn, TabTxn, TabKV, TxIterCallback, TxQueryCallback, DBResult, MetaTxn, Tab, TabBuilder, TxCallback, TxState, Cursor, UsizeResult};

use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::cell::RefCell;
use std::mem;

pub type Rwlog = HashMap<Arc<Vec<u8>>, Rwlogv>;
pub type MemeryKV = Tree<Arc<Vec<u8>>, Arc<Vec<u8>>>;
pub type Root = OrdMap<MemeryKV>;
pub type MemeryResult<T> = Result<T, String>;

#[derive(Clone, Debug)]
pub enum Rwlogv {
	Read,
	Write(Option<Arc<Vec<u8>>>),
}

pub struct MemeryTab {
	pub prepare: HashMap<Guid, Rwlog>,
	pub root: Root,
	pub tab: Atom,
}

pub struct MemeryDB {
	tabs: ArcMutexTab,
}

pub struct MemeryTxn {
	id: Guid,
	root: Root,
	old: Root,
	tab: ArcMutexTab,
	rwlog: Rwlog,
	state: TxState,
}

pub type ArcMutexTab = Arc<Mutex<MemeryTab>>;
pub type RefMemeryTxn = RefCell<MemeryTxn>;

impl MemeryTxn {
	//开始事务
	pub fn begin(tab: ArcMutexTab, id: &Guid) -> RefMemeryTxn {
		let rwlog: Rwlog = HashMap::new();
		let root = &tab.lock().unwrap().root;
		let tab = MemeryTxn {
			id: id.clone(),
			root: root.clone(),
			old: root.clone(),
			tab: tab.clone(),
			rwlog,
			state: TxState::Ok,
		};
		return RefCell::new(tab)
	}
	//获取数据
	pub fn get(&mut self, key: Arc<Vec<u8>>) -> Option<Arc<Vec<u8>>> {
		match self.root.get(&key) {
			Some(v) => {
				match self.rwlog.get(&key) {
					Some(_) => (),
					None => {
						&mut self.rwlog.insert(key, Rwlogv::Read);
						()
					}
				}
				return Some(v.clone())
			},
			None => return None
		}
	}
	//插入/修改数据
	pub fn upsert(&mut self, key: Arc<Vec<u8>>, value: Arc<Vec<u8>>) -> MemeryResult<()> {
		self.root.upsert(key.clone(), value.clone(), false);
		&mut self.rwlog.insert(key.clone(), Rwlogv::Write(Some(value.clone())));
		Ok(())
	}
	//删除
	pub fn delete(&mut self, key: Arc<Vec<u8>>) -> MemeryResult<()> {
		self.root.delete(&key, false);
		self.rwlog.insert(key.clone(), Rwlogv::Write(None));
		Ok(())
	}
	//迭代
	pub fn select<F>(&self, key: Option<&Arc<Vec<u8>>>, descending: bool, func: &mut F) where F: FnMut(&Entry<Arc<Vec<u8>>, Arc<Vec<u8>>>) {
		self.root.select(key, descending, func)
	}
	//预提交
	pub fn prepare1(&mut self) -> MemeryResult<()> {
		let mut tab = self.tab.lock().unwrap();
		//遍历事务中的读写日志
		for (key, rw_v) in self.rwlog.iter() {
			//检查预提交是否冲突
			for o_rwlog in tab.prepare.values() {
				match o_rwlog.get(key) {
					Some(Rwlogv::Read) => match rw_v {
						Rwlogv::Read => (),
						_ => return Err(String::from("parpare conflicted rw"))
					},
					None => (),
					Some(_e) => {
						return Err(String::from("parpare conflicted rw2"))
					},
				}
			}
			//检查Tab根节点是否改变
			if tab.root.ptr_eq(&self.old) == false {
				match tab.root.get(&key) {
					Some(r1) => match self.old.get(&key) {
						Some(r2) if (r1 as *const Arc<Vec<u8>>) == (r2 as *const Arc<Vec<u8>>) => (),
						_ => return Err(String::from("parpare conflicted value diff"))
					},
					_ => match self.old.get(&key) {
						None => (),
						_ => return Err(String::from("parpare conflicted old not None"))
					}
				}
			}
		}
		let rwlog = mem::replace(&mut self.rwlog, HashMap::with_capacity_and_hasher(0, Default::default()));
		//写入预提交
		tab.prepare.insert(self.id.clone(), rwlog);
		return Ok(())
	}
	//提交
	pub fn commit1(&mut self) -> MemeryResult<()> {
		let mut tab = self.tab.lock().unwrap();
		match tab.prepare.remove(&self.id) {
			Some(rwlog) => {
				let root_if_eq = tab.root.ptr_eq(&self.old);
				//判断根节点是否相等
				if root_if_eq == false {
					for (k, rw_v) in rwlog.iter() {
						match rw_v {
							Rwlogv::Read => (),
							_ => {
								match rw_v {
									Rwlogv::Write(None) => {
										tab.root.delete(&k, false);
										()
									},
									Rwlogv::Write(Some(v)) => {
										tab.root.upsert(k.clone(), v.clone(), false);
										()
									},
									_ => (),
								}
								()
							},
						}
					}
				} else {
					tab.root = self.root.clone();
				}
			},
			None => return Err(String::from("error prepare null"))
		}
		Ok(())
	}
	//回滚
	pub fn rollback1(&mut self) -> MemeryResult<()> {
		self.tab.lock().unwrap().prepare.remove(&self.id);
		Ok(())
	}
}

impl Txn for RefMemeryTxn {
	// 获得事务的状态
	fn get_state(&self) -> TxState {
		self.borrow().state.clone()
	}
	// 预提交一个事务
	fn prepare(&self, _cb: TxCallback) -> UsizeResult {
		let mut txn = self.borrow_mut();
		txn.state = TxState::Preparing;
		match txn.prepare1() {
			Ok(()) => {
				txn.state = TxState::PreparOk;
				return Some(Ok(1))
			},
			Err(e) => {
				txn.state = TxState::PreparFail;
				return Some(Err(e.to_string()))
			},
		}
	}
	// 提交一个事务
	fn commit(&self, _cb: TxCallback) -> UsizeResult {
		let mut txn = self.borrow_mut();
		txn.state = TxState::Committing;
		match txn.commit1() {
			Ok(()) => {
				txn.state = TxState::Commited;
				return Some(Ok(1))
			},
			Err(e) => return Some(Err(e.to_string())),
		}
	}
	// 回滚一个事务
	fn rollback(&self, _cb: TxCallback) -> UsizeResult {
		let mut txn = self.borrow_mut();
		txn.state = TxState::Rollbacking;
		match txn.rollback1() {
			Ok(()) => {
				txn.state = TxState::Rollbacked;
				return Some(Ok(1))
			},
			Err(e) => return Some(Err(e.to_string())),
		}
	}
}

impl TabTxn for RefMemeryTxn {
	// 键锁，key可以不存在，根据lock_time的值决定是锁还是解锁
	fn key_lock(&self, _arr: Arc<Vec<TabKV>>, _lock_time: usize, _readonly: bool, _cb: TxCallback) -> UsizeResult {
		None
	}
	// 查询
	fn query(
		&self,
		arr: Arc<Vec<TabKV>>,
		_lock_time: Option<usize>,
		_readonly: bool,
		_cb: TxQueryCallback,
	) -> Option<DBResult<Vec<TabKV>>> {
		let mut txn = self.borrow_mut();
		let mut value_arr = Vec::new();
		for tabkv in arr.iter() {
			let mut value = None;
			match txn.get(Arc::new(tabkv.key.clone())) {
				Some(v) =>
					{
						value = Some(v);
						()
					},
				_ =>
					{
						return Some(Err(String::from("null")))
					},
			}
			value_arr.push(
				TabKV{
				tab: tabkv.tab.clone(),
				key: tabkv.key.clone(),
				index: tabkv.index.clone(),
				value: value,
				}
			)
		}
		Some(Ok(value_arr))
	}
	// 修改，插入、删除及更新
	fn modify(&self, arr: Arc<Vec<TabKV>>, _lock_time: Option<usize>, _readonly: bool, _cb: TxCallback) -> UsizeResult {
		let mut txn = self.borrow_mut();
		let len = arr.len();
		for tabkv in arr.iter() {
			if tabkv.value == None {
				match txn.delete(Arc::new(tabkv.key.clone())) {
				Ok(_) => (),
				Err(e) => 
					{
						return Some(Err(e.to_string()))
					},
				};
			} else {
				match txn.upsert(Arc::new(tabkv.key.clone()), tabkv.value.clone().unwrap()) {
				Ok(_) => (),
				Err(e) =>
					{
						return Some(Err(e.to_string()))
					},
				};
			}
		}
		Some(Ok(len))
	}
	// 迭代
	fn iter(
		&self,
		_tab: &Atom,
		_key: Option<Vec<u8>>,
		_descending: bool,
		_key_only: bool,
		_filter: String,
		_cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>> {
        None
	}
	// 索引迭代
	fn index(
		&self,
		_tab: &Atom,
		_key: Option<Vec<u8>>,
		_descending: bool,
		_filter: String,
		_cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>> {
		None
	}
	// 表的大小
	fn tab_size(&self, _cb: TxCallback) -> UsizeResult {
		None
	}
}

impl Tab for ArcMutexTab {
	fn transaction(&self, id: &Guid, _writable: bool, _timeout: usize) -> Arc<TabTxn> {
		let txn = MemeryTxn::begin(self.clone(), id);
		return Arc::new(txn)
	}
}

impl MemeryDB {
	pub fn new(tab_name: Atom) -> Self {
		let tree:MemeryKV = None;
		let root= OrdMap::new(tree);
		let tab = MemeryTab {
			prepare: HashMap::new(),
			root: root,
			tab: tab_name,
		};
		let tab: ArcMutexTab = Arc::new(Mutex::new(tab));
		MemeryDB {
			tabs: tab,
		}
	}
}

impl MetaTxn for RefMemeryTxn {
	// 创建表、修改指定表的元数据
	fn alter(
		&self,
		tab: &Atom,
		meta: Option<Arc<StructInfo>>,
		_cb: TxCallback,
	) -> UsizeResult {
		let value;
		match meta {
			None => value = None,
			Some(m) => {
				let mut meta_buf = BonBuffer::new();
				m.encode(&mut meta_buf);
				value = Some(Arc::new(meta_buf.unwrap()));
			}
		}
		let mut arr = Vec::new();
		let tab_name = &**tab;
		let mut kv = TabKV::new(tab.clone(), tab_name.clone().into_bytes());
		kv.value = value;
		arr.push(kv);
		&self.modify(Arc::new(arr), None, false, Arc::new(|_v|{}));
		Some(Ok(1))
	}
	// 修改指定表的名字
	fn rename(
		&self,
		_tab: &Atom,
		_new_name: &Atom,
		_cb: TxCallback,
	) -> UsizeResult {
		Some(Ok(1))
	}
}

impl TabBuilder for MemeryDB {
	// 列出全部的表
	fn list(&self) -> Vec<(Atom, Arc<StructInfo>)> {
		return vec![]
	}
	// 打开指定的表，表必须有meta
	fn open(
		&self,
		tab: &Atom,
		_cb: Box<Fn(DBResult<Arc<Tab>>)>,
	) -> Option<DBResult<Arc<Tab>>> {
		let tree:MemeryKV = None;
		let root= OrdMap::new(tree);
		let tab = MemeryTab {
			prepare: HashMap::new(),
			root: root,
			tab: tab.clone(),
		};
		let tab: ArcMutexTab = Arc::new(Mutex::new(tab));
		Some(Ok(Arc::new(tab)))
	}
	// 检查该表是否可以创建
	fn check(
		&self,
		tab: &Atom,
		meta: &Arc<StructInfo>,
	) -> DBResult<()> {
		// let guid_gen = GuidGen::new(1, now_nanos() as u32);
		// let guid = guid_gen.gen(2);
		// let mut txn = self.transaction(&guid, 10);
		// txn.alter(tab, Some(meta.clone()), Arc::new(|_v|{}));
		// txn.prepare(Arc::new(|_v|{}));
		// txn.commit(Arc::new(|_v|{}));
		Ok(())
	}

	// 创建一个meta事务
	fn transaction(&self, id: &Guid, _timeout: usize) -> Arc<MetaTxn> {
		let txn = MemeryTxn::begin(self.tabs.clone(), id);
		return Arc::new(txn)
	}
}