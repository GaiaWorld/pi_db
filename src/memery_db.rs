
use std::sync::Arc;
use std::mem;

use hash::XHashMap;
use ordmap::ordmap::{OrdMap, Entry, Iter as OIter, Keys};
use ordmap::asbtree::{Tree};
use atom::{Atom};
use guid::Guid;
use apm::counter::{GLOBAL_PREF_COLLECT, PrefCounter};

use crate::db::{Bin, TabKV, SResult, IterResult, KeyIterResult, NextResult, Event, Filter, TxState, Iter, RwLog, Bon, TabMeta, CommitResult, DBResult};
use crate::tabs::{TabLog, Tabs, Prepare};
use crate::db::BuildDbType;
use r#async::lock::mutex_lock::Mutex;
use r#async::lock::rw_lock::RwLock;
use crate::tabs::TxnType;

//内存表前缀
const MEMORY_TABLE_PREFIX: &'static str = "mem_table_";
//内存表事务创建数量后缀
const MEMORY_TABLE_TRANS_COUNT_SUFFIX: &'static str = "_trans_count";
//内存表事务预提交数量后缀
const MEMORY_TABLE_PREPARE_COUNT_SUFFIX: &'static str = "_prepare_count";
//内存表事务提交数量后缀
const MEMORY_TABLE_COMMIT_COUNT_SUFFIX: &'static str = "_commit_count";
//内存表事务回滚数量后缀
const MEMORY_TABLE_ROLLBACK_COUNT_SUFFIX: &'static str = "_rollback_count";
//内存表读记录数量后缀
const MEMORY_TABLE_READ_COUNT_SUFFIX: &'static str = "_read_count";
//内存表读记录字节数量后缀
const MEMORY_TABLE_READ_BYTE_COUNT_SUFFIX: &'static str = "_read_byte_count";
//内存表写记录数量后缀
const MEMORY_TABLE_WRITE_COUNT_SUFFIX: &'static str = "_write_count";
//内存表写记录字节数量后缀
const MEMORY_TABLE_WRITE_BYTE_COUNT_SUFFIX: &'static str = "_write_byte_count";
//内存表删除记录数量后缀
const MEMORY_TABLE_REMOVE_COUNT_SUFFIX: &'static str = "_remove_count";
//内存表删除记录字节数量后缀
const MEMORY_TABLE_REMOVE_BYTE_COUNT_SUFFIX: &'static str = "_remove_byte_count";
//内存表关键字迭代数量后缀
const MEMORY_TABLE_KEY_ITER_COUNT_SUFFIX: &'static str = "_key_iter_count";
//内存表关键字迭代字节数量后缀
const MEMORY_TABLE_KEY_ITER_BYTE_COUNT_SUFFIX: &'static str = "_key_iter_byte_count";
//内存表迭代数量后缀
const MEMORY_TABLE_ITER_COUNT_SUFFIX: &'static str = "_iter_count";
//内存表关键字迭代字节数量后缀
const MEMORY_TABLE_ITER_BYTE_COUNT_SUFFIX: &'static str = "_iter_byte_count";

lazy_static! {
	//内存库创建数量
	static ref MEMORY_WARE_CREATE_COUNT: PrefCounter = GLOBAL_PREF_COLLECT.new_static_counter(Atom::from("mem_ware_create_count"), 0).unwrap();
	//内存表创建数量
	static ref MEMORY_TABLE_CREATE_COUNT: PrefCounter = GLOBAL_PREF_COLLECT.new_static_counter(Atom::from("mem_table_create_count"), 0).unwrap();
}

/*
* 内存表的句柄，跨线程安全，异步安全
*/
#[derive(Clone)]
pub struct MTab(Arc<Mutex<MemeryTab>>);

impl MTab {
	//创建内存表的句柄
	pub fn new(tab: &Atom) -> Self {
		MEMORY_WARE_CREATE_COUNT.sum(1);

		let tab = MemeryTab {
			prepare: Prepare::new(XHashMap::with_capacity_and_hasher(0, Default::default())),
			root: OrdMap::new(None),
			tab: tab.clone(),
			trans_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_TRANS_COUNT_SUFFIX), 0).unwrap(),
			prepare_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_PREPARE_COUNT_SUFFIX), 0).unwrap(),
			commit_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_COMMIT_COUNT_SUFFIX), 0).unwrap(),
			rollback_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_ROLLBACK_COUNT_SUFFIX), 0).unwrap(),
			read_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_READ_COUNT_SUFFIX), 0).unwrap(),
			read_byte: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_READ_BYTE_COUNT_SUFFIX), 0).unwrap(),
			write_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_WRITE_COUNT_SUFFIX), 0).unwrap(),
			write_byte: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_WRITE_BYTE_COUNT_SUFFIX), 0).unwrap(),
			remove_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_REMOVE_COUNT_SUFFIX), 0).unwrap(),
			remove_byte: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_REMOVE_BYTE_COUNT_SUFFIX), 0).unwrap(),
		};
		MTab(Arc::new(Mutex::new(tab)))
	}

	//创建一个内存表事务的引用
	pub async fn transaction(&self, id: &Guid, writable: bool) -> RefMemeryTxn {
		self.0.lock().await.trans_count.sum(1);

		MemeryTxn::new(self.clone(), id, writable).await
	}
}

/**
* 内存数据库
*/
#[derive(Clone)]
pub struct MemDB(Arc<Tabs>);

impl MemDB {
	/**
	* 构建内存数据库
	* @returns 返回内存数据库
	*/
	pub fn new() -> Self {
		MEMORY_WARE_CREATE_COUNT.sum(1);

		MemDB(Arc::new(Tabs::new()))
	}

	//打开指定名称的内存表
	pub async fn open(tab: &Atom) -> SResult<MTab> {
		Ok(MTab::new(tab))
	}

	//拷贝全部的内存表
	pub async fn tabs_clone(&self) -> Arc<Self> {
		Arc::new(MemDB(Arc::new(self.0.clone_map())))
	}

	//列出全部的内存表
	pub async fn list(&self) -> Box<dyn Iterator<Item=Atom>> {
		Box::new(self.0.list().await)
	}

	//获取该库对预提交后的处理超时时间, 事务会用最大超时时间来预提交
	pub fn timeout(&self) -> usize {
		TIMEOUT
	}

	//获取指定表的元信息，tab_name表名，例如"db/user"
	pub async fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		self.0.get(tab_name).await
	}

	//获取当前内存数据库的快照
	pub async fn snapshot(&self) -> Arc<MemDBSnapshot> {
		Arc::new(MemDBSnapshot(self.clone(), Mutex::new(self.0.snapshot().await)))
	}
}

//内存数据库快照，包括内存数据库和内存数据库的元信息
pub struct MemDBSnapshot(MemDB, Mutex<TabLog>);

impl MemDBSnapshot {
	//列出全部的表
	pub async fn list(&self) -> Box<dyn Iterator<Item=Atom>> {
		Box::new(self.1.lock().await.list())
	}

	//表的元信息
	pub async fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		self.1.lock().await.get(tab_name)
	}

	//检查该表是否可以创建
	pub fn check(&self, _tab: &Atom, _meta: &Option<Arc<TabMeta>>) -> DBResult {
		Ok(())
	}

	//新增、修改和删除表
	pub async fn alter(&self, tab_name: &Atom, meta: Option<Arc<TabMeta>>) {
		self.1.lock().await.alter(tab_name, meta)
	}

	//创建指定表的表事务
	pub async fn tab_txn(&self, tab_name: &Atom, id: &Guid, writable: bool) -> SResult<TxnType> {
		self.1.lock().await.build(BuildDbType::MemoryDB, tab_name, id, writable).await
	}

	//创建一个元信息表事务
	pub fn meta_txn(&self, _id: &Guid) -> Arc<MemeryMetaTxn> {
		Arc::new(MemeryMetaTxn)
	}

	//元信息表的预提交
	pub async fn prepare(&self, id: &Guid) -> DBResult{
		(self.0).0.prepare(id, &mut *self.1.lock().await).await
	}

	//元信息表的提交
	pub async fn commit(&self, id: &Guid){
		(self.0).0.commit(id).await
	}

	//元信息表的回滚
	pub async fn rollback(&self, id: &Guid){
		(self.0).0.rollback(id).await
	}

	//内存库修改通知
	pub fn notify(&self, _event: Event) {}
}

/*
* 内存表事务的引用
*/
pub struct RefMemeryTxn(Mutex<MemeryTxn>);

impl RefMemeryTxn {
	//获得事务的状态
	pub async fn get_state(&self) -> TxState {
		self.0.lock().await.state.clone()
	}

	//查询指定主键集的记录集
	pub async fn query(
		&self,
		arr: Arc<Vec<TabKV>>,
		_lock_time: Option<usize>,
		_readonly: bool
	) -> SResult<Vec<TabKV>> {
		let mut value_arr = Vec::new();
		for tabkv in arr.iter() {
			let value = match self.0.lock().await.get(tabkv.key.clone()).await {
				Some(v) => Some(v),
				_ => None
			};

			value_arr.push(
				TabKV {
					ware: tabkv.ware.clone(),
					tab: tabkv.tab.clone(),
					key: tabkv.key.clone(),
					index: tabkv.index.clone(),
					value,
				}
			)
		}
		Ok(value_arr)
	}

	//插入、修改和删除指定主键集的记录集，值为None就是删除，主键不存在则为插入，主键存在则为修改
	pub async fn modify(&self, arr: Arc<Vec<TabKV>>, _lock_time: Option<usize>, _readonly: bool) -> DBResult {
		for tabkv in arr.iter() {
			if tabkv.value == None {
				match self.0.lock().await.delete(tabkv.key.clone()).await {
					Ok(_) => (),
					Err(e) =>
						{
							return Err(e.to_string())
						},
				};
			} else {
				match self.0.lock().await.upsert(tabkv.key.clone(), tabkv.value.clone().unwrap()).await {
					Ok(_) => (),
					Err(e) =>
						{
							return Err(e.to_string())
						},
				};
			}
		}
		Ok(())
	}

	//获取指定表的记录迭代器
	//key为None则从表头或表尾开始迭代，由descending确定，descending为true表示从表尾迭代，否则从表头迭代，key为Some一个指定主键的二进制，则从表的指定主键开始迭代，迭代方向由descending确定
	pub async fn iter(
		&self,
		tab: &Atom,
		key: Option<Bin>,
		descending: bool,
		filter: Filter
	) -> IterResult {
		let b = self.0.lock().await;
		let key = match key {
			Some(k) => Some(Bon::new(k)),
			None => None,
		};
		let key = match &key {
			&Some(ref k) => Some(k),
			None => None,
		};

		Ok(Box::new(MemIter::new(tab, b.root.clone(), b.root.iter( key, descending), filter)))
	}

	//获取指定表的主键迭代器
	//key为None则从表头或表尾开始迭代，由descending确定，descending为true表示从表尾迭代，否则从表头迭代，key为Some一个指定主键的二进制，则从表的指定主键开始迭代，迭代方向由descending确定
	pub async fn key_iter(
		&self,
		key: Option<Bin>,
		descending: bool,
		filter: Filter
	) -> KeyIterResult {
		let b = self.0.lock().await;
		let key = match key {
			Some(k) => Some(Bon::new(k)),
			None => None,
		};
		let key = match &key {
			&Some(ref k) => Some(k),
			None => None,
		};
		let tab = b.tab.0.lock().await.tab.clone();
		Ok(Box::new(MemKeyIter::new(&tab, b.root.clone(), b.root.keys(key, descending), filter)))
	}

	//获取表的索引迭代器
	//TODO...
	pub fn index(
		&self,
		_tab: &Atom,
		_index_key: &Atom,
		_key: Option<Bin>,
		_descending: bool,
		_filter: Filter,
	) -> IterResult {
		Err("not implemented".to_string())
	}

	//获取指定表的记录数量
	pub async fn tab_size(&self) -> SResult<usize> {
		let txn = self.0.lock().await;
		Ok(txn.root.size())
	}

	//预提交一个事务
	pub async fn prepare(&self, _timeout: usize) -> DBResult {
		let mut txn = self.0.lock().await;
		txn.state = TxState::Preparing;
		match txn.prepare_inner().await {
			Ok(()) => {
				txn.state = TxState::PreparOk;
				return Ok(())
			},
			Err(e) => {
				txn.state = TxState::PreparFail;
				return Err(e.to_string())
			},
		}
	}

	//提交一个事务
	pub async fn commit(&self) -> CommitResult {
		let mut txn = self.0.lock().await;
		txn.state = TxState::Committing;
		match txn.commit_inner().await {
			Ok(log) => {
				txn.state = TxState::Commited;
				return Ok(log)
			},
			Err(e) => return Err(e.to_string()),
		}
	}

	//回滚一个事务
	pub async fn rollback(&self) -> DBResult {
		let mut txn = self.0.lock().await;
		txn.state = TxState::Rollbacking;
		match txn.rollback_inner().await {
			Ok(()) => {
				txn.state = TxState::Rollbacked;
				return Ok(())
			},
			Err(e) => return Err(e.to_string())
		}
	}

	//记录锁，主键可以不存在，根据lock_time的值决定是锁还是解锁
	pub async fn key_lock(&self, _arr: Arc<Vec<TabKV>>, _lock_time: usize, _readonly: bool) -> DBResult {
		Ok(())
	}
}

/*
* 内存表事务
*/
pub struct MemeryTxn {
	id: Guid,	        			//事务id
	writable: bool,					//是否是可写事务
	tab: MTab,						//内存表的句柄
	root: BinMap,					//内存表的句柄，在创建内存表事务时从内存表的句柄拷贝，在事务过程中可能会修改
	old: BinMap,					//内存表的句柄，保留创建内存表事务时内存表的句柄，在事务过程中不会修改
	rwlog: XHashMap<Bin, RwLog>,	//内存表事务的操作日志，Bin为主键的二进制，RwLog为事务的操作日志
	state: TxState,					//事务的状态
}

impl MemeryTxn {
	//开始事务
	pub async fn new(tab: MTab, id: &Guid, writable: bool) -> RefMemeryTxn {
		let root = tab.0.lock().await.root.clone();
		let txn = MemeryTxn {
			id: id.clone(),
			writable,
			root: root.clone(),
			tab,
			old: root,
			rwlog: XHashMap::with_capacity_and_hasher(0, Default::default()),
			state: TxState::Ok,
		};
		return RefMemeryTxn(Mutex::new(txn))
	}

	//获取指定主键的记录的值
	pub async fn get(&mut self, key: Bin) -> Option<Bin> {
		self.tab.0.lock().await.read_count.sum(1);

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

				self.tab.0.lock().await.read_byte.sum(v.len());

				return Some(v.clone())
			},
			None => return None
		}
	}

	//插入或修改指定主键的记录
	pub async fn upsert(&mut self, key: Bin, value: Bin) -> DBResult {
		self.root.upsert(Bon::new(key.clone()), value.clone(), false);
		self.rwlog.insert(key.clone(), RwLog::Write(Some(value.clone())));

		{
			let tab = self.tab.0.lock().await;
			tab.write_byte.sum(value.len());
			tab.write_count.sum(1);
		}

		Ok(())
	}

	//删除指定主键的记录
	pub async fn delete(&mut self, key: Bin) -> DBResult {
		if let Some(Some(value)) = self.root.delete(&Bon::new(key.clone()), false) {
			{
				let tab = self.tab.0.lock().await;
				tab.remove_byte.sum(key.len() + value.len());
				tab.remove_count.sum(1);
			}
		}
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
						_ => return Err(String::from("prepare conflicted value diff"))
					},
					_ => match self.old.get(&key) {
						None => (),
						_ => return Err(String::from("prepare conflicted old not None"))
					}
				}
			}
		}
		let rwlog = mem::replace(&mut self.rwlog, XHashMap::with_capacity_and_hasher(0, Default::default()));
		//写入预提交
		tab.prepare.insert(self.id.clone(), rwlog);

		tab.prepare_count.sum(1);

		return Ok(())
	}

	//提交
	pub async fn commit_inner(&mut self) -> CommitResult {
		let prepare = &mut self.tab.0.lock().await.prepare;
		let logs = prepare.get(&self.id);
		let logs = match logs {
			Some(rwlog) => {
				let root_if_eq = self.tab.0.lock().await.root.ptr_eq(&self.old);
				//判断根节点是否相等
				if !root_if_eq {
					for (k, rw_v) in rwlog.iter() {
						match rw_v {
							RwLog::Read => (),
							_ => {
								let k = Bon::new(k.clone());
								match rw_v {
									RwLog::Write(None) => {
										self.tab.0.lock().await.root.delete(&k, false);
									},
									RwLog::Write(Some(v)) => {
										self.tab.0.lock().await.root.upsert(k.clone(), v.clone(), false);
									},
									_ => (),
								}
								()
							},
						}
					}
				} else {
					self.tab.0.lock().await.root = self.root.clone();
				}

				prepare.remove(&self.id).unwrap()
			},
			None => {
				let _ = prepare.remove(&self.id);
				return Err(String::from("error prepare null"))
			}
		};

		self.tab.0.lock().await.commit_count.sum(1);

		Ok(logs)
	}

	//回滚
	pub async fn rollback_inner(&mut self) -> DBResult {
		let mut tab = self.tab.0.lock().await;
		tab.prepare.remove(&self.id);

		tab.rollback_count.sum(1);

		Ok(())
	}
}

//================================ 内部结构和方法
const TIMEOUT: usize = 100;

//内存表的句柄，保存了内存表中所有的记录，Tree根据声明宏在编译期确定是Arc或Rc
type BinMap = OrdMap<Tree<Bon, Bin>>;

//内存表
struct MemeryTab {
	pub prepare: Prepare,			//预提交
	pub root: BinMap,				//内存表的句柄
	pub tab: Atom,					//表名，例如"db/user"
	trans_count:	PrefCounter,	//事务计数
	prepare_count:	PrefCounter,	//预提交计数
	commit_count:	PrefCounter,	//提交计数
	rollback_count:	PrefCounter,	//回滚计数
	read_count:		PrefCounter,	//读计数
	read_byte:		PrefCounter,	//读字节
	write_count:	PrefCounter,	//写计数
	write_byte:		PrefCounter,	//写字节
	remove_count:	PrefCounter,	//删除计数
	remove_byte:	PrefCounter,	//删除字节
}

pub struct MemIter{
	_root: BinMap,
	_filter: Filter,
	point: usize,
	iter_count:		PrefCounter,	//迭代计数
	iter_byte:		PrefCounter,	//迭代字节
}

impl Drop for MemIter{
	fn drop(&mut self) {
        unsafe{Box::from_raw(self.point as *mut <Tree<Bin, Bin> as OIter<'_>>::IterType)};
    }
}

impl MemIter{
	pub fn new<'a>(tab: &Atom, root: BinMap, it: <Tree<Bon, Bin> as OIter<'a>>::IterType, filter: Filter) -> MemIter{
		MemIter{
			_root: root,
			_filter: filter,
			point: Box::into_raw(Box::new(it)) as usize,
			iter_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_ITER_COUNT_SUFFIX), 0).unwrap(),
			iter_byte: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_ITER_BYTE_COUNT_SUFFIX), 0).unwrap(),
		}
	}
}

impl Iter for MemIter{
	type Item = (Bin, Bin);
	fn next(&mut self) -> Option<NextResult<Self::Item>>{
		self.iter_count.sum(1);

		let mut it = unsafe{Box::from_raw(self.point as *mut <Tree<Bin, Bin> as OIter<'_>>::IterType)};
		// println!("MemIter next----------------------------------------------------------------");
		let r = Some(Ok(match it.next() {
			Some(&Entry(ref k, ref v)) => {
				self.iter_byte.sum(k.len() + v.len());

				Some((k.clone(), v.clone()))
			},
			None => None,
		}));
		mem::forget(it);
		r
	}
}

pub struct MemKeyIter{
	_root: BinMap,
	_filter: Filter,
	point: usize,
	iter_count:		PrefCounter,	//迭代计数
	iter_byte:		PrefCounter,	//迭代字节
}

impl Drop for MemKeyIter{
	fn drop(&mut self) {
        unsafe{Box::from_raw(self.point as *mut Keys<'_, Tree<Bin, Bin>>)};
    }
}

impl MemKeyIter{
	pub fn new(tab: &Atom, root: BinMap, keys: Keys<'_, Tree<Bon, Bin>>, filter: Filter) -> MemKeyIter{
		MemKeyIter{
			_root: root,
			_filter: filter,
			point: Box::into_raw(Box::new(keys)) as usize,
			iter_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_KEY_ITER_COUNT_SUFFIX), 0).unwrap(),
			iter_byte: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_KEY_ITER_BYTE_COUNT_SUFFIX), 0).unwrap(),
		}
	}
}

impl Iter for MemKeyIter{
	type Item = Bin;
	fn next(&mut self) -> Option<NextResult<Self::Item>>{
		self.iter_count.sum(1);

		let it = unsafe{Box::from_raw(self.point as *mut Keys<'_, Tree<Bin, Bin>>)};
		let r = Some(Ok(match unsafe{Box::from_raw(self.point as *mut Keys<'_, Tree<Bin, Bin>>)}.next() {
			Some(k) => {
				self.iter_byte.sum(k.len());

				Some(k.clone())
			},
			None => None,
		}));
		mem::forget(it);
		r
	}
}

#[derive(Clone)]
pub struct MemeryMetaTxn;

impl MemeryMetaTxn {
	//创建表、修改指定表的元数据
	pub async fn alter(&self, _tab: &Atom, _meta: Option<Arc<TabMeta>>) -> DBResult {
		Ok(())
	}

	//快照拷贝表
	pub async fn snapshot(&self, _tab: &Atom, _from: &Atom) -> DBResult {
		Ok(())
	}

	//修改指定表的名字
	pub async fn rename(&self, _tab: &Atom, _new_name: &Atom) -> DBResult {
		Ok(())
	} 

	//获得事务的状态
	pub fn get_state(&self) -> TxState {
		TxState::Ok
	}

	//预提交一个事务
	pub async fn prepare(&self, _timeout: usize) -> DBResult {
		Ok(())
	}

	//提交一个事务
	pub async fn commit(&self) -> CommitResult {
		Ok(XHashMap::with_capacity_and_hasher(0, Default::default()))
	}

	//回滚一个事务
	pub async fn rollback(&self) -> DBResult {
		Ok(())
	}
}
