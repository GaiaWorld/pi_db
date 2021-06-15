/**
 * 基于2pc的db管理器，每个db实现需要将自己注册到管理器上
 */

use std::{sync::Arc, env};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::mem;
use std::fmt;

use hash::XHashMap;
use ordmap::ordmap::{OrdMap, Entry, ImOrdMap, Keys};
use ordmap::asbtree::{Tree, new};
use atom::Atom;
use guid::{Guid, GuidGen};
use r#async::lock::mutex_lock::Mutex;
use r#async::rt::{AsyncRuntime, AsyncMapReduce, multi_thread::MultiTaskRuntime};
use bon::{ReadBuffer, Decode, Encode, WriteBuffer, ReadBonErr};

use crate::db::{SResult, IterResult, KeyIterResult, Filter, TabKV, TxCallback, TxState, Event, Bin, RwLog, TabMeta, CommitResult, DBResult};
use crate::memery_db::{MemDBSnapshot, MemDB, RefMemeryTxn, MemeryMetaTxn};
use crate::tabs::TxnType;
use crate::log_file_db::{LogFileDBSnapshot, RefLogFileTxn, LogFileMetaTxn, LogFileDB, DB_META_TAB_NAME};
use crate::fork::{ALL_TABLES, TableMetaInfo};

/**
* 表库及事务管理器
*/
// #[derive(Clone)]
// pub struct Mgr(Arc<Mutex<WareMap>>, Arc<GuidGen>);

#[derive(Clone)]
pub struct Mgr {
	ware_map: Arc<Mutex<WareMap>>,				//库映射表，根据类型分类
	guid: Arc<GuidGen>,							//Guid生成器
	// 所有的表分叉信息, 根据这些信息, 计算加载顺序
	// forks: XHashMap<String, TableMetaInfo>
}

unsafe impl Send for Mgr {}
unsafe impl Sync for Mgr {}

const TIMEOUT: usize = 100;

impl Mgr {
	/**
	* 构建表库及事务管理器
	* @param gen 全局唯一id生成器
	* @returns 返回表库及事务管理器管理器
	*/
	pub fn new(gen: GuidGen) -> Self {
		Self {
			ware_map: Arc::new(Mutex::new(WareMap::new())),
			guid: Arc::new(gen)
		}
	}

	/**
	* 浅拷贝，库表不同，共用同一个统计信息和GuidGen
	* 浅拷贝是指Clone了不同类型的数据库的内存OrderMap的Arc指针
	* @returns 返回Mgr的clone对象
	*/
	pub async fn shallow_clone(&self) -> Self {
		Self {
			ware_map: Arc::new(Mutex::new(self.ware_map.lock().await.wares_clone().await)),
			guid: self.guid.clone()
		}
	}

	/**
	* 深拷贝，库表及统计信息不同
	* @param clone_guid_gen 是否克隆guid
	* @returns 返回Mgr的clone对象
	*/
	pub fn deep_clone(&self, clone_guid_gen: bool) -> Self {
		let gen = if clone_guid_gen {
			self.guid.clone()
		}else{
			Arc::new(GuidGen::new(self.guid.node_time(), self.guid.node_id()))
		};
		Self {
			ware_map: Arc::new(Mutex::new(WareMap::new())),
			guid: gen
		}
	}

	/**
	* 注册库
	* @param ware_name 库名，即数据库的类型名，例如"logfile"
	* @param ware 库的实例，即不同类型数据库的实例
	* @returns 是否注册成功
	*/
	pub async fn register(&self, ware_name: Atom, ware: Arc<DatabaseWare>) -> bool {
		self.ware_map.lock().await.register(ware_name, ware)
	}

	/**
	* 取消注册数据库
	* @param ware_name 库名
	* @returns 是否取消注册成功
	*/
	pub async fn unregister(&mut self, ware_name: &Atom) -> bool {
		self.ware_map.lock().await.unregister(ware_name)
	}

	/**
	* 获取指定类型的库的指定表的元信息
	* @param ware_name 库名，即数据库的类型名，例如"logfile"
	* @param tab_name 表名，例如"db/user"
	* @returns 返回表的元信息
	*/
	pub async fn tab_info(&self, ware_name:&Atom, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		match self.find(ware_name).await {
			Some(b) => b.tab_info(tab_name).await,
			_ => None
		}
	}

	/**
	* 创建一个运行在指定异步运行时上的事务
	* @param writable 是否为写事务
	* @param rt	异步运行时
	* @returns 返回事务
	*/
	pub async fn transaction(&self, writable: bool, rt: Option<MultiTaskRuntime<()>>) -> Tr {
		let id = self.guid.gen(0);
        let mut map = XHashMap::default();
        let mut tmp = vec![];
        for Entry(k, v) in self.ware_map.lock().await.0.iter(None, false) {
            tmp.push((k.clone(), v));
        }

        for (k, v) in tmp {
            map.insert(k, v.snapshot().await);
        }

        let rt = rt.as_ref().unwrap().clone();

        Tr {
            writable,
            timeout: TIMEOUT,
            id: id.clone(),
            ware_log_map: map,
            state: TxState::Ok,
            rt: Some(rt),
            ..Default::default()
        }
	}

	/**
	* 获取所有的库类型名
	* @returns 库类型名集合
	*/
	pub async fn ware_name_list(&self) -> Vec<String> {
		let mut arr = Vec::new();
		let lock = self.ware_map.lock().await;
		let mut iter = lock.keys(None, false);
		loop {
			match iter.next() {
				Some(e) => arr.push(e.as_str().to_string()),
				None => break,
			}
		}
		arr
	}

	/**
	* 寻找指定的库
	* ware_name 库名，即数据库的类型名，例如"logfile"
	* @returns 库信息，即指定类型的数据库
	*/
	pub async fn find(&self, ware_name: &Atom) -> Option<Arc<DatabaseWare>> {
		let map = {
			self.ware_map.lock().await.clone()
		};
		map.find(ware_name)
	}
}

pub trait Monitor {
	fn notify(&self, event: Event, mgr: Mgr);
}


//库表，Atom表示库类型的原子描述，DatabaseWare表示数据库的句柄
#[derive(Clone)]
struct WareMap(OrdMap<Tree<Atom, Arc<DatabaseWare>>>);

impl fmt::Debug for WareMap {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "WareMap size: {:?}", self.0.size())
	}
}

/**
* 数据库快照的句柄
*/
enum DatabaseWareSnapshot {
	MemSnapshot(Arc<MemDBSnapshot>),			//内存数据库快照
	LogFileSnapshot(Arc<LogFileDBSnapshot>)		//日志文件数据库快照
}

impl DatabaseWareSnapshot {
	pub fn new_mem_ware_snapshot(snapshot: MemDBSnapshot) -> DatabaseWareSnapshot {
		DatabaseWareSnapshot::MemSnapshot(Arc::new(snapshot))
	}

	/**
	* 列出所有表
	* @returns 所有表的迭代器
	*/
	pub async fn list(&self) -> Box<dyn Iterator<Item=Atom>> {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				shot.list().await
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				shot.list().await
			}
		}
	}

	/**
	* 查询表的元信息
	* @param tab_name 表名
	* @returns 表元信息
	*/
	pub async fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				shot.tab_info(tab_name).await
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				shot.tab_info(tab_name).await
			}
		}
	}

	/**
	* 检查表是否可以创建
	* @param tab 表名
	* @param meta 表元信息
	* @returns
	*/
	pub fn check(&self, tab: &Atom, meta: &Option<Arc<TabMeta>>) -> DBResult {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				shot.check(tab, meta)
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				shot.check(tab, meta)
			}
		}
	}

	/**
	* 创建或删除表
	* @param tab_name 表名
	* @param meta 表元信息, 如果为None， 表示删除表
	* @returns
	*/
	pub async fn alter(&self, tab_name: &Atom, meta: Option<Arc<TabMeta>>) {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				shot.alter(tab_name, meta).await
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				shot.alter(tab_name, meta).await
			}
		}
	}

	/**
	* 创建表事务
	* @param tab_name 表名
	* @param id 事务的唯一标识id
	* @param writable 是否是可写事务
	* @returns
	*/
	pub async fn tab_txn(&self, tab_name: &Atom, id: &Guid, writable: bool) -> SResult<Arc<DatabaseTabTxn>> {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				match shot.tab_txn(tab_name, id, writable).await {
					Ok(t) => {
						match t {
							TxnType::MemTxn(t1) => Ok(Arc::new(DatabaseTabTxn::MemTabTxn(t1))),
							TxnType::LogFileTxn(t1) => Ok(Arc::new(DatabaseTabTxn::LogFileTabTxn(t1)))
						}
					}
					Err(e) => Err(e)
				}
				
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				match shot.tab_txn(tab_name, id, writable).await {
					Ok(t) => {
						match t {
							TxnType::MemTxn(t1) => Ok(Arc::new(DatabaseTabTxn::MemTabTxn(t1))),
							TxnType::LogFileTxn(t1) => Ok(Arc::new(DatabaseTabTxn::LogFileTabTxn(t1)))
						}
					}
					Err(e) => Err(e)
				}
			}
		}
	}

	/**
	* 创建元信息表事务
	* @param id 事务的唯一标识id
	* @returns 数据库元信息事务
	*/
	pub fn meta_txn(&self, id: &Guid) -> Arc<DatabaseMetaTxn> {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				Arc::new(DatabaseMetaTxn::MemMetaTxn(shot.meta_txn(id)))
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				Arc::new(DatabaseMetaTxn::LogFileMetaTxn(shot.meta_txn(id)))
			}
		}
	}

	/**
	* 预提交元信息表事务
	* @param id 事务的唯一标识id
	* @returns
	*/
	pub async fn prepare(&self, id: &Guid) -> DBResult {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				shot.prepare(id).await
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				shot.prepare(id).await
			}
		}
	}

	/**
	* 提交元信息表事务
	* @param id 事务的唯一标识id
	* @returns
	*/
	pub async fn commit(&self, id: &Guid) {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				shot.commit(id).await
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				shot.commit(id).await
			}
		}
	}

	/**
	* 回滚元信息表事务
	* @param id 事务的唯一标识id
	* @returns
	*/
	pub async fn rollback(&self, id: &Guid) {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				shot.rollback(id).await
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				shot.rollback(id).await
			}
		}
	}

	//通知元信息表已修改，暂时未实现
	pub fn notify(&self, _event: Event) {}
}

/**
* 数据库的句柄，跨线程安全
*/
pub enum DatabaseWare {
	MemWare(Arc<MemDB>),			//内存数据库
	LogFileWare(Arc<LogFileDB>)		//日志文件数据库
}

unsafe impl Send for DatabaseWare {}
unsafe impl Sync for DatabaseWare {}

impl DatabaseWare {
	/**
	* 创建内存库
	* @param db 内存数据库实例
	* @returns
	*/
	pub fn new_mem_ware(db: MemDB) -> DatabaseWare {
		DatabaseWare::MemWare(Arc::new(db))
	}

	/**
	* 创建日志文件数据库
	* @param db 日志文件数据库实例
	* @returns
	*/
	pub fn new_log_file_ware(db: LogFileDB) -> DatabaseWare {
		DatabaseWare::LogFileWare(Arc::new(db))
	}

	/**
	* 拷贝全部的表
	* @returns
	*/
	async fn tabs_clone(&self) -> Arc<DatabaseWare> {
		match self {
			DatabaseWare::MemWare(memdb) => {
				let cloned = memdb.tabs_clone().await;
				Arc::new(DatabaseWare::MemWare(cloned))
			}
			DatabaseWare::LogFileWare(logfiledb) => {
				let cloned = logfiledb.tabs_clone().await;
				Arc::new(DatabaseWare::LogFileWare(cloned))
			}
		}
	}

	/**
	* 获取当前数据库的快照句柄
	* @returns
	*/
	async fn snapshot(&self) -> Arc<DatabaseWareSnapshot> {
		match self {
			DatabaseWare::MemWare(memdb) => {
				let shot = memdb.snapshot().await;
				Arc::new(DatabaseWareSnapshot::MemSnapshot(shot))
			}
			DatabaseWare::LogFileWare(logfiledb) => {
				let shot = logfiledb.snapshot().await;
				Arc::new(DatabaseWareSnapshot::LogFileSnapshot(shot))
			}
		}
	}

	/**
	* 获取表的元信息
	* @param tab_name 表名，例如"db/user"
	* @returns 表的元信息
	*/
	async fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		match self {
			DatabaseWare::MemWare(memdb) => {
				memdb.tab_info(tab_name).await
			}
			DatabaseWare::LogFileWare(logfiledb) => {
				logfiledb.tab_info(tab_name).await
			}
		}
	}

	/**
	* 列出库的所有表
	* @returns 所有表的迭代器
	*/
	async fn list(&self) -> Box<dyn Iterator<Item=Atom>> {
		match self {
			DatabaseWare::MemWare(memdb) => {
				memdb.list().await
			}
			DatabaseWare::LogFileWare(logfiledb) => {
				logfiledb.list().await
			}
		}
	}

	/**
	* 数据库事务超时时间
	* @returns 数据库事务超时时间
	*/
	fn timeout(&self) -> usize {
		match self {
			DatabaseWare::MemWare(memdb) => {
				memdb.timeout()
			}
			DatabaseWare::LogFileWare(logfiledb) => {
				logfiledb.timeout()
			}
		}
	}
}

/**
 * 数据库表事务
*/
pub enum DatabaseTabTxn {
	MemTabTxn(Arc<RefMemeryTxn>),		//内存表事务
	LogFileTabTxn(Arc<RefLogFileTxn>)	//日志文件表事务
}

impl DatabaseTabTxn {
	/**
	* 创建内存表事务
	* @param txn 内存表事务
	* @returns 内存表事务
	*/
	fn new_mem_tab_txn(txn: RefMemeryTxn) -> DatabaseTabTxn {
		DatabaseTabTxn::MemTabTxn(Arc::new(txn))
	}

	/**
	* 创建日志文件表事务
	* @param txn 日志文件表事务
	* @returns 文件表事务
	*/
	fn new_log_file_tab_txn(txn: RefLogFileTxn) -> DatabaseTabTxn {
		DatabaseTabTxn::LogFileTabTxn(Arc::new(txn))
	}

	/**
	* 获取事务状态
	* @returns 事务状态
	*/
	async fn get_state(&self) -> TxState {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.get_state().await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.get_state().await
			}
		}
	}

	/**
	* 预提交事务
	* @param timeout 预提交超时时间
	* @returns 预提交结果
	*/
	pub async fn prepare(&self, timeout: usize) -> DBResult {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.prepare(timeout).await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.prepare(timeout).await
			}
		}
	}

	/**
	* 提交事务
	* @returns 提交结果
	*/
	pub async fn commit(&self) -> CommitResult {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.commit().await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.commit().await
			}
		}
	}

	/**
	* 回滚事务
	* @returns 回滚事务结果
	*/
	pub async fn rollback(&self) -> DBResult {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.rollback().await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.rollback().await
			}
		}
	}

	pub async fn key_lock(&self, _arr: Arc<Vec<TabKV>>, _lock_time: usize, _readonly: bool) -> DBResult {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.key_lock(_arr, _lock_time, _readonly).await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.key_lock(_arr, _lock_time, _readonly).await
			}
		}
	}

	/**
	* 数据库查询
	* @param arr 查询参数
	* @param _lock_time 锁定时间
	* @param _readonly 是否只读
	* @returns 查询结果
	*/
	pub async fn query(
		&self,
		arr: Arc<Vec<TabKV>>,
		_lock_time: Option<usize>,
		_readonly: bool
	) -> SResult<Vec<TabKV>> {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.query(arr, _lock_time, _readonly).await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.query(arr, _lock_time, _readonly).await
			}
		}
	}

	/**
	* 数据库修改
	* @param arr 修改参数
	* @param _lock_time 锁定时间
	* @param _readonly 是否只读
	* @returns 修改结果
	*/
	pub async fn modify(&self, arr: Arc<Vec<TabKV>>, _lock_time: Option<usize>, _readonly: bool) -> DBResult {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.modify(arr, _lock_time, _readonly).await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.modify(arr, _lock_time, _readonly).await
			}
		}
	}

	/**
	* 数据库迭代
	* @param tab 要迭代的表
	* @param key 迭代的起始key
	* @param descending 是否降序迭代
	* @param filter 过滤规则
	* @returns 数据库迭代器
	*/
	pub async fn iter(
		&self,
		tab: &Atom,
		key: Option<Bin>,
		descending: bool,
		filter: Filter
	) -> IterResult {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.iter(tab, key, descending, filter).await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.iter(tab, key, descending, filter).await
			}
		}
	}

	/**
	* 数据库键迭代
	* @param key 迭代的起始key
	* @param descending 是否降序迭代
	* @param filter 过滤规则
	* @returns 数据库键迭代器
	*/
	pub async fn key_iter(
		&self,
		key: Option<Bin>,
		descending: bool,
		filter: Filter
	) -> KeyIterResult {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.key_iter(key, descending, filter).await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.key_iter(key, descending, filter).await
			}
		}
	}

	/**
	* 数据库索引(未实现)
	* @returns 索引迭代器
	*/
	pub fn index(
		&self,
		_tab: &Atom,
		_index_key: &Atom,
		_key: Option<Bin>,
		_descending: bool,
		_filter: Filter,
	) -> IterResult {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.index(_tab, _index_key, _key, _descending, _filter)
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.index(_tab, _index_key, _key, _descending, _filter)
			}
		}
	}

	/**
	* 数据库表记录数
	* @returns 数据库表记录数量
	*/
	pub async fn tab_size(&self) -> SResult<usize> {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.tab_size().await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.tab_size().await
			}
		}
	}

	pub async fn fork_prepare(&self, ware: Atom, tab_name: Atom, fork_tab_name: Atom, meta: TabMeta) -> DBResult {
		match self {
			DatabaseTabTxn::MemTabTxn(_) => unimplemented!(),
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.fork_prepare(ware, tab_name, fork_tab_name, meta).await
			}
		}
	}

	pub async fn fork_commit(&self, ware: Atom, tab_name: Atom, fork_tab_name: Atom, meta: TabMeta) -> DBResult {
		match self {
			DatabaseTabTxn::MemTabTxn(_) => unimplemented!(),
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.fork_commit(ware, tab_name, fork_tab_name, meta).await
			}
		}
	}

	pub async fn fork_rollback(&self) -> DBResult {
		match self {
			DatabaseTabTxn::MemTabTxn(_) => unimplemented!(),
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.fork_rollback().await
			}
		}
	}

	pub async fn force_fork(&self) -> std::io::Result<usize> {
		match self {
			DatabaseTabTxn::MemTabTxn(_) => unimplemented!(),
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.force_fork().await
			}
		}
	}
}

/**
* 数据库元信息事务
*/
enum DatabaseMetaTxn {
	// 内存数据库元信息事务
	MemMetaTxn(Arc<MemeryMetaTxn>),
	// 日志文件数据库元信息事务
	LogFileMetaTxn(Arc<LogFileMetaTxn>)
}

impl DatabaseMetaTxn {
	/**
	* 创建内存数据库元信息事务
	* @param txn 内存数据库元事务
	* @returns 索引迭代器
	*/
	fn new_mem_meta_txn(txn: MemeryMetaTxn) -> DatabaseMetaTxn {
		DatabaseMetaTxn::MemMetaTxn(Arc::new(txn))
	}

	/**
	* 创建日志文件数据库元信息事务
	* @param txn 日志文件数据库元事务
	* @returns 索引迭代器
	*/
	fn new_log_file_meta_txn(txn: LogFileMetaTxn) -> DatabaseMetaTxn {
		DatabaseMetaTxn::LogFileMetaTxn(Arc::new(txn))
	}

	/**
	* 修改表的元信息
	* @param _tab 表名
	* @param _meta 元信息
	* @returns 修改结果
	*/
	async fn alter(&self, tab_name: &Atom, meta: Option<Arc<TabMeta>>) -> DBResult {
		match self {
			DatabaseMetaTxn::MemMetaTxn(txn) => {
				txn.alter(tab_name, meta).await
			}
			DatabaseMetaTxn::LogFileMetaTxn(txn) => {
				txn.alter(tab_name, meta).await
			}
		}
	}

	/**
	* 元信息表快照
	* @param _tab 表名
	* @param _from 表名
	* @returns 快照结果
	*/
	async fn snapshot(&self, _tab: &Atom, _from: &Atom) -> DBResult {
		match self {
			DatabaseMetaTxn::MemMetaTxn(txn) => {
				txn.snapshot(_tab, _from).await
			}
			DatabaseMetaTxn::LogFileMetaTxn(txn) => {
				txn.snapshot(_tab, _from).await
			}
		}
	}

	/**
	* 修改表名
	* @param _tab 旧表名
	* @param _new_name 新表名
	* @returns 快照结果
	*/
	async fn rename(&self, _tab: &Atom, _new_name: &Atom) -> DBResult {
		match self {
			DatabaseMetaTxn::MemMetaTxn(txn) => {
				txn.rename(_tab, _new_name).await
			}
			DatabaseMetaTxn::LogFileMetaTxn(txn) => {
				txn.rename(_tab, _new_name).await
			}
		}
	}

	/**
	* 获取元信息事务状态
	* @returns 状态
	*/
	fn get_state(&self) -> TxState {
		TxState::Ok
	}

	/**
	* 元信息事务预提交
	* @param _timeout 预提交超时时间
	* @returns 预提交结果
	*/
	async fn prepare(&self, _timeout: usize) -> DBResult {
		match self {
			DatabaseMetaTxn::MemMetaTxn(txn) => {
				txn.prepare(_timeout).await
			}
			DatabaseMetaTxn::LogFileMetaTxn(txn) => {
				txn.prepare(_timeout).await
			}
		}
	}

	/**
	* 元信息事务提交
	* @returns 提交结果
	*/
	async fn commit(&self) -> CommitResult {
		match self {
			DatabaseMetaTxn::MemMetaTxn(txn) => {
				txn.commit().await
			}
			DatabaseMetaTxn::LogFileMetaTxn(txn) => {
				txn.commit().await
			}
		}

	}

	/**
	* 元信息事务回滚
	* @returns 回滚提交结果
	*/
	async fn rollback(&self) -> DBResult {
		match self {
			DatabaseMetaTxn::MemMetaTxn(txn) => {
				txn.rollback().await
			}
			DatabaseMetaTxn::LogFileMetaTxn(txn) => {
				txn.rollback().await
			}
		}
	}
}

impl WareMap {
	fn new() -> Self {
		WareMap(OrdMap::new(new()))
	}

	async fn wares_clone(&self) -> Self{
		let mut wares = Vec::new();
		for ware in self.0.iter(None, false){
			wares.push(Entry(ware.0.clone(), ware.1.tabs_clone().await));
		}
		WareMap(OrdMap::new(Tree::from_order(wares)))
	}

	fn register(&mut self, ware_name: Atom, ware: Arc<DatabaseWare>) -> bool {
		self.0.insert(ware_name, ware)
	}

	fn unregister(&mut self, ware_name: &Atom) -> bool {
		match self.0.delete(ware_name, false) {
			Some(_) => true,
			_ => false,
		}
	}

	//通过库的类型名，查找指定类型的数据库
	fn find(&self, ware_name: &Atom) -> Option<Arc<DatabaseWare>> {
		match self.0.get(&ware_name) {
			Some(b) => Some(b.clone()),
			_ => None
		}
	}

	fn keys(&self, key: Option<&Atom>, descending: bool) -> Keys<Tree<Atom, Arc<DatabaseWare>>> {
		self.0.keys(key, descending)
	}
}

/*
* 事务，所有独立事务的抽象
*/
#[derive(Default)]
pub struct Tr {
	writable: bool,																//是否是可写事务
	timeout: usize, 															//事务的预提交的超时时长，暂时未使用
	id: Guid,																	//事务id
	ware_log_map: XHashMap<Atom, Arc<DatabaseWareSnapshot>>,					//库类型名对应的库快照
	state: TxState,																//事务状态
	tab_txns: XHashMap<(Atom, Atom), Arc<DatabaseTabTxn>>, 						//数据库表事务映射表
	meta_txns: XHashMap<Atom, Arc<DatabaseMetaTxn>>, 							//元信息表事务映射表
	fork_txns: XHashMap<(Atom, Atom, Atom), (TabMeta, Arc<DatabaseTabTxn>)>, 	//这个事务中产生的所有分叉操作
	rt: Option<MultiTaskRuntime<()>>,											//异步运行时
}

impl Tr {
	/**
	* 获得事务的状态
	* @returns 事务状态
	*/
	pub fn get_state(&self) -> TxState {
		self.state.clone()
	}

	/**
	* 数据库元信息操作
	* @param ware_name 库类型名
	* @param tab_name 表名
	* @param meta 表元信息， None表示删除表，如果对应的库类型名和表名存在，则修改对应表在元信息表中的元信息，如果对应的库类型名存在而表名不存在，则插入对应表的元信息到元信息表中，如果库类型名不存在，则返回错误
	* @returns 元信息操作结果
	*/
	pub async fn alter(&mut self, ware_name: &Atom, tab_name: &Atom, meta: Option<Arc<TabMeta>>) -> DBResult {
		self.state = TxState::Doing;
		let ware = match self.ware_log_map.get(ware_name) {
			Some(w) => match w.check(tab_name, &meta) { // 检查
				Ok(_) =>{
					w.alter(tab_name, meta.clone()).await;
					w
				},
				Err(s) => {
					self.state = TxState::Err;
					return Err(s)
				}
			},
			_ => {
				self.state = TxState::Err;
				return Err(format!("ware not found:{}", ware_name.as_str()))
			}
		};
		let id = &self.id;
		let txn = self.meta_txns.entry(ware_name.clone()).or_insert_with(|| {
			ware.meta_txn(&id)
		}).clone();

		match txn.alter(tab_name, meta.clone()).await {
			Ok(r) => {
				self.state = TxState::Ok;
				Ok(r)
			}
			Err(e) => {
				self.state = TxState::Err;
				Err(e)
			}
		}
	}

	/**
	* 表改名(未实现)
	* @returns 改名结果
	*/
	pub fn rename(&mut self, _ware_name: &Atom, _old_name: &Atom, _new_name: Atom, _cb: TxCallback) -> DBResult {
		self.state = TxState::Doing;
		Ok(())
	}

	/**
	* 获取表的元信息
	* @param ware_name 库名
	* @param tab_name 表名
	* @returns 表元信息
	*/
	pub async fn tab_info(&self, ware_name:&Atom, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		match self.ware_log_map.get(ware_name) {
			Some(ware) => ware.tab_info(tab_name).await,
			_ => None
		}
	}

	/**
	* 列出指定库的所有表
	* @param ware_name 库名
	* @returns 所有表的集合
	*/
	pub async fn list(&self, ware_name: &Atom) -> Option<Vec<String>> {
		match self.ware_log_map.get(ware_name) {
			Some(ware) => {
				let mut arr = Vec::new();
				for e in ware.list().await {
					arr.push(e.to_string())
				}
				Some(arr)
			},
			_ => None
		}
	}

	/**
	* 构建表事务
	* @param ware_name 库类型名
	* @param tab_name 表名
	* @returns 数据库表事务
	*/
	async fn build(&mut self, ware_name: &Atom, tab_name: &Atom) -> SResult<Arc<DatabaseTabTxn>> {
		let txn_key = (ware_name.clone(), tab_name.clone());
		let txn = match self.tab_txns.get(&txn_key) {
			Some(r) => return Ok(r.clone()),
			_ => match self.ware_log_map.get(ware_name) {
				Some(ware) => match ware.tab_txn(tab_name, &self.id, self.writable).await {
					Ok(txn) => txn,
					Err(e) =>{
						self.state = TxState::Err;
						return Err(e)
					}
				},
				_ => return Err(format!("WareNotFound: {:?}", ware_name))
			}
		};
		self.tab_txns.insert(txn_key, txn.clone());
		Ok(txn)
	}

	/**
	* 数据库查询
	* @param arr 查询参数
	* @param lock_time 锁定时间
	* @param read_lock 是否读锁
	* @returns 查询结果
	*/
	pub async fn query(
		&mut self,
		arr: Vec<TabKV>,
		lock_time: Option<usize>,
		read_lock: bool
	) -> SResult<Vec<TabKV>> {
		let len = arr.len();
		if arr.len() == 0 {
			return Ok(Vec::new())
		}
		self.state = TxState::Doing;
		// 创建指定长度的结果集，接收结果
		let mut vec = Vec::with_capacity(len);
		vec.resize(len, Default::default());
		let rvec = Arc::new(Mutex::new((len, vec)));
		let mut res = vec![];

		let map = tab_map(arr);
		for ((ware_name, tab_name), val) in map.into_iter() {
			let tkv = Arc::new(val);
			match self.build(&ware_name, &tab_name).await {
				Ok(t) => match t.query(tkv, lock_time, read_lock).await {
					Ok(vec) => {
						match merge_result(&rvec, vec).await {
							Ok(v) => {
								self.state = TxState::Ok;
								res.extend(v.into_iter());
							}
							Err(e) => {
								self.state = TxState::Err;
								break
							}
						}
					}
					Err(e) => {
						self.state = TxState::Err;
						break
					}
				},
				Err(e) => return Err(e)
			}
		}

		if self.state == TxState::Ok {
			Ok(res)
		} else {
			Err("Tr::query error".to_string())
		}
	}

	/**
	* 数据库修改
	* @param arr 修改参数
	* @param lock_time 锁定时间
	* @param read_lock 是否读锁
	* @returns 查询结果
	*/
	pub async fn modify(&mut self, arr: Vec<TabKV>, lock_time: Option<usize>, read_lock: bool) -> DBResult {
		if arr.len() == 0 {
			return Ok(())
		}

		// 保存每个txid的修改
		let arr_len = arr.len();
		let map = tab_map(arr);
		self.state = TxState::Doing;

		let rt = (*self.rt.as_ref().unwrap()).clone();
		let mut async_map = rt.map_reduce(arr_len);

		let mut txns = vec![];
		for ((ware_name, tab_name), val) in map.into_iter() {
			match self.build(&ware_name, &tab_name).await {
				Ok(txn) => {
					txns.push((txn, val));
				}
				Err(e) => {
					return Err(e.to_string());
				}
			}
		}

		for (txn, tkv) in txns {
			async_map.map(AsyncRuntime::Multi(rt.clone()), async move {
				match txn.modify(Arc::new(tkv), lock_time, read_lock).await {
					Ok(_) => {
						Ok(true)
					}
					Err(e) => {
						Ok(false)
					}
				}
			});
		}

		let mut error = false;
		match async_map.reduce(true).await {
			Ok(res) => {
				for r in res {
					if r.is_ok() && !r.unwrap() {
						error = true;
						break;
					}
				}
			}
			Err(e) => {
				error = true;
			}
		}

		if error {
			Err("modify failed".to_string())
		} else {
			Ok(())
		}
	}

	/**
	* 数据库迭代
	* @param ware 迭代的库名
	* @param tab 表名
	* @param key 键
	* @param descending 是否降序迭代
	* @param filter 迭代过滤器
	* @returns 迭代器
	*/
	pub async fn iter(&mut self, ware: &Atom, tab: &Atom, key: Option<Bin>, descending: bool, filter: Filter) -> IterResult {
		self.state = TxState::Doing;
		match self.build(&ware, &tab).await {
			Ok(t) => {
				match t.iter(&tab, key, descending, filter).await {
					Ok(iter) => {
						self.state = TxState::Ok;
						Ok(iter)
					}
					Err(e) => {
						self.state = TxState::Err;
						Err(e)
					}
				}
			}
			Err(e) => Err(e)
		}
	}

	/**
	* 数据库键迭代
	* @param ware 迭代的库名
	* @param tab 表名
	* @param key 键
	* @param descending 是否降序迭代
	* @param filter 迭代过滤器
	* @returns 键迭代器
	*/
	pub async fn key_iter(&mut self, ware: &Atom, tab: &Atom, key: Option<Bin>, descending: bool, filter: Filter) -> KeyIterResult {
		self.state = TxState::Doing;
		match self.build(&ware, &tab).await {
			Ok(t) => {
				match t.key_iter(key, descending, filter).await {
					Ok(iter) => {
						self.state = TxState::Ok;
						Ok(iter)
					}
					Err(e) => {
						self.state = TxState::Err;
						Err(e)
					}
				}
			}
			Err(e) => Err(e)
		}
	}

	/**
	* 数据库表的记录数
	* @param ware_name 库名
	* @param tab_name 表名
	* @returns 数据库表的记录数
	*/
	pub async fn tab_size(&mut self, ware_name: &Atom, tab_name: &Atom) -> SResult<usize> {
		self.state = TxState::Doing;
		match self.build(ware_name, tab_name).await {
			Ok(t) => {
				match t.tab_size().await {
					Ok(size) => {
						self.state = TxState::Ok;
						Ok(size)
					}
					Err(e) => {
						self.state = TxState::Err;
						Err(e)
					}
				}
			}
			Err(e) => Err(e)
		}
	}

	/**
	* 预提交事务
	* @returns 预提交结果
	*/
	pub async fn prepare(&mut self) -> DBResult {
		//如果预提交内容为空，直接返回预提交成功
		if self.meta_txns.len() == 0 && self.tab_txns.len() == 0 {
			self.state = TxState::PreparOk;
			return Ok(());
		}
		self.state = TxState::Preparing;

		//检查并插入元信息表事务在表管理器中的预提交信息
		let alter_len = self.meta_txns.len();
		if alter_len > 0 {
			for ware in self.meta_txns.keys() {
				match self.ware_log_map.get_mut(ware).unwrap().prepare(&self.id).await {
					Err(s) =>{
						self.state = TxState::PreparFail;
						return Err(s)
					},
					_ => ()
				}
			}
		}
		let len = self.tab_txns.len() + alter_len + self.fork_txns.len() ;
		let count = Arc::new(AtomicUsize::new(len));

		//处理每个数据库表的预提交
		for val in self.tab_txns.values() {
			match val.prepare(self.timeout).await {
				Ok(_) => {
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						self.state = TxState::PreparOk;
						return Ok(())
					}
				}
				Err(e) => {
					self.state = TxState::PreparFail;
					return Err(e);
				}
			}
		}

		//处理元信息表alter的预提交(预提交不会修改元信息表，所以暂时是空实现)
		for val in self.meta_txns.values() {
			match val.prepare(self.timeout).await {
				Ok(_) => {
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						self.state = TxState::PreparOk;
						return Ok(())
					}
				}
				Err(e) => {
					self.state = TxState::PreparFail;
					return Err(e)
				}
			}
		}

		//处理每个表事务的分叉预提交
		for (k, v) in self.fork_txns.iter() {
			match v.1.fork_prepare(k.0.clone(), k.1.clone(), k.2.clone(), v.0.clone()).await {
				Ok(_) => {
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						self.state = TxState::PreparOk;
						return Ok(())
					}
				}
				Err(e) => {
					self.state = TxState::PreparFail;
					return Err(e)
				}
			}
		}

		Ok(())
	}

	/**
	* 提交事务
	* @returns 提交结果
	*/
	pub async fn commit(&mut self) -> DBResult {
		self.state = TxState::Committing;
		//检查并移除元信息表事务在表管理器中的预提交信息
		let alter_len = self.meta_txns.len();
		if alter_len > 0 {
			for ware in self.meta_txns.keys() {
				self.ware_log_map.get(ware).unwrap().commit(&self.id).await;
			}
		}
		let len = self.tab_txns.len() + alter_len + self.fork_txns.len();
		if len == 0 {
			return Ok(())
		}
		// println!(" ======== pi_db::mgr::commit txid: {:?}, alter_len: {:?}, tab_txn_len: {:?}", self.id.time(), alter_len, self.tab_txns.len());
		let count = Arc::new(AtomicUsize::new(len));
		let rt = (*self.rt.as_ref().unwrap()).clone();
		let mut async_map = rt.map_reduce(self.tab_txns.len());

		//提交数据库表的事务
		for (txn_name, val) in self.tab_txns.iter_mut() {
			let val = val.clone();
			async_map.map(AsyncRuntime::Multi(rt.clone()), async move {
				match val.commit().await {
					Ok(logs) => {
						Ok(true)
					}
					Err(e) => {
						Ok(false)
					}
				}
			});
		}

		match async_map.reduce(true).await {
			Ok(res) => {
				for r in res {
					if r.is_ok() && r.unwrap() {
						if count.fetch_sub(1, Ordering::SeqCst) == 1 {
							self.state = TxState::Commited;
							return Ok(())
						}
					}
				}
			}
			Err(e) => {
				self.state = TxState::CommitFail;
				return Err(e.to_string())
			}
		}

		//提交元信息表的事务，并修改内存和文件数据
		for val in self.meta_txns.values() {
			match val.commit().await {
				Ok(_) => {
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						self.state = TxState::Commited;
						return Ok(())
					}
				}
				Err(e) => {
					self.state = TxState::CommitFail;
					return Err(e)
				}
			}
		}

		// 处理表分叉的提交
		for (k, v) in self.fork_txns.iter() {
			match v.1.fork_commit(k.0.clone(), k.1.clone(), k.2.clone(), v.0.clone()).await {
				Ok(_) => {
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						self.state = TxState::Commited;
						return Ok(())
					}
				}
				Err(e) => {
					self.state = TxState::CommitFail;
					return Err(e)
				}
			}
		}

		Ok(())
	}

	/**
	* 回滚事务
	* @returns 回滚事务结果
	*/
	pub async fn rollback(&mut self) -> DBResult {
		self.state = TxState::Rollbacking;
		//先移除元信息表事务在表管理器中的预提交信息
		let alter_len = self.meta_txns.len();
		if alter_len > 0 {
			for ware in self.meta_txns.keys() {
				self.ware_log_map.get(ware).unwrap().rollback(&self.id).await;
			}
		}
		let len = self.tab_txns.len() + alter_len + self.fork_txns.len();
		let count = Arc::new(AtomicUsize::new(len));
		
		//处理数据库表的回滚
		for val in self.tab_txns.values() {
			match val.rollback().await {
				Ok(()) => {
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						self.state = TxState::Rollbacked;
						return Ok(())
					}
				}
				Err(e) => {
					self.state = TxState::RollbackFail;
					return Err(e)
				}
			}
		}

		//回滚元信息表上的事务(回滚不会修改元信息表，所以暂时是空实现)
		for val in self.meta_txns.values() {
			match val.rollback().await {
				Ok(()) => {
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						self.state = TxState::Rollbacked;
						return Ok(())
					}
				}
				Err(e) => {
					self.state = TxState::RollbackFail;
					return Err(e)
				}
			}
		}

		// 处理表分叉的回滚
		for (k, v) in self.fork_txns.iter() {
			match v.1.fork_rollback().await {
				Ok(()) => {
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						self.state = TxState::Rollbacked;
						return Ok(())
					}
				}
				Err(e) => {
					self.state = TxState::RollbackFail;
					return Err(e)
				}
			}
		}

		Ok(())
	}

	///创建指定数据库表的一个分叉表，允许分叉后的表有新的元信息(主键和值类型)
	///原表的log会立即强制分裂，生成一个新的log文件和文件id，之前的数据就是两个表的公共数据
	///强制分裂后的最新的只读log文件就是分叉点
	pub async fn fork_tab(&mut self, ware: Atom, tab_name: Atom, fork_tab_name: Atom, new_meta: TabMeta) -> DBResult {
		//判断本事务中是否有冲突的分叉表名，例如
		if let Some(_) = self.fork_txns.get(&(ware.clone(), tab_name.clone(), fork_tab_name.clone()))  {
			return Err("duplicate fork tab name".to_string())
		}
		let txn = match self.ware_log_map.get(&ware) {
			Some(ware) => match ware.tab_txn(&tab_name, &self.id, self.writable).await {
				Ok(txn) => txn,
				Err(e) =>{
					return Err(e)
				}
			},
			_ => return Err(String::from("WareNotFound"))
		};
		self.fork_txns.insert((ware, tab_name, fork_tab_name), (new_meta, txn));

		Ok(())
	}

	/**
	* 记录锁(未实现)
	* @returns
	*/
	async fn key_lock(&mut self, tr: &Tr, arr: Vec<TabKV>, lock_time: usize, read_lock: bool) -> DBResult {
		Ok(())
	}
}

//================================ 内部静态方法
// 创建每表的键参数表，不负责键的去重
fn tab_map(mut arr: Vec<TabKV>) -> XHashMap<(Atom, Atom), Vec<TabKV>> {
	let mut len = arr.len();
	let mut map = XHashMap::with_capacity_and_hasher(len * 3 / 2, Default::default());
	while len > 0 {
		let mut tk = arr.pop().unwrap();
		tk.index = len;
		len -= 1;
		let r = map.entry((tk.ware.clone(), tk.tab.clone())).or_insert(Vec::new());
		r.push(tk);
	}
	return map;
}

// 合并结果集
#[inline]
async fn merge_result(rvec: &Arc<Mutex<(usize, Vec<TabKV>)>>, vec: Vec<TabKV>) -> SResult<Vec<TabKV>> {
	let mut t = rvec.lock().await;
	t.0 -= vec.len();
	for r in vec.into_iter() {
		let i = (&r).index - 1;
		t.1[i] = r;
	}
	if t.0 == 0 {
		// 将结果集向量转移出来，没有拷贝
		return Ok(mem::replace(&mut t.1, Vec::new()));
	}

	Ok(vec![])
}
