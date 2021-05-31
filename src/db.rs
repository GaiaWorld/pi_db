/**
 * DB的定义
 */

use std::result::Result;
use std::sync::Arc;
use std::vec::Vec;
use std::ops::{Deref};
use std::cmp::{Ord, Eq, PartialOrd, PartialEq, Ordering};

use atom::Atom;
use sinfo::EnumType;
use bon::{ReadBuffer, Decode, Encode, WriteBuffer, ReadBonErr};
use hash::XHashMap;

/*
* 二进制数据，主键或值
*/
pub type Bin = Arc<Vec<u8>>;

/*
* 结果
*/
pub type SResult<T> = Result<T, String>;

/*
* 结果
*/
pub type DBResult = SResult<()>;

/*
* 事务提交结果，Bin表示提交成功的主键的二进制，RwLog表示事务的操作日志
*/
pub type CommitResult = SResult<XHashMap<Bin, RwLog>>;

/*
* 表的记录迭代器
*/
pub type IterResult = SResult<Box<dyn Iter<Item = (Bin, Bin)> + Send>>;

/*
* 表的主键迭代器
*/
pub type KeyIterResult = SResult<Box<dyn Iter<Item = Bin>>>;

pub type NextResult<T> = SResult<Option<T>>;

pub type TxCallback = Arc<dyn Fn(DBResult)>;
pub type TxQueryCallback = Arc<dyn Fn(SResult<Vec<TabKV>>)>;

// 这个类型定义了，但从未使用，没实现Send， 不能在多线程运行时使用
// pub type Filter = Option<Arc<dyn Fn(Bin)-> Option<Bin>>>;
pub type Filter = Option<bool>;

/**
* 表的元信息
* 注：因为当前pi_pt是在ts层自动生成表元信息对应的Class，并为Class生成了对应的序列化和反序列化方法，所以底层必没有使用解码后的表元信息进行序列化和反序列化
* 但分叉需要读取和修改底层的表元信息
*/
#[derive(Debug, Clone)]
pub struct TabMeta {
	pub k: EnumType,	//表的主键类型
	pub v: EnumType		//表的值类型
}

impl PartialEq for TabMeta {
	fn eq(&self, other: &Self) -> bool {
		if self.k == other.k && self.v == other.v {
			true
		} else {
			false
		}
	}
}

impl Eq for TabMeta {}

impl Default for TabMeta {
	//默认表的主键和值类型都是字符串
	fn default() -> Self {
		TabMeta {
			k: EnumType::Str,
			v: EnumType::Str
		}
	}
}

impl TabMeta {
	//构建表元信息
	pub fn new(k: EnumType, v: EnumType) -> TabMeta{
		TabMeta{k, v}
	}
}

impl Decode for TabMeta{
	//解码已序列化的表元信息，并返回表元信息
	fn decode(bb: &mut ReadBuffer) -> Result<Self, ReadBonErr>{
		Ok(TabMeta{k: EnumType::decode(bb)?, v: EnumType::decode(bb)?})
	}
}

impl Encode for TabMeta{
	//编码表元信息，并序列化
	fn encode(&self, bb: &mut WriteBuffer){
		self.k.encode(bb);
		self.v.encode(bb);
	}
}

#[derive(Debug)]
pub struct Event {
	// 数据库同步序列号
	pub seq: u64,
	pub ware: Atom,
	pub tab: Atom,
	pub other: EventType
}

#[derive(Debug)]
pub enum EventType{
	Meta(Option<EnumType>),
	Tab{key: Bin, value: Option<Bin>},
}

/*
* 事务的状态
*/
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxState {
	Ok = 1,			//事务成功
	Doing,			//事务正在执行
	Err,			//事务错误
	Preparing,		//事务正在预提交
	PreparOk,		//事务预提交成功
	PreparFail,		//事务预提交失败
	Committing,		//事务正在提交
	Commited,		//事务已提交
	CommitFail,		//事务提交失败
	Rollbacking,	//事务正在回滚
	Rollbacked,		//事务已回滚
	RollbackFail,	//事务回滚失败
}

impl Default for TxState {
	fn default() -> Self {
		TxState::Ok
	}
}

impl ToString for TxState{
	fn to_string(&self) -> String{
		match self {
			TxState::Ok => String::from("TxState::Ok"),
			TxState::Doing => String::from("TxState::Doing"),
			TxState::Err => String::from("TxState::Err"),
			TxState::Preparing => String::from("TxState::Preparing"),
			TxState::PreparOk => String::from("TxState::PreparOk"),
			TxState::PreparFail => String::from("TxState::PreparFail"),
			TxState::Committing => String::from("TxState::Committing"),
			TxState::Commited => String::from("TxState::Commited"),
			TxState::CommitFail => String::from("TxState::CommitFail"),
			TxState::Rollbacking => String::from("TxState::Rollbacking"),
			TxState::Rollbacked => String::from("TxState::Rollbacked"),
			TxState::RollbackFail => String::from("TxState::RollbackFail"),
		}
	}
}

impl TxState {
	//将整数转换为事务状态
	pub fn to_state(n: usize) -> Self {
		match n {
            1 => TxState::Ok,
            2 => TxState::Doing,
            3 => TxState::Err,
            4 => TxState::Preparing,
            5 => TxState::PreparOk,
            6 => TxState::PreparFail,
            7 => TxState::Committing,
            8 => TxState::Commited,
            9 => TxState::CommitFail,
            10 => TxState::Rollbacking,
            11 => TxState::Rollbacked,
            _ => TxState::RollbackFail,
        }
	}
}

/**
* 表键值条目
*/
#[derive(Default, Clone, Debug)]
pub struct TabKV {
	pub ware: Atom,
	pub tab: Atom,
	pub key: Bin,
	pub index: usize,
	pub value: Option<Bin>,
}
impl TabKV {
	pub fn new(ware: Atom, tab: Atom, key: Bin) -> Self {
		TabKV{
			ware,
			tab,
			key,
			index: 0,
			value: None,
		}
	}
}

pub trait Iter {
	type Item;
	fn next(&mut self) -> Option<NextResult<Self::Item>>;
}

/*
* 预提交和提交时使用的操作日志
*/
#[derive(Clone, Debug)]
pub enum RwLog {
	Read,				//读操作
	Write(Option<Bin>),	//写操作，为None则表示删除，否则主键不存在则为插入，主键存在则为更新
	Meta(Option<Bin>),	//运行时创建、修改或删除表操作，为None表示删除，否则表名不存在则为创建，表名存在则为修改
}

//为了按照Bon协议比较字节数组， 定义了类型Bon
#[derive(Default, Clone, Hash)]
pub struct Bon(Arc<Vec<u8>>);

impl Bon{
	pub fn new(inner: Arc<Vec<u8>>) -> Bon{
		Bon(inner)
	}
}

impl Deref for Bon{
	type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialOrd for Bon {
	fn partial_cmp(&self, other: &Bon) -> Option<Ordering> {
		ReadBuffer::new(self.0.as_slice(), 0).partial_cmp(&ReadBuffer::new(other.0.as_slice(), 0))
	}
}

impl PartialEq for Bon{
	 fn eq(&self, other: &Bon) -> bool {
        match self.partial_cmp(other){
			Some(Ordering::Equal) => return true,
			_ => return false
		};
    }
}

impl Eq for Bon{}

impl Ord for Bon{
	fn cmp(&self, other: &Bon) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

//创建表事务的类型
pub enum BuildDbType {
	MemoryDB,	//内存表事务
	LogFileDB,	//日志文件表事务
}