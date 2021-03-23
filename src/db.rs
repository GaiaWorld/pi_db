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

pub type Bin = Arc<Vec<u8>>;
pub type SResult<T> = Result<T, String>;
pub type DBResult = SResult<()>;
pub type CommitResult = SResult<XHashMap<Bin, RwLog>>;
pub type IterResult = SResult<Box<dyn Iter<Item = (Bin, Bin)> + Send>>;
pub type KeyIterResult = SResult<Box<dyn Iter<Item = Bin>>>;
pub type NextResult<T> = SResult<Option<T>>;

pub type TxCallback = Arc<dyn Fn(DBResult)>;
pub type TxQueryCallback = Arc<dyn Fn(SResult<Vec<TabKV>>)>;

// 这个类型定义了，但从未使用，没实现Send， 不能在多线程运行时使用
// pub type Filter = Option<Arc<dyn Fn(Bin)-> Option<Bin>>>;
pub type Filter = Option<bool>;

/**
* 表的元信息
*/
#[derive(Debug, Clone)]
pub struct TabMeta {
	pub k: EnumType,
	pub v: EnumType
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
	fn default() -> Self {
		TabMeta {
			k: EnumType::Str,
			v: EnumType::Str
		}
	}
}

impl TabMeta {
	pub fn new(k: EnumType, v: EnumType) -> TabMeta{
		TabMeta{k, v}
	}
}

impl Decode for TabMeta{
	fn decode(bb: &mut ReadBuffer) -> Result<Self, ReadBonErr>{
		Ok(TabMeta{k: EnumType::decode(bb)?, v: EnumType::decode(bb)?})
	}
}

impl Encode for TabMeta{
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxState {
	Ok = 1,
	Doing,
	Err,
	Preparing,
	PreparOk,
	PreparFail,
	Committing,
	Commited,
	CommitFail,
	Rollbacking,
	Rollbacked,
	RollbackFail,
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

#[derive(Clone, Debug)]
pub enum RwLog {
	Read,
	Write(Option<Bin>),
	Meta(Option<Bin>),
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

pub enum BuildDbType {
	MemoryDB,
	LogFileDB,
}