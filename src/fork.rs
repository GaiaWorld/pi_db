use std::{collections::HashMap, sync::Arc};

use atom::Atom;
use bon::{Encode, Decode, WriteBuffer, ReadBuffer, ReadBonErr};
use r#async::lock::mutex_lock::Mutex;

use crate::db::TabMeta;

lazy_static! {
	pub static ref ALL_TABLES: Arc<Mutex<HashMap<Atom, TableMetaInfo>>> = Arc::new(Mutex::new(HashMap::new()));
}

/// TODO: 被分叉表和分叉表之间的字段转换， 可以用一个转换函数来描述

/// 两种实现方法：
/// 1. fork 时及记录fork关系
/// 2. 系统初始化的时候重建fork关系
/// 两种方式在删除表时候的操作方便程度

/// 记录所有的元信息表，数据库打开时就应该先加载这部分的内容，取得各个表的元信息。
/// 需要用一个表专门存储这些信息
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TableMetaInfo {
	/// 表名
	pub tab_name: Atom,
	/// key, value 类型
	pub meta: TabMeta,
	/// 父表, 一个表最多只有一个父表， 父表上可以分叉产生多个子表
	pub parent: Option<Atom>,
	/// 该表是从父表的哪个log file id分叉而来
	pub parent_log_id: Option<usize>,
	/// 表的引用计数， 产生一个分叉则引用计数加1， 删除一个叶节点表，父表引用计数减1， 引用计数为0才可以安全删除这个表
	pub ref_count: usize,
}

impl TableMetaInfo {
	pub fn new(tab_name: Atom, meta: TabMeta) -> Self {
		Self {
			tab_name,
			meta,
			.. TableMetaInfo::default()
		}
	}


	/// 增加表的引用计数
	pub fn inc_refcount(&mut self) {
		self.ref_count += 1;
	}

	/// 减少表的引用计数
	pub fn dec_refcount(&mut self) {
		self.ref_count -= 1;

	}
}


/// 从根表到叶表的分叉链
/// 有表删除时需要维护变化
#[derive(Debug, Default)]
struct ForkChains {
	/// key 为根节点， value是从根节点到叶节点的链
	chains: HashMap<Atom, Vec<TableMetaInfo>>,
}


/// TableMetaInfo 的序列化方法
impl Encode for TableMetaInfo {
	fn encode(&self, bb: &mut WriteBuffer) {
		let mut b = WriteBuffer::new();
		self.tab_name.encode(&mut b);
		bb.write_bin(b.bytes.as_ref(), 0..b.bytes.len());
		let mut bin = WriteBuffer::new();
		self.meta.encode(&mut bin);
		bb.write_bin(bin.bytes.as_ref(), 0..bin.bytes.len());
		let mut bin2 = WriteBuffer::new();
		self.parent.encode(&mut bin2);
		bb.write_bin(bin2.bytes.as_ref(), 0..bin2.len());
		let mut bin4 = WriteBuffer::new();
		self.parent_log_id.encode(&mut bin4);
		bb.write_bin(bin4.bytes.as_ref(), 0..bin4.bytes.len());
		let mut bin5 = WriteBuffer::new();
		self.ref_count.encode(&mut bin5);
		bb.write_bin(bin5.bytes.as_ref(), 0..bin5.bytes.len());
	}
}

/// TableMetaInfo 的反序列化方法
impl Decode for TableMetaInfo {
	fn decode(bb: &mut ReadBuffer) -> Result<Self, ReadBonErr> {
		let b = bb.read_bin()?;
		let tab_name = Atom::decode(&mut ReadBuffer::new(&b, 0))?;
		let bin1 = bb.read_bin()?;
		let meta = TabMeta::decode(&mut ReadBuffer::new(&bin1, 0))?;
		let bin2 = bb.read_bin()?;
		let parent = Option::decode(&mut ReadBuffer::new(&bin2, 0))?;
		let bin4 = bb.read_bin()?;
		let parent_log_id = Option::decode(&mut ReadBuffer::new(&bin4, 0))?;
		let bin5 = bb.read_bin()?;
		let ref_count = usize::decode(&mut ReadBuffer::new(&bin5, 0))?;

		Ok(Self {
			tab_name,
			meta,
			parent,
			parent_log_id,
			ref_count
		})
	}
}

/// 从根表到目标表路径
pub async fn build_fork_chain(tab_name: Atom) -> Vec<TableMetaInfo> {
	let mut chains = vec![];
	let lock = ALL_TABLES.lock().await;
	if let Some(mut tab_info) = lock.get(&tab_name) {
		chains.push(tab_info.clone());
		while tab_info.parent.is_some() {
			tab_info = lock.get(&tab_info.parent.clone().unwrap()).unwrap();
			let t = tab_info.clone();
			chains.push(t);
		}
	}

	chains
}

/// 根据分叉路径，加载数据
pub fn load_data_from_fork_chain(chain: &[TableMetaInfo]) {
	unimplemented!()
}

mod tests {
	use sinfo::EnumType;
	use super::*;
	#[test]
	fn test_table_meta_info_codec() {
		let info = TableMetaInfo {
			tab_name: Atom::from("hello"),
			meta: TabMeta::new(EnumType::Str, EnumType::Str),
			parent: Some(Atom::from("world")),
			parent_log_id: Some(1),
			ref_count: 0,
		};

		let mut bin = WriteBuffer::new();
		info.encode(&mut bin);

		println!("encoded ==== {:?}", bin.bytes);

		let decoded = TableMetaInfo::decode(&mut ReadBuffer::new(bin.bytes.as_ref(), 0)).unwrap();
		println!("decoded === {:?}", decoded);

		assert_eq!(info, decoded);
	}

	// #[test]
	// fn test_fork_chain() {
	// 	let t1 = TableMetaInfo {
	// 		tab_name: Atom::from("A"),
	// 		meta: TabMeta::new(EnumType::Str, EnumType::Str),
	// 		parent: None,
	// 		parent_log_id: None,
	// 		ref_count: 3,
	// 	};
	// 	let t2 = TableMetaInfo {
	// 		tab_name: Atom::from("B"),
	// 		meta: TabMeta::new(EnumType::Str, EnumType::Str),
	// 		parent: Some(Atom::from("A")),
	// 		parent_log_id: Some(2),
	// 		ref_count: 1
	// 	};
	// 	let t3 = TableMetaInfo {
	// 		tab_name: Atom::from("C"),
	// 		meta: TabMeta::new(EnumType::Str, EnumType::Str),
	// 		parent: Some(Atom::from("A")),
	// 		parent_log_id: Some(4),
	// 		ref_count: 0
	// 	};
	// 	let t4 = TableMetaInfo {
	// 		tab_name: Atom::from("D"),
	// 		meta: TabMeta::new(EnumType::Str, EnumType::Str),
	// 		parent: Some(Atom::from("B")),
	// 		parent_log_id: Some(3),
	// 		ref_count: 1
	// 	};
	// 	let t5 = TableMetaInfo {
	// 		tab_name: Atom::from("E"),
	// 		meta: TabMeta::new(EnumType::Str, EnumType::Str),
	// 		parent: Some(Atom::from("A")),
	// 		parent_log_id: Some(7),
	// 		ref_count: 0
	// 	};
	// 	let t6 = TableMetaInfo {
	// 		tab_name: Atom::from("F"),
	// 		meta: TabMeta::new(EnumType::Str, EnumType::Str),
	// 		parent: Some(Atom::from("D")),
	// 		parent_log_id: Some(2),
	// 		ref_count: 0
	// 	};

	// 	ALL_TABLES.lock().unwrap().insert(t1.tab_name.clone(), t1);
	// 	ALL_TABLES.lock().unwrap().insert(t2.tab_name.clone(), t2);
	// 	ALL_TABLES.lock().unwrap().insert(t3.tab_name.clone(), t3);
	// 	ALL_TABLES.lock().unwrap().insert(t4.tab_name.clone(), t4);
	// 	ALL_TABLES.lock().unwrap().insert(t5.tab_name.clone(), t5);
	// 	ALL_TABLES.lock().unwrap().insert(t6.tab_name.clone(), t6);

	// 	let chains = build_fork_chain(Atom::from("F"));
	// 	let mut load_seq = vec![];
	// 	for ch in &chains {
	// 		load_seq.push(ch.tab_name.as_ref());
	// 	}
	// 	assert_eq!(vec!["F", "D", "B", "A"], load_seq); // "F" 表的加载顺序是 F -> D -> B -> A
	// 	println!("chains ==== {:?}", chains);
	// }

}