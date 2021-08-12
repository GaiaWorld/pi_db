use std::ops::Deref;
use std::fmt::Debug;
use std::hash::Hash;
use std::path::{Path, PathBuf};

use futures::{future::{FutureExt, BoxFuture},
              stream::BoxStream};
use bytes::BufMut;

use atom::Atom;
use async_transaction::{AsyncTransaction,
                        Transaction2Pc,
                        UnitTransaction,
                        SequenceTransaction,
                        TransactionTree};

use crate::{Binary, KVAction};

pub mod mem_ord_table;

///
/// 默认的数据库表名最大长度，64KB
///
const DEFAULT_DB_TABLE_NAME_MAX_LEN: usize = 0xffff;

///
/// 抽象的键值对表
///
pub trait KVTable: Send + Sync + 'static {
    type Name: AsRef<str> + Debug + Clone + Send + 'static;
    type Tr: KVAction + TransactionTree + SequenceTransaction + UnitTransaction + Transaction2Pc + AsyncTransaction;
    type Error: Debug + Send + 'static;

    /// 获取表名，表名最长为64KB
    fn name(&self) -> <Self as KVTable>::Name;

    /// 获取表所在目录的路径
    fn path(&self) -> Option<&Path>;

    /// 是否可持久化的表
    fn is_persistent(&self) -> bool;

    /// 是否是有序表
    fn is_ordered(&self) -> bool;

    /// 获取表的记录数
    fn len(&self) -> usize;

    /// 获取表事务
    fn transaction(&self,
                   source: Atom,
                   is_writable: bool,
                   prepare_timeout: u64,
                   commit_timeout: u64)
                   -> Self::Tr;

    /// 初始化指定的预提交输出缓冲区，并将本次表事务的预提交操作的键值对数量写入预提交输出缓冲区中
    fn init_table_prepare_output(&self,
                                 prepare_output: &mut <<Self as KVTable>::Tr as Transaction2Pc>::PrepareOutput,
                                 writed_len: u64) {
        let table_name = self.name().as_ref().to_string();
        let bytes = table_name.as_bytes();
        let bytes_len = bytes.len();
        if bytes_len == 0 || bytes_len > DEFAULT_DB_TABLE_NAME_MAX_LEN {
            //无效的表名长度，则立即抛出异常
            panic!("Init table prepare output failed, table_name: {:?}, reason: invalid table name length", table_name);
        }

        prepare_output.put_u16_le(bytes_len as u16); //写入表名长度
        prepare_output.put_slice(bytes); //写入表名
        prepare_output.put_u64_le(writed_len); //写入本次事务的预提交操作的键值对数量，这描述了后续会追加到预提交输出缓冲区中的键值对数量
    }

    /// 追加预提交成功的键值对，到指定的预提交输出缓冲区中
    fn append_key_value_to_table_prepare_output(&self,
                                                prepare_output: &mut <<Self as KVTable>::Tr as Transaction2Pc>::PrepareOutput,
                                                key: &<<Self as KVTable>::Tr as KVAction>::Key,
                                                value: Option<&<<Self as KVTable>::Tr as KVAction>::Value>) {
        let bytes: &[u8] = key.as_ref();
        prepare_output.put_u16_le(bytes.len() as u16); //写入关键字长度
        prepare_output.put_slice(bytes); //写入关键字

        if let Some(value) = value {
            //有值
            let bytes: &[u8] = value.as_ref();
            prepare_output.put_u32_le(bytes.len() as u32); //写入值长度
            prepare_output.put_slice(bytes); //写入值
        } else {
            //无值
            prepare_output.put_u32_le(0);
        }
    }
}

///
/// 表键值
///
#[derive(Debug, Clone)]
pub struct TableKV {
    pub table:  Atom,           //表名
    pub key:    Binary,         //关键字
    pub value:  Option<Binary>, //值
}

unsafe impl Send for TableKV {}
unsafe impl Sync for TableKV {}

impl TableKV {
    pub fn new(table: Atom,
               key: Binary,
               value: Option<Binary>) -> Self {
        TableKV {
            table,
            key,
            value,
        }
    }
}