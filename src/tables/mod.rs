use std::fmt::Debug;
use std::path::Path;

use bytes::{Buf, BufMut};

use atom::Atom;
use async_transaction::{AsyncTransaction,
                        Transaction2Pc,
                        UnitTransaction,
                        SequenceTransaction,
                        TransactionTree};

use crate::{Binary,
            KVAction};

pub mod meta_table;
pub mod mem_ord_table;
pub mod log_ord_table;
pub mod log_write_table;

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
                   is_persistent: bool,
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
            panic!("Init table prepare output failed, table_name: {:?}, reason: invalid table name length", table_name.as_str());
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
            prepare_output.put_u32_le(0); //写入值长度
        }
    }

    /// 获取预提交输出缓冲区的表初始化内容，包括表名和预提交操作的键值对数量，并返回读取后的偏移
    fn get_init_table_prepare_output(prepare_output: &<<Self as KVTable>::Tr as Transaction2Pc>::PrepareOutput,
                                     mut offset: usize)
        -> (Atom, u64, usize) {
        //获取需要读取的缓冲区
        let mut bytes: &[u8] = prepare_output.as_ref();
        bytes.advance(offset); //移动缓冲区指针

        //读取缓冲区数据
        let table_name_len = bytes.get_u16_le() as usize; //获取表名长度
        offset += 2;
        let table_name_string = String::from_utf8(bytes[0..table_name_len].to_vec()).unwrap();
        let table_name = Atom::from(table_name_string); //获取表名
        bytes.advance(table_name_len); //移动缓冲区指针
        offset += table_name_len;
        let kvs_len = bytes.get_u64_le(); //获取本次事务的预提交操作的键值对数量
        offset += 8;

        (table_name, kvs_len, offset)
    }

    /// 获取预提交输出缓冲区中指定数量的表键值列表，并返回读取后的偏移
    fn get_all_key_value_from_table_prepare_output(prepare_output: &<<Self as KVTable>::Tr as Transaction2Pc>::PrepareOutput,
                                                   table: &Atom,
                                                   kvs_len: u64,
                                                   mut offset: usize)
        -> (Vec<TableKV>, usize) {
        //获取需要读取的缓冲区
        let mut bytes: &[u8] = prepare_output.as_ref();
        bytes.advance(offset); //移动缓冲区指针

        //读取缓冲区数据
        let mut tkvs = Vec::with_capacity(kvs_len as usize);
        for index in 0..kvs_len {
            let key_len = bytes.get_u16_le() as usize; //获取关键字长度
            offset += 2;
            let key = Binary::from_slice(&bytes[0..key_len]); //获取关键字
            bytes.advance(key_len); //移动缓冲区指针
            offset += key_len;

            let value_len = bytes.get_u32_le() as usize; //获取值长度
            offset += 4;
            if value_len > 0 {
                //有值
                let value = Binary::from_slice(&bytes[0..value_len]); //获取值
                bytes.advance(value_len); //移动缓冲区指针
                offset += value_len;

                tkvs.push(TableKV::new(table.clone(), key, Some(value)));
            } else {
                //无值
                tkvs.push(TableKV::new(table.clone(), key, None));
            }
        }

        (tkvs, offset)
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
    /// 构建一个表键值
    pub fn new(table: Atom,
               key: Binary,
               value: Option<Binary>) -> Self {
        TableKV {
            table,
            key,
            value,
        }
    }

    /// 判断是否有值
    pub fn exist_value(&self) -> bool {
        self.value.is_some()
    }
}

