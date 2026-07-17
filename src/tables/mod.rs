use std::fmt::Debug;
use std::path::Path;

use futures::future::BoxFuture;
use bytes::{Buf, BufMut};

use pi_atom::Atom;
use pi_async_transaction::{AsyncTransaction,
                           Transaction2Pc,
                           UnitTransaction,
                           SequenceTransaction,
                           TransactionTree};

use crate::{Binary,
            KVAction,
            MAX_TABLE_NAME_BYTES};

mod ordmap_snapshot;

/// 数据库表注册信息及持久化元数据表。
pub mod meta_table;
/// 基于 COW 有序 Map 的内存表。
pub mod mem_ord_table;
/// 以日志文件保存数据的可查询有序表。
pub mod log_ord_table;
/// 只写日志表；当前不属于允许外部使用的表类型。
pub mod log_write_table;
// pub mod b_tree_ord_table_old;
/// 以事务内 COW 缓存和 redb 数据文件组成的 Btree 表。
pub mod b_tree_ord_table;

/// 五类键值表共同遵守的表级契约。
///
/// trait 同时定义表属性、子事务构造、整理入口以及根 WAL 中表片段的编码/解码格式。事务
/// manager 通过关联的 `Tr` 把表事务挂入根事务树；应用层应通过 `KVDBManager`/
/// `KVDBTransaction` 使用这些能力，而不是直接构造表或表事务。默认 WAL helper 信任输入是
/// 当前库生成的完整缓冲区，畸形或截断数据可能 panic，不能作为不可信网络解码器使用。
pub trait KVTable: Send + Sync + 'static {
    /// 表名句柄；当前内置实现使用 `Atom`，名称须满足全库表名边界。
    type Name: AsRef<str> + Debug + Clone + Send + 'static;
    /// 与该表绑定的事务树节点，必须同时实现动作、生命周期、顺序和 2PC 契约。
    type Tr: KVAction + TransactionTree + SequenceTransaction + UnitTransaction + Transaction2Pc + AsyncTransaction;
    /// 表构造、整理或持久化操作返回的错误类型。
    type Error: Debug + Send + 'static;

    /// 获取表名；UTF-8 编码长度必须位于 `1..=MAX_TABLE_NAME_BYTES`
    fn name(&self) -> <Self as KVTable>::Name;

    /// 获取表所在目录的路径
    fn path(&self) -> Option<&Path>;

    /// 是否可持久化的表
    fn is_persistent(&self) -> bool;

    /// 是否是有序表
    fn is_ordered(&self) -> bool;

    /// 获取表的记录数
    fn len(&self) -> usize;

    /// 获取表的字节大小
    fn size(&self) -> u64;

    /// 获取表事务
    fn transaction(&self,
                   source: Atom,
                   is_writable: bool,
                   is_persistent: bool,
                   prepare_timeout: u64,
                   commit_timeout: u64)
                   -> Self::Tr;

    /// 准备表整理，返回成功则可以开始表整理
    fn ready_collect(&self) -> BoxFuture<Result<(), Self::Error>>;

    /// 表整理
    fn collect(&self) -> BoxFuture<Result<(), Self::Error>>;

    /// 初始化指定的预提交输出缓冲区，并将本次表事务的预提交操作的键值对数量写入预提交输出缓冲区中
    fn init_table_prepare_output(&self,
                                 prepare_output: &mut <<Self as KVTable>::Tr as Transaction2Pc>::PrepareOutput,
                                 writed_len: u64) {
        let table_name = self.name().as_ref().to_string();
        let bytes = table_name.as_bytes();
        let bytes_len = bytes.len();
        if bytes_len == 0 || bytes_len > MAX_TABLE_NAME_BYTES {
            // DDL/启动入口必须先拒绝非法名称；这里是防止内部构造绕过公开边界后截断 u16
            // 长度并生成不可恢复 WAL 的最后一道不变量检查。
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
        for _index in 0..kvs_len {
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

/// 版本批量事务与根事务分发表动作时使用的表/Key/Value 三元组。
///
/// `value = Some` 表示 upsert，`value = None` 表示 delete。它不是带版本号的快照，也不验证
/// 表是否存在、Key 编码是否合法或 Value 是否为空；这些边界由数据库入口和协议层负责。
/// clone 会克隆 `Atom` 并增加 `Binary` 的共享引用计数，不复制二进制 payload。
#[derive(Debug, Clone)]
pub struct TableKV {
    /// 目标表名。
    pub table:  Atom,
    /// 目标 Key 的表类型编码。
    pub key:    Binary,
    /// `Some` 为写入值，`None` 为逻辑删除。
    pub value:  Option<Binary>,
}

// SAFETY: `Atom` 和 `Binary` 都是线程安全共享 owner，`Option<Binary>` 不增加额外内部可变性；
// 移动三元组不会创建可变别名。合法表名/编码属于协议正确性，不属于内存安全前提。
unsafe impl Send for TableKV {}
// SAFETY: 所有字段仅通过共享不可变引用公开，底层引用计数对象可安全跨线程共享。
unsafe impl Sync for TableKV {}

impl TableKV {
    /// 构造一个尚未校验的表动作。
    ///
    /// O(1)，不执行表查找、锁、I/O 或版本登记；调用方仍须通过合法事务入口提交。
    pub fn new(table: Atom,
               key: Binary,
               value: Option<Binary>) -> Self {
        TableKV {
            table,
            key,
            value,
        }
    }

    /// 判断该动作是否携带 upsert 值。
    ///
    /// 返回 `false` 表示 delete tombstone，而不是“写入空二进制”。O(1)、纯只读。
    pub fn exist_value(&self) -> bool {
        self.value.is_some()
    }
}
