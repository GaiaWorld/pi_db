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

    /// 克隆表的逻辑名称。
    ///
    /// 内置表返回 `Atom`，clone 不复制名称字符串。能够进入公开 DDL、根 WAL 或启动加载的
    /// 完整 UTF-8 名称必须位于 `1..=MAX_TABLE_NAME_BYTES`；低层直接构造表不会重新执行该
    /// 校验。操作应为 O(1)、只读、无锁且无 I/O。
    fn name(&self) -> <Self as KVTable>::Name;

    /// 借用表实现的数据位置；没有独立数据文件的表返回 `None`。
    ///
    /// 各实现的路径形态并不统一：日志表通常返回目录，Btree 返回 redb 文件，Memory 即使
    /// 参与根 WAL 也返回 `None`。返回值不承诺路径存在、可写或已经完成提交确认。
    fn path(&self) -> Option<&Path>;

    /// 返回该表动作是否可以要求写入根 WAL。
    ///
    /// 该标志不是“拥有独立数据文件”的同义词；`Memory+persistence=true` 是合法组合。
    /// 最终是否写 WAL 仍由根事务实际动作和事务树聚合决定。
    fn is_persistent(&self) -> bool;

    /// 返回表实现声明的有序属性。
    ///
    /// 当前五类表都报告有序，包括只写 LogWrite；因此 `true` 不能推导 query、delete、range
    /// stream 或旧值返回等具体能力，调用方仍须服从逐表契约。
    fn is_ordered(&self) -> bool;

    /// 返回表实现当前报告的记录数。
    ///
    /// 这是瞬时统计而非事务快照；不同实现可能统计 COW 根、日志根或 redb 与 overlay 的合并
    /// 近似值。错误通道和复杂度由具体表决定，不能用该值判断持久化或存储健康。
    fn len(&self) -> usize;

    /// 返回表实现当前报告的缓存 payload 字节数。
    ///
    /// 该值不等于进程 RSS、WAL/数据文件大小或 allocator 实际占用；Btree 只统计 overlay。
    /// 结果是瞬时观测，不能作为事务提交确认或资源已释放的证明。
    fn size(&self) -> u64;

    /// 创建与本表绑定的事务树节点。
    ///
    /// `source` 用于诊断；`is_writable` 和 `is_persistent` 是根事务向子节点下传的当前能力与
    /// WAL 需求；两个 timeout 当前只是协议字段，不构成强制截止。该低层入口不把节点注册到
    /// `Transaction2PcManager`，应用层必须经 `KVDBManager/KVDBTransaction` 装配事务树，并且
    /// 不得把 clone 当作可重复 prepare/commit 的新事务。
    fn transaction(&self,
                   source: Atom,
                   is_writable: bool,
                   is_persistent: bool,
                   prepare_timeout: u64,
                   commit_timeout: u64)
                   -> Self::Tr;

    /// 执行表实现的整理准备阶段。
    ///
    /// Memory/Btree 当前为 no-op，日志表可能轮换文件。成功不产生一次性 token，也不自动调用
    /// [`Self::collect`]；方法可能持锁、等待文件 I/O，错误和幂等边界由具体实现决定。
    fn ready_collect(&self) -> BoxFuture<Result<(), Self::Error>>;

    /// 执行表实现的整理或压缩阶段。
    ///
    /// 该操作不属于根事务 2PC，不追加根 WAL，也不提供跨表原子性。并发 DDL/事务、取消、重试
    /// 和文件副作用必须按具体表及 `KVDBManager::collect_table` 的约束处理。
    fn collect(&self) -> BoxFuture<Result<(), Self::Error>>;

    /// 向根 prepare buffer 追加一个表片段头。
    ///
    /// 当前稳定布局为 `table_name_len:u16-le + table_name:utf8 + action_count:u64-le`。本方法
    /// 只追加，不清空已有前缀，因此根事务可以在 16 字节 TID 后顺序拼接多个表片段。名称为空
    /// 或超过 [`MAX_TABLE_NAME_BYTES`] 时 panic；公开 DDL/启动入口必须更早返回结构化错误。
    ///
    /// 操作复杂度为 O(table name bytes)，可能扩容 `prepare_output`，无 I/O、await 或共享锁。
    /// 合法格式的直接证据见 `tests/table_wal_codec_contract.rs`。
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

    /// 向当前表片段追加一个 Key 动作。
    ///
    /// 布局为 `key_len:u16-le + key + value_len:u32-le + value`。`Some(value)` 表示 upsert，
    /// `None` 写入零长度并表示 delete tombstone；零长度绝不能解释为合法持久化空 Value。
    /// 合法调用域要求上层保证 Key 长度 `1..=u16::MAX`、非空 Value 且 Value 长度不超过
    /// `u32::MAX`，helper 自身只做窄化转换，协议外输入可能截断长度并生成不可恢复数据。当前
    /// 普通 upsert 尚未统一拒绝空 Value，属于 `FIND-DATA-001`，不能据此放宽本格式契约。
    ///
    /// 操作复杂度为 O(key bytes + value bytes)，只修改调用方 buffer，不访问表状态。
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

    /// 从给定偏移读取表片段头并返回 `(table, action_count, next_offset)`。
    ///
    /// `offset` 必须指向由 [`Self::init_table_prepare_output`] 生成的完整边界；返回偏移指向第一
    /// 个动作。当前 decoder 使用 `Buf` 固定读取、切片和 UTF-8 `unwrap`，对截断、越界或非法
    /// UTF-8 会 panic，因此只适用于经过根日志外层校验且由当前库生成的受信 WAL，不是不可信
    /// 网络或任意损坏文件的可恢复解析器。复杂度为 O(table name bytes) 并会构造 `Atom`。
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

    /// 从给定偏移读取指定数量的动作并返回 `(actions, next_offset)`。
    ///
    /// 每项按 [`Self::append_key_value_to_table_prepare_output`] 的布局解析；`value_len == 0` 唯一
    /// 表示 delete，非零才构造 upsert。返回的 [`TableKV`] 拥有新复制的 Key/Value payload，
    /// 总时间和空间均为 O(动作数 + payload 总字节数)。`kvs_len`、offset 和全部长度字段必须
    /// 来自受信完整 WAL；截断或伪造长度可能 panic/过度分配，损坏输入策略仍由
    /// `FIND-CODEC-001` 单独归档，不能用本 helper 处理外部字节。
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
/// `value` 必须结合消费 API 解释：版本 `write_set` 和 WAL/replay 动作中 `Some` 表示 upsert、
/// `None` 表示 delete；普通根 `upsert/dirty_upsert` 会跳过 `None`，`query/dirty_query` 与
/// `delete/dirty_delete` 则忽略输入 value。它不是带版本号的快照，也不验证表是否存在、Key
/// 编码是否合法或 Value 是否为空；这些边界由数据库入口和协议层负责。
/// 仅构造该值不会选择 Ordinary/Versioned 协议，也不会创建表事务、获取锁、修改版本缓存、
/// 写 WAL 或执行 I/O。
///
/// clone 会克隆 `Atom` 并增加 `Binary` 的共享引用计数，不复制二进制 payload；活跃 clone 会
/// 延长 Key/Value allocation 生命周期，但结构中没有反向引用或引用环。构造和
/// [`TableKV::exist_value`] 都是 O(1)，可跨线程移动/共享。公开协议和逐 API 的 `value` 语义见
/// [CORE-PUBLIC-TYPES-001](../../docs/CORE_PUBLIC_TYPES_CONTRACT.md#core-public-types-version-payload)。
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
    /// 返回值只反映 `value.is_some()`，不脱离调用 API 判定动作：版本 write-set/WAL 中 `false`
    /// 表示 delete tombstone，普通根 upsert 中会被跳过，query/delete 输入中则会被忽略。无论
    /// 哪种情况都不表示“写入空二进制”。O(1)、纯只读、无校验或数据库副作用。
    pub fn exist_value(&self) -> bool {
        self.value.is_some()
    }
}
