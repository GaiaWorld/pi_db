#![feature(fn_traits)]
#![feature(once_cell)]
#![feature(const_trait_impl)]
#![feature(unboxed_closures)]
#![feature(min_specialization)]

//! `pi_db` 是建立在 `pi_async_transaction` 与 `pi_store` 之上的异步事务型键值数据库。
//!
//! 数据库管理器、根事务及事务树编排位于 [`db`]；Meta、Memory、LogOrdered、LogWrite 和
//! Btree 五类表及其子事务位于 [`tables`]；[`inspector`] 提供 WAL/日志表诊断读取；[`utils`]
//! 定义建表选项与数据库事件。正常写事务依次经历子表预提交、根 WAL 成功落地、内存/表状态
//! 提交、异步数据持久化和提交确认；“根 WAL 已提交”与“全部数据文件已确认”是两个有序但
//! 不等价的阶段。
//!
//! [`db::KVDBManager::query_with_version`]、[`db::KVDBTransaction::prepare_with_version`] 和
//! [`db::KVDBTransaction::commit_with_version`] 组成供外部缓存使用的独立版本事务协议。该协议
//! 不得与同一事务上的普通 `query/upsert/delete/prepare/commit` 混用。详细架构、语义边界和
//! 验收入口分别见仓库本地 `docs/PI_DB_ARCHITECTURE.md`、`docs/SEMANTIC_CONTRACTS.md` 与
//! `docs/TEST_AND_BENCHMARK_STRATEGY.md`。
//!
//! 当前事务安全结论不覆盖根 WAL 自身因磁盘空间、配额、只读文件系统、设备 I/O、runtime
//! 拒绝异步任务或文件大小限制而失败的环境域；这类失败的返回值不能证明 WAL 实际写入了
//! 0、部分还是全部字节。该限制不改变根 WAL 成功后由未确认日志驱动的数据修复契约。

use std::ops::Deref;
use std::fmt::Debug;
use std::hash::Hash;
use std::cmp::Ordering as CmpOrdering;
use std::sync::{Arc,
                atomic::{AtomicUsize, Ordering}};

use futures::{future::BoxFuture,
              stream::BoxStream};
use bytes::{Buf, BufMut};
use log::warn;

use pi_bon::{WriteBuffer, ReadBuffer, Encode, Decode, ReadBonErr};
use pi_sinfo::EnumType;
use pi_async_rt::rt::{AsyncRuntime,
                      multi_thread::MultiTaskRuntime};
use pi_async_transaction::{AsyncCommitLog, TransactionConflictError, TransactionError, ErrorLevel};
use pi_guid::Guid;
use pi_ordmap::asbtree::TreeByteSize;

/// 数据库管理器、根/子事务树、DDL、版本事务协议以及启动修复流程。
pub mod db;
/// 五类表、表事务和通用表/WAL 编解码契约。
pub mod tables;
/// 根提交日志与日志表的只读诊断工具。
pub mod inspector;
/// 建表选项、提交事件及内部调度辅助类型。
pub mod utils;
mod key_version;

pub use key_version::{TableKey, TableKeyVersion, Version};

/// 表名 UTF-8 编码的最大字节数。
///
/// 该上限应用于完整表名字符串；使用路径分隔符表达嵌套表路径时，也按完整相对路径的
/// UTF-8 字节数计算。长度 `1..=4096` 属于名称长度的合法域，空字符串或超过 4096 字节的
/// 名称必须由能够返回错误的 DDL 入口以 `io::ErrorKind::InvalidInput` 拒绝，且拒绝发生在
/// 注册表、Meta、WAL 和文件系统副作用之前。启动时从持久化 Meta 读到超限名称属于
/// `InvalidData`。
///
/// 这是数据库格式和公开 API 边界，不是文件系统路径能力承诺：持久化表的单个路径组件仍
/// 可能受操作系统更小的限制。当前相对路径及 `..` 校验是独立的 `FIND-PATH-001` 事项。
/// 常量读取为 O(1)、纯函数语义，无分配、锁、I/O 或线程安全副作用。
pub const MAX_TABLE_NAME_BYTES: usize = 4 * 1024;

/// 由 `Arc<Vec<u8>>` 持有的共享不可变二进制数据。
///
/// `Binary` 同时用于数据库 Key、Value、元数据和事务日志字段。克隆只增加强引用，不复制
/// payload；[`Binary::from_slice`] 才会复制输入。类型不在构造时验证空值、Key 编码或
/// `KVTableMeta` 声明的值类型：
///
/// - 作为有序表 Key 时，调用方必须提供可由 `pi_bon::ReadBuffer` 按表 `key_type` 正确比较的
///   合法编码。当前排序不是任意原始 bytes 的字典序，空或畸形 Key 可能 panic；这是
///   `Q-KEY-001` / `FIND-DATA-002` 记录的当前前置条件，不是最终输入校验设计。
/// - 构造空 `Binary` 本身合法，但数据库严格禁止把长度为 0 的 Value 持久化。当前 upsert
///   入口尚未统一拒绝，见 `CONTRACT-DATA-002` / `FIND-DATA-001`；调用方不得把“能构造”
///   解释为“能持久化”。
///
/// 内容在安全 API 下只读，`Clone`、`to_shared` 和 `from_shared` 均为 O(1) 时间/空间并执行
/// 原子引用计数操作；从 slice 构造为 O(n) 时间和 O(n) 新空间。实例可在线程间移动和共享，
/// 不持锁、不执行 I/O、不回调用户代码。活跃 clone 会延长 payload 生命周期，但内部没有
/// 反向引用，不形成引用环。
///
/// # Example
///
/// ```
/// use pi_db::Binary;
///
/// let value = Binary::new(vec![1, 2, 3]);
/// let shared = value.clone();
/// assert_eq!(shared.as_ref(), &[1, 2, 3]);
/// assert!(Binary::binary_equal(&value, &shared));
/// ```
#[derive(Debug, Hash)]
pub struct Binary(Arc<Vec<u8>>);

// SAFETY: `Binary` 只包含 `Arc<Vec<u8>>`。公开 API 只暴露共享不可变字节或另一个 `Arc`，
// 不会为底层 Vec 创建可变别名。`Vec<u8>` 实现了 Send + Sync，因此在线程间移动该 owner
// 仍然遵守标准 `Arc` 的安全契约。保留这个显式实现是为了兼容既有类型定义；仅按字段组成，
// 编译器本来也可以自动推导 Send。
unsafe impl Send for Binary {}

impl Clone for Binary {
    /// 克隆共享 owner，不复制 payload。
    ///
    /// 时间和新增空间均为 O(1)，副作用仅为原子强引用计数增加。返回值与原对象可在线程间
    /// 独立移动；任一 owner 存活都会保持底层字节有效。
    fn clone(&self) -> Self {
        Binary(self.0.clone())
    }
}

impl AsRef<[u8]> for Binary {
    /// 借用完整 payload，不解码也不复制。
    ///
    /// 返回 slice 只在 `self` 借用期间有效。操作为 O(1)、只读、无分配且不阻塞。
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl Deref for Binary {
    type Target = [u8];

    /// 将 `Binary` 只读解引用为完整字节分片。
    ///
    /// 语义和复杂度与 [`AsRef::as_ref`] 相同；这不会验证数据库 Key/Value 合法域。
    fn deref(&self) -> &Self::Target {
        self.0.as_slice()
    }
}

impl From<KVTableMeta> for Binary {
    /// 按 Meta 表的当前持久化格式序列化表元信息。
    ///
    /// 格式依次为：1 字节表类型、1 字节 persistence 标记、2 字节小端 Key 类型长度、
    /// Key 的 `pi_bon` 编码、2 字节小端 Value 类型长度、Value 的 `pi_bon` 编码。编码器
    /// 只产生 `0/1` persistence 标记。
    ///
    /// `src` 被消费，返回值独占新分配的编码 buffer。时间和空间复杂度均为 O(k + v)，
    /// 其中 k、v 是两段类型描述的编码长度；函数不执行 I/O、不持锁且可跨线程调用。
    /// 当前长度会直接转换为 `u16` 而不检查溢出，构造函数也不验证类型与实际数据一致，
    /// 因此调用方只能传入受支持且编码长度不超过 `u16::MAX` 的元信息。对应信任边界见
    /// `CONTRACT-DATA-003` 与 `FIND-CODEC-001`。
    fn from(src: KVTableMeta) -> Self {
        let mut buf = Vec::new();

        //写入键值对表的类型
        buf.put_u8(src.table_type as u8);

        if src.persistence {
            //写入键值对表需要持久化的标记
            buf.put_u8(1);
        } else {
            //写入键值对表不需要持久化的标记
            buf.put_u8(0);
        }

        //写入键值对表的关键字类型
        let mut write_buffer = WriteBuffer::new();
        src.key.encode(&mut write_buffer);
        buf.put_u16_le(write_buffer.len() as u16); //写入关键字类型的长度
        buf.put_slice(write_buffer.get_byte().as_slice());

        //写入键值对表的值类型
        let mut write_buffer = WriteBuffer::new();
        src.value.encode(&mut write_buffer);
        buf.put_u16_le(write_buffer.len() as u16); //写入值类型的长度
        buf.put_slice(write_buffer.get_byte().as_slice());

        Binary::new(buf)
    }
}

impl Ord for Binary {
    /// 按 `pi_bon::ReadBuffer` 解码后的值序列执行全序比较。
    ///
    /// 合法、规范的 `pi_bon` Key 上复杂度为 O(n)，n 为首次差异前读取的字节数。当前实现
    /// 会为 `expect` 提前格式化完整 panic 消息，因此成功比较也可能产生 O(n) 临时分配；
    /// 这是 `FIND-PERF-BINARY-001` 已归档的性能问题，不是调用方可依赖的语义。空或畸形
    /// 输入可能 panic。
    fn cmp(&self, other: &Binary) -> CmpOrdering {
        self.partial_cmp(other).expect(&format!("Can't compare two binaries, {:?}, {:?}", self, other))
    }
}

impl PartialOrd for Binary {
    /// 尝试按 `pi_bon` 值语义比较两段编码。
    ///
    /// 成功时返回解码后值序列的顺序，而不是原始字节字典序；无法比较时可能返回 `None`，
    /// 但底层类型块读取对部分畸形输入会先 panic。仅合法 Key 编码属于当前调用域。
    fn partial_cmp(&self, other: &Binary) -> Option<CmpOrdering> {
        ReadBuffer::new(self.0.as_slice(), 0)
            .partial_cmp(&ReadBuffer::new(other.0.as_slice(), 0))
    }
}

impl Eq for Binary {}

impl PartialEq for Binary {
    /// 按与排序相同的 `pi_bon` 值语义判断相等。
    ///
    /// 这不同于 [`Binary::binary_equal`] 的 allocation 身份比较，也不保证等同于原始字节
    /// 相等。调用方必须使用规范编码维持 `Eq`、`Ord` 与派生 `Hash` 的一致性；畸形输入边界
    /// 见 `Q-KEY-001` / `FIND-DATA-002`。
    fn eq(&self, other: &Binary) -> bool {
        match self.partial_cmp(other){
            Some(CmpOrdering::Equal) => true,
            _ => false
        }
    }
}

impl Default for Binary {
    /// 构造长度为 0 的共享字节对象。
    ///
    /// 该对象可作为普通 Rust 值或内部占位，但不是合法有序 Key，也不得作为数据库 Value
    /// 持久化。构造为 O(1)，会分配空 `Vec` 的 `Arc` control block。
    fn default() -> Self {
        Binary(Arc::new(Vec::default()))
    }
}

impl TreeByteSize for Binary {
    /// 返回 payload 长度，供 `pi_ordmap` 估算树中数据字节数。
    ///
    /// 结果不包含 `Binary`、`Arc` control block、`Vec` capacity 或树节点开销。操作为 O(1)、
    /// 只读、无分配且不会阻塞。
    fn tree_bytes_size(&self) -> u64 {
        self.0.len() as u64
    }
}

impl Binary {
    /// 获取 `bin` 的所有权并构造二进制对象。
    ///
    /// 本函数不复制 payload，时间和新增辅助空间为 O(1)，但会为 `Arc` control block
    /// 分配内存。它不验证空值或 `pi_bon` Key 编码；空输入的数据库边界见类型级说明。
    /// 除分配外无副作用，不阻塞、不持锁且不会 panic（内存分配失败导致的进程级行为除外）。
    pub fn new(bin: Vec<u8>) -> Self {
        Binary(Arc::new(bin))
    }

    /// 判断两个实例是否共享完全相同的 `Arc<Vec<u8>>` allocation。
    ///
    /// 这是 O(1) 的所有者身份比较，不比较内容：两个独立 allocation 即使字节完全相等也会
    /// 返回 `false`。函数纯只读、幂等、线程安全，不分配、不阻塞且不会 panic。内容相等应
    /// 使用 `==`；有序 Key 的 `==` 服从 `pi_bon` 语义，边界见类型级说明。
    pub fn binary_equal(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.0, &other.0)
    }

    /// 从已有共享 allocation 构造 `Binary`。
    ///
    /// `shared` 的所有权移入本对象，不复制 payload；时间和空间为 O(1)。之后通过
    /// [`Binary::to_shared`] 或 clone 获得的 owner 指向同一 allocation。函数不验证数据域，
    /// 不执行 I/O、不阻塞且除参数所有权转移外无副作用。
    pub fn from_shared(shared: Arc<Vec<u8>>) -> Self {
        Binary(shared)
    }

    /// 复制 `slice` 的全部字节并构造独立 `Binary`。
    ///
    /// 输入只在调用期间借用，不被保存。时间和新增空间均为 O(n)，其中 n 为分片长度；
    /// 即使源分片来自另一个 `Binary`，新对象也不共享其 allocation。函数不验证数据域，
    /// 不执行 I/O、不持锁且仅有内存分配副作用。
    pub fn from_slice<B: AsRef<[u8]>>(slice: B) -> Self {
        Binary(Arc::new(Vec::from(slice.as_ref())))
    }

    /// 返回 payload 的字节长度。
    ///
    /// O(1) 纯读取；不分配、不阻塞、无副作用且线程安全。返回 0 只描述对象内容，不表示
    /// 该对象可作为数据库持久化 Value。
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// 克隆并返回底层 `Arc<Vec<u8>>` owner。
    ///
    /// 返回值与 `self` 共享同一 allocation，调用方负责释放新增强引用。时间和空间为 O(1)，
    /// 不复制 payload、不执行 I/O、不阻塞；副作用仅为原子强引用计数增加。
    pub fn to_shared(&self) -> Arc<Vec<u8>> {
        self.0.clone()
    }
}

/// 子表事务提供的低层键值操作边界。
///
/// `KVAction` 由 Meta、Memory、LogOrdered、LogWrite 和 Btree 五类子表事务实现；数据库
/// 对外的批量入口由 `KVDBTransaction::RootTr` 按表名路由到这些方法。trait 本身不负责
/// 创建/提交/回滚事务，也不验证事务是否可写、是否已经 prepare/commit、Key 编码是否符合
/// 表元数据或 Value 是否为空。调用方必须先满足事务状态机和 [`Binary`] 的合法域。
///
/// # 当前一致性边界
///
/// - Meta、Memory、LogOrdered 在子事务首次创建时克隆表的 COW 根，点读观察该根加本事务
///   后续修改；子事务由根事务首次访问某张表时惰性创建，不一定与根事务创建同一时刻。
/// - Btree 在子事务创建时固定内存 overlay，但 overlay 缺席的点读会在每次调用时同步读取
///   redb；因此它不是统一的“事务创建时磁盘快照”。普通/dirty 点读都会记录普通 `Read`。
/// - LogWrite 只接受 upsert 动作；query/delete 不观察日志内容，迭代器还有已归档的空哨兵
///   行为。不能仅凭本 trait 的方法集合推断每种表都支持完整 CRUD。
/// - dirty 不是统一隔离级别：前三类 COW 表用 `DirtyWrite` 或不记读，Btree 的 dirty 方法
///   当前直接复用普通方法，LogWrite 只区分写动作。非持久 COW prepare 仍检查根冲突。
///   这些是 `FIND-DIRTY-001` 的当前实现结论，不是最终或最佳事务语义。
/// - 外部协议要求同一根事务及其子事务只能选择一个点操作族：要么只使用 `dirty_*`，要么
///   只使用普通 `query/upsert/delete`。禁止在同一事务混用两族；违规调用当前不会被 guard
///   拒绝，但不保证任何事务安全性。Btree 的 dirty 方法即使内部委托普通方法，在外部协议中
///   仍属于 `dirty_*` 调用族。
///
/// # 并发、性能与副作用
///
/// trait 要求事务句柄 `Send + Sync`，返回的 boxed future/stream 可在线程间移动；同一事务的
/// 并发调用由各实现的同步锁保护，但不额外承诺调用间线性化、可串行化或跨表原子性。点操作
/// 通常会克隆一个 `Arc` 事务句柄，并在首次 poll 时持有短期 `SpinLock`/`parking_lot` guard；
/// 当前实现不跨 yield 保持这些 guard。Btree 的 redb fallback 是同步文件/映射访问，可能
/// 阻塞异步 worker。写操作只修改事务私有根/overlay 和动作表，不在调用时写根 WAL 或数据
/// 文件；真正的持久化发生在 2PC commit 及后续 collector。
///
/// 当前合法点操作由 `tests/kv_action_contract.rs` 验证；流的生命周期和快照矩阵见
/// `tests/iterator_snapshot_safety.rs`，Btree 删除旧值见
/// `tests/btree_delete_old_value.rs`。完整契约入口为 `CONTRACT-ACTION-001`。
pub trait KVAction: Send + Sync + 'static {
    /// Key 的 owned 类型。
    ///
    /// 实现要求它可哈希、全序、克隆和跨线程移动。对 `pi_db` 内置实现该类型均为
    /// [`Binary`]；排序能力不表示任意 bytes 都是合法 Key，编码前置条件见 [`Binary`]。
    type Key: AsRef<[u8]> + Deref<Target = [u8]> + Hash + PartialEq + Eq + PartialOrd + Ord + Clone + Send + 'static;
    /// Value 的 owned 类型。
    ///
    /// 内置实现均为 [`Binary`]。`Default` 只是 trait 形状要求，不代表默认空值可以持久化；
    /// 长度为 0 的持久化 Value 被数据库契约禁止。
    type Value: AsRef<[u8]> + Deref<Target = [u8]> + Default + Clone + Send + 'static;
    /// 写、删除和锁方法的错误类型。
    ///
    /// query/stream 的当前签名没有错误通道；内置实现使用 [`KVTableTrError`]。调用方不能把
    /// `None` 或流提前结束一律解释为“无错误且 Key 不存在”。
    type Error: Debug + 'static;

    /// 查询当前子事务视图，但采用该表实现的 dirty 读记录策略。
    ///
    /// `key` 的所有权移入 future；返回的 `Some` 是 owned Value，`None` 表示该实现本次未
    /// 取得值。Meta/Memory/LogOrdered 不写 `Read` 动作，读取事务私有 COW 根；Btree 直接
    /// 调用普通 [`KVAction::query`]，仍记录 `Read`，overlay 缺席时同步回读 redb；LogWrite
    /// 无条件返回 `None`。因此“可能读旧值”不是跨表可比较的一致性保证。
    ///
    /// COW 查找为 O(log n)，Btree 为 O(log c + log n) 且可能同步阻塞；LogWrite 为 O(1)。
    /// 方法不是纯函数：除 LogWrite 外可能读取共享存储，Btree 还可能更新事务的冲突参考
    /// cache；但本方法不写用户值、WAL 或数据文件。当前签名不能返回读取错误。
    fn dirty_query(&self, key: <Self as KVAction>::Key)
        -> BoxFuture<Option<<Self as KVAction>::Value>>;

    /// 查询当前子事务视图，并按当前表实现记录普通读动作。
    ///
    /// Meta/Memory/LogOrdered 只在该 Key 尚无事务动作时插入 `KVActionLog::Read`，不会用读
    /// 覆盖已有 upsert/delete；随后从事务私有 COW 根返回值。Btree 同样先记 `Read`，再查
    /// 私有 overlay/tombstone；overlay 缺席时为本次调用建立 redb read transaction，读取
    /// 错误当前被吞并为 `None`。LogWrite 不记录动作并始终返回 `None`。
    ///
    /// 返回值、复杂度、同步阻塞和无错误通道边界与 [`KVAction::dirty_query`] 相同。记录读
    /// 动作会影响后续 prepare 冲突判断，因此本方法不是纯读取意义上的无副作用函数。
    fn query(&self, key: <Self as KVAction>::Key)
        -> BoxFuture<Option<<Self as KVAction>::Value>>;

    /// 在事务私有视图中插入或替换值，并采用该表实现的 dirty 写策略。
    ///
    /// Meta/Memory/LogOrdered/LogWrite 记录 `DirtyWrite(Some(value))`；前三者还更新 COW 根，
    /// LogWrite 只保留动作。Btree 当前直接复用普通 [`KVAction::upsert`]，记录 `Write` 而非
    /// `DirtyWrite`。后续同 Key 动作会替换本次记录。
    ///
    /// `key`/`value` 均被 future 消费。当前实现不在这里拒绝只读事务、非法状态、畸形 Key
    /// 或空 Value，也不写 WAL/数据文件；成功仅表示私有动作已记录。COW/Btree 更新通常为
    /// O(log n)/O(log c)，LogWrite 动作表平均 O(1)，并可能分配 COW 节点。操作持有短期同步
    /// 锁、不跨 yield，除内存分配外通常不阻塞。
    fn dirty_upsert(&self,
                    key: <Self as KVAction>::Key,
                    value: <Self as KVAction>::Value)
        -> BoxFuture<Result<(), <Self as KVAction>::Error>>;

    /// 在事务私有视图中插入或替换值，并记录普通写动作。
    ///
    /// 所有内置实现记录 `Write(Some(value))`；Meta/Memory/LogOrdered/Btree 同时更新事务私有
    /// 根或 overlay，LogWrite 只记录动作。成功、输入校验、持久化时点、锁和复杂度边界与
    /// [`KVAction::dirty_upsert`] 相同，区别仅在当前冲突分类。该调用不是幂等的操作历史：
    /// 相同 Key 的后续动作覆盖动作表记录；相同值重复写在最终数据上可能等效，但仍会重写
    /// 事务状态并参与 prepare。
    fn upsert(&self,
              key: <Self as KVAction>::Key,
              value: <Self as KVAction>::Value)
        -> BoxFuture<Result<(), <Self as KVAction>::Error>>;

    /// 异步脏删除指定 Key；返回值是表类型相关契约，不是统一的“删除前值”。
    ///
    /// Meta、Memory 和 LogOrdered 记录 `DirtyWrite(None)` 并删除事务私有 COW 根中的值，
    /// 但有意不复制、不返回旧值：无论 Key 删除前存在还是不存在，成功路径都返回
    /// `Ok(None)`。调用方不得根据这三类表的返回值判断 Key 是否曾存在。LogWrite 不支持
    /// 删除，当前是无动作的 `Ok(None)`。只有 Btree 要求在能够读取删除前值时返回
    /// `Ok(Some(value))`；它当前复用普通 delete，因此记录 `Write(None)` 而不是
    /// `DirtyWrite(None)`。
    ///
    /// `key` 的所有权移入返回的 future。Btree 严格区分三态只写缓存：缓存值直接返回；
    /// 已有 tombstone/重复删除返回 `None` 且不回读；缓存完全缺席时同步建立 redb 读事务
    /// 并返回该调用时快照中的旧值。redb `begin_read/open_table/get` 错误记录详细 error
    /// 日志并降级为 `Ok(None)`，但 tombstone 保留且删除仍可提交。redb 读取不写
    /// `cache_ref`，不代表事务创建时快照，也不参与冲突基线。Btree 当前复用普通 `delete`
    /// 路径，本契约不把其 dirty 冲突行为冻结为最终设计。
    ///
    /// 跨表返回矩阵见
    /// [`CONTRACT-ACTION-001`](../docs/SEMANTIC_CONTRACTS.md#contract-action-001) 与
    /// [`tests/kv_action_contract.rs`](../tests/kv_action_contract.rs)；Btree 完整边界与真实
    /// 专项见 `CONTRACT-BTREE-DELETE-001` 和 `tests/btree_delete_old_value.rs`。
    fn dirty_delete(&self, key: <Self as KVAction>::Key)
        -> BoxFuture<Result<Option<<Self as KVAction>::Value>, <Self as KVAction>::Error>>;

    /// 异步删除指定 Key；返回值是表类型相关契约，不是统一的“删除前值”。
    ///
    /// Meta、Memory 和 LogOrdered 记录 `Write(None)` 并删除事务私有 COW 根中的值，但
    /// 有意不复制、不返回旧值：命中与未命中都返回 `Ok(None)`，因此返回值不能表达删除
    /// 是否实际移除了记录。LogWrite 不支持删除，当前同样返回无动作的 `Ok(None)`。只有
    /// Btree 要求尽可能返回删除前值；缓存值直接返回，已有 tombstone 或重复删除返回
    /// `None` 且不回读，缓存完全缺席时同步读取本次调用时的 redb 快照。
    ///
    /// `key` 的所有权移入返回的 future。Btree 的 redb `begin_read/open_table/get` 错误会
    /// 记录详细 error 日志并降级为 `Ok(None)`；读取不写 `cache_ref`、不参与冲突检测，
    /// 删除 tombstone 仍保留并可继续提交。该同步 redb 读取可能短暂阻塞当前异步 worker，
    /// 批量删除当前为每个缓存缺席 Key 建立独立读事务。Meta/Memory/LogOrdered 使用
    /// `pi_ordmap::OrdMap::delete(copy=false)`，避免为返回值增加一次 `Binary` 引用计数克隆；
    /// 它们仍执行同一次 O(log n) COW 删除，不因省略返回值改变提交或回滚语义。
    ///
    /// 跨表返回矩阵见
    /// [`CONTRACT-ACTION-001`](../docs/SEMANTIC_CONTRACTS.md#contract-action-001) 与
    /// [`tests/kv_action_contract.rs`](../tests/kv_action_contract.rs)；Btree 完整边界与真实
    /// 专项见 `CONTRACT-BTREE-DELETE-001` 和 `tests/btree_delete_old_value.rs`。
    fn delete(&self, key: <Self as KVAction>::Key)
        -> BoxFuture<Result<Option<<Self as KVAction>::Value>, <Self as KVAction>::Error>>;

    /// 获取从指定关键字开始的关键字异步流。
    ///
    /// 对有序表实现，`key=None` 表示从最小 key（`descending=false`）或最大 key
    /// （`descending=true`）开始；`Some(key)` 是包含边界。方法返回前固定当时可见的表
    /// 快照，返回项是 owned key。流创建后，同一事务及其它事务可以继续写入，但旧流不
    /// 观察这些修改；这不等于可串行化，也不与 commit/rollback 原子绑定。
    ///
    /// 创建该流的事务必须存活到流耗尽或被释放。虽然返回 lifetime 当前没有在类型签名中
    /// 绑定 `&self`，事务释放后继续 poll 不属于合法调用域。流是 `Send` 的单消费者对象，
    /// 不应并发 poll；提前 drop 是取消方式，并会释放其 COW/redb 快照资源。
    ///
    /// Memory、Meta、LogOrdered 的创建成本为 O(1) COW 根克隆加 O(log n) 起点定位，
    /// 每项复制 owned key；构造和 poll 不跨 yield 持锁。Btree 还会同步建立 redb 读事务，
    /// 可能短暂阻塞，并在合并过程中使用至多 O(n) 去重空间。该 item 类型没有错误通道，
    /// Btree 当前的读/范围错误可能表现为空流或提前结束；这是已归档的现状边界，不是最佳
    /// 错误设计。根克隆本身不是 O(n) 拷贝，但活跃流会保留其创建时可达的 COW 节点；若
    /// 流存活期间大量改写，旧节点可能形成 O(n) 级临时保留。Btree 读事务也可能延长旧
    /// 页面/版本的保留时间。LogWrite 的当前哨兵流语义另行归档，不能按有序快照解释。
    ///
    /// 契约、修复依据和专项验证见 `CONTRACT-ITER-001`、`FIND-ITER-001` 及
    /// `tests/iterator_snapshot_safety.rs`。
    fn keys<'a>(&self,
                key: Option<<Self as KVAction>::Key>,
                descending: bool)
        -> BoxStream<'a, <Self as KVAction>::Key>;

    /// 获取从指定关键字开始的键值对异步流。
    ///
    /// 方向、包含边界、创建时快照、事务生命周期、取消、线程/异步安全、错误通道和复杂度
    /// 与 [`KVAction::keys`] 相同；每个 item 额外包含创建时快照中的 owned value。流不会
    /// 返回随后更新的 value，也不会因随后删除而丢失创建时已有项。
    fn values<'a>(&self,
                  key: Option<<Self as KVAction>::Key>,
                  descending: bool)
        -> BoxStream<'a, (<Self as KVAction>::Key, <Self as KVAction>::Value)>;

    /// 调用当前表实现的 Key 锁钩子。
    ///
    /// **当前五类内置表全部忽略 `key` 并立即返回 `Ok(())`**：不建立排他锁，不等待，不
    /// 检查 owner/重入，不提供内存可见性或事务隔离保证。该现状记录在 `FIND-LOCK-001`，
    /// 不是最终或最佳锁 API；不得用它保护并发临界区。操作为 O(1)、无分配、无 I/O、无
    /// 锁副作用且取消安全。
    fn lock_key(&self, key: <Self as KVAction>::Key)
        -> BoxFuture<Result<(), <Self as KVAction>::Error>>;

    /// 调用当前表实现的 Key 解锁钩子。
    ///
    /// 与 [`KVAction::lock_key`] 相同，当前所有内置实现都是忽略 Key 的 O(1) 成功 no-op；
    /// 即使此前未调用 `lock_key` 也成功，不做 owner 校验。它不释放任何真实锁，也不能作为
    /// 并发同步原语。
    fn unlock_key(&self, key: <Self as KVAction>::Key)
        -> BoxFuture<Result<(), <Self as KVAction>::Error>>;
}

/// 数据库表引擎类型，也是 Meta 表持久化格式的一部分。
///
/// 当前编码固定使用值 `1..=4`，由 [`KVTableMeta`] 到 [`Binary`] 的转换写入并由
/// [`KVDBTableType::from`] 读取。枚举没有声明 FFI `repr`，不得把 Rust 内存布局当作 ABI；
/// 只有下面的持久化判别值属于稳定格式。转换和 clone 均为 O(1)，类型本身不持有资源、
/// 不执行 I/O，也不会主动创建表。
///
/// 实际能力、数据文件和迭代边界见 `docs/PI_DB_ARCHITECTURE.md#arch-table-matrix`；有效组合
/// 仍由建表 API 与选项共同决定，单独构造枚举不执行校验。
#[derive(Debug, Clone, PartialEq)]
pub enum KVDBTableType {
    /// 基于 COW `pi_ordmap` 的有序内存表。
    ///
    /// `persistence=true` 合法：动作参与根 WAL，但该表不创建独立数据文件。
    MemOrdTab = 1,
    /// 基于完整内存有序根和 `pi_store::LogFile` 顺序数据日志的有序表。
    LogOrdTab,
    /// 只记录 upsert 动作的数据日志表。
    ///
    /// 当前 query/delete 不返回数据，keys/values 仍是已归档的哨兵行为，不应按有序表使用。
    LogWTab,
    /// 以 redb 为稳定数据、COW 内存 overlay/tombstone 为事务写集的有序 B 树表。
    BtreeOrdTab,
}

impl From<u8> for KVDBTableType {
    /// 从 Meta 持久化判别值恢复表类型。
    ///
    /// 仅 `1..=4` 合法。其它值会立即 panic，而不是返回可恢复错误，因此本转换只能用于已经
    /// 由上层完整性协议验证的受信输入；磁盘损坏/不可信输入边界仍记录在
    /// `FIND-CODEC-001`。操作为 O(1)，无分配、无 I/O 且无共享状态副作用。
    ///
    /// # Panics
    ///
    /// `src` 不在 `1..=4` 时 panic。
    fn from(src: u8) -> Self {
        match src {
            1 => KVDBTableType::MemOrdTab,
            2 => KVDBTableType::LogOrdTab,
            3 => KVDBTableType::LogWTab,
            4 => KVDBTableType::BtreeOrdTab,
            _ => panic!("From u8 to KVDBTableType failed, src: {}, reason: invalid src", src),
        }
    }
}

/// 一张用户表的持久化定义。
///
/// Meta 表以“表名 -> `KVTableMeta` 编码”保存数据库表目录；启动和修复先解码这些记录，
/// 再据此实例化具体表引擎。字段分别决定引擎类型、事务动作是否参与根 WAL，以及 Key/Value
/// 声明的 `pi_sinfo::EnumType`。
///
/// `persistence=true` 对 Memory 表同样合法，但只产生根 WAL，不产生 Memory 数据文件。
/// `key`/`value` 当前是声明性元数据：构造和普通 upsert/query 入口不会逐项验证传入
/// [`Binary`] 是否与其匹配。长度为 0 的持久化 Value 仍被数据库契约禁止，不能因这里声明
/// `EnumType::Bin` 而放宽。
///
/// 本类型的 clone 成本取决于嵌套 `EnumType`，其中复杂描述通过 `Arc` 共享；getter 均为
/// O(1) 借用。类型本身不执行 I/O、不持锁、无内部可变性，可在线程间随字段能力移动/共享。
/// 持久化格式与受信解码边界由本类型和 [`Binary`] 之间的两个 `From` 实现定义。
///
/// # Example
///
/// ```
/// use pi_db::{Binary, KVDBTableType, KVTableMeta};
/// use pi_sinfo::EnumType;
///
/// let meta = KVTableMeta::new(
///     KVDBTableType::MemOrdTab,
///     true,
///     EnumType::U64,
///     EnumType::Bin,
/// );
/// let restored = KVTableMeta::from(Binary::from(meta.clone()));
/// assert_eq!(restored, meta);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct KVTableMeta {
    table_type:     KVDBTableType,  //表类型
    persistence:    bool,           //是否持久化
    key:            EnumType,       //关键字类型
    value:          EnumType,       //值类型
}

impl From<Binary> for KVTableMeta {
    /// 从当前 Meta 表持久化格式恢复表元信息。
    ///
    /// `src` 被消费；返回值拥有恢复后的类型描述，不借用输入。解码时间和新增空间为
    /// O(k + v)，其中 k、v 是 Key/Value 类型描述长度；不会执行文件 I/O 或持有数据库锁。
    /// persistence 字节 `0` 解码为 `false`，其它任意值均解码为 `true`。当前实现不检查尾随
    /// 字节，也不验证类型描述是否与表数据相符。
    ///
    /// 这是受信内部格式的无错误返回转换：它直接读取固定字段、按声明长度切片，并对
    /// `EnumType::decode` 使用 `unwrap`。不可信、截断、长度损坏或非法类型输入可能 panic；
    /// 该现状不是最终/最佳错误设计，见 `FIND-CODEC-001`。调用方不得用本 API 检查任意文件。
    ///
    /// # Panics
    ///
    /// 输入短于固定头、声明长度越界、表类型不在 `1..=4`，或依赖解码器拒绝/直接 panic 时。
    fn from(src: Binary) -> Self {
        let mut buf = src.as_ref();
        let mut offset = 0;

        //读取键值对表的类型
        let table_type = KVDBTableType::from(buf.get_u8());
        offset += 1;

        let persistence = if buf.get_u8() == 0 {
            //读取键值对表不需要持久化的标记
            false
        } else {
            //读取键值对表需要持久化的标记
            true
        };
        offset += 1;

        //读取键值对表的关键字类型
        let key_len = buf.get_u16_le() as usize;
        offset += 2;
        let mut read_buffer = ReadBuffer::new(&buf[0..key_len], 0);
        buf.advance(key_len); //移动缓冲区指针
        offset += key_len;
        let key = EnumType::decode(&mut read_buffer).unwrap();

        let value_len = buf.get_u16_le() as usize;
        offset += 2;
        let mut read_buffer = ReadBuffer::new(&buf[0..value_len], 0);
        buf.advance(value_len);
        offset += value_len;
        let value = EnumType::decode(&mut read_buffer).unwrap();

        KVTableMeta {
            table_type,
            persistence,
            key,
            value,
        }
    }
}

impl KVTableMeta {
    /// 构造表元信息，不创建表也不验证字段组合。
    ///
    /// - `table_type` 选择后续建表时的引擎；对应 options 是否有效由建表 API 检查。
    /// - `persistence` 表示写事务是否参与根 WAL；Memory 为 `true` 时仍没有独立数据文件。
    /// - `key` 是有序比较所需的声明类型，调用方仍须提供合法、规范的 `pi_bon` Key 编码。
    /// - `value` 是 Value 声明类型，当前实现不做逐值运行时验证，也不允许持久化空 Value。
    ///
    /// 参数所有权全部移入返回值。函数为 O(1)，不分配数据库资源、不执行 I/O、不持锁，
    /// 并且相同参数重复构造没有外部副作用。
    pub fn new(table_type: KVDBTableType,
               persistence: bool,
               key: EnumType,
               value: EnumType) -> Self {
        KVTableMeta {
            table_type,
            persistence,
            key,
            value,
        }
    }

    /// 从旧格式中连续解码 Key/Value 类型，并与显式表属性组成元信息。
    ///
    /// `table_type` 和 `persistence` 不从 `bin` 读取；`bin` 必须依次包含两个合法的
    /// `EnumType` `pi_bon` 编码。输入只在调用期间借用，不被保存。可恢复的边界/类型错误以
    /// [`ReadBonErr`] 返回；当前依赖的 `EnumType::decode` 对部分非法判别值仍会 panic，且本
    /// 函数不拒绝第二个类型之后的尾随字节。这是当前兼容入口事实，不是最终安全 decoder。
    ///
    /// 时间和新增空间为 O(k + v)，不执行 I/O、不持锁、无共享状态副作用。成功只说明旧
    /// 类型描述可解码，不说明实际 Key/Value 数据符合声明。
    ///
    /// # Errors
    ///
    /// 输入为空、截断或可恢复地不符合两个 `EnumType` 编码时返回 `ReadBonErr`。
    ///
    /// # Panics
    ///
    /// 依赖解码器遇到其以 panic 表达的非法类型判别值时可能 panic。
    pub fn with_compatibled(table_type: KVDBTableType,
                            persistence: bool,
                            bin: &[u8]) -> Result<Self, ReadBonErr> {
        let mut buffer = ReadBuffer::new(bin, 0);
        let key = EnumType::decode(&mut buffer)?;
        let value = EnumType::decode(&mut buffer)?;

        Ok(Self::new(table_type, persistence, key, value))
    }

    /// 借用表引擎类型。
    ///
    /// 返回值与 `self` 生命周期相同，不创建快照、不转移所有权。O(1)、纯只读、无分配且
    /// 不阻塞。
    pub fn table_type(&self) -> &KVDBTableType {
        &self.table_type
    }

    /// 返回该表是否参与持久化事务/WAL 协议。
    ///
    /// 这是 O(1) 的元数据读取，不查询文件或当前事务状态。对 Memory 返回 `true` 只表示
    /// 参与根 WAL，不表示存在独立表数据文件。
    pub fn is_persistence(&self) -> bool {
        self.persistence
    }

    /// 借用声明的 Key 类型。
    ///
    /// 返回值不是运行时校验结果；当前调用方仍负责保证 Key 是匹配的规范 `pi_bon` 编码。
    /// 操作为 O(1)、纯只读、无分配且不阻塞。
    pub fn key_type(&self) -> &EnumType {
        &self.key
    }

    /// 借用声明的 Value 类型。
    ///
    /// 返回值不是运行时校验结果，也不放宽空 Value 禁令。操作是 O(1) 纯借用，无分配、
    /// 无锁且不阻塞。
    pub fn value_type(&self) -> &EnumType {
        &self.value
    }
}

/// `pi_async_transaction::UnitTransaction` 使用的表事务服务质量标签。
///
/// 该枚举只描述当前事务承诺，不自行加锁、持久化或改变调度。当前根事务始终报告 [`Safe`](Self::Safe)；
/// 各内置子表事务在 `require_persistence=true` 时报告 `Safe`，否则报告
/// [`ThreadSafe`](Self::ThreadSafe)。[`Unsafe`](Self::Unsafe) 当前没有内置生产返回点，保留
/// 为类型域的一部分，不能据此推断存在已支持的无同步事务模式。
///
/// clone/default/比较均为 O(1)，无分配、无 I/O、无副作用并可跨线程使用。该分类不是 Rust
/// 内存安全开关：无论返回何值，安全公开 API 都不得产生 data race 或 UB。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableTrQos {
    /// 不承诺事务数据安全；当前内置事务不返回此值。
    Unsafe = 0,
    /// 保证并发访问的线程安全，但不承诺通过根 WAL 实现崩溃后恢复。
    ThreadSafe,
    /// 事务参与当前数据库的完整安全/持久化协议。
    Safe,
}

impl Default for TableTrQos {
    /// 返回最保守的 [`TableTrQos::Safe`] 标签。
    ///
    /// 操作为 O(1)、纯函数且不会分配或 panic。
    fn default() -> Self {
        TableTrQos::Safe
    }
}

/// 单个表事务针对一个 Key 保留的当前动作。
///
/// 各表事务用 `HashMap<Binary, KVActionLog>` 保存动作；同一事务对同一 Key 的后续动作会通过
/// `insert` 替换先前记录，因此当前值代表最终记录，不是完整操作历史。`Some(value)` 表示
/// upsert 的新值，`None` 表示删除/tombstone，不表示合法空 Value。
///
/// `Write` 与 `DirtyWrite` 的真实差异在冲突策略：普通写参与预提交表和事务创建根的冲突
/// 检查；dirty 写跳过根冲突检查，并在当前表的 prepare 冲突规则中允许覆盖/并存。旧表述
/// “dirty 写不会覆盖读记录”与实现不符；当前 dirty upsert/delete 同样会替换本事务该 Key
/// 的既有 `Read`。这是当前实现语义，不是最终或最佳隔离级别设计。
///
/// clone 的 Binary payload 通过 `Arc` 共享，成本为 O(1)。枚举自身无内部可变性、不执行
/// I/O、不持锁；真正副作用发生在持有它的表事务状态机中。该内部动作类型虽公开可构造，
/// 外部构造不会自动应用到数据库。
#[derive(Debug, Clone)]
pub enum KVActionLog {
    /// 普通 query 首次访问该 Key 时记录的读动作；已有写动作不会被 query 降级为读。
    Read,
    /// 需要普通冲突检查的写动作；`Some` 为 upsert，`None` 为删除。
    Write(Option<Binary>),
    /// 放宽普通冲突检查的写动作；`Some` 为 dirty upsert，`None` 为 dirty delete。
    DirtyWrite(Option<Binary>),
}

impl KVActionLog {
    /// 判断当前 variant 是否为 [`KVActionLog::DirtyWrite`]。
    ///
    /// payload 不会被读取、克隆或修改。函数为 O(1) 纯读取，幂等、线程安全、不分配、不
    /// 持锁且不会 panic。返回 `false` 同时覆盖 `Read` 和普通 `Write`。
    #[inline]
    pub fn is_dirty_writed(&self) -> bool {
        if let KVActionLog::DirtyWrite(_) = self {
            true
        } else {
            false
        }
    }
}

/// 聚合一个根事务中各持久化子表的最终持久化成功信号。
///
/// 根 WAL 已经 append 并 flush 成功后，各子表才会发布内存根并异步写入自己的数据文件。
/// 本类型保存需要持久化的子表数量；每个子表仅在最终持久化成功后调用一次
/// `confirm(tid, cid, Ok(()))`。最后一个成功信号将异步投递
/// [`AsyncCommitLog::confirm`]，使根 WAL 中对应事务进入最终确认状态。
///
/// # 成功信号协议
///
/// [`Transaction2Pc::CommitConfirm`](pi_async_transaction::Transaction2Pc::CommitConfirm)
/// 的通用签名使用 `Result<ConfirmOutput, ConfirmError>`，因此本类型在类型层面能够接收
/// `Err`。但在当前 `pi_db` 内置协议中，**合法生产调用只允许传入 `Ok(())`**：
///
/// - Memory+persistence 没有独立表数据文件，内存根发布完成后立即发送成功信号；
/// - Meta、LogOrdered、LogWrite 和 Btree 只在日志或 redb 事务持久化成功后发送成功信号；
/// - 表持久化失败时不调用本确认器，使成功计数无法归零并保留根 WAL，供重试或
///   `try_repair` 恢复。
///
/// 因此该回调是“成功计数器”，不是要求成功和失败都必须到达的“完成回调”。直接传入
/// `Err` 不属于内置合法调用域。当前实现对 Fatal `Err` 直接返回错误，而 Normal/Conflicts
/// `Err` 会继续进入计数；这是非协议输入的当前实现细节，调用方不得依赖，也不能据此推断
/// 内置持久化失败会提前确认 WAL。
///
/// # 构造与调用约束
///
/// - `tid` 和 `cid` 必须与根 WAL 中待确认事务完全一致；
/// - `cid` 在任何回调发生前必须为 `Some`；当前错误组合会在调用路径中 `unwrap` panic；
/// - `count` 必须等于需要持久化的子表数；没有持久化子表时允许为 0，但该确认器仅作为 inert
///   参数穿过可写空 WAL 事务树，任何节点都不得调用它；
/// - 每个计数内子表必须恰好调用一次，重复或超额调用不是幂等操作，当前可能下溢或重复投递；
/// - 根事务自身不参与该计数，非持久化子表也不得调用。
///
/// # 返回值与副作用
///
/// 匹配的成功信号返回 `Ok(())`；UID 不匹配或 Fatal 非协议输入返回
/// [`KVTableTrError`]。返回 `Ok(())` 只表示同步计数步骤完成，以及最后一个信号可能已成功
/// 请求 runtime 投递后台任务；它不表示 logger 的异步 I/O 已经完成。后台任务投递结果和
/// [`AsyncCommitLog::confirm`] 错误当前不会返回给调用方，错误仅记录日志，WAL 的最终物理状态
/// 由 logger 决定。
///
/// 调用不是纯函数：它会原子修改共享计数，并可能分配/投递一个后台任务和执行根 WAL I/O。
/// 同步路径不持有同步锁或异步锁、不等待 I/O，也不执行用户回调；时间复杂度和额外同步空间
/// 均为 O(1)。最后一个成功信号额外创建一个 O(1) 后台任务。克隆实例共享同一计数状态，
/// 可由多线程并发发送不同子表的成功信号；调用顺序不影响“最后一个”判定。
///
/// # 安全与生命周期
///
/// 类型通过 `Arc` 共享 runtime、logger、UID 和原子计数。投递的任务持有克隆，因此调用返回后
/// 所需状态仍存活。当前 `Send`/`Sync` 依赖 `MultiTaskRuntime` 与 `AsyncCommitLog` 的线程安全
/// 契约以及“只有原子计数可变”的内部布局；本 API 不涉及 V8 或 FFI。任务不可由调用方取消，
/// runtime 拒绝投递时当前只表现为 WAL 长期未确认。
///
/// # 示例
///
/// 下面的 helper 只能在表数据已经最终持久化之后调用：
///
/// ```no_run
/// use pi_async_transaction::AsyncCommitLog;
/// use pi_db::{KVDBCommitConfirm, KVTableTrError};
/// use pi_guid::Guid;
///
/// fn report_persisted<C, Log>(
///     confirm: &KVDBCommitConfirm<C, Log>,
///     transaction_uid: Guid,
///     commit_uid: Guid,
/// ) -> Result<(), KVTableTrError>
/// where
///     C: Clone + Send + 'static,
///     Log: AsyncCommitLog<C = C, Cid = Guid>,
/// {
///     confirm(transaction_uid, commit_uid, Ok(()))
/// }
/// ```
///
/// # 设计与验证
///
/// 完整协议、生产调用点和历史误判说明见本地
/// [CONTRACT-CFM-001](../docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only)；合法成功计数
/// 由 `tests/commit_confirmation_contract.rs` 验证，真实 LogOrdered 持久化失败、WAL 保留和
/// 重启恢复由 `tests/commit_confirmation_real_environment.rs` 验证。公开低层构造、非法组合、
/// 重复调用和任务投递可观测性仍分别记录在 `FIND-CONFIRM-001`、`FIND-SPAWN-001`，当前说明
/// 不把这些风险提升为受支持行为。
#[derive(Clone)]
pub struct KVDBCommitConfirm<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<(
    MultiTaskRuntime<()>,   //异步运行时
    Log,                    //提交日志记录器
    Guid,                   //事务唯一id
    Option<Guid>,           //提交唯一id，只有需要持久化的事务，才分配提交唯一id
    AtomicUsize,            //事务提交确认的计数
)>);

// SAFETY: 共享字段在构造后不再替换，唯一内部可变状态是 AtomicUsize；Log 的 trait
// 边界要求 Send + Sync，MultiTaskRuntime 的共享句柄用于跨线程投递任务。该依据和剩余
// 审计缺口记录在 docs/REVIEW_FINDINGS.md#find-unsafe-001。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for KVDBCommitConfirm<C, Log> {}
// SAFETY: 与上面的 Send 实现相同；并发调用只通过 SeqCst 原子操作修改成功计数，logger
// 和 runtime 的共享并发责任由各自 trait/类型契约承担。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for KVDBCommitConfirm<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> FnOnce<(Guid, Guid, Result<(), KVTableTrError>)> for KVDBCommitConfirm<C, Log> {
    type Output = Result<(), KVTableTrError>;

    extern "rust-call" fn call_once(self, args: (Guid, Guid, Result<(), KVTableTrError>))
                                    -> Self::Output {
        self.call(args)
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> FnMut<(Guid, Guid, Result<(), KVTableTrError>)> for KVDBCommitConfirm<C, Log> {
    extern "rust-call" fn call_mut(&mut self, args: (Guid, Guid, Result<(), KVTableTrError>))
                                   -> Self::Output {
        self.call(args)
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Fn<(Guid, Guid, Result<(), KVTableTrError>)> for KVDBCommitConfirm<C, Log> {
    extern "rust-call" fn call(&self, args: (Guid, Guid, Result<(), KVTableTrError>))
                               -> Self::Output {
        if let Err(e) = args.2 {
            // Transaction2Pc 的通用回调类型允许 Err，但 pi_db 内置表不会用 Err 报告
            // 持久化失败，而是完全不调用确认器。这里保留的是非协议输入的当前防御行为：
            // Fatal 不计数；Normal/Conflicts 仍会继续计数，调用方不得依赖该分支。
            // 详见 CONTRACT-CFM-001：
            // docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only。
            if let ErrorLevel::Fatal = &e.level() {
                // Fatal 非协议输入不贡献成功确认计数。
                return Err(e);
            }
        }

        self.confirm_commited(args.0, args.1)
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVDBCommitConfirm<C, Log> {
    /// 构建一个根 WAL 事务的持久化子表成功确认聚合器。
    ///
    /// `rt` 用于在最后一个成功信号到达时投递异步 logger 确认任务，并会随返回值一起保存；
    /// `commit_logger` 是该根 WAL 的 logger，也会被返回值持有；`tid` 和 `cid` 必须对应同一
    /// 已落地但尚未最终确认的根事务；`count` 必须精确等于该根事务中需要最终持久化的子表数。
    /// `count=0` 只用于没有根 WAL、没有持久化子表回调的可写空输出提交，构造后不得调用。
    ///
    /// 当前构造本身只分配一个 `Arc`，时间和空间复杂度均为 O(1)，不执行 I/O、不阻塞、
    /// 不持锁，也不验证参数组合。`cid=None`、对 `count=0` 实例发出回调、计数不准确、重复回调
    /// 和错误 UID 属于调用方违反前置条件；其中部分组合会在后续调用时 panic、下溢或错误投递。该公开
    /// 构造风险见 [FIND-CONFIRM-001](../docs/REVIEW_FINDINGS.md#find-confirm-001)。
    pub fn new(rt: MultiTaskRuntime<()>,
               commit_logger: Log,
               tid: Guid,
               cid: Option<Guid>,
               count: usize) -> Self {
        KVDBCommitConfirm(Arc::new((
            rt,
            commit_logger,
            tid,
            cid,
            AtomicUsize::new(count),
        )))
    }

    /// 借用该确认器保存的根提交日志记录器。
    ///
    /// 返回值与 `self` 生命周期相同，不转移所有权，也不代表 logger 状态快照。该访问为
    /// O(1) 纯借用，不分配、不阻塞、不持锁且没有副作用；调用方仍必须遵守 logger 自身的
    /// 并发、异步和生命周期契约。当前生产代码只在调试特性下读取 checkpoint。
    pub fn commit_logger(&self) -> &Log {
        &(self.0).1
    }

    // 接受一个子表的最终持久化成功信号。先校验根事务/提交 UID，再以 SeqCst 原子递减
    // 成功计数；最后一个信号异步投递根 WAL 确认。该函数不等待 logger I/O，且忽略 spawn
    // 返回值。调用 count=0 实例或重复/超额调用会破坏计数假设，属于 FIND-CONFIRM-001 的
    // 非协议输入；count=0 实例本身是空 WAL 提交所需的合法 inert 参数。
    // 正反向协议入口：docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only。
    #[inline(always)]
    fn confirm_commited(&self, tid: Guid, cid: Guid) -> Result<(), KVTableTrError> {
        if (self.0).2 != tid || (self.0).3.clone().unwrap() != cid {
            //提交确认的事务唯一id或提交唯一id与待确主人的唯一id不匹配，则立即返回错误原因
            return Err(KVTableTrError::new_transaction_error(ErrorLevel::Normal,
                                                             format!("Confirm commited failed, require_transaction_uid: {:?}, require_commit_uid: {:?}, transaction_uid: {:?}, commit_uid: {:?}, reason: invalid transaction_uid or commit_uid", (self.0).2, (self.0).3, tid, cid)));
        }

        // 只对合法的子表持久化成功信号计数；持久化失败路径不会调用本函数。
        if (self.0).4.fetch_sub(1, Ordering::SeqCst) <= 1 {
            // 所有持久化子表的成功信号均已到达，异步确认根 WAL；同步调用立即返回。
            let confirmer = self.clone();
            let _ = (self.0).0.spawn(async move {
                let last = COMMITED_LEN.fetch_add(1, Ordering::Relaxed);
                //事务已确认提交
                if let Err(e) = (confirmer.0)
                    .1
                    .confirm(cid.clone())
                    .await {
                    //提交日志的确认错误
                    warn!("Confirm commit log failed, transaction_uid: {:?}, commit_uid: {:?}, reason: {:?}", tid, cid, e);
                }
                #[cfg(feature = "log_table_debug")]
                {
                    let event = TransactionDebugEvent::End(tid.clone(), cid.clone());
                    let logger = transaction_debug_logger();
                    logger.log(event);
                }
            });
        }

        Ok(())
    }
}

static COMMITED_LEN: AtomicUsize = AtomicUsize::new(0);

/// `pi_db` 表事务和根事务共享的错误类型。
///
/// [`Common`](Self::Common) 保存 `pi_async_transaction::ErrorLevel` 和已格式化原因；
/// [`Conflicts`](Self::Conflicts) 保存首个冲突表名与 Key；[`AllConflicts`](Self::AllConflicts)
/// 保存完整冲突集合，两者始终按可恢复的 `Normal` 等级对待。Fatal 表示不可恢复且不可
/// rollback；Normal/Conflicts 会使当前事务树失败，但在
/// 其它节点没有 Fatal 时仍属于可 rollback、可重试或可忽略的失败。
///
/// 本类型实现 `Debug`，未实现 `Display`/`std::error::Error`。字段拥有其数据，不借用事务；
/// `Common` 的空间取决于消息长度，冲突 payload 拥有表名和 Key。检查 variant/level 为 O(1)，
/// 构造普通错误需要 O(n) 格式化，完整冲突归并会按原始字节排序和去重。
///
/// 类型可跨线程移动和共享且无内部可变性；它本身不执行 rollback、日志、I/O 或回调，真正
/// 的错误传播由事务树负责。测试入口见 `tests/core_types_contract.rs`；可恢复与 Fatal 边界见
/// `CONTRACT-TR-003` / `CONTRACT-TR-004`。
#[derive(Debug)]
pub enum KVTableTrError {
    /// 普通事务错误：错误等级和拥有的诊断字符串。
    Common(ErrorLevel, String),
    /// 预提交冲突：首个冲突表名和 Key；其 [`KVTableTrError::level`] 固定返回 Normal。
    Conflicts(Atom, Binary),
    /// 版本协议完整冲突：非空、去重并按表名/Key 原始字节确定性排序。
    AllConflicts(Vec<TableKey>),
}

// SAFETY: 三个 variant 只包含拥有的 ErrorLevel/String/Atom/Binary/Vec；它们在构造后没有
// 内部可变状态。String/Vec 由标准库保证 Send，Atom/Binary 的跨线程安全由各自类型契约保证。
unsafe impl Send for KVTableTrError {}
// SAFETY: 共享引用只能读取不可变字段；没有裸指针、线程 owner 状态或非同步 interior
// mutability。Atom 的全局池同步与 Binary 的 Arc 引用计数由依赖类型负责。
unsafe impl Sync for KVTableTrError {}

impl TransactionError for KVTableTrError {
    /// 将任意 `Debug + 'static` 原因格式化为 [`KVTableTrError::Common`]。
    ///
    /// `level` 原样保存；`reason` 仅在调用期间借用用于 `Debug` 格式化，返回值保存字符串而
    /// 不保存原错误。格式固定带有 `Table transaction error, reason:` 前缀。时间和空间为
    /// O(n)，n 为格式化消息长度；除分配外无副作用，不执行 I/O 或事务状态转换。
    fn new_transaction_error<E>(level: ErrorLevel, reason: E) -> Self
        where E: Debug + Sized + 'static
    {
        KVTableTrError::Common(
            level,
            format!("Table transaction error, reason: {:?}", reason),
        )
    }
}

impl TransactionConflictError for KVTableTrError {
    type ConflictSet = Vec<TableKey>;

    fn into_conflict_set(self) -> Result<Self::ConflictSet, Self> {
        match self {
            KVTableTrError::Conflicts(table, key) => {
                Ok(vec![TableKey { table, key }])
            },
            KVTableTrError::AllConflicts(conflicts) => Ok(conflicts),
            error => Err(error),
        }
    }

    fn from_conflict_set(conflicts: Self::ConflictSet) -> Self {
        let conflicts = key_version::normalize_conflicts(conflicts);
        assert!(!conflicts.is_empty(),
                "Construct all conflicts failed, reason: conflict set must not be empty");
        KVTableTrError::AllConflicts(conflicts)
    }

    fn merge_conflict_sets(target: &mut Self::ConflictSet, mut source: Self::ConflictSet) {
        target.append(&mut source);
    }
}

impl KVTableTrError {
    /// 构造版本协议的非空、确定性排序且去重的完整冲突集合。
    pub(crate) fn new_all_conflicts_error(conflicts: Vec<TableKey>) -> Self {
        let conflicts = key_version::normalize_conflicts(conflicts);
        assert!(!conflicts.is_empty(),
                "Construct all conflicts failed, reason: conflict set must not be empty");
        Self::AllConflicts(conflicts)
    }

    /// 构造拥有首个冲突位置的可恢复预提交冲突错误。
    ///
    /// `table` 和 `key` 的所有权移入；当前实现重新从表名内容取得 `Atom`，并复制 Key 字节
    /// 到独立 `Binary` allocation，因此返回错误不依赖调用方原 owner 生命周期。Key 仍须是
    /// 合法编码，但本函数不验证。时间/新增空间为 O(table_len + key_len)，可能访问全局 Atom
    /// 池并分配内存；不执行数据库 I/O、不持有事务锁且不修改事务状态。
    pub fn new_conflicts_error(table: Atom, key: Binary) -> Self {
        KVTableTrError::Conflicts(
            table.as_ref().into(),
            Binary::from_slice(key),
        )
    }

    /// 判断是否为 [`KVTableTrError::Common`]。
    ///
    /// O(1) 纯读取；不分配、不阻塞、幂等且不会 panic。
    pub fn is_common(&self) -> bool {
        if let Self::Common(_, _) = self {
            true
        } else {
            false
        }
    }

    /// 判断是否为任一冲突 variant。
    ///
    /// O(1) 纯读取；不分配、不阻塞、幂等且不会 panic。
    pub fn is_conflicts(&self) -> bool {
        matches!(self, Self::Conflicts(_, _) | Self::AllConflicts(_))
    }

    /// 判断是否为版本协议返回的完整冲突集合。
    ///
    /// O(1) 纯读取；不分配、不阻塞、幂等且不会 panic。
    pub fn is_all_conflicts(&self) -> bool {
        matches!(self, Self::AllConflicts(_))
    }

    /// 返回事务树用于判定可恢复性的错误等级。
    ///
    /// `Common` 返回保存的等级；两个冲突 variant 固定返回 [`ErrorLevel::Normal`]，表示冲突不是
    /// Fatal。返回值为 owned clone，O(1)、无分配、无副作用且不阻塞。
    pub fn level(&self) -> ErrorLevel {
        if let Self::Common(level, _) = self {
            level.clone()
        } else {
            ErrorLevel::Normal
        }
    }

    /// 借用预提交冲突的首个表名和 Key。
    ///
    /// `Conflicts` 返回自身位置；`AllConflicts` 返回集合第一项；`Common` 返回 `None`。引用与
    /// `self` 生命周期相同，不克隆 payload，也不代表实时事务状态。O(1)、纯只读、无分配且
    /// 不阻塞。
    pub fn conflicts(&self) -> Option<(&Atom, &Binary)> {
        match self {
            Self::Conflicts(table, binary) => Some((table, binary)),
            Self::AllConflicts(conflicts) => conflicts
                .first()
                .map(|conflict| (&conflict.table, &conflict.key)),
            Self::Common(_, _) => None,
        }
    }

    /// 借用完整冲突集合。
    ///
    /// 仅 `AllConflicts` 返回非空 slice；其它 variant 返回 None。O(1)、无克隆和副作用。
    pub fn all_conflicts(&self) -> Option<&[TableKey]> {
        if let Self::AllConflicts(conflicts) = self {
            Some(conflicts.as_slice())
        } else {
            None
        }
    }
}

use std::sync::OnceLock;
static TRANSACTION_DEBUG_LOGGER: OnceLock<TransactionDebugLogger> = OnceLock::new();

/// 初始化进程级事务调试日志器并启动周期性落盘任务。
///
/// `path` 是调试日志文件路径；`interval` 是事件批次轮询间隔，`timeout` 是单次日志提交超时，
/// 两者单位均为毫秒且小于 1000 的值会被提升为 1000。首次成功设置全局槽位的调用启动一个
/// 永久后台任务；后续调用不会替换全局实例，但当前实现仍会先同步构造并打开一个临时日志器
/// 再将其丢弃，因此该 API 应在进程初始化阶段恰好调用一次。
///
/// 构造过程会把打开文件的异步任务提交给 `rt`，随后同步等待结果。文件打开失败、runtime
/// 无法执行任务或内部通道异常会 panic；不要在无法继续调度该任务的 runtime owner 线程上
/// 调用。该诊断设施不参与事务提交、确认或恢复正确性，也没有显式 shutdown API。
pub fn init_transaction_debug_logger<P: AsRef<Path>>(rt: MultiTaskRuntime<()>,
                                                     path: P,
                                                     interval: usize,
                                                     timeout:  usize) {
    let logger = TransactionDebugLogger::new(rt, path);
    if let Ok(_) = TRANSACTION_DEBUG_LOGGER.set(logger.clone()) {
        logger.startup(interval, timeout);
    }
}

/// 返回进程级事务调试日志器的共享引用。
///
/// 必须先成功调用 [`init_transaction_debug_logger`]，否则本函数 panic。返回引用由静态
/// [`OnceLock`] 持有，可跨线程共享，读取为 O(1)，不分配、不执行 I/O，也不刷新日志。
pub fn transaction_debug_logger<'a>() -> &'a TransactionDebugLogger {
    TRANSACTION_DEBUG_LOGGER
        .get()
        .unwrap()
}

use pi_atom::Atom;
use pi_async_transaction::manager_2pc::Transaction2PcStatus;
/// 事务调试日志器能够记录的生命周期事件。
///
/// 这些事件只用于诊断，不推进事务状态，也不能代替 WAL、提交确认或 manager 计数。调用方
/// 必须按真实事务时序发送；日志器会记录缺失 Begin、重复 TID 等异常，但不会修复事务。
pub enum TransactionDebugEvent {
    /// 事务开始：`(tid, 当前状态, 是否可写, 是否要求根 WAL, 预提交输出容量)`。
    Begin(Guid, Transaction2PcStatus, bool, bool, usize),
    /// 子表提交：`(tid, cid, 当前状态, 表名, 动作数, WAL/表日志索引)`。
    Commit(Guid, Guid, Transaction2PcStatus, Atom, usize, usize),
    /// 子表提交确认：`(tid, cid, 表名, 是否可写, 是否要求根 WAL)`。
    CommitConfirm(Guid, Guid, Atom, bool, bool),
    /// 根事务结束：`(tid, cid)`；处理后会移除该 TID 的开始时间记录。
    End(Guid, Guid),
}

use std::path::Path;
use std::time::Instant;
use crossbeam_channel::{Sender, Receiver, unbounded, bounded};
use dashmap::{DashMap, mapref::entry::Entry};
use pi_store::log_store::log_file::{LogFile, LogMethod};

/// 以非有界通道接收事务事件并周期性写入独立日志文件的共享诊断器。
///
/// clone 仅增加内部 `Arc` 引用。生产者调用 [`Self::log`] 时不等待磁盘 I/O，但如果消费者
/// 长期停滞，非有界队列会持续占用内存；后台 [`Self::startup`] 任务会永久持有实例，当前
/// 没有停止或 drain 完成回执。该类型不是事务一致性组件，日志缺失不能改变提交结果。
pub struct TransactionDebugLogger(Arc<InnerTransactionDebugLogger>);

// SAFETY: 内部 runtime/通道/并发 Map/LogFile 均通过各自线程安全接口共享，外层只移动 Arc
// owner，不暴露内部可变引用或裸指针。事务诊断事件的时序正确性仍由调用方负责。
unsafe impl Send for TransactionDebugLogger {}
// SAFETY: 所有共享可变状态分别由 channel、DashMap 或 LogFile 自身同步；`&self` 方法不会
// 绕过这些同步原语。该实现不保证 `startup` 重复调用的业务语义。
unsafe impl Sync for TransactionDebugLogger {}

impl Clone for TransactionDebugLogger {
    fn clone(&self) -> Self {
        TransactionDebugLogger(self.0.clone())
    }
}

impl TransactionDebugLogger {
    /// 打开调试日志并构造尚未启动消费者循环的日志器。
    ///
    /// 本函数提交异步文件打开任务后同步等待，所以可能阻塞当前 OS 线程；文件打开、任务
    /// 调度或内部通道失败会 panic。成功只表示日志文件已打开，调用方仍须恰好调用一次
    /// [`Self::startup`] 才会消费事件。路径、runtime 和日志文件句柄会保留到所有 clone 及
    /// 永久后台任务释放为止。
    pub fn new<P: AsRef<Path>>(rt: MultiTaskRuntime<()>,
                               path: P) -> Self {
        let (sender, receiver) = unbounded();
        let times = DashMap::new();
        let rt_copy = rt.clone();
        let (s, r) = bounded(1);
        let path = path.as_ref().to_path_buf();
        rt.spawn(async move {
            let log = LogFile
            ::open(rt_copy.clone(),
                   path,
                   8096,
                   128 * 1024 * 1024,
                   None)
                .await
                .unwrap();
            s.send(log);
        });
        let log = r.recv().unwrap();

        let inner = InnerTransactionDebugLogger {
            rt,
            sender,
            receiver,
            times,
            log,
        };

        TransactionDebugLogger(Arc::new(inner))
    }

    /// 将一个诊断事件同步放入非有界内存通道。
    ///
    /// 该调用不等待文件写入、提交或 flush，也不返回耐久性回执；当前实现忽略通道发送错误。
    /// 单次操作通常为 O(1)，可能分配队列节点，并可能在消费者停滞时造成无界积压。
    pub fn log(&self, event: TransactionDebugEvent) {
        self.0
            .sender
            .send(event);
    }

    /// 启动永久的事件批处理与日志提交循环。
    ///
    /// `interval` 和 `timeout` 均以毫秒计并至少为 1000。每轮先一次性收集当前通道中的全部
    /// 事件，更新 TID 起始时间表并 append 诊断记录，再提交本轮最后一个日志 ID，然后等待
    /// 下一轮。方法立即返回，不代表首轮已运行或日志已落盘。
    ///
    /// 当前实现没有防重复 guard；同一实例多次调用会创建多个消费者并改变事件分配与提交
    /// 顺序，因此只允许启动一次。后台任务没有显式停止接口，会持有 runtime、日志文件和
    /// 内部 `Arc` 直至 runtime/进程结束。
    pub fn startup(self,
                   mut interval: usize,
                   mut timeout: usize) {
        if interval < 1000 {
            interval = 1000;
        }
        if timeout < 1000 {
            timeout = 1000;
        }

        let logger = self.clone();
        self.0.rt.spawn(async move {
            loop {
                let events: Vec<TransactionDebugEvent> = logger.0.receiver.try_iter().collect();

                let mut log_id = 0;
                for event in events {
                    match event {
                        TransactionDebugEvent::Begin(tid, status, writable, require_persistence, output_size) => {
                            match logger.0.times.entry(tid.clone()) {
                                Entry::Occupied(o) => {
                                    //事务ID冲突
                                    log_id = logger
                                        .0
                                        .log
                                        .append(LogMethod::PlainAppend,
                                                format!("{:?}", tid).as_bytes(),
                                                format!("Transaction id conflict:\n\tstatus: {:?}\n\twritable: {:?}\n\trequire_persistence: {:?}\n\toutput_size: {:?}\n",
                                                        status,
                                                        writable,
                                                        require_persistence,
                                                        output_size).as_bytes());
                                },
                                Entry::Vacant(v) => {
                                    log_id = logger
                                        .0
                                        .log
                                        .append(LogMethod::PlainAppend,
                                                format!("{:?}", tid).as_bytes(),
                                                format!("Begin transaction:\n\tstatus: {:?}\n\twritable: {:?}\n\trequire_persistence: {:?}\n\toutput_size: {:?}\n",
                                                        status,
                                                        writable,
                                                        require_persistence,
                                                        output_size).as_bytes());
                                    v.insert(Instant::now());
                                },
                            }
                        },
                        TransactionDebugEvent::Commit(tid, cid, status, table, actions_len, log_index) => {
                            if let Some(item) = logger.0.times.get(&tid) {
                                //事务存在
                                let time = item.value().elapsed();
                                log_id = logger
                                    .0
                                    .log
                                    .append(LogMethod::PlainAppend,
                                            format!("{:?}", tid).as_bytes(),
                                            format!("Commit transaction successed:\n\tlog_index: {:?}\n\tcid: {:?}\n\tstatus: {:?}\n\ttable: {:?}\n\tactions_len: {:?}\n\ttime: {:?}\n",
                                                    log_index,
                                                    cid,
                                                    status,
                                                    table.as_str(),
                                                    actions_len,
                                                    time).as_bytes());
                            } else {
                                log_id = logger
                                    .0
                                    .log
                                    .append(LogMethod::PlainAppend,
                                            format!("{:?}", tid).as_bytes(),
                                            format!("Commit transaction failed, transaction not exist:\n\tlog_index: {:?}\n\tcid: {:?}\n\tstatus: {:?}\n\ttable: {:?}\n\tactions_len: {:?}\n",
                                                    log_index,
                                                    cid,
                                                    status,
                                                    table.as_str(),
                                                    actions_len).as_bytes());
                            }
                        },
                        TransactionDebugEvent::CommitConfirm(tid, cid, table, writable, require_persistence) => {
                            if let Some(item) = logger.0.times.get(&tid) {
                                //事务存在
                                let time = item.value().elapsed();
                                log_id = logger
                                    .0
                                    .log
                                    .append(LogMethod::PlainAppend,
                                            format!("{:?}", tid).as_bytes(),
                                            format!("Commit confirm transaction successed:\n\tcid: {:?}\n\ttable: {:?}\n\twritable: {:?}\n\trequire_persistence: {:?}\n\ttime: {:?}\n",
                                                    cid,
                                                    table.as_str(),
                                                    writable,
                                                    require_persistence,
                                                    time).as_bytes());
                            } else {
                                log_id = logger
                                    .0
                                    .log
                                    .append(LogMethod::PlainAppend,
                                            format!("{:?}", tid).as_bytes(),
                                            format!("Commit confirm transaction failed, transaction not exist:\n\tcid: {:?}\n\ttable: {:?}\n\twritable: {:?}\n\trequire_persistence: {:?}\n",
                                                    cid,
                                                    table.as_str(),
                                                    writable,
                                                    require_persistence).as_bytes());
                            }
                        },
                        TransactionDebugEvent::End(tid, cid) => {
                            if let Some((_tid, now)) = logger.0.times.remove(&tid) {
                                //事务存在
                                let time = now.elapsed();
                                log_id = logger
                                    .0
                                    .log
                                    .append(LogMethod::PlainAppend,
                                            format!("{:?}", tid).as_bytes(),
                                            format!("End transaction:\n\tcid: {:?}\n\ttime: {:?}\n",
                                                    cid,
                                                    time).as_bytes());
                            } else {
                                log_id = logger
                                    .0
                                    .log
                                    .append(LogMethod::PlainAppend,
                                            format!("{:?}", tid).as_bytes(),
                                            format!("End transaction failed, transaction not exist:\n\tcid: {:?}\n",
                                                    cid).as_bytes());
                            }
                        },
                    }
                }

                let _ = logger
                    .0
                    .log
                    .commit(log_id,
                            false,
                            false,
                            Some(timeout)).await;
                logger
                    .0
                    .rt
                    .timeout(interval)
                    .await;
            }
        });
    }
}

struct InnerTransactionDebugLogger {
    rt:         MultiTaskRuntime<()>,               //运行时
    sender:     Sender<TransactionDebugEvent>,      //事务事件发送器
    receiver:   Receiver<TransactionDebugEvent>,    //事务事件接收器
    times:      DashMap<Guid, Instant>,             //事务时间表
    log:        LogFile,                            //日志文件
}
