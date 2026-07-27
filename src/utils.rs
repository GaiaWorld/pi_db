use std::fmt::Debug;
use std::hint::spin_loop;

use pi_atom::Atom;

use crate::KVDBTableType;

/// 创建表时传给具体存储引擎的运行参数。
///
/// 该枚举只用于本次表实例的创建/打开，不写入 [`crate::KVTableMeta`]。数据库重启后会按
/// `KVDBManagerBuilder::startup` 的固定默认值重新打开 LogOrdered/Btree 表，而不会恢复首次
/// 创建时的自定义参数；这是当前实现边界，不是持久化配置契约。
///
/// `create_table_with_options` 只严格匹配 LogOrdered 和 Btree 的对应 variant：类型不匹配时
/// 返回 `io::ErrorKind::Other`。Memory 与 LogWrite 当前忽略整个 options 值，传入任意 variant
/// 都不会报错；这不是最终或最佳的配置校验设计。枚举 clone 为 O(1)，本身不持有文件、锁
/// 或运行时资源，也不执行 I/O。
///
/// 当前值对象边界由 `tests/utils_contract.rs` 验证；真实 DDL 路由将在
/// `tests/kv_action_contract.rs` 和后续 DDL 专项中验证。
#[derive(Debug, Clone)]
pub enum CreateTableOptions {
    /// 无专用存储参数。
    ///
    /// [`crate::KVDBTableType::MemOrdTab`] 和 [`crate::KVDBTableType::LogWTab`] 的默认创建路径
    /// 使用该值；用于 LogOrdered/Btree 会被拒绝。
    Empty,
    /// LogOrdered 的 `(日志文件大小上限, 日志块大小上限, 加载缓冲区长度)`，单位均为字节。
    ///
    /// 前两项直接传给 `pi_store::LogFile::open`：当前依赖把不在 `1 MiB..=16 GiB` 的文件
    /// 上限回退为 `16 MiB`，把不在 `32 B..=2 GiB` 的块上限回退为 `8000 B`，不会向
    /// `pi_db` 返回参数错误。第三项作为日志加载读取长度传入；`pi_db` 不做范围校验，过小或
    /// 过大值可能改变启动 I/O 次数和临时内存。默认创建值为 `(512 MiB, 2 MiB, 2 MiB)`。
    LogOrdTab(usize, usize, usize),
    /// Btree 的 `(redb 页面缓存字节数, 是否请求 compact)`。
    ///
    /// 缓存小于 `32 KiB` 时当前回退为 `2 MiB`；默认创建传入 `16 MiB`。`enable_compact`
    /// 会保存到表实例，但当前生产代码没有读取该字段，见 `FIND-COMPACT-001`，因此不能据此
    /// 宣称已经启用压缩。该布尔值也与建表 API 单独传入的 accelerated repair 开关无关。
    BtreeOrdTab(usize, bool),
}

/// 由数据库可选监听器接收的批量通知值。
///
/// 事件通道只在 [`crate::db::KVDBManagerBuilder::startup_with_listener`] 收到 `Some(listener)`
/// 时创建；通道无界，监听器在一个 runtime 任务中串行调用。回调获得可变 `Vec` 后必须在
/// 返回前 `drain`/`clear` 已处理事件：框架不会自动清空，保留元素会导致同一批被重复调用，
/// 满批时还可能形成持续循环。回调是同步函数，阻塞会占用 runtime worker，panic 会终止
/// 监听任务；跨表 collector 并发发送时没有全局事务顺序保证。
///
/// 当前只有 Meta、LogOrdered 和 Btree collector 发送提交相关事件。表数据持久化 I/O 失败
/// 在调用确认器之前返回，因此不会产生 [`KVDBEvent::CommitFailed`]；Memory 和 LogWrite
/// 确认路径也不发送这些事件。`ConfirmCommited` 只表示子表调用确认器时同步返回 `Ok`，最后
/// 一个信号至多已请求投递根 WAL 异步确认任务，不证明该 I/O 已完成或 WAL 已改名 `.bak`。
///
/// Meta 和 LogOrdered 当前都错误/不足地携带 `BtreeOrdTab` 标签；该现状见
/// `FIND-EVENT-001`，不是准确逻辑表类型，也不是最终或最佳事件模型。事件是观测信息，不能
/// 替代事务 API 返回值、WAL 状态或恢复检查。clone 的 Atom/Guid/枚举成本为 O(1)，事件
/// 值本身无内部可变性、无 I/O，可在 `Cid: Send` 时跨线程移动。
///
/// 字段和值对象分类由 `tests/utils_contract.rs` 验证；正常批处理和真实 collector payload 由
/// `tests/manager_listener_contract.rs` 验证，生产投递链和缺口见 `CONTRACT-EVENT-001`。
#[derive(Debug, Clone)]
pub enum KVDBEvent<Cid: Debug + Clone + Send + PartialEq + Eq + 'static> {
    /// 由 `KVDBManager::report_transaction_info` 主动发送的无 payload 请求标记。
    ///
    /// 框架不自动生成报告内容；监听器收到后自行读取传入的 manager/2PC manager。
    ReportTrInfo,
    /// 表数据已持久化，但调用子表成功确认器时同步返回错误。
    ///
    /// 字段依次是：事务 source、表名、生产者填入的表类型标签、事务 UID、提交 UID。
    /// 这不表示表数据 I/O 失败；内置合法确认协议通常只传 `Ok(())`，当前主要错误来源是 UID
    /// 或低层确认器前置条件不匹配。
    CommitFailed(Atom, Atom, KVDBTableType, Cid, Cid),
    /// 调用子表成功确认器时同步返回 `Ok(())`。
    ///
    /// 字段顺序与 [`KVDBEvent::CommitFailed`] 相同。名称中的 `Commited` 是既有公开拼写；事件
    /// 不证明根 WAL 的异步确认或 `.bak` 轮换已经完成。
    ConfirmCommited(Atom, Atom, KVDBTableType, Cid, Cid),
}

impl<Cid: Debug + Clone + Send + PartialEq + Eq + 'static> KVDBEvent<Cid> {
    /// 判断是否为 [`KVDBEvent::ReportTrInfo`]。
    ///
    /// O(1) 纯读取，幂等、无分配、无锁且不会 panic；其它两个 variant 均返回 `false`。
    pub fn is_report_transaction_info(&self) -> bool {
        if let Self::ReportTrInfo = self {
            true
        } else {
            false
        }
    }

    /// 判断是否为 [`KVDBEvent::CommitFailed`]。
    ///
    /// O(1) 纯读取，不检查或解释五个 payload 字段。
    pub fn is_commit_failed(&self) -> bool {
        if let Self::CommitFailed(_, _, _, _, _) = self {
            true
        } else {
            false
        }
    }

    /// 判断是否为 [`KVDBEvent::ConfirmCommited`]。
    ///
    /// O(1) 纯读取；返回 `true` 不扩大 variant 自身的异步确认边界。
    pub fn is_confirm_commited(&self) -> bool {
        if let Self::ConfirmCommited(_, _, _, _, _) = self {
            true
        } else {
            false
        }
    }
}

/// 执行有上限的指数自旋，并返回下一次 backoff 状态。
///
/// 输入先限制到 `1..=10`，随后执行 `2^state` 次 [`spin_loop`]，返回 `state + 1`。因此返回
/// 范围是 `2..=11`；将 11 再传入会继续按上限 10 自旋并返回 11。函数不分配、不持锁、
/// 不执行 I/O 且不会 panic，但会同步占用当前 OS 线程，不能在异步 worker 上当作等待机制。
/// 当前 crate 没有生产调用点，它是保留的内部 backoff helper。
#[inline]
pub(crate) fn spin(mut len: u32) -> u32 {
    if len < 1 {
        len = 1;
    } else if len > 10 {
        len = 10;
    }

    for _ in 0..(1 << len) {
        spin_loop()
    }

    len + 1
}

#[cfg(test)]
mod tests {
    use super::spin;

    /// 验证内部 backoff 的最小值、正常推进、最大值和饱和行为。
    ///
    /// 该测试只保护纯 CPU helper，不把它提升为公开 API，也不在异步 runtime 中执行。
    #[test]
    fn test_spin_clamps_and_saturates_backoff_state() {
        assert_eq!(spin(0), 2);
        assert_eq!(spin(1), 2);
        assert_eq!(spin(2), 3);
        assert_eq!(spin(10), 11);
        assert_eq!(spin(11), 11);
        assert_eq!(spin(u32::MAX), 11);
    }
}
