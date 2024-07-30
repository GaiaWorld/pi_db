use std::fmt::Debug;
use std::hint::spin_loop;

use pi_atom::Atom;

use crate::KVDBTableType;

///
/// 创建表选项
///
#[derive(Debug, Clone)]
pub enum CreateTableOptions {
    Empty,                          //空选项
    LogOrdTab(usize, usize, usize), //有序日志表的选项
    BtreeOrdTab(usize, bool),       //有序B树表的选项
}

///
/// 键值对数据库事件
///
#[derive(Debug, Clone)]
pub enum KVDBEvent<Cid: Debug + Clone + Send + PartialEq + Eq + 'static> {
    Statistics(bool),                                       //统计
    CommitFailed(Atom, Atom, KVDBTableType, Cid, Cid),      //提交已失败
    ConfirmCommited(Atom, Atom, KVDBTableType, Cid, Cid),   //确认已提交
}

// 自旋
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