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
    ReportTrInfo,                                           //报告事务信息
    CommitFailed(Atom, Atom, KVDBTableType, Cid, Cid),      //提交已失败
    ConfirmCommited(Atom, Atom, KVDBTableType, Cid, Cid),   //确认已提交
}

impl<Cid: Debug + Clone + Send + PartialEq + Eq + 'static> KVDBEvent<Cid> {
    /// 判断是否是报告事务信息事件
    pub fn is_report_transaction_info(&self) -> bool {
        if let Self::ReportTrInfo = self {
            true
        } else {
            false
        }
    }

    /// 判断是否是提交已失败事件
    pub fn is_commit_failed(&self) -> bool {
        if let Self::CommitFailed(_, _, _, _, _) = self {
            true
        } else {
            false
        }
    }

    /// 判断是否是确认已提交事件
    pub fn is_confirm_commited(&self) -> bool {
        if let Self::ConfirmCommited(_, _, _, _, _) = self {
            true
        } else {
            false
        }
    }
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