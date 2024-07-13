
///
/// 创建表选项
///
#[derive(Debug, Clone)]
pub enum CreateTableOptions {
    Empty,                          //空选项
    LogOrdTab(usize, usize, usize), //有序日志表的选项
    BtreeOrdTab(usize, bool),       //有序B树表的选项
}