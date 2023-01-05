
///
/// 创建表选项
///
#[derive(Debug, Clone)]
pub enum CreateTableOptions {
    LogOrdTab(usize, usize, usize), //有序日志表的选项
}