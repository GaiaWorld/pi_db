//! 录制事务的只读根 WAL 索引与按位置读取。
//!
//! 本模块只读取归档文件，不构造数据库或 CommitLogger，不执行 repair、confirm 或日志整理。
//! 成功 tid 由录制提交回执提供；文件或记录缺失只代表无法覆盖，不反向改变原提交结论。
//! 文件操作同步执行，应由离线工具或文件读取线程调用，不能放入数据库提交临界区。

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};

use pi_atom::Atom;

use crate::MAX_TABLE_NAME_BYTES;

const BLOCK_TRAILER_BYTES: usize = 16;

/// 本模块支持的现有根 WAL 布局标识；由索引保存，不向原 WAL 增加版本字段。
pub const RECORDED_WAL_LAYOUT: u16 = 1;

/// 一组归档副本的身份与根目录。实例身份由上层批次清单分配，不使用运行时 db_id。
#[derive(Debug, Clone)]
pub struct RecordedLogArchive {
    pub instance_id: [u8; 16],
    pub root: PathBuf,
}

/// 读取预算，防止损坏长度触发无界分配；不是数据库在线写入上限。
#[derive(Debug, Clone, Copy)]
pub struct RecordedLogLimits {
    pub max_block_bytes: usize,
    pub max_actions: usize,
    pub max_value_bytes: usize,
}

impl Default for RecordedLogLimits {
    fn default() -> Self {
        Self {
            max_block_bytes: 64 * 1024 * 1024,
            max_actions: 1_000_000,
            max_value_bytes: 64 * 1024 * 1024,
        }
    }
}

/// 一笔成功录制事务的物理位置。偏移为完整块的起始位置，长度包含块尾 16 字节。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedTransactionLocation {
    pub archive_instance_id: [u8; 16],
    pub layout: u16,
    pub tid: u128,
    pub cid: Option<u128>,
    /// 相对于归档根目录的路径；不允许绝对路径或父目录组件。
    pub file: PathBuf,
    pub block_offset: u64,
    pub block_length: u64,
}

/// WAL 的原始表动作；None 表示删除，不表示空值。
///
/// 包含日志中存在的元数据表动作，不转换为调试字符串；调用方须区分 DDL 与普通 KV。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedWrite {
    /// 同一表片段内共享名称，避免每个 Key 重复复制长表名。
    pub table: Atom,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

/// 仅包含目标事务写入根 WAL 的动作，不推断未记录的非持久化数据或被删除旧值。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedTransaction {
    pub tid: u128,
    pub cid: u128,
    pub writes: Vec<RecordedWrite>,
}

/// 可作为不覆盖指标 reason 的有界分类；原始 ID 和路径应放日志而非指标标签。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordedReadErrorKind {
    InvalidInput,
    FileMissing,
    Io,
    LimitExceeded,
    InvalidData,
    ChecksumMismatch,
    IdentityMismatch,
    NotFound,
    Ambiguous,
    UnsupportedLayout,
}

impl RecordedReadErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::FileMissing => "file_missing",
            Self::Io => "io_error",
            Self::LimitExceeded => "limit_exceeded",
            Self::InvalidData => "invalid_data",
            Self::ChecksumMismatch => "checksum_mismatch",
            Self::IdentityMismatch => "identity_mismatch",
            Self::NotFound => "not_found",
            Self::Ambiguous => "ambiguous",
            Self::UnsupportedLayout => "unsupported_layout",
        }
    }
}

/// 读取失败代表无法提供覆盖材料，不代表录制事务未提交。
#[derive(Debug)]
pub struct RecordedReadError {
    pub kind: RecordedReadErrorKind,
    pub message: String,
    pub source: Option<io::Error>,
}

impl fmt::Display for RecordedReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.kind.as_str(), self.message)
    }
}

impl std::error::Error for RecordedReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|error| error as &(dyn std::error::Error + 'static))
    }
}

type ReadResult<T> = Result<T, RecordedReadError>;

/// 文件级索引问题；其余完整块或文件已取得的位置仍可使用，不自动修复归档。
#[derive(Debug)]
pub struct RecordedIndexIssue {
    pub file: PathBuf,
    pub error: RecordedReadError,
}

/// 按成功 tid 构建的归档实例内索引；缺失或歧义项不会借用其他事务的位置。
#[derive(Debug)]
pub struct RecordedLogIndex {
    pub archive_instance_id: [u8; 16],
    pub transactions: BTreeMap<u128, RecordedTransactionLocation>,
    pub missing_tids: Vec<u128>,
    pub issues: Vec<RecordedIndexIssue>,
}

/// 离线扫描清单中的归档根 WAL，建立成功 tid 到文件/完整块位置的索引。
///
/// 只索引调用方提供的成功 tid，不能把日志存在推断为业务成功。文件/块错误记入 issues，
/// 不妨碍其他文件；同一 tid 出现多个候选时移除该项并报告 Ambiguous。文件顺序无要求。
/// 调用方应保留这些文件的稳定副本；本函数不创建文件、运行时或数据库。
pub fn build_recorded_log_index(
    archive: &RecordedLogArchive,
    files: &[PathBuf],
    successful_tids: &[u128],
    limits: RecordedLogLimits,
) -> ReadResult<RecordedLogIndex> {
    validate_limits(limits)?;
    let wanted: BTreeSet<u128> = successful_tids.iter().copied().collect();
    let mut index = RecordedLogIndex {
        archive_instance_id: archive.instance_id,
        transactions: BTreeMap::new(),
        missing_tids: Vec::new(),
        issues: Vec::new(),
    };
    let mut ambiguous = BTreeSet::new();
    let mut seen_files = BTreeSet::new();
    for relative in files {
        let result = (|| {
            let path = archive_path(archive, relative)?;
            if !seen_files.insert(path.clone()) {
                return Err(error(
                    RecordedReadErrorKind::InvalidInput,
                    "archive file was listed more than once",
                ));
            }
            let mut file = File::open(&path).map_err(io_error)?;
            let mut end = file.metadata().map_err(io_error)?.len();
            while end != 0 {
                if end < BLOCK_TRAILER_BYTES as u64 {
                    return Err(error(
                        RecordedReadErrorKind::InvalidData,
                        "short WAL trailer",
                    ));
                }
                let mut trailer = [0; BLOCK_TRAILER_BYTES];
                file.seek(SeekFrom::Start(end - BLOCK_TRAILER_BYTES as u64))
                    .map_err(io_error)?;
                file.read_exact(&mut trailer).map_err(io_error)?;
                let payload_len = u32::from_le_bytes(trailer[12..16].try_into().map_err(|_| {
                    error(RecordedReadErrorKind::InvalidData, "invalid WAL trailer")
                })?) as u64;
                let length = payload_len + BLOCK_TRAILER_BYTES as u64;
                let start = end.checked_sub(length).ok_or_else(|| {
                    error(
                        RecordedReadErrorKind::InvalidData,
                        "WAL block extends before file",
                    )
                })?;
                let block = read_block(&mut file, start, length, limits)?;
                visit_records(&block, |cid, payload| {
                    let tid = payload_tid(payload)?;
                    if !wanted.contains(&tid) || ambiguous.contains(&tid) {
                        return Ok(());
                    }
                    let location = RecordedTransactionLocation {
                        archive_instance_id: archive.instance_id,
                        layout: RECORDED_WAL_LAYOUT,
                        tid,
                        cid: Some(cid),
                        file: relative.clone(),
                        block_offset: start,
                        block_length: length,
                    };
                    if index.transactions.remove(&tid).is_some() {
                        ambiguous.insert(tid);
                        index.issues.push(RecordedIndexIssue {
                            file: relative.clone(),
                            error: error(
                                RecordedReadErrorKind::Ambiguous,
                                format!("multiple WAL records for successful tid {tid}"),
                            ),
                        });
                    } else {
                        index.transactions.insert(tid, location);
                    }
                    Ok(())
                })?;
                end = start;
            }
            Ok(())
        })();
        if let Err(error) = result {
            index.issues.push(RecordedIndexIssue {
                file: relative.clone(),
                error,
            });
        }
    }
    index.missing_tids = wanted
        .into_iter()
        .filter(|tid| !index.transactions.contains_key(tid))
        .collect();
    Ok(index)
}

/// Stream candidate locations from one stable WAL file, retaining at most one bounded block.
/// A WAL record is not proof of business success: the caller must join against recorded
/// successful tid/cid receipts. Duplicate detection also belongs to that disk-backed join.
/// Does not construct/repair a database or change the original in-memory index API.
pub fn visit_recorded_log_locations(
    archive: &RecordedLogArchive,
    relative: &Path,
    limits: RecordedLogLimits,
    mut visitor: impl FnMut(RecordedTransactionLocation) -> ReadResult<()>,
) -> ReadResult<u64> {
    validate_limits(limits)?;
    let path = archive_path(archive, relative)?;
    let mut file = File::open(&path).map_err(io_error)?;
    let original_len = file.metadata().map_err(io_error)?.len();
    let mut end = original_len;
    let mut count = 0u64;
    while end != 0 {
        if end < BLOCK_TRAILER_BYTES as u64 {
            return Err(error(RecordedReadErrorKind::InvalidData, "short WAL trailer"));
        }
        let mut trailer = [0; BLOCK_TRAILER_BYTES];
        file.seek(SeekFrom::Start(end - BLOCK_TRAILER_BYTES as u64)).map_err(io_error)?;
        file.read_exact(&mut trailer).map_err(io_error)?;
        let payload_len = u32::from_le_bytes(trailer[12..16].try_into().map_err(|_| {
            error(RecordedReadErrorKind::InvalidData, "invalid WAL trailer")
        })?) as u64;
        let length = payload_len + BLOCK_TRAILER_BYTES as u64;
        let start = end.checked_sub(length).ok_or_else(|| {
            error(RecordedReadErrorKind::InvalidData, "WAL block extends before file")
        })?;
        let block = read_block(&mut file, start, length, limits)?;
        visit_records(&block, |cid, payload| {
            let tid = payload_tid(payload)?;
            visitor(RecordedTransactionLocation {
                archive_instance_id: archive.instance_id,
                layout: RECORDED_WAL_LAYOUT,
                tid,
                cid: Some(cid),
                file: relative.into(),
                block_offset: start,
                block_length: length,
            })?;
            count = count.checked_add(1).ok_or_else(|| {
                error(RecordedReadErrorKind::LimitExceeded, "WAL record counter exhausted")
            })?;
            Ok(())
        })?;
        end = start;
    }
    if file.metadata().map_err(io_error)?.len() != original_len {
        return Err(error(RecordedReadErrorKind::InvalidData, "WAL source changed while scanning"));
    }
    Ok(count)
}

/// 静态只读入口：按索引位置取得目标成功事务的 WAL 写集。
///
/// 文件缺失、读取/查询/校验失败均返回结构化错误；调用方应选择不覆盖并记录一次指标，
/// 保留本次原始写集。返回空 writes 只表示目标日志的合法空动作，绝不用于掩盖读取失败。
pub fn load_recorded_transaction(
    archive: &RecordedLogArchive,
    location: &RecordedTransactionLocation,
    limits: RecordedLogLimits,
) -> ReadResult<RecordedTransaction> {
    validate_limits(limits)?;
    if location.archive_instance_id != archive.instance_id {
        return Err(error(
            RecordedReadErrorKind::IdentityMismatch,
            "archive instance differs",
        ));
    }
    if location.layout != RECORDED_WAL_LAYOUT {
        return Err(error(
            RecordedReadErrorKind::UnsupportedLayout,
            "unknown WAL layout",
        ));
    }
    let path = archive_path(archive, &location.file)?;
    let mut file = File::open(path).map_err(io_error)?;
    let block = read_block(
        &mut file,
        location.block_offset,
        location.block_length,
        limits,
    )?;
    let mut found = None;
    visit_records(&block, |cid, payload| {
        let tid = payload_tid(payload)?;
        if let Some(expected_cid) = location.cid {
            if cid != expected_cid {
                return Ok(());
            }
            if tid != location.tid {
                return Err(error(
                    RecordedReadErrorKind::IdentityMismatch,
                    "WAL tid differs",
                ));
            }
        } else if tid != location.tid {
            return Ok(());
        }
        if found.is_some() {
            return Err(error(
                RecordedReadErrorKind::Ambiguous,
                "multiple matching WAL records",
            ));
        }
        found = Some(decode_transaction(cid, payload, limits)?);
        Ok(())
    })?;
    found.ok_or_else(|| {
        error(
            RecordedReadErrorKind::NotFound,
            "transaction not in indexed block",
        )
    })
}

fn archive_path(archive: &RecordedLogArchive, relative: &Path) -> ReadResult<PathBuf> {
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(error(
            RecordedReadErrorKind::InvalidInput,
            "expected an archive-relative path",
        ));
    }
    let root = archive.root.canonicalize().map_err(io_error)?;
    let path = root.join(relative).canonicalize().map_err(io_error)?;
    if !path.starts_with(&root) {
        return Err(error(
            RecordedReadErrorKind::InvalidInput,
            "file is outside archive",
        ));
    }
    Ok(path)
}

fn validate_limits(limits: RecordedLogLimits) -> ReadResult<()> {
    if limits.max_block_bytes < BLOCK_TRAILER_BYTES
        || limits.max_actions == 0
        || limits.max_value_bytes == 0
    {
        return Err(error(
            RecordedReadErrorKind::InvalidInput,
            "invalid read limits",
        ));
    }
    Ok(())
}

fn read_block(
    file: &mut File,
    offset: u64,
    length: u64,
    limits: RecordedLogLimits,
) -> ReadResult<Vec<u8>> {
    let length = usize::try_from(length).map_err(|_| {
        error(
            RecordedReadErrorKind::LimitExceeded,
            "block length does not fit usize",
        )
    })?;
    if length < BLOCK_TRAILER_BYTES || length > limits.max_block_bytes {
        return Err(error(
            RecordedReadErrorKind::LimitExceeded,
            "block length outside read budget",
        ));
    }
    let end = offset
        .checked_add(length as u64)
        .ok_or_else(|| error(RecordedReadErrorKind::InvalidData, "block range overflow"))?;
    if end > file.metadata().map_err(io_error)?.len() {
        return Err(error(RecordedReadErrorKind::InvalidData, "short WAL block"));
    }
    let mut block = Vec::new();
    block.try_reserve_exact(length).map_err(|_| {
        error(
            RecordedReadErrorKind::LimitExceeded,
            "cannot allocate WAL block",
        )
    })?;
    block.resize(length, 0);
    file.seek(SeekFrom::Start(offset)).map_err(io_error)?;
    file.read_exact(&mut block).map_err(io_error)?;
    validate_block(&block)?;
    Ok(block)
}

fn validate_block(block: &[u8]) -> ReadResult<()> {
    let trailer_start = block
        .len()
        .checked_sub(BLOCK_TRAILER_BYTES)
        .ok_or_else(|| error(RecordedReadErrorKind::InvalidData, "short WAL trailer"))?;
    let mut trailer = Cursor::new(&block[trailer_start..]);
    trailer.take(8)?;
    let expected_crc = trailer.u32()?;
    let payload_length = trailer.u32()? as usize;
    if payload_length != trailer_start {
        return Err(error(
            RecordedReadErrorKind::InvalidData,
            "indexed block length differs",
        ));
    }
    // pi_store 的 CRC32/IEEE 覆盖 payload + 8 字节时间戳，不包含 CRC 与负载长度。
    if crc32(&block[..trailer_start + 8]) != expected_crc {
        return Err(error(
            RecordedReadErrorKind::ChecksumMismatch,
            "WAL checksum differs",
        ));
    }
    Ok(())
}

fn visit_records(
    block: &[u8],
    mut visit: impl FnMut(u128, &[u8]) -> ReadResult<()>,
) -> ReadResult<()> {
    let payload_end = block
        .len()
        .checked_sub(BLOCK_TRAILER_BYTES)
        .ok_or_else(|| error(RecordedReadErrorKind::InvalidData, "short WAL block"))?;
    let mut cursor = Cursor::new(&block[..payload_end]);
    while cursor.remaining() != 0 {
        let tag = cursor.take(1)?[0];
        let key_len = cursor.u16()? as usize;
        let key = cursor.take(key_len)?;
        if key.len() != 16 {
            return Err(error(
                RecordedReadErrorKind::InvalidData,
                "root WAL cid must be 16 bytes",
            ));
        }
        let cid = u128::from_le_bytes(
            key.try_into()
                .map_err(|_| error(RecordedReadErrorKind::InvalidData, "invalid cid"))?,
        );
        match tag {
            0 => (), // 根日志的删除/整理标记没有事务 payload。
            1 => {
                let value_len = cursor.u32()? as usize;
                visit(cid, cursor.take(value_len)?)?;
            }
            _ => {
                return Err(error(
                    RecordedReadErrorKind::InvalidData,
                    "unknown WAL record tag",
                ))
            }
        }
    }
    Ok(())
}

fn payload_tid(payload: &[u8]) -> ReadResult<u128> {
    Cursor::new(payload).u128()
}

fn decode_transaction(
    cid: u128,
    payload: &[u8],
    limits: RecordedLogLimits,
) -> ReadResult<RecordedTransaction> {
    let mut cursor = Cursor::new(payload);
    let tid = cursor.u128()?;
    let mut writes = Vec::new();
    while cursor.remaining() != 0 {
        let table_len = cursor.u16()? as usize;
        if table_len == 0 || table_len > MAX_TABLE_NAME_BYTES {
            return Err(error(
                RecordedReadErrorKind::InvalidData,
                "invalid table name length",
            ));
        }
        let table = std::str::from_utf8(cursor.take(table_len)?).map_err(|_| {
            error(
                RecordedReadErrorKind::InvalidData,
                "invalid table name UTF-8",
            )
        })?;
        let table = Atom::from(table);
        let count = usize::try_from(cursor.u64()?).map_err(|_| {
            error(
                RecordedReadErrorKind::LimitExceeded,
                "action count does not fit usize",
            )
        })?;
        if count > limits.max_actions.saturating_sub(writes.len()) {
            return Err(error(
                RecordedReadErrorKind::LimitExceeded,
                "too many WAL actions",
            ));
        }
        for _ in 0..count {
            let key_len = cursor.u16()? as usize;
            if key_len == 0 {
                return Err(error(RecordedReadErrorKind::InvalidData, "empty WAL key"));
            }
            let key = cursor.take(key_len)?.to_vec();
            let value_len = cursor.u32()? as usize;
            if value_len > limits.max_value_bytes {
                return Err(error(
                    RecordedReadErrorKind::LimitExceeded,
                    "WAL value exceeds budget",
                ));
            }
            let value = if value_len == 0 {
                None
            } else {
                Some(cursor.take(value_len)?.to_vec())
            };
            writes.push(RecordedWrite {
                table: table.clone(),
                key,
                value,
            });
        }
    }
    Ok(RecordedTransaction { tid, cid, writes })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn take(&mut self, length: usize) -> ReadResult<&'a [u8]> {
        let end = self.position.checked_add(length).ok_or_else(|| {
            error(
                RecordedReadErrorKind::InvalidData,
                "WAL field length overflow",
            )
        })?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| error(RecordedReadErrorKind::InvalidData, "truncated WAL field"))?;
        self.position = end;
        Ok(bytes)
    }

    fn u16(&mut self) -> ReadResult<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().map_err(
            |_| error(RecordedReadErrorKind::InvalidData, "invalid u16"),
        )?))
    }

    fn u32(&mut self) -> ReadResult<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().map_err(
            |_| error(RecordedReadErrorKind::InvalidData, "invalid u32"),
        )?))
    }

    fn u64(&mut self) -> ReadResult<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().map_err(
            |_| error(RecordedReadErrorKind::InvalidData, "invalid u64"),
        )?))
    }

    fn u128(&mut self) -> ReadResult<u128> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().map_err(
            |_| error(RecordedReadErrorKind::InvalidData, "invalid u128"),
        )?))
    }
}

fn error(kind: RecordedReadErrorKind, message: impl Into<String>) -> RecordedReadError {
    RecordedReadError {
        kind,
        message: message.into(),
        source: None,
    }
}

fn io_error(source: io::Error) -> RecordedReadError {
    let kind = if source.kind() == io::ErrorKind::NotFound {
        RecordedReadErrorKind::FileMissing
    } else {
        RecordedReadErrorKind::Io
    };
    RecordedReadError {
        kind,
        message: source.to_string(),
        source: Some(source),
    }
}

const fn crc32_table() -> [u32; 256] {
    let mut table = [0; 256];
    let mut index = 0;
    while index < 256 {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            value = if value & 1 == 0 {
                value >> 1
            } else {
                (value >> 1) ^ 0xedb8_8320
            };
            bit += 1;
        }
        table[index] = value;
        index += 1;
    }
    table
}

fn crc32(bytes: &[u8]) -> u32 {
    const TABLE: [u32; 256] = crc32_table();
    let mut crc = !0u32;
    for byte in bytes {
        crc = TABLE[((crc ^ u32::from(*byte)) & 0xff) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_store::log_store::log_file::{LogBlock, LogMethod};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct ArchiveFixture(RecordedLogArchive);

    impl ArchiveFixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(1);
            let root = std::env::temp_dir().join(format!(
                "pi-db-recorded-log-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&root).unwrap();
            Self(RecordedLogArchive {
                instance_id: [7; 16],
                root,
            })
        }

        fn write(&self, name: &str, bytes: &[u8]) -> PathBuf {
            std::fs::write(self.0.root.join(name), bytes).unwrap();
            PathBuf::from(name)
        }
    }

    impl Drop for ArchiveFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0.root);
        }
    }

    fn transaction_payload(tid: u128) -> Vec<u8> {
        let mut payload = tid.to_le_bytes().to_vec();
        payload.extend_from_slice(&5u16.to_le_bytes());
        payload.extend_from_slice(b"roles");
        payload.extend_from_slice(&2u64.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.push(1);
        payload.extend_from_slice(&3u32.to_le_bytes());
        payload.extend_from_slice(&[9, 8, 7]);
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.push(2);
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload
    }

    fn physical_block(entries: &[(u128, Vec<u8>)]) -> Vec<u8> {
        // 使用现有 pi_store 编码器，验证新读取器兼容原布局和 CRC，而非自编自解。
        let mut block = LogBlock::new(4096);
        for (cid, payload) in entries {
            block.append(LogMethod::PlainAppend, &cid.to_le_bytes(), payload);
        }
        block.into()
    }

    #[test]
    fn streaming_locations_match_original_index_and_stop_on_callback_failure() {
        let fixture = ArchiveFixture::new();
        let first = physical_block(&[(31, transaction_payload(11)), (32, transaction_payload(12))]);
        let second = physical_block(&[(33, transaction_payload(13))]);
        let mut bytes = first;
        bytes.extend(second);
        let file = fixture.write("stream.wal", &bytes);
        let limits = RecordedLogLimits::default();
        let expected = build_recorded_log_index(&fixture.0, &[file.clone()], &[11, 12, 13], limits).unwrap();
        let mut streamed = BTreeMap::new();
        let count = visit_recorded_log_locations(&fixture.0, &file, limits, |location| {
            streamed.insert(location.tid, location);
            Ok(())
        }).unwrap();
        assert_eq!(count, 3);
        assert_eq!(streamed, expected.transactions);
        let mut calls = 0;
        let failure = visit_recorded_log_locations(&fixture.0, &file, limits, |_| {
            calls += 1;
            Err(error(RecordedReadErrorKind::Io, "caller stopped"))
        }).unwrap_err();
        assert_eq!(calls, 1);
        assert_eq!(failure.kind, RecordedReadErrorKind::Io);
    }

    #[test]
    fn indexes_only_successful_tids_and_reads_one_transaction_from_shared_block() {
        let fixture = ArchiveFixture::new();
        let block = physical_block(&[(31, transaction_payload(11)), (32, transaction_payload(12))]);
        let file = fixture.write("root.wal", &block);
        let limits = RecordedLogLimits::default();
        let index = build_recorded_log_index(&fixture.0, &[file], &[12, 99], limits).unwrap();
        assert!(index.issues.is_empty());
        assert_eq!(index.transactions.len(), 1);
        assert_eq!(index.missing_tids, vec![99]);
        let transaction =
            load_recorded_transaction(&fixture.0, &index.transactions[&12], limits).unwrap();
        assert_eq!((transaction.tid, transaction.cid), (12, 32));
        assert_eq!(transaction.writes.len(), 2);
        assert_eq!(transaction.writes[0].table.as_str(), "roles");
        assert_eq!(transaction.writes[0].value.as_deref(), Some(&[9, 8, 7][..]));
        assert_eq!(transaction.writes[1].key, vec![2]);
        assert_eq!(transaction.writes[1].value, None);
        assert_eq!(
            std::fs::read(fixture.0.root.join("root.wal")).unwrap(),
            block
        );
    }

    #[test]
    fn duplicate_tid_in_different_files_is_unavailable_instead_of_arbitrarily_chosen() {
        let fixture = ArchiveFixture::new();
        let one = fixture.write("one.wal", &physical_block(&[(31, transaction_payload(11))]));
        let two = fixture.write("two.wal", &physical_block(&[(32, transaction_payload(11))]));
        let index =
            build_recorded_log_index(&fixture.0, &[one, two], &[11], RecordedLogLimits::default())
                .unwrap();
        assert!(index.transactions.is_empty());
        assert_eq!(index.missing_tids, vec![11]);
        assert_eq!(index.issues[0].error.kind, RecordedReadErrorKind::Ambiguous);
    }

    #[test]
    fn missing_file_does_not_prevent_other_successful_transactions_from_being_indexed() {
        let fixture = ArchiveFixture::new();
        let file = fixture.write(
            "root.wal",
            &physical_block(&[(31, transaction_payload(11))]),
        );
        let index = build_recorded_log_index(
            &fixture.0,
            &[PathBuf::from("missing.wal"), file],
            &[11],
            RecordedLogLimits::default(),
        )
        .unwrap();
        assert!(index.transactions.contains_key(&11));
        assert_eq!(
            index.issues[0].error.kind,
            RecordedReadErrorKind::FileMissing
        );
    }

    #[test]
    fn checksum_failure_and_wrong_tid_are_errors_not_empty_writes() {
        let fixture = ArchiveFixture::new();
        let block = physical_block(&[(31, transaction_payload(11))]);
        let file = fixture.write("root.wal", &block);
        let limits = RecordedLogLimits::default();
        let index = build_recorded_log_index(&fixture.0, &[file], &[11], limits).unwrap();
        let location = index.transactions[&11].clone();
        let mut wrong = location.clone();
        wrong.tid = 12;
        assert_eq!(
            load_recorded_transaction(&fixture.0, &wrong, limits)
                .unwrap_err()
                .kind,
            RecordedReadErrorKind::IdentityMismatch
        );
        let mut damaged = block;
        damaged[0] ^= 1;
        fixture.write("root.wal", &damaged);
        assert_eq!(
            load_recorded_transaction(&fixture.0, &location, limits)
                .unwrap_err()
                .kind,
            RecordedReadErrorKind::ChecksumMismatch
        );
    }

    #[test]
    fn malformed_inner_lengths_return_errors_even_when_outer_crc_is_valid() {
        let limits = RecordedLogLimits::default();
        let mut payload = transaction_payload(11);
        payload.truncate(payload.len() - 1);
        let block = physical_block(&[(31, payload)]);
        validate_block(&block).unwrap();
        let result = visit_records(&block, |cid, payload| {
            decode_transaction(cid, payload, limits).map(|_| ())
        });
        assert_eq!(result.unwrap_err().kind, RecordedReadErrorKind::InvalidData);
    }

    #[test]
    fn crc_uses_ieee_polynomial_and_empty_transaction_is_distinct_from_missing() {
        assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
        let transaction =
            decode_transaction(31, &11u128.to_le_bytes(), RecordedLogLimits::default()).unwrap();
        assert!(transaction.writes.is_empty());
        assert_eq!((transaction.tid, transaction.cid), (11, 31));
        assert_eq!(
            payload_tid(&[]).unwrap_err().kind,
            RecordedReadErrorKind::InvalidData
        );
    }
}
