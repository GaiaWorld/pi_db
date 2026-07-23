//! `KVTable` 根 WAL 表片段编解码 helper 的合法输入契约。
//!
//! 本 target 不读取旧测试，也不替代真实事务、WAL、repair 和 data-only 恢复专项。它直接使用
//! 生产 `MemoryOrderedTable` 与公开 `KVTable` 默认方法，独立验证固定字节布局、根 TID 后的
//! 非零偏移、多表顺序拼接、最大合法表名、最大 `u16` Key、upsert/delete 区分和最终偏移。
//!
//! 测试输入全部属于当前受信 codec 合法域。空 Value、超长 Key、截断/伪造长度和非法 UTF-8
//! 分别属于 `FIND-DATA-001` / `FIND-CODEC-001`，本 target 不用 panic 结果把这些缺陷固化成
//! 受支持语义。

use pi_atom::Atom;
use pi_db::{
    tables::{mem_ord_table::MemoryOrderedTable, KVTable, TableKV},
    Binary, MAX_TABLE_NAME_BYTES,
};
use pi_store::commit_logger::CommitLogger;

type CodecTable = MemoryOrderedTable<usize, CommitLogger>;

const ROOT_TID_BYTES: usize = 16;
const MAX_KEY_BYTES: usize = u16::MAX as usize;
const LARGE_VALUE_BYTES: usize = 70_000;

#[derive(Debug)]
struct ExpectedAction {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

#[test]
fn test_table_wal_codec_valid_layout_and_roundtrip() {
    let short_name = "a";
    let max_name = "m".repeat(MAX_TABLE_NAME_BYTES);
    let short_table = CodecTable::new(Atom::from(short_name), true);
    let max_table = CodecTable::new(Atom::from(max_name.clone()), true);

    let short_actions = vec![
        ExpectedAction {
            key: vec![0x11],
            value: Some(vec![0x22; LARGE_VALUE_BYTES]),
        },
        ExpectedAction {
            key: vec![0x33; MAX_KEY_BYTES],
            value: None,
        },
    ];
    let max_actions = vec![ExpectedAction {
        key: (0u8..32).collect(),
        value: Some(vec![0x44]),
    }];

    let mut output = vec![0xa5; ROOT_TID_BYTES];
    let short_segment_start = output.len();
    append_segment(&short_table, &short_actions, &mut output);
    let max_segment_start = output.len();
    append_segment(&max_table, &max_actions, &mut output);

    assert_eq!(&output[..ROOT_TID_BYTES], &[0xa5; ROOT_TID_BYTES]);
    assert_segment_bytes(
        &output,
        short_segment_start,
        short_name,
        &short_actions,
        max_segment_start,
    );
    assert_segment_bytes(
        &output,
        max_segment_start,
        &max_name,
        &max_actions,
        output.len(),
    );

    let (decoded_short_name, short_count, short_actions_offset) =
        <CodecTable as KVTable>::get_init_table_prepare_output(
            &output,
            short_segment_start,
        );
    assert_eq!(decoded_short_name.as_str(), short_name);
    assert_eq!(short_count, short_actions.len() as u64);
    let (decoded_short_actions, decoded_max_segment_start) =
        <CodecTable as KVTable>::get_all_key_value_from_table_prepare_output(
            &output,
            &decoded_short_name,
            short_count,
            short_actions_offset,
        );
    assert_eq!(decoded_max_segment_start, max_segment_start);
    assert_actions(
        &decoded_short_actions,
        short_name,
        &short_actions,
    );

    let (decoded_max_name, max_count, max_actions_offset) =
        <CodecTable as KVTable>::get_init_table_prepare_output(
            &output,
            decoded_max_segment_start,
        );
    assert_eq!(decoded_max_name.as_str(), max_name);
    assert_eq!(max_count, max_actions.len() as u64);
    let (decoded_max_actions, final_offset) =
        <CodecTable as KVTable>::get_all_key_value_from_table_prepare_output(
            &output,
            &decoded_max_name,
            max_count,
            max_actions_offset,
        );
    assert_eq!(final_offset, output.len());
    assert_actions(&decoded_max_actions, &max_name, &max_actions);
}

fn append_segment(
    table: &CodecTable,
    actions: &[ExpectedAction],
    output: &mut Vec<u8>,
) {
    table.init_table_prepare_output(output, actions.len() as u64);
    for action in actions {
        let key = Binary::from_slice(&action.key);
        let value = action
            .value
            .as_ref()
            .map(|value| Binary::from_slice(value));
        table.append_key_value_to_table_prepare_output(output, &key, value.as_ref());
    }
}

fn assert_segment_bytes(
    output: &[u8],
    start: usize,
    table_name: &str,
    actions: &[ExpectedAction],
    expected_end: usize,
) {
    let mut offset = start;
    assert_eq!(read_u16(output, offset) as usize, table_name.len());
    offset += 2;
    assert_eq!(&output[offset..offset + table_name.len()], table_name.as_bytes());
    offset += table_name.len();
    assert_eq!(read_u64(output, offset), actions.len() as u64);
    offset += 8;

    for action in actions {
        assert_eq!(read_u16(output, offset) as usize, action.key.len());
        offset += 2;
        assert_eq!(&output[offset..offset + action.key.len()], action.key.as_slice());
        offset += action.key.len();

        let value_len = action.value.as_ref().map_or(0, Vec::len);
        assert_eq!(read_u32(output, offset) as usize, value_len);
        offset += 4;
        if let Some(value) = &action.value {
            assert!(!value.is_empty(), "valid upsert value must not be empty");
            assert_eq!(&output[offset..offset + value.len()], value.as_slice());
            offset += value.len();
        }
    }
    assert_eq!(offset, expected_end);
}

fn assert_actions(
    actual: &[TableKV],
    table_name: &str,
    expected: &[ExpectedAction],
) {
    assert_eq!(actual.len(), expected.len());
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        assert_eq!(actual.table.as_str(), table_name, "table mismatch at {index}");
        assert_eq!(actual.key.as_ref(), expected.key.as_slice(), "key mismatch at {index}");
        match (&actual.value, &expected.value) {
            (Some(actual), Some(expected)) => {
                assert_eq!(actual.as_ref(), expected.as_slice(), "value mismatch at {index}");
                assert!(actual.len() > 0, "decoded upsert value must remain nonempty");
            },
            (None, None) => {}
            (actual, expected) => {
                panic!("action kind mismatch at {index}: actual={actual:?}, expected={expected:?}");
            },
        }
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}
