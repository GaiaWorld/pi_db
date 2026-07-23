#![feature(test)]
//! 根 WAL 表片段合法编解码的纯 CPU/分配基准。
//!
//! 本 target 使用生产 `KVTable` 默认 helper，但不启动 runtime、事务管理器、文件系统或 WAL
//! logger，因此只衡量 buffer 分配、字节复制和 decode 时 `TableKV/Binary` 构造成本。它不能
//! 代表完整 prepare、WAL append/flush、commit、confirm 或 repair 延迟。
//!
//! 每轮编码都从 16 字节根 TID 前缀开始；256 项场景每 4 项包含 3 个 upsert 和 1 个 delete，
//! 1 项场景只包含一个 upsert。
//! Key/Value 是 codec 合法的非空 bytes，但不进入有序表比较器，不能外推为某种 BON 类型的
//! 业务负载。所有样本都检查精确长度、动作数和最终 offset，避免测到截断或失败快路。

extern crate test;

use pi_atom::Atom;
use pi_db::{
    tables::{mem_ord_table::MemoryOrderedTable, KVTable},
    Binary,
};
use pi_store::commit_logger::CommitLogger;
use test::{black_box, Bencher};

type CodecTable = MemoryOrderedTable<usize, CommitLogger>;

const ROOT_TID_BYTES: usize = 16;

#[bench]
fn bench_table_wal_codec_encode_1x16x64(b: &mut Bencher) {
    let fixture = CodecFixture::new(1, 16, 64);
    fixture.verify();
    b.iter(|| black_box(fixture.encode_once()));
}

#[bench]
fn bench_table_wal_codec_decode_1x16x64(b: &mut Bencher) {
    let fixture = CodecFixture::new(1, 16, 64);
    fixture.verify();
    b.iter(|| black_box(fixture.decode_once()));
}

#[bench]
fn bench_table_wal_codec_encode_256x32x256(b: &mut Bencher) {
    let fixture = CodecFixture::new(256, 32, 256);
    fixture.verify();
    b.iter(|| black_box(fixture.encode_once()));
}

#[bench]
fn bench_table_wal_codec_decode_256x32x256(b: &mut Bencher) {
    let fixture = CodecFixture::new(256, 32, 256);
    fixture.verify();
    b.iter(|| black_box(fixture.decode_once()));
}

struct CodecFixture {
    table: CodecTable,
    table_name: Atom,
    actions: Vec<(Binary, Option<Binary>)>,
    encoded: Vec<u8>,
    expected_len: usize,
}

impl CodecFixture {
    fn new(action_count: usize, key_bytes: usize, value_bytes: usize) -> Self {
        assert!(action_count > 0);
        assert!(key_bytes > 0 && key_bytes <= u16::MAX as usize);
        assert!(value_bytes > 0 && value_bytes <= u32::MAX as usize);

        let table_name = Atom::from("bench_table_wal_codec");
        let table = CodecTable::new(table_name.clone(), true);
        let actions = (0..action_count)
            .map(|index| {
                let key = Binary::new(vec![(index % 251) as u8; key_bytes]);
                let value = if index % 4 == 3 {
                    None
                } else {
                    Some(Binary::new(vec![(index % 239) as u8; value_bytes]))
                };
                (key, value)
            })
            .collect::<Vec<_>>();
        let expected_len = encoded_len(table_name.as_str().len(), &actions);
        let mut fixture = Self {
            table,
            table_name,
            actions,
            encoded: Vec::new(),
            expected_len,
        };
        fixture.encoded = fixture.encode_once();
        fixture
    }

    fn encode_once(&self) -> Vec<u8> {
        let mut output = vec![0x5a; ROOT_TID_BYTES];
        self.table
            .init_table_prepare_output(&mut output, self.actions.len() as u64);
        for (key, value) in &self.actions {
            self.table
                .append_key_value_to_table_prepare_output(&mut output, key, value.as_ref());
        }
        assert_eq!(output.len(), self.expected_len);
        output
    }

    fn decode_once(&self) -> usize {
        let (table, action_count, actions_offset) =
            <CodecTable as KVTable>::get_init_table_prepare_output(
                &self.encoded,
                ROOT_TID_BYTES,
            );
        assert_eq!(table.as_str(), self.table_name.as_str());
        assert_eq!(action_count, self.actions.len() as u64);
        let (actions, final_offset) =
            <CodecTable as KVTable>::get_all_key_value_from_table_prepare_output(
                &self.encoded,
                &table,
                action_count,
                actions_offset,
            );
        assert_eq!(actions.len(), self.actions.len());
        assert_eq!(final_offset, self.expected_len);
        black_box(actions);
        final_offset
    }

    fn verify(&self) {
        assert_eq!(&self.encoded[..ROOT_TID_BYTES], &[0x5a; ROOT_TID_BYTES]);
        assert_eq!(self.decode_once(), self.expected_len);
    }
}

fn encoded_len(
    table_name_bytes: usize,
    actions: &[(Binary, Option<Binary>)],
) -> usize {
    ROOT_TID_BYTES
        + 2
        + table_name_bytes
        + 8
        + actions
            .iter()
            .map(|(key, value)| 2 + key.len() + 4 + value.as_ref().map_or(0, Binary::len))
            .sum::<usize>()
}
