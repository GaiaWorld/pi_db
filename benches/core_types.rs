#![feature(test)]
//! 公共核心值对象的纯 CPU、引用计数与分配基准。
//!
//! 本 target 不启动 runtime、事务管理器、版本缓存、表、文件系统或 WAL，只测量合法 canonical
//! 输入上的局部成本。结果不能代表完整 query/prepare/commit、DDL、listener 或 repair 延迟。
//! `Binary::cmp` 会包含当前 `FIND-PERF-BINARY-001` 已归档的成功路径格式化成本，本基准只建立
//! 现状基线，不在本切片修复该行为。

extern crate test;

use std::{
    cmp::Ordering,
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{
    Binary, KVDBTableType, KVTableMeta,
    tables::TableKV,
};
use pi_sinfo::EnumType;
use test::{Bencher, black_box};

fn encode_bon<T: Encode>(value: &T) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::from_slice(buffer.get_byte())
}

#[bench]
fn bench_binary_clone_shared_4k(b: &mut Bencher) {
    let value = Binary::new(vec![0x5a; 4096]);
    let cloned = value.clone();
    assert!(Binary::binary_equal(&value, &cloned));

    b.iter(|| black_box(black_box(&value).clone()));
}

#[bench]
fn bench_binary_from_slice_copy_4k(b: &mut Bencher) {
    let value = vec![0xa5; 4096];
    let copied = Binary::from_slice(&value);
    assert_eq!(copied.as_ref(), value.as_slice());

    b.iter(|| black_box(Binary::from_slice(black_box(value.as_slice()))));
}

#[bench]
fn bench_binary_canonical_u64_compare(b: &mut Bencher) {
    let left = encode_bon(&17_u64);
    let right = encode_bon(&19_u64);
    assert_eq!(left.cmp(&right), Ordering::Less);

    b.iter(|| black_box(black_box(&left).cmp(black_box(&right))));
}

#[bench]
fn bench_binary_hash_4k(b: &mut Bencher) {
    let value = Binary::new(vec![0x3c; 4096]);
    let mut verifier = DefaultHasher::new();
    value.hash(&mut verifier);
    assert_ne!(verifier.finish(), 0);

    b.iter(|| {
        let mut hasher = DefaultHasher::new();
        black_box(&value).hash(&mut hasher);
        black_box(hasher.finish())
    });
}

#[bench]
fn bench_table_meta_encode_nested_types(b: &mut Bencher) {
    let meta = KVTableMeta::new(
        KVDBTableType::BtreeOrdTab,
        true,
        EnumType::Option(std::sync::Arc::new(EnumType::U64)),
        EnumType::Map(
            std::sync::Arc::new(EnumType::Str),
            std::sync::Arc::new(EnumType::Arr(std::sync::Arc::new(EnumType::Bin))),
        ),
    );
    let encoded = Binary::from(meta.clone());
    assert_eq!(KVTableMeta::from(encoded), meta);

    b.iter(|| black_box(Binary::from(black_box(meta.clone()))));
}

#[bench]
fn bench_table_meta_decode_nested_types(b: &mut Bencher) {
    let meta = KVTableMeta::new(
        KVDBTableType::BtreeOrdTab,
        true,
        EnumType::Option(std::sync::Arc::new(EnumType::U64)),
        EnumType::Map(
            std::sync::Arc::new(EnumType::Str),
            std::sync::Arc::new(EnumType::Arr(std::sync::Arc::new(EnumType::Bin))),
        ),
    );
    let encoded = Binary::from(meta.clone());
    assert_eq!(KVTableMeta::from(encoded.clone()), meta);

    b.iter(|| black_box(KVTableMeta::from(black_box(encoded.clone()))));
}

#[bench]
fn bench_table_kv_clone_shared_4k_value(b: &mut Bencher) {
    let action = TableKV::new(
        Atom::from("core_types_benchmark"),
        encode_bon(&23_u64),
        Some(Binary::new(vec![0xc3; 4096])),
    );
    let cloned = action.clone();
    assert!(Binary::binary_equal(&action.key, &cloned.key));
    assert!(Binary::binary_equal(
        action.value.as_ref().expect("original benchmark value"),
        cloned.value.as_ref().expect("cloned benchmark value"),
    ));

    b.iter(|| black_box(black_box(&action).clone()));
}
