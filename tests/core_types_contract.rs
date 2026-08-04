//! L1 核心值对象、元数据与错误类型的独立契约测试。
//!
//! 本 target 与历史 `tests/test.rs` 完全独立，覆盖 `Binary`、`KVDBTableType`、
//! `KVTableMeta`、`TableTrQos`、`KVActionLog`、版本值对象、`TableKV` 和 `KVTableTrError` 的
//! 合法公开调用域。测试只用内存和标准线程，不依赖 runtime、文件系统、时钟、端口或执行
//! 顺序，可以离线稳定运行。
//!
//! 这里刻意不把下列当前缺陷写成通过断言：畸形/空 Key 比较 panic、损坏元数据解码 panic、
//! 持久化空 Value，以及非规范编码的 `Eq`/`Hash` 边界。它们分别由 `Q-KEY-001`、
//! `FIND-CODEC-001` 和 `FIND-DATA-001` 归档，必须在设计冻结或修复方案验收时使用独立专项。
//! 本 target 保护的是合法契约，不把已知偏离误写成正确行为。

use std::{
    cmp::Ordering,
    collections::{HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::{Arc, Weak},
    thread,
};

use pi_async_transaction::{ErrorLevel, TransactionConflictError, TransactionError};
use pi_atom::Atom;
use pi_bon::{Encode, WriteBuffer};
use pi_db::{Binary, KVActionLog, KVDBTableType, KVTableMeta, KVTableTrError,
            TableKey, TableKeyConflict, TableKeyVersion, TableTrQos, Version,
            VersionConflictKind,
            tables::TableKV};
use pi_guid::Guid;
use pi_ordmap::asbtree::TreeByteSize;
use pi_sinfo::EnumType;

/// 使用生产依赖 `pi_bon` 的规范编码器构造合法 `Binary`。
///
/// 该 helper 只服务于 Key 比较和错误上下文测试，避免手写字节把非法输入混入合法契约。
fn encode_bon<T: Encode>(value: &T) -> Binary {
    let mut buffer = WriteBuffer::new();
    value.encode(&mut buffer);
    Binary::from_slice(buffer.get_byte())
}

/// 编译期约束公开类型必须同时实现 `Send + Sync`。
fn assert_send_sync<T: Send + Sync>() {}

/// 计算值对象通过其公开 `Hash` 实现产生的确定进程内摘要。
fn hash_of<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// 验证 `Binary` 的 owned/shared/slice 构造、只读视图、allocation 身份和字节统计。
///
/// 该测试区分“内容相同”和“共享同一 allocation”，并验证 `TreeByteSize` 只统计 payload。
#[test]
fn test_binary_ownership_views_and_size_contract() {
    let binary = Binary::new(vec![7, 11, 13, 17]);

    assert_eq!(binary.len(), 4);
    assert_eq!(binary.as_ref(), &[7, 11, 13, 17]);
    assert_eq!(&*binary, &[7, 11, 13, 17]);
    assert_eq!(binary.tree_bytes_size(), 4);

    let cloned = binary.clone();
    assert!(Binary::binary_equal(&binary, &cloned));

    let shared = binary.to_shared();
    let from_shared = Binary::from_shared(shared);
    assert!(Binary::binary_equal(&binary, &from_shared));

    let copied = Binary::from_slice(binary.as_ref());
    assert_eq!(copied.as_ref(), binary.as_ref());
    assert!(!Binary::binary_equal(&binary, &copied));

    let empty = Binary::default();
    assert_eq!(empty.len(), 0);
    assert!(empty.as_ref().is_empty());
    assert_eq!(empty.tree_bytes_size(), 0);
}

/// 验证 `Binary` 的跨线程只读能力和最后一个 owner 释放 payload 的生命周期。
///
/// `Weak` 断言不会把进程级 allocator 行为误当成释放证明：它只验证 `Binary` 内部没有留下
/// 强引用环，并且最后一个 owner drop 后该 `Arc` allocation 已不可升级。
#[test]
fn test_binary_cross_thread_lifetime_contract() {
    assert_send_sync::<Binary>();

    let shared = Arc::new(vec![23, 29, 31]);
    let weak: Weak<Vec<u8>> = Arc::downgrade(&shared);
    let binary = Binary::from_shared(shared);
    let remote = binary.clone();

    thread::spawn(move || {
        assert_eq!(remote.as_ref(), &[23, 29, 31]);
        assert_eq!(remote.len(), 3);
    })
    .join()
    .expect("Binary cross-thread reader must not panic");

    assert!(
        weak.upgrade().is_some(),
        "the local Binary still owns the payload"
    );
    drop(binary);
    assert!(
        weak.upgrade().is_none(),
        "the final Binary drop must release the Arc payload"
    );
}

/// 验证有序 Key 比较使用 `pi_bon` 值语义，而不是原始字节字典序。
///
/// `"aa"` 的规范编码首字节因长度大于 `"z"`，原始 bytes 顺序与字符串语义顺序相反；这使
/// 断言能够客观区分两种比较实现。所有输入都由规范编码器生成，不覆盖畸形 Key 边界。
#[test]
fn test_binary_orders_canonical_bon_values() {
    let aa = encode_bon(&String::from("aa"));
    let z = encode_bon(&String::from("z"));
    let aa_copy = encode_bon(&String::from("aa"));

    assert_eq!(aa.as_ref().cmp(z.as_ref()), Ordering::Greater);
    assert_eq!(aa.partial_cmp(&z), Some(Ordering::Less));
    assert!(aa < z);
    assert_eq!(aa, aa_copy);
    assert_eq!(hash_of(&aa), hash_of(&aa_copy));
    assert!(!Binary::binary_equal(&aa, &aa_copy));
}

/// 验证四种表判别值、两种 persistence 状态和当前 Meta 持久化格式的完整 round-trip。
///
/// 每个 case 同时检查精确头字段、两段长度边界、getter 与解码结果，避免仅用派生相等掩盖
/// 字段错位。Memory+persistence=true 被显式纳入合法元数据域。
#[test]
fn test_table_meta_current_format_round_trip_matrix() {
    let table_types = [
        (1, KVDBTableType::MemOrdTab),
        (2, KVDBTableType::LogOrdTab),
        (3, KVDBTableType::LogWTab),
        (4, KVDBTableType::BtreeOrdTab),
    ];

    for (tag, table_type) in table_types {
        assert_eq!(KVDBTableType::from(tag), table_type);

        for persistence in [false, true] {
            let expected = KVTableMeta::new(
                table_type.clone(),
                persistence,
                EnumType::U64,
                EnumType::Bin,
            );
            let encoded = Binary::from(expected.clone());
            let bytes = encoded.as_ref();

            assert_eq!(bytes[0], tag);
            assert_eq!(bytes[1], u8::from(persistence));

            let key_len = u16::from_le_bytes([bytes[2], bytes[3]]) as usize;
            assert!(key_len > 0);
            let value_len_offset = 4 + key_len;
            let value_len =
                u16::from_le_bytes([bytes[value_len_offset], bytes[value_len_offset + 1]]) as usize;
            assert!(value_len > 0);
            assert_eq!(bytes.len(), value_len_offset + 2 + value_len);

            let restored = KVTableMeta::from(encoded);
            assert_eq!(restored, expected);
            assert_eq!(restored.table_type(), &table_type);
            assert_eq!(restored.is_persistence(), persistence);
            assert_eq!(restored.key_type(), &EnumType::U64);
            assert_eq!(restored.value_type(), &EnumType::Bin);
        }
    }
}

/// 验证 Meta 长度字段能够覆盖多层嵌套 `EnumType`，而不只覆盖单字节标量描述。
///
/// 该测试仍使用合法、规模受控的类型树；超出 `u16` 长度上限的当前未校验风险不在主回归中
/// 固化，而由 `FIND-CODEC-001` 归档。
#[test]
fn test_table_meta_nested_type_descriptors_round_trip() {
    let key_type = EnumType::Option(Arc::new(EnumType::Str));
    let value_type = EnumType::Map(
        Arc::new(EnumType::U64),
        Arc::new(EnumType::Arr(Arc::new(EnumType::Bin))),
    );
    let expected = KVTableMeta::new(
        KVDBTableType::BtreeOrdTab,
        true,
        key_type.clone(),
        value_type.clone(),
    );
    let encoded = Binary::from(expected.clone());
    let bytes = encoded.as_ref();
    let key_len = u16::from_le_bytes([bytes[2], bytes[3]]) as usize;
    let value_len_offset = 4 + key_len;
    let value_len =
        u16::from_le_bytes([bytes[value_len_offset], bytes[value_len_offset + 1]]) as usize;

    assert!(
        key_len > 1,
        "nested key descriptor must exercise a length greater than one"
    );
    assert!(
        value_len > key_len,
        "nested map/array descriptor must exercise a distinct longer value segment"
    );
    assert_eq!(bytes.len(), value_len_offset + 2 + value_len);

    let restored = KVTableMeta::from(encoded);
    assert_eq!(restored, expected);
    assert_eq!(restored.key_type(), &key_type);
    assert_eq!(restored.value_type(), &value_type);
}

/// 验证旧格式兼容入口按顺序读取两个合法 `EnumType`，并对确定可恢复的截断输入返回错误。
///
/// 本测试不注入会触发依赖 panic 的非法类型判别值；该风险属于 `FIND-CODEC-001` 专项范围。
#[test]
fn test_table_meta_compatibility_decoder_contract() {
    let mut legacy = WriteBuffer::new();
    EnumType::Usize.encode(&mut legacy);
    EnumType::Str.encode(&mut legacy);

    let restored = KVTableMeta::with_compatibled(KVDBTableType::LogOrdTab, true, legacy.get_byte())
        .expect("two canonical EnumType values must decode");

    assert_eq!(restored.table_type(), &KVDBTableType::LogOrdTab);
    assert!(restored.is_persistence());
    assert_eq!(restored.key_type(), &EnumType::Usize);
    assert_eq!(restored.value_type(), &EnumType::Str);
    assert!(KVTableMeta::with_compatibled(KVDBTableType::MemOrdTab, false, &[],).is_err());
}

/// 验证 QoS 默认值以及普通/dirty 动作的公开 variant 分类和 payload 含义。
///
/// 这只验证值对象，不宣称外部构造的 `KVActionLog` 会自动影响事务状态。
#[test]
fn test_qos_and_action_log_value_contracts() {
    assert_eq!(TableTrQos::default(), TableTrQos::Safe);
    assert_ne!(TableTrQos::ThreadSafe, TableTrQos::Safe);
    assert_ne!(TableTrQos::Unsafe, TableTrQos::ThreadSafe);

    let value = Binary::from_slice([37, 41, 43]);
    let read = KVActionLog::Read;
    let write = KVActionLog::Write(Some(value.clone()));
    let delete = KVActionLog::Write(None);
    let dirty_write = KVActionLog::DirtyWrite(Some(value));
    let dirty_delete = KVActionLog::DirtyWrite(None);

    assert!(!read.is_dirty_writed());
    assert!(!write.is_dirty_writed());
    assert!(!delete.is_dirty_writed());
    assert!(dirty_write.is_dirty_writed());
    assert!(dirty_delete.is_dirty_writed());

    match write {
        KVActionLog::Write(Some(payload)) => assert_eq!(payload.as_ref(), &[37, 41, 43]),
        _ => panic!("ordinary upsert action must preserve its Some payload"),
    }
    assert!(matches!(delete, KVActionLog::Write(None)));
    assert!(matches!(dirty_delete, KVActionLog::DirtyWrite(None)));
}

/// 验证公开版本载荷拥有字段、保持 variant、按内容相等/Hash，且不会借用版本缓存。
///
/// 固定 Guid 只用于值对象断言，不冒充事务管理器生成或已经提交的生产 transaction UID。
#[test]
fn test_version_and_table_key_value_object_contracts() {
    assert_send_sync::<Version>();
    assert_send_sync::<TableKeyVersion>();
    assert_send_sync::<TableKey>();

    let table = Atom::from("versioned_users");
    let key = encode_bon(&71_u64);
    let upsert_uid = Guid(0x1020_3040_5060_7080_90a0_b0c0_d0e0_f001);
    let delete_uid = Guid(0x1020_3040_5060_7080_90a0_b0c0_d0e0_f002);
    let upsert = Version::Upsert(upsert_uid.clone());
    let delete = Version::Delete(delete_uid.clone());

    match &upsert {
        Version::Upsert(uid) => assert_eq!(uid, &upsert_uid),
        other => panic!("upsert version changed variant: {other:?}"),
    }
    match &delete {
        Version::Delete(uid) => assert_eq!(uid, &delete_uid),
        other => panic!("delete version changed variant: {other:?}"),
    }
    assert_ne!(upsert, delete);

    let observed = TableKeyVersion {
        table: table.clone(),
        key: key.clone(),
        version: upsert,
    };
    let observed_clone = observed.clone();
    assert_eq!(observed_clone, observed);
    assert!(Binary::binary_equal(&observed.key, &observed_clone.key));
    assert_eq!(hash_of(&observed), hash_of(&observed_clone));

    let mut observed_set = HashSet::new();
    assert!(observed_set.insert(observed.clone()));
    assert!(!observed_set.insert(observed_clone));
    assert_eq!(observed_set.len(), 1);

    let table_key = TableKey {
        table,
        key,
    };
    let table_key_clone = table_key.clone();
    assert_eq!(table_key_clone, table_key);
    assert!(Binary::binary_equal(&table_key.key, &table_key_clone.key));
    assert_eq!(hash_of(&table_key), hash_of(&table_key_clone));
}

/// 验证 `TableKV` 的 Some/None 载荷表达、O(1) clone 所有权和跨线程释放生命周期。
///
/// 该测试不调用数据库 API，因此不会把三元组构造误写为事务选择、表校验或 WAL 副作用。
#[test]
fn test_table_kv_owned_payload_and_lifetime_contract() {
    assert_send_sync::<TableKV>();

    let key_owner = encode_bon(&73_u64).to_shared();
    let key_weak = Arc::downgrade(&key_owner);
    let value_owner = Arc::new(vec![79, 83, 89]);
    let value_weak = Arc::downgrade(&value_owner);
    let action = TableKV::new(
        Atom::from("owned_actions"),
        Binary::from_shared(key_owner),
        Some(Binary::from_shared(value_owner)),
    );

    assert!(action.exist_value());
    assert_eq!(action.table.as_str(), "owned_actions");
    assert_eq!(action.value.as_ref().expect("upsert value").as_ref(), &[79, 83, 89]);

    let cloned = action.clone();
    assert!(Binary::binary_equal(&action.key, &cloned.key));
    assert!(Binary::binary_equal(
        action.value.as_ref().expect("original value"),
        cloned.value.as_ref().expect("cloned value"),
    ));
    thread::spawn(move || {
        assert!(cloned.exist_value());
        assert_eq!(cloned.table.as_str(), "owned_actions");
    })
    .join()
    .expect("moving a cloned TableKV across an OS thread must not panic");

    assert!(key_weak.upgrade().is_some());
    assert!(value_weak.upgrade().is_some());
    drop(action);
    assert!(key_weak.upgrade().is_none());
    assert!(value_weak.upgrade().is_none());

    let delete = TableKV::new(
        Atom::from("owned_actions"),
        encode_bon(&97_u64),
        None,
    );
    assert!(!delete.exist_value());
    assert!(delete.value.is_none());
}

/// 验证 Common/Fatal/Conflicts 的分类、等级、诊断信息和 owned 冲突上下文。
///
/// 断言严格区分不可恢复 Fatal 与固定为 Normal 的冲突；不执行事务 rollback，因为该行为由
/// 事务树专项负责，而不是错误值对象本身的副作用。
#[test]
fn test_table_transaction_error_contract() {
    let normal = <KVTableTrError as TransactionError>::new_transaction_error(
        ErrorLevel::Normal,
        "retryable",
    );
    assert!(normal.is_common());
    assert!(!normal.is_conflicts());
    assert!(matches!(normal.level(), ErrorLevel::Normal));
    assert!(normal.conflicts().is_none());
    match &normal {
        KVTableTrError::Common(ErrorLevel::Normal, message) => {
            assert!(message.contains("Table transaction error"));
            assert!(message.contains("retryable"));
        }
        _ => panic!("normal constructor must produce Common(Normal, message)"),
    }

    let fatal = <KVTableTrError as TransactionError>::new_transaction_error(
        ErrorLevel::Fatal,
        "unrecoverable",
    );
    assert!(fatal.is_common());
    assert!(matches!(fatal.level(), ErrorLevel::Fatal));

    let expected_key = encode_bon(&42_u64);
    let original_key_owner = expected_key.clone();
    let expected_bytes = expected_key.as_ref().to_vec();
    let conflict = KVTableTrError::new_conflicts_error(Atom::from("users"), expected_key);

    assert!(!conflict.is_common());
    assert!(conflict.is_conflicts());
    assert!(matches!(conflict.level(), ErrorLevel::Normal));
    let (table, key) = conflict
        .conflicts()
        .expect("Conflicts must expose its owned context");
    assert_eq!(table.as_str(), "users");
    assert_eq!(key.as_ref(), expected_bytes.as_slice());
    assert!(
        !Binary::binary_equal(&original_key_owner, key),
        "new_conflicts_error must own a copied Key allocation"
    );
    assert!(conflict.all_conflicts().is_none());

    let all_conflicts =
        <KVTableTrError as TransactionConflictError>::from_conflict_set(vec![
            TableKeyConflict {
                table: Atom::from("users"),
                key: encode_bon(&43_u64),
                kind: VersionConflictKind::TransactionConflict,
            },
            TableKeyConflict {
                table: Atom::from("accounts"),
                key: encode_bon(&7_u64),
                kind: VersionConflictKind::TransactionConflict,
            },
            TableKeyConflict {
                table: Atom::from("users"),
                key: encode_bon(&43_u64),
                kind: VersionConflictKind::ReadSetVersionMismatch,
            },
        ]);
    assert!(all_conflicts.is_all_conflicts());
    assert!(matches!(all_conflicts.level(), ErrorLevel::Normal));
    let classified = all_conflicts
        .all_conflicts()
        .expect("AllConflicts must expose its classified complete set");
    assert_eq!(classified.len(), 2);
    assert_eq!(classified[0].table.as_str(), "accounts");
    assert_eq!(classified[0].kind, VersionConflictKind::TransactionConflict);
    assert_eq!(classified[1].table.as_str(), "users");
    assert_eq!(classified[1].kind, VersionConflictKind::ReadSetVersionMismatch);
    let first = all_conflicts
        .conflicts()
        .expect("AllConflicts must preserve the compatibility first position");
    assert_eq!(first.0, &classified[0].table);
    assert_eq!(first.1, &classified[0].key);
}

/// 验证事务框架冲突集合转换只转换冲突 variant、合并阶段只追加、最终构造才归一化。
#[test]
fn test_table_transaction_conflict_set_conversion_and_merge_contract() {
    let ordinary_key = encode_bon(&101_u64);
    let ordinary = KVTableTrError::new_conflicts_error(
        Atom::from("ordinary"),
        ordinary_key.clone(),
    );
    let ordinary_set = ordinary
        .into_conflict_set()
        .expect("ordinary conflict must convert into one classified item");
    assert_eq!(ordinary_set.len(), 1);
    assert_eq!(ordinary_set[0].table.as_str(), "ordinary");
    assert_eq!(ordinary_set[0].key, ordinary_key);
    assert_eq!(ordinary_set[0].kind, VersionConflictKind::TransactionConflict);

    let common = <KVTableTrError as TransactionError>::new_transaction_error(
        ErrorLevel::Normal,
        "not-a-conflict",
    );
    let returned = common
        .into_conflict_set()
        .expect_err("Common must remain an error instead of becoming an empty conflict set");
    assert!(returned.is_common());
    assert!(!returned.is_conflicts());

    let duplicate_key = encode_bon(&103_u64);
    let mut target = vec![TableKeyConflict {
        table: Atom::from("merged"),
        key: duplicate_key.clone(),
        kind: VersionConflictKind::TransactionConflict,
    }];
    let source = vec![
        TableKeyConflict {
            table: Atom::from("other"),
            key: encode_bon(&107_u64),
            kind: VersionConflictKind::TransactionConflict,
        },
        TableKeyConflict {
            table: Atom::from("merged"),
            key: duplicate_key,
            kind: VersionConflictKind::ReadSetVersionMismatch,
        },
    ];
    <KVTableTrError as TransactionConflictError>::merge_conflict_sets(&mut target, source);
    assert_eq!(target.len(), 3, "merge must append without premature normalization");
    assert_eq!(target[0].table.as_str(), "merged");
    assert_eq!(target[1].table.as_str(), "other");
    assert_eq!(target[2].table.as_str(), "merged");

    let normalized = <KVTableTrError as TransactionConflictError>::from_conflict_set(target);
    let classified = normalized
        .all_conflicts()
        .expect("final construction must produce a classified complete set");
    assert_eq!(classified.len(), 2);
    assert_eq!(classified[0].table.as_str(), "merged");
    assert_eq!(classified[0].kind, VersionConflictKind::ReadSetVersionMismatch);
    assert_eq!(classified[1].table.as_str(), "other");
    let expected = classified.to_vec();

    let round_trip = normalized
        .into_conflict_set()
        .expect("AllConflicts must return its owned normalized set");
    assert_eq!(round_trip, expected);
}

/// 验证显式 `Send/Sync` 错误类型可被多个真实 OS 线程并发只读。
///
/// 线程只借助 `Arc` 共享 immutable error，不添加外部锁；这覆盖 variant 查询和冲突上下文
/// 借用的并发边界，同时确保所有线程都观察到一致内容。
#[test]
fn test_table_transaction_error_cross_thread_read_contract() {
    assert_send_sync::<KVTableTrError>();
    assert_send_sync::<TableKeyConflict>();
    assert_send_sync::<VersionConflictKind>();

    let conflict = Arc::new(KVTableTrError::new_conflicts_error(
        Atom::from("accounts"),
        encode_bon(&7_u64),
    ));
    let mut readers = Vec::new();

    for _ in 0..4 {
        let conflict = conflict.clone();
        readers.push(thread::spawn(move || {
            assert!(conflict.is_conflicts());
            assert!(matches!(conflict.level(), ErrorLevel::Normal));
            let (table, key) = conflict.conflicts().expect("context must remain available");
            assert_eq!(table.as_str(), "accounts");
            assert_eq!(key.as_ref(), encode_bon(&7_u64).as_ref());
        }));
    }

    for reader in readers {
        reader
            .join()
            .expect("concurrent immutable error reader must not panic");
    }
}
