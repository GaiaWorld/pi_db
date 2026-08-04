//! `utils` 公共值对象与当前边界的独立契约测试。
//!
//! 本 target 不引用或运行旧测试。它验证 [`CreateTableOptions`] 的字段顺序/所有权，以及
//! [`KVDBEvent`] 三个 variant 的互斥分类、payload 顺序、clone 和真实 OS 线程移动。
//!
//! 本 target 刻意不把以下已归档现状包装为正确设计：Meta/LogOrdered 生产事件当前使用
//! `BtreeOrdTab` 标签、监听回调必须自行清空批次、Btree `enable_compact` 尚未被消费，以及
//! 自定义建表参数重启后不会恢复。这些边界由生产注释和 `FIND-EVENT-001` /
//! `FIND-COMPACT-001` 记录，后续真实 DDL/事件专项负责生产可达性。
//!
//! 被测入口：`pi_db::utils::{CreateTableOptions, KVDBEvent}`。
//! 文档入口：`docs/CORE_PUBLIC_TYPES_CONTRACT.md#core-public-types-errors-events`、
//! `docs/SEMANTIC_CONTRACTS.md#contract-observability-events`。

use std::thread;

use pi_atom::Atom;
use pi_db::{
    utils::{CreateTableOptions, KVDBEvent},
    KVDBTableType,
};

/// 编译期约束当前无借用的公共工具值对象可在线程间移动和共享。
fn assert_send_sync<T: Send + Sync>() {}

/// 精确验证三个建表选项 variant 的字段顺序和值在 clone 后保持不变。
#[test]
fn test_create_table_options_preserve_exact_payloads() {
    assert_send_sync::<CreateTableOptions>();

    assert!(matches!(
        CreateTableOptions::Empty.clone(),
        CreateTableOptions::Empty
    ));

    let log = CreateTableOptions::LogOrdTab(17, 23, 31);
    match log.clone() {
        CreateTableOptions::LogOrdTab(log_file_limit, block_limit, load_buf_len) => {
            assert_eq!(log_file_limit, 17);
            assert_eq!(block_limit, 23);
            assert_eq!(load_buf_len, 31);
        }
        other => panic!("LogOrdTab clone changed variant: {other:?}"),
    }

    let btree = CreateTableOptions::BtreeOrdTab(65_536, true);
    match btree.clone() {
        CreateTableOptions::BtreeOrdTab(cache_size, enable_compact) => {
            assert_eq!(cache_size, 65_536);
            assert!(enable_compact);
        }
        other => panic!("BtreeOrdTab clone changed variant: {other:?}"),
    }

    // 值对象本身不做范围回退或合法性校验；这里只证明极值和 false 被原样保存，不能外推为
    // 具体表引擎会按这些值成功创建。
    match CreateTableOptions::LogOrdTab(0, usize::MAX, 1).clone() {
        CreateTableOptions::LogOrdTab(file_limit, block_limit, load_len) => {
            assert_eq!((file_limit, block_limit, load_len), (0, usize::MAX, 1));
        }
        other => panic!("boundary LogOrdTab changed variant: {other:?}"),
    }
    match CreateTableOptions::BtreeOrdTab(0, false).clone() {
        CreateTableOptions::BtreeOrdTab(cache_size, enable_compact) => {
            assert_eq!(cache_size, 0);
            assert!(!enable_compact);
        }
        other => panic!("boundary BtreeOrdTab changed variant: {other:?}"),
    }
}

/// 验证每个事件只命中自己的分类谓词，避免调用方把名称相近事件混为一类。
#[test]
fn test_event_predicates_are_mutually_exclusive() {
    assert_send_sync::<KVDBEvent<u64>>();

    let report = KVDBEvent::<u64>::ReportTrInfo;
    assert!(report.is_report_transaction_info());
    assert!(!report.is_commit_failed());
    assert!(!report.is_confirm_commited());

    let failed = KVDBEvent::CommitFailed(
        Atom::from("source-failed"),
        Atom::from("table-failed"),
        KVDBTableType::LogOrdTab,
        101_u64,
        201_u64,
    );
    assert!(!failed.is_report_transaction_info());
    assert!(failed.is_commit_failed());
    assert!(!failed.is_confirm_commited());

    let confirmed = KVDBEvent::ConfirmCommited(
        Atom::from("source-confirmed"),
        Atom::from("table-confirmed"),
        KVDBTableType::BtreeOrdTab,
        102_u64,
        202_u64,
    );
    assert!(!confirmed.is_report_transaction_info());
    assert!(!confirmed.is_commit_failed());
    assert!(confirmed.is_confirm_commited());
}

/// 在 clone 后逐字段匹配 payload，并把具体事件移动到真实 OS 线程再取回。
///
/// 这证明值对象不会借用构造栈；它不证明监听器任务、通道积压或 collector 的生命周期。
#[test]
fn test_event_payload_order_clone_and_thread_move() {
    let event = KVDBEvent::ConfirmCommited(
        Atom::from("payload-source"),
        Atom::from("payload-table"),
        KVDBTableType::MemOrdTab,
        0x11_u64,
        0x22_u64,
    );

    let cloned = event.clone();
    let returned = thread::spawn(move || {
        assert!(cloned.is_confirm_commited());
        cloned
    })
    .join()
    .expect("moving a concrete KVDBEvent across an OS thread must not panic");

    match returned {
        KVDBEvent::ConfirmCommited(source, table, table_type, transaction_uid, commit_uid) => {
            assert_eq!(source.as_str(), "payload-source");
            assert_eq!(table.as_str(), "payload-table");
            assert_eq!(table_type, KVDBTableType::MemOrdTab);
            assert_eq!(transaction_uid, 0x11);
            assert_eq!(commit_uid, 0x22);
        }
        other => panic!("event clone/thread move changed variant: {other:?}"),
    }

    // 原 owner 在远端 clone 完成和释放后仍保持完整 payload。
    assert!(event.is_confirm_commited());
    match event {
        KVDBEvent::ConfirmCommited(source, table, _, transaction_uid, commit_uid) => {
            assert_eq!(source.as_str(), "payload-source");
            assert_eq!(table.as_str(), "payload-table");
            assert_eq!((transaction_uid, commit_uid), (0x11, 0x22));
        }
        other => panic!("original event changed variant: {other:?}"),
    }
}
