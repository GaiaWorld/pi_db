use std::convert::TryInto;
use std::io::Result as IOResult;

use futures::{future::{FutureExt, BoxFuture},
              stream::{StreamExt, BoxStream}};
use bytes::BufMut;
use env_logger;

use atom::Atom;
use guid::{GuidGen, Guid};
use sinfo::EnumType;
use time::run_nanos;
use r#async::rt::multi_thread::MultiTaskRuntimeBuilder;
use async_transaction::{AsyncCommitLog,
                        ErrorLevel,
                        manager_2pc::Transaction2PcManager};
use pi_store::commit_logger::{CommitLoggerBuilder, CommitLogger};

use pi_db::{Binary,
            KVDBTableType,
            KVTableMeta,
            db::KVDBManagerBuilder,
            tables::TableKV};

#[test]
fn test_memory_table() {
    use std::thread;
    use std::time::Duration;

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    rt.spawn(rt.alloc(), async move {
        let guid_gen = GuidGen::new(run_nanos(), 0);
        let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), "./.commit_log");
        let commit_logger = commit_logger_builder
            .build()
            .await
            .unwrap();

        let tr_mgr = Transaction2PcManager::new(rt_copy.clone(),
                                                guid_gen,
                                                commit_logger);

        let mut builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./db");
        match builder.startup().await {
            Err(e) => {
                panic!(e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_memory");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::MemOrdTab,
                                                                 false,
                                                                 EnumType::U8,
                                                                 EnumType::Str)).await {
                    //创建有序内存表失败
                    println!("!!!!!!create memory ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;

                println!("!!!!!!db table size: {:?}", db.table_size().await);

                //查询表信息
                rt_copy.wait_timeout(1500).await;
                println!("");

                println!("!!!!!!test_memory is exist: {:?}", db.is_exist(&table_name).await);
                println!("!!!!!!test_memory is ordered table: {:?}", db.is_ordered_table(&table_name).await);
                println!("!!!!!!test_memory is persistent table: {:?}", db.is_persistent_table(&table_name).await);
                println!("!!!!!!test_memory table_dir: {:?}", db.table_path(&table_name).await);
                println!("!!!!!!test_memory table len: {:?}", db.table_record_size(&table_name).await);

                //操作数据库事务
                rt_copy.wait_timeout(1500).await;
                println!("");

                let tr = db.transaction(Atom::from("test memory table"), true, 500, 500).unwrap();
                let r = tr.query(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: None
                    }
                ]).await;
                println!("!!!!!!query result: {:?}", r);

                let r = tr.upsert(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    }
                ]).await;
                println!("!!!!!!upsert result: {:?}", r);

                let r = tr.query(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: None
                    }
                ]).await;
                println!("!!!!!!query result: {:?}", r);

                let r = tr.delete(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: None
                    }
                ]).await;
                println!("!!!!!!delete result: {:?}", r);

                let r = tr.query(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: None
                    }
                ]).await;
                println!("!!!!!!query result: {:?}", r);

                let mut table_kv_list = Vec::new();
                for key in 0..10u8 {
                    table_kv_list.push(TableKV {
                        table: table_name.clone(),
                        key: Binary::new(key.to_le_bytes().to_vec()),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    });
                }
                let r = tr.upsert(table_kv_list).await;
                println!("!!!!!!batch upsert, result: {:?}", r);

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    None,
                    true
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    Some(Binary::new(6u8.to_le_bytes().to_vec())),
                    false
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    Some(Binary::new(6u8.to_le_bytes().to_vec())),
                    true
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    true
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    Some(Binary::new(6u8.to_le_bytes().to_vec())),
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    Some(Binary::new(6u8.to_le_bytes().to_vec())),
                    true
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                let mut table_kv_list = Vec::new();
                for key in 0..10u8 {
                    table_kv_list.push(TableKV {
                        table: table_name.clone(),
                        key: Binary::new(key.to_le_bytes().to_vec()),
                        value: None,
                    });
                }
                let r = tr.delete(table_kv_list).await;
                println!("!!!!!!batch delete result: {:?}", r);

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                match tr.prepare_modified().await {
                    Err(e) => {
                        println!("prepare failed, reason: {:?}", e);
                        if let Err(e) = tr.rollback_modified().await {
                            println!("rollback failed, reason: {:?}", e);
                        } else {
                            println!("rollback ok for prepare");
                        }
                    },
                    Ok(output) => {
                        println!("prepare ok, output: {:?}", output);
                        match tr.commit_modified(output).await {
                            Err(e) => {
                                println!("commit failed, reason: {:?}", e);
                                if let ErrorLevel::Fatal = &e.level() {
                                    println!("rollback failed, reason: commit fatal error");
                                } else {
                                    println!("rollbakc ok for commit");
                                }
                            },
                            Ok(()) => {
                                println!("commit ok");
                            },
                        }
                    },
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_commit_log() {
    use std::thread;
    use std::time::Duration;

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    rt.spawn(rt.alloc(), async move {
        let guid_gen = GuidGen::new(run_nanos(), 0);
        let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), "./.commit_log");
        let commit_logger = commit_logger_builder
            .build()
            .await
            .unwrap();

        let tr_mgr = Transaction2PcManager::new(rt_copy.clone(),
                                                guid_gen,
                                                commit_logger);

        let mut builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./db");
        match builder.startup().await {
            Err(e) => {
                panic!(e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_memory");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::MemOrdTab,
                                                                 true,
                                                                 EnumType::U8,
                                                                 EnumType::Str)).await {
                    //创建有序内存表失败
                    println!("!!!!!!create memory ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;

                println!("!!!!!!db table size: {:?}", db.table_size().await);

                //查询表信息
                rt_copy.wait_timeout(1500).await;
                println!("");

                println!("!!!!!!test_memory is exist: {:?}", db.is_exist(&table_name).await);
                println!("!!!!!!test_memory is ordered table: {:?}", db.is_ordered_table(&table_name).await);
                println!("!!!!!!test_memory is persistent table: {:?}", db.is_persistent_table(&table_name).await);
                println!("!!!!!!test_memory table_dir: {:?}", db.table_path(&table_name).await);
                println!("!!!!!!test_memory table len: {:?}", db.table_record_size(&table_name).await);

                //操作数据库事务
                rt_copy.wait_timeout(1500).await;
                println!("");

                let tr = db.transaction(Atom::from("test memory table"), true, 500, 500).unwrap();
                let r = tr.query(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: None
                    }
                ]).await;
                println!("!!!!!!query result: {:?}", r);

                let r = tr.upsert(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    }
                ]).await;
                println!("!!!!!!upsert result: {:?}", r);

                let r = tr.query(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: None
                    }
                ]).await;
                println!("!!!!!!query result: {:?}", r);

                let r = tr.delete(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: None
                    }
                ]).await;
                println!("!!!!!!delete result: {:?}", r);

                let r = tr.query(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: None
                    }
                ]).await;
                println!("!!!!!!query result: {:?}", r);

                let mut table_kv_list = Vec::new();
                for key in 0..10u8 {
                    table_kv_list.push(TableKV {
                        table: table_name.clone(),
                        key: Binary::new(key.to_le_bytes().to_vec()),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    });
                }
                let r = tr.upsert(table_kv_list).await;
                println!("!!!!!!batch upsert, result: {:?}", r);

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    None,
                    true
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    Some(Binary::new(6u8.to_le_bytes().to_vec())),
                    false
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    Some(Binary::new(6u8.to_le_bytes().to_vec())),
                    true
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    true
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    Some(Binary::new(6u8.to_le_bytes().to_vec())),
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    Some(Binary::new(6u8.to_le_bytes().to_vec())),
                    true
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                let mut table_kv_list = Vec::new();
                for key in 0..10u8 {
                    table_kv_list.push(TableKV {
                        table: table_name.clone(),
                        key: Binary::new(key.to_le_bytes().to_vec()),
                        value: None,
                    });
                }
                let r = tr.delete(table_kv_list).await;
                println!("!!!!!!batch delete result: {:?}", r);

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                match tr.prepare_modified().await {
                    Err(e) => {
                        println!("prepare failed, reason: {:?}", e);
                        if let Err(e) = tr.rollback_modified().await {
                            println!("rollback failed, reason: {:?}", e);
                        } else {
                            println!("rollback ok for prepare");
                        }
                    },
                    Ok(output) => {
                        println!("prepare ok, output: {:?}", output);
                        match tr.commit_modified(output).await {
                            Err(e) => {
                                println!("commit failed, reason: {:?}", e);
                                if let ErrorLevel::Fatal = &e.level() {
                                    println!("rollback failed, reason: commit fatal error");
                                } else {
                                    println!("rollbakc ok for commit");
                                }
                            },
                            Ok(()) => {
                                println!("commit ok");
                            },
                        }
                    },
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_table() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    rt.spawn(rt.alloc(), async move {
        let guid_gen = GuidGen::new(run_nanos(), 0);
        let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), "./.commit_log");
        let commit_logger = commit_logger_builder
            .build()
            .await
            .unwrap();

        let tr_mgr = Transaction2PcManager::new(rt_copy.clone(),
                                                guid_gen,
                                                commit_logger);

        let mut builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./db");
        match builder.startup().await {
            Err(e) => {
                panic!(e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::U8,
                                                                 EnumType::Str)).await {
                    //创建有序内存表失败
                    println!("!!!!!!create log ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;

                println!("!!!!!!db table size: {:?}", db.table_size().await);

                //查询表信息
                rt_copy.wait_timeout(1500).await;
                println!("");

                println!("!!!!!!test_log is exist: {:?}", db.is_exist(&table_name).await);
                println!("!!!!!!test_log is ordered table: {:?}", db.is_ordered_table(&table_name).await);
                println!("!!!!!!test_log is persistent table: {:?}", db.is_persistent_table(&table_name).await);
                println!("!!!!!!test_log table_dir: {:?}", db.table_path(&table_name).await);
                println!("!!!!!!test_log table len: {:?}", db.table_record_size(&table_name).await);

                //操作数据库事务
                rt_copy.wait_timeout(1500).await;
                println!("");

                let tr = db.transaction(Atom::from("test log table"), true, 500, 500).unwrap();

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                let r = tr.query(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: None
                    }
                ]).await;
                println!("!!!!!!query result: {:?}", r);

                let r = tr.upsert(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    }
                ]).await;
                println!("!!!!!!upsert result: {:?}", r);

                let r = tr.query(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: None
                    }
                ]).await;
                println!("!!!!!!query result: {:?}", r);

                let r = tr.delete(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: None
                    }
                ]).await;
                println!("!!!!!!delete result: {:?}", r);

                let r = tr.query(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: None
                    }
                ]).await;
                println!("!!!!!!query result: {:?}", r);

                let mut table_kv_list = Vec::new();
                for key in 0..10u8 {
                    table_kv_list.push(TableKV {
                        table: table_name.clone(),
                        key: Binary::new(key.to_le_bytes().to_vec()),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    });
                }
                let r = tr.upsert(table_kv_list).await;
                println!("!!!!!!batch upsert, result: {:?}", r);

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    None,
                    true
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    Some(Binary::new(6u8.to_le_bytes().to_vec())),
                    false
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    Some(Binary::new(6u8.to_le_bytes().to_vec())),
                    true
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    true
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    Some(Binary::new(6u8.to_le_bytes().to_vec())),
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    Some(Binary::new(6u8.to_le_bytes().to_vec())),
                    true
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                let mut table_kv_list = Vec::new();
                for key in 0..10u8 {
                    table_kv_list.push(TableKV {
                        table: table_name.clone(),
                        key: Binary::new(key.to_le_bytes().to_vec()),
                        value: None,
                    });
                }
                let r = tr.delete(table_kv_list).await;
                println!("!!!!!!batch delete result: {:?}", r);

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                let mut table_kv_list = Vec::new();
                for key in 0..10u8 {
                    table_kv_list.push(TableKV {
                        table: table_name.clone(),
                        key: Binary::new(key.to_le_bytes().to_vec()),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    });
                }
                let r = tr.upsert(table_kv_list).await;
                println!("!!!!!!batch upsert, result: {:?}", r);

                rt_copy.wait_timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.wait_timeout(1500).await;
                println!("");

                match tr.prepare_modified().await {
                    Err(e) => {
                        println!("prepare failed, reason: {:?}", e);
                        if let Err(e) = tr.rollback_modified().await {
                            println!("rollback failed, reason: {:?}", e);
                        } else {
                            println!("rollback ok for prepare");
                        }
                    },
                    Ok(output) => {
                        println!("prepare ok, output: {:?}", output);
                        match tr.commit_modified(output).await {
                            Err(e) => {
                                println!("commit failed, reason: {:?}", e);
                                if let ErrorLevel::Fatal = &e.level() {
                                    println!("rollback failed, reason: commit fatal error");
                                } else {
                                    println!("rollbakc ok for commit");
                                }
                            },
                            Ok(()) => {
                                println!("commit ok");
                            },
                        }
                    },
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

