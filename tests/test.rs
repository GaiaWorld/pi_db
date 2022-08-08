use std::convert::TryInto;
use std::io::Result as IOResult;
use std::time::Instant;

use futures::{future::{FutureExt, BoxFuture},
              stream::{StreamExt, BoxStream}};
use crossbeam_channel::{unbounded, bounded};
use bytes::BufMut;
use env_logger;

use pi_atom::Atom;
use pi_guid::{GuidGen, Guid};
use pi_sinfo::EnumType;
use pi_bon::{WriteBuffer, ReadBuffer, Encode, Decode, ReadBonErr};
use pi_time::run_nanos;
use pi_async::rt::{AsyncRuntime, multi_thread::MultiTaskRuntimeBuilder};
use pi_async_transaction::{AsyncCommitLog, ErrorLevel, manager_2pc::Transaction2PcManager, Transaction2Pc};
use futures::future::err;
use pi_store::commit_logger::{CommitLoggerBuilder, CommitLogger};

use pi_db::{Binary,
            KVDBTableType,
            KVTableMeta,
            db::KVDBManagerBuilder,
            tables::TableKV,
            inspector::{CommitLogInspector, LogTableInspector}};

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
                rt_copy.timeout(1500).await;
                println!("");

                println!("!!!!!!test_memory is exist: {:?}", db.is_exist(&table_name).await);
                println!("!!!!!!test_memory is ordered table: {:?}", db.is_ordered_table(&table_name).await);
                println!("!!!!!!test_memory is persistent table: {:?}", db.is_persistent_table(&table_name).await);
                println!("!!!!!!test_memory table_dir: {:?}", db.table_path(&table_name).await);
                println!("!!!!!!test_memory table len: {:?}", db.table_record_size(&table_name).await);

                //操作数据库事务
                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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
fn test_memory_table_conflict() {
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
                                                                 EnumType::Usize,
                                                                 EnumType::Usize)).await {
                    //创建有序内存表失败
                    println!("!!!!!!create memory ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;

                println!("!!!!!!db table size: {:?}", db.table_size().await);

                //查询表信息
                rt_copy.timeout(1500).await;
                println!("");

                println!("!!!!!!test_memory is exist: {:?}", db.is_exist(&table_name).await);
                println!("!!!!!!test_memory is ordered table: {:?}", db.is_ordered_table(&table_name).await);
                println!("!!!!!!test_memory is persistent table: {:?}", db.is_persistent_table(&table_name).await);
                println!("!!!!!!test_memory table_dir: {:?}", db.table_path(&table_name).await);
                println!("!!!!!!test_memory table len: {:?}", db.table_record_size(&table_name).await);

                //操作数据库事务
                rt_copy.timeout(1500).await;
                println!("");

                {
                    let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();

                    let _r = tr.upsert(vec![
                        TableKV {
                            table: table_name.clone(),
                            key: usize_to_binary(0),
                            value: Some(Binary::new(0usize.to_le_bytes().to_vec()))
                        }
                    ]).await;

                    if let Ok(output) = tr.prepare_modified().await {
                        tr.commit_modified(output).await.is_ok();
                    }
                }

                let (sender, receiver) = unbounded();
                let start = Instant::now();
                for _ in 0..1000 {
                    let rt_copy_ = rt_copy.clone();
                    let db_copy = db.clone();
                    let table_name_copy = table_name.clone();
                    let sender_copy = sender.clone();

                    rt_copy.spawn(rt_copy.alloc(), async move {
                        let now = Instant::now();
                        let mut is_ok = false;

                        while now.elapsed().as_millis() <= 60000 {
                            let tr = db_copy.transaction(Atom::from("test memory table"), true, 500, 500).unwrap();
                            let r = tr.query(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: None
                                }
                            ]).await;
                            let last_value = usize::from_le_bytes(r[0].as_ref().unwrap().as_ref().try_into().unwrap());

                            let new_value = last_value + 1;
                            let _r = tr.upsert(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: Some(Binary::new(new_value.to_le_bytes().to_vec()))
                                }
                            ]).await;

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        rt_copy_.timeout(0);
                                        continue;
                                    }
                                },
                                Ok(output) => {
                                    match tr.commit_modified(output).await {
                                        Err(e) => {
                                            if let ErrorLevel::Fatal = &e.level() {
                                                println!("rollback failed, reason: commit fatal error");
                                            } else {
                                                if let Err(e) = tr.rollback_modified().await {
                                                    println!("rollback failed, reason: {:?}", e);
                                                } else {
                                                    rt_copy_.timeout(0);
                                                    continue;
                                                }
                                            }
                                        },
                                        Ok(()) => {
                                            is_ok = true;
                                            break;
                                        },
                                    }
                                },
                            }
                        }

                        if !is_ok {
                            println!("writed timeout");
                        }

                        sender_copy.send(());
                    });
                }

                let mut count = 0;
                loop {
                    match receiver.recv_timeout(Duration::from_millis(10000)) {
                        Err(e) => {
                            println!(
                                "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                                rt_copy.wait_len(),
                                rt_copy.len(),
                                e
                            );
                            continue;
                        },
                        Ok(_result) => {
                            count += 1;
                            if count >= 1000 {
                                println!("!!!!!!time: {:?}, count: {}", start.elapsed(), count);
                                break;
                            }
                        },
                    }
                }

                {
                    let tr = db.transaction(Atom::from("test memory table"), true, 500, 500).unwrap();

                    let r = tr.query(vec![
                        TableKV {
                            table: table_name.clone(),
                            key: Binary::new(vec![0]),
                            value: None
                        }
                    ]).await;
                    let last_value = usize::from_le_bytes(r[0].as_ref().unwrap().as_ref().try_into().unwrap());

                    assert_eq!(last_value, 1000);
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
                rt_copy.timeout(1500).await;
                println!("");

                println!("!!!!!!test_memory is exist: {:?}", db.is_exist(&table_name).await);
                println!("!!!!!!test_memory is ordered table: {:?}", db.is_ordered_table(&table_name).await);
                println!("!!!!!!test_memory is persistent table: {:?}", db.is_persistent_table(&table_name).await);
                println!("!!!!!!test_memory table_dir: {:?}", db.table_path(&table_name).await);
                println!("!!!!!!test_memory table len: {:?}", db.table_record_size(&table_name).await);

                //操作数据库事务
                rt_copy.timeout(1500).await;
                println!("");

                let tr = db.transaction(Atom::from("test memory table"), true, 500, 500).unwrap();
                let r = tr.query(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: u8_to_binary(0),
                        value: None
                    }
                ]).await;
                println!("!!!!!!query result: {:?}", r);

                let r = tr.upsert(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: u8_to_binary(0),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    }
                ]).await;
                println!("!!!!!!upsert result: {:?}", r);

                let r = tr.query(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: u8_to_binary(0),
                        value: None
                    }
                ]).await;
                println!("!!!!!!query result: {:?}", r);

                let r = tr.delete(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: u8_to_binary(0),
                        value: None
                    }
                ]).await;
                println!("!!!!!!delete result: {:?}", r);

                let r = tr.query(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: u8_to_binary(0),
                        value: None
                    }
                ]).await;
                println!("!!!!!!query result: {:?}", r);

                let mut table_kv_list = Vec::new();
                for key in 0..10u8 {
                    table_kv_list.push(TableKV {
                        table: table_name.clone(),
                        key: u8_to_binary(key),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    });
                }
                let r = tr.upsert(table_kv_list).await;
                println!("!!!!!!batch upsert, result: {:?}", r);

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    Some(u8_to_binary(6)),
                    false
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    Some(u8_to_binary(6)),
                    true
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", u8::from_le_bytes(key.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 binary_to_u8(&key),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    true
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 binary_to_u8(&key),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    Some(u8_to_binary(6)),
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 binary_to_u8(&key),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    Some(u8_to_binary(6)),
                    true
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 binary_to_u8(&key),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                let mut table_kv_list = Vec::new();
                for key in 0..10u8 {
                    table_kv_list.push(TableKV {
                        table: table_name.clone(),
                        key: u8_to_binary(key),
                        value: None,
                    });
                }
                let r = tr.delete(table_kv_list).await;
                println!("!!!!!!batch delete result: {:?}", r);

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 binary_to_u8(&key),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.timeout(1500).await;
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
                rt_copy.timeout(1500).await;
                println!("");

                println!("!!!!!!test_log is exist: {:?}", db.is_exist(&table_name).await);
                println!("!!!!!!test_log is ordered table: {:?}", db.is_ordered_table(&table_name).await);
                println!("!!!!!!test_log is persistent table: {:?}", db.is_persistent_table(&table_name).await);
                println!("!!!!!!test_log table_dir: {:?}", db.table_path(&table_name).await);
                println!("!!!!!!test_log table len: {:?}", db.table_record_size(&table_name).await);

                //操作数据库事务
                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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
                        key: u8_to_binary(key),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    });
                }
                let r = tr.upsert(table_kv_list).await;
                println!("!!!!!!batch upsert, result: {:?}", r);

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", binary_to_u8(&key));
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    None,
                    true
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", binary_to_u8(&key));
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    Some(u8_to_binary(6)),
                    false
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", binary_to_u8(&key));
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    Some(u8_to_binary(6)),
                    true
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", binary_to_u8(&key));
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 binary_to_u8(&key),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    true
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 binary_to_u8(&key),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    Some(u8_to_binary(6)),
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 binary_to_u8(&key),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    Some(u8_to_binary(6)),
                    true
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 binary_to_u8(&key),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                let mut table_kv_list = Vec::new();
                for key in 0..10u8 {
                    table_kv_list.push(TableKV {
                        table: table_name.clone(),
                        key: u8_to_binary(key),
                        value: None,
                    });
                }
                let r = tr.delete(table_kv_list).await;
                println!("!!!!!!batch delete result: {:?}", r);

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.keys(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some(key) = r.next().await {
                        println!("!!!!!!next key: {:?}", binary_to_u8(&key));
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 binary_to_u8(&key),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                let mut table_kv_list = Vec::new();
                for key in 0..10u8 {
                    table_kv_list.push(TableKV {
                        table: table_name.clone(),
                        key: u8_to_binary(key),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    });
                }
                let r = tr.upsert(table_kv_list).await;
                println!("!!!!!!batch upsert, result: {:?}", r);

                rt_copy.timeout(1500).await;
                println!("");

                if let Some(mut r) = tr.values(
                    table_name.clone(),
                    None,
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!next key: {:?}, value: {:?}",
                                 binary_to_u8(&key),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.timeout(1500).await;
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
fn test_log_table_read_only_while_writing() {
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
                rt_copy.timeout(1500).await;
                println!("");

                println!("!!!!!!test_log is exist: {:?}", db.is_exist(&table_name).await);
                println!("!!!!!!test_log is ordered table: {:?}", db.is_ordered_table(&table_name).await);
                println!("!!!!!!test_log is persistent table: {:?}", db.is_persistent_table(&table_name).await);
                println!("!!!!!!test_log table_dir: {:?}", db.table_path(&table_name).await);
                println!("!!!!!!test_log table len: {:?}", db.table_record_size(&table_name).await);

                //操作数据库事务
                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
                println!("");

                let r = tr.query(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: Binary::new(vec![0]),
                        value: None
                    }
                ]).await;
                println!("!!!!!!query result: {:?}", r);

                rt_copy.timeout(1500).await;
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
fn test_log_table_conflict() {
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

                let table_name = Atom::from("test_log");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize)).await {
                    //创建有序内存表失败
                    println!("!!!!!!create log ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;

                println!("!!!!!!db table size: {:?}", db.table_size().await);

                //查询表信息
                rt_copy.timeout(1500).await;
                println!("");

                println!("!!!!!!test_log is exist: {:?}", db.is_exist(&table_name).await);
                println!("!!!!!!test_log is ordered table: {:?}", db.is_ordered_table(&table_name).await);
                println!("!!!!!!test_log is persistent table: {:?}", db.is_persistent_table(&table_name).await);
                println!("!!!!!!test_log table_dir: {:?}", db.table_path(&table_name).await);
                println!("!!!!!!test_log table len: {:?}", db.table_record_size(&table_name).await);

                //操作数据库事务
                rt_copy.timeout(1500).await;
                println!("");

                {
                    let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();

                    let _r = tr.upsert(vec![
                        TableKV {
                            table: table_name.clone(),
                            key: usize_to_binary(0),
                            value: Some(Binary::new(0usize.to_le_bytes().to_vec()))
                        }
                    ]).await;

                    if let Ok(output) = tr.prepare_modified().await {
                        tr.commit_modified(output).await.is_ok();
                    }
                }

                let (sender, receiver) = unbounded();
                let start = Instant::now();
                for _ in 0..1000 {
                    let rt_copy_ = rt_copy.clone();
                    let db_copy = db.clone();
                    let table_name_copy = table_name.clone();
                    let sender_copy = sender.clone();

                    rt_copy.spawn(rt_copy.alloc(), async move {
                        let now = Instant::now();
                        let mut is_ok = false;

                        while now.elapsed().as_millis() <= 60000 {
                            let tr = db_copy.transaction(Atom::from("test log table"), true, 500, 500).unwrap();
                            let r = tr.query(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: None
                                }
                            ]).await;
                            let last_value = usize::from_le_bytes(r[0].as_ref().unwrap().as_ref().try_into().unwrap());

                            let new_value = last_value + 1;
                            let _r = tr.upsert(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: Some(Binary::new(new_value.to_le_bytes().to_vec()))
                                }
                            ]).await;

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        rt_copy_.timeout(0).await;
                                        continue;
                                    }
                                },
                                Ok(output) => {
                                    match tr.commit_modified(output).await {
                                        Err(e) => {
                                            if let ErrorLevel::Fatal = &e.level() {
                                                println!("rollback failed, reason: commit fatal error");
                                            } else {
                                                if let Err(e) = tr.rollback_modified().await {
                                                    println!("rollback failed, reason: {:?}", e);
                                                } else {
                                                    rt_copy_.timeout(0).await;
                                                    continue;
                                                }
                                            }
                                        },
                                        Ok(()) => {
                                            is_ok = true;
                                            break;
                                        },
                                    }
                                },
                            }
                        }

                        if !is_ok {
                            println!("writed timeout");
                        }

                        sender_copy.send(());
                    });
                }

                let mut count = 0;
                loop {
                    match receiver.recv_timeout(Duration::from_millis(10000)) {
                        Err(e) => {
                            println!(
                                "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                                rt_copy.wait_len(),
                                rt_copy.len(),
                                e
                            );
                            continue;
                        },
                        Ok(_result) => {
                            count += 1;
                            if count >= 1000 {
                                println!("!!!!!!time: {:?}, count: {}", start.elapsed(), count);
                                break;
                            }
                        },
                    }
                }

                {
                    let tr = db.transaction(Atom::from("test log table"), true, 500, 500).unwrap();

                    let r = tr.query(vec![
                        TableKV {
                            table: table_name.clone(),
                            key: usize_to_binary(0),
                            value: None
                        }
                    ]).await;
                    let last_value = usize::from_le_bytes(r[0].as_ref().unwrap().as_ref().try_into().unwrap());

                    assert_eq!(last_value, 1000);
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_write_table() {
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

                let table_name = Atom::from("test_log_write");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogWTab,
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
                rt_copy.timeout(1500).await;
                println!("");

                println!("!!!!!!test_log_write is exist: {:?}", db.is_exist(&table_name).await);
                println!("!!!!!!test_log_write is ordered table: {:?}", db.is_ordered_table(&table_name).await);
                println!("!!!!!!test_log_write is persistent table: {:?}", db.is_persistent_table(&table_name).await);
                println!("!!!!!!test_log_write table_dir: {:?}", db.table_path(&table_name).await);
                println!("!!!!!!test_log_write table len: {:?}", db.table_record_size(&table_name).await);

                //操作数据库事务
                rt_copy.timeout(1500).await;
                println!("");

                let tr = db.transaction(Atom::from("test log table"), true, 500, 500).unwrap();

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

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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

                rt_copy.timeout(1500).await;
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
fn test_db_repair() {
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
                panic!("!!!!!!startup db failed, reason: {:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name0 = Atom::from("test_log0");
                let table_name1 = Atom::from("test_log1");
                let tr = db.transaction(Atom::from("test db repair"), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name0.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::U8,
                                                                 EnumType::Usize)).await {
                    //创建有序内存表失败
                    println!("!!!!!!create log ordered table failed, reason: {:?}", e);
                }
                if let Err(e) = tr.create_table(table_name1.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::U8,
                                                                 EnumType::Usize)).await {
                    //创建有序内存表失败
                    println!("!!!!!!create log ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;

                println!("!!!!!!db table size: {:?}", db.table_size().await);

                //操作数据库事务
                rt_copy.timeout(5000).await;
                println!("");

                let tr = db.transaction(Atom::from("test log table"), true, 500, 500).unwrap();

                if let Some(mut r) = tr.values(
                    table_name0.clone(),
                    None,
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!table: {:?}, next key: {:?}, value: {:?}",
                                 table_name0.clone(),
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 usize::from_le_bytes(value.as_ref().try_into().unwrap()));
                    }
                }
                if let Some(mut r) = tr.values(
                    table_name1.clone(),
                    None,
                    false
                ).await {
                    while let Some((key, value)) = r.next().await {
                        println!("!!!!!!table: {:?}, next key: {:?}, value: {:?}",
                                 table_name1.clone(),
                                 u8::from_le_bytes(key.as_ref().try_into().unwrap()),
                                 usize::from_le_bytes(value.as_ref().try_into().unwrap()));
                    }
                }

                rt_copy.timeout(10000).await;
                println!("");

                let mut table_kv_list = Vec::new();
                for index in 0..10u8 {
                    let vec = tr.query(vec![TableKV {
                        table: table_name0.clone(),
                        key: Binary::new(index.to_le_bytes().to_vec()),
                        value: None
                    }]).await;

                    if let Some(last_value) = &vec[0] {
                        //有值，则加1
                        let last_value = usize::from_le_bytes(last_value.as_ref().try_into().unwrap());
                        table_kv_list.push(TableKV {
                            table: table_name0.clone(),
                            key: Binary::new(index.to_le_bytes().to_vec()),
                            value: Some(Binary::new((last_value + 1).to_le_bytes().to_vec()))
                        });
                    } else {
                        //无值，则初始化
                        table_kv_list.push(TableKV {
                            table: table_name0.clone(),
                            key: Binary::new(index.to_le_bytes().to_vec()),
                            value: Some(Binary::new((index as usize * 1000000).to_le_bytes().to_vec()))
                        });
                    }

                    let vec = tr.query(vec![TableKV {
                        table: table_name1.clone(),
                        key: Binary::new(index.to_le_bytes().to_vec()),
                        value: None
                    }]).await;

                    if let Some(last_value) = &vec[0] {
                        //有值，则加1
                        let last_value = usize::from_le_bytes(last_value.as_ref().try_into().unwrap());
                        table_kv_list.push(TableKV {
                            table: table_name1.clone(),
                            key: Binary::new(index.to_le_bytes().to_vec()),
                            value: Some(Binary::new((last_value + 1).to_le_bytes().to_vec()))
                        });
                    } else {
                        //无值，则初始化
                        table_kv_list.push(TableKV {
                            table: table_name1.clone(),
                            key: Binary::new(index.to_le_bytes().to_vec()),
                            value: Some(Binary::new((index as usize * 1000000).to_le_bytes().to_vec()))
                        });
                    }
                }
                let r = tr.upsert(table_kv_list).await;
                println!("!!!!!!batch upsert, result: {:?}", r);

                rt_copy.timeout(1500).await;
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
                                println!("commit ok, commit_uid: {:?}", tr.get_commit_uid());
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
fn test_multi_tables_repair() {
    use std::thread;
    use std::time::{Duration, Instant};

    env_logger::init();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
    rt.spawn(rt.alloc(), async move {
        let guid_gen = GuidGen::new(run_nanos(), 0);
        let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), "./.commit_log");
        let commit_logger = commit_logger_builder
            .log_file_limit(1024)
            .build()
            .await
            .unwrap();

        let tr_mgr = Transaction2PcManager::new(rt_copy.clone(),
                                                guid_gen,
                                                commit_logger);

        let builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./db");
        match builder.startup().await {
            Err(e) => {
                println!("!!!!!!startup db failed, reason: {:?}", e);
            },
            Ok(db) => {
                for index in 0..10 {
                    let tr = db.transaction(Atom::from("test_log"), true, 500, 500).unwrap();
                    if let Err(e) = tr.create_table(Atom::from("test_log".to_string() + index.to_string().as_str()),
                                                    KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                     true,
                                                                     EnumType::Usize,
                                                                     EnumType::Usize)).await {
                        //创建有序日志表失败
                        println!("!!!!!!create log ordered table failed, reason: {:?}", e);
                    }
                    let output = tr.prepare_modified().await.unwrap();
                    let _ = tr.commit_modified(output).await;
                }
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                sender.send(db);
            },
        }
    });

    let db = receiver.recv().unwrap();
    let mut table_names = Vec::new();
    for index in 0..10 {
        table_names.push(Atom::from("test_log".to_string() + index.to_string().as_str()));
    }

    let db_copy = db.clone();
    let table_names_copy = table_names.clone();
    let (sender, receiver) = bounded(1);
    rt.spawn(rt.alloc(), async move  {
        for table_name in &table_names_copy {
            let tr = db_copy.transaction(table_name.clone(), true, 500, 500).unwrap();

            //检查所有有序日志表
            let mut count = 0;
            let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
            while let Some((key, value)) = values.next().await {
                let val = usize::from_le_bytes(value.as_ref().try_into().unwrap());
                if val != 100 {
                    println!("Check value failed, key: {}, value: {}", binary_to_usize(&key).unwrap(), val);
                }
                count += 1;
            }
            if count != 0 && count != 10 {
                println!("Check key amount failed, table: {:?}, count: {}", table_name, count);
            }

            //重置所有有序日志表
            for index in 0..10 {
                let _r = tr.upsert(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: usize_to_binary(index),
                        value: Some(Binary::new(0usize.to_le_bytes().to_vec()))
                    }
                ]).await;
            }

            if let Ok(output) = tr.prepare_modified().await {
                tr.commit_modified(output).await.unwrap();
            }
        }

        sender.send(());
    });

    receiver.recv().unwrap();
    println!("!!!!!!ready write...");
    thread::sleep(Duration::from_millis(5000));
    println!("!!!!!!start write");

    let (sender, receiver) = unbounded();
    let start = Instant::now();
    for _ in 0..100 {
        for index in 0..10 {
            let rt_copy = rt.clone();
            let db_copy = db.clone();
            let table_names_copy = table_names.clone();
            let sender_copy = sender.clone();

            rt.spawn(rt.alloc(), async move {
                let mut result = true;
                let now = Instant::now();
                while now.elapsed().as_millis() < 5000 {
                    let tr = db_copy.transaction(Atom::from("Test multi table repair"), true, 500, 500).unwrap();

                    for table_name in &table_names_copy {
                        let key = usize_to_binary(index);
                        let r = tr.query(vec![TableKV::new(table_name.clone(), key.clone(), None)]).await;
                        let last_value = usize::from_le_bytes(r[0].as_ref().unwrap().as_ref().try_into().unwrap());
                        let new_last_value = last_value + 1;

                        tr.delete(vec![TableKV::new(table_name.clone(), key.clone(), None)]);
                        tr.upsert(vec![TableKV::new(table_name.clone(), key, Some(Binary::new(new_last_value.to_le_bytes().to_vec())))]).await.unwrap();
                    }

                    match tr.prepare_modified().await {
                        Err(e) => {
                            if let ErrorLevel::Normal = e.level() {
                                tr.rollback_modified().await.unwrap();
                            }
                            rt_copy.timeout(0).await;
                            continue;
                        },
                        Ok(output) => {
                            tr.commit_modified(output).await.unwrap();
                            result = false;
                            break;
                        },
                    }
                }

                sender_copy.send(result);
            });
        }
    }

    let mut count = 0;
    let mut err_count = 0;
    loop {
        match receiver.recv_timeout(Duration::from_millis(10000)) {
            Err(e) => {
                println!(
                    "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                    rt.wait_len(),
                    rt.len(),
                    e
                );
                continue;
            },
            Ok(result) => {
                if result {
                    err_count += 1;
                } else {
                    count += 1;
                }

                if count + err_count >= 1000 {
                    println!("!!!!!!time: {:?}, ok: {}, error: {}", start.elapsed(), count, err_count);
                    break;
                }
            },
        }
    }

    thread::sleep(Duration::from_millis(1000000000));
}

// 先执行test_multi_tables_repair
#[test]
fn test_commit_log_inspector() {
    use std::thread;
    use std::time::Duration;

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), "./.commit_log");
        let commit_logger = commit_logger_builder
            .log_file_limit(1024)
            .build()
            .await
            .unwrap();

        let inspector = CommitLogInspector::new(rt_copy, commit_logger);
        if inspector.begin() {
            while let Some(result) = inspector.next() {
                if result.2.as_str() == ".tables_meta" {
                    //元信息表
                    let key = String::from_utf8(result.4).unwrap();
                    let value = String::from_utf8(result.5).unwrap();
                    println!("Inspect next, tid: {}, cid: {}, table: {}, method: {}, key: {}, value: {}", result.0, result.1, result.2, result.3, key, value);
                } else {
                    //用户表
                    // if result.2.as_str() == "config/db/Record.DramaNumberRecord" {
                    //     let key = binary_to_usize(&Binary::new(result.4)).unwrap();
                    //     if key == 112800000 {
                    //         println!("Inspect next, tid: {}, cid: {}, table: {}, method: {}, key: {}, value: {:?}", result.0, result.1, result.2, result.3, key, result.5);
                    //     }
                    // }
                    let key = binary_to_usize(&Binary::new(result.4)).unwrap();
                    let value = usize::from_le_bytes(result.5.as_slice().try_into().unwrap());
                    println!("Inspect next, tid: {}, cid: {}, table: {}, method: {}, key: {}, value: {}", result.0, result.1, result.2, result.3, key, value);
                }
            }
            println!("Inspect finish");
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 先执行test_multi_tables_repair，并等待写入表文件
#[test]
fn test_log_table_inspector() {
    use std::thread;
    use std::time::Duration;

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let inspector = LogTableInspector::new(rt_copy, "./db/.tables/config/db/Record.DramaNumberRecord").unwrap();
        if inspector.begin() {
            while let Some(result) = inspector.next() {
                // let key = binary_to_usize(&Binary::new(result.2)).unwrap();
                // if key == 112800000 {
                //     println!("Inspect next, file: {}, method: {}, key: {}, value: {:?}", result.0, result.1, key, result.3);
                // }
                let key = binary_to_usize(&Binary::new(result.2)).unwrap();
                let value = usize::from_le_bytes(result.3.as_slice().try_into().unwrap());
                println!("Inspect next, file: {}, method: {}, key: {}, value: {}", result.0, result.1, key, value);
            }
            println!("Inspect finish");
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 分阶段写多个表，并在最后写入后，不等待同步直接关闭进程，在重启时修复，并检查修复数据是否成功
// 也可以使用test_commit_log_inspector或test_log_table_inspector进行侦听
#[test]
fn test_multi_tables_write_and_repair() {
    use std::thread;
    use std::time::{Duration, Instant};

    env_logger::init();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
    rt.spawn(rt.alloc(), async move {
        let guid_gen = GuidGen::new(run_nanos(), 0);
        let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), "./.commit_log");
        let commit_logger = commit_logger_builder
            .log_file_limit(1024)
            .build()
            .await
            .unwrap();

        let tr_mgr = Transaction2PcManager::new(rt_copy.clone(),
                                                guid_gen,
                                                commit_logger);

        let builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./db");
        match builder.startup().await {
            Err(e) => {
                println!("!!!!!!startup db failed, reason: {:?}", e);
            },
            Ok(db) => {
                for index in 0..10 {
                    let tr = db.transaction(Atom::from("test_log"), true, 500, 500).unwrap();
                    if let Err(e) = tr.create_table(Atom::from("test_log".to_string() + index.to_string().as_str()),
                                                    KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                     true,
                                                                     EnumType::Usize,
                                                                     EnumType::Usize)).await {
                        //创建有序日志表失败
                        println!("!!!!!!create log ordered table failed, reason: {:?}", e);
                    }
                    let output = tr.prepare_modified().await.unwrap();
                    let _ = tr.commit_modified(output).await;
                }
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                sender.send(db);
            },
        }
    });

    let db = receiver.recv().unwrap();
    let mut table_names = Vec::new();
    for index in 0..10 {
        table_names.push(Atom::from("test_log".to_string() + index.to_string().as_str()));
    }

    let db_copy = db.clone();
    let table_names_copy = table_names.clone();
    let (sender, receiver) = bounded(1);
    rt.spawn(rt.alloc(), async move  {
        for table_name in &table_names_copy {
            let tr = db_copy.transaction(table_name.clone(), true, 500, 500).unwrap();

            //检查所有有序日志表
            let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
            while let Some((key, value)) = values.next().await {
                let val = usize::from_le_bytes(value.as_ref().try_into().unwrap());
                println!("!!!!!!table: {}, key: {}, value: {}", table_name.as_str(), binary_to_usize(&key).unwrap(), val);
            }

            if let Ok(output) = tr.prepare_modified().await {
                tr.commit_modified(output).await.unwrap();
            }
        }

        sender.send(());
    });

    receiver.recv().unwrap();
    println!("!!!!!!ready write...");
    thread::sleep(Duration::from_millis(5000));
    println!("!!!!!!start write");

    let (sender, receiver) = unbounded();
    let start = Instant::now();
    for _ in 0..100 {
        for index in 0..10 {
            let rt_copy = rt.clone();
            let db_copy = db.clone();
            let table_names_copy = table_names.clone();
            let sender_copy = sender.clone();

            rt.spawn(rt.alloc(), async move {
                let mut result = true;
                let now = Instant::now();
                while now.elapsed().as_millis() < 5000 {
                    let tr = db_copy.transaction(Atom::from("Test multi table repair"), true, 500, 500).unwrap();

                    for table_name in &table_names_copy {
                        let key = usize_to_binary(index);
                        let r = tr.query(vec![TableKV::new(table_name.clone(), key.clone(), None)]).await;
                        let new_last_value = if r[0].is_none() {
                            0
                        } else {
                            let last_value = usize::from_le_bytes(r[0].as_ref().unwrap().as_ref().try_into().unwrap());
                            last_value + 1
                        };

                        tr.upsert(vec![TableKV::new(table_name.clone(), key, Some(Binary::new(new_last_value.to_le_bytes().to_vec())))]).await.unwrap();
                    }

                    match tr.prepare_modified().await {
                        Err(e) => {
                            if let ErrorLevel::Normal = e.level() {
                                tr.rollback_modified().await.unwrap();
                            }
                            rt_copy.timeout(0).await;
                            continue;
                        },
                        Ok(output) => {
                            tr.commit_modified(output).await;
                            result = false;
                            break;
                        },
                    }
                }

                sender_copy.send(result);
            });
        }
    }

    let mut count = 0;
    let mut err_count = 0;
    loop {
        match receiver.recv_timeout(Duration::from_millis(10000)) {
            Err(e) => {
                println!(
                    "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                    rt.wait_len(),
                    rt.len(),
                    e
                );
                continue;
            },
            Ok(result) => {
                if result {
                    err_count += 1;
                } else {
                    count += 1;
                }

                if count + err_count >= 1000 {
                    println!("!!!!!!time: {:?}, ok: {}, error: {}", start.elapsed(), count, err_count);
                    break;
                }
            },
        }
    }

    println!("!!!!!!ready sync...");
    thread::sleep(Duration::from_millis(60000));
    println!("!!!!!!sync finish");

    let (sender, receiver) = unbounded();
    let start = Instant::now();
    for _ in 0..1 {
        for index in 0..10 {
            let rt_copy = rt.clone();
            let db_copy = db.clone();
            let table_names_copy = table_names.clone();
            let sender_copy = sender.clone();

            rt.spawn(rt.alloc(), async move {
                let mut result = true;
                let now = Instant::now();
                while now.elapsed().as_millis() < 5000 {
                    let tr = db_copy.transaction(Atom::from("Test multi table repair"), true, 500, 500).unwrap();

                    for table_name in &table_names_copy {
                        let key = usize_to_binary(index);
                        let r = tr.query(vec![TableKV::new(table_name.clone(), key.clone(), None)]).await;
                        let new_last_value = if r[0].is_none() {
                            0
                        } else {
                            let last_value = usize::from_le_bytes(r[0].as_ref().unwrap().as_ref().try_into().unwrap());
                            last_value + 1
                        };

                        tr.upsert(vec![TableKV::new(table_name.clone(), key, Some(Binary::new(new_last_value.to_le_bytes().to_vec())))]).await.unwrap();
                    }

                    match tr.prepare_modified().await {
                        Err(e) => {
                            if let ErrorLevel::Normal = e.level() {
                                tr.rollback_modified().await.unwrap();
                            }
                            rt_copy.timeout(0).await;
                            continue;
                        },
                        Ok(output) => {
                            tr.commit_modified(output).await;
                            result = false;
                            break;
                        },
                    }
                }

                sender_copy.send(result);
            });
        }
    }

    let mut count = 0;
    let mut err_count = 0;
    loop {
        match receiver.recv_timeout(Duration::from_millis(10000)) {
            Err(e) => {
                println!(
                    "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                    rt.wait_len(),
                    rt.len(),
                    e
                );
                continue;
            },
            Ok(result) => {
                if result {
                    err_count += 1;
                } else {
                    count += 1;
                }

                if count + err_count >= 10 {
                    println!("!!!!!!time: {:?}, ok: {}, error: {}", start.elapsed(), count, err_count);
                    break;
                }
            },
        }
    }
}

// 分阶段写多个表，并在最后写入后，不等待同步直接关闭进程，在重启时修复，并检查修复数据是否成功
// 类似test_multi_tables_write_and_repair，但值不是累加
#[test]
fn test_multi_tables_write_and_repair1() {
    use std::thread;
    use std::time::{Duration, Instant};

    env_logger::init();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
    rt.spawn(rt.alloc(), async move {
        let guid_gen = GuidGen::new(run_nanos(), 0);
        let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), "./.commit_log");
        let commit_logger = commit_logger_builder
            .log_file_limit(1024)
            .build()
            .await
            .unwrap();

        let tr_mgr = Transaction2PcManager::new(rt_copy.clone(),
                                                guid_gen,
                                                commit_logger);

        let builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./db");
        match builder.startup().await {
            Err(e) => {
                println!("!!!!!!startup db failed, reason: {:?}", e);
            },
            Ok(db) => {
                for index in 0..10 {
                    let tr = db.transaction(Atom::from("test_log"), true, 500, 500).unwrap();
                    if let Err(e) = tr.create_table(Atom::from("test_log".to_string() + index.to_string().as_str()),
                                                    KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                     true,
                                                                     EnumType::Usize,
                                                                     EnumType::Usize)).await {
                        //创建有序日志表失败
                        println!("!!!!!!create log ordered table failed, reason: {:?}", e);
                    }
                    let output = tr.prepare_modified().await.unwrap();
                    let _ = tr.commit_modified(output).await;
                }
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                sender.send(db);
            },
        }
    });

    let db = receiver.recv().unwrap();
    let mut table_names = Vec::new();
    for index in 0..10 {
        table_names.push(Atom::from("test_log".to_string() + index.to_string().as_str()));
    }

    let db_copy = db.clone();
    let table_names_copy = table_names.clone();
    let (sender, receiver) = bounded(1);
    rt.spawn(rt.alloc(), async move  {
        for table_name in &table_names_copy {
            let tr = db_copy.transaction(table_name.clone(), true, 500, 500).unwrap();

            //检查所有有序日志表
            let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
            while let Some((key, value)) = values.next().await {
                let val = usize::from_le_bytes(value.as_ref().try_into().unwrap());
                println!("!!!!!!table: {}, key: {}, value: {}", table_name.as_str(), binary_to_usize(&key).unwrap(), val);
            }

            if let Ok(output) = tr.prepare_modified().await {
                tr.commit_modified(output).await.unwrap();
            }
        }

        sender.send(());
    });

    receiver.recv().unwrap();
    println!("!!!!!!ready write...");
    thread::sleep(Duration::from_millis(5000));
    println!("!!!!!!start write");

    let (sender, receiver) = unbounded();
    let start = Instant::now();
    for _ in 0..10 {
        for index in 0..10 {
            let rt_copy = rt.clone();
            let db_copy = db.clone();
            let table_names_copy = table_names.clone();
            let sender_copy = sender.clone();

            rt.spawn(rt.alloc(), async move {
                let mut result = true;
                let now = Instant::now();
                while now.elapsed().as_millis() < 5000 {
                    let tr = db_copy.transaction(Atom::from("Test multi table repair"), true, 500, 500).unwrap();

                    for table_name in &table_names_copy {
                        let key = usize_to_binary(index);
                        let r = tr.query(vec![TableKV::new(table_name.clone(), key.clone(), None)]).await;
                        let new_last_value = if r[0].is_none() {
                            0
                        } else {
                            let last_value = match usize::from_le_bytes(r[0].as_ref().unwrap().as_ref().try_into().unwrap()) {
                                0 => 10,
                                10 => 20,
                                20 => 30,
                                30 => 40,
                                40 => 50,
                                50 => 60,
                                60 => 70,
                                70 => 80,
                                80 => 90,
                                _ => 100,
                            };

                            last_value as usize
                        };

                        tr.upsert(vec![TableKV::new(table_name.clone(), key, Some(Binary::new(new_last_value.to_le_bytes().to_vec())))]).await.unwrap();
                    }

                    match tr.prepare_modified().await {
                        Err(e) => {
                            if let ErrorLevel::Normal = e.level() {
                                tr.rollback_modified().await.unwrap();
                            }
                            rt_copy.timeout(0).await;
                            continue;
                        },
                        Ok(output) => {
                            tr.commit_modified(output).await;
                            result = false;
                            break;
                        },
                    }
                }

                sender_copy.send(result);
            });
        }
    }

    let mut count = 0;
    let mut err_count = 0;
    loop {
        match receiver.recv_timeout(Duration::from_millis(10000)) {
            Err(e) => {
                println!(
                    "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                    rt.wait_len(),
                    rt.len(),
                    e
                );
                continue;
            },
            Ok(result) => {
                if result {
                    err_count += 1;
                } else {
                    count += 1;
                }

                if count + err_count >= 100 {
                    println!("!!!!!!time: {:?}, ok: {}, error: {}", start.elapsed(), count, err_count);
                    break;
                }
            },
        }
    }

    println!("!!!!!!ready sync...");
    thread::sleep(Duration::from_millis(60000));
    println!("!!!!!!sync finish");
}

// 将u8序列化为二进制数据
fn u8_to_binary(number: u8) -> Binary {
    let mut buffer = WriteBuffer::new();
    number.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

// 将二进制数据反序列化为u8
fn binary_to_u8(bin: &Binary) -> Result<u8, ReadBonErr> {
    let mut buffer = ReadBuffer::new(bin, 0);
    u8::decode(&mut buffer)
}

// 将usize序列化为二进制数据
fn usize_to_binary(number: usize) -> Binary {
    let mut buffer = WriteBuffer::new();
    number.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

// 将二进制数据反序列化为usize
fn binary_to_usize(bin: &Binary) -> Result<usize, ReadBonErr> {
    let mut buffer = ReadBuffer::new(bin, 0);
    usize::decode(&mut buffer)
}

