#![feature(stmt_expr_attributes)]
#![feature(ptr_metadata)]

use std::{mem, thread};
use std::sync::Arc;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::collections::{HashMap,
                       btree_map::{BTreeMap, Entry}};
use std::pin::Pin;
use chrono::format::Item;
use futures::stream::{BoxStream, StreamExt};
use crossbeam_channel::{unbounded, bounded};
use parking_lot::Mutex;
use env_logger;
use futures::Stream;
use pi_atom::Atom;
use pi_guid::{GuidGen, Guid};
use pi_sinfo::EnumType;
use pi_bon::{WriteBuffer, ReadBuffer, Encode, Decode, ReadBonErr};
use pi_time::run_nanos;
use pi_async_rt::rt::{AsyncRuntime, startup_global_time_loop, multi_thread::MultiTaskRuntimeBuilder, AsyncRuntimeBuilder};
use pi_async_transaction::{ErrorLevel, Transaction2Pc,
                           manager_2pc::Transaction2PcManager};
use pi_ordmap::ordmap::OrdMap;
use pi_store::{log_store::log_file::{PairLoader, LogFile, LogMethod},
               commit_logger::CommitLoggerBuilder};
use redb::{Builder, TableDefinition, ReadOnlyTable, ReadableTable, Table};

use pi_db::{Binary,
            KVDBTableType,
            KVTableMeta,
            init_transaction_debug_logger,
            db::{KVDBManagerBuilder, DBStartupRepairMode},
            tables::TableKV,
            inspector::{CommitLogInspector, LogTableInspector}};

#[test]
fn test_table_meta() {
    use std::sync::Arc;
    use pi_sinfo::{EnumType, StructInfo, FieldInfo};

    let name = Atom::from("Hello");
    let name_hash = name.str_hash() as u32;
    let mut struct_info = StructInfo::new(Atom::from("Hello"), name_hash);
    let mut map = HashMap::new();
    map.insert(Atom::from("default"), Atom::from(""));
    let field_info = FieldInfo {
        name: Atom::from("name"),
        ftype: EnumType::Str,
        notes: Some(map),
        is_ignore_name: false,
        is_ignore_ftype: false,
        is_ignore_notes: true,
    };
    struct_info.fields = vec![field_info];
    let meta0 = EnumType::Struct(Arc::new(struct_info));

    let mut struct_info = StructInfo::new(Atom::from("Hello"), name_hash);
    let mut map = HashMap::new();
    map.insert(Atom::from("default"), Atom::from("hello"));
    let field_info = FieldInfo {
        name: Atom::from("name"),
        ftype: EnumType::Str,
        notes: Some(map),
        is_ignore_name: false,
        is_ignore_ftype: false,
        is_ignore_notes: true,
    };
    struct_info.fields = vec![field_info];
    let meta1 = EnumType::Struct(Arc::new(struct_info));

    assert_eq!(meta0, meta1);
}

#[test]
fn test_ordmap() {
    let mut map: OrdMap<pi_ordmap::asbtree::Tree<usize, usize>> = OrdMap::new(None);
    for _ in 0..1000 {
        if map.insert(100000, 100000) {
            let mut iter: pi_ordmap::asbtree::IterTree<usize, usize> = map.iter(None, false);
            let mut count = 0;
            if let Some(_) = iter.next() {
                count += 1;
            }
            assert_eq!(count, 1);

            map.delete(&100000, false);

            let mut iter: pi_ordmap::asbtree::IterTree<usize, usize> = map.iter(None, false);
            if let Some(_) = iter.next() {
                panic!("invalid delete");
            }
        } else {
            panic!("invalid insert");
        }
    }
}

// 测试并发读写Ordmap后修改Ordmap是否保证了多线程安全
// 测试需要多次长时间测试保证不会异常或崩溃
#[test]
fn test_ordmap_concurrency() {
    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let mut map: Arc<Mutex<OrdMap<pi_ordmap::asbtree::Tree<Binary, Option<Binary>>>>> = Arc::new(Mutex::new(OrdMap::new(None)));

    for _ in 0..10 {
        let rt_copy = rt.clone();
        let mut map_copy = map.clone();
        let _ = rt.spawn(async move {
            loop {
                rt_copy.timeout(100).await;
                for index in 0..1000 {
                    let mut map_mut = map_copy.lock().clone();
                    let _ = map_mut
                        .upsert(usize_to_binary(index),
                                Some(usize_to_binary(index)),
                                false);
                    let mut locked = map_copy.lock();
                    *locked = map_mut;
                }
            }
        });
    }

    loop {
        let rt_copy = rt.clone();
        let mut map_copy = map.clone();
        let map_clone = map_copy.lock().clone(); //必须锁住Clone并在迭代器内部持续握有这个Ordmap的引用，否则会因为Ordmap在迭代器迭代完成前被回收导致段错误
        let iter: pi_ordmap::asbtree::IterTree<Binary, Option<Binary>> = map_copy.lock().clone().iter(None, false);
        let ptr = Box::into_raw(Box::new(iter)) as usize;
        let _ = rt.spawn(async move {
            let _map = map_clone;
            let mut iter = unsafe {
                Box::from_raw(ptr as *mut pi_ordmap::asbtree::IterTree<Binary, Option<Binary>>)
            };

            let mut count = 0;
            for _item in iter {
                rt_copy.timeout(100).await;
                count += 1;
            }
            println!("!!!!!!count: {:?}", count);
        });
        thread::sleep(Duration::from_millis(100));
    }
}

// 测试迭代Ordmap时修改源
// 测试需要多次长时间测试保证不会异常或崩溃
#[test]
fn test_ordmap_keys() {
    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let mut map: Arc<Mutex<OrdMap<pi_ordmap::asbtree::Tree<Binary, Option<Binary>>>>> = Arc::new(Mutex::new(OrdMap::new(None)));

    let db = Builder::new().create("./tests/table.dat").unwrap();
    let tr = db.begin_write().unwrap();
    {
        let mut table = tr.open_table(TableDefinition::<Binary, Binary>::new("$default")).unwrap();
        for index in 0..100 {
            table.insert(usize_to_binary(index),
                         usize_to_binary(index))
                .unwrap();
        }
    }
    tr.commit().unwrap();

    let rt_copy = rt.clone();
    let mut map_copy = map.clone();
    let _ = rt.spawn(async move {
        for index in 0..100 {
            let mut map_mut = map_copy.lock().clone();
            let _ = map_mut
                .upsert(usize_to_binary(index),
                        Some(usize_to_binary(index)),
                        false);
            let mut locked = map_copy.lock();
            *locked = map_mut;
        }
        println!("!!!!!!insert finish");

        rt_copy.timeout(5000).await;
        loop {
            for index in 0..100 {
                let mut map_mut = map_copy.lock().clone(); //必须锁住Clone并保存在局部变量上，否则会因为Ordmap在迭代器迭代完成前被回收导致段错误
                let _ = map_mut
                    .upsert(usize_to_binary(index),
                            Some(usize_to_binary(index)),
                            false);
                let mut locked = map_copy.lock();
                *locked = map_mut;
            }
            rt_copy.timeout(16).await;
        }
    });

    thread::sleep(Duration::from_millis(5000));

    println!("!!!!!!start iterate");
    loop {
        let tr = db.begin_read().unwrap();
        let table: ReadOnlyTable<Binary, Binary> = tr.open_table(TableDefinition::new("$default")).unwrap();
        let map_copy = map.lock().clone();
        let keys = map_copy.keys(None, false);
        for key in keys {
            if let Ok(None) = table.get(key) {
                //记录只在缓存中的关键字
            }
        }
        thread::sleep(Duration::from_millis(1000));
    }
}

#[test]
fn test_memory_table() {
    use std::thread;
    use std::time::Duration;

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_memory");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::MemOrdTab,
                                                                 false,
                                                                 EnumType::U8,
                                                                 EnumType::Str),
                                                true).await {
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
                    Some(u8_to_binary(6u8)),
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
                    Some(u8_to_binary(6u8)),
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
                    Some(u8_to_binary(6u8)),
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
                    Some(u8_to_binary(6u8)),
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

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_memory");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::MemOrdTab,
                                                                 false,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
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
                            value: Some(usize_to_binary(0)),
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

                    let _ = rt_copy.spawn(async move {
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
                            let last_value = binary_to_usize((&r[0]).as_ref().unwrap()).unwrap();

                            let new_value = last_value + 1;
                            let _r = tr.upsert(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: Some(usize_to_binary(new_value)),
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
                            key: usize_to_binary(0),
                            value: None
                        }
                    ]).await;
                    let last_value = binary_to_usize((&r[0]).as_ref().unwrap()).unwrap();

                    assert_eq!(last_value, 1000);
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_memory_table_read_write_delete_iteraton() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log/a/b/c");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::MemOrdTab,
                                                                 false,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
                    //创建有序内存表失败
                    println!("!!!!!!create b-tree ordered table failed, reason: {:?}", e);
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

                let (sender, receiver) = unbounded();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let sender_copy = sender.clone();
                let start = Instant::now();
                let _ = rt_copy.spawn(async move {
                    let index = 100000;
                    for n in 0..3000 {
                        let tr = db_copy.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                        let _r = tr.upsert(vec![
                            TableKV {
                                table: table_name_copy.clone(),
                                key: usize_to_binary(index),
                                value: Some(usize_to_binary(index))
                            }
                        ]).await;
                        match tr.prepare_modified().await {
                            Err(_e) => {
                                if let Err(e) = tr.rollback_modified().await {
                                    println!("rollback failed, reason: {:?}", e);
                                }
                            },
                            Ok(output) => {
                                if let Err(e) = tr.commit_modified(output).await {
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: commit fatal error");
                                    } else {
                                        if let Err(e) = tr.rollback_modified().await {
                                            println!("rollback failed, reason: {:?}", e);
                                        }
                                    }
                                } else {
                                    ()
                                }
                            },
                        }



                        let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                        let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
                        while let Some((key, value)) = values.next().await {
                            assert_eq!(binary_to_usize(&key).unwrap(), binary_to_usize(&value).unwrap())
                        }



                        let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                        let r = tr.query(vec![
                            TableKV {
                                table: table_name.clone(),
                                key: usize_to_binary(index),
                                value: None
                            }
                        ]).await;
                        assert!(r.len() == 1 && binary_to_usize((&r[0]).as_ref().unwrap()).unwrap() == index);



                        let tr = db.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                        let r = tr.delete(vec![
                            TableKV {
                                table: table_name_copy.clone(),
                                key: usize_to_binary(index),
                                value: None
                            }
                        ]).await;
                        assert!(r.is_ok());
                        match tr.prepare_modified().await {
                            Err(_e) => {
                                if let Err(e) = tr.rollback_modified().await {
                                    println!("rollback failed, reason: {:?}", e);
                                }
                            },
                            Ok(output) => {
                                if let Err(e) = tr.commit_modified(output).await {
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: commit fatal error");
                                    } else {
                                        if let Err(e) = tr.rollback_modified().await {
                                            println!("rollback failed, reason: {:?}", e);
                                        }
                                    }
                                } else {
                                    ()
                                }
                            },
                        }
                        // assert_eq!(db_copy.table_record_size(&table_name_copy).await, Some(0));



                        let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                        let r = tr.query(vec![
                            TableKV {
                                table: table_name.clone(),
                                key: usize_to_binary(index),
                                value: None
                            }
                        ]).await;
                        assert!(r.len() == 1 && r[0].is_none());



                        let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                        let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
                        while let Some((key, value)) = values.next().await {
                            panic!("n: {:?}, key: {:?}, value: {:?}", n, binary_to_usize(&key).unwrap(), binary_to_usize(&value).unwrap());
                        }
                    }

                    sender_copy.send(());
                });

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
                            if count >= 1 {
                                println!("======> insert and delete finish, time: {:?}, count: {}", start.elapsed(), count);
                                break;
                            }
                        },
                    }
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

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_memory");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::MemOrdTab,
                                                                 true,
                                                                 EnumType::U8,
                                                                 EnumType::Str),
                                                true).await {
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

//执行test_log_table后再执行
#[test]
fn test_load_log_table() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        let now = Instant::now();
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}, time: {:?}", db.table_size().await, now.elapsed());

                let table_name = Atom::from("test_log");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::U8,
                                                                 EnumType::Str),
                                                true).await {
                    //创建有序内存表失败
                    println!("!!!!!!create log ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;

                println!("!!!!!!db table size: {:?}", db.table_size().await);
                println!("!!!!!!test_log is exist: {:?}", db.is_exist(&table_name).await);
                println!("!!!!!!test_log is ordered table: {:?}", db.is_ordered_table(&table_name).await);
                println!("!!!!!!test_log is persistent table: {:?}", db.is_persistent_table(&table_name).await);
                println!("!!!!!!test_log table_dir: {:?}", db.table_path(&table_name).await);
                println!("!!!!!!test_log table len: {:?}", db.table_record_size(&table_name).await);
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

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::U8,
                                                                 EnumType::Str),
                                                true).await {
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
                                 binary_to_u8(&key),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

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

//测试在可写事务中只读
#[test]
fn test_log_table_read_only_while_writing() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::U8,
                                                                 EnumType::Str),
                                                true).await {
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
                                 binary_to_u8(&key),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                let r = tr.query(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: u8_to_binary(0),
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

// 测试在只读事务中写入
#[test]
fn test_log_table_write_while_read_only() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log");
                let tr = db.transaction(table_name.clone(), false, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::U8,
                                                                 EnumType::Str),
                                                true).await {
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
                                 binary_to_u8(&key),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

                let r = tr.upsert(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: u8_to_binary(0),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec())),
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

// 测试LogTable写冲突
#[test]
fn test_log_table_conflict() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
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
                            value: Some(usize_to_binary(0))
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

                    let _ = rt_copy.spawn(async move {
                        let now = Instant::now();
                        let mut is_ok = false;

                        while now.elapsed().as_millis() <= 120000 {
                            let tr = db_copy.transaction(Atom::from("test log table"), true, 500, 500).unwrap();
                            let r = tr.query(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: None
                                }
                            ]).await;
                            let last_value = binary_to_usize((&r[0]).as_ref().unwrap()).unwrap();

                            let new_value = last_value + 1;
                            let _r = tr.upsert(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: Some(usize_to_binary(new_value))
                                }
                            ]).await;

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        rt_copy_.timeout(1).await;
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
                                                    rt_copy_.timeout(1).await;
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
                    let last_value = binary_to_usize((&r[0]).as_ref().unwrap()).unwrap();

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

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log_write");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogWTab,
                                                                 true,
                                                                 EnumType::U8,
                                                                 EnumType::Str),
                                                true).await {
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
fn test_b_tree_table() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log/a/b/c");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                                                 true,
                                                                 EnumType::U8,
                                                                 EnumType::Str),
                                                true
                ).await {
                    //创建有序B树表失败
                    println!("!!!!!!create b-tree ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;

                println!("!!!!!!db table size: {:?}", db.table_size().await);

                //查询表信息
                rt_copy.timeout(1500).await;
                println!("======0");

                println!("!!!!!!test_log is exist: {:?}", db.is_exist(&table_name).await);
                println!("!!!!!!test_log is ordered table: {:?}", db.is_ordered_table(&table_name).await);
                println!("!!!!!!test_log is persistent table: {:?}", db.is_persistent_table(&table_name).await);
                println!("!!!!!!test_log table_dir: {:?}", db.table_path(&table_name).await);
                println!("!!!!!!test_log table len: {:?}", db.table_record_size(&table_name).await);

                //操作数据库事务
                rt_copy.timeout(1500).await;
                println!("======1");

                let tr = db.transaction(Atom::from("test log table"), true, 500, 500).unwrap();

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
                println!("======2");

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
                println!("======3");

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
                println!("======4");

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
                println!("======5");

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
                println!("======6");

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
                println!("======7");

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
                println!("======8");

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
                println!("======9");

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
                println!("======10");

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
                println!("======11");

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
                println!("======12");

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
                println!("======13");

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
                println!("======14");

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
                println!("======15");

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
                println!("======16");

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

/// 测试 B-tree 表的读、写、删除和迭代操作。
///
/// 该函数初始化数据库，执行一系列基本操作，包括插入数据、查询特定键值、更新数据、删除数据以及遍历所有表项，
/// 并验证这些操作是否正确。
#[test]
fn test_b_tree_table_read_write_delete_iteraton() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log/a/b/c");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
                    //创建有序内存表失败
                    println!("!!!!!!create b-tree ordered table failed, reason: {:?}", e);
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

                let (sender, receiver) = unbounded();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let sender_copy = sender.clone();
                let start = Instant::now();
                let _ = rt_copy.spawn(async move {
                    let tr = db_copy.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();

                    for index in 1000..2000 {
                        let _r = tr.upsert(vec![
                            TableKV {
                                table: table_name_copy.clone(),
                                key: usize_to_binary(index),
                                value: Some(usize_to_binary(index))
                            }
                        ]).await;
                    }

                    match tr.prepare_modified().await {
                        Err(_e) => {
                            if let Err(e) = tr.rollback_modified().await {
                                println!("rollback failed, reason: {:?}", e);
                            }
                        },
                        Ok(output) => {
                            if let Err(e) = tr.commit_modified(output).await {
                                if let ErrorLevel::Fatal = &e.level() {
                                    println!("rollback failed, reason: commit fatal error");
                                } else {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    }
                                }
                            } else {
                                sender_copy.send(());
                            }
                        },
                    }
                });

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
                            if count >= 1 {
                                println!("======> insert finish, time: {:?}, count: {}", start.elapsed(), count);
                                break;
                            }
                        },
                    }
                }

                let start = Instant::now();
                for index in 1000..2000 {
                    let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();

                    let r = tr.query(vec![
                        TableKV {
                            table: table_name.clone(),
                            key: usize_to_binary(index),
                            value: None
                        }
                    ]).await;
                    assert_eq!(binary_to_usize((&r[0]).as_ref().unwrap()).unwrap(), index);
                }
                println!("======> query finish, time: {:?}", start.elapsed());

                let start = Instant::now();
                let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
                let mut index = 1000;
                while let Some((key, value)) = values.next().await {
                    assert_eq!(binary_to_usize(&key).unwrap(), index);
                    assert_eq!(binary_to_usize(&value).unwrap(), index);
                    index += 1;
                }
                assert_eq!(index, 2000);
                println!("======> iterate 0 finish, count: {:?}, time: {:?}", index - 1000, start.elapsed());

                println!("======> wait 65s...");
                rt_copy.timeout(65000).await;
                println!("======> start iterate");

                let start = Instant::now();
                let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
                let mut index = 1000;
                while let Some((key, value)) = values.next().await {
                    assert_eq!(binary_to_usize(&key).unwrap(), index);
                    assert_eq!(binary_to_usize(&value).unwrap(), index);
                    index += 1;
                }
                assert_eq!(index, 2000);
                println!("======> iterate 1 finish, count: {:?}, time: {:?}", index - 1000, start.elapsed());

                let (sender, receiver) = unbounded();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let sender_copy = sender.clone();
                let start = Instant::now();
                let _ = rt_copy.spawn(async move {
                    let tr = db_copy.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();

                    for index in 0..2000 {
                        let _r = tr.upsert(vec![
                            TableKV {
                                table: table_name_copy.clone(),
                                key: usize_to_binary(index),
                                value: Some(usize_to_binary(index))
                            }
                        ]).await;
                    }

                    for index in 2000..3000 {
                        let _r = tr.upsert(vec![
                            TableKV {
                                table: table_name_copy.clone(),
                                key: usize_to_binary(index),
                                value: Some(usize_to_binary(index))
                            }
                        ]).await;
                    }

                    match tr.prepare_modified().await {
                        Err(_e) => {
                            if let Err(e) = tr.rollback_modified().await {
                                println!("rollback failed, reason: {:?}", e);
                            }
                        },
                        Ok(output) => {
                            if let Err(e) = tr.commit_modified(output).await {
                                if let ErrorLevel::Fatal = &e.level() {
                                    println!("rollback failed, reason: commit fatal error");
                                } else {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    }
                                }
                            } else {
                                sender_copy.send(());
                            }
                        },
                    }
                });

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
                            if count >= 1 {
                                println!("======> insert finish, time: {:?}, count: {}", start.elapsed(), count);
                                break;
                            }
                        },
                    }
                }

                let start = Instant::now();
                let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
                let mut index = 0;
                while let Some((key, value)) = values.next().await {
                    assert_eq!(binary_to_usize(&key).unwrap(), index);
                    assert_eq!(binary_to_usize(&value).unwrap(), index);
                    index += 1;
                }
                assert_eq!(index, 3000);
                println!("======> iterate 2 finish, count: {:?}, time: {:?}", index, start.elapsed());

                println!("======> wait 65s...");
                rt_copy.timeout(65000).await;
                println!("======> start iterate");

                let start = Instant::now();
                let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
                let mut index = 0;
                while let Some((key, value)) = values.next().await {
                    assert_eq!(binary_to_usize(&key).unwrap(), index);
                    assert_eq!(binary_to_usize(&value).unwrap(), index);
                    index += 1;
                }
                assert_eq!(index, 3000);
                println!("======> iterate 3 finish, count: {:?}, time: {:?}", index, start.elapsed());

                let tr = db.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                let _r = tr.delete(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: usize_to_binary(1000),
                        value: None
                    }
                ]).await;
                match tr.prepare_modified().await {
                    Err(_e) => {
                        if let Err(e) = tr.rollback_modified().await {
                            println!("rollback failed, reason: {:?}", e);
                        }
                    },
                    Ok(output) => {
                        if let Err(e) = tr.commit_modified(output).await {
                            if let ErrorLevel::Fatal = &e.level() {
                                println!("rollback failed, reason: commit fatal error");
                            } else {
                                if let Err(e) = tr.rollback_modified().await {
                                    println!("rollback failed, reason: {:?}", e);
                                }
                            }
                        } else {
                            ()
                        }
                    },
                }
                println!("======> delete key = 1000 finish, count: {:?}, time: {:?}", count, start.elapsed());

                let start = Instant::now();
                let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                let r = tr.query(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: usize_to_binary(1000),
                        value: None
                    }
                ]).await;
                assert!(r.len() == 1 && r[0].is_none());
                println!("======> query key = 1000 finish, count: 0, time: {:?}", start.elapsed());

                let start = Instant::now();
                let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
                while let Some((key, value)) = values.next().await {
                    if binary_to_usize(&key).unwrap() == 1000 {
                        panic!("key: {:?}, value: {:?}", binary_to_usize(&key).unwrap(), binary_to_usize(&value).unwrap());
                    }
                }
                println!("======> iterate key = 1000 finish, count: 0, time: {:?}", start.elapsed());

                let (sender, receiver) = unbounded();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let sender_copy = sender.clone();
                let start = Instant::now();
                let _ = rt_copy.spawn(async move {
                    let tr = db_copy.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();

                    for index in 0..3000 {
                        let _r = tr.upsert(vec![
                            TableKV {
                                table: table_name_copy.clone(),
                                key: usize_to_binary(index),
                                value: Some(usize_to_binary(index))
                            }
                        ]).await;
                    }

                    match tr.prepare_modified().await {
                        Err(_e) => {
                            if let Err(e) = tr.rollback_modified().await {
                                println!("rollback failed, reason: {:?}", e);
                            }
                        },
                        Ok(output) => {
                            if let Err(e) = tr.commit_modified(output).await {
                                if let ErrorLevel::Fatal = &e.level() {
                                    println!("rollback failed, reason: commit fatal error");
                                } else {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    }
                                }
                            } else {
                                sender_copy.send(());
                            }
                        },
                    }
                });

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
                            if count >= 1 {
                                println!("======> insert and delete finish, time: {:?}, count: {}", start.elapsed(), count);
                                break;
                            }
                        },
                    }
                }

                let start = Instant::now();
                let mut count = 0;
                let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                for index in 0..3000 {
                    let r = tr.query(vec![
                        TableKV {
                            table: table_name.clone(),
                            key: usize_to_binary(index),
                            value: None
                        }
                    ]).await;
                    assert_eq!(binary_to_usize((&r[0]).as_ref().unwrap()).unwrap(), index);
                    count += 1;
                }
                println!("======> query all finish by 1, count: {:?}, time: {:?}", count, start.elapsed());

                let start = Instant::now();
                let mut count = 0;
                let table_name_copy = table_name.clone();
                let tr = db.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                for index in 0..3000 {
                    let _r = tr.delete(vec![
                        TableKV {
                            table: table_name_copy.clone(),
                            key: usize_to_binary(index),
                            value: None
                        }
                    ]).await;
                    count += 1;
                }
                match tr.prepare_modified().await {
                    Err(_e) => {
                        if let Err(e) = tr.rollback_modified().await {
                            println!("rollback failed, reason: {:?}", e);
                        }
                    },
                    Ok(output) => {
                        if let Err(e) = tr.commit_modified(output).await {
                            if let ErrorLevel::Fatal = &e.level() {
                                println!("rollback failed, reason: commit fatal error");
                            } else {
                                if let Err(e) = tr.rollback_modified().await {
                                    println!("rollback failed, reason: {:?}", e);
                                }
                            }
                        } else {
                            ()
                        }
                    },
                }
                println!("======> delete all finish, count: {:?}, time: {:?}", count, start.elapsed());

                let start = Instant::now();
                let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                for index in 0..3000 {
                    let r = tr.query(vec![
                        TableKV {
                            table: table_name.clone(),
                            key: usize_to_binary(index),
                            value: None
                        }
                    ]).await;
                    assert!(r.len() == 1 && r[0].is_none());
                }
                println!("======> query all finish by 2, count: 0, time: {:?}", start.elapsed());

                let start = Instant::now();
                let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
                while let Some((key, value)) = values.next().await {
                    panic!("key: {:?}, value: {:?}", binary_to_usize(&key).unwrap(), binary_to_usize(&value).unwrap());
                }
                println!("======> iterate 4 finish, count: 0, time: {:?}", start.elapsed());

                println!("======> wait 65s...");
                rt_copy.timeout(65000).await;
                println!("======> start iterate");

                let start = Instant::now();
                let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
                while let Some((key, value)) = values.next().await {
                    panic!("key: {:?}, value: {:?}", binary_to_usize(&key).unwrap(), binary_to_usize(&value).unwrap());
                }
                println!("======> iterate 5 finish, count: 0, time: {:?}", start.elapsed());

                let start = Instant::now();
                if let Err(e) = db.collect_table(&table_name).await {
                    panic!("{:?}", e);
                }
                println!("======> Compact finish, time: {:?}", start.elapsed());
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 测试B树表并发写和迭代，并将迭代的数据删除，需要清空数据库后再测试
#[test]
fn test_b_tree_table_write_delete_iteraton() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log/a/b/c");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
                    //创建有序内存表失败
                    println!("!!!!!!create b-tree ordered table failed, reason: {:?}", e);
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

                let rt_clone = rt_copy.clone();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let _ = rt_copy.spawn(async move {
                    loop {
                        let tr = db_copy.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                        let mut values = tr.values(table_name_copy.clone(), None, false).await.unwrap();

                        let mut table_kv_list = Vec::new();
                        while let Some((key, _value)) = values.next().await {
                            rt_clone.timeout(100).await;
                            table_kv_list.push(TableKV {
                                table: table_name_copy.clone(),
                                key,
                                value: None
                            });
                        }

                        let r = tr.delete(table_kv_list.clone()).await;
                        assert!(r.is_ok());
                        match tr.prepare_modified().await {
                            Err(_e) => {
                                if let Err(e) = tr.rollback_modified().await {
                                    println!("rollback failed, reason: {:?}", e);
                                }
                            },
                            Ok(output) => {
                                if let Err(e) = tr.commit_modified(output).await {
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: commit fatal error");
                                    } else {
                                        if let Err(e) = tr.rollback_modified().await {
                                            println!("rollback failed, reason: {:?}", e);
                                        }
                                    }
                                } else {
                                    ()
                                }
                            },
                        }

                        if table_kv_list.len() == 0 {
                            rt_clone.timeout(100).await;
                        }
                        println!("!!!!!!delete finish, len: {:?}", table_kv_list.len());
                    }
                });

                for n in 0..10000 {
                    rt_copy.timeout(16).await;

                    let db_copy = db.clone();
                    let table_name_copy = table_name.clone();
                    let _ = rt_copy.spawn(async move {
                        let mut table_kv_list = Vec::with_capacity(100);
                        for index in 0..100 {
                            table_kv_list.push(
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(index),
                                    value: Some(usize_to_binary(index))
                                }
                            );
                        }

                        let tr = db_copy.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                        let _r = tr.upsert(table_kv_list.clone()).await;
                        match tr.prepare_modified().await {
                            Err(_e) => {
                                if let Err(e) = tr.rollback_modified().await {
                                    println!("rollback failed, reason: {:?}", e);
                                }
                            },
                            Ok(output) => {
                                if let Err(e) = tr.commit_modified(output).await {
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: commit fatal error");
                                    } else {
                                        if let Err(e) = tr.rollback_modified().await {
                                            println!("rollback failed, reason: {:?}", e);
                                        }
                                    }
                                } else {
                                    ()
                                }
                            },
                        }
                        table_kv_list.clear();

                        println!("!!!!!!write finish, n: {:?}", n);
                    });
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 测试B树表删除后迭代，需要清空数据库后再测试
#[test]
fn test_b_tree_table_delete_iteraton() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log/a/b/c");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
                    //创建有序内存表失败
                    println!("!!!!!!create b-tree ordered table failed, reason: {:?}", e);
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

                let index = 100000;
                let tr = db.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                let _r = tr.upsert(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: usize_to_binary(index),
                        value: Some(usize_to_binary(index))
                    }
                ]).await;
                match tr.prepare_modified().await {
                    Err(_e) => {
                        if let Err(e) = tr.rollback_modified().await {
                            println!("rollback failed, reason: {:?}", e);
                        }
                    },
                    Ok(output) => {
                        if let Err(e) = tr.commit_modified(output).await {
                            if let ErrorLevel::Fatal = &e.level() {
                                println!("rollback failed, reason: commit fatal error");
                            } else {
                                if let Err(e) = tr.rollback_modified().await {
                                    println!("rollback failed, reason: {:?}", e);
                                }
                            }
                        } else {
                            ()
                        }
                    },
                }

                println!("wait 65s...");
                rt_copy.timeout(65000).await;

                let (sender, receiver) = unbounded();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let sender_copy = sender.clone();
                let start = Instant::now();
                let _ = rt_copy.spawn(async move {
                    let index = 100000;
                    for n in 0..100 {
                        let tr = db_copy.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                        let _r = tr.upsert(vec![
                            TableKV {
                                table: table_name_copy.clone(),
                                key: usize_to_binary(index),
                                value: Some(usize_to_binary(index))
                            }
                        ]).await;
                        match tr.prepare_modified().await {
                            Err(_e) => {
                                if let Err(e) = tr.rollback_modified().await {
                                    println!("rollback failed, reason: {:?}", e);
                                }
                            },
                            Ok(output) => {
                                if let Err(e) = tr.commit_modified(output).await {
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: commit fatal error");
                                    } else {
                                        if let Err(e) = tr.rollback_modified().await {
                                            println!("rollback failed, reason: {:?}", e);
                                        }
                                    }
                                } else {
                                    ()
                                }
                            },
                        }



                        let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                        let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
                        while let Some((key, value)) = values.next().await {
                            assert_eq!(binary_to_usize(&key).unwrap(), binary_to_usize(&value).unwrap())
                        }



                        let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                        let r = tr.query(vec![
                            TableKV {
                                table: table_name.clone(),
                                key: usize_to_binary(index),
                                value: None
                            }
                        ]).await;
                        assert!(r.len() == 1 && binary_to_usize((&r[0]).as_ref().unwrap()).unwrap() == index);



                        let tr = db.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                        let r = tr.delete(vec![
                            TableKV {
                                table: table_name_copy.clone(),
                                key: usize_to_binary(index),
                                value: None
                            }
                        ]).await;
                        assert!(r.is_ok());
                        match tr.prepare_modified().await {
                            Err(_e) => {
                                if let Err(e) = tr.rollback_modified().await {
                                    println!("rollback failed, reason: {:?}", e);
                                }
                            },
                            Ok(output) => {
                                if let Err(e) = tr.commit_modified(output).await {
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: commit fatal error");
                                    } else {
                                        if let Err(e) = tr.rollback_modified().await {
                                            println!("rollback failed, reason: {:?}", e);
                                        }
                                    }
                                } else {
                                    ()
                                }
                            },
                        }



                        let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                        let r = tr.query(vec![
                            TableKV {
                                table: table_name.clone(),
                                key: usize_to_binary(index),
                                value: None
                            }
                        ]).await;
                        assert!(r.len() == 1 && r[0].is_none());



                        let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                        let mut values = tr.values(table_name.clone(), None, false).await.unwrap();

                        let values_raw: *mut (dyn Stream<Item = (Binary, Binary)> + Send) = Box::into_raw(unsafe { Pin::into_inner_unchecked(values) });
                        let x = values_raw as *mut () as usize;
                        let metadata: std::ptr::DynMetadata<dyn Stream<Item = (Binary, Binary)> + Send> = std::ptr::metadata(values_raw);
                        let y = Box::into_raw(Box::new(metadata)) as usize;
                        let metadata = unsafe { Box::from_raw(y as *mut std::ptr::DynMetadata<dyn Stream<Item = (Binary, Binary)> + Send>) };
                        let fat_ptr: *mut (dyn Stream<Item = (Binary, Binary)> + Send) = unsafe { std::ptr::from_raw_parts_mut(x as *mut (), *metadata) };
                        let boxed: Box<dyn Stream<Item = (Binary, Binary)> + Send> = unsafe { Box::from_raw(fat_ptr) };
                        let mut values = unsafe { Pin::new_unchecked(boxed) };

                        while let Some((key, value)) = values.next().await {
                            panic!("n: {:?}, key: {:?}, value: {:?}", n, binary_to_usize(&key).unwrap(), binary_to_usize(&value).unwrap());
                        }
                    }

                    sender_copy.send(());
                });

                let mut count = 0;
                loop {
                    match receiver.recv_timeout(Duration::from_millis(30000)) {
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
                            if count >= 1 {
                                println!("======> insert and delete and iterate finish, time: {:?}, count: {}", start.elapsed(), count);
                                break;
                            }
                        },
                    }
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 测试B树表删除后查询，可以不用初始化数据库反复测试
// 注意一定要等提交日志全部提交确认完成后再进行下一次测试
#[test]
fn test_b_tree_table_delete_query() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name0 = Atom::from("test_log/a/b/c");
                let tr = db.transaction(table_name0.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name0.clone(),
                                                KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
                    //创建有序内存表失败
                    println!("!!!!!!create b-tree ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;

                let table_name1 = Atom::from("test_log/d/e/f");
                let tr = db.transaction(table_name1.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name1.clone(),
                                                KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
                    //创建有序内存表失败
                    println!("!!!!!!create b-tree ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified_conflicts().await.unwrap();
                let _ = tr.commit_modified(output).await;

                println!("!!!!!!db table size: {:?}", db.table_size().await);

                //查询表信息
                rt_copy.timeout(1500).await;
                println!("");

                println!("!!!!!!test_log is exist: {:?}", db.is_exist(&table_name0).await);
                println!("!!!!!!test_log is ordered table: {:?}", db.is_ordered_table(&table_name0).await);
                println!("!!!!!!test_log is persistent table: {:?}", db.is_persistent_table(&table_name0).await);
                println!("!!!!!!test_log table_dir: {:?}", db.table_path(&table_name0).await);
                println!("!!!!!!test_log table len: {:?}", db.table_record_size(&table_name0).await);

                //操作数据库事务
                rt_copy.timeout(1500).await;
                println!("");

                let key = 100000;
                let r = tr.query(vec![
                    TableKV {
                        table: table_name0.clone(),
                        key: usize_to_binary(key),
                        value: None
                    }
                ]).await;
                let index = if r[0].is_none() {
                    //初始化
                    let tr = db.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                    let r = tr.upsert(vec![
                        TableKV {
                            table: table_name0.clone(),
                            key: usize_to_binary(key),
                            value: Some(usize_to_binary(0))
                        }
                    ]).await;
                    match tr.prepare_modified_conflicts().await {
                        Err(e0) => {
                            if let Err(e1) = tr.rollback_modified().await {
                                println!("rollback failed, reason: {:?}", e1);
                            }
                            panic!("prepare failed, reason: {:?}", e0);
                        },
                        Ok(output) => {
                            if let Err(e0) = tr.commit_modified(output).await {
                                if let ErrorLevel::Fatal = &e0.level() {
                                    println!("rollback failed, reason: commit fatal error");
                                } else {
                                    if let Err(e1) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e1);
                                    }
                                }
                                panic!("commit failed, reason: {:?}", e0);
                            } else {
                                0
                            }
                        },
                    }
                } else {
                    binary_to_usize(r[0].as_ref().unwrap()).unwrap()
                };

                let (sender, receiver) = unbounded();
                let rt_clone = rt_copy.clone();
                let sender_copy = sender.clone();
                let start = Instant::now();
                let _ = rt_copy.spawn(async move {
                    let tr = db.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                    let r = tr.query(vec![
                        TableKV {
                            table: table_name1.clone(),
                            key: usize_to_binary(index),
                            value: None
                        }
                    ]).await;
                    let _r = tr.upsert(vec![
                        TableKV {
                            table: table_name0.clone(),
                            key: usize_to_binary(key),
                            value: Some(usize_to_binary(index + 1))
                        }
                    ]).await;
                    let _r = tr.upsert(vec![
                        TableKV {
                            table: table_name1.clone(),
                            key: usize_to_binary(index + 1),
                            value: Some(usize_to_binary(index + 1))
                        }
                    ]).await;
                    let r = tr.delete(vec![
                        TableKV {
                            table: table_name1.clone(),
                            key: usize_to_binary(index),
                            value: None
                        }
                    ]).await;
                    assert!(r.is_ok());
                    match tr.prepare_modified_conflicts().await {
                        Err(_e) => {
                            if let Err(e) = tr.rollback_modified().await {
                                println!("rollback failed, reason: {:?}", e);
                            }
                        },
                        Ok(output) => {
                            if let Err(e) = tr.commit_modified(output).await {
                                if let ErrorLevel::Fatal = &e.level() {
                                    println!("rollback failed, reason: commit fatal error");
                                } else {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    }
                                }
                            } else {
                                ()
                            }
                        },
                    }



                    let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                    let r = tr.query(vec![
                        TableKV {
                            table: table_name0.clone(),
                            key: usize_to_binary(key),
                            value: None
                        }
                    ]).await;
                    assert!(r.len() == 1 && r[0].is_some());

                    rt_clone.timeout(10000).await;

                    let tr = db.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                    let r = tr.query(vec![
                        TableKV {
                            table: table_name1.clone(),
                            key: usize_to_binary(index),
                            value: None
                        }
                    ]).await;
                    assert!(r.len() == 1 && r[0].is_none());

                    sender_copy.send(());
                });

                let mut count = 0;
                loop {
                    match receiver.recv_timeout(Duration::from_millis(150000)) {
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
                            if count >= 1 {
                                println!("======> insert and delete and query finish, time: {:?}, count: {}", start.elapsed(), count);
                                break;
                            }
                        },
                    }
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 测试上一个写事务清理开始后完成前新事务提交完成
#[test]
fn test_b_tree_commit_before_clean() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log/a/b/c");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
                    //创建有序内存表失败
                    println!("!!!!!!create b-tree ordered table failed, reason: {:?}", e);
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

                //初始化测试表
                let tr = db.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                let _r = tr.upsert(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: usize_to_binary(0),
                        value: Some(usize_to_binary(0))
                    }
                ]).await;
                match tr.prepare_modified().await {
                    Err(_e) => {
                        if let Err(e) = tr.rollback_modified().await {
                            println!("rollback failed, reason: {:?}", e);
                        }
                    },
                    Ok(output) => {
                        if let Err(e) = tr.commit_modified(output).await {
                            if let ErrorLevel::Fatal = &e.level() {
                                println!("rollback failed, reason: commit fatal error");
                            } else {
                                if let Err(e) = tr.rollback_modified().await {
                                    println!("rollback failed, reason: {:?}", e);
                                }
                            }
                        } else {
                            ()
                        }
                    },
                }

                let rt_clone = rt_copy.clone();
                let (sender, receiver) = unbounded();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let sender_copy = sender.clone();
                let start = Instant::now();
                let _ = rt_copy.spawn(async move {
                    for index in 0..10000 {
                        rt_clone.timeout(16).await;

                        let tr = db_copy.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                        let r = tr.query(vec![
                            TableKV {
                                table: table_name_copy.clone(),
                                key: usize_to_binary(0),
                                value: None,
                            }
                        ]).await;
                        let last_value = binary_to_usize((&r[0]).as_ref().unwrap()).unwrap();
                        println!("!!!!!!index: {:?}, last_value: {:?}", index, last_value);
                        assert_eq!(index, last_value); //关键测试用断言

                        let new_value = last_value + 1;
                        let _r = tr.upsert(vec![
                            TableKV {
                                table: table_name_copy.clone(),
                                key: usize_to_binary(0),
                                value: Some(usize_to_binary(new_value))
                            }
                        ]).await;
                        match tr.prepare_modified().await {
                            Err(_e) => {
                                if let Err(e) = tr.rollback_modified().await {
                                    println!("rollback failed, reason: {:?}", e);
                                }
                            },
                            Ok(output) => {
                                if let Err(e) = tr.commit_modified(output).await {
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: commit fatal error");
                                    } else {
                                        if let Err(e) = tr.rollback_modified().await {
                                            println!("rollback failed, reason: {:?}", e);
                                        }
                                    }
                                } else {
                                    ()
                                }
                            },
                        }
                    }

                    sender_copy.send(());
                });

                let mut count = 0;
                loop {
                    match receiver.recv_timeout(Duration::from_millis(30000)) {
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
                            if count >= 1 {
                                println!("======> test b-tree commit before clean finish, time: {:?}, count: {}", start.elapsed(), count);
                                break;
                            }
                        },
                    }
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 测试BtreeTable写冲突
#[test]
fn test_b_tree_table_conflict() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log/a/b/c");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
                    //创建有序内存表失败
                    println!("!!!!!!create b-tree ordered table failed, reason: {:?}", e);
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
                            value: Some(usize_to_binary(0))
                        }
                    ]).await;

                    if let Ok(output) = tr.prepare_modified().await {
                        tr.commit_modified(output).await.is_ok();
                        println!("Waiting init...");
                        rt_copy.timeout(1000).await;
                    } else {
                        panic!("Init failed");
                    }
                }

                let (sender, receiver) = unbounded();
                let start = Instant::now();
                for _ in 0..1000 {
                    let rt_copy_ = rt_copy.clone();
                    let db_copy = db.clone();
                    let table_name_copy = table_name.clone();
                    let sender_copy = sender.clone();

                    let _ = rt_copy.spawn(async move {
                        let now = Instant::now();
                        let mut is_ok = false;

                        while now.elapsed().as_millis() <= 120000 {
                            let tr = db_copy.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                            let r = tr.query(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: None
                                }
                            ]).await;
                            let last_value = if r[0].is_none() {
                                0
                            }  else {
                                binary_to_usize((&r[0]).as_ref().unwrap()).unwrap()
                            };

                            let new_value = last_value + 1;
                            let _r = tr.upsert(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: Some(usize_to_binary(new_value))
                                }
                            ]).await;

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        rt_copy_.timeout(1).await;
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
                                                    rt_copy_.timeout(1).await;
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

                let rt_clone = rt_copy.clone();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let _ = rt_copy.spawn(async move {
                    loop {
                        rt_clone.timeout(1000).await;
                        let tr = db_copy.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                        let mut values = tr.values(table_name_copy.clone(), None, false).await.unwrap();

                        let values_raw: *mut (dyn Stream<Item = (Binary, Binary)> + Send) = Box::into_raw(unsafe { Pin::into_inner_unchecked(values) });
                        let x = values_raw as *mut () as usize;
                        let metadata: std::ptr::DynMetadata<dyn Stream<Item = (Binary, Binary)> + Send> = std::ptr::metadata(values_raw);
                        let y = Box::into_raw(Box::new(metadata)) as usize;

                        println!("origin, x: {:?}, y: {:?}", x, y);
                        rt_clone.timeout(1000).await;

                        loop {
                            let metadata = unsafe { Box::from_raw(y as *mut std::ptr::DynMetadata<dyn Stream<Item = (Binary, Binary)> + Send>) };
                            let fat_ptr: *mut (dyn Stream<Item = (Binary, Binary)> + Send) = unsafe { std::ptr::from_raw_parts_mut(x as *mut (), *metadata) };
                            let boxed: Box<dyn Stream<Item = (Binary, Binary)> + Send> = unsafe { Box::from_raw(fat_ptr) };
                            let mut values = unsafe { Pin::new_unchecked(boxed) };

                            let r = values.next().await;
                            if r.is_some() {
                                let _ = Box::into_raw(unsafe { Pin::into_inner_unchecked(values) });
                                let _ = Box::into_raw(metadata);
                            } else {
                                break;
                            }
                        }
                    }
                });

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

                // {
                //     let tr = db.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                //
                //     let r = tr.query(vec![
                //         TableKV {
                //             table: table_name.clone(),
                //             key: usize_to_binary(0),
                //             value: None
                //         }
                //     ]).await;
                //     let last_value = binary_to_usize((&r[0]).as_ref().unwrap()).unwrap();
                //
                //     assert_eq!(last_value, 1000);
                // }

                loop {
                    rt_copy.timeout(1000).await;
                    println!("{:?}, {:?}", pi_ordmap::ordmap::ordmap_shared_count(), pi_ordmap::asbtree::itertree_shared_count());
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

/// 测试并发写和迭代对内存的影响
/// 注意只在Linux下测试
#[test]
fn test_b_tree_table_write_iteraton_for_memory() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let rt = AsyncRuntimeBuilder::default_multi_thread(None, None, None, None);
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let mut table_names = Vec::with_capacity(10);
                let tr = db.transaction(Atom::from("create b-tree table"), true, 500, 500).unwrap();
                for index in 0..10 {
                    let table_name = Atom::from("test_log/".to_string() + &index.to_string());
                    if let Err(e) = tr.create_table(table_name.clone(),
                                                    KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                                                     true,
                                                                     EnumType::Usize,
                                                                     EnumType::Str),
                                                    true).await {
                        //创建有序内存表失败
                        println!("!!!!!!create b-tree ordered table failed, reason: {:?}", e);
                    } else {
                        table_names.push(table_name);
                    }
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;

                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let rt_clone = rt_copy.clone();
                let db_copy = db.clone();
                let _ = rt_copy.spawn(async move {
                    loop {
                        rt_clone.timeout(126000).await;
                        let mut b = false;
                        // #[cfg(target_os = "linux")]
                        // b = db_copy.cleanup_buffer_after_collect_table();
                        println!("!!!!!!cleanup_buffer_after_collect_table: {:?}", b);
                    }
                });

                //操作数据库事务
                rt_copy.timeout(1500).await;
                println!("");

                let table_names_copy = table_names.clone();
                for table_name_copy in table_names_copy.clone() {
                    let rt_clone = rt_copy.clone();
                    let db_copy = db.clone();
                    let _ = rt_copy.spawn(async move {
                        let value = generate_string_with_repeated_char(1 * 1024, 'a');
                        for index in 0..1000000 {
                            rt_clone.timeout(1000).await;

                            let tr = db_copy.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();
                            let _r = tr.upsert(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(index),
                                    value: Some(string_to_binary(value.clone()))
                                }
                            ]).await;
                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("upsert rollback failed, reason: {:?}", e);
                                    }
                                },
                                Ok(output) => {
                                    if let Err(e) = tr.commit_modified(output).await {
                                        if let ErrorLevel::Fatal = &e.level() {
                                            println!("upsert rollback failed, reason: commit fatal error");
                                        } else {
                                            if let Err(e) = tr.rollback_modified().await {
                                                println!("upsert rollback failed, reason: {:?}", e);
                                            }
                                        }
                                    }
                                },
                            }
                        }
                    });
                }

                loop {
                    rt_copy.timeout(1000).await;
                    for table_name_copy in &table_names_copy {
                        let rt_clone = rt_copy.clone();
                        let db_copy = db.clone();
                        let tr = db_copy.transaction(Atom::from("test b-tree table"), false, 500, 500).unwrap();
                        let mut iter = tr.values(table_name_copy.clone(), None, false).await.unwrap();
                        let _ = rt_copy.spawn(async move {
                            while let Some((_key, _value)) = iter.next().await {

                            }
                        });

                        println!("!!!!!!table: {:?}, table_size: {:?}", table_name_copy, db_copy.table_cache_size(&table_name_copy).await);
                    }
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

//执行三次，第一次执行在数据落地前中止执行，第二次执行则等待数据落地后再退出，第三次执行查看所有值是否剩以1000000并加1
#[test]
fn test_db_repair() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
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
                                                                 EnumType::Usize),
                                                true).await {
                    //创建有序内存表失败
                    println!("!!!!!!create log ordered table failed, reason: {:?}", e);
                }
                if let Err(e) = tr.create_table(table_name1.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::U8,
                                                                 EnumType::Usize),
                                                true).await {
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
                                 binary_to_u8(&key),
                                 binary_to_usize(&value));
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
                                 binary_to_u8(&key),
                                 binary_to_usize(&value));
                    }
                }

                rt_copy.timeout(10000).await;
                println!("");

                let mut table_kv_list = Vec::new();
                for index in 0..10u8 {
                    let vec = tr.query(vec![TableKV {
                        table: table_name0.clone(),
                        key: u8_to_binary(index),
                        value: None
                    }]).await;

                    if let Some(last_value) = &vec[0] {
                        //有值，则加1
                        let last_value = binary_to_usize(last_value).unwrap();
                        table_kv_list.push(TableKV {
                            table: table_name0.clone(),
                            key: u8_to_binary(index),
                            value: Some(usize_to_binary(last_value + 1))
                        });
                    } else {
                        //无值，则初始化
                        table_kv_list.push(TableKV {
                            table: table_name0.clone(),
                            key: u8_to_binary(index),
                            value: Some(usize_to_binary(index as usize * 1000000))
                        });
                    }

                    let vec = tr.query(vec![TableKV {
                        table: table_name1.clone(),
                        key: u8_to_binary(index),
                        value: None
                    }]).await;

                    if let Some(last_value) = &vec[0] {
                        //有值，则加1
                        let last_value = binary_to_usize(last_value).unwrap();
                        table_kv_list.push(TableKV {
                            table: table_name1.clone(),
                            key: u8_to_binary(index),
                            value: Some(usize_to_binary(last_value + 1))
                        });
                    } else {
                        //无值，则初始化
                        table_kv_list.push(TableKV {
                            table: table_name1.clone(),
                            key: u8_to_binary(index),
                            value: Some(usize_to_binary(index as usize * 1000000))
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
fn test_db_repair_by_specific() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    init_transaction_debug_logger(rt.clone(),
                                  "./specific_db/log_table_debug",
                                  10000,
                                  10000);

    let _ = rt.spawn(async move {
        let guid_gen = GuidGen::new(run_nanos(), 0);
        let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), "./specific_db/.commit_log");
        let commit_logger = commit_logger_builder
            .build()
            .await
            .unwrap();

        let tr_mgr = Transaction2PcManager::new(rt_copy.clone(),
                                                guid_gen,
                                                commit_logger);

        let mut builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./specific_db");
        match builder.startup(true).await {
            Err(e) => {
                panic!("!!!!!!startup db failed, reason: {:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db tables: {:?}", db.tables().await);
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

//执行两次，第一次执行在数据落地前中止执行，然后执行test_commit_log_inspector，第二次执行则等待数据落地后再退出，然后执行test_log_table_inspector
#[test]
fn test_multi_tables_repair() {
    use std::thread;
    use std::time::{Duration, Instant};

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
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
                                                                     EnumType::Usize),
                                                    true).await {
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
    let _ = rt.spawn(async move  {
        for table_name in &table_names_copy {
            let tr = db_copy.transaction(table_name.clone(), true, 500, 500).unwrap();

            //检查所有有序日志表
            let mut count = 0;
            let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
            while let Some((key, value)) = values.next().await {
                let val = binary_to_usize(&value).unwrap();
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
                        value: Some(usize_to_binary(0))
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

            let _ = rt.spawn(async move {
                let mut result = true;
                let now = Instant::now();
                while now.elapsed().as_millis() < 30000 {
                    let tr = db_copy.transaction(Atom::from("Test multi table repair"), true, 500, 500).unwrap();

                    for table_name in &table_names_copy {
                        let key = usize_to_binary(index);
                        let r = tr.query(vec![TableKV::new(table_name.clone(), key.clone(), None)]).await;
                        let last_value = binary_to_usize((&r[0]).as_ref().unwrap()).unwrap();
                        let new_last_value = last_value + 1;

                        tr.delete(vec![TableKV::new(table_name.clone(), key.clone(), None)]);
                        tr.upsert(vec![TableKV::new(table_name.clone(), key, Some(usize_to_binary(new_last_value)))]).await.unwrap();
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

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    let _ = rt.spawn(async move {
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
                    let value = binary_to_usize(&Binary::new(result.5)).unwrap();
                    println!("Inspect next, tid: {}, cid: {}, table: {}, method: {}, key: {}, value: {}", result.0, result.1, result.2, result.3, key, value);
                }
            }
            println!("Inspect finish");
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 需要先有提交日志
#[test]
fn test_commit_log_inspector_with_callback() {
    use std::thread;
    use std::time::Duration;
    use chrono::{TimeZone, FixedOffset, Utc};
    use std::sync::{Arc,
                    atomic::{AtomicUsize, Ordering}};

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    let _ = rt.spawn(async move {
        let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), "./.commit_log");
        let commit_logger = commit_logger_builder
            .log_file_limit(1024)
            .build()
            .await
            .unwrap();

        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let mut count = Arc::new(AtomicUsize::new(0));
        let inspector = CommitLogInspector::new(rt_copy, commit_logger);
        inspector.begin_with_callback(move |event| {
            if let Some((tid, cid, table, method, time, key, value)) = event {
                let datetime = Utc
                    .timestamp_millis_opt(time as i64)
                    .single()
                    .unwrap();
                let datetime_ = datetime.with_timezone(&offset);

                println!("Inspect commit log, tid: {:?}, cid: {:?}, table: {:?}, method: {:?}, time: {:?}, key: {:?}, value: {:?}",
                         tid,
                         cid,
                         table,
                         method,
                         datetime_,
                         key.len(),
                         value.len());
                count.fetch_add(1, Ordering::AcqRel);
            } else {
                println!("Inspect commit log already finished, count: {:?}", count.load(Ordering::Acquire));
            }
        });
    });

    thread::sleep(Duration::from_millis(1000000000));
}

// 先执行test_multi_tables_repair，并等待写入表文件
#[test]
fn test_log_table_inspector() {
    use std::thread;
    use std::time::Duration;

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();

    let rt_copy = rt.clone();
    let _ = rt.spawn(async move {
        let inspector = LogTableInspector::new(rt_copy, "./db/.tables/config/db/Record.DramaNumberRecord").unwrap();
        if inspector.begin() {
            while let Some(result) = inspector.next() {
                // let key = binary_to_usize(&Binary::new(result.2)).unwrap();
                // if key == 112800000 {
                //     println!("Inspect next, file: {}, method: {}, key: {}, value: {:?}", result.0, result.1, key, result.3);
                // }
                let key = binary_to_usize(&Binary::new(result.2)).unwrap();
                let value = binary_to_usize(&Binary::new(result.3)).unwrap();
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

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
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
                                                                     EnumType::Usize),
                                                    true).await {
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
    let _ = rt.spawn(async move  {
        for table_name in &table_names_copy {
            let tr = db_copy.transaction(table_name.clone(), true, 500, 500).unwrap();

            //检查所有有序日志表
            let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
            while let Some((key, value)) = values.next().await {
                let val = binary_to_usize(&value).unwrap();
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

            let _ = rt.spawn(async move {
                let mut result = true;
                let now = Instant::now();
                while now.elapsed().as_millis() < 60000 {
                    let tr = db_copy.transaction(Atom::from("Test multi table repair"), true, 500, 500).unwrap();

                    for table_name in &table_names_copy {
                        let key = usize_to_binary(index);
                        let r = tr.query(vec![TableKV::new(table_name.clone(), key.clone(), None)]).await;
                        let new_last_value = if r[0].is_none() {
                            0
                        } else {
                            let last_value = binary_to_usize((&r[0]).as_ref().unwrap()).unwrap();
                            last_value + 1
                        };

                        tr.upsert(vec![TableKV::new(table_name.clone(), key, Some(usize_to_binary(new_last_value)))]).await.unwrap();
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

            let _ = rt.spawn(async move {
                let mut result = true;
                let now = Instant::now();
                while now.elapsed().as_millis() < 30000 {
                    let tr = db_copy.transaction(Atom::from("Test multi table repair"), true, 500, 500).unwrap();

                    for table_name in &table_names_copy {
                        let key = usize_to_binary(index);
                        let r = tr.query(vec![TableKV::new(table_name.clone(), key.clone(), None)]).await;
                        let new_last_value = if r[0].is_none() {
                            0
                        } else {
                            let last_value = binary_to_usize((&r[0]).as_ref().unwrap()).unwrap();
                            last_value + 1
                        };

                        tr.upsert(vec![TableKV::new(table_name.clone(), key, Some(usize_to_binary(new_last_value)))]).await.unwrap();
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

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
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
                                                                     EnumType::Usize),
                                                    true).await {
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
    let _ = rt.spawn(async move  {
        for table_name in &table_names_copy {
            let tr = db_copy.transaction(table_name.clone(), true, 500, 500).unwrap();

            //检查所有有序日志表
            let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
            while let Some((key, value)) = values.next().await {
                let val = binary_to_usize(&value).unwrap();
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

            let _ = rt.spawn(async move {
                let mut result = true;
                let now = Instant::now();
                while now.elapsed().as_millis() < 10000 {
                    let tr = db_copy.transaction(Atom::from("Test multi table repair"), true, 500, 500).unwrap();

                    for table_name in &table_names_copy {
                        let key = usize_to_binary(index);
                        let r = tr.query(vec![TableKV::new(table_name.clone(), key.clone(), None)]).await;
                        let new_last_value = if r[0].is_none() {
                            0
                        } else {
                            let last_value = match binary_to_usize((&r[0]).as_ref().unwrap()).unwrap() {
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

                        tr.upsert(vec![TableKV::new(table_name.clone(), key, Some(usize_to_binary(new_last_value)))]).await.unwrap();
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

#[test]
fn test_multi_tables_write_commit_log() {
    use std::thread;
    use std::time::{Duration, Instant};

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
    let _ = rt.spawn(async move {
        let guid_gen = GuidGen::new(run_nanos(), 0);
        let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), "./.commit_log");
        let commit_logger = commit_logger_builder
            .log_file_limit(2 * 1024 * 1024)
            .build()
            .await
            .unwrap();

        let tr_mgr = Transaction2PcManager::new(rt_copy.clone(),
                                                guid_gen,
                                                commit_logger);

        let builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./db");
        match builder.startup(true).await {
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
                                                                     EnumType::Usize),
                                                    true).await {
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
    let _ = rt.spawn(async move  {
        for table_name in &table_names_copy {
            let tr = db_copy.transaction(table_name.clone(), true, 500, 500).unwrap();

            //检查所有有序日志表
            let mut values = tr.values(table_name.clone(), None, false).await.unwrap();
            while let Some((key, value)) = values.next().await {
                let val = binary_to_usize(&value).unwrap();
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
    for _ in 0..10000 {
        for index in 0..10 {
            let rt_copy = rt.clone();
            let db_copy = db.clone();
            let table_names_copy = table_names.clone();
            let sender_copy = sender.clone();

            let _ = rt.spawn(async move {
                let mut result = true;
                let now = Instant::now();
                let mut retry_count = 0;
                while now.elapsed().as_millis() < 60000 {
                    let tr = db_copy.transaction(Atom::from("Test multi table repair"), true, 500, 500).unwrap();

                    for table_name in &table_names_copy {
                        let key = usize_to_binary(index);
                        let r = tr.query(vec![TableKV::new(table_name.clone(), key.clone(), None)]).await;
                        let new_last_value = if r[0].is_none() {
                            0
                        } else {
                            let last_value = match binary_to_usize(&(r[0]).as_ref().unwrap()).unwrap() {
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

                        tr.upsert(vec![TableKV::new(table_name.clone(), key, Some(usize_to_binary(new_last_value)))]).await.unwrap();
                    }

                    match tr.prepare_modified().await {
                        Err(e) => {
                            if let ErrorLevel::Normal = e.level() {
                                tr.rollback_modified().await.unwrap();
                            }

                            #[cfg(target_os = "windows")]
                            {
                                if retry_count > 10 {
                                    rt_copy.timeout(15).await;
                                } else {
                                    rt_copy.timeout(0).await;
                                }
                            }
                            #[cfg(target_os = "linux")]
                            rt_copy.timeout(0).await;

                            retry_count += 1;
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

        thread::sleep(Duration::from_millis(15));
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

                if count + err_count >= 100000 {
                    println!("!!!!!!time: {:?}, ok: {}, error: {}", start.elapsed(), count, err_count);
                    break;
                }
            },
        }
    }

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_query_conflict() {
    use std::thread;
    use std::time::Duration;

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt1 = builder.build();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
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
                            value: Some(usize_to_binary(0))
                        }
                    ]).await;

                    if let Ok(output) = tr.prepare_modified().await {
                        tr.commit_modified(output).await.is_ok();
                    }
                }

                let rt_copy_ = rt1.clone();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let _ = rt1.spawn(async move {
                    let mut conflict_count = 0;
                    for _ in 0..1000 {
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
                            let last_value = binary_to_usize((&r[0]).as_ref().unwrap()).unwrap();

                            let new_value = last_value + 1;
                            let _r = tr.upsert(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: Some(usize_to_binary(new_value))
                                }
                            ]).await;

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        conflict_count += 1;
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
                    }
                    println!("!!!!!!writable conflict, {}", conflict_count);
                });

                let rt_copy_ = rt_copy.clone();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let _ = rt_copy.spawn(async move {
                    let mut conflict_count = 0;
                    for _ in 0..10000 {
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

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        conflict_count += 1;
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
                    }
                    println!("!!!!!!only read conflict, {}", conflict_count);
                });
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_dirty_query_conflict() {
    use std::thread;
    use std::time::Duration;

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt1 = builder.build();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
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
                            value: Some(usize_to_binary(0))
                        }
                    ]).await;

                    if let Ok(output) = tr.prepare_modified().await {
                        tr.commit_modified(output).await.is_ok();
                    }
                }

                let rt_copy_ = rt1.clone();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let _ = rt1.spawn(async move {
                    let mut conflict_count = 0;
                    for _ in 0..1000 {
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
                            let last_value = binary_to_usize(&(r[0]).as_ref().unwrap()).unwrap();

                            let new_value = last_value + 1;
                            let _r = tr.upsert(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: Some(usize_to_binary(new_value))
                                }
                            ]).await;

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        conflict_count += 1;
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
                    }
                    println!("!!!!!!writable conflict, {}", conflict_count);
                });

                let rt_copy_ = rt_copy.clone();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let _ = rt_copy.spawn(async move {
                    let mut conflict_count = 0;
                    for _ in 0..10000 {
                        let now = Instant::now();
                        let mut is_ok = false;

                        while now.elapsed().as_millis() <= 60000 {
                            let tr = db_copy.transaction(Atom::from("test log table"), true, 500, 500).unwrap();
                            let r = tr.dirty_query(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: None
                                }
                            ]).await;

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        conflict_count += 1;
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
                    }
                    println!("!!!!!!only read conflict, {}", conflict_count);
                });
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_upsert_conflict() {
    use std::thread;
    use std::time::Duration;

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt1 = builder.build();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
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
                            value: Some(usize_to_binary(0))
                        }
                    ]).await;

                    if let Ok(output) = tr.prepare_modified().await {
                        tr.commit_modified(output).await.is_ok();
                    }
                }

                let rt_copy_ = rt1.clone();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let _ = rt1.spawn(async move {
                    let mut conflict_count = 0;
                    for _ in 0..1000 {
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
                            let last_value = binary_to_usize((&r[0]).as_ref().unwrap()).unwrap();

                            let new_value = last_value + 1;
                            let _r = tr.upsert(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: Some(usize_to_binary(new_value))
                                }
                            ]).await;

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        conflict_count += 1;
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
                    }
                    println!("!!!!!!writable conflict, {}", conflict_count);
                });

                let rt_copy_ = rt1.clone();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let _ = rt1.spawn(async move {
                    let mut conflict_count = 0;
                    for _ in 0..1000 {
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
                            let last_value = binary_to_usize((&r[0]).as_ref().unwrap()).unwrap();

                            let new_value = last_value + 1;
                            let _r = tr.upsert(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: Some(usize_to_binary(new_value))
                                }
                            ]).await;

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        conflict_count += 1;
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
                    }
                    println!("!!!!!!writable conflict, {}", conflict_count);
                });
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_dirty_upsert_conflict() {
    use std::thread;
    use std::time::Duration;

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt1 = builder.build();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
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
                            value: Some(usize_to_binary(0))
                        }
                    ]).await;

                    if let Ok(output) = tr.prepare_modified().await {
                        tr.commit_modified(output).await.is_ok();
                    }
                }

                let rt_copy_ = rt1.clone();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let _ = rt1.spawn(async move {
                    let mut conflict_count = 0;
                    for _ in 0..1000 {
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
                            let last_value = binary_to_usize((&r[0]).as_ref().unwrap()).unwrap();

                            let new_value = last_value + 1;
                            let _r = tr.upsert(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: Some(usize_to_binary(new_value))
                                }
                            ]).await;

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        conflict_count += 1;
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
                    }
                    println!("!!!!!!writable conflict, {}", conflict_count);
                });

                let rt_copy_ = rt1.clone();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let _ = rt1.spawn(async move {
                    let mut conflict_count = 0;
                    for _ in 0..1000 {
                        let now = Instant::now();
                        let mut is_ok = false;

                        while now.elapsed().as_millis() <= 60000 {
                            let tr = db_copy.transaction(Atom::from("test log table"), true, 500, 500).unwrap();
                            let r = tr.dirty_query(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: None
                                }
                            ]).await;
                            let last_value = binary_to_usize((&r[0]).as_ref().unwrap()).unwrap();

                            let new_value = last_value + 1;
                            let _r = tr.dirty_upsert(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: Some(usize_to_binary(new_value))
                                }
                            ]).await;

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        conflict_count += 1;
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
                    }
                    println!("!!!!!!writable conflict, {}", conflict_count);
                });
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_delete_conflict() {
    use std::thread;
    use std::time::Duration;

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt1 = builder.build();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
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
                            value: Some(usize_to_binary(0))
                        }
                    ]).await;

                    if let Ok(output) = tr.prepare_modified().await {
                        tr.commit_modified(output).await.is_ok();
                    }
                }

                let rt_copy_ = rt1.clone();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let _ = rt1.spawn(async move {
                    let mut conflict_count = 0;
                    for _ in 0..1000 {
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

                            let _r = tr.delete(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: None
                                }
                            ]).await;

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        conflict_count += 1;
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
                    }
                    println!("!!!!!!writable conflict, {}", conflict_count);
                });

                let rt_copy_ = rt1.clone();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let _ = rt1.spawn(async move {
                    let mut conflict_count = 0;
                    for _ in 0..1000 {
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

                            let _r = tr.delete(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: None
                                }
                            ]).await;

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        conflict_count += 1;
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
                    }
                    println!("!!!!!!writable conflict, {}", conflict_count);
                });
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_dirty_delete_conflict() {
    use std::thread;
    use std::time::Duration;

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt1 = builder.build();

    let _ = rt.spawn(async move {
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
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Usize),
                                                true).await {
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
                            value: Some(usize_to_binary(0))
                        }
                    ]).await;

                    if let Ok(output) = tr.prepare_modified().await {
                        tr.commit_modified(output).await.is_ok();
                    }
                }

                let rt_copy_ = rt1.clone();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let _ = rt1.spawn(async move {
                    let mut conflict_count = 0;
                    for _ in 0..1000 {
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

                            let _r = tr.delete(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: None
                                }
                            ]).await;

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        conflict_count += 1;
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
                    }
                    println!("!!!!!!writable conflict, {}", conflict_count);
                });

                let rt_copy_ = rt1.clone();
                let db_copy = db.clone();
                let table_name_copy = table_name.clone();
                let _ = rt1.spawn(async move {
                    let mut conflict_count = 0;
                    for _ in 0..1000 {
                        let now = Instant::now();
                        let mut is_ok = false;

                        while now.elapsed().as_millis() <= 60000 {
                            let tr = db_copy.transaction(Atom::from("test log table"), true, 500, 500).unwrap();
                            let r = tr.dirty_query(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: None
                                }
                            ]).await;

                            let _r = tr.dirty_delete(vec![
                                TableKV {
                                    table: table_name_copy.clone(),
                                    key: usize_to_binary(0),
                                    value: None
                                }
                            ]).await;

                            match tr.prepare_modified().await {
                                Err(_e) => {
                                    if let Err(e) = tr.rollback_modified().await {
                                        println!("rollback failed, reason: {:?}", e);
                                    } else {
                                        conflict_count += 1;
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
                    }
                    println!("!!!!!!writable conflict, {}", conflict_count);
                });
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_table_debug() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(100);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    init_transaction_debug_logger(rt.clone(),
                                  "./db/log_table_debug",
                                  10000,
                                  10000);

    let _ = rt.spawn(async move {
        let guid_gen = GuidGen::new(run_nanos(), 0);
        let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), "./db/.commit_log");
        let commit_logger = commit_logger_builder
            .build()
            .await
            .unwrap();

        let tr_mgr = Transaction2PcManager::new(rt_copy.clone(),
                                                guid_gen,
                                                commit_logger);

        let mut builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./db");
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::U8,
                                                                 EnumType::Str),
                                                true).await {
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
                                 binary_to_u8(&key),
                                 String::from_utf8_lossy(value.as_ref()).as_ref());
                    }
                }

                rt_copy.timeout(1500).await;
                println!("");

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
fn test_append_new_commit_log() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
        let guid_gen = GuidGen::new(run_nanos(), 0);
        let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), "./db/.commit_log");
        let commit_logger = commit_logger_builder
            .build()
            .await
            .unwrap();

        let tr_mgr = Transaction2PcManager::new(rt_copy.clone(),
                                                guid_gen,
                                                commit_logger);

        let mut builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./db");
        match builder.startup(true).await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let table_name = Atom::from("test_log");
                let tr = db.transaction(table_name.clone(), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::U8,
                                                                 EnumType::Str),
                                                true).await {
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

                let _new_index = db.append_new_commit_log().await.unwrap();
                let new_index = db.append_new_commit_log().await.unwrap();
                println!("!!!!!!append new commit log ok, new_index: {}", new_index);

                let r = tr.upsert(vec![
                    TableKV {
                        table: table_name.clone(),
                        key: u8_to_binary(0),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    }
                ]).await;
                println!("!!!!!!upsert result: {:?}", r);

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
fn test_load_log_table_debug() {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    let _ = rt.spawn(async move {
        if let Ok(log) = LogFile::open(rt_copy,
                                       "./tests/db/.log_table_debug",
                                       8096,
                                       128 * 1024 * 1024,
                                       None)
            .await {
            let mut loader = LogTableDebugLoader::new();
            match log.load_before(&mut loader,
                                  None,
                                  2 * 1024 * 1024,
                                  true)
                .await {
                Err(e) => {
                    println!("!!!!!!load failed, reason: {:?}", e);
                },
                Ok(_) => {
                    println!("!!!!!!load ok, complated len: {}, incomplated len: {}",
                             loader.complated.len(),
                             loader.incomplated.len());
                    for (tid, (_, index, _, _)) in loader.incomplated.iter() {
                        println!("!!!!!!incomplated, tid: {:?}, log_index: {:?}", tid, index);
                    }
                },
            }
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

struct LogTableDebugLoader {
    log_path:       Option<PathBuf>,                                        //当前正在加载的日志文件路径
    complated:      HashMap<Guid, (Guid, usize, usize, usize)>,             //完成的事务
    incomplated:    BTreeMap<Guid, (Option<Guid>, usize, usize, usize)>,    //未完成的事务
    buf:            Vec<(Vec<u8>, Vec<u8>)>,                                //当前加载完成的日志文件的缓冲
}

impl PairLoader for LogTableDebugLoader {
    fn is_require(&self, _log_file: Option<&PathBuf>, _key: &Vec<u8>) -> bool {
        //提交日志的所有日志都需要加载
        true
    }

    fn load(&mut self,
            log_file: Option<&PathBuf>,
            _method: LogMethod,
            key: Vec<u8>,
            value: Option<Vec<u8>>) {
        if let Some(log_file) = log_file {
            if self.log_path.is_none() {
                //正在加载首个日志文件的首个键值对，则设置当前正在加载的日志文件路径到提交日志加载器
                self.log_path = Some(log_file.clone());
            }

            if self.log_path.as_ref().unwrap() != log_file {
                //提交日志加载器正在加载的日志文件与正在加载的日志文件不相同，表示已加载完一个日志文件，则重置当前正在加载的日志文件路径到提交日志加载器
                self.log_path = Some(log_file.clone());

                //分析已加载完的事务跟踪日志
                let mut is_remove = false;
                while let Some((key, value)) = self.buf.pop() {
                    //获取事务信息
                    let key_string = String::from_utf8(key).unwrap();
                    let vec: Vec<&str> = key_string.split("Guid(").collect();
                    let vec: Vec<&str> = vec[1].split(")").collect();
                    let tid = Guid(vec[0].parse().unwrap());
                    match self.incomplated.entry(tid.clone()) {
                        Entry::Occupied(mut o) => {
                            //已存在
                            let val = o.get_mut();
                            if val.0.is_none() {
                                let value_string = String::from_utf8(value).unwrap();
                                if value_string.find("Commit transaction successed:\n\t").is_some() {
                                    let vec_: Vec<&str> = value_string.split("\n\t").collect();
                                    let cid = vec_[2]
                                        .split("cid: Guid")
                                        .collect::<Vec<&str>>()[1]
                                        .trim_matches(|c| c == '(' || c == ')')
                                        .parse()
                                        .unwrap();
                                    let log_index = vec_[1]
                                        .split("log_index: ")
                                        .collect::<Vec<&str>>()[1]
                                        .parse()
                                        .unwrap();
                                    val.0 = Some(Guid(cid));
                                    val.1 = log_index;
                                    val.2 = 1;
                                }
                            } else {
                                let value_string = String::from_utf8(value).unwrap();
                                if value_string.find("Commit transaction successed:\n\t").is_some() {
                                    let vec_: Vec<&str> = value_string.split("\n\t").collect();
                                    let cid = vec_[2]
                                        .split("cid: Guid")
                                        .collect::<Vec<&str>>()[1]
                                        .trim_matches(|c| c == '(' || c == ')')
                                        .parse()
                                        .unwrap();
                                    let log_index = vec_[1]
                                        .split("log_index: ")
                                        .collect::<Vec<&str>>()[1]
                                        .parse::<usize>()
                                        .unwrap();
                                    if val.0.as_ref().unwrap() == &Guid(cid) {
                                        if val.1 <= log_index {
                                            val.2 += 1;
                                        } else {
                                            panic!("Load commit transaction log failed, tid: {:?}, log_index: {:?}, current: {:?}, reason: not matched log_index",
                                                   tid,
                                                   val.1,
                                                   log_index);
                                        }
                                    } else {
                                        panic!("Load commit transaction log failed, tid: {:?}, cid: {:?}, current: {:?}, reason: not matched cid",
                                               tid,
                                               val.0.as_ref(),
                                               &cid);
                                    }
                                } else if value_string.find("Commit confirm transaction successed:\n\t").is_some() {
                                    let vec_: Vec<&str> = value_string.split("\n\t").collect();
                                    let cid = vec_[1]
                                        .split("cid: Guid")
                                        .collect::<Vec<&str>>()[1]
                                        .trim_matches(|c| c == '(' || c == ')')
                                        .parse()
                                        .unwrap();
                                    if val.0.as_ref().unwrap() == &Guid(cid) {
                                        val.3 += 1;
                                    } else {
                                        panic!("Load commit confirm transaction failed, tid: {:?}, cid: {:?}, current: {:?}, reason: not matched cid",
                                               tid,
                                               val.0.as_ref(),
                                               &cid);
                                    }
                                } else if value_string.find("End transaction:\n\t").is_some() {
                                    let vec_: Vec<&str> = value_string.split("\n\t").collect();
                                    let cid = vec_[1]
                                        .split("cid: Guid")
                                        .collect::<Vec<&str>>()[1]
                                        .trim_matches(|c| c == '(' || c == ')')
                                        .parse()
                                        .unwrap();
                                    if val.0.as_ref().unwrap() == &Guid(cid) {
                                        if val.2 == val.3 {
                                            self.complated.insert(tid.clone(), (Guid(cid), val.1, val.2, val.3));
                                            is_remove = true;
                                        } else {
                                            panic!("Load end transaction failed, tid: {:?}, len: {:?}, current: {:?}, reason: not matched table size",
                                                   tid,
                                                   val.2,
                                                   val.3);
                                        }
                                    } else {
                                        panic!("Load end transaction failed, tid: {:?}, cid: {:?}, current: {:?}, reason: not matched cid",
                                               tid,
                                               val.0.as_ref(),
                                               &cid);
                                    }
                                }
                            }
                        },
                        Entry::Vacant(v) => {
                            //未存在
                            let value_string = String::from_utf8(value).unwrap();
                            if value_string.find("Begin transaction:\n\t").is_some() {
                                let vec_: Vec<&str> = value_string.split("\n\t").collect();
                                let persistence = vec_[3]
                                    .split("require_persistence: ")
                                    .collect::<Vec<&str>>()[1];
                                if persistence == "true" {
                                    //只记录需要持久化的事务
                                    v.insert((None, 0, 0, 0));
                                }
                            }
                        },
                    }

                    if is_remove {
                        //从未完成事务表中移除已完成的事务
                        let _ = self.incomplated.remove(&tid);
                        is_remove = false;
                    }
                }
            }

            if let Some(val) = value {
                //缓冲当前日志文件中有值的事务
                self.buf.push((key, val));
            }
        }
    }
}

impl LogTableDebugLoader {
    /// 构建一个事务跟踪日志加载器
    pub fn new() -> Self {
        LogTableDebugLoader {
            log_path: None,
            complated: HashMap::new(),
            incomplated: BTreeMap::new(),
            buf: Vec::new(),
        }
    }

    /// 获取已完成事务表
    pub fn complated(&self) -> &HashMap<Guid, (Guid, usize, usize, usize)> {
        &self.complated
    }

    /// 获取未完成事务表
    pub fn incomplated(&self) -> &BTreeMap<Guid, (Option<Guid>, usize, usize, usize)> {
        &self.incomplated
    }
}

const QUICK_REPAIR_LOG_ORD_TABLE: &str = "repair_log_ord";
const QUICK_REPAIR_LOG_WRITE_TABLE: &str = "repair_log_write";
const QUICK_REPAIR_BTREE_TABLE: &str = "repair_btree";
const QUICK_REPAIR_MEM_TABLE: &str = "repair_mem";
const QUICK_REPAIR_RECREATE_TABLE: &str = "repair_recreate";
const QUICK_REPAIR_TMP_ROOT: &str = "./tmp_quick_repair";
const QUICK_REPAIR_BTREE_DEF: TableDefinition<Binary, Binary> = TableDefinition::new("$default");

#[derive(Debug, PartialEq, Eq)]
struct QuickRepairSnapshot {
    tables:      Vec<String>,
    log_ord:     BTreeMap<usize, usize>,
    log_write:   BTreeMap<usize, Option<usize>>,
    btree:       BTreeMap<usize, usize>,
    mem_ord:     BTreeMap<usize, usize>,
}

#[derive(Debug, PartialEq, Eq)]
struct QuickRepairDiskState {
    meta_tables: Vec<String>,
    log_ord:     BTreeMap<usize, usize>,
    log_write:   BTreeMap<usize, usize>,
    btree:       BTreeMap<usize, usize>,
}

#[derive(Debug, PartialEq, Eq)]
struct RecreateTableSnapshot {
    tables:   Vec<String>,
    recreate: BTreeMap<usize, usize>,
}

#[derive(Debug, PartialEq, Eq)]
struct RecreateTableDiskState {
    meta_tables: Vec<String>,
    recreate:    BTreeMap<usize, usize>,
}

struct LatestLogTableLoader {
    entries: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl LatestLogTableLoader {
    fn new() -> Self {
        LatestLogTableLoader {
            entries: HashMap::new(),
        }
    }
}

impl PairLoader for LatestLogTableLoader {
    fn is_require(&self, _log_file: Option<&PathBuf>, key: &Vec<u8>) -> bool {
        !self.entries.contains_key(key)
    }

    fn load(&mut self,
            _log_file: Option<&PathBuf>,
            _method: LogMethod,
            key: Vec<u8>,
            value: Option<Vec<u8>>) {
        self.entries.insert(key, value);
    }
}

static QUICK_REPAIR_TEST_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();

// quick repair 用例会创建真实数据库目录并拉起子进程制造未确认 commit log。
// 默认测试线程并行执行时，这几条用例会互相争抢资源并放大超时风险，因此在进程内串行化。
fn quick_repair_test_guard() -> std::sync::MutexGuard<'static, ()> {
    QUICK_REPAIR_TEST_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

// 用真实未确认提交日志验证 quick repair：
// 1. 子进程先生成 commit log 已落盘但未确认的 fixture；
// 2. 父进程启动数据库并走 TryQuickRepair；
// 3. 同时校验修复后的内存态和各持久化表的磁盘态。
#[test]
fn test_quick_repair_recovers_real_unconfirmed_commit_log() {
    let _guard = quick_repair_test_guard();

    if let Some(root) = quick_repair_child_root("quick_recovery_fixture") {
        generate_quick_repair_fixture(&root);
        return;
    }

    let root = quick_repair_tmp_dir("real_recovery");
    spawn_quick_repair_fixture("test_quick_repair_recovers_real_unconfirmed_commit_log",
                               "quick_recovery_fixture",
                               &root);

    let (snapshot, disk_state) = startup_db_and_collect_quick_repair_state(&root,
                                                                            DBStartupRepairMode::TryQuickRepair);

    assert_eq!(snapshot, expected_quick_repair_snapshot());
    assert_eq!(disk_state, expected_quick_repair_disk_state());
}

// 对同一份 .table 和 .commit_log 分别执行 try_repair 与 try_quick_repair，
// 通过比较修复后的表数据判断 quick repair 是否与旧 repair 结果一致。
#[test]
fn test_try_repair_and_try_quick_repair_are_consistent() {
    let _guard = quick_repair_test_guard();

    if let Some(root) = quick_repair_child_root("quick_compare_fixture") {
        generate_quick_repair_fixture(&root);
        return;
    }

    let base_root = quick_repair_tmp_dir("compare/source");
    let try_repair_root = quick_repair_tmp_dir("compare/try_repair");
    let try_quick_repair_root = quick_repair_tmp_dir("compare/try_quick_repair");

    spawn_quick_repair_fixture("test_try_repair_and_try_quick_repair_are_consistent",
                               "quick_compare_fixture",
                               &base_root);
    copy_dir_all(&base_root, &try_repair_root);
    copy_dir_all(&base_root, &try_quick_repair_root);

    let try_repair_snapshot = startup_db_and_snapshot(&try_repair_root,
                                                      DBStartupRepairMode::TryRepair);
    let try_quick_repair_snapshot = startup_db_and_snapshot(&try_quick_repair_root,
                                                            DBStartupRepairMode::TryQuickRepair);

    assert_eq!(try_repair_snapshot, try_quick_repair_snapshot);
    assert_eq!(try_quick_repair_snapshot, expected_quick_repair_snapshot());
}

// 用元信息表 create/remove 的真实 crash fixture 对照旧 repair 和 quick repair：
// 1. 两条修复路径必须恢复出完全一致的逻辑视图和磁盘视图；
// 2. 该用例同时覆盖 repair 删除表时的元信息 key 解码路径。
#[test]
fn test_try_repair_and_try_quick_repair_meta_fixture_are_consistent() {
    let _guard = quick_repair_test_guard();

    if let Some(root) = quick_repair_child_root("quick_meta_compare_fixture") {
        generate_quick_repair_meta_fixture(&root);
        return;
    }

    let base_root = quick_repair_tmp_dir("meta_compare/source");
    let try_repair_root = quick_repair_tmp_dir("meta_compare/try_repair");
    let try_quick_repair_root = quick_repair_tmp_dir("meta_compare/try_quick_repair");

    spawn_quick_repair_fixture("test_try_repair_and_try_quick_repair_meta_fixture_are_consistent",
                               "quick_meta_compare_fixture",
                               &base_root);

    let (try_repair_snapshot, try_quick_repair_snapshot) = compare_repair_snapshots_on_fixture(&base_root,
                                                                                                &try_repair_root,
                                                                                                &try_quick_repair_root);

    assert_eq!(try_repair_snapshot, try_quick_repair_snapshot);
    assert_eq!(try_quick_repair_snapshot, expected_quick_repair_meta_snapshot());
}

// 验证 quick repair 对元信息表的真实崩溃恢复：
// 1. 未确认提交日志中同时包含“创建新表”和“删除旧表”；
// 2. 修复后元信息表和已加载表集合必须一致，删除表的数据不能再参与恢复；
// 3. 新建表的数据也必须随同元信息恢复成功。
#[test]
fn test_quick_repair_meta_create_remove_real_unconfirmed_commit_log() {
    let _guard = quick_repair_test_guard();

    if let Some(root) = quick_repair_child_root("quick_meta_fixture") {
        generate_quick_repair_meta_fixture(&root);
        return;
    }

    let root = quick_repair_tmp_dir("meta_recovery");
    spawn_quick_repair_fixture("test_quick_repair_meta_create_remove_real_unconfirmed_commit_log",
                               "quick_meta_fixture",
                               &root);

    let (snapshot, disk_state) = startup_db_and_collect_quick_repair_state(&root,
                                                                            DBStartupRepairMode::TryQuickRepair);

    assert_eq!(snapshot, expected_quick_repair_meta_snapshot());
    assert_eq!(disk_state, expected_quick_repair_meta_disk_state());
}

// 验证 quick repair 对只写日志表的真实崩溃恢复：
// 1. 同一个未确认事务中，同 key 的多次 upsert 必须只保留最终值；
// 2. 修复完成返回时，日志表数据必须已经落盘，而不是只停留在等待队列。
#[test]
fn test_quick_repair_log_write_real_unconfirmed_commit_log() {
    let _guard = quick_repair_test_guard();

    if let Some(root) = quick_repair_child_root("quick_log_write_fixture") {
        generate_quick_repair_log_write_fixture(&root);
        return;
    }

    let root = quick_repair_tmp_dir("log_write_recovery");
    spawn_quick_repair_fixture("test_quick_repair_log_write_real_unconfirmed_commit_log",
                               "quick_log_write_fixture",
                               &root);

    let (snapshot, disk_state) = startup_db_and_collect_quick_repair_state(&root,
                                                                            DBStartupRepairMode::TryQuickRepair);

    assert_eq!(snapshot, expected_quick_repair_log_write_snapshot());
    assert_eq!(disk_state, expected_quick_repair_log_write_disk_state());
}

// 用只写日志表的真实 crash fixture 对照旧 repair 和 quick repair：
// 1. 同一 key 的多次 upsert 结果必须一致；
// 2. quick repair 的快速 flush 不得改变最终磁盘状态。
#[test]
fn test_try_repair_and_try_quick_repair_log_write_fixture_are_consistent() {
    let _guard = quick_repair_test_guard();

    if let Some(root) = quick_repair_child_root("quick_log_write_compare_fixture") {
        generate_quick_repair_log_write_fixture(&root);
        return;
    }

    let base_root = quick_repair_tmp_dir("log_write_compare/source");
    let try_repair_root = quick_repair_tmp_dir("log_write_compare/try_repair");
    let try_quick_repair_root = quick_repair_tmp_dir("log_write_compare/try_quick_repair");

    spawn_quick_repair_fixture("test_try_repair_and_try_quick_repair_log_write_fixture_are_consistent",
                               "quick_log_write_compare_fixture",
                               &base_root);

    let (try_repair_snapshot, try_quick_repair_snapshot) = compare_repair_snapshots_on_fixture(&base_root,
                                                                                                &try_repair_root,
                                                                                                &try_quick_repair_root);

    assert_eq!(try_repair_snapshot, try_quick_repair_snapshot);
    assert_eq!(try_quick_repair_snapshot, expected_quick_repair_log_write_snapshot());
}

// 验证 quick repair 对 BTree 表的真实崩溃恢复：
// 1. 未确认提交日志里的更新、插入和删除必须按顺序生效；
// 2. 逻辑视图和物理 `table.dat` 中的最终结果必须一致。
#[test]
fn test_quick_repair_btree_real_unconfirmed_commit_log() {
    let _guard = quick_repair_test_guard();

    if let Some(root) = quick_repair_child_root("quick_btree_fixture") {
        generate_quick_repair_btree_fixture(&root);
        return;
    }

    let root = quick_repair_tmp_dir("btree_recovery");
    spawn_quick_repair_fixture("test_quick_repair_btree_real_unconfirmed_commit_log",
                               "quick_btree_fixture",
                               &root);

    let (snapshot, disk_state) = startup_db_and_collect_quick_repair_state(&root,
                                                                            DBStartupRepairMode::TryQuickRepair);

    assert_eq!(snapshot, expected_quick_repair_btree_snapshot());
    assert_eq!(disk_state, expected_quick_repair_btree_disk_state());
}

// 验证 quick repair 在多个未确认事务顺序 replay 下的真实恢复：
// 1. 同一批 key 会跨多个未确认事务被反复更新、删除和重建；
// 2. 修复后必须体现“按事务顺序最终生效”的结果；
// 3. quick repair 返回时，涉及的持久化表也必须已经落盘。
#[test]
fn test_quick_repair_multi_transaction_real_unconfirmed_commit_log() {
    let _guard = quick_repair_test_guard();

    if let Some(root) = quick_repair_child_root("quick_multi_transaction_fixture") {
        generate_quick_repair_multi_transaction_fixture(&root);
        return;
    }

    let root = quick_repair_tmp_dir("multi_transaction_recovery");
    spawn_quick_repair_fixture("test_quick_repair_multi_transaction_real_unconfirmed_commit_log",
                               "quick_multi_transaction_fixture",
                               &root);

    let (snapshot, disk_state) = startup_db_and_collect_quick_repair_state(&root,
                                                                            DBStartupRepairMode::TryQuickRepair);

    assert_eq!(snapshot, expected_quick_repair_multi_transaction_snapshot());
    assert_eq!(disk_state, expected_quick_repair_multi_transaction_disk_state());
}

// 用多事务未确认提交日志对照旧 repair 和 quick repair：
// 1. 多个事务对同一批 key 的顺序覆盖结果必须一致；
// 2. quick repair 不得因为快装载而破坏跨事务顺序语义。
#[test]
fn test_try_repair_and_try_quick_repair_multi_transaction_fixture_are_consistent() {
    let _guard = quick_repair_test_guard();

    if let Some(root) = quick_repair_child_root("quick_multi_transaction_compare_fixture") {
        generate_quick_repair_multi_transaction_fixture(&root);
        return;
    }

    let base_root = quick_repair_tmp_dir("multi_transaction_compare/source");
    let try_repair_root = quick_repair_tmp_dir("multi_transaction_compare/try_repair");
    let try_quick_repair_root = quick_repair_tmp_dir("multi_transaction_compare/try_quick_repair");

    spawn_quick_repair_fixture("test_try_repair_and_try_quick_repair_multi_transaction_fixture_are_consistent",
                               "quick_multi_transaction_compare_fixture",
                               &base_root);

    let (try_repair_snapshot, try_quick_repair_snapshot) = compare_repair_snapshots_on_fixture(&base_root,
                                                                                                &try_repair_root,
                                                                                                &try_quick_repair_root);

    assert_eq!(try_repair_snapshot, try_quick_repair_snapshot);
    assert_eq!(try_quick_repair_snapshot, expected_quick_repair_multi_transaction_snapshot());
}

// 用 BTree 表的真实 crash fixture 对照旧 repair 和 quick repair：
// 1. 更新、插入、删除顺序必须一致；
// 2. `table.dat` 的最终内容也必须一致。
#[test]
fn test_try_repair_and_try_quick_repair_btree_fixture_are_consistent() {
    let _guard = quick_repair_test_guard();

    if let Some(root) = quick_repair_child_root("quick_btree_compare_fixture") {
        generate_quick_repair_btree_fixture(&root);
        return;
    }

    let base_root = quick_repair_tmp_dir("btree_compare/source");
    let try_repair_root = quick_repair_tmp_dir("btree_compare/try_repair");
    let try_quick_repair_root = quick_repair_tmp_dir("btree_compare/try_quick_repair");

    spawn_quick_repair_fixture("test_try_repair_and_try_quick_repair_btree_fixture_are_consistent",
                               "quick_btree_compare_fixture",
                               &base_root);

    let (try_repair_snapshot, try_quick_repair_snapshot) = compare_repair_snapshots_on_fixture(&base_root,
                                                                                                &try_repair_root,
                                                                                                &try_quick_repair_root);

    assert_eq!(try_repair_snapshot, try_quick_repair_snapshot);
    assert_eq!(try_quick_repair_snapshot, expected_quick_repair_btree_snapshot());
}

// 验证 quick repair 对“删除表后重建同名表”的真实恢复：
// 1. 未确认事务中先删除旧表，再以同名新表继续写入；
// 2. 修复后应只看到最终重建出来的新表数据；
// 3. 该场景同时验证元信息表在多事务下的表生命周期顺序。
#[test]
fn test_quick_repair_recreate_table_real_unconfirmed_commit_log() {
    let _guard = quick_repair_test_guard();

    if let Some(root) = quick_repair_child_root("quick_recreate_fixture") {
        generate_quick_repair_recreate_fixture(&root);
        return;
    }

    let root = quick_repair_tmp_dir("recreate_recovery");
    spawn_quick_repair_fixture("test_quick_repair_recreate_table_real_unconfirmed_commit_log",
                               "quick_recreate_fixture",
                               &root);

    let (snapshot, disk_state) = startup_db_and_collect_recreate_state(&root,
                                                                       DBStartupRepairMode::TryQuickRepair);

    assert_eq!(snapshot, expected_recreate_table_snapshot());
    assert_eq!(disk_state, expected_recreate_table_disk_state());
}

// 用“删除后重建同名表”的真实 crash fixture 对照旧 repair 和 quick repair：
// 1. 旧 repair 与 quick repair 对表生命周期顺序的理解必须一致；
// 2. 最终可见表集合和新表数据必须完全一致。
#[test]
fn test_try_repair_and_try_quick_repair_recreate_fixture_are_consistent() {
    let _guard = quick_repair_test_guard();

    if let Some(root) = quick_repair_child_root("quick_recreate_compare_fixture") {
        generate_quick_repair_recreate_fixture(&root);
        return;
    }

    let base_root = quick_repair_tmp_dir("recreate_compare/source");
    let try_repair_root = quick_repair_tmp_dir("recreate_compare/try_repair");
    let try_quick_repair_root = quick_repair_tmp_dir("recreate_compare/try_quick_repair");

    spawn_quick_repair_fixture("test_try_repair_and_try_quick_repair_recreate_fixture_are_consistent",
                               "quick_recreate_compare_fixture",
                               &base_root);

    let (try_repair_snapshot, try_quick_repair_snapshot) = compare_recreate_snapshots_on_fixture(&base_root,
                                                                                                  &try_repair_root,
                                                                                                  &try_quick_repair_root);

    assert_eq!(try_repair_snapshot, try_quick_repair_snapshot);
    assert_eq!(try_quick_repair_snapshot, expected_recreate_table_snapshot());
}

// 如果当前测试进程是被父测试拉起的子进程，则返回子进程专用的 fixture 根目录。
// 这样同一个测试函数既能负责生成 fixture，也能负责父进程侧的恢复验证。
fn quick_repair_child_root(expected_mode: &str) -> Option<PathBuf> {
    match std::env::var("PI_DB_QUICK_REPAIR_CHILD_MODE") {
        Err(_) => None,
        Ok(mode) => {
            if mode == expected_mode {
                Some(PathBuf::from(std::env::var("PI_DB_QUICK_REPAIR_CHILD_ROOT").unwrap()))
            } else {
                None
            }
        },
    }
}

// quick repair 集成测试会生成真实数据库目录。
// 所有临时目录统一收敛到 `./tmp_quick_repair/` 下，便于集中清理。
fn quick_repair_tmp_dir(name: &str) -> PathBuf {
    PathBuf::from(QUICK_REPAIR_TMP_ROOT).join(name)
}

// 拉起当前测试二进制的子进程，专门制造“提交日志已写但未确认”的真实 fixture。
fn spawn_quick_repair_fixture(test_name: &str,
                              mode: &str,
                              root: &std::path::Path) {
    remove_dir_if_exists(root);
    if let Some(parent) = root.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env("PI_DB_QUICK_REPAIR_CHILD_MODE", mode)
        .env("PI_DB_QUICK_REPAIR_CHILD_ROOT", root.to_str().unwrap())
        .status()
        .unwrap();

    assert!(status.success(),
            "spawn quick repair fixture failed, test: {:?}, mode: {:?}, root: {:?}, status: {:?}",
            test_name,
            mode,
            root,
            status);
}

// 生成 quick repair 测试所需的真实数据库目录：
// - 创建四张表，覆盖元信息表、日志表、BTree 表和内存表；
// - 提交两批持久化写；
// - 保留一份未确认 commit log，供后续恢复测试直接复用。
fn generate_quick_repair_fixture(root: &std::path::Path) {
    remove_dir_if_exists(root);
    std::fs::create_dir_all(root).unwrap();

    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();
    let root_copy = root.to_path_buf();
    let (sender, receiver) = bounded(1);

    let _ = rt.spawn(async move {
        let guid_gen = GuidGen::new(run_nanos(), 0);
        let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), root_copy.join(".commit_log"));
        let commit_logger = commit_logger_builder
            .log_file_limit(1024)
            .build()
            .await
            .unwrap();

        let tr_mgr = Transaction2PcManager::new(rt_copy.clone(),
                                                guid_gen,
                                                commit_logger);

        let builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, root_copy.join("db"));
        let db = builder.startup_by_repair(true, DBStartupRepairMode::TryQuickRepair)
            .await
            .unwrap();

        let log_ord_table = Atom::from(QUICK_REPAIR_LOG_ORD_TABLE);
        let log_write_table = Atom::from(QUICK_REPAIR_LOG_WRITE_TABLE);
        let btree_table = Atom::from(QUICK_REPAIR_BTREE_TABLE);
        let mem_table = Atom::from(QUICK_REPAIR_MEM_TABLE);

        let tr = db.transaction(Atom::from("quick repair fixture create table"), true, 500, 500).unwrap();
        tr.create_table(log_ord_table.clone(),
                        KVTableMeta::new(KVDBTableType::LogOrdTab,
                                         true,
                                         EnumType::Usize,
                                         EnumType::Usize),
                        true).await.unwrap();
        tr.create_table(log_write_table.clone(),
                        KVTableMeta::new(KVDBTableType::LogWTab,
                                         true,
                                         EnumType::Usize,
                                         EnumType::Usize),
                        true).await.unwrap();
        tr.create_table(btree_table.clone(),
                        KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                         true,
                                         EnumType::Usize,
                                         EnumType::Usize),
                        true).await.unwrap();
        tr.create_table(mem_table.clone(),
                        KVTableMeta::new(KVDBTableType::MemOrdTab,
                                         false,
                                         EnumType::Usize,
                                         EnumType::Usize),
                        true).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair fixture insert"), true, 500, 500).unwrap();
        tr.upsert(vec![
            TableKV::new(log_ord_table.clone(), usize_to_binary(1), Some(usize_to_binary(1000))),
            TableKV::new(log_ord_table.clone(), usize_to_binary(2), Some(usize_to_binary(1002))),
            TableKV::new(log_ord_table.clone(), usize_to_binary(3), Some(usize_to_binary(1003))),
            TableKV::new(log_write_table.clone(), usize_to_binary(5), Some(usize_to_binary(3000))),
            TableKV::new(log_write_table.clone(), usize_to_binary(6), Some(usize_to_binary(3002))),
            TableKV::new(btree_table.clone(), usize_to_binary(10), Some(usize_to_binary(2000))),
            TableKV::new(btree_table.clone(), usize_to_binary(11), Some(usize_to_binary(2002))),
            TableKV::new(mem_table.clone(), usize_to_binary(99), Some(usize_to_binary(9999))),
        ]).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair fixture update"), true, 500, 500).unwrap();
        tr.upsert(vec![
            TableKV::new(log_ord_table.clone(), usize_to_binary(1), Some(usize_to_binary(1001))),
            TableKV::new(log_write_table.clone(), usize_to_binary(5), Some(usize_to_binary(3001))),
            TableKV::new(btree_table.clone(), usize_to_binary(10), Some(usize_to_binary(2001))),
            TableKV::new(mem_table.clone(), usize_to_binary(99), Some(usize_to_binary(10000))),
        ]).await.unwrap();
        tr.delete(vec![
            TableKV::new(log_ord_table.clone(), usize_to_binary(2), None),
            TableKV::new(btree_table.clone(), usize_to_binary(11), None),
        ]).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let _ = sender.send(());
    });

    receiver.recv_timeout(Duration::from_secs(60)).unwrap();
}

// 生成元信息表 create/remove 的真实修复 fixture：
// - 先创建并持久化 `repair_log_ord` 和 `repair_log_write`；
// - 再在未确认事务中删除 `repair_log_write`，同时创建 `repair_btree` 并写入数据；
// - 用于验证 quick repair 对元信息表和物理目录的一致恢复。
fn generate_quick_repair_meta_fixture(root: &std::path::Path) {
    remove_dir_if_exists(root);
    std::fs::create_dir_all(root).unwrap();

    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();
    let root_copy = root.to_path_buf();
    let (sender, receiver) = bounded(1);

    let _ = rt.spawn(async move {
        let db = startup_quick_repair_test_db(rt_copy.clone(),
                                              root_copy.clone(),
                                              DBStartupRepairMode::TryQuickRepair).await;

        let log_ord_table = Atom::from(QUICK_REPAIR_LOG_ORD_TABLE);
        let log_write_table = Atom::from(QUICK_REPAIR_LOG_WRITE_TABLE);
        let btree_table = Atom::from(QUICK_REPAIR_BTREE_TABLE);

        let tr = db.transaction(Atom::from("quick repair meta fixture create table"), true, 500, 500).unwrap();
        tr.create_table(log_ord_table.clone(),
                        KVTableMeta::new(KVDBTableType::LogOrdTab,
                                         true,
                                         EnumType::Usize,
                                         EnumType::Usize),
                        true).await.unwrap();
        tr.create_table(log_write_table.clone(),
                        KVTableMeta::new(KVDBTableType::LogWTab,
                                         true,
                                         EnumType::Usize,
                                         EnumType::Usize),
                        true).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair meta fixture insert"), true, 500, 500).unwrap();
        tr.upsert(vec![
            TableKV::new(log_ord_table.clone(), usize_to_binary(1), Some(usize_to_binary(101))),
            TableKV::new(log_write_table.clone(), usize_to_binary(5), Some(usize_to_binary(501))),
        ]).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair meta fixture update"), true, 500, 500).unwrap();
        tr.create_table(btree_table.clone(),
                        KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                         true,
                                         EnumType::Usize,
                                         EnumType::Usize),
                        true).await.unwrap();
        tr.upsert(vec![
            TableKV::new(log_ord_table.clone(), usize_to_binary(2), Some(usize_to_binary(102))),
            TableKV::new(btree_table.clone(), usize_to_binary(10), Some(usize_to_binary(2001))),
        ]).await.unwrap();
        tr.remove_table(log_write_table.clone()).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let _ = sender.send(());
    });

    receiver.recv_timeout(Duration::from_secs(60)).unwrap();
}

// 生成只写日志表的真实修复 fixture：
// - 先持久化旧值；
// - 再在未确认事务中对同一 key 连续多次 upsert；
// - 用于验证 quick repair 只保留该事务内的最终值，并确保数据立即落盘。
fn generate_quick_repair_log_write_fixture(root: &std::path::Path) {
    remove_dir_if_exists(root);
    std::fs::create_dir_all(root).unwrap();

    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();
    let root_copy = root.to_path_buf();
    let (sender, receiver) = bounded(1);

    let _ = rt.spawn(async move {
        let db = startup_quick_repair_test_db(rt_copy.clone(),
                                              root_copy.clone(),
                                              DBStartupRepairMode::TryQuickRepair).await;

        let log_write_table = Atom::from(QUICK_REPAIR_LOG_WRITE_TABLE);

        let tr = db.transaction(Atom::from("quick repair log write fixture create table"), true, 500, 500).unwrap();
        tr.create_table(log_write_table.clone(),
                        KVTableMeta::new(KVDBTableType::LogWTab,
                                         true,
                                         EnumType::Usize,
                                         EnumType::Usize),
                        true).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair log write fixture insert"), true, 500, 500).unwrap();
        tr.upsert(vec![
            TableKV::new(log_write_table.clone(), usize_to_binary(5), Some(usize_to_binary(3000))),
            TableKV::new(log_write_table.clone(), usize_to_binary(6), Some(usize_to_binary(3001))),
        ]).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair log write fixture update"), true, 500, 500).unwrap();
        tr.upsert(vec![
            TableKV::new(log_write_table.clone(), usize_to_binary(5), Some(usize_to_binary(3100))),
            TableKV::new(log_write_table.clone(), usize_to_binary(5), Some(usize_to_binary(3101))),
            TableKV::new(log_write_table.clone(), usize_to_binary(7), Some(usize_to_binary(3007))),
        ]).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let _ = sender.send(());
    });

    receiver.recv_timeout(Duration::from_secs(60)).unwrap();
}

// 生成 BTree 表的真实修复 fixture：
// - 先写入稳定基线数据；
// - 再在未确认事务中同时执行更新、删除和新增；
// - 用于验证 quick repair 对 BTree tombstone 和最终落盘结果的处理。
fn generate_quick_repair_btree_fixture(root: &std::path::Path) {
    remove_dir_if_exists(root);
    std::fs::create_dir_all(root).unwrap();

    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();
    let root_copy = root.to_path_buf();
    let (sender, receiver) = bounded(1);

    let _ = rt.spawn(async move {
        let db = startup_quick_repair_test_db(rt_copy.clone(),
                                              root_copy.clone(),
                                              DBStartupRepairMode::TryQuickRepair).await;

        let btree_table = Atom::from(QUICK_REPAIR_BTREE_TABLE);

        let tr = db.transaction(Atom::from("quick repair btree fixture create table"), true, 500, 500).unwrap();
        tr.create_table(btree_table.clone(),
                        KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                         true,
                                         EnumType::Usize,
                                         EnumType::Usize),
                        true).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair btree fixture insert"), true, 500, 500).unwrap();
        tr.upsert(vec![
            TableKV::new(btree_table.clone(), usize_to_binary(10), Some(usize_to_binary(2000))),
            TableKV::new(btree_table.clone(), usize_to_binary(11), Some(usize_to_binary(2001))),
            TableKV::new(btree_table.clone(), usize_to_binary(13), Some(usize_to_binary(2003))),
        ]).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair btree fixture update"), true, 500, 500).unwrap();
        tr.upsert(vec![
            TableKV::new(btree_table.clone(), usize_to_binary(10), Some(usize_to_binary(2100))),
            TableKV::new(btree_table.clone(), usize_to_binary(12), Some(usize_to_binary(2102))),
        ]).await.unwrap();
        tr.delete(vec![
            TableKV::new(btree_table.clone(), usize_to_binary(11), None),
        ]).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let _ = sender.send(());
    });

    receiver.recv_timeout(Duration::from_secs(60)).unwrap();
}

// 生成多未确认事务顺序 replay 的真实修复 fixture：
// - 先写入一批稳定基线数据；
// - 再连续提交多笔会互相覆盖、删除、重建同 key 的事务；
// - 用于验证 quick repair 和旧 repair 在跨事务顺序上的一致性。
fn generate_quick_repair_multi_transaction_fixture(root: &std::path::Path) {
    remove_dir_if_exists(root);
    std::fs::create_dir_all(root).unwrap();

    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();
    let root_copy = root.to_path_buf();
    let (sender, receiver) = bounded(1);

    let _ = rt.spawn(async move {
        let db = startup_quick_repair_test_db(rt_copy.clone(),
                                              root_copy.clone(),
                                              DBStartupRepairMode::TryQuickRepair).await;

        let log_ord_table = Atom::from(QUICK_REPAIR_LOG_ORD_TABLE);
        let log_write_table = Atom::from(QUICK_REPAIR_LOG_WRITE_TABLE);
        let btree_table = Atom::from(QUICK_REPAIR_BTREE_TABLE);

        let tr = db.transaction(Atom::from("quick repair multi transaction fixture create table"), true, 500, 500).unwrap();
        tr.create_table(log_ord_table.clone(),
                        KVTableMeta::new(KVDBTableType::LogOrdTab,
                                         true,
                                         EnumType::Usize,
                                         EnumType::Usize),
                        true).await.unwrap();
        tr.create_table(log_write_table.clone(),
                        KVTableMeta::new(KVDBTableType::LogWTab,
                                         true,
                                         EnumType::Usize,
                                         EnumType::Usize),
                        true).await.unwrap();
        tr.create_table(btree_table.clone(),
                        KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                         true,
                                         EnumType::Usize,
                                         EnumType::Usize),
                        true).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair multi transaction fixture base"), true, 500, 500).unwrap();
        tr.upsert(vec![
            TableKV::new(log_ord_table.clone(), usize_to_binary(1), Some(usize_to_binary(10))),
            TableKV::new(log_ord_table.clone(), usize_to_binary(2), Some(usize_to_binary(20))),
            TableKV::new(log_write_table.clone(), usize_to_binary(5), Some(usize_to_binary(50))),
            TableKV::new(btree_table.clone(), usize_to_binary(10), Some(usize_to_binary(100))),
        ]).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair multi transaction fixture tx1"), true, 500, 500).unwrap();
        tr.upsert(vec![
            TableKV::new(log_ord_table.clone(), usize_to_binary(1), Some(usize_to_binary(11))),
            TableKV::new(log_write_table.clone(), usize_to_binary(5), Some(usize_to_binary(51))),
            TableKV::new(btree_table.clone(), usize_to_binary(11), Some(usize_to_binary(110))),
        ]).await.unwrap();
        tr.delete(vec![
            TableKV::new(btree_table.clone(), usize_to_binary(10), None),
        ]).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair multi transaction fixture tx2"), true, 500, 500).unwrap();
        tr.upsert(vec![
            TableKV::new(log_ord_table.clone(), usize_to_binary(2), Some(usize_to_binary(22))),
            TableKV::new(log_ord_table.clone(), usize_to_binary(3), Some(usize_to_binary(33))),
            TableKV::new(log_write_table.clone(), usize_to_binary(5), Some(usize_to_binary(52))),
            TableKV::new(log_write_table.clone(), usize_to_binary(6), Some(usize_to_binary(60))),
            TableKV::new(btree_table.clone(), usize_to_binary(10), Some(usize_to_binary(101))),
        ]).await.unwrap();
        tr.delete(vec![
            TableKV::new(log_ord_table.clone(), usize_to_binary(1), None),
        ]).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair multi transaction fixture tx3"), true, 500, 500).unwrap();
        tr.upsert(vec![
            TableKV::new(log_ord_table.clone(), usize_to_binary(1), Some(usize_to_binary(13))),
            TableKV::new(btree_table.clone(), usize_to_binary(11), Some(usize_to_binary(111))),
            TableKV::new(btree_table.clone(), usize_to_binary(12), Some(usize_to_binary(120))),
        ]).await.unwrap();
        tr.delete(vec![
            TableKV::new(log_write_table.clone(), usize_to_binary(6), None),
        ]).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let _ = sender.send(());
    });

    receiver.recv_timeout(Duration::from_secs(60)).unwrap();
}

// 生成“删除后重建同名表”的真实修复 fixture：
// - 先创建一个日志有序表并写入旧数据；
// - 再在后续事务中删除该表，并以相同名称创建 BTree 表；
// - 用于验证 quick repair 对表生命周期顺序的恢复。
fn generate_quick_repair_recreate_fixture(root: &std::path::Path) {
    remove_dir_if_exists(root);
    std::fs::create_dir_all(root).unwrap();

    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();
    let root_copy = root.to_path_buf();
    let (sender, receiver) = bounded(1);

    let _ = rt.spawn(async move {
        let db = startup_quick_repair_test_db(rt_copy.clone(),
                                              root_copy.clone(),
                                              DBStartupRepairMode::TryQuickRepair).await;

        let recreate_table = Atom::from(QUICK_REPAIR_RECREATE_TABLE);

        let tr = db.transaction(Atom::from("quick repair recreate fixture create log table"), true, 500, 500).unwrap();
        tr.create_table(recreate_table.clone(),
                        KVTableMeta::new(KVDBTableType::LogOrdTab,
                                         true,
                                         EnumType::Usize,
                                         EnumType::Usize),
                        true).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair recreate fixture insert old data"), true, 500, 500).unwrap();
        tr.upsert(vec![
            TableKV::new(recreate_table.clone(), usize_to_binary(1), Some(usize_to_binary(111))),
            TableKV::new(recreate_table.clone(), usize_to_binary(2), Some(usize_to_binary(222))),
        ]).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair recreate fixture remove old table"), true, 500, 500).unwrap();
        tr.remove_table(recreate_table.clone()).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair recreate fixture create new btree table"), true, 500, 500).unwrap();
        tr.create_table(recreate_table.clone(),
                        KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                         true,
                                         EnumType::Usize,
                                         EnumType::Usize),
                        true).await.unwrap();
        tr.upsert(vec![
            TableKV::new(recreate_table.clone(), usize_to_binary(8), Some(usize_to_binary(800))),
            TableKV::new(recreate_table.clone(), usize_to_binary(9), Some(usize_to_binary(900))),
        ]).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let tr = db.transaction(Atom::from("quick repair recreate fixture update new table"), true, 500, 500).unwrap();
        tr.upsert(vec![
            TableKV::new(recreate_table.clone(), usize_to_binary(8), Some(usize_to_binary(801))),
            TableKV::new(recreate_table.clone(), usize_to_binary(10), Some(usize_to_binary(1000))),
        ]).await.unwrap();
        tr.delete(vec![
            TableKV::new(recreate_table.clone(), usize_to_binary(9), None),
        ]).await.unwrap();
        let output = tr.prepare_modified().await.unwrap();
        tr.commit_modified(output).await.unwrap();

        let _ = sender.send(());
    });

    receiver.recv_timeout(Duration::from_secs(60)).unwrap();
}

// 按统一参数启动 quick repair 相关测试数据库。
// 这个 helper 会创建 commit logger、事务管理器和数据库实例，并按指定修复模式启动。
async fn startup_quick_repair_test_db(rt_copy: pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
                                      root_copy: PathBuf,
                                      repair_mode: DBStartupRepairMode)
    -> pi_db::db::KVDBManager<usize, pi_store::commit_logger::CommitLogger> {
    let guid_gen = GuidGen::new(run_nanos(), 0);
    let commit_logger_builder = CommitLoggerBuilder::new(rt_copy.clone(), root_copy.join(".commit_log"));
    let commit_logger = commit_logger_builder
        .log_file_limit(1024)
        .build()
        .await
        .unwrap();

    let tr_mgr = Transaction2PcManager::new(rt_copy.clone(),
                                            guid_gen,
                                            commit_logger);

    let builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, root_copy.join("db"));
    builder.startup_by_repair(true, repair_mode).await.unwrap()
}

// 以指定修复模式启动数据库，并采集修复完成后的逻辑视图快照。
// 这个快照用于比较不同 repair 路径的最终表数据是否一致。
fn startup_db_and_snapshot(root: &std::path::Path,
                           repair_mode: DBStartupRepairMode) -> QuickRepairSnapshot {
    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();
    let root_copy = root.to_path_buf();
    let (sender, receiver) = bounded(1);

    let _ = rt.spawn(async move {
        let db = startup_quick_repair_test_db(rt_copy.clone(), root_copy.clone(), repair_mode).await;

        let mut tables = db.tables().await
            .into_iter()
            .map(|table| table.as_str().to_string())
            .collect::<Vec<_>>();
        tables.sort();

        let tr = db.transaction(Atom::from("quick repair snapshot"), false, 500, 500).unwrap();
        let snapshot = QuickRepairSnapshot {
            tables,
            log_ord: read_table_values(&tr, Atom::from(QUICK_REPAIR_LOG_ORD_TABLE)).await,
            log_write: read_known_queries(&tr,
                                          Atom::from(QUICK_REPAIR_LOG_WRITE_TABLE),
                                          &[5, 6, 7]).await,
            btree: read_table_values(&tr, Atom::from(QUICK_REPAIR_BTREE_TABLE)).await,
            mem_ord: read_table_values(&tr, Atom::from(QUICK_REPAIR_MEM_TABLE)).await,
        };

        let _ = sender.send(snapshot);
    });

    receiver.recv_timeout(Duration::from_secs(60)).unwrap()
}

// 以指定修复模式启动数据库，并同时采集逻辑视图和磁盘视图。
// 逻辑视图用于验证事务可见结果，磁盘视图用于确认 quick flush 已真正落盘。
fn startup_db_and_collect_quick_repair_state(root: &std::path::Path,
                                             repair_mode: DBStartupRepairMode) -> (QuickRepairSnapshot, QuickRepairDiskState) {
    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();
    let root_copy = root.to_path_buf();
    let (sender, receiver) = bounded(1);

    let _ = rt.spawn(async move {
        let db = startup_quick_repair_test_db(rt_copy.clone(), root_copy.clone(), repair_mode).await;

        let mut tables = db.tables().await
            .into_iter()
            .map(|table| table.as_str().to_string())
            .collect::<Vec<_>>();
        tables.sort();

        let tr = db.transaction(Atom::from("quick repair disk snapshot"), false, 500, 500).unwrap();
        let snapshot = QuickRepairSnapshot {
            tables,
            log_ord: read_table_values(&tr, Atom::from(QUICK_REPAIR_LOG_ORD_TABLE)).await,
            log_write: read_known_queries(&tr,
                                          Atom::from(QUICK_REPAIR_LOG_WRITE_TABLE),
                                          &[5, 6, 7]).await,
            btree: read_table_values(&tr, Atom::from(QUICK_REPAIR_BTREE_TABLE)).await,
            mem_ord: read_table_values(&tr, Atom::from(QUICK_REPAIR_MEM_TABLE)).await,
        };

        let disk_state = QuickRepairDiskState {
            meta_tables: load_meta_table_names(rt_copy.clone(), root_copy.join("db/.tables_meta")).await,
            log_ord: load_usize_log_table_state(rt_copy.clone(), root_copy.join("db/.tables").join(QUICK_REPAIR_LOG_ORD_TABLE)).await,
            log_write: load_usize_log_table_state(rt_copy.clone(), root_copy.join("db/.tables").join(QUICK_REPAIR_LOG_WRITE_TABLE)).await,
            btree: load_btree_table_state(root_copy.join("db/.tables")
                                                   .join(QUICK_REPAIR_BTREE_TABLE)
                                                   .join("table.dat")),
        };

        let _ = sender.send((snapshot, disk_state));
    });

    receiver.recv_timeout(Duration::from_secs(60)).unwrap()
}

// 对同一份 crash fixture 分别执行 TryRepair 和 TryQuickRepair，
// 并返回两条修复路径的逻辑视图与磁盘视图，供一致性断言复用。
fn compare_repair_snapshots_on_fixture(base_root: &std::path::Path,
                                       try_repair_root: &std::path::Path,
                                       try_quick_repair_root: &std::path::Path)
    -> (QuickRepairSnapshot, QuickRepairSnapshot) {
    remove_dir_if_exists(try_repair_root);
    remove_dir_if_exists(try_quick_repair_root);
    copy_dir_all(base_root, try_repair_root);
    copy_dir_all(base_root, try_quick_repair_root);

    let try_repair_snapshot = startup_db_and_snapshot(try_repair_root,
                                                      DBStartupRepairMode::TryRepair);
    let try_quick_repair_snapshot = startup_db_and_snapshot(try_quick_repair_root,
                                                            DBStartupRepairMode::TryQuickRepair);

    (try_repair_snapshot, try_quick_repair_snapshot)
}

// 对“删除后重建同名表”的 crash fixture 分别执行 TryRepair 和 TryQuickRepair，
// 比较两条修复路径对表生命周期顺序的最终恢复结果。
fn compare_recreate_snapshots_on_fixture(base_root: &std::path::Path,
                                         try_repair_root: &std::path::Path,
                                         try_quick_repair_root: &std::path::Path)
    -> (RecreateTableSnapshot, RecreateTableSnapshot) {
    remove_dir_if_exists(try_repair_root);
    remove_dir_if_exists(try_quick_repair_root);
    copy_dir_all(base_root, try_repair_root);
    copy_dir_all(base_root, try_quick_repair_root);

    let try_repair_snapshot = startup_db_and_snapshot_recreate_table(try_repair_root,
                                                                     DBStartupRepairMode::TryRepair);
    let try_quick_repair_snapshot = startup_db_and_snapshot_recreate_table(try_quick_repair_root,
                                                                           DBStartupRepairMode::TryQuickRepair);

    (try_repair_snapshot, try_quick_repair_snapshot)
}

// 读取有序表或 BTree 表的全部逻辑值。
// 这里通过 values 迭代数据库可见结果，适合做修复后的最终状态断言。
async fn read_table_values<C, Log>(tr: &pi_db::db::KVDBTransaction<C, Log>,
                                   table_name: Atom) -> BTreeMap<usize, usize>
    where C: Clone + Send + 'static,
          Log: pi_async_transaction::AsyncCommitLog<C = C, Cid = Guid>
{
    let mut values_map = BTreeMap::new();

    if let Some(mut values) = tr.values(table_name, None, false).await {
        while let Some((key, value)) = values.next().await {
            values_map.insert(binary_to_usize(&key).unwrap(),
                              binary_to_usize(&value).unwrap());
        }
    }

    values_map
}

async fn read_known_queries<C, Log>(tr: &pi_db::db::KVDBTransaction<C, Log>,
                                    table_name: Atom,
                                    keys: &[usize]) -> BTreeMap<usize, Option<usize>>
    where C: Clone + Send + 'static,
          Log: pi_async_transaction::AsyncCommitLog<C = C, Cid = Guid>
{
    let table_kv_list = keys.iter()
        .map(|key| TableKV::new(table_name.clone(), usize_to_binary(*key), None))
        .collect::<Vec<_>>();
    let values = tr.query(table_kv_list).await;
    let mut result = BTreeMap::new();

    for (index, value) in values.into_iter().enumerate() {
        result.insert(keys[index], value.map(|bin| binary_to_usize(&bin).unwrap()));
    }

    result
}

// 启动数据库并采集“删除后重建同名表”场景的最终逻辑视图。
fn startup_db_and_snapshot_recreate_table(root: &std::path::Path,
                                          repair_mode: DBStartupRepairMode) -> RecreateTableSnapshot {
    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();
    let root_copy = root.to_path_buf();
    let (sender, receiver) = bounded(1);

    let _ = rt.spawn(async move {
        let db = startup_quick_repair_test_db(rt_copy.clone(), root_copy.clone(), repair_mode).await;

        let mut tables = db.tables().await
            .into_iter()
            .map(|table| table.as_str().to_string())
            .collect::<Vec<_>>();
        tables.sort();

        let tr = db.transaction(Atom::from("quick repair recreate snapshot"), false, 500, 500).unwrap();
        let snapshot = RecreateTableSnapshot {
            tables,
            recreate: read_table_values(&tr, Atom::from(QUICK_REPAIR_RECREATE_TABLE)).await,
        };

        let _ = sender.send(snapshot);
    });

    receiver.recv_timeout(Duration::from_secs(60)).unwrap()
}

// 启动数据库并采集“删除后重建同名表”场景的逻辑视图和磁盘视图。
fn startup_db_and_collect_recreate_state(root: &std::path::Path,
                                         repair_mode: DBStartupRepairMode) -> (RecreateTableSnapshot, RecreateTableDiskState) {
    let _handle = startup_global_time_loop(10);
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();
    let root_copy = root.to_path_buf();
    let (sender, receiver) = bounded(1);

    let _ = rt.spawn(async move {
        let db = startup_quick_repair_test_db(rt_copy.clone(), root_copy.clone(), repair_mode).await;

        let mut tables = db.tables().await
            .into_iter()
            .map(|table| table.as_str().to_string())
            .collect::<Vec<_>>();
        tables.sort();

        let tr = db.transaction(Atom::from("quick repair recreate disk snapshot"), false, 500, 500).unwrap();
        let snapshot = RecreateTableSnapshot {
            tables,
            recreate: read_table_values(&tr, Atom::from(QUICK_REPAIR_RECREATE_TABLE)).await,
        };

        let disk_state = RecreateTableDiskState {
            meta_tables: load_meta_table_names(rt_copy.clone(), root_copy.join("db/.tables_meta")).await,
            recreate: load_btree_table_state(root_copy.join("db/.tables")
                                                      .join(QUICK_REPAIR_RECREATE_TABLE)
                                                      .join("table.dat")),
        };

        let _ = sender.send((snapshot, disk_state));
    });

    receiver.recv_timeout(Duration::from_secs(60)).unwrap()
}

async fn load_latest_log_entries(rt: pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
                                 path: PathBuf) -> HashMap<Vec<u8>, Option<Vec<u8>>> {
    if !path.exists() {
        return HashMap::new();
    }

    let log = LogFile::open(rt,
                            path,
                            2 * 1024 * 1024,
                            512 * 1024 * 1024,
                            None).await.unwrap();
    let mut loader = LatestLogTableLoader::new();
    log.load(&mut loader, None, 2 * 1024 * 1024, true).await.unwrap();
    loader.entries
}

async fn load_usize_log_table_state(rt: pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
                                    path: PathBuf) -> BTreeMap<usize, usize> {
    let entries = load_latest_log_entries(rt, path).await;
    let mut result = BTreeMap::new();

    for (key, value) in entries {
        if let Some(value) = value {
            result.insert(binary_to_usize(&Binary::new(key)).unwrap(),
                          binary_to_usize(&Binary::new(value)).unwrap());
        }
    }

    result
}

async fn load_meta_table_names(rt: pi_async_rt::rt::multi_thread::MultiTaskRuntime<()>,
                               path: PathBuf) -> Vec<String> {
    let entries = load_latest_log_entries(rt, path).await;
    let mut result = Vec::new();

    for (key, value) in entries {
        if value.is_some() {
            result.push(binary_to_atom(&Binary::new(key)).unwrap().as_str().to_string());
        }
    }
    result.sort();

    result
}

fn load_btree_table_state(path: PathBuf) -> BTreeMap<usize, usize> {
    if !path.exists() {
        return BTreeMap::new();
    }

    let snapshot_path = path.with_extension("snapshot");
    if snapshot_path.exists() {
        let _ = std::fs::remove_file(&snapshot_path);
    }
    std::fs::copy(&path, &snapshot_path).unwrap();

    let db = Builder::new().open(&snapshot_path).unwrap();
    let transaction = db.begin_read().unwrap();
    let table = transaction.open_table(QUICK_REPAIR_BTREE_DEF).unwrap();
    let mut result = BTreeMap::new();
    let iter = table.iter().unwrap();

    for item in iter {
        let (key, value) = item.unwrap();
        result.insert(binary_to_usize(&key.value()).unwrap(),
                      binary_to_usize(&value.value()).unwrap());
    }

    drop(table);
    drop(transaction);
    drop(db);
    let _ = std::fs::remove_file(snapshot_path);

    result
}

fn expected_quick_repair_snapshot() -> QuickRepairSnapshot {
    let mut log_ord = BTreeMap::new();
    log_ord.insert(1, 1001);
    log_ord.insert(3, 1003);

    let mut log_write = BTreeMap::new();
    log_write.insert(5, None);
    log_write.insert(6, None);
    log_write.insert(7, None);

    let mut btree = BTreeMap::new();
    btree.insert(10, 2001);

    QuickRepairSnapshot {
        tables: vec![
            ".tables_meta".to_string(),
            QUICK_REPAIR_BTREE_TABLE.to_string(),
            QUICK_REPAIR_LOG_ORD_TABLE.to_string(),
            QUICK_REPAIR_LOG_WRITE_TABLE.to_string(),
            QUICK_REPAIR_MEM_TABLE.to_string(),
        ],
        log_ord,
        log_write,
        btree,
        mem_ord: BTreeMap::new(),
    }
}

fn expected_quick_repair_disk_state() -> QuickRepairDiskState {
    let mut log_ord = BTreeMap::new();
    log_ord.insert(1, 1001);
    log_ord.insert(3, 1003);

    let mut log_write = BTreeMap::new();
    log_write.insert(5, 3001);
    log_write.insert(6, 3002);

    let mut btree = BTreeMap::new();
    btree.insert(10, 2001);

    QuickRepairDiskState {
        meta_tables: vec![
            QUICK_REPAIR_BTREE_TABLE.to_string(),
            QUICK_REPAIR_LOG_ORD_TABLE.to_string(),
            QUICK_REPAIR_LOG_WRITE_TABLE.to_string(),
            QUICK_REPAIR_MEM_TABLE.to_string(),
        ],
        log_ord,
        log_write,
        btree,
    }
}

fn expected_quick_repair_meta_snapshot() -> QuickRepairSnapshot {
    let mut log_ord = BTreeMap::new();
    log_ord.insert(1, 101);
    log_ord.insert(2, 102);

    let mut log_write = BTreeMap::new();
    log_write.insert(5, None);
    log_write.insert(6, None);
    log_write.insert(7, None);

    let mut btree = BTreeMap::new();
    btree.insert(10, 2001);

    QuickRepairSnapshot {
        tables: vec![
            ".tables_meta".to_string(),
            QUICK_REPAIR_BTREE_TABLE.to_string(),
            QUICK_REPAIR_LOG_ORD_TABLE.to_string(),
        ],
        log_ord,
        log_write,
        btree,
        mem_ord: BTreeMap::new(),
    }
}

fn expected_quick_repair_meta_disk_state() -> QuickRepairDiskState {
    let mut log_ord = BTreeMap::new();
    log_ord.insert(1, 101);
    log_ord.insert(2, 102);

    let mut btree = BTreeMap::new();
    btree.insert(10, 2001);

    QuickRepairDiskState {
        meta_tables: vec![
            QUICK_REPAIR_BTREE_TABLE.to_string(),
            QUICK_REPAIR_LOG_ORD_TABLE.to_string(),
        ],
        log_ord,
        log_write: BTreeMap::new(),
        btree,
    }
}

fn expected_quick_repair_log_write_snapshot() -> QuickRepairSnapshot {
    let mut log_write = BTreeMap::new();
    log_write.insert(5, None);
    log_write.insert(6, None);
    log_write.insert(7, None);

    QuickRepairSnapshot {
        tables: vec![
            ".tables_meta".to_string(),
            QUICK_REPAIR_LOG_WRITE_TABLE.to_string(),
        ],
        log_ord: BTreeMap::new(),
        log_write,
        btree: BTreeMap::new(),
        mem_ord: BTreeMap::new(),
    }
}

fn expected_quick_repair_log_write_disk_state() -> QuickRepairDiskState {
    let mut log_write = BTreeMap::new();
    log_write.insert(5, 3101);
    log_write.insert(6, 3001);
    log_write.insert(7, 3007);

    QuickRepairDiskState {
        meta_tables: vec![QUICK_REPAIR_LOG_WRITE_TABLE.to_string()],
        log_ord: BTreeMap::new(),
        log_write,
        btree: BTreeMap::new(),
    }
}

fn expected_quick_repair_btree_snapshot() -> QuickRepairSnapshot {
    let mut log_write = BTreeMap::new();
    log_write.insert(5, None);
    log_write.insert(6, None);
    log_write.insert(7, None);

    let mut btree = BTreeMap::new();
    btree.insert(10, 2100);
    btree.insert(12, 2102);
    btree.insert(13, 2003);

    QuickRepairSnapshot {
        tables: vec![
            ".tables_meta".to_string(),
            QUICK_REPAIR_BTREE_TABLE.to_string(),
        ],
        log_ord: BTreeMap::new(),
        log_write,
        btree,
        mem_ord: BTreeMap::new(),
    }
}

fn expected_quick_repair_btree_disk_state() -> QuickRepairDiskState {
    let mut btree = BTreeMap::new();
    btree.insert(10, 2100);
    btree.insert(12, 2102);
    btree.insert(13, 2003);

    QuickRepairDiskState {
        meta_tables: vec![QUICK_REPAIR_BTREE_TABLE.to_string()],
        log_ord: BTreeMap::new(),
        log_write: BTreeMap::new(),
        btree,
    }
}

fn expected_quick_repair_multi_transaction_snapshot() -> QuickRepairSnapshot {
    let mut log_ord = BTreeMap::new();
    log_ord.insert(1, 13);
    log_ord.insert(2, 22);
    log_ord.insert(3, 33);

    let mut log_write = BTreeMap::new();
    log_write.insert(5, None);
    log_write.insert(6, None);
    log_write.insert(7, None);

    let mut btree = BTreeMap::new();
    btree.insert(10, 101);
    btree.insert(11, 111);
    btree.insert(12, 120);

    QuickRepairSnapshot {
        tables: vec![
            ".tables_meta".to_string(),
            QUICK_REPAIR_BTREE_TABLE.to_string(),
            QUICK_REPAIR_LOG_ORD_TABLE.to_string(),
            QUICK_REPAIR_LOG_WRITE_TABLE.to_string(),
        ],
        log_ord,
        log_write,
        btree,
        mem_ord: BTreeMap::new(),
    }
}

fn expected_quick_repair_multi_transaction_disk_state() -> QuickRepairDiskState {
    let mut log_ord = BTreeMap::new();
    log_ord.insert(1, 13);
    log_ord.insert(2, 22);
    log_ord.insert(3, 33);

    let mut log_write = BTreeMap::new();
    log_write.insert(5, 52);
    // LogWTab 的删除不会在物理日志中生成“抹掉旧值”的最终状态，
    // 所以磁盘快照里仍然保留最后一次成功落盘的写入记录。
    log_write.insert(6, 60);

    let mut btree = BTreeMap::new();
    btree.insert(10, 101);
    btree.insert(11, 111);
    btree.insert(12, 120);

    QuickRepairDiskState {
        meta_tables: vec![
            QUICK_REPAIR_BTREE_TABLE.to_string(),
            QUICK_REPAIR_LOG_ORD_TABLE.to_string(),
            QUICK_REPAIR_LOG_WRITE_TABLE.to_string(),
        ],
        log_ord,
        log_write,
        btree,
    }
}

fn expected_recreate_table_snapshot() -> RecreateTableSnapshot {
    let mut recreate = BTreeMap::new();
    recreate.insert(8, 801);
    recreate.insert(10, 1000);

    RecreateTableSnapshot {
        tables: vec![
            ".tables_meta".to_string(),
            QUICK_REPAIR_RECREATE_TABLE.to_string(),
        ],
        recreate,
    }
}

fn expected_recreate_table_disk_state() -> RecreateTableDiskState {
    let mut recreate = BTreeMap::new();
    recreate.insert(8, 801);
    recreate.insert(10, 1000);

    RecreateTableDiskState {
        meta_tables: vec![QUICK_REPAIR_RECREATE_TABLE.to_string()],
        recreate,
    }
}

fn remove_dir_if_exists(path: &std::path::Path) {
    if path.exists() {
        std::fs::remove_dir_all(path).unwrap();
    }
}

fn copy_dir_all(src: &std::path::Path,
                dst: &std::path::Path) {
    remove_dir_if_exists(dst);
    std::fs::create_dir_all(dst).unwrap();

    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let file_type = entry.file_type().unwrap();
        let target = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(entry.path().as_path(), target.as_path());
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn binary_to_atom(bin: &Binary) -> Result<Atom, ReadBonErr> {
    let mut buffer = ReadBuffer::new(bin, 0);
    Atom::decode(&mut buffer)
}

// 生成指定长度的重复字符串
fn generate_string_with_repeated_char(len: usize, c: char) -> String {
    std::iter::repeat(c).take(len).collect()
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

// 将String序列化为二进制数据
fn string_to_binary(str: String) -> Binary {
    let mut buffer = WriteBuffer::new();
    str.encode(&mut buffer);
    Binary::new(buffer.bytes)
}

// 将二进制数据反序列化为String
fn binary_to_string(bin: &Binary) -> Result<String, ReadBonErr> {
    let mut buffer = ReadBuffer::new(bin, 0);
    String::decode(&mut buffer)
}



