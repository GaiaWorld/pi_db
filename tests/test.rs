#![feature(stmt_expr_attributes)]

use std::thread;
use std::sync::Arc;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::collections::{HashMap,
                       btree_map::{BTreeMap, Entry}};

use futures::stream::StreamExt;
use crossbeam_channel::{unbounded, bounded};
use parking_lot::Mutex;
use env_logger;
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
use redb::{Builder, TableDefinition, ReadOnlyTable, Table};

use pi_db::{Binary,
            KVDBTableType,
            KVTableMeta,
            init_transaction_debug_logger,
            db::KVDBManagerBuilder,
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
        match builder.startup().await {
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
        match builder.startup().await {
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
        match builder.startup().await {
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
                                                                 EnumType::Usize)).await {
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
        match builder.startup().await {
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
        match builder.startup().await {
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
                                                                 EnumType::Str)).await {
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
        match builder.startup().await {
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
        match builder.startup().await {
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
        match builder.startup().await {
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
        match builder.startup().await {
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
        match builder.startup().await {
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
        match builder.startup().await {
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
                                                                 EnumType::Str)
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
        match builder.startup().await {
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
                                                                 EnumType::Usize)).await {
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
        match builder.startup().await {
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
                                                                 EnumType::Usize)).await {
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
        match builder.startup().await {
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
                                                                 EnumType::Usize)).await {
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
        match builder.startup().await {
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
                                                                 EnumType::Usize)).await {
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
        match builder.startup().await {
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
                                                                 EnumType::Usize)).await {
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
                        rt_copy.timeout(65000).await;
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
                    let tr = db.transaction(Atom::from("test b-tree table"), true, 500, 500).unwrap();

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

/// 测试并发写和迭代对内存的影响
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
        match builder.startup().await {
            Err(e) => {
                panic!("{:?}", e);
            },
            Ok(db) => {
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                let mut table_names = Vec::with_capacity(100);
                let tr = db.transaction(Atom::from("create b-tree table"), true, 500, 500).unwrap();
                for index in 0..100 {
                    let table_name = Atom::from("test_log/".to_string() + &index.to_string());
                    if let Err(e) = tr.create_table(table_name.clone(),
                                                    KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                                                     true,
                                                                     EnumType::Usize,
                                                                     EnumType::Str)).await {
                        //创建有序内存表失败
                        println!("!!!!!!create b-tree ordered table failed, reason: {:?}", e);
                    } else {
                        table_names.push(table_name);
                    }
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;

                println!("!!!!!!db table size: {:?}", db.table_size().await);

                //操作数据库事务
                rt_copy.timeout(1500).await;
                println!("");

                let rt_clone = rt_copy.clone();
                let db_copy = db.clone();
                let _ = rt_copy.spawn(async move {
                    loop {
                        rt_clone.timeout(65000).await;
                        let mut b = false;
                        #[cfg(target_os = "linux")]
                        b = db_copy.cleanup_buffer_after_collect_table();
                        println!("!!!!!!cleanup_buffer_after_collect_table: {:?}", b);
                    }
                });

                let table_names_copy = table_names.clone();
                for table_name_copy in table_names_copy {
                    let rt_clone = rt_copy.clone();
                    let db_copy = db.clone();
                    let _ = rt_copy.spawn(async move {
                        let value = generate_string_with_repeated_char(256, 'a');

                        let now = Instant::now();
                        let mut trs = Vec::with_capacity(10000);
                        for index in 0..10000 {
                            if (index + 1) % 1000 == 0 {
                                println!("!!!!!!insert, table: {:?}, table_size: {:?}, count: {:?}, time: {:?}", table_name_copy, db_copy.table_cache_size(&table_name_copy).await, index + 1, now.elapsed());
                            }

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
                                    } else {
                                        trs.push(tr);
                                    }
                                },
                            }
                        }

                        println!("!!!!!!table0: {:?}, table_size: {:?}", table_name_copy, db_copy.table_cache_size(&table_name_copy).await);
                        println!("transatction count: {:?}, wait release...", trs.len());
                        rt_clone.timeout(65000).await;
                        println!("!!!!!!table1: {:?}, table_size: {:?}", table_name_copy, db_copy.table_cache_size(&table_name_copy).await);
                    });
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
        match builder.startup().await {
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
        match builder.startup().await {
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
        match builder.startup().await {
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
        match builder.startup().await {
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
        match builder.startup().await {
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
        match builder.startup().await {
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
        match builder.startup().await {
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
        match builder.startup().await {
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
        match builder.startup().await {
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



