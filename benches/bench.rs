#![feature(test)]

extern crate test;

use test::Bencher;

use std::thread;
use std::time::Instant;

use futures::stream::StreamExt;
use crossbeam_channel::{unbounded, bounded};
use env_logger;

use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_time::run_nanos;
use pi_ordmap::{ordmap::{Iter, ImOrdMap, OrdMap, Entry}, asbtree::Tree};
use pi_atom::Atom;
use pi_async_rt::rt::{AsyncRuntime,
                      multi_thread::MultiTaskRuntimeBuilder};
use pi_async_transaction::{TransactionError,
                           ErrorLevel,
                           AsyncCommitLog,
                           manager_2pc::Transaction2PcManager};
use pi_guid::Guid;
use pi_store::commit_logger::{CommitLogger, CommitLoggerBuilder};
use async_stream::stream;
use pi_async_rt::prelude::startup_global_time_loop;
use pi_bon::{WriteBuffer, ReadBuffer, Encode, Decode, ReadBonErr};
use pi_db::{Binary, KVDBTableType, KVTableMeta,
            db::{KVDBManagerBuilder, KVDBManager},
            tables::TableKV,
            utils::KVDBEvent,
            KVTableTrError};

#[bench]
fn bench_insert_ordmap_by_small(b: &mut Bencher) {
    b.iter(move || {
        let mut tree = OrdMap::<Tree<usize, usize>>::new(Tree::new());

        for index in 0..100000usize {
            let _ = tree.upsert(index,
                                index,
                                false);
        }
    });
}

#[bench]
fn bench_insert_ordmap(b: &mut Bencher) {
    b.iter(move || {
        let mut tree = OrdMap::<Tree<Vec<u8>, Vec<u8>>>::new(Tree::new());

        for key in 0..100000usize {
            let _ = tree.upsert(key.to_le_bytes().to_vec(),
                                "Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!".as_bytes().to_vec(),
                                false);
        }
    });
}

#[bench]
fn bench_update_ordmap(b: &mut Bencher) {
    let mut tree = OrdMap::<Tree<Vec<u8>, Vec<u8>>>::new(Tree::new());

    for key in 0..100000usize {
        let _ = tree.upsert(key.to_le_bytes().to_vec(),
                            "Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!".as_bytes().to_vec(),
                            false);
    }

    b.iter(move || {
        for key in 0..100000usize {
            let _ = tree.upsert(key.to_le_bytes().to_vec(),
                                "Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!".as_bytes().to_vec(),
                                false);
        }
    });
}

#[bench]
fn bench_read_ordmap(b: &mut Bencher) {
    let mut tree = OrdMap::<Tree<Vec<u8>, Vec<u8>>>::new(Tree::new());

    for key in 0..100000usize {
        let _ = tree.upsert(key.to_le_bytes().to_vec(),
                            "Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!".as_bytes().to_vec(),
                            false);
    }

    b.iter(move || {
        for key in 0..100000usize {
            if let None = tree.get(&key.to_le_bytes().to_vec()) {
                panic!("read ordmap failed");
            }
        }
    });
}

#[bench]
fn bench_iter_ordmap(b: &mut Bencher) {
    let mut tree = OrdMap::<Tree<Vec<u8>, Vec<u8>>>::new(Tree::new());

    for key in 0..100000usize {
        let _ = tree.upsert(key.to_le_bytes().to_vec(),
                            "Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!".as_bytes().to_vec(),
                            false);
    }

    b.iter(move || {
        let mut iter = tree.iter(None, false);

        while let Some(_) = iter.next() {}
    });
}

#[bench]
fn bench_iter_ordmap_by_desc(b: &mut Bencher) {
    let mut tree = OrdMap::<Tree<Vec<u8>, Vec<u8>>>::new(Tree::new());

    for key in 0..100000usize {
        let _ = tree.upsert(key.to_le_bytes().to_vec(),
                            "Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!".as_bytes().to_vec(),
                            false);
    }

    b.iter(move || {
        let mut iter = tree.iter(None, true);

        while let Some(_) = iter.next() {}
    });
}

#[bench]
fn bench_async_iter_ordmap(b: &mut Bencher) {
    let rt = MultiTaskRuntimeBuilder::default().build();

    let mut tree = OrdMap::<Tree<Vec<u8>, Vec<u8>>>::new(Tree::new());

    for key in 0..100000usize {
        let _ = tree.upsert(key.to_le_bytes().to_vec(),
                            "Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!".as_bytes().to_vec(),
                            false);
    }

    b.iter(move || {
        let ptr = Box::into_raw(Box::new(tree.iter(None, false))) as usize;
        let mut stream = stream! {
            let mut iterator = unsafe {
                Box::from_raw(ptr as *mut <Tree<Vec<u8>, Vec<u8>> as Iter<'_>>::IterType)
            };

            while let Some(Entry(key, value)) = iterator.next() {
                yield (key.clone(), value.clone());
            }
        }.boxed();

        let (sender, receiver) = bounded(1);
        rt.spawn(async move {
            while let Some(_) = stream.next().await {}
            sender.send(());
        });
        let _ = receiver.recv().unwrap();
    });
}

#[bench]
fn bench_memory_table(b: &mut Bencher) {
    use std::thread;
    use std::time::{Duration, Instant};

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
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

        let builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./db");
        match builder.startup().await {
            Err(e) => {
                println!("!!!!!!startup db failed, reason: {:?}", e);
            },
            Ok(db) => {
                let tr = db.transaction(Atom::from("test_memory"), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(Atom::from("test_memory"),
                                                KVTableMeta::new(KVDBTableType::MemOrdTab,
                                                                 false,
                                                                 EnumType::Usize,
                                                                 EnumType::Str)).await {
                    //创建有序内存表失败
                    println!("!!!!!!create memory ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                sender.send(db);
            },
        }
    });
    thread::sleep(Duration::from_millis(3000));

    let rt_copy = rt.clone();
    let db = receiver.recv().unwrap();
    b.iter(move || {
        let (s, r) = unbounded();
        let table_name = Atom::from("test_memory");

        let now = Instant::now();
        for index in 0..10000usize {
            let s_copy = s.clone();
            let db_copy = db.clone();
            let table_name_copy = table_name.clone();

            let _ = rt_copy.spawn(async move {
                let tr = db_copy
                    .transaction(Atom::from("test memory table"),
                                 true,
                                 500,
                                 500)
                    .unwrap();

                let _ = tr.upsert(vec![TableKV {
                    table: table_name_copy,
                    key: Binary::new(index.to_le_bytes().to_vec()),
                    value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                }]).await;

                loop {
                    match tr.prepare_modified().await {
                        Err(e) => {
                            println!("prepare failed, reason: {:?}", e);
                            if let Err(e) = tr.rollback_modified().await {
                                println!("rollback failed, reason: {:?}", e);
                                break;
                            } else {
                                println!("rollback ok for prepare");
                                continue;
                            }
                        },
                        Ok(output) => {
                            match tr.commit_modified(output).await {
                                Err(e) => {
                                    println!("commit failed, reason: {:?}", e);
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: commit fatal error");
                                    } else {
                                        println!("rollbakc ok for commit");
                                    }
                                    break;
                                },
                                Ok(()) => {
                                    s_copy.send(());
                                    break;
                                },
                            }
                        },
                    }
                }
            });
        }

        let mut count = 0;
        loop {
            match r.recv_timeout(Duration::from_millis(10000)) {
                Err(e) => {
                    println!(
                        "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                        rt_copy.wait_len(),
                        rt_copy.len(),
                        e
                    );
                    continue;
                },
                Ok(_) => {
                    count += 1;
                    if count >= 10000 {
                        break;
                    }
                },
            }
        }
        println!("time: {:?}", Instant::now() - now);
    });
}

#[bench]
fn bench_multi_memory_table(b: &mut Bencher) {
    use std::thread;
    use std::time::{Duration, Instant};

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
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

        let builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./db");
        match builder.startup().await {
            Err(e) => {
                println!("!!!!!!startup db failed, reason: {:?}", e);
            },
            Ok(db) => {
                //创建指定数量的有序内存表
                for index in 0..100 {
                    let tr = db.transaction(Atom::from("test_memory"), true, 500, 500).unwrap();
                    if let Err(e) = tr.create_table(Atom::from("test_memory".to_string() + index.to_string().as_str()),
                                                    KVTableMeta::new(KVDBTableType::MemOrdTab,
                                                                     false,
                                                                     EnumType::Usize,
                                                                     EnumType::Str)).await {
                        //创建有序内存表失败
                        println!("!!!!!!create memory ordered table failed, reason: {:?}", e);
                    }
                    let output = tr.prepare_modified().await.unwrap();
                    let _ = tr.commit_modified(output).await;
                }
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                sender.send(db);
            },
        }
    });
    thread::sleep(Duration::from_millis(3000));

    let rt_copy = rt.clone();
    let db = receiver.recv().unwrap();
    let mut table_names = Vec::new();
    for index in 0..100 {
        table_names.push(Atom::from("test_memory".to_string() + index.to_string().as_str()));
    }
    b.iter(move || {
        let (s, r) = unbounded();

        let now = Instant::now();
        for index in 0..1000usize {
            let s_copy = s.clone();
            let db_copy = db.clone();
            let table_names_copy = table_names.clone();

            let _ = rt_copy.spawn(async move {
                let tr = db_copy
                    .transaction(Atom::from("test memory table"),
                                 true,
                                 500,
                                 500)
                    .unwrap();

                //操作指定数量的表
                for table_name in table_names_copy {
                    let _ = tr.upsert(vec![TableKV {
                        table: table_name.clone(),
                        key: Binary::new(index.to_le_bytes().to_vec()),
                        value: Some(Binary::new("Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!".as_bytes().to_vec()))
                    }]).await;
                }

                loop {
                    match tr.prepare_modified().await {
                        Err(e) => {
                            println!("prepare failed, reason: {:?}", e);
                            if let Err(e) = tr.rollback_modified().await {
                                println!("rollback failed, reason: {:?}", e);
                                break;
                            } else {
                                println!("rollback ok for prepare");
                                continue;
                            }
                        },
                        Ok(output) => {
                            match tr.commit_modified(output).await {
                                Err(e) => {
                                    println!("commit failed, reason: {:?}", e);
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: commit fatal error");
                                    } else {
                                        println!("rollbakc ok for commit");
                                    }
                                    break;
                                },
                                Ok(()) => {
                                    s_copy.send(());
                                    break;
                                },
                            }
                        },
                    }
                }
            });
        }

        let mut count = 0;
        loop {
            match r.recv_timeout(Duration::from_millis(10000)) {
                Err(e) => {
                    println!(
                        "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                        rt_copy.wait_len(),
                        rt_copy.len(),
                        e
                    );
                    continue;
                },
                Ok(_) => {
                    count += 1;
                    if count >= 1000 {
                        break;
                    }
                },
            }
        }
        println!("time: {:?}", Instant::now() - now);
    });
}

#[bench]
fn bench_commit_log(b: &mut Bencher) {
    use std::thread;
    use std::time::{Duration, Instant};

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
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

        let builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./db");
        match builder.startup().await {
            Err(e) => {
                println!("!!!!!!startup db failed, reason: {:?}", e);
            },
            Ok(db) => {
                let tr = db.transaction(Atom::from("test_memory"), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(Atom::from("test_memory"),
                                                KVTableMeta::new(KVDBTableType::MemOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Str)).await {
                    //创建有序内存表失败
                    println!("!!!!!!create memory ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                sender.send(db);
            },
        }
    });
    thread::sleep(Duration::from_millis(3000));

    let rt_copy = rt.clone();
    let db = receiver.recv().unwrap();
    b.iter(move || {
        let (s, r) = unbounded();
        let table_name = Atom::from("test_memory");

        let now = Instant::now();
        for index in 0..10000usize {
            let s_copy = s.clone();
            let db_copy = db.clone();
            let table_name_copy = table_name.clone();

            let _ = rt_copy.spawn(async move {
                let tr = db_copy
                    .transaction(Atom::from("test memory table"),
                                 true,
                                 500,
                                 500)
                    .unwrap();

                let _ = tr.upsert(vec![TableKV {
                    table: table_name_copy,
                    key: Binary::new(index.to_le_bytes().to_vec()),
                    value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                }]).await;

                loop {
                    match tr.prepare_modified().await {
                        Err(e) => {
                            println!("prepare failed, reason: {:?}", e);
                            if let Err(e) = tr.rollback_modified().await {
                                println!("rollback failed, reason: {:?}", e);
                                break;
                            } else {
                                println!("rollback ok for prepare");
                                continue;
                            }
                        },
                        Ok(output) => {
                            match tr.commit_modified(output).await {
                                Err(e) => {
                                    println!("commit failed, reason: {:?}", e);
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: commit fatal error");
                                    } else {
                                        println!("rollbakc ok for commit");
                                    }
                                    break;
                                },
                                Ok(()) => {
                                    s_copy.send(());
                                    break;
                                },
                            }
                        },
                    }
                }
            });
        }

        let mut count = 0;
        loop {
            match r.recv_timeout(Duration::from_millis(10000)) {
                Err(e) => {
                    println!(
                        "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                        rt_copy.wait_len(),
                        rt_copy.len(),
                        e
                    );
                    continue;
                },
                Ok(_) => {
                    count += 1;
                    if count >= 10000 {
                        break;
                    }
                },
            }
        }
        println!("time: {:?}", Instant::now() - now);
    });
}

#[bench]
fn bench_multi_commit_log(b: &mut Bencher) {
    use std::thread;
    use std::time::{Duration, Instant};

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
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

        let builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./db");
        match builder.startup().await {
            Err(e) => {
                println!("!!!!!!startup db failed, reason: {:?}", e);
            },
            Ok(db) => {
                //创建指定数量的有序内存表
                for index in 0..10 {
                    let tr = db.transaction(Atom::from("test_memory"), true, 500, 500).unwrap();
                    if let Err(e) = tr.create_table(Atom::from("test_memory".to_string() + index.to_string().as_str()),
                                                    KVTableMeta::new(KVDBTableType::MemOrdTab,
                                                                     true,
                                                                     EnumType::Usize,
                                                                     EnumType::Str)).await {
                        //创建有序内存表失败
                        println!("!!!!!!create memory ordered table failed, reason: {:?}", e);
                    }
                    let output = tr.prepare_modified().await.unwrap();
                    let _ = tr.commit_modified(output).await;
                }
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                sender.send(db);
            },
        }
    });
    thread::sleep(Duration::from_millis(3000));

    let rt_copy = rt.clone();
    let db = receiver.recv().unwrap();
    let mut table_names = Vec::new();
    for index in 0..10 {
        table_names.push(Atom::from("test_memory".to_string() + index.to_string().as_str()));
    }
    b.iter(move || {
        let (s, r) = unbounded();

        let now = Instant::now();
        for index in 0..1000usize {
            let s_copy = s.clone();
            let db_copy = db.clone();
            let table_names_copy = table_names.clone();

            let _ = rt_copy.spawn(async move {
                let tr = db_copy
                    .transaction(Atom::from("test memory table"),
                                 true,
                                 500,
                                 500)
                    .unwrap();

                //操作指定数量的表
                for table_name in table_names_copy {
                    let _ = tr.upsert(vec![TableKV {
                        table: table_name.clone(),
                        key: Binary::new(index.to_le_bytes().to_vec()),
                        value: Some(Binary::new("Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!".as_bytes().to_vec()))
                    }]).await;
                }

                loop {
                    match tr.prepare_modified().await {
                        Err(e) => {
                            println!("prepare failed, reason: {:?}", e);
                            if let Err(e) = tr.rollback_modified().await {
                                println!("rollback failed, reason: {:?}", e);
                                break;
                            } else {
                                println!("rollback ok for prepare");
                                continue;
                            }
                        },
                        Ok(output) => {
                            match tr.commit_modified(output).await {
                                Err(e) => {
                                    println!("commit failed, reason: {:?}", e);
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: commit fatal error");
                                    } else {
                                        println!("rollbakc ok for commit");
                                    }
                                    break;
                                },
                                Ok(()) => {
                                    s_copy.send(());
                                    break;
                                },
                            }
                        },
                    }
                }
            });
        }

        let mut count = 0;
        loop {
            match r.recv_timeout(Duration::from_millis(10000)) {
                Err(e) => {
                    println!(
                        "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                        rt_copy.wait_len(),
                        rt_copy.len(),
                        e
                    );
                    continue;
                },
                Ok(_) => {
                    count += 1;
                    if count >= 1000 {
                        break;
                    }
                },
            }
        }
        println!("time: {:?}", Instant::now() - now);
    });
}

#[bench]
fn bench_log_table(b: &mut Bencher) {
    use std::thread;
    use std::time::{Duration, Instant};

    env_logger::init();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
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

        let builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./db");
        match builder.startup().await {
            Err(e) => {
                println!("!!!!!!startup db failed, reason: {:?}", e);
            },
            Ok(db) => {
                let tr = db.transaction(Atom::from("test_log"), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(Atom::from("test_log"),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Str)).await {
                    //创建有序日志表失败
                    println!("!!!!!!create log ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                sender.send(db);
            },
        }
    });
    thread::sleep(Duration::from_millis(3000));

    let rt_copy = rt.clone();
    let db = receiver.recv().unwrap();
    b.iter(move || {
        let (s, r) = unbounded();
        let table_name = Atom::from("test_log");

        let now = Instant::now();
        for index in 0..10000usize {
            let s_copy = s.clone();
            let db_copy = db.clone();
            let table_name_copy = table_name.clone();

            let _ = rt_copy.spawn(async move {
                let tr = db_copy
                    .transaction(Atom::from("test log table"),
                                 true,
                                 500,
                                 500)
                    .unwrap();

                let _ = tr.upsert(vec![TableKV {
                    table: table_name_copy,
                    key: Binary::new(index.to_le_bytes().to_vec()),
                    value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                }]).await;

                loop {
                    match tr.prepare_modified().await {
                        Err(e) => {
                            println!("prepare failed, reason: {:?}", e);
                            if let Err(e) = tr.rollback_modified().await {
                                println!("rollback failed, reason: {:?}", e);
                                break;
                            } else {
                                println!("rollback ok for prepare");
                                continue;
                            }
                        },
                        Ok(output) => {
                            match tr.commit_modified(output).await {
                                Err(e) => {
                                    println!("commit failed, reason: {:?}", e);
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: commit fatal error");
                                    } else {
                                        println!("rollbakc ok for commit");
                                    }
                                    break;
                                },
                                Ok(()) => {
                                    s_copy.send(());
                                    break;
                                },
                            }
                        },
                    }
                }
            });
        }

        let mut count = 0;
        loop {
            match r.recv_timeout(Duration::from_millis(10000)) {
                Err(e) => {
                    println!(
                        "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                        rt_copy.wait_len(),
                        rt_copy.len(),
                        e
                    );
                    continue;
                },
                Ok(_) => {
                    count += 1;
                    if count >= 10000 {
                        break;
                    }
                },
            }
        }
        println!("time: {:?}", Instant::now() - now);
    });

    thread::sleep(Duration::from_millis(30000));
}

#[bench]
fn bench_multi_log_table(b: &mut Bencher) {
    use std::thread;
    use std::time::{Duration, Instant};

    env_logger::init();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
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
                                                                     EnumType::Str)).await {
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
    thread::sleep(Duration::from_millis(3000));

    let rt_copy = rt.clone();
    let db = receiver.recv().unwrap();
    let mut table_names = Vec::new();
    for index in 0..10 {
        table_names.push(Atom::from("test_log".to_string() + index.to_string().as_str()));
    }
    b.iter(move || {
        let (s, r) = unbounded();

        let now = Instant::now();
        for index in 0..1000usize {
            let s_copy = s.clone();
            let db_copy = db.clone();
            let table_names_copy = table_names.clone();

            let _ = rt_copy.spawn(async move {
                let tr = db_copy
                    .transaction(Atom::from("test log table"),
                                 true,
                                 500,
                                 500).unwrap();

                for table_name in table_names_copy {
                    let _ = tr.upsert(vec![TableKV {
                        table: table_name,
                        key: Binary::new(index.to_le_bytes().to_vec()),
                        value: Some(Binary::new("Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!".as_bytes().to_vec()))
                    }]).await;
                }

                loop {
                    match tr.prepare_modified().await {
                        Err(e) => {
                            println!("prepare failed, reason: {:?}", e);
                            if let Err(e) = tr.rollback_modified().await {
                                println!("rollback failed, reason: {:?}", e);
                                break;
                            } else {
                                println!("rollback ok for prepare");
                                continue;
                            }
                        },
                        Ok(output) => {
                            match tr.commit_modified(output).await {
                                Err(e) => {
                                    println!("commit failed, reason: {:?}", e);
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: commit fatal error");
                                    } else {
                                        println!("rollbakc ok for commit");
                                    }
                                    break;
                                },
                                Ok(()) => {
                                    s_copy.send(());
                                    break;
                                },
                            }
                        },
                    }
                }
            });
        }

        let mut count = 0;
        loop {
            match r.recv_timeout(Duration::from_millis(10000)) {
                Err(e) => {
                    println!(
                        "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                        rt_copy.wait_len(),
                        rt_copy.len(),
                        e
                    );
                    continue;
                },
                Ok(_) => {
                    count += 1;
                    if count >= 1000 {
                        break;
                    }
                },
            }
        }
        println!("time: {:?}", Instant::now() - now);
    });

    thread::sleep(Duration::from_millis(80000));
}

#[bench]
fn bench_iterator_table(b: &mut Bencher) {
    use std::thread;
    use std::time::Duration;

    env_logger::init();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
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
                                                                     EnumType::Str)).await {
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
    thread::sleep(Duration::from_millis(3000));

    let rt_copy = rt.clone();
    let db = receiver.recv().unwrap();
    let mut table_names = Vec::new();
    for index in 0..10 {
        table_names.push(Atom::from("test_log".to_string() + index.to_string().as_str()));
    }
    b.iter(move || {
        let (s, r) = unbounded();

        for _ in 0..1000usize {
            let s_copy = s.clone();
            let db_copy = db.clone();
            let table_names_copy = table_names.clone();

            let _ = rt_copy.spawn(async move {
                let tr = db_copy
                    .transaction(Atom::from("test log table"),
                                 true,
                                 500,
                                 500).unwrap();

                for table_name in table_names_copy {
                    let mut iterator = tr.values(table_name, None, true).await.unwrap();
                    while let Some(_) = iterator.next().await {}
                }

                s_copy.send(());
            });
        }

        let mut count = 0;
        loop {
            match r.recv_timeout(Duration::from_millis(10000)) {
                Err(e) => {
                    println!(
                        "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                        rt_copy.wait_len(),
                        rt_copy.len(),
                        e
                    );
                    continue;
                },
                Ok(_) => {
                    count += 1;
                    if count >= 1000 {
                        break;
                    }
                },
            }
        }
    });

    thread::sleep(Duration::from_millis(70000));
}

#[bench]
fn bench_sequence_upsert(b: &mut Bencher) {
    use std::time::{Duration, Instant};
    use fastrand;

    env_logger::init();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
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

                let table_name = Atom::from("test_log");
                let tr = db.transaction(Atom::from("test seq upsert"), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
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

                sender.send(db);
            },
        }
    });

    let rt_copy = rt.clone();
    let db = receiver.recv().unwrap();

    b.iter(move || {
        let (s, r) = unbounded();
        let db_copy = db.clone();
        let table_name = Atom::from("test_log");
        let s_copy = s.clone();

        let now = Instant::now();
        let _ = rt_copy.spawn(async move {
            for _ in 0..1000 {
                let table_name0_copy = table_name.clone();
                let mut table_kv_list = Vec::new();

                table_kv_list.push(TableKV {
                    table: table_name0_copy.clone(),
                    key: Binary::new(255usize.to_le_bytes().to_vec()),
                    value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                });

                let tr = db_copy
                    .transaction(Atom::from("test table conflict"),
                                 true,
                                 500,
                                 500).unwrap();
                if let Err(e) = tr.upsert(table_kv_list).await {
                    println!("!!!!!!upsert failed, reason: {:?}", e);
                    return;
                }

                match tr.prepare_modified().await {
                    Err(e) => {
                        if let Err(err) = tr.rollback_modified().await {
                            println!("rollback failed, error: {:?}, reason: {:?}", e, err);
                            s_copy.send(Err(e));
                        }
                    },
                    Ok(output) => {
                        match tr.commit_modified(output).await {
                            Err(e) => {
                                if let ErrorLevel::Fatal = &e.level() {
                                    println!("rollback failed, reason: {:?}", e);
                                    s_copy.send(Err(e));
                                }
                            },
                            Ok(()) => {
                                s_copy.send(Ok(()));
                            },
                        }
                    },
                }
            }
        });

        let mut error_count = 0;
        let mut count = 0;
        loop {
            match r.recv_timeout(Duration::from_millis(10000)) {
                Err(e) => {
                    println!(
                        "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                        rt_copy.wait_len(),
                        rt_copy.len(),
                        e
                    );
                    continue;
                },
                Ok(result) => {
                    if result.is_err() {
                        error_count += 1;
                    }

                    count += 1;
                    if count >= 1000 {
                        break;
                    }
                },
            }
        }
        println!("!!!!!!error: {}, time: {:?}", error_count, Instant::now() - now);
    });
}

#[bench]
fn bench_table_conflict(b: &mut Bencher) {
    use std::time::{Duration, Instant};
    use fastrand;

    env_logger::init();

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
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
                let tr = db.transaction(Atom::from("test table conflict"), true, 500, 500).unwrap();
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

                sender.send(db);
            },
        }
    });

    let rt_copy = rt.clone();
    let db = receiver.recv().unwrap();

    b.iter(move || {
        let (s, r) = unbounded();

        let now = Instant::now();
        for key in 0..32usize {
            let rt_copy_ = rt_copy.clone();
            let db_copy = db.clone();
            let table_name0 = Atom::from("test_log0");
            let table_name1 = Atom::from("test_log1");
            let s_copy = s.clone();

            let _ = rt_copy.spawn(async move {
                let table_name0_copy = table_name0.clone();
                let table_name1_copy = table_name1.clone();

                let start = Instant::now();
                while start.elapsed() <= Duration::from_millis(5000) {
                    let mut table_kv_list = Vec::new();

                    table_kv_list.push(TableKV {
                        table: table_name0_copy.clone(),
                        key: Binary::new(255usize.to_le_bytes().to_vec()),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    });
                    table_kv_list.push(TableKV {
                        table: table_name1_copy.clone(),
                        key: Binary::new(key.to_le_bytes().to_vec()),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    });

                    let tr = db_copy
                        .transaction(Atom::from("test table conflict"),
                                     true,
                                     500,
                                     500).unwrap();
                    if let Err(e) = tr.upsert(table_kv_list).await {
                        println!("!!!!!!upsert failed, reason: {:?}", e);
                        return;
                    }

                    match tr.prepare_modified().await {
                        Err(e) => {
                            if let Err(err) = tr.rollback_modified().await {
                                println!("rollback failed, error: {:?}, reason: {:?}", e, err);
                                s_copy.send(Err(e));
                                return;
                            } else {
                                rt_copy_.timeout(0).await;
                                continue;
                            }
                        },
                        Ok(output) => {
                            match tr.commit_modified(output).await {
                                Err(e) => {
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: {:?}", e);
                                        s_copy.send(Err(e));
                                        return;
                                    } else {
                                        rt_copy_.timeout(0).await;
                                        continue;
                                    }
                                },
                                Ok(()) => {
                                    s_copy.send(Ok(()));
                                    return;
                                },
                            }
                        },
                    }
                }

                s_copy.send(Err(KVTableTrError::new_transaction_error(ErrorLevel::Normal, "reprepare timeout")));
            });
        }

        let mut error_count = 0;
        let mut count = 0;
        loop {
            match r.recv_timeout(Duration::from_millis(10000)) {
                Err(e) => {
                    println!(
                        "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                        rt_copy.wait_len(),
                        rt_copy.len(),
                        e
                    );
                    continue;
                },
                Ok(result) => {
                    if result.is_err() {
                        error_count += 1;
                    }

                    count += 1;
                    if count >= 32 {
                        break;
                    }
                },
            }
        }
        println!("!!!!!!error: {}, time: {:?}", error_count, Instant::now() - now);
    });
}

#[bench]
fn bench_log_table_commit_log_error(b: &mut Bencher) {
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
                let tr = db.transaction(Atom::from("test_log"), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(Atom::from("test_log"),
                                                KVTableMeta::new(KVDBTableType::LogOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Str)).await {
                    //创建有序日志表失败
                    println!("!!!!!!create log ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                sender.send(db);
            },
        }
    });
    thread::sleep(Duration::from_millis(3000));

    let rt_copy = rt.clone();
    let db = receiver.recv().unwrap();
    b.iter(move || {
        let (s, r) = unbounded();
        let table_name = Atom::from("test_log");

        let now = Instant::now();
        for index in 0..10000usize {
            let s_copy = s.clone();
            let db_copy = db.clone();
            let table_name_copy = table_name.clone();

            if index % 10 == 0 {
                let _ = rt_copy.spawn(async move {
                    let tr = db_copy
                        .transaction(Atom::from("test log table0"),
                                     false,
                                     500,
                                     500)
                        .unwrap();

                    let table_name = Atom::from(("test_log/".to_string() + index.to_string().as_str()).as_str());
                    let _ = tr.create_table(table_name.clone(),
                                            KVTableMeta::new(KVDBTableType::LogOrdTab, true, EnumType::Str, EnumType::Str))
                        .await;

                    loop {
                        match tr.prepare_modified().await {
                            Err(e) => {
                                println!("prepare failed, reason: {:?}", e);
                                if let Err(e) = tr.rollback_modified().await {
                                    println!("rollback failed, reason: {:?}", e);
                                    break;
                                } else {
                                    println!("rollback ok for prepare");
                                    continue;
                                }
                            },
                            Ok(output) => {
                                match tr.commit_modified(output).await {
                                    Err(e) => {
                                        println!("commit failed, reason: {:?}", e);
                                        if let ErrorLevel::Fatal = &e.level() {
                                            println!("rollback failed, reason: commit fatal error");
                                        } else {
                                            println!("rollbakc ok for commit");
                                        }
                                        break;
                                    },
                                    Ok(()) => {
                                        s_copy.send(());
                                        break;
                                    },
                                }
                            },
                        }
                    }
                });
            } else {
                let _ = rt_copy.spawn(async move {
                    let tr = db_copy
                        .transaction(Atom::from("test log table1"),
                                     true,
                                     500,
                                     500)
                        .unwrap();

                    let _ = tr.upsert(vec![TableKV {
                        table: table_name_copy,
                        key: usize_to_binary(index),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    }]).await;

                    loop {
                        match tr.prepare_modified().await {
                            Err(e) => {
                                println!("prepare failed, reason: {:?}", e);
                                if let Err(e) = tr.rollback_modified().await {
                                    println!("rollback failed, reason: {:?}", e);
                                    break;
                                } else {
                                    println!("rollback ok for prepare");
                                    continue;
                                }
                            },
                            Ok(output) => {
                                match tr.commit_modified(output).await {
                                    Err(e) => {
                                        println!("commit failed, reason: {:?}", e);
                                        if let ErrorLevel::Fatal = &e.level() {
                                            println!("rollback failed, reason: commit fatal error");
                                        } else {
                                            println!("rollbakc ok for commit");
                                        }
                                        break;
                                    },
                                    Ok(()) => {
                                        s_copy.send(());
                                        break;
                                    },
                                }
                            },
                        }
                    }
                });
            }
        }

        let mut count = 0;
        loop {
            match r.recv_timeout(Duration::from_millis(10000)) {
                Err(e) => {
                    println!(
                        "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                        rt_copy.wait_len(),
                        rt_copy.len(),
                        e
                    );
                    continue;
                },
                Ok(_) => {
                    count += 1;
                    if count >= 10000 {
                        break;
                    }
                },
            }
        }
        println!("time: {:?}", Instant::now() - now);
    });

    thread::sleep(Duration::from_millis(180000));
}

// 测试写性能，还会测试写数据，持久化提交数据，读已缓存数据和读未缓存数据的功能
#[bench]
fn bench_b_tree_table(b: &mut Bencher) {
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
            .build()
            .await
            .unwrap();

        let tr_mgr = Transaction2PcManager::new(rt_copy.clone(),
                                                guid_gen,
                                                commit_logger);

        let builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, "./db");
        let mut count = 0usize;
        let listener = move |
            db_mgr: &KVDBManager<usize, CommitLogger>,
            tr_mgr: &Transaction2PcManager<usize, CommitLogger>,
            events: &mut Vec<KVDBEvent<Guid>>,
        | {
            if events.len() == 1 {
                if events[0].is_report_transaction_info() {

                    println!("!!!!!!> commit total: {:?}, confirm total: {:?}, start total: {:?}, end total: {:?}, active: {:?}", tr_mgr.commit_logger().append_total_count(), tr_mgr.commit_logger().confirm_total_count(), tr_mgr.produced_transaction_total(), tr_mgr.consumed_transaction_total(), tr_mgr.transaction_len());
                }
            }
            count += events.len();
            events.clear();
            println!("!!!!!!> count: {:?}, commit total: {:?}, confirm total: {:?}, start total: {:?}, end total: {:?}, active: {:?}", count, tr_mgr.commit_logger().append_total_count(), tr_mgr.commit_logger().confirm_total_count(), tr_mgr.produced_transaction_total(), tr_mgr.consumed_transaction_total(), tr_mgr.transaction_len());
        };
        match builder.startup_with_listener(Some(listener)).await {
            Err(e) => {
                println!("!!!!!!startup db failed, reason: {:?}", e);
            },
            Ok(db) => {
                let tr = db.transaction(Atom::from("test_log"), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(Atom::from("test_log/a/b/c"),
                                                KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Str)).await {
                    //创建有序日志表失败
                    println!("!!!!!!create b-tree ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified().await.unwrap();
                if let Err(e) = tr.commit_modified(output).await {
                    panic!("init db table failed, reason: {:?}", e);
                }
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                sender.send(db);
            },
        }
    });
    thread::sleep(Duration::from_millis(3000));

    let rt_copy = rt.clone();
    let db = receiver.recv().unwrap();
    let db_copy = db.clone();
    b.iter(move || {
        let (s, r) = unbounded();
        let table_name = Atom::from("test_log/a/b/c");

        let now = Instant::now();
        for index in 0..1000usize {
            let s_copy = s.clone();
            let db_copy = db.clone();
            let table_name_copy = table_name.clone();

            let rt_clone = rt_copy.clone();
            let _ = rt_copy.spawn(async move {
                loop {
                    let tr = db_copy
                        .transaction(Atom::from("test b-tree table"),
                                     true,
                                     500,
                                     500)
                        .unwrap();

                    let _ = tr.upsert(vec![TableKV {
                        table: table_name_copy.clone(),
                        key: usize_to_binary(index),
                        value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                    }]).await;

                    match tr.prepare_modified().await {
                        Err(_e) => {
                            if let Err(e) = tr.rollback_modified().await {
                                println!("rollback failed, reason: {:?}", e);
                                break;
                            } else {
                                rt_clone.timeout(1).await;
                                continue;
                            }
                        },
                        Ok(output) => {
                            match tr.commit_modified(output).await {
                                Err(e) => {
                                    println!("commit failed, reason: {:?}", e);
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: commit fatal error");
                                    } else {
                                        println!("rollbakc ok for commit");
                                    }
                                    break;
                                },
                                Ok(()) => {
                                    s_copy.send(());
                                    break;
                                },
                            }
                        },
                    }
                }
            });
        }

        let mut count = 0;
        loop {
            match r.recv_timeout(Duration::from_millis(10000)) {
                Err(e) => {
                    println!(
                        "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                        rt_copy.wait_len(),
                        rt_copy.len(),
                        e
                    );
                    continue;
                },
                Ok(_) => {
                    count += 1;
                    if count >= 1000 {
                        break;
                    }
                },
            }
        }
        println!("time: {:?}", Instant::now() - now);
    });

    let db_clone = db_copy.clone();
    rt.spawn(async move {
        let mut transaction = db_clone
            .transaction(Atom::from("test_log/a/b/c"), false, 5000, 5000)
            .unwrap();
        let mut stream = transaction.values(Atom::from("test_log/a/b/c"), None, false).await.unwrap();
        for index in 0..1000 {
            if let Some((key, value)) = stream.next().await {
                assert_eq!(binary_to_usize(&key).unwrap(), index);
                assert_eq!(String::from_utf8_lossy(value.as_ref()).as_ref(), "Hello World!");
            } else {
                panic!("assert failed, index: {:?}", index);
            }
        }
        println!("======> assert cache before clean ok, len: {:?}", db_clone.table_record_size(&Atom::from("test_log/a/b/c")).await.unwrap());
    });

    thread::sleep(Duration::from_millis(70000));

    let db_clone = db_copy.clone();
    rt.spawn(async move {
        let mut transaction = db_clone
            .transaction(Atom::from("test_log/a/b/c"), false, 5000, 5000)
            .unwrap();
        let mut stream = transaction.values(Atom::from("test_log/a/b/c"), None, false).await.unwrap();
        for index in 0..1000 {
            if let Some((key, value)) = stream.next().await {
                assert_eq!(binary_to_usize(&key).unwrap(), index);
                assert_eq!(String::from_utf8_lossy(value.as_ref()).as_ref(), "Hello World!");
            } else {
                panic!("assert failed, index: {:?}", index);
            }
        }
        println!("======> assert cache after clean ok");
    });

    rt.spawn(async move {
        db_copy.report_transaction_info().await;
    });

    thread::sleep(Duration::from_millis(1000));
}

#[bench]
fn bench_multi_b_tree_table(b: &mut Bencher) {
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
                                                    KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                                                     true,
                                                                     EnumType::Usize,
                                                                     EnumType::Str)).await {
                        //创建有序日志表失败
                        println!("!!!!!!create b-tree ordered table failed, reason: {:?}", e);
                    }
                    let output = tr.prepare_modified().await.unwrap();
                    let _ = tr.commit_modified(output).await;
                }
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                sender.send(db);
            },
        }
    });
    thread::sleep(Duration::from_millis(3000));

    let rt_copy = rt.clone();
    let db = receiver.recv().unwrap();
    let mut table_names = Vec::new();
    for index in 0..10 {
        table_names.push(Atom::from("test_log".to_string() + index.to_string().as_str()));
    }
    b.iter(move || {
        let (s, r) = unbounded();

        let now = Instant::now();
        for index in 0..100usize {
            let s_copy = s.clone();
            let db_copy = db.clone();
            let table_names_copy = table_names.clone();

            let rt_clone = rt_copy.clone();
            let _ = rt_copy.spawn(async move {
                loop {
                    let tr = db_copy
                        .transaction(Atom::from("test log table"),
                                     true,
                                     500,
                                     500).unwrap();

                    for table_name in table_names_copy.clone() {
                        let _ = tr.upsert(vec![TableKV {
                            table: table_name,
                            key: usize_to_binary(index),
                            value: Some(Binary::new("Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!".as_bytes().to_vec()))
                        }]).await;
                    }

                    match tr.prepare_modified().await {
                        Err(_e) => {
                            if let Err(e) = tr.rollback_modified().await {
                                println!("rollback failed, reason: {:?}", e);
                                break;
                            } else {
                                rt_clone.timeout(1).await;
                                continue;
                            }
                        },
                        Ok(output) => {
                            match tr.commit_modified(output).await {
                                Err(e) => {
                                    println!("commit failed, reason: {:?}", e);
                                    if let ErrorLevel::Fatal = &e.level() {
                                        println!("rollback failed, reason: commit fatal error");
                                    } else {
                                        println!("rollbakc ok for commit");
                                    }
                                    break;
                                },
                                Ok(()) => {
                                    s_copy.send(());
                                    break;
                                },
                            }
                        },
                    }
                }
            });
        }

        let mut count = 0;
        loop {
            match r.recv_timeout(Duration::from_millis(10000)) {
                Err(e) => {
                    println!(
                        "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                        rt_copy.wait_len(),
                        rt_copy.len(),
                        e
                    );
                    continue;
                },
                Ok(_) => {
                    count += 1;
                    if count >= 100 {
                        break;
                    }
                },
            }
        }
        println!("time: {:?}", Instant::now() - now);
    });

    thread::sleep(Duration::from_millis(80000));
}

#[bench]
fn bench_iterator_b_tree_table(b: &mut Bencher) {
    use std::thread;
    use std::time::Duration;

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
                                                    KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                                                     true,
                                                                     EnumType::Usize,
                                                                     EnumType::Str)).await {
                        //创建有序日志表失败
                        println!("!!!!!!create b-tree ordered table failed, reason: {:?}", e);
                    }
                    let output = tr.prepare_modified().await.unwrap();
                    let _ = tr.commit_modified(output).await;
                }
                println!("!!!!!!db table size: {:?}", db.table_size().await);

                sender.send(db);
            },
        }
    });
    thread::sleep(Duration::from_millis(3000));

    let rt_copy = rt.clone();
    let db = receiver.recv().unwrap();
    let mut table_names = Vec::new();
    for index in 0..10 {
        table_names.push(Atom::from("test_log".to_string() + index.to_string().as_str()));
    }
    b.iter(move || {
        let (s, r) = unbounded();

        for _ in 0..1000usize {
            let s_copy = s.clone();
            let db_copy = db.clone();
            let table_names_copy = table_names.clone();

            let _ = rt_copy.spawn(async move {
                let tr = db_copy
                    .transaction(Atom::from("test log table"),
                                 true,
                                 500,
                                 500).unwrap();

                for table_name in table_names_copy {
                    let mut iterator = tr.values(table_name, None, true).await.unwrap();
                    while let Some(_) = iterator.next().await {}
                }

                s_copy.send(());
            });
        }

        let mut count = 0;
        loop {
            match r.recv_timeout(Duration::from_millis(10000)) {
                Err(e) => {
                    println!(
                        "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                        rt_copy.wait_len(),
                        rt_copy.len(),
                        e
                    );
                    continue;
                },
                Ok(_) => {
                    count += 1;
                    if count >= 1000 {
                        break;
                    }
                },
            }
        }
    });

    thread::sleep(Duration::from_millis(70000));
}

#[bench]
fn bench_sequence_b_tree_upsert(b: &mut Bencher) {
    use std::time::{Duration, Instant};
    use fastrand;

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

                let table_name = Atom::from("test_log/a/b/c");
                let tr = db.transaction(Atom::from("test seq upsert"), true, 500, 500).unwrap();
                if let Err(e) = tr.create_table(table_name.clone(),
                                                KVTableMeta::new(KVDBTableType::BtreeOrdTab,
                                                                 true,
                                                                 EnumType::Usize,
                                                                 EnumType::Str)).await {
                    //创建有序内存表失败
                    println!("!!!!!!create b-tree ordered table failed, reason: {:?}", e);
                }
                let output = tr.prepare_modified().await.unwrap();
                let _ = tr.commit_modified(output).await;

                println!("!!!!!!db table size: {:?}", db.table_size().await);

                sender.send(db);
            },
        }
    });

    let rt_copy = rt.clone();
    let db = receiver.recv().unwrap();

    b.iter(move || {
        let (s, r) = unbounded();
        let db_copy = db.clone();
        let table_name = Atom::from("test_log/a/b/c");
        let s_copy = s.clone();

        let now = Instant::now();
        let _ = rt_copy.spawn(async move {
            for _ in 0..1000 {
                let table_name0_copy = table_name.clone();
                let mut table_kv_list = Vec::new();

                table_kv_list.push(TableKV {
                    table: table_name0_copy.clone(),
                    key: usize_to_binary(255),
                    value: Some(Binary::new("Hello World!".as_bytes().to_vec()))
                });

                let tr = db_copy
                    .transaction(Atom::from("test table conflict"),
                                 true,
                                 500,
                                 500).unwrap();
                if let Err(e) = tr.upsert(table_kv_list).await {
                    println!("!!!!!!upsert failed, reason: {:?}", e);
                    return;
                }

                match tr.prepare_modified().await {
                    Err(e) => {
                        println!("!!!!!!> prepare failed, reason: {:?}", e);
                        if let Err(err) = tr.rollback_modified().await {
                            println!("rollback failed, error: {:?}, reason: {:?}", e, err);
                            s_copy.send(Err(e));
                        }
                    },
                    Ok(output) => {
                        match tr.commit_modified(output).await {
                            Err(e) => {
                                if let ErrorLevel::Fatal = &e.level() {
                                    println!("rollback failed, reason: {:?}", e);
                                    s_copy.send(Err(e));
                                }
                            },
                            Ok(()) => {
                                s_copy.send(Ok(()));
                            },
                        }
                    },
                }
            }
        });

        let mut error_count = 0;
        let mut count = 0;
        loop {
            match r.recv_timeout(Duration::from_millis(10000)) {
                Err(e) => {
                    println!(
                        "!!!!!!recv timeout, len: {}, timer_len: {}, e: {:?}",
                        rt_copy.wait_len(),
                        rt_copy.len(),
                        e
                    );
                    continue;
                },
                Ok(result) => {
                    if result.is_err() {
                        error_count += 1;
                    }

                    count += 1;
                    if count >= 1000 {
                        break;
                    }
                },
            }
        }
        println!("!!!!!!error: {}, time: {:?}", error_count, Instant::now() - now);
    });

    thread::sleep(Duration::from_millis(70000));
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


