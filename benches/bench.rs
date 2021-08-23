#![feature(test)]

extern crate test;
use test::Bencher;

use std::convert::TryInto;
use std::io::Result as IOResult;

use futures::{future::{FutureExt, BoxFuture},
              stream::{StreamExt, BoxStream}};
use crossbeam_channel::{unbounded, bounded};
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

#[bench]
fn bench_memory_table(b: &mut Bencher) {
    use std::thread;
    use std::time::{Duration, Instant};

    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();

    //异步构建数据库管理器
    let (sender, receiver) = bounded(1);
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

            rt_copy.spawn(rt_copy.alloc(), async move {
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
                        rt_copy.timing_len(),
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

            rt_copy.spawn(rt_copy.alloc(), async move {
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
                        rt_copy.timing_len(),
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

            rt_copy.spawn(rt_copy.alloc(), async move {
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
                        rt_copy.timing_len(),
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

            rt_copy.spawn(rt_copy.alloc(), async move {
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
                        rt_copy.timing_len(),
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

            rt_copy.spawn(rt_copy.alloc(), async move {
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
                        rt_copy.timing_len(),
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

            rt_copy.spawn(rt_copy.alloc(), async move {
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
                        rt_copy.timing_len(),
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
        for _ in 0..1000usize {
            let s_copy = s.clone();
            let db_copy = db.clone();
            let table_names_copy = table_names.clone();

            rt_copy.spawn(rt_copy.alloc(), async move {
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
                        rt_copy.timing_len(),
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