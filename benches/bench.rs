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
use async_transaction::{TransactionError,
                        AsyncCommitLog,
                        ErrorLevel,
                        manager_2pc::Transaction2PcManager};
use pi_store::commit_logger::{CommitLoggerBuilder, CommitLogger};

use pi_db::{Binary, KVDBTableType, KVTableMeta, db::KVDBManagerBuilder, tables::TableKV, KVTableTrError};

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

#[bench]
fn bench_sequence_upsert(b: &mut Bencher) {
    use std::thread;
    use std::time::{Duration, Instant};
    use fastrand;

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
        rt_copy.spawn(rt_copy.alloc(), async move {
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
                        rt_copy.timing_len(),
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
    use std::thread;
    use std::time::{Duration, Instant};
    use fastrand;

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

            rt_copy.spawn(rt_copy.alloc(), async move {
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
                                rt_copy_.wait_timeout(0).await;
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
                                        rt_copy_.wait_timeout(0).await;
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
                        rt_copy.timing_len(),
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