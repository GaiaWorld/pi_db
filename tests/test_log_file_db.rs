use std::thread;
use std::sync::Arc;
use std::path::PathBuf;
use std::time::Duration;
use std::collections::{VecDeque, BTreeMap};
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam_channel::bounded;
use pi_db::{log_file_db::STORE_RUNTIME, mgr::{ DatabaseWare, Mgr }};
use pi_db::log_file_db::{LOG_FILE_SIZE, AsyncLogFileStore, LogFileDB};
use pi_atom::Atom;
use pi_sinfo;
use pi_guid::GuidGen;
use pi_async::{lock::spin_lock::SpinLock, rt::multi_thread::{MultiTaskRuntimeBuilder, MultiTaskRuntime}};
use pi_db::db::{TabKV, TabMeta};
use pi_bon::WriteBuffer;
use pi_hash::XHashMap;

#[test]
fn test_collect_log_file_db() {
	//初始化日志服务器
	env_logger::init();

	let builder = MultiTaskRuntimeBuilder::default();
	let rt  = builder.build();

	let rt_copy = rt.clone();
	rt.spawn(rt.alloc(), async move {
		*STORE_RUNTIME.write().await = Some(rt_copy.clone());
		LOG_FILE_SIZE.store(1, Ordering::SeqCst);

		let path = PathBuf::from("./tests/log");
		let file = match AsyncLogFileStore::open(path.clone(), 8000, 200 * 1024 * 1024, None).await {
			Err(e) => panic!("!!!!!!open table = {:?} failed, e: {:?}", path, e),
			Ok(file) => file
		};

		let mut store = AsyncLogFileStore {
			removed: Arc::new(SpinLock::new(XHashMap::default())),
			map: Arc::new(SpinLock::new(BTreeMap::new())),
			log_file: file.clone(),
			tmp_map: Arc::new(SpinLock::new(XHashMap::default())),
			writable_path: Arc::new(SpinLock::new(None)),
			is_statistics: Arc::new(AtomicBool::new(false)),
			is_init: Arc::new(AtomicBool::new(true)),
			statistics: Arc::new(SpinLock::new(VecDeque::new())),
		};

		println!("!!!!!!Load meta table start");
		file.load(&mut store, Some(path.clone()), 32 * 1024, false).await;
		store.is_init.store(false, Ordering::SeqCst);

		let map_len = store.map.lock().len();
		let writable_path = store.writable_path.lock().as_ref().cloned();
		let is_statistics = store.is_statistics.load(Ordering::Relaxed);
		println!("!!!!!!Load ok, path: {:?}, map len: {}, writable_path: {:?}, is_statistics: {}", path, map_len, writable_path, is_statistics);

		let mut log_total_len = 0;
		for (log_file, log_len, key_len) in store.statistics.lock().iter() {
			log_total_len += log_len;
			println!("!!!!!!Load ok, file: {:?}, log len: {}, key len: {}", log_file, log_len, key_len);
		}
		println!("!!!!!!Load finish, log total len: {}", log_total_len);

		println!("!!!!!!Init DB env");
		let mgr = Mgr::new(GuidGen::new(0, 0));
		let mgr_copy = mgr.clone();

		let ware = DatabaseWare::new_log_file_ware(
			LogFileDB::new(Atom::from("./tests/log"), 1024 * 1024 * 1024).await,
		);
		let _ = mgr_copy
			.register(Atom::from("./tests"), Arc::new(ware))
			.await;

		let mut tr = mgr_copy.transaction(true, Some(rt_copy.clone())).await;

		let meta = TabMeta::new(pi_sinfo::EnumType::Str, pi_sinfo::EnumType::Str);

		tr.alter(
			&Atom::from("./tests"),
			&Atom::from("./tests/log"),
			Some(Arc::new(meta)),
		)
			.await;
		tr.prepare().await;
		tr.commit().await;

		println!("!!!!!!Init Tab");
		let mut tr = mgr.transaction(true, Some(rt_copy.clone())).await;
		for index in 0..100000 {
			let mut items = vec![];

			let mut wb = WriteBuffer::new();
			let string = "Test".to_string() + index.to_string().as_str();
			let key = string.as_bytes();
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("./tests"),
				tab: Atom::from("./tests/log"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			});

			let _ = tr.modify(items, None, false).await;
		}
		let _ = tr.prepare().await;
		let _ = tr.commit().await;

		let mut tr = mgr.transaction(true, Some(rt_copy.clone())).await;
		for index in 0..100000 {
			let mut items = vec![];

			let mut wb = WriteBuffer::new();
			let string = "Test".to_string() + index.to_string().as_str();
			let key = string.as_bytes();
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("./tests"),
				tab: Atom::from("./tests/log"),
				key: Arc::new(wb.bytes.clone()),
				value: None,
				index: 0,
			});

			let _ = tr.modify(items, None, false).await;
		}
		let _ = tr.prepare().await;
		let _ = tr.commit().await;

		let mut tr = mgr.transaction(true, Some(rt_copy.clone())).await;
		for index in 0..100000 {
			let mut items = vec![];

			let mut wb = WriteBuffer::new();
			let string = "Test".to_string() + index.to_string().as_str();
			let key = string.as_bytes();
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("./tests"),
				tab: Atom::from("./tests/log"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			});

			let _ = tr.modify(items, None, false).await;
		}
		let _ = tr.prepare().await;
		let _ = tr.commit().await;

		let mut tr = mgr.transaction(true, Some(rt_copy.clone())).await;
		for index in 100000..200000 {
			let mut items = vec![];

			let mut wb = WriteBuffer::new();
			let string = "Test".to_string() + index.to_string().as_str();
			let key = string.as_bytes();
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("./tests"),
				tab: Atom::from("./tests/log"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			});

			let _ = tr.modify(items, None, false).await;
		}
		let _ = tr.prepare().await;
		let _ = tr.commit().await;

		let mut tr = mgr.transaction(true, Some(rt_copy.clone())).await;
		for index in 0..100000 {
			let mut items = vec![];

			let mut wb = WriteBuffer::new();
			let string = "Test".to_string() + index.to_string().as_str();
			let key = string.as_bytes();
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("./tests"),
				tab: Atom::from("./tests/log"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			});

			let _ = tr.modify(items, None, false).await;
		}
		let _ = tr.prepare().await;
		let _ = tr.commit().await;

		let mut tr = mgr.transaction(true, Some(rt_copy.clone())).await;
		for index in 100000..200000 {
			let mut items = vec![];

			let mut wb = WriteBuffer::new();
			let string = "Test".to_string() + index.to_string().as_str();
			let key = string.as_bytes();
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("./tests"),
				tab: Atom::from("./tests/log"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			});

			let _ = tr.modify(items, None, false).await;
		}
		let _ = tr.prepare().await;
		let _ = tr.commit().await;
		println!("!!!!!!Init Tab finish");

		rt_copy.wait_timeout(5000).await;

		let rt0 = rt_copy.clone();
		rt_copy.spawn(rt_copy.alloc(), async move {
			let mut tr = mgr.transaction(true, Some(rt0.clone())).await;
			for index in 100000..200000 {
				let mut items = vec![];

				let mut wb = WriteBuffer::new();
				let string = "Test".to_string() + index.to_string().as_str();
				let key = string.as_bytes();
				wb.write_bin(key, 0..key.len());

				items.push(TabKV {
					ware: Atom::from("./tests"),
					tab: Atom::from("./tests/log"),
					key: Arc::new(wb.bytes.clone()),
					value: Some(Arc::new(wb.bytes)),
					index: 0,
				});

				let _ = tr.modify(items, None, false).await;
			}
			let _ = tr.prepare().await;
			let _ = tr.commit().await;
		});

		println!("!!!!!!Test collect 0 start");
		if let Err(e) = LogFileDB::collect().await {
			panic!("Test collect failed, reason: {}", e);
		}
		println!("!!!!!!Test collect 0 finish");

		println!("!!!!!!Test collect 1 start");
		if let Err(e) = LogFileDB::collect().await {
			panic!("Test collect failed, reason: {}", e);
		}
		println!("!!!!!!Test collect 1 finish");
	});

	thread::sleep(Duration::from_millis(100000000));
}

#[test]
fn test_log_file_db() {
	let builder = MultiTaskRuntimeBuilder::default();
	let rt  = builder.build();
	let rt1 = rt.clone();

	let _ = rt1.spawn(rt.alloc(), async move {
		*STORE_RUNTIME.write().await = Some(rt.clone());
		let mgr = Mgr::new(GuidGen::new(0, 0));
		let ware = DatabaseWare::new_log_file_ware(LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024).await);
		let _ = mgr.register(Atom::from("logfile"), Arc::new(ware)).await;
		let mut tr = mgr.transaction(true, Some(rt.clone())).await;

		let meta = TabMeta::new(pi_sinfo::EnumType::Str, pi_sinfo::EnumType::Str);
		let meta1 = TabMeta::new(pi_sinfo::EnumType::Str, pi_sinfo::EnumType::Str);

		tr.alter(&Atom::from("logfile"), &Atom::from("./testlogfile/hello"), Some(Arc::new(meta))).await;
		tr.alter(&Atom::from("logfile"), &Atom::from("./testlogfile/world"), Some(Arc::new(meta1))).await;
		let p = tr.prepare().await;
		println!("tr prepare ---- {:?}", p);
		tr.commit().await;

		let info = tr.tab_info(&Atom::from("logfile"), &Atom::from("./testlogfile/hello")).await;
		println!("info ---- {:?} ", info);

		let mut wb = WriteBuffer::new();
		wb.write_bin(b"hello", 0..5);

		println!("wb = {:?}", wb.bytes);

		let mut item1 = TabKV {
			ware: Atom::from("logfile"),
			tab: Atom::from("./testlogfile/hello"),
			key: Arc::new(wb.bytes.clone()),
			value: Some(Arc::new(wb.bytes)),
			index: 0
		};

		let mut writes = vec![];
		let key_template = "keyyyyyyyyyyyyyyyyyyyyyyyy".to_string();
		let value_template = "valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevalue".to_string();

		for i in 0..100000 {
			let mut key_wb = WriteBuffer::new();
			let key = format!("{:?}{:?}", key_template, i);
			key_wb.write_bin(key.as_bytes(), 0..key.len());

			let mut value_wb = WriteBuffer::new();
			let value = format!("{:?}{:?}", value_template, i);
			value_wb.write_bin(value.as_bytes(), 0..value.len());
			writes.push(TabKV {
				ware: Atom::from("logfile"),
				tab: Atom::from("./testlogfile/hello"),
				key: Arc::new(key_wb.bytes.clone()),
				value: Some(Arc::new(value_wb.bytes.clone())),
				index: i
			})
		}

		let mut tr2 = mgr.transaction(true, Some(rt.clone())).await;

		let r = tr2.modify(writes, None, false).await;
		println!("logfile result = {:?}", r);
		let p = tr2.prepare().await;
		tr2.commit().await;

		let mut tr3 = mgr.transaction(false, Some(rt.clone())).await;
		item1.value = None;

		let q = tr3.query(vec![item1], None, false).await;
		println!("query item = {:?}", q);
		tr3.prepare().await;
		tr3.commit().await;

		let mut tr4 = mgr.transaction(false, Some(rt.clone())).await;
		let size = tr4.tab_size(&Atom::from("logfile"), &Atom::from("./testlogfile/hello")).await;
		println!("tab size = {:?}", size);
		{
			let iter = tr4.iter(&Atom::from("logfile"), &Atom::from("./testlogfile/hello"), None, false, None).await;

			if let Ok(mut it) = iter {
				loop {
					let item = it.next();
					println!("iter item = {:?}", item);
					match item {
						Some(Ok(None)) | Some(Err(_)) => break,
						_ => {}
					}
				}
			}
		}

		let tabs = tr4.list(&Atom::from("logfile")).await;
		println!("tabs = {:?}", tabs);

		tr4.prepare().await;
		tr4.commit().await;
	});

	std::thread::sleep(std::time::Duration::from_secs(20));
}

#[test]
fn write_test_data() {
	let builder = MultiTaskRuntimeBuilder::default();
	let rt  = builder.build();
	let rt1 = rt.clone();

	let _ = rt1.spawn(rt.alloc(), async move {
		*STORE_RUNTIME.write().await = Some(rt.clone());
		let mgr = Mgr::new(GuidGen::new(0, 0));
		let ware = DatabaseWare::new_log_file_ware(LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024).await);
		let _ = mgr.register(Atom::from("logfile"), Arc::new(ware)).await;
		let mut tr = mgr.transaction(true, Some(rt.clone())).await;

		let meta = TabMeta::new(pi_sinfo::EnumType::Str, pi_sinfo::EnumType::Str);
		let meta1 = TabMeta::new(pi_sinfo::EnumType::Str, pi_sinfo::EnumType::Str);

		tr.alter(&Atom::from("logfile"), &Atom::from("./testlogfile/testtab"), Some(Arc::new(meta))).await;
		let p = tr.prepare().await;
		println!("tr prepare ---- {:?}", p);
		tr.commit().await;

		let info = tr.tab_info(&Atom::from("logfile"), &Atom::from("./testlogfile/testtab")).await;
		println!("info ---- {:?} ", info);

		let mut writes = vec![];
		let key_template = "keyyyyyyyyyyyyyyyyyyyyyyyy".to_string();
		let value_template = "valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevalue".to_string();

		for i in 0..1000 {
			let mut key_wb = WriteBuffer::new();
			let key = format!("{:?}{:?}", key_template, i);
			key_wb.write_bin(key.as_bytes(), 0..key.len());

			let mut value_wb = WriteBuffer::new();
			let value = format!("{:?}{:?}", value_template, i);
			value_wb.write_bin(value.as_bytes(), 0..value.len());
			writes.push(TabKV {
				ware: Atom::from("logfile"),
				tab: Atom::from("./testlogfile/testtab"),
				key: Arc::new(key_wb.bytes.clone()),
				value: Some(Arc::new(value_wb.bytes.clone())),
				index: i
			})
		}

		let mut tr2 = mgr.transaction(true, Some(rt.clone())).await;

		let r = tr2.modify(writes, None, false).await;
		println!("logfile result = {:?}", r);
		let p = tr2.prepare().await;
		println!("prepare result {:?}", p);
		tr2.commit().await;


		let mut tr4 = mgr.transaction(false, Some(rt.clone())).await;

		let size = tr4.tab_size(&Atom::from("logfile"), &Atom::from("./testlogfile/testtab")).await;
		println!("tab size = {:?}", size);
		let tabs = tr4.list(&Atom::from("logfile")).await;
		println!("tabs = {:?}", tabs);

		tr4.prepare().await;
		tr4.commit().await;
	});

	std::thread::sleep(std::time::Duration::from_secs(200));
}

#[test]
fn read_test_data() {
	let builder = MultiTaskRuntimeBuilder::default();
	let rt  = builder.build();
	let rt1 = rt.clone();

	let _ = rt1.spawn(rt.alloc(), async move {
		*STORE_RUNTIME.write().await = Some(rt.clone());
		let mgr = Mgr::new(GuidGen::new(0, 0));
		let ware = DatabaseWare::new_log_file_ware(LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024).await);
		let _ = mgr.register(Atom::from("logfile"), Arc::new(ware)).await;
		let mut tr = mgr.transaction(true, Some(rt.clone())).await;

		let meta = TabMeta::new(pi_sinfo::EnumType::Str, pi_sinfo::EnumType::Str);

		let a = tr.alter(&Atom::from("logfile"), &Atom::from("./testlogfile/testtab"), Some(Arc::new(meta))).await;
		println!("alter result ==== {:?}", a);
		let p = tr.prepare().await;
		println!("tr prepare ---- {:?}", p);
		tr.commit().await;

		let info = tr.tab_info(&Atom::from("logfile"), &Atom::from("./testlogfile/testtab")).await;
		println!("info ---- {:?} ", info);

		let mut tr4 = mgr.transaction(false, Some(rt.clone())).await;
		let size = tr4.tab_size(&Atom::from("logfile"), &Atom::from("./testlogfile/testtab")).await;
		println!("tab size = {:?}", size);
		{
			let iter = tr4.iter(&Atom::from("logfile"), &Atom::from("./testlogfile/testtab"), None, false, None).await;

			if let Ok(mut it) = iter {
				loop {
					let item = it.next();
					println!("iter item = {:?}", item);
					match item {
						Some(Ok(None)) | Some(Err(_)) => break,
						_ => {}
					}
				}
			}
		}
	});

	std::thread::sleep(std::time::Duration::from_secs(200));
}

#[test]
fn bench_log_file_write() {
	let builder = MultiTaskRuntimeBuilder::default();
	let rt  = builder.build();

    let mgr = Mgr::new(GuidGen::new(0, 0));
    let mgr_copy = mgr.clone();

    let rt1 = rt.clone();
    rt.spawn(rt.alloc(), async move {
        if STORE_RUNTIME.read().await.is_none() {
            *STORE_RUNTIME.write().await = Some(rt1.clone());
        }

        let ware = DatabaseWare::new_log_file_ware(
            LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024).await,
        );
        let _ = mgr_copy
            .register(Atom::from("logfile"), Arc::new(ware))
            .await;

        let mut tr = mgr_copy.transaction(true, Some(rt1.clone())).await;

        let meta = TabMeta::new(pi_sinfo::EnumType::Str, pi_sinfo::EnumType::Str);

        tr.alter(
            &Atom::from("logfile"),
            &Atom::from("./testlogfile/hello"),
            Some(Arc::new(meta)),
        )
        .await;
        tr.prepare().await;
        tr.commit().await;
    });

    std::thread::sleep(std::time::Duration::from_millis(5000));

	let rt_copy = rt.clone();

	let mut total_time = 0;
	for index in 0..100 {
		let rt_copy1 = rt_copy.clone();
		let mgr_copy = mgr.clone();
		let (s, r) = bounded(1);

		let start = std::time::Instant::now();
		let _ = rt.spawn(rt.alloc(), async move {
			for index in 0..1000 {
				log_file_write(&rt_copy1, &mgr_copy, index).await;
			}
			s.send(());
		});
		r.recv();
		let time = start.elapsed();
		total_time += time.as_micros();
		println!("!!!!!!index: {}, time: {:?}, total_time: {}", index, time, total_time);
	}
	println!("!!!!!!time: {}", total_time / 100);
}

async fn log_file_write(rt: &MultiTaskRuntime<()>, mgr: &Mgr, index: usize) {
    let mut tr = mgr.transaction(true, Some((*rt).clone())).await;
    let mut items = vec![];

    let mut wb = WriteBuffer::new();
    let string = "hello world".to_string() + index.to_string().as_str();
    let key = string.as_bytes();
    wb.write_bin(key, 0..key.len());

    items.push(TabKV {
        ware: Atom::from("logfile"),
        tab: Atom::from("./testlogfile/hello"),
        key: Arc::new(wb.bytes.clone()),
        value: Some(Arc::new(wb.bytes)),
        index: 0,
    });

    let _ = tr.modify(items, None, false).await;
    let _ = tr.prepare().await;
    let _ = tr.commit().await;
}