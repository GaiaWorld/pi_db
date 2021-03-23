use std::{collections::HashMap, sync::Arc};
use std::thread;
use std::time::Duration;
use std::sync::Mutex;
use atom::Atom;
use bon::{Encode, Decode, WriteBuffer, ReadBuffer, ReadBonErr};
use pi_db::{log_file_db, mgr::{ DatabaseWare, Mgr }};
use pi_db::log_file_db::LogFileDB;
use sinfo;
use guid::GuidGen;
use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use pi_db::db::{TabKV, TabMeta};
use pi_db::fork::TableMetaInfo;

use log_file_db::STORE_RUNTIME;

#[test]
fn test_fork() {
	let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
	let rt: MultiTaskRuntime<()>  = pool.startup(true);
	let rt1 = rt.clone();

	let _ = rt1.spawn(rt.alloc(), async move {
		*STORE_RUNTIME.write().await = Some(rt.clone());

		let mgr = Mgr::new(GuidGen::new(0, 0));
		let ware = DatabaseWare::new_log_file_ware(LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024).await);
		let _ = mgr.register(Atom::from("logfile"), Arc::new(ware)).await;

		let mut tr = mgr.transaction(true, Some(rt.clone())).await;
		let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Bin);
		let meta1 = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

		// 创建一个用于存储元信息的表
		tr.alter(&Atom::from("logfile"), &Atom::from("./testlogfile/tabs_meta"), Some(Arc::new(meta))).await;
		tr.alter(&Atom::from("logfile"), &Atom::from("./testlogfile/hello"), Some(Arc::new(meta1))).await;
		let p = tr.prepare().await;
		tr.commit().await;

		let mut k = WriteBuffer::new();
		k.write_bin(b"hello1", 0..6);
		let mut v = WriteBuffer::new();
		v.write_bin(b"world1", 0..6);

		let item1 = TabKV {
			ware: Atom::from("logfile"),
			tab: Atom::from("./testlogfile/hello"),
			key: Arc::new(k.bytes.clone()),
			value: Some(Arc::new(v.bytes)),
			index: 0
		};

		let mut k = WriteBuffer::new();
		k.write_bin(b"hello2", 0..6);
		let mut v = WriteBuffer::new();
		v.write_bin(b"world2", 0..6);

		let item2 = TabKV {
			ware: Atom::from("logfile"),
			tab: Atom::from("./testlogfile/hello"),
			key: Arc::new(k.bytes.clone()),
			value: Some(Arc::new(v.bytes)),
			index: 0
		};

		let mut k = WriteBuffer::new();
		k.write_bin(b"hello3", 0..6);
		let mut v = WriteBuffer::new();
		v.write_bin(b"world3", 0..6);

		let item3 = TabKV {
			ware: Atom::from("logfile"),
			tab: Atom::from("./testlogfile/hello"),
			key: Arc::new(k.bytes.clone()),
			value: Some(Arc::new(v.bytes)),
			index: 0
		};

		let mut k = WriteBuffer::new();
		k.write_bin(b"hello4", 0..6);
		let mut v = WriteBuffer::new();
		v.write_bin(b"world4", 0..6);

		let item4 = TabKV {
			ware: Atom::from("logfile"),
			tab: Atom::from("./testlogfile/hello"),
			key: Arc::new(k.bytes.clone()),
			value: Some(Arc::new(v.bytes)),
			index: 0
		};

		let mut tr2 = mgr.transaction(true, Some(rt.clone())).await;
		tr2.modify(vec![item1, item2, item3, item4], None, false).await;
		tr2.prepare().await;
		tr2.commit().await;


		// 需要开一个新的事务来执行分叉操作？？
		let mut tr3 = mgr.transaction(true, Some(rt.clone())).await;
		let tabs = tr3.list(&Atom::from("logfile")).await;
		println!("tabs === {:?}", tabs);
		let tm = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);
		tr3.fork_tab(Atom::from("logfile"), Atom::from("./testlogfile/hello"), Atom::from("./testlogfile/hello_fork"), tm).await;
		let p = tr3.prepare().await;
		println!("prepare ==== {:?}", p);
		let c = tr3.commit().await;
		println!("commit=== {:?}", c);

		let mut tr4 = mgr.transaction(true, Some(rt.clone())).await;

		let mut k = WriteBuffer::new();
		k.write_bin(b"hello5", 0..6);
		let mut v = WriteBuffer::new();
		v.write_bin(b"world5", 0..6);

		let item5 = TabKV {
			ware: Atom::from("logfile"),
			tab: Atom::from("./testlogfile/hello"),
			key: Arc::new(k.bytes.clone()),
			value: Some(Arc::new(v.bytes)),
			index: 0
		};

		let mut k = WriteBuffer::new();
		k.write_bin(b"hello6", 0..6);
		let mut v = WriteBuffer::new();
		v.write_bin(b"world6", 0..6);

		let item6 = TabKV {
			ware: Atom::from("logfile"),
			tab: Atom::from("./testlogfile/hello"),
			key: Arc::new(k.bytes.clone()),
			value: Some(Arc::new(v.bytes)),
			index: 0
		};

		tr4.modify(vec![item5, item6], None, false).await;
		tr4.prepare().await;
		tr4.commit().await;

	});

	thread::sleep(Duration::from_secs(3));
}

#[test]
fn test_load_data() {
	let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
	let rt: MultiTaskRuntime<()>  = pool.startup(true);
	let rt1 = rt.clone();

	let _ = rt1.spawn(rt.alloc(), async move {
		*STORE_RUNTIME.write().await = Some(rt.clone());
		let mgr = Mgr::new(GuidGen::new(0, 0));
		let ware = DatabaseWare::new_log_file_ware(LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024).await);
		let _ = mgr.register(Atom::from("logfile"), Arc::new(ware)).await;

		let mut tr1 = mgr.transaction(true, Some(rt.clone())).await;
		let meta1 = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);
		tr1.alter(&Atom::from("logfile"), &Atom::from("./testlogfile/hello"), Some(Arc::new(meta1.clone()))).await;
		tr1.alter(&Atom::from("logfile"), &Atom::from("./testlogfile/hello_fork"), Some(Arc::new(meta1.clone()))).await;
		tr1.alter(&Atom::from("logfile"), &Atom::from("./testlogfile/hello_fork2"), Some(Arc::new(meta1))).await;
		tr1.prepare().await;
		tr1.commit().await;

		let mut tr2 = mgr.transaction(false, Some(rt.clone())).await;
		let mut iter = tr2.iter(&Atom::from("logfile"), &Atom::from("./testlogfile/hello_fork"), None, false, None).await.unwrap();
		println!("hello_fork");
		while let Some(Ok(Some(elem))) = iter.next() {
			println!("elem = {:?}", elem);
		}
		tr2.prepare().await;
		tr2.commit().await;

		let mut tr3 = mgr.transaction(true, Some(rt.clone())).await;
	
		let mut k = WriteBuffer::new();
		k.write_bin(b"hello7", 0..6);
		let mut v = WriteBuffer::new();
		v.write_bin(b"world7", 0..6);

		let item7 = TabKV {
			ware: Atom::from("logfile"),
			tab: Atom::from("./testlogfile/hello_fork"),
			key: Arc::new(k.bytes.clone()),
			value: Some(Arc::new(v.bytes)),
			index: 0
		};
		tr3.modify(vec![item7], None, false).await;
		tr3.prepare().await;
		tr3.commit().await;

		let mut tr4 = mgr.transaction(true, Some(rt.clone())).await;
		let tm = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);
		tr4.fork_tab(Atom::from("logfile"), Atom::from("./testlogfile/hello_fork"), Atom::from("./testlogfile/hello_fork2"), tm).await;
		tr4.prepare().await;
		tr4.commit().await;

		let mut tr5 = mgr.transaction(true, Some(rt.clone())).await;
		let mut k = WriteBuffer::new();
		k.write_bin(b"hello8", 0..6);
		let mut v = WriteBuffer::new();
		v.write_bin(b"world8", 0..6);

		let item8 = TabKV {
			ware: Atom::from("logfile"),
			tab: Atom::from("./testlogfile/hello_fork2"),
			key: Arc::new(k.bytes.clone()),
			value: Some(Arc::new(v.bytes)),
			index: 0
		};
		tr5.modify(vec![item8], None, false).await;
		tr5.prepare().await;
		tr5.commit().await;

		let mut tr6 = mgr.transaction(false, Some(rt.clone())).await;
		let mut iter = tr6.iter(&Atom::from("logfile"), &Atom::from("./testlogfile/hello_fork2"), None, false, None).await.unwrap();
		println!("hello_fork2");
		while let Some(Ok(Some(elem))) = iter.next() {
			println!("elem = {:?}", elem);
		}
		tr6.prepare().await;
		tr6.commit().await;

		let mut tr7 = mgr.transaction(false, Some(rt.clone())).await;
		let mut iter = tr7.iter(&Atom::from("logfile"), &Atom::from("./testlogfile/hello"), None, false, None).await.unwrap();
		println!("hello");

		while let Some(Ok(Some(elem))) = iter.next() {
			println!("elem = {:?}", elem);
		}
		tr7.prepare().await;
		tr7.commit().await;

	});

	thread::sleep(Duration::from_secs(3));
}