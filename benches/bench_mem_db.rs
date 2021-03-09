#![feature(test)]
extern crate test;
use test::Bencher;

use std::{sync::atomic::AtomicBool, thread, time::Duration};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Once;
use std::sync::RwLock;

use atom::Atom;
use bon::WriteBuffer;
use guid::GuidGen;
use pi_db::{db::{TabKV, TabMeta}, log_file_db::STORE_RUNTIME};
use pi_db::memery_db::MemDB;
use pi_db::mgr::{DatabaseWare, Mgr};
use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use r#async::rt::single_thread::SingleTaskRunner;
use r#async::rt::AsyncRuntime;
use sinfo;

static INIT: Once = Once::new();
static mut RT: Option<MultiTaskRuntime<()>> = None;

fn get_rt() -> MultiTaskRuntime<()> {
    unsafe {
        INIT.call_once(|| {
            let pool =
                MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
            let rt: MultiTaskRuntime<()> = pool.startup(true);
            RT = Some(rt);
        });
        RT.as_ref().unwrap().clone()
    }
}

#[bench]
fn bench_mem_db_write_single_tab(b: &mut Bencher) {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(true);

    let mgr = Mgr::new(GuidGen::new(0, 0));
    let mgr_copy = mgr.clone();
    let rt1 = rt.clone();

	let _ = rt.spawn(rt.alloc(), async move {
        if STORE_RUNTIME.read().await.is_none() {
            *STORE_RUNTIME.write().await = Some(rt1.clone());
        }
		let ware = DatabaseWare::new_mem_ware(MemDB::new());

        let _ = mgr.register(Atom::from("memory"), ware).await;
		let mut tr = mgr.transaction(true, Some(rt1.clone())).await;

		let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

		let _ = tr
			.alter(&Atom::from("memory"), &Atom::from("hello"), Some(meta))
			.await;
		let _ = tr.commit().await;
    });

	thread::sleep(Duration::from_secs(2));

    let mut wb = WriteBuffer::new();
    wb.write_bin(b"hello", 0..5);

    let item1 = TabKV {
        ware: Atom::from("memory"),
        tab: Atom::from("hello"),
        key: Arc::new(wb.bytes.clone()),
        value: Some(Arc::new(wb.bytes)),
        index: 0,
    };

	let rt_copy = rt.clone();

    b.iter(|| {
		let rt_copy1 = rt_copy.clone();
        let mgr_copy = mgr_copy.clone();
		let item = item1.clone();
	
		let (s, r) = crossbeam_channel::bounded(1);
        let _ = rt.spawn(rt.alloc(), async move {
			test_mem_db_write(&rt_copy1, &mgr_copy, vec![item.clone()]).await;
			let _ = s.send(());
        });
		let _ = r.recv();
    });
}

#[bench]
fn bench_mem_db_read_single_tab(b: &mut Bencher) {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(true);

    b.iter(|| {
        let _ = rt.spawn(rt.alloc(), async move { test_mem_db_read().await });
    });
}

#[bench]
fn bench_mem_db_concurrent_write(b: &mut Bencher) {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(true);

    b.iter(|| {
        let rt_clone = rt.clone();
        let _ = rt.spawn(rt.alloc(), async move {
            test_mem_db_concurrent_write(rt_clone).await
        });
    });
}

#[bench]
fn bench_mem_db_concurrent_read(b: &mut Bencher) {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(true);

    b.iter(|| {
        let rt_clone = rt.clone();
        let _ = rt.spawn(rt.alloc(), async move {
            test_mem_db_concurrent_read(rt_clone).await
        });
    });
}

#[bench]
fn bench_mem_db_iter(b: &mut Bencher) {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 8, 1024 * 1024, 10, Some(10));
    let rtm: MultiTaskRuntime<()> = pool.startup(false);

    let runner = SingleTaskRunner::new();
    let rt = runner.startup().unwrap();

    let flag = Arc::new(AtomicBool::new(false));
    let flag_clone = flag.clone();
    let mgr11 = Arc::new(RwLock::new(None));
    let mgr11_clone = mgr11.clone();

    let _ = rtm.spawn(rtm.alloc(), async move {
        let mgr = setup_data().await;
        *mgr11_clone.write().unwrap() = Some(mgr);
        flag_clone.store(true, Ordering::Relaxed);
    });

    loop {
        if flag.load(Ordering::Relaxed) {
            break;
        }
    }

    let mgr = mgr11.write().unwrap().take().unwrap();

    b.iter(|| {
        let mgr_clone = mgr.clone();
        let _ = rt.spawn(rt.alloc(), async move {
            test_mem_db_iter(mgr_clone).await;
        });
        let _ = runner.run_once();
    });
}

async fn setup_data() -> Mgr {
    let rt = get_rt();
    let mgr = Mgr::new(GuidGen::new(0, 0));
    let ware = DatabaseWare::new_mem_ware(MemDB::new());
    let _ = mgr.register(Atom::from("memory"), ware).await;
    let mut tr = mgr.transaction(true, Some(rt.clone())).await;
    let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);
    let _ = tr
        .alter(&Atom::from("memory"), &Atom::from("hello"), Some(meta))
        .await;

    let _ = tr.commit().await;
    let _ = tr.commit().await;

    let mut tr2 = mgr.transaction(true, Some(rt.clone())).await;

    let mut items = vec![];

    for i in 0..20000 {
        let key = format!("hello world {:?}", i);
        let mut wb = WriteBuffer::new();
        wb.write_bin(key.as_bytes(), 0..key.len());

        items.push(TabKV {
            ware: Atom::from("memory"),
            tab: Atom::from("hello"),
            key: Arc::new(wb.bytes.clone()),
            value: Some(Arc::new(wb.bytes)),
            index: 0,
        });
    }

    let _ = tr2.modify(items.clone(), None, false).await;
    let _ = tr2.prepare().await;
    let _ = tr2.commit().await;

    mgr
}

async fn test_mem_db_iter(mgr: Mgr) {
    let rt = get_rt();
    let mut tr3 = mgr.transaction(false, Some(rt)).await;

    let mut iter = tr3
        .iter(
            &Atom::from("memory"),
            &Atom::from("hello"),
            None,
            false,
            None,
        )
        .await
        .unwrap();

    while let Some(Ok(Some(_elem))) = iter.next() {
        // println!("elem = {:?}", elem);
    }

    let _ = tr3.prepare().await;
    let _ = tr3.commit().await;
}

async fn test_mem_db_concurrent_read(rt: MultiTaskRuntime<()>) {
    let mgr = Mgr::new(GuidGen::new(0, 0));
    let ware = DatabaseWare::new_mem_ware(MemDB::new());
    let _ = mgr.register(Atom::from("memory"), ware).await;
    let mut tr = mgr.transaction(true, Some(rt.clone())).await;
    let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

    let _ = tr.alter(&Atom::from("memory"), &Atom::from("hello"), Some(meta));

    let _ = tr.commit().await;
    let _ = tr.commit().await;

    let mgr2 = mgr.clone();
    let mgr3 = mgr.clone();
    let mgr5 = mgr.clone();
    let mgr4 = mgr.clone();
    let mgr6 = mgr.clone();

    let mut map = rt.map();
    let rt1 = rt.clone();
    let rt2 = rt.clone();
    let rt3 = rt.clone();
    let rt4 = rt.clone();
    let rt5 = rt.clone();

    async move {
        map.join(AsyncRuntime::Multi(rt.clone()), async move {
            let mut wb = WriteBuffer::new();
            wb.write_bin(b"hello1", 0..6);

            let item1 = TabKV {
                ware: Atom::from("memory"),
                tab: Atom::from("hello"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            };

            let mut tr2 = mgr2.transaction(true, Some(rt.clone())).await;

            let _ = tr2.query(vec![item1], None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt2.clone()), async move {
            let mut wb = WriteBuffer::new();
            wb.write_bin(b"hello2", 0..6);

            let item1 = TabKV {
                ware: Atom::from("memory"),
                tab: Atom::from("hello"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            };

            let mut tr2 = mgr3.transaction(true, Some(rt2.clone())).await;

            let _ = tr2.query(vec![item1], None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt3.clone()), async move {
            let mut wb = WriteBuffer::new();
            wb.write_bin(b"hello3", 0..6);

            let item1 = TabKV {
                ware: Atom::from("memory"),
                tab: Atom::from("hello"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            };

            let mut tr2 = mgr4.transaction(true, Some(rt3.clone())).await;

            let _ = tr2.query(vec![item1], None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt4.clone()), async move {
            let mut wb = WriteBuffer::new();
            wb.write_bin(b"hello4", 0..6);

            let item1 = TabKV {
                ware: Atom::from("memory"),
                tab: Atom::from("hello"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            };

            let mut tr2 = mgr5.transaction(true, Some(rt4.clone())).await;

            let _ = tr2.query(vec![item1], None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt5.clone()), async move {
            let mut wb = WriteBuffer::new();
            wb.write_bin(b"hello5", 0..6);

            let item1 = TabKV {
                ware: Atom::from("memory"),
                tab: Atom::from("hello"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            };

            let mut tr2 = mgr6.transaction(true, Some(rt5.clone())).await;

            let _ = tr2.query(vec![item1], None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });
        let _ = map.map(AsyncRuntime::Multi(rt1.clone())).await;
    }
    .await
}

async fn test_mem_db_concurrent_write(rt: MultiTaskRuntime<()>) {
    let mgr = Mgr::new(GuidGen::new(0, 0));
    let ware = DatabaseWare::new_mem_ware(MemDB::new());
    let _ = mgr.register(Atom::from("memory"), ware).await;
    let mut tr = mgr.transaction(true, Some(rt.clone())).await;
    let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

    let _ = tr
        .alter(&Atom::from("memory"), &Atom::from("hello"), Some(meta))
        .await;

    let _ = tr.commit().await;
    let _ = tr.commit().await;

    let mgr2 = mgr.clone();
    let mgr3 = mgr.clone();
    let mgr5 = mgr.clone();
    let mgr4 = mgr.clone();
    let mgr6 = mgr.clone();

    let mut map = rt.map();
    let rt1 = rt.clone();
    let rt2 = rt.clone();
    let rt3 = rt.clone();
    let rt4 = rt.clone();
    let rt5 = rt.clone();
    let rt6 = rt.clone();

    async move {
        map.join(AsyncRuntime::Multi(rt.clone()), async move {
            let mut wb = WriteBuffer::new();
            wb.write_bin(b"hello1", 0..6);

            let item1 = TabKV {
                ware: Atom::from("memory"),
                tab: Atom::from("hello"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            };

            let mut tr2 = mgr2.transaction(true, Some(rt1.clone())).await;

            let _ = tr2.modify(vec![item1], None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt2.clone()), async move {
            let mut wb = WriteBuffer::new();
            wb.write_bin(b"hello2", 0..6);

            let item1 = TabKV {
                ware: Atom::from("memory"),
                tab: Atom::from("hello"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            };

            let mut tr2 = mgr3.transaction(true, Some(rt2.clone())).await;

            let _ = tr2.modify(vec![item1], None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt3.clone()), async move {
            let mut wb = WriteBuffer::new();
            wb.write_bin(b"hello3", 0..6);

            let item1 = TabKV {
                ware: Atom::from("memory"),
                tab: Atom::from("hello"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            };

            let mut tr2 = mgr4.transaction(true, Some(rt3.clone())).await;

            let _ = tr2.modify(vec![item1], None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt4.clone()), async move {
            let mut wb = WriteBuffer::new();
            wb.write_bin(b"hello4", 0..6);

            let item1 = TabKV {
                ware: Atom::from("memory"),
                tab: Atom::from("hello"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            };

            let mut tr2 = mgr5.transaction(true, Some(rt4.clone())).await;

            let _ = tr2.modify(vec![item1], None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt5.clone()), async move {
            let mut wb = WriteBuffer::new();
            wb.write_bin(b"hello5", 0..6);

            let item1 = TabKV {
                ware: Atom::from("memory"),
                tab: Atom::from("hello"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            };

            let mut tr2 = mgr6.transaction(true, Some(rt5.clone())).await;

            let _ = tr2.modify(vec![item1], None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });
        let _ = map.map(AsyncRuntime::Multi(rt6.clone())).await;
    }
    .await
}

async fn test_mem_db_write(rt: &MultiTaskRuntime<()>, mgr: &Mgr, item: Vec<TabKV>) {
	for _ in 0..1000 {
		let mut tr2 = mgr.transaction(true, Some(rt.clone())).await;
		let _ = tr2.modify(item.clone(), None, false).await;
		let _ = tr2.prepare().await;
		let _ = tr2.commit().await;
	}
}

async fn test_mem_db_read() {
    let rt = get_rt();
    let mgr = Mgr::new(GuidGen::new(0, 0));
    let ware = DatabaseWare::new_mem_ware(MemDB::new());
    let _ = mgr.register(Atom::from("memory"), ware).await;
    let mut tr = mgr.transaction(true, Some(rt.clone())).await;

    let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

    let _ = tr
        .alter(&Atom::from("memory"), &Atom::from("hello"), Some(meta))
        .await;
    let _ = tr.commit().await;
    let _ = tr.commit().await;

    let _ = tr
        .tab_info(&Atom::from("memory"), &Atom::from("hello"))
        .await;

    let mut wb = WriteBuffer::new();
    wb.write_bin(b"hello", 0..5);

    let item1 = TabKV {
        ware: Atom::from("memory"),
        tab: Atom::from("hello"),
        key: Arc::new(wb.bytes.clone()),
        value: Some(Arc::new(wb.bytes)),
        index: 0,
    };

    let mut tr2 = mgr.transaction(true, Some(rt.clone())).await;

    let _ = tr2.query(vec![item1], None, false).await;
    let _ = tr2.prepare().await;
    let _ = tr2.commit().await;
}
