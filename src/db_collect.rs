use std::{collections::BTreeMap, env, path::PathBuf, sync::Arc};
use std::fs;
use std::str::FromStr;
use std::sync::atomic::{Ordering, AtomicI64};

use atom::Atom;
use bon::{Decode, ReadBuffer};
use hash::XHashMap;
use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use r#async::rt::{AsyncRuntime, AsyncValue};
use r#async::lock::spin_lock::SpinLock;
use chrono::prelude::*;

use crate::log_file_db::{AsyncLogFileStore, DB_META_TAB_NAME, LogFileDB, LogFileTab};

lazy_static! {
	// 最近一次整理的时间戳
	static ref LAST_COLLECT_DATE: Arc<AtomicI64> = Arc::new(AtomicI64::new(0));
}

pub fn collect_db(rt: MultiTaskRuntime<()>, start_at: u32) {
	let _ = rt.clone().spawn_timing(rt.clone().alloc(), async move {
		if should_collect(start_at) {
			let start = Local::now();
			// 记录整理完成的时间
			LAST_COLLECT_DATE.store(start.timestamp(), Ordering::Release);
			LogFileDB::force_split().await; // 强制分裂
			let meta = LogFileDB::open(&Atom::from(DB_META_TAB_NAME)).await.unwrap();
			let map = meta.1.map.lock();
			for (key, _) in map.iter() {
				let tab_name = Atom::decode(&mut ReadBuffer::new(key, 0)).unwrap();
				let mut file = LogFileDB::open(&tab_name).await.unwrap();
				info!("collect tab {:?} ", tab_name);
				file.1.log_file.collect(1024 * 1024, false).await;
			}
			let end = Local::now();
			info!("db collect done, start time =  {}, end time = {}", start, end);
			
		}
		collect_db(rt.clone(), start_at);
	// 每10分钟轮询一次
	}, 10 * 60 * 1000);
}

fn should_collect(start_at: u32) -> bool {
	let current_date: DateTime<Local> = Local::now();
	let year = current_date.year();
	let month = current_date.month();
	let day = current_date.day();
	let hour = current_date.hour();

	let within_one_hour = hour >= start_at && hour < start_at + 1;

	// 整理时间误差在1小时以内
	if !within_one_hour {
		return false
	}

	// 没有整理过
	if LAST_COLLECT_DATE.load(Ordering::Acquire) == 0 {
		return true
	}

	let prev_collect_date = DateTime::<Local>::from_utc(
		NaiveDateTime::from_timestamp(LAST_COLLECT_DATE.load(Ordering::Acquire), 0),
		FixedOffset::east(8 * 3600));

	// 跨年
	if year > prev_collect_date.year() {
		return true
	}
	// 跨月
	if month > prev_collect_date.month() {
		return true
	}
	// 跨天
	if day > prev_collect_date.day() {
		return true
	}

	false
}