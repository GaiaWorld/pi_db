use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::MultiTaskRuntimeBuilder, serial::AsyncValue, startup_global_time_loop,
    AsyncRuntime, AsyncRuntimeExt,
};
use pi_guid::{Guid, GuidGen};
use pi_time::run_nanos;

use pi_async_transaction::{manager_2pc::Transaction2PcManager, ErrorLevel, Transaction2Pc};
use pi_store::{
    commit_logger::{CommitLogger, CommitLoggerBuilder},
    log_store::log_file::{LogFile, LogMethod, PairLoader},
};
use std::{
    path::Path,
    thread,
    time::{Duration, Instant},
};

use futures::stream::StreamExt;

use pi_atom::Atom;

use pi_db::{
    db::{KVDBManager, KVDBManagerBuilder},
    init_transaction_debug_logger,
    inspector::{CommitLogInspector, LogTableInspector},
    tables::TableKV,
    utils::{CreateTableOptions, KVDBEvent},
    Binary, KVDBTableType, KVTableMeta,
};

use pi_sinfo::EnumType;

pub fn db_test(db: String) -> Result<(), String> {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let (s, r) = bounded(1);
    rt.spawn(async move {
        let db_mgr = start_db::<
            fn(
                &KVDBManager<usize, CommitLogger>,
                &Transaction2PcManager<usize, CommitLogger>,
                &mut Vec<KVDBEvent<Guid>>,
            ),
        >(db, None)
        .await
        .unwrap();
        for (tab_name, _, size) in get_tables(db_mgr).await.unwrap() {
            println!("tab_name:{tab_name}, size:{size}");
        }
        s.send(Ok(()));
    });
    r.recv().or_else(|e| Err(e.to_string()))?
}

pub fn conversion(src: String, out: String, batch_count: usize) -> Result<(), String> {
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let (s, r) = bounded(1);
    rt.spawn(async move {
        let r = handle(src, out, batch_count).await;
        s.send(r);
    });
    r.recv().or_else(|e| Err(e.to_string()))?
}

async fn handle(src: String, out: String, batch_count: usize) -> Result<(), String> {
    let (s, r) = bounded(1);
    let mut count = 1;
    let listener = move |db_mgr: &KVDBManager<usize, CommitLogger>,
                         tr_mgr: &Transaction2PcManager<usize, CommitLogger>,
                         events: &mut Vec<KVDBEvent<Guid>>| {
        count += events.len();
        events.clear();
        println!(
            "!!!!!!> start total: {:?}, end total: {:?}, active: {:?}, count:{count}",
            tr_mgr.produced_transaction_total(),
            tr_mgr.consumed_transaction_total(),
            tr_mgr.transaction_len()
        );
        if tr_mgr.produced_transaction_total() == tr_mgr.consumed_transaction_total()
            && tr_mgr.transaction_len() == 0
            && count == tr_mgr.consumed_transaction_total()
        {
            s.send(true);
        }
    };
    let src_db_mgr = start_db::<
        fn(
            &KVDBManager<usize, CommitLogger>,
            &Transaction2PcManager<usize, CommitLogger>,
            &mut Vec<KVDBEvent<Guid>>,
        ),
    >(src, None)
    .await?;
    let out_db_mgr = start_db(out, Some(listener)).await?;
    let tables = get_tables(src_db_mgr.clone()).await?;
    /// 创建表
    create_table(out_db_mgr.clone(), &tables, KVDBTableType::BtreeOrdTab).await?;
    // println!("out_db_mgr tables:{:?}", out_db_mgr.tables().await);
    /// 批量转换表数据
    batch(
        src_db_mgr.clone(),
        out_db_mgr.clone(),
        &tables,
        KVDBTableType::BtreeOrdTab,
        batch_count,
    )
    .await?;
    println!("=======等待数据库落地==========");
    /// 等待数据库落地
    r.recv().or_else(|e| Err(e.to_string()))?;
    println!("=======检测记录数是否正确======");
    /// 检测记录数是否正确
    check_table_size(out_db_mgr.clone(), &tables).await?;
    Ok(())
}

async fn start_db<F>(
    path: String,
    listener: Option<F>,
) -> Result<KVDBManager<usize, CommitLogger>, String>
where
    F: FnMut(
            &KVDBManager<usize, CommitLogger>,
            &Transaction2PcManager<usize, CommitLogger>,
            &mut Vec<KVDBEvent<Guid>>,
        ) + Send
        + Sync
        + 'static,
{
    let builder = MultiTaskRuntimeBuilder::default();
    let rt = builder.build();
    let rt_copy = rt.clone();
    let value = AsyncValue::new();
    let value_copy = value.clone();
    rt.spawn(async move {
        let path = Path::new(&path);
        let guid_gen = GuidGen::new(run_nanos(), 0);
        let commit_logger_builder =
            CommitLoggerBuilder::new(rt_copy.clone(), path.join("./.commit_log"));
        let commit_logger = commit_logger_builder.build().await.unwrap();

        let tr_mgr = Transaction2PcManager::new(rt_copy.clone(), guid_gen, commit_logger);

        let mut builder = KVDBManagerBuilder::new(rt_copy.clone(), tr_mgr, path);
        let now = Instant::now();
        match builder.startup_with_listener(listener).await {
            Err(e) => {
                panic!("{:?}", e);
                value.set(Err(e.to_string()));
            }
            Ok(db_mgr) => {
                value.set(Ok(db_mgr));
            }
        }
    });
    value_copy.await
}

async fn get_tables(
    db_mgr: KVDBManager<usize, CommitLogger>,
) -> Result<Vec<(Atom, KVTableMeta, usize)>, String> {
    let mut result = vec![];
    let tabs = db_mgr.tables().await;
    let tr = db_mgr
        .transaction(Atom::from("get_tables"), false, 1000, 1000)
        .ok_or_else(|| "transaction error".to_string())?;
    for tab_name in tabs {
        if let Some(meta) = tr.table_meta(tab_name.clone()).await {
            let size = db_mgr.table_record_size(&tab_name).await.unwrap_or(0);
            // 只返回文件表
            if meta.is_persistence() && size > 0 {
                result.push((tab_name.clone(), meta, size));
            }
        }
    }
    Ok(result)
}

async fn create_table(
    db_mgr: KVDBManager<usize, CommitLogger>,
    tabs: &Vec<(Atom, KVTableMeta, usize)>,
    to_tab_type: KVDBTableType,
) -> Result<(), String> {
    let tr = db_mgr
        .transaction(Atom::from("create_table"), true, 1000, 1000)
        .ok_or_else(|| "create transaction error")?;
    for (tab_name, meta, size) in tabs {
        if size.clone() > 0 {
            let btree_meta = KVTableMeta::new(
                to_tab_type.clone(),
                meta.is_persistence(),
                meta.key_type().clone(),
                meta.value_type().clone(),
            );
            tr.create_table(Atom::from(tab_name.clone()), btree_meta)
                .await
                .or_else(|e| Err(e.to_string()))?;
        }
    }
    match tr.prepare_modified().await {
        Err(e) => {
            let _ = tr.rollback_modified().await;
            return Err(format!("{:?}", e));
        }
        Ok(output) => match tr.commit_modified(output).await {
            Err(e) => {
                let _ = tr.rollback_modified().await;
                return Err(format!("{:?}", e));
            }
            Ok(()) => {}
        },
    }

    Ok(())
}

async fn batch(
    src_db_mgr: KVDBManager<usize, CommitLogger>,
    out_db_mgr: KVDBManager<usize, CommitLogger>,
    tabs: &Vec<(Atom, KVTableMeta, usize)>,
    to_tab_type: KVDBTableType,
    batch_count: usize,
) -> Result<(), String> {
    let src_tr = src_db_mgr
        .transaction(Atom::from("src_batch"), false, 1000, 1000)
        .ok_or_else(|| "create transaction error")?;

    for (tab_name, meta, size) in tabs {
        let mut src_values = src_tr
            .values(tab_name.clone(), None, false)
            .await
            .ok_or_else(|| "src values error".to_string())?;

        let mut upsert_list = None;
        let mut count = 0;
        while let Some((key, value)) = src_values.next().await {
            count += 1;
            if upsert_list.is_none() {
                upsert_list = Some(Vec::with_capacity(batch_count));
            }
            // println!("batch key:{:?}, value:{:?}", key, value);
            upsert_list.as_mut().unwrap().push(TableKV {
                table: tab_name.clone(),
                key: key.clone(),
                value: Some(value.clone()),
            });
            if upsert_list.as_ref().unwrap().len() >= batch_count {
                write_data(out_db_mgr.clone(), upsert_list.take().unwrap()).await?;
            }
        }
        // 写入剩余部分
        if upsert_list.as_ref().unwrap().len() > 0 {
            write_data(out_db_mgr.clone(), upsert_list.take().unwrap()).await?;
        }
        println!("tab_name:{tab_name} ok count:{count}");
    }

    let output = src_tr.prepare_modified().await.unwrap();
    let _ = src_tr.commit_modified(output).await;
    Ok(())
}

async fn check_table_size(
    out_db_mgr: KVDBManager<usize, CommitLogger>,
    tabs: &Vec<(Atom, KVTableMeta, usize)>,
) -> Result<(), String> {
    for (tab_name, _, size) in tabs {
        if size.clone() > 0 {
            let out_size = out_db_mgr.table_record_size(tab_name).await.unwrap();
            if size.clone() != out_size {
                return Err(format!(
                    "check table {tab_name} record size error {size} {out_size}"
                ));
            }
        }
    }
    Ok(())
}

async fn write_data(
    out_db_mgr: KVDBManager<usize, CommitLogger>,
    data: Vec<TableKV>,
) -> Result<(), String> {
    // let len = data.len();
    /// 创建目标数据库的写事务
    let out_tr = out_db_mgr
        .transaction(Atom::from("write_data"), true, 1000000, 1000000)
        .ok_or_else(|| "create transaction error")?;
    out_tr.upsert(data).await;
    /// 目标数据库的事务提交
    match out_tr.prepare_modified().await {
        Err(e) => {
            let _ = out_tr.rollback_modified().await;
            return Err(format!("{:?}", e));
        }
        Ok(output) => match out_tr.commit_modified(output).await {
            Err(e) => {
                let _ = out_tr.rollback_modified().await;
                return Err(format!("{:?}", e));
            }
            Ok(()) => {}
        },
    }
    // println!("write_data:{len}");
    Ok(())
}
