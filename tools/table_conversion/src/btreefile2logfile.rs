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

// 定义一个函数，用于将输入的字符串转换为输出字符串，并按照指定的批次数量进行处理
pub fn conversion(src: String, out: String, batch_count: usize) -> Result<(), String> {
    // 创建一个多任务运行时构建器
    let builder = MultiTaskRuntimeBuilder::default();
    // 构建运行时
    let rt = builder.build();
    // 创建一个容量为1的有界通道
    let (s, r) = bounded(1);
    // 在运行时中异步执行handle函数，并将结果发送到通道中
    rt.spawn(async move {
        let r = handle(src, out, batch_count).await;
        s.send(r);
    });
    // 从通道中接收结果，如果接收失败，则返回错误信息
    r.recv().or_else(|e| Err(e.to_string()))?
}

/// 处理函数，用于处理数据库转换
async fn handle(src: String, out: String, batch_count: usize) -> Result<(), String> {
    // 创建一个有缓冲区的通道，用于发送和接收信号
    let (s, r) = bounded(1);
    // 初始化计数器
    let mut count = 0;
    // 定义一个闭包，用于处理数据库事件
    let listener = move |db_mgr: &KVDBManager<usize, CommitLogger>,
                         tr_mgr: &Transaction2PcManager<usize, CommitLogger>,
                         events: &mut Vec<KVDBEvent<Guid>>| {
        // 计数器增加事件数量
        count += events.len();
        // 清空事件列表
        events.clear();
        // 打印事务总数、已消费事务总数和当前活动事务数量
        println!(
            "!!!!!!> start total: {:?}, end total: {:?}, active: {:?}, count:{count}",
            tr_mgr.produced_transaction_total(),
            tr_mgr.consumed_transaction_total(),
            tr_mgr.transaction_len()
        );
        // 如果已生产的事务总数等于已消费的事务总数，且当前活动事务数量为0，且计数器等于已消费的事务总数，则发送信号
        if tr_mgr.produced_transaction_total() == tr_mgr.consumed_transaction_total()
            && tr_mgr.transaction_len() == 0
            && count == tr_mgr.consumed_transaction_total()
        {
            s.send(true);
        }
    };
    // 启动源数据库
    let src_db_mgr = start_db::<
        fn(
            &KVDBManager<usize, CommitLogger>,
            &Transaction2PcManager<usize, CommitLogger>,
            &mut Vec<KVDBEvent<Guid>>,
        ),
    >(src, None)
    .await?;
    // 启动目标数据库，并传入监听器
    let out_db_mgr = start_db(out, Some(listener)).await?;
    // 获取源数据库中的表
    let tables = get_tables(src_db_mgr.clone()).await?;
    /// 创建表
    create_table(out_db_mgr.clone(), &tables, KVDBTableType::LogOrdTab).await?;
    // println!("out_db_mgr tables:{:?}", out_db_mgr.tables().await);
    /// 批量转换表数据
    batch(
        src_db_mgr.clone(),
        out_db_mgr.clone(),
        &tables,
        KVDBTableType::LogOrdTab,
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

// 异步函数，用于启动数据库
async fn start_db<F>(
    // 数据库路径
    path: String,
    // 监听器
    listener: Option<F>,
) -> Result<KVDBManager<usize, CommitLogger>, String>
where
    F: FnMut(
            // 数据库管理器
            &KVDBManager<usize, CommitLogger>,
            // 2PC事务管理器
            &Transaction2PcManager<usize, CommitLogger>,
            // 事件向量
            &mut Vec<KVDBEvent<Guid>>,
        ) + Send
        + Sync
        + 'static,
{
    // 创建多任务运行时构建器
    let builder = MultiTaskRuntimeBuilder::default();
    // 构建运行时
    let rt = builder.build();
    // 克隆运行时
    let rt_copy = rt.clone();
    // 创建异步值
    let value = AsyncValue::new();
    // 克隆异步值
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
        match builder.startup_with_listener(true, listener).await {
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

// 异步函数，用于获取数据库中的表
async fn get_tables(
    // KVDBManager类型的参数，用于管理数据库
    db_mgr: KVDBManager<usize, CommitLogger>,
) -> Result<Vec<(Atom, KVTableMeta, usize)>, String> {
    // 定义一个空的结果向量
    let mut result = vec![];
    // 获取数据库中的所有表
    let tabs = db_mgr.tables().await;
    // 开启一个事务
    let tr = db_mgr
        .transaction(Atom::from("get_tables"), false, 1000, 1000)
        .ok_or_else(|| "transaction error".to_string())?;
    // 遍历所有表
    for tab_name in tabs {
        // 获取表的元数据
        if let Some(meta) = tr.table_meta(tab_name.clone()).await {
            // 获取表的记录大小
            let size = db_mgr.table_record_size(&tab_name).await.unwrap_or(0);
            // 只返回文件表
            if meta.is_persistence() && size > 0 {
                result.push((tab_name.clone(), meta, size));
            }
        }
    }
    // 返回结果
    Ok(result)
}

// 异步函数，用于创建表
async fn create_table(
    // KVDBManager实例，用于管理数据库
    db_mgr: KVDBManager<usize, CommitLogger>,
    // 表的元数据，包括表名、表类型和大小
    tabs: &Vec<(Atom, KVTableMeta, usize)>,
    // 表的类型
    to_tab_type: KVDBTableType,
) -> Result<(), String> {
    // 创建一个事务
    let tr = db_mgr
        .transaction(Atom::from("create_table"), true, 1000, 1000)
        .ok_or_else(|| "create transaction error")?;
    // 遍历表元数据
    for (tab_name, meta, size) in tabs {
        // 如果表大小大于0
        if size.clone() > 0 {
            // 创建B树元数据
            let btree_meta = KVTableMeta::new(
                to_tab_type.clone(),
                meta.is_persistence(),
                meta.key_type().clone(),
                meta.value_type().clone(),
            );
            // 创建表
            tr.create_table(Atom::from(tab_name.clone()), btree_meta, true)
                .await
                .or_else(|e| Err(e.to_string()))?;
        }
    }
    // 准备修改
    match tr.prepare_modified().await {
        // 如果出错，回滚修改
        Err(e) => {
            let _ = tr.rollback_modified().await;
            return Err(format!("{:?}", e));
        }
        // 如果成功，提交修改
        Ok(output) => match tr.commit_modified(output).await {
            Err(e) => {
                let _ = tr.rollback_modified().await;
                return Err(format!("{:?}", e));
            }
            Ok(()) => {}
        },
    }

    // 返回成功
    Ok(())
}

// 异步函数batch，用于批量处理数据
async fn batch(
    // 源数据库管理器
    src_db_mgr: KVDBManager<usize, CommitLogger>,
    // 输出数据库管理器
    out_db_mgr: KVDBManager<usize, CommitLogger>,
    // 表列表
    tabs: &Vec<(Atom, KVTableMeta, usize)>,
    // 表类型
    to_tab_type: KVDBTableType,
    // 批量数量
    batch_count: usize,
) -> Result<(), String> {
    // 创建源数据库事务
    let src_tr = src_db_mgr
        .transaction(Atom::from("src_batch"), false, 1000, 1000)
        .ok_or_else(|| "create transaction error")?;

    // 遍历表列表
    for (tab_name, meta, size) in tabs {
        // 获取源数据库中的值
        let mut src_values = src_tr
            .values(tab_name.clone(), None, false)
            .await
            .ok_or_else(|| "src values error".to_string())?;

        // 初始化upsert列表和计数器
        let mut upsert_list = None;
        let mut count = 0;
        // 遍历源数据库中的值
        while let Some((key, value)) = src_values.next().await {
            count += 1;
            // 如果upsert列表为空，则初始化
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

// 异步函数，用于检查表的大小
async fn check_table_size(
    // 输出数据库管理器
    out_db_mgr: KVDBManager<usize, CommitLogger>,
    // 表的元数据
    tabs: &Vec<(Atom, KVTableMeta, usize)>,
) -> Result<(), String> {
    // 遍历表
    for (tab_name, _, size) in tabs {
        // 如果表的大小大于0
        if size.clone() > 0 {
            // 获取输出数据库中表的大小
            let out_size = out_db_mgr.table_record_size(tab_name).await.unwrap();
            // 如果表的大小不相等
            if size.clone() != out_size {
                // 返回错误信息
                return Err(format!(
                    "check table {tab_name} record size error {size} {out_size}"
                ));
            }
        }
    }
    // 返回成功
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
