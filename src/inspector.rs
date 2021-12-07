use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::io::{Error, Result, ErrorKind};
use std::sync::{Arc, atomic::{AtomicIsize, Ordering}};

use crossbeam_channel::{Sender, Receiver, bounded};

use atom::Atom;
use r#async::rt::multi_thread::MultiTaskRuntime;
use async_transaction::AsyncCommitLog;
use pi_store::commit_logger::CommitLogger;
use guid::Guid;

use crate::{KVTableMeta,
            db::{DEFAULT_DB_TABLES_META_DIR, binary_to_table},
            tables::{KVTable,
                     meta_table::MetaTable}};

///
/// 提交日志侦听器
///
pub struct CommitLogInspector {
    rt:                 MultiTaskRuntime<()>,                                           //运行时
    logger:             CommitLogger,                                                   //提交日志
    status:             Arc<AtomicIsize>,                                               //侦听状态
    request_sender:     Sender<()>,                                                     //请求发送器
    request_receiver:   Receiver<()>,                                                   //请求接收器
    response_sender:    Sender<Option<(Guid, Guid, Atom, bool, Vec<u8>, Vec<u8>)>>,     //响应发送器
    response_receiver:  Receiver<Option<(Guid, Guid, Atom, bool, Vec<u8>, Vec<u8>)>>,   //响应接收器
}

unsafe impl Send for CommitLogInspector {}
unsafe impl Sync for CommitLogInspector {}

impl CommitLogInspector {
    /// 构建提交日志侦听器
    pub fn new(rt: MultiTaskRuntime<()>, logger: CommitLogger) -> Self {
        let (request_sender, request_receiver) = bounded(1);
        let (response_sender, response_receiver) = bounded(1);

        CommitLogInspector {
            rt,
            logger,
            status: Arc::new(AtomicIsize::new(0)),
            request_sender,
            request_receiver,
            response_sender,
            response_receiver,
        }
    }

    /// 开始侦听
    pub fn begin(&self) -> bool {
        match self.status.compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed) {
            Err(_) => {
                //不允许正在侦听时，开始侦听
                return false;
            },
            Ok(_) => {
                //侦听未开始，则开始侦听
                ()
            },
        }

        let request_receiver = self.request_receiver.clone();
        let response_sender = self.response_sender.clone();

        let inspect_callback = move |commit_uid: Guid, prepare_output: Vec<u8>| -> Result<()> {
            let meta_table_name = Atom::from(DEFAULT_DB_TABLES_META_DIR);
            let bytes_len = prepare_output.len(); //获取日志缓冲区长度
            let mut offset = 0; //日志缓冲区偏移
            let bytes = prepare_output.as_slice();
            let uid = u128::from_le_bytes(bytes[0..16].try_into().unwrap()); //获取事务唯一id
            let transaciton_uid = Guid(uid);
            offset += 16; //移动缓冲区指针

            //迭代日志缓冲区中，本次未确认的提交日志中执行写操作的表和相关键值对
            while offset < bytes_len {
                //获取表名、操作的键值对数量和新的日志缓冲区偏移
                let (table, kvs_len, new_offset) =
                    <MetaTable<usize, CommitLogger> as KVTable>::get_init_table_prepare_output(&prepare_output, offset);

                //获取操作的表键值列表和新的日志缓冲区偏移
                let (writes, new_offset)
                    = <MetaTable<usize, CommitLogger> as KVTable>::get_all_key_value_from_table_prepare_output(&prepare_output, &table, kvs_len, new_offset);

                if table == meta_table_name {
                    //未确认的提交日志操作的表是元信息表
                    for write in writes {
                        if let Some(value) = write.value {
                            //有值，则创建表
                            pause(&request_receiver)?;

                            let table_name = match binary_to_table(&write.key) {
                                Err(e) => {
                                    //反序列化表名失败
                                    return Err(Error::new(ErrorKind::Other, format!("From binary to table name failed, reason: {:?}", e)));
                                },
                                Ok(table_name) => {
                                    //反序列化表名成功
                                    table_name
                                }
                            };
                            let table_meta = KVTableMeta::from(value);

                            //响应元信息表的插入日志
                            response_sender.send(Some((transaciton_uid.clone(),
                                                  commit_uid.clone(),
                                                  meta_table_name.clone(),
                                                  true, table_name.as_str().as_bytes().to_vec(),
                                                  format!("{:?}", table_meta).as_bytes().to_vec())));
                        } else {
                            //无值，则删除表
                            pause(&request_receiver)?;

                            let table_name = Atom::from(write.key.as_ref());

                            //响应元信息表的删除日志
                            response_sender.send(Some((transaciton_uid.clone(),
                                                  commit_uid.clone(),
                                                  meta_table_name.clone(),
                                                  false,
                                                  table_name.as_str().as_bytes().to_vec(),
                                                  vec![0])));
                        }
                    }
                } else {
                    //未确认的提交日志操作的表是其它表
                    for write in writes {
                        if write.exist_value() {
                            //有值，则执行插入或更新操作
                            pause(&request_receiver)?;

                            //响应用户表的插入日志
                            response_sender.send(Some((transaciton_uid.clone(),
                                                  commit_uid.clone(),
                                                  write.table,
                                                  true,
                                                  write.key.as_ref().to_vec(),
                                                  write.value.unwrap().as_ref().to_vec())));
                        } else {
                            //无值，则执行删除操作
                            pause(&request_receiver)?;

                            response_sender.send(Some((transaciton_uid.clone(),
                                                  commit_uid.clone(),
                                                  write.table,
                                                  false,
                                                  write.key.as_ref().to_vec(),
                                                  write.value.unwrap().as_ref().to_vec())));
                        }
                    }
                }

                //更新日志缓冲区偏移
                offset = new_offset;
            }

            Ok(())
        };

        let logger = self.logger.clone();
        let status = self.status.clone();
        let request_receiver = self.request_receiver.clone();
        let response_sender = self.response_sender.clone();
        self.rt.spawn(self.rt.alloc(), async move {
            logger.start_replay(Arc::new(inspect_callback)).await;

            //侦听已结束
            status.compare_exchange(1,
                                    0,
                                    Ordering::Acquire,
                                    Ordering::Relaxed);
            match request_receiver.recv() {
                Err(e) => {
                    panic!("Inspect next failed, reason: {:?}", e);
                },
                Ok(_) => {
                    //响应侦听已结束
                    response_sender.send(None);
                },
            }
        });

        true
    }

    //继续侦听下一条提交日志，并返回提交日志的信息，如果侦听结束，则返回None
    pub fn next(&self) -> Option<(String, String, String, bool, Vec<u8>, Vec<u8>)> {
        if self.status.load(Ordering::Relaxed) == 0 {
            //侦听已完成，则立即返回侦听结束
            return None;
        }

        if self.request_sender.send(()).is_err() {
            //侦听请求错误，则立即返回侦听结束
            return None;
        }

        //侦听请求成功，则等待侦听响应
        match self.response_receiver.recv() {
            Err(_) => {
                //侦听响应错误，则立即返回侦听结束
                None
            },
            Ok(result) => {
                //侦听响应成功
                if let Some((tid, cid, table, method, key, value)) = result {
                    Some((tid.0.to_string(), cid.0.to_string(), table.as_str().to_string(), method, key, value))
                } else {
                    None
                }
            },
        }
    }
}

// 暂停侦听，直到收到继续侦听的请求
#[inline]
fn pause(receiver: &Receiver<()>) -> Result<()> {
    match receiver.recv() {
        Err(e) => {
            Err(Error::new(ErrorKind::ConnectionAborted, format!("Inspect next failed, reason: {:?}", e)))
        },
        Ok(_) => {
            //接收到侦听下一个提交日志的请求
            Ok(())
        },
    }
}