use std::fmt::Debug;
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::io::{Error, Result, ErrorKind};
use std::collections::hash_map::Entry as HashMapEntry;
use std::sync::{Arc, atomic::{AtomicIsize, Ordering}};

use crossbeam_channel::{Sender, Receiver, bounded};

use atom::Atom;
use r#async::rt::multi_thread::MultiTaskRuntime;
use async_transaction::AsyncCommitLog;
use pi_store::{commit_logger::CommitLogger,
               log_store::log_file::{PairLoader, LogMethod, LogFile}};
use guid::Guid;
use hash::XHashMap;

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
                                                  true,
                                                  table_name.as_str().as_bytes().to_vec(),
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
                                                  vec![0])));
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

///
/// 日志表侦听器
///
pub struct LogTableInspector {
    rt:                 MultiTaskRuntime<()>,                               //运行时
    log_file:           LogFile,                                            //日志文件
    status:             Arc<AtomicIsize>,                                   //侦听状态
    request_sender:     Sender<()>,                                         //请求发送器
    request_receiver:   Receiver<()>,                                       //请求接收器
    response_sender:    Sender<Option<(String, bool, Vec<u8>, Vec<u8>)>>,   //响应发送器
    response_receiver:  Receiver<Option<(String, bool, Vec<u8>, Vec<u8>)>>, //响应接收器
}

unsafe impl Send for LogTableInspector {}
unsafe impl Sync for LogTableInspector {}

impl LogTableInspector {
    /// 构建日志表侦听器
    pub fn new<P: AsRef<Path> + Debug + Clone + Send + Sync + 'static>(rt: MultiTaskRuntime<()>,
                                                                       table_path: P) -> Result<Self> {
        let rt_copy = rt.clone();
        let table_path_copy = table_path.clone();
        let (sender, receiver) = bounded(1);
        rt.spawn(rt.alloc(), async move {
            match LogFile::open(rt_copy,
                                table_path_copy,
                                2 * 1024 * 1024,
                                512 * 1024 * 1024,
                                None).await {
                Err(e) => {
                    //打开日志文件失败，则立即抛出异常
                    sender.send(Err(format!("Open log ordered table failed, reason: {:?}", e)));
                },
                Ok(log_file) => {
                    sender.send(Ok(log_file));
                }
            }
        });

        let log_file = match receiver.recv() {
            Err(e) => {
                return Err(Error::new(ErrorKind::Other, format!("Create LogTableInspector failed, path: {:?}, reason: {:?}", e, table_path)));
            },
            Ok(result) => {
                match result {
                    Err(e) => {
                        return Err(Error::new(ErrorKind::Other, format!("Create LogTableInspector failed, path: {:?}, reason: {:?}", e, table_path)));
                    },
                    Ok(log_file) => {
                        log_file
                    }
                }
            }
        };

        let (request_sender, request_receiver) = bounded(1);
        let (response_sender, response_receiver) = bounded(1);

        Ok(LogTableInspector {
            rt,
            log_file,
            status: Arc::new(AtomicIsize::new(0)),
            request_sender,
            request_receiver,
            response_sender,
            response_receiver,
        })
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

        let log_file = self.log_file.clone();
        let status = self.status.clone();
        let request_receiver = self.request_receiver.clone();
        let response_sender = self.response_sender.clone();
        self.rt.spawn(self.rt.alloc(), async move {
            let mut loader = LogTableLoader {
                statistics: XHashMap::default(),
                removed: XHashMap::default(),
                inserted: XHashMap::default(),
                request_receiver: request_receiver.clone(),
                response_sender: response_sender.clone(),
            };

            if let Err(e) = log_file.load(&mut loader,
                                          None,
                                          8192,
                                          true).await {
                //加载指定的日志文件失败，则立即抛出异常
                panic!("Load log ordered table failed, path: {:?}, reason: {:?}",
                       log_file.path(),
                       e);
            }

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

    //继续侦听下一条日志，并返回日志的信息，如果侦听结束，则返回None
    pub fn next(&self) -> Option<(String, bool, Vec<u8>, Vec<u8>)> {
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
                if let Some((file, method, key, value)) = result {
                    Some((file.as_str().to_string(), method, key, value))
                } else {
                    None
                }
            },
        }
    }
}

// 日志表的加载器
struct LogTableLoader {
    statistics:         XHashMap<PathBuf, (u64, u64)>,                      //加载统计信息，包括关键字数量和键值对的字节数
    removed:            XHashMap<Vec<u8>, ()>,                              //已删除关键字表
    inserted:           XHashMap<Vec<u8>, ()>,                              //已插入关键字表
    request_receiver:   Receiver<()>,                                       //请求接收器
    response_sender:    Sender<Option<(String, bool, Vec<u8>, Vec<u8>)>>,   //响应发送器
}

impl PairLoader for LogTableLoader {
    fn is_require(&self, _log_file: Option<&PathBuf>, key: &Vec<u8>) -> bool {
        //不在已删除关键字表中且不在已插入关键字表中的关键字，才允许被加载
        !self
            .removed
            .contains_key(key)
            &&
            !self
                .inserted
                .contains_key(key)
    }

    fn load(&mut self,
            log_file: Option<&PathBuf>,
            _method: LogMethod,
            key: Vec<u8>,
            value: Option<Vec<u8>>) {
        if let Some(value) = value {
            //插入或更新指定关键字的值
            if let Err(_) = pause(&self.request_receiver) {
                self.response_sender.send(None);
            }

            let path = if let Some(path) = log_file {
                match self.statistics.entry(path.clone()) {
                    HashMapEntry::Occupied(mut o) => {
                        //指定日志文件的统计信息存在，则继续统计
                        let statistics = o.get_mut();
                        statistics.0 += 1;
                        statistics.1 += (key.len() + value.len()) as u64;
                    },
                    HashMapEntry::Vacant(v) => {
                        //指定日志文件的统计信息不存在，则初始化统计
                        v.insert((1, (key.len() + value.len()) as u64));
                    },
                }

                path.to_str().unwrap().to_string()
            } else {
                "".to_string()
            };

            //加入已插入关键字表中
            self.inserted.insert(key.clone(), ());

            //响应日志表的插入日志
            self.response_sender.send(Some((path,
                                            true,
                                            key,
                                            value)));
        } else {
            if let Err(_) = pause(&self.request_receiver) {
                self.response_sender.send(None);
            }

            let path = if let Some(path) = log_file {
                path.to_str().unwrap().to_string()
            } else {
                "".to_string()
            };

            //删除指定关键字的值，则不需要加载到有序日志表的内存表中，并记录到已删除关键字表中
            self.removed.insert(key.clone(), ());

            //响应日志表的删除日志
            self.response_sender.send(Some((path,
                                            false,
                                            key,
                                            vec![0])));
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
            //接收到侦听下一个日志的请求
            Ok(())
        },
    }
}