//! 根提交日志与单个日志表的同步拉取式诊断读取器。
//!
//! inspector 在 `MultiTaskRuntime` 上异步 replay，但 `new`/`next` 通过有界通道同步等待；它们
//! 是运维与离线诊断工具，不应放在数据库事务热路径或不能阻塞的 runtime owner 线程中。
//! 解析器信任日志由当前 `pi_db` 版本生成，不能把任意不可信字节作为输入。

use std::fmt::Debug;
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::io::{Error, Result, ErrorKind};
use std::sync::{Arc,
                atomic::{AtomicIsize, Ordering}};

use crossbeam_channel::{Sender, Receiver, bounded};

use pi_atom::Atom;
use pi_async_rt::rt::{AsyncRuntime, multi_thread::MultiTaskRuntime};
use pi_async_transaction::AsyncCommitLog;
use pi_store::{commit_logger::{CommitLoggerExt, CommitLogger},
               log_store::log_file::{PairLoader, LogMethod, LogFile}};
use pi_guid::Guid;

use crate::{KVTableMeta,
            db::{DEFAULT_DB_TABLES_META_DIR, binary_to_table},
            tables::{KVTable,
                     meta_table::MetaTable}};

/// 逐项检查根提交日志中的事务表动作。
///
/// [`Self::begin`] 与 [`Self::next`] 组成单消费者 pull 协议；
/// [`Self::begin_with_callback`] 是互斥的 callback 协议。实例用原子状态禁止两种协议同时
///启动，但不保证多个线程并发调用 `next` 时响应归属稳定，因此每次检查只应有一个消费者。
/// inspector 不修改 WAL 的确认、`.bak`、repair 或数据库数据状态。
pub struct CommitLogInspector {
    rt:                 MultiTaskRuntime<()>,                                           //运行时
    logger:             CommitLogger,                                                   //提交日志
    status:             Arc<AtomicIsize>,                                               //侦听状态
    request_sender:     Sender<()>,                                                     //请求发送器
    request_receiver:   Receiver<()>,                                                   //请求接收器
    response_sender:    Sender<Option<(Guid, Guid, Atom, bool, Vec<u8>, Vec<u8>)>>,     //响应发送器
    response_receiver:  Receiver<Option<(Guid, Guid, Atom, bool, Vec<u8>, Vec<u8>)>>,   //响应接收器
}

// SAFETY: runtime、CommitLogger、channel 和 AtomicIsize 均提供跨线程同步；外层没有裸指针或
// 可变引用。业务层仍须遵守单消费者 `next` 协议。
unsafe impl Send for CommitLogInspector {}
// SAFETY: `&self` 访问只经线程安全句柄和原子状态；并发调用是否有稳定响应归属是协议问题，
// 不会形成内存数据竞争。
unsafe impl Sync for CommitLogInspector {}

impl CommitLogInspector {
    /// 构造一个尚未开始 replay 的根提交日志 inspector。
    ///
    /// O(1)，只 clone logger/runtime 并创建容量为 1 的请求/响应通道，不读取文件。logger 和
    /// runtime 必须在后续整个检查期间保持可用；共享句柄由本实例持有。
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

    /// 启动 pull 模式 replay。
    ///
    /// 返回 `false` 表示本实例已有进行中的 pull 或 callback replay；返回 `true` 仅表示状态已
    /// 切换且任务已提交，不表示 WAL 已成功打开、解析或读取。每个动作都会等待一次
    /// [`Self::next`] 请求，消费者停止调用会让 replay 任务停在通道等待处。底层 replay 错误
    /// 当前不会通过本 API 返回；截断或格式不匹配的 prepare buffer 可能 panic。
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
                            let _ = response_sender.send(Some((transaciton_uid.clone(),
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
                            let _ = response_sender.send(Some((transaciton_uid.clone(),
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
                            let _ = response_sender.send(Some((transaciton_uid.clone(),
                                                               commit_uid.clone(),
                                                               write.table,
                                                               true,
                                                               write.key.as_ref().to_vec(),
                                                               write.value.unwrap().as_ref().to_vec())));
                        } else {
                            //无值，则执行删除操作
                            pause(&request_receiver)?;

                            let _ = response_sender.send(Some((transaciton_uid.clone(),
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
        let _ = self.rt.spawn(async move {
            let _ = logger.start_replay(Arc::new(inspect_callback)).await;

            //侦听已结束
            let _ = status.compare_exchange(1,
                                            0,
                                            Ordering::Acquire,
                                            Ordering::Relaxed);
            match request_receiver.recv() {
                Err(e) => {
                    panic!("Inspect next failed, reason: {:?}", e);
                },
                Ok(_) => {
                    //响应侦听已结束
                    let _ = response_sender.send(None);
                },
            }
        });

        true
    }

    /// 同步请求并返回下一条根提交日志动作。
    ///
    /// tuple 依次为 `(tid, cid, table, is_upsert, key, value)`；两个 ID 以十进制字符串返回。
    /// `is_upsert = false` 表示 delete，此时 `value` 是当前诊断格式使用的 `[0]` 占位，不是
    /// 被删除的旧值。Meta 的 Key/Value 会转换为表名和调试形式的表元数据。
    ///
    /// 未开始、replay 已结束或通道失败时返回 `None`；无法从 `None` 区分这些原因。活动期会
    /// 阻塞当前 OS 线程直到 runtime 产生响应，只允许单消费者按顺序调用。
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

    /// 启动 callback 模式 replay。
    ///
    /// callback 在 runtime replay 任务中同步执行；每条记录依次收到
    /// `(tid, cid, table, method, timestamp, key, value)`，结束时收到一次 `None`。长时间阻塞、
    /// panic 或重入 callback 会直接影响 replay 任务，调用方必须自行把重工作转交其它执行器。
    /// 返回 `false` 表示已有检查正在进行；`true` 只表示任务已提交，底层 replay 错误当前不
    /// 向调用方回传。本模式不与 [`Self::next`] 联用。
    pub fn begin_with_callback(&self,
                               callback: impl Fn(Option<(Guid, Guid, String, LogMethod, u64, Vec<u8>, Vec<u8>)>) + Send + Sync + 'static)
        -> bool
    {
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

        let inspect_callback = move |response: Option<(Guid, LogMethod, u64, Vec<u8>)>| -> Result<()>
            {
                let (commit_uid,
                    method,
                    time,
                    prepare_output) = if let Some(response) = response
                {
                    response
                } else {
                    //侦听已完成
                    callback(None);
                    return Ok(());
                };

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
                                let table_name = match binary_to_table(&write.key) {
                                    Err(e) => {
                                        //反序列化表名失败
                                        return Err(Error::new(ErrorKind::Other,
                                                              format!("From binary to table name failed, reason: {:?}",
                                                                      e)));
                                    },
                                    Ok(table_name) => {
                                        //反序列化表名成功
                                        table_name
                                    }
                                };
                                let table_meta = KVTableMeta::from(value);

                                //回调元信息表的插入日志
                                let response = Some((transaciton_uid.clone(),
                                                     commit_uid.clone(),
                                                     meta_table_name.as_str().to_string(),
                                                     method,
                                                     time,
                                                     table_name.as_str().as_bytes().to_vec(),
                                                     format!("{:?}", table_meta).as_bytes().to_vec()));
                                callback(response);
                            } else {
                                //无值，则删除表
                                let table_name = Atom::from(write.key.as_ref());

                                //回调元信息表的删除日志
                                let response = Some((transaciton_uid.clone(),
                                                     commit_uid.clone(),
                                                     meta_table_name.as_str().to_string(),
                                                     method,
                                                     time,
                                                     table_name.as_str().as_bytes().to_vec(),
                                                     vec![0]));
                                callback(response);
                            }
                        }
                    } else {
                        //未确认的提交日志操作的表是其它表
                        for write in writes {
                            if write.exist_value() {
                                //有值则回调用户表的插入日志
                                let response = Some((transaciton_uid.clone(),
                                                     commit_uid.clone(),
                                                     write.table.as_str().to_string(),
                                                     method,
                                                     time,
                                                     write.key.as_ref().to_vec(),
                                                     write.value.unwrap().as_ref().to_vec()));
                                callback(response);
                            } else {
                                //无值，则回调用户表的删除日志
                                let response = Some((transaciton_uid.clone(),
                                                     commit_uid.clone(),
                                                     write.table.as_str().to_string(),
                                                     method,
                                                     time,
                                                     write.key.as_ref().to_vec(),
                                                     vec![0]));
                                callback(response);
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
        let _ = self.rt.spawn(async move {
            let _ = logger.start_replay_ext(Arc::new(inspect_callback)).await;

            //侦听已结束
            let _ = status.compare_exchange(1,
                                            0,
                                            Ordering::Acquire,
                                            Ordering::Relaxed);
        });

        true
    }
}

/// 逐项检查单个 LogOrdered/日志格式表目录的 pull 式读取器。
///
/// 该工具读取日志记录而不修改表，不建立事务快照，也不提供数据库查询语义。调用方必须按
/// `new -> begin -> next* -> None` 使用，并保持单消费者。
pub struct LogTableInspector {
    rt:                 MultiTaskRuntime<()>,                               //运行时
    log_file:           LogFile,                                            //日志文件
    status:             Arc<AtomicIsize>,                                   //侦听状态
    request_sender:     Sender<()>,                                         //请求发送器
    request_receiver:   Receiver<()>,                                       //请求接收器
    response_sender:    Sender<Option<(String, bool, Vec<u8>, Vec<u8>)>>,   //响应发送器
    response_receiver:  Receiver<Option<(String, bool, Vec<u8>, Vec<u8>)>>, //响应接收器
}

// SAFETY: 所有字段均为线程安全 runtime/LogFile/channel/atomic 句柄；移动 inspector 不移动
// 自引用数据。单消费者是响应归属约束，不是内存安全前提。
unsafe impl Send for LogTableInspector {}
// SAFETY: 共享访问只通过 LogFile、channel 和 AtomicIsize 的同步 API，不暴露内部可变引用。
unsafe impl Sync for LogTableInspector {}

impl LogTableInspector {
    /// 打开指定表目录并构造尚未开始加载的 inspector。
    ///
    /// 文件打开在 `rt` 上异步执行，但本函数同步等待有界通道，因此可能阻塞当前 OS 线程。
    /// 打开或通道失败返回 `io::ErrorKind::Other`；成功不表示日志内容已完整校验。不要在无法
    /// 继续调度打开任务的 runtime owner 线程中调用。
    pub fn new<P: AsRef<Path> + Debug + Clone + Send + Sync + 'static>(rt: MultiTaskRuntime<()>,
                                                                       table_path: P) -> Result<Self> {
        let rt_copy = rt.clone();
        let table_path_copy = table_path.clone();
        let (sender, receiver) = bounded(1);
        let _ = rt.spawn(async move {
            match LogFile::open(rt_copy,
                                table_path_copy,
                                2 * 1024 * 1024,
                                512 * 1024 * 1024,
                                None).await {
                Err(e) => {
                    //打开日志文件失败，则立即抛出异常
                    let _ = sender.send(Err(format!("Open log ordered table failed, reason: {:?}", e)));
                },
                Ok(log_file) => {
                    let _ = sender.send(Ok(log_file));
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

    /// 启动表日志 pull 加载。
    ///
    /// 已有加载任务时返回 `false`；否则切换状态、提交异步 load 并返回 `true`。`true` 不代表
    /// load 已成功，底层加载错误会在 runtime 任务中 panic。每条记录都等待一次
    /// [`Self::next`] 请求；停止消费会使任务停在通道等待处。
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
        let _ = self.rt.spawn(async move {
            let mut loader = LogTableLoader {
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
            let _ = status.compare_exchange(1,
                                            0,
                                            Ordering::Acquire,
                                            Ordering::Relaxed);
            match request_receiver.recv() {
                Err(_e) => {
                    let _ = response_sender.send(None);
                },
                Ok(_) => {
                    //响应侦听已结束
                    let _ = response_sender.send(None);
                },
            }
        });

        true
    }

    /// 同步请求并返回下一条表日志记录。
    ///
    /// tuple 为 `(log_file, is_upsert, key, value)`；delete 记录的 `is_upsert` 为 `false`，
    /// `value` 是 `[0]` 占位而非旧值。未开始、已结束或通道错误均返回 `None`，活动期会阻塞
    /// 当前 OS 线程等待 runtime 响应。多个线程不得并发消费同一实例。
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
    request_receiver:   Receiver<()>,                                       //请求接收器
    response_sender:    Sender<Option<(String, bool, Vec<u8>, Vec<u8>)>>,   //响应发送器
}

impl PairLoader for LogTableLoader {
    fn is_require(&self, _log_file: Option<&PathBuf>, _key: &Vec<u8>) -> bool {
        true
    }

    fn load(&mut self,
            log_file: Option<&PathBuf>,
            _method: LogMethod,
            key: Vec<u8>,
            value: Option<Vec<u8>>) {
        if let Some(value) = value {
            //插入或更新指定关键字的值
            if let Err(_) = pause(&self.request_receiver) {
                let _ = self.response_sender.send(None);
            }

            let path = if let Some(path) = log_file {
                path.to_str().unwrap().to_string()
            } else {
                "".to_string()
            };

            //响应日志表的插入日志
            let _ = self.response_sender.send(Some((path,
                                                    true,
                                                    key,
                                                    value)));
        } else {
            if let Err(_) = pause(&self.request_receiver) {
                let _ = self.response_sender.send(None);
            }

            let path = if let Some(path) = log_file {
                path.to_str().unwrap().to_string()
            } else {
                "".to_string()
            };

            //响应日志表的删除日志
            let _ = self.response_sender.send(Some((path,
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
