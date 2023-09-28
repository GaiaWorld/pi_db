#![feature(fn_traits)]
#![feature(unboxed_closures)]

use std::ops::Deref;
use std::fmt::Debug;
use std::hash::Hash;
use std::cmp::Ordering as CmpOrdering;
use std::convert::TryInto;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

use futures::{future::BoxFuture,
              stream::BoxStream};
use bytes::{Buf, BufMut};
use log::warn;

use pi_bon::{WriteBuffer, ReadBuffer, Encode, Decode, ReadBonErr};
use pi_guid::Guid;
use pi_sinfo::EnumType;
use pi_async_rt::rt::{AsyncRuntime,
                      multi_thread::MultiTaskRuntime};
use pi_async_transaction::{AsyncCommitLog, TransactionError, ErrorLevel};

pub mod db;
pub mod tables;
pub mod inspector;
pub mod utils;

///
/// 二进制数据
///
#[derive(Debug, Clone, Hash)]
pub struct Binary(Arc<Vec<u8>>);

unsafe impl Send for Binary {}

impl AsRef<[u8]> for Binary {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl Deref for Binary {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_slice()
    }
}

impl From<KVTableMeta> for Binary {
    //将键值对表的元信息序列化为二进制数据
    fn from(src: KVTableMeta) -> Self {
        let mut buf = Vec::new();

        //写入键值对表的类型
        buf.put_u8(src.table_type as u8);

        if src.persistence {
            //写入键值对表需要持久化的标记
            buf.put_u8(1);
        } else {
            //写入键值对表不需要持久化的标记
            buf.put_u8(0);
        }

        //写入键值对表的关键字类型
        let mut write_buffer = WriteBuffer::new();
        src.key.encode(&mut write_buffer);
        buf.put_u16_le(write_buffer.len() as u16); //写入关键字类型的长度
        buf.put_slice(write_buffer.get_byte().as_slice());

        //写入键值对表的值类型
        let mut write_buffer = WriteBuffer::new();
        src.value.encode(&mut write_buffer);
        buf.put_u16_le(write_buffer.len() as u16); //写入值类型的长度
        buf.put_slice(write_buffer.get_byte().as_slice());

        Binary::new(buf)
    }
}

impl Ord for Binary {
    fn cmp(&self, other: &Binary) -> CmpOrdering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialOrd for Binary {
    fn partial_cmp(&self, other: &Binary) -> Option<CmpOrdering> {
        ReadBuffer::new(self.0.as_slice(), 0)
            .partial_cmp(&ReadBuffer::new(other.0.as_slice(), 0))
    }
}

impl Eq for Binary {}

impl PartialEq for Binary {
    fn eq(&self, other: &Binary) -> bool {
        match self.partial_cmp(other){
            Some(CmpOrdering::Equal) => true,
            _ => false
        }
    }
}

impl Default for Binary {
    fn default() -> Self {
        Binary(Arc::new(Vec::default()))
    }
}

impl Binary {
    /// 构建指定的二进制数据
    pub fn new(bin: Vec<u8>) -> Self {
        Binary(Arc::new(bin))
    }

    /// 判断两个二进制数据是否相等
    pub fn binary_equal(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.0, &other.0)
    }

    /// 从指定的共享二进制转换为二进制数据
    pub fn from_shared(shared: Arc<Vec<u8>>) -> Self {
        Binary(shared)
    }

    /// 从指定的二进制分片复制为二进制数据
    pub fn from_slice<B: AsRef<[u8]>>(slice: B) -> Self {
        Binary(Arc::new(Vec::from(slice.as_ref())))
    }

    /// 获取二进制数据长度
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// 将二进制数据转换为共享二进制
    pub fn to_shared(&self) -> Arc<Vec<u8>> {
        self.0.clone()
    }
}

///
/// 抽象的键值对操作
///
pub trait KVAction: Send + Sync + 'static {
    type Key: AsRef<[u8]> + Deref<Target = [u8]> + Hash + PartialEq + Eq + PartialOrd + Ord + Clone + Send + 'static;
    type Value: AsRef<[u8]> + Deref<Target = [u8]> + Default + Clone + Send + 'static;
    type Error: Debug + 'static;

    /// 异步查询指定关键字的值，可能会查询到旧值
    fn dirty_query(&self, key: <Self as KVAction>::Key)
        -> BoxFuture<Option<<Self as KVAction>::Value>>;

    /// 异步查询指定关键字的值
    fn query(&self, key: <Self as KVAction>::Key)
        -> BoxFuture<Option<<Self as KVAction>::Value>>;

    /// 异步插入或更新指定关键字的值，插入或更新可能会被覆蓋
    fn dirty_upsert(&self,
                    key: <Self as KVAction>::Key,
                    value: <Self as KVAction>::Value)
        -> BoxFuture<Result<(), <Self as KVAction>::Error>>;

    /// 异步插入或更新指定关键字的值
    fn upsert(&self,
              key: <Self as KVAction>::Key,
              value: <Self as KVAction>::Value)
        -> BoxFuture<Result<(), <Self as KVAction>::Error>>;

    /// 异步删除指定关键值的值，并返回删除值，删除可能会被覆蓋
    fn dirty_delete(&self, key: <Self as KVAction>::Key)
        -> BoxFuture<Result<Option<<Self as KVAction>::Value>, <Self as KVAction>::Error>>;

    /// 异步删除指定关键值的值，并返回删除值
    fn delete(&self, key: <Self as KVAction>::Key)
        -> BoxFuture<Result<Option<<Self as KVAction>::Value>, <Self as KVAction>::Error>>;

    /// 获取从指定关键字开始，从前向后或从后向前的关键字异步流
    fn keys<'a>(&self,
                key: Option<<Self as KVAction>::Key>,
                descending: bool)
        -> BoxStream<'a, <Self as KVAction>::Key>;

    /// 获取从指定关键字开始，从前向后或从后向前的键值对异步流
    fn values<'a>(&self,
                  key: Option<<Self as KVAction>::Key>,
                  descending: bool)
        -> BoxStream<'a, (<Self as KVAction>::Key, <Self as KVAction>::Value)>;

    /// 锁住指定关键字
    fn lock_key(&self, key: <Self as KVAction>::Key)
        -> BoxFuture<Result<(), <Self as KVAction>::Error>>;

    /// 解锁指定关键字
    fn unlock_key(&self, key: <Self as KVAction>::Key)
        -> BoxFuture<Result<(), <Self as KVAction>::Error>>;
}

///
/// 键值对数据库的表类型
///
#[derive(Debug, Clone, PartialEq)]
pub enum KVDBTableType {
    MemOrdTab = 1,  //有序内存表
    LogOrdTab,      //有序日志表
    LogWTab,        //只写日志表
}

impl From<u8> for KVDBTableType {
    fn from(src: u8) -> Self {
        match src {
            1 => KVDBTableType::MemOrdTab,
            2 => KVDBTableType::LogOrdTab,
            3 => KVDBTableType::LogWTab,
            _ => panic!("From u8 to KVDBTableType failed, src: {}, reason: invalid src", src),
        }
    }
}

///
/// 键值对表的元信息
///
#[derive(Debug, Clone, PartialEq)]
pub struct KVTableMeta {
    table_type:     KVDBTableType,  //表类型
    persistence:    bool,           //是否持久化
    key:            EnumType,       //关键字类型
    value:          EnumType,       //值类型
}

impl From<Binary> for KVTableMeta {
    //将二进制数据反序列化为键值对表的元信息
    fn from(src: Binary) -> Self {
        let mut buf = src.as_ref();
        let mut offset = 0;

        //读取键值对表的类型
        let table_type = KVDBTableType::from(buf.get_u8());
        offset += 1;

        let persistence = if buf.get_u8() == 0 {
            //读取键值对表不需要持久化的标记
            false
        } else {
            //读取键值对表需要持久化的标记
            true
        };
        offset += 1;

        //读取键值对表的关键字类型
        let key_len = buf.get_u16_le() as usize;
        offset += 2;
        let mut read_buffer = ReadBuffer::new(&buf[0..key_len], 0);
        buf.advance(key_len); //移动缓冲区指针
        offset += key_len;
        let key = EnumType::decode(&mut read_buffer).unwrap();

        let value_len = buf.get_u16_le() as usize;
        offset += 2;
        let mut read_buffer = ReadBuffer::new(&buf[0..value_len], 0);
        buf.advance(value_len);
        offset += value_len;
        let value = EnumType::decode(&mut read_buffer).unwrap();

        KVTableMeta {
            table_type,
            persistence,
            key,
            value,
        }
    }
}

impl KVTableMeta {
    /// 构建一个键值对表的元信息
    pub fn new(table_type: KVDBTableType,
               persistence: bool,
               key: EnumType,
               value: EnumType) -> Self {
        KVTableMeta {
            table_type,
            persistence,
            key,
            value,
        }
    }

    /// 构建一个兼容旧的元信息的键值对表的元信息
    pub fn with_compatibled(table_type: KVDBTableType,
                            persistence: bool,
                            bin: &[u8]) -> Result<Self, ReadBonErr> {
        let mut buffer = ReadBuffer::new(bin, 0);
        let key = EnumType::decode(&mut buffer)?;
        let value = EnumType::decode(&mut buffer)?;

        Ok(Self::new(table_type, persistence, key, value))
    }

    /// 获取键值对表的类型
    pub fn table_type(&self) -> &KVDBTableType {
        &self.table_type
    }

    /// 判断键值对表是否需要持久化
    pub fn is_persistence(&self) -> bool {
        self.persistence
    }

    /// 获取键值对表的关键字类型
    pub fn key_type(&self) -> &EnumType {
        &self.key
    }

    /// 获取键值对表的值类型
    pub fn value_type(&self) -> &EnumType {
        &self.value
    }
}

///
/// 表事务服务质量
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableTrQos {
    Unsafe = 0, //不保证数据安全
    ThreadSafe, //只保证数据的线程安全
    Safe,       //保证数据安全
}

impl Default for TableTrQos {
    fn default() -> Self {
        TableTrQos::Safe
    }
}

///
/// 键值对操作记录
///
#[derive(Debug, Clone)]
pub enum KVActionLog {
    Read,				        //读操作，读操作记录不允许覆盖写操作记录
    Write(Option<Binary>),	    //写操作，为None则表示删除，否则主键不存在则为插入，主键存在则为更新，写操作记录会覆盖读操作记录
    DirtyWrite(Option<Binary>), //脏写操作，为None则表示删除，否则主键不存在则为插入，主键存在则为更新，脏写操作记录不会覆盖读操作记录
}

impl KVActionLog {
    /// 判断是否是脏写操作记录
    #[inline]
    pub fn is_dirty_writed(&self) -> bool {
        if let KVActionLog::DirtyWrite(_) = self {
            true
        } else {
            false
        }
    }
}

///
/// 键值对数据库事务的提交确认器
///
#[derive(Clone)]
pub struct KVDBCommitConfirm<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<(
    MultiTaskRuntime<()>,   //异步运行时
    Log,                    //提交日志记录器
    Guid,                   //事务唯一id
    Option<Guid>,           //提交唯一id，只有需要持久化的事务，才分配提交唯一id
    AtomicUsize,            //事务提交确认的计数
)>);

unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for KVDBCommitConfirm<C, Log> {}
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for KVDBCommitConfirm<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> FnOnce<(Guid, Guid, Result<(), KVTableTrError>)> for KVDBCommitConfirm<C, Log> {
    type Output = Result<(), KVTableTrError>;

    extern "rust-call" fn call_once(self, args: (Guid, Guid, Result<(), KVTableTrError>))
                                    -> Self::Output {
        self.call(args)
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> FnMut<(Guid, Guid, Result<(), KVTableTrError>)> for KVDBCommitConfirm<C, Log> {
    extern "rust-call" fn call_mut(&mut self, args: (Guid, Guid, Result<(), KVTableTrError>))
                                   -> Self::Output {
        self.call(args)
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Fn<(Guid, Guid, Result<(), KVTableTrError>)> for KVDBCommitConfirm<C, Log> {
    extern "rust-call" fn call(&self, args: (Guid, Guid, Result<(), KVTableTrError>))
                               -> Self::Output {
        if let Err(e) = args.2 {
            //键值对数据库事务的子事务的异步提交错误
            if let ErrorLevel::Fatal = &e.level {
                //忽略提交的严重错误
                return Err(e);
            }
        }

        self.confirm_commited(args.0, args.1)
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVDBCommitConfirm<C, Log> {
    /// 构建一个键值对数据库事务的提交确认器
    pub fn new(rt: MultiTaskRuntime<()>,
               commit_logger: Log,
               tid: Guid,
               cid: Option<Guid>,
               count: usize) -> Self {
        KVDBCommitConfirm(Arc::new((
            rt,
            commit_logger,
            tid,
            cid,
            AtomicUsize::new(count),
        )))
    }

    // 确认一个键值对数据库事务的子事务的异步提交已完成
    #[inline(always)]
    fn confirm_commited(&self, tid: Guid, cid: Guid) -> Result<(), KVTableTrError> {
        if (self.0).2 != tid || (self.0).3.clone().unwrap() != cid {
            //提交确认的事务唯一id或提交唯一id与待确主人的唯一id不匹配，则立即返回错误原因
            return Err(KVTableTrError::new_transaction_error(ErrorLevel::Normal,
                                                             format!("Confirm commited failed, require_transaction_uid: {:?}, require_commit_uid: {:?}, transaction_uid: {:?}, commit_uid: {:?}, reason: invalid transaction_uid or commit_uid", (self.0).2, (self.0).3, tid, cid)));
        }

        //对子事务的确认提交计数
        if (self.0).4.fetch_sub(1, Ordering::SeqCst) <= 1 {
            //本次事务的所有子事务已确认提交，则异步的确认本次事务已提交，并立即返回成功
            let confirmer = self.clone();
            let _ = (self.0).0.spawn(async move {
                let last = COMMITED_LEN.fetch_add(1, Ordering::Relaxed);
                println!("!!!!!!last commited len: {}", last);
                //事务已确认提交
                if let Err(e) = (confirmer.0)
                    .1
                    .confirm(cid.clone())
                    .await {
                    //提交日志的确认错误
                    warn!("Confirm commit log failed, transaction_uid: {:?}, commit_uid: {:?}, reason: {:?}", tid, cid, e);
                }
            });
        }

        Ok(())
    }
}

static COMMITED_LEN: AtomicUsize = AtomicUsize::new(0);

///
/// 键值对数据库事务错误
///
#[derive(Debug)]
pub struct KVTableTrError {
    level:  ErrorLevel, //事务错误级别
    reason: String,     //事务错误原因
}

unsafe impl Send for KVTableTrError {}
unsafe impl Sync for KVTableTrError {}

impl TransactionError for KVTableTrError {
    fn new_transaction_error<E>(level: ErrorLevel, reason: E) -> Self
        where E: Debug + Sized + 'static {
        KVTableTrError {
            level,
            reason: format!("Table transaction error, reason: {:?}", reason),
        }
    }
}

impl KVTableTrError {
    /// 获取错误等级
    pub fn level(&self) -> ErrorLevel {
        self.level.clone()
    }
}