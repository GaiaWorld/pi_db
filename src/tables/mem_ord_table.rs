//! 纯内存 COW 有序表及其 2PC 子事务实现。
//!
//! Memory 表的数据只存在于进程内 `OrdMap`，没有独立数据文件。表元数据中的
//! `persistence=true` 仍然合法：写动作会进入根 WAL，并在内存根发布后立即调用子表确认器；
//! 重启恢复依赖根 WAL replay，而不是加载 Memory 数据文件。`persistence=false` 则不生成该表
//! 的根 WAL 片段。

use std::mem;
use std::sync::Arc;
use std::path::Path;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use futures::{future::{FutureExt, BoxFuture}, stream::{StreamExt, BoxStream}};
use async_stream::stream;

use pi_atom::Atom;
use pi_guid::Guid;
use pi_hash::XHashMap;
use pi_ordmap::{ordmap::OrdMap, asbtree::Tree};
use pi_async_rt::lock::spin_lock::SpinLock;
use pi_async_transaction::{AsyncTransaction,
                           Transaction2Pc,
                           Transaction2PcAllConflicts,
                           UnitTransaction,
                           SequenceTransaction,
                           TransactionTree,
                           TransactionError,
                           AsyncCommitLog,
                           ErrorLevel,
                           manager_2pc::Transaction2PcStatus};
use pi_ordmap::ordmap::ImOrdMap;
use crate::{Binary,
            KVAction,
            TableTrQos,
            KVActionLog,
            KVDBCommitConfirm,
            KVTableTrError,
            TableKey,
            db::{KVDBTransaction, KVDBChildTrList},
            key_version::{KeyVersions,
                          PrepareMode,
                          PreparedActions,
                          TableVersionContext,
                          Version,
                          VersionReceipt,
                          binary_state_equal,
                          has_prepared_conflict},
            tables::{KVTable, ordmap_snapshot::OrdMapSnapshot}};

/// 以 COW `OrdMap` 保存已提交数据的有序 Memory 表共享句柄。
///
/// `root` 是当前逻辑数据，`prepare` 是跨事务的 TID 预留表。clone 只增加内部 `Arc` 引用；
/// `path()` 永远返回 `None`，`ready_collect/collect` 是成功 no-op。表可配置参与根 WAL，但这
/// 不会创建独立表日志或数据文件。
#[derive(Clone)]
pub struct MemoryOrderedTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerMemoryOrderedTable<C, Log>>);

// SAFETY: root/prepare 分别由 Mutex 保护，剩余字段不可变；外层只移动 Arc owner。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for MemoryOrderedTable<C, Log> {}
// SAFETY: 所有共享可变访问均经表级锁；泛型仅由 PhantomData 约束类型关系。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for MemoryOrderedTable<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVTable for MemoryOrderedTable<C, Log> {
    type Name = Atom;
    type Tr = MemOrdTabTr<C, Log>;
    type Error = KVTableTrError;

    fn name(&self) -> <Self as KVTable>::Name {
        self.0.name.clone()
    }

    fn path(&self) -> Option<&Path> {
        None
    }

    #[inline]
    fn is_persistent(&self) -> bool {
        self.0.persistence
    }

    fn is_ordered(&self) -> bool {
        true
    }

    fn len(&self) -> usize {
        self.0.root.lock().size()
    }

    fn size(&self) -> u64 {
        let root_copy = self.0.root.lock().clone();
        root_copy.full_bytes_size()
    }

    fn transaction(&self,
                   source: Atom,
                   is_writable: bool,
                   is_persistent: bool,
                   prepare_timeout: u64,
                   commit_timeout: u64) -> Self::Tr {
        MemOrdTabTr::new(source,
                         is_writable,
                         is_persistent,
                         prepare_timeout,
                         commit_timeout,
                         self.clone())
    }

    fn ready_collect(&self) -> BoxFuture<Result<(), Self::Error>> {
        async move {
            //有序内存表，忽略准备整理
            Ok(())
        }.boxed()
    }

    fn collect(&self) -> BoxFuture<Result<(), Self::Error>> {
        async move {
            //有序内存表，忽略整理
            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> MemoryOrderedTable<C, Log> {
    /// 在不创建事务的前提下读取当前已提交 COW 根；调用方负责 publication 同步。
    pub(crate) fn query_committed(&self, key: &Binary) -> Option<Binary> {
        self.0.root.lock().get(key).cloned()
    }

    /// 构造一个空 Memory 表。
    ///
    /// `is_persistence` 只决定该表写事务是否要求生成根 WAL 片段；无论取值如何都不创建路径、
    /// 表日志、后台任务或数据文件。该低层构造器不注册表名/版本缓存，外部必须通过数据库
    /// DDL 使用，直接构造属于协议外调用域。
    pub fn new(name: Atom,
               is_persistence: bool) -> Self {
        let root = Mutex::new(OrdMap::new(None));
        let prepare = Mutex::new(XHashMap::default());

        let inner = InnerMemoryOrderedTable {
            name,
            persistence: is_persistence,
            root,
            prepare,
            marker: PhantomData,
        };

        MemoryOrderedTable(Arc::new(inner))
    }
}

/// Memory 表的共享状态。
struct InnerMemoryOrderedTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    name:           Atom,                                                   // 逻辑表名。
    persistence:    bool,                                                   // 是否参与根 WAL；不表示存在独立数据文件。
    root:           Mutex<OrdMap<Tree<Binary, Binary>>>,                    // 当前已提交 COW 数据根。
    prepare:        Mutex<XHashMap<Guid, PreparedActions>>,                 // TID -> 已通过检查、尚未 commit 的动作。
    marker:         PhantomData<(C, Log)>,                                  // 保留与根确认器泛型的类型绑定，不持有实例。
}

// SAFETY: 共享可变字段均由 Mutex 保护，PhantomData 不引入运行时内存。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for InnerMemoryOrderedTable<C, Log> {}
// SAFETY: `&self` 无法绕过 root/prepare 锁取得可变引用。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for InnerMemoryOrderedTable<C, Log> {}

/// Memory 表的单元子事务共享句柄。
///
/// `root_ref` 固定创建时基线，`root_mut` 承载事务内 COW 修改，`actions` 保存每 Key 最终动作。
/// prepare 成功后动作所有权转移到表级 `prepare`；commit 在 publication 写门内发布数据与
/// 版本。clone 共享同一事务，不能作为重复 prepare/commit 的新事务使用。
#[derive(Clone)]
pub struct MemOrdTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerMemOrdTabTr<C, Log>>);

/// 冲突集合面向三种 prepare API 的返回投影。
#[derive(Clone, Copy)]
enum PrepareConflictKind {
    /// 普通可恢复错误，不携带结构化 Key 集合。
    Common,
    /// 结构化返回首个冲突。
    First,
    /// 结构化返回全部冲突。
    All,
}

// SAFETY: 内部状态由 SpinLock/AtomicBool 和表级锁同步；外层只移动 Arc。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for MemOrdTabTr<C, Log> {}
// SAFETY: 共享引用不产生无同步可变别名；合法事务调用顺序由外部协议保证。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for MemOrdTabTr<C, Log> {}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> AsyncTransaction for MemOrdTabTr<C, Log> {
    type Output = ();
    type Error = KVTableTrError;

    fn is_writable(&self) -> bool {
        self.0.writable
    }

    fn is_concurrent_commit(&self) -> bool {
        false
    }

    fn is_concurrent_rollback(&self) -> bool {
        false
    }

    fn get_source(&self) -> Atom {
        self.0.source.clone()
    }

    fn init(&self)
            -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        async move {
            Ok(())
        }.boxed()
    }

    fn rollback(&self)
                -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        let tr = self.clone();

        async move {
            // rollback 仅移除 prepared 预留、释放版本 lease；未发布的私有 COW 根随事务释放。
            let transaction_uid = tr.get_transaction_uid().unwrap();
            let _ = tr.0.table.0.prepare.lock().remove(&transaction_uid);
            if let Some(context) = tr.0.version_context.as_ref() {
                context.release_snapshot();
            }

            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Transaction2Pc for MemOrdTabTr<C, Log> {
    type Tid = Guid;
    type Pid = Guid;
    type Cid = Guid;
    type PrepareOutput = Vec<u8>;
    type PrepareError = KVTableTrError;
    type ConfirmOutput = ();
    type ConfirmError = KVTableTrError;
    type CommitConfirm = KVDBCommitConfirm<C, Log>;

    fn is_require_persistence(&self) -> bool {
        self.0.persistence.load(Ordering::Relaxed)
    }

    fn require_persistence(&self) {
        self.0.persistence.store(true, Ordering::Relaxed);
    }

    fn is_concurrent_prepare(&self) -> bool {
        false
    }

    fn is_enable_inherit_uid(&self) -> bool {
        true
    }

    fn get_transaction_uid(&self) -> Option<<Self as Transaction2Pc>::Tid> {
        self.0.tid.lock().clone()
    }

    fn set_transaction_uid(&self, uid: <Self as Transaction2Pc>::Tid) {
        *self.0.tid.lock() = Some(uid);
    }

    fn get_prepare_uid(&self) -> Option<<Self as Transaction2Pc>::Pid> {
        None
    }

    fn set_prepare_uid(&self, _uid: <Self as Transaction2Pc>::Pid) {

    }

    fn get_commit_uid(&self) -> Option<<Self as Transaction2Pc>::Cid> {
        self.0.cid.lock().clone()
    }

    fn set_commit_uid(&self, uid: <Self as Transaction2Pc>::Cid) {
        *self.0.cid.lock() = Some(uid);
    }

    fn get_prepare_timeout(&self) -> u64 {
        self.0.prepare_timeout
    }

    fn get_commit_timeout(&self) -> u64 {
        self.0.commit_timeout
    }

    fn prepare(&self)
               -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        let tr = self.clone();

        async move {
            tr.prepare_registered(PrepareConflictKind::Common).await
        }.boxed()
    }

    fn prepare_conflicts(&self) -> BoxFuture<Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        let tr = self.clone();

        async move {
            tr.prepare_registered(PrepareConflictKind::First).await
        }.boxed()
    }

    fn commit(&self, confirm: <Self as Transaction2Pc>::CommitConfirm)
              -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        let tr = self.clone();

        async move {
            let transaction_uid = tr.get_transaction_uid().unwrap();
            // publication 写门覆盖单表数据根、版本 revision 和回执的完整发布过程。
            // prepare 预留在门内先移除并立即释放锁，因此后续 prepare 无法观察到“预留已删、
            // 数据尚未发布”的中间状态，也不会把同步 prepare 锁带入根节点临界区。
            let publication = match tr.0.version_context.as_ref() {
                Some(context) => Some(context.versions().publication().write().await),
                None => None,
            };
            let prepared = tr.0.table.0.prepare.lock().remove(&transaction_uid);
            let mut committed_versions = Vec::new();

            if let Some(prepared) = prepared {
                let has_writes = prepared.actions.values().any(|action| {
                    matches!(action, KVActionLog::Write(_) | KVActionLog::DirtyWrite(_))
                });
                if has_writes {
                    // 只有真实写动作分配 revision；纯读/空动作不发布版本也不产生回执。
                    let revision = match tr.0.version_context.as_ref() {
                        Some(context) => {
                            match context.versions().checked_next_revision() {
                                Some(revision) => Some(revision),
                                None => {
                                    drop(publication);
                                    context.release_snapshot();
                                    return Err(KVTableTrError::new_transaction_error(
                                        ErrorLevel::Fatal,
                                        format!("Commit memory ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, reason: key version revision exhausted",
                                                tr.0.table.name().as_str(),
                                                tr.0.source,
                                                transaction_uid)));
                                },
                            }
                        },
                        None => None,
                    };

                    // commit 的 COW 快路径可以保留：prepare 已逐 Key 完成冲突检查；若当前根
                    // 仍与事务快照同源，整根替换与逐动作合并具有相同逻辑结果。
                    let mut root = tr.0.table.0.root.lock();
                    if root.ptr_eq(&tr.0.root_ref) {
                        *root = tr.0.root_mut.lock().clone();
                    } else {
                        for (key, action) in &prepared.actions {
                            match action {
                                KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                                    root.delete(key, false);
                                },
                                KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                                    root.upsert(key.clone(), value.clone(), false);
                                },
                                KVActionLog::Read => (),
                            }
                        }
                    }

                    if let (Some(context), Some(revision)) =
                        (tr.0.version_context.as_ref(), revision) {
                        for (key, action) in &prepared.actions {
                            let value = match action {
                                KVActionLog::Write(value) | KVActionLog::DirtyWrite(value) => value,
                                KVActionLog::Read => continue,
                            };
                            committed_versions.push(context.versions().publish(
                                tr.0.table.name(),
                                key.clone(),
                                value.as_ref(),
                                transaction_uid.clone(),
                                revision));
                        }
                        // Release store 只在本表全部数据和 Key 版本都已发布后推进 revision。
                        context.versions().complete_revision(revision);
                        if let Some(receipt) = context.receipt() {
                            receipt.append(committed_versions);
                        }
                    }
                }
            }

            drop(publication);
            if let Some(context) = tr.0.version_context.as_ref() {
                context.release_snapshot();
            }

            if tr.is_require_persistence() {
                // Memory+persistence 只把动作写入根 WAL，不创建独立表数据文件；当前
                // 实现没有需要等待的表数据文件阶段，因此内存根发布后立即发送唯一一次
                // Ok 成功信号。
                // 详见 CONTRACT-CFM-001：
                // docs/SEMANTIC_CONTRACTS.md#contract-confirm-success-only。
                let commit_uid = tr.get_commit_uid().unwrap();
                if let Err(e) = confirm(transaction_uid.clone(), commit_uid, Ok(())) {
                    return Err(e);
                }
            }

            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Transaction2PcAllConflicts for MemOrdTabTr<C, Log> {
    fn precheck_all_conflicts(&self)
        -> BoxFuture<'_, Result<(), <Self as Transaction2Pc>::PrepareError>> {
        let tr = self.clone();
        async move {
            tr.precheck_versions().await
        }.boxed()
    }

    fn prepare_all_conflicts(&self)
        -> BoxFuture<'_, Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        let tr = self.clone();
        async move {
            tr.prepare_registered(PrepareConflictKind::All).await
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> UnitTransaction for MemOrdTabTr<C, Log> {
    type Status = Transaction2PcStatus;
    type Qos = TableTrQos;

    //有序内存表事务，一定是单元事务
    fn is_unit(&self) -> bool {
        true
    }

    fn get_status(&self) -> <Self as UnitTransaction>::Status {
        self.0.status.lock().clone()
    }

    fn set_status(&self, status: <Self as UnitTransaction>::Status) {
        *self.0.status.lock() = status;
    }

    fn qos(&self) -> <Self as UnitTransaction>::Qos {
        if self.is_require_persistence() {
            TableTrQos::Safe
        } else {
            TableTrQos::ThreadSafe
        }
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> SequenceTransaction for MemOrdTabTr<C, Log> {
    type Item = Self;

    //有序内存表事务，一定不是顺序事务
    fn is_sequence(&self) -> bool {
        false
    }

    fn prev_item(&self) -> Option<<Self as SequenceTransaction>::Item> {
        None
    }

    fn next_item(&self) -> Option<<Self as SequenceTransaction>::Item> {
        None
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> TransactionTree for MemOrdTabTr<C, Log> {
    type Node = KVDBTransaction<C, Log>;
    type NodeInterator = KVDBChildTrList<C, Log>;

    //有序内存表事务，一定不是事务树
    fn is_tree(&self) -> bool {
        false
    }

    fn children_len(&self) -> usize {
        0
    }

    fn to_children(&self) -> Self::NodeInterator {
        KVDBChildTrList::new()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> KVAction for MemOrdTabTr<C, Log> {
    type Key = Binary;
    type Value = Binary;
    type Error = KVTableTrError;

    fn dirty_query(&self, key: <Self as KVAction>::Key)
                   -> BoxFuture<Option<<Self as KVAction>::Value>> {
        let tr = self.clone();

        async move {
            if let Some(value) = tr.0.root_mut.lock().get(&key) {
                //指定关键值存在
                return Some(value.clone());
            }

            None
        }.boxed()
    }

    fn query(&self, key: <Self as KVAction>::Key)
             -> BoxFuture<Option<<Self as KVAction>::Value>> {
        let tr = self.clone();

        async move {
            let mut actions_locked = tr.0.actions.lock();

            if let None = actions_locked.get(&key) {
                //在事务内还未未记录指定关键字的操作，则记录对指定关键字的读操作
                let _ = actions_locked.insert(key.clone(), KVActionLog::Read);
            }

            if let Some(value) = tr.0.root_mut.lock().get(&key) {
                //指定关键值存在
                return Some(value.clone());
            }

            None
        }.boxed()
    }

    fn dirty_upsert(&self,
                    key: <Self as KVAction>::Key,
                    value: <Self as KVAction>::Value)
                    -> BoxFuture<Result<(), <Self as KVAction>::Error>> {
        let tr = self.clone();

        async move {
            //记录对指定关键字的最新插入或更新操作
            let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::DirtyWrite(Some(value.clone())));

            //插入或更新指定的键值对
            let _ = tr.0.root_mut.lock().upsert(key, value, false);

            Ok(())
        }.boxed()
    }

    fn upsert(&self,
              key: <Self as KVAction>::Key,
              value: <Self as KVAction>::Value)
              -> BoxFuture<Result<(), <Self as KVAction>::Error>> {
        let tr = self.clone();

        async move {
            //记录对指定关键字的最新插入或更新操作
            let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::Write(Some(value.clone())));

            //插入或更新指定的键值对
            let _ = tr.0.root_mut.lock().upsert(key, value, false);

            Ok(())
        }.boxed()
    }

    fn dirty_delete(&self, key: <Self as KVAction>::Key)
                    -> BoxFuture<Result<Option<<Self as KVAction>::Value>, <Self as KVAction>::Error>> {
        let tr = self.clone();

        async move {
            //记录对指定关键字的最新删除操作，并增加写操作计数
            let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::DirtyWrite(None));

            // `copy=false` 是 Memory 表的既定 API 语义：删除事务私有根中的 Key，但不为
            // 返回值克隆旧 Binary。命中与未命中最终都返回 Ok(None)，调用方不得用返回值
            // 判断 Key 是否存在。见 docs/SEMANTIC_CONTRACTS.md#contract-action-001 与
            // docs/REVIEW_FINDINGS.md#find-ordered-delete-001。
            if let Some(Some(value)) = tr.0.root_mut.lock().delete(&key, false) {
                //指定关键字存在
                return Ok(Some(value));
            }

            Ok(None)
        }.boxed()
    }

    fn delete(&self, key: <Self as KVAction>::Key)
              -> BoxFuture<Result<Option<<Self as KVAction>::Value>, <Self as KVAction>::Error>> {
        let tr = self.clone();

        async move {
            //记录对指定关键字的最新删除操作，并增加写操作计数
            let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::Write(None));

            // `copy=false` 是 Memory 表的既定 API 语义：删除事务私有根中的 Key，但不为
            // 返回值克隆旧 Binary。命中与未命中最终都返回 Ok(None)，调用方不得用返回值
            // 判断 Key 是否存在。见 docs/SEMANTIC_CONTRACTS.md#contract-action-001 与
            // docs/REVIEW_FINDINGS.md#find-ordered-delete-001。
            if let Some(Some(value)) = tr.0.root_mut.lock().delete(&key, false) {
                //指定关键字存在
                return Ok(Some(value));
            }

            Ok(None)
        }.boxed()
    }

    fn keys<'a>(&self,
                key: Option<<Self as KVAction>::Key>,
                descending: bool)
                -> BoxStream<'a, <Self as KVAction>::Key> {
        // 在锁内只克隆 O(1) COW 根，owner 随流存活；流返回后同一事务可继续改写
        // root_mut，既有流仍只观察创建时快照。创建事务本身必须活到流结束。
        // CONTRACT-ITER-001 / tests/iterator_snapshot_safety.rs。
        let root = self.0.root_mut.lock().clone();
        let mut iterator = OrdMapSnapshot::new(root, key.as_ref(), descending);

        let stream = stream! {
            while let Some(key) = iterator.next_key() {
                // 从 owner 保活的创建时快照获取下一个 owned key。
                yield key;
            }
        };

        stream.boxed()
    }

    fn values<'a>(&self,
                  key: Option<<Self as KVAction>::Key>,
                  descending: bool)
                  -> BoxStream<'a, (<Self as KVAction>::Key, <Self as KVAction>::Value)> {
        // 与 keys 使用相同的 owning snapshot；不跨 yield 持锁，也不把依赖引用暴露给流。
        let root = self.0.root_mut.lock().clone();
        let mut iterator = OrdMapSnapshot::new(root, key.as_ref(), descending);

        let stream = stream! {
            while let Some((key, value)) = iterator.next_entry() {
                // 从 owner 保活的创建时快照获取下一个 owned 键值对。
                yield (key, value);
            }
        };

        stream.boxed()
    }

    fn lock_key(&self, _key: <Self as KVAction>::Key)
                -> BoxFuture<Result<(), <Self as KVAction>::Error>> {
        async move {
            Ok(())
        }.boxed()
    }

    fn unlock_key(&self, _key: <Self as KVAction>::Key)
                  -> BoxFuture<Result<(), <Self as KVAction>::Error>> {
        async move {
            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> MemOrdTabTr<C, Log> {
    // 构建一个有序内存表事务
    #[inline]
    fn new(source: Atom,
           is_writable: bool,
           is_persistence: bool,
           prepare_timeout: u64,
           commit_timeout: u64,
           table: MemoryOrderedTable<C, Log>) -> Self {
        let root_ref = table.0.root.lock().clone();

        let inner = InnerMemOrdTabTr {
            source,
            tid: SpinLock::new(None),
            cid: SpinLock::new(None),
            status: SpinLock::new(Transaction2PcStatus::default()),
            writable: is_writable,
            persistence: AtomicBool::new(is_persistence),
            prepare_timeout,
            commit_timeout,
            root_mut: SpinLock::new(root_ref.clone()),
            root_ref,
            table,
            actions: SpinLock::new(XHashMap::default()),
            version_context: None,
        };

        MemOrdTabTr(Arc::new(inner))
    }

    /// 构建由数据库管理器装配的事务，并在同一根 guard 内固定数据快照和版本 revision。
    pub(crate) fn new_managed(source: Atom,
                              is_writable: bool,
                              is_persistence: bool,
                              prepare_timeout: u64,
                              commit_timeout: u64,
                              table: MemoryOrderedTable<C, Log>,
                              versions: KeyVersions,
                              mode: PrepareMode,
                              expected: XHashMap<Binary, Version>,
                              receipt: Option<VersionReceipt>,
                              actions: XHashMap<Binary, KVActionLog>) -> Self {
        let root_locked = table.0.root.lock();
        let root_ref = root_locked.clone();
        let snapshot = versions.lease_current();
        drop(root_locked);
        let mut root_mut = root_ref.clone();
        for (key, action) in &actions {
            match action {
                KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                    let _ = root_mut.upsert(key.clone(), value.clone(), false);
                },
                KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                    let _ = root_mut.delete(key, false);
                },
                KVActionLog::Read => (),
            }
        }
        let version_context = TableVersionContext::new(versions,
                                                       snapshot,
                                                       mode,
                                                       expected,
                                                       receipt);
        let inner = InnerMemOrdTabTr {
            source,
            tid: SpinLock::new(None),
            cid: SpinLock::new(None),
            status: SpinLock::new(Transaction2PcStatus::default()),
            writable: is_writable,
            persistence: AtomicBool::new(is_persistence),
            prepare_timeout,
            commit_timeout,
            root_mut: SpinLock::new(root_mut),
            root_ref,
            table,
            actions: SpinLock::new(actions),
            version_context: Some(version_context),
        };

        MemOrdTabTr(Arc::new(inner))
    }

    async fn precheck_versions(&self) -> Result<(), KVTableTrError> {
        // 版本协议阶段一只检查外部 read-set；publication 读门防止观察到提交中间状态。
        let Some(context) = self.0.version_context.as_ref() else {
            return Ok(());
        };
        if context.mode() != PrepareMode::Versioned {
            return Ok(());
        }

        let _publication = context.versions().publication().read().await;
        let mut conflicts = Vec::new();
        for (key, expected) in context.expected() {
            if context.versions().current_version(key).as_ref() != Some(expected) {
                conflicts.push(TableKey {
                    table: self.0.table.name(),
                    key: key.clone(),
                });
            }
        }
        if conflicts.is_empty() {
            Ok(())
        } else {
            Err(KVTableTrError::new_all_conflicts_error(conflicts))
        }
    }

    async fn prepare_registered(&self,
                                conflict_kind: PrepareConflictKind)
        -> Result<Option<Vec<u8>>, KVTableTrError> {
        // 只读表事务立即短路；根 manager 仍负责整棵事务树的生命周期。
        if !self.is_writable() {
            return Ok(None);
        }

        // 固定锁序：publication(read) -> prepare。guard 覆盖版本、当前根和 prepared 预留检查。
        let _publication = match self.0.version_context.as_ref() {
            Some(context) => Some(context.versions().publication().read().await),
            None => None,
        };
        let actions = self.0.actions.lock().clone();
        let mode = self
            .0
            .version_context
            .as_ref()
            .map(TableVersionContext::mode)
            .unwrap_or(PrepareMode::Ordinary);
        let mut conflict_keys = Vec::new();

        if let Some(context) = self.0.version_context.as_ref() {
            if context.mode() == PrepareMode::Versioned {
                // 阶段一之后仍可能有提交，因此在真正预留前必须再次核对全部期望版本。
                for (key, expected) in context.expected() {
                    if context.versions().current_version(key).as_ref() != Some(expected) {
                        conflict_keys.push(key.clone());
                    }
                }
            }
        }

        let current_root = self.0.table.0.root.lock().clone();
        for (key, action) in &actions {
            // 当前实现只有“要求根 WAL 的 DirtyWrite”跳过值状态比较；非持久化事务即使调用
            // dirty_* 也执行比较。这是现状单义分支，不代表 dirty/普通 API 可以混用。
            let require_state_check = !self.is_require_persistence() || !action.is_dirty_writed();
            if !require_state_check {
                continue;
            }
            if let Some(context) = self.0.version_context.as_ref() {
                if context
                    .versions()
                    .has_committed_after(key, context.snapshot_revision()) {
                    conflict_keys.push(key.clone());
                    continue;
                }
            }
            if !binary_state_equal(self.0.root_ref.get(key), current_root.get(key)) {
                conflict_keys.push(key.clone());
            }
        }

        let write_buf = self.prepare_output(&actions);
        let mut prepare = self.0.table.0.prepare.lock();
        // 检查全部既有预留并插入当前 TID 必须位于同一锁临界区，保证首次插入冲突原子化。
        for (key, action) in &actions {
            if has_prepared_conflict(&prepare, key, mode, action) {
                conflict_keys.push(key.clone());
            }
        }
        if !conflict_keys.is_empty() {
            return Err(self.prepare_conflict_error(conflict_kind, conflict_keys));
        }

        // 冲突失败前不转移动作；成功后同一事务不允许再次 prepare。
        let _ = mem::replace(&mut *self.0.actions.lock(), XHashMap::default());
        prepare.insert(self.get_transaction_uid().unwrap(), PreparedActions {
            mode,
            actions,
        });
        Ok(write_buf)
    }

    fn prepare_output(&self,
                      actions: &XHashMap<Binary, KVActionLog>) -> Option<Vec<u8>> {
        // Memory 的 persistence 只控制根 WAL：false 直接没有表片段，true 才编码写动作；两者
        // 都没有独立 Memory 数据文件。纯读即使 persistence=true 也返回 None。
        if !self.is_require_persistence() {
            return None;
        }
        let writed_count = actions
            .values()
            .filter(|action| matches!(action,
                                     KVActionLog::Write(_) | KVActionLog::DirtyWrite(_)))
            .count() as u64;
        if writed_count == 0 {
            return None;
        }

        let mut buf = Vec::new();
        self.0.table.init_table_prepare_output(&mut buf, writed_count);
        for (key, action) in actions {
            match action {
                KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                    self.0.table.append_key_value_to_table_prepare_output(&mut buf, key, None);
                },
                KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                    self.0.table.append_key_value_to_table_prepare_output(&mut buf,
                                                                          key,
                                                                          Some(value));
                },
                KVActionLog::Read => (),
            }
        }
        Some(buf)
    }

    fn prepare_conflict_error(&self,
                              conflict_kind: PrepareConflictKind,
                              keys: Vec<Binary>) -> KVTableTrError {
        // 仅在 keys 非空时调用；All 集合由根 manager 跨表归一化。
        let key = keys[0].clone();
        match conflict_kind {
            PrepareConflictKind::Common => {
                KVTableTrError::new_transaction_error(
                    ErrorLevel::Normal,
                    format!("Prepare memory ordered table conflicted, table: {:?}, key: {:?}, source: {:?}, transaction_uid: {:?}, reason: committed state or prepared reservation changed",
                            self.0.table.name().as_str(),
                            key,
                            self.0.source,
                            self.get_transaction_uid()))
            },
            PrepareConflictKind::First => {
                KVTableTrError::new_conflicts_error(self.0.table.name(), key)
            },
            PrepareConflictKind::All => {
                KVTableTrError::new_all_conflicts_error(keys
                    .into_iter()
                    .map(|key| TableKey {
                        table: self.0.table.name(),
                        key,
                    })
                    .collect())
            },
        }
    }

    /// 为根 WAL repair 重建 Memory 表已提交动作。
    ///
    /// 受信 replay 已决定这些动作应生效，因此本入口跳过在线冲突检查，直接修改当前内存根并
    /// 以指定 TID 建立普通 prepared 记录。它不创建版本上下文，不得由在线业务调用。
    pub(crate) fn prepare_repair(&self, transaction_uid: Guid) {
        //获取事务的当前操作记录，并重置事务的当前操作记录
        let actions = mem::replace(&mut *self.0.actions.lock(), XHashMap::default());

        //在事务对应的表的根节点，执行操作记录中的所有写操作
        for (key, action) in &actions {
            match action {
                KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                    //执行插入或更新指定关键字的值的操作
                    self
                        .0
                        .table
                        .0
                        .root
                        .lock()
                        .upsert(key.clone(), value.clone(), false);
                },
                KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                    //执行删除指定关键字的值的操作
                    self.0.table.0.root.lock().delete(key, false);
                },
                KVActionLog::Read => (), //忽略读操作
            }
        }

        //将事务的当前操作记录，写入表的预提交表
        self.0.table.0.prepare.lock().insert(transaction_uid, PreparedActions {
            mode: PrepareMode::Ordinary,
            actions,
        });
    }
}

/// Memory 子事务的共享状态及数据/版本观察点。
struct InnerMemOrdTabTr<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> {
    source:             Atom,                                       // 诊断事件源。
    tid:                SpinLock<Option<Guid>>,                     // 根 manager 分配并向子树传播的事务 ID。
    cid:                SpinLock<Option<Guid>>,                     // 根 WAL 确认占位/回执使用的 commit ID。
    status:             SpinLock<Transaction2PcStatus>,             // 事务框架推进的 2PC 状态。
    writable:           bool,                                       // 创建后不变；false 时 prepare 短路。
    persistence:        AtomicBool,                                 // 是否生成根 WAL 片段，不表示数据文件。
    prepare_timeout:    u64,                                        // 预提交超时，毫秒。
    commit_timeout:     u64,                                        // 提交超时，毫秒。
    root_mut:           SpinLock<OrdMap<Tree<Binary, Binary>>>,     // 应用本事务动作后的私有 COW 根。
    root_ref:           OrdMap<Tree<Binary, Binary>>,               // 创建事务时的值状态冲突基线。
    table:              MemoryOrderedTable<C, Log>,                 // 表共享 owner，保活根和预留表。
    actions:            SpinLock<XHashMap<Binary, KVActionLog>>,    // 每 Key 最终动作，prepare 后转移。
    version_context:    Option<TableVersionContext>,                 // managed lease/revision/expected/receipt。
}
