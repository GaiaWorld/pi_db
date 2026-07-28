//! 纯内存 COW 有序表及其 2PC 子事务实现。
//!
//! Memory 表的数据只存在于进程内 `OrdMap`，没有独立数据文件。表元数据中的
//! `persistence=true` 仍然合法：写动作会进入根 WAL，并在内存根发布后立即调用子表确认器；
//! `persistence=false` 则不生成该表的根 WAL 片段。未确认根 WAL 可以在启动 repair 中重放
//! Memory 动作；一旦事务已确认且对应 WAL 可移走，后续只从数据文件启动只能恢复表定义，
//! 不会恢复 Memory 业务值。这是 Memory 的易失性边界，不是持久表语义。
//!
//! 普通/版本子事务由根事务按“每表唯一 owner”装配并加入 2PC 子树；纯迭代器可使用不加入
//! 子树的 detached 只读事务，流自身持有创建时 COW 根。两者都通过 `Arc` 保活表，但表不会
//! 反向持有事务：表级 `prepare` 只保存 TID 到冻结动作的映射，因此引用图不成环。
//!
//! 完整结构、锁序、生命周期、普通/版本/repair 流程及证据矩阵见
//! `docs/MEMORY_TABLE_INTERNAL_CONTRACT.md#memory-table-internal-contract-index`。

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
            TableKeyConflict,
            db::{KVDBTransaction, KVDBChildTrList},
            key_version::{KeyVersions,
                          PrepareMode,
                          PreparedActions,
                          PreparedCommitError,
                          TableVersionContext,
                          Version,
                          VersionConflictKind,
                          VersionReceipt,
                          binary_state_equal,
                          has_prepared_conflict,
                          has_prepared_transaction,
                          take_prepared_for_commit},
            tables::{KVTable, ordmap_snapshot::OrdMapSnapshot}};

/// 以 COW `OrdMap` 保存已提交数据的有序 Memory 表共享句柄。
///
/// `root` 是当前已提交逻辑数据，`prepare` 是跨事务的 TID 预留表。clone 只增加内部 `Arc`
/// 引用；`path()` 永远返回 `None`，`ready_collect/collect` 是成功 no-op。表可配置参与根 WAL，
/// 但这不会创建独立表日志、后台持久化任务或数据文件。
///
/// 表句柄可以跨线程共享。单次 `len/query_committed/size` 只保证各自锁内的一致瞬时观察，不
/// 组成跨调用快照；事务快照由 `MemOrdTabTr::{root_ref,root_mut}` 独立承担。
#[derive(Clone)]
pub struct MemoryOrderedTable<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
>(Arc<InnerMemoryOrderedTable<C, Log>>);

// SAFETY: `root` 与 `prepare` 分别由 parking_lot::Mutex 保护，剩余运行时字段构造后不可变；
// 外层只移动/clone Arc owner。C/Log 仅出现在 PhantomData 中，本结构不保存、访问或析构它们
// 的实例，因此不会绕过泛型实例自身的线程边界。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for MemoryOrderedTable<C, Log> {}
// SAFETY: 所有共享可变访问均经表级锁，安全 API 不返回锁内引用；事务/迭代器只单向持有表
// Arc，表级 prepare map 不持有事务 Arc，故不存在由本结构形成的引用环。
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
        // Atom clone 只增加共享名称引用，不读取数据根或持锁。
        self.0.name.clone()
    }

    fn path(&self) -> Option<&Path> {
        // 即使 persistence=true，Memory 也没有独立表路径或数据文件。
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
        // OrdMap 维护元素计数；这里只在根锁内读取 O(1) 聚合值。
        self.0.root.lock().size()
    }

    fn size(&self) -> u64 {
        // 克隆 COW 根后立即释放表锁，再读取 OrdMap 缓存的 payload/节点估算；该值不是 RSS、
        // allocator 实际占用、WAL 大小或某个事务快照的精确物理内存。
        let root_copy = self.0.root.lock().clone();
        root_copy.full_bytes_size()
    }

    fn transaction(&self,
                   source: Atom,
                   is_writable: bool,
                   is_persistent: bool,
                   prepare_timeout: u64,
                   commit_timeout: u64) -> Self::Tr {
        // 当前生产调用只用此低层构造器建立 detached iterator owner；普通/版本 2PC 必须走
        // new_managed，以同时租用版本快照并受根事务“每表唯一 owner”约束。
        MemOrdTabTr::new(source,
                         is_writable,
                         is_persistent,
                         prepare_timeout,
                         commit_timeout,
                         self.clone())
    }

    fn ready_collect(&self) -> BoxFuture<Result<(), Self::Error>> {
        async move {
            // Memory 没有文件、collector 状态或待轮换资源，准备整理是幂等成功 no-op。
            Ok(())
        }.boxed()
    }

    fn collect(&self) -> BoxFuture<Result<(), Self::Error>> {
        async move {
            // Memory 不做压缩/落盘；调用成功不会清空数据、释放表或改变版本缓存。
            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> MemoryOrderedTable<C, Log> {
    /// 在不创建事务的前提下读取当前已提交 COW 根；调用方负责 publication 同步。
    ///
    /// 该方法在根 mutex 内克隆命中值，时间为 O(log n)，不会记录事务 Read、租用版本快照或
    /// 刷新版本 TTL。`query_with_version` 必须在外层 publication 协议内把本结果与版本绑定。
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
///
/// 固定锁关系为：在线 prepare 可持有版本 publication 读门后短暂取得 `root`、再取得
/// `prepare`；commit 可持有 publication 写门后先短暂取得 `prepare`，释放后再取得 `root`。
/// 本结构内不存在同时持有 `root` 与 `prepare` 的路径，也没有锁内 `.await`。repair 在数据库
/// 尚未对业务开放的启动阶段直接更新 `root`，其串行前提由根 repair 流程保证。
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

// SAFETY: 共享可变字段均由 Mutex 保护且 guard 不逃逸；name/persistence 构造后不可变，
// PhantomData 不持有 C/Log 实例、不引入运行时内存或析构行为。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for InnerMemoryOrderedTable<C, Log> {}
// SAFETY: `&self` 无法绕过 root/prepare 锁取得可变引用；两张表中的 Key/Value/动作均为
// owned 值，不保存指向事务私有根的借用或裸指针。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Sync for InnerMemoryOrderedTable<C, Log> {}

/// Memory 表的单元子事务共享句柄。
///
/// `root_ref` 固定创建时基线，`root_mut` 承载事务内 COW 修改，`actions` 保存每 Key 最终动作。
/// prepare 成功后动作所有权转移到表级 `prepare`；commit 在 publication 写门内发布数据与
/// 版本。clone 共享同一事务，不能作为重复 prepare/commit 的新事务使用。
///
/// managed 子事务是根事务树中的叶节点：根 manager 向任意深度子树传播同一个 TID/CID，
/// 本节点用 TID 占用表级 prepare 项，用 CID 参与根 WAL 的提交确认占位。detached 迭代事务不
/// 加入 2PC 树、没有版本上下文，也不应单独 prepare/commit。
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

// SAFETY: 内部可变状态由 SpinLock/AtomicBool 和表级 Mutex 同步；`root_ref` 是不可变 COW
// owner，外层只移动/clone Arc。任何安全方法都不会返回对锁内动作或根节点的借用。
unsafe impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> Send for MemOrdTabTr<C, Log> {}
// SAFETY: 共享引用不产生无同步可变别名；事务只单向持有表 Arc，表不反向持有事务。重复
// prepare/commit、prepare 后继续动作等协议约束影响事务正确性但不允许形成 data race；这些
// 非法状态调用当前未被统一 guard，见 `docs/REVIEW_FINDINGS.md#find-tr-002`。
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
        // 创建后不可变；只读节点由 manager 在 prepare/commit 阶段短路。
        self.0.writable
    }

    fn is_concurrent_commit(&self) -> bool {
        // 同一根中的 Memory 叶节点由事务 manager 按子节点顺序提交。
        false
    }

    fn is_concurrent_rollback(&self) -> bool {
        // rollback 会访问共享 prepare map 和版本 lease，当前声明为顺序回滚。
        false
    }

    fn get_source(&self) -> Atom {
        self.0.source.clone()
    }

    fn init(&self)
            -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        async move {
            // 快照和事务私有根已在构造阶段完成，不存在异步初始化资源。
            Ok(())
        }.boxed()
    }

    fn rollback(&self)
                -> BoxFuture<Result<<Self as AsyncTransaction>::Output, <Self as AsyncTransaction>::Error>> {
        let tr = self.clone();

        async move {
            // rollback 仅移除 prepared 预留、释放版本 lease；未发布的私有 COW 根随事务释放。
            // 它不会反向修改已发布根，也不会撤销 WAL 已成功落地后的 Fatal 提交阶段。合法
            // 调用要求 manager 已分配 TID，且同一事务只 rollback 一次。
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
        // Relaxed 足够：该位只做单调 false->true 能力聚合，不发布其它内存状态。
        self.0.persistence.load(Ordering::Relaxed)
    }

    fn require_persistence(&self) {
        // 单调提升，不能降级；表示本节点输出可进入根 WAL，不表示 Memory 拥有数据文件。
        self.0.persistence.store(true, Ordering::Relaxed);
    }

    fn is_concurrent_prepare(&self) -> bool {
        // 根 manager 不会并发 prepare 同一 Memory 节点；跨根并发由表级 prepare mutex 仲裁。
        false
    }

    fn is_enable_inherit_uid(&self) -> bool {
        // 允许根 manager 分配并向整棵 owned 事务树传播同一 TID/CID。
        true
    }

    fn get_transaction_uid(&self) -> Option<<Self as Transaction2Pc>::Tid> {
        self.0.tid.lock().clone()
    }

    fn set_transaction_uid(&self, uid: <Self as Transaction2Pc>::Tid) {
        *self.0.tid.lock() = Some(uid);
    }

    fn get_prepare_uid(&self) -> Option<<Self as Transaction2Pc>::Pid> {
        // Memory 没有独立 prepare ID；表级预留直接以根 TID 为唯一键。
        None
    }

    fn set_prepare_uid(&self, _uid: <Self as Transaction2Pc>::Pid) {
        // 与 get_prepare_uid 对称的 no-op；根 TID 已足够标识表级 prepared 动作。
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
            // 普通 managed 事务同样带有版本上下文并使用此门，以保证普通写也推进全局 Key
            // 版本；detached iterator 没有版本上下文且不进入本提交路径。
            let publication = match tr.0.version_context.as_ref() {
                Some(context) => Some(context.versions().publication().write().await),
                None => None,
            };
            // 正常 prepare 与 WAL repair 的 prepare_repair 都必须先登记该项；repair 使用
            // Ordinary mode，因此“replay 跳过事务框架标准 prepare”不等于允许这里缺项。
            let expected_mode = tr
                .0
                .version_context
                .as_ref()
                .map(TableVersionContext::mode)
                .unwrap_or(PrepareMode::Ordinary);
            let prepared = {
                let mut prepare = tr.0.table.0.prepare.lock();
                take_prepared_for_commit(&mut prepare,
                                         &transaction_uid,
                                         expected_mode,
                                         tr.is_writable())
            };
            let prepared = match prepared {
                Ok(prepared) => prepared,
                Err(PreparedCommitError::ModeMismatch(prepared_mode)) => {
                    drop(publication);
                    if let Some(context) = tr.0.version_context.as_ref() {
                        context.release_snapshot();
                    }
                    return Err(KVTableTrError::new_transaction_error(
                        ErrorLevel::Fatal,
                        format!("Commit memory ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, expected_mode: {:?}, prepared_mode: {:?}, reason: prepared action protocol mismatch after entering non-rollbackable commit",
                                tr.0.table.name().as_str(),
                                tr.0.source,
                                transaction_uid,
                                expected_mode,
                                prepared_mode)));
                },
                Err(PreparedCommitError::Missing) => {
                    drop(publication);
                    if let Some(context) = tr.0.version_context.as_ref() {
                        context.release_snapshot();
                    }
                    return Err(KVTableTrError::new_transaction_error(
                        ErrorLevel::Fatal,
                        format!("Commit memory ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, expected_mode: {:?}, reason: prepared actions missing after entering non-rollbackable commit",
                                tr.0.table.name().as_str(),
                                tr.0.source,
                                transaction_uid,
                                expected_mode)));
                },
            };
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
                    // 这一等价性还依赖合法协议禁止 prepare 成功后继续 upsert/delete；否则
                    // root_mut 可能包含未进入冻结 actions/WAL 的后置动作。当前库没有统一状态
                    // guard，这一非最终边界归档于 docs/REVIEW_FINDINGS.md#find-tr-002。
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
                // Ok 成功信号。该信号完成的是本子表确认回执，不改变“整个根 WAL 中全部事务
                // 都确认后才可标记 .bak/移走”的根级规则。
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
            // 第一阶段只检查版本 read-set，不登记 prepare 项、不转移动作、不生成 WAL。
            tr.precheck_versions().await
        }.boxed()
    }

    fn prepare_all_conflicts(&self)
        -> BoxFuture<'_, Result<Option<<Self as Transaction2Pc>::PrepareOutput>, <Self as Transaction2Pc>::PrepareError>> {
        let tr = self.clone();
        async move {
            // 第二阶段重新核对版本和值状态并在同一表级临界区登记完整动作。
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

    // Memory 表事务是根树中的叶节点，自己不组合其它事务。
    fn is_unit(&self) -> bool {
        true
    }

    fn get_status(&self) -> <Self as UnitTransaction>::Status {
        // 状态由根事务 manager 推进；表动作方法当前不会据此自行拒绝非法后置调用。
        self.0.status.lock().clone()
    }

    fn set_status(&self, status: <Self as UnitTransaction>::Status) {
        *self.0.status.lock() = status;
    }

    fn qos(&self) -> <Self as UnitTransaction>::Qos {
        // Safe 表示参与根 WAL；ThreadSafe 表示仍有同步保护但不承诺崩溃恢复。
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

    // Memory 叶节点没有表内前驱/后继；跨表顺序由根 childs 列表表达。
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

    // Memory 叶节点不再拥有子节点；任意深度嵌套关系由上层事务树节点表达。
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
            // dirty_query 与 query 读取同一个事务私有根，并非越过事务读取共享最新根；唯一
            // 差异是它不登记 Read 动作，因此不能单独建立值状态冲突依赖。外部协议要求一个
            // 事务只使用 dirty_* 族或只使用事务安全族，混用不保证事务安全。
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
                // 只在尚无动作时登记 Read；同 Key 已有 Write/DirtyWrite 时不能被查询降级。
                let _ = actions_locked.insert(key.clone(), KVActionLog::Read);
            }
            // 当前实现让 actions guard 保持到 async 块结束，因此随后读取 root_mut 时形成
            // actions -> root_mut 的短嵌套锁；锁内没有 await。其它动作路径不建立反向的
            // root_mut -> actions 锁序。是否缩短该历史临界区属于生产同步变更，不在本轮处理。

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
            // 同 Key 后写覆盖先前 Read/Write/DirtyWrite，actions 只保存最终动作，不保留历史。
            let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::DirtyWrite(Some(value.clone())));

            // root_mut 是事务私有 COW 根；此时不修改表的共享已提交 root。
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
            // 同 Key 后写覆盖先前动作；prepare 将用创建时状态/版本和表级预留检查冲突。
            let _ = tr.0.actions.lock().insert(key.clone(), KVActionLog::Write(Some(value.clone())));

            // 仅修改事务私有 COW 根，commit 前对其它事务不可见。
            let _ = tr.0.root_mut.lock().upsert(key, value, false);

            Ok(())
        }.boxed()
    }

    fn dirty_delete(&self, key: <Self as KVAction>::Key)
                    -> BoxFuture<Result<Option<<Self as KVAction>::Value>, <Self as KVAction>::Error>> {
        let tr = self.clone();

        async move {
            // None 是内部 tombstone，不是持久化空 Value；同 Key 先前动作被最终删除覆盖。
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
            // None 是内部 tombstone，不是持久化空 Value；同 Key 先前动作被最终删除覆盖。
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
        // root_mut，既有流仍只观察创建时快照。按公开协议，创建事务必须活到流结束或 drop；
        // 流不参与 read-set，也不承诺与创建事务后续写入之间的事务安全性。
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
        // Key 不被读取；构造 boxed future 会分配一次，但首次 poll 立即完成且不触碰
        // root/actions/prepare/version。根包装层的 managed 快照副作用见 ROOT-KEY-HOOK-001。
        async move {
            // 当前是兼容性 no-op，不建立互斥、预留或事务冲突边；见 FIND-LOCK-001。
            Ok(())
        }.boxed()
    }

    fn unlock_key(&self, _key: <Self as KVAction>::Key)
                  -> BoxFuture<Result<(), <Self as KVAction>::Error>> {
        // 与 lock_key 相同，只分配立即 ready 的 boxed future，不检查 owner 或既有锁状态。
        async move {
            // 与 lock_key 对称的 no-op；调用成功不证明当前线程/事务拥有任何锁。
            Ok(())
        }.boxed()
    }
}

impl<
    C: Clone + Send + 'static,
    Log: AsyncCommitLog<C = C, Cid = Guid>,
> MemOrdTabTr<C, Log> {
    /// 构建不加入根 2PC 子树、也不租用 Key 版本快照的低层事务。
    ///
    /// 当前生产用途是 detached iterator owner：它固定创建时数据根，流再克隆该 COW owner。
    /// 该入口不会注册每表唯一 owner、版本上下文或事务 manager 身份，业务读写不得绕过
    /// `KVDBTransaction` 直接使用它完成 2PC。
    #[inline]
    fn new(source: Atom,
           is_writable: bool,
           is_persistence: bool,
           prepare_timeout: u64,
           commit_timeout: u64,
           table: MemoryOrderedTable<C, Log>) -> Self {
        // 只在表根锁内克隆 O(1) COW owner；锁不会进入事务对象或异步阶段。
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

    /// 构建由数据库管理器装配的事务，并固定一致的数据根与版本 revision 基线。
    ///
    /// 调用方必须已经执行根协议选择和“每表唯一 owner”检查。构造期间先持有表 `root` guard
    /// 克隆 COW 基线，再租用当前版本 revision；并发 commit 若已取得 publication 写门但尚在
    /// 等待 `root`，只能在本 guard 释放后同时发布数据与完成 revision，因此不会得到“新版本+
    /// 旧数据”或“新数据+旧版本”的起始组合。释放 root 后，传入动作只应用到私有 root_mut。
    ///
    /// `mode` 区分 Ordinary/SchemaCreate/Versioned 冲突矩阵；`expected` 是版本 read-set，
    /// `receipt` 只供版本业务写收集本事务回执。构造本身不登记 prepare、不生成 WAL、不发布值。
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
        // 普通/SchemaCreate 或 detached 事务没有此阶段副作用，直接成功。
        let Some(context) = self.0.version_context.as_ref() else {
            return Ok(());
        };
        if context.mode() != PrepareMode::Versioned {
            return Ok(());
        }

        // 读门可跨本段 await 获取，但取得后只做内存读取；guard 内没有后续 await、表锁或 I/O。
        let _publication = context.versions().publication().read().await;
        let mut conflicts = Vec::new();
        for (key, expected) in context.expected() {
            if context.versions().current_version(key).as_ref() != Some(expected) {
                conflicts.push(TableKeyConflict {
                    table: self.0.table.name(),
                    key: key.clone(),
                    kind: VersionConflictKind::ReadSetVersionMismatch,
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
        // 只读表事务立即短路；不读取 TID、不登记预留、不生成 WAL。根 manager 仍负责整棵
        // 事务树的状态推进和最终释放。
        if !self.is_writable() {
            return Ok(None);
        }

        // 固定顺序：先异步取得 publication(read)，随后分别短暂读取 actions、root，最后取得
        // prepare mutex。root guard 在 clone 后已经释放，因此 publication 与 prepare 是唯一
        // 同时存活的共享 guard；整个 publication 临界区内没有后续 await 或 I/O。
        let _publication = match self.0.version_context.as_ref() {
            Some(context) => Some(context.versions().publication().read().await),
            None => None,
        };
        // clone 固定本次 prepare 的最终动作视图；合法协议禁止 prepare 后继续动作。当前缺少
        // 统一状态 guard，边界见 docs/REVIEW_FINDINGS.md#find-tr-002。
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
                        conflict_keys.push((key.clone(),
                                            VersionConflictKind::ReadSetVersionMismatch));
                    }
                }
            }
        }

        let current_root = self.0.table.0.root.lock().clone();
        for (key, action) in &actions {
            // 当前实现只有“要求根 WAL 的 DirtyWrite”跳过值状态比较；非持久化事务即使调用
            // dirty_* 也执行比较。这是当前实现结论，不是最终或最佳 dirty 隔离设计；dirty
            // 与普通动作混用不保证事务安全，详见 docs/REVIEW_FINDINGS.md#find-dirty-001。
            let require_state_check = !self.is_require_persistence() || !action.is_dirty_writed();
            if !require_state_check {
                continue;
            }
            if let Some(context) = self.0.version_context.as_ref() {
                if context
                    .versions()
                    .has_committed_after(key, context.snapshot_revision()) {
                    conflict_keys.push((key.clone(),
                                        VersionConflictKind::TransactionConflict));
                    continue;
                }
            }
            if !binary_state_equal(self.0.root_ref.get(key), current_root.get(key)) {
                conflict_keys.push((key.clone(),
                                    VersionConflictKind::TransactionConflict));
            }
        }

        let write_buf = self.prepare_output(&actions);
        let mut prepare = self.0.table.0.prepare.lock();
        let transaction_uid = self.get_transaction_uid().unwrap();
        // 表级 prepare 以根 TID 为索引；同一 TID 已存在意味着事务树错误地包含了同表兄弟
        // 节点或发生重复 prepare。必须在转移动作前拒绝，绝不能覆盖先前节点的冻结动作。
        if has_prepared_transaction(&prepare, &transaction_uid) {
            return Err(KVTableTrError::new_transaction_error(
                ErrorLevel::Normal,
                format!("Prepare memory ordered table failed, table: {:?}, source: {:?}, transaction_uid: {:?}, reason: duplicate prepared transaction uid",
                        self.0.table.name().as_str(),
                        self.0.source,
                        transaction_uid)));
        }
        // 检查全部既有预留并插入当前 TID 必须位于同一锁临界区，保证首次插入冲突原子化。
        for (key, action) in &actions {
            if has_prepared_conflict(&prepare, key, mode, action) {
                conflict_keys.push((key.clone(),
                                    VersionConflictKind::TransactionConflict));
            }
        }
        if !conflict_keys.is_empty() {
            return Err(self.prepare_conflict_error(conflict_kind, conflict_keys));
        }

        // 冲突失败前不转移动作，调用方可 rollback；成功后动作所有权从事务局部状态转移到
        // 表级 TID 预留，同一事务不允许再次 prepare，后续只能 commit 或按非 Fatal 路径
        // rollback。重复调用限制由外部协议保证，本层只防止覆盖同 TID prepared 项。
        let _ = mem::replace(&mut *self.0.actions.lock(), XHashMap::default());
        prepare.insert(transaction_uid, PreparedActions {
            mode,
            actions,
        });
        Ok(write_buf)
    }

    fn prepare_output(&self,
                      actions: &XHashMap<Binary, KVActionLog>) -> Option<Vec<u8>> {
        // Memory 的 persistence 只控制根 WAL：false 直接没有表片段，true 才编码写动作；两者
        // 都没有独立 Memory 数据文件。纯读即使 persistence=true 也返回 None，但可写子事务
        // 仍须由 manager 调用 commit 以消费 prepared 项，不能用 None 推导“跳过提交”。
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
                              keys: Vec<(Binary, VersionConflictKind)>) -> KVTableTrError {
        // 仅在 keys 非空时调用；All 集合由错误构造器排序/去重，根 manager 再跨表归并。
        // 分类与同 Key 优先级见 docs/VERSION_CONFLICT_KIND_DESIGN.md。
        let key = keys[0].0.clone();
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
                    .map(|(key, kind)| TableKeyConflict {
                        table: self.0.table.name(),
                        key,
                        kind,
                    })
                    .collect())
            },
        }
    }

    /// 为根 WAL repair 重建 Memory 表已提交动作。
    ///
    /// 受信 replay 已决定这些动作应生效，因此本入口跳过在线冲突检查，直接修改当前内存根并
    /// 以指定 TID 建立普通 prepared 记录。它不创建版本上下文，不得由在线业务调用。
    ///
    /// repair 发生在数据库对业务开放前，并由根 WAL 顺序驱动；最终 upsert/delete 重放本身
    /// 幂等。这里先应用根、再登记 prepared，随后 `replay_commit` 仍会按普通 commit 路径消费
    /// 该 TID 并完成确认。任一步失败时启动整体失败，尚未发布给业务的内存根不会形成在线
    /// 可见的半修复状态。确认后的 WAL 移走后，Memory 值不再具备后续 data-only 恢复来源。
    pub(crate) fn prepare_repair(&self, transaction_uid: Guid) {
        // 冻结并转移动作；repair transaction 后续不得再追加动作或重复 prepare。
        let actions = mem::replace(&mut *self.0.actions.lock(), XHashMap::default());

        // repair 启动阶段没有在线事务；逐项锁根保持当前实现边界，不把锁带入其它状态。
        for (key, action) in &actions {
            match action {
                KVActionLog::Write(Some(value)) | KVActionLog::DirtyWrite(Some(value)) => {
                    // upsert 的最终状态重复应用仍得到同一值。
                    self
                        .0
                        .table
                        .0
                        .root
                        .lock()
                        .upsert(key.clone(), value.clone(), false);
                },
                KVActionLog::Write(None) | KVActionLog::DirtyWrite(None) => {
                    // tombstone 重复应用仍保持 Key 不存在。
                    self.0.table.0.root.lock().delete(key, false);
                },
                KVActionLog::Read => (), // Read 不改变 repair 后数据，也不进入版本发布。
            }
        }

        // replay_commit 需要按相同根 TID 取得该项；模式固定 Ordinary，repair 不返回版本回执。
        self.0.table.0.prepare.lock().insert(transaction_uid, PreparedActions {
            mode: PrepareMode::Ordinary,
            actions,
        });
    }
}

/// Memory 子事务的共享状态及数据/版本观察点。
///
/// 引用图为 `MemOrdTabTr Arc -> InnerMemOrdTabTr -> MemoryOrderedTable Arc`，表只保存 owned
/// prepared actions，不保存事务 Arc；`TableVersionContext` 持有版本 registry/lease/可选回执，
/// registry 的反向关系使用 `Weak`。因此正常 drop/rollback/commit 后不存在由这些字段构成的
/// 强引用环。迭代流只持有独立 COW 根，不反向持有本事务。
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

#[cfg(test)]
mod memory_local_contract_tests {
    //! Memory 表内部局部不变量测试。
    //!
    //! 这些测试直接观察私有 COW 根和 prepare map，只证明本模块的结构、所有权与分支事实；
    //! 真实 manager、根事务树、WAL、确认、repair 启动和 data-only 重启仍由独立集成测试证明。

    use std::sync::Arc;

    use futures::executor::block_on;
    use pi_bon::{Encode, WriteBuffer};
    use pi_store::commit_logger::CommitLogger;

    use super::*;

    type TestTable = MemoryOrderedTable<usize, CommitLogger>;

    fn bon_usize(value: usize) -> Binary {
        let mut buffer = WriteBuffer::new();
        value.encode(&mut buffer);
        Binary::new(buffer.bytes)
    }

    fn assert_binary(actual: Option<Binary>, expected: Option<&Binary>, label: &str) {
        match (actual, expected) {
            (Some(actual), Some(expected)) => {
                assert_eq!(actual.as_ref(), expected.as_ref(), "{label}: value mismatch");
            },
            (None, None) => (),
            (actual, expected) => {
                panic!("{label}: presence mismatch, actual: {}, expected: {}",
                       actual.is_some(),
                       expected.is_some());
            },
        }
    }

    /// 表属性、叶节点拓扑、根身份字段、状态与 persistence 提升必须保持单义。
    #[test]
    fn test_memory_metadata_leaf_identity_and_qos_contract() {
        let table = TestTable::new(Atom::from("memory_local_identity"), true);
        assert_eq!(table.name().as_str(), "memory_local_identity");
        assert!(table.path().is_none());
        assert!(table.is_persistent());
        assert!(table.is_ordered());
        assert_eq!(table.len(), 0);
        assert_eq!(table.size(), 0);

        let transaction = table.transaction(Atom::from("memory local identity source"),
                                            true,
                                            false,
                                            1_234,
                                            5_678);
        assert!(transaction.is_writable());
        assert!(!transaction.is_concurrent_prepare());
        assert!(!transaction.is_concurrent_commit());
        assert!(!transaction.is_concurrent_rollback());
        assert!(transaction.is_enable_inherit_uid());
        assert_eq!(transaction.get_source().as_str(), "memory local identity source");
        assert_eq!(transaction.get_prepare_timeout(), 1_234);
        assert_eq!(transaction.get_commit_timeout(), 5_678);
        assert_eq!(transaction.get_status(), Transaction2PcStatus::Start);
        assert!(transaction.is_unit());
        assert!(!transaction.is_sequence());
        assert!(!transaction.is_tree());
        assert!(transaction.prev_item().is_none());
        assert!(transaction.next_item().is_none());
        assert_eq!(transaction.children_len(), 0);
        assert_eq!(transaction.to_children().count(), 0);
        assert_eq!(transaction.qos(), TableTrQos::ThreadSafe);
        assert!(block_on(transaction.init()).is_ok());

        let tid = Guid(101);
        let cid = Guid(102);
        let unused_prepare_uid = Guid(103);
        assert!(transaction.get_transaction_uid().is_none());
        assert!(transaction.get_commit_uid().is_none());
        assert!(transaction.get_prepare_uid().is_none());
        transaction.set_transaction_uid(tid.clone());
        transaction.set_commit_uid(cid.clone());
        transaction.set_prepare_uid(unused_prepare_uid);
        assert_eq!(transaction.get_transaction_uid(), Some(tid));
        assert_eq!(transaction.get_commit_uid(), Some(cid));
        assert!(transaction.get_prepare_uid().is_none());
        transaction.set_status(Transaction2PcStatus::Actioning);
        assert_eq!(transaction.get_status(), Transaction2PcStatus::Actioning);

        transaction.require_persistence();
        transaction.require_persistence();
        assert!(transaction.is_require_persistence());
        assert_eq!(transaction.qos(), TableTrQos::Safe);

        let read_only = table.transaction(Atom::from("memory local read only"),
                                          false,
                                          true,
                                          7,
                                          9);
        assert!(matches!(block_on(read_only.prepare()), Ok(None)));
        assert!(table.0.prepare.lock().is_empty());
    }

    /// 动作必须只修改私有 COW 根，同 Key 后写只保留最终动作，Memory delete 固定不返回旧值。
    #[test]
    fn test_memory_private_cow_and_final_action_contract() {
        let table = TestTable::new(Atom::from("memory_local_actions"), false);
        let retained_key = bon_usize(1);
        let deleted_key = bon_usize(2);
        let committed_value = bon_usize(10);
        let first_value = bon_usize(11);
        let final_value = bon_usize(12);
        table.0.root.lock().upsert(deleted_key.clone(), committed_value.clone(), false);

        let transaction = table.transaction(Atom::from("memory local actions source"),
                                            true,
                                            false,
                                            100,
                                            200);
        block_on(transaction.upsert(retained_key.clone(), first_value))
            .expect("first private upsert must succeed");
        assert_binary(block_on(transaction.query(retained_key.clone())),
                      Some(&bon_usize(11)),
                      "transaction must observe first private upsert");
        block_on(transaction.upsert(retained_key.clone(), final_value.clone()))
            .expect("final private upsert must succeed");
        let removed = block_on(transaction.delete(deleted_key.clone()))
            .expect("private delete must succeed");
        assert!(removed.is_none(), "Memory delete must not expose the old value");

        assert_binary(transaction.0.root_mut.lock().get(&retained_key).cloned(),
                      Some(&final_value),
                      "private root must contain final upsert");
        assert_binary(transaction.0.root_mut.lock().get(&deleted_key).cloned(),
                      None,
                      "private root must contain final delete");
        assert_binary(table.query_committed(&retained_key),
                      None,
                      "uncommitted upsert must not reach shared root");
        assert_binary(table.query_committed(&deleted_key),
                      Some(&committed_value),
                      "uncommitted delete must not reach shared root");

        let actions = transaction.0.actions.lock();
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions.get(&retained_key),
                         Some(KVActionLog::Write(Some(value)))
                         if value.as_ref() == final_value.as_ref()));
        assert!(matches!(actions.get(&deleted_key), Some(KVActionLog::Write(None))));
    }

    /// prepare 只编码最终写并转移完整动作所有权；rollback 必须释放预留且不发布私有根。
    #[test]
    fn test_memory_prepare_wal_ownership_and_rollback_contract() {
        let table = TestTable::new(Atom::from("memory_local_prepare"), true);
        let upsert_key = bon_usize(21);
        let delete_key = bon_usize(22);
        let read_key = bon_usize(23);
        let old_value = bon_usize(210);
        let new_value = bon_usize(211);
        table.0.root.lock().upsert(delete_key.clone(), old_value.clone(), false);

        let transaction = table.transaction(Atom::from("memory local prepare source"),
                                            true,
                                            true,
                                            300,
                                            400);
        let tid = Guid(201);
        transaction.set_transaction_uid(tid.clone());
        block_on(transaction.upsert(upsert_key.clone(), new_value.clone()))
            .expect("private upsert before prepare must succeed");
        block_on(transaction.delete(delete_key.clone()))
            .expect("private delete before prepare must succeed");
        assert!(block_on(transaction.query(read_key.clone())).is_none());

        let output = block_on(transaction.prepare_conflicts())
            .expect("prepare must succeed")
            .expect("persistent writes must produce a Memory WAL fragment");
        let (table_name, write_count, offset) =
            <TestTable as KVTable>::get_init_table_prepare_output(&output, 0);
        let (writes, end) =
            <TestTable as KVTable>::get_all_key_value_from_table_prepare_output(
                &output,
                &table_name,
                write_count,
                offset);
        assert_eq!(table_name.as_str(), "memory_local_prepare");
        assert_eq!(write_count, 2, "Read must not be encoded into the WAL fragment");
        assert_eq!(writes.len(), 2);
        assert_eq!(end, output.len(), "decoder must consume the complete fragment");
        assert!(writes.iter().any(|entry| {
            entry.key.as_ref() == upsert_key.as_ref()
                && entry.value.as_ref().map(Binary::as_ref) == Some(new_value.as_ref())
        }));
        assert!(writes.iter().any(|entry| {
            entry.key.as_ref() == delete_key.as_ref() && entry.value.is_none()
        }));

        assert!(transaction.0.actions.lock().is_empty(),
                "successful prepare must transfer transaction-local actions");
        {
            let prepared = table.0.prepare.lock();
            let item = prepared.get(&tid).expect("prepare map must reserve the root TID");
            assert_eq!(item.mode, PrepareMode::Ordinary);
            assert_eq!(item.actions.len(), 3, "prepared state must retain the Read dependency");
            assert!(matches!(item.actions.get(&read_key), Some(KVActionLog::Read)));
            assert!(matches!(item.actions.get(&delete_key), Some(KVActionLog::Write(None))));
        }
        assert_binary(table.query_committed(&upsert_key),
                      None,
                      "prepare must not publish the private upsert");
        assert_binary(table.query_committed(&delete_key),
                      Some(&old_value),
                      "prepare must not publish the private delete");

        block_on(transaction.rollback()).expect("rollback must release prepared state");
        assert!(table.0.prepare.lock().is_empty());
        assert_binary(table.query_committed(&upsert_key),
                      None,
                      "rollback must not publish private upsert");
        assert_binary(table.query_committed(&delete_key),
                      Some(&old_value),
                      "rollback must retain committed value");
    }

    /// 当前 dirty 分支以 persistence 区分值状态检查；该测试记录事实，不冻结最终隔离设计。
    #[test]
    fn test_memory_dirty_prepare_current_persistence_branch() {
        let table = TestTable::new(Atom::from("memory_local_dirty"), true);
        let key = bon_usize(31);
        let initial_value = bon_usize(310);
        let concurrent_value = bon_usize(311);
        let nonpersistent_value = bon_usize(312);
        let persistent_value = bon_usize(313);
        let later_value = bon_usize(314);
        table.0.root.lock().upsert(key.clone(), initial_value, false);

        let nonpersistent = table.transaction(Atom::from("memory local nonpersistent dirty"),
                                              true,
                                              false,
                                              500,
                                              600);
        nonpersistent.set_transaction_uid(Guid(301));
        block_on(nonpersistent.dirty_upsert(key.clone(), nonpersistent_value))
            .expect("nonpersistent dirty upsert must succeed locally");
        table.0.root.lock().upsert(key.clone(), concurrent_value, false);
        let error = block_on(nonpersistent.prepare_conflicts())
            .expect_err("nonpersistent dirty write currently checks committed value state");
        assert!(error.is_conflicts());
        assert!(table.0.prepare.lock().is_empty());

        let persistent = table.transaction(Atom::from("memory local persistent dirty"),
                                           true,
                                           true,
                                           700,
                                           800);
        let persistent_tid = Guid(302);
        persistent.set_transaction_uid(persistent_tid.clone());
        block_on(persistent.dirty_upsert(key.clone(), persistent_value))
            .expect("persistent dirty upsert must succeed locally");
        table.0.root.lock().upsert(key.clone(), later_value.clone(), false);
        assert!(block_on(persistent.prepare_conflicts())
            .expect("persistent dirty write currently skips committed value-state comparison")
            .is_some());
        assert!(table.0.prepare.lock().contains_key(&persistent_tid));
        assert_binary(table.query_committed(&key),
                      Some(&later_value),
                      "prepare must not publish persistent dirty value");
        block_on(persistent.rollback()).expect("persistent dirty rollback must succeed");
        assert!(table.0.prepare.lock().is_empty());
    }

    /// 受信 repair 必须直接得到最终数据状态、登记普通 prepared 项，并可幂等重放最终动作。
    #[test]
    fn test_memory_repair_final_state_and_local_idempotence() {
        let table = TestTable::new(Atom::from("memory_local_repair"), true);
        let upsert_key = bon_usize(41);
        let delete_key = bon_usize(42);
        let old_value = bon_usize(410);
        let repaired_value = bon_usize(411);
        table.0.root.lock().upsert(delete_key.clone(), old_value, false);

        let first = table.transaction(Atom::from("memory local repair first"),
                                      true,
                                      true,
                                      900,
                                      1_000);
        block_on(first.upsert(upsert_key.clone(), repaired_value.clone()))
            .expect("first repair upsert action must be staged");
        block_on(first.delete(delete_key.clone()))
            .expect("first repair delete action must be staged");
        let first_tid = Guid(401);
        first.prepare_repair(first_tid.clone());

        assert_binary(table.query_committed(&upsert_key),
                      Some(&repaired_value),
                      "repair must apply upsert directly");
        assert_binary(table.query_committed(&delete_key),
                      None,
                      "repair must apply delete directly");
        assert!(first.0.actions.lock().is_empty());
        {
            let prepared = table.0.prepare.lock();
            let item = prepared.get(&first_tid).expect("repair must register first TID");
            assert_eq!(item.mode, PrepareMode::Ordinary);
            assert_eq!(item.actions.len(), 2);
        }

        let second = table.transaction(Atom::from("memory local repair second"),
                                       true,
                                       true,
                                       1_100,
                                       1_200);
        block_on(second.upsert(upsert_key.clone(), repaired_value.clone()))
            .expect("second repair upsert action must be staged");
        block_on(second.delete(delete_key.clone()))
            .expect("second repair delete action must be staged");
        let second_tid = Guid(402);
        second.prepare_repair(second_tid.clone());

        assert_eq!(table.len(), 1);
        assert_binary(table.query_committed(&upsert_key),
                      Some(&repaired_value),
                      "repeated repair must retain final upsert");
        assert_binary(table.query_committed(&delete_key),
                      None,
                      "repeated repair must retain final delete");
        let mut prepared = table.0.prepare.lock();
        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared.remove(&first_tid).map(|item| item.mode),
                   Some(PrepareMode::Ordinary));
        assert_eq!(prepared.remove(&second_tid).map(|item| item.mode),
                   Some(PrepareMode::Ordinary));
        assert!(prepared.is_empty());
    }

    /// 事务必须单向保活表，事务释放后不得因表/prepare 反向引用形成强引用环。
    #[test]
    fn test_memory_transaction_table_arc_lifecycle_has_no_cycle() {
        let table = TestTable::new(Atom::from("memory_local_arc_lifecycle"), false);
        let weak = Arc::downgrade(&table.0);
        let transaction = table.transaction(Atom::from("memory local arc lifecycle source"),
                                            true,
                                            false,
                                            1_300,
                                            1_400);
        assert_eq!(Arc::strong_count(&table.0), 2);

        drop(table);
        assert!(weak.upgrade().is_some(), "transaction must keep its table owner alive");
        drop(transaction);
        assert!(weak.upgrade().is_none(), "dropping the last transaction must release the table");
    }
}
