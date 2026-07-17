//! 全局 Key 版本、每表 publication 门、快照租约和 TTL 回收。
//!
//! 本模块只保存版本证据，不保存表数据，也不执行 WAL 或数据文件 I/O。公开载荷用于
//! `query_with_version -> prepare_with_version -> commit_with_version` 协议；其余类型均为
//! `KVDBManager` 正常装配表时使用的 crate-private 实现。

use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result as IOResult};
use std::mem;
use std::sync::{Arc, Weak,
                atomic::{AtomicBool, AtomicU64, Ordering}};
use std::time::{Duration, Instant};

use async_channel::{Receiver, Sender, bounded};
use async_lock::RwLock;
use crossbeam_channel::{Receiver as SyncReceiver, Sender as SyncSender, TryRecvError,
                        unbounded as sync_unbounded};
use dashmap::{DashMap, mapref::entry::Entry};
use futures::{FutureExt, future::{Either, select}};
use parking_lot::Mutex;

use pi_async_rt::rt::{AsyncRuntime, multi_thread::MultiTaskRuntime};
use pi_atom::Atom;
use pi_guid::Guid;
use pi_hash::XHashMap;

use crate::{Binary, KVActionLog};

const NO_DEADLINE: u64 = u64::MAX;
const MAX_TICK: u64 = u64::MAX - 1;
const TTL_SCAN_BATCH_SIZE: usize = 256;

/// 一个 Key 的公开版本。
///
/// Upsert/Delete 都携带产生该版本的 transaction UID；首次观察使用事务管理器同一 Guid
/// 生成器独立分配 UID，但不表示发生过真实事务提交。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Version {
    /// 最新公开状态由插入或更新产生。
    Upsert(Guid),
    /// 最新公开状态由逻辑删除产生，或首次观察确认 Key 不存在。
    Delete(Guid),
}

/// 表、Key 和公开版本的联合载荷。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableKeyVersion {
    /// 表名。
    pub table: Atom,
    /// Key 的规范二进制编码。
    pub key: Binary,
    /// 与该 Table/Key 状态对应的公开版本。
    pub version: Version,
}

/// 完整冲突集合中的表和 Key。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableKey {
    /// 表名。
    pub table: Atom,
    /// 冲突 Key 的规范二进制编码。
    pub key: Binary,
}

/// 按公开协议要求对冲突集合执行原始字节排序和去重。
pub(crate) fn normalize_conflicts(mut conflicts: Vec<TableKey>) -> Vec<TableKey> {
    conflicts.sort_by(|left, right| {
        left
            .table
            .as_str()
            .as_bytes()
            .cmp(right.table.as_str().as_bytes())
            .then_with(|| left.key.as_ref().cmp(right.key.as_ref()))
    });
    conflicts.dedup_by(|left, right| {
        left.table.as_str().as_bytes() == right.table.as_str().as_bytes()
            && left.key.as_ref() == right.key.as_ref()
    });
    conflicts
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VersionSource {
    FirstObservation,
    CommittedWrite,
}

#[derive(Debug, Clone)]
pub(crate) struct VersionRecord {
    pub(crate) version: Version,
    pub(crate) source: VersionSource,
    pub(crate) revision: u64,
    deadline_tick: u64,
    generation: u64,
}

impl VersionRecord {
    fn exact_eq(&self, other: &Self) -> bool {
        self.version == other.version
            && self.source == other.source
            && self.revision == other.revision
            && self.deadline_tick == other.deadline_tick
            && self.generation == other.generation
    }
}

/// builder 中 TTL 配置换算后的单调毫秒参数。
#[derive(Debug, Clone, Copy)]
pub(crate) struct KeyVersionConfig {
    ttl_ticks: Option<u64>,
    poll_interval_ticks: u64,
}

impl KeyVersionConfig {
    /// 在数据库目录和文件副作用前校验并换算配置。
    pub(crate) fn new(ttl: Duration, poll_interval: Duration) -> IOResult<Self> {
        if ttl.is_zero() {
            return Ok(Self {
                ttl_ticks: None,
                poll_interval_ticks: 0,
            });
        }
        if poll_interval.is_zero() {
            return Err(Error::new(ErrorKind::InvalidInput,
                                  "Start database failed, reason: key version TTL is enabled but poll interval is zero"));
        }

        Ok(Self {
            ttl_ticks: Some(duration_to_ticks(ttl)),
            poll_interval_ticks: duration_to_ticks(poll_interval),
        })
    }

    fn ttl_ticks(&self) -> Option<u64> {
        self.ttl_ticks
    }

    fn poll_interval_ticks(&self) -> u64 {
        self.poll_interval_ticks
    }
}

fn duration_to_ticks(duration: Duration) -> u64 {
    // TTL 的最小单位是 1ms：非零 sub-ms 值按 1ms 处理，其余不足 1ms 的小数按契约忽略。
    // 历史 BUG-KV-TTL-001 由 deadline 基准向下取整导致；这里的配置量化不是缺陷且保持不变。
    let millis = duration.as_millis();
    if millis == 0 {
        1
    } else if millis >= MAX_TICK as u128 {
        MAX_TICK
    } else {
        millis as u64
    }
}

fn timeout_ticks(ticks: u64) -> usize {
    if ticks >= usize::MAX as u64 {
        usize::MAX
    } else {
        ticks as usize
    }
}

#[derive(Clone)]
pub(crate) struct KeyVersionRegistry(Arc<InnerKeyVersionRegistry>);

struct InnerKeyVersionRegistry {
    tables: DashMap<Atom, KeyVersions>,
    ttl_ticks: Option<u64>,
    poll_interval_ticks: u64,
    origin: Instant,
    shutdown_tx: Option<Sender<()>>,
}

impl KeyVersionRegistry {
    pub(crate) fn new(config: KeyVersionConfig) -> (Self, Option<Receiver<()>>) {
        let (shutdown_tx, shutdown_rx) = if config.ttl_ticks().is_some() {
            let (tx, rx) = bounded(1);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let inner = InnerKeyVersionRegistry {
            tables: DashMap::new(),
            ttl_ticks: config.ttl_ticks(),
            poll_interval_ticks: config.poll_interval_ticks(),
            origin: Instant::now(),
            shutdown_tx,
        };
        (Self(Arc::new(inner)), shutdown_rx)
    }

    pub(crate) fn create_table_versions(&self) -> KeyVersions {
        KeyVersions::new(Arc::downgrade(&self.0))
    }

    pub(crate) fn install(&self, table: Atom, versions: KeyVersions) {
        self.0.tables.insert(table, versions);
    }

    pub(crate) fn remove_exact(&self, table: &Atom, versions: &KeyVersions) {
        let _ = self
            .0
            .tables
            .remove_if(table, |_name, current| current.ptr_eq(versions));
    }

    /// 修复完成后清空外部不可见的恢复期版本记录，但保留单调 revision。
    pub(crate) fn clear_records(&self) {
        for entry in self.0.tables.iter() {
            entry.value().clear_records();
        }
    }

    /// 启动唯一固定周期 TTL 任务。TTL 关闭时 receiver 为 None，不创建任务。
    pub(crate) fn start_ttl_task(&self,
                                 rt: MultiTaskRuntime<()>,
                                 receiver: Option<Receiver<()>>) {
        let Some(receiver) = receiver else {
            return;
        };
        let weak = Arc::downgrade(&self.0);
        let task_rt = rt.clone();
        let _ = rt.spawn(async move {
            ttl_loop(task_rt, weak, receiver).await;
        });
    }
}

async fn ttl_loop(rt: MultiTaskRuntime<()>,
                  registry: Weak<InnerKeyVersionRegistry>,
                  receiver: Receiver<()>) {
    let Some(initial) = registry.upgrade() else {
        return;
    };
    let interval = initial.poll_interval_ticks;
    let ttl = initial.ttl_ticks.unwrap_or(0);
    drop(initial);

    log::info!(target: "pi_db::key_version_ttl",
               "Key version TTL task started, ttl_ms: {}, interval_ms: {}",
               ttl,
               interval);
    let mut round = 0u64;
    loop {
        let timeout = rt.timeout(timeout_ticks(interval)).fuse();
        let shutdown = receiver.recv().fuse();
        futures::pin_mut!(timeout, shutdown);
        match select(timeout, shutdown).await {
            Either::Left((_timeout_result, _shutdown_future)) => (),
            Either::Right((_shutdown_result, _timeout_future)) => break,
        }

        let Some(registry_ref) = registry.upgrade() else {
            break;
        };
        round = round.saturating_add(1);
        let statistics = collect_ttl_round(&rt, &registry_ref, round).await;
        drop(registry_ref);
        statistics.log();
    }
    log::info!(target: "pi_db::key_version_ttl",
               "Key version TTL task stopped, ttl_ms: {}, interval_ms: {}, completed_rounds: {}",
               ttl,
               interval,
               round);
}

#[derive(Default)]
struct TtlCollectStatistics {
    round: u64,
    ttl_ticks: u64,
    interval_ticks: u64,
    elapsed: Duration,
    registered_tables: usize,
    due_tables: usize,
    scanned_tables: usize,
    scanned_records: usize,
    due_candidates: usize,
    removed_records: usize,
    removed_first_observations: usize,
    removed_committed_writes: usize,
    snapshot_blocked: usize,
    stale_candidates: usize,
    records_before: usize,
    records_after: usize,
    batches: usize,
    yields: usize,
    next_deadline: u64,
}

impl TtlCollectStatistics {
    fn log(&self) {
        log::info!(target: "pi_db::key_version_ttl",
                   "Key version TTL round completed, round: {}, ttl_ms: {}, interval_ms: {}, elapsed_ms: {}, registered_tables: {}, due_tables: {}, scanned_tables: {}, scanned_records: {}, due_candidates: {}, removed_records: {}, removed_first_observations: {}, removed_committed_writes: {}, snapshot_blocked: {}, stale_candidates: {}, records_before: {}, records_after: {}, batches: {}, yields: {}, next_deadline_tick: {}",
                   self.round,
                   self.ttl_ticks,
                   self.interval_ticks,
                   self.elapsed.as_millis(),
                   self.registered_tables,
                   self.due_tables,
                   self.scanned_tables,
                   self.scanned_records,
                   self.due_candidates,
                   self.removed_records,
                   self.removed_first_observations,
                   self.removed_committed_writes,
                   self.snapshot_blocked,
                   self.stale_candidates,
                   self.records_before,
                   self.records_after,
                   self.batches,
                   self.yields,
                   self.next_deadline);
    }
}

async fn collect_ttl_round(rt: &MultiTaskRuntime<()>,
                           registry: &Arc<InnerKeyVersionRegistry>,
                           round: u64) -> TtlCollectStatistics {
    let started = Instant::now();
    let now = monotonic_tick(registry.origin);
    let mut statistics = TtlCollectStatistics {
        round,
        ttl_ticks: registry.ttl_ticks.unwrap_or(0),
        interval_ticks: registry.poll_interval_ticks,
        next_deadline: NO_DEADLINE,
        ..TtlCollectStatistics::default()
    };
    // 这是 TTL 唯一保留的 DashMap iterator：它只枚举通常远少于 Key 数的表级 registry，
    // 在同步表达式内克隆 owner 后立即释放全部分片 guard，且不与 DDL 写、Key Map 写或 await
    // 交叠。它仍可能短暂延迟同分片 DDL；当前合法协议不允许 DDL 与事务读写混用，若未来放宽
    // 该边界必须改用独立表索引。后续统计复用同一快照，避免本轮第二次枚举。验收见
    // docs/KEY_VERSION_TTL_FIFO_ACCEPTANCE.md#kv-ttl-fifo-outer-registry。
    let tables: Vec<KeyVersions> = registry
        .tables
        .iter()
        .map(|entry| entry.value().clone())
        .collect();
    statistics.registered_tables = tables.len();
    statistics.records_before = tables.iter().map(KeyVersions::len).sum();

    for versions in &tables {
        let earliest = versions.0.earliest_deadline.load(Ordering::Acquire);
        let blocked_changed = versions.0.has_blocked_expiry.load(Ordering::Acquire)
            && versions.0.lease_epoch.load(Ordering::Acquire)
                != versions.0.blocked_lease_epoch.load(Ordering::Acquire);
        if earliest > now && !blocked_changed {
            statistics.next_deadline = statistics.next_deadline.min(earliest);
            continue;
        }

        statistics.due_tables += 1;
        statistics.scanned_tables += 1;
        versions
            .collect_expired(rt, now, &mut statistics)
            .await;
        statistics.next_deadline = statistics
            .next_deadline
            .min(versions.0.earliest_deadline.load(Ordering::Acquire));
    }

    statistics.records_after = tables.iter().map(KeyVersions::len).sum();
    statistics.elapsed = started.elapsed();
    statistics
}

fn monotonic_tick(origin: Instant) -> u64 {
    let elapsed = origin.elapsed().as_millis();
    if elapsed >= MAX_TICK as u128 {
        MAX_TICK
    } else {
        elapsed as u64
    }
}

fn deadline_tick(elapsed: Duration, ttl_ticks: u64) -> u64 {
    let elapsed_millis = elapsed.as_millis();
    let rounded_millis = if elapsed.subsec_nanos() % 1_000_000 == 0 {
        elapsed_millis
    } else {
        elapsed_millis.saturating_add(1)
    };
    let base_tick = if rounded_millis >= MAX_TICK as u128 {
        MAX_TICK
    } else {
        rounded_millis as u64
    };
    base_tick.saturating_add(ttl_ticks).min(MAX_TICK)
}

/// 每表 TTL 活动 Key 的非等待 FIFO 索引。
///
/// DashMap 仍是版本状态的唯一事实来源。索引只保存 O(1) 的 `Binary` 共享 owner，使 scanner
/// 可以分批执行单 Key 点读而无需持有 DashMap 分片 iterator。sender/receiver 由同一对象共同
/// 持有，因此 unbounded channel 在对象存活期间不会断开，也不会因容量产生 Full。
/// 设计、竞态和生命周期证据见
/// `docs/KEY_VERSION_TTL_FIFO_ACCEPTANCE.md#kv-ttl-fifo-algorithm`。
struct TtlKeyIndex {
    sender: SyncSender<Binary>,
    receiver: SyncReceiver<Binary>,
}

impl TtlKeyIndex {
    fn new() -> Self {
        let (sender, receiver) = sync_unbounded();
        Self { sender, receiver }
    }

    fn len(&self) -> usize {
        self.receiver.len()
    }

    fn push(&self, key: Binary) {
        if self.sender.try_send(key).is_err() {
            unreachable!("TTL key index channel disconnected while its receiver is alive");
        }
    }

    fn push_batch(&self, keys: Vec<Binary>) {
        for key in keys {
            self.push(key);
        }
    }

    fn take_batch(&self, limit: usize) -> Vec<Binary> {
        let mut keys = Vec::with_capacity(limit);
        for _ in 0..limit {
            match self.receiver.try_recv() {
                Ok(key) => keys.push(key),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    unreachable!("TTL key index channel disconnected while its sender is alive");
                },
            }
        }
        keys
    }

    fn clear(&self) {
        loop {
            match self.receiver.try_recv() {
                Ok(_) => (),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    unreachable!("TTL key index channel disconnected while its sender is alive");
                },
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct KeyVersions(Arc<InnerKeyVersions>);

/// 单个已注册表实例的版本状态。
///
/// `versions` 是 `(Binary -> VersionRecord)` 的唯一事实来源；`ttl_keys` 只是避免遍历
/// DashMap 的活动 Key 索引。`publication` 只在线性化表数据提交与版本提交，不保护 TTL：TTL
/// 依赖 DashMap 单 Key 原子操作和完整记录签名安全竞态。`completed_revision` 与
/// `active_snapshots` 共同保存普通事务识别同值写和 ABA 所需的最小历史窗口。
///
/// 强引用方向固定为 registry/table/transaction -> KeyVersions，反向只允许 Weak registry；
/// TTL task 同样只持 Weak registry，禁止形成数据库或表无法释放的 Arc 环。
struct InnerKeyVersions {
    versions: DashMap<Binary, VersionRecord>,
    ttl_keys: TtlKeyIndex,
    pub(crate) publication: RwLock<()>,
    completed_revision: AtomicU64,
    active_snapshots: Mutex<BTreeMap<u64, usize>>,
    lease_epoch: AtomicU64,
    earliest_deadline: AtomicU64,
    has_blocked_expiry: AtomicBool,
    blocked_lease_epoch: AtomicU64,
    registry: Weak<InnerKeyVersionRegistry>,
}

impl KeyVersions {
    fn new(registry: Weak<InnerKeyVersionRegistry>) -> Self {
        Self(Arc::new(InnerKeyVersions {
            versions: DashMap::new(),
            ttl_keys: TtlKeyIndex::new(),
            publication: RwLock::new(()),
            completed_revision: AtomicU64::new(0),
            active_snapshots: Mutex::new(BTreeMap::new()),
            lease_epoch: AtomicU64::new(0),
            earliest_deadline: AtomicU64::new(NO_DEADLINE),
            has_blocked_expiry: AtomicBool::new(false),
            blocked_lease_epoch: AtomicU64::new(0),
            registry,
        }))
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// 返回每表 publication 门；调用方必须遵守 publication -> prepare 的单向锁序。
    pub(crate) fn publication(&self) -> &RwLock<()> {
        &self.0.publication
    }

    pub(crate) fn len(&self) -> usize {
        self.0.versions.len()
    }

    pub(crate) fn current(&self, key: &Binary) -> Option<VersionRecord> {
        self.0.versions.get(key).map(|record| record.clone())
    }

    pub(crate) fn current_version(&self, key: &Binary) -> Option<Version> {
        self.0
            .versions
            .get(key)
            .map(|record| record.version.clone())
    }

    /// 返回当前逻辑值对应的现有版本，或为首次观察原子创建一个版本。
    ///
    /// 调用方必须持有本表 publication read，使表 commit 不能在“读数据 -> 取版本”之间穿入。
    /// TTL 不获取 publication，仍可能在本调用前后删除版本；Vacant entry 是并发首次观察的唯一
    /// 线性化点，因此返回的新版本仍与调用方已经读取的同一逻辑值一致。版本在方法返回后立即
    /// 到期只会使后续 prepare 产生保守冲突，不会把旧值与新提交版本错误配对。
    ///
    /// 命中路径 O(1) 平均时间且不分配 Guid；缺失路径分配一个 Guid、一个记录和至多一个 FIFO
    /// token。方法不 await，不获取 prepare/root/cache 锁，也不执行 WAL 或数据文件 I/O。
    pub(crate) fn first_observation<F>(&self,
                                       key: Binary,
                                       exists: bool,
                                       alloc_uid: F) -> Version
        where F: FnOnce() -> Guid
    {
        match self.0.versions.entry(key) {
            Entry::Occupied(entry) => entry.get().version.clone(),
            Entry::Vacant(entry) => {
                let version = if exists {
                    Version::Upsert(alloc_uid())
                } else {
                    Version::Delete(alloc_uid())
                };
                let deadline = self.next_deadline();
                let record = VersionRecord {
                    version: version.clone(),
                    source: VersionSource::FirstObservation,
                    revision: self.0.completed_revision.load(Ordering::Acquire),
                    deadline_tick: deadline,
                    generation: 1,
                };
                let ttl_key = if deadline == NO_DEADLINE {
                    None
                } else {
                    Some(entry.key().clone())
                };
                // 必须先释放 DashMap entry guard，再执行索引操作；scanner 也从不反向重叠两者。
                drop(entry.insert(record));
                if let Some(ttl_key) = ttl_key {
                    self.0.ttl_keys.push(ttl_key);
                }
                self.register_deadline(deadline);
                version
            },
        }
    }

    /// 租用当前已完成 revision，防止 TTL 提前删除本事务识别后续提交所需的版本记录。
    ///
    /// 调用方必须同时持有该表的数据 root/cache guard：commit 在相同 guard 内先发布数据和版本，
    /// 再 Release-store 新 revision，因此“数据快照 + revision + 活跃租约”不会被一次 commit
    /// 撕裂。不得在 publication/prepare 的反向锁序中调用本方法；内部仅短暂获取
    /// `active_snapshots` 同步 mutex，不 await、不执行 I/O。
    ///
    /// 返回 lease 可跨线程移动，显式 release 与 Drop 均幂等。活跃计数按 revision 聚合，创建和
    /// 释放平均 O(log r)，其中 r 是当前活跃 revision 数，而不是 Key 数。
    pub(crate) fn lease_current(&self) -> SnapshotLease {
        let revision = self.0.completed_revision.load(Ordering::Acquire);
        let mut active = self.0.active_snapshots.lock();
        let count = active.entry(revision).or_insert(0);
        *count = count.saturating_add(1);
        drop(active);
        SnapshotLease {
            versions: self.clone(),
            revision,
            released: AtomicBool::new(false),
        }
    }

    /// Acquire-load 最近一次完整发布的数据/版本 revision。
    pub(crate) fn completed_revision(&self) -> u64 {
        self.0.completed_revision.load(Ordering::Acquire)
    }

    /// 计算下一 revision；只能在本表 publication write 内用于一次实际写提交。
    ///
    /// 返回 None 表示 u64 空间耗尽。调用方必须在修改数据前把它提升为不可 rollback 的 Fatal，
    /// 绝不能回绕或复用 revision。
    pub(crate) fn checked_next_revision(&self) -> Option<u64> {
        self.completed_revision().checked_add(1)
    }

    /// 在本表全部数据和写 Key 版本完成发布后推进 completed revision。
    ///
    /// 调用方必须仍持有 publication write 和对应数据 root/cache guard。Release-store 与事务创建
    /// 侧的 Acquire-load 配对；本方法不校验单调性，因为唯一写者由 publication write 保证。
    pub(crate) fn complete_revision(&self, revision: u64) {
        self.0.completed_revision.store(revision, Ordering::Release);
    }

    /// 判断该 Key 是否存在晚于事务快照的真实提交版本。
    ///
    /// FirstObservation 不表示写提交，不能制造普通事务冲突；CommittedWrite 的最新记录足以代表
    /// 此 Key 在快照后至少发生过一次写。调用方持 publication read 完成 prepare 复核；活跃 lease
    /// 同时保证 TTL 不会删除 `revision > snapshot_revision` 的必要证据。
    pub(crate) fn has_committed_after(&self,
                                      key: &Binary,
                                      snapshot_revision: u64) -> bool {
        if let Some(record) = self.0.versions.get(key) {
            record.source == VersionSource::CommittedWrite
                && record.revision > snapshot_revision
        } else {
            false
        }
    }

    /// 发布本事务对一个 Key 的最终版本，并返回只描述本事务的回执项。
    ///
    /// 调用方必须持有本表 publication write 和数据 root/cache guard，并传入当前根事务的 TID
    /// 与本表本次提交唯一 revision。`Some` 生成 Upsert，`None` 生成 Delete；不会读取提交后的
    /// “全局最新版本”，所以随后其它事务覆盖该 Key 也不会改变已返回回执。
    ///
    /// TTL 可并发 exact-remove，但 DashMap 分片原子操作保证两种结果都保有有效 token：删除先发生
    /// 时本次 insert 视为首次并入队，更新先发生时 scanner 看到签名变化并归还原 token。方法不
    /// await、不获取 prepare 锁、不执行 I/O；平均 O(1)，首次记录额外创建一个 O(1) Binary owner。
    pub(crate) fn publish(&self,
                          table: Atom,
                          key: Binary,
                          value: Option<&Binary>,
                          transaction_uid: Guid,
                          revision: u64) -> TableKeyVersion {
        let version = if value.is_some() {
            Version::Upsert(transaction_uid)
        } else {
            Version::Delete(transaction_uid)
        };
        let deadline = self.next_deadline();
        let generation = self
            .0
            .versions
            .get(&key)
            .map(|record| record.generation.saturating_add(1))
            .unwrap_or(1);
        let inserted = self.0.versions.insert(key.clone(), VersionRecord {
            version: version.clone(),
            source: VersionSource::CommittedWrite,
            revision,
            deadline_tick: deadline,
            generation,
        }).is_none();
        // 已有 Key 的唯一 token 可能正在 scanner 本地批次中；只有首次插入才创建新 token。
        if inserted && deadline != NO_DEADLINE {
            self.0.ttl_keys.push(key.clone());
        }
        self.register_deadline(deadline);
        TableKeyVersion {
            table,
            key,
            version,
        }
    }

    pub(crate) fn clear_records(&self) {
        // 恢复期清理发生在 TTL task 启动前。先清索引再清 Map，即使未来出现并发插入也只可能
        // 留下可自清理的 stale token，不会留下没有 token 的活动记录。
        self.0.ttl_keys.clear();
        self.0.versions.clear();
        self.0.earliest_deadline.store(NO_DEADLINE, Ordering::Release);
        self.0.has_blocked_expiry.store(false, Ordering::Release);
        self.0.blocked_lease_epoch.store(
            self.0.lease_epoch.load(Ordering::Acquire),
            Ordering::Release);
    }

    fn next_deadline(&self) -> u64 {
        let Some(registry) = self.0.registry.upgrade() else {
            return NO_DEADLINE;
        };
        let Some(ttl) = registry.ttl_ticks else {
            return NO_DEADLINE;
        };
        // scanner 的当前 tick 向下取整，因此 deadline 基准必须向上取整；否则版本可能比
        // 量化后的有效 TTL 提前不足 1ms 淘汰。这里只改变时间边界，不改变 TTL 的 1ms
        // 量化契约、扫描比较或版本并发控制。证据见 docs/KEY_VERSION_TTL_EARLY_EXPIRY_BUG.md。
        deadline_tick(registry.origin.elapsed(), ttl)
    }

    fn register_deadline(&self, deadline: u64) {
        if deadline != NO_DEADLINE {
            self.0.earliest_deadline.fetch_min(deadline, Ordering::AcqRel);
        }
    }

    async fn collect_expired(&self,
                             rt: &MultiTaskRuntime<()>,
                             now: u64,
                             statistics: &mut TtlCollectStatistics) {
        self.0.earliest_deadline.swap(NO_DEADLINE, Ordering::AcqRel);
        self.0.has_blocked_expiry.store(false, Ordering::Release);
        let scan_epoch = self.0.lease_epoch.load(Ordering::Acquire);
        // 固定本轮开始时的 token 数量。FIFO 中尚未处理的旧 token 始终位于并发新插入和本轮
        // 重新入队 token 之前，因此只消费该固定数量即可把后两类工作严格推迟到后续轮次。
        let mut remaining = self.0.ttl_keys.len();
        let mut blocked = false;

        while remaining > 0 {
            let batch = self
                .0
                .ttl_keys
                .take_batch(remaining.min(TTL_SCAN_BATCH_SIZE));
            if batch.is_empty() {
                break;
            }
            remaining -= batch.len();
            statistics.scanned_records += batch.len();
            statistics.batches += 1;
            let mut retained = Vec::with_capacity(batch.len());
            for key in batch {
                let Some(candidate) = self.current(&key) else {
                    continue;
                };
                if candidate.deadline_tick > now {
                    self.register_deadline(candidate.deadline_tick);
                    retained.push(key);
                    continue;
                }
                statistics.due_candidates += 1;

                if candidate.source == VersionSource::CommittedWrite {
                    let min_active = self
                        .0
                        .active_snapshots
                        .lock()
                        .keys()
                        .next()
                        .copied();
                    if min_active
                        .map(|revision| candidate.revision > revision)
                        .unwrap_or(false) {
                        statistics.snapshot_blocked += 1;
                        blocked = true;
                        retained.push(key);
                        continue;
                    }
                }

                let removed = self
                    .0
                    .versions
                    .remove_if(&key, |_key, current| current.exact_eq(&candidate));
                if removed.is_some() {
                    statistics.removed_records += 1;
                    match candidate.source {
                        VersionSource::FirstObservation => {
                            statistics.removed_first_observations += 1;
                        },
                        VersionSource::CommittedWrite => {
                            statistics.removed_committed_writes += 1;
                        },
                    }
                } else {
                    statistics.stale_candidates += 1;
                    if let Some(current) = self.current(&key) {
                        if current.deadline_tick > now
                            || current.source == VersionSource::FirstObservation {
                            self.register_deadline(current.deadline_tick);
                        } else {
                            let min_active = self
                                .0
                                .active_snapshots
                                .lock()
                                .keys()
                                .next()
                                .copied();
                            if min_active
                                .map(|revision| current.revision > revision)
                                .unwrap_or(false) {
                                statistics.snapshot_blocked += 1;
                                blocked = true;
                            } else {
                                self.register_deadline(current.deadline_tick);
                            }
                        }
                        retained.push(key);
                    }
                }
            }
            // 不允许在 token 临时离开索引时 await；先归还全部保留 Key，再主动让出 runtime。
            self.0.ttl_keys.push_batch(retained);
            rt.timeout(0).await;
            statistics.yields += 1;
        }

        if blocked {
            self.0.blocked_lease_epoch.store(scan_epoch, Ordering::Release);
            self.0.has_blocked_expiry.store(true, Ordering::Release);
        }

        // 插入方先入队再 fetch_min，已有记录更新也始终 fetch_min；因此 swap 前后的并发写要么
        // 由本轮 token 重新登记，要么在 swap 后直接登记，无需阻塞写入的 DashMap 全表复扫。
    }
}

/// 一个表事务的数据快照 revision 租约。
///
/// lease 不持有 root/cache guard，也不固定表数据本身；COW 数据根由表事务单独拥有。它只阻止
/// TTL 删除 `CommittedWrite.revision` 晚于本 revision 的记录。`released` 使 commit、rollback
/// 与 Drop 的重复清理收敛为一次，避免活跃计数下溢。
pub(crate) struct SnapshotLease {
    versions: KeyVersions,
    revision: u64,
    released: AtomicBool,
}

impl SnapshotLease {
    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }

    /// 幂等释放活跃 revision，并推进 lease_epoch 以唤醒被长事务阻塞的后续 TTL 轮询。
    ///
    /// 方法短暂获取同步 mutex，不 await。先更新活跃集合再推进 epoch，保证 scanner 即使与释放
    /// 交错，也只会至多多等待一个固定轮询间隔，不会永久遗失到期记录。
    pub(crate) fn release(&self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut active = self.versions.0.active_snapshots.lock();
        if let Some(count) = active.get_mut(&self.revision) {
            if *count <= 1 {
                active.remove(&self.revision);
            } else {
                *count -= 1;
            }
        }
        drop(active);
        self.versions.0.lease_epoch.fetch_add(1, Ordering::AcqRel);
    }
}

impl Drop for SnapshotLease {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrepareMode {
    Ordinary,
    Versioned,
}

pub(crate) struct PreparedActions {
    pub(crate) mode: PrepareMode,
    pub(crate) actions: XHashMap<Binary, KVActionLog>,
}

/// 版本化根事务的共享提交回执汇聚器。
///
/// 每个子表只在自己的 publication write 内追加本事务最终写集合；根事务在整棵树 commit 成功
/// 后一次性 take。内部 mutex 仅保护短 Vec 操作，不与其它表 publication 形成反向锁序，不执行
/// await 或 I/O。普通 commit 不安装本对象，因此不承担公开 Vec 的收集成本。
#[derive(Clone)]
pub(crate) struct VersionReceipt(Arc<Mutex<Vec<TableKeyVersion>>>);

impl VersionReceipt {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    pub(crate) fn append(&self, mut versions: Vec<TableKeyVersion>) {
        self.0.lock().append(&mut versions);
    }

    pub(crate) fn take(&self) -> Vec<TableKeyVersion> {
        mem::take(&mut *self.0.lock())
    }

    pub(crate) fn clear(&self) {
        self.0.lock().clear();
    }
}

/// 一个由 `KVDBManager` 装配的表事务所需的版本协议上下文。
///
/// `versions` 绑定精确表实例；`snapshot` 固定事务创建时 revision；`expected` 是外部真实读缓存
/// 提交的显式版本；`mode` 决定 prepare 预留兼容矩阵；`receipt` 只在版本 API 族存在。上下文随
/// 表事务 Arc 存活，显式终结遗漏时 SnapshotLease 的 Drop 仍负责最终资源释放。
pub(crate) struct TableVersionContext {
    versions: KeyVersions,
    snapshot: SnapshotLease,
    mode: PrepareMode,
    expected: XHashMap<Binary, Version>,
    receipt: Option<VersionReceipt>,
}

impl TableVersionContext {
    pub(crate) fn new(versions: KeyVersions,
                      snapshot: SnapshotLease,
                      mode: PrepareMode,
                      expected: XHashMap<Binary, Version>,
                      receipt: Option<VersionReceipt>) -> Self {
        Self {
            versions,
            snapshot,
            mode,
            expected,
            receipt,
        }
    }

    pub(crate) fn versions(&self) -> &KeyVersions {
        &self.versions
    }

    pub(crate) fn snapshot_revision(&self) -> u64 {
        self.snapshot.revision()
    }

    pub(crate) fn mode(&self) -> PrepareMode {
        self.mode
    }

    pub(crate) fn expected(&self) -> &XHashMap<Binary, Version> {
        &self.expected
    }

    pub(crate) fn receipt(&self) -> Option<&VersionReceipt> {
        self.receipt.as_ref()
    }

    pub(crate) fn release_snapshot(&self) {
        self.snapshot.release();
    }
}

/// 判断两个已登记动作在指定 prepare 模式下是否互斥。
pub(crate) fn prepared_actions_conflict(existing_mode: PrepareMode,
                                         existing: &KVActionLog,
                                         current_mode: PrepareMode,
                                         current: &KVActionLog) -> bool {
    if existing_mode == PrepareMode::Versioned || current_mode == PrepareMode::Versioned {
        return !matches!((existing, current), (KVActionLog::Read, KVActionLog::Read));
    }

    match existing {
        KVActionLog::Read => matches!(current, KVActionLog::Write(_)),
        KVActionLog::DirtyWrite(_) => false,
        KVActionLog::Write(_) => !matches!(current, KVActionLog::DirtyWrite(_)),
    }
}

/// 严格比较两个逻辑值状态；Missing 与任何 value 都不相等。
pub(crate) fn binary_state_equal(left: Option<&Binary>, right: Option<&Binary>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => Binary::binary_equal(left, right),
        _ => false,
    }
}

/// 判断指定动作是否与任一已登记事务的同 Key 动作冲突。
pub(crate) fn has_prepared_conflict(prepare: &XHashMap<Guid, PreparedActions>,
                                    key: &Binary,
                                    mode: PrepareMode,
                                    action: &KVActionLog) -> bool {
    prepare.values().any(|prepared| {
        prepared
            .actions
            .get(key)
            .map(|existing| prepared_actions_conflict(prepared.mode,
                                                      existing,
                                                      mode,
                                                      action))
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet,
              sync::{Arc, atomic::Ordering},
              thread,
              time::Duration};

    use pi_atom::Atom;
    use pi_bon::{Encode, WriteBuffer};
    use pi_guid::Guid;

    use crate::Binary;

    use super::{KeyVersionConfig,
                KeyVersionRegistry,
                MAX_TICK,
                TTL_SCAN_BATCH_SIZE,
                TtlKeyIndex,
                deadline_tick,
                duration_to_ticks};

    /// TTL 使用 1ms 最小单位；非零 sub-ms 值提升到最小单位，其余小数直接忽略。
    ///
    /// 该测试固定配置量化契约，不是 `BUG-KV-TTL-001` 的红证据；提前淘汰由独立真实 target
    /// 按量化后的有效 TTL 验证。
    #[test]
    fn test_duration_to_ticks_uses_one_millisecond_granularity() {
        assert_eq!(duration_to_ticks(Duration::from_nanos(1)), 1);
        assert_eq!(duration_to_ticks(Duration::from_micros(999)), 1);
        assert_eq!(duration_to_ticks(Duration::from_millis(1)), 1);
        assert_eq!(duration_to_ticks(Duration::from_micros(1_999)), 1);
        assert_eq!(duration_to_ticks(Duration::from_millis(2)), 2);
    }

    /// deadline 基准只在存在 sub-ms 余数时向上取整，并对极大时间和 TTL 饱和。
    #[test]
    fn test_deadline_tick_never_shortens_effective_ttl() {
        assert_eq!(deadline_tick(Duration::from_millis(10), 20), 30);
        assert_eq!(deadline_tick(
            Duration::from_millis(10) + Duration::from_nanos(1), 20), 31);
        assert_eq!(deadline_tick(Duration::from_micros(10_999), 20), 31);
        assert_eq!(deadline_tick(Duration::from_millis(MAX_TICK - 1), 20), MAX_TICK);
        assert_eq!(deadline_tick(Duration::from_millis(MAX_TICK), MAX_TICK), MAX_TICK);
    }

    /// scanner 固定轮次起始 token 数后，本轮重入队和并发新增 token 必须留给下一轮。
    #[test]
    fn test_ttl_key_index_round_snapshot_defers_requeued_and_new_tokens() {
        let index = TtlKeyIndex::new();
        index.push(binary_from_u32(1));
        index.push(binary_from_u32(2));
        index.push(binary_from_u32(3));

        let mut remaining = index.len();
        let first = index.take_batch(2);
        remaining -= first.len();
        assert_eq!(binary_values(&first), vec![1, 2]);

        index.push(first[0].clone());
        index.push(binary_from_u32(4));
        let rest_of_round = index.take_batch(remaining);
        assert_eq!(binary_values(&rest_of_round), vec![3]);

        let deferred = index.take_batch(index.len());
        assert_eq!(binary_values(&deferred), vec![1, 4]);
        assert_eq!(index.len(), 0);
    }

    /// 每次取出至多一个 scanner 批次，不会因大表 token 数改变固定上限。
    #[test]
    fn test_ttl_key_index_honors_batch_limit() {
        let index = TtlKeyIndex::new();
        for value in 0..(TTL_SCAN_BATCH_SIZE + 44) {
            index.push(binary_from_u32(value as u32));
        }

        let first = index.take_batch(TTL_SCAN_BATCH_SIZE);
        assert_eq!(first.len(), TTL_SCAN_BATCH_SIZE);
        assert_eq!(index.len(), 44);
        let second = index.take_batch(TTL_SCAN_BATCH_SIZE);
        assert_eq!(second.len(), 44);
        assert_eq!(index.len(), 0);
    }

    /// 多线程生产者只能增加各自的唯一 token；单 scanner 必须无丢失、无重复地全部取得。
    #[test]
    fn test_ttl_key_index_accepts_concurrent_unique_producers() {
        const PRODUCERS: usize = 4;
        const TOKENS_PER_PRODUCER: usize = 128;

        let index = Arc::new(TtlKeyIndex::new());
        thread::scope(|scope| {
            for producer in 0..PRODUCERS {
                let index = index.clone();
                scope.spawn(move || {
                    for offset in 0..TOKENS_PER_PRODUCER {
                        let value = producer * TOKENS_PER_PRODUCER + offset;
                        index.push(binary_from_u32(value as u32));
                    }
                });
            }
        });

        let tokens = index.take_batch(index.len());
        assert_eq!(tokens.len(), PRODUCERS * TOKENS_PER_PRODUCER);
        let values: BTreeSet<u32> = binary_values(&tokens).into_iter().collect();
        assert_eq!(values.len(), PRODUCERS * TOKENS_PER_PRODUCER);
        assert_eq!(values.first(), Some(&0));
        assert_eq!(values.last(), Some(&((PRODUCERS * TOKENS_PER_PRODUCER - 1) as u32)));
        assert_eq!(index.len(), 0);
    }

    /// 首次观察创建唯一 token，cache hit 和已有记录 publication 不重复入队；TTL 精确删除后
    /// publication 重建记录时必须重新创建 token。
    #[test]
    fn test_key_versions_maintains_one_token_per_current_record() {
        let config = KeyVersionConfig::new(Duration::from_secs(1),
                                           Duration::from_millis(10)).unwrap();
        let (registry, _shutdown) = KeyVersionRegistry::new(config);
        let versions = registry.create_table_versions();
        let key = encode_usize(7);
        let value = binary_from_u32(70);

        let first = versions.first_observation(key.clone(), false, || Guid(1));
        assert_eq!(index_len(&versions), 1);
        let cached = versions.first_observation(key.clone(), false, || {
            panic!("cache hit must not allocate another first-observation Guid")
        });
        assert_eq!(cached, first);
        assert_eq!(index_len(&versions), 1);

        let _ = versions.publish(Atom::from("ttl-unit"),
                                 key.clone(),
                                 Some(&value),
                                 Guid(2),
                                 1);
        assert_eq!(index_len(&versions), 1);

        let token = versions.0.ttl_keys.take_batch(1);
        assert_eq!(token.len(), 1);
        assert_eq!(token[0].as_ref(), key.as_ref());
        let candidate = versions.current(&key).unwrap();
        assert!(versions
            .0
            .versions
            .remove_if(&key, |_key, current| current.exact_eq(&candidate))
            .is_some());
        assert_eq!(index_len(&versions), 0);

        let _ = versions.publish(Atom::from("ttl-unit"),
                                 key.clone(),
                                 None,
                                 Guid(3),
                                 2);
        assert_eq!(index_len(&versions), 1);
    }

    /// 清空索引必须释放 token 持有的 Binary payload owner，不能把表或 Key 生命周期永久延长。
    #[test]
    fn test_ttl_key_index_clear_releases_binary_owner() {
        let index = TtlKeyIndex::new();
        let key = binary_from_u32(99);
        let payload = Arc::downgrade(&key.0);
        index.push(key.clone());
        drop(key);
        assert!(payload.upgrade().is_some());

        index.clear();
        assert!(payload.upgrade().is_none());
        assert_eq!(index.len(), 0);
    }

    /// registry 精确移除表后，`KeyVersions` 与 FIFO/Map 共同持有的 Key payload 必须随最后
    /// 一个表 owner 一起释放；内部只有指向 registry 的 Weak，不得形成引用环。
    #[test]
    fn test_key_versions_drop_releases_table_and_binary_owners() {
        let config = KeyVersionConfig::new(Duration::from_secs(1),
                                           Duration::from_millis(10)).unwrap();
        let (registry, _shutdown) = KeyVersionRegistry::new(config);
        let versions = registry.create_table_versions();
        let versions_owner = Arc::downgrade(&versions.0);
        let table = Atom::from("ttl-drop-unit");
        let key = encode_usize(101);
        let payload = Arc::downgrade(&key.0);

        let _ = versions.first_observation(key.clone(), false, || Guid(1));
        assert_eq!(index_len(&versions), 1);
        registry.install(table.clone(), versions.clone());
        drop(key);
        assert!(versions_owner.upgrade().is_some());
        assert!(payload.upgrade().is_some());

        registry.remove_exact(&table, &versions);
        assert!(registry.0.tables.get(&table).is_none());
        drop(versions);

        assert!(versions_owner.upgrade().is_none());
        assert!(payload.upgrade().is_none());
    }

    /// 显式 commit/rollback 清理与 Drop 后备清理必须共享一次性门，不能重复递减活跃快照。
    #[test]
    fn test_snapshot_lease_release_is_idempotent() {
        let config = KeyVersionConfig::new(Duration::ZERO, Duration::ZERO).unwrap();
        let (registry, _shutdown) = KeyVersionRegistry::new(config);
        let versions = registry.create_table_versions();
        let lease = versions.lease_current();

        assert_eq!(lease.revision(), 0);
        assert_eq!(versions.0.active_snapshots.lock().get(&0), Some(&1));
        lease.release();
        assert!(versions.0.active_snapshots.lock().is_empty());
        assert_eq!(versions.0.lease_epoch.load(Ordering::Acquire), 1);

        lease.release();
        drop(lease);
        assert!(versions.0.active_snapshots.lock().is_empty());
        assert_eq!(versions.0.lease_epoch.load(Ordering::Acquire), 1);
    }

    /// revision 空间耗尽必须显式返回 None，不能回绕为 0 并破坏 ABA 判定。
    #[test]
    fn test_next_revision_never_wraps() {
        let config = KeyVersionConfig::new(Duration::ZERO, Duration::ZERO).unwrap();
        let (registry, _shutdown) = KeyVersionRegistry::new(config);
        let versions = registry.create_table_versions();

        versions.0.completed_revision.store(u64::MAX - 1, Ordering::Release);
        assert_eq!(versions.checked_next_revision(), Some(u64::MAX));
        versions.complete_revision(u64::MAX);
        assert_eq!(versions.completed_revision(), u64::MAX);
        assert_eq!(versions.checked_next_revision(), None);
    }

    fn index_len(versions: &super::KeyVersions) -> usize {
        versions.0.ttl_keys.len()
    }

    fn binary_from_u32(value: u32) -> Binary {
        Binary::new(value.to_le_bytes().to_vec())
    }

    fn encode_usize(value: usize) -> Binary {
        let mut buffer = WriteBuffer::new();
        value.encode(&mut buffer);
        Binary::new(buffer.bytes)
    }

    fn binary_values(tokens: &[Binary]) -> Vec<u32> {
        tokens
            .iter()
            .map(|token| u32::from_le_bytes(token.as_ref().try_into().unwrap()))
            .collect()
    }
}
