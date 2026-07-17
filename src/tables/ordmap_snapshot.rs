//! `pi_ordmap` 创建时快照的私有所有权适配层。
//!
//! `pi_ordmap 0.5.0::IterTree` 只保存树节点裸指针，不保活生成 iterator 的 COW 根；其
//! `Iter<'a>` 实现也没有把 `'a` 绑定到 `&self`。本模块把 iterator 与完全相同的根 owner
//! 封装在一起，只向表实现返回克隆后的 owned 数据，避免引用或伪造 lifetime 逃逸。
//!
//! 该适配层只修复 `pi_db` 的合法流生命周期，不扩展公开 API，也不替代后续对
//! `pi_ordmap` 公共 lifetime/`Send` 边界的独立审计。冻结设计和验收矩阵见
//! `docs/ITERATOR_SNAPSHOT_FIX_PLAN.md#iter-fix-plan-index`。

use pi_ordmap::{
    asbtree::Tree,
    ordmap::{Entry, Iter, OrdMap},
};

use crate::Binary;

type SnapshotIterator<V> = <Tree<Binary, V> as Iter<'static>>::IterType;

/// 拥有创建 iterator 所用 COW 根的私有快照游标。
///
/// 字段顺序是安全不变式：Rust 按声明顺序释放字段，因此 `iterator` 必须位于根 owner
/// 之前。正常结束、提前取消和 panic unwind 都会先释放 iterator，再释放其节点来源。
pub(super) struct OrdMapSnapshot<V>
where
    V: Clone + Send + Sync + 'static,
{
    iterator: SnapshotIterator<V>,
    _root_owner: Box<OrdMap<Tree<Binary, V>>>,
}

impl<V> OrdMapSnapshot<V>
where
    V: Clone + Send + Sync + 'static,
{
    /// 固定 `root` 的当前版本并从包含边界 `start` 开始迭代。
    ///
    /// 构造只移动一个已完成的 O(1) COW 根克隆并执行 O(log n) 起点定位；不会物化数据，
    /// 也不会保存 `start`。调用方必须在进入本函数前释放保护可写根的锁。owner 会让创建
    /// 时仍可达的共享节点至少存活到快照释放；大量并发改写下，这属于契约必需的历史节点
    /// 临时保留，而不是创建阶段的全量复制。
    pub(super) fn new(
        root: OrdMap<Tree<Binary, V>>,
        start: Option<&Binary>,
        descending: bool,
    ) -> Self {
        // Box 使 owner 本身也具有稳定地址；当前依赖只保存 Arc 节点地址，但该布局能让
        // 所有权关系更直接，并保持与修复前每个流一次 Box 分配相同的数量级。
        let root_owner = Box::new(root);

        // SAFETY: `iterator_with_owned_root` 返回的引用只能通过下面返回 owned clone 的方法
        // 被观察。`root_owner` 与 iterator 一起移动进本结构，且字段释放顺序保证它后释放。
        let iterator = unsafe { iterator_with_owned_root(root_owner.as_ref(), start, descending) };

        Self {
            iterator,
            _root_owner: root_owner,
        }
    }

    /// 推进一次并只克隆 key；不会暴露依赖 iterator 返回的引用。
    #[inline]
    pub(super) fn next_key(&mut self) -> Option<Binary> {
        self.iterator.next().map(|Entry(key, _)| key.clone())
    }

    /// 推进一次并克隆完整键值对；不会暴露依赖 iterator 返回的引用。
    #[inline]
    pub(super) fn next_entry(&mut self) -> Option<(Binary, V)> {
        self.iterator
            .next()
            .map(|Entry(key, value)| (key.clone(), value.clone()))
    }

    /// 透传剩余项估计，仅用于为 Btree 合并去重表预分配容量。
    #[inline]
    pub(super) fn size_hint(&self) -> (usize, Option<usize>) {
        self.iterator.size_hint()
    }
}

/// 把依赖未绑定到 `&self` 的 iterator lifetime 限定在拥有根的私有封装中。
///
/// # Safety
///
/// 调用方必须保证：
///
/// - `root` 指向的 owner 在返回 iterator 的整个生命周期内保持存活；
/// - iterator 先于 owner 释放；
/// - iterator 产生的引用不逃逸，只能在 owner 存活时复制为 owned 值；
/// - 依赖实现仍是已审计的 `pi_ordmap 0.5.0`：节点由 `Arc` 分配在稳定堆地址，COW 写只
///   替换根而不原地修改共享节点，`start` 只在构造时比较且不被 iterator 保存。
///
/// 任意 `pi_ordmap` 版本或 iterator 实现变化都必须重新审计这些条件。
unsafe fn iterator_with_owned_root<V>(
    root: &OrdMap<Tree<Binary, V>>,
    start: Option<&Binary>,
    descending: bool,
) -> SnapshotIterator<V>
where
    V: Clone + Send + Sync + 'static,
{
    // `Iter<'a>::iter` 的签名没有把 `'a` 绑定到 `root`，所以依赖允许这里选择 `'static`。
    // 安全性不来自该签名，而来自外层 `OrdMapSnapshot` 强制执行的 owner 和释放顺序。
    <Tree<Binary, V> as Iter<'static>>::iter(root.as_ref(), start, descending)
}

#[cfg(test)]
mod tests {
    //! `OrdMapSnapshot` 所有权与析构顺序的精确白盒测试。
    //!
    //! 每个值只在树节点中保存一个 `Arc`，测试侧仅保留 `Weak`。旧根从当前 `OrdMap`
    //! 删除后，`Weak` 在快照活跃期间必须仍可升级；快照结束后必须全部失效。这样可直接
    //! 证明 owner 的保活和释放，而不使用不稳定的 RSS、墙钟或分配器缓存推断。

    use std::{
        panic::{catch_unwind, AssertUnwindSafe},
        sync::{Arc, Weak},
        thread,
    };

    use pi_bon::{Encode, WriteBuffer};

    use super::*;

    #[derive(Debug)]
    struct DropProbe(usize);

    type ProbeRoot = OrdMap<Tree<Binary, Arc<DropProbe>>>;

    fn root_with_probes(len: usize) -> (ProbeRoot, Vec<Binary>, Vec<Weak<DropProbe>>) {
        let mut root = OrdMap::new(None);
        let mut keys = Vec::with_capacity(len);
        let mut probes = Vec::with_capacity(len);
        for index in 0..len {
            let mut buffer = WriteBuffer::new();
            index.encode(&mut buffer);
            let key = Binary::new(buffer.bytes);
            let probe = Arc::new(DropProbe(index));
            probes.push(Arc::downgrade(&probe));
            let _ = root.upsert(key.clone(), probe, false);
            keys.push(key);
        }
        (root, keys, probes)
    }

    fn detach_current_root(root: &mut ProbeRoot, keys: &[Binary]) {
        for key in keys {
            assert!(root.delete(key, false).is_some());
        }
        assert!(root.is_empty());
    }

    fn assert_all_alive(probes: &[Weak<DropProbe>]) {
        for (index, probe) in probes.iter().enumerate() {
            let value = probe
                .upgrade()
                .unwrap_or_else(|| panic!("snapshot released probe {index} too early"));
            assert_eq!(value.0, index);
        }
    }

    fn assert_all_released(probes: &[Weak<DropProbe>]) {
        for (index, probe) in probes.iter().enumerate() {
            assert!(
                probe.upgrade().is_none(),
                "snapshot retained probe {index} after owner release"
            );
        }
    }

    /// 0 poll 取消必须释放快照独占的整棵旧根。
    #[test]
    fn test_zero_poll_drop_releases_owned_root() {
        let (mut root, keys, probes) = root_with_probes(64);
        let snapshot = OrdMapSnapshot::new(root.clone(), None, false);
        detach_current_root(&mut root, &keys);
        drop(root);

        assert_all_alive(&probes);
        drop(snapshot);
        assert_all_released(&probes);
    }

    /// 部分消费产生的 owned 输出释放后，取消快照不得遗留节点引用。
    #[test]
    fn test_partial_poll_drop_releases_owned_root() {
        let (mut root, keys, probes) = root_with_probes(64);
        let mut snapshot = OrdMapSnapshot::new(root.clone(), None, false);
        detach_current_root(&mut root, &keys);
        drop(root);

        let (key, value) = snapshot.next_entry().expect("snapshot must yield one item");
        assert_eq!(key, keys[0]);
        assert_eq!(value.0, 0);
        drop(value);
        assert_all_alive(&probes);

        drop(snapshot);
        assert_all_released(&probes);
    }

    /// helper 即使已经耗尽，根仍由 helper 本身持有，并在 helper drop 时一次性释放。
    #[test]
    fn test_exhausted_snapshot_releases_root_on_drop() {
        let (mut root, keys, probes) = root_with_probes(64);
        let mut snapshot = OrdMapSnapshot::new(root.clone(), None, false);
        detach_current_root(&mut root, &keys);
        drop(root);

        let mut observed = Vec::with_capacity(keys.len());
        while let Some((key, value)) = snapshot.next_entry() {
            observed.push((key, value.0));
        }
        assert_eq!(observed.len(), keys.len());
        drop(observed);
        assert_all_alive(&probes);

        drop(snapshot);
        assert_all_released(&probes);
    }

    /// panic unwind 必须执行字段声明顺序析构，不能留下 owner 或发生重复释放。
    #[test]
    fn test_unwind_releases_owned_root() {
        let (mut root, keys, probes) = root_with_probes(64);
        let snapshot = OrdMapSnapshot::new(root.clone(), None, false);
        detach_current_root(&mut root, &keys);
        drop(root);

        let result = catch_unwind(AssertUnwindSafe(move || {
            let _snapshot = snapshot;
            panic!("intentional unwind after snapshot construction");
        }));
        assert!(result.is_err());
        assert_all_released(&probes);
    }

    /// 把 owner 和裸指针 iterator 一起移动到其它线程后，读取与释放必须仍然有效。
    #[test]
    fn test_cross_thread_consume_and_drop_releases_owned_root() {
        let (mut root, keys, probes) = root_with_probes(64);
        let mut snapshot = OrdMapSnapshot::new(root.clone(), None, false);
        detach_current_root(&mut root, &keys);
        drop(root);

        let observed = thread::spawn(move || {
            let mut observed = Vec::new();
            while let Some((key, value)) = snapshot.next_entry() {
                observed.push((key, value.0));
            }
            observed
        })
        .join()
        .expect("cross-thread snapshot consumer must not panic");
        assert_eq!(observed.len(), keys.len());
        assert_all_released(&probes);
    }
}
