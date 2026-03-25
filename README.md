# pi_db

`pi_db` 是一个基于全缓存模型的 Rust 异步键值数据库，支持多表、事务、两阶段提交、提交日志恢复，以及多种表实现（内存表、有序日志表、只写日志表、有序 BTree 表）。

## 功能简介

`pi_db` 的核心目标是把“事务一致性”和“多种表形态”统一到同一套接口下：

- 支持根事务统一调度多个表事务。
- 支持两阶段提交，事务写入先 `prepare_modified`，后 `commit_modified`。
- 支持通过提交日志恢复未正常确认的事务。
- 支持启动修复模式切换：
  - `TryRepair`：兼容旧修复流程。
  - `TryQuickRepair`：快速修复流程，按顺序重放提交日志，只快速装载持久化表动作，并在 replay 完成后立即 flush 受影响的持久化表。
- 提供提交日志和日志表的 inspector，方便做调试、排障和恢复验证。

## 关键名词

- `Binary`
  `pi_db` 中统一的二进制键和值封装，内部使用共享内存持有字节数组。
- `KVDBTableType`
  表类型枚举，当前支持 `MemOrdTab`、`LogOrdTab`、`LogWTab`、`BtreeOrdTab`。
- `KVTableMeta`
  表元信息，描述表类型、是否持久化、键类型和值类型。
- `KVDBManagerBuilder`
  数据库启动入口，负责加载元信息表、装载其他表并执行启动修复。
- `KVDBManager`
  数据库管理器，提供事务创建、表枚举、表路径和表整理等能力。
- `KVDBTransaction`
  根事务统一入口，可在多个表上执行读写删、建表删表、预提交、提交和回滚。
- `TableKV`
  跨表操作时使用的键值载体，包含表名、键和值。
- `prepare_output`
  事务预提交输出，描述本次事务对各表的最终修改，会被提交日志和 repair 流程复用。
- 未确认 `commit log` 记录
  一条未追加 `.bak` 的提交日志记录，对应一个已经写入提交日志但尚未完成最终确认的根事务。
- 表块
  `prepare_output` 中按表编码的一段修改集合；一个根事务的提交日志里可以包含多个表块。
- `TryRepair`
  旧修复流程。启动时顺序解析未确认提交日志，对每条写操作逐个走普通 `upsert/delete`，再执行 `prepare_repair` 和 `commit_repair`。
- `TryQuickRepair`
  新修复流程。启动时顺序解析未确认提交日志，只对持久化表快速装载 repair 动作，之后仍调用 `prepare_repair` 和 `commit_repair`，并在每个 `commit log` 文件批次 replay 完成后立即 flush 当前文件触达的持久化表。

## 表类型概览

| 表类型 | 是否有序 | 是否可持久化 | 典型用途 |
| --- | --- | --- | --- |
| `MemOrdTab` | 是 | 否 | 纯内存临时数据、可丢失缓存 |
| `LogOrdTab` | 是 | 是 | 有序日志型 KV，适合需要恢复和有序扫描的场景 |
| `LogWTab` | 否 | 是 | 只写日志型数据，适合偏追加写的场景 |
| `BtreeOrdTab` | 是 | 是 | 大规模持久化有序数据，适合范围扫描和稳定落盘 |

## 快速开始

### 依赖

```toml
[dependencies]
pi_db = "0.15.6"
pi_async_rt = "~0.2"
pi_async_transaction = "~0.10"
pi_store = "~0.9"
pi_atom = "~0.6"
pi_guid = "~0.1"
pi_sinfo = "~0.5"
```

### 基本启动流程

1. 准备异步运行时。
2. 准备提交日志 `CommitLogger`。
3. 创建 `Transaction2PcManager`。
4. 用 `KVDBManagerBuilder` 启动数据库。
5. 通过 `db.transaction(...)` 创建根事务。
6. 执行 `create_table`、`upsert`、`delete`、`query` 等操作。
7. 调用 `prepare_modified` 和 `commit_modified` 完成事务提交。

## API 详细说明

### 核心数据类型

#### `Binary`

- `Binary::new(Vec<u8>)`
  从拥有所有权的字节数组构建二进制值。
- `Binary::from_slice(&[u8])`
  从切片复制构建二进制值。
- `Binary::len()`
  获取字节长度。
- `Binary::to_shared()`
  转换为 `Arc<Vec<u8>>`。

#### `KVTableMeta`

- `KVTableMeta::new(table_type, persistence, key, value)`
  定义表类型、是否持久化、键类型和值类型。
- `table_type()`
  返回表类型。
- `is_persistence()`
  返回表是否持久化。
- `key_type()` / `value_type()`
  返回键和值的类型信息。

### 数据库启动

#### `KVDBManagerBuilder`

- `KVDBManagerBuilder::new(rt, tr_mgr, path)`
  创建数据库构建器。`path` 是数据库根目录；提交日志目录通常放在相邻的 `.commit_log` 目录。
- `startup(enable_accelerated_repair)`
  按默认修复模式启动数据库。当前默认走 `TryQuickRepair`。
- `startup_by_repair(enable_accelerated_repair, repair_mode)`
  显式选择启动修复模式，适合做兼容性验证和性能对比。
- `startup_with_listener(...)`
  启动数据库并挂接数据库事件监听器。
- `startup_with_listener_by_repair(...)`
  同时控制修复模式和事件监听器的完整启动入口。

#### `DBStartupRepairMode`

- `TryRepair`
  旧修复流程，兼容多年线上逻辑，适合作为基线校验。
- `TryQuickRepair`
  新修复流程，默认启用；只快速恢复持久化表，不参与内存表恢复，并在 replay 结束后立即 flush 持久化表。
  返回值中的第一个 `usize` 表示本次启动修复批次实际重放的未确认 `commit log` 条数，也就是这次批次修复的事务数；第二个 `usize` 表示这次批次重放的提交日志总字节数。

### 数据库管理器

#### `KVDBManager`

- `transaction(source, is_writable, prepare_timeout, commit_timeout)`
  创建根事务。`source` 用于标识事务来源。
- `is_exist(table_name)`
  判断表是否存在。
- `tables()`
  返回当前数据库中已加载的全部表名。
- `table_path(table_name)`
  返回表目录路径。
- `is_persistent_table(table_name)`
  判断表是否持久化。
- `is_ordered_table(table_name)`
  判断表是否有序。
- `table_record_size(table_name)`
  返回表记录数。
- `table_cache_size(table_name)`
  返回表缓存字节大小。
- `ready_collect_table(table_name)`
  为表整理做准备。
- `collect_table(table_name)`
  主动整理表。
- `append_new_commit_log()`
  追加新的提交日志检查点。
- `close()`
  关闭数据库并阻止新事务创建。

### 事务接口

#### `KVDBTransaction`

- `create_table(name, meta, enable_accelerated_repair)`
  创建表。
- `remove_table(name)`
  删除表。
- `upsert(Vec<TableKV>)`
  批量插入或更新。
- `delete(Vec<TableKV>)`
  批量删除。
- `query(Vec<TableKV>)`
  查询多个表键。
- `dirty_query(Vec<TableKV>)`
  读取当前事务视图下可能未最终提交的值。
- `keys(table, start, descending)`
  遍历键。
- `values(table, start, descending)`
  遍历键值对。
- `lock_key(table, key)` / `unlock_key(table, key)`
  显式锁定或解锁某个表键。
- `prepare_modified()`
  预提交事务，返回 `prepare_output`。
- `prepare_modified_conflicts()`
  预提交事务；若发生冲突，会返回首个冲突表和键。
- `commit_modified(prepare_output)`
  提交事务。
- `rollback_modified()`
  回滚事务。

### 调试与排障

#### `CommitLogInspector`

- 用于顺序查看提交日志里的事务提交内容。
- 常见用途：
  - 检查某次事务最终写入了哪些表和键。
  - 验证 crash recovery 前后的提交日志内容。

#### `LogTableInspector`

- 用于读取日志表物理文件中的日志项。
- 注意它是按物理日志顺序流式返回，不等价于“最终最新值”；若要验证最终状态，需要按“新值优先”语义自行合并同 key 的多条记录。

## 示例代码

### 示例 1：启动数据库并创建一张有序日志表

```rust,no_run
use pi_async_rt::rt::{startup_global_time_loop, multi_thread::MultiTaskRuntimeBuilder};
use pi_async_transaction::manager_2pc::Transaction2PcManager;
use pi_atom::Atom;
use pi_guid::GuidGen;
use pi_sinfo::EnumType;
use pi_store::commit_logger::CommitLoggerBuilder;
use pi_time::run_nanos;

use pi_db::{
    KVDBTableType,
    KVTableMeta,
    db::KVDBManagerBuilder,
};

async fn bootstrap() {
    let _time_loop = startup_global_time_loop(10);
    let rt = MultiTaskRuntimeBuilder::default().build();
    let guid_gen = GuidGen::new(run_nanos(), 0);

    let commit_logger = CommitLoggerBuilder::new(rt.clone(), "./example_db/.commit_log")
        .build()
        .await
        .unwrap();

    let tr_mgr = Transaction2PcManager::new(rt.clone(), guid_gen, commit_logger);
    let builder = KVDBManagerBuilder::new(rt.clone(), tr_mgr, "./example_db/db");
    let db = builder.startup(true).await.unwrap();

    let table = Atom::from("users");
    let tr = db.transaction(Atom::from("bootstrap"), true, 500, 500).unwrap();
    tr.create_table(
        table.clone(),
        KVTableMeta::new(
            KVDBTableType::LogOrdTab,
            true,
            EnumType::Usize,
            EnumType::Str,
        ),
        true,
    ).await.unwrap();

    let output = tr.prepare_modified().await.unwrap();
    tr.commit_modified(output).await.unwrap();
}
```

### 示例 2：写入、查询并提交事务

```rust,no_run
use pi_atom::Atom;

use pi_db::{
    Binary,
    db::KVDBTransaction,
    tables::TableKV,
};

async fn write_user<C, Log>(tr: &KVDBTransaction<C, Log>)
where
    C: Clone + Send + 'static,
    Log: pi_async_transaction::AsyncCommitLog<C = C, Cid = pi_guid::Guid>,
{
    let table = Atom::from("users");
    let key = Binary::from_slice(&(1usize).to_le_bytes());
    let value = Binary::from_slice("alice".as_bytes());

    tr.upsert(vec![TableKV::new(table.clone(), key.clone(), Some(value))])
        .await
        .unwrap();

    let values = tr.query(vec![TableKV::new(table.clone(), key, None)])
        .await;
    assert!(values[0].is_some());

    let output = tr.prepare_modified().await.unwrap();
    tr.commit_modified(output).await.unwrap();
}
```

### 示例 3：显式指定启动修复模式

```rust,no_run
use pi_db::db::{DBStartupRepairMode, KVDBManagerBuilder};

async fn startup_with_repair_mode<C, Log>(
    builder: KVDBManagerBuilder<C, Log>,
) -> std::io::Result<pi_db::db::KVDBManager<C, Log>>
where
    C: Clone + Send + 'static,
    Log: pi_async_transaction::AsyncCommitLog<C = C, Cid = pi_guid::Guid>,
{
    builder
        .startup_by_repair(true, DBStartupRepairMode::TryRepair)
        .await
}
```

## 注意事项

1. `pi_db` 依赖异步运行时；如果在测试或独立工具里自行构建运行时，建议先启动全局时间循环。
2. `MemOrdTab` 是纯内存表，进程重启后数据会丢失，因此 quick repair 不会尝试恢复内存表内容。
3. 事务提交必须遵循 `prepare_modified -> commit_modified` 的顺序；只拿到 `prepare_output` 但没有执行提交时，启动恢复会依赖提交日志补齐提交。
4. `TryQuickRepair` 默认启用，但它并没有改变正常事务路径的提交流程，只是在启动恢复阶段减少逐 key 普通写路径的开销。
5. `LogTableInspector` 返回的是物理日志顺序，不是最终最新值；用它做断言时必须自行合并。
6. 如果需要对大表做主动整理，应先调用 `ready_collect_table`，再调用 `collect_table`。
7. quick repair 集成测试的临时目录现在统一放在 `./tmp_quick_repair/` 下；如需清理，可执行 `bash tools/cleanup_quick_repair_dirs.sh current`，如需连历史散落目录一起清理，可执行 `bash tools/cleanup_quick_repair_dirs.sh all`。
8. 示例代码为了聚焦接口，省略了业务错误包装、目录清理和更多并发控制逻辑；实际项目中建议为数据库目录和 `.commit_log` 目录分别做生命周期管理。
9. quick repair 的显式 flush 粒度不是“每条事务一次”，也不是“每个表块一次”；它是在每个 `commit log` 文件批次 replay 结束后，对该文件触达的持久化表各执行一次 flush。
10. 文件级 flush 不等于文件级 confirm；quick repair 仍沿用原有 `confirm_replay -> finish_replay` 的统一确认流程，因此在“部分文件已 flush、整体尚未 `finish_replay`”时再次启动，恢复流程仍必须保持幂等。
11. 如果某张表在 replay 过程中因为等待队列累计大小达到 `waits_limit` 而提前触发了整理，这属于表自身原有的阈值行为，不表示 quick repair 改成了逐事务 flush。
12. 修复时会自动忽略所有带 `.bak` 的 `commit log` 文件；如果需要让既有样本参与修复，只能对“连续后缀”那一段物理日志文件去掉 `.bak`，不能跳着激活中间文件。
13. `Repair db succeeded` 当前表示“数据 replay/flush 已完成且数据库可继续 startup 提供服务”，不等价于“所有历史 `commit log` 都已经立即补成 `.bak`”；如果 replay confirm 仍在后台追赶，`.bak` 可能会稍后补上，但这不会改变已提交事务的数据完整性、顺序性和幂等恢复语义。
14. 为增强可观测性，修复过程中每当一个物理 `commit log` 文件真正完成 replay confirm 并被推进成 `.bak` 时，底层提交日志记录器会输出一条 `info` 日志，包含文件路径、事务日志数量、日志字节数和从进入 replay 到完成 `.bak` 推进的总耗时。
15. 如需进一步区分“数据 replay/flush 已完成”和“replay confirm 仍在追赶”，当前已经把修复关键节点日志提升到 `info`。推荐优先只打开修复相关模块：
    - 只看修复关键模块：
      `RUST_LOG=off,pi_db::db=info,pi_store::commit_logger=info`
    - 如需同时保留其它模块的全局告警：
      `RUST_LOG=warn,pi_db::db=info,pi_store::commit_logger=info`
    这些日志会覆盖以下关键节点：
    - `try_repair` / `try_quick_repair` 入口与 `finish_replay` 返回
    - 每个物理 `commit log` 文件批次的 replay 开始、flush 开始/结束
    - 每个物理 `commit log` 文件 replay 完成但尚未推进 `.bak`
    - `finish_replay` 开始/结束时的 buffered confirm / pending file 摘要
16. 上述修复关键日志故意不打在逐事务、逐 key、逐次 `confirm_replay` 或 waits 入队等高频热路径上，避免在生产环境形成日志风暴；如果看到 `Repair db succeeded` / `Startup db succeeded` 之后仍持续出现“.bak promoted”日志，通常表示数据库已可提供服务，而 replay confirm 仍在后台正常追赶。

## 相关模块

- `src/db.rs`
  启动、数据库管理器、事务与修复主链路。
- `src/lib.rs`
  公共类型、错误类型和基础工具。
- `src/tables/`
  各类表实现。
- `src/inspector.rs`
  提交日志与日志表检查工具。

## 当前修复相关状态

- 默认启动入口当前走 `TryQuickRepair`。
- `TryRepair` 仍完整保留，可用于可靠性基线对比。
- quick repair 会跳过内存表，只修复持久化表。
- quick repair replay 完成后会对本次触达的持久化表执行一次立即 flush，避免继续等待默认后台整理周期。
- 一次 quick repair 启动修复批次处理多少事务，取决于本次启动时扫描到多少条未确认 `commit log`；不是固定数量。
- 当前文件级 flush barrier 已固定为“按当前 DB 视图逐张串行 `quick_flush_waits`”，不再采用试验性的分表并发 flush。
- 当前本地正式对比基准已确认：在 `MetaTab + Btree` 主导、`3` 个约 `32MB` 物理日志文件的近生产样本上，`try_quick_repair(depth=2)` 相对 `try_repair` 的端到端启动修复总时长约快 `3.674x`，且两者修复后的逻辑结果与最终磁盘状态完全一致。
- 当前剩余主热点仍集中在 `BtreeOrdTab collect_waits(...)` 的 `apply + redb_commit`；在不改变 `try_repair`、不改变正常事务语义、并把改动限制在 `pi_db/pi_store` 内的前提下，内部可控优化空间已经明显收窄。
- 当前线上已确认：startup 成功后历史 `commit log` 的 `.bak` 推进可能略晚于数据 replay/flush 完成；这更像是共享 replay confirm 收尾滞后，而不是 quick repair 数据修复失败。
- 当前仓库已包含稳定复现测试 `test_repair_confirm_lag_only_delays_bak_promotion_after_startup`，用于验证“数据已修好但 `.bak` 稍后补上”的窗口，并支撑后续修复前后对照。
- 当前还额外提供了低频修复关键 `info` 日志，可直接配合 per-file `.bak` info 日志一起在线上判定：是“数据已修好、confirm 正在追赶”，还是某个具体修复阶段真的阻塞。
