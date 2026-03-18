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
  新修复流程。启动时顺序解析未确认提交日志，只对持久化表快速装载 repair 动作，之后仍调用 `prepare_repair` 和 `commit_repair`，最后在整轮 replay 结束后立即 flush 本次修复涉及的持久化表。

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
  返回值中的第一个 `usize` 表示本轮实际重放的未确认 `commit log` 条数，也就是本轮修复的事务数；第二个 `usize` 表示本轮重放的提交日志总字节数。

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
9. quick repair 的显式 flush 粒度不是“每条事务一次”，也不是“每个表块一次”；它是在整轮启动修复 replay 结束后，对本轮触达的每个持久化表各执行一次兜底 flush。
10. 如果某张表在 replay 过程中因为等待队列累计大小达到 `waits_limit` 而提前触发了整理，这属于表自身原有的阈值行为，不表示 quick repair 改成了逐事务 flush。

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
- 一轮 quick repair 处理多少事务，取决于本次启动时扫描到多少条未确认 `commit log`；不是固定数量。
