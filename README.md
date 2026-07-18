# pi_db

`pi_db` 是基于事务、前导日志和多种表存储实现的异步键值数据库。

## 当前关键语义

### 启动恢复

`KVDBManagerBuilder::startup` 和 `startup_with_listener` 默认通过 `try_repair` 重放尚未最终
确认的根事务前导日志。`enable_accelerated_repair` 只控制 Btree/redb 的快速崩溃恢复写入
模式，不会选择其它根日志恢复路径，也不会跳过 WAL 或提前确认事务。

### 事务树身份与根 WAL

一次公开事务生命周期只注册一个外层根，根及全部子表事务共享同一个事务 ID；需要写根 WAL 时
还共享同一个提交 ID。子表事务是根拥有的内部节点，不得独立调用 `start/prepare/commit/
rollback/finish`。`prepare_len` 和 `commit_len` 只表示当前正在运行的可写外层 future 数，错误、
取消或 panic unwind 后会自动收口，但不能用作全局静止点。

`persistence=true` 只表示对应事务动作需要进入根 WAL，不表示该表拥有独立数据文件。Memory 表
允许 `persistence=true`，其动作会写入根 WAL，但不会创建 Memory 数据文件；可写非持久化事务
不写 WAL，仍会执行完整子表 commit。

显式只读事务不需要 commit 回调。可写事务即使只有读动作、prepare 输出为空，也必须执行完整
事务树 commit，以释放 prepare 阶段建立的读预留；空输出只会跳过 WAL append/flush。建表和删表
应各自使用独立根事务，不与业务表读写混用，创建成功后再用新事务操作该表。

当前事务安全保证以根 WAL append/flush 能在健康存储和可用 runtime 上完成为环境前提。根 WAL
自身因磁盘空间/配额不足、只读或故障文件系统、设备 I/O、runtime 拒绝任务、文件大小限制而失败
时，不保证事务仍具备原子性、可回滚性、checkpoint 收口或确定的重启结果。现有普通 I/O 错误不
携带失败阶段和累计写入字节；因此 `LogCommitFailed`、Normal 错误或 rollback 返回成功均不能在
这些阶段外情形中证明 WAL 完全没有落盘。该边界不是空 WAL：空 prepare 输出会直接跳过物理 WAL
写入。根 WAL 已成功后发生的子表数据文件持久化失败仍保留未确认 WAL，并继续适用既有启动恢复
语义。

### 迭代器快照

`KVDBTransaction::keys` 和 `values` 返回创建流时的稳定快照。创建流的事务必须一直存活到
流耗尽或被丢弃；在此期间，该事务自身及其它事务可以继续 `upsert/delete`，但已有流不会
观察这些后续修改。该保证不是可串行化、实时可见或与 commit/rollback 绑定的事务安全保证。

纯迭代器在同表没有普通子事务时使用脱离根 2PC 的只读快照事务，不选择 Ordinary 或 Versioned
协议，也不会自动加入版本 read-set；因此允许先创建并保持流，再执行版本预提交和提交。若同表
已经存在普通子事务，迭代器仍复用它以保留 read-your-own-write，此时该根已经选择普通协议，
不能再切换为版本协议。

Memory、Meta 和 LogOrdered 流持有创建时的 COW 根；Btree 流同时持有 overlay COW 根和
redb 读事务。流正常耗尽、提前取消或 panic unwind 都会释放这些额外 owner。

### Btree 删除旧值

`delete/dirty_delete` 对 Btree 使用三态只写缓存：缓存值返回 `Some(old)`；已有 tombstone
或重复删除返回 `None`；缓存完全缺席时读取调用时的 redb 快照并返回可能的旧值。所有分支
都会保留 tombstone，实际 redb 删除仍由根 WAL 成功后的 collector 完成。

redb 的 `begin_read/open_table/get` 失败会记录详细错误日志并降级为 `Ok(None)`，删除仍可
提交。因此 Btree 返回的 `None` 不能无条件解释为持久化存储中原本不存在该 Key。

### Btree 普通事务冲突基线

新建空 Btree 可能已经存在 redb 数据库文件，但尚未物理创建 `$default` 表；该精确状态是逻辑
空表。`query_with_version` 会返回 `None` 和首次 Delete 版本，只有其它 redb 表错误、类型错误或
点读错误才按可恢复数据库错误返回。

普通 Btree 事务通过 `query/delete` 成功读取的逻辑值会成为该 Key 的独立预提交基线，但
redb 读取结果不会写入事务创建时的只写缓存根。预提交先用 Key 的提交 revision 判断创建事务
后是否发生过写入；没有后续写入时，再以共享 allocation 身份作为 O(1) 快路径，并在 redb
重复解码或 collector 仅搬迁物理表示导致 allocation 不同时回退到原始字节比较。由此，逻辑值
未变化不会因 `Arc` 地址不同误报冲突，同值写和 A -> B -> A 仍由 revision 判为冲突。

redb 删除返回的旧值与事务内基线分别持有独立 owner：调用方释放返回值后，基线会保留到事务
commit、rollback 或整体释放。该保留是冲突检测所需的有限生命周期，不会写回全局 overlay，
也不改变 WAL、collector 或提交确认流程。

### 基于 Key 版本的事务 API

版本协议由 `KVDBManager::query_with_version`、`KVDBTransaction::prepare_with_version` 和
`commit_with_version` 组成，供外部 `Table/Key/Value/Version` 缓存执行批量乐观事务。它是与普通
`query/upsert/delete/prepare/commit` 分离的调用族；同一事务不得混用两套动作或终结方法。
`pi_db` 保证同一根 2PC 树不会同时包含 Ordinary 与 Versioned 子节点：版本预提交发现任意普通
子节点时，会在分配事务 ID 和写 WAL 前拒绝；版本树按每表唯一子节点原子安装。纯 iterator 是
不参与 2PC 的中性快照例外，不会掩盖此前已经注册的普通动作。

`query_with_version` 不创建数据库事务，原子返回当前逻辑值和同一线性化状态的版本。版本缺席时
会用事务管理器的 Guid 生成器创建首次观测版本；并发读取同一缺失 Key 只会公布一个版本。
`prepare_with_version` 先完整比较外部读集，再执行标准事务冲突检查；版本或标准冲突以
`KVTableTrError::AllConflicts` 返回完整、去重、确定顺序的 `Table/Key` 集合，非 Fatal 失败须由
调用方 rollback，并用全新事务重试。

根 WAL 成功后，`commit_with_version` 按表依次在短 publication write 临界区同时发布数据和本
事务版本，并只返回本事务最终写集的 `TableKeyVersion`。单表内的值/版本观察是原子的；多表间
允许按提交顺序逐表可见，不承诺全库瞬时同时可见。该返回表示事务已提交，不表示所有表数据文件
已经完成异步持久化和提交确认。提交完成后还会严格验证每个最终写 `(Table, Key)` 恰好具有一项
同事务 ID、同 Upsert/Delete 类型的回执；缺失、重复、额外或错配属于不可 rollback 的 Fatal
内部错误，不会再静默返回部分或空回执。

版本缓存只存在于当前数据库进程内，不写入 WAL 或表数据文件。collector 只搬迁 Btree 物理表示，
不得改变已发布版本；正常冷启动或 repair 完成后版本缓存为空，第一次
`query_with_version` 会根据恢复后的真实值生成新版本。因此外部缓存不得跨数据库实例继续把旧
版本当作当前版本，必须重新读取或以新提交回执刷新。

### 删表提交耐久性

`KVDBTransaction::remove_table` 的 `Ok(())` 只表示删除动作已登记；调用方仍需依次执行
`prepare_modified` 和 `commit_modified`。删表根事务会把 Meta tombstone 写入根 WAL，因而在
`commit_modified` 成功后、Meta 数据文件尚未异步持久化时立即崩溃，正常启动仍会从原 WAL
恢复删除。所有持久化子表完成后，提交确认才允许对应 WAL 文件进入可移走状态。

内部 `.tables_meta` 不能通过公开删表 API 删除，恢复时遇到该目标或无法解码的 Meta tombstone
Key 也会按无效持久化数据拒绝。表名当前合法范围为 `1..=4096` 个 UTF-8 字节。

当前建表/删表尚不具备完整事务 rollback 或取消原子性：`remove_table` 返回前后的注册表变化
不能单独视为事务提交，rollback 不保证恢复已移除的注册项，删表也不负责物理删除表目录或
强制释放仍被事务、迭代器或后台任务持有的资源。

### Key 版本 TTL

`KVDBManagerBuilder::key_version_ttl` 和 `key_version_ttl_poll_interval` 的内部最小时间单位均为
1ms。TTL 为 `Duration::ZERO` 时关闭自动淘汰；其它非零且小于 1ms 的值按 1ms 处理，大于等于
1ms 的值会舍弃不足 1ms 的小数部分，例如 `20.999ms` 的有效值为 `20ms`。

版本记录不会早于量化后的有效 TTL 被淘汰。实际淘汰仍可能因轮询周期、runtime 调度、分批扫描
或活跃事务快照而延后；TTL 是最短保留时间，不是精确触发时刻。

指定 Key 的淘汰时间只在两类状态发布时刷新：`query_with_version` 首次发现版本缺席并成功公布
首次观测版本；以及数据库启动完成后的普通提交或版本提交成功发布该 Key 的 Upsert/Delete 版本。
WAL repair/replay 的表提交内部会暂时走相同发布路径，但 builder 在恢复完成、数据库对外可用前
统一清空全部版本记录，因此不会留下可观察的 TTL 刷新。缓存命中的 `query_with_version`、普通
`query/dirty_query`、`keys/values`、prepare、冲突检查、rollback 和提交确认都不刷新；TTL scanner
只安排全局扫描期限，也不延长某条记录。TTL 为 `Duration::ZERO` 时整个淘汰机制关闭。

版本 `DashMap` 是唯一状态事实。TTL 开启时，每个当前版本记录额外在所属表的非等待 FIFO
索引中保存一个共享 Key token；首次插入或过期后重建时入队，更新已存在记录不会重复入队。
后台每批最多点查 256 个 Key，不迭代表内版本 `DashMap`；未到期、并发刷新或被活跃快照保护的
token 会在让出 runtime 前返回索引。因此额外空间为 O(当前活跃版本记录数)，不保留热 Key 的
历史 token。每轮仍会短暂枚举表级注册表；该同步 guard 不跨越 `await`，但可能短暂延迟同分片
的建表/删表，并依赖当前“DDL 不与业务事务读写混用”协议边界。该索引只影响内存版本回收，不改变
表数据、WAL、提交确认或恢复语义。

## 运行时验收范围

本轮线程安全验收实际解析并测试了 `pi-async-rt 0.5.2`，包括正确 ABI 的 TSan 和 ASan。
`Cargo.toml` 仍保持 `pi-async-rt = "~0.5"`，不限制下游选择具体 `0.5.x` 版本。因此本轮
sanitizer 结论只适用于已测试的 `0.5.2` 依赖图，不能自动外推到下游自行解析的历史版本。

## Key 版本协议验收基线

本轮正式无 patch 复验实际解析 `pi_async_transaction 0.12.1`、`pi-async-rt 0.5.2` 和
`pi_sinfo 0.6.0`；`Cargo.toml` 继续保持既有 `~0.12`、`~0.5` 和 `~0.6` 范围，不通过收窄版本
约束代替兼容性验证。版本 API、完整冲突、四个在用表 publication、Btree 冷启动、TTL、快照
生命周期和真实 WAL/回执聚焦目标均已通过；正确 ABI TSan 未报告数据竞争，ASan 未报告 UAF、
越界或 double free。ASan 使用 `detect_leaks=0`，不构成通用泄漏证明。

真实端到端基准中，Btree、Memory 的 1/16/256 Key 和 Meta 的 1/16 Key 普通/版本 pair 没有形成
稳定退化；Meta/256 Key 独立三轮中位数为 `7.126ms -> 9.113ms`，版本协议因 256 次版本读取、
完整冲突检查和回执校验增加约 `27.88%`。该值只描述当前机器、工具链、依赖图和 workload，不是
跨硬件 SLA。详细证据保存在本地开发文档 `docs/KEY_VERSION_PUBLICATION_ACCEPTANCE.md`；事务树、
根 WAL、提交确认、checkpoint、`.bak` 和 `try_repair` 的中文流程图、状态图及时序图保存在
`docs/TRANSACTION_WAL_RECOVERY_FLOWS.md`。
