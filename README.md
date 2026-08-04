# pi_db

`pi_db` 是基于事务、前导日志和多种表存储实现的异步键值数据库。

## 当前关键语义

### 启动恢复

`KVDBManagerBuilder::startup` 和 `startup_with_listener` 默认通过 `try_repair` 重放尚未最终
确认的根事务前导日志。`enable_accelerated_repair` 只控制 Btree/redb 的快速崩溃恢复写入
模式，不会选择其它根日志恢复路径，也不会跳过 WAL 或提前确认事务。该值只作用于本次从 Meta
加载或由根 WAL repair-create 的 Btree，并由这些表保存供后续 redb 写事务使用；它不是 manager
级建表默认值，启动后显式创建 Btree 仍使用建表 API 自己的参数。真实三进程
`false -> true -> false` 重启、两次非正常进程退出、collector、最终数据与空 WAL 一致性由
`tests/key_version_btree_restart_consistency.rs` 验证；该功能专项不代表性能 A/B 结论。

启动阶段会以最多 8192 条 Meta 记录为一批装配用户表；该值只是内部临时内存和单批异步工作
规模，不是公开表数量上限。跨批边界的当前记录会进入下一批，因此第 8193、16385 等位置的
合法表定义不会被静默跳过；权威 Meta 中的全部定义必须成功装配后，启动才会继续进入根 WAL
修复和可用状态。

启动的 `IOResult` 当前只覆盖明确传播的 TTL 配置、目录创建、表名校验、部分批量装配及根 WAL
repair 错误，并不覆盖全部失败。Meta/持久表构造和受信元数据 decoder 仍有 panic 分支；异步
批量加载中的表任务 panic 或 runtime 拒绝任务时，共享完成值可能不再推进，startup future
可能持续等待。失败前创建的目录、文件、表对象或 collector 也不会自动 rollback。普通文件
阻断数据库目录的 Linux 真实专项已证明目录错误保留 `NotADirectory`，且不会调用 listener、
创建事务或写根 WAL；损坏 Meta/redb 的 panic/挂起策略仍分别由 `FIND-CODEC-001` 和
`FIND-CTOR-001` 管理，当前不是最终或最佳错误模型，不能把现状 panic/挂起当作稳定 API 契约。

<a id="startup-repair-execution-context"></a>

#### 启动恢复执行上下文

存在未确认根 WAL 时，`CommitLogger::start_replay` 会在轮询启动 future 的当前线程同步调用
`pi_db` 的 replay callback。callback 把真正的表动作、`prepare_repair` 和 `commit_repair` 投递到
该数据库实例的 runtime，并同步等待这条 repair task 返回；callback 返回后，存储层才能按原有
顺序推进对应 checkpoint。该同步边界是当前 replay/WAL 归属语义的一部分，不能改成先返回再后台
修复。

已确认的活性限制必须同时满足以下条件才会触发：存在会调用 callback 的非空未确认 WAL；startup
future 正由数据库 runtime 自身的 worker 驱动；所有可执行 worker 都被 startup/replay callback
同步等待占用，以致刚投递的 repair task 没有 worker 可以首次 poll。此时 startup 会持续等待，
数据库不会进入可用状态；这是活性问题，不是 data race 或 UB，当前也没有数据损坏证据。单 worker
配置本身不是触发条件：空 WAL 不调用 callback；由 runtime 外部线程驱动 startup 时，唯一 worker
仍可执行 repair；同一 runtime 中尚有空闲 worker 时也可以推进。

当前正式生产链 `pi-launcher::start_storage_db_server -> futures::executor::block_on ->
pi_db_server::init_db_server -> DbInstance::build -> KVDBManagerBuilder::startup` 由 launcher 启动线程
轮询 startup。`pi_db_server` 为每个实例创建独立数据库 runtime，并在 registry 构建期间顺序启动
实例；启动期 worker 只处理文件、repair commit 和提交确认任务。因此当前装配即使配置一个 worker，
也不会提供上述自阻塞窗口。直接集成 `pi_db` 的调用方若改为在同一个数据库 runtime 内启动实例，
必须确保 replay 等待期间至少保留一个可执行 worker，或改由 runtime 外部线程驱动 startup；不得
同时并发启动足以占满共享 runtime 全部 worker 的多个待 repair 实例。

本轮不修改 repair、checkpoint、确认、WAL 格式、公开 API 或正常生产执行路径，只归档该条件性
限制。`tests/startup_repair_liveness.rs` 使用真实 runtime、事务管理器、CommitLogger、LogOrdered
表、文件系统和公开 startup：外部线程驱动的单 worker 非空 WAL 必须恢复成功并校验最终值；同一
runtime 唯一 worker 的特定窗口必须在有限截止内精确命中特征化红线，且不能退化为 I/O 错误、
panic、channel 断开或测试进程永久挂起。完整证据与未来候选方案见
`docs/STARTUP_REPAIR_RUNTIME_LIVENESS_BUG.md`。

<a id="offline-inspector-lifecycle"></a>

### 离线日志检查器

`CommitLogInspector` 和 `LogTableInspector` 是专用、离线、单消费者、单实例一次性使用的诊断
工具，不允许在生产数据库运行期间使用，也不能与在线数据库共享 logger、表目录或 runtime
生命周期。它们不修改数据库表数据、不执行 repair，但“诊断读取”不等于物理文件严格只读：
根 WAL replay 可以分裂日志、调整 checkpoint 或标记 `.bak`，`LogFile::open` 也可能创建初始
日志或整理目录。重复检查必须在上一次结束后创建新 Inspector。

`CommitLogInspector` 的 pull 和 callback 模式都会在 `start_replay*` 正常返回 `Ok/Err` 后调用
`finish_replay`。callback 的最终 `None` 只在 finish 尝试及状态复位后发出；它表示后台检查
生命周期已闭合，不表示 WAL 解析或缓冲确认一定成功，具体错误只写日志。可信 WAL 解析或用户
callback panic、runtime 被提前终止仍不保证异步 finish，调用方必须废弃该一次性诊断
runtime/进程。`LogTableInspector` 不进入 `CommitLogger` replay，因此不调用 `finish_replay`。
完整边界与红绿证据见 `docs/INSPECTOR_REPLAY_LIFECYCLE_FIX.md`。

<a id="transaction-debug-logger-boundary"></a>

### `log_table_debug` 历史诊断 feature

`log_table_debug` 是 `pi_db` crate 自身的可选 Cargo feature，不是 `tools/` 下的独立子库，
也不是 `log::debug!`、OpenTelemetry tracing、根 WAL 或事务管理器的一部分。
`TransactionDebugLogger`、`TransactionDebugEvent`、`init_transaction_debug_logger` 等公开 API
无论 feature 是否启用都会编译；该 feature 只打开少量自动事件发送点。

当前自动事件链并不覆盖任意事务树。`Begin/Commit/CommitConfirm` 只由 LogOrdered 子表发送，
`End` 则由所有进入最终根 WAL 确认分支的事务发送。单个持久化 LogOrdered 子表的简单根事务
可以形成相对完整的链；同一根包含多个 LogOrdered 子表会重复发送同一 TID 的 `Begin`，其它表
可能只有 `End`，非持久化 LogOrdered 还可能没有用于清理时间表的 `End`。因此日志中的
“Transaction id conflict”或“transaction not exist”不能直接解释为真实事务冲突或提交失败。

启用 feature 不会自动初始化全局日志器。调用方必须在事务开始前从不会占尽同一 runtime
执行能力的上下文恰好调用一次 `init_transaction_debug_logger`；否则自动发送点会 panic。
日志器使用非有界队列、永久后台任务和独立 `LogFile` 目录，没有 shutdown/drain 回执；重复
初始化会先构造临时实例，重复 `startup` 会产生多个消费者，后台 spawn/日志 commit 错误不会
形成事务错误回执。必须特别区分：自动发送点是内联调用，未初始化全局日志器导致的 panic
可以中断 LogOrdered prepare、持久化队列登记或确认回调；这也是当前禁止把它当作透明观测能力
的原因。其输出只可用于历史诊断，绝不能作为提交成功、持久化完成、WAL 确认或恢复正确性的
判定依据。

该 feature 当前未被 `pi_db_server`、`pi_db_terminal` 或 `pi-launcher` 的生产装配启用，项目已
决定暂时忽略其功能演进、修复和动态测试，只保留兼容并详尽标注现状。重新启用生产用途、要求
覆盖任意表/多表事务、要求可关闭/可重复初始化或依赖日志作正确性判断时，必须先重新冻结设计。
完整归档见 `docs/TRANSACTION_DEBUG_LOGGER_BOUNDARY.md`（FIND-DEBUG-001）。

<a id="log-write-deferred-boundary"></a>

### LogWrite 暂挂边界

LogWrite 当前没有外部使用，也不允许外部业务直接构造、创建或操作。Rust 表类型枚举、统一
trait 和数据库内部启动/DDL/repair 仍保留兼容路径，但这只表示实现可表达，不构成公开可用性
承诺。其 query 固定返回 `None`，delete 是 no-op，keys/values 各产生一个非法业务空哨兵；这些
都不是最终或最佳 CRUD 设计。

内部共享 COW 根会由冷启动 loader 重建，并在根 WAL 成功后的在线 commit 中立即发布，因此
`len` 返回当前内部唯一 Key 数，`size` 返回该根的 payload 估值，并非仅有启动基线。query 和
iterator 故意不暴露这个根；统计也不表示独立表日志已经落盘。持久化事务随后进入表级 FIFO，
只有 `LogFile::delay_commit` 成功才确认根 WAL，失败会保留 WAL 供启动 repair。

本轮按 HC-059 只完成静态注释、文档和结构映射，不修改行为，也不新增 LogWrite 动态测试、
sanitizer 或基准。未来下游准备启用该表时，必须先重新冻结 CRUD、统计、错误、关闭、恢复和
性能语义。完整结构、锁序、生命周期与证据边界见
`docs/LOG_WRITE_TABLE_INTERNAL_CONTRACT.md`。

<a id="manager-soft-close-boundary"></a>

### Manager 软关闭边界

`KVDBManager::close()` 当前是立即返回的软关闭标记，不是 graceful shutdown。它会阻止后续
`transaction()` 创建新根，但不会取消 close 前已经创建的根，也不会阻断已经 Prepared 的普通/
版本提交或可恢复失败事务的 rollback；需要根 WAL 的既有提交仍沿原链确认。最后一个活跃事务
结束后状态不会自动从 Closing 推进 Closed，需再次调用 `close()`。

该方法不等待 collector/listener 退出、文件句柄释放、所有异步数据持久化或根 WAL 确认，
因此返回不代表数据库已经可以安全地在同一路径重建。`tests/manager_contract.rs` 使用真实
runtime、事务管理器、CommitLogger、Memory 表和文件系统验证上述当前行为，并同时检查最终值、
版本回执、事务计数、根 WAL append 以及确认完成数/等待数的严格守恒；它不要求 `close()`
返回时其它既有异步确认已经完成。并发创建与 close 的线性化、关闭后 maintenance/listener 行为
及完整资源释放仍不属于现有保证。完整调用链、状态边界和验收矩阵见
`docs/MANAGER_SOFT_CLOSE_ACCEPTANCE.md`；本轮只读审计发现但尚未动态确认的 listener、路径与
maintenance 后续项见 `docs/MANAGER_AUDIT_FOLLOWUPS.md`。

Manager 的路径和表查询 API 不参与事务，也不读取软关闭状态：路径 getter 返回构建时词法
路径，逐表查询在当前 registry 中查找，未注册的空名、超限名和已删表按缺表返回；
Closing/Closed 后只要句柄仍存活就可继续查询。删表在动作阶段已经移除 registry，提交负责
Meta tombstone 的根 WAL 闭环，但不会删除旧物理目录；多次查询之间不构成共同快照。当前精确
矩阵、锁边界、统计限制和真实验收见 `docs/MANAGER_QUERY_PATH_ACCEPTANCE.md`。这不是路径
containment、删表 rollback 或 graceful shutdown 保证。

`startup_with_listener` 使用无界事件通道和单个长期 runtime 任务串行调用同步回调。持续有事件
时每批最多 3072 项并在满批后立即交付；不足上限时会等待最多五轮 10ms 空闲窗口再交付，因此
它不是实时通知 SLA。框架会复用批次 `Vec`，回调必须在返回前 `drain` 或 `clear` 已处理事件。
回调阻塞会占用 worker，panic 会终止当前 listener 任务；生产速度持续超过消费速度会导致无界
队列增长。`ConfirmCommited` 只表示对应表持久化后同步确认器返回成功，不证明根 WAL 异步确认
和 `.bak` 已完成。Meta 与 LogOrdered 当前都携带 `BtreeOrdTab` 标签，这是已归档且非最终的
事件模型边界，不能按准确表类型解释。正常路径的真实装配、严格断言和未覆盖异常见
`docs/MANAGER_LISTENER_ACCEPTANCE.md`。

### 事务树身份与根 WAL

`KVDBManager::transaction` 只构造一个尚未开始 2PC 的共享根：初始状态为 `Start`，没有
TID/CID、子节点、manager 登记或 WAL 副作用。`source`、writable 和两个 `u64` timeout 原样保存
并传给后续子节点；timeout 当前不执行实际截止。clone 共享同一逻辑根，首次 prepare 的
`start` 才分配 TID、递归发布身份并登记唯一外层根。表子节点按首次触表顺序且每表唯一；
`KVDBChildTrList` clone 是节点集合快照，不是第二棵事务树。真实构造、未 prepare Drop、两表
身份继承、manager 计数、WAL 确认和最终数据由
`tests/root_transaction_construction.rs` 验证；完整边界见
`docs/ROOT_TRANSACTION_CONSTRUCTION_ACCEPTANCE.md`。

一次公开事务生命周期只注册一个外层根，根及全部子表事务共享同一个事务 ID；需要写根 WAL 时
还共享同一个提交 ID。子表事务是根拥有的内部节点，不得独立调用 `start/prepare/commit/
rollback/finish`。`prepare_len` 和 `commit_len` 只表示当前正在运行的可写外层 future 数，错误、
取消或 panic unwind 后会自动收口，但不能用作全局静止点。

`persistence=true` 只表示对应事务动作需要进入根 WAL，不表示该表拥有独立数据文件。Memory 表
允许 `persistence=true`，其动作会写入根 WAL，但不会创建 Memory 数据文件；可写非持久化事务
不写 WAL，仍会执行完整子表 commit。未确认根 WAL 可在启动 repair 中恢复 Memory 动作；事务
全部确认且 WAL 可移走后，data-only 重启只恢复 Memory 表定义，不恢复其业务值。内部结构、锁序
和证据边界见 `docs/MEMORY_TABLE_INTERNAL_CONTRACT.md`。

一个普通可写根可以按首次触表顺序直接拥有多个表级单元子事务；这属于单层多表事务树，不是
单元事务。需要 WAL 时，根与全部叶子共享同一个事务 ID 和提交 ID，三个表的 prepare 输出只由
外层根追加、刷新一次 WAL。`commit_modified` 返回后，Memory 已发布内存根，LogOrdered/Btree
仍可处于异步表文件持久化阶段；只有所有持久化叶子的成功信号到齐，根 WAL 才能最终确认。
`tests/ordinary_multi_table_recovery.rs` 使用一个普通业务根同时写三表，并以 manager 配平、单次
业务 WAL、`.bak`、Btree overlay 清空及连续两次移走 WAL 后冷启动的最终值共同验证该闭环。

普通 `prepare_modified_conflicts` 按首次触表顺序在首个冲突处停止，只返回一个 Normal
`Conflicts(table,key)`，不提供版本协议 `AllConflicts` 的完整集合语义。失败时，冲突前的叶子
可能已经 `Prepared`，冲突叶子为 `PrepareFailed`，后续叶子仍为 `Inited`；合法非 Fatal 路径的
`rollback_modified` 会处理整棵树并将该事务永久关闭，重试必须创建新根。
`tests/ordinary_multi_table_conflict_rollback.rs` 分别在 Memory、LogOrdered、Btree 作为首个、
中间、最后失败叶子时验证状态、零失败 WAL、manager 配平、同 Key 新事务重试、repair、`.bak`
及移走 WAL 后最终数据。

`tests/ordinary_multi_table_concurrency.rs` 进一步在真实双 runtime 上以 36 个同步波次执行 288 次
普通三表事务尝试，并确定性形成高、中、低三种冲突率。每个事务同时处理 counter/marker 两个
Key，marker 在 upsert/delete 间切换；独立参考模型和精确计数要求恰好 120 次提交、168 次
`Conflicts`、其它 Normal/Fatal/commit/rollback/timeout 失败为零。最终门禁同时检查 122 条合法
根 WAL、全部确认、repair、`.bak`、Btree overlay 清空，以及移走 WAL 后连续两次冷启动时
LogOrdered/Btree 的逐 Key 最终值；Memory 只由 WAL 恢复，不被误判为拥有独立数据文件。

首次触表顺序不是固定的 Memory/LogOrdered/Btree 顺序。`tests/ordinary_multi_table_ordering.rs`
逐一提交三表全部六种排列，并让六个排列根对同一组三表新 Key 通过异步 barrier 并发 prepare。
由于每个根按自身 child list 串行建立表级预留，交叉顺序允许全部冲突，也允许恰有一个 winner；
不承诺固定 winner 或至少一个成功。但绝不允许两个 winner、非普通冲突错误、提交前值泄漏或
rollback 后预留残留。所有失败根关闭后，全新根必须成功重试；最终仍以根 WAL repair、确认、
`.bak`、overlay 归零及两次 data-only 冷启动的精确数据状态为门禁。

普通 `upsert` 遇到缺表当前返回 Fatal。该分支发生在事务 manager start 和根 WAL 之前，批次
前缀也只存在于事务私有 COW/overlay；但“当前没有共享副作用”不表示 Fatal 可以 rollback。
调用方必须立即丢弃整个根，不得再 query/delete/prepare/commit 或复用。`kv_action_contract`
使用独立根分别验证缺表读删、Fatal 写、非法 rollback 被拒绝及新根所见数据/manager/WAL 均未
变化；future 被取消同样不会自动 rollback/finish，服务端必须由独立 owner 将 2PC 推进到明确
终态。

显式只读事务推荐在读取完成后直接释放，不进入 2PC。当前实现仍兼容对只读根调用 prepare；
一旦这样做，根会登记到 manager 并返回空 token，必须继续 commit 才能注销。可写事务即使
只有读动作、prepare 输出为空，也必须执行完整事务树 commit，以释放 prepare 阶段建立的读
预留；空输出只会跳过 WAL append/flush。删表仍属于普通事务；`table_meta` 和位于首次业务动作
前的 `create_table*` 可以作为普通/版本共享 schema prelude，完整 DDL rollback/取消原子性仍
不保证。

### 普通事务公开 2PC

普通事务在 `prepare_modified` 与 `prepare_modified_conflicts` 中二选一。两者执行相同的
事务启动、共享 TID/CID、表级冲突检查、prepared 预留和 WAL token 聚合；差异只在普通表冲突
的公开错误投影：前者返回 `Common(Normal, ..)`，后者返回首个
`Conflicts(Table, Key)`，不提供版本协议 `AllConflicts` 的完整集合。

prepare 返回的 `Vec<u8>` 是与同一根最近一次成功 prepare 绑定的一次性 opaque token，调用方
只能逐字节原样传给一次 `commit_modified`。当前实现没有在 commit 前校验 token 的事务身份、
内容或重复使用；修改、截断、附加、跨事务交换和重复使用均不属于合法调用域。空 token 只表示
本次不执行物理根 WAL append/flush，不表示事务只读或可以跳过 commit。

只有事务实际处于 `ActionFailed`、`PrepareFailed` 或受支持的 `LogCommitFailed`，并且整棵树
没有 Fatal 时，才能调用 `rollback_modified`。成功 rollback 会关闭旧根，稍后重试必须创建
全新事务；Fatal 永不可 rollback。`commit_modified` 成功后 manager `finish` 只完成活动根注销，
不等待表数据文件、根 WAL 异步确认或 `.bak`。

完整签名、合法/禁止调用域、状态图、时序图、锁/取消/性能边界和真实证据见
`docs/ROOT_ORDINARY_2PC_CONTRACT.md`。当前上游 generic prepare 对未来 Fatal prepare 错误存在
等级降级风险，但五类内置表的合法 prepare 失败目前只产生 Normal，因此该分支在现有生产路径
不可达；任何表未来新增 prepare Fatal 前必须先修复并复验上游传播。

### Key 锁兼容钩子

`KVDBTransaction::lock_key/unlock_key` 当前不是实际 Key 锁。五类表实现都会忽略 Key 并立即成功，
不提供排他、等待、owner、重入、内存可见性或事务隔离；未锁、非 owner、重复解锁和多个根同时
“锁定”同一 Key 都会成功，不能用它们保护业务临界区。

根层调用仍有事务副作用：它在查表前选择 Ordinary；缺表也保留该选择；首次命中已有表会创建
非持久化 managed 子事务，固定当时数据 COW/cache 根并租用版本 revision。后续同表普通写复用
这条较早冲突基线并按需提升 persistence。纯 hook 的空普通 2PC 不写 WAL、数据或版本，也不得
覆盖并发事务已经发布的新根。当前每次表 hook 还会分配一个首次 poll 即完成的 boxed future。

这是经接受的当前实现说明，不是最终或最佳锁 API。合法 Key 仍须是非空、匹配表类型且编码长度
不超过 `u16::MAX` 的规范 BON 数据；Meta 不允许外部直接操作，LogWrite 仍不允许外部使用。完整
调用链、取消/锁边界、真实矩阵与六项性能口径见 `docs/ROOT_KEY_HOOK_CONTRACT.md`。

当前事务安全保证以根 WAL append/flush 能在健康存储和可用 runtime 上完成为环境前提。根 WAL
自身因磁盘空间/配额不足、只读或故障文件系统、设备 I/O、runtime 拒绝任务、文件大小限制而失败
时，不保证事务仍具备原子性、可回滚性、checkpoint 收口或确定的重启结果。现有普通 I/O 错误不
携带失败阶段和累计写入字节；因此 `LogCommitFailed`、Normal 错误或 rollback 返回成功均不能在
这些阶段外情形中证明 WAL 完全没有落盘。该边界不是空 WAL：空 prepare 输出会直接跳过物理 WAL
写入。根 WAL 已成功后发生的子表数据文件持久化失败仍保留未确认 WAL，并继续适用既有启动恢复
语义。

### 根 WAL checkpoint 轮换

`KVDBManager::append_new_commit_log` 允许与正常事务提交并发。轮换必须保证事务 CID 登记的
checkpoint 就是该事务 WAL 实际写入的文件：当前实现会在根 logger 的 `check_points` 锁内先
把非空 current 块完整提交到旧 writable，再 split 并发布新 checkpoint。这样即使事务已经
append 但原始延迟 flush 尚未执行，也不会把其 WAL 写入轮换后的新文件。

该修复位于 `pi_store::CommitLogger/LogFile`，没有修改 `pi_db` 公开签名、WAL 格式、普通/版本
2PC、confirm、replay、`try_repair` 或表级日志语义。普通 append/flush 热路径只增加一次已提交
句柄的 relaxed 原子快路；非空轮换会多一次低频任务派发并等待原本就必须完成的 WAL sync。
公开 `LogFile::delay_commit` 成功只表示块写入和 waiter 唤醒，不是自动 split 已完成的文件
拓扑屏障；维护方必须等待显式 checkpoint/split 返回。

确认回收会把零长度只读 checkpoint 视为无需事务确认，但仍严格按队首连续顺序推进 `.bak`。
因此零长度中间文件不会永久阻塞后继 WAL，也不能让后继非空已确认 WAL 越过更早的非空未确认
WAL。`tests/root_wal_checkpoint_rotation.rs` 同时验证精确 replay、真实 manager/Memory 路径和
Btree `try_repair` 最终数据，并在移走全部根 WAL 后连续两次冷启动；完整方案、安全/取消边界、
修复前后性能和 E5 结果见 `docs/ROOT_WAL_CHECKPOINT_ROTATION_BUG.md`。

### 根 WAL 表片段格式

根事务的 prepare buffer 以 16 字节事务 ID 开始，随后按子表 prepare 顺序拼接零个或多个表
片段。每个片段的稳定布局是：

```text
table_name_len:u16-le
table_name:[u8; table_name_len]
action_count:u64-le
repeat action_count times:
    key_len:u16-le
    key:[u8; key_len]
    value_len:u32-le
    value:[u8; value_len]
```

`value_len == 0` 只表示 delete tombstone，不表示合法空 Value。数据库的持久化协议严格禁止
写入长度为 0 的 Value；当前普通 upsert 入口尚未统一实现该拒绝，因此调用方不得传入空 Value，
该实现偏离继续按 `FIND-DATA-001` 归档，不能把运行时暂时接受解释为稳定能力。合法表名为
`1..=4096` UTF-8 字节，能够进入版本协议的 Key 为 `1..=u16::MAX` 字节；codec helper 本身是
受信内部边界，不会为每项重复做结构化校验。

解码 helper 接受调用方提供的 offset，供根 TID 前缀和多表片段顺序解析使用，并返回下一片段
的精确 offset。当前实现对截断、伪造长度或非法 UTF-8 可能 panic，只能用于经过根日志外层校验、
由当前 `pi_db` 生成的完整 WAL，不能直接处理不可信网络输入或任意损坏文件。损坏输入的可恢复
错误策略仍属于独立 `FIND-CODEC-001`，本轮没有改变 WAL 字节格式或 replay 行为。

`tests/table_wal_codec_contract.rs` 独立验证非零起始偏移、多表拼接、4096 字节表名、65535 字节
Key、非空 upsert、delete 占位及最终 offset；真实 WAL 落地、提交确认和 repair 仍由事务/恢复
专项负责。`benches/table_wal_codec.rs` 只建立纯 CPU、分配和字节复制基线，不代表文件 I/O 或
完整事务延迟。

### 协议中立建表前导

一个全新的可写根事务在选择业务协议前，可以先调用 `table_meta` 检查定义，并在缺表时调用
`create_table/create_table_with_options`；随后可以在同一根事务中操作新表并二选一进入普通 2PC
或 `prepare_with_version/commit_with_version`。纯 `keys/values` 流和空动作也不会选择业务协议。
建表必须先于任何非空普通/dirty KV、`remove_table`、普通 prepare 或版本 prepare；普通和版本
业务 API 仍禁止在同一事务树中混用。

`table_meta` 是协议中立点读，不登记普通 Read，也不加入版本 read-set。`create_table*` 使用内部
唯一的 Schema Meta 子事务，参与同一根 TID、冲突、WAL、commit、异步确认、rollback prepared
清理和 Key 版本发布，但不会出现在 `commit_with_version` 返回的业务 `TableKeyVersion` 中。
`table_meta` 本身也可在尚未 prepare 的普通事务中读取当前 Meta 私有/已提交视图；它不因此改变
根协议。外部不得通过普通或版本 KV API 直接读写/删除内部 `.tables_meta`。

建表成功返回不等于事务提交；调用方仍须完成所选 2PC。当前 DDL 尚不具完整 rollback/取消
原子性，prepare 失败、rollback 或取消后，进程 registry、目录或文件可能已经存在；本能力不提供
补偿删除或同根事务重试。

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

### 根事务普通与 dirty 点读

`KVDBTransaction::query/dirty_query` 只在根事务上使用，批量返回与输入严格等长同序；重复
Key 会重复返回，缺表返回 `None`，输入 `TableKV.value` 被忽略。非空调用选择 Ordinary 协议，
普通族与 dirty 族不得在同一根中混用；空批次保持协议中立。

根事务不是数据库级统一快照。每张表在首次触达时惰性创建子事务：Memory/LogOrdered 固定该
时点的 COW 根；Btree 固定 overlay，但 overlay 缺席时每次重新读取 redb。因此同一根首次访问
不同表可以跨越其它事务的提交边界，同一 Btree 根两次查询也可能返回不同 redb 值。Btree
prepare 仍保留第一次读取确定的冲突基线，不会因后一次返回新值而刷新。

显式只读根可以在查询后直接释放，不分配事务/提交 ID，也不写 WAL。可写根即使只有读动作，
仍必须完成普通 prepare/commit；空 prepare 输出不代表可以跳过 commit。普通 query 会建立读
冲突；dirty Memory/LogOrdered 不建立 Read 冲突，dirty Btree 仍委托普通 query。完整契约、
真实三表/重启/TSan 专项和 Release 基准见 `docs/ROOT_QUERY_CONTRACT.md` 与
`tests/root_query_contract.rs`。

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
`KVTableTrError::AllConflicts(Vec<TableKeyConflict>)` 返回完整、去重、确定顺序的
`Table/Key/Kind` 集合。`ReadSetVersionMismatch` 表示版本缺失、TTL 淘汰、版本不等或只读表身份
变化，外部必须失效 Value/Version 并重新读取；`TransactionConflict` 表示 revision、值状态或
prepared 预留阻止本次事务。冲突项不返回 expected/current Version；同一 Table/Key 同时观察到
两类原因时版本失配优先。两者都是非 Fatal 失败，调用方均须 rollback，并用全新事务重试。
完整分类和下游 wire 迁移见
[`VERSION-CONFLICT-KIND-001`](docs/VERSION_CONFLICT_KIND_DESIGN.md#version-conflict-kind-design-index)。

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

TTL 非零但轮询间隔为 `Duration::ZERO` 属于非法启动配置，`startup*` 会在创建数据库目录、表、
事件 channel 和后台任务之前返回 `io::ErrorKind::InvalidInput`。TTL 本身为 ZERO 时轮询间隔完全
不生效，因此 interval 为 ZERO 也合法，并且不会创建版本淘汰任务；同一缓存记录会一直保留到
提交覆盖、删表、启动清空或数据库对象释放。真实配置边界由
`tests/manager_startup_configuration.rs` 验证。

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

### Trace 指标观测

启用 `trace` feature 后，既有 15 秒 tracing loop 会通过同一 OpenTelemetry Meter 上报以下指标：

该进程级共享 Meter 的 instrumentation scope 固定为 `pi_db`。宿主需要导出指标时，必须在
启动数据库前通过 OpenTelemetry `global` 安装并持续持有 MeterProvider；未安装 Provider 时按
OpenTelemetry 标准语义使用 no-op Meter，但 tracing loop 仍会正常运行，不会等待旧版
`pi_logger::opentelemetry::is_init()` 状态。`Loop tracing succeeded...` 为 INFO 日志，是否可见
还取决于宿主日志 bridge 和过滤级别。

- `pi_db.db.key_version_cache_record_count{table}`：当前注册表中每张表的活动 Key 版本记录数。
- `pi_db.db.key_version_cache_estimated_memory_bytes{table}`：活动记录可归属的动态内存估值。
- `pi_db.db.key_version_query_calls{result=success|failure}`：`query_with_version` 调用结果。
- `pi_db.db.key_version_2pc_calls{phase=prepare|commit,result=success|failure}`：版本 2PC 阶段结果。
- `pi_db.db.transaction_lifecycle{event=created|closed}`：成功创建与最终 owner 析构的根事务数。

版本记录数和内存估值在结构插入、TTL exact-remove 和启动恢复清空处通过 `Relaxed` 原子增减；
采集不迭代表内版本 `DashMap`，也不进入 publication/prepare 临界区。API 和事务热路径只更新
内部原子，tracing loop 再按累计快照差量写 Counter。取消或 unwind 的已进入调用计为 failure；
数据库状态拒绝并返回 `None` 的事务创建不计 created。`closed` 只表示最后一个根事务 owner 已
析构，不等于数据库 close、事务管理器 finish、提交确认或迭代器释放。

内存 Gauge 使用 Key 的实际 `Vec::capacity()`，并估算 Map 逻辑 entry、`Arc<Vec<u8>>` 控制字段
及 TTL FIFO owner/slot；它不包含 allocator 舍入、DashMap 空闲 bucket、channel 空闲 block、
表数据、WAL、redb 页面缓存或 RSS，因此只能解释为稳定的活动缓存动态内存估值。删表后 loop
会为消失的表写一次零值。当前默认部署单进程只启动一个 `pi_db` 实例，指标沿用既有 `table`
单标签；同进程多实例的同名表会合并为同一 time series，这是观测边界，不影响数据库语义。

未启用 `trace` 时，上述字段、原子读写和采集代码均由条件编译移除，不增加默认构建运行时成本。

## 表级整理边界

`KVDBManager::ready_collect_table` 和 `collect_table` 是表存储维护入口，不属于事务 2PC，也不会
追加根 WAL。缺表当前静默成功；Memory 的两个阶段都是 no-op；Meta/LogOrdered 的 ready 阶段
会 split 各自表日志，collect 阶段会整理只读日志；Btree 的 ready 阶段为 no-op，collect 阶段
执行 redb 持久化提交和 compact。Btree compact 最多总计尝试三次：任意一次成功立即返回，
只有前两次失败各同步等待一秒，第三次失败返回可恢复的 `Normal` 错误。LogWrite 当前不允许
外部业务使用，本轮没有新增其动态测试。

`tests/manager_table_maintenance.rs` 在真实非空 Meta、Memory、LogOrdered、Btree 上验证当前成功
路径：三个独立普通根写入并删除部分 Key，等待根 WAL 全部确认和 Btree overlay 清空后逐表整理，
每个阶段都要求逻辑值、记录数、缓存字节、manager 状态和根 WAL 文件快照不变；整体移走已确认
WAL 后，两次独立冷启动还必须从 Meta/LogOrdered/Btree 数据文件恢复精确状态，Memory 业务值则
按其无独立数据文件的语义消失。

这些 API 当前会在等待表级 I/O 时持有 registry 读 guard，不应与 DDL 并发使用，也不提供全库
原子快照、一次性 ready token、幂等或取消保证。Btree compact 的历史重试/成功退出缺陷
`FIND-TABLE-001` 已按上述边界修复；同文件确定性测试精确约束调用/等待次数，真实 redb 活动
读事务专项验证三次失败会返回错误、释放整理 owner，流释放后可再次成功整理。完整证据见
`docs/BTREE_COLLECT_RETRY_BUG.md`。Linux 上的 `cleanup_buffer_after_collect_table` 只是进程级
`malloc_trim` 提示，可能同步阻塞且返回值非确定，不是表整理完成或数据安全的证明。

## 运行时验收范围

本轮线程安全验收实际解析并测试了 `pi-async-rt 0.5.2`，包括正确 ABI 的 TSan 和 ASan。
`Cargo.toml` 仍保持 `pi-async-rt = "~0.5"`，不限制下游选择具体 `0.5.x` 版本。因此本轮
sanitizer 结论只适用于已测试的 `0.5.2` 依赖图，不能自动外推到下游自行解析的历史版本。

## Key 版本协议验收基线

本轮正式无 patch 复验实际解析 `pi_async_transaction 0.12.2`、`pi-async-rt 0.5.2` 和
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

## 公共核心值对象验收基线

`Binary`、表类型/Meta、QoS/动作载荷、版本载荷、`TableKV`、`KVTableTrError`、
`CreateTableOptions` 和 `KVDBEvent` 已按当前合法调用域完成契约校准。该切片没有修改公开 API
签名、生产执行逻辑、事务/WAL/repair/event 语义或下游协议；只补充公开注释、严格值对象测试和
独立纯 CPU 基准。`TableKV::value` 必须由具体 API 解释：版本 write-set 中 `Some` 为 upsert、
`None` 为 delete，普通根 `upsert` 跳过 `None`，而普通 `query/delete` 忽略输入 value。

`nightly-2026-06-25` 下，值对象目标 Debug/Release 均 `12/12`，options/events 目标均 `3/3`；
完整新回归共 128 个成功结果块、273 项测试通过、0 失败，旧 `tests/test.rs` 未进入。当前机器的
首份基准为：4 KiB `Binary` 共享 clone `2.84ns`、slice 复制 `49.00ns`、Hash `581.05ns`，
规范 u64 比较 `91.20ns`，嵌套 Meta encode/decode `77.50/100.70ns`，4 KiB value 的
`TableKV` clone `10.54ns`。这些数值只用于同机同工具链回归，不是跨硬件 SLA。

损坏/截断 codec、非法或空 Key、持久化空 Value、事件表类型偏差和 `Binary::cmp` 的既有额外
格式化成本仍是独立归档项，不能因上述合法域验收而视为已解决。

## 公开 API 主线验收边界

本轮已按实际导出面逐组对账 crate 根值对象、`KVAction/KVTable`、Builder/Manager、根事务与
普通/版本 2PC、五类表、listener/trace、Inspector 和历史 debug logger。面向外部生产使用的
合法域均有独立新测试或对应的真实跨层专项；直接表构造和子事务 variant 仍只是框架兼容面，
外部必须通过 `KVDBManager/KVDBTransaction` 使用。LogWrite 和 `log_table_debug` 分别保持既有
静态 E2 暂挂，不因本轮收口而变成生产可用能力。

“主线验收完成”只表示所有公开组均已分类、支持域已有严格证据、禁止/暂挂/非目标域已有稳定
说明，不表示损坏输入、路径 containment、graceful shutdown、collector Join、统一 dirty 隔离、
真实 Key 锁、事件标签和 DDL 完整 rollback 原子性已经实现。完整逐项矩阵、测试入口、性能与
sanitizer 适用范围及保留问题见 `docs/PUBLIC_API_MAINLINE_ACCEPTANCE.md`；旧 `tests/test.rs`
永远不进入新回归入口。

## 普通 Memory 并发基准

`benches/ordinary_memory_concurrency.rs` 使用一套共享 8-worker 数据库 runtime、一套共享
8-worker 调用 runtime，并为每个 case 创建独立真实数据库、事务管理器、CommitLogger、根 WAL
和临时目录。九个 case 覆盖 1/2/4/8 writer 以及 0%、50%、75%、87.5% 的确定性 Key 冲突率；
每轮都精确断言 prepare 成功/冲突、commit/rollback 状态、manager 配平、WAL 增量和最终权威值。
当前 x86_64 样本使用 5 字节 BON Usize Key 和 7 字节 Value，并为每轮分配新 Key，因此描述
插入/冲突路径而不是固定旧 Key 更新稳态。

当前 `nightly-2026-06-25` Release 首基线为约 `10.09~10.11ms/波次`，派生尝试吞吐约从
`99.05` 扩展到 `793.04 attempts/s`。不同冲突率的延迟差值小于样本离散，不能解释为稳定性能
收益或退化；该结果只适用于当前持久化 Memory 和根 WAL 装配，不代表 LogOrdered/Btree、版本
协议、数据文件确认或跨机器 SLA。运行入口：

```text
cargo +nightly-2026-06-25 bench --locked --offline -p pi_db \
  --bench ordinary_memory_concurrency -- --nocapture
```
