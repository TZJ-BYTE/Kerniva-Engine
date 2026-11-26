# Kerniva

Kerniva 是 Go 实现的 Redis 协议兼容内存数据库，支持标准库 net 与 gnet 两种网络入口。当前毕设方案由 Go 处理请求和追加分段日志，独立 Rust 进程生成全量快照并回收旧段，Go 启动时通过 C ABI 复用 Rust 恢复逻辑。

Go 主库提供两个持久化开关。`go_log_enabled` 控制每次变更是否写入增量日志，`snapshot_enabled` 控制定时保存全量快照。可在 YAML 或 Windows 受管实例的设置中分别开关。单机从已有 WAL 首次切到仅快照时，主库用同版 Rust 动态库读取旧日志，之后可直接从全量快照启动。重新开启日志时，主库会从最新快照为各逻辑库建立基线。[持久化开关、恢复及切换示例](docs/verification/persistence-modes.md)

数据库工作台提供原生桌面窗口、鼠标终端、浏览器和命令式终端。可连接本地或远程 Kerniva、Redis 单机和 Redis Cluster，在「数据浏览」切换当前逻辑库、分页查看键值。Windows x64 离线安装器自带工作台和单机引擎。在「连接 → 本机实例」选引擎和数据位置后即可部署，关掉桌面窗口仍能使用数据库。Windows 便携包的两个入口是 `Kerniva Workbench.exe` 和 `Kerniva-TUI.cmd`，Linux 包的两个入口是 `./kerniva-workbench` 和 `./kerniva-tui`。[工作台用法](docs/database_workbench.md) · [桌面打包与验收](docs/verification/native-workbench.md) · [Windows 安装与系统服务](packaging/desktop/windows-installation.md)

0.1.3 可以在「数据浏览」固定常用键。分别固定 DB 0 和 DB 1 的 `demo:marker` 后，重开桌面或鼠标终端，点对应位置即可读取该库的最新值。交互终端支持 `/pins`、`/pin-open ID` 和 `/unpin ID`。[演示、截图与安装包校验值](docs/verification/workbench-pinned-keys.md)

答辩演示使用受管的一主两从环境。选择 DB 0 或 DB 1 写入标记，暂停一台副本后继续写入，再恢复它并对照读值和应用序号。交互终端可用 `/help`、`/connect`、`/status`、`/get` 和 `/replica` 执行同一工作台的操作。[启动复制演示](docs/golog_replica_demo.md)

~~~powershell
.\kerniva.cmd workbench
.\kerniva.cmd shell
~~~

~~~bash
bash kerniva workbench
bash kerniva shell
~~~

## 主要能力

- String、Hash、List、Set、ZSet、Stream，以及 TTL、多逻辑数据库和批量操作。
- MULTI / EXEC / WATCH、发布订阅、阻塞列表和 Stream 消费组。
- Lua 库白名单、超时、指令与采样分配预算。
- Go 常驻分片内存、按库分段日志、原子写批次和绝对 TTL。
- Rust 独立调度快照、清单原子发布、封存段回收与恢复。
- 一主两从按逻辑库追赶封存日志，落后时用快照补齐。可通过 `kerniva-replica follow --snapshot-concurrency N` 限制单个副本的快照并发数，默认 `0`。[复制配置与恢复语义](docs/architecture/adr-0003-sealed-log-replication.md)
- 同步持久化时的异分片热读并行与提交后缓存发布。
- 配置校验、轮转日志、优雅停止，以及覆盖 Rust 和 Go 的测试脚本。

COMMAND 返回命令及子命令信息。具体参数边界以命令实现和回归测试为准。

## 构建与运行

需要 Go 1.25.8 或兼容版本、Rust 1.89 或更新版本，以及对应平台的 Rust 链接工具链。本机验证使用 Rust 1.95。Windows 可以使用 MSVC Build Tools，或完整的 MinGW GNU 工具链。Go 的 -race 检测另需 C 编译器。

### Windows

~~~powershell
./scripts/build.ps1
./bin/kerniva-server.exe -config ./config.golog.yaml
~~~

另开终端启动维护进程或使用客户端。

~~~powershell
./bin/kerniva-maintainer.exe --data-dir ./data/go-log-v1
./kerniva.ps1 client 127.0.0.1 16379
~~~

### WSL / Linux / macOS

WSL 2 是主要开发和性能测试环境。将仓库克隆到 Linux 家目录下，在 ext4 上构建并运行。

~~~bash
bash scripts/build.sh
./bin/kerniva-server -config ./config.golog.yaml
~~~

另开终端启动维护进程。以 `redis-cli` 或自带客户端对服务端发送命令。

~~~bash
./bin/kerniva-maintainer --data-dir ./data/go-log-v1
~~~

~~~bash
redis-cli -h 127.0.0.1 -p 16379 PING
redis-cli -p 16379 SET sample value
redis-cli -p 16379 GET sample
~~~

构建脚本把服务端、客户端、维护程序和 Rust 动态库放进 `bin/`。Linux 构建同时生成 `kerniva-replica`，可在干净检出上直接构建后用 `bash kerniva workbench --with-demo` 启动统一演示。Windows 可原生启动工作台，并通过 WSL 启动复制进程。动态库可用 `KERNIVA_ENGINE_LIBRARY` 指定绝对路径。`INFO` 显示后端、持久化规则、日志提交序号和封存积压。维护程序持续检查封存段，`--once` 可运行一次维护后退出。[Windows 与 WSL 启动示例](docs/golog_replica_demo.md)

## 配置

毕设运行入口为 `-config ./config.golog.yaml`。原来的 `config.yaml` 仍可读取旧格式数据。`KERNIVA_*` 环境变量覆盖配置文件。YAML 字段拼写错误、格式错误和非法值会使启动失败。

| 项目 | 仓库配置 | 语义 |
| --- | --- | --- |
| 监听 | 127.0.0.1:16379 | network_type 为 std 或 gnet |
| 数据库数量 | 16 | 每库独立缓存和持久化目录 |
| max_memory | 268435456 | 每库近似 payload 缓存预算，0 表示无限制 |
| 持久化 | go-log | 每库位于 `data/go-log-v1/db_N` |
| 冷启动 | load_all | Rust 从快照和日志恢复，Go 装入分片内存 |
| durability | sync | Go 同步日志文件后确认写入 |
| go_log_group_max | 8 | 每次磁盘同步最多合并的已排队请求数 |
| go_log_enabled / snapshot_enabled | 均省略 | 保留既有 Go 日志和外部 Rust 整理行为。显式设置可独立选择增量日志与定时全量快照 |
| snapshot_interval_seconds | 300 | 开启独立快照时两次定时保存的间隔，单位为秒 |
| 封存条件 | 4 MiB 或 30 秒 | 非空日志达到任意条件可封存 |
| 日志 | `./logs/kerniva-go-log.log` | 同时写 stderr，记录进程运行情况 |

`none` 在追加完成后确认，掉电可丢失未同步的数据。`sync` 等待文件同步。`max_memory` 按键和值的近似大小统计，每库应用 `noeviction`。TTL 由 Go 判定，快照保留绝对到期时间。

`go_log_group_max` 取 0 或 1 时逐请求同步。同步 Go 日志可以设置 2 到 32。写者只取已经排队的请求，每条保留独立提交序号和日志帧，整批同步成功后才确认。切回仅支持上限 16 的旧服务端前，先把配置调到 16 或以下。

## 整体框架

~~~mermaid
flowchart TD
    A[TCP std / gnet] --> B[RESP 解析与有界连接队列]
    B --> C[Go 连接 worker]
    C --> D[命令 / 事务 / Lua / PubSub]
    D --> E[DBManager 与分片缓存]
    E --> F[变更暂存 / 失败回滚 / 写批次]
    F --> G[Go 按库追加日志]
    G --> H[(封存日志段)]
    H --> I[独立 Rust 维护进程]
    I --> J[(按提交序号发布快照和 CURRENT)]
    H --> K[Rust 恢复]
    G --> K
    J --> K
    K -->|启动时 C ABI| E
~~~

数据库以操作锁、分片锁和变更日志协调缓存、WATCH 版本和持久化提交。Go 日志的一帧是单库恢复的原子单位。Rust 按封存段选择连续前缀，回收只覆盖已经进入快照的日志。

每库具有独立日志。EXEC 和 Lua 中多次命令调用按各自命令提交，崩溃后可能保留已提交的前缀。跨库 MOVE 先写目标再删源，中途崩溃可能留下双副本。

### 目录

~~~text
cmd/server, cmd/client      进程入口与客户端
config/                     YAML、环境覆盖与校验
internal/protocol/          RESP 解析与编码
internal/server/            std/gnet、连接、事务、PubSub
internal/command/           命令、元数据与 Lua
internal/database/          缓存、TTL、预算、变更日志与持久化
internal/datastruct/        数据类型、序列化与 ZSet 有序索引
internal/golog/             Go 日志编码、追加和封存
internal/gologreplica/      Linux 本地异步复制与副本安装
internal/rustengine/        跨平台动态库加载与 C ABI
kerniva_engine/             Rust 恢复、维护命令、历史引擎与 C header
pkg/logger/                 分级与轮转日志
scripts/                    构建、清理、测试和 TCP benchmark
.github/workflows/          Windows / Linux / macOS CI
~~~

## 验证与性能

2026-11-26 的 WSL gnet 五配置对照完成 40 个正式窗。50% 写、3,000 请求/s 和 95% 读、6,000 请求/s 共投递 540 万次，零失败。6 个窗的 SET p99 超过 100 ms，包含主库单跑时段。高读档随后复验的完整拓扑增量为 +0.442、+0.934 ms，未达到预登记的重复性阈值。[逐窗尾延迟、复制正确性与独立复算](docs/verification/replica-set-tail.md)

随后逐秒复算了 44 个旧窗，又在 95% GET、6,000 请求/s 下实测 24 个新窗，共 432 万请求，零失败。旧数据在 38 个慢秒出现大量客户端排队。新一轮仅在关闭新探针的校准窗复现尖峰，16 个正式窗的 SET p99 为 5.734–6.914 ms。[逐请求诊断、适用范围与复现命令](docs/verification/replica-request-stalls.md) 同一客户端又通过隔离回环服务验证，两次都区分了约 250 ms 的回包等待和后继约 241 ms 的发前排队。[已知慢回复的工具验收](docs/verification/replica-trace-fixture.md)

一次主库运行时跟踪捕获了 3,978 个超过 100 ms 的请求。主库第一个 GC 暂停发生在它们全部完成至少 6.086 秒之后。[GC 与写入尖峰的逐请求核对](docs/verification/primary-gc-stalls.md)

后续实测把单条 SET 对到了真实的日志批次，定位到慢同步与监控命令之间的全局锁等待。现已修复普通 SELECT、INFO 和工作台 PING 对跨库读的阻塞。让 DB 0 同步返回延迟 350 ms 时，DB 1 的 GET 从 204–205 ms 降至 0.56–0.77 ms。六个自然窗口的 108 万请求全部成功，修复版本仍复现过整体长尾。[逐批数字、修复前后对照及复现命令](docs/verification/observer-execution-gate.md)

修复版又完成 144 万次请求，两个自然窗口出现至少 100 ms 的延迟。其中一个窗口在 24 个长同步内取得 72 个完整的 Windows 整盘采样间隔，仅记录到一次 8 KiB 写入完成；另一个窗口的慢同步伴随来宾 I/O 压力。逐请求记录显示写队列和同连接后继请求的发送等待都会放大长尾。[Windows 与 WSL 同窗实测、已知延迟对照和复算](docs/verification/primary-host-disk.md)

内核跟踪复验完成 72 万次请求，零失败。一条 SET 的回包用了 635.784 ms，其中在服务端写队列等待 365.817 ms，所在批次的同步耗时 269.653 ms。写线程在 ext4 内核路径睡眠等待了 269.593 ms，唤醒后等 CPU 合计 0.030 ms。两个追踪窗里，6 个达到 10 ms 的测量批次都核对到了同一活动日志的 fsync。[逐批内核时序、220 ms 对照与复现命令](docs/verification/primary-kernel-writer.md)

后续两组真实负载中，51 个可逐笔核验的慢同步批次累计睡眠 1,435.613 ms，其中 95.1% 在等 ext4 提交日志。另做 8 窗配置对照，234 万次请求全部成功。把封存年龄从 1 秒改到 30 秒，DB 0 每窗新封存的文件从 43–44 个降为 2 个，50% 写入、每秒投递 3,000 次时仍有明显排队。两种设置均通过进程崩溃恢复，保留 30 秒默认值。[单笔拆解、八窗对照及复跑方法](docs/verification/primary-journal-load.md)

WSL gnet 单主、同步 WAL、关闭独立快照，在 64 连接、10 万个键、50% 写、3,000 请求/s 的两组反序实测中，把 `go_log_group_max` 从 16 调至 32，耗时至少 500 ms 的请求分别减少约 78% 和 94%。四个 125 秒生产窗口各完成 375,000 次请求，零失败，强杀重启后两个库的序号和抽样值一致。上限 32 的两窗仍各有 552–559、236–245 次请求达到 500 ms。另四个带同步计数的高读对照窗（95% 读、6,000/s）GET p99 为 1.27–1.34 ms，均无请求达到 100 ms。[逐窗对照、单笔跟踪与复现方法](docs/verification/primary-durable-queue.md)

Windows 与 WSL 的一次联合录制复现了 611.8 ms 的慢写，其中 579.3 ms 在等同一连接上的前序请求。115 个半秒级请求集中在约 0.8 秒内；这段时间宿主数据盘的刷新中位数为 14.62 ms，其余正式时段为 0.56 ms。两个存储层都记录到长刷新，单笔来宾命令与宿主请求之间的对应关系尚未确认。[联合实测与证据索引](docs/verification/primary-durable-queue.md#真实联合窗口的结果) · [联合采集命令](docs/verification/primary-durable-queue.md#把同一次慢写对齐到-windows-磁盘事件)

固定同步 WAL，分别关闭和开启 15 秒周期快照，8 个 WSL gnet 窗口共完成 234 万次请求，全部成功并通过强杀恢复。开快照时共观察到 16 次正式窗内发布，高写和高读的实测快照各有一份通过独立冷启动。该对照发现旧版在快照开启时，高读 GET p99.9 两组均上升。[快照开关对照和恢复核验](docs/verification/primary-snapshot-load.md)

Go 主库现按存活条目分块收集快照，减少在全局执行锁内复制数组的时间。在同样开启快照、95% 读、6,000/s、10 万个压测键的无探针对照中，全窗 GET p99.9 两组分别从 11.01 降至 1.68 ms、从 13.11 降至 1.63 ms。GET p99 仍约为 1.3 ms。[持锁测量、无探针对照与兼容性核验](docs/verification/primary-snapshot-load.md#最终实现的无探针对照)

~~~powershell
./scripts/test.ps1 -Race
~~~

~~~bash
bash scripts/test.sh -race
~~~

脚本依次运行 Rust 测试、构建 release 动态库和 Go 二进制，再运行 Go 测试。原生库缺失或任一步失败会使脚本失败。原生持久化回归使用临时目录。

从 WSL 的 ext4 仓库执行 `bash scripts/golog-unattended.sh --profile formal --redis-rewrite`，可依次跑正确性门禁、Redis 对照、后台维护和恢复测试。

WSL 的一主两从验证运行 `bash scripts/build.sh` 后执行 `python3 -B scripts/golog-replica-check.py`。它使用独立测试目录和端口，覆盖增量追赶、断线跨回收点以及重启恢复。副本通过本地 HTTP 查询已同步的字符串和位置。普通增量只校验和应用新封存段，快照和重启走完整恢复。 [复制设计与命令](docs/architecture/adr-0003-sealed-log-replication.md)

加 `--stress` 可同时验证并发写入、Rust 自动快照、从库下载中断、主库强制重启和三个节点的逐键核对。再加 `--stress-seconds 60` 会继续读写至少 60 秒，核对新的快照和从库位置。用 `--stress-retain-segments 8` 可验证整理进程保留最多 8 个已覆盖段时的增量追赶和快照补齐。 [验收记录](docs/golog_grouped_replica_report.md)

从旧备份恢复为新主库前，在恢复目录执行 `kerniva-replica rotate-source-id`，然后让新副本从空目录同步。常规重启保留已有来源 ID。 [恢复步骤](docs/architecture/adr-0003-sealed-log-replication.md#从备份恢复主库)

TCP benchmark 应对隔离实例执行，写入前缀为 kerniva:bench:。

~~~bash
go run ./scripts/bench -addr 127.0.0.1:16379 -clients 1,16,64 -requests 10000 -network std -durability memory
~~~

-network 和 -durability 记录标签，服务端模式由服务端配置决定。基准包含 32B GET/SET、逐回复校验和 p50/p95/p99。每条连接按请求与回复顺序执行。

## 文档

- [毕设最终架构与验收方案](docs/architecture/adr-0002-thesis-go-cache-rust-segments.md)
- [Go-log 主库、Rust 快照和只读副本架构图](docs/architecture/kerniva-current-architecture.svg)
- [无人值守实施与对照计划](docs/golog_unattended_plan.md)
- 副本安装同一份 64 MiB 增量，四个时段的 64 窗均低于 2 秒。两轮截断恢复及一主两从断源重启通过。[验收与复现](docs/verification/replica-install-latency.md) · [逐窗数据](docs/verification/replica-install-latency.json)
- 真实 TCP 主库与 Rust 整理、一主两从同时运行的对照，覆盖 50% 写 3,000/s 和 95% 读 6,000/s。[主库尾延迟、复制可见时间与复现](docs/verification/replica-live-load.md)
- [Go 日志与 Rust 快照 WSL 实测](docs/golog_wsl_report.md)
- [合并同步与复制 WSL 实测](docs/golog_grouped_replica_report.md)
- [副本快照恢复的内存归因、峰值对照与断源重启](docs/verification/replica-recovery-memory.md)
- [16 库副本每次空轮询的开销、连接复用收益与新写入可见延迟](docs/verification/replica-idle-polling.md)
- [副本读取缓冲区复用、常驻内存及 64 MiB 增量追赶实测](docs/verification/replica-reader-reuse.md)
- [统一工作台验收](docs/verification/unified-workbench.md)
- [现阶段验收计划](docs/golog_next_stage_plan.md)
- [历史 Rust 主存内核实验记录](docs/rust_memory_wsl_report.md)
- [历史 WSL 与 Redis 对照实验](docs/thesis_second_overnight_report.md)

项目由 TZJ-BYTE 维护。[提交或查看问题](https://github.com/TZJ-BYTE/Kerniva-Engine/issues)
