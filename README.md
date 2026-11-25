# Kerniva

Kerniva 是 Go 实现的 Redis 协议兼容内存数据库，支持标准库 net 与 gnet 两种网络入口。当前毕设方案由 Go 处理请求和追加分段日志，独立 Rust 进程生成全量快照并回收旧段，Go 启动时通过 C ABI 复用 Rust 恢复逻辑。

当前处于原型工程化阶段。命令覆盖、故障恢复、跨语言生命周期和构建验证已经形成闭环，兼容性和负载规模仍需持续验证。

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
| go_log_group_max | 8 | 一个同步批次最多合并 8 个已排队请求 |
| 封存条件 | 4 MiB 或 30 秒 | 非空日志达到任意条件可封存 |
| 日志 | `./logs/kerniva-go-log.log` | 同时写 stderr，记录进程运行情况 |

`none` 在追加完成后确认，掉电可丢失未同步的数据。`sync` 等待文件同步。`max_memory` 按键和值的近似大小统计，每库应用 `noeviction`。TTL 由 Go 判定，快照保留绝对到期时间。

`go_log_group_max` 取 0 或 1 时逐请求同步。2 到 16 仅适用于 Go 日志的 `sync` 模式。合并只处理已经排队的请求，每条请求保留独立提交序号和日志帧。

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
