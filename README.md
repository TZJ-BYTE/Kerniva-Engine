# Kerniva

Kerniva 是 Go 实现的 Redis 协议兼容服务，支持标准库 net 与 gnet 两种网络入口。持久化使用项目自研的 Rust kerniva_engine，通过 C ABI 和运行时动态库加载接入 Go。

当前处于原型工程化阶段。命令覆盖、故障恢复、跨语言生命周期和构建验证已经形成闭环，兼容性和负载规模仍需持续验证。

## 主要能力

- String、Hash、List、Set、ZSet、Stream，以及 TTL、多逻辑数据库和批量操作。
- MULTI / EXEC / WATCH、发布订阅、阻塞列表和 Stream 消费组。
- Lua 库白名单、超时、指令与采样分配预算。
- 内存模式和 Rust 持久化模式，支持 lazy_load 与 load_all。
- segment log、原子写批次、checkpoint 校验、快照引用保护和条件空间回收。
- 同步持久化时的异分片热读并行、提交后缓存发布，以及独立的 Rust 已提交索引读取。
- 配置校验、轮转日志、优雅停止，以及覆盖 Rust 和 Go 的测试脚本。

COMMAND 返回命令及子命令信息。具体参数边界以命令实现和回归测试为准。

## 构建与运行

需要 Go 1.25.8 或兼容版本、Rust 1.89 或更新版本，以及对应平台的 Rust 链接工具链。本机验证使用 Rust 1.95。Windows 可以使用 MSVC Build Tools，或完整的 MinGW GNU 工具链。Go 的 -race 检测另需 C 编译器。

### Windows

~~~powershell
./scripts/build.ps1
./kerniva.ps1 start
./kerniva.ps1 status
./kerniva.ps1 client 127.0.0.1 16379
./kerniva.ps1 logs --follow --tail 200
./kerniva.ps1 stop
~~~

### WSL / Linux / macOS

WSL 2 是主要开发和性能测试环境。将仓库克隆到 Linux 家目录下，在 ext4 上构建并运行。

~~~bash
bash scripts/build.sh
bash kerniva start
bash kerniva status
bash kerniva client 127.0.0.1 16379
bash kerniva stop
~~~

构建脚本将服务端、客户端和 Rust 动态库放在 bin/。启动脚本在服务端文件缺失时构建，修改源码后使用 kerniva build 更新产物。

从项目根目录前台运行并连接。

~~~bash
./bin/kerniva-server -config ./config.yaml
redis-cli -h 127.0.0.1 -p 16379 PING
~~~

Windows 服务端文件名为 kerniva-server.exe。部署时将平台对应的 kerniva_engine.dll、libkerniva_engine.so 或 libkerniva_engine.dylib 放在服务端旁边，也可通过 KERNIVA_ENGINE_LIBRARY 指定绝对库路径。库在运行阶段加载，构建由构建脚本负责。

## 配置

-config 默认指向 config.yaml。文件缺失时使用内置默认值并记录警告。YAML 字段拼写错误、格式错误和非法值会使启动失败。KERNIVA_* 环境变量覆盖文件，例如 KERNIVA_PORT、KERNIVA_NETWORK_TYPE、KERNIVA_MAX_MEMORY、KERNIVA_PERSISTENCE_ENABLED 和 KERNIVA_PERSISTENCE_DURABILITY。

| 项目 | 仓库配置 | 语义 |
| --- | --- | --- |
| 监听 | 127.0.0.1:16379 | network_type 为 std 或 gnet |
| 数据库数量 | 16 | 每库独立缓存和持久化目录 |
| max_memory | 268435456 | 每库近似 payload 缓存预算，0 表示无限制 |
| 持久化 | true / kerniva | 每库位于 data/db_N |
| 冷启动 | lazy_load | 恢复 Rust 索引，按需解码 Go 缓存 |
| durability | sync | 每个 record 或 batch 提交时执行 sync_all |
| segment 大小 | 4194304 | 达到阈值后轮转 |
| checkpoint 阈值 | 1024 | 维护调用检查累计操作数 |
| 日志 | info / ./logs/kerniva.log | 同时写 stderr，默认 100MB、3 份备份、保留 7 天 |

none 在 append 完成后返回，flush 调用文件 flush，两者成功回复均允许掉电丢失。sync 在文件同步成功后回复。旧别名 wal 对应 flush，wal_fsync 和 lsm 对应 sync。persistence_write_mode 保留兼容入口，实际确认边界由 durability 决定。

内存预算按 key 和 DataValue 的近似大小统计。used_memory_bytes 包含并发 SET 的在途预留，Database.GetStats 中的 pending_memory_bytes 是其中的暂存分项，当前 INFO 尚未输出该分项。Go/Rust 索引、map 开销、连接缓冲和运行时堆需要额外预算。持久化模式超限时卸载 Go 解码缓存，Rust 中的逻辑 key 继续保留。

TTL 由 Go 管理。后台抽样清理缓存中的过期 key，冷数据在读取和键枚举时判断过期并触发持久化删除。长期无人访问的过期冷数据会继续占据磁盘空间，直到访问删除和后续 cleaner 回收。

并行热读适用于缓存命中且无 TTL 的值。noeviction 策略下，已缓存的无 TTL 普通字符串 SET 在暂存预算允许时与异分片读取并行，提交成功后发布缓存。冷键、TTL、同分片争用及复杂命令的等待边界见 [毕设设计说明](docs/thesis_read_isolation_design.md)。

## 整体框架

~~~mermaid
flowchart TD
    A[TCP std / gnet] --> B[RESP 解析与有界连接队列]
    B --> C[连接 worker 与执行协调]
    C --> D[命令 / 事务 / Lua / PubSub]
    D --> E[DBManager 与分片缓存]
    E --> F[变更暂存 / 失败回滚 / 写批次]
    F --> G[运行时 C ABI 桥接]
    G --> H[Rust BTreeMap 与 segment log]
    H --> I[checkpoint / snapshot / cleaner]
~~~

审计过的单 key 请求共享执行门。复合命令及 EXEC 使用排他执行门。数据库以操作锁、分片锁和变更日志协调缓存、WATCH 版本和持久化提交。Rust 批次是单引擎的恢复原子单位。

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
internal/rustengine/        跨平台动态库加载与 C ABI
kerniva_engine/             Rust 内核、C header 与示例
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

TCP benchmark 应对隔离实例执行，写入前缀为 kerniva:bench:。

~~~bash
go run ./scripts/bench -addr 127.0.0.1:16379 -clients 1,16,64 -requests 10000 -network std -durability memory
~~~

-network 和 -durability 记录标签，服务端模式由服务端配置决定。基准包含 32B GET/SET、逐回复校验和 p50/p95/p99。每条连接按请求与回复顺序执行。

## 文档

- [后续开发路线与 WSL 主环境决策](docs/architecture/adr-0001-wsl-rust-memory-core.md)
- [Rust 内存内核无人值守开发计划](docs/rust_memory_unattended_plan.md)
- [整体架构与数据流](docs/kerniva_engine_architecture.md)
- [存储格式、恢复与 C ABI](docs/kerniva_engine_design.md)
- [五阶段改进和后续计划](docs/project_review_and_plan.md)
- [性能验证与复现方法](docs/performance_validation.md)
- [毕设读隔离设计与三虚拟机后续计划](docs/thesis_read_isolation_design.md)
- [第二轮 WSL 与 Redis 对照实验](docs/thesis_second_overnight_report.md)
- [首轮实验、反向复测与验收报告](docs/thesis_experiment_report.md)
- [Rust 独立引擎说明](kerniva_engine/README.md)

项目由 TZJ-BYTE 维护。问题可提交至 [项目 issue 页面](https://github.com/TZJ-BYTE/Kerniva-Engine/issues)。
