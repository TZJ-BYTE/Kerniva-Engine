# Kerniva 整体架构

本文对应 2026-11-15 工作区实现，覆盖 Go 服务与 Rust 内核。本轮尾延迟优化的具体设计见 thesis_read_isolation_design.md。

## 目标与当前阶段

项目面向 Redis 风格的 KV 命令负载，以 Go 承担协议、命令和数据类型语义，以 Rust 承担追加日志、索引、恢复和空间回收。当前完成原型的工程化改造，兼容性和负载规模仍需持续验证。

存储主结构为 segment log、BTreeMap 内存索引和 checkpoint。Rust 核心使用标准库，通过 C ABI 暴露能力。

## 模块与职责

| 模块 | 责任 |
| --- | --- |
| cmd/server | 配置、日志、pprof、信号与退出 |
| config | 默认配置、严格 YAML、环境覆盖与校验 |
| internal/protocol | RESP 长度限制、增量解析和响应编码 |
| internal/server | std/gnet 入口、连接队列、执行协调、事务和阻塞等待 |
| internal/command | 参数、命令、元数据与 Lua |
| internal/database | 多库、缓存、TTL、预算、WATCH 与变更提交 |
| internal/datastruct | String/List/Hash/Set/ZSet/Stream 表示与序列化 |
| internal/rustengine | 动态库发现、平台加载、句柄生命周期与 buffer 释放 |
| kerniva_engine | 日志、批次、索引、恢复、checkpoint 与 cleaner |

## 请求与并发模型

~~~mermaid
sequenceDiagram
    participant Client as 客户端
    participant Network as std / gnet
    participant Worker as 连接 worker
    participant DB as 逻辑数据库
    participant Engine as Rust Engine
    Client->>Network: RESP 请求
    Network->>Worker: 解析并加入有界队列
    Worker->>DB: 按连接顺序执行命令
    alt 缓存命中且符合并发条件的普通 SET
        DB->>DB: 预留预算并构造私有待提交值
        DB->>Engine: WriteBatch
        Engine->>Engine: append + durability + 发布索引
        Engine-->>DB: 成功或错误
        DB->>DB: 成功后发布缓存、版本与通知并结算预算
    else 其他持久化变更
        DB->>DB: 排他协调并记录变更前状态
        DB->>Engine: 原子 WriteBatch
        Engine-->>DB: 成功或错误
        DB->>DB: 发布版本或回滚本次变更
    end
    DB-->>Worker: 返回结果
    Worker-->>Client: 按序编码响应
~~~

网络回调负责解析和排队。连接 worker 负责有序执行。输入队列、客户端缓冲和发布订阅输出具有预算及取消路径，阻塞命令通过连接上下文参与退出。

全局执行门使用读写锁。审计过的单 key 操作共享门，复合命令和 EXEC 独占门。数据库操作锁协调多 key 变更与生命周期。缓存命中且无 TTL 的读取共享数据库锁，并使用分片热锁。noeviction 策略下，已缓存、无 TTL 的普通字符串 SET 在预算允许时以私有待提交值完成同步持久化，再发布缓存。缓存未命中、TTL、类型变化和预算压力进入排他协调路径。持久化写入由 Rust writer Mutex 串行提交。

MULTI 先校验参数并复制入队。WATCH 记录 key 版本和库 epoch。EXEC 在排他门内校验并执行队列。队列内每个命令仍按其数据库 API 边界提交持久化。

## 数据表示与缓存

每库维护分片 map，DataValue 保存类型值、过期时间和访问时间。对外读取返回独立值或受控表示，需要跨调用保存的输入 byte slice 在存储时复制。

max_memory 按每库统计 key 和近似 payload。内存模式执行淘汰策略或返回 OOM。持久化模式卸载 Go 缓存并保留 Rust 逻辑 key。并发字符串 SET 先预留待提交值与序列化空间的近似预算，成功或失败后结算。used_memory_bytes 包含预留，Database.GetStats 中的 pending_memory_bytes 是其中的暂存分项，当前 INFO 尚未输出该分项。Rust BTreeMap、连接缓冲和语言运行时内存需要额外部署预算。

ZSet 维护 member 到 score 的查找表和有序切片。单成员变更二分定位后移动切片，批量添加合并后排序一次，切片移动仍有 O(N) 成本。

Stream 序列化覆盖消息、消费组、PEL、消费者和投递计数，保留历史格式读取兼容。

## 冷启动与 TTL

lazy_load 恢复 Rust 索引，按需读取并解码 key。load_all 枚举持久化数据后加载，同时执行缓存预算控制。no_load 保留历史配置入口。

缓存 TTL 每 200ms 尝试抽样最多 64 个候选。后台扫描依次尝试执行门的 TryRLock 和数据库的 TryLock，忙时延后本轮，避免排队写锁阻止新的热读取进入。带 TTL 的前台访问走协调路径检查逻辑过期。冷持久化 key 在读取、存在性和键枚举等访问路径判断过期并触发删除。磁盘空间在 tombstone 提交后由 cleaner 回收。持续繁忙时的物理清理进度以及冷 TTL 全库扫描预算需要专门测量。

扫描器 Start、Stop 与 Close 由 expireMu 串行协调。Close 先禁止再次启动并等待 worker 退出，再取得数据库排他锁关闭存储。

维护周期约 10 秒，执行条件空间回收并检查 checkpoint 阈值。维护使用 Rust 锁，与前台写入存在竞争。大索引 checkpoint 和大 segment cleaner 的单次耗时仍需压测。

## Rust 写入路径

1. 锁外复制、校验输入并构造记录。
2. 在 writer Mutex 内完成最终记录编码并追加到 active segment。
3. 按 None / Flush / SyncData 执行同步。
4. 成功后短暂取得独立 index 写锁，发布新的 BTreeMap 索引位置，批次整体发布。
5. append 或同步失败时尝试截断回滚，无法安全回滚时拒绝后续写入。

批次编码为 kind=3 的单条外层记录，包含子操作和整体 checksum。恢复仅应用完整批次。kind=4 为全库清空 marker，固定 20 字节，按同步策略成功后清除当前内存索引，旧快照继续保留原视图。旧 kind=1/2 记录继续可读。

## 读取与 Reader Cache

BTreeMap 的 O(log N) 查找得到位置。在索引读锁保护下取得目标 segment 文件句柄，然后释放索引锁并定位读取及校验。该路径可以在 writer 等待文件同步时读取已提交值。reader cache 最多缓存 128 个文件句柄，按逻辑访问时钟选择淘汰对象。

这是 Rust 直接 API 的并行能力。完整 RESP 服务的冷 GET 仍进入 Go 数据库排他协调，会等待在途 SET，并使该库后续热 GET 等待。

OS 定位读取避免共享 seek 位置问题。取得句柄与 cleaner 回收之间由锁及文件句柄生命周期协调。

## 文件布局与恢复

~~~text
data/db_N/
  LOCK
  segments/
    0000000001.seg
    0000000002.seg
  meta/
    checkpoint.bin
~~~

打开目录时先取得进程间独占锁，再加载 checkpoint，并从记录的 offset 回放增量 segment。最后一段的不完整尾记录可以截断。checksum 错误、非法长度、损坏 checkpoint 和中间段截断会返回错误。Go 启动路径传播打开错误。

checkpoint v2 校验完整文件，采用临时文件、文件同步、原子替换及平台目录同步。v1 checkpoint 保留读取兼容。checkpoint 持有 writer 保持提交边界稳定，索引读锁允许 GET 共享读取。

## Snapshot 与 Cleaner

snapshot 和 iterator 固定索引副本，pin 住涉及的 segment，并持有目录租约。旧段仍有租约时进入延迟回收集合，释放后再尝试回收。索引副本的创建成本随 key 数增长。

MaybeCompact 在 sealed segment 可回收字节大于 0 且占比达到 50% 后执行。等待 snapshot 释放的旧段参与延迟回收，避免重复改写。显式 CompactUntilStable 提供限定轮数的强制维护入口。回收先保存 checkpoint，再删除可回收旧段。

## 统计与生命周期

Rust stats 提供 keys、active segment、segment 数、reader cache、pinned segment 和 pending reclaim 数。Go INFO 还展示缓存、过期抽样及持久化错误等状态。

Go Engine 生命周期读写锁保护所有读写与 Close。C ABI 返回 native buffer 后，Go 复制内容，再调用 Rust free。输入指针通过直接转换和可见指针字段保留逃逸与生命周期信息。

## 跨平台交付

Windows 通过平台库加载 API、Linux/macOS 通过 dlopen/dlsym 找到符号，函数调用由 purego 完成。运行时查找显式 KERNIVA_ENGINE_LIBRARY、可执行文件目录和开发目录的预构建库。构建脚本负责 release 动态库与 Go 二进制的生成和打包。

CI 配置 Windows、Linux、macOS 原生测试矩阵。本机验证状态见改进记录。

## 一致性与资源边界

- 单引擎 WriteBatch 是崩溃恢复原子单位，覆盖同库多 key 数据库操作。
- EXEC 提供整段执行隔离，崩溃恢复可能保留已成功提交的命令前缀。Lua 多次 redis.call 同样逐命令持久化。
- 跨库 MOVE 先提交目标写入再删除源，源删除失败时尝试补偿目标写入，中途崩溃可能留下双副本。
- Lua 分配预算每 4096 条指令采样进程累计分配，可能包括并发请求，也允许采样间隙超限，属于软预算。
- none 和 flush 的成功回复允许掉电丢失。sync 等待本次提交的文件同步。
- Reset 通过单条 Clear marker 完成，无需读取历史 value。内存索引释放的成本随 key 数增长。
- WATCH 版本历史保留至 FLUSHDB，位于 payload 预算外，唯一 key 的高频创建和删除会增加该索引内存。

后续研究包括 group commit、增量快照与 cleaner、跨库恢复协调、MVCC 和冷 TTL 索引。每项都需要格式迁移、确认时点和故障回归设计。
