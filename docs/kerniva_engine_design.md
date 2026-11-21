# Kerniva Engine 设计文档

本文对应 2026-11-15 实现。整体服务框架见架构说明，本页集中描述存储格式、提交和恢复。

## 1. 设计目标

面向 Redis 兼容 KV 负载提供可嵌入的 Rust 存储内核，通过 C ABI 服务 Go 及其他语言。核心代码使用 Rust 标准库，数据格式、恢复、checkpoint 和空间回收由项目管理。

提交策略明确成功回复的边界，记录和 checkpoint 校验用于识别损坏，快照租约保护空间回收。

## 2. 架构总览

~~~text
Go 数据库层
  -> runtime loader + purego C ABI
    -> Engine
      -> writer Mutex / active segment / sealed segments
      -> index RwLock / BTreeMap<key, ValueLocation>
      -> checkpoint / reader cache / snapshot registry
~~~

Windows 使用平台动态库加载 API，Linux/macOS 使用 dlopen/dlsym。库由构建脚本预先生成，默认与服务端一起部署。

## 3. 核心数据结构

### 3.1 Record

~~~text
magic(4) + version(1) + kind(1) + reserved(2)
+ key_len(4) + value_len(4) + checksum(4)
+ key bytes + value bytes
~~~

magic 为 RDG2，整数采用小端编码，头部为 20 字节。kind=1 为 Put，2 为 Delete，3 为 Batch，4 为 Clear。长度和算术溢出在编码及恢复时校验。checksum 覆盖记录类型和内容，用于识别损坏。

Batch 的 key 为空，value 为操作数量 u32 和若干完整 Put/Delete 子记录。外层 checksum 覆盖整个批次，操作数和 FFI 数组长度上限根据 u32 payload 及目标平台可寻址范围校验。恢复验证整体后才应用子操作，旧 Put/Delete 格式继续可读。

Clear 的 key/value 均为空，固定 20 字节。提交同步成功后清空当前索引，恢复时验证完整 marker 后执行清空。它保留旧快照引用的 segment。索引释放仍随 key 数增长。

### 3.2 ValueLocation

~~~rust
struct ValueLocation {
    segment_id: u64,
    record_offset: u64,
    value_offset: u64,
    value_len: u32,
}
~~~

BTreeMap 将 key 映射到日志中的最新值位置。批次内部的 Put 位置指向子记录，读取时校验记录和 key。索引点查复杂度为 O(log N)。实际内存取决于 key 长度、树节点、分配器和存储规模，容量需实测。

### 3.3 Checkpoint

~~~text
magic(8) = RDG2CKPT
version(4) = 2
active_segment_id(8) + active_offset(8) + entry_count(8)
repeat entries:
  key_len(4) + key
  segment_id(8) + record_offset(8) + value_offset(8) + value_len(4)
checksum(4)
~~~

v2 的尾部 checksum 校验之前的整个文件。读取兼容 v1，同时校验版本、条目数、长度、位置、重复 key 和多余字节。恢复时验证索引引用的 segment 存在且 offset 落在文件范围内。

## 4. 写入路径

~~~text
锁外校验并构造记录
  -> writer Mutex
  -> 必要时轮转 segment
  -> 最终记录编码
  -> write_all
  -> 应用同步策略
  -> 短暂取得 index 写锁并整体发布索引变化
  -> 累计操作数
~~~

Put/Delete 与 WriteBatch 都在记录完整追加且同步策略成功后发布索引。批次只执行一次提交同步。

append 或同步失败时，使用独立可写句柄将 active segment 截断回原长度并同步。回滚失败会记录写错误并拒绝后续写入与 checkpoint。调用方获得错误，Go 协调路径通过 mutation journal 恢复缓存与统计。符合并发条件的普通 SET 则丢弃操作私有待提交值并释放预留预算，WATCH 版本在实际提交后更新。

### 4.1 Sync 策略

| 策略 | 返回前行为 | 掉电持久性 |
| --- | --- | --- |
| None | 完整 append | 成功回复允许丢失 |
| Flush | append 后调用文件 flush | 成功回复允许丢失 |
| SyncData | 每次 record/batch 调用 sync_all | 等待本次同步成功 |

显式 sync() 同步 active segment。轮转、checkpoint 和 close 也执行相应同步步骤。Rust Options 中的 sync_interval_ms 和 sync_after_ops 是保留字段，当前 SyncData 每次提交同步。Go C ABI 提供三种策略。

group commit 属于后续研究。实现时需要聚合多个等待者，并在共享同步成功后才确认这一批 sync 请求。

### 4.2 Checkpoint 策略

写入只累计操作数。maybe_checkpoint() 在达到 checkpoint_after_ops 时同步生成 checkpoint，由 Go 维护周期调用。显式 checkpoint() 强制生成。

checkpoint 持有 writer Mutex，并共享 index 读锁写出完整索引，单次耗时随索引规模增长。GET 可以共享该索引读锁，前台写入仍等待 writer。增量 checkpoint 和维护调度是后续优化方向。

## 5. 读路径

在索引读锁内查找位置并取得目标 segment 的独立 OS 句柄，然后释放索引锁，使用定位读取读取并验证值。该路径独立于 writer 的文件同步等待，前台未提交的新值在索引中不可见。cleaner 可以逐条替换内容等价的记录位置，源段保留至 checkpoint 成功持久化。

该能力对应 Rust 直接 API。完整 RESP 服务的冷 GET 仍取得 Go 数据库排他锁，会排在在途 SET 之后，并使该库后续热 GET 等待。

reader cache 上限为 128 个文件句柄，按逻辑访问时钟淘汰。文件定位读取避免共享 seek 位置冲突。Go 得到 Rust buffer 后复制成 Go slice，并调用 kerniva_buf_free 释放 native 内存。

## 6. 空间回收

compact_once 在 writer Mutex 下重写旧段中仍被索引引用的 live entry，逐条短暂取得 index 写锁发布新位置，保存持久化 checkpoint 后再删除可回收段。snapshot 引用中的段进入延迟回收。

maybe_compact 统计 sealed segment 总字节和 live record 字节，可回收字节大于 0 且达到 50% 时触发。已经等待快照释放的段被排除，减少重复改写。compact_until_stable(max_rounds) 提供强制维护和轮数上限。

当前单轮 cleaner 仍可能处理较大数据量。需以长时间覆盖写入、快照持有和 p99 压测评估停顿和写放大。

## 7. 崩溃恢复

1. 创建或检查目录并取得 LOCK 文件的进程间独占锁。
2. 加载和验证 checkpoint。
3. 从恢复位置扫描增量 segment，完整解析后应用单记录或批次。
4. 对最后一个 segment 的不完整尾记录截断至完整边界。
5. 打开 active segment 并恢复提供服务。

checksum 损坏、非法格式、中间段截断和无效 checkpoint 返回错误。区分不完整尾部与完整记录损坏，避免把损坏数据当作可忽略尾部。恢复时间取决于索引大小、增量日志量、介质和校验成本。

## 8. 磁盘布局

~~~text
data_dir/
  LOCK
  segments/
    0000000001.seg
    0000000002.seg
  meta/
    checkpoint.bin
~~~

segment 从 1 编号，十位零填充。Rust 默认大小为 64MiB，仓库服务配置为 4MiB。

checkpoint 写入同目录临时文件并 flush + sync_all，随后执行平台原子替换。Unix 追加目录 fsync，Windows 使用 MoveFileExW 的 REPLACE_EXISTING 与 WRITE_THROUGH。新 segment 执行文件同步，Unix 另做目录 fsync。Windows 的 sync_directory 当前直接返回，其目录项断电持久性仍需验证。

## 9. 并发、快照和生命周期

writer Mutex 串行化追加、同步、回滚和维护，index RwLock 保护已提交索引。写者按 writer 到 index 的顺序取锁，普通 GET 只进入 index 与 reader cache。reader cache 使用独立 Mutex。snapshot 在 index 读锁内复制索引并登记 segment pin，继续持有目录租约。iterator 复用此只读视图，创建成本随 key 数增长。

即使原 Engine 释放，存活 snapshot/iterator 仍持有目录锁租约。Go Engine 读写锁协调所有操作与 Close。C 调用方负责避免关闭句柄与使用同一裸句柄并发。

## 10. API 总览

| Rust API | 作用 |
| --- | --- |
| open / close | 获取目录租约、恢复与同步退出 |
| put / get / delete | KV 读写 |
| write_batch | 单条外层 record 的原子批次 |
| clear | 全库清空 marker |
| sync / checkpoint | 显式同步和索引快照 |
| maybe_checkpoint / should_checkpoint | 阈值维护 |
| maybe_compact | 条件空间回收 |
| compact_once / compact_until_stable | 强制回收 |
| snapshot / iter | 固定视图和遍历 |
| stats | key、segment、reader 与回收状态 |

Go Reset 调用原生 clear，写入单条 marker，无需枚举 value。marker 的日志大小与 key 数无关。

## 11. FFI

精确声明以 include/kerniva_engine.h 为准。核心返回约定为 0 成功、-1 错误。get/iter_next 返回 1 找到、0 结束或缺失、-1 错误。iter_close 返回 void。

~~~c
typedef struct {
    uint32_t kind;
    const uint8_t* key_ptr;
    uintptr_t key_len;
    const uint8_t* value_ptr;
    uintptr_t value_len;
} KernivaWriteOp;

int32_t kerniva_engine_write_batch(
    uintptr_t engine, const KernivaWriteOp* ops,
    uintptr_t ops_len, KernivaBuf* err);

int32_t kerniva_engine_maybe_compact(
    uintptr_t engine, uint64_t* rewritten, KernivaBuf* err);

int32_t kerniva_engine_clear(uintptr_t engine, KernivaBuf* err);
~~~

kind=1 为 Put，2 为 Delete。Rust 复制输入后操作，返回的 KernivaBuf 由 kerniva_buf_free 释放。句柄和长度采用 uintptr_t，调用方应使用当前目标平台位宽。

## 12. 构建和打包

~~~bash
cargo build --release --manifest-path kerniva_engine/Cargo.toml
~~~

默认输出 target/release 下的平台动态库。配置 Cargo target 时产物位于对应 target 子目录。根目录构建脚本检测并复制产物至 bin。

完整服务交付包括服务端、客户端、Rust 动态库和 config.yaml。本机 GNU release 经 objdump 检查只依赖 Windows 系统 DLL。

## 13. 默认配置

| 项目 | Rust 默认 | 仓库服务配置 |
| --- | --- | --- |
| segment_size_bytes | 64MiB | 4MiB |
| checkpoint_after_ops | 1024 | 1024 |
| sync_policy | None | SyncData |
| data_dir | ./kerniva_engine_data | ./data/db_N |

Rust 保留的同步间隔字段由未来调用方维护设计使用。当前三种策略与提交时点遵循第 4 节。

## 14. 与上层服务的边界

Go 管理类型序列化、TTL、WATCH 和缓存预算。冷 TTL 按访问触发删除。Rust 存储序列化字节并提供单引擎批次原子性。

EXEC 和 Lua 多次调用逐命令提交，崩溃可能保留成功前缀。跨库 MOVE 先写目标后删源，中途崩溃可能出现双副本。跨库恢复协议和事务级持久化需要进一步设计。

## 15. 验证与后续计划

Rust 回归包含截断/损坏、批次原子恢复、写失败回滚、目录锁、快照租约、读取与回收并发，以及强杀子进程恢复。Go 回归验证桥接、缓存回滚、重启、类型状态和网络命令。

后续依次评估大索引 checkpoint、增量 cleaner、group commit、冷 TTL 索引及事务级提交协议。性能数据和验证状态见项目评审与性能报告。
