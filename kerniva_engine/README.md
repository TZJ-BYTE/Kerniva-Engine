# Kerniva Engine

Rust 标准库实现的嵌入式 KV 内核，采用 segment log、BTreeMap 内存索引和 checkpoint，通过 C ABI 提供平台动态库。

## 构建与测试

~~~bash
cargo build --release --manifest-path kerniva_engine/Cargo.toml
cargo test --manifest-path kerniva_engine/Cargo.toml
cargo clippy --manifest-path kerniva_engine/Cargo.toml --all-targets -- -D warnings
~~~

从项目根目录执行。最低需要 Rust 1.89，本机验证使用 1.95。Windows 需要 MSVC 或完整 MinGW 链接环境。产物为 kerniva_engine.dll、libkerniva_engine.so 或 libkerniva_engine.dylib。指定 Cargo target 后，产物增加对应 target 子目录。

根目录 scripts/build.ps1 和 scripts/build.sh 会构建库及 Go 服务并复制到 bin。Go 运行时加载预构建库，KERNIVA_ENGINE_LIBRARY 可覆盖路径。

## 当前能力

- Put/Get/Delete、单记录 WriteBatch 和固定 20 字节 Clear marker。
- 每条记录 checksum，完整批次恢复，旧 Put/Delete 记录兼容。
- checkpoint v2 文件 checksum、原子替换，兼容读取 v1。
- 最后段不完整尾部截断，以及格式和损坏校验。
- 数据目录独占锁，snapshot/iterator 目录租约。
- 128 文件上限的 reader cache、定位读取和读取校验。
- snapshot pin、延迟回收、条件 cleaner 与强制 compact。
- 显式 sync、checkpoint、阈值检查和统计。

## Rust 主存 KV 原型

`MemoryEngine` 是独立的 Rust 字节键值库。构造后可直接读写，配置项为分片数和常驻键值字节上限。

~~~rust
use kerniva_engine::{MemoryEngine, MemoryError};

fn main() -> Result<(), MemoryError> {
    let engine = MemoryEngine::new(16, Some(16 * 1024 * 1024))?;
    engine.put(b"key", b"value")?;
    assert_eq!(engine.get(b"key")?.as_deref(), Some(&b"value"[..]));
    assert_eq!(engine.stats()?.payload_bytes, 8);
    assert!(engine.delete(b"key")?);
    Ok(())
}
~~~

分片数取正的 2 的幂，`None` 表示不限常驻字节。超限的写入返回 `PayloadLimitExceeded`，保留已有值。GET 返回不可变的 `Arc<[u8]>`，覆盖或删除后旧读者仍可使用自己持有的值。字节限额统计当前键和值，RSS 还包括哈希表容量、分配器和旧读者持有的值。统计会取得所有分片的读锁，适合低频查询。

Rust API 还支持绝对毫秒过期时间、按存在状态或版本条件写入，以及跨分片原子批次。`get_many_at` 一次读取同一个状态下的多个键。`put_with_at` 条件失败时返回 `None`，`apply_batch_at` 条件失败时返回 `false`，旧值和预算保持原状。重复键在批次中按输入顺序计算，预算按最终状态检查。

过期键从截止时刻起对读取和条件判断不可见。它的常驻字节在覆盖、删除或显式调用 `purge_expired_at` 后返还。清理按单分片扫描，适合由调用方在低频维护路径调度。版本是绑定到实例和键的比较令牌。

Go 服务接入使用独立的 [内存 C ABI](include/kerniva_memory.h)，运行方法见[项目 README](../README.md)。

## 主存内核的同步 WAL

`DurableMemoryEngine::open` 在独立的已有目录内恢复或创建 `memory-v1.wal`。运行时从 Rust 分片内存中读取。写入先完成一帧 WAL 的同步，再进行状态发布并返回成功。版本和绝对过期时间会随提交恢复。目录锁确保同一目录同时只有一个实例打开。

以下原生示例创建独立实验目录，批量预填 32B 键值，测量 95% 读负载下的读写延迟以及 WAL 同步次数。

~~~bash
dir=$(mktemp -d "$HOME/kerniva-durable-bench.XXXXXX")
cargo run --release --manifest-path kerniva_engine/Cargo.toml --example memory_benchmark -- \
  --durable-directory "$dir" --shards 16 --threads 16 --keys 100000 \
  --operations 300000 --warmup-per-thread 100 --read-percent 95 --sample-every 1
~~~

日志回滚失败时，实例保留已发布读视图并停止接受新写入。重新打开时，完整帧可恢复，截断尾会回退到最后一帧结束。完整帧损坏会报告错误。失去成功回复的请求可能已经同步，处理重试时应考虑这种结果。当前日志会随提交增长，恢复须扫描日志，启动预算按最终常驻数据检查。

## 同步策略

| 策略 | 行为 |
| --- | --- |
| None / 0 | append 完成后返回，允许掉电丢失 |
| Flush / 1 | 调用文件 flush，允许掉电丢失 |
| SyncData / 2 | 每次 record/batch 提交执行 sync_all |

Options 的 sync_interval_ms 和 sync_after_ops 为保留字段。SyncData 当前每次提交同步。group commit 需要单独设计请求聚合与确认协议。

checkpoint 由调用方定时或显式触发，在 writer Mutex 下生成完整索引，index 读锁允许 GET 共享读取。普通 GET 在索引读锁内取得位置和文件句柄后执行定位读取。大索引对写入的暂停时间需测量，Go 服务层冷读仍进入数据库排他协调。

## C ABI

声明以 [C header](include/kerniva_engine.h) 为准。

| 操作 | 导出 |
| --- | --- |
| 打开/关闭 | kerniva_engine_open / close |
| KV | kerniva_engine_put / get / delete |
| 批次 | kerniva_engine_write_batch |
| 全库清空 | kerniva_engine_clear |
| 同步 | kerniva_engine_sync |
| checkpoint | kerniva_engine_checkpoint / maybe_checkpoint / should_checkpoint |
| 回收 | kerniva_engine_maybe_compact / compact_until_stable |
| 遍历 | kerniva_engine_iter_open / next / close |
| 统计 | kerniva_engine_stats |
| buffer 释放 | kerniva_buf_free |

批次 KernivaWriteOp 的 kind=1 为 Put、2 为 Delete，输入指针及长度仅在调用期间借用。Rust 返回的 buffer 由 kerniva_buf_free 释放。iter_close 返回 void，其余状态码见 header 和设计文档。

## 配置

| 字段 | 默认值 |
| --- | --- |
| segment_size_bytes | 64MiB |
| checkpoint_after_ops | 1024 |
| sync_policy | None |

服务仓库配置使用 4MiB segment 和 SyncData。每个逻辑库拥有独立目录与 Engine。

## 规模和一致性边界

BTreeMap 占用与 key 数、key 长度和分配器有关。snapshot/iterator 创建时复制完整索引。checkpoint 写全量索引，cleaner 的单轮工作量与旧 segment 数据量有关。

WriteBatch 提供单引擎恢复原子性。上层多命令 EXEC/Lua 和跨库 MOVE 有独立提交边界。TTL 和类型序列化由 Go 负责。

## 详细文档

- [整体架构](../docs/kerniva_engine_architecture.md)
- [格式、恢复和生命周期设计](../docs/kerniva_engine_design.md)
- [性能验证](../docs/performance_validation.md)
