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

当前原型通过 Rust API 使用。TTL、条件写、跨分片批次及 Go 服务接入见[开发计划](../docs/rust_memory_unattended_plan.md)。

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
