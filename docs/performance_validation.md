# 本机性能验证

## 测量范围

2026-11-14 对最终构建产物执行一轮 24 组 TCP 测量，组合为 std / gnet、memory / sync、1 / 16 / 64 并发以及 SET / GET。

每组 10,000 个测量请求，value 固定 32B，每连接一个独立 key，最多 64 个热 key，SET 反复覆盖。每连接先预写并预热 20 次，预热和连接建立排除在测量之外。单连接逐请求等待回复，逐条校验 SET 的 OK 和 GET 的完整值。计时覆盖 TCP 请求、服务端执行和完整回复。

Windows 客户端使用 QueryPerformanceCounter。ops/s 由整组墙钟时间计算，延迟使用最近秩 p50/p95/p99。多个连接共同完成总计 10,000 次请求。

## 环境与配置

- Windows 11 家庭版，系统构建 10.0.26300。
- Intel Core i9-14900HX，24 核 / 32 逻辑处理器，约 16GB 内存。
- Go 1.25.8，Rust 1.95.0，x86_64-pc-windows-gnu，GCC 16.2。
- 客户端与服务端同机，127.0.0.1 TCP。每个后端与模式组合启动独立新进程。
- 持久化数据、日志和配置均位于新建系统临时目录。
- db_count=1，max_memory=256MiB，noeviction，lazy_load，strong，segment=4MiB，checkpoint_after_ops=1024，log_level=error。
- memory 使用 persistence_enabled=false。sync 使用 persistence_enabled=true 与 persistence_durability=sync，每次写批次执行 sync_all。

产物 SHA256 用于标识测量版本。

~~~text
kerniva-server.exe
CD3EF03F2C609836ADD14331395264B6D6DBC73E7F926CACEB9D0B7740CFAFC6
kerniva_engine.dll
2A74816A0298691C3B4D0097915B767CCB09EE363BA9B5F2CC58AAC0F657C710
~~~

## 内存模式

| 后端 | 并发 | 操作 | ops/s | p50 μs | p95 μs | p99 μs |
| --- | --- | --- | --- | --- | --- | --- |
| std | 1 | SET | 55392 | 16.4 | 21.5 | 40.7 |
| std | 1 | GET | 58138 | 16.2 | 19.8 | 28.9 |
| std | 16 | SET | 228836 | 68.8 | 98.4 | 140.4 |
| std | 16 | GET | 274369 | 58.2 | 86.7 | 116.6 |
| std | 64 | SET | 464001 | 119.6 | 241.7 | 419.0 |
| std | 64 | GET | 505779 | 100.8 | 262.2 | 437.5 |
| gnet | 1 | SET | 65983 | 14.3 | 18.0 | 30.4 |
| gnet | 1 | GET | 68595 | 13.6 | 17.8 | 29.0 |
| gnet | 16 | SET | 245271 | 64.4 | 104.0 | 154.8 |
| gnet | 16 | GET | 323133 | 46.7 | 80.4 | 140.7 |
| gnet | 64 | SET | 544983 | 80.5 | 221.6 | 757.8 |
| gnet | 64 | GET | 865329 | 58.8 | 146.9 | 238.0 |

## 同步持久化模式

| 后端 | 并发 | 操作 | ops/s | p50 μs | p95 μs | p99 μs |
| --- | --- | --- | --- | --- | --- | --- |
| std | 1 | SET | 4566 | 198.1 | 302.8 | 479.2 |
| std | 1 | GET | 49015 | 17.0 | 32.0 | 59.8 |
| std | 16 | SET | 5079 | 3092.8 | 3542.6 | 4050.4 |
| std | 16 | GET | 258004 | 60.6 | 97.4 | 127.7 |
| std | 64 | SET | 5063 | 12526.7 | 13485.2 | 14670.7 |
| std | 64 | GET | 330416 | 153.6 | 393.4 | 643.8 |
| gnet | 1 | SET | 4695 | 197.5 | 287.2 | 459.3 |
| gnet | 1 | GET | 65460 | 14.4 | 18.3 | 31.6 |
| gnet | 16 | SET | 5078 | 3072.3 | 3561.8 | 4111.6 |
| gnet | 16 | GET | 254013 | 59.9 | 109.3 | 189.0 |
| gnet | 64 | SET | 5137 | 12309.8 | 13081.1 | 16948.4 |
| gnet | 64 | GET | 345897 | 166.6 | 325.1 | 509.4 |

64 并发 sync SET 的 p99 为 std 14.67ms、gnet 16.95ms。单写者串行同步在该负载下约为每秒 5,100 次提交，并发增加主要体现为等待时间。

GET 工作集已预热，测量主要覆盖网络、分发和 Go 缓存。持久化 GET 的结果也属于热缓存场景。

## 复现

先构建服务和 benchmark。

~~~powershell
./scripts/build.ps1
go build -o ./bin/kerniva-bench.exe ./scripts/bench
~~~

新建隔离测试目录，使用独立端口和绝对数据路径。将以下配置保存到该目录的 config.yaml，并替换 data_dir 和 log_path。

~~~yaml
host: "127.0.0.1"
port: 16380
network_type: "std"
db_count: 1
max_memory: 268435456
max_memory_policy: "noeviction"
persistence_enabled: false
persistence_type: "kerniva"
data_dir: "<absolute-benchmark-directory>/data"
cold_start_strategy: "lazy_load"
persistence_write_mode: "strong"
persistence_durability: "sync"
engine_segment_size: 4194304
engine_checkpoint_after_ops: 1024
log_level: "error"
log_path: "<absolute-benchmark-directory>/server.log"
~~~

启动该实例，然后在另一个终端执行 benchmark。

~~~powershell
./bin/kerniva-server.exe -config <absolute-config-path>
./bin/kerniva-bench.exe -addr 127.0.0.1:16380 -clients 1,16,64 -requests 10000 -network std -durability memory
~~~

每组后端/模式组合重新启动实例。network_type 选择 std 或 gnet。sync 组设置 persistence_enabled=true，保留 persistence_durability=sync。同步修改 benchmark 的 -network / -durability 标签。标签仅用于记录，服务端行为由配置决定。

Linux/macOS 可通过 bash scripts/build.sh 构建，再使用 go run ./scripts/bench 执行同样参数。工具输出 JSONL，包含操作、标签、并发、请求数、value 大小、ops/s 和延迟分位数。

## ZSet 数据结构微基准

同一进程比较当前增量维护与测试中保留的旧重建排序算法，三轮各 500ms。

~~~bash
go test ./internal/datastruct -run '^$' -bench BenchmarkZSetScoreUpdate -benchmem -benchtime=500ms -count=3
~~~

| 成员数 | 当前增量更新 ns/op，三轮 | 旧重建 ns/op，三轮 |
| --- | --- | --- |
| 100 | 50.26 / 50.60 / 50.91 | 6288 / 6297 / 6376 |
| 10000 | 3217 / 3255 / 3261 | 1154534 / 1155403 / 1159115 |

10,000 成员场景，当前算法 0B/op、0allocs/op，旧算法 245856B/op、4allocs/op。该结果描述 score 单成员更新的局部成本，端到端网络收益需要独立测量。历史 GetBytes 裸返回可变指针的基准与当前安全复制语义存在差异，性能对比需采用相同语义。

## 解读与后续测量

本次为小热工作集的单轮样本，操作数较少，结果受调度、频率、后台任务与存储状态影响。Redis 同机对照、重复轮次和置信区间仍待补充。

接下来补充冷读、大 value、多 key、大索引 checkpoint、长期覆盖写入 cleaner、快照长期持有和恢复时间。none / flush 需要各自独立测量并标明掉电语义。当前数据支持本机原型负载评估，跨产品和跨硬件结论需要匹配配置及持久性后再验证。

group commit、MVCC 和新索引属于后续研究方向。有界 group commit 与维护预算化原型可并行推进，扫描时间窗口、字节和操作数上限，保留 sync 成功回复的确认边界。实验记录逐请求 p99、实际批次大小和吞吐，按 p99-吞吐曲线选择窗口。跨库恢复作为独立可靠性路线推进。
