# 主库慢同步期间，WSL 和 Windows 磁盘分别做了什么

2026-11-26。在已经修复 PING、INFO、SELECT 跨库阻塞的 C 版本上，8 个自然窗口中的两个重现了长尾。144 万次真实请求全部成功。两窗都看到了慢同步与排队，设备表现却不同。第 5 窗的来宾明确在等 I/O。第 7 窗的多次长同步期间，主机整盘几乎没有读写完成。现有证据可以定位到等待阶段，尚不能判定物理 SSD、虚拟盘传输或线程唤醒各自贡献的时长。[上轮锁修复](observer-execution-gate.md) · [逐窗机器结果](primary-host-disk.json)

## 八个自然窗口

WSL ext4 单主，gnet、strong/sync、合并上限 8 帧。每窗使用相同二进制、10 万个小键、64 条 TCP 连接，预热 5 秒后按每秒 6,000 个请求投递 30 秒，95% GET。客户端保留固定样本和全部达到 100 ms 的请求，服务端记录所有写入批次。按源端口、连接内请求序号和操作序列核对了全部 71,072 条 SET，共涉及 61,976 个同步批次。监控连接偶尔与压测连接共享一个批次。下表延迟从计划投递计至完整收到回复。

| 窗口 | Windows 采样 | GET p99（ms） | SET p99（ms） | ≥100 ms GET / SET | 最长同步（ms） |
|---|---|---:|---:|---:|---:|
| 1 | 关 | 1.327 | 6.816 | 0 / 0 | 12.9 |
| 2 | 开 | 1.290 | 6.767 | 0 / 0 | 8.6 |
| 3 | 开 | 1.303 | 6.357 | 0 / 0 | 7.8 |
| 4 | 开 | 1.319 | 6.472 | 0 / 0 | 11.6 |
| 5 | 开 | 218.628 | 389.022 | 2,688 / 257 | 84.8 |
| 6 | 开 | 1.294 | 6.341 | 0 / 0 | 8.8 |
| 7 | 开 | 439.353 | 589.300 | 11,520 / 1,241 | 211.5 |
| 8 | 关 | 1.274 | 6.095 | 0 / 0 | 10.7 |

15,706 个慢请求中，14,898 个在客户端发送前已等待至少 100 ms，988 个从发出到收齐回复也耗时至少 100 ms，两项可以重叠。同一连接需等前一条回复才能发送后续请求。窗口 5 的一条 SET 从发送到收到回复用了 415.302 ms，其中在服务端写队列等了 368.828 ms，自己的同步用了 46.194 ms。窗口 7 的一条 SET 在发送前等了 335.697 ms，发出后又等了 462.886 ms，其中写队列占 268.112 ms，自己的同步占 194.264 ms。两个关闭主机采样的窗都正常，这组固定顺序不足以估计探针对尖峰概率的影响。

## 对到同一个批次时

- **窗口 5，批次 1386。** 同步耗时 84.837 ms。与之重叠的三个来宾采样间隔中，设备忙碌时间分别增加 40 ms，I/O PSI full 各增加约 39.6–39.9 ms。完整落在同步内的一个来宾间隔累计完成 2 次 flush，共计 43 ms。同口径下 Windows 的一个整盘间隔完成 4 次写入，平均每次约 0.065 ms。两层计数覆盖不同设备、不同操作。
- **窗口 7，24 个至少 100 ms 的同步批次。** 完全落在这些同步内的 72 个 Windows 采样间隔中，整盘合计完成 0 次读、1 次 8 KiB 写。21 个批次具有完整来宾间隔，合计 67 个，期间完成 0 次写、0 次 flush。该窗口漏掉 10 个来宾采样时槽，最长采样间隔 198.248 ms。另有 3 个较短批次的边缘超出可保证的来宾覆盖范围。每个分析事件都单独记录覆盖及长间隔。
- **第 7 窗内还存在不同的调度表现。** 在 7025 和 7067 两个约 200 ms 的同步内，来宾采样线程各按期完成五次采样，期间仍有上下文切换。在批次 7197 内，一次采样起步晚了 111.871 ms，CPU PSI some 增加 38.255 ms。全窗最慢的来宾磁盘计数读取仅用 0.788 ms。两次对 CPU 统计的读取区间分别跨了约 156 和 198 ms，这段墙钟时间包含采样线程可能失去运行机会的时间。

WSL 块设备聚合计数与 Windows F 盘的整盘完成计数都不能标识某一条 fsync。一个长期未完成的请求在窗口中可以没有完成计数，Windows 采样接口也没有单独的 flush 数据。旧实验曾用 Go 运行时跟踪排除特定尖峰中的 GC 暂停，本轮两个新尖峰没有同步采集 GC 事件。[旧窗 GC 证据](primary-gc-stalls.md)

## 有已知答案的对照

另起两个独立实例，对同一条 DB 0 SET 保持相同路径。在一个实例用 strace 把 fsync 返回延迟 220 ms。各臂的测量 SET 都只匹配到一条 DB 0 活动日志 fsync。该夹具中的人为等待只用于检验诊断方法。

| 观测 | 无注入 | 返回延迟 220 ms |
|---|---:|---:|
| DB 0 客户端 SET | 2.218 ms | 222.928 ms |
| Go 写者测得的同步调用 | 1.620 ms | 221.950 ms |
| strace 的系统调用用时字段 | 1.552 ms | 1.470 ms |
| 独立连接 DB 1 GET | 1.459 ms | 0.787 ms |

人为延迟的同步内，主机和来宾各有 5 个完整采样间隔，均没有完成写入，来宾也没有完成 flush。这个对照验证了应用端可以观察到约 220 ms 同步等待，同时两层设备完成计数都很低。

另用独立的 WSL tracefs 实例给一个普通用户进程做了单笔 4 KiB 写入实验。首次过滤误用了 WSL 命名空间内的 PID，第二次由子进程先写 trace marker，取得内核跟踪使用的线程号。正式一笔 fsync 从进入内核到返回为 3.140 ms，三次唤醒至重新运行分别为 11、1、1 µs。该线程发出的 4 KiB 写与同扇区、同长度的唯一完成事件相距 87 µs。留存 27 条事件，内核报告零丢弃，实验实例已销毁。后台日志和零长度 flush 的事件没有唯一请求标识。单笔夹具与以上两个自然尖峰独立。[内核夹具源码](../../scripts/golog_kernel_trace_probe.py)

## 采集质量与复算

六个主机采样窗共取得 5,631 条 Windows 观测。F 盘映射到 PhysicalDrive1，采样期间缓存在内存，结束后才写到 E 盘。名义周期 40 ms，实际最长间隔 42.205 ms。Windows 原始短周期 CPU 增量有 34 段不一致，分析剔除出现这种情况的整窗 CPU 序列，磁盘计数独立校验。采样器退出时停止轮询、关闭设备句柄。磁盘计数是该盘所有活动的总和。[Windows 磁盘计数接口](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntdddisk/ni-ntdddisk-ioctl_disk_performance)

主机采样前后各做 12 次跨 Windows/WSL 进程往返校时，保存原始因果边界。复算以两端时钟相对速率偏差不超过 200 ppm 为假设，在每个端点扩展误差区间。仅在所有可行时钟映射下都成立时才记为「确定重叠」或「完整落在同步内」。WSL 与客户端的观测沿用上一轮的双端时钟投影。来宾的 8 窗共有 6,006 个实际样本，诊断保留每次采样的迟到、区间和漏点。实验文件位于 `logs/golog-host-sync-20261126`。

从仓库根目录运行以下命令复算。原始 Windows 读数、各窗两端请求和批次、WSL 设备样本、夹具跟踪与校验清单封存在本地 `dist/Kerniva-host-disk-20261126-evidence.zip` 中。

```powershell
$root = 'logs/golog-host-sync-20261126'
python -B scripts/golog_host_disk_report.py --raw "$root/raw" --host-root "$root/host" --out "$root/natural-replay.json" --summary "$root/summary-replay.json"
python -B scripts/golog_host_control_report.py --raw "$root/control-raw" --host "$root/control-host" --out "$root/control-replay.json"
python -B scripts/golog_guest_stall_detail.py "$root/raw" --out "$root/guest-replay.json"
```

复跑采集时，在 WSL 的仓库根目录执行以下命令。两个输出目录都须为新目录。Windows Python 从 WSL 的 PATH 查找，也可通过 `--windows-python` 指定。

```bash
setup=logs/set-tail-stalls-20261126/setup.json
bin=/home/lemon/kerniva-ping-build.793ihwSZ/Cbin
manifest=logs/golog-batch-20261126/reviews/ping-gate/C-binaries.sha256
out=$(mktemp -d -p logs host-disk.XXXXXXXX)
python3 -B scripts/golog_host_capture.py --setup "$setup" --bin "$bin" --binary-manifest "$manifest" --host-root "$out/host" --mirror "$out/raw"
python3 -B scripts/golog_host_capture.py --controls --setup "$setup" --bin "$bin" --binary-manifest "$manifest" --host-root "$out/control-host" --mirror "$out/control-raw"
```
