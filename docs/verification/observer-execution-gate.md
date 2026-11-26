# 主库慢同步、监控命令与跨库 GET

2026-11-26，Kerniva 在两次 WSL 自然尖峰里找到同一条阻塞链。写者连续遇到慢同步，随后监控连接的 `SELECT` 排到全局执行锁前面，后来到达的普通 GET 也要等待。在受控实测里修复了这个跨库问题。同步写自己的磁盘等待、单连接后继请求的排队仍会形成长尾。

## 一次慢 SET 在哪里花了时间

单主、两逻辑库、真实 gnet，开启 strong/sync，每次最多合并 8 帧。64 个独立 TCP 连接对 10 万小键发起 95% GET、5% SET，每秒 6,000 个请求。每窗预热 5 秒并投递 30 秒。延迟从计划投递计到客户端读完回复。下表 A 为原有门锁，B 放宽普通 INFO、SELECT 的门锁。每窗都保留了客户端的固定样本和全部 ≥100 ms 请求。

| 顺序 | 版本 | 服务端批次追踪 | GET p99（ms） | SET p99（ms） | ≥100 ms GET / SET |
| --- | --- | --- | ---: | ---: | ---: |
| 1 | A | 开 | 1.274 | 6.423 | 0 / 0 |
| 2 | B | 开 | 1.270 | 6.226 | 0 / 0 |
| 3 | B | 开 | 1.278 | 6.275 | 0 / 0 |
| 4 | A | 开 | 273.678 | 360.710 | 3,490 / 206 |
| 5 | A | 关 | 1.274 | 6.046 | 0 / 0 |
| 6 | B | 关 | 162.529 | 295.698 | 2,275 / 222 |

六窗合计 1,080,000 次请求，全部成功。四个开追踪窗各自将全部 8,884 条测量 SET 对到真实批次。连接始终未重连，以客户端源端口和连接内序号关联，并用确定性操作序列和两端零丢记录交叉校验。第 4 窗还用 756 条两端都有时间戳的 SET 将两进程的单调时钟偏移约束到 23.895 µs。[逐窗结果、时间分解与时钟界限](observer-gate-windows.json)

- 按计划时间最早的三条慢 SET 全在 **第 1380 批，8 帧**。前面第 1379 批的同步用了 39.831 ms。三条 SET 在前批同步期间入队，等了 39.355–39.622 ms，本批同步又用 36.893 ms。最早一条在客户端发出前已经等了 52.349 ms。
- 网络交换最慢的一条慢 SET 是第 4253 号请求，第 1400 批。其 238.589 ms 交换中，228.884 ms 花在提交队列，9.479 ms 花在本批同步，服务端解析到提交回复以外共 0.134 ms。一个请求可以同时受到前面多次慢同步的影响。
- 第 1380 批开始同步之后，两条监控 SELECT 开始等全局锁，分别等了 103.907 和 198.467 ms。之后最长的一条等了 232.696 ms，与 142 条慢请求的网络交换可以确定重叠。从该 SELECT 取得锁到回复准备完毕仅 0.017 ms。这次开头 1.2 秒内，87 批有 33 次同步至少 10 ms，最长 87.487 ms。两个 B 开追踪窗同一时段各 330 批，均无一次达到 10 ms。第 6 窗缺少逐批追踪，单靠该窗客户端数据无法给它指定同样的原因。
- 对上一次 3,978 条慢请求的主库原始运行时跟踪另做重放。与异常时段相交的 434 段共享执行锁等待里，93 段至少 100 ms，最长 280.647 ms。对应的同键等待仅两段，最长 5.849 ms。统计的是协程进入等待到变成可运行的间隔，各协程的等待可以同时发生。该次主库 GC 暂停发生在尖峰结束至少 6.086 秒之后。[原始 GC 对照与时钟依据](primary-gc-stalls.md)

## 同步不放松，让其他连接继续读

普通 SELECT 只切换当前连接的逻辑库。INFO 通过各库已有的锁读取统计。现在它们与普通单键请求可以并行进入，共享门仍与 EXEC、跨库变更等独占操作互斥。INFO 是动态采样，各项统计可在不同瞬间取得。工作台建立或测试连接还会发 PING，最终版 C 单独让合法的普通 PING 走共享门，保留普通、带参数、订阅态和 MULTI/EXEC 中的回复语义。

用 strace 在 WSL 外部延迟 DB 0 活动日志的同步系统调用返回 350 ms。每种情况都是一次单独的 TCP 时序复验，DB 1 读连接提前选好库，读取已存在的值。记录验证观察命令和 GET 均在 DB 0 已进入本次同步后发起。表内是各次 DB 1 GET 的端到端用时。

| DB 0 同步期间发起的观察 | A 原版 | B 修 INFO/SELECT | C 再修 PING |
| --- | ---: | ---: | ---: |
| 无观察，与 SELECT/INFO 同组 | 0.56 ms | 0.59 ms | — |
| SELECT | 204.21 ms | 0.56 ms | — |
| INFO | 205.22 ms | 0.77 ms | — |
| 无观察，与 PING 同组 | 0.90 ms | 0.56 ms | 0.57 ms |
| PING | 204.07 ms | 204.20 ms | 0.58 ms |

两组实验的 DB 0 SET 均用约 354 ms 才成功。B 中，INFO 自己仍用了约 280 ms，但 DB 1 的 GET 比 DB 0 的实际同步返回至少早 202.145 ms。相同条件下，A 的 GET 在同步返回至少 1.607 ms 后才完成。C 的 PING 本身用了 0.998 ms。此处给出的改善只适用于这条监控阻塞链。[对照的机器验收](observer-gate-fixture.json)

对 B 的真实 gnet 主库执行 SIGKILL，重启后确认过的新键和覆盖值逐个读回正确，数据库序号为 4，下一次成功写入推进至 5。最终版的写入确认、回滚和关闭顺序与 B 相同。

Windows 和 WSL 的项目完整竞态回归均已通过。Windows 执行了 97 个 Rust 测试和 16 个 Go 测试包，一项创建符号链接的断言因系统权限跳过。WSL 从提交 `538be84` 的独立 ext4 快照执行 `bash scripts/test.sh -race`，包括 Linux 专用用例与集成测试。首轮 gnet 追踪启动被网络类型检查拒绝，修正并验证真实 gnet 连接后重新构建了对照两臂，正式数字全部来自重建后的运行。

## 写者本身遇到慢同步时

最终版 C 以 8 帧再跑四个 30 秒自然窗，同时每 40 ms 采一次 WSL 来宾块设备和 CPU、I/O 压力计数。720,000 次请求全部成功，均低于 100 ms。GET p99 为 1.303–1.384 ms，SET p99 为 5.997–6.799 ms。35,536 笔 SET 全部匹配到批次。提交排队最长 8.139 ms，单批同步最长 9.104 ms，全局执行门等待最长 0.209 ms。3,005 份来宾样本无漏采。数据目录位于 F 盘 NVMe SSD 上的 WSL ext4 虚拟磁盘。此次只获得正常窗口的设备参照，仍无法区分异常时的虚拟磁盘、Windows 主机调度和物理盘耗时。

旧版自然尖峰里批次多次达到 8 帧。为测量提高合并上限能减少多少积压，另用相同 C 二进制、64 条连接、95% GET、6,000 请求/s，并将每次活动日志的同步返回人为延后 32 ms。每窗 5 秒、4,096 个预置键，使用独立数据副本，每窗投递 30,000 次。下表两档的区间各来自两窗，顺序为 8、16、16、8。

| 同步合并上限 | SET 从入队到写者取走 p99 | ≥100 ms 的 GET+SET（逐窗） | 客户端失败（逐窗） | 投递期内成功请求/s |
| --- | ---: | ---: | ---: | ---: |
| 8 帧 | 203.14–211.21 ms | 22,779 / 23,057 | 1,377 / 1,242 | 4,706–4,728 |
| 16 帧 | 33.63–33.96 ms | 708 / 1,212 | 0 / 0 | 5,935–5,980 |

两档实际同步中位数都约 34 ms。8 帧两窗各有 178 个批次装满。16 帧两窗各只有 6 批装满，每次同步可以确认更多已排队写入。以每批 34 ms 估算，8 帧即使批批装满也只能确认约 235 次写/s，低于实验每秒约 300 次写的投递量。每条测量连接按顺序发送请求，一个迟迟未回复的 SET 也会推迟同连接后继 GET 的发送。两档另各跑一个无注入对照，均 30,000 次成功、零条达到 100 ms。8 帧的注入窗中分别有 14 和 12 条已发送的 SET 在客户端超时后仍然提交。调用方仅凭超时无法判断是否已经写入。16 帧是针对持续慢同步和队列积压验证过的可选配置 `go_log_group_max: 16`。

最终又用 10 万键和同一 C 二进制直接运行两个毕设负载。四窗关闭服务端追踪、逐请求采样和窗内监控，使用客户端原生逐秒直方图核验。540,000 次请求全部成功，零重连、零超时。每个 30 秒窗均以指定速率完整投递。50% 写两窗在截止前均完成 2,999.87 请求/s，95% 读两窗为 5,999.97 请求/s。各档各上限只有一窗。[PING、注入与两档自然流量机器结果](observer-gate-capacity.json)

| 真实 gnet 负载 | 合并上限 | GET p99 | SET p99 |
| --- | ---: | ---: | ---: |
| 50% 写、3,000 请求/s | 8 | 1.221 ms | 7.274 ms |
| 50% 写、3,000 请求/s | 16 | 1.221 ms | 6.881 ms |
| 95% 读、6,000 请求/s | 8 | 1.286 ms | 5.538 ms |
| 95% 读、6,000 请求/s | 16 | 1.270 ms | 5.587 ms |

本轮保留原有配置，只在持续慢同步、8 帧频繁装满且写队列增长的场景把 16 列为可选缓解。定位底层慢同步还需要一次异常窗内的主机与来宾同步观测。

## 复算与复现

下列 PowerShell 命令从仓库根目录运行。原始窗口目录名中的 A 是单主拓扑名，门锁版本见各窗 `capture.json` 的 `gate_arm`。服务端的 `response_start_ns` 表示开始提交回复，gnet 的 `response_end_ns` 是 AsyncWrite 回调。客户端另行记录完整回复。服务端批次跟踪用环境变量 `KERNIVA_GOLOG_BATCH_TRACE` 指向一个新文件，正常停机后导出。端口、序号、命令名和耗时足以做离线关联。

本地证据包为 `dist/Kerniva-observer-gate-20261126-evidence.zip`，内附两端请求日志、系统调用记录、构建差异、测试日志和 SHA-256 清单。解压后保留目录结构即可独立复算。

```powershell
$raw = 'logs/golog-execution-gate-20261126T033543Z/raw'
$windows = (Get-ChildItem $raw -Directory | Where-Object Name -Match '^\d\d-').FullName
python -B scripts/golog_batch_report.py @windows --out "$env:TEMP/kerniva-observer-windows.json"
python -B scripts/golog_observer_fixture_report.py $raw --out "$env:TEMP/kerniva-observer-fixture.json"
```

旧窗等待栈可用 Go 1.26 的解析器重新导出。在 WSL 仓库根目录运行以下命令。原始运行时跟踪来自上一轮证据包。

```bash
go tool trace -d=parsed logs/golog-gc-20261126/raw/02-r95-q6000-A/server-runtime.trace |
  python3 scripts/golog_execution_gate_waits.py --start-ns 123474635815363 --end-ns 123475614249645 --out /tmp/kerniva-waits.json
```

在该项目的 WSL 环境重新执行时，可直接使用验收时封存的二进制。以下命令从仓库根目录运行，每次创建独立的结果目录，采集脚本会取得同一性能锁。

```bash
setup=logs/set-tail-stalls-20261126/setup.json
ab=/home/lemon/kerniva-gate-build.kanpExl8
c=/home/lemon/kerniva-ping-build.793ihwSZ/Cbin
out=$(mktemp -d)
python3 -B scripts/golog_batch_capture.py --setup "$setup" --baseline-bin "$ab/Abin" --bin "$ab/Bbin" --mirror "$out/observer" --order A,B,B,A,Aoff,Boff --recovery --fixture
python3 -B scripts/golog_group_cap_capture.py --setup "$setup" --bin "$c" --bin-manifest logs/golog-batch-20261126/reviews/ping-gate/C-binaries.sha256 --mirror "$out/group-cap"
python3 -B scripts/golog_group_limit_check.py --setup "$setup" --c-build-receipt logs/golog-batch-20261126/reviews/ping-gate/raw/run.json --mirror "$out/profiles"
```
