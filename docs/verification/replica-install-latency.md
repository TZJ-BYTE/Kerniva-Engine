# 副本安装 64 MiB 大增量的长尾调查

本次测量使用 WSL 2 的真实 HTTP 源和只读副本。每窗从 DB 15 的游标 4 开始，一次安装同一份 18 段封存日志，共 67,126,416 字节，发布到 260。安装后逐一校验 18 个文件与 256 个键。生产源码取自 `8eda4dd`，运行号为 `kerniva-replica-latency.pIx8R4RG`。本轮虚拟磁盘位于 F 盘。历史 E 盘上两次超过 2 秒的观测保留为历史参照。[调查计划](../golog_replica_24h_plan.md)

## 校准

11 月 25 日（上海时间）首次粗探针重复性对照的 8 窗耗时为 304.290–451.958 ms，中位 317.841 ms。这个区间从清单校验完成计到 `CURRENT` 最后一次目录同步成功。两臂使用同一个已验收的粗探针二进制。

| 校准组 | 窗数 | 配对结果 |
| --- | ---: | --- |
| 同一诊断二进制，细探针与粗探针 | 8 | 四对安装耗时变化为 +0.471%、−2.007%、−0.356%、+1.738%，中位 +0.058%。小于预定的 5% 扰动限值。 |
| 同一外部观察者，粗探针与普通构建 | 8 | 启动至首次正确 GET 的配对时间差中位数落在 −4.923 至 +7.424 ms。 |
| 同一外部观察者，关闭追踪的诊断构建与普通构建 | 8 | 同口径差值落在 −14.609 至 −2.446 ms。 |

三组校准均通过原始事件与文件复核。外部观察者记录源已可取的见证、进程启动区间、磁盘 `CURRENT` 首次可见、首次正确 GET。路径可见发生在重命名之后。正确 GET 为最后一次目录同步已成功提供了时间上界。粗、细探针用同一单调时钟给出持久化完成的内部时间点。细事件保留每次传输、同步、回放以及 Go 回调的嵌套关系，汇总时父子耗时分别解释。

Astra 已批准正式块使用细探针。每个时段运行两个完整八窗块，在同一物理存储上统计。达到慢窗门槛的窗口需与同一时段、同一二进制和探针模式的正常窗比较。共同外部对照与校准均单独记账。

## 三个时段的正式测量

11 月 25 日以同一诊断构建运行 6 个八窗块，48 个窗口中出现 **0 个 ≥2 秒慢窗**。安装耗时最短 288.569 ms，中位 324.071 ms，最长 400.252 ms。从发起拉取到持久化的有效吞吐中位为 196.902 MiB/s，到发布读视图为 196.813 MiB/s。每个时段内两块均在原计划时限内首次通过。

| 时段起点（上海时间） | 窗数 | 两块各自最长安装耗时 |
| --- | ---: | --- |
| 05:45:39 | 16 | 363.744 / 350.916 ms |
| 07:45:39 | 16 | 366.746 / 344.143 ms |
| 09:45:39 | 16 | 400.252 / 339.124 ms |

48 个正常窗口里，准备读视图的中位耗时为 196.973 ms，其中 Rust 增量恢复包含 Go 行解码回调。接收并保存日志段的中位耗时为 117.396 ms，其中逐段同步合计 92.549 ms，复制合计 23.356 ms。最后一次目录同步的中位耗时为 1.763 ms。父阶段内包含子阶段，各行中位数单独计算。这些数字可以描述正常安装成本。归因历史数秒慢窗仍需要实际捕获慢窗。

外部观察者共取得 375 个有效进程样本，每窗 7–9 个，采样错误为 0，最长相邻间隔 55.41 ms。每窗首末样本间副本消耗的 CPU 时间为 210–310 ms，中位 240 ms。内核记账的写入量每窗均为 67,215,360 字节。所有窗口开始和结束时 F 盘均有约 213.14 GiB 可用。逐窗的 `b-external.jsonl` 保留原始计数和采样时刻。

## 已验收版本的故障恢复

同一份 DB 15 增量已用普通生产副本做独立恢复测试。在源已发出 524,288 字节时截断传输，并让后续拉取持续返回 503。游标仍为 4，原标记可读，256 个新键均返回 404。保持断源重启后仍读到同一旧代。恢复源后只有一次完整发布，游标到达 260，原生事件证实同代的最后一次目录同步先于读视图切换。再次断源重启后，18 个封存文件的 SHA-256 和 256 个 GET 值均一致，两次独立启动的恢复读均无需新安装。

另一个一主两从故障窗使用 16 库真实夹具，限并发 4。DB 1 的快照传输到 524,288 字节时截断，完整重试接收 8,400,792 字节，安装快照 260 和后缀 261。15 个快照库与 1 个轻库在 39.126 秒内追齐。两从的 32 个磁盘游标、3 次跨库写入和 5 个进程的退出状态均通过复核。最后一次重启将 B 指向无监听端口，核对 16 库本地读值。Astra 已接纳两项基线可靠性证据。

## 复验入口

原始记录在 WSL 的 `/home/lemon/kerniva-replica-latency.pIx8R4RG`。独占锁沿用开发树 `logs/golog-followup-bSuIKDwd/.cpu-exclusive.lock`。Windows 镜像位于仓库内的 `logs/golog-replica-latency/kerniva-replica-latency.pIx8R4RG`，每块保留运行参数、退出状态与逐窗摘要，审查决定在 `reviews/`。前端关闭、第二执行者争锁、子任务中断和跳过已完成块的演练保存在同级 `smoke-witness`。

镜像的 `frozen/` 保存本轮实际使用的脚本源码和生成的 Go 探针文件，`fixture/result.json` 保存冻结夹具索引。正式块使用的诊断程序 SHA-256 为 `33c5b88ad96ea91935943d9c983b876f23ccf339181ec46be5e95ba579b4a8f7`。归档中的 `control/probe-inputs.json` 锁定了外部采样器源码。复现本轮历史读数时使用冻结的脚本和相同封存文件。

在 Windows 脱离 WSL 重新计算时，按镜像 `reviews/mirror-recompute.md` 执行 PowerShell。三个正式时段与四组校准的 298 份账本文件摘要通过核验，80 窗逐项与原报告吻合。另存于 `history/direct-1`、`history/direct-2` 的 56 份小文件（59,458 字节）与旧盘归档逐字节一致；独立复算得历史慢窗 2/16，均在第一块。历史粗探针只能量出完整安装区间。

仓库版工具另加了逐窗断点。驱动在每窗完成后写入索引，调度器把索引摘要写入尝试账本。中断后，复算器单列能核对摘要的窗口和缺少完整证据的窗口。正式块、配置与补采放行规则仍按本轮预登记口径复算。Windows 上重新读取三时段和四组校准的全部 80 窗，两版复算逐窗一致。版本摘要与中断验证结果见镜像的 `reviews/checkpoint-audit-smoke.txt`。

下例复算已完成的校准块。在本项目根目录执行，仅读已归档数据。复算器校验冻结夹具、块与各窗结果摘要，随后从原始事件重算耗时。运行账本提供失败和中断记录。

~~~bash
run=/home/lemon/kerniva-replica-latency.pIx8R4RG
frozen=/home/lemon/kerniva-reader.SQXPRb7E/direct-1
id=calibration-coarse-fine-01
python3 -B scripts/diagnostics/golog_install_latency/summarize.py \
  --fixture "$frozen/result.json" --ledger "$run/ledger.json" \
  --block setup "$id" calibration "$run/blocks/$id/attempt-001/result"
~~~

要重放相同夹具，可使用下例。输出目录由 `mktemp` 创建。普通臂对应历史验收过的 `candidate-B`，同一个细探针构建可通过参数切换 `coarse`、`fine` 或 `off`。

~~~bash
lock=/home/lemon/kerniva-development-20261124/logs/golog-followup-bSuIKDwd/.cpu-exclusive.lock
out=$(mktemp -d "$HOME/kerniva-install-replay.XXXXXXXX")
diag="$run/build/fine/bin/kerniva-replica"
flock -n "$lock" python3 -B scripts/golog_replica_direct_delta.py \
  --root "$out" --origin /home/lemon/kerniva-memory-compare.AwbOfm/ordinary-2-candidate-1-cap0 \
  --prepared "$frozen" --bin /home/lemon/kerniva-memory-phases.apFiuiuT/src/bin \
  --baseline "$diag" --candidate "$diag" --baseline-mode coarse --candidate-mode fine \
  --observe --sample-interval-ms 50 --host-disk /mnt/f --order ABBA-BAAB
~~~
