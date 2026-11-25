# 真实 TCP 主库与 Rust 整理、一主两从的固定速率对照

2026 年 11 月 25 日，WSL ext4 上的 gnet 主库在两档负载、两种部署中均通过。8 个正式窗各测量 30 秒，合计发出 1,080,000 个请求，全部成功。其中 1,079,983 个在各自的测量窗内完成。**完整拓扑的同步 SET p99 在四对中都上升，增加 0.705–1.114 ms。** 95% 读负载的两对 GET p99 一升一降。[逐窗机器数据](replica-live-load.json) · [开发路线](../golog_next_stage_plan.md)

## 主库的实际负载和尾延迟

A 只运行主库，B 使用相同的主库程序和持久化设置，加入 Rust 整理、复制源和两个独立目录的只读副本。每档按 A、B、B、A 顺序运行，对应相邻的正向和反向两对。每窗预填 10 万个 32 字节值，使用 64 条连接、均匀键分布和种子 42，预热 5 秒。两臂在相同的计划偏移写入相同的标记。额外观测命令另计。延迟从计划发送计到收到完整回复，包括客户端排队。

| 正式窗 | 读比例与计划请求/s | 拓扑 | 实际成功/s（含排空） | GET p99 / p99.9（ms） | SET p99 / p99.9（ms） |
| --- | --- | --- | ---: | ---: | ---: |
| 01 | 50%，3,000 | A | 2,999.779 | 1.188 / 2.060 | 7.225 / 9.470 |
| 02 | 50%，3,000 | B | 2,999.823 | 1.200 / 1.749 | 7.930 / 11.403 |
| 03 | 50%，3,000 | B | 2,999.402 | 1.204 / 1.700 | 7.930 / 11.600 |
| 04 | 50%，3,000 | A | 2,999.713 | 1.196 / 1.507 | 6.832 / 9.011 |
| 05 | 95%，6,000 | A | 5,999.958 | 1.262 / 1.483 | 5.259 / 8.192 |
| 06 | 95%，6,000 | B | 5,999.970 | 1.290 / 1.520 | 6.373 / 9.896 |
| 07 | 95%，6,000 | B | 5,999.979 | 1.294 / 1.544 | 6.406 / 9.372 |
| 08 | 95%，6,000 | A | 5,999.978 | 1.348 / 1.548 | 5.652 / 8.323 |

每窗均为零失败，含排空和窗口内的发送、成功速率四项都达到目标的 98%，逐秒队列通过既有的持续增长检查。50% 写的两对 GET p99 分别增加 0.012 和 0.008 ms。95% 读的两对分别增加 0.029、降低 0.053 ms。四对 SET p99.9 均增加，范围为 1.049–2.589 ms。每档只有两对。配对差描述本机完整拓扑和这些观测操作的合计影响。

## 封存后多久能读到

DB 0 承载均匀业务请求，DB 1 只承载种子和探测标记。每个窗口每库写 3 个唯一标记。48 个标记均从封存段中重新匹配到原始 SET 帧、CRC 和确切序号。在四个 B 窗中，每个标记分别跟踪到两个副本，共得到 48 组源端可取至正确 GET 的区间，以及 48 组至磁盘 `CURRENT` 路径可见的区间。每项均有前后观测。

| 负载 | 标记与副本组合 | 源端可取至 GET 的中位区间 | 最大观测上界 | 源端可取至 CURRENT 路径可见的中位区间 |
| --- | ---: | ---: | ---: | ---: |
| 50% 写，3,000/s | 24 | 25.228–81.272 ms | 132.528 ms | 0–142.564 ms |
| 95% 读，6,000/s | 24 | 12.530–66.860 ms | 132.142 ms | 0–150.505 ms |

源端上界来自实际拉取含该标记的封存段，副本读值的上界来自首次正确 GET 的回复。上下界分别取中位数。磁盘轮询只能界定读者看到新路径的时间。表中的 0 表示两个观测区间重叠。副本收到哪一纳秒的 fsync 成功返回，需要内部探针另行量测。

在每一档、每一库，两从合计各有 476 次盘上游标采样完整落在投递窗内。DB 0 在 50% 写档有 16 次可以确定落后，最大落后 2,300 个已封存提交序号。95% 读档有 24 次，最大 467 个序号。DB 1 两档的最大值均为 1。四组中位滞后均为 0。该指标从同来源的封存前沿计算，单位是提交序号。每个 B 窗终局均核对主库、源、两从的序号，两个库的最终序号按负载分别是 `[52873,4]` 和 `[10807,4]`。三节点每端均核对 32 个预先确定的业务键，并校验两个库的标记。

每个 B 窗都记录了 23 次完全落在投递窗内的 Rust 快照截止序号推进，共 92 次。两从在各窗内均有推进。24 个标记中，7 个的发送至可读区间与快照发布的观测区间相交。没有一次能把快照发布确定置于“源端已验证可取”和“副本最后一次返回 404”之间。B 侧为观测另发出 24 次源拉取，共读取 1,619,972 字节，并发出 2,543 次副本 GET。06 窗期间 Windows 宿主约有 4 秒在 E 盘镜像上复算另一份 JSON；无法从该对照单独量出它的影响。

## 证据与复现

运行基于 `b754e3e` 的 Go、Rust 和 bench 二进制，使用新建的 WSL ext4 路径和既有独占性能锁。闭环拓扑冒烟的记录在 `logs/replica-live-smoke-20261125T125213185Z/`。正式对照镜像为 `logs/replica-live-load-20261125/`。其中 `pilot` 的四窗只具备终局磁盘游标。`pilot-002` 新增逐标记的路径可见区间和原始 GET 轮询，又跑完四窗才取得正式放行。两组试窗均单列，正式分母为 8。

正式矩阵退出码为 0，使用的 20 个端口均释放。Rust 整理进程在各 B 窗结束后按计划收到 SIGTERM，主库、源、两从及客户端正常退出。137 份正式业务证据、9,551,606 字节经 ext4 与 Windows 逐文件 SHA-256 回读。完整镜像的校验清单覆盖 326 份文件、21,082,966 字节。

| 文件 | SHA-256 |
| --- | --- |
| 镜像 `sha256.txt` | `6c75d21b38e0f1882a9b446bd58f6eea226deaab833481ccc07766b0d4492d1e` |
| `reviews/formal-ext4-readback.json` | `8aef39a03679830b4f9889c0acd5399c1e99a965ce45b4bd5db8e296065b6022` |
| `reviews/formal-observer-independent.json` | `91530e2bef871f71dda15a6ca14fcd4b143d0dc0c7c7b510709f86ef556d92cd` |
| `reviews/formal-astra.json` | `705a79b1801c597569dde232c56141ecfb7567b0f48f95e2a579c31c7e027e3c` |
| 公开逐窗 JSON（UTF-8、LF） | `641072c1836684b6238ec7e1ae63fb04d5f2d707cb74630b66aa83c9113f2b17` |

在有本地镜像的仓库根目录执行以下命令即可离线复算公开数据。第一项检验清单完整性，第二项从逐秒计数和稀疏直方图重算四项速率、分位数及配对差，并附上标记与快照记录。第三项是独立的观测器核验，会从封存日志重新解析标记。

~~~bash
set -euo pipefail
mirror=logs/replica-live-load-20261125
out=$(mktemp -d)
printf '%s  %s\n' 6c75d21b38e0f1882a9b446bd58f6eea226deaab833481ccc07766b0d4492d1e "$mirror/sha256.txt" | sha256sum -c -
sha256sum --quiet -c "$mirror/sha256.txt"
python3 -B scripts/golog_replica_live_compare.py "$mirror/formal" --phase formal --with-replication --output "$out/formal.json"
printf '%s  %s\n' 641072c1836684b6238ec7e1ae63fb04d5f2d707cb74630b66aa83c9113f2b17 "$out/formal.json" | sha256sum -c -
python3 -B "$mirror/reviews/live-observer-independent.py" "$mirror/formal" --output "$out/observer.json"
~~~

在保有已冻结二进制的原 WSL 主机上，下面的命令在全新的 ext4 目录中重放完整八窗。原始的逐参数命令在镜像的 `formal/command.sh`。独立重放使用新的截止时间。重建二进制所需的源码归档、Go/Rust 版本和构建命令分别在镜像的 `source.tar`、`toolchain.txt`、`prepare.sh`。

~~~bash
set -euo pipefail
mirror=$(realpath logs/replica-live-load-20261125)
src=$(cat "$mirror/source-root.txt")/source
run=$(mktemp -d "$HOME/kerniva-repeat-live.XXXXXXXX")
lock=/home/lemon/kerniva-development-20261124/logs/golog-followup-bSuIKDwd/.cpu-exclusive.lock
(cd "$src" && sha256sum --quiet -c "$mirror/binaries.sha256")
(cd "$src/scripts" && sha256sum --quiet -c "$mirror/frozen/formal/scripts.sha256")
python3 -B "$src/scripts/golog_replica_live_load.py" --stage formal --root "$run/formal" \
  --bin "$src/bin" --lock "$lock" --duration 30 --warmup 5 --stop-opening "$(date -Is -d '+1 hour')" \
  --source-revision "$(cat "$mirror/source-revision.txt")" \
  --source-archive-sha256 eaaecfe746734cc7330ca3395ee69a899a7423650b57a7a3ad960aac613bf780 \
  --pilot-plan "$mirror/reviews/pilot-plan.json" --pilot-plan-sha256 a4af127ff96cc916ca57921531484141ee078d3b8f94c41b5469a244b5e4a2da \
  --approval "$mirror/reviews/pilot-002-astra.json" --approval-sha256 68f9318b1a347331379258c1aaf6974cb4031820c367e00d788a526cb1365c3a
~~~

Astra 已接纳完整拓扑下的固定速率与复制观测。下一项实验可以固定观测开销，分别启用 Rust 整理、复制源和两从，再重复相同的相邻配对，判断哪一项能复现同步 SET 尾延迟增加。现有生产实现继续作为基线。
