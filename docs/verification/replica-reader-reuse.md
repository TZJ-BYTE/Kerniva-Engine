# 16 库副本复用响应读取缓冲区

本轮在已接纳 HTTP 连接池的版本上，将副本每次拉取创建的 `bufio.Reader` 改为每个逻辑库一个可复用的 reader。一个库的拉取与发布由同一把 `pollGate` 串行保护。获得 HTTP 响应后才绑定 reader，每次返回前用 `Reset(nil)` 丢弃未读状态并解绑响应体。各库仍可独立拉取。[连接池基线与上一轮结果](replica-idle-polling.md)

**验收通过，采用提交 `4c2b9cd`。** 每次成功空拉取少分配 4,119–4,222 字节。16 库全部预热后，缓冲与结构字段合计多占 65.375 KiB。真实增量、快照与故障实验均通过预设门槛。正式基线 B 二进制 SHA-256 以 `e2473a87` 开头，正式候选以 `cc76db06` 开头；从完整回归工作区独立构建的候选与实测二进制逐字节相同。[机器记录](replica-reader-reuse.json)

## 成功空拉取的分配

使用同一份已追齐的 16 库夹具、HTTP/1.1 回环源与 Rust 动态库。每库先完成 3 次预热，全部库在两次拉取之间停稳后记录 Go 的 `TotalAlloc`。分母只计测量窗内完整读取、通过校验且游标未变的成功空响应。源端独立核实请求数等于这个分母加 48。A 表示连接池基线，B 表示复用 reader 的候选版。

| 每库间隔与顺序 | 完成次数 A / B | 基线 B/次 | 候选 B/次 | 每次节省 | 降幅 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 100 ms，B→A | 2,384 / 2,384 | 10,598 | 6,394 | 4,204 B | 39.7% |
| 100 ms，A→B | 2,384 / 2,368 | 10,579 | 6,399 | 4,180 B | 39.5% |
| 1 s，B→A | 240 / 240 | 10,651 | 6,532 | 4,119 B | 38.7% |
| 1 s，A→B | 240 / 240 | 10,766 | 6,543 | 4,222 B | 39.2% |

所有正式精确窗均建立 16 条 TCP 连接。100 ms 正向对照中，B 的每个库都少做一轮，故上表使用各窗实际次数。初次 A→B 与一次短功能测试的准确时序无法核实，原数据保留，另开干净的 A→B 两档测量作为正式配对。四组均超过预设的 3,584 B/次节省门槛。

独立剖析窗口中的 `pullOnce → bufio.NewReaderSize` 栈在基线两个间隔均有采样。候选两个窗口均未采到该应用栈。100 ms 候选窗仍有少量 HTTP transport 建连时的读取缓冲分配。剖析采用 1 KiB 采样和前后快照，仅用于定位调用栈。[诊断工具](../../scripts/diagnostics/golog_idle_polling/README.md)

### 复用保留了多少内存

用相同的结构探针打开两版真实副本，记录首次请求前以及 16、32、48 个成功请求后的容量与每库缓冲地址。候选在打开时尚未分配缓冲；16 库各拉取一次后共有 16 个不同的 4,096 字节数组，后续地址稳定。逐库检查确认轮询之间 reader 已解绑 HTTP 响应。Go/Linux amd64 上，单库结构从 152 增至 240 字节。16 库额外容量为 **65,536 + 16 × 88 = 66,944 字节**，未计分配器按大小类别取整。主动关闭后，若调用方继续持有该副本对象，缓冲也继续可达。

默认间隔普通窗的终点 `HeapAlloc` 两版均约为 300 MB，八个普通空闲窗均未触发 GC。该进程级数字包含整库已发布数据，上述 66,944 字节来自实际结构和缓冲容量的单独测量。[结构探针源码](../../scripts/diagnostics/golog_idle_polling/reader_footprint_linux_test.go.txt)

## 普通写入与副本可见性

正式 CLI 两档间隔各按 A、B、B、A 运行。每个窗口先测约 15 秒空闲分配，再关闭该 B，以相同目录启动无诊断的 B，分别向 DB 15、0、7、14 发送真实 gnet / RESP 同步写入。源可取时间由封存文件与源端正确 HTTP 载荷夹定，随后轮询 B 的磁盘 `CURRENT` 与外部 GET。

| 每库间隔 | 空闲分配 A，MiB/s | 空闲分配 B，MiB/s | GET 上界中位数 / 最大值 A | GET 上界中位数 / 最大值 B |
| --- | ---: | ---: | ---: | ---: |
| 100 ms | 1.597 / 1.593 | 0.962 / 0.962 | 82.10 / 108.03 ms | 86.24 / 128.70 ms |
| 1 s（默认） | 0.165 / 0.164 | 0.099 / 0.098 | 462.10 / 931.45 ms | 515.05 / 1,018.43 ms |

32 个标记的 `CURRENT` 和 GET 均符合期望。默认间隔下，候选 DB 7 有一笔源可取至 GET 为 995.60–1,018.43 ms。每版每档只有 8 个普通样本，报告保留逐笔观测区间。[逐窗数据](replica-reader-reuse.json)

默认间隔的磁盘游标上界中位数 / 最大值为 A 461.63 / 931.11 ms、B 514.70 / 1,018.11 ms。普通窗口结束时，多数观测为 16 条到源的已建立连接和 25 个 FD；候选有一个默认窗为 15 条和 24 个 FD。正式精确计数窗口单独统计自进程启动以来新建过的连接数。

### 控制写入落在轮询周期中的位置

副本用同一单调时钟记录前一轮空拉取结束、新请求、清单校验、`CURRENT` 目录同步和读视图发布。源可取的测量区间须不宽于 15 ms，各窗实际相位须在目标及其他入组样本的 20 ms 内。先对 DB 15、0、7 分别在前轮结束后约 25、450、850 ms 触发封存，12 个标记均在其首个可取请求持久发布。B 相对 A 的请求至视图中位数差分别为 +0.33、−1.37、−1.63 ms。一次基线的 DB 15 源可取时刻超出匹配容差，保留失败记录并重跑。

额外在前一轮刚结束时触发 DB 0 封存。四笔均在首轮发布，A 的请求至读视图为 153.67 / 16.03 ms，B 为 13.98 / 74.58 ms。两个较慢样本中，清单校验至 `CURRENT` 目录同步完成分别占 153.14 和 73.98 ms。该段包含文件安装和同步，现有时间戳只能定位到这个区间。其中一次候选尝试因封存相位偏差被排除。

针对普通窗口中最长的 DB 7 再做 +5 ms 实验。四个窗口均在首个可取轮次发布。A 的两笔 GET 上界为 1,008.72 和 1,010.63 ms，B 为 1,014.05 和 1,016.89 ms。请求至读视图的中位数差为 +1.77 ms。三个场景共 20 个合格标记，探针均完整。各库仍从上一次拉取结束开始等待默认的 1 秒。

## 快照、增量与故障

真实一主两从、15 库并行快照、1 个轻库，B 使用默认快照并发参数，链路共享 8 MiB/s 限额。两版复用同一源和 A 副本，只替换负责恢复的 B。每窗核对快照哈希、后缀、两从的磁盘游标与读值，并在快照传输途中向轻库写入并读取 3 个标记。

| 窗口顺序 | A1（复跑） | B1 | B2 | A2 |
| --- | ---: | ---: | ---: | ---: |
| 16 库追齐，秒 | 39.045 | 39.283 | 39.082 | 39.071 |
| 共享链路实际载荷，MiB/s | 7.271 | 7.286 | 7.252 | 7.246 |

12 个轻库标记均成功。初次 A1 实速仅 6.16 MiB/s，低于脚本原有的 6.4 MiB/s 门槛，当窗最大采样空档为 7.19 秒。原窗留档，复跑达到门槛。固定带宽下的结果适用于这条共享链路。

另用候选版把快照并发上限设为 4，传到 524,288 字节时截断 DB 8 响应。旧磁盘游标保持序号 2，GET 返回旧值。重试完整接收 8,400,792 字节并清掉中断的临时文件。随后断开源，重启 B，16 库都能从已发布位置恢复。

### 无整形直连大增量

在已追齐的 DB 15 通过真实 gnet 主库同步写入 256 个不同键，每值 256 KiB，待最后一段封存后冻结这批文件。每窗从同一旧游标向同一回环 HTTP 源拉取，逐段核对源载荷、磁盘安装文件的 SHA-256，逐键核对 256 个 GET，最后验证诊断事件完整。有效吞吐取实际文件字节数除以首个有效请求发起至 `CURRENT` 持久化或读视图发布的时间，计入校验、安装和磁盘同步。

事先固定顺序为 ABBA、BAAB，相邻窗组成四对。读视图和持久化两个口径都要求候选/基线吞吐比的中位数至少为 0.95，且低于 0.90 的配对少于两对。如果首块越线，或比值极差超过 0.20，就用同一套已封存文件重跑完整八窗。16 库限速快照窗口另行列示。

| 64.02 MiB 直连有效吞吐，候选 / 基线 | 第 1 对 | 第 2 对 | 第 3 对 | 第 4 对 | 中位数 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 第一块，请求至读视图 | 0.969 | 2.296 | 1.047 | 1.014 | 1.031 |
| 同文件复验，请求至读视图 | 1.008 | 1.190 | 1.199 | 1.041 | 1.116 |

首块第二对从清单校验至 `CURRENT` 同步的耗时为 A 5.463 秒、B 2.379 秒，触发极差复验。第二块两种口径的极差均约 0.191，达到预设标准。两块八对的中位数均约 1.044，最低 0.969。两块使用相同的 18 个封存段，共 67,126,416 字节，16 窗均在一次请求中将游标 4 推进到 260。可得出的结论是这批大增量通过了退化复验，稳定加速幅度仍待更多环境验证。

第二块第一次准备时，源进程刚启动，首个见证请求遇到连接拒绝，当时还没有进入任何测量窗口。脚本增加源监听就绪检查，保留这次准备记录，然后用相同封存文件完成复验。[直连驱动与校验](../../scripts/golog_replica_direct_delta.py)

## 恢复与竞态回归

- 新增同库尾随字节回归。先成功发布序号 1，再喂入含未读尾随内容的响应，核对 GET 仍返回旧值、`CURRENT` 原字节及序号未变；立即用内容不同的完整响应重试，发布序号 3 并读到新值。
- 完整回归通过 Rust 97 项、Go 205 个顶层测试及 102 个子测试。供父测试单独调用的崩溃子进程入口在普通枚举中跳过。额外对同库重试、并发轮询和关闭、快照等候、损坏恢复及连接池等六项分别执行 8 轮竞态检测，全部通过。
- 旧源配新从、新源配旧从的真实 gnet 一主两从均通过增量、快照补齐及主库重启。下一阶段优先把两个版本都出现过的慢窗细分到文件接收与校验、回放、文件同步和目录同步，定位后再选改动。

## 复现

需要 Linux ext4、Go、Rust、Python 3.12 以上和 Bash。按下列顺序运行，一次仅启动一个实验。正式数据使用上一阶段停止写入的同一套源与副本。下面从代码新建等价夹具，重新生成的来源 ID 和各段哈希会不同。

~~~bash
set -euo pipefail
repo="$PWD"
run=$(mktemp -d "$HOME/kerniva-reader.XXXXXXXX")
old=1aab141
new=4c2b9cd
for ver in baseline candidate; do
  rev=$old
  if [[ $ver == candidate ]]; then rev=$new; fi
  mkdir "$run/$ver-src"
  git archive "$rev" | tar -x -C "$run/$ver-src"
  (cd "$run/$ver-src" && go build -trimpath -o "$run/$ver-B" ./cmd/kerniva-replica)
done
(cd "$run/baseline-src" && bash scripts/build.sh)
bin="$run/baseline-src/bin"
mkdir "$run/seed" "$run/ordinary" "$run/phase" "$run/phase-db0" "$run/phase-db7" "$run/loaded" "$run/direct-1"
snap=(python3 -B scripts/golog_replica_snapshot_capacity.py --bin "$bin")
"${snap[@]}" --root "$run/seed" --prepare
"${snap[@]}" --root "$run/seed" --name seed --limit 0
origin="$run/seed/seed-1-cap0"
diag=(python3 -B scripts/diagnostics/golog_idle_polling/run.py --repo "$repo"
      --origin "$origin" --library "$bin/libkerniva_engine.so")
"${diag[@]}" --revision "$old" --root "$run/A1"
source_bin="$run/A1/idle-diagnostic"
"${diag[@]}" --revision "$new" --root "$run/B1" --source-binary "$source_bin"
for entry in "B2:$new" "A2:$old" "A3:$old" "B3:$new"; do
  "${diag[@]}" --revision "${entry#*:}" --root "$run/${entry%%:*}" --source-binary "$source_bin" \
    --window 1s exact --window 100ms exact
done
python3 -B scripts/golog_replica_poll_ordinary.py --root "$run/ordinary" --origin "$origin" \
  --bin "$bin" --baseline "$run/baseline-B" --candidate "$run/candidate-B"

for ver in baseline candidate; do
  python3 -B scripts/diagnostics/golog_idle_polling/build_phase.py \
    --checkout "$run/$ver-src" --root "$run/phase-$ver"
done
pair=(--origin "$origin" --bin "$bin" --baseline "$run/phase-baseline/bin/kerniva-replica"
      --candidate "$run/phase-candidate/bin/kerniva-replica")
for dir in phase phase-db0 phase-db7; do
  flags=()
  if [[ $dir == phase-db0 ]]; then flags=(--phase 0 5); fi
  if [[ $dir == phase-db7 ]]; then flags=(--phase 7 5); fi
  python3 -B scripts/golog_replica_poll_phase.py "${pair[@]}" --root "$run/$dir" "${flags[@]}"
done
for entry in 01-A:baseline 02-B:candidate 03-B:candidate 04-A:baseline; do
  "${snap[@]}" --fixture "$run/seed/fixture" --root "$run/loaded" --limit 0 \
    --follower "$run/${entry#*:}-B" --name "${entry%%:*}"
done
"${snap[@]}" --fixture "$run/seed/fixture" --root "$run/loaded" --limit 4 --fault \
  --follower "$run/candidate-B" --name candidate-fault
python3 -B scripts/golog_replica_direct_delta.py "${pair[@]}" --root "$run/direct-1"
# 如配对波动触发复验，复用首块逐字节相同的源文件。
mkdir "$run/direct-2"
python3 -B scripts/golog_replica_direct_delta.py "${pair[@]}" --root "$run/direct-2" --prepared "$run/direct-1"
(cd "$run/candidate-src" && bash scripts/test.sh -race)

mkdir "$run/footprint"
cp -a "$origin/primary-data" "$run/footprint/primary-data"
for ver in baseline candidate; do
  cp scripts/diagnostics/golog_idle_polling/reader_footprint_linux_test.go.txt \
    "$run/$ver-src/internal/gologreplica/reader_footprint_linux_test.go"
  cp -a "$origin/b" "$run/footprint/$ver-state"
  (cd "$run/$ver-src" && KERNIVA_ENGINE_LIBRARY="$bin/libkerniva_engine.so" \
    IDLE_DIAGNOSTIC_SOURCE_ROOT="$run/footprint/primary-data/go-log-v1" \
    IDLE_DIAGNOSTIC_FOLLOWER_ROOT="$run/footprint/$ver-state" \
    KERNIVA_READER_FOOTPRINT="$run/footprint/$ver.json" \
    go test -count=1 -run '^TestReaderFootprint$' ./internal/gologreplica)
done
python3 -B scripts/diagnostics/golog_idle_polling/summarize_reader.py --root "$run" --output "$run/summary.json"
~~~

限速恢复若低于脚本原有的最低有效速率，保留失败窗口的 `result.json`，将重跑前缀命名为 `01-A-retry1` 等，汇总器才能将两次尝试并排记录。诊断副本与普通副本使用不同二进制，两版的探针位置相同。[真实无整形增量脚本](../../scripts/golog_replica_direct_delta.py) · [指标复算脚本](../../scripts/diagnostics/golog_idle_polling/summarize_reader.py)
