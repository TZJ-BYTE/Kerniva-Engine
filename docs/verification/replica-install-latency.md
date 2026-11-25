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

## 第四时段补采

11 月 25 日 13:45:39 在同一台机器、同一版本和同一批封存文件上开始补采。两块均首次通过，控制器于 13:45:57 正常退出。**新增 16 窗，0 窗达到 2 秒。** 独立复算器从原始事件、外部观测和文件摘要重建了每个窗口。[逐窗数据](replica-install-latency.json)

| 补采块 | 有效窗 | 安装中位数 | 安装最大值 |
| --- | ---: | ---: | ---: |
| B1 | 8/8 | 372.667 ms | 399.842 ms |
| B2 | 8/8 | 327.159 ms | 374.499 ms |
| 两块合计 | 16/16 | 357.999 ms | 399.842 ms |

补采从发起请求到持久化、到读视图发布的吞吐中位数分别为 178.388 和 178.346 MiB/s。132 份有效进程采样中错误为 0。本时段两块的安装中位数相差 45.508 ms，两块使用相同构建。

综合前三个时段的 48 窗与补采的 16 窗，本机观测为 **0/64** 个慢窗，安装范围 288.569–400.252 ms，中位 328.777 ms。历史 E 盘的两个慢窗都来自同一个八窗块。本轮没有捕获可用于归因的慢窗，也没有新的生产性能候选。镜像的 `reviews/fourth-ledger.receipt.json` 记录源侧两次散列与 Windows 回读的同一账本摘要；独立收据 `reviews/independent-fourth.json` 的 SHA-256 为 `bc13e6803f5c79dda0594da56075545b614bd8e9565c046abcb33dc1c91030e7`。

Astra 在 T+13 审查中结束性能补采，放行 15:45:39 的截断增量恢复和 20:45:39 的一主两从快照恢复。两项均使用生产基线，各自按实际故障过程验收。审查记录为镜像的 `reviews/t-plus-13.json`。

## 已验收版本的故障恢复

同一份 DB 15 增量已用普通生产副本做独立恢复测试。在源已发出 524,288 字节时截断传输，并让后续拉取持续返回 503。游标仍为 4，原标记可读，256 个新键均返回 404。保持断源重启后仍读到同一旧代。恢复源后只有一次完整发布，游标到达 260，原生事件证实同代的最后一次目录同步先于读视图切换。再次断源重启后，18 个封存文件的 SHA-256 和 256 个 GET 值均一致，两次独立启动的恢复读均无需新安装。

另一个一主两从故障窗使用 16 库真实夹具，限并发 4。DB 1 的快照传输到 524,288 字节时截断，完整重试接收 8,400,792 字节，安装快照 260 和后缀 261。15 个快照库与 1 个轻库在 39.126 秒内追齐。两从的 32 个磁盘游标、3 次跨库写入和 5 个进程的退出状态均通过复核。最后一次重启将 B 指向无监听端口，核对 16 库本地读值。Astra 已接纳两项基线可靠性证据。

## 复验入口

原始记录在 WSL 的 `/home/lemon/kerniva-replica-latency.pIx8R4RG`。独占锁沿用开发树 `logs/golog-followup-bSuIKDwd/.cpu-exclusive.lock`。Windows 镜像位于仓库内的 `logs/golog-replica-latency/kerniva-replica-latency.pIx8R4RG`，每块保留运行参数、退出状态与逐窗摘要，审查决定在 `reviews/`。前端关闭、第二执行者争锁、子任务中断和跳过已完成块的演练保存在同级 `smoke-witness`。

镜像的 `frozen/` 保存本轮实际使用的脚本源码和生成的 Go 探针文件，`fixture/result.json` 保存冻结夹具索引。正式块使用的诊断程序 SHA-256 为 `33c5b88ad96ea91935943d9c983b876f23ccf339181ec46be5e95ba579b4a8f7`。归档中的 `control/probe-inputs.json` 锁定了外部采样器源码。复现本轮历史读数时使用冻结的脚本和相同封存文件。

在 Windows 脱离 WSL 重新计算时，按镜像 `reviews/mirror-recompute.md` 执行 PowerShell。三个正式时段与四组校准的 298 份账本文件摘要通过核验，80 窗逐项与原报告吻合。另存于 `history/direct-1`、`history/direct-2` 的 56 份小文件（59,458 字节）与旧盘归档逐字节一致；独立复算得历史慢窗 2/16，均在第一块。历史粗探针只能量出完整安装区间。

### 脱离原实验目录重放

镜像的 `fixture/db15-linux-amd64-replay.tar.gz` 包含 DB 15 的原始封存文件、游标 4 的副本种子、冻结的 Linux amd64 主库、源端、副本、动态库和驱动。压缩包为 20,438,115 字节，SHA-256 为 `96e4f126625851b1a2b5361ce769996b4c57438b39a9d5a6bbae3bd951cfecd0`。包内 `inputs.json` 可逐项追溯到登记的输入文件，`sha256.txt` 可核验全部 71 份输入，合计 110,831,298 字节。

在新建的 ext4 目录中解包复验，包内源端通过真实 HTTP 返回 18 段、67,126,416 字节，哈希与正式夹具逐项相同。另用包内副本在断源条件下从种子启动，读回 `capacity:marker` 的旧值 `fixed-link-snapshot-capacity-marker-4`，新键返回 404，磁盘序号仍为 4。三个进程均以 0 退出，三个监听端口随后释放。此次检查没有运行计时安装窗。操作与原始记录在镜像的 `reviews/check-replay-fixture.py`、`reviews/replay-ready/`。调度中断与恢复演练的 111 份原始文件也已从 ext4 逐字节回读到 `smoke/`，摘要在 `reviews/smoke-mirror-readback.json`。

第三个正式时段结束后，同一台 WSL 在空档完成 Linux 工作台打包、Redis 三主客户端验证及上述夹具读回，记录分别在 12:06、12:08、12:19（上海时间）写出。夹具验证与集群操作均已释放实验锁。补采预约从 13:45:39 开始。跨时段解释耗时时保留宿主负载和缓存状态变化的可能性。

在 WSL 的 Bash 中可用以下命令按相同输入重放一个八窗块。将 `mirror` 指向交付的镜像。实验机使用登记的独占锁，输出与解包目录均在本机 ext4。换宿主时将 `--host-disk` 指向存放虚拟磁盘的实际挂载盘。

~~~bash
set -euo pipefail
mirror='/mnt/e/project/go/Kerniva Engine/logs/golog-replica-latency/kerniva-replica-latency.pIx8R4RG'
archive="$mirror/fixture/db15-linux-amd64-replay.tar.gz"
printf '%s  %s\n' 96e4f126625851b1a2b5361ce769996b4c57438b39a9d5a6bbae3bd951cfecd0 "$archive" | sha256sum -c -
bundle=$(mktemp -d "$HOME/kerniva-replay-inputs.XXXXXXXX")
out=$(mktemp -d "$HOME/kerniva-replay-result.XXXXXXXX")
tar -xzf "$archive" -C "$bundle"
(cd "$bundle" && sha256sum --quiet -c sha256.txt)
lock=/home/lemon/kerniva-development-20261124/logs/golog-followup-bSuIKDwd/.cpu-exclusive.lock
flock -n "$lock" python3 -B "$bundle/scripts/golog_replica_direct_delta.py" \
  --root "$out" --origin "$bundle/origin" --prepared "$bundle/prepared" \
  --bin "$bundle/bin" --baseline "$bundle/fine/kerniva-replica" --candidate "$bundle/fine/kerniva-replica" \
  --baseline-mode fine --candidate-mode fine --order ABBA-BAAB --observe --sample-interval-ms 50 --host-disk /mnt/f
~~~

### 在原 WSL 复核或重跑故障窗口

下面的 Bash 命令按冻结参数读取两次早段故障实验的结果。它会核对 DB 15 截断时的旧值、重启读数和文件摘要，以及一主两从的磁盘游标、跨库标记和进程退出记录。两个参数文件也给出了原始驱动和夹具的绝对路径。

~~~bash
set -eu
run=/home/lemon/kerniva-replica-latency.pIx8R4RG
python3 -B - "$run" <<'PY'
import json, subprocess, sys
from pathlib import Path

run = Path(sys.argv[1])
for kind in ("db15", "snapshot"):
    block = f"baseline-reliability-{kind}-01"
    spec = json.loads((run / "control" / f"{block}.json").read_bytes())
    result = run / "blocks" / block / "attempt-001" / "result"
    subprocess.run([*spec["audit"], str(result)], cwd=spec["cwd"], check=True)
PY
~~~

要再次注入故障，在保存了这些输入的 WSL 主机上运行下一段命令。它沿用上述 `run` 变量和正式实验的独占锁，为每次运行新建 ext4 结果目录。命令先读取每个块的冻结启动参数，故障运行成功后立即用对应驱动审查输出。结果目录会打印到终端，供两次运行对照。

~~~bash
lock=/home/lemon/kerniva-development-20261124/logs/golog-followup-bSuIKDwd/.cpu-exclusive.lock
flock -n "$lock" python3 -B - "$run" <<'PY'
import json, subprocess, sys, tempfile
from pathlib import Path

run = Path(sys.argv[1])
for kind in ("db15", "snapshot"):
    block = f"baseline-reliability-{kind}-01"
    spec = json.loads((run / "control" / f"{block}.json").read_bytes())
    result = tempfile.mkdtemp(prefix=f"kerniva-recovery-{kind}-", dir=run.parent)
    print(result, flush=True)
    subprocess.run([*spec["command"], "--root", result], cwd=spec["cwd"], check=True)
    subprocess.run([*spec["audit"], result], cwd=spec["cwd"], check=True)
PY
~~~

### 从账本和逐窗记录复算

要从 Windows 镜像重建四个时段的逐窗 JSON，在项目根目录的 PowerShell 7 中运行以下命令。三个原定时段由 `--schedule` 读取，校准、第四时段与历史参照分别指定。命令从保存的第四时段账本创建隔离副本；证据仍保存在原镜像中。

~~~powershell
$mirror = (Resolve-Path 'logs/golog-replica-latency/kerniva-replica-latency.pIx8R4RG').Path
$copy = Join-Path ([IO.Path]::GetTempPath()) ("kerniva-four-periods-" + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $copy | Out-Null
foreach ($name in 'blocks','control','run.json') {
    Copy-Item -LiteralPath (Join-Path $mirror $name) -Destination $copy -Recurse
}
Copy-Item -LiteralPath (Join-Path $mirror 'reviews/fourth-ledger.json') -Destination (Join-Path $copy 'ledger.json')
$params = @('-B','scripts/diagnostics/golog_install_latency/summarize.py',
    '--fixture',(Join-Path $mirror 'fixture/result.json'),'--schedule',$copy)
$groups = [ordered]@{
    'calibration-aa-01' = 'repeatability'
    'calibration-coarse-fine-01' = 'calibration'
    'calibration-ordinary-coarse-01' = 'external'
    'calibration-ordinary-off-01' = 'external'
    'supplement-b1' = 'formal'
    'supplement-b2' = 'formal'
}
foreach ($id in $groups.Keys) {
    $period = if ($id.StartsWith('supplement-')) { 'supplement' } else { 'setup' }
    $params += @('--block',$period,$id,$groups[$id],(Join-Path $copy "blocks/$id/attempt-001/result"))
}
foreach ($id in 'direct-1','direct-2') {
    $params += @('--block','historic',$id,'reference',(Join-Path $mirror "history/$id"))
}
$out = Join-Path $copy 'four-periods.json'
python @params --output $out
if ($LASTEXITCODE) { throw '逐窗复算未通过' }
Get-FileHash -LiteralPath $out
~~~

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

以下命令用本轮冻结的驱动重放粗、细探针校准块。两臂运行同一个二进制。另两组外部对照使用 `/home/lemon/kerniva-reader.SQXPRb7E/candidate-B` 作为普通臂，分别对比诊断构建的 `coarse` 和 `off` 模式。各组的真实执行顺序和完整参数在对应的 `control/calibration-*.json`。

~~~bash
lock=/home/lemon/kerniva-development-20261124/logs/golog-followup-bSuIKDwd/.cpu-exclusive.lock
out=$(mktemp -d "$HOME/kerniva-install-replay.XXXXXXXX")
diag="$run/build/fine/bin/kerniva-replica"
flock -n "$lock" python3 -B "$run/build/probe-overlay/scripts/golog_replica_direct_delta.py" \
  --root "$out" --origin /home/lemon/kerniva-memory-compare.AwbOfm/ordinary-2-candidate-1-cap0 \
  --prepared "$frozen" --bin /home/lemon/kerniva-memory-phases.apFiuiuT/src/bin \
  --baseline "$diag" --candidate "$diag" --baseline-mode coarse --candidate-mode fine \
  --observe --sample-interval-ms 50 --host-disk /mnt/f --order ABBA-BAAB
~~~
