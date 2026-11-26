# 配置增量日志与全量快照

在 `config.golog.yaml` 中按需设置 `go_log_enabled` 和 `snapshot_enabled`，重启主库生效。每个开关都可以单独设置。Go 主库默认开启增量日志、关闭独立的全量快照，因此要在默认日志基础上增加快照，只需写 `snapshot_enabled: true`。下面的例子关闭日志，每 60 秒保存一次全量快照。也可以用 `KERNIVA_GO_LOG_ENABLED`、`KERNIVA_SNAPSHOT_ENABLED` 和 `KERNIVA_SNAPSHOT_INTERVAL_SECONDS` 覆盖同名 YAML 字段。

```yaml
storage_backend: go-log
persistence_enabled: true
go_log_enabled: false
snapshot_enabled: true
snapshot_interval_seconds: 60
```

| 增量日志 | 全量快照 | 写成功和重启恢复的含义 |
|---|---|---|
| 关 | 关 | 只写内存。在新数据根目录上不会创建 WAL 或快照。重启为空，期间的改动不会写回先前的持久化数据。 |
| 开 | 关 | 在 `persistence_durability: sync` 下，日志批次同步完成才确认写入。从原有恢复基线和后续日志重建。配套 Rust 维护器即使已启动，也会跳过创建新快照和回收日志。 |
| 关 | 开 | 写入先在内存中生效。主库按周期保存并在正常关闭时再保存一次。强杀后恢复最后完整发布的快照，其后确认的写入可能丢失。无需 Rust 动态库或日志目录。 |
| 开 | 开 | 每次写入沿用日志的确认规则，主库同时生成独立的内存快照。崩溃重启优先恢复日志，包含最新一次快照以后的已同步写入。 |

默认快照周期为 300 秒。`persistence_enabled: false` 关闭两个新路径。原有配置如省略两个新开关，仍使用 Go 日志，另起的 Rust 维护器仍能生成日志整理所需的快照。复制继续消费 `go-log-v1` 的封存段和 Rust `CURRENT`。只开内存快照的主库没有可供现有副本追赶的增量流。配置主库与外部维护器时应使用同一版本。

运行 `INFO` 可读取 `go_log_enabled`、`snapshot_enabled` 和 `log_maintenance_allowed`。开启独立快照时还会显示实际周期。独立快照放在 `data_dir/go-snapshot-v1`，一个文件覆盖所有逻辑库，包含数据类型和到期时间。写文件期间主库可以继续服务；在内存中捕获一致视图时需要短暂锁定数据库，锁持有时间随数据量变化。发布前同步文件并原子改名，保留最近两代。最新代校验失败会使启动报错，避免退回旧代复活已经删除的键。

## 在同一数据目录切换

- **日志转仅快照。** 先双开并正常停止一次，主库将最后接受的写入保存为完整快照并记录交接代号。下一次可只开快照。如果双开时遭遇强杀，先重新以双开模式从日志恢复并正常关闭。
- **仅快照转日志。** 新日志目录为空时，主库在开始监听前从快照为所有逻辑库建立日志基线。中断后可以重试。若目录里保留着以前的旧日志，主库会拒绝使用可能过期的历史。停止主库和维护器，将整个旧 `go-log-v1` 目录保存在数据根目录以外，保留原地的 `go-snapshot-v1` 与模式记录，再以开启日志的配置启动。重新建立日志历史后，已有复制部署需核对并重新建立源与副本的一致基线。
- **仅日志与双开之间切换。** 两者都以日志为权威来源。关闭独立快照只停止后续发布，仍需保留恢复现有日志所依赖的 Rust 快照及 `CURRENT`。

## 复验

Windows 标准 TCP、Windows gnet 和 WSL gnet 各跑四种配置，共 12 个组合。通过真实 TCP 写入、读取、强杀或正常关闭再启动。只开快照及纯内存阶段特意指向不存在的动态库。只开日志时给同版 Rust 维护器发送强制整理请求，封存日志仍在；双开对照在相同命令下确实生成了 Rust 快照。专项单测另覆盖六种数据类型、TTL、删除、损坏的最新代、多逻辑库、建立日志基线时中断以及关闭时的并发写入。

下面以构建后的 Linux 二进制复跑。Windows 使用相同参数，更换为本平台的可执行文件和动态库，并可将 `--network` 改为 `std`。每次指定一个新的空目录名，原始请求、配置、回包和 SHA-256 均可在结果中核对。[真实 TCP 复验脚本](../../scripts/golog_persistence_modes_check.py)

```bash
python3 -B scripts/golog_persistence_modes_check.py \
  --server ./bin/kerniva-server --library ./bin/libkerniva_engine.so \
  --maintainer ./bin/kerniva-maintainer --network gnet \
  --root ./logs/persistence-modes-new
```
