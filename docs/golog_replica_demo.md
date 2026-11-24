# Go-log 复制演示

「复制控制台」与连接、键浏览、运行状况和设置共用一个工作台地址，默认是 `http://127.0.0.1:18782/`。它操作单独绑定的受管实例。侧栏的普通连接可以接入其他数据库。

## 从干净检出启动

在 Linux 或 WSL 2 的仓库根目录安装 README 所列 Go、Rust 和 Python 工具链，在 Linux 文件系统上构建并运行。

~~~bash
bash scripts/build.sh
bash kerniva workbench --with-demo
~~~

打开终端打印的地址，在侧栏选择「复制控制台」。`--with-demo` 默认从仓库的 `bin/` 读取同一构建的 `kerniva-server`、`kerniva-replica` 和 `libkerniva_engine.so`。用 `--bin-dir /path/to/linux/bin` 可指定其他 Linux 构建目录。工作台分配独立的数据目录和子服务端口，每次启动以拓扑显示的端点为准。`--port 0` 可自动分配工作台端口。

Windows 可在仓库根目录用 Windows Python 启动图形工作台，由 WSL 2 运行复制进程。先在所选发行版中用 `bash scripts/build.sh` 生成 Linux 构建产物，再把下面的发行版、用户与 **WSL 内的目录**换成自己的值。省略 `--wsl-distro` 和 `--wsl-user` 会使用 WSL 的默认发行版与默认用户。

~~~powershell
.\kerniva.cmd workbench --with-demo --wsl-distro Ubuntu --wsl-user your-user --bin-dir /home/your-user/Kerniva-Engine/bin
~~~

也可以在 Linux 或 WSL 中直接运行第一组命令。停止创建演示的工作台会结束它管理的四个服务。

## 附着已有实例

如果已有回环控制器，可用以下命令将其绑定到统一工作台。旧脚本仍可作为兼容控制器启动已有流程。

~~~bash
python3 -B scripts/kerniva-replica-demo.py --port 18780
~~~

另开终端启动工作台，Windows 上相应使用 `.\kerniva.cmd workbench`。

~~~bash
bash kerniva workbench --demo-url http://127.0.0.1:18780/
~~~

附着地址保存到当前工作台配置，后续启动可复用。退出附着工作台时，控制器和演示进程继续运行。要结束由旧脚本启动的前台实例，可在其原终端按 Ctrl+C。对于由旧脚本启动的后台实例，在相同 Linux 用户和工作树中执行 `python3 -B scripts/kerniva-replica-demo.py --stop --port 18780`。该停止方式依赖 Linux pidfd 和 Python 3.9 或更新版本。

## 工作台页面

路由均以工作台地址为基础，可以直接收藏所选逻辑库和实验筛选。

| 页面 | 能看到什么 |
| --- | --- |
| 实时拓扑 · `#/replication` 或 `#/replication?db=1` | 查看主库、复制源、A 与 B 的实际端点，切换 DB 0 或 DB 1，比较主库提交序号、复制源可取序号及各副本应用序号。来源相同且序号可比较时显示副本距可取序号的差值。 |
| 核验命令 · `#/replication?tab=commands&db=1` | 按所选逻辑库和本次实例的实际端口生成主库 `redis-cli` 和副本 `curl` 查询。在演示进程所在系统中执行可复核读值。 |
| 操作记录 · `#/replication?tab=activity&page=1` | 按时间倒序查看当前复制来源经统一工作台 API 确认的写入、暂停和恢复，每页五条。 |
| 实验归档 · `#/replication?tab=experiments&page=1` | 查看两组 WSL 归档实验的 16 个有效窗口。支持筛选、分页和展开核对轻库 GET 延迟上界、结果路径、SHA-256 与测量口径。 |

演示时可先写入一个标记并等待两个副本追齐，再暂停 B、写入新标记，观察 A 先推进。恢复 B 后，B 从持久化游标继续追赶。暂停期间的「上次读取」来自当前工作台页面的缓存，追赶中的「旧值」来自当前探测，但序号仍低于同源主库。切换到另一来源时，工作台清除上一个来源的读值缓存。

操作记录由统一工作台按来源 ID 持久保存，浏览器与通过该服务执行的 shell 操作可一起查看。旧独立面板的 IndexedDB 记录、直接访问旧控制器的操作及外部读写留在各自原有范围。隔离新建演示有独立来源，历史记录随原来源保留。

## 在终端复核

可在另一个终端运行 `bash kerniva shell --url http://127.0.0.1:18782/`。Windows 使用 `.\kerniva.cmd shell`，并按实际端口补充 `--url`。用 `/replica status` 查看绑定的实例，用 `/replica mark 1 新标记`、`/replica pause b` 和 `/replica resume b` 执行同一套受管操作。用 `/replica activity 1` 读取操作记录。已保存的主库 RESP 连接可以按单机模式用 `/connect 名称` 与 `/get demo:marker` 复核指定逻辑库的读值。[工作台命令与配置](database_workbench.md)

## 实测与复现

早期独立实例的 Edge 回归用 12 次实际操作覆盖三页记录，用 16 个已归档的实验覆盖四页列表。还验证了 DB 1 中 B 暂停时 A 单独推进、B 恢复后同源追齐、DB 0 标记保持可读、真实 CLI 回读与 390 像素宽的移动视图。测试命令、逐轮结果和截图保存在本机归档 `logs/golog-followup-bSuIKDwd/panel-pages-20261124/`，早期磁盘游标记录位于 `logs/golog-followup-bSuIKDwd/replica-panel-20261124/live-cursors.json`。[统一工作台验收](verification/unified-workbench.md) · [复制与实验报告](golog_grouped_replica_report.md)

持有完整本机归档时，可以用早期提取脚本重新核对旧面板的数据与原始实验结果。

~~~powershell
python -B logs/golog-followup-bSuIKDwd/panel-pages-20261124/extract-experiment-records.py --check --check-ui
~~~
