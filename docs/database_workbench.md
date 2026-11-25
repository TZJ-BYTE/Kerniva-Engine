# 数据库工作台

用桌面、浏览器或交互终端连接 Kerniva、Redis 单机和 Redis Cluster。左栏提供「连接」「数据浏览」「运行状况」「复制控制台」「设置」五个入口。保存的连接在「连接 → 已有连接」中管理。客户端共享连接配置和浏览设置，各自建立数据库会话。

## 启动

Windows x64 用户可运行离线安装器，选择工作台的安装目录。安装包和带引擎的 Windows 便携包可以在「连接 → 本机实例」部署 Kerniva 单机。[Windows 安装与系统服务](../packaging/desktop/windows-installation.md)

解压桌面便携包后，Windows 双击 `Kerniva Workbench.exe`，Linux 运行 `./kerniva-workbench`。窗口、浏览器和终端共用连接配置。桌面程序会在回环地址启动自己的管理服务，关闭窗口即释放该服务。若已有工作台，可给桌面程序传 `--url http://127.0.0.1:18782` 附着。目标数据库的本地或远程地址填在「新建连接」中。Windows 窗口需要系统 WebView2，Linux 窗口使用 GTK 4 与 WebKitGTK 6。[桌面构建与实测](verification/native-workbench.md)

从源码使用浏览器工作台或原有交互终端，需要 Python 3.9 或更新版本和标准库。在仓库根目录启动后，打开终端打印的地址，默认是 `http://127.0.0.1:18782/`。Windows 上的普通工作台由 Windows Python 原生运行。

~~~powershell
.\kerniva.cmd workbench
~~~

~~~sh
bash kerniva workbench
~~~

可指定端口与独立配置文件。

~~~sh
bash kerniva workbench --port 18800 --config ./workbench.json
~~~

面板只监听启动它的主机的回环地址。**连接表单里的主机和端口是目标数据库的地址**。例如，在 Windows 启动工作台后，可以填 WSL 虚拟机的私网地址，也可以填办公室服务器的 DNS 名称。集群的种子节点及其公布的各个主节点都需要从这台工作台主机可达。按 Ctrl+C 退出工作台；通过保存的连接访问的数据库继续运行。

## 连接与浏览

1. 在「连接」选择「新建连接」，填写名称、主机和端口。Kerniva 或普通 Redis 选「单机」并指定逻辑库。Redis Cluster 填一个种子节点，逻辑库使用 DB 0。
2. 有访问控制时填写 ACL 用户名。服务要求 TLS 时开启 TLS，使用系统信任库，私有 CA 可填工作台主机上的 CA 文件绝对路径。连接超时为 1–30 秒。
3. 已有连接的认证与连接按钮在右侧，点「编辑连接配置」可改参数。连接成功后打开「数据浏览」，在页内切换逻辑库。切换只对当前会话生效，保存的默认库和其他窗口各自独立。Redis Cluster 使用 DB 0，可在数据页选择要查看的节点。工作台会在断开连接、管理服务退出或会话闲置一小时后要求重新认证。
4. 用通配符扫描，例如 `user:*`。集群可以扫描全部主节点，也可以指定一个主节点。点击键打开详情；知道完整键名时可以直接打开，即使账号没有 `SCAN` 权限也能使用自身获准的读取命令。

| 数据 | 详情视图 |
| --- | --- |
| String | 文本、字节长度、当前分段与 TTL。完整 JSON 可格式化，数值字面量保持原样。二进制和不可显示控制字符用 Base64。 |
| Hash | 字段和值逐行显示。 |
| List、Set | 列表元素按顺序查看，集合成员通过游标分页。 |
| Sorted Set | 每行显示成员和分数。 |
| Stream | 每条记录显示 ID 和字段值。 |

扫描结果每页最多显示设定的条数。键匹配较稀疏时可以出现空页，有后续游标就能继续扫描。数据库在翻页期间有写入时，整次遍历可能重复看到键或观察到键消失。详情会标出已截断的字段，字符串可以继续向后读取。实例支持的命令和当前账号的权限共同决定能展示的内容。[Redis SCAN 语义](https://redis.io/docs/latest/commands/scan/)

## 管理本机 Kerniva

在 Windows 桌面打开「连接 → 本机实例 → 部署 Kerniva」。确认实例名称和本机端口，展开「安装和数据位置」可选择引擎安装目录与实例专属的空数据目录。例如，工作台装在 C 盘，数据可放在 F 盘。点「安装并启动」后可直接浏览数据、查看日志、重启或正常停止实例。相同版本的引擎可以共用。切库无需保存第二份连接。

普通实例使用当前 Windows 账户在后台运行。关掉工作台后仍可连接数据库。重新打开工作台能凭实际启动命令和文件位置找到原实例。若移走已有数据目录，再次启动时会显示需要恢复原位置。卸载工作台时默认保留数据。

「设置」里的「登录 Windows 时打开此工作台」控制桌面窗口。需要数据库在无人登录时随系统运行，可由管理员为它单独登记 Windows 系统服务。Linux 桌面目前支持连接已有本地和远程数据库，复制演示可使用下文的控制台。[系统服务的安装和核验](../packaging/desktop/windows-installation.md) · [部署路线图](workbench-deployment-plan.md)

## 运行状况

打开「运行状况」读取当前连接的 `INFO`。单机显示当前实例，集群按主节点分别显示地址、版本、角色、运行时间、内存、客户端连接、每秒操作量和键数。Kerniva 有上报时也展示运行环境、存储后端和提交序号。内存按服务端实际报告的口径显示，缺失指标用破折号，节点查询失败时显示错误。

## 鼠标终端

Windows 便携包双击 `Kerniva-TUI.cmd`，Linux 包运行 `./kerniva-tui`。点「连接」选择或新建数据库连接，需要认证时在密码框输入。进入「键浏览」可切换单机会话的逻辑库、选择集群节点、翻页并点开键值。切库只作用于本次会话。页面还能查看运行状态、修改工作台设置，以及操作已配置的复制控制器。窄终端会把键列表与详情上下排列。

Tab 和方向键移动焦点，Enter 选择，Esc 关闭对话框。Ctrl+R 刷新当前视图，Ctrl+Q 退出并恢复终端模式。连接名、地址与设置使用现有工作台配置；`--config` 可选独立文件，`--url` 可附着到指定工作台。便携包包含 Textual 和运行时。源码使用者可在仓库根目录安装 TUI 依赖并启动。

~~~powershell
python -m venv bin/workbench-tui-venv
.\bin\workbench-tui-venv\Scripts\python.exe -m pip install -r scripts/requirements-workbench-tui.txt
.\kerniva.cmd tui
~~~

~~~sh
python3 -m venv bin/workbench-tui-venv
bin/workbench-tui-venv/bin/python3 -m pip install -r scripts/requirements-workbench-tui.txt
bash kerniva tui
~~~

## 命令式交互终端

在仓库根目录运行 `./kerniva.cmd shell`（Windows）或 `bash kerniva shell`（Linux、WSL 或 macOS）。如果同端口已有使用同一配置文件的工作台，终端会复用其服务。其他已启动的工作台可用 `shell --url http://127.0.0.1:18800` 显式附着。工作台未运行时，终端也可直接读取本地配置并建立自己的连接。

| 命令 | 用途 |
| --- | --- |
| `/help` | 列出工作台支持的命令。 |
| `/profiles`、`/connect` | 查看已保存的连接。在交互终端执行 `/connect` 后输入编号，也可直接执行 `/connect #1` 或用名称、ID 连接。需要认证时会提示输入密码。 |
| `/add "本地 Redis" 127.0.0.1 --port 6379` | 保存新的连接配置，可用 `--db` 选库、`--mode cluster` 连接集群。 |
| `/status` | 查看当前数据库连接和实例指标。 |
| `/scan user:*`、`/next`、`/prev` | 开始扫描及翻页，可继续浏览支持分页的键值。 |
| `/get user:42`、`/get-b64 Base64` | 按完整键名读值。键名可包含空格，单独执行 `/get` 可读空键名。 |
| `/settings pageSize 25`、`/settings autoRefresh on` | 保存每页条数或切换自动刷新。单独执行 `/settings` 可查看当前配置。 |
| `/replica status`、`/replica activity 1` | 查看绑定的受管复制实例及其操作记录。 |
| `/replica mark 1 演示标记`、`/replica pause b`、`/replica resume b` | 写入演示标记，暂停或恢复演示副本。 |
| `/disconnect`、`/exit` | 断开当前连接，或结束整个终端会话。 |

普通数据库连接上的工作台命令用于只读检查，本地连接配置和设置可以编辑。复制操作作用于单独绑定的演示拓扑。需要通用 RESP 命令时使用 `kerniva client` 或 `redis-cli`。

## 设置如何生效

「设置」保存工作台偏好。所有连接使用这些显示设置，每个连接单独保存地址、认证身份、TLS、逻辑库及超时。这个划分参考了 Claude Code 区分长期偏好与会话参数的做法。[Claude Code 设置作用域](https://code.claude.com/docs/en/settings)

| 选项 | 默认值 | 可调范围或效果 |
| --- | --- | --- |
| 每页条数 | 50 | 1–200，同时作用于键列表和集合详情。 |
| 预览字节 | 4096 | 64–65536，限制字符串分段和集合字段预览。 |
| 自动刷新 | 关闭 | 开启后在键浏览页按 2–300 秒间隔刷新，初始间隔 10 秒。 |
| 格式化 JSON | 开启 | 切换完整 JSON 的缩进和原始文本显示。 |

配置文件由启动工作台的用户持有，可从界面或文本编辑器修改。Windows 默认位于 `%APPDATA%\kerniva\workbench.json`，Linux 使用 `$XDG_CONFIG_HOME/kerniva/workbench.json`，未设置 XDG 时使用 `~/.config/kerniva/workbench.json`。外部编辑之后刷新页面可加载新设置。更改连接参数后需重新连接。配置文件与数据库自身的运行参数各自独立。

## 与复制演示一起用

现有演示的主库使用 RESP，保存一次单机连接后，即可在「数据浏览」切换 DB 0 和 DB 1，查看各库的 `demo:marker`。已有连接沿用各自保存的默认库。演示的 A/B 副本使用 Kerniva 专用 HTTP 接口，副本进度、暂停和恢复在复制控制台查看。

使用 `--with-demo` 创建由工作台管理的实例，或用 `--demo-url http://127.0.0.1:18780/` 配置现有的回环复制控制器。工作台保存通过格式校验的控制器地址，后续启动可复用；控制器不可达时，复制控制台会显示读取错误。侧栏中的「复制控制台」使用同一个工作台地址。普通工作台可单独运行。[复制控制台](golog_replica_demo.md)

## 验收

Windows 原生客户端已通过私网 TCP 连接 WSL 2 中的单机和三主 Redis。实测包含缺失凭据、错误密码、成功认证、全键遍历和只准精确读键的 ACL。两种拓扑分别遍历了 163 个匹配键。

Edge 覆盖了受限账号直接查键、二进制分段、集群节点切换、分页和窄屏。Windows 和 WSL 还验证了两个并行客户端进程保存 12 个连接配置、设置重启后保留以及旧会话需重新认证。上述早期验收的机器记录和复现脚本保存在本机归档 `logs/golog-followup-bSuIKDwd/db-client-20261124/`。[统一工作台验收](verification/unified-workbench.md)
