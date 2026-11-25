# 原生桌面工作台与鼠标终端

桌面窗口和终端连接同一个工作台时，共用保存的连接、设置与复制控制器，各自拥有数据库会话。解压一个包即可使用两种入口。包内带管理服务、页面、Textual 和 Python 运行时。数据库可以在本机或远端运行。[使用说明](../database_workbench.md) · [架构决策](../architecture/adr-0004-desktop-workbench.md)

| 平台 | 桌面入口 | 鼠标终端入口 |
| --- | --- | --- |
| Windows x64 | `Kerniva Workbench.exe` | `Kerniva-TUI.cmd` |
| Linux x64 | `./kerniva-workbench` | `./kerniva-tui` |
| macOS 原生构建 | `Kerniva Workbench.app` | 应用目录内的 `Contents/MacOS/kerniva-tui` |

Windows 使用系统 WebView2，Linux 使用 GTK 4 和 WebKitGTK 6。解压时保留整个目录结构，两个入口旁边的 `backend` 目录包含管理服务。macOS 将服务放在应用包的 `Contents/Resources`。桌面默认自行启动管理服务，也支持 `--url http://127.0.0.1:18782` 附着到另一个工作台。连接远程数据库时，在界面的连接表单填数据库地址。[WebView2 运行时](https://learn.microsoft.com/en-us/microsoft-edge/webview2/concepts/distribution)

## 0.1.2 交付与验收

2026-11-25，Windows x64 完成离线安装、自选引擎和数据目录、数据库后台运行以及卸载留存验收。Linux x64 完成桌面和终端客户端验收。工作台的数据浏览、运行状况与复制演示各有独立入口。

| 发行文件（仓库 `dist/`） | 字节 | SHA-256 |
| --- | ---: | --- |
| `Kerniva-Workbench-0.1.2-windows-amd64-setup.exe` | 24,673,077 | `00c678ab81c77a22f15137e5ed30bbb815782b6441b8a45269e0bd195991b768` |
| `Kerniva-Workbench-0.1.2-windows-amd64.zip` | 29,539,073 | `ffe676529925e670c730ce63a214c93b9f9a986d4bc7190a53ab391b450c7041` |
| `Kerniva-Workbench-0.1.2-linux-amd64.tar.gz` | 21,576,828 | `804e358169fd30a040f42e114b59100f38749dace04dfd9af373bf77d2977347` |
| `Kerniva-Service-0.1.2-windows-amd64.ps1` | 12,717 | `56735bece6bbdbaa4bbdf1ca9c2c5896715e95cdb275fcee25ca61465de13777` |

| 操作 | 观察到的结果 | 证据（相对仓库 `logs/`） |
| --- | --- | --- |
| 在数据页切库、返回键详情、逐页扫描 | DB 0 和 DB 1 的同名键 `shared` 分别读回 `zero-only-value`、`one-only-value`。另一个会话仍读 DB 0，连接的默认库为 0。切换到 DB 99 被拒后仍在 DB 1。每页 1 条时 9 页共 9 个不同键。12 条界面流程包含 393px 窄屏。 | `workbench-native-dev/navigation-12064634/result.json` 与同目录三张截图 |
| 用 Windows 包连接 Redis 8.0.5 三主集群 | 错误 ACL 密码被拒。正确密码提交后输入框清空，配置未保存密码。界面固定 DB 0，跨节点扫描 5 页。三个槽段各自读回指定键，三个测试节点均以 0 退出。 | `workbench-native-dev/cluster-012-12083030/result.json` |
| 在真实 Windows 桌面选择中文目录、部署实例 | 取消选择保留输入。选择目录并部署后，两库各自读回预设值。关闭工作台后实例仍可写，重新打开可停止它。设置页勾选登录启动后读出当前桌面 EXE 路径，取消后注册表恢复。 | `workbench-native-dev/instances-login-012-12095630/result.json` |
| 两个工作台同时部署同一实例、断开全部工作台，再次启动 | 两个请求只建立一个实例。重启后 DB 1 原值可读。占用同一端口的另一数据库保留原进程并返回 `external-owner`。移走受管数据目录后启动被拒，历史实例登记迁移后仍保留。 | `workbench-deployment/acceptance-0.1.2/instances.json` |
| 安装与卸载 | 普通用户可选含中文的安装位置，首次启动与重启后均读到数据。卸载保留数据和用户文件。移动的卸载器、安装标记不匹配或后端被占用时提前退出，解除占用后可重试。 | 同一验收目录的 `installer-smoke.json`、`uninstaller-safety.json`、`bundle-readback.json` |
| 原生窗口和鼠标终端 | 本次 Windows 单次表单就绪为 864 ms。两个包内终端入口均在 ConPTY 中通过鼠标点击、读值、调整尺寸与退出后模式恢复。Linux GTK 窗口单次 1.410 秒就绪，11 步操作在 DB 1 读到 `界面读值-826`，关窗回收所属管理服务。Linux 包内终端启用鼠标模式并正常退出。 | `workbench-native-dev/standalone-012-12080173/result.json`、Windows 验收目录的 `tui-backend.txt` 和 `tui-launcher.txt`、`workbench-deployment/linux-0.1.2/{verification,window,tui}.json` |

Windows 与 Linux 的打包后端均在独立数据库上复验了中文键、二进制值与哈希，两份 `verification.json` 记录相同页面资源摘要。macOS 两种架构的前端通过交叉编译与 Mach-O 可执行头检查，见 Windows 验收目录的 `macos-cross-compile.json`。macOS 原生窗口、Windows 系统服务在服务账户下的启动和无人登录重启仍待真机验收。普通用户实例的关窗、重启与桌面登录项各自有单独的操作记录。

从项目根目录可用 PowerShell 复验解包后的 Windows 版本。将 `$bundle` 和 `$edge` 改为本机的实际位置，`--evidence` 选择一个新目录。

~~~powershell
$bundle = 'C:\unpacked\Kerniva Workbench'
$edge = 'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe'
$server = Join-Path $bundle 'engine/kerniva-server.exe'
$service = Join-Path $bundle 'backend/kerniva-workbench-service.exe'
python -B packaging/desktop/verify_instances.py --bundle $bundle --report instances.json
python -B scripts/test_workbench_navigation.py --server $server --service $service --browser $edge --evidence navigation-check
./desktop/test-instances-windows.ps1 -App (Join-Path $bundle 'Kerniva Workbench.exe')
~~~

集群复验使用同一导航脚本，把 `--server $server` 替换为 `--cluster --distro <发行版> --user <WSL用户> --lock <WSL内绝对锁路径>`，使用另一新证据目录。脚本在独占实验锁下启动临时三主并在退出时收回进程。

## 0.1.0 Windows 实测

2026-11-25 从含空格和中文的独立目录解压 Windows x64 包。文件 `Kerniva-Workbench-0.1.0-windows-amd64.zip` 为 20,727,083 字节，SHA-256 为 `bce555cb28ceedc9f7075edde7800bae08490fbff8d6ce71cb9d6db5b080c9b8`。启动包内服务时，PATH 只含系统目录。用隔离的真实 Kerniva 连接 DB 1，扫描中文键、读取含 NUL 和非法 UTF-8 字节的字符串及哈希值，切到 DB 0 验证隔离。关闭两次服务的父进程管道，两次均正常退出，重新打开后仍能读取保存的连接。

实际 WebView2 窗口通过 Windows UI Automation 新建连接，从键浏览页面读回 `seen-in-webview2`，查看 INFO 并把每页条数改为 17。最终包该次启动到界面就绪为 **1.136 秒**，桌面主进程工作集快照为 **78,434,304 字节**，统计范围为该进程。附着目标暂时离线时，窗口显示重连操作。目标恢复后点击即可载入。主动关闭窗口及直接终止窗口进程，均观察到其自有管理服务退出。关闭附着窗口后，原工作台仍可响应。

Windows 原生 ConPTY 实际发送鼠标事件，点选数据库连接和键、调整窗口大小，再按 Ctrl+Q。包内控制台程序和 `Kerniva-TUI.cmd` 两个入口均读回 `catalog-0`，退出后控制台输入输出模式恢复至启动前的值。终端的 API 集成测试另验证了失败切库后继续读原库、49 列窄屏和前后翻页。

Windows 鼠标终端还实际连接了 WSL 中临时启动的 Redis 8.0.5 三主集群。错误 ACL 密码被拒绝，正确密码输入时掩码显示，保存的配置没有密码。逐个点选三个主节点，各读到预设键值。跨节点扫描的三个 HTTP 响应每页各含一个键，选 DB 1 被拒后仍在 DB 0。按响应游标取证的结果在 `logs/workbench-native-20261125/tui-cluster/verified-pages.json`。早期测试曾将被按钮忽略的一次重复点击计入页数，原记录和逐次响应都保留。

机器记录保存在仓库的 `logs/workbench-desktop-20261125/windows-delivery.json`、`logs/workbench-native-dev/final-11264816/result.json` 和 `logs/workbench-native-20261125/tui-windows-final-package.txt`。Windows 原生工作台与 WSL 一主两从的浏览器和命令行回归记录在 `logs/workbench-native-20261125/native-control-eof.log`。

## 0.1.0 Linux 实测

在 WSL 2 的 Ubuntu 26.04 x86-64 原生构建 `Kerniva-Workbench-0.1.0-linux-amd64.tar.gz`。文件为 **21,553,171 字节**，SHA-256 为 `5f8d5e66c45b3fedd4a3b8fc01f0db4c043a3e136673415ec7c9610b7908b90b`。包内管理服务在空 PATH 下完成单机连接、逻辑库隔离、字符串和哈希二进制读取、配置重启与两次 EOF 退出。

在独立的 X11 显示和 D-Bus 会话内，解压后的 GTK 4 原生窗口从启动到可以点选「新建连接」为 **1.203 秒**。验收通过系统无障碍接口点选控件，并用系统键盘输入名称、主机、端口与逻辑库。Windows 和 Linux 各自建立了隔离数据库。Linux 窗口在 DB 1 实际扫描出 `native:中文`，读回 `界面读值-826`。关闭前桌面主进程 RSS 为 204,536 KiB。发送窗口关闭事件后桌面进程退出码为 0，所属管理服务退出。使用包内 `kerniva-tui` 在真实 POSIX 伪终端中进入中文界面，观测到 SGR 鼠标模式开启；Ctrl+Q 退出码为 0，终端属性恢复。

Linux 打包输入摘要、构建日志、原生窗口 11 步操作、终端模式与数据库读值分别保存在 `logs/workbench-desktop-20261125/linux/` 下的 `build-inputs.sha256`、`build.log`、`window.json`、`tui.json` 和 `verification.json`。两平台使用不同系统 WebView 和内存指标，两次单窗启动与内存快照各自说明测试环境。

## macOS 验证范围

Apple Silicon 和 Intel 前端均已交叉编译，检查 Mach-O 头确认目标分别为 arm64 和 amd64，原始摘要见 `logs/workbench-desktop-20261125/macos-cross-compile.json`。完整 `.app`、包内 Python 后端及系统 WebKit 窗口仍待 macOS 原生验收。仓库提供手动触发的双架构工作流，在 Mac runner 上原生打包、解压、校验签名，并测试数据库读值、资源、EOF 退出和终端。该工作流本次未触发。[macOS 打包工作流](../../.github/workflows/desktop-macos.yml)

## 构建与复验

在目标平台原生构建。桌面模块使用 Go 1.26 及以上；打包脚本使用 Python 3.11 及以上。在仓库工作目录运行以下命令。Windows 可将 `bin/package-venv/bin/python` 换为 `bin\package-venv\Scripts\python.exe`。

~~~sh
python3 -m venv bin/package-venv
bin/package-venv/bin/python -m pip install -r packaging/desktop/requirements.txt
bin/package-venv/bin/python packaging/desktop/build.py --output-dir dist --version 0.1.2
~~~

每个压缩包都有同名 `.sha256` 校验文件。Windows 构建带引擎的 ZIP、安装器和独立管理员工具时，加上 `--windows-installer`。在独立目录解包后，用下面的命令验证本机数据库的读值、静态资源和两次进程退出。`--server` 是供验收使用的本机 Kerniva 服务端，Linux 下相应原生库放在该程序旁边。日常连接现有数据库时可直接使用便携包。[Windows 构建依赖](../../packaging/desktop/windows-installation.md)

~~~sh
python3 -B packaging/desktop/verify.py --bundle-dir "/path/to/Kerniva Workbench" \
  --server /path/to/kerniva-server --report /tmp/kerniva-package-result.json
~~~

Windows 原生窗口复验使用 PowerShell 7，从全新证据目录运行。程序会写入隔离的数据库和工作台配置。

~~~powershell
./desktop/test-windows.ps1 -App 'C:\unpacked\Kerniva Workbench\Kerniva Workbench.exe' -Server .\bin\kerniva-server.exe
~~~

Linux 图形验收使用 Python 的 PyGObject、AT-SPI 与 python-xlib，以及 Xvfb、D-Bus 和 xdotool。下例创建单独的显示和会话。终端验收使用 Python 标准库。

~~~sh
bundle='/path/to/Kerniva Workbench'
GTK_A11Y=atspi GDK_BACKEND=x11 xvfb-run -a dbus-run-session -- \
  python3 -B packaging/desktop/verify_linux_window.py --bundle-dir "$bundle" \
  --server /path/to/kerniva-server --report /tmp/kerniva-window.json
python3 -B packaging/desktop/verify_posix_tui.py "$bundle/kerniva-tui" --report /tmp/kerniva-tui.json
~~~
