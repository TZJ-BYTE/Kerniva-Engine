# 工作台 0.1.3 · 跨客户端常用键

同一个键名可以分别固定到两个逻辑库。桌面关闭后，从另一个连接改写值，重新打开常用键即可读到最新值。工作台保存连接 ID、库号和键名字节，各个窗口拥有独立的数据库会话。[操作说明](../database_workbench.md)

## 三分钟演示

1. 在已经运行的复制演示中，分别给 DB 0 和 DB 1 写入不同的 `demo:marker`。打开「数据浏览」，在两个库中各点一次键详情旁的「固定」。
2. 展开「常用键」，先点 DB 0，再点 DB 1。库号和读值一起变化。在另一个窗口打开 DB 0 并保持该页。
3. 关闭第一个窗口，用复制控制台更新 DB 1 的标记。打开鼠标终端，进入「键浏览 → 常用键」，点 DB 1 的标记，核对新的值。另一个窗口仍可在 DB 0 查看原值。
4. 在共用同一工作台的命令终端执行 `/pins`，将某一行的 ID 传给 `/pin-open ID`。执行 `/unpin ID` 后再用 `/get demo:marker` 核对原键仍然存在。

## 验收记录

原始操作记录和截图位于仓库本地的 `logs/workbench-pins-20261126/`。表内路径从该目录起算。

| 场景 | 实际读回 | 原始证据 |
| --- | --- | --- |
| Windows 安装后启动原生窗口 | 切到 DB 0 读到 `seen-in-webview2`，DB 1 为 `seen-in-db1`。关闭后独立更新 DB 1，再次点开读到 `updated-after-native-close`。移除 DB 1 的位置后，DB 0 保留。 | `windows-install.json`、`windows-native/result.json` |
| Windows 便携包鼠标终端 | 从包内启动脚本和控制台程序各做一轮。真实鼠标事件依次点击连接、键、固定、常用键及列表项。两轮都从 `catalog-0` 读到外部更新后的 `reopened-from-pin`，缩到 52 列后退出，控制台模式恢复，服务端的测试会话清零。 | `windows-conpty-launcher-001/`、`windows-conpty-backend-001/`，各含原始终端输出和 SHA-256 |
| Linux x64 原生窗口和鼠标终端 | GTK 窗口中固定 DB 1 的 `native:中文`，离开页面再打开，读到 `界面读值-826`。包内终端记录了 12 次鼠标点击，从列表读到 `latest-Linux-DB1`，另一会话仍读到 `current-DB0`，集合第二页为 `tail-page`。 | `linux-package-001/{window,package,terminal}.json`、`linux-mouse-002/result.json` |
| 两个独立管理进程 | 并发固定不同位置后两项均保留，并发固定同一位置得到一个 ID。连接改名后仍能按原位置读取，删除该连接清除了关联的 3 项。配置检查通过，错误会话返回 401，过时库号返回 409。 | `api-001/result.json` |
| 空键、二进制、分页和过期 | 浏览器分别打开空键、`/w==`、`/g==` 两个二进制键，列表翻页读到 `first-page`、`second-page`。键自然过期后显示不存在。直达常用键时提示可主动扫描，执行无匹配扫描后才提示该页为空。交互命令固定的位置也能在 49 列鼠标终端打开和移除。 | `package-edge-001/result.json`、[终端集成复验](../../scripts/test_workbench_tui.py) |
| Redis Cluster 与受限账号 | Redis 8.0.5 三个主节点各有一把测试键，都通过 DB 0 按槽读回。直达过程中隐式扫描次数为 0，手动扫描和越权读键都返回 `NOPERM`。错误密码被拒绝，配置文件未保存测试密码。 | `package-cluster-001/result.json` |

截图取自运行最终版管理服务的 Edge。两张图都展示 DB 1 的实时读值以及同名的跨库位置。[桌面宽度](workbench-pins-desktop.png) · [393 像素宽度](workbench-pins-393px.png)

| 本轮发布文件，位于 `dist/` | 字节 | SHA-256 |
| --- | ---: | --- |
| `Kerniva-Workbench-0.1.3-windows-amd64-setup.exe` | 24,687,168 | `77493e935ef2d7bfc44726863de2297c4ac5433ce7390b66d6de1766a999073b` |
| `Kerniva-Workbench-0.1.3-windows-amd64.zip` | 29,549,832 | `77229f511b116754a8d352f6b90aec952a867525667a1b5be87928593ae0bc6c` |
| `Kerniva-Workbench-0.1.3-linux-amd64.tar.gz` | 21,587,743 | `9ccd4e081a2c4f053b00f6a81b5b0dc20aa774c36087f03a620b00015bb36023` |
| `Kerniva-Service-0.1.3-windows-amd64.ps1` | 12,717 | `56735bece6bbdbaa4bbdf1ca9c2c5896715e95cdb275fcee25ca61465de13777` |

本轮验证的平台为 Windows x64 和 WSL Linux x64。macOS 原生桌面仍待实机验证。

Astra 独立复算四件交付物的校验值并接纳上述范围。原始审查收据为 `astra-review.json`，SHA-256 为 `40e86012fc8db3c804c2f22cff59ad14463df3457b4ebdd11a3876cd854cac60`。早期浏览器分页和 Linux 鼠标驱动各有一次失败尝试，记录在 `edge-001/`、`linux-mouse-001/`。最终包复验分别保存在 `package-edge-001/`、`linux-mouse-002/`。

## 复验命令

在仓库根目录执行。Windows 需要已构建的数据库服务端和已安装项目终端依赖的 Python 环境。真实控制台鼠标复验还使用 pywinpty。将 `$bundle` 指向解压的便携包目录，将 `$evidence` 指向新的验收目录。

~~~powershell
$python = '.\bin\workbench-tui-venv\Scripts\python.exe'
$env:PYTHONPATH = (Resolve-Path scripts).Path
& $python -B -m unittest discover -s scripts -p test_workbench_shell.py
& $python -B -m unittest -v test_workbench_tui.WorkbenchTerminalTests.test_shell_and_narrow_mouse_reopen_shared_keys
$env:KERNIVA_TUI_LAUNCHER = Join-Path $bundle 'Kerniva-TUI.cmd'
$env:KERNIVA_TUI_EVIDENCE = Join-Path $evidence 'terminal'
& $python -B -m unittest -v test_workbench_tui.WorkbenchTerminalTests.test_windows_conpty_mouse_read_and_console_restoration
& $python -B scripts/test_workbench_pins.py --server bin/kerniva-server.exe --service (Join-Path $bundle 'backend/kerniva-workbench-service.exe') --evidence (Join-Path $evidence 'shared-config')
~~~

Linux 的图形验收依赖见[桌面构建说明](native-workbench.md)。将 `bundle` 和 `server` 设为解压目录与已构建的服务端绝对路径，`out` 指向新的证据目录。

~~~bash
mkdir -p "$out"
GTK_A11Y=atspi GDK_BACKEND=x11 xvfb-run -a dbus-run-session -- \
  python3 -B packaging/desktop/verify_linux_window.py --pins --bundle-dir "$bundle" --server "$server" --report "$out/window.json"
python3 -B packaging/desktop/verify_linux_pins_terminal.py --bundle-dir "$bundle" --server "$server" --evidence "$out/mouse"
~~~
