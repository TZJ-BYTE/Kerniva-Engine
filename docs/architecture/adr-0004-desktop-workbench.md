# ADR 0004 · Kerniva 桌面工作台与命令行

状态为部分采纳。记录日期为 2026-11-24。统一浏览器工作台和交互终端已交付，Wails 桌面安装包仍是拟议方案。

## 使用方式与视觉方向

演示者需要在一个窗口里看清三个读值的差别，在终端里也能独立核实结果。工作台使用暖米纸底、墨色正文和细分隔线，陶土色只用于主要操作与当前工作区。左侧负责导航，正文留给集群拓扑、按库读值、标记写入和操作记录。节点名称用正文，命令、地址和序号用等宽字体；暂停时同时显示状态文字与上次读值。显示一个真实地址或一条可复制的读命令，比模拟终端提示符更适合现场演示。

## 已采纳的统一入口

本地 Python 工作台通过一个回环地址提供连接、键浏览、运行状况、设置和复制控制台。普通连接通过 RESP 读数据和 `INFO`，集群向每个主节点读取指标。受管复制拓扑通过单独的管理接口提供探测、标记写入及副本启停。浏览器和交互终端共享配置及管理语义，每个客户端分别建立数据库会话。

Linux 和 WSL 上可以创建由工作台所有的演示实例。Windows 可原生运行工作台，通过 WSL 子进程管理 Linux 复制服务。退出工作台时结束由它创建的服务；通过 `--demo-url` 附着的实例保留原控制器的生命周期。操作记录按复制来源隔离，记录经统一管理接口确认的操作。旧浏览器的历史保留在原面板存储中。[统一工作台用法](../database_workbench.md) · [复制演示](../golog_replica_demo.md)

### 终端交互待办

后续为终端增加类似 Codex 的可点击文本界面（TUI）。用户可以用鼠标点选连接、库和操作，同时保留键盘导航与现有脚本入口。当前终端通过输入命令和编号操作。本项排在复制主线验收之后。

## 拟议桌面方案

采用 **Wails v3 + Go** 作为桌面客户端。沿用当前 HTML、CSS 和 JavaScript。Go 负责连接本地管理服务并把操作结果传回界面。Windows 使用 WebView2，macOS 使用系统 WebKit，Linux 使用 WebKitGTK。每个平台都要在对应的 WebView 上验收中文排版、键盘操作和剪贴板。桌面端采用独立 Go 模块，Wails v3.1.0 要求 Go 1.26，仓库主模块当前声明 Go 1.25。 [Wails 技术架构](https://wails.io/contributing/overview/) · [v3.1.0 发布记录](https://github.com/wailsapp/wails/releases/tag/v3.1.0) · [桌面模块工具链要求](https://raw.githubusercontent.com/wailsapp/wails/v3.1.0/v3/go.mod)

~~~mermaid
flowchart LR
    B[浏览器工作台] -->|同源 HTTP| M[本地管理服务]
    S[交互终端 kerniva shell] -->|回环 HTTP| M
    D[拟议的 Wails 桌面工作台] -->|Go 绑定| C[本地管理客户端]
    C -->|回环 HTTP| M
    M -->|RESP| P[Kerniva 或 Redis 数据库]
    M -->|托管或附着| R[Go-log 演示拓扑]
    T[kerniva-client 或 redis-cli] -->|RESP| P
~~~

第一版桌面封装可以先接入现有本地工作台服务。桌面静态页面通过 Go 绑定调用同一组状态、写标记、暂停和恢复操作，复用管理接口的字段、错误和来源检查。关闭附着的窗口时断开自己的连接，已有服务继续运行。macOS 可通过本地端口转发接入 Linux 测试实例，验证窗口和连接流程。[当前管理服务](../../scripts/kerniva-workbench.py) · [交互终端](../../scripts/workbench_shell.py) · [通用 RESP 客户端](../../cmd/client)

## 其他可行选择

| 方案 | 对 Kerniva 的取舍 |
| --- | --- |
| Tauri 2 | 系统 WebView 和外部可执行文件打包均适用。如果桌面端的设备或进程管理以后主要写在 Rust，优先重评。当前管理接口和后续控制逻辑更贴近 Go，采用 Tauri 会增加 Rust 与 Go 之间的接口维护。 [Tauri 外部程序](https://v2.tauri.app/develop/sidecar/) |
| Electron | 自带 Chromium 和 Node，适合严格要求三端同一浏览器内核运行的产品。Kerniva 当前只有少数本地控制页面，随每个安装包发行、更新一套浏览器运行时的收益有限。 [Electron 架构](https://www.electronjs.org/docs/latest/) |

## 平台边界与验收

1. **统一入口。** 验收 Windows 到 WSL 的普通数据库连接，以及同一工作台中写标记、暂停 B、A 推进、恢复 B 和切换逻辑库。浏览器和终端均应能独立核实结果。[统一工作台验收](../verification/unified-workbench.md)
2. **原生窗口。** 计划在 Windows、macOS 和 Linux 原生窗口复做上述流程，分别记录构建产物、冷启动耗时、客户端空闲 RSS 与断线重连结果。原生窗口需要保留当前的实例所有权规则。[WSL 进程管理](../../scripts/workbench_wsl.py)
3. **原生复制单独验收。** 当前 `kerniva-replica` 和复制实现带 Linux 构建条件。将一主两从打包到 Windows 或 macOS 前，每个平台须验证跨进程文件锁、打开文件时的回收语义、`CURRENT` 原子发布与异常退出恢复，并携带相应架构的 Rust 动态库。 [复制入口](../../cmd/kerniva-replica/main_linux.go) · [复制设计](adr-0003-sealed-log-replication.md) · [构建脚本](../../scripts/build.sh)
