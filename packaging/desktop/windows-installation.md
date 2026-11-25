# Windows 工作台安装与数据库服务

在 Windows 原生环境构建离线安装器。使用相同架构的 Python 3、Go 和 Rust，以及 NSIS 的 makensis。若使用 GNU Rust 目标，先配置对应的链接器。

~~~powershell
python -m pip install -r packaging/desktop/requirements.txt
python -B packaging/desktop/build.py --windows-installer --version 0.1.2
~~~

构建输出包括普通用户 setup.exe、工作台 ZIP、独立管理员 PowerShell 工具及各自的 SHA-256 清单。setup.exe 可选择新的程序目录，包含桌面、终端和 engine 目录。engine/manifest.json 记录匹配版本的服务端与 Rust DLL 文件大小、SHA-256 和目标架构。用工作台创建的普通用户实例使用单独的数据目录。卸载程序仅移除安装包交付文件和工作台注册项，保留数据目录以及未由安装包写入的文件。

## 在普通用户会话检查交付

~~~powershell
$engine = 'C:\path\to\Kerniva Workbench\engine'
$manifest = Get-Content -LiteralPath (Join-Path $engine 'manifest.json') -Raw | ConvertFrom-Json
$manifest | Select-Object product, version, os, arch
foreach ($entry in $manifest.files.PSObject.Properties) {
    $path = Join-Path $engine $entry.Name
    $item = Get-Item -LiteralPath $path
    if ($item.Length -ne $entry.Value.size -or (Get-FileHash -LiteralPath $path).Hash -ine $entry.Value.sha256) {
        throw "Engine artifact mismatch: $($entry.Name)"
    }
}
Get-FileHash -LiteralPath (Join-Path $engine 'manifest.json') -Algorithm SHA256
Get-Service -Name 'Kerniva-my-instance' -ErrorAction SilentlyContinue |
    Select-Object Name, Status, StartType
~~~

## 可选的 Windows 系统服务

系统服务由管理员主动注册，普通用户安装器和工作台的同用户实例不会触发该操作。管理员从可信来源获取并核实独立的管理员脚本和目标发行版的 manifest SHA-256，再从可信的管理员控制目录执行脚本。

~~~powershell
./windows-service.ps1 -Action Install -Instance my-instance -EngineBundle $engine -ManifestSha256 $verifiedManifestHash -Port 16379
./windows-service.ps1 -Action Start -Instance my-instance
./windows-service.ps1 -Action Status -Instance my-instance
~~~

服务注册到 Kerniva-<实例名>，默认启动类型为 Manual。注册工具将校验过的服务端和 DLL 复制到 Program Files 下的独立保护目录，生成绑定 127.0.0.1 的实例配置，并为该服务身份授权数据目录。服务以 LocalService 运行，配置文件和引擎文件由管理员控制。管理员可使用 Stop、Restart、EnableAutoStart、DisableAutoStart 和 Uninstall。Uninstall 删除 SCM 注册并保留保护目录中的配置和数据。

服务端接受 SCM 参数 --windows-service、--config、--engine-library。它读取绝对路径的配置，使用 std 网络与 go-log 存储，监听成功后向 SCM 报告 Running。SCM 的停止请求触发现有持久化关闭路径。普通用户环境可验证安装、匹配引擎文件、正常启停、重启读回和卸载留存。SCM 注册、服务账户下真实启动和开机无登录启动需要在管理员隔离机验证。
