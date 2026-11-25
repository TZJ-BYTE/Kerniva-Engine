#requires -Version 7.0
param(
    [Parameter(Mandatory)][string]$App,
    [string]$Server = (Join-Path $PSScriptRoot '../bin/kerniva-server.exe'),
    [string]$Evidence = (Join-Path ([IO.Path]::GetTempPath()) ('kerniva-desktop-' + [guid]::NewGuid().ToString('N')))
)
$ErrorActionPreference = 'Stop'
$App = (Resolve-Path -LiteralPath $App).Path
$Server = (Resolve-Path -LiteralPath $Server).Path
$Service = Join-Path (Split-Path $App) 'backend/kerniva-workbench-service.exe'
if (Test-Path -LiteralPath $Evidence) { throw "Choose a new evidence directory: $Evidence" }
$Evidence = (New-Item -ItemType Directory -Path $Evidence).FullName
. (Join-Path $PSScriptRoot 'windows-ui-helpers.ps1')

$db = $Desktop = $attached = $null
$settings = Join-Path $Evidence '工作台.json'
$report = [ordered]@{ app = $App; appSHA256 = (Get-FileHash -LiteralPath $App).Hash; started = (Get-Date -Format o) }
try {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    $listener.Start()
    $port = $listener.LocalEndpoint.Port
    $listener.Stop()
    $yaml = Join-Path $Evidence 'server.yaml'
    @"
host: "127.0.0.1"
port: $port
network_type: "std"
db_count: 16
persistence_enabled: false
log_path: "./server.log"
"@ | Set-Content -LiteralPath $yaml -Encoding utf8
    $stop = Join-Path $Evidence 'stop-server'
    $db = Start-TestProcess $Server @('-config', $yaml, '-stop-file', $stop)
    $deadline = [datetime]::UtcNow.AddSeconds(10)
    do {
        if ($db.HasExited) { throw "Kerniva server exited ($($db.ExitCode))" }
        $tcp = [Net.Sockets.TcpClient]::new()
        try { $tcp.Connect('127.0.0.1', $port); break } catch { $tcp.Dispose(); Start-Sleep -Milliseconds 100 }
    } while ([datetime]::UtcNow -lt $deadline)
    $tcp.Dispose()
    Set-TestMarker $port 0 'desktop:smoke' 'seen-in-webview2'
    Set-TestMarker $port 1 'desktop:smoke' 'seen-in-db1'

    Open-Desktop @('--config', $settings)
    [void](Find-Control ([Windows.Automation.AutomationElement]::NameProperty) '连接名称' 'Edit')
    $report.readyMs = $Clock.ElapsedMilliseconds
    Start-Sleep -Seconds 3
    $report.idleAfterMs = 3000
    $report.idleProcesses = @(Measure-DesktopProcesses)
    $report.idleProcessTreeWorkingSetSumBytes = ($report.idleProcesses | Measure-Object WorkingSetSize -Sum).Sum
    Set-ControlValue '连接名称' 'Edit' '原生窗口验收'
    Set-ControlValue '主机' 'Edit' '127.0.0.1'
    Set-ControlValue '端口' 'Spinner' $port
    Click-WindowButton 'save-profile'
    Invoke-ID 'connect-button'
    Invoke-ID 'open-browser'
    Invoke-Name 'desktop:smoke' 'Hyperlink'
    [void](Find-Control ([Windows.Automation.AutomationElement]::NameProperty) 'seen-in-webview2' 'Text')
    $dbInput = Find-Control ([Windows.Automation.AutomationElement]::AutomationIdProperty) 'session-db' 'Spinner'
    $dbInput.GetCurrentPattern([Windows.Automation.ValuePattern]::Pattern).SetValue('1')
    Click-WindowButton 'switch-db'
    [void](Find-Control ([Windows.Automation.AutomationElement]::NameProperty) '已连接「原生窗口验收」 · 当前库 DB 1' 'Text')
    Invoke-Name 'desktop:smoke' 'Hyperlink'
    [void](Find-Control ([Windows.Automation.AutomationElement]::NameProperty) 'seen-in-db1' 'Text')
    $report.sameKeyAcrossDBs = @('seen-in-webview2', 'seen-in-db1')
    Invoke-Name '运行状况' 'Hyperlink'
    [void](Find-Control ([Windows.Automation.AutomationElement]::AutomationIdProperty) 'status-refresh' 'Button')
    Invoke-Name '设置' 'Hyperlink'
    Set-ControlValue '每页条数' 'Spinner' 17
    Invoke-ID 'save-settings'
    $deadline = [datetime]::UtcNow.AddSeconds(5)
    while ((Get-Content -Raw -LiteralPath $settings | ConvertFrom-Json).settings.pageSize -ne 17) {
        if ([datetime]::UtcNow -gt $deadline) { throw 'Native setting did not persist' }
        Start-Sleep -Milliseconds 100
    }
    $report.visibleValue = 'seen-in-webview2'
    $report.savedPageSize = 17
    $profile = (Get-Content -Raw -LiteralPath $settings | ConvertFrom-Json).profiles[0]
    if ($profile.db -ne 0) { throw 'Switch changed the default DB of the saved profile' }
    $Desktop.Refresh()
    $report.windowPid = $Desktop.Id
    $report.windowWorkingSetBytes = $Desktop.WorkingSet64
    $owned = @(Get-CimInstance Win32_Process -Filter "ParentProcessId=$($Desktop.Id)" | Where-Object Name -EQ 'kerniva-workbench-service.exe')
    if ($owned.Count -ne 1) { throw "Expected one owned service, found $($owned.Count)" }
    $report.ownedPid = $owned[0].ProcessId
    Close-Desktop
    if (Get-Process -Id $report.ownedPid -ErrorAction SilentlyContinue) { throw 'Owned service survived window close' }
    $report.ownedReaped = $true

    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    $listener.Start()
    $attachPort = $listener.LocalEndpoint.Port
    $listener.Stop()
    $origin = "http://127.0.0.1:$attachPort/"
    Open-Desktop @('--url', $origin)
    [void](Find-Control ([Windows.Automation.AutomationElement]::NameProperty) '重新连接' 'Hyperlink')
    $attached = Start-TestProcess $Service @('--port', "$attachPort", '--config', $settings, '--parent-stdin') -Pipes
    $ready = $attached.StandardOutput.ReadLineAsync()
    if (!$ready.Wait(15000) -or $ready.Result -notmatch '^Open (http://127\.0\.0\.1:\d+/)$') { throw 'Attach service did not start' }
    if ($matches[1] -ne $origin) { throw 'Unexpected attachment address' }
    Invoke-Name '重新连接' 'Hyperlink'
    [void](Find-Control ([Windows.Automation.AutomationElement]::AutomationIdProperty) ('profile-' + $profile.id) 'Hyperlink')
    Close-Desktop
    $state = Invoke-RestMethod -Uri ($origin + 'api/state') -TimeoutSec 3
    if ($attached.HasExited -or $state.settings.pageSize -ne 17) { throw 'Attached service did not survive with its configuration' }
    $report.attachedAliveAfterClose = $true
    $report.reconnectedInWindow = $true
    Open-Desktop @('--config', $settings)
    [void](Find-Control ([Windows.Automation.AutomationElement]::AutomationIdProperty) ('profile-' + $profile.id) 'Hyperlink')
    $owned = @(Get-CimInstance Win32_Process -Filter "ParentProcessId=$($Desktop.Id)" | Where-Object Name -EQ 'kerniva-workbench-service.exe')
    if ($owned.Count -ne 1) { throw 'Missing service before simulated abrupt desktop exit' }
    $child = Get-Process -Id $owned[0].ProcessId
    $Desktop.Kill()
    $Desktop.WaitForExit()
    if (!$child.WaitForExit(10000)) { throw 'Owned service survived abrupt desktop exit' }
    $report.ownedReapedAfterAbruptExit = $true
    $report.passed = $true
} catch {
    $report.passed = $false
    $report.error = $_.Exception.Message
    throw
} finally {
    if ($Desktop -and !$Desktop.HasExited) {
        if (!$Desktop.CloseMainWindow() -or !$Desktop.WaitForExit(8000)) { $Desktop.Kill(); $Desktop.WaitForExit() }
    }
    if ($attached -and !$attached.HasExited) {
        $attached.StandardInput.Close()
        if (!$attached.WaitForExit(8000)) { $attached.Kill(); $attached.WaitForExit() }
    }
    if ($db -and !$db.HasExited) {
        [IO.File]::WriteAllText($stop, 'stop')
        if (!$db.WaitForExit(8000)) { $db.Kill(); $db.WaitForExit() }
    }
    $report.finished = Get-Date -Format o
    $report | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $Evidence 'result.json') -Encoding utf8
    Write-Output $Evidence
}
