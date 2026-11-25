#requires -Version 7.0
param(
    [Parameter(Mandatory)][string]$App,
    [string]$SourceEngine,
    [switch]$TestLoginStartup,
    [string]$Evidence = (Join-Path ([IO.Path]::GetTempPath()) ('kerniva-native-instance-' + [guid]::NewGuid().ToString('N')))
)
$ErrorActionPreference = 'Stop'
$App = (Resolve-Path -LiteralPath $App).Path
if (Test-Path -LiteralPath $Evidence) { throw "Choose a new evidence directory: $Evidence" }
$Evidence = (New-Item -ItemType Directory -Path $Evidence).FullName
. (Join-Path $PSScriptRoot 'windows-ui-helpers.ps1')

Add-Type @"
using System;
using System.Runtime.InteropServices;
public static class WorkbenchChooser {
    [DllImport("user32.dll", CharSet=CharSet.Unicode)] public static extern IntPtr FindWindow(string type, string name);
    [DllImport("user32.dll")] public static extern IntPtr GetDlgItem(IntPtr dialog, int id);
    [DllImport("user32.dll", EntryPoint="SendMessageW", CharSet=CharSet.Unicode)]
    public static extern IntPtr SendText(IntPtr hwnd, int message, IntPtr ignored, string text);
}
"@

function Wait-ForDialog {
    $deadline = [datetime]::UtcNow.AddSeconds(10)
    do {
        $hwnd = [WorkbenchChooser]::FindWindow('#32770', '选择 Kerniva 目录')
        if ($hwnd -ne 0) {
            $dialog = [Windows.Automation.AutomationElement]::FromHandle($hwnd)
            if ($dialog.Current.ProcessId -eq $Desktop.Id) { return $dialog }
        }
        Start-Sleep -Milliseconds 80
    } while ([datetime]::UtcNow -lt $deadline)
    throw 'The native folder dialog did not open in the tested app'
}
function Finish-Dialog($Dialog, [int]$ButtonID) {
    $hwnd = [WorkbenchChooser]::GetDlgItem([IntPtr]$Dialog.Current.NativeWindowHandle, $ButtonID)
    if ($hwnd -eq 0 -or ![WorkbenchMouse]::PostMessage($hwnd, 0x00F5, [IntPtr]::Zero, [IntPtr]::Zero)) {
        throw "Cannot click native dialog button $ButtonID"
    }
}
function Input-ByID($ID) {
    $inputItem = Find-Control ([Windows.Automation.AutomationElement]::AutomationIdProperty) $ID 'Edit'
    return $inputItem.GetCurrentPattern([Windows.Automation.ValuePattern]::Pattern)
}
function Wait-ForPath($ID, $Expected) {
    $deadline = [datetime]::UtcNow.AddSeconds(10)
    do {
        if ((Input-ByID $ID).Current.Value -ceq $Expected) { return }
        Start-Sleep -Milliseconds 80
    } while ([datetime]::UtcNow -lt $deadline)
    throw "$ID does not display the expected directory"
}

$startupPath = 'Software\Microsoft\Windows\CurrentVersion\Run'
$startupLabel = 'Kerniva Workbench'
$startupCommand = '"' + $App + '"'
$testingStartup = $false
function Read-Startup {
    $key = [Microsoft.Win32.Registry]::CurrentUser.OpenSubKey($startupPath)
    try { if ($key) { return $key.GetValue($startupLabel, $null) } } finally { if ($key) { $key.Dispose() } }
}
function Wait-Startup($Expected) {
    $deadline = [datetime]::UtcNow.AddSeconds(5)
    do {
        if ((Read-Startup) -ceq $Expected) { return }
        Start-Sleep -Milliseconds 90
    } while ([datetime]::UtcNow -lt $deadline)
    throw 'Windows login registration did not reach the requested state'
}

$settings = Join-Path $Evidence '独立工作台.json'
$engine = Join-Path $Evidence '引擎安装'
$data = (New-Item -ItemType Directory -Path (Join-Path $Evidence '数据目录')).FullName
$Desktop = $null
$devService = $null
$windowArgs = @('--config', $settings)
$report = [ordered]@{app=$App; appSHA256=(Get-FileHash -LiteralPath $App).Hash; started=(Get-Date -Format o)}
try {
    if ($SourceEngine) {
        $source = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '../scripts/kerniva-workbench.py')).Path
        $sourceEngineDir = (Resolve-Path -LiteralPath $SourceEngine).Path
        $devService = Start-TestProcess (Get-Command python).Source @('-u','-B',$source,'--engine-bundle',$sourceEngineDir,'--config',$settings,'--port','0','--parent-stdin') -Pipes
        $line = $devService.StandardOutput.ReadLineAsync()
        if (!$line.Wait(10000) -or $line.Result -notmatch '^Open (http://127\.0\.0\.1:\d+/)$') { throw 'Fixture workbench did not start' }
        $windowArgs = @('--url', $Matches[1])
        $report.sourceEngine = $sourceEngineDir
    }
    Open-Desktop $windowArgs
    Invoke-ID 'instances-tab'
    Invoke-Name '部署 Kerniva' 'Button'
    Set-ControlValue '实例名称' 'Edit' '原生临时实例'
    $holder = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0)
    $holder.Start()
    $port = $holder.LocalEndpoint.Port
    $holder.Stop()
    Set-ControlValue '本机端口' 'Spinner' $port
    $paths = Find-Control ([Windows.Automation.AutomationElement]::NameProperty) '安装和数据位置' $null
    $paths.GetCurrentPattern([Windows.Automation.ExpandCollapsePattern]::Pattern).Expand()
    $original = (Input-ByID 'instance-engine-root').Current.Value
    Click-WindowButton 'choose-engine-root'
    $dialog = Wait-ForDialog
    Finish-Dialog $dialog 2
    Wait-ForPath 'instance-engine-root' $original
    $report.cancelPreserved = $true
    (Input-ByID 'instance-engine-root').SetValue($engine)
    Click-WindowButton 'choose-data-dir'
    $dialog = Wait-ForDialog
    $scope = [Windows.Automation.TreeScope]::Descendants
    $deadline = [datetime]::UtcNow.AddSeconds(6)
    do {
        $destination = $dialog.FindFirst($scope,[Windows.Automation.PropertyCondition]::new(
            [Windows.Automation.AutomationElement]::AutomationIdProperty, '1152'))
        if ($destination) { break }
        Start-Sleep -Milliseconds 120
    } while ([datetime]::UtcNow -lt $deadline)
    if (!$destination) {
        $dialog.FindAll($scope,[Windows.Automation.Condition]::TrueCondition) |
            ForEach-Object { "$($_.Current.ControlType.ProgrammaticName) $($_.Current.AutomationId) $($_.Current.ClassName)" } |
            Set-Content -LiteralPath (Join-Path $Evidence 'chooser-controls.txt') -Encoding utf8
        throw 'Native folder name field was not found'
    }
    $inputWindow = [IntPtr]$destination.Current.NativeWindowHandle
    if ($destination.Current.ClassName -ne 'Edit' -or $inputWindow -eq 0 -or
        [WorkbenchChooser]::SendText($inputWindow, 0x000C, [IntPtr]::Zero, $data) -eq 0) {
        throw 'Cannot populate the native dialog folder field'
    }
    Finish-Dialog $dialog 1
    Wait-ForPath 'instance-data-dir' $data
    $report.chosenDataDirectory = $data
    Click-WindowButton 'deploy-instance'
    Invoke-Name '浏览数据' 'Button'
    [void](Find-Control ([Windows.Automation.AutomationElement]::NameProperty) '已连接「原生临时实例」 · 当前库 DB 0' 'Text')
    Set-TestMarker $port 0 'workbench:native' 'value-from-zero'
    Set-TestMarker $port 1 'workbench:native' 'value-from-one'
    Click-WindowButton 'refresh'
    Invoke-Name 'workbench:native' 'Hyperlink'
    [void](Find-Control ([Windows.Automation.AutomationElement]::NameProperty) 'value-from-zero' 'Text')
    $dbInput = Find-Control ([Windows.Automation.AutomationElement]::AutomationIdProperty) 'session-db' 'Spinner'
    $dbInput.GetCurrentPattern([Windows.Automation.ValuePattern]::Pattern).SetValue('1')
    Click-WindowButton 'switch-db'
    [void](Find-Control ([Windows.Automation.AutomationElement]::NameProperty) '已连接「原生临时实例」 · 当前库 DB 1' 'Text')
    Invoke-Name 'workbench:native' 'Hyperlink'
    [void](Find-Control ([Windows.Automation.AutomationElement]::NameProperty) 'value-from-one' 'Text')
    $report.nativeValues = @('value-from-zero','value-from-one')
    $profile = (Get-Content -Raw -LiteralPath $settings | ConvertFrom-Json).profiles
    if (@($profile).Count -ne 1 -or $profile[0].db -ne 0 -or $profile[0].port -ne $port) {
        throw 'Unexpected persisted instance connection'
    }
    $report.profile = $profile[0] | Select-Object name,host,port,db,mode
    Close-Desktop
    Set-TestMarker $port 1 'workbench:after-close' 'instance-survived'
    $report.aliveAfterWindowClosed = $true
    Open-Desktop $windowArgs
    Invoke-ID 'instances-tab'
    Invoke-Name '停止' 'Button'
    [void](Find-Control ([Windows.Automation.AutomationElement]::NameProperty) '启动' 'Button')
    $probe = [Net.Sockets.TcpClient]::new()
    try {
        try { $probe.Connect('127.0.0.1',$port) } catch [Net.Sockets.SocketException] {}
        if ($probe.Connected) { throw 'Owned database port remained open' }
    } finally { $probe.Dispose() }
    $report.closedFromReopenedWorkbench = $true
    if ($TestLoginStartup) {
        if ($SourceEngine) { throw 'Login startup requires a packaged desktop' }
        if ($null -ne (Read-Startup)) { throw 'A login setting already exists; leaving it untouched' }
        Invoke-Name '设置' 'Hyperlink'
        $startup = Find-Control ([Windows.Automation.AutomationElement]::NameProperty) '登录 Windows 时打开此工作台' 'CheckBox'
        $toggle = $startup.GetCurrentPattern([Windows.Automation.TogglePattern]::Pattern)
        if ($toggle.Current.ToggleState -ne 'Off') { throw 'Unexpected starting state for login checkbox' }
        $testingStartup = $true
        $toggle.Toggle()
        Wait-Startup $startupCommand
        $startup = Find-Control ([Windows.Automation.AutomationElement]::NameProperty) '登录 Windows 时打开此工作台' 'CheckBox'
        $startup.GetCurrentPattern([Windows.Automation.TogglePattern]::Pattern).Toggle()
        Wait-Startup $null
        $report.loginStartupInitiallyAbsent = $true
        $report.loginStartupEnabledCommand = $startupCommand
        $report.loginStartupRestored = $true
    }
    Close-Desktop
    $report.passed = $true
} catch {
    $report.passed = $false
    $report.error = $_.Exception.Message
    throw
} finally {
    if ($testingStartup -and (Read-Startup) -ceq $startupCommand) {
        $key = [Microsoft.Win32.Registry]::CurrentUser.OpenSubKey($startupPath, $true)
        try { if ($key.GetValue($startupLabel) -ceq $startupCommand) { $key.DeleteValue($startupLabel, $false) } }
        finally { $key.Dispose() }
    }
    if ($Desktop -and !$Desktop.HasExited) {
        $hwnd = [WorkbenchChooser]::FindWindow('#32770', '选择 Kerniva 目录')
        if ($hwnd -ne 0) {
            $modal = [Windows.Automation.AutomationElement]::FromHandle($hwnd)
            if ($modal.Current.ProcessId -eq $Desktop.Id) { Finish-Dialog $modal 2 }
        }
        if (!$Desktop.CloseMainWindow() -or !$Desktop.WaitForExit(8000)) { $Desktop.Kill(); $Desktop.WaitForExit() }
    }
    $control = Join-Path ($settings + '.instances') 'control'
    if (Test-Path -LiteralPath $control) {
        foreach ($record in Get-ChildItem -LiteralPath $control -Directory) {
            [IO.File]::WriteAllText((Join-Path $record.FullName 'stop'), 'stop')
        }
    }
    if ($devService -and !$devService.HasExited) {
        $devService.StandardInput.Close()
        if (!$devService.WaitForExit(10000)) { $devService.Kill(); $devService.WaitForExit() }
    }
    $report.finished = Get-Date -Format o
    $report | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $Evidence 'result.json') -Encoding utf8
    Write-Output $Evidence
}
