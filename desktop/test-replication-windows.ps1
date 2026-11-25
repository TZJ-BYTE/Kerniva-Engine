#requires -Version 7.0
param(
    [Parameter(Mandatory)][string]$App,
    [Parameter(Mandatory)][string]$Distro,
    [Parameter(Mandatory)][string]$LinuxUser,
    [Parameter(Mandatory)][string]$LinuxBin,
    [Parameter(Mandatory)][string]$PerformanceLock,
    [string]$Evidence = (Join-Path ([IO.Path]::GetTempPath()) ('kerniva-native-replica-' + [guid]::NewGuid().ToString('N')))
)
$ErrorActionPreference = 'Stop'
$App = (Resolve-Path -LiteralPath $App).Path
$repo = Split-Path $PSScriptRoot
if (Test-Path -LiteralPath $Evidence) { throw "Choose a new evidence directory: $Evidence" }
$Evidence = (New-Item -ItemType Directory -Path $Evidence).FullName
. (Join-Path $PSScriptRoot 'windows-ui-helpers.ps1')
function Select-Database($number) {
    $item = Find-Control ([Windows.Automation.AutomationElement]::AutomationIdProperty) 'replication-db' 'ComboBox'
    $item.SetFocus()
    $pt = $item.GetClickablePoint()
    $pixel = [WorkbenchMouse+Point]::new()
    $pixel.X = [int]$pt.X; $pixel.Y = [int]$pt.Y
    $hwnd = [WorkbenchMouse]::WindowFromPoint($pixel)
    if ([WorkbenchMouse]::GetAncestor($hwnd, 2) -ne $Desktop.MainWindowHandle) { throw 'Database selector is occluded' }
    $key = if ($number -eq 1) { 0x23 } else { 0x24 } # End / Home on the focused two-item select
    if (![WorkbenchMouse]::PostMessage($hwnd, 0x0100, [IntPtr]$key, [IntPtr]1) -or
        ![WorkbenchMouse]::PostMessage($hwnd, 0x0101, [IntPtr]$key, [IntPtr]0xC0000001L)) { throw 'Database selection failed' }
    [void](Find-Control ([Windows.Automation.AutomationElement]::NameProperty) "DB $number · 标记读取对照" 'Text')
}
function Snapshot($name, [scriptblock]$matches) {
    $deadline = [datetime]::UtcNow.AddSeconds(24)
    do {
        $state = Invoke-RestMethod ($origin + 'api/replication/status') -TimeoutSec 12
        if (& $matches $state) {
            $state | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $Evidence "$name.json") -Encoding utf8
            return $state
        }
        Start-Sleep -Milliseconds 220
    } while ([datetime]::UtcNow -lt $deadline)
    throw "Topology did not reach $name"
}
function Cursor($label) {
    $copy = Join-Path $Evidence "$label-CURRENT.json"
    Copy-Item -LiteralPath (Join-Path $demoDir 'replica-b/follower-v1/db_1/CURRENT') -Destination $copy
    return (Get-Content -Raw -LiteralPath $copy | ConvertFrom-Json)
}

$lockProc = $manager = $Desktop = $null
$report = [ordered]@{ appSHA256 = (Get-FileHash -LiteralPath $App).Hash; started = (Get-Date -Format o) }
try {
    # The WSL process owns the lock until this test closes its input.
    $cmd = @('-d', $Distro, '-u', $LinuxUser, '--exec', '/usr/bin/flock', '-n', $PerformanceLock,
             '/bin/sh', '-c', 'echo acquired; cat >/dev/null')
    $lockProc = Start-TestProcess (Get-Command wsl.exe).Source $cmd -Pipes
    $handshake = $lockProc.StandardOutput.ReadLineAsync()
    if (!$handshake.Wait(7000) -or $handshake.Result -ne 'acquired') { throw 'The WSL performance lock is busy or unavailable' }
    $settings = Join-Path $Evidence 'demo-workbench.json'
    $cmd = @('-B','-u',(Join-Path $repo 'scripts/kerniva-workbench.py'),'--port','0','--config',$settings,
             '--with-demo','--wsl-distro',$Distro,'--wsl-user',$LinuxUser,'--bin-dir',$LinuxBin,'--parent-stdin')
    $manager = Start-TestProcess (Get-Command python).Source $cmd -Pipes
    $ready = $manager.StandardOutput.ReadLineAsync()
    if (!$ready.Wait(45000) -or $ready.Result -notmatch '^Open (http://127\.0\.0\.1:\d+/)$') { throw 'No managed workbench origin' }
    $origin = $matches[1]
    $config = Invoke-RestMethod ($origin + 'api/state') -TimeoutSec 5
    if ($config.replicationLabel -notmatch ('^WSL ' + [regex]::Escape($Distro) + ':(/tmp/kerniva-workbench-demo-[\w-]+)$')) { throw 'Expected a newly owned WSL demo directory' }
    $demoDir = Join-Path "\\wsl.localhost\$Distro" $matches[1].TrimStart('/').Replace('/','\')
    $report.managedDir = $config.replicationLabel
    $report.workbench = $origin
    $old = 'DB1 initial'
    $marker = 'native-' + [guid]::NewGuid().ToString('N')
    $initial = Snapshot 'initial' { param($s) $s.replicas.b.dbs.'1'.value -eq $old -and $s.replicas.a.dbs.'1'.value -eq $old }
    $initialCursor = Cursor 'initial'
    Open-Desktop @('--url', $origin)
    Invoke-Name '复制控制台' 'Hyperlink'
    Select-Database 1
    [void](Find-Control ([Windows.Automation.AutomationElement]::NameProperty) $old 'Text')
    Click-WindowButton 'control-b'
    [void](Snapshot 'paused' { param($s) $s.replicas.b.paused -and !$s.replicas.b.running })
    Set-ControlValue '标记值' 'Edit' $marker
    Click-WindowButton 'send-marker'
    $ahead = Snapshot 'a-advanced' { param($s) $s.primary.dbs.'1'.value -eq $marker -and $s.replicas.a.dbs.'1'.value -eq $marker -and $s.replicas.b.paused }
    [void](Find-Control ([Windows.Automation.AutomationElement]::NameProperty) '已暂停 · 上次读取' 'Text')
    [void](Find-Control ([Windows.Automation.AutomationElement]::NameProperty) $old 'Text')
    $stillOld = Cursor 'paused-after-marker'
    if ((Get-FileHash -LiteralPath (Join-Path $Evidence 'initial-CURRENT.json')).Hash -ne
        (Get-FileHash -LiteralPath (Join-Path $Evidence 'paused-after-marker-CURRENT.json')).Hash) { throw 'B cursor changed while paused' }
    Click-WindowButton 'control-b'
    $caught = Snapshot 'caught-up' { param($s) $s.replicas.b.dbs.'1'.value -eq $marker -and $s.replicas.b.dbs.'1'.caught_up }
    $published = Cursor 'caught-up'
    if ($published.source -ne $initialCursor.source -or $published.sequence -ne $caught.replicas.a.dbs.'1'.sequence -or
        $published.sequence -le $stillOld.sequence) { throw 'B did not durably catch A' }
    Select-Database 0
    [void](Find-Control ([Windows.Automation.AutomationElement]::NameProperty) 'DB0 initial' 'Text')
    Close-Desktop
    [void](Snapshot 'after-window-close' { param($s) $s.replicas.b.dbs.'1'.value -eq $marker -and $s.replicas.a.dbs.'0'.value -eq 'DB0 initial' })
    $report.marker = $marker
    $report.bBefore = $initialCursor.sequence
    $report.bPaused = $stillOld.sequence
    $report.aAfter = $ahead.replicas.a.dbs.'1'.sequence
    $report.bAfter = $published.sequence
    $report.source = $published.source
    $report.passed = $true
} catch {
    $report.passed = $false
    $report.error = $_.Exception.Message
    throw
} finally {
    try {
        if ($Desktop -and !$Desktop.HasExited) {
            if (!$Desktop.CloseMainWindow() -or !$Desktop.WaitForExit(8000)) { $Desktop.Kill(); $Desktop.WaitForExit() }
        }
        if ($manager) {
            $manager.StandardInput.Close()
            if (!$manager.WaitForExit(30000)) {
                $manager.Kill()
                $manager.WaitForExit()
                throw 'Managed controller did not shut down gracefully'
            }
            $report.controllerExit = $manager.ExitCode
            if ($manager.ExitCode -ne 0) { throw "Managed controller exited $($manager.ExitCode)" }
        }
    } catch {
        $report.passed = $false
        $report.cleanupError = $_.Exception.Message
        throw
    } finally {
        if ($lockProc) {
            $lockProc.StandardInput.Close()
            if (!$lockProc.WaitForExit(5000)) { $lockProc.Kill(); $lockProc.WaitForExit() }
            $report.lockExit = $lockProc.ExitCode
        }
        $report.finished = Get-Date -Format o
        $report | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $Evidence 'result.json') -Encoding utf8
        Write-Output $Evidence
    }
}
