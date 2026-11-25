Add-Type -AssemblyName UIAutomationClient
Add-Type @'
using System.Runtime.InteropServices;
public static class WorkbenchTestWindow {
    [DllImport("user32.dll")] public static extern bool ShowWindow(System.IntPtr hwnd, int cmd);
}
'@

function Start-TestProcess($File, [string[]]$Arguments, [switch]$Pipes) {
    $info = [Diagnostics.ProcessStartInfo]::new($File)
    $info.WorkingDirectory = $Evidence
    $info.UseShellExecute = $false
    $info.CreateNoWindow = $true
    $info.RedirectStandardInput = $Pipes.IsPresent
    $info.RedirectStandardOutput = $Pipes.IsPresent
    if ($Pipes) { $info.StandardOutputEncoding = [Text.Encoding]::UTF8 }
    if ($File -eq $App) {
        $webviewProfile = Join-Path $Evidence 'native-profile'
        [void][IO.Directory]::CreateDirectory($webviewProfile)
        $info.Environment['APPDATA'] = $webviewProfile
    }
    foreach ($arg in $Arguments) { $info.ArgumentList.Add($arg) }
    return [Diagnostics.Process]::Start($info)
}

function Find-Control($Property, $Value, $Role) {
    $deadline = [datetime]::UtcNow.AddSeconds(15)
    $condition = [Windows.Automation.PropertyCondition]::new($Property, $Value)
    do {
        $items = $script:Window.FindAll([Windows.Automation.TreeScope]::Subtree, $condition)
        foreach ($element in $items) {
            if ($element.Current.IsEnabled -and (!$Role -or $element.Current.ControlType.ProgrammaticName -eq "ControlType.$Role")) {
                return $element
            }
        }
        Start-Sleep -Milliseconds 120
    } while ([datetime]::UtcNow -lt $deadline)
    $tree = $script:Window.FindAll([Windows.Automation.TreeScope]::Subtree, [Windows.Automation.Condition]::TrueCondition)
    @($tree | ForEach-Object { "$($_.Current.ControlType.ProgrammaticName) $($_.Current.AutomationId) $($_.Current.Name)" }) |
        Set-Content -LiteralPath (Join-Path $Evidence 'failed-ui-tree.txt') -Encoding utf8
    throw "Native UI control missing: $Value ($Role)"
}
function Invoke-Name($Name, $Role) {
    $item = Find-Control ([Windows.Automation.AutomationElement]::NameProperty) $Name $Role
    $item.GetCurrentPattern([Windows.Automation.InvokePattern]::Pattern).Invoke()
}
function Invoke-ID($ID) {
    $item = Find-Control ([Windows.Automation.AutomationElement]::AutomationIdProperty) $ID
    $item.GetCurrentPattern([Windows.Automation.InvokePattern]::Pattern).Invoke()
}
function Set-ControlValue($Name, $Role, $Value) {
    $item = Find-Control ([Windows.Automation.AutomationElement]::NameProperty) $Name $Role
    $item.GetCurrentPattern([Windows.Automation.ValuePattern]::Pattern).SetValue([string]$Value)
}
function Open-Desktop([string[]]$Arguments) {
    $script:Clock = [Diagnostics.Stopwatch]::StartNew()
    $script:Desktop = Start-TestProcess $App $Arguments
    $deadline = [datetime]::UtcNow.AddSeconds(20)
    do {
        $Desktop.Refresh()
        if ($Desktop.HasExited) { throw "Desktop exited ($($Desktop.ExitCode))" }
        if ($Desktop.MainWindowHandle -ne 0) { break }
        Start-Sleep -Milliseconds 100
    } while ([datetime]::UtcNow -lt $deadline)
    if ($Desktop.MainWindowHandle -eq 0) { throw 'Native window did not appear' }
    [void][WorkbenchTestWindow]::ShowWindow($Desktop.MainWindowHandle, 9)
    $script:Window = [Windows.Automation.AutomationElement]::FromHandle($Desktop.MainWindowHandle)
}
function Close-Desktop {
    if (!$Desktop.CloseMainWindow() -or !$Desktop.WaitForExit(12000)) { throw 'Desktop did not quit after closing its window' }
    if ($Desktop.ExitCode -ne 0) { throw "Desktop exited with $($Desktop.ExitCode)" }
}

function Set-TestMarker([int]$Port, [int]$Database, [string]$Key, [string]$Value) {
    $client = [Net.Sockets.TcpClient]::new('127.0.0.1', $Port)
    try {
        $wire = $client.GetStream()
        $wire.ReadTimeout = 3000
        $reader = [IO.StreamReader]::new($wire)
        foreach ($parts in @(@('SELECT', "$Database"), @('SET', $Key, $Value))) {
            $frame = '*' + $parts.Count + "`r`n"
            foreach ($part in $parts) {
                $frame += '$' + [Text.Encoding]::UTF8.GetByteCount($part) + "`r`n" + $part + "`r`n"
            }
            $wire.Write([Text.Encoding]::UTF8.GetBytes($frame))
            if ($reader.ReadLine() -ne '+OK') { throw "Kerniva fixture did not confirm $($parts[0])" }
        }
    } finally { $client.Dispose() }
}

function Measure-DesktopProcesses {
    $all = @(Get-CimInstance Win32_Process -Property ProcessId, ParentProcessId, Name, WorkingSetSize)
    $members = [Collections.Generic.HashSet[uint32]]::new()
    [void]$members.Add($Desktop.Id)
    do {
        $n = $members.Count
        foreach ($process in $all) {
            if ($members.Contains($process.ParentProcessId)) { [void]$members.Add($process.ProcessId) }
        }
    } while ($members.Count -gt $n)
    return @($all | Where-Object { $members.Contains($_.ProcessId) } |
        Select-Object ProcessId, ParentProcessId, Name, WorkingSetSize)
}

Add-Type @'
using System;
using System.Runtime.InteropServices;
public static class WorkbenchMouse {
    [StructLayout(LayoutKind.Sequential)] public struct Point { public int X,Y; }
    [DllImport("user32.dll")] public static extern IntPtr WindowFromPoint(Point point);
    [DllImport("user32.dll")] public static extern IntPtr GetAncestor(IntPtr h, uint flags);
    [DllImport("user32.dll")] public static extern bool ScreenToClient(IntPtr h, ref Point point);
    [DllImport("user32.dll")] public static extern bool PostMessage(IntPtr h, uint message, IntPtr wp, IntPtr lp);
    [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr h);
    [DllImport("user32.dll")] public static extern IntPtr SetThreadDpiAwarenessContext(IntPtr context);
}
'@

function Click-WindowButton($ID) {
    $item = Find-Control ([Windows.Automation.AutomationElement]::AutomationIdProperty) $ID 'Button'
    $item.GetCurrentPattern([Windows.Automation.ScrollItemPattern]::Pattern).ScrollIntoView()
    [void][WorkbenchMouse]::SetForegroundWindow($Desktop.MainWindowHandle)
    Start-Sleep -Milliseconds 150
    $oldDpi = [WorkbenchMouse]::SetThreadDpiAwarenessContext([IntPtr](-4))
    try {
        $pixel = [WorkbenchMouse+Point]::new()
        try {
            $pt = $item.GetClickablePoint()
            $pixel.X = [int]$pt.X; $pixel.Y = [int]$pt.Y
        } catch {
            $bounds = $item.Current.BoundingRectangle
            if ($bounds.IsEmpty) { throw "Native button has no bounds: $ID" }
            $pixel.X = [int]($bounds.Left + $bounds.Width / 2)
            $pixel.Y = [int]($bounds.Top + $bounds.Height / 2)
        }
        $hwnd = [WorkbenchMouse]::WindowFromPoint($pixel)
        if ([WorkbenchMouse]::GetAncestor($hwnd, 2) -ne $Desktop.MainWindowHandle) { throw "Test button is occluded: $ID" }
        if (![WorkbenchMouse]::ScreenToClient($hwnd, [ref]$pixel)) { throw "Cannot locate button: $ID" }
        $lp = [IntPtr](($pixel.Y -shl 16) -bor ($pixel.X -band 0xffff))
        foreach ($event in @(@(0x0200,0), @(0x0201,1), @(0x0202,0))) {
            if (![WorkbenchMouse]::PostMessage($hwnd, $event[0], [IntPtr]$event[1], $lp)) { throw "Mouse input failed: $ID" }
        }
    } finally { [void][WorkbenchMouse]::SetThreadDpiAwarenessContext($oldDpi) }
}
