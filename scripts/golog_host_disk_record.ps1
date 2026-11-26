#Requires -Version 5.1
<#
.SYNOPSIS
    Record a bounded Windows DiskIO trace around a separately run WSL workload.
.DESCRIPTION
    Run in a Windows PowerShell with permission to start DiskIO recording.
    A guest supervisor can start after host.ready appears and create guest.done
    after the post-workload clock probes. The host saves the ETL on timeout too.
#>
[CmdletBinding()]
param([ValidateRange(30, 900)][int]$MaxSeconds = 420)

$ErrorActionPreference = 'Stop'
$wpr = Join-Path ([Environment]::SystemDirectory) 'wpr.exe'
$vhdProfile = (Get-Item -LiteralPath (Join-Path $PSScriptRoot 'golog_host_vhd.wprp')).FullName
$instance = 'KernivaWal' + [guid]::NewGuid().ToString('N')
$capture = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) "Kerniva\diagnostics\$instance"
$scratch = Join-Path $capture 'wpr-temp'
[void][IO.Directory]::CreateDirectory($scratch)
$trace = Join-Path $capture 'disk.etl'
$receiptPath = Join-Path $capture 'host.json'
$ready = Join-Path $capture 'host.ready'
$done = Join-Path $capture 'guest.done'

function Invoke-Wpr([string]$logName, [string[]]$wprArgs) {
    # Windows PowerShell treats redirected native stderr as error records.
    $ErrorActionPreference = 'Continue'
    & $wpr @wprArgs 2>&1 | Out-File -Encoding utf8 -LiteralPath (Join-Path $capture $logName)
    return $LASTEXITCODE
}

$hasher = [Security.Cryptography.SHA256]::Create()
try {
    $profileHash = [BitConverter]::ToString($hasher.ComputeHash([IO.File]::ReadAllBytes($vhdProfile))).Replace('-', '')
} finally {
    $hasher.Dispose()
}
$receipt = [ordered]@{
    instance = $instance
    host_pid = $PID
    profile = 'DiskIO.Verbose.File'
    virtual_disk_profile = 'KernivaVhd.Verbose.File'
    virtual_disk_profile_sha256 = $profileHash
    wpr_version = (Get-Item -LiteralPath $wpr).VersionInfo.FileVersion
    qpc_frequency_hz = [Diagnostics.Stopwatch]::Frequency
    start_utc = [DateTimeOffset]::UtcNow.ToString('o')
    max_seconds = $MaxSeconds
    start_exit = $null
    end_reason = 'start_failed'
    stop_exit = $null
    etl_bytes = 0
}
Write-Output "Capture directory: $capture"
$receipt.start_exit = Invoke-Wpr 'start.log' @('-start','DiskIO','-start',($vhdProfile + '!KernivaVhd'),'-filemode','-recordtempto',$scratch,'-instancename',$instance)
if ($receipt.start_exit -ne 0) {
    $receipt | ConvertTo-Json | Set-Content -Encoding utf8 -LiteralPath $receiptPath
    throw "WPR start failed (exit $($receipt.start_exit)). See $capture\start.log."
}

try {
    $receipt.end_reason = 'interrupted'
    $timer = [Diagnostics.Stopwatch]::StartNew()
    $receipt | ConvertTo-Json | Set-Content -Encoding utf8 -LiteralPath $receiptPath
    [IO.File]::WriteAllText($ready, $instance)
    Write-Output "Recording. Waiting for $done"
    while (-not [IO.File]::Exists($done) -and $timer.Elapsed.TotalSeconds -lt $MaxSeconds) {
        Start-Sleep -Milliseconds 250
    }
    $receipt.end_reason = if ([IO.File]::Exists($done)) { 'guest_done' } else { 'timeout' }
} finally {
    $receipt.stop_requested_utc = [DateTimeOffset]::UtcNow.ToString('o')
    $receipt.stop_exit = Invoke-Wpr 'stop.log' @('-stop',$trace,'-skipPdbGen','-instancename',$instance)
    if ($receipt.stop_exit -ne 0) {
        $receipt.cancel_exit = Invoke-Wpr 'cancel.log' @('-cancel','-instancename',$instance)
    }
    if ([IO.File]::Exists($ready)) { [IO.File]::Delete($ready) }
    if ([IO.File]::Exists($trace)) { $receipt.etl_bytes = (Get-Item -LiteralPath $trace).Length }
    $receipt | ConvertTo-Json | Set-Content -Encoding utf8 -LiteralPath $receiptPath
}
if ($receipt.stop_exit -ne 0 -or $receipt.etl_bytes -eq 0) {
    throw "WPR did not save a complete trace. See $receiptPath."
}
Write-Output "Saved $trace ($($receipt.end_reason))."
