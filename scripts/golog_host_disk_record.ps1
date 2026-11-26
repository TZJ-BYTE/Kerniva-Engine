#Requires -Version 5.1
<#
.SYNOPSIS
    Record a bounded Windows trace around a separately run workload.
.DESCRIPTION
    Run in a Windows PowerShell with permission to start kernel recording.
    The default profile records the host disk and virtual disk around a WSL workload.
    Use -NativeSync for native file and disk I/O and thread scheduling.
    The workload supervisor starts after host.ready appears and creates guest.done
    when finished. The recorder also saves the ETL on timeout.
#>
[CmdletBinding()]
param(
    [ValidateRange(30, 900)][int]$MaxSeconds = 420,
    [switch]$NativeSync
)

$ErrorActionPreference = 'Stop'
$wpr = Join-Path ([Environment]::SystemDirectory) 'wpr.exe'
if ($NativeSync) {
    $profileName = 'KernivaNativeSync'
    $profileFile = Join-Path $PSScriptRoot 'golog_host_native_sync.wprp'
    $instancePrefix = 'KernivaNativeSync'
} else {
    $profileName = 'KernivaVhd'
    $profileFile = Join-Path $PSScriptRoot 'golog_host_vhd.wprp'
    $instancePrefix = 'KernivaWal'
}
$profileSpec = "$profileFile!$profileName"
$instance = $instancePrefix + [guid]::NewGuid().ToString('N')
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
    $profileHash = [BitConverter]::ToString($hasher.ComputeHash([IO.File]::ReadAllBytes($profileFile))).Replace('-', '')
} finally {
    $hasher.Dispose()
}
$receipt = [ordered]@{
    instance = $instance
    host_pid = $PID
    profile = 'DiskIO.Verbose.File'
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
$startProfiles = @('-start', $profileSpec)
if ($NativeSync) {
    $receipt.profile = "$profileName.Verbose.File"
    $receipt.profile_sha256 = $profileHash
    $receipt.profile_details_exit = Invoke-Wpr 'profile.log' @('-profiledetails', $profileSpec, '-filemode')
    if ($receipt.profile_details_exit -ne 0) {
        $receipt.end_reason = 'profile_failed'
        $receipt | ConvertTo-Json | Set-Content -Encoding utf8 -LiteralPath $receiptPath
        throw "WPR profile validation failed (exit $($receipt.profile_details_exit)). See $capture\profile.log."
    }
    $receipt.start_utc = [DateTimeOffset]::UtcNow.ToString('o')
    $receipt.start_qpc = [Diagnostics.Stopwatch]::GetTimestamp()
} else {
    $receipt.virtual_disk_profile = "$profileName.Verbose.File"
    $receipt.virtual_disk_profile_sha256 = $profileHash
    $startProfiles = @('-start', 'DiskIO') + $startProfiles
}
$startArgs = $startProfiles + @('-filemode', '-recordtempto', $scratch, '-instancename', $instance)
$receipt.start_exit = Invoke-Wpr 'start.log' $startArgs
if ($receipt.start_exit -ne 0) {
    $receipt | ConvertTo-Json | Set-Content -Encoding utf8 -LiteralPath $receiptPath
    throw "WPR start failed (exit $($receipt.start_exit)). See $capture\start.log."
}

try {
    $receipt.end_reason = 'interrupted'
    $timer = [Diagnostics.Stopwatch]::StartNew()
    if ($NativeSync) {
        $receipt.ready_qpc = [Diagnostics.Stopwatch]::GetTimestamp()
        $receipt.ready_utc = [DateTimeOffset]::UtcNow.ToString('o')
    }
    $receipt | ConvertTo-Json | Set-Content -Encoding utf8 -LiteralPath $receiptPath
    [IO.File]::WriteAllText($ready, $instance)
    Write-Output "Recording. Waiting for $done"
    while (-not [IO.File]::Exists($done) -and $timer.Elapsed.TotalSeconds -lt $MaxSeconds) {
        Start-Sleep -Milliseconds 250
    }
    $receipt.end_reason = if ([IO.File]::Exists($done)) { 'guest_done' } else { 'timeout' }
} finally {
    $receipt.stop_requested_utc = [DateTimeOffset]::UtcNow.ToString('o')
    if ($NativeSync) { $receipt.stop_requested_qpc = [Diagnostics.Stopwatch]::GetTimestamp() }
    $receipt.stop_exit = Invoke-Wpr 'stop.log' @('-stop',$trace,'-skipPdbGen','-instancename',$instance)
    if ($NativeSync) {
        $receipt.stop_finished_qpc = [Diagnostics.Stopwatch]::GetTimestamp()
        $receipt.stop_finished_utc = [DateTimeOffset]::UtcNow.ToString('o')
    }
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
