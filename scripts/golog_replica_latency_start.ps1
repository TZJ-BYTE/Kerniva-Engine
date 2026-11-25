[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidatePattern('^/[A-Za-z0-9/._-]+$')]
    [string]$RunRoot,

    [Parameter(Mandatory)]
    [ValidatePattern('^/[A-Za-z0-9/._-]+$')]
    [string]$Block,

    [ValidatePattern('^[A-Za-z0-9._-]+$')]
    [string]$Distribution = 'Kerniva-Restored',
    [ValidatePattern('^[A-Za-z0-9._-]+$')]
    [string]$LinuxUser = 'lemon',
    [string]$LogDirectory = (Join-Path $PSScriptRoot '..\logs\golog-replica-latency'),
    [switch]$Queue,
    [switch]$Followup,
    [switch]$Resume,
    [switch]$Worker
)

$ErrorActionPreference = 'Stop'
$workerExecutable = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$wsl = Join-Path $env:SystemRoot 'System32\wsl.exe'
if ($Queue -and $Followup) {
    throw 'Choose a queue or a follow-up worker per launch.'
}

function Write-LaunchRecord {
    param([string]$Name, [hashtable]$Value)

    $path = Join-Path $LogDirectory $Name
    [System.IO.File]::WriteAllText("$path.tmp", ($Value | ConvertTo-Json -Depth 5))
    [System.IO.File]::Move("$path.tmp", $path)
}

function ConvertTo-CommandLineArgument {
    param([string]$Value)

    $escaped = [regex]::Replace($Value, '(\\*)"', '$1$1\"')
    $escaped = [regex]::Replace($escaped, '(\\+)$', '$1$1')
    return '"' + $escaped + '"'
}

if ($Worker) {
    $startedAt = [DateTimeOffset]::UtcNow.ToString('o')
    $exitCode = 1
    try {
        Write-LaunchRecord 'worker.json' @{
            started_at = $startedAt
            worker_pid = $PID
            run_root = $RunRoot
            block = $Block
            queue = $Queue.IsPresent
            followup = $Followup.IsPresent
            resume = $Resume.IsPresent
        }
        $wslArguments = @('-d', $Distribution, '-u', $LinuxUser, '--cd', '/', '--exec',
            'python3', '-B')
        if ($Followup) {
            $wslArguments += @("$RunRoot/control/golog_replica_latency_followup.py", '--root', $RunRoot, '--config', $Block)
        }
        else {
            $wslArguments += "$RunRoot/control/golog_replica_latency_run.py"
            if ($Queue) {
                $wslArguments += @('queue', '--root', $RunRoot, '--queue', $Block)
            }
            else {
                $wslArguments += @('run', '--root', $RunRoot, '--block', $Block)
            }
        }
        if ($Resume) {
            $wslArguments += '--resume'
        }
        $processOptions = @{
            FilePath = $wsl
            ArgumentList = $wslArguments
            RedirectStandardOutput = (Join-Path $LogDirectory 'wsl.stdout.log')
            RedirectStandardError = (Join-Path $LogDirectory 'wsl.stderr.log')
            WindowStyle = 'Hidden'
            PassThru = $true
        }
        $process = Start-Process @processOptions
        $null = $process.Handle
        Write-LaunchRecord 'wsl.json' @{ pid = $process.Id }
        $process.WaitForExit()
        $exitCode = $process.ExitCode
    }
    catch {
        Write-LaunchRecord 'worker-error.json' @{ error = $_.ToString() }
    }
    finally {
        Write-LaunchRecord 'exit.json' @{
            finished_at = [DateTimeOffset]::UtcNow.ToString('o')
            exit_code = $exitCode
            worker_pid = $PID
        }
    }
    exit $exitCode
}

$launchId = [DateTimeOffset]::UtcNow.ToString('yyyyMMddTHHmmssfffZ') + '-' + [guid]::NewGuid().ToString('N').Substring(0, 8)
$runId = $RunRoot.TrimEnd('/').Split('/')[-1]
$LogDirectory = [System.IO.Path]::GetFullPath((Join-Path (Join-Path $LogDirectory $runId) "launch\$launchId"))
[System.IO.Directory]::CreateDirectory($LogDirectory) | Out-Null
$command = @($workerExecutable, '-NoLogo', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
    '-File', $PSCommandPath, '-Worker', '-RunRoot', $RunRoot, '-Block', $Block,
    '-Distribution', $Distribution, '-LinuxUser', $LinuxUser, '-LogDirectory', $LogDirectory)
if ($Resume) {
    $command += '-Resume'
}
if ($Queue) {
    $command += '-Queue'
}
if ($Followup) {
    $command += '-Followup'
}

# WMI creates the worker outside the invoking terminal's process tree.
$startup = New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly -Property @{ ShowWindow = [uint16]0 }
$launch = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{
    CommandLine = ($command | ForEach-Object { ConvertTo-CommandLineArgument $_ }) -join ' '
    CurrentDirectory = $LogDirectory
    ProcessStartupInformation = $startup
}
Write-LaunchRecord 'launch.json' @{
    requested_at = [DateTimeOffset]::UtcNow.ToString('o')
    launcher_pid = $PID
    worker_pid = $launch.ProcessId
    return_value = $launch.ReturnValue
    run_root = $RunRoot
    block = $Block
    queue = $Queue.IsPresent
    followup = $Followup.IsPresent
}
if ($launch.ReturnValue -ne 0) {
    throw "Win32_Process.Create returned $($launch.ReturnValue). See $LogDirectory"
}
[pscustomobject]@{ WorkerPid = $launch.ProcessId; LogDirectory = $LogDirectory }
