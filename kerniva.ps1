param(
  [Parameter(Position = 0)]
  [string] $Command,

  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]] $ArgsRest
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

function RepoRoot {
  $PSScriptRoot
}

function EnsureDir([string]$Path) {
  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -ItemType Directory -Path $Path | Out-Null
  }
}

function ReadPid([string]$PidPath) {
  if (-not (Test-Path -LiteralPath $PidPath)) { return $null }
  $raw = (Get-Content -LiteralPath $PidPath -ErrorAction SilentlyContinue | Select-Object -First 1)
  if (-not $raw) { return $null }
  $parsedPid = 0
  if ([int]::TryParse($raw.Trim(), [ref]$parsedPid)) { return $parsedPid }
  return $null
}

function GetProcessPathById([int]$ProcessId) {
  try {
    $p = Get-CimInstance Win32_Process -Filter "ProcessId=$ProcessId"
    if ($null -eq $p) { return $null }
    return $p.ExecutablePath
  } catch {
    return $null
  }
}

function IsServerProcess([int]$ProcessId, [string]$ExpectedExe) {
  try {
    $p = Get-Process -Id $ProcessId -ErrorAction Stop
  } catch {
    return $false
  }
  $path = GetProcessPathById $ProcessId
  if (-not $path) { return $false }
  return ([IO.Path]::GetFullPath($path) -ieq [IO.Path]::GetFullPath($ExpectedExe))
}

function Usage {
  $u = @"
用法:
  .\kerniva build
  .\kerniva start
  .\kerniva stop
  .\kerniva restart
  .\kerniva status
  .\kerniva logs [--follow] [--tail N]
  .\kerniva client [host] [port]
  .\kerniva workbench [--port PORT] [--config PATH]
  .\kerniva shell [--config PATH | --url http://127.0.0.1:PORT]

说明:
  - build 调用 scripts\build.ps1 构建服务和原生库
  - start 在缺少可执行文件时自动构建
  - stop 基于 logs\kerniva.pid 停止进程
"@
  Write-Host $u
}

$root = RepoRoot
$binDir = Join-Path $root "bin"
$logDir = Join-Path $root "logs"
$pidPath = Join-Path $logDir "kerniva.pid"
$stopPath = Join-Path $logDir "kerniva.stop"
$serverExe = Join-Path $binDir "kerniva-server.exe"
$clientExe = Join-Path $binDir "kerniva-client.exe"
$stdoutPath = Join-Path $logDir "server.out.log"
$stderrPath = Join-Path $logDir "server.err.log"

EnsureDir $logDir
EnsureDir $binDir

if (-not $Command -or $Command -in @("help", "-h", "--help", "/?")) {
  Usage
  exit 0
}

switch ($Command.ToLowerInvariant()) {
  "shell" {
    & python -X utf8 -B (Join-Path $root "scripts\kerniva-workbench.py") --shell @ArgsRest
    exit $LASTEXITCODE
  }
  "workbench" {
    & python -X utf8 -B (Join-Path $root "scripts\kerniva-workbench.py") @ArgsRest
    exit $LASTEXITCODE
  }
  "build" {
    & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $root "scripts\build.ps1")
    exit $LASTEXITCODE
  }
  "start" {
    $existingPid = ReadPid $pidPath
    if ($existingPid -and (IsServerProcess $existingPid $serverExe)) {
      Write-Host "已在运行: pid=$existingPid"
      exit 0
    }

    $buildScript = Join-Path $root "scripts\build.ps1"
    if (-not (Test-Path -LiteralPath $serverExe) -and (Test-Path -LiteralPath $buildScript)) {
      & powershell -NoProfile -ExecutionPolicy Bypass -File $buildScript | Out-Host
      if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }

    if (-not (Test-Path -LiteralPath $serverExe)) {
      throw "找不到服务端可执行文件: $serverExe"
    }

    Remove-Item -LiteralPath $stopPath -ErrorAction SilentlyContinue
    # Let cmd own real file handles so log redirection survives this script.
    $env:KERNIVA_LAUNCH_SERVER = $serverExe
    $env:KERNIVA_LAUNCH_STOP = $stopPath
    $env:KERNIVA_LAUNCH_OUT = $stdoutPath
    $env:KERNIVA_LAUNCH_ERR = $stderrPath
    $launchCommand = '""%KERNIVA_LAUNCH_SERVER%" -stop-file "%KERNIVA_LAUNCH_STOP%" <NUL >"%KERNIVA_LAUNCH_OUT%" 2>"%KERNIVA_LAUNCH_ERR%""'
    $launcher = Start-Process -FilePath $env:ComSpec -ArgumentList @("/d", "/v:off", "/s", "/c", $launchCommand) -WorkingDirectory $root -WindowStyle Hidden -PassThru
    $serverProcess = $null
    for ($i = 0; $i -lt 100; $i++) {
      $serverProcess = Get-CimInstance Win32_Process -Filter "ParentProcessId=$($launcher.Id)" |
        Where-Object { $_.ExecutablePath -ieq $serverExe } | Select-Object -First 1
      if ($serverProcess -or $launcher.HasExited) { break }
      Start-Sleep -Milliseconds 100
    }
    if (-not $serverProcess) { throw "启动失败，请查看 $stderrPath" }
    $serverProcessId = $serverProcess.ProcessId
    Set-Content -LiteralPath $pidPath -Value $serverProcessId -Encoding ascii

    Start-Sleep -Milliseconds 200
    if (IsServerProcess $serverProcessId $serverExe) {
      Write-Host "启动成功: pid=$serverProcessId"
      exit 0
    }
    throw "启动失败: pid=$serverProcessId"
  }

  "stop" {
    $serverPid = ReadPid $pidPath
    if (-not $serverPid) {
      Write-Host "未找到 pid 文件: $pidPath"
      exit 0
    }

    if (-not (IsServerProcess $serverPid $serverExe)) {
      Remove-Item -LiteralPath $pidPath -ErrorAction SilentlyContinue
      Write-Host "进程不存在或不是当前服务端: pid=$serverPid"
      exit 0
    }

    Set-Content -LiteralPath $stopPath -Value "stop" -Encoding ascii
    for ($i = 0; $i -lt 300; $i++) {
      if (-not (IsServerProcess $serverPid $serverExe)) { break }
      Start-Sleep -Milliseconds 100
    }
    if (IsServerProcess $serverPid $serverExe) { throw "Graceful shutdown timed out. Inspect logs before retrying." }

    Remove-Item -LiteralPath $pidPath -ErrorAction SilentlyContinue
    Write-Host "已停止: pid=$serverPid"
    exit 0
  }

  "restart" {
    & $PSCommandPath stop | Out-Host
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    & $PSCommandPath start | Out-Host
    exit $LASTEXITCODE
  }

  "status" {
    $serverPid = ReadPid $pidPath
    if (-not $serverPid) {
      Write-Host "状态: stopped"
      exit 0
    }
    if (IsServerProcess $serverPid $serverExe) {
      Write-Host "状态: running (pid=$serverPid)"
      exit 0
    }
    Write-Host "状态: stale pid (pid=$serverPid)"
    exit 0
  }

  "logs" {
    $follow = $false
    $tail = 200
    for ($i = 0; $i -lt $ArgsRest.Count; $i++) {
      $a = $ArgsRest[$i]
      if ($a -eq "--follow") { $follow = $true; continue }
      if ($a -eq "--tail" -and $i + 1 -lt $ArgsRest.Count) {
        $n = 0
        if ([int]::TryParse($ArgsRest[$i + 1], [ref]$n)) { $tail = $n }
        $i++
        continue
      }
    }

    $target = $stderrPath
    if (-not (Test-Path -LiteralPath $target)) {
      Write-Host "未找到日志文件: $target"
      exit 0
    }
    if ($follow) {
      Get-Content -LiteralPath $target -Tail $tail -Wait
    } else {
      Get-Content -LiteralPath $target -Tail $tail
    }
    exit 0
  }

  "client" {
    $buildScript = Join-Path $root "scripts\build.ps1"
    if (-not (Test-Path -LiteralPath $clientExe) -and (Test-Path -LiteralPath $buildScript)) {
      & powershell -NoProfile -ExecutionPolicy Bypass -File $buildScript | Out-Host
      if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    if (-not (Test-Path -LiteralPath $clientExe)) {
      throw "找不到客户端可执行文件: $clientExe"
    }
    $serverHost = "127.0.0.1"
    $serverPort = "16379"
    if ($ArgsRest.Count -ge 1) { $serverHost = $ArgsRest[0] }
    if ($ArgsRest.Count -ge 2) { $serverPort = $ArgsRest[1] }
    & $clientExe $serverHost $serverPort
    exit $LASTEXITCODE
  }

  default {
    Usage
    exit 1
  }
}
