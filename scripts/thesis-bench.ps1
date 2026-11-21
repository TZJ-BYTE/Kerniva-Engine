[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$RunRoot,
    [string[]]$Variants = @("baseline", "final"),
    [ValidateSet("std", "gnet")][string[]]$Networks = @("std", "gnet"),
    [ValidateSet("memory", "none", "flush", "sync")][string[]]$Durabilities = @("sync"),
    [int[]]$ReadPercents = @(95, 80, 50),
    [long[]]$Rates = @(5000, 10000, 20000),
    [int]$Rounds = 3,
    [int]$DurationSeconds = 30,
    [int]$WarmupSeconds = 3,
    [long]$Keys = 100000,
    [int]$Clients = 64,
    [int]$HotPercent = 0,
    [long]$HotKeys = 1000,
    [long]$Seed = 42,
    [int]$TimeoutMilliseconds = 2000,
    [int]$QueuePerClient = 4096,
    [int]$ReconnectBackoffMilliseconds = 1000,
    [long]$MaxMemoryBytes = 2147483648,
    [string]$Suite = "main",
    [switch]$RebuildClient
)

$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$RunRoot = (Resolve-Path -LiteralPath $RunRoot).Path
$Utf8 = New-Object System.Text.UTF8Encoding($false)

function Write-JsonAtomic([string]$Path, $Value) {
    $temporary = $Path + ".tmp"
    [IO.File]::WriteAllText($temporary, ($Value | ConvertTo-Json -Depth 100), $Utf8)
    if ([IO.File]::Exists($Path)) {
        [IO.File]::Replace($temporary, $Path, [NullString]::Value)
    } else {
        [IO.File]::Move($temporary, $Path)
    }
}

function Quote-Argument([string]$Value) {
    if ($Value.Contains('"')) { throw "A process argument contains a double quote" }
    return '"' + $Value + '"'
}

function Read-RespLine($Stream) {
    $builder = New-Object Text.StringBuilder
    while ($true) {
        $next = $Stream.ReadByte()
        if ($next -lt 0) { throw "Unexpected RESP end of stream" }
        [void]$builder.Append([char]$next)
        if ($next -eq 10) { return $builder.ToString() }
    }
}

function Invoke-ServerCommand([int]$Port, [string]$Command) {
    $connection = New-Object Net.Sockets.TcpClient
    try {
        $connection.SendTimeout = 2000
        $connection.ReceiveTimeout = 2000
        $connection.Connect("127.0.0.1", $Port)
        $stream = $connection.GetStream()
        $request = [Text.Encoding]::ASCII.GetBytes("*1`r`n`$" + $Command.Length + "`r`n" + $Command + "`r`n")
        $stream.Write($request, 0, $request.Length)
        $header = Read-RespLine $stream
        if ($header.StartsWith('+')) { return $header.Substring(1).TrimEnd("`r", "`n") }
        if (-not $header.StartsWith('$')) { throw "Unexpected response $header" }
        $length = [int]::Parse($header.Substring(1).Trim())
        if ($length -lt 0) { throw "Null response to $Command" }
        $payload = New-Object byte[] ($length + 2)
        $offset = 0
        while ($offset -lt $payload.Length) {
            $read = $stream.Read($payload, $offset, $payload.Length - $offset)
            if ($read -eq 0) { throw "Short RESP bulk response" }
            $offset += $read
        }
        if ($payload[$length] -ne 13 -or $payload[$length + 1] -ne 10) { throw "Invalid RESP bulk ending" }
        return [Text.Encoding]::UTF8.GetString($payload, 0, $length)
    } finally {
        $connection.Close()
    }
}

function Get-ServerInfo([int]$Port) {
    $raw = Invoke-ServerCommand $Port "INFO"
    $values = [ordered]@{}
    foreach ($line in $raw.Split("`n")) {
        $separator = $line.IndexOf(':')
        if ($separator -gt 0) {
            $values[$line.Substring(0, $separator)] = $line.Substring($separator + 1).Trim()
        }
    }
    return $values
}

function Get-FreePort {
    for ($attempt = 0; $attempt -lt 100; $attempt++) {
        $port = Get-Random -Minimum 12000 -Maximum 30000
        $listener = New-Object Net.Sockets.TcpListener([Net.IPAddress]::Loopback, $port)
        try {
            $listener.Start()
            return $port
        } catch {
            if ($attempt -eq 99) { throw }
        } finally { $listener.Stop() }
    }
}

function Start-IsolatedServer($Variant, [string]$Configuration, [string]$StopFile, [string]$WorkDirectory, [string]$CaseDirectory) {
    $saved = @{}
    foreach ($entry in Get-ChildItem Env: | Where-Object { $_.Name.StartsWith("KERNIVA_") }) {
        $saved[$entry.Name] = $entry.Value
        [Environment]::SetEnvironmentVariable($entry.Name, $null, "Process")
    }
    try {
        $env:KERNIVA_ENGINE_LIBRARY = $Variant.library
        $arguments = @("-config", (Quote-Argument $Configuration), "-stop-file", (Quote-Argument $StopFile))
        return Start-Process -FilePath $Variant.server -ArgumentList $arguments -WorkingDirectory $WorkDirectory -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $CaseDirectory "server.stdout.log") -RedirectStandardError (Join-Path $CaseDirectory "server.stderr.log")
    } finally {
        foreach ($entry in Get-ChildItem Env: | Where-Object { $_.Name.StartsWith("KERNIVA_") }) {
            [Environment]::SetEnvironmentVariable($entry.Name, $null, "Process")
        }
        foreach ($name in $saved.Keys) { [Environment]::SetEnvironmentVariable($name, $saved[$name], "Process") }
    }
}

function Stop-OwnedServer($Process, [string]$StopFile, [switch]$AllowUnavailableExitCode) {
    if ($null -eq $Process) { return }
    if (-not $Process.HasExited) {
        [IO.File]::WriteAllText($StopFile, "stop", $Utf8)
        if (-not $Process.WaitForExit(30000)) {
            $Process.Kill()
            $Process.WaitForExit()
            throw "The isolated benchmark server required forced termination"
        }
    }
    $Process.WaitForExit()
    if ($null -eq $Process.ExitCode -and $AllowUnavailableExitCode) { return }
    if ($Process.ExitCode -ne 0) { throw "The isolated benchmark server exited with code $($Process.ExitCode)" }
}

function Stop-PreviousAttempt([string]$StatePath) {
    if (-not (Test-Path -LiteralPath $StatePath)) { return }
    $state = Get-Content -LiteralPath $StatePath -Raw | ConvertFrom-Json
    if ($state.client_pid) {
        $previousClient = Get-Process -Id $state.client_pid -ErrorAction SilentlyContinue
        if ($previousClient) {
            $clientDetail = Get-CimInstance Win32_Process -Filter "ProcessId=$($state.client_pid)"
            if ($clientDetail.ExecutablePath -ine $state.client -or -not $clientDetail.CommandLine.Contains($state.start_file)) {
                throw "The previous benchmark client identity could not be verified"
            }
            $previousClient.Kill()
            $previousClient.WaitForExit()
        }
    }
    $previous = Get-Process -Id $state.server_pid -ErrorAction SilentlyContinue
    if ($null -eq $previous) { return }
    $detail = Get-CimInstance Win32_Process -Filter "ProcessId=$($state.server_pid)"
    if ($detail.ExecutablePath -ine $state.server -or -not $detail.CommandLine.Contains($state.stop_file)) {
        throw "The previous benchmark process identity could not be verified"
    }
    Stop-OwnedServer $previous $state.stop_file -AllowUnavailableExitCode
}

function Remove-OwnedTemporaryRoot([string]$Path) {
    $temporaryBase = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\') + '\'
    $absolute = [IO.Path]::GetFullPath($Path).TrimEnd('\')
    if (-not $absolute.StartsWith($temporaryBase, [StringComparison]::OrdinalIgnoreCase) -or [IO.Path]::GetFileName($absolute) -notmatch '^kerniva-thesis-[0-9a-f]{32}$') {
        throw "Temporary benchmark directory ownership could not be verified"
    }
    if (Test-Path -LiteralPath $absolute) {
        $resolved = (Resolve-Path -LiteralPath $absolute).ProviderPath.TrimEnd('\')
        if ($resolved -ine $absolute) { throw "Temporary benchmark directory resolution changed its identity" }
        Remove-Item -LiteralPath $absolute -Recurse -Force
    }
}

function Get-ProcessSample($Process, [double]$Seconds) {
    $Process.Refresh()
    return [ordered]@{
        utc = [DateTime]::UtcNow.ToString("o")
        seconds = $Seconds
        cpu_seconds = $Process.TotalProcessorTime.TotalSeconds
        working_set_bytes = $Process.WorkingSet64
        private_bytes = $Process.PrivateMemorySize64
        threads = $Process.Threads.Count
    }
}

function Invoke-Case($Variant, [string]$CaseDirectory, $Configuration, $CaseMetadata) {
    [void][IO.Directory]::CreateDirectory($CaseDirectory)
    $statePath = Join-Path $CaseDirectory "running.json"
    Stop-PreviousAttempt $statePath
    $temporaryRoot = Join-Path ([IO.Path]::GetTempPath()) ("kerniva-thesis-" + [Guid]::NewGuid().ToString("N"))
    [void][IO.Directory]::CreateDirectory($temporaryRoot)
    $configurationPath = Join-Path $CaseDirectory "config.json"
    $Configuration.data_dir = Join-Path $temporaryRoot "data"
    $Configuration.log_path = Join-Path $CaseDirectory "server.log"
    Write-JsonAtomic $configurationPath $Configuration
    $stopFile = Join-Path $temporaryRoot "stop"
    $readyFile = Join-Path $temporaryRoot "ready"
    $startFile = Join-Path $temporaryRoot "start"
    $server = $null
    $client = $null
    $sampleWriter = $null
    $samples = New-Object 'Collections.Generic.List[object]'
    try {
        $server = Start-IsolatedServer $Variant $configurationPath $stopFile $temporaryRoot $CaseDirectory
        $state = [ordered]@{server_pid=$server.Id; server=$Variant.server; stop_file=$stopFile; temporary_root=$temporaryRoot; started_utc=[DateTime]::UtcNow.ToString("o"); client_pid=$null; client=$Benchmark; start_file=$startFile}
        Write-JsonAtomic $statePath $state
        $readyDeadline = [DateTime]::UtcNow.AddSeconds(30)
        while ($true) {
            if ($server.HasExited) { throw "Server exited before readiness" }
            if ([DateTime]::UtcNow -gt $readyDeadline) { throw "Server PING readiness deadline exceeded" }
            try {
                if ((Invoke-ServerCommand $Configuration.port "PING") -eq "PONG") { break }
            } catch {
                if ([DateTime]::UtcNow -gt $readyDeadline) { throw }
            }
            Start-Sleep -Milliseconds 100
        }
        $stdout = Join-Path $CaseDirectory "client.jsonl"
        $stderr = Join-Path $CaseDirectory "client.stderr.log"
        $histogram = Join-Path $CaseDirectory "histogram.json"
        $arguments = @(
            "-mode", "mixed", "-addr", ("127.0.0.1:" + $Configuration.port),
            "-clients", $Clients, "-rate", $CaseMetadata.rate,
            "-duration", ($DurationSeconds.ToString() + "s"), "-warmup", ($WarmupSeconds.ToString() + "s"),
            "-timeout", ($TimeoutMilliseconds.ToString() + "ms"), "-keys", $Keys,
            "-read-percent", $CaseMetadata.read_percent, "-hot-percent", $HotPercent, "-hot-keys", $HotKeys,
            "-seed", $Seed, "-queue-per-client", $QueuePerClient,
            "-reconnect-backoff", ($ReconnectBackoffMilliseconds.ToString() + "ms"),
            "-network", $CaseMetadata.network, "-durability", $CaseMetadata.durability,
            "-histogram-out", (Quote-Argument $histogram),
            "-ready-file", (Quote-Argument $readyFile), "-start-file", (Quote-Argument $startFile)
        )
        $client = Start-Process -FilePath $Benchmark -ArgumentList $arguments -WorkingDirectory $temporaryRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput $stdout -RedirectStandardError $stderr
        $state.client_pid = $client.Id
        Write-JsonAtomic $statePath $state
        $preparationDeadline = [DateTime]::UtcNow.AddMinutes(5)
        while (-not (Test-Path -LiteralPath $readyFile)) {
            if ($client.HasExited) { throw "Client exited during preparation. Inspect client.stderr.log" }
            if ($server.HasExited) { throw "Server exited during preload/warmup" }
            if ([DateTime]::UtcNow -gt $preparationDeadline) { throw "Preload/warmup deadline exceeded" }
            Start-Sleep -Milliseconds 100
        }
        $infoBefore = Get-ServerInfo $Configuration.port
        Write-JsonAtomic (Join-Path $CaseDirectory "info-before.json") $infoBefore
        $clock = [Diagnostics.Stopwatch]::StartNew()
        $firstSample = Get-ProcessSample $server 0
        $samples.Add($firstSample)
        $sampleWriter = New-Object IO.StreamWriter((Join-Path $CaseDirectory "process.jsonl"), $false, $Utf8)
        $sampleWriter.WriteLine(($firstSample | ConvertTo-Json -Compress))
        $sampleWriter.Flush()
        [IO.File]::WriteAllText($startFile, "start", $Utf8)
        $nextSample = 1.0
        $measurementDeadline = $DurationSeconds + $TimeoutMilliseconds/1000.0 + 30
        while (-not $client.HasExited) {
            if ($server.HasExited) { throw "Server exited during measurement" }
            if ($clock.Elapsed.TotalSeconds -gt $measurementDeadline) { throw "Measurement deadline exceeded" }
            if ($clock.Elapsed.TotalSeconds -ge $nextSample) {
                $sample = Get-ProcessSample $server $clock.Elapsed.TotalSeconds
                $samples.Add($sample)
                $sampleWriter.WriteLine(($sample | ConvertTo-Json -Compress))
                $sampleWriter.Flush()
                $nextSample += 1.0
            }
            Start-Sleep -Milliseconds 100
        }
        $client.WaitForExit()
        $lastSample = Get-ProcessSample $server $clock.Elapsed.TotalSeconds
        $samples.Add($lastSample)
        $sampleWriter.WriteLine(($lastSample | ConvertTo-Json -Compress))
        $sampleWriter.Close()
        $sampleWriter = $null
        if ($client.ExitCode -ne 0) { throw "Benchmark client failed with exit code $($client.ExitCode)" }
        $measurement = Get-Content -LiteralPath $stdout -Raw | ConvertFrom-Json
        $infoAfter = Get-ServerInfo $Configuration.port
        Write-JsonAtomic (Join-Path $CaseDirectory "info-after.json") $infoAfter
        $cpuSeconds = $lastSample.cpu_seconds - $firstSample.cpu_seconds
        $resources = [ordered]@{
            sample_interval_seconds = 1
            resource_window_seconds = $lastSample.seconds - $firstSample.seconds
            cpu_seconds = $cpuSeconds
            cpu_percent_one_core = 100*$cpuSeconds/$lastSample.seconds
            cpu_percent_machine = 100*$cpuSeconds/$lastSample.seconds/[Environment]::ProcessorCount
            rss_peak_bytes = ($samples | ForEach-Object { [long]$_.working_set_bytes } | Measure-Object -Maximum).Maximum
            private_peak_bytes = ($samples | ForEach-Object { [long]$_.private_bytes } | Measure-Object -Maximum).Maximum
            heap_alloc_before_bytes = [long]$infoBefore.heap_alloc_bytes
            heap_alloc_after_bytes = [long]$infoAfter.heap_alloc_bytes
            go_num_gc_delta = [long]$infoAfter.num_gc - [long]$infoBefore.num_gc
            go_pause_ns_delta = [long]$infoAfter.gc_pause_total_ns - [long]$infoBefore.gc_pause_total_ns
            info_sampling = "before_start_marker_and_after_client_exit"
        }
        $result = [ordered]@{
            status = "complete"
            completed_utc = [DateTime]::UtcNow.ToString("o")
            plan_id = $PlanId
            case = $CaseMetadata
            variant = $Variant
            client_sha256 = $BenchmarkHash
            measurement = $measurement
            resources = $resources
            configuration = $Configuration
            temporary_root = $temporaryRoot
        }
        Stop-OwnedServer $server $stopFile
        Remove-OwnedTemporaryRoot $temporaryRoot
        $result.temporary_data_removed = $true
        Write-JsonAtomic (Join-Path $CaseDirectory "result.json") $result
        Remove-Item -LiteralPath $statePath -ErrorAction SilentlyContinue
        return $result
    } finally {
        if ($sampleWriter) { $sampleWriter.Close() }
        if ($client -and -not $client.HasExited) { $client.Kill(); $client.WaitForExit() }
        Stop-OwnedServer $server $stopFile
    }
}

if ($Rounds -le 0 -or $DurationSeconds -le 0 -or $WarmupSeconds -lt 0 -or $Keys -le 0 -or $Clients -le 0) {
    throw "Rounds, duration, keys and clients must be positive, warmup must be nonnegative"
}
if ($Variants.Count -lt 1) { throw "At least one variant is required" }
$Benchmark = Join-Path $RunRoot "kerniva-thesis-bench.exe"
if ($RebuildClient -or -not (Test-Path -LiteralPath $Benchmark)) {
    Push-Location -LiteralPath $ProjectRoot
    try {
        & go build -trimpath -o $Benchmark ./scripts/bench
        if ($LASTEXITCODE -ne 0) { throw "Benchmark build failed" }
    } finally { Pop-Location }
}
$BenchmarkHash = (Get-FileHash -LiteralPath $Benchmark -Algorithm SHA256).Hash
$variantDetails = @()
foreach ($name in $Variants) {
    $server = Join-Path $RunRoot ($name + "\bin\kerniva-server.exe")
    $library = Join-Path $RunRoot ($name + "\bin\kerniva_engine.dll")
    if (-not (Test-Path -LiteralPath $server) -or -not (Test-Path -LiteralPath $library)) { throw "Missing binary pair for $name" }
    $variantDetails += [ordered]@{name=$name; server=$server; library=$library; server_sha256=(Get-FileHash -LiteralPath $server -Algorithm SHA256).Hash; library_sha256=(Get-FileHash -LiteralPath $library -Algorithm SHA256).Hash}
}
$plan = [ordered]@{
    schema_version=4; suite=$Suite; variants=$variantDetails; benchmark_sha256=$BenchmarkHash
    runner_sha256=(Get-FileHash -LiteralPath $PSCommandPath -Algorithm SHA256).Hash
    networks=$Networks; durabilities=$Durabilities; read_percents=$ReadPercents; rates=$Rates
    rounds=$Rounds; duration_seconds=$DurationSeconds; warmup_seconds=$WarmupSeconds
    keys=$Keys; clients=$Clients; hot_percent=$HotPercent; hot_keys=$HotKeys; seed=$Seed
    timeout_ms=$TimeoutMilliseconds; queue_per_client=$QueuePerClient; max_memory_bytes=$MaxMemoryBytes
    reconnect_backoff_ms=$ReconnectBackoffMilliseconds
    created_from="scripts/thesis-bench.ps1"
}
$hasher = [Security.Cryptography.SHA256]::Create()
try {
    $hash = $hasher.ComputeHash($Utf8.GetBytes(($plan | ConvertTo-Json -Depth 20 -Compress)))
} finally { $hasher.Dispose() }
$PlanId = ([BitConverter]::ToString($hash).Replace("-", "").ToLowerInvariant()).Substring(0, 16)
$suiteDirectory = Join-Path $RunRoot ("measurements\" + $Suite + "-" + $PlanId)
[void][IO.Directory]::CreateDirectory($suiteDirectory)
$suiteLock = [IO.File]::Open((Join-Path $suiteDirectory "suite.lock"), [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
try {
Write-JsonAtomic (Join-Path $suiteDirectory "plan.json") $plan
$completed = 0
for ($round = 1; $round -le $Rounds; $round++) {
    foreach ($network in $Networks) {
        foreach ($durability in $Durabilities) {
            foreach ($readPercent in $ReadPercents) {
                foreach ($rate in $Rates) {
                    if ($rate -le 0 -or $readPercent -lt 0 -or $readPercent -gt 100) { throw "Invalid rate or read percentage" }
                    $order = @()
                    for ($position = 0; $position -lt $variantDetails.Count; $position++) {
                        $order += $variantDetails[($position + $round - 1) % $variantDetails.Count]
                    }
                    $caseId = "$network-$durability-r$readPercent-q$rate-k$Keys-h$HotPercent-$HotKeys-c$Clients-round$round"
                    foreach ($variant in $order) {
                        $caseDirectory = Join-Path $suiteDirectory ("cases\" + $caseId + "\" + $variant.name)
                        $resultPath = Join-Path $caseDirectory "result.json"
                        if (Test-Path -LiteralPath $resultPath) {
                            $existing = Get-Content -LiteralPath $resultPath -Raw | ConvertFrom-Json
                            if ($existing.status -eq "complete" -and $existing.plan_id -eq $PlanId) {
                                $completed++
                                Write-Host "SKIP $caseId $($variant.name)"
                                continue
                            }
                        }
                        $metadata = [ordered]@{id=$caseId; round=$round; network=$network; durability=$durability; read_percent=$readPercent; rate=$rate; keys=$Keys; hot_percent=$HotPercent; hot_keys=$HotKeys; clients=$Clients; seed=$Seed}
                        $configuration = [ordered]@{
                            host="127.0.0.1"; port=(Get-FreePort); network_type=$network; db_count=1
                            max_memory=$MaxMemoryBytes; max_memory_policy="noeviction"
                            persistence_enabled=($durability -ne "memory"); persistence_type="kerniva"; data_dir=""
                            cold_start_strategy="lazy_load"; persistence_write_mode="strong"
                            persistence_durability=$(if ($durability -eq "memory") { "sync" } else { $durability })
                            engine_segment_size=4194304; engine_checkpoint_after_ops=1024
                            log_level="error"; log_path=""; pprof_enabled=$false
                        }
                        Write-Host ("RUN " + [DateTime]::UtcNow.ToString("o") + " " + $caseId + " " + $variant.name)
                        $result = Invoke-Case $variant $caseDirectory $configuration $metadata
                        $completed++
                        Write-JsonAtomic (Join-Path $suiteDirectory "progress.json") ([ordered]@{completed=$completed; last_case=$caseId; last_variant=$variant.name; updated_utc=[DateTime]::UtcNow.ToString("o")})
                        Write-Host ("DONE " + $variant.name + " offered=" + $result.measurement.offered + " success=" + $result.measurement.succeeded + " failures=" + $result.measurement.failures + " qps=" + [math]::Round($result.measurement.actual_success_rate))
                    }
                }
            }
        }
    }
}
Write-JsonAtomic (Join-Path $suiteDirectory "complete.json") ([ordered]@{completed=$completed; completed_utc=[DateTime]::UtcNow.ToString("o"); plan_id=$PlanId})
Write-Host ("SUITE COMPLETE " + $suiteDirectory)
} finally {
    $suiteLock.Dispose()
}
