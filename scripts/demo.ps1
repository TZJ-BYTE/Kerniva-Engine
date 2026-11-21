param(
    [string]$Listen = "127.0.0.1:18080",
    [string]$Addr = "127.0.0.1:16379",
    [string]$ResultsDir = "bench-results",
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Push-Location -LiteralPath $projectRoot
try {
    if (-not $SkipBuild) {
        New-Item -ItemType Directory -Path "bin" -Force | Out-Null
        & go build -trimpath -o bin/kerniva-bench.exe ./scripts/bench
        if ($LASTEXITCODE -ne 0) { throw "Benchmark client build failed" }
        & go build -trimpath -o bin/kerniva-demo.exe ./cmd/demo
        if ($LASTEXITCODE -ne 0) { throw "Demo build failed" }
    }
    & (Join-Path $projectRoot "bin\kerniva-demo.exe") -listen $Listen -addr $Addr -bench (Join-Path $projectRoot "bin\kerniva-bench.exe") -results-dir $ResultsDir
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
