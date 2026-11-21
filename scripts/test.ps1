param([switch]$Race)
$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location -LiteralPath $ProjectRoot
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw "Go is required in PATH" }
if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) { throw "Cargo is required in PATH" }
& cargo test --manifest-path kerniva_engine/Cargo.toml
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& (Join-Path $PSScriptRoot "build.ps1")
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
$env:KERNIVA_ENGINE_LIBRARY = Join-Path $ProjectRoot "bin/kerniva_engine.dll"
$testArgs = @("test", "./...", "-count=1", "-timeout=180s")
if ($Race) { $testArgs += "-race" }
& go @testArgs
exit $LASTEXITCODE
