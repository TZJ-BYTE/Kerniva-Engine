param([switch]$SkipRust)
$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location -LiteralPath $ProjectRoot
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { throw "Go is required in PATH" }
New-Item -ItemType Directory -Path "bin" -Force | Out-Null
if (-not $SkipRust) {
    if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) { throw "Cargo is required in PATH" }
    $messages = & cargo build --release --manifest-path kerniva_engine/Cargo.toml --message-format=json-render-diagnostics
    if ($LASTEXITCODE -ne 0) { throw "Rust engine build failed" }
    $library = $messages | ForEach-Object {
        $message = $_ | ConvertFrom-Json
        if ($message.reason -eq "compiler-artifact" -and $message.target.name -eq "kerniva_engine") {
            $message.filenames | Where-Object { $_ -match '\.(dll|so|dylib)$' }
        }
    } | Select-Object -Last 1
    if (-not $library) { throw "Cargo did not produce the engine shared library" }
    Copy-Item -LiteralPath $library -Destination "bin" -Force
    $maintainer = $messages | ForEach-Object {
        $message = $_ | ConvertFrom-Json
        if ($message.reason -eq "compiler-artifact" -and $message.target.name -eq "kerniva-maintainer") {
            $message.executable
        }
    } | Select-Object -Last 1
    if (-not $maintainer) { throw "Cargo did not produce the maintenance executable" }
    Copy-Item -LiteralPath $maintainer -Destination "bin" -Force
}
& go build -trimpath -o bin/kerniva-server.exe ./cmd/server
if ($LASTEXITCODE -ne 0) { throw "Server build failed" }
& go build -trimpath -o bin/kerniva-client.exe ./cmd/client
if ($LASTEXITCODE -ne 0) { throw "Client build failed" }
Write-Host "Build complete. Server, client, and native engine are in bin."
