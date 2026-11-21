$ErrorActionPreference = "Stop"

# Ensure we are in the project root
$ProjectRoot = (Resolve-Path "$PSScriptRoot\..").Path
Set-Location -LiteralPath $ProjectRoot

Write-Host "Cleaning build files..."

foreach ($name in @("bin", "build", "dist")) {
    $target = [IO.Path]::GetFullPath((Join-Path $ProjectRoot $name))
    if (-not $target.StartsWith($ProjectRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) { throw "Unexpected cleanup target" }
    if (Test-Path -LiteralPath $target) { Remove-Item -LiteralPath $target -Recurse -Force }
}

Write-Host "Clean Complete."
