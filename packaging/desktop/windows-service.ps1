#Requires -Version 7.2
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('Install', 'Status', 'Start', 'Stop', 'Restart', 'EnableAutoStart', 'DisableAutoStart', 'Uninstall')]
    [string]$Action,

    [Parameter(Mandatory)]
    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9-]{0,39}$')]
    [string]$Instance,

    [string]$EngineBundle,

    [ValidatePattern('^[a-fA-F0-9]{64}$')]
    [string]$ManifestSha256,

    [ValidateRange(1024, 65535)]
    [int]$Port = 16379
)

$ErrorActionPreference = 'Stop'
$name = "Kerniva-$Instance"
$programFiles = [Environment]::GetFolderPath([Environment+SpecialFolder]::ProgramFiles)
$root = Join-Path $programFiles "Kerniva-Service-$Instance"
$server = Join-Path $root 'kerniva-server.exe'
$library = Join-Path $root 'kerniva_engine.dll'
$config = Join-Path $root 'instance.json'
$data = Join-Path $root 'data'
$state = Join-Path $root 'service-state.json'
$serverCommand = '"{0}" --windows-service {1} --config "{2}" --engine-library "{3}"' -f $server, $name, $config, $library
$registryPath = "HKLM:\SYSTEM\CurrentControlSet\Services\$name"
$manager = Join-Path ([Environment]::SystemDirectory) 'sc.exe'
$localService = [Security.Principal.SecurityIdentifier]::new('S-1-5-19')
$system = [Security.Principal.SecurityIdentifier]::new('S-1-5-18')
$administrators = [Security.Principal.SecurityIdentifier]::new('S-1-5-32-544')

function Invoke-Manager {
    param([string[]]$Arguments)
    $start = [Diagnostics.ProcessStartInfo]::new($manager)
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }
    $process = [Diagnostics.Process]::Start($start)
    try {
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        $process.WaitForExit()
        if ($process.ExitCode -ne 0) {
            throw "SCM command failed ($($process.ExitCode)): $($stdout.Result.Trim()) $($stderr.Result.Trim())"
        }
    } finally {
        $process.Dispose()
    }
}

function New-ProtectedDirectory {
    param(
        [string]$Path,
        [Security.Principal.SecurityIdentifier]$ServiceAccount,
        [Security.AccessControl.FileSystemRights]$Rights
    )
    if (Test-Path -LiteralPath $Path) { throw "Directory already exists: $Path" }
    $security = [Security.AccessControl.DirectorySecurity]::new()
    $security.SetAccessRuleProtection($true, $false)
    $security.SetOwner($administrators)
    $inherit = [Security.AccessControl.InheritanceFlags]'ContainerInherit, ObjectInherit'
    foreach ($identity in $administrators, $system, $ServiceAccount) {
        $access = if ($identity -eq $ServiceAccount) { $Rights } else { 'FullControl' }
        $security.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new(
            $identity, $access, $inherit, 'None', 'Allow'
        ))
    }
    [IO.FileSystemAclExtensions]::Create([IO.DirectoryInfo]::new($Path), $security)
    Assert-OrdinaryPath $Path
}

function Assert-OrdinaryPath {
    param([string]$Path)
    $item = Get-Item -LiteralPath $Path -Force
    if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) {
        throw "Reparse points are not allowed for protected service paths: $Path"
    }
}

function Set-ProtectedOwner {
    param([string]$Path)
    $security = Get-Acl -LiteralPath $Path
    $security.SetOwner($administrators)
    Set-Acl -LiteralPath $Path -AclObject $security
}

function Assert-ProtectedFile {
    param(
        [string]$Path,
        [Security.Principal.SecurityIdentifier]$Writer
    )
    Assert-OrdinaryPath $Path
    $security = Get-Acl -LiteralPath $Path
    $owner = $security.GetOwner([Security.Principal.SecurityIdentifier])
    if ($owner -ne $administrators -and $owner -ne $system) {
        throw "Unexpected owner of protected service path: $Path"
    }
    $writeRights = [Security.AccessControl.FileSystemRights](
        'WriteData, AppendData, WriteAttributes, WriteExtendedAttributes, DeleteSubdirectoriesAndFiles, Delete, ChangePermissions, TakeOwnership'
    )
    $writerGranted = $false
    foreach ($rule in $security.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier])) {
        if ($rule.AccessControlType -eq 'Allow' -and ($rule.FileSystemRights -band $writeRights) -and
            $rule.IdentityReference -ne $administrators -and $rule.IdentityReference -ne $system -and
            $rule.IdentityReference -ne $Writer) {
            throw "Non-administrative write access to protected service path: $Path"
        }
        $modify = [Security.AccessControl.FileSystemRights]::Modify
        if ($rule.AccessControlType -eq 'Allow' -and $rule.IdentityReference -eq $Writer -and
            ($rule.FileSystemRights -band $modify) -eq $modify) {
            $writerGranted = $true
        }
    }
    if ($Writer -and -not $writerGranted) {
        throw "Service identity lacks data access: $Path"
    }
}

function Assert-ServiceRegistration {
    $registered = Get-ItemProperty -LiteralPath $registryPath
    if ($registered.ImagePath -cne $serverCommand -or $registered.ObjectName -ine 'NT AUTHORITY\LocalService') {
        throw "SCM service $name does not match this helper's protected configuration"
    }
}

function Assert-ManagedService {
    Assert-ServiceRegistration
    if ($Action -eq 'Status') { return }
    Assert-ProtectedFile $root
    Assert-ProtectedFile $server
    Assert-ProtectedFile $library
    Assert-ProtectedFile $config
    Assert-ProtectedFile $state
    Assert-ProtectedFile $data (Get-ServiceSid)
}

function Get-ServiceSid {
    [Security.Principal.NTAccount]::new('NT SERVICE', $name).Translate(
        [Security.Principal.SecurityIdentifier]
    )
}

function Assert-EngineArtifact {
    param([string]$Path, $Record)
    Assert-ProtectedFile $Path
    if ((Get-Item -LiteralPath $Path).Length -ne $Record.size -or
        (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash -ine $Record.sha256) {
        throw "Protected engine file does not match the release manifest: $Path"
    }
}

function Wait-ManagedService {
    param([ServiceProcess.ServiceControllerStatus]$Status)
    (Get-Service -Name $name -ErrorAction Stop).WaitForStatus($Status, [TimeSpan]::FromMinutes(2))
}

if ($Action -ne 'Status') {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw 'Open a trusted administrator PowerShell session for this system service operation.'
    }
}

if ($Action -eq 'Install') {
    if (-not $EngineBundle -or -not $ManifestSha256) {
        throw 'Supply -EngineBundle and the separately verified -ManifestSha256 for the expected release.'
    }
    $registered = Test-Path -LiteralPath $registryPath
    if ($registered) {
        Assert-ServiceRegistration
        $service = Get-Service -Name $name
        if ($service.Status -ne 'Stopped' -or $service.StartType -ne 'Disabled' -or
            -not (Test-Path -LiteralPath $state)) {
            throw "SCM service $name already exists."
        }
    }
    $manifest = [IO.File]::ReadAllBytes((Join-Path $EngineBundle 'manifest.json'))
    $manifestDigest = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($manifest))
    if ($manifestDigest -ine $ManifestSha256) { throw 'Engine manifest SHA-256 mismatch.' }
    $release = [Text.Encoding]::UTF8.GetString($manifest) | ConvertFrom-Json
    $architecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString().ToLowerInvariant()
    $architecture = if ($architecture -eq 'x64') { 'amd64' } else { $architecture }
    if ($release.schema_version -ne 1 -or $release.product -ne 'kerniva-engine' -or $release.os -ne 'windows' -or
        $release.arch -ne $architecture) {
        throw 'The manifest does not describe a Windows engine for this architecture.'
    }
    for ($parent = [IO.DirectoryInfo]::new($programFiles); $null -ne $parent; $parent = $parent.Parent) {
        Assert-OrdinaryPath $parent.FullName
    }
    $files = 'kerniva-server.exe', 'kerniva_engine.dll'
    $stateJson = [ordered]@{
        schema_version = 1
        service_name = $name
        manifest_sha256 = $manifestDigest.ToLowerInvariant()
        port = $Port
    } | ConvertTo-Json -Compress
    $configJson = [ordered]@{
        host = '127.0.0.1'
        port = $Port
        network_type = 'std'
        storage_backend = 'go-log'
        persistence_enabled = $true
        cold_start_strategy = 'load_all'
        data_dir = $data
        log_path = (Join-Path $data 'kerniva.log')
    } | ConvertTo-Json -Compress
    if (-not (Test-Path -LiteralPath $root)) {
        $staged = $root + '.pending-' + [guid]::NewGuid().ToString('N')
        $ownsStage = $false
        try {
            New-ProtectedDirectory $staged $localService 'ReadAndExecute'
            $ownsStage = $true
            foreach ($file in $files) {
                $destination = Join-Path $staged $file
                Copy-Item -LiteralPath (Join-Path $EngineBundle $file) -Destination $destination
                Assert-OrdinaryPath $destination
                Set-ProtectedOwner $destination
                Assert-EngineArtifact $destination $release.files.PSObject.Properties[$file].Value
            }
            $stagedState = Join-Path $staged 'service-state.json'
            $stagedConfig = Join-Path $staged 'instance.json'
            [IO.File]::WriteAllText($stagedState, $stateJson)
            [IO.File]::WriteAllText($stagedConfig, $configJson)
            Set-ProtectedOwner $stagedState
            Set-ProtectedOwner $stagedConfig
            [IO.Directory]::Move($staged, $root)
            $ownsStage = $false
        } finally {
            if ($ownsStage) {
                foreach ($file in $files + @('service-state.json', 'instance.json')) {
                    [IO.File]::Delete((Join-Path $staged $file))
                }
                [IO.Directory]::Delete($staged, $false)
            }
        }
    }

    Assert-ProtectedFile $root
    Assert-ProtectedFile $state
    Assert-ProtectedFile $config
    if ([IO.File]::ReadAllText($state) -cne $stateJson -or [IO.File]::ReadAllText($config) -cne $configJson) {
        throw "Retained service identity or configuration differs. Use the same release, name, and port: $root"
    }
    foreach ($file in $files) {
        Assert-EngineArtifact (Join-Path $root $file) $release.files.PSObject.Properties[$file].Value
    }

    $created = $false
    try {
        if (-not $registered) {
            Invoke-Manager @('create', $name, 'binPath=', $serverCommand, 'start=', 'disabled',
                             'obj=', 'NT AUTHORITY\LocalService', 'DisplayName=', "Kerniva ($Instance)")
            $created = $true
        }
        Invoke-Manager @('sidtype', $name, 'unrestricted')
        $serviceSid = Get-ServiceSid
        if (-not (Test-Path -LiteralPath $data)) {
            New-ProtectedDirectory $data $serviceSid 'Modify'
        }
        Assert-ManagedService
        Set-Service -Name $name -StartupType Manual
    } catch {
        if ($created) {
            try { Invoke-Manager @('delete', $name) } catch { Write-Warning $_ }
        }
        throw
    }
} else {
    Assert-ManagedService
    switch ($Action) {
        'Start' { Start-Service -Name $name; Wait-ManagedService Running }
        'Stop' { Stop-Service -Name $name; Wait-ManagedService Stopped }
        'Restart' {
            Stop-Service -Name $name
            Wait-ManagedService Stopped
            Start-Service -Name $name
            Wait-ManagedService Running
        }
        'EnableAutoStart' { Set-Service -Name $name -StartupType Automatic }
        'DisableAutoStart' { Set-Service -Name $name -StartupType Manual }
        'Uninstall' {
            if ((Get-Service -Name $name).Status -ne 'Stopped') {
                Stop-Service -Name $name
                Wait-ManagedService Stopped
            }
            Invoke-Manager @('delete', $name)
            Write-Output "Service removed. Protected files and data remain in $root"
            return
        }
    }
}

Get-Service -Name $name | Select-Object Name, Status, StartType
