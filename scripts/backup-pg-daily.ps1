#Requires -Version 5.1
<#
.SYNOPSIS
  Daily Postgres FloraFlow backup to this PC.

.DESCRIPTION
  1) pg_dump to a temp file
  2) on success (exit 0 + non-empty) atomically replace floraflow-latest.dump
  3) on error leave previous latest untouched
  Optionally keeps dated snapshot floraflow-YYYY-MM-DD.dump

  Secrets only in local file (not in git):
    %USERPROFILE%\.floraflow\backup-pg.env
  or next to script: scripts\backup-pg.env

.EXAMPLE
  .\scripts\backup-pg-daily.ps1
  .\scripts\backup-pg-daily.ps1 -RegisterSchedule
  .\scripts\backup-pg-daily.ps1 -UnregisterSchedule
#>
param(
    [string]$BackupDir = '',
    [string]$EnvFile = '',
    [string]$DatabaseUrl = $env:FLORAFLOW_BACKUP_DATABASE_URL,
    [int]$KeepDaily = 14,
    [switch]$RegisterSchedule,
    [switch]$UnregisterSchedule,
    [string]$ScheduleTime = '03:30'
)

$ErrorActionPreference = 'Stop'
$TaskName = 'FloraFlow-Postgres-Daily-Backup'

function Write-Log([string]$Message, [string]$Level = 'INFO') {
    $line = '{0:yyyy-MM-dd HH:mm:ss} [{1}] {2}' -f (Get-Date), $Level, $Message
    Write-Host $line
    if ($script:LogPath) {
        Add-Content -LiteralPath $script:LogPath -Value $line -Encoding UTF8
    }
}

function Get-PgDumpPath {
    $cmd = Get-Command pg_dump -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    # Prefer newest: server is PG17 on Amvera Miami; pg_dump must be >= server major
    foreach ($ver in 18, 17, 16, 15, 14) {
        $p = "C:\Program Files\PostgreSQL\$ver\bin\pg_dump.exe"
        if (Test-Path -LiteralPath $p) { return $p }
    }
    throw 'pg_dump not found. Install PostgreSQL 17+ client tools (winget install PostgreSQL.PostgreSQL.17).'
}

function Resolve-EnvFile {
    param([string]$Explicit)
    $candidates = @()
    if ($Explicit) { $candidates += $Explicit }
    $candidates += (Join-Path $env:USERPROFILE '.floraflow\backup-pg.env')
    $candidates += (Join-Path $PSScriptRoot 'backup-pg.env')
    $candidates += (Join-Path $PSScriptRoot '..\.env.backup-pg')
    foreach ($c in $candidates) {
        if ($c -and (Test-Path -LiteralPath $c)) { return (Resolve-Path -LiteralPath $c).Path }
    }
    return $null
}

function Import-DotEnv([string]$Path) {
    Get-Content -LiteralPath $Path -Encoding UTF8 | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith('#')) { return }
        $i = $line.IndexOf('=')
        if ($i -lt 1) { return }
        $k = $line.Substring(0, $i).Trim()
        $v = $line.Substring($i + 1).Trim()
        if (($v.StartsWith('"') -and $v.EndsWith('"')) -or ($v.StartsWith("'") -and $v.EndsWith("'"))) {
            $v = $v.Substring(1, $v.Length - 2)
        }
        [Environment]::SetEnvironmentVariable($k, $v, 'Process')
        Set-Item -Path "Env:$k" -Value $v
    }
}

function Convert-ToPgUri([string]$Url) {
    $u = if ($null -eq $Url) { '' } else { ([string]$Url).Trim() }
    if ($u.StartsWith('postgres://')) {
        $u = 'postgresql://' + $u.Substring('postgres://'.Length)
    }
    if (-not $u.StartsWith('postgresql://')) {
        throw 'Need URL like postgresql://user:pass@host:5432/FloraFlow?sslmode=require'
    }
    # Internal Amvera host is not reachable from a PC
    if ($u -match '@cnpg-[^/:]+:' -or $u -match '@amvera-[^/:]+:') {
        throw 'URL uses internal Amvera host. Use external POSTGRES domain (*.tcp-*.amvera.tech) and sslmode=require.'
    }
    if ($u -notmatch 'sslmode=') {
        $sep = if ($u.Contains('?')) { '&' } else { '?' }
        $u = "$u${sep}sslmode=require"
    }
    return $u
}

function Register-BackupTask {
    $scriptPath = $PSCommandPath
    if (-not $scriptPath) { $scriptPath = $MyInvocation.MyCommand.Path }
    $action = New-ScheduledTaskAction -Execute 'powershell.exe' `
        -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""
    $trigger = New-ScheduledTaskTrigger -Daily -At $ScheduleTime
    $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
    $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
        -Settings $settings -Principal $principal -Force | Out-Null
    Write-Host "Task '$TaskName' created: daily at $ScheduleTime" -ForegroundColor Green
    Write-Host "Check: Get-ScheduledTask -TaskName '$TaskName'"
}

function Unregister-BackupTask {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Task '$TaskName' removed (if it existed)." -ForegroundColor Yellow
}

if ($UnregisterSchedule) {
    Unregister-BackupTask
    return
}
if ($RegisterSchedule) {
    Register-BackupTask
    return
}

# --- backup run ---
$resolvedEnv = Resolve-EnvFile -Explicit $EnvFile
if ($resolvedEnv) {
    Import-DotEnv $resolvedEnv
    if (-not $DatabaseUrl) {
        $DatabaseUrl = $env:FLORAFLOW_BACKUP_DATABASE_URL
    }
    if (-not $DatabaseUrl) {
        $DatabaseUrl = $env:DATABASE_URL
    }
}

if (-not $BackupDir) {
    if ($env:FLORAFLOW_BACKUP_DIR) {
        $BackupDir = $env:FLORAFLOW_BACKUP_DIR
    } else {
        $BackupDir = Join-Path $PSScriptRoot '..\backups\pg-daily'
    }
}
$BackupDir = [System.IO.Path]::GetFullPath($BackupDir)
New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null
$script:LogPath = Join-Path $BackupDir 'backup.log'

if (-not $DatabaseUrl) {
    Write-Log 'No DB URL. Create env file with FLORAFLOW_BACKUP_DATABASE_URL=...' 'ERROR'
    Write-Host @"

Create file:
  $env:USERPROFILE\.floraflow\backup-pg.env

Contents (example):
  FLORAFLOW_BACKUP_DATABASE_URL=postgresql://USER:PASS@HOST:5432/FloraFlow?sslmode=require
  FLORAFLOW_BACKUP_DIR=$BackupDir

HOST = external Amvera POSTGRES domain (not cnpg-...-rw).
"@ -ForegroundColor Yellow
    exit 2
}

$uri = Convert-ToPgUri $DatabaseUrl
$pgDump = Get-PgDumpPath
$stamp = Get-Date -Format 'yyyy-MM-dd'
$latest = Join-Path $BackupDir 'floraflow-latest.dump'
$dated = Join-Path $BackupDir ("floraflow-{0}.dump" -f $stamp)
$tmp = Join-Path $BackupDir ("floraflow-{0}.{1}.tmp" -f $stamp, [guid]::NewGuid().ToString('N').Substring(0, 8))

Write-Log "Backup start -> $BackupDir"
Write-Log "pg_dump: $pgDump"

try {
    if (Test-Path -LiteralPath $tmp) { Remove-Item -LiteralPath $tmp -Force }
    # Start-Process splits URI; call directly
    & $pgDump --format=custom --no-owner --no-acl --dbname=$uri --file=$tmp
    if ($LASTEXITCODE -ne 0) {
        throw "pg_dump exited with code $LASTEXITCODE"
    }
    if (-not (Test-Path -LiteralPath $tmp)) {
        throw 'Temp dump file was not created'
    }
    $size = (Get-Item -LiteralPath $tmp).Length
    if ($size -lt 1024) {
        throw "Dump too small ($size bytes) - not replacing latest"
    }

    # Atomic latest replace: only after successful dump
    $latestTmp = "$latest.partial"
    if (Test-Path -LiteralPath $latestTmp) { Remove-Item -LiteralPath $latestTmp -Force }
    Copy-Item -LiteralPath $tmp -Destination $latestTmp -Force
    Move-Item -LiteralPath $latestTmp -Destination $latest -Force

    Copy-Item -LiteralPath $tmp -Destination $dated -Force
    Remove-Item -LiteralPath $tmp -Force

    Write-Log ("OK latest={0} dated={1} size={2:N0} bytes" -f (Split-Path $latest -Leaf), (Split-Path $dated -Leaf), $size)

    if ($KeepDaily -gt 0) {
        $old = Get-ChildItem -LiteralPath $BackupDir -Filter 'floraflow-????-??-??.dump' |
            Sort-Object Name -Descending |
            Select-Object -Skip $KeepDaily
        foreach ($f in $old) {
            Remove-Item -LiteralPath $f.FullName -Force
            Write-Log "Removed old snapshot $($f.Name)"
        }
    }
    exit 0
}
catch {
    Write-Log $_.Exception.Message 'ERROR'
    if (Test-Path -LiteralPath $tmp) {
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path -LiteralPath "$latest.partial") {
        Remove-Item -LiteralPath "$latest.partial" -Force -ErrorAction SilentlyContinue
    }
    Write-Log 'Previous floraflow-latest.dump NOT changed' 'WARN'
    exit 1
}
