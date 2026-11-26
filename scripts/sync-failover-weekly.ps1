#Requires -Version 5.1
<#
.SYNOPSIS
  Еженедельный снимок витрины → Amvera kdoor (/data) + опционально push кода.

.DESCRIPTION
  1) Экспорт SQLite + фото с проверкой целостности (MANIFEST sha256)
  2) Загрузка snapshot.tar.gz на kdoor POST /internal/failover-sync
  3) (опц.) git push кода в kdoor
  4) Проверка status на Amvera

  Секреты: %USERPROFILE%\.floraflow\failover-sync.env
    FAILOVER_SYNC_TOKEN=...
    FAILOVER_SYNC_URL=https://kdoor-warchesko.msk1.amvera.tech/internal/failover-sync
    # optional:
    KDOOR_DEPLOY_DIR=C:\Users\warch\Documents\FloraFlow\_kdoor-deploy
    PUSH_CODE=1

.EXAMPLE
  .\scripts\sync-failover-weekly.ps1
  .\scripts\sync-failover-weekly.ps1 -RegisterSchedule
#>
param(
    [string]$EnvFile = '',
    [string]$RepoRoot = '',
    [switch]$RegisterSchedule,
    [switch]$UnregisterSchedule,
    [switch]$SkipUpload,
    [switch]$SkipCodePush,
    [string]$ScheduleTime = '00:00'
)

$ErrorActionPreference = 'Stop'
$TaskName = 'FloraFlow-Failover-Weekly-Sync'

function Write-Log([string]$Message, [string]$Level = 'INFO') {
    $line = '{0:yyyy-MM-dd HH:mm:ss} [{1}] {2}' -f (Get-Date), $Level, $Message
    Write-Host $line
    if ($script:LogPath) {
        Add-Content -LiteralPath $script:LogPath -Value $line -Encoding UTF8
    }
}

function Resolve-EnvFile([string]$Explicit) {
    $candidates = @()
    if ($Explicit) { $candidates += $Explicit }
    $candidates += (Join-Path $env:USERPROFILE '.floraflow\failover-sync.env')
    $candidates += (Join-Path $PSScriptRoot 'failover-sync.env')
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

function Register-WeeklyTask {
    $scriptPath = $PSCommandPath
    if (-not $scriptPath) { $scriptPath = $MyInvocation.MyCommand.Path }
    $action = New-ScheduledTaskAction -Execute 'powershell.exe' `
        -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""
    # Воскресенье 00:00 (локальное время ПК)
    $trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At $ScheduleTime
    $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 4)
    $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
        -Settings $settings -Principal $principal -Force | Out-Null
    Write-Host "Task '$TaskName': every Sunday at $ScheduleTime" -ForegroundColor Green
    Write-Host "Check: Get-ScheduledTask -TaskName '$TaskName' | Get-ScheduledTaskInfo"
}

function Unregister-WeeklyTask {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Task '$TaskName' removed (if it existed)." -ForegroundColor Yellow
}

if ($UnregisterSchedule) { Unregister-WeeklyTask; return }
if ($RegisterSchedule) { Register-WeeklyTask; return }

# --- sync run ---
if (-not $RepoRoot) {
    $RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
}
$logDir = Join-Path $RepoRoot 'backups\failover-weekly'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$script:LogPath = Join-Path $logDir 'sync.log'

$resolvedEnv = Resolve-EnvFile -Explicit $EnvFile
if ($resolvedEnv) {
    Import-DotEnv $resolvedEnv
    Write-Log "Env: $resolvedEnv"
} else {
    Write-Log 'No failover-sync.env — upload may fail without FAILOVER_SYNC_TOKEN' 'WARN'
}

$token = $env:FAILOVER_SYNC_TOKEN
$syncUrl = $env:FAILOVER_SYNC_URL
if (-not $syncUrl) {
    $syncUrl = 'https://kdoor-warchesko.msk1.amvera.tech/internal/failover-sync'
}
$pushCode = ($env:PUSH_CODE -eq '1') -and (-not $SkipCodePush)
$deployDir = $env:KDOOR_DEPLOY_DIR
if (-not $deployDir) {
    $deployDir = Join-Path $RepoRoot '_kdoor-deploy'
}

$py = Get-Command python -ErrorAction SilentlyContinue
if (-not $py) { $py = Get-Command py -ErrorAction SilentlyContinue }
if (-not $py) { throw 'python not found in PATH' }

$outDir = Join-Path $RepoRoot 'amvera-shop-door\data-snapshot'
$archive = Join-Path $RepoRoot 'amvera-shop-door\snapshot.tar.gz'

Write-Log '=== Weekly failover sync start ==='
Write-Log "Repo: $RepoRoot"

# 1) Export + integrity
Write-Log 'Export snapshot (DB + photos + MANIFEST)…'
Push-Location $RepoRoot
try {
    & $py.Source scripts\export-shop-failover-snapshot.py --out $outDir --pack
    if ($LASTEXITCODE -ne 0) { throw "export exited $LASTEXITCODE" }
} finally {
    Pop-Location
}
if (-not (Test-Path -LiteralPath $archive)) {
    # export пишет рядом с out
    $alt = Join-Path (Split-Path $outDir -Parent) 'snapshot.tar.gz'
    if (Test-Path -LiteralPath $alt) { $archive = $alt }
}
if (-not (Test-Path -LiteralPath $archive)) {
    throw "snapshot.tar.gz not found after export"
}
$size = (Get-Item -LiteralPath $archive).Length
Write-Log ("Archive OK size={0:N0} bytes" -f $size)
if ($size -lt 1024) { throw 'Archive too small' }

# 2) Upload to Amvera
if ($SkipUpload) {
    Write-Log 'SkipUpload set — not uploading'
} elseif (-not $token) {
    Write-Log 'FAILOVER_SYNC_TOKEN empty — cannot upload. Fill .floraflow\failover-sync.env' 'ERROR'
    exit 2
} else {
    Write-Log "Upload → $syncUrl"
    $bytes = [System.IO.File]::ReadAllBytes($archive)
    $headers = @{
        Authorization = "Bearer $token"
        'Content-Type' = 'application/gzip'
    }
    try {
        $resp = Invoke-WebRequest -Uri $syncUrl -Method POST -Headers $headers -Body $bytes `
            -UseBasicParsing -TimeoutSec 1800
        Write-Log "Upload HTTP $($resp.StatusCode) $($resp.Content.Substring(0, [Math]::Min(300, $resp.Content.Length)))"
        if ($resp.StatusCode -ge 300) { throw "upload status $($resp.StatusCode)" }
    } catch {
        Write-Log $_.Exception.Message 'ERROR'
        if ($_.Exception.Response) {
            try {
                $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                Write-Log $sr.ReadToEnd() 'ERROR'
            } catch {}
        }
        exit 1
    }

    $statusUrl = $syncUrl.TrimEnd('/') + '/status'
    try {
        $st = Invoke-WebRequest -Uri $statusUrl -Headers @{ Authorization = "Bearer $token" } `
            -UseBasicParsing -TimeoutSec 60
        Write-Log "Status: $($st.Content.Substring(0, [Math]::Min(400, $st.Content.Length)))"
    } catch {
        Write-Log "Status check failed: $($_.Exception.Message)" 'WARN'
    }
}

# 3) Code push to kdoor (shop UI + door)
if ($pushCode -and (Test-Path -LiteralPath $deployDir)) {
    Write-Log "Push code via $deployDir"
    $copyList = @(
        'app', 'scripts', 'wsgi_door.py', 'amvera.door.yml', 'requirements.txt',
        'failover', 'amvera-shop-door\README.md'
    )
    foreach ($rel in $copyList) {
        $src = Join-Path $RepoRoot $rel
        $dst = Join-Path $deployDir $rel
        if (-not (Test-Path -LiteralPath $src)) { continue }
        if (Test-Path -LiteralPath $src -PathType Container) {
            if (Test-Path -LiteralPath $dst) { Remove-Item -LiteralPath $dst -Recurse -Force }
            Copy-Item -LiteralPath $src -Destination $dst -Recurse -Force
        } else {
            $parent = Split-Path $dst -Parent
            New-Item -ItemType Directory -Force -Path $parent | Out-Null
            Copy-Item -LiteralPath $src -Destination $dst -Force
        }
    }
    # amvera.yml = door config on Amvera
    Copy-Item -LiteralPath (Join-Path $RepoRoot 'amvera.door.yml') `
        -Destination (Join-Path $deployDir 'amvera.yml') -Force

    Push-Location $deployDir
    try {
        git add -A
        $pending = git status --porcelain
        if ($pending) {
            git -c user.email='failover-sync@floraflow.local' -c user.name='failover-sync' `
                commit -m "weekly failover sync $(Get-Date -Format 'yyyy-MM-dd')"
            git push origin master
            if ($LASTEXITCODE -ne 0) { throw "git push failed: $LASTEXITCODE" }
            Write-Log 'Code pushed to kdoor'
        } else {
            Write-Log 'Code unchanged — skip push'
        }
    } finally {
        Pop-Location
    }
} elseif ($pushCode) {
    Write-Log "KDOOR_DEPLOY_DIR missing: $deployDir" 'WARN'
}

Write-Log '=== Weekly failover sync done ==='
exit 0
