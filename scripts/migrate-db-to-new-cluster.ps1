#Requires -Version 5.1
<#
.SYNOPSIS
  Миграция FloraFlow ERP: старый Amvera Postgres (толстый диск) → новый кластер 15 ГБ.

.DESCRIPTION
  Делает pg_dump со старой БД и pg_restore в новую.
  Приложения (ERP + shop) должны быть на паузе во время дампа/переключения.

.EXAMPLE
  # 1) Дамп со старой БД
  .\scripts\migrate-db-to-new-cluster.ps1 -Action dump `
    -OldUrl "postgresql://user:pass@old-host:5432/FloraFlow?sslmode=require"

  # 2) Restore в новую
  .\scripts\migrate-db-to-new-cluster.ps1 -Action restore `
    -NewUrl "postgresql://user:pass@new-host:5432/FloraFlow?sslmode=require"

  # 3) Сверка COUNT
  .\scripts\migrate-db-to-new-cluster.ps1 -Action verify `
    -OldUrl "..." -NewUrl "..."

  # 4) Показать новый DATABASE_URL для Amvera (оба проекта)
  .\scripts\migrate-db-to-new-cluster.ps1 -Action print-url -NewUrl "..."
#>
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('dump', 'restore', 'verify', 'print-url', 'check-tools')]
    [string]$Action,

    [string]$OldUrl = $env:OLD_DATABASE_URL,
    [string]$NewUrl = $env:NEW_DATABASE_URL,

    [string]$DumpPath = (Join-Path $PSScriptRoot '..\backups\floraflow.dump'),

    # Роль на новой БД (имя пользователя из Amvera)
    [string]$NewRole = ''
)

$ErrorActionPreference = 'Stop'

function Get-PgBin {
    $names = @('pg_dump', 'pg_restore', 'psql')
    $found = @{}
    foreach ($n in $names) {
        $cmd = Get-Command $n -ErrorAction SilentlyContinue
        if ($cmd) {
            $found[$n] = $cmd.Source
            continue
        }
        $candidates = @(
            "C:\Program Files\PostgreSQL\16\bin\$n.exe",
            "C:\Program Files\PostgreSQL\15\bin\$n.exe",
            "C:\Program Files\PostgreSQL\14\bin\$n.exe",
            "C:\Program Files\PostgreSQL\17\bin\$n.exe"
        )
        foreach ($c in $candidates) {
            if (Test-Path $c) { $found[$n] = $c; break }
        }
    }
    return $found
}

function Convert-DatabaseUrlToLibpq {
    param([Parameter(Mandatory = $true)][string]$Url)
    # postgresql://user:pass@host:5432/db?sslmode=require
    # → env-friendly pieces; we use connection URI form for pg tools
    $u = $Url.Trim()
    if ($u.StartsWith('postgres://')) {
        $u = 'postgresql://' + $u.Substring('postgres://'.Length)
    }
    if (-not ($u.StartsWith('postgresql://') -or $u.StartsWith('postgres://'))) {
        throw "Ожидался URL вида postgresql://user:pass@host:5432/FloraFlow"
    }
    return $u
}

function Assert-Url([string]$Url, [string]$Label) {
    if ([string]::IsNullOrWhiteSpace($Url)) {
        throw "Не задан $Label. Передайте -OldUrl/-NewUrl или env OLD_DATABASE_URL/NEW_DATABASE_URL"
    }
}

$bins = Get-PgBin

switch ($Action) {
    'check-tools' {
        Write-Host "=== PostgreSQL client tools ===" -ForegroundColor Cyan
        foreach ($k in @('pg_dump', 'pg_restore', 'psql')) {
            if ($bins.ContainsKey($k)) {
                Write-Host "OK  $k = $($bins[$k])" -ForegroundColor Green
            } else {
                Write-Host "MISSING  $k" -ForegroundColor Red
            }
        }
        if (-not $bins.ContainsKey('pg_dump')) {
            Write-Host ""
            Write-Host "Установите клиент PostgreSQL (достаточно Command Line Tools):" -ForegroundColor Yellow
            Write-Host "  winget install --id PostgreSQL.PostgreSQL.16 -e"
            Write-Host "или скачайте с https://www.enterprisedb.com/downloads/postgres-postgresql-downloads"
            exit 1
        }
        exit 0
    }

    'dump' {
        Assert-Url $OldUrl 'OldUrl'
        if (-not $bins.ContainsKey('pg_dump')) { throw 'pg_dump не найден. Сначала: -Action check-tools' }
        $uri = Convert-DatabaseUrlToLibpq $OldUrl
        $dir = Split-Path -Parent $DumpPath
        if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }

        Write-Host "Дамп → $DumpPath" -ForegroundColor Cyan
        Write-Host "ВАЖНО: ERP и shop должны быть на паузе в Amvera." -ForegroundColor Yellow
        & $bins['pg_dump'] --format=custom --no-owner --no-acl --dbname=$uri --file=$DumpPath
        if ($LASTEXITCODE -ne 0) { throw "pg_dump завершился с кодом $LASTEXITCODE" }
        $size = (Get-Item $DumpPath).Length
        Write-Host ("Готово. Размер дампа: {0:N1} KB" -f ($size / 1KB)) -ForegroundColor Green
    }

    'restore' {
        Assert-Url $NewUrl 'NewUrl'
        if (-not $bins.ContainsKey('pg_restore')) { throw 'pg_restore не найден' }
        if (-not (Test-Path $DumpPath)) { throw "Нет файла дампа: $DumpPath" }
        $uri = Convert-DatabaseUrlToLibpq $NewUrl

        $restoreArgs = @(
            '--no-owner',
            '--no-acl',
            '--dbname=' + $uri,
            $DumpPath
        )
        if ($NewRole) {
            $restoreArgs = @("--role=$NewRole") + $restoreArgs
        }

        Write-Host "Restore → новая БД" -ForegroundColor Cyan
        & $bins['pg_restore'] @restoreArgs
        # pg_restore часто возвращает 1 при warning'ах — проверим через verify
        Write-Host "pg_restore exit=$LASTEXITCODE (warnings допустимы). Запустите -Action verify" -ForegroundColor Yellow
    }

    'verify' {
        Assert-Url $OldUrl 'OldUrl'
        Assert-Url $NewUrl 'NewUrl'
        if (-not $bins.ContainsKey('psql')) { throw 'psql не найден' }

        $tables = @('action_log', 'order', 'order_item', 'tg_task', 'expense', 'client')
        $oldUri = Convert-DatabaseUrlToLibpq $OldUrl
        $newUri = Convert-DatabaseUrlToLibpq $NewUrl

        Write-Host ("{0,-22} {1,12} {2,12} {3}" -f 'table', 'old', 'new', 'ok?') -ForegroundColor Cyan
        $allOk = $true
        foreach ($t in $tables) {
            $q = "SELECT COUNT(*) FROM `"$t`";"
            $oldC = & $bins['psql'] --dbname=$oldUri -t -A -c $q 2>$null
            $newC = & $bins['psql'] --dbname=$newUri -t -A -c $q 2>$null
            $oldC = ($oldC | Out-String).Trim()
            $newC = ($newC | Out-String).Trim()
            $ok = ($oldC -eq $newC) -and ($oldC -match '^\d+$')
            if (-not $ok) { $allOk = $false }
            $mark = if ($ok) { 'YES' } else { 'NO' }
            Write-Host ("{0,-22} {1,12} {2,12} {3}" -f $t, $oldC, $newC, $mark)
        }
        if ($allOk) {
            Write-Host "Сверка OK — можно переключать DATABASE_URL" -ForegroundColor Green
            exit 0
        } else {
            Write-Host "Сверка НЕ совпала — не переключайте URL" -ForegroundColor Red
            exit 1
        }
    }

    'print-url' {
        Assert-Url $NewUrl 'NewUrl'
        $uri = Convert-DatabaseUrlToLibpq $NewUrl
        Write-Host ""
        Write-Host "Вставьте этот DATABASE_URL в Amvera → переменные:" -ForegroundColor Cyan
        Write-Host "  1) проект FloraFlowERP"
        Write-Host "  2) проект knyajestvo (shop)"
        Write-Host ""
        Write-Host $uri -ForegroundColor Green
        Write-Host ""
        Write-Host "Затем запустите оба приложения и проверьте логин / заказы / витрину."
        Write-Host "Старый floraflowerp-db удалите через 24–48 часов (см. decommission-old-db.md)."
    }
}
