# Деплой ERP → Amvera FloraFlowERP
# Запуск из корня репозитория FloraFlow:
#   .\scripts\deploy-erp.ps1 "описание правок"
#
# Важно: Amvera собирает код из git.amvera.ru, НЕ из GitHub.
# Push только в origin (GitHub) контейнер не обновляет.

param(
    [string]$Message = "deploy erp"
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

Write-Host "=== FloraFlow ERP ===" -ForegroundColor Cyan
git status --short

git add -A
git diff --cached --quiet
$hasStaged = ($LASTEXITCODE -ne 0)

if ($hasStaged) {
    git commit -m $Message
} else {
    Write-Host "Нечего коммитить — пушим текущий HEAD." -ForegroundColor Yellow
}

# GitHub (бэкап / история)
git push origin master

# Amvera (реальный деплой контейнера)
$amveraUrl = "https://git.amvera.ru/warchesko/floraflowerp"
$hasAmvera = git remote | Select-String -Pattern "^amvera$"
if (-not $hasAmvera) {
    git remote add amvera $amveraUrl
}
git push amvera master

Write-Host "OK: push в GitHub + Amvera. Смотрите лог сборки — HEAD должен быть как локальный master." -ForegroundColor Green
