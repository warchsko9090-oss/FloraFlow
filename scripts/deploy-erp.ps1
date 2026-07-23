# Деплой ERP → Amvera FloraFlowERP
# Запуск из корня репозитория FloraFlow:
#   .\scripts\deploy-erp.ps1 "описание правок"

param(
    [string]$Message = "deploy erp"
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

Write-Host "=== FloraFlow ERP ===" -ForegroundColor Cyan
git status --short

git add -A
git diff --cached --quiet
if ($LASTEXITCODE -eq 0) {
    Write-Host "Нечего коммитить." -ForegroundColor Yellow
    exit 0
}

git commit -m $Message
git push origin master

Write-Host "OK: push выполнен. Amvera FloraFlowERP пересоберёт контейнер." -ForegroundColor Green
