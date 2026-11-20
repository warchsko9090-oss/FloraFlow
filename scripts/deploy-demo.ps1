# Deploy DEMO ERP -> Amvera FloraFlow1 (Moscow), параллельно с продом ff-m1 (Miami).
# Прод (ff-m1) НЕ трогаем. На контейнере FloraFlow1 задайте переменные:
#   DEMO_MODE=1
#   DISABLE_SCHEDULER=1
#   DISABLE_AI_SEED=1
#   TG_USE_POLLING=0
#   SECRET_KEY=demo-floraflow-client-preview
# Без DATABASE_URL — SQLite на /data (пустая → автосид при старте).
#
#   .\scripts\deploy-demo.ps1 "demo erp parallel to prod"

param(
    [string]$Message = "deploy demo erp"
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

Write-Host "=== FloraFlow DEMO -> FloraFlow1 (parallel to ff-m1 prod) ===" -ForegroundColor Cyan
git status --short

git add -A
git diff --cached --quiet
$hasStaged = ($LASTEXITCODE -ne 0)

if ($hasStaged) {
    git commit -m $Message
} else {
    Write-Host "Nothing to commit - pushing current HEAD." -ForegroundColor Yellow
}

git push origin master

# Amvera Moscow — FloraFlow1 (не ff-m1)
$amveraUrl = "https://git.msk1.amvera.ru/warchesko/floraflow1"
$hasDemo = git remote | Select-String -Pattern "^amvera-demo$"
if (-not $hasDemo) {
    git remote add amvera-demo $amveraUrl
} else {
    git remote set-url amvera-demo $amveraUrl
}

git push amvera-demo master

$head = git rev-parse --short HEAD
Write-Host "OK: pushed GitHub + Amvera FloraFlow1. HEAD=$head" -ForegroundColor Green
Write-Host "Prod ff-m1 was NOT pushed by this script." -ForegroundColor Yellow
Write-Host "Amvera env: DEMO_MODE=1 DISABLE_SCHEDULER=1 DISABLE_AI_SEED=1 TG_USE_POLLING=0" -ForegroundColor Yellow
