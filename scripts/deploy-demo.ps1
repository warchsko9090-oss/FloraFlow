# Deploy DEMO ERP -> Amvera ff-demo (параллельно с продом ff-m1).
# Прод (ff-m1) НЕ трогаем. На контейнере должны быть переменные:
#   DEMO_MODE=1
#   DISABLE_SCHEDULER=1
#   DISABLE_AI_SEED=1
#   TG_USE_POLLING=0
#   SECRET_KEY=... (свой, не прод)
# Без DATABASE_URL — SQLite на /data (пустая → автосид при старте).
#
#   .\scripts\deploy-demo.ps1 "demo: visible TG cards + parallel amvera"

param(
    [string]$Message = "deploy demo erp"
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

Write-Host "=== FloraFlow DEMO -> ff-demo (parallel to ff-m1 prod) ===" -ForegroundColor Cyan
git status --short

git add -A
git diff --cached --quiet
$hasStaged = ($LASTEXITCODE -ne 0)

if ($hasStaged) {
    git commit -m $Message
} else {
    Write-Host "Nothing to commit - pushing current HEAD." -ForegroundColor Yellow
}

# GitHub backup
git push origin master

# Amvera Miami — отдельный проект, не ff-m1
$amveraUrl = "https://git.mia0.amvera.ru/warchesko/ff-demo"
$hasDemo = git remote | Select-String -Pattern "^amvera-demo$"
if (-not $hasDemo) {
    git remote add amvera-demo $amveraUrl
} else {
    git remote set-url amvera-demo $amveraUrl
}

git push amvera-demo master

$head = git rev-parse --short HEAD
Write-Host "OK: pushed GitHub + Amvera ff-demo. HEAD=$head" -ForegroundColor Green
Write-Host "Prod ff-m1 was NOT pushed by this script." -ForegroundColor Yellow
Write-Host "Amvera env required: DEMO_MODE=1 DISABLE_SCHEDULER=1 DISABLE_AI_SEED=1 TG_USE_POLLING=0" -ForegroundColor Yellow
