# Deploy ERP -> Amvera ff-m1 (Miami)
# From FloraFlow repo root:
#   .\scripts\deploy-erp.ps1 "change description"
#
# Miami ff-m1 pulls from git.mia0.amvera.ru (NOT GitHub, NOT git.amvera.ru).
# Push to origin alone does not update the container.

param(
    [string]$Message = "deploy erp"
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

Write-Host "=== FloraFlow ERP -> ff-m1 ===" -ForegroundColor Cyan
git status --short

git add -A
git diff --cached --quiet
$hasStaged = ($LASTEXITCODE -ne 0)

if ($hasStaged) {
    git commit -m $Message
} else {
    Write-Host "Nothing to commit - pushing current HEAD." -ForegroundColor Yellow
}

# GitHub (backup / history)
git push origin master

# Amvera Miami (real ff-m1 deploy)
$amveraUrl = "https://git.mia0.amvera.ru/warchesko/ff-m1"
$hasAmvera = git remote | Select-String -Pattern "^amvera$"
if (-not $hasAmvera) {
    git remote add amvera $amveraUrl
} else {
    git remote set-url amvera $amveraUrl
}

git push amvera master

$head = git rev-parse --short HEAD
Write-Host "OK: pushed GitHub + Amvera ff-m1. Build log HEAD should be $head." -ForegroundColor Green
