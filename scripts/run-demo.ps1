# Локальный демо-стенд ERP для показа клиенту (отдельная БД, без прод-данных).
# Из корня репозитория:
#   .\scripts\run-demo.ps1
#   .\scripts\run-demo.ps1 -Reset
#   .\scripts\run-demo.ps1 -Port 5001

param(
    [switch]$Reset,
    [int]$Port = 5001
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

$demoDb = Join-Path (Get-Location) "demo_nursery.db"
$needSeed = $Reset -or -not (Test-Path $demoDb)

if ($needSeed) {
    Write-Host "=== Seed demo DB ===" -ForegroundColor Cyan
    $seedArgs = @("tools/demo_seed.py", "--year", "2026", "--seed", "2026")
    if ($Reset -or -not (Test-Path $demoDb)) {
        $seedArgs += "--reset"
    }
    python @seedArgs
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

$uri = "sqlite:///" + ($demoDb -replace "\\", "/")
$env:DATABASE_URL = $uri
$env:DISABLE_SCHEDULER = "1"
$env:DISABLE_AI_SEED = "1"
$env:TG_USE_POLLING = "0"
$env:DEMO_MODE = "1"
$env:FLASK_ENV = "development"
$env:SECRET_KEY = "demo-floraflow-client-preview"
Remove-Item Env:TG_BOT_TOKEN -ErrorAction SilentlyContinue
Remove-Item Env:GROQ_API_KEY -ErrorAction SilentlyContinue
Remove-Item Env:GEMINI_API_KEY -ErrorAction SilentlyContinue
Remove-Item Env:AMVERA -ErrorAction SilentlyContinue

Write-Host "=== Demo ERP on http://127.0.0.1:$Port ===" -ForegroundColor Green
Write-Host "Login: demo / demo2026  (также boss, manager, sales, hr, brigadier)" -ForegroundColor Green
Write-Host "DB: $demoDb" -ForegroundColor DarkGray
Write-Host "Prod nursery.db is NOT used." -ForegroundColor DarkGray

python -c "from app import create_app; app=create_app(); app.run(host='127.0.0.1', port=$Port, debug=False, use_reloader=False)"
