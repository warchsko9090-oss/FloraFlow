#Requires -Version 5.1
param([switch]$Unregister)
$ErrorActionPreference = "Stop"
$TaskName = "FloraFlow-Failover-Watch"
$FailoverDir = $PSScriptRoot
$Repo = Split-Path -Parent $FailoverDir

if ($Unregister) {
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
  Write-Host "Removed $TaskName"
  return
}

$envFile = Join-Path $FailoverDir ".env"
if (-not (Test-Path $envFile)) {
  Copy-Item (Join-Path $FailoverDir ".env.example") $envFile
  Write-Host "Created $envFile — fill REGRU_* and AMVERA_* then re-run." -ForegroundColor Yellow
  exit 2
}

$py = $null
foreach ($c in @("python","py")) {
  $cmd = Get-Command $c -ErrorAction SilentlyContinue
  if ($cmd) { $py = $cmd.Source; break }
}
if (-not $py) { throw "python not in PATH" }

$watch = Join-Path $FailoverDir "watch.py"
$ps = @"
Set-Location '$Repo'
Get-Content '$envFile' | ForEach-Object {
  `$line = `$_.Trim()
  if (-not `$line -or `$line.StartsWith('#')) { return }
  `$i = `$line.IndexOf('=')
  if (`$i -lt 1) { return }
  `$k = `$line.Substring(0,`$i).Trim()
  `$v = `$line.Substring(`$i+1).Trim().Trim('"').Trim("'")
  [Environment]::SetEnvironmentVariable(`$k, `$v, 'Process')
}
& '$py' '$watch'
if (`$LASTEXITCODE -ne 0) { exit `$LASTEXITCODE }
"@
$arg = "-NoProfile -ExecutionPolicy Bypass -Command `$code = @'`n$ps`n'@; Invoke-Expression `$code"
# simpler: write runner helper
$runner = Join-Path $FailoverDir "run-watch.ps1"
@"
`$ErrorActionPreference = 'Stop'
Set-Location '$Repo'
`$envFile = '$envFile'
Get-Content `$envFile | ForEach-Object {
  `$line = `$_.Trim()
  if (-not `$line -or `$line.StartsWith('#')) { return }
  `$i = `$line.IndexOf('=')
  if (`$i -lt 1) { return }
  `$k = `$line.Substring(0,`$i).Trim()
  `$v = `$line.Substring(`$i+1).Trim().Trim('"').Trim("'")
  [Environment]::SetEnvironmentVariable(`$k, `$v, 'Process')
}
& '$py' '$watch'
exit `$LASTEXITCODE
"@ | Set-Content -LiteralPath $runner -Encoding UTF8

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$runner`""
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date -RepetitionInterval (New-TimeSpan -Minutes 10) -RepetitionDuration (New-TimeSpan -Days 3650)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Minutes 10)
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null
Write-Host "Task '$TaskName' every 10 minutes." -ForegroundColor Green
Write-Host "When this PC is off, GitHub Actions must be configured (see FILL-ME.txt)."
