# WOW-DB 로컬 개발 데이터 초기화 (Windows PowerShell)
param(
    [string]$DataDir = "$env:TEMP\wowdb-dev",
    [switch]$Force
)

function Write-WowLog  { param([string]$Msg) Write-Host "[WOW-DB] $Msg" -ForegroundColor Green }
function Write-WowWarn { param([string]$Msg) Write-Host "[WARN]   $Msg" -ForegroundColor Yellow }

$PidFile = "$DataDir\wowdb.pids"
if (Test-Path $PidFile) {
    Write-WowWarn "Cluster appears to be running. Stop it first:"
    Write-WowWarn "  .\scripts\dev\stop-local.ps1"
    exit 1
}

if (-not $Force) {
    Write-WowWarn "This will delete all local WOW-DB data at: $DataDir"
    $confirm = Read-Host "Continue? [y/N]"
    if ($confirm -notmatch "^[yY]$") {
        Write-WowLog "Aborted."; exit 0
    }
}

Remove-Item -Path $DataDir -Recurse -Force -ErrorAction SilentlyContinue
Write-WowLog "Local data cleared: $DataDir"
Write-WowLog "Run '.\scripts\dev\run-local.ps1' to start fresh."
