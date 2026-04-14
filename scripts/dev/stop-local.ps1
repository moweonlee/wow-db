# WOW-DB 로컬 클러스터 종료 (Windows PowerShell)
param(
    [string]$DataDir = "$env:TEMP\wowdb-dev"
)

$PidFile = "$DataDir\wowdb.pids"

function Write-WowLog  { param([string]$Msg) Write-Host "[WOW-DB] $Msg" -ForegroundColor Green }
function Write-WowWarn { param([string]$Msg) Write-Host "[WARN]   $Msg" -ForegroundColor Yellow }

if (-not (Test-Path $PidFile)) {
    Write-WowWarn "No PID file found at $PidFile."
    # 포트 기반 fallback
    foreach ($port in @(9060, 9040, 9030)) {
        $conn = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue
        if ($conn) {
            $pid = $conn | Select-Object -First 1 -ExpandProperty OwningProcess
            Write-WowLog "Killing process on port $port (PID $pid)"
            Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
        }
    }
    exit 0
}

Get-Content $PidFile | ForEach-Object {
    if ($_ -match "^(\w+)=(\d+)$") {
        $name = $Matches[1]; $pid = [int]$Matches[2]
        try {
            Stop-Process -Id $pid -Force
            Write-WowLog "Stopped $name (PID $pid)"
        } catch {
            Write-WowWarn "$name (PID $pid) already stopped."
        }
    }
}

Remove-Item -Path $PidFile -ErrorAction SilentlyContinue
Write-WowLog "All WOW-DB processes stopped."
