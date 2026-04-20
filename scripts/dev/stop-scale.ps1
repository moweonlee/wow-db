# WOW-DB 스케일업 노드 종료 스크립트
# scale-local.ps1 으로 추가된 노드만 종료 (기존 QN-1/CN-1~2/SN-1~3 은 유지)

param(
    [string]$DataDir = "$env:TEMP\wowdb-dev"
)

$PidFile = "$DataDir\wowdb-scale.pids"

function Write-WowLog { param([string]$Msg) Write-Host "[STOP]   $Msg" -ForegroundColor Yellow }

if (-not (Test-Path $PidFile)) {
    Write-WowLog "PID 파일이 없습니다: $PidFile"
    Write-WowLog "scale-local.ps1 으로 기동된 노드가 없거나 이미 종료되었습니다."
    exit 0
}

Get-Content $PidFile | ForEach-Object {
    if ($_ -match "^(\S+)=(\d+)$") {
        $name = $Matches[1]
        $pid  = [int]$Matches[2]
        try {
            $p = Get-Process -Id $pid -ErrorAction Stop
            Stop-Process -Id $pid -Force
            Write-WowLog "Stopped $name (PID $pid)"
        } catch {
            Write-WowLog "$name (PID $pid) — already exited"
        }
    }
}

Remove-Item -Path $PidFile -ErrorAction SilentlyContinue
Write-WowLog "Scale-up nodes stopped."
