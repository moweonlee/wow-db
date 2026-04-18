# =============================================================================
# WOW-DB 로컬 단일 인스턴스 실행 스크립트 (Windows PowerShell)
# 용도: Docker 없이 로컬에서 빠르게 QN×1 + CN×1 + SN×1 기동
#
# 사용법:
#   .\scripts\dev\run-local.ps1                 # 빌드 후 기동
#   .\scripts\dev\run-local.ps1 -NoBuild        # 빌드 생략
#   .\scripts\dev\run-local.ps1 -Release        # release 빌드 (기본: dev)
#   .\scripts\dev\run-local.ps1 -DataDir D:\wowdb-dev
#
# 종료: Ctrl+C (자동으로 모든 프로세스 정리)
# =============================================================================

param(
    [switch]$NoBuild,
    [switch]$Release,
    [string]$DataDir = "$env:TEMP\wowdb-dev",
    [string]$LogDir  = "$env:TEMP\wowdb-dev\logs"
)

$ErrorActionPreference = "Stop"
$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot   = (Get-Item "$ScriptDir\..\..").FullName
$ConfigDir  = "$RepoRoot\dev\configs"
$PidFile    = "$DataDir\wowdb.pids"
$Processes  = @{}

function Write-WowLog  { param([string]$Msg) Write-Host "[WOW-DB] $Msg" -ForegroundColor Green }
function Write-WowInfo { param([string]$Msg) Write-Host "[INFO]   $Msg" -ForegroundColor Cyan }
function Write-WowWarn { param([string]$Msg) Write-Host "[WARN]   $Msg" -ForegroundColor Yellow }
function Write-WowErr  { param([string]$Msg) Write-Host "[ERR]    $Msg" -ForegroundColor Red }

function Stop-AllProcesses {
    Write-WowLog "Stopping WOW-DB local cluster..."
    foreach ($entry in $Processes.GetEnumerator()) {
        $p = $entry.Value
        if (-not $p.HasExited) {
            try { Stop-Process -Id $p.Id -Force; Write-WowLog "  Stopped $($entry.Key) (PID $($p.Id))" }
            catch { Write-WowWarn "  Could not stop $($entry.Key): $_" }
        }
    }
    Remove-Item -Path $PidFile -ErrorAction SilentlyContinue
    Write-WowLog "All processes stopped."
}

# Ctrl+C / exit 처리
[Console]::TreatControlCAsInput = $false
$null = [Console]::CancelKeyPress | ForEach-Object { }
try {
    # 이미 실행 중인지 확인
    if (Test-Path $PidFile) {
        Write-WowWarn "PID file found at $PidFile. Already running?"
        Write-WowWarn "Run '.\scripts\dev\stop-local.ps1' first."
        exit 1
    }

    # 디렉토리 준비
    New-Item -ItemType Directory -Force -Path "$DataDir\sn", $LogDir | Out-Null

    # =========================================================================
    # 빌드
    # =========================================================================
    if (-not $NoBuild) {
        Write-WowLog "Building WOW-DB..."
        Push-Location $RepoRoot
        if ($Release) {
            cargo build --release -p storage-node -p compute-node -p query-node
        } else {
            cargo build -p storage-node -p compute-node -p query-node
        }
        Pop-Location
        Write-WowLog "Build complete."
    }

    $BinDir = if ($Release) { "$RepoRoot\target\release" } else { "$RepoRoot\target\debug" }

    foreach ($bin in @("storage-node.exe", "compute-node.exe", "query-node.exe")) {
        if (-not (Test-Path "$BinDir\$bin")) {
            Write-WowErr "Binary not found: $BinDir\$bin"
            Write-WowErr "Run without -NoBuild to build first."
            exit 1
        }
    }

    Write-Host ""
    Write-WowLog "======================================================================"
    Write-WowLog " WOW-DB Local Cluster (QN x1 + CN x1 + SN x1)"
    Write-WowLog " Data  : $DataDir"
    Write-WowLog " Logs  : $LogDir"
    Write-WowLog " Config: $ConfigDir"
    Write-WowLog "======================================================================"
    Write-Host ""

    # =========================================================================
    # Storage Node
    # =========================================================================
    Write-WowLog "[1/3] Starting Storage Node..."
    Write-WowInfo "  gRPC  : 127.0.0.1:9060"
    Write-WowInfo "  HTTP  : 127.0.0.1:8040"
    Write-WowInfo "  Data  : $DataDir\sn"
    Write-WowInfo "  Log   : $LogDir\sn.log"

    $env:NODE_ID    = "sn-local-1"
    $env:DATA_DIR   = "$DataDir\sn"
    $env:RUST_LOG   = "storage_node=info,shared=info"

    $sn = Start-Process -FilePath "$BinDir\storage-node.exe" `
        -ArgumentList "--config", "$ConfigDir\storage-node-local.toml" `
        -RedirectStandardOutput "$LogDir\sn.log" `
        -RedirectStandardError  "$LogDir\sn-err.log" `
        -PassThru -NoNewWindow
    $Processes["SN"] = $sn

    # 포트 오픈 대기
    $ready = $false
    for ($i = 0; $i -lt 20; $i++) {
        try {
            $tcp = New-Object System.Net.Sockets.TcpClient("127.0.0.1", 9060)
            $tcp.Close(); $ready = $true; break
        } catch { Start-Sleep -Milliseconds 500 }
    }
    if (-not $ready) { Write-WowErr "Storage Node did not start. Check $LogDir\sn.log"; Stop-AllProcesses; exit 1 }
    Write-WowLog "  Storage Node ready (PID $($sn.Id))"

    # =========================================================================
    # Compute Node
    # =========================================================================
    Write-WowLog "[2/3] Starting Compute Node..."
    Write-WowInfo "  gRPC  : 127.0.0.1:9040"
    Write-WowInfo "  Log   : $LogDir\cn.log"

    $env:NODE_ID       = "cn-local-1"
    $env:STORAGE_NODES = "127.0.0.1:9060"
    $env:RUST_LOG      = "compute_node=info,shared=info"

    $cn = Start-Process -FilePath "$BinDir\compute-node.exe" `
        -ArgumentList "--config", "$ConfigDir\compute-node-local.toml" `
        -RedirectStandardOutput "$LogDir\cn.log" `
        -RedirectStandardError  "$LogDir\cn-err.log" `
        -PassThru -NoNewWindow
    $Processes["CN"] = $cn

    $ready = $false
    for ($i = 0; $i -lt 20; $i++) {
        try {
            $tcp = New-Object System.Net.Sockets.TcpClient("127.0.0.1", 9040)
            $tcp.Close(); $ready = $true; break
        } catch { Start-Sleep -Milliseconds 500 }
    }
    if (-not $ready) { Write-WowErr "Compute Node did not start. Check $LogDir\cn.log"; Stop-AllProcesses; exit 1 }
    Write-WowLog "  Compute Node ready (PID $($cn.Id))"

    # =========================================================================
    # Query Node
    # =========================================================================
    Write-WowLog "[3/3] Starting Query Node..."
    Write-WowInfo "  MySQL : 127.0.0.1:9030"
    Write-WowInfo "  Web UI: http://127.0.0.1:8080"
    Write-WowInfo "  Log   : $LogDir\qn.log"

    $env:NODE_ID       = "qn-local-1"
    $env:RAFT_PEERS    = "qn-local-1:9010"
    $env:COMPUTE_NODES = "127.0.0.1:9040"
    $env:QN_PEERS      = "qn-local-1:9011"
    $env:RUST_LOG      = "query_node=info,shared=info"

    $qn = Start-Process -FilePath "$BinDir\query-node.exe" `
        -ArgumentList "--config", "$ConfigDir\query-node-local.toml" `
        -RedirectStandardOutput "$LogDir\qn.log" `
        -RedirectStandardError  "$LogDir\qn-err.log" `
        -PassThru -NoNewWindow
    $Processes["QN"] = $qn

    $ready = $false
    for ($i = 0; $i -lt 30; $i++) {
        try {
            $tcp = New-Object System.Net.Sockets.TcpClient("127.0.0.1", 9030)
            $tcp.Close(); $ready = $true; break
        } catch { Start-Sleep -Milliseconds 500 }
    }
    if (-not $ready) { Write-WowErr "Query Node did not start. Check $LogDir\qn.log"; Stop-AllProcesses; exit 1 }
    Write-WowLog "  Query Node ready (PID $($qn.Id))"

    # PID 파일 기록
    "SN=$($sn.Id)`nCN=$($cn.Id)`nQN=$($qn.Id)" | Out-File -FilePath $PidFile -Encoding utf8

    Write-Host ""
    Write-WowLog "======================================================================"
    Write-WowLog " WOW-DB local cluster is READY"
    Write-WowLog "======================================================================"
    Write-WowLog ""
    Write-WowLog "  MySQL 접속:  mysql -h 127.0.0.1 -P 9030 -u admin -p ''"
    Write-WowLog "  Web UI:      http://localhost:8080"
    Write-WowLog "  Metrics:     http://localhost:8080/metrics"
    Write-WowLog "  로그 확인:   Get-Content $LogDir\qn.log -Wait"
    Write-WowLog ""
    Write-WowLog "  빠른 테스트:"
    Write-WowLog "    mysql -h 127.0.0.1 -P 9030 -u admin -p '' < test_suite.sql"
    Write-WowLog ""
    Write-WowLog "  종료: Ctrl+C  또는  .\scripts\dev\stop-local.ps1"
    Write-WowLog "======================================================================"
    Write-Host ""

    # 프로세스 종료 모니터링
    while ($true) {
        Start-Sleep -Seconds 2
        foreach ($entry in $Processes.GetEnumerator()) {
            if ($entry.Value.HasExited) {
                Write-WowErr "$($entry.Key) (PID $($entry.Value.Id)) exited unexpectedly with code $($entry.Value.ExitCode)"
                Write-WowErr "Check log: $LogDir\$($entry.Key.ToLower()).log"
            }
        }
    }
} finally {
    Stop-AllProcesses
}
