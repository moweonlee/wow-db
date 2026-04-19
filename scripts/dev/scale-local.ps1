# =============================================================================
# WOW-DB Local Scale-Up Script
# Extends existing cluster (QN x1 + CN x2 + SN x3) to QN x3 + CN x10 + SN x10
#
# Port layout:
#   SN-N : gRPC = 9059+N (9060~9069), HTTP = 8039+N (8040~8049)
#   CN-N : gRPC = 9039+N (9040~9049), HTTP = 10039+N (10040~10049)
#   QN-N : MySQL= 19029+N (19030~19032), Web = 18079+N (18080~18082)
#
# Usage:
#   .\scripts\dev\scale-local.ps1
#   .\scripts\dev\scale-local.ps1 -SnCount 5
#   .\scripts\dev\scale-local.ps1 -QnCount 1
#
# Requires: run-local.ps1 cluster already running (QN-1 at 18080)
# Stop:     .\scripts\dev\stop-scale.ps1
# =============================================================================

param(
    [int]$SnCount  = 10,
    [int]$CnCount  = 10,
    [int]$QnCount  = 3,
    [string]$DataDir = "$env:TEMP\wowdb-dev",
    [string]$LogDir  = "$env:TEMP\wowdb-dev\logs",
    [string]$QnHttp  = "127.0.0.1:18080"
)

$ErrorActionPreference = "Stop"
$RepoRoot  = (Get-Item "$PSScriptRoot\..\..").FullName
$ConfigDir = "$RepoRoot\dev\configs"
$BinDir    = "$RepoRoot\target\debug"
$PidFile   = "$DataDir\wowdb-scale.pids"
$Processes = @{}

function Write-WowLog  { param([string]$Msg) Write-Host "[SCALE]  $Msg" -ForegroundColor Green }
function Write-WowInfo { param([string]$Msg) Write-Host "[INFO]   $Msg" -ForegroundColor Cyan }
function Write-WowWarn { param([string]$Msg) Write-Host "[WARN]   $Msg" -ForegroundColor Yellow }
function Write-WowErr  { param([string]$Msg) Write-Host "[ERR]    $Msg" -ForegroundColor Red }

function Wait-Port {
    param([string]$Hostname, [int]$Port, [int]$TimeoutSec = 10)
    for ($i = 0; $i -lt ($TimeoutSec * 2); $i++) {
        try {
            $tcp = New-Object System.Net.Sockets.TcpClient($Hostname, $Port)
            $tcp.Close(); return $true
        } catch { Start-Sleep -Milliseconds 500 }
    }
    return $false
}

function Port-InUse { param([int]$Port)
    return $null -ne (netstat -an | Select-String "\s:$Port\s")
}

function Stop-ScaleProcesses {
    Write-WowLog "Stopping scaled-up nodes..."
    foreach ($entry in $Processes.GetEnumerator()) {
        $p = $entry.Value
        if (-not $p.HasExited) {
            try { Stop-Process -Id $p.Id -Force; Write-WowLog "  Stopped $($entry.Key) (PID $($p.Id))" }
            catch { Write-WowWarn "  Could not stop $($entry.Key)" }
        }
    }
    Remove-Item -Path $PidFile -ErrorAction SilentlyContinue
}

# ── Binary check ──────────────────────────────────────────────────────────────
foreach ($bin in @("storage-node.exe", "compute-node.exe", "query-node.exe")) {
    if (-not (Test-Path "$BinDir\$bin")) {
        Write-WowErr "Binary not found: $BinDir\$bin -- run cargo build first"
        exit 1
    }
}

# ── QN-1 pre-check (optional — will be started in QN section if not running) ─
Write-WowLog "Checking base Query Node at $QnHttp ..."
try {
    $resp = Invoke-WebRequest -Uri "http://$QnHttp/healthz" -UseBasicParsing -TimeoutSec 3
    Write-WowInfo "  QN-1 already running: HTTP $($resp.StatusCode)"
} catch {
    Write-WowInfo "  QN-1 not running — will be started in QN section"
}

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

Write-Host ""
Write-WowLog "======================================================================"
Write-WowLog " WOW-DB Scale-Up: QN x$QnCount + SN x$SnCount + CN x$CnCount"
Write-WowLog " Base QN : http://$QnHttp"
Write-WowLog " Data    : $DataDir"
Write-WowLog " Logs    : $LogDir"
Write-WowLog "======================================================================"
Write-Host ""

$pidLines = @()

# =============================================================================
# 1. Storage Nodes  SN-N: gRPC=9059+N  HTTP=8039+N
# =============================================================================
Write-WowLog "[ SN ] Starting Storage Nodes 1 ~ $SnCount ..."
$snStarted = 0

for ($n = 1; $n -le $SnCount; $n++) {
    $grpcPort = 9059 + $n
    $httpPort = 8039 + $n
    $nodeId   = "sn-local-$n"
    $dataPath = "$DataDir\sn\sn$n"
    $logBase  = "$LogDir\sn$n"

    if (Port-InUse $grpcPort) {
        Write-WowInfo ("  SN-{0}  gRPC:{1} -- already running, skip" -f $n, $grpcPort)
        continue
    }

    New-Item -ItemType Directory -Force -Path $dataPath | Out-Null

    $env:NODE_ID       = $nodeId
    $env:GRPC_PORT     = [string]$grpcPort
    $env:HTTP_PORT     = [string]$httpPort
    $env:DATA_DIR      = $dataPath
    $env:QN_HTTP_PEERS = $QnHttp
    $env:RUST_LOG      = "storage_node=info,shared=info"
    Remove-Item Env:\LOG_DIR -ErrorAction SilentlyContinue

    $p = Start-Process -FilePath "$BinDir\storage-node.exe" `
        -ArgumentList "--config", "$ConfigDir\storage-node-local.toml" `
        -RedirectStandardOutput "$logBase.log" `
        -RedirectStandardError  "$logBase-err.log" `
        -PassThru -NoNewWindow

    $Processes["SN-$n"] = $p

    if (Wait-Port "127.0.0.1" $grpcPort 10) {
        Write-WowInfo ("  SN-{0}  gRPC:{1}  HTTP:{2}  PID={3}" -f $n, $grpcPort, $httpPort, $p.Id)
        $pidLines += ("SN-{0}={1}" -f $n, $p.Id)
        $snStarted++
    } else {
        Write-WowWarn ("  SN-{0} timeout -- check {1}-err.log" -f $n, $logBase)
    }
}
Write-Host ""

# =============================================================================
# 2. Compute Nodes  CN-N: gRPC=9039+N  HTTP=10039+N
# =============================================================================
$allSnAddrs      = (1..$SnCount) | ForEach-Object { "127.0.0.1:" + (9059 + $_) }
$storageNodesEnv = $allSnAddrs -join ","

Write-WowLog "[ CN ] Starting Compute Nodes 1 ~ $CnCount ..."
Write-WowInfo "  STORAGE_NODES = $storageNodesEnv"
$cnStarted = 0

for ($n = 1; $n -le $CnCount; $n++) {
    $grpcPort = 9039 + $n
    $httpPort = 10039 + $n
    $nodeId   = "cn-local-$n"
    $logBase  = "$LogDir\cn$n"

    if (Port-InUse $grpcPort) {
        Write-WowInfo ("  CN-{0}  gRPC:{1} -- already running, skip" -f $n, $grpcPort)
        continue
    }

    $env:NODE_ID       = $nodeId
    $env:GRPC_PORT     = [string]$grpcPort
    $env:HTTP_PORT     = [string]$httpPort
    $env:STORAGE_NODES = $storageNodesEnv
    $env:QN_HTTP_PEERS = $QnHttp
    $env:RUST_LOG      = "compute_node=info,shared=info"
    Remove-Item Env:\LOG_DIR -ErrorAction SilentlyContinue

    $p = Start-Process -FilePath "$BinDir\compute-node.exe" `
        -ArgumentList "--config", "$ConfigDir\compute-node-local.toml" `
        -RedirectStandardOutput "$logBase.log" `
        -RedirectStandardError  "$logBase-err.log" `
        -PassThru -NoNewWindow

    $Processes["CN-$n"] = $p

    if (Wait-Port "127.0.0.1" $grpcPort 10) {
        Write-WowInfo ("  CN-{0}  gRPC:{1}  HTTP:{2}  PID={3}" -f $n, $grpcPort, $httpPort, $p.Id)
        $pidLines += ("CN-{0}={1}" -f $n, $p.Id)
        $cnStarted++
    } else {
        Write-WowWarn ("  CN-{0} timeout -- check {1}-err.log" -f $n, $logBase)
    }
}
Write-Host ""

# =============================================================================
# 3. Query Nodes  QN-N: MySQL=19029+N  Web=18079+N
# =============================================================================
$allCnAddrs      = (1..$CnCount) | ForEach-Object { "127.0.0.1:" + (9039 + $_) }
$computeNodesEnv = $allCnAddrs -join ","

# QN Raft peers (host:raft_port)
$allQnRaftPeers = (1..$QnCount) | ForEach-Object {
    $rPort = 9009 + $_
    "qn-local-" + $_ + ":" + $rPort
}
$raftPeersEnv = $allQnRaftPeers -join ","

# QN HTTP peers — used for StarRocks FE-style mutual registration
# QN-1 (18080) is always first so it is recognised as Leader
$allQnHttpPeers = (1..$QnCount) | ForEach-Object { "127.0.0.1:" + (18079 + $_) }
$qnHttpPeersEnv = $allQnHttpPeers -join ","

Write-WowLog "[ QN ] Starting Query Nodes 1 ~ $QnCount ..."
Write-WowInfo "  COMPUTE_NODES = $computeNodesEnv"
$qnStarted = 0

for ($n = 1; $n -le $QnCount; $n++) {
    $mysqlPort = 19029 + $n
    $webPort   = 18079 + $n
    $raftPort  = 9009  + $n
    $grpcPort  = 9019  + $n
    $nodeId    = "qn-local-$n"
    $logBase   = "$LogDir\qn$n"

    if (Port-InUse $mysqlPort) {
        Write-WowInfo ("  QN-{0}  MySQL:{1} -- already running, skip" -f $n, $mysqlPort)
        continue
    }

    $env:NODE_ID           = $nodeId
    $env:MYSQL_PORT        = [string]$mysqlPort
    $env:WEB_PORT          = [string]$webPort
    $env:RAFT_PORT         = [string]$raftPort
    $env:GRPC_PORT         = [string]$grpcPort
    $env:COMPUTE_NODES     = $computeNodesEnv
    $env:QN_PEERS          = $raftPeersEnv
    $env:RAFT_PEERS        = $raftPeersEnv
    $env:QN_HTTP_PEERS     = $qnHttpPeersEnv   # FE mutual registration
    $env:RUST_LOG          = "query_node=info,shared=info"
    Remove-Item Env:\LOG_DIR -ErrorAction SilentlyContinue

    $p = Start-Process -FilePath "$BinDir\query-node.exe" `
        -ArgumentList "--config", "$ConfigDir\query-node-local.toml" `
        -RedirectStandardOutput "$logBase.log" `
        -RedirectStandardError  "$logBase-err.log" `
        -PassThru -NoNewWindow

    $Processes["QN-$n"] = $p

    if (Wait-Port "127.0.0.1" $mysqlPort 15) {
        Write-WowInfo ("  QN-{0}  MySQL:{1}  Web:http://localhost:{2}  PID={3}" -f $n, $mysqlPort, $webPort, $p.Id)
        $pidLines += ("QN-{0}={1}" -f $n, $p.Id)
        $qnStarted++
    } else {
        Write-WowWarn ("  QN-{0} timeout -- check {1}-err.log" -f $n, $logBase)
    }
}

# Save PID file
$pidLines | Out-File -FilePath $PidFile -Encoding utf8

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host ""
Write-WowLog "======================================================================"
Write-WowLog " Scale-Up Complete"
Write-WowLog ("   SN: {0} new  (total {1},  HTTP :8040~:{2})" -f $snStarted, $SnCount, (8039+$SnCount))
Write-WowLog ("   CN: {0} new  (total {1},  HTTP :10040~:{2})" -f $cnStarted, $CnCount, (10039+$CnCount))
Write-WowLog ("   QN: {0} new  (total {1})" -f $qnStarted, $QnCount)
Write-WowLog ""
Write-WowLog " Query Node endpoints:"
for ($n = 1; $n -le $QnCount; $n++) {
    $mp = 19029 + $n
    $wp = 18079 + $n
    Write-WowLog ("   QN-{0}  MySQL:{1}  Dashboard: http://localhost:{2}/dashboard" -f $n, $mp, $wp)
}
Write-WowLog ""
Write-WowLog " Cluster status: curl http://localhost:18080/api/v1/cluster"
Write-WowLog " Stop:           .\scripts\dev\stop-scale.ps1"
Write-WowLog "======================================================================"
Write-Host ""

# ── Process monitor loop ──────────────────────────────────────────────────────
try {
    while ($true) {
        Start-Sleep -Seconds 5
        foreach ($entry in $Processes.GetEnumerator()) {
            if ($entry.Value.HasExited) {
                $code = $entry.Value.ExitCode
                Write-WowWarn ("  {0} (PID {1}) exited unexpectedly (code {2})" -f $entry.Key, $entry.Value.Id, $code)
            }
        }
    }
} finally {
    Stop-ScaleProcesses
}
