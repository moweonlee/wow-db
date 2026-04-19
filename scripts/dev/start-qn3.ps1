# Start QN×3 with full STORAGE_NODES + COMPUTE_NODES env
param(
    [string]$DataDir = "$env:TEMP\wowdb-dev",
    [string]$LogDir  = "$env:TEMP\wowdb-dev\logs"
)

$RepoRoot  = (Get-Item "$PSScriptRoot\..\..").FullName
$BinDir    = "$RepoRoot\target\debug"
$ConfigDir = "$RepoRoot\dev\configs"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$allSnAddrs      = (1..10) | ForEach-Object { "127.0.0.1:" + (9059 + $_) }
$storageNodesEnv = $allSnAddrs -join ","

$allCnAddrs      = (1..10) | ForEach-Object { "127.0.0.1:" + (9039 + $_) }
$computeNodesEnv = $allCnAddrs -join ","

$allQnRaftPeers = (1..3) | ForEach-Object { "qn-local-" + $_ + ":" + (9009 + $_) }
$raftPeersEnv   = $allQnRaftPeers -join ","

$allQnHttpPeers = (1..3) | ForEach-Object { "127.0.0.1:" + (18079 + $_) }
$qnHttpPeersEnv = $allQnHttpPeers -join ","

Write-Host "STORAGE_NODES=$storageNodesEnv"
Write-Host "COMPUTE_NODES=$computeNodesEnv"

for ($n = 1; $n -le 3; $n++) {
    $mysqlPort = 19029 + $n
    $webPort   = 18079 + $n
    $raftPort  = 9009  + $n
    $grpcPort  = 9019  + $n
    $logBase   = "$LogDir\qn$n"

    $env:NODE_ID        = "qn-local-$n"
    $env:MYSQL_PORT     = [string]$mysqlPort
    $env:WEB_PORT       = [string]$webPort
    $env:RAFT_PORT      = [string]$raftPort
    $env:GRPC_PORT      = [string]$grpcPort
    $env:COMPUTE_NODES  = $computeNodesEnv
    $env:STORAGE_NODES  = $storageNodesEnv
    $env:QN_PEERS       = $raftPeersEnv
    $env:RAFT_PEERS     = $raftPeersEnv
    $env:QN_HTTP_PEERS  = $qnHttpPeersEnv
    $env:RUST_LOG       = "query_node=debug,shared=info"

    $p = Start-Process -FilePath "$BinDir\query-node.exe" `
        -ArgumentList "--config", "$ConfigDir\query-node-local.toml" `
        -RedirectStandardOutput "$logBase.log" `
        -RedirectStandardError  "$logBase-err.log" `
        -PassThru -NoNewWindow
    Write-Host "Started QN-$n PID=$($p.Id) MySQL:$mysqlPort Web:$webPort"
}
Write-Host "Waiting for QN ports..."
Start-Sleep 3
Write-Host "Done. Try: mysql -h 127.0.0.1 -P 19030 -u root"
