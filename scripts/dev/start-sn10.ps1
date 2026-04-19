# Start SN×10 fresh
param(
    [string]$DataDir = "$env:TEMP\wowdb-dev",
    [string]$LogDir  = "$env:TEMP\wowdb-dev\logs"
)

$RepoRoot  = (Get-Item "$PSScriptRoot\..\..").FullName
$BinDir    = "$RepoRoot\target\debug"
$ConfigDir = "$RepoRoot\dev\configs"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

for ($n = 1; $n -le 10; $n++) {
    $grpcPort = 9059 + $n
    $httpPort = 8039 + $n
    $dataPath = "$DataDir\sn\sn$n"
    $logBase  = "$LogDir\sn$n"

    New-Item -ItemType Directory -Force -Path $dataPath | Out-Null

    $env:NODE_ID       = "sn-local-$n"
    $env:GRPC_PORT     = [string]$grpcPort
    $env:HTTP_PORT     = [string]$httpPort
    $env:DATA_DIR      = $dataPath
    $env:RUST_LOG      = "storage_node=info,shared=info"

    $p = Start-Process -FilePath "$BinDir\storage-node.exe" `
        -ArgumentList "--config", "$ConfigDir\storage-node-local.toml" `
        -RedirectStandardOutput "$logBase.log" `
        -RedirectStandardError  "$logBase-err.log" `
        -PassThru -NoNewWindow

    Write-Host "Started SN-$n PID=$($p.Id) gRPC:$grpcPort HTTP:$httpPort"
}

Write-Host "Waiting for SNs to start..."
Start-Sleep 3
Write-Host "Done."
