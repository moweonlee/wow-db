# WOW-DB local dev test script (Windows PowerShell)
#
# Usage:
#   .\scripts\dev\dev-test.ps1              # build + start + 1 cycle
#   .\scripts\dev\dev-test.ps1 -NoBuild     # skip build
#   .\scripts\dev\dev-test.ps1 -Cycles 3   # repeat 3 times
#   .\scripts\dev\dev-test.ps1 -Watch       # keep cluster alive after test
#   .\scripts\dev\dev-test.ps1 -OnlyTest    # test already-running cluster
#
# Ports (from actual binaries):
#   SN : HTTP  :8040  (gRPC :9060 is Phase-B TODO)
#   CN : HTTP  :9040
#   QN : MySQL :9030  /  Web UI :8080
param(
    [switch]$NoBuild,
    [switch]$Release,
    [int]$Cycles     = 1,
    [switch]$Watch,
    [switch]$OnlyTest,
    [string]$DataDir = "$env:TEMP\wowdb-dev",
    [string]$LogDir  = "$env:TEMP\wowdb-dev\logs"
)

$ErrorActionPreference = "Stop"
$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot   = (Get-Item "$ScriptDir\..\..").FullName
$ConfigDir  = "$RepoRoot\dev\configs"
$BinProfile = if ($Release) { "release" } else { "debug" }
$BinDir     = "$RepoRoot\target\$BinProfile"

function Log-Ok   { param([string]$m) Write-Host "[  OK  ] $m" -ForegroundColor Green }
function Log-Fail { param([string]$m) Write-Host "[ FAIL ] $m" -ForegroundColor Red }
function Log-Info { param([string]$m) Write-Host "[ INFO ] $m" -ForegroundColor Cyan }
function Log-Warn { param([string]$m) Write-Host "[ WARN ] $m" -ForegroundColor Yellow }
function Log-Step { param([string]$m) Write-Host "" ; Write-Host "=== $m ===" -ForegroundColor Magenta }

$Script:Procs  = @{}
$Script:Errors = 0

function Stop-Cluster {
    Log-Info "Stopping cluster..."
    foreach ($kv in $Script:Procs.GetEnumerator()) {
        $p = $kv.Value
        if ($p -and (-not $p.HasExited)) {
            try { Stop-Process -Id $p.Id -Force } catch {}
            Log-Info "  $($kv.Key) PID=$($p.Id) stopped"
        }
    }
    $Script:Procs = @{}
}

function Wait-Port {
    param([int]$Port, [string]$Name, [int]$MaxTries = 30)
    Log-Info "Waiting for $Name on :$Port ..."
    for ($i = 0; $i -lt $MaxTries; $i++) {
        try {
            $tcp = New-Object System.Net.Sockets.TcpClient
            $tcp.Connect("127.0.0.1", $Port)
            $tcp.Close()
            return $true
        } catch { Start-Sleep -Milliseconds 500 }
    }
    return $false
}

function Test-Http {
    param([string]$Url, [int]$TimeoutSec = 3)
    try {
        $r = Invoke-WebRequest -Uri $Url -UseBasicParsing `
                 -TimeoutSec $TimeoutSec -ErrorAction Stop
        return ($r.StatusCode -eq 200)
    } catch { return $false }
}

function Start-StorageNode {
    New-Item -ItemType Directory -Force -Path "$DataDir\sn", $LogDir | Out-Null
    $env:NODE_ID          = "sn-local-1"
    $env:DATA_DIR         = "$DataDir\sn"
    $env:RUST_LOG         = "storage_node=debug,shared=info"
    $env:QN_GRPC_ENDPOINT = "127.0.0.1:9011"
    $p = Start-Process `
            -FilePath "$BinDir\storage-node.exe" `
            -ArgumentList @("--config", "$ConfigDir\storage-node-local.toml") `
            -RedirectStandardOutput "$LogDir\sn.log" `
            -RedirectStandardError  "$LogDir\sn-err.log" `
            -PassThru -NoNewWindow
    $Script:Procs["SN"] = $p
    Log-Info "  SN PID=$($p.Id) -> $LogDir\sn.log"
    if (-not (Wait-Port -Port 8040 -Name "SN-HTTP")) {
        Log-Fail "SN :8040 failed to start"
        Get-Content "$LogDir\sn-err.log" -Tail 15 | ForEach-Object { Write-Host "  | $_" -ForegroundColor DarkRed }
        return $false
    }
    Log-Ok "SN :8040 ready"
    return $true
}

function Start-ComputeNode {
    $env:NODE_ID       = "cn-local-1"
    $env:STORAGE_NODES = "127.0.0.1:9060"
    $env:RUST_LOG      = "compute_node=debug,shared=info"
    $p = Start-Process `
            -FilePath "$BinDir\compute-node.exe" `
            -ArgumentList @("--config", "$ConfigDir\compute-node-local.toml") `
            -RedirectStandardOutput "$LogDir\cn.log" `
            -RedirectStandardError  "$LogDir\cn-err.log" `
            -PassThru -NoNewWindow
    $Script:Procs["CN"] = $p
    Log-Info "  CN PID=$($p.Id) -> $LogDir\cn.log"
    if (-not (Wait-Port -Port 9040 -Name "CN-gRPC")) {
        Log-Fail "CN gRPC :9040 failed to start"
        Get-Content "$LogDir\cn-err.log" -Tail 15 | ForEach-Object { Write-Host "  | $_" -ForegroundColor DarkRed }
        return $false
    }
    Log-Ok "CN :9040 (gRPC) ready"
    return $true
}

function Start-QueryNode {
    $env:NODE_ID       = "qn-local-1"
    $env:RAFT_PEERS    = "qn-local-1:9010"
    $env:COMPUTE_NODES = "127.0.0.1:9040"
    $env:QN_PEERS      = "qn-local-1:9011"
    $env:RUST_LOG      = "query_node=debug,shared=info"
    $p = Start-Process `
            -FilePath "$BinDir\query-node.exe" `
            -ArgumentList @("--config", "$ConfigDir\query-node-local.toml") `
            -RedirectStandardOutput "$LogDir\qn.log" `
            -RedirectStandardError  "$LogDir\qn-err.log" `
            -PassThru -NoNewWindow
    $Script:Procs["QN"] = $p
    Log-Info "  QN PID=$($p.Id) -> $LogDir\qn.log"
    if (-not (Wait-Port -Port 9030 -Name "QN-MySQL" -MaxTries 40)) {
        Log-Fail "QN MySQL :9030 failed to start"
        Get-Content "$LogDir\qn-err.log" -Tail 20 | ForEach-Object { Write-Host "  | $_" -ForegroundColor DarkRed }
        return $false
    }
    Log-Ok "QN :9030 (MySQL) ready"
    return $true
}

function Check-ProcessAlive {
    Log-Step "Process alive check"
    foreach ($kv in $Script:Procs.GetEnumerator()) {
        $p = $kv.Value
        if ($p.HasExited) {
            Log-Fail "$($kv.Key) PID=$($p.Id) exited unexpectedly (code=$($p.ExitCode))"
            $Script:Errors++
        } else {
            $mb  = [math]::Round($p.WorkingSet64 / 1MB, 1)
            $cpu = try { [math]::Round($p.CPU, 2) } catch { "N/A" }
            Log-Ok "$($kv.Key) PID=$($p.Id) UP  mem=${mb}MB  cpu=${cpu}s"
        }
    }
}

function Run-HealthChecks {
    Log-Step "HTTP health checks"
    $pass = 0
    $fail = 0
    $checks = @(
        [pscustomobject]@{ Url = "http://127.0.0.1:8040/health";      Name = "SN  /health" },
        [pscustomobject]@{ Url = "http://127.0.0.1:8040/healthz";     Name = "SN  /healthz" },
        [pscustomobject]@{ Url = "http://127.0.0.1:8040/api/v1/info"; Name = "SN  /api/v1/info" },
        [pscustomobject]@{ Url = "http://127.0.0.1:10040/health";      Name = "CN  /health" },
        [pscustomobject]@{ Url = "http://127.0.0.1:10040/healthz";    Name = "CN  /healthz" },
        [pscustomobject]@{ Url = "http://127.0.0.1:10040/api/v1/info"; Name = "CN  /api/v1/info" },
        [pscustomobject]@{ Url = "http://127.0.0.1:8080/health";      Name = "QN  Web /health" },
        [pscustomobject]@{ Url = "http://127.0.0.1:8080/metrics";     Name = "QN  /metrics" }
    )
    foreach ($c in $checks) {
        if (Test-Http -Url $c.Url) {
            Log-Ok  "$($c.Name)"
            $pass++
        } else {
            Log-Fail "$($c.Name)  ($($c.Url))"
            $fail++
            $Script:Errors++
        }
    }
    $color = if ($fail -eq 0) { "Green" } else { "Red" }
    Write-Host "  Result: $pass pass / $fail fail" -ForegroundColor $color
}

function Run-MysqlSmoke {
    Log-Step "MySQL smoke tests (via Docker)"

    $dockerCmd = Get-Command docker -ErrorAction SilentlyContinue
    if (-not $dockerCmd) {
        Log-Warn "docker not found -- skipping MySQL tests"
        return
    }

    function Invoke-Sql {
        param([string]$Sql)
        $dargs = @("run","--rm","mysql:8.0","mysql","-h","host.docker.internal","-P","9030","-u","admin","--password=","--connect-timeout=5","-e",$Sql)
        $prev = $ErrorActionPreference; $ErrorActionPreference = "Continue"
        $out = & docker @dargs 2>&1
        $ok  = ($LASTEXITCODE -eq 0)
        $ErrorActionPreference = $prev
        $text = ($out | Where-Object { "$_" -notmatch "Warning" }) -join "`n"
        return [pscustomobject]@{ Ok = $ok; Out = $text }
    }
    function Invoke-SqlVal {
        param([string]$Sql)
        $dargs = @("run","--rm","mysql:8.0","mysql","-h","host.docker.internal","-P","9030","-u","admin","--password=","--connect-timeout=5","-sN","-e",$Sql)
        $prev = $ErrorActionPreference; $ErrorActionPreference = "Continue"
        $out = & docker @dargs 2>&1
        $ok  = ($LASTEXITCODE -eq 0)
        $ErrorActionPreference = $prev
        $lines = ($out | Where-Object { "$_" -notmatch "Warning" -and "$_".Trim() -ne "" })
        $last  = ($lines | Select-Object -Last 1)
        return [pscustomobject]@{ Ok = $ok; Out = "$last".Trim() }
    }

    # 1. SELECT 1
    $r = Invoke-SqlVal "SELECT 1"
    if ($r.Ok -and $r.Out -match "1") {
        Log-Ok "SELECT 1 -> $($r.Out)"
    } else {
        Log-Fail "SELECT 1 failed: $($r.Out)"
        $Script:Errors++; return
    }

    # 2. SHOW TABLES
    $r = Invoke-Sql "SHOW TABLES"
    if ($r.Ok) { Log-Ok "SHOW TABLES" } else { Log-Fail "SHOW TABLES failed"; $Script:Errors++ }

    # 3. CREATE CUBE
    $ddl  = "CREATE CUBE IF NOT EXISTS _smoke_events"
    $ddl += " (event_time DATETIME NOT NULL,"
    $ddl += " user_id VARCHAR(64) NOT NULL,"
    $ddl += " event_name VARCHAR(128) NOT NULL)"
    $ddl += " PARTITION BY RANGE(event_time) (PARTITION AUTO GRANULARITY DAY)"
    $ddl += " DISTRIBUTED BY HASH(user_id) BUCKETS 4"
    $ddl += " ORDER BY (user_id, event_time)"
    $r = Invoke-Sql $ddl
    if ($r.Ok) {
        Log-Ok "CREATE CUBE _smoke_events"
    } else {
        Log-Fail "CREATE CUBE failed: $($r.Out)"
        $Script:Errors++; return
    }

    # 4. INSERT
    $ins  = "INSERT INTO _smoke_events VALUES"
    $ins += " (NOW(), 'u-001', 'page_view'),"
    $ins += " (NOW(), 'u-002', 'purchase')"
    $r = Invoke-Sql $ins
    if ($r.Ok) { Log-Ok "INSERT 2 rows" } else { Log-Fail "INSERT failed: $($r.Out)"; $Script:Errors++; return }

    # 5. SELECT COUNT
    $r = Invoke-SqlVal "SELECT COUNT(*) FROM _smoke_events"
    if ($r.Ok -and ([int]$r.Out -ge 2)) {
        Log-Ok "SELECT COUNT(*) = $($r.Out)  (>= 2)"
    } else {
        Log-Fail "SELECT COUNT failed, got: $($r.Out)"
        $Script:Errors++
    }

    # 6. cleanup
    Invoke-Sql "DROP CUBE IF EXISTS _smoke_events" | Out-Null
    Log-Ok "DROP CUBE _smoke_events (cleanup)"
}

function Show-LogTail {
    param([int]$Lines = 10)
    Log-Step "Recent error logs"
    foreach ($node in @("sn", "cn", "qn")) {
        $f = "$LogDir\$node-err.log"
        if (Test-Path $f) {
            $c = Get-Content $f -Tail $Lines
            if ($c) {
                Write-Host "  -- $($node.ToUpper()) stderr --" -ForegroundColor DarkGray
                $c | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
            }
        }
    }
}

function Build-All {
    Log-Step "cargo build ($BinProfile)"
    Push-Location $RepoRoot
    try {
        if ($Release) {
            cargo build --release -p storage-node -p compute-node -p query-node
        } else {
            cargo build -p storage-node -p compute-node -p query-node
        }
        Log-Ok "Build complete"
    } finally { Pop-Location }
}

function Run-Cycle {
    param([int]$CycleNum)
    $Script:Errors = 0
    $t0 = Get-Date
    $ts = Get-Date -Format "HH:mm:ss"

    Write-Host ""
    Write-Host ("=" * 60) -ForegroundColor Magenta
    Write-Host "  Cycle $CycleNum of $Cycles  at $ts" -ForegroundColor Magenta
    Write-Host ("=" * 60) -ForegroundColor Magenta

    Log-Step "[1/3] Storage Node"
    if (-not (Start-StorageNode)) { Stop-Cluster; return $false }

    Log-Step "[2/3] Compute Node"
    if (-not (Start-ComputeNode)) { Stop-Cluster; return $false }

    Log-Step "[3/3] Query Node"
    if (-not (Start-QueryNode))   { Stop-Cluster; return $false }

    Check-ProcessAlive
    Run-HealthChecks
    Run-MysqlSmoke

    $elapsed = [math]::Round(((Get-Date) - $t0).TotalSeconds, 1)
    Write-Host ""
    Write-Host ("-" * 60) -ForegroundColor DarkGray

    if ($Script:Errors -eq 0) {
        Log-Ok "Cycle $CycleNum PASSED (${elapsed}s)"
        $result = $true
    } else {
        Log-Fail "Cycle $CycleNum FAILED errors=$($Script:Errors) (${elapsed}s)"
        Show-LogTail
        $result = $false
    }

    if ($Watch -and ($CycleNum -eq $Cycles)) {
        Write-Host ""
        Write-Host "[ WATCH ] Cluster is running -- Ctrl+C to stop" -ForegroundColor Yellow
        Write-Host "  mysql -h 127.0.0.1 -P 9030 -u admin -p''" -ForegroundColor Cyan
        Write-Host "  http://localhost:8080  (Web UI)" -ForegroundColor Cyan
        Write-Host "  http://localhost:8040/api/v1/info  (SN)" -ForegroundColor Cyan
        Write-Host "  http://localhost:9040/api/v1/info  (CN)" -ForegroundColor Cyan
        Write-Host ""
        try {
            while ($true) {
                Start-Sleep -Seconds 5
                $ts2 = Get-Date -Format "HH:mm:ss"
                $line = "[$ts2]"
                foreach ($kv in $Script:Procs.GetEnumerator()) {
                    $alive = if (-not $kv.Value.HasExited) { "UP" } else { "DOWN" }
                    $line += "  $($kv.Key):$alive"
                }
                Write-Host $line -ForegroundColor DarkGray
            }
        } catch { Write-Host "" }
        Stop-Cluster
        return $result
    }

    Stop-Cluster
    return $result
}

# =============================================================================
# Main
# =============================================================================
try {
    Write-Host ""
    Write-Host "  WOW-DB dev-test.ps1" -ForegroundColor Cyan
    Write-Host "  SN:8040  CN:9040  QN:9030/8080" -ForegroundColor Cyan
    Write-Host ""

    if (-not $OnlyTest) {
        foreach ($port in @(8040, 9040, 9030, 8080)) {
            $inUse = $false
            try {
                $t = New-Object System.Net.Sockets.TcpClient
                $t.Connect("127.0.0.1", $port)
                $t.Close()
                $inUse = $true
            } catch {}
            if ($inUse) {
                Log-Warn "Port $port already in use -- stop existing processes first"
            }
        }
    }

    if ((-not $NoBuild) -and (-not $OnlyTest)) {
        Build-All
    }

    if ($OnlyTest) {
        $Script:Errors = 0
        Run-HealthChecks
        Run-MysqlSmoke
        exit $(if ($Script:Errors -eq 0) { 0 } else { 1 })
    }

    $totalFail = 0
    for ($c = 1; $c -le $Cycles; $c++) {
        if (-not (Run-Cycle -CycleNum $c)) { $totalFail++ }
        if ($c -lt $Cycles) {
            Log-Info "Waiting 2s before next cycle..."
            Start-Sleep -Seconds 2
        }
    }

    Write-Host ""
    $color = if ($totalFail -eq 0) { "Green" } else { "Red" }
    Write-Host ("=" * 60) -ForegroundColor $color
    if ($totalFail -eq 0) {
        Write-Host "  ALL $Cycles CYCLE(S) PASSED" -ForegroundColor Green
    } else {
        Write-Host "  FAILED: $totalFail / $Cycles cycles" -ForegroundColor Red
    }
    Write-Host ("=" * 60) -ForegroundColor $color
    Write-Host ""

    exit $(if ($totalFail -eq 0) { 0 } else { 1 })

} finally {
    Stop-Cluster
}
