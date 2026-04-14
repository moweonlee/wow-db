#!/usr/bin/env bash
# =============================================================================
# WOW-DB 로컬 클러스터 자동화 검증 스크립트 (T203 / T204)
#
# T203: 기동 → MySQL 접속 → SHOW TABLES → CREATE TABLE → INSERT → SELECT
# T204: reset → run 3회 반복 정상 동작 확인
#
# 사전 조건:
#   - mysql 클라이언트 (mysql-client 또는 mariadb-client)가 PATH에 있어야 함
#   - 클러스터가 실행 중이거나 --start 플래그로 자동 기동
#
# 사용법:
#   ./scripts/dev/test-local.sh                 # 이미 실행 중인 클러스터 검증
#   ./scripts/dev/test-local.sh --start         # 클러스터 기동 후 검증 후 종료
#   ./scripts/dev/test-local.sh --cycles 3      # T204: 3회 반복 reset+run 검증
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

QN_HOST="127.0.0.1"
QN_PORT="9030"
QN_USER="admin"
QN_PASS=""
MYSQL_TIMEOUT=5        # seconds
START_CLUSTER=false
STOP_AFTER=false
CYCLES=1               # number of reset+run cycles (T204)

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()   { echo -e "${GREEN}[TEST]${NC}  $*"; }
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
pass()  { echo -e "${GREEN}[PASS]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*" >&2; }

for arg in "$@"; do
    case "$arg" in
        --start)         START_CLUSTER=true; STOP_AFTER=true ;;
        --cycles=*)      CYCLES="${arg#--cycles=}" ;;
        --cycles)        shift; CYCLES="$1" ;;
    esac
done

# ─── 헬퍼: mysql 클라이언트 존재 확인 ────────────────────────────────────────

check_mysql_client() {
    if ! command -v mysql &>/dev/null; then
        warn "mysql client not found — skipping live SQL validation."
        warn "Install: apt-get install mysql-client  OR  brew install mysql-client"
        return 1
    fi
    return 0
}

# ─── 헬퍼: MySQL 연결 대기 ────────────────────────────────────────────────────

wait_for_mysql() {
    local max=20
    info "Waiting for MySQL on $QN_HOST:$QN_PORT..."
    for i in $(seq 1 $max); do
        if mysql -h "$QN_HOST" -P "$QN_PORT" -u "$QN_USER" \
            --password="$QN_PASS" \
            --connect-timeout="$MYSQL_TIMEOUT" \
            -e "SELECT 1" &>/dev/null 2>&1; then
            log "MySQL is ready (attempt $i)"
            return 0
        fi
        sleep 0.5
    done
    fail "MySQL not reachable at $QN_HOST:$QN_PORT after ${max} attempts"
    return 1
}

# ─── T203: 단일 플로우 검증 ──────────────────────────────────────────────────

run_sql_validation() {
    if ! check_mysql_client; then
        log "Skipping live SQL check (no mysql client)"
        return 0
    fi

    wait_for_mysql || return 1

    local start_ts
    start_ts=$(date +%s%3N)    # ms since epoch

    log "Running SQL validation flow..."

    # SHOW TABLES
    mysql -h "$QN_HOST" -P "$QN_PORT" -u "$QN_USER" \
        --password="$QN_PASS" --connect-timeout="$MYSQL_TIMEOUT" \
        -e "SHOW TABLES;" >/dev/null
    pass "SHOW TABLES"

    # CREATE TABLE
    mysql -h "$QN_HOST" -P "$QN_PORT" -u "$QN_USER" \
        --password="$QN_PASS" --connect-timeout="$MYSQL_TIMEOUT" \
        -e "CREATE TABLE IF NOT EXISTS _test_events (
                event_time   DATETIME     NOT NULL,
                user_id      VARCHAR(64)  NOT NULL,
                event_name   VARCHAR(128) NOT NULL
            )
            PARTITION BY RANGE(event_time) ()
            AUTO PARTITION BY DAY
            ORDER BY (event_time, user_id)
            DISTRIBUTED BY HASH(user_id) BUCKETS 4;" >/dev/null
    pass "CREATE TABLE _test_events"

    # INSERT
    mysql -h "$QN_HOST" -P "$QN_PORT" -u "$QN_USER" \
        --password="$QN_PASS" --connect-timeout="$MYSQL_TIMEOUT" \
        -e "INSERT INTO _test_events VALUES
            (NOW(), 'u-001', 'page_view'),
            (NOW(), 'u-002', 'purchase');" >/dev/null
    pass "INSERT (2 rows)"

    # SELECT COUNT(*)
    local count
    count=$(mysql -h "$QN_HOST" -P "$QN_PORT" -u "$QN_USER" \
        --password="$QN_PASS" --connect-timeout="$MYSQL_TIMEOUT" \
        -sN -e "SELECT COUNT(*) FROM _test_events;" 2>/dev/null)
    if [[ "$count" -ge 2 ]]; then
        pass "SELECT COUNT(*) = $count (≥2)"
    else
        fail "SELECT COUNT(*) = $count (expected ≥2)"
        return 1
    fi

    local end_ts
    end_ts=$(date +%s%3N)
    local elapsed=$(( end_ts - start_ts ))
    if [[ "$elapsed" -le 5000 ]]; then
        pass "Full flow completed in ${elapsed}ms (≤5000ms)"
    else
        warn "Full flow took ${elapsed}ms (>5000ms, check cluster performance)"
    fi

    return 0
}

# ─── T204: reset + run 반복 검증 ─────────────────────────────────────────────

run_cycle_validation() {
    local cycle="$1"
    log "=== Cycle $cycle ==="

    # Stop existing cluster
    "$SCRIPT_DIR/stop-local.sh" 2>/dev/null || true

    # Reset data
    "$SCRIPT_DIR/reset-local.sh" --force
    pass "Cycle $cycle: data reset"

    # Start cluster in background
    "$SCRIPT_DIR/run-local.sh" --no-build &
    local run_pid=$!

    # Wait for cluster to be ready (poll MySQL port)
    local up=false
    for i in $(seq 1 40); do
        if (echo > /dev/tcp/127.0.0.1/9030) 2>/dev/null; then
            up=true
            break
        fi
        sleep 0.5
    done

    if [[ "$up" != "true" ]]; then
        kill "$run_pid" 2>/dev/null || true
        fail "Cycle $cycle: cluster did not start within 20s"
        return 1
    fi
    pass "Cycle $cycle: cluster started"

    # SQL validation
    run_sql_validation || { kill "$run_pid" 2>/dev/null || true; return 1; }
    pass "Cycle $cycle: SQL validation OK"

    # Stop cluster
    kill "$run_pid" 2>/dev/null || true
    "$SCRIPT_DIR/stop-local.sh" 2>/dev/null || true
    pass "Cycle $cycle: cluster stopped cleanly"
}

# ─── メイン ──────────────────────────────────────────────────────────────────

ERRORS=0

if [[ "$CYCLES" -gt 1 ]]; then
    # T204: multi-cycle reset+run validation
    log "Running T204 multi-cycle validation ($CYCLES cycles)..."

    # Build once before cycles
    log "Building WOW-DB binaries..."
    cd "$REPO_ROOT"
    cargo build -p storage-node -p compute-node -p query-node 2>&1 | tail -3

    for cycle in $(seq 1 "$CYCLES"); do
        if ! run_cycle_validation "$cycle"; then
            fail "Cycle $cycle FAILED"
            ERRORS=$(( ERRORS + 1 ))
        fi
    done

else
    # T203: single run validation
    if [[ "$START_CLUSTER" == "true" ]]; then
        log "Building and starting local cluster..."
        "$SCRIPT_DIR/run-local.sh" &
        RUN_PID=$!
        sleep 3  # give processes time to start
    fi

    if ! run_sql_validation; then
        ERRORS=$(( ERRORS + 1 ))
    fi

    if [[ "$STOP_AFTER" == "true" ]]; then
        "$SCRIPT_DIR/stop-local.sh" 2>/dev/null || true
        kill "$RUN_PID" 2>/dev/null || true
    fi
fi

echo ""
if [[ "$ERRORS" -eq 0 ]]; then
    pass "============================================================"
    pass "All validations PASSED"
    pass "============================================================"
    exit 0
else
    fail "============================================================"
    fail "$ERRORS validation(s) FAILED"
    fail "============================================================"
    exit 1
fi
