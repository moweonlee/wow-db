#!/usr/bin/env bash
# =============================================================================
# WOW-DB 로컬 단일 인스턴스 실행 스크립트 (Linux / macOS)
# 용도: Docker 없이 로컬에서 빠르게 QN×1 + CN×1 + SN×1 기동
#
# 사용법:
#   ./scripts/dev/run-local.sh              # 빌드 후 기동
#   ./scripts/dev/run-local.sh --no-build   # 빌드 생략, 기존 바이너리 사용
#   ./scripts/dev/run-local.sh --release    # release 빌드 (기본: dev)
#
# 종료: Ctrl+C (자동으로 모든 프로세스 정리)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONFIG_DIR="$REPO_ROOT/dev/configs"
DATA_DIR="${WOWDB_DATA_DIR:-/tmp/wowdb-dev}"
LOG_DIR="${WOWDB_LOG_DIR:-/tmp/wowdb-dev/logs}"
PID_FILE="$DATA_DIR/wowdb.pids"

BUILD=true
PROFILE="dev"

# 인수 파싱
for arg in "$@"; do
    case "$arg" in
        --no-build)  BUILD=false ;;
        --release)   PROFILE="release" ;;
    esac
done

# 출력 헬퍼
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()    { echo -e "${GREEN}[WOW-DB]${NC} $*"; }
info()   { echo -e "${CYAN}[INFO]${NC}   $*"; }
warn()   { echo -e "${YELLOW}[WARN]${NC}   $*"; }
err()    { echo -e "${RED}[ERR]${NC}    $*" >&2; }

PIDS=()

cleanup() {
    echo ""
    log "Stopping WOW-DB local cluster..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    # PID 파일 제거
    rm -f "$PID_FILE"
    wait "${PIDS[@]}" 2>/dev/null || true
    log "All processes stopped. Logs: $LOG_DIR"
}
trap cleanup EXIT INT TERM

# 이미 실행 중인지 확인
if [[ -f "$PID_FILE" ]]; then
    warn "PID file found at $PID_FILE. Already running?"
    warn "Run './scripts/dev/stop-local.sh' first to stop existing processes."
    exit 1
fi

# 디렉토리 준비
mkdir -p "$DATA_DIR/sn" "$LOG_DIR"

# =============================================================================
# 빌드
# =============================================================================
if [[ "$BUILD" == true ]]; then
    log "Building WOW-DB ($PROFILE)..."
    cd "$REPO_ROOT"
    if [[ "$PROFILE" == "release" ]]; then
        cargo build --release -p storage-node -p compute-node -p query-node
    else
        cargo build -p storage-node -p compute-node -p query-node
    fi
    log "Build complete."
fi

if [[ "$PROFILE" == "release" ]]; then
    BIN="$REPO_ROOT/target/release"
else
    BIN="$REPO_ROOT/target/debug"
fi

# 바이너리 존재 확인
for bin in storage-node compute-node query-node; do
    if [[ ! -f "$BIN/$bin" ]]; then
        err "Binary not found: $BIN/$bin"
        err "Run without --no-build to build first."
        exit 1
    fi
done

echo ""
log "======================================================================"
log " WOW-DB Local Cluster (QN×1 + CN×1 + SN×1)"
log " Data  : $DATA_DIR"
log " Logs  : $LOG_DIR"
log " Config: $CONFIG_DIR"
log "======================================================================"
echo ""

# =============================================================================
# Storage Node 기동
# =============================================================================
log "[1/3] Starting Storage Node..."
info "  gRPC  : 127.0.0.1:9060"
info "  HTTP  : 127.0.0.1:8040  (Spark Stream Load)"
info "  Data  : $DATA_DIR/sn"
info "  Log   : $LOG_DIR/sn.log"

RUST_LOG="${RUST_LOG:-storage_node=info,shared=info}" \
NODE_ID="sn-local-1" \
DATA_DIR="$DATA_DIR/sn" \
"$BIN/storage-node" --config "$CONFIG_DIR/storage-node-local.toml" \
    > "$LOG_DIR/sn.log" 2>&1 &
SN_PID=$!
PIDS+=($SN_PID)

# SN 기동 대기 (gRPC 포트 오픈 확인)
for i in $(seq 1 20); do
    if (echo > /dev/tcp/127.0.0.1/9060) 2>/dev/null; then
        log "  Storage Node ready (PID $SN_PID)"
        break
    fi
    if [[ $i -eq 20 ]]; then
        err "Storage Node did not start in time. Check $LOG_DIR/sn.log"
        exit 1
    fi
    sleep 0.5
done

# =============================================================================
# Compute Node 기동
# =============================================================================
log "[2/3] Starting Compute Node..."
info "  gRPC  : 127.0.0.1:9040"
info "  Log   : $LOG_DIR/cn.log"

RUST_LOG="${RUST_LOG:-compute_node=info,shared=info}" \
NODE_ID="cn-local-1" \
STORAGE_NODES="127.0.0.1:9060" \
"$BIN/compute-node" --config "$CONFIG_DIR/compute-node-local.toml" \
    > "$LOG_DIR/cn.log" 2>&1 &
CN_PID=$!
PIDS+=($CN_PID)

for i in $(seq 1 20); do
    if (echo > /dev/tcp/127.0.0.1/9040) 2>/dev/null; then
        log "  Compute Node ready (PID $CN_PID)"
        break
    fi
    if [[ $i -eq 20 ]]; then
        err "Compute Node did not start in time. Check $LOG_DIR/cn.log"
        exit 1
    fi
    sleep 0.5
done

# =============================================================================
# Query Node 기동
# =============================================================================
log "[3/3] Starting Query Node..."
info "  MySQL : 127.0.0.1:9030"
info "  Web UI: http://127.0.0.1:8080"
info "  Raft  : 127.0.0.1:9010 (single-node mode)"
info "  Log   : $LOG_DIR/qn.log"

RUST_LOG="${RUST_LOG:-query_node=info,shared=info}" \
NODE_ID="qn-local-1" \
RAFT_PEERS="qn-local-1:9010" \
COMPUTE_NODES="127.0.0.1:9040" \
QN_PEERS="qn-local-1:9011" \
"$BIN/query-node" --config "$CONFIG_DIR/query-node-local.toml" \
    > "$LOG_DIR/qn.log" 2>&1 &
QN_PID=$!
PIDS+=($QN_PID)

for i in $(seq 1 30); do
    if (echo > /dev/tcp/127.0.0.1/9030) 2>/dev/null; then
        log "  Query Node ready (PID $QN_PID)"
        break
    fi
    if [[ $i -eq 30 ]]; then
        err "Query Node did not start in time. Check $LOG_DIR/qn.log"
        exit 1
    fi
    sleep 0.5
done

# PID 파일 기록
echo "SN=$SN_PID" >  "$PID_FILE"
echo "CN=$CN_PID" >> "$PID_FILE"
echo "QN=$QN_PID" >> "$PID_FILE"

echo ""
log "======================================================================"
log " WOW-DB local cluster is READY"
log "======================================================================"
log ""
log "  MySQL 접속:  mysql -h 127.0.0.1 -P 9030 -u admin -p''"
log "  Web UI:      http://localhost:8080"
log "  Metrics:     http://localhost:8080/metrics"
log "  로그 확인:   tail -f $LOG_DIR/qn.log"
log ""
log "  빠른 테스트:"
log "    mysql -h 127.0.0.1 -P 9030 -u admin -p'' < test_suite.sql"
log ""
log "  종료: Ctrl+C  또는  ./scripts/dev/stop-local.sh"
log "======================================================================"
echo ""

# 프로세스가 종료될 때까지 대기
wait "${PIDS[@]}"
