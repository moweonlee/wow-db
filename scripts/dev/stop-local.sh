#!/usr/bin/env bash
# WOW-DB 로컬 클러스터 종료 스크립트
# run-local.sh로 기동된 프로세스를 PID 파일 기반으로 종료

DATA_DIR="${WOWDB_DATA_DIR:-/tmp/wowdb-dev}"
PID_FILE="$DATA_DIR/wowdb.pids"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log()  { echo -e "${GREEN}[WOW-DB]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }

if [[ ! -f "$PID_FILE" ]]; then
    warn "No PID file found at $PID_FILE. Is the cluster running?"
    # 포트 기반 fallback 종료
    for port in 9060 9040 9030; do
        pid=$(lsof -ti tcp:$port 2>/dev/null || true)
        if [[ -n "$pid" ]]; then
            log "Killing process on port $port (PID $pid)"
            kill "$pid" 2>/dev/null || true
        fi
    done
    exit 0
fi

while IFS='=' read -r name pid; do
    if kill -0 "$pid" 2>/dev/null; then
        log "Stopping $name (PID $pid)..."
        kill "$pid" 2>/dev/null || true
    else
        warn "$name (PID $pid) already stopped."
    fi
done < "$PID_FILE"

rm -f "$PID_FILE"
log "All WOW-DB processes stopped."
