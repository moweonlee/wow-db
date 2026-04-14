#!/usr/bin/env bash
# WOW-DB 로컬 개발 데이터 초기화 스크립트
# 경고: 로컬 개발 데이터를 전부 삭제합니다.

DATA_DIR="${WOWDB_DATA_DIR:-/tmp/wowdb-dev}"

YELLOW='\033[1;33m'; GREEN='\033[0;32m'; NC='\033[0m'
log()  { echo -e "${GREEN}[WOW-DB]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }

# 실행 중 확인
PID_FILE="$DATA_DIR/wowdb.pids"
if [[ -f "$PID_FILE" ]]; then
    warn "Cluster appears to be running. Stop it first:"
    warn "  ./scripts/dev/stop-local.sh"
    exit 1
fi

warn "This will delete all local WOW-DB data at: $DATA_DIR"
read -rp "Continue? [y/N] " confirm
if [[ "${confirm,,}" != "y" ]]; then
    log "Aborted."
    exit 0
fi

rm -rf "$DATA_DIR"
log "Local data cleared: $DATA_DIR"
log "Run './scripts/dev/run-local.sh' to start fresh."
