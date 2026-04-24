# wow-db Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-04-24

## Active Technologies
- Rust 1.87 stable + axum (already in use), tokio, serde_json, tracing (002-wow-db-srs-v02)
- 읽기 전용 — Raft KV (cube 목록), SN gRPC scan (LSM 상태 폴링) (002-wow-db-srs-v02)
- Rust 1.87 stable (DB engine), Python 3.10+ (benchmark/loader scripts), Bash/PowerShell (test harness) + tokio, axum, tonic, opensrv-mysql, sqlparser (engine); mysql-connector-python, tabulate (scripts) (002-wow-db-srs-v02)
- Native LSM-Tree (local NVMe/SSD); MinIO S3-compatible (optional cold tier) (002-wow-db-srs-v02)

- Rust 1.87 stable (주), C++ 없음 (rdkafka의 librdkafka 제외) (002-wow-db-srs-v02)

## Project Structure

```text
query-node/       # Query Node: MySQL Protocol, SQL Parser, CBO, Raft, Web UI
compute-node/     # Compute Node: SIMD Executor, Hash Join, Analytics Functions
storage-node/     # Storage Node: LSM-Tree, Columnar Files, S3/HDFS Backend
shared/           # 공통 타입, Arrow IPC 코덱, 에러 처리
proto/            # Protobuf 정의 (tonic+prost)
integration-tests/
docker/           # Docker Compose, Dockerfiles, 설정 템플릿 (Docker 실행용)
dev/configs/      # 로컬 Native 실행 설정 (query/compute/storage-node-local.toml)
scripts/dev/      # 로컬 실행 스크립트 (run/stop/reset-local.sh + .ps1)
```

## Commands

```bash
# ── 로컬 Native 실행 (Docker 없이, 빠른 개발) ──────────────────────────────
./scripts/dev/run-local.sh          # QN×1+CN×1+SN×1 기동 (Linux/macOS)
./scripts/dev/run-local.sh --no-build  # 빌드 생략
./scripts/dev/stop-local.sh         # 종료
./scripts/dev/reset-local.sh        # 데이터 초기화

.\scripts\dev\run-local.ps1         # Windows PowerShell
.\scripts\dev\stop-local.ps1        # 종료
.\scripts\dev\reset-local.ps1 -Force  # 데이터 초기화

# ── 단위 테스트 ──────────────────────────────────────────────────────────────
cargo test --workspace              # 전체 단위 테스트
cargo test -p query-node            # 특정 모듈 테스트

# ── Docker 클러스터 (E2E, 통합 테스트) ───────────────────────────────────────
docker compose up --build           # 전체 클러스터 기동 (QN×3, CN×2, SN×3)
docker compose -f docker/docker-compose.dev.yml up --build  # 개발용 단일 노드
docker compose down -v              # 클러스터 종료 + 데이터 초기화

# ── 코드 품질 ────────────────────────────────────────────────────────────────
cargo clippy --workspace            # Lint

# ── TPC-H 벤치마크 스크립트 ────────────────────────────────────────────────
# Prerequisites: pip install pymysql
python scripts/tpch/bench.py --sf 0.01                         # SF=0.01 correctness
python scripts/tpch/bench.py --sf 0.01 --queries all           # all 22 queries
python scripts/tpch/bench.py --sf 0.01 --queries all --output json --report out.json
python scripts/tpch/bench.py --no-load --queries Q01           # skip data load
# Port: default 9030 (full cluster); use --port 19030 for single-node dev

# ── 테스트 스크립트 ──────────────────────────────────────────────────────────
python scripts/test/persistence_test.py   # graceful + hard-kill restart durability
python scripts/test/distribution_test.py  # per-SN row count skew (±20% tolerance)
python scripts/test/concurrent_test.py    # 1 writer + 5 readers, 10s, 0 errors

# ── 샘플 데이터 로드 ─────────────────────────────────────────────────────────
mysql -h 127.0.0.1 -P 9030 -u root < samples/web-analytics/schema.sql
python samples/web-analytics/generate.py  # 500 sessions, ~10K events
mysql -h 127.0.0.1 -P 9030 -u root < samples/teardown.sql     # clean up all sample tables
```

## Code Style

Rust 1.87 stable (주), C++ 없음 (rdkafka의 librdkafka 제외): Follow standard conventions

## Recent Changes
- 002-wow-db-srs-v02: Added Rust 1.87 stable (DB engine), Python 3.10+ (benchmark/loader scripts), Bash/PowerShell (test harness) + tokio, axum, tonic, opensrv-mysql, sqlparser (engine); mysql-connector-python, tabulate (scripts)
- 002-wow-db-srs-v02: Added Rust 1.87 stable + axum (already in use), tokio, serde_json, tracing

- 002-wow-db-srs-v02: Added Rust 1.87 stable (주), C++ 없음 (rdkafka의 librdkafka 제외)

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
