# WOW-DB 로컬 개발 빠른 시작

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-12

---

## 사전 요구 사항

| 도구 | 버전 | 비고 |
|---|---|---|
| Rust | 1.87+ | `rustup update stable` |
| Docker | 25.0+ | Docker Desktop (Mac/Windows) 또는 Docker Engine (Linux) |
| Docker Compose | v2.24+ | `docker compose version` |
| protoc | 3.21+ | `brew install protobuf` / `apt install protobuf-compiler` |
| grpc_health_probe | 최신 | 헬스체크 CLI |

---

## 1. 저장소 클론 및 의존성 확인

```bash
git clone <repo-url> wow-db
cd wow-db
git checkout 002-wow-db-srs-v02

# Rust 도구체인 설치
rustup target add x86_64-unknown-linux-gnu

# protoc 생성 확인
cargo build -p shared 2>&1 | head -20
```

---

## 2. 로컬 Native 실행 (Docker 없이, 빠른 개발용) ⚡

Docker 빌드 시간 없이 `cargo build` 후 바이너리를 직접 실행하는 방법이다.  
**QN×1 + CN×1 + SN×1** 단일 인스턴스 구성으로, 기능 개발 및 단위 테스트에 적합하다.

### 사전 요구 사항 (추가)

| 도구 | 용도 |
|---|---|
| Rust 1.87+ | 바이너리 빌드 |
| protoc 3.21+ | gRPC 코드 생성 |
| mysql CLI | 접속 테스트 |

### Linux / macOS

```bash
# 기동 (빌드 포함)
./scripts/dev/run-local.sh

# 빌드 생략 (이미 빌드된 경우)
./scripts/dev/run-local.sh --no-build

# release 빌드로 기동 (성능 테스트 시)
./scripts/dev/run-local.sh --release

# 다른 터미널에서 종료
./scripts/dev/stop-local.sh

# 데이터 초기화 (재시작 시 깨끗한 상태로)
./scripts/dev/reset-local.sh
```

### Windows (PowerShell)

```powershell
# 기동 (빌드 포함)
.\scripts\dev\run-local.ps1

# 빌드 생략
.\scripts\dev\run-local.ps1 -NoBuild

# release 빌드
.\scripts\dev\run-local.ps1 -Release

# 데이터 디렉토리 지정
.\scripts\dev\run-local.ps1 -DataDir D:\wowdb-dev

# 종료
.\scripts\dev\stop-local.ps1

# 데이터 초기화
.\scripts\dev\reset-local.ps1 -Force
```

### 기동 후 확인

```bash
# MySQL 접속
mysql -h 127.0.0.1 -P 9030 -u admin -p''

# Web UI
open http://localhost:8080        # macOS
start http://localhost:8080       # Windows

# 로그 확인 (Linux/macOS)
tail -f /tmp/wowdb-dev/logs/qn.log

# 로그 확인 (Windows)
Get-Content "$env:TEMP\wowdb-dev\logs\qn.log" -Wait
```

### 로컬 설정 파일

| 파일 | 용도 |
|---|---|
| `dev/configs/query-node-local.toml` | QN 단일 노드 Raft, 소형 버퍼 |
| `dev/configs/compute-node-local.toml` | CN localhost SN 연결 |
| `dev/configs/storage-node-local.toml` | SN native backend, 복제본 1개 |

환경변수로 런타임 오버라이드 가능:

```bash
WOWDB_DATA_DIR=/data/dev RUST_LOG=debug ./scripts/dev/run-local.sh
```

### Docker와 비교

| 항목 | Local Native | Docker Compose Dev |
|---|---|---|
| 기동 시간 | ~5초 (빌드 후) | 30~120초 |
| 노드 구성 | QN×1 + CN×1 + SN×1 | 동일 |
| 데이터 경로 | `/tmp/wowdb-dev/sn` | Docker Volume |
| Kafka/MinIO | 불필요 | 포함 |
| 용도 | 기능 개발, 단위 테스트 | E2E 테스트, Kafka 수집 테스트 |

---

## 3. 전체 스택 로컬 실행 (Docker Compose)

```bash
# 전체 클러스터 기동 (QN×3, CN×2, SN×3, MinIO, Redpanda)
docker compose up --build

# 백그라운드 실행
docker compose up --build -d

# 상태 확인
docker compose ps
```

**서비스 기동 순서** (자동 헬스체크 기반):
1. MinIO, Redpanda (인프라 서비스)
2. Storage Node × 3 (`sn-1`, `sn-2`, `sn-3`)
3. Compute Node × 2 (`cn-1`, `cn-2`)
4. Query Node × 3 (`qn-1`, `qn-2`, `qn-3`) — Raft Leader 선출

**정상 기동 확인 (약 30-60초 소요):**
```bash
# MySQL 클라이언트로 접속
mysql -h 127.0.0.1 -P 9030 -u admin -p''

# Web UI 접속
open http://localhost:8080

# 모니터링 메트릭
curl http://localhost:8080/metrics | head -30
```

---

## 3. 부분 스택 실행 (개발 프로파일)

```bash
# Storage Node만 기동
docker compose --profile storage up -d

# Storage Node + Compute Node
docker compose --profile compute up -d

# 인프라 서비스만 (MinIO, Redpanda)
docker compose --profile infra up -d
```

---

## 4. 개발 중 빠른 재빌드 (Docker Compose Watch)

```bash
# 소스 변경 감지 → 자동 컨테이너 재빌드
docker compose watch

# 특정 노드만 재빌드
docker compose watch query-node
```

---

## 5. 단위 테스트 및 통합 테스트

```bash
# 전체 단위 테스트
cargo test --workspace

# 특정 모듈 단위 테스트
cargo test -p query-node
cargo test -p compute-node
cargo test -p storage-node

# 통합 테스트 (Docker Compose 클러스터 필요)
docker compose up -d --wait  # 클러스터 준비 대기
cargo test -p integration-tests --features integration

# 커버리지 (cargo-tarpaulin 필요)
cargo tarpaulin --workspace --out Html
```

---

## 6. 샘플 데이터 로드 및 쿼리

```sql
-- MySQL 클라이언트 접속 후

-- 1. Cube 생성
CREATE CUBE IF NOT EXISTS page_events (
    event_time   DATETIME     NOT NULL,
    user_id      VARCHAR(64)  NOT NULL,
    event_name   VARCHAR(128) NOT NULL,
    page_url     TEXT,
    properties   JSON
)
AUTO PARTITION BY DAY
ORDER BY (event_time, user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 8;

-- 2. 샘플 이벤트 INSERT
INSERT INTO page_events VALUES
('2026-04-12 10:00:00', 'user_001', 'page_view',   '/home',     '{"ref":"google"}'),
('2026-04-12 10:01:00', 'user_001', 'add_to_cart', '/products', '{"item":"A100"}'),
('2026-04-12 10:03:00', 'user_001', 'purchase',    '/checkout', '{"amount":49.99}'),
('2026-04-12 10:00:30', 'user_002', 'page_view',   '/home',     '{}'),
('2026-04-12 10:05:00', 'user_002', 'page_view',   '/products', '{}');

-- 3. Behavioral Table 생성 (Session MV)
--    Funnel / Cohort / Path 분석에 최적화된 물리 레이아웃.
--    생성 후 FUNNEL_COUNT 쿼리는 자동으로 이 테이블을 사용한다 (Behavioral Routing).
CREATE SESSION MATERIALIZED VIEW page_events_sessions
FROM page_events
USER KEY (user_id)
SESSION TIMEOUT 30 MINUTE;

-- 4. Funnel 분석
--    FROM page_events 에 쿼리하지만, Behavioral Router가 자동으로
--    page_events_sessions (Behavioral Table) 을 사용하여 실행한다.
SELECT FUNNEL_COUNT(
    user_key  => user_id,
    timestamp => event_time,
    window    => INTERVAL 24 HOUR,
    steps     => [
        event_name = 'page_view',
        event_name = 'add_to_cart',
        event_name = 'purchase'
    ]
) AS funnel
FROM page_events
WHERE event_time >= '2026-04-12';
```

---

## 7. Kafka 수집 테스트

```bash
# Redpanda 토픽 생성
docker compose exec redpanda \
  rpk topic create web-events --partitions 4

# 테스트 이벤트 전송
docker compose exec redpanda \
  rpk topic produce web-events <<'EOF'
{"event_time":"2026-04-12T10:00:00Z","user_id":"u001","event_name":"page_view"}
{"event_time":"2026-04-12T10:01:00Z","user_id":"u001","event_name":"purchase"}
EOF
```

```sql
-- Routine Load 잡 생성
CREATE ROUTINE LOAD web_events_load ON page_events
FORMAT AS JSON
FROM KAFKA (
    "kafka_broker_list" = "redpanda:9092",
    "kafka_topic"       = "web-events"
);

-- 잡 상태 확인
SHOW ROUTINE LOAD FOR web_events_load;
```

---

## 8. 로그 및 디버깅

```bash
# 특정 노드 로그
docker compose logs -f qn-1
docker compose logs -f cn-1
docker compose logs -f sn-1

# 전체 로그 (최근 100줄)
docker compose logs --tail 100

# Query Profiler (Web UI)
open http://localhost:8080/profiler

# Prometheus 메트릭
curl http://localhost:8080/metrics
```

---

## 9. 데이터 분포 가시성 — SHOW 명령 (FR-043~FR-046)

WOW-DB는 데이터가 파티션·Shard·LSM Part 단위로 Storage Node에 어떻게 분산되어 있는지를 MySQL 클라이언트에서 직접 확인할 수 있는 계층별 SHOW 명령을 제공한다.

```
Cube
  └─ SHOW PARTITIONS → Partition (시간/키 범위 단위)
        └─ SHOW SHARDS → Shard/Tablet (SN 배치 단위, 분산 키 기준)
              └─ SHOW PARTS → Part (LSM SSTable, 실제 물리 파일 단위)
```

### 9.1 SHOW PARTITIONS — 파티션 분포 조회

```sql
-- 전체 파티션 목록 (partition_id, 키 범위, row_count, size_bytes, shard_count, tier)
SHOW PARTITIONS FROM page_events;

-- 특정 범위 파티션 필터 (파티션 키가 event_time인 경우)
SHOW PARTITIONS FROM page_events WHERE range_start >= '2024-01-01';

-- 크기 내림차순 — 가장 큰 파티션 확인
SHOW PARTITIONS FROM page_events ORDER BY size_bytes DESC LIMIT 10;
```

### 9.2 SHOW SHARDS — Shard 분산 조회

```sql
-- 전체 Shard 목록 (shard_id, 소속 SN, bucket_id, role, row_count, part_count)
SHOW SHARDS FROM page_events;

-- 특정 파티션의 Shard만
SHOW SHARDS FROM page_events PARTITION '<partition_id>';

-- 특정 SN에 몰린 Shard 파악 (데이터 편중 진단)
SHOW SHARDS FROM page_events WHERE sn_node_id = 'sn-01';

-- Hot Shard 파악 (크기 기준 내림차순)
SHOW SHARDS FROM page_events ORDER BY size_bytes DESC LIMIT 20;
```

### 9.3 SHOW PARTS — LSM Part(SSTable) 조회

```sql
-- 전체 Part 목록 (part_id, shard_id, level, seq_num, sort key 범위, 크기)
SHOW PARTS FROM page_events;

-- 특정 파티션의 Part만 (Partition 단위 drill-down)
SHOW PARTS FROM page_events PARTITION '<partition_id>';

-- 파티션 우선 명시 방식 (별칭 구문)
SHOW PARTS ON PARTITION '<partition_id>' FROM page_events;

-- 특정 Shard의 Part만 (Shard 단위 drill-down)
SHOW PARTS FROM page_events SHARD '<shard_id>';

-- L0 Part만 조회 — 많으면 Compaction 지연 신호
SHOW PARTS FROM page_events WHERE level = 0;

-- 특정 SN의 대용량 Part 조회
SHOW PARTS FROM page_events WHERE sn_node_id = 'sn-01' ORDER BY size_bytes DESC;
```

> **운영 팁**: `SHOW PARTS WHERE level = 0` 결과가 수백 개 이상이면 Compaction이 쓰기 속도를 따라가지 못하는 것을 의미한다. L0 Part 비율이 높으면 읽기 성능도 저하된다.

### 9.4 SHOW DISTRIBUTED STATUS — 노드별 분포 요약

```sql
-- SN별 Shard 수, 총 크기, 행 수, 평균 Part 수 요약
SHOW DISTRIBUTED STATUS FROM page_events;

-- 가장 부하가 높은 SN 파악
SHOW DISTRIBUTED STATUS FROM page_events ORDER BY size_bytes DESC;
```

**결과 예시**:

| sn_node_id | sn_endpoint | shard_count | leader_shard_count | row_count | size_bytes | avg_part_per_shard |
|------------|-------------|-------------|-------------------|-----------|------------|-------------------|
| sn-01 | 10.0.0.1:9060 | 8 | 4 | 2,500,000,000 | 480 GB | 3.2 |
| sn-02 | 10.0.0.2:9060 | 8 | 4 | 2,480,000,000 | 475 GB | 3.1 |
| sn-03 | 10.0.0.3:9060 | 8 | 4 | 2,510,000,000 | 482 GB | 3.3 |

> `shard_count`와 `size_bytes`가 특정 SN에 편중되어 있으면 분산 키 선택을 재검토해야 한다.

---

## 10. 클러스터 종료 및 데이터 초기화

```bash
# 클러스터 종료 (데이터 유지)
docker compose down

# 클러스터 종료 + 볼륨 삭제 (데이터 초기화)
docker compose down -v
```
