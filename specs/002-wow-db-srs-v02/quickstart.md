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

## 2. 전체 스택 로컬 실행 (Docker Compose)

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

-- 3. Session MV 생성
CREATE SESSION MATERIALIZED VIEW page_events_sessions
FROM page_events
USER KEY (user_id)
SESSION TIMEOUT 30 MINUTE;

-- 4. Funnel 분석
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

## 9. 클러스터 종료 및 데이터 초기화

```bash
# 클러스터 종료 (데이터 유지)
docker compose down

# 클러스터 종료 + 볼륨 삭제 (데이터 초기화)
docker compose down -v
```
