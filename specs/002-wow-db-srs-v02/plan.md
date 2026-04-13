# 구현 계획: WOW-DB 웹 분석 데이터베이스 플랫폼

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-12 | **Spec**: [spec.md](spec.md)  
**Input**: specs/002-wow-db-srs-v02/spec.md

---

## Summary

WOW-DB는 200억+ 레코드 규모의 웹 이벤트 분석에 특화된 MySQL 호환 OLAP 데이터베이스이다.  
**Rust-first** (SIMD AVX2/AVX-512 직접 인트린식) + **3-Tier 분산 아키텍처** (Query Node / Compute Node / Storage Node)로 구현한다. 모든 노드 간 통신은 `tonic` + `prost` gRPC, 메타데이터 고가용성은 `openraft`, MySQL 클라이언트 호환은 `opensrv-mysql`로 처리한다. Docker Compose로 전체 클러스터를 로컬에서 단일 명령으로 기동할 수 있다.

---

## Technical Context

**Language/Version**: Rust 1.87 stable (주), C++ 없음 (rdkafka의 librdkafka 제외)  
**Primary Dependencies**:
- `tokio` 1.x (async runtime, 전 노드 공통)
- `tonic` + `prost` (gRPC, 전 노드 공통)
- `openraft` (Raft 합의, QN 전용)
- `opensrv-mysql` (MySQL Wire Protocol 서버, QN 전용)
- `sqlparser-rs` + 커스텀 확장 (SQL 파싱, QN 전용)
- `axum` (HTTP/WebSocket Web UI, QN 전용)
- `arrow2` (인메모리 컬럼 표현, CN 전용)
- `std::arch::x86_64` (AVX2/AVX-512 SIMD, CN 전용)
- `object_store` (S3/MinIO, SN 전용)
- `opendal` + `hdfs-native-client` (HDFS+Kerberos, SN 전용)
- `rdkafka` (Kafka 컨슈머, QN Ingestion GW)
- `tokio-uring` (Linux io_uring NVMe DIO, SN 전용)

**Storage**: 
- 인메모리: Skip-list MemTable + Arrow2 컬럼 배치
- 영구 저장: 자체 구현 LSM-Tree (컬럼별 독립 파일: `.col`, `.bloom`, `.min_max`)
- 백엔드: Native NVMe / S3(object_store) / HDFS(opendal)

**Testing**: `cargo test` (단위), Docker Compose + `cargo test --features integration` (통합)  
**Target Platform**: Linux x86_64 (프로덕션); Linux/macOS (개발 — io_uring은 Linux 전용 feature flag)  
**Project Type**: 분산 데이터베이스 시스템 (다중 바이너리 Cargo workspace)  
**Performance Goals**:
- 쓰기: Kafka 100만+ 이벤트/초 수집 (TC-PERF-002)
- 읽기: 10억 레코드 FUNNEL 쿼리 결과 반환
- 수집 레이턴시: Kafka 이벤트 → 쿼리 가능 ≤ 60초
- 장애 복구: 단일 노드 장애 후 무중단 자동 복구

**Constraints**:
- Storage-Compute 분리: CN과 SN 독립 확장 가능
- MySQL 8.0 Wire Protocol 완전 호환
- HDFS 사용 시 Kerberos 인증 필수
- LSM Merge는 파티션 경계 내에서만 발생 (inter-partition merge 금지)
- QN은 홀수 개 (최소 3개) Raft 클러스터 구성
- LSM Sort Key: 최대 4개 컬럼, 직렬화 크기 128 bytes 이하 (DDL 시 강제 검증)
- LSM Leveled Compaction: L0 overlapping 허용, L1+ non-overlapping 불변 조건
- L0 파일 수 트리거: compact=4, slowdown=8, stop=12
- Bloom Filter: xxHash3, 10 bits/key (FPR 1%), SSTable-level + Granule-level 2-tier

**Scale/Scope**: 200억+ 레코드, 최대 30개 SN, 16개 CN, 5개 QN

---

## Constitution Check

*헌법 파일(`.specify/memory/constitution.md`)이 아직 프로젝트 전용으로 작성되지 않음(템플릿 상태). 범용 소프트웨어 원칙에 따른 자체 검증을 수행한다.*

| 원칙 | 상태 | 비고 |
|---|---|---|
| **단일 책임** | ✅ | QN/CN/SN 각자 명확한 책임 분리 |
| **독립 테스트 가능** | ✅ | 각 모듈 독립 cargo test 지원 |
| **의존성 최소화** | ✅ | C++ 의존성 1개(rdkafka)로 제한 |
| **스토리지 추상화** | ✅ | `object_store` + `opendal`로 백엔드 교체 가능 |
| **관찰 가능성** | ✅ | Prometheus `/metrics`, Query Profiler 내장 |
| **고가용성** | ✅ | QN Raft, SN Tablet 복제본 3개 |

**게이트 위반 없음** — 구현 진행 가능.

---

## Project Structure

### Documentation (this feature)

```text
specs/002-wow-db-srs-v02/
├── spec.md           ✅ 요구사항 명세
├── plan.md           ✅ 이 파일 (구현 계획)
├── research.md       ✅ Phase 0 기술 스택 결정 (§13~15: LSM/Bloom/SortKey)
├── data-model.md     ✅ Phase 1 데이터 모델 (§2.4~2.8: LSM 내부 구조 상세)
├── quickstart.md     ✅ Phase 1 빠른 시작
├── contracts/
│   ├── grpc-interfaces.md  ✅ gRPC 인터페이스 계약
│   └── sql-extensions.md  ✅ SQL 확장 문법 계약
├── design/
│   └── lsm-engine.md ✅ LSM Engine 상세 설계 (Leveling, Bloom Filter, Sort Key, 물리 파일 포맷)
└── tasks.md          ✅ 구현 태스크 목록 (Phase 11: LSM Engine 강화 T107~T117)
```

### Source Code (Repository Root)

```text
wow-db/
├── Cargo.toml                  # Cargo workspace root
├── Cargo.lock
│
├── query-node/                 # Query Node 바이너리 크레이트
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs
│       ├── mysql_protocol/     # MySQL 8.0 Wire Protocol (opensrv-mysql)
│       ├── sql_parser/         # sqlparser-rs + WOW-DB 커스텀 AST 확장
│       ├── planner/
│       │   ├── logical.rs      # AST → LogicalPlan
│       │   ├── physical.rs     # LogicalPlan → PhysicalPlan (Fragment 할당)
│       │   └── cbo/            # Cost-Based Optimizer (통계 기반)
│       ├── raft/               # openraft 기반 메타데이터 클러스터
│       ├── meta/               # Cube/Tablet/SMV 메타데이터 관리
│       ├── ingestion/
│       │   ├── kafka.rs        # Routine Load (rdkafka)
│       │   └── spark.rs        # Stream Load HTTP 엔드포인트
│       ├── session_mv/         # SMV 대화형 생성 및 갱신 스케줄
│       ├── web_ui/             # axum 기반 Web SQL Client + REST API
│       ├── profiler.rs         # Query Profiler (Circular Buffer 1,000건)
│       └── monitoring.rs       # Prometheus 메트릭 노출
│
├── compute-node/               # Compute Node 바이너리 크레이트
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs
│       ├── executor/
│       │   ├── pipeline.rs     # 비동기 파이프라인 실행기 (백프레셔)
│       │   ├── simd/           # std::arch::x86_64 SIMD 오퍼레이터
│       │   │   ├── scan.rs     # AVX2 컬럼 스캔
│       │   │   ├── filter.rs   # AVX2 predicate 필터
│       │   │   ├── agg.rs      # AVX-512 집계
│       │   │   └── hash_join.rs # AVX2 Hash Build/Probe
│       │   ├── vectorized_agg.rs  # GROUP BY 벡터화 집계
│       │   └── sort.rs         # 정렬, Merge Sort, Top-K
│       ├── analytics/
│       │   ├── funnel.rs       # FUNNEL_COUNT 실행기 (비트마스크)
│       │   ├── cohort.rs       # COHORT_ANALYSIS 실행기
│       │   └── path.rs         # PATH_ANALYSIS 실행기
│       ├── shuffle.rs          # CN 간 데이터 교환 (Shuffle/Broadcast)
│       └── runtime_filter.rs  # Build Side 필터 → Probe Side 전파
│
├── storage-node/               # Storage Node (Data Node) 바이너리 크레이트
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs
│       ├── lsm/                # 자체 구현 LSM-Tree
│       │   ├── memtable.rs     # Skip-list MemTable (Arc+RwLock)
│       │   ├── wal.rs          # Write-Ahead Log (CRC32, 세그먼트 로테이션)
│       │   ├── sstable.rs      # SSTable 읽기/쓰기 (컬럼별 독립 파일)
│       │   ├── compaction.rs   # Leveled Compaction (파티션 경계 내)
│       │   └── bloom.rs        # Bloom Filter (per-SSTable, per-Granule)
│       ├── columnar/
│       │   ├── encoding.rs     # Dictionary/Delta/BitPacking/Plain 인코딩
│       │   ├── compression.rs  # LZ4/ZSTD 압축
│       │   └── flat_json.rs    # Flat JSON 자동 컬럼 추출 (Compaction 시)
│       ├── index/
│       │   ├── minmax.rs       # MINMAX 인덱스
│       │   ├── bloom_index.rs  # BLOOM_FILTER 인덱스
│       │   ├── set_index.rs    # SET 인덱스
│       │   └── ngrambf.rs      # NGRAMBF_V1 인덱스
│       ├── partition.rs        # 파티션 라우팅 및 Auto Partition
│       ├── block_cache.rs      # LRU SSTable 블록 캐시
│       ├── ttl.rs              # TTL 만료 및 Tiered Storage 이동
│       └── backend/
│           ├── native.rs       # 로컬 NVMe (tokio-uring/tokio::fs)
│           ├── s3.rs           # S3/MinIO (object_store)
│           └── hdfs.rs         # HDFS+Kerberos (opendal)
│
├── shared/                     # 공통 타입, 프로토콜, 에러
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── types.rs            # DataType, Value, SortKey, ColumnRef 등
│       ├── error.rs            # thiserror 기반 공통 에러 타입
│       ├── codec.rs            # Arrow IPC 직렬화/역직렬화
│       └── node_id.rs          # NodeId, ClusterId 타입
│
├── proto/                      # Protobuf 정의 (prost로 컴파일)
│   ├── compute.proto           # QN→CN Fragment 실행
│   ├── storage.proto           # CN→SN Tablet 읽기/쓰기
│   ├── raft.proto              # QN 메타데이터 서비스
│   ├── ingestion.proto         # Routine Load / Stream Load
│   └── health.proto            # 공통 헬스체크
│
├── integration-tests/          # Docker Compose 기반 통합 테스트
│   ├── Cargo.toml
│   └── tests/
│       ├── ddl_tests.rs        # CREATE CUBE, SMV DDL
│       ├── ingestion_tests.rs  # Kafka, Spark, INSERT 수집
│       ├── analytics_tests.rs  # FUNNEL, COHORT, PATH 쿼리
│       ├── compat_tests.rs     # MySQL 클라이언트 호환성
│       └── failover_tests.rs   # 노드 장애 복구
│
└── docker/
    ├── docker-compose.yml      # 전체 클러스터 (QN×3, CN×2, SN×3)
    ├── docker-compose.dev.yml  # 개발용 단일 노드
    ├── Dockerfile.query-node
    ├── Dockerfile.compute-node
    ├── Dockerfile.storage-node
    └── configs/
        ├── query-node.toml     # QN 설정 템플릿
        ├── compute-node.toml   # CN 설정 템플릿
        └── storage-node.toml   # SN 설정 템플릿
```

**구조 결정**: Cargo workspace + 3 바이너리 크레이트 (`query-node`, `compute-node`, `storage-node`) + 1 라이브러리 크레이트 (`shared`) + 통합 테스트 크레이트. 각 노드가 독립 Docker 이미지로 빌드됨.

---

## Complexity Tracking

| 항목 | 이유 | 단순화 대안 거부 이유 |
|---|---|---|
| LSM-Tree 자체 구현 | SRS 요구: "Rust로 자체 구현", 파티션 경계 내 Merge 커스텀 제어 필요 | `rocksdb` 크레이트: 파티션 경계 제어 불가, C++ 빌드 의존성 |
| `rdkafka` C 의존성 | `rskafka`는 exactly-once semantics 미지원 (2025 기준), 100만+/초 처리 검증 필요 | `rskafka`: 프로덕션 검증 부족 |
| `tokio-uring` (Linux 전용) | NVMe DIO 10-20% 레이턴시 개선, SRS "io_uring" 명시 요구 | `tokio::fs`: portable하지만 성능 목표 미달 가능 |
| 커스텀 SQL 파서 확장 | FUNNEL_COUNT, CREATE CUBE 등 MySQL 비표준 문법 필수 | 전용 파서: MySQL 방언 중복 유지보수 |

---

## Docker Compose 구성

```yaml
# docker/docker-compose.yml (핵심 구조)

services:
  # ── 인프라 서비스 ──────────────────────────────
  minio:
    image: minio/minio:latest
    profiles: ["full", "infra"]
    ports: ["9000:9000", "9001:9001"]
    command: server /data --console-address ":9001"
    volumes: [minio-data:/data]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]

  redpanda:
    image: redpandadata/redpanda:v24.2
    profiles: ["full", "infra"]
    ports: ["9092:9092", "9644:9644"]
    command:
      - redpanda start
      - --overprovisioned
      - --smp 1
      - --memory 512M
    healthcheck:
      test: ["CMD", "rpk", "cluster", "info"]

  # ── Storage Node ×3 ────────────────────────────
  sn-1:
    build:
      context: ..
      dockerfile: docker/Dockerfile.storage-node
    profiles: ["full", "storage", "compute"]
    ports: ["19060:9060", "18040:8040"]
    volumes: [sn-1-data:/data]
    environment:
      NODE_ID: sn-1
      LISTEN_GRPC: "0.0.0.0:9060"
      STORAGE_BACKEND: native
      DATA_DIR: /data
    healthcheck:
      test: ["CMD", "grpc_health_probe", "-addr=:9060"]
      interval: 5s
      retries: 5

  sn-2:
    # sn-1과 동일 구조, NODE_ID=sn-2, 포트 오프셋
    extends: { service: sn-1 }
    ports: ["29060:9060", "28040:8040"]
    volumes: [sn-2-data:/data]
    environment:
      NODE_ID: sn-2

  sn-3:
    extends: { service: sn-1 }
    ports: ["39060:9060", "38040:8040"]
    volumes: [sn-3-data:/data]
    environment:
      NODE_ID: sn-3

  # ── Compute Node ×2 ────────────────────────────
  cn-1:
    build:
      context: ..
      dockerfile: docker/Dockerfile.compute-node
    profiles: ["full", "compute"]
    ports: ["19040:9040"]
    depends_on:
      sn-1: { condition: service_healthy }
      sn-2: { condition: service_healthy }
      sn-3: { condition: service_healthy }
    environment:
      NODE_ID: cn-1
      STORAGE_NODES: "sn-1:9060,sn-2:9060,sn-3:9060"
    healthcheck:
      test: ["CMD", "grpc_health_probe", "-addr=:9040"]

  cn-2:
    extends: { service: cn-1 }
    ports: ["29040:9040"]
    environment:
      NODE_ID: cn-2

  # ── Query Node ×3 (Raft 클러스터) ──────────────
  qn-1:
    build:
      context: ..
      dockerfile: docker/Dockerfile.query-node
    profiles: ["full", "query"]
    ports:
      - "9030:9030"   # MySQL
      - "8080:8080"   # Web UI
      - "9010:9010"   # Raft
    depends_on:
      cn-1: { condition: service_healthy }
      cn-2: { condition: service_healthy }
    environment:
      NODE_ID: qn-1
      RAFT_PEERS: "qn-1:9010,qn-2:9010,qn-3:9010"
      COMPUTE_NODES: "cn-1:9040,cn-2:9040"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 10s
      retries: 6

  qn-2:
    extends: { service: qn-1 }
    ports: ["19030:9030", "18080:8080", "19010:9010"]
    environment:
      NODE_ID: qn-2

  qn-3:
    extends: { service: qn-1 }
    ports: ["29030:9030", "28080:8080", "29010:9010"]
    environment:
      NODE_ID: qn-3

volumes:
  minio-data:
  sn-1-data:
  sn-2-data:
  sn-3-data:

networks:
  default:
    driver: bridge
    name: wowdb-net
```

---

## Dockerfile 패턴 (공통)

```dockerfile
# docker/Dockerfile.query-node (멀티스테이지)

# Stage 1: 의존성 캐시 (cargo-chef)
FROM rust:1.87-bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: 의존성 빌드 (캐시 레이어)
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Stage 3: 소스 빌드
COPY . .
RUN cargo build --release -p query-node

# Stage 4: 최소 런타임 이미지
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y \
    ca-certificates libssl3 curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/query-node /usr/local/bin/
EXPOSE 9030 8080 9010 9011
CMD ["query-node"]
```

---

## 구현 단계 개요 (tasks.md 상세화 예정)

### Phase A: 기반 인프라 (4-6주)
- [ ] Cargo workspace 초기 구조 설정
- [ ] Proto 파일 작성 및 `build.rs` 코드 생성
- [ ] `shared` 크레이트: 공통 타입, Arrow IPC 코덱, 에러 처리
- [ ] Docker Compose 파일 작성 및 헬스체크 설정
- [ ] 기본 gRPC 서버/클라이언트 보일러플레이트 (각 노드)

### Phase B: Storage Node (6-8주)
- [ ] LSM MemTable (Skip-list) + WAL 구현
- [ ] 컬럼 파일 쓰기/읽기 (Dictionary/Delta/BitPacking 인코딩)
- [ ] LZ4/ZSTD 압축
- [ ] Level-0 Flush + Leveled Compaction (파티션 경계 내)
- [ ] Bloom Filter (SSTable/Granule)
- [ ] Data Skipping Index (MINMAX, BLOOM_FILTER, SET, NGRAMBF_V1)
- [ ] Block Cache (LRU)
- [ ] Native 백엔드 (tokio-uring / tokio::fs 조건부)
- [ ] S3 백엔드 (object_store)

### Phase C: Compute Node (5-7주)
- [ ] SIMD 오퍼레이터: 컬럼 스캔, 필터, 집계 (AVX2 + CPUID 런타임 디스패치)
- [ ] Hash Join (SIMD 해시 빌드/프로브)
- [ ] 비동기 파이프라인 실행기 (backpressure)
- [ ] Runtime Filter (Bloom/In-list/MinMax → SN 전파)
- [ ] Shuffle (CN 간 데이터 교환)
- [ ] FUNNEL_COUNT 실행기 (비트마스크 기반)
- [ ] COHORT_ANALYSIS 실행기
- [ ] PATH_ANALYSIS 실행기

### Phase D: Query Node (8-10주)
- [ ] MySQL Wire Protocol 서버 (opensrv-mysql, 포트 9030)
- [ ] SQL 파서 (sqlparser-rs + 커스텀 WOW-DB AST 노드)
- [ ] Logical Planner (AST → LogicalPlan)
- [ ] CBO (컬럼 통계, 파티션 Pruning, Join 순서 최적화)
- [ ] Physical Planner (Fragment 생성, CN 할당)
- [ ] Raft 메타데이터 클러스터 (openraft, 포트 9010)
- [ ] Cube/SMV/MV DDL 처리
- [ ] Kafka Routine Load (rdkafka)
- [ ] Spark Stream Load (HTTP, 포트 8040)
- [ ] Async INSERT Buffer
- [ ] Web UI 서버 (axum, 포트 8080)
- [ ] Query Profiler (Circular Buffer, 1,000건)
- [ ] Prometheus 메트릭 노출

### Phase E: 고급 기능 (4-6주)
- [ ] Flat JSON 자동 컬럼 추출 (Compaction 시)
- [ ] TTL + Tiered Storage (Hot→Cold 자동 이동)
- [ ] HDFS 백엔드 (opendal + Kerberos)
- [ ] Pre-aggregation MV (INSERT 시점/주기 갱신)
- [ ] Global Dictionary (저기수 문자열 클러스터 공유 사전)
- [ ] Query Result Cache (Tablet 단위 CN 메모리 캐시)
- [ ] Resource Group (사용자/역할별 CPU/메모리/동시성 제한)
- [ ] External Table (S3, HDFS, Iceberg)
- [ ] Colocate Group (로컬 Join 최적화)

### Phase F: 통합 테스트 및 성능 검증 (3-4주)
- [ ] 통합 테스트 스위트 (DDL, 수집, 분석, 호환성, 장애 복구)
- [ ] 성능 벤치마크 (SC-001: 200억 레코드, SC-005: 60초 수집 레이턴시)
- [ ] 장애 복구 시나리오 (SN 장애, QN 장애, 네트워크 파티션)
