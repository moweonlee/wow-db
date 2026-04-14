<!--
SYNC IMPACT REPORT
==================
Version change:    [template] → 1.0.0
Bump rationale:    First substantive constitution — all placeholders replaced with
                   WOW-DB-specific content. Major (1.x.x) baseline established.

Principles added (7 total):
  I.   Dual-Layout Data Model (Event Table & Behavioral Table)
  II.  Behavioral Routing — Seamless Auto-Selection
  III. LSM-Tree Write-First Storage
  IV.  SIMD-First Execution Engine
  V.   Kubernetes-Native Stateless Architecture
  VI.  MySQL Protocol Compatibility
  VII. Storage-Compute Separation

Sections added:
  - Technology Constraints
  - Quality Gates & Testing Discipline
  - Governance

Templates reviewed:
  ✅ .specify/templates/plan-template.md  — Constitution Check gate aligns
  ✅ .specify/templates/spec-template.md  — User story format compatible
  ✅ .specify/templates/tasks-template.md — Task structure compatible
  ⚠  .specify/templates/agent-file-template.md — No WOW-DB specific content; no update needed

Deferred TODOs: none
-->

# WOW-DB Constitution

## Core Principles

### I. Dual-Layout Data Model — Event Table & Behavioral Table (NON-NEGOTIABLE)

WOW-DB의 핵심 자산은 **두 개의 물리 레이아웃**이다. 모든 웹 분석 워크로드는
이 두 레이아웃의 존재를 전제로 설계되어야 한다.

- **Event Table** (`CREATE CUBE`): 이벤트 1건 = 1행. 원시 이벤트 스트림 저장소.
  시계열 집계(COUNT, GROUP BY date) 및 원시 이벤트 조회에 최적화.
- **Behavioral Table** (`CREATE SESSION MATERIALIZED VIEW`): 세션 1개 = 1행.
  Event Table로부터 파생된 행동 집약 레이아웃.
  Funnel / Cohort / Path 분석에 최적화. `session_id`, `event_sequence` 등 자동 생성 컬럼 포함.

**강제 규칙**:
- Event Table과 Behavioral Table은 항상 **쌍(pair)**으로 설계되어야 한다.
- Behavioral Table 없이 FUNNEL_COUNT / COHORT_ANALYSIS / PATH_ANALYSIS 를 구현하는 코드는
  반드시 Behavioral Guidance 경고를 생성해야 한다 (FR-NEW-001-07).
- 두 레이아웃의 동일한 쿼리에 대한 **결과 동등성(result equivalence)**은 항상 보장되어야 한다.
- DDL 문법(`CREATE SESSION MATERIALIZED VIEW`)은 변경 금지.
  개념 이름만 "Behavioral Table"로 표현한다.

### II. Behavioral Routing — Seamless Auto-Selection (NON-NEGOTIABLE)

사용자는 항상 Event Table에 쿼리를 작성한다. 엔진이 쿼리 패턴을 분석하여 최적 레이아웃을
**자동으로, 오류 없이, 투명하게** 선택해야 한다.

- Behavioral Query(FUNNEL/COHORT/PATH 함수, `session_*` 컬럼 참조) →
  Active Behavioral Table이 있으면 자동 라우팅.
- Event Query(COUNT, GROUP BY 집계) → Event Table 유지.
- Behavioral Table이 없거나 STALE → Event Table로 Fallback + Behavioral Guidance 힌트.
  **오류를 반환해서는 안 된다.**

**강제 규칙**:
- Behavioral Router는 LogicalPlan 생성 직후, CBO 이전에 실행되어야 한다.
- `FROM page_events` 를 `FROM page_events_sessions` 로 수정하는 사용자 개입은 요구하지 않는다.
- BT pair가 없는 일반 테이블(CREATE TABLE)은 Routing/Guidance 대상에서 제외한다.
- EXPLAIN MUST show `Behavioral Routing` 섹션 (라우팅 여부, 사용 BT, 예상 성능 향상).
- Behavioral Guidance 메시지는 반드시 권장 DDL 전체를 포함해야 한다.

### III. LSM-Tree Write-First Storage (NON-NEGOTIABLE)

모든 쓰기는 WAL → MemTable → Immutable MemTable → SSTable(Level 0) 경로를 따른다.
Storage Node는 이 경로 이외의 쓰기 최적화(in-place update 등)를 허용하지 않는다.

- **WAL**: 모든 쓰기는 WAL에 먼저 기록된 후 MemTable에 삽입되어야 한다. CRC32 체크섬 필수.
- **Partition-scoped Compaction**: LSM Merge는 파티션 경계 내에서만 발생한다.
  서로 다른 파티션의 SSTable을 병합하는 Compaction은 **절대 발생하지 않는다.**
- **Leveled Compaction 불변 조건**: L1+ 에서 동일 레벨 내 SSTable들의 Sort Key 범위는
  서로 겹치지 않아야 한다.
- **컬럼 지향 저장**: 각 컬럼은 파티션 단위로 독립된 파일(`.col`, `.bloom`, `.min_max`)에 저장.
  분석 쿼리는 필요한 컬럼만 읽어야 한다.
- **Bloom Filter**: SSTable별 Bloom Filter를 유지해야 하며, Compaction으로 새 SSTable이
  생성될 때 이전 Bloom Filter를 재사용하지 않고 새로 빌드한다.

**강제 규칙**:
- MemTable 임계값 초과 시 `is_full()` 반환 → 즉시 Flush 트리거 (지연 금지).
- MANIFEST 파일은 현재 활성 SSTable 전체 목록을 원자적으로 관리해야 한다.
  크래시 복구 시 MANIFEST만으로 전체 LSM 상태를 복원할 수 있어야 한다.
- Sort Key: 최대 4개 컬럼, 직렬화 크기 128 bytes 이하 (DDL 시 강제 검증).

### IV. SIMD-First Execution Engine (NON-NEGOTIABLE)

모든 열 지향 처리 핫패스는 AVX2 이상의 SIMD 명령을 사용해야 한다.
스칼라 폴백(scalar fallback)은 hot path에서 허용되지 않는다.

대상 연산:
- **컬럼 스캔/필터**: AVX2 256-bit 비트마스크 필터 (이벤트 유형 스캔 등)
- **집계 함수**: SUM, COUNT, MIN, MAX — 벡터화 루프
- **Hash Join**: SIMD 기반 해시 빌드 및 프로브 (Partitioned Hash Join)
- **Funnel Step 비트마스크**: AVX2 OR/AND 연산으로 per-user step accumulation
- **Dictionary Encoding 비교**: 정수 코드 SIMD 비교

**강제 규칙**:
- `#[cfg(target_arch = "x86_64")]` 아래 AVX2/AVX-512 구현 필수.
  `is_x86_feature_detected!("avx2")` 런타임 감지 후 분기.
- SIMD 코드는 `unsafe { std::arch::x86_64::... }` 블록 사용을 허용하는 유일한 예외다.
  모든 `unsafe` 사용에는 // SAFETY: 주석이 반드시 동반되어야 한다.
- 신규 집계 함수는 SIMD 처리 경로가 없으면 PR이 거부된다.
- 성능 벤치마크(`cargo bench`)는 SIMD 연산의 throughput(rows/sec)을 측정해야 한다.

### V. Kubernetes-Native Stateless Query Nodes (NON-NEGOTIABLE)

Query Node는 완전히 Stateless 설계여야 한다. 모든 영속 상태는 Raft KV에 저장되며,
어떤 QN Pod에 요청이 도달해도 동일한 결과를 반환해야 한다.

- **단일 시스템 이미지(Single System Image)**: 모든 QN은 Raft quorum 커밋 후 동일한
  메타데이터를 반영해야 한다. QN 간 메타데이터 뷰의 불일치는 허용하지 않는다.
- **메타데이터 캐시 Staleness 상한**: DDL 변경은 write-through 무효화.
  CBO 통계 최대 staleness 500ms (설정 가능).
- **Web Client 세션 토큰**: Raft KV(`/sessions/{token}`)에 저장. 모든 QN에서 검증 가능.
  TTL 기본 24시간.
- **헬스체크**: `/health`, `/healthz` 엔드포인트는 모든 노드에서 반드시 응답해야 한다.
- **Raft 클러스터**: QN은 홀수 개(최소 3개). 과반수 실패 시 쓰기 중단 (가용성보다 일관성 우선).

**강제 규칙**:
- QN Pod 로컬 디스크에 쿼리 상태나 메타데이터를 저장하는 코드는 금지.
  모든 영속 상태는 Raft KV 또는 Storage Node를 통해야 한다.
- K8s HPA(Horizontal Pod Autoscaler)로 QN Pod 수를 동적으로 조정할 수 있어야 한다.
  이를 방해하는 로컬 상태 의존성은 PR 거부 대상이다.
- Prometheus `/metrics` 엔드포인트는 모든 노드에서 제공해야 한다.

### VI. MySQL Protocol Compatibility (MUST)

WOW-DB는 기존 MySQL 8.0 클라이언트, JDBC 드라이버, BI 도구가 **수정 없이** 연결할 수 있어야 한다.

- **Wire Protocol**: MySQL 8.0 클라이언트 인증 및 패킷 형식 완전 호환.
  포트 9030에서 MySQL 프로토콜 수신.
- **DDL/DML 확장**: `CREATE CUBE`, `ALTER CUBE`, `CREATE SESSION MATERIALIZED VIEW` 등은
  MySQL 표준 위에 **추가(additive)**된 확장이다. 기존 MySQL DDL/DML을 제거하거나
  의미를 변경해서는 안 된다.
- **결과셋 형식**: 모든 쿼리 결과는 MySQL Result Set 형식으로 반환되어야 한다.
- **SHOW 명령**: `SHOW TABLES`, `SHOW DATABASES`, `DESCRIBE`, `EXPLAIN` 은
  MySQL 호환 형식으로 응답해야 한다.

**강제 규칙**:
- `opensrv-mysql` 크레이트를 MySQL 프로토콜 구현의 기반으로 사용한다.
  직접 바이트 레벨 프로토콜 구현은 금지 (유지보수성 위반).
- 커스텀 문법은 SQL 파서에서 명시적으로 처리하고, 파싱 실패 시 MySQL 호환 오류 코드를 반환.
- MySQL Workbench, mysql CLI, Python mysql-connector 에서 기본 쿼리가 동작하는지
  PR 전 반드시 수동 검증한다.

### VII. Storage-Compute Separation (MUST)

Storage Node(SN)와 Compute Node(CN)는 독립적으로 확장 가능해야 한다.
스토리지 백엔드는 플러그어블(pluggable)하게 설계되어야 한다.

- **Storage Backend**: Native LSM(로컬 NVMe) / S3(+ MinIO, Ceph RGW) / HDFS(Kerberos 인증 필수).
  동일한 인터페이스(`StorageBackend` trait)를 통해 추상화.
- **Co-located 모드**: CN과 SN을 동일 호스트에 배치하여 로컬 IO 성능을 최대화할 수 있어야 한다.
- **Decoupled 모드**: CN과 SN을 별도 호스트에 배치하여 CN만 독립 확장할 수 있어야 한다.
- **Tiered Storage**: 오래된 파티션을 Hot(NVMe) → Cold(S3)으로 자동 이동.

**강제 규칙**:
- CN 코드는 SN 내부 파일 포맷이나 디렉토리 구조에 직접 의존하면 안 된다.
  모든 SN 접근은 gRPC `ShardScanRequest` 프로토콜을 통해야 한다.
- HDFS 백엔드 사용 시 Kerberos 인증은 필수이며, 비인증 HDFS 접속을 허용하는 코드는 금지.
- `StorageBackend` trait 변경은 Major version bump 대상이다.

---

## Technology Constraints

| 항목 | 제약 |
|---|---|
| **주 언어** | Rust 1.87 stable. C++는 `rdkafka`의 `librdkafka` C 바인딩 한정 허용. |
| **unsafe 코드** | SIMD 연산(`std::arch`) 전용. 반드시 `// SAFETY:` 주석 동반. |
| **비동기 런타임** | `tokio` 전용. `async-std` 또는 커스텀 스레드풀 혼용 금지. |
| **직렬화** | 노드 간 통신 — Protobuf (tonic + prost). 컬럼 교환 — Apache Arrow IPC. |
| **합의 알고리즘** | openraft 기반 Raft. 자체 Raft 구현 금지. |
| **스토리지 클라이언트** | S3 — `object_store` 또는 `aws-sdk-s3`. HDFS — `hdrs` (Kerberos 지원). |
| **Kafka** | `rdkafka` (librdkafka C 바인딩). Rust 순수 구현체 허용하지 않음. |
| **테스트** | `cargo test --workspace`. 단위 테스트 + 통합 테스트(integration-tests 크레이트) 분리. |
| **컨테이너** | Docker + Docker Compose v2. 프로덕션 환경은 Kubernetes (K8s 1.28+). |
| **로깅** | `tracing` + `tracing-subscriber` + `tracing-appender`. 구조화 로그 JSON 포맷. |
| **메트릭** | Prometheus `/metrics`. `metrics` 또는 `prometheus` 크레이트. |

---

## Quality Gates & Testing Discipline

모든 PR은 다음 게이트를 통과해야 한다.

### 기능 게이트

- **Behavioral Routing 결과 동등성**: BT 사용 전후 동일 쿼리의 결과가 일치해야 한다.
  Funnel/Cohort/Path 결과 불일치는 즉각 P0 버그로 처리한다.
- **LSM 불변 조건**: L1+ non-overlapping SSTable range 불변 조건이 깨지는 코드는 머지 금지.
- **WAL 내구성**: 크래시 복구 테스트에서 커밋된 데이터 손실이 0건이어야 한다.
- **MySQL 호환성**: `mysql CLI`, Python `mysql-connector` 에서 기본 쿼리 동작 확인.

### 성능 게이트

- **SIMD 회귀 방지**: `cargo bench` 실행 결과, SIMD 집계 연산 throughput이 기준치 대비
  10% 이상 저하되면 PR 거부.
- **Behavioral Routing 오버헤드**: Behavioral Router 자체 실행 시간이 전체 쿼리 계획 시간의
  5% 이하여야 한다.
- **100k 행 Funnel 응답시간**: `analytics_large_dataset_tests.rs` 기준 5,000ms 이내.

### 코드 품질 게이트

- `cargo clippy --workspace -- -D warnings`: 경고 0건.
- `cargo fmt --check`: 포맷 위반 0건.
- 새 unsafe 블록: `// SAFETY:` 주석 없으면 PR 거부.
- 새 gRPC 서비스: `.proto` 파일과 구현이 동시에 머지되어야 한다.

---

## Governance

이 Constitution은 WOW-DB 프로젝트의 **최상위 설계 원칙 문서**이다.
모든 구현 결정, PR 리뷰, 아키텍처 변경은 이 Constitution과 일치해야 한다.

### 우선순위 규칙

충돌 발생 시 다음 순서로 우선한다:
1. 이 Constitution의 NON-NEGOTIABLE 원칙
2. 이 Constitution의 MUST 원칙
3. `specs/002-wow-db-srs-v02/spec.md` 의 FR 요구사항
4. 개별 feature plan.md 의 기술적 결정

### 개정 절차

- **MAJOR(x.0.0)**: 핵심 원칙 제거 또는 재정의 (예: Behavioral Routing 원칙 제거).
  팀 전체 합의 + migration plan 필수.
- **MINOR(0.x.0)**: 원칙 추가, 섹션 신설, 제약 확대.
  기술 리드 승인 + spec 문서 동기화 필수.
- **PATCH(0.0.x)**: 표현 개선, 오타 수정, 비의미적 변경.
  단독 커밋 허용.

### 준수 검토

- 모든 PR의 description에 "Constitution Check" 섹션을 포함해야 한다.
  위반되는 원칙이 없으면 "All principles satisfied" 로 명시.
- 원칙 위반이 불가피한 경우, PR에 위반 근거와 임시 예외(RFC) 를 명시하고
  다음 MINOR 개정 시 Constitution에 반영한다.
- 분기별 Constitution 준수 리뷰를 수행한다.

### 런타임 개발 가이드

- `.specify/memory/constitution.md` — 이 파일 (최상위 원칙)
- `CLAUDE.md` — AI 에이전트용 구현 가이드라인
- `specs/002-wow-db-srs-v02/` — 기능 명세 및 설계 문서
- `specs/002-wow-db-srs-v02/design/query-routing-smv.md` — Behavioral Routing 상세 설계

**Version**: 1.0.0 | **Ratified**: 2026-04-14 | **Last Amended**: 2026-04-14
