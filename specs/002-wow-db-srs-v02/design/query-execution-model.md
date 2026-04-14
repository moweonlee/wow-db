# Query Execution Model 상세 설계

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-14  
**모듈**: `query-node/src/planner/`, `compute-node/src/executor/`  
**요구사항**: FR-037, FR-041  
**참조**: `design/logical-to-physical-mapping.md` (논리-물리 매핑 상세)

---

## 1. 개요

WOW-DB의 쿼리 실행은 **3계층 파이프라인**으로 이루어진다.

```
┌─────────────────────────────────────────────────────────────┐
│  Query Node (QN)                                            │
│  SQL → AST → LogicalPlan → PhysicalPlan → Fragment 배포     │
└─────────────────────────┬───────────────────────────────────┘
                          │ Fragment Plan (gRPC, 포트 9040)
             ┌────────────┼────────────┐
             ▼            ▼            ▼
┌────────────────┐ ┌──────────────┐ ┌──────────────┐
│ Compute Node 1 │ │ CN 2         │ │ CN N         │
│ (Shard A, B)   │ │ (Shard C, D) │ │ (Shard ...)  │
└───────┬────────┘ └──────┬───────┘ └──────┬───────┘
        │                  │                │
   Shard Scan (gRPC)   Shard Scan       Shard Scan
        │                  │                │
        ▼                  ▼                ▼
┌──────────────┐   ┌──────────────┐  ┌──────────────┐
│ Storage Node │   │ Storage Node │  │ Storage Node │
│ (Part 스캔)  │   │ (Part 스캔)  │  │ (Part 스캔)  │
└──────────────┘   └──────────────┘  └──────────────┘
        │                  │                │
        └──────────────────┴────────────────┘
                           │
              Partial Result → QN 최종 병합
```

**핵심 설계 철학**:
- QN은 **무엇을** 실행할지 결정 (논리 계획 + 통계 기반 최적화)
- CN은 **어떻게** 실행할지 결정 (물리 파이프라인 + SIMD 실행)
- SN은 **어디서** 읽을지 결정 (MANIFEST + Bloom/MINMAX 기반 파일 스킵)

---

## 2. Query Node의 계획 수립

### 2.1 LogicalPlan 생성

```
SQL 입력
  ↓
SQL Parser (sqlparser-rs + 커스텀 확장)
  → AST 생성 (WOW-DB 확장 문법 포함: FUNNEL, COHORT, PATH)
  ↓
Logical Planner
  → AST → LogicalPlan 변환
  → Table 이름 → TableSchema 해석
  → 서브쿼리 인라이닝, 중복 제거
  ↓
CBO Optimizer
  → 통계 기반 논리 최적화:
    ① Partition Pruning       (Partition 수준 통계)
    ② Predicate Pushdown      (SN까지 필터 내려보냄)
    ③ Column Projection       (필요한 컬럼만)
    ④ Join 순서 최적화        (Shard 수준 NDV/row_count)
    ⑤ Aggregation 전략 선택  (Hash Agg vs Sort Agg)
    ⑥ Runtime Filter 결정    (Hash Join Build Side 크기 기반)
```

### 2.2 Partition Pruning 상세

CBO는 WHERE 절의 predicate와 Partition 수준 통계를 비교하여 스캔 대상 Partition을 사전 제거한다.

```
예: WHERE event_time >= '2024-01-01' AND event_time < '2024-04-01'

Partition 목록:
  p_2024_q1: min=2024-01-01, max=2024-03-31  → 포함 ✓
  p_2024_q2: min=2024-04-01, max=2024-06-30  → 제외 ✗ (완전 범위 밖)
  p_2023_q4: min=2023-10-01, max=2023-12-31  → 제외 ✗

Pruning 결과: p_2024_q1만 스캔
```

Partition Pruning이 완료된 후에야 해당 Partition 내의 Shard 목록을 Raft 메타데이터에서 조회한다.

### 2.3 PhysicalPlan (Fragment) 생성

CBO 최적화 완료 후 Physical Planner는 실행 Fragment를 생성한다.

```rust
// query-node/src/planner/physical.rs

pub struct PhysicalPlan {
    pub query_id:    Uuid,
    pub fragments:   Vec<Fragment>,
    pub exchanges:   Vec<Exchange>,    // CN 간 데이터 교환 명세
    pub final_merge: MergeOp,          // QN에서의 최종 병합 방식
}

pub struct Fragment {
    pub fragment_id:    Uuid,
    pub assigned_cn:    NodeId,        // 이 Fragment를 실행할 CN
    pub operators:      Vec<PhysOp>,   // 실행 오퍼레이터 순서
    pub shard_scans:    Vec<ShardScan>, // 이 Fragment가 읽을 Shard 목록
    pub runtime_filter: Option<RuntimeFilter>,
}

pub struct ShardScan {
    pub shard_id:    Uuid,
    pub sn_endpoint: SocketAddr,       // 데이터를 보유한 SN
    pub columns:     Vec<String>,      // Column Projection
    pub predicates:  Vec<Predicate>,   // Pushdown 필터
    pub scan_range:  LsmScanRange,     // 스캔할 LSM 레벨 범위
}

pub struct LsmScanRange {
    pub min_level: u32,   // 기본 0 (L0부터)
    pub max_level: u32,   // 기본 6 (L6까지)
}
```

### 2.4 CN 할당 전략

Physical Planner는 다음 기준으로 Shard를 CN에 할당한다:

| 우선순위 | 전략 | 적용 조건 |
|---|---|---|
| 1 | **Data Locality** | CN과 SN이 Co-located 모드일 때, 로컬 Shard 우선 할당 |
| 2 | **Load Balancing** | 각 CN의 현재 활성 Fragment 수 기반 균등 분배 |
| 3 | **Colocate Group** | 동일 Colocate Group의 Shard는 같은 CN에 할당 (네트워크 Shuffle 제거) |
| 4 | **DOP 제한** | CN당 동시 처리 Fragment 수는 CPU 코어 수 × DOP 계수 이하 |

---

## 3. Compute Node의 Fragment 실행

### 3.1 Fragment 수신 및 파이프라인 구성

CN은 QN으로부터 gRPC로 Fragment Plan을 수신하고 비동기 파이프라인을 구성한다.

```rust
// compute-node/src/executor/pipeline.rs

pub struct FragmentExecutor {
    fragment:    Fragment,
    pipeline:    Vec<Box<dyn PipelineStage>>,
    backpressure: BackpressureController,
}

// 파이프라인 스테이지 순서 (예: Shard Scan → Filter → Agg)
//
//  ┌─────────────────────────────────────────────────────────┐
//  │ Stage 1: ShardScanStage                                 │
//  │  - SN에 ShardScanRequest gRPC 발송                      │
//  │  - Arrow2 RecordBatch 스트림 수신                        │
//  │  - 복수 Shard를 async 병렬 스캔 (tokio::join!)           │
//  └───────────────────────────┬─────────────────────────────┘
//                              ▼
//  ┌─────────────────────────────────────────────────────────┐
//  │ Stage 2: SIMDFilterStage                                │
//  │  - AVX2 predicate 벡터 평가                             │
//  │  - 필터 통과 행만 다음 스테이지로 전달                    │
//  └───────────────────────────┬─────────────────────────────┘
//                              ▼
//  ┌─────────────────────────────────────────────────────────┐
//  │ Stage 3: VectorizedAggStage                             │
//  │  - AVX-512 GROUP BY 집계                                │
//  │  - Hash Agg: 해시 테이블 빌드                            │
//  └───────────────────────────┬─────────────────────────────┘
//                              ▼
//  ┌─────────────────────────────────────────────────────────┐
//  │ Stage 4: ExchangeStage (필요 시)                         │
//  │  - Shuffle: 집계 키 해시 기반으로 다른 CN에 분배          │
//  │  - Broadcast: 작은 쪽 테이블을 모든 CN에 복사            │
//  │  - Gather: QN으로 부분 결과 반환                         │
//  └─────────────────────────────────────────────────────────┘
```

### 3.2 ShardScan → SN 요청 변환

CN의 ShardScanStage는 Fragment에 포함된 ShardScan을 SN의 gRPC 요청으로 변환한다.

```rust
// compute-node/src/executor/shard_scan.rs

async fn execute_shard_scan(scan: &ShardScan) -> impl Stream<Item = RecordBatch> {
    let client = StorageNodeClient::connect(scan.sn_endpoint).await?;

    let request = ShardScanRequest {
        shard_id:       scan.shard_id,
        columns:        scan.columns.clone(),
        predicates:     scan.predicates.clone(),    // Pushdown 필터
        runtime_filter: scan.runtime_filter.clone(), // Runtime Bloom Filter
        scan_range:     scan.scan_range,
    };

    // SN으로부터 Arrow2 RecordBatch 스트림 수신
    client.scan_shard(request).await
}
```

### 3.3 다중 Shard 병렬 스캔

하나의 Fragment가 여러 Shard를 담당하는 경우, CN은 이를 병렬로 스캔한다.

```rust
// compute-node/src/executor/shard_scan.rs

pub async fn scan_all_shards(
    scans: Vec<ShardScan>,
    output: Sender<RecordBatch>,
) {
    let tasks: Vec<_> = scans.into_iter()
        .map(|scan| tokio::spawn(async move {
            execute_shard_scan(&scan).await
        }))
        .collect();

    // 모든 Shard 스캔 결과를 합류 (순서 무관)
    futures::future::join_all(tasks).await
        .into_iter()
        .flatten()
        .for_each(|batch| output.send(batch).unwrap());
}
```

### 3.4 Backpressure (배압) 관리

CN과 SN 사이, CN과 QN 사이의 데이터 흐름 속도를 조절한다.

```
SN 스캔 속도 > CN 처리 속도 → SN에 gRPC flow control 적용
CN 처리 속도 > QN 병합 속도 → CN 출력 채널 버퍼 꽉 참 → CN 처리 일시 중단
```

- gRPC 스트리밍의 기본 flow control 메커니즘을 활용
- CN 내부 파이프라인 스테이지 간 tokio::mpsc 채널 크기로 배압 제어

---

## 4. Exchange 오퍼레이터 (CN 간 데이터 교환)

### 4.1 Exchange 종류

| Exchange 타입 | 설명 | 사용 예 |
|---|---|---|
| **Shuffle** | 특정 키의 해시값으로 대상 CN 결정. 동일 키의 행은 항상 같은 CN으로 | 분산 GROUP BY, Hash Join |
| **Broadcast** | 동일 데이터를 모든 CN에 복사 | 작은 테이블(Dimension)의 Join |
| **Gather** | 모든 CN의 부분 결과를 QN 또는 지정 CN으로 수집 | 최종 집계, ORDER BY + LIMIT |
| **Colocate** | Colocate Group 내 Shard 배치가 동일 → Shuffle 불필요 | Colocate Join |

### 4.2 두 단계 집계 (2-Stage Aggregation)

분산 GROUP BY의 경우 Shuffle 전후에 두 번의 집계가 발생한다:

```
Stage 1 (각 CN — Shard 로컬):
  Shard 스캔 → 로컬 Partial Aggregation
  → Partial 결과를 Shuffle (집계 키 해시 기반)

Stage 2 (Shuffle 수신 CN):
  Partial 결과 수신 → Final Aggregation
  → Gather → QN 최종 병합
```

---

## 5. 분석 쿼리 전용 실행기 (FUNNEL / COHORT / PATH)

### 5.1 FUNNEL 실행 모델

```sql
-- 예시 쿼리
SELECT FUNNEL_COUNT(
  user_id,
  STEP_1 = (event_name = 'page_view'),
  STEP_2 = (event_name = 'add_to_cart'),
  STEP_3 = (event_name = 'purchase')
  WITHIN INTERVAL '7 DAY'
)
FROM page_events
WHERE event_time BETWEEN '2024-01-01' AND '2024-03-31'
```

**실행 계획**:
```
1. QN: user_id 기반 Shard 분배 (동일 user_id는 동일 CN)
   → user_id Hash → Shuffle Fragment 할당

2. CN: 각 user_id의 이벤트 시퀀스를 시간순 정렬
   → SIMD 이벤트 타입 스캔 (AVX2)
   → 비트마스크 방식 Step 달성 추적:
     user_X: [STEP_1=1, STEP_2=1, STEP_3=0] → Step 2까지 도달
   → 시간 윈도우(7일) 필터 적용

3. CN → QN: 사용자별 달성 Step 집계 (step_counts: [1000, 850, 600])

4. QN: 모든 CN 부분 결과 합산 → 최종 Funnel 카운트
```

### 5.2 COHORT 실행 모델

```
1. QN: 코호트 진입 이벤트 기준으로 Partition Pruning
2. CN Stage-1: user_id 기반 Shuffle
3. CN Stage-2: 각 user_id의 최초 진입 이벤트 날짜 식별 → 코호트 그룹 결정
4. CN Stage-2: 코호트 그룹별 × 이후 기간별 재방문/전환 집계
5. QN: 코호트 행렬(cohort_date × period) 최종 병합
```

### 5.3 PATH 실행 모델

```
1. QN: session_id 기반 Shard 분배 (세션 이벤트 co-located)
2. CN: 각 세션 내 이벤트 시퀀스 → 정규화된 Path 문자열 생성
   예: "page_view → search → product_view → add_to_cart"
3. CN: Path별 빈도 카운트
4. CN → QN (Shuffle by path_hash → Sort → Top-N)
5. QN: 전체 Path Top-N 최종 정렬
```

---

## 6. 실행 계획 캐시 및 재사용

### 6.1 Query Result Cache

동일한 Logical Plan + 동일한 Partition 버전이면 이전 CN 실행 결과를 재사용한다.

```
캐시 키 = Hash(
  canonical_sql,              // 논리적으로 동등한 SQL의 정규화 표현
  partition_version_map,      // {partition_id: raft_index} 스냅샷
)

캐시 저장소: CN 로컬 메모리 (LRU, 기본 10% of heap)
캐시 무효화: 대상 Partition에 새 Part가 추가(flush/compaction) 시 해당 Partition 버전 증가
```

### 6.2 Prepared Statement 캐시

QN은 최근 100개의 파싱된 AST와 최적화된 LogicalPlan을 LRU 캐시에 보존한다.  
동일 SQL이 재실행되면 Parse → CBO 과정을 건너뛰고 PhysicalPlan 생성부터 시작한다.

---

## 7. Query Profiler 연동

쿼리 실행의 각 단계에서 Profiler에 메트릭을 기록한다.

```rust
// query-node/src/profiler.rs

pub struct QueryProfile {
    pub query_id:      Uuid,
    pub sql_text:      String,
    pub started_at:    Instant,
    pub stages:        Vec<StageMetric>,
    pub fragments:     Vec<FragmentMetric>,  // CN별 실행 메트릭
}

pub struct FragmentMetric {
    pub fragment_id:    Uuid,
    pub cn_node_id:     NodeId,
    pub shards_scanned: u32,
    pub parts_opened:   u32,          // 실제 열린 Part 수
    pub parts_skipped:  u32,          // Bloom/MINMAX로 스킵된 Part 수
    pub granules_read:  u64,
    pub granules_skipped: u64,
    pub rows_scanned:   u64,
    pub rows_returned:  u64,
    pub bytes_read:     u64,
    pub duration_ms:    u64,
}
```

---

## 8. 관련 문서

- **논리-물리 매핑**: `specs/002-wow-db-srs-v02/design/logical-to-physical-mapping.md`
- **LSM Engine 설계**: `specs/002-wow-db-srs-v02/design/lsm-engine.md`
- **요구사항**: `specs/002-wow-db-srs-v02/spec.md` FR-037, FR-041
- **구현 태스크**: `specs/002-wow-db-srs-v02/tasks.md` Phase 13
