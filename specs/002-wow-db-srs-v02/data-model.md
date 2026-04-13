# 데이터 모델: WOW-DB

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-12  
**Phase**: 1 — 설계 및 계약

---

## 1. 핵심 엔티티

### 1.1 CubeSchema (Cube 메타데이터)

```
CubeSchema {
    cube_id:         Uuid,            // 전역 고유 식별자
    name:            String,          // 사용자 정의 이름 (예: "page_events")
    database:        String,          // 데이터베이스 이름
    columns:         Vec<ColumnDef>,  // 컬럼 정의 목록
    partition_key:   PartitionKey,    // 파티션 전략
    sort_key:        Vec<ColumnRef>,  // ORDER BY (LSM Sort Key)
    distribution:    Distribution,    // 버킷 분산 전략
    colocate_group:  Option<String>,  // Colocate Group 이름
    storage_backend: StorageBackend,  // Native/S3/HDFS
    ttl_policy:      Option<TtlPolicy>,
    tiering_policy:  Option<TieringPolicy>,
    created_at:      Timestamp,
    version:         u64,             // Raft 메타데이터 버전
}
```

### 1.2 ColumnDef (컬럼 정의)

```
ColumnDef {
    name:           String,
    data_type:      DataType,      // INT64, FLOAT64, STRING, DATETIME, JSON, BOOLEAN
    nullable:       bool,
    encoding:       Encoding,      // PLAIN, DICTIONARY, DELTA, BITPACKING
    compression:    Compression,   // LZ4, ZSTD, NONE
    skipping_index: Option<SkippingIndex>,  // MINMAX / BLOOM_FILTER / SET / NGRAMBF_V1
    flat_json:      Option<FlatJsonConfig>, // JSON 컬럼 자동 컬럼화 설정
}
```

### 1.3 PartitionKey (파티션 전략)

```
PartitionKey {
    variant: PartitionVariant,
    // PartitionVariant::Range { column, granularity: Day|Month|Year }
    // PartitionVariant::List  { column, values: Vec<Value> }
    auto_partition: bool,   // INSERT 시 자동 파티션 생성 여부
}
```

### 1.4 Partition (파티션 인스턴스)

```
Partition {
    partition_id:  Uuid,
    cube_id:       Uuid,
    range_start:   Value,
    range_end:     Value,
    tablets:       Vec<TabletRef>,
    row_count:     u64,
    size_bytes:    u64,
    created_at:    Timestamp,
    ttl_expires:   Option<Timestamp>,
    tier:          StorageTier,   // Hot | Cold
}
```

### 1.5 Tablet (분산 스토리지 단위)

```
Tablet {
    tablet_id:    Uuid,
    partition_id: Uuid,
    bucket_id:    u32,           // 분산 키 해시 버킷
    replicas:     Vec<TabletReplica>,
}

TabletReplica {
    node_id:    NodeId,
    role:       ReplicaRole,   // Leader | Follower
    state:      ReplicaState,  // Normal | Catching Up | Offline
    lsn:        u64,           // 최종 적용 LSN
}
```

### 1.6 SessionMaterializedView (세션 뷰 메타데이터)

```
SessionMaterializedView {
    smv_id:           Uuid,
    name:             String,
    source_cube_id:   Uuid,
    user_key_column:  ColumnRef,
    session_timeout:  Duration,      // 비활성 세션 타임아웃
    refresh_schedule: RefreshSchedule,
    state:            SmvState,      // Building | Ready | Refreshing | Error
    last_refreshed:   Option<Timestamp>,
    version:          u64,
}
```

### 1.7 Event (런타임 이벤트 행)

```
// 논리 표현 — 실제 저장은 컬럼 파일로 분리됨
Event {
    // 필수 예약 컬럼
    __event_time:  Timestamp,   // 이벤트 발생 시각 (파티션 키 기준)
    __insert_time: Timestamp,   // WOW-DB 수집 시각
    __row_id:      u64,         // Sort Key 내 단조 증가 행 ID

    // 사용자 정의 컬럼 (CubeSchema.columns 기반)
    // 예: user_id, event_name, properties (JSON)
}
```

### 1.8 Session (세션 집계 행 — SMV 저장)

```
Session {
    session_id:    Uuid,       // 생성된 세션 고유 ID
    user_key:      Value,      // 사용자 식별자 (CubeSchema.user_key_column)
    session_start: Timestamp,
    session_end:   Timestamp,
    event_count:   u32,
    events:        Vec<EventRef>,   // 정렬된 이벤트 시퀀스 (압축 저장)
    // 소스 Cube의 모든 컬럼도 세션별 집계/첫번째 값으로 포함 가능
}
```

### 1.9 PreAggMV (사전 집계 Materialized View)

```
PreAggMV {
    mv_id:          Uuid,
    name:           String,
    source_cube_id: Uuid,
    group_by:       Vec<ColumnRef>,
    aggregates:     Vec<AggExpr>,   // SUM, COUNT, MIN, MAX, HLL 등
    refresh_mode:   RefreshMode,    // OnInsert | Scheduled(cron)
    state:          MvState,
    version:        u64,
}
```

### 1.10 ResourceGroup (리소스 정책)

```
ResourceGroup {
    group_id:         Uuid,
    name:             String,
    max_cpu_cores:    Option<f64>,
    max_memory_mb:    Option<u64>,
    max_concurrency:  Option<u32>,
    query_timeout_ms: Option<u64>,
    assigned_to:      Vec<UserOrRole>,
}
```

### 1.11 QueryProfile (쿼리 실행 이력)

```
QueryProfile {
    query_id:       Uuid,
    sql_text:       String,
    submitted_at:   Timestamp,
    finished_at:    Timestamp,
    total_ms:       u64,
    rows_scanned:   u64,
    rows_returned:  u64,
    status:         QueryStatus,   // Success | Error | Cancelled
    error_msg:      Option<String>,
    stages:         Vec<StageMetric>,
    node_metrics:   Vec<NodeMetric>,
}

StageMetric {
    stage_name: String,   // Parse | Plan | Scan | Agg | Join | Network | Merge
    duration_ms: u64,
    rows_in:    u64,
    rows_out:   u64,
}
```

---

## 2. LSM-Tree 내부 구조 (Storage Node)

### 2.1 MemTable

```
MemTable {
    skip_list:    SkipList<SortKey, ColumnarRow>,
    size_bytes:   AtomicUsize,
    flush_threshold: usize,    // 기본 64MB
}
```

### 2.2 SSTable (디스크 파일 구조)

```
SSTable {
    metadata:  SstMeta {
        sst_id:         Uuid,
        level:          u8,         // 0 = L0 (flush), 1+ = Compaction 결과
        sequence_num:   u64,        // 파티션 내 단조 증가 시퀀스 번호
                                    // flush 또는 compaction 완료 시 할당
                                    // 읽기 시 동일 key에 대해 최신 seq 우선
        generation:     u64,        // Compaction 세대 번호
                                    // 0 = MemTable flush 결과 (원본)
                                    // N = N번째 Compaction 거친 결과
        partition_id:   Uuid,
        sort_key_min:   SortKey,
        sort_key_max:   SortKey,
        row_count:      u64,
        size_bytes:     u64,
        created_at:     Timestamp,
        compacted_from: Vec<Uuid>,  // 이 SSTable 생성 시 병합된 입력 SSTable ID들
                                    // flush의 경우 빈 배열
    },
    columns:   HashMap<ColumnName, ColumnFile>,
    bloom:     BloomFilter,      // per-SSTable Bloom (Sort Key 기준)
    min_max:   MinMaxIndex,      // CBO용 컬럼별 min/max
}

// 디스크 레이아웃 (파티션 기준):
// partition=p_YYYY_MM/
// └── lsm/
//     ├── L0/
//     │   └── <sst_id>/          ← L0: 파일들 간 key range 겹침 허용
//     │       ├── _meta.json     ← SstMeta 직렬화 (JSON)
//     │       ├── <col>.col      ← 압축된 컬럼 데이터 (LZ4/ZSTD)
//     │       ├── <col>.bloom    ← Bloom Filter (xxHash3, 10 bits/key)
//     │       └── <col>.min_max  ← Granule별 MINMAX 인덱스
//     ├── L1/
//     │   └── <sst_id>/          ← L1+: 파티션 내 key range 비중첩 불변 조건
//     ├── L2/ ... L6/
//     └── MANIFEST               ← 현재 레벨별 활성 SSTable 목록 (원자적 갱신)
```

### 2.3 Granule (Data Skipping 단위)

```
Granule {
    granule_id:   u32,          // SSTable 내 순서 (0-based)
    row_offset:   u64,          // 파일 내 바이트 오프셋
    row_count:    u16,          // 기본 8,192행
    // 각 컬럼의 per-Granule 인덱스:
    minmax:  Option<(Value, Value)>,
    bloom:   Option<BloomFilter>,      // BLOOM_FILTER 인덱스
    set:     Option<HashSet<Value>>,   // SET 인덱스
    ngrambf: Option<NGramBloomFilter>, // NGRAMBF_V1
}
```

### 2.4 Compaction 레벨 구조

```
CompactionConfig {
    // L0 파일 수 기반 트리거
    level0_file_compaction_trigger: usize,  // 기본 4  (L0→L1 compact 시작)
    level0_slowdown_write_trigger:  usize,  // 기본 8  (쓰기 속도 제한)
    level0_stop_write_trigger:      usize,  // 기본 12 (쓰기 중단, 배압)

    // L1+ 크기 기반 트리거
    level1_max_bytes:    u64,   // 기본 256MB
    level_multiplier:    u32,   // 기본 10 (Ln max = L1 × 10^(n-1))
    max_levels:          u8,    // 기본 7 (L0~L6)
    target_file_size_mb: u64,   // 기본 64MB (Compaction 출력 SSTable 목표 크기)
}

LevelState {
    level:        u8,           // 0~6
    max_bytes:    u64,          // CompactionConfig 수식으로 계산
    // L0: 파일 수 제한 기반 / L1+: 크기 기반
    sstables:     Vec<SstMeta>, // L0: 겹침 허용, L1+: 비중첩 불변 조건
    total_bytes:  u64,
    // 다음 compact 대상 (least recently compacted):
    compaction_score: f64,      // total_bytes / max_bytes (>1.0 이면 compact 필요)
}

// 레벨별 최대 크기 및 예상 SSTable 수 (target_file_size=64MB 기준):
// L0: 무제한 (파일 수로 제어)
// L1: 256 MB  → SSTable ~4개
// L2: 2.5 GB  → SSTable ~40개
// L3: 25 GB   → SSTable ~390개
// L4: 250 GB  → SSTable ~3,900개
// L5: 2.5 TB  → SSTable ~39,000개
// L6: 무제한  → 최종 수렴 레벨 (Full Compaction 목적지)
```

### 2.5 Bloom Filter 설정

```
BloomFilterConfig {
    hash_fn:             HashFn,   // xxHash3 (128-bit, SIMD-friendly, 기본)
    bits_per_key:        u8,       // 10 = FPR 1% (기본), 14 = FPR 0.1%
    false_positive_rate: f64,      // 0.01 (1%)
    // hash 함수 수 k = bits_per_key × ln(2) ≈ 7 (bits=10 기준)
    // xxHash3 128-bit 출력을 k개 구간으로 분할하여 k개 해시 함수 시뮬레이션
}
```

### 2.6 Sort Key 제약 조건

```
// CubeSchema.sort_key 에 적용되는 검증 규칙 (DDL 파싱 시 강제):
SortKeyConstraints {
    max_columns:     usize,  // 4 (초과 시 DDL 에러)
    max_bytes:       usize,  // 128 bytes (직렬화 크기 초과 시 DDL 에러)
    string_max_len:  usize,  // 64 bytes (STRING 컬럼 Sort Key 시 적용)
    allowed_types:   &[DataType],  // INT64, FLOAT64, DATETIME, STRING, BOOLEAN
    // JSON 컬럼은 Sort Key 불허
    null_ordering:   NullOrdering, // NullsFirst (NULL을 최솟값으로 처리)
}

// 직렬화 크기 계산:
// INT64    → 8 bytes (빅엔디언, 부호 비트 flip)
// FLOAT64  → 8 bytes (IEEE 754 빅엔디언 + 부호 처리)
// DATETIME → 8 bytes (Unix timestamp nanoseconds)
// BOOLEAN  → 1 byte
// STRING   → min(actual_len, string_max_len) bytes + 1 NULL 종료자
// NULLABLE → +1 byte prefix (0x00=NULL, 0x01=NonNull)
```

### 2.7 물리 컬럼 파일 포맷 (`.col` 바이너리 레이아웃)

```
// .col 파일 바이너리 구조 (총 파일)
┌──────────────────────────────────────────────┐
│ MAGIC [8 bytes]  "WOWDBCOL"                  │
│ VERSION [4 bytes] 파일 포맷 버전 (현재 1)    │
│ FLAGS [4 bytes]  압축 방식, 인코딩 플래그     │
│ COLUMN_ID [64 bytes] 컬럼 이름 (UTF-8 패딩)  │
│ SST_SEQUENCE [8 bytes] SSTable sequence_num  │
│ ROW_COUNT [8 bytes]                          │
│ GRANULE_COUNT [4 bytes]                      │
│ GRANULE_SIZE [4 bytes] 기본 8192             │
│ RESERVED [16 bytes]                          │
├──────────────────────────────────────────────┤ ← 헤더 총 120 bytes
│ GRANULE_OFFSET_TABLE                         │
│  - GRANULE_COUNT × 16 bytes                  │
│  - 각 항목: [8B offset | 8B compressed_len]  │
├──────────────────────────────────────────────┤
│ DATA BLOCKS (Granule별 압축 블록)            │
│  Granule_0:                                  │
│    [4B block_len][N B 압축 데이터]           │
│  Granule_1: ...                              │
│  ...                                         │
├──────────────────────────────────────────────┤
│ FOOTER                                       │
│  [8B offset_table_offset]                    │
│  [4B CRC32 of header+offset_table]           │
│  [8 bytes] "WOWDBEND"                        │
└──────────────────────────────────────────────┘ ← 푸터 총 20 bytes

// .bloom 파일 바이너리 구조
┌──────────────────────────────────────────────┐
│ MAGIC [8 bytes]  "WOWBLOOM"                  │
│ VERSION [4 bytes]                            │
│ SST_SEQUENCE [8 bytes]                       │
│ BITS_PER_KEY [1 byte]                        │
│ HASH_FN [1 byte]  0=xxHash3, 1=MurmurHash3  │
│ NUM_HASH_FUNCS [1 byte]  k 값               │
│ RESERVED [5 bytes]                           │
│ BIT_ARRAY_LEN [8 bytes]  비트 배열 크기     │
├──────────────────────────────────────────────┤ ← 헤더 총 36 bytes
│ BIT_ARRAY [BIT_ARRAY_LEN / 8 bytes]          │
├──────────────────────────────────────────────┤
│ CRC32 [4 bytes]  비트 배열 체크섬            │
└──────────────────────────────────────────────┘

// .min_max 파일 바이너리 구조 (Granule별 MINMAX 인덱스)
┌──────────────────────────────────────────────┐
│ MAGIC [8 bytes]  "WOWMINMX"                  │
│ VERSION [4 bytes]                            │
│ SST_SEQUENCE [8 bytes]                       │
│ GRANULE_COUNT [4 bytes]                      │
│ VALUE_TYPE [1 byte]  DataType 코드           │
│ RESERVED [7 bytes]                           │
├──────────────────────────────────────────────┤ ← 헤더 총 32 bytes
│ GRANULE_ENTRIES                              │
│  각 항목 (고정 크기 또는 가변):              │
│  [8B min_val | 8B max_val | 1B null_flag]    │
│  STRING 타입: [2B len | N B data] × 2        │
└──────────────────────────────────────────────┘
```

### 2.8 SSTable 버전 진행 및 Compaction 수렴

```
// SSTable sequence_num 할당 규칙:
// - 파티션 내 전역 AtomicU64 카운터에서 단조 증가로 할당
// - MemTable flush 시: 1개의 새 sequence_num 할당 → L0 SSTable 생성
// - Compaction 완료 시: 출력 SSTable 수만큼 새 sequence_num 할당
//   → 입력 SSTable(들)은 MANIFEST에서 제거 후 파일 삭제

// generation 번호 예시:
// gen=0: MemTable flush → L0 SSTable (원본 데이터)
// gen=1: L0→L1 compact → L1 SSTable (1회 merge됨)
// gen=2: L1→L2 compact → L2 SSTable (2회 merge됨)
// ...
// gen=N: L(N-1)→LN compact → 최종 수렴 SSTable

// MANIFEST 파일 (원자적 갱신):
// - 현재 활성 SSTable 전체 목록 (레벨, ID, key range, sequence_num)
// - Compaction 완료 시 단일 파일 교체 (rename 원자적 조작)
// - 크래시 복구 시 MANIFEST만 읽으면 전체 상태 복원 가능

// Full Compaction 수렴:
// 새 쓰기가 없는 상태에서 충분한 시간이 지나면:
//
//   L0: 0개 파일 (모두 L1으로 merge)
//   L1: 4개 파일 (256MB / 64MB)
//   L2: 40개 파일
//   ...
//   L6: 모든 데이터 → 단일 정렬된 key space
//
// 최종 수렴 상태에서:
//   - 전체 데이터가 L6에만 존재 (비중첩 정렬)
//   - 읽기 증폭 최소 (L6 SSTable 1~2개만 확인)
//   - CBO 통계 최신화 → 최적 파티션 pruning
//
// Full Compaction 명시적 트리거:
//   OPTIMIZE TABLE <cube_name> FORCE;  (관리 명령)
//   → 파티션별 강제 full compaction (모든 레벨 → L6 단일 통합)
//   → 주의: 매우 높은 I/O 부하, 운영 시간 외 실행 권장
```

---

## 3. 런타임 계획 구조 (Query Node / Compute Node)

### 3.1 LogicalPlan

```
LogicalPlan {
    // 노드 타입 (열거형)
    Scan { cube_id, projections, predicates, partitions_pruned }
    Filter { input, predicate }
    Aggregate { input, group_by, aggregates }
    Join { left, right, join_type, condition }
    Sort { input, order_by }
    Limit { input, n }
    Projection { input, exprs }
    // 분석 전용
    FunnelAnalysis { input, steps, time_window, user_key }
    CohortAnalysis { input, entry_event, cohort_period, user_key }
    PathAnalysis { input, max_depth, user_key }
}
```

### 3.2 PhysicalPlan (Fragment)

```
PhysicalPlan {
    fragment_id:  Uuid,
    assigned_cn:  NodeId,
    exchanges:    Vec<Exchange>,   // Shuffle / Broadcast
    operators:    Vec<PhysOp>,
}

PhysOp {
    // 열거형
    TabletScan    { tablet_ids, columns, runtime_filter }
    SIMDFilter    { predicate_bitmap }
    VectorizedAgg { group_keys, agg_funcs }
    HashJoin      { build_side, probe_side, hash_keys }
    RuntimeFilter { filter_type, bloom_bits }
    Exchange      { exchange_type: Shuffle | Broadcast | Gather }
    Sort          { keys, top_k }
    FunnelExec    { steps, window, bitmask_width }
    CohortExec    { cohort_def, periods }
    PathExec      { max_depth, top_n }
}
```

---

## 4. 분산 메타데이터 (Raft KV 저장 항목)

| 키 패턴 | 값 타입 | 설명 |
|---|---|---|
| `/cubes/{cube_id}` | `CubeSchema` | Cube 스키마 |
| `/partitions/{cube_id}/{partition_id}` | `Partition` | 파티션 메타데이터 |
| `/tablets/{tablet_id}` | `Tablet` | Tablet → DN 매핑 |
| `/smv/{smv_id}` | `SessionMaterializedView` | SMV 정의 |
| `/mv/{mv_id}` | `PreAggMV` | 사전 집계 MV |
| `/nodes/{node_id}` | `NodeInfo` | 노드 상태 (QN/CN/SN) |
| `/stats/{cube_id}/{column}` | `ColumnStats` | CBO 통계 |
| `/routine_load/{job_id}` | `RoutineLoadJob` | Kafka 수집 잡 |
| `/sessions/{token}` | `WebSession` | Web UI 세션 토큰 |
| `/resource_groups/{name}` | `ResourceGroup` | 리소스 정책 |

---

## 5. 상태 전이

### 5.1 SMV 상태 전이

```
[없음] --CREATE--> [Building] --구체화 완료--> [Ready]
[Ready] --refresh 시작--> [Refreshing] --완료--> [Ready]
[Building|Refreshing] --오류--> [Error] --재시도--> [Building]
```

### 5.2 Tablet Replica 상태 전이

```
[Offline] --노드 시작--> [Catching Up] --LSN 동기화 완료--> [Normal]
[Normal] --노드 장애--> [Offline]
[Normal] --Leader 선출--> [Leader] | [Follower]
```

### 5.3 RoutineLoad 잡 상태 전이

```
[Created] --시작--> [Running] --일시중지--> [Paused] --재개--> [Running]
[Running] --오류--> [Error] --재시도--> [Running]
[Running|Paused] --취소--> [Cancelled]
```
