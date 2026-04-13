# Logical-to-Physical Mapping 상세 설계

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-14  
**모듈**: `query-node/src/meta/`, `storage-node/src/partition/`  
**요구사항**: FR-037, FR-038, FR-039, FR-040, FR-042  
**참조**: `design/query-execution-model.md` (쿼리 실행 모델), `design/lsm-engine.md` (LSM 물리 구조)

---

## 1. 왜 두 계층인가

WOW-DB는 **논리 계층**과 **물리 계층**을 명확하게 분리한다.

| 계층 | 주체 | 단위 | 관심사 |
|---|---|---|---|
| **논리 계층** | Query Node, Compute Node | Table → Partition → Shard | 어떤 데이터를 읽을 것인가? |
| **물리 계층** | Storage Node | Shard Dir → Part → Granule → Column File | 어디서, 어떻게 읽을 것인가? |

이 분리를 통해:
- QN/CN은 스토리지 내부 구조(LSM 레벨, 파일 레이아웃)를 알 필요가 없다
- SN은 쿼리 최적화 방식(Join 순서, 집계 전략)을 알 필요가 없다
- 두 계층 모두 독립적으로 진화할 수 있다

---

## 2. 논리 단위 계층 (Query Node 관점)

```
Database
  └── Table (= Cube)
        ├── Partition p_2024_q1
        │     ├── Shard 0  (distribution_key % N == 0)
        │     ├── Shard 1  (distribution_key % N == 1)
        │     └── Shard K  (...)
        ├── Partition p_2024_q2
        │     ├── Shard 0
        │     └── ...
        └── Partition p_2024_q3
              └── ...
```

### 2.1 Table (= Cube)

```
Table {
    table_id:     Uuid,           // = cube_id
    name:         String,         // 사용자 정의 이름 (예: "page_events")
    database:     String,
    schema:       CubeSchema,     // 컬럼 정의, Sort Key, 분산 키 등
    partitions:   Vec<Partition>, // 현재 활성 파티션 목록
    stats:        TableStats,     // 전체 테이블 수준 통계
}

TableStats {
    row_count:     u64,       // 전체 행 수 (증분 추적)
    size_bytes:    u64,       // 전체 물리 크기 (압축 포함)
    last_analyzed: Timestamp, // ANALYZE TABLE 마지막 실행 시각
}

// Raft KV 경로: /cubes/{table_id}
// CBO 통계 경로: /stats/{table_id}
```

### 2.2 Partition

```
Partition {
    partition_id:   Uuid,
    table_id:       Uuid,
    range_start:    Value,    // 파티션 키 시작값 (inclusive)
    range_end:      Value,    // 파티션 키 종료값 (exclusive)
    shards:         Vec<Shard>,
    stats:          PartitionStats,
    state:          PartitionState,  // Active | Frozen | Dropping
    tier:           StorageTier,     // Hot | Cold
    ttl_expires:    Option<Timestamp>,
}

PartitionStats {
    row_count:     u64,
    size_bytes:    u64,
    partition_key_min: Value,
    partition_key_max: Value,
}

// Raft KV 경로: /partitions/{table_id}/{partition_id}
// CBO 통계 경로: /stats/{table_id}/{partition_id}
```

**Partition Pruning 판단**:
```
WHERE event_time >= '2024-01-01' AND event_time < '2024-04-01'

For each partition p:
  if p.range_end <= '2024-01-01': PRUNE  (파티션 전체가 범위 앞)
  if p.range_start >= '2024-04-01': PRUNE  (파티션 전체가 범위 뒤)
  else: INCLUDE
```

### 2.3 Shard (= Tablet)

Shard는 Partition 내에서 Distribution Key의 해시 버킷으로 나눈 분산 스케줄링의 최소 단위이다.

```
Shard {
    shard_id:       Uuid,           // = tablet_id
    partition_id:   Uuid,
    bucket_id:      u32,            // distribution_key 해시 버킷 번호 (0 ~ N-1)
    replicas:       Vec<ShardReplica>,  // 복제본 목록
    stats:          ShardStats,
}

ShardReplica {
    sn_node_id:  NodeId,
    role:        ReplicaRole,   // Leader | Follower
    state:       ReplicaState,  // Normal | CatchingUp | Offline
    shard_dir:   PathBuf,       // SN 상의 물리 디렉토리 경로 (절대 경로)
    lsn:         u64,           // 마지막 적용 WAL LSN
}

ShardStats {
    row_count:     u64,
    size_bytes:    u64,
    // 컬럼별 통계 (CBO용)
    column_stats:  HashMap<ColumnName, ColumnStats>,
}

ColumnStats {
    min_val:    Value,
    max_val:    Value,
    ndv:        u64,        // Number of Distinct Values (HyperLogLog 추정)
    null_count: u64,
    histogram:  Option<Histogram>,  // 100 버킷, ANALYZE TABLE 후 생성
}

// Raft KV 경로: /tablets/{shard_id}
// CBO 통계 경로: /stats/{table_id}/{partition_id}/{shard_id}
```

---

## 3. 물리 단위 계층 (Storage Node 관점)

```
Storage Node 파일시스템
  └── {shard_dir}/                   ← 논리 Shard에 대응하는 물리 디렉토리
        ├── MANIFEST                  ← 현재 활성 Part 목록 (JSON Lines)
        ├── MANIFEST.tmp              ← 원자적 갱신용 임시 파일
        ├── WAL/
        │   ├── 000001.wal
        │   └── 000002.wal
        ├── L0/                       ← Level 0 Part (key range 겹침 허용)
        │   ├── {part_id}/
        │   │   ├── _meta.json        ← Part 메타데이터 (SstRef 직렬화)
        │   │   ├── {col_name}.col    ← 컬럼 데이터 (LZ4/ZSTD 압축)
        │   │   ├── {col_name}.bloom  ← Bloom Filter (xxHash3, 10 bits/key)
        │   │   └── {col_name}.min_max ← Granule별 MINMAX 인덱스
        │   └── {part_id}/
        │       └── ...
        ├── L1/                       ← Level 1+ (key range 비중첩 불변 조건)
        │   └── {part_id}/
        │       └── ...
        └── L2/ ... L6/
```

### 3.1 Part (= SSTable)

Part는 LSM-Tree의 한 레벨 내 개별 정렬 파일이다. Compute Node가 스캔을 요청하는 최소 단위.

```
Part {
    part_id:        Uuid,           // = sst_id
    level:          u32,            // 0~6
    sequence_num:   u64,            // 단조 증가 (동일 Sort Key에서 최신값 결정)
    generation:     u64,            // 0=flush 결과, N=N번째 Compaction 결과
    compacted_from: Vec<Uuid>,      // 이 Part 생성에 병합된 입력 Part ID들
    size_bytes:     u64,
    row_count:      u64,
    min_sort_key:   Vec<u8>,        // Partition 내 최솟값 (Predicate 스킵용)
    max_sort_key:   Vec<u8>,        // Partition 내 최댓값
    columns:        Vec<String>,    // 포함된 컬럼 이름 목록
    // MANIFEST에 저장됨 (SstRef 구조체)
}
```

### 3.2 Granule

Granule은 Part 내의 고정 행 블록이다. Data Skipping Index의 최소 단위이며, 건너뛸 수 있는 최소 IO 단위다.

```
Granule {
    granule_index:  u32,            // Part 내 0-based 인덱스
    row_offset:     u64,            // .col 파일 내 바이트 오프셋
    row_count:      u16,            // 기본 8,192행 (마지막 Granule은 더 작을 수 있음)
    // .min_max 파일에 저장되는 per-Granule 인덱스:
    col_min_max:    HashMap<ColumnName, (Value, Value)>,  // MINMAX 인덱스
}
```

### 3.3 Column File 종류

| 파일 확장자 | 내용 | Magic Bytes | 용도 |
|---|---|---|---|
| `.col` | 압축된 컬럼 데이터 블록 (Granule 단위) | `WOWDBCOL` | 실제 컬럼 값 읽기 |
| `.bloom` | Bloom Filter 비트 배열 (xxHash3, 10 bits/key) | `WOWBLOOM` | Point Lookup 스킵 |
| `.min_max` | Granule별 min/max 값 배열 | `WOWMINMX` | 범위 Predicate 스킵 |

---

## 4. 논리 → 물리 매핑 해석 알고리즘

### 4.1 QN: Shard → SN 매핑 조회

```
Input: shard_id (Uuid)
Output: (SN NodeId, shard_dir: PathBuf)

Algorithm:
  1. Raft KV에서 /tablets/{shard_id} 조회
  2. ShardReplica 목록에서 Leader replica 선택
     (Leader 없으면 state=Normal인 Follower 선택)
  3. 반환: (replica.sn_node_id, replica.shard_dir)

캐싱: QN 로컬 캐시 (TTL 500ms, DDL/Tablet 재배치 시 즉시 무효화)
```

### 4.2 CN: ShardScan → SN gRPC 요청 변환

```
Input: ShardScan { shard_id, sn_endpoint, columns, predicates, scan_range }
Output: Arrow2 RecordBatch 스트림

Algorithm:
  1. SN gRPC 엔드포인트에 ShardScanRequest 전송
  2. SN은 MANIFEST에서 Part 목록 반환
  3. CN은 각 Part의 min/max Sort Key와 predicates 비교
     → Sort Key 범위 밖 Part: 스킵 (응답에 포함되나 CN이 요청 안 함)
     → Sort Key 범위 안 Part: 스캔 요청
  4. 스킵되지 않은 Part에 대해 SN 스캔 실행 요청
```

### 4.3 SN: Part 스캔 실행 (4단계 필터링)

```
Input: PartScanRequest {
  part_id, columns, predicates,
  runtime_filter, bloom_probe_keys
}

단계 1 — SSTable-Level Bloom Filter (Part 단위 스킵):
  for each part in manifest:
    if bloom_probe_keys is not empty:
      if NOT part.bloom.may_contain_any(bloom_probe_keys):
        SKIP part  (Bloom Filter가 해당 key들이 없다고 판단)

단계 2 — Sort Key 범위 스킵 (Part 단위):
  if part.max_sort_key < predicate.min_val: SKIP
  if part.min_sort_key > predicate.max_val: SKIP

단계 3 — Granule MINMAX 스킵:
  for each granule in part:
    for each predicate:
      if granule.col_min_max[col].max < predicate.min_val: SKIP granule
      if granule.col_min_max[col].min > predicate.max_val: SKIP granule

단계 4 — Column File 읽기:
  for each surviving granule:
    for each requested column:
      read granule block from {col_name}.col
      decompress (LZ4/ZSTD)
      yield Arrow2 column chunk

출력: Arrow2 RecordBatch 스트림 (CN에 gRPC 스트리밍 반환)
```

### 4.4 L0 Multi-Version Read (시퀀스 번호 충돌 해결)

L0 Part는 key range 겹침이 허용되므로, 동일 Sort Key에 대해 여러 Part에 값이 존재할 수 있다.

```
규칙: 동일 Sort Key에 대해 sequence_num이 더 큰 Part의 값이 우선

구현:
  L0 Part 스캔 시 Sort Key 기준으로 결과를 Merge Sort
  동일 Sort Key가 중복되면 max(sequence_num)의 값만 출력

L1+ Part: key range 비중첩 불변 조건 → 중복 없음 → Merge Sort 불필요
```

---

## 5. CBO 통계 계층적 저장 구조

### 5.1 저장 위치 요약

```
논리 계층 (Raft KV 저장, 모든 QN에서 동일하게 조회 가능):

  /stats/{table_id}
    → TableStats { row_count, size_bytes, last_analyzed }

  /stats/{table_id}/{partition_id}
    → PartitionStats { row_count, size_bytes, partition_key_min, partition_key_max }

  /stats/{table_id}/{partition_id}/{shard_id}
    → ShardStats {
        row_count, size_bytes,
        column_stats: {
          "event_name": { min, max, ndv, null_count, histogram },
          "user_id":    { min, max, ndv, null_count, histogram },
          ...
        }
      }

물리 계층 (MANIFEST 내 저장, SN 로컬):

  {shard_dir}/MANIFEST
    → SstRef[] { part_id, level, sequence_num, generation,
                 row_count, min_sort_key, max_sort_key, size_bytes }
```

### 5.2 통계 수집 시점

| 통계 종류 | 수집 시점 | 업데이트 방식 |
|---|---|---|
| Table/Partition row_count | MemTable Flush, Compaction 완료 | 증분 (+/-) |
| Table/Partition size_bytes | MemTable Flush, Compaction 완료 | 증분 (+/-) |
| Shard row_count | MemTable Flush | 증분 (+) |
| Column min/max | MemTable Flush | 새 Part의 값과 비교하여 갱신 |
| Column NDV | 주기적 샘플링 또는 `ANALYZE TABLE` | 전체 재계산 |
| Column Histogram | `ANALYZE TABLE` 명시 실행 | 전체 재계산 |
| Part min_sort_key/max_sort_key | MemTable Flush, Compaction 완료 | MANIFEST 갱신 |

### 5.3 CBO가 계층별 통계를 활용하는 방식

```
쿼리 최적화 단계별 사용 통계:

1. Partition Pruning (QN CBO):
   - 사용: PartitionStats.partition_key_min/max
   - 목적: 쿼리 범위 밖 파티션 조기 제거

2. Join 순서 결정 (QN CBO):
   - 사용: ShardStats.row_count, ColumnStats.ndv
   - 목적: 작은 쪽을 Build Side로 선택

3. 집계 전략 선택 (QN CBO):
   - 사용: ColumnStats.ndv
   - ndv < 임계값 → Hash Agg (낮은 기수성)
   - ndv ≥ 임계값 → Sort Agg (높은 기수성)

4. Predicate Selectivity 추정 (QN CBO):
   - 사용: ColumnStats.histogram (100 버킷)
   - 목적: 각 WHERE 조건이 몇 행을 통과시킬지 추정

5. Part 범위 스킵 (SN):
   - 사용: SstRef.min_sort_key/max_sort_key (MANIFEST)
   - 목적: Sort Key 범위 밖 Part 즉시 스킵

6. Granule 스킵 (SN):
   - 사용: Granule.col_min_max (`.min_max` 파일)
   - 목적: 범위 predicate 밖 Granule 건너뜀
```

---

## 6. Raft KV 매핑 테이블 (전체)

| 키 패턴 | 값 타입 | 설명 | 갱신 주체 |
|---|---|---|---|
| `/cubes/{table_id}` | `CubeSchema` | Cube 스키마 정의 | QN (DDL) |
| `/partitions/{table_id}/{partition_id}` | `Partition` | 파티션 메타데이터 | QN (DDL/AutoPartition) |
| `/tablets/{shard_id}` | `Shard` (with replicas) | Shard → SN 매핑 | QN (Tablet 할당) |
| `/stats/{table_id}` | `TableStats` | 테이블 수준 통계 | QN (증분 갱신) |
| `/stats/{table_id}/{partition_id}` | `PartitionStats` | 파티션 수준 통계 | QN (증분 갱신) |
| `/stats/{table_id}/{partition_id}/{shard_id}` | `ShardStats` | Shard 수준 컬럼 통계 | SN → QN (보고) |
| `/smv/{smv_id}` | `SessionMaterializedView` | SMV 정의 | QN |
| `/mv/{mv_id}` | `PreAggMV` | 사전 집계 MV | QN |
| `/nodes/{node_id}` | `NodeInfo` | 노드 상태 (QN/CN/SN) | 각 노드 (heartbeat) |
| `/routine_load/{job_id}` | `RoutineLoadJob` | Kafka 수집 잡 | QN |
| `/sessions/{token}` | `WebSession` | Web UI 세션 토큰 (24h TTL) | QN |
| `/resource_groups/{name}` | `ResourceGroup` | 리소스 정책 | QN |
| `/cluster/read_only_state` | `ClusterReadOnlyState` | Read-Only 모드 상태 | QN Leader |

---

## 7. Shard 스캔 요청 프로토콜 (gRPC)

### 7.1 proto 정의 (요약)

```protobuf
// proto/storage.proto

message ShardScanRequest {
    bytes shard_id    = 1;  // UUID bytes
    repeated string columns = 2;
    repeated Predicate predicates = 3;
    LsmScanRange scan_range = 4;
    optional RuntimeFilter runtime_filter = 5;
    repeated bytes bloom_probe_keys = 6;  // Point Lookup Bloom 검사용
}

message LsmScanRange {
    uint32 min_level = 1;   // 기본 0
    uint32 max_level = 2;   // 기본 6
}

message Predicate {
    string column     = 1;
    PredicateOp op    = 2;  // EQ, NE, LT, LE, GT, GE, IN, NOT_IN, IS_NULL
    repeated bytes values = 3;
}

message PartMeta {
    bytes part_id        = 1;
    uint32 level         = 2;
    uint64 sequence_num  = 3;
    uint64 row_count     = 4;
    bytes min_sort_key   = 5;
    bytes max_sort_key   = 6;
    uint64 size_bytes    = 7;
}

// SN → CN 응답 (스트리밍)
message ShardScanResponse {
    oneof payload {
        PartListResponse  part_list  = 1;  // 첫 응답: 활성 Part 목록
        RecordBatchChunk  batch      = 2;  // 이후: Arrow2 RecordBatch 청크
        ScanStats         stats      = 3;  // 마지막: 스캔 통계
    }
}

message PartListResponse {
    repeated PartMeta parts = 1;
}

message ScanStats {
    uint32 parts_scanned  = 1;
    uint32 parts_skipped  = 2;
    uint64 granules_read  = 3;
    uint64 granules_skip  = 4;
    uint64 rows_returned  = 5;
    uint64 bytes_read     = 6;
}
```

---

## 8. 물리 경로 명명 규칙

```
Storage Node 로컬 파일시스템:

{storage.data_dir}/
└── {database}/
    └── {table_name}/                         ← Table
        └── partition={partition_id}/         ← Partition
            └── shard={shard_id}/             ← Shard (= shard_dir)
                ├── MANIFEST
                ├── WAL/
                │   └── {lsn:012}.wal
                ├── L0/
                │   └── {part_id}/
                │       ├── _meta.json
                │       ├── event_time.col
                │       ├── event_time.bloom
                │       ├── event_time.min_max
                │       ├── event_name.col
                │       ├── event_name.bloom
                │       └── ...
                ├── L1/
                └── L2/ ... L6/

예시 실제 경로:
  /data/wowdb/analytics/page_events/
    partition=018e1234-0001/
      shard=018e5678-0002/
        MANIFEST
        L0/018eaaaa-0003/
          event_time.col
          event_name.col
          user_id.bloom
```

---

## 9. 관련 문서

- **쿼리 실행 모델**: `specs/002-wow-db-srs-v02/design/query-execution-model.md`
- **LSM Engine 설계**: `specs/002-wow-db-srs-v02/design/lsm-engine.md`
- **데이터 모델**: `specs/002-wow-db-srs-v02/data-model.md` §2, §4
- **요구사항**: `specs/002-wow-db-srs-v02/spec.md` FR-037~FR-042
- **구현 태스크**: `specs/002-wow-db-srs-v02/tasks.md` Phase 13
