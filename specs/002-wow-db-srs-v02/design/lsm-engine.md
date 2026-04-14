# LSM Engine 상세 설계

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-14  
**모듈**: `storage-node/src/lsm/`  
**참조**: RocksDB 7.x, LevelDB, ClickHouse MergeTree

---

## 1. 개요

WOW-DB Storage Node의 LSM-Tree 엔진은 웹 이벤트의 대용량 append-only 쓰기를 최우선으로 설계된다. 핵심 설계 철학은 다음과 같다.

| 원칙 | 내용 |
|---|---|
| **파티션 격리** | LSM Merge(Compaction)는 파티션 경계 내에서만 발생. 파티션 간 merge 없음 |
| **컬럼 독립 파일** | 각 컬럼을 별도 `.col` 파일로 저장. 쿼리 시 필요한 컬럼만 I/O |
| **Leveled Compaction** | L1+ 레벨 내 key range 비중첩 보장으로 읽기 증폭 최소화 |
| **2-Tier Bloom Filter** | SSTable 수준 + Granule 수준 2단계 cascade bloom filter |
| **Sort Key 제약** | 최대 4개 컬럼, 직렬화 크기 128 bytes 이하 (비교 연산 캐시라인 내 완료) |

---

## 2. Write Path (쓰기 경로)

```
이벤트 도착
  │
  ▼
┌─────────────────────────────────────────┐
│  WAL Segment (Append-Only)              │
│  - CRC32 체크섬 (crc32fast)             │
│  - Group Commit: fdatasync 배치 (≤4ms)  │
│  - 세그먼트 크기: 기본 64MB             │
└─────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────┐
│  Active MemTable (Skip-list)            │
│  - Arc<RwLock<SkipList<SortKey, Row>>>  │
│  - Sort Key 기준 정렬 삽입              │
│  - 크기 임계값: 기본 64MB              │
└─────────────────────────────────────────┘
  │ 임계값 초과
  ▼
┌─────────────────────────────────────────┐
│  Immutable MemTable                     │
│  - 읽기 전용, 새 쓰기는 새 Active MT로  │
│  - 동시에 여러 Immutable MT 존재 가능   │
└─────────────────────────────────────────┘
  │ Flush (백그라운드 tokio task)
  ▼
┌─────────────────────────────────────────┐
│  Level-0 SSTable 파일들                 │
│  - 컬럼별 독립 파일: .col, .bloom, .min_max │
│  - L0끼리는 key range 겹침 허용         │
│  - L0 파일 수 ≥ level0_file_trigger(4) │
│    → L0→L1 Compaction 트리거           │
└─────────────────────────────────────────┘
  │ Compaction (백그라운드)
  ▼
Level-1, Level-2, ..., Level-N
(L1+ 내에서 key range 비중첩 불변 조건)
```

---

## 3. WAL (Write-Ahead Log) 상세

### 3.1 세그먼트 구조

```
WAL 디렉토리 레이아웃:
partition=p_2024_q1/
└── wal/
    ├── 000001.wal   ← 완료된 세그먼트 (flush 대기 또는 retention 중)
    ├── 000002.wal   ← 완료된 세그먼트
    └── 000003.wal   ← 현재 active 세그먼트 (append 중)

세그먼트 내부 레코드 포맷:
┌──────────────────────────────────────────┐
│  [4B] CRC32 체크섬                       │
│  [4B] payload 길이                       │
│  [1B] 레코드 타입 (Full/First/Middle/Last)│
│  [N B] payload (직렬화된 행 데이터)      │
└──────────────────────────────────────────┘
```

### 3.2 Group Commit

WAL fdatasync는 고비용 시스템 콜이다. Group Commit으로 배치 처리한다.

```rust
// 구현 방식 (개념)
struct WalWriter {
    pending_writes: Vec<(Sender<Result<()>>, Vec<u8>)>,
    flush_interval:  Duration,  // 기본 4ms
    max_batch_bytes: usize,     // 기본 4MB
}

// 동작:
// 1. 쓰기 요청을 pending_writes에 누적
// 2. flush_interval 경과 또는 max_batch_bytes 도달 시
//    → 한 번의 write() + fdatasync() 로 배치 플러시
// 3. 모든 대기 요청에 완료 알림
```

**성능 영향**: 4ms group commit 기준 약 250 IOPS → 1MB/req × 250 = 250MB/s WAL throughput.

### 3.3 세그먼트 Retention 정책

```
MemTable flush 완료 → 해당 MemTable과 연관된 WAL 세그먼트 삭제 가능
보관 조건:
  - 세그먼트의 마지막 LSN > 모든 Immutable MemTable의 최솟값 LSN
  - 크래시 복구 시 필요한 세그먼트만 보관
  - 설정: wal_retention_bytes (기본 0 = flush 즉시 삭제 가능)
```

### 3.4 크래시 복구 절차

```
1. wal/ 디렉토리에서 세그먼트 파일 목록 조회 (오름차순 정렬)
2. 각 세그먼트 레코드 순회:
   a. CRC32 검증 실패 → 해당 레코드부터 이후 모두 무시 (partial write)
   b. 검증 성공 → MemTable에 replay
3. Replay 완료 후 정상 동작 재개
4. 복구된 MemTable은 즉시 flush (새 SSTable 생성)
```

---

## 4. MemTable 상세

### 4.1 Skip-list 선택 근거

| 자료구조 | 장점 | 단점 | WOW-DB 선택 여부 |
|---|---|---|---|
| Skip-list | 동시 읽기 허용, O(log n) 삽입 | 캐시 비친화적 | **선택** |
| Red-Black Tree | 균형 보장 | 구조적 변경 시 락 경합 | 미선택 |
| 정렬 벡터 | 캐시 친화적, 이진 탐색 | 삽입 O(n), flush 전용 정렬 | 미선택 |

**선택 근거**: MemTable은 flush 전까지 읽기/쓰기가 동시에 발생. Skip-list는 Arc<RwLock>으로 읽기 병렬성을 유지하면서 정렬 순서를 보장.

### 4.2 Sort Key 직렬화 및 비교

```rust
// SortKey 직렬화 규칙 (Byte-comparable 형식)
// INT64  → 빅엔디언 8바이트 (부호 비트 flip으로 음수 정렬 보장)
// FLOAT64 → IEEE 754 빅엔디언 + 부호 처리
// DATETIME → Unix timestamp 빅엔디언 8바이트
// STRING → 길이 접두어 없는 UTF-8 + 0x00 종료자 (NULL 정렬 최솟값)
// NULLABLE → 0x00 (NULL) 또는 0x01 (NonNull) prefix
//
// 전체 직렬화 크기 제한: 128 bytes (컬럼 수: 최대 4개)
```

### 4.3 Flush 트리거

```
조건 1: size_bytes.load() >= flush_threshold (기본 64MB)
조건 2: WAL 세그먼트 크기 >= wal_segment_bytes (기본 64MB)
조건 3: 명시적 FLUSH 명령 수신 (관리 API)

트리거 시 동작:
  1. active MemTable → Immutable MemTable 전환 (원자적 포인터 교체)
  2. 새 active MemTable 생성, 새 WAL 세그먼트 시작
  3. 백그라운드 flush task 깨움
```

---

## 5. SSTable 파일 구조

### 5.1 컬럼별 독립 파일 레이아웃

```
partition=p_2024_q1/
└── lsm/
    ├── L0/
    │   ├── <sst_id_A>/
    │   │   ├── device_id.col       ← 컬럼 데이터 (LZ4/ZSTD 압축)
    │   │   ├── device_id.bloom     ← per-SSTable Bloom (Sort Key용)
    │   │   ├── device_id.min_max   ← Granule별 MINMAX 인덱스
    │   │   ├── event_name.col
    │   │   ├── event_name.bloom    ← BLOOM_FILTER 인덱스 (사용자 정의)
    │   │   ├── event_time.col
    │   │   ├── event_time.min_max
    │   │   └── _meta.json          ← SSTable 메타데이터
    │   └── <sst_id_B>/             ← L0: sst_id_A와 key range 겹침 가능
    │       └── ...
    ├── L1/
    │   ├── <sst_id_C>/             ← L1: key range 겹침 없음 (불변 조건)
    │   └── <sst_id_D>/
    └── L2/
        └── ...
```

### 5.2 _meta.json 형식

```json
{
  "sst_id": "550e8400-e29b-41d4-a716-446655440000",
  "level": 0,
  "partition_id": "...",
  "sort_key_min": ["\x00\x00...device_01", "\x00\x00\x65\xd0\xa5\x40"],
  "sort_key_max": ["\x00\x00...device_99", "\x00\x00\x65\xd0\xb9\x00"],
  "row_count": 131072,
  "size_bytes": 52428800,
  "created_at": "2026-04-14T10:00:00Z",
  "bloom_bits_per_key": 10,
  "granule_count": 16,
  "compression": "LZ4"
}
```

### 5.3 Granule (Data Skipping 단위)

```
SSTable 내 Granule 구조:
  - granule_size: 기본 8,192행 (설정 가능)
  - 각 Granule은 연속된 행 블록
  - 각 컬럼 파일에서 Granule 오프셋은 별도 offset_table에 저장

per-Granule 인덱스 (컬럼별):
  MINMAX: (min_val, max_val) → WHERE col > X 필터링
  BLOOM:  Bloom Filter       → WHERE col = X 필터링
  SET:    HashSet<Value>     → WHERE col IN (...) 필터링
  NGRAMBF: NGram Bloom      → WHERE col LIKE '%str%' 필터링
```

---

## 6. Leveled Compaction 상세

### 6.1 레벨 구조 및 크기 배율

```
레벨  │  max_bytes  │  특성                    │  예상 SSTable 수
──────┼─────────────┼──────────────────────────┼──────────────────
 L0   │  무제한     │  key range 겹침 허용      │  0~8 (트리거 전)
 L1   │  256 MB     │  비중첩, 크기 기반 compact│  ~26개 × 10MB
 L2   │  2.5 GB     │  비중첩                   │  ~250개
 L3   │  25 GB      │  비중첩                   │  ~2,500개
 L4   │  250 GB     │  비중첩                   │  ~25,000개
 L5   │  2.5 TB     │  비중첩                   │  ~250,000개
 L6   │  무제한     │  비중첩, 최하위 레벨      │  제한 없음

level1_max_bytes  = 256 MB  (설정 가능)
level_multiplier  = 10      (설정 가능)
max_levels        = 7       (L0~L6)
```

**200억 레코드 스케일 추정** (레코드 평균 200 bytes):
```
총 데이터: 200억 × 200B = 4TB
압축 후(LZ4 50%): ~2TB
→ L5(2.5TB) 이상이 주 데이터 레벨
→ L6 overflow 방지를 위해 L5 이상에서 Tiered Storage 연계
```

### 6.2 L0의 특수성 — Overlapping 허용

**이유**: L0 SSTable은 MemTable flush 결과이다. 각 MemTable이 Sort Key 전체 범위를 커버할 수 있으므로 L0 파일 간 key range 중첩이 발생한다.

```
예시:
  L0_A: sort_key ∈ ["device_001", "device_999"]  ← MemTable-1 flush
  L0_B: sort_key ∈ ["device_002", "device_998"]  ← MemTable-2 flush
  → L0_A와 L0_B의 key range 겹침 (정상)

읽기 시 영향:
  → key "device_500" 조회 시 L0_A, L0_B 모두 확인 필요
  → L0 파일이 많을수록 읽기 증폭 증가
  → L0 파일 수를 낮게 유지하는 것이 읽기 성능의 핵심
```

### 6.3 L1+ 불변 조건 — Non-overlapping

**이 불변 조건은 Leveled Compaction의 정의이다. 위반 시 읽기 정확성이 깨진다.**

```
L1 SSTable들의 key range는 서로 겹치지 않아야 한다:
  L1_C: sort_key ∈ ["device_000", "device_333"]
  L1_D: sort_key ∈ ["device_334", "device_666"]  ← C와 겹치지 않음
  L1_E: sort_key ∈ ["device_667", "device_999"]  ← D와 겹치지 않음
  → L1_C, D, E를 정렬된 순서로 나열하면 전체 key space를 분할

L0→L1 Compaction 시 불변 조건 유지 방법:
  1. L0 파일들에서 겹치는 key range를 가진 L1 파일들을 선택
  2. L0 + 선택된 L1 파일들을 함께 merge-sort
  3. 새 L1 SSTable을 non-overlapping하게 생성
  4. 이전 L0/L1 파일들 삭제
```

### 6.4 Compaction 트리거 조건

```rust
// L0 트리거 (파일 수 기반)
const LEVEL0_FILE_COMPACTION_TRIGGER: usize = 4;   // 기본값
const LEVEL0_SLOWDOWN_WRITE_TRIGGER:  usize = 8;   // 쓰기 속도 제한 시작
const LEVEL0_STOP_WRITE_TRIGGER:      usize = 12;  // 쓰기 중단 (배압)

// L1+ 트리거 (크기 기반)
// 각 레벨의 total_bytes > max_bytes 이면 Compaction 대상

// 우선순위 (높을수록 먼저):
// 1. L0 파일 수 > STOP_WRITE_TRIGGER → 강제 compaction (쓰기 차단)
// 2. L0 파일 수 > COMPACTION_TRIGGER → L0→L1 compaction 시작
// 3. Li total_bytes > max_bytes → Li→L(i+1) compaction 예약
// 4. (낮은 우선순위) L6 내 오래된 파일 → 주기적 rewrite (데이터 정렬 유지)
```

### 6.5 Compaction 파일 선택 전략

```
L0→L1 compaction:
  - 모든 L0 파일 선택
  - key range 겹치는 L1 파일 모두 포함
  - 합산 크기 기준 제한 없음 (L0 파일 수 제어가 목적)

L1→L2 (및 이하) compaction:
  - Least Recently Compacted 파일 선택 (균등 compaction 보장)
  - 선택된 L_i 파일의 key range와 겹치는 L_(i+1) 파일들 선택
  - target_file_size: 기본 64MB (생성되는 새 SSTable 크기 목표)
```

### 6.6 Write Amplification 분석

```
Leveled Compaction Write Amplification = 레벨 수 × level_multiplier

이론값:
  L0→L1: ×1 (L0는 직접 flush)
  L1→L2: ×10 (L1 데이터가 10배 큰 L2로 merge)
  L2→L3: ×10
  L3→L4: ×10
  총 WA (L1~L4 기준): 약 30×

실측값 (RocksDB 기준, 랜덤 쓰기):
  평균 WA ≈ 10~30×

WOW-DB 웹 이벤트 (sequential/append 특성) 예상:
  정렬된 Sort Key (event_time 순서) 입력 시 WA ≈ 5~10×
  이유: 이미 정렬된 입력은 L1→L2 compaction 시 전체 범위 재정렬 불필요

대안 비교:
  Tiered(Size-Tiered) Compaction WA ≈ 2~3× (쓰기 최적)
  → 하지만 읽기 증폭(RA)이 Leveled 대비 3~5배 높음
  → WOW-DB는 분석 쿼리(읽기) 최적화 우선 → Leveled 선택 유지
```

### 6.7 파티션 경계 제약

**LSM Merge는 파티션 경계를 절대 넘지 않는다. 이는 WOW-DB의 핵심 설계 불변 조건이다.**

```
근거:
  1. 파티션 Pruning: CBO가 파티션 단위로 min/max 통계를 관리
     → 파티션 간 merge 발생 시 통계 무효화
  2. TTL 정책: 파티션 단위로 만료 → 파티션 삭제로 한 번에 제거 가능
  3. Tiered Storage: 파티션 단위로 Hot→Cold 이동
  4. 병렬 처리: 서로 다른 파티션의 compaction은 완전 독립 실행 가능

구현 방식:
  - 각 파티션은 독립된 LSM 인스턴스를 보유
  - Compaction task는 partition_id 기준으로 격리
  - 다른 파티션의 SSTable을 입력으로 받는 compaction은 에러
```

### 6.8 TTL 통합

```
TTL 적용 시점: Compaction (행 단위 필터링)
TTL 정책 종류:
  1. 파티션 단위 TTL: partition.ttl_expires < now() → 파티션 전체 삭제
  2. 행 단위 TTL: TableSchema.ttl_policy.column_name(보통 event_time) 기준

Compaction 시 TTL 처리:
  for record in merge_iterator {
    if record.ttl_column_value < now() - ttl_duration {
      continue;  // 출력에 포함하지 않음 (삭제)
    }
    output.write(record);
  }

주의: TTL 행은 즉시 삭제되지 않음. 다음 compaction 시 제거.
      Tombstone(삭제 마커) 방식이 아닌 컬럼 값 비교 방식 사용.
```

---

## 7. Bloom Filter 상세 설계

### 7.1 2-Tier Cascade 구조

```
쿼리: WHERE device_id = 'D12345' AND sort_key = (...)

[Tier 1] SSTable-Level Bloom Filter (per-SSTable)
  목적: 이 SSTable이 해당 Sort Key를 포함하는지 확인
  키:   Sort Key 전체 직렬화 값
  사용: Point lookup, Compaction 시 key 존재 확인
  위치: <sst_id>.bloom 파일 (SSTable과 동일 디렉토리)
  크기: row_count × bits_per_key bits

    ↓ (Bloom이 "있을 수도 있음" 반환 시)

[Tier 2] Granule-Level Bloom Filter (per-Granule, 사용자 정의 컬럼)
  목적: 해당 Granule(8,192행 블록) 내에 값이 존재하는지 확인
  키:   사용자가 BLOOM_FILTER 인덱스를 지정한 컬럼 값
  사용: 개별 컬럼 predicate skipping
  위치: per-Granule 인덱스 데이터 (SSTable 내 offset table)
  크기: granule_row_count × bits_per_key bits

    ↓ (Granule Bloom이 "있을 수도 있음" 반환 시)

[실제 데이터 읽기]
  해당 Granule의 .col 파일에서 행 데이터 읽기
```

### 7.2 Hash 함수 선택: xxHash3

```
선택 근거:
  - xxHash3: 128-bit, SIMD-accelerated (AVX2), 최고 throughput
  - MurmurHash3: 128-bit, 검증됨, SIMD 미지원
  - CityHash: 구글 내부 용도, 유지보수 중단
  - SipHash: 암호화 강도, Bloom Filter에는 과도한 비용

xxHash3 성능 (벤치마크):
  - 스칼라: ~20 GB/s
  - AVX2: ~60 GB/s (3× 향상)

WOW-DB SIMD-first 철학에 부합 → xxHash3 선택
```

### 7.3 FPR(False Positive Rate) 및 bits-per-key

```
수학적 관계 (Bloom Filter):
  bits_per_key = -log2(FPR) / ln(2) = -1.44 × log2(FPR)

FPR 선택표:
  FPR 10%  → 5  bits/key  → 메모리 절약 (저가치 컬럼용)
  FPR 1%   → 10 bits/key  → 기본값 (SSTable-level bloom)
  FPR 0.1% → 14 bits/key  → 고선택도 컬럼 (device_id, user_id)
  FPR 0.01%→ 20 bits/key  → 특수 목적

WOW-DB 기본 설정:
  SSTable-level bloom: bits_per_key = 10 (FPR 1%)
  Granule-level bloom: bits_per_key = 10 (FPR 1%) [기본]
                       사용자가 CREATE TABLE 시 BLOOM_FILTER(bits=14) 지정 가능

최적 hash 함수 수:
  k = bits_per_key × ln(2) ≈ bits_per_key × 0.693
  bits=10 → k ≈ 7개 해시 함수
  xxHash3 128-bit 출력을 k개 구간으로 분할하여 해시 함수 k개 시뮬레이션
```

### 7.4 메모리 예산 분석

```
시나리오: 200억 레코드, 8,192행/Granule, 10 bits/key

SSTable-level bloom:
  200억 레코드 × 10 bits = 25 GB
  → 전체를 RAM에 올리는 것 불가능
  → Block Cache에서 filter 블록 별도 풀로 관리 (Section 9 참조)
  → 자주 접근하는 SSTable의 bloom만 캐시

Granule-level bloom:
  200억 ÷ 8,192 = 약 2,440만 Granule
  각 Granule bloom: 8,192 × 10 bits = ~10 KB
  → 총 2,440만 × 10 KB = 240 GB (전체)
  → 활성 쿼리 범위의 Granule만 캐시 (대부분 최근 파티션)
  → 실제 메모리 사용: 전체의 1~5% (수 GB)
```

### 7.5 Compaction 시 Bloom Filter 재생성

```
Compaction 과정:
  1. 입력 SSTable들의 레코드를 merge-sort으로 순회
  2. 출력 SSTable 생성 시:
     a. 새 Bloom Filter 빌더 초기화
     b. 각 레코드의 Sort Key를 bloom에 추가
     c. 출력 SSTable 완성 시 bloom filter 파일 (.bloom) 함께 생성
  3. 이전 SSTable들 (bloom 포함) 삭제

중요: 입력 SSTable의 bloom filter를 merge하는 것이 아니라
      출력 레코드 기반으로 새로 빌드해야 함.
      이유: Compaction 시 TTL 만료, 중복 제거 등으로 실제 레코드 수가 줄어들기 때문.

비용: Compaction 처리 시간의 약 5~10% (xxHash3 SIMD 덕분에 경미)
```

### 7.6 NGRAMBF_V1 (N-gram Bloom Filter)

```
목적: LIKE '%substr%' 쿼리 가속

작동 방식:
  컬럼 값 "page_view" → 3-gram 분해:
    "pag", "age", "ge_", "e_v", "_vi", "vie", "iew"
  각 N-gram을 Bloom Filter에 추가

쿼리 시:
  WHERE event_name LIKE '%view%'
  → "vie", "iew" N-gram을 bloom에서 확인
  → bloom에 없으면 Granule 스킵 (false negative 없음)
  → bloom에 있으면 실제 컬럼 데이터 확인 (false positive 가능)

설정: NGRAMBF_V1(n=3, bloom_size=1048576, hash_functions=4)
적합 컬럼: event_name, page_url, country_code 등
```

---

## 8. Sort Key 설계 제약

### 8.1 제약 조건 정의

```rust
// Sort Key 검증 규칙 (query-node/src/sql_parser/table_ddl.rs)
const SORT_KEY_MAX_COLUMNS:    usize = 4;
const SORT_KEY_MAX_BYTES:      usize = 128;
const SORT_KEY_STRING_MAX_LEN: usize = 64;  // STRING 컬럼 Sort Key 시 최대 길이

// 허용 데이터 타입
const SORT_KEY_ALLOWED_TYPES: &[DataType] = &[
    DataType::Int64,
    DataType::Float64,
    DataType::DateTime,
    DataType::String,   // 길이 제한 적용
    DataType::Boolean,
];
// JSON 컬럼은 Sort Key 불허 (정렬 기준 불명확)
```

### 8.2 직렬화 크기 계산

```
INT64    → 8 bytes
FLOAT64  → 8 bytes
DATETIME → 8 bytes (Unix timestamp nano)
BOOLEAN  → 1 byte
STRING   → 실제 데이터 길이 (최대 64 bytes)

예시:
  ORDER BY (device_id, event_time)
    device_id VARCHAR(36):  최대 36 bytes (UUID)
    event_time DATETIME:    8 bytes
    합계: 44 bytes ✅ (128 bytes 이하)

  ORDER BY (page_url, device_id, event_name, event_time)
    page_url VARCHAR(255): 최대 64 bytes (제한 적용)
    device_id:             36 bytes
    event_name:            20 bytes
    event_time:            8 bytes
    합계: 128 bytes ✅ (경계값, 허용)

  ORDER BY (full_page_url, referrer_url)
    full_page_url VARCHAR(500): 64 bytes (제한)
    referrer_url  VARCHAR(500): 64 bytes (제한)
    합계: 128 bytes ✅ (경계값)
```

### 8.3 성능 임계값

```
Sort Key 비교 비용 (CPU 캐시라인 기준):
  캐시라인 = 64 bytes
  키 크기 ≤ 64 bytes  → 1회 캐시라인 로드로 비교 완료 (최적)
  키 크기 65~128 bytes→ 2회 캐시라인 로드 (허용)
  키 크기 > 128 bytes → 3+ 캐시라인, Compaction 시 CPU bound 위험

Compaction에서의 키 비교 횟수:
  L2→L3 Compaction: 약 2.5억 레코드 처리 (25GB ÷ 100 bytes/record)
  각 레코드: merge-sort 시 평균 log2(input_files)회 비교 ≈ 10~20회
  총 키 비교: 약 25~50억 회
  → Sort Key 크기 2× 증가 = Compaction 시간 1.5~2× 증가
```

### 8.4 웹 분석 권장 패턴

```sql
-- 패턴 1: 사용자 중심 분석 (Funnel/Cohort/Path 최적)
CREATE TABLE page_events (...) 
  PARTITION BY RANGE(event_time) INTERVAL '1 MONTH'
  ORDER BY (device_id, event_time);
-- 이유: 파티션 내에서 device_id로 클러스터링 → 사용자별 이벤트 시퀀스가
--       물리적으로 인접 → Funnel/Cohort 스캔 시 I/O 최소화

-- 패턴 2: 이벤트 타입 필터링 최적
CREATE TABLE page_events (...)
  PARTITION BY RANGE(event_time) INTERVAL '1 DAY'
  ORDER BY (event_name, event_time);
-- 이유: MINMAX 인덱스로 event_name = 'purchase' 파티션 내 범위 스킵

-- 패턴 3: 복합 (사용자 + 이벤트)
CREATE TABLE page_events (...)
  PARTITION BY RANGE(event_time) INTERVAL '1 MONTH'
  ORDER BY (device_id, event_name, event_time);
-- 이유: 사용자별 특정 이벤트 타입 조회 최적. 총 크기 ≈ 36+20+8 = 64 bytes

-- 안티패턴: 피해야 할 패턴
-- ORDER BY (page_url, properties);  -- JSON 불허, URL 너무 긴 경우 경고
-- ORDER BY (col1, col2, col3, col4, col5);  -- 최대 4개 초과 → 에러
```

### 8.5 Sort Key와 컬럼 인코딩 상호작용

```
Sort Key 첫 번째 컬럼이 단조 증가(event_time 등)인 경우:
  → Delta Encoding 자동 적용 (SortKey의 연속 값 차이만 저장)
  → 압축률: 8 bytes/value → 평균 2~4 bytes/value

Sort Key 첫 번째 컬럼이 저기수(event_name 등)인 경우:
  → Dictionary Encoding 자동 적용
  → 압축률: 20 bytes/value → 4 bytes/value (사전 참조)

Prefix Compression (Sort Key 중복 접두어):
  Sort Key가 (device_id, event_time)이고 같은 device_id 연속 시:
    full:   "device_A", 1000 / "device_A", 1001 / "device_A", 1002
    prefix: 공유 접두어 "device_A" 한 번 → 이후는 차분값만 저장
  → SSTable 내 Sort Key 저장 공간 30~50% 절감
```

---

## 9. Block Cache 설계

### 9.1 캐시 풀 분리

```
전체 Block Cache 크기: block_cache_size (설정, 기본 RAM의 30%)

풀 분할:
  ┌─────────────────────────────────────────────┐
  │ Data Block Pool: 전체의 70%                  │
  │  - 실제 컬럼 데이터 (.col 파일 블록)         │
  │  - LRU 교체 정책                             │
  │  - 압축 해제된 상태로 캐시                   │
  ├─────────────────────────────────────────────┤
  │ Filter Block Pool: 전체의 20%               │
  │  - Bloom Filter 데이터 (.bloom 파일)         │
  │  - Granule 인덱스 (MINMAX, SET, NGRAMBF)    │
  │  - 우선 보존 정책 (Data보다 교체 저항)       │
  │  이유: Bloom Filter 캐시 미스 = 쓸모없는    │
  │        Data Block 읽기 발생               │
  ├─────────────────────────────────────────────┤
  │ Index Block Pool: 전체의 10%               │
  │  - SSTable 내 Granule 오프셋 테이블         │
  │  - 항상 캐시 유지 권장 (크기 작음)          │
  └─────────────────────────────────────────────┘
```

### 9.2 캐시 접근 순서 (읽기 최적화)

```
Tablet Scan 시 캐시 접근 순서:
  1. Index Block Pool → Granule 오프셋 확인
  2. Filter Block Pool → SSTable-level bloom 확인
     → bloom "없음" → 다음 SSTable로 이동 (Data 읽기 없음)
     → bloom "있을 수도" → 계속
  3. Filter Block Pool → Granule-level bloom/MINMAX 확인
     → Granule 스킵 가능 → 건너뜀
     → 스킵 불가 → 계속
  4. Data Block Pool → 실제 컬럼 데이터 읽기
     → 캐시 미스 → NVMe/S3/HDFS에서 읽기 후 캐시 저장
```

---

## 10. Read Path (읽기 경로)

```
쿼리: SELECT event_name, COUNT(*) FROM page_events
      WHERE device_id = 'D12345' AND event_time > '2026-01-01'
      GROUP BY event_name

1. CBO: 파티션 Pruning
   → partition min_max 통계로 event_time 조건 불만족 파티션 제외

2. 대상 파티션에서 레벨별 SSTable 후보 선정
   L6 → L5 → ... → L0 (최신 데이터가 L0)

3. 각 SSTable에 대해:
   a. [Tier-1 Bloom] Sort Key bloom 확인 (device_id+event_time 범위)
      → miss: SSTable 완전 스킵
      → hit(가능성 있음): 계속

   b. [MINMAX Index] 각 Granule의 device_id min/max 확인
      → min > 'D12345' 또는 max < 'D12345': Granule 스킵

   c. [Tier-2 Bloom] Granule bloom 확인 (device_id)
      → miss: Granule 스킵
      → hit: 실제 데이터 읽기

   d. [데이터 읽기] device_id.col + event_time.col + event_name.col
      → SIMD 필터 적용 (AVX2)
      → 조건 만족 행만 집계

4. 여러 MemTable (Active + Immutable) 조회 (최신 데이터)

5. 모든 결과 병합 (Merge Iterator)
   → 같은 Sort Key에 대해 최신 레코드 우선 (LSM 의미론)
```

---

## 11. 설정 파라미터 참조

```toml
# storage-node.toml — LSM 관련 설정

[lsm]
# MemTable
memtable_size_mb         = 64      # MemTable flush 임계값 (MB)
max_immutable_memtables  = 3       # Immutable MemTable 최대 동시 보유 수

# WAL
wal_segment_size_mb      = 64      # WAL 세그먼트 크기
wal_sync_interval_ms     = 4       # Group Commit flush 주기 (ms)
wal_retention_mb         = 0       # flush 후 WAL 보관량 (0 = 즉시 삭제)

# Leveled Compaction
level0_file_compaction_trigger = 4    # L0 파일 수 초과 시 L0→L1 compact
level0_slowdown_write_trigger  = 8    # 쓰기 속도 제한 트리거
level0_stop_write_trigger      = 12   # 쓰기 중단 트리거
level1_max_bytes_mb            = 256  # L1 최대 크기 (MB)
level_size_multiplier          = 10   # 레벨 크기 배율
max_levels                     = 7    # 최대 레벨 수 (L0~L6)
target_file_size_mb            = 64   # Compaction 출력 SSTable 목표 크기
compaction_threads             = 4    # 백그라운드 Compaction 스레드 수

# Bloom Filter
bloom_bits_per_key       = 10     # SSTable-level bloom: 10 bits/key (FPR ~1%)
granule_bloom_bits_per_key = 10   # Granule-level bloom: 10 bits/key

# Granule
granule_size_rows        = 8192   # Granule 크기 (행 수)

# Block Cache
block_cache_size_mb      = 4096   # 전체 블록 캐시 크기 (MB)
data_block_pool_ratio    = 0.70   # 데이터 블록 풀 비율
filter_block_pool_ratio  = 0.20   # 필터 블록 풀 비율
index_block_pool_ratio   = 0.10   # 인덱스 블록 풀 비율

# Sort Key
sort_key_max_columns     = 4      # Sort Key 최대 컬럼 수
sort_key_max_bytes       = 128    # Sort Key 최대 직렬화 크기 (bytes)
sort_key_string_max_len  = 64     # STRING 컬럼 Sort Key 최대 길이 (bytes)
```

---

## 12. SSTable 버전 번호 및 MANIFEST

### 12.1 sequence_num — 읽기 정확성의 핵심

```
sequence_num은 동일 Sort Key에 대해 "어느 쪽이 최신인가"를 결정한다.

할당 규칙:
  - 파티션 내 전역 AtomicU64 카운터
  - MemTable flush 시: 해당 Immutable MemTable의 WAL 마지막 LSN을 sequence_num으로 사용
  - Compaction 완료 시: Compaction 시작 시점의 최대 입력 sequence_num + 1 할당

읽기 시 사용:
  동일 Sort Key가 여러 SSTable에 존재 (L0 overlapping 상황):
    → sequence_num이 높은 것이 최신 버전
    → Merge Iterator에서 sequence_num 기준으로 최신 값만 반환
    → Compaction 시에도 동일 Sort Key는 sequence_num 최대값만 출력 (중복 제거)

예시:
  L0_A (seq=100): device_001 → event="page_view"
  L0_B (seq=150): device_001 → event="purchase"    ← 최신
  읽기 결과: device_001 = "purchase" (seq=150 우선)
```

### 12.2 generation — Compaction 이력 추적

```
generation 번호 진행 예시:

MemTable flush:
  [gen=0, seq=001] L0_SST_001  ← 원본 데이터 (flush)
  [gen=0, seq=002] L0_SST_002  ← 원본 데이터 (flush)
  [gen=0, seq=003] L0_SST_003  ← 원본 데이터 (flush)
  [gen=0, seq=004] L0_SST_004  ← L0 파일 4개 → compaction 트리거

L0 → L1 Compaction:
  입력:  L0_SST_001, L0_SST_002, L0_SST_003, L0_SST_004 (gen=0)
  출력:  [gen=1, seq=005] L1_SST_005
         [gen=1, seq=006] L1_SST_006   ← key range 분할 (target_file_size=64MB 기준)
  이전 L0 파일 삭제 (MANIFEST 갱신)

L1 → L2 Compaction:
  입력:  L1_SST_005 (gen=1), L1_SST_006 (gen=1) + 겹치는 L2 파일들
  출력:  [gen=2, seq=007] L2_SST_007
  이전 L1 파일 삭제

→ generation이 높을수록 "여러 번 merge된" 안정적인 데이터
→ L6에 도달한 데이터 (gen=5~6+): 최대로 압축되고 정렬된 상태
```

### 12.3 compacted_from — 혈통 추적

```
각 SSTable의 SstMeta.compacted_from = Vec<Uuid>:
  - flush된 SSTable: compacted_from = [] (원본)
  - compaction 결과: compacted_from = [입력_SST_ID, ...]

용도:
  1. 디버깅: 특정 SSTable이 어떤 원본 데이터에서 왔는지 추적
  2. Audit: 데이터 유출/손실 조사 시 원인 파악
  3. 향후 확장: Incremental Backup 시 new SSTable 식별
```

### 12.4 MANIFEST 파일

```
MANIFEST = LSM 상태의 진실(Source of Truth)

위치: partition=p_YYYY_MM/lsm/MANIFEST

내용 (JSON Lines 형식, 각 줄이 하나의 변경 이벤트):
  {"type":"ADD",    "sst_id":"...", "level":0, "seq":1, "gen":0, ...}
  {"type":"ADD",    "sst_id":"...", "level":0, "seq":2, "gen":0, ...}
  {"type":"REMOVE", "sst_id":"...", "level":0}
  {"type":"ADD",    "sst_id":"...", "level":1, "seq":5, "gen":1, ...}
  ...

원자적 갱신:
  1. 새 MANIFEST.tmp 파일에 변경 이벤트 기록
  2. fsync(MANIFEST.tmp)
  3. rename(MANIFEST.tmp → MANIFEST)  ← 원자적 파일 교체
  4. 이전 삭제 대상 SSTable 파일 삭제

크래시 복구:
  1. MANIFEST 파일 읽기
  2. 각 ADD 이벤트에서 SSTable 파일 존재 확인
  3. REMOVE된 SSTable은 이미 삭제됨 (또는 재삭제)
  4. 남은 SSTable들로 LevelState 재구성
  5. 복구된 상태에서 정상 동작 재개
```

---

## 13. 물리 파일 크기 및 레이아웃 상세

### 13.1 `.col` 파일 내부 블록 크기

```
파일 구성 요소별 크기 (64MB SSTable, 10개 컬럼, 8개 Granule 기준):

Header:           120 bytes
Offset Table:     GRANULE_COUNT × 16 bytes = 8 × 16 = 128 bytes
Data Blocks:      ~63 MB (전체의 ~99%)
  각 Granule:    ~8 MB (63MB / 8개)
  압축 후:       ~2~6 MB (LZ4 50~75% 압축률)
Footer:           20 bytes

총 파일 크기:     ~120 + 128 + 실제_데이터 + 20 ≈ 오버헤드 약 0.04%
```

### 13.2 컬럼별 파일 크기 추정

```
SSTable: 8,192 rows/granule × 16 granules = 131,072 rows

컬럼별 크기 (압축 전 → 압축 후):
  INT64    (8 bytes/row): 131,072 × 8 = 1.0 MB → LZ4 ~0.3 MB
  DATETIME (8 bytes/row, delta 인코딩): ~0.2 MB
  STRING 저기수 (dict): 131,072 × 4 = 0.5 MB (사전 포함 ~0.6 MB)
  STRING 고기수 (plain + LZ4): 131,072 × 36 = 4.5 MB → ~2 MB
  JSON (ZSTD): 131,072 × 200 = 25 MB → ~5 MB

bloom 파일:
  row_count × bits_per_key / 8 = 131,072 × 10 / 8 = ~164 KB

min_max 파일:
  GRANULE_COUNT × (8+8+1) bytes/entry = 16 × 17 = 272 bytes (INT64/DATETIME)
  → 사실상 무시할 수 있는 크기
```

### 13.3 파티션 전체 크기 예측

```
월별 파티션, 하루 1억 이벤트, 30일:
  총 레코드: 30억 개
  레코드당 평균 크기: 200 bytes (원본)
  압축 후: ~40 bytes/레코드 (ZSTD LZ4 혼합 80% 압축)

  30억 × 40 bytes = 120 GB/파티션

SSTable 수 (target 64MB):
  120 GB / 64 MB ≈ 1,875개

레벨 분포 (안정 상태):
  L0: 0~4개 (compaction 직후)
  L1: ~4개  (256MB)
  L2: ~40개 (2.5GB)
  L3: ~390개 (25GB)
  L4: ~1,440개 (92.5GB 남음)
  → L4 overflow → L5로 일부 이동
```

---

## 14. Full Compaction 수렴 (최종 병합)

### 14.1 자연적 수렴 과정

```
새 쓰기가 완전히 멈춘 후 자연 수렴 과정:

시간 T0: 쓰기 중단
  L0: 8개 파일 (마지막 batch)
  L1: 4개 파일
  L2: 40개 파일
  ...

시간 T1: L0→L1 완료 (L0 파일 수 트리거)
  L0: 0개
  L1: 8개 (이전 4개 + L0 merge 결과)

시간 T2: L1→L2 완료 (L1 크기 초과)
  L1: 4개 (크기 내)
  L2: 50개

... (각 레벨이 크기 임계값 이하로 안정화될 때까지 반복)

최종 안정 상태:
  L0: 0개
  L1: 4개   (256MB, 비중첩)
  L2: 40개  (2.5GB, 비중첩)
  L3: 390개 (25GB, 비중첩)
  ...
  L6: N개   (전체 - 하위 레벨 합)

특성:
  - 전체 데이터의 90%+가 L6에 집중
  - 읽기 시 대부분 L6만 확인 → 읽기 증폭 최소
  - 동일 Sort Key의 중복이 모두 제거됨
```

### 14.2 명시적 Full Compaction (`OPTIMIZE TABLE FORCE`)

```sql
-- 관리 명령 (FR-030)
OPTIMIZE TABLE page_events FORCE;
-- 또는 파티션 지정:
OPTIMIZE TABLE page_events PARTITION p_2024_01 FORCE;
```

```
Full Compaction 동작:
  1. 대상 파티션의 모든 레벨(L0~L5) SSTable을 단일 compaction 작업으로 처리
  2. 모든 입력 SSTable → merge-sort → L6 SSTable들로 출력
  3. TTL 만료 행 필터링 포함
  4. Bloom Filter, MINMAX 인덱스 새로 빌드
  5. 완료 후: L0~L5 = 0개 파일, L6 = 전체 데이터

결과:
  - 읽기 성능 최적화 (단일 레벨 탐색)
  - 디스크 공간 회수 (space amplification 해소)
  - CBO 통계 정확도 향상

비용:
  - 전체 파티션 데이터 읽기/쓰기 (예: 120GB 파티션 → 120GB I/O)
  - Compaction 중 쓰기 속도 저하 가능 (I/O 경합)
  - 운영 시간 외 또는 낮은 부하 시간대 실행 권장
```

### 14.3 수렴 후 상태 검증

```
SHOW COMPACTION STATUS FOR page_events;

결과 예시:
  ┌───────────────┬──────────┬──────────────┬──────────────┐
  │ partition     │ level    │ sst_count    │ total_bytes  │
  ├───────────────┼──────────┼──────────────┼──────────────┤
  │ p_2024_01     │ L0       │ 0            │ 0 bytes      │
  │ p_2024_01     │ L1       │ 4            │ 245 MB       │
  │ p_2024_01     │ L2       │ 38           │ 2.3 GB       │
  │ p_2024_01     │ L3       │ 0            │ 0 bytes      │
  │ p_2024_01     │ L4       │ 0            │ 0 bytes      │
  │ p_2024_01     │ L5       │ 0            │ 0 bytes      │
  │ p_2024_01     │ L6       │ 1,456        │ 92.1 GB      │
  └───────────────┴──────────┴──────────────┴──────────────┘

compaction_score:
  L1: 245MB / 256MB = 0.96  (< 1.0 → 안정)
  L2: 2.3GB / 2.5GB = 0.92  (< 1.0 → 안정)
  → 전체 안정 상태 확인
```

---

## 15. 알려진 제약 및 향후 개선

| 항목 | 현재 제약 | 향후 개선 방향 |
|---|---|---|
| **Compaction 전략** | Leveled 고정 | Table별 Tiered/Leveled 선택 (FR 추가 필요) |
| **Write Amplification** | 10~30× | Leveled-N + Tiered 하이브리드 전략 |
| **Bloom Filter 크기** | 전체 메모리 불가 | 계층적 Block Cache로 Hot Bloom만 유지 |
| **Sort Key 타입** | 기본 타입만 허용 | ENUM, DECIMAL, FIXED_STRING 추가 검토 |
| **Prefix Compression** | 미구현 | Skip-list 기반 SSTable prefix 압축 추가 |
| **Compaction Priority** | 파일 수/크기 기반 | 쿼리 패턴 기반 우선순위 조정 (adaptive) |
