# WOW-DB 로깅 아키텍처 설계

| 항목 | 내용 |
|---|---|
| 문서 버전 | v1.0 |
| 작성일 | 2026-04-14 |
| 상태 | 확정 (Approved) |
| 관련 태스크 | T-LOG-001 ~ T-LOG-010 |

---

## 1. 개요

WOW-DB는 3-Tier 분산 아키텍처(QN/CN/SN)를 채택하며, 각 노드가 독립적으로 운영된다.
성공적인 운영과 디버깅을 위해 **체계적인 구조화 로깅(Structured Logging)**이 필수적이다.

### 1.1 핵심 요구사항

| 요구사항 | 설명 |
|---|---|
| **모듈 분리** | QN / CN / SN 각 노드별 독립 로그 파일 |
| **파일 저장** | 콘솔 + 파일 듀얼 싱크 (stdout + rolling file) |
| **자동 로테이션** | 일별(Daily) 로테이션, 기본 30일 보존 |
| **구조화 필드** | `node_id`, `shard_id`, `partition`, `tx_id` 등 추적 가능 키 |
| **LSM 진단** | MemTable flush, Compaction, WAL 이벤트 세부 추적 |
| **성능 진단** | 슬로우 쿼리, 쓰기 증폭 비율, 캐시 히트율 |
| **환경 변수 제어** | `RUST_LOG` 로 런타임 레벨 변경 가능 |

### 1.2 로깅 라이브러리 스택

```
애플리케이션 코드
    ↓ tracing::info! / warn! / debug! / error!
tracing (0.1)          ← 계측(Instrumentation) 레이어
    ↓
tracing-subscriber (0.3)  ← 구독자(Subscriber) 레이어
    ├── fmt::Layer (stdout)   ← 개발용 컬러 출력
    └── fmt::Layer (file)     ← 파일 기록 (구조화)
         ↓
tracing-appender (0.2)    ← 비동기 Rolling File I/O
```

---

## 2. 로그 파일 위치 및 명명 규칙

### 2.1 디렉토리 구조

```
<module_root>/
└── logs/
    ├── query-node.2026-04-14.log
    ├── query-node.2026-04-15.log
    └── ...

<module_root>/
└── logs/
    ├── compute-node.2026-04-14.log
    └── ...

<data_dir>/               ← DATA_DIR 환경변수 (기본: /data)
└── logs/
    ├── storage-node.2026-04-14.log
    └── ...
```

### 2.2 환경 변수

| 변수명 | 기본값 | 설명 |
|---|---|---|
| `RUST_LOG` | `{node}=info,shared=info` | 로그 레벨 필터 |
| `LOG_DIR` | `./logs` (QN/CN), `$DATA_DIR/logs` (SN) | 로그 파일 디렉토리 |
| `LOG_ROTATION` | `DAILY` | 로테이션 주기 (DAILY \| HOURLY \| NEVER) |
| `LOG_MAX_DAYS` | `30` | 보존 일수 (삭제는 외부 logrotate 또는 cron 위임) |
| `DATA_DIR` | `/data` | SN 데이터 디렉토리 (로그도 여기에 저장) |

### 2.3 파일명 형식

```
{node-name}.{YYYY-MM-DD}.log

예시:
  query-node.2026-04-14.log    ← QN 일별 로그
  compute-node.2026-04-14.log  ← CN 일별 로그
  storage-node.2026-04-14.log  ← SN 일별 로그
```

---

## 3. 로그 레벨 정의

| 레벨 | 용도 | 예시 |
|---|---|---|
| `ERROR` | 데이터 유실·서비스 중단 가능성 있는 오류 | WAL 쓰기 실패, Raft 합의 실패 |
| `WARN` | 성능 저하·재시도 가능한 일시 오류 | Compaction 지연, 슬로우 쿼리 |
| `INFO` | 주요 오퍼레이션 완료 및 상태 변경 | Table 생성, Tablet 할당, Flush 완료 |
| `DEBUG` | 내부 처리 흐름 (개발·QA 환경용) | WHERE 조건 평가, 파티션 Pruning 결과 |
| `TRACE` | 매우 세밀한 디버깅 (성능 측정 포함) | 개별 행 삽입, Bloom Filter 프로브 |

### 3.1 기본 레벨 구성

```
# 운영(Production)
RUST_LOG=query_node=info,compute_node=info,storage_node=info,shared=info

# QA / 통합 테스트
RUST_LOG=query_node=debug,compute_node=debug,storage_node=debug,shared=debug

# LSM 집중 디버깅
RUST_LOG=storage_node::lsm=trace,storage_node=debug,shared=info

# Compaction 진단
RUST_LOG=storage_node::lsm::compaction=trace,storage_node=info

# 쿼리 플래너 진단
RUST_LOG=query_node::planner=trace,query_node=info
```

---

## 4. 모듈별 로그 설정

### 4.1 Query Node (QN)

| 서브모듈 | 기본 레벨 | 주요 이벤트 |
|---|---|---|
| `mysql_protocol` | INFO | 커넥션 수락, COM_QUERY 처리, 오류 응답 |
| `planner::cbo` | INFO | 쿼리 실행 계획 선택, 파티션 Pruning 결과 |
| `meta::table` | INFO | Table DDL(CREATE/ALTER/DROP), 스키마 버전 변경 |
| `raft` | INFO | Leader 선출, 메타데이터 복제 완료 |
| `session_mv` | INFO | SMV 생성·갱신 시작/완료 |
| `profiler` | WARN | 슬로우 쿼리 (> 1초) |
| `ingestion` | INFO | Kafka 배치 처리 완료, 오프셋 커밋 |

### 4.2 Compute Node (CN)

| 서브모듈 | 기본 레벨 | 주요 이벤트 |
|---|---|---|
| `executor` | INFO | Fragment 실행 시작/완료, 처리 행 수 |
| `analytics` | INFO | FUNNEL/COHORT/PATH 연산 완료, 소요 시간 |
| `shuffle` | DEBUG | CN 간 데이터 교환 배치 크기 |
| `result_cache` | DEBUG | 캐시 히트/미스, 항목 교체 |
| `runtime_filter` | DEBUG | Runtime Filter 생성·적용 통계 |

### 4.3 Storage Node (SN)

| 서브모듈 | 기본 레벨 | 주요 이벤트 |
|---|---|---|
| `lsm::wal` | INFO | WAL 열기, 세그먼트 로테이션, 재생 완료 |
| `lsm::memtable` | DEBUG | 삽입 행 수, 임계값 초과, Freeze 완료 |
| `lsm::sstable` | INFO | Flush 완료 (seq, row_count, size_bytes) |
| `lsm::compaction` | INFO | Compaction 시작/완료 (레벨, 입출력 파일 수) |
| `lsm::manifest` | WARN | MANIFEST 파싱 오류, 미완료 Compaction 발견 |
| `block_cache` | DEBUG | LRU 교체, 캐시 히트율 |
| `tiering` | INFO | Hot→Cold 이동 트리거, 이동 파티션 |

---

## 5. 구조화 필드 규약 (Structured Field Conventions)

모든 tracing 호출에서 아래 필드 네이밍 규칙을 따른다.

### 5.1 공통 필드

| 필드명 | 타입 | 설명 |
|---|---|---|
| `node_id` | String | 노드 식별자 (예: `qn-1`, `sn-2`) |
| `table` | String | Table/테이블 이름 |
| `database` | String | 데이터베이스 이름 |
| `tx_id` | u64 | 트랜잭션 ID |
| `query_id` | UUID | 쿼리 식별자 |
| `elapsed_ms` | u64 | 소요 시간 (밀리초) |
| `err` | String | 오류 메시지 (warn/error 레벨에만 포함) |

### 5.2 LSM 전용 필드

| 필드명 | 타입 | 설명 |
|---|---|---|
| `partition` | String | 파티션 디렉토리 경로 또는 파티션 ID |
| `shard_id` | UUID | Shard(Tablet) 식별자 |
| `part_id` | UUID | SSTable Part 식별자 |
| `level` | u32 | LSM 레벨 (0~N) |
| `input_level` | u32 | Compaction 입력 레벨 |
| `output_level` | u32 | Compaction 출력 레벨 |
| `input_count` | usize | Compaction 입력 파일 수 |
| `output_count` | usize | Compaction 출력 파일 수 |
| `row_count` | u64 | 처리된 행 수 |
| `size_bytes` | u64 | 파일/배치 크기 (바이트) |
| `seq` | u64 | WAL/SSTable 시퀀스 번호 |
| `compaction_id` | UUID | Compaction 작업 ID |

### 5.3 쿼리 전용 필드

| 필드명 | 타입 | 설명 |
|---|---|---|
| `sql` | String | SQL 텍스트 (슬로우 쿼리 시 포함) |
| `plan_type` | String | 실행 계획 유형 (HashJoin, MergeJoin, etc.) |
| `partitions_scanned` | u32 | 스캔한 파티션 수 |
| `partitions_pruned` | u32 | Pruning으로 스킵한 파티션 수 |
| `rows_scanned` | u64 | 스캔한 행 수 |
| `rows_returned` | u64 | 반환된 행 수 |

---

## 6. LSM 진단 이벤트 카탈로그

### 6.1 WAL 이벤트

```rust
// WAL 열기
info!(node_id, dir = %wal_dir, seq = last_seq, "WAL opened");

// WAL 세그먼트 로테이션
info!(node_id, seq = new_seq, size_bytes = prev_size, "WAL segment rotated");

// WAL 재생 완료
info!(node_id, dir = %wal_dir, entries_replayed = count, "WAL replay complete");

// WAL Group Commit flush 실패 (WARN)
warn!(node_id, seq, err = %e, "WAL Group Commit flush failed");

// WAL CRC 불일치 (WARN - 크래시 복구 경계)
warn!(node_id, seq, tx_id, err = %e, "WAL CRC mismatch — truncating segment");
```

### 6.2 MemTable 이벤트

```rust
// 임계값 초과 → Flush 필요
debug!(
    partition = %partition_dir,
    size_bytes = current_size,
    threshold = threshold_bytes,
    "MemTable threshold exceeded — scheduling flush"
);

// MemTable Freeze (Immutable 전환)
info!(
    partition = %partition_dir,
    row_count,
    size_bytes,
    seq = last_seq,
    "MemTable frozen → ImmutableMemTable"
);

// MemTable Flush 완료
info!(
    partition = %partition_dir,
    part_id = %sst_id,
    row_count,
    size_bytes,
    level = 0u32,
    elapsed_ms,
    "MemTable flush complete → L0 SSTable"
);
```

### 6.3 Compaction 이벤트

```rust
// Compaction 시작
info!(
    partition = %partition_dir,
    compaction_id = %job.id,
    input_level  = job.input_level,
    output_level = job.output_level,
    input_count  = job.inputs.len(),
    "Compaction started"
);

// Compaction 완료
info!(
    partition = %partition_dir,
    compaction_id = %job.id,
    output_count  = output_ssts.len(),
    rows_merged   = merged_rows,
    rows_ttl_dropped = ttl_dropped,
    write_amp     = write_amplification,
    elapsed_ms,
    "Compaction complete"
);

// Compaction 오류 (WARN)
warn!(
    partition = %partition_dir,
    compaction_id = %job.id,
    err = %e,
    "Compaction failed — will retry"
);
```

### 6.4 SSTable 이벤트

```rust
// SSTable Flush 완료
info!(
    partition = %partition_dir,
    part_id    = %sst.id,
    seq        = sst.sequence_num,
    level      = sst.level,
    row_count  = sst.row_count,
    size_bytes = sst.size_bytes,
    columns    = sst.columns.len(),
    bloom_fpr  = bloom_false_positive_rate,
    "SSTable flushed"
);

// SSTable 스캔 완료 (DEBUG)
debug!(
    part_id      = %sst.id,
    rows_scanned = scan_count,
    rows_returned,
    bloom_probes = bloom_hit + bloom_miss,
    bloom_hits   = bloom_hit,
    granules_skipped,
    elapsed_ms,
    "SSTable scan complete"
);
```

---

## 7. 쿼리 진단 이벤트 카탈로그

```rust
// 쿼리 시작 (DEBUG)
debug!(query_id = %qid, sql = %sql_text, "Query started");

// 파티션 Pruning 결과 (DEBUG)
debug!(
    query_id          = %qid,
    table              = %table_name,
    partitions_total  = total,
    partitions_pruned = pruned,
    partitions_scanned = total - pruned,
    "Partition pruning applied"
);

// 슬로우 쿼리 (WARN — 1초 초과)
warn!(
    query_id      = %qid,
    sql           = %sql_truncated,
    elapsed_ms    = elapsed.as_millis(),
    rows_scanned,
    rows_returned,
    "Slow query detected"
);

// 쿼리 완료 (INFO)
info!(
    query_id      = %qid,
    elapsed_ms    = elapsed.as_millis(),
    rows_returned,
    "Query complete"
);
```

---

## 8. 로그 포맷

### 8.1 콘솔 출력 (개발용, 컬러 포함)

```
2026-04-14T10:23:45.123Z  INFO storage_node::lsm::compaction: Compaction started
    partition=/data/shards/abc123/partition=p_2026_q1
    compaction_id=f8a1b2c3-...
    input_level=0 output_level=1 input_count=4
```

### 8.2 파일 출력 (운영용, 구조화 키=값)

```
2026-04-14T10:23:45.123Z  INFO storage_node::lsm::compaction Compaction started partition="/data/shards/abc123/partition=p_2026_q1" compaction_id="f8a1b2c3-..." input_level=0 output_level=1 input_count=4 thread_id=ThreadId(5)
```

> **Note**: 파일은 `with_ansi(false)` 설정으로 ANSI 이스케이프 없이 저장.
> JSON 포맷이 필요한 경우 `LOG_FORMAT=json` 환경 변수로 전환 가능 (Phase E에서 구현 예정).

---

## 9. `shared::logging` 공개 API

```rust
/// WOW-DB 노드 로깅 초기화
///
/// # 인자
/// - `node_name`: 노드 이름 (예: "query-node", "storage-node")
/// - `log_dir`:   로그 파일 디렉토리 경로
/// - `default_filter`: RUST_LOG 미설정 시 기본 필터
///
/// # 반환
/// `LogGuard` — drop 되면 파일 버퍼가 flush·닫힘. main()에서 생존 기간 내내 보유해야 함.
///
/// # 예시
/// ```rust
/// let _log_guard = shared::logging::init_logging(
///     "query-node",
///     "./logs",
///     "query_node=info,shared=info",
/// );
/// ```
pub fn init_logging(node_name: &str, log_dir: &str, default_filter: &str) -> LogGuard;

pub struct LogGuard {
    _guard: tracing_appender::non_blocking::WorkerGuard,
}
```

---

## 10. 구현 상태

| 컴포넌트 | 상태 | 파일 |
|---|---|---|
| `shared::logging::init_logging` | ✅ 구현 완료 | `shared/src/logging.rs` |
| QN main.rs 연동 | ✅ 구현 완료 | `query-node/src/main.rs` |
| CN main.rs 연동 | ✅ 구현 완료 | `compute-node/src/main.rs` |
| SN main.rs 연동 | ✅ 구현 완료 | `storage-node/src/main.rs` |
| WAL 구조화 로깅 | ✅ 기존 구현 + 필드 강화 | `storage-node/src/lsm/wal.rs` |
| MemTable 구조화 로깅 | ✅ 구현 완료 | `storage-node/src/lsm/memtable.rs` |
| Compaction 구조화 로깅 | ✅ 기존 구현 + 필드 강화 | `storage-node/src/lsm/compaction.rs` |
| SSTable 구조화 로깅 | ✅ 기존 구현 | `storage-node/src/lsm/sstable.rs` |
| JSON 로그 포맷 | ❌ Phase E 예정 | — |
| Log 보존 정책 (자동 삭제) | ❌ Phase E 예정 (logrotate 위임) | — |
| Prometheus 로그 메트릭 연동 | ❌ Phase E 예정 | — |

---

## 11. 로그 분석 가이드

### 11.1 Compaction 병목 진단

```bash
# Compaction이 느린 경우 — elapsed_ms 기준 정렬
grep "Compaction complete" storage-node.*.log | \
  grep -oP 'elapsed_ms=\K[0-9]+' | sort -n | tail -20

# Write Amplification 추이
grep "Compaction complete" storage-node.*.log | \
  grep -oP 'write_amp=\K[0-9.]+' | awk '{sum+=$1; count++} END {print "avg:", sum/count}'
```

### 11.2 슬로우 쿼리 목록

```bash
# 최근 슬로우 쿼리 상위 10개
grep "Slow query" query-node.*.log | \
  grep -oP '(query_id|elapsed_ms|sql)="\K[^"]+' | paste - - - | sort -t$'\t' -k2 -rn | head -10
```

### 11.3 WAL 재생 이상 감지

```bash
# CRC 불일치 건수 (0이 정상)
grep -c "WAL CRC mismatch" storage-node.*.log

# 미완료 Compaction (크래시 후 복구 시 발생)
grep "미완료 compaction 발견" storage-node.*.log
```

### 11.4 MemTable Flush 빈도

```bash
# 시간당 Flush 횟수
grep "MemTable flush complete" storage-node.*.log | \
  grep -oP '^\d{4}-\d{2}-\d{2}T\d{2}' | sort | uniq -c
```

---

## 12. Docker Compose 로그 수집 연동

```yaml
# docker/docker-compose.yml 발췌
services:
  query-node:
    environment:
      - RUST_LOG=query_node=info,shared=info
      - LOG_DIR=/app/logs
    volumes:
      - qn_logs:/app/logs

  storage-node:
    environment:
      - RUST_LOG=storage_node=info,shared=info
      - DATA_DIR=/data
    volumes:
      - sn_data:/data   # /data/logs/ 에 로그 저장

volumes:
  qn_logs:
  sn_data:
```

> **운영 팁**: 컨테이너 로그는 `docker logs` 명령으로 stdout을 실시간 확인하고,
> 파일 로그는 볼륨 마운트된 호스트 경로에서 `tail -f` 또는 Filebeat/Fluentd로 수집한다.
