# Read-Only Mode 상세 설계 (FR-036)

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-14  
**요구사항**: FR-036 — 디스크 용량 초과 시 클러스터 전체 Read-Only 모드 전환  
**참조 모듈**: `storage-node/src/disk_monitor.rs`, `query-node/src/meta/cluster_guard.rs`

---

## 1. 개요

Storage Node의 데이터 디스크 또는 Query Node의 Raft WAL 디스크 사용량이 임계값에 도달하면, 새로운 데이터를 더 이상 안전하게 기록할 수 없는 상태가 된다. 이 경우 데이터 손실이나 Raft 로그 손상을 방지하기 위해 **클러스터 전체를 Read-Only 모드로 전환**한다.

Read-Only 모드에서는 모든 쓰기 작업(INSERT, DDL, Compaction 출력 등)이 차단되며, SELECT·SHOW·DESCRIBE·EXPLAIN 등의 읽기 작업은 정상적으로 계속 처리된다. 디스크 사용량이 복구 임계값 이하로 내려가면 자동으로 정상 모드로 복귀한다.

---

## 2. 트리거 조건

### 2.1 진입 임계값 (`disk_full_threshold`)

다음 조건 중 **하나라도** 충족되면 Read-Only 모드 진입:

| 노드 | 모니터링 대상 경로 | 임계값 (기본) |
|---|---|---|
| **Storage Node** | 데이터 디스크 (`storage.data_dir`) | 사용량 ≥ 95% |
| **Storage Node** | WAL 디스크 (`storage.wal_dir`, data_dir와 다른 경우) | 사용량 ≥ 95% |
| **Query Node** | Raft WAL 디스크 (`raft.wal_dir`) | 사용량 ≥ 95% |
| **Query Node** | Raft 스냅샷 디스크 (`raft.snapshot_dir`) | 사용량 ≥ 95% |

설정 예시:
```toml
# storage-node.toml
[disk_monitor]
disk_full_threshold     = 0.95   # 95% — Read-Only 모드 진입
disk_recovery_threshold = 0.85   # 85% — Read-Only 모드 자동 해제
check_interval_ms       = 5000   # 디스크 사용량 폴링 주기 (5초)
```

### 2.2 복구 임계값 (`disk_recovery_threshold`)

모든 모니터링 경로의 사용량이 **85% 이하**로 내려가면 자동으로 정상 모드 복귀.  
진입/복구 임계값 사이에 히스테리시스(hysteresis)를 두어 빈번한 상태 전환을 방지한다.

```
사용량
  100% ┤
   95% ┤─────────────────────── ← Read-Only 진입 임계값 (disk_full_threshold)
       │  Read-Only 모드
   85% ┤─────────────────────── ← 복구 임계값 (disk_recovery_threshold)
       │  정상 모드
    0% ┤
```

---

## 3. 감지 메커니즘

### 3.1 DiskMonitor (각 노드 로컬)

각 Storage Node와 Query Node는 독립적인 `DiskMonitor` 태스크를 백그라운드에서 실행한다.

```rust
// storage-node/src/disk_monitor.rs

pub struct DiskMonitor {
    config:        DiskMonitorConfig,
    cluster_guard: Arc<ClusterGuard>,  // 상태 전파 채널
}

pub struct DiskMonitorConfig {
    pub data_dir:               PathBuf,
    pub wal_dir:                Option<PathBuf>,
    pub disk_full_threshold:    f64,     // 기본 0.95
    pub disk_recovery_threshold: f64,   // 기본 0.85
    pub check_interval_ms:      u64,    // 기본 5_000
}

impl DiskMonitor {
    /// 백그라운드 tokio 태스크로 실행
    pub async fn run(self) {
        let mut interval = tokio::time::interval(
            Duration::from_millis(self.config.check_interval_ms)
        );
        loop {
            interval.tick().await;
            let usage = self.sample_disk_usage();
            if usage >= self.config.disk_full_threshold {
                self.cluster_guard.trigger_read_only(ReadOnlyReason::DiskFull {
                    path:   self.config.data_dir.clone(),
                    usage,
                }).await;
            } else if usage < self.config.disk_recovery_threshold {
                self.cluster_guard.try_exit_read_only(ReadOnlyReason::DiskFull {
                    path:   self.config.data_dir.clone(),
                    usage,
                }).await;
            }
        }
    }

    fn sample_disk_usage(&self) -> f64 {
        // statvfs()/GetDiskFreeSpaceEx() 시스템 콜
        // (total_blocks - free_blocks) / total_blocks
        todo!()
    }
}
```

### 3.2 폴링 주기 선택 근거

| 주기 | 장점 | 단점 |
|---|---|---|
| 1초 | 즉각 감지 | 시스템 콜 오버헤드, 불필요한 Raft 상태 변경 |
| **5초** (기본) | 합리적 지연, 낮은 오버헤드 | 최대 5초 지연 (5초 내 95% → 100% 드물음) |
| 30초 | 매우 낮은 오버헤드 | 디스크 폭발적 증가 시 대응 지연 |

5초 폴링 + 95% 임계값의 조합은 실제 디스크가 꽉 차기 전 충분한 여유를 확보한다.

---

## 4. 상태 전파

### 4.1 Raft KV 기반 클러스터 전체 공유

단일 노드가 디스크 초과를 감지하면, 이를 **Raft KV에 기록**하여 모든 QN이 즉시 인식할 수 있도록 한다.

```
Raft KV Key : /cluster/read_only_state
Raft KV Value: ReadOnlyState (JSON 직렬화)

ReadOnlyState {
    enabled:    bool,
    reasons:    Vec<ReadOnlyReason>,
    entered_at: Option<Timestamp>,   // 최초 진입 시각
    updated_at: Timestamp,
}

ReadOnlyReason {
    variant:   DiskFull | ManualOverride,
    node_id:   NodeId,               // 원인 노드
    path:      String,               // 문제가 된 디렉토리
    usage:     f64,                  // 측정된 사용률 (0.0~1.0)
    timestamp: Timestamp,
}
```

### 4.2 상태 전파 시퀀스

```
Storage Node (DiskMonitor)
  1. 사용량 ≥ 95% 감지
  2. gRPC: QN Leader에게 ReportDiskFull(node_id, path, usage) 전송
       (포트 9011, MetaService.ReportDiskFull RPC)

Query Node Leader
  3. Raft 로그에 SetReadOnly 엔트리 기록
  4. 과반수 팔로워 복제 완료 → 커밋
  5. QN 상태 머신에 read_only_state = true 적용

Query Node Follower (모든 QN)
  6. Raft 로그 복제 수신 → 로컬 상태 머신 갱신
  7. 이후 모든 쓰기 요청 거부 시작

Prometheus
  8. wowdb_cluster_read_only{reason="disk_full"} = 1 노출
```

### 4.3 상태 전파 지연 최대값

Raft 커밋 레이턴시 (일반 네트워크 환경):
- 단일 DC 클러스터: < 10ms
- 감지 → 전체 QN 쓰기 차단: ≤ 5초(폴링) + 10ms(Raft) ≈ **최대 5.01초**

---

## 5. 쓰기 차단 동작

### 5.1 차단되는 작업

Read-Only 모드 진입 후 **즉시** 다음 작업을 거부한다:

| 작업 종류 | 예시 구문 | 차단 위치 |
|---|---|---|
| 행 삽입 | `INSERT INTO ...` | QN MySQL Protocol Handler |
| Kafka 수집 | Routine Load 새 배치 | QN Ingestion Gateway |
| Spark 수집 | Stream Load HTTP | QN Ingestion Gateway |
| Async INSERT | QN 버퍼 → DN 플러시 | QN Async INSERT Flusher |
| DDL — Table 생성 | `CREATE TABLE ...` | QN SQL Parser / Table Manager |
| DDL — Table 변경 | `ALTER TABLE ...` | QN SQL Parser / Table Manager |
| DDL — Table 삭제 | `DROP TABLE ...` | QN SQL Parser / Table Manager |
| Compaction 파일 출력 | 새 SSTable 기록 | SN Compaction Service |
| MemTable Flush | Immutable → L0 파일 쓰기 | SN LSM Engine |

### 5.2 차단 시 MySQL 오류 응답

모든 차단된 쓰기 작업에 대해 MySQL Wire Protocol로 다음 오류를 반환한다:

```
ERROR 1290 (HY000): The WOW-DB server is running in read-only mode so it 
cannot execute this statement. Disk usage: 97.3% on node sn-01 
(/data/wowdb). Admin can override with: SET GLOBAL wowdb_read_only = OFF;
```

- **오류 코드**: `1290` (`ER_OPTION_PREVENTS_STATEMENT` — MySQL 표준)
- **SQL State**: `HY000`
- **메시지**: 원인 노드, 사용률, 관리자 명령 포함

### 5.3 허용되는 작업 (차단 없음)

| 작업 종류 | 예시 구문 |
|---|---|
| 쿼리 | `SELECT ...`, `EXPLAIN ...` |
| 메타데이터 조회 | `SHOW TABLES`, `DESCRIBE <table>` |
| 세션 조회 | `SHOW PROCESSLIST` |
| 클러스터 상태 조회 | `SHOW CLUSTER STATUS` |
| 읽기 전용 분석 | `FUNNEL_COUNT(...)`, `COHORT_ANALYSIS(...)` |
| Profiler 조회 | `SELECT * FROM information_schema.wowdb_query_history` |
| 통계 조회 | `SHOW STATS FOR <table>` |

---

## 6. 자동 복구

### 6.1 복구 조건

모든 모니터링 경로(data_dir, wal_dir)의 디스크 사용량이 **`disk_recovery_threshold` (기본 85%) 이하**로 내려가면 자동 복구한다.

복구가 가능한 경우:
- 관리자가 오래된 데이터를 직접 삭제
- TTL Compaction 이 완료되어 만료 데이터 삭제
- 외부 디스크 증설 또는 마운트 변경

### 6.2 복구 시퀀스

```
Storage Node (DiskMonitor)
  1. 사용량 < 85% 감지 (모든 모니터링 경로)
  2. gRPC: QN Leader에게 ReportDiskRecovered(node_id, path, usage) 전송

Query Node Leader
  3. Raft 로그에 ClearReadOnly 엔트리 기록 (해당 노드의 DiskFull reason 제거)
  4. 모든 reason이 제거되면 read_only_state = false
  5. Raft 커밋 → 전체 QN 상태 반영

QN MySQL Protocol Handler
  6. read_only_state = false 확인 후 쓰기 요청 정상 처리 재개

Prometheus
  7. wowdb_cluster_read_only{reason="disk_full"} = 0
```

### 6.3 부분 복구 시나리오

여러 노드에서 DiskFull이 동시에 발생한 경우, **모든 원인 노드**가 복구 임계값을 달성해야 전체 클러스터가 쓰기 모드로 복귀한다.

```
예: sn-01 디스크 96%, sn-02 디스크 98% → Read-Only 진입
  → sn-01 디스크 83% (복구 가능) → 아직 Read-Only (sn-02 원인 잔존)
  → sn-02 디스크 84% (복구 가능) → 비로소 정상 모드 복귀
```

---

## 7. Prometheus 메트릭 및 알림

### 7.1 노출 메트릭

```
# HELP wowdb_cluster_read_only 클러스터 Read-Only 모드 상태 (1=Read-Only, 0=정상)
# TYPE wowdb_cluster_read_only gauge
wowdb_cluster_read_only{reason="disk_full"} 1

# HELP wowdb_disk_usage_ratio 노드별 디스크 사용률 (0.0~1.0)
# TYPE wowdb_disk_usage_ratio gauge
wowdb_disk_usage_ratio{node="sn-01",path="/data/wowdb"} 0.973
wowdb_disk_usage_ratio{node="sn-01",path="/wal/wowdb"} 0.612
wowdb_disk_usage_ratio{node="qn-01",path="/raft/wal"}  0.431

# HELP wowdb_write_blocked_total Read-Only 모드로 인해 차단된 쓰기 요청 수 (누적)
# TYPE wowdb_write_blocked_total counter
wowdb_write_blocked_total{reason="disk_full",type="insert"} 4821
wowdb_write_blocked_total{reason="disk_full",type="ddl"}    12
wowdb_write_blocked_total{reason="disk_full",type="kafka"}  9403
```

### 7.2 권장 Prometheus 알림 규칙

```yaml
# prometheus/alerts/wowdb_disk.yml
groups:
  - name: wowdb_disk
    rules:

      # 경고: 디스크 80% 이상 — 조기 경보
      - alert: WowDbDiskHigh
        expr: wowdb_disk_usage_ratio > 0.80
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "WOW-DB 디스크 사용량 높음 ({{ $labels.node }})"
          description: "{{ $labels.path }} 사용률 {{ $value | humanizePercentage }}. Read-Only 임계값(95%)까지 여유 부족."

      # 위험: 클러스터 Read-Only 모드 진입
      - alert: WowDbClusterReadOnly
        expr: wowdb_cluster_read_only{reason="disk_full"} == 1
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "WOW-DB 클러스터 Read-Only 모드 진입"
          description: "디스크 사용량 초과로 쓰기 차단됨. 즉각 디스크 용량 확보 필요."

      # 위험: 다량의 쓰기 차단 발생
      - alert: WowDbWritesBlocked
        expr: rate(wowdb_write_blocked_total[5m]) > 100
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "WOW-DB 쓰기 요청 다량 차단 중"
          description: "분당 {{ $value }}건의 쓰기 요청이 Read-Only 모드로 차단되고 있음."
```

---

## 8. 관리자 명령

### 8.1 수동 Read-Only 진입 (Maintenance용)

관리자가 점검 목적으로 수동으로 Read-Only 모드를 활성화할 수 있다:

```sql
-- 수동 진입 (Raft를 통해 클러스터 전체 반영)
SET GLOBAL wowdb_read_only = ON;

-- 수동 해제 (디스크 DiskFull reason이 없는 경우만 유효)
SET GLOBAL wowdb_read_only = OFF;
```

수동 해제 시 DiskFull reason이 여전히 존재하면 오류를 반환한다:

```
ERROR 1290 (HY000): Cannot exit read-only mode: disk usage on node sn-01 is 
97.3% (threshold: 85%). Resolve disk space issue first.
```

### 8.2 현재 상태 조회

```sql
-- 클러스터 Read-Only 상태 조회
SHOW GLOBAL STATUS LIKE 'wowdb_read_only';
-- +------------------+-------+
-- | Variable_name    | Value |
-- +------------------+-------+
-- | wowdb_read_only  | ON    |
-- +------------------+-------+

-- 상세 원인 조회
SELECT * FROM information_schema.wowdb_cluster_status WHERE key = 'read_only_state';
-- 반환 예:
-- {
--   "enabled": true,
--   "reasons": [
--     {
--       "variant": "DiskFull",
--       "node_id": "sn-01",
--       "path": "/data/wowdb",
--       "usage": 0.973,
--       "timestamp": "2026-04-14T10:23:45Z"
--     }
--   ],
--   "entered_at": "2026-04-14T10:23:47Z"
-- }
```

### 8.3 긴급 강제 해제 (디스크 오류 무시)

디스크 교체 작업 등으로 강제 해제가 필요한 경우 `--force` 옵션 사용:

```sql
-- 경고: 디스크 DiskFull reason이 있어도 강제 해제 (위험)
SET GLOBAL wowdb_read_only = OFF FORCE;
```

이 명령은 감사 로그(`/var/log/wowdb/audit.log`)에 기록된다.

---

## 9. Rust 데이터 모델

### 9.1 핵심 타입 정의

```rust
// query-node/src/meta/cluster_guard.rs

use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;
use chrono::{DateTime, Utc};

/// 클러스터 Read-Only 모드의 원인
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum ReadOnlyReason {
    /// 디스크 용량 초과 (자동 감지)
    DiskFull {
        node_id: String,
        path:    String,
        usage:   f64,   // 0.0~1.0
    },
    /// 관리자 수동 설정
    ManualOverride {
        admin:   String,
        comment: Option<String>,
    },
}

/// Raft KV에 저장되는 클러스터 상태
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ClusterReadOnlyState {
    pub enabled:    bool,
    pub reasons:    Vec<ReadOnlyReason>,
    pub entered_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl ClusterReadOnlyState {
    pub fn add_reason(&mut self, reason: ReadOnlyReason) {
        if !self.reasons.contains(&reason) {
            if !self.enabled {
                self.enabled    = true;
                self.entered_at = Some(Utc::now());
            }
            self.reasons.push(reason);
            self.updated_at = Utc::now();
        }
    }

    pub fn remove_reason(&mut self, reason: &ReadOnlyReason) {
        self.reasons.retain(|r| r != reason);
        if self.reasons.is_empty() {
            self.enabled    = false;
            self.entered_at = None;
            self.updated_at = Utc::now();
        }
    }
}

/// 로컬 캐시 (QN 내 인메모리, Raft 커밋 시 갱신)
pub struct ClusterGuard {
    state: Arc<RwLock<ClusterReadOnlyState>>,
}

impl ClusterGuard {
    /// 쓰기 허용 여부 확인 (모든 쓰기 경로에서 호출)
    pub async fn check_write_allowed(&self) -> Result<(), ReadOnlyError> {
        let state = self.state.read().await;
        if state.enabled {
            Err(ReadOnlyError::ClusterReadOnly {
                reasons: state.reasons.clone(),
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadOnlyError {
    #[error("클러스터가 Read-Only 모드입니다 (원인: {reasons:?})")]
    ClusterReadOnly { reasons: Vec<ReadOnlyReason> },
}
```

### 9.2 디스크 용량 감시 타입

```rust
// storage-node/src/disk_monitor.rs

#[derive(Debug, Clone)]
pub struct DiskMonitorConfig {
    pub data_dir:                PathBuf,
    pub wal_dir:                 Option<PathBuf>, // None이면 data_dir와 동일
    pub disk_full_threshold:     f64,             // 기본 0.95
    pub disk_recovery_threshold: f64,             // 기본 0.85
    pub check_interval_ms:       u64,             // 기본 5_000
}

impl Default for DiskMonitorConfig {
    fn default() -> Self {
        Self {
            data_dir:                PathBuf::from("/data/wowdb"),
            wal_dir:                 None,
            disk_full_threshold:     0.95,
            disk_recovery_threshold: 0.85,
            check_interval_ms:       5_000,
        }
    }
}

/// 디스크 사용량 샘플 결과
#[derive(Debug, Clone)]
pub struct DiskUsageSample {
    pub path:        PathBuf,
    pub total_bytes: u64,
    pub used_bytes:  u64,
    pub usage_ratio: f64,   // used_bytes / total_bytes
    pub sampled_at:  std::time::Instant,
}
```

---

## 10. 구현 체크리스트

| 항목 | 담당 모듈 | 완료 조건 |
|---|---|---|
| `DiskMonitor` 백그라운드 태스크 | `storage-node/src/disk_monitor.rs` | 폴링 주기 내 사용량 측정 후 QN gRPC 호출 |
| `DiskMonitor` QN 전용 (Raft WAL 경로) | `query-node/src/disk_monitor.rs` | Raft WAL 경로 모니터링 |
| `ClusterGuard.check_write_allowed()` | `query-node/src/meta/cluster_guard.rs` | 모든 쓰기 경로(MySQL Handler, Ingestion GW 등)에서 호출 |
| `MetaService.ReportDiskFull` gRPC | `proto/meta.proto` + `query-node/src/raft/` | SN/QN → QN Leader 전파 RPC |
| `ClusterReadOnlyState` Raft KV 직렬화 | `query-node/src/meta/` | `/cluster/read_only_state` CRUD |
| MySQL 오류 코드 1290 반환 | `query-node/src/mysql_protocol/` | `ER_OPTION_PREVENTS_STATEMENT` 포맷 |
| Prometheus 메트릭 3종 | `query-node/src/monitoring.rs` + `storage-node/src/monitoring.rs` | Grafana 대시보드에서 확인 |
| `SET GLOBAL wowdb_read_only` DDL | `query-node/src/sql_parser/` | ON/OFF/OFF FORCE 처리 |
| 통합 테스트 | `storage-node/tests/disk_full_tests.rs` | 95% → Read-Only 진입, 85% → 복귀 시나리오 |

---

## 11. 관련 문서

- **FR-036** 요구사항 정의: `specs/002-wow-db-srs-v02/spec.md` § 클러스터 보호 요구사항
- **LSM Engine 설계**: `specs/002-wow-db-srs-v02/design/lsm-engine.md`
- **Raft 메타데이터 키 목록**: `specs/002-wow-db-srs-v02/data-model.md` § 4. 분산 메타데이터
- **구현 태스크**: `specs/002-wow-db-srs-v02/tasks.md` Phase 12 (T118~T125)
