# Data Model: Web Monitoring Dashboard

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-17

---

## 1. 신규 엔티티

### 1.1 `LsmNodeStatus` — SN의 LSM Compaction 상태

각 SN이 `/api/v1/lsm-status` 엔드포인트로 반환하는 응답.

```
LsmNodeStatus {
    node_id:          String        // SN 식별자 (예: "sn-local-1")
    sn_endpoint:      String        // HTTP 주소 (예: "127.0.0.1:8040")
    partitions:       Vec<PartitionLsmStatus>
    total_l0_files:   u32           // 전체 파티션 합산 L0 파일 수
    compaction_running: bool        // 현재 Compaction 실행 중 여부
    write_control:    WriteControl  // Normal | Slowdown | Stop
    updated_at_ms:    u64           // Unix 밀리초
}
```

```
PartitionLsmStatus {
    partition_name:     String        // 파티션 이름 (예: "p_2024_01")
    cube_name:          String        // 소속 테이블 이름
    l0_file_count:      u32
    total_levels:       u32           // 사용 중인 최대 레벨 번호
    level_sizes:        Vec<u64>      // 레벨별 바이트 크기 [L0, L1, ..., LN]
    compaction_score:   f64           // 1.0 이상이면 compaction 필요
    compaction_status:  CompactionStatus  // Idle | Running | Pending
    last_compaction_ms: Option<u64>   // 마지막 Compaction 완료 시각
}
```

```
CompactionStatus = Idle | Running | Pending
WriteControl     = Normal | Slowdown | Stop
```

**Validation rules**:
- `l0_file_count` ≥ 0
- `compaction_score` ≥ 0.0
- `level_sizes.len()` == `total_levels` + 1

---

### 1.2 `ClusterLsmOverview` — QN이 집계한 전체 LSM 현황

QN의 `/api/v1/lsm` 엔드포인트 응답.

```
ClusterLsmOverview {
    nodes:              Vec<LsmNodeStatus>
    total_l0_files:     u32           // 클러스터 전체 L0 파일 합계
    nodes_with_slowdown: u32          // WriteControl = Slowdown인 노드 수
    nodes_with_stop:    u32           // WriteControl = Stop인 노드 수
    fetched_at_ms:      u64
}
```

---

### 1.3 `LogEntry` — CN/SN 인메모리 로그 항목

각 노드의 `/logs` 엔드포인트 응답에 포함되는 항목.

```
LogEntry {
    timestamp_ms:  u64         // Unix 밀리초
    level:         LogLevel    // INFO | WARN | ERROR | DEBUG
    target:        String      // tracing target (예: "compute_node::grpc::server")
    message:       String      // 로그 메시지 (최대 2,000자)
    fields:        HashMap<String, String>  // 구조화 필드 (key=value)
}
```

```
LogLevel = DEBUG | INFO | WARN | ERROR
```

```
LogsResponse {
    node_id:   String
    role:      String           // "compute" | "storage"
    entries:   Vec<LogEntry>    // 최신 순 정렬, 최대 100개
    total_buffered: u32         // 버퍼에 보관 중인 전체 수 (max 100)
}
```

---

## 2. 기존 엔티티 (대시보드가 읽기 전용으로 사용)

### 2.1 `ClusterOverview` (기존 — `monitoring.rs`)

`/api/v1/cluster` 응답. 변경 없이 재사용.

```
ClusterOverview {
    timestamp_ms:      u64
    query_nodes:       Vec<NodeStatus>
    compute_nodes:     Vec<NodeStatus>
    data_nodes:        Vec<NodeStatus>
    raft_leader_id:    String
    raft_term:         u64
    total_tablets:     u64
    total_sstables:    u64
    queries_in_flight: u64
    queries_total:     u64
}

NodeStatus {
    id:          String
    address:     String
    role:        String      // "Leader" | "Follower" | "Worker" | "Storage"
    alive:       bool
    cpu_pct:     f64
    memory_mb:   f64
    last_seen_ms: u64
}
```

### 2.2 `CubeSchema` (기존 — `meta/cube.rs`)

`/api/cubes` 응답. 변경 없이 재사용.

```
CubeSchema {
    cube_id:      Uuid
    name:         String
    columns:      Vec<ColumnDef>
    distribution: Distribution      // bucket_count, column
    partition:    PartitionKey
    sort_key:     Vec<String>
    // ... 기타 필드
}
```

---

## 3. 상태 전이

### 3.1 Compaction 상태 전이

```
         새 L0 파일 추가
Idle ─────────────────► Pending
                           │
             Compaction    │
             워커 시작      ▼
                        Running
                           │
             완료 또는      │
             오류           ▼
                         Idle
```

### 3.2 노드 ALIVE 상태

```
        heartbeat 응답 OK     heartbeat 타임아웃
JOIN ──────────────────► alive:true ──────────► alive:false (OFFLINE 표시)
                             │
                 heartbeat   │
                 재개         ▼
                         alive:true
```

---

## 4. 인메모리 로그 버퍼 구조

```rust
// 각 노드 프로세스 내 전역 싱글톤
pub struct LogBuffer {
    entries:  VecDeque<LogEntry>,  // 최대 100개
    capacity: usize,               // 기본값: 100
}
```

버퍼가 가득 차면 가장 오래된 항목을 제거하고 신규 항목을 추가한다 (FIFO 원형 덮어쓰기).
