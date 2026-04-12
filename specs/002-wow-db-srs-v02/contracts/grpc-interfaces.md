# gRPC 인터페이스 계약: WOW-DB 노드 간 통신

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-12

모든 노드 간 통신은 `tonic` + `prost` 기반 gRPC(HTTP/2)를 사용한다.  
Proto 파일 위치: `proto/` (workspace root)

---

## 1. QN → CN: 실행 계획 전달 (포트 9040)

```protobuf
// proto/compute.proto

service ComputeService {
    // QN이 CN에게 Fragment 실행 계획 전달
    rpc ExecuteFragment(FragmentRequest) returns (stream FragmentResult);
    
    // 쿼리 취소
    rpc CancelFragment(CancelRequest) returns (CancelResponse);
    
    // 헬스체크
    rpc Health(HealthRequest) returns (HealthResponse);
}

message FragmentRequest {
    string query_id     = 1;
    string fragment_id  = 2;
    bytes  plan         = 3;  // 직렬화된 PhysicalPlan (prost)
    map<string, string> options = 4;
}

message FragmentResult {
    string query_id    = 1;
    string fragment_id = 2;
    bytes  batch       = 3;  // Arrow IPC 직렬화 결果 배치
    bool   is_last     = 4;
    StageMetrics metrics = 5;
}

message StageMetrics {
    uint64 rows_returned = 1;
    uint64 bytes_read    = 2;
    uint64 duration_ms   = 3;
    map<string, uint64> stage_times = 4;  // stage_name → ms
}
```

---

## 2. CN → SN: Tablet 읽기/쓰기 (포트 9060)

```protobuf
// proto/storage.proto

service StorageService {
    // 컬럼 청크 읽기 (Tablet 스캔)
    rpc ScanTablet(ScanRequest) returns (stream ScanBatch);
    
    // 데이터 쓰기 (WAL + MemTable)
    rpc WriteRows(WriteRequest) returns (WriteResponse);
    
    // 트랜잭션 Prepare (2PC)
    rpc Prepare(PrepareRequest) returns (PrepareResponse);
    
    // 트랜잭션 Commit
    rpc Commit(CommitRequest) returns (CommitResponse);
    
    // 트랜잭션 Rollback
    rpc Rollback(RollbackRequest) returns (RollbackResponse);
    
    // Tablet 메타데이터 조회
    rpc GetTabletMeta(TabletMetaRequest) returns (TabletMetaResponse);
    
    // 헬스체크
    rpc Health(HealthRequest) returns (HealthResponse);
}

message ScanRequest {
    string tablet_id             = 1;
    repeated string columns      = 2;  // 투영할 컬럼 목록 (빈 배열 = 전체)
    bytes  predicate             = 3;  // 직렬화된 Predicate (선택적)
    RuntimeFilter runtime_filter = 4;  // 동적 런타임 필터
    uint32 batch_size            = 5;  // 기본 4096행
}

message ScanBatch {
    string tablet_id = 1;
    bytes  batch     = 2;  // Arrow IPC 포맷 컬럼 배치
    bool   is_last   = 3;
    uint64 rows_read = 4;
    uint64 granules_skipped = 5;  // Data Skipping 효과 측정
}

message WriteRequest {
    string tx_id      = 1;
    string tablet_id  = 2;
    bytes  batch      = 3;  // Arrow IPC 포맷 행 배치
}

message RuntimeFilter {
    string filter_type        = 1;  // "bloom" | "in_list" | "min_max"
    bytes  bloom_bytes        = 2;
    repeated bytes in_values  = 3;
    bytes  min_val            = 4;
    bytes  max_val            = 5;
    string target_column      = 6;
}
```

---

## 3. QN 내부: Raft 메타데이터 통신 (포트 9010)

```protobuf
// proto/raft.proto
// openraft 크레이트가 내부적으로 처리 — 직접 구현 불필요
// 하지만 애플리케이션 레벨 메타데이터 RPC는 별도 정의

service MetaService {
    // Cube DDL 처리
    rpc CreateCube(CreateCubeRequest)   returns (DdlResponse);
    rpc AlterCube(AlterCubeRequest)     returns (DdlResponse);
    rpc DropCube(DropCubeRequest)       returns (DdlResponse);
    
    // Cube 조회
    rpc GetCubeSchema(GetCubeRequest)   returns (CubeSchemaResponse);
    rpc ListCubes(ListCubesRequest)     returns (ListCubesResponse);
    
    // SMV 관리
    rpc CreateSmv(CreateSmvRequest)     returns (DdlResponse);
    rpc DropSmv(DropSmvRequest)         returns (DdlResponse);
    rpc GetSmv(GetSmvRequest)           returns (SmvResponse);
    
    // 통계
    rpc UpdateStats(UpdateStatsRequest) returns (StatsResponse);
    rpc GetStats(GetStatsRequest)       returns (StatsResponse);
    
    // Tablet 할당
    rpc AllocateTablets(AllocateRequest) returns (AllocateResponse);
    rpc GetTabletMap(TabletMapRequest)   returns (TabletMapResponse);
}
```

---

## 4. QN → 외부: Kafka Ingestion Gateway (내부)

```protobuf
// proto/ingestion.proto

service IngestionService {
    // Routine Load 잡 관리
    rpc CreateRoutineLoad(CreateRLRequest)  returns (RLResponse);
    rpc PauseRoutineLoad(PauseRLRequest)    returns (RLResponse);
    rpc ResumeRoutineLoad(ResumeRLRequest)  returns (RLResponse);
    rpc DropRoutineLoad(DropRLRequest)      returns (RLResponse);
    rpc GetRoutineLoadStatus(GetRLRequest)  returns (RLStatusResponse);
    
    // Stream Load (Spark HTTP 수집 — HTTP REST 별도)
    rpc BeginStreamLoad(BeginSLRequest)    returns (BeginSLResponse);
    rpc CommitStreamLoad(CommitSLRequest)  returns (CommitSLResponse);
    rpc AbortStreamLoad(AbortSLRequest)    returns (AbortSLResponse);
}

message RLStatusResponse {
    string job_id        = 1;
    string state         = 2;  // RUNNING | PAUSED | ERROR | CANCELLED
    uint64 consumed_rows = 3;
    uint64 error_rows    = 4;
    string last_error    = 5;
    int64  current_lag   = 6;  // Kafka 토픽 lag (행 수)
    repeated PartitionOffset offsets = 7;
}
```

---

## 5. 노드 공통 헬스체크

```protobuf
// proto/health.proto

service HealthService {
    rpc Check(HealthRequest) returns (HealthResponse);
}

message HealthRequest {
    string service = 1;  // "query-node" | "compute-node" | "storage-node"
}

message HealthResponse {
    enum Status {
        UNKNOWN  = 0;
        SERVING  = 1;
        NOT_SERVING = 2;
    }
    Status status      = 1;
    string version     = 2;
    string node_id     = 3;
    map<string, string> details = 4;  // 추가 상태 정보
}
```

---

## 포트 요약

| 노드 | 포트 | 프로토콜 | 용도 |
|---|---|---|---|
| Query Node | 9030 | TCP (MySQL Wire) | MySQL 클라이언트 연결 |
| Query Node | 8080 | HTTP/WebSocket | Web SQL Client |
| Query Node | 9010 | gRPC | Raft 합의 (QN 간) |
| Query Node | 9011 | gRPC | QN ↔ CN/SN 내부 메타 |
| Compute Node | 9040 | gRPC | Fragment 실행 계획 수신 |
| Storage Node | 9060 | gRPC | 스토리지 API |
| Storage Node | 8040 | HTTP | Spark Stream Load |
