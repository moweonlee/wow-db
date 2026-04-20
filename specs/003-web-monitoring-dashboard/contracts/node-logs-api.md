# API Contract: CN/SN Node Logs Endpoints

**Services**: Compute Node (port 10040) / Storage Node (port 8040)  
**Date**: 2026-04-17

---

## Compute Node

### `GET /logs`

**Description**: 최근 처리된 Fragment 실행 로그 (최대 100개, 최신순).

**Request**: 없음

**Response** (`application/json`):

```json
{
  "node_id": "cn-local-1",
  "role": "compute",
  "entries": [
    {
      "timestamp_ms": 1776352574000,
      "level": "INFO",
      "target": "compute_node::grpc::server",
      "message": "ExecuteFragment — QN→CN→SN",
      "fields": {
        "query_id": "f1bf02d7-87a6-4363-b5f7-ea9bea829fe7",
        "fragment_id": "c3eab03b-f092-4bf4-a6ee-34b5b46b253b"
      }
    },
    {
      "timestamp_ms": 1776352574003,
      "level": "INFO",
      "target": "compute_node::grpc::server",
      "message": "ExecuteFragment: SN scan OK, sending results",
      "fields": {
        "rows": "3",
        "query_id": "f1bf02d7-87a6-4363-b5f7-ea9bea829fe7"
      }
    }
  ],
  "total_buffered": 2
}
```

**Status codes**:
- `200 OK`: 정상 응답 (entries 빈 배열 가능)

---

## Storage Node

### `GET /logs`

**Description**: 최근 WAL 기록, Compaction, Write/Read 요청 로그 (최대 100개, 최신순).

**Request**: 없음

**Response** (`application/json`):

```json
{
  "node_id": "sn-local-1",
  "role": "storage",
  "entries": [
    {
      "timestamp_ms": 1776352574000,
      "level": "INFO",
      "target": "storage_node::grpc::server",
      "message": "WriteRows OK — WAL + MemTable",
      "fields": {
        "tablet_id": "events",
        "rows": "3"
      }
    },
    {
      "timestamp_ms": 1776352573000,
      "level": "INFO",
      "target": "storage_node::lsm::wal",
      "message": "WAL replay 완료 — MemTable 복구",
      "fields": {
        "tablet_id": "events",
        "rows": "5"
      }
    }
  ],
  "total_buffered": 2
}
```

### `GET /api/v1/lsm-status`

**Description**: 이 SN의 LSM Compaction 상태 (QN이 폴링).

**Response** (`application/json`):

```json
{
  "node_id": "sn-local-1",
  "sn_endpoint": "127.0.0.1:8040",
  "partitions": [
    {
      "partition_name": "p_2024_01",
      "cube_name": "events",
      "l0_file_count": 2,
      "total_levels": 2,
      "level_sizes": [20971520, 268435456],
      "compaction_score": 0.5,
      "compaction_status": "Idle",
      "last_compaction_ms": null
    }
  ],
  "total_l0_files": 2,
  "compaction_running": false,
  "write_control": "Normal",
  "updated_at_ms": 1776352574620
}
```

**Notes**:
- 파티션 정보는 `TabletWriterRegistry`에 등록된 TabletWriter에서 조회
- 실시간 MemTable 상태 포함 (flush 대기 중인 데이터 크기)

---

## 공통 규칙

- 모든 응답은 `Content-Type: application/json; charset=utf-8`
- 로그 레벨 필터링: 쿼리 파라미터 `?level=WARN` 으로 특정 레벨 이상만 반환 가능 (기본: 전체)
- `timestamp_ms` 기준 내림차순 정렬 (최신이 첫 번째)
- 인증/인가: 없음 (내부 운영 도구)
