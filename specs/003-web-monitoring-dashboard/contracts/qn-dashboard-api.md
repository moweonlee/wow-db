# API Contract: QN Dashboard Endpoints

**Service**: Query Node Web UI (port 8080)  
**Date**: 2026-04-17

---

## 신규 엔드포인트

### `GET /` 또는 `GET /dashboard`

**Description**: HTML 모니터링 대시보드 메인 페이지.  
**Response**: `text/html; charset=utf-8`  
**Layout**: 3개 탭 — Cluster / Storage / LSM Status  
**Auto-refresh**: JavaScript `setInterval` 5초마다 각 API 호출  

**동작**:
- 페이지 로드 시 `#cluster-tab` 기본 활성화
- 각 탭은 해당 API (`/api/v1/cluster`, `/api/cubes`, `/api/v1/lsm`)를 비동기 fetch
- 오류 발생 시 해당 섹션에 에러 메시지 표시 (나머지 탭 정상 동작 유지)

---

### `GET /api/v1/lsm`

**Description**: 클러스터 내 모든 SN의 LSM Compaction 상태 집계.

**Request**: 없음

**Response** (`application/json`):

```json
{
  "nodes": [
    {
      "node_id": "sn-local-1",
      "sn_endpoint": "127.0.0.1:8040",
      "partitions": [
        {
          "partition_name": "p_2024_01",
          "cube_name": "events",
          "l0_file_count": 3,
          "total_levels": 3,
          "level_sizes": [31457280, 268435456, 0],
          "compaction_score": 0.75,
          "compaction_status": "Idle",
          "last_compaction_ms": 1776352000000
        }
      ],
      "total_l0_files": 3,
      "compaction_running": false,
      "write_control": "Normal",
      "updated_at_ms": 1776352574620
    }
  ],
  "total_l0_files": 3,
  "nodes_with_slowdown": 0,
  "nodes_with_stop": 0,
  "fetched_at_ms": 1776352574620
}
```

**오류 처리**:
- SN 접속 불가 시 해당 node의 `write_control` = `"Unknown"`, 파티션 목록 빈 배열
- QN이 SN 주소를 모를 경우 빈 nodes 배열 반환 (HTTP 200)

**Status codes**:
- `200 OK`: 정상 응답 (SN 일부 실패 포함)
- `503 Service Unavailable`: QN이 Raft quorum 없는 상태 (메타데이터 읽기 불가)

---

### `POST /api/v1/nodes/register`

**Description**: 노드 자가 등록 엔드포인트 (StarRocks FE 패턴). QN/CN/SN이 기동 시 QN에 자신을 등록하고, 15초마다 heartbeat으로 재등록한다. QN끼리도 서로에게 이 엔드포인트를 호출하여 멤버십을 공유한다.

**Request** (`application/json`):

```json
{
  "node_id": "qn-local-2",
  "node_type": "query",
  "mysql_addr": "127.0.0.1:19031",
  "http_addr": "127.0.0.1:18081",
  "role": "Follower"
}
```

| 필드 | 타입 | 필수 | 설명 |
|------|------|------|------|
| `node_id` | string | Y | 노드 식별자 (NODE_ID 환경변수) |
| `node_type` | string | Y | `"query"` / `"compute"` / `"storage"` |
| `mysql_addr` | string | N | MySQL 프로토콜 주소 (QN만 해당) |
| `http_addr` | string | N | HTTP 웹 포트 주소 |
| `role` | string | N | QN: `"Leader"` / `"Follower"`, CN/SN: 생략 |

**Response** (`application/json`):

```json
{ "status": "registered" }
```

**동작**:
- `nodes` HashMap에 `"{node_type}:{node_id}"` 키로 `NodeStatus` 저장
- 이미 존재하는 키 → `last_seen` 갱신 (heartbeat 처리)
- 동일 node_id가 다른 node_type으로 등록 시 두 항목 모두 유지 (다른 키)

**QN 피어 등록 흐름**:
1. QN 기동 → `QN_HTTP_PEERS` 환경변수 파싱
2. 자신의 HTTP 주소 제외한 나머지 QN 피어에 `POST /api/v1/nodes/register` 전송
3. 15초마다 tokio task에서 반복 (heartbeat)
4. QN_HTTP_PEERS 첫 번째 주소 = 자신 → `role: "Leader"`, 아니면 `"Follower"`

**Status codes**:
- `200 OK`: 등록 성공
- `400 Bad Request`: `node_id` 또는 `node_type` 누락

---

## 기존 재사용 엔드포인트 (변경 없음)

### `GET /api/v1/cluster`

클러스터 노드 목록 + 상태. Cluster 탭에서 직접 사용.

**Key fields used by dashboard**:
- `query_nodes[].id`, `query_nodes[].address`, `query_nodes[].alive`
- `compute_nodes[].id`, `compute_nodes[].address`, `compute_nodes[].alive`
- `data_nodes[].id`, `data_nodes[].address`, `data_nodes[].alive`

### `GET /api/cubes`

테이블(Cube) 목록. Storage 탭에서 사용.

**Key fields used by dashboard**:
- `[].name`, `[].columns[].name`, `[].columns[].data_type`
- `[].distribution.bucket_count`, `[].distribution.column`

### `GET /metrics`

Prometheus 형식 메트릭. Storage 탭 요약 숫자에 활용.

**Key metrics used by dashboard**:
- `wowdb_queries_total`
- `wowdb_data_nodes_online`
- `wowdb_compute_nodes_online`
- `wowdb_compaction_running`
