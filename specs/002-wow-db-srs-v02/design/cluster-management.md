# WOW-DB 클러스터 관리 설계 (Cluster Management Design)

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-15  
**상태**: 명세 (구현 예정)

---

## 1. 개요

WOW-DB 클러스터는 **QN이 단일 진입점**이다. 모든 노드(QN, CN, SN)는 기동 시 QN의 주소와 포트를 통해 클러스터에 참가하며, QN의 Raft 메타스토어가 전체 클러스터 토폴로지를 권위 있게(authoritative) 관리한다.

### 1.1 설계 원칙

- **QN-Centric Discovery**: 모든 노드는 QN 주소:포트를 유일한 부트스트랩 엔드포인트로 사용한다. SN이 직접 CN에 등록하는 경로는 없다.
- **Raft-Backed Topology**: 클러스터 멤버십 변경(JOIN/DRAIN/DISMISS)은 Raft 쓰기를 통해 커밋되므로, 모든 QN이 동일한 토폴로지 뷰를 갖는다.
- **Background Rebalance**: 노드 추가/제거 시 Shard 재분배는 쿼리 경로에 영향을 주지 않는 백그라운드 프로세스로 실행된다.
- **Graceful Drain**: 데이터가 있는 노드는 반드시 DRAIN 완료 후 종료되어야 한다. 즉시 종료는 `FORCE` 플래그로만 가능하며, 이 경우 Shard 메타데이터가 삭제된다.
- **K8s-Native**: ConfigMap에 QN 피어 목록을 저장하고, Helm으로 모든 노드 타입의 수평 확장을 제어한다.

---

## 2. 노드 상태 모델 (Node State Model)

모든 노드 타입(QN, CN, SN)은 동일한 4-상태 라이프사이클을 따른다. DISMISSED는 노드가 클러스터에서 제거된 상태로, Raft 메타데이터에 항목이 존재하지 않는다.

```
           자동
  [시작]  ──────►  ACTIVE  ◄─────  자동 복구
                     │
              ALTER CLUSTER DRAIN
                     │
                     ▼
                 READONLY   ← INSERT 라우팅 제외 즉시 적용
                     │
              (데이터 마이그레이션 시작 — SN)
              (인플라이트 쿼리 완료 대기 — CN)
              (Raft 탈퇴 준비 — QN)
                     │
                     ▼
                 DRAINING   ← 백그라운드 데이터 이전 중
                     │
           마이그레이션 완료 / 쿼리 완료
                     │
              ALTER CLUSTER DISMISS
                     │
                     ▼
               [DISMISSED]  ← Raft 메타데이터에서 제거 (= 존재하지 않음)
```

### 2.1 상태별 허용 작업

| 상태 | INSERT 라우팅 | SELECT 서빙 | Compaction | 새 Shard 수신 |
|------|--------------|-------------|------------|--------------|
| ACTIVE | ✅ | ✅ | ✅ | ✅ |
| READONLY | ❌ | ✅ | ✅ (진행 중인 것 완료) | ❌ |
| DRAINING | ❌ | ✅ (마이그레이션 중인 Shard는 원본에서 서빙) | ❌ | ❌ |
| DISMISSED | N/A | N/A | N/A | N/A |

### 2.2 노드 타입별 DRAIN 동작 차이

**SN (Storage Node) DRAIN**:
1. 상태 → READONLY (INSERT 라우팅에서 즉시 제외)
2. 상태 → DRAINING (백그라운드 Shard 마이그레이션 시작)
3. QN이 `RebalancePlanner`를 통해 해당 SN의 모든 Shard를 나머지 ACTIVE SN에 분배
4. 각 Shard는 순서대로 복제 후 메타데이터 업데이트 (Cut-Over)
5. 모든 Shard 이전 완료 → DISMISS 허용

**CN (Compute Node) DRAIN**:
1. 상태 → READONLY (새 쿼리 Fragment 배분 제외)
2. 상태 → DRAINING (실행 중인 Fragment 완료 대기)
3. 인플라이트 쿼리가 모두 완료되면 → DISMISS 허용

**QN (Query Node) DRAIN**:
1. 상태 → READONLY (Raft Leader라면 Leadership 양도)
2. 상태 → DRAINING (Raft 클러스터에서 graceful 탈퇴)
3. Raft quorum이 유지되는 경우에만 진행 가능 (3노드에서 1개 DRAIN 가능, 2개 동시 DRAIN 불가)
4. Raft 탈퇴 완료 → DISMISS 허용

---

## 3. 동적 노드 등록 (Dynamic Node Discovery)

### 3.1 노드 자가 등록 프로세스

모든 노드는 기동 시 환경변수 또는 설정 파일에서 QN 엔드포인트를 읽고 JOIN 요청을 전송한다.

```
노드 기동 순서:
1. 환경변수 QN_PEERS (또는 ConfigMap/설정 파일) 에서 QN 주소 읽기
2. QN 주소로 gRPC RegisterNode 요청 전송
3. QN이 Raft 쓰기로 노드 등록 커밋
4. QN이 신규 SN에 대해 RebalancePlanner 트리거 (SN인 경우)
5. 응답으로 NodeId, 초기 Shard 목록 수신
6. 노드 상태 → ACTIVE
```

```protobuf
// proto/cluster.proto
service ClusterService {
  // 노드 자가 등록 (기동 시 자동 호출)
  rpc RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse);
  // 노드 상태 갱신
  rpc UpdateNodeState(UpdateNodeStateRequest) returns (UpdateNodeStateResponse);
  // 클러스터 토폴로지 조회
  rpc GetClusterTopology(GetClusterTopologyRequest) returns (GetClusterTopologyResponse);
  // Rebalance 진행 상황 조회
  rpc GetRebalanceStatus(GetRebalanceStatusRequest) returns (GetRebalanceStatusResponse);
}

message RegisterNodeRequest {
  string node_type = 1;    // "QN" | "CN" | "SN"
  string node_id   = 2;    // 설정에 지정된 노드 ID (예: "sn-4")
  string address   = 3;    // 노드의 gRPC 리스닝 주소
  uint32 port      = 4;    // 포트
  map<string, string> meta = 5;  // 추가 메타데이터 (데이터 디렉토리, 디스크 용량 등)
}
```

### 3.2 Raft KV 토폴로지 스키마

```
Raft KV 키 구조:
/cluster/nodes/{node_id}         → NodeInfo (JSON: type, address, port, state, joined_at)
/cluster/nodes/{node_id}/state   → NodeState ("ACTIVE" | "READONLY" | "DRAINING")
/cluster/rebalance/jobs/{job_id} → RebalanceJob (JSON: plan, progress, started_at)
/cluster/shards/{shard_id}/location → ShardLocation (primary_sn, replica_sns)
```

---

## 4. SQL 클러스터 관리 명령 (Cluster Management Commands)

### 4.1 ALTER CLUSTER JOIN

노드를 클러스터에 수동으로 추가한다. 일반적으로 Helm/K8s init container가 자동 실행하지만, 운영자가 수동으로 실행할 수도 있다.

```sql
-- 문법
ALTER CLUSTER JOIN <node_type> <address>:<port> [AS '<node_id>'];

-- 예시
ALTER CLUSTER JOIN QN '10.0.0.5:9010';
ALTER CLUSTER JOIN QN '10.0.0.5:9010' AS 'qn-4';
ALTER CLUSTER JOIN CN '10.0.0.6:9040';
ALTER CLUSTER JOIN CN '10.0.0.6:9040' AS 'cn-3';
ALTER CLUSTER JOIN SN '10.0.0.7:9060';
ALTER CLUSTER JOIN SN '10.0.0.7:9060' AS 'sn-4';
```

**동작**:
- `AS` 절이 없으면 QN이 자동으로 NodeId를 생성 (`{type}-{sequence}`)
- SN 추가 시 백그라운드 Rebalance 자동 시작
- QN 추가 시 Raft 클러스터에 learner → voter 순서로 합류 (openraft 프로토콜)
- 결과: `Query OK, 1 node added. Rebalance started (job_id: <uuid>).`

**제약**:
- QN은 홀수 개를 유지해야 한다. 짝수가 되는 JOIN은 경고와 함께 진행 (다음 QN 추가 전까지 quorum 위험)
- 동일 address:port가 이미 등록된 경우 오류: `ERROR: Node '10.0.0.7:9060' is already registered as 'sn-3'`

### 4.2 ALTER CLUSTER DRAIN

노드를 READONLY → DRAINING 상태로 전환하고 데이터 마이그레이션을 시작한다.

```sql
-- 문법
ALTER CLUSTER DRAIN '<node_id>';

-- 예시
ALTER CLUSTER DRAIN 'sn-4';
ALTER CLUSTER DRAIN 'cn-2';
ALTER CLUSTER DRAIN 'qn-3';
```

**동작**:
1. 즉시: 대상 노드를 READONLY 상태로 전환. INSERT 라우팅에서 제외.
2. 비동기: 백그라운드 Drain 프로세스 시작 (SN: Shard 이전, CN: 인플라이트 완료, QN: Raft 탈퇴)
3. 진행 상황은 `SHOW CLUSTER REBALANCE` 로 모니터링 가능
4. DRAIN 완료 알림은 Raft KV 상태 변경으로 확인

**결과**:
```
Query OK. Node 'sn-4' is now DRAINING.
Tablets to migrate: 16 | Estimated time: ~2m 30s
Monitor with: SHOW CLUSTER REBALANCE;
```

**제약**:
- 이미 DRAINING 상태인 노드에 재실행하면 현재 진행 상황 표시
- QN DRAIN 시 남은 QN 수 < 3이 되면 오류: `ERROR: Cannot drain 'qn-1': Raft quorum would be lost (2 nodes remaining)`
- ACTIVE 상태가 아닌 노드에는 실행 불가

### 4.3 ALTER CLUSTER DISMISS

DRAIN 완료된 노드를 클러스터에서 영구 제거한다.

```sql
-- 문법
ALTER CLUSTER DISMISS '<node_id>' [FORCE];

-- 예시
ALTER CLUSTER DISMISS 'sn-4';         -- DRAIN 완료 후 정상 제거
ALTER CLUSTER DISMISS 'sn-4' FORCE;  -- 데이터 강제 삭제 후 제거
ALTER CLUSTER DISMISS 'cn-2';
ALTER CLUSTER DISMISS 'qn-3';
```

**정상 DISMISS** (DRAIN 완료 후):
- 노드에 데이터가 없으므로 즉시 Raft 메타데이터 삭제
- `DELETE /cluster/nodes/{node_id}` 및 관련 모든 Raft KV 항목 제거

**FORCE DISMISS** (데이터가 있는 노드):
- `FORCE` 없이 데이터가 있는 노드를 DISMISS하면 오류:
  ```
  ERROR 3001 (WW000): Node 'sn-4' has 16 shards with data.
  Run 'ALTER CLUSTER DRAIN sn-4' first, or use 'ALTER CLUSTER DISMISS sn-4 FORCE' 
  to permanently delete all data on this node.
  ```
- `FORCE` 사용 시:
  1. 해당 노드에 있는 모든 Shard의 Raft 메타데이터 삭제 (`/cluster/shards/{shard_id}/*`)
  2. CBO 통계에서 해당 Shard 항목 삭제
  3. Shard-Partition-Table 매핑에서 해당 Shard 제거
  4. 노드 메타데이터 삭제
  5. **경고**: FORCE DISMISS는 데이터 손실을 초래한다. 복제본이 다른 SN에 없는 경우 해당 데이터는 영구 삭제된다.

### 4.4 SHOW CLUSTER 명령

```sql
-- 전체 노드 목록 및 상태
SHOW CLUSTER NODES;

-- 출력 예시:
-- +---------+------+------------------+--------+-----------+-----------+
-- | node_id | type | address          | state  | shards    | joined_at |
-- +---------+------+------------------+--------+-----------+-----------+
-- | qn-1    | QN   | 10.0.0.1:9010    | ACTIVE | -         | 2026-04-01|
-- | qn-2    | QN   | 10.0.0.2:9010    | ACTIVE | -         | 2026-04-01|
-- | qn-3    | QN   | 10.0.0.3:9010    | ACTIVE | -         | 2026-04-01|
-- | cn-1    | CN   | 10.0.0.4:9040    | ACTIVE | -         | 2026-04-01|
-- | cn-2    | CN   | 10.0.0.5:9040    | ACTIVE | -         | 2026-04-01|
-- | sn-1    | SN   | 10.0.0.6:9060    | ACTIVE | 32        | 2026-04-01|
-- | sn-2    | SN   | 10.0.0.7:9060    | ACTIVE | 32        | 2026-04-01|
-- | sn-3    | SN   | 10.0.0.8:9060    | ACTIVE | 32        | 2026-04-01|
-- | sn-4    | SN   | 10.0.0.9:9060    | DRAINING| 8/16 →0  | 2026-04-14|
-- +---------+------+------------------+--------+-----------+-----------+

-- 현재 진행 중인 Rebalance 작업 조회
SHOW CLUSTER REBALANCE;

-- 출력 예시:
-- +--------------------------------------+-----------+----------+--------+-----------+-----------+
-- | job_id                               | type      | from_node| to_node| shards_done| eta_secs |
-- +--------------------------------------+-----------+----------+--------+-----------+-----------+
-- | a1b2c3d4-...                         | DRAIN     | sn-4     | sn-1,2 | 8/16      | 90        |
-- +--------------------------------------+-----------+----------+--------+-----------+-----------+

-- 클러스터 전체 상태 요약
SHOW CLUSTER STATUS;

-- 출력 예시:
-- +------------------+-------+
-- | metric           | value |
-- +------------------+-------+
-- | total_qn         | 3     |
-- | total_cn         | 2     |
-- | total_sn         | 4     |
-- | draining_nodes   | 1     |
-- | rebalance_active | true  |
-- | raft_leader      | qn-1  |
-- | cluster_mode     | READWRITE |
-- +------------------+-------+
```

---

## 5. Shard Rebalancing — 백그라운드 재분배

### 5.1 트리거 조건

Rebalance는 다음 이벤트에서 자동으로 시작된다:

| 이벤트 | Rebalance 유형 |
|--------|---------------|
| 새 SN JOIN | 신규 노드로 Shard 이전 (부하 분산) |
| SN DRAIN 시작 | 드레이닝 노드의 Shard를 나머지 SN으로 이전 |
| SN 예기치 않은 종료 (장애) | 복제 인자가 부족한 Shard 복제 복구 |
| 수동 `ALTER CLUSTER REBALANCE` | 노드 간 불균형 수동 재조정 |

### 5.2 Rebalance 알고리즘

```
RebalancePlanner (QN Leader 실행):

1. 현재 Shard 분포 스냅샷 수집
   - 각 ACTIVE SN의 Shard 수 및 크기 합계
   
2. 목표 분포 계산 (균등 분산)
   - target_shards_per_sn = total_shards / active_sn_count (올림)
   
3. 이전 계획 생성
   - 과부하 SN (shards > target) → source
   - 부족 SN (shards < target) → destination
   - Shard 선택: 가장 작은 Shard부터 (이전 시간 최소화)
   
4. RebalanceJob 생성 → Raft 쓰기 커밋
   - /cluster/rebalance/jobs/{job_id}

5. ShardMigrator 실행 (비동기):
   For each (shard, from_sn, to_sn) in plan:
     a. to_sn에 빈 Shard 생성
     b. from_sn → to_sn Shard 데이터 스트리밍 복제 (gRPC ShardCopyStream)
     c. 복제 완료 후 Raft KV에서 /cluster/shards/{shard_id}/location 업데이트
     d. from_sn의 해당 Shard 데이터 삭제 (Raft 쓰기 확인 후)
     e. 진행 상황 업데이트: /cluster/rebalance/jobs/{job_id}/progress
```

### 5.3 Rebalance 중 읽기/쓰기 동작

- **읽기**: 마이그레이션 진행 중인 Shard는 복제 완료 전까지 원본 SN에서 서빙
- **쓰기**: 마이그레이션 진행 중인 Shard는 READONLY (원본) → 복제 완료 후 대상 SN으로 쓰기 전환
- **데이터 일관성**: Raft KV의 location 업데이트가 쓰기 전환의 cut-over 시점

---

## 6. Kubernetes / Helm 통합

### 6.1 ConfigMap 기반 QN Discovery

모든 노드는 Helm 배포 시 `wowdb-config` ConfigMap에서 QN 피어 목록을 읽는다.

```yaml
# helm/templates/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: wowdb-config
data:
  qn.peers: "qn-0.qn-headless.{{ .Release.Namespace }}.svc.cluster.local:9010,\
             qn-1.qn-headless.{{ .Release.Namespace }}.svc.cluster.local:9010,\
             qn-2.qn-headless.{{ .Release.Namespace }}.svc.cluster.local:9010"
  sn.data_dir: "/data/wowdb"
  sn.replicas: "{{ .Values.storageNode.replicas }}"
  cn.replicas: "{{ .Values.computeNode.replicas }}"
  cluster.rebalance_enabled: "true"
  cluster.rebalance_concurrency: "2"   # 동시 Shard 이전 수
```

### 6.2 노드 타입별 K8s 리소스

| 노드 | K8s 리소스 | 이유 |
|------|-----------|------|
| QN | StatefulSet (홀수) | Raft: 안정적인 네트워크 ID (`qn-0`, `qn-1`, ...) 필요 |
| CN | Deployment | Stateless: HPA로 자유롭게 확장/축소 |
| SN | StatefulSet + PVC | 로컬 데이터: 재스케줄 시 동일 PVC 재연결 필요 |

### 6.3 수평 확장 시나리오

**SN 스케일 아웃** (Helm values.yaml 변경):
```yaml
# values.yaml
storageNode:
  replicas: 5  # 3 → 5로 증가
```
```bash
helm upgrade wowdb ./helm/wowdb -f values.yaml
```
→ Helm이 StatefulSet replicas를 5로 업데이트  
→ 새 SN Pod (`sn-3`, `sn-4`) 기동  
→ 각 Pod의 init container가 `ALTER CLUSTER JOIN SN` 자동 실행  
→ QN이 Rebalance 시작

**CN 스케일 아웃**:
```yaml
computeNode:
  replicas: 4  # 2 → 4로 증가
```
→ 새 CN Pod 기동 → QN에 자가 등록 → 즉시 쿼리 Fragment 라우팅에 포함

**QN 스케일 아웃** (반드시 홀수 유지):
```yaml
queryNode:
  replicas: 5  # 3 → 5로 증가 (반드시 홀수)
```
→ 새 QN Pod가 Raft learner로 합류 → 동기화 완료 후 voter 승격

### 6.4 Helm Values 참조

```yaml
# helm/wowdb/values.yaml (전체 클러스터 설정)

queryNode:
  replicas: 3                          # 반드시 홀수
  image: "wowdb/query-node:latest"
  resources:
    requests: { cpu: "2", memory: "4Gi" }
  service:
    mysqlPort: 9030
    webPort: 8080
    raftPort: 9010
    grpcPort: 9011

computeNode:
  replicas: 2
  image: "wowdb/compute-node:latest"
  autoscaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 16
    targetCPUUtilizationPercentage: 70

storageNode:
  replicas: 3
  image: "wowdb/storage-node:latest"
  storage:
    backend: "native"          # "native" | "s3" | "hdfs"
    size: "500Gi"
    storageClass: "fast-nvme"

cluster:
  rebalanceEnabled: true
  rebalanceConcurrency: 2      # 동시 Shard 이전 수
  rebalanceThresholdPercent: 20 # 노드 간 불균형이 20% 초과 시 자동 재조정
```

---

## 7. 오류 코드 정의

| 코드 | 이름 | 상황 |
|------|------|------|
| `3001` | `ER_NODE_HAS_DATA` | DISMISS 시 대상 노드에 데이터 존재, FORCE 없음 |
| `3002` | `ER_RAFT_QUORUM_LOSS` | QN DRAIN 시 quorum 손실 위험 |
| `3003` | `ER_NODE_ALREADY_EXISTS` | 동일 address:port가 이미 등록됨 |
| `3004` | `ER_NODE_NOT_FOUND` | 존재하지 않는 node_id 참조 |
| `3005` | `ER_INVALID_STATE_TRANSITION` | 허용되지 않는 상태 전환 (예: DRAINING → JOIN) |
| `3006` | `ER_REBALANCE_IN_PROGRESS` | 이미 Rebalance 진행 중, 중복 요청 |

---

## 8. 관련 요구사항 참조

- **FR-CM-001 ~ FR-CM-020**: `spec.md` 클러스터 관리 요구사항 섹션
- **Constitution Principle V (K8s-Native)**: 수평 확장, Stateless QN
- **Constitution Principle VII (Storage-Compute Separation)**: SN 독립 확장
- **SQL Syntax Reference**: `design/sql_syntax.md` — Section 12 클러스터 관리 명령
- **구현 Task**: `tasks-cluster-management.md`
