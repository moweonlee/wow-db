# WOW-DB 로컬 개발 환경 설계

**작성일**: 2026-04-15  
**참조**: NFR-DEV-001 (로컬 단일 인스턴스 기동), quickstart.md §2

---

## 1. 목적 및 범위

Docker 빌드/실행 시간(30~120초) 없이, **5초 이내**에 개발 환경을 시작할 수 있도록 로컬 Native 실행 스크립트와 설정을 제공한다.

- **대상**: 기능 개발, 단위 테스트, SQL 파서 검증, 쿼리 플래닝 디버깅
- **구성**: QN×1 + CN×1 + SN×1 (최소 단일 인스턴스)
- **비대상**: Kafka 수집, S3/HDFS 백엔드, 다중 노드 Raft, 복제본 3개 이상

---

## 2. 구성 요소

### 2.1 스크립트

| 파일 | 플랫폼 | 용도 |
|---|---|---|
| `scripts/dev/run-local.sh` | Linux/macOS | QN+CN+SN 순서대로 기동, Ctrl+C로 전체 종료 |
| `scripts/dev/run-local.ps1` | Windows | 동일 기능, PowerShell |
| `scripts/dev/stop-local.sh` | Linux/macOS | PID 파일 기반 종료, 포트 기반 fallback |
| `scripts/dev/stop-local.ps1` | Windows | 동일 기능, PowerShell |
| `scripts/dev/reset-local.sh` | Linux/macOS | 데이터 디렉토리(`/tmp/wowdb-dev`) 초기화 |
| `scripts/dev/reset-local.ps1` | Windows | 동일 기능, `$TEMP\wowdb-dev` 초기화 |

### 2.2 설정 파일

| 파일 | Docker 설정 대비 주요 차이 |
|---|---|
| `dev/configs/query-node-local.toml` | RAFT_PEERS=자기 자신(단일 노드 Raft), max_connections=100, rebalance=false |
| `dev/configs/compute-node-local.toml` | storage_nodes=["127.0.0.1:9060"], dop=2 |
| `dev/configs/storage-node-local.toml` | native backend, data_dir=/tmp/wowdb-dev/sn, replica_count=1, 소형 버퍼 |

---

## 3. 기동 순서 및 포트 대기

```
cargo build (dev profile)
  → Storage Node (127.0.0.1:9060 오픈 대기, 최대 10초)
    → Compute Node (127.0.0.1:9040 오픈 대기, 최대 10초)
      → Query Node (127.0.0.1:9030 오픈 대기, 최대 15초)
        → PID 파일 기록 (/tmp/wowdb-dev/wowdb.pids)
          → "READY" 출력
```

포트 오픈 확인은 TCP 연결 시도로 수행한다 (`/dev/tcp` on bash, `TcpClient` on PowerShell).

---

## 4. 포트 할당 (로컬 동일, Docker와 충돌 없음)

| 서비스 | 포트 | 용도 |
|---|---|---|
| QN MySQL | 9030 | `mysql -h 127.0.0.1 -P 9030` |
| QN Web UI | 8080 | `http://localhost:8080` |
| QN Raft | 9010 | 단일 노드 Raft 내부 |
| QN gRPC | 9011 | ClusterService (CN/SN 자가 등록) |
| CN gRPC | 9040 | Fragment 실행 |
| SN gRPC | 9060 | Tablet 읽기/쓰기 |
| SN HTTP | 8040 | Spark Stream Load |

로컬 실행과 `docker-compose.dev.yml` 실행은 동일 포트를 사용하므로, **둘을 동시에 실행하면 포트 충돌이 발생한다**. 동시 실행 금지.

---

## 5. 데이터 경로

| 플랫폼 | 기본 경로 | 환경변수 오버라이드 |
|---|---|---|
| Linux/macOS | `/tmp/wowdb-dev/` | `WOWDB_DATA_DIR=/path` |
| Windows | `%TEMP%\wowdb-dev\` | `-DataDir D:\path` |

```
/tmp/wowdb-dev/
├── sn/            ← SN 데이터 (SSTable, WAL, MANIFEST)
├── logs/
│   ├── sn.log
│   ├── cn.log
│   └── qn.log
└── wowdb.pids     ← 실행 중 PID 기록 (종료 시 삭제)
```

---

## 6. 환경변수

스크립트가 설정하는 환경변수:

| 변수 | SN | CN | QN | 의미 |
|---|---|---|---|---|
| `NODE_ID` | `sn-local-1` | `cn-local-1` | `qn-local-1` | 노드 식별자 |
| `DATA_DIR` | `/tmp/wowdb-dev/sn` | — | — | SN 데이터 경로 |
| `STORAGE_NODES` | — | `127.0.0.1:9060` | — | SN gRPC 주소 |
| `COMPUTE_NODES` | — | — | `127.0.0.1:9040` | CN gRPC 주소 |
| `RAFT_PEERS` | — | — | `qn-local-1:9010` | QN 단일 노드 Raft |
| `QN_PEERS` | — | — | `qn-local-1:9011` | ClusterService 주소 |
| `RUST_LOG` | `storage_node=info` | `compute_node=info` | `query_node=info` | 로그 레벨 |

`RUST_LOG` 오버라이드:
```bash
# 디버그 레벨로 실행
RUST_LOG=debug ./scripts/dev/run-local.sh

# 특정 모듈만 trace
RUST_LOG="query_node::sql_parser=trace,shared=debug" ./scripts/dev/run-local.sh --no-build
```

---

## 7. 단일 노드 Raft 제약

로컬 개발 환경은 Raft 클러스터가 아닌 단일 노드 Raft로 실행된다.

- Leader 선출 즉시 완료 (투표 필요 없음)
- Raft 쿼럼: 1 (자기 자신)
- **메타데이터 내구성은 보장되지 않음** — 개발 테스트 전용
- `ALTER CLUSTER DRAIN`, `ALTER CLUSTER DISMISS` 실행 시 Quorum 보호 로직은 단일 노드에서는 스킵됨

---

## 8. 구현 요구사항 (NFR)

| 요구사항 | 기준 |
|---|---|
| **NFR-DEV-001** | `run-local.sh` 실행 후 MySQL 접속 가능까지 **빌드 제외 5초 이내** |
| **NFR-DEV-002** | Ctrl+C 또는 `stop-local.sh` 실행 시 **1초 이내** 전체 프로세스 종료 |
| **NFR-DEV-003** | 환경변수 `RUST_LOG` 로 로그 레벨 변경 가능 |
| **NFR-DEV-004** | `reset-local.sh`로 데이터 완전 초기화 후 재기동 정상 동작 |
| **NFR-DEV-005** | Linux, macOS, Windows(PowerShell) 세 플랫폼 지원 |
| **NFR-DEV-006** | `--no-build` 옵션으로 빌드 생략 시 기존 바이너리 검증 후 실행 |

---

## 9. 미지원 기능 (로컬 단일 인스턴스)

로컬 환경에서 테스트할 수 없는 기능:

- Kafka / Redpanda Routine Load (별도 브로커 필요)
- S3 / HDFS 스토리지 백엔드 (MinIO 또는 실제 클러스터 필요)
- Raft Leader 전환 / QN 장애 복구
- Shard Rebalance (단일 SN에서는 이동 불가)
- CN HPA (Kubernetes 환경 전용)

위 기능 테스트는 `docker-compose.yml` (전체 클러스터) 또는 `docker-compose.dev.yml` + Redpanda/MinIO를 사용한다.
