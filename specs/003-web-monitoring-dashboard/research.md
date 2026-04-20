# Research: Web Monitoring Dashboard

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-17

---

## 1. 기존 인프라 현황 (What Already Exists)

### 1.1 QN 웹 서버 (port 8080)

`query-node/src/web_ui/server.rs` — 이미 axum 라우터가 구성되어 있음.

**기존 엔드포인트:**

| 경로 | 용도 | 대시보드 재사용 여부 |
|------|------|-------------------|
| `GET /api/v1/cluster` | 클러스터 노드 목록 + 상태 | ✅ 직접 재사용 |
| `GET /metrics` | Prometheus 메트릭 | ✅ 직접 재사용 |
| `GET /api/cubes` | 테이블(Cube) 목록 | ✅ 직접 재사용 |
| `GET /api/v1/profiler` | 쿼리 프로파일러 | ✅ 직접 재사용 |
| `GET /health` | 헬스체크 | ✅ 이미 존재 |

**필요하지만 없는 엔드포인트:**

| 경로 | 용도 | 구현 위치 |
|------|------|----------|
| `GET /` 또는 `GET /dashboard` | HTML 대시보드 메인 페이지 | QN `dashboard.rs` 신규 |
| `GET /api/v1/lsm` | 모든 SN의 LSM Compaction 상태 집계 | QN `monitoring.rs` 추가 |

### 1.2 SN 웹 서버 (port 8040)

`storage-node/src/main.rs` — `/health`, `/healthz`, `/api/v1/info` 만 존재.

**필요한 추가 엔드포인트:**

| 경로 | 용도 | 구현 위치 |
|------|------|----------|
| `GET /api/v1/lsm-status` | 이 SN의 Compaction 상태 JSON | SN `main.rs` 추가 |
| `GET /logs` | 최근 로그 100개 | SN `main.rs` 추가 |

### 1.3 CN 웹 서버 (port 10040)

`compute-node/src/main.rs` — `/health`, `/healthz`, `/api/v1/info` 만 존재.

**필요한 추가 엔드포인트:**

| 경로 | 용도 | 구현 위치 |
|------|------|----------|
| `GET /logs` | 최근 로그 100개 | CN `main.rs` 추가 |

---

## 2. 기술 결정

### 2.1 프론트엔드 방식

**Decision**: Rust 핸들러에서 HTML 문자열 반환 (Server-Side Rendering)  
**Rationale**:
- npm/webpack/Node.js 등 별도 빌드 도구 불필요
- 단일 Rust 바이너리에 UI 포함
- 의존성 최소화 (axum 이미 사용 중)

**Alternatives considered**:
- React/Vue SPA: 빌드 파이프라인 추가 필요, 이 규모에서 과함
- 외부 HTML 파일 읽기: 배포 시 파일 경로 관리 복잡성

**Implementation Pattern**:
```rust
// dashboard.rs
pub async fn dashboard_handler() -> impl IntoResponse {
    Html(r#"
        <!DOCTYPE html>
        <html>...
        <script>
            setInterval(() => fetch('/api/v1/cluster').then(...), 30000);
        </script>
        </html>
    "#)
}
```

### 2.2 데이터 갱신 방식

**Decision**: JavaScript `setInterval` 폴링 (30초 주기)  
**Rationale**: WebSocket 구현 대비 단순함. 모니터링 대시보드에서 30초 지연 허용.  
**Alternatives considered**: WebSocket streaming — 현재 `/ws/sql` stub 존재하나 미구현

### 2.3 LSM 상태 수집 방식

**Decision**: QN이 STORAGE_NODES 환경변수에서 SN 주소를 읽어 각 SN의 `/api/v1/lsm-status` HTTP 엔드포인트를 폴링하여 집계.  
**Rationale**:
- gRPC를 통한 방법도 가능하나, HTTP 폴링이 대시보드 용도에 충분
- SN 내부 LSM 상태는 이미 `CompactionWorker`에 존재 (`l0_count()`, `compaction_score()`)

### 2.4 로그 저장 방식

**Decision**: 각 노드에서 최근 로그 100개를 `Arc<Mutex<VecDeque<LogEntry>>>` 인메모리 원형 버퍼로 보관.  
**Rationale**: 영속 로그는 파일/외부 시스템 담당. 대시보드는 실시간 진단용 최근 100개만 필요.  
**Alternatives considered**: tracing-subscriber 커스텀 레이어 — 복잡도 증가, 100개 버퍼로 충분

### 2.5 HTML 스타일링

**Decision**: 인라인 CSS (외부 CDN 없음)  
**Rationale**: 오프라인 환경 지원, CDN 의존성 제거  
**Style approach**: 다크 테마 테이블 기반 레이아웃 (Bootstrap/Tailwind 없이)

---

## 3. 데이터 가용성 분석

### 3.1 클러스터 노드 현황 (FR-001)

- **기존**: `/api/v1/cluster` → `ClusterOverview` JSON (`monitoring.rs:77-113`)
- **문제**: `NodeStatus.alive: bool`만 있고 JOIN 상태(ACTIVE/DRAINING/OFFLINE) 구분 없음
- **해결**: `ClusterOverview` 응답에 상태 문자열 추가 또는 클라이언트에서 `alive` 기준으로 표시

### 3.2 테이블(Cube) 목록 (FR-003)

- **기존**: `/api/cubes` → `Vec<CubeSchema>` (`api.rs`)
- **CubeSchema 포함 데이터**: 이름, 컬럼 정의, 분산키, 파티션 정보
- **부족한 데이터**: 총 행 수, 데이터 크기 → `stats_reporter`에서 ShardStats로 관리하나 현재 stub
- **해결**: 가능한 stats 표시, 없으면 "N/A" 표시

### 3.3 LSM Compaction 상태 (FR-004)

- **기존 SN 코드**: `PartitionLevels.l0_count()`, `compaction_score()`, `write_control()` (levels.rs)
- **없는 것**: 이 데이터를 HTTP로 노출하는 엔드포인트
- **새로 만들 것**: `GET /api/v1/lsm-status` (SN), `GET /api/v1/lsm` (QN 집계)

### 3.4 최근 로그 (FR-007~010)

- **기존**: `tracing` 구조체 로그가 stdout/파일로만 출력
- **새로 만들 것**: `tracing-subscriber` 커스텀 레이어 또는 로그 수집 전역 버퍼

---

## 4. 구현 순서

```
Phase A: QN HTML 대시보드 페이지 (FR-001 ~ FR-006)
  └─ dashboard.rs 신규 + server.rs 라우트 추가
  └─ /api/v1/cluster 기존 재사용 (클러스터 노드 화면)
  └─ /api/cubes 기존 재사용 (테이블 정보 화면)

Phase B: LSM 상태 API (FR-004)
  └─ SN: /api/v1/lsm-status 신규
  └─ QN: /api/v1/lsm 신규 (SN 목록 폴링 + 집계)

Phase C: CN/SN 로그 페이지 (FR-007 ~ FR-010)
  └─ 공통: LogBuffer (VecDeque<LogEntry>, max 100)
  └─ CN: /logs 엔드포인트
  └─ SN: /logs 엔드포인트
```
