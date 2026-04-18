# Tasks: WOW-DB Web Monitoring Dashboard

**Input**: `specs/003-web-monitoring-dashboard/` (spec.md, plan.md, research.md, data-model.md, contracts/)
**Branch**: `002-wow-db-srs-v02`

추가 요구사항 (user input):
- LSM 상태 화면: 레벨별 Part 수를 테이블(Cube)별로 표시하는 테이블 뷰
- UI는 기능 우선 (미려함 불필요)
- WOW-DB 브랜드 로고 포함

## Format: `[ID] [P?] [Story?] Description with file path`

- **[P]**: 다른 파일에 작업하므로 병렬 실행 가능
- **[US1~US4]**: 해당 User Story 번호

---

## Phase 1: Setup — 공통 기반

**목적**: 모든 User Story가 공유하는 기반 구조 초기화

- [x] T001 기존 axum 웹 서버 및 라우트 구조 파악 (`query-node/src/web_ui/server.rs`, `monitoring.rs`, `api.rs`)
- [x] T002 `query-node/src/web_ui/dashboard.rs` 신규 파일 생성 (HTML 대시보드 핸들러 스텁)
- [x] T003 [P] WOW-DB ASCII/SVG 로고 상수 정의 (`query-node/src/web_ui/dashboard.rs` — `WOW_DB_LOGO` 상수)
- [x] T004 `query-node/src/web_ui/mod.rs` 에 `dashboard` 모듈 공개 선언 추가
- [x] T005 `query-node/src/web_ui/server.rs` 에 `GET /dashboard` 라우트 등록 (dashboard 모듈 핸들러 연결)
- [x] T006 [P] `LogEntry`, `LogsResponse` 구조체 정의 (`shared/src/types.rs` 또는 각 노드 `mod.rs`)

**체크포인트**: `cargo build --workspace` 오류 없이 통과

---

## Phase 2: Foundational — 데이터 수집 인프라

**목적**: 모든 User Story가 의존하는 데이터 수집 계층 완성

- [x] T007 `storage-node/src/main.rs` 에 axum `State` 로 `TabletWriterRegistry` 공유 준비 (`Arc<TabletWriterRegistry>`)
- [x] T008 `storage-node/src/lsm/levels.rs` 에 `LsmLevelStats` 직렬화 구조체 추가 (`LsmLevelStats { level, file_count, size_bytes, compaction_score }`)
- [x] T009 `storage-node/src/grpc/write.rs` 의 `TabletWriter` 에 `get_level_stats() -> Vec<LsmLevelStats>` 메서드 추가
- [x] T010 `storage-node/src/grpc/write.rs` 의 `TabletWriterRegistry` 에 `get_all_lsm_stats() -> Vec<TabletLsmStats>` 메서드 추가 (tablet_id + 레벨별 Part 수 반환)

**체크포인트**: `cargo test -p storage-node lsm` 단위 테스트 통과

---

## Phase 3: US1 — 클러스터 노드 현황 (P1)

**목표**: 브라우저로 `/dashboard` 접속 시 클러스터 노드 목록(IP, 역할, 상태) 확인

**독립 테스트**: `curl http://localhost:8080/dashboard` → HTML 응답에 노드 테이블 포함

### 구현

- [x] T011 [US1] `query-node/src/web_ui/dashboard.rs` — `dashboard_handler()` 비동기 핸들러 구현: `WebUiState`에서 Raft/CubeManager 접근, HTML 기본 골격 반환 (DOCTYPE, `<head>`, 3-탭 네비게이션)
- [x] T012 [P] [US1] `query-node/src/web_ui/dashboard.rs` — WOW-DB 로고 SVG/ASCII 헤더 HTML 생성 함수 (`render_header() -> String`)
- [x] T013 [US1] `query-node/src/web_ui/dashboard.rs` — Cluster 탭 콘텐츠 함수 `render_cluster_tab() -> String`: `/api/v1/cluster` fetch 후 QN/CN/SN 각각 테이블로 렌더링 (ID, Address, Role, Status 컬럼)
- [x] T014 [US1] `query-node/src/web_ui/dashboard.rs` — JavaScript 자동갱신 스크립트 인라인 삽입: `setInterval(fetchCluster, 5000)` — Cluster 탭 데이터만 5초마다 갱신
- [x] T015 [P] [US1] `query-node/src/web_ui/dashboard.rs` — alive:false 노드를 시각적으로 구분하는 인라인 CSS 추가 (예: `color:red` 또는 `background:#fdd`)
- [x] T016 [US1] `query-node/src/web_ui/server.rs` — `WebUiState` 를 `dashboard_handler` 에 전달하도록 라우터 업데이트

**체크포인트**:
- `curl http://localhost:8080/dashboard` → HTTP 200, `<html>` 시작
- 브라우저에서 Cluster 탭에 노드 테이블 표시 확인

---

## Phase 4: US2 — Storage & 테이블 정보 (P2)

**목표**: Storage 탭에서 테이블 목록, 컬럼 수, 파티션 정보 확인

**독립 테스트**: Storage 탭 클릭 시 `/api/cubes` 데이터를 테이블로 표시

### 구현

- [x] T017 [US2] `query-node/src/web_ui/dashboard.rs` — `render_storage_tab() -> String`: `/api/cubes` fetch 후 테이블명, 컬럼 수, 분산키, 파티션 타입을 HTML 테이블로 렌더링
- [x] T018 [P] [US2] `query-node/src/web_ui/dashboard.rs` — 컬럼 상세 접기/펼치기 없이 컬럼 이름 콤마 구분 목록으로 표시 (UI 단순화)
- [x] T019 [US2] `query-node/src/web_ui/dashboard.rs` — Storage 탭 JavaScript: `fetchStorage()` 함수 추가, 5초마다 갱신
- [x] T020 [P] [US2] `query-node/src/web_ui/dashboard.rs` — 테이블이 없을 때 "No tables found" 메시지 표시

**체크포인트**:
- 테이블 3개 생성 후 Storage 탭에 모두 표시되는지 확인
- Docker MySQL: `CREATE CUBE t1 ...; CREATE CUBE t2 ...; CREATE CUBE t3 ...` 후 대시보드 확인

---

## Phase 5: US3 — LSM Compaction 상태 (P2)

**목표**: LSM 탭에서 SN별·테이블별·레벨별 Part(SSTable) 수 확인

**독립 테스트**: `GET /api/v1/lsm` → JSON에 partition별 레벨별 파일 수 포함

### 백엔드

- [x] T021 [US3] `storage-node/src/main.rs` — `GET /api/v1/lsm-status` 엔드포인트 추가: `TabletWriterRegistry`의 `get_all_lsm_stats()` 호출 후 JSON 반환 (axum `Json(stats)`)
- [x] T022 [P] [US3] `storage-node/src/main.rs` — `axum::extract::State(Arc<TabletWriterRegistry>)` 라우터 상태 주입 (기존 HTTP 라우터에 State 추가)
- [x] T023 [US3] `query-node/src/web_ui/monitoring.rs` — `GET /api/v1/lsm` 엔드포인트 추가: `STORAGE_NODES` 환경변수에서 SN 주소 파싱 후 각 SN의 `/api/v1/lsm-status` HTTP GET 폴링, 결과 `ClusterLsmOverview` JSON 반환 (reqwest 사용)
- [x] T024 [P] [US3] `query-node/src/web_ui/server.rs` — `/api/v1/lsm` 라우트 등록

### 프론트엔드 — LSM 테이블 뷰

- [x] T025 [US3] `query-node/src/web_ui/dashboard.rs` — `render_lsm_tab() -> String`: `/api/v1/lsm` fetch 후 아래 구조의 HTML 테이블 렌더링:
  ```
  SN 노드  | 테이블(Cube) | L0 files | L1 files | L2 files | ... | Compaction | WriteControl
  ---------|------------|---------|---------|---------|-----|------------|------------
  sn-1     | events      |    3    |    12   |    0    | ... | Idle       | Normal
  sn-1     | page_views  |    1    |    5    |    0    | ... | Idle       | Normal
  ```
- [x] T026 [P] [US3] `query-node/src/web_ui/dashboard.rs` — L0 파일 수가 compaction_trigger(기본 4) 이상이면 셀을 강조 표시 (인라인 style="color:orange")
- [x] T027 [P] [US3] `query-node/src/web_ui/dashboard.rs` — WriteControl이 Slowdown이면 yellow, Stop이면 red 강조
- [x] T028 [US3] `query-node/src/web_ui/dashboard.rs` — LSM 탭 JavaScript: `fetchLsm()` 함수 추가, 5초마다 갱신
- [x] T029 [P] [US3] `query-node/src/web_ui/dashboard.rs` — SN에 접속 불가 시 해당 행에 "Unreachable" 표시 (에러 처리)

**체크포인트**:
- `curl http://localhost:8080/api/v1/lsm` → JSON with `nodes[]` 배열
- LSM 탭에 SN별·테이블별 레벨 파일 수 테이블 표시
- 대량 INSERT 후 L0 파일 수 증가 확인

---

## Phase 6: US4 — CN/SN 노드 상세 로그 (P3)

**목표**: CN/SN 웹 포트 `/logs` 경로에서 최근 처리 로그 조회

**독립 테스트**: `curl http://localhost:10040/logs` → JSON 로그 100개 이하

### 공통 로그 버퍼

- [x] T030 [US4] `shared/src/log_buffer.rs` 신규 파일: `LogEntry`, `LogLevel`, `LogBuffer(VecDeque, capacity=100)` 구현, `push()`, `recent(n)`, `recent_by_level(level, n)` 메서드
- [x] T031 [P] [US4] `shared/src/lib.rs` 에 `log_buffer` 모듈 공개

### Compute Node

- [x] T032 [US4] `compute-node/src/main.rs` — 전역 `LOG_BUFFER: LazyLock<Arc<Mutex<LogBuffer>>>` 싱글톤 선언
- [x] T033 [US4] `compute-node/src/main.rs` — tracing 이벤트를 `LOG_BUFFER`에 기록하는 커스텀 레이어 또는 `tracing::info!` 후 수동 push (단순화: 주요 gRPC 핸들러에서 직접 push)
- [x] T034 [US4] `compute-node/src/main.rs` — `GET /logs` 엔드포인트 추가: `LOG_BUFFER.lock().recent(100)` → `LogsResponse` JSON 반환
- [x] T035 [P] [US4] `compute-node/src/grpc/server.rs` — `ExecuteFragment` 핸들러에서 `LOG_BUFFER.push(LogEntry { level: INFO, message: "ExecuteFragment...", fields: {query_id, rows} })` 추가

### Storage Node

- [x] T036 [US4] `storage-node/src/main.rs` — 전역 `LOG_BUFFER: LazyLock<Arc<Mutex<LogBuffer>>>` 싱글톤 선언
- [x] T037 [US4] `storage-node/src/main.rs` — `GET /logs` 엔드포인트 추가: `LogsResponse` JSON 반환
- [x] T038 [P] [US4] `storage-node/src/grpc/server.rs` — `write_rows`, `scan_tablet` 핸들러에서 `LOG_BUFFER.push(...)` 추가
- [x] T039 [P] [US4] `storage-node/src/grpc/write.rs` — WAL replay 완료 시 `LOG_BUFFER.push(LogEntry { level: INFO, message: "WAL replay ...", fields: {tablet_id, rows} })` 추가

### 레벨 필터 쿼리 파라미터

- [x] T040 [P] [US4] CN/SN 모두 — `GET /logs?level=WARN` 쿼리 파라미터 처리: `LogLevel::from_str(level_param)` 이상만 필터링

**체크포인트**:
- `curl http://localhost:10040/logs` → JSON `{node_id, role, entries: [...], total_buffered}`
- `curl http://localhost:8040/logs` → JSON 동일 구조
- Fragment 실행 후 CN 로그에 `ExecuteFragment` 항목 확인

---

## Phase 7: Polish — 브랜드 & 완성도

**목적**: WOW-DB 로고, 탭 네비게이션 최종 조립, 통합 확인

- [x] T041 `query-node/src/web_ui/dashboard.rs` — WOW-DB ASCII 로고 헤더 완성:
  ```
  ██╗    ██╗ ██████╗ ██╗    ██╗      ██████╗ ██████╗
  ██║    ██║██╔═══██╗██║    ██║      ██╔══██╗██╔══██╗
  ██║ █╗ ██║██║   ██║██║ █╗ ██║█████╗██║  ██║██████╔╝
  ██║███╗██║██║   ██║██║███╗██║╚════╝██║  ██║██╔══██╗
  ╚███╔███╔╝╚██████╔╝╚███╔███╔╝      ██████╔╝██████╔╝
  ```
  페이지 상단 `<pre>` 태그 내 고정폭 폰트로 출력
- [x] T042 [P] `query-node/src/web_ui/dashboard.rs` — 탭 네비게이션 JavaScript: 탭 클릭 시 해당 섹션만 `display:block`, 나머지 `display:none`으로 전환
- [x] T043 [P] `query-node/src/web_ui/dashboard.rs` — 페이지 하단 "Last updated: {timestamp}" 표시 (각 탭 갱신 시 업데이트)
- [x] T044 `query-node/src/web_ui/server.rs` — `GET /` 루트 경로도 `/dashboard` 로 리다이렉트 (301)
- [x] T045 [P] `query-node/src/web_ui/dashboard.rs` — 페이지 타이틀 `<title>WOW-DB Dashboard</title>` 설정
- [x] T046 전체 빌드 + 테스트: `cargo build --workspace && cargo test --workspace` 오류 없이 통과

**체크포인트**:
- `curl -L http://localhost:8080/` → `/dashboard` 리다이렉트 후 HTML 반환
- 브라우저에서 로고·3탭·자동갱신 모두 동작 확인
- `cargo test --workspace` 기존 테스트 회귀 없음

---

## 의존성 그래프

```
Phase 1 (T001-T006)       ← 모든 Phase의 전제
     │
Phase 2 (T007-T010)       ← US3(LSM) 의존
     │
 ┌───┼───────────────────┐
 │   │                   │
US1  US2                 US3 (Phase 2 필요)
(T011-T016) (T017-T020)  (T021-T029)
     │                        │
     └──────────┬─────────────┘
                │
              US4 (T030-T040, Phase 2 LogBuffer 후)
                │
            Polish (T041-T046)
```

## 병렬 실행 기회

| 병렬 그룹 | 태스크 |
|-----------|--------|
| Phase 1 병렬 | T003 (로고 상수) + T006 (LogEntry 구조체) |
| US1 내 병렬 | T012 (로고 렌더), T015 (CSS) 는 T011 완료 후 병렬 |
| US2 내 병렬 | T018 (컬럼 표시), T020 (빈 상태) 는 T017 완료 후 병렬 |
| US3 백엔드 병렬 | T022 (State 주입) + T024 (라우트 등록) — T021, T023 각각 독립 |
| US3 프론트 병렬 | T026 (L0 강조), T027 (WriteControl 강조), T029 (오류 처리) — T025 완료 후 병렬 |
| US4 CN/SN 병렬 | T033~T035 (CN) 와 T036~T039 (SN) 는 T030-T031 완료 후 병렬 |

## MVP 범위

**MVP = Phase 1 + Phase 2 + Phase 3 (US1)**

US1(클러스터 노드 현황)만 완성해도 운영자가 클러스터 상태를 브라우저로 즉시 확인할 수 있는 최소 유용한 제품이 된다.

- T001~T016 (총 16개 태스크)
- 예상 결과: `http://localhost:8080/dashboard` → WOW-DB 로고 + 노드 현황 테이블

**전체**: 57개 태스크 / 9 Phase

---

## Phase 8: QN 클러스터 멤버십 — StarRocks FE 패턴

**목적**: QN 노드 간 상호 자기등록 및 5초 heartbeat 구현. 대시보드 Cluster 탭에서 QN Leader/Follower 전체가 표시되려면 반드시 필요한 인프라.

**배경**: StarRocks에서 FE(Front-End) 노드들은 서로 Raft 멤버십을 공유한다. WOW-DB QN은 동일한 패턴으로, 각 QN이 피어 QN의 HTTP `/api/v1/nodes/register`를 직접 호출하여 자신을 등록하고 5초마다 heartbeat으로 재등록한다.

### 구현

- [x] T047 `query-node/src/startup.rs` 신규 파일 — `register_with_qn_peers(node_id, mysql_addr, web_port)` 구현:
  - `QN_HTTP_PEERS` 환경변수에서 피어 QN HTTP 주소(콤마 구분) 읽기
  - 자기 자신 주소는 건너뜀
  - 각 피어에 `POST /api/v1/nodes/register` 전송: `{node_type: "query", address: mysql_addr, role: "Leader"|"Follower"}`
  - 5초마다 재등록하는 백그라운드 heartbeat tokio::spawn
  - Leader 판별: `QN_HTTP_PEERS` 첫 번째 항목이 자신이면 Leader, 아니면 Follower
- [x] T048 `query-node/src/main.rs` — `mod startup` 선언 + Web UI 서버 기동 직후 `startup::register_with_qn_peers()` tokio::spawn 호출 (500ms 대기 후 실행)
- [x] T049 [P] `query-node/src/web_ui/server.rs` — `WebUiState`에 `node_id: String` 필드 추가 (QN 자신의 NODE_ID 보관)
- [x] T050 [P] `query-node/src/web_ui/monitoring.rs` — `cluster_overview`에서 `state.raft.node_id.0.to_string()` 대신 `state.node_id.clone()` 사용 (QN-1 ID가 "1"이 아닌 "qn-local-1"로 정상 표시)
- [x] T051 [P] `query-node/src/web_ui/monitoring.rs` — `self_qn.address`를 `"localhost:{port}"` → `"127.0.0.1:{port}"` 형식으로 통일 (다른 QN과 일관된 형식)
- [x] T052 `scripts/dev/scale-local.ps1` — QN 기동 시 `QN_HTTP_PEERS` 환경변수를 모든 QN HTTP 주소(콤마 구분)로 설정. QN-1이 첫 번째 위치 → Leader 역할 자동 부여
- [x] T053 [P] `query-node/src/web_ui/monitoring.rs` 기존 검증 — `register_node` 핸들러가 `node_type: "query"` 를 `"query:{node_id}"` 키로 저장하고, `cluster_overview`가 `key.starts_with("query:")` 로 QN 목록에 추가하는 경로 확인 (이미 구현됨)

**체크포인트**:
- QN×3 기동 후 `curl http://localhost:18080/api/v1/cluster` → `query_nodes` 배열에 3개 모두 표시
- `id` 값이 "1"이 아닌 "qn-local-1" 형식으로 통일
- 대시보드 Cluster 탭에서 QN Leader 1개 + Follower 2개 구분 표시

## 병렬 실행 기회 (추가)

| 병렬 그룹 | 태스크 |
|-----------|--------|
| Phase 8 병렬 | T049 (WebUiState 필드) + T051 (address 형식) + T053 (기존 경로 확인) — T047, T048 완료 후 병렬 |

---

## Phase 9: Analyze 결과 반영 — HIGH 이슈 수정

**출처**: `/speckit-analyze` 결과 I1, I2, C1 (HIGH severity)

### 구현

- [x] T054 `specs/003-web-monitoring-dashboard/contracts/qn-dashboard-api.md` — Auto-refresh 기술 오류 수정: `30초` → `5초` (I1)
- [x] T055 `query-node/src/startup.rs` — QN heartbeat 주기 15초 → 5초 변경 (I2: SC-002 OFFLINE 감지 시간 단축)
- [x] T056 `query-node/src/web_ui/monitoring.rs` — TTL_MS 30,000 → 15,000 ms (I2: heartbeat 5s × 3배 버퍼)
- [x] T057 [P] `query-node/src/web_ui/api.rs` + `dashboard.rs` — CubeSummary에 `partition_count`, `row_count`, `size_bytes` 필드 추가 및 Storage 탭 테이블에 Partitions/Rows/Size 컬럼 표시 (C1: FR-003 부분 이행; 실제 값은 SN 집계 Phase D 구현)

**체크포인트**:
- `cargo build -p query-node` 오류 없이 완료
- Storage 탭 HTML에 Partitions / Rows / Size 컬럼 표시 확인
- QN 피어가 장애 시 15초 이내 OFFLINE 전환 확인
