# Feature Specification: Web Monitoring Dashboard

**Feature Branch**: `003-web-monitoring-dashboard`  
**Created**: 2026-04-17  
**Status**: Draft  
**Input**: User description: "QN 에는 접속 가능한 web 페이지가 있어야 해, 이 페이지에 접속하면 다음과 같은 기능을 제공해야해 1. Cluster 에 접속 중인 각각의 NODE 의 IP 주소와 현황 ( JOIN 상태인지 아닌지 NODE 의 상태를 보여주기 ), 2. 전체 DB 의 Storage 상세 및 table 정보, 3. Storage 노드의 LSM Merge 상태를 보여주는 화면 이 web 페이지는 QN 으로 접속했을때에 보여주지만 상세한 로그 메시지는 CN 과 SN 을 접속했을때에 보여줘야 겠지"

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 — 클러스터 노드 현황 조회 (Priority: P1)

운영자가 브라우저로 QN 웹 페이지에 접속하면 클러스터에 참여 중인 모든 노드(QN, CN, SN)의 IP 주소, 역할, JOIN 상태를 한눈에 확인할 수 있다.

**Why this priority**: 클러스터 운영 중 가장 빈번하게 필요한 정보이며, 노드 장애를 즉시 파악하기 위한 핵심 화면이다.

**Independent Test**: QN 웹 UI에 접속하여 "Cluster" 탭을 열면 노드 목록과 각 노드의 상태(JOIN / LEAVING / OFFLINE)가 표시되는지 확인한다. 노드 하나를 종료했을 때 해당 노드가 OFFLINE으로 표시되면 통과.

**Acceptance Scenarios**:

1. **Given** QN이 실행 중이고 SN 2개, CN 1개가 클러스터에 JOIN되어 있을 때, **When** 운영자가 웹 UI 클러스터 탭을 열면, **Then** 모든 노드의 IP 주소, 역할(QN/CN/SN), 상태(JOIN)가 목록으로 표시된다.
2. **Given** SN 하나가 네트워크 장애로 응답하지 않을 때, **When** 운영자가 클러스터 탭을 새로고침하면, **Then** 해당 SN이 OFFLINE 또는 UNREACHABLE 상태로 표시된다.
3. **Given** 새 CN이 클러스터에 JOIN할 때, **When** 운영자가 클러스터 탭을 확인하면, **Then** 해당 CN이 목록에 JOIN 상태로 추가되어 표시된다.

---

### User Story 2 — DB 스토리지 및 테이블 정보 조회 (Priority: P2)

운영자가 QN 웹 UI에서 현재 존재하는 모든 테이블(Cube)의 목록, 테이블별 파티션 수, 데이터 크기, 컬럼 구성을 확인할 수 있다.

**Why this priority**: 데이터가 잘 적재되고 있는지, 스토리지가 얼마나 사용되고 있는지 파악하는 운영의 기본 요구사항이다.

**Independent Test**: 테이블 3개를 생성하고 데이터를 삽입한 뒤 웹 UI Storage 탭을 열면 각 테이블의 행 수, 파티션 수, 추정 크기가 표시되는지 확인한다.

**Acceptance Scenarios**:

1. **Given** `events`, `page_views` 두 테이블이 존재할 때, **When** 운영자가 Storage 탭을 열면, **Then** 두 테이블의 이름, 컬럼 목록, 파티션 수, 총 행 수, 총 데이터 크기가 표시된다.
2. **Given** 테이블에 데이터를 삽입한 후, **When** 운영자가 Storage 탭을 새로고침하면, **Then** 해당 테이블의 행 수와 크기가 갱신된 값을 반영한다.
3. **Given** 테이블이 하나도 없을 때, **When** 운영자가 Storage 탭을 열면, **Then** "테이블 없음" 상태 메시지가 표시된다.

---

### User Story 3 — LSM Merge(Compaction) 상태 모니터링 (Priority: P2)

운영자가 QN 웹 UI에서 각 SN의 LSM Compaction 진행 상태(진행 중 여부, 완료된 Compaction 횟수, L0 파일 수)를 확인할 수 있다.

**Why this priority**: Compaction이 밀리면 읽기 성능이 저하되므로 운영자가 적시에 파악하고 대응할 수 있어야 한다.

**Independent Test**: 대량 데이터를 삽입하여 L0 파일이 쌓인 상태에서 웹 UI LSM 탭을 열면 해당 SN의 Compaction 상태가 "Running" 또는 "Pending"으로 표시되는지 확인한다.

**Acceptance Scenarios**:

1. **Given** SN에 L0 파일이 4개 이상 누적되어 Compaction이 진행 중일 때, **When** 운영자가 LSM 탭을 열면, **Then** 해당 SN의 Compaction 상태가 "Running"으로, L0 파일 수와 대상 레벨이 표시된다.
2. **Given** Compaction이 완료된 후, **When** 운영자가 LSM 탭을 새로고침하면, **Then** 완료 횟수가 1 증가하고 L0 파일 수가 감소된 값으로 표시된다.
3. **Given** 모든 SN의 Compaction이 비활성 상태일 때, **When** 운영자가 LSM 탭을 열면, **Then** 각 SN이 "Idle" 상태로 표시된다.

---

### User Story 4 — CN/SN 노드 상세 로그 조회 (Priority: P3)

운영자가 CN 또는 SN의 웹 포트로 직접 접속하면 해당 노드의 상세 운영 로그(최근 N개)를 확인할 수 있다.

**Why this priority**: 전체 클러스터 개요는 QN에서 제공하지만, 특정 노드의 세부 동작을 디버깅할 때 해당 노드에 직접 접속하여 로그를 볼 수 있어야 한다.

**Independent Test**: CN 웹 포트로 접속하면 "Logs" 페이지가 열리고, 최근 실행된 FragmentRequest 처리 로그가 표시되는지 확인한다.

**Acceptance Scenarios**:

1. **Given** CN 웹 서버가 실행 중일 때, **When** 운영자가 CN 웹 포트의 `/logs` 경로로 접속하면, **Then** 최근 100개 이상의 처리 로그가 타임스탬프와 함께 표시된다.
2. **Given** SN 웹 서버가 실행 중일 때, **When** 운영자가 SN 웹 포트의 `/logs` 경로로 접속하면, **Then** WAL 기록, Compaction 이벤트, 읽기/쓰기 요청 로그가 표시된다.
3. **Given** 특정 노드에 오류가 발생했을 때, **When** 운영자가 해당 노드의 로그 페이지를 열면, **Then** 오류 로그가 강조(에러 레벨 표시)되어 구분된다.

---

### Edge Cases

- QN이 일부 SN에 연결할 수 없을 때, 해당 SN의 데이터는 "정보 없음"으로 표시하고 나머지 노드 정보는 정상 표시한다.
- 클러스터에 노드가 하나도 없을 때(단독 실행 시), 빈 클러스터 목록 화면을 표시한다.
- 매우 많은 테이블(100개 이상)이 있을 때, Storage 탭 상단에 `<input type="text" placeholder="테이블 검색...">` 필터를 배치하여 클라이언트 사이드에서 테이블 이름 실시간 필터링을 제공한다. 입력값과 일치하지 않는 행은 `display:none` 처리한다. 서버 재요청 없이 동작해야 한다.
- 브라우저에서 자동 새로고침 주기를 `<select>` 드롭다운(5초 / 10초 / 30초 / 끄기)으로 설정할 수 있어야 한다(기본값: 5초). 주기 변경 시 기존 `setInterval`을 `clearInterval`로 취소하고 새 주기로 재등록한다. Cluster / Storage / LSM 탭 모두 동일 주기가 적용된다.

---

## Requirements *(mandatory)*

### Functional Requirements

**QN 웹 UI (포트 8080)**

- **FR-000**: **QN 피어 멤버십 — StarRocks FE 패턴**: QN 노드는 기동 시 `QN_HTTP_PEERS` 환경변수에서 피어 QN의 HTTP 주소(콤마 구분)를 읽고, 각 피어의 `POST /api/v1/nodes/register` 엔드포인트에 자신의 `node_id`, `mysql_addr`, `role(Leader|Follower)`을 등록해야 한다. 이후 5초마다 heartbeat으로 재등록하여 alive 상태를 유지해야 한다. `QN_HTTP_PEERS`의 첫 번째 주소가 자신의 HTTP 주소이면 Leader, 아니면 Follower 역할이다. 이 메커니즘은 `/api/v1/cluster` 응답의 `query_nodes` 배열에 모든 QN이 표시되기 위한 전제 조건이다. **[TEMP-EXCEPTION — Constitution §V]** `POST /api/v1/nodes/register`는 QN 간 피어 자동 발견을 위한 내부 구현 메커니즘이다. 운영자가 노드를 추가·제거하는 사용자 인터페이스는 반드시 SQL `ALTER CLUSTER JOIN/DRAIN/DISMISS` 명령이어야 한다. 이 HTTP 등록은 Phase B Raft 완성 시 대체된다.
- **FR-001**: QN 웹 UI는 클러스터의 모든 노드(QN, CN, SN) 목록을 IP 주소, 역할(Leader/Follower/Worker/Storage), 상태(JOINING / ACTIVE / DRAINING / DISCONNECTED)와 함께 표시해야 한다.
- **FR-001-A**: QN 목록은 Raft 역할(Leader 1개 + Follower N개)을 명확히 구분하여 표시해야 한다.
- **FR-001-B**: **노드 상태 모델** — 각 노드는 아래 4개 상태를 가지며, 단방향 전환만 허용된다.

  | 상태 | 의미 | 진입 조건 | UI 색상 |
  |------|------|-----------|---------|
  | `JOINING` | 클러스터 접속 후 메타데이터·스키마 동기화 진행 중. 쿼리 라우팅 제외. | 노드 기동 후 첫 heartbeat 수신 시 | 파란색 |
  | `ACTIVE` | 정상 운영 중. 읽기·쓰기 모두 허용. | 메타데이터 동기화 완료 후 | 초록색 |
  | `DRAINING` | `ALTER CLUSTER DRAIN` 수신. 데이터를 다른 ACTIVE 노드로 백그라운드 이전 중. 읽기 허용, INSERT 라우팅 즉시 제외. | `ALTER CLUSTER DRAIN` SQL 실행 시 | 노란색 |
  | `DISCONNECTED` | `ALTER CLUSTER DISMISS` 완료 또는 heartbeat 타임아웃(15초) 초과. 클러스터에서 제거됨. UI에 기록 잔류. | DRAIN 완료 후 DISMISS 시 / heartbeat 타임아웃 시 | 회색 |

  상태 전환: `JOINING → ACTIVE → DRAINING → DISCONNECTED` (역방향 불허)
- **FR-002**: 클러스터 노드 목록은 5초마다 자동으로 갱신되어야 한다.
- **FR-003**: QN 웹 UI는 현재 존재하는 모든 테이블의 이름, 컬럼 수, 파티션 수, 총 행 수, 추정 데이터 크기를 표시해야 한다. 파티션 수·총 행 수·추정 데이터 크기는 각 SN의 `GET /api/v1/lsm-status` API를 QN에서 집계하여 제공해야 한다.
- **FR-004**: QN 웹 UI는 각 SN의 LSM Compaction 상태(Idle / Running / Pending), L0 파일 수, 완료된 Compaction 횟수를 SN별로 표시해야 한다.
- **FR-005**: QN 웹 UI는 별도 설치 없이 브라우저로 접근 가능한 HTML 기반 페이지여야 한다.
- **FR-006**: QN 웹 UI는 탭 또는 섹션으로 구분된 네비게이션을 제공해야 한다 (Cluster Overview / Storage / LSM Status).

**CN/SN 노드 웹 UI (각 노드의 웹 포트)**

- **FR-007**: CN 웹 UI는 최근 처리된 Fragment 실행 로그(타임스탬프, Fragment ID, 처리 시간, 결과 행 수)를 표시해야 한다.
- **FR-008**: SN 웹 UI는 최근 WAL 기록 이벤트, Compaction 이벤트, Write/Read 요청 로그를 표시해야 한다.
- **FR-009**: 로그 항목은 레벨(INFO / WARN / ERROR)에 따라 시각적으로 구분되어야 한다.
- **FR-010**: 각 노드의 웹 UI는 최소 최근 100개 로그를 표시해야 한다.

### Key Entities

- **ClusterNode**: 노드 ID, IP 주소, 역할(QN/CN/SN), 상태(JOINING/ACTIVE/DRAINING/DISCONNECTED), 마지막 heartbeat 시각
- **TableInfo**: 테이블 이름, 컬럼 목록, 파티션 수, 총 행 수, 추정 바이트 크기
- **LsmCompactionStatus**: SN 노드 ID, L0 파일 수, 현재 Compaction 상태, 완료 횟수, 마지막 갱신 시각
- **NodeLogEntry**: 타임스탬프, 로그 레벨, 메시지, 컨텍스트(노드 ID, 작업 유형)

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 운영자가 클러스터 전체 노드 상태를 브라우저 접속 후 5초 이내에 확인할 수 있다.
- **SC-002**: 노드 장애 발생 시 5초 이내에 웹 UI에 해당 노드의 OFFLINE 상태가 반영된다.
- **SC-003**: 10개 이상의 테이블이 있는 경우에도 Storage 탭이 3초 이내에 전체 정보를 표시한다. *[검증: `curl -s -w "%{time_total}\n" -o /dev/null http://localhost:8080/api/cubes` 결과 3.0 미만]*
- **SC-004**: LSM 탭에서 각 SN의 Compaction 상태 변화(Idle→Running→Idle)가 다음 새로고침 주기(5초) 이내에 반영된다.
- **SC-005**: CN/SN 로그 페이지가 최근 100개 로그를 2초 이내에 표시한다. *[검증: `curl -s -w "%{time_total}\n" -o /dev/null http://localhost:10040/logs` 결과 2.0 미만]*
- **SC-006**: 웹 UI는 별도 로그인 없이 클러스터 내부 네트워크에서 접근 가능하다 (외부 인증 불필요, 내부 운영 도구로 간주).

---

## Assumptions

- 웹 UI는 내부 운영 도구이므로 별도 인증/권한 관리 없이 접근 가능하다고 가정한다.
- QN 웹 서버는 이미 포트 8080에서 실행 중이므로, 기존 웹 서버에 새 라우트를 추가하는 방식으로 구현한다.
- CN 웹 서버(포트 10040)와 SN 웹 서버(포트 8040)는 이미 실행 중이며, 새 `/logs` 엔드포인트를 추가한다.
- 실시간 스트리밍(WebSocket)이 아닌 주기적 폴링(5초) 방식으로 데이터를 갱신한다.
- 로그 저장소는 노드 메모리 내 원형 버퍼(최대 1,000개 항목)를 사용하며, 영구 저장은 범위 밖이다.
- 모바일 브라우저 지원은 선택사항이며, 데스크탑 브라우저(Chrome/Firefox/Edge 최신 버전)를 주 대상으로 한다.
- QN이 각 CN/SN의 상태를 주기적으로 polling하여 클러스터 상태를 집계한다.
- **QN 피어 등록**: 각 QN은 기동 시 `QN_HTTP_PEERS` 환경변수로 설정된 피어 QN에 HTTP POST로 자기등록하며, 5초 heartbeat으로 재등록한다. 이는 StarRocks FE(Front-End) 노드 간 멤버십 공유 방식을 참고한 설계다. Raft 합의가 완전 구현(Phase B)되기 전까지 HTTP 기반 등록으로 QN 클러스터 가시성을 확보한다.
- QN 노드 ID는 `NODE_ID` 환경변수 또는 설정 파일의 `node.id` 값을 사용하며, 숫자 Raft ID와 혼용하지 않는다.
