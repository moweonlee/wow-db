# Feature Specification: WOW-DB 웹 분석 OLAP 데이터베이스

**Feature Branch**: `001-wow-db-web-analytics`  
**Created**: 2026-04-12  
**Status**: Draft  
**Input**: WOW-DB SRS v0.1 + TC v0.1 (srs.md + tc.md)

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 - 이벤트 데이터 수집 및 Cube 생성 (Priority: P1)

데이터 엔지니어가 웹 서비스의 사용자 행동 이벤트(page_view, click, purchase 등)를 WOW-DB에 수집하기 위해 `CREATE CUBE` DDL로 이벤트 스키마를 정의하고, Kafka Routine Load / Spark 커넥터 / MySQL INSERT를 통해 대용량 이벤트를 지속적으로 적재할 수 있다.

**Why this priority**: 데이터 수집이 없으면 분석도 없다. Cube 생성과 Ingestion은 시스템의 최우선 기반 기능이다.

**Independent Test**: MySQL 클라이언트로 `CREATE CUBE page_events (...)` 실행 후 Kafka Routine Load로 이벤트를 수집하고, `SELECT COUNT(*) FROM page_events` 로 데이터가 적재됨을 확인하면 독립적으로 검증 가능하다.

**Acceptance Scenarios**:

1. **Given** WOW-DB 클러스터가 정상 기동 중이고, **When** 데이터 엔지니어가 `CREATE CUBE page_events (...) ENGINE = WOW_LSM DISTRIBUTED BY HASH(device_id) BUCKETS 8` 을 실행하면, **Then** `Query OK` 응답과 함께 `SHOW CUBES` 에 `page_events` 가 표시된다.

2. **Given** `page_events` Cube가 존재하고, **When** Kafka Routine Load를 등록하여 `web-events-test` 토픽으로 이벤트를 발행하면, **Then** 30초 이내에 해당 이벤트가 `page_events` 에서 SELECT 조회된다.

3. **Given** `page_events` Cube가 존재하고, **When** MySQL `INSERT INTO page_events (...) VALUES (...)` 를 실행하면, **Then** 1초 이내에 삽입된 레코드가 SELECT로 조회된다.

4. **Given** 운영 중인 Cube에, **When** `ALTER CUBE page_events ADD COLUMN browser_name VARCHAR(64)` 를 실행하면, **Then** 기존 데이터 중단 없이 컬럼이 추가되고 기존 레코드의 새 컬럼은 NULL이다.

5. **Given** Session MV가 연결된 Cube에, **When** `DROP CUBE page_events` (CASCADE 없이) 를 실행하면, **Then** 오류가 반환되고, `DROP CUBE page_events CASCADE` 로 실행하면 Cube와 연관 SMV가 함께 삭제된다.

---

### User Story 2 - Session Materialized View 자동 생성 및 세션 분석 (Priority: P2)

분석가가 이벤트 Cube로부터 자동 세션화(Sessionization) Materialized View를 생성하여, 복잡한 세션 로직을 직접 구현하지 않고도 세션 기반 집계 및 분석 쿼리를 즉시 실행할 수 있다.

**Why this priority**: WOW-DB의 핵심 차별화 기능. 기존 OLAP DB와 달리 세션 변환 파이프라인을 자동화하여 분석가의 작업을 대폭 단축한다.

**Independent Test**: Cube 생성 후 `CREATE SESSION MATERIALIZED VIEW ... FROM page_events USER_KEY = device_id SESSION_TIMEOUT = 30 MINUTE` 실행, 이벤트 삽입 후 `SELECT session_id, session_duration_sec FROM page_events_sessions LIMIT 10` 으로 세션 경계가 올바르게 분리됐는지 확인하면 독립 검증 가능하다.

**Acceptance Scenarios**:

1. **Given** `page_events` Cube에 데이터가 1,000건 이상 있고, **When** `CREATE SESSION MATERIALIZED VIEW page_events_sessions FROM page_events USER_KEY = device_id SESSION_TIMEOUT = 30 MINUTE REFRESH REALTIME` 을 실행하면, **Then** `DESCRIBE page_events_sessions` 에서 `session_id, session_start_time, session_end_time, session_duration_sec, session_event_count, session_seq, is_new_user` 컬럼이 모두 존재한다.

2. **Given** 동일 `device_id` 로 4분 간격(세션 유지)과 6분 간격(세션 분리, timeout=5분) 이벤트를 삽입하고, **When** SMV를 갱신하면, **Then** 4분 간격 이벤트는 같은 `session_id`, 6분 간격 이벤트는 새 `session_id` 로 분류된다.

3. **Given** Web SQL Client에서 Cube 생성을 완료했을 때, **When** 시스템이 "Session Materialized View를 생성하시겠습니까?" 팝업을 표시하면, **Then** 사용자가 User Key(드롭다운), Session Timeout(5분/30분/1시간/직접입력), MV 이름(자동제안)을 선택하여 5단계 대화 흐름으로 SMV를 생성할 수 있다.

4. **Given** SMV가 `COALESCE(user_id, device_id)` 로 User Key가 설정되어 있고, **When** `user_id = NULL` 인 비로그인 이벤트와 `user_id = 'usr-001'` 인 로그인 이벤트가 삽입되면, **Then** 각각 다른 effective key로 세션이 구분된다.

---

### User Story 3 - 웹 분석 전용 쿼리 실행 (Funnel / Cohort / Path) (Priority: P3)

분석가가 커스텀 SQL 없이 `FUNNEL_COUNT()`, `COHORT_ANALYSIS()`, `PATH_ANALYSIS()` 내장 함수를 사용하여 구매 전환율, 사용자 리텐션, 이동 경로 분석을 수행하고 결과를 즉시 얻을 수 있다.

**Why this priority**: WOW-DB의 두 번째 핵심 차별화 기능. 분석가가 Funnel/Cohort/Path를 위해 복잡한 SQL을 직접 작성할 필요가 없다.

**Independent Test**: 사전 정의된 이벤트 시퀀스(dev-A~dev-E)를 삽입 후 `FUNNEL_COUNT(event_name = 'landing_page' STEP 1, ...)` 쿼리를 실행하고 단계별 카운트가 `[4, 3, 2, 1]` 임을 확인하면 독립 검증 가능하다.

**Acceptance Scenarios**:

1. **Given** landing→product→cart→purchase 시퀀스를 가진 5명의 이벤트가 있고, **When** `FUNNEL_COUNT(..., TIME_WINDOW => INTERVAL 7 DAY, STRICT => TRUE)` 를 실행하면, **Then** `step1=4, step2=3, step3=2, step4=1` 이 반환된다 (landing 없이 시작한 dev-E 제외).

2. **Given** 8일 후 purchase 이벤트(시간 초과)와 6일 후 purchase 이벤트(시간 내)가 있고, **When** `TIME_WINDOW => INTERVAL 7 DAY` Funnel 쿼리를 실행하면, **Then** 7일 초과 이벤트는 카운트에서 제외된다.

3. **Given** Week1 코호트 100명의 first_visit 이벤트가 있고 Week2에 70명, Week3에 50명이 재방문했을 때, **When** `COHORT_ANALYSIS(METRIC => 'RETENTION_RATE', TIME_UNIT => 'WEEK', MAX_PERIODS => 3)` 를 실행하면, **Then** `period=1: 70.0%, period=2: 50.0%` 가 반환된다.

4. **Given** 세션 이벤트 데이터가 있고, **When** `PATH_ANALYSIS(ENTRY_FILTER => "event_name = 'landing_page'", MIN_SUPPORT => 0.01)` 을 실행하면, **Then** `'landing_page → product_view → ...'` 형식의 경로와 `path_count, support_rate` 가 반환된다.

---

### User Story 4 - MySQL 클라이언트 호환 접속 (Priority: P4)

개발자와 BI 툴이 기존 MySQL 8.0 CLI, JDBC 드라이버, MySQL Workbench, DBeaver를 추가 설정 없이 WOW-DB에 연결하여 쿼리를 실행할 수 있다.

**Why this priority**: MySQL 호환성은 기존 인프라와의 통합 비용을 없애고 도입 장벽을 낮추는 핵심 기능이다.

**Independent Test**: `mysql -h wowdb-dsl-1 -P 9030 -u admin -p analytics` 로 접속하여 `SELECT VERSION()` 과 `SHOW TABLES` 가 정상 반환되면 독립 검증 가능하다.

**Acceptance Scenarios**:

1. **Given** WOW-DB가 포트 9030에서 실행 중일 때, **When** MySQL CLI 8.0으로 접속하면, **Then** `mysql>` 프롬프트가 표시되고 `VERSION()` 이 `8.0.x-WOW-DB-1.0.0` 형태로 반환된다.

2. **Given** MySQL JDBC 8.x 드라이버를 사용하여, **When** `jdbc:mysql://wowdb-dsl-1:9030/analytics` URL로 연결하면, **Then** 연결이 성공하고 `DatabaseProductName = "WOW-DB"`, PreparedStatement가 정상 실행된다.

3. **Given** WOW-DB에 `page_events` Cube가 존재할 때, **When** `SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'analytics'` 를 실행하면, **Then** `page_events` 가 목록에 표시된다.

4. **Given** `EXPLAIN PHYSICAL SELECT ...` 을 실행하면, **Then** SL 노드별 실행 계획 트리, 파티션 Pruning 여부, 예상 행 수가 출력된다.

---

### User Story 5 - Web SQL Client로 쿼리 편집 및 분석 (Priority: P5)

분석가가 브라우저에서 내장 Web SQL Editor를 통해 SQL을 작성·실행하고, Cube Builder GUI로 스키마를 설계하며, Funnel/Cohort/Path 분석 결과를 차트로 시각화할 수 있다.

**Why this priority**: MySQL 클라이언트 없이 브라우저만으로 분석 환경을 제공하여 진입 장벽을 낮춘다.

**Independent Test**: 브라우저에서 `http://wowdb-dsl-1:8080` 접속 후 SQL 에디터에서 `SELECT * FROM page_events LIMIT 10` 실행, 결과 테이블 표시 확인으로 독립 검증 가능하다.

**Acceptance Scenarios**:

1. **Given** Web SQL Editor에서 `SELECT * FROM pa` 를 입력하면, **When** 자동완성이 트리거되면, **Then** `page_events`, `page_events_sessions` 가 제안된다.

2. **Given** 쿼리를 실행하면, **When** Ctrl+Enter 또는 실행 버튼을 클릭하면, **Then** 1초 이내에 결과 테이블이 표시되고 CSV 다운로드가 가능하다.

3. **Given** `EXPLAIN PHYSICAL SELECT ...` 을 Web SQL Editor에서 실행하면, **Then** 우측 패널에 실행 계획이 트리 형태로 시각화되고 노드 클릭 시 상세 정보가 표시된다.

4. **Given** Cube Builder에서 GUI로 컬럼을 정의하고 "Create Cube"를 클릭하면, **Then** "Session MV를 생성하시겠습니까?" 팝업이 표시되고 5단계 대화 흐름으로 SMV 생성이 가능하다.

---

### User Story 6 - 대용량 분산 스토리지 및 고가용성 (Priority: P6)

시스템이 200억+ 레코드를 분산 저장하고, SL 노드 1개 장애 시 자동 복구하며, 초당 100만건 이상의 이벤트를 지속 수집할 수 있다. DSL 장애 시 Raft 기반 자동 Leader 선출로 15초 이내 복구된다.

**Why this priority**: 대용량 처리와 고가용성은 기반 인프라 요구사항으로, 실제 운영을 위해 필수적이다.

**Independent Test**: 10억 건 데이터로 GROUP BY 쿼리 100회 반복 실행하여 P99 ≤ 3초를 측정하고, SL 노드 1개를 kill 후 복제본으로 자동 전환되는지 확인하면 독립 검증 가능하다.

**Acceptance Scenarios**:

1. **Given** 10 SL 노드 클러스터에서 10억 행 테이블의 일별 집계 쿼리를 100회 실행하면, **Then** P50 ≤ 500ms, P99 ≤ 3,000ms 를 달성한다.

2. **Given** Kafka Routine Load가 실행 중일 때, **When** 초당 200,000건 메시지를 발행하면, **Then** 10 SL 노드 기준 초당 1,000,000건 이상 안정적으로 수집된다.

3. **Given** SL Node 1이 강제 종료(`kill -9`)되면, **Then** 복제본(Replication Factor 3)이 자동으로 서비스를 인수하고 쿼리가 중단되지 않는다.

4. **Given** DSL Leader가 장애 발생하면, **Then** Raft 프로토콜로 15초 이내 새 Leader가 선출되고 쓰기 서비스가 재개된다.

5. **Given** 200 동시 쿼리가 10분간 실행될 때, **Then** 성공률 ≥ 99.9%, P99 응답시간 ≤ 5,000ms 를 유지한다.

---

### Edge Cases

- `CREATE CUBE IF NOT EXISTS` 로 이미 존재하는 Cube를 재생성하면? → `Query OK` + Warning 반환 (오류 없음)
- `DROP CUBE` 실행 시 연관 Session MV가 존재하면? → CASCADE 없이는 오류, CASCADE 포함 시 SMV도 함께 삭제
- Session Timeout 경계에서 정확히 timeout과 같은 시간 간격의 이벤트는? → 초과(`>`)시에만 새 세션, 정확히 같으면 동일 세션 유지
- Kafka 수집 중 SL 노드 장애 발생 시? → TX Rollback + Kafka Offset 유지 → 재기동 후 재처리 (Exactly-once 보장)
- Spark 트랜잭션 도중 네트워크 파티션 발생 시? → 트랜잭션 타임아웃 후 원자적 롤백 (부분 적재 없음)
- COALESCE User Key에서 user_id가 null인 경우? → device_id를 effective key로 사용하여 별도 세션 구분
- `ALTER CUBE MODIFY COLUMN` 으로 타입을 축소하려 하면? → 오류 반환 (타입 확장만 허용)
- SMV의 원본 컬럼이 `DROP`으로 삭제되면? → SMV 자동 무효화 경고 출력
- AVX2를 지원하지 않는 CPU에서 실행하면? → 런타임 `cpuid` 감지로 최적 경로 선택 또는 지원 불가 오류
- JSON properties 컬럼에 잘못된 JSON이 삽입되면? → 스키마 검증 실패, dead letter queue로 격리

---

## Requirements *(mandatory)*

### Functional Requirements

#### FR-CUBE: Event Cube 관리

- **FR-CUBE-001**: 시스템은 `CREATE CUBE` DDL을 통해 이벤트 스키마를 정의할 수 있어야 한다. 지원 옵션: 컬럼 정의(필수), ENGINE=WOW_LSM(필수), DISTRIBUTED BY HASH(필수), PARTITION BY RANGE(선택), ORDER BY(권장), PROPERTIES(선택)
- **FR-CUBE-002**: 시스템은 TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, VARCHAR, CHAR, TEXT, DATE, DATETIME, TIMESTAMP, JSON, BOOLEAN, HLL, BITMAP 데이터 타입을 지원해야 한다.
- **FR-CUBE-003**: 시스템은 `IF NOT EXISTS` 옵션으로 중복 생성 시 오류 대신 경고를 반환해야 한다.
- **FR-CUBE-004**: 시스템은 `ALTER CUBE ADD COLUMN` 으로 운영 중 온라인 컬럼 추가를 지원해야 한다.
- **FR-CUBE-005**: 시스템은 `ALTER CUBE MODIFY COLUMN` 으로 타입 확장만 허용하고 축소는 거부해야 한다.
- **FR-CUBE-006**: 시스템은 `DROP CUBE` 시 연관 Session MV가 있으면 CASCADE 옵션을 필수 요구해야 한다.

#### FR-SMV: Session Materialized View

- **FR-SMV-001**: 시스템은 `CREATE SESSION MATERIALIZED VIEW ... FROM <cube> USER_KEY = <col> SESSION_TIMEOUT = <n> MINUTE` DDL을 지원해야 한다.
- **FR-SMV-002**: SMV 생성 시 `session_id(VARCHAR64), session_start_time(DATETIME), session_end_time(DATETIME), session_duration_sec(BIGINT), session_event_count(INT), session_seq(INT), is_new_user(BOOLEAN)` 컬럼이 자동 추가되어야 한다.
- **FR-SMV-003**: 시스템은 `REFRESH REALTIME`, `REFRESH ON DEMAND`, `REFRESH EVERY INTERVAL <n> <UNIT>` 세 가지 갱신 모드를 지원해야 한다.
- **FR-SMV-004**: 세션 경계 판단 알고리즘: 동일 User Key의 연속 이벤트 간격이 SESSION_TIMEOUT을 초과(>)하면 새 세션으로 분리해야 한다.
- **FR-SMV-005**: User Key는 단일 컬럼 또는 `COALESCE(col1, col2)` 형태의 복합 표현식을 지원해야 한다.
- **FR-SMV-006**: Web SQL Client에서 Cube 생성 완료 후 5단계 대화형 SMV 생성 흐름을 제공해야 한다: (1)생성여부 확인 → (2)User Key 선택 → (3)Session Timeout 설정 → (4)MV 이름 입력(자동제안) → (5)요약 확인 및 생성.

#### FR-QE: 쿼리 엔진

- **FR-QE-001**: 시스템은 MySQL 8.0 호환 SQL을 지원해야 한다: SELECT(FROM/WHERE/GROUP BY/HAVING/ORDER BY/LIMIT/OFFSET), JOIN(INNER/LEFT OUTER/CROSS), 집계함수(COUNT/SUM/AVG/MIN/MAX/COUNT DISTINCT), 윈도우함수(ROW_NUMBER/RANK/LAG/LEAD 등), 서브쿼리, CTE(재귀 포함), JSON함수, 날짜함수, 문자열함수.
- **FR-QE-002**: 시스템은 `FUNNEL_COUNT(<조건> STEP <n>, ..., TIME_WINDOW => INTERVAL <n> <UNIT>, STRICT => TRUE|FALSE)` 집계 함수를 지원해야 한다. STRICT=TRUE는 순서 엄격 준수, STRICT=FALSE는 중간 이탈 허용.
- **FR-QE-003**: 시스템은 `FUNNEL_ANALYSIS(SOURCE, USER_KEY, TIME_COL, STEPS, TIME_WINDOW)` 테이블 함수를 지원해야 한다.
- **FR-QE-004**: 시스템은 `COHORT_ANALYSIS(SOURCE, ENTRY_EVENT, RETURN_EVENT, COHORT_DATE, METRIC, TIME_UNIT, MAX_PERIODS, USER_KEY)` 테이블 함수를 지원해야 한다. METRIC: RETENTION_RATE | USER_COUNT | CONVERSION_RATE.
- **FR-QE-005**: 시스템은 `PATH_ANALYSIS(SOURCE, PATH_EVENT, USER_KEY, SESSION_KEY, MAX_DEPTH, MIN_SUPPORT, ENTRY_FILTER, EXIT_FILTER)` 테이블 함수를 지원해야 한다.
- **FR-QE-006**: 시스템은 WOW-DB 전용 확장 구문을 지원해야 한다: `CREATE CUBE`, `CREATE SESSION MATERIALIZED VIEW`, `REFRESH MATERIALIZED VIEW`, `CREATE ROUTINE LOAD`, `SHOW CUBES`, `SHOW SESSIONS`, `EXPLAIN PHYSICAL`, `SHOW TABLET STATUS`.

#### FR-ING: 데이터 수집

- **FR-ING-001**: 시스템은 표준 MySQL `INSERT INTO ... VALUES` 와 `INSERT INTO ... SELECT` 를 지원해야 한다. 단일 DSL 노드 기준 초당 100,000건 이상 처리.
- **FR-ING-002**: 시스템은 `CREATE ROUTINE LOAD` 로 Kafka 지속 수집을 지원해야 한다. 지원 포맷: JSON, Avro, CSV. Exactly-once 보장, SASL/SSL 지원.
- **FR-ING-003**: 시스템은 Spark DataSource API 공식 커넥터를 제공해야 한다. Spark 3.1(DataSource V1)과 Spark 3.4(DataSource V2, 구조화 스트리밍) 지원.
- **FR-ING-004**: Kafka, Spark 수집 모두 2PC(Two-Phase Commit) 트랜잭션을 지원해야 한다. 실패 시 자동 롤백, 타임아웃 기본 5분.

#### FR-ST: 스토리지 엔진

- **FR-ST-001**: Rust로 자체 LSM-Tree를 구현해야 한다: Skip List 기반 MemTable, Append-only WAL(CRC32), 블록 컬럼 저장 SSTable(Bloom Filter+LZ4/ZSTD), LRU Block Cache, Leveled Compaction, MVCC 스냅샷 격리.
- **FR-ST-002**: SIMD 가속을 구현해야 한다: 컬럼 스캔/WHERE 필터/SUM/COUNT/MIN/MAX에 AVX2, LIKE 패턴에 SSE4.2, 해시 조인/Bitmap 집계에 AVX2. 런타임 cpuid로 최적 경로 선택, AVX-512 자동 활성화.
- **FR-ST-003**: SMV는 별도 Cube로 물리 저장하며, 증분 갱신(영향 세션만 재계산)과 원본 Cube와의 메타데이터 링크를 유지해야 한다.

#### FR-WEB: Web SQL Client

- **FR-WEB-001**: SQL 편집기는 MySQL+WOW-DB 구문 코드 하이라이팅, 자동완성(테이블/컬럼/함수명), 결과 테이블 표시+CSV 다운로드, 최근 100개 쿼리 히스토리, 다중 탭, EXPLAIN PHYSICAL 시각화 트리를 제공해야 한다.
- **FR-WEB-002**: Cube Builder는 GUI 기반 컬럼 추가/삭제(드래그앤드롭), 파티션/분산 키 시각적 설정, DDL 미리보기, Session MV 생성 버튼을 제공해야 한다.
- **FR-WEB-003**: 분석 대시보드는 Funnel, Cohort, Path 분석 결과 기본 차트와 MySQL JDBC 접속 정보 내보내기를 제공해야 한다.

#### FR-COMPAT: MySQL 프로토콜 호환성

- **FR-COMPAT-001**: 시스템은 MySQL 8.0 Wire Protocol을 포트 9030에서 지원해야 한다. 인증: mysql_native_password, caching_sha2_password.
- **FR-COMPAT-002**: 시스템은 `information_schema.TABLES`, `information_schema.COLUMNS`, `SHOW DATABASES`, `SHOW TABLES`, `SHOW CREATE TABLE` 을 지원해야 한다.
- **FR-COMPAT-003**: `UPDATE` / `DELETE` 는 단일 Tablet 범위로 제한된다 (OLAP 특성).

#### FR-DIST: 분산 처리

- **FR-DIST-001**: SL 노드 추가 시 온라인 Tablet 재분배(무중단)를 지원해야 한다.
- **FR-DIST-002**: DSL Leader 장애 시 Raft 기반 자동 선출(장애 감지 3초 이내, 선출 완료 15초 이내)을 지원해야 한다.
- **FR-DIST-003**: SL 노드 1개 장애 시 복제본(Replication Factor 3)으로 자동 전환해야 한다.

---

### Key Entities

- **Event**: 웹 서비스 사용자의 단일 행동 단위. 필수 속성: event_time, event_name, user 식별자. 선택 속성: page_url, properties(JSON), country_code, revenue.
- **Cube**: 이벤트 데이터 저장 논리 스키마 단위. 속성: 컬럼 정의, 파티션 전략, 분산 키, 복제본 수, 압축 알고리즘.
- **Tablet**: Cube 데이터를 물리 저장하는 최소 단위. SL 노드에 분산되며 기본 3개 복제본 유지.
- **Session Materialized View (SMV)**: Cube에서 자동 생성된 세션화 뷰. 속성: 원본 Cube 컬럼 + 7개 자동 세션 컬럼, User Key, Session Timeout, 갱신 모드.
- **Session**: 동일 User Key의 연속 이벤트 묶음. 속성: session_id, session_start_time, session_end_time, session_duration_sec, session_event_count.
- **Routine Load Job**: Kafka 지속 수집 작업. 속성: Kafka 연결 설정, 포맷, 병렬 Consumer 수, Offset 상태.
- **DSL Node**: 클라이언트 요청 진입점. 역할: Protocol Handler, Query Planner, Coordinator, Cube Manager, Session Manager, Ingestion Gateway, Transaction Manager.
- **SL Node**: 실제 데이터 저장 및 쿼리 실행 노드. 역할: LSM-Tree Engine, SIMD Executor, MV Manager, Compaction Service, Block Cache, WAL.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 단일 DSL 노드에서 초당 100,000건 이상의 MySQL INSERT 요청을 처리할 수 있다.
- **SC-002**: 10 SL 노드 클러스터에서 초당 1,000,000건 이상의 이벤트를 Kafka Routine Load로 수집할 수 있다.
- **SC-003**: 10억 행 테이블에 대한 일별 GROUP BY 집계 쿼리의 P50 응답시간이 500ms 이하, P99 응답시간이 3,000ms 이하이다.
- **SC-004**: 10억 행, 4단계 Funnel 쿼리의 응답시간이 10초 이하이다.
- **SC-005**: 200 동시 쿼리 요청을 처리할 때 성공률 99.9% 이상, P99 응답시간 5,000ms 이하를 유지한다.
- **SC-006**: 200억+ 레코드를 분산 클러스터에 저장하고 정상적으로 쿼리할 수 있다.
- **SC-007**: SL 노드 1개 장애 시 복제본 자동 전환으로 쿼리 중단 없이 서비스가 유지된다.
- **SC-008**: DSL Leader 장애 시 15초 이내 새 Leader 선출로 쓰기 서비스가 재개된다.
- **SC-009**: 가용성 99.99% (연간 다운타임 52분 이하).
- **SC-010**: WAL 기반 RPO ≤ 1분, RTO ≤ 30초.
- **SC-011**: MySQL 8.0 CLI, JDBC 8.x, MySQL Workbench, DBeaver가 추가 드라이버 없이 포트 9030으로 연결되어 정상 동작한다.
- **SC-012**: Session Timeout 경계 정확도: 테스트용 이벤트 시퀀스에서 세션 분리/유지 결과가 100% 기대 값과 일치한다.
- **SC-013**: Spark 트랜잭션 실패 시 부분 데이터가 0건으로 원자적으로 롤백된다.
- **SC-014**: 분석가가 Cube 생성부터 SMV 생성까지 Web UI 대화 흐름으로 5분 이내 완료할 수 있다.

---

## Assumptions

- 운영 환경은 Linux(Ubuntu 20.04+ 또는 RHEL 8+) 또는 Docker/Kubernetes 컨테이너 환경이다.
- 구현 언어는 Rust(stable toolchain, edition 2021 이상)이며, x86-64 아키텍처(AVX2 지원 CPU)를 전제로 한다.
- DSL 노드 최소 사양: 16GB RAM. SL 노드 최소 사양: 64GB RAM + NVMe SSD. 네트워크: 10GbE 이상.
- 최소 운영 구성: DSL 1 Leader + 2 Follower, SL 3 노드.
- OLTP 워크로드(빈번한 단건 UPDATE/DELETE)는 지원하지 않는다.
- 지리공간(GIS) 분석 및 머신러닝 학습은 범위 외이다.
- MySQL 클라이언트 호환을 위해 MySQL Wire Protocol 8.0을 기준으로 하며, MySQL 5.x 클라이언트 지원은 명시적으로 보장하지 않는다.
- Kafka 연동 시 Kafka 2.x 이상을 전제로 한다.
- Spark 커넥터는 Spark 3.1(DataSource V1)과 Spark 3.4(DataSource V2) 두 버전을 별도 아티팩트로 제공한다.
- `UPDATE` / `DELETE` 는 단일 Tablet 범위로 제한되어 광범위한 OLTP 업데이트에는 적합하지 않다.
- TLS 설정은 클라이언트 ↔ DSL, DSL ↔ SL 모든 통신에 적용 가능하나, 내부 테스트 환경에서는 비활성화 가능하다.
- 별도 ZooKeeper 없이 내장 Raft 기반 Key-Value Store로 DSL 메타데이터를 관리한다.
- Web SQL Client는 모던 브라우저(Chrome, Firefox, Edge 최신 버전)를 대상으로 한다.
- SRS v0.1 및 TC v0.1 문서는 이 스펙의 기준 입력 문서이며, 구현 세부사항(Rust 크레이트 선택, 내부 포맷 등)은 설계/구현 단계에서 확정된다.
