# 기능 명세: WOW-DB 웹 분석 데이터베이스 플랫폼

**기능 브랜치**: `002-wow-db-srs-v02`  
**작성일**: 2026-04-12  
**상태**: 초안 (Draft)  
**입력**: "WOW-DB SRS v0.2 — 이벤트 기반 데이터 수집·저장·분석, 자동 세션화, 내장 Funnel/Cohort/Path 분석, Storage-Compute 분리를 갖춘 MySQL 호환 OLAP 데이터베이스"

---

## 핵심 워크플로우: 이벤트 수집 → Behavioral Table → 자동 라우팅 분석

WOW-DB의 가장 중요한 설계 철학은 **두 개의 물리 레이아웃(Event Table / Behavioral Table)을 유지하면서, 엔진이 쿼리 패턴을 분석하여 자동으로 최적 레이아웃을 선택하는 3단계 구조**이다.

웹 분석에서 "table"란 특정 목적으로 구조화된 데이터 덩어리를 가리키는 일반적인 OLAP 용어이다. WOW-DB는 이 개념을 두 종류의 물리 테이블로 구체화한다.

---

### 1단계: Event Table (이벤트 원시 저장소)

사용자가 발생시키는 모든 행동(page_view, click, purchase 등)을 **시계열 순서 그대로** 저장하는 테이블이다. `CREATE TABLE` DDL로 정의하며, WOW-DB의 스토리지 엔진(LSM-Tree, 컬럼 지향, 파티션 분산)이 그대로 적용된다.

```sql
CREATE TABLE page_events (
    event_time   DATETIME     NOT NULL,
    device_id    VARCHAR(64)  NOT NULL,
    event_name   VARCHAR(128) ENCODING(DICT),
    properties   JSON
)
PARTITION BY RANGE(event_time) INTERVAL DAY AUTO
ORDER BY (device_id, event_time)
DISTRIBUTED BY HASH(device_id) BUCKETS 32;
```

- **저장 단위**: 개별 이벤트 1건 = 1행
- **정렬 기준**: `(device_id, event_time)` — 같은 사용자의 이벤트가 물리적으로 인접하게 정렬되어 세션 계산에 유리
- **분산 기준**: `HASH(device_id)` — 같은 사용자의 이벤트가 동일 Shard에 모임
- **최적 워크로드**: 날짜별 이벤트 수 집계, 이벤트 유형별 집계, 원시 이벤트 조회

### 2단계: Behavioral Table (행동 분석 파생 테이블)

Event Table로부터 자동 생성되는 **세션 단위 파생 테이블**이다. 동일 사용자(`device_id`)의 연속 이벤트를 시간 간격 기준으로 묶어 세션 ID를 부여하고, 세션 시작/종료 시각, 이벤트 시퀀스를 계산한다.

`CREATE SESSION MATERIALIZED VIEW` DDL로 생성하며, Funnel/Cohort/Path 분석에 최적화된 물리 레이아웃을 갖는다.

```sql
CREATE SESSION MATERIALIZED VIEW page_events_sessions
FROM page_events
USER KEY device_id
SESSION TIMEOUT 30 MINUTES
REFRESH INCREMENTAL;
```

- **자동 생성 컬럼**: `session_id`, `session_start`, `session_end`, `event_sequence`
- **세션 분리 기준**: 연속 이벤트 간 간격이 `SESSION TIMEOUT`을 초과하면 새 세션
- **갱신 방식**: `INCREMENTAL` — 신규 이벤트 파티션만 증분 처리
- **최적 워크로드**: Funnel 전환율, Cohort 재방문율, Path 이동 경로 분석

### 전체 흐름

```
[ 원시 이벤트 수집 ]
  Kafka / Spark / MySQL INSERT
          │
          ▼
┌─────────────────────────┐
│   Event Table (Table)    │  ← CREATE TABLE
│  device_id, event_time  │     이벤트 1건 = 1행
│  event_name, properties │     LSM + 컬럼 지향 저장
└───────────┬─────────────┘
            │  CREATE SESSION MATERIALIZED VIEW
            │  (gap-based 세션 윈도우 자동 계산)
            ▼
┌──────────────────────────────────────────┐
│   Behavioral Table (Session MV)          │  ← 파생 테이블 (BT)
│  session_id, session_start, session_end  │     세션 단위로 집약
│  device_id, event_sequence               │     Behavioral Query의 최적 대상
└───────────┬──────────────────────────────┘
            │
     ┌──────┴───────┐────────────────┐
     ▼              ▼                ▼
FUNNEL_COUNT  COHORT_ANALYSIS  PATH_ANALYSIS
(전환율)        (재방문율)         (이벤트 경로)
```

**두 테이블의 관계 요약**:

| 구분 | Event Table | Behavioral Table |
|---|---|---|
| 약어 | ET | BT |
| 생성 방법 | `CREATE TABLE` | `CREATE SESSION MATERIALIZED VIEW ... FROM <event_table>` |
| 저장 단위 | 이벤트 1건 = 1행 | 세션 1개 = 1행 |
| 주요 용도 | 수집, 이벤트 롤업, 원시 이벤트 조회 | Funnel / Cohort / Path 행동 분석 |
| 데이터 갱신 | INSERT 즉시 | INCREMENTAL 또는 SCHEDULED |
| 의존 관계 | 독립 | Event Table 없이 생성 불가 |
| 쿼리 분류 | Event Query | Behavioral Query |

### 3단계: Behavioral Routing — 자동 쿼리 라우팅 (WOW-DB 최대 차별화 요소)

분석가는 항상 `FROM page_events` (Event Table)만 사용한다. WOW-DB의 Behavioral Router가 쿼리 내용을 분석하여 어떤 물리 레이아웃이 최적인지를 **seamlessly** 결정한다.

```
분석가가 작성하는 것:                     WOW-DB Behavioral Router가 결정:
──────────────────────────────────────────────────────────────────────────
SELECT FUNNEL_COUNT(...)            →  Behavioral Table 사용
FROM page_events                       (세션 경계 사전 계산됨, 4–10× 빠름)

SELECT COHORT_ANALYSIS(...)         →  Behavioral Table 사용
FROM page_events                       (재방문 패턴, BT가 최적)

SELECT event_name, COUNT(*)         →  Event Table 사용
FROM page_events GROUP BY event_name   (단순 집계, BT 불필요)

SELECT COUNT(*) FROM page_events    →  Event Table 사용
WHERE DATE(event_time) = YESTERDAY     (시계열 카운트)
```

**Behavioral Routing 결정 규칙**:
- `FUNNEL_COUNT`, `COHORT_ANALYSIS`, `PATH_ANALYSIS` 함수 → Behavioral Table 라우팅
- `session_id`, `session_start`, `session_end`, `event_sequence` 컬럼 참조 → Behavioral Table 라우팅
- 단순 집계, 이벤트 수 카운트, 원시 이벤트 조회 → Event Table 유지
- Behavioral Table이 없거나 갱신 지연 상태 → Event Table로 자동 Fallback + Behavioral Guidance 힌트

**Behavioral Guidance** — Behavioral Table이 없을 때, 시스템이 자동으로 생성을 권장한다:
```
Warning: No Behavioral Table found for 'page_events'.
Query took 4,200ms. Estimated 420ms with Behavioral Table.
Suggestion: CREATE SESSION MATERIALIZED VIEW page_events_sessions
            FROM page_events USER KEY device_id SESSION TIMEOUT 30 MINUTE;
```

웹 분석에서 Funnel/Cohort/Path는 필수 워크로드이므로, WOW-DB는 자연스럽게 Behavioral Table 생성을 유도한다. 한번 생성하면 이후 모든 Behavioral Query가 자동으로 최적 레이아웃을 사용한다.

상세 설계: `specs/002-wow-db-srs-v02/design/query-routing-smv.md`

---

## 사용자 시나리오 및 테스트 *(필수)*

<!--
  각 사용자 스토리는 독립적으로 테스트 가능한 최소 가치 단위(MVP)이며,
  중요도 순서(P1 최우선)로 정렬한다.
-->

### 사용자 스토리 1 - 웹 이벤트 분석 쿼리 (우선순위: P1)

데이터 분석가는 수십억 건의 웹 이벤트가 저장된 WOW-DB에서 퍼널(Funnel) 전환율, 코호트(Cohort) 재방문율, 사용자 내비게이션 경로(Path)를 친숙한 SQL 문법으로 분석해야 한다.

**이 우선순위인 이유**: WOW-DB가 존재하는 핵심 이유이다. 이벤트 데이터에 대한 Funnel·Cohort·Path 분석이 없으면 범용 OLAP 데이터베이스 대비 차별화 가치가 없다.

**독립 테스트**: MySQL 클라이언트로 WOW-DB에 접속하여 샘플 이벤트 데이터를 적재한 뒤 FUNNEL_COUNT, COHORT_ANALYSIS, PATH_ANALYSIS 쿼리를 실행하면 독립적으로 검증할 수 있다.

**인수 시나리오**:

1. **Given** page_view·add_to_cart·purchase 이벤트가 포함된 Table, **When** 순서가 정해진 단계 조건과 시간 윈도우를 지정하여 FUNNEL_COUNT 쿼리를 실행하면, **Then** 각 퍼널 단계에 도달한 사용자 수가 반환된다.
2. **Given** 30일 이상의 이벤트 데이터가 있는 Table, **When** 최초 이벤트 날짜를 기준으로 사용자를 그룹화하는 COHORT_ANALYSIS 쿼리를 실행하면, **Then** 코호트별·기간별 재방문율이 반환된다.
3. **Given** 세션 기반 이벤트 뷰, **When** PATH_ANALYSIS 쿼리를 실행하면, **Then** 사용자들이 실제로 따른 가장 빈도 높은 이벤트 시퀀스 패턴이 반환된다.
4. **Given** MySQL Workbench, JDBC 드라이버, MySQL CLI 중 어떤 클라이언트에서든 위 쿼리를 제출하면, **Then** 표준 MySQL 결과셋 형식으로 결과가 반환된다.

---

### 사용자 스토리 2 - 대용량 이벤트 데이터 수집 (우선순위: P1)

백엔드 엔지니어는 Kafka 토픽, Spark 잡(Job), 또는 MySQL INSERT 구문을 통해 하루 수억 건의 웹 이벤트를 WOW-DB로 지속적으로 스트리밍해야 한다. 별도의 커스텀 파이프라인 구축 없이 가능해야 한다.

**이 우선순위인 이유**: 데이터가 없으면 분석도 없다. 복수 소스에서 신뢰할 수 있는 고처리량 수집은 모든 기능의 전제 조건이다.

**독립 테스트**: Kafka 프로듀서를 WOW-DB에 연결하고 고용량 이벤트를 전송한 뒤, 수집된 이벤트가 즉시 쿼리 가능한지 확인하면 독립적으로 검증할 수 있다.

**인수 시나리오**:

1. **Given** JSON 형식의 웹 이벤트를 수신하는 Kafka 토픽, **When** 해당 토픽을 가리키는 상시 수집 잡을 설정하면, **Then** WOW-DB가 이벤트를 실시간으로 수집하여 쿼리 가능하게 만들고, 브로커·네트워크 장애 시 데이터 손실·중복 없이 자동으로 복구한다.
2. **Given** 이벤트 데이터셋을 생성하는 Spark 잡, **When** Spark 커넥터가 배치를 WOW-DB에 제출하면, **Then** 데이터가 원자적으로(전부 커밋 또는 전부 롤백) 처리되어 커밋 직후 즉시 쿼리 가능해진다.
3. **Given** MySQL 프로토콜로 고빈도 소형 INSERT를 수행하는 애플리케이션, **When** 버퍼 삽입 모드가 활성화되면, **Then** 시스템이 INSERT를 누적하여 효율적인 배치로 플러시하며, 장애나 종료 시에도 데이터를 손실하지 않는다.
4. **Given** 배치 수집 도중 일시적 장애(네트워크 오류, 노드 재시작)가 발생하면, **Then** 부분 배치는 커밋되지 않으며, 장애 해소 후 마지막 확인된 체크포인트부터 수집이 자동으로 재개된다.

---

### 사용자 스토리 3 - Behavioral Table 생성 유도 및 Web UI 마법사 (우선순위: P2)

복잡한 SQL을 작성하기 어려운 데이터 분석가는 Web UI 마법사를 이용해 Event Table로부터 Behavioral Table을 생성해야 한다. 또한 시스템이 첫 Behavioral Query 실행 시 자동으로 Behavioral Table 생성을 유도해야 한다.

**이 우선순위인 이유**: Behavioral Table은 WOW-DB의 핵심 차별화 요소이며, 웹 분석에서 Funnel/Cohort/Path를 올바르게 실행하기 위한 필수 레이아웃이다. Event Table(P1) 생성에 의존하므로 P2이다.

**독립 테스트**: (1) 첫 FUNNEL_COUNT 쿼리 실행 시 Behavioral Guidance 힌트가 표시되는지, (2) Web UI 마법사로 Behavioral Table을 생성 후 동일 쿼리가 자동으로 Behavioral Table을 사용하는지 EXPLAIN으로 확인하면 독립적으로 검증할 수 있다.

**인수 시나리오**:

1. **Given** Behavioral Table이 없는 상태에서 분석가가 FUNNEL_COUNT 쿼리를 실행하면, **Then** 시스템이 Event Table로 Fallback하여 결과를 반환하면서, 응답에 `"Behavioral Table을 생성하면 약 X배 빨라집니다"` Guidance 힌트와 권장 DDL을 포함한다.
2. **Given** Table이 생성된 상태, **When** 분석가가 Web UI에서 "Behavioral Table 생성" 마법사를 실행하면, **Then** 시스템이 User Key 컬럼(드롭다운) 선택과 Session Timeout 값을 안내하고 DDL 미리보기를 표시한다.
3. **Given** 분석가가 DDL 미리보기를 확정하면, **Then** 시스템이 Behavioral Table을 구체화하고 결과 데이터(`session_id`, `event_sequence` 포함) 샘플을 보여준다.
4. **Given** Behavioral Table 생성 완료 후 분석가가 동일한 FUNNEL_COUNT 쿼리를 다시 실행하면, **Then** 쿼리 수정 없이 자동으로 Behavioral Table을 사용하며, 응답 시간이 이전 대비 유의미하게 개선된다.
5. **Given** Behavioral Table 생성 후 소스 Event Table에 새 이벤트가 도착하면, **Then** 예약된 갱신이 실행될 때 새 이벤트가 Behavioral Table에 반영된다.

---

### 사용자 스토리 4 - 이벤트 스키마(Table) 정의 (우선순위: P2)

데이터 엔지니어는 SQL DDL을 이용해 웹 이벤트의 스키마(컬럼, 데이터 타입, 파티션 전략, 스토리지 설정)를 명시적으로 정의해야 한다.

**이 우선순위인 이유**: 스키마 정의는 분석의 기반이 되는 초기 설정으로, 한 번 수행되는 전제 조건 작업이다. 지속적인 사용자 가치를 직접 제공하지 않으므로 P1 분석보다 낮다.

**독립 테스트**: MySQL 클라이언트에서 CREATE TABLE 구문을 실행하고, 샘플 행을 삽입한 뒤 쿼리 가능함을 확인하면 독립적으로 검증할 수 있다.

**인수 시나리오**:

1. **Given** 데이터 엔지니어가 컬럼 정의, 파티션 키, 분산 전략을 포함한 CREATE TABLE 구문을 작성하면, **When** 이를 실행하면, **Then** 스키마가 등록되어 즉시 데이터를 받을 수 있게 된다.
2. **Given** 기존 Table에 새 컬럼이 필요한 경우, **When** ALTER TABLE 구문으로 컬럼을 추가하면, **Then** 데이터 손실 없이 컬럼이 추가되고 기존 행은 새 컬럼에 NULL을 갖는다.
3. **Given** 시간 기반 파티셔닝이 설정된 Table, **When** 기존 파티션이 커버하지 않는 날짜로 이벤트가 도착하면, **Then** 관리자 개입 없이 새 파티션이 자동으로 생성된다.
4. **Given** Table에 JSON properties 컬럼이 있고 시간이 지남에 따라 데이터가 적재되면, **When** 시스템이 특정 JSON 키가 레코드 전반에 걸쳐 자주 출현함을 감지하면, **Then** 해당 키들이 일반 컬럼과 동일한 필터링·집계 성능을 갖는 독립 컬럼으로 쿼리 가능해진다.

---

### 사용자 스토리 5 - MySQL 클라이언트 호환성 (우선순위: P2)

분석 팀은 이미 다른 데이터베이스에 MySQL Workbench, Python mysql-connector, Tableau(MySQL 드라이버)를 사용하고 있다. 코드 변경이나 새 드라이버 설치 없이 WOW-DB에 연결하고 쿼리할 수 있어야 한다.

**이 우선순위인 이유**: 도입 장벽을 크게 낮춘다. 분석가들이 기존 도구로 즉시 WOW-DB를 사용할 수 있다. 핵심 분석 기능에 비해 부차적이다.

**독립 테스트**: MySQL Workbench와 표준 mysql-connector를 사용하는 Python 스크립트를 WOW-DB에 연결하고 표준 SQL을 실행하면 독립적으로 검증할 수 있다.

**인수 시나리오**:

1. **Given** WOW-DB의 호스트와 포트로 설정된 MySQL 클라이언트, **When** 표준 MySQL 사용자명과 비밀번호로 연결하면, **Then** 드라이버 수정이나 특별한 설정 없이 연결이 성공한다.
2. **Given** 연결된 MySQL 클라이언트, **When** 표준 SQL 구문(SELECT, INSERT, SHOW TABLES, DESCRIBE)을 실행하면, **Then** 결과가 표준 MySQL 결과셋 형식으로 반환된다.
3. **Given** 표준 MySQL JDBC 드라이버로 설정된 JDBC 기반 BI 도구, **When** WOW-DB에 연결하고 Table을 쿼리하면, **Then** 오류 없이 데이터와 스키마 정보가 올바르게 표시된다.

---

### 사용자 스토리 7 - 데이터 분포 가시성 (우선순위: P2)

플랫폼 엔지니어와 데이터 엔지니어는 각 Table의 데이터가 Storage Node들에 어떻게 분산되어 있는지, 어떤 파티션에 얼마나 많은 데이터가 있는지, 어떤 분산 키로 어떤 Shard가 어느 SN에 위치하는지, 그리고 각 Shard의 LSM Part 수와 크기를 실시간으로 확인해야 한다.

**이 우선순위인 이유**: 운영 중 데이터 불균형 감지, Compaction 상태 파악, Hot Shard 진단, 파티션 프루닝 효과 확인에 필수적이다. StarRocks의 `SHOW PARTITIONS`나 ClickHouse의 `system.parts` 테이블이 제공하는 수준의 운영 가시성이 요구된다.

**독립 테스트**: `SHOW PARTITIONS FROM page_events` 실행 후 각 파티션의 row_count, size_bytes, shard_count가 실제 데이터와 일치하는지 확인하면 독립적으로 검증할 수 있다.

**인수 시나리오**:

1. **Given** 데이터가 로딩된 Table `page_events`, **When** `SHOW PARTITIONS FROM page_events`를 실행하면, **Then** 각 파티션의 ID, 키 범위, row_count, size_bytes, shard_count, 티어 상태가 반환된다.
2. **Given** 분산 키 `device_id`로 정의된 Table, **When** `SHOW SHARDS FROM page_events`를 실행하면, **Then** 각 Shard의 shard_id, 소속 Storage Node, bucket_id, 역할(Leader/Follower), row_count, LSM part_count가 반환된다.
3. **Given** 특정 파티션, **When** `SHOW PARTS FROM page_events PARTITION <partition_id>`를 실행하면, **Then** 해당 파티션의 모든 LSM Part 목록과 레벨, sort key 범위, 크기, Bloom Filter 크기가 반환된다.
4. **Given** 클러스터 운영자, **When** `SHOW DISTRIBUTED STATUS FROM page_events`를 실행하면, **Then** 각 SN별 Shard 수, 총 데이터 크기, 평균 Part 수가 반환되어 데이터 편중 여부를 즉시 파악할 수 있다.

---

### 사용자 스토리 6 - 클러스터 모니터링 및 쿼리 프로파일링 (우선순위: P3)

WOW-DB 클러스터를 관리하는 플랫폼 엔지니어는 노드 상태를 모니터링하고, 리소스 사용을 추적하며, 느린 쿼리를 진단하여 성능 SLA를 유지하고 장애를 신속하게 해결해야 한다.

**이 우선순위인 이유**: 운영 도구는 프로덕션 안정성에 중요하지만 핵심 분석 기능을 막지는 않는다. 핵심 데이터·쿼리 경로가 안정화된 후에 다룬다.

**독립 테스트**: 일련의 쿼리를 실행한 뒤 Web 모니터링 대시보드와 Query Profiler에 접근하면 독립적으로 검증할 수 있다.

**인수 시나리오**:

1. **Given** 실행 중인 WOW-DB 클러스터, **When** 운영자가 Web UI의 모니터링 대시보드에 접근하면, **Then** 클러스터의 모든 노드 상태(정상/저하/오프라인), 리소스 사용률, 역할을 볼 수 있다.
2. **Given** 시스템에 쿼리가 실행된 후, **When** 운영자가 Query Profiler를 열면, **Then** 최근 1,000건의 쿼리(SQL 텍스트, 총 실행 시간, 처리 행 수, 노드별 처리 분석 포함)를 조회할 수 있다.
3. **Given** Profiler에서 느린 쿼리가 식별되면, **When** 운영자가 해당 쿼리를 선택하면, **Then** 파싱·플래닝·스캔·집계·네트워크 중 어떤 단계가 병목인지 확인할 수 있다.
4. **Given** 모니터링 서비스가 실행 중일 때, **When** 외부 모니터링 도구(Prometheus, Grafana)가 메트릭 엔드포인트를 스크레이핑하면, **Then** 호환 형식으로 모든 클러스터 상태 메트릭을 수신한다.

---

### 엣지 케이스

- Table 스키마에 정의되지 않은 필드를 가진 이벤트가 Kafka 토픽을 통해 유입되면 어떻게 처리하는가?
- Session MV 갱신 중 소스 Table에 동시에 쓰기가 발생하면 어떻게 처리하는가?
- 쿼리 실행 도중 Data Node가 오프라인이 되면 실행 중인 쿼리는 어떻게 처리되는가?
- 파티션의 TTL이 만료되면 데이터가 즉시 삭제되는가, 아니면 다음 예약된 Compaction 시점에 삭제되는가?
- 극단적인 지속 쓰기 부하에서 버퍼 삽입 대기열이 용량에 도달하면 어떻게 되는가?
- JSON 컬럼에 깊게 중첩된 구조가 포함된 경우 자동 컬럼 승격(Flat JSON) 시 어떻게 처리하는가?
- Query Node 장애 시 진행 중인 2PC 트랜잭션은 어떻게 복구되는가?
- HDFS 백엔드에서 Kerberos 티켓이 만료되면 진행 중인 읽기·쓰기 작업은 어떻게 처리되는가?

---

## 요구사항 *(필수)*

### 기능 요구사항

- **FR-000**: **Behavioral Routing — 자동 쿼리 라우팅 (핵심 차별화 요소)** — 시스템은 사용자가 항상 Event Table(Table)에 쿼리를 작성하더라도, Behavioral Router가 쿼리 패턴을 분석하여 Event Query 또는 Behavioral Query 중 최적의 물리 레이아웃으로 자동 라우팅해야 한다. `FUNNEL_COUNT`, `COHORT_ANALYSIS`, `PATH_ANALYSIS` 함수 사용 또는 Behavioral Table 전용 컬럼(`session_id`, `session_start`, `session_end`, `event_sequence`) 참조가 감지되면 Behavioral Table(Session MV)로 라우팅한다. Behavioral Table이 없거나 갱신 지연(STALE) 상태이면 오류 없이 Event Table로 Fallback하며 Behavioral Guidance 힌트를 제공한다. 상세 설계: `specs/002-wow-db-srs-v02/design/query-routing-smv.md` 참조.
- **FR-001**: 사용자는 컬럼명, 데이터 타입, 파티션 키, 정렬 순서, 분산 전략을 지정하는 SQL DDL(`CREATE TABLE`)을 사용하여 이벤트 스키마("Table")를 정의할 수 있어야 한다.
- **FR-002**: 시스템은 수신 데이터가 기존 파티션이 커버하지 않는 날짜 범위에 해당할 때 시간 기반 파티션을 자동으로 생성해야 한다(`Auto Partition`).
- **FR-003**: 사용자는 JSON 또는 Avro 형식의 Apache Kafka 토픽으로부터 상시 수집(`Routine Load`)을 설정할 수 있어야 하며, 자동 장애 복구와 정확히 한 번(exactly-once) 전달을 보장해야 한다.
- **FR-004**: 사용자는 Apache Spark(버전 3.1 및 3.4) 잡에서 이벤트 데이터를 단일 원자적 트랜잭션으로 Table에 적재할 수 있어야 한다.
- **FR-005**: 시스템은 이벤트 수집을 위한 표준 MySQL INSERT 구문을 받아야 하며, 고빈도 소형 INSERT를 누적하여 효율적인 배치로 플러시하는 설정 가능한 버퍼 모드(`Async INSERT`)를 제공해야 한다.
- **FR-006**: 시스템은 설정 가능한 시간 윈도우 내에서 정해진 순서의 이벤트 타입 조건에 걸친 사용자 전환율을 계산하는 `FUNNEL_COUNT` 함수를 제공해야 한다.
- **FR-007**: 시스템은 최초 자격 이벤트(코호트 진입)를 기준으로 사용자를 그룹화하고 이후 기간에 걸친 행동을 추적하는 `COHORT_ANALYSIS` 함수를 제공해야 한다.
- **FR-008**: 시스템은 사용자들이 따르는 가장 빈도 높은 이벤트 시퀀스를 발견하고 순위를 매기는 `PATH_ANALYSIS` 함수를 제공해야 한다.
- **FR-009**: 사용자는 User Key 컬럼과 Session Timeout 기간을 지정하여 Event Table로부터 Behavioral Table(`CREATE SESSION MATERIALIZED VIEW`)을 생성할 수 있어야 하며, 시스템이 자동으로 이벤트를 세션으로 그룹화하여 Funnel/Cohort/Path 분석에 최적화된 물리 레이아웃을 생성해야 한다.
- **FR-010**: Web UI는 Behavioral Table 생성을 위한 대화형 마법사를 제공해야 하며, 드롭다운을 통한 컬럼 선택, 사전 설정 타임아웃 옵션, 변경 확정 전 DDL 미리보기 단계를 포함해야 한다. 첫 Behavioral Query 실행 시 Behavioral Table이 없으면 마법사를 자동으로 제안해야 한다 (Behavioral Guidance, FR-NEW-001-07 참조).
- **FR-011**: 시스템은 MySQL 8.0 호환 클라이언트(CLI 도구, GUI 클라이언트, JDBC 드라이버, Python 커넥터, BI 도구)로부터의 연결을 받아야 한다.
- **FR-012**: 사용자는 각 Data Node의 물리 스토리지를 로컬 디스크, S3 호환 오브젝트 스토리지, 또는 HDFS(Kerberos 인증 필수)로 설정할 수 있어야 한다.
- **FR-013**: 사용자는 Table 데이터로부터 GROUP BY 집계를 사전 계산하여 반복적인 대시보드·리포팅 쿼리를 가속하는 사전 집계 Materialized View(`Pre-aggregation MV`)를 정의할 수 있어야 한다.
- **FR-014**: 사용자는 파티션 수준에서 데이터 만료 정책(`TTL`)을 정의하여, 지정된 기간이 지난 데이터가 백그라운드 유지보수 과정에서 자동으로 제거되도록 할 수 있어야 한다.
- **FR-015**: 사용자는 지정된 기간보다 오래된 데이터가 수동 개입 없이 빠른 로컬 스토리지에서 저비용 오브젝트 스토리지로 자동 마이그레이션되도록 스토리지 티어링(`Tiered Storage`)을 설정할 수 있어야 한다.
- **FR-016**: 시스템은 로컬 소프트웨어 설치 없이 쿼리를 작성·실행하고 결과를 볼 수 있는 브라우저 기반 SQL 에디터를 제공해야 한다.
- **FR-017**: 시스템은 최근 1,000건의 쿼리 실행(SQL 텍스트, 총 소요 시간, 처리 행 수, 노드별 처리 메트릭 포함)을 기록하는 Query Profiler를 유지해야 한다.
- **FR-018**: 시스템은 표준 모니터링 플랫폼(Prometheus 등)과 호환되는 엔드포인트(`/metrics`)를 통해 클러스터 상태 메트릭(노드 상태, 리소스 사용률, 쿼리 처리량, 수집 속도)을 노출해야 한다.
- **FR-019**: 사용자는 외부 스토리지 시스템(S3, HDFS, Hive 호환 카탈로그, Iceberg 테이블)의 데이터를 데이터 임포트 없이 네이티브 Table처럼 쿼리할 수 있어야 한다(`External Table`).
- **FR-020**: 관리자는 사용자 또는 역할(Role)별로 CPU 점유율, 메모리 사용량, 동시 쿼리 수, 최대 쿼리 실행 시간 제한을 설정하는 리소스 정책(`Resource Group`)을 정의할 수 있어야 한다.
- **FR-021**: 사용자는 Table에 JSON 컬럼을 정의할 수 있어야 하며, 시스템은 백그라운드 처리(`Flat JSON`)를 통해 자주 출현하는 JSON 키를 자동으로 식별하여 SIMD 스캔·Bloom Filter·CBO 통계가 적용되는 효율적인 하위 컬럼으로 쿼리 가능하게 만들어야 한다.
- **FR-022**: 시스템은 Granule 단위로 MINMAX, BLOOM_FILTER, SET, NGRAMBF_V1 인덱스를 지원하여 불필요한 Granule 읽기를 건너뛸 수 있어야 한다(`Data Skipping Index`).
- **FR-023**: Query Node는 홀수 개(최소 3개)로 구성되는 Raft 클러스터를 형성하여 메타데이터 고가용성을 보장해야 하며, 모든 QN이 동일한 복제 메타데이터를 보유하여 K8s LoadBalancer의 무작위 라우팅을 지원해야 한다.
- **FR-024**: 시스템은 Hash Join의 Build Side에서 생성한 Bloom/In-list/MinMax 필터를 Probe Side CN 및 DN 스캔에 동적으로 전파하여 스캔량을 감소시키는 Runtime Filter를 지원해야 한다.
- **FR-025**: 시스템은 동일한 논리 플랜과 파티션 버전에 대한 Tablet 단위 집계 결과를 CN 메모리에 캐시하는 Query Result Cache를 제공해야 한다.

### LSM 엔진 동작 요구사항

- **FR-026**: `CREATE TABLE`의 `ORDER BY` 절(Sort Key)은 최대 4개 컬럼을 허용하며, 직렬화된 키 크기가 128 bytes를 초과하면 DDL 에러를 반환해야 한다. JSON 타입 컬럼은 Sort Key에 사용할 수 없다.
- **FR-027**: LSM Compaction은 파티션 경계 내에서만 발생해야 한다. 서로 다른 파티션의 SSTable을 병합하는 Compaction은 절대 발생하지 않아야 한다.
- **FR-028**: Leveled Compaction에서 Level-1 이상의 동일 레벨 내 SSTable들은 Sort Key 범위가 서로 겹치지 않는 불변 조건을 항상 만족해야 한다. Level-0 SSTable은 key range 겹침이 허용된다.
- **FR-029**: Storage Node는 SSTable당 Bloom Filter를 유지해야 하며, Compaction으로 새 SSTable이 생성될 때 이전 Bloom Filter를 재사용하지 않고 출력 레코드 기반으로 새로 빌드해야 한다.
- **FR-030**: 관리자는 `OPTIMIZE TABLE <table_name> FORCE` 명령으로 특정 Table의 파티션별 Full Compaction(모든 레벨 → L6 단일 통합)을 명시적으로 트리거할 수 있어야 한다.
- **FR-031**: SSTable 물리 파일(`.col`, `.bloom`, `.min_max`)은 파일 헤더에 magic bytes, 포맷 버전 번호, SSTable sequence_num을 포함해야 하며, 버전 불일치 시 로딩을 거부해야 한다.
- **FR-032**: Storage Node는 MANIFEST 파일을 통해 현재 활성 SSTable 전체 목록을 원자적으로 관리해야 하며, 크래시 복구 시 MANIFEST만으로 전체 LSM 상태를 복원할 수 있어야 한다.

### Query Node 메타데이터 일관성 요구사항

- **FR-033**: **단일 시스템 이미지(Single System Image) 보장** — 클라이언트가 어떤 Query Node에 접속하더라도 동일한 Table 스키마, 파티션 목록, Tablet 위치 정보, CBO 통계를 조회할 수 있어야 한다. DDL(CREATE/ALTER/DROP TABLE)이 Raft quorum 쓰기로 커밋되면, 해당 변경은 이후 어떤 QN에서 발행하는 메타데이터 조회에도 즉시 반영되어야 한다. QN 간 메타데이터 뷰의 불일치는 허용하지 않는다.

- **FR-034**: **메타데이터 캐시 Staleness 상한** — QN의 로컬 메타데이터 캐시는 스키마 버전(`TableSchema.version`)을 기준으로 TTL 무효화를 적용해야 한다. DDL 변경(CREATE/ALTER/DROP TABLE)은 캐시 즉시 무효화(write-through) 방식으로 처리해야 하며, CBO 통계(min/max/NDV/Histogram)의 최대 staleness는 설정 가능(기본 500ms)해야 한다. 캐시의 스키마 버전이 Raft 메타스토어의 최신 버전과 불일치하는 경우 즉시 재조회해야 한다.

- **FR-035**: **세션 토큰 클러스터 공유** — Web Client 세션 토큰은 Raft KV(`/sessions/{token}`)에 저장되어야 하며, 클러스터 내 모든 QN에서 유효성 검증이 가능해야 한다. 특정 QN이 재시작되더라도 기존 웹 세션이 유지되어야 하며, 토큰 만료는 Raft KV의 TTL(기본 24시간, 설정 가능)로 관리되어야 한다.

### 클러스터 보호 요구사항

- **FR-036**: **디스크 용량 초과 시 읽기 전용 모드(Read-Only Mode)** — Storage Node의 데이터 디스크 또는 Query Node의 Raft WAL 디스크의 사용률이 설정된 임계값(기본 95%, `disk_full_threshold`)을 초과하여 새로운 데이터·메타데이터를 기록할 수 없는 상태가 감지되면, 클러스터 전체는 즉시 읽기 전용 모드로 전환되어야 한다.
  - **쓰기 차단 대상**: INSERT, CREATE TABLE, ALTER TABLE, DROP TABLE, Kafka Routine Load 수집, Spark Stream Load, Async INSERT, Compaction 출력 쓰기
  - **정상 처리 대상**: SELECT, SHOW TABLES, DESCRIBE, EXPLAIN, SHOW STATUS 등 모든 읽기 전용 연산
  - **클라이언트 오류 응답**: MySQL 호환 에러 — `ERROR 1290 (HY000): The WOW-DB server is running in read-only mode so it cannot execute this statement`
  - **자동 복귀**: 디스크 사용률이 회복 임계값(기본 85%, `disk_recovery_threshold`) 이하로 감소하면 쓰기 가능 상태로 자동 복귀해야 한다
  - **Prometheus 지표**: `wowdb_cluster_read_only{reason="disk_full"}` 메트릭을 노출해야 하며, 읽기 전용 진입 시 값 1, 복귀 시 값 0으로 설정되어야 한다
  - **상세 설계**: `specs/002-wow-db-srs-v02/design/readonly-mode.md` 참조

### 쿼리 실행 단위 및 논리-물리 매핑 요구사항

- **FR-037**: **논리 단위 계층 (Query Node 관점)** — Query Node는 데이터를 세 계층의 논리 단위로 인식하고 스케줄링 결정을 내려야 한다.
  - **Table**: 최상위 논리 엔티티. SQL에서 FROM 절에 명시되는 단위
  - **Partition**: Table을 시간 범위 또는 키 범위로 나눈 논리 분할 단위. CBO의 Partition Pruning 최소 단위
  - **Shard** (= Tablet): Partition 내에서 분산 키(distribution key)의 해시 버킷으로 나눈 스케줄링 최소 단위. Fragment가 CN에 할당될 때의 기본 단위이며, 각 Shard는 하나 이상의 Storage Node에 복제본을 가진다
  - CBO 통계는 이 세 계층 모두에서 독립적으로 저장·조회되어야 한다

- **FR-038**: **물리 단위 계층 (Storage Node 관점)** — Storage Node는 데이터를 네 계층의 물리 단위로 관리하며, Compute Node의 스캔 요청은 이 단위 기준으로 명령을 내려야 한다.
  - **Shard Directory**: Storage Node 파일시스템 상의 물리 디렉토리. 논리 Shard 하나에 1:1 대응
  - **Part** (= SSTable): LSM-Tree의 한 레벨 내 개별 정렬 파일. Compute Node가 스캔을 요청하는 최소 단위. 각 Part는 자신의 min/max Sort Key 범위를 가진다
  - **Granule**: Part 내의 고정 행 블록(기본 8,192행). Data Skipping Index(MINMAX/BLOOM)의 최소 단위. Predicate 평가로 건너뛸 수 있는 최소 IO 단위
  - **Column File** (`.col`, `.bloom`, `.min_max`): Granule별 컬럼 데이터 파일. 쿼리가 필요한 컬럼만 IO 발생

- **FR-039**: **논리-물리 매핑 해석** — Query Node는 논리 계획(Table/Partition/Shard)을 물리 계획(SN NodeId/Shard Directory/Part 목록)으로 변환할 수 있어야 한다.
  - **Shard → SN 매핑**: QN은 Raft 메타데이터에서 `Shard ID → (SN NodeId, Shard Directory 경로)` 매핑을 조회한다
  - **Shard → Part 목록**: CN이 스캔 명령을 실행할 때, SN은 해당 Shard의 MANIFEST를 기반으로 현재 활성 Part 목록을 반환한다. CN은 반환된 Part 목록 중 min/max Sort Key와 쿼리 predicate를 비교하여 스캔 대상을 추려낸다
  - **Part → Column File**: SN은 CN의 column projection 요청에 따라 필요한 `.col` 파일만 읽는다
  - **상세 설계**: `specs/002-wow-db-srs-v02/design/logical-to-physical-mapping.md` 참조

- **FR-040**: **CBO 통계 계층적 저장** — CBO 통계는 논리 단위 계층(Table/Partition/Shard)과 물리 단위(Part) 모두에서 저장·관리되어야 한다.
  - **Table 수준** (Raft KV `/stats/{table_id}`): 전체 row_count, size_bytes, last_analyzed
  - **Partition 수준** (Raft KV `/stats/{table_id}/{partition_id}`): row_count, size_bytes, partition key min/max
  - **Shard 수준** (Raft KV `/stats/{table_id}/{partition_id}/{shard_id}`): row_count, size_bytes, 컬럼별 min/max/NDV/null_count/Histogram
  - **Part 수준** (MANIFEST 내 SstRef 필드): row_count, min_sort_key, max_sort_key (Raft KV 비저장, SN 로컬)
  - CBO Optimizer는 Partition Pruning 시 Partition 수준 통계를, Join 순서 결정 시 Shard 수준 통계를, 파일 스킵 시 Part 수준 통계를 각각 활용해야 한다

- **FR-041**: **CN Fragment 스케줄링 (Shard 단위)** — Physical Planner는 각 Shard를 하나의 CN에 할당하는 Fragment를 생성해야 하며, 각 Fragment에는 다음 정보가 포함되어야 한다.
  - `shard_id`: 대상 Shard 식별자
  - `sn_endpoint`: Shard 데이터를 보유하는 SN의 gRPC 엔드포인트
  - `columns`: 필요한 컬럼 이름 목록 (Column Projection)
  - `predicates`: SN Scan에 Pushdown할 필터 표현식
  - `runtime_filter`: Hash Join Build Side에서 생성된 Bloom/MinMax 필터 (있는 경우)
  - `scan_range`: 스캔할 LSM 레벨 범위 (기본: 전체 레벨, Full Compaction 이후 L6만 지정 가능)
  - 하나의 CN은 여러 Fragment(Shard)를 병렬 파이프라인으로 처리할 수 있어야 한다

- **FR-042**: **SN 스캔 실행 (Part 수준)** — Storage Node의 스캔 실행기는 CN으로부터 Shard 스캔 요청을 수신하면 다음 순서로 처리해야 한다.
  1. MANIFEST에서 해당 Shard의 활성 Part 목록 조회 (레벨별)
  2. Part의 min/max Sort Key와 쿼리 predicate 비교 → 범위 밖 Part 즉시 스킵
  3. 남은 Part에 대해 SSTable-level Bloom Filter 검사 → false면 해당 Part 스킵
  4. 통과한 Part의 각 Granule에 대해 MINMAX Index 검사 → predicate 범위 밖 Granule 스킵
  5. 남은 Granule의 Column File(`.col`)에서 필요한 컬럼 데이터 블록만 읽어 Arrow2 RecordBatch 형태로 CN에 스트리밍 반환
  6. L0 Part는 key range 겹침이 있으므로 동일 Sort Key에 대해 최신 sequence_num의 값을 우선 사용 (Multi-Version Read)
  - **상세 설계**: `specs/002-wow-db-srs-v02/design/logical-to-physical-mapping.md` 및 `design/query-execution-model.md` 참조

### 데이터 분포 가시성 요구사항 (FR-043~FR-046)

- **FR-043**: **SHOW PARTITIONS — 파티션 분포 조회** — MySQL 클라이언트에서 `SHOW PARTITIONS FROM <table>` 명령으로 해당 Table의 모든 파티션 목록과 메타데이터를 조회할 수 있어야 한다.

  **출력 컬럼**:
  | 컬럼명 | 타입 | 설명 |
  |--------|------|------|
  | `partition_id` | STRING | 파티션 UUID |
  | `range_start` | STRING | 파티션 키 범위 시작 (NULL이면 -∞) |
  | `range_end` | STRING | 파티션 키 범위 끝 (NULL이면 +∞) |
  | `row_count` | BIGINT | 해당 파티션의 총 레코드 수 (CBO 통계 기반) |
  | `size_bytes` | BIGINT | 압축된 저장 크기 (bytes) |
  | `shard_count` | INT | 이 파티션을 담당하는 Shard (Tablet) 수 |
  | `part_count` | INT | 전체 활성 LSM Part 수 (모든 Shard 합산) |
  | `tier` | STRING | `Hot` (NVMe) / `Cold` (S3/HDFS) |
  | `created_at` | DATETIME | 파티션 생성 시각 |

  **지원 구문**:
  ```sql
  -- 전체 파티션 목록
  SHOW PARTITIONS FROM page_events;

  -- 키 범위로 필터 (파티션 키가 event_time인 경우)
  SHOW PARTITIONS FROM page_events WHERE range_start >= '2024-01-01';

  -- 크기 기준 정렬
  SHOW PARTITIONS FROM page_events ORDER BY size_bytes DESC LIMIT 10;
  ```

- **FR-044**: **SHOW SHARDS — Shard 분산 조회** — `SHOW SHARDS FROM <table>` 명령으로 Table의 모든 Shard(Tablet)와 각 Shard를 담당하는 Storage Node 정보를 조회할 수 있어야 한다. 분산 키별로 어떤 데이터가 어느 SN에 저장되어 있는지, 데이터 편중 여부를 확인하는 데 사용한다.

  **출력 컬럼**:
  | 컬럼명 | 타입 | 설명 |
  |--------|------|------|
  | `shard_id` | STRING | Shard UUID (= Tablet UUID) |
  | `partition_id` | STRING | 소속 파티션 UUID |
  | `partition_range` | STRING | 소속 파티션 범위 (`[start, end)` 형식) |
  | `sn_node_id` | STRING | 담당 Storage Node ID (예: `sn-01`) |
  | `sn_endpoint` | STRING | Storage Node gRPC 주소 (예: `10.0.0.1:9060`) |
  | `bucket_id` | INT | 분산 키 해시 버킷 번호 |
  | `role` | STRING | `Leader` 또는 `Follower` |
  | `state` | STRING | `Normal` / `Compacting` / `Migrating` |
  | `row_count` | BIGINT | 이 Shard의 레코드 수 |
  | `size_bytes` | BIGINT | 이 Shard의 압축 저장 크기 |
  | `part_count` | INT | 활성 LSM Part 수 |
  | `lsn` | BIGINT | 최신 Log Sequence Number |

  **지원 구문**:
  ```sql
  -- 전체 Shard 목록
  SHOW SHARDS FROM page_events;

  -- 특정 파티션의 Shard만
  SHOW SHARDS FROM page_events PARTITION '<partition_id>';

  -- 특정 SN의 Shard만 (데이터 편중 진단용)
  SHOW SHARDS FROM page_events WHERE sn_node_id = 'sn-01';

  -- 크기 기준 내림차순 (Hot Shard 파악)
  SHOW SHARDS FROM page_events ORDER BY size_bytes DESC LIMIT 20;
  ```

- **FR-045**: **SHOW PARTS — LSM Part(SSTable) 조회** — `SHOW PARTS FROM <table>` 명령으로 Table의 모든 물리 LSM Part 목록과 각 Part의 레벨, Sort Key 범위, 크기, Bloom Filter 정보를 조회할 수 있어야 한다. `SHOW PARTS ON PARTITION <partition_id> FROM <table>` 구문을 통해 특정 파티션으로 드릴다운이 가능해야 한다.

  **계층 구조 (Drill-down)**:
  ```
  Table
    └─ SHOW PARTITIONS → Partition
          └─ SHOW SHARDS → Shard (Tablet, SN별 분산 단위)
                └─ SHOW PARTS → Part (LSM SSTable, 실제 파일 단위)
  ```

  **출력 컬럼**:
  | 컬럼명 | 타입 | 설명 |
  |--------|------|------|
  | `part_id` | STRING | Part UUID |
  | `shard_id` | STRING | 소속 Shard UUID |
  | `partition_id` | STRING | 소속 파티션 UUID |
  | `sn_node_id` | STRING | 위치한 Storage Node ID |
  | `level` | INT | LSM 레벨 (0=Immutable MemTable flush, 1~6=Compacted) |
  | `sequence_num` | BIGINT | 생성 순번 (L0 Multi-Version Read 기준) |
  | `row_count` | BIGINT | 이 Part의 레코드 수 |
  | `size_bytes` | BIGINT | 압축 크기 (bytes) |
  | `min_sort_key` | STRING | Sort Key 최솟값 (CBO Pruning 기준) |
  | `max_sort_key` | STRING | Sort Key 최댓값 (CBO Pruning 기준) |
  | `bloom_size_bytes` | BIGINT | Bloom Filter 파일 크기 (bytes) |
  | `created_at` | DATETIME | Part 생성 시각 (flush 완료 시각) |

  **지원 구문**:
  ```sql
  -- 전체 Part 목록 (Table 전체)
  SHOW PARTS FROM page_events;

  -- 특정 파티션의 Part만 (Partition 단위 drill-down)
  SHOW PARTS FROM page_events PARTITION '<partition_id>';
  -- 별칭 구문 (파티션 우선 명시 방식)
  SHOW PARTS ON PARTITION '<partition_id>' FROM page_events;

  -- 특정 Shard의 Part만 (Shard 단위 drill-down)
  SHOW PARTS FROM page_events SHARD '<shard_id>';

  -- L0 Part만 조회 (Compaction 지연 진단용)
  SHOW PARTS FROM page_events WHERE level = 0;

  -- 특정 SN의 Part만
  SHOW PARTS FROM page_events WHERE sn_node_id = 'sn-01' ORDER BY size_bytes DESC;
  ```

  > **운영 팁**: `level = 0` Part가 많다면 Compaction이 지연되고 있는 것을 의미한다. `size_bytes`가 비정상적으로 큰 Part는 Hot Shard의 징후일 수 있다.

- **FR-046**: **SHOW DISTRIBUTED STATUS — 노드별 분포 요약** — `SHOW DISTRIBUTED STATUS FROM <table>` 명령으로 각 Storage Node가 담당하는 Shard 수, 총 데이터 크기, 행 수를 요약 조회할 수 있어야 한다. 데이터 편중(skew) 여부를 신속히 파악하는 데 사용한다.

  **출력 컬럼**:
  | 컬럼명 | 타입 | 설명 |
  |--------|------|------|
  | `sn_node_id` | STRING | Storage Node ID |
  | `sn_endpoint` | STRING | Storage Node 주소 |
  | `shard_count` | INT | 담당 Shard 수 (Leader + Follower 합산) |
  | `leader_shard_count` | INT | Leader Shard 수 (쓰기 부하 기준) |
  | `partition_count` | INT | 담당 파티션 수 |
  | `row_count` | BIGINT | 총 레코드 수 |
  | `size_bytes` | BIGINT | 총 저장 크기 |
  | `avg_part_per_shard` | FLOAT | Shard당 평균 Part 수 (Compaction 상태 지표) |

  **지원 구문**:
  ```sql
  -- 노드별 분포 요약
  SHOW DISTRIBUTED STATUS FROM page_events;

  -- 크기 기준 정렬 (가장 부하가 높은 SN 파악)
  SHOW DISTRIBUTED STATUS FROM page_events ORDER BY size_bytes DESC;
  ```

- **FR-047**: **EXPLAIN — 분산 쿼리 실행 계획 출력** — `EXPLAIN <sql>` 명령으로 CBO가 생성한 분산 실행 계획을 Fragment 단위로 출력해야 한다. 각 Fragment에는 다음 정보가 포함되어야 한다.

  - 집계 위치 (CN-level Partial Agg vs QN-level Merge Agg)
  - SN별 예상 I/O 크기 (파티션 프루닝 전/후 비교)
  - 인덱스 활용 여부 (Bloom Filter / MinMax / Data Skipping)
  - 파티션 프루닝 결과 (전체 파티션 수 vs 실제 스캔할 파티션 수)
  - CBO 통계 기반 예상 행 수 및 데이터 크기
  - Join 유형 결정 근거 (Hash Join vs Sort-Merge Join)
  - CN 간 데이터 Shuffle 방식 (BROADCAST vs HASH_SHUFFLE vs COLOCATE)
  - Aggregate Pushdown 적용 여부 (SN 레벨 부분 집계 → 전송 크기 감소)
  - 노드별 실행 단계 분해 (Fragment별 독립 출력)

  **지원 변형**:
  | 구문 | 출력 내용 |
  |------|-----------|
  | `EXPLAIN <sql>` | Fragment 구조 + Shuffle 방식 + 기본 CBO 수치 |
  | `EXPLAIN VERBOSE <sql>` | 기본 출력 + Push-down 프레디케이트 + 인덱스 상세 + 컬럼 투영 목록 |
  | `EXPLAIN COSTS <sql>` | 기본 출력 + 행 수 추정 + 바이트 추정 + 각 노드 비용 합계 |

  **출력 컬럼**:
  | 컬럼명 | 타입 | 설명 |
  |--------|------|------|
  | `Fragment_Id` | STRING | Fragment 식별자 (`FRAGMENT 0`, `FRAGMENT 1`, ...) |
  | `Plan` | TEXT | 해당 Fragment 실행 계획 텍스트 (들여쓰기 형식) |

  각 행이 실행 계획의 한 줄을 담는다. `Fragment_Id` 컬럼은 소속 Fragment ID를 표시하고 새 Fragment 시작 시 값이 변경된다.

  **출력 예시** (`EXPLAIN SELECT ... FROM page_events WHERE event_time > '2026-01-01'`):
  ```
  PLAN FRAGMENT 0
    OUTPUT EXPRS: user_id, event_name, event_time
    PARTITION: UNPARTITIONED

    RESULT SINK

    0: AGGREGATION [QN-MERGE]
       CBO: rows=50,000  bytes=4.2MB

  PLAN FRAGMENT 1
    OUTPUT EXPRS: user_id, event_name, event_time
    PARTITION: HASH(user_id)

    STREAM DATA SINK
      EXCHANGE ID: 01
      HASH_PARTITIONED: user_id

    1: HASH AGGREGATE [CN-PARTIAL]
       |--- 2: SCAN (page_events) [SN-01, SN-02, SN-03]
              Partitions: 3/10 pruned (7 skipped by CBO)
              Parts: 24 scanned (12 skipped by Bloom/MinMax)
              Push-down predicates: event_time > '2026-01-01'
              Index: MinMax on event_time [HIT]
              Aggregate pushdown: count(*) [YES]
  ```

- **FR-048**: **EXPLAIN — Colocate Join 표시** — 동일 Colocate Group 내 두 Table을 Join할 때 EXPLAIN 출력에서 Join 노드가 `[COLOCATE]` 태그로 표시되어야 한다. Colocate Join은 분산 키(Distribution Key)와 버킷 수(Bucket Count)가 동일한 두 Table 사이에서 Shuffle 없이 로컬 Join으로 실행된다.

  **Colocate Join 조건**:
  1. 두 Table이 동일한 `colocate_group` PROPERTIES 값을 가진다.
  2. 두 Table의 `DISTRIBUTED BY HASH(<col>)` 분산 키가 동일하다.
  3. 두 Table의 `BUCKETS <n>` 버킷 수가 동일하다.
  4. JOIN 조건 컬럼이 분산 키와 일치한다.

  **EXPLAIN 출력 비교**:
  ```
  -- 일반 Hash Join (Shuffle 필요)
  HASH JOIN [BROADCAST]
    CBO: rows=200,000  bytes=16MB
    |--- 3: SCAN (page_events)   [SN-01, SN-02, SN-03]
    |--- 4: SCAN (user_profiles) [SN-01, SN-02, SN-03]

  -- Colocate Join (Shuffle 없음, 로컬 실행)
  HASH JOIN [COLOCATE]
    CBO: rows=200,000  bytes=16MB  shuffle=NONE
    |--- 3: SCAN (page_events)   [SN-01, SN-02, SN-03]
    |--- 4: SCAN (user_profiles) [SN-01, SN-02, SN-03]
         (same bucket distribution — no shuffle required)
  ```

  Colocate Join이 성립하지 않는 경우(분산 키 불일치, 버킷 수 차이, Colocate Group 미설정) EXPLAIN은 `[HASH_SHUFFLE]` 또는 `[BROADCAST]`를 표시한다.

### 클러스터 관리 요구사항 (FR-CM)

상세 설계: `specs/002-wow-db-srs-v02/design/cluster-management.md`  
구현 태스크: `specs/002-wow-db-srs-v02/tasks-cluster-management.md`

- **FR-CM-001**: **동적 노드 등록 (Dynamic Node Discovery)** — QN, CN, SN 노드는 기동 시 환경변수 `QN_PEERS`에 지정된 QN 주소(주소:포트)로 자동 자가 등록(self-registration)해야 한다. 수동 설정 파일 편집이나 클러스터 재시작 없이 노드가 합류해야 한다. Kubernetes 환경에서는 ConfigMap의 `qn.peers` 값을 통해 QN 주소를 주입한다.

- **FR-CM-002**: **노드 상태 모델** — 모든 노드는 `ACTIVE → READONLY → DRAINING` 의 단방향 상태를 가지며, `DISMISSED` 상태는 Raft 메타데이터에서 노드 항목이 제거된 것을 의미한다. 역방향 전환(`DRAINING → ACTIVE`)은 허용되지 않는다.
  - **ACTIVE**: 쓰기·읽기 모두 허용. INSERT 라우팅 대상.
  - **READONLY**: 읽기만 허용. INSERT 라우팅에서 즉시 제외. `ALTER CLUSTER DRAIN` 명령으로 진입.
  - **DRAINING**: 읽기 허용, 쓰기 불가. 백그라운드 Shard 마이그레이션 진행 중.
  - **DISMISSED**: 클러스터에서 영구 제거됨. Raft KV에 항목 없음.

- **FR-CM-003**: **`ALTER CLUSTER JOIN` 명령** — 운영자는 `ALTER CLUSTER JOIN <type> '<address>:<port>' [AS '<node_id>']` 명령으로 새 노드를 클러스터에 수동 추가할 수 있어야 한다. SN 추가 시 백그라운드 Rebalance가 자동으로 시작되어야 한다. QN 추가 시 Raft learner → voter 순서로 합류해야 한다.

- **FR-CM-004**: **`ALTER CLUSTER DRAIN` 명령** — 운영자는 `ALTER CLUSTER DRAIN '<node_id>'` 명령으로 특정 노드를 READONLY 상태로 전환하고 데이터 마이그레이션을 시작할 수 있어야 한다. DRAIN 명령 후 최대 100ms 이내에 해당 SN으로의 INSERT 라우팅이 중단되어야 한다. QN DRAIN 시 남은 QN이 quorum을 구성할 수 없는 경우(3노드 중 2번째 DRAIN 등) 오류를 반환해야 한다.

- **FR-CM-005**: **`ALTER CLUSTER DISMISS` 명령** — 운영자는 DRAIN 완료 후 `ALTER CLUSTER DISMISS '<node_id>'` 명령으로 노드를 영구 제거할 수 있어야 한다. 대상 노드에 데이터가 남아있고 `FORCE` 플래그가 없는 경우 오류(`ER_NODE_HAS_DATA`)를 반환해야 한다. `FORCE` 플래그 사용 시 해당 노드의 모든 Shard 메타데이터를 Raft KV에서 삭제하고 노드를 제거한다 (데이터 손실 경고 포함).

- **FR-CM-006**: **자동 Shard Rebalancing** — 새 SN이 클러스터에 추가되거나 기존 SN이 DRAIN을 시작할 때, 시스템은 백그라운드 프로세스로 Shard 재분배 작업을 자동으로 시작해야 한다. Rebalance 중 쿼리 및 수집 작업은 중단 없이 처리되어야 한다.
  - Rebalance 알고리즘: ACTIVE SN 간 균등 분산(Shard 수 기준), 최소 이전 원칙
  - 마이그레이션 중 읽기: 원본 SN에서 계속 서빙
  - 마이그레이션 중 쓰기: 원본 SN READONLY, 이전 완료 후 대상 SN으로 전환
  - 동시 Shard 이전 수: 설정 가능(`cluster.rebalance_concurrency`, 기본 2)

- **FR-CM-007**: **SHOW CLUSTER 명령** — 운영자는 다음 명령으로 클러스터 상태를 조회할 수 있어야 한다.
  - `SHOW CLUSTER NODES`: 전체 노드 목록, 타입, 주소, 상태, Shard 수, 참가 시각
  - `SHOW CLUSTER STATUS`: QN/CN/SN 수, 드레이닝 노드 수, Raft Leader, 읽기/쓰기 모드
  - `SHOW CLUSTER REBALANCE`: 진행 중인 Rebalance 작업 목록, 진행률, 예상 완료 시각

- **FR-CM-008**: **K8s/Helm 지원** — 클러스터의 모든 노드 타입은 Helm Chart를 통해 수평 확장이 가능해야 한다. Helm `values.yaml`의 `replicas` 값 변경만으로 노드 수를 조정할 수 있어야 한다.
  - QN: K8s StatefulSet (홀수 개 유지 필수). ConfigMap `qn.peers`에 Headless Service DNS 기반 주소 설정.
  - CN: K8s Deployment + HorizontalPodAutoscaler (CPU 기반 자동 확장 지원).
  - SN: K8s StatefulSet + PersistentVolumeClaim (재스케줄 시 데이터 보존).
  - 각 노드 Pod의 init container가 기동 시 `ALTER CLUSTER JOIN` 자동 실행.

- **FR-CM-009**: **노드 장애 자동 감지** — QN은 각 노드에 대해 주기적으로 헬스체크를 수행하고, 일정 시간 응답이 없는 노드를 DRAINING 상태로 자동 전환해야 한다. SN 장애 시 해당 SN의 Shard 복제본이 다른 SN에 있으면 자동 복구 Rebalance를 시작한다.

- **FR-CM-010**: **Rebalance 수동 트리거** — 운영자는 `ALTER CLUSTER REBALANCE` 명령으로 노드 간 불균형을 수동으로 재조정할 수 있어야 한다. 이 명령은 Rebalance가 이미 진행 중인 경우 오류를 반환해야 한다.

### 핵심 엔티티

- **이벤트(Event)**: 웹 서비스에서 사용자가 발생시킨 단일 행동 단위. 타임스탬프, 사용자 식별자, 이벤트 타입명, 선택적 properties 페이로드를 포함한다.
- **Table**: 이벤트 데이터를 위한 명명된 논리 스키마 및 스토리지 컨테이너. 컬럼, 파티션 전략, 정렬 순서로 정의된다. WOW-DB의 기본 데이터 조직 단위.
- **세션(Session)**: 설정 가능한 비활성 타임아웃으로 경계가 정해지는 연속적 활동 기간 내에 단일 사용자에 귀속된 연속적인 이벤트 그룹.
- **Session Materialized View (SMV)**: 이벤트를 세션으로 조직화하는 Table의 파생되고 사전 계산된 뷰. 세션 ID, 세션 시작·종료 시각, 세션별 정렬된 이벤트 시퀀스를 포함한다.
- **파티션(Partition)**: 쿼리 범위 지정 및 유지보수 작업의 효율적인 처리를 위해 Table 데이터를 시간 범위 또는 키 범위로 분할한 단위. 파티션 내에서만 LSM Merge 발생.
- **퍼널 단계(Funnel Step)**: 사용자 여정 분석의 한 단계를 나타내는 명명된 이벤트 매칭 조건.
- **코호트(Cohort)**: 공유된 자격 최초 이벤트와 진입 날짜로 정의되는 사용자 그룹.
- **사전 집계 Materialized View(Pre-aggregation MV)**: 특정 GROUP BY 형태로 Table 데이터의 사전 계산된 집계를 저장하는 물리 뷰.
- **리소스 그룹(Resource Group)**: 하나 이상의 사용자 또는 역할에 할당된 CPU, 메모리, 동시 쿼리 수, 쿼리 타임아웃 제한 설정.
- **External Table**: 데이터 임포트 없이 외부 스토리지 시스템(S3, HDFS, Iceberg)에 매핑되어 즉시 쿼리 가능한 가상 테이블.
- **Query Node (QN)**: SQL 파싱·CBO·플래닝·메타데이터 관리·사용자 Frontend를 담당하는 노드. 홀수 개로 Raft 클러스터 구성.
- **Compute Node (CN)**: Query Node의 물리 실행 계획을 수행하는 Worker 노드.
- **Data Node (DN)**: 컬럼 지향 LSM-Tree 기반 스토리지를 담당하는 노드. 수평 확장 가능.
- **Tablet**: Data Node에서 데이터를 분산 저장하는 최소 물리 단위.

---

## 성공 기준 *(필수)*

### 측정 가능한 결과

- **SC-001**: 시스템은 스키마 마이그레이션, 수동 Compaction 트리거, 스토리지 재구성 없이 200억 건 이상의 이벤트 레코드를 저장하고 분석 쿼리를 처리해야 한다.
- **SC-002**: 10억 건 이상의 이벤트 레코드 데이터셋에 대한 Funnel·Cohort·Path 분석 쿼리가 결과를 반환해야 한다.
- **SC-003**: WOW-DB 경험이 없는 데이터 분석가가 시스템에 처음 접근한 후 15분 이내에 Table을 생성하고, Web UI 마법사를 통해 Session Materialized View를 생성하며, Funnel 분석 쿼리를 실행할 수 있어야 한다.
- **SC-004**: 표준 MySQL 클라이언트 도구, JDBC 기반 BI 도구, Python mysql-connector 스크립트가 코드 변경, 드라이버 교체, 특별한 설정 플래그 없이 WOW-DB에 연결하고 쿼리할 수 있어야 한다.
- **SC-005**: 설정된 Kafka 토픽에 게시된 이벤트는 정상 운영 조건에서 브로커에 도착한 후 60초 이내에 WOW-DB에서 쿼리 가능해야 한다.
- **SC-006**: 단일 Query Node 또는 Data Node 장애 발생 후 진행 중인 쿼리가 데이터 손상 없이 재시도되거나 완료되며, 관리자 개입 없이 시스템이 자동으로 완전한 운영을 재개해야 한다.
- **SC-007**: 운영자가 Web UI 모니터링 및 프로파일링 도구만을 사용하여 최근 24시간 내 가장 느린 쿼리를 식별하고 병목 처리 단계를 파악하는 데 5분 이내에 가능해야 한다.

---

## 가정 사항

- 대상 사용자는 세 가지 역할에 걸쳐 있다: 이벤트 데이터를 쿼리하는 데이터 분석가, 수집 파이프라인을 관리하는 데이터/백엔드 엔지니어, 클러스터를 운영하는 플랫폼 엔지니어.
- OLTP 워크로드(고빈도 포인트 업데이트, 단일 행 삭제)는 명시적으로 범위 밖이다. WOW-DB는 추가 위주의 분석 워크로드를 위해 설계되었다.
- 지리공간(GIS) 분석과 머신러닝 모델 훈련은 범위 밖이다. WOW-DB는 ML 워크플로우에 입력을 제공하지만 모델을 훈련하지 않는다.
- HDFS 스토리지 백엔드 배포는 Hadoop 클러스터에 Kerberos 인증이 설정되어 있다고 가정한다. 비인증 HDFS 접속은 지원하지 않는다.
- MySQL 프로토콜로 연결하는 클라이언트는 MySQL 8.0 Wire Protocol과 호환되는 드라이버를 사용한다고 가정한다.
- Web SQL 에디터는 데스크톱 브라우저(Chrome, Firefox, Edge, Safari)를 대상으로 한다. 초기 릴리스에서 모바일 브라우저 지원은 요구사항이 아니다.
- Session MV 마법사는 소스 Table이 이미 생성되어 있고 사용자 식별자로 사용할 수 있는 컬럼이 하나 이상 포함되어 있다고 가정한다.
- 시스템은 고가용성을 위해 최소 3개의 Query Node와 데이터 중복성을 위해 최소 3개의 Data Node를 갖춘 환경에 배포된다고 가정한다.
- Avro 형식의 Kafka 이벤트는 Confluent Schema Registry API와 호환되는 스키마 레지스트리에 스키마가 등록되어 있다고 가정한다.
- HDFS 스토리지 백엔드를 사용하는 경우, `kerberos_renew_interval_sec` 설정을 통해 keytab 자동 갱신이 구성되어 있다고 가정한다.
