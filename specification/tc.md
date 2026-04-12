# WOW-DB Test Cases

---

| 항목 | 내용 |
|---|---|
| 문서 버전 | v0.2 Draft |
| 작성일 | 2026-04-12 |
| 참조 문서 | srs.md v0.2 |

---

## 테스트 케이스 구성 규칙

각 TC는 다음 형식으로 작성한다.

```
TC-{카테고리}-{번호}
카테고리: DDL, DML, ING(Ingestion), ANA(Analytics), SMV, COMPAT, PERF, WEB,
          QN(Query Node), CBO, CN(Compute Node), DN(Data Node), MON(Monitoring), PROF(Profiler)
```

**Pass 기준 컬럼 정의:**

| 등급 | 의미 |
|---|---|
| P (Pass) | 기대 결과와 완전히 일치 |
| F (Fail) | 기대 결과와 불일치 또는 오류 발생 |
| B (Blocked) | 선행 조건 미충족으로 실행 불가 |

---

## TC-DDL: DDL 테스트

---

### TC-DDL-001: 기본 Cube 생성

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-DDL-001 |
| **테스트명** | 기본 Event Cube 생성 |
| **관련 요구사항** | FR-CUBE-001 |
| **우선순위** | Critical |

**사전 조건:**
- WOW-DB 클러스터 정상 기동 (QN 3노드 Raft + CN 2노드 + DN 3노드)
- MySQL 클라이언트로 QN 접속 완료 (`analytics` 데이터베이스 선택)

**테스트 절차:**

```sql
-- Step 1: Cube 생성
CREATE CUBE page_events (
    event_time   DATETIME     NOT NULL,
    device_id    VARCHAR(64)  NOT NULL,
    event_name   VARCHAR(128) NOT NULL,
    page_url     VARCHAR(2048),
    properties   JSON
)
ENGINE = WOW_LSM
DISTRIBUTED BY HASH(device_id) BUCKETS 8
ORDER BY (event_time, device_id);

-- Step 2: 생성 확인
SHOW CUBES;
SHOW CREATE TABLE page_events;
```

**기대 결과:**

| 스텝 | 기대 결과 |
|---|---|
| Step 1 | `Query OK, 0 rows affected` 반환 |
| Step 2 (SHOW CUBES) | `page_events` 가 목록에 표시됨 |
| Step 2 (SHOW CREATE TABLE) | 정의된 컬럼과 옵션이 정확히 반환됨 |

**Pass 기준:** Step 1, 2 모두 기대 결과 충족 시 Pass

---

### TC-DDL-002: Cube 생성 - Range + Hash 파티셔닝

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-DDL-002 |
| **테스트명** | Range 파티션 + Hash 분산 Cube 생성 |
| **관련 요구사항** | FR-CUBE-001 |
| **우선순위** | High |

**사전 조건:** TC-DDL-001 완료 후 `page_events` DROP 또는 별도 이름 사용

**테스트 절차:**

```sql
CREATE CUBE page_events_partitioned (
    event_time   DATETIME     NOT NULL,
    device_id    VARCHAR(64)  NOT NULL,
    event_name   VARCHAR(128) NOT NULL,
    properties   JSON,
    INDEX idx_evt (event_name) USING BITMAP
)
ENGINE = WOW_LSM
PARTITION BY RANGE(event_time) (
    PARTITION p_2024_q1 VALUES [('2024-01-01'), ('2024-04-01')),
    PARTITION p_2024_q2 VALUES [('2024-04-01'), ('2024-07-01')),
    PARTITION p_future  VALUES [('2024-07-01'), (MAXVALUE))
)
DISTRIBUTED BY HASH(device_id) BUCKETS 16
ORDER BY (event_time, device_id)
PROPERTIES ("replication_num" = "3", "compression" = "LZ4");

-- 파티션 상태 확인
SHOW TABLET STATUS FROM page_events_partitioned;
```

**기대 결과:**
- Cube 생성 성공
- 파티션 p_2024_q1, p_2024_q2, p_future 각각 생성 확인
- `replication_num=3` 기준 각 Tablet에 3개 복제본 존재 확인

**Pass 기준:** 파티션 3개 및 Bucket 16개 × 3 복제본 = 48 Tablet 확인

---

### TC-DDL-003: IF NOT EXISTS 중복 생성

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-DDL-003 |
| **테스트명** | 동일 이름 Cube 중복 생성 시 처리 |
| **관련 요구사항** | FR-CUBE-001 |
| **우선순위** | Medium |

**테스트 절차:**

```sql
-- Step 1: 첫 번째 생성 (성공해야 함)
CREATE CUBE dup_test (event_time DATETIME, device_id VARCHAR(64))
ENGINE = WOW_LSM DISTRIBUTED BY HASH(device_id) BUCKETS 4;

-- Step 2: IF NOT EXISTS 없이 재생성 (오류 기대)
CREATE CUBE dup_test (event_time DATETIME, device_id VARCHAR(64))
ENGINE = WOW_LSM DISTRIBUTED BY HASH(device_id) BUCKETS 4;

-- Step 3: IF NOT EXISTS 포함 재생성 (경고만 기대)
CREATE CUBE IF NOT EXISTS dup_test (event_time DATETIME, device_id VARCHAR(64))
ENGINE = WOW_LSM DISTRIBUTED BY HASH(device_id) BUCKETS 4;
```

**기대 결과:**

| 스텝 | 기대 결과 |
|---|---|
| Step 1 | 성공 |
| Step 2 | `ERROR 1050: Table 'dup_test' already exists` |
| Step 3 | `Query OK` + Warning 1 (Note: Table already exists) |

---

### TC-DDL-004: Cube 컬럼 추가

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-DDL-004 |
| **테스트명** | 운영 중 Cube에 컬럼 추가 (Online DDL) |
| **관련 요구사항** | FR-CUBE-002 |
| **우선순위** | High |

**사전 조건:** TC-DDL-001의 `page_events` Cube 존재

**테스트 절차:**

```sql
-- Step 1: 현재 컬럼 수 확인
SELECT COUNT(*) FROM information_schema.COLUMNS
WHERE TABLE_NAME = 'page_events';

-- Step 2: 컬럼 추가
ALTER CUBE page_events
    ADD COLUMN browser_name VARCHAR(64) COMMENT '브라우저 이름',
    ADD COLUMN os_name      VARCHAR(64) COMMENT '운영체제 이름';

-- Step 3: 추가 확인
DESCRIBE page_events;

-- Step 4: 새 컬럼에 INSERT 테스트
INSERT INTO page_events (event_time, device_id, event_name, browser_name, os_name)
VALUES (NOW(), 'dev-001', 'page_view', 'Chrome', 'Windows');
```

**기대 결과:**
- Step 2: 성공 (온라인 DDL, 기존 데이터 무중단)
- Step 3: `browser_name`, `os_name` 컬럼 표시
- Step 4: INSERT 성공, 기존 레코드의 새 컬럼은 NULL

---

### TC-DDL-005: Cube DROP (CASCADE)

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-DDL-005 |
| **테스트명** | Session MV가 있는 Cube DROP 시 CASCADE 요구 |
| **관련 요구사항** | FR-CUBE-003 |
| **우선순위** | High |

**테스트 절차:**

```sql
-- Step 1: Cube 및 SMV 생성
CREATE CUBE drop_test (event_time DATETIME NOT NULL, device_id VARCHAR(64) NOT NULL)
ENGINE = WOW_LSM DISTRIBUTED BY HASH(device_id) BUCKETS 4;

CREATE SESSION MATERIALIZED VIEW drop_test_sessions
FROM drop_test USER_KEY = device_id SESSION_TIMEOUT = 30 MINUTE REFRESH REALTIME;

-- Step 2: CASCADE 없이 DROP (오류 기대)
DROP CUBE drop_test;

-- Step 3: CASCADE 포함 DROP (성공 기대)
DROP CUBE drop_test CASCADE;

-- Step 4: SMV도 함께 삭제됐는지 확인
SHOW SESSIONS;
```

**기대 결과:**

| 스텝 | 기대 결과 |
|---|---|
| Step 2 | `ERROR: Cube has dependent Session Materialized Views. Use CASCADE.` |
| Step 3 | 성공 |
| Step 4 | `drop_test_sessions` 이 목록에서 제거됨 |

---

## TC-SMV: Session Materialized View 테스트

---

### TC-SMV-001: SQL 방식 SMV 생성

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-SMV-001 |
| **테스트명** | CREATE SESSION MATERIALIZED VIEW (SQL) |
| **관련 요구사항** | FR-SMV-001, FR-SMV-002 |
| **우선순위** | Critical |

**사전 조건:** `page_events` Cube 존재 + 데이터 1,000건 이상 적재

**테스트 절차:**

```sql
-- Step 1: SMV 생성
CREATE SESSION MATERIALIZED VIEW page_events_sessions
FROM page_events
USER_KEY        = device_id
SESSION_TIMEOUT = 30 MINUTE
REFRESH REALTIME;

-- Step 2: 자동 생성 컬럼 확인
DESCRIBE page_events_sessions;

-- Step 3: 세션 데이터 확인
SELECT session_id, device_id, session_start_time, session_end_time,
       session_duration_sec, session_event_count, is_new_user
FROM page_events_sessions
LIMIT 10;

-- Step 4: 세션 경계 검증 (30분 초과 이벤트는 다른 session_id)
SELECT s1.device_id, s1.session_id, s1.event_time,
       s2.session_id AS next_session_id, s2.event_time AS next_event_time,
       TIMESTAMPDIFF(MINUTE, s1.event_time, s2.event_time) AS gap_minutes
FROM page_events_sessions s1
JOIN page_events_sessions s2
    ON s1.device_id = s2.device_id
    AND s1.session_seq = s2.session_seq - 1
WHERE s1.session_id <> s2.session_id
LIMIT 20;
```

**기대 결과:**
- Step 2: `session_id, session_start_time, session_end_time, session_duration_sec, session_event_count, session_seq, is_new_user` 컬럼 모두 존재
- Step 4: `gap_minutes >= 30` 인 경우에만 세션 ID 변경 확인

---

### TC-SMV-002: Session Timeout 경계 검증

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-SMV-002 |
| **테스트명** | Session Timeout 기준 세션 분리 정확성 |
| **관련 요구사항** | FR-SMV-004 |
| **우선순위** | Critical |

**테스트 절차:**

```sql
-- 5분 timeout SMV 생성 (테스트 전용)
CREATE SESSION MATERIALIZED VIEW test_5min_sessions
FROM page_events USER_KEY = device_id SESSION_TIMEOUT = 5 MINUTE REFRESH ON DEMAND;

-- 같은 device_id로 이벤트 삽입 (4분 간격, 6분 간격)
INSERT INTO page_events (event_time, device_id, event_name) VALUES
    ('2024-01-15 10:00:00', 'test-device', 'ev_a'),  -- session 1 start
    ('2024-01-15 10:04:00', 'test-device', 'ev_b'),  -- +4분 → 같은 세션
    ('2024-01-15 10:10:01', 'test-device', 'ev_c'),  -- +6분 1초 → 새 세션
    ('2024-01-15 10:14:00', 'test-device', 'ev_d');  -- +4분 → 같은 세션 (세션 2)

REFRESH MATERIALIZED VIEW test_5min_sessions;

-- 결과 검증
SELECT device_id, session_id, event_name, event_time, session_seq
FROM test_5min_sessions
WHERE device_id = 'test-device'
ORDER BY event_time;
```

**기대 결과:**

| event_name | session_seq | 예상 session_id |
|---|---|---|
| ev_a | 1 | session-001 |
| ev_b | 2 | session-001 (동일) |
| ev_c | 1 | session-002 (새 세션) |
| ev_d | 2 | session-002 (동일) |

---

### TC-SMV-003: COALESCE User Key SMV

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-SMV-003 |
| **테스트명** | COALESCE 복합 User Key (로그인/비로그인 통합) |
| **관련 요구사항** | FR-SMV-001 |
| **우선순위** | High |

**테스트 절차:**

```sql
CREATE SESSION MATERIALIZED VIEW page_events_user_sessions
FROM page_events
USER_KEY        = COALESCE(user_id, device_id)
SESSION_TIMEOUT = 30 MINUTE
REFRESH REALTIME;

-- 비로그인 이벤트 (user_id = NULL)
INSERT INTO page_events (event_time, user_id, device_id, event_name) VALUES
    ('2024-01-15 10:00:00', NULL,      'dev-001', 'page_view'),
    ('2024-01-15 10:05:00', 'usr-001', 'dev-001', 'login'),      -- 로그인
    ('2024-01-15 10:10:00', 'usr-001', 'dev-001', 'purchase');

-- 검증: user_id 있으면 user_id로, 없으면 device_id로 세션 구분
SELECT COALESCE(user_id, device_id) AS effective_key, session_id, event_name
FROM page_events_user_sessions
WHERE device_id = 'dev-001'
ORDER BY event_time;
```

**기대 결과:**
- `page_view`: effective_key = 'dev-001'
- `login`, `purchase`: effective_key = 'usr-001'
- 세션 ID는 2개 발생 (30분 이내이므로 분리 없음 → key가 다르므로 별도 세션)

---

## TC-DML: DML 테스트

---

### TC-DML-001: 단건 INSERT 및 즉시 조회

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-DML-001 |
| **테스트명** | 단건 INSERT 후 실시간 SELECT |
| **관련 요구사항** | FR-ING-001 |
| **우선순위** | Critical |

**테스트 절차:**

```sql
-- Step 1: INSERT
INSERT INTO page_events (event_time, device_id, event_name, page_url)
VALUES (NOW(), 'dml-test-device', 'test_event', 'https://example.com/test');

-- Step 2: 즉시 조회 (실시간 가시성 확인)
SELECT event_time, device_id, event_name, page_url
FROM page_events
WHERE device_id = 'dml-test-device'
ORDER BY event_time DESC
LIMIT 1;
```

**기대 결과:**
- Step 2에서 Step 1에서 삽입한 레코드가 즉시 조회됨
- 지연 허용: 1초 이내

---

### TC-DML-002: 배치 INSERT 성능

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-DML-002 |
| **테스트명** | 10,000건 배치 INSERT 처리 속도 |
| **관련 요구사항** | FR-ING-001, NFR-PERF |
| **우선순위** | High |

**테스트 절차:**

```sql
-- Python 또는 테스트 스크립트로 10,000건 배치 INSERT
-- 예시: 단일 INSERT 문에 1,000건씩 10회 실행

INSERT INTO page_events (event_time, device_id, event_name)
VALUES
    ('2024-01-15 10:00:01', 'perf-dev-0001', 'page_view'),
    ('2024-01-15 10:00:02', 'perf-dev-0002', 'page_view'),
    -- ... (1,000건)
    ('2024-01-15 10:00:00', 'perf-dev-1000', 'page_view');

-- 완료 후 수량 확인
SELECT COUNT(*) FROM page_events WHERE device_id LIKE 'perf-dev-%';
```

**기대 결과:**
- 10,000건 INSERT 완료 시간: 1초 이하
- 완료 후 `COUNT(*)` = 10,000 확인

---

### TC-DML-003: JSON properties INSERT 및 추출

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-DML-003 |
| **테스트명** | JSON 속성 삽입 및 JSON_VALUE 추출 |
| **관련 요구사항** | FR-QE-001 |
| **우선순위** | High |

**테스트 절차:**

```sql
INSERT INTO page_events (event_time, device_id, event_name, properties)
VALUES (
    '2024-01-15 11:00:00',
    'json-test-device',
    'purchase',
    '{"product_id": 42, "product_name": "노트북", "price": 1200000, "quantity": 1, "tags": ["electronics", "premium"]}'
);

-- JSON 추출 쿼리
SELECT
    JSON_VALUE(properties, '$.product_id')   AS product_id,
    JSON_VALUE(properties, '$.product_name') AS product_name,
    JSON_VALUE(properties, '$.price')        AS price,
    JSON_EXTRACT(properties, '$.tags')       AS tags
FROM page_events
WHERE device_id = 'json-test-device' AND event_name = 'purchase';
```

**기대 결과:**

| product_id | product_name | price | tags |
|---|---|---|---|
| 42 | 노트북 | 1200000 | ["electronics", "premium"] |

---

## TC-ING: Ingestion 테스트

---

### TC-ING-001: Kafka Routine Load 기본 동작

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-ING-001 |
| **테스트명** | Kafka Routine Load 등록 및 실시간 수집 확인 |
| **관련 요구사항** | FR-ING-002 |
| **우선순위** | Critical |

**사전 조건:**
- Kafka 클러스터 기동 (`kafka1:9092`)
- `web-events-test` 토픽 생성
- `page_events` Cube 존재

**테스트 절차:**

```sql
-- Step 1: Routine Load 생성
CREATE ROUTINE LOAD test_db.load_test ON page_events
PROPERTIES (
    "desired_concurrent_number" = "2",
    "max_batch_interval"        = "5",
    "max_batch_rows"            = "10000",
    "format"                    = "json",
    "jsonpaths"                 = "[\"$.ts\",\"$.did\",\"$.evt\",\"$.url\"]",
    "timezone"                  = "Asia/Seoul"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka1:9092",
    "kafka_topic"       = "web-events-test",
    "kafka_offsets"     = "OFFSET_END",
    "property.group.id" = "wowdb-test-loader"
);

-- Step 2: 상태 확인
SHOW ROUTINE LOAD FOR load_test;
```

```bash
# Step 3: Kafka에 테스트 메시지 발행 (외부 도구)
kafka-console-producer.sh --broker-list kafka1:9092 --topic web-events-test <<EOF
{"ts": "2024-01-15 10:00:00", "did": "kafka-dev-001", "evt": "page_view", "url": "https://ex.com"}
{"ts": "2024-01-15 10:00:05", "did": "kafka-dev-001", "evt": "click",     "url": "https://ex.com/btn"}
EOF
```

```sql
-- Step 4: 수집 확인 (30초 이내)
SELECT device_id, event_name, event_time
FROM page_events
WHERE device_id = 'kafka-dev-001'
ORDER BY event_time;
```

**기대 결과:**
- Step 2: `state = RUNNING`, `lag` 감소 확인
- Step 4: 2건 레코드 조회됨

---

### TC-ING-002: Kafka Ingestion 트랜잭션 장애 복구

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-ING-002 |
| **테스트명** | SL 노드 장애 시 Kafka Offset 보존 및 재처리 |
| **관련 요구사항** | FR-ING-002, FR-ING-004 |
| **우선순위** | High |

**테스트 시나리오:**

1. Kafka에 100건 메시지 발행
2. 50건 처리 중 DN Node 1 강제 종료 (`kill -9`)
3. DN Node 1 재기동
4. Kafka Offset 롤백 후 재처리 확인
5. 중복 없이 100건 정확히 적재됐는지 확인

**기대 결과:**
- SL 복구 후 남은 50건 자동 재처리
- 최종 `COUNT(*)` = 100 (중복 없음)
- Kafka Offset이 커밋된 위치까지만 처리됨 (Exactly-once)

---

### TC-ING-003: Spark 3.1 Batch Load

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-ING-003 |
| **테스트명** | Spark 3.1 커넥터로 1,000,000건 배치 적재 |
| **관련 요구사항** | FR-ING-003 |
| **우선순위** | Critical |

**사전 조건:**
- Spark 3.1 클러스터 기동
- `wowdb-spark-connector-1.0.0.jar` 드라이버 등록
- 1,000,000건 Parquet 파일 준비 (`hdfs:///test-data/events_1m.parquet`)

**테스트 절차:**

```bash
spark-submit \
  --master yarn \
  --jars wowdb-spark-connector-1.0.0.jar \
  --class WowDbSpark31Load \
  --conf spark.executor.instances=4 \
  --conf spark.executor.memory=8g \
  test-jobs.jar \
  --src hdfs:///test-data/events_1m.parquet \
  --target analytics.page_events
```

```sql
-- 적재 완료 후 확인
SELECT COUNT(*) FROM page_events WHERE device_id LIKE 'spark31-test-%';
```

**기대 결과:**
- Job 성공 종료 (Exit code 0)
- COUNT = 1,000,000
- 소요 시간: 3분 이내 (4 Executor 기준)

---

### TC-ING-004: Spark 3.4 Transaction Rollback

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-ING-004 |
| **테스트명** | Spark 3.4 트랜잭션 도중 실패 시 Rollback 확인 |
| **관련 요구사항** | FR-ING-003, FR-ING-004 |
| **우선순위** | High |

**테스트 시나리오:**

1. `wowdb.transaction.enable = true` 로 500,000건 로드 시작
2. 로드 50% 완료 시점에 네트워크 파티션 시뮬레이션
3. 트랜잭션 타임아웃 발생 확인
4. WOW-DB에 부분 데이터 없음 확인 (롤백 완료)

```sql
-- 롤백 확인
SELECT COUNT(*) FROM page_events WHERE device_id LIKE 'tx-test-%';
-- 기대: 0 (부분 적재 없음)
```

**기대 결과:**
- Spark Job 실패 보고 (`TransactionTimeoutException`)
- WOW-DB에 0건 (원자적 롤백 보장)

---

### TC-ING-005: Spark 3.4 Structured Streaming

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-ING-005 |
| **테스트명** | Spark 3.4 Structured Streaming 30초 마이크로배치 |
| **관련 요구사항** | FR-ING-003 |
| **우선순위** | High |

**테스트 절차:**

1. `WowDbSpark34Streaming` Job 기동
2. Kafka에 10분간 이벤트 지속 발행 (초당 10,000건)
3. 30초마다 WOW-DB에 적재되는지 확인
4. 10분 후 총 수량 검증

**기대 결과:**
- 30초마다 마이크로배치 커밋 로그 확인
- 최종 COUNT ≈ 6,000,000 ± 1% (허용 오차 내)
- 체크포인트 디렉토리에 오프셋 저장 확인

---

## TC-ANA: Analytics 쿼리 테스트

---

### TC-ANA-001: 기본 GROUP BY 집계

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-ANA-001 |
| **테스트명** | 일별 DAU(Daily Active Users) 집계 |
| **관련 요구사항** | FR-QE-001 |
| **우선순위** | Critical |

**사전 조건:** `page_events`에 2024-01-01 ~ 2024-01-07 기간 데이터 존재

**테스트 절차:**

```sql
SELECT
    DATE(event_time)              AS event_date,
    COUNT(DISTINCT device_id)     AS dau,
    COUNT(*)                      AS total_events
FROM page_events
WHERE event_time BETWEEN '2024-01-01' AND '2024-01-07 23:59:59'
GROUP BY event_date
ORDER BY event_date;
```

**기대 결과:**
- 7행 반환 (일별 1행)
- `dau` 및 `total_events` 값이 사전에 준비한 데이터와 일치
- 쿼리 응답 시간: 500ms 이하 (1억 건 이하 테이블 기준)

---

### TC-ANA-002: Funnel Analysis - 4단계 퍼널

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-ANA-002 |
| **테스트명** | 구매 전환 4단계 Funnel 분석 정확성 |
| **관련 요구사항** | FR-QE-002 |
| **우선순위** | Critical |

**사전 조건:**

다음 이벤트 시퀀스가 정확히 삽입되어 있어야 한다.

| device_id | events (순서) | 기대 Funnel Step |
|---|---|---|
| dev-A | landing → product → cart → purchase | Step 4 도달 |
| dev-B | landing → product → cart | Step 3 도달 |
| dev-C | landing → product | Step 2 도달 |
| dev-D | landing | Step 1 도달 |
| dev-E | product → cart → purchase (landing 없음) | 카운트 제외 |

**테스트 절차:**

```sql
SELECT
    step_counts[1] AS step1_landing,
    step_counts[2] AS step2_product,
    step_counts[3] AS step3_cart,
    step_counts[4] AS step4_purchase
FROM (
    SELECT
        FUNNEL_COUNT(
            event_name = 'landing_page' STEP 1,
            event_name = 'product_view' STEP 2,
            event_name = 'add_to_cart'  STEP 3,
            event_name = 'purchase'     STEP 4,
            TIME_WINDOW => INTERVAL 7 DAY,
            STRICT => TRUE
        ) AS step_counts
    FROM page_events_sessions
    WHERE session_start_time BETWEEN '2024-01-01' AND '2024-01-31'
) t;
```

**기대 결과:**

| step1 | step2 | step3 | step4 |
|---|---|---|---|
| 4 | 3 | 2 | 1 |

---

### TC-ANA-003: Funnel - TIME_WINDOW 경계 검증

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-ANA-003 |
| **테스트명** | TIME_WINDOW 초과 시 Funnel 카운트 제외 |
| **관련 요구사항** | FR-QE-002 |
| **우선순위** | High |

**사전 조건:**

```sql
-- dev-timeout: landing 후 8일 뒤 purchase (7일 윈도우 초과)
INSERT INTO page_events (event_time, device_id, event_name) VALUES
    ('2024-01-01 10:00:00', 'dev-timeout', 'landing_page'),
    ('2024-01-09 10:00:00', 'dev-timeout', 'purchase');  -- 8일 후

-- dev-ok: landing 후 6일 뒤 purchase (7일 윈도우 이내)
INSERT INTO page_events (event_time, device_id, event_name) VALUES
    ('2024-01-01 10:00:00', 'dev-ok', 'landing_page'),
    ('2024-01-07 10:00:00', 'dev-ok', 'purchase');  -- 6일 후
```

**기대 결과:**
- `step2_purchase`: 1 (dev-ok만 카운트, dev-timeout 제외)

---

### TC-ANA-004: Cohort Retention Analysis

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-ANA-004 |
| **테스트명** | 주간 코호트 리텐션 분석 정확성 |
| **관련 요구사항** | FR-QE-003 |
| **우선순위** | Critical |

**사전 조건:**

- Week 1 코호트(2024-01-01~07): 100명 first_visit
  - Week 2에 70명 재방문 (70% 리텐션 기대)
  - Week 3에 50명 재방문 (50% 기대)

**테스트 절차:**

```sql
SELECT
    cohort_date,
    period,
    cohort_size,
    returned_users,
    ROUND(metric_value * 100, 1) AS retention_pct
FROM COHORT_ANALYSIS(
    SOURCE       => 'page_events_sessions',
    ENTRY_EVENT  => 'first_visit',
    RETURN_EVENT => 'any_event',
    COHORT_DATE  => DATE(session_start_time),
    METRIC       => 'RETENTION_RATE',
    TIME_UNIT    => 'WEEK',
    MAX_PERIODS  => 3,
    USER_KEY     => 'device_id'
)
WHERE cohort_date = '2024-01-01'
ORDER BY period;
```

**기대 결과:**

| cohort_date | period | cohort_size | returned_users | retention_pct |
|---|---|---|---|---|
| 2024-01-01 | 0 | 100 | 100 | 100.0 |
| 2024-01-01 | 1 | 100 | 70 | 70.0 |
| 2024-01-01 | 2 | 100 | 50 | 50.0 |

---

### TC-ANA-005: Path Analysis

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-ANA-005 |
| **테스트명** | 사용자 이동 경로 분석 및 지원율 계산 |
| **관련 요구사항** | FR-QE-004 |
| **우선순위** | High |

**테스트 절차:**

```sql
SELECT
    path,
    path_count,
    unique_users,
    ROUND(support_rate * 100, 2) AS support_pct
FROM PATH_ANALYSIS(
    SOURCE       => 'page_events_sessions',
    PATH_EVENT   => 'event_name',
    USER_KEY     => 'device_id',
    SESSION_KEY  => 'session_id',
    MAX_DEPTH    => 4,
    MIN_SUPPORT  => 0.01,
    ENTRY_FILTER => "event_name = 'landing_page'"
)
ORDER BY path_count DESC
LIMIT 10;
```

**기대 결과:**
- 최소 1건 이상 경로 반환
- `support_pct` 합계 ≤ 100%
- `path_count` 내림차순 정렬 확인
- `path` 형식: `'landing_page → product_view → ...'`

---

### TC-ANA-006: 윈도우 함수 (LAG, ROW_NUMBER)

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-ANA-006 |
| **테스트명** | LAG / ROW_NUMBER 윈도우 함수 정확성 |
| **관련 요구사항** | FR-QE-001 |
| **우선순위** | High |

**테스트 절차:**

```sql
SELECT
    device_id,
    event_time,
    event_name,
    ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY event_time) AS seq,
    LAG(event_name, 1) OVER (PARTITION BY device_id ORDER BY event_time) AS prev_event,
    TIMESTAMPDIFF(SECOND,
        LAG(event_time, 1) OVER (PARTITION BY device_id ORDER BY event_time),
        event_time
    ) AS sec_since_prev
FROM page_events
WHERE device_id = 'window-test-device'
ORDER BY event_time;
```

**기대 결과:**
- `seq`: 1부터 시작하는 순차 번호
- 첫 번째 행 `prev_event`: NULL
- `sec_since_prev`: 첫 번째 행 NULL, 이후 실제 시간 차이 (초)

---

## TC-COMPAT: MySQL 호환성 테스트

---

### TC-COMPAT-001: MySQL 8.0 CLI 접속

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-COMPAT-001 |
| **테스트명** | MySQL 공식 CLI로 WOW-DB 접속 |
| **관련 요구사항** | FR-COMPAT |
| **우선순위** | Critical |

**테스트 절차:**

```bash
# MySQL CLI (8.0.x)로 WOW-DB 접속
mysql -h wowdb-qn-1 -P 9030 -u admin -p analytics

# 접속 성공 확인
SELECT VERSION();
SHOW DATABASES;
SHOW TABLES;
```

**기대 결과:**
- 접속 성공 (`mysql>` 프롬프트 표시)
- `VERSION()`: `8.0.x-WOW-DB-1.0.0` 형태 반환
- `SHOW DATABASES`: analytics 포함 목록
- `SHOW TABLES`: page_events 포함 목록

---

### TC-COMPAT-002: JDBC 드라이버 연결

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-COMPAT-002 |
| **테스트명** | MySQL JDBC 8.x 드라이버로 Connection Pool 연결 |
| **관련 요구사항** | FR-COMPAT |
| **우선순위** | Critical |

**테스트 절차:**

```java
// MySQL JDBC 8.0.x
String url = "jdbc:mysql://wowdb-qn-1:9030/analytics"
           + "?useSSL=false&allowPublicKeyRetrieval=true";
Connection conn = DriverManager.getConnection(url, "admin", "password");

// 메타데이터 확인
DatabaseMetaData meta = conn.getMetaData();
System.out.println(meta.getDatabaseProductName());  // 기대: WOW-DB

// 쿼리 실행
PreparedStatement ps = conn.prepareStatement(
    "SELECT COUNT(*) FROM page_events WHERE event_time > ?"
);
ps.setTimestamp(1, Timestamp.valueOf("2024-01-01 00:00:00"));
ResultSet rs = ps.executeQuery();
```

**기대 결과:**
- 연결 성공
- `DatabaseProductName`: "WOW-DB" 반환
- PreparedStatement 정상 실행

---

### TC-COMPAT-003: information_schema 조회

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-COMPAT-003 |
| **테스트명** | MySQL information_schema 호환 조회 |
| **관련 요구사항** | FR-COMPAT |
| **우선순위** | High |

**테스트 절차:**

```sql
-- 테이블 목록
SELECT TABLE_NAME, TABLE_TYPE, ENGINE
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'analytics';

-- 컬럼 목록
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_COMMENT
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'analytics' AND TABLE_NAME = 'page_events'
ORDER BY ORDINAL_POSITION;
```

**기대 결과:**
- `information_schema.TABLES`: Cube 목록 반환 (`TABLE_TYPE = 'BASE TABLE'`)
- `information_schema.COLUMNS`: Cube 컬럼 정보 정확히 반환

---

### TC-COMPAT-004: EXPLAIN 실행 계획

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-COMPAT-004 |
| **테스트명** | EXPLAIN PHYSICAL 실행 계획 출력 |
| **관련 요구사항** | FR-QE-005 |
| **우선순위** | Medium |

**테스트 절차:**

```sql
EXPLAIN PHYSICAL
SELECT DATE(event_time) AS dt, COUNT(*) AS cnt
FROM page_events
WHERE event_time >= '2024-01-01'
GROUP BY dt;
```

**기대 결과:**
- 계획 트리 출력 (DN/CN 노드별 실행 계획 포함)
- 파티션 Pruning 적용 여부 표시
- 예상 행 수 (Estimated rows) 표시

---

## TC-PERF: 성능 테스트

---

### TC-PERF-001: 10억 행 GROUP BY 쿼리

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-PERF-001 |
| **테스트명** | 10억 행 테이블 일별 집계 P99 응답 시간 |
| **관련 요구사항** | NFR-PERF |
| **우선순위** | Critical |

**사전 조건:**
- `page_events`에 10억 건 데이터 적재
- SL 10 노드 클러스터

**테스트 절차:**

```sql
-- 100회 반복 실행 후 P99 측정
SELECT
    DATE(event_time)           AS event_date,
    COUNT(DISTINCT device_id)  AS dau,
    COUNT(*)                   AS event_count
FROM page_events
WHERE event_time BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY event_date
ORDER BY event_date;
```

**기대 결과:**

| 지표 | 기대값 |
|---|---|
| P50 응답 시간 | ≤ 500ms |
| P99 응답 시간 | ≤ 3,000ms |
| CPU 활용률 | ≥ 80% (SIMD 활용 확인) |
| IO 처리량 | ≥ 5 GB/s (Block Cache Miss 시) |

---

### TC-PERF-002: Kafka 수집 처리량

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-PERF-002 |
| **테스트명** | Kafka Routine Load 초당 수집 처리량 |
| **관련 요구사항** | NFR-PERF |
| **우선순위** | High |

**테스트 절차:**

1. Kafka에 초당 200,000건 메시지 발행 (10분간)
2. WOW-DB 수집 Lag 모니터링
3. `SHOW ROUTINE LOAD` 로 초당 처리 건수 확인

**기대 결과:**
- 안정 상태 처리량: ≥ 1,000,000 건/초 (10 SL 노드 기준)
- Kafka Lag: 10분 이내 따라잡기 (실시간 수준)
- 메모리 사용량: SL 노드당 < 90%

---

### TC-PERF-003: 동시 쿼리 처리

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-PERF-003 |
| **테스트명** | 200 동시 쿼리 처리 안정성 |
| **관련 요구사항** | NFR-PERF |
| **우선순위** | High |

**테스트 절차:**

```bash
# 부하 테스트 도구 (예: sysbench 또는 custom Python script)
# 200개 스레드가 동시에 아래 쿼리 실행 (10분간)
python load_test.py \
  --threads 200 \
  --duration 600 \
  --query "SELECT DATE(event_time), COUNT(*) FROM page_events WHERE event_time >= '2024-01-01' GROUP BY 1 ORDER BY 1"
```

**기대 결과:**

| 지표 | 기대값 |
|---|---|
| 성공률 | ≥ 99.9% |
| P99 응답 시간 | ≤ 5,000ms |
| 오류 수 | 0 (타임아웃 오류 포함 ≤ 0.1%) |

---

## TC-WEB: Web SQL Client 테스트

---

### TC-WEB-001: Session MV 대화형 생성 (Web UI)

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-WEB-001 |
| **테스트명** | Web UI에서 Cube 생성 후 Session MV 대화 흐름 |
| **관련 요구사항** | FR-WEB-002, FR-SMV-001 |
| **우선순위** | Critical |

**테스트 절차:**

1. 브라우저에서 `http://wowdb-dsl-1:8080` 접속
2. "Cube Builder" 탭 클릭
3. GUI로 컬럼 정의 후 "Create Cube" 클릭
4. **팝업 확인**: "Session Materialized View를 생성하시겠습니까?" 표시 여부
5. "예" 클릭
6. User Key 드롭다운에서 `device_id` 선택
7. Session Timeout에서 "30분" 버튼 클릭
8. MV 이름 확인 (자동 제안값 표시 여부)
9. "생성" 클릭
10. 완료 알림 및 MV 미리보기 확인

**기대 결과:**

| 스텝 | 기대 결과 |
|---|---|
| Step 4 | 팝업 정확히 표시됨 |
| Step 6 | Cube 컬럼 목록 드롭다운 표시 |
| Step 8 | `{cube_name}_sessions` 자동 제안 |
| Step 10 | SMV 컬럼 목록 + 샘플 데이터 미리보기 |

---

### TC-WEB-002: Web SQL 에디터 기본 동작

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-WEB-002 |
| **테스트명** | SQL 에디터 자동완성 및 결과 출력 |
| **관련 요구사항** | FR-WEB-001 |
| **우선순위** | High |

**테스트 절차:**

1. Web SQL Editor 탭 열기
2. `SELECT * FROM pa` 입력 후 자동완성 팝업 확인 (`page_events` 제안)
3. 자동완성 선택 후 `LIMIT 10` 추가
4. Ctrl+Enter (또는 실행 버튼)으로 쿼리 실행
5. 결과 테이블 표시 확인
6. "CSV 다운로드" 버튼 클릭

**기대 결과:**
- Step 2: `page_events`, `page_events_sessions` 자동완성 제안
- Step 4: 결과 테이블 10행 표시 (1초 이내)
- Step 6: CSV 파일 다운로드 성공

---

### TC-WEB-003: EXPLAIN 시각화

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-WEB-003 |
| **테스트명** | Web에서 EXPLAIN PHYSICAL 결과 트리 시각화 |
| **관련 요구사항** | FR-WEB-001 |
| **우선순위** | Medium |

**테스트 절차:**

1. SQL 에디터에서 `EXPLAIN PHYSICAL SELECT ...` 실행
2. 우측 패널에 실행 계획 트리 시각화 표시 확인

**기대 결과:**
- 트리 형태로 각 노드(Scan, Aggregate, Exchange 등) 표시
- 노드 클릭 시 상세 정보 (예상 행 수, 처리 비용) 팝오버 표시

---

## TC-QN: Query Node 테스트

---

### TC-QN-001: Raft 리더 선출 및 홀수 구성 검증

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-QN-001 |
| **테스트명** | QN 3노드 Raft 리더 선출 및 메타데이터 동기화 |
| **관련 요구사항** | FR-QN-001, FR-QN-002 |
| **우선순위** | Critical |

**사전 조건:**
- QN 3노드 기동 (`qn-1`, `qn-2`, `qn-3`, 포트 9010 Raft)
- 각 노드 `wow.conf` 에 `[raft] peers = ["qn-1:9010","qn-2:9010","qn-3:9010"]` 설정

**테스트 절차:**

```bash
# Step 1: 리더 확인
curl -s http://qn-1:9011/raft/status | jq '.role'
curl -s http://qn-2:9011/raft/status | jq '.role'
curl -s http://qn-3:9011/raft/status | jq '.role'
```

```sql
-- Step 2: 임의 QN에서 DDL 실행
CREATE CUBE raft_test (event_time DATETIME NOT NULL, device_id VARCHAR(64))
ENGINE = WOW_LSM DISTRIBUTED BY HASH(device_id) BUCKETS 4;

-- Step 3: Follower QN에서 메타데이터 반영 확인
-- qn-2, qn-3에 각각 접속
SHOW CUBES;
```

```bash
# Step 4: 짝수 구성 시도 (4노드) → 경고 확인
curl -X POST http://qn-1:9011/raft/add-peer -d '{"peer":"qn-4:9010"}'
```

**기대 결과:**

| 스텝 | 기대 결과 |
|---|---|
| Step 1 | 3노드 중 정확히 1개만 `"Leader"`, 나머지 2개 `"Follower"` |
| Step 2 | DDL 성공 (`Query OK`) |
| Step 3 | 모든 QN에서 `raft_test` Cube 목록 포함 (메타 동기화 확인) |
| Step 4 | `WARN: Even number of Raft peers (4) detected. Recommend odd number.` |

**Pass 기준:** Step 1~3 모두 충족 시 Pass

---

### TC-QN-002: Stateless K8s 동일 엔드포인트 응답 검증

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-QN-002 |
| **테스트명** | K8s LoadBalancer 통해 임의 QN Pod에 접속해도 동일 결과 반환 |
| **관련 요구사항** | FR-QN-003 |
| **우선순위** | Critical |

**사전 조건:**
- K8s 클러스터에 QN 3개 Pod 배포 (`wow-qn-0`, `wow-qn-1`, `wow-qn-2`)
- K8s Service (ClusterIP/LoadBalancer) `wow-qn-svc:9030` 설정
- `page_events` Cube + 데이터 10,000건 존재

**테스트 절차:**

```bash
# Step 1: 각 Pod에 직접 접속하여 동일 쿼리 실행
for pod in wow-qn-0 wow-qn-1 wow-qn-2; do
  echo "=== $pod ==="
  kubectl exec $pod -- mysql -P 9030 -u admin -p -e \
    "SELECT COUNT(*) FROM page_events WHERE event_time >= '2024-01-01';"
done

# Step 2: LoadBalancer 경유로 100회 반복 실행 (랜덤 Pod 라우팅)
for i in $(seq 1 100); do
  mysql -h wow-qn-svc -P 9030 -u admin -p -e \
    "SELECT COUNT(*) FROM page_events;" 2>/dev/null
done | sort | uniq -c
```

**기대 결과:**
- Step 1: 3개 Pod 모두 동일한 `COUNT(*)` 값 반환
- Step 2: 100회 결과가 모두 동일한 단일 값 (분산 라우팅에 관계없이 일관성 보장)

**Pass 기준:** 모든 응답값이 동일한 숫자일 때 Pass

---

### TC-QN-003: QN 리더 장애 시 Failover

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-QN-003 |
| **테스트명** | QN Leader 강제 종료 후 새 리더 선출 및 서비스 복구 |
| **관련 요구사항** | FR-QN-001, FR-QN-002 |
| **우선순위** | High |

**테스트 시나리오:**

1. `qn-1` (현재 Leader) 강제 종료 (`kill -9`)
2. 30초 이내에 `qn-2` 또는 `qn-3`가 새 Leader로 선출되는지 확인
3. 새 Leader에 DDL/DML 실행 가능 여부 확인
4. `qn-1` 재기동 후 Follower로 복귀 및 메타데이터 동기화 확인

```bash
# Step 2: 새 리더 확인 (qn-2에 질의)
sleep 10 && curl -s http://qn-2:9011/raft/status | jq '.role'
```

```sql
-- Step 3: 새 리더에서 DML 실행
INSERT INTO page_events (event_time, device_id, event_name)
VALUES (NOW(), 'failover-test', 'qn_failover_event');
SELECT COUNT(*) FROM page_events WHERE device_id = 'failover-test';
```

**기대 결과:**
- Step 2: `qn-2` 또는 `qn-3` 가 `"Leader"` 로 변경됨 (30초 이내)
- Step 3: INSERT 및 SELECT 정상 실행
- Step 4: `qn-1` 재기동 후 `"Follower"` 상태로 복귀, 신규 메타데이터 동기화 확인

**Pass 기준:** Failover 30초 이내, INSERT/SELECT 무중단 확인 시 Pass

---

## TC-CBO: Cost-Based Optimizer 테스트

---

### TC-CBO-001: ANALYZE TABLE 통계 수집

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-CBO-001 |
| **테스트명** | ANALYZE TABLE 실행 후 SHOW STATS 통계 항목 확인 |
| **관련 요구사항** | FR-CBO-001 |
| **우선순위** | Critical |

**사전 조건:** `page_events` Cube에 1,000,000건 이상 데이터 존재

**테스트 절차:**

```sql
-- Step 1: 통계 수집
ANALYZE TABLE page_events;

-- Step 2: 전체 테이블 통계 확인
SHOW STATS FOR page_events;

-- Step 3: 컬럼별 상세 통계 확인
SHOW STATS FOR page_events COLUMNS (event_time, device_id, event_name);
```

**기대 결과:**

| 스텝 | 기대 결과 |
|---|---|
| Step 1 | `Analyze complete. X rows analyzed.` |
| Step 2 | `row_count`, `partition_count`, `last_analyzed` 필드 포함 반환 |
| Step 3 | 컬럼별 `min`, `max`, `ndv` (Distinct Value Count), `null_count`, `histogram` 포함 |

**Pass 기준:** Step 3에서 모든 5개 통계 항목 (min/max/ndv/null_count/histogram) 반환 시 Pass

---

### TC-CBO-002: 파티션 프루닝 효과 검증

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-CBO-002 |
| **테스트명** | WHERE 절 파티션 키 조건으로 파티션 프루닝 확인 |
| **관련 요구사항** | FR-CBO-002 |
| **우선순위** | Critical |

**사전 조건:**
- Range 파티션 `page_events_partitioned` (p_2024_q1, p_2024_q2, p_future)
- ANALYZE TABLE 완료

**테스트 절차:**

```sql
-- Step 1: 전체 파티션 스캔 쿼리 (프루닝 없음)
EXPLAIN PHYSICAL
SELECT COUNT(*) FROM page_events_partitioned;

-- Step 2: 특정 파티션 범위 쿼리 (p_2024_q1만 스캔 기대)
EXPLAIN PHYSICAL
SELECT COUNT(*) FROM page_events_partitioned
WHERE event_time BETWEEN '2024-01-01' AND '2024-03-31';

-- Step 3: 실제 수행 시간 비교
SELECT COUNT(*) FROM page_events_partitioned;  -- 전체
SELECT COUNT(*) FROM page_events_partitioned
WHERE event_time BETWEEN '2024-01-01' AND '2024-03-31';  -- 프루닝
```

**기대 결과:**
- Step 1: `partitions: [p_2024_q1, p_2024_q2, p_future]` (전체 3개)
- Step 2: `partitions: [p_2024_q1]` (1개만 스캔)
- Step 3: 프루닝 쿼리가 전체 스캔 대비 수행 시간 ≤ 40% (2.5배 이상 빠름)

**Pass 기준:** Step 2에서 `p_2024_q1` 1개 파티션만 표시 시 Pass

---

### TC-CBO-003: NDV 기반 조인 순서 최적화

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-CBO-003 |
| **테스트명** | NDV 통계 기반으로 CBO가 소형 테이블을 Build Side로 선택 |
| **관련 요구사항** | FR-CBO-003 |
| **우선순위** | High |

**사전 조건:**
- `page_events` (대형, 1억 건)
- `dim_devices` (소형, 1만 건) Cube 생성 후 ANALYZE TABLE 수행

**테스트 절차:**

```sql
-- Step 1: 조인 쿼리 EXPLAIN
EXPLAIN PHYSICAL
SELECT p.event_name, d.device_brand, COUNT(*) AS cnt
FROM page_events p
JOIN dim_devices d ON p.device_id = d.device_id
WHERE p.event_time >= '2024-01-01'
GROUP BY p.event_name, d.device_brand;
```

**기대 결과:**
- `JOIN type: HashJoin`
- `Build side: dim_devices` (소형 테이블이 Build side)
- `Probe side: page_events` (대형 테이블이 Probe side)
- EXPLAIN 출력에 추정 행 수 (`estimated_rows`) 표시

**Pass 기준:** Build/Probe 역할 할당이 통계 기반으로 올바를 때 Pass

---

## TC-CN: Compute Node 테스트

---

### TC-CN-001: CN 코-로케이션 모드 vs 분리 모드 결과 일치

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-CN-001 |
| **테스트명** | DN 코-로케이션 CN과 독립형 CN의 쿼리 결과 동일성 |
| **관련 요구사항** | FR-CN-001 |
| **우선순위** | High |

**사전 조건:**
- 환경 A: CN이 DN과 동일 호스트에 배포 (코-로케이션 모드)
- 환경 B: CN이 독립 호스트에 배포 (분리 모드)
- 동일한 `page_events` 데이터셋 (동일 스냅샷)

**테스트 절차:**

```sql
-- 환경 A와 환경 B에서 동일 쿼리 각각 실행
SELECT
    DATE(event_time)             AS event_date,
    COUNT(DISTINCT device_id)    AS dau,
    COUNT(*)                     AS total_events,
    AVG(session_duration_sec)    AS avg_session_sec
FROM page_events_sessions
WHERE event_time BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY event_date
ORDER BY event_date;
```

**기대 결과:**
- 환경 A와 환경 B의 모든 행이 동일 (행 수, 각 셀 값)
- 수행 시간 차이는 허용 (코-로케이션이 보통 빠름)

**Pass 기준:** 결과 집합 100% 일치 시 Pass

---

### TC-CN-002: CN 노드 장애 시 쿼리 재실행 및 복구

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-CN-002 |
| **테스트명** | 쿼리 실행 중 CN 장애 시 오류 반환 및 재시도 동작 |
| **관련 요구사항** | FR-CN-002 |
| **우선순위** | High |

**테스트 시나리오:**

1. 복수의 CN (cn-1, cn-2) 환경에서 대형 쿼리 실행 시작 (예상 소요 30초 이상)
2. 쿼리 실행 중 `cn-1` 강제 종료
3. QN이 `cn-2`로 Fragment Plan 재분배하는지 확인
4. 쿼리 최종 성공 또는 명확한 오류 메시지 반환 확인

```sql
-- 대형 집계 쿼리 (30초+ 소요 데이터)
SELECT event_name, COUNT(DISTINCT device_id) AS uu
FROM page_events
GROUP BY event_name
ORDER BY uu DESC;
```

**기대 결과:**
- CN 장애 후 QN이 남은 CN으로 재분배 시도
- 가용 CN 존재 시 쿼리 최종 완료
- 가용 CN 없을 때 `ERROR: All Compute Nodes unavailable` 명확 반환

**Pass 기준:** 재분배 성공 시 결과 정상 반환, CN 전무 시 명확 오류 메시지 Pass

---

## TC-DN: Data Node 테스트

---

### TC-DN-001: S3 백엔드 적재 및 조회

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-DN-001 |
| **테스트명** | DN S3 백엔드 설정 후 데이터 적재 및 SELECT 조회 |
| **관련 요구사항** | FR-DN-002 |
| **우선순위** | Critical |

**사전 조건:**

```toml
# dn.conf (S3 설정)
[storage]
backend       = "s3"
s3_bucket     = "wowdb-test-bucket"
s3_region     = "ap-northeast-2"
s3_prefix     = "wowdb/data/"
aws_access_key = "AKIAXXXXXXXXXXXXXXXX"
aws_secret_key = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
```

**테스트 절차:**

```sql
-- Step 1: S3 백엔드 DN에 Cube 생성
CREATE CUBE s3_events (
    event_time DATETIME NOT NULL,
    device_id  VARCHAR(64) NOT NULL,
    event_name VARCHAR(128) NOT NULL
)
ENGINE = WOW_LSM
DISTRIBUTED BY HASH(device_id) BUCKETS 4
ORDER BY (event_time, device_id)
PROPERTIES ("storage_backend" = "s3");

-- Step 2: 데이터 적재 (10,000건)
INSERT INTO s3_events
SELECT event_time, device_id, event_name FROM page_events LIMIT 10000;

-- Step 3: 조회
SELECT COUNT(*) FROM s3_events;
SELECT event_name, COUNT(*) FROM s3_events GROUP BY event_name ORDER BY 2 DESC LIMIT 5;
```

```bash
# Step 4: S3에 실제 파일 생성 확인
aws s3 ls s3://wowdb-test-bucket/wowdb/data/ --recursive | head -20
```

**기대 결과:**
- Step 2: INSERT 성공
- Step 3: `COUNT(*)` = 10,000; GROUP BY 결과 정상
- Step 4: `seg_XXXX.col`, `seg_XXXX.min_max`, `seg_XXXX.bloom` 파일이 S3에 존재

**Pass 기준:** Step 3, Step 4 모두 충족 시 Pass

---

### TC-DN-002: HDFS + Kerberos 인증 적재 및 조회

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-DN-002 |
| **테스트명** | HDFS Kerberos 인증 후 DN 데이터 적재 및 조회 |
| **관련 요구사항** | FR-DN-003 |
| **우선순위** | Critical |

**사전 조건:**

```toml
# dn.conf (HDFS + Kerberos 설정)
[storage]
backend                   = "hdfs"
namenode                  = "hdfs://namenode:8020"
kerberos_keytab           = "/etc/security/keytabs/wowdb.keytab"
kerberos_principal        = "wowdb/dn-host@REALM.COM"
kerberos_renew_interval_sec = 3600
```

- KDC 서버 기동, `wowdb.keytab` 발급 완료
- HDFS에 `/wowdb/data/` 경로에 wowdb 서비스 계정 쓰기 권한 부여

**테스트 절차:**

```bash
# Step 1: Kerberos 티켓 획득 확인
klist -kt /etc/security/keytabs/wowdb.keytab

# Step 2: HDFS 접근 테스트
hdfs dfs -ls /wowdb/data/
```

```sql
-- Step 3: HDFS 백엔드 Cube 생성
CREATE CUBE hdfs_events (
    event_time DATETIME NOT NULL,
    device_id  VARCHAR(64) NOT NULL,
    event_name VARCHAR(128) NOT NULL
)
ENGINE = WOW_LSM
DISTRIBUTED BY HASH(device_id) BUCKETS 4
ORDER BY (event_time, device_id)
PROPERTIES ("storage_backend" = "hdfs");

-- Step 4: 데이터 적재
INSERT INTO hdfs_events
SELECT event_time, device_id, event_name FROM page_events LIMIT 5000;

-- Step 5: 조회 확인
SELECT COUNT(*) FROM hdfs_events;
```

```bash
# Step 6: HDFS 파일 생성 확인
hdfs dfs -ls /wowdb/data/hdfs_events/
```

**기대 결과:**
- Step 1: keytab의 Principal 목록 정상 출력
- Step 2: HDFS 경로 접근 성공
- Step 5: `COUNT(*)` = 5,000
- Step 6: 컬럼별 `.col`, `.bloom`, `.min_max` 파일 HDFS에 존재

**Kerberos 인증 실패 케이스:**
- keytab 삭제 후 DN 재기동 → `ERROR: Kerberos authentication failed for principal wowdb/dn-host@REALM.COM` 반환 확인

**Pass 기준:** 정상 케이스 Pass + 인증 실패 케이스 오류 메시지 정확 시 Pass

---

### TC-DN-003: 컬럼별 파일 분리 저장 구조 검증

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-DN-003 |
| **테스트명** | 데이터 적재 후 DN 로컬 파일 시스템에서 컬럼별 파일 분리 확인 |
| **관련 요구사항** | FR-DN-001 |
| **우선순위** | High |

**사전 조건:**
- Native LSM 백엔드 DN (`storage.backend = "native"`)
- `page_events` Cube에 100,000건 적재 + flush 완료 (메모리 → 디스크)

**테스트 절차:**

```bash
# Step 1: DN 데이터 디렉토리 구조 확인
find /data/wowdb/analytics/page_events -type f | sort

# Step 2: 컬럼 파일 형식 확인
ls /data/wowdb/analytics/page_events/partition=p_2024_q1/event_name/

# Step 3: min_max 파일 존재 확인 (파티션 프루닝용)
ls /data/wowdb/analytics/page_events/partition=p_2024_q1/event_time/

# Step 4: _meta 디렉토리 확인
cat /data/wowdb/analytics/page_events/partition=p_2024_q1/_meta/stats.json
```

**기대 결과:**

```
# Step 1 예상 출력 (일부):
.../page_events/partition=p_2024_q1/device_id/seg_0001.col
.../page_events/partition=p_2024_q1/device_id/seg_0001.bloom
.../page_events/partition=p_2024_q1/event_name/seg_0001.col
.../page_events/partition=p_2024_q1/event_name/seg_0001.dict
.../page_events/partition=p_2024_q1/event_time/seg_0001.col
.../page_events/partition=p_2024_q1/event_time/seg_0001.min_max
.../page_events/partition=p_2024_q1/_meta/schema.json
.../page_events/partition=p_2024_q1/_meta/stats.json

# Step 4 예상 stats.json:
{"row_count": 100000, "min_event_time": "...", "max_event_time": "..."}
```

**Pass 기준:** 각 컬럼이 독립 디렉토리에 `.col` 파일로 존재하고 `_meta/stats.json` 포함 시 Pass

---

## TC-MON: Monitoring 테스트

---

### TC-MON-001: SHOW CLUSTER STATUS 전체 노드 상태 조회

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-MON-001 |
| **테스트명** | SHOW CLUSTER STATUS로 QN/CN/DN 전체 노드 상태 및 메트릭 확인 |
| **관련 요구사항** | FR-MON-001 |
| **우선순위** | Critical |

**사전 조건:** QN 3 + CN 2 + DN 3 = 8노드 클러스터 정상 기동

**테스트 절차:**

```sql
-- Step 1: 전체 클러스터 상태 조회
SHOW CLUSTER STATUS;

-- Step 2: 특정 노드 타입 필터
SHOW CLUSTER STATUS WHERE node_type = 'QN';
SHOW CLUSTER STATUS WHERE node_type = 'DN';

-- Step 3: DN 1개 강제 종료 후 상태 확인
-- (외부에서 kill -9 후 10초 대기)
SHOW CLUSTER STATUS WHERE node_type = 'DN';
```

**기대 결과:**

| 스텝 | 기대 결과 |
|---|---|
| Step 1 | 8개 노드 행 반환, 각 행에 `node_id`, `node_type`, `host`, `port`, `status`, `cpu_pct`, `mem_pct`, `disk_pct` 컬럼 |
| Step 2 (QN) | 3행, `status = ALIVE`, `raft_role` 컬럼 포함 (Leader/Follower 구분) |
| Step 3 | 종료된 DN의 `status = DEAD` 또는 `UNREACHABLE` 표시 |

**Pass 기준:** Step 1, 2 충족 + Step 3에서 장애 DN 상태 변경 감지 시 Pass

---

### TC-MON-002: Prometheus /metrics 엔드포인트

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-MON-002 |
| **테스트명** | 각 노드 :9090/metrics 엔드포인트가 Prometheus 형식 메트릭 반환 |
| **관련 요구사항** | FR-MON-002 |
| **우선순위** | High |

**테스트 절차:**

```bash
# Step 1: 각 노드 유형별 /metrics 엔드포인트 확인
curl -s http://qn-1:9090/metrics | grep -E "^wowdb_"
curl -s http://cn-1:9090/metrics | grep -E "^wowdb_"
curl -s http://dn-1:9090/metrics | grep -E "^wowdb_"

# Step 2: QN 핵심 메트릭 항목 확인
curl -s http://qn-1:9090/metrics | grep -E "wowdb_qn_(query_total|active_connections|raft_term)"

# Step 3: DN 핵심 메트릭 항목 확인
curl -s http://dn-1:9090/metrics | grep -E "wowdb_dn_(lsm_compaction|disk_used_bytes|tablet_count)"
```

**기대 결과:**

| 메트릭 키 | 노드 타입 | 기대 존재 여부 |
|---|---|---|
| `wowdb_qn_query_total` | QN | 존재 |
| `wowdb_qn_active_connections` | QN | 존재 |
| `wowdb_qn_raft_term` | QN | 존재 |
| `wowdb_cn_fragment_execution_total` | CN | 존재 |
| `wowdb_cn_simd_rows_processed_total` | CN | 존재 |
| `wowdb_dn_lsm_compaction_total` | DN | 존재 |
| `wowdb_dn_disk_used_bytes` | DN | 존재 |
| `wowdb_dn_tablet_count` | DN | 존재 |

**Pass 기준:** 표에 나열된 8개 메트릭이 모두 각 노드에서 반환될 때 Pass

---

## TC-PROF: Query Profiler 테스트

---

### TC-PROF-001: SHOW QUERY PROFILE 조회 및 항목 검증

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-PROF-001 |
| **테스트명** | 쿼리 실행 후 SHOW QUERY PROFILE로 실행 기록 확인 |
| **관련 요구사항** | FR-PROF-001 |
| **우선순위** | Critical |

**사전 조건:** QN 접속 완료

**테스트 절차:**

```sql
-- Step 1: 쿼리 실행 (프로파일 대상)
SELECT DATE(event_time) AS dt, COUNT(*) AS cnt
FROM page_events
WHERE event_time >= '2024-01-01'
GROUP BY dt ORDER BY dt;

-- Step 2: 최근 쿼리 목록 조회
SHOW QUERY PROFILE;

-- Step 3: 특정 쿼리 상세 조회 (query_id는 Step 2에서 확인)
SHOW QUERY PROFILE FOR '{query_id}';
```

**기대 결과:**

| 스텝 | 기대 결과 |
|---|---|
| Step 2 | `query_id`, `start_time`, `end_time`, `duration_ms`, `status`, `sql_text` 컬럼 포함 |
| Step 2 | Step 1의 SELECT 쿼리가 최상단에 위치 |
| Step 3 | Fragment별 실행 시간, CN별 처리 행 수, DN별 스캔 행 수, SIMD 적용 여부 포함 |

**Pass 기준:** Step 2, 3 모두 기대 결과 충족 시 Pass

---

### TC-PROF-002: 1,000개 순환 버퍼 한도 검증

| 항목 | 내용 |
|---|---|
| **TC ID** | TC-PROF-002 |
| **테스트명** | 1,001건째 쿼리 실행 시 가장 오래된 기록이 삭제되는 순환 버퍼 동작 |
| **관련 요구사항** | FR-PROF-002 |
| **우선순위** | High |

**테스트 절차:**

```bash
# Step 1: 쿼리 1,001건 연속 실행 (스크립트)
for i in $(seq 1 1001); do
  mysql -h qn-1 -P 9030 -u admin -p -e \
    "SELECT $i AS seq, COUNT(*) FROM page_events LIMIT 1;" 2>/dev/null
done
```

```sql
-- Step 2: 프로파일 건수 확인
SELECT COUNT(*) AS profile_count FROM INFORMATION_SCHEMA.QUERY_PROFILES;

-- Step 3: 가장 오래된 쿼리 확인
SELECT query_id, start_time, sql_text
FROM INFORMATION_SCHEMA.QUERY_PROFILES
ORDER BY start_time ASC
LIMIT 1;
```

**기대 결과:**
- Step 2: `profile_count` = 1,000 (1,001번째 쿼리 이후에도 1,000 유지)
- Step 3: `sql_text`에 `SELECT 2 AS seq` 이상의 쿼리 (최초 `SELECT 1 AS seq` 삭제 확인)

**Pass 기준:** `COUNT(*) = 1000` 이고 최초 쿼리 삭제 확인 시 Pass

---

## TC 실행 매트릭스

| TC ID | 카테고리 | 우선순위 | 자동화 가능 | 선행 TC |
|---|---|---|---|---|
| TC-DDL-001 | DDL | Critical | 가능 | - |
| TC-DDL-002 | DDL | High | 가능 | TC-DDL-001 |
| TC-DDL-003 | DDL | Medium | 가능 | TC-DDL-001 |
| TC-DDL-004 | DDL | High | 가능 | TC-DDL-001 |
| TC-DDL-005 | DDL | High | 가능 | TC-SMV-001 |
| TC-SMV-001 | SMV | Critical | 가능 | TC-DDL-001 |
| TC-SMV-002 | SMV | Critical | 가능 | TC-DDL-001 |
| TC-SMV-003 | SMV | High | 가능 | TC-DDL-001 |
| TC-DML-001 | DML | Critical | 가능 | TC-DDL-001 |
| TC-DML-002 | DML | High | 가능 | TC-DDL-001 |
| TC-DML-003 | DML | High | 가능 | TC-DDL-001 |
| TC-ING-001 | Ingestion | Critical | 가능 (Kafka 필요) | TC-DDL-001 |
| TC-ING-002 | Ingestion | High | 반자동 | TC-ING-001 |
| TC-ING-003 | Ingestion | Critical | 가능 (Spark 필요) | TC-DDL-001 |
| TC-ING-004 | Ingestion | High | 반자동 | TC-ING-003 |
| TC-ING-005 | Ingestion | High | 가능 | TC-ING-003 |
| TC-ANA-001 | Analytics | Critical | 가능 | TC-DML-002 |
| TC-ANA-002 | Analytics | Critical | 가능 | TC-SMV-001 |
| TC-ANA-003 | Analytics | High | 가능 | TC-SMV-001 |
| TC-ANA-004 | Analytics | Critical | 가능 | TC-SMV-001 |
| TC-ANA-005 | Analytics | High | 가능 | TC-SMV-001 |
| TC-ANA-006 | Analytics | High | 가능 | TC-DML-002 |
| TC-COMPAT-001 | Compat | Critical | 가능 | - |
| TC-COMPAT-002 | Compat | Critical | 가능 | - |
| TC-COMPAT-003 | Compat | High | 가능 | TC-DDL-001 |
| TC-COMPAT-004 | Compat | Medium | 가능 | TC-DDL-001 |
| TC-PERF-001 | Perf | Critical | 가능 (대규모 데이터 필요) | TC-ING-003 |
| TC-PERF-002 | Perf | High | 가능 (Kafka 필요) | TC-ING-001 |
| TC-PERF-003 | Perf | High | 가능 | TC-DML-002 |
| TC-WEB-001 | Web UI | Critical | 수동 | TC-DDL-001 |
| TC-WEB-002 | Web UI | High | 수동 | TC-DDL-001 |
| TC-WEB-003 | Web UI | Medium | 수동 | TC-DDL-001 |
| TC-QN-001 | Query Node | Critical | 가능 | - |
| TC-QN-002 | Query Node | Critical | 가능 (K8s 필요) | TC-QN-001 |
| TC-QN-003 | Query Node | High | 반자동 | TC-QN-001 |
| TC-CBO-001 | CBO | Critical | 가능 | TC-DML-002 |
| TC-CBO-002 | CBO | Critical | 가능 | TC-CBO-001, TC-DDL-002 |
| TC-CBO-003 | CBO | High | 가능 | TC-CBO-001 |
| TC-CN-001 | Compute Node | High | 가능 | TC-DDL-001 |
| TC-CN-002 | Compute Node | High | 반자동 | TC-CN-001 |
| TC-DN-001 | Data Node | Critical | 가능 (S3 필요) | TC-DDL-001 |
| TC-DN-002 | Data Node | Critical | 가능 (HDFS/Kerberos 필요) | TC-DDL-001 |
| TC-DN-003 | Data Node | High | 가능 | TC-DML-002 |
| TC-MON-001 | Monitoring | Critical | 가능 | - |
| TC-MON-002 | Monitoring | High | 가능 | - |
| TC-PROF-001 | Profiler | Critical | 가능 | TC-DML-001 |
| TC-PROF-002 | Profiler | High | 가능 | TC-PROF-001 |
