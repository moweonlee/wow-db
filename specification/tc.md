# WOW-DB Test Cases

---

| 항목 | 내용 |
|---|---|
| 문서 버전 | v0.1 Draft |
| 작성일 | 2026-04-12 |
| 참조 문서 | srs.md v0.1 |

---

## 테스트 케이스 구성 규칙

각 TC는 다음 형식으로 작성한다.

```
TC-{카테고리}-{번호}
카테고리: DDL, DML, ING(Ingestion), ANA(Analytics), SMV, COMPAT, PERF, WEB
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
- WOW-DB 클러스터 정상 기동 (DSL 1 Leader + SL 3 노드)
- MySQL 클라이언트로 DSL 접속 완료 (`analytics` 데이터베이스 선택)

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
2. 50건 처리 중 SL Node 1 강제 종료 (`kill -9`)
3. SL Node 1 재기동
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
mysql -h wowdb-dsl-1 -P 9030 -u admin -p analytics

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
String url = "jdbc:mysql://wowdb-dsl-1:9030/analytics"
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
- 계획 트리 출력 (SL 노드별 실행 계획 포함)
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
