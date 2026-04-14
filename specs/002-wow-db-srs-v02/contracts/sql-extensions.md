# SQL 확장 문법 계약: WOW-DB 커스텀 DDL/함수

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-12

WOW-DB는 MySQL 8.0 표준 SQL에 다음 확장을 추가한다.  
파서: `sqlparser-rs` (MySQL 방언) + 커스텀 AST 확장

---

## 1. DDL 확장: CREATE TABLE

```sql
CREATE TABLE [IF NOT EXISTS] <table_name>
(
    <column_name> <data_type> [NOT NULL] [DEFAULT <value>],
    ...
)
PARTITION BY RANGE(<column>) (
    PARTITION <name> VALUES LESS THAN (<value>),
    ...
)
[AUTO PARTITION BY (DAY | MONTH | YEAR)]
ORDER BY (<column> [, <column>]*)
DISTRIBUTED BY HASH(<column>) BUCKETS <n>
[COLOCATE WITH <group_name>]
[STORAGE BACKEND = (NATIVE | S3 | HDFS)]
[PROPERTIES (
    "key" = "value",
    ...
)];

-- 예시:
CREATE TABLE IF NOT EXISTS page_events (
    event_time   DATETIME     NOT NULL,
    user_id      VARCHAR(64)  NOT NULL,
    event_name   VARCHAR(128) NOT NULL,
    page_url     TEXT,
    properties   JSON
)
PARTITION BY RANGE(event_time) ()
AUTO PARTITION BY DAY
ORDER BY (event_time, user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 32
COLOCATE WITH web_analytics_group
PROPERTIES (
    "compression" = "lz4",
    "ttl_days"    = "90"
);
```

---

## 2. DDL 확장: CREATE SESSION MATERIALIZED VIEW (Behavioral Table 생성)

> **개념**: 이 DDL로 생성되는 테이블을 **Behavioral Table (BT)** 라고 부른다.  
> Funnel / Cohort / Path 분석에 최적화된 세션 단위 물리 레이아웃.  
> **Behavioral Routing**: 생성 후 엔진이 자동으로 Behavioral Query를 이 테이블로 라우팅한다.

```sql
CREATE SESSION MATERIALIZED VIEW [IF NOT EXISTS] <bt_name>
FROM <source_table>
USER KEY (<user_key_column>)
SESSION TIMEOUT <n> (MINUTE | HOUR | SECOND)
[REFRESH (
    ON INSERT
    | EVERY <n> (MINUTE | HOUR | DAY)
    | MANUAL
)]
[PROPERTIES ("key" = "value")];

-- 예시:
CREATE SESSION MATERIALIZED VIEW page_events_sessions
FROM page_events
USER KEY (user_id)
SESSION TIMEOUT 30 MINUTE
REFRESH EVERY 5 MINUTE;
```

**Behavioral Table 자동 생성 컬럼:**
- `session_id` VARCHAR(36) — 자동 생성 UUID
- `session_start` DATETIME — 세션 첫 이벤트 시각
- `session_end` DATETIME — 세션 마지막 이벤트 시각
- `session_event_count` INT — 세션 내 이벤트 수
- `event_sequence` ARRAY — 시간 순 이벤트 시퀀스 (Behavioral Routing의 핵심 컬럼)
- 소스 Table의 모든 컬럼 (세션 첫 이벤트 값 또는 집계)

> **Behavioral Guidance**: Behavioral Table이 없는 상태에서 FUNNEL_COUNT / COHORT_ANALYSIS / PATH_ANALYSIS 쿼리가 실행되면, 엔진이 자동으로 이 DDL 생성을 권장하고 예상 성능 향상을 안내한다.

---

## 3. DDL 확장: CREATE MATERIALIZED VIEW (사전 집계)

```sql
CREATE MATERIALIZED VIEW <mv_name>
AS
SELECT
    <group_by_columns>,
    <aggregate_functions>
FROM <source_table>
GROUP BY <group_by_columns>
[REFRESH (ON INSERT | EVERY <interval> | MANUAL)];

-- 예시:
CREATE MATERIALIZED VIEW daily_event_counts
AS
SELECT
    DATE(event_time) AS event_date,
    event_name,
    COUNT(*)         AS cnt,
    COUNT(DISTINCT user_id) AS unique_users
FROM page_events
GROUP BY DATE(event_time), event_name
REFRESH ON INSERT;
```

---

## 4. DML 확장: ROUTINE LOAD (Kafka 상시 수집)

```sql
CREATE ROUTINE LOAD <job_name> ON <table_name>
COLUMNS TERMINATED BY ','           -- CSV 구분자 (선택)
FORMAT AS (JSON | CSV | AVRO)
PROPERTIES (
    "max_batch_interval" = "5",     -- 초 단위
    "max_batch_rows"     = "100000",
    "max_error_rate"     = "0.1"
)
FROM KAFKA (
    "kafka_broker_list" = "broker1:9092,broker2:9092",
    "kafka_topic"       = "web-events",
    "kafka_partitions"  = "0,1,2,3",
    "kafka_offsets"     = "OFFSET_BEGINNING"
);

-- 잡 관리
PAUSE  ROUTINE LOAD FOR <job_name>;
RESUME ROUTINE LOAD FOR <job_name>;
STOP   ROUTINE LOAD FOR <job_name>;
SHOW   ROUTINE LOAD FOR <job_name>;
```

---

## 5. 분석 함수: FUNNEL_COUNT

```sql
-- 문법
SELECT FUNNEL_COUNT(
    user_key  => <user_key_column>,
    timestamp => <event_time_column>,
    window    => INTERVAL <n> (HOUR | DAY),
    steps     => [
        <condition_1>,  -- Step 1
        <condition_2>,  -- Step 2
        ...             -- Step N
    ]
) AS funnel_result
FROM <table_or_smv>
[WHERE <filter>]
[GROUP BY <dimension>];

-- 반환: ARRAY<BIGINT> — 각 단계에 도달한 사용자 수

-- 예시:
SELECT
    DATE(event_time) AS day,
    FUNNEL_COUNT(
        user_key  => user_id,
        timestamp => event_time,
        window    => INTERVAL 24 HOUR,
        steps     => [
            event_name = 'page_view',
            event_name = 'add_to_cart',
            event_name = 'checkout',
            event_name = 'purchase'
        ]
    ) AS purchase_funnel
FROM page_events
WHERE event_time >= '2026-01-01'
GROUP BY DATE(event_time);
```

---

## 6. 분석 함수: COHORT_ANALYSIS

```sql
-- 문법
SELECT COHORT_ANALYSIS(
    user_key       => <user_key_column>,
    timestamp      => <event_time_column>,
    entry_event    => <entry_condition>,
    return_event   => <return_condition>,
    cohort_period  => (DAY | WEEK | MONTH),
    periods        => <n>
) AS cohort_result
FROM <table_or_smv>
[WHERE <filter>];

-- 반환: TABLE(cohort_date DATE, period INT, users BIGINT, retention FLOAT)

-- 예시:
SELECT *
FROM COHORT_ANALYSIS(
    user_key      => user_id,
    timestamp     => event_time,
    entry_event   => event_name = 'signup',
    return_event  => event_name = 'purchase',
    cohort_period => WEEK,
    periods       => 12
)
OVER (
    SELECT * FROM page_events
    WHERE event_time BETWEEN '2026-01-01' AND '2026-04-01'
);
```

---

## 7. 분석 함수: PATH_ANALYSIS

```sql
-- 문법
SELECT PATH_ANALYSIS(
    user_key  => <user_key_column>,
    timestamp => <event_time_column>,
    event_col => <event_name_column>,
    max_depth => <n>,      -- 최대 경로 깊이 (기본 5)
    top_n     => <n>       -- 상위 N개 경로 반환 (기본 20)
) AS path_result
FROM <table_or_smv>
[WHERE <filter>];

-- 반환: TABLE(path ARRAY<VARCHAR>, user_count BIGINT, pct FLOAT)

-- 예시:
SELECT path, user_count, pct
FROM PATH_ANALYSIS(
    user_key  => user_id,
    timestamp => event_time,
    event_col => event_name,
    max_depth => 6,
    top_n     => 30
)
OVER (
    SELECT * FROM page_events_sessions
    WHERE session_start >= '2026-04-01'
);
```

---

## 8. 인덱스 DDL 확장

```sql
-- Data Skipping Index 추가
ALTER TABLE <table_name>
ADD INDEX <index_name> (<column>)
TYPE (MINMAX | BLOOM_FILTER | SET | NGRAMBF_V1)
[GRANULARITY <n>];

-- 예시
ALTER TABLE page_events
ADD INDEX idx_event_name (event_name) TYPE BLOOM_FILTER GRANULARITY 1;

ALTER TABLE page_events
ADD INDEX idx_url_ngram (page_url) TYPE NGRAMBF_V1 GRANULARITY 2;
```

---

## 9. 기타 확장 DDL

```sql
-- 외부 테이블
CREATE EXTERNAL TABLE <name> (...)
ENGINE = S3 | HDFS | ICEBERG | HIVE
PROPERTIES (...);

-- 리소스 그룹
CREATE RESOURCE GROUP <name>
WITH (
    cpu_cores         = <n>,
    memory_limit      = '<n>GB',
    concurrency_limit = <n>,
    query_timeout     = <n>  -- ms
);

-- 통계 수동 수집
ANALYZE TABLE <table_name> [(<column> [, ...])];

-- Tiered Storage 설정
ALTER TABLE <table_name>
SET TIERING POLICY (
    hot_ttl_days  = 30,
    cold_backend  = 's3',
    cold_prefix   = 's3://bucket/cold/'
);
```
