# WOW-DB SQL 구문 참조 (SQL Syntax Reference)

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-14  
**상태**: 구현 완료 기준 문서 (구현된 항목 ✅, 스텁/TODO ⚠️, 미구현 ❌)

---

## 목차

1. [DDL — Cube 정의](#1-ddl--cube-정의)
2. [DDL — 데이터베이스 관리](#2-ddl--데이터베이스-관리)
3. [DML — 데이터 쓰기](#3-dml--데이터-쓰기)
4. [DQL — 데이터 조회](#4-dql--데이터-조회)
5. [SHOW 명령 — 메타데이터 조회](#5-show-명령--메타데이터-조회)
6. [SHOW 명령 — 데이터 분포 가시성](#6-show-명령--데이터-분포-가시성)
7. [분석 함수 — WOW-DB 전용](#7-분석-함수--wow-db-전용)
8. [Session Materialized View](#8-session-materialized-view)
9. [시스템 명령](#9-시스템-명령)
10. [MySQL 호환 명령](#10-mysql-호환-명령)

---

## 1. DDL — Cube 정의

### CREATE CUBE ✅

WOW-DB의 기본 스키마 단위. MySQL `CREATE TABLE`과 유사하지만 파티션·분산·정렬 전략이 포함된다.

```sql
CREATE CUBE [IF NOT EXISTS] <cube_name> (
    <column_name> <data_type> [NOT NULL] [DEFAULT <value>],
    ...
)
[PARTITION BY RANGE (<column>) [AUTO]]
[ORDER BY (<sort_key_col1> [, <sort_key_col2> ...])]
[DISTRIBUTED BY HASH (<dist_column>) BUCKETS <n>]
[PROPERTIES (
    "storage_backend" = "native" | "s3" | "hdfs",
    "ttl_days"        = "<n>",
    "colocate_group"  = "<group_name>"
)];
```

**지원 데이터 타입**:

| WOW-DB 타입 | MySQL 별칭 | 설명 |
|-------------|-----------|------|
| `BOOLEAN` | `BOOL` | true/false |
| `INT8` | `TINYINT` | 8-bit 정수 |
| `INT16` | `SMALLINT` | 16-bit 정수 |
| `INT32` | `INT`, `INTEGER` | 32-bit 정수 |
| `INT64` | `BIGINT` | 64-bit 정수 |
| `FLOAT32` | `FLOAT` | 32-bit 부동소수 |
| `FLOAT64` | `DOUBLE`, `DECIMAL`, `NUMERIC` | 64-bit 부동소수 |
| `STRING` | `VARCHAR`, `TEXT`, `CHAR`, `STRING` | UTF-8 문자열 |
| `DATETIME` | `DATETIME`, `TIMESTAMP` | 날짜+시각 |
| `DATE` | `DATE` | 날짜 |
| `JSON` | `JSON` | JSON 문서 (Flat JSON 자동 추출 지원) |
| `BINARY` | `BINARY`, `BLOB`, `VARBINARY` | 이진 데이터 |

**Sort Key 제약** (FR-026):
- ORDER BY 컬럼 수 ≤ 4
- 직렬화 크기 ≤ 128 bytes
- JSON 타입 컬럼 불가
- STRING 컬럼 > 64 bytes 시 경고

**예시**:

```sql
CREATE CUBE IF NOT EXISTS page_events (
    event_id    BIGINT     NOT NULL,
    event_time  DATETIME   NOT NULL,
    user_id     VARCHAR    NOT NULL,
    device_id   VARCHAR    NOT NULL,
    event_name  VARCHAR    NOT NULL,
    page_url    VARCHAR,
    session_id  VARCHAR,
    properties  JSON
)
PARTITION BY RANGE (event_time) AUTO
ORDER BY (event_time, user_id)
DISTRIBUTED BY HASH (device_id) BUCKETS 32;
```

---

### DROP CUBE / DROP TABLE ✅

```sql
DROP CUBE [IF EXISTS] <cube_name>;
DROP TABLE [IF EXISTS] <cube_name>;
```

---

### ALTER CUBE ⚠️ (스텁)

```sql
-- 컬럼 추가
ALTER CUBE <cube_name> ADD COLUMN <col_name> <data_type>;

-- TTL 정책 변경
ALTER CUBE <cube_name> SET PROPERTIES ("ttl_days" = "90");

-- Sort Key 변경 (신규 파티션에만 적용)
ALTER CUBE <cube_name> ORDER BY (<new_sort_key>);
```

> ⚠️ ALTER CUBE는 파서는 구현되었으나 실행 엔진은 Phase F에서 완성 예정

---

## 2. DDL — 데이터베이스 관리

### CREATE DATABASE ✅

```sql
CREATE DATABASE [IF NOT EXISTS] <db_name>;
CREATE SCHEMA [IF NOT EXISTS] <db_name>;
```

### DROP DATABASE ✅

```sql
DROP DATABASE [IF EXISTS] <db_name>;
DROP SCHEMA [IF EXISTS] <db_name>;
```

### USE ✅

```sql
USE <db_name>;
```

---

## 3. DML — 데이터 쓰기

### INSERT ✅ (메모리 스토어)

```sql
-- 단건 삽입
INSERT INTO <cube_name> (col1, col2, ...) VALUES (val1, val2, ...);

-- 다건 삽입
INSERT INTO <cube_name> (col1, col2, ...) VALUES
    (val1, val2, ...),
    (val3, val4, ...);
```

> ⚠️ 현재 구현: MEM_STORE (인메모리) 기반. LSM 영속화는 Phase E에서 완성 예정.  
> Read-Only 모드에서는 `ERROR 1290 (HY000)` 반환.

### DELETE / TRUNCATE ✅ (메모리 스토어)

```sql
DELETE FROM <cube_name> [WHERE <condition>];
TRUNCATE [TABLE] <cube_name>;
```

---

## 4. DQL — 데이터 조회

### SELECT ✅

```sql
SELECT <expr_list>
FROM <cube_name>
[WHERE <condition>]
[GROUP BY <col_list>]
[HAVING <condition>]
[ORDER BY <col_list> [ASC | DESC]]
[LIMIT <n>]
[OFFSET <m>];
```

**지원 표현식**:
- 컬럼 참조: `col_name`, `table.col_name`
- 집계 함수: `COUNT(*)`, `SUM(col)`, `AVG(col)`, `MIN(col)`, `MAX(col)`
- 조건 연산: `=`, `!=`, `<`, `>`, `<=`, `>=`, `BETWEEN`, `IN`, `LIKE`, `IS NULL`, `IS NOT NULL`
- 논리 연산: `AND`, `OR`, `NOT`
- 산술 연산: `+`, `-`, `*`, `/`

**예시**:

```sql
-- 기본 조회
SELECT event_name, COUNT(*) AS cnt
FROM page_events
WHERE event_time >= '2024-01-01'
  AND event_time <  '2024-04-01'
GROUP BY event_name
ORDER BY cnt DESC
LIMIT 20;

-- 시스템 변수
SELECT 1;
SELECT @@version;
```

> ⚠️ 현재 구현: MEM_STORE (인메모리) 기반. 분산 실행 엔진은 Phase D에서 완성 예정.

---

### EXPLAIN ⚠️ (스텁)

```sql
EXPLAIN SELECT ...;
EXPLAIN ANALYZE SELECT ...;
```

---

## 5. SHOW 명령 — 메타데이터 조회

### SHOW DATABASES ✅

```sql
SHOW DATABASES;
```

반환 컬럼: `Database`

---

### SHOW TABLES ✅

```sql
SHOW TABLES;
SHOW TABLES FROM <db_name>;
SHOW FULL TABLES;
```

반환 컬럼: `Tables_in_<db_name>` (FULL: `Table_type` 추가)

---

### SHOW CUBES ✅

```sql
SHOW CUBES;
```

반환 컬럼: `Cube`, `Full_Name`, `Partition`, `Distribution`

---

### DESCRIBE / DESC ✅

```sql
DESCRIBE <cube_name>;
DESC <cube_name>;
```

반환 컬럼: `Field`, `Type`, `Null`, `Key`, `Default`, `Extra`

---

### SHOW CREATE TABLE / SHOW CREATE CUBE ✅

```sql
SHOW CREATE TABLE <cube_name>;
SHOW CREATE CUBE <cube_name>;
```

반환 컬럼: `Table`, `Create Table`

---

### SHOW COLUMNS ✅

```sql
SHOW COLUMNS FROM <cube_name>;
SHOW FULL COLUMNS FROM <cube_name>;
```

반환 컬럼: `Field`, `Type`, `Null`, `Key`, `Default`, `Extra`

---

### SHOW STATUS / SHOW VARIABLES ✅

```sql
SHOW STATUS;
SHOW GLOBAL STATUS;
SHOW SESSION STATUS;
SHOW VARIABLES;
SHOW GLOBAL VARIABLES;
```

반환 컬럼: `Variable_name`, `Value`

---

## 6. SHOW 명령 — 데이터 분포 가시성

> 계층 구조: **Cube → Partition → Shard → Part (LSM SSTable)**

### SHOW PARTITIONS ✅ (컬럼 정의 완료, 데이터 연동은 T138에서)

```sql
-- 기본 조회
SHOW PARTITIONS FROM <cube_name>;

-- 조건 필터 (파서 지원, 실행은 Phase E)
SHOW PARTITIONS FROM <cube_name> WHERE range_start >= '2024-01-01';

-- 정렬
SHOW PARTITIONS FROM <cube_name> ORDER BY size_bytes DESC LIMIT 10;
```

반환 컬럼: `partition_id`, `range_start`, `range_end`, `row_count`, `size_bytes`, `shard_count`, `part_count`, `tier`, `created_at`

---

### SHOW SHARDS ✅ (컬럼 정의 완료, 데이터 연동은 T138에서)

```sql
-- 전체 Shard 조회
SHOW SHARDS FROM <cube_name>;

-- 특정 파티션의 Shard
SHOW SHARDS FROM <cube_name> PARTITION '<partition_id>';

-- 특정 SN의 Shard
SHOW SHARDS FROM <cube_name> WHERE sn_node_id = 'sn-01';
```

반환 컬럼: `shard_id`, `partition_id`, `partition_range`, `sn_node_id`, `sn_endpoint`, `bucket_id`, `role`, `state`, `row_count`, `size_bytes`, `part_count`, `lsn`

---

### SHOW PARTS ✅ (컬럼 정의 완료, 데이터 연동은 T138/T142에서)

```sql
-- 전체 Part 조회
SHOW PARTS FROM <cube_name>;

-- 특정 파티션의 Part
SHOW PARTS FROM <cube_name> PARTITION '<partition_id>';

-- 파티션 우선 명시 구문 (별칭)
SHOW PARTS ON PARTITION '<partition_id>' FROM <cube_name>;

-- 특정 Shard의 Part
SHOW PARTS FROM <cube_name> SHARD '<shard_id>';

-- L0 Part만 (Compaction 진단용)
SHOW PARTS FROM <cube_name> WHERE level = 0;

-- 특정 SN, 크기 내림차순
SHOW PARTS FROM <cube_name> WHERE sn_node_id = 'sn-01' ORDER BY size_bytes DESC;
```

반환 컬럼: `part_id`, `shard_id`, `partition_id`, `sn_node_id`, `level`, `sequence_num`, `row_count`, `size_bytes`, `min_sort_key`, `max_sort_key`, `bloom_size_bytes`, `created_at`

---

### SHOW DISTRIBUTED STATUS ✅ (컬럼 정의 완료, 데이터 연동은 T138에서)

```sql
SHOW DISTRIBUTED STATUS FROM <cube_name>;

-- 부하 높은 SN 파악
SHOW DISTRIBUTED STATUS FROM <cube_name> ORDER BY size_bytes DESC;
```

반환 컬럼: `sn_node_id`, `sn_endpoint`, `shard_count`, `leader_shard_count`, `partition_count`, `row_count`, `size_bytes`, `avg_part_per_shard`

---

## 7. 분석 함수 — WOW-DB 전용

### FUNNEL_COUNT ✅

퍼널 전환율 분석. 순서가 지정된 이벤트 단계를 지정된 시간 창 내에 완료한 사용자 수를 반환한다.

```sql
SELECT FUNNEL_COUNT(
    user_id,
    event_name,
    event_time,
    WINDOW 7 DAYS,
    STEP 'page_view',
    STEP 'add_to_cart',
    STEP 'purchase'
) AS funnel_result
FROM page_events
WHERE event_time BETWEEN '2024-01-01' AND '2024-03-31';
```

**파라미터**:
- `user_id`: 사용자 키 컬럼
- `event_name`: 이벤트 타입 컬럼
- `event_time`: 시간 컬럼
- `WINDOW <n> DAYS|HOURS`: 퍼널 완료 허용 시간 창
- `STEP '<event_name>'`: 순서에 따른 단계 (2개 이상)

**반환**: 단계별 완료 사용자 수 (`step_1_count`, `step_2_count`, ..., `conversion_rate`)

---

### COHORT_ANALYSIS ✅

코호트 분석. 진입 이벤트 기준으로 그룹화한 사용자들의 재방문/전환을 추적한다.

```sql
SELECT COHORT_ANALYSIS(
    user_id,
    event_name,
    event_time,
    ENTRY_EVENT   'signup',
    RETURN_EVENT  'purchase',
    GRANULARITY   'week',
    PERIODS       8
) AS cohort_result
FROM page_events
WHERE event_time BETWEEN '2024-01-01' AND '2024-03-31';
```

**파라미터**:
- `ENTRY_EVENT '<event>'`: 코호트 진입 이벤트
- `RETURN_EVENT '<event>'`: 추적할 재방문/전환 이벤트
- `GRANULARITY 'day'|'week'|'month'`: 집계 단위
- `PERIODS <n>`: 추적 기간 수

---

### PATH_ANALYSIS ✅

경로 분석. 사용자가 실제로 이동한 이벤트 시퀀스 패턴을 분석한다.

```sql
SELECT PATH_ANALYSIS(
    user_id,
    event_name,
    event_time,
    MAX_STEPS  5,
    SESSION_TIMEOUT 30 MINUTES
) AS path_result
FROM page_events
WHERE event_time BETWEEN '2024-01-01' AND '2024-03-31';
```

**파라미터**:
- `MAX_STEPS <n>`: 추적할 최대 단계 수
- `SESSION_TIMEOUT <n> MINUTES|HOURS`: 세션 구분 비활성 시간

**반환**: 상위 N개 경로와 각 경로의 사용자 수, 전환율

---

## 8. Session Materialized View

### CREATE SESSION MATERIALIZED VIEW ✅

```sql
CREATE SESSION MATERIALIZED VIEW <mv_name>
ON <cube_name>
USER_KEY    <user_key_column>
TIMEOUT     <n> MINUTES | HOURS
[REFRESH SCHEDULE '<cron_expr>'];
```

**예시**:

```sql
CREATE SESSION MATERIALIZED VIEW page_sessions
ON   page_events
USER_KEY    device_id
TIMEOUT     30 MINUTES;
```

> 대화형 Web UI에서 단계별 마법사로 생성 권장 (FR-028)

---

## 9. 시스템 명령

### SET ✅

```sql
SET <variable> = <value>;
SET NAMES utf8mb4;
SET character_set_client = utf8mb4;
SET GLOBAL wowdb_read_only = OFF;  -- Read-Only 수동 해제
```

---

### ANALYZE TABLE ⚠️ (스텁)

```sql
ANALYZE TABLE <cube_name>;
ANALYZE TABLE <cube_name> PARTITION <partition_id>;
```

CBO 통계 (min/max/NDV/Histogram) 전체 재계산. 자동 통계 수집이 기본값이므로 수동 실행은 선택적.

---

## 10. MySQL 호환 명령

WOW-DB는 MySQL 8.0 Wire Protocol을 지원하므로 표준 MySQL 클라이언트 명령이 그대로 동작한다.

| 명령 | 지원 여부 | 비고 |
|------|---------|------|
| `SELECT 1` | ✅ | 연결 테스트 |
| `SELECT @@version` | ✅ | `8.0.0-wowdb-1.0` 반환 |
| `SELECT @@version_comment` | ✅ | |
| `SET NAMES utf8mb4` | ✅ | 클라이언트 초기화 |
| `SET character_set_client` | ✅ | |
| `SHOW DATABASES` | ✅ | |
| `SHOW TABLES` | ✅ | |
| `SHOW COLUMNS FROM <tbl>` | ✅ | |
| `SHOW CREATE TABLE <tbl>` | ✅ | |
| `DESCRIBE <tbl>` | ✅ | |
| `SHOW STATUS` | ✅ | 핵심 변수만 |
| `SHOW VARIABLES` | ✅ | 핵심 변수만 |
| `SHOW GLOBAL STATUS` | ✅ | |
| `USE <db>` | ✅ | COM_INIT_DB 포함 |
| `CREATE DATABASE` | ✅ | |
| `DROP DATABASE` | ✅ | |
| `INFORMATION_SCHEMA` 쿼리 | ⚠️ | 기본 호환만 |
| `SHOW INDEX FROM <tbl>` | ❌ | Phase F 예정 |
| `SHOW PROCESSLIST` | ❌ | Phase F 예정 |

---

## 에러 코드

| 에러 코드 | SQL State | 상황 |
|---------|-----------|------|
| 1064 `ER_PARSE_ERROR` | 42000 | SQL 파싱 실패 |
| 1050 `ER_TABLE_EXISTS_ERROR` | 42S01 | Cube 이미 존재 |
| 1051 `ER_BAD_TABLE_ERROR` | 42S02 | 없는 Cube에 DDL |
| 1049 `ER_BAD_DB_ERROR` | 42000 | 없는 DB DROP |
| 1290 `ER_OPTION_PREVENTS_STATEMENT` | HY000 | Read-Only 모드에서 쓰기 시도 |

---

## 구현 상태 요약

| 카테고리 | 명령 | 상태 |
|---------|------|------|
| DDL | `CREATE CUBE` | ✅ 완전 구현 |
| DDL | `DROP CUBE` | ✅ 완전 구현 |
| DDL | `ALTER CUBE` | ⚠️ 파서만 (실행 Phase F) |
| DDL | `CREATE DATABASE` | ✅ 완전 구현 |
| DDL | `DROP DATABASE` | ✅ 완전 구현 |
| DML | `INSERT` | ✅ 메모리 스토어 (LSM Phase E) |
| DML | `DELETE/TRUNCATE` | ✅ 메모리 스토어 |
| DQL | `SELECT` | ✅ 메모리 스토어 (분산 실행 Phase D) |
| SHOW | `SHOW DATABASES` | ✅ 완전 구현 |
| SHOW | `SHOW TABLES/CUBES` | ✅ 완전 구현 |
| SHOW | `DESCRIBE/DESC` | ✅ 완전 구현 |
| SHOW | `SHOW CREATE TABLE` | ✅ 완전 구현 |
| SHOW | `SHOW PARTITIONS` | ✅ 컬럼 정의 완료 (데이터 T138) |
| SHOW | `SHOW SHARDS` | ✅ 컬럼 정의 완료 (데이터 T138) |
| SHOW | `SHOW PARTS` | ✅ 컬럼 정의 완료 (데이터 T141/T142) |
| SHOW | `SHOW DISTRIBUTED STATUS` | ✅ 컬럼 정의 완료 (데이터 T138) |
| 분석 | `FUNNEL_COUNT` | ✅ 완전 구현 (스텁 데이터) |
| 분석 | `COHORT_ANALYSIS` | ✅ 완전 구현 (스텁 데이터) |
| 분석 | `PATH_ANALYSIS` | ✅ 완전 구현 (스텁 데이터) |
| SMV | `CREATE SESSION MATERIALIZED VIEW` | ✅ 메타 등록 구현 |
| 시스템 | `SET` | ✅ 완전 구현 |
| 시스템 | `ANALYZE TABLE` | ⚠️ 스텁 |
