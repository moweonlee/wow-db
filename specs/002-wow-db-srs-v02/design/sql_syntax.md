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
8. [Behavioral Table (Session Materialized View)](#8-behavioral-table-session-materialized-view)
9. [시스템 명령](#9-시스템-명령)
9.5. [EXPLAIN — 분산 실행 계획 출력](#95-explain--분산-실행-계획-출력)
10. [MySQL 호환 명령](#10-mysql-호환-명령)
11. [오류 처리 — 미지원 SQL](#11-오류-처리--미지원-sql)
12. [클러스터 관리 명령](#12-클러스터-관리-명령)

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

### ALTER CUBE ✅

```sql
-- 컬럼 추가 (nullable / NOT NULL 지원)
ALTER CUBE <cube_name> ADD COLUMN <col_name> <data_type> [NOT NULL];

-- 컬럼 삭제 (Sort Key 컬럼 삭제 시 오류)
ALTER CUBE <cube_name> DROP COLUMN <col_name>;

-- TTL 정책 변경
ALTER CUBE <cube_name> SET PROPERTIES ("ttl_days" = "90");

-- Data Skipping Index 추가
ALTER CUBE <cube_name> ADD INDEX <idx_name> (<col>) [USING BLOOMFILTER|MINMAX|SET];

-- Data Skipping Index 삭제
ALTER CUBE <cube_name> DROP INDEX <idx_name>;

-- MySQL 호환: ALTER TABLE <name> … 도 동일하게 처리됨
```

> ✅ 파서(WowDbParser) + 실행 엔진(AlterCubeHandler) 완전 연결. Raft KV에 스키마 영속화.
> Sort Key 컬럼 DROP 시도 시 오류 반환. 중복 컬럼 추가 시 오류 반환.

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

### EXPLAIN ✅

```sql
EXPLAIN SELECT ...;
EXPLAIN ANALYZE SELECT ...;
```

> Section 9.5 참조

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

### FUNNEL_COUNT ⚠️ (파서·AST 구현, 실행 엔진 Phase C)

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

### COHORT_ANALYSIS ⚠️ (파서·AST 구현, 실행 엔진 Phase C)

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

### PATH_ANALYSIS ⚠️ (파서·AST 구현, 실행 엔진 Phase C)

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

## 8. Behavioral Table (Session Materialized View)

> **개념**: Behavioral Table (BT) — Event Table로부터 파생된 세션 단위 행동 분석 테이블.  
> **DDL**: `CREATE SESSION MATERIALIZED VIEW` (SQL 문법은 이 이름을 유지)  
> **역할**: Funnel / Cohort / Path 분석에 최적화된 물리 레이아웃 제공.  
> **Behavioral Routing**: Behavioral Query 감지 시 엔진이 자동으로 이 테이블을 사용 (사용자 명시 불필요).

### CREATE SESSION MATERIALIZED VIEW ✅

```sql
CREATE SESSION MATERIALIZED VIEW [IF NOT EXISTS] <mv_name>
FROM <source_cube>
USER KEY <user_key_column>
SESSION TIMEOUT <n> MINUTES | HOURS | SECONDS
[REFRESH INCREMENTAL | MANUAL | SCHEDULED '<cron_expr>'];
```

**파라미터**:

| 절 | 설명 |
|---|---|
| `FROM <source_cube>` | Behavioral Table을 파생할 원본 Event Table (Cube) |
| `USER KEY <col>` | 사용자 식별 컬럼 (`device_id`, `user_id` 등 String/Int 타입) |
| `SESSION TIMEOUT <n> MINUTES\|HOURS\|SECONDS` | 연속 이벤트 간 허용 비활성 시간. 초과 시 새 세션 시작 |
| `REFRESH INCREMENTAL` | INSERT 시점 증분 갱신 (기본값) |
| `REFRESH MANUAL` | 수동 트리거로만 갱신 |
| `REFRESH SCHEDULED '<cron>'` | cron 표현식 기반 주기 갱신 |

**자동 생성 컬럼** (Behavioral Table 전용):

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `session_id` | VARCHAR(36) | 세션 고유 UUID (자동 생성) |
| `session_start` | DATETIME | 세션 첫 이벤트 시각 |
| `session_end` | DATETIME | 세션 마지막 이벤트 시각 |
| `session_event_count` | INT | 세션 내 이벤트 수 |
| `event_sequence` | ARRAY | 시간 순 정렬된 이벤트 시퀀스 |

> **Behavioral Routing 연동**: 위 컬럼 중 하나라도 쿼리에서 참조되거나, FUNNEL_COUNT / COHORT_ANALYSIS / PATH_ANALYSIS 함수가 사용되면 엔진이 자동으로 이 Behavioral Table로 라우팅한다.

**예시**:

```sql
-- 기본 (30분 타임아웃, 증분 갱신)
CREATE SESSION MATERIALIZED VIEW page_events_sessions
FROM page_events
USER KEY device_id
SESSION TIMEOUT 30 MINUTES
REFRESH INCREMENTAL;

-- 1시간 타임아웃, 매시 정각 갱신
CREATE SESSION MATERIALIZED VIEW IF NOT EXISTS hourly_sessions
FROM page_events
USER KEY device_id
SESSION TIMEOUT 1 HOUR
REFRESH SCHEDULED '0 * * * *';
```

> **생성 권장 시점**: 첫 FUNNEL_COUNT / COHORT_ANALYSIS / PATH_ANALYSIS 쿼리 실행 시 Behavioral Table이 없으면 Web UI가 자동으로 생성 마법사를 제안한다 (Behavioral Guidance, FR-010, FR-NEW-001-07).

### DROP SESSION MATERIALIZED VIEW ✅

```sql
DROP SESSION MATERIALIZED VIEW [IF EXISTS] <mv_name>;
```

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

## 9.5. EXPLAIN — 분산 실행 계획 출력

### EXPLAIN ✅

`EXPLAIN`은 주어진 SQL을 실제로 실행하지 않고 CBO가 생성하는 분산 실행 계획을 Fragment 단위로 출력한다. StarRocks의 EXPLAIN과 유사한 형식을 사용한다.

```sql
EXPLAIN <sql>;
EXPLAIN VERBOSE <sql>;
EXPLAIN COSTS <sql>;
```

**변형별 출력 범위**:

| 변형 | 설명 |
|------|------|
| `EXPLAIN` | Fragment 구조, Shuffle 방식, 기본 CBO 수치 |
| `EXPLAIN VERBOSE` | 기본 + Push-down 프레디케이트, 인덱스 상세, 컬럼 투영 목록 |
| `EXPLAIN COSTS` | 기본 + 행 수 추정, 바이트 추정, 각 노드 비용 합계 |

**반환 컬럼**:

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `Fragment_Id` | STRING | 소속 Fragment 식별자 (`FRAGMENT 0`, `FRAGMENT 1`, ...) |
| `Plan` | TEXT | 실행 계획 텍스트 한 줄 |

---

### EXPLAIN 출력 구조

출력은 `PLAN FRAGMENT N` 블록으로 나뉜다. Fragment 0은 최종 결과를 QN으로 전달하는 최상위 Fragment이다. Fragment 번호가 증가할수록 스캔에 가까운 하위 Fragment이다.

```
PLAN FRAGMENT 0
  OUTPUT EXPRS: <output_columns>
  PARTITION: UNPARTITIONED

  RESULT SINK

  0: AGGREGATION [QN-MERGE]
     CBO: rows=<n>  bytes=<size>

PLAN FRAGMENT 1
  OUTPUT EXPRS: <output_columns>
  PARTITION: HASH(<dist_key>)

  STREAM DATA SINK
    EXCHANGE ID: 01
    HASH_PARTITIONED: <dist_key>

  1: HASH AGGREGATE [CN-PARTIAL]
     |--- 2: SCAN (<cube_name>) [SN-01, SN-02, SN-03]
            Partitions: <scanned>/<total> pruned (<skipped> skipped by CBO)
            Parts: <scanned> scanned (<skipped> skipped by Bloom/MinMax)
            Push-down predicates: <predicate_expr>
            Index: MinMax on <col> [HIT|MISS]
            Aggregate pushdown: <func> [YES|NO]
```

---

### 노드 타입 참조

| 노드 타입 | 설명 |
|-----------|------|
| `SCAN (<cube>)` | Storage Node에서 데이터 스캔 |
| `HASH AGGREGATE [CN-PARTIAL]` | CN에서 부분 집계 수행 |
| `AGGREGATION [QN-MERGE]` | QN에서 최종 집계 병합 |
| `HASH JOIN [BROADCAST]` | 소형 테이블을 모든 CN에 방송 후 Join |
| `HASH JOIN [HASH_SHUFFLE]` | 양쪽 테이블을 분산 키로 재분산 후 Join |
| `HASH JOIN [COLOCATE]` | 동일 Colocate Group — Shuffle 없이 로컬 Join |
| `EXCHANGE [HASH_SHUFFLE]` | CN 간 Hash Partition 기반 재분산 |
| `EXCHANGE [BROADCAST]` | 모든 CN에 데이터 복제 전송 |
| `EXCHANGE [GATHER]` | 모든 CN 결과를 단일 노드로 수집 |
| `FUNNEL ANALYSIS` | FUNNEL_COUNT 전용 분석 단계 |
| `RESULT SINK` | 최종 결과를 클라이언트에 반환 |

---

### EXPLAIN COSTS 예시

```sql
EXPLAIN COSTS
SELECT event_name, count(*) AS cnt
FROM page_events
WHERE event_time > '2026-01-01'
GROUP BY event_name;
```

```
Fragment_Id  | Plan
-------------+------
FRAGMENT 0   | PLAN FRAGMENT 0
FRAGMENT 0   |   OUTPUT EXPRS: event_name, cnt
FRAGMENT 0   |   PARTITION: UNPARTITIONED
FRAGMENT 0   |
FRAGMENT 0   |   RESULT SINK
FRAGMENT 0   |
FRAGMENT 0   |   0: AGGREGATION [QN-MERGE]
FRAGMENT 0   |      CBO: rows=500  bytes=48KB  cost=1.2
FRAGMENT 1   | PLAN FRAGMENT 1
FRAGMENT 1   |   OUTPUT EXPRS: event_name, count(*)
FRAGMENT 1   |   PARTITION: HASH(event_name)
FRAGMENT 1   |
FRAGMENT 1   |   STREAM DATA SINK
FRAGMENT 1   |     EXCHANGE ID: 01
FRAGMENT 1   |     HASH_PARTITIONED: event_name
FRAGMENT 1   |
FRAGMENT 1   |   1: HASH AGGREGATE [CN-PARTIAL]
FRAGMENT 1   |      CBO: rows=50,000  bytes=4.2MB  cost=24.7
FRAGMENT 1   |      |--- 2: SCAN (page_events) [SN-01, SN-02, SN-03]
FRAGMENT 1   |             Partitions: 3/10 pruned (7 skipped by CBO)
FRAGMENT 1   |             Parts: 24 scanned (12 skipped by Bloom/MinMax)
FRAGMENT 1   |             Push-down predicates: event_time > '2026-01-01'
FRAGMENT 1   |             Index: MinMax on event_time [HIT]
FRAGMENT 1   |             Aggregate pushdown: count(*) [YES]
FRAGMENT 1   |             CBO: rows=200,000  bytes=16MB  cost=18.3
```

---

### Colocate Join EXPLAIN 예시

```sql
-- page_events와 user_profiles가 동일 colocate_group이고
-- 동일 분산 키(device_id)와 버킷 수(32)를 사용하는 경우
EXPLAIN
SELECT e.event_name, u.country, count(*) AS cnt
FROM page_events e
JOIN user_profiles u ON e.device_id = u.device_id
WHERE e.event_time > '2026-01-01'
GROUP BY e.event_name, u.country;
```

```
PLAN FRAGMENT 0
  OUTPUT EXPRS: event_name, country, cnt
  PARTITION: UNPARTITIONED

  RESULT SINK

  0: AGGREGATION [QN-MERGE]
     CBO: rows=1,000  bytes=96KB

PLAN FRAGMENT 1
  OUTPUT EXPRS: event_name, country, count(*)
  PARTITION: HASH(device_id)

  STREAM DATA SINK
    EXCHANGE ID: 01
    HASH_PARTITIONED: device_id

  1: HASH JOIN [COLOCATE]
     CBO: rows=200,000  bytes=16MB  shuffle=NONE
     |--- 2: SCAN (page_events)   [SN-01, SN-02, SN-03]
            Push-down predicates: event_time > '2026-01-01'
            Index: MinMax on event_time [HIT]
     |--- 3: SCAN (user_profiles) [SN-01, SN-02, SN-03]
            (same bucket distribution — no shuffle required)
```

> **Colocate Join 조건**: 두 Cube가 동일한 `colocate_group` PROPERTIES, 동일한 분산 키, 동일한 버킷 수를 가지며 JOIN 조건 컬럼이 분산 키와 일치할 때만 `[COLOCATE]`로 표시된다.

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
| `INFORMATION_SCHEMA` 쿼리 | ✅ | TABLES/COLUMNS/SCHEMATA 구현 |
| `SHOW INDEX FROM <tbl>` | ✅ | MySQL 13-컬럼 호환 결과 |
| `SHOW PROCESSLIST` | ✅ | 8-컬럼 호환 (현재 커넥션 1행) |

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
| DDL | `ALTER CUBE` | ✅ 파서·실행 엔진 완전 연결 (ADD/DROP COLUMN, TTL, INDEX) |
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
| 분석 | `FUNNEL_COUNT` | ⚠️ 파서·AST 구현 (실행 엔진 Phase C) |
| 분석 | `COHORT_ANALYSIS` | ⚠️ 파서·AST 구현 (실행 엔진 Phase C) |
| 분석 | `PATH_ANALYSIS` | ⚠️ 파서·AST 구현 (실행 엔진 Phase C) |
| SMV | `CREATE SESSION MATERIALIZED VIEW` | ✅ 파서·메타 등록 구현 (세션 계산 Phase F) |
| SMV | `DROP SESSION MATERIALIZED VIEW` | ✅ 파서·메타 삭제 구현 |
| 시스템 | `SET` | ✅ 완전 구현 |
| 시스템 | `ANALYZE TABLE` | ✅ 구현 (통계 수집 예약 응답; CBO 연동 Phase D) |
| 실행 계획 | `EXPLAIN` | ✅ 완전 구현 |
| 실행 계획 | `EXPLAIN VERBOSE` | ✅ 완전 구현 |
| 실행 계획 | `EXPLAIN COSTS` | ✅ 완전 구현 |
| 실행 계획 | `EXPLAIN ANALYZE` | ✅ 구현 (예상 통계 기반, 실측치는 Phase D) |
| MySQL 호환 | `SHOW INDEX FROM` | ✅ 13-컬럼 MySQL 호환 |
| MySQL 호환 | `SHOW PROCESSLIST` | ✅ 8-컬럼 MySQL 호환 |
| MySQL 호환 | `INFORMATION_SCHEMA.TABLES` | ✅ Cube 목록 반환 |
| MySQL 호환 | `INFORMATION_SCHEMA.COLUMNS` | ✅ 모든 Cube 컬럼 반환 |
| MySQL 호환 | `INFORMATION_SCHEMA.SCHEMATA` | ✅ DB 목록 반환 |

---

## 11. 오류 처리 — 미지원 SQL

WOW-DB는 인식되지 않거나 아직 구현되지 않은 SQL에 대해 MySQL 표준 오류 코드를 반환한다. 결과 행(`stub`)을 반환하지 않는다. 상세 사양: [`design/error-handling.md`](./error-handling.md)

### 오류 분류

| 상황 | Error Code | 예시 |
|------|-----------|------|
| 문법 오류 / 임의 문자열 | `1064 ER_PARSE_ERROR` | `sdc sdf;` |
| 유효하나 미구현 문 | `1235 ER_NOT_SUPPORTED_YET` | `UPDATE`, `CREATE TABLE` |
| 존재하지 않는 테이블 | `1146 ER_NO_SUCH_TABLE` | `SELECT * FROM nonexistent` |
| Cube 이미 존재 | `1050 ER_TABLE_EXISTS_ERROR` | 중복 `CREATE CUBE` |
| 읽기 전용 모드 | `1290 ER_OPTION_PREVENTS_STATEMENT` | 읽기 전용 클러스터 쓰기 |

### 미지원 MySQL 문 — WOW-DB 대체

| MySQL 문 ❌ | WOW-DB 대체 ✅ |
|------------|----------------|
| `CREATE TABLE` | `CREATE CUBE` |
| `CREATE INDEX` | `ALTER CUBE ... ADD INDEX` |
| `CREATE VIEW` | `CREATE SESSION MATERIALIZED VIEW` |
| `UPDATE` | 해당 없음 (OLAP — point update 미지원) |
| `BEGIN` / `COMMIT` | 해당 없음 (내부 2PC만 지원) |

### 동작 예시

```
mysql> sdc
       sdf;
ERROR 1064 (42000): You have an error in your SQL syntax near 'sdc' (line 1)

mysql> UPDATE page_events SET event_name = 'click' WHERE id = 1;
ERROR 1235 (42000): This version of WOW-DB doesn't yet support 'UPDATE statement'

mysql> CREATE TABLE foo (id INT);
ERROR 1235 (42000): This version of WOW-DB doesn't yet support 'CREATE TABLE (use CREATE CUBE instead)'
```

---

## 12. 클러스터 관리 명령

❌ 미구현 (Phase CM — `tasks-cluster-management.md` 참조)  
상세 설계: [`design/cluster-management.md`](./cluster-management.md)

### ALTER CLUSTER JOIN ❌

새 노드를 클러스터에 추가한다. 등록된 QN 주소 기반으로 모든 노드 타입이 참가한다.

```sql
-- 문법
ALTER CLUSTER JOIN <node_type> '<address>:<port>' [AS '<node_id>'];

-- 예시
ALTER CLUSTER JOIN QN  '10.0.0.5:9010';
ALTER CLUSTER JOIN QN  '10.0.0.5:9010' AS 'qn-4';
ALTER CLUSTER JOIN CN  '10.0.0.6:9040' AS 'cn-3';
ALTER CLUSTER JOIN SN  '10.0.0.7:9060' AS 'sn-4';
```

**응답 예시**:
```
Query OK. Node 'sn-4' joined the cluster. Rebalance started (job_id: a1b2-...).
```

**제약**:
- QN은 홀수 개를 유지해야 한다 (짝수 시 경고)
- 동일 address:port 중복 등록 시 `ERROR 3003 (WW000): Node already registered`

---

### ALTER CLUSTER DRAIN ❌

노드를 READONLY → DRAINING 상태로 전환한다. INSERT 라우팅에서 즉시 제외되며, 백그라운드 데이터 이전이 시작된다.

```sql
-- 문법
ALTER CLUSTER DRAIN '<node_id>';

-- 예시
ALTER CLUSTER DRAIN 'sn-4';
ALTER CLUSTER DRAIN 'cn-2';
ALTER CLUSTER DRAIN 'qn-3';
```

**응답 예시**:
```
Query OK. Node 'sn-4' is now DRAINING.
Shards to migrate: 16 | Estimated time: ~2m 30s
Monitor: SHOW CLUSTER REBALANCE;
```

**오류 케이스**:
```
ERROR 3002 (WW000): Cannot drain 'qn-1': Raft quorum would be lost (2 nodes remaining)
```

---

### ALTER CLUSTER DISMISS ❌

DRAIN 완료된 노드를 영구 제거한다. 데이터가 남아있는 경우 `FORCE`가 필요하다.

```sql
-- 문법
ALTER CLUSTER DISMISS '<node_id>' [FORCE];

-- DRAIN 완료 후 정상 제거
ALTER CLUSTER DISMISS 'sn-4';

-- 데이터가 있는 노드 강제 제거 (Shard 메타데이터 삭제 → 데이터 영구 손실)
ALTER CLUSTER DISMISS 'sn-4' FORCE;
```

**FORCE 없이 데이터 있는 노드 DISMISS 시 오류**:
```
ERROR 3001 (WW000): Node 'sn-4' has 8 shards with data.
Run 'ALTER CLUSTER DRAIN sn-4' first, or use FORCE to permanently delete all data.
```

**⚠ 주의**: `FORCE` 는 해당 노드의 모든 Shard 메타데이터를 Raft KV에서 삭제한다.
복제본이 없는 경우 데이터가 영구 손실된다.

---

### ALTER CLUSTER REBALANCE ❌

노드 간 Shard 불균형을 수동으로 재조정한다.

```sql
ALTER CLUSTER REBALANCE;
```

---

### SHOW CLUSTER ❌

```sql
-- 전체 노드 목록
SHOW CLUSTER NODES;

-- 클러스터 상태 요약
SHOW CLUSTER STATUS;

-- 진행 중인 Rebalance 작업
SHOW CLUSTER REBALANCE;
```

**SHOW CLUSTER NODES 출력 컬럼**: `node_id, type, address, state, shards, joined_at`  
**SHOW CLUSTER STATUS 출력 컬럼**: `metric, value`  
**SHOW CLUSTER REBALANCE 출력 컬럼**: `job_id, type, from_node, to_node, shards_done, eta_secs`
