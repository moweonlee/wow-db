# WOW-DB 오류 처리 명세 (Error Handling Specification)

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-14  
**상태**: 구현 기준 문서

---

## 1. 개요

WOW-DB는 MySQL 8.0 Wire Protocol을 준수한다. 클라이언트(MySQL Workbench, mysql CLI, JDBC 드라이버 등)는 WOW-DB에서 반환하는 오류가 표준 MySQL 오류 형식을 따를 것을 기대한다. 인식되지 않는 SQL에 대해 임의의 결과 행을 반환하는 것은 클라이언트를 혼동시키고, 자동화 스크립트의 오류 탐지를 방해하며, MySQL 호환성 원칙(Constitution Principle VI)을 위반한다.

### 1.1 오류 응답 형식

MySQL Error Packet 형식:
```
ERROR <error_code> (<sqlstate>): <error_message>
```

예시:
```
ERROR 1064 (42000): You have an error in your SQL syntax; check the manual...
ERROR 1235 (42000): This version of WOW-DB doesn't yet support 'UPDATE statement'
```

---

## 2. 오류 분류 체계

### 2.1 파싱 오류 (Parse Error)

| 항목 | 값 |
|------|-----|
| MySQL Error Code | `1064` (`ER_PARSE_ERROR`) |
| SQLSTATE | `42000` |
| 적용 조건 | SQL 문법이 MySQL 8.0 방언을 따르지 않는 경우 |
| 메시지 형식 | `You have an error in your SQL syntax near '<token>' at line <N>` |

**트리거 조건**:
- `sqlparser-rs` MySQL 방언 파서가 파싱에 실패한 경우
- 의미 없는 임의 문자열 (예: `sdc`, `abc def`, `???`)
- 불완전한 SQL (예: `SELECT FROM`, `INSERT INTO`)
- WOW-DB 커스텀 파서도 인식하지 못하는 경우

**구현 원칙**: `WowDbParser::parse()` 호출 결과가 `Err(_)`이면 `ER_PARSE_ERROR` 반환.

### 2.2 미지원 기능 오류 (Not Supported Yet)

| 항목 | 값 |
|------|-----|
| MySQL Error Code | `1235` (`ER_NOT_SUPPORTED_YET`) |
| SQLSTATE | `42000` |
| 적용 조건 | 문법적으로 유효한 SQL이지만 WOW-DB가 아직 실행을 지원하지 않는 경우 |
| 메시지 형식 | `This version of WOW-DB doesn't yet support '<statement type>'` |

**트리거 조건**:
- `sqlparser-rs`가 파싱에 성공했으나 WOW-DB 실행 엔진이 해당 문을 처리하지 못하는 경우
- 현재 미구현 상태(Phase D 이후 구현 예정)인 DDL/DML 문

**현재 해당되는 문 목록**:

| SQL 문 | 상태 | 비고 |
|--------|------|------|
| `UPDATE` | ❌ 미지원 | OLAP DB — point update 없음 |
| `REPLACE` | ❌ 미지원 | WOW-DB 설계 범위 외 |
| `CALL` | ❌ 미지원 | 저장 프로시저 없음 |
| `CREATE VIEW` | ❌ 미지원 | `CREATE SESSION MATERIALIZED VIEW` 사용 |
| `CREATE INDEX` | ❌ 미지원 | `ALTER CUBE ... ADD INDEX` 사용 |
| `CREATE TABLE` | ❌ 미지원 | `CREATE CUBE` 사용 |
| `CREATE FUNCTION` | ❌ 미지원 | UDF 미구현 |
| `BEGIN` / `COMMIT` / `ROLLBACK` | ❌ 미지원 | 명시적 트랜잭션 미구현 (내부 2PC만 지원) |
| `LOCK TABLES` | ❌ 미지원 | 잠금 없음 |
| `GRANT` / `REVOKE` | ❌ 미지원 | 권한 관리 미구현 |
| `PREPARE` / `EXECUTE` (prepared stmt) | ⚠️ 미구현 | Phase D 예정 |
| `LOAD DATA` | ❌ 미지원 | Stream Load HTTP API 사용 |

### 2.3 런타임 오류 (Runtime Error)

| 항목 | 값 |
|------|-----|
| MySQL Error Code | 상황별 상이 (아래 표 참조) |
| 적용 조건 | 파싱은 성공했으나 실행 중 오류 발생 |

| 상황 | Error Code | 메시지 |
|------|-----------|--------|
| 존재하지 않는 테이블/Cube 참조 | `1146` (`ER_NO_SUCH_TABLE`) | `Table '<db>.<table>' doesn't exist` |
| 존재하지 않는 Cube에 INSERT | `1146` (`ER_NO_SUCH_TABLE`) | `Table '<name>' doesn't exist` |
| Cube 이미 존재 | `1050` (`ER_TABLE_EXISTS_ERROR`) | `Table '<name>' already exists` |
| 컬럼 타입 불일치 | `1366` (`ER_TRUNCATED_WRONG_VALUE_FOR_FIELD`) | `Incorrect value for column '<col>'` |
| 읽기 전용 모드 쓰기 시도 | `1290` (`ER_OPTION_PREVENTS_STATEMENT`) | `WOW-DB is in read-only mode` |
| Cube 파싱 실패 | `1064` (`ER_PARSE_ERROR`) | `<파서 오류 메시지>` |
| 알 수 없는 데이터베이스 | `1049` (`ER_BAD_DB_ERROR`) | `Unknown database '<name>'` |
| 데이터베이스 삭제 대상 없음 | `1008` (`ER_DB_DROP_EXISTS`) | `Unknown database '<name>'` |

---

## 3. 오류 우선순위 결정 규칙

SQL 수신 시 다음 순서로 오류를 분류한다:

```
1. 읽기 전용 검사 (쓰기 문에 한함)
   → 실패 시: ER_OPTION_PREVENTS_STATEMENT (1290)

2. WOW-DB 커스텀 문 인식 (CREATE CUBE, ALTER CUBE, CREATE SESSION MV 등)
   → 파싱 실패 시: ER_PARSE_ERROR (1064)
   → 실행 실패 시: 상황별 런타임 오류

3. 표준 MySQL 문 인식 (sqlparser-rs MySQL 방언)
   → 파싱 실패 시: ER_PARSE_ERROR (1064)
   → 파싱 성공 + 미구현 실행기: ER_NOT_SUPPORTED_YET (1235)

4. 위 모두 해당 없음 (fallback)
   → ER_PARSE_ERROR (1064): "Unrecognized SQL statement"
```

---

## 4. 구현 요구사항

### FR-ERR-001: Stub 응답 금지

**현재 문제**: `handler.rs`의 fallback 경로가 `(Query stub: ...)` 형태의 결과 행을 반환한다.

```
mysql> sdc
       sdf;
+---------------------------+
| result                    |
+---------------------------+
| (Query stub: sdc\nsdf)    |
+---------------------------+
```

**요구사항**: Stub 응답 대신 적절한 MySQL 오류 코드를 반환해야 한다.

**기대 동작**:
```
mysql> sdc
       sdf;
ERROR 1064 (42000): You have an error in your SQL syntax near 'sdc' (line 1)
```

### FR-ERR-002: 미지원 SQL 오류

**기대 동작**:
```
mysql> UPDATE page_events SET event_name = 'click' WHERE id = 1;
ERROR 1235 (42000): This version of WOW-DB doesn't yet support 'UPDATE statement'

mysql> CREATE TABLE foo (id INT);
ERROR 1235 (42000): This version of WOW-DB doesn't yet support 'CREATE TABLE (use CREATE CUBE instead)'
```

### FR-ERR-003: Prepared Statement 오류

COM_STMT_PREPARE / COM_STMT_EXECUTE는 현재 미구현이다. Stub 응답 대신 오류를 반환해야 한다.

**기대 동작**: COM_STMT_EXECUTE 호출 시 `ER_NOT_SUPPORTED_YET` 반환.

### FR-ERR-004: 오류 메시지 품질

- 오류 메시지는 클라이언트가 문제를 식별하는 데 충분한 컨텍스트를 포함해야 한다.
- `ER_PARSE_ERROR` 메시지는 문제가 된 SQL 앞부분(최대 60자)을 포함해야 한다.
- `ER_NOT_SUPPORTED_YET` 메시지는 미지원 문 유형을 명시해야 한다.
- 영문으로 작성 (MySQL 클라이언트 호환을 위해).

---

## 5. 분류별 SQL 예시

### 5.1 ER_PARSE_ERROR를 반환해야 하는 경우

```sql
sdc sdf              -- 임의 문자열
SELECT FROM          -- 불완전한 SELECT
INSERT               -- 테이블 없는 INSERT
???                  -- 특수 문자
```

### 5.2 ER_NOT_SUPPORTED_YET을 반환해야 하는 경우

```sql
UPDATE t SET a = 1   -- 유효한 MySQL 문법, WOW-DB 미지원
CREATE TABLE foo (id INT)  -- CREATE TABLE (CREATE CUBE 사용 권고)
BEGIN                -- 명시적 트랜잭션 미지원
CALL my_proc()       -- 저장 프로시저 없음
```

### 5.3 정상 처리되어야 하는 경우 (오류 없음)

```sql
CREATE CUBE page_events (...)   -- WOW-DB DDL
SELECT * FROM page_events       -- SELECT
INSERT INTO page_events (...)   -- INSERT
SHOW CUBES                      -- 메타데이터 조회
```

---

## 6. 미구현 SQL 문에 대한 안내 메시지

일부 MySQL 표준 문은 WOW-DB 전용 문으로 대체되어야 한다. 이 경우 오류 메시지에 안내를 포함한다.

| MySQL 문 | WOW-DB 대체 | 안내 메시지 |
|----------|------------|------------|
| `CREATE TABLE` | `CREATE CUBE` | `use CREATE CUBE instead` |
| `CREATE INDEX` | `ALTER CUBE ... ADD INDEX` | `use ALTER CUBE ... ADD INDEX instead` |
| `CREATE VIEW` | `CREATE SESSION MATERIALIZED VIEW` | `use CREATE SESSION MATERIALIZED VIEW` |

---

## 7. 관련 요구사항 참조

- **FR-001 ~ FR-025**: `spec.md` 기능 요구사항
- **Constitution Principle VI (MySQL Compat)**: MySQL 8.0 클라이언트 비호환 변경 금지
- **FR-NEW-001 (Behavioral Routing)**: `design/query-routing-smv.md`
- **SQL Syntax Reference**: `design/sql_syntax.md` — 지원 문 목록
