# 자동 쿼리 라우팅 — Event Table ↔ Behavioral Table 투명 선택

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-14  
**모듈**: `query-node/src/planner/behavioral_router.rs`  
**관련 요구사항**: FR-000, FR-NEW-001 (이 문서에서 신규 정의)  
**참조**: `design/query-execution-model.md`, `design/logical-to-physical-mapping.md`

---

## 용어 정의

| 용어 | 설명 |
|---|---|
| **Event Table** | `CREATE TABLE` DDL로 생성. 이벤트 1건 = 1행. 원시 이벤트 저장소 |
| **Behavioral Table (BT)** | `CREATE SESSION MATERIALIZED VIEW` DDL로 생성. 세션 1개 = 1행. Funnel/Cohort/Path 분석에 최적화된 파생 테이블 |
| **Behavioral Query** | FUNNEL_COUNT, COHORT_ANALYSIS, PATH_ANALYSIS 등 행동 패턴 분석 쿼리 |
| **Event Query** | COUNT, GROUP BY 집계 등 단순 이벤트 롤업 쿼리 |
| **Behavioral Routing** | 쿼리 패턴을 분석하여 Event Table 또는 Behavioral Table로 자동 라우팅하는 기능 |
| **Behavioral Guidance** | 시스템이 사용자에게 Behavioral Table 생성을 자연스럽게 유도하는 UX/성능 힌트 |

---

## 1. 핵심 개념 및 설계 동기

### 1.1 WOW-DB의 핵심 차별화 요소

WOW-DB의 가장 근본적인 차별점은 **두 개의 물리 레이아웃을 유지하면서, 어느 레이아웃에 쿼리를 보낼지를 사용자가 아닌 엔진이 자동으로 결정한다**는 데 있다.

```
사용자가 항상 작성하는 것 (베이스 테이블만 기억):
  SELECT FUNNEL_COUNT(...) FROM page_events WHERE ...

엔진이 실제로 실행하는 것 (Behavioral Routing):
  ┌─────────────────────────────────────────────────────────┐
  │ Query Pattern Detector                                  │
  │                                                         │
  │  "이 쿼리가 행동 분석(Behavioral)에 해당하는가?"         │
  │                                                         │
  │  YES → page_events_sessions (Behavioral Table) 에서 실행│
  │  NO  → page_events (Event Table) 에서 실행              │
  └─────────────────────────────────────────────────────────┘
```

이것이 가능한 이유는 WOW-DB가 이벤트 데이터를 수집할 때부터 **두 가지 물리 레이아웃**을 동시에 유지하기 때문이다.

| 레이아웃 | 물리 형태 | 최적인 워크로드 |
|---|---|---|
| **Event Table** | 이벤트 1건 = 1행, `ORDER BY (user_id, event_time)` | 날짜별 집계, 이벤트 수 카운트, 원시 이벤트 조회 |
| **Behavioral Table** | 세션 1개 = 1행, `session_id, event_sequence[]` 포함 | Funnel, Cohort, Path, 세션 길이 분석 |

### 1.2 왜 사용자가 직접 선택하게 해서는 안 되는가

세션 기반 분석 시스템의 공통 문제:

- 분석가는 "어떤 물리 레이아웃이 이 쿼리에 최적인지"를 알지 못한다
- Behavioral Table을 명시적으로 참조해야 한다면 그냥 테이블이 하나 더 생긴 것과 다를 바 없다
- 쿼리마다 `FROM page_events_sessions` 대 `FROM page_events`를 선택해야 한다면 도입 장벽이 높아진다

**WOW-DB의 보장**: 사용자는 항상 `FROM page_events` (Event Table)만 기억하면 된다. 엔진이 쿼리 패턴을 분석하여 최적 레이아웃을 seamlessly 선택한다.

### 1.3 Behavioral Table이 필요한 이유 (웹 분석의 본질)

웹 분석에서 가장 가치 있는 질문은 거의 모두 **행동 패턴**에 관한 것이다:

- "사용자가 어떤 순서로 행동하는가?" → Funnel
- "한 번 온 사용자가 다시 돌아오는가?" → Cohort  
- "가장 흔한 이동 경로는 무엇인가?" → Path

이 질문들은 **이벤트 스트림(Event Table)** 이 아니라 **세션 단위의 행동 집약(Behavioral Table)** 에서 자연스럽게 표현된다. Behavioral Table 없이 Event Table에서 직접 Funnel을 계산하면:

```
Event Table에서 Funnel 계산:
  1. user_id별 이벤트 가져오기 (수억 행 스캔)
  2. 이벤트를 시간 순으로 정렬 (O(N log N))
  3. 각 사용자별로 step sequence 순회 (O(N) per user)
  4. time window 필터 적용
  총 비용: 매우 높음 (이벤트 수 비례)

Behavioral Table에서 Funnel 계산:
  1. event_sequence[] 배열 직접 접근 (세션 이미 구성됨)
  2. step bitmask SIMD 연산 (단순 비트 비교)
  총 비용: 4–10× 더 낮음 (세션 수 비례, 이미 정렬됨)
```

웹 분석에서 Behavioral Table은 선택사항이 아닌 **성능과 분석 정확성을 동시에 보장하는 필수 레이아웃**이다.

---

## 2. Behavioral Guidance — Behavioral Table 생성으로의 자연스러운 유도

### 2.1 개념

**Behavioral Guidance**는 WOW-DB가 사용자에게 Behavioral Table 생성의 **가치를 실시간으로 인식시키고** 자연스럽게 생성을 유도하는 메커니즘이다.

이것은 강제가 아니다. Behavioral Table이 없어도 Event Table에서 쿼리가 실행된다 (Fallback). 하지만 시스템이 "지금 Behavioral Table을 만들면 이 쿼리가 X배 빨라집니다"를 알려줌으로써 사용자 스스로 생성을 결정하게 한다.

### 2.2 Guidance 트리거 시점

| 상황 | Guidance 내용 |
|---|---|
| Behavioral Query가 처음 실행되고 Behavioral Table이 없을 때 | `"이 쿼리는 Behavioral Table을 생성하면 약 {X}배 빨라집니다. [지금 생성하기]"` |
| EXPLAIN 실행 시 Behavioral Table이 없을 때 | `"Behavioral Table 부재: 현재 Event Table 스캔 {N}행. 생성 시 {M}행으로 감소 예상"` |
| 쿼리 실행 시간이 임계값 초과 + Behavioral Table로 해결 가능할 때 | Query Profiler에 `"Behavioral Table 생성으로 개선 가능"` 표시 |
| Web UI에서 Table 생성 직후 | `"Session Materialized View를 생성하시겠습니까?"` 마법사 자동 제안 |

### 2.3 생성 유도 흐름

```
[최초 FUNNEL_COUNT 쿼리 실행]
         │
         ▼
 Behavioral Table 존재?
  │ NO
  ├── Event Table로 Fallback 실행 (결과 반환, 오류 없음)
  ├── 응답 헤더에 힌트 추가:
  │     Warning: No Behavioral Table found for 'page_events'.
  │     Query took 4,200ms. Estimated 420ms with Behavioral Table.
  │     Run: CREATE SESSION MATERIALIZED VIEW page_events_sessions
  │          FROM page_events USER KEY user_id SESSION TIMEOUT 30 MINUTE;
  └── Web UI: 배너로 "Behavioral Table을 생성하면 10배 빠릅니다" 표시

[사용자가 Behavioral Table 생성 결정]
         │
         ▼
 CREATE SESSION MATERIALIZED VIEW 실행
         │
         ▼
 이후 모든 Behavioral Query → 자동으로 Behavioral Table 사용
 (사용자가 쿼리를 수정할 필요 없음)
```

---

## 3. 쿼리 패턴 분류 규칙

Behavioral Router는 논리 계획(LogicalPlan) 생성 직후 CBO 단계 이전에 실행된다. AST 분석을 통해 쿼리를 **세 가지 패턴**으로 분류한다.

### 3.1 패턴 A: Behavioral Query (행동 분석 쿼리)

아래 조건 중 하나라도 해당하면 Behavioral Table이 더 유리하다고 판단한다.

| 분류 신호 | 예시 | 판단 이유 |
|---|---|---|
| `FUNNEL_COUNT(user_key=...)` 함수 사용 | `FUNNEL_COUNT(user_key => user_id, ...)` | Funnel은 세션 내 순서 보장 필요 |
| `COHORT_ANALYSIS(user_key=...)` 함수 사용 | `COHORT_ANALYSIS(user_key => user_id, ...)` | Cohort는 사용자 최초 진입 기준 그룹화 |
| `PATH_ANALYSIS(user_key=...)` 함수 사용 | `PATH_ANALYSIS(user_key => user_id, ...)` | Path는 세션 내 이벤트 시퀀스 필요 |
| `GROUP BY session_id` 포함 | `SELECT session_id, COUNT(*) FROM ... GROUP BY session_id` | session_id는 Behavioral Table 전용 컬럼 |
| `WHERE session_id = ...` 포함 | `WHERE session_id = 'abc'` | session_id는 Behavioral Table에만 존재 |
| `session_start` / `session_end` 컬럼 참조 | `SELECT session_start, session_end FROM ...` | Behavioral Table 자동 생성 컬럼 참조 |
| `event_sequence` 배열 접근 | `event_sequence[1]` | Behavioral Table 자동 생성 배열 컬럼 참조 |
| `SESSION_TIMEOUT` 윈도우 파라미터 명시 | `FUNNEL_COUNT(session_timeout => 30 MINUTE, ...)` | 세션 경계 기반 분석 |

### 3.2 패턴 B: Event Query (이벤트 집계 쿼리)

아래 조건에 해당하면 Event Table이 더 유리하다고 판단한다.

| 분류 신호 | 예시 | 판단 이유 |
|---|---|---|
| `COUNT(*)` / `COUNT(event_name)` | `SELECT COUNT(*) FROM page_events` | 단순 이벤트 수 집계 |
| `GROUP BY event_name` (사용자 키 없음) | `SELECT event_name, COUNT(*) FROM ... GROUP BY event_name` | 이벤트 유형별 집계 |
| `GROUP BY DATE(event_time)` | 날짜별 이벤트 수 | 시계열 이벤트 집계 |
| Behavioral 함수 없이 단순 SELECT | `SELECT * FROM page_events LIMIT 100` | 원시 이벤트 조회 |
| `WHERE user_id = 'xxx'` 단일 사용자 조회 | 특정 사용자 이벤트 검색 | 단일 사용자 디버깅 |
| 이벤트 테이블 전용 컬럼만 참조 | `properties->>'$.ref'` | JSON 원본 필드 접근 |

### 3.3 패턴 C: Hybrid Query (혼합 쿼리)

Event Table과 Behavioral Table 양쪽을 모두 필요로 하는 쿼리. 예:

```sql
-- 일별 이벤트 수 + 그날의 Funnel 전환율 동시 조회
SELECT
    DATE(event_time) AS dt,
    COUNT(*) AS total_events,
    FUNNEL_COUNT(user_key => user_id, ...) AS funnel
FROM page_events
GROUP BY dt;
```

이 경우 CBO가 비용을 비교하여 결정한다:
- `FUNNEL_COUNT`의 비용이 지배적이면 → Behavioral Table로 라우팅
- `COUNT(*)`의 비용이 지배적이면 → Event Table 스캔 후 Behavioral Table에서 Funnel만 조회

---

## 4. Behavioral Router 알고리즘 (SMV Rewrite Pass)

### 4.1 파이프라인 내 위치

```
SQL 입력
  ↓
SQL Parser → AST
  ↓
Logical Planner → LogicalPlan
  ↓
[★] Behavioral Router (= SMV Rewrite Pass)  ← 이 문서에서 정의하는 단계
  ↓ (LogicalPlan의 FROM 절 테이블 참조가 교체될 수 있음)
CBO (비용 기반 최적화)
  ↓
Physical Planner → Fragment 배포
```

### 4.2 라우팅 알고리즘

```
입력: LogicalPlan (FROM page_events 참조 포함)
출력: LogicalPlan (필요시 FROM page_events_sessions 로 교체)

1. LogicalPlan에서 Event Table을 참조하는 모든 TableScan 노드를 추출
2. Metadata Store에서 해당 Event Table에 연결된 Behavioral Table 목록 조회
   → 없으면: 원본 LogicalPlan 반환 + Behavioral Guidance 힌트 생성
3. 각 TableScan 노드에 대해 Pattern Detector 실행:
   a. AST에서 Funnel/Cohort/Path 함수 시그니처 탐색
   b. session_id, session_start, session_end, event_sequence 컬럼 참조 탐색
   c. 패턴 A 신호 하나라도 발견 → behavioral_candidate 목록에 추가
4. behavioral_candidate가 비어 있으면 → 원본 LogicalPlan 반환
5. 각 Behavioral Table 후보에 대해 상태 확인:
   a. BT.state == ACTIVE && BT.last_refresh 유효 → 라우팅 후보
   b. BT.state == BUILDING || BT.state == STALE → Fallback 대상
6. 유효한 Behavioral Table이 있으면:
   a. TableScan(page_events) → TableScan(page_events_sessions) 교체
   b. 컬럼 매핑 적용 (event_time → session_start 등)
   c. "BEHAVIORAL_REWRITE" annotation을 LogicalPlan에 추가
7. 반환: 재작성된 LogicalPlan
```

### 4.3 컬럼 매핑 테이블

Behavioral Table로 라우팅 시 쿼리 내 컬럼 참조가 자동으로 매핑된다.

| Event Table 컬럼 | Behavioral Table 컬럼 | 비고 |
|---|---|---|
| `event_time` (필터 용도) | `session_start` | 시간 범위 필터는 세션 시작 기준으로 재적용 |
| `user_id` / `device_id` | `user_id` / `device_id` | 동일 (User Key) |
| `event_name` | `event_sequence` 내 조회 | 배열 접근으로 변환 |
| — (미존재) | `session_id` | Behavioral Table 자동 생성 |
| — (미존재) | `session_end` | Behavioral Table 자동 생성 |
| — (미존재) | `session_event_count` | Behavioral Table 자동 생성 |
| `properties` | `properties` | 동일 (복사됨) |

---

## 5. 비용 모델 (CBO 통합)

Behavioral Router가 후보를 생성한 뒤, CBO는 두 실행 계획의 비용을 추정하여 최종 선택을 한다.

### 5.1 Event Table 비용 추정

```
C_event = scan_cost(page_events, column_list, predicate)
         + agg_cost(funnel_steps, row_count)

scan_cost = (bytes_to_scan / io_bandwidth) * selectivity_factor
agg_cost  = Funnel은 per-user 정렬 및 순서 검증 → O(N log N)
```

Behavioral 함수는 이벤트 레벨 데이터에서 **사용자별 그룹화 + 이벤트 시퀀스 재구성**이 필요하므로 비용이 높다.

### 5.2 Behavioral Table 비용 추정

```
C_bt = scan_cost(page_events_sessions, column_list, predicate)
      + agg_cost(funnel_steps, session_count)

session_count << event_count  →  C_bt < C_event (대부분의 경우)
```

세션 경계 계산이 이미 완료된 데이터를 스캔하므로, 같은 Funnel을 계산하더라도 Event Table 대비 훨씬 적은 처리량으로 결과를 얻는다.

### 5.3 라우팅 결정 임계값

| 조건 | 라우팅 결정 |
|---|---|
| `C_bt < C_event * 0.7` | Behavioral Table 사용 (30% 이상 비용 절감) |
| `C_bt >= C_event * 0.7` | Event Table 사용 (차이가 작으면 원본 유지) |
| Behavioral Table 상태가 STALE | Event Table Fallback + Guidance 힌트 |
| Behavioral Table 상태가 BUILDING | Event Table Fallback |
| Hybrid 쿼리 | Behavioral 비용이 지배적인 쪽 기준으로 판단 |

---

## 6. 투명성 보장 (Transparency Guarantee)

### 6.1 결과 동등성

Behavioral Table로 라우팅된 쿼리는 Event Table에서 실행한 쿼리와 **수학적으로 동일한 결과**를 반환한다.

이것이 보장되는 조건:
1. Behavioral Table의 `last_refresh` 시각 이후 INSERT된 이벤트가 없거나, 쿼리의 시간 범위에 포함되지 않는 경우
2. Behavioral Table의 세션 분리 로직(`SESSION TIMEOUT`)이 Funnel/Path 함수의 시간 윈도우 파라미터와 일치하는 경우

### 6.2 Fallback 동작

| 상황 | 처리 방식 |
|---|---|
| Behavioral Table 없음 | Event Table로 Fallback + Behavioral Guidance 힌트 |
| Behavioral Table STALE | Event Table로 Fallback + Guidance 힌트 |
| Session Timeout ≠ 쿼리 window 파라미터 | Event Table로 Fallback |
| Behavioral Table 부분 파티션만 커버 | Event Table로 Fallback (전체 범위 조회) |

모든 Fallback은 **오류 없이 자동으로** 처리된다. 사용자에게는 결과만 반환되고, Guidance 힌트는 선택적으로 표시된다.

---

## 7. 쿼리 힌트 (고급 제어)

```sql
-- Behavioral Table 강제 사용 (신선도 경고 무시)
SELECT FUNNEL_COUNT(...)
FROM page_events /*+ FORCE_BT(page_events_sessions) */
WHERE event_time >= '2026-04-01';

-- Event Table 강제 사용 (Behavioral Routing 비활성화)
SELECT FUNNEL_COUNT(...)
FROM page_events /*+ NO_BT */
WHERE event_time >= '2026-04-01';

-- 라우팅 결정 설명 (DRY RUN)
EXPLAIN BEHAVIORAL ROUTING
SELECT FUNNEL_COUNT(...) FROM page_events WHERE ...;
-- 출력:
-- Routing: YES → page_events_sessions (Behavioral Table)
-- Reason:  FUNNEL_COUNT(user_key=user_id) detected
-- BT Status: ACTIVE (last_refresh: 2026-04-14 10:00:00)
-- Estimated speedup: 4.7x
-- Rows (Event Table): 84,700,000
-- Rows (Behavioral Table): 18,200,000
```

---

## 8. EXPLAIN 출력 형식

`EXPLAIN` 실행 시 Behavioral Routing 여부가 항상 표시된다.

```
== Logical Plan ==
Project [step1_count, step2_count, step3_count]
  FunnelAggregate [user_key=user_id, steps=3, window=24h]
    Filter [session_start >= '2026-04-01']
      TableScan [page_events_sessions]   ← Behavioral Table로 라우팅됨
        Columns: [user_id, session_start, event_sequence]
        Partitions: 14 / 90 (pruned by session_start)

== Behavioral Routing ==
  Original table:      page_events (Event Table)
  Routed to:           page_events_sessions (Behavioral Table)
  Routing trigger:     FUNNEL_COUNT function
  BT last refresh:     2026-04-14 10:00:00
  Row count (event):   84,700,000
  Row count (behav.):  18,200,000
  Estimated speedup:   4.6x

== Physical Plan ==
Exchange (Merge)
  Compute Node Fragment × 4
    FunnelExec [AVX2, bitmask_steps=3]
      ScanExec [page_events_sessions, partition_prune=14/90]
```

---

## 9. 쿼리 패턴별 라우팅 결정 매트릭스

| 쿼리 형태 | 패턴 | Behavioral Table 라우팅 | 비고 |
|---|---|---|---|
| `SELECT COUNT(*) FROM page_events` | Event | ❌ Event Table | 단순 이벤트 카운트 |
| `SELECT event_name, COUNT(*) GROUP BY event_name` | Event | ❌ Event Table | 이벤트 유형별 집계 |
| `SELECT * FROM page_events WHERE user_id='u1'` | Event | ❌ Event Table | 단일 사용자 조회 |
| `SELECT DATE(event_time), COUNT(*) GROUP BY 1` | Event | ❌ Event Table | 시계열 집계 |
| `SELECT FUNNEL_COUNT(...) FROM page_events` | Behavioral | ✅ Behavioral Table | Funnel 함수 감지 |
| `SELECT COHORT_ANALYSIS(...) FROM page_events` | Behavioral | ✅ Behavioral Table | Cohort 함수 감지 |
| `SELECT PATH_ANALYSIS(...) FROM page_events` | Behavioral | ✅ Behavioral Table | Path 함수 감지 |
| `SELECT ... WHERE session_id='xxx'` | Behavioral | ✅ Behavioral Table | BT 전용 컬럼 참조 |
| `SELECT COUNT(*), FUNNEL_COUNT(...)` | Hybrid | CBO 결정 | 비용 비교 후 결정 |
| `SELECT FUNNEL_COUNT(...) /*+ NO_BT */` | Behavioral | ❌ Event Table | 힌트로 강제 우회 |
| BT 없음 | Behavioral | ❌ Event Table + Guidance | Fallback + 생성 유도 |
| BT 상태 = STALE | Behavioral | ❌ Event Table + Guidance | Fallback + 갱신 유도 |

---

## 10. 구현 위치

| 컴포넌트 | 파일 경로 | 책임 |
|---|---|---|
| Behavioral Router | `query-node/src/planner/behavioral_router.rs` | 패턴 감지, LogicalPlan 재작성 |
| Pattern Detector | `query-node/src/planner/behavioral_pattern.rs` | AST 분석, 패턴 A/B/C 분류 |
| BT Metadata Registry | `query-node/src/meta/bt_registry.rs` | BT 상태, 파티션 범위, 갱신 시각 |
| Routing Cost Model | `query-node/src/planner/behavioral_cost.rs` | Event Table vs BT 비용 추정 |
| Column Mapping | `query-node/src/planner/behavioral_column_map.rs` | 컬럼 참조 자동 변환 |
| Behavioral Guidance | `query-node/src/planner/behavioral_guidance.rs` | 힌트 생성, 성능 예측 메시지 |
| EXPLAIN 출력 | `query-node/src/planner/explain.rs` | Behavioral Routing 섹션 출력 |
| Query Profiler | `query-node/src/profiler/mod.rs` | behavioral_rewrite, guidance_triggered 기록 |

---

## 11. 기능 요구사항 (FR-NEW-001 시리즈)

| ID | 요구사항 | 우선순위 |
|---|---|---|
| FR-NEW-001-01 | 사용자는 항상 Event Table(Table)만을 FROM에 사용해도 된다. Behavioral Table 참조를 강제하지 않는다. | P0 |
| FR-NEW-001-02 | Behavioral Router가 FUNNEL_COUNT, COHORT_ANALYSIS, PATH_ANALYSIS 함수 사용을 감지하면 Behavioral Table 라우팅을 시도한다. | P0 |
| FR-NEW-001-03 | session_id, session_start, session_end, event_sequence 컬럼 참조 감지 시 Behavioral Table 라우팅을 시도한다. | P0 |
| FR-NEW-001-04 | Behavioral Table 라우팅 시 컬럼 참조(event_time → session_start 등)를 자동으로 매핑한다. 사용자 쿼리 수정 불필요. | P0 |
| FR-NEW-001-05 | CBO는 Event Table 실행 비용과 Behavioral Table 실행 비용을 비교하여 최종 라우팅을 결정한다. | P1 |
| FR-NEW-001-06 | Behavioral Table이 없거나 STALE 상태이면 자동으로 Event Table로 Fallback하며 오류를 반환하지 않는다. | P0 |
| FR-NEW-001-07 | Behavioral Table이 없을 때 Behavioral Query가 실행되면 Behavioral Guidance 힌트를 응답에 포함한다 (생성 권장 DDL, 예상 성능 향상). | P1 |
| FR-NEW-001-08 | EXPLAIN 출력에 Behavioral Routing 여부, 라우팅 이유, 예상 성능 향상을 표시한다. | P1 |
| FR-NEW-001-09 | `/*+ FORCE_BT(bt_name) */` 힌트로 Behavioral Table을 강제 사용할 수 있다. | P2 |
| FR-NEW-001-10 | `/*+ NO_BT */` 힌트로 Behavioral Routing을 비활성화할 수 있다. | P2 |
| FR-NEW-001-11 | Query Profiler가 behavioral_rewrite 여부, fallback_reason, 예상 성능 향상 배수를 기록한다. | P1 |

---

## 12. 웹 분석 워크로드 자동 최적화 요약

WOW-DB가 자동으로 처리하는 두 가지 핵심 웹 분석 워크로드:

```
분석가가 원하는 것                    WOW-DB가 자동으로 하는 것
────────────────────────────────────────────────────────────────
"퍼널 전환율을 보고 싶다"             → Behavioral Table의 event_sequence에서 
  → FUNNEL_COUNT FROM page_events        step bitmask SIMD 연산
                                         (Event Table보다 4–10× 빠름)

"코호트별 재방문율을 보고 싶다"       → Behavioral Table에서 user별
  → COHORT_ANALYSIS FROM page_events     첫 세션 날짜 기준 그룹화
                                         (재계산 없이 파생 컬럼 활용)

"사용자 이동 경로를 보고 싶다"        → Behavioral Table의 event_sequence[]
  → PATH_ANALYSIS FROM page_events       배열을 순회하며 패턴 매칭
                                         (이벤트 재정렬 비용 없음)

"어제 이벤트가 몇 건이냐"            → Event Table에서 직접 스캔
  → SELECT COUNT(*) FROM page_events     (Behavioral Table 불필요)
  WHERE DATE(event_time) = YESTERDAY

"일별 페이지뷰 트렌드"               → Event Table에서 날짜 집계
  → SELECT DATE(event_time), COUNT(*)    (시계열 집계는 Event Table이 적합)
  FROM page_events GROUP BY 1
```

이것이 WOW-DB가 "웹 분석에 특화된" 데이터베이스인 이유다.  
웹 분석에서 반복되는 두 가지 워크로드 — **이벤트 롤업(Event Query)** 과 **행동 패턴 분석(Behavioral Query)** — 를 엔진이 자동으로 인식하고, 각각에 최적화된 물리 레이아웃으로 seamlessly 실행한다.  
그리고 Behavioral Table이 없는 경우 자연스럽게 생성을 유도함으로써, 사용자가 의도치 않게 성능 최적화된 시스템을 구축하도록 안내한다.
