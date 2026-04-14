# Tasks: Behavioral Routing 구현 및 검증

**Branch**: `002-wow-db-srs-v02`  
**관련 요구사항**: FR-000, FR-NEW-001-01 ~ FR-NEW-001-11  
**상세 설계**: `design/query-routing-smv.md`  
**작성일**: 2026-04-14

---

## 핵심 검증 목표

이 Task 파일은 두 가지를 집중 검증한다.

```
검증 목표 1 — Behavioral Guidance
  Event Table에 Behavioral Query(FUNNEL/COHORT/PATH)가 실행될 때
  Behavioral Table(BT) pair가 없으면 → 경고 + 생성 권장 DDL 반환

검증 목표 2 — Behavioral Routing
  BT pair가 등록된 후 동일한 Behavioral Query를 실행하면
  → 쿼리 수정 없이 자동으로 BT를 사용하여 실행
```

**중요 전제**: BT pair가 없는 일반 테이블(일반 이벤트 데이터, 일반 집계 등)은 라우팅 대상이 아니다. Routing은 오직 (1) BT pair가 등록되어 있고 (2) 쿼리가 Behavioral 패턴일 때만 발동한다.

---

## Format

- **[P]**: 의존성 없음 — 병렬 실행 가능
- **[BG]**: Behavioral Guidance 관련
- **[BR]**: Behavioral Routing 관련
- **[TEST]**: 검증 테스트

---

## Phase A: 기반 인프라

**목적**: Pattern Detector와 BT Registry — 모든 후속 태스크의 전제 조건

### A-001 [P] Query Pattern Detector 구현

**파일**: `query-node/src/planner/behavioral_pattern.rs`

AST를 받아 쿼리가 Behavioral / Event / Hybrid 중 어느 패턴인지 분류한다.

```rust
pub enum QueryPattern {
    /// FUNNEL_COUNT / COHORT_ANALYSIS / PATH_ANALYSIS 감지, 또는 BT 전용 컬럼 참조
    Behavioral { triggers: Vec<BehavioralTrigger> },
    /// COUNT(*), GROUP BY event_name 등 단순 이벤트 집계
    Event,
    /// 양쪽이 섞인 쿼리
    Hybrid,
}

pub enum BehavioralTrigger {
    FunnelFunction,
    CohortFunction,
    PathFunction,
    SessionIdColumn,
    SessionStartColumn,
    SessionEndColumn,
    EventSequenceColumn,
    SessionTimeoutParam,
}

pub fn detect_pattern(ast: &Statement) -> QueryPattern { ... }
```

**Behavioral 감지 조건** (하나라도 해당하면 `Behavioral`):
- 함수 이름이 `FUNNEL_COUNT`, `COHORT_ANALYSIS`, `PATH_ANALYSIS`인 FunctionCall 노드 존재
- 컬럼 참조가 `session_id`, `session_start`, `session_end`, `event_sequence`, `session_event_count`인 노드 존재

**인수 기준**:
- `SELECT FUNNEL_COUNT(...) FROM t` → `Behavioral { triggers: [FunnelFunction] }`
- `SELECT COUNT(*) FROM t` → `Event`
- `SELECT COUNT(*), FUNNEL_COUNT(...) FROM t` → `Hybrid`
- `SELECT * FROM t WHERE session_id = 'x'` → `Behavioral { triggers: [SessionIdColumn] }`

---

### A-002 [P] BT Registry 구현

**파일**: `query-node/src/meta/bt_registry.rs`

Event Table(Cube)와 Behavioral Table(Session MV) 간의 페어링 메타데이터를 관리한다.

```rust
pub struct BtRegistry {
    /// event_table_name → Vec<BtEntry>
    pairs: HashMap<String, Vec<BtEntry>>,
}

pub struct BtEntry {
    pub bt_name:        String,       // Session MV 이름
    pub event_table:    String,       // 원본 Event Table 이름
    pub user_key:       String,       // USER KEY 컬럼
    pub session_timeout_sec: u64,
    pub state:          BtState,
    pub last_refresh:   Option<SystemTime>,
}

pub enum BtState {
    Building,
    Active,
    Refreshing,
    Stale,
    Error(String),
}

impl BtRegistry {
    /// Event Table 이름으로 연결된 BT 목록 반환
    pub fn get_bt_for_table(&self, event_table: &str) -> Vec<&BtEntry> { ... }

    /// BT pair 등록 (CREATE SESSION MATERIALIZED VIEW 실행 시 호출)
    pub fn register(&mut self, entry: BtEntry) { ... }

    /// BT 상태 업데이트
    pub fn update_state(&mut self, bt_name: &str, state: BtState) { ... }

    /// Active 상태인 BT만 반환 (라우팅 후보)
    pub fn get_active_bt(&self, event_table: &str) -> Option<&BtEntry> { ... }
}
```

**인수 기준**:
- `page_events`에 대한 BT가 없으면 `get_bt_for_table("page_events")` → 빈 Vec
- `CREATE SESSION MATERIALIZED VIEW` 실행 후 `register()` 호출 → `get_active_bt` 반환 Some
- BT 상태가 `Stale`이면 `get_active_bt` → None (라우팅 대상에서 제외)

---

### A-003 [P] BT Registry를 CubeManager에 통합

**파일**: `query-node/src/meta/cube_manager.rs` (기존 파일 수정)

`CubeManager`가 `BtRegistry`를 보유하고, `CREATE SESSION MATERIALIZED VIEW` DDL 처리 시 자동으로 Registry에 등록한다.

```rust
impl CubeManager {
    pub fn bt_registry(&self) -> &BtRegistry { ... }
    pub fn bt_registry_mut(&mut self) -> &mut BtRegistry { ... }

    /// CREATE SESSION MATERIALIZED VIEW 처리 후 자동 호출
    fn register_behavioral_table(&mut self, smv_def: &SessionMvDef) -> Result<()> {
        let entry = BtEntry {
            bt_name:             smv_def.name.clone(),
            event_table:         smv_def.source_cube.clone(),
            user_key:            smv_def.user_key.clone(),
            session_timeout_sec: smv_def.session_timeout_sec,
            state:               BtState::Building,
            last_refresh:        None,
        };
        self.bt_registry_mut().register(entry);
        Ok(())
    }
}
```

**인수 기준**:
- `CREATE SESSION MATERIALIZED VIEW page_events_sessions FROM page_events ...` 실행 후
  → `cube_mgr.bt_registry().get_active_bt("page_events")` 가 `Some(_)` 반환

---

## Phase B: Behavioral Guidance

**목적**: BT pair 없이 Behavioral Query 실행 시 경고 + 생성 권장 DDL 반환

### B-001 [BG] Behavioral Guidance 생성기 구현

**파일**: `query-node/src/planner/behavioral_guidance.rs`

```rust
pub struct BehavioralGuidance {
    /// MySQL Warning 메시지 (클라이언트에 전달)
    pub warning:          String,
    /// 권장 생성 DDL
    pub suggested_ddl:    String,
    /// 예상 성능 향상 배수 (CBO 추정, 없으면 None)
    pub estimated_speedup: Option<f64>,
    /// 실제 실행 소요 시간 (ms)
    pub actual_duration_ms: u64,
}

pub fn build_guidance(
    event_table:       &str,
    triggers:          &[BehavioralTrigger],
    actual_duration_ms: u64,
    row_count:         u64,
) -> BehavioralGuidance {
    let ddl = format!(
        "CREATE SESSION MATERIALIZED VIEW {event_table}_sessions\n\
         FROM {event_table}\n\
         USER KEY <user_key_column>\n\
         SESSION TIMEOUT 30 MINUTE\n\
         REFRESH INCREMENTAL;",
    );
    let estimated_speedup = estimate_speedup(row_count);
    let warning = format!(
        "No Behavioral Table found for '{event_table}'. \
         Query ran on Event Table ({actual_duration_ms}ms). \
         Estimated {speedup:.1}x faster with Behavioral Table. \
         Suggested: {ddl}",
        speedup = estimated_speedup.unwrap_or(5.0),
        ddl = ddl,
    );
    BehavioralGuidance { warning, suggested_ddl: ddl, estimated_speedup, actual_duration_ms }
}
```

**인수 기준**:
- `FUNNEL_COUNT` 트리거로 호출 시 → `warning` 문자열에 `"No Behavioral Table"` 포함
- `warning` 문자열에 `CREATE SESSION MATERIALIZED VIEW` DDL 포함
- `estimated_speedup` 이 `row_count >= 10_000` 이면 `Some(x)` where `x >= 2.0`

---

### B-002 [BG] MySQL 응답에 Guidance Warning 첨부

**파일**: `query-node/src/mysql_protocol/handler.rs` (기존 파일 수정)

쿼리 실행 후 `BehavioralGuidance`가 생성된 경우 MySQL `OK_PACKET` 또는 결과셋의 `warnings` 필드에 포함한다.

```rust
// handler.rs의 query 처리 흐름에 추가
async fn execute_query(&mut self, sql: &str) -> QueryResult {
    let pattern = detect_pattern(&ast);
    
    if matches!(pattern, QueryPattern::Behavioral { .. } | QueryPattern::Hybrid) {
        let active_bt = self.cube_mgr.bt_registry().get_active_bt(&table_name);
        if active_bt.is_none() {
            // Fallback: Event Table에서 실행
            let result = self.execute_on_event_table(sql).await?;
            
            // Guidance 생성 및 첨부
            let guidance = build_guidance(
                &table_name,
                &triggers,
                result.duration_ms,
                result.rows_scanned,
            );
            warn!(
                event_table = %table_name,
                triggers    = ?triggers,
                duration_ms = guidance.actual_duration_ms,
                "Behavioral Guidance: no BT pair found"
            );
            return result.with_warning(guidance.warning);
        }
    }
    // ... 정상 실행 또는 Behavioral Routing
}
```

**인수 기준**:
- BT 없는 상태에서 FUNNEL_COUNT 실행 → 결과 반환 + `warnings` 필드에 Guidance 메시지 포함
- BT 있는 상태에서 FUNNEL_COUNT 실행 → `warnings` 없음 (라우팅 성공)
- 일반 COUNT(*) 쿼리 → BT 없어도 `warnings` 없음 (Behavioral 패턴 아님)

---

### B-003 [TEST][BG] Guidance 검증 테스트

**파일**: `query-node/src/mysql_protocol/behavioral_routing_tests.rs` (신규)

```rust
// 테스트 1: BT 없을 때 Behavioral Query → Guidance Warning
#[tokio::test]
async fn test_guidance_when_no_bt() {
    setup_event_table_with_data();  // page_events 생성 + 데이터 삽입 (BT 없음)

    let result = execute_query(
        "SELECT FUNNEL_COUNT(user_key => user_id, timestamp => event_time, \
         window => INTERVAL 24 HOUR, \
         steps => [event_name='page_view', event_name='purchase']) \
         FROM page_events"
    ).await;

    // 결과는 정상 반환 (오류 없음 — Event Table Fallback)
    assert!(result.is_ok(), "Behavioral Query must not fail even without BT");

    // Warning이 포함되어야 함
    let warnings = result.warnings();
    assert!(!warnings.is_empty(), "Expected Behavioral Guidance warning");
    assert!(warnings[0].contains("No Behavioral Table"),
        "Warning must mention 'No Behavioral Table', got: {}", warnings[0]);
    assert!(warnings[0].contains("CREATE SESSION MATERIALIZED VIEW"),
        "Warning must include suggested DDL");
}

// 테스트 2: Event Query는 BT 없어도 Warning 없음
#[tokio::test]
async fn test_no_guidance_for_event_query() {
    setup_event_table_with_data();  // BT 없음

    let result = execute_query(
        "SELECT COUNT(*) FROM page_events WHERE event_time >= '2026-04-01'"
    ).await;

    assert!(result.is_ok());
    assert!(result.warnings().is_empty(),
        "Event Query must not trigger Behavioral Guidance");
}

// 테스트 3: 일반 테이블 (BT pair 없는 비-Cube) — Guidance 없음
#[tokio::test]
async fn test_no_guidance_for_plain_table() {
    // 일반 테이블 생성 (CREATE CUBE 아닌 CREATE TABLE, BT pair 없음)
    execute_query("CREATE TABLE raw_sessions (session_id VARCHAR(36), user_id VARCHAR(64), \
                   session_start DATETIME, event_sequence JSON)").await.unwrap();

    let result = execute_query(
        "SELECT COUNT(*) FROM raw_sessions WHERE session_id IS NOT NULL"
    ).await;

    // session_id 컬럼이 있어도 BT pair 미등록 테이블은 라우팅/Guidance 대상 아님
    assert!(result.is_ok());
    assert!(result.warnings().is_empty(),
        "Plain table without BT pair must not trigger Behavioral Guidance");
}
```

**인수 기준**:
- test_guidance_when_no_bt: PASS (결과 OK + Warning 포함)
- test_no_guidance_for_event_query: PASS (결과 OK + Warning 없음)
- test_no_guidance_for_plain_table: PASS (결과 OK + Warning 없음)

---

## Phase C: Behavioral Router (자동 라우팅)

**목적**: BT pair가 있을 때 Behavioral Query를 자동으로 BT에서 실행

### C-001 [BR] LogicalPlan Rewriter 구현

**파일**: `query-node/src/planner/behavioral_router.rs`

```rust
pub struct BehavioralRouter<'a> {
    bt_registry: &'a BtRegistry,
}

pub struct RoutingResult {
    pub plan:        LogicalPlan,
    pub routed:      bool,
    pub bt_used:     Option<String>,  // 사용된 BT 이름
    pub guidance:    Option<BehavioralGuidance>,  // BT 없을 때 채워짐
}

impl<'a> BehavioralRouter<'a> {
    pub fn route(&self, plan: LogicalPlan, pattern: &QueryPattern) -> RoutingResult {
        // 1. Event 패턴이면 라우팅 없음
        if matches!(pattern, QueryPattern::Event) {
            return RoutingResult { plan, routed: false, bt_used: None, guidance: None };
        }

        // 2. 쿼리에서 스캔 대상 Event Table 이름 추출
        let Some(event_table) = extract_scan_table(&plan) else {
            return RoutingResult { plan, routed: false, bt_used: None, guidance: None };
        };

        // 3. BT Registry에서 Active BT 조회
        let Some(bt_entry) = self.bt_registry.get_active_bt(&event_table) else {
            // BT 없음 → Guidance 생성, 원본 plan 반환
            let guidance = build_guidance(&event_table, &[], 0, 0);
            return RoutingResult { plan, routed: false, bt_used: None, guidance: Some(guidance) };
        };

        // 4. TableScan 노드를 BT로 교체 + 컬럼 매핑
        let rewritten = rewrite_table_scan(plan, &event_table, &bt_entry.bt_name);
        let mapped    = apply_column_mapping(rewritten);

        RoutingResult {
            plan:     mapped,
            routed:   true,
            bt_used:  Some(bt_entry.bt_name.clone()),
            guidance: None,
        }
    }
}
```

**인수 기준**:
- Behavioral 패턴 + Active BT 존재 → `routed = true`, `bt_used = Some("page_events_sessions")`
- Behavioral 패턴 + BT 없음 → `routed = false`, `guidance = Some(_)`
- Event 패턴 → `routed = false`, `guidance = None`
- Hybrid 패턴 + BT 존재 → CBO 비용 비교 후 결정

---

### C-002 [BR] Column Mapping 구현

**파일**: `query-node/src/planner/behavioral_column_map.rs`

Behavioral Router가 TableScan을 교체할 때 컬럼 참조도 함께 변환한다.

```rust
/// Event Table → Behavioral Table 컬럼 매핑 테이블
pub fn map_column(col: &str, context: MappingContext) -> &str {
    match (col, context) {
        // 시간 범위 필터 용도 → 세션 시작 기준으로 재적용
        ("event_time", MappingContext::WhereFilter) => "session_start",
        // SELECT 절에서 event_time → session_start
        ("event_time", MappingContext::Projection)  => "session_start",
        // User Key 컬럼들 — 동일하게 유지
        ("user_id", _)    => "user_id",
        ("device_id", _)  => "device_id",
        // 그 외 컬럼 — 동일하게 유지 (BT에도 복사됨)
        (other, _)        => other,
    }
}
```

**인수 기준**:
- `WHERE event_time >= '2026-04-01'` → `WHERE session_start >= '2026-04-01'`
- `SELECT user_id, event_time` → `SELECT user_id, session_start`
- `WHERE user_id = 'u1'` → `WHERE user_id = 'u1'` (변경 없음)

---

### C-003 [BR] Behavioral Router를 Query 실행 파이프라인에 통합

**파일**: `query-node/src/mysql_protocol/handler.rs` (기존 파일 수정)

Logical Plan 생성 직후, CBO 이전에 Behavioral Router를 실행한다.

```rust
async fn plan_and_execute(&mut self, sql: &str) -> QueryResult {
    // 1. Parse
    let ast = self.parser.parse(sql)?;

    // 2. Logical Plan
    let logical_plan = self.logical_planner.plan(&ast)?;

    // 3. [★] Behavioral Routing (CBO 이전)
    let pattern      = detect_pattern(&ast);
    let router       = BehavioralRouter::new(self.cube_mgr.bt_registry());
    let routing      = router.route(logical_plan, &pattern);

    if let Some(guidance) = routing.guidance {
        // BT 없음 → Event Table로 실행 + Warning 첨부
        let result = self.execute_plan(routing.plan).await?;
        return result.with_warning(guidance.warning);
    }

    if routing.routed {
        // BT로 라우팅됨
        info!(
            bt  = %routing.bt_used.as_deref().unwrap_or("?"),
            sql = %sql,
            "Behavioral Routing: query routed to Behavioral Table"
        );
    }

    // 4. CBO
    let physical_plan = self.cbo.optimize(routing.plan)?;

    // 5. Execute
    self.execute_plan(physical_plan).await
}
```

**인수 기준**:
- BT 있음 + FUNNEL_COUNT 쿼리 → `plan.scan_table == "page_events_sessions"`
- BT 있음 + COUNT(*) 쿼리 → `plan.scan_table == "page_events"` (라우팅 없음)
- BT 없음 + FUNNEL_COUNT 쿼리 → `plan.scan_table == "page_events"` + warning 생성

---

### C-004 [BR] EXPLAIN 출력에 Behavioral Routing 정보 추가

**파일**: `query-node/src/planner/explain.rs` (기존 파일 수정)

```
== Behavioral Routing ==
  Original table:  page_events (Event Table)
  Routed to:       page_events_sessions (Behavioral Table)
  Routing trigger: FUNNEL_COUNT function
  BT last refresh: 2026-04-14 10:00:00
  Row count (event): 84,700,000
  Row count (bt):    18,200,000
  Estimated speedup: 4.6x
```

BT 없는 경우:
```
== Behavioral Routing ==
  Original table:  page_events (Event Table)
  Routed to:       (none — no Behavioral Table found)
  Guidance:        CREATE SESSION MATERIALIZED VIEW page_events_sessions
                   FROM page_events USER KEY user_id SESSION TIMEOUT 30 MINUTE;
```

**인수 기준**:
- BT 있음 + `EXPLAIN SELECT FUNNEL_COUNT(...)` → `"Behavioral Routing"` 섹션에 `"Routed to: page_events_sessions"` 포함
- BT 없음 + `EXPLAIN SELECT FUNNEL_COUNT(...)` → `"Guidance:"` 섹션에 DDL 포함
- `EXPLAIN SELECT COUNT(*) ...` → `"Behavioral Routing"` 섹션 없음

---

## Phase D: 통합 검증 테스트

**목적**: 전체 Behavioral Routing 흐름을 end-to-end로 검증

**파일**: `query-node/src/mysql_protocol/behavioral_routing_tests.rs`

---

### D-001 [TEST][BR] BT 생성 후 자동 라우팅 검증 — FUNNEL

```rust
#[tokio::test]
async fn test_routing_to_bt_on_funnel_query() {
    // Setup: Event Table 생성 + 데이터 삽입
    setup_event_table_with_funnel_data(); // 1000명 사용자, page_view→purchase 시나리오

    // Step 1: BT 없는 상태에서 FUNNEL_COUNT 실행 → Guidance Warning
    let r1 = execute_funnel_query().await;
    assert!(r1.is_ok());
    assert!(!r1.warnings().is_empty(), "Must warn: no BT found");

    // Step 2: Behavioral Table(Session MV) 생성
    execute_query(
        "CREATE SESSION MATERIALIZED VIEW page_events_sessions \
         FROM page_events USER KEY user_id SESSION TIMEOUT 30 MINUTE"
    ).await.unwrap();
    
    // Step 3: BT 있는 상태에서 동일한 FUNNEL_COUNT 실행 → 자동 라우팅
    let r2 = execute_funnel_query().await;
    assert!(r2.is_ok());
    assert!(r2.warnings().is_empty(), "Must not warn after BT created");

    // Step 4: EXPLAIN으로 BT 사용 확인
    let explain = execute_query(
        "EXPLAIN SELECT FUNNEL_COUNT(user_key => user_id, timestamp => event_time, \
         window => INTERVAL 24 HOUR, \
         steps => [event_name='page_view', event_name='purchase']) \
         FROM page_events"
    ).await.unwrap();
    let plan_text = explain.as_text();
    assert!(plan_text.contains("page_events_sessions"),
        "EXPLAIN must show BT: {}", plan_text);
    assert!(plan_text.contains("Behavioral Routing"),
        "EXPLAIN must show routing section: {}", plan_text);

    // Step 5: 결과 동등성 확인 — BT 사용 전후 동일한 Funnel 결과
    let result_without_bt = r1.funnel_counts();
    let result_with_bt    = r2.funnel_counts();
    assert_eq!(result_without_bt, result_with_bt,
        "Results must be identical whether using Event Table or Behavioral Table");
}
```

**인수 기준**:
- Step 1: OK + warnings 있음
- Step 3: OK + warnings 없음
- Step 4: EXPLAIN에 `"page_events_sessions"`, `"Behavioral Routing"` 포함
- Step 5: Funnel 카운트가 BT 사용 전후 동일

---

### D-002 [TEST][BR] BT 생성 후 자동 라우팅 검증 — COHORT

```rust
#[tokio::test]
async fn test_routing_to_bt_on_cohort_query() {
    setup_event_table_with_cohort_data(); // signup → purchase 코호트 데이터

    // BT 없음 → Warning
    let r1 = execute_query(
        "SELECT COHORT_ANALYSIS(user_key => user_id, timestamp => event_time, \
         entry_event => 'signup', return_event => 'purchase', \
         granularity => 'week', periods => 4) FROM page_events"
    ).await.unwrap();
    assert!(!r1.warnings().is_empty());

    // BT 생성
    execute_query(
        "CREATE SESSION MATERIALIZED VIEW page_events_sessions \
         FROM page_events USER KEY user_id SESSION TIMEOUT 60 MINUTE"
    ).await.unwrap();

    // BT 있음 → 라우팅, Warning 없음
    let r2 = execute_query(
        "SELECT COHORT_ANALYSIS(user_key => user_id, timestamp => event_time, \
         entry_event => 'signup', return_event => 'purchase', \
         granularity => 'week', periods => 4) FROM page_events"
    ).await.unwrap();
    assert!(r2.warnings().is_empty(), "No warning after BT created");

    // 결과 동등성
    assert_eq!(r1.cohort_rows(), r2.cohort_rows(),
        "Cohort results must be identical");
}
```

---

### D-003 [TEST][BR] BT 생성 후 자동 라우팅 검증 — PATH

```rust
#[tokio::test]
async fn test_routing_to_bt_on_path_query() {
    setup_event_table_with_path_data();

    // BT 없음 → Warning
    let r1 = execute_query(
        "SELECT PATH_ANALYSIS(user_key => user_id, timestamp => event_time, \
         max_steps => 5, session_timeout => INTERVAL 30 MINUTE, top_n => 10) \
         FROM page_events"
    ).await.unwrap();
    assert!(!r1.warnings().is_empty());

    execute_query(
        "CREATE SESSION MATERIALIZED VIEW page_events_sessions \
         FROM page_events USER KEY user_id SESSION TIMEOUT 30 MINUTE"
    ).await.unwrap();

    let r2 = execute_query(
        "SELECT PATH_ANALYSIS(user_key => user_id, timestamp => event_time, \
         max_steps => 5, session_timeout => INTERVAL 30 MINUTE, top_n => 10) \
         FROM page_events"
    ).await.unwrap();
    assert!(r2.warnings().is_empty());
    assert_eq!(r1.path_rows(), r2.path_rows());
}
```

---

### D-004 [TEST][BR] Event Query는 BT 있어도 라우팅되지 않음

```rust
#[tokio::test]
async fn test_event_query_not_routed_even_with_bt() {
    setup_event_table_with_data();
    // BT 생성
    execute_query(
        "CREATE SESSION MATERIALIZED VIEW page_events_sessions \
         FROM page_events USER KEY user_id SESSION TIMEOUT 30 MINUTE"
    ).await.unwrap();

    // COUNT(*) — Event Query → BT 라우팅 없음
    let explain = execute_query(
        "EXPLAIN SELECT COUNT(*) FROM page_events WHERE event_time >= '2026-04-01'"
    ).await.unwrap();
    let plan = explain.as_text();
    assert!(plan.contains("page_events") && !plan.contains("page_events_sessions"),
        "Event Query must scan Event Table, not BT: {}", plan);
    assert!(!plan.contains("Behavioral Routing"),
        "Behavioral Routing section must not appear for Event Query: {}", plan);

    // GROUP BY event_name — Event Query → BT 라우팅 없음
    let explain2 = execute_query(
        "EXPLAIN SELECT event_name, COUNT(*) FROM page_events GROUP BY event_name"
    ).await.unwrap();
    assert!(!explain2.as_text().contains("Behavioral Routing"));
}
```

**인수 기준**:
- COUNT(*), GROUP BY event_name 쿼리 → `"Behavioral Routing"` 섹션 없음
- EXPLAIN에 `page_events` (Event Table) 사용으로 표시

---

### D-005 [TEST][BR] 일반 테이블 (비-Cube, BT pair 미등록) — 라우팅/Guidance 없음

```rust
#[tokio::test]
async fn test_no_routing_for_plain_table_without_bt_pair() {
    // 일반 테이블 생성 — Cube가 아닌 일반 CREATE TABLE
    // session_id, session_start 등 BT 전용 컬럼명이 있어도 무관
    execute_query(
        "CREATE TABLE user_sessions (
            session_id   VARCHAR(36),
            user_id      VARCHAR(64),
            session_start DATETIME,
            session_end  DATETIME,
            event_count  INT
         )"
    ).await.unwrap();

    execute_query(
        "INSERT INTO user_sessions VALUES \
         ('s1', 'u1', '2026-04-01 10:00:00', '2026-04-01 10:30:00', 5)"
    ).await.unwrap();

    // session_id 참조 쿼리 — BT pair 미등록이므로 라우팅 없음
    let result = execute_query(
        "SELECT COUNT(*) FROM user_sessions WHERE session_start >= '2026-04-01'"
    ).await.unwrap();
    assert!(result.warnings().is_empty(),
        "Plain table without BT pair must not trigger Behavioral Guidance");

    // 일반 집계도 정상 동작
    let result2 = execute_query(
        "SELECT user_id, COUNT(*) FROM user_sessions GROUP BY user_id"
    ).await.unwrap();
    assert!(result2.is_ok());
    assert!(result2.warnings().is_empty());
}
```

**인수 기준**: 일반 테이블은 컬럼 이름에 관계없이 BT pair가 없으면 어떤 경우에도 Routing/Guidance 발동 안함

---

### D-006 [TEST][BR] BT STALE 상태 → Fallback + Guidance

```rust
#[tokio::test]
async fn test_fallback_when_bt_is_stale() {
    setup_event_table_with_data();
    execute_query(
        "CREATE SESSION MATERIALIZED VIEW page_events_sessions \
         FROM page_events USER KEY user_id SESSION TIMEOUT 30 MINUTE"
    ).await.unwrap();

    // BT 상태를 강제로 Stale로 변경 (테스트 유틸)
    set_bt_state("page_events_sessions", BtState::Stale);

    // FUNNEL_COUNT 실행 → Fallback + Guidance (Stale 이유 포함)
    let result = execute_query(
        "SELECT FUNNEL_COUNT(user_key => user_id, timestamp => event_time, \
         window => INTERVAL 24 HOUR, \
         steps => [event_name='page_view', event_name='purchase']) \
         FROM page_events"
    ).await.unwrap();

    assert!(result.is_ok(), "Must not error on Stale BT");
    let warnings = result.warnings();
    assert!(!warnings.is_empty(), "Must warn on Stale BT");
    assert!(warnings[0].contains("Stale") || warnings[0].contains("stale") ||
            warnings[0].contains("No Behavioral Table"),
        "Warning must mention stale state: {}", warnings[0]);
}
```

---

### D-007 [TEST][BR] BT pair 등록 순서 검증 (CREATE SESSION MV 후 즉시 라우팅 가능)

```rust
#[tokio::test]
async fn test_bt_registered_immediately_after_create() {
    setup_event_table_with_data();

    // CREATE 직후 바로 다음 쿼리에서 라우팅되어야 함
    execute_query(
        "CREATE SESSION MATERIALIZED VIEW page_events_sessions \
         FROM page_events USER KEY user_id SESSION TIMEOUT 30 MINUTE"
    ).await.unwrap();

    // 즉시 EXPLAIN → BT 사용 확인 (지연 없음)
    let explain = execute_query(
        "EXPLAIN SELECT FUNNEL_COUNT(user_key => user_id, timestamp => event_time, \
         window => INTERVAL 24 HOUR, \
         steps => [event_name='page_view', event_name='purchase']) \
         FROM page_events"
    ).await.unwrap();

    assert!(explain.as_text().contains("page_events_sessions"),
        "BT must be available for routing immediately after CREATE");
}
```

---

### D-008 [TEST][BG] Guidance 메시지 품질 검증

```rust
#[tokio::test]
async fn test_guidance_message_quality() {
    setup_event_table_with_data(); // 50,000행 삽입

    let result = execute_query(
        "SELECT FUNNEL_COUNT(user_key => user_id, timestamp => event_time, \
         window => INTERVAL 24 HOUR, \
         steps => [event_name='page_view', event_name='purchase']) \
         FROM page_events"
    ).await.unwrap();

    let warnings = result.warnings();
    assert!(!warnings.is_empty());
    let w = &warnings[0];

    // 필수 포함 요소
    assert!(w.contains("page_events"), "Must mention table name");
    assert!(w.contains("CREATE SESSION MATERIALIZED VIEW"),
        "Must include DDL keyword");
    assert!(w.contains("page_events"), "DDL must reference source table");
    assert!(w.contains("USER KEY"), "DDL must include USER KEY clause");
    assert!(w.contains("SESSION TIMEOUT"), "DDL must include SESSION TIMEOUT");
    // 성능 힌트 (배수 또는 ms 언급)
    assert!(w.contains("ms") || w.contains("faster") || w.contains("x"),
        "Must include performance hint: {}", w);
}
```

---

## Phase E: 성능 검증 (선택적)

**목적**: BT 라우팅이 실제로 성능 향상을 가져오는지 측정

### E-001 [TEST][BR] BT 라우팅 성능 비교

**파일**: `query-node/src/mysql_protocol/behavioral_routing_tests.rs`

```rust
#[tokio::test]
async fn test_bt_routing_performance_improvement() {
    // 100,000행 삽입 (analytics_large_dataset_tests.rs 의 데이터 재사용)
    setup_large_event_dataset(); // TABLE: _routing_perf_events, 100k rows

    // Without BT: Event Table에서 FUNNEL_COUNT
    let t1_start = Instant::now();
    let r1 = execute_query(
        "SELECT FUNNEL_COUNT(user_key => user_id, timestamp => event_time, \
         window => INTERVAL 24 HOUR, \
         steps => [event_name='page_view', event_name='add_to_cart', \
                   event_name='purchase']) \
         FROM _routing_perf_events"
    ).await.unwrap();
    let t1_ms = t1_start.elapsed().as_millis();

    // Create BT
    execute_query(
        "CREATE SESSION MATERIALIZED VIEW _routing_perf_events_sessions \
         FROM _routing_perf_events USER KEY user_id SESSION TIMEOUT 30 MINUTE"
    ).await.unwrap();

    // With BT: 동일 쿼리 (자동 라우팅)
    let t2_start = Instant::now();
    let r2 = execute_query(
        "SELECT FUNNEL_COUNT(user_key => user_id, timestamp => event_time, \
         window => INTERVAL 24 HOUR, \
         steps => [event_name='page_view', event_name='add_to_cart', \
                   event_name='purchase']) \
         FROM _routing_perf_events"
    ).await.unwrap();
    let t2_ms = t2_start.elapsed().as_millis();

    // 결과 동등성
    assert_eq!(r1.funnel_counts(), r2.funnel_counts(),
        "Results must be identical");

    // 성능 향상 확인 (메모리 DB이므로 2× 이상 기대)
    // 실제 LSM+Columnar 환경에서는 4–10× 기대
    println!("Without BT: {}ms, With BT: {}ms, speedup: {:.1}x",
             t1_ms, t2_ms, t1_ms as f64 / t2_ms as f64);
    assert!(t2_ms <= t1_ms,
        "Routing to BT must not be slower than Event Table scan");
}
```

---

## 태스크 요약 및 실행 순서

```
Phase A (기반) — 병렬 가능
  A-001 [P] Query Pattern Detector
  A-002 [P] BT Registry
  A-003    BT Registry ↔ CubeManager 통합  (A-001, A-002 완료 후)

Phase B (Guidance) — A-003 완료 후
  B-001 [P] Guidance Generator
  B-002    MySQL 응답에 Guidance 첨부 (B-001 완료 후)
  B-003 [TEST] Guidance 검증 테스트 (B-002 완료 후)

Phase C (Router) — A-003 완료 후, B와 병렬 가능
  C-001 [P] LogicalPlan Rewriter
  C-002 [P] Column Mapping
  C-003    Router ↔ handler.rs 통합 (C-001, C-002 완료 후)
  C-004    EXPLAIN 출력 업데이트 (C-003 완료 후)

Phase D (통합 검증) — B-002, C-003 모두 완료 후
  D-001 ~ D-008 [TEST] 순차 실행 (독립적이므로 병렬 가능)

Phase E (성능) — D 완료 후 선택 실행
  E-001 [TEST] 성능 비교
```

---

## 완료 기준 (Definition of Done)

| 항목 | 기준 |
|---|---|
| **Behavioral Guidance** | BT 없는 상태에서 FUNNEL/COHORT/PATH 쿼리 실행 시 warnings에 DDL 제안 포함 |
| **Event Query 비간섭** | COUNT(*), GROUP BY 등 Event Query는 BT 여부에 관계없이 Warning 없음 |
| **일반 테이블 비간섭** | CREATE TABLE로 만든 테이블은 BT pair 미등록이면 Routing/Guidance 대상 아님 |
| **Behavioral Routing** | BT 생성 후 Behavioral Query는 쿼리 수정 없이 자동으로 BT 사용 |
| **결과 동등성** | BT 사용 전후 Funnel/Cohort/Path 결과 동일 |
| **EXPLAIN 표시** | `EXPLAIN` 출력에 라우팅 여부, BT 이름, 이유 표시 |
| **즉시 활성화** | `CREATE SESSION MATERIALIZED VIEW` 완료 직후 다음 쿼리부터 라우팅 |
| **STALE Fallback** | BT STALE 상태 시 오류 없이 Event Table Fallback + Warning |
| **테스트 통과** | D-001 ~ D-008 모두 PASS |
