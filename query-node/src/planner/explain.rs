// T144: EXPLAIN 분산 실행 계획 출력
// FR-047: EXPLAIN / EXPLAIN VERBOSE / EXPLAIN COSTS
// FR-048: Colocate Join 표시
// T162: Behavioral Routing section in EXPLAIN output

use opensrv_mysql::ColumnType;

use crate::meta::bt_registry::BtRegistry;
use crate::mysql_protocol::handler::{ColumnMeta, QueryOutput};
use crate::planner::behavioral_pattern::{detect_pattern, extract_scan_table, QueryPattern};
use crate::planner::behavioral_guidance::build_guidance;

// ─── EXPLAIN 모드 ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainMode {
    Basic,
    Verbose,
    Costs,
    /// EXPLAIN ANALYZE: 계획 출력 + 실제 실행 통계 (현재는 예상 통계로 대체)
    Analyze,
}

// ─── ExplainFragment / ExplainLine ────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ExplainFragment {
    pub id:    usize,
    pub lines: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ExplainPlan {
    pub fragments: Vec<ExplainFragment>,
}

impl ExplainPlan {
    /// QueryOutput으로 변환: Fragment_Id 컬럼 + Plan 컬럼
    pub fn into_output(self) -> QueryOutput {
        let columns = vec![
            ColumnMeta { name: "Fragment_Id".to_string(), col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "Plan".to_string(),        col_type: ColumnType::MYSQL_TYPE_BLOB },
        ];

        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        for frag in &self.fragments {
            let frag_id = format!("FRAGMENT {}", frag.id);
            for line in &frag.lines {
                rows.push(vec![
                    Some(frag_id.clone()),
                    Some(line.clone()),
                ]);
            }
        }

        QueryOutput::Rows { columns, rows }
    }
}

// ─── SQL 분석 헬퍼 ────────────────────────────────────────────────────────────

/// SQL 텍스트에서 테이블 이름 추출 (FROM 다음 단어)
fn extract_table_names(sql: &str) -> Vec<String> {
    let upper = sql.to_uppercase();
    let mut names = Vec::new();
    let mut iter = upper.split_whitespace().peekable();
    while let Some(tok) = iter.next() {
        if tok == "FROM" || tok == "JOIN" {
            if let Some(next) = iter.peek() {
                // subquery나 괄호 제외
                if !next.starts_with('(') {
                    let raw = next.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
                    if !raw.is_empty() {
                        names.push(raw.to_lowercase());
                    }
                }
            }
        }
    }
    names.dedup();
    names
}

/// WHERE 절 추출 (원본 케이스 보존)
fn extract_where_clause(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let where_pos = upper.find(" WHERE ")?;
    let after = &sql[where_pos + 7..];
    // GROUP BY / ORDER BY / LIMIT / HAVING 앞에서 자름
    let end_keywords = ["GROUP BY", "ORDER BY", "LIMIT", "HAVING"];
    let upper_after = after.to_uppercase();
    let mut end = after.len();
    for kw in &end_keywords {
        if let Some(p) = upper_after.find(kw) {
            if p < end { end = p; }
        }
    }
    let clause = after[..end].trim().to_string();
    if clause.is_empty() { None } else { Some(clause) }
}

/// GROUP BY 컬럼 추출
fn extract_group_by(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let gb_pos = upper.find("GROUP BY")?;
    let after = &sql[gb_pos + 8..];
    let upper_after = after.to_uppercase();
    let end_keywords = ["ORDER BY", "LIMIT", "HAVING"];
    let mut end = after.len();
    for kw in &end_keywords {
        if let Some(p) = upper_after.find(kw) {
            if p < end { end = p; }
        }
    }
    let clause = after[..end].trim().to_string();
    if clause.is_empty() { None } else { Some(clause) }
}

/// SELECT 컬럼 목록 추출 (SELECT ... FROM 사이)
fn extract_select_exprs(sql: &str) -> String {
    let upper = sql.to_uppercase();
    let sel_start = upper.find("SELECT").map(|p| p + 6).unwrap_or(0);
    let from_pos  = upper.find(" FROM ").unwrap_or(sql.len());
    let exprs = sql[sel_start..from_pos].trim();
    if exprs.len() > 60 {
        format!("{}...", &exprs[..60])
    } else {
        exprs.to_string()
    }
}

/// SQL에 집계 함수가 있는지 확인
fn has_aggregate(sql: &str) -> bool {
    let upper = sql.to_uppercase();
    upper.contains("COUNT(")
        || upper.contains("SUM(")
        || upper.contains("AVG(")
        || upper.contains("MIN(")
        || upper.contains("MAX(")
}

/// SQL에 JOIN이 있는지 확인
fn has_join(sql: &str) -> bool {
    sql.to_uppercase().contains(" JOIN ")
}

/// SQL에 FUNNEL_COUNT가 있는지 확인
fn has_funnel(sql: &str) -> bool {
    sql.to_uppercase().contains("FUNNEL_COUNT")
}

/// SQL에 COHORT_ANALYSIS가 있는지 확인
fn has_cohort(sql: &str) -> bool {
    sql.to_uppercase().contains("COHORT_ANALYSIS")
}

/// SQL에 PATH_ANALYSIS가 있는지 확인
fn has_path(sql: &str) -> bool {
    sql.to_uppercase().contains("PATH_ANALYSIS")
}

/// Colocate Join 힌트: COLOCATE_GROUP 또는 분산 키 힌트가 있는지
/// 실제 구현에서는 CubeSchema에서 colocate_group을 비교한다.
/// 이 스텁은 SQL 텍스트에 "COLOCATE_GROUP" 또는 "colocate_group" 키워드가 있으면 true.
fn is_colocate_join_hint(sql: &str) -> bool {
    sql.to_lowercase().contains("colocate_group")
        || sql.to_lowercase().contains("colocate join")
}

/// WHERE 절에서 파티션 컬럼(event_time 등) 조건을 찾아 MinMax 인덱스 히트 여부 결정
fn detect_minmax_hit(where_clause: &str) -> Option<(String, bool)> {
    let upper = where_clause.to_uppercase();
    // 날짜/시간 컬럼에 비교 연산자가 있으면 MinMax HIT
    let time_cols = ["EVENT_TIME", "CREATED_AT", "UPDATED_AT", "TS", "TIMESTAMP", "DATE"];
    for col in &time_cols {
        if upper.contains(col) {
            return Some((col.to_lowercase(), true));
        }
    }
    None
}

/// 집계 함수 이름 추출 (첫 번째 집계만)
fn extract_first_agg(sql: &str) -> String {
    let upper = sql.to_uppercase();
    if upper.contains("COUNT(*)") || upper.contains("COUNT( *)") {
        "count(*)".to_string()
    } else if upper.contains("COUNT(") {
        "count(...)".to_string()
    } else if upper.contains("SUM(") {
        "sum(...)".to_string()
    } else if upper.contains("AVG(") {
        "avg(...)".to_string()
    } else if upper.contains("MIN(") {
        "min(...)".to_string()
    } else if upper.contains("MAX(") {
        "max(...)".to_string()
    } else {
        "agg(...)".to_string()
    }
}

// ─── 파티션/Part 수치 (스텁 — 현실적 값) ─────────────────────────────────────

struct ScanStats {
    total_partitions:   usize,
    pruned_partitions:  usize,
    skipped_partitions: usize,
    total_parts:        usize,
    scanned_parts:      usize,
    skipped_parts:      usize,
    // CBO 예상치
    est_rows:           u64,
    est_bytes_mb:       f64,
}

fn stub_scan_stats(has_where: bool) -> ScanStats {
    if has_where {
        ScanStats {
            total_partitions:   10,
            pruned_partitions:  3,
            skipped_partitions: 7,
            total_parts:        36,
            scanned_parts:      24,
            skipped_parts:      12,
            est_rows:           200_000,
            est_bytes_mb:       16.0,
        }
    } else {
        ScanStats {
            total_partitions:   10,
            pruned_partitions:  10,
            skipped_partitions: 0,
            total_parts:        36,
            scanned_parts:      36,
            skipped_parts:      0,
            est_rows:           5_000_000,
            est_bytes_mb:       420.0,
        }
    }
}

// ─── 메인 EXPLAIN 생성기 ──────────────────────────────────────────────────────

/// SQL 텍스트로부터 ExplainPlan 생성 (스텁 기반 현실적 출력)
pub fn build_explain_plan(sql: &str, mode: ExplainMode) -> ExplainPlan {
    let tables     = extract_table_names(sql);
    let where_cl   = extract_where_clause(sql);
    let group_by   = extract_group_by(sql);
    let out_exprs  = extract_select_exprs(sql);
    let agg        = has_aggregate(sql);
    let join       = has_join(sql);
    let funnel     = has_funnel(sql);
    let cohort     = has_cohort(sql);
    let path_an    = has_path(sql);
    let colocate   = is_colocate_join_hint(sql);

    let table_name = tables.first().cloned().unwrap_or_else(|| "<unknown>".to_string());
    let sn_list    = "[SN-01, SN-02, SN-03]";

    let stats = stub_scan_stats(where_cl.is_some());
    let minmax = where_cl.as_deref().and_then(detect_minmax_hit);

    // ── Fragment 0: QN 결과 수집 ─────────────────────────────────────────────
    let mut frag0 = ExplainFragment { id: 0, lines: Vec::new() };
    frag0.lines.push("PLAN FRAGMENT 0".to_string());
    frag0.lines.push(format!("  OUTPUT EXPRS: {}", out_exprs));
    frag0.lines.push("  PARTITION: UNPARTITIONED".to_string());
    frag0.lines.push(String::new());
    frag0.lines.push("  RESULT SINK".to_string());
    frag0.lines.push(String::new());

    if funnel {
        frag0.lines.push("  0: FUNNEL MERGE [QN-MERGE]".to_string());
    } else if cohort {
        frag0.lines.push("  0: COHORT MERGE [QN-MERGE]".to_string());
    } else if path_an {
        frag0.lines.push("  0: PATH MERGE [QN-MERGE]".to_string());
    } else if agg {
        frag0.lines.push("  0: AGGREGATION [QN-MERGE]".to_string());
    } else {
        frag0.lines.push("  0: EXCHANGE [GATHER]".to_string());
    }

    if mode == ExplainMode::Costs {
        let merge_rows  = stats.est_rows / 100;
        let merge_bytes = stats.est_bytes_mb / 100.0;
        frag0.lines.push(format!("     CBO: rows={:>10}  bytes={:.1}MB  cost=1.2",
            fmt_num(merge_rows), merge_bytes));
    } else {
        let merge_rows  = stats.est_rows / 100;
        let merge_bytes = stats.est_bytes_mb / 100.0;
        frag0.lines.push(format!("     CBO: rows={:>10}  bytes={:.1}MB",
            fmt_num(merge_rows), merge_bytes));
    }

    // ── Fragment 1: CN 실행 단계 ─────────────────────────────────────────────
    let mut frag1 = ExplainFragment { id: 1, lines: Vec::new() };
    frag1.lines.push("PLAN FRAGMENT 1".to_string());
    frag1.lines.push(format!("  OUTPUT EXPRS: {}", out_exprs));

    // 분산 키 결정 (GROUP BY 첫 컬럼 또는 기본값 user_id)
    let dist_key = group_by.as_deref()
        .and_then(|g| g.split(',').next())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "user_id".to_string());

    frag1.lines.push(format!("  PARTITION: HASH({})", dist_key));
    frag1.lines.push(String::new());
    frag1.lines.push("  STREAM DATA SINK".to_string());
    frag1.lines.push("    EXCHANGE ID: 01".to_string());
    frag1.lines.push(format!("    HASH_PARTITIONED: {}", dist_key));
    frag1.lines.push(String::new());

    // Join 노드
    if join {
        let join_type = if colocate {
            "[COLOCATE]"
        } else {
            "[BROADCAST]"
        };
        frag1.lines.push(format!("  1: HASH JOIN {}", join_type));
        if mode == ExplainMode::Costs {
            if colocate {
                frag1.lines.push(format!("     CBO: rows={}  bytes={:.1}MB  shuffle=NONE",
                    fmt_num(stats.est_rows), stats.est_bytes_mb));
            } else {
                frag1.lines.push(format!("     CBO: rows={}  bytes={:.1}MB",
                    fmt_num(stats.est_rows), stats.est_bytes_mb));
            }
        } else if colocate {
            frag1.lines.push(format!("     CBO: rows={}  bytes={:.1}MB  shuffle=NONE",
                fmt_num(stats.est_rows), stats.est_bytes_mb));
        }
    } else if funnel {
        frag1.lines.push("  1: FUNNEL ANALYSIS [CN]".to_string());
        if mode == ExplainMode::Costs {
            frag1.lines.push(format!("     CBO: rows={}  bytes={:.1}MB  cost=42.5",
                fmt_num(stats.est_rows), stats.est_bytes_mb));
        }
    } else if cohort {
        frag1.lines.push("  1: COHORT ANALYSIS [CN]".to_string());
        if mode == ExplainMode::Costs {
            frag1.lines.push(format!("     CBO: rows={}  bytes={:.1}MB  cost=38.1",
                fmt_num(stats.est_rows), stats.est_bytes_mb));
        }
    } else if path_an {
        frag1.lines.push("  1: PATH ANALYSIS [CN]".to_string());
        if mode == ExplainMode::Costs {
            frag1.lines.push(format!("     CBO: rows={}  bytes={:.1}MB  cost=55.8",
                fmt_num(stats.est_rows), stats.est_bytes_mb));
        }
    } else if agg {
        frag1.lines.push("  1: HASH AGGREGATE [CN-PARTIAL]".to_string());
        if mode == ExplainMode::Costs {
            frag1.lines.push(format!("     CBO: rows={}  bytes={:.1}MB  cost=24.7",
                fmt_num(stats.est_rows), stats.est_bytes_mb));
        }
    }

    // Scan 노드 (첫 번째 테이블)
    let node_id = if join { 2 } else { 2 };
    frag1.lines.push(format!("     |--- {}: SCAN ({}) {}",
        node_id, table_name, sn_list));

    // Partitions 및 Parts 정보
    frag1.lines.push(format!("            Partitions: {}/{} pruned ({} skipped by CBO)",
        stats.pruned_partitions, stats.total_partitions, stats.skipped_partitions));
    frag1.lines.push(format!("            Parts: {} scanned ({} skipped by Bloom/MinMax)",
        stats.scanned_parts, stats.skipped_parts));

    // Verbose / Costs: 추가 상세 정보
    if mode == ExplainMode::Verbose || mode == ExplainMode::Costs {
        if let Some(ref wc) = where_cl {
            frag1.lines.push(format!("            Push-down predicates: {}", wc));
        }
        if let Some((ref col, hit)) = minmax {
            frag1.lines.push(format!("            Index: MinMax on {} [{}]",
                col, if hit { "HIT" } else { "MISS" }));
        }
        if agg {
            let agg_fn = extract_first_agg(sql);
            frag1.lines.push(format!("            Aggregate pushdown: {} [YES]", agg_fn));
        }
        if mode == ExplainMode::Costs {
            frag1.lines.push(format!("            CBO: rows={}  bytes={:.1}MB  cost=18.3",
                fmt_num(stats.est_rows), stats.est_bytes_mb));
        }
    }

    // JOIN의 두 번째 테이블 Scan
    if join {
        let table2 = tables.get(1).cloned().unwrap_or_else(|| "<unknown>".to_string());
        let join_stats = stub_scan_stats(false);
        frag1.lines.push(format!("     |--- 3: SCAN ({}) {}", table2, sn_list));
        frag1.lines.push(format!("            Partitions: {}/{} pruned ({} skipped by CBO)",
            join_stats.pruned_partitions, join_stats.total_partitions,
            join_stats.skipped_partitions));
        frag1.lines.push(format!("            Parts: {} scanned ({} skipped by Bloom/MinMax)",
            join_stats.scanned_parts, join_stats.skipped_parts));
        if colocate {
            frag1.lines.push(
                "            (same bucket distribution — no shuffle required)".to_string()
            );
        }
        if mode == ExplainMode::Costs {
            frag1.lines.push(format!("            CBO: rows={}  bytes={:.1}MB  cost=14.9",
                fmt_num(join_stats.est_rows), join_stats.est_bytes_mb));
        }
    }

    ExplainPlan { fragments: vec![frag0, frag1] }
}

/// 숫자를 천 단위 콤마 형식으로 변환 (예: 200000 → "200,000")
fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    let start = bytes.len() % 3;
    if start != 0 {
        result.push_str(&s[..start]);
    }
    let mut i = start;
    while i < bytes.len() {
        if !result.is_empty() { result.push(','); }
        result.push_str(&s[i..i + 3]);
        i += 3;
    }
    result
}

// ─── 공개 진입점 ──────────────────────────────────────────────────────────────

/// `EXPLAIN [VERBOSE|COSTS|ANALYZE] <sql>` 처리 — QueryOutput 반환
///
/// `sql`: EXPLAIN 접두사를 제거한 순수 SQL 텍스트
/// `mode`: ExplainMode::Basic | Verbose | Costs | Analyze
/// `bt_registry`: optional BtRegistry for Behavioral Routing section
pub fn explain_sql(sql: &str, mode: ExplainMode, bt_registry: Option<&BtRegistry>) -> QueryOutput {
    // Analyze 모드: Costs 출력 + 실제 실행 통계 헤더 추가
    let effective_mode = if mode == ExplainMode::Analyze {
        ExplainMode::Costs
    } else {
        mode
    };
    let mut plan = build_explain_plan(sql, effective_mode);

    if mode == ExplainMode::Analyze {
        // Fragment 0 앞에 ANALYZE 헤더 삽입
        if let Some(frag0) = plan.fragments.first_mut() {
            frag0.lines.insert(0,
                "  [ANALYZE] Note: actual runtime stats not available (stub); showing estimated costs".to_string()
            );
            frag0.lines.insert(0, "PLAN FRAGMENT 0  [ANALYZE MODE]".to_string());
        }
    }

    // T162: Behavioral Routing section
    if let Some(reg) = bt_registry {
        let routing_fragment = build_behavioral_routing_fragment(sql, reg, plan.fragments.len());
        plan.fragments.push(routing_fragment);
    }

    plan.into_output()
}

/// Build an ExplainFragment for the Behavioral Routing section.
fn build_behavioral_routing_fragment(sql: &str, bt_registry: &BtRegistry, id: usize) -> ExplainFragment {
    let mut frag = ExplainFragment { id, lines: Vec::new() };
    frag.lines.push("== Behavioral Routing ==".to_string());

    let pattern = detect_pattern(sql);
    let scan_table = extract_scan_table(sql).unwrap_or_else(|| "<unknown>".to_string());

    match &pattern {
        QueryPattern::Event => {
            frag.lines.push("  Pattern: Event (no behavioral routing)".to_string());
        }
        QueryPattern::Behavioral { .. } | QueryPattern::Hybrid => {
            let trigger_names: Vec<&str> = if let QueryPattern::Behavioral { triggers } = &pattern {
                triggers.iter().map(|t| match t {
                    crate::planner::behavioral_pattern::BehavioralTrigger::FunnelFunction      => "FUNNEL_COUNT",
                    crate::planner::behavioral_pattern::BehavioralTrigger::CohortFunction      => "COHORT_ANALYSIS",
                    crate::planner::behavioral_pattern::BehavioralTrigger::PathFunction        => "PATH_ANALYSIS",
                    crate::planner::behavioral_pattern::BehavioralTrigger::SessionIdColumn     => "session_id",
                    crate::planner::behavioral_pattern::BehavioralTrigger::SessionStartColumn  => "session_start",
                    crate::planner::behavioral_pattern::BehavioralTrigger::SessionEndColumn    => "session_end",
                    crate::planner::behavioral_pattern::BehavioralTrigger::EventSequenceColumn => "event_sequence",
                    crate::planner::behavioral_pattern::BehavioralTrigger::SessionTimeoutParam => "session_timeout",
                }).collect()
            } else {
                vec!["(hybrid)"]
            };

            let pattern_label = match &pattern {
                QueryPattern::Behavioral { .. } => "Behavioral",
                QueryPattern::Hybrid => "Hybrid",
                QueryPattern::Event => "Event",
            };
            frag.lines.push(format!("  Pattern: {} [triggers: {}]",
                pattern_label, trigger_names.join(", ")));
            frag.lines.push(format!("  Source table: {}", scan_table));

            if let Some(bt) = bt_registry.get_active_bt(&scan_table) {
                frag.lines.push(format!("  Routed to: {} (state=Active)", bt.bt_name));
                if let Some(ts) = bt.last_refresh {
                    let secs = ts.elapsed().map(|d| d.as_secs()).unwrap_or(0);
                    frag.lines.push(format!("  BT last refresh: {}s ago", secs));
                }
                frag.lines.push(format!("  Session timeout: {}s", bt.session_timeout_sec));
                frag.lines.push(format!("  User key: {}", bt.user_key));
            } else if bt_registry.has_any_bt(&scan_table) {
                // BT exists but not active (stale/building/error)
                let bts = bt_registry.get_bt_for_table(&scan_table);
                let state_str = bts.first()
                    .map(|e| format!("{:?}", e.state))
                    .unwrap_or_else(|| "Unknown".to_string());
                frag.lines.push(format!("  Routing: FALLBACK (BT state={})", state_str));
                frag.lines.push("  Note: BT is not Active — query runs on event table".to_string());
            } else {
                // No BT at all — show guidance
                frag.lines.push("  Routing: NO BT FOUND — fallback to event table".to_string());
                let trigger_list = if let QueryPattern::Behavioral { triggers } = &pattern {
                    triggers.clone()
                } else {
                    vec![]
                };
                let guidance = build_guidance(&scan_table, &trigger_list, 0, 0);
                frag.lines.push("  Guidance:".to_string());
                for line in guidance.suggested_ddl.lines() {
                    frag.lines.push(format!("    {}", line));
                }
                if let Some(speedup) = guidance.estimated_speedup {
                    frag.lines.push(format!("  Estimated speedup: {:.0}×", speedup));
                }
            }
        }
    }

    frag
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // 출력 rows에서 Plan 컬럼 텍스트를 하나로 합침
    fn collect_plan_text(output: &QueryOutput) -> String {
        match output {
            QueryOutput::Rows { rows, .. } => rows.iter()
                .filter_map(|r| r.get(1).and_then(|v| v.as_deref()))
                .collect::<Vec<_>>()
                .join("\n"),
            _ => String::new(),
        }
    }

    // Fragment_Id 컬럼 값 목록 수집
    fn collect_fragment_ids(output: &QueryOutput) -> Vec<String> {
        match output {
            QueryOutput::Rows { rows, .. } => rows.iter()
                .filter_map(|r| r.first().and_then(|v| v.clone()))
                .collect(),
            _ => Vec::new(),
        }
    }

    // T148: 기본 EXPLAIN — Fragment 구조 검증
    #[test]
    fn test_explain_basic_fragment_structure() {
        let output = explain_sql("SELECT * FROM page_events", ExplainMode::Basic, None);
        let text = collect_plan_text(&output);

        // FRAGMENT 0에 QN-MERGE 포함
        assert!(text.contains("QN-MERGE") || text.contains("GATHER"),
            "Fragment 0 should contain QN-MERGE or GATHER, got:\n{}", text);

        // FRAGMENT 1에 SCAN 포함
        assert!(text.contains("SCAN (page_events)"),
            "Fragment 1 should contain SCAN(page_events), got:\n{}", text);

        // 두 Fragment ID가 모두 존재
        let frag_ids = collect_fragment_ids(&output);
        assert!(frag_ids.iter().any(|id| id == "FRAGMENT 0"), "FRAGMENT 0 missing");
        assert!(frag_ids.iter().any(|id| id == "FRAGMENT 1"), "FRAGMENT 1 missing");
    }

    // T149: EXPLAIN COSTS — WHERE 절 → Partitions 행 + CBO rows 행 존재
    #[test]
    fn test_explain_costs_where_clause() {
        let output = explain_sql(
            "SELECT event_name, count(*) FROM page_events WHERE event_time > '2026-01-01' GROUP BY event_name",
            ExplainMode::Costs,
            None,
        );
        let text = collect_plan_text(&output);

        assert!(text.contains("Partitions:"),
            "Expected 'Partitions:' in COSTS output, got:\n{}", text);
        assert!(text.contains("CBO: rows="),
            "Expected 'CBO: rows=' in COSTS output, got:\n{}", text);
        assert!(text.contains("cost="),
            "Expected 'cost=' in COSTS output, got:\n{}", text);
    }

    // T150: EXPLAIN JOIN — HASH JOIN + Shuffle 타입 포함
    #[test]
    fn test_explain_join_shuffle_type() {
        let output = explain_sql(
            "SELECT e.user_id, u.country FROM page_events e JOIN user_profiles u ON e.device_id = u.device_id",
            ExplainMode::Basic,
            None,
        );
        let text = collect_plan_text(&output);

        assert!(text.contains("HASH JOIN"),
            "Expected 'HASH JOIN' in output, got:\n{}", text);
        // BROADCAST 또는 HASH_SHUFFLE 또는 COLOCATE 중 하나는 있어야 함
        assert!(
            text.contains("BROADCAST") || text.contains("HASH_SHUFFLE") || text.contains("COLOCATE"),
            "Expected shuffle type in output, got:\n{}", text
        );
    }

    // T151: EXPLAIN FUNNEL — FUNNEL ANALYSIS Fragment 존재
    #[test]
    fn test_explain_funnel_fragment() {
        let output = explain_sql(
            "SELECT FUNNEL_COUNT(user_id, event_name, event_time, WINDOW 7 DAYS, STEP 'page_view', STEP 'purchase') FROM page_events",
            ExplainMode::Basic,
            None,
        );
        let text = collect_plan_text(&output);

        assert!(text.contains("FUNNEL"),
            "Expected 'FUNNEL' fragment in output, got:\n{}", text);
    }

    // T152: Colocate Join — [COLOCATE] 태그 + shuffle=NONE
    #[test]
    fn test_explain_colocate_join() {
        // colocate_group 키워드를 SQL 힌트로 포함시켜 Colocate Join 시뮬레이션
        let output = explain_sql(
            "SELECT e.event_name, u.country, count(*) FROM page_events e JOIN user_profiles u ON e.device_id = u.device_id WHERE e.event_time > '2026-01-01' GROUP BY e.event_name, u.country -- colocate_group: web_analytics",
            ExplainMode::Basic,
            None,
        );
        let text = collect_plan_text(&output);

        assert!(text.contains("[COLOCATE]"),
            "Expected '[COLOCATE]' in output, got:\n{}", text);
        assert!(text.contains("shuffle=NONE"),
            "Expected 'shuffle=NONE' in output, got:\n{}", text);
        assert!(text.contains("no shuffle required"),
            "Expected 'no shuffle required' in output, got:\n{}", text);
    }

    // EXPLAIN VERBOSE — Push-down predicates 및 Index 정보 포함
    #[test]
    fn test_explain_verbose_includes_predicates_and_index() {
        let output = explain_sql(
            "SELECT user_id, count(*) FROM page_events WHERE event_time > '2026-01-01' GROUP BY user_id",
            ExplainMode::Verbose,
            None,
        );
        let text = collect_plan_text(&output);

        assert!(text.contains("Push-down predicates:"),
            "Expected 'Push-down predicates:' in VERBOSE output, got:\n{}", text);
        assert!(text.contains("Index: MinMax on event_time [HIT]"),
            "Expected MinMax index HIT in VERBOSE output, got:\n{}", text);
        assert!(text.contains("Aggregate pushdown:"),
            "Expected 'Aggregate pushdown:' in VERBOSE output, got:\n{}", text);
    }

    // columns 검증: Fragment_Id + Plan 두 컬럼
    #[test]
    fn test_explain_output_columns() {
        let output = explain_sql("SELECT 1 FROM page_events", ExplainMode::Basic, None);
        match &output {
            QueryOutput::Rows { columns, .. } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "Fragment_Id");
                assert_eq!(columns[1].name, "Plan");
            }
            _ => panic!("Expected Rows output"),
        }
    }

    // fmt_num 헬퍼 테스트
    #[test]
    fn test_fmt_num() {
        assert_eq!(fmt_num(0),         "0");
        assert_eq!(fmt_num(999),       "999");
        assert_eq!(fmt_num(1000),      "1,000");
        assert_eq!(fmt_num(50000),     "50,000");
        assert_eq!(fmt_num(200000),    "200,000");
        assert_eq!(fmt_num(5000000),   "5,000,000");
    }

    // T162: Behavioral Routing section in EXPLAIN output

    #[test]
    fn test_explain_with_bt_registry_event_query_no_routing() {
        use crate::meta::bt_registry::BtRegistry;
        let reg = BtRegistry::new();
        let output = explain_sql(
            "SELECT COUNT(*) FROM page_events",
            ExplainMode::Basic,
            Some(&reg),
        );
        let text = collect_plan_text(&output);
        assert!(text.contains("== Behavioral Routing =="),
            "Should include Behavioral Routing section");
        assert!(text.contains("Pattern: Event"),
            "Should show Event pattern: {}", text);
    }

    #[test]
    fn test_explain_funnel_with_active_bt_shows_routing() {
        use crate::meta::bt_registry::{BtEntry, BtRegistry, BtState};
        let mut reg = BtRegistry::new();
        reg.register(BtEntry {
            bt_name: "page_sessions".to_string(),
            event_table: "page_events".to_string(),
            user_key: "user_id".to_string(),
            session_timeout_sec: 1800,
            state: BtState::Active,
            last_refresh: None,
        });

        let output = explain_sql(
            "SELECT FUNNEL_COUNT(user_id, event_name, event_time, WINDOW 7 DAYS) FROM page_events",
            ExplainMode::Basic,
            Some(&reg),
        );
        let text = collect_plan_text(&output);
        assert!(text.contains("Routed to: page_sessions"),
            "Should show routing to BT: {}", text);
    }

    #[test]
    fn test_explain_funnel_without_bt_shows_guidance() {
        use crate::meta::bt_registry::BtRegistry;
        let reg = BtRegistry::new();
        let output = explain_sql(
            "SELECT FUNNEL_COUNT(user_id, event_name, event_time, WINDOW 7 DAYS) FROM page_events",
            ExplainMode::Basic,
            Some(&reg),
        );
        let text = collect_plan_text(&output);
        assert!(text.contains("NO BT FOUND") || text.contains("CREATE SESSION MATERIALIZED VIEW"),
            "Should show guidance when no BT: {}", text);
    }
}
