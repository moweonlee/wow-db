// T054 stub: FUNNEL_COUNT, COHORT_ANALYSIS, PATH_ANALYSIS 커스텀 AST
// 완전 구현은 Phase C (T054)

use sqlparser::ast::Expr;

// ─── 분석 함수 AST ────────────────────────────────────────────────────────────

/// FUNNEL_COUNT(user_key, window_sec, step1_cond, step2_cond, ...)
#[derive(Debug, Clone)]
pub struct FunnelCountExpr {
    pub user_key:   String,
    pub window_sec: u64,
    pub steps:      Vec<FunnelStep>,
}

#[derive(Debug, Clone)]
pub struct FunnelStep {
    pub name:      String,
    pub condition: String, // 원시 SQL 조건 문자열 (Phase C에서 Expr로 변환)
}

/// COHORT_ANALYSIS(user_key, entry_event, return_event, window_days)
#[derive(Debug, Clone)]
pub struct CohortAnalysisExpr {
    pub user_key:    String,
    pub entry_event: String,
    pub return_event: String,
    pub window_days: u32,
}

/// PATH_ANALYSIS(user_key, max_depth, top_n)
#[derive(Debug, Clone)]
pub struct PathAnalysisExpr {
    pub user_key:  String,
    pub max_depth: u32,
    pub top_n:     u32,
}

// ─── 분석 함수 감지 ───────────────────────────────────────────────────────────

/// SELECT 문에 WOW-DB 분석 함수가 포함되어 있는지 확인
pub fn contains_analytics_function(sql: &str) -> bool {
    let upper = sql.to_uppercase();
    upper.contains("FUNNEL_COUNT")
        || upper.contains("COHORT_ANALYSIS")
        || upper.contains("PATH_ANALYSIS")
}
