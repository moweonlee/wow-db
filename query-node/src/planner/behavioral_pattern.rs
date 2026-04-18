// T153: Behavioral Pattern Detection
// Detects if a SQL query is Behavioral, Event, or Hybrid based on
// FUNNEL/COHORT/PATH functions and session-related column references.

// ─── BehavioralTrigger ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
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

// ─── QueryPattern ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryPattern {
    /// Contains only behavioral references (funnel/cohort/path functions or session columns)
    Behavioral { triggers: Vec<BehavioralTrigger> },
    /// Pure event table scan — no behavioral references
    Event,
    /// Mix of behavioral and non-behavioral references
    Hybrid,
}

// ─── Pattern Detection ────────────────────────────────────────────────────────

/// Detect whether a SQL string is Behavioral, Event, or Hybrid.
///
/// Rules:
/// - FUNNEL_COUNT, COHORT_ANALYSIS, PATH_ANALYSIS → Behavioral trigger
/// - Columns: session_id, session_start, session_end, event_sequence,
///   session_event_count, session_timeout → Behavioral trigger
/// - If only behavioral triggers found → Behavioral
/// - If behavioral + non-behavioral references → Hybrid
/// - Otherwise → Event
pub fn detect_pattern(sql: &str) -> QueryPattern {
    let upper = sql.to_uppercase();
    let mut triggers: Vec<BehavioralTrigger> = Vec::new();

    // Function-based triggers
    if upper.contains("FUNNEL_COUNT") {
        triggers.push(BehavioralTrigger::FunnelFunction);
    }
    if upper.contains("COHORT_ANALYSIS") {
        triggers.push(BehavioralTrigger::CohortFunction);
    }
    if upper.contains("PATH_ANALYSIS") {
        triggers.push(BehavioralTrigger::PathFunction);
    }

    // Column-based triggers (word-boundary check to avoid false positives)
    if contains_column(&upper, "SESSION_ID") {
        triggers.push(BehavioralTrigger::SessionIdColumn);
    }
    if contains_column(&upper, "SESSION_START") {
        triggers.push(BehavioralTrigger::SessionStartColumn);
    }
    if contains_column(&upper, "SESSION_END") {
        triggers.push(BehavioralTrigger::SessionEndColumn);
    }
    if contains_column(&upper, "EVENT_SEQUENCE") {
        triggers.push(BehavioralTrigger::EventSequenceColumn);
    }
    if contains_column(&upper, "SESSION_EVENT_COUNT") {
        triggers.push(BehavioralTrigger::EventSequenceColumn);
    }
    if contains_column(&upper, "SESSION_TIMEOUT") {
        triggers.push(BehavioralTrigger::SessionTimeoutParam);
    }

    if triggers.is_empty() {
        return QueryPattern::Event;
    }

    // If ANY function-based trigger is present (FUNNEL/COHORT/PATH), the query is
    // definitively Behavioral regardless of column references (those columns are
    // arguments to the analytics function, not independent event table references).
    let has_function_trigger = triggers.iter().any(|t| matches!(
        t,
        BehavioralTrigger::FunnelFunction
            | BehavioralTrigger::CohortFunction
            | BehavioralTrigger::PathFunction
    ));

    if has_function_trigger {
        return QueryPattern::Behavioral { triggers };
    }

    // For column-based triggers only: check if there are also standard event references.
    // If both exist → Hybrid; if only behavioral columns → Behavioral.
    let has_non_behavioral = has_standard_event_references(&upper);

    if has_non_behavioral {
        QueryPattern::Hybrid
    } else {
        QueryPattern::Behavioral { triggers }
    }
}

/// Extract the primary scan table from a SQL FROM clause.
/// Returns the first table name after FROM (case-insensitive).
pub fn extract_scan_table(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let mut iter = upper.split_whitespace().peekable();
    while let Some(tok) = iter.next() {
        if tok == "FROM" {
            if let Some(next) = iter.peek() {
                // Skip subqueries
                if !next.starts_with('(') {
                    // Strip trailing commas, semicolons, backticks
                    let raw = next.trim_matches(|c: char| {
                        c == ',' || c == ';' || c == '`' || c == '\'' || c == '"'
                    });
                    // Get the original-case version from the original SQL
                    return find_table_name_original_case(sql, raw);
                }
            }
        }
    }
    None
}

// ─── Internal Helpers ─────────────────────────────────────────────────────────

/// Check if a column name appears as a word in the SQL (not as part of a larger identifier).
fn contains_column(upper_sql: &str, col_upper: &str) -> bool {
    let mut start = 0;
    while let Some(pos) = upper_sql[start..].find(col_upper) {
        let abs_pos = start + pos;
        let before_ok = abs_pos == 0 || {
            let c = upper_sql.as_bytes()[abs_pos - 1] as char;
            !c.is_alphanumeric() && c != '_'
        };
        let after_ok = abs_pos + col_upper.len() >= upper_sql.len() || {
            let c = upper_sql.as_bytes()[abs_pos + col_upper.len()] as char;
            !c.is_alphanumeric() && c != '_'
        };
        if before_ok && after_ok {
            return true;
        }
        start = abs_pos + 1;
    }
    false
}

/// Check if the SQL has standard (non-behavioral) event column references.
/// This is used to decide Behavioral vs Hybrid.
fn has_standard_event_references(upper_sql: &str) -> bool {
    // Standard event columns that indicate raw event table access
    let event_cols = [
        "EVENT_NAME",
        "EVENT_TIME",
        "EVENT_TYPE",
        "PAGE_URL",
        "REFERRER",
        "DEVICE_TYPE",
        "BROWSER",
        "COUNTRY",
        "IP_ADDRESS",
        "REVENUE",
        "PRODUCT_ID",
    ];
    for col in &event_cols {
        if contains_column(upper_sql, col) {
            return true;
        }
    }
    false
}

/// Locate the FROM table name in its original case from the SQL.
fn find_table_name_original_case(sql: &str, upper_name: &str) -> Option<String> {
    // Walk through tokens in the original SQL to find the match
    let mut iter = sql.split_whitespace().peekable();
    while let Some(tok) = iter.next() {
        if tok.to_uppercase() == "FROM" {
            if let Some(next) = iter.peek() {
                if !next.starts_with('(') {
                    let raw = next.trim_matches(|c: char| {
                        c == ',' || c == ';' || c == '`' || c == '\'' || c == '"'
                    });
                    if raw.to_uppercase() == upper_name {
                        return Some(raw.to_string());
                    }
                    // Might have dot-qualified name like db.table
                    let base = raw.rsplit('.').next().unwrap_or(raw);
                    if base.to_uppercase() == upper_name {
                        return Some(base.to_string());
                    }
                }
            }
        }
    }
    None
}

// ─── Unit Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // T158: detect_pattern — FUNNEL_COUNT → Behavioral
    #[test]
    fn test_funnel_count_is_behavioral() {
        let sql = "SELECT FUNNEL_COUNT(user_id, event_name, event_time, WINDOW 7 DAYS, STEP 'page_view', STEP 'purchase') FROM page_events";
        let pattern = detect_pattern(sql);
        match &pattern {
            QueryPattern::Behavioral { triggers } => {
                assert!(triggers.contains(&BehavioralTrigger::FunnelFunction),
                    "Expected FunnelFunction trigger");
            }
            other => panic!("Expected Behavioral, got {:?}", other),
        }
    }

    // T158: detect_pattern — COUNT(*) → Event
    #[test]
    fn test_count_star_is_event() {
        let sql = "SELECT COUNT(*) FROM page_events WHERE event_time > '2026-01-01'";
        let pattern = detect_pattern(sql);
        assert_eq!(pattern, QueryPattern::Event, "COUNT(*) should be Event pattern");
    }

    #[test]
    fn test_cohort_analysis_is_behavioral() {
        let sql = "SELECT COHORT_ANALYSIS(user_id, event_name, event_time) FROM page_events";
        let pattern = detect_pattern(sql);
        match &pattern {
            QueryPattern::Behavioral { triggers } => {
                assert!(triggers.contains(&BehavioralTrigger::CohortFunction));
            }
            other => panic!("Expected Behavioral, got {:?}", other),
        }
    }

    #[test]
    fn test_path_analysis_is_behavioral() {
        let sql = "SELECT PATH_ANALYSIS(user_id, event_name, event_time) FROM page_events";
        let pattern = detect_pattern(sql);
        match &pattern {
            QueryPattern::Behavioral { triggers } => {
                assert!(triggers.contains(&BehavioralTrigger::PathFunction));
            }
            other => panic!("Expected Behavioral, got {:?}", other),
        }
    }

    #[test]
    fn test_session_id_column_is_behavioral() {
        let sql = "SELECT session_id, COUNT(*) FROM sessions GROUP BY session_id";
        let pattern = detect_pattern(sql);
        match &pattern {
            QueryPattern::Behavioral { triggers } => {
                assert!(triggers.contains(&BehavioralTrigger::SessionIdColumn));
            }
            other => panic!("Expected Behavioral, got {:?}", other),
        }
    }

    #[test]
    fn test_session_start_column_is_behavioral() {
        let sql = "SELECT session_start, session_end FROM user_sessions";
        let pattern = detect_pattern(sql);
        match &pattern {
            QueryPattern::Behavioral { triggers } => {
                assert!(triggers.contains(&BehavioralTrigger::SessionStartColumn));
                assert!(triggers.contains(&BehavioralTrigger::SessionEndColumn));
            }
            other => panic!("Expected Behavioral, got {:?}", other),
        }
    }

    #[test]
    fn test_event_sequence_column_is_behavioral() {
        let sql = "SELECT event_sequence FROM page_sessions WHERE user_id = 1";
        let pattern = detect_pattern(sql);
        match &pattern {
            QueryPattern::Behavioral { .. } => {}
            other => panic!("Expected Behavioral, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_scan_table_basic() {
        let sql = "SELECT * FROM page_events WHERE event_time > '2026-01-01'";
        assert_eq!(extract_scan_table(sql), Some("page_events".to_string()));
    }

    #[test]
    fn test_extract_scan_table_with_funnel() {
        let sql = "SELECT FUNNEL_COUNT(user_id, event_name, event_time, WINDOW 7 DAYS) FROM page_events";
        assert_eq!(extract_scan_table(sql), Some("page_events".to_string()));
    }

    #[test]
    fn test_extract_scan_table_none_for_no_from() {
        let sql = "SELECT 1";
        assert_eq!(extract_scan_table(sql), None);
    }

    #[test]
    fn test_simple_select_is_event() {
        let sql = "SELECT user_id, event_name FROM page_events LIMIT 10";
        let pattern = detect_pattern(sql);
        assert_eq!(pattern, QueryPattern::Event);
    }
}
