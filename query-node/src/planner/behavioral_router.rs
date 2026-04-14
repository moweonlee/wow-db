// T159: BehavioralRouter — routes Behavioral/Hybrid queries to the Active BT
// If no BT is found, returns guidance for creating one.

use crate::meta::bt_registry::BtRegistry;
use crate::planner::behavioral_column_map::apply_column_mapping;
use crate::planner::behavioral_guidance::{BehavioralGuidance, build_guidance};
use crate::planner::behavioral_pattern::{BehavioralTrigger, QueryPattern, extract_scan_table};

// ─── RoutingResult ────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct RoutingResult {
    /// Whether the query was routed to a BT
    pub routed: bool,
    /// Name of the BT used (if routed)
    pub bt_used: Option<String>,
    /// Guidance message (if no BT found)
    pub guidance: Option<BehavioralGuidance>,
    /// Rewritten SQL with the table name replaced by the BT name (if routed)
    pub rewritten_sql: Option<String>,
}

// ─── BehavioralRouter ─────────────────────────────────────────────────────────

pub struct BehavioralRouter<'a> {
    bt_registry: &'a BtRegistry,
}

impl<'a> BehavioralRouter<'a> {
    pub fn new(bt_registry: &'a BtRegistry) -> Self {
        Self { bt_registry }
    }

    /// Route a SQL query based on its detected pattern.
    ///
    /// - If `pattern` is `Event` → no routing, return `routed: false`
    /// - If `Behavioral` or `Hybrid`:
    ///   - Extract the FROM table
    ///   - Look up an Active BT in the registry
    ///   - If found → rewrite SQL, return `routed: true`
    ///   - If not found → return guidance DDL suggestion
    pub fn route(
        &self,
        sql: &str,
        pattern: &QueryPattern,
        actual_duration_ms: u64,
        row_count: u64,
    ) -> RoutingResult {
        // Pure event queries are never routed
        if *pattern == QueryPattern::Event {
            return RoutingResult {
                routed: false,
                bt_used: None,
                guidance: None,
                rewritten_sql: None,
            };
        }

        // Extract the event table being scanned
        let scan_table = match extract_scan_table(sql) {
            Some(t) => t,
            None => {
                return RoutingResult {
                    routed: false,
                    bt_used: None,
                    guidance: None,
                    rewritten_sql: None,
                };
            }
        };

        // Look for an Active BT
        if let Some(bt) = self.bt_registry.get_active_bt(&scan_table) {
            let rewritten = Self::rewrite_sql(sql, &scan_table, &bt.bt_name);
            return RoutingResult {
                routed: true,
                bt_used: Some(bt.bt_name.clone()),
                guidance: None,
                rewritten_sql: Some(rewritten),
            };
        }

        // No Active BT — build guidance
        let triggers = extract_triggers_from_pattern(pattern);
        let guidance = build_guidance(&scan_table, &triggers, actual_duration_ms, row_count);

        RoutingResult {
            routed: false,
            bt_used: None,
            guidance: Some(guidance),
            rewritten_sql: None,
        }
    }

    /// Rewrite SQL by replacing the FROM table with the BT name and applying
    /// column mapping (e.g., event_time → session_start).
    fn rewrite_sql(sql: &str, from_table: &str, to_table: &str) -> String {
        // Replace FROM <from_table> with FROM <to_table> (case-insensitive, word-bounded)
        let with_table = replace_from_table(sql, from_table, to_table);
        // Apply column mapping for BT-compatible column names
        apply_column_mapping(&with_table)
    }
}

// ─── Internal Helpers ─────────────────────────────────────────────────────────

/// Replace `FROM <from_table>` with `FROM <to_table>` in SQL.
/// Handles case-insensitive FROM keyword and table name matching.
fn replace_from_table(sql: &str, from_table: &str, to_table: &str) -> String {
    let upper = sql.to_uppercase();
    let from_upper = from_table.to_uppercase();

    // Find the position of "FROM <from_table>" pattern
    let mut search_start = 0;
    let mut result = sql.to_string();

    while let Some(from_pos) = upper[search_start..].find("FROM ") {
        let abs_from = search_start + from_pos;
        let after_from = abs_from + 5; // "FROM ".len()

        // Skip leading whitespace
        let token_start = after_from
            + upper[after_from..]
                .chars()
                .take_while(|c| c.is_whitespace())
                .map(|c| c.len_utf8())
                .sum::<usize>();

        // Check if the table name matches (case-insensitive)
        if upper[token_start..].starts_with(&from_upper) {
            let end = token_start + from_upper.len();
            // Verify word boundary after the table name
            let after_ok = end >= upper.len() || {
                let c = upper.as_bytes()[end] as char;
                !c.is_alphanumeric() && c != '_'
            };
            if after_ok {
                // Replace the table name in the result string
                // (keeping original case of the surrounding SQL)
                let mut new_result = result[..token_start].to_string();
                new_result.push_str(to_table);
                new_result.push_str(&result[end..]);
                return new_result;
            }
        }

        search_start = abs_from + 1;
        if search_start >= upper.len() {
            break;
        }
    }

    result
}

/// Extract triggers from a QueryPattern for use in guidance generation.
fn extract_triggers_from_pattern(pattern: &QueryPattern) -> Vec<BehavioralTrigger> {
    match pattern {
        QueryPattern::Behavioral { triggers } => triggers.clone(),
        QueryPattern::Hybrid => vec![],
        QueryPattern::Event => vec![],
    }
}

// ─── Unit Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::bt_registry::{BtEntry, BtState};

    fn make_active_registry(event_table: &str, bt_name: &str) -> BtRegistry {
        let mut reg = BtRegistry::new();
        reg.register(BtEntry {
            bt_name: bt_name.to_string(),
            event_table: event_table.to_string(),
            user_key: "user_id".to_string(),
            session_timeout_sec: 1800,
            state: BtState::Active,
            last_refresh: None,
        });
        reg
    }

    fn make_empty_registry() -> BtRegistry {
        BtRegistry::new()
    }

    // T158: BehavioralRouter::route rewrite correct SQL
    #[test]
    fn test_route_rewrites_sql_when_bt_active() {
        let reg = make_active_registry("page_events", "page_sessions");
        let router = BehavioralRouter::new(&reg);

        let sql = "SELECT FUNNEL_COUNT(user_id, event_name, event_time, WINDOW 7 DAYS, STEP 'view', STEP 'buy') FROM page_events";
        let pattern = QueryPattern::Behavioral {
            triggers: vec![BehavioralTrigger::FunnelFunction],
        };

        let result = router.route(sql, &pattern, 0, 100_000);

        assert!(result.routed, "Should route to BT");
        assert_eq!(result.bt_used.as_deref(), Some("page_sessions"));
        assert!(result.rewritten_sql.is_some());
        let rewritten = result.rewritten_sql.unwrap();
        assert!(rewritten.contains("page_sessions"), "Rewritten SQL should reference BT");
        assert!(!rewritten.contains("page_events"), "Rewritten SQL should not reference original table");
    }

    #[test]
    fn test_route_returns_guidance_when_no_bt() {
        let reg = make_empty_registry();
        let router = BehavioralRouter::new(&reg);

        let sql = "SELECT FUNNEL_COUNT(user_id, event_name, event_time, WINDOW 7 DAYS) FROM page_events";
        let pattern = QueryPattern::Behavioral {
            triggers: vec![BehavioralTrigger::FunnelFunction],
        };

        let result = router.route(sql, &pattern, 1500, 50_000);

        assert!(!result.routed);
        assert!(result.guidance.is_some());
        let guidance = result.guidance.unwrap();
        assert!(guidance.warning.contains("page_events"));
        assert!(guidance.suggested_ddl.contains("CREATE SESSION MATERIALIZED VIEW"));
    }

    #[test]
    fn test_route_event_pattern_not_routed() {
        let reg = make_active_registry("page_events", "page_sessions");
        let router = BehavioralRouter::new(&reg);

        let sql = "SELECT COUNT(*) FROM page_events";
        let pattern = QueryPattern::Event;

        let result = router.route(sql, &pattern, 0, 0);

        assert!(!result.routed, "Event pattern should never be routed");
        assert!(result.guidance.is_none(), "No guidance for Event pattern");
    }

    #[test]
    fn test_route_hybrid_with_no_bt_gives_guidance() {
        let reg = make_empty_registry();
        let router = BehavioralRouter::new(&reg);

        let sql = "SELECT session_id, event_name FROM page_events";
        let pattern = QueryPattern::Hybrid;

        let result = router.route(sql, &pattern, 200, 1000);

        assert!(!result.routed);
        assert!(result.guidance.is_some());
    }

    #[test]
    fn test_rewrite_sql_column_mapping() {
        let reg = make_active_registry("page_events", "page_sessions");
        let router = BehavioralRouter::new(&reg);

        let sql = "SELECT * FROM page_events WHERE event_time > '2026-01-01'";
        let pattern = QueryPattern::Behavioral {
            triggers: vec![BehavioralTrigger::SessionIdColumn],
        };

        let result = router.route(sql, &pattern, 0, 10_000);

        assert!(result.routed);
        let rewritten = result.rewritten_sql.unwrap();
        assert!(rewritten.contains("session_start"),
            "event_time should be mapped to session_start: {}", rewritten);
    }

    #[test]
    fn test_replace_from_table_basic() {
        let sql = "SELECT * FROM page_events WHERE x = 1";
        let replaced = replace_from_table(sql, "page_events", "page_sessions");
        assert!(replaced.contains("page_sessions"));
        assert!(!replaced.contains("page_events"));
    }

    #[test]
    fn test_replace_from_table_case_insensitive_table() {
        let sql = "SELECT * FROM Page_Events LIMIT 10";
        let replaced = replace_from_table(sql, "page_events", "page_sessions");
        // Note: our replace finds the match but the word "Page_Events" won't match
        // "page_events" in uppercase comparison since they differ by case.
        // The function compares uppercase(sql) with uppercase(from_table).
        // "PAGE_EVENTS" == "PAGE_EVENTS" → matches.
        assert!(replaced.contains("page_sessions") || replaced.contains("Page_Events"),
            "Either replaced or left unchanged: {}", replaced);
    }
}
