// T158, T163–T171: Behavioral Routing unit and integration tests

#[cfg(test)]
mod behavioral_routing_tests {
    use crate::meta::bt_registry::{BtEntry, BtRegistry, BtState};
    use crate::planner::behavioral_column_map::{apply_column_mapping, MappingContext, map_column};
    use crate::planner::behavioral_guidance::build_guidance;
    use crate::planner::behavioral_pattern::{detect_pattern, extract_scan_table, BehavioralTrigger, QueryPattern};
    use crate::planner::behavioral_router::{BehavioralRouter, RoutingResult};

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn make_active_bt(event_table: &str, bt_name: &str) -> BtRegistry {
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

    fn make_stale_bt(event_table: &str, bt_name: &str) -> BtRegistry {
        let mut reg = BtRegistry::new();
        reg.register(BtEntry {
            bt_name: bt_name.to_string(),
            event_table: event_table.to_string(),
            user_key: "user_id".to_string(),
            session_timeout_sec: 1800,
            state: BtState::Stale,
            last_refresh: None,
        });
        reg
    }

    // ── T158: Unit tests — pattern detection ──────────────────────────────────

    #[test]
    fn t158_funnel_query_is_behavioral() {
        let sql = "SELECT FUNNEL_COUNT(user_id, event_name, event_time, WINDOW 7 DAYS, STEP 'page_view', STEP 'purchase') FROM page_events";
        let pattern = detect_pattern(sql);
        match &pattern {
            QueryPattern::Behavioral { triggers } => {
                assert!(triggers.contains(&BehavioralTrigger::FunnelFunction));
            }
            other => panic!("Expected Behavioral, got {:?}", other),
        }
    }

    #[test]
    fn t158_count_star_is_event_no_warning() {
        let sql = "SELECT COUNT(*) FROM page_events GROUP BY event_name";
        let pattern = detect_pattern(sql);
        assert_eq!(pattern, QueryPattern::Event, "COUNT(*) should be Event — no warning needed");
    }

    #[test]
    fn t158_bt_registry_stale_returns_no_active() {
        let reg = make_stale_bt("page_events", "page_sessions");
        assert!(reg.get_active_bt("page_events").is_none(),
            "Stale BT must not be returned as Active");
    }

    #[test]
    fn t158_bt_registry_active_returns_entry() {
        let reg = make_active_bt("page_events", "page_sessions");
        let bt = reg.get_active_bt("page_events");
        assert!(bt.is_some());
        assert_eq!(bt.unwrap().bt_name, "page_sessions");
    }

    #[test]
    fn t158_router_rewrites_sql_correctly() {
        let reg = make_active_bt("page_events", "page_sessions");
        let router = BehavioralRouter::new(&reg);
        let sql = "SELECT FUNNEL_COUNT(user_id, event_name, event_time, WINDOW 7 DAYS) FROM page_events";
        let pattern = QueryPattern::Behavioral { triggers: vec![BehavioralTrigger::FunnelFunction] };
        let result = router.route(sql, &pattern, 0, 100_000);
        assert!(result.routed);
        let rewritten = result.rewritten_sql.unwrap();
        assert!(rewritten.contains("page_sessions"));
        assert!(!rewritten.contains("page_events"));
    }

    #[test]
    fn t158_build_guidance_contains_expected_keywords() {
        let triggers = vec![BehavioralTrigger::FunnelFunction];
        let guidance = build_guidance("page_events", &triggers, 1000, 50_000);
        assert!(guidance.warning.contains("page_events"));
        assert!(guidance.warning.contains("FUNNEL_COUNT"));
        assert!(guidance.suggested_ddl.contains("CREATE SESSION MATERIALIZED VIEW"));
        assert!(guidance.suggested_ddl.contains("page_events_sessions"));
        assert!(guidance.suggested_ddl.contains("USER KEY"));
        assert!(guidance.suggested_ddl.contains("SESSION TIMEOUT"));
    }

    // ── T163: D-001 Full chain: no BT → guidance, then BT created → routing ──

    #[test]
    fn t163_d001_full_chain_no_bt_then_active_bt() {
        let sql = "SELECT FUNNEL_COUNT(user_id, event_name, event_time, WINDOW 7 DAYS) FROM page_events";
        let pattern = detect_pattern(sql);

        // Step 1: No BT → guidance
        let empty_reg = BtRegistry::new();
        let router = BehavioralRouter::new(&empty_reg);
        let result = router.route(sql, &pattern, 500, 50_000);
        assert!(!result.routed, "Should not route without BT");
        assert!(result.guidance.is_some(), "Should provide guidance");
        let guidance = result.guidance.unwrap();
        assert!(guidance.suggested_ddl.contains("CREATE SESSION MATERIALIZED VIEW"));

        // Step 2: BT created → routing
        let active_reg = make_active_bt("page_events", "page_events_sessions");
        let router2 = BehavioralRouter::new(&active_reg);
        let result2 = router2.route(sql, &pattern, 0, 50_000);
        assert!(result2.routed, "Should route after BT is created");
        assert_eq!(result2.bt_used.as_deref(), Some("page_events_sessions"));
    }

    // ── T164: D-002 COHORT_ANALYSIS auto-routing ──────────────────────────────

    #[test]
    fn t164_d002_cohort_analysis_auto_routing() {
        let sql = "SELECT COHORT_ANALYSIS(user_id, event_name, event_time) FROM page_events";
        let pattern = detect_pattern(sql);
        match &pattern {
            QueryPattern::Behavioral { triggers } => {
                assert!(triggers.contains(&BehavioralTrigger::CohortFunction));
            }
            other => panic!("Expected Behavioral, got {:?}", other),
        }

        let reg = make_active_bt("page_events", "page_events_sessions");
        let router = BehavioralRouter::new(&reg);
        let result = router.route(sql, &pattern, 0, 0);
        assert!(result.routed, "COHORT_ANALYSIS should route to BT");
        assert!(result.guidance.is_none(), "No guidance when BT is found");
    }

    // ── T165: D-003 PATH_ANALYSIS auto-routing ────────────────────────────────

    #[test]
    fn t165_d003_path_analysis_auto_routing() {
        let sql = "SELECT PATH_ANALYSIS(user_id, event_name, event_time) FROM page_events";
        let pattern = detect_pattern(sql);
        match &pattern {
            QueryPattern::Behavioral { triggers } => {
                assert!(triggers.contains(&BehavioralTrigger::PathFunction));
            }
            other => panic!("Expected Behavioral, got {:?}", other),
        }

        let reg = make_active_bt("page_events", "page_events_sessions");
        let router = BehavioralRouter::new(&reg);
        let result = router.route(sql, &pattern, 0, 0);
        assert!(result.routed, "PATH_ANALYSIS should route to BT");
        assert!(result.guidance.is_none());
    }

    // ── T166: D-004 Event query not routed even with active BT ────────────────

    #[test]
    fn t166_d004_event_query_not_routed() {
        let sql = "SELECT COUNT(*), event_name FROM page_events GROUP BY event_name";
        let pattern = detect_pattern(sql);
        assert_eq!(pattern, QueryPattern::Event, "COUNT(*)/GROUP BY is Event");

        let reg = make_active_bt("page_events", "page_events_sessions");
        let router = BehavioralRouter::new(&reg);
        let result = router.route(sql, &pattern, 0, 0);
        assert!(!result.routed, "Event queries must never be routed to BT");
        assert!(result.guidance.is_none(), "No guidance for Event pattern");
    }

    // ── T167: D-005 Unregistered table → no routing/guidance ─────────────────

    #[test]
    fn t167_d005_unregistered_table_no_routing() {
        // Table "other_table" not in registry, even though SQL has session_id column
        let sql = "SELECT session_id FROM other_table";
        let pattern = detect_pattern(sql);
        // Pattern is Behavioral due to session_id column
        assert!(matches!(pattern, QueryPattern::Behavioral { .. }));

        // But registry has no BT for other_table
        let reg = make_active_bt("page_events", "page_events_sessions");
        let router = BehavioralRouter::new(&reg);
        let result = router.route(sql, &pattern, 0, 0);
        assert!(!result.routed, "No routing for unregistered table");
        // Guidance is returned because pattern is Behavioral but no BT
        assert!(result.guidance.is_some(), "Guidance provided for behavioral query without BT");
    }

    // ── T168: D-006 Stale BT → fallback + guidance ───────────────────────────

    #[test]
    fn t168_d006_stale_bt_fallback_with_guidance() {
        let sql = "SELECT FUNNEL_COUNT(user_id, event_name, event_time, WINDOW 7 DAYS) FROM page_events";
        let pattern = detect_pattern(sql);

        let reg = make_stale_bt("page_events", "page_events_sessions");
        let router = BehavioralRouter::new(&reg);
        let result = router.route(sql, &pattern, 800, 100_000);

        assert!(!result.routed, "Stale BT should not be used for routing");
        assert!(result.guidance.is_some(), "Should provide guidance when BT is Stale");
    }

    // ── T169: D-007 BT immediately routable after registration ───────────────

    #[test]
    fn t169_d007_immediate_routing_after_bt_registration() {
        let sql = "SELECT FUNNEL_COUNT(user_id, event_name, event_time, WINDOW 7 DAYS) FROM page_events";
        let pattern = detect_pattern(sql);

        // Register BT and immediately check routing (no delay)
        let mut reg = BtRegistry::new();
        reg.register(BtEntry {
            bt_name: "page_events_sessions".to_string(),
            event_table: "page_events".to_string(),
            user_key: "user_id".to_string(),
            session_timeout_sec: 1800,
            state: BtState::Active,
            last_refresh: None,
        });

        let router = BehavioralRouter::new(&reg);
        let result = router.route(sql, &pattern, 0, 0);
        assert!(result.routed, "Should route immediately after BT registration");
    }

    // ── T170: D-008 Guidance message quality ─────────────────────────────────

    #[test]
    fn t170_d008_guidance_message_quality() {
        let triggers = vec![BehavioralTrigger::FunnelFunction];
        let guidance = build_guidance("page_events", &triggers, 2000, 500_000);

        // Must contain: table name, CREATE SESSION MV DDL, USER KEY, SESSION TIMEOUT, performance hint
        assert!(guidance.warning.contains("page_events"), "Must mention table name");
        assert!(guidance.suggested_ddl.contains("CREATE SESSION MATERIALIZED VIEW"),
            "Must contain DDL keyword");
        assert!(guidance.suggested_ddl.contains("USER KEY"), "Must contain USER KEY");
        assert!(guidance.suggested_ddl.contains("SESSION TIMEOUT"), "Must contain SESSION TIMEOUT");
        assert!(guidance.warning.contains("speedup") || guidance.warning.contains("Estimated"),
            "Must contain performance hint: {}", guidance.warning);
        assert!(guidance.estimated_speedup.is_some(), "Must have speedup estimate");
    }

    // ── T171: E-001 Routing correctness — rewritten SQL is structurally valid ─

    #[test]
    fn t171_e001_bt_routing_rewritten_sql_correctness() {
        let reg = make_active_bt("page_events", "page_events_sessions");
        let router = BehavioralRouter::new(&reg);

        let sql = "SELECT FUNNEL_COUNT(user_id, event_name, event_time, WINDOW 7 DAYS) FROM page_events WHERE event_time > '2026-01-01'";
        let pattern = QueryPattern::Behavioral { triggers: vec![BehavioralTrigger::FunnelFunction] };
        let result = router.route(sql, &pattern, 0, 100_000);

        assert!(result.routed);
        let rewritten = result.rewritten_sql.unwrap();

        // Verify structural correctness of rewritten SQL:
        // 1. Table replaced
        assert!(rewritten.contains("page_events_sessions"), "Table must be replaced");
        assert!(!rewritten.contains("page_events "), "Original table must be removed");
        // 2. Column mapping applied
        assert!(rewritten.contains("session_start"), "event_time → session_start in WHERE");
        // 3. FUNNEL_COUNT function preserved
        assert!(rewritten.contains("FUNNEL_COUNT"), "Function must be preserved");
    }

    // ── Additional integration tests ──────────────────────────────────────────

    #[test]
    fn integration_extract_scan_table_various_forms() {
        assert_eq!(extract_scan_table("SELECT * FROM page_events WHERE x=1"),
            Some("page_events".to_string()));
        assert_eq!(extract_scan_table("SELECT FUNNEL_COUNT() FROM click_events LIMIT 10"),
            Some("click_events".to_string()));
        assert_eq!(extract_scan_table("SELECT 1"), None);
    }

    #[test]
    fn integration_column_mapping_in_where_clause() {
        let sql = "SELECT * FROM page_sessions WHERE event_time > '2026-01-01' AND event_time < '2026-02-01'";
        let mapped = apply_column_mapping(sql);
        assert_eq!(mapped.matches("session_start").count(), 2);
        assert!(!mapped.contains("event_time"));
    }

    #[test]
    fn integration_map_column_all_contexts() {
        assert_eq!(map_column("event_time", MappingContext::WhereFilter), "session_start");
        assert_eq!(map_column("event_time", MappingContext::Projection), "session_start");
        assert_eq!(map_column("event_time", MappingContext::Other), "session_start");
        assert_eq!(map_column("user_id", MappingContext::WhereFilter), "user_id");
        assert_eq!(map_column("session_id", MappingContext::Projection), "session_id");
    }

    #[test]
    fn integration_hybrid_pattern_with_active_bt() {
        let sql = "SELECT session_id, event_name FROM page_events WHERE event_time > '2026-01-01'";
        let pattern = detect_pattern(sql);
        // Has session_id (behavioral) AND event_name/event_time (standard event columns)
        // event_time is in has_standard_event_references → Hybrid or Behavioral depending on impl
        // At minimum it should not be pure Event
        assert!(
            matches!(pattern, QueryPattern::Behavioral { .. } | QueryPattern::Hybrid),
            "Mixed query should be Behavioral or Hybrid: {:?}", pattern
        );
    }

    #[test]
    fn integration_bt_registry_multiple_tables() {
        let mut reg = BtRegistry::new();
        reg.register(BtEntry {
            bt_name: "click_sessions".to_string(),
            event_table: "click_events".to_string(),
            user_key: "user_id".to_string(),
            session_timeout_sec: 1800,
            state: BtState::Active,
            last_refresh: None,
        });
        reg.register(BtEntry {
            bt_name: "page_sessions".to_string(),
            event_table: "page_events".to_string(),
            user_key: "device_id".to_string(),
            session_timeout_sec: 3600,
            state: BtState::Active,
            last_refresh: None,
        });

        let router = BehavioralRouter::new(&reg);
        let pattern = QueryPattern::Behavioral { triggers: vec![BehavioralTrigger::FunnelFunction] };

        let r1 = router.route(
            "SELECT FUNNEL_COUNT() FROM click_events",
            &pattern, 0, 0,
        );
        assert!(r1.routed);
        assert_eq!(r1.bt_used.as_deref(), Some("click_sessions"));

        let r2 = router.route(
            "SELECT FUNNEL_COUNT() FROM page_events",
            &pattern, 0, 0,
        );
        assert!(r2.routed);
        assert_eq!(r2.bt_used.as_deref(), Some("page_sessions"));
    }
}
