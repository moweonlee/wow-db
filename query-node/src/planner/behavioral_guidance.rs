// T156: BehavioralGuidance — warning + DDL suggestion when no BT is found

use crate::planner::behavioral_pattern::BehavioralTrigger;

// ─── BehavioralGuidance ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct BehavioralGuidance {
    /// MySQL-style warning message
    pub warning: String,
    /// Suggested CREATE SESSION MATERIALIZED VIEW DDL
    pub suggested_ddl: String,
    /// Estimated query speedup multiplier if BT were used (None if unknown)
    pub estimated_speedup: Option<f64>,
    /// Actual duration of the fallback query in milliseconds
    pub actual_duration_ms: u64,
}

// ─── build_guidance ───────────────────────────────────────────────────────────

/// Build a BehavioralGuidance for when no Active BT is found for the event table.
///
/// - `event_table`:        the event table being scanned
/// - `triggers`:           behavioral triggers detected in the query
/// - `actual_duration_ms`: elapsed time of the fallback execution
/// - `row_count`:          number of rows in the event table (used for speedup estimate)
pub fn build_guidance(
    event_table: &str,
    triggers: &[BehavioralTrigger],
    actual_duration_ms: u64,
    row_count: u64,
) -> BehavioralGuidance {
    let bt_name = format!("{}_sessions", event_table);
    let trigger_names = describe_triggers(triggers);

    let estimated_speedup = if row_count >= 10_000 {
        Some(5.0_f64)
    } else {
        Some(2.0_f64)
    };

    let suggested_ddl = format!(
        "CREATE SESSION MATERIALIZED VIEW {bt_name}\n  ON {event_table}\n  USER KEY user_id\n  SESSION TIMEOUT 30 MINUTES\n  REFRESH INCREMENTAL;",
        bt_name = bt_name,
        event_table = event_table,
    );

    let speedup_hint = estimated_speedup
        .map(|s| format!("Estimated speedup: {:.0}×. ", s))
        .unwrap_or_default();

    let warning = format!(
        "No Behavioral Table found for '{event_table}'. \
Query used triggers: [{trigger_names}] but no Active Session Materialized View is registered. \
{speedup_hint}\
Run the following DDL to create one:\n{suggested_ddl}",
        event_table = event_table,
        trigger_names = trigger_names,
        speedup_hint = speedup_hint,
        suggested_ddl = suggested_ddl,
    );

    BehavioralGuidance {
        warning,
        suggested_ddl,
        estimated_speedup,
        actual_duration_ms,
    }
}

// ─── Internal Helpers ─────────────────────────────────────────────────────────

fn describe_triggers(triggers: &[BehavioralTrigger]) -> String {
    triggers
        .iter()
        .map(|t| match t {
            BehavioralTrigger::FunnelFunction      => "FUNNEL_COUNT",
            BehavioralTrigger::CohortFunction      => "COHORT_ANALYSIS",
            BehavioralTrigger::PathFunction        => "PATH_ANALYSIS",
            BehavioralTrigger::SessionIdColumn     => "session_id",
            BehavioralTrigger::SessionStartColumn  => "session_start",
            BehavioralTrigger::SessionEndColumn    => "session_end",
            BehavioralTrigger::EventSequenceColumn => "event_sequence",
            BehavioralTrigger::SessionTimeoutParam => "session_timeout",
        })
        .collect::<Vec<_>>()
        .join(", ")
}

// ─── Unit Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::behavioral_pattern::BehavioralTrigger;

    // T158: build_guidance includes expected keywords
    #[test]
    fn test_build_guidance_includes_expected_keywords() {
        let triggers = vec![BehavioralTrigger::FunnelFunction];
        let guidance = build_guidance("page_events", &triggers, 1500, 100_000);

        assert!(guidance.warning.contains("page_events"),
            "Warning should mention the event table");
        assert!(guidance.warning.contains("FUNNEL_COUNT"),
            "Warning should mention the trigger");
        assert!(guidance.warning.contains("CREATE SESSION MATERIALIZED VIEW"),
            "Warning should contain DDL suggestion");
        assert!(guidance.suggested_ddl.contains("page_events_sessions"),
            "DDL should name the BT after the event table");
        assert!(guidance.suggested_ddl.contains("USER KEY"),
            "DDL should include USER KEY clause");
        assert!(guidance.suggested_ddl.contains("SESSION TIMEOUT"),
            "DDL should include SESSION TIMEOUT clause");
    }

    #[test]
    fn test_estimated_speedup_large_table() {
        let guidance = build_guidance(
            "page_events",
            &[BehavioralTrigger::FunnelFunction],
            500,
            10_000,
        );
        assert_eq!(guidance.estimated_speedup, Some(5.0));
    }

    #[test]
    fn test_estimated_speedup_small_table() {
        let guidance = build_guidance(
            "page_events",
            &[BehavioralTrigger::FunnelFunction],
            500,
            100,
        );
        assert_eq!(guidance.estimated_speedup, Some(2.0));
    }

    #[test]
    fn test_guidance_actual_duration() {
        let guidance = build_guidance(
            "page_events",
            &[BehavioralTrigger::CohortFunction],
            2345,
            50_000,
        );
        assert_eq!(guidance.actual_duration_ms, 2345);
    }

    #[test]
    fn test_guidance_ddl_format() {
        let guidance = build_guidance(
            "click_events",
            &[BehavioralTrigger::PathFunction],
            100,
            5_000,
        );
        assert!(guidance.suggested_ddl.contains("click_events_sessions"));
        assert!(guidance.suggested_ddl.contains("ON click_events"));
        assert!(guidance.suggested_ddl.contains("REFRESH INCREMENTAL"));
    }

    #[test]
    fn test_multiple_triggers_in_warning() {
        let triggers = vec![
            BehavioralTrigger::FunnelFunction,
            BehavioralTrigger::SessionIdColumn,
        ];
        let guidance = build_guidance("events", &triggers, 100, 1_000);
        assert!(guidance.warning.contains("FUNNEL_COUNT"));
        assert!(guidance.warning.contains("session_id"));
    }
}
