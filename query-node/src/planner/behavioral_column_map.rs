// T160: Behavioral Column Mapping
// Maps raw event table column names to their Session MV (BT) equivalents.

// ─── MappingContext ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MappingContext {
    WhereFilter,
    Projection,
    Other,
}

// ─── Column Mapping ───────────────────────────────────────────────────────────

/// Map a single column name from event-table space to BT space.
///
/// Currently maps:
/// - `event_time` → `session_start`
/// All other columns are returned unchanged.
pub fn map_column<'a>(col: &'a str, _context: MappingContext) -> &'a str {
    match col {
        "event_time" => "session_start",
        other => other,
    }
}

/// Apply column mapping to a full SQL string.
///
/// Replaces `event_time` references with `session_start` as a word-boundary
/// replacement (avoids mangling identifiers like `event_timestamp`).
pub fn apply_column_mapping(sql: &str) -> String {
    replace_word_bounded(sql, "event_time", "session_start")
}

// ─── Internal Helpers ─────────────────────────────────────────────────────────

/// Replace all word-bounded occurrences of `from` with `to` in `text`.
/// A "word boundary" means the character before/after is not alphanumeric or `_`.
fn replace_word_bounded(text: &str, from: &str, to: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let bytes = text.as_bytes();
    let from_bytes = from.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        // Try to match `from` at position `i`
        if bytes[i..].starts_with(from_bytes) {
            let before_ok = i == 0 || {
                let c = bytes[i - 1] as char;
                !c.is_alphanumeric() && c != '_'
            };
            let after_ok = i + from_bytes.len() >= bytes.len() || {
                let c = bytes[i + from_bytes.len()] as char;
                !c.is_alphanumeric() && c != '_'
            };

            if before_ok && after_ok {
                result.push_str(to);
                i += from_bytes.len();
                continue;
            }
        }
        result.push(bytes[i] as char);
        i += 1;
    }

    result
}

// ─── Unit Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_column_event_time() {
        assert_eq!(map_column("event_time", MappingContext::WhereFilter), "session_start");
        assert_eq!(map_column("event_time", MappingContext::Projection), "session_start");
    }

    #[test]
    fn test_map_column_passthrough() {
        assert_eq!(map_column("user_id", MappingContext::WhereFilter), "user_id");
        assert_eq!(map_column("session_id", MappingContext::Projection), "session_id");
        assert_eq!(map_column("device_id", MappingContext::Other), "device_id");
    }

    #[test]
    fn test_apply_column_mapping_in_where() {
        let sql = "SELECT * FROM page_sessions WHERE event_time > '2026-01-01'";
        let mapped = apply_column_mapping(sql);
        assert!(mapped.contains("session_start"), "Should replace event_time with session_start");
        assert!(!mapped.contains("event_time"), "Should not contain event_time after mapping");
    }

    #[test]
    fn test_apply_column_mapping_no_false_positive() {
        // event_timestamp should NOT be replaced
        let sql = "SELECT event_timestamp FROM events";
        let mapped = apply_column_mapping(sql);
        assert!(mapped.contains("event_timestamp"), "Should not mangle event_timestamp");
        assert!(!mapped.contains("session_start"), "Should not add session_start");
    }

    #[test]
    fn test_apply_column_mapping_multiple_occurrences() {
        let sql = "SELECT event_time FROM t WHERE event_time > '2026-01-01' ORDER BY event_time";
        let mapped = apply_column_mapping(sql);
        assert_eq!(mapped.matches("session_start").count(), 3,
            "Should replace all three occurrences of event_time");
    }

    #[test]
    fn test_apply_column_mapping_no_event_time() {
        let sql = "SELECT session_id, user_id FROM sessions";
        let mapped = apply_column_mapping(sql);
        assert_eq!(mapped, sql, "SQL without event_time should be unchanged");
    }
}
