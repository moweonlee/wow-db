// T154: BtRegistry — Behavioral Table (Session MV) registry
// Tracks Event Table ↔ BT (Session Materialized View) pairings.

use std::collections::HashMap;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

// ─── BtState ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BtState {
    /// Initial build in progress
    Building,
    /// Ready to serve queries
    Active,
    /// Refresh in progress (still queryable from last snapshot)
    Refreshing,
    /// Data is stale — should fall back to event table
    Stale,
    /// Error state with message
    Error(String),
}

// ─── BtEntry ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BtEntry {
    /// Name of the Behavioral Table (Session MV)
    pub bt_name: String,
    /// Source event table this BT was built from
    pub event_table: String,
    /// User/session key column used for session grouping
    pub user_key: String,
    /// Session inactivity timeout in seconds
    pub session_timeout_sec: u64,
    /// Current state of this BT
    pub state: BtState,
    /// Last successful refresh timestamp
    pub last_refresh: Option<SystemTime>,
}

// ─── BtRegistry ──────────────────────────────────────────────────────────────

/// Registry mapping event table names → list of associated Behavioral Tables.
pub struct BtRegistry {
    /// Key: event_table name (lowercase), Value: list of BT entries
    pairs: HashMap<String, Vec<BtEntry>>,
}

impl BtRegistry {
    pub fn new() -> Self {
        Self {
            pairs: HashMap::new(),
        }
    }

    /// Get all BT entries associated with the given event table.
    pub fn get_bt_for_table(&self, event_table: &str) -> Vec<&BtEntry> {
        self.pairs
            .get(&event_table.to_lowercase())
            .map(|v| v.iter().collect())
            .unwrap_or_default()
    }

    /// Register a new BT entry. If a BT with the same name already exists for
    /// this event table, it is replaced.
    pub fn register(&mut self, entry: BtEntry) {
        let key = entry.event_table.to_lowercase();
        let list = self.pairs.entry(key).or_default();
        // Replace existing entry with same bt_name
        if let Some(idx) = list.iter().position(|e| e.bt_name == entry.bt_name) {
            list[idx] = entry;
        } else {
            list.push(entry);
        }
    }

    /// Update the state of a BT by name (searches across all event tables).
    pub fn update_state(&mut self, bt_name: &str, state: BtState) {
        for list in self.pairs.values_mut() {
            for entry in list.iter_mut() {
                if entry.bt_name == bt_name {
                    entry.state = state;
                    return;
                }
            }
        }
    }

    /// Get the first Active BT for the given event table.
    /// Returns `None` if no BT exists or if the best candidate is Stale/Error.
    pub fn get_active_bt(&self, event_table: &str) -> Option<&BtEntry> {
        self.pairs
            .get(&event_table.to_lowercase())
            .and_then(|list| {
                list.iter().find(|e| e.state == BtState::Active)
            })
    }

    /// Get the first Refreshing BT for the given event table (also queryable).
    pub fn get_refreshing_bt(&self, event_table: &str) -> Option<&BtEntry> {
        self.pairs
            .get(&event_table.to_lowercase())
            .and_then(|list| {
                list.iter().find(|e| e.state == BtState::Refreshing)
            })
    }

    /// Get an Active or Refreshing BT — both are safe to query.
    pub fn get_queryable_bt(&self, event_table: &str) -> Option<&BtEntry> {
        self.get_active_bt(event_table)
            .or_else(|| self.get_refreshing_bt(event_table))
    }

    /// Returns true if any BT (in any state) exists for the given event table.
    pub fn has_any_bt(&self, event_table: &str) -> bool {
        self.pairs
            .get(&event_table.to_lowercase())
            .map(|v| !v.is_empty())
            .unwrap_or(false)
    }
}

impl Default for BtRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Unit Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(bt_name: &str, event_table: &str, state: BtState) -> BtEntry {
        BtEntry {
            bt_name: bt_name.to_string(),
            event_table: event_table.to_string(),
            user_key: "user_id".to_string(),
            session_timeout_sec: 1800,
            state,
            last_refresh: None,
        }
    }

    // T158: get_active_bt returns None when BT is Stale
    #[test]
    fn test_get_active_bt_returns_none_when_stale() {
        let mut reg = BtRegistry::new();
        reg.register(make_entry("page_sessions", "page_events", BtState::Stale));
        assert!(reg.get_active_bt("page_events").is_none(),
            "Stale BT should not be returned by get_active_bt");
    }

    #[test]
    fn test_get_active_bt_returns_active() {
        let mut reg = BtRegistry::new();
        reg.register(make_entry("page_sessions", "page_events", BtState::Active));
        let bt = reg.get_active_bt("page_events");
        assert!(bt.is_some());
        assert_eq!(bt.unwrap().bt_name, "page_sessions");
    }

    #[test]
    fn test_register_replaces_existing() {
        let mut reg = BtRegistry::new();
        reg.register(make_entry("page_sessions", "page_events", BtState::Building));
        reg.register(make_entry("page_sessions", "page_events", BtState::Active));

        let bts = reg.get_bt_for_table("page_events");
        assert_eq!(bts.len(), 1, "Should replace, not duplicate");
        assert_eq!(bts[0].state, BtState::Active);
    }

    #[test]
    fn test_update_state() {
        let mut reg = BtRegistry::new();
        reg.register(make_entry("page_sessions", "page_events", BtState::Building));
        reg.update_state("page_sessions", BtState::Active);

        let bt = reg.get_active_bt("page_events");
        assert!(bt.is_some());
        assert_eq!(bt.unwrap().state, BtState::Active);
    }

    #[test]
    fn test_get_bt_for_table_case_insensitive() {
        let mut reg = BtRegistry::new();
        reg.register(make_entry("page_sessions", "Page_Events", BtState::Active));
        let bts = reg.get_bt_for_table("page_events");
        assert_eq!(bts.len(), 1);
    }

    #[test]
    fn test_multiple_bts_for_same_table() {
        let mut reg = BtRegistry::new();
        reg.register(make_entry("sessions_v1", "page_events", BtState::Stale));
        reg.register(make_entry("sessions_v2", "page_events", BtState::Active));

        let bts = reg.get_bt_for_table("page_events");
        assert_eq!(bts.len(), 2);
        assert_eq!(reg.get_active_bt("page_events").unwrap().bt_name, "sessions_v2");
    }

    #[test]
    fn test_error_state_not_returned_as_active() {
        let mut reg = BtRegistry::new();
        reg.register(make_entry("page_sessions", "page_events",
            BtState::Error("disk full".to_string())));
        assert!(reg.get_active_bt("page_events").is_none());
    }

    #[test]
    fn test_has_any_bt() {
        let mut reg = BtRegistry::new();
        assert!(!reg.has_any_bt("page_events"));
        reg.register(make_entry("page_sessions", "page_events", BtState::Stale));
        assert!(reg.has_any_bt("page_events"));
    }
}
