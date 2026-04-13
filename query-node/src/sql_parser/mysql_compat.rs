// T089: MySQL 8.0 호환 DDL/DML 처리 — SET, USE, SHOW VARIABLES, Prepared Statement

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

// ─── MySQL 호환 문장 ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MySqlCompatStmt {
    /// USE database
    UseDatabase(String),
    /// SET var = value
    SetVariable { name: String, value: String },
    /// SHOW VARIABLES [LIKE pattern]
    ShowVariables { like_pattern: Option<String> },
    /// SHOW PROCESSLIST
    ShowProcessList,
    /// SHOW STATUS [LIKE pattern]
    ShowStatus { like_pattern: Option<String> },
    /// KILL [QUERY|CONNECTION] id
    Kill { id: u64, query_only: bool },
    /// SELECT @@system_var
    SystemVarQuery { var: String },
}

/// MySQL 호환 문장 파싱 시도 — 해당하면 Some, 아니면 None
pub fn try_parse_mysql_compat(sql: &str) -> Option<MySqlCompatStmt> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper   = trimmed.to_uppercase();

    // USE database
    if upper.starts_with("USE ") {
        let db = trimmed[4..].trim().trim_matches('`').to_string();
        return Some(MySqlCompatStmt::UseDatabase(db));
    }

    // SET [GLOBAL|SESSION|LOCAL] var = value
    if upper.starts_with("SET ") {
        if let Some(stmt) = parse_set_stmt(trimmed) {
            return Some(stmt);
        }
    }

    // SHOW VARIABLES
    if upper.starts_with("SHOW VARIABLES") || upper.starts_with("SHOW GLOBAL VARIABLES") ||
       upper.starts_with("SHOW SESSION VARIABLES")
    {
        let like_pattern = extract_like_pattern(trimmed);
        return Some(MySqlCompatStmt::ShowVariables { like_pattern });
    }

    // SHOW PROCESSLIST
    if upper == "SHOW PROCESSLIST" || upper == "SHOW FULL PROCESSLIST" {
        return Some(MySqlCompatStmt::ShowProcessList);
    }

    // SHOW STATUS
    if upper.starts_with("SHOW STATUS") || upper.starts_with("SHOW GLOBAL STATUS") {
        let like_pattern = extract_like_pattern(trimmed);
        return Some(MySqlCompatStmt::ShowStatus { like_pattern });
    }

    // KILL
    if upper.starts_with("KILL ") {
        let rest  = trimmed[5..].trim();
        let upper_rest = rest.to_uppercase();
        let (id_str, query_only) = if upper_rest.starts_with("QUERY ") {
            (&rest["QUERY ".len()..], true)
        } else if upper_rest.starts_with("CONNECTION ") {
            (&rest["CONNECTION ".len()..], false)
        } else {
            (rest, false)
        };
        if let Ok(id) = id_str.trim().parse::<u64>() {
            return Some(MySqlCompatStmt::Kill { id, query_only });
        }
    }

    // SELECT @@var
    if upper.starts_with("SELECT @@") {
        let var = trimmed[9..].trim()
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_end_matches(';')
            .to_lowercase();
        return Some(MySqlCompatStmt::SystemVarQuery { var });
    }

    None
}

fn parse_set_stmt(sql: &str) -> Option<MySqlCompatStmt> {
    // SET [GLOBAL|SESSION|LOCAL] `var` = 'value' / SET NAMES charset
    let after_set = sql[4..].trim();
    let upper_after = after_set.to_uppercase();

    // Skip GLOBAL/SESSION/LOCAL qualifier
    let after_qualifier = if upper_after.starts_with("GLOBAL ") {
        after_set["GLOBAL ".len()..].trim()
    } else if upper_after.starts_with("SESSION ") {
        after_set["SESSION ".len()..].trim()
    } else if upper_after.starts_with("LOCAL ") {
        after_set["LOCAL ".len()..].trim()
    } else {
        after_set
    };

    // SET NAMES charset
    let upper_q = after_qualifier.to_uppercase();
    if upper_q.starts_with("NAMES ") {
        let charset = after_qualifier["NAMES ".len()..].trim().trim_matches('\'').trim_matches('"').to_string();
        return Some(MySqlCompatStmt::SetVariable {
            name:  "character_set_client".to_string(),
            value: charset,
        });
    }

    // SET var = value
    if let Some(eq_pos) = after_qualifier.find('=') {
        let name = after_qualifier[..eq_pos]
            .trim()
            .trim_matches('`')
            .trim_start_matches('@')
            .to_string();
        let value = after_qualifier[eq_pos + 1..]
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        return Some(MySqlCompatStmt::SetVariable { name, value });
    }

    None
}

fn extract_like_pattern(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let pos   = upper.find("LIKE ")?;
    let rest  = sql[pos + 5..].trim();
    let pat   = rest.trim_matches('\'').trim_matches('"').to_string();
    if pat.is_empty() { None } else { Some(pat) }
}

// ─── 시스템 변수 응답 ─────────────────────────────────────────────────────────

/// MySQL 시스템 변수 값 반환 (클라이언트 초기화에 필요한 변수들)
pub fn get_system_variable(name: &str) -> Option<String> {
    let lower = name.to_lowercase();
    let lower = lower.trim_start_matches("@@")
                     .trim_start_matches("global.")
                     .trim_start_matches("session.");

    Some(match lower {
        "version"                         => "8.0.0-wowdb-1.0".to_string(),
        "version_comment"                 => "WOW-DB 1.0 (Rust/OLAP)".to_string(),
        "version_compile_os"              => "linux".to_string(),
        "character_set_client"            => "utf8mb4".to_string(),
        "character_set_connection"        => "utf8mb4".to_string(),
        "character_set_results"           => "utf8mb4".to_string(),
        "character_set_server"            => "utf8mb4".to_string(),
        "collation_connection"            => "utf8mb4_unicode_ci".to_string(),
        "collation_server"                => "utf8mb4_unicode_ci".to_string(),
        "max_allowed_packet"              => "16777216".to_string(),
        "net_buffer_length"               => "16384".to_string(),
        "net_read_timeout"                => "30".to_string(),
        "net_write_timeout"               => "60".to_string(),
        "lower_case_table_names"          => "0".to_string(),
        "init_connect"                    => String::new(),
        "interactive_timeout"             => "28800".to_string(),
        "wait_timeout"                    => "28800".to_string(),
        "time_zone"                       => "+00:00".to_string(),
        "system_time_zone"                => "UTC".to_string(),
        "auto_increment_increment"        => "1".to_string(),
        "auto_increment_offset"           => "1".to_string(),
        "sql_mode"                        => "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION".to_string(),
        "transaction_isolation"           | "tx_isolation" => "READ-COMMITTED".to_string(),
        "foreign_key_checks"              => "0".to_string(),
        "unique_checks"                   => "0".to_string(),
        _                                 => return None,
    })
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_use() {
        let stmt = try_parse_mysql_compat("USE analytics").unwrap();
        assert_eq!(stmt, MySqlCompatStmt::UseDatabase("analytics".to_string()));
    }

    #[test]
    fn test_parse_set_names() {
        let stmt = try_parse_mysql_compat("SET NAMES utf8mb4").unwrap();
        assert!(matches!(stmt, MySqlCompatStmt::SetVariable { name, .. } if name == "character_set_client"));
    }

    #[test]
    fn test_parse_set_variable() {
        let stmt = try_parse_mysql_compat("SET autocommit = 1").unwrap();
        match stmt {
            MySqlCompatStmt::SetVariable { name, value } => {
                assert_eq!(name,  "autocommit");
                assert_eq!(value, "1");
            }
            _ => panic!("Expected SetVariable"),
        }
    }

    #[test]
    fn test_parse_show_variables() {
        let stmt = try_parse_mysql_compat("SHOW VARIABLES LIKE 'version%'").unwrap();
        match stmt {
            MySqlCompatStmt::ShowVariables { like_pattern: Some(p) } => {
                assert_eq!(p, "version%");
            }
            _ => panic!("Expected ShowVariables with pattern"),
        }
    }

    #[test]
    fn test_parse_kill() {
        let stmt = try_parse_mysql_compat("KILL QUERY 42").unwrap();
        assert_eq!(stmt, MySqlCompatStmt::Kill { id: 42, query_only: true });
    }

    #[test]
    fn test_parse_select_system_var() {
        let stmt = try_parse_mysql_compat("SELECT @@version").unwrap();
        match stmt {
            MySqlCompatStmt::SystemVarQuery { var } => assert_eq!(var, "version"),
            _ => panic!("Expected SystemVarQuery"),
        }
    }

    #[test]
    fn test_get_system_variable() {
        assert!(get_system_variable("version").is_some());
        assert_eq!(get_system_variable("character_set_client").as_deref(), Some("utf8mb4"));
        assert!(get_system_variable("nonexistent_var").is_none());
    }

    #[test]
    fn test_non_mysql_compat() {
        assert!(try_parse_mysql_compat("SELECT * FROM page_events").is_none());
        assert!(try_parse_mysql_compat("CREATE CUBE events (...)").is_none());
    }
}
