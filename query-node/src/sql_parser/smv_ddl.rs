// T079: CREATE SESSION MATERIALIZED VIEW 문법 파싱 — FROM, USER KEY, SESSION TIMEOUT, REFRESH

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

// ─── SMV AST ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSmvStmt {
    pub mv_name:         String,
    pub source_cube:     String,
    pub user_key_col:    String,
    pub session_timeout: SessionTimeout,
    pub refresh_mode:    SmvRefreshMode,
    pub if_not_exists:   bool,
    /// QN이 자동 생성할 SMV 이름 제안 여부
    pub auto_name:       bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionTimeout {
    pub seconds: u64,
}

impl SessionTimeout {
    pub fn minutes(m: u64) -> Self { Self { seconds: m * 60 } }
    pub fn hours(h: u64)   -> Self { Self { seconds: h * 3600 } }

    pub fn as_minutes(&self) -> u64 { self.seconds / 60 }
}

impl Default for SessionTimeout {
    fn default() -> Self { Self::minutes(30) }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SmvRefreshMode {
    /// INSERT 시점 증분 갱신
    Incremental,
    /// 수동 트리거
    Manual,
    /// cron 기반 주기 갱신
    Scheduled { cron_expr: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropSmvStmt {
    pub mv_name:   String,
    pub if_exists: bool,
}

// ─── 파서 ─────────────────────────────────────────────────────────────────────

/// ```sql
/// CREATE SESSION MATERIALIZED VIEW [IF NOT EXISTS] mv_name
/// FROM source_cube
/// USER KEY user_key_col
/// SESSION TIMEOUT [N MINUTES | N HOURS | N SECONDS]
/// [REFRESH INCREMENTAL | MANUAL | SCHEDULED 'cron_expr']
/// ```
pub fn parse_create_smv(sql: &str) -> Result<CreateSmvStmt> {
    let upper = sql.to_uppercase();

    let if_not_exists = upper.contains("IF NOT EXISTS");

    // MV 이름 — VIEW 다음 토큰 (IF NOT EXISTS 스킵)
    let mv_name = if if_not_exists {
        extract_token_after_keyword(sql, "EXISTS")
            .ok_or_else(|| anyhow!("Expected MV name after EXISTS"))?
    } else {
        extract_token_after_keyword(sql, "VIEW")
            .ok_or_else(|| anyhow!("Expected MV name after VIEW"))?
    };

    // FROM 절
    let source_cube = extract_token_after_keyword(sql, "FROM")
        .ok_or_else(|| anyhow!("Expected source cube name after FROM"))?;

    // USER KEY 절
    let user_key_col = extract_token_after_keyword(sql, "KEY")
        .ok_or_else(|| anyhow!("Expected column name after USER KEY"))?;

    // SESSION TIMEOUT 절
    let session_timeout = parse_session_timeout(sql).unwrap_or_default();

    // REFRESH 절
    let refresh_mode = parse_smv_refresh(sql);

    Ok(CreateSmvStmt {
        mv_name,
        source_cube,
        user_key_col,
        session_timeout,
        refresh_mode,
        if_not_exists,
        auto_name: false,
    })
}

/// DROP SESSION MATERIALIZED VIEW [IF EXISTS] mv_name
pub fn parse_drop_smv(sql: &str) -> Result<DropSmvStmt> {
    let upper = sql.to_uppercase();
    let if_exists = upper.contains("IF EXISTS");

    let mv_name = if if_exists {
        extract_token_after_keyword(sql, "EXISTS")
            .ok_or_else(|| anyhow!("Expected MV name after EXISTS"))?
    } else {
        extract_token_after_keyword(sql, "VIEW")
            .ok_or_else(|| anyhow!("Expected MV name after VIEW"))?
    };

    Ok(DropSmvStmt { mv_name, if_exists })
}

// ─── 헬퍼 ─────────────────────────────────────────────────────────────────────

fn extract_token_after_keyword(sql: &str, keyword: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let kw    = keyword.to_uppercase();
    let pos   = upper.find(&kw)?;
    let rest  = &sql[pos + keyword.len()..];
    let tok   = rest.split_whitespace().next()?
        .trim_end_matches(';')
        .trim_matches('`')
        .trim_matches('"')
        .to_string();
    if tok.is_empty() { None } else { Some(tok) }
}

/// SESSION TIMEOUT 절 파싱
/// - `SESSION TIMEOUT 30 MINUTES`
/// - `SESSION TIMEOUT 1 HOUR`
/// - `SESSION TIMEOUT 1800 SECONDS`
fn parse_session_timeout(sql: &str) -> Option<SessionTimeout> {
    let upper = sql.to_uppercase();
    let pos = upper.find("SESSION TIMEOUT")?;
    let rest = &sql[pos + "SESSION TIMEOUT".len()..].trim_start();

    let mut parts = rest.split_whitespace();
    let n: u64 = parts.next()?.parse().ok()?;
    let unit = parts.next().unwrap_or("MINUTES").to_uppercase();

    Some(match unit.as_str() {
        "SECOND" | "SECONDS" | "SEC" => SessionTimeout { seconds: n },
        "HOUR" | "HOURS"             => SessionTimeout::hours(n),
        _                            => SessionTimeout::minutes(n),  // MINUTES default
    })
}

/// REFRESH 절 파싱
fn parse_smv_refresh(sql: &str) -> SmvRefreshMode {
    let upper = sql.to_uppercase();
    if upper.contains("REFRESH INCREMENTAL") {
        SmvRefreshMode::Incremental
    } else if upper.contains("REFRESH SCHEDULED") {
        let cron = extract_quoted_after(sql, "SCHEDULED")
            .unwrap_or_else(|| "0 * * * *".to_string());
        SmvRefreshMode::Scheduled { cron_expr: cron }
    } else if upper.contains("REFRESH MANUAL") {
        SmvRefreshMode::Manual
    } else {
        SmvRefreshMode::Incremental  // 기본값
    }
}

fn extract_quoted_after(sql: &str, keyword: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let kw    = keyword.to_uppercase();
    let pos   = upper.find(&kw)?;
    let rest  = sql[pos + keyword.len()..].trim_start();
    let quote = rest.chars().next()?;
    if quote == '\'' || quote == '"' {
        let inner = &rest[1..];
        let end   = inner.find(quote)?;
        Some(inner[..end].to_string())
    } else {
        None
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_smv_basic() {
        let sql = r#"
            CREATE SESSION MATERIALIZED VIEW page_events_sessions
            FROM page_events
            USER KEY device_id
            SESSION TIMEOUT 30 MINUTES
            REFRESH INCREMENTAL
        "#;
        let stmt = parse_create_smv(sql).unwrap();
        assert_eq!(stmt.mv_name,      "page_events_sessions");
        assert_eq!(stmt.source_cube,  "page_events");
        assert_eq!(stmt.user_key_col, "device_id");
        assert_eq!(stmt.session_timeout.seconds, 1800);
        assert_eq!(stmt.refresh_mode, SmvRefreshMode::Incremental);
        assert!(!stmt.if_not_exists);
    }

    #[test]
    fn test_parse_create_smv_if_not_exists() {
        let sql = r#"
            CREATE SESSION MATERIALIZED VIEW IF NOT EXISTS events_smv
            FROM events
            USER KEY user_id
            SESSION TIMEOUT 1 HOUR
        "#;
        let stmt = parse_create_smv(sql).unwrap();
        assert!(stmt.if_not_exists);
        assert_eq!(stmt.mv_name,      "events_smv");
        assert_eq!(stmt.session_timeout.seconds, 3600);
    }

    #[test]
    fn test_parse_create_smv_scheduled() {
        let sql = r#"
            CREATE SESSION MATERIALIZED VIEW hourly_sessions
            FROM events
            USER KEY device_id
            SESSION TIMEOUT 5 MINUTES
            REFRESH SCHEDULED '0 * * * *'
        "#;
        let stmt = parse_create_smv(sql).unwrap();
        assert_eq!(
            stmt.refresh_mode,
            SmvRefreshMode::Scheduled { cron_expr: "0 * * * *".to_string() }
        );
    }

    #[test]
    fn test_parse_create_smv_seconds_unit() {
        let sql = r#"
            CREATE SESSION MATERIALIZED VIEW mv
            FROM cube
            USER KEY uid
            SESSION TIMEOUT 900 SECONDS
        "#;
        let stmt = parse_create_smv(sql).unwrap();
        assert_eq!(stmt.session_timeout.seconds, 900);
    }

    #[test]
    fn test_session_timeout_helpers() {
        assert_eq!(SessionTimeout::minutes(30).seconds, 1800);
        assert_eq!(SessionTimeout::hours(2).seconds, 7200);
        assert_eq!(SessionTimeout::minutes(30).as_minutes(), 30);
    }

    #[test]
    fn test_parse_drop_smv() {
        let sql = "DROP SESSION MATERIALIZED VIEW IF EXISTS old_sessions";
        let stmt = parse_drop_smv(sql).unwrap();
        assert_eq!(stmt.mv_name, "old_sessions");
        assert!(stmt.if_exists);
    }

    #[test]
    fn test_parse_drop_smv_no_if_exists() {
        let sql = "DROP SESSION MATERIALIZED VIEW my_smv";
        let stmt = parse_drop_smv(sql).unwrap();
        assert_eq!(stmt.mv_name, "my_smv");
        assert!(!stmt.if_exists);
    }
}
