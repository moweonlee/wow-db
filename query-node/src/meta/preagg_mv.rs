// T075: CREATE MATERIALIZED VIEW AS SELECT GROUP BY 파싱 및 메타데이터 등록

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

use crate::raft::{RaftCommand, RaftManager};

// ─── Pre-aggregation MV 스키마 ────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreAggMvDef {
    pub mv_id:        String,
    pub mv_name:      String,
    pub source_cube:  String,
    pub group_by:     Vec<String>,
    pub agg_cols:     Vec<AggColumnDef>,
    /// 갱신 방식: INSERT 시점(incremental) 또는 주기적(scheduled)
    pub refresh_mode: MvRefreshMode,
    /// CREATE AS SELECT 원본 SQL
    pub definition_sql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggColumnDef {
    pub alias:    String,
    pub func:     AggFunction,
    pub src_col:  String,
    pub distinct: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CountDistinct,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MvRefreshMode {
    /// INSERT 시점에 증분 갱신
    Incremental,
    /// cron 표현식 기반 주기 갱신
    Scheduled { cron_expr: String },
    /// 수동 갱신
    Manual,
}

// ─── MV Manager ──────────────────────────────────────────────────────────────

pub struct PreAggMvManager {
    raft: Arc<RaftManager>,
}

impl PreAggMvManager {
    pub fn new(raft: Arc<RaftManager>) -> Self {
        Self { raft }
    }

    /// CREATE MATERIALIZED VIEW 등록
    pub async fn create(&self, mv: PreAggMvDef) -> Result<()> {
        let key  = format!("preagg_mv:{}", mv.mv_name);
        let json = serde_json::to_string(&mv)?;

        // 중복 확인
        if self.raft.read(&key).await.is_some() {
            return Err(anyhow!("Pre-agg MV '{}' already exists", mv.mv_name));
        }

        self.raft.write(RaftCommand::UpsertKv { key, value: json }).await?;
        info!(mv = %mv.mv_name, source = %mv.source_cube, "Pre-agg MV registered");
        Ok(())
    }

    /// MV 조회
    pub async fn get(&self, mv_name: &str) -> Result<Option<PreAggMvDef>> {
        let key = format!("preagg_mv:{}", mv_name);
        match self.raft.read(&key).await {
            None       => Ok(None),
            Some(json) => Ok(Some(serde_json::from_str(&json)?)),
        }
    }

    /// 소스 Cube의 모든 MV 목록
    pub async fn list_for_cube(&self, cube_name: &str) -> Vec<PreAggMvDef> {
        self.raft.scan_prefix("preagg_mv:").await
            .into_iter()
            .filter_map(|(_, v)| serde_json::from_str::<PreAggMvDef>(&v).ok())
            .filter(|mv| mv.source_cube == cube_name)
            .collect()
    }

    /// MV 삭제
    pub async fn drop(&self, mv_name: &str) -> Result<()> {
        let key = format!("preagg_mv:{}", mv_name);
        self.raft.write(RaftCommand::DeleteKv { key }).await?;
        info!(mv = %mv_name, "Pre-agg MV dropped");
        Ok(())
    }
}

// ─── CREATE MATERIALIZED VIEW 간이 파서 ─────────────────────────────────────

/// ```sql
/// CREATE MATERIALIZED VIEW mv_name
/// AS SELECT col1, col2, COUNT(*) AS cnt, SUM(col3) AS s
/// FROM source_cube
/// GROUP BY col1, col2
/// [REFRESH INCREMENTAL|SCHEDULED '<cron>'|MANUAL]
/// ```
pub fn parse_create_mv(sql: &str) -> Result<PreAggMvDef> {
    let upper = sql.to_uppercase();

    // MV 이름
    let mv_name = extract_token_after(sql, "VIEW")
        .ok_or_else(|| anyhow!("Expected MV name after VIEW"))?;

    // SOURCE CUBE (FROM 절)
    let source_cube = extract_token_after(sql, "FROM")
        .ok_or_else(|| anyhow!("Expected table name after FROM"))?;

    // GROUP BY
    let group_by = parse_group_by(sql);

    // 집계 함수
    let agg_cols = parse_agg_cols(sql)?;

    // REFRESH MODE
    let refresh_mode = if upper.contains("REFRESH INCREMENTAL") {
        MvRefreshMode::Incremental
    } else if upper.contains("REFRESH SCHEDULED") {
        let cron = extract_quoted_after(sql, "SCHEDULED")
            .unwrap_or_else(|| "0 * * * *".to_string());
        MvRefreshMode::Scheduled { cron_expr: cron }
    } else {
        MvRefreshMode::Manual
    };

    Ok(PreAggMvDef {
        mv_id:          Uuid::new_v4().to_string(),
        mv_name,
        source_cube,
        group_by,
        agg_cols,
        refresh_mode,
        definition_sql: sql.to_string(),
    })
}

fn extract_token_after(sql: &str, keyword: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let kw    = keyword.to_uppercase();
    let pos   = upper.find(&kw)?;
    let rest  = &sql[pos + keyword.len()..];
    let token = rest.split_whitespace().next()?
        .trim_end_matches(';')
        .trim_matches('`')
        .trim_matches('"')
        .to_string();
    if token.is_empty() { None } else { Some(token) }
}

fn extract_quoted_after(sql: &str, keyword: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let kw    = keyword.to_uppercase();
    let pos   = upper.find(&kw)?;
    let rest  = &sql[pos + keyword.len()..].trim_start();
    let quote = rest.chars().next()?;
    if quote == '\'' || quote == '"' {
        let inner = &rest[1..];
        let end   = inner.find(quote)?;
        Some(inner[..end].to_string())
    } else {
        None
    }
}

fn parse_group_by(sql: &str) -> Vec<String> {
    let upper = sql.to_uppercase();
    let pos   = upper.find("GROUP BY").unwrap_or(usize::MAX);
    if pos == usize::MAX { return Vec::new(); }

    let rest = &sql[pos + 8..];
    // GROUP BY 이후 첫 줄 또는 끝까지
    let end = rest.to_uppercase()
        .find('\n')
        .or_else(|| rest.len().into())
        .unwrap_or(rest.len());

    rest[..end].split(',')
        .map(|s| s.trim().trim_matches('`').to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn parse_agg_cols(sql: &str) -> Result<Vec<AggColumnDef>> {
    let upper = sql.to_uppercase();
    // SELECT와 FROM 사이 추출
    let sel_start = upper.find("SELECT").map(|p| p + 6).unwrap_or(0);
    let from_pos  = upper.find("FROM").unwrap_or(sql.len());
    let select_clause = &sql[sel_start..from_pos];

    let mut agg_cols = Vec::new();

    // 각 SELECT 항목 파싱
    for item in split_top_level_commas(select_clause) {
        let item = item.trim();
        if item.is_empty() { continue; }
        let item_upper = item.to_uppercase();

        // ALIAS 추출
        let (expr, alias) = if item_upper.contains(" AS ") {
            let pos = item_upper.find(" AS ").unwrap();
            (item[..pos].trim().to_string(), item[pos+4..].trim().to_string())
        } else {
            (item.to_string(), item.to_string())
        };

        let expr_upper = expr.to_uppercase();

        // 집계 함수 감지
        let func_and_col: Option<(AggFunction, String, bool)> =
            if expr_upper.starts_with("COUNT(DISTINCT") || expr_upper.starts_with("COUNT(DISTINCT ") {
                let col = extract_between(&expr, "(", ")")
                    .map(|s| s.to_uppercase().trim_start_matches("DISTINCT").trim().to_string())
                    .unwrap_or_else(|| "*".to_string());
                Some((AggFunction::CountDistinct, col, true))
            } else if expr_upper.starts_with("COUNT(") {
                let col = extract_between(&expr, "(", ")").unwrap_or_else(|| "*".to_string());
                Some((AggFunction::Count, col.trim().to_string(), false))
            } else if expr_upper.starts_with("SUM(") {
                let col = extract_between(&expr, "(", ")").unwrap_or_default();
                Some((AggFunction::Sum, col.trim().to_string(), false))
            } else if expr_upper.starts_with("AVG(") {
                let col = extract_between(&expr, "(", ")").unwrap_or_default();
                Some((AggFunction::Avg, col.trim().to_string(), false))
            } else if expr_upper.starts_with("MIN(") {
                let col = extract_between(&expr, "(", ")").unwrap_or_default();
                Some((AggFunction::Min, col.trim().to_string(), false))
            } else if expr_upper.starts_with("MAX(") {
                let col = extract_between(&expr, "(", ")").unwrap_or_default();
                Some((AggFunction::Max, col.trim().to_string(), false))
            } else {
                None
            };

        if let Some((func, col, distinct)) = func_and_col {
            agg_cols.push(AggColumnDef {
                alias:    alias.trim_matches('`').to_string(),
                func,
                src_col:  col,
                distinct,
            });
        }
    }

    Ok(agg_cols)
}

fn extract_between(s: &str, open: &str, close: &str) -> Option<String> {
    let su = s.to_uppercase();
    let ou = open.to_uppercase();
    let cu = close.to_uppercase();
    let start = su.find(&ou)? + open.len();
    let end   = su[start..].rfind(&cu)? + start;
    Some(s[start..end].to_string())
}

fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut parts   = Vec::new();
    let mut current = String::new();
    let mut depth   = 0i32;
    for c in s.chars() {
        match c {
            '(' | '[' => { depth += 1; current.push(c); }
            ')' | ']' => { depth -= 1; current.push(c); }
            ',' if depth == 0 => { parts.push(current.trim().to_string()); current.clear(); }
            _ => current.push(c),
        }
    }
    if !current.trim().is_empty() { parts.push(current.trim().to_string()); }
    parts
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_mv() {
        let sql = r#"
            CREATE MATERIALIZED VIEW daily_summary
            AS SELECT
                event_name,
                device_id,
                COUNT(*) AS event_count,
                SUM(revenue) AS total_revenue,
                MIN(event_time) AS first_event
            FROM page_events
            GROUP BY event_name, device_id
            REFRESH INCREMENTAL
        "#;

        let mv = parse_create_mv(sql).unwrap();
        assert_eq!(mv.mv_name, "daily_summary");
        assert_eq!(mv.source_cube, "page_events");
        assert_eq!(mv.group_by, vec!["event_name", "device_id"]);
        assert_eq!(mv.agg_cols.len(), 3);
        assert_eq!(mv.agg_cols[0].func, AggFunction::Count);
        assert_eq!(mv.agg_cols[0].alias, "event_count");
        assert_eq!(mv.agg_cols[1].func, AggFunction::Sum);
        assert_eq!(mv.refresh_mode, MvRefreshMode::Incremental);
    }

    #[test]
    fn test_parse_mv_scheduled() {
        let sql = r#"
            CREATE MATERIALIZED VIEW hourly_agg
            AS SELECT COUNT(*) AS cnt
            FROM events
            GROUP BY event_name
            REFRESH SCHEDULED '0 * * * *'
        "#;

        let mv = parse_create_mv(sql).unwrap();
        assert_eq!(
            mv.refresh_mode,
            MvRefreshMode::Scheduled { cron_expr: "0 * * * *".to_string() }
        );
    }

    #[tokio::test]
    async fn test_mv_manager_lifecycle() {
        let raft = Arc::new(crate::raft::RaftManager::new_local());
        let mgr  = PreAggMvManager::new(raft);

        let mv = PreAggMvDef {
            mv_id:          "test-id".to_string(),
            mv_name:        "test_mv".to_string(),
            source_cube:    "events".to_string(),
            group_by:       vec!["event_name".to_string()],
            agg_cols:       vec![],
            refresh_mode:   MvRefreshMode::Manual,
            definition_sql: "SELECT ...".to_string(),
        };

        mgr.create(mv.clone()).await.unwrap();
        let got = mgr.get("test_mv").await.unwrap().unwrap();
        assert_eq!(got.mv_name, "test_mv");

        let for_cube = mgr.list_for_cube("events").await;
        assert_eq!(for_cube.len(), 1);

        mgr.drop("test_mv").await.unwrap();
        assert!(mgr.get("test_mv").await.unwrap().is_none());
    }
}
