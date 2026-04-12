// T034: sqlparser-rs MySQL 방언 파서 통합
// WOW-DB 커스텀 AST 노드 뼈대 정의 (FUNNEL, COHORT, PATH, CREATE CUBE, ROUTINE LOAD)

pub mod analytics;

use anyhow::{bail, Result};
use sqlparser::{
    ast::{Expr, Query, Statement},
    dialect::MySqlDialect,
    parser::Parser,
};
use tracing::debug;

// ─── WOW-DB 커스텀 AST ────────────────────────────────────────────────────────

/// WOW-DB 전용 확장 문장 (표준 SQL에 없는 DDL/DML)
#[derive(Debug, Clone)]
pub enum WowDbStatement {
    /// 표준 SQL 문 (sqlparser AST 그대로 사용)
    Standard(Statement),
    /// WOW-DB 커스텀 문
    Custom(WowDbCustom),
}

/// WOW-DB 커스텀 DDL/DML
#[derive(Debug, Clone)]
pub enum WowDbCustom {
    /// CREATE CUBE
    CreateCube(CreateCubeStmt),
    /// ALTER CUBE
    AlterCube(AlterCubeStmt),
    /// DROP CUBE
    DropCube(DropCubeStmt),
    /// CREATE SESSION MATERIALIZED VIEW
    CreateSmv(CreateSmvStmt),
    /// CREATE ROUTINE LOAD
    CreateRoutineLoad(CreateRoutineLoadStmt),
    /// PAUSE ROUTINE LOAD
    PauseRoutineLoad { job_name: String },
    /// RESUME ROUTINE LOAD
    ResumeRoutineLoad { job_name: String },
    /// STOP ROUTINE LOAD
    StopRoutineLoad { job_name: String },
}

// ─── CREATE CUBE ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreateCubeStmt {
    pub name:         String,
    pub database:     Option<String>,
    pub columns:      Vec<CubeColumnDef>,
    pub partition_by: Option<CubePartitionBy>,
    pub order_by:     Vec<String>,
    pub distribution: Option<CubeDistribution>,
    pub properties:   Vec<(String, String)>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CubeColumnDef {
    pub name:       String,
    pub data_type:  String,
    pub nullable:   bool,
    pub encoding:   Option<String>,
    pub comment:    Option<String>,
}

#[derive(Debug, Clone)]
pub struct CubePartitionBy {
    pub columns:     Vec<String>,
    pub granularity: String, // "DAY" | "MONTH" | "YEAR"
    pub auto:        bool,
}

#[derive(Debug, Clone)]
pub struct CubeDistribution {
    pub columns: Vec<String>,
    pub buckets: u32,
}

// ─── ALTER CUBE ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AlterCubeStmt {
    pub name:   String,
    pub action: AlterCubeAction,
}

#[derive(Debug, Clone)]
pub enum AlterCubeAction {
    AddColumn(CubeColumnDef),
    DropColumn(String),
    AddIndex { name: String, columns: Vec<String>, index_type: String },
    DropIndex(String),
    ModifyTtl { days: u32 },
}

// ─── DROP CUBE ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DropCubeStmt {
    pub name:     String,
    pub if_exists: bool,
}

// ─── CREATE SESSION MATERIALIZED VIEW ────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreateSmvStmt {
    pub name:            String,
    pub source_cube:     String,
    pub user_key:        String,
    pub session_timeout: u64,  // 초 단위
    pub refresh_mode:    SmvRefreshMode,
}

#[derive(Debug, Clone)]
pub enum SmvRefreshMode {
    Async,
    Manual,
    Scheduled { cron_expr: String },
}

// ─── ROUTINE LOAD ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreateRoutineLoadStmt {
    pub job_name:    String,
    pub cube_name:   String,
    pub kafka_topic: String,
    pub brokers:     Vec<String>,
    pub format:      String, // "JSON" | "AVRO"
    pub properties:  Vec<(String, String)>,
}

// ─── 파서 ─────────────────────────────────────────────────────────────────────

pub struct WowDbParser;

impl WowDbParser {
    /// SQL 문자열을 WowDbStatement 목록으로 파싱
    pub fn parse(sql: &str) -> Result<Vec<WowDbStatement>> {
        let sql_trimmed = sql.trim();

        // WOW-DB 전용 키워드로 시작하면 커스텀 파서 시도
        let upper = sql_trimmed.to_uppercase();
        if upper.starts_with("CREATE CUBE")
            || upper.starts_with("ALTER CUBE")
            || upper.starts_with("DROP CUBE")
            || upper.starts_with("CREATE SESSION MATERIALIZED VIEW")
            || upper.starts_with("CREATE ROUTINE LOAD")
            || upper.starts_with("PAUSE ROUTINE LOAD")
            || upper.starts_with("RESUME ROUTINE LOAD")
            || upper.starts_with("STOP ROUTINE LOAD")
        {
            let custom = Self::parse_custom(sql_trimmed)?;
            return Ok(vec![WowDbStatement::Custom(custom)]);
        }

        // FUNNEL_COUNT / COHORT_ANALYSIS / PATH_ANALYSIS 를 포함하는 SELECT
        // → sqlparser가 파싱한 AST를 그대로 반환 (analytics.rs에서 추가 분석)
        let dialect  = MySqlDialect {};
        let stmts    = Parser::parse_sql(&dialect, sql_trimmed)?;
        debug!(count = stmts.len(), "sqlparser-rs 파싱 완료");

        Ok(stmts
            .into_iter()
            .map(WowDbStatement::Standard)
            .collect())
    }

    // ─── 커스텀 키워드 파서 (간이 파서 — Phase D에서 완전 구현) ──────────────

    fn parse_custom(sql: &str) -> Result<WowDbCustom> {
        let upper = sql.to_uppercase();

        if upper.starts_with("CREATE CUBE") {
            return Ok(WowDbCustom::CreateCube(Self::parse_create_cube(sql)?));
        }
        if upper.starts_with("DROP CUBE") {
            let if_exists = upper.contains("IF EXISTS");
            let name = Self::extract_token_after(sql, if if_exists { "EXISTS" } else { "CUBE" })?;
            return Ok(WowDbCustom::DropCube(DropCubeStmt { name, if_exists }));
        }
        if upper.starts_with("CREATE SESSION MATERIALIZED VIEW") {
            return Ok(WowDbCustom::CreateSmv(Self::parse_create_smv(sql)?));
        }
        if upper.starts_with("PAUSE ROUTINE LOAD") {
            let job = Self::extract_token_after(sql, "LOAD")?;
            return Ok(WowDbCustom::PauseRoutineLoad { job_name: job });
        }
        if upper.starts_with("RESUME ROUTINE LOAD") {
            let job = Self::extract_token_after(sql, "LOAD")?;
            return Ok(WowDbCustom::ResumeRoutineLoad { job_name: job });
        }
        if upper.starts_with("STOP ROUTINE LOAD") {
            let job = Self::extract_token_after(sql, "LOAD")?;
            return Ok(WowDbCustom::StopRoutineLoad { job_name: job });
        }

        bail!("Unknown WOW-DB custom statement: {}", &sql[..sql.len().min(60)])
    }

    /// 간이 CREATE CUBE 파서 (완전 구현은 Phase D)
    fn parse_create_cube(sql: &str) -> Result<CreateCubeStmt> {
        let if_not_exists = sql.to_uppercase().contains("IF NOT EXISTS");
        let name = if if_not_exists {
            Self::extract_token_after(sql, "EXISTS")?
        } else {
            Self::extract_token_after(sql, "CUBE")?
        };
        Ok(CreateCubeStmt {
            name,
            database:     None,
            columns:      vec![],
            partition_by: None,
            order_by:     vec![],
            distribution: None,
            properties:   vec![],
            if_not_exists,
        })
    }

    /// 간이 CREATE SESSION MATERIALIZED VIEW 파서
    fn parse_create_smv(sql: &str) -> Result<CreateSmvStmt> {
        let name = Self::extract_token_after(sql, "VIEW")?;
        Ok(CreateSmvStmt {
            name,
            source_cube:     String::new(),
            user_key:        String::new(),
            session_timeout: 1800, // 기본 30분
            refresh_mode:    SmvRefreshMode::Async,
        })
    }

    /// 지정 키워드 다음 토큰 추출 (공백 구분)
    fn extract_token_after(sql: &str, keyword: &str) -> Result<String> {
        let upper = sql.to_uppercase();
        let pos   = upper
            .find(keyword)
            .ok_or_else(|| anyhow::anyhow!("Keyword '{}' not found in SQL", keyword))?;
        let rest  = &sql[pos + keyword.len()..];
        let token = rest
            .split_whitespace()
            .next()
            .ok_or_else(|| anyhow::anyhow!("Expected token after '{}'", keyword))?
            .trim_end_matches(';')
            .trim_matches('`')
            .trim_matches('"')
            .to_string();
        Ok(token)
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_standard_select() {
        let stmts = WowDbParser::parse("SELECT 1").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], WowDbStatement::Standard(_)));
    }

    #[test]
    fn test_parse_create_cube() {
        let sql = "CREATE CUBE IF NOT EXISTS page_events (event_time DATETIME)";
        let stmts = WowDbParser::parse(sql).unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            WowDbStatement::Custom(WowDbCustom::CreateCube(s)) => {
                assert_eq!(s.name, "page_events");
                assert!(s.if_not_exists);
            }
            other => panic!("Expected CreateCube, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_drop_cube() {
        let sql = "DROP CUBE IF EXISTS old_cube";
        let stmts = WowDbParser::parse(sql).unwrap();
        assert!(matches!(
            &stmts[0],
            WowDbStatement::Custom(WowDbCustom::DropCube(s)) if s.name == "old_cube" && s.if_exists
        ));
    }

    #[test]
    fn test_parse_routine_load() {
        let sql = "STOP ROUTINE LOAD my_job";
        let stmts = WowDbParser::parse(sql).unwrap();
        assert!(matches!(
            &stmts[0],
            WowDbStatement::Custom(WowDbCustom::StopRoutineLoad { job_name }) if job_name == "my_job"
        ));
    }
}
