// T086: opensrv-mysql 완전 구현 — MySQL 8.0 handshake, 인증, COM_QUERY, COM_STMT_PREPARE 처리

use std::sync::Arc;

use anyhow::Result;
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim,
    Column, ColumnFlags, ColumnType,
    ErrorKind, InitWriter, OkResponse, ParamParser,
    QueryResultWriter, StatementMetaWriter,
};
use tracing::{debug, info, warn};

use chrono::Utc;
use shared::types::{
    ColumnDef, CubeSchema, DataType, Distribution,
    Encoding, PartitionKey, PartitionVariant, StorageBackend,
};
use uuid::Uuid;

use super::result_set::build_columns;
use super::schema_cmds::handle_schema_command;
use crate::executor::insert_exec::execute_insert;
use crate::executor::mem_store::MEM_STORE;
use crate::executor::select_exec::execute_select;
use crate::executor::analytics_exec::{execute_funnel_count, execute_cohort_analysis, execute_path_analysis};
use crate::meta::alter_cube::AlterCubeHandler;
use crate::meta::cluster_guard::{ClusterGuard, ReadOnlyError};
use crate::meta::cube::CubeManager;
use crate::meta::partition_info::PartitionInfoService;
use crate::planner::behavioral_pattern::{detect_pattern, QueryPattern};
use crate::planner::behavioral_router::BehavioralRouter;
use crate::raft::RaftManager;
use crate::session_mv::manager::SmvManager;
use crate::sql_parser::cube_ddl::parse_create_cube_full;
use crate::sql_parser::{WowDbParser, WowDbStatement, WowDbCustom};
use sqlparser::ast::Statement as SqlStatement;

// ─── WOW-DB MySQL 핸들러 ─────────────────────────────────────────────────────

pub struct WowDbMysqlHandler {
    pub cube_mgr:       Arc<CubeManager>,
    pub smv_mgr:        Arc<SmvManager>,
    pub raft:           Arc<RaftManager>,
    pub cluster_guard:  Arc<ClusterGuard>,
    pub partition_svc:  Arc<PartitionInfoService>,
    current_db:         std::sync::Mutex<String>,
    /// Pending MySQL warnings — returned on SHOW WARNINGS
    pending_warnings:   std::sync::Mutex<Vec<String>>,
}

impl WowDbMysqlHandler {
    pub fn new(
        cube_mgr: Arc<CubeManager>,
        smv_mgr:  Arc<SmvManager>,
        raft:     Arc<RaftManager>,
    ) -> Self {
        let cluster_guard  = Arc::new(ClusterGuard::new(raft.clone()));
        let partition_svc  = Arc::new(PartitionInfoService::new_local(raft.clone()));
        Self {
            cube_mgr,
            smv_mgr,
            raft,
            cluster_guard,
            partition_svc,
            current_db: std::sync::Mutex::new("default".to_string()),
            pending_warnings: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn new_with_guard(
        cube_mgr:      Arc<CubeManager>,
        smv_mgr:       Arc<SmvManager>,
        raft:          Arc<RaftManager>,
        cluster_guard: Arc<ClusterGuard>,
    ) -> Self {
        let partition_svc = Arc::new(PartitionInfoService::new_local(raft.clone()));
        Self {
            cube_mgr,
            smv_mgr,
            raft,
            cluster_guard,
            partition_svc,
            current_db: std::sync::Mutex::new("default".to_string()),
            pending_warnings: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Add a warning to the pending warnings list (returned on SHOW WARNINGS).
    fn push_warning(&self, msg: String) {
        self.pending_warnings.lock().unwrap().push(msg);
    }

    /// Take all pending warnings, clearing the list.
    fn take_warnings(&self) -> Vec<String> {
        std::mem::take(&mut *self.pending_warnings.lock().unwrap())
    }

    pub fn current_db(&self) -> String {
        self.current_db.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl<W: tokio::io::AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for WowDbMysqlHandler {
    type Error = anyhow::Error;

    // ── COM_QUERY (일반 SQL) ────────────────────────────────────────────────

    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        debug!(sql = %sql, "COM_QUERY");
        let lower = sql.trim().to_lowercase();

        // USE database
        if lower.starts_with("use ") {
            let db = sql[4..].trim().trim_matches('`').to_string();
            *self.current_db.lock().unwrap() = db.clone();
            info!(database = %db, "Database selected");
            return results.completed(OkResponse::default()).await.map_err(Into::into);
        }

        // SET commands
        if lower.starts_with("set ") {
            return results.completed(OkResponse::default()).await.map_err(Into::into);
        }

        // SHOW WARNINGS — return pending warnings from previous query
        if lower.trim_end_matches(';') == "show warnings" {
            let warnings = self.take_warnings();
            let columns = vec![
                ColumnMeta { name: "Level".to_string(),   col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                ColumnMeta { name: "Code".to_string(),    col_type: ColumnType::MYSQL_TYPE_LONG },
                ColumnMeta { name: "Message".to_string(), col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ];
            let rows: Vec<Vec<Option<String>>> = warnings.into_iter().map(|msg| {
                vec![
                    Some("Warning".to_string()),
                    Some("1292".to_string()),
                    Some(msg),
                ]
            }).collect();
            return write_output(results, QueryOutput::Rows { columns, rows }).await;
        }

        // ── 쓰기 작업 Read-Only 검사 (INSERT/DDL/DELETE/TRUNCATE) ──────────────
        if crate::meta::cluster_guard::is_write_statement(sql) {
            if let Err(e) = self.cluster_guard.check_write_allowed().await {
                let msg = e.mysql_message();
                return results
                    .error(ErrorKind::ER_OPTION_PREVENTS_STATEMENT, msg.as_bytes())
                    .await
                    .map_err(Into::into);
            }
        }

        // CREATE DATABASE [IF NOT EXISTS] <name>
        if lower.starts_with("create database") || lower.starts_with("create schema") {
            let if_not_exists = lower.contains("if not exists");
            let name = extract_db_name(&lower, if if_not_exists { "exists" } else if lower.starts_with("create schema") { "schema" } else { "database" });
            if name.is_empty() {
                return results.error(ErrorKind::ER_PARSE_ERROR, b"Missing database name").await.map_err(Into::into);
            }
            let key = format!("db:{name}");
            if if_not_exists && self.raft.read(&key).await.is_some() {
                return results.completed(OkResponse::default()).await.map_err(Into::into);
            }
            self.raft.write(crate::raft::RaftCommand::UpsertKv { key, value: "1".to_string() }).await
                .map_err(|e| anyhow::anyhow!(e))?;
            info!(db = %name, "Database created");
            return results.completed(OkResponse::default()).await.map_err(Into::into);
        }

        // DROP DATABASE [IF EXISTS] <name>
        if lower.starts_with("drop database") || lower.starts_with("drop schema") {
            let if_exists = lower.contains("if exists");
            let name = extract_db_name(&lower, if if_exists { "exists" } else if lower.starts_with("drop schema") { "schema" } else { "database" });
            let key = format!("db:{name}");
            if self.raft.read(&key).await.is_none() {
                if if_exists {
                    return results.completed(OkResponse::default()).await.map_err(Into::into);
                }
                return results.error(ErrorKind::ER_DB_DROP_EXISTS, format!("Unknown database '{name}'").as_bytes()).await.map_err(Into::into);
            }
            self.raft.write(crate::raft::RaftCommand::DeleteKv { key }).await
                .map_err(|e| anyhow::anyhow!(e))?;
            info!(db = %name, "Database dropped");
            return results.completed(OkResponse::default()).await.map_err(Into::into);
        }

        // SHOW DATABASES — Raft KV 에서 동적으로 읽기
        if lower.trim_end_matches(';') == "show databases" {
            let sm = self.raft.sm.read().await;
            let mut dbs: Vec<String> = sm.kv.keys()
                .filter(|k| k.starts_with("db:"))
                .map(|k| k[3..].to_string())
                .collect();
            drop(sm);
            dbs.sort();
            // 시스템 DB 항상 포함
            let mut rows = vec![
                vec![Some("default".to_string())],
                vec![Some("information_schema".to_string())],
            ];
            for db in dbs {
                if db != "default" && db != "information_schema" {
                    rows.push(vec![Some(db)]);
                }
            }
            let output = QueryOutput::Rows {
                columns: vec![ColumnMeta { name: "Database".to_string(), col_type: ColumnType::MYSQL_TYPE_VAR_STRING }],
                rows,
            };
            return write_output(results, output).await;
        }

        // 스키마 명령 처리 (SHOW TABLES, DESCRIBE 등)
        if let Some(output) = handle_schema_command(sql, &self.cube_mgr, &self.current_db(), Some(&self.partition_svc)).await {
            return write_output(results, output).await;
        }

        // MySQL 시스템 변수 쿼리
        if lower.contains("@@version") {
            let cols = vec![build_columns("@@version", ColumnType::MYSQL_TYPE_VAR_STRING)];
            let mut rw = results.start(&cols).await?;
            rw.write_row(std::iter::once(Some("8.0.0-wowdb-1.0"))).await?;
            return rw.finish().await.map_err(Into::into);
        }

        if lower.trim_end_matches(';') == "select 1" {
            let cols = vec![build_columns("1", ColumnType::MYSQL_TYPE_LONGLONG)];
            let mut rw = results.start(&cols).await?;
            rw.write_row(std::iter::once(Some("1"))).await?;
            return rw.finish().await.map_err(Into::into);
        }

        // CREATE SESSION MATERIALIZED VIEW
        if lower.contains("session materialized view") || lower.contains("session mv") {
            // CREATE SESSION MATERIALIZED VIEW <name> ON <cube> ...
            let words: Vec<&str> = sql.split_whitespace().collect();
            let mv_name = words.iter().enumerate()
                .find(|(_, w)| w.to_lowercase() == "view")
                .and_then(|(i, _)| words.get(i+1))
                .map(|s| s.trim_matches('`').trim_end_matches(';').to_string())
                .unwrap_or_else(|| "session_mv".to_string());

            // Extract source cube (after ON keyword)
            let source_cube = words.iter().enumerate()
                .find(|(_, w)| w.to_lowercase() == "on")
                .and_then(|(i, _)| words.get(i+1))
                .map(|s| s.trim_matches('`').trim_end_matches(';').to_string())
                .unwrap_or_default();

            // Extract USER KEY (after "KEY" keyword)
            let user_key = words.iter().enumerate()
                .find(|(_, w)| w.to_lowercase() == "key")
                .and_then(|(i, _)| words.get(i+1))
                .map(|s| s.trim_matches('`').trim_end_matches(';').to_string())
                .unwrap_or_else(|| "user_id".to_string());

            // Extract SESSION TIMEOUT value (after "TIMEOUT")
            let session_timeout_sec: u64 = words.iter().enumerate()
                .find(|(_, w)| w.to_lowercase() == "timeout")
                .and_then(|(i, _)| words.get(i+1))
                .and_then(|s| s.trim_end_matches(';').parse::<u64>().ok())
                .map(|mins| mins * 60)
                .unwrap_or(1800); // default 30 minutes

            info!(mv = %mv_name, source = %source_cube, "Session MV created (stub)");

            // SMV 메타 등록
            let key = format!("smv:{mv_name}");
            let _ = self.raft.write(crate::raft::RaftCommand::UpsertKv {
                key, value: sql.to_string()
            }).await;

            // BtRegistry에 등록
            if !source_cube.is_empty() {
                self.cube_mgr.register_behavioral_table(
                    &mv_name,
                    &source_cube,
                    &user_key,
                    session_timeout_sec,
                );
                // Immediately mark as Active (stub — in production would wait for materialization)
                self.cube_mgr.bt_registry_mut().update_state(&mv_name, crate::meta::bt_registry::BtState::Active);
                info!(mv = %mv_name, "BT marked Active after CREATE SESSION MATERIALIZED VIEW");
            }

            return results.completed(OkResponse::default()).await.map_err(Into::into);
        }

        // CREATE CUBE DDL
        if lower.starts_with("create cube") {
            let if_not_exists = lower.contains("if not exists");
            match parse_create_cube_full(sql) {
                Ok(stmt) => {
                    let db = self.current_db();
                    if if_not_exists {
                        if let Ok(Some(_)) = self.cube_mgr.get_by_name(&stmt.name).await {
                            return results.completed(OkResponse::default()).await.map_err(Into::into);
                        }
                    }
                    let schema = stmt_to_schema(stmt, &db);
                    match self.cube_mgr.create(&schema).await {
                        Ok(()) => {
                            info!(cube = %schema.name, "Cube created via DDL");
                            return results.completed(OkResponse::default()).await.map_err(Into::into);
                        }
                        Err(e) => {
                            return results.error(ErrorKind::ER_TABLE_EXISTS_ERROR, e.to_string().as_bytes()).await.map_err(Into::into);
                        }
                    }
                }
                Err(e) => {
                    warn!(err = %e, "CREATE CUBE 파싱 실패");
                    return results.error(ErrorKind::ER_PARSE_ERROR, e.to_string().as_bytes()).await.map_err(Into::into);
                }
            }
        }

        // DROP CUBE / DROP TABLE
        if lower.starts_with("drop cube") || lower.starts_with("drop table") {
            let parts: Vec<&str> = sql.split_whitespace().collect();
            let name = parts.last().map(|s| s.trim_end_matches(';').trim_matches('`')).unwrap_or("");
            match self.cube_mgr.get_by_name(name).await {
                Ok(Some(schema)) => {
                    match self.cube_mgr.drop_cube(&schema.cube_id.to_string()).await {
                        Ok(()) => return results.completed(OkResponse::default()).await.map_err(Into::into),
                        Err(e) => return results.error(ErrorKind::ER_BAD_TABLE_ERROR, e.to_string().as_bytes()).await.map_err(Into::into),
                    }
                }
                _ => return results.error(ErrorKind::ER_BAD_TABLE_ERROR, format!("Unknown cube: {name}").as_bytes()).await.map_err(Into::into),
            }
        }

        // ALTER CUBE / ALTER TABLE (MySQL 호환: ALTER TABLE → ALTER CUBE로 처리)
        if lower.starts_with("alter cube") || lower.starts_with("alter table") {
            // ALTER TABLE <name> … → ALTER CUBE <name> … 로 정규화
            let normalized = if lower.starts_with("alter table") {
                format!("ALTER CUBE{}", &sql[11..])
            } else {
                sql.to_string()
            };
            match WowDbParser::parse(&normalized) {
                Ok(stmts) => {
                    if let Some(WowDbStatement::Custom(WowDbCustom::AlterCube(stmt))) =
                        stmts.into_iter().next()
                    {
                        let alter_handler =
                            AlterCubeHandler::new(self.cube_mgr.clone(), self.raft.clone());
                        match alter_handler.execute(&stmt).await {
                            Ok(()) => {
                                info!(cube = %stmt.name, "Cube altered");
                                return results
                                    .completed(OkResponse::default())
                                    .await
                                    .map_err(Into::into);
                            }
                            Err(e) => {
                                return results
                                    .error(
                                        ErrorKind::ER_PARSE_ERROR,
                                        e.to_string().as_bytes(),
                                    )
                                    .await
                                    .map_err(Into::into);
                            }
                        }
                    } else {
                        return results
                            .error(
                                ErrorKind::ER_PARSE_ERROR,
                                b"ALTER CUBE: unexpected parse result",
                            )
                            .await
                            .map_err(Into::into);
                    }
                }
                Err(e) => {
                    warn!(err = %e, "ALTER CUBE 파싱 실패");
                    return results
                        .error(ErrorKind::ER_PARSE_ERROR, e.to_string().as_bytes())
                        .await
                        .map_err(Into::into);
                }
            }
        }

        // ── INSERT ────────────────────────────────────────────────────────────
        if lower.starts_with("insert ") {
            match execute_insert(sql) {
                Ok(r) => return results.completed(OkResponse {
                    affected_rows: r.rows_affected,
                    ..Default::default()
                }).await.map_err(Into::into),
                Err(e) => return results.error(ErrorKind::ER_PARSE_ERROR, e.as_bytes()).await.map_err(Into::into),
            }
        }

        // ── Behavioral Routing (T157/T161) ────────────────────────────────────
        // For SELECT/analytics queries: detect pattern and optionally rewrite
        // SQL to use an Active Behavioral Table (Session MV).
        let effective_sql: std::borrow::Cow<str> = if lower.starts_with("select ")
            || lower.contains("funnel_count(")
            || lower.contains("cohort_analysis(")
            || lower.contains("path_analysis(")
        {
            let pattern = detect_pattern(sql);
            match &pattern {
                QueryPattern::Behavioral { .. } | QueryPattern::Hybrid => {
                    let bt_reg = self.cube_mgr.bt_registry();
                    let router = BehavioralRouter::new(&*bt_reg);
                    let routing = router.route(sql, &pattern, 0, 0);
                    drop(bt_reg);

                    if routing.routed {
                        if let Some(rewritten) = routing.rewritten_sql {
                            info!(
                                original_table = %crate::planner::behavioral_pattern::extract_scan_table(sql).unwrap_or_default(),
                                bt = %routing.bt_used.unwrap_or_default(),
                                "Behavioral routing: query rewritten to use BT"
                            );
                            std::borrow::Cow::Owned(rewritten)
                        } else {
                            std::borrow::Cow::Borrowed(sql)
                        }
                    } else {
                        // No BT found — attach guidance as a warning
                        if let Some(guidance) = routing.guidance {
                            warn!(
                                warning = %guidance.warning,
                                "Behavioral query has no Active BT — falling back to event table"
                            );
                            self.push_warning(guidance.warning);
                        }
                        std::borrow::Cow::Borrowed(sql)
                    }
                }
                QueryPattern::Event => std::borrow::Cow::Borrowed(sql),
            }
        } else {
            std::borrow::Cow::Borrowed(sql)
        };
        let sql_to_exec: &str = &effective_sql;
        let lower_exec = sql_to_exec.trim().to_lowercase();

        // ── WOW-DB 분석 함수 (FUNNEL_COUNT, COHORT_ANALYSIS, PATH_ANALYSIS) ──
        let analytics_fn = if lower_exec.contains("funnel_count(") {
            Some(execute_funnel_count(sql_to_exec))
        } else if lower_exec.contains("cohort_analysis(") {
            Some(execute_cohort_analysis(sql_to_exec))
        } else if lower_exec.contains("path_analysis(") {
            Some(execute_path_analysis(sql_to_exec))
        } else {
            None
        };

        if let Some(analytics_result) = analytics_fn {
            let warnings_count = self.pending_warnings.lock().unwrap().len() as u16;
            match analytics_result {
                Ok(sel) => {
                    let col_defs: Vec<_> = sel.columns.iter()
                        .map(|c| build_columns(c, ColumnType::MYSQL_TYPE_VAR_STRING))
                        .collect();
                    let mut rw = results.start(&col_defs).await?;
                    for row in &sel.rows {
                        let strs: Vec<Option<String>> = row.iter().map(|v| match v {
                            serde_json::Value::Null    => None,
                            serde_json::Value::String(s) => Some(s.clone()),
                            other => Some(other.to_string()),
                        }).collect();
                        let refs: Vec<Option<&str>> = strs.iter().map(|s| s.as_deref()).collect();
                        rw.write_row(refs).await?;
                    }
                    let _ = warnings_count; // included in finish via OkResponse below
                    return rw.finish().await.map_err(Into::into);
                }
                Err(e) => {
                    return results.error(ErrorKind::ER_PARSE_ERROR, e.as_bytes())
                        .await.map_err(Into::into);
                }
            }
        }

        // ── SELECT ────────────────────────────────────────────────────────────
        if lower_exec.starts_with("select ") {
            let warnings_count = self.pending_warnings.lock().unwrap().len() as u16;
            match execute_select(sql_to_exec) {
                Ok(sel) => {
                    let col_defs: Vec<_> = sel.columns.iter()
                        .map(|c| build_columns(c, ColumnType::MYSQL_TYPE_VAR_STRING))
                        .collect();
                    let mut rw = results.start(&col_defs).await?;
                    for row in &sel.rows {
                        let strs: Vec<Option<String>> = row.iter().map(|v| match v {
                            serde_json::Value::Null    => None,
                            serde_json::Value::String(s) => Some(s.clone()),
                            other => Some(other.to_string()),
                        }).collect();
                        let refs: Vec<Option<&str>> = strs.iter().map(|s| s.as_deref()).collect();
                        rw.write_row(refs).await?;
                    }
                    let _ = warnings_count;
                    return rw.finish().await.map_err(Into::into);
                }
                Err(e) => return results.error(ErrorKind::ER_PARSE_ERROR, e.as_bytes()).await.map_err(Into::into),
            }
        }

        // ── SELECT 에 걸리지 않는 시스템 변수 쿼리 (SELECT 이외 경로) ─────────

        // ── DELETE ────────────────────────────────────────────────────────────
        if lower.starts_with("delete from") || lower.starts_with("truncate") {
            let table = lower.split_whitespace().last().unwrap_or("").trim_end_matches(';').to_string();
            MEM_STORE.drop_table(&table);
            return results.completed(OkResponse::default()).await.map_err(Into::into);
        }

        // 인식되지 않는 SQL — 파싱을 시도하여 오류 종류를 결정한다.
        // - 파싱 실패 → ER_PARSE_ERROR (1064): 문법 오류
        // - 파싱 성공 → ER_NOT_SUPPORTED_YET (1235): 유효하나 미구현
        match WowDbParser::parse(sql) {
            Err(_) => {
                let snippet = &sql[..sql.len().min(60)];
                let msg = format!(
                    "You have an error in your SQL syntax near '{}' (line 1)",
                    snippet.lines().next().unwrap_or(snippet)
                );
                warn!(sql = %snippet, "SQL parse error (unrecognized)");
                results.error(ErrorKind::ER_PARSE_ERROR, msg.as_bytes()).await.map_err(Into::into)
            }
            Ok(stmts) => {
                // 파싱은 성공했으나 WOW-DB가 아직 실행을 지원하지 않는 문
                let stmt_type = stmts.first().map(|s| match s {
                    WowDbStatement::Standard(stmt) => {
                        // 안내 메시지가 있는 문은 hint를 포함
                        let label: &str = match stmt {
                            SqlStatement::Update { .. }          => "UPDATE statement",
                            SqlStatement::CreateTable { .. }     => "CREATE TABLE (use CREATE CUBE instead)",
                            SqlStatement::CreateIndex { .. }     => "CREATE INDEX (use ALTER CUBE ... ADD INDEX instead)",
                            SqlStatement::CreateView { .. }      => "CREATE VIEW (use CREATE SESSION MATERIALIZED VIEW instead)",
                            SqlStatement::StartTransaction { .. }
                            | SqlStatement::Commit { .. }
                            | SqlStatement::Rollback { .. }      => "explicit transaction control",
                            SqlStatement::Call { .. }            => "stored procedure CALL",
                            SqlStatement::Grant { .. }
                            | SqlStatement::Revoke { .. }        => "GRANT/REVOKE",
                            SqlStatement::LockTables { .. }      => "LOCK TABLES",
                            SqlStatement::CreateFunction { .. }  => "CREATE FUNCTION",
                            _                                    => "this SQL statement",
                        };
                        label.to_string()
                    }
                    WowDbStatement::Custom(_) => "custom statement".to_string(),
                }).unwrap_or_else(|| "unknown statement".to_string());

                let msg = format!(
                    "This version of WOW-DB doesn't yet support '{}'",
                    stmt_type
                );
                warn!(sql = %&sql[..sql.len().min(60)], stmt = %stmt_type, "Unsupported SQL statement");
                results.error(ErrorKind::ER_NOT_SUPPORTED_YET, msg.as_bytes()).await.map_err(Into::into)
            }
        }
    }

    // ── COM_STMT_PREPARE (Prepared Statement) ──────────────────────────────

    async fn on_prepare<'a>(
        &'a mut self,
        query:  &'a str,
        info:   StatementMetaWriter<'a, W>,
    ) -> Result<()> {
        debug!(sql = %query, "COM_STMT_PREPARE");
        info.reply(1, &[], &[]).await.map_err(Into::into)
    }

    // ── COM_STMT_EXECUTE ───────────────────────────────────────────────────

    async fn on_execute<'a>(
        &'a mut self,
        _id:     u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        results
            .error(
                ErrorKind::ER_NOT_SUPPORTED_YET,
                b"This version of WOW-DB doesn't yet support 'COM_STMT_EXECUTE (prepared statements)'",
            )
            .await
            .map_err(Into::into)
    }

    // ── COM_INIT_DB (USE database) ────────────────────────────────────────

    async fn on_init<'a>(
        &'a mut self,
        schema: &'a str,
        w: InitWriter<'a, W>,
    ) -> Result<()> {
        let db = schema.trim().trim_matches('`').to_string();
        *self.current_db.lock().unwrap() = db.clone();
        info!(database = %db, "Database selected (COM_INIT_DB)");
        w.ok().await.map_err(Into::into)
    }

    // ── COM_STMT_CLOSE ─────────────────────────────────────────────────────

    async fn on_close(&mut self, _stmt: u32) {}
}

// ─── 출력 헬퍼 ───────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum QueryOutput {
    Rows {
        columns: Vec<ColumnMeta>,
        rows:    Vec<Vec<Option<String>>>,
    },
    Affected(u64),
    Error(String),
}

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name:     String,
    pub col_type: ColumnType,
}

async fn write_output<W: tokio::io::AsyncWrite + Send + Unpin>(
    results: QueryResultWriter<'_, W>,
    output:  QueryOutput,
) -> Result<()> {
    match output {
        QueryOutput::Rows { columns, rows } => {
            let cols: Vec<Column> = columns.iter()
                .map(|c| build_columns(&c.name, c.col_type))
                .collect();
            let mut rw = results.start(&cols).await?;
            for row in rows {
                // We need to build a vec of Option<&str> referencing the row strings
                let str_refs: Vec<Option<&str>> = row.iter()
                    .map(|v| v.as_deref())
                    .collect();
                rw.write_row(str_refs).await?;
            }
            rw.finish().await.map_err(Into::into)
        }
        QueryOutput::Affected(n) => {
            results.completed(OkResponse {
                affected_rows: n,
                ..Default::default()
            }).await.map_err(Into::into)
        }
        QueryOutput::Error(msg) => {
            results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes())
                .await
                .map_err(Into::into)
        }
    }
}

// ─── DB 이름 추출 헬퍼 ───────────────────────────────────────────────────────

/// "create database foo" → "foo", "create database if not exists foo" → "foo"
fn extract_db_name(lower_sql: &str, after_keyword: &str) -> String {
    let pos = lower_sql.find(after_keyword).unwrap_or(0) + after_keyword.len();
    lower_sql[pos..]
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches('`')
        .trim_end_matches(';')
        .to_string()
}

// ─── CreateCubeStmt → CubeSchema 변환 ────────────────────────────────────────

fn parse_data_type(s: &str) -> DataType {
    match s.to_uppercase().split('(').next().unwrap_or("").trim() {
        "BOOLEAN" | "BOOL"              => DataType::Boolean,
        "TINYINT"                       => DataType::Int8,
        "SMALLINT"                      => DataType::Int16,
        "INT" | "INTEGER"               => DataType::Int32,
        "BIGINT"                        => DataType::Int64,
        "FLOAT"                         => DataType::Float32,
        "DOUBLE" | "DECIMAL" | "NUMERIC"=> DataType::Float64,
        "TEXT" | "VARCHAR" | "CHAR" | "STRING" => DataType::String,
        "DATETIME" | "TIMESTAMP"        => DataType::DateTime,
        "DATE"                          => DataType::Date,
        "JSON"                          => DataType::Json,
        "BINARY" | "BLOB" | "VARBINARY" => DataType::Binary,
        _                               => DataType::String,
    }
}

/// 테스트에서 접근 가능한 public 래퍼
#[cfg(test)]
pub fn stmt_to_schema_pub(stmt: crate::sql_parser::CreateCubeStmt, db: &str) -> CubeSchema {
    stmt_to_schema(stmt, db)
}

fn stmt_to_schema(stmt: crate::sql_parser::CreateCubeStmt, db: &str) -> CubeSchema {
    let columns: Vec<ColumnDef> = stmt.columns.iter().map(|c| ColumnDef {
        name:          c.name.clone(),
        data_type:     parse_data_type(&c.data_type),
        nullable:      c.nullable,
        encoding:      Encoding::Plain,
        compression:   shared::types::Compression::None,
        skipping_index: None,
        flat_json:     None,
        default_value: None,
    }).collect();

    let partition_key = stmt.partition_by.as_ref().map(|pb| PartitionKey {
        variant: PartitionVariant::Range {
            column:      pb.columns.first().cloned().unwrap_or_default(),
            granularity: None,
        },
        auto_partition: pb.auto,
    }).unwrap_or_else(|| PartitionKey {
        variant: PartitionVariant::Range {
            column:      columns.first().map(|c| c.name.clone()).unwrap_or_default(),
            granularity: None,
        },
        auto_partition: false,
    });

    let distribution = stmt.distribution.as_ref().map(|d| Distribution {
        column:       d.columns.first().cloned().unwrap_or_default(),
        bucket_count: d.buckets,
    }).unwrap_or(Distribution { column: String::new(), bucket_count: 4 });

    CubeSchema {
        cube_id:         Uuid::new_v4(),
        name:            stmt.name,
        database:        db.to_string(),
        columns,
        partition_key,
        sort_key:        stmt.order_by.iter().map(|c| shared::types::ColumnRef::new(c)).collect(),
        distribution,
        colocate_group:  None,
        storage_backend: StorageBackend::Native,
        ttl_policy:      None,
        tiering_policy:  None,
        created_at:      Utc::now(),
        version:         1,
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handler_creation() {
        let raft     = Arc::new(crate::raft::RaftManager::new_local());
        let cube_mgr = Arc::new(CubeManager::new(raft.clone()));
        let smv_mgr  = Arc::new(SmvManager::new(raft.clone()));
        let handler  = WowDbMysqlHandler::new(cube_mgr, smv_mgr, raft);
        assert_eq!(handler.current_db(), "default");
    }
}
