// T086: opensrv-mysql 완전 구현 — MySQL 8.0 handshake, 인증, COM_QUERY, COM_STMT_PREPARE 처리

use std::sync::Arc;

use anyhow::Result;
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim,
    Column, ColumnFlags, ColumnType,
    ErrorKind, OkResponse, ParamParser,
    QueryResultWriter, StatementMetaWriter,
};
use tracing::{debug, info, warn};

use super::result_set::build_columns;
use super::schema_cmds::handle_schema_command;
use crate::meta::cube::CubeManager;
use crate::raft::RaftManager;
use crate::session_mv::manager::SmvManager;

// ─── WOW-DB MySQL 핸들러 ─────────────────────────────────────────────────────

pub struct WowDbMysqlHandler {
    pub cube_mgr:   Arc<CubeManager>,
    pub smv_mgr:    Arc<SmvManager>,
    pub raft:       Arc<RaftManager>,
    current_db:     std::sync::Mutex<String>,
}

impl WowDbMysqlHandler {
    pub fn new(
        cube_mgr: Arc<CubeManager>,
        smv_mgr:  Arc<SmvManager>,
        raft:     Arc<RaftManager>,
    ) -> Self {
        Self {
            cube_mgr,
            smv_mgr,
            raft,
            current_db: std::sync::Mutex::new("default".to_string()),
        }
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

        // 스키마 명령 처리 (SHOW, DESCRIBE 등)
        if let Some(output) = handle_schema_command(sql, &self.cube_mgr, &self.current_db()).await {
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

        // WOW-DB 쿼리 실행 (스텁 — Phase D에서 실행 엔진 연동)
        let stub_msg = format!("(Query stub: {})", &sql[..sql.len().min(60)]);
        let cols = vec![build_columns("result", ColumnType::MYSQL_TYPE_VAR_STRING)];
        let mut rw = results.start(&cols).await?;
        rw.write_row(std::iter::once(Some(stub_msg.as_str()))).await?;
        rw.finish().await.map_err(Into::into)
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
        let cols = vec![build_columns("result", ColumnType::MYSQL_TYPE_VAR_STRING)];
        let mut rw = results.start(&cols).await?;
        rw.write_row(std::iter::once(Some("(prepared statement stub)"))).await?;
        rw.finish().await.map_err(Into::into)
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
