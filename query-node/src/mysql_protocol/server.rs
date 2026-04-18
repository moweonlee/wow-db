// T033: MySQL Wire Protocol 서버 스켈레톤 (포트 9030)
// Phase D에서 opensrv-mysql AsyncMysqlShim 완전 구현 예정
// 현재: TCP 연결 수락 + 기본 MySQL 핸드쉐이크 stub

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, ErrorKind, OkResponse,
    ParamParser, QueryResultWriter, StatementMetaWriter,
};
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::meta::cube::CubeManager;
use crate::raft::RaftManager;
use crate::session_mv::manager::SmvManager;

// ─── 쿼리 핸들러 트레이트 ─────────────────────────────────────────────────────

#[async_trait::async_trait]
pub trait QueryHandler: Send + Sync + 'static {
    async fn execute_query(&self, sql: &str) -> Result<QueryOutput>;
}

pub enum QueryOutput {
    Rows {
        columns: Vec<ColumnDesc>,
        rows:    Vec<Vec<Option<Vec<u8>>>>,
    },
    Affected { rows: u64 },
    Error   { message: String },
}

#[derive(Debug, Clone)]
pub struct ColumnDesc {
    pub name:     String,
    pub col_type: opensrv_mysql::ColumnType,
    pub flags:    opensrv_mysql::ColumnFlags,
}

// ─── 임시 쿼리 핸들러 ─────────────────────────────────────────────────────────

pub struct StubQueryHandler;

#[async_trait::async_trait]
impl QueryHandler for StubQueryHandler {
    async fn execute_query(&self, sql: &str) -> Result<QueryOutput> {
        let lower = sql.trim().to_lowercase();
        if lower.contains("@@version") {
            return Ok(QueryOutput::Rows {
                columns: vec![ColumnDesc {
                    name:     "@@version".into(),
                    col_type: opensrv_mysql::ColumnType::MYSQL_TYPE_VAR_STRING,
                    flags:    opensrv_mysql::ColumnFlags::empty(),
                }],
                rows: vec![vec![Some(b"8.0.0-wowdb-0.1".to_vec())]],
            });
        }
        if lower == "select 1" {
            return Ok(QueryOutput::Rows {
                columns: vec![ColumnDesc {
                    name:     "1".into(),
                    col_type: opensrv_mysql::ColumnType::MYSQL_TYPE_LONG,
                    flags:    opensrv_mysql::ColumnFlags::empty(),
                }],
                rows: vec![vec![Some(b"1".to_vec())]],
            });
        }
        Ok(QueryOutput::Error {
            message: format!("WOW-DB Phase D 미구현: {}", &sql[..sql.len().min(80)]),
        })
    }
}

// ─── MySQL Shim 래퍼 ──────────────────────────────────────────────────────────

struct WowDbShim {
    handler: Arc<dyn QueryHandler>,
}

#[async_trait::async_trait]
impl<W: tokio::io::AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for WowDbShim {
    type Error = anyhow::Error;

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        info!(sql = %query, "MySQL query");
        match self.handler.execute_query(query).await {
            Ok(QueryOutput::Rows { columns, rows }) => {
                let cols: Vec<opensrv_mysql::Column> = columns
                    .iter()
                    .map(|c| opensrv_mysql::Column {
                        table:    String::new(),
                        column:   c.name.clone(),
                        coltype:  c.col_type,
                        colflags: c.flags,
                    })
                    .collect();
                let mut rw = results.start(&cols).await?;
                for row in rows {
                    let values: Vec<Option<&[u8]>> =
                        row.iter().map(|v| v.as_deref()).collect();
                    rw.write_row(values).await?;
                }
                rw.finish().await.map_err(Into::into)
            }
            Ok(QueryOutput::Affected { rows }) => results
                .completed(OkResponse {
                    affected_rows: rows,
                    ..Default::default()
                })
                .await
                .map_err(Into::into),
            Ok(QueryOutput::Error { message }) => results
                .error(ErrorKind::ER_UNKNOWN_ERROR, message.as_bytes())
                .await
                .map_err(Into::into),
            Err(e) => results
                .error(ErrorKind::ER_UNKNOWN_ERROR, e.to_string().as_bytes())
                .await
                .map_err(Into::into),
        }
    }

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<()> {
        info.error(ErrorKind::ER_UNKNOWN_ERROR, b"Prepared statements: Phase D")
            .await
            .map_err(Into::into)
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        results
            .error(ErrorKind::ER_UNKNOWN_ERROR, b"Prepared statements: Phase D")
            .await
            .map_err(Into::into)
    }

    async fn on_close(&mut self, _stmt: u32) {}
}

// ─── 서버 기동 ────────────────────────────────────────────────────────────────

pub async fn serve(addr: SocketAddr, handler: Arc<dyn QueryHandler>) -> Result<()> {
    let listener = TcpListener::bind(&addr).await?;
    info!(%addr, "MySQL Protocol 서버 시작 (포트 9030)");

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v)  => v,
            Err(e) => {
                error!(err = %e, "Accept 실패");
                continue;
            }
        };
        info!(%peer, "MySQL 클라이언트 연결");

        let shim   = WowDbShim { handler: handler.clone() };
        let (r, w) = tokio::io::split(stream);

        tokio::spawn(async move {
            if let Err(e) = AsyncMysqlIntermediary::run_on(shim, r, w).await {
                warn!(%peer, err = %e, "MySQL 세션 오류");
            }
        });
    }
}

pub async fn serve_stub(addr: SocketAddr) -> Result<()> {
    serve(addr, Arc::new(StubQueryHandler)).await
}

/// WowDbMysqlHandler를 사용하는 프로덕션 MySQL 서버 기동
pub async fn serve_with_managers(
    addr:     SocketAddr,
    cube_mgr: Arc<CubeManager>,
    smv_mgr:  Arc<SmvManager>,
    raft:     Arc<RaftManager>,
) -> Result<()> {
    use super::handler::WowDbMysqlHandler;

    let listener = TcpListener::bind(&addr).await?;
    info!(%addr, "MySQL Protocol 서버 시작 (WowDbMysqlHandler)");

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v)  => v,
            Err(e) => { error!(err = %e, "Accept 실패"); continue; }
        };
        info!(%peer, "MySQL 클라이언트 연결");

        let handler = WowDbMysqlHandler::new(
            cube_mgr.clone(),
            smv_mgr.clone(),
            raft.clone(),
        );
        let (r, w) = tokio::io::split(stream);
        tokio::spawn(async move {
            if let Err(e) = AsyncMysqlIntermediary::run_on(handler, r, w).await {
                warn!(%peer, err = %e, "MySQL 세션 오류");
            }
        });
    }
}
