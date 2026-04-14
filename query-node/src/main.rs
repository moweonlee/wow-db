// Query Node — WOW-DB
// 역할: SQL 파싱, CBO, 플래닝, 메타데이터(Raft), MySQL Protocol, Web UI, Kafka 수집

mod gen;
mod raft;
mod meta;
mod mysql_protocol;
mod sql_parser;
mod planner;
mod execution;
mod ingestion;
mod transaction;
mod session_mv;
mod web_ui;
mod profiler;
mod monitoring;
mod resource_group;
mod executor;
mod disk_monitor;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use tracing::info;

use crate::meta::cube::CubeManager;
use crate::raft::RaftManager;
use crate::session_mv::manager::SmvManager;
use crate::web_ui::server::WebUiState;

#[tokio::main]
async fn main() -> Result<()> {
    // ── 로깅 초기화 ───────────────────────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "query_node=info,shared=info".into()),
        )
        .init();

    let node_id = std::env::var("NODE_ID").unwrap_or_else(|_| "qn-1".to_string());
    let mysql_port: u16 = std::env::var("MYSQL_PORT")
        .unwrap_or_else(|_| "9030".to_string())
        .parse().unwrap_or(9030);
    let web_port: u16 = std::env::var("WEB_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse().unwrap_or(8080);
    let raft_port: u16 = std::env::var("RAFT_PORT")
        .unwrap_or_else(|_| "9010".to_string())
        .parse().unwrap_or(9010);
    let grpc_port: u16 = std::env::var("GRPC_PORT")
        .unwrap_or_else(|_| "9011".to_string())
        .parse().unwrap_or(9011);

    info!(
        node_id = %node_id,
        mysql_port,
        web_port,
        raft_port,
        grpc_port,
        "Query Node starting"
    );

    // ── 공유 매니저 초기화 ────────────────────────────────────────────────────
    let raft     = Arc::new(RaftManager::new_local());
    let cube_mgr = Arc::new(CubeManager::new(raft.clone()));
    let smv_mgr  = Arc::new(SmvManager::new(raft.clone()));

    // ── Web UI 서버 기동 (포트 8080) ─────────────────────────────────────────
    let web_state = WebUiState {
        cube_mgr: cube_mgr.clone(),
        smv_mgr:  smv_mgr.clone(),
        raft:     raft.clone(),
    };
    let web_port_copy = web_port;
    tokio::spawn(async move {
        if let Err(e) = web_ui::server::start(web_state, web_port_copy).await {
            tracing::error!(err = %e, "Web UI server error");
        }
    });
    info!(port = web_port, "Web UI server started");

    // ── MySQL Protocol 서버 기동 (포트 9030) ─────────────────────────────────
    let mysql_addr: SocketAddr = format!("0.0.0.0:{}", mysql_port).parse()?;
    let cube_mgr2 = cube_mgr.clone();
    let smv_mgr2  = smv_mgr.clone();
    let raft2     = raft.clone();
    tokio::spawn(async move {
        if let Err(e) = mysql_protocol::server::serve_with_managers(
            mysql_addr, cube_mgr2, smv_mgr2, raft2
        ).await {
            tracing::error!(err = %e, "MySQL protocol server error");
        }
    });
    info!(port = mysql_port, "MySQL protocol server started");

    // ── Shutdown 대기 ─────────────────────────────────────────────────────────
    tokio::signal::ctrl_c().await?;
    info!("Query Node shutting down");
    Ok(())
}
