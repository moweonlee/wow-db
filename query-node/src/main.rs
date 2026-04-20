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
mod rpc;
mod startup;
pub mod storage_client;
pub mod cn_client;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use serde::Deserialize;
use tracing::info;

use crate::meta::cube::CubeManager;
use crate::raft::RaftManager;
use crate::session_mv::manager::SmvManager;
use crate::web_ui::server::WebUiState;

// ── TOML 설정 구조체 ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
struct NodeCfg {
    id: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct MysqlCfg {
    port: Option<u16>,
}

#[derive(Debug, Deserialize, Default)]
struct WebCfg {
    port: Option<u16>,
}

#[derive(Debug, Deserialize, Default)]
struct RaftCfg {
    port: Option<u16>,
    peers: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Default)]
struct GrpcCfg {
    port: Option<u16>,
}

#[derive(Debug, Deserialize, Default)]
struct ComputeNodesCfg {
    addresses: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Default)]
struct QueryNodeConfig {
    node:          NodeCfg,
    mysql:         MysqlCfg,
    web:           WebCfg,
    raft:          RaftCfg,
    grpc:          GrpcCfg,
    compute_nodes: ComputeNodesCfg,
}

fn load_config() -> Result<QueryNodeConfig> {
    if let Some(path) = shared::config::parse_config_path() {
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("Failed to read config file '{}': {}", path, e))?;
        let cfg: QueryNodeConfig = toml::from_str(&raw)
            .map_err(|e| anyhow::anyhow!("Failed to parse config file '{}': {}", path, e))?;
        info!(path = %path, "Loaded config from file");
        Ok(cfg)
    } else {
        Ok(QueryNodeConfig::default())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // ── 로깅 초기화 (파일 + 콘솔 듀얼 싱크, 일별 로테이션) ──────────────────
    let log_dir = std::env::var("LOG_DIR")
        .unwrap_or_else(|_| "./logs".to_string());
    let _log_guard = shared::logging::init_logging(
        "query-node",
        &log_dir,
        "query_node=info,shared=info",
    );

    // ── 설정 로드 (TOML 파일 → env var 오버라이드 순) ────────────────────────
    let cfg = load_config()?;

    let node_id = std::env::var("NODE_ID")
        .or_else(|_| std::env::var("QN_NODE_ID"))
        .unwrap_or_else(|_| cfg.node.id.clone().unwrap_or_else(|| "qn-1".to_string()));

    let mysql_port: u16 = std::env::var("MYSQL_PORT")
        .ok().and_then(|v| v.parse().ok())
        .unwrap_or_else(|| cfg.mysql.port.unwrap_or(9030));

    let web_port: u16 = std::env::var("WEB_PORT")
        .ok().and_then(|v| v.parse().ok())
        .unwrap_or_else(|| cfg.web.port.unwrap_or(8080));

    let raft_port: u16 = std::env::var("RAFT_PORT")
        .ok().and_then(|v| v.parse().ok())
        .unwrap_or_else(|| cfg.raft.port.unwrap_or(9010));

    let grpc_port: u16 = std::env::var("GRPC_PORT")
        .ok().and_then(|v| v.parse().ok())
        .unwrap_or_else(|| cfg.grpc.port.unwrap_or(9011));

    // QN_PEERS: Raft peers, 콤마 구분 (Kubernetes discovery용)
    let _raft_peers: Vec<String> = std::env::var("QN_PEERS")
        .or_else(|_| std::env::var("RAFT_PEERS"))
        .map(|s| s.split(',').map(|p| p.trim().to_string()).collect())
        .unwrap_or_else(|_| cfg.raft.peers.clone().unwrap_or_default());

    let _compute_nodes: Vec<String> = std::env::var("COMPUTE_NODES")
        .map(|s| s.split(',').map(|p| p.trim().to_string()).collect())
        .unwrap_or_else(|_| cfg.compute_nodes.addresses.clone().unwrap_or_default());

    info!(
        node_id = %node_id,
        mysql_port,
        web_port,
        raft_port,
        grpc_port,
        "Query Node starting"
    );

    // ── 공유 매니저 초기화 (Raft KV 영속화 활성화) ───────────────────────────
    let qn_data_dir = {
        let base = std::env::var("QN_DATA_DIR")
            .unwrap_or_else(|_| {
                let tmp = std::env::temp_dir();
                format!("{}/wowdb-dev/{}/meta", tmp.display(), node_id)
            });
        std::path::PathBuf::from(base)
    };
    let raft     = Arc::new(RaftManager::new_with_persistence(1, &qn_data_dir));
    let cube_mgr = Arc::new(CubeManager::new(raft.clone()));
    let smv_mgr  = Arc::new(SmvManager::new(raft.clone()));

    // ── Web UI 서버 기동 (포트 8080) ─────────────────────────────────────────
    let node_registry = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
    let web_state = WebUiState {
        cube_mgr: cube_mgr.clone(),
        smv_mgr:  smv_mgr.clone(),
        raft:     raft.clone(),
        node_id:  node_id.clone(),
        nodes:    node_registry,
    };
    let web_port_copy = web_port;
    tokio::spawn(async move {
        if let Err(e) = web_ui::server::start(web_state, web_port_copy).await {
            tracing::error!(err = %e, "Web UI server error");
        }
    });
    info!(port = web_port, "Web UI server started");

    // ── QN 피어 등록 (StarRocks FE 패턴: QN끼리 서로 등록) ──────────────────
    {
        let nid       = node_id.clone();
        let mysql_str = format!("127.0.0.1:{}", mysql_port);
        tokio::spawn(async move {
            // Web UI 서버가 바인딩될 때까지 잠시 대기
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            if let Err(e) = startup::register_with_qn_peers(nid, mysql_str, web_port).await {
                tracing::warn!(err = %e, "QN peer registration error");
            }
        });
    }

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
