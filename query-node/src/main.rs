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

use anyhow::Result;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // 로깅 초기화
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "query_node=info,shared=info".into()),
        )
        .init();

    let node_id = std::env::var("NODE_ID").unwrap_or_else(|_| "qn-1".to_string());
    let mysql_port: u16 = std::env::var("MYSQL_PORT")
        .unwrap_or_else(|_| "9030".to_string())
        .parse()
        .unwrap_or(9030);
    let web_port: u16 = std::env::var("WEB_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .unwrap_or(8080);
    let raft_port: u16 = std::env::var("RAFT_PORT")
        .unwrap_or_else(|_| "9010".to_string())
        .parse()
        .unwrap_or(9010);
    let grpc_port: u16 = std::env::var("GRPC_PORT")
        .unwrap_or_else(|_| "9011".to_string())
        .parse()
        .unwrap_or(9011);

    info!(
        node_id = %node_id,
        mysql_port,
        web_port,
        raft_port,
        grpc_port,
        "Query Node starting"
    );

    // TODO (Phase B~D):
    // 1. Raft 클러스터 초기화 (openraft)
    // 2. MySQL Protocol 서버 기동 (opensrv-mysql, mysql_port)
    // 3. gRPC MetaService 서버 기동 (tonic, grpc_port)
    // 4. Web UI HTTP 서버 기동 (axum, web_port)
    // 5. Kafka Routine Load 워커 초기화

    // 임시: shutdown signal 대기
    tokio::signal::ctrl_c().await?;
    info!("Query Node shutting down");
    Ok(())
}
