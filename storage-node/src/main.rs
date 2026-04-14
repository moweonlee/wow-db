// Storage Node (Data Node) — WOW-DB
// 역할: LSM-Tree 스토리지, 컬럼 파일 I/O, WAL, Compaction, S3/HDFS 백엔드

mod gen;
mod lsm;
mod backend;
mod grpc;
mod columnar;
mod block_cache;
mod index;
mod transaction;
mod partition;
mod ttl;
mod tiering;

use anyhow::Result;
use axum::{routing::get, Router, Json};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // ── 로깅 초기화 (파일 + 콘솔 듀얼 싱크, 일별 로테이션) ──────────────────
    // SN은 DATA_DIR 하위 logs/에 저장 (스토리지와 함께 볼륨 마운트)
    let log_dir = {
        let data = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string());
        std::env::var("LOG_DIR").unwrap_or_else(|_| format!("{}/logs", data))
    };
    let _log_guard = shared::logging::init_logging(
        "storage-node",
        &log_dir,
        "storage_node=info,shared=info",
    );

    let node_id = std::env::var("NODE_ID").unwrap_or_else(|_| "sn-1".to_string());
    let grpc_port: u16 = std::env::var("GRPC_PORT")
        .unwrap_or_else(|_| "9060".to_string())
        .parse().unwrap_or(9060);
    let http_port: u16 = std::env::var("HTTP_PORT")
        .unwrap_or_else(|_| "8040".to_string())
        .parse().unwrap_or(8040);
    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string());
    let backend  = std::env::var("STORAGE_BACKEND").unwrap_or_else(|_| "native".to_string());

    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
    info!("io_uring support: DISABLED (using tokio::fs fallback)");

    info!(
        node_id = %node_id,
        grpc_port,
        http_port,
        data_dir = %data_dir,
        storage_backend = %backend,
        "Storage Node starting"
    );

    // ── HTTP 서버 기동 (헬스체크 + Stream Load) ────────────────────────────
    let nid = node_id.clone();
    let router = Router::new()
        .route("/health", get(|| async { Json(serde_json::json!({ "status": "ok" })) }))
        .route("/healthz", get(|| async { Json(serde_json::json!({ "status": "ok" })) }))
        .route("/api/v1/info", get(move || {
            let id = nid.clone();
            async move { Json(serde_json::json!({ "node_id": id, "role": "storage" })) }
        }));

    let addr = format!("0.0.0.0:{}", http_port);
    let listener = TcpListener::bind(&addr).await?;
    info!(port = http_port, "Storage Node HTTP server listening");

    // TODO (Phase B): gRPC StorageService 서버 추가 (tonic, grpc_port)
    // TODO (Phase B): LSM-Tree 엔진 초기화 및 WAL 복구

    tokio::select! {
        result = axum::serve(listener, router) => {
            if let Err(e) = result { tracing::error!(err = %e, "HTTP server error"); }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Storage Node shutting down");
        }
    }
    Ok(())
}
