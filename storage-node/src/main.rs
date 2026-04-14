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
use serde::Deserialize;
use tokio::net::TcpListener;
use tracing::info;

// ── TOML 설정 구조체 ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
struct NodeCfg {
    id: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct GrpcCfg {
    port: Option<u16>,
}

#[derive(Debug, Deserialize, Default)]
struct HttpCfg {
    port: Option<u16>,
}

#[derive(Debug, Deserialize, Default)]
struct StorageCfg {
    backend:  Option<String>,
    data_dir: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct TabletCfg {
    replica_count: Option<u32>,
}

#[derive(Debug, Deserialize, Default)]
struct StorageNodeConfig {
    node:    NodeCfg,
    grpc:    GrpcCfg,
    http:    HttpCfg,
    storage: StorageCfg,
    tablet:  TabletCfg,
}

fn load_config() -> Result<StorageNodeConfig> {
    if let Some(path) = shared::config::parse_config_path() {
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("Failed to read config file '{}': {}", path, e))?;
        let cfg: StorageNodeConfig = toml::from_str(&raw)
            .map_err(|e| anyhow::anyhow!("Failed to parse config file '{}': {}", path, e))?;
        info!(path = %path, "Loaded config from file");
        Ok(cfg)
    } else {
        Ok(StorageNodeConfig::default())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // ── 설정 로드 (TOML 파일 → env var 오버라이드 순) ────────────────────────
    // 로깅 초기화 전 파일 경로 결정을 위해 먼저 로드
    let cfg = load_config()?;

    // SN은 DATA_DIR 하위 logs/에 저장 (스토리지와 함께 볼륨 마운트)
    let data_dir = std::env::var("DATA_DIR")
        .unwrap_or_else(|_| cfg.storage.data_dir.clone().unwrap_or_else(|| "/data".to_string()));

    let log_dir = std::env::var("LOG_DIR")
        .unwrap_or_else(|_| format!("{}/logs", data_dir));

    let _log_guard = shared::logging::init_logging(
        "storage-node",
        &log_dir,
        "storage_node=info,shared=info",
    );

    let node_id = std::env::var("NODE_ID")
        .unwrap_or_else(|_| cfg.node.id.clone().unwrap_or_else(|| "sn-1".to_string()));

    let grpc_port: u16 = std::env::var("GRPC_PORT")
        .ok().and_then(|v| v.parse().ok())
        .unwrap_or_else(|| cfg.grpc.port.unwrap_or(9060));

    let http_port: u16 = std::env::var("HTTP_PORT")
        .ok().and_then(|v| v.parse().ok())
        .unwrap_or_else(|| cfg.http.port.unwrap_or(8040));

    let backend = std::env::var("STORAGE_BACKEND")
        .unwrap_or_else(|_| cfg.storage.backend.clone().unwrap_or_else(|| "native".to_string()));

    let _replica_count = std::env::var("TABLET_REPLICA_COUNT")
        .ok().and_then(|v| v.parse::<u32>().ok())
        .unwrap_or_else(|| cfg.tablet.replica_count.unwrap_or(3));

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
