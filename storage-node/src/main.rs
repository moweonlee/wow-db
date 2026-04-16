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
mod disk_monitor;
mod stats_reporter;
mod startup;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use axum::{routing::get, Router, Json};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::time::{Duration, sleep};
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

    // ── 클러스터 자가 등록 ─────────────────────────────────────────────────
    let grpc_addr = format!("{}:{}", node_id, grpc_port);
    let http_addr_str = format!("0.0.0.0:{}", http_port);
    startup::register_with_cluster(&node_id, &grpc_addr, &http_addr_str).await?;

    // ── DiskMonitor 시작 (FR-036: 디스크 용량 감시) ───────────────────────
    let qn_endpoint = std::env::var("QN_GRPC_ENDPOINT")
        .unwrap_or_else(|_| "127.0.0.1:9011".to_string());
    let watch_paths = vec![PathBuf::from(&data_dir)];
    let disk_mon = Arc::new(disk_monitor::create_disk_monitor(
        node_id.clone(),
        watch_paths,
        qn_endpoint.clone(),
    ));
    tokio::spawn({
        let mon = Arc::clone(&disk_mon);
        async move { mon.run().await }
    });
    info!("DiskMonitor 시작 (data_dir={})", data_dir);

    // ── TieringManager 시작 (FR-015: Tiered Storage) ──────────────────────
    let tiering_policy = tiering::TieringPolicy::default();
    let tiering_mgr = Arc::new(tiering::TieringManager::new(tiering_policy));
    tokio::spawn({
        let mgr = Arc::clone(&tiering_mgr);
        async move {
            let interval = Duration::from_secs(300); // 5분마다 확인
            loop {
                sleep(interval).await;
                let (moved, errors) = mgr.run_once().await;
                if moved > 0 || errors > 0 {
                    info!(moved, errors, "TieringManager run_once 완료");
                }
            }
        }
    });
    info!("TieringManager 시작 (hot_to_cold=30일)");

    // ── StatsReporter 시작 (FR-040: Shard 통계 보고) ─────────────────────
    let grpc_reporter = stats_reporter::GrpcQnReporter { qn_endpoint };
    let (_stats_tx, stats_reporter) = stats_reporter::create_stats_reporter(grpc_reporter);
    tokio::spawn(async move { stats_reporter.run().await });
    info!("StatsReporter 시작");

    // ── gRPC StorageService 기동 (WAL + MemTable 영속 쓰기/읽기) ─────────────
    let grpc_bind = format!("0.0.0.0:{}", grpc_port);
    let grpc_node = node_id.clone();
    let grpc_data = data_dir.clone();
    tokio::spawn(async move {
        let addr: std::net::SocketAddr = grpc_bind.parse().expect("invalid grpc addr");
        if let Err(e) = grpc::server::serve(addr, grpc_node, grpc_data).await {
            tracing::error!(err = %e, "gRPC StorageService error");
        }
    });
    info!(port = grpc_port, "Storage Node gRPC server started (WAL+MemTable)");

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
