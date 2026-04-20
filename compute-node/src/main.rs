// Compute Node — WOW-DB
// 역할: PhysicalPlan Fragment 실행, SIMD 연산, Analytics 함수, CN 간 Shuffle

mod gen;
mod executor;
mod grpc;
mod runtime_filter;
mod shuffle;
mod ingestion;
mod mv_refresh;
mod analytics;
mod result_cache;
mod startup;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, LazyLock};

use anyhow::Result;
use axum::{routing::get, Router, Json, extract::Query};
use serde::Deserialize;
use tokio::net::TcpListener;
use tracing::info;

use shared::log_buffer::{LogBuffer, LogEntry, LogLevel, LogsResponse, make_entry, now_ms};

// ── 전역 로그 버퍼 ────────────────────────────────────────────────────────────
pub static LOG_BUFFER: LazyLock<Arc<Mutex<LogBuffer>>> =
    LazyLock::new(|| Arc::new(Mutex::new(LogBuffer::new(100))));

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
struct StorageNodesCfg {
    addresses: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Default)]
struct ExecutionCfg {
    dop: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct HttpCfg {
    port: Option<u16>,
}

#[derive(Debug, Deserialize, Default)]
struct ComputeNodeConfig {
    node:                    NodeCfg,
    grpc:                    GrpcCfg,
    #[serde(default)]
    http:                    HttpCfg,
    storage_nodes:           StorageNodesCfg,
    execution:               ExecutionCfg,
}

fn load_config() -> Result<ComputeNodeConfig> {
    if let Some(path) = shared::config::parse_config_path() {
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("Failed to read config file '{}': {}", path, e))?;
        let cfg: ComputeNodeConfig = toml::from_str(&raw)
            .map_err(|e| anyhow::anyhow!("Failed to parse config file '{}': {}", path, e))?;
        info!(path = %path, "Loaded config from file");
        Ok(cfg)
    } else {
        Ok(ComputeNodeConfig::default())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // ── 로깅 초기화 (파일 + 콘솔 듀얼 싱크, 일별 로테이션) ──────────────────
    let log_dir = std::env::var("LOG_DIR")
        .unwrap_or_else(|_| "./logs".to_string());
    let _log_guard = shared::logging::init_logging(
        "compute-node",
        &log_dir,
        "compute_node=info,shared=info",
    );

    // ── 설정 로드 (TOML 파일 → env var 오버라이드 순) ────────────────────────
    let cfg = load_config()?;

    let node_id = std::env::var("NODE_ID")
        .unwrap_or_else(|_| cfg.node.id.clone().unwrap_or_else(|| "cn-1".to_string()));

    let grpc_port: u16 = std::env::var("GRPC_PORT")
        .ok().and_then(|v| v.parse().ok())
        .unwrap_or_else(|| cfg.grpc.port.unwrap_or(9040));

    // HTTP health check 포트 (gRPC 포트 + 1000, 기본 10040)
    let http_port: u16 = std::env::var("HTTP_PORT")
        .ok().and_then(|v| v.parse().ok())
        .unwrap_or_else(|| cfg.http.port.unwrap_or(grpc_port + 1000));

    let sn_addrs = std::env::var("STORAGE_NODES")
        .unwrap_or_else(|_| {
            cfg.storage_nodes.addresses
                .as_ref()
                .map(|v| v.join(","))
                .unwrap_or_else(|| "sn-1:9060,sn-2:9060,sn-3:9060".to_string())
        });

    let _dop = std::env::var("EXECUTION_DOP")
        .ok().and_then(|v| v.parse().ok())
        .unwrap_or_else(|| cfg.execution.dop.unwrap_or(num_cpus()));

    // SIMD 지원 여부 로깅
    #[cfg(target_arch = "x86_64")]
    {
        let has_avx2    = is_x86_feature_detected!("avx2");
        let has_avx512f = is_x86_feature_detected!("avx512f");
        info!(
            node_id = %node_id,
            grpc_port,
            has_avx2,
            has_avx512f,
            storage_nodes = %sn_addrs,
            "Compute Node starting"
        );
    }
    #[cfg(not(target_arch = "x86_64"))]
    info!(node_id = %node_id, grpc_port, storage_nodes = %sn_addrs, "Compute Node starting");

    // ── 클러스터 자가 등록 (QN_HTTP_PEERS 설정 시) ────────────────────────────
    let cn_grpc_addr = format!("127.0.0.1:{}", grpc_port);
    startup::register_with_cluster(&node_id, &cn_grpc_addr).await?;

    // ── gRPC ComputeService 기동 (QN 의 ExecuteFragment 요청 수신) ──────────
    let grpc_addr_str = format!("0.0.0.0:{}", grpc_port);
    let grpc_node     = node_id.clone();
    tokio::spawn(async move {
        let addr: std::net::SocketAddr = grpc_addr_str.parse().expect("invalid grpc addr");
        if let Err(e) = grpc::server::serve(addr, grpc_node).await {
            tracing::error!(err = %e, "Compute gRPC server error");
        }
    });
    info!(port = grpc_port, "Compute Node gRPC server started (ExecuteFragment)");

    // 시작 로그 기록
    LOG_BUFFER.lock().unwrap().push(make_entry(
        LogLevel::Info, "compute_node", "Compute Node started",
        vec![("node_id", &node_id), ("grpc_port", &grpc_port.to_string())],
    ));

    // ── HTTP 서버 기동 (헬스체크 + /logs) ────────────────────────────────────
    let nid = node_id.clone();
    let nid2 = node_id.clone();
    let router = Router::new()
        .route("/health", get(|| async { Json(serde_json::json!({ "status": "ok" })) }))
        .route("/healthz", get(|| async { Json(serde_json::json!({ "status": "ok" })) }))
        .route("/api/v1/info", get(move || {
            let id = nid.clone();
            async move { Json(serde_json::json!({ "node_id": id, "role": "compute" })) }
        }))
        .route("/logs", get({
            let nid = nid2.clone();
            move |q: Query<LogQuery>| {
                let nid = nid.clone();
                async move {
                    let min_level = q.level.as_deref()
                        .map(LogLevel::from_str)
                        .unwrap_or(LogLevel::Debug);
                    let buf = LOG_BUFFER.lock().unwrap();
                    let entries = buf.recent_by_level(&min_level, 100);
                    let total = buf.len();
                    Json(LogsResponse { node_id: nid, role: "compute".into(), entries, total_buffered: total })
                }
            }
        }));

    let http_addr = format!("0.0.0.0:{}", http_port);
    let listener  = TcpListener::bind(&http_addr).await?;
    info!(grpc_port, http_port, "Compute Node HTTP server listening");

    tokio::select! {
        result = axum::serve(listener, router) => {
            if let Err(e) = result { tracing::error!(err = %e, "HTTP server error"); }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Compute Node shutting down");
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct LogQuery {
    level: Option<String>,
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
}
