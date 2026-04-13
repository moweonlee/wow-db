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

use anyhow::Result;
use axum::{routing::get, Router, Json};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "compute_node=info,shared=info".into()),
        )
        .init();

    let node_id = std::env::var("NODE_ID").unwrap_or_else(|_| "cn-1".to_string());
    let grpc_port: u16 = std::env::var("GRPC_PORT")
        .unwrap_or_else(|_| "9040".to_string())
        .parse().unwrap_or(9040);
    let sn_addrs = std::env::var("STORAGE_NODES")
        .unwrap_or_else(|_| "sn-1:9060,sn-2:9060,sn-3:9060".to_string());

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

    // ── HTTP 서버 기동 (헬스체크) ────────────────────────────────────────────
    // gRPC 서버가 구현되면 별도 포트로 분리; 지금은 health 만 제공
    let nid = node_id.clone();
    let router = Router::new()
        .route("/health", get(|| async { Json(serde_json::json!({ "status": "ok" })) }))
        .route("/healthz", get(|| async { Json(serde_json::json!({ "status": "ok" })) }))
        .route("/api/v1/info", get(move || {
            let id = nid.clone();
            async move { Json(serde_json::json!({ "node_id": id, "role": "compute" })) }
        }));

    let addr     = format!("0.0.0.0:{}", grpc_port);
    let listener = TcpListener::bind(&addr).await?;
    info!(port = grpc_port, "Compute Node HTTP server listening");

    // TODO (Phase C): gRPC ComputeService 서버 기동 (tonic)

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
