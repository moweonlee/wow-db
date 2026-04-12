// Compute Node — WOW-DB
// 역할: PhysicalPlan Fragment 실행, SIMD 연산, Analytics 함수, CN 간 Shuffle

mod gen;
mod executor;
mod grpc;
mod runtime_filter;
mod shuffle;
mod ingestion;

use anyhow::Result;
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
        .parse()
        .unwrap_or(9040);
    let sn_addrs = std::env::var("STORAGE_NODES")
        .unwrap_or_else(|_| "sn-1:9060,sn-2:9060,sn-3:9060".to_string());

    // SIMD 지원 여부 로깅
    #[cfg(target_arch = "x86_64")]
    {
        let has_avx2 = is_x86_feature_detected!("avx2");
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
    {
        info!(
            node_id = %node_id,
            grpc_port,
            storage_nodes = %sn_addrs,
            "Compute Node starting (non-x86_64, scalar fallback)"
        );
    }

    // TODO (Phase C):
    // 1. gRPC ComputeService 서버 기동 (tonic, grpc_port)
    // 2. Storage Node 클라이언트 연결 풀 초기화
    // 3. SIMD executor 초기화 (CPUID 기반 AVX2/AVX-512 디스패치)

    tokio::signal::ctrl_c().await?;
    info!("Compute Node shutting down");
    Ok(())
}
