// Storage Node (Data Node) — WOW-DB
// 역할: LSM-Tree 스토리지, 컬럼 파일 I/O, WAL, Compaction, S3/HDFS 백엔드

mod gen;
mod lsm;
mod backend;
mod grpc;
mod columnar;
mod block_cache;
mod index;

use anyhow::Result;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "storage_node=info,shared=info".into()),
        )
        .init();

    let node_id = std::env::var("NODE_ID").unwrap_or_else(|_| "sn-1".to_string());
    let grpc_port: u16 = std::env::var("GRPC_PORT")
        .unwrap_or_else(|_| "9060".to_string())
        .parse()
        .unwrap_or(9060);
    let http_port: u16 = std::env::var("HTTP_PORT")
        .unwrap_or_else(|_| "8040".to_string())
        .parse()
        .unwrap_or(8040);
    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string());
    let backend = std::env::var("STORAGE_BACKEND").unwrap_or_else(|_| "native".to_string());

    // io_uring 지원 여부 로깅
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        info!("io_uring support: ENABLED");
    }
    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
    {
        info!("io_uring support: DISABLED (using tokio::fs fallback)");
    }

    info!(
        node_id = %node_id,
        grpc_port,
        http_port,
        data_dir = %data_dir,
        storage_backend = %backend,
        "Storage Node starting"
    );

    // TODO (Phase B):
    // 1. LSM-Tree 엔진 초기화 (data_dir 기반)
    // 2. WAL 복구 (크래시 복구)
    // 3. gRPC StorageService 서버 기동 (tonic, grpc_port)
    // 4. HTTP Stream Load 서버 기동 (axum, http_port) — Spark 수집용
    // 5. Background Compaction 워커 기동

    tokio::signal::ctrl_c().await?;
    info!("Storage Node shutting down");
    Ok(())
}
