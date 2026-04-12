// T027: Storage Node gRPC 서버 스켈레톤
// StorageService: Health, WriteRows, ScanTablet (stream), Prepare/Commit/Rollback

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info};

// proto 생성 타입 (cargo build 후 src/gen/ 에 생성됨)
use crate::gen::wowdb::{
    compute::{HealthRequest, HealthResponse},
    storage::{
        storage_service_server::{StorageService, StorageServiceServer},
        CommitRequest, CommitResponse, PrepareRequest, PrepareResponse,
        RollbackRequest, RollbackResponse, ScanBatch, ScanRequest,
        TabletMetaRequest, TabletMetaResponse, WriteRequest, WriteResponse,
    },
};

// ─── 서비스 구현체 ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct StorageServiceImpl {
    node_id:  Arc<String>,
    data_dir: Arc<String>,
}

impl StorageServiceImpl {
    pub fn new(node_id: String, data_dir: String) -> Self {
        Self {
            node_id:  Arc::new(node_id),
            data_dir: Arc::new(data_dir),
        }
    }
}

type ScanStream = Pin<Box<dyn futures::Stream<Item = Result<ScanBatch, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl StorageService for StorageServiceImpl {
    type ScanTabletStream = ScanStream;

    async fn health(&self, _req: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status:  1, // SERVING
            version: env!("CARGO_PKG_VERSION").to_string(),
            node_id: (*self.node_id).clone(),
            details: Default::default(),
        }))
    }

    async fn write_rows(&self, req: Request<WriteRequest>) -> Result<Response<WriteResponse>, Status> {
        let r = req.into_inner();
        info!(tablet_id = %r.tablet_id, tx_id = %r.tx_id, "WriteRows");
        // TODO (Phase B): WAL append + MemTable insert
        Ok(Response::new(WriteResponse { success: true, lsn: 0, error: String::new() }))
    }

    async fn scan_tablet(&self, req: Request<ScanRequest>) -> Result<Response<ScanStream>, Status> {
        let r = req.into_inner();
        info!(tablet_id = %r.tablet_id, "ScanTablet");
        // TODO (Phase B): SSTable 컬럼 읽기 + 필터 적용
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        drop(tx);
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn prepare(&self, req: Request<PrepareRequest>) -> Result<Response<PrepareResponse>, Status> {
        let r = req.into_inner();
        info!(tx_id = %r.tx_id, "Prepare");
        Ok(Response::new(PrepareResponse { success: true, error: String::new() }))
    }

    async fn commit(&self, req: Request<CommitRequest>) -> Result<Response<CommitResponse>, Status> {
        let r = req.into_inner();
        info!(tx_id = %r.tx_id, "Commit");
        Ok(Response::new(CommitResponse { success: true, error: String::new() }))
    }

    async fn rollback(&self, req: Request<RollbackRequest>) -> Result<Response<RollbackResponse>, Status> {
        let r = req.into_inner();
        info!(tx_id = %r.tx_id, "Rollback");
        Ok(Response::new(RollbackResponse { success: true }))
    }

    async fn get_tablet_meta(&self, req: Request<TabletMetaRequest>) -> Result<Response<TabletMetaResponse>, Status> {
        let r = req.into_inner();
        info!(tablet_id = %r.tablet_id, "GetTabletMeta");
        Err(Status::unimplemented("Phase B: GetTabletMeta not yet implemented"))
    }
}

// ─── 서버 기동 ────────────────────────────────────────────────────────────────

pub async fn serve(addr: SocketAddr, node_id: String, data_dir: String) -> Result<()> {
    let svc = StorageServiceImpl::new(node_id, data_dir);
    info!(%addr, "Storage gRPC 서버 시작");
    tonic::transport::Server::builder()
        .add_service(StorageServiceServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}
