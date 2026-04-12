// T029: Compute Node gRPC 서버 스켈레톤
// ComputeService: ExecuteFragment (stream), CancelFragment, Health

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::gen::wowdb::compute::{
    compute_service_server::{ComputeService, ComputeServiceServer},
    CancelRequest, CancelResponse, FragmentRequest, FragmentResult,
    HealthRequest, HealthResponse,
};

// ─── 서비스 구현체 ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ComputeServiceImpl {
    node_id: Arc<String>,
}

impl ComputeServiceImpl {
    pub fn new(node_id: String) -> Self {
        Self { node_id: Arc::new(node_id) }
    }
}

type FragmentStream =
    Pin<Box<dyn futures::Stream<Item = Result<FragmentResult, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl ComputeService for ComputeServiceImpl {
    type ExecuteFragmentStream = FragmentStream;

    async fn health(&self, _req: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status:  1, // SERVING
            version: env!("CARGO_PKG_VERSION").to_string(),
            node_id: (*self.node_id).clone(),
            details: Default::default(),
        }))
    }

    async fn execute_fragment(
        &self,
        req: Request<FragmentRequest>,
    ) -> Result<Response<FragmentStream>, Status> {
        let r = req.into_inner();
        info!(query_id = %r.query_id, fragment_id = %r.fragment_id, "ExecuteFragment");

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<FragmentResult, Status>>(16);
        let query_id    = r.query_id.clone();
        let fragment_id = r.fragment_id.clone();
        let _plan_bytes = Bytes::from(r.plan);

        tokio::spawn(async move {
            info!(query_id = %query_id, fragment_id = %fragment_id, "Fragment stub 실행");
            // TODO (Phase C): pipeline::execute_fragment → Arrow IPC 직렬화
            drop(tx);
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn cancel_fragment(
        &self,
        req: Request<CancelRequest>,
    ) -> Result<Response<CancelResponse>, Status> {
        let r = req.into_inner();
        info!(query_id = %r.query_id, fragment_id = %r.fragment_id, "CancelFragment");
        Ok(Response::new(CancelResponse { success: true, message: String::new() }))
    }
}

// ─── 서버 기동 ────────────────────────────────────────────────────────────────

pub async fn serve(addr: SocketAddr, node_id: String) -> Result<()> {
    let svc = ComputeServiceImpl::new(node_id);
    info!(%addr, "Compute gRPC 서버 시작");
    tonic::transport::Server::builder()
        .add_service(ComputeServiceServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}
