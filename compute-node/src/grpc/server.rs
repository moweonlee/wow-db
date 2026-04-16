// T029: Compute Node gRPC 서버 — ExecuteFragment 실제 구현 (QN→CN→SN 연결)
// ComputeService: ExecuteFragment (stream), CancelFragment, Health

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

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
        info!(query_id = %r.query_id, fragment_id = %r.fragment_id, "ExecuteFragment — QN→CN→SN");

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<FragmentResult, Status>>(16);
        let query_id    = r.query_id.clone();
        let fragment_id = r.fragment_id.clone();
        let plan_bytes  = r.plan.clone();

        tokio::spawn(async move {
            // 1. plan JSON 역직렬화 (QN이 보낸 FragmentPlan)
            let plan: FragmentPlan = match serde_json::from_slice(&plan_bytes) {
                Ok(p) => p,
                Err(e) => {
                    warn!(err = %e, "plan deserialize failed");
                    let _ = tx.send(Err(Status::invalid_argument(format!("plan JSON error: {e}")))).await;
                    return;
                }
            };

            info!(
                query_id    = %query_id,
                fragment_id = %fragment_id,
                table       = %plan.table,
                sn          = %plan.sn_endpoint,
                "ExecuteFragment: scanning SN"
            );

            // 2. SN scan_tablet gRPC 호출
            let rows = match scan_sn(&plan.table, &plan.sn_endpoint).await {
                Ok(r) => r,
                Err(e) => {
                    warn!(err = %e, "SN scan failed");
                    let _ = tx.send(Err(Status::internal(format!("SN scan error: {e}")))).await;
                    return;
                }
            };

            let row_count = rows.len() as u64;
            info!(
                query_id = %query_id,
                rows     = row_count,
                "ExecuteFragment: SN scan OK, sending results"
            );

            // 3. 결과를 JSON bytes 로 직렬화 → FragmentResult.batch
            let batch_bytes = serde_json::to_vec(&rows).unwrap_or_default();

            let result = FragmentResult {
                query_id:    query_id.clone(),
                fragment_id: fragment_id.clone(),
                batch:       batch_bytes,
                is_last:     true,
                metrics:     None,
                error_msg:   String::new(),
            };

            let _ = tx.send(Ok(result)).await;
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

// ─── Fragment Plan (QN 이 직렬화하여 CN 에 전달) ─────────────────────────────

/// QN 에서 CN 으로 전달되는 실행 계획 (JSON 직렬화)
#[derive(Debug, Serialize, Deserialize)]
pub struct FragmentPlan {
    pub table:       String,
    pub columns:     Vec<String>,
    pub sn_endpoint: String,
    pub sql:         String,
}

// ─── SN 스캔 헬퍼 ─────────────────────────────────────────────────────────────

/// CN → SN scan_tablet gRPC 호출 → Vec<HashMap<String, Value>>
async fn scan_sn(
    table:       &str,
    sn_endpoint: &str,
) -> anyhow::Result<Vec<HashMap<String, Value>>> {
    use crate::gen::wowdb::storage::storage_service_client::StorageServiceClient;
    use crate::gen::wowdb::storage::ScanRequest;
    use tokio_stream::StreamExt;

    let addr = format!("http://{}", sn_endpoint);
    let mut client = StorageServiceClient::connect(addr.clone()).await
        .map_err(|e| anyhow::anyhow!("SN connect {}: {}", addr, e))?;

    let req = ScanRequest {
        tablet_id: table.to_string(),
        columns:   vec![],
        predicate: vec![],
        ..Default::default()
    };

    let mut stream = client.scan_tablet(req).await
        .map_err(|e| anyhow::anyhow!("scan_tablet error: {}", e))?
        .into_inner();

    let mut all_rows: Vec<HashMap<String, Value>> = Vec::new();
    loop {
        match stream.next().await {
            Some(Ok(batch)) => {
                if !batch.batch.is_empty() {
                    let parsed: Vec<HashMap<String, Value>> =
                        serde_json::from_slice(&batch.batch).unwrap_or_default();
                    all_rows.extend(parsed);
                }
                if batch.is_last { break; }
            }
            Some(Err(e)) => {
                warn!(table = %table, err = %e, "SN scan stream error");
                break;
            }
            None => break,
        }
    }
    Ok(all_rows)
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
