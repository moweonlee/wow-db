// T027: Storage Node gRPC 서버 스켈레톤
// StorageService: Health, WriteRows, ScanTablet (stream), Prepare/Commit/Rollback

use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info, warn};
use uuid::Uuid;

use shared::types::{LsmScanRange, ShardPredicate, ShardPredicateOp, ShardScanRequest, Value};

// proto 생성 타입 (cargo build 후 src/gen/ 에 생성됨)
use crate::gen::wowdb::{
    compute::{HealthRequest, HealthResponse},
    storage::{
        storage_service_server::{StorageService, StorageServiceServer},
        shard_scan_response::Payload,
        CommitRequest, CommitResponse, PrepareRequest, PrepareResponse,
        GetPartListRequest, GetShardInfoRequest, ShardInfoResponse, PartInfo,
        PartListResponse, PartMeta, RecordBatchChunk, RollbackRequest, RollbackResponse,
        ScanBatch, ScanRequest, ScanStatsResponse, ShardScanRequest as ProtoShardScanRequest,
        ShardScanResponse, ShardStatsAck, ShardStatsReport,
        TabletMetaRequest, TabletMetaResponse, WriteRequest, WriteResponse,
    },
};

use crate::grpc::scan::{ShardScanner, ShardScanItem};
use crate::grpc::write::TabletWriterRegistry;

// ─── 서비스 구현체 ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct StorageServiceImpl {
    node_id:  Arc<String>,
    data_dir: Arc<String>,
    /// TabletWriterRegistry: WAL + MemTable 에 실제 데이터를 쓰고 읽는 핵심 컴포넌트
    registry: Arc<TabletWriterRegistry>,
}

impl StorageServiceImpl {
    pub fn new(node_id: String, data_dir: String) -> Self {
        let registry = Arc::new(TabletWriterRegistry::new(PathBuf::from(&data_dir)));
        Self {
            node_id:  Arc::new(node_id),
            data_dir: Arc::new(data_dir),
            registry,
        }
    }

    pub fn new_with_registry(node_id: String, data_dir: String, registry: Arc<TabletWriterRegistry>) -> Self {
        Self {
            node_id:  Arc::new(node_id),
            data_dir: Arc::new(data_dir),
            registry,
        }
    }
}

type ScanStream      = Pin<Box<dyn futures::Stream<Item = Result<ScanBatch,        Status>> + Send + 'static>>;
type ShardScanStream = Pin<Box<dyn futures::Stream<Item = Result<ShardScanResponse, Status>> + Send + 'static>>;
type PartInfoStream  = Pin<Box<dyn futures::Stream<Item = Result<PartInfo,          Status>> + Send + 'static>>;

#[tonic::async_trait]
impl StorageService for StorageServiceImpl {
    type ScanTabletStream    = ScanStream;
    type ScanShardStream     = ShardScanStream;
    type GetPartListStream   = PartInfoStream;

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
        let tablet_id = r.tablet_id.clone();
        info!(tablet_id = %tablet_id, "WriteRows — WAL + MemTable");

        // batch 필드에 JSON 직렬화된 행 목록이 들어있음
        // 형식: Vec<HashMap<String, serde_json::Value>>
        let rows_json: Vec<serde_json::Map<String, serde_json::Value>> =
            serde_json::from_slice(&r.batch)
                .map_err(|e| Status::invalid_argument(format!("batch JSON parse error: {e}")))?;

        // JSON → WriteRow 변환
        use crate::grpc::write::WriteRow;
        use std::collections::HashMap;
        use bytes::Bytes;

        let write_rows: Vec<WriteRow> = rows_json
            .iter()
            .enumerate()
            .map(|(i, map)| {
                let sort_key_bytes = i.to_be_bytes().to_vec();
                let columns: HashMap<String, Bytes> = map
                    .iter()
                    .map(|(k, v)| {
                        let bytes = match v {
                            serde_json::Value::String(s) => Bytes::from(s.clone()),
                            other => Bytes::from(other.to_string()),
                        };
                        (k.clone(), bytes)
                    })
                    .collect();
                WriteRow { sort_key_bytes, columns, tx_id: 0 }
            })
            .collect();

        let count = write_rows.len() as u64;
        let writer = self.registry.get_or_create(&tablet_id).await
            .map_err(|e| Status::internal(format!("TabletWriter error: {e}")))?;

        writer.write_rows(write_rows).await
            .map_err(|e| Status::internal(format!("write_rows error: {e}")))?;

        info!(tablet_id = %tablet_id, rows = count, "WriteRows OK — WAL + MemTable");
        Ok(Response::new(WriteResponse { success: true, lsn: count, error: String::new() }))
    }

    async fn scan_tablet(&self, req: Request<ScanRequest>) -> Result<Response<ScanStream>, Status> {
        let r = req.into_inner();
        let tablet_id = r.tablet_id.clone();
        info!(tablet_id = %tablet_id, "ScanTablet — MemTable scan");

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<ScanBatch, Status>>(16);

        let registry = self.registry.clone();
        tokio::spawn(async move {
            let writer = match registry.get_or_create(&tablet_id).await {
                Ok(w)  => w,
                Err(e) => {
                    let _ = tx.send(Err(Status::internal(format!("TabletWriter error: {e}")))).await;
                    return;
                }
            };

            let rows = writer.scan_memtable_rows().await;
            let row_count = rows.len() as u64;

            // rows → JSON → ScanBatch.batch
            let json_rows: Vec<serde_json::Map<String, serde_json::Value>> = rows
                .into_iter()
                .map(|(_key, cols)| {
                    let mut map = serde_json::Map::new();
                    for (col_name, col_bytes) in cols {
                        let val = String::from_utf8(col_bytes.to_vec())
                            .map(serde_json::Value::String)
                            .unwrap_or_else(|_| serde_json::Value::Null);
                        map.insert(col_name, val);
                    }
                    map
                })
                .collect();

            let batch_bytes = serde_json::to_vec(&json_rows).unwrap_or_default();

            let batch = ScanBatch {
                tablet_id:        tablet_id.clone(),
                batch:            batch_bytes,
                is_last:          true,
                rows_read:        row_count,
                granules_scanned: 0,
                granules_skipped: 0,
            };

            let _ = tx.send(Ok(batch)).await;
            info!(tablet_id = %tablet_id, rows = row_count, "ScanTablet OK");
        });

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

    // ─── ShardScan (T134, FR-041) ────────────────────────────────────────────

    async fn scan_shard(
        &self,
        req: Request<ProtoShardScanRequest>,
    ) -> Result<Response<ShardScanStream>, Status> {
        let r         = req.into_inner();
        let shard_dir = PathBuf::from(&r.shard_dir);
        let shard_id_bytes = &r.shard_id;
        let shard_id_str   = if shard_id_bytes.len() == 16 {
            Uuid::from_slice(shard_id_bytes)
                .map(|u| u.to_string())
                .unwrap_or_else(|_| format!("{:02x?}", &shard_id_bytes[..8.min(shard_id_bytes.len())]))
        } else {
            format!("{:02x?}", &shard_id_bytes[..8.min(shard_id_bytes.len())])
        };

        info!(shard_id = %shard_id_str, shard_dir = %r.shard_dir, "ScanShard 시작");

        // proto → shared::types 변환
        let scan_range = r.scan_range.map(|sr| LsmScanRange {
            min_level: sr.min_level,
            max_level: sr.max_level,
        }).unwrap_or_else(LsmScanRange::all_levels);

        let predicates: Vec<ShardPredicate> = r.predicates.iter().map(|p| {
            let op = match p.op.as_str() {
                "eq"          => ShardPredicateOp::Eq,
                "ne"          => ShardPredicateOp::Ne,
                "lt"          => ShardPredicateOp::Lt,
                "le"          => ShardPredicateOp::Le,
                "gt"          => ShardPredicateOp::Gt,
                "ge"          => ShardPredicateOp::Ge,
                "is_null"     => ShardPredicateOp::IsNull,
                "is_not_null" => ShardPredicateOp::IsNotNull,
                _             => ShardPredicateOp::Eq,
            };
            let value = if p.value.is_empty() {
                None
            } else {
                serde_json::from_slice::<Value>(&p.value).ok()
            };
            ShardPredicate { column: p.column.clone(), op, value }
        }).collect();

        let shard_id_uuid = if shard_id_bytes.len() == 16 {
            Uuid::from_slice(shard_id_bytes).unwrap_or_else(|_| Uuid::new_v4())
        } else {
            Uuid::new_v4()
        };

        let internal_req = ShardScanRequest {
            shard_id:         shard_id_uuid,
            shard_dir:        shard_dir.clone(),
            columns:          r.columns.clone(),
            predicates,
            scan_range,
            bloom_probe_keys: r.bloom_probe_keys.clone(),
        };

        let scanner = ShardScanner::new(shard_dir);
        let (item_tx, mut item_rx) = mpsc::channel::<Result<ShardScanItem>>(32);

        tokio::spawn(async move {
            if let Err(e) = scanner.scan(&internal_req, item_tx).await {
                warn!(shard_id = %shard_id_str, err = %e, "ShardScanner 오류");
            }
        });

        // ShardScanItem → ShardScanResponse 변환 스트림
        let (resp_tx, resp_rx) = mpsc::channel::<Result<ShardScanResponse, Status>>(32);
        tokio::spawn(async move {
            while let Some(item) = item_rx.recv().await {
                let resp = match item {
                    Ok(ShardScanItem::PartList(parts)) => {
                        let part_metas: Vec<PartMeta> = parts.iter().map(|s| PartMeta {
                            part_id:          s.id.as_bytes().to_vec(),
                            level:            s.level,
                            sequence_num:     s.sequence_num,
                            row_count:        s.row_count,
                            min_sort_key:     s.min_sort_key.clone(),
                            max_sort_key:     s.max_sort_key.clone(),
                            size_bytes:       s.size_bytes,
                            bloom_size_bytes: 0,
                            created_at_ms:    0,
                        }).collect();
                        Ok(ShardScanResponse {
                            payload: Some(Payload::PartList(PartListResponse { parts: part_metas })),
                        })
                    }
                    Ok(ShardScanItem::Batch { ipc_bytes, rows, is_last }) => {
                        Ok(ShardScanResponse {
                            payload: Some(Payload::Batch(RecordBatchChunk {
                                ipc_batch: ipc_bytes,
                                rows,
                                is_last,
                            })),
                        })
                    }
                    Ok(ShardScanItem::Stats(s)) => {
                        Ok(ShardScanResponse {
                            payload: Some(Payload::Stats(ScanStatsResponse {
                                parts_scanned:    s.parts_scanned,
                                parts_skipped:    s.parts_skipped,
                                granules_read:    s.granules_read,
                                granules_skipped: s.granules_skipped,
                                rows_returned:    s.rows_returned,
                                bytes_read:       s.bytes_read,
                            })),
                        })
                    }
                    Err(e) => Err(Status::internal(e.to_string())),
                };
                if resp_tx.send(resp).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(resp_rx))))
    }

    // ─── GetPartList (T137: SHOW PARTS 데이터 연동) ──────────────────────────
    // QN이 특정 Shard의 Part 목록을 SN에서 스트리밍으로 수신한다.
    // 현재는 빈 스트림을 반환 (Phase B에서 LSM Part 목록 연동 예정).

    async fn get_part_list(
        &self,
        req: Request<GetPartListRequest>,
    ) -> Result<Response<PartInfoStream>, Status> {
        let r = req.into_inner();
        info!(
            shard_id = %format!("{:02x?}", &r.shard_id[..8.min(r.shard_id.len())]),
            "GetPartList 요청 수신 (stub — 빈 스트림 반환)"
        );
        // TODO (Phase B): SN LSM 엔진에서 실제 Part 목록 조회
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<PartInfo, Status>>(1);
        drop(tx); // 즉시 스트림 종료
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    // ─── GetShardInfo (T137: SHOW SHARDS 데이터 연동) ────────────────────────
    // QN이 특정 Shard의 요약 통계(row_count, size_bytes, part_count, lsn)를 조회한다.
    // 현재는 0으로 채운 더미 응답 반환 (Phase B에서 실제 통계 연동 예정).

    async fn get_shard_info(
        &self,
        req: Request<GetShardInfoRequest>,
    ) -> Result<Response<ShardInfoResponse>, Status> {
        let r = req.into_inner();
        info!(
            shard_id = %format!("{:02x?}", &r.shard_id[..8.min(r.shard_id.len())]),
            "GetShardInfo 요청 수신 (stub — 더미 통계 반환)"
        );
        // TODO (Phase B): SN LSM 엔진에서 실제 Shard 통계 조회
        Ok(Response::new(ShardInfoResponse {
            row_count:  0,
            size_bytes: 0,
            part_count: 0,
            lsn:        0,
        }))
    }

    // ─── ReportShardStats (FR-040) ───────────────────────────────────────────

    async fn report_shard_stats(
        &self,
        req: Request<ShardStatsReport>,
    ) -> Result<Response<ShardStatsAck>, Status> {
        let r = req.into_inner();
        info!(
            shard_id     = %format!("{:02x?}", &r.shard_id[..8.min(r.shard_id.len())]),
            row_count    = r.row_count,
            is_incremental = r.is_incremental,
            "ReportShardStats 수신 (QN 전달용 — 현재 stub)"
        );
        // TODO: QN gRPC 전달 또는 로컬 stats 저장
        Ok(Response::new(ShardStatsAck { success: true, error: String::new() }))
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
