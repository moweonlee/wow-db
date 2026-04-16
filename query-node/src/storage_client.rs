// storage_client.rs — query-node 에서 storage-node gRPC 를 호출하는 클라이언트
//
// INSERT 경로: execute_insert → STORAGE.write_rows(table, rows)
// SELECT 경로: execute_select → STORAGE.scan_rows(table) → Vec<Row>
//
// STORAGE_NODES 환경변수 (콤마 구분) 에서 첫 번째 SN 주소를 사용.
// 환경변수 없으면 in-memory MEM_STORE 로 폴백.

use std::sync::LazyLock;

use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::gen::wowdb::storage::{
    storage_service_client::StorageServiceClient,
    ScanRequest, WriteRequest,
};

// ── 타입 별칭 ──────────────────────────────────────────────────────────────────
pub type Row = HashMap<String, Value>;

// ── 글로벌 클라이언트 ─────────────────────────────────────────────────────────
//
// Option<StorageClient> — None 이면 SN 없음 (MEM_STORE 폴백)
pub static STORAGE: LazyLock<Mutex<StoragePool>> =
    LazyLock::new(|| Mutex::new(StoragePool::from_env()));

// ── StoragePool ────────────────────────────────────────────────────────────────

pub struct StoragePool {
    /// 연결할 SN 주소 목록
    endpoints: Vec<String>,
    /// 연결된 클라이언트 (lazy 연결)
    client: Option<StorageServiceClient<Channel>>,
}

impl StoragePool {
    pub fn from_env() -> Self {
        let endpoints: Vec<String> = std::env::var("STORAGE_NODES")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        Self { endpoints, client: None }
    }

    /// 첫 번째 SN 에 lazy 연결. 실패하면 None 반환.
    pub async fn get_client(&mut self) -> Option<&mut StorageServiceClient<Channel>> {
        if self.client.is_none() && !self.endpoints.is_empty() {
            let addr = format!("http://{}", &self.endpoints[0]);
            match StorageServiceClient::connect(addr.clone()).await {
                Ok(c) => {
                    info!(addr = %addr, "StorageServiceClient connected");
                    self.client = Some(c);
                }
                Err(e) => {
                    warn!(addr = %addr, err = %e, "StorageServiceClient connect failed — MEM_STORE fallback");
                }
            }
        }
        self.client.as_mut()
    }

    /// SN 가용 여부
    pub fn has_storage(&self) -> bool {
        !self.endpoints.is_empty()
    }

    /// table 에 rows 를 gRPC WriteRows 로 저장.
    /// 반환: 저장된 행 수. SN 없으면 None (MEM_STORE 폴백용).
    pub async fn write_rows(&mut self, table: &str, rows: &[Row]) -> Option<u64> {
        if rows.is_empty() { return Some(0); }
        let client = self.get_client().await?;

        // rows → JSON bytes
        let json_rows: Vec<serde_json::Map<String, Value>> = rows
            .iter()
            .map(|r| {
                r.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
            })
            .collect();
        let batch = serde_json::to_vec(&json_rows).unwrap_or_default();

        let req = WriteRequest {
            tx_id:     "0".to_string(),
            tablet_id: table.to_string(),
            batch,
        };

        match client.write_rows(req).await {
            Ok(resp) => {
                let r = resp.into_inner();
                debug!(table = %table, rows = r.lsn, "StorageClient write_rows OK");
                Some(r.lsn)
            }
            Err(e) => {
                warn!(table = %table, err = %e, "StorageClient write_rows failed");
                None
            }
        }
    }

    /// table 을 gRPC ScanTablet 으로 스캔.
    /// 반환: Row 목록. SN 없으면 None (MEM_STORE 폴백용).
    pub async fn scan_rows(&mut self, table: &str) -> Option<Vec<Row>> {
        let client = self.get_client().await?;

        let req = ScanRequest {
            tablet_id: table.to_string(),
            columns:   vec![],
            predicate: vec![],
            ..Default::default()
        };

        let mut stream = match client.scan_tablet(req).await {
            Ok(resp) => resp.into_inner(),
            Err(e) => {
                warn!(table = %table, err = %e, "StorageClient scan_tablet failed");
                return None;
            }
        };

        use tokio_stream::StreamExt;
        let mut all_rows: Vec<Row> = Vec::new();
        loop {
            match stream.next().await {
                Some(Ok(batch)) => {
                    if batch.batch.is_empty() {
                        if batch.is_last { break; }
                        continue;
                    }
                    let parsed: Vec<serde_json::Map<String, Value>> =
                        serde_json::from_slice(&batch.batch).unwrap_or_default();
                    for map in parsed {
                        all_rows.push(map.into_iter().collect());
                    }
                    if batch.is_last { break; }
                }
                Some(Err(e)) => {
                    warn!(table = %table, err = %e, "StorageClient scan stream error");
                    break;
                }
                None => break,
            }
        }

        debug!(table = %table, rows = all_rows.len(), "StorageClient scan_rows OK");
        Some(all_rows)
    }
}
