// storage_client.rs — query-node 에서 storage-node gRPC 를 호출하는 클라이언트
//
// INSERT 경로: execute_insert → write_rows_sharded(dist_col 기반 SN 선택)
//              또는 write_rows(단순 첫 SN)
// SELECT 경로: execute_select → scan_all_rows(전체 SN 스캔 머지)
//
// STORAGE_NODES 환경변수 (콤마 구분): SN gRPC 주소 목록
// 예: STORAGE_NODES=127.0.0.1:9060,127.0.0.1:9061,127.0.0.1:9062

use std::collections::HashMap;
use std::sync::LazyLock;

use serde_json::Value;
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
pub static STORAGE: LazyLock<Mutex<StoragePool>> =
    LazyLock::new(|| Mutex::new(StoragePool::from_env()));

// ── StoragePool ────────────────────────────────────────────────────────────────

pub struct StoragePool {
    /// SN gRPC 주소 목록
    endpoints: Vec<String>,
    /// 인덱스별 lazy 연결 (None = 아직 연결 안 됨 또는 실패)
    clients: Vec<Option<StorageServiceClient<Channel>>>,
}

impl StoragePool {
    pub fn from_env() -> Self {
        let endpoints: Vec<String> = std::env::var("STORAGE_NODES")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let n = endpoints.len();
        Self { endpoints, clients: vec![None; n] }
    }

    pub fn has_storage(&self) -> bool { !self.endpoints.is_empty() }

    pub fn sn_count(&self) -> usize { self.endpoints.len() }

    /// SN 인덱스에 lazy 연결, 성공하면 클라이언트 반환
    async fn ensure_client(&mut self, idx: usize) -> bool {
        if idx >= self.endpoints.len() { return false; }
        if self.clients[idx].is_some() { return true; }

        let addr = format!("http://{}", self.endpoints[idx]);
        match StorageServiceClient::connect(addr.clone()).await {
            Ok(c) => {
                info!(addr = %addr, sn_idx = idx, "StoragePool: SN connected");
                self.clients[idx] = Some(c);
                true
            }
            Err(e) => {
                warn!(addr = %addr, sn_idx = idx, err = %e, "StoragePool: SN connect failed");
                false
            }
        }
    }

    // ── 단일 SN 쓰기 (기존 호환용: 첫 SN) ────────────────────────────────────
    pub async fn write_rows(&mut self, table: &str, rows: &[Row]) -> Option<u64> {
        self.write_rows_to(0, table, rows).await
    }

    // ── 특정 SN 인덱스에 쓰기 ─────────────────────────────────────────────────
    pub async fn write_rows_to(&mut self, idx: usize, table: &str, rows: &[Row]) -> Option<u64> {
        if rows.is_empty() { return Some(0); }
        if !self.ensure_client(idx).await { return None; }

        let batch = rows_to_batch(rows);
        let req = WriteRequest { tx_id: "0".to_string(), tablet_id: table.to_string(), batch };

        match self.clients[idx].as_mut()?.write_rows(req).await {
            Ok(resp) => {
                let r = resp.into_inner();
                debug!(table, sn_idx = idx, rows = r.lsn, "write_rows_to OK");
                Some(r.lsn)
            }
            Err(e) => {
                warn!(table, sn_idx = idx, err = %e, "write_rows_to failed — dropping client");
                self.clients[idx] = None;
                None
            }
        }
    }

    /// 분산 키 컬럼(dist_col) 값을 해시하여 SN 인덱스를 결정하고 해당 SN에 쓰기.
    /// rows 는 동일 dist_col 값을 가질 수도 있고 다양할 수 있다.
    /// 각 row 를 개별 shard 로 라우팅하여 그룹별로 한 번에 전송한다.
    pub async fn write_rows_sharded(
        &mut self,
        table:    &str,
        rows:     &[Row],
        dist_col: &str,
    ) -> Option<u64> {
        if self.endpoints.is_empty() { return None; }
        let n = self.endpoints.len();

        // shard_idx → rows 그룹화
        let mut buckets: Vec<Vec<Row>> = vec![Vec::new(); n];
        for row in rows {
            let shard = shard_index(row, dist_col, n);
            buckets[shard].push(row.clone());
        }

        let mut total = 0u64;
        for (idx, bucket) in buckets.iter().enumerate() {
            if bucket.is_empty() { continue; }
            info!(
                table,
                sn_idx = idx,
                rows = bucket.len(),
                dist_col,
                "write_rows_sharded → SN-{}",
                idx + 1
            );
            if let Some(n) = self.write_rows_to(idx, table, bucket).await {
                total += n;
            }
        }
        Some(total)
    }

    /// 모든 SN 을 순차 스캔하여 행을 머지한다.
    /// 각 SN 이 독립적인 shard 를 소유하므로 중복은 없다.
    pub async fn scan_all_rows(&mut self, table: &str) -> Option<Vec<Row>> {
        if self.endpoints.is_empty() { return None; }

        let mut all_rows: Vec<Row> = Vec::new();
        let n = self.endpoints.len();

        for idx in 0..n {
            if !self.ensure_client(idx).await { continue; }

            let req = ScanRequest {
                tablet_id: table.to_string(),
                columns:   vec![],
                predicate: vec![],
                ..Default::default()
            };

            let mut stream = match self.clients[idx].as_mut()?.scan_tablet(req).await {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!(table, sn_idx = idx, err = %e, "scan_all_rows: SN scan failed");
                    self.clients[idx] = None;
                    continue;
                }
            };

            use tokio_stream::StreamExt;
            loop {
                match stream.next().await {
                    Some(Ok(batch)) => {
                        if !batch.batch.is_empty() {
                            let parsed: Vec<serde_json::Map<String, Value>> =
                                serde_json::from_slice(&batch.batch).unwrap_or_default();
                            let count = parsed.len();
                            all_rows.extend(parsed.into_iter().map(|m| m.into_iter().collect()));
                            debug!(table, sn_idx = idx, rows = count, "scan_all_rows: batch received");
                        }
                        if batch.is_last { break; }
                    }
                    Some(Err(e)) => {
                        warn!(table, sn_idx = idx, err = %e, "scan_all_rows: stream error");
                        break;
                    }
                    None => break,
                }
            }
        }

        info!(table, total = all_rows.len(), "scan_all_rows: merged from {} SNs", n);
        Some(all_rows)
    }

    /// 기존 단일 SN 스캔 (하위 호환)
    pub async fn scan_rows(&mut self, table: &str) -> Option<Vec<Row>> {
        self.scan_all_rows(table).await
    }
}

// ── 헬퍼 함수 ─────────────────────────────────────────────────────────────────

fn rows_to_batch(rows: &[Row]) -> Vec<u8> {
    let maps: Vec<serde_json::Map<String, Value>> = rows
        .iter()
        .map(|r| r.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .collect();
    serde_json::to_vec(&maps).unwrap_or_default()
}

/// row 의 dist_col 값을 해시하여 [0, n) 범위의 shard 인덱스를 반환.
fn shard_index(row: &Row, dist_col: &str, n: usize) -> usize {
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;

    let key_str = row.get(dist_col)
        .map(|v| match v {
            Value::String(s) => s.clone(),
            other            => other.to_string(),
        })
        .unwrap_or_else(|| "0".to_string());

    let mut h = DefaultHasher::new();
    key_str.hash(&mut h);
    (h.finish() as usize) % n
}
