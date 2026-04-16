// cn_client.rs — QN 에서 CN gRPC ExecuteFragment 를 호출하는 클라이언트
//
// 플로우:
//   SELECT sql → build plan JSON → FragmentRequest.plan → CN gRPC
//   → CN scans SN → FragmentResult stream → rows → QN 결과 반환
//
// Plan JSON 형식:
//   {"table": "events", "columns": [], "sn_endpoint": "127.0.0.1:9060"}

use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::gen::wowdb::compute::{
    compute_service_client::ComputeServiceClient,
    FragmentRequest,
};
use crate::executor::select_exec::SelectResult;

// ── Plan 직렬화 형식 ──────────────────────────────────────────────────────────

/// QN 이 CN 에 전달하는 fragment plan
#[derive(Debug, Serialize, Deserialize)]
pub struct FragmentPlan {
    /// 스캔할 테이블(tablet) 이름
    pub table:       String,
    /// 요청 컬럼 목록 (빈 배열 = 전체)
    pub columns:     Vec<String>,
    /// SN gRPC 주소 (CN 이 SN 에 직접 접속)
    pub sn_endpoint: String,
    /// 원본 SQL (CN 이 추가 필터링에 사용)
    pub sql:         String,
}

// ── 글로벌 CN 클라이언트 ──────────────────────────────────────────────────────

pub static CN: LazyLock<Mutex<CnPool>> =
    LazyLock::new(|| Mutex::new(CnPool::from_env()));

pub struct CnPool {
    endpoints: Vec<String>,
    client:    Option<ComputeServiceClient<Channel>>,
}

impl CnPool {
    pub fn from_env() -> Self {
        let endpoints: Vec<String> = std::env::var("COMPUTE_NODES")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        Self { endpoints, client: None }
    }

    pub fn has_compute(&self) -> bool {
        !self.endpoints.is_empty()
    }

    async fn get_client(&mut self) -> Option<&mut ComputeServiceClient<Channel>> {
        if self.client.is_none() && !self.endpoints.is_empty() {
            let addr = format!("http://{}", &self.endpoints[0]);
            match ComputeServiceClient::connect(addr.clone()).await {
                Ok(c) => {
                    info!(addr = %addr, "ComputeServiceClient connected");
                    self.client = Some(c);
                }
                Err(e) => {
                    warn!(addr = %addr, err = %e, "ComputeServiceClient connect failed");
                }
            }
        }
        self.client.as_mut()
    }

    /// SELECT sql を CN に送って行を受け取る
    pub async fn execute_select(
        &mut self,
        sql:         &str,
        table:       &str,
        sn_endpoint: &str,
    ) -> Option<SelectResult> {
        let client = self.get_client().await?;

        let plan = FragmentPlan {
            table:       table.to_string(),
            columns:     vec![],
            sn_endpoint: sn_endpoint.to_string(),
            sql:         sql.to_string(),
        };
        let plan_bytes = serde_json::to_vec(&plan).unwrap_or_default();

        let query_id    = Uuid::new_v4().to_string();
        let fragment_id = Uuid::new_v4().to_string();

        let req = FragmentRequest {
            query_id:        query_id.clone(),
            fragment_id:     fragment_id.clone(),
            plan:            plan_bytes,
            options:         Default::default(),
            runtime_filters: vec![],
        };

        let mut stream = match client.execute_fragment(req).await {
            Ok(resp) => resp.into_inner(),
            Err(e) => {
                warn!(err = %e, "CN execute_fragment RPC failed");
                return None;
            }
        };

        // FragmentResult stream 수집 → rows
        use tokio_stream::StreamExt;
        let mut all_rows: Vec<HashMap<String, Value>> = Vec::new();
        let mut columns: Vec<String> = Vec::new();

        loop {
            match stream.next().await {
                Some(Ok(result)) => {
                    if !result.error_msg.is_empty() {
                        warn!(err = %result.error_msg, "CN fragment error");
                        return None;
                    }
                    if !result.batch.is_empty() {
                        // batch = JSON Vec<HashMap<String, Value>>
                        let parsed: Vec<HashMap<String, Value>> =
                            serde_json::from_slice(&result.batch).unwrap_or_default();
                        if columns.is_empty() {
                            if let Some(first) = parsed.first() {
                                columns = first.keys().cloned().collect();
                                columns.sort();
                            }
                        }
                        all_rows.extend(parsed);
                    }
                    if result.is_last { break; }
                }
                Some(Err(e)) => {
                    warn!(err = %e, "CN stream error");
                    return None;
                }
                None => break,
            }
        }

        debug!(
            table  = %table,
            rows   = all_rows.len(),
            "CN execute_select OK"
        );

        // HashMap rows → SelectResult rows
        let result_rows: Vec<Vec<Value>> = all_rows.iter().map(|row| {
            columns.iter().map(|col| {
                row.get(col).cloned().unwrap_or(Value::Null)
            }).collect()
        }).collect();

        Some(SelectResult {
            columns,
            rows: result_rows,
        })
    }
}
