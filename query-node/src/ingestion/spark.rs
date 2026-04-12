// T066: axum HTTP Stream Load 엔드포인트 — 포트 8040, TxID 수신, 컬럼 데이터 전달, Commit

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{get, post, put},
    Router,
};
use axum::body::Bytes;
use serde::{Deserialize, Serialize};
use tracing::{error, info};
use uuid::Uuid;

use crate::transaction::TransactionManager;

// ─── HTTP 요청/응답 타입 ─────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct BeginTxParams {
    pub cube: String,
}

#[derive(Debug, Serialize)]
pub struct BeginTxResponse {
    pub tx_id:   String,
    pub success: bool,
    pub message: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CommitTxParams {
    pub tx_id: String,
}

#[derive(Debug, Serialize)]
pub struct CommitTxResponse {
    pub tx_id:      String,
    pub success:    bool,
    pub rows_loaded: u64,
    pub message:    Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StreamLoadResponse {
    pub tx_id:   String,
    pub success: bool,
    pub rows:    u64,
    pub message: Option<String>,
}

// ─── 앱 상태 ──────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct StreamLoadState {
    pub tx_mgr: Arc<TransactionManager>,
    /// tx_id → 수신된 행 수 (실제 구현에서는 CN으로 전달)
    pub pending: Arc<tokio::sync::Mutex<HashMap<String, u64>>>,
}

impl StreamLoadState {
    pub fn new(tx_mgr: Arc<TransactionManager>) -> Self {
        Self {
            tx_mgr,
            pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }
}

// ─── 핸들러 ──────────────────────────────────────────────────────────────────

/// GET /api/stream-load/begin?cube=<name>
/// Spark Driver가 TxID를 요청
async fn begin_tx(
    State(state): State<StreamLoadState>,
    Query(params): Query<BeginTxParams>,
) -> Json<BeginTxResponse> {
    match state.tx_mgr.begin(&params.cube).await {
        Ok(tx_id) => {
            info!(tx_id = %tx_id, cube = %params.cube, "Stream Load TX started");
            state.pending.lock().await.insert(tx_id.clone(), 0);
            Json(BeginTxResponse { tx_id, success: true, message: None })
        }
        Err(e) => {
            error!("Failed to begin TX: {}", e);
            Json(BeginTxResponse {
                tx_id:   String::new(),
                success: false,
                message: Some(e.to_string()),
            })
        }
    }
}

/// PUT /api/stream-load/<tx_id>
/// Spark Executor가 컬럼 데이터를 직접 POST (CSV 또는 Arrow IPC)
async fn stream_load(
    State(state): State<StreamLoadState>,
    axum::extract::Path(tx_id): axum::extract::Path<String>,
    body: Bytes,
) -> Json<StreamLoadResponse> {
    // TX 상태 확인
    let tx_state = state.tx_mgr.get_state(&tx_id).await;
    if tx_state.is_none() {
        return Json(StreamLoadResponse {
            tx_id:   tx_id.clone(),
            success: false,
            rows:    0,
            message: Some("Transaction not found".into()),
        });
    }

    // 행 수 추정 (실제로는 CN으로 전달 후 처리)
    let rows_estimate = estimate_rows(&body);

    let mut pending = state.pending.lock().await;
    *pending.entry(tx_id.clone()).or_insert(0) += rows_estimate;

    info!(
        tx_id = %tx_id,
        bytes = body.len(),
        rows_estimate,
        "Stream Load data received"
    );

    // TODO (Phase D): 실제 CN gRPC 전달
    // let cn_client = get_cn_client();
    // cn_client.ingest_columnar(tx_id, body).await?;

    Json(StreamLoadResponse {
        tx_id,
        success: true,
        rows:    rows_estimate,
        message: None,
    })
}

/// POST /api/stream-load/commit
/// Spark Driver가 모든 Executor 완료 후 Commit
async fn commit_tx(
    State(state): State<StreamLoadState>,
    Json(params): Json<CommitTxParams>,
) -> Json<CommitTxResponse> {
    let tx_id = &params.tx_id;

    let rows_loaded = state.pending.lock().await
        .remove(tx_id)
        .unwrap_or(0);

    // Prepare (참가자 없으므로 단순 commit)
    match state.tx_mgr.mark_committed(tx_id).await {
        Ok(()) => {
            info!(tx_id = %tx_id, rows_loaded, "Stream Load TX committed");
            Json(CommitTxResponse {
                tx_id:       tx_id.clone(),
                success:     true,
                rows_loaded,
                message:     None,
            })
        }
        Err(e) => {
            // mark_committed 실패 시 rollback
            let _ = state.tx_mgr.rollback(tx_id).await;
            error!(tx_id = %tx_id, "Commit failed: {}", e);
            Json(CommitTxResponse {
                tx_id:       tx_id.clone(),
                success:     false,
                rows_loaded: 0,
                message:     Some(e.to_string()),
            })
        }
    }
}

/// POST /api/stream-load/abort
async fn abort_tx(
    State(state): State<StreamLoadState>,
    Json(params): Json<CommitTxParams>,
) -> Json<CommitTxResponse> {
    let tx_id = &params.tx_id;
    state.pending.lock().await.remove(tx_id);
    let _ = state.tx_mgr.rollback(tx_id).await;
    info!(tx_id = %tx_id, "Stream Load TX aborted");
    Json(CommitTxResponse {
        tx_id:       tx_id.clone(),
        success:     true,
        rows_loaded: 0,
        message:     Some("Aborted".into()),
    })
}

// ─── 행 수 추정 (Content-Type 미확인 간이 구현) ────────────────────────────────

fn estimate_rows(body: &[u8]) -> u64 {
    // CSV: 줄바꿈 수 기준
    body.iter().filter(|&&b| b == b'\n').count() as u64
}

// ─── Router 생성 ─────────────────────────────────────────────────────────────

pub fn stream_load_router(state: StreamLoadState) -> Router {
    Router::new()
        .route("/api/stream-load/begin",  get(begin_tx))
        .route("/api/stream-load/:tx_id", put(stream_load))
        .route("/api/stream-load/commit", post(commit_tx))
        .route("/api/stream-load/abort",  post(abort_tx))
        .with_state(state)
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_rows() {
        let csv = b"a,b,c\n1,2,3\n4,5,6\n";
        assert_eq!(estimate_rows(csv), 3);
    }

    #[test]
    fn test_estimate_rows_empty() {
        assert_eq!(estimate_rows(b""), 0);
    }

    #[tokio::test]
    async fn test_stream_load_state_new() {
        let state = StreamLoadState::new(Arc::new(TransactionManager::new()));
        let tx_id = state.tx_mgr.begin("events").await.unwrap();
        assert!(!tx_id.is_empty());
        state.pending.lock().await.insert(tx_id.clone(), 0);
        assert!(state.pending.lock().await.contains_key(&tx_id));
    }
}
