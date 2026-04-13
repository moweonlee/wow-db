// T082: axum HTTP/WebSocket 서버 — 포트 8080, 정적 파일 서빙, API 라우팅

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use axum::{
    Router,
    extract::{State, WebSocketUpgrade, ws::{Message, WebSocket}},
    response::IntoResponse,
    routing::{get, post},
    Json,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::meta::cube::CubeManager;
use crate::raft::RaftManager;
use crate::session_mv::manager::SmvManager;

// ─── 앱 상태 ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct WebUiState {
    pub cube_mgr: Arc<CubeManager>,
    pub smv_mgr:  Arc<SmvManager>,
    pub raft:     Arc<RaftManager>,
}

// ─── 라우터 빌드 ─────────────────────────────────────────────────────────────

pub fn build_router(state: WebUiState) -> Router {
    Router::new()
        // 헬스체크
        .route("/healthz", get(health_handler))
        .route("/health",  get(health_handler))
        // Web UI API
        .route("/api/cubes",        get(super::api::list_cubes))
        .route("/api/cubes/:name",  get(super::api::get_cube))
        .route("/api/cubes",        post(super::api::create_cube))
        // SMV 마법사
        .route("/api/smv/wizard/preview",  post(super::smv_wizard::preview_smv))
        .route("/api/smv/wizard/create",   post(super::smv_wizard::create_smv_handler))
        .route("/api/smv",                 get(super::smv_wizard::list_smv_handler))
        // SQL 에디터
        .route("/api/sql/execute",     post(super::sql_editor::execute_sql))
        .route("/api/sql/history",     get(super::sql_editor::query_history))
        // 모니터링
        .route("/api/v1/cluster",    get(super::monitoring::cluster_overview))
        .route("/api/v1/profiler",   get(super::monitoring::profiler_summary))
        .route("/metrics",           get(super::monitoring::prometheus_metrics))
        // WebSocket SQL 스트리밍
        .route("/ws/sql", get(ws_sql_handler))
        .with_state(state)
}

/// Web UI 서버 기동
pub async fn start(state: WebUiState, port: u16) -> Result<()> {
    let router = build_router(state);
    let addr   = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;

    info!(port, "Web UI HTTP server listening");
    axum::serve(listener, router).await?;
    Ok(())
}

// ─── 핸들러 ──────────────────────────────────────────────────────────────────

async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

/// WebSocket SQL 스트리밍 핸들러
async fn ws_sql_handler(
    ws: WebSocketUpgrade,
    State(state): State<WebUiState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws_sql(socket, state))
}

async fn handle_ws_sql(mut socket: WebSocket, _state: WebUiState) {
    while let Some(msg) = socket.recv().await {
        match msg {
            Ok(Message::Text(sql)) => {
                info!(sql = %sql, "WebSocket SQL received");
                // TODO (Phase D): 실제 쿼리 실행 후 스트리밍 반환
                let resp = serde_json::json!({
                    "status": "stub",
                    "message": format!("Received SQL (stub): {}", sql)
                });
                if socket.send(Message::Text(resp.to_string().into())).await.is_err() {
                    break;
                }
            }
            Ok(Message::Close(_)) => break,
            Err(e) => {
                warn!(err = %e, "WebSocket error");
                break;
            }
            _ => {}
        }
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_builds() {
        let raft     = Arc::new(crate::raft::RaftManager::new_local());
        let cube_mgr = Arc::new(CubeManager::new(raft.clone()));
        let smv_mgr  = Arc::new(SmvManager::new(raft.clone()));

        let state = WebUiState { cube_mgr, smv_mgr, raft };
        let _router = build_router(state);
        // 라우터가 패닉 없이 생성되면 통과
    }
}
