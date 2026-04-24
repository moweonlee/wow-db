// T082: axum HTTP/WebSocket 서버 — 포트 8080, 정적 파일 서빙, API 라우팅

use std::collections::HashMap;
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
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::meta::cube::CubeManager;
use crate::raft::RaftManager;
use crate::session_mv::manager::SmvManager;

// ─── 노드 레지스트리 ──────────────────────────────────────────────────────────

/// key: "{node_type}:{node_id}"  (예: "storage:sn-local-1")
pub type NodeRegistry = RwLock<HashMap<String, super::monitoring::NodeStatus>>;

// ─── 앱 상태 ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct WebUiState {
    pub cube_mgr: Arc<CubeManager>,
    pub smv_mgr:  Arc<SmvManager>,
    pub raft:     Arc<RaftManager>,
    /// 이 QN의 NODE_ID (환경변수 NODE_ID 또는 설정에서 로드)
    pub node_id:  String,
    /// 동적 노드 등록 레지스트리 (QN/SN/CN이 HTTP POST로 자가 등록)
    pub nodes:    Arc<NodeRegistry>,
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
        .route("/api/v1/cluster",           get(super::monitoring::cluster_overview))
        .route("/api/v1/nodes/register",    post(super::monitoring::register_node))
        .route("/api/v1/profiler",          get(super::monitoring::profiler_summary))
        .route("/api/v1/lsm",               get(super::monitoring::lsm_overview))
        .route("/metrics",                  get(super::monitoring::prometheus_metrics))
        // 대시보드 HTML
        .route("/",          get(super::dashboard::root_redirect))
        .route("/dashboard", get(super::dashboard::dashboard_handler))
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
                let resp = match crate::executor::select_exec::execute_select(&sql).await {
                    Ok(sel) => {
                        let row_count = sel.rows.len();
                        serde_json::json!({
                            "status":    "success",
                            "columns":   sel.columns,
                            "rows":      sel.rows,
                            "row_count": row_count,
                        })
                    },
                    Err(e) => serde_json::json!({
                        "status": "error",
                        "error":  e,
                    }),
                };
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

        let nodes = Arc::new(NodeRegistry::default());
        let state = WebUiState { cube_mgr, smv_mgr, raft, node_id: "qn-test".to_string(), nodes };
        let _router = build_router(state);
        // 라우터가 패닉 없이 생성되면 통과
    }
}
