// T092: Web UI 모니터링 대시보드 API
// 전체 노드 상태, 역할, 리소스 사용률 JSON API

use std::time::{SystemTime, UNIX_EPOCH};

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};

use crate::monitoring::METRICS;
use crate::profiler::PROFILER;

use super::server::WebUiState;

// ─── 노드 등록 요청 타입 ──────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub node_id:   String,
    pub node_type: String,  // "query" | "compute" | "storage"
    pub address:   String,  // grpc 또는 접속 주소
    pub role:      Option<String>,
    pub http_addr: Option<String>,
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

/// POST /api/v1/nodes/register — SN/CN이 기동 시 호출하여 레지스트리에 등록
pub async fn register_node(
    State(state): State<WebUiState>,
    Json(body): Json<RegisterRequest>,
) -> impl IntoResponse {
    let role = body.role.unwrap_or_else(|| match body.node_type.as_str() {
        "storage" => "Storage".to_string(),
        "compute" => "Worker".to_string(),
        _         => body.node_type.clone(),
    });
    let node = NodeStatus {
        id:           body.node_id.clone(),
        address:      body.address,
        role,
        alive:        true,
        cpu_pct:      0.0,
        memory_mb:    0.0,
        last_seen_ms: now_ms(),
    };
    let key = format!("{}:{}", body.node_type, body.node_id);
    state.nodes.write().await.insert(key, node);
    tracing::info!(node_id = %body.node_id, node_type = %body.node_type, "Node registered");
    Json(serde_json::json!({"ok": true}))
}

// ─── 응답 타입 ────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterOverview {
    pub timestamp_ms:       u64,
    pub query_nodes:        Vec<NodeStatus>,
    pub compute_nodes:      Vec<NodeStatus>,
    pub data_nodes:         Vec<NodeStatus>,
    pub raft_leader_id:     Option<String>,
    pub raft_term:          u64,
    pub total_tablets:      i64,
    pub total_sstables:     i64,
    pub queries_in_flight:  i64,
    pub queries_total:      u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeStatus {
    pub id:          String,
    pub address:     String,
    pub role:        String,
    pub alive:       bool,
    pub cpu_pct:     f64,
    pub memory_mb:   f64,
    pub last_seen_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProfilerSummary {
    pub total_queries:    usize,
    pub recent_queries:   Vec<QuerySummary>,
    pub slow_queries:     Vec<QuerySummary>,
    pub in_flight:        Vec<QuerySummary>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QuerySummary {
    pub query_id:      String,
    pub sql_preview:   String,
    pub started_at_ms: u64,
    pub total_ms:      Option<f64>,
    pub rows_scanned:  u64,
    pub rows_returned: u64,
    pub stages:        usize,
    pub error:         Option<String>,
}

fn to_summary(p: &crate::profiler::QueryProfile) -> QuerySummary {
    QuerySummary {
        query_id:      p.query_id.clone(),
        sql_preview:   p.sql.chars().take(120).collect(),
        started_at_ms: p.started_at_ms,
        total_ms:      p.total_us.map(|us| us as f64 / 1000.0),
        rows_scanned:  p.rows_scanned,
        rows_returned: p.rows_returned,
        stages:        p.stages.len(),
        error:         p.error.clone(),
    }
}

// ─── 핸들러 ───────────────────────────────────────────────────────────────────

/// GET /api/v1/cluster — 클러스터 전체 상태 개요
pub async fn cluster_overview(
    State(state): State<WebUiState>,
) -> impl IntoResponse {
    let ts = now_ms();
    const TTL_MS: u64 = 15_000; // 15초 이상 heartbeat 없으면 offline (heartbeat=5s × 3)

    // QN 자신의 상태
    let self_cpu    = METRICS.cpu_usage_pct.get();
    let self_mem_mb = METRICS.memory_used_bytes.get() / (1024.0 * 1024.0);
    let node_id_str = state.node_id.clone();

    let mysql_port  = std::env::var("MYSQL_PORT").unwrap_or_else(|_| "9030".to_string());
    let self_qn = NodeStatus {
        id:           node_id_str.clone(),
        address:      format!("127.0.0.1:{}", mysql_port),
        role:         "Leader".to_string(),
        alive:        true,
        cpu_pct:      self_cpu,
        memory_mb:    self_mem_mb,
        last_seen_ms: ts,
    };

    // 레지스트리에서 읽기
    let reg = state.nodes.read().await;
    let mut query_nodes  = vec![self_qn];
    let mut compute_nodes: Vec<NodeStatus> = Vec::new();
    let mut data_nodes:    Vec<NodeStatus> = Vec::new();

    for (key, node) in reg.iter() {
        let mut n = node.clone();
        // TTL 초과 → offline
        if ts.saturating_sub(n.last_seen_ms) > TTL_MS {
            n.alive = false;
        }
        if key.starts_with("query:")   { query_nodes.push(n); }
        else if key.starts_with("compute:") { compute_nodes.push(n); }
        else if key.starts_with("storage:") { data_nodes.push(n); }
    }
    drop(reg);

    // 레지스트리가 비어 있을 때 스텁 유지 (하위 호환)
    if compute_nodes.is_empty() {
        compute_nodes = build_cn_status(ts);
    }
    if data_nodes.is_empty() {
        data_nodes = build_dn_status(ts);
    }

    let overview = ClusterOverview {
        timestamp_ms:      ts,
        query_nodes,
        compute_nodes,
        data_nodes,
        raft_leader_id:    Some(node_id_str),
        raft_term:         METRICS.raft_term.get() as u64,
        total_tablets:     METRICS.tablets_total.get(),
        total_sstables:    METRICS.sstables_total.get(),
        queries_in_flight: METRICS.queries_in_flight.get(),
        queries_total:     METRICS.queries_total.get(),
    };

    Json(overview)
}

/// GET /api/v1/profiler — Query Profiler 요약
pub async fn profiler_summary() -> impl IntoResponse {
    let recent = PROFILER.recent(20);
    let slow   = PROFILER.slow_queries(1000, 10);  // 1초 이상
    let inflt  = PROFILER.in_flight();

    let summary = ProfilerSummary {
        total_queries:  PROFILER.count(),
        recent_queries: recent.iter().map(to_summary).collect(),
        slow_queries:   slow.iter().map(to_summary).collect(),
        in_flight:      inflt.iter().map(to_summary).collect(),
    };

    Json(summary)
}

/// GET /metrics — Prometheus 텍스트 포맷
pub async fn prometheus_metrics() -> impl IntoResponse {
    let text = crate::monitoring::render_metrics();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        text,
    )
}

// ─── 스텁 노드 상태 생성 ──────────────────────────────────────────────────────

fn build_cn_status(now_ms: u64) -> Vec<NodeStatus> {
    // Phase D에서 실제 CN 레지스트리에서 읽어옴
    let count = METRICS.compute_nodes_online.get().max(0) as usize;
    (0..count.max(1))
        .map(|i| NodeStatus {
            id:           format!("cn-{}", i + 1),
            address:      format!("compute-node-{}:9040", i + 1),
            role:         "Worker".to_string(),
            alive:        true,
            cpu_pct:      0.0,
            memory_mb:    0.0,
            last_seen_ms: now_ms,
        })
        .collect()
}

/// GET /api/v1/lsm — 모든 SN의 LSM Compaction 상태 집계
pub async fn lsm_overview() -> impl IntoResponse {
    // STORAGE_HTTP_NODES 우선, 없으면 STORAGE_NODES 에서 gRPC 포트 → HTTP 포트 변환 시도
    let sn_list: Vec<String> = {
        let http_nodes = std::env::var("STORAGE_HTTP_NODES").unwrap_or_default();
        if !http_nodes.trim().is_empty() {
            http_nodes.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
        } else {
            // STORAGE_NODES 의 포트 9060 → 8040 으로 대체 (dev 기본값)
            std::env::var("STORAGE_NODES").unwrap_or_default()
                .split(',')
                .map(|s| s.trim().replace(":9060", ":8040").replace(":9061", ":8041").replace(":9062", ":8042"))
                .filter(|s| !s.is_empty())
                .collect()
        }
    };

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let mut nodes: Vec<serde_json::Value> = Vec::new();
    let mut total_l0 = 0u64;
    let mut slowdown = 0u32;
    let mut stop_count = 0u32;

    for sn_addr in &sn_list {
        let url = format!("http://{}/api/v1/lsm-status", sn_addr);
        match reqwest::get(&url).await {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<serde_json::Value>().await {
                    Ok(data) => {
                        let l0 = data.get("total_l0_files").and_then(|v| v.as_u64()).unwrap_or(0);
                        total_l0 += l0;
                        let wc = data.get("write_control").and_then(|v| v.as_str()).unwrap_or("Normal");
                        if wc == "Slowdown" { slowdown += 1; }
                        if wc == "Stop"     { stop_count += 1; }
                        nodes.push(data);
                    }
                    Err(e) => {
                        nodes.push(serde_json::json!({
                            "node_id": sn_addr, "sn_endpoint": sn_addr,
                            "partitions": [], "total_l0_files": 0,
                            "compaction_running": false, "write_control": "Unknown",
                            "error": e.to_string(), "updated_at_ms": now_ms,
                        }));
                    }
                }
            }
            _ => {
                nodes.push(serde_json::json!({
                    "node_id": sn_addr, "sn_endpoint": sn_addr,
                    "partitions": [], "total_l0_files": 0,
                    "compaction_running": false, "write_control": "Unknown",
                    "error": "unreachable", "updated_at_ms": now_ms,
                }));
            }
        }
    }

    Json(serde_json::json!({
        "nodes": nodes,
        "total_l0_files": total_l0,
        "nodes_with_slowdown": slowdown,
        "nodes_with_stop": stop_count,
        "fetched_at_ms": now_ms,
    }))
}

fn build_dn_status(now_ms: u64) -> Vec<NodeStatus> {
    let count = METRICS.data_nodes_online.get().max(0) as usize;
    (0..count.max(1))
        .map(|i| NodeStatus {
            id:           format!("dn-{}", i + 1),
            address:      format!("storage-node-{}:9060", i + 1),
            role:         "Storage".to_string(),
            alive:        true,
            cpu_pct:      0.0,
            memory_mb:    0.0,
            last_seen_ms: now_ms,
        })
        .collect()
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiler::QueryProfiler;
    use std::time::Duration;

    #[test]
    fn test_to_summary_basic() {
        let p   = QueryProfiler::new();
        let id  = p.begin("SELECT * FROM events");
        p.record_stage(&id, "Parse", Duration::from_micros(100), 0, None);
        p.finish(&id, 500, None);

        let profile = p.get(&id).unwrap();
        let summary = to_summary(&profile);

        assert!(!summary.query_id.is_empty());
        assert!(summary.sql_preview.contains("SELECT"));
        assert!(summary.total_ms.is_some());
        assert_eq!(summary.stages, 1);
        assert_eq!(summary.rows_returned, 500);
        assert!(summary.error.is_none());
    }

    #[test]
    fn test_to_summary_sql_truncation() {
        let long_sql = "SELECT ".to_string() + &"col, ".repeat(100);
        let p  = QueryProfiler::new();
        let id = p.begin(&long_sql);
        p.finish(&id, 0, None);

        let profile = p.get(&id).unwrap();
        let summary = to_summary(&profile);
        assert!(summary.sql_preview.len() <= 120);
    }
}
