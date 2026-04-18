// T092: Web UI 모니터링 대시보드 API
// 전체 노드 상태, 역할, 리소스 사용률 JSON API

use std::time::{SystemTime, UNIX_EPOCH};

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};

use crate::monitoring::METRICS;
use crate::profiler::PROFILER;

use super::server::WebUiState;

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
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // QN 자신의 상태 (단일 노드 스텁)
    let self_cpu    = METRICS.cpu_usage_pct.get();
    let self_mem_mb = METRICS.memory_used_bytes.get() / (1024.0 * 1024.0);

    let node_id_str = state.raft.node_id.0.to_string();
    let qn_node = NodeStatus {
        id:           node_id_str.clone(),
        address:      "localhost:9030".to_string(),
        role:         "Leader".to_string(), // Phase B에서 실제 Raft 역할로 교체
        alive:        true,
        cpu_pct:      self_cpu,
        memory_mb:    self_mem_mb,
        last_seen_ms: now_ms,
    };

    let overview = ClusterOverview {
        timestamp_ms:      now_ms,
        query_nodes:       vec![qn_node],
        compute_nodes:     build_cn_status(now_ms),
        data_nodes:        build_dn_status(now_ms),
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
