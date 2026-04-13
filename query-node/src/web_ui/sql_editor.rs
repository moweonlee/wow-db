// T085: Web SQL 에디터 — SQL 실행, 결과 스트리밍, 쿼리 히스토리 표시

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info};

use super::server::WebUiState;

// ─── 쿼리 히스토리 ───────────────────────────────────────────────────────────

/// 최근 쿼리 이력 엔트리
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryHistoryEntry {
    pub query_id:     String,
    pub sql:          String,
    pub status:       QueryStatus,
    pub started_at:   i64,   // Unix timestamp (ms)
    pub duration_ms:  u64,
    pub rows_returned: u64,
    pub error:        Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryStatus {
    Running,
    Success,
    Error,
    Cancelled,
}

/// 인메모리 Circular Buffer (최대 1,000건) — 단순 구현
pub struct QueryHistoryBuffer {
    entries:  Vec<QueryHistoryEntry>,
    capacity: usize,
}

impl QueryHistoryBuffer {
    pub fn new(capacity: usize) -> Self {
        Self { entries: Vec::new(), capacity }
    }

    pub fn push(&mut self, entry: QueryHistoryEntry) {
        if self.entries.len() >= self.capacity {
            self.entries.remove(0); // oldest 항목 제거
        }
        self.entries.push(entry);
    }

    pub fn recent(&self, n: usize) -> Vec<&QueryHistoryEntry> {
        let start = self.entries.len().saturating_sub(n);
        self.entries[start..].iter().rev().collect()
    }

    pub fn len(&self) -> usize { self.entries.len() }
}

/// 앱 전역 쿼리 히스토리 (WebUiState에 추가 예정 — 현재는 thread_local 스텁)
static QUERY_HISTORY: std::sync::LazyLock<Arc<RwLock<QueryHistoryBuffer>>> =
    std::sync::LazyLock::new(|| Arc::new(RwLock::new(QueryHistoryBuffer::new(1000))));

// ─── 요청/응답 타입 ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ExecuteSqlRequest {
    pub sql:      String,
    pub database: Option<String>,
    /// 최대 반환 행 수 (기본 1,000)
    pub limit:    Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct ExecuteSqlResponse {
    pub query_id:     String,
    pub status:       String,
    pub columns:      Vec<ColumnMeta>,
    pub rows:         Vec<Vec<serde_json::Value>>,
    pub row_count:    u64,
    pub duration_ms:  u64,
    pub error:        Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ColumnMeta {
    pub name:      String,
    pub data_type: String,
}

// ─── 핸들러 ──────────────────────────────────────────────────────────────────

/// POST /api/sql/execute — SQL 실행 (동기)
pub async fn execute_sql(
    State(_state): State<WebUiState>,
    Json(req):     Json<ExecuteSqlRequest>,
) -> impl IntoResponse {
    let query_id = uuid::Uuid::new_v4().to_string();
    let start    = Instant::now();

    info!(query_id = %query_id, sql = %req.sql, "Web SQL execute");

    let started_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    // TODO (Phase D): 실제 쿼리 실행 엔진 연동
    // 현재는 파서 검증 + 스텁 응답
    let (status, columns, rows, error) = execute_stub(&req.sql);

    let duration_ms = start.elapsed().as_millis() as u64;
    let row_count   = rows.len() as u64;

    // 히스토리 기록
    let entry = QueryHistoryEntry {
        query_id:     query_id.clone(),
        sql:          req.sql.clone(),
        status:       if error.is_none() { QueryStatus::Success } else { QueryStatus::Error },
        started_at,
        duration_ms,
        rows_returned: row_count,
        error:         error.clone(),
    };
    QUERY_HISTORY.write().await.push(entry);

    let http_status = if error.is_some() { StatusCode::OK } else { StatusCode::OK };
    (http_status, Json(ExecuteSqlResponse {
        query_id,
        status,
        columns,
        rows,
        row_count,
        duration_ms,
        error,
    })).into_response()
}

/// GET /api/sql/history — 최근 쿼리 이력
pub async fn query_history(State(_state): State<WebUiState>) -> impl IntoResponse {
    let history = QUERY_HISTORY.read().await;
    let recent: Vec<_> = history.recent(50).into_iter().cloned().collect();
    Json(serde_json::json!({
        "history": recent,
        "count":   recent.len(),
    }))
}

// ─── 스텁 실행 ────────────────────────────────────────────────────────────────

fn execute_stub(sql: &str) -> (String, Vec<ColumnMeta>, Vec<Vec<serde_json::Value>>, Option<String>) {
    let upper = sql.trim().to_uppercase();

    if upper.starts_with("SELECT") {
        // 간단한 스텁 결과
        let cols = vec![
            ColumnMeta { name: "_stub_col".to_string(), data_type: "String".to_string() },
        ];
        let rows = vec![
            vec![serde_json::json!("(stub result — query engine not yet connected)")],
        ];
        ("success".to_string(), cols, rows, None)
    } else if upper.starts_with("SHOW") {
        let cols = vec![ColumnMeta { name: "Value".to_string(), data_type: "String".to_string() }];
        let rows = vec![vec![serde_json::json!("(SHOW stub)")]];
        ("success".to_string(), cols, rows, None)
    } else if upper.starts_with("CREATE") || upper.starts_with("DROP") || upper.starts_with("ALTER") {
        ("success".to_string(), vec![], vec![], None)
    } else {
        ("error".to_string(), vec![], vec![], Some("Unsupported statement type (stub)".to_string()))
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_history_buffer() {
        let mut buf = QueryHistoryBuffer::new(3);
        for i in 0..5u64 {
            buf.push(QueryHistoryEntry {
                query_id:      format!("q{}", i),
                sql:           format!("SELECT {}", i),
                status:        QueryStatus::Success,
                started_at:    i as i64,
                duration_ms:   10,
                rows_returned: 1,
                error:         None,
            });
        }
        assert_eq!(buf.len(), 3, "최대 3건 유지");
        let recent = buf.recent(3);
        // 최근 순서 — q4, q3, q2
        assert_eq!(recent[0].query_id, "q4");
        assert_eq!(recent[1].query_id, "q3");
    }

    #[test]
    fn test_execute_stub_select() {
        let (status, cols, rows, err) = execute_stub("SELECT * FROM events");
        assert_eq!(status, "success");
        assert!(!cols.is_empty());
        assert!(!rows.is_empty());
        assert!(err.is_none());
    }

    #[test]
    fn test_execute_stub_create() {
        let (status, _, _, err) = execute_stub("CREATE CUBE test (...)");
        assert_eq!(status, "success");
        assert!(err.is_none());
    }

    #[test]
    fn test_execute_stub_unknown() {
        let (status, _, _, err) = execute_stub("UNKNOWN STATEMENT");
        assert_eq!(status, "error");
        assert!(err.is_some());
    }
}
