// T090: Query Profiler — Circular Buffer 1,000건
// query_id, SQL text, 시작시각, 단계별 메트릭, 총 소요시간 기록

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

const MAX_ENTRIES: usize = 1000;

// ─── 단계별 메트릭 ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageMetric {
    /// 단계 이름 (Parse, Plan, Execute, Merge 등)
    pub stage:        String,
    /// 소요 시간 (마이크로초)
    pub elapsed_us:   u64,
    /// 처리 행 수
    pub rows:         u64,
    /// 노드 ID (CN 등)
    pub node_id:      Option<String>,
}

// ─── 쿼리 프로파일 항목 ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryProfile {
    /// 고유 쿼리 ID
    pub query_id:       String,
    /// SQL 원문 (최대 4096자)
    pub sql:            String,
    /// 쿼리 시작 시각 (Unix epoch ms)
    pub started_at_ms:  u64,
    /// 총 소요 시간 (마이크로초). 실행 중이면 None
    pub total_us:       Option<u64>,
    /// 단계별 메트릭
    pub stages:         Vec<StageMetric>,
    /// 총 스캔 행 수
    pub rows_scanned:   u64,
    /// 결과 행 수
    pub rows_returned:  u64,
    /// 오류 메시지 (실패 시)
    pub error:          Option<String>,
}

impl QueryProfile {
    fn new(query_id: &str, sql: &str) -> Self {
        let started_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        Self {
            query_id:      query_id.to_string(),
            sql:           sql.chars().take(4096).collect(),
            started_at_ms,
            total_us:      None,
            stages:        vec![],
            rows_scanned:  0,
            rows_returned: 0,
            error:         None,
        }
    }
}

// ─── 진행 중인 쿼리 추적 ────────────────────────────────────────────────────────

struct InFlight {
    profile:  QueryProfile,
    start:    Instant,
}

// ─── Query Profiler ────────────────────────────────────────────────────────────

pub struct QueryProfiler {
    inner: Mutex<ProfilerInner>,
}

struct ProfilerInner {
    /// 완료된 쿼리 이력 (Circular Buffer)
    history:   VecDeque<QueryProfile>,
    /// 진행 중인 쿼리 (query_id → InFlight)
    in_flight: std::collections::HashMap<String, InFlight>,
}

impl QueryProfiler {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(ProfilerInner {
                history:   VecDeque::with_capacity(MAX_ENTRIES),
                in_flight: std::collections::HashMap::new(),
            }),
        }
    }

    /// 쿼리 시작 — query_id 반환
    pub fn begin(&self, sql: &str) -> String {
        let query_id = Uuid::new_v4().to_string();
        let profile  = QueryProfile::new(&query_id, sql);
        let mut inner = self.inner.lock().unwrap();
        inner.in_flight.insert(query_id.clone(), InFlight {
            profile,
            start: Instant::now(),
        });
        query_id
    }

    /// 단계 완료 메트릭 기록
    pub fn record_stage(
        &self,
        query_id: &str,
        stage:    &str,
        elapsed:  Duration,
        rows:     u64,
        node_id:  Option<&str>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(in_flight) = inner.in_flight.get_mut(query_id) {
            in_flight.profile.stages.push(StageMetric {
                stage:      stage.to_string(),
                elapsed_us: elapsed.as_micros() as u64,
                rows,
                node_id:    node_id.map(str::to_string),
            });
            in_flight.profile.rows_scanned += rows;
        }
    }

    /// 쿼리 완료
    pub fn finish(
        &self,
        query_id:      &str,
        rows_returned: u64,
        error:         Option<&str>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(mut in_flight) = inner.in_flight.remove(query_id) {
            let total_us = in_flight.start.elapsed().as_micros() as u64;
            in_flight.profile.total_us      = Some(total_us);
            in_flight.profile.rows_returned = rows_returned;
            in_flight.profile.error         = error.map(str::to_string);

            // Circular Buffer: 초과 시 가장 오래된 항목 제거
            if inner.history.len() >= MAX_ENTRIES {
                inner.history.pop_front();
            }
            inner.history.push_back(in_flight.profile);
        }
    }

    /// 최근 N건 조회 (최신순)
    pub fn recent(&self, limit: usize) -> Vec<QueryProfile> {
        let inner = self.inner.lock().unwrap();
        inner.history
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    /// 전체 이력 건수
    pub fn count(&self) -> usize {
        self.inner.lock().unwrap().history.len()
    }

    /// 특정 query_id 조회
    pub fn get(&self, query_id: &str) -> Option<QueryProfile> {
        let inner = self.inner.lock().unwrap();
        inner.history.iter().find(|p| p.query_id == query_id).cloned()
    }

    /// 진행 중인 쿼리 목록
    pub fn in_flight(&self) -> Vec<QueryProfile> {
        let inner = self.inner.lock().unwrap();
        inner.in_flight.values().map(|f| f.profile.clone()).collect()
    }

    /// 느린 쿼리 조회 (지정 임계값 ms 이상)
    pub fn slow_queries(&self, threshold_ms: u64, limit: usize) -> Vec<QueryProfile> {
        let inner = self.inner.lock().unwrap();
        inner.history
            .iter()
            .rev()
            .filter(|p| p.total_us.unwrap_or(0) >= threshold_ms * 1000)
            .take(limit)
            .cloned()
            .collect()
    }
}

impl Default for QueryProfiler {
    fn default() -> Self { Self::new() }
}

// ─── 글로벌 싱글턴 ────────────────────────────────────────────────────────────

use std::sync::LazyLock;

pub static PROFILER: LazyLock<Arc<QueryProfiler>> =
    LazyLock::new(|| Arc::new(QueryProfiler::new()));

/// 쿼리 시작 — 편의 함수
pub fn profile_begin(sql: &str) -> String { PROFILER.begin(sql) }

/// 쿼리 완료 — 편의 함수
pub fn profile_finish(query_id: &str, rows_returned: u64, error: Option<&str>) {
    PROFILER.finish(query_id, rows_returned, error);
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_begin_and_finish() {
        let p    = QueryProfiler::new();
        let id   = p.begin("SELECT 1");
        p.finish(&id, 1, None);
        assert_eq!(p.count(), 1);
        let profile = p.get(&id).unwrap();
        assert_eq!(profile.sql, "SELECT 1");
        assert!(profile.total_us.unwrap() < 1_000_000); // < 1s
        assert_eq!(profile.rows_returned, 1);
        assert!(profile.error.is_none());
    }

    #[test]
    fn test_stage_metrics() {
        let p  = QueryProfiler::new();
        let id = p.begin("SELECT * FROM page_events");
        p.record_stage(&id, "Parse",   Duration::from_micros(100), 0, None);
        p.record_stage(&id, "Execute", Duration::from_micros(5000), 1000, Some("cn-1"));
        p.finish(&id, 1000, None);

        let profile = p.get(&id).unwrap();
        assert_eq!(profile.stages.len(), 2);
        assert_eq!(profile.stages[0].stage, "Parse");
        assert_eq!(profile.stages[1].rows, 1000);
        assert_eq!(profile.rows_scanned, 1000);
    }

    #[test]
    fn test_circular_buffer_overflow() {
        let p = QueryProfiler::new();
        // MAX_ENTRIES + 50 건 삽입
        for i in 0..(MAX_ENTRIES + 50) {
            let id = p.begin(&format!("SELECT {}", i));
            p.finish(&id, 0, None);
        }
        assert_eq!(p.count(), MAX_ENTRIES);
        // 가장 최근 항목 = "SELECT 1049"
        let recent = p.recent(1);
        assert!(recent[0].sql.contains(&format!("{}", MAX_ENTRIES + 49)));
    }

    #[test]
    fn test_in_flight() {
        let p  = QueryProfiler::new();
        let id = p.begin("SELECT sleep(1)");
        assert_eq!(p.in_flight().len(), 1);
        p.finish(&id, 0, None);
        assert_eq!(p.in_flight().len(), 0);
    }

    #[test]
    fn test_slow_queries() {
        let p = QueryProfiler::new();

        // 빠른 쿼리
        let id1 = p.begin("SELECT 1");
        thread::sleep(Duration::from_millis(1));
        p.finish(&id1, 1, None);

        // 느린 쿼리 (1ms 이상)
        let id2 = p.begin("SELECT * FROM huge_table");
        thread::sleep(Duration::from_millis(5));
        p.finish(&id2, 10000, None);

        let slow = p.slow_queries(3, 10);
        assert!(slow.iter().any(|q| q.query_id == id2));
    }

    #[test]
    fn test_error_recording() {
        let p  = QueryProfiler::new();
        let id = p.begin("SELECT * FROM nonexistent");
        p.finish(&id, 0, Some("Table not found"));

        let profile = p.get(&id).unwrap();
        assert_eq!(profile.error.as_deref(), Some("Table not found"));
    }

    #[test]
    fn test_recent_ordering() {
        let p = QueryProfiler::new();
        for i in 0..5 {
            let id = p.begin(&format!("SELECT {}", i));
            p.finish(&id, 0, None);
        }
        let recent = p.recent(3);
        assert_eq!(recent.len(), 3);
        // 최신순이어야 함
        assert!(recent[0].sql.contains("4"));
        assert!(recent[1].sql.contains("3"));
        assert!(recent[2].sql.contains("2"));
    }
}
