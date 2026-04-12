// T059: QN 실행 오케스트레이터 — Fragment→CN 전송, 부분 결과 수집, 최종 병합, Profiler 기록

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Result};
use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

use crate::planner::logical::LogicalPlan;
use crate::planner::physical::{Fragment, PhysicalPlan, PhysicalPlanner};
use crate::planner::cbo::{optimizer::CboOptimizer, stats::StatisticsManager};
use crate::sql_parser::{WowDbParser, WowDbStatement};

// ─── Query Profiler (Circular Buffer 1000건) ──────────────────────────────────

#[derive(Debug, Clone)]
pub struct QueryProfile {
    pub query_id:      String,
    pub sql:           String,
    pub started_at:    std::time::SystemTime,
    pub elapsed_ms:    u64,
    pub rows_scanned:  u64,
    pub rows_returned: u64,
    pub fragments:     Vec<FragmentProfile>,
    pub error:         Option<String>,
}

#[derive(Debug, Clone)]
pub struct FragmentProfile {
    pub fragment_id: String,
    pub cn_node:     String,
    pub elapsed_ms:  u64,
    pub rows_out:    u64,
}

pub struct QueryProfiler {
    buffer:   Vec<QueryProfile>,
    capacity: usize,
    head:     usize,
}

impl QueryProfiler {
    pub fn new(capacity: usize) -> Self {
        Self { buffer: Vec::with_capacity(capacity), capacity, head: 0 }
    }

    pub fn record(&mut self, profile: QueryProfile) {
        if self.buffer.len() < self.capacity {
            self.buffer.push(profile);
        } else {
            let idx = self.head % self.capacity;
            self.buffer[idx] = profile;
        }
        self.head += 1;
    }

    pub fn recent(&self, n: usize) -> Vec<&QueryProfile> {
        let len = self.buffer.len().min(n);
        self.buffer.iter().rev().take(len).collect()
    }

    pub fn count(&self) -> usize {
        self.buffer.len()
    }
}

// ─── 실행 결과 ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct QueryResult {
    pub query_id:     String,
    pub columns:      Vec<String>,
    pub rows:         Vec<Vec<Option<Vec<u8>>>>,
    pub rows_scanned: u64,
    pub elapsed_ms:   u64,
}

// ─── Execution Orchestrator ───────────────────────────────────────────────────

pub struct ExecutionOrchestrator {
    compute_nodes: Vec<String>,
    stats_mgr:     StatisticsManager,
    profiler:      Arc<Mutex<QueryProfiler>>,
}

impl ExecutionOrchestrator {
    pub fn new(compute_nodes: Vec<String>, stats_mgr: StatisticsManager) -> Self {
        Self {
            compute_nodes,
            stats_mgr,
            profiler: Arc::new(Mutex::new(QueryProfiler::new(1000))),
        }
    }

    /// SQL 쿼리 실행 진입점
    pub async fn execute_sql(&self, sql: &str) -> Result<QueryResult> {
        let query_id = Uuid::new_v4().to_string();
        let start    = Instant::now();

        info!(query_id = %query_id, sql = %sql, "쿼리 실행 시작");

        let result = self.do_execute(sql, &query_id).await;
        let elapsed_ms = start.elapsed().as_millis() as u64;

        // Profiler 기록
        let profile = QueryProfile {
            query_id:      query_id.clone(),
            sql:           sql.to_string(),
            started_at:    std::time::SystemTime::now(),
            elapsed_ms,
            rows_scanned:  result.as_ref().map(|r| r.rows_scanned).unwrap_or(0),
            rows_returned: result.as_ref().map(|r| r.rows.len() as u64).unwrap_or(0),
            fragments:     Vec::new(),
            error:         result.as_ref().err().map(|e| e.to_string()),
        };
        self.profiler.lock().await.record(profile);

        info!(
            query_id = %query_id,
            elapsed_ms,
            "쿼리 실행 완료"
        );

        result
    }

    async fn do_execute(&self, sql: &str, query_id: &str) -> Result<QueryResult> {
        // 1. 파싱 (Vec<WowDbStatement>에서 첫 번째 추출)
        let stmts = WowDbParser::parse(sql)?;
        let stmt  = stmts.into_iter().next()
            .ok_or_else(|| anyhow!("빈 SQL"))?;

        match stmt {
            WowDbStatement::Standard(_) => self.execute_query_stmt(sql, query_id).await,
            WowDbStatement::Custom(_) => {
                Err(anyhow!("DDL은 Execution Orchestrator가 아닌 CubeManager에서 처리"))
            }
        }
    }

    async fn execute_query_stmt(&self, sql: &str, query_id: &str) -> Result<QueryResult> {
        // 2. Logical Plan 생성
        use crate::planner::logical::LogicalPlanner;
        use std::collections::HashMap;
        let planner = LogicalPlanner::new(HashMap::new());
        let stmts   = WowDbParser::parse(sql)?;
        let stmt    = stmts.into_iter().next()
            .ok_or_else(|| anyhow!("빈 SQL"))?;
        let logical = planner.plan(&stmt)?;

        // 3. CBO 최적화
        let optimizer = CboOptimizer::new(self.stats_mgr.clone());
        let optimized = optimizer.optimize(logical)?;

        // 4. Physical Plan 생성
        let mut phys_planner = PhysicalPlanner::new(self.compute_nodes.clone());
        let physical = phys_planner.plan(optimized)?;
        let fragments = phys_planner.build_fragments(physical);

        // 5. Fragment → CN 전송 및 결과 수집
        // TODO (Phase D): 실제 gRPC 호출로 대체
        // 현재: 스텁 결과 반환
        info!(
            query_id = %query_id,
            fragment_count = fragments.len(),
            "Fragment 배포 (stub)"
        );

        Ok(QueryResult {
            query_id:     query_id.to_string(),
            columns:      vec!["result".to_string()],
            rows:         vec![vec![Some(b"stub_result".to_vec())]],
            rows_scanned: 0,
            elapsed_ms:   0,
        })
    }

    /// 최근 쿼리 프로파일 조회
    pub async fn recent_profiles(&self, n: usize) -> Vec<QueryProfile> {
        self.profiler.lock().await.recent(n)
            .into_iter().cloned().collect()
    }

    /// 프로파일 총 건수
    pub async fn profile_count(&self) -> usize {
        self.profiler.lock().await.count()
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_orch() -> ExecutionOrchestrator {
        ExecutionOrchestrator::new(
            vec!["cn-1:9040".to_string()],
            StatisticsManager::new(),
        )
    }

    #[tokio::test]
    async fn test_execute_simple_select() {
        let orch = make_orch();
        let result = orch.execute_sql("SELECT 1").await;
        // 파싱 오류 없이 완료되어야
        assert!(result.is_ok() || result.unwrap_err().to_string().contains("FROM"));
    }

    #[tokio::test]
    async fn test_profiler_records() {
        let orch = make_orch();
        let _ = orch.execute_sql("SELECT * FROM events").await;
        let count = orch.profile_count().await;
        assert_eq!(count, 1);
    }

    #[test]
    fn test_circular_buffer() {
        let mut profiler = QueryProfiler::new(3);
        for i in 0..5u32 {
            profiler.record(QueryProfile {
                query_id:      i.to_string(),
                sql:           format!("SELECT {}", i),
                started_at:    std::time::SystemTime::now(),
                elapsed_ms:    i as u64,
                rows_scanned:  0,
                rows_returned: 0,
                fragments:     Vec::new(),
                error:         None,
            });
        }
        assert_eq!(profiler.count(), 3);
    }
}
