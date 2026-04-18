// T076: Pre-aggregation MV INSERT 시점/주기 갱신 실행기

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

// ─── MV 갱신 요청 ─────────────────────────────────────────────────────────────

/// QN에서 CN으로 전달되는 MV 갱신 요청
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MvRefreshRequest {
    pub mv_name:     String,
    pub source_cube: String,
    pub group_by:    Vec<String>,
    pub agg_cols:    Vec<MvAggSpec>,
    pub mode:        MvRefreshMode,
    /// INSERT 배치 데이터 (Incremental 모드에서 사용)
    pub new_rows:    Vec<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MvAggSpec {
    pub alias:   String,
    pub func:    MvAggFunc,
    pub src_col: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MvAggFunc { Count, Sum, Avg, Min, Max, CountDistinct }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MvRefreshMode {
    /// INSERT 시점 증분 갱신
    Incremental,
    /// 전체 재계산
    Full,
}

// ─── 인메모리 MV 집계 상태 ────────────────────────────────────────────────────

/// GROUP BY 키 → 집계 버킷
#[derive(Debug, Clone, Default)]
pub struct AggBucket {
    /// alias → 현재 집계 값
    pub values: HashMap<String, AggState>,
}

#[derive(Debug, Clone)]
pub enum AggState {
    Count(i64),
    Sum(f64),
    Min(f64),
    Max(f64),
    Avg { sum: f64, count: i64 },
    CountDistinct(std::collections::HashSet<String>),
}

impl AggState {
    fn initial(func: &MvAggFunc) -> Self {
        match func {
            MvAggFunc::Count        => AggState::Count(0),
            MvAggFunc::Sum          => AggState::Sum(0.0),
            MvAggFunc::Min          => AggState::Min(f64::MAX),
            MvAggFunc::Max          => AggState::Max(f64::MIN),
            MvAggFunc::Avg          => AggState::Avg { sum: 0.0, count: 0 },
            MvAggFunc::CountDistinct => AggState::CountDistinct(std::collections::HashSet::new()),
        }
    }

    fn update(&mut self, val: &serde_json::Value) {
        let num = val.as_f64().unwrap_or(0.0);
        let str_val = val.as_str().map(String::from).unwrap_or_else(|| val.to_string());
        match self {
            AggState::Count(c)  => *c += 1,
            AggState::Sum(s)    => *s += num,
            AggState::Min(m)    => if num < *m { *m = num; },
            AggState::Max(m)    => if num > *m { *m = num; },
            AggState::Avg { sum, count } => { *sum += num; *count += 1; }
            AggState::CountDistinct(set) => { set.insert(str_val); }
        }
    }

    pub fn to_value(&self) -> serde_json::Value {
        match self {
            AggState::Count(c)   => serde_json::json!(c),
            AggState::Sum(s)     => serde_json::json!(s),
            AggState::Min(m)     => serde_json::json!(if *m == f64::MAX { 0.0 } else { *m }),
            AggState::Max(m)     => serde_json::json!(if *m == f64::MIN { 0.0 } else { *m }),
            AggState::Avg { sum, count } => {
                serde_json::json!(if *count == 0 { 0.0 } else { sum / *count as f64 })
            }
            AggState::CountDistinct(set) => serde_json::json!(set.len()),
        }
    }
}

// ─── MV 갱신 실행기 ───────────────────────────────────────────────────────────

/// 인메모리 집계 상태를 유지하며 증분 갱신을 수행하는 실행기
pub struct MvRefreshExecutor {
    /// mv_name → (group_key → AggBucket)
    state: Arc<Mutex<HashMap<String, HashMap<String, AggBucket>>>>,
}

impl MvRefreshExecutor {
    pub fn new() -> Self {
        Self { state: Arc::new(Mutex::new(HashMap::new())) }
    }

    /// MV 증분 갱신 — 새 행들을 집계 상태에 반영
    pub async fn apply_incremental(&self, req: &MvRefreshRequest) -> Result<MvRefreshResult> {
        if req.mode != MvRefreshMode::Incremental {
            return Err(anyhow!("apply_incremental called with non-incremental mode"));
        }

        let mut state = self.state.lock().await;
        let mv_state = state.entry(req.mv_name.clone()).or_default();

        let mut rows_processed = 0u64;

        for row in &req.new_rows {
            // GROUP BY 키 조합 생성
            let group_key = req.group_by.iter()
                .map(|col| {
                    row.get(col)
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect::<Vec<_>>()
                .join("|");

            let bucket = mv_state.entry(group_key).or_default();

            // 각 집계 컬럼 갱신
            for agg in &req.agg_cols {
                let state_entry = bucket.values
                    .entry(agg.alias.clone())
                    .or_insert_with(|| AggState::initial(&agg.func));

                let val = row.get(&agg.src_col).cloned().unwrap_or(serde_json::Value::Null);
                state_entry.update(&val);
            }

            rows_processed += 1;
        }

        debug!(
            mv = %req.mv_name,
            rows = rows_processed,
            "MV incremental refresh applied"
        );

        Ok(MvRefreshResult {
            mv_name:        req.mv_name.clone(),
            rows_processed,
            groups_updated: mv_state.len() as u64,
        })
    }

    /// MV 전체 재계산 — 기존 상태 초기화 후 재계산
    pub async fn apply_full(&self, req: &MvRefreshRequest) -> Result<MvRefreshResult> {
        let mut state = self.state.lock().await;
        // 기존 상태 초기화
        state.remove(&req.mv_name);

        let mv_state = state.entry(req.mv_name.clone()).or_default();
        let mut rows_processed = 0u64;

        for row in &req.new_rows {
            let group_key = req.group_by.iter()
                .map(|col| {
                    row.get(col)
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect::<Vec<_>>()
                .join("|");

            let bucket = mv_state.entry(group_key).or_default();

            for agg in &req.agg_cols {
                let state_entry = bucket.values
                    .entry(agg.alias.clone())
                    .or_insert_with(|| AggState::initial(&agg.func));
                let val = row.get(&agg.src_col).cloned().unwrap_or(serde_json::Value::Null);
                state_entry.update(&val);
            }

            rows_processed += 1;
        }

        info!(
            mv = %req.mv_name,
            rows = rows_processed,
            "MV full refresh complete"
        );

        Ok(MvRefreshResult {
            mv_name:        req.mv_name.clone(),
            rows_processed,
            groups_updated: mv_state.len() as u64,
        })
    }

    /// 현재 집계 결과를 행 목록으로 변환 (쿼리 응답용)
    pub async fn read_mv(&self, mv_name: &str, group_by: &[String], agg_cols: &[MvAggSpec])
        -> Vec<HashMap<String, serde_json::Value>>
    {
        let state = self.state.lock().await;
        let Some(mv_state) = state.get(mv_name) else { return Vec::new() };

        mv_state.iter().map(|(group_key, bucket)| {
            let mut row = HashMap::new();

            // GROUP BY 컬럼 복원
            let parts: Vec<&str> = group_key.split('|').collect();
            for (i, col) in group_by.iter().enumerate() {
                let val = parts.get(i).copied().unwrap_or("NULL");
                row.insert(col.clone(), serde_json::Value::String(val.to_string()));
            }

            // 집계 결과
            for agg in agg_cols {
                let val = bucket.values.get(&agg.alias)
                    .map(|s| s.to_value())
                    .unwrap_or(serde_json::Value::Null);
                row.insert(agg.alias.clone(), val);
            }

            row
        }).collect()
    }

    /// MV 전체 상태 삭제 (DROP MV)
    pub async fn drop_mv(&self, mv_name: &str) {
        self.state.lock().await.remove(mv_name);
        info!(mv = %mv_name, "MV state dropped");
    }

    pub async fn mv_count(&self) -> usize {
        self.state.lock().await.len()
    }
}

impl Default for MvRefreshExecutor {
    fn default() -> Self { Self::new() }
}

// ─── 갱신 결과 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MvRefreshResult {
    pub mv_name:        String,
    pub rows_processed: u64,
    pub groups_updated: u64,
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_rows(data: &[(&str, i64, f64)]) -> Vec<HashMap<String, serde_json::Value>> {
        data.iter()
            .map(|(name, count, revenue)| {
                let mut row = HashMap::new();
                row.insert("event_name".to_string(), serde_json::json!(name));
                row.insert("cnt_src".to_string(), serde_json::json!(count));
                row.insert("revenue".to_string(), serde_json::json!(revenue));
                row
            })
            .collect()
    }

    fn make_req(rows: Vec<HashMap<String, serde_json::Value>>) -> MvRefreshRequest {
        MvRefreshRequest {
            mv_name:     "daily_summary".to_string(),
            source_cube: "events".to_string(),
            group_by:    vec!["event_name".to_string()],
            agg_cols:    vec![
                MvAggSpec { alias: "cnt".to_string(),     func: MvAggFunc::Count, src_col: "cnt_src".to_string() },
                MvAggSpec { alias: "total_rev".to_string(), func: MvAggFunc::Sum, src_col: "revenue".to_string() },
            ],
            mode:     MvRefreshMode::Incremental,
            new_rows: rows,
        }
    }

    #[tokio::test]
    async fn test_incremental_basic() {
        let exec = MvRefreshExecutor::new();
        let rows = make_rows(&[
            ("click", 1, 0.0),
            ("click", 1, 10.0),
            ("view",  1, 5.0),
        ]);
        let result = exec.apply_incremental(&make_req(rows)).await.unwrap();
        assert_eq!(result.rows_processed, 3);
        assert_eq!(result.groups_updated, 2); // click, view
    }

    #[tokio::test]
    async fn test_read_mv_results() {
        let exec = MvRefreshExecutor::new();
        let rows = make_rows(&[
            ("click", 1, 10.0),
            ("click", 1, 20.0),
        ]);
        exec.apply_incremental(&make_req(rows)).await.unwrap();

        let agg_cols = vec![
            MvAggSpec { alias: "cnt".to_string(),      func: MvAggFunc::Count, src_col: "cnt_src".to_string() },
            MvAggSpec { alias: "total_rev".to_string(), func: MvAggFunc::Sum,   src_col: "revenue".to_string() },
        ];
        let result = exec.read_mv("daily_summary", &["event_name".to_string()], &agg_cols).await;
        assert_eq!(result.len(), 1);
        let row = &result[0];
        assert_eq!(row["event_name"], serde_json::json!("\"click\""));
        assert_eq!(row["cnt"], serde_json::json!(2i64));
        assert!((row["total_rev"].as_f64().unwrap() - 30.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_full_refresh_resets_state() {
        let exec = MvRefreshExecutor::new();

        // 초기 증분 삽입
        let rows1 = make_rows(&[("click", 1, 100.0)]);
        exec.apply_incremental(&make_req(rows1)).await.unwrap();

        // Full refresh — 새 데이터로 초기화
        let mut req2 = make_req(make_rows(&[("view", 1, 5.0)]));
        req2.mode = MvRefreshMode::Full;
        exec.apply_full(&req2).await.unwrap();

        let agg_cols = vec![
            MvAggSpec { alias: "cnt".to_string(), func: MvAggFunc::Count, src_col: "cnt_src".to_string() },
        ];
        let result = exec.read_mv("daily_summary", &["event_name".to_string()], &agg_cols).await;
        // click는 사라지고 view만 남아야 함
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["event_name"], serde_json::json!("\"view\""));
    }

    #[tokio::test]
    async fn test_drop_mv() {
        let exec = MvRefreshExecutor::new();
        let rows = make_rows(&[("click", 1, 1.0)]);
        exec.apply_incremental(&make_req(rows)).await.unwrap();
        assert_eq!(exec.mv_count().await, 1);

        exec.drop_mv("daily_summary").await;
        assert_eq!(exec.mv_count().await, 0);
    }

    #[tokio::test]
    async fn test_min_max_agg() {
        let exec = MvRefreshExecutor::new();
        let mut rows = Vec::new();
        for &rev in &[10.0f64, 50.0, 5.0, 30.0] {
            let mut row = HashMap::new();
            row.insert("event_name".to_string(), serde_json::json!("purchase"));
            row.insert("revenue".to_string(), serde_json::json!(rev));
            rows.push(row);
        }

        let req = MvRefreshRequest {
            mv_name:     "price_stats".to_string(),
            source_cube: "events".to_string(),
            group_by:    vec!["event_name".to_string()],
            agg_cols:    vec![
                MvAggSpec { alias: "min_rev".to_string(), func: MvAggFunc::Min, src_col: "revenue".to_string() },
                MvAggSpec { alias: "max_rev".to_string(), func: MvAggFunc::Max, src_col: "revenue".to_string() },
            ],
            mode:     MvRefreshMode::Incremental,
            new_rows: rows,
        };

        exec.apply_incremental(&req).await.unwrap();

        let agg_cols = vec![
            MvAggSpec { alias: "min_rev".to_string(), func: MvAggFunc::Min, src_col: "revenue".to_string() },
            MvAggSpec { alias: "max_rev".to_string(), func: MvAggFunc::Max, src_col: "revenue".to_string() },
        ];
        let result = exec.read_mv("price_stats", &["event_name".to_string()], &agg_cols).await;
        assert_eq!(result.len(), 1);
        assert!((result[0]["min_rev"].as_f64().unwrap() - 5.0).abs() < 0.001);
        assert!((result[0]["max_rev"].as_f64().unwrap() - 50.0).abs() < 0.001);
    }
}
