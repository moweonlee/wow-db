// T056: 컬럼 통계 수집/저장 — row_count, min/max, NDV HyperLogLog, null_count

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

// ─── 컬럼 통계 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnStats {
    pub row_count:  u64,
    pub null_count: u64,
    /// 직렬화된 최솟값 (bytes, 사전순 비교)
    pub min_val:    Option<Vec<u8>>,
    /// 직렬화된 최댓값
    pub max_val:    Option<Vec<u8>>,
    /// HyperLogLog 추정 NDV (유니크 값 수)
    pub ndv:        u64,
    /// 히스토그램 버킷 (버킷 상한값, 버킷 행 수) 쌍
    pub histogram:  Vec<(Vec<u8>, u64)>,
}

impl ColumnStats {
    /// Selectivity 추정: col = val
    pub fn selectivity_eq(&self, val: &[u8]) -> f64 {
        if self.row_count == 0 || self.ndv == 0 {
            return 1.0;
        }
        // 1/NDV 추정
        let base = 1.0 / self.ndv as f64;
        // NULL 조정
        let null_ratio = self.null_count as f64 / self.row_count as f64;
        base * (1.0 - null_ratio)
    }

    /// Selectivity 추정: col BETWEEN lo AND hi
    pub fn selectivity_range(&self, lo: &[u8], hi: &[u8]) -> f64 {
        if self.row_count == 0 {
            return 1.0;
        }
        // Min/max 기반 구간 비율 추정
        let (min, max) = match (&self.min_val, &self.max_val) {
            (Some(min), Some(max)) if min != max => (min.as_slice(), max.as_slice()),
            _ => return 1.0 / self.ndv.max(1) as f64,
        };

        // 간단한 선형 추정 (수치형 컬럼에 적합)
        // TODO: 히스토그램 기반 정밀 추정
        let range_min = lo.max(min);
        let range_max = hi.min(max);
        if range_min > range_max {
            return 0.0;
        }
        1.0 / self.ndv.max(1) as f64 * self.ndv as f64 / 2.0
    }
}

// ─── 테이블 통계 ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableStats {
    pub cube_name:    String,
    pub partition:    String,
    pub row_count:    u64,
    pub column_stats: HashMap<String, ColumnStats>,
}

impl TableStats {
    pub fn get_column(&self, name: &str) -> Option<&ColumnStats> {
        self.column_stats.get(name)
    }
}

// ─── Statistics Manager ───────────────────────────────────────────────────────

#[derive(Clone)]
pub struct StatisticsManager {
    /// cube_name → 파티션 → 통계
    store: Arc<RwLock<HashMap<String, HashMap<String, TableStats>>>>,
}

impl StatisticsManager {
    pub fn new() -> Self {
        Self { store: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn upsert(&self, stats: TableStats) {
        let mut guard = self.store.write().unwrap();
        guard
            .entry(stats.cube_name.clone())
            .or_default()
            .insert(stats.partition.clone(), stats);
    }

    pub fn get_table(&self, cube: &str, partition: &str) -> Option<TableStats> {
        let guard = self.store.read().unwrap();
        guard.get(cube)?.get(partition).cloned()
    }

    /// 전체 파티션 집계 통계 (row_count 합산, min/max 병합)
    pub fn get_aggregate(&self, cube: &str) -> TableStats {
        let guard = self.store.read().unwrap();
        let partitions = match guard.get(cube) {
            Some(p) => p,
            None    => return TableStats { cube_name: cube.to_string(), ..Default::default() },
        };

        let mut agg = TableStats { cube_name: cube.to_string(), partition: "*".to_string(), ..Default::default() };
        for stats in partitions.values() {
            agg.row_count += stats.row_count;
            for (col, cs) in &stats.column_stats {
                let entry = agg.column_stats.entry(col.clone()).or_default();
                entry.row_count  += cs.row_count;
                entry.null_count += cs.null_count;
                entry.ndv = entry.ndv.max(cs.ndv);

                if let Some(ref min) = cs.min_val {
                    entry.min_val = Some(match &entry.min_val {
                        None    => min.clone(),
                        Some(m) => if min < m { min.clone() } else { m.clone() },
                    });
                }
                if let Some(ref max) = cs.max_val {
                    entry.max_val = Some(match &entry.max_val {
                        None    => max.clone(),
                        Some(m) => if max > m { max.clone() } else { m.clone() },
                    });
                }
            }
        }
        agg
    }

    /// 컬럼 통계 증분 업데이트 (쓰기 완료 시 호출)
    pub fn increment_row_count(&self, cube: &str, partition: &str, delta: u64) {
        let mut guard = self.store.write().unwrap();
        if let Some(entry) = guard.get_mut(cube).and_then(|p| p.get_mut(partition)) {
            entry.row_count += delta;
        }
    }
}

impl Default for StatisticsManager {
    fn default() -> Self {
        Self::new()
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_stats(cube: &str, partition: &str, rows: u64, ndv: u64) -> TableStats {
        let mut stats = TableStats {
            cube_name: cube.to_string(),
            partition: partition.to_string(),
            row_count: rows,
            ..Default::default()
        };
        stats.column_stats.insert("event_name".to_string(), ColumnStats {
            row_count: rows,
            ndv,
            min_val: Some(b"click".to_vec()),
            max_val: Some(b"view".to_vec()),
            ..Default::default()
        });
        stats
    }

    #[test]
    fn test_upsert_and_aggregate() {
        let mgr = StatisticsManager::new();
        mgr.upsert(make_stats("page_events", "p_2024_q1", 1_000_000, 5));
        mgr.upsert(make_stats("page_events", "p_2024_q2", 2_000_000, 5));

        let agg = mgr.get_aggregate("page_events");
        assert_eq!(agg.row_count, 3_000_000);
        assert_eq!(agg.column_stats["event_name"].ndv, 5);
    }

    #[test]
    fn test_selectivity_eq() {
        let cs = ColumnStats { row_count: 1000, ndv: 10, null_count: 0, ..Default::default() };
        assert!((cs.selectivity_eq(b"view") - 0.1).abs() < 0.01);
    }
}
