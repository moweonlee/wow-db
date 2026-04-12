// T047: GROUP BY 벡터화 집계 — Arrow2 batch 단위 처리

use std::collections::HashMap;

use ahash::AHashMap;
use anyhow::Result;
use arrow2::array::{Array, Int64Array, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};

use crate::executor::simd::agg::{AggOp, AggResult};

// ─── 집계 명세 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AggSpec {
    pub col_idx: usize,
    pub op:      AggOp,
    pub alias:   String,
}

// ─── 그룹별 집계 상태 ────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
struct GroupState {
    sum:   i64,
    count: u64,
    min:   Option<i64>,
    max:   Option<i64>,
}

impl GroupState {
    fn update(&mut self, val: Option<i64>) {
        if let Some(v) = val {
            self.sum += v;
            self.count += 1;
            self.min = Some(self.min.map_or(v, |m| m.min(v)));
            self.max = Some(self.max.map_or(v, |m| m.max(v)));
        }
    }

    fn merge(&mut self, other: &GroupState) {
        self.sum   += other.sum;
        self.count += other.count;
        if let Some(o) = other.min {
            self.min = Some(self.min.map_or(o, |m| m.min(o)));
        }
        if let Some(o) = other.max {
            self.max = Some(self.max.map_or(o, |m| m.max(o)));
        }
    }

    fn result(&self, op: AggOp) -> i64 {
        match op {
            AggOp::Sum   => self.sum,
            AggOp::Count | AggOp::CountDistinct => self.count as i64,
            AggOp::Min   => self.min.unwrap_or(0),
            AggOp::Max   => self.max.unwrap_or(0),
        }
    }
}

// ─── VectorizedGroupByAgg ─────────────────────────────────────────────────────

/// Arrow2 배치 단위 GROUP BY 집계
/// group_key_cols: GROUP BY 컬럼 인덱스 목록
/// agg_specs:      집계 명세
pub struct VectorizedGroupByAgg {
    group_key_cols: Vec<usize>,
    agg_specs:      Vec<AggSpec>,
    /// key: group key 직렬화 (컬럼별 i64 이어붙이기)
    state:          AHashMap<Vec<u8>, Vec<GroupState>>,
}

impl VectorizedGroupByAgg {
    pub fn new(group_key_cols: Vec<usize>, agg_specs: Vec<AggSpec>) -> Self {
        Self {
            group_key_cols,
            agg_specs,
            state: AHashMap::new(),
        }
    }

    /// 배치를 집계 상태에 누적
    pub fn accumulate(&mut self, batch: &Chunk<Box<dyn Array>>) -> Result<()> {
        let n = batch.len();
        let n_aggs = self.agg_specs.len();

        for row in 0..n {
            // 그룹 키 직렬화
            let mut key = Vec::with_capacity(self.group_key_cols.len() * 8);
            for &col_idx in &self.group_key_cols {
                let val = extract_i64_value(batch, col_idx, row).unwrap_or(i64::MIN);
                key.extend_from_slice(&val.to_le_bytes());
            }

            // 집계 상태 업데이트
            let states = self.state.entry(key).or_insert_with(|| {
                vec![GroupState::default(); n_aggs]
            });

            for (i, spec) in self.agg_specs.iter().enumerate() {
                let val = extract_i64_value(batch, spec.col_idx, row);
                states[i].update(val);
            }
        }
        Ok(())
    }

    /// 부분 집계 결과 병합 (CN 간 분산 집계)
    pub fn merge(&mut self, other: VectorizedGroupByAgg) {
        for (key, other_states) in other.state {
            let states = self.state.entry(key).or_insert_with(|| {
                vec![GroupState::default(); self.agg_specs.len()]
            });
            for (i, os) in other_states.iter().enumerate() {
                if i < states.len() {
                    states[i].merge(os);
                }
            }
        }
    }

    /// 최종 결과를 Arrow2 배치로 직렬화
    pub fn finish(self) -> Result<Chunk<Box<dyn Array>>> {
        let n_groups = self.state.len();

        // 그룹 키 컬럼 (i64)
        let n_key_cols = self.group_key_cols.len();
        let mut key_cols: Vec<Vec<Option<i64>>> = vec![Vec::with_capacity(n_groups); n_key_cols];

        // 집계 결과 컬럼
        let mut agg_cols: Vec<Vec<Option<i64>>> =
            vec![Vec::with_capacity(n_groups); self.agg_specs.len()];

        for (key_bytes, states) in &self.state {
            // 키 역직렬화
            for (k, key_col) in key_cols.iter_mut().enumerate() {
                let offset = k * 8;
                let val = if offset + 8 <= key_bytes.len() {
                    i64::from_le_bytes(key_bytes[offset..offset+8].try_into().unwrap_or([0;8]))
                } else { 0 };
                key_col.push(Some(val));
            }
            // 집계값
            for (i, spec) in self.agg_specs.iter().enumerate() {
                agg_cols[i].push(Some(states[i].result(spec.op)));
            }
        }

        // Arrow2 배열 생성
        let mut arrays: Vec<Box<dyn Array>> = Vec::new();
        for col in key_cols {
            arrays.push(Box::new(Int64Array::from(col)));
        }
        for col in agg_cols {
            arrays.push(Box::new(Int64Array::from(col)));
        }

        Ok(Chunk::new(arrays))
    }
}

fn extract_i64_value(batch: &Chunk<Box<dyn Array>>, col_idx: usize, row: usize) -> Option<i64> {
    use arrow2::array::PrimitiveArray;
    let arr = batch.arrays().get(col_idx)?;
    let prim = arr.as_any().downcast_ref::<PrimitiveArray<i64>>()?;
    if prim.is_valid(row) { Some(prim.value(row)) } else { None }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow2::array::PrimitiveArray;

    fn make_batch(keys: Vec<i64>, vals: Vec<i64>) -> Chunk<Box<dyn Array>> {
        let k: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(keys));
        let v: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vals));
        Chunk::new(vec![k, v])
    }

    #[test]
    fn test_group_by_sum() {
        let batch = make_batch(vec![1, 2, 1, 2, 1], vec![10, 20, 30, 40, 50]);
        let mut agg = VectorizedGroupByAgg::new(
            vec![0],
            vec![AggSpec { col_idx: 1, op: AggOp::Sum, alias: "sum_v".into() }],
        );
        agg.accumulate(&batch).unwrap();
        let result = agg.finish().unwrap();

        // 2개 그룹 (key=1: sum=90, key=2: sum=60)
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_group_by_count() {
        let batch = make_batch(vec![1, 1, 2], vec![0, 0, 0]);
        let mut agg = VectorizedGroupByAgg::new(
            vec![0],
            vec![AggSpec { col_idx: 1, op: AggOp::Count, alias: "cnt".into() }],
        );
        agg.accumulate(&batch).unwrap();
        let result = agg.finish().unwrap();
        assert_eq!(result.len(), 2);
    }
}
