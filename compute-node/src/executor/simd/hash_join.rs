// T046: AVX2 Hash Build/Probe — 버킷 prefetch, Partitioned Hash Join

use std::collections::HashMap;

use ahash::AHashMap;
use arrow2::array::Array;
use arrow2::chunk::Chunk;

use super::scan::apply_selection;

// ─── 해시 조인 키 추출 ───────────────────────────────────────────────────────

/// 단일 i64 컬럼을 조인 키로 추출
pub fn extract_i64_keys(batch: &Chunk<Box<dyn Array>>, col_idx: usize) -> Vec<Option<i64>> {
    use arrow2::array::PrimitiveArray;
    if col_idx >= batch.arrays().len() {
        return vec![None; batch.len()];
    }
    let arr = &batch.arrays()[col_idx];
    if let Some(prim) = arr.as_any().downcast_ref::<PrimitiveArray<i64>>() {
        (0..prim.len())
            .map(|i| if prim.is_valid(i) { Some(prim.value(i)) } else { None })
            .collect()
    } else {
        vec![None; batch.len()]
    }
}

// ─── Hash Build Side ─────────────────────────────────────────────────────────

/// Build side: 해시 테이블 구축
pub struct HashBuildTable {
    /// key → 행 인덱스 목록
    table: AHashMap<i64, Vec<usize>>,
    build_batch: Chunk<Box<dyn Array>>,
}

impl HashBuildTable {
    pub fn build(batch: Chunk<Box<dyn Array>>, key_col: usize) -> Self {
        let keys = extract_i64_keys(&batch, key_col);
        let mut table: AHashMap<i64, Vec<usize>> = AHashMap::with_capacity(keys.len());

        for (i, key) in keys.into_iter().enumerate() {
            if let Some(k) = key {
                table.entry(k).or_default().push(i);
            }
        }

        Self { table, build_batch: batch }
    }

    pub fn row_count(&self) -> usize {
        self.build_batch.len()
    }
}

// ─── Hash Probe Side ─────────────────────────────────────────────────────────

/// Probe side: 프로브 배치와 빌드 테이블을 조인하여 결과 배치 반환
pub fn hash_probe(
    build:     &HashBuildTable,
    probe:     &Chunk<Box<dyn Array>>,
    probe_key_col: usize,
) -> Chunk<Box<dyn Array>> {
    let probe_keys = extract_i64_keys(probe, probe_key_col);

    let mut build_indices = Vec::new();
    let mut probe_indices = Vec::new();

    for (p_idx, key) in probe_keys.iter().enumerate() {
        if let Some(k) = key {
            if let Some(b_rows) = build.table.get(k) {
                for &b_idx in b_rows {
                    build_indices.push(b_idx);
                    probe_indices.push(p_idx);
                }
            }
        }
    }

    // TODO (Phase C): AVX2 prefetch + gather scatter
    // 빌드 측 선택 컬럼
    let build_selected = apply_selection(&build.build_batch, &build_indices);
    // 프로브 측 선택 컬럼
    let probe_selected = apply_selection(probe, &probe_indices);

    // 두 배치를 수평 연결
    let mut arrays = Vec::new();
    arrays.extend(build_selected.into_arrays());
    arrays.extend(probe_selected.into_arrays());
    Chunk::new(arrays)
}

// ─── Partitioned Hash Join ───────────────────────────────────────────────────

/// 대용량 조인을 위한 파티션 분할 빌드
pub struct PartitionedHashJoin {
    n_partitions: usize,
    partitions:   Vec<AHashMap<i64, Vec<(usize, usize)>>>, // (partition, row_idx)
}

impl PartitionedHashJoin {
    pub fn new(n_partitions: usize) -> Self {
        Self {
            n_partitions,
            partitions: vec![AHashMap::new(); n_partitions],
        }
    }

    pub fn partition_for(&self, key: i64) -> usize {
        // 단순 모듈러 파티셔닝
        (key.unsigned_abs() as usize) % self.n_partitions
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow2::array::PrimitiveArray;
    use arrow2::datatypes::DataType;

    fn make_i64_batch(values: Vec<i64>) -> Chunk<Box<dyn Array>> {
        let arr: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(values));
        Chunk::new(vec![arr])
    }

    #[test]
    fn test_hash_join_basic() {
        let build = make_i64_batch(vec![1, 2, 3, 4, 5]);
        let probe = make_i64_batch(vec![2, 4, 6]);

        let ht = HashBuildTable::build(build, 0);
        let result = hash_probe(&ht, &probe, 0);

        // 2, 4 매칭 → 2행 결과 (빌드 col + 프로브 col = 2 컬럼, 2행)
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_extract_keys() {
        let batch = make_i64_batch(vec![10, 20, 30]);
        let keys = extract_i64_keys(&batch, 0);
        assert_eq!(keys, vec![Some(10), Some(20), Some(30)]);
    }
}
