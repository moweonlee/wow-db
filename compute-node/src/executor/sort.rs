// T048: 외부 정렬, Merge Sort, Top-K

use std::cmp::Ordering;

use anyhow::Result;
use arrow2::array::{Array, PrimitiveArray};
use arrow2::chunk::Chunk;

use crate::executor::simd::scan::apply_selection;

// ─── 정렬 키 명세 ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SortKey {
    pub col_idx:    usize,
    pub descending: bool,
    pub nulls_first: bool,
}

// ─── 인메모리 정렬 ────────────────────────────────────────────────────────────

/// Arrow2 배치를 Sort Key 기준으로 정렬하여 반환
pub fn sort_batch(
    batch: &Chunk<Box<dyn Array>>,
    sort_keys: &[SortKey],
) -> Result<Chunk<Box<dyn Array>>> {
    let n = batch.len();
    let mut indices: Vec<usize> = (0..n).collect();

    indices.sort_unstable_by(|&a, &b| {
        for sk in sort_keys {
            let ord = compare_rows(batch, a, b, sk);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });

    Ok(apply_selection(batch, &indices))
}

fn compare_rows(
    batch:    &Chunk<Box<dyn Array>>,
    row_a:    usize,
    row_b:    usize,
    sort_key: &SortKey,
) -> Ordering {
    let arr = match batch.arrays().get(sort_key.col_idx) {
        Some(a) => a,
        None    => return Ordering::Equal,
    };

    let va = extract_i64(arr.as_ref(), row_a);
    let vb = extract_i64(arr.as_ref(), row_b);

    let ord = match (va, vb) {
        (None, None)     => Ordering::Equal,
        (None, Some(_))  => if sort_key.nulls_first { Ordering::Less } else { Ordering::Greater },
        (Some(_), None)  => if sort_key.nulls_first { Ordering::Greater } else { Ordering::Less },
        (Some(a), Some(b)) => a.cmp(&b),
    };

    if sort_key.descending { ord.reverse() } else { ord }
}

fn extract_i64(arr: &dyn Array, row: usize) -> Option<i64> {
    let prim = arr.as_any().downcast_ref::<PrimitiveArray<i64>>()?;
    if prim.is_valid(row) { Some(prim.value(row)) } else { None }
}

// ─── Top-K ────────────────────────────────────────────────────────────────────

/// Arrow2 배치에서 Top-K 행 반환
pub fn top_k(
    batch:     &Chunk<Box<dyn Array>>,
    k:         usize,
    sort_keys: &[SortKey],
) -> Result<Chunk<Box<dyn Array>>> {
    let n = batch.len();
    if k >= n {
        return sort_batch(batch, sort_keys);
    }

    let mut indices: Vec<usize> = (0..n).collect();
    // partial_sort: k개 선택 후 정렬
    indices.select_nth_unstable_by(k - 1, |&a, &b| {
        for sk in sort_keys {
            let ord = compare_rows(batch, a, b, sk);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });

    indices.truncate(k);
    indices.sort_unstable_by(|&a, &b| {
        for sk in sort_keys {
            let ord = compare_rows(batch, a, b, sk);
            if ord != Ordering::Equal { return ord; }
        }
        Ordering::Equal
    });

    Ok(apply_selection(batch, &indices))
}

// ─── Merge Sort (다중 배치 병합) ─────────────────────────────────────────────

/// 이미 정렬된 여러 배치를 Merge Sort로 병합
pub fn merge_sorted_batches(
    batches:   Vec<Chunk<Box<dyn Array>>>,
    sort_keys: &[SortKey],
) -> Result<Chunk<Box<dyn Array>>> {
    if batches.is_empty() {
        return Ok(Chunk::new(Vec::new()));
    }
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }

    // 단순 구현: 전체 연결 후 정렬
    // TODO (Phase C): k-way merge heap 기반 효율적 구현
    use arrow2::compute::concatenate::concatenate;

    let n_cols = batches[0].arrays().len();
    let mut merged_arrays: Vec<Box<dyn Array>> = Vec::new();

    for col_idx in 0..n_cols {
        let col_arrs: Vec<&dyn Array> = batches.iter()
            .map(|b| b.arrays()[col_idx].as_ref())
            .collect();
        let merged = concatenate(&col_arrs)?;
        merged_arrays.push(merged);
    }

    let combined = Chunk::new(merged_arrays);
    sort_batch(&combined, sort_keys)
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_batch(vals: Vec<i64>) -> Chunk<Box<dyn Array>> {
        let arr: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vals));
        Chunk::new(vec![arr])
    }

    #[test]
    fn test_sort_ascending() {
        let batch = make_batch(vec![5, 3, 1, 4, 2]);
        let sk    = vec![SortKey { col_idx: 0, descending: false, nulls_first: false }];
        let sorted = sort_batch(&batch, &sk).unwrap();
        let arr = sorted.arrays()[0].as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
        let vals: Vec<i64> = (0..arr.len()).map(|i| arr.value(i)).collect();
        assert_eq!(vals, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_sort_descending() {
        let batch = make_batch(vec![3, 1, 2]);
        let sk    = vec![SortKey { col_idx: 0, descending: true, nulls_first: false }];
        let sorted = sort_batch(&batch, &sk).unwrap();
        let arr = sorted.arrays()[0].as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
        let vals: Vec<i64> = (0..arr.len()).map(|i| arr.value(i)).collect();
        assert_eq!(vals, vec![3, 2, 1]);
    }

    #[test]
    fn test_top_k() {
        let batch = make_batch(vec![5, 1, 3, 2, 4]);
        let sk    = vec![SortKey { col_idx: 0, descending: false, nulls_first: false }];
        let result = top_k(&batch, 3, &sk).unwrap();
        assert_eq!(result.len(), 3);
        let arr = result.arrays()[0].as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value(2), 3);
    }
}
