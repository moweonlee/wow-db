// T044: AVX2 predicate 필터 — 비트마스크 패킹, 배치 선택

use arrow2::array::Array;
use arrow2::chunk::Chunk;

use super::scan::apply_selection;

// ─── 필터 조건 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum FilterExpr {
    Eq   { col_idx: usize, value: i64 },
    Range { col_idx: usize, lo: i64, hi: i64 },
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
}

// ─── 비트마스크 기반 필터 ──────────────────────────────────────────────────────

/// i64 배열에 대한 비교 결과 비트마스크 생성
pub fn build_bitmask_i64_eq(values: &[i64], target: i64) -> Vec<u8> {
    let n = values.len();
    let n_bytes = (n + 7) / 8;
    let mut mask = vec![0u8; n_bytes];
    // TODO (Phase C): AVX2 _mm256_cmpeq_epi64 + movemask
    for (i, &v) in values.iter().enumerate() {
        if v == target {
            mask[i / 8] |= 1 << (i % 8);
        }
    }
    mask
}

pub fn build_bitmask_i64_range(values: &[i64], lo: i64, hi: i64) -> Vec<u8> {
    let n = values.len();
    let n_bytes = (n + 7) / 8;
    let mut mask = vec![0u8; n_bytes];
    for (i, &v) in values.iter().enumerate() {
        if v >= lo && v <= hi {
            mask[i / 8] |= 1 << (i % 8);
        }
    }
    mask
}

/// 비트마스크에서 선택 인덱스 목록 추출
pub fn bitmask_to_indices(mask: &[u8], n: usize) -> Vec<usize> {
    let mut result = Vec::new();
    for i in 0..n {
        if mask[i / 8] & (1 << (i % 8)) != 0 {
            result.push(i);
        }
    }
    result
}

pub fn bitmask_and(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b.iter()).map(|(x, y)| x & y).collect()
}

pub fn bitmask_or(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b.iter()).map(|(x, y)| x | y).collect()
}

/// Arrow2 배치에서 bitmask 조건을 만족하는 행만 선택
pub fn filter_batch(
    batch: &Chunk<Box<dyn Array>>,
    mask:  &[u8],
) -> Chunk<Box<dyn Array>> {
    let n = batch.len();
    let indices = bitmask_to_indices(mask, n);
    apply_selection(batch, &indices)
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmask_eq() {
        let vals = vec![1i64, 2, 3, 2, 1];
        let mask = build_bitmask_i64_eq(&vals, 2);
        let idx  = bitmask_to_indices(&mask, 5);
        assert_eq!(idx, vec![1, 3]);
    }

    #[test]
    fn test_bitmask_range() {
        let vals: Vec<i64> = (0..10).collect();
        let mask = build_bitmask_i64_range(&vals, 3, 7);
        let idx  = bitmask_to_indices(&mask, 10);
        assert_eq!(idx, vec![3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_bitmask_and() {
        let a = vec![0b00001111u8];
        let b = vec![0b00110011u8];
        let c = bitmask_and(&a, &b);
        assert_eq!(c, vec![0b00000011]);
    }
}
