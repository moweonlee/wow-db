// T045: AVX-512 64비트 누산 집계 — SUM/COUNT/MIN/MAX

use super::scan::has_avx512;

// ─── 집계 연산 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggOp {
    Sum,
    Count,
    Min,
    Max,
    CountDistinct,
}

// ─── i64 집계 (nullable) ──────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct AggResult {
    pub sum:   i64,
    pub count: u64,
    pub min:   Option<i64>,
    pub max:   Option<i64>,
}

/// i64 nullable 배열에 대한 집계
/// AVX-512 가용 시 SIMD 경로 (현재: 안전한 스칼라 구현)
pub fn agg_i64(values: &[Option<i64>]) -> AggResult {
    // TODO (Phase C): AVX-512 _mm512_add_epi64 + masked operations
    let mut result = AggResult::default();
    for &v in values {
        if let Some(x) = v {
            result.sum += x;
            result.count += 1;
            result.min = Some(match result.min {
                None    => x,
                Some(m) => m.min(x),
            });
            result.max = Some(match result.max {
                None    => x,
                Some(m) => m.max(x),
            });
        }
    }
    result
}

/// 비-nullable i64 배열 고속 집계
pub fn agg_i64_non_null(values: &[i64]) -> AggResult {
    if has_avx512() {
        agg_i64_simd(values)
    } else {
        agg_i64_scalar(values)
    }
}

fn agg_i64_scalar(values: &[i64]) -> AggResult {
    let mut result = AggResult { count: values.len() as u64, ..Default::default() };
    for &v in values {
        result.sum += v;
        result.min = Some(match result.min { None => v, Some(m) => m.min(v) });
        result.max = Some(match result.max { None => v, Some(m) => m.max(v) });
    }
    result
}

#[cfg(target_arch = "x86_64")]
fn agg_i64_simd(values: &[i64]) -> AggResult {
    // TODO (Phase C): AVX-512 _mm512_reduce_add_epi64
    agg_i64_scalar(values)
}

#[cfg(not(target_arch = "x86_64"))]
fn agg_i64_simd(values: &[i64]) -> AggResult {
    agg_i64_scalar(values)
}

// ─── f64 집계 ─────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct AggResultF64 {
    pub sum:   f64,
    pub count: u64,
    pub min:   Option<f64>,
    pub max:   Option<f64>,
}

pub fn agg_f64(values: &[Option<f64>]) -> AggResultF64 {
    let mut result = AggResultF64::default();
    for &v in values {
        if let Some(x) = v {
            result.sum += x;
            result.count += 1;
            result.min = Some(match result.min { None => x, Some(m) => m.min(x) });
            result.max = Some(match result.max { None => x, Some(m) => m.max(x) });
        }
    }
    result
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_agg_i64_basic() {
        let vals: Vec<Option<i64>> = vec![Some(1), Some(2), None, Some(3), Some(4)];
        let r = agg_i64(&vals);
        assert_eq!(r.sum, 10);
        assert_eq!(r.count, 4);
        assert_eq!(r.min, Some(1));
        assert_eq!(r.max, Some(4));
    }

    #[test]
    fn test_agg_i64_non_null() {
        let vals: Vec<i64> = (1..=100).collect();
        let r = agg_i64_non_null(&vals);
        assert_eq!(r.sum, 5050);
        assert_eq!(r.count, 100);
        assert_eq!(r.min, Some(1));
        assert_eq!(r.max, Some(100));
    }

    #[test]
    fn test_agg_empty() {
        let r = agg_i64(&[]);
        assert_eq!(r.count, 0);
        assert_eq!(r.min, None);
    }
}
