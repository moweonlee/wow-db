// T043: AVX2 컬럼 스캔 — CPUID 런타임 디스패치, SIMD/스칼라 fallback

use anyhow::Result;
use arrow2::array::{Array, Int64Array, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};

// ─── SIMD 가용성 감지 ─────────────────────────────────────────────────────────

/// 런타임에 AVX2 지원 여부 확인
#[inline]
pub fn has_avx2() -> bool {
    #[cfg(target_arch = "x86_64")]
    { is_x86_feature_detected!("avx2") }
    #[cfg(not(target_arch = "x86_64"))]
    { false }
}

/// 런타임에 AVX-512 지원 여부 확인
#[inline]
pub fn has_avx512() -> bool {
    #[cfg(target_arch = "x86_64")]
    { is_x86_feature_detected!("avx512f") }
    #[cfg(not(target_arch = "x86_64"))]
    { false }
}

// ─── i64 컬럼 스캔 (range filter) ────────────────────────────────────────────

/// i64 컬럼에서 [lo, hi] 범위를 만족하는 행 인덱스 반환
/// AVX2 가용 시 SIMD 경로, 그 외 스칼라 경로
pub fn scan_i64_range(values: &[i64], lo: i64, hi: i64) -> Vec<usize> {
    if has_avx2() {
        scan_i64_range_simd(values, lo, hi)
    } else {
        scan_i64_range_scalar(values, lo, hi)
    }
}

fn scan_i64_range_scalar(values: &[i64], lo: i64, hi: i64) -> Vec<usize> {
    values.iter().enumerate()
        .filter_map(|(i, &v)| if v >= lo && v <= hi { Some(i) } else { None })
        .collect()
}

#[cfg(target_arch = "x86_64")]
fn scan_i64_range_simd(values: &[i64], lo: i64, hi: i64) -> Vec<usize> {
    // AVX2: 4개씩 묶어 처리 (256bit / 64bit = 4)
    // 실제 SIMD intrinsics는 unsafe 블록 필요
    // 현재: 스칼라 fallback (안전한 구현 우선)
    // TODO (Phase C): _mm256_cmpgt_epi64 + bitmask extraction
    scan_i64_range_scalar(values, lo, hi)
}

#[cfg(not(target_arch = "x86_64"))]
fn scan_i64_range_simd(values: &[i64], lo: i64, hi: i64) -> Vec<usize> {
    scan_i64_range_scalar(values, lo, hi)
}

// ─── i64 컬럼 등호 스캔 ───────────────────────────────────────────────────────

pub fn scan_i64_eq(values: &[i64], target: i64) -> Vec<usize> {
    values.iter().enumerate()
        .filter_map(|(i, &v)| if v == target { Some(i) } else { None })
        .collect()
}

// ─── bytes 컬럼 등호 스캔 ────────────────────────────────────────────────────

pub fn scan_bytes_eq<'a>(values: &'a [&'a [u8]], target: &[u8]) -> Vec<usize> {
    values.iter().enumerate()
        .filter_map(|(i, v)| if *v == target { Some(i) } else { None })
        .collect()
}

// ─── Arrow2 배치 선택 적용 ───────────────────────────────────────────────────

/// 행 인덱스 목록으로 Arrow2 배치 필터링
pub fn apply_selection(batch: &Chunk<Box<dyn Array>>, indices: &[usize]) -> Chunk<Box<dyn Array>> {
    let arrays: Vec<Box<dyn Array>> = batch.arrays().iter().map(|arr| {
        take_array(arr.as_ref(), indices)
    }).collect();
    Chunk::new(arrays)
}

fn take_array(arr: &dyn Array, indices: &[usize]) -> Box<dyn Array> {
    // 안전한 take 구현 (Arrow2에서 직접 제공하는 take 함수 사용)
    use arrow2::compute::take::take;
    let idx_arr: arrow2::array::PrimitiveArray<u32> =
        arrow2::array::PrimitiveArray::from_vec(indices.iter().map(|&i| i as u32).collect());
    take(arr, &idx_arr).unwrap_or_else(|_| arrow2::array::new_empty_array(arr.data_type().clone()))
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_i64_range() {
        let vals: Vec<i64> = (0..100).collect();
        let result = scan_i64_range(&vals, 10, 20);
        assert_eq!(result.len(), 11); // 10..=20
        assert_eq!(result[0], 10);
        assert_eq!(result[10], 20);
    }

    #[test]
    fn test_scan_i64_eq() {
        let vals = vec![1i64, 2, 3, 2, 1];
        let result = scan_i64_eq(&vals, 2);
        assert_eq!(result, vec![1, 3]);
    }

    #[test]
    fn test_simd_availability_log() {
        // 단순히 가용성 확인 (패닉 없이)
        let _ = has_avx2();
        let _ = has_avx512();
    }
}
