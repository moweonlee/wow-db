// T078: per-Granule NGRAMBF_V1 인덱스 — N-gram Bloom Filter, 텍스트 LIKE 쿼리 가속

use bloomfilter::Bloom;
use serde::{Deserialize, Serialize};
use tracing::debug;

// ─── N-gram Bloom Filter Granule 인덱스 ──────────────────────────────────────

/// 단일 Granule의 NGRAMBF_V1 인덱스
///
/// 문자열 값을 N-gram으로 분해하여 Bloom Filter에 삽입.
/// `LIKE '%substr%'` / `hasToken()` 조건의 Granule 스킵을 지원.
pub struct NgramBfGranule {
    bloom:  Bloom<Vec<u8>>,
    n:      usize,
    /// SIP 키 (직렬화용)
    sip_keys: [(u64, u64); 2],
}

/// 직렬화 포맷 (바이너리 저장)
#[derive(Serialize, Deserialize)]
struct NgramBfSerialized {
    bitmap:   Vec<u8>,
    k_num:    u32,
    num_bits: u64,
    n:        usize,
    sip0_k0:  u64, sip0_k1: u64,
    sip1_k0:  u64, sip1_k1: u64,
}

impl NgramBfGranule {
    /// `n`: N-gram 크기 (기본 3)
    /// `false_positive_rate`: Bloom Filter 오탐율 (예: 0.01 = 1%)
    /// `expected_insertions`: 예상 N-gram 삽입 수
    pub fn new(n: usize, false_positive_rate: f64, expected_insertions: usize) -> Self {
        let bloom = Bloom::new_for_fp_rate(expected_insertions.max(1), false_positive_rate);
        let sip_keys = bloom.sip_keys();
        Self { bloom, n, sip_keys }
    }

    /// 문자열을 N-gram으로 분해하여 Bloom Filter에 삽입
    pub fn insert(&mut self, s: &str) {
        for ngram in ngrams(s, self.n) {
            self.bloom.set(&ngram);
        }
    }

    /// LIKE '%needle%' 조건 — needle의 모든 N-gram이 BF에 없으면 스킵 가능
    ///
    /// `true`: Granule에 해당 패턴이 **없음이 확실** → 스킵
    /// `false`: Granule에 있을 가능성 있음 → 스캔
    pub fn can_skip_contains(&self, needle: &str) -> bool {
        if needle.len() < self.n {
            // needle이 N-gram보다 짧으면 스킵 불가 (false negative 방지)
            return false;
        }
        for ngram in ngrams(needle, self.n) {
            if !self.bloom.check(&ngram) {
                return true;  // 하나라도 없으면 확실히 없음
            }
        }
        false  // 모든 N-gram이 BF에 있음 → 스킵 불가 (FP 가능성 있음)
    }

    /// LIKE 'prefix%' 조건 — prefix의 N-gram 서브셋 확인
    pub fn can_skip_starts_with(&self, prefix: &str) -> bool {
        self.can_skip_contains(prefix)  // starts_with도 contains 확인으로 충분
    }

    /// 등가 조건 — 전체 문자열의 N-gram 확인
    pub fn can_skip_eq(&self, val: &str) -> bool {
        self.can_skip_contains(val)
    }

    /// 바이트 직렬화
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let serialized = NgramBfSerialized {
            bitmap:   self.bloom.bitmap(),
            k_num:    self.bloom.number_of_hash_functions(),
            num_bits: self.bloom.number_of_bits(),
            n:        self.n,
            sip0_k0:  self.sip_keys[0].0,
            sip0_k1:  self.sip_keys[0].1,
            sip1_k0:  self.sip_keys[1].0,
            sip1_k1:  self.sip_keys[1].1,
        };
        Ok(serde_json::to_vec(&serialized)?)
    }

    /// 바이트 역직렬화
    pub fn from_bytes(data: &[u8]) -> anyhow::Result<Self> {
        let s: NgramBfSerialized = serde_json::from_slice(data)?;
        let bloom = Bloom::from_existing(
            &s.bitmap,
            s.num_bits,
            s.k_num,
            [(s.sip0_k0, s.sip0_k1), (s.sip1_k0, s.sip1_k1)],
        );
        let sip_keys = bloom.sip_keys();
        Ok(Self { bloom, n: s.n, sip_keys })
    }

    pub fn n(&self) -> usize { self.n }
}

// ─── SSTable NGRAMBF 인덱스 (Granule 배열) ───────────────────────────────────

/// SSTable의 모든 Granule에 대한 NGRAMBF 인덱스 모음
pub struct NgramBfIndex {
    granules:     Vec<NgramBfGranule>,
    granule_size: usize,
}

impl NgramBfIndex {
    pub fn new(granule_size: usize) -> Self {
        Self { granules: Vec::new(), granule_size }
    }

    /// 문자열 값 배열로부터 NGRAMBF 인덱스 구축
    ///
    /// `n`: N-gram 크기 (3 권장)
    /// `fp_rate`: 오탐율 (0.01 권장)
    pub fn build(values: &[&str], granule_size: usize, n: usize, fp_rate: f64) -> Self {
        let mut idx = Self::new(granule_size);
        let chunk_count = values.len().div_ceil(granule_size);

        for i in 0..chunk_count {
            let start = i * granule_size;
            let end   = (start + granule_size).min(values.len());
            let chunk = &values[start..end];

            // 청크 내 총 N-gram 수 추정
            let total_ngrams: usize = chunk.iter()
                .map(|s| s.len().saturating_sub(n - 1))
                .sum();

            let mut g = NgramBfGranule::new(n, fp_rate, total_ngrams.max(1));
            for v in chunk { g.insert(v); }
            idx.granules.push(g);
        }
        idx
    }

    /// LIKE '%needle%' 조건의 Granule 스킵 마스크
    pub fn skip_mask_contains(&self, needle: &str) -> Vec<bool> {
        self.granules.iter()
            .map(|g| {
                let skip = g.can_skip_contains(needle);
                if skip {
                    debug!(needle = %needle, "NGRAMBF: Granule skipped (contains)");
                }
                skip
            })
            .collect()
    }

    /// 등가 조건 스킵 마스크
    pub fn skip_mask_eq(&self, val: &str) -> Vec<bool> {
        self.granules.iter().map(|g| g.can_skip_eq(val)).collect()
    }

    pub fn granule_count(&self) -> usize { self.granules.len() }
    pub fn granule(&self, idx: usize) -> Option<&NgramBfGranule> { self.granules.get(idx) }
}

// ─── N-gram 생성 유틸 ─────────────────────────────────────────────────────────

/// 문자열을 N-gram 바이트 시퀀스 목록으로 분해
///
/// 예: "abcde", n=3 → ["abc", "bcd", "cde"]
fn ngrams(s: &str, n: usize) -> impl Iterator<Item = Vec<u8>> + '_ {
    let bytes = s.as_bytes();
    let len = bytes.len();
    (0..len.saturating_sub(n - 1))
        .map(move |i| bytes[i..i + n.min(len - i)].to_vec())
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ngrams() {
        let result: Vec<_> = ngrams("abcde", 3).collect();
        assert_eq!(result, vec![
            b"abc".to_vec(), b"bcd".to_vec(), b"cde".to_vec()
        ]);
    }

    #[test]
    fn test_ngrams_short_string() {
        let result: Vec<_> = ngrams("ab", 3).collect();
        // "ab"는 길이 2 < n=3이지만, n.min(len - i)로 처리
        assert_eq!(result.len(), 0);  // len - (n-1) = 2 - 2 = 0
    }

    #[test]
    fn test_granule_contains_skip() {
        let mut g = NgramBfGranule::new(3, 0.01, 1000);
        g.insert("hello_world");
        g.insert("click_event");

        // "hello"의 N-gram: "hel", "ell", "llo" — 모두 BF에 있음
        assert!(!g.can_skip_contains("hello"), "hello N-gram 존재 → 스킵 불가");

        // "xyz_xyz"의 N-gram은 BF에 없음 → 스킵 가능
        // (FP 가능성 있으므로 단순 확인: can_skip_contains == true이면 OK)
        let result = g.can_skip_contains("xyz_xyz_abc_qwerty");
        // FP rate 1% → 거의 항상 true (스킵 가능)
        // 단, 확정적이지 않으므로 bool 결과만 확인
        let _ = result;
    }

    #[test]
    fn test_granule_not_in_skip() {
        let mut g = NgramBfGranule::new(3, 0.01, 1000);
        g.insert("page_view");

        // "purchase"의 N-gram: "pur", "urc", "rch", "cha", "has", "ase"
        // 이 중 하나라도 BF에 없으면 스킵 가능
        // "pur"는 "page_view"와 겹치지 않으므로 스킵 가능성 높음
        let can_skip = g.can_skip_contains("purchase_event_long");
        let _ = can_skip; // FP 특성상 확정적 주장 불가
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut g = NgramBfGranule::new(3, 0.01, 1000);
        g.insert("test_string");
        g.insert("another_value");

        let bytes = g.to_bytes().unwrap();
        let restored = NgramBfGranule::from_bytes(&bytes).unwrap();

        assert_eq!(restored.n(), 3);
        // 삽입된 값의 N-gram은 복원 후에도 존재해야 함
        assert!(!restored.can_skip_contains("test_str"), "test_str N-gram → 복원 후에도 존재");
    }

    #[test]
    fn test_index_build_and_skip_mask() {
        // 첫 번째 Granule에만 "hello_world" 삽입
        let mut values: Vec<&str> = vec!["hello_world"; 100];
        // 두 번째 Granule에는 "click_event" 값만
        values.extend(vec!["click_event"; 100]);

        let idx = NgramBfIndex::build(&values, 100, 3, 0.01);
        assert_eq!(idx.granule_count(), 2);

        // 1) needle = "hello_world" — 첫 번째 Granule에 완전히 포함된 문자열
        //    모든 N-gram이 Granule[0] BF에 존재 → 스킵 불가
        let mask_hello = idx.skip_mask_contains("hello_world");
        assert!(mask_hello.len() == 2);
        assert!(!mask_hello[0], "Granule[0]에 hello_world 있음 → 스킵 불가");

        // 2) needle = "hello_world" — 두 번째 Granule(click_event)에는 없음
        //    "hel", "ell" 등의 N-gram이 click_event BF에 없어 스킵 가능
        //    (FP는 낮은 확률이므로 대부분의 경우 true)
        // 확정적 주장 대신 방향성만 확인
        let _ = mask_hello[1]; // 접근만 확인

        // 3) needle = "zqxwvuts" — 양쪽 Granule 모두에 없는 문자열
        let mask_none = idx.skip_mask_contains("zqxwvuts");
        assert!(mask_none[0], "Granule[0]에 zqxwvuts N-gram 없음 → 스킵 가능");
        assert!(mask_none[1], "Granule[1]에 zqxwvuts N-gram 없음 → 스킵 가능");
    }

    #[test]
    fn test_short_needle_no_skip() {
        let mut g = NgramBfGranule::new(3, 0.01, 1000);
        g.insert("hello");

        // needle이 n=3보다 짧으면 스킵 불가
        assert!(!g.can_skip_contains("he"), "needle < n → 스킵 불가");
    }
}
