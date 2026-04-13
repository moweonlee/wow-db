// T077: per-Granule SET 인덱스 — IN 조건 가속, 저기수 컬럼 최적화

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use tracing::debug;

// ─── Granule SET 인덱스 ───────────────────────────────────────────────────────

/// 단일 Granule(8,192행 블록)에 대한 SET 인덱스
///
/// 해당 Granule에 존재하는 유니크 값 집합을 저장.
/// `WHERE col IN (v1, v2, ...)` 조건의 Granule 스킵을 지원.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GranuleSetIndex {
    /// Granule 내 유니크 값 집합 (string 표현)
    values: HashSet<String>,
    /// 최대 집합 크기. 초과 시 `overflow = true`로 마킹 (스킵 불가)
    max_values: usize,
    /// 값 집합이 `max_values`를 초과했는지 여부
    overflow: bool,
}

impl GranuleSetIndex {
    pub fn new(max_values: usize) -> Self {
        Self { values: HashSet::new(), max_values, overflow: false }
    }

    /// 값 추가 (Granule 구축 시 호출)
    pub fn insert(&mut self, val: &str) {
        if self.overflow { return; }
        self.values.insert(val.to_string());
        if self.values.len() > self.max_values {
            self.overflow = true;
            self.values.clear(); // 메모리 절약
        }
    }

    /// IN 조건 — 이 Granule을 스킵할 수 있는지 판단
    ///
    /// `true`: Granule에 해당 값이 **없음이 확실** → 스킵 가능
    /// `false`: Granule에 해당 값이 있을 수 있음 → 스캔 필요
    pub fn can_skip_in(&self, candidates: &HashSet<String>) -> bool {
        if self.overflow { return false; }
        // 교집합이 없으면 스킵 가능
        candidates.iter().all(|v| !self.values.contains(v))
    }

    /// 단일 값 등가 조건 (= 연산)
    pub fn can_skip_eq(&self, val: &str) -> bool {
        if self.overflow { return false; }
        !self.values.contains(val)
    }

    /// NOT IN 조건 — 집합의 모든 값이 NOT IN 목록에 포함되면 스킵 가능
    pub fn can_skip_not_in(&self, excluded: &HashSet<String>) -> bool {
        if self.overflow { return false; }
        // Granule 내 모든 값이 excluded에 속하면 NOT IN 결과는 모두 false
        !self.values.is_empty() && self.values.iter().all(|v| excluded.contains(v))
    }

    pub fn is_overflow(&self) -> bool { self.overflow }
    pub fn value_count(&self) -> usize { self.values.len() }
    pub fn values(&self) -> &HashSet<String> { &self.values }

    /// 바이트 직렬화
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// 바이트 역직렬화
    pub fn from_bytes(data: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }
}

// ─── SSTable SET 인덱스 (Granule 배열) ───────────────────────────────────────

/// SSTable의 모든 Granule에 대한 SET 인덱스 모음
pub struct SetIndex {
    granules:     Vec<GranuleSetIndex>,
    granule_size: usize, // 기본 8,192
}

impl SetIndex {
    pub fn new(granule_size: usize, max_values_per_granule: usize) -> Self {
        Self {
            granules:     Vec::new(),
            granule_size,
        }
    }

    /// 컬럼 값 배열로부터 SET 인덱스 구축
    pub fn build(values: &[&str], granule_size: usize, max_values: usize) -> Self {
        let mut idx = Self::new(granule_size, max_values);
        let chunk_count = values.len().div_ceil(granule_size);

        for i in 0..chunk_count {
            let start = i * granule_size;
            let end   = (start + granule_size).min(values.len());
            let mut g = GranuleSetIndex::new(max_values);
            for v in &values[start..end] {
                g.insert(v);
            }
            idx.granules.push(g);
        }
        idx
    }

    /// IN 조건에 대해 스킵 가능한 Granule 비트마스크 반환
    /// `true` = 스킵, `false` = 스캔 필요
    pub fn skip_mask_in(&self, candidates: &HashSet<String>) -> Vec<bool> {
        self.granules.iter()
            .map(|g| {
                let skip = g.can_skip_in(candidates);
                if skip {
                    debug!(granule_size = self.granule_size, "SET index: Granule skipped (IN)");
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
    pub fn granule(&self, idx: usize) -> Option<&GranuleSetIndex> { self.granules.get(idx) }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_granule_set_in_skip() {
        let mut g = GranuleSetIndex::new(100);
        for v in &["click", "view", "scroll"] { g.insert(v); }

        let mut cands = HashSet::new();
        cands.insert("purchase".to_string());
        assert!(g.can_skip_in(&cands), "purchase 없음 → 스킵 가능");

        cands.insert("click".to_string());
        assert!(!g.can_skip_in(&cands), "click 있음 → 스킵 불가");
    }

    #[test]
    fn test_granule_set_eq() {
        let mut g = GranuleSetIndex::new(100);
        g.insert("alpha");
        g.insert("beta");

        assert!(g.can_skip_eq("gamma"), "gamma 없음 → 스킵");
        assert!(!g.can_skip_eq("alpha"), "alpha 있음 → 스캔");
    }

    #[test]
    fn test_overflow_disables_skip() {
        let mut g = GranuleSetIndex::new(2); // 최대 2개
        g.insert("a");
        g.insert("b");
        g.insert("c"); // 초과 → overflow

        assert!(g.is_overflow());
        assert!(!g.can_skip_eq("z"), "overflow → 스킵 불가");

        let mut cands = HashSet::new();
        cands.insert("z".to_string());
        assert!(!g.can_skip_in(&cands), "overflow → IN 스킵 불가");
    }

    #[test]
    fn test_set_index_build() {
        let values: Vec<&str> = (0..20000).map(|i| {
            if i % 3 == 0 { "click" } else if i % 3 == 1 { "view" } else { "scroll" }
        }).collect();

        let idx = SetIndex::build(&values, 8192, 1000);
        assert_eq!(idx.granule_count(), 3); // ceil(20000/8192) = 3

        // 모든 Granule에 click, view, scroll 존재
        let mut cands = HashSet::new();
        cands.insert("purchase".to_string());
        let mask = idx.skip_mask_in(&cands);
        assert!(mask.iter().all(|&s| s), "purchase 없음 → 모두 스킵");

        let mask_click = idx.skip_mask_eq("click");
        assert!(mask_click.iter().all(|&s| !s), "click 있음 → 모두 스캔");
    }

    #[test]
    fn test_serialization() {
        let mut g = GranuleSetIndex::new(100);
        g.insert("event_a");
        g.insert("event_b");

        let bytes = g.to_bytes().unwrap();
        let restored = GranuleSetIndex::from_bytes(&bytes).unwrap();
        assert_eq!(restored.value_count(), 2);
        assert!(!restored.can_skip_eq("event_a"));
        assert!(restored.can_skip_eq("event_c"));
    }

    #[test]
    fn test_not_in_skip() {
        let mut g = GranuleSetIndex::new(100);
        g.insert("old_event");

        let mut excluded = HashSet::new();
        excluded.insert("old_event".to_string());

        assert!(g.can_skip_not_in(&excluded), "모든 값이 NOT IN 목록 → 스킵");

        excluded.clear();
        excluded.insert("other".to_string());
        assert!(!g.can_skip_not_in(&excluded), "값 일부가 NOT IN 외부 → 스캔");
    }
}
