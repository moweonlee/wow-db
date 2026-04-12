// T040: per-Granule MINMAX 인덱스 — 8,192행 단위, CBO predicate pushdown 지원

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// 기본 그래뉼 크기 (행 수)
pub const DEFAULT_GRANULE_SIZE: usize = 8192;

// ─── MinMax 레코드 ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GranuleMinMax {
    pub granule_idx: u32,
    pub row_offset:  u64,   // 세그먼트 내 시작 행 오프셋
    pub row_count:   u32,
    pub min_val:     Option<Vec<u8>>,  // 직렬화된 최솟값
    pub max_val:     Option<Vec<u8>>,  // 직렬화된 최댓값
}

// ─── MINMAX 인덱스 빌더 ───────────────────────────────────────────────────────

pub struct MinMaxBuilder {
    granule_size: usize,
    granules:     Vec<GranuleMinMax>,
}

impl MinMaxBuilder {
    pub fn new(granule_size: usize) -> Self {
        Self { granule_size, granules: Vec::new() }
    }

    /// bytes 슬라이스 값 배열로 MINMAX 인덱스 구축 (사전순 비교)
    pub fn build_bytes(&mut self, values: &[Option<Bytes>]) {
        self.granules.clear();
        for (g_idx, chunk) in values.chunks(self.granule_size).enumerate() {
            let row_offset = (g_idx * self.granule_size) as u64;
            let non_null: Vec<&Bytes> = chunk.iter().filter_map(|v| v.as_ref()).collect();

            let (min_val, max_val) = if non_null.is_empty() {
                (None, None)
            } else {
                let min = non_null.iter().min().map(|b| b.to_vec());
                let max = non_null.iter().max().map(|b| b.to_vec());
                (min, max)
            };

            self.granules.push(GranuleMinMax {
                granule_idx: g_idx as u32,
                row_offset,
                row_count: chunk.len() as u32,
                min_val,
                max_val,
            });
        }
    }

    pub fn finish(self) -> MinMaxIndex {
        MinMaxIndex { granules: self.granules }
    }
}

// ─── MINMAX 인덱스 ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinMaxIndex {
    pub granules: Vec<GranuleMinMax>,
}

impl MinMaxIndex {
    /// predicate: col >= lo AND col <= hi (bytes 범위 조건)
    /// 범위에 겹칠 수 있는 granule 인덱스 반환 (Data Skipping)
    pub fn candidate_granules_range(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
    ) -> Vec<usize> {
        self.granules
            .iter()
            .enumerate()
            .filter_map(|(i, g)| {
                // granule max < lo → skip
                if let (Some(ref gmax), Some(lo)) = (&g.max_val, lo) {
                    if gmax.as_slice() < lo { return None; }
                }
                // granule min > hi → skip
                if let (Some(ref gmin), Some(hi)) = (&g.min_val, hi) {
                    if gmin.as_slice() > hi { return None; }
                }
                Some(i)
            })
            .collect()
    }

    /// predicate: col = val (point lookup)
    pub fn candidate_granules_eq(&self, val: &[u8]) -> Vec<usize> {
        self.candidate_granules_range(Some(val), Some(val))
    }

    /// 인덱스 직렬화
    pub fn to_bytes(&self) -> anyhow::Result<Bytes> {
        Ok(Bytes::from(serde_json::to_vec(self)?))
    }

    /// 인덱스 역직렬화
    pub fn from_bytes(data: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }

    pub fn granule_count(&self) -> usize {
        self.granules.len()
    }

    /// 전체 통계: (global_min, global_max)
    pub fn global_min_max(&self) -> (Option<&[u8]>, Option<&[u8]>) {
        let min = self.granules.iter()
            .filter_map(|g| g.min_val.as_deref())
            .min();
        let max = self.granules.iter()
            .filter_map(|g| g.max_val.as_deref())
            .max();
        (min, max)
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes { Bytes::from(s.to_string()) }

    #[test]
    fn test_build_and_candidate() {
        let values: Vec<Option<Bytes>> = (0u32..20000)
            .map(|i| Some(Bytes::from(format!("{:08}", i))))
            .collect();

        let mut builder = MinMaxBuilder::new(8192);
        builder.build_bytes(&values);
        let idx = builder.finish();

        // 20000 / 8192 = 3 granules
        assert_eq!(idx.granule_count(), 3);

        // "00010000" 는 두 번째 그래뉼(8192..16383) 범위
        let candidates = idx.candidate_granules_eq(b"00010000").as_slice().to_vec();
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_range_pruning() {
        let values: Vec<Option<Bytes>> = vec![
            Some(b("a")), Some(b("b")), Some(b("c")),
            Some(b("x")), Some(b("y")), Some(b("z")),
        ];
        let mut builder = MinMaxBuilder::new(3); // granule_size=3
        builder.build_bytes(&values);
        let idx = builder.finish();
        assert_eq!(idx.granule_count(), 2);

        // 쿼리: col >= "m" (두 번째 그래뉼만)
        let candidates = idx.candidate_granules_range(Some(b"m"), None);
        assert_eq!(candidates, vec![1]);

        // 쿼리: col <= "c" (첫 번째 그래뉼만)
        let candidates = idx.candidate_granules_range(None, Some(b"c"));
        assert_eq!(candidates, vec![0]);
    }

    #[test]
    fn test_serialization() {
        let values: Vec<Option<Bytes>> = (0u32..100).map(|i| Some(Bytes::from(i.to_le_bytes().to_vec()))).collect();
        let mut builder = MinMaxBuilder::new(32);
        builder.build_bytes(&values);
        let idx = builder.finish();

        let bytes = idx.to_bytes().unwrap();
        let restored = MinMaxIndex::from_bytes(&bytes).unwrap();
        assert_eq!(restored.granule_count(), idx.granule_count());
    }
}
