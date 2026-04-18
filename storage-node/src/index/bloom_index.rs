// T041: per-Granule BLOOM_FILTER 인덱스

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::lsm::bloom::{GranuleBloomIndex, SsTableBloom}; // GranuleBloomIndex used below

pub const DEFAULT_GRANULE_SIZE: usize = 8192;

// ─── 그래뉼 Bloom 인덱스 (직렬화 포함) ──────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct GranuleBloomEntry {
    pub granule_idx: u32,
    pub row_offset:  u64,
    pub row_count:   u32,
    pub bloom_bytes: Vec<u8>,  // SsTableBloom 직렬화
}

pub struct BloomIndexBuilder {
    granule_size: usize,
}

impl BloomIndexBuilder {
    pub fn new(granule_size: usize) -> Self {
        Self { granule_size }
    }

    pub fn build(&self, values: &[Option<Bytes>]) -> anyhow::Result<BloomFilterIndex> {
        let mut entries = Vec::new();
        for (g_idx, chunk) in values.chunks(self.granule_size).enumerate() {
            let row_offset = (g_idx * self.granule_size) as u64;
            let non_null: Vec<&[u8]> = chunk.iter()
                .filter_map(|v| v.as_deref())
                .collect();

            let mut bf = SsTableBloom::with_defaults(non_null.len().max(1));
            for key in &non_null {
                bf.insert(key);
            }
            let bloom_bytes = bf.to_bytes()?.to_vec();

            entries.push(GranuleBloomEntry {
                granule_idx: g_idx as u32,
                row_offset,
                row_count:   chunk.len() as u32,
                bloom_bytes,
            });
        }
        Ok(BloomFilterIndex { entries, granule_size: self.granule_size })
    }
}

// ─── BloomFilterIndex ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct BloomFilterIndex {
    pub entries:      Vec<GranuleBloomEntry>,
    pub granule_size: usize,
}

impl BloomFilterIndex {
    /// key를 포함할 수 있는 granule 인덱스 목록 반환
    pub fn candidate_granules(&self, key: &[u8]) -> Vec<usize> {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(i, entry)| {
                let bf = SsTableBloom::from_bytes(&entry.bloom_bytes).ok()?;
                if bf.may_contain(key) { Some(i) } else { None }
            })
            .collect()
    }

    pub fn to_bytes(&self) -> anyhow::Result<Bytes> {
        Ok(Bytes::from(serde_json::to_vec(self)?))
    }

    pub fn from_bytes(data: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }

    pub fn granule_count(&self) -> usize {
        self.entries.len()
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_index_no_false_negative() {
        let values: Vec<Option<Bytes>> = (0u32..1000)
            .map(|i| Some(Bytes::from(format!("key_{:06}", i))))
            .collect();

        let builder = BloomIndexBuilder::new(256);
        let idx = builder.build(&values).unwrap();

        // 삽입된 키는 반드시 candidate에 포함
        for i in 0u32..1000 {
            let key = format!("key_{:06}", i);
            let candidates = idx.candidate_granules(key.as_bytes());
            assert!(!candidates.is_empty(), "key {} not found in any granule", key);
        }
    }

    #[test]
    fn test_bloom_index_serialization() {
        let values: Vec<Option<Bytes>> = vec![
            Some(Bytes::from_static(b"alpha")),
            Some(Bytes::from_static(b"beta")),
            None,
            Some(Bytes::from_static(b"gamma")),
        ];
        let builder = BloomIndexBuilder::new(4);
        let idx = builder.build(&values).unwrap();

        let bytes = idx.to_bytes().unwrap();
        let restored = BloomFilterIndex::from_bytes(&bytes).unwrap();

        assert_eq!(restored.granule_count(), idx.granule_count());
        assert!(!restored.candidate_granules(b"alpha").is_empty());
    }
}
