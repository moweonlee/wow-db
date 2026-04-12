// T037: per-SSTable Bloom Filter — false positive rate 0.1%, Sort Key point lookup

use bloomfilter::Bloom;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

// ─── SSTable Bloom Filter ─────────────────────────────────────────────────────

/// SSTable 세그먼트당 하나의 Bloom Filter
/// Sort Key 기반 point lookup 최적화 (존재하지 않는 키 조기 제거)
pub struct SsTableBloom {
    bloom: Bloom<[u8]>,
}

impl SsTableBloom {
    const FALSE_POSITIVE_RATE: f64 = 0.001; // 0.1%

    pub fn new(expected_items: usize) -> Self {
        let n = expected_items.max(1);
        Self { bloom: Bloom::new_for_fp_rate(n, Self::FALSE_POSITIVE_RATE) }
    }

    /// 키 추가
    pub fn insert(&mut self, key: &[u8]) {
        self.bloom.set(key);
    }

    /// 키 존재 여부 확인 (false positive 가능, false negative 불가)
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom.check(key)
    }

    /// 직렬화 (비트맵 바이트 배열)
    pub fn to_bytes(&self) -> anyhow::Result<Bytes> {
        let serialized = BloomSerialized {
            bitmap:     self.bloom.bitmap(),
            k_num:      self.bloom.number_of_hash_functions(),
            num_bits:   self.bloom.number_of_bits() as u64,
        };
        let bytes = serde_json::to_vec(&serialized)?;
        Ok(Bytes::from(bytes))
    }

    /// 역직렬화
    pub fn from_bytes(data: &[u8]) -> anyhow::Result<Self> {
        let serialized: BloomSerialized = serde_json::from_slice(data)?;
        let bloom = Bloom::from_existing(
            &serialized.bitmap,
            serialized.num_bits,
            serialized.k_num,
            [(0, 0), (1, 1)],   // seed (ignored by from_existing)
        );
        Ok(Self { bloom })
    }
}

#[derive(Serialize, Deserialize)]
struct BloomSerialized {
    bitmap:   Vec<u8>,
    k_num:    u32,
    num_bits: u64,
}

// ─── Granule-level Bloom Index ────────────────────────────────────────────────

/// 여러 Granule의 Bloom Filter 집합 (Data Skipping Index용)
pub struct GranuleBloomIndex {
    granule_size: usize,            // 그래뉼당 행 수 (기본 8,192)
    filters:      Vec<SsTableBloom>,
}

impl GranuleBloomIndex {
    pub fn new(granule_size: usize) -> Self {
        Self { granule_size, filters: Vec::new() }
    }

    /// 값 배열을 그래뉼 단위로 나누어 Bloom Filter 구축
    pub fn build(&mut self, keys: &[Bytes]) {
        self.filters.clear();
        for chunk in keys.chunks(self.granule_size) {
            let mut bf = SsTableBloom::new(chunk.len());
            for key in chunk {
                bf.insert(key.as_ref());
            }
            self.filters.push(bf);
        }
    }

    /// 쿼리 key가 속할 수 있는 granule 인덱스 목록 반환
    pub fn candidate_granules(&self, key: &[u8]) -> Vec<usize> {
        self.filters
            .iter()
            .enumerate()
            .filter_map(|(i, bf)| if bf.may_contain(key) { Some(i) } else { None })
            .collect()
    }

    pub fn granule_count(&self) -> usize {
        self.filters.len()
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_no_false_negative() {
        let mut bf = SsTableBloom::new(1000);
        let keys: Vec<Vec<u8>> = (0u32..500).map(|i| i.to_le_bytes().to_vec()).collect();
        for k in &keys {
            bf.insert(k);
        }
        for k in &keys {
            assert!(bf.may_contain(k), "false negative is impossible");
        }
    }

    #[test]
    fn test_bloom_serialization() {
        let mut bf = SsTableBloom::new(100);
        bf.insert(b"sort_key_1");
        bf.insert(b"sort_key_2");

        let bytes = bf.to_bytes().unwrap();
        let restored = SsTableBloom::from_bytes(&bytes).unwrap();

        assert!(restored.may_contain(b"sort_key_1"));
        assert!(restored.may_contain(b"sort_key_2"));
        assert!(!restored.may_contain(b"nonexistent_key_xyz_12345_unique"));
    }

    #[test]
    fn test_granule_index() {
        let keys: Vec<Bytes> = (0u32..20000)
            .map(|i| Bytes::from(i.to_le_bytes().to_vec()))
            .collect();
        let mut idx = GranuleBloomIndex::new(8192);
        idx.build(&keys);

        // 20000 / 8192 = 3 granules (ceil)
        assert!(idx.granule_count() >= 2);

        // 삽입된 키는 반드시 후보 granule에 포함
        let target = Bytes::from(1234u32.to_le_bytes().to_vec());
        let candidates = idx.candidate_granules(&target);
        assert!(!candidates.is_empty());
    }
}
