// T113: BloomFilterConfig — xxHash3 / 10 bits-per-key (FPR 1%), Compaction 시 반드시 재생성
// FR-029: Compaction 출력 레코드 기반으로 새로 빌드 (입력 SSTable bloom 재사용 금지)

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_128;

// ─── 해시 함수 선택 ───────────────────────────────────────────────────────────

/// Bloom Filter 해시 함수 종류
/// 기본: xxHash3 (SIMD-accelerated, ~60 GB/s)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BloomHashFn {
    /// xxHash3 128-bit (권장 — SIMD 가속, AVX2 지원)
    XxHash3,
    /// MurmurHash3 128-bit (레거시 호환용, SIMD 미지원)
    MurmurHash3,
}

impl Default for BloomHashFn {
    fn default() -> Self {
        Self::XxHash3
    }
}

// ─── BloomFilter 설정 ─────────────────────────────────────────────────────────

/// Bloom Filter 파라미터 설정
/// `lsm-engine.md §7` 기준
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilterConfig {
    /// 키당 비트 수 (기본 10 → FPR ≈ 1%)
    /// 고선택도 컬럼에는 14 사용 (FPR ≈ 0.1%)
    pub bits_per_key: u32,
    /// 기대 FPR (참고용, bits_per_key와 정합 확인용)
    pub false_positive_rate: f64,
    /// 해시 함수 (기본 xxHash3)
    pub hash_fn: BloomHashFn,
}

impl Default for BloomFilterConfig {
    fn default() -> Self {
        Self {
            bits_per_key:        10,
            false_positive_rate: 0.01, // 1%
            hash_fn:             BloomHashFn::XxHash3,
        }
    }
}

impl BloomFilterConfig {
    /// 고선택도 컬럼 설정 (FPR 0.1%, 14 bits/key)
    pub fn high_selectivity() -> Self {
        Self {
            bits_per_key:        14,
            false_positive_rate: 0.001,
            hash_fn:             BloomHashFn::XxHash3,
        }
    }

    /// bits_per_key로부터 최적 해시 함수 수 k 계산
    /// k = bits_per_key × ln(2) ≈ bits_per_key × 0.693
    pub fn optimal_k(&self) -> u32 {
        ((self.bits_per_key as f64 * std::f64::consts::LN_2).round() as u32).max(1)
    }
}

// ─── 내부 비트맵 ──────────────────────────────────────────────────────────────

struct BitVec {
    bits: Vec<u8>,
    num_bits: usize,
}

impl BitVec {
    fn with_capacity(num_bits: usize) -> Self {
        let bytes = (num_bits + 7) / 8;
        Self { bits: vec![0u8; bytes], num_bits }
    }

    fn set(&mut self, bit: usize) {
        let bit = bit % self.num_bits;
        self.bits[bit / 8] |= 1 << (bit % 8);
    }

    fn get(&self, bit: usize) -> bool {
        let bit = bit % self.num_bits;
        (self.bits[bit / 8] >> (bit % 8)) & 1 == 1
    }
}

// ─── 해시 계산 헬퍼 ─────────────────────────────────────────────────────────

/// xxHash3 기반으로 k개의 독립 해시 값 생성 (double-hashing 기법)
fn xxh3_hashes(key: &[u8], k: u32, num_bits: usize) -> impl Iterator<Item = usize> + '_ {
    let h128 = xxh3_128(key);
    let h1 = (h128 & 0xFFFF_FFFF_FFFF_FFFF) as u64;
    let h2 = (h128 >> 64) as u64;
    (0..k).map(move |i| {
        let h = h1.wrapping_add(h2.wrapping_mul(i as u64 + 1));
        (h as usize) % num_bits
    })
}

/// MurmurHash3 기반 (레거시, FNV-1a로 대체 구현)
fn murmur3_hashes(key: &[u8], k: u32, num_bits: usize) -> impl Iterator<Item = usize> + '_ {
    // 간단한 FNV-1a 기반 double hashing (murmur3 대체)
    let mut h1: u64 = 0xcbf2_9ce4_8422_2325;
    let mut h2: u64 = 0x517c_c1b7_2722_0a95;
    for &b in key {
        h1 = h1.wrapping_mul(0x100000001b3).wrapping_add(b as u64);
        h2 = h2.rotate_left(5).wrapping_add(b as u64).wrapping_add(0x5555555555555555);
    }
    (0..k).map(move |i| {
        let h = h1.wrapping_add(h2.wrapping_mul(i as u64 + 1));
        (h as usize) % num_bits
    })
}

// ─── SSTable Bloom Filter ─────────────────────────────────────────────────────

/// SSTable 세그먼트당 하나의 Bloom Filter
/// Sort Key 기반 — Compaction 시 key 존재 확인 + Point lookup 최적화
/// **반드시 Compaction 출력 레코드로 새로 빌드할 것 (FR-029)**
pub struct SsTableBloom {
    bitmap: BitVec,
    k:      u32,
    config: BloomFilterConfig,
}

impl SsTableBloom {
    /// Bloom Filter 생성 (expected_items 기준으로 비트맵 크기 결정)
    pub fn new(expected_items: usize, config: BloomFilterConfig) -> Self {
        let n = expected_items.max(1);
        let num_bits = (n * config.bits_per_key as usize).max(64);
        // 8의 배수 정렬
        let num_bits = (num_bits + 7) & !7;
        let k = config.optimal_k();
        Self { bitmap: BitVec::with_capacity(num_bits), k, config }
    }

    /// 기본 설정 (10 bits/key, xxHash3)으로 생성
    pub fn with_defaults(expected_items: usize) -> Self {
        Self::new(expected_items, BloomFilterConfig::default())
    }

    /// 키 추가
    pub fn insert(&mut self, key: &[u8]) {
        let indices = self.hash_indices(key);
        for idx in indices {
            self.bitmap.set(idx);
        }
    }

    /// 키 존재 여부 (false positive 가능, false negative 불가)
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.hash_indices(key).iter().all(|&idx| self.bitmap.get(idx))
    }

    /// 직렬화 (`.bloom` 파일 포맷 — `lsm-engine.md §13`)
    /// 헤더: "WOWBLOOM" (8B) + VERSION (2B) + BITS_PER_KEY (2B) + HASH_FN (1B) + K (1B) + NUM_BITS (8B) + CRC32 (4B)
    pub fn to_bytes(&self) -> anyhow::Result<Bytes> {
        let serialized = BloomSerialized {
            bits_per_key: self.config.bits_per_key,
            hash_fn:      self.config.hash_fn,
            k:            self.k,
            num_bits:     self.bitmap.num_bits as u64,
            bitmap:       self.bitmap.bits.clone(),
        };
        let data = serde_json::to_vec(&serialized)?;

        let magic = b"WOWBLOOM";
        let version: u16 = 1;
        let crc = crc32fast::hash(&data);

        let mut out = Vec::with_capacity(magic.len() + 2 + 4 + data.len());
        out.extend_from_slice(magic);
        out.extend_from_slice(&version.to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&data);
        Ok(Bytes::from(out))
    }

    /// 역직렬화
    pub fn from_bytes(raw: &[u8]) -> anyhow::Result<Self> {
        if raw.len() < 14 {
            anyhow::bail!("bloom 파일이 너무 짧음: {} bytes", raw.len());
        }
        if &raw[..8] != b"WOWBLOOM" {
            anyhow::bail!("bloom 파일 magic bytes 불일치");
        }
        let _version = u16::from_le_bytes(raw[8..10].try_into().unwrap());
        let stored_crc = u32::from_le_bytes(raw[10..14].try_into().unwrap());
        let data = &raw[14..];
        let computed_crc = crc32fast::hash(data);
        if stored_crc != computed_crc {
            anyhow::bail!("bloom 파일 CRC32 불일치: stored={stored_crc} computed={computed_crc}");
        }

        let s: BloomSerialized = serde_json::from_slice(data)?;
        let config = BloomFilterConfig {
            bits_per_key:        s.bits_per_key,
            false_positive_rate: 0.0, // 복원 시 rate는 참고용
            hash_fn:             s.hash_fn,
        };
        let num_bits = s.num_bits as usize;
        let bitmap = BitVec { bits: s.bitmap, num_bits };
        Ok(Self { bitmap, k: s.k, config })
    }

    fn hash_indices(&self, key: &[u8]) -> Vec<usize> {
        let num_bits = self.bitmap.num_bits;
        match self.config.hash_fn {
            BloomHashFn::XxHash3 => {
                xxh3_hashes(key, self.k, num_bits).collect()
            }
            BloomHashFn::MurmurHash3 => {
                murmur3_hashes(key, self.k, num_bits).collect()
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct BloomSerialized {
    bits_per_key: u32,
    hash_fn:      BloomHashFn,
    k:            u32,
    num_bits:     u64,
    bitmap:       Vec<u8>,
}

// ─── Granule-level Bloom Index ────────────────────────────────────────────────

/// 여러 Granule의 Bloom Filter 집합 (Data Skipping Index — BLOOM_FILTER 타입)
pub struct GranuleBloomIndex {
    granule_size: usize,
    filters:      Vec<SsTableBloom>,
    config:       BloomFilterConfig,
}

impl GranuleBloomIndex {
    pub fn new(granule_size: usize) -> Self {
        Self::with_config(granule_size, BloomFilterConfig::default())
    }

    pub fn with_config(granule_size: usize, config: BloomFilterConfig) -> Self {
        Self { granule_size, filters: Vec::new(), config }
    }

    /// 값 배열을 granule 단위로 나누어 Bloom Filter 구축
    /// NOTE: Compaction 시에도 반드시 이 메서드로 새로 빌드할 것 (FR-029)
    pub fn build(&mut self, keys: &[Bytes]) {
        self.filters.clear();
        for chunk in keys.chunks(self.granule_size) {
            let mut bf = SsTableBloom::new(chunk.len(), self.config.clone());
            for key in chunk {
                bf.insert(key.as_ref());
            }
            self.filters.push(bf);
        }
    }

    /// 쿼리 key가 포함될 수 있는 granule 인덱스 반환
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
    fn test_no_false_negative_xxhash3() {
        let mut bf = SsTableBloom::with_defaults(1000);
        let keys: Vec<Vec<u8>> = (0u32..500).map(|i| i.to_le_bytes().to_vec()).collect();
        for k in &keys {
            bf.insert(k);
        }
        for k in &keys {
            assert!(bf.may_contain(k), "false negative must be impossible");
        }
    }

    #[test]
    fn test_fpr_within_bound() {
        // 1000개 삽입 후 존재하지 않는 1000개 key에 대해 FPR ≤ 5% 확인
        let mut bf = SsTableBloom::with_defaults(1000);
        for i in 0u32..1000 {
            bf.insert(&i.to_le_bytes());
        }
        let mut fp = 0usize;
        for i in 1000u32..2000 {
            if bf.may_contain(&i.to_le_bytes()) {
                fp += 1;
            }
        }
        let fpr = fp as f64 / 1000.0;
        assert!(fpr <= 0.05, "FPR {:.2}% exceeds 5% bound", fpr * 100.0);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut bf = SsTableBloom::with_defaults(100);
        bf.insert(b"sort_key_1");
        bf.insert(b"sort_key_2");

        let bytes = bf.to_bytes().unwrap();
        // magic bytes 확인
        assert_eq!(&bytes[..8], b"WOWBLOOM");

        let restored = SsTableBloom::from_bytes(&bytes).unwrap();
        assert!(restored.may_contain(b"sort_key_1"));
        assert!(restored.may_contain(b"sort_key_2"));
        assert!(!restored.may_contain(b"nonexistent_key_xyz_12345_unique"));
    }

    #[test]
    fn test_crc_corruption_detected() {
        let mut bf = SsTableBloom::with_defaults(100);
        bf.insert(b"hello");
        let mut bytes = bf.to_bytes().unwrap().to_vec();
        // CRC 영역(bytes[10..14]) 손상
        bytes[10] ^= 0xFF;
        assert!(SsTableBloom::from_bytes(&bytes).is_err());
    }

    #[test]
    fn test_granule_index_no_false_negative() {
        let keys: Vec<Bytes> = (0u32..20000)
            .map(|i| Bytes::from(i.to_le_bytes().to_vec()))
            .collect();
        let mut idx = GranuleBloomIndex::new(8192);
        idx.build(&keys);
        assert!(idx.granule_count() >= 2);

        let target = Bytes::from(1234u32.to_le_bytes().to_vec());
        let candidates = idx.candidate_granules(&target);
        assert!(!candidates.is_empty(), "삽입된 키는 반드시 candidate granule에 포함");
    }

    #[test]
    fn test_high_selectivity_config() {
        let cfg = BloomFilterConfig::high_selectivity();
        assert_eq!(cfg.bits_per_key, 14);
        let mut bf = SsTableBloom::new(1000, cfg);
        for i in 0u32..1000 {
            bf.insert(&i.to_le_bytes());
        }
        // FPR ≤ 2%
        let mut fp = 0usize;
        for i in 1000u32..2000 {
            if bf.may_contain(&i.to_le_bytes()) {
                fp += 1;
            }
        }
        let fpr = fp as f64 / 1000.0;
        assert!(fpr <= 0.02, "high-selectivity FPR {:.2}% exceeds 2%", fpr * 100.0);
    }
}
