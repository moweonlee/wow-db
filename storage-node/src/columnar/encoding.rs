// T035: 컬럼 인코딩 — Dictionary, Delta, BitPacking, Plain

use bytes::{Bytes, BytesMut, BufMut};

// ─── 인코딩 타입 ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Plain,
    Dictionary,
    Delta,
    BitPacking,
}

// ─── Dictionary Encoding ─────────────────────────────────────────────────────

/// 저기수(NDV < 1000) 문자열 컬럼용 Dictionary Encoding
pub struct DictEncoder {
    dict:    Vec<Bytes>,        // 고유값 목록 (index → value)
    lookup:  std::collections::HashMap<Bytes, u32>, // value → index
    codes:   Vec<u32>,          // 코드 배열
}

impl DictEncoder {
    pub fn new() -> Self {
        Self { dict: Vec::new(), lookup: std::collections::HashMap::new(), codes: Vec::new() }
    }

    pub fn push(&mut self, value: Bytes) {
        let next_id = self.dict.len() as u32;
        let code = *self.lookup.entry(value.clone()).or_insert_with(|| {
            self.dict.push(value);
            next_id
        });
        self.codes.push(code);
    }

    /// (dict_bytes, codes_bytes) 두 파트로 직렬화
    pub fn finish(self) -> (Bytes, Bytes) {
        // dict: [n:4LE][len_i:4LE][bytes_i ...] ...
        let mut dict_buf = BytesMut::new();
        dict_buf.put_u32_le(self.dict.len() as u32);
        for entry in &self.dict {
            dict_buf.put_u32_le(entry.len() as u32);
            dict_buf.put_slice(entry);
        }

        // codes: [n:4LE][code_i:4LE ...]
        let mut codes_buf = BytesMut::new();
        codes_buf.put_u32_le(self.codes.len() as u32);
        for code in &self.codes {
            codes_buf.put_u32_le(*code);
        }

        (dict_buf.freeze(), codes_buf.freeze())
    }
}

pub struct DictDecoder;

impl DictDecoder {
    pub fn decode(dict_bytes: &[u8], codes_bytes: &[u8]) -> Vec<Bytes> {
        let mut pos = 0usize;

        // dict 파싱
        let n_dict = u32::from_le_bytes(dict_bytes[pos..pos+4].try_into().unwrap()) as usize;
        pos += 4;
        let mut dict = Vec::with_capacity(n_dict);
        for _ in 0..n_dict {
            let len = u32::from_le_bytes(dict_bytes[pos..pos+4].try_into().unwrap()) as usize;
            pos += 4;
            dict.push(Bytes::copy_from_slice(&dict_bytes[pos..pos+len]));
            pos += len;
        }

        // codes 파싱
        let mut cpos = 0usize;
        let n_codes = u32::from_le_bytes(codes_bytes[cpos..cpos+4].try_into().unwrap()) as usize;
        cpos += 4;
        let mut result = Vec::with_capacity(n_codes);
        for _ in 0..n_codes {
            let code = u32::from_le_bytes(codes_bytes[cpos..cpos+4].try_into().unwrap()) as usize;
            cpos += 4;
            result.push(dict[code].clone());
        }
        result
    }
}

// ─── Delta Encoding ──────────────────────────────────────────────────────────

/// 단조 증가 i64 시퀀스(event_time, sequence_id)용 Delta Encoding
pub fn delta_encode(values: &[i64]) -> Bytes {
    if values.is_empty() {
        return Bytes::new();
    }
    let mut buf = BytesMut::new();
    buf.put_i64_le(values[0]);              // 기준값
    for w in values.windows(2) {
        buf.put_i64_le(w[1] - w[0]);        // 차분값
    }
    buf.freeze()
}

pub fn delta_decode(data: &[u8]) -> Vec<i64> {
    if data.is_empty() {
        return Vec::new();
    }
    let n = data.len() / 8;
    let mut result = Vec::with_capacity(n);
    let base = i64::from_le_bytes(data[0..8].try_into().unwrap());
    result.push(base);
    let mut prev = base;
    for i in 1..n {
        let delta = i64::from_le_bytes(data[i*8..(i+1)*8].try_into().unwrap());
        prev += delta;
        result.push(prev);
    }
    result
}

// ─── BitPacking Encoding ──────────────────────────────────────────────────────

/// 정수 컬럼(session_event_count 등)용 BitPacking
/// 단순 구현: 값 범위에 따라 필요한 bit width 계산 후 compact packing
pub fn bitpack_encode(values: &[u32]) -> Bytes {
    if values.is_empty() {
        return Bytes::new();
    }
    let max_val = *values.iter().max().unwrap_or(&0);
    let bit_width = if max_val == 0 { 1u8 } else { (u32::BITS - max_val.leading_zeros()) as u8 };

    let mut buf = BytesMut::new();
    buf.put_u8(bit_width);
    buf.put_u32_le(values.len() as u32);

    // 단순 바이트 패킹 (bit_width <= 32)
    let mut bits: u64 = 0;
    let mut bits_used: u8 = 0;
    for &v in values {
        bits |= (v as u64) << bits_used;
        bits_used += bit_width;
        while bits_used >= 8 {
            buf.put_u8(bits as u8);
            bits >>= 8;
            bits_used -= 8;
        }
    }
    if bits_used > 0 {
        buf.put_u8(bits as u8);
    }
    buf.freeze()
}

pub fn bitpack_decode(data: &[u8]) -> Vec<u32> {
    if data.len() < 5 {
        return Vec::new();
    }
    let bit_width = data[0] as u64;
    let n = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
    let mask = if bit_width == 32 { u32::MAX as u64 } else { (1u64 << bit_width) - 1 };

    let mut result = Vec::with_capacity(n);
    let payload = &data[5..];
    let mut bits: u64 = 0;
    let mut bits_avail: u8 = 0;
    let mut byte_pos = 0usize;

    for _ in 0..n {
        while bits_avail < bit_width as u8 && byte_pos < payload.len() {
            bits |= (payload[byte_pos] as u64) << bits_avail;
            bits_avail += 8;
            byte_pos += 1;
        }
        result.push((bits & mask) as u32);
        bits >>= bit_width;
        bits_avail = bits_avail.saturating_sub(bit_width as u8);
    }
    result
}

// ─── Plain Encoding ───────────────────────────────────────────────────────────

/// Plain: raw bytes 연결 (길이 프리픽스 포함)
pub fn plain_encode(values: &[Bytes]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u32_le(values.len() as u32);
    for v in values {
        buf.put_u32_le(v.len() as u32);
        buf.put_slice(v);
    }
    buf.freeze()
}

pub fn plain_decode(data: &[u8]) -> Vec<Bytes> {
    let mut pos = 0usize;
    if data.len() < 4 { return Vec::new(); }
    let n = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap()) as usize;
    pos += 4;
    let mut result = Vec::with_capacity(n);
    for _ in 0..n {
        if pos + 4 > data.len() { break; }
        let len = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > data.len() { break; }
        result.push(Bytes::copy_from_slice(&data[pos..pos+len]));
        pos += len;
    }
    result
}

// ─── 인코딩 선택 로직 ─────────────────────────────────────────────────────────

/// NDV와 값 특성을 기반으로 최적 인코딩 선택
pub fn select_encoding(ndv: usize, total: usize, is_monotonic: bool) -> Encoding {
    if is_monotonic {
        Encoding::Delta
    } else if ndv > 0 && total / ndv >= 2 && ndv < 1000 {
        Encoding::Dictionary
    } else {
        Encoding::Plain
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dict_roundtrip() {
        let mut enc = DictEncoder::new();
        for v in &["page_view", "click", "page_view", "purchase", "click"] {
            enc.push(Bytes::from_static(v.as_bytes()));
        }
        let (dict_bytes, codes_bytes) = enc.finish();
        let decoded = DictDecoder::decode(&dict_bytes, &codes_bytes);
        assert_eq!(decoded.len(), 5);
        assert_eq!(decoded[0], Bytes::from_static(b"page_view"));
        assert_eq!(decoded[2], Bytes::from_static(b"page_view"));
    }

    #[test]
    fn test_delta_roundtrip() {
        let vals = vec![1000i64, 1001, 1003, 1010, 1020];
        let encoded = delta_encode(&vals);
        let decoded = delta_decode(&encoded);
        assert_eq!(decoded, vals);
    }

    #[test]
    fn test_bitpack_roundtrip() {
        let vals: Vec<u32> = (0..100).collect();
        let encoded = bitpack_encode(&vals);
        let decoded = bitpack_decode(&encoded);
        assert_eq!(decoded, vals);
    }

    #[test]
    fn test_plain_roundtrip() {
        let vals = vec![
            Bytes::from_static(b"hello"),
            Bytes::from_static(b"world"),
            Bytes::from_static(b""),
        ];
        let encoded = plain_encode(&vals);
        let decoded = plain_decode(&encoded);
        assert_eq!(decoded, vals);
    }

    #[test]
    fn test_select_encoding() {
        assert_eq!(select_encoding(0, 0, true),  Encoding::Delta);
        assert_eq!(select_encoding(5, 1000, false), Encoding::Dictionary);
        assert_eq!(select_encoding(900, 1000, false), Encoding::Plain);
    }
}
