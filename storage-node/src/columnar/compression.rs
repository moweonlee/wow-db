// T036: LZ4/ZSTD 압축/해제 래퍼

use anyhow::{anyhow, Result};
use bytes::Bytes;

// ─── 압축 알고리즘 ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Codec {
    #[default]
    None,
    Lz4,
    Zstd,
}

impl Codec {
    pub fn as_str(self) -> &'static str {
        match self {
            Codec::None => "none",
            Codec::Lz4  => "lz4",
            Codec::Zstd => "zstd",
        }
    }
}

// ─── 압축 ─────────────────────────────────────────────────────────────────────

/// 데이터를 지정 코덱으로 압축
pub fn compress(data: &[u8], codec: Codec) -> Result<Bytes> {
    match codec {
        Codec::None => Ok(Bytes::copy_from_slice(data)),
        Codec::Lz4  => {
            // lz4_flex: prepend_size 포함하여 원본 크기 복원 가능
            Ok(Bytes::from(lz4_flex::compress_prepend_size(data)))
        }
        Codec::Zstd => {
            let compressed = zstd::encode_all(data, 3)
                .map_err(|e| anyhow!("ZSTD compress: {}", e))?;
            Ok(Bytes::from(compressed))
        }
    }
}

/// 압축 해제
pub fn decompress(data: &[u8], codec: Codec) -> Result<Bytes> {
    match codec {
        Codec::None => Ok(Bytes::copy_from_slice(data)),
        Codec::Lz4  => {
            lz4_flex::decompress_size_prepended(data)
                .map(Bytes::from)
                .map_err(|e| anyhow!("LZ4 decompress: {}", e))
        }
        Codec::Zstd => {
            let decompressed = zstd::decode_all(data)
                .map_err(|e| anyhow!("ZSTD decompress: {}", e))?;
            Ok(Bytes::from(decompressed))
        }
    }
}

/// 파일 확장자에서 코덱 감지
pub fn codec_from_ext(path: &str) -> Codec {
    if path.ends_with(".lz4") { Codec::Lz4 }
    else if path.ends_with(".zst") || path.ends_with(".zstd") { Codec::Zstd }
    else { Codec::None }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(codec: Codec) {
        let original = b"hello world! this is a test of compression. ".repeat(100);
        let compressed   = compress(&original, codec).unwrap();
        let decompressed = decompress(&compressed, codec).unwrap();
        assert_eq!(decompressed, original);
        if codec != Codec::None {
            assert!(compressed.len() < original.len(), "should compress well for repetitive data");
        }
    }

    #[test] fn test_none()  { roundtrip(Codec::None); }
    #[test] fn test_lz4()   { roundtrip(Codec::Lz4);  }
    #[test] fn test_zstd()  { roundtrip(Codec::Zstd); }
}
