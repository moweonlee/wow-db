// T025: SSTable — ImmutableMemTable → 컬럼별 독립 파일 flush
// 포맷: <partition_dir>/<column>/seg_NNNN.col + .bloom + .min_max

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use bloomfilter::Bloom;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::memtable::ImmutableMemTable;

// ─── SSTable 메타데이터 ───────────────────────────────────────────────────────

/// 하나의 SSTable 세그먼트 메타데이터 (파티션 디렉토리 내 `_meta/sstable_<seq>.json`)
#[derive(Debug, Serialize, Deserialize)]
pub struct SSTableMeta {
    pub seq:          u64,
    pub row_count:    u64,
    pub columns:      Vec<ColumnFileMeta>,
    pub min_sort_key: Vec<u8>,
    pub max_sort_key: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnFileMeta {
    pub name:      String,
    pub col_file:  String,   // 상대 경로
    pub bloom_file: Option<String>,
    pub min_max_file: Option<String>,
    pub min_bytes: Option<Vec<u8>>,
    pub max_bytes: Option<Vec<u8>>,
    pub null_count: u64,
    pub row_count:  u64,
    pub compressed_bytes: u64,
}

// ─── Min/Max 파일 ─────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
struct MinMaxRecord {
    min: Vec<u8>,
    max: Vec<u8>,
    null_count: u64,
    row_count:  u64,
}

// ─── SSTable Flusher ──────────────────────────────────────────────────────────

pub struct SsTableFlusher {
    partition_dir: PathBuf,
}

impl SsTableFlusher {
    pub fn new(partition_dir: impl AsRef<Path>) -> Self {
        Self { partition_dir: partition_dir.as_ref().to_path_buf() }
    }

    /// ImmutableMemTable → 컬럼 파일로 flush
    pub async fn flush(&self, imm: ImmutableMemTable, seq: u64) -> Result<SSTableMeta> {
        if imm.entries.is_empty() {
            anyhow::bail!("flush: ImmutableMemTable is empty");
        }

        // 컬럼별 데이터 수집
        let mut col_data: HashMap<String, Vec<Bytes>> = HashMap::new();
        let mut sort_key_min = imm.entries[0].0.sort_key.values[0].encoded.clone();
        let mut sort_key_max = sort_key_min.clone();

        for (key, row) in &imm.entries {
            if !row.deleted {
                for (col_name, val_bytes) in &row.columns {
                    col_data.entry(col_name.clone()).or_default().push(val_bytes.clone());
                }
            }
            let encoded = &key.sort_key.values[0].encoded;
            if encoded < &sort_key_min { sort_key_min = encoded.clone(); }
            if encoded > &sort_key_max { sort_key_max = encoded.clone(); }
        }

        let row_count = imm.entries.iter().filter(|(_, r)| !r.deleted).count() as u64;
        let mut col_metas = Vec::new();

        for (col_name, values) in &col_data {
            let col_meta = self.flush_column(col_name, values, seq).await?;
            col_metas.push(col_meta);
        }

        let meta = SSTableMeta {
            seq,
            row_count,
            columns: col_metas,
            min_sort_key: sort_key_min,
            max_sort_key: sort_key_max,
        };

        // 메타 파일 기록
        let meta_dir = self.partition_dir.join("_meta");
        tokio::fs::create_dir_all(&meta_dir).await?;
        let meta_path = meta_dir.join(format!("sstable-{:010}.json", seq));
        let meta_json = serde_json::to_vec_pretty(&meta)?;
        tokio::fs::write(&meta_path, meta_json).await?;

        info!(
            seq,
            row_count,
            columns = meta.columns.len(),
            dir = %self.partition_dir.display(),
            "SSTable flush 완료"
        );
        Ok(meta)
    }

    async fn flush_column(
        &self,
        col_name: &str,
        values:   &[Bytes],
        seq:      u64,
    ) -> Result<ColumnFileMeta> {
        let col_dir = self.partition_dir.join(col_name);
        tokio::fs::create_dir_all(&col_dir).await?;

        let seg_stem = format!("seg-{:010}", seq);

        // ── .col 파일: 원시 바이트 연결 + LZ4 압축 ─────────────────────────
        let raw: Vec<u8> = values.iter().flat_map(|b| b.iter().copied()).collect();
        let compressed  = lz4_flex::compress_prepend_size(&raw);
        let col_rel     = format!("{}/seg-{:010}.col", col_name, seq);
        let col_path    = self.partition_dir.join(&col_rel);
        tokio::fs::write(&col_path, &compressed).await?;

        // ── .bloom 파일 ───────────────────────────────────────────────────
        let bloom_rel = format!("{}/{}.bloom", col_name, seg_stem);
        let mut bloom: Bloom<&[u8]> = Bloom::new_for_fp_rate(values.len().max(1), 0.001);
        for v in values {
            let slice: &[u8] = v.as_ref();
            bloom.set(&slice);
        }
        let bloom_bytes = serde_json::to_vec(&bloom.bitmap())?;
        let bloom_path  = self.partition_dir.join(&bloom_rel);
        tokio::fs::write(&bloom_path, &bloom_bytes).await?;

        // ── .min_max 파일 ─────────────────────────────────────────────────
        let (min_bytes, max_bytes) = if values.is_empty() {
            (None, None)
        } else {
            let min = values.iter().min().map(|b| b.to_vec());
            let max = values.iter().max().map(|b| b.to_vec());
            (min, max)
        };

        let null_count = 0u64; // 이 단계에서는 null 카운트 미지원 (Phase C에서 확장)

        let min_max = MinMaxRecord {
            min:        min_bytes.clone().unwrap_or_default(),
            max:        max_bytes.clone().unwrap_or_default(),
            null_count,
            row_count:  values.len() as u64,
        };
        let mm_rel  = format!("{}/{}.min_max", col_name, seg_stem);
        let mm_path = self.partition_dir.join(&mm_rel);
        tokio::fs::write(&mm_path, serde_json::to_vec(&min_max)?).await?;

        Ok(ColumnFileMeta {
            name:             col_name.to_string(),
            col_file:         col_rel,
            bloom_file:       Some(bloom_rel),
            min_max_file:     Some(mm_rel),
            min_bytes,
            max_bytes,
            null_count,
            row_count:        values.len() as u64,
            compressed_bytes: compressed.len() as u64,
        })
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsm::memtable::{MemTable, DEFAULT_MEMTABLE_THRESHOLD};
    use shared::types::{SortKey, SortKeyComponent};
    use tempfile::TempDir;

    fn make_key(v: u64) -> SortKey {
        SortKey::new(vec![SortKeyComponent {
            column:  "ts".into(),
            encoded: v.to_be_bytes().to_vec(),
        }])
    }

    #[tokio::test]
    async fn test_flush_creates_files() {
        let tmp     = TempDir::new().unwrap();
        let flusher = SsTableFlusher::new(tmp.path());

        let mt = MemTable::new(DEFAULT_MEMTABLE_THRESHOLD);
        for i in 0u64..5 {
            mt.insert(
                make_key(i),
                vec![("event_name".into(), Bytes::from(format!("click-{i}")))],
                1,
            );
        }
        let imm  = mt.freeze();
        let meta = flusher.flush(imm, 1).await.unwrap();

        assert_eq!(meta.row_count, 5);
        assert!(!meta.columns.is_empty());

        // .col 파일 존재 확인
        let col_path = tmp.path().join(&meta.columns[0].col_file);
        assert!(col_path.exists(), "col file must exist: {:?}", col_path);
    }
}
