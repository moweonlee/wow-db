// T111/T112: SSTable — magic bytes "WOWDBCOL", 물리 파일 포맷 v1, sequence_num/generation/compacted_from
// FR-031: 파일 헤더 magic bytes + VERSION, 버전 불일치 시 거부
// 포맷: <partition_dir>/<column>/seg_NNNN.col + .bloom + .min_max

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{bail, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

use super::bloom::{BloomFilterConfig, SsTableBloom};
use super::memtable::ImmutableMemTable;

// ─── 파일 포맷 상수 (lsm-engine.md §13) ────────────────────────────────────

pub const COL_MAGIC: &[u8; 8]     = b"WOWDBCOL";
pub const BLOOM_MAGIC: &[u8; 8]   = b"WOWBLOOM";
pub const MIN_MAX_MAGIC: &[u8; 8] = b"WOWMINMX";
pub const FILE_FORMAT_VERSION: u16 = 1;

// ─── SSTable 메타데이터 ───────────────────────────────────────────────────────

/// 하나의 SSTable 세그먼트 메타데이터 (파티션 디렉토리 내 `_meta/sstable_<seq>.json`)
#[derive(Debug, Serialize, Deserialize)]
pub struct SSTableMeta {
    pub id:             Uuid,
    pub seq:            u64,
    /// 파티션 내 단조 증가 시퀀스 번호.
    /// 동일 Sort Key에 대해 이 값이 큰 SSTable이 최신 버전.
    pub sequence_num:   u64,
    /// Compaction 세대 번호 (0 = MemTable flush, N = N번째 compact 출력)
    pub generation:     u64,
    /// 이 SSTable 생성 시 입력된 SSTable ID 목록 (계보 추적)
    pub compacted_from: Vec<Uuid>,
    pub row_count:      u64,
    pub columns:        Vec<ColumnFileMeta>,
    pub min_sort_key:   Vec<u8>,
    pub max_sort_key:   Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnFileMeta {
    pub name:             String,
    pub col_file:         String,
    pub bloom_file:       Option<String>,
    pub min_max_file:     Option<String>,
    pub min_bytes:        Option<Vec<u8>>,
    pub max_bytes:        Option<Vec<u8>>,
    pub null_count:       u64,
    pub row_count:        u64,
    pub compressed_bytes: u64,
}

// ─── Min/Max 파일 ─────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
struct MinMaxRecord {
    min:        Vec<u8>,
    max:        Vec<u8>,
    null_count: u64,
    row_count:  u64,
}

// ─── 파일 포맷 헬퍼 ──────────────────────────────────────────────────────────

/// `.col` 파일 헤더 작성 (120 bytes)
/// "WOWDBCOL" (8) + VERSION (2) + FLAGS (2) + COLUMN_ID (4) + SST_SEQUENCE (8)
/// + ROW_COUNT (8) + GRANULE_COUNT (4) + reserved (84) = 120 bytes
fn write_col_header(
    buf:          &mut Vec<u8>,
    column_id:    u32,
    sst_sequence: u64,
    row_count:    u64,
    granule_count: u32,
) {
    buf.extend_from_slice(COL_MAGIC);
    buf.extend_from_slice(&FILE_FORMAT_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes()); // FLAGS (reserved)
    buf.extend_from_slice(&column_id.to_le_bytes());
    buf.extend_from_slice(&sst_sequence.to_le_bytes());
    buf.extend_from_slice(&row_count.to_le_bytes());
    buf.extend_from_slice(&granule_count.to_le_bytes());
    // reserved padding → total 120 bytes
    let current = buf.len();
    let target = 120;
    if current < target {
        buf.extend(std::iter::repeat(0u8).take(target - current));
    }
}

/// `.col` 파일 헤더에서 버전 읽기 + 검증
pub fn verify_col_magic(data: &[u8]) -> Result<()> {
    if data.len() < 10 {
        bail!("col 파일이 너무 짧음: {} bytes", data.len());
    }
    if &data[..8] != COL_MAGIC {
        bail!("col 파일 magic bytes 불일치: {:?}", &data[..8]);
    }
    let version = u16::from_le_bytes(data[8..10].try_into().unwrap());
    if version != FILE_FORMAT_VERSION {
        bail!(
            "col 파일 버전 불일치: 지원 버전={}, 파일 버전={}",
            FILE_FORMAT_VERSION,
            version
        );
    }
    Ok(())
}

/// `.min_max` 파일 헤더 작성 ("WOWMINMX" + VERSION + CRC32 + JSON payload)
fn write_min_max_file(min_max: &MinMaxRecord) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(min_max)?;
    let crc = crc32fast::hash(&payload);
    let mut out = Vec::with_capacity(MIN_MAX_MAGIC.len() + 2 + 4 + payload.len());
    out.extend_from_slice(MIN_MAX_MAGIC);
    out.extend_from_slice(&FILE_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

// ─── 파티션 시퀀스 카운터 ─────────────────────────────────────────────────────

/// 파티션당 하나의 단조 증가 SST sequence 카운터
pub struct PartitionSeqCounter(Arc<AtomicU64>);

impl PartitionSeqCounter {
    pub fn new(start: u64) -> Self {
        Self(Arc::new(AtomicU64::new(start)))
    }

    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst)
    }

    pub fn current(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

impl Clone for PartitionSeqCounter {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

// ─── SSTable Flusher ──────────────────────────────────────────────────────────

pub struct SsTableFlusher {
    partition_dir: PathBuf,
    bloom_config:  BloomFilterConfig,
}

impl SsTableFlusher {
    pub fn new(partition_dir: impl AsRef<Path>) -> Self {
        Self {
            partition_dir: partition_dir.as_ref().to_path_buf(),
            bloom_config:  BloomFilterConfig::default(),
        }
    }

    pub fn with_bloom_config(mut self, config: BloomFilterConfig) -> Self {
        self.bloom_config = config;
        self
    }

    /// ImmutableMemTable → 컬럼 파일 flush
    /// generation=0 (MemTable에서 직접 flush)
    pub async fn flush(
        &self,
        imm:         ImmutableMemTable,
        seq:         u64,
        sequence_num: u64,
    ) -> Result<SSTableMeta> {
        self.flush_with_lineage(imm, seq, sequence_num, 0, vec![]).await
    }

    /// Compaction 출력 flush (generation + lineage 포함)
    /// FR-029: Compaction 시 반드시 이 메서드로 새 bloom 생성
    pub async fn flush_with_lineage(
        &self,
        imm:           ImmutableMemTable,
        seq:           u64,
        sequence_num:  u64,
        generation:    u64,
        compacted_from: Vec<Uuid>,
    ) -> Result<SSTableMeta> {
        if imm.entries.is_empty() {
            bail!("flush: ImmutableMemTable is empty");
        }

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

        for (col_idx, (col_name, values)) in col_data.iter().enumerate() {
            let col_meta = self
                .flush_column(col_name, values, seq, sequence_num, col_idx as u32)
                .await?;
            col_metas.push(col_meta);
        }

        let id = Uuid::new_v4();
        let meta = SSTableMeta {
            id,
            seq,
            sequence_num,
            generation,
            compacted_from,
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
            sequence_num,
            generation,
            row_count,
            columns = meta.columns.len(),
            dir = %self.partition_dir.display(),
            "SSTable flush 완료"
        );
        Ok(meta)
    }

    async fn flush_column(
        &self,
        col_name:     &str,
        values:       &[Bytes],
        seq:          u64,
        sequence_num: u64,
        column_id:    u32,
    ) -> Result<ColumnFileMeta> {
        let col_dir = self.partition_dir.join(col_name);
        tokio::fs::create_dir_all(&col_dir).await?;

        let seg_stem = format!("seg-{:010}", seq);

        // ── .col 파일: 헤더(120B) + LZ4 압축 데이터 ──────────────────────────
        let raw: Vec<u8> = values.iter().flat_map(|b| b.iter().copied()).collect();
        let compressed = lz4_flex::compress_prepend_size(&raw);

        let granule_size = 8192usize;
        let granule_count = (values.len().saturating_sub(1) / granule_size + 1) as u32;

        let mut col_bytes = Vec::with_capacity(120 + compressed.len());
        write_col_header(&mut col_bytes, column_id, sequence_num, values.len() as u64, granule_count);
        col_bytes.extend_from_slice(&compressed);

        // CRC32 footer
        let crc = crc32fast::hash(&col_bytes);
        col_bytes.extend_from_slice(&crc.to_le_bytes());

        let col_rel  = format!("{}/seg-{:010}.col", col_name, seq);
        let col_path = self.partition_dir.join(&col_rel);
        tokio::fs::write(&col_path, &col_bytes).await?;

        // ── .bloom 파일 — FR-029: 출력 레코드 기반 새로 빌드 ────────────────
        let bloom_rel = format!("{}/{}.bloom", col_name, seg_stem);
        let mut bloom = SsTableBloom::new(values.len().max(1), self.bloom_config.clone());
        for v in values {
            bloom.insert(v.as_ref());
        }
        let bloom_bytes = bloom.to_bytes()?;
        let bloom_path = self.partition_dir.join(&bloom_rel);
        tokio::fs::write(&bloom_path, bloom_bytes.as_ref()).await?;

        // ── .min_max 파일 ("WOWMINMX" + VERSION + CRC32 + JSON) ──────────────
        let (min_bytes, max_bytes) = if values.is_empty() {
            (None, None)
        } else {
            let min = values.iter().min().map(|b| b.to_vec());
            let max = values.iter().max().map(|b| b.to_vec());
            (min, max)
        };

        let null_count = 0u64;
        let min_max = MinMaxRecord {
            min:        min_bytes.clone().unwrap_or_default(),
            max:        max_bytes.clone().unwrap_or_default(),
            null_count,
            row_count:  values.len() as u64,
        };
        let mm_bytes = write_min_max_file(&min_max)?;
        let mm_rel   = format!("{}/{}.min_max", col_name, seg_stem);
        let mm_path  = self.partition_dir.join(&mm_rel);
        tokio::fs::write(&mm_path, &mm_bytes).await?;

        Ok(ColumnFileMeta {
            name:             col_name.to_string(),
            col_file:         col_rel,
            bloom_file:       Some(bloom_rel),
            min_max_file:     Some(mm_rel),
            min_bytes,
            max_bytes,
            null_count,
            row_count:        values.len() as u64,
            compressed_bytes: col_bytes.len() as u64,
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
    async fn test_flush_creates_files_with_magic() {
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
        let meta = flusher.flush(imm, 1, 1).await.unwrap();

        assert_eq!(meta.seq, 1);
        assert_eq!(meta.sequence_num, 1);
        assert_eq!(meta.generation, 0);
        assert!(meta.compacted_from.is_empty());
        assert_eq!(meta.row_count, 5);

        // .col 파일 존재 + magic bytes 확인
        let col_path = tmp.path().join(&meta.columns[0].col_file);
        assert!(col_path.exists());
        let col_data = tokio::fs::read(&col_path).await.unwrap();
        assert!(verify_col_magic(&col_data).is_ok(), "WOWDBCOL magic 확인 실패");
    }

    #[tokio::test]
    async fn test_flush_with_lineage() {
        let tmp     = TempDir::new().unwrap();
        let flusher = SsTableFlusher::new(tmp.path());

        let mt = MemTable::new(DEFAULT_MEMTABLE_THRESHOLD);
        mt.insert(make_key(0), vec![("col".into(), Bytes::from("v"))], 1);
        let imm = mt.freeze();

        let parent_id = Uuid::new_v4();
        let meta = flusher
            .flush_with_lineage(imm, 2, 5, 1, vec![parent_id])
            .await
            .unwrap();

        assert_eq!(meta.generation, 1);
        assert_eq!(meta.compacted_from, vec![parent_id]);
        assert_eq!(meta.sequence_num, 5);
    }

    #[test]
    fn test_version_mismatch_rejected() {
        // 잘못된 magic
        let mut bad = b"BADMAGIC".to_vec();
        bad.extend_from_slice(&1u16.to_le_bytes());
        assert!(verify_col_magic(&bad).is_err());

        // 잘못된 버전
        let mut wrong_ver = b"WOWDBCOL".to_vec();
        wrong_ver.extend_from_slice(&9999u16.to_le_bytes());
        assert!(verify_col_magic(&wrong_ver).is_err());
    }
}
