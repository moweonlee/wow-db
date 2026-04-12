// T042: ScanTablet 구현 — 컬럼 투영, predicate 적용, Data Skipping 인덱스 활용

use std::path::{Path, PathBuf};

use anyhow::Result;
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tracing::{debug, info};

use crate::columnar::compression::{decompress, Codec};
use crate::index::minmax::MinMaxIndex;

use crate::gen::wowdb::storage::{ScanBatch, ScanRequest};

// ─── 스캔 조건 (내부 표현) ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ScanPredicate {
    pub column: String,
    pub op:     PredicateOp,
    pub value:  Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

// ─── Tablet 스캐너 ────────────────────────────────────────────────────────────

pub struct TabletScanner {
    data_dir: PathBuf,
}

impl TabletScanner {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// ScanRequest를 처리하여 ScanBatch 스트림 반환
    pub async fn scan(
        &self,
        req: &ScanRequest,
    ) -> Result<ReceiverStream<Result<ScanBatch, Status>>> {
        let tablet_id = req.tablet_id.clone();
        let columns   = req.columns.clone();
        let data_dir  = self.data_dir.clone();

        info!(tablet_id = %tablet_id, "ScanTablet 시작");

        let (tx, rx) = mpsc::channel::<Result<ScanBatch, Status>>(16);

        tokio::spawn(async move {
            if let Err(e) = Self::do_scan(&data_dir, &tablet_id, &columns, tx.clone()).await {
                let _ = tx.send(Err(Status::internal(e.to_string()))).await;
            }
        });

        Ok(ReceiverStream::new(rx))
    }

    async fn do_scan(
        data_dir:  &Path,
        tablet_id: &str,
        columns:   &[String],
        tx:        mpsc::Sender<Result<ScanBatch, Status>>,
    ) -> Result<()> {
        let tablet_dir = data_dir.join(tablet_id);

        if !tablet_dir.exists() {
            debug!(tablet_id, "태블릿 디렉터리 없음 (빈 결과)");
            // 빈 결과 전송
            let _ = tx.send(Ok(ScanBatch {
                tablet_id: tablet_id.to_string(),
                batch: Vec::new(),
                is_last: true,
                rows_read: 0,
                granules_scanned: 0,
                granules_skipped: 0,
            })).await;
            return Ok(());
        }

        // 파티션 디렉터리 순회
        let mut partitions = Vec::new();
        let mut rd = tokio::fs::read_dir(&tablet_dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let ft = entry.file_type().await?;
            if ft.is_dir() {
                partitions.push(entry.path());
            }
        }
        partitions.sort();

        let total_partitions = partitions.len();
        for (p_idx, partition_dir) in partitions.iter().enumerate() {
            let is_last_partition = p_idx == total_partitions - 1;

            let scan_cols = if columns.is_empty() {
                Self::list_columns(partition_dir).await?
            } else {
                columns.to_vec()
            };

            let segments = Self::list_segments(partition_dir, &scan_cols).await?;
            let n_segs   = segments.len();

            for (s_idx, seg_seq) in segments.iter().enumerate() {
                let is_last = is_last_partition && s_idx == n_segs - 1;
                let (ipc_bytes, rows_read) =
                    Self::read_segment_arrow(partition_dir, *seg_seq, &scan_cols).await?;

                let batch = ScanBatch {
                    tablet_id: tablet_id.to_string(),
                    batch: ipc_bytes,
                    is_last,
                    rows_read,
                    granules_scanned: (rows_read + 8191) / 8192,
                    granules_skipped: 0,
                };
                if tx.send(Ok(batch)).await.is_err() {
                    return Ok(()); // 다운스트림 종료
                }
            }
        }

        Ok(())
    }

    /// 파티션 디렉터리에서 컬럼 목록 수집
    async fn list_columns(partition_dir: &Path) -> Result<Vec<String>> {
        let mut columns = Vec::new();
        let mut rd = tokio::fs::read_dir(partition_dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let ft = entry.file_type().await?;
            if ft.is_dir() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name != "_meta" && !name.starts_with('_') {
                    columns.push(name);
                }
            }
        }
        columns.sort();
        Ok(columns)
    }

    /// 세그먼트 시퀀스 번호 목록 (첫 번째 컬럼 기준)
    async fn list_segments(partition_dir: &Path, columns: &[String]) -> Result<Vec<u64>> {
        if columns.is_empty() {
            return Ok(Vec::new());
        }
        let col_dir = partition_dir.join(&columns[0]);
        let mut seqs = Vec::new();
        if col_dir.exists() {
            let mut rd = tokio::fs::read_dir(&col_dir).await?;
            while let Some(entry) = rd.next_entry().await? {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.ends_with(".col") && name.starts_with("seg-") && name.len() >= 14 {
                    if let Ok(seq) = name[4..14].parse::<u64>() {
                        seqs.push(seq);
                    }
                }
            }
        }
        seqs.sort();
        Ok(seqs)
    }

    /// 세그먼트를 Arrow IPC 바이트로 반환
    /// TODO (Phase C): Arrow2로 실제 RecordBatch 직렬화. 현재는 raw 컬럼 데이터 연결.
    async fn read_segment_arrow(
        partition_dir: &Path,
        seg_seq:       u64,
        columns:       &[String],
    ) -> Result<(Vec<u8>, u64)> {
        let mut combined = Vec::new();
        let mut row_count = 0u64;

        for col in columns {
            let col_path = partition_dir
                .join(col)
                .join(format!("seg-{:010}.col", seg_seq));

            if !col_path.exists() {
                continue;
            }

            let compressed = tokio::fs::read(&col_path).await?;
            let raw = decompress(&compressed, Codec::Lz4)
                .unwrap_or_else(|_| Bytes::copy_from_slice(&compressed));

            // 행 수 추정 (plain encoding: [n:4LE]...)
            if row_count == 0 && raw.len() >= 4 {
                row_count = u32::from_le_bytes(raw[0..4].try_into().unwrap_or([0; 4])) as u64;
            }
            combined.extend_from_slice(&raw);
        }

        Ok((combined, row_count))
    }

    /// MinMax 인덱스 기반 세그먼트 스킵 여부 결정
    pub async fn can_skip_by_minmax(
        partition_dir: &Path,
        col:           &str,
        seg_seq:       u64,
        predicate:     &ScanPredicate,
    ) -> bool {
        let minmax_path = partition_dir
            .join(col)
            .join(format!("seg-{:010}.min_max", seg_seq));

        let data = match tokio::fs::read(&minmax_path).await {
            Ok(d) => d,
            Err(_) => return false,
        };

        let index = match MinMaxIndex::from_bytes(&data) {
            Ok(i) => i,
            Err(_) => return false,
        };

        let (global_min, global_max) = index.global_min_max();

        match predicate.op {
            PredicateOp::Eq => {
                if let Some(min) = global_min {
                    if min > predicate.value.as_ref() { return true; }
                }
                if let Some(max) = global_max {
                    if max < predicate.value.as_ref() { return true; }
                }
                false
            }
            PredicateOp::Gt | PredicateOp::Ge => {
                if let Some(max) = global_max {
                    return max < predicate.value.as_ref();
                }
                false
            }
            PredicateOp::Lt | PredicateOp::Le => {
                if let Some(min) = global_min {
                    return min > predicate.value.as_ref();
                }
                false
            }
        }
    }
}
