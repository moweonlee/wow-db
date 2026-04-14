// T042 + T134: ScanTablet 구현 및 ShardScanner — 4단계 필터링
// T134: ShardScanner — ① SSTable Bloom → ② Sort Key Range → ③ MINMAX → ④ Column read
// L0 Multi-Version: sequence_num 내림차순 스캔, 중복 키 처리

use std::path::{Path, PathBuf};

use anyhow::Result;
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tracing::{debug, info, warn};
use uuid::Uuid;

use shared::types::{LsmScanRange, ShardPredicate, ShardPredicateOp, ShardScanRequest, Value};

use crate::columnar::compression::{decompress, Codec};
use crate::index::minmax::MinMaxIndex;
use crate::lsm::levels::SstRef;
use crate::lsm::bloom::SsTableBloom;

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

// ─── ShardScanner (T134, FR-041) ─────────────────────────────────────────────
//
// 4단계 필터링 파이프라인:
//   ① SSTable 전체 Bloom Filter  → Part(SSTable) 단위 스킵
//   ② Sort Key Range             → min/max_sort_key로 Part 단위 스킵
//   ③ Granule MINMAX 인덱스      → Granule(8192행) 단위 스킵
//   ④ Column File 읽기           → 필요 컬럼만 읽어 Arrow IPC 배치 생성
//
// L0 Multi-Version Read:
//   - L0 SSTable은 key range 중첩 가능
//   - sequence_num 내림차순으로 순회하여 최신 버전 우선 적용

/// ShardScanner: SN 내부에서 단일 Shard를 스캔하는 실행기
pub struct ShardScanner {
    /// Shard 로컬 디렉토리 (Manifest와 컬럼 파일이 위치)
    shard_dir: PathBuf,
}

/// 스캔 결과 요약
#[derive(Debug, Default)]
pub struct ShardScanStats {
    pub parts_scanned:     u32,
    pub parts_skipped:     u32,
    pub granules_read:     u64,
    pub granules_skipped:  u64,
    pub rows_returned:     u64,
    pub bytes_read:        u64,
}

/// Shard 스캔 응답 아이템 (스트림 원소)
pub enum ShardScanItem {
    /// Part 목록 (스트림 첫 번째)
    PartList(Vec<SstRef>),
    /// Arrow IPC 청크 데이터
    Batch { ipc_bytes: Vec<u8>, rows: u64, is_last: bool },
    /// 스캔 완료 통계 (스트림 마지막)
    Stats(ShardScanStats),
}

impl ShardScanner {
    pub fn new(shard_dir: PathBuf) -> Self {
        Self { shard_dir }
    }

    /// ShardScanRequest를 처리하여 아이템 스트림 반환
    pub async fn scan(
        &self,
        req:  &ShardScanRequest,
        tx:   mpsc::Sender<Result<ShardScanItem>>,
    ) -> Result<()> {
        // Manifest에서 활성 SSTable 목록 로드
        let sstables = self.load_active_parts().await?;

        // Part 목록 전송 (스트림 첫 번째)
        let _ = tx.send(Ok(ShardScanItem::PartList(sstables.clone()))).await;

        let mut stats = ShardScanStats::default();
        let scan_range = &req.scan_range;

        // L0은 sequence_num 내림차순 (최신 우선), L1+는 min_sort_key 기준 정렬
        let mut ordered = sstables.clone();
        ordered.sort_by(|a, b| {
            if a.level == 0 && b.level == 0 {
                b.sequence_num.cmp(&a.sequence_num) // L0: 최신 우선
            } else {
                a.level.cmp(&b.level)
                    .then(a.min_sort_key.cmp(&b.min_sort_key))
            }
        });

        // 레벨 범위 필터
        let filtered: Vec<&SstRef> = ordered.iter()
            .filter(|s| s.level >= scan_range.min_level && s.level <= scan_range.max_level)
            .collect();

        let total = filtered.len();
        let mut last_ipc: Option<(Vec<u8>, u64)> = None;

        for (idx, sst) in filtered.iter().enumerate() {
            // ① Bloom Filter 단계: Point Lookup Bloom 프로브
            if !req.bloom_probe_keys.is_empty() {
                if self.can_skip_by_bloom(sst, &req.bloom_probe_keys).await {
                    stats.parts_skipped += 1;
                    continue;
                }
            }

            // ② Sort Key Range 단계: Part min/max_sort_key 비교
            if self.can_skip_by_sort_range(sst, &req.predicates) {
                stats.parts_skipped += 1;
                continue;
            }

            stats.parts_scanned += 1;

            // ③ + ④ Granule MINMAX → Column 읽기
            let result = self.read_sst_with_filter(
                sst,
                &req.columns,
                &req.predicates,
                &mut stats,
            ).await;

            match result {
                Ok((ipc_bytes, rows)) => {
                    // 이전 배치 전송
                    if let Some((prev_ipc, prev_rows)) = last_ipc.take() {
                        let _ = tx.send(Ok(ShardScanItem::Batch {
                            ipc_bytes: prev_ipc,
                            rows:      prev_rows,
                            is_last:   false,
                        })).await;
                    }
                    last_ipc = Some((ipc_bytes, rows));
                }
                Err(e) => {
                    warn!(shard_dir = ?self.shard_dir, err = %e, "SSTable 읽기 오류");
                }
            }
        }

        // 마지막 배치 전송
        if let Some((ipc_bytes, rows)) = last_ipc {
            stats.rows_returned += rows;
            let _ = tx.send(Ok(ShardScanItem::Batch {
                ipc_bytes,
                rows,
                is_last: true,
            })).await;
        }

        // 통계 전송 (스트림 마지막)
        let _ = tx.send(Ok(ShardScanItem::Stats(stats))).await;

        Ok(())
    }

    // ─── 단계 ①: Bloom Filter ────────────────────────────────────────────────

    /// Point Lookup을 위한 SSTable 전체 Bloom 프로브
    /// .bloom 파일이 없으면 스킵 불가 (보수적 판단)
    async fn can_skip_by_bloom(&self, sst: &SstRef, probe_keys: &[Vec<u8>]) -> bool {
        let bloom_path = sst.path.with_extension("bloom");
        let data = match tokio::fs::read(&bloom_path).await {
            Ok(d)  => d,
            Err(_) => return false, // bloom 없으면 스킵 불가
        };

        if let Ok(bf) = SsTableBloom::from_bytes(&data) {
            // 모든 probe_key가 bloom에 없어야 스킵 가능
            for key in probe_keys {
                if bf.may_contain(key) {
                    return false; // 하나라도 있을 수 있으면 스킵 불가
                }
            }
            return true; // 모든 키가 확실히 없음
        }

        false // 파싱 실패 시 스킵 불가
    }

    // ─── 단계 ②: Sort Key Range ──────────────────────────────────────────────

    /// Part의 min/max_sort_key와 predicate 범위를 비교하여 스킵 여부 결정
    fn can_skip_by_sort_range(&self, sst: &SstRef, predicates: &[ShardPredicate]) -> bool {
        for pred in predicates {
            let skip = match &pred.op {
                ShardPredicateOp::Ge | ShardPredicateOp::Gt => {
                    if let Some(Value::Bytes(v)) = &pred.value {
                        sst.max_sort_key.as_slice() < v.as_slice()
                    } else { false }
                }
                ShardPredicateOp::Le | ShardPredicateOp::Lt => {
                    if let Some(Value::Bytes(v)) = &pred.value {
                        sst.min_sort_key.as_slice() > v.as_slice()
                    } else { false }
                }
                ShardPredicateOp::Eq => {
                    if let Some(Value::Bytes(v)) = &pred.value {
                        let below_min = sst.min_sort_key.as_slice() > v.as_slice();
                        let above_max = sst.max_sort_key.as_slice() < v.as_slice();
                        below_min || above_max
                    } else { false }
                }
                _ => false,
            };
            if skip { return true; }
        }
        false
    }

    // ─── 단계 ③④: Granule MINMAX + Column 읽기 ──────────────────────────────

    /// SSTable을 읽어 Arrow IPC 바이트와 행 수를 반환
    /// 내부적으로 MINMAX를 확인하여 불필요한 Granule을 스킵함
    async fn read_sst_with_filter(
        &self,
        sst:        &SstRef,
        columns:    &[String],
        predicates: &[ShardPredicate],
        stats:      &mut ShardScanStats,
    ) -> Result<(Vec<u8>, u64)> {
        let part_dir = &sst.path.parent()
            .unwrap_or_else(|| sst.path.as_path());

        // 컬럼 목록 결정
        let scan_cols = if columns.is_empty() {
            Self::list_columns_in_dir(part_dir).await?
        } else {
            columns.to_vec()
        };

        if scan_cols.is_empty() {
            return Ok((Vec::new(), 0));
        }

        // 단계 ③: 첫 번째 컬럼의 MINMAX 인덱스로 Granule 스킵 여부 확인
        let seq_str = format!("{:010}", sst.sequence_num);
        let minmax_path = part_dir
            .join(&scan_cols[0])
            .join(format!("seg-{}.min_max", seq_str));

        let granules_total;
        let granules_skipped;

        if let Ok(data) = tokio::fs::read(&minmax_path).await {
            if let Ok(index) = MinMaxIndex::from_bytes(&data) {
                let (total, skipped) = self.count_skippable_granules(&index, predicates);
                granules_total   = total;
                granules_skipped = skipped;
            } else {
                granules_total   = 0;
                granules_skipped = 0;
            }
        } else {
            granules_total   = 0;
            granules_skipped = 0;
        }

        stats.granules_read    += granules_total.saturating_sub(granules_skipped) as u64;
        stats.granules_skipped += granules_skipped as u64;

        // 단계 ④: 컬럼 파일 읽기
        let (ipc_bytes, rows) = self.read_segment_raw(part_dir, sst.sequence_num, &scan_cols).await?;
        stats.bytes_read += ipc_bytes.len() as u64;

        Ok((ipc_bytes, rows))
    }

    fn count_skippable_granules(
        &self,
        index:      &MinMaxIndex,
        predicates: &[ShardPredicate],
    ) -> (usize, usize) {
        let n = index.granule_count();
        let mut skipped = 0usize;
        for g_idx in 0..n {
            if self.granule_can_skip(index, g_idx, predicates) {
                skipped += 1;
            }
        }
        (n, skipped)
    }

    fn granule_can_skip(
        &self,
        index:      &MinMaxIndex,
        g_idx:      usize,
        predicates: &[ShardPredicate],
    ) -> bool {
        for pred in predicates {
            let (g_min, g_max) = match index.granule_min_max(g_idx) {
                Some(v) => v,
                None    => continue,
            };
            let skip = match &pred.op {
                ShardPredicateOp::Ge | ShardPredicateOp::Gt => {
                    if let Some(Value::Bytes(v)) = &pred.value {
                        g_max < v.as_slice()
                    } else { false }
                }
                ShardPredicateOp::Le | ShardPredicateOp::Lt => {
                    if let Some(Value::Bytes(v)) = &pred.value {
                        g_min > v.as_slice()
                    } else { false }
                }
                ShardPredicateOp::Eq => {
                    if let Some(Value::Bytes(v)) = &pred.value {
                        g_min > v.as_slice() || g_max < v.as_slice()
                    } else { false }
                }
                _ => false,
            };
            if skip { return true; }
        }
        false
    }

    /// 파티션 내 컬럼 디렉터리 목록
    async fn list_columns_in_dir(dir: &Path) -> Result<Vec<String>> {
        let mut cols = Vec::new();
        if let Ok(mut rd) = tokio::fs::read_dir(dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                if entry.file_type().await.map(|t| t.is_dir()).unwrap_or(false) {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if !name.starts_with('_') {
                        cols.push(name);
                    }
                }
            }
        }
        cols.sort();
        Ok(cols)
    }

    /// 컬럼 파일을 읽어 raw 바이트 + 행 수 반환
    async fn read_segment_raw(
        &self,
        part_dir:  &Path,
        seq_num:   u64,
        columns:   &[String],
    ) -> Result<(Vec<u8>, u64)> {
        let mut combined  = Vec::new();
        let mut row_count = 0u64;
        let seq_str = format!("{:010}", seq_num);

        for col in columns {
            let col_path = part_dir.join(col).join(format!("seg-{}.col", seq_str));
            if !col_path.exists() {
                continue;
            }
            let compressed = tokio::fs::read(&col_path).await?;
            let raw = decompress(&compressed, Codec::Lz4)
                .unwrap_or_else(|_| Bytes::copy_from_slice(&compressed));

            if row_count == 0 && raw.len() >= 4 {
                row_count = u32::from_le_bytes(raw[0..4].try_into().unwrap_or([0; 4])) as u64;
            }
            combined.extend_from_slice(&raw);
        }

        Ok((combined, row_count))
    }

    /// Shard 디렉토리의 MANIFEST에서 활성 SSTable 목록 로드
    async fn load_active_parts(&self) -> Result<Vec<SstRef>> {
        // MANIFEST는 storage-node/src/lsm/manifest.rs의 Manifest::open()으로 읽음
        // 현재는 파일시스템 스캔으로 fallback (MANIFEST 연동은 lsm 모듈 리팩토링 후)
        let mut sstables = Vec::new();
        let mut seq = 0u64;

        if !self.shard_dir.exists() {
            return Ok(sstables);
        }

        let mut rd = tokio::fs::read_dir(&self.shard_dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();
            // seg-NNNNNNNNNN.col 형태 파일로부터 SstRef 구성
            if name.ends_with(".col") && name.starts_with("seg-") {
                if let Ok(s) = name[4..14].parse::<u64>() {
                    let path = entry.path();
                    let size = tokio::fs::metadata(&path).await
                        .map(|m| m.len()).unwrap_or(0);
                    sstables.push(SstRef {
                        id:             Uuid::new_v4(),
                        sequence_num:   s,
                        generation:     0,
                        compacted_from: Vec::new(),
                        level:          0,
                        path,
                        size_bytes:     size,
                        row_count:      0,
                        min_sort_key:   Vec::new(),
                        max_sort_key:   Vec::new(),
                    });
                    seq = seq.max(s);
                }
            }
        }

        sstables.sort_by_key(|s| s.sequence_num);
        Ok(sstables)
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod shard_scanner_tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    fn make_scan_request(shard_dir: &Path) -> ShardScanRequest {
        ShardScanRequest {
            shard_id:         Uuid::new_v4(),
            shard_dir:        shard_dir.to_path_buf(),
            columns:          vec![],
            predicates:       vec![],
            scan_range:       LsmScanRange::all_levels(),
            bloom_probe_keys: vec![],
        }
    }

    #[tokio::test]
    async fn test_scan_empty_shard_returns_stats() {
        let tmp = TempDir::new().unwrap();
        let scanner = ShardScanner::new(tmp.path().to_path_buf());
        let req     = make_scan_request(tmp.path());

        let (tx, mut rx) = mpsc::channel(16);
        scanner.scan(&req, tx).await.unwrap();

        // PartList, Stats 수신
        let item1 = rx.recv().await.unwrap().unwrap();
        assert!(matches!(item1, ShardScanItem::PartList(_)));
        let item2 = rx.recv().await.unwrap().unwrap();
        assert!(matches!(item2, ShardScanItem::Stats(_)));
    }

    #[test]
    fn test_sort_key_skip_above_max() {
        let scanner = ShardScanner::new(PathBuf::from("/tmp"));
        let sst = SstRef {
            id:             Uuid::new_v4(),
            sequence_num:   1,
            generation:     0,
            compacted_from: vec![],
            level:          0,
            path:           PathBuf::from("/tmp/seg.col"),
            size_bytes:     100,
            row_count:      10,
            min_sort_key:   vec![0x01],
            max_sort_key:   vec![0x0F],
        };
        // Ge 조건: sort_key >= 0x20 → max(0x0F) < 0x20 → 스킵
        let pred = ShardPredicate {
            column: "sk".to_string(),
            op:     ShardPredicateOp::Ge,
            value:  Some(Value::Bytes(vec![0x20])),
        };
        assert!(scanner.can_skip_by_sort_range(&sst, &[pred]));
    }

    #[test]
    fn test_sort_key_no_skip_overlapping() {
        let scanner = ShardScanner::new(PathBuf::from("/tmp"));
        let sst = SstRef {
            id:             Uuid::new_v4(),
            sequence_num:   1,
            generation:     0,
            compacted_from: vec![],
            level:          0,
            path:           PathBuf::from("/tmp/seg.col"),
            size_bytes:     100,
            row_count:      10,
            min_sort_key:   vec![0x01],
            max_sort_key:   vec![0x10],
        };
        // Ge 조건: sort_key >= 0x05 → max(0x10) >= 0x05 → 스킵 불가
        let pred = ShardPredicate {
            column: "sk".to_string(),
            op:     ShardPredicateOp::Ge,
            value:  Some(Value::Bytes(vec![0x05])),
        };
        assert!(!scanner.can_skip_by_sort_range(&sst, &[pred]));
    }
}
