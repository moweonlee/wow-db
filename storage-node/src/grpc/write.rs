// T064: WriteRows 완전 구현 — WAL append, MemTable insert, TxID 연결

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, warn};

use shared::types::{SortKey, SortKeyComponent};

use crate::lsm::levels::{CompactionConfig, PartitionLevels, SstRef, WriteControl};
use crate::lsm::memtable::{MemTable, DEFAULT_MEMTABLE_THRESHOLD};
use crate::lsm::sstable::{PartitionSeqCounter, SsTableFlusher};
use crate::lsm::wal::{Wal, WalEntry};

// ─── 쓰기 요청 타입 ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct WriteRow {
    /// Sort Key 컬럼 값 (직렬화된 바이트)
    pub sort_key_bytes: Vec<u8>,
    /// 컬럼별 원시 바이트 (column_name → value bytes)
    pub columns:        HashMap<String, Bytes>,
    /// 트랜잭션 ID (0 = auto-commit)
    pub tx_id:          u64,
}

impl WriteRow {
    pub fn to_sort_key(&self) -> SortKey {
        SortKey::new(vec![SortKeyComponent {
            column:  "sort_key".to_string(),
            encoded: self.sort_key_bytes.clone(),
        }])
    }
}

/// WAL 직렬화 포맷: [row_count: 4B][key_len: 4B][key_bytes][col_count: 4B]
///                  ([col_name_len: 4B][col_name][val_len: 4B][val_bytes])*
pub fn serialize_write_batch(rows: &[WriteRow]) -> Bytes {
    let mut buf = Vec::new();

    // row count
    buf.extend_from_slice(&(rows.len() as u32).to_le_bytes());

    for row in rows {
        // tx_id
        buf.extend_from_slice(&row.tx_id.to_le_bytes());
        // sort key
        buf.extend_from_slice(&(row.sort_key_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&row.sort_key_bytes);
        // columns
        buf.extend_from_slice(&(row.columns.len() as u32).to_le_bytes());
        for (name, val) in &row.columns {
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);
            buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
            buf.extend_from_slice(val);
        }
    }

    Bytes::from(buf)
}

pub fn deserialize_write_batch(data: &[u8]) -> Result<Vec<WriteRow>> {
    let mut pos = 0;

    let read_u32 = |pos: &mut usize, data: &[u8]| -> Result<u32> {
        if *pos + 4 > data.len() { return Err(anyhow!("Buffer underflow reading u32")); }
        let v = u32::from_le_bytes(data[*pos..*pos+4].try_into()?);
        *pos += 4;
        Ok(v)
    };

    let read_u64 = |pos: &mut usize, data: &[u8]| -> Result<u64> {
        if *pos + 8 > data.len() { return Err(anyhow!("Buffer underflow reading u64")); }
        let v = u64::from_le_bytes(data[*pos..*pos+8].try_into()?);
        *pos += 8;
        Ok(v)
    };

    let read_bytes = |pos: &mut usize, data: &[u8], len: usize| -> Result<Vec<u8>> {
        if *pos + len > data.len() { return Err(anyhow!("Buffer underflow reading {} bytes", len)); }
        let v = data[*pos..*pos+len].to_vec();
        *pos += len;
        Ok(v)
    };

    let row_count = read_u32(&mut pos, data)? as usize;
    let mut rows = Vec::with_capacity(row_count);

    for _ in 0..row_count {
        let tx_id = read_u64(&mut pos, data)?;
        let key_len = read_u32(&mut pos, data)? as usize;
        let sort_key_bytes = read_bytes(&mut pos, data, key_len)?;

        let col_count = read_u32(&mut pos, data)? as usize;
        let mut columns = HashMap::with_capacity(col_count);

        for _ in 0..col_count {
            let name_len = read_u32(&mut pos, data)? as usize;
            let name_bytes = read_bytes(&mut pos, data, name_len)?;
            let name = String::from_utf8(name_bytes)
                .map_err(|e| anyhow!("Invalid UTF-8 column name: {}", e))?;

            let val_len = read_u32(&mut pos, data)? as usize;
            let val_bytes = read_bytes(&mut pos, data, val_len)?;
            columns.insert(name, Bytes::from(val_bytes));
        }

        rows.push(WriteRow { sort_key_bytes, columns, tx_id });
    }

    Ok(rows)
}

// ─── Tablet Writer ────────────────────────────────────────────────────────────

/// 단일 Tablet의 쓰기 담당 (WAL + MemTable + SSTable flush pipeline)
pub struct TabletWriter {
    tablet_id:   String,
    wal:         Arc<Wal>,
    memtable:    Arc<Mutex<MemTable>>,
    data_dir:    PathBuf,
    levels:      Arc<RwLock<PartitionLevels>>,
    seq_counter: Arc<PartitionSeqCounter>,
}

impl TabletWriter {
    pub async fn new(data_dir: PathBuf, tablet_id: String) -> Result<Self> {
        let wal_dir = data_dir.join(&tablet_id).join("wal");
        let wal = Arc::new(Wal::open(&wal_dir).await?);
        let memtable = Arc::new(Mutex::new(MemTable::new(DEFAULT_MEMTABLE_THRESHOLD)));

        let levels      = Arc::new(RwLock::new(PartitionLevels::new(CompactionConfig::default())));
        let seq_counter = Arc::new(PartitionSeqCounter::new(0));

        let writer = Self { tablet_id: tablet_id.clone(), wal, memtable, data_dir, levels, seq_counter };

        // WAL replay: 크래시/재시작 후 MemTable 복구
        writer.replay_wal().await?;

        Ok(writer)
    }

    /// WAL 에 기록된 모든 row 배치를 순서대로 MemTable 에 재삽입.
    /// PREPARE/COMMIT/ROLLBACK 마커는 무시 (현재 단순화).
    async fn replay_wal(&self) -> Result<()> {
        let entries = self.wal.replay()?;
        if entries.is_empty() { return Ok(()); }

        let mut replayed = 0usize;
        let mut mem = self.memtable.lock().await;

        for entry in entries {
            // 마커 엔트리 (PREPARE:/COMMIT:/ROLLBACK:) 은 건너뜀
            let prefix = String::from_utf8_lossy(&entry.payload);
            if prefix.starts_with("PREPARE:") || prefix.starts_with("COMMIT:") || prefix.starts_with("ROLLBACK:") {
                continue;
            }

            match deserialize_write_batch(&entry.payload) {
                Ok(rows) => {
                    for row in rows {
                        let sort_key = row.to_sort_key();
                        let cols: Vec<(String, Bytes)> = row.columns.into_iter().collect();
                        mem.insert(sort_key, cols, row.tx_id);
                        replayed += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        tablet_id = %self.tablet_id,
                        err = %e,
                        "WAL replay: 역직렬화 실패 — 엔트리 스킵"
                    );
                }
            }
        }

        if replayed > 0 {
            info!(
                tablet_id = %self.tablet_id,
                rows = replayed,
                "WAL replay 완료 — MemTable 복구"
            );
            // FR-008: WAL 이벤트 — main.rs 의 LOG_BUFFER 는 바이너리 전용이라 직접 참조 불가.
            // 향후 LogBuffer 를 TabletWriter 에 주입하는 방식으로 개선 가능.
            // 현재는 info! tracing 로그로만 기록 (운영 로그 파일에는 남음).
        }
        Ok(())
    }

    /// 행 배치 쓰기: WAL append → MemTable insert
    pub async fn write_rows(&self, rows: Vec<WriteRow>) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        // 1. WAL에 배치 직렬화 기록 (크래시 복구용)
        let payload = serialize_write_batch(&rows);
        let tx_id   = rows.first().map(|r| r.tx_id).unwrap_or(0);

        self.wal.append(&WalEntry { tx_id, payload }).await?;

        // 2. MemTable에 삽입
        let row_count = rows.len();
        let mut mem = self.memtable.lock().await;

        for row in rows {
            let sort_key = row.to_sort_key();
            let cols: Vec<(String, Bytes)> = row.columns.into_iter().collect();
            mem.insert(sort_key, cols, row.tx_id);
        }

        debug!(
            tablet_id = %self.tablet_id,
            rows = row_count,
            full = mem.is_full(),
            "MemTable insert complete"
        );

        // MemTable이 가득 찬 경우: 스냅샷 → reset → 백그라운드 SSTable flush
        if mem.is_full() {
            let imm = mem.snapshot_as_immutable();
            mem.reset();
            drop(mem);

            info!(tablet_id = %self.tablet_id, "MemTable full, triggering SSTable flush");
            let levels        = Arc::clone(&self.levels);
            let seq_counter   = Arc::clone(&self.seq_counter);
            let partition_dir = self.data_dir.join(&self.tablet_id);
            let tablet_id     = self.tablet_id.clone();

            tokio::spawn(async move {
                let seq     = seq_counter.next();
                let flusher = SsTableFlusher::new(&partition_dir);
                match flusher.flush(imm, seq, seq).await {
                    Ok(meta) => {
                        let size_bytes: u64 =
                            meta.columns.iter().map(|c| c.compressed_bytes).sum();
                        let sst = SstRef {
                            id:             meta.id,
                            sequence_num:   meta.sequence_num,
                            generation:     meta.generation,
                            compacted_from: meta.compacted_from,
                            level:          0,
                            path:           partition_dir
                                .join(format!("_meta/sstable-{:010}.json", meta.seq)),
                            size_bytes,
                            row_count:      meta.row_count,
                            min_sort_key:   meta.min_sort_key,
                            max_sort_key:   meta.max_sort_key,
                        };
                        let mut lvls = levels.write().await;
                        lvls.add(sst);
                        info!(
                            tablet_id = %tablet_id,
                            seq,
                            l0_count = lvls.l0_count(),
                            "SSTable flushed → L0"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            tablet_id = %tablet_id,
                            err = %e,
                            "SSTable flush 실패"
                        );
                    }
                }
            });
        }

        Ok(row_count as u64)
    }

    /// Prepare Phase: WAL에 prepare 마커 기록
    pub async fn prepare(&self, tx_id: u64) -> Result<()> {
        let marker = Bytes::from(format!("PREPARE:{}", tx_id));
        self.wal.append(&WalEntry { tx_id, payload: marker }).await?;
        info!(tablet_id = %self.tablet_id, tx_id, "Prepare recorded in WAL");
        Ok(())
    }

    /// Commit Phase: WAL에 commit 마커 기록
    pub async fn commit(&self, tx_id: u64) -> Result<()> {
        let marker = Bytes::from(format!("COMMIT:{}", tx_id));
        self.wal.append(&WalEntry { tx_id, payload: marker }).await?;
        info!(tablet_id = %self.tablet_id, tx_id, "Commit recorded in WAL");
        Ok(())
    }

    /// Rollback: 해당 tx_id의 행 삭제 마커 삽입
    pub async fn rollback(&self, tx_id: u64) -> Result<()> {
        // WAL에 rollback 마커
        let marker = Bytes::from(format!("ROLLBACK:{}", tx_id));
        self.wal.append(&WalEntry { tx_id, payload: marker }).await?;

        // MemTable에서 해당 tx_id 행 삭제 마커
        let sort_key = SortKey::new(vec![SortKeyComponent {
            column:  "tx_id".to_string(),
            encoded: tx_id.to_le_bytes().to_vec(),
        }]);
        let mem = self.memtable.lock().await;
        mem.delete(sort_key, tx_id);

        warn!(tablet_id = %self.tablet_id, tx_id, "Rollback recorded in WAL");
        Ok(())
    }

    /// MemTable 전체 스캔 → (sort_key_bytes, columns) 목록 반환
    ///
    /// SELECT 경로에서 query-node 가 gRPC ScanTablet 을 호출할 때 사용.
    /// 삭제 마커(tombstone) 행은 제외한다.
    pub async fn scan_memtable_rows(&self) -> Vec<(Vec<u8>, HashMap<String, Bytes>)> {
        let mem = self.memtable.lock().await;
        mem.iter_rows()
            .into_iter()
            .map(|(key_bytes, cols)| {
                let col_map: HashMap<String, Bytes> = cols.into_iter().collect();
                (key_bytes, col_map)
            })
            .collect()
    }

    pub fn tablet_id(&self) -> &str {
        &self.tablet_id
    }

    pub async fn memtable_size(&self) -> usize {
        self.memtable.lock().await.size_bytes()
    }

    /// LSM 레벨 통계 반환 (대시보드 `/api/v1/lsm-status` 용)
    pub async fn get_lsm_stats(&self) -> crate::lsm::levels::TabletLsmStats {
        use crate::lsm::levels::TabletLsmStats;

        let mem = self.memtable.lock().await;
        let mem_size = mem.size_bytes();
        let mem_rows = mem.row_count() as u64;
        drop(mem);

        let (cube_name, partition_name) = if let Some(pos) = self.tablet_id.rfind('/') {
            (self.tablet_id[..pos].to_string(), self.tablet_id[pos+1..].to_string())
        } else {
            (self.tablet_id.clone(), "default".to_string())
        };

        let lvls = self.levels.read().await;
        let mut level_stats = lvls.level_stats();
        // 아직 flush 안 된 MemTable 데이터를 L0 크기에 합산
        if let Some(l0) = level_stats.first_mut() {
            l0.size_bytes += mem_size as u64;
        }
        let l0_count         = lvls.l0_count();
        let sst_rows: u64    = lvls.read_snapshot().iter().map(|s| s.row_count).sum();
        let compaction_score = lvls.compaction_score(0);
        let level_sizes: Vec<u64> = level_stats.iter().map(|ls| ls.size_bytes).collect();
        let write_control    = match lvls.write_control() {
            WriteControl::Stop     => "Stop",
            WriteControl::Slowdown => "Slowdown",
            WriteControl::Normal   => "Normal",
        }.to_string();
        drop(lvls);

        TabletLsmStats {
            tablet_id:          self.tablet_id.clone(),
            cube_name,
            partition_name,
            levels:             level_stats,
            l0_file_count:      l0_count,
            total_levels:       level_sizes.len() as u32,
            level_sizes,
            compaction_score,
            compaction_status:  "Idle".to_string(),
            write_control,
            last_compaction_ms: None,
            total_rows:         mem_rows + sst_rows,
        }
    }
}

// ─── Tablet Writer Registry ───────────────────────────────────────────────────

/// 노드 내 모든 TabletWriter 관리
pub struct TabletWriterRegistry {
    writers:  Arc<Mutex<HashMap<String, Arc<TabletWriter>>>>,
    data_dir: PathBuf,
}

impl TabletWriterRegistry {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            writers:  Arc::new(Mutex::new(HashMap::new())),
            data_dir,
        }
    }

    /// Tablet 을 레지스트리에서 제거하고 물리 데이터 디렉토리 삭제 (DROP CUBE / TRUNCATE)
    pub async fn drop_tablet(&self, tablet_id: &str) {
        {
            let mut writers = self.writers.lock().await;
            writers.remove(tablet_id);
        }
        // 락 밖에서 디스크 I/O 수행
        let tablet_dir = self.data_dir.join(tablet_id);
        if tablet_dir.exists() {
            match tokio::fs::remove_dir_all(&tablet_dir).await {
                Ok(_)  => tracing::info!(tablet_id, "Tablet data directory deleted"),
                Err(e) => tracing::warn!(tablet_id, err = %e, "Failed to delete tablet data dir"),
            }
        }
        tracing::info!(tablet_id = %tablet_id, "TabletWriter dropped and data purged");
    }

    /// 등록된 모든 Tablet 의 LSM 통계 반환 (대시보드 용)
    pub async fn get_all_lsm_stats(&self) -> Vec<crate::lsm::levels::TabletLsmStats> {
        let writers = self.writers.lock().await;
        let mut stats = Vec::new();
        for writer in writers.values() {
            stats.push(writer.get_lsm_stats().await);
        }
        stats
    }

    pub async fn get_or_create(&self, tablet_id: &str) -> Result<Arc<TabletWriter>> {
        let mut writers = self.writers.lock().await;
        if let Some(w) = writers.get(tablet_id) {
            return Ok(w.clone());
        }
        let writer = TabletWriter::new(self.data_dir.clone(), tablet_id.to_string()).await?;
        let writer = Arc::new(writer);
        writers.insert(tablet_id.to_string(), writer.clone());
        Ok(writer)
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_row(tx_id: u64, key: &[u8]) -> WriteRow {
        let mut cols = HashMap::new();
        cols.insert("event".into(), Bytes::from("click"));
        cols.insert("user_id".into(), Bytes::from(1i64.to_le_bytes().to_vec()));
        WriteRow {
            sort_key_bytes: key.to_vec(),
            columns:        cols,
            tx_id,
        }
    }

    #[test]
    fn test_serialization_roundtrip() {
        let rows = vec![
            make_row(1, b"key_001"),
            make_row(2, b"key_002"),
        ];
        let bytes = serialize_write_batch(&rows);
        let restored = deserialize_write_batch(&bytes).unwrap();
        assert_eq!(restored.len(), 2);
        assert_eq!(restored[0].tx_id, 1);
        assert_eq!(restored[0].sort_key_bytes, b"key_001");
        assert_eq!(restored[1].columns["event"], Bytes::from("click"));
    }

    #[tokio::test]
    async fn test_tablet_writer_write() {
        let dir = tempdir().unwrap();
        let writer = TabletWriter::new(dir.path().to_path_buf(), "tablet-001".into())
            .await.unwrap();

        let rows = vec![make_row(1, b"sort_001"), make_row(1, b"sort_002")];
        let count = writer.write_rows(rows).await.unwrap();
        assert_eq!(count, 2);
        assert!(writer.memtable_size().await > 0);
    }

    #[tokio::test]
    async fn test_prepare_commit() {
        let dir = tempdir().unwrap();
        let writer = TabletWriter::new(dir.path().to_path_buf(), "tablet-002".into())
            .await.unwrap();
        writer.prepare(42).await.unwrap();
        writer.commit(42).await.unwrap();
    }
}
