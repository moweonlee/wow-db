// T064: WriteRows 완전 구현 — WAL append, MemTable insert, TxID 연결

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use shared::types::{SortKey, SortKeyComponent};

use crate::lsm::memtable::{MemTable, DEFAULT_MEMTABLE_THRESHOLD};
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

/// 단일 Tablet의 쓰기 담당 (WAL + MemTable)
pub struct TabletWriter {
    tablet_id: String,
    wal:       Arc<Wal>,
    memtable:  Arc<Mutex<MemTable>>,
    data_dir:  PathBuf,
}

impl TabletWriter {
    pub async fn new(data_dir: PathBuf, tablet_id: String) -> Result<Self> {
        let wal_dir = data_dir.join(&tablet_id).join("wal");
        let wal = Arc::new(Wal::open(&wal_dir).await?);
        let memtable = Arc::new(Mutex::new(MemTable::new(DEFAULT_MEMTABLE_THRESHOLD)));

        Ok(Self { tablet_id, wal, memtable, data_dir })
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

        // MemTable이 가득 찬 경우: flush 트리거 (비동기 — Phase D에서 완전 구현)
        if mem.is_full() {
            info!(tablet_id = %self.tablet_id, "MemTable full, flush needed");
            // TODO: freeze → SSTable flush 트리거
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

    pub fn tablet_id(&self) -> &str {
        &self.tablet_id
    }

    pub async fn memtable_size(&self) -> usize {
        self.memtable.lock().await.size_bytes()
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
