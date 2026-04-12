// T023: WAL (Write-Ahead Log) — Append-only 세그먼트, CRC32 체크섬, 세그먼트 로테이션, 크래시 복구

use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crc32fast::Hasher as Crc32Hasher;
use tokio::sync::Mutex;
use tracing::{info, warn};

/// 기본 세그먼트 최대 크기: 64 MiB
const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// WAL 레코드 포맷: [crc32: 4B][tx_id: 8B][length: 4B][payload: N B]
const RECORD_HEADER_SIZE: usize = 16;

// ─── 공개 타입 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub tx_id:   u64,
    pub payload: Bytes,
}

// ─── 내부: 세그먼트 ───────────────────────────────────────────────────────────

struct Segment {
    path:   PathBuf,
    writer: BufWriter<std::fs::File>,
    size:   u64,
    seq:    u64,
}

// ─── WAL ──────────────────────────────────────────────────────────────────────

pub struct Wal {
    dir:              PathBuf,
    segment:          Arc<Mutex<Segment>>,
    max_segment_bytes: u64,
}

impl Wal {
    /// WAL 열기 (디렉토리가 없으면 생성)
    pub async fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&dir).await?;

        let seq     = Self::latest_seq(&dir)?;
        let segment = Self::open_segment(&dir, seq)?;

        info!(dir = %dir.display(), seq, "WAL opened");
        Ok(Self {
            dir,
            segment: Arc::new(Mutex::new(segment)),
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
        })
    }

    /// 엔트리 추가 (필요 시 세그먼트 로테이션)
    pub async fn append(&self, entry: &WalEntry) -> Result<()> {
        let checksum = Self::checksum(entry.tx_id, &entry.payload);
        let record_len = (RECORD_HEADER_SIZE + entry.payload.len()) as u64;

        let mut seg = self.segment.lock().await;

        // 세그먼트 용량 초과 → 로테이션
        if seg.size + record_len > self.max_segment_bytes {
            seg.writer.flush()?;
            let new_seq = seg.seq + 1;
            let new_seg = Self::open_segment(&self.dir, new_seq)?;
            *seg = new_seg;
            info!(seq = seg.seq, "WAL segment rotated");
        }

        // 레코드 직렬화: [crc32][tx_id][length][payload]
        let payload_len = entry.payload.len() as u32;
        seg.writer.write_all(&checksum.to_le_bytes())?;
        seg.writer.write_all(&entry.tx_id.to_le_bytes())?;
        seg.writer.write_all(&payload_len.to_le_bytes())?;
        seg.writer.write_all(&entry.payload)?;
        seg.writer.flush()?;

        seg.size += record_len;
        Ok(())
    }

    /// 크래시 복구: 모든 세그먼트에서 유효한 엔트리 재생
    pub fn replay(&self) -> Result<Vec<WalEntry>> {
        let mut seqs = Self::all_seqs(&self.dir)?;
        seqs.sort_unstable();

        let mut entries = Vec::new();
        for seq in seqs {
            let path = Self::seg_path(&self.dir, seq);
            match Self::replay_segment(&path) {
                Ok(mut v)  => entries.append(&mut v),
                Err(e) => warn!(seq, err = %e, "WAL 세그먼트 replay 실패 (부분 데이터 무시)"),
            }
        }
        info!(count = entries.len(), "WAL replay 완료");
        Ok(entries)
    }

    /// 현재 세그먼트 강제 fsync
    pub async fn sync(&self) -> Result<()> {
        let mut seg = self.segment.lock().await;
        seg.writer.flush()?;
        Ok(())
    }

    /// `up_to_seq` 미만 세그먼트 삭제 (체크포인트 이후 정리)
    pub async fn truncate_before(&self, up_to_seq: u64) -> Result<()> {
        let current_seq = self.segment.lock().await.seq;
        for seq in Self::all_seqs(&self.dir)? {
            if seq < up_to_seq && seq < current_seq {
                let path = Self::seg_path(&self.dir, seq);
                if let Err(e) = std::fs::remove_file(&path) {
                    warn!(seq, err = %e, "WAL 세그먼트 삭제 실패");
                } else {
                    info!(seq, "WAL 세그먼트 삭제");
                }
            }
        }
        Ok(())
    }

    // ─── 내부 헬퍼 ──────────────────────────────────────────────────────────

    fn seg_path(dir: &Path, seq: u64) -> PathBuf {
        dir.join(format!("wal-{:010}.log", seq))
    }

    fn latest_seq(dir: &Path) -> Result<u64> {
        let seqs = Self::all_seqs(dir)?;
        Ok(seqs.into_iter().max().unwrap_or(0))
    }

    fn all_seqs(dir: &Path) -> Result<Vec<u64>> {
        let mut seqs = Vec::new();
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                let name = e.file_name();
                let s    = name.to_string_lossy();
                if let Some(rest) = s.strip_prefix("wal-") {
                    if let Some(n) = rest.strip_suffix(".log") {
                        if let Ok(seq) = n.parse::<u64>() {
                            seqs.push(seq);
                        }
                    }
                }
            }
        }
        Ok(seqs)
    }

    fn open_segment(dir: &Path, seq: u64) -> Result<Segment> {
        let path = Self::seg_path(dir, seq);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let size   = file.metadata()?.len();
        let writer = BufWriter::with_capacity(256 * 1024, file);
        Ok(Segment { path, writer, size, seq })
    }

    fn checksum(tx_id: u64, payload: &[u8]) -> u32 {
        let mut h = Crc32Hasher::new();
        h.update(&tx_id.to_le_bytes());
        h.update(payload);
        h.finalize()
    }

    fn replay_segment(path: &Path) -> Result<Vec<WalEntry>> {
        let file   = std::fs::File::open(path)?;
        let mut rd = std::io::BufReader::new(file);
        let mut entries = Vec::new();
        let mut hdr = [0u8; RECORD_HEADER_SIZE];

        loop {
            match rd.read_exact(&mut hdr) {
                Ok(())                                          => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let stored_crc = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
            let tx_id      = u64::from_le_bytes(hdr[4..12].try_into().unwrap());
            let len        = u32::from_le_bytes(hdr[12..16].try_into().unwrap()) as usize;

            let mut data = vec![0u8; len];
            if let Err(e) = rd.read_exact(&mut data) {
                warn!(err = %e, "WAL 레코드 payload 읽기 실패 — replay 중단");
                break;
            }

            // CRC 검증
            let computed = Self::checksum(tx_id, &data);
            if computed != stored_crc {
                warn!(tx_id, "WAL CRC 불일치 — 이 지점에서 replay 중단");
                break;
            }

            entries.push(WalEntry { tx_id, payload: Bytes::from(data) });
        }
        Ok(entries)
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_append_and_replay() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::open(tmp.path()).await.unwrap();

        for i in 0u64..10 {
            wal.append(&WalEntry {
                tx_id:   i,
                payload: Bytes::from(format!("payload-{i}")),
            })
            .await
            .unwrap();
        }

        let replayed = wal.replay().unwrap();
        assert_eq!(replayed.len(), 10);
        assert_eq!(replayed[0].tx_id, 0);
        assert_eq!(replayed[9].tx_id, 9);
    }

    #[tokio::test]
    async fn test_segment_rotation() {
        let tmp = TempDir::new().unwrap();
        let mut wal = Wal::open(tmp.path()).await.unwrap();
        // 극소 세그먼트 크기 → 강제 로테이션
        wal.max_segment_bytes = 64;

        for i in 0u64..5 {
            wal.append(&WalEntry {
                tx_id:   i,
                payload: Bytes::from(vec![0u8; 32]),
            })
            .await
            .unwrap();
        }

        let replayed = wal.replay().unwrap();
        assert_eq!(replayed.len(), 5);
    }
}
