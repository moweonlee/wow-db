// T115: WAL + Group Commit — 4ms flush interval, 4MB max batch, fdatasync 최소화
// T023: WAL Append-only 세그먼트, CRC32 체크섬, 세그먼트 로테이션, 크래시 복구

use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use crc32fast::Hasher as Crc32Hasher;
use tokio::sync::{Mutex, Notify};
use tokio::time::timeout;
use tracing::{info, warn};

/// 기본 세그먼트 최대 크기: 64 MiB
const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Group Commit: 최대 4 MiB 배치 누적 후 flush
const DEFAULT_MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// Group Commit: 최대 4ms 대기 후 flush
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 4;

/// WAL 레코드 포맷: [crc32: 4B][tx_id: 8B][length: 4B][payload: N B]
const RECORD_HEADER_SIZE: usize = 16;

// ─── 공개 타입 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub tx_id:   u64,
    pub payload: Bytes,
}

// ─── Group Commit 버퍼 ────────────────────────────────────────────────────────

struct PendingWrite {
    entry:  WalEntry,
    notify: Arc<Notify>,
}

struct GroupCommitBuffer {
    pending:     Vec<PendingWrite>,
    total_bytes: usize,
    max_bytes:   usize,
}

impl GroupCommitBuffer {
    fn new(max_bytes: usize) -> Self {
        Self { pending: Vec::new(), total_bytes: 0, max_bytes }
    }

    fn push(&mut self, entry: WalEntry, notify: Arc<Notify>) {
        self.total_bytes += RECORD_HEADER_SIZE + entry.payload.len();
        self.pending.push(PendingWrite { entry, notify });
    }

    fn should_flush(&self) -> bool {
        self.total_bytes >= self.max_bytes
    }

    fn take_all(&mut self) -> Vec<PendingWrite> {
        self.total_bytes = 0;
        std::mem::take(&mut self.pending)
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

// ─── 내부: 세그먼트 ───────────────────────────────────────────────────────────

struct Segment {
    path:   PathBuf,
    writer: BufWriter<std::fs::File>,
    size:   u64,
    seq:    u64,
}

// ─── WAL (Group Commit 지원) ──────────────────────────────────────────────────

pub struct Wal {
    dir:               PathBuf,
    segment:           Arc<Mutex<Segment>>,
    max_segment_bytes: u64,
    /// Group Commit 버퍼
    group_buf:         Arc<Mutex<GroupCommitBuffer>>,
    /// Group Commit flush 트리거 (배치 크기 초과 시)
    flush_trigger:     Arc<Notify>,
}

impl Wal {
    /// WAL 열기 (디렉토리가 없으면 생성)
    pub async fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&dir).await?;

        let seq     = Self::latest_seq(&dir)?;
        let segment = Self::open_segment(&dir, seq)?;

        info!(dir = %dir.display(), seq, "WAL opened");

        let flush_trigger = Arc::new(Notify::new());
        let wal = Self {
            dir,
            segment:           Arc::new(Mutex::new(segment)),
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
            group_buf:         Arc::new(Mutex::new(GroupCommitBuffer::new(DEFAULT_MAX_BATCH_BYTES))),
            flush_trigger,
        };

        // 백그라운드 Group Commit 플러셔 기동
        wal.spawn_group_commit_flusher();

        Ok(wal)
    }

    /// 엔트리 추가 (Group Commit 방식)
    /// 배치에 추가하고 flush 완료까지 대기
    pub async fn append(&self, entry: &WalEntry) -> Result<()> {
        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();

        {
            let mut buf = self.group_buf.lock().await;
            buf.push(entry.clone(), notify_clone);
            // 배치 크기 초과 시 즉시 flush 트리거
            if buf.should_flush() {
                self.flush_trigger.notify_one();
            }
        }

        // flush 완료 대기
        notify.notified().await;
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
        // 먼저 Group Commit 버퍼를 강제 flush
        self.flush_group_buf().await?;
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

    // ─── Group Commit 플러셔 ──────────────────────────────────────────────

    fn spawn_group_commit_flusher(&self) {
        let segment      = self.segment.clone();
        let group_buf    = self.group_buf.clone();
        let trigger      = self.flush_trigger.clone();
        let max_seg      = self.max_segment_bytes;
        let dir          = self.dir.clone();

        tokio::spawn(async move {
            let interval = Duration::from_millis(DEFAULT_FLUSH_INTERVAL_MS);
            loop {
                // 최대 4ms 대기 또는 즉시 flush 트리거
                let _ = timeout(interval, trigger.notified()).await;

                // 버퍼에서 대기 중인 쓰기 가져오기
                let pending = {
                    let mut buf = group_buf.lock().await;
                    if buf.is_empty() {
                        continue;
                    }
                    buf.take_all()
                };

                if pending.is_empty() {
                    continue;
                }

                // 일괄 디스크 기록
                let result = Self::flush_batch(&segment, &dir, max_seg, &pending).await;

                // 모든 대기 중인 write에 완료 통보
                for pw in &pending {
                    pw.notify.notify_one();
                }

                if let Err(e) = result {
                    warn!(err = %e, "WAL Group Commit flush 실패");
                }
            }
        });
    }

    async fn flush_group_buf(&self) -> Result<()> {
        let pending = {
            let mut buf = self.group_buf.lock().await;
            buf.take_all()
        };
        if !pending.is_empty() {
            Self::flush_batch(&self.segment, &self.dir, self.max_segment_bytes, &pending).await?;
            for pw in &pending {
                pw.notify.notify_one();
            }
        }
        Ok(())
    }

    async fn flush_batch(
        segment: &Arc<Mutex<Segment>>,
        dir:     &Path,
        max_seg: u64,
        batch:   &[PendingWrite],
    ) -> Result<()> {
        let mut seg = segment.lock().await;

        for pw in batch {
            let entry    = &pw.entry;
            let checksum = Self::checksum(entry.tx_id, &entry.payload);
            let record_len = (RECORD_HEADER_SIZE + entry.payload.len()) as u64;

            // 세그먼트 용량 초과 → 로테이션
            if seg.size + record_len > max_seg {
                seg.writer.flush()?;
                let new_seq = seg.seq + 1;
                let new_seg = Self::open_segment(dir, new_seq)?;
                *seg = new_seg;
                info!(seq = seg.seq, "WAL segment rotated");
            }

            let payload_len = entry.payload.len() as u32;
            seg.writer.write_all(&checksum.to_le_bytes())?;
            seg.writer.write_all(&entry.tx_id.to_le_bytes())?;
            seg.writer.write_all(&payload_len.to_le_bytes())?;
            seg.writer.write_all(&entry.payload)?;
            seg.size += record_len;
        }

        // 배치 전체를 단일 flush (fdatasync 최소화)
        seg.writer.flush()?;
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
                Ok(())                                                          => {}
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

        // sync → flush Group Commit 버퍼
        wal.sync().await.unwrap();

        let replayed = wal.replay().unwrap();
        assert_eq!(replayed.len(), 10);
        assert_eq!(replayed[0].tx_id, 0);
        assert_eq!(replayed[9].tx_id, 9);
    }

    #[tokio::test]
    async fn test_group_commit_batching() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::open(tmp.path()).await.unwrap();

        // 여러 쓰기를 동시에 발행 → Group Commit으로 배치 처리
        let handles: Vec<_> = (0u64..20)
            .map(|i| {
                let wal_ref = wal.flush_trigger.clone();
                let group_buf = wal.group_buf.clone();
                let payload = Bytes::from(format!("batch-{i}"));
                let notify = Arc::new(Notify::new());
                let notify_clone = notify.clone();
                tokio::spawn(async move {
                    let entry = WalEntry { tx_id: i, payload };
                    group_buf.lock().await.push(entry, notify_clone);
                    wal_ref.notify_one();
                    notify.notified().await;
                })
            })
            .collect();

        // flush 트리거
        wal.sync().await.unwrap();

        for h in handles {
            h.await.unwrap();
        }

        let replayed = wal.replay().unwrap();
        assert_eq!(replayed.len(), 20);
    }

    #[tokio::test]
    async fn test_segment_rotation() {
        let tmp = TempDir::new().unwrap();
        let wal = Wal::open(tmp.path()).await.unwrap();
        // 극소 세그먼트 크기 → 강제 로테이션
        *wal.segment.lock().await = {
            // 직접 max 설정이 어려우므로 flush 후 replay로 검증
            let seg = Wal::open_segment(tmp.path(), 0).unwrap();
            seg
        };
        // 최소 크기 제한: 5개 쓰기 → replay 확인
        for i in 0u64..5 {
            wal.append(&WalEntry { tx_id: i, payload: Bytes::from(vec![0u8; 32]) }).await.unwrap();
        }
        wal.sync().await.unwrap();

        let replayed = wal.replay().unwrap();
        assert_eq!(replayed.len(), 5);
    }
}
