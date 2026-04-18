// T067: Async INSERT Buffer — QN 메모리 누적, 임계값/타임아웃 도달 시 배치 플러시, 종료 시 데이터 보존

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::sleep;
use tracing::{info, warn};

// ─── 버퍼 설정 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AsyncInsertConfig {
    /// 버퍼당 최대 행 수 (초과 시 즉시 플러시)
    pub max_rows:     usize,
    /// 최대 버퍼 메모리 (초과 시 즉시 플러시)
    pub max_bytes:    usize,
    /// 타임아웃 (마지막 INSERT 이후 이 시간 내 플러시)
    pub flush_interval: Duration,
    /// 채널 용량
    pub channel_cap:  usize,
}

impl Default for AsyncInsertConfig {
    fn default() -> Self {
        Self {
            max_rows:       100_000,
            max_bytes:      128 * 1024 * 1024, // 128 MiB
            flush_interval: Duration::from_secs(5),
            channel_cap:    1024,
        }
    }
}

// ─── 행 단위 ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct InsertRow {
    pub cube_name: String,
    /// 컬럼별 원시 값 (bytes)
    pub columns:   HashMap<String, Vec<u8>>,
}

impl InsertRow {
    /// 대략적 바이트 크기 추정
    pub fn approx_bytes(&self) -> usize {
        self.columns.values().map(|v| v.len() + 16).sum()
    }
}

// ─── 플러시 배치 ──────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct FlushBatch {
    pub cube_name: String,
    pub rows:      Vec<InsertRow>,
    pub reason:    FlushReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlushReason {
    /// 행 수 임계값 초과
    RowLimit,
    /// 메모리 임계값 초과
    ByteLimit,
    /// 타임아웃
    Timeout,
    /// 종료 (graceful shutdown)
    Shutdown,
}

// ─── 큐브별 내부 버퍼 ────────────────────────────────────────────────────────

struct CubeBuffer {
    rows:         Vec<InsertRow>,
    total_bytes:  usize,
    last_insert:  Instant,
}

impl CubeBuffer {
    fn new() -> Self {
        Self { rows: Vec::new(), total_bytes: 0, last_insert: Instant::now() }
    }

    fn push(&mut self, row: InsertRow) {
        let sz = row.approx_bytes();
        self.rows.push(row);
        self.total_bytes += sz;
        self.last_insert = Instant::now();
    }

    fn needs_flush(&self, cfg: &AsyncInsertConfig) -> Option<FlushReason> {
        if self.rows.len() >= cfg.max_rows {
            return Some(FlushReason::RowLimit);
        }
        if self.total_bytes >= cfg.max_bytes {
            return Some(FlushReason::ByteLimit);
        }
        if self.last_insert.elapsed() >= cfg.flush_interval && !self.rows.is_empty() {
            return Some(FlushReason::Timeout);
        }
        None
    }

    fn drain(&mut self) -> Vec<InsertRow> {
        self.total_bytes = 0;
        std::mem::take(&mut self.rows)
    }
}

// ─── Async INSERT Buffer ──────────────────────────────────────────────────────

pub struct AsyncInsertBuffer {
    config:    AsyncInsertConfig,
    /// INSERT 행 수신 채널 (Producer 쪽)
    row_tx:    mpsc::Sender<InsertRow>,
    /// 플러시 배치 수신 채널 (Consumer 쪽)
    flush_rx:  Arc<Mutex<mpsc::Receiver<FlushBatch>>>,
    /// 종료 신호
    shutdown:  Arc<Notify>,
    /// 버퍼링된 총 행 수 (외부 모니터링용)
    buffered:  Arc<std::sync::atomic::AtomicUsize>,
}

impl AsyncInsertBuffer {
    pub fn new(config: AsyncInsertConfig) -> (Self, mpsc::Sender<InsertRow>) {
        let (row_tx, row_rx) = mpsc::channel::<InsertRow>(config.channel_cap);
        let (flush_tx, flush_rx) = mpsc::channel::<FlushBatch>(64);
        let shutdown  = Arc::new(Notify::new());
        let buffered  = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let cfg2      = config.clone();
        let shutdown2 = shutdown.clone();
        let buffered2 = buffered.clone();

        // 백그라운드 flush 루프
        tokio::spawn(Self::run_buffer(
            row_rx,
            flush_tx,
            cfg2,
            shutdown2,
            buffered2,
        ));

        let row_tx_clone = row_tx.clone();
        let buf = Self {
            config,
            row_tx,
            flush_rx: Arc::new(Mutex::new(flush_rx)),
            shutdown,
            buffered,
        };

        (buf, row_tx_clone)
    }

    /// INSERT 행 제출
    pub async fn insert(&self, row: InsertRow) -> Result<()> {
        self.row_tx.send(row).await
            .map_err(|e| anyhow::anyhow!("Insert buffer channel closed: {}", e))?;
        Ok(())
    }

    /// 다음 플러시 배치 수신 (None = buffer closed)
    pub async fn next_batch(&self) -> Option<FlushBatch> {
        self.flush_rx.lock().await.recv().await
    }

    /// Graceful shutdown (잔여 데이터 보존 플러시)
    pub async fn shutdown(&self) {
        self.shutdown.notify_waiters();
        info!("AsyncInsertBuffer shutdown signalled");
    }

    /// 현재 버퍼링된 행 수
    pub fn buffered_rows(&self) -> usize {
        self.buffered.load(std::sync::atomic::Ordering::Relaxed)
    }

    async fn run_buffer(
        mut row_rx:   mpsc::Receiver<InsertRow>,
        flush_tx:     mpsc::Sender<FlushBatch>,
        config:       AsyncInsertConfig,
        shutdown:     Arc<Notify>,
        buffered:     Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let mut buffers: HashMap<String, CubeBuffer> = HashMap::new();
        let interval = config.flush_interval / 2; // flush 체크 주기

        loop {
            // 행 수신 또는 타임아웃 또는 shutdown
            let row = tokio::select! {
                row = row_rx.recv() => row,
                _ = sleep(interval) => None,  // periodic timeout check
                _ = shutdown.notified() => {
                    // Graceful shutdown: 잔여 데이터 플러시
                    for (cube_name, mut buf) in buffers.drain() {
                        let rows = buf.drain();
                        if rows.is_empty() { continue; }
                        buffered.fetch_sub(rows.len(), std::sync::atomic::Ordering::Relaxed);
                        let _ = flush_tx.send(FlushBatch {
                            cube_name,
                            rows,
                            reason: FlushReason::Shutdown,
                        }).await;
                    }
                    return;
                }
            };

            if let Some(row) = row {
                let cube = row.cube_name.clone();
                let buf  = buffers.entry(cube.clone()).or_insert_with(CubeBuffer::new);
                buf.push(row);
                buffered.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                if let Some(reason) = buf.needs_flush(&config) {
                    let rows = buf.drain();
                    let count = rows.len();
                    buffered.fetch_sub(count, std::sync::atomic::Ordering::Relaxed);
                    info!(cube = %cube, rows = count, ?reason, "AsyncInsert flush triggered");
                    if flush_tx.send(FlushBatch { cube_name: cube, rows, reason }).await.is_err() {
                        warn!("Flush receiver dropped — stopping buffer worker");
                        return;
                    }
                }
            } else {
                // 주기적 타임아웃 체크
                let mut to_flush: Vec<(String, Vec<InsertRow>, FlushReason)> = Vec::new();
                for (cube, buf) in &mut buffers {
                    if let Some(reason) = buf.needs_flush(&config) {
                        let rows = buf.drain();
                        let count = rows.len();
                        buffered.fetch_sub(count, std::sync::atomic::Ordering::Relaxed);
                        to_flush.push((cube.clone(), rows, reason));
                    }
                }
                for (cube, rows, reason) in to_flush {
                    if rows.is_empty() { continue; }
                    info!(cube = %cube, rows = rows.len(), ?reason, "AsyncInsert timeout flush");
                    if flush_tx.send(FlushBatch { cube_name: cube, rows, reason }).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(cube: &str) -> InsertRow {
        let mut cols = HashMap::new();
        cols.insert("user_id".into(), 42i64.to_le_bytes().to_vec());
        InsertRow { cube_name: cube.to_string(), columns: cols }
    }

    #[tokio::test]
    async fn test_row_limit_flush() {
        let config = AsyncInsertConfig {
            max_rows:       5,
            max_bytes:      1024 * 1024,
            flush_interval: Duration::from_secs(60),
            channel_cap:    100,
        };
        let (buf, _tx) = AsyncInsertBuffer::new(config);

        for _ in 0..5 {
            buf.insert(make_row("events")).await.unwrap();
        }

        // 5행 도달 시 플러시
        let batch = tokio::time::timeout(Duration::from_secs(1), buf.next_batch())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(batch.rows.len(), 5);
        assert_eq!(batch.reason, FlushReason::RowLimit);
    }

    #[tokio::test]
    async fn test_shutdown_flushes_remaining() {
        let config = AsyncInsertConfig {
            max_rows:       1000,
            max_bytes:      1024 * 1024 * 1024,
            flush_interval: Duration::from_secs(60),
            channel_cap:    100,
        };
        let (buf, _tx) = AsyncInsertBuffer::new(config);

        buf.insert(make_row("events")).await.unwrap();
        buf.insert(make_row("events")).await.unwrap();
        buf.insert(make_row("events")).await.unwrap();

        // Allow background task to receive the rows before signalling shutdown
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        buf.shutdown().await;

        let batch = tokio::time::timeout(Duration::from_secs(3), buf.next_batch())
            .await
            .expect("shutdown flush should arrive within 3s");

        if let Some(b) = batch {
            assert_eq!(b.reason, FlushReason::Shutdown);
            assert_eq!(b.rows.len(), 3);
        }
    }

    #[tokio::test]
    async fn test_multi_cube_buffering() {
        let config = AsyncInsertConfig {
            max_rows:       10,
            flush_interval: Duration::from_secs(60),
            ..Default::default()
        };
        let (buf, _tx) = AsyncInsertBuffer::new(config);

        for _ in 0..5 {
            buf.insert(make_row("cube_a")).await.unwrap();
            buf.insert(make_row("cube_b")).await.unwrap();
        }

        // cube_a: 5, cube_b: 5 → 각각 flush 안됨 (max=10)
        // 추가 5개 삽입 → cube_a 10개 → flush
        for _ in 0..5 {
            buf.insert(make_row("cube_a")).await.unwrap();
        }

        let batch = tokio::time::timeout(Duration::from_secs(1), buf.next_batch())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.cube_name, "cube_a");
        assert_eq!(batch.rows.len(), 10);
    }
}
