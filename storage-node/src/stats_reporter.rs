// T135: StatsReporter — Flush/Compaction 완료 후 ShardStats를 QN에 보고 (FR-040)
//
// 동작:
//   1. Flush 완료 또는 Compaction 완료 이벤트를 수신
//   2. 해당 Shard의 컬럼 파일에서 min/max/null_count를 계산
//   3. QN gRPC ReportShardStats 호출 (or mock in tests)
//
// QN 통계 키: /stats/{table_id}/{partition_id}/{shard_id}

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use uuid::Uuid;

use shared::types::{ShardColumnStats, ShardStats, Value};

// ─── 통계 보고 이벤트 ────────────────────────────────────────────────────────

/// StatsReporter에 보내는 작업 이벤트
#[derive(Debug)]
pub enum StatsEvent {
    /// MemTable Flush 완료: Shard 전체 통계 재계산 필요
    FlushCompleted {
        table_id:     Uuid,
        partition_id: Uuid,
        shard_id:     Uuid,
        shard_dir:    PathBuf,
    },
    /// Compaction 완료: 증분 통계 업데이트
    CompactionCompleted {
        table_id:     Uuid,
        partition_id: Uuid,
        shard_id:     Uuid,
        shard_dir:    PathBuf,
        /// true면 증분, false면 전체 재계산
        is_incremental: bool,
    },
}

// ─── QN 통계 보고 추상화 ──────────────────────────────────────────────────────

/// QN에 통계를 전송하는 트레이트
/// 실제 환경: gRPC ReportShardStats RPC
/// 테스트: MockQnReporter
#[async_trait::async_trait]
pub trait QnReporter: Send + Sync + 'static {
    async fn report(
        &self,
        table_id:       Uuid,
        partition_id:   Uuid,
        shard_id:       Uuid,
        stats:          ShardStats,
        is_incremental: bool,
    ) -> Result<()>;
}

/// gRPC QnReporter stub (proto 재생성 후 구현)
pub struct GrpcQnReporter {
    pub qn_endpoint: String,
}

#[async_trait::async_trait]
impl QnReporter for GrpcQnReporter {
    async fn report(
        &self,
        table_id:       Uuid,
        partition_id:   Uuid,
        shard_id:       Uuid,
        stats:          ShardStats,
        is_incremental: bool,
    ) -> Result<()> {
        // TODO: tonic QueryServiceClient::report_shard_stats(ShardStatsReport) 호출
        // storage.proto 업데이트 후 cargo build 시 자동 생성
        info!(
            shard_id       = %shard_id,
            table_id       = %table_id,
            partition_id   = %partition_id,
            row_count      = stats.row_count,
            is_incremental = is_incremental,
            qn_endpoint    = %self.qn_endpoint,
            "QN 통계 보고 (stub — proto 재생성 필요)"
        );
        Ok(())
    }
}

// ─── StatsReporter ────────────────────────────────────────────────────────────

/// Flush/Compaction 완료 후 Shard 통계를 수집하여 QN에 보고하는 백그라운드 서비스
pub struct StatsReporter<R: QnReporter = GrpcQnReporter> {
    reporter: Arc<R>,
    rx:       mpsc::Receiver<StatsEvent>,
}

impl<R: QnReporter> StatsReporter<R> {
    pub fn new(reporter: R, rx: mpsc::Receiver<StatsEvent>) -> Self {
        Self { reporter: Arc::new(reporter), rx }
    }

    /// 이벤트 루프 실행 (tokio::spawn으로 백그라운드 실행)
    pub async fn run(mut self) {
        info!("StatsReporter 시작");
        while let Some(event) = self.rx.recv().await {
            let reporter = Arc::clone(&self.reporter);
            tokio::spawn(async move {
                if let Err(e) = Self::handle_event(reporter, event).await {
                    warn!(err = %e, "StatsReporter 이벤트 처리 오류");
                }
            });
        }
        info!("StatsReporter 종료");
    }

    async fn handle_event(reporter: Arc<R>, event: StatsEvent) -> Result<()> {
        match event {
            StatsEvent::FlushCompleted { table_id, partition_id, shard_id, shard_dir } => {
                debug!(shard_id = %shard_id, "Flush 완료 — 통계 계산 시작");
                let stats = compute_shard_stats(&shard_dir).await
                    .with_context(|| format!("Shard {} 통계 계산 실패", shard_id))?;
                reporter.report(table_id, partition_id, shard_id, stats, false).await?;
            }
            StatsEvent::CompactionCompleted {
                table_id, partition_id, shard_id, shard_dir, is_incremental,
            } => {
                debug!(shard_id = %shard_id, is_incremental, "Compaction 완료 — 통계 계산 시작");
                let stats = compute_shard_stats(&shard_dir).await
                    .with_context(|| format!("Shard {} 통계 계산 실패 (compaction)", shard_id))?;
                reporter.report(table_id, partition_id, shard_id, stats, is_incremental).await?;
            }
        }
        Ok(())
    }
}

// ─── 통계 계산 ───────────────────────────────────────────────────────────────

/// Shard 디렉토리에서 컬럼 통계를 수집
/// 각 컬럼의 .min_max 파일에서 min/max 값을 읽고 .col 파일에서 행 수를 추정
pub async fn compute_shard_stats(shard_dir: &Path) -> Result<ShardStats> {
    let mut stats    = ShardStats::default();
    let mut col_map: HashMap<String, ShardColumnStats> = HashMap::new();

    if !shard_dir.exists() {
        return Ok(stats);
    }

    // 컬럼 디렉터리 순회
    let mut rd = tokio::fs::read_dir(shard_dir).await?;
    while let Some(entry) = rd.next_entry().await? {
        let ft = entry.file_type().await?;
        if !ft.is_dir() { continue; }

        let col_name = entry.file_name().to_string_lossy().to_string();
        if col_name.starts_with('_') { continue; } // _meta, _flat 등 스킵

        let col_dir = entry.path();
        let col_stats = collect_column_stats(&col_dir).await;
        col_map.insert(col_name, col_stats);
    }

    // 총 행 수: 첫 번째 컬럼의 .col 파일로 추정
    if let Some(first_col) = col_map.keys().next().cloned() {
        let col_dir = shard_dir.join(&first_col);
        stats.row_count  = estimate_row_count(&col_dir).await;
        stats.size_bytes = estimate_size_bytes(shard_dir).await;
    }

    stats.column_stats = col_map;
    stats.updated_at   = Utc::now();
    Ok(stats)
}

/// 컬럼 디렉터리에서 min/max/null_count 수집
async fn collect_column_stats(col_dir: &Path) -> ShardColumnStats {
    let mut cs = ShardColumnStats::default();

    // .min_max 파일들에서 전체 min/max 집계
    if let Ok(mut rd) = tokio::fs::read_dir(col_dir).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.ends_with(".min_max") { continue; }

            if let Ok(data) = tokio::fs::read(entry.path()).await {
                if let Ok(index) = crate::index::minmax::MinMaxIndex::from_bytes(&data) {
                    let (gmin, gmax) = index.global_min_max();

                    if let Some(min_bytes) = gmin {
                        let new_min = Value::Bytes(min_bytes.to_vec());
                        cs.min_val = Some(match &cs.min_val {
                            None    => new_min,
                            Some(existing) => {
                                if min_bytes < bytes_of(existing) { new_min }
                                else { existing.clone() }
                            }
                        });
                    }
                    if let Some(max_bytes) = gmax {
                        let new_max = Value::Bytes(max_bytes.to_vec());
                        cs.max_val = Some(match &cs.max_val {
                            None    => new_max,
                            Some(existing) => {
                                if max_bytes > bytes_of(existing) { new_max }
                                else { existing.clone() }
                            }
                        });
                    }
                }
            }
        }
    }

    cs
}

fn bytes_of(v: &Value) -> &[u8] {
    match v {
        Value::Bytes(b) => b.as_slice(),
        _               => &[],
    }
}

/// 컬럼 디렉터리 내 .col 파일들의 첫 4 bytes (행 수 헤더) 합산
async fn estimate_row_count(col_dir: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(mut rd) = tokio::fs::read_dir(col_dir).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.ends_with(".col") { continue; }
            if let Ok(data) = tokio::fs::read(entry.path()).await {
                if data.len() >= 4 {
                    let n = u32::from_le_bytes(data[0..4].try_into().unwrap_or([0; 4]));
                    total += n as u64;
                }
            }
        }
    }
    total
}

/// Shard 디렉터리 내 모든 파일 크기 합산
async fn estimate_size_bytes(shard_dir: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(mut rd) = tokio::fs::read_dir(shard_dir).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            if let Ok(meta) = entry.metadata().await {
                if meta.is_file() {
                    total += meta.len();
                }
            }
        }
    }
    total
}

// ─── StatsReporter 빌더 ──────────────────────────────────────────────────────

/// StatsReporter 채널 페어 생성
pub fn create_stats_reporter(
    reporter: impl QnReporter,
) -> (mpsc::Sender<StatsEvent>, StatsReporter<impl QnReporter>) {
    let (tx, rx) = mpsc::channel::<StatsEvent>(256);
    let reporter = StatsReporter::new(reporter, rx);
    (tx, reporter)
}

// ─── 단위 테스트 ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tempfile::TempDir;

    struct MockReporter {
        reports: Mutex<Vec<(Uuid, Uuid, Uuid, u64)>>,
    }

    impl MockReporter {
        fn new() -> Self { Self { reports: Mutex::new(Vec::new()) } }
    }

    #[async_trait::async_trait]
    impl QnReporter for MockReporter {
        async fn report(
            &self,
            table_id:       Uuid,
            partition_id:   Uuid,
            shard_id:       Uuid,
            stats:          ShardStats,
            _incremental:   bool,
        ) -> Result<()> {
            self.reports.lock().unwrap()
                .push((table_id, partition_id, shard_id, stats.row_count));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_flush_event_triggers_report() {
        let tmp   = TempDir::new().unwrap();
        let (tx, rx) = mpsc::channel(8);
        let mock  = Arc::new(MockReporter::new());
        let mock2 = Arc::clone(&mock);

        let reporter  = StatsReporter::new(
            {
                struct Wrapped(Arc<MockReporter>);
                #[async_trait::async_trait]
                impl QnReporter for Wrapped {
                    async fn report(
                        &self,
                        table_id:     Uuid,
                        partition_id: Uuid,
                        shard_id:     Uuid,
                        stats:        ShardStats,
                        incremental:  bool,
                    ) -> Result<()> {
                        self.0.report(table_id, partition_id, shard_id, stats, incremental).await
                    }
                }
                Wrapped(mock2)
            },
            rx,
        );

        let tid = Uuid::new_v4();
        let pid = Uuid::new_v4();
        let sid = Uuid::new_v4();

        tx.send(StatsEvent::FlushCompleted {
            table_id:     tid,
            partition_id: pid,
            shard_id:     sid,
            shard_dir:    tmp.path().to_path_buf(),
        }).await.unwrap();

        drop(tx); // 채널 닫기 → reporter 루프 종료

        reporter.run().await;

        // 이벤트가 spawn으로 비동기 처리되므로 짧은 대기
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let reports = mock.reports.lock().unwrap();
        assert_eq!(reports.len(), 1);
        let (rt, rp, rs, _) = reports[0];
        assert_eq!(rt, tid);
        assert_eq!(rp, pid);
        assert_eq!(rs, sid);
    }

    #[tokio::test]
    async fn test_compute_stats_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let stats = compute_shard_stats(tmp.path()).await.unwrap();
        assert_eq!(stats.row_count, 0);
        assert!(stats.column_stats.is_empty());
    }
}
