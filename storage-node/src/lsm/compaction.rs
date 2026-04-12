// T038: Leveled Compaction — L0→L1, 파티션 경계 내에서만 Merge, 백그라운드 tokio task

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tracing::{debug, info, warn};

// ─── SSTable 레벨 메타데이터 ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SsTableRef {
    pub seq:       u64,
    pub level:     u32,
    pub path:      PathBuf,
    pub size_bytes: u64,
    pub row_count:  u64,
}

// ─── Compaction 전략 ──────────────────────────────────────────────────────────

/// Leveled Compaction 설정
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// L0 파일이 이 수 이상이면 compaction 트리거
    pub l0_file_count_trigger: usize,
    /// L1 최대 크기 (바이트)
    pub l1_max_bytes: u64,
    /// 각 레벨별 크기 배수
    pub level_size_multiplier: u64,
    /// 최대 레벨 수
    pub max_levels: u32,
    /// Compaction 주기 (백그라운드)
    pub check_interval_ms: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            l0_file_count_trigger: 4,
            l1_max_bytes:          256 * 1024 * 1024, // 256 MiB
            level_size_multiplier: 10,
            max_levels:            7,
            check_interval_ms:     1_000,
        }
    }
}

// ─── 파티션 단위 Compaction 상태 ─────────────────────────────────────────────

pub struct PartitionCompactionState {
    pub partition_dir: PathBuf,
    pub levels:        Vec<Vec<SsTableRef>>,  // levels[0] = L0, ...
    config:            CompactionConfig,
}

impl PartitionCompactionState {
    pub fn new(partition_dir: PathBuf, config: CompactionConfig) -> Self {
        let max_levels = config.max_levels as usize;
        Self {
            partition_dir,
            levels: vec![Vec::new(); max_levels],
            config,
        }
    }

    /// L0에 새 SSTable 등록
    pub fn register_l0(&mut self, sst: SsTableRef) {
        self.levels[0].push(sst);
    }

    /// Compaction 트리거 여부 확인
    pub fn needs_compaction(&self) -> bool {
        // L0 파일 수 임계값 초과
        if self.levels[0].len() >= self.config.l0_file_count_trigger {
            return true;
        }
        // 각 레벨 크기 초과 확인
        let mut max_bytes = self.config.l1_max_bytes;
        for (level, ssts) in self.levels.iter().enumerate().skip(1) {
            let total: u64 = ssts.iter().map(|s| s.size_bytes).sum();
            if total > max_bytes {
                return true;
            }
            max_bytes *= self.config.level_size_multiplier;
            let _ = level; // suppress warning
        }
        false
    }

    /// L0→L1 Compaction 실행
    /// 실제 구현에서는 파일 머지 + Sort Key 기준 정렬 + 새 SSTable 생성
    pub async fn compact_l0_to_l1(&mut self) -> Result<usize> {
        if self.levels[0].is_empty() {
            return Ok(0);
        }

        let l0_count = self.levels[0].len();
        info!(
            partition = %self.partition_dir.display(),
            l0_count,
            "L0→L1 compaction 시작"
        );

        // TODO (Phase B): 실제 파일 머지 구현
        // 1. L0 파일들을 Sort Key 기준으로 읽기
        // 2. L1과 머지 (파티션 경계 내에서만)
        // 3. 새 L1 SSTable 파일 작성
        // 4. 구 파일 삭제

        // 현재: L0를 L1으로 이동 (stub)
        let l0_files: Vec<SsTableRef> = self.levels[0].drain(..).collect();
        for mut sst in l0_files {
            sst.level = 1;
            self.levels[1].push(sst);
        }

        info!(
            partition = %self.partition_dir.display(),
            "L0→L1 compaction 완료"
        );
        Ok(l0_count)
    }
}

// ─── 백그라운드 Compaction 워커 ───────────────────────────────────────────────

pub struct CompactionWorker {
    states: Arc<Mutex<Vec<PartitionCompactionState>>>,
    config: CompactionConfig,
}

impl CompactionWorker {
    pub fn new(config: CompactionConfig) -> Self {
        Self {
            states: Arc::new(Mutex::new(Vec::new())),
            config,
        }
    }

    pub fn add_partition(&self, partition_dir: PathBuf) {
        let config = self.config.clone();
        let states = self.states.clone();
        tokio::spawn(async move {
            let state = PartitionCompactionState::new(partition_dir, config);
            states.lock().await.push(state);
        });
    }

    /// 백그라운드 compaction 루프 기동
    pub async fn run(self) {
        let interval = Duration::from_millis(self.config.check_interval_ms);
        loop {
            sleep(interval).await;
            let mut states = self.states.lock().await;
            for state in states.iter_mut() {
                if state.needs_compaction() {
                    if let Err(e) = state.compact_l0_to_l1().await {
                        warn!(
                            partition = %state.partition_dir.display(),
                            err = %e,
                            "Compaction 오류"
                        );
                    }
                }
            }
            debug!("Compaction 체크 완료 (파티션 수: {})", states.len());
        }
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_sst(seq: u64, size: u64) -> SsTableRef {
        SsTableRef {
            seq,
            level: 0,
            path: PathBuf::from(format!("seg-{:010}.col", seq)),
            size_bytes: size,
            row_count:  100,
        }
    }

    #[test]
    fn test_needs_compaction_l0_threshold() {
        let mut state = PartitionCompactionState::new(
            PathBuf::from("/tmp/test"),
            CompactionConfig { l0_file_count_trigger: 4, ..Default::default() },
        );
        for i in 0..3 {
            state.register_l0(make_sst(i, 1024));
        }
        assert!(!state.needs_compaction());
        state.register_l0(make_sst(3, 1024));
        assert!(state.needs_compaction());
    }

    #[tokio::test]
    async fn test_compact_l0_to_l1() {
        let mut state = PartitionCompactionState::new(
            PathBuf::from("/tmp/test"),
            CompactionConfig::default(),
        );
        for i in 0..4 {
            state.register_l0(make_sst(i, 1024));
        }
        let compacted = state.compact_l0_to_l1().await.unwrap();
        assert_eq!(compacted, 4);
        assert_eq!(state.levels[0].len(), 0);
        assert_eq!(state.levels[1].len(), 4);
    }
}
