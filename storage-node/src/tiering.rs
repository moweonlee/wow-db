// T095: Tiered Storage — Hot(NVMe)→Cold(S3) 자동 이동, 파티션 age 기반 트리거

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

// ─── Tier 정의 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTier {
    /// Hot: 로컬 NVMe SSD
    Hot,
    /// Cold: S3 / MinIO / Ceph
    Cold,
    /// Archive: 장기 보관 저비용 스토리지 (Glacier 등)
    Archive,
}

impl std::fmt::Display for StorageTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageTier::Hot     => write!(f, "hot"),
            StorageTier::Cold    => write!(f, "cold"),
            StorageTier::Archive => write!(f, "archive"),
        }
    }
}

// ─── Tiering 정책 ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringPolicy {
    /// Hot → Cold 이동 기준 age (초)
    pub hot_to_cold_age:    u64,
    /// Cold → Archive 이동 기준 age (초). None = Archive 없음
    pub cold_to_archive_age: Option<u64>,
    /// 이동 작업 최대 동시 실행 수
    pub max_parallel_moves: u32,
    /// 이동 대상 최소 파티션 크기 (바이트). 너무 작은 파티션은 이동 제외
    pub min_partition_bytes: u64,
}

impl Default for TieringPolicy {
    fn default() -> Self {
        Self {
            hot_to_cold_age:     30 * 24 * 3600,   // 30일
            cold_to_archive_age: Some(180 * 24 * 3600), // 6개월
            max_parallel_moves:  2,
            min_partition_bytes: 1024 * 1024,        // 1MB
        }
    }
}

// ─── 파티션 Tier 상태 ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionTierState {
    pub partition_name:  String,
    pub cube_name:       String,
    pub current_tier:    StorageTier,
    /// 파티션 생성 시각 (Unix epoch 초)
    pub created_at:      u64,
    /// 총 크기 (바이트)
    pub size_bytes:      u64,
    /// 마지막 tier 이동 시각
    pub last_moved_at:   Option<u64>,
    /// 이동 진행 중 여부
    pub moving:          bool,
}

impl PartitionTierState {
    pub fn age_secs(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(self.created_at)
    }
}

// ─── Tiering Manager ──────────────────────────────────────────────────────────

pub struct TieringManager {
    policy:     TieringPolicy,
    partitions: Mutex<HashMap<String, PartitionTierState>>,
}

impl TieringManager {
    pub fn new(policy: TieringPolicy) -> Self {
        Self {
            policy,
            partitions: Mutex::new(HashMap::new()),
        }
    }

    /// 파티션 등록
    pub async fn register_partition(
        &self,
        cube_name:      &str,
        partition_name: &str,
        size_bytes:     u64,
        created_at:     u64,
    ) {
        let key = format!("{}/{}", cube_name, partition_name);
        let mut parts = self.partitions.lock().await;
        parts.entry(key).or_insert_with(|| PartitionTierState {
            partition_name: partition_name.to_string(),
            cube_name:      cube_name.to_string(),
            current_tier:   StorageTier::Hot,
            created_at,
            size_bytes,
            last_moved_at:  None,
            moving:         false,
        });
    }

    /// Tier 이동이 필요한 파티션 목록 반환
    pub async fn candidates(&self) -> Vec<PartitionTierState> {
        let parts = self.partitions.lock().await;
        parts.values()
            .filter(|p| {
                !p.moving && p.size_bytes >= self.policy.min_partition_bytes
                    && self.should_move(p)
            })
            .cloned()
            .collect()
    }

    fn should_move(&self, p: &PartitionTierState) -> bool {
        let age = p.age_secs();
        match p.current_tier {
            StorageTier::Hot  => age >= self.policy.hot_to_cold_age,
            StorageTier::Cold => self.policy.cold_to_archive_age
                .map(|thresh| age >= thresh)
                .unwrap_or(false),
            StorageTier::Archive => false,
        }
    }

    /// Tier 이동 실행 (Hot → Cold)
    pub async fn move_to_cold(
        &self,
        cube_name:      &str,
        partition_name: &str,
    ) -> Result<()> {
        let key = format!("{}/{}", cube_name, partition_name);
        {
            let mut parts = self.partitions.lock().await;
            let state = parts.get_mut(&key)
                .ok_or_else(|| anyhow!("Partition not found: {}", key))?;
            if state.current_tier != StorageTier::Hot {
                return Err(anyhow!("Partition '{}' is not in Hot tier", key));
            }
            if state.moving {
                return Err(anyhow!("Partition '{}' is already being moved", key));
            }
            state.moving = true;
        }

        info!(cube = %cube_name, partition = %partition_name, "Tiering: Hot → Cold");

        // TODO (Phase D): 실제 SSTable 파일 복사 (Native → S3)
        // 1. 파티션 디렉토리의 모든 .col/.bloom/.min_max 파일 목록
        // 2. 각 파일을 S3Backend::put()으로 업로드
        // 3. 업로드 완료 확인 후 로컬 파일 삭제
        // 4. Bloom Filter / Index 파일도 S3로 이동
        // 5. Tablet 맵 메타데이터 업데이트 (storage = "s3")

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut parts = self.partitions.lock().await;
        if let Some(state) = parts.get_mut(&key) {
            state.current_tier  = StorageTier::Cold;
            state.moving        = false;
            state.last_moved_at = Some(now);
        }

        Ok(())
    }

    /// Tier 이동 실행 (Cold → Archive)
    pub async fn move_to_archive(
        &self,
        cube_name:      &str,
        partition_name: &str,
    ) -> Result<()> {
        let key = format!("{}/{}", cube_name, partition_name);
        {
            let mut parts = self.partitions.lock().await;
            let state = parts.get_mut(&key)
                .ok_or_else(|| anyhow!("Partition not found: {}", key))?;
            if state.current_tier != StorageTier::Cold {
                return Err(anyhow!("Partition '{}' is not in Cold tier", key));
            }
            state.moving = true;
        }

        info!(cube = %cube_name, partition = %partition_name, "Tiering: Cold → Archive");

        // TODO (Phase D): S3 → Glacier / 저비용 스토리지 이동

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut parts = self.partitions.lock().await;
        if let Some(state) = parts.get_mut(&key) {
            state.current_tier  = StorageTier::Archive;
            state.moving        = false;
            state.last_moved_at = Some(now);
        }

        Ok(())
    }

    /// 백그라운드 Tiering 실행 루프 (한 번 실행)
    pub async fn run_once(&self) -> (u32, u32) {
        let cands = self.candidates().await;
        let mut moved  = 0u32;
        let mut errors = 0u32;

        let max = self.policy.max_parallel_moves as usize;
        for batch in cands.chunks(max) {
            for p in batch {
                let result = match p.current_tier {
                    StorageTier::Hot  => self.move_to_cold(&p.cube_name, &p.partition_name).await,
                    StorageTier::Cold => self.move_to_archive(&p.cube_name, &p.partition_name).await,
                    _ => continue,
                };
                match result {
                    Ok(())  => moved  += 1,
                    Err(e) => { warn!(err = %e, "Tiering move failed"); errors += 1; }
                }
            }
        }

        debug!(moved, errors, "Tiering run_once complete");
        (moved, errors)
    }

    pub async fn get_state(&self, cube: &str, partition: &str) -> Option<PartitionTierState> {
        let key = format!("{}/{}", cube, partition);
        self.partitions.lock().await.get(&key).cloned()
    }

    pub async fn all_states(&self) -> Vec<PartitionTierState> {
        self.partitions.lock().await.values().cloned().collect()
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[tokio::test]
    async fn test_register_and_get_state() {
        let mgr = TieringManager::new(TieringPolicy::default());
        mgr.register_partition("page_events", "p_2024_q1", 10 * 1024 * 1024, now_secs()).await;

        let state = mgr.get_state("page_events", "p_2024_q1").await.unwrap();
        assert_eq!(state.current_tier, StorageTier::Hot);
        assert!(!state.moving);
    }

    #[tokio::test]
    async fn test_move_hot_to_cold() {
        let mgr = TieringManager::new(TieringPolicy::default());
        mgr.register_partition("events", "p_2023_q1", 5 * 1024 * 1024, now_secs()).await;
        mgr.move_to_cold("events", "p_2023_q1").await.unwrap();

        let state = mgr.get_state("events", "p_2023_q1").await.unwrap();
        assert_eq!(state.current_tier, StorageTier::Cold);
        assert!(state.last_moved_at.is_some());
    }

    #[tokio::test]
    async fn test_move_cold_to_archive() {
        let mgr = TieringManager::new(TieringPolicy::default());
        mgr.register_partition("events", "p_2022_q1", 2 * 1024 * 1024, now_secs()).await;
        mgr.move_to_cold("events", "p_2022_q1").await.unwrap();
        mgr.move_to_archive("events", "p_2022_q1").await.unwrap();

        let state = mgr.get_state("events", "p_2022_q1").await.unwrap();
        assert_eq!(state.current_tier, StorageTier::Archive);
    }

    #[tokio::test]
    async fn test_cannot_move_archive() {
        let mgr = TieringManager::new(TieringPolicy::default());
        mgr.register_partition("events", "old", 2 * 1024 * 1024, now_secs()).await;
        mgr.move_to_cold("events", "old").await.unwrap();
        mgr.move_to_archive("events", "old").await.unwrap();
        // Archive에서 Archive로는 불가
        let result = mgr.move_to_archive("events", "old").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_candidates_age_filter() {
        // hot_to_cold_age = 0 으로 설정하면 즉시 후보
        let policy = TieringPolicy {
            hot_to_cold_age:     0,
            cold_to_archive_age: None,
            max_parallel_moves:  4,
            min_partition_bytes: 0,
        };
        let mgr = TieringManager::new(policy);
        mgr.register_partition("events", "p1", 1024, now_secs() - 1).await;
        let cands = mgr.candidates().await;
        assert!(!cands.is_empty());
    }

    #[tokio::test]
    async fn test_run_once() {
        let policy = TieringPolicy {
            hot_to_cold_age:     0,
            cold_to_archive_age: None,
            max_parallel_moves:  4,
            min_partition_bytes: 0,
        };
        let mgr = TieringManager::new(policy);
        mgr.register_partition("e", "p1", 100, now_secs() - 1).await;
        mgr.register_partition("e", "p2", 100, now_secs() - 1).await;
        let (moved, errors) = mgr.run_once().await;
        assert_eq!(moved, 2);
        assert_eq!(errors, 0);
    }
}
