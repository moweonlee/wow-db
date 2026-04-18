// T032: Tablet 할당 및 Tablet → SN 매핑 관리

use std::sync::Arc;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use shared::types::{TabletId, CubeId};
use tracing::info;
use uuid::Uuid;

use crate::raft::{RaftCommand, RaftManager};

// ─── Tablet 정보 ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabletInfo {
    pub tablet_id:  TabletId,
    pub cube_id:    CubeId,
    pub partition:  String,   // 예: "p_2024_q1"
    pub bucket:     u32,      // 버킷 번호 (분산 키 해시)
    pub sn_nodes:   Vec<u64>, // 복제본이 위치한 SN 노드 ID 목록 (최대 3개)
    pub leader_sn:  u64,      // 현재 Primary SN
    pub version:    u64,      // 스키마 버전
}

// ─── Tablet 관리자 ────────────────────────────────────────────────────────────

pub struct TabletManager {
    raft: Arc<RaftManager>,
}

impl TabletManager {
    pub fn new(raft: Arc<RaftManager>) -> Self {
        Self { raft }
    }

    // ── 할당 ─────────────────────────────────────────────────────────────────

    /// 새 Tablet 할당 (Cube 생성 시 파티션 × 버킷 수만큼 호출)
    pub async fn allocate(
        &self,
        cube_id:   CubeId,
        partition: &str,
        bucket:    u32,
        sn_nodes:  Vec<u64>, // 가용 SN 노드 목록에서 복제 인수만큼 선택
    ) -> Result<TabletInfo> {
        if sn_nodes.is_empty() {
            bail!("No SN nodes available for tablet allocation");
        }
        let leader_sn = sn_nodes[0];
        let tablet = TabletInfo {
            tablet_id: Uuid::new_v4(),
            cube_id,
            partition: partition.to_string(),
            bucket,
            sn_nodes:  sn_nodes.clone(),
            leader_sn,
            version:   1,
        };

        self.persist(&tablet).await?;
        info!(
            tablet_id = %tablet.tablet_id,
            partition = %partition,
            bucket,
            leader_sn,
            "Tablet allocated"
        );
        Ok(tablet)
    }

    /// Cube의 전체 Tablet 할당 (파티션 × 버킷)
    /// `available_nodes`: 현재 클러스터 SN 노드 ID 목록
    /// `bucket_count`:    Cube 분산 버킷 수
    /// `replication`:     복제 인수 (기본 3)
    pub async fn allocate_for_cube(
        &self,
        cube_id:        CubeId,
        partition:      &str,
        bucket_count:   u32,
        available_nodes: &[u64],
        replication:    usize,
    ) -> Result<Vec<TabletInfo>> {
        let rep = replication.min(available_nodes.len());
        let mut tablets = Vec::new();

        for bucket in 0..bucket_count {
            // 버킷마다 다른 노드 세트를 선택 (round-robin 방식)
            let start  = (bucket as usize * rep) % available_nodes.len().max(1);
            let nodes: Vec<u64> = (0..rep)
                .map(|i| available_nodes[(start + i) % available_nodes.len()])
                .collect();

            let t = self.allocate(cube_id, partition, bucket, nodes).await?;
            tablets.push(t);
        }
        Ok(tablets)
    }

    // ── 조회 ─────────────────────────────────────────────────────────────────

    pub async fn get(&self, tablet_id: &TabletId) -> Result<Option<TabletInfo>> {
        let key = format!("tablet:{tablet_id}");
        match self.raft.read(&key).await {
            None       => Ok(None),
            Some(json) => Ok(Some(serde_json::from_str(&json)?)),
        }
    }

    /// Cube의 모든 Tablet 목록
    pub async fn list_for_cube(&self, cube_id: &str) -> Result<Vec<TabletInfo>> {
        let sm = self.raft.sm.read().await;
        let mut result = Vec::new();
        for (k, v) in &sm.kv {
            if k.starts_with("tablet:") {
                if let Ok(info) = serde_json::from_str::<TabletInfo>(v) {
                    if info.cube_id.to_string() == cube_id {
                        result.push(info);
                    }
                }
            }
        }
        Ok(result)
    }

    /// Cube의 특정 파티션 내 모든 Tablet 목록
    pub async fn list_for_partition(
        &self,
        cube_id:   &CubeId,
        partition: &str,
    ) -> Result<Vec<TabletInfo>> {
        let sm = self.raft.sm.read().await;
        let mut result = Vec::new();
        for (k, v) in &sm.kv {
            if k.starts_with("tablet:") {
                if let Ok(info) = serde_json::from_str::<TabletInfo>(v) {
                    if &info.cube_id == cube_id && info.partition == partition {
                        result.push(info);
                    }
                }
            }
        }
        result.sort_by_key(|t| t.bucket);
        Ok(result)
    }

    /// 버킷 해시로 Tablet 라우팅
    pub async fn route(
        &self,
        cube_id:   &CubeId,
        partition: &str,
        hash_val:  u64,
        bucket_count: u32,
    ) -> Result<Option<TabletInfo>> {
        let bucket = (hash_val % bucket_count as u64) as u32;
        let tablets = self.list_for_partition(cube_id, partition).await?;
        Ok(tablets.into_iter().find(|t| t.bucket == bucket))
    }

    // ── 리더 변경 (Tablet HA) ─────────────────────────────────────────────────

    pub async fn update_leader(&self, tablet_id: &TabletId, new_leader: u64) -> Result<()> {
        let mut tablet = match self.get(tablet_id).await? {
            Some(t) => t,
            None    => bail!("Tablet {} not found", tablet_id),
        };
        tablet.leader_sn = new_leader;
        self.persist(&tablet).await?;
        info!(%tablet_id, new_leader, "Tablet leader updated");
        Ok(())
    }

    // ─── 내부 헬퍼 ──────────────────────────────────────────────────────────

    async fn persist(&self, tablet: &TabletInfo) -> Result<()> {
        let json = serde_json::to_string(tablet)?;
        self.raft
            .write(RaftCommand::RegisterTablet {
                tablet_id: tablet.tablet_id.to_string(),
                node_id:   tablet.leader_sn,
                partition: tablet.partition.clone(),
            })
            .await?;
        // 전체 JSON을 별도 키에도 저장 (상세 조회용)
        let mut sm = self.raft.sm.write().await;
        sm.kv.insert(format!("tablet:{}", tablet.tablet_id), json);
        Ok(())
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_allocate_and_get() {
        let raft = Arc::new(RaftManager::new(1));
        let mgr  = TabletManager::new(raft);
        let cid  = Uuid::new_v4();

        let t = mgr.allocate(cid, "p_2024_q1", 0, vec![1, 2, 3]).await.unwrap();
        let got = mgr.get(&t.tablet_id).await.unwrap();
        assert!(got.is_some());
        assert_eq!(got.unwrap().bucket, 0);
    }

    #[tokio::test]
    async fn test_allocate_for_cube() {
        let raft = Arc::new(RaftManager::new(1));
        let mgr  = TabletManager::new(raft);
        let cid  = Uuid::new_v4();

        let tablets = mgr
            .allocate_for_cube(cid, "p_2024_q1", 4, &[1, 2, 3], 3)
            .await
            .unwrap();
        assert_eq!(tablets.len(), 4);
        let buckets: Vec<_> = tablets.iter().map(|t| t.bucket).collect();
        assert_eq!(buckets, vec![0, 1, 2, 3]);
    }
}
