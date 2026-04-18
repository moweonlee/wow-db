// T179: ShardPlacement — 신규 Shard 배치 시 Active SN 필터링
// DDL(CREATE TABLE) 시 버킷별 Primary replica SN 선택에 사용된다.

use std::sync::Arc;

use anyhow::{anyhow, Result};

use shared::cluster::NodeInfo;
use crate::meta::cluster_topology::ClusterTopologyManager;

/// Strategy for selecting Storage Nodes for new shard placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PlacementStrategy {
    /// Round-robin across Active SNs (default)
    #[default]
    RoundRobin,
    /// Least-loaded SN (fewest shards)
    LeastLoaded,
}

/// ShardPlacement: 신규 shard에 대한 SN 할당을 결정한다.
///
/// 핵심 보장: Draining/Offline 상태의 SN에는 절대 새 shard를 배치하지 않는다.
pub struct ShardPlacement {
    topology: Arc<ClusterTopologyManager>,
    strategy: PlacementStrategy,
}

impl ShardPlacement {
    pub fn new(topology: Arc<ClusterTopologyManager>) -> Self {
        Self {
            topology,
            strategy: PlacementStrategy::RoundRobin,
        }
    }

    pub fn with_strategy(mut self, strategy: PlacementStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Active SN 목록에서 `count`개의 SN을 선택해 반환한다.
    /// 선택된 SN이 replica_count보다 적으면 에러를 반환한다.
    pub async fn select_nodes(&self, count: usize) -> Result<Vec<NodeInfo>> {
        let active_sns = self.topology.eligible_storage_nodes().await;

        if active_sns.is_empty() {
            return Err(anyhow!(
                "No Active Storage Nodes available for shard placement"
            ));
        }
        if active_sns.len() < count {
            return Err(anyhow!(
                "Requested {} replicas but only {} Active SNs available",
                count,
                active_sns.len()
            ));
        }

        let selected = match self.strategy {
            PlacementStrategy::RoundRobin => {
                // 알파벳 순으로 정렬 후 앞에서 count개 선택 (단순 결정론적 배치)
                let mut sorted = active_sns.clone();
                sorted.sort_by(|a, b| a.node_id.cmp(&b.node_id));
                sorted.into_iter().take(count).collect()
            }
            PlacementStrategy::LeastLoaded => {
                // 현재 구현에서는 RoundRobin과 동일 (Phase D에서 shard count 기반으로 개선)
                let mut sorted = active_sns.clone();
                sorted.sort_by(|a, b| a.node_id.cmp(&b.node_id));
                sorted.into_iter().take(count).collect()
            }
        };

        Ok(selected)
    }

    /// `bucket_count` 버킷에 대한 Primary SN 배정 맵 반환.
    /// 반환값: bucket_index(0-based) → sn_node_id
    pub async fn assign_buckets(
        &self,
        bucket_count: usize,
        replica_count: usize,
    ) -> Result<Vec<Vec<String>>> {
        let active_sns = self.topology.eligible_storage_nodes().await;

        if active_sns.is_empty() {
            return Err(anyhow!("No Active SNs for bucket assignment"));
        }

        let mut sorted = active_sns.clone();
        sorted.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        let n = sorted.len();

        let assignments: Vec<Vec<String>> = (0..bucket_count)
            .map(|i| {
                // replica_count 개의 SN을 선택 (라운드로빈, 인접하지 않게 간격 배치)
                (0..replica_count.min(n))
                    .map(|r| sorted[(i + r) % n].node_id.clone())
                    .collect()
            })
            .collect();

        Ok(assignments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared::cluster::{NodeInfo, NodeState, NodeType};

    async fn make_topology_with_sns(ids: &[&str]) -> Arc<ClusterTopologyManager> {
        let t = Arc::new(ClusterTopologyManager::new());
        for id in ids {
            t.register_node(NodeInfo::new(*id, NodeType::StorageNode, "127.0.0.1:9060"))
                .await.unwrap();
        }
        t
    }

    #[tokio::test]
    async fn select_nodes_from_active_sns() {
        let t  = make_topology_with_sns(&["sn-1", "sn-2", "sn-3"]).await;
        let sp = ShardPlacement::new(t);
        let selected = sp.select_nodes(2).await.unwrap();
        assert_eq!(selected.len(), 2);
    }

    #[tokio::test]
    async fn draining_sn_excluded() {
        let t = make_topology_with_sns(&["sn-1", "sn-2", "sn-3"]).await;
        t.set_draining("sn-1").await.unwrap();
        let sp = ShardPlacement::new(t);
        let selected = sp.select_nodes(2).await.unwrap();
        assert!(selected.iter().all(|n| n.node_id != "sn-1"));
    }

    #[tokio::test]
    async fn not_enough_active_sns_returns_error() {
        let t  = make_topology_with_sns(&["sn-1"]).await;
        let sp = ShardPlacement::new(t);
        assert!(sp.select_nodes(3).await.is_err());
    }

    #[tokio::test]
    async fn no_active_sns_returns_error() {
        let t  = make_topology_with_sns(&["sn-1"]).await;
        t.set_draining("sn-1").await.unwrap();
        let sp = ShardPlacement::new(t);
        assert!(sp.select_nodes(1).await.is_err());
    }

    #[tokio::test]
    async fn assign_buckets_round_robin() {
        let t  = make_topology_with_sns(&["sn-1", "sn-2", "sn-3"]).await;
        let sp = ShardPlacement::new(t);
        let assignments = sp.assign_buckets(6, 2).await.unwrap();
        assert_eq!(assignments.len(), 6);
        // Each bucket should have 2 replicas
        for bucket in &assignments {
            assert_eq!(bucket.len(), 2);
        }
    }
}
