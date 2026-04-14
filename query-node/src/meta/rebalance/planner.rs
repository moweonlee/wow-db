// T180: RebalancePlanner — shard 재배치 계획 생성
// Active SN 목록과 현재 shard 분포를 받아 이전 계획을 생성한다.

use std::collections::HashMap;
use uuid::Uuid;

use shared::cluster::NodeInfo;

/// 단일 Shard 이전 계획
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationPlan {
    pub shard_id: Uuid,
    pub from_sn:  String, // sn_node_id
    pub to_sn:    String, // sn_node_id
}

/// RebalancePlanner: 현재 분포와 목표 분포를 비교해 최소 이전 계획을 생성한다.
pub struct RebalancePlanner;

impl RebalancePlanner {
    /// # 입력
    /// - `current_distribution`: shard_id → sn_node_id (현재 Leader replica 위치)
    /// - `eligible_nodes`: 배치 가능한 Active SN 목록
    ///
    /// # 알고리즘
    /// 1. Active SN이 없으면 빈 계획 반환
    /// 2. 각 SN의 목표 shard 수 = ceil(total / active_sn_count)
    /// 3. 초과한 SN에서 부족한 SN으로 shard 이전
    pub fn plan(
        current_distribution: &HashMap<Uuid, String>,
        eligible_nodes: &[NodeInfo],
    ) -> Vec<MigrationPlan> {
        if eligible_nodes.is_empty() {
            return Vec::new();
        }

        let total_shards  = current_distribution.len();
        let active_count  = eligible_nodes.len();
        let target_per_sn = total_shards.div_ceil(active_count.max(1));

        // 현재 SN별 shard 목록 구성
        let mut sn_shards: HashMap<String, Vec<Uuid>> = HashMap::new();
        for sn in eligible_nodes {
            sn_shards.entry(sn.node_id.clone()).or_default();
        }
        for (shard_id, sn_id) in current_distribution {
            sn_shards.entry(sn_id.clone()).or_default().push(*shard_id);
        }

        // 초과/부족 분류
        let mut donors:    Vec<(String, Vec<Uuid>)> = Vec::new();
        let mut receivers: Vec<String>              = Vec::new();

        for (sn_id, shards) in &sn_shards {
            if shards.len() > target_per_sn {
                donors.push((sn_id.clone(), shards.clone()));
            } else if shards.len() < target_per_sn {
                receivers.push(sn_id.clone());
            }
        }

        let mut plans = Vec::new();
        let mut di = 0usize;
        let mut shard_cursor = 0usize;

        for receiver in &receivers {
            let current_count = sn_shards[receiver].len();
            let needed = target_per_sn.saturating_sub(current_count);
            let mut given = 0;

            while given < needed && di < donors.len() {
                let (donor_id, donor_shards) = &donors[di];
                let excess = donor_shards.len()
                    .saturating_sub(target_per_sn)
                    .saturating_sub(shard_cursor);

                if excess == 0 {
                    di += 1;
                    shard_cursor = 0;
                    continue;
                }

                let take = excess.min(needed - given);
                for k in 0..take {
                    let shard_idx = donor_shards.len() - 1 - shard_cursor - k;
                    plans.push(MigrationPlan {
                        shard_id: donor_shards[shard_idx],
                        from_sn:  donor_id.clone(),
                        to_sn:    receiver.clone(),
                    });
                }
                shard_cursor += take;
                given        += take;
            }
        }

        plans
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared::cluster::{NodeInfo, NodeType};

    fn sn(id: &str) -> NodeInfo {
        NodeInfo::new(id, NodeType::StorageNode, "127.0.0.1:9060")
    }

    #[test]
    fn no_rebalance_needed_even_distribution() {
        // 3 SNs, 6 shards, 2 each
        let shards: HashMap<Uuid, String> = (0..6)
            .map(|i| (Uuid::new_v4(), format!("sn-{}", i % 3 + 1)))
            .collect();
        let nodes = vec![sn("sn-1"), sn("sn-2"), sn("sn-3")];
        let plans = RebalancePlanner::plan(&shards, &nodes);
        assert!(plans.is_empty(), "Even distribution should produce no migrations");
    }

    #[test]
    fn rebalance_new_sn_added() {
        // sn-1 has 4 shards, sn-2 has 0 → after rebalance each should have 2
        let ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
        let shards: HashMap<Uuid, String> = ids.iter()
            .map(|id| (*id, "sn-1".to_string()))
            .collect();
        let nodes = vec![sn("sn-1"), sn("sn-2")];
        let plans = RebalancePlanner::plan(&shards, &nodes);
        assert!(!plans.is_empty(), "Should generate migration plans");
        assert!(plans.iter().all(|p| p.from_sn == "sn-1" && p.to_sn == "sn-2"));
    }

    #[test]
    fn empty_eligible_nodes_returns_no_plan() {
        let shards: HashMap<Uuid, String> = (0..4)
            .map(|_| (Uuid::new_v4(), "sn-1".to_string()))
            .collect();
        let plans = RebalancePlanner::plan(&shards, &[]);
        assert!(plans.is_empty());
    }
}
