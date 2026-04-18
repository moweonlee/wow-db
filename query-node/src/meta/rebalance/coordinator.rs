// T182: RebalanceCoordinator — 이벤트 기반 리밸런싱 오케스트레이션

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{Mutex, mpsc};
use tracing::{info, warn};
use uuid::Uuid;

use super::migrator::{MigrationState, ShardMigrator};
use super::planner::{MigrationPlan, RebalancePlanner};
use crate::meta::cluster_topology::ClusterTopologyManager;

/// 리밸런싱 이벤트
#[derive(Debug)]
pub enum RebalanceEvent {
    /// 새 노드 추가 후 리밸런싱 트리거
    NodeAdded(String),
    /// 노드 제거 후 리밸런싱 트리거
    NodeRemoved(String),
    /// 수동 리밸런싱 요청 (ALTER CLUSTER REBALANCE)
    ManualRebalance,
    /// 셧다운
    Shutdown,
}

/// RebalanceCoordinator: 이벤트를 받아 리밸런싱 계획을 실행한다.
pub struct RebalanceCoordinator {
    topology:   Arc<ClusterTopologyManager>,
    /// shard_id → sn_node_id (현재 분포; 실제는 ShardMap에서 읽음)
    current_dist: Arc<Mutex<HashMap<Uuid, String>>>,
    event_tx: mpsc::Sender<RebalanceEvent>,
    event_rx: Arc<Mutex<mpsc::Receiver<RebalanceEvent>>>,
}

impl RebalanceCoordinator {
    pub fn new(topology: Arc<ClusterTopologyManager>) -> Self {
        let (tx, rx) = mpsc::channel(64);
        Self {
            topology,
            current_dist: Arc::new(Mutex::new(HashMap::new())),
            event_tx: tx,
            event_rx: Arc::new(Mutex::new(rx)),
        }
    }

    /// 이벤트 송신 채널 반환 (외부에서 이벤트 주입에 사용)
    pub fn event_sender(&self) -> mpsc::Sender<RebalanceEvent> {
        self.event_tx.clone()
    }

    /// 현재 shard 분포 설정 (초기화 / 테스트용)
    pub async fn set_distribution(&self, dist: HashMap<Uuid, String>) {
        *self.current_dist.lock().await = dist;
    }

    /// 리밸런싱 계획 생성 및 실행 (1회)
    pub async fn run_once(&self) -> Result<Vec<MigrationState>> {
        let eligible = self.topology.eligible_storage_nodes().await;
        let dist     = self.current_dist.lock().await.clone();
        let plans    = RebalancePlanner::plan(&dist, &eligible);

        if plans.is_empty() {
            info!("Rebalance: no migrations needed");
            return Ok(Vec::new());
        }

        info!(plan_count = plans.len(), "Rebalance: executing migration plans");
        let mut results = Vec::with_capacity(plans.len());

        for plan in &plans {
            let mut migrator = ShardMigrator::new(
                plan.shard_id,
                plan.from_sn.clone(),
                plan.to_sn.clone(),
            );
            match migrator.run().await {
                Ok(_) => {
                    // 성공 시 분포 갱신
                    self.current_dist.lock().await
                        .insert(plan.shard_id, plan.to_sn.clone());
                    results.push(MigrationState::Done);
                }
                Err(e) => {
                    warn!(
                        shard_id = %plan.shard_id,
                        err      = %e,
                        "Migration failed"
                    );
                    results.push(MigrationState::Failed(e.to_string()));
                }
            }
        }
        Ok(results)
    }

    /// 이벤트 루프 실행 (백그라운드 태스크로 spawn)
    pub async fn run_event_loop(self: Arc<Self>) {
        let mut rx = self.event_rx.lock().await;
        while let Some(event) = rx.recv().await {
            match event {
                RebalanceEvent::Shutdown => {
                    info!("RebalanceCoordinator: shutdown");
                    break;
                }
                RebalanceEvent::NodeAdded(id) => {
                    info!(node_id = %id, "RebalanceCoordinator: node added, triggering rebalance");
                    if let Err(e) = self.run_once().await {
                        warn!(err = %e, "Rebalance error on NodeAdded");
                    }
                }
                RebalanceEvent::NodeRemoved(id) => {
                    info!(node_id = %id, "RebalanceCoordinator: node removed, triggering rebalance");
                    if let Err(e) = self.run_once().await {
                        warn!(err = %e, "Rebalance error on NodeRemoved");
                    }
                }
                RebalanceEvent::ManualRebalance => {
                    info!("RebalanceCoordinator: manual rebalance triggered");
                    if let Err(e) = self.run_once().await {
                        warn!(err = %e, "Rebalance error on ManualRebalance");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared::cluster::{NodeInfo, NodeType};

    fn sn(id: &str) -> NodeInfo {
        NodeInfo::new(id, NodeType::StorageNode, "127.0.0.1:9060")
    }

    #[tokio::test]
    async fn no_migrations_for_balanced_cluster() {
        let topology = Arc::new(ClusterTopologyManager::new());
        topology.register_node(sn("sn-1")).await.unwrap();
        topology.register_node(sn("sn-2")).await.unwrap();

        let coord = RebalanceCoordinator::new(topology);
        // 2 SNs, 4 shards, 2 each
        let dist: HashMap<Uuid, String> = (0..4)
            .map(|i| (Uuid::new_v4(), format!("sn-{}", i % 2 + 1)))
            .collect();
        coord.set_distribution(dist).await;

        let results = coord.run_once().await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn migrations_generated_for_imbalanced_cluster() {
        let topology = Arc::new(ClusterTopologyManager::new());
        topology.register_node(sn("sn-1")).await.unwrap();
        topology.register_node(sn("sn-2")).await.unwrap();

        let coord = RebalanceCoordinator::new(topology);
        // sn-1 has 4 shards, sn-2 has none
        let dist: HashMap<Uuid, String> = (0..4)
            .map(|_| (Uuid::new_v4(), "sn-1".to_string()))
            .collect();
        coord.set_distribution(dist).await;

        let results = coord.run_once().await.unwrap();
        assert!(!results.is_empty());
        assert!(results.iter().all(|s| *s == MigrationState::Done));
    }

    #[tokio::test]
    async fn event_sender_can_send() {
        let topology = Arc::new(ClusterTopologyManager::new());
        let coord    = RebalanceCoordinator::new(topology);
        let tx = coord.event_sender();
        // Sending should not block (channel has capacity 64)
        tx.send(RebalanceEvent::ManualRebalance).await.unwrap();
        tx.send(RebalanceEvent::Shutdown).await.unwrap();
    }
}
