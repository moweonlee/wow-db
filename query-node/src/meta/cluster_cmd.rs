// T177: Cluster command executor (ALTER CLUSTER / SHOW CLUSTER)
// MySQL handler calls these after parsing cluster management SQL.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use tracing::info;

use shared::cluster::{NodeInfo, NodeType};

use crate::meta::cluster_topology::ClusterTopologyManager;
use crate::meta::rebalance::coordinator::{RebalanceCoordinator, RebalanceEvent};

/// High-level cluster management commands called by the MySQL protocol handler.
pub struct ClusterCommandExecutor {
    pub topology:    Arc<ClusterTopologyManager>,
    pub coordinator: Arc<RebalanceCoordinator>,
}

impl ClusterCommandExecutor {
    pub fn new(
        topology:    Arc<ClusterTopologyManager>,
        coordinator: Arc<RebalanceCoordinator>,
    ) -> Self {
        Self { topology, coordinator }
    }

    /// ALTER CLUSTER JOIN <node_id> AS (QUERY|COMPUTE|STORAGE) NODE AT '<addr>'
    pub async fn join_node(
        &self,
        node_id:   &str,
        node_type: NodeType,
        grpc_addr: &str,
    ) -> Result<String> {
        let node = NodeInfo::new(node_id, node_type, grpc_addr);
        self.topology.register_node(node).await?;
        // Notify coordinator to rebalance if a storage node joined
        if node_type == NodeType::StorageNode {
            self.coordinator.event_sender()
                .send(RebalanceEvent::NodeAdded(node_id.to_string()))
                .await
                .ok();
        }
        info!(node_id, ?node_type, grpc_addr, "Node joined cluster");
        Ok(format!("Node '{}' joined cluster as {:?}", node_id, node_type))
    }

    /// ALTER CLUSTER DRAIN '<node_id>'
    /// Sets node state to Readonly → Draining and triggers shard migration.
    pub async fn drain_node(&self, node_id: &str) -> Result<String> {
        self.topology.set_readonly(node_id).await?;
        self.topology.set_draining(node_id).await?;
        self.coordinator.event_sender()
            .send(RebalanceEvent::NodeRemoved(node_id.to_string()))
            .await
            .ok();
        info!(node_id, "Node drain initiated");
        Ok(format!("Node '{}' draining", node_id))
    }

    /// ALTER CLUSTER DISMISS '<node_id>'
    /// Removes node from topology after drain completes.
    pub async fn dismiss_node(&self, node_id: &str) -> Result<String> {
        let node = self.topology.get_node(node_id).await
            .ok_or_else(|| anyhow!("Node '{}' not found", node_id))?;
        if node.state != shared::cluster::NodeState::Draining {
            return Err(anyhow!(
                "Node '{}' must be in DRAINING state before DISMISS (current: {})",
                node_id, node.state
            ));
        }
        self.topology.remove_node(node_id).await?;
        info!(node_id, "Node dismissed from cluster");
        Ok(format!("Node '{}' dismissed", node_id))
    }

    /// ALTER CLUSTER REBALANCE — manual rebalance trigger
    pub async fn manual_rebalance(&self) -> Result<String> {
        self.coordinator.event_sender()
            .send(RebalanceEvent::ManualRebalance)
            .await
            .map_err(|_| anyhow!("RebalanceCoordinator channel closed"))?;
        Ok("Rebalance triggered".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_executor() -> ClusterCommandExecutor {
        let topology    = Arc::new(ClusterTopologyManager::new());
        let coordinator = Arc::new(RebalanceCoordinator::new(topology.clone()));
        ClusterCommandExecutor::new(topology, coordinator)
    }

    #[tokio::test]
    async fn join_and_drain_and_dismiss() {
        let exec = make_executor();

        exec.join_node("sn-1", NodeType::StorageNode, "127.0.0.1:9060")
            .await.unwrap();
        let n = exec.topology.get_node("sn-1").await.unwrap();
        assert_eq!(n.state, shared::cluster::NodeState::Active);

        exec.drain_node("sn-1").await.unwrap();
        let n = exec.topology.get_node("sn-1").await.unwrap();
        assert_eq!(n.state, shared::cluster::NodeState::Draining);

        exec.dismiss_node("sn-1").await.unwrap();
        assert!(exec.topology.get_node("sn-1").await.is_none());
    }

    #[tokio::test]
    async fn dismiss_active_node_returns_error() {
        let exec = make_executor();
        exec.join_node("sn-1", NodeType::StorageNode, "127.0.0.1:9060")
            .await.unwrap();
        let result = exec.dismiss_node("sn-1").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("DRAINING"));
    }

    #[tokio::test]
    async fn join_node_query_does_not_trigger_rebalance_event() {
        let exec = make_executor();
        // Query node join should succeed without error
        exec.join_node("qn-1", NodeType::QueryNode, "127.0.0.1:9011")
            .await.unwrap();
    }
}
