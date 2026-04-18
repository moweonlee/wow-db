// T174: ClusterService gRPC stub
// Full tonic service impl in Phase D when cluster.proto codegen is wired up.
//
// This module provides the in-process API that mirrors what the gRPC handler
// will eventually call. Unit tests cover the logic layer independent of tonic.

use std::sync::Arc;

use anyhow::Result;
use tracing::info;

use shared::cluster::NodeType;
use crate::meta::cluster_topology::ClusterTopologyManager;
use crate::meta::cluster_cmd::ClusterCommandExecutor;
use crate::meta::rebalance::coordinator::RebalanceCoordinator;

/// Request to join a node into the cluster
#[derive(Debug, Clone)]
pub struct JoinRequest {
    pub node_id:   String,
    pub node_type: NodeType,
    pub grpc_addr: String,
    pub http_addr: Option<String>,
}

/// Response from cluster operations
#[derive(Debug, Clone)]
pub struct ClusterResponse {
    pub ok:      bool,
    pub message: String,
}

/// ClusterService: logic layer for cluster management operations.
/// Backed by ClusterCommandExecutor; exposed via gRPC in Phase D.
pub struct ClusterService {
    executor: ClusterCommandExecutor,
}

impl ClusterService {
    pub fn new(
        topology:    Arc<ClusterTopologyManager>,
        coordinator: Arc<RebalanceCoordinator>,
    ) -> Self {
        Self {
            executor: ClusterCommandExecutor::new(topology, coordinator),
        }
    }

    /// Handle JOIN request (from self-registering nodes or ALTER CLUSTER JOIN)
    pub async fn join(&self, req: JoinRequest) -> Result<ClusterResponse> {
        let msg = self.executor.join_node(
            &req.node_id,
            req.node_type,
            &req.grpc_addr,
        ).await?;
        info!(node_id = %req.node_id, "ClusterService::Join");
        Ok(ClusterResponse { ok: true, message: msg })
    }

    /// Handle DRAIN request
    pub async fn drain(&self, node_id: &str) -> Result<ClusterResponse> {
        let msg = self.executor.drain_node(node_id).await?;
        Ok(ClusterResponse { ok: true, message: msg })
    }

    /// Handle DISMISS request
    pub async fn dismiss(&self, node_id: &str) -> Result<ClusterResponse> {
        let msg = self.executor.dismiss_node(node_id).await?;
        Ok(ClusterResponse { ok: true, message: msg })
    }

    /// Handle REBALANCE request
    pub async fn rebalance(&self) -> Result<ClusterResponse> {
        let msg = self.executor.manual_rebalance().await?;
        Ok(ClusterResponse { ok: true, message: msg })
    }

    /// Reference to the topology manager (for SHOW CLUSTER queries)
    pub fn topology(&self) -> &Arc<ClusterTopologyManager> {
        &self.executor.topology
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_service() -> ClusterService {
        let topology    = Arc::new(ClusterTopologyManager::new());
        let coordinator = Arc::new(RebalanceCoordinator::new(topology.clone()));
        ClusterService::new(topology, coordinator)
    }

    #[tokio::test]
    async fn join_returns_ok() {
        let svc = make_service();
        let resp = svc.join(JoinRequest {
            node_id:   "sn-1".to_string(),
            node_type: NodeType::StorageNode,
            grpc_addr: "127.0.0.1:9060".to_string(),
            http_addr: Some("127.0.0.1:8040".to_string()),
        }).await.unwrap();
        assert!(resp.ok);
    }

    #[tokio::test]
    async fn drain_then_dismiss() {
        let svc = make_service();
        svc.join(JoinRequest {
            node_id:   "sn-1".to_string(),
            node_type: NodeType::StorageNode,
            grpc_addr: "127.0.0.1:9060".to_string(),
            http_addr: None,
        }).await.unwrap();

        svc.drain("sn-1").await.unwrap();
        svc.dismiss("sn-1").await.unwrap();

        assert!(svc.topology().get_node("sn-1").await.is_none());
    }
}
