// T173: Cluster Topology Manager
// 노드 레지스트리, 상태 전이, 하트비트 관리

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use chrono::Utc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use shared::cluster::{NodeInfo, NodeState, NodeType};

/// 하트비트 타임아웃: 이 시간 이상 하트비트 없으면 Offline 처리
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);

/// ClusterTopologyManager: 클러스터 전체 노드의 등록/상태/하트비트 관리.
///
/// Query Node Raft 리더가 이 레지스트리를 권위적으로 관리한다.
/// Follower QN들은 Raft 복제를 통해 동기화된 읽기 전용 뷰를 유지한다.
pub struct ClusterTopologyManager {
    /// node_id → NodeInfo (RwLock으로 동시 읽기 허용)
    nodes: Arc<RwLock<HashMap<String, NodeInfo>>>,
}

impl ClusterTopologyManager {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // ── 등록 / 해제 ───────────────────────────────────────────────────────────

    /// 새 노드 등록 (ALTER CLUSTER JOIN 또는 자가 등록)
    pub async fn register_node(&self, node: NodeInfo) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        if nodes.contains_key(&node.node_id) {
            // 재기동 후 재등록은 정상 — 상태/타임스탬프만 갱신
            info!(node_id = %node.node_id, "Node re-registered");
        } else {
            info!(
                node_id   = %node.node_id,
                node_type = %node.node_type,
                grpc_addr = %node.grpc_addr,
                "Node registered"
            );
        }
        nodes.insert(node.node_id.clone(), node);
        Ok(())
    }

    /// 노드 제거 (ALTER CLUSTER DISMISS 완료 후 호출)
    pub async fn remove_node(&self, node_id: &str) -> Result<NodeInfo> {
        let mut nodes = self.nodes.write().await;
        nodes.remove(node_id)
            .ok_or_else(|| anyhow!("Node '{}' not found in topology", node_id))
    }

    // ── 상태 전이 ─────────────────────────────────────────────────────────────

    /// 노드를 Readonly 상태로 전환 (DRAIN 준비 단계)
    pub async fn set_readonly(&self, node_id: &str) -> Result<()> {
        self.transition(node_id, NodeState::Readonly).await
    }

    /// 노드를 Draining 상태로 전환 (shard 이전 진행 중)
    pub async fn set_draining(&self, node_id: &str) -> Result<()> {
        self.transition(node_id, NodeState::Draining).await
    }

    /// 노드를 Active 상태로 전환 (재활성화)
    pub async fn set_active(&self, node_id: &str) -> Result<()> {
        self.transition(node_id, NodeState::Active).await
    }

    async fn transition(&self, node_id: &str, new_state: NodeState) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        let node = nodes.get_mut(node_id)
            .ok_or_else(|| anyhow!("Node '{}' not found", node_id))?;
        let old = node.state;
        node.state = new_state;
        info!(
            node_id    = %node_id,
            old_state  = %old,
            new_state  = %new_state,
            "Node state transition"
        );
        Ok(())
    }

    // ── 하트비트 ─────────────────────────────────────────────────────────────

    /// 노드의 마지막 하트비트 시각 갱신
    pub async fn heartbeat(&self, node_id: &str) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        let node = nodes.get_mut(node_id)
            .ok_or_else(|| anyhow!("Node '{}' not found — heartbeat ignored", node_id))?;
        node.last_heartbeat = Utc::now();
        Ok(())
    }

    /// 하트비트 타임아웃 노드를 Offline 표시 (주기적으로 QN 리더가 호출)
    pub async fn evict_stale_nodes(&self) -> Vec<String> {
        let mut nodes = self.nodes.write().await;
        let mut evicted = Vec::new();
        for (id, node) in nodes.iter_mut() {
            let age = Utc::now()
                .signed_duration_since(node.last_heartbeat)
                .to_std()
                .unwrap_or(Duration::MAX);
            if age > HEARTBEAT_TIMEOUT && node.state == NodeState::Active {
                warn!(
                    node_id = %id,
                    age_secs = age.as_secs(),
                    "Node heartbeat timeout — marking Draining"
                );
                node.state = NodeState::Draining;
                evicted.push(id.clone());
            }
        }
        evicted
    }

    // ── 조회 ─────────────────────────────────────────────────────────────────

    /// 특정 노드 조회
    pub async fn get_node(&self, node_id: &str) -> Option<NodeInfo> {
        self.nodes.read().await.get(node_id).cloned()
    }

    /// 전체 노드 목록
    pub async fn list_nodes(&self) -> Vec<NodeInfo> {
        self.nodes.read().await.values().cloned().collect()
    }

    /// 특정 타입의 Active 노드만 조회
    pub async fn active_nodes_of_type(&self, node_type: NodeType) -> Vec<NodeInfo> {
        self.nodes.read().await
            .values()
            .filter(|n| n.node_type == node_type && n.state == NodeState::Active)
            .cloned()
            .collect()
    }

    /// Shard 배치 가능한 Storage Node 목록 (Active SN만)
    pub async fn eligible_storage_nodes(&self) -> Vec<NodeInfo> {
        self.active_nodes_of_type(NodeType::StorageNode).await
    }

    /// 클러스터 요약 통계
    pub async fn topology_summary(&self) -> TopologySummary {
        let nodes = self.nodes.read().await;
        let mut summary = TopologySummary::default();
        for node in nodes.values() {
            match node.node_type {
                NodeType::QueryNode   => summary.query_nodes   += 1,
                NodeType::ComputeNode => summary.compute_nodes += 1,
                NodeType::StorageNode => summary.storage_nodes += 1,
            }
            match node.state {
                NodeState::Active   => summary.active_count   += 1,
                NodeState::Readonly => summary.readonly_count += 1,
                NodeState::Draining => summary.draining_count += 1,
            }
        }
        summary.total = nodes.len();
        summary
    }
}

impl Default for ClusterTopologyManager {
    fn default() -> Self { Self::new() }
}

/// 클러스터 통계 요약 (SHOW CLUSTER STATUS 에서 사용)
#[derive(Debug, Default, Clone)]
pub struct TopologySummary {
    pub total:         usize,
    pub query_nodes:   usize,
    pub compute_nodes: usize,
    pub storage_nodes: usize,
    pub active_count:  usize,
    pub readonly_count:usize,
    pub draining_count:usize,
}

// ─── 단위 테스트 ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use shared::cluster::NodeInfo;

    fn sn(id: &str) -> NodeInfo {
        NodeInfo::new(id, NodeType::StorageNode, format!("127.0.0.1:{}", 9060))
    }

    fn qn(id: &str) -> NodeInfo {
        NodeInfo::new(id, NodeType::QueryNode, format!("127.0.0.1:{}", 9011))
    }

    #[tokio::test]
    async fn register_and_list() {
        let mgr = ClusterTopologyManager::new();
        mgr.register_node(sn("sn-1")).await.unwrap();
        mgr.register_node(sn("sn-2")).await.unwrap();
        mgr.register_node(qn("qn-1")).await.unwrap();

        let all = mgr.list_nodes().await;
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn state_transition() {
        let mgr = ClusterTopologyManager::new();
        mgr.register_node(sn("sn-1")).await.unwrap();

        mgr.set_readonly("sn-1").await.unwrap();
        let n = mgr.get_node("sn-1").await.unwrap();
        assert_eq!(n.state, NodeState::Readonly);

        mgr.set_draining("sn-1").await.unwrap();
        let n = mgr.get_node("sn-1").await.unwrap();
        assert_eq!(n.state, NodeState::Draining);
    }

    #[tokio::test]
    async fn remove_node() {
        let mgr = ClusterTopologyManager::new();
        mgr.register_node(sn("sn-1")).await.unwrap();
        mgr.remove_node("sn-1").await.unwrap();
        assert!(mgr.get_node("sn-1").await.is_none());
    }

    #[tokio::test]
    async fn remove_nonexistent_returns_error() {
        let mgr = ClusterTopologyManager::new();
        assert!(mgr.remove_node("ghost").await.is_err());
    }

    #[tokio::test]
    async fn eligible_storage_nodes_filters_draining() {
        let mgr = ClusterTopologyManager::new();
        mgr.register_node(sn("sn-1")).await.unwrap();
        mgr.register_node(sn("sn-2")).await.unwrap();
        mgr.set_draining("sn-2").await.unwrap();

        let eligible = mgr.eligible_storage_nodes().await;
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].node_id, "sn-1");
    }

    #[tokio::test]
    async fn topology_summary() {
        let mgr = ClusterTopologyManager::new();
        mgr.register_node(sn("sn-1")).await.unwrap();
        mgr.register_node(sn("sn-2")).await.unwrap();
        mgr.register_node(qn("qn-1")).await.unwrap();
        mgr.set_draining("sn-2").await.unwrap();

        let s = mgr.topology_summary().await;
        assert_eq!(s.total,          3);
        assert_eq!(s.storage_nodes,  2);
        assert_eq!(s.query_nodes,    1);
        assert_eq!(s.active_count,   2);
        assert_eq!(s.draining_count, 1);
    }

    #[tokio::test]
    async fn heartbeat_unknown_node_returns_error() {
        let mgr = ClusterTopologyManager::new();
        assert!(mgr.heartbeat("ghost").await.is_err());
    }
}
