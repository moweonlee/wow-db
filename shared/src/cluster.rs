// Cluster management types shared across all node types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Role of a node in the WOW-DB cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NodeType {
    /// Query Node — MySQL protocol, SQL parsing, CBO, Raft, Web UI
    QueryNode,
    /// Compute Node — SIMD executor, analytics functions, shuffle
    ComputeNode,
    /// Storage Node — LSM-Tree storage, columnar files, WAL, compaction
    StorageNode,
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::QueryNode   => write!(f, "QUERY_NODE"),
            NodeType::ComputeNode => write!(f, "COMPUTE_NODE"),
            NodeType::StorageNode => write!(f, "STORAGE_NODE"),
        }
    }
}

/// Lifecycle state of a cluster node.
///
/// ```text
/// Active ──► Readonly ──► Draining ──► (removed)
///   ▲                         │
///   └─────────────────────────┘ (re-join)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NodeState {
    /// Node is healthy and accepting read/write traffic.
    Active,
    /// Node is healthy but not accepting new writes (pre-drain).
    Readonly,
    /// Node is draining — migrating shards away; no new traffic.
    Draining,
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeState::Active   => write!(f, "ACTIVE"),
            NodeState::Readonly => write!(f, "READONLY"),
            NodeState::Draining => write!(f, "DRAINING"),
        }
    }
}

/// Full description of a cluster node as stored in the topology registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique node identifier (e.g. `qn-1`, `cn-local-1`, `sn-3`)
    pub node_id:    String,
    /// Role of this node
    pub node_type:  NodeType,
    /// Current lifecycle state
    pub state:      NodeState,
    /// Host:port for gRPC / internal communication
    pub grpc_addr:  String,
    /// Optional host:port for MySQL wire protocol (Query Nodes only)
    pub mysql_addr: Option<String>,
    /// Optional host:port for HTTP health / Stream Load (Storage Nodes)
    pub http_addr:  Option<String>,
    /// Timestamp when this node first registered with the cluster
    pub registered_at: DateTime<Utc>,
    /// Timestamp of the last heartbeat received from this node
    pub last_heartbeat: DateTime<Utc>,
}

impl NodeInfo {
    /// Create a new `NodeInfo` with `Active` state and current timestamps.
    pub fn new(
        node_id:   impl Into<String>,
        node_type: NodeType,
        grpc_addr: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            node_id:        node_id.into(),
            node_type,
            state:          NodeState::Active,
            grpc_addr:      grpc_addr.into(),
            mysql_addr:     None,
            http_addr:      None,
            registered_at:  now,
            last_heartbeat: now,
        }
    }

    /// Returns `true` if the node is eligible to receive new shard assignments.
    pub fn is_eligible_for_placement(&self) -> bool {
        self.state == NodeState::Active
    }

    /// Returns `true` if the node is a storage node eligible for data placement.
    pub fn is_active_storage(&self) -> bool {
        self.node_type == NodeType::StorageNode && self.state == NodeState::Active
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_type_display() {
        assert_eq!(NodeType::QueryNode.to_string(),   "QUERY_NODE");
        assert_eq!(NodeType::ComputeNode.to_string(), "COMPUTE_NODE");
        assert_eq!(NodeType::StorageNode.to_string(), "STORAGE_NODE");
    }

    #[test]
    fn node_state_display() {
        assert_eq!(NodeState::Active.to_string(),   "ACTIVE");
        assert_eq!(NodeState::Readonly.to_string(), "READONLY");
        assert_eq!(NodeState::Draining.to_string(), "DRAINING");
    }

    #[test]
    fn node_info_default_state() {
        let n = NodeInfo::new("sn-1", NodeType::StorageNode, "127.0.0.1:9060");
        assert_eq!(n.state, NodeState::Active);
        assert!(n.is_active_storage());
        assert!(n.is_eligible_for_placement());
    }

    #[test]
    fn draining_node_not_eligible() {
        let mut n = NodeInfo::new("sn-2", NodeType::StorageNode, "127.0.0.1:9060");
        n.state = NodeState::Draining;
        assert!(!n.is_eligible_for_placement());
        assert!(!n.is_active_storage());
    }

    #[test]
    fn serialization_roundtrip() {
        let n = NodeInfo::new("cn-1", NodeType::ComputeNode, "10.0.0.1:9040");
        let json = serde_json::to_string(&n).unwrap();
        let n2: NodeInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(n2.node_id, "cn-1");
        assert_eq!(n2.node_type, NodeType::ComputeNode);
        assert_eq!(n2.state, NodeState::Active);
    }
}
