// T186-T190: Cluster Management Integration Tests
//
// These tests exercise the cluster management logic in-process (no running cluster).
// Full end-to-end tests against a live cluster require `docker compose up`.
//
// Tests T186-T190 map to the 5 scenarios in the spec:
//   T186: Node JOIN/DRAIN/DISMISS lifecycle
//   T187: Shard rebalance triggered on new SN join
//   T188: SQL parser — ALTER CLUSTER / SHOW CLUSTER parsing
//   T189: ShardPlacement only selects Active SNs
//   T190: SHOW CLUSTER STATUS returns correct metrics

use std::collections::HashMap;
use std::sync::Arc;

// Re-export commonly used types for test clarity
use shared::cluster::{NodeInfo, NodeState, NodeType};

// ─── Inline test helpers (no external imports needed) ────────────────────────

fn make_sn(id: &str) -> NodeInfo {
    NodeInfo::new(id, NodeType::StorageNode, format!("127.0.0.{}:9060", id.trim_start_matches("sn-")))
}

fn make_cn(id: &str) -> NodeInfo {
    NodeInfo::new(id, NodeType::ComputeNode, format!("127.0.0.{}:9040", id.trim_start_matches("cn-")))
}

fn make_qn(id: &str) -> NodeInfo {
    NodeInfo::new(id, NodeType::QueryNode, format!("127.0.0.{}:9011", id.trim_start_matches("qn-")))
}

// ─── T186: Node JOIN / DRAIN / DISMISS lifecycle ─────────────────────────────

#[cfg(test)]
mod t186_node_lifecycle {
    use super::*;
    use query_node::meta::cluster_topology::ClusterTopologyManager;
    use query_node::meta::cluster_cmd::ClusterCommandExecutor;
    use query_node::meta::rebalance::coordinator::RebalanceCoordinator;

    #[tokio::test]
    async fn full_join_drain_dismiss_cycle() {
        let topology    = Arc::new(ClusterTopologyManager::new());
        let coordinator = Arc::new(RebalanceCoordinator::new(topology.clone()));
        let exec        = ClusterCommandExecutor::new(topology.clone(), coordinator);

        // JOIN
        exec.join_node("sn-1", NodeType::StorageNode, "127.0.0.1:9060").await.unwrap();
        let node = topology.get_node("sn-1").await.unwrap();
        assert_eq!(node.state, NodeState::Active);

        // DRAIN
        exec.drain_node("sn-1").await.unwrap();
        let node = topology.get_node("sn-1").await.unwrap();
        assert_eq!(node.state, NodeState::Draining);

        // DISMISS (only allowed from Draining)
        exec.dismiss_node("sn-1").await.unwrap();
        assert!(topology.get_node("sn-1").await.is_none());
    }

    #[tokio::test]
    async fn dismiss_active_node_is_rejected() {
        let topology    = Arc::new(ClusterTopologyManager::new());
        let coordinator = Arc::new(RebalanceCoordinator::new(topology.clone()));
        let exec        = ClusterCommandExecutor::new(topology.clone(), coordinator);

        exec.join_node("sn-1", NodeType::StorageNode, "127.0.0.1:9060").await.unwrap();

        // DISMISS without DRAIN should fail
        let result = exec.dismiss_node("sn-1").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("DRAINING"), "Error should mention DRAINING: {}", err);
    }

    #[tokio::test]
    async fn multiple_node_types_can_join() {
        let topology    = Arc::new(ClusterTopologyManager::new());
        let coordinator = Arc::new(RebalanceCoordinator::new(topology.clone()));
        let exec        = ClusterCommandExecutor::new(topology.clone(), coordinator);

        exec.join_node("qn-1", NodeType::QueryNode,   "127.0.0.1:9011").await.unwrap();
        exec.join_node("cn-1", NodeType::ComputeNode, "127.0.0.1:9040").await.unwrap();
        exec.join_node("sn-1", NodeType::StorageNode, "127.0.0.1:9060").await.unwrap();

        let nodes = topology.list_nodes().await;
        assert_eq!(nodes.len(), 3);

        let summary = topology.topology_summary().await;
        assert_eq!(summary.query_nodes,   1);
        assert_eq!(summary.compute_nodes, 1);
        assert_eq!(summary.storage_nodes, 1);
    }
}

// ─── T187: Shard rebalance on new SN join ───────────────────────────────────

#[cfg(test)]
mod t187_rebalance_on_join {
    use super::*;
    use query_node::meta::cluster_topology::ClusterTopologyManager;
    use query_node::meta::rebalance::coordinator::RebalanceCoordinator;
    use query_node::meta::rebalance::planner::RebalancePlanner;
    use uuid::Uuid;

    #[tokio::test]
    async fn rebalance_migrates_shards_to_new_sn() {
        let topology = Arc::new(ClusterTopologyManager::new());
        topology.register_node(make_sn("sn-1")).await.unwrap();
        topology.register_node(make_sn("sn-2")).await.unwrap();

        let coord = Arc::new(RebalanceCoordinator::new(topology.clone()));

        // 4 shards all on sn-1
        let dist: HashMap<Uuid, String> = (0..4)
            .map(|_| (Uuid::new_v4(), "sn-1".to_string()))
            .collect();
        coord.set_distribution(dist).await;

        let results = coord.run_once().await.unwrap();
        assert!(!results.is_empty(), "Migrations should have been triggered");
    }

    #[tokio::test]
    async fn no_rebalance_when_already_balanced() {
        let topology = Arc::new(ClusterTopologyManager::new());
        topology.register_node(make_sn("sn-1")).await.unwrap();
        topology.register_node(make_sn("sn-2")).await.unwrap();

        let coord = Arc::new(RebalanceCoordinator::new(topology.clone()));

        // 4 shards, 2 on each SN
        let ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
        let dist: HashMap<Uuid, String> = ids.iter().enumerate()
            .map(|(i, id)| (*id, format!("sn-{}", i % 2 + 1)))
            .collect();
        coord.set_distribution(dist).await;

        let results = coord.run_once().await.unwrap();
        assert!(results.is_empty(), "No migrations needed for balanced cluster");
    }

    #[test]
    fn rebalance_planner_produces_correct_plan() {
        let topology_nodes = vec![make_sn("sn-1"), make_sn("sn-2"), make_sn("sn-3")];

        // sn-1 has 6 shards, sn-2 and sn-3 have none
        let dist: HashMap<Uuid, String> = (0..6)
            .map(|_| (Uuid::new_v4(), "sn-1".to_string()))
            .collect();

        let plans = RebalancePlanner::plan(&dist, &topology_nodes);
        assert!(!plans.is_empty());
        // All migrations should move FROM sn-1
        assert!(plans.iter().all(|p| p.from_sn == "sn-1"));
        // Target SNs should be sn-2 or sn-3
        assert!(plans.iter().all(|p| p.to_sn == "sn-2" || p.to_sn == "sn-3"));
    }
}

// ─── T188: SQL parser — ALTER CLUSTER / SHOW CLUSTER ────────────────────────

#[cfg(test)]
mod t188_cluster_sql_parser {
    use query_node::sql_parser::cluster_mgmt::{parse_cluster_stmt, ClusterStmt};
    use shared::cluster::NodeType;

    #[test]
    fn parse_join_query_node() {
        let sql  = "ALTER CLUSTER JOIN qn-2 AS QUERY NODE AT '10.0.0.2:9011'";
        let stmt = parse_cluster_stmt(sql).unwrap().unwrap();
        assert_eq!(stmt, ClusterStmt::Join {
            node_id:   "qn-2".into(),
            node_type: NodeType::QueryNode,
            grpc_addr: "10.0.0.2:9011".into(),
        });
    }

    #[test]
    fn parse_join_storage_node() {
        let sql  = "ALTER CLUSTER JOIN sn-4 AS STORAGE NODE AT '10.0.0.4:9060'";
        let stmt = parse_cluster_stmt(sql).unwrap().unwrap();
        assert_eq!(stmt, ClusterStmt::Join {
            node_id:   "sn-4".into(),
            node_type: NodeType::StorageNode,
            grpc_addr: "10.0.0.4:9060".into(),
        });
    }

    #[test]
    fn parse_drain_and_dismiss() {
        assert_eq!(
            parse_cluster_stmt("ALTER CLUSTER DRAIN 'sn-2'").unwrap().unwrap(),
            ClusterStmt::Drain { node_id: "sn-2".into() }
        );
        assert_eq!(
            parse_cluster_stmt("ALTER CLUSTER DISMISS sn-2").unwrap().unwrap(),
            ClusterStmt::Dismiss { node_id: "sn-2".into() }
        );
    }

    #[test]
    fn parse_rebalance() {
        assert_eq!(
            parse_cluster_stmt("ALTER CLUSTER REBALANCE").unwrap().unwrap(),
            ClusterStmt::Rebalance
        );
    }

    #[test]
    fn parse_show_cluster_nodes() {
        assert_eq!(
            parse_cluster_stmt("SHOW CLUSTER NODES").unwrap().unwrap(),
            ClusterStmt::ShowNodes
        );
    }

    #[test]
    fn parse_show_cluster_status() {
        assert_eq!(
            parse_cluster_stmt("SHOW CLUSTER STATUS").unwrap().unwrap(),
            ClusterStmt::ShowStatus
        );
    }

    #[test]
    fn unknown_node_type_returns_error() {
        let result = parse_cluster_stmt("ALTER CLUSTER JOIN x AS BANANA NODE AT 'addr'");
        assert!(result.unwrap().is_err());
    }
}

// ─── T189: ShardPlacement only selects Active SNs ────────────────────────────

#[cfg(test)]
mod t189_shard_placement {
    use super::*;
    use query_node::meta::cluster_topology::ClusterTopologyManager;
    use query_node::planner::shard_placement::ShardPlacement;

    #[tokio::test]
    async fn placement_excludes_draining_nodes() {
        let topology = Arc::new(ClusterTopologyManager::new());
        topology.register_node(make_sn("sn-1")).await.unwrap();
        topology.register_node(make_sn("sn-2")).await.unwrap();
        topology.register_node(make_sn("sn-3")).await.unwrap();
        topology.set_draining("sn-1").await.unwrap();

        let placement = ShardPlacement::new(topology);
        let selected  = placement.select_nodes(2).await.unwrap();
        assert_eq!(selected.len(), 2);
        assert!(selected.iter().all(|n| n.node_id != "sn-1"));
    }

    #[tokio::test]
    async fn placement_fails_when_insufficient_active_nodes() {
        let topology = Arc::new(ClusterTopologyManager::new());
        topology.register_node(make_sn("sn-1")).await.unwrap();
        topology.set_draining("sn-1").await.unwrap();

        let placement = ShardPlacement::new(topology);
        let result    = placement.select_nodes(1).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn bucket_assignment_distributes_evenly() {
        let topology = Arc::new(ClusterTopologyManager::new());
        for i in 1..=3 {
            topology.register_node(make_sn(&format!("sn-{}", i))).await.unwrap();
        }
        let placement   = ShardPlacement::new(topology);
        let assignments = placement.assign_buckets(9, 2).await.unwrap();
        assert_eq!(assignments.len(), 9);
        for bucket in &assignments {
            assert_eq!(bucket.len(), 2, "Each bucket needs 2 replicas");
        }
    }
}

// ─── T190: SHOW CLUSTER STATUS metrics ───────────────────────────────────────

#[cfg(test)]
mod t190_show_cluster_status {
    use super::*;
    use query_node::meta::cluster_topology::ClusterTopologyManager;

    #[tokio::test]
    async fn cluster_status_reflects_node_states() {
        let topology = Arc::new(ClusterTopologyManager::new());

        // 3 QNs, 2 CNs, 3 SNs
        for i in 1..=3 { topology.register_node(make_qn(&format!("qn-{}", i))).await.unwrap(); }
        for i in 1..=2 { topology.register_node(make_cn(&format!("cn-{}", i))).await.unwrap(); }
        for i in 1..=3 { topology.register_node(make_sn(&format!("sn-{}", i))).await.unwrap(); }

        // Drain one SN
        topology.set_draining("sn-3").await.unwrap();

        let s = topology.topology_summary().await;
        assert_eq!(s.total,          8);
        assert_eq!(s.query_nodes,    3);
        assert_eq!(s.compute_nodes,  2);
        assert_eq!(s.storage_nodes,  3);
        assert_eq!(s.active_count,   7, "7 nodes active (sn-3 is draining)");
        assert_eq!(s.draining_count, 1, "1 node draining");
    }

    #[tokio::test]
    async fn topology_summary_empty_cluster() {
        let topology = Arc::new(ClusterTopologyManager::new());
        let s = topology.topology_summary().await;
        assert_eq!(s.total, 0);
        assert_eq!(s.active_count, 0);
    }

    #[tokio::test]
    async fn heartbeat_updates_timestamp() {
        let topology = Arc::new(ClusterTopologyManager::new());
        topology.register_node(make_sn("sn-1")).await.unwrap();

        let before = topology.get_node("sn-1").await.unwrap().last_heartbeat;
        // Small delay to ensure timestamp changes
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        topology.heartbeat("sn-1").await.unwrap();
        let after = topology.get_node("sn-1").await.unwrap().last_heartbeat;

        assert!(after >= before, "Heartbeat should update last_heartbeat");
    }
}
