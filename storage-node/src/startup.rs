// T175: Storage Node self-registration with Query Node (via QN_PEERS env var)
//
// On startup, SN reads `QN_PEERS` (comma-separated `host:grpc_port` list)
// and calls the ClusterService `Join` RPC on the first reachable QN.

use anyhow::Result;
use tracing::{info, warn};

/// Attempt to register this Storage Node with the cluster.
///
/// Reads `QN_PEERS` environment variable (comma-separated `host:port`).
/// Falls back silently in standalone/dev mode when no peers are configured.
pub async fn register_with_cluster(
    node_id:   &str,
    grpc_addr: &str,
    http_addr: &str,
) -> Result<()> {
    let peers_raw = match std::env::var("QN_PEERS") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            info!("QN_PEERS not set — running in standalone mode (no cluster registration)");
            return Ok(());
        }
    };

    let peers: Vec<&str> = peers_raw.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    info!(
        node_id,
        grpc_addr,
        http_addr,
        peer_count = peers.len(),
        "Registering Storage Node with cluster"
    );

    for peer in &peers {
        match try_register(node_id, grpc_addr, http_addr, peer).await {
            Ok(_) => {
                info!(qn_peer = peer, "Self-registration successful");
                return Ok(());
            }
            Err(e) => {
                warn!(qn_peer = peer, err = %e, "Self-registration attempt failed, trying next peer");
            }
        }
    }

    warn!(
        node_id,
        "All QN peers unreachable — Storage Node running without cluster registration"
    );
    Ok(())
}

/// Send JOIN request to a single QN peer.
/// Stub implementation — replaced by gRPC call in Phase D.
async fn try_register(node_id: &str, grpc_addr: &str, http_addr: &str, qn_peer: &str) -> Result<()> {
    // TODO (Phase D): Replace stub with actual gRPC ClusterService::Join call
    //
    // let mut client = ClusterServiceClient::connect(format!("http://{}", qn_peer)).await?;
    // client.join(JoinRequest {
    //     node_id:   node_id.to_string(),
    //     node_type: NodeTypeProto::StorageNode as i32,
    //     grpc_addr: grpc_addr.to_string(),
    //     http_addr: Some(http_addr.to_string()),
    // }).await?;

    info!(
        node_id,
        grpc_addr,
        http_addr,
        qn_peer,
        "ClusterService::Join stub called (Phase D: replace with real gRPC)"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_without_qn_peers_succeeds() {
        std::env::remove_var("QN_PEERS");
        let result = register_with_cluster("sn-1", "127.0.0.1:9060", "127.0.0.1:8040").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn register_with_qn_peers_stub_succeeds() {
        std::env::set_var("QN_PEERS", "127.0.0.1:9011,127.0.0.2:9011");
        let result = register_with_cluster("sn-1", "127.0.0.1:9060", "127.0.0.1:8040").await;
        std::env::remove_var("QN_PEERS");
        // Stub always returns Ok
        assert!(result.is_ok());
    }
}
