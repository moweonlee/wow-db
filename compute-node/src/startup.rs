// T175: Compute Node self-registration with Query Node (via QN_PEERS env var)
//
// On startup, CN reads `QN_PEERS` (comma-separated `host:grpc_port` list)
// and calls the ClusterService `Join` RPC on the first reachable QN.

use anyhow::Result;
use tracing::{info, warn};

/// Attempt to register this Compute Node with the cluster.
///
/// Reads `QN_PEERS` environment variable (comma-separated `host:port`).
/// Falls back silently in standalone/dev mode when no peers are configured.
pub async fn register_with_cluster(
    node_id:   &str,
    grpc_addr: &str,
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
        peer_count = peers.len(),
        "Registering Compute Node with cluster"
    );

    for peer in &peers {
        match try_register(node_id, grpc_addr, peer).await {
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
        "All QN peers unreachable — Compute Node running without cluster registration"
    );
    Ok(())
}

/// Send JOIN request to a single QN peer.
/// In the current implementation this is a stub; replaced by actual gRPC call in Phase D.
async fn try_register(node_id: &str, grpc_addr: &str, qn_peer: &str) -> Result<()> {
    // TODO (Phase D): Replace stub with actual gRPC ClusterService::Join call
    //
    // let mut client = ClusterServiceClient::connect(format!("http://{}", qn_peer)).await?;
    // client.join(JoinRequest {
    //     node_id:   node_id.to_string(),
    //     node_type: NodeTypeProto::ComputeNode as i32,
    //     grpc_addr: grpc_addr.to_string(),
    // }).await?;

    info!(
        node_id,
        grpc_addr,
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
        // No QN_PEERS set → standalone mode, no error
        std::env::remove_var("QN_PEERS");
        let result = register_with_cluster("cn-1", "127.0.0.1:9040").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn register_with_empty_qn_peers_succeeds() {
        std::env::set_var("QN_PEERS", "");
        let result = register_with_cluster("cn-1", "127.0.0.1:9040").await;
        std::env::remove_var("QN_PEERS");
        assert!(result.is_ok());
    }
}
