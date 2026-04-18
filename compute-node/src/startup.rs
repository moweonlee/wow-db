// Compute Node self-registration with Query Node via HTTP
//
// On startup, CN reads `QN_HTTP_PEERS` (comma-separated `host:http_port`)
// and POSTs to /api/v1/nodes/register on the first reachable QN.
// Then spawns a heartbeat task to re-register every 15 seconds.

use anyhow::Result;
use tracing::{info, warn};

/// Register this CN with QN and spawn a background heartbeat task.
pub async fn register_with_cluster(
    node_id:   &str,
    grpc_addr: &str,
) -> Result<()> {
    let peers_raw = match std::env::var("QN_HTTP_PEERS") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            info!("QN_HTTP_PEERS not set — standalone mode (no cluster registration)");
            return Ok(());
        }
    };

    let peers: Vec<String> = peers_raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let registered = try_register_all(node_id, grpc_addr, &peers).await;
    if !registered {
        warn!(node_id, "All QN peers unreachable — will retry via heartbeat");
    }

    // 백그라운드 heartbeat (15초마다)
    let nid  = node_id.to_string();
    let grpc = grpc_addr.to_string();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(15));
        interval.tick().await;
        loop {
            interval.tick().await;
            try_register_all(&nid, &grpc, &peers).await;
        }
    });

    Ok(())
}

async fn try_register_all(node_id: &str, grpc_addr: &str, peers: &[String]) -> bool {
    for peer in peers {
        if try_register(node_id, grpc_addr, peer).await.is_ok() {
            info!(qn_peer = %peer, node_id, "Self-registration OK");
            return true;
        }
    }
    false
}

async fn try_register(node_id: &str, grpc_addr: &str, qn_http: &str) -> Result<()> {
    let url = format!("http://{}/api/v1/nodes/register", qn_http);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;
    client.post(&url)
        .json(&serde_json::json!({
            "node_id":   node_id,
            "node_type": "compute",
            "address":   grpc_addr,
            "role":      "Worker",
        }))
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("register POST {}: {}", url, e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_without_qn_peers_succeeds() {
        std::env::remove_var("QN_HTTP_PEERS");
        let result = register_with_cluster("cn-1", "127.0.0.1:9040").await;
        assert!(result.is_ok());
    }
}
