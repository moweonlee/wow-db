// Query Node self-registration with peer QNs — StarRocks FE pattern
//
// On startup, QN reads `QN_HTTP_PEERS` (comma-separated "host:http_port")
// and POSTs to /api/v1/nodes/register on every reachable peer QN.
// Also writes self-registration into the local Raft KV.
// Spawns a heartbeat task to re-register every 15 seconds.
//
// Role determination:
//   - NODE_ID matches first entry in QN_HTTP_PEERS or self is QN_LEADER → Leader
//   - Otherwise → Follower

use anyhow::Result;
use tracing::{info, warn};

/// Register this QN with all peer QNs and start a heartbeat.
/// `node_id`    : this QN's node id (e.g. "qn-local-2")
/// `mysql_addr` : this QN's MySQL address shown in the dashboard (e.g. "127.0.0.1:19031")
/// `web_port`   : this QN's own HTTP port (used to expose /api/v1/nodes/register to others)
pub async fn register_with_qn_peers(
    node_id:    String,
    mysql_addr: String,
    web_port:   u16,
) -> Result<()> {
    let peers_raw = match std::env::var("QN_HTTP_PEERS") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            info!("QN_HTTP_PEERS not set — single QN mode, no peer registration");
            return Ok(());
        }
    };

    let peers: Vec<String> = peers_raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if peers.is_empty() {
        return Ok(());
    }

    // Determine role: Leader if our web_port matches the first peer, Follower otherwise.
    // The base cluster always starts QN-1 (18080) as Leader.
    let self_http = format!("127.0.0.1:{}", web_port);
    let role = if peers.first().map(|p| p == &self_http).unwrap_or(false) {
        "Leader"
    } else {
        "Follower"
    };

    info!(
        node_id = %node_id,
        role    = role,
        peers   = peers.len(),
        "QN peer registration starting (StarRocks FE pattern)"
    );

    // Register with all peers (skip self)
    let registered = try_register_all(&node_id, &mysql_addr, role, &peers, &self_http).await;
    if !registered {
        warn!(node_id = %node_id, "All QN peers unreachable — will retry via heartbeat");
    }

    // Background heartbeat every 5 seconds (matches dashboard auto-refresh + SC-002 OFFLINE detection)
    let nid    = node_id.clone();
    let maddr  = mysql_addr.clone();
    let role_s = role.to_string();
    let self_h = self_http.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        interval.tick().await;
        loop {
            interval.tick().await;
            try_register_all(&nid, &maddr, &role_s, &peers, &self_h).await;
        }
    });

    Ok(())
}

async fn try_register_all(
    node_id:    &str,
    mysql_addr: &str,
    role:       &str,
    peers:      &[String],
    self_http:  &str,
) -> bool {
    let mut any_ok = false;
    for peer in peers {
        // Skip registering with ourselves
        if peer == self_http {
            continue;
        }
        match try_register(node_id, mysql_addr, role, peer).await {
            Ok(_) => {
                info!(qn_peer = %peer, node_id = %node_id, role = %role, "QN self-registered with peer");
                any_ok = true;
            }
            Err(e) => {
                warn!(qn_peer = %peer, err = %e, "QN peer registration failed");
            }
        }
    }
    any_ok
}

async fn try_register(
    node_id:    &str,
    mysql_addr: &str,
    role:       &str,
    qn_http:    &str,
) -> Result<()> {
    let url = format!("http://{}/api/v1/nodes/register", qn_http);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;
    client
        .post(&url)
        .json(&serde_json::json!({
            "node_id":   node_id,
            "node_type": "query",
            "address":   mysql_addr,
            "role":      role,
        }))
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("POST {} failed: {}", url, e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_without_peers_is_noop() {
        std::env::remove_var("QN_HTTP_PEERS");
        let result = register_with_qn_peers(
            "qn-test-1".to_string(),
            "127.0.0.1:19030".to_string(),
            18080,
        )
        .await;
        assert!(result.is_ok());
    }
}
