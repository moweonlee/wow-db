// T176: ALTER CLUSTER / SHOW CLUSTER SQL parser

use anyhow::{bail, Result};
use shared::cluster::NodeType;

/// Parsed cluster management statement
#[derive(Debug, Clone, PartialEq)]
pub enum ClusterStmt {
    /// ALTER CLUSTER JOIN <node_id> AS (QUERY|COMPUTE|STORAGE) NODE AT '<addr>'
    Join {
        node_id:   String,
        node_type: NodeType,
        grpc_addr: String,
    },
    /// ALTER CLUSTER DRAIN '<node_id>'
    Drain { node_id: String },
    /// ALTER CLUSTER DISMISS '<node_id>'
    Dismiss { node_id: String },
    /// ALTER CLUSTER REBALANCE
    Rebalance,
    /// SHOW CLUSTER NODES
    ShowNodes,
    /// SHOW CLUSTER STATUS
    ShowStatus,
}

/// Parse `ALTER CLUSTER ...` or `SHOW CLUSTER ...` statements.
pub fn parse_cluster_stmt(sql: &str) -> Option<Result<ClusterStmt>> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper   = trimmed.to_uppercase();

    if upper.starts_with("ALTER CLUSTER") {
        Some(parse_alter_cluster(trimmed, &upper))
    } else if upper.starts_with("SHOW CLUSTER") {
        Some(parse_show_cluster(&upper))
    } else {
        None
    }
}

fn parse_alter_cluster(sql: &str, upper: &str) -> Result<ClusterStmt> {
    // ALTER CLUSTER JOIN <node_id> AS <TYPE> NODE AT '<addr>'
    if upper.contains("JOIN") {
        return parse_join(sql, upper);
    }
    // ALTER CLUSTER DRAIN '<node_id>'
    if upper.contains("DRAIN") {
        let node_id = extract_quoted_or_bare(sql, "DRAIN")?;
        return Ok(ClusterStmt::Drain { node_id });
    }
    // ALTER CLUSTER DISMISS '<node_id>'
    if upper.contains("DISMISS") {
        let node_id = extract_quoted_or_bare(sql, "DISMISS")?;
        return Ok(ClusterStmt::Dismiss { node_id });
    }
    // ALTER CLUSTER REBALANCE
    if upper.contains("REBALANCE") {
        return Ok(ClusterStmt::Rebalance);
    }
    bail!("Unknown ALTER CLUSTER syntax: {}", &sql[..sql.len().min(80)])
}

fn parse_join(sql: &str, upper: &str) -> Result<ClusterStmt> {
    // ALTER CLUSTER JOIN <node_id> AS (QUERY|COMPUTE|STORAGE) NODE AT '<addr>'
    let after_join = {
        let pos = upper.find("JOIN")
            .ok_or_else(|| anyhow::anyhow!("No JOIN keyword"))?;
        sql[pos + 4..].trim()
    };

    let mut parts = after_join.split_whitespace();
    let node_id = parts.next()
        .ok_or_else(|| anyhow::anyhow!("JOIN: missing node_id"))?
        .trim_matches('\'').trim_matches('"').to_string();

    // AS
    let keyword = parts.next().map(|s| s.to_uppercase());
    if keyword.as_deref() != Some("AS") {
        bail!("JOIN: expected AS, got {:?}", keyword);
    }

    // QUERY | COMPUTE | STORAGE
    let type_str = parts.next()
        .ok_or_else(|| anyhow::anyhow!("JOIN: missing node type"))?
        .to_uppercase();
    let node_type = match type_str.as_str() {
        "QUERY"   => NodeType::QueryNode,
        "COMPUTE" => NodeType::ComputeNode,
        "STORAGE" => NodeType::StorageNode,
        other     => bail!("JOIN: unknown node type '{}'", other),
    };

    // NODE
    let node_kw = parts.next().map(|s| s.to_uppercase());
    if node_kw.as_deref() != Some("NODE") {
        bail!("JOIN: expected NODE keyword, got {:?}", node_kw);
    }

    // AT
    let at_kw = parts.next().map(|s| s.to_uppercase());
    if at_kw.as_deref() != Some("AT") {
        bail!("JOIN: expected AT keyword, got {:?}", at_kw);
    }

    // '<addr>'
    let addr_raw = parts.next()
        .ok_or_else(|| anyhow::anyhow!("JOIN: missing address"))?;
    let grpc_addr = addr_raw.trim_matches('\'').trim_matches('"').to_string();

    Ok(ClusterStmt::Join { node_id, node_type, grpc_addr })
}

fn parse_show_cluster(upper: &str) -> Result<ClusterStmt> {
    if upper.contains("STATUS") {
        return Ok(ClusterStmt::ShowStatus);
    }
    if upper.contains("NODES") || upper.contains("NODE") {
        return Ok(ClusterStmt::ShowNodes);
    }
    // Default: SHOW CLUSTER → SHOW CLUSTER NODES
    Ok(ClusterStmt::ShowNodes)
}

/// Extract the token after `keyword`, stripping quotes.
fn extract_quoted_or_bare(sql: &str, keyword: &str) -> Result<String> {
    let upper = sql.to_uppercase();
    let pos = upper.find(keyword)
        .ok_or_else(|| anyhow::anyhow!("Keyword '{}' not found", keyword))?;
    let rest = sql[pos + keyword.len()..].trim();
    let token = rest.split_whitespace().next()
        .ok_or_else(|| anyhow::anyhow!("Expected token after '{}'", keyword))?
        .trim_matches('\'').trim_matches('"')
        .trim_end_matches(';').to_string();
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_join_query_node() {
        let sql = "ALTER CLUSTER JOIN 'qn-2' AS QUERY NODE AT '10.0.0.2:9011'";
        let stmt = parse_cluster_stmt(sql).unwrap().unwrap();
        assert_eq!(stmt, ClusterStmt::Join {
            node_id:   "qn-2".into(),
            node_type: NodeType::QueryNode,
            grpc_addr: "10.0.0.2:9011".into(),
        });
    }

    #[test]
    fn parse_join_storage_node() {
        let sql = "ALTER CLUSTER JOIN sn-4 AS STORAGE NODE AT '10.0.0.4:9060'";
        let stmt = parse_cluster_stmt(sql).unwrap().unwrap();
        assert_eq!(stmt, ClusterStmt::Join {
            node_id:   "sn-4".into(),
            node_type: NodeType::StorageNode,
            grpc_addr: "10.0.0.4:9060".into(),
        });
    }

    #[test]
    fn parse_drain() {
        let sql = "ALTER CLUSTER DRAIN 'sn-2'";
        let stmt = parse_cluster_stmt(sql).unwrap().unwrap();
        assert_eq!(stmt, ClusterStmt::Drain { node_id: "sn-2".into() });
    }

    #[test]
    fn parse_dismiss() {
        let sql = "ALTER CLUSTER DISMISS sn-2";
        let stmt = parse_cluster_stmt(sql).unwrap().unwrap();
        assert_eq!(stmt, ClusterStmt::Dismiss { node_id: "sn-2".into() });
    }

    #[test]
    fn parse_rebalance() {
        let sql = "ALTER CLUSTER REBALANCE";
        let stmt = parse_cluster_stmt(sql).unwrap().unwrap();
        assert_eq!(stmt, ClusterStmt::Rebalance);
    }

    #[test]
    fn parse_show_nodes() {
        let sql = "SHOW CLUSTER NODES";
        let stmt = parse_cluster_stmt(sql).unwrap().unwrap();
        assert_eq!(stmt, ClusterStmt::ShowNodes);
    }

    #[test]
    fn parse_show_status() {
        let sql = "SHOW CLUSTER STATUS";
        let stmt = parse_cluster_stmt(sql).unwrap().unwrap();
        assert_eq!(stmt, ClusterStmt::ShowStatus);
    }

    #[test]
    fn non_cluster_sql_returns_none() {
        assert!(parse_cluster_stmt("SELECT 1").is_none());
        assert!(parse_cluster_stmt("CREATE TABLE t (id INT)").is_none());
    }

    #[test]
    fn unknown_node_type_returns_error() {
        let sql = "ALTER CLUSTER JOIN x AS BANANA NODE AT 'addr'";
        let result = parse_cluster_stmt(sql).unwrap();
        assert!(result.is_err());
    }
}
