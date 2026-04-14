// T088: SHOW TABLES, SHOW DATABASES, DESCRIBE, INFORMATION_SCHEMA 가상 테이블
// T138: SHOW PARTITIONS / SHARDS / PARTS / DISTRIBUTED STATUS — PartitionInfoService 연동
// T147: EXPLAIN / EXPLAIN VERBOSE / EXPLAIN COSTS — FR-047

use std::sync::Arc;

use opensrv_mysql::ColumnType;

use super::handler::{ColumnMeta, QueryOutput};
use crate::meta::cube::CubeManager;
use crate::meta::partition_info::PartitionInfoService;
use crate::planner::explain::{explain_sql, ExplainMode};

/// 스키마 관련 명령 처리 — 해당하면 Some(QueryOutput), 아니면 None
///
/// `partition_svc`: SHOW PARTITIONS/SHARDS/PARTS/DISTRIBUTED STATUS에서 실제 데이터를
/// 제공하는 서비스. `None`이면 컬럼 헤더만 반환하고 빈 행셋을 반환한다 (스텁 모드).
pub async fn handle_schema_command(
    sql:           &str,
    cube_mgr:      &Arc<CubeManager>,
    current_db:    &str,
    partition_svc: Option<&PartitionInfoService>,
) -> Option<QueryOutput> {
    let trimmed = sql.trim();
    let upper   = trimmed.to_uppercase();

    // USE database
    if upper.starts_with("USE ") {
        return Some(QueryOutput::Affected(0));
    }

    // SET commands (클라이언트 초기화 등)
    if upper.starts_with("SET ") {
        return Some(QueryOutput::Affected(0));
    }

    // ── EXPLAIN [VERBOSE|COSTS] <sql> (FR-047) ─────────────────────────────
    if upper.starts_with("EXPLAIN ") || upper == "EXPLAIN" {
        // "EXPLAIN" 단어를 제거한 나머지 부분
        let after_explain = trimmed
            .trim_start_matches(|c: char| !c.is_whitespace())
            .trim_start();
        let upper_after = after_explain.to_uppercase();
        let (mode, inner_sql) = if upper_after.starts_with("VERBOSE ") {
            (ExplainMode::Verbose, after_explain[7..].trim())
        } else if upper_after.starts_with("COSTS ") {
            (ExplainMode::Costs, after_explain[6..].trim())
        } else {
            (ExplainMode::Basic, after_explain)
        };
        return Some(explain_sql(inner_sql, mode));
    }

    // SHOW DATABASES
    if upper == "SHOW DATABASES" || upper == "SHOW DATABASES;" {
        return Some(QueryOutput::Rows {
            columns: vec![ColumnMeta { name: "Database".to_string(), col_type: ColumnType::MYSQL_TYPE_VAR_STRING }],
            rows:    vec![
                vec![Some("default".to_string())],
                vec![Some("information_schema".to_string())],
            ],
        });
    }

    // SHOW TABLES [FROM db]
    if upper.starts_with("SHOW TABLES") {
        let cubes = cube_mgr.list().await.unwrap_or_default();
        let col_header = format!("Tables_in_{}", current_db);
        let rows = cubes.iter()
            .map(|c| vec![Some(c.name.clone())])
            .collect();
        return Some(QueryOutput::Rows {
            columns: vec![ColumnMeta { name: col_header, col_type: ColumnType::MYSQL_TYPE_VAR_STRING }],
            rows,
        });
    }

    // SHOW CREATE TABLE / CUBE
    if upper.starts_with("SHOW CREATE TABLE") || upper.starts_with("SHOW CREATE CUBE") {
        let name = extract_last_token(trimmed);
        let ddl  = if let Ok(Some(cube)) = cube_mgr.get_by_name(&name).await {
            format!(
                "CREATE CUBE {} (\n{}\n) DISTRIBUTED BY HASH({}) BUCKETS {}",
                cube.name,
                cube.columns.iter()
                    .map(|c| format!("  {} {:?}{}", c.name, c.data_type, if c.nullable { "" } else { " NOT NULL" }))
                    .collect::<Vec<_>>()
                    .join(",\n"),
                cube.distribution.column,
                cube.distribution.bucket_count,
            )
        } else {
            format!("-- Cube '{}' not found", name)
        };

        return Some(QueryOutput::Rows {
            columns: vec![
                ColumnMeta { name: "Table".to_string(),       col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                ColumnMeta { name: "Create Table".to_string(), col_type: ColumnType::MYSQL_TYPE_BLOB },
            ],
            rows: vec![vec![Some(name), Some(ddl)]],
        });
    }

    // DESCRIBE / DESC table
    if upper.starts_with("DESCRIBE ") || upper.starts_with("DESC ") {
        let name = extract_last_token(trimmed);
        if let Ok(Some(cube)) = cube_mgr.get_by_name(&name).await {
            let rows = cube.columns.iter().map(|c| vec![
                Some(c.name.clone()),
                Some(format!("{:?}", c.data_type).to_lowercase()),
                Some(if c.nullable { "YES" } else { "NO" }.to_string()),
                Some(String::new()),  // Key
                Some(String::new()),  // Default
                Some(String::new()),  // Extra
            ]).collect();

            return Some(QueryOutput::Rows {
                columns: vec![
                    ColumnMeta { name: "Field".to_string(),   col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                    ColumnMeta { name: "Type".to_string(),    col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                    ColumnMeta { name: "Null".to_string(),    col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                    ColumnMeta { name: "Key".to_string(),     col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                    ColumnMeta { name: "Default".to_string(), col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                    ColumnMeta { name: "Extra".to_string(),   col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                ],
                rows,
            });
        }
        return Some(QueryOutput::Error(format!("Table '{}' not found", name)));
    }

    // SHOW COLUMNS FROM table
    if upper.starts_with("SHOW COLUMNS FROM") || upper.starts_with("SHOW FULL COLUMNS FROM") {
        let name = extract_last_token(trimmed);
        if let Ok(Some(cube)) = cube_mgr.get_by_name(&name).await {
            let rows = cube.columns.iter().map(|c| vec![
                Some(c.name.clone()),
                Some(format!("{:?}", c.data_type).to_lowercase()),
                Some(if c.nullable { "YES" } else { "NO" }.to_string()),
                Some(String::new()),
                Some(String::new()),
                Some(String::new()),
            ]).collect();

            return Some(QueryOutput::Rows {
                columns: vec![
                    ColumnMeta { name: "Field".to_string(),   col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                    ColumnMeta { name: "Type".to_string(),    col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                    ColumnMeta { name: "Null".to_string(),    col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                    ColumnMeta { name: "Key".to_string(),     col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                    ColumnMeta { name: "Default".to_string(), col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                    ColumnMeta { name: "Extra".to_string(),   col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                ],
                rows,
            });
        }
    }

    // SHOW CUBES
    if upper.starts_with("SHOW CUBES") {
        let cubes = cube_mgr.list().await.unwrap_or_default();
        let rows = cubes.iter().map(|c| vec![
            Some(c.name.clone()),
            Some(format!("{}.{}", current_db, c.name)),
            Some(format!("{:?}", c.partition_key)),
            Some(format!("HASH({}), {} buckets", c.distribution.column, c.distribution.bucket_count)),
        ]).collect();
        return Some(QueryOutput::Rows {
            columns: vec![
                ColumnMeta { name: "Cube".to_string(), col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                ColumnMeta { name: "Full_Name".to_string(), col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                ColumnMeta { name: "Partition".to_string(), col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                ColumnMeta { name: "Distribution".to_string(), col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ],
            rows,
        });
    }

    // ── SHOW PARTITIONS FROM <cube> (FR-043) ───────────────────────────────
    // 구문: SHOW PARTITIONS FROM <cube> [WHERE ...] [ORDER BY ...] [LIMIT N]
    if upper.starts_with("SHOW PARTITIONS FROM") || upper.starts_with("SHOW PARTITIONS") && upper.contains(" FROM ") {
        let cube_name = extract_from_token(trimmed, "FROM");

        let columns = vec![
            ColumnMeta { name: "partition_id".to_string(),   col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "range_start".to_string(),    col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "range_end".to_string(),      col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "row_count".to_string(),      col_type: ColumnType::MYSQL_TYPE_LONGLONG },
            ColumnMeta { name: "size_bytes".to_string(),     col_type: ColumnType::MYSQL_TYPE_LONGLONG },
            ColumnMeta { name: "shard_count".to_string(),    col_type: ColumnType::MYSQL_TYPE_LONG },
            ColumnMeta { name: "part_count".to_string(),     col_type: ColumnType::MYSQL_TYPE_LONG },
            ColumnMeta { name: "tier".to_string(),           col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "created_at".to_string(),     col_type: ColumnType::MYSQL_TYPE_DATETIME },
        ];

        let rows = if let Some(svc) = partition_svc {
            // cube_name이 UUID가 아닐 수 있으므로 이름으로 Cube 조회 후 ID 사용
            let cube_id = cube_mgr.get_by_name(&cube_name).await.ok().flatten()
                .map(|c| c.cube_id.to_string())
                .unwrap_or(cube_name.clone());

            svc.list_partitions(&cube_id).await.unwrap_or_default()
                .into_iter()
                .map(|p| vec![
                    Some(p.partition_id),
                    Some(p.range_start),
                    Some(p.range_end),
                    Some(p.row_count.to_string()),
                    Some(p.size_bytes.to_string()),
                    Some(p.shard_count.to_string()),
                    Some(p.part_count.to_string()),
                    Some(p.tier),
                    Some(p.created_at),
                ])
                .collect()
        } else {
            vec![]
        };

        return Some(QueryOutput::Rows { columns, rows });
    }

    // ── SHOW SHARDS FROM <cube> [PARTITION <pid>] (FR-044) ─────────────────
    // 구문: SHOW SHARDS FROM <cube> [PARTITION '<partition_id>'] [WHERE ...]
    if upper.starts_with("SHOW SHARDS FROM") || upper.starts_with("SHOW SHARDS") && upper.contains(" FROM ") {
        let cube_name    = extract_from_token(trimmed, "FROM");
        // Optional PARTITION filter: SHOW SHARDS FROM t PARTITION 'pid'
        let partition_filter = if upper.contains(" PARTITION ") {
            Some(extract_from_token(trimmed, "PARTITION"))
        } else {
            None
        };

        let columns = vec![
            ColumnMeta { name: "shard_id".to_string(),        col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "partition_id".to_string(),    col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "partition_range".to_string(), col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "sn_node_id".to_string(),      col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "sn_endpoint".to_string(),     col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "bucket_id".to_string(),       col_type: ColumnType::MYSQL_TYPE_LONG },
            ColumnMeta { name: "role".to_string(),            col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "state".to_string(),           col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "row_count".to_string(),       col_type: ColumnType::MYSQL_TYPE_LONGLONG },
            ColumnMeta { name: "size_bytes".to_string(),      col_type: ColumnType::MYSQL_TYPE_LONGLONG },
            ColumnMeta { name: "part_count".to_string(),      col_type: ColumnType::MYSQL_TYPE_LONG },
            ColumnMeta { name: "lsn".to_string(),             col_type: ColumnType::MYSQL_TYPE_LONGLONG },
        ];

        let rows = if let Some(svc) = partition_svc {
            let cube_id = cube_mgr.get_by_name(&cube_name).await.ok().flatten()
                .map(|c| c.cube_id.to_string())
                .unwrap_or(cube_name.clone());

            svc.list_shards(&cube_id, partition_filter.as_deref()).await.unwrap_or_default()
                .into_iter()
                .map(|s| vec![
                    Some(s.shard_id),
                    Some(s.partition_id),
                    Some(s.partition_range),
                    Some(s.sn_node_id),
                    Some(s.sn_endpoint),
                    Some(s.bucket_id.to_string()),
                    Some(s.role),
                    Some(s.state),
                    Some(s.row_count.to_string()),
                    Some(s.size_bytes.to_string()),
                    Some(s.part_count.to_string()),
                    Some(s.lsn.to_string()),
                ])
                .collect()
        } else {
            vec![]
        };

        return Some(QueryOutput::Rows { columns, rows });
    }

    // ── SHOW PARTS FROM <cube> / SHOW PARTS ON PARTITION <pid> FROM <cube> (FR-045) ──
    // 구문:
    //   SHOW PARTS FROM <cube> [PARTITION '<pid>'] [SHARD '<sid>'] [WHERE ...]
    //   SHOW PARTS ON PARTITION '<pid>' FROM <cube>
    if upper.starts_with("SHOW PARTS") {
        // Resolve cube name
        let cube_name = if upper.contains(" ON PARTITION ") {
            // "SHOW PARTS ON PARTITION 'pid' FROM cube"
            extract_from_token(trimmed, "FROM")
        } else {
            extract_from_token(trimmed, "FROM")
        };

        // Optional partition filter
        let partition_filter: Option<String> = if upper.contains(" PARTITION ") && !upper.contains(" ON PARTITION ") {
            Some(extract_from_token(trimmed, "PARTITION"))
        } else if upper.contains(" ON PARTITION ") {
            Some(extract_from_token(trimmed, "PARTITION"))
        } else {
            None
        };

        // Optional shard filter
        let shard_filter: Option<String> = if upper.contains(" SHARD ") {
            Some(extract_from_token(trimmed, "SHARD"))
        } else {
            None
        };

        let columns = vec![
            ColumnMeta { name: "part_id".to_string(),          col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "shard_id".to_string(),         col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "partition_id".to_string(),     col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "sn_node_id".to_string(),       col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "level".to_string(),            col_type: ColumnType::MYSQL_TYPE_LONG },
            ColumnMeta { name: "sequence_num".to_string(),     col_type: ColumnType::MYSQL_TYPE_LONGLONG },
            ColumnMeta { name: "row_count".to_string(),        col_type: ColumnType::MYSQL_TYPE_LONGLONG },
            ColumnMeta { name: "size_bytes".to_string(),       col_type: ColumnType::MYSQL_TYPE_LONGLONG },
            ColumnMeta { name: "min_sort_key".to_string(),     col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "max_sort_key".to_string(),     col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "bloom_size_bytes".to_string(), col_type: ColumnType::MYSQL_TYPE_LONGLONG },
            ColumnMeta { name: "created_at".to_string(),       col_type: ColumnType::MYSQL_TYPE_DATETIME },
        ];

        let rows = if let Some(svc) = partition_svc {
            let cube_id = cube_mgr.get_by_name(&cube_name).await.ok().flatten()
                .map(|c| c.cube_id.to_string())
                .unwrap_or(cube_name.clone());

            svc.list_parts(&cube_id, partition_filter.as_deref(), shard_filter.as_deref())
                .await.unwrap_or_default()
                .into_iter()
                .map(|p| vec![
                    Some(p.part_id),
                    Some(p.shard_id),
                    Some(p.partition_id),
                    Some(p.sn_node_id),
                    Some(p.level.to_string()),
                    Some(p.sequence_num.to_string()),
                    Some(p.row_count.to_string()),
                    Some(p.size_bytes.to_string()),
                    Some(p.min_sort_key),
                    Some(p.max_sort_key),
                    Some(p.bloom_size_bytes.to_string()),
                    Some(p.created_at),
                ])
                .collect()
        } else {
            vec![]
        };

        return Some(QueryOutput::Rows { columns, rows });
    }

    // ── SHOW DISTRIBUTED STATUS FROM <cube> (FR-046) ───────────────────────
    if upper.starts_with("SHOW DISTRIBUTED STATUS") || upper.starts_with("SHOW DISTRIBUTED") && upper.contains("STATUS") {
        let cube_name = extract_from_token(trimmed, "FROM");

        let columns = vec![
            ColumnMeta { name: "sn_node_id".to_string(),          col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "sn_endpoint".to_string(),         col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ColumnMeta { name: "shard_count".to_string(),         col_type: ColumnType::MYSQL_TYPE_LONG },
            ColumnMeta { name: "leader_shard_count".to_string(),  col_type: ColumnType::MYSQL_TYPE_LONG },
            ColumnMeta { name: "partition_count".to_string(),     col_type: ColumnType::MYSQL_TYPE_LONG },
            ColumnMeta { name: "row_count".to_string(),           col_type: ColumnType::MYSQL_TYPE_LONGLONG },
            ColumnMeta { name: "size_bytes".to_string(),          col_type: ColumnType::MYSQL_TYPE_LONGLONG },
            ColumnMeta { name: "avg_part_per_shard".to_string(),  col_type: ColumnType::MYSQL_TYPE_FLOAT },
        ];

        let rows = if let Some(svc) = partition_svc {
            let cube_id = cube_mgr.get_by_name(&cube_name).await.ok().flatten()
                .map(|c| c.cube_id.to_string())
                .unwrap_or(cube_name.clone());

            svc.get_distributed_status(&cube_id).await.unwrap_or_default()
                .into_iter()
                .map(|s| vec![
                    Some(s.sn_node_id),
                    Some(s.sn_endpoint),
                    Some(s.shard_count.to_string()),
                    Some(s.leader_shard_count.to_string()),
                    Some(s.partition_count.to_string()),
                    Some(s.row_count.to_string()),
                    Some(s.size_bytes.to_string()),
                    Some(format!("{:.2}", s.avg_part_per_shard)),
                ])
                .collect()
        } else {
            vec![]
        };

        return Some(QueryOutput::Rows { columns, rows });
    }

    // SHOW STATUS / SHOW VARIABLES
    if upper.starts_with("SHOW STATUS") || upper.starts_with("SHOW VARIABLES") ||
       upper.starts_with("SHOW GLOBAL") ||
       (upper.starts_with("SHOW SESSION") && !upper.contains("MATERIALIZED") && !upper.contains("VIEW"))
    {
        return Some(QueryOutput::Rows {
            columns: vec![
                ColumnMeta { name: "Variable_name".to_string(), col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
                ColumnMeta { name: "Value".to_string(),         col_type: ColumnType::MYSQL_TYPE_VAR_STRING },
            ],
            rows: vec![
                vec![Some("version".to_string()),           Some("8.0.0-wowdb".to_string())],
                vec![Some("character_set_client".to_string()), Some("utf8mb4".to_string())],
                vec![Some("max_allowed_packet".to_string()), Some("16777216".to_string())],
            ],
        });
    }

    None
}

fn extract_last_token(sql: &str) -> String {
    sql.split_whitespace()
        .last()
        .unwrap_or("")
        .trim_end_matches(';')
        .trim_matches('`')
        .trim_matches('"')
        .to_string()
}

/// "SHOW PARTITIONS FROM page_events" → "page_events"
/// keyword: "FROM", "PARTITION", "SHARD" 등
fn extract_from_token(sql: &str, keyword: &str) -> String {
    let upper = sql.to_uppercase();
    let kw    = keyword.to_uppercase();
    if let Some(pos) = upper.find(&kw) {
        let after = &sql[pos + kw.len()..];
        return after
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_end_matches(';')
            .trim_matches('`')
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
    }
    String::new()
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RaftManager;

    fn make_cube_mgr() -> Arc<CubeManager> {
        let raft = Arc::new(RaftManager::new_local());
        Arc::new(CubeManager::new(raft))
    }

    #[tokio::test]
    async fn test_show_databases() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW DATABASES", &mgr, "default", None).await.unwrap();
        match out {
            QueryOutput::Rows { rows, .. } => assert!(rows.len() >= 1),
            _ => panic!("Expected Rows"),
        }
    }

    #[tokio::test]
    async fn test_set_command() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SET NAMES utf8mb4", &mgr, "default", None).await.unwrap();
        assert!(matches!(out, QueryOutput::Affected(0)));
    }

    #[tokio::test]
    async fn test_use_command() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("USE analytics", &mgr, "default", None).await.unwrap();
        assert!(matches!(out, QueryOutput::Affected(0)));
    }

    #[tokio::test]
    async fn test_describe_not_found() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("DESCRIBE nonexistent_table", &mgr, "default", None).await.unwrap();
        assert!(matches!(out, QueryOutput::Error(_)));
    }

    #[test]
    fn test_extract_last_token() {
        assert_eq!(extract_last_token("SHOW TABLES FROM analytics"), "analytics");
        assert_eq!(extract_last_token("DESCRIBE `my_table`"), "my_table");
        assert_eq!(extract_last_token("DROP TABLE page_events;"), "page_events");
    }

    // ── T143: SHOW PARTITIONS/SHARDS/PARTS/DISTRIBUTED STATUS 테스트 ───────

    #[tokio::test]
    async fn test_show_partitions_returns_correct_columns() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW PARTITIONS FROM page_events", &mgr, "default", None)
            .await
            .unwrap();
        match out {
            QueryOutput::Rows { columns, rows } => {
                // 9개 컬럼 검증
                assert_eq!(columns.len(), 9, "SHOW PARTITIONS: 9 컬럼");
                let col_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                assert!(col_names.contains(&"partition_id"));
                assert!(col_names.contains(&"range_start"));
                assert!(col_names.contains(&"row_count"));
                assert!(col_names.contains(&"shard_count"));
                assert!(col_names.contains(&"tier"));
                // partition_svc=None → 빈 결과셋
                assert!(rows.is_empty(), "스텁 모드: 빈 결과셋");
            }
            _ => panic!("Expected Rows output"),
        }
    }

    #[tokio::test]
    async fn test_show_shards_returns_correct_columns() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW SHARDS FROM page_events", &mgr, "default", None)
            .await
            .unwrap();
        match out {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(columns.len(), 12, "SHOW SHARDS: 12 컬럼");
                let col_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                assert!(col_names.contains(&"shard_id"));
                assert!(col_names.contains(&"sn_node_id"));
                assert!(col_names.contains(&"bucket_id"));
                assert!(col_names.contains(&"role"));
                assert!(col_names.contains(&"lsn"));
                assert!(rows.is_empty());
            }
            _ => panic!("Expected Rows output"),
        }
    }

    #[tokio::test]
    async fn test_show_parts_returns_correct_columns() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW PARTS FROM page_events", &mgr, "default", None)
            .await
            .unwrap();
        match out {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(columns.len(), 12, "SHOW PARTS: 12 컬럼");
                let col_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                assert!(col_names.contains(&"part_id"));
                assert!(col_names.contains(&"level"));
                assert!(col_names.contains(&"sequence_num"));
                assert!(col_names.contains(&"min_sort_key"));
                assert!(col_names.contains(&"bloom_size_bytes"));
                assert!(rows.is_empty());
            }
            _ => panic!("Expected Rows output"),
        }
    }

    #[tokio::test]
    async fn test_show_parts_on_partition_syntax() {
        let mgr = make_cube_mgr();
        // SHOW PARTS ON PARTITION ... FROM ... 구문
        let out = handle_schema_command(
            "SHOW PARTS ON PARTITION 'part-uuid-001' FROM page_events",
            &mgr,
            "default",
            None,
        ).await;
        // 이 구문은 "SHOW PARTS"로 시작하므로 처리되어야 함
        assert!(out.is_some(), "SHOW PARTS ON PARTITION 구문 처리되어야 함");
    }

    #[tokio::test]
    async fn test_show_distributed_status_returns_correct_columns() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW DISTRIBUTED STATUS FROM page_events", &mgr, "default", None)
            .await
            .unwrap();
        match out {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(columns.len(), 8, "SHOW DISTRIBUTED STATUS: 8 컬럼");
                let col_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                assert!(col_names.contains(&"sn_node_id"));
                assert!(col_names.contains(&"leader_shard_count"));
                assert!(col_names.contains(&"avg_part_per_shard"));
                assert!(rows.is_empty());
            }
            _ => panic!("Expected Rows output"),
        }
    }

    #[test]
    fn test_extract_from_token() {
        assert_eq!(extract_from_token("SHOW PARTITIONS FROM page_events", "FROM"), "page_events");
        assert_eq!(extract_from_token("SHOW SHARDS FROM page_events PARTITION 'abc'", "PARTITION"), "abc");
        assert_eq!(extract_from_token("", "FROM"), "");
    }
}
