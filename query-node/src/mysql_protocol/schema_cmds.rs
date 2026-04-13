// T088: SHOW TABLES, SHOW DATABASES, DESCRIBE, INFORMATION_SCHEMA 가상 테이블

use std::sync::Arc;

use opensrv_mysql::ColumnType;

use super::handler::{ColumnMeta, QueryOutput};
use crate::meta::cube::CubeManager;

/// 스키마 관련 명령 처리 — 해당하면 Some(QueryOutput), 아니면 None
pub async fn handle_schema_command(
    sql:      &str,
    cube_mgr: &Arc<CubeManager>,
    current_db: &str,
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
        let out = handle_schema_command("SHOW DATABASES", &mgr, "default").await.unwrap();
        match out {
            QueryOutput::Rows { rows, .. } => assert!(rows.len() >= 1),
            _ => panic!("Expected Rows"),
        }
    }

    #[tokio::test]
    async fn test_set_command() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SET NAMES utf8mb4", &mgr, "default").await.unwrap();
        assert!(matches!(out, QueryOutput::Affected(0)));
    }

    #[tokio::test]
    async fn test_use_command() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("USE analytics", &mgr, "default").await.unwrap();
        assert!(matches!(out, QueryOutput::Affected(0)));
    }

    #[tokio::test]
    async fn test_describe_not_found() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("DESCRIBE nonexistent_table", &mgr, "default").await.unwrap();
        assert!(matches!(out, QueryOutput::Error(_)));
    }

    #[test]
    fn test_extract_last_token() {
        assert_eq!(extract_last_token("SHOW TABLES FROM analytics"), "analytics");
        assert_eq!(extract_last_token("DESCRIBE `my_table`"), "my_table");
        assert_eq!(extract_last_token("DROP TABLE page_events;"), "page_events");
    }
}
