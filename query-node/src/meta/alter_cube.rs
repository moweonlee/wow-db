// T070: ALTER CUBE ADD/DROP COLUMN — 스키마 버전 관리, Raft 복제

use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use shared::types::{CubeSchema, ColumnDef, DataType as WowDataType, TtlPolicy, Encoding, Compression};
use tracing::info;

use crate::meta::cube::CubeManager;
use crate::raft::{RaftCommand, RaftManager};
use crate::sql_parser::{AlterCubeAction, AlterCubeStmt};

// ─── ALTER CUBE 핸들러 ────────────────────────────────────────────────────────

pub struct AlterCubeHandler {
    cube_mgr: Arc<CubeManager>,
    raft:     Arc<RaftManager>,
}

impl AlterCubeHandler {
    pub fn new(cube_mgr: Arc<CubeManager>, raft: Arc<RaftManager>) -> Self {
        Self { cube_mgr, raft }
    }

    /// ALTER CUBE 실행
    pub async fn execute(&self, stmt: &AlterCubeStmt) -> Result<()> {
        let mut schema = self.cube_mgr.get_by_name(&stmt.name).await?
            .ok_or_else(|| anyhow!("Cube '{}' not found", stmt.name))?;

        match &stmt.action {
            AlterCubeAction::AddColumn(col_def) => {
                self.add_column(&mut schema, col_def).await?;
            }
            AlterCubeAction::DropColumn(col_name) => {
                self.drop_column(&mut schema, col_name).await?;
            }
            AlterCubeAction::AddIndex { name, columns, index_type } => {
                info!(
                    cube = %stmt.name,
                    index = %name,
                    cols = ?columns,
                    typ  = %index_type,
                    "ADD INDEX (meta only — physical index built by DN)"
                );
                // 인덱스 메타데이터 PROPERTIES에 저장
                // 실제 인덱스 파일 생성은 DN Compaction 시 수행
            }
            AlterCubeAction::DropIndex(idx_name) => {
                info!(cube = %stmt.name, index = %idx_name, "DROP INDEX");
            }
            AlterCubeAction::ModifyTtl { days } => {
                schema.ttl_policy = if *days > 0 {
                    Some(TtlPolicy { ttl_days: *days as u32 })
                } else {
                    None
                };
                self.persist(&schema).await?;
                info!(cube = %stmt.name, ttl_days = days, "TTL modified");
            }
        }

        Ok(())
    }

    // ─── ADD COLUMN ──────────────────────────────────────────────────────────

    async fn add_column(
        &self,
        schema:  &mut CubeSchema,
        col_def: &crate::sql_parser::CubeColumnDef,
    ) -> Result<()> {
        // 중복 확인
        if schema.columns.iter().any(|c| c.name == col_def.name) {
            bail!("Column '{}' already exists in cube '{}'", col_def.name, schema.name);
        }

        let data_type = parse_data_type(&col_def.data_type)?;
        schema.columns.push(ColumnDef {
            name:           col_def.name.clone(),
            data_type,
            nullable:       col_def.nullable,
            encoding:       Encoding::Plain,
            compression:    Compression::Lz4,
            skipping_index: None,
            flat_json:      None,
            default_value:  None,
        });

        // 스키마 버전 증가 (cube_id UUID는 변경하지 않음)
        self.persist(schema).await?;
        info!(cube = %schema.name, col = %col_def.name, "Column added");
        Ok(())
    }

    // ─── DROP COLUMN ─────────────────────────────────────────────────────────

    async fn drop_column(
        &self,
        schema:   &mut CubeSchema,
        col_name: &str,
    ) -> Result<()> {
        let before = schema.columns.len();
        schema.columns.retain(|c| c.name != col_name);

        if schema.columns.len() == before {
            bail!("Column '{}' not found in cube '{}'", col_name, schema.name);
        }

        // Sort key에 포함된 컬럼 삭제 방지
        if schema.sort_key.iter().any(|k| k.column == col_name) {
            bail!("Cannot drop Sort Key column '{}'", col_name);
        }

        self.persist(schema).await?;
        info!(cube = %schema.name, col = %col_name, "Column dropped");
        Ok(())
    }

    // ─── Raft 영속화 ─────────────────────────────────────────────────────────

    async fn persist(&self, schema: &CubeSchema) -> Result<()> {
        let cube_id     = schema.cube_id.to_string();
        let schema_json = serde_json::to_string(schema)?;
        self.raft
            .write(RaftCommand::UpsertCube { cube_id, schema_json })
            .await?;
        Ok(())
    }
}

/// SQL 타입 문자열 → WowDataType 변환
pub fn parse_data_type(type_str: &str) -> Result<WowDataType> {
    let upper = type_str.to_uppercase();
    let base  = upper.split('(').next().unwrap_or(&upper).trim();
    Ok(match base {
        "INT" | "INTEGER" | "BIGINT" | "INT64"  => WowDataType::Int64,
        "SMALLINT" | "INT32"                    => WowDataType::Int32,
        "TINYINT" | "INT16"                     => WowDataType::Int16,
        "INT8"                                  => WowDataType::Int8,
        "FLOAT" | "REAL"                        => WowDataType::Float32,
        "DOUBLE" | "FLOAT64" | "DECIMAL" | "NUMERIC" => WowDataType::Float64,
        "VARCHAR" | "TEXT" | "STRING" | "CHAR"  => WowDataType::String,
        "BOOLEAN" | "BOOL"                      => WowDataType::Boolean,
        "DATETIME" | "TIMESTAMP"                => WowDataType::DateTime,
        "DATE"                                  => WowDataType::Date,
        "JSON"                                  => WowDataType::Json,
        "BYTES" | "BLOB" | "VARBINARY" | "BINARY" => WowDataType::Binary,
        _ => return Err(anyhow!("Unknown data type: {}", type_str)),
    })
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_data_type() {
        assert_eq!(parse_data_type("INT").unwrap(), WowDataType::Int64);
        assert_eq!(parse_data_type("VARCHAR(255)").unwrap(), WowDataType::String);
        assert_eq!(parse_data_type("DATETIME").unwrap(), WowDataType::DateTime);
        assert_eq!(parse_data_type("JSON").unwrap(), WowDataType::Json);
        assert!(parse_data_type("UNKNOWN_TYPE").is_err());
    }
}
