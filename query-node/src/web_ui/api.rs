// T083: Web UI REST API — Cube 목록, 컬럼 목록, Cube 생성 엔드포인트

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use shared::types::{
    CubeSchema, ColumnDef, DataType, Distribution, Encoding, Compression,
    PartitionKey, PartitionVariant, PartitionGranularity,
};
use tracing::info;

use super::server::WebUiState;

// ─── 응답 타입 ────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct CubeListResponse {
    pub cubes: Vec<CubeSummary>,
    pub count: usize,
}

#[derive(Debug, Serialize)]
pub struct CubeSummary {
    pub name:        String,
    pub database:    String,
    pub column_count: usize,
    pub storage_backend: String,
}

#[derive(Debug, Serialize)]
pub struct CubeDetailResponse {
    pub name:     String,
    pub database: String,
    pub columns:  Vec<ColumnSummary>,
    pub sort_key: Vec<String>,
    pub distribution_col: String,
    pub bucket_count: u32,
}

#[derive(Debug, Serialize)]
pub struct ColumnSummary {
    pub name:      String,
    pub data_type: String,
    pub nullable:  bool,
}

#[derive(Debug, Deserialize)]
pub struct CreateCubeRequest {
    pub name:          String,
    pub database:      String,
    pub columns:       Vec<ColumnRequest>,
    pub partition_col: String,
    pub sort_key:      Vec<String>,
    pub dist_col:      String,
    pub bucket_count:  u32,
}

#[derive(Debug, Deserialize)]
pub struct ColumnRequest {
    pub name:      String,
    pub data_type: String,
    pub nullable:  bool,
}

// ─── 핸들러 ──────────────────────────────────────────────────────────────────

/// GET /api/cubes — Cube 목록 반환
pub async fn list_cubes(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.cube_mgr.list().await {
        Ok(cubes) => {
            let summaries: Vec<_> = cubes.iter().map(|c| CubeSummary {
                name:            c.name.clone(),
                database:        c.database.clone(),
                column_count:    c.columns.len(),
                storage_backend: format!("{:?}", c.storage_backend),
            }).collect();
            let count = summaries.len();
            (StatusCode::OK, Json(CubeListResponse { cubes: summaries, count })).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ).into_response(),
    }
}

/// GET /api/cubes/:name — 특정 Cube 상세 정보
pub async fn get_cube(
    Path(name):    Path<String>,
    State(state):  State<WebUiState>,
) -> impl IntoResponse {
    match state.cube_mgr.get_by_name(&name).await {
        Ok(Some(cube)) => {
            let resp = CubeDetailResponse {
                name:     cube.name.clone(),
                database: cube.database.clone(),
                columns:  cube.columns.iter().map(|c| ColumnSummary {
                    name:      c.name.clone(),
                    data_type: format!("{:?}", c.data_type),
                    nullable:  c.nullable,
                }).collect(),
                sort_key: cube.sort_key.iter().map(|k| k.column.clone()).collect(),
                distribution_col: cube.distribution.column.clone(),
                bucket_count:     cube.distribution.bucket_count,
            };
            (StatusCode::OK, Json(resp)).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("Cube '{}' not found", name) })),
        ).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ).into_response(),
    }
}

/// POST /api/cubes — Cube 생성
pub async fn create_cube(
    State(state):  State<WebUiState>,
    Json(req):     Json<CreateCubeRequest>,
) -> impl IntoResponse {
    // 컬럼 변환
    let columns: Vec<ColumnDef> = req.columns.iter().map(|c| ColumnDef {
        name:           c.name.clone(),
        data_type:      parse_data_type_simple(&c.data_type),
        nullable:       c.nullable,
        encoding:       Encoding::Plain,
        compression:    Compression::Lz4,
        skipping_index: None,
        flat_json:      None,
        default_value:  None,
    }).collect();

    let partition_key = PartitionKey {
        variant: PartitionVariant::Range {
            column:      req.partition_col.clone(),
            granularity: Some(PartitionGranularity::Month),
        },
        auto_partition: true,
    };

    let sort_key: Vec<shared::types::ColumnRef> = req.sort_key.iter()
        .map(|s| shared::types::ColumnRef::new(s.clone()))
        .collect();

    let distribution = Distribution {
        column:       req.dist_col.clone(),
        bucket_count: req.bucket_count,
    };

    let schema = CubeSchema::new(
        req.name.clone(),
        req.database.clone(),
        columns,
        partition_key,
        sort_key,
        distribution,
    );

    match state.cube_mgr.create(&schema).await {
        Ok(_) => {
            info!(cube = %req.name, "Cube created via Web UI");
            (StatusCode::CREATED, Json(serde_json::json!({
                "status":  "created",
                "cube_id": schema.cube_id.to_string(),
                "name":    req.name,
            }))).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": e.to_string() })),
        ).into_response(),
    }
}

fn parse_data_type_simple(s: &str) -> DataType {
    match s.to_uppercase().as_str() {
        "INT" | "INTEGER" | "BIGINT" => DataType::Int64,
        "FLOAT" | "DOUBLE" | "DECIMAL" => DataType::Float64,
        "VARCHAR" | "TEXT" | "STRING" => DataType::String,
        "BOOLEAN" | "BOOL"            => DataType::Boolean,
        "DATETIME" | "TIMESTAMP"      => DataType::DateTime,
        "DATE"                        => DataType::Date,
        "JSON"                        => DataType::Json,
        "BYTES" | "BLOB"              => DataType::Binary,
        _                             => DataType::String,
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_data_type_simple() {
        assert!(matches!(parse_data_type_simple("INT"),  DataType::Int64));
        assert!(matches!(parse_data_type_simple("JSON"), DataType::Json));
        assert!(matches!(parse_data_type_simple("BLOB"), DataType::Binary));
        assert!(matches!(parse_data_type_simple("UNKNOWN"), DataType::String));
    }
}
