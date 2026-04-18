// T084: SMV 마법사 API — User Key 드롭다운, 타임아웃 프리셋, DDL 미리보기 생성, 확정 시 SMV 생성 실행

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::info;

use super::server::WebUiState;
use crate::sql_parser::smv_ddl::{CreateSmvStmt, SessionTimeout, SmvRefreshMode};

// ─── 요청/응답 타입 ───────────────────────────────────────────────────────────

/// POST /api/smv/wizard/preview — DDL 미리보기 생성
#[derive(Debug, Deserialize)]
pub struct SmvPreviewRequest {
    pub source_cube:      String,
    pub mv_name:          Option<String>,   // 없으면 자동 생성
    pub user_key_col:     String,
    pub timeout_minutes:  u64,
    pub refresh_mode:     String,           // "incremental", "manual", "scheduled:<cron>"
}

#[derive(Debug, Serialize)]
pub struct SmvPreviewResponse {
    pub mv_name:    String,
    pub ddl_preview: String,
    /// User Key 컬럼 선택 가능 목록 (소스 Cube 기준)
    pub available_user_keys: Vec<String>,
    /// 타임아웃 프리셋 목록
    pub timeout_presets: Vec<TimeoutPreset>,
}

#[derive(Debug, Serialize, Clone)]
pub struct TimeoutPreset {
    pub label:   String,
    pub minutes: u64,
}

/// POST /api/smv/wizard/create — SMV 생성 확정
#[derive(Debug, Deserialize)]
pub struct SmvCreateRequest {
    pub source_cube:     String,
    pub mv_name:         String,
    pub user_key_col:    String,
    pub timeout_minutes: u64,
    pub refresh_mode:    String,
}

#[derive(Debug, Serialize)]
pub struct SmvCreateResponse {
    pub smv_id:  String,
    pub mv_name: String,
    pub status:  String,
}

// ─── 핸들러 ──────────────────────────────────────────────────────────────────

/// POST /api/smv/wizard/preview
pub async fn preview_smv(
    State(state): State<WebUiState>,
    Json(req):    Json<SmvPreviewRequest>,
) -> impl IntoResponse {
    // Cube 존재 확인 및 컬럼 목록 조회
    let cube = match state.cube_mgr.get_by_name(&req.source_cube).await {
        Ok(Some(c)) => c,
        Ok(None) => return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("Cube '{}' not found", req.source_cube) })),
        ).into_response(),
        Err(e) => return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ).into_response(),
    };

    // MV 이름 자동 생성
    let mv_name = req.mv_name.clone()
        .unwrap_or_else(|| format!("{}_sessions", req.source_cube));

    // User Key 컬럼 후보 (String/Int 타입)
    let available_user_keys: Vec<String> = cube.columns.iter()
        .filter(|c| matches!(c.data_type,
            shared::types::DataType::String |
            shared::types::DataType::Int64  |
            shared::types::DataType::Int32
        ))
        .map(|c| c.name.clone())
        .collect();

    // REFRESH 절
    let refresh_clause = parse_refresh_mode_str(&req.refresh_mode);

    // DDL 미리보기 생성
    let ddl = format!(
        "CREATE SESSION MATERIALIZED VIEW {}\nFROM {}\nUSER KEY {}\nSESSION TIMEOUT {} MINUTES\nREFRESH {};",
        mv_name,
        req.source_cube,
        req.user_key_col,
        req.timeout_minutes,
        refresh_clause,
    );

    let presets = vec![
        TimeoutPreset { label: "5분".to_string(),   minutes: 5   },
        TimeoutPreset { label: "30분".to_string(),  minutes: 30  },
        TimeoutPreset { label: "1시간".to_string(), minutes: 60  },
        TimeoutPreset { label: "4시간".to_string(), minutes: 240 },
    ];

    (StatusCode::OK, Json(SmvPreviewResponse {
        mv_name,
        ddl_preview:         ddl,
        available_user_keys,
        timeout_presets:     presets,
    })).into_response()
}

/// POST /api/smv/wizard/create — SMV 생성 확정
pub async fn create_smv_handler(
    State(state): State<WebUiState>,
    Json(req):    Json<SmvCreateRequest>,
) -> impl IntoResponse {
    let refresh_mode = match req.refresh_mode.to_lowercase().as_str() {
        "incremental"  => SmvRefreshMode::Incremental,
        "manual"       => SmvRefreshMode::Manual,
        s if s.starts_with("scheduled:") => {
            let cron = s.trim_start_matches("scheduled:").to_string();
            SmvRefreshMode::Scheduled { cron_expr: cron }
        }
        _ => SmvRefreshMode::Incremental,
    };

    let stmt = CreateSmvStmt {
        mv_name:         req.mv_name.clone(),
        source_cube:     req.source_cube.clone(),
        user_key_col:    req.user_key_col.clone(),
        session_timeout: SessionTimeout::minutes(req.timeout_minutes),
        refresh_mode,
        if_not_exists:   false,
        auto_name:       false,
    };

    match state.smv_mgr.create(&stmt).await {
        Ok(meta) => {
            info!(
                smv    = %req.mv_name,
                source = %req.source_cube,
                "SMV created via wizard"
            );
            // 구체화 완료 처리 (실제로는 CN 실행 후 비동기 완료)
            let _ = state.smv_mgr.mark_materialized(&req.mv_name).await;

            (StatusCode::CREATED, Json(SmvCreateResponse {
                smv_id:  meta.smv_id,
                mv_name: req.mv_name.clone(),
                status:  "active".to_string(),
            })).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": e.to_string() })),
        ).into_response(),
    }
}

/// GET /api/smv — SMV 목록
pub async fn list_smv_handler(State(state): State<WebUiState>) -> impl IntoResponse {
    let smvs = state.smv_mgr.list().await;
    Json(serde_json::json!({ "smvs": smvs, "count": smvs.len() }))
}

// ─── 헬퍼 ─────────────────────────────────────────────────────────────────────

fn parse_refresh_mode_str(s: &str) -> String {
    match s.to_lowercase().as_str() {
        "incremental" => "INCREMENTAL".to_string(),
        "manual"      => "MANUAL".to_string(),
        other if other.starts_with("scheduled:") => {
            let cron = other.trim_start_matches("scheduled:");
            format!("SCHEDULED '{}'", cron)
        }
        _ => "INCREMENTAL".to_string(),
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_refresh_mode_str() {
        assert_eq!(parse_refresh_mode_str("incremental"), "INCREMENTAL");
        assert_eq!(parse_refresh_mode_str("manual"),      "MANUAL");
        assert_eq!(parse_refresh_mode_str("scheduled:0 * * * *"), "SCHEDULED '0 * * * *'");
    }
}
