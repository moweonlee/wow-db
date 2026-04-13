// T080: SMV 라이프사이클 — 생성/삭제, 갱신 스케줄 관리, 구체화 진행 상태 추적

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{info, warn};
use uuid::Uuid;

use crate::raft::{RaftCommand, RaftManager};
use crate::sql_parser::smv_ddl::{CreateSmvStmt, SessionTimeout, SmvRefreshMode};

// ─── SMV 메타데이터 ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmvMeta {
    pub smv_id:          String,
    pub mv_name:         String,
    pub source_cube:     String,
    pub user_key_col:    String,
    pub session_timeout: u64,      // 초
    pub refresh_mode:    SmvRefreshModeProto,
    pub status:          SmvStatus,
    pub created_at:      i64,      // Unix timestamp
    pub materialized_at: Option<i64>,
    /// 전체 진행률 (0~100)
    pub progress:        u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SmvRefreshModeProto {
    Incremental,
    Manual,
    Scheduled { cron_expr: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SmvStatus {
    /// 생성 중 (초기 구체화 실행)
    Creating,
    /// 활성 (쿼리 가능)
    Active,
    /// 갱신 중
    Refreshing,
    /// 오류 상태
    Error { message: String },
    /// 삭제됨 (메타데이터 Tombstone)
    Dropped,
}

// ─── SMV Manager ─────────────────────────────────────────────────────────────

pub struct SmvManager {
    raft: Arc<RaftManager>,
    /// 진행 중 구체화 작업 추적 (인메모리 — Raft에는 없음)
    in_progress: Arc<RwLock<HashMap<String, MaterializationTask>>>,
}

#[derive(Debug, Clone)]
struct MaterializationTask {
    smv_id:     String,
    started_at: std::time::Instant,
    progress:   u8,
}

impl SmvManager {
    pub fn new(raft: Arc<RaftManager>) -> Self {
        Self {
            raft,
            in_progress: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // ── CREATE SMV ───────────────────────────────────────────────────────────

    pub async fn create(&self, stmt: &CreateSmvStmt) -> Result<SmvMeta> {
        let key = format!("smv:{}", stmt.mv_name);

        // 중복 확인
        if let Some(existing) = self.raft.read(&key).await {
            let meta: SmvMeta = serde_json::from_str(&existing)?;
            if meta.status != SmvStatus::Dropped {
                return Err(anyhow!("SMV '{}' already exists", stmt.mv_name));
            }
        }

        let smv_id = Uuid::new_v4().to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let refresh_mode = match &stmt.refresh_mode {
            SmvRefreshMode::Incremental => SmvRefreshModeProto::Incremental,
            SmvRefreshMode::Manual      => SmvRefreshModeProto::Manual,
            SmvRefreshMode::Scheduled { cron_expr } => {
                SmvRefreshModeProto::Scheduled { cron_expr: cron_expr.clone() }
            }
        };

        let meta = SmvMeta {
            smv_id:          smv_id.clone(),
            mv_name:         stmt.mv_name.clone(),
            source_cube:     stmt.source_cube.clone(),
            user_key_col:    stmt.user_key_col.clone(),
            session_timeout: stmt.session_timeout.seconds,
            refresh_mode,
            status:          SmvStatus::Creating,
            created_at:      now,
            materialized_at: None,
            progress:        0,
        };

        self.persist(&meta).await?;

        // 구체화 작업 시작 기록
        let task = MaterializationTask {
            smv_id:     smv_id.clone(),
            started_at: std::time::Instant::now(),
            progress:   0,
        };
        self.in_progress.write().await.insert(stmt.mv_name.clone(), task);

        info!(
            smv    = %stmt.mv_name,
            source = %stmt.source_cube,
            user_key = %stmt.user_key_col,
            timeout_secs = stmt.session_timeout.seconds,
            "SMV creation started"
        );

        Ok(meta)
    }

    // ── 구체화 완료 보고 ─────────────────────────────────────────────────────

    pub async fn mark_materialized(&self, mv_name: &str) -> Result<()> {
        let mut meta = self.get(mv_name).await?
            .ok_or_else(|| anyhow!("SMV '{}' not found", mv_name))?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        meta.status = SmvStatus::Active;
        meta.materialized_at = Some(now);
        meta.progress = 100;
        self.persist(&meta).await?;

        // 진행 중 작업 제거
        self.in_progress.write().await.remove(mv_name);

        info!(smv = %mv_name, "SMV materialization complete");
        Ok(())
    }

    // ── 진행 상태 갱신 ──────────────────────────────────────────────────────

    pub async fn update_progress(&self, mv_name: &str, progress: u8) -> Result<()> {
        if let Some(task) = self.in_progress.write().await.get_mut(mv_name) {
            task.progress = progress;
        }

        if let Some(mut meta) = self.get(mv_name).await? {
            meta.progress = progress;
            self.persist(&meta).await?;
        }
        Ok(())
    }

    // ── DROP SMV ─────────────────────────────────────────────────────────────

    pub async fn drop(&self, mv_name: &str, if_exists: bool) -> Result<()> {
        match self.get(mv_name).await? {
            None => {
                if if_exists {
                    info!(smv = %mv_name, "DROP SMV IF EXISTS: not found, skipping");
                    return Ok(());
                }
                return Err(anyhow!("SMV '{}' not found", mv_name));
            }
            Some(mut meta) => {
                meta.status = SmvStatus::Dropped;
                self.persist(&meta).await?;

                // Raft KV에서 실제 삭제
                self.raft.write(RaftCommand::DeleteKv {
                    key: format!("smv:{}", mv_name),
                }).await?;

                self.in_progress.write().await.remove(mv_name);
                info!(smv = %mv_name, "SMV dropped");
            }
        }
        Ok(())
    }

    // ── 조회 ─────────────────────────────────────────────────────────────────

    pub async fn get(&self, mv_name: &str) -> Result<Option<SmvMeta>> {
        let key = format!("smv:{}", mv_name);
        match self.raft.read(&key).await {
            None    => Ok(None),
            Some(s) => Ok(Some(serde_json::from_str(&s)?)),
        }
    }

    pub async fn list(&self) -> Vec<SmvMeta> {
        self.raft.scan_prefix("smv:").await
            .into_iter()
            .filter_map(|(_, v)| serde_json::from_str::<SmvMeta>(&v).ok())
            .filter(|m| m.status != SmvStatus::Dropped)
            .collect()
    }

    /// 특정 소스 Cube의 SMV 목록
    pub async fn list_for_cube(&self, cube_name: &str) -> Vec<SmvMeta> {
        self.list().await
            .into_iter()
            .filter(|m| m.source_cube == cube_name)
            .collect()
    }

    /// 스케줄 갱신 대상 SMV 목록 (현재는 스텁 — Phase F에서 cron 스케줄러 연동)
    pub async fn due_for_refresh(&self) -> Vec<SmvMeta> {
        self.list().await
            .into_iter()
            .filter(|m| matches!(m.refresh_mode, SmvRefreshModeProto::Scheduled { .. }))
            .filter(|m| m.status == SmvStatus::Active)
            .collect()
    }

    // ─── 내부 헬퍼 ──────────────────────────────────────────────────────────

    async fn persist(&self, meta: &SmvMeta) -> Result<()> {
        let key  = format!("smv:{}", meta.mv_name);
        let json = serde_json::to_string(meta)?;
        self.raft.write(RaftCommand::UpsertKv { key, value: json }).await?;
        Ok(())
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_parser::smv_ddl::SessionTimeout;

    fn make_stmt(name: &str, source: &str) -> CreateSmvStmt {
        CreateSmvStmt {
            mv_name:         name.to_string(),
            source_cube:     source.to_string(),
            user_key_col:    "device_id".to_string(),
            session_timeout: SessionTimeout::minutes(30),
            refresh_mode:    SmvRefreshMode::Incremental,
            if_not_exists:   false,
            auto_name:       false,
        }
    }

    #[tokio::test]
    async fn test_create_and_get() {
        let raft = Arc::new(crate::raft::RaftManager::new_local());
        let mgr  = SmvManager::new(raft);

        let stmt = make_stmt("page_sessions", "page_events");
        let meta = mgr.create(&stmt).await.unwrap();
        assert_eq!(meta.status, SmvStatus::Creating);
        assert_eq!(meta.progress, 0);

        let got = mgr.get("page_sessions").await.unwrap().unwrap();
        assert_eq!(got.mv_name,     "page_sessions");
        assert_eq!(got.source_cube, "page_events");
    }

    #[tokio::test]
    async fn test_mark_materialized() {
        let raft = Arc::new(crate::raft::RaftManager::new_local());
        let mgr  = SmvManager::new(raft);

        let stmt = make_stmt("smv1", "cube1");
        mgr.create(&stmt).await.unwrap();
        mgr.mark_materialized("smv1").await.unwrap();

        let meta = mgr.get("smv1").await.unwrap().unwrap();
        assert_eq!(meta.status, SmvStatus::Active);
        assert_eq!(meta.progress, 100);
        assert!(meta.materialized_at.is_some());
    }

    #[tokio::test]
    async fn test_drop_if_exists() {
        let raft = Arc::new(crate::raft::RaftManager::new_local());
        let mgr  = SmvManager::new(raft);

        // IF EXISTS: 없어도 오류 없음
        mgr.drop("nonexistent", true).await.unwrap();

        // IF EXISTS 없음: 오류
        assert!(mgr.drop("nonexistent", false).await.is_err());
    }

    #[tokio::test]
    async fn test_list_for_cube() {
        let raft = Arc::new(crate::raft::RaftManager::new_local());
        let mgr  = SmvManager::new(raft);

        mgr.create(&make_stmt("smv_a", "events")).await.unwrap();
        mgr.create(&make_stmt("smv_b", "events")).await.unwrap();
        mgr.create(&make_stmt("smv_c", "other_cube")).await.unwrap();

        let for_events = mgr.list_for_cube("events").await;
        assert_eq!(for_events.len(), 2);
        let for_other = mgr.list_for_cube("other_cube").await;
        assert_eq!(for_other.len(), 1);
    }

    #[tokio::test]
    async fn test_duplicate_creation_fails() {
        let raft = Arc::new(crate::raft::RaftManager::new_local());
        let mgr  = SmvManager::new(raft);

        let stmt = make_stmt("dup_smv", "cube");
        mgr.create(&stmt).await.unwrap();
        assert!(mgr.create(&stmt).await.is_err(), "중복 생성 오류");
    }
}
