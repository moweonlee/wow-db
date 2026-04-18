// T122: ClusterGuard — 클러스터 Read-Only 상태 관리 (FR-036)
// ClusterReadOnlyState Raft KV 직렬화/역직렬화
// add_reason/remove_reason으로 다중 원인 관리
// check_write_allowed() → ReadOnlyError → MySQL ER_OPTION_PREVENTS_STATEMENT(1290)

use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::raft::{RaftCommand, RaftManager};

// ─── Raft KV 키 ─────────────────────────────────────────────────────────────

pub const READ_ONLY_STATE_KEY: &str = "/cluster/read_only_state";

// ─── 도메인 타입 ────────────────────────────────────────────────────────────

/// 클러스터 Read-Only 모드의 원인
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ReadOnlyReason {
    /// 디스크 용량 초과 (자동 감지)
    DiskFull {
        node_id: String,
        path:    String,
        usage:   f64, // 0.0~1.0
    },
    /// 관리자 수동 설정
    ManualOverride {
        admin:   String,
        comment: Option<String>,
    },
}

impl ReadOnlyReason {
    /// reason 식별 문자열 (로그/메트릭용)
    pub fn label(&self) -> &'static str {
        match self {
            Self::DiskFull { .. }      => "disk_full",
            Self::ManualOverride { .. } => "manual_override",
        }
    }

    /// 이 reason이 같은 노드/경로를 가리키는지 비교 (DiskFull 전용)
    pub fn same_source(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::DiskFull { node_id: n1, path: p1, .. },
                Self::DiskFull { node_id: n2, path: p2, .. },
            ) => n1 == n2 && p1 == p2,
            _ => self == other,
        }
    }
}

/// Raft KV에 저장되는 클러스터 상태
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterReadOnlyState {
    pub enabled:    bool,
    pub reasons:    Vec<ReadOnlyReason>,
    pub entered_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl ClusterReadOnlyState {
    /// reason 추가 (중복 소스 덮어쓰기)
    pub fn add_reason(&mut self, reason: ReadOnlyReason) {
        // 같은 소스(node_id+path)의 기존 항목 제거 후 재삽입 (usage 갱신용)
        self.reasons.retain(|r| !r.same_source(&reason));

        if !self.enabled {
            self.enabled    = true;
            self.entered_at = Some(Utc::now());
        }
        self.reasons.push(reason);
        self.updated_at = Utc::now();
    }

    /// reason 제거 (node_id+path 기준)
    pub fn remove_reason(&mut self, reason: &ReadOnlyReason) {
        self.reasons.retain(|r| !r.same_source(reason));
        if self.reasons.is_empty() {
            self.enabled    = false;
            self.entered_at = None;
            self.updated_at = Utc::now();
        }
    }

    /// JSON 직렬화 (Raft KV 저장용)
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    /// JSON 역직렬화 (Raft KV 조회용)
    pub fn from_json(s: &str) -> Result<Self> {
        Ok(serde_json::from_str(s)?)
    }

    /// DiskFull reason 요약 메시지 (MySQL 오류 메시지용)
    pub fn disk_full_summary(&self) -> String {
        let disk_reasons: Vec<String> = self.reasons.iter()
            .filter_map(|r| {
                if let ReadOnlyReason::DiskFull { node_id, path, usage } = r {
                    Some(format!("{}:{} ({:.1}%)", node_id, path, usage * 100.0))
                } else {
                    None
                }
            })
            .collect();
        if disk_reasons.is_empty() {
            "manual override".to_string()
        } else {
            disk_reasons.join(", ")
        }
    }
}

// ─── ReadOnlyError ───────────────────────────────────────────────────────────

/// Read-Only 모드 쓰기 거부 오류
#[derive(Debug, thiserror::Error)]
pub enum ReadOnlyError {
    #[error("클러스터가 Read-Only 모드입니다: {summary}")]
    ClusterReadOnly {
        reasons: Vec<ReadOnlyReason>,
        summary: String,
    },
}

impl ReadOnlyError {
    /// MySQL ER_OPTION_PREVENTS_STATEMENT(1290) 오류 메시지
    pub fn mysql_message(&self) -> String {
        match self {
            Self::ClusterReadOnly { summary, .. } => {
                format!(
                    "The WOW-DB server is running in read-only mode so it cannot execute this statement. \
                    Disk usage: {}. Admin can override with: SET GLOBAL wowdb_read_only = OFF;",
                    summary
                )
            }
        }
    }

    /// MySQL 오류 코드 (1290 = ER_OPTION_PREVENTS_STATEMENT)
    pub const fn mysql_error_code() -> u16 { 1290 }

    /// MySQL SQL State
    pub const fn mysql_sql_state() -> &'static str { "HY000" }
}

// ─── ClusterGuard ────────────────────────────────────────────────────────────

/// 클러스터 Read-Only 상태 관리자
/// - 로컬 캐시 (Arc<RwLock<ClusterReadOnlyState>>)를 유지
/// - Raft KV에 상태 영속화
/// - 쓰기 경로에서 check_write_allowed() 호출
pub struct ClusterGuard {
    /// 로컬 인메모리 캐시 (Raft 커밋 시 동기적으로 갱신)
    state: Arc<RwLock<ClusterReadOnlyState>>,
    /// Raft 클러스터 인터페이스 (상태 영속화용)
    raft:  Arc<RaftManager>,
}

impl ClusterGuard {
    pub fn new(raft: Arc<RaftManager>) -> Self {
        Self {
            state: Arc::new(RwLock::new(ClusterReadOnlyState::default())),
            raft,
        }
    }

    /// Raft KV에서 상태 로드 (시작 시 복원용)
    pub async fn load_from_raft(&self) -> Result<()> {
        if let Some(json) = self.raft.read(READ_ONLY_STATE_KEY).await {
            match ClusterReadOnlyState::from_json(&json) {
                Ok(state) => {
                    *self.state.write().await = state;
                    info!("ClusterReadOnlyState Raft KV에서 복원됨");
                }
                Err(e) => {
                    warn!(err = %e, "ClusterReadOnlyState 역직렬화 실패 — 기본값 사용");
                }
            }
        }
        Ok(())
    }

    /// 쓰기 허용 여부 확인 (모든 쓰기 경로에서 호출)
    /// Read-Only 모드이면 ReadOnlyError 반환
    pub async fn check_write_allowed(&self) -> std::result::Result<(), ReadOnlyError> {
        let state = self.state.read().await;
        if state.enabled {
            Err(ReadOnlyError::ClusterReadOnly {
                summary: state.disk_full_summary(),
                reasons: state.reasons.clone(),
            })
        } else {
            Ok(())
        }
    }

    /// 현재 상태 스냅샷 반환 (Prometheus 메트릭용)
    pub async fn current_state(&self) -> ClusterReadOnlyState {
        self.state.read().await.clone()
    }

    /// Read-Only 원인 추가 + Raft KV 갱신
    pub async fn add_reason(&self, reason: ReadOnlyReason) -> Result<()> {
        let json = {
            let mut state = self.state.write().await;
            let entered = !state.enabled;
            state.add_reason(reason.clone());
            if entered {
                info!(reason = ?reason, "클러스터 Read-Only 모드 진입");
            }
            state.to_json()?
        };
        self.raft.write(RaftCommand::UpsertKv {
            key:   READ_ONLY_STATE_KEY.to_string(),
            value: json,
        }).await?;
        Ok(())
    }

    /// Read-Only 원인 제거 + Raft KV 갱신
    pub async fn remove_reason(&self, reason: &ReadOnlyReason) -> Result<()> {
        let json = {
            let mut state = self.state.write().await;
            state.remove_reason(reason);
            if !state.enabled {
                info!("클러스터 Read-Only 모드 해제 (모든 원인 제거됨)");
            }
            state.to_json()?
        };
        self.raft.write(RaftCommand::UpsertKv {
            key:   READ_ONLY_STATE_KEY.to_string(),
            value: json,
        }).await?;
        Ok(())
    }

    /// 관리자 수동 Read-Only 진입
    pub async fn set_manual_read_only(&self, admin: String, comment: Option<String>) -> Result<()> {
        self.add_reason(ReadOnlyReason::ManualOverride { admin, comment }).await
    }

    /// 관리자 수동 Read-Only 해제 (DiskFull reason이 없는 경우만 허용)
    /// force=true이면 DiskFull reason이 있어도 강제 해제
    pub async fn clear_manual_read_only(&self, admin: &str, force: bool) -> Result<String> {
        let state = self.state.read().await;
        let has_disk_full = state.reasons.iter()
            .any(|r| matches!(r, ReadOnlyReason::DiskFull { .. }));

        if has_disk_full && !force {
            return Err(anyhow::anyhow!(
                "디스크 DiskFull reason이 존재합니다. 디스크 공간 확보 후 재시도하거나 FORCE 옵션을 사용하세요."
            ));
        }
        drop(state);

        let reason = ReadOnlyReason::ManualOverride {
            admin:   admin.to_string(),
            comment: None,
        };
        self.remove_reason(&reason).await?;
        Ok(format!("Read-Only 모드 해제됨 (admin: {})", admin))
    }
}

// ─── SQL 쓰기 감지 헬퍼 ──────────────────────────────────────────────────────

/// SQL 문이 쓰기 작업인지 판별
/// true이면 ClusterGuard.check_write_allowed() 호출 필요
pub fn is_write_statement(sql: &str) -> bool {
    let upper = sql.trim_start().to_uppercase();
    upper.starts_with("INSERT")
        || upper.starts_with("UPDATE")
        || upper.starts_with("DELETE")
        || upper.starts_with("CREATE")
        || upper.starts_with("ALTER")
        || upper.starts_with("DROP")
        || upper.starts_with("TRUNCATE")
        || upper.starts_with("LOAD")
        || upper.starts_with("REPLACE")
}

// ─── 단위 테스트 ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_disk_full_reason(node: &str, path: &str, usage: f64) -> ReadOnlyReason {
        ReadOnlyReason::DiskFull {
            node_id: node.to_string(),
            path:    path.to_string(),
            usage,
        }
    }

    #[test]
    fn test_state_add_remove_reason() {
        let mut state = ClusterReadOnlyState::default();
        assert!(!state.enabled);

        let r1 = make_disk_full_reason("sn-01", "/data", 0.97);
        state.add_reason(r1.clone());
        assert!(state.enabled);
        assert_eq!(state.reasons.len(), 1);

        let r2 = make_disk_full_reason("sn-02", "/data", 0.96);
        state.add_reason(r2);
        assert_eq!(state.reasons.len(), 2);

        state.remove_reason(&r1);
        assert_eq!(state.reasons.len(), 1);
        assert!(state.enabled); // sn-02가 아직 있으므로

        let r2_ref = make_disk_full_reason("sn-02", "/data", 0.0);
        state.remove_reason(&r2_ref);
        assert!(!state.enabled);
        assert!(state.reasons.is_empty());
    }

    #[test]
    fn test_state_updates_existing_usage() {
        let mut state = ClusterReadOnlyState::default();
        let r1 = make_disk_full_reason("sn-01", "/data", 0.96);
        state.add_reason(r1);

        // 같은 노드/경로에 새 usage로 업데이트
        let r2 = make_disk_full_reason("sn-01", "/data", 0.98);
        state.add_reason(r2);
        assert_eq!(state.reasons.len(), 1, "중복 소스는 덮어써야 함");
        if let ReadOnlyReason::DiskFull { usage, .. } = &state.reasons[0] {
            assert_eq!(*usage, 0.98, "usage가 0.98로 갱신되어야 함");
        }
    }

    #[test]
    fn test_json_roundtrip() {
        let mut state = ClusterReadOnlyState::default();
        state.add_reason(make_disk_full_reason("sn-01", "/data/wowdb", 0.973));
        let json = state.to_json().unwrap();
        let restored = ClusterReadOnlyState::from_json(&json).unwrap();
        assert!(restored.enabled);
        assert_eq!(restored.reasons.len(), 1);
    }

    #[tokio::test]
    async fn test_check_write_allowed_normal() {
        let raft  = Arc::new(RaftManager::new_local());
        let guard = ClusterGuard::new(raft);
        assert!(guard.check_write_allowed().await.is_ok());
    }

    #[tokio::test]
    async fn test_check_write_blocked_in_read_only() {
        let raft  = Arc::new(RaftManager::new_local());
        let guard = ClusterGuard::new(raft);

        guard.add_reason(make_disk_full_reason("sn-01", "/data", 0.96)).await.unwrap();

        let result = guard.check_write_allowed().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(ReadOnlyError::mysql_error_code(), 1290);
        assert!(err.mysql_message().contains("read-only mode"));
    }

    #[tokio::test]
    async fn test_multi_reason_recovery() {
        let raft  = Arc::new(RaftManager::new_local());
        let guard = ClusterGuard::new(raft);

        let r1 = make_disk_full_reason("sn-01", "/data", 0.96);
        let r2 = make_disk_full_reason("sn-02", "/data", 0.97);

        guard.add_reason(r1.clone()).await.unwrap();
        guard.add_reason(r2.clone()).await.unwrap();
        assert!(guard.check_write_allowed().await.is_err());

        guard.remove_reason(&r1).await.unwrap();
        assert!(guard.check_write_allowed().await.is_err(), "sn-02 원인 잔존");

        guard.remove_reason(&r2).await.unwrap();
        assert!(guard.check_write_allowed().await.is_ok(), "모든 원인 제거 후 허용");
    }

    #[test]
    fn test_is_write_statement() {
        assert!(is_write_statement("INSERT INTO events VALUES (...)"));
        assert!(is_write_statement("CREATE CUBE events ..."));
        assert!(is_write_statement("  ALTER CUBE events ..."));
        assert!(!is_write_statement("SELECT * FROM events"));
        assert!(!is_write_statement("SHOW CUBES"));
        assert!(!is_write_statement("DESCRIBE events"));
    }
}
