// T119: WebSession — Raft KV /sessions/{token} CRUD (FR-035)
// UUID v4 토큰 발급, 24시간 TTL
// 모든 QN에서 동일 토큰 검증 가능 (Raft KV 기반 분산 저장)
// QN 재시작 후 세션 유지 (Raft 영속화)

use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::raft::{RaftCommand, RaftManager};

// ─── 세션 TTL ────────────────────────────────────────────────────────────────

const SESSION_TTL_HOURS: i64 = 24;

// ─── WebSession ──────────────────────────────────────────────────────────────

/// 웹 클라이언트 세션 (Raft KV에 저장됨)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSession {
    /// UUID v4 토큰 (URL-safe)
    pub token:      String,
    /// 로그인 사용자명 (Anonymous면 None)
    pub username:   Option<String>,
    /// 세션 생성 시각
    pub created_at: DateTime<Utc>,
    /// 세션 만료 시각 (created_at + 24h, renew 시 갱신)
    pub expires_at: DateTime<Utc>,
    /// 현재 선택된 데이터베이스 (USE <db> 반영)
    pub current_db: Option<String>,
}

impl WebSession {
    /// 새 세션 생성 (UUID v4 토큰 자동 발급)
    pub fn new(username: Option<String>) -> Self {
        let now = Utc::now();
        Self {
            token:      Uuid::new_v4().to_string(),
            username,
            created_at: now,
            expires_at: now + Duration::hours(SESSION_TTL_HOURS),
            current_db: None,
        }
    }

    /// 세션 만료 여부 확인
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }

    /// TTL 리셋 (마지막 활동 기준 24h 연장)
    pub fn renew(&mut self) {
        self.expires_at = Utc::now() + Duration::hours(SESSION_TTL_HOURS);
    }

    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_json(s: &str) -> Result<Self> {
        Ok(serde_json::from_str(s)?)
    }
}

// ─── SessionStore ─────────────────────────────────────────────────────────────

/// Raft KV 기반 분산 세션 저장소
///
/// 키 형식: `/sessions/{token}`
/// - 모든 QN 노드가 동일 Raft 상태 머신을 공유하므로 어떤 QN에서도 동일 토큰 검증 가능
/// - QN 재시작 후에도 Raft에서 세션 복원
pub struct SessionStore {
    raft: Arc<RaftManager>,
}

impl SessionStore {
    pub fn new(raft: Arc<RaftManager>) -> Self {
        Self { raft }
    }

    /// 새 세션 생성 + Raft KV 저장 → 세션 반환
    pub async fn create(&self, username: Option<String>) -> Result<WebSession> {
        let session = WebSession::new(username);
        let key     = session_key(&session.token);
        let value   = session.to_json()?;
        self.raft.write(RaftCommand::UpsertKv { key, value }).await?;
        Ok(session)
    }

    /// 토큰으로 세션 조회
    /// 만료된 세션은 자동 삭제 후 None 반환
    pub async fn get(&self, token: &str) -> Option<WebSession> {
        let json = self.raft.read(&session_key(token)).await?;
        match WebSession::from_json(&json) {
            Ok(s) if s.is_expired() => {
                // 만료 세션 비동기 정리 (best-effort)
                let key = session_key(token);
                let _ = self.raft.write(RaftCommand::DeleteKv { key }).await;
                None
            }
            Ok(s)  => Some(s),
            Err(_) => None,
        }
    }

    /// 세션 유효성 검증 (존재 + 미만료)
    pub async fn validate(&self, token: &str) -> bool {
        self.get(token).await.is_some()
    }

    /// TTL 리셋 (활동 감지 시 호출)
    pub async fn renew(&self, token: &str) -> Result<()> {
        let json = match self.raft.read(&session_key(token)).await {
            Some(j) => j,
            None    => anyhow::bail!("세션 없음: {token}"),
        };
        let mut session = WebSession::from_json(&json)?;
        if session.is_expired() {
            anyhow::bail!("만료된 세션: {token}");
        }
        session.renew();
        self.raft.write(RaftCommand::UpsertKv {
            key:   session_key(token),
            value: session.to_json()?,
        }).await?;
        Ok(())
    }

    /// 세션 삭제 (명시적 로그아웃)
    pub async fn delete(&self, token: &str) -> Result<()> {
        self.raft.write(RaftCommand::DeleteKv {
            key: session_key(token),
        }).await?;
        Ok(())
    }

    /// 현재 데이터베이스 업데이트 (USE <db> 처리)
    pub async fn set_current_db(&self, token: &str, db: String) -> Result<()> {
        let json = match self.raft.read(&session_key(token)).await {
            Some(j) => j,
            None    => anyhow::bail!("세션 없음: {token}"),
        };
        let mut session = WebSession::from_json(&json)?;
        session.current_db = Some(db);
        self.raft.write(RaftCommand::UpsertKv {
            key:   session_key(token),
            value: session.to_json()?,
        }).await?;
        Ok(())
    }
}

fn session_key(token: &str) -> String {
    format!("/sessions/{token}")
}

// ─── 단위 테스트 ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_and_get_session() {
        let raft  = Arc::new(RaftManager::new_local());
        let store = SessionStore::new(raft);

        let session = store.create(Some("alice".to_string())).await.unwrap();
        assert!(!session.is_expired());
        assert_eq!(session.username.as_deref(), Some("alice"));

        let retrieved = store.get(&session.token).await;
        assert!(retrieved.is_some(), "생성된 세션은 조회 가능해야 함");
        assert_eq!(retrieved.unwrap().token, session.token);
    }

    #[tokio::test]
    async fn test_session_not_found() {
        let raft  = Arc::new(RaftManager::new_local());
        let store = SessionStore::new(raft);
        assert!(store.get("nonexistent-token").await.is_none());
    }

    #[tokio::test]
    async fn test_delete_session() {
        let raft  = Arc::new(RaftManager::new_local());
        let store = SessionStore::new(raft);

        let session = store.create(None).await.unwrap();
        store.delete(&session.token).await.unwrap();
        assert!(store.get(&session.token).await.is_none(), "삭제된 세션은 없어야 함");
    }

    #[tokio::test]
    async fn test_set_current_db() {
        let raft  = Arc::new(RaftManager::new_local());
        let store = SessionStore::new(raft);

        let session = store.create(None).await.unwrap();
        store.set_current_db(&session.token, "analytics".to_string()).await.unwrap();

        let updated = store.get(&session.token).await.unwrap();
        assert_eq!(updated.current_db.as_deref(), Some("analytics"));
    }

    #[tokio::test]
    async fn test_validate_valid_session() {
        let raft  = Arc::new(RaftManager::new_local());
        let store = SessionStore::new(raft);

        let session = store.create(Some("bob".to_string())).await.unwrap();
        assert!(store.validate(&session.token).await, "유효한 세션은 validate 통과");
        assert!(!store.validate("bad-token").await, "없는 토큰은 validate 실패");
    }

    #[test]
    fn test_session_json_roundtrip() {
        let session  = WebSession::new(Some("charlie".to_string()));
        let json     = session.to_json().unwrap();
        let restored = WebSession::from_json(&json).unwrap();
        assert_eq!(restored.token, session.token);
        assert_eq!(restored.username.as_deref(), Some("charlie"));
        assert!(!restored.is_expired());
    }

    #[test]
    fn test_new_session_not_expired() {
        let s = WebSession::new(None);
        assert!(!s.is_expired(), "새 세션은 만료되지 않아야 함");
    }
}
