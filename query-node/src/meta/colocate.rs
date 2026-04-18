// T099: Colocate Group — 동일 분산 키/버킷 수 Cube 동일 SN 버킷 배치
// 그룹 내 Join은 네트워크 Shuffle 없이 로컬 처리

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::raft::{RaftCommand, RaftManager};

// ─── Colocate Group 정의 ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColocateGroup {
    /// 그룹 이름 (unique)
    pub name:            String,
    /// 분산 키 컬럼 이름 (모든 멤버 Cube가 동일해야 함)
    pub distribution_key: Vec<String>,
    /// 버킷 수 (모든 멤버 Cube가 동일해야 함)
    pub bucket_count:    u32,
    /// 그룹에 속한 Cube 이름 목록
    pub members:         HashSet<String>,
    /// 생성 시각
    pub created_at:      u64,
    /// Colocate 상태 (모든 멤버가 동일한 DN 배치를 갖출 때 true)
    pub stable:          bool,
}

impl ColocateGroup {
    pub fn new(
        name:             String,
        distribution_key: Vec<String>,
        bucket_count:     u32,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            name,
            distribution_key,
            bucket_count,
            members:    HashSet::new(),
            created_at: now,
            stable:     false,
        }
    }
}

// ─── Colocate 검증 결과 ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ColocateCheck {
    /// 두 Cube가 colocate Join 가능한지
    pub can_colocate: bool,
    /// 불가한 경우 이유
    pub reason:       Option<String>,
    /// 공통 그룹 이름 (가능한 경우)
    pub group_name:   Option<String>,
}

// ─── Colocate Group Manager ───────────────────────────────────────────────────

const COLOCATE_PREFIX: &str = "colocate_group/";

pub struct ColocateManager {
    raft: Arc<RaftManager>,
}

impl ColocateManager {
    pub fn new(raft: Arc<RaftManager>) -> Self {
        Self { raft }
    }

    fn raft_key(name: &str) -> String {
        format!("{}{}", COLOCATE_PREFIX, name)
    }

    // ── 그룹 DDL ──────────────────────────────────────────────────────────────

    /// Colocate Group 생성
    pub async fn create_group(
        &self,
        name:             &str,
        distribution_key: Vec<String>,
        bucket_count:     u32,
    ) -> Result<()> {
        let key = Self::raft_key(name);
        if self.raft.read(&key).await.is_some() {
            return Err(anyhow!("Colocate group '{}' already exists", name));
        }

        if distribution_key.is_empty() {
            return Err(anyhow!("Distribution key must have at least one column"));
        }
        if bucket_count == 0 || bucket_count > 65536 {
            return Err(anyhow!("bucket_count must be 1..65536"));
        }

        let group = ColocateGroup::new(name.to_string(), distribution_key, bucket_count);
        let json  = serde_json::to_string(&group)?;
        self.raft.write(RaftCommand::UpsertKv { key, value: json }).await?;
        info!(name, "Colocate group created");
        Ok(())
    }

    /// Colocate Group 삭제
    pub async fn drop_group(&self, name: &str, if_exists: bool) -> Result<()> {
        let key = Self::raft_key(name);
        let json = self.raft.read(&key).await;
        if json.is_none() {
            if if_exists { return Ok(()); }
            return Err(anyhow!("Colocate group '{}' not found", name));
        }

        // 멤버 확인 — 멤버가 있으면 삭제 거부
        if let Some(j) = json {
            let group: ColocateGroup = serde_json::from_str(&j)?;
            if !group.members.is_empty() {
                return Err(anyhow!(
                    "Colocate group '{}' has {} member(s). Remove all members first.",
                    name, group.members.len()
                ));
            }
        }

        self.raft.write(RaftCommand::DeleteKv { key }).await?;
        info!(name, "Colocate group dropped");
        Ok(())
    }

    // ── 멤버 관리 ─────────────────────────────────────────────────────────────

    /// Cube를 Colocate Group에 추가
    pub async fn add_member(
        &self,
        group_name:       &str,
        cube_name:        &str,
        cube_dist_key:    &[&str],
        cube_bucket_count: u32,
    ) -> Result<()> {
        let key  = Self::raft_key(group_name);
        let json = self.raft.read(&key).await
            .ok_or_else(|| anyhow!("Colocate group '{}' not found", group_name))?;
        let mut group: ColocateGroup = serde_json::from_str(&json)?;

        // 분산 키 / 버킷 수 호환성 검사
        if cube_bucket_count != group.bucket_count {
            return Err(anyhow!(
                "Bucket count mismatch: group={}, cube={}",
                group.bucket_count, cube_bucket_count
            ));
        }
        let cube_key_cols: Vec<String> = cube_dist_key.iter().map(|s| s.to_string()).collect();
        if cube_key_cols != group.distribution_key {
            return Err(anyhow!(
                "Distribution key mismatch: group={:?}, cube={:?}",
                group.distribution_key, cube_key_cols
            ));
        }

        group.members.insert(cube_name.to_string());
        group.stable = group.members.len() >= 2; // 2개 이상이면 colocate 의미 있음

        self.raft.write(RaftCommand::UpsertKv {
            key,
            value: serde_json::to_string(&group)?,
        }).await?;

        info!(group = %group_name, cube = %cube_name, "Cube added to colocate group");
        Ok(())
    }

    /// Cube를 Colocate Group에서 제거
    pub async fn remove_member(&self, group_name: &str, cube_name: &str) -> Result<()> {
        let key  = Self::raft_key(group_name);
        let json = self.raft.read(&key).await
            .ok_or_else(|| anyhow!("Colocate group '{}' not found", group_name))?;
        let mut group: ColocateGroup = serde_json::from_str(&json)?;
        group.members.remove(cube_name);
        group.stable = group.members.len() >= 2;
        self.raft.write(RaftCommand::UpsertKv {
            key,
            value: serde_json::to_string(&group)?,
        }).await?;
        info!(group = %group_name, cube = %cube_name, "Cube removed from colocate group");
        Ok(())
    }

    // ── 조회 ──────────────────────────────────────────────────────────────────

    pub async fn get_group(&self, name: &str) -> Result<Option<ColocateGroup>> {
        let key = Self::raft_key(name);
        match self.raft.read(&key).await {
            Some(j) => Ok(Some(serde_json::from_str(&j)?)),
            None    => Ok(None),
        }
    }

    pub async fn list_groups(&self) -> Result<Vec<ColocateGroup>> {
        let pairs = self.raft.scan_prefix(COLOCATE_PREFIX).await;
        let mut groups = vec![];
        for (_, json) in pairs {
            if let Ok(g) = serde_json::from_str::<ColocateGroup>(&json) {
                groups.push(g);
            }
        }
        Ok(groups)
    }

    /// 두 Cube 간 Colocate Join 가능 여부 확인
    pub async fn check_colocate(
        &self,
        cube_a: &str,
        cube_b: &str,
    ) -> ColocateCheck {
        let groups = match self.list_groups().await {
            Ok(g)  => g,
            Err(e) => {
                return ColocateCheck {
                    can_colocate: false,
                    reason:       Some(format!("Failed to list groups: {}", e)),
                    group_name:   None,
                };
            }
        };

        for group in &groups {
            if group.stable
                && group.members.contains(cube_a)
                && group.members.contains(cube_b)
            {
                return ColocateCheck {
                    can_colocate: true,
                    reason:       None,
                    group_name:   Some(group.name.clone()),
                };
            }
        }

        ColocateCheck {
            can_colocate: false,
            reason:       Some(format!(
                "No stable colocate group contains both '{}' and '{}'",
                cube_a, cube_b
            )),
            group_name:   None,
        }
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RaftManager;

    fn make_mgr() -> ColocateManager {
        ColocateManager::new(Arc::new(RaftManager::new_local()))
    }

    #[tokio::test]
    async fn test_create_and_get_group() {
        let mgr = make_mgr();
        mgr.create_group("grp1", vec!["user_id".to_string()], 32).await.unwrap();
        let g = mgr.get_group("grp1").await.unwrap().unwrap();
        assert_eq!(g.bucket_count, 32);
        assert_eq!(g.distribution_key, vec!["user_id"]);
        assert!(!g.stable);
    }

    #[tokio::test]
    async fn test_add_member_key_mismatch() {
        let mgr = make_mgr();
        mgr.create_group("grp", vec!["device_id".to_string()], 16).await.unwrap();
        // 다른 분산 키
        let result = mgr.add_member("grp", "events", &["user_id"], 16).await;
        assert!(result.is_err());
        // 다른 버킷 수
        let result2 = mgr.add_member("grp", "events", &["device_id"], 32).await;
        assert!(result2.is_err());
    }

    #[tokio::test]
    async fn test_add_two_members_becomes_stable() {
        let mgr = make_mgr();
        mgr.create_group("grp", vec!["user_id".to_string()], 8).await.unwrap();
        mgr.add_member("grp", "page_events", &["user_id"], 8).await.unwrap();
        let g = mgr.get_group("grp").await.unwrap().unwrap();
        assert!(!g.stable, "1 member — not stable yet");

        mgr.add_member("grp", "click_events", &["user_id"], 8).await.unwrap();
        let g2 = mgr.get_group("grp").await.unwrap().unwrap();
        assert!(g2.stable, "2 members — stable");
    }

    #[tokio::test]
    async fn test_colocate_check() {
        let mgr = make_mgr();
        mgr.create_group("grp", vec!["user_id".to_string()], 8).await.unwrap();
        mgr.add_member("grp", "A", &["user_id"], 8).await.unwrap();
        mgr.add_member("grp", "B", &["user_id"], 8).await.unwrap();

        let result = mgr.check_colocate("A", "B").await;
        assert!(result.can_colocate);
        assert_eq!(result.group_name.as_deref(), Some("grp"));

        let no_result = mgr.check_colocate("A", "C").await;
        assert!(!no_result.can_colocate);
    }

    #[tokio::test]
    async fn test_drop_with_members_fails() {
        let mgr = make_mgr();
        mgr.create_group("grp", vec!["uid".to_string()], 4).await.unwrap();
        mgr.add_member("grp", "cube_x", &["uid"], 4).await.unwrap();
        assert!(mgr.drop_group("grp", false).await.is_err());

        mgr.remove_member("grp", "cube_x").await.unwrap();
        assert!(mgr.drop_group("grp", false).await.is_ok());
    }

    #[tokio::test]
    async fn test_drop_nonexistent_if_exists() {
        let mgr = make_mgr();
        assert!(mgr.drop_group("nonexistent", true).await.is_ok());
        assert!(mgr.drop_group("nonexistent", false).await.is_err());
    }
}
