// T093: Resource Group 정책 적용
// CPU/메모리/동시 쿼리 수/타임아웃 제한, 사용자/롤 매핑

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

// ─── Resource Group 정의 ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceGroupConfig {
    /// 그룹 이름 (unique)
    pub name:               String,
    /// CPU 사용률 상한 (0~100%). None = 제한 없음
    pub cpu_limit_pct:      Option<f64>,
    /// 메모리 상한 (bytes). None = 제한 없음
    pub memory_limit_bytes: Option<u64>,
    /// 동시 실행 쿼리 수 상한. None = 제한 없음
    pub max_concurrency:    Option<u32>,
    /// 쿼리 실행 타임아웃. None = 제한 없음
    pub query_timeout:      Option<Duration>,
    /// 이 그룹의 CPU 우선순위 (1 = 최저, 10 = 최고)
    pub priority:           u8,
}

impl Default for ResourceGroupConfig {
    fn default() -> Self {
        Self {
            name:               "default".to_string(),
            cpu_limit_pct:      None,
            memory_limit_bytes: None,
            max_concurrency:    None,
            query_timeout:      None,
            priority:           5,
        }
    }
}

// ─── Resource Group 런타임 상태 ───────────────────────────────────────────────

#[derive(Debug, Default)]
struct GroupState {
    /// 현재 실행 중인 쿼리 수
    active_queries: u32,
    /// 누적 거부된 쿼리 수 (concurrency 초과)
    rejected_total: u64,
}

// ─── Resource Group Manager ───────────────────────────────────────────────────

pub struct ResourceGroupManager {
    inner: Mutex<RgmInner>,
}

struct RgmInner {
    /// 그룹 설정 맵 (name → config)
    groups:    HashMap<String, ResourceGroupConfig>,
    /// 그룹 런타임 상태 맵
    states:    HashMap<String, GroupState>,
    /// 사용자 → 그룹 이름 매핑
    user_map:  HashMap<String, String>,
    /// 롤 → 그룹 이름 매핑
    role_map:  HashMap<String, String>,
}

impl ResourceGroupManager {
    pub fn new() -> Self {
        let mut groups = HashMap::new();
        let mut states = HashMap::new();

        // 기본 그룹 등록
        groups.insert("default".to_string(), ResourceGroupConfig::default());
        states.insert("default".to_string(), GroupState::default());

        // 내부 시스템 그룹 (제한 없음, 최고 우선순위)
        groups.insert("system".to_string(), ResourceGroupConfig {
            name:     "system".to_string(),
            priority: 10,
            ..Default::default()
        });
        states.insert("system".to_string(), GroupState::default());

        Self {
            inner: Mutex::new(RgmInner {
                groups,
                states,
                user_map: HashMap::new(),
                role_map: HashMap::new(),
            }),
        }
    }

    // ── DDL 조작 ──────────────────────────────────────────────────────────────

    /// 새 Resource Group 생성
    pub fn create_group(&self, config: ResourceGroupConfig) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap();
        if inner.groups.contains_key(&config.name) {
            return Err(format!("Resource group '{}' already exists", config.name));
        }
        inner.states.insert(config.name.clone(), GroupState::default());
        inner.groups.insert(config.name.clone(), config);
        Ok(())
    }

    /// Resource Group 설정 변경
    pub fn alter_group(&self, config: ResourceGroupConfig) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.groups.contains_key(&config.name) {
            return Err(format!("Resource group '{}' not found", config.name));
        }
        inner.groups.insert(config.name.clone(), config);
        Ok(())
    }

    /// Resource Group 삭제
    pub fn drop_group(&self, name: &str) -> Result<(), String> {
        if name == "default" || name == "system" {
            return Err(format!("Cannot drop built-in group '{}'", name));
        }
        let mut inner = self.inner.lock().unwrap();
        // 이 그룹에 매핑된 사용자를 default로 이전
        for v in inner.user_map.values_mut() {
            if v == name { *v = "default".to_string(); }
        }
        for v in inner.role_map.values_mut() {
            if v == name { *v = "default".to_string(); }
        }
        inner.groups.remove(name);
        inner.states.remove(name);
        Ok(())
    }

    // ── 사용자/롤 매핑 ────────────────────────────────────────────────────────

    /// 사용자 → 그룹 매핑
    pub fn map_user(&self, user: &str, group: &str) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.groups.contains_key(group) {
            return Err(format!("Group '{}' not found", group));
        }
        inner.user_map.insert(user.to_string(), group.to_string());
        Ok(())
    }

    /// 롤 → 그룹 매핑
    pub fn map_role(&self, role: &str, group: &str) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.groups.contains_key(group) {
            return Err(format!("Group '{}' not found", group));
        }
        inner.role_map.insert(role.to_string(), group.to_string());
        Ok(())
    }

    // ── 쿼리 승인 / 해제 ──────────────────────────────────────────────────────

    /// 사용자의 그룹 이름 반환 (매핑 없으면 "default")
    pub fn resolve_group(&self, user: &str, roles: &[&str]) -> String {
        let inner = self.inner.lock().unwrap();
        // 사용자 직접 매핑 우선
        if let Some(g) = inner.user_map.get(user) {
            return g.clone();
        }
        // 롤 매핑 (첫 번째 일치)
        for role in roles {
            if let Some(g) = inner.role_map.get(*role) {
                return g.clone();
            }
        }
        "default".to_string()
    }

    /// 쿼리 실행 허용 여부 확인 및 카운터 증가
    /// Ok(timeout) = 허용 (타임아웃 있으면 Some), Err = 거부 이유
    pub fn admit(&self, group_name: &str) -> Result<Option<Duration>, String> {
        let mut inner = self.inner.lock().unwrap();
        let config = inner.groups.get(group_name)
            .cloned()
            .ok_or_else(|| format!("Unknown group '{}'", group_name))?;

        let state = inner.states.entry(group_name.to_string())
            .or_insert_with(GroupState::default);

        if let Some(max) = config.max_concurrency {
            if state.active_queries >= max {
                state.rejected_total += 1;
                return Err(format!(
                    "Resource group '{}' concurrency limit ({}) reached",
                    group_name, max
                ));
            }
        }

        state.active_queries += 1;
        Ok(config.query_timeout)
    }

    /// 쿼리 완료 — 카운터 감소
    pub fn release(&self, group_name: &str) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(state) = inner.states.get_mut(group_name) {
            if state.active_queries > 0 {
                state.active_queries -= 1;
            }
        }
    }

    // ── 조회 ──────────────────────────────────────────────────────────────────

    pub fn list_groups(&self) -> Vec<ResourceGroupConfig> {
        let inner = self.inner.lock().unwrap();
        inner.groups.values().cloned().collect()
    }

    pub fn get_group(&self, name: &str) -> Option<ResourceGroupConfig> {
        self.inner.lock().unwrap().groups.get(name).cloned()
    }

    pub fn active_queries(&self, group_name: &str) -> u32 {
        self.inner.lock().unwrap()
            .states.get(group_name)
            .map(|s| s.active_queries)
            .unwrap_or(0)
    }
}

impl Default for ResourceGroupManager {
    fn default() -> Self { Self::new() }
}

// ─── 글로벌 싱글턴 ────────────────────────────────────────────────────────────

use std::sync::LazyLock;

pub static RESOURCE_GROUPS: LazyLock<Arc<ResourceGroupManager>> =
    LazyLock::new(|| Arc::new(ResourceGroupManager::new()));

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_mgr() -> ResourceGroupManager { ResourceGroupManager::new() }

    #[test]
    fn test_default_groups_exist() {
        let mgr = make_mgr();
        assert!(mgr.get_group("default").is_some());
        assert!(mgr.get_group("system").is_some());
    }

    #[test]
    fn test_create_and_drop_group() {
        let mgr = make_mgr();
        mgr.create_group(ResourceGroupConfig {
            name:            "analytics".to_string(),
            max_concurrency: Some(4),
            priority:        7,
            ..Default::default()
        }).unwrap();

        assert!(mgr.get_group("analytics").is_some());
        mgr.drop_group("analytics").unwrap();
        assert!(mgr.get_group("analytics").is_none());
    }

    #[test]
    fn test_cannot_drop_builtin() {
        let mgr = make_mgr();
        assert!(mgr.drop_group("default").is_err());
        assert!(mgr.drop_group("system").is_err());
    }

    #[test]
    fn test_user_mapping_and_resolve() {
        let mgr = make_mgr();
        mgr.create_group(ResourceGroupConfig {
            name: "analysts".to_string(), ..Default::default()
        }).unwrap();
        mgr.map_user("alice", "analysts").unwrap();
        assert_eq!(mgr.resolve_group("alice", &[]), "analysts");
        assert_eq!(mgr.resolve_group("bob", &[]), "default");
    }

    #[test]
    fn test_role_mapping_and_resolve() {
        let mgr = make_mgr();
        mgr.create_group(ResourceGroupConfig {
            name: "reports".to_string(), ..Default::default()
        }).unwrap();
        mgr.map_role("reporter", "reports").unwrap();
        assert_eq!(mgr.resolve_group("carol", &["reporter"]), "reports");
        // 사용자 매핑이 롤보다 우선
        mgr.map_user("carol", "default").unwrap();
        assert_eq!(mgr.resolve_group("carol", &["reporter"]), "default");
    }

    #[test]
    fn test_concurrency_limit() {
        let mgr = make_mgr();
        mgr.create_group(ResourceGroupConfig {
            name:            "limited".to_string(),
            max_concurrency: Some(2),
            ..Default::default()
        }).unwrap();

        assert!(mgr.admit("limited").is_ok());
        assert!(mgr.admit("limited").is_ok());
        assert!(mgr.admit("limited").is_err()); // 3번째 거부

        mgr.release("limited");
        assert!(mgr.admit("limited").is_ok()); // 슬롯 해제 후 허용
    }

    #[test]
    fn test_admit_no_limit() {
        let mgr = make_mgr();
        for _ in 0..100 {
            assert!(mgr.admit("default").is_ok());
        }
        assert_eq!(mgr.active_queries("default"), 100);
    }

    #[test]
    fn test_timeout_returned() {
        let mgr = make_mgr();
        mgr.create_group(ResourceGroupConfig {
            name:          "timed".to_string(),
            query_timeout: Some(Duration::from_secs(30)),
            ..Default::default()
        }).unwrap();

        let timeout = mgr.admit("timed").unwrap();
        assert_eq!(timeout, Some(Duration::from_secs(30)));
    }
}
