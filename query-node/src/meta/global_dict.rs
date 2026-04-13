// T096: Global Dictionary — 저기수 문자열 컬럼 클러스터 전체 공유 정수 사전
// QN Raft KV 저장, DN 전체 동일 코드 사용으로 문자열 비교 연산 가속

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::raft::{RaftCommand, RaftManager};

// ─── 사전 항목 ────────────────────────────────────────────────────────────────

/// 문자열 → 정수 코드 양방향 매핑
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDict {
    /// 컬럼 식별자 (cube_name.column_name)
    pub column_id:    String,
    /// 문자열 → 코드 (인코딩)
    pub str_to_code:  HashMap<String, u32>,
    /// 코드 → 문자열 (디코딩)
    pub code_to_str:  Vec<String>,
    /// 전체 행에서의 컬럼 NDV 기준 (이 이상이면 사전 제외)
    pub max_ndv:      u32,
    /// 버전 (갱신 시마다 증가)
    pub version:      u64,
}

impl ColumnDict {
    pub fn new(column_id: String, max_ndv: u32) -> Self {
        Self {
            column_id,
            str_to_code: HashMap::new(),
            code_to_str: vec![],
            max_ndv,
            version: 0,
        }
    }

    /// 문자열에 코드 할당 (없으면 새 코드, 있으면 기존 코드)
    /// max_ndv 초과 시 Err 반환 (사전 해제 필요)
    pub fn encode(&mut self, value: &str) -> Result<u32> {
        if let Some(&code) = self.str_to_code.get(value) {
            return Ok(code);
        }
        if self.code_to_str.len() as u32 >= self.max_ndv {
            return Err(anyhow!(
                "Dictionary overflow: column '{}' exceeds max_ndv={}",
                self.column_id, self.max_ndv
            ));
        }
        let code = self.code_to_str.len() as u32;
        self.code_to_str.push(value.to_string());
        self.str_to_code.insert(value.to_string(), code);
        self.version += 1;
        Ok(code)
    }

    /// 코드 → 문자열 디코딩
    pub fn decode(&self, code: u32) -> Option<&str> {
        self.code_to_str.get(code as usize).map(String::as_str)
    }

    /// NDV (현재 사전 크기)
    pub fn ndv(&self) -> u32 { self.code_to_str.len() as u32 }

    /// 사전이 꽉 찼는지 여부
    pub fn is_full(&self) -> bool { self.ndv() >= self.max_ndv }
}

// ─── Global Dictionary Manager ────────────────────────────────────────────────

/// 클러스터 전체 공유 Global Dictionary 관리자
///
/// - QN Raft KV에 직렬화 저장하여 모든 QN에서 동일한 사전 공유
/// - DN은 주기적으로 QN gRPC를 통해 사전 갱신 수신
pub struct GlobalDictManager {
    raft:   Arc<RaftManager>,
    /// 로컬 캐시 (column_id → ColumnDict)
    cache:  RwLock<HashMap<String, ColumnDict>>,
}

const DICT_KV_PREFIX: &str = "global_dict/";
const DEFAULT_MAX_NDV: u32 = 65536;  // 64K 고유 값

impl GlobalDictManager {
    pub fn new(raft: Arc<RaftManager>) -> Self {
        Self {
            raft,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// 컬럼 사전 가져오기 (없으면 새로 생성)
    pub async fn get_or_create_dict(
        &self,
        cube_name:   &str,
        column_name: &str,
    ) -> Result<ColumnDict> {
        let col_id = format!("{}.{}", cube_name, column_name);

        // 캐시 확인
        {
            let cache = self.cache.read().await;
            if let Some(dict) = cache.get(&col_id) {
                return Ok(dict.clone());
            }
        }

        // Raft KV에서 로드
        let raft_key = format!("{}{}", DICT_KV_PREFIX, col_id);
        if let Some(json) = self.raft.read(&raft_key).await {
            if let Ok(dict) = serde_json::from_str::<ColumnDict>(&json) {
                let mut cache = self.cache.write().await;
                cache.insert(col_id, dict.clone());
                return Ok(dict);
            }
        }

        // 신규 생성
        let dict = ColumnDict::new(col_id.clone(), DEFAULT_MAX_NDV);
        self.persist_dict(&dict).await?;
        let mut cache = self.cache.write().await;
        cache.insert(col_id, dict.clone());
        Ok(dict)
    }

    /// 값을 인코딩하고 갱신된 사전 저장
    pub async fn encode(
        &self,
        cube_name:   &str,
        column_name: &str,
        value:       &str,
    ) -> Result<u32> {
        let col_id = format!("{}.{}", cube_name, column_name);

        let mut cache = self.cache.write().await;
        let dict = cache.entry(col_id.clone())
            .or_insert_with(|| ColumnDict::new(col_id.clone(), DEFAULT_MAX_NDV));

        let code = dict.encode(value)?;

        // 버전이 바뀌면 Raft에 영속화
        let dict_clone = dict.clone();
        drop(cache);
        self.persist_dict(&dict_clone).await?;

        Ok(code)
    }

    /// 코드 디코딩
    pub async fn decode(
        &self,
        cube_name:   &str,
        column_name: &str,
        code:        u32,
    ) -> Result<Option<String>> {
        let col_id = format!("{}.{}", cube_name, column_name);
        let dict   = self.get_or_create_dict(cube_name, column_name).await?;
        Ok(dict.decode(code).map(str::to_string))
    }

    /// 사전을 Raft KV에 영속화
    async fn persist_dict(&self, dict: &ColumnDict) -> Result<()> {
        let raft_key = format!("{}{}", DICT_KV_PREFIX, dict.column_id);
        let json     = serde_json::to_string(dict)?;
        self.raft.write(RaftCommand::UpsertKv {
            key:   raft_key,
            value: json,
        }).await?;
        Ok(())
    }

    /// 사전 삭제 (DROP CUBE 등)
    pub async fn drop_column_dict(
        &self,
        cube_name:   &str,
        column_name: &str,
    ) -> Result<()> {
        let col_id   = format!("{}.{}", cube_name, column_name);
        let raft_key = format!("{}{}", DICT_KV_PREFIX, col_id);
        self.raft.write(RaftCommand::DeleteKv { key: raft_key }).await?;
        self.cache.write().await.remove(&col_id);
        info!(column_id = %col_id, "Global Dictionary dropped");
        Ok(())
    }

    /// Cube의 모든 컬럼 사전 삭제
    pub async fn drop_cube_dicts(&self, cube_name: &str) -> Result<()> {
        let prefix = format!("{}{}/", DICT_KV_PREFIX, cube_name);
        let pairs  = self.raft.scan_prefix(&prefix).await;
        for (key, _) in pairs {
            self.raft.write(RaftCommand::DeleteKv { key: key.clone() }).await?;
            // 캐시에서도 제거
            let col_id = key[DICT_KV_PREFIX.len()..].to_string();
            self.cache.write().await.remove(&col_id);
        }
        Ok(())
    }

    /// 사전 캐시 무효화 (다른 QN에서 갱신됐을 때)
    pub async fn invalidate(&self, cube_name: &str, column_name: &str) {
        let col_id = format!("{}.{}", cube_name, column_name);
        self.cache.write().await.remove(&col_id);
    }

    /// 저기수 여부 판단 (NDV ≤ max_ndv/2 이면 사전 대상)
    pub async fn is_low_cardinality(
        &self,
        cube_name:   &str,
        column_name: &str,
    ) -> bool {
        let col_id = format!("{}.{}", cube_name, column_name);
        let cache  = self.cache.read().await;
        if let Some(dict) = cache.get(&col_id) {
            dict.ndv() <= DEFAULT_MAX_NDV / 2
        } else {
            true // 알 수 없으면 저기수로 가정
        }
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RaftManager;

    fn make_mgr() -> GlobalDictManager {
        let raft = Arc::new(RaftManager::new_local());
        GlobalDictManager::new(raft)
    }

    #[test]
    fn test_column_dict_encode_decode() {
        let mut dict = ColumnDict::new("events.event_name".to_string(), 100);
        let c1 = dict.encode("page_view").unwrap();
        let c2 = dict.encode("click").unwrap();
        let c3 = dict.encode("page_view").unwrap(); // 중복 → 같은 코드

        assert_eq!(c1, 0);
        assert_eq!(c2, 1);
        assert_eq!(c3, 0);
        assert_eq!(dict.decode(0), Some("page_view"));
        assert_eq!(dict.decode(1), Some("click"));
        assert!(dict.decode(99).is_none());
    }

    #[test]
    fn test_dict_overflow() {
        let mut dict = ColumnDict::new("events.country".to_string(), 3);
        dict.encode("KR").unwrap();
        dict.encode("US").unwrap();
        dict.encode("JP").unwrap();
        assert!(dict.is_full());
        assert!(dict.encode("DE").is_err());
    }

    #[test]
    fn test_dict_version_increments() {
        let mut dict = ColumnDict::new("test.col".to_string(), 100);
        assert_eq!(dict.version, 0);
        dict.encode("a").unwrap();
        assert_eq!(dict.version, 1);
        dict.encode("a").unwrap(); // 이미 있는 값 — 버전 변화 없음
        assert_eq!(dict.version, 1);
        dict.encode("b").unwrap();
        assert_eq!(dict.version, 2);
    }

    #[tokio::test]
    async fn test_global_dict_encode_persist() {
        let mgr = make_mgr();
        let c1 = mgr.encode("page_events", "event_name", "page_view").await.unwrap();
        let c2 = mgr.encode("page_events", "event_name", "click").await.unwrap();
        let c3 = mgr.encode("page_events", "event_name", "page_view").await.unwrap();

        assert_eq!(c1, 0);
        assert_eq!(c2, 1);
        assert_eq!(c3, 0);
    }

    #[tokio::test]
    async fn test_global_dict_decode() {
        let mgr = make_mgr();
        mgr.encode("events", "country", "KR").await.unwrap();
        mgr.encode("events", "country", "US").await.unwrap();

        let v = mgr.decode("events", "country", 0).await.unwrap();
        assert_eq!(v.as_deref(), Some("KR"));
    }

    #[tokio::test]
    async fn test_drop_column_dict() {
        let mgr = make_mgr();
        mgr.encode("events", "event_name", "click").await.unwrap();
        mgr.drop_column_dict("events", "event_name").await.unwrap();

        // 삭제 후 다시 가져오면 빈 사전
        let dict = mgr.get_or_create_dict("events", "event_name").await.unwrap();
        assert_eq!(dict.ndv(), 0);
    }
}
