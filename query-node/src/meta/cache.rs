// T118: MetadataCacheEntry — Raft 인덱스 기반 캐시 무효화 (FR-033, FR-034)
// DDL write-through 즉시 무효화
// CBO 통계: max_staleness_ms=500ms 후 re-fetch
// 버전 불일치 시 Raft Leader에서 강제 재조회

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::debug;

// ─── 상수 ─────────────────────────────────────────────────────────────────────

/// CBO 통계 최대 허용 오래됨 (500ms)
const MAX_STATS_STALENESS: Duration = Duration::from_millis(500);

// ─── 캐시 항목 종류 ──────────────────────────────────────────────────────────

/// 캐시 항목의 무효화 정책 종류
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheKind {
    /// DDL 메타데이터 (Cube 스키마 등) — Raft 인덱스 변경 시 즉시 무효화
    Ddl,
    /// CBO 통계 — 500ms 허용 (Raft 인덱스와 무관)
    Stats,
}

// ─── MetadataCacheEntry ──────────────────────────────────────────────────────

/// 단일 메타데이터 캐시 항목
#[derive(Debug, Clone)]
pub struct MetadataCacheEntry {
    /// 캐시된 값 (JSON 직렬화)
    pub value:       String,
    /// 캐시된 시점의 Raft 로그 인덱스
    pub raft_index:  u64,
    /// 캐시 저장 시각
    pub cached_at:   Instant,
    /// 무효화 정책 종류
    pub kind:        CacheKind,
}

impl MetadataCacheEntry {
    pub fn new(value: String, raft_index: u64, kind: CacheKind) -> Self {
        Self {
            value,
            raft_index,
            cached_at: Instant::now(),
            kind,
        }
    }

    /// 현재 Raft 인덱스 기준으로 항목이 유효한지 확인
    ///
    /// - DDL: 캐시된 raft_index < current_raft_index → 무효
    /// - Stats: 캐시된 지 500ms 초과 → 무효
    pub fn is_valid(&self, current_raft_index: u64) -> bool {
        match self.kind {
            CacheKind::Ddl => self.raft_index >= current_raft_index,
            CacheKind::Stats => self.cached_at.elapsed() < MAX_STATS_STALENESS,
        }
    }
}

// ─── MetadataCache ────────────────────────────────────────────────────────────

/// QN 로컬 메타데이터 캐시
///
/// - DDL 변경 시 write-through 즉시 무효화
/// - CBO 통계는 500ms 지연 허용
/// - Raft 인덱스 불일치 감지 시 Leader에서 강제 재조회
#[derive(Debug, Clone)]
pub struct MetadataCache {
    inner: Arc<RwLock<MetadataCacheInner>>,
}

#[derive(Debug, Default)]
struct MetadataCacheInner {
    entries:             HashMap<String, MetadataCacheEntry>,
    current_raft_index:  u64,
}

impl MetadataCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MetadataCacheInner::default())),
        }
    }

    /// Raft 인덱스 진행 (Raft 커밋 완료 콜백 시 호출)
    pub async fn advance_raft_index(&self, index: u64) {
        let mut inner = self.inner.write().await;
        if index > inner.current_raft_index {
            inner.current_raft_index = index;
        }
    }

    /// 현재 Raft 인덱스 조회
    pub async fn current_raft_index(&self) -> u64 {
        self.inner.read().await.current_raft_index
    }

    /// 캐시에서 값 조회 (유효하지 않으면 None 반환 → 호출자가 Raft에서 re-fetch)
    pub async fn get(&self, key: &str) -> Option<String> {
        let inner = self.inner.read().await;
        inner.entries.get(key)
            .filter(|e| e.is_valid(inner.current_raft_index))
            .map(|e| e.value.clone())
    }

    /// DDL 메타데이터 캐시 삽입 (write-through 쓰기 후 호출)
    pub async fn put_ddl(&self, key: String, value: String) {
        let mut inner = self.inner.write().await;
        let idx = inner.current_raft_index;
        inner.entries.insert(key, MetadataCacheEntry::new(value, idx, CacheKind::Ddl));
    }

    /// CBO 통계 캐시 삽입
    pub async fn put_stats(&self, key: String, value: String) {
        let mut inner = self.inner.write().await;
        let idx = inner.current_raft_index;
        inner.entries.insert(key, MetadataCacheEntry::new(value, idx, CacheKind::Stats));
    }

    /// DDL 변경 write-through 무효화 — 해당 키 즉시 제거
    ///
    /// CREATE/ALTER/DROP CUBE 등 DDL 쓰기 직후 반드시 호출
    pub async fn invalidate_ddl(&self, key: &str) {
        self.inner.write().await.entries.remove(key);
        debug!(key, "DDL 캐시 즉시 무효화");
    }

    /// Raft Leader 재선출 등 전체 일관성이 깨진 상황에서 전체 클리어
    pub async fn clear_all(&self) {
        self.inner.write().await.entries.clear();
        debug!("메타데이터 캐시 전체 클리어");
    }

    /// 만료된 항목 정리 (주기적 GC 호출용)
    pub async fn evict_expired(&self) {
        let mut inner = self.inner.write().await;
        let current_idx = inner.current_raft_index;
        inner.entries.retain(|_, e| e.is_valid(current_idx));
    }

    /// 버전 불일치 감지 — 클라이언트가 제시한 Raft 인덱스가 로컬보다 높으면 true
    ///
    /// true이면 Raft Leader에서 강제 재조회 필요
    pub async fn needs_leader_fetch(&self, client_raft_index: u64) -> bool {
        client_raft_index > self.inner.read().await.current_raft_index
    }
}

impl Default for MetadataCache {
    fn default() -> Self { Self::new() }
}

// ─── 단위 테스트 ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_ddl_cache_valid_at_same_raft_index() {
        let cache = MetadataCache::new();
        cache.put_ddl("cube:events".to_string(), r#"{"name":"events"}"#.to_string()).await;
        let val = cache.get("cube:events").await;
        assert!(val.is_some(), "같은 Raft 인덱스에서 DDL 캐시는 유효해야 함");
    }

    #[tokio::test]
    async fn test_ddl_cache_invalidated_on_raft_advance() {
        let cache = MetadataCache::new();
        // Raft 인덱스 0에서 캐시
        cache.put_ddl("cube:events".to_string(), "v1".to_string()).await;
        // Raft 인덱스 1로 진행 → DDL 캐시 무효화
        cache.advance_raft_index(1).await;
        let val = cache.get("cube:events").await;
        assert!(val.is_none(), "Raft 인덱스 진행 후 DDL 캐시는 무효화되어야 함");
    }

    #[tokio::test]
    async fn test_stats_cache_valid_within_500ms() {
        let cache = MetadataCache::new();
        cache.put_stats("stats:events".to_string(), r#"{"ndv":1000}"#.to_string()).await;
        let val = cache.get("stats:events").await;
        assert!(val.is_some(), "500ms 내 통계 캐시는 유효해야 함");
    }

    #[tokio::test]
    async fn test_stats_cache_expired_after_500ms() {
        let cache = MetadataCache::new();
        cache.put_stats("stats:events".to_string(), "v1".to_string()).await;
        sleep(Duration::from_millis(510)).await;
        let val = cache.get("stats:events").await;
        assert!(val.is_none(), "500ms 경과 후 통계 캐시는 만료되어야 함");
    }

    #[tokio::test]
    async fn test_invalidate_ddl_removes_entry() {
        let cache = MetadataCache::new();
        cache.put_ddl("cube:tmp".to_string(), "v1".to_string()).await;
        cache.invalidate_ddl("cube:tmp").await;
        assert!(cache.get("cube:tmp").await.is_none(), "명시적 무효화 후 캐시 항목 없어야 함");
    }

    #[tokio::test]
    async fn test_stats_not_affected_by_raft_advance() {
        let cache = MetadataCache::new();
        cache.put_stats("stats:events".to_string(), "v1".to_string()).await;
        // Raft 인덱스 진행해도 Stats는 500ms 이내면 유효
        cache.advance_raft_index(100).await;
        let val = cache.get("stats:events").await;
        assert!(val.is_some(), "Stats 캐시는 Raft 인덱스에 무관하게 500ms 내 유효해야 함");
    }

    #[tokio::test]
    async fn test_needs_leader_fetch() {
        let cache = MetadataCache::new();
        // 로컬 인덱스 0, 클라이언트가 5를 제시
        assert!(cache.needs_leader_fetch(5).await, "클라이언트 인덱스가 높으면 Leader 재조회 필요");
        cache.advance_raft_index(5).await;
        assert!(!cache.needs_leader_fetch(5).await, "동일 인덱스면 재조회 불필요");
    }
}
