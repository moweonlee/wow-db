// T097: Query Result Cache — 동일 LogicalPlan + 파티션 버전 기준 Tablet 단위 집계 결과 캐시
// CN 메모리에 캐시, TTL 만료 시 자동 제거

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, info};

// ─── 캐시 키 ─────────────────────────────────────────────────────────────────

/// 캐시 키 = LogicalPlan 해시 + 파티션 버전 맵
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheKey {
    /// Logical Plan의 해시 (SHA-256 앞 8바이트 hex)
    pub plan_hash:         String,
    /// 파티션별 버전 (partition_name → version)
    pub partition_versions: Vec<(String, u64)>,
}

impl CacheKey {
    pub fn new(plan_hash: impl Into<String>, mut partition_versions: Vec<(String, u64)>) -> Self {
        // 정규화: 파티션 이름 정렬
        partition_versions.sort_by(|a, b| a.0.cmp(&b.0));
        Self {
            plan_hash: plan_hash.into(),
            partition_versions,
        }
    }

    /// 문자열 기반 해시 (테스트/stub용)
    pub fn from_sql_and_partitions(sql: &str, partitions: Vec<(String, u64)>) -> Self {
        // 실제 구현에서는 CBO LogicalPlan 구조체를 해시
        let hash = format!("{:016x}", simple_hash(sql));
        Self::new(hash, partitions)
    }
}

fn simple_hash(s: &str) -> u64 {
    let mut h: u64 = 14695981039346656037;
    for b in s.bytes() {
        h = h.wrapping_mul(1099511628211);
        h ^= b as u64;
    }
    h
}

// ─── 캐시 항목 ───────────────────────────────────────────────────────────────

/// Tablet 단위 집계 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabletAggResult {
    pub tablet_id:  String,
    /// 직렬화된 집계 결과 행 (Arrow IPC 포맷 또는 JSON 스텁)
    pub data:       Vec<u8>,
    /// 행 수
    pub row_count:  u64,
    /// 스캔된 바이트 수
    pub bytes_read: u64,
}

struct CacheEntry {
    tablets:     Vec<TabletAggResult>,
    inserted_at: Instant,
    ttl:         Duration,
    hit_count:   u64,
}

impl CacheEntry {
    fn is_expired(&self) -> bool {
        self.inserted_at.elapsed() > self.ttl
    }
}

// ─── Result Cache ─────────────────────────────────────────────────────────────

pub struct QueryResultCache {
    entries:     Mutex<HashMap<CacheKey, CacheEntry>>,
    default_ttl: Duration,
    max_entries: usize,
    max_bytes:   usize,
}

impl QueryResultCache {
    pub fn new(default_ttl: Duration, max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries:     Mutex::new(HashMap::new()),
            default_ttl,
            max_entries,
            max_bytes,
        }
    }

    /// 캐시에서 조회
    pub async fn get(&self, key: &CacheKey) -> Option<Vec<TabletAggResult>> {
        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.get_mut(key) {
            if entry.is_expired() {
                entries.remove(key);
                return None;
            }
            entry.hit_count += 1;
            debug!(
                plan_hash = %key.plan_hash,
                hits = entry.hit_count,
                "Cache hit"
            );
            return Some(entry.tablets.clone());
        }
        None
    }

    /// 캐시에 저장
    pub async fn put(
        &self,
        key:     CacheKey,
        tablets: Vec<TabletAggResult>,
        ttl:     Option<Duration>,
    ) {
        let ttl = ttl.unwrap_or(self.default_ttl);

        let total_bytes: usize = tablets.iter().map(|t| t.data.len()).sum();
        if total_bytes > self.max_bytes {
            debug!(bytes = total_bytes, "Cache put skipped: entry too large");
            return;
        }

        let mut entries = self.entries.lock().await;

        // 용량 초과 시 오래된 항목 제거
        if entries.len() >= self.max_entries {
            self.evict_expired_locked(&mut entries);
            if entries.len() >= self.max_entries {
                self.evict_lru_locked(&mut entries);
            }
        }

        entries.insert(key.clone(), CacheEntry {
            tablets,
            inserted_at: Instant::now(),
            ttl,
            hit_count: 0,
        });
        debug!(plan_hash = %key.plan_hash, "Cache put");
    }

    /// 파티션 버전 변경으로 무효화 (파티션 쓰기 발생 시)
    pub async fn invalidate_by_partition(&self, partition_name: &str) {
        let mut entries = self.entries.lock().await;
        let before = entries.len();
        entries.retain(|k, _| {
            !k.partition_versions.iter().any(|(p, _)| p == partition_name)
        });
        let removed = before - entries.len();
        if removed > 0 {
            info!(partition = %partition_name, removed, "Cache invalidated by partition update");
        }
    }

    /// 만료된 항목 모두 제거
    pub async fn evict_expired(&self) -> usize {
        let mut entries = self.entries.lock().await;
        let before = entries.len();
        self.evict_expired_locked(&mut entries);
        before - entries.len()
    }

    fn evict_expired_locked(&self, entries: &mut HashMap<CacheKey, CacheEntry>) {
        entries.retain(|_, v| !v.is_expired());
    }

    fn evict_lru_locked(&self, entries: &mut HashMap<CacheKey, CacheEntry>) {
        // 가장 오래전에 삽입된 항목 제거
        if let Some(oldest_key) = entries
            .iter()
            .min_by_key(|(_, v)| v.inserted_at)
            .map(|(k, _)| k.clone())
        {
            entries.remove(&oldest_key);
        }
    }

    /// 전체 캐시 초기화
    pub async fn clear(&self) {
        self.entries.lock().await.clear();
    }

    /// 현재 항목 수
    pub async fn entry_count(&self) -> usize {
        self.entries.lock().await.len()
    }

    /// 현재 캐시 사용 바이트
    pub async fn total_bytes(&self) -> usize {
        let entries = self.entries.lock().await;
        entries.values()
            .flat_map(|e| e.tablets.iter())
            .map(|t| t.data.len())
            .sum()
    }
}

impl Default for QueryResultCache {
    fn default() -> Self {
        Self::new(
            Duration::from_secs(300),  // 5분 TTL
            10_000,                     // 최대 10K 항목
            512 * 1024 * 1024,          // 512MB
        )
    }
}

// ─── 글로벌 싱글턴 ────────────────────────────────────────────────────────────

use std::sync::{Arc, LazyLock};

pub static RESULT_CACHE: LazyLock<Arc<QueryResultCache>> =
    LazyLock::new(|| Arc::new(QueryResultCache::default()));

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cache(ttl_ms: u64) -> QueryResultCache {
        QueryResultCache::new(
            Duration::from_millis(ttl_ms),
            100,
            1024 * 1024,
        )
    }

    fn make_result(tablet_id: &str, data: &[u8]) -> TabletAggResult {
        TabletAggResult {
            tablet_id:  tablet_id.to_string(),
            data:       data.to_vec(),
            row_count:  10,
            bytes_read: data.len() as u64,
        }
    }

    fn make_key(sql: &str) -> CacheKey {
        CacheKey::from_sql_and_partitions(sql, vec![
            ("p_2024_q1".to_string(), 1),
            ("p_2024_q2".to_string(), 3),
        ])
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let cache = make_cache(5000);
        let key   = make_key("SELECT count(*) FROM events");
        cache.put(key.clone(), vec![make_result("t1", b"result_data")], None).await;

        let result = cache.get(&key).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap()[0].tablet_id, "t1");
    }

    #[tokio::test]
    async fn test_miss() {
        let cache = make_cache(5000);
        let key   = make_key("SELECT * FROM unknown");
        assert!(cache.get(&key).await.is_none());
    }

    #[tokio::test]
    async fn test_ttl_expiry() {
        let cache = make_cache(1); // 1ms TTL
        let key   = make_key("SELECT 1");
        cache.put(key.clone(), vec![make_result("t1", b"data")], None).await;
        std::thread::sleep(Duration::from_millis(10));
        assert!(cache.get(&key).await.is_none(), "Entry should be expired");
    }

    #[tokio::test]
    async fn test_partition_invalidation() {
        let cache = make_cache(60000);
        let key   = CacheKey::new("abcdef", vec![
            ("p_2024_q1".to_string(), 1),
        ]);
        cache.put(key.clone(), vec![make_result("t1", b"data")], None).await;
        assert!(cache.get(&key).await.is_some());

        cache.invalidate_by_partition("p_2024_q1").await;
        assert!(cache.get(&key).await.is_none(), "Should be invalidated");
    }

    #[tokio::test]
    async fn test_partition_key_normalization() {
        // 파티션 순서와 무관하게 같은 키여야 함
        let k1 = CacheKey::new("abc", vec![("p2".to_string(), 1), ("p1".to_string(), 2)]);
        let k2 = CacheKey::new("abc", vec![("p1".to_string(), 2), ("p2".to_string(), 1)]);
        assert_eq!(k1, k2);
    }

    #[tokio::test]
    async fn test_evict_expired() {
        let cache = make_cache(1);
        for i in 0..5 {
            let key = make_key(&format!("SELECT {}", i));
            cache.put(key, vec![make_result("t", b"data")], None).await;
        }
        std::thread::sleep(Duration::from_millis(10));
        let removed = cache.evict_expired().await;
        assert_eq!(removed, 5);
        assert_eq!(cache.entry_count().await, 0);
    }

    #[tokio::test]
    async fn test_entry_count_and_clear() {
        let cache = make_cache(60000);
        for i in 0..3 {
            let key = make_key(&format!("SELECT {} FROM t", i));
            cache.put(key, vec![make_result("t", b"data")], None).await;
        }
        assert_eq!(cache.entry_count().await, 3);
        cache.clear().await;
        assert_eq!(cache.entry_count().await, 0);
    }
}
