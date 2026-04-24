// T072: object_store 기반 S3/MinIO 백엔드 — SSTable PUT/GET/DELETE, 로컬 LRU 캐시 레이어

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use object_store::ObjectStore;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

// ─── S3 설정 ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket:           String,
    pub prefix:           String,
    pub region:           String,
    pub endpoint:         Option<String>,   // MinIO 등 커스텀 엔드포인트
    pub access_key_id:    Option<String>,
    pub secret_key:       Option<String>,
    pub local_cache_dir:  PathBuf,
    pub local_cache_size: u64,  // 최대 캐시 크기 (바이트)
}

impl S3Config {
    pub fn from_env() -> Self {
        Self {
            bucket:           std::env::var("S3_BUCKET").unwrap_or_else(|_| "wowdb".to_string()),
            prefix:           std::env::var("S3_PREFIX").unwrap_or_else(|_| "analytics/".to_string()),
            region:           std::env::var("AWS_REGION").unwrap_or_else(|_| "ap-northeast-2".to_string()),
            endpoint:         std::env::var("S3_ENDPOINT").ok(),
            access_key_id:    std::env::var("AWS_ACCESS_KEY_ID").ok(),
            secret_key:       std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
            local_cache_dir:  PathBuf::from(
                std::env::var("S3_CACHE_DIR").unwrap_or_else(|_| "/tmp/wowdb_cache".to_string())
            ),
            local_cache_size: std::env::var("S3_CACHE_SIZE_MB")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(128) * 1024 * 1024,
        }
    }
}

// ─── 로컬 LRU 블록 캐시 ────────────────────────────────────────────────────────

struct LocalCacheEntry {
    path:        PathBuf,
    size_bytes:  u64,
    last_access: std::time::Instant,
}

struct LocalCache {
    dir:        PathBuf,
    entries:    HashMap<String, LocalCacheEntry>,
    total_used: u64,
    max_size:   u64,
}

impl LocalCache {
    fn new(dir: PathBuf, max_size: u64) -> Self {
        Self { dir, entries: HashMap::new(), total_used: 0, max_size }
    }

    fn cache_path(&self, key: &str) -> PathBuf {
        // S3 key를 로컬 파일명으로 변환 (/ → __)
        let safe_name = key.replace('/', "__").replace(':', "_");
        self.dir.join(safe_name)
    }

    async fn get(&mut self, key: &str) -> Option<Bytes> {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.last_access = std::time::Instant::now();
            match tokio::fs::read(&entry.path).await {
                Ok(data) => {
                    debug!(key = %key, "S3 cache hit");
                    return Some(Bytes::from(data));
                }
                Err(e) => {
                    warn!(key = %key, "Cache read error: {}", e);
                    self.entries.remove(key);
                }
            }
        }
        None
    }

    async fn put(&mut self, key: &str, data: &[u8]) -> Result<()> {
        let path = self.cache_path(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, data).await?;

        let size = data.len() as u64;
        self.total_used += size;
        self.entries.insert(key.to_string(), LocalCacheEntry {
            path,
            size_bytes:  size,
            last_access: std::time::Instant::now(),
        });

        // 캐시 초과 시 LRU 항목 제거
        self.evict_if_needed().await;
        Ok(())
    }

    async fn evict_if_needed(&mut self) {
        while self.total_used > self.max_size && !self.entries.is_empty() {
            // LRU 항목 찾기
            let oldest_key = self.entries.iter()
                .min_by_key(|(_, e)| e.last_access)
                .map(|(k, _)| k.clone());

            if let Some(key) = oldest_key {
                if let Some(entry) = self.entries.remove(&key) {
                    self.total_used = self.total_used.saturating_sub(entry.size_bytes);
                    let _ = tokio::fs::remove_file(&entry.path).await;
                    debug!(key = %key, "S3 cache evicted");
                }
            } else {
                break;
            }
        }
    }

    fn remove(&mut self, key: &str) {
        if let Some(entry) = self.entries.remove(key) {
            self.total_used = self.total_used.saturating_sub(entry.size_bytes);
        }
    }
}

// ─── S3 Backend ──────────────────────────────────────────────────────────────

pub struct S3Backend {
    config: S3Config,
    cache:  Arc<Mutex<LocalCache>>,
    store:  Option<Arc<dyn ObjectStore>>,
}

impl S3Backend {
    pub async fn new(config: S3Config) -> Result<Self> {
        tokio::fs::create_dir_all(&config.local_cache_dir).await?;

        // Build the object_store client if credentials are configured
        let store: Option<Arc<dyn ObjectStore>> = Self::build_store(&config);

        info!(
            bucket    = %config.bucket,
            prefix    = %config.prefix,
            cache_dir = %config.local_cache_dir.display(),
            connected = store.is_some(),
            "S3 backend initialized"
        );

        let cache = LocalCache::new(config.local_cache_dir.clone(), config.local_cache_size);
        Ok(Self { cache: Arc::new(Mutex::new(cache)), config, store })
    }

    fn build_store(cfg: &S3Config) -> Option<Arc<dyn ObjectStore>> {
        use object_store::aws::AmazonS3Builder;
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&cfg.bucket)
            .with_region(&cfg.region);
        if let Some(ep) = &cfg.endpoint {
            builder = builder.with_endpoint(ep).with_allow_http(true);
        }
        if let (Some(ak), Some(sk)) = (&cfg.access_key_id, &cfg.secret_key) {
            builder = builder.with_access_key_id(ak).with_secret_access_key(sk);
        }
        match builder.build() {
            Ok(s) => Some(Arc::new(s)),
            Err(e) => {
                warn!(err = %e, "S3 backend: failed to build ObjectStore — running cache-only");
                None
            }
        }
    }

    /// S3 객체 키 생성
    fn object_key(&self, path: &str) -> String {
        format!("{}{}", self.config.prefix.trim_end_matches('/'), path)
    }

    /// SSTable 파일 업로드 (PUT)
    pub async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        let key = self.object_key(path);
        debug!(key = %key, size = data.len(), "S3 PUT");

        self.cache.lock().await.put(&key, &data).await?;

        if let Some(store) = &self.store {
            let location = object_store::path::Path::from(key.as_str());
            store.put(&location, data.into()).await
                .map_err(|e| anyhow!("S3 PUT failed: {e}"))?;
        }

        Ok(())
    }

    /// SSTable 파일 다운로드 (GET) — 캐시 우선
    pub async fn get(&self, path: &str) -> Result<Bytes> {
        let key = self.object_key(path);

        if let Some(data) = self.cache.lock().await.get(&key).await {
            return Ok(data);
        }

        debug!(key = %key, "S3 GET (cache miss)");

        let store = self.store.as_ref()
            .ok_or_else(|| anyhow!("S3 backend not connected: key={}", key))?;
        let location = object_store::path::Path::from(key.as_str());
        let result = store.get(&location).await
            .map_err(|e| anyhow!("S3 GET failed: {e}"))?;
        let data = result.bytes().await
            .map_err(|e| anyhow!("S3 GET read failed: {e}"))?;
        self.cache.lock().await.put(&key, &data).await?;
        Ok(data)
    }

    /// SSTable 파일 삭제 (DELETE)
    pub async fn delete(&self, path: &str) -> Result<()> {
        let key = self.object_key(path);
        debug!(key = %key, "S3 DELETE");
        self.cache.lock().await.remove(&key);

        if let Some(store) = &self.store {
            let location = object_store::path::Path::from(key.as_str());
            store.delete(&location).await
                .map_err(|e| anyhow!("S3 DELETE failed: {e}"))?;
        }
        Ok(())
    }

    /// 경로 하위 모든 파일 목록
    pub async fn list(&self, prefix_path: &str) -> Result<Vec<String>> {
        let key_prefix = self.object_key(prefix_path);
        let store = match &self.store {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };

        use tokio_stream::StreamExt;
        let prefix = object_store::path::Path::from(key_prefix.as_str());
        let mut stream = store.list(Some(&prefix));
        let mut keys = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(meta) => keys.push(meta.location.to_string()),
                Err(e)   => warn!(err = %e, "S3 list error"),
            }
        }
        Ok(keys)
    }

    /// 객체 존재 여부 확인
    pub async fn exists(&self, path: &str) -> bool {
        let key = self.object_key(path);
        // 캐시에 있으면 존재
        self.cache.lock().await.entries.contains_key(&key)
    }

    pub fn config(&self) -> &S3Config {
        &self.config
    }

    pub async fn cache_used_bytes(&self) -> u64 {
        self.cache.lock().await.total_used
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_config(cache_dir: PathBuf) -> S3Config {
        S3Config {
            bucket:           "test-bucket".into(),
            prefix:           "test/".into(),
            region:           "us-east-1".into(),
            endpoint:         None,
            access_key_id:    None,
            secret_key:       None,
            local_cache_dir:  cache_dir,
            local_cache_size: 1024 * 1024, // 1 MiB
        }
    }

    #[tokio::test]
    async fn test_put_and_cache_hit() {
        let dir     = tempdir().unwrap();
        let config  = make_config(dir.path().to_path_buf());
        let backend = S3Backend::new(config).await.unwrap();

        let data = Bytes::from("test SSTable data");
        backend.put("/partition/col/seg_0001.col", data.clone()).await.unwrap();

        // 캐시 히트
        assert!(backend.exists("/partition/col/seg_0001.col").await);
    }

    #[tokio::test]
    async fn test_delete() {
        let dir     = tempdir().unwrap();
        let config  = make_config(dir.path().to_path_buf());
        let backend = S3Backend::new(config).await.unwrap();

        backend.put("/col/test.col", Bytes::from("data")).await.unwrap();
        assert!(backend.exists("/col/test.col").await);
        backend.delete("/col/test.col").await.unwrap();
        assert!(!backend.exists("/col/test.col").await);
    }

    #[tokio::test]
    async fn test_cache_eviction() {
        let dir = tempdir().unwrap();
        let config = S3Config {
            local_cache_size: 100, // 100 bytes
            ..make_config(dir.path().to_path_buf())
        };
        let backend = S3Backend::new(config).await.unwrap();

        // 50 bytes × 3 = 150 bytes > 100 bytes → eviction 발생
        for i in 0..3u32 {
            backend.put(&format!("/col/seg_{}.col", i), Bytes::from(vec![0u8; 50])).await.unwrap();
        }

        assert!(backend.cache_used_bytes().await <= 100);
    }
}
