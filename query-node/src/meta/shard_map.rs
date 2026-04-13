// T129: Shard → SN 매핑 조회 (FR-039)
// Raft KV /tablets/{shard_id}에서 ShardReplica 목록 조회
// Leader replica 우선 선택, QN 로컬 캐시 TTL 500ms

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use tokio::sync::RwLock;
use tracing::{debug, warn};
use uuid::Uuid;

// ─── Replica 상태 ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicaRole {
    Leader,
    Follower,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicaState {
    Normal,
    CatchingUp,
    Offline,
}

/// Shard의 단일 복제본 정보
#[derive(Debug, Clone)]
pub struct ShardReplica {
    pub sn_node_id:  String,
    pub sn_endpoint: SocketAddr,  // gRPC 주소 (ip:9060)
    pub role:        ReplicaRole,
    pub state:       ReplicaState,
    pub shard_dir:   PathBuf,     // SN 상의 물리 디렉토리 절대 경로
    pub lsn:         u64,
}

/// Shard 매핑 엔트리 (Raft KV에서 읽어온 데이터)
#[derive(Debug, Clone)]
pub struct ShardEntry {
    pub shard_id:  Uuid,
    pub replicas:  Vec<ShardReplica>,
}

impl ShardEntry {
    /// 읽기에 사용할 최적 replica 선택
    /// 우선순위: 1) Leader(Normal) → 2) Follower(Normal) → 3) Leader(CatchingUp)
    pub fn best_replica(&self) -> Option<&ShardReplica> {
        // 1순위: Normal Leader
        if let Some(r) = self.replicas.iter()
            .find(|r| r.role == ReplicaRole::Leader && r.state == ReplicaState::Normal)
        {
            return Some(r);
        }
        // 2순위: Normal Follower (LSN 가장 높은 것)
        let best_follower = self.replicas.iter()
            .filter(|r| r.state == ReplicaState::Normal)
            .max_by_key(|r| r.lsn);
        if best_follower.is_some() {
            return best_follower;
        }
        // 3순위: CatchingUp (Offline 제외)
        self.replicas.iter().find(|r| r.state != ReplicaState::Offline)
    }
}

// ─── 캐시 엔트리 ─────────────────────────────────────────────────────────────

const CACHE_TTL: Duration = Duration::from_millis(500);

struct CacheEntry {
    entry:      ShardEntry,
    cached_at:  Instant,
}

impl CacheEntry {
    fn is_fresh(&self) -> bool {
        self.cached_at.elapsed() < CACHE_TTL
    }
}

// ─── ShardMap ────────────────────────────────────────────────────────────────

/// QN 로컬 Shard 매핑 캐시
/// 실제 배포에서는 Raft KV를 백엔드로 사용한다.
/// 여기서는 인메모리 HashMap으로 시뮬레이션하며 TTL 500ms 캐시를 적용한다.
pub struct ShardMap {
    /// 캐시: shard_id → (entry, cached_at)
    cache: Arc<RwLock<HashMap<Uuid, CacheEntry>>>,
    /// Raft KV 백업 저장소 (테스트/초기화용)
    backing: Arc<RwLock<HashMap<Uuid, ShardEntry>>>,
}

impl ShardMap {
    pub fn new() -> Self {
        Self {
            cache:   Arc::new(RwLock::new(HashMap::new())),
            backing: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 테스트/초기화: Shard 엔트리 직접 등록
    pub async fn register(&self, entry: ShardEntry) {
        let shard_id = entry.shard_id;
        self.backing.write().await.insert(shard_id, entry.clone());
        // 즉시 캐시에도 저장
        self.cache.write().await.insert(shard_id, CacheEntry {
            entry,
            cached_at: Instant::now(),
        });
    }

    /// Tablet 재배치/DDL 시 특정 Shard 캐시 무효화
    pub async fn invalidate(&self, shard_id: Uuid) {
        self.cache.write().await.remove(&shard_id);
        debug!(shard_id = %shard_id, "Shard 캐시 무효화");
    }

    /// 모든 Shard 캐시 무효화 (DDL 전체 반영 시)
    pub async fn invalidate_all(&self) {
        self.cache.write().await.clear();
        debug!("전체 Shard 캐시 무효화");
    }

    /// Shard ID → (SN endpoint, shard_dir) 조회
    /// 캐시 hit: 500ms 이내의 캐시 데이터 반환
    /// 캐시 miss: Raft KV(backing) 조회 후 캐시 갱신
    pub async fn lookup(&self, shard_id: Uuid) -> Result<(SocketAddr, PathBuf)> {
        // 캐시 확인 (읽기 락)
        {
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(&shard_id) {
                if entry.is_fresh() {
                    if let Some(replica) = entry.entry.best_replica() {
                        debug!(
                            shard_id = %shard_id,
                            node = %replica.sn_node_id,
                            "Shard 매핑 캐시 HIT"
                        );
                        return Ok((replica.sn_endpoint, replica.shard_dir.clone()));
                    }
                }
            }
        }

        // 캐시 miss — Raft KV 조회
        debug!(shard_id = %shard_id, "Shard 매핑 캐시 MISS — Raft KV 조회");
        let shard_entry = self.fetch_from_raft(shard_id).await?;
        let replica = shard_entry.best_replica()
            .ok_or_else(|| anyhow!("Shard {} 에 유효한 replica 없음", shard_id))?;
        let result = (replica.sn_endpoint, replica.shard_dir.clone());

        // 캐시 갱신 (쓰기 락)
        self.cache.write().await.insert(shard_id, CacheEntry {
            entry:     shard_entry,
            cached_at: Instant::now(),
        });

        Ok(result)
    }

    /// 전체 ShardEntry 조회 (복제본 목록 포함)
    pub async fn get_shard_entry(&self, shard_id: Uuid) -> Result<ShardEntry> {
        // 캐시 확인
        {
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(&shard_id) {
                if entry.is_fresh() {
                    return Ok(entry.entry.clone());
                }
            }
        }
        self.fetch_from_raft(shard_id).await
    }

    /// Raft KV에서 ShardEntry 조회 (내부 헬퍼)
    async fn fetch_from_raft(&self, shard_id: Uuid) -> Result<ShardEntry> {
        // 실제 구현에서는 openraft KV에서 /tablets/{shard_id} 조회
        // 현재는 backing HashMap에서 조회
        self.backing.read().await
            .get(&shard_id)
            .cloned()
            .ok_or_else(|| anyhow!("Shard {} 를 Raft KV에서 찾을 수 없음", shard_id))
    }
}

impl Default for ShardMap {
    fn default() -> Self { Self::new() }
}

// ─── 단위 테스트 ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_shard(shard_id: Uuid, addr: &str, dir: &str, role: ReplicaRole) -> ShardEntry {
        ShardEntry {
            shard_id,
            replicas: vec![ShardReplica {
                sn_node_id:  "sn-01".to_string(),
                sn_endpoint: addr.parse().unwrap(),
                role,
                state:       ReplicaState::Normal,
                shard_dir:   PathBuf::from(dir),
                lsn:         100,
            }],
        }
    }

    #[tokio::test]
    async fn test_lookup_cache_hit() {
        let smap = ShardMap::new();
        let sid  = Uuid::new_v4();
        let entry = make_shard(sid, "127.0.0.1:9060", "/data/shard-01", ReplicaRole::Leader);
        smap.register(entry).await;

        let (addr, dir) = smap.lookup(sid).await.unwrap();
        assert_eq!(addr.to_string(), "127.0.0.1:9060");
        assert_eq!(dir, PathBuf::from("/data/shard-01"));
    }

    #[tokio::test]
    async fn test_lookup_missing_shard_returns_error() {
        let smap = ShardMap::new();
        let result = smap.lookup(Uuid::new_v4()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invalidate_forces_raft_fetch() {
        let smap = ShardMap::new();
        let sid   = Uuid::new_v4();
        let entry = make_shard(sid, "127.0.0.1:9060", "/data/shard-01", ReplicaRole::Leader);
        smap.register(entry).await;

        // 캐시 무효화 후 재조회 — backing에서 다시 가져와야 함
        smap.invalidate(sid).await;
        let (addr, _) = smap.lookup(sid).await.unwrap();
        assert_eq!(addr.to_string(), "127.0.0.1:9060");
    }

    #[tokio::test]
    async fn test_best_replica_prefers_normal_leader() {
        let sid = Uuid::new_v4();
        let entry = ShardEntry {
            shard_id: sid,
            replicas: vec![
                ShardReplica {
                    sn_node_id:  "sn-01".to_string(),
                    sn_endpoint: "127.0.0.1:9060".parse().unwrap(),
                    role:  ReplicaRole::Follower,
                    state: ReplicaState::Normal,
                    shard_dir: PathBuf::from("/data/follower"),
                    lsn: 90,
                },
                ShardReplica {
                    sn_node_id:  "sn-02".to_string(),
                    sn_endpoint: "127.0.0.2:9060".parse().unwrap(),
                    role:  ReplicaRole::Leader,
                    state: ReplicaState::Normal,
                    shard_dir: PathBuf::from("/data/leader"),
                    lsn: 100,
                },
            ],
        };
        let best = entry.best_replica().unwrap();
        assert_eq!(best.role, ReplicaRole::Leader);
        assert_eq!(best.sn_node_id, "sn-02");
    }
}
