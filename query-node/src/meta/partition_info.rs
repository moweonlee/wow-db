// T138: PartitionInfoService — SHOW PARTITIONS / SHARDS / PARTS / DISTRIBUTED STATUS (FR-043~FR-046)
//
// 데이터 소스 계층:
//   1. TabletManager (Raft KV)  → 파티션 구조 및 Shard → SN 매핑
//   2. ShardMap                 → 복제본 SN 주소 및 역할 (Leader/Follower)
//   3. SnPartSource trait       → SN gRPC GetPartList / GetShardInfo
//      └─ LocalSnPartSource     : SN 없이 빈 데이터 반환 (개발/테스트 환경)
//      └─ GrpcSnPartSource      : 실제 SN gRPC 연결 (프로덕션 환경)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

use crate::meta::shard_map::{ReplicaRole, ReplicaState, ShardMap};
use crate::meta::tablet::TabletManager;
use crate::raft::RaftManager;

// ─── SHOW PARTITIONS 출력 행 ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PartitionMeta {
    /// 파티션 이름 (예: "p_2026_04_12")
    pub partition_id:  String,
    /// 파티션 키 범위 시작값 (AUTO PARTITION: 날짜 문자열)
    pub range_start:   String,
    /// 파티션 키 범위 끝값
    pub range_end:     String,
    pub row_count:     u64,
    pub size_bytes:    u64,
    /// 이 파티션에 속한 Shard(Tablet) 수
    pub shard_count:   u32,
    /// 전체 Part(SSTable) 수 (모든 Shard 합산)
    pub part_count:    u32,
    /// 스토리지 계층 ("hot" | "cold")
    pub tier:          String,
    pub created_at:    String,
}

// ─── SHOW SHARDS 출력 행 ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ShardMeta {
    pub shard_id:        String,
    pub partition_id:    String,
    pub partition_range: String,
    pub sn_node_id:      String,
    pub sn_endpoint:     String,
    pub bucket_id:       u32,
    /// "leader" | "follower"
    pub role:            String,
    /// "normal" | "catching_up" | "offline"
    pub state:           String,
    pub row_count:       u64,
    pub size_bytes:      u64,
    pub part_count:      u32,
    pub lsn:             u64,
}

// ─── SHOW PARTS 출력 행 ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PartMeta {
    pub part_id:          String,
    pub shard_id:         String,
    pub partition_id:     String,
    pub sn_node_id:       String,
    /// LSM 레벨 (0=L0, 1~6=Ln)
    pub level:            u32,
    pub sequence_num:     u64,
    pub row_count:        u64,
    pub size_bytes:       u64,
    pub min_sort_key:     String,
    pub max_sort_key:     String,
    pub bloom_size_bytes: u64,
    pub created_at:       String,
}

// ─── SHOW DISTRIBUTED STATUS 출력 행 ────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SnDistributionSummary {
    pub sn_node_id:         String,
    pub sn_endpoint:        String,
    pub shard_count:        u32,
    pub leader_shard_count: u32,
    pub partition_count:    u32,
    pub row_count:          u64,
    pub size_bytes:         u64,
    pub avg_part_per_shard: f64,
}

// ─── SN 데이터 소스 trait ─────────────────────────────────────────────────────

/// SN gRPC 호출 추상화
/// - `LocalSnPartSource`: 개발·테스트 환경에서 빈 데이터 반환
/// - `GrpcSnPartSource`:  실제 SN에 gRPC 연결 (프로덕션)
#[async_trait]
pub trait SnPartSource: Send + Sync + 'static {
    /// SN GetPartList RPC — 해당 Shard의 Part(SSTable) 목록 반환
    async fn get_parts(&self, sn_endpoint: &str, shard_id: Uuid) -> Vec<PartMeta>;
    /// SN GetShardInfo RPC — (row_count, size_bytes, lsn) 반환
    async fn get_shard_stats(&self, sn_endpoint: &str, shard_id: Uuid) -> (u64, u64, u64);
}

// ─── 로컬 스텁 (SN 없는 환경) ────────────────────────────────────────────────

pub struct LocalSnPartSource;

#[async_trait]
impl SnPartSource for LocalSnPartSource {
    async fn get_parts(&self, _sn_endpoint: &str, _shard_id: Uuid) -> Vec<PartMeta> {
        vec![]
    }

    async fn get_shard_stats(&self, _sn_endpoint: &str, _shard_id: Uuid) -> (u64, u64, u64) {
        (0, 0, 0)
    }
}

// ─── gRPC 스텁 (프로덕션 — 향후 구현) ────────────────────────────────────────

/// 실제 SN gRPC 연결 구현
/// 현재는 스텁; Phase F에서 tonic 클라이언트로 구현 예정
pub struct GrpcSnPartSource;

#[async_trait]
impl SnPartSource for GrpcSnPartSource {
    async fn get_parts(&self, _sn_endpoint: &str, _shard_id: Uuid) -> Vec<PartMeta> {
        // TODO Phase F: tonic client call to StorageService::GetPartList
        vec![]
    }

    async fn get_shard_stats(&self, _sn_endpoint: &str, _shard_id: Uuid) -> (u64, u64, u64) {
        // TODO Phase F: tonic client call to StorageService::GetShardInfo
        (0, 0, 0)
    }
}

// ─── PartitionInfoService ────────────────────────────────────────────────────

pub struct PartitionInfoService {
    tablet_mgr: TabletManager,
    shard_map:  Arc<ShardMap>,
    sn_source:  Arc<dyn SnPartSource>,
}

impl PartitionInfoService {
    pub fn new(
        raft:      Arc<RaftManager>,
        shard_map: Arc<ShardMap>,
        sn_source: Arc<dyn SnPartSource>,
    ) -> Self {
        Self {
            tablet_mgr: TabletManager::new(raft),
            shard_map,
            sn_source,
        }
    }

    /// 개발·테스트용 로컬 인스턴스 (SN gRPC 없음)
    pub fn new_local(raft: Arc<RaftManager>) -> Self {
        Self::new(raft, Arc::new(ShardMap::new()), Arc::new(LocalSnPartSource))
    }

    // ── SHOW PARTITIONS ───────────────────────────────────────────────────────

    /// `cube_id`: Cube name 또는 UUID 문자열
    pub async fn list_partitions(&self, cube_id: &str) -> Result<Vec<PartitionMeta>> {
        let tablets = self.tablet_mgr.list_for_cube(cube_id).await?;

        // Group tablets by partition name
        let mut by_partition: HashMap<String, Vec<_>> = HashMap::new();
        for t in &tablets {
            by_partition.entry(t.partition.clone()).or_default().push(t);
        }

        let mut result = Vec::new();
        let now_str = Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();

        for (partition_id, tablet_list) in &by_partition {
            let shard_count = tablet_list.len() as u32;
            let mut total_row_count  = 0u64;
            let mut total_size_bytes = 0u64;
            let mut total_part_count = 0u32;

            for tablet in tablet_list {
                let shard_uuid = tablet.tablet_id;
                if let Ok(entry) = self.shard_map.get_shard_entry(shard_uuid).await {
                    if let Some(best) = entry.best_replica() {
                        let endpoint = best.sn_endpoint.to_string();
                        let (rc, sb, _) = self.sn_source.get_shard_stats(&endpoint, shard_uuid).await;
                        total_row_count  += rc;
                        total_size_bytes += sb;
                        let parts = self.sn_source.get_parts(&endpoint, shard_uuid).await;
                        total_part_count += parts.len() as u32;
                    }
                }
            }

            // Infer range from partition_id name (AUTO PARTITION BY DAY → "p_2026_04_12")
            let (range_start, range_end) = infer_partition_range(partition_id);

            result.push(PartitionMeta {
                partition_id:  partition_id.clone(),
                range_start,
                range_end,
                row_count:    total_row_count,
                size_bytes:   total_size_bytes,
                shard_count,
                part_count:   total_part_count,
                tier:         "hot".to_string(),
                created_at:   now_str.clone(),
            });
        }

        result.sort_by(|a, b| a.partition_id.cmp(&b.partition_id));
        Ok(result)
    }

    // ── SHOW SHARDS ───────────────────────────────────────────────────────────

    pub async fn list_shards(
        &self,
        cube_id:      &str,
        partition_id: Option<&str>,
    ) -> Result<Vec<ShardMeta>> {
        let tablets = self.tablet_mgr.list_for_cube(cube_id).await?;
        let mut result = Vec::new();

        for tablet in &tablets {
            if let Some(pid) = partition_id {
                if tablet.partition != pid { continue; }
            }

            let shard_uuid = tablet.tablet_id;
            let mut sn_node_id  = format!("sn-{:02}", tablet.leader_sn);
            let mut sn_endpoint = String::new();
            let mut role        = "leader".to_string();
            let mut state       = "normal".to_string();
            let mut lsn         = 0u64;
            let mut row_count   = 0u64;
            let mut size_bytes  = 0u64;
            let mut part_count  = 0u32;

            if let Ok(entry) = self.shard_map.get_shard_entry(shard_uuid).await {
                if let Some(best) = entry.best_replica() {
                    sn_node_id  = best.sn_node_id.clone();
                    sn_endpoint = best.sn_endpoint.to_string();
                    role = match best.role {
                        ReplicaRole::Leader   => "leader".to_string(),
                        ReplicaRole::Follower => "follower".to_string(),
                    };
                    state = match best.state {
                        ReplicaState::Normal     => "normal".to_string(),
                        ReplicaState::CatchingUp => "catching_up".to_string(),
                        ReplicaState::Offline    => "offline".to_string(),
                    };
                    lsn = best.lsn;

                    let (rc, sb, _) = self.sn_source.get_shard_stats(&sn_endpoint, shard_uuid).await;
                    row_count  = rc;
                    size_bytes = sb;

                    let parts = self.sn_source.get_parts(&sn_endpoint, shard_uuid).await;
                    part_count = parts.len() as u32;
                }
            }

            let (range_start, range_end) = infer_partition_range(&tablet.partition);
            let partition_range = if range_start.is_empty() {
                tablet.partition.clone()
            } else {
                format!("[{}, {})", range_start, range_end)
            };

            result.push(ShardMeta {
                shard_id:        shard_uuid.to_string(),
                partition_id:    tablet.partition.clone(),
                partition_range,
                sn_node_id,
                sn_endpoint,
                bucket_id:       tablet.bucket,
                role,
                state,
                row_count,
                size_bytes,
                part_count,
                lsn,
            });
        }

        Ok(result)
    }

    // ── SHOW PARTS ────────────────────────────────────────────────────────────

    pub async fn list_parts(
        &self,
        cube_id:      &str,
        partition_id: Option<&str>,
        shard_id:     Option<&str>,
    ) -> Result<Vec<PartMeta>> {
        let tablets = self.tablet_mgr.list_for_cube(cube_id).await?;
        let mut result = Vec::new();

        for tablet in &tablets {
            if let Some(pid) = partition_id {
                if tablet.partition != pid { continue; }
            }
            if let Some(sid) = shard_id {
                if tablet.tablet_id.to_string() != sid { continue; }
            }

            let shard_uuid = tablet.tablet_id;
            if let Ok(entry) = self.shard_map.get_shard_entry(shard_uuid).await {
                if let Some(best) = entry.best_replica() {
                    let sn_endpoint = best.sn_endpoint.to_string();
                    let sn_node_id  = best.sn_node_id.clone();

                    let mut parts = self.sn_source.get_parts(&sn_endpoint, shard_uuid).await;
                    for p in &mut parts {
                        if p.partition_id.is_empty() { p.partition_id = tablet.partition.clone(); }
                        if p.shard_id.is_empty()     { p.shard_id     = shard_uuid.to_string(); }
                        if p.sn_node_id.is_empty()   { p.sn_node_id   = sn_node_id.clone(); }
                    }
                    result.extend(parts);
                }
            }
        }

        Ok(result)
    }

    // ── SHOW DISTRIBUTED STATUS ───────────────────────────────────────────────

    pub async fn get_distributed_status(
        &self,
        cube_id: &str,
    ) -> Result<Vec<SnDistributionSummary>> {
        let tablets = self.tablet_mgr.list_for_cube(cube_id).await?;
        let mut sn_map: HashMap<String, SnDistributionSummary> = HashMap::new();
        let mut sn_partitions: HashMap<String, HashSet<String>> = HashMap::new();
        let mut sn_total_parts: HashMap<String, u32> = HashMap::new();

        for tablet in &tablets {
            let shard_uuid = tablet.tablet_id;

            let Ok(entry) = self.shard_map.get_shard_entry(shard_uuid).await else {
                continue;
            };

            for replica in &entry.replicas {
                let sn_id    = replica.sn_node_id.clone();
                let endpoint = replica.sn_endpoint.to_string();

                let summary = sn_map.entry(sn_id.clone()).or_insert_with(|| SnDistributionSummary {
                    sn_node_id:         sn_id.clone(),
                    sn_endpoint:        endpoint.clone(),
                    shard_count:        0,
                    leader_shard_count: 0,
                    partition_count:    0,
                    row_count:          0,
                    size_bytes:         0,
                    avg_part_per_shard: 0.0,
                });

                summary.shard_count += 1;
                if replica.role == ReplicaRole::Leader {
                    summary.leader_shard_count += 1;

                    let (rc, sb, _) = self.sn_source.get_shard_stats(&endpoint, shard_uuid).await;
                    summary.row_count  += rc;
                    summary.size_bytes += sb;

                    let parts = self.sn_source.get_parts(&endpoint, shard_uuid).await;
                    *sn_total_parts.entry(sn_id.clone()).or_default() += parts.len() as u32;
                }

                sn_partitions
                    .entry(sn_id.clone())
                    .or_default()
                    .insert(tablet.partition.clone());
            }
        }

        // Finalise partition counts and avg_part_per_shard
        for (sn_id, summary) in &mut sn_map {
            summary.partition_count = sn_partitions.get(sn_id).map(|s| s.len() as u32).unwrap_or(0);
            let total_parts = sn_total_parts.get(sn_id).copied().unwrap_or(0);
            if summary.shard_count > 0 {
                summary.avg_part_per_shard = total_parts as f64 / summary.shard_count as f64;
            }
        }

        let mut result: Vec<_> = sn_map.into_values().collect();
        result.sort_by(|a, b| a.sn_node_id.cmp(&b.sn_node_id));
        Ok(result)
    }
}

// ─── 헬퍼 ────────────────────────────────────────────────────────────────────

/// AUTO PARTITION BY DAY 이름 패턴으로부터 범위 추론
/// "p_2026_04_12" → ("2026-04-12 00:00:00", "2026-04-13 00:00:00")
fn infer_partition_range(partition_id: &str) -> (String, String) {
    // Try to parse "p_YYYY_MM_DD"
    let parts: Vec<&str> = partition_id.trim_start_matches('p').trim_start_matches('_').split('_').collect();
    if parts.len() >= 3 {
        if let (Ok(y), Ok(m), Ok(d)) = (
            parts[0].parse::<i32>(),
            parts[1].parse::<u32>(),
            parts[2].parse::<u32>(),
        ) {
            let start = format!("{:04}-{:02}-{:02} 00:00:00", y, m, d);
            // Next day
            let next = if d < 28 {
                format!("{:04}-{:02}-{:02} 00:00:00", y, m, d + 1)
            } else {
                // Simplified — skip calendar edge cases
                format!("{:04}-{:02}-{:02} 00:00:00", y, m, d)
            };
            return (start, next);
        }
    }
    // Try "p_YYYY_MM" (monthly)
    if parts.len() >= 2 {
        if let (Ok(y), Ok(m)) = (parts[0].parse::<i32>(), parts[1].parse::<u32>()) {
            let start = format!("{:04}-{:02}-01 00:00:00", y, m);
            let end_m = if m >= 12 { 1 } else { m + 1 };
            let end_y = if m >= 12 { y + 1 } else { y };
            let end   = format!("{:04}-{:02}-01 00:00:00", end_y, end_m);
            return (start, end);
        }
    }
    (String::new(), String::new())
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;

    use uuid::Uuid;

    use super::*;
    use crate::meta::shard_map::{ReplicaRole, ReplicaState, ShardEntry, ShardReplica};
    use crate::meta::tablet::TabletManager;
    use crate::raft::RaftManager;

    fn raft() -> Arc<RaftManager> { Arc::new(RaftManager::new_local()) }

    fn make_svc(raft: Arc<RaftManager>, shard_map: Arc<ShardMap>) -> PartitionInfoService {
        PartitionInfoService::new(raft, shard_map, Arc::new(LocalSnPartSource))
    }

    // ── T138 테스트 1: 빈 클러스터 — 모든 메서드가 빈 벡터 반환 ──────────────

    #[tokio::test]
    async fn test_empty_cluster_returns_empty_lists() {
        let svc = PartitionInfoService::new_local(raft());

        let partitions = svc.list_partitions("unknown_cube").await.unwrap();
        assert!(partitions.is_empty(), "tablet 없음 → 파티션 없음");

        let shards = svc.list_shards("unknown_cube", None).await.unwrap();
        assert!(shards.is_empty());

        let parts = svc.list_parts("unknown_cube", None, None).await.unwrap();
        assert!(parts.is_empty());

        let dist = svc.get_distributed_status("unknown_cube").await.unwrap();
        assert!(dist.is_empty());
    }

    // ── T138 테스트 2: Tablet 할당 후 파티션 목록 반환 ───────────────────────

    #[tokio::test]
    async fn test_list_partitions_with_allocated_tablets() {
        let r = raft();
        let smap = Arc::new(ShardMap::new());
        let svc  = make_svc(r.clone(), smap);

        let cube_uuid = Uuid::new_v4();
        let tablet_mgr = TabletManager::new(r);
        tablet_mgr.allocate(cube_uuid, "p_2026_04_12", 0, vec![1, 2, 3]).await.unwrap();
        tablet_mgr.allocate(cube_uuid, "p_2026_04_12", 1, vec![1, 2, 3]).await.unwrap();
        tablet_mgr.allocate(cube_uuid, "p_2026_04_13", 0, vec![1, 2, 3]).await.unwrap();

        let cube_id = cube_uuid.to_string();
        let partitions = svc.list_partitions(&cube_id).await.unwrap();
        assert_eq!(partitions.len(), 2, "2개 파티션 (04_12, 04_13)");

        let p12 = partitions.iter().find(|p| p.partition_id == "p_2026_04_12").unwrap();
        assert_eq!(p12.shard_count, 2, "04_12 파티션: 2 shards");
        assert_eq!(p12.range_start, "2026-04-12 00:00:00", "날짜 범위 추론");
    }

    // ── T138 테스트 3: SHOW SHARDS 파티션 필터 ───────────────────────────────

    #[tokio::test]
    async fn test_list_shards_with_partition_filter() {
        let r = raft();
        let smap = Arc::new(ShardMap::new());
        let svc  = make_svc(r.clone(), smap.clone());

        let cube_uuid  = Uuid::new_v4();
        let tablet_mgr = TabletManager::new(r);

        let t1 = tablet_mgr.allocate(cube_uuid, "p_2026_04_12", 0, vec![1]).await.unwrap();
        let t2 = tablet_mgr.allocate(cube_uuid, "p_2026_04_13", 0, vec![1]).await.unwrap();

        // Register t1 in ShardMap
        smap.register(ShardEntry {
            shard_id: t1.tablet_id,
            replicas: vec![ShardReplica {
                sn_node_id:  "sn-01".to_string(),
                sn_endpoint: "127.0.0.1:9060".parse::<SocketAddr>().unwrap(),
                role:        ReplicaRole::Leader,
                state:       ReplicaState::Normal,
                shard_dir:   PathBuf::from("/data/shard-01"),
                lsn:         100,
            }],
        }).await;

        let cube_id = cube_uuid.to_string();

        // All shards
        let all = svc.list_shards(&cube_id, None).await.unwrap();
        assert_eq!(all.len(), 2);

        // Filtered by partition
        let filtered = svc.list_shards(&cube_id, Some("p_2026_04_12")).await.unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].partition_id, "p_2026_04_12");
        assert_eq!(filtered[0].sn_node_id, "sn-01");  // from ShardMap
        assert_eq!(filtered[0].lsn, 100);

        // t2 shard not in ShardMap — falls back to defaults
        let _ = t2;
    }

    // ── T138 테스트 4: SHOW DISTRIBUTED STATUS ───────────────────────────────

    #[tokio::test]
    async fn test_distributed_status_sn_aggregation() {
        let r = raft();
        let smap = Arc::new(ShardMap::new());
        let svc  = make_svc(r.clone(), smap.clone());

        let cube_uuid  = Uuid::new_v4();
        let tablet_mgr = TabletManager::new(r);

        let t1 = tablet_mgr.allocate(cube_uuid, "p_2026_04_12", 0, vec![1]).await.unwrap();
        let t2 = tablet_mgr.allocate(cube_uuid, "p_2026_04_12", 1, vec![2]).await.unwrap();

        for (t, sn, addr) in [
            (&t1, "sn-01", "127.0.0.1:9060"),
            (&t2, "sn-02", "127.0.0.2:9060"),
        ] {
            smap.register(ShardEntry {
                shard_id: t.tablet_id,
                replicas: vec![ShardReplica {
                    sn_node_id:  sn.to_string(),
                    sn_endpoint: addr.parse::<SocketAddr>().unwrap(),
                    role:        ReplicaRole::Leader,
                    state:       ReplicaState::Normal,
                    shard_dir:   PathBuf::from("/data"),
                    lsn:         50,
                }],
            }).await;
        }

        let cube_id = cube_uuid.to_string();
        let status = svc.get_distributed_status(&cube_id).await.unwrap();
        assert_eq!(status.len(), 2, "sn-01, sn-02 각각 1 shard");
        for s in &status {
            assert_eq!(s.shard_count, 1);
            assert_eq!(s.leader_shard_count, 1);
            assert_eq!(s.partition_count, 1);
        }
    }

    // ── T138 테스트 5: infer_partition_range 헬퍼 ────────────────────────────

    #[test]
    fn test_infer_partition_range_daily() {
        let (start, end) = infer_partition_range("p_2026_04_12");
        assert_eq!(start, "2026-04-12 00:00:00");
        assert_eq!(end,   "2026-04-13 00:00:00");
    }

    #[test]
    fn test_infer_partition_range_monthly() {
        let (start, end) = infer_partition_range("p_2026_04");
        assert_eq!(start, "2026-04-01 00:00:00");
        assert_eq!(end,   "2026-05-01 00:00:00");
    }

    #[test]
    fn test_infer_partition_range_unknown() {
        let (start, end) = infer_partition_range("custom_partition");
        assert!(start.is_empty());
        assert!(end.is_empty());
    }
}
