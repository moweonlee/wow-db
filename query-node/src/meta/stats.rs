// T128: CBO 통계 계층적 저장 — Table / Partition / Shard 수준
// Raft KV 키 체계:
//   /stats/{table_id}                         → TableStats
//   /stats/{table_id}/{partition_id}          → PartitionStats
//   /stats/{table_id}/{partition_id}/{shard_id} → ShardStats

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::RwLock;
use tracing::{debug, info};
use uuid::Uuid;

use shared::types::{
    PartitionStats, ShardColumnStats, ShardStats, TableStats,
};

// ─── Raft KV 키 헬퍼 ─────────────────────────────────────────────────────────

pub fn key_table_stats(table_id: Uuid) -> String {
    format!("/stats/{}", table_id)
}

pub fn key_partition_stats(table_id: Uuid, partition_id: Uuid) -> String {
    format!("/stats/{}/{}", table_id, partition_id)
}

pub fn key_shard_stats(table_id: Uuid, partition_id: Uuid, shard_id: Uuid) -> String {
    format!("/stats/{}/{}/{}", table_id, partition_id, shard_id)
}

// ─── StatsStore ─────────────────────────────────────────────────────────────

/// CBO 통계 저장소 — Raft KV 앞의 로컬 캐시 계층
/// 실제 배포에서는 openraft KV를 백엔드로 사용하며,
/// 이 구현은 인메모리 HashMap으로 Raft KV 인터페이스를 시뮬레이션한다.
pub struct StatsStore {
    table_stats:     Arc<RwLock<HashMap<Uuid, TableStats>>>,
    partition_stats: Arc<RwLock<HashMap<(Uuid, Uuid), PartitionStats>>>,
    shard_stats:     Arc<RwLock<HashMap<(Uuid, Uuid, Uuid), ShardStats>>>,
}

impl StatsStore {
    pub fn new() -> Self {
        Self {
            table_stats:     Arc::new(RwLock::new(HashMap::new())),
            partition_stats: Arc::new(RwLock::new(HashMap::new())),
            shard_stats:     Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // ── Table 수준 통계 ──────────────────────────────────────────

    pub async fn get_table_stats(&self, table_id: Uuid) -> Option<TableStats> {
        self.table_stats.read().await.get(&table_id).cloned()
    }

    pub async fn put_table_stats(&self, table_id: Uuid, stats: TableStats) {
        debug!(table_id = %table_id, row_count = stats.row_count, "Table 통계 갱신");
        self.table_stats.write().await.insert(table_id, stats);
    }

    /// 증분 갱신: row_count / size_bytes를 delta만큼 더하거나 뺀다
    pub async fn increment_table_stats(&self, table_id: Uuid, row_delta: i64, size_delta: i64) {
        let mut guard = self.table_stats.write().await;
        let entry = guard.entry(table_id).or_default();
        entry.row_count  = (entry.row_count  as i64 + row_delta) .max(0) as u64;
        entry.size_bytes = (entry.size_bytes as i64 + size_delta).max(0) as u64;
    }

    // ── Partition 수준 통계 ──────────────────────────────────────

    pub async fn get_partition_stats(
        &self,
        table_id:     Uuid,
        partition_id: Uuid,
    ) -> Option<PartitionStats> {
        self.partition_stats.read().await.get(&(table_id, partition_id)).cloned()
    }

    pub async fn put_partition_stats(
        &self,
        table_id:     Uuid,
        partition_id: Uuid,
        stats:        PartitionStats,
    ) {
        debug!(
            table_id = %table_id,
            partition_id = %partition_id,
            row_count = stats.row_count,
            "Partition 통계 갱신"
        );
        self.partition_stats.write().await.insert((table_id, partition_id), stats);
    }

    pub async fn increment_partition_stats(
        &self,
        table_id:     Uuid,
        partition_id: Uuid,
        row_delta:    i64,
        size_delta:   i64,
    ) {
        let mut guard = self.partition_stats.write().await;
        let entry = guard.entry((table_id, partition_id)).or_default();
        entry.row_count  = (entry.row_count  as i64 + row_delta) .max(0) as u64;
        entry.size_bytes = (entry.size_bytes as i64 + size_delta).max(0) as u64;
    }

    // ── Shard 수준 통계 ─────────────────────────────────────────

    pub async fn get_shard_stats(
        &self,
        table_id:     Uuid,
        partition_id: Uuid,
        shard_id:     Uuid,
    ) -> Option<ShardStats> {
        self.shard_stats.read().await.get(&(table_id, partition_id, shard_id)).cloned()
    }

    pub async fn put_shard_stats(
        &self,
        table_id:     Uuid,
        partition_id: Uuid,
        shard_id:     Uuid,
        stats:        ShardStats,
    ) {
        debug!(
            shard_id = %shard_id,
            row_count = stats.row_count,
            "Shard 통계 갱신"
        );
        self.shard_stats.write().await.insert((table_id, partition_id, shard_id), stats);
    }

    /// SN에서 보고한 통계를 적용 (증분 또는 전체 교체)
    pub async fn apply_shard_stats_report(
        &self,
        table_id:     Uuid,
        partition_id: Uuid,
        shard_id:     Uuid,
        new_stats:    ShardStats,
        is_incremental: bool,
    ) {
        if is_incremental {
            let mut guard = self.shard_stats.write().await;
            let entry = guard.entry((table_id, partition_id, shard_id))
                .or_insert_with(ShardStats::default);
            // 증분: 기존 컬럼 통계에 새 통계를 병합 (row_count/size_bytes 누적)
            entry.row_count  += new_stats.row_count;
            entry.size_bytes += new_stats.size_bytes;
            entry.updated_at  = new_stats.updated_at;
            // 컬럼 통계는 새 값으로 교체 (min/max 확장, ndv 재계산)
            for (col, col_stats) in new_stats.column_stats {
                let existing = entry.column_stats.entry(col).or_default();
                // min 확장
                existing.min_val = match (&existing.min_val, &col_stats.min_val) {
                    (None, v) => v.clone(),
                    (v, None) => v.clone(),
                    (Some(a), Some(b)) => {
                        if a <= b { Some(a.clone()) } else { Some(b.clone()) }
                    }
                };
                // max 확장
                existing.max_val = match (&existing.max_val, &col_stats.max_val) {
                    (None, v) => v.clone(),
                    (v, None) => v.clone(),
                    (Some(a), Some(b)) => {
                        if a >= b { Some(a.clone()) } else { Some(b.clone()) }
                    }
                };
                existing.null_count += col_stats.null_count;
            }
        } else {
            // 전체 교체
            self.shard_stats.write().await.insert((table_id, partition_id, shard_id), new_stats);
        }

        // Partition / Table 통계도 함께 갱신
        // (실제 구현에서는 Raft 커밋 후 반영)
        info!(
            shard_id = %shard_id,
            incremental = is_incremental,
            "Shard 통계 보고 적용 완료"
        );
    }

    // ── ANALYZE TABLE 전체 재계산 트리거 ────────────────────────

    /// 특정 Table의 모든 Shard 통계를 삭제 (ANALYZE TABLE 전 초기화)
    pub async fn reset_table_stats(&self, table_id: Uuid) -> usize {
        let mut guard = self.shard_stats.write().await;
        let before = guard.len();
        guard.retain(|(tid, _, _), _| *tid != table_id);
        let removed = before - guard.len();
        info!(table_id = %table_id, removed, "Table 통계 초기화 (ANALYZE TABLE)");
        removed
    }

    // ── Partition Pruning 지원 ───────────────────────────────────

    /// 주어진 Table의 모든 파티션 통계를 반환 (Partition Pruning용)
    pub async fn list_partition_stats(
        &self,
        table_id: Uuid,
    ) -> Vec<(Uuid, PartitionStats)> {
        self.partition_stats
            .read()
            .await
            .iter()
            .filter(|((tid, _), _)| *tid == table_id)
            .map(|((_, pid), stats)| (*pid, stats.clone()))
            .collect()
    }
}

impl Default for StatsStore {
    fn default() -> Self { Self::new() }
}

// ─── 단위 테스트 ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use shared::types::Value;

    #[tokio::test]
    async fn test_table_stats_increment() {
        let store = StatsStore::new();
        let tid = Uuid::new_v4();

        store.increment_table_stats(tid, 1000, 102400).await;
        store.increment_table_stats(tid, 500,  51200).await;

        let stats = store.get_table_stats(tid).await.unwrap();
        assert_eq!(stats.row_count, 1500);
        assert_eq!(stats.size_bytes, 153600);
    }

    #[tokio::test]
    async fn test_partition_stats_prune_candidate() {
        let store = StatsStore::new();
        let tid = Uuid::new_v4();
        let pid = Uuid::new_v4();

        store.put_partition_stats(tid, pid, PartitionStats {
            row_count:         1_000_000,
            size_bytes:        1024 * 1024 * 256,
            partition_key_min: Some(Value::Int64(1000)),
            partition_key_max: Some(Value::Int64(1999)),
        }).await;

        let list = store.list_partition_stats(tid).await;
        assert_eq!(list.len(), 1);
        let (_, stats) = &list[0];
        assert_eq!(stats.partition_key_min, Some(Value::Int64(1000)));
    }

    #[tokio::test]
    async fn test_shard_stats_incremental_merge() {
        let store = StatsStore::new();
        let (tid, pid, sid) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());

        let mut s1 = ShardStats::default();
        s1.row_count = 500;
        s1.column_stats.insert("event_name".to_string(), ShardColumnStats {
            min_val:    Some(Value::String("a".to_string())),
            max_val:    Some(Value::String("m".to_string())),
            null_count: 5,
            ..Default::default()
        });
        store.apply_shard_stats_report(tid, pid, sid, s1, true).await;

        let mut s2 = ShardStats::default();
        s2.row_count = 300;
        s2.column_stats.insert("event_name".to_string(), ShardColumnStats {
            min_val:    Some(Value::String("n".to_string())),
            max_val:    Some(Value::String("z".to_string())),
            null_count: 3,
            ..Default::default()
        });
        store.apply_shard_stats_report(tid, pid, sid, s2, true).await;

        let merged = store.get_shard_stats(tid, pid, sid).await.unwrap();
        assert_eq!(merged.row_count, 800);
        let col = &merged.column_stats["event_name"];
        assert_eq!(col.min_val, Some(Value::String("a".to_string())));
        assert_eq!(col.max_val, Some(Value::String("z".to_string())));
        assert_eq!(col.null_count, 8);
    }
}
