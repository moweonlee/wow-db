// T068: Auto Partition — INSERT 시 매핑 파티션 부재 시 파티션 생성, 일/월/연 시간 단위

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::sync::RwLock;
use tracing::{info, warn};

// ─── 파티션 시간 단위 ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeGranularity {
    Day,
    Month,
    Year,
}

impl TimeGranularity {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "DAY"   => Some(TimeGranularity::Day),
            "MONTH" => Some(TimeGranularity::Month),
            "YEAR"  => Some(TimeGranularity::Year),
            _       => None,
        }
    }
}

// ─── 파티션 범위 ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    /// 파티션 이름 (예: p_2024_01, p_2024_q1, p_2024)
    pub name:   String,
    /// 파티션 시작 타임스탬프 (Unix 초, inclusive)
    pub lo_ts:  i64,
    /// 파티션 종료 타임스탬프 (Unix 초, exclusive)
    pub hi_ts:  i64,
}

impl PartitionKey {
    /// 타임스탬프가 이 파티션에 속하는지 확인
    pub fn contains(&self, ts: i64) -> bool {
        ts >= self.lo_ts && ts < self.hi_ts
    }
}

// ─── Auto Partition Manager ──────────────────────────────────────────────────

/// 큐브별 파티션 맵 (partition_name → PartitionKey)
pub struct AutoPartitionManager {
    /// cube_name → 파티션 목록
    partitions:  Arc<RwLock<HashMap<String, Vec<PartitionKey>>>>,
    granularity: TimeGranularity,
}

impl AutoPartitionManager {
    pub fn new(granularity: TimeGranularity) -> Self {
        Self {
            partitions:  Arc::new(RwLock::new(HashMap::new())),
            granularity,
        }
    }

    /// 타임스탬프에 맞는 파티션 조회 또는 자동 생성
    pub async fn get_or_create(
        &self,
        cube_name: &str,
        ts: i64,
    ) -> Result<PartitionKey> {
        // 1. 기존 파티션 검색
        {
            let partitions = self.partitions.read().await;
            if let Some(list) = partitions.get(cube_name) {
                if let Some(pk) = list.iter().find(|p| p.contains(ts)) {
                    return Ok(pk.clone());
                }
            }
        }

        // 2. 파티션 없음 → 자동 생성
        let new_partition = self.create_partition(ts);
        info!(
            cube  = %cube_name,
            part  = %new_partition.name,
            lo_ts = new_partition.lo_ts,
            hi_ts = new_partition.hi_ts,
            "Auto partition created"
        );

        let mut partitions = self.partitions.write().await;
        let list = partitions.entry(cube_name.to_string()).or_default();
        // 중복 방지
        if !list.iter().any(|p| p.name == new_partition.name) {
            list.push(new_partition.clone());
            list.sort_by_key(|p| p.lo_ts);
        }
        Ok(new_partition)
    }

    /// 타임스탬프로부터 파티션 범위 계산
    fn create_partition(&self, ts: i64) -> PartitionKey {
        use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};

        let dt = Utc.timestamp_opt(ts, 0).single()
            .unwrap_or_else(Utc::now);

        match self.granularity {
            TimeGranularity::Day => {
                let lo = Utc
                    .with_ymd_and_hms(dt.year(), dt.month(), dt.day(), 0, 0, 0)
                    .single().unwrap();
                let hi = lo + chrono::Duration::days(1);
                PartitionKey {
                    name:  format!("p_{:04}_{:02}_{:02}", dt.year(), dt.month(), dt.day()),
                    lo_ts: lo.timestamp(),
                    hi_ts: hi.timestamp(),
                }
            }
            TimeGranularity::Month => {
                let lo = Utc
                    .with_ymd_and_hms(dt.year(), dt.month(), 1, 0, 0, 0)
                    .single().unwrap();
                let (next_year, next_month) = if dt.month() == 12 {
                    (dt.year() + 1, 1)
                } else {
                    (dt.year(), dt.month() + 1)
                };
                let hi = Utc
                    .with_ymd_and_hms(next_year, next_month, 1, 0, 0, 0)
                    .single().unwrap();
                PartitionKey {
                    name:  format!("p_{:04}_{:02}", dt.year(), dt.month()),
                    lo_ts: lo.timestamp(),
                    hi_ts: hi.timestamp(),
                }
            }
            TimeGranularity::Year => {
                let lo = Utc
                    .with_ymd_and_hms(dt.year(), 1, 1, 0, 0, 0)
                    .single().unwrap();
                let hi = Utc
                    .with_ymd_and_hms(dt.year() + 1, 1, 1, 0, 0, 0)
                    .single().unwrap();
                PartitionKey {
                    name:  format!("p_{:04}", dt.year()),
                    lo_ts: lo.timestamp(),
                    hi_ts: hi.timestamp(),
                }
            }
        }
    }

    /// 큐브의 모든 파티션 목록
    pub async fn list_partitions(&self, cube_name: &str) -> Vec<PartitionKey> {
        self.partitions.read().await
            .get(cube_name)
            .cloned()
            .unwrap_or_default()
    }

    /// 파티션 삭제 (DROP PARTITION / TTL 만료)
    pub async fn drop_partition(&self, cube_name: &str, partition_name: &str) -> bool {
        let mut partitions = self.partitions.write().await;
        if let Some(list) = partitions.get_mut(cube_name) {
            let before = list.len();
            list.retain(|p| p.name != partition_name);
            let removed = list.len() < before;
            if removed {
                warn!(cube = %cube_name, part = %partition_name, "Partition dropped");
            }
            return removed;
        }
        false
    }

    /// 시간 범위에 걸치는 파티션 목록 (파티션 Pruning 지원)
    pub async fn partitions_in_range(
        &self,
        cube_name: &str,
        lo: Option<i64>,
        hi: Option<i64>,
    ) -> Vec<PartitionKey> {
        self.partitions.read().await
            .get(cube_name)
            .map(|list| {
                list.iter()
                    .filter(|p| {
                        let above_lo = lo.map(|l| p.hi_ts > l).unwrap_or(true);
                        let below_hi = hi.map(|h| p.lo_ts < h).unwrap_or(true);
                        above_lo && below_hi
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_auto_create_monthly_partition() {
        let mgr = AutoPartitionManager::new(TimeGranularity::Month);

        // 2024-01-15 12:00:00 UTC
        let ts = chrono::DateTime::parse_from_rfc3339("2024-01-15T12:00:00Z")
            .unwrap().timestamp();

        let pk = mgr.get_or_create("events", ts).await.unwrap();
        assert_eq!(pk.name, "p_2024_01");
        assert!(pk.contains(ts));

        // 같은 달 — 기존 파티션 반환
        let ts2 = chrono::DateTime::parse_from_rfc3339("2024-01-31T23:59:59Z")
            .unwrap().timestamp();
        let pk2 = mgr.get_or_create("events", ts2).await.unwrap();
        assert_eq!(pk2.name, "p_2024_01");

        // 파티션 목록 확인
        let list = mgr.list_partitions("events").await;
        assert_eq!(list.len(), 1);
    }

    #[tokio::test]
    async fn test_auto_create_daily_partition() {
        let mgr = AutoPartitionManager::new(TimeGranularity::Day);

        let ts = chrono::DateTime::parse_from_rfc3339("2024-03-15T08:00:00Z")
            .unwrap().timestamp();
        let pk = mgr.get_or_create("page_events", ts).await.unwrap();
        assert_eq!(pk.name, "p_2024_03_15");
        assert!(pk.contains(ts));
    }

    #[tokio::test]
    async fn test_auto_create_yearly_partition() {
        let mgr = AutoPartitionManager::new(TimeGranularity::Year);

        let ts = chrono::DateTime::parse_from_rfc3339("2024-07-04T00:00:00Z")
            .unwrap().timestamp();
        let pk = mgr.get_or_create("events", ts).await.unwrap();
        assert_eq!(pk.name, "p_2024");
        assert!(pk.contains(ts));
    }

    #[tokio::test]
    async fn test_month_boundary() {
        let mgr = AutoPartitionManager::new(TimeGranularity::Month);

        let dec_ts = chrono::DateTime::parse_from_rfc3339("2024-12-15T00:00:00Z")
            .unwrap().timestamp();
        let jan_ts = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
            .unwrap().timestamp();

        let pk_dec = mgr.get_or_create("events", dec_ts).await.unwrap();
        let pk_jan = mgr.get_or_create("events", jan_ts).await.unwrap();
        assert_eq!(pk_dec.name, "p_2024_12");
        assert_eq!(pk_jan.name, "p_2025_01");

        let list = mgr.list_partitions("events").await;
        assert_eq!(list.len(), 2);
        // 정렬 확인 (lo_ts 기준)
        assert!(list[0].lo_ts < list[1].lo_ts);
    }

    #[tokio::test]
    async fn test_partitions_in_range() {
        let mgr = AutoPartitionManager::new(TimeGranularity::Month);

        for month in 1..=6u32 {
            let ts_str = format!("2024-{:02}-15T00:00:00Z", month);
            let ts = chrono::DateTime::parse_from_rfc3339(&ts_str).unwrap().timestamp();
            mgr.get_or_create("events", ts).await.unwrap();
        }

        // 2024-02 ~ 2024-04 범위
        let lo = chrono::DateTime::parse_from_rfc3339("2024-02-01T00:00:00Z").unwrap().timestamp();
        let hi = chrono::DateTime::parse_from_rfc3339("2024-05-01T00:00:00Z").unwrap().timestamp();
        let range = mgr.partitions_in_range("events", Some(lo), Some(hi)).await;
        assert_eq!(range.len(), 3);
    }

    #[tokio::test]
    async fn test_drop_partition() {
        let mgr = AutoPartitionManager::new(TimeGranularity::Month);

        let ts = chrono::DateTime::parse_from_rfc3339("2024-01-15T00:00:00Z").unwrap().timestamp();
        mgr.get_or_create("events", ts).await.unwrap();

        let removed = mgr.drop_partition("events", "p_2024_01").await;
        assert!(removed);
        assert!(mgr.list_partitions("events").await.is_empty());
    }
}
