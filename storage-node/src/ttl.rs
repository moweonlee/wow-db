// T074: TTL 만료 — Compaction 시 파티션 단위 행 자동 삭제

use std::time::{SystemTime, UNIX_EPOCH};

use tracing::{debug, info};

// ─── TTL 정책 ─────────────────────────────────────────────────────────────────

/// 파티션 또는 행 수준 TTL 정책
#[derive(Debug, Clone)]
pub enum TtlPolicy {
    /// 파티션 전체 삭제 (파티션 생성 시각 기준)
    Partition {
        /// 보존 기간 (초)
        retain_seconds: u64,
    },
    /// 특정 컬럼의 timestamp 기준 행 단위 삭제
    Row {
        /// timestamp 컬럼명 (Unix epoch seconds 또는 milliseconds)
        timestamp_col: String,
        /// 보존 기간 (초)
        retain_seconds: u64,
        /// timestamp 단위 (1 = 초, 1000 = 밀리초)
        ts_unit: u64,
    },
}

impl TtlPolicy {
    /// 현재 시각 기준 만료 경계 Unix timestamp (초)
    pub fn expiry_boundary_secs(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let retain = match self {
            TtlPolicy::Partition { retain_seconds } => *retain_seconds,
            TtlPolicy::Row { retain_seconds, .. }   => *retain_seconds,
        };
        now.saturating_sub(retain)
    }
}

// ─── TTL 필터 ─────────────────────────────────────────────────────────────────

/// Compaction 중 TTL 만료 행/파티션 필터
pub struct TtlFilter {
    policy: TtlPolicy,
}

impl TtlFilter {
    pub fn new(policy: TtlPolicy) -> Self {
        Self { policy }
    }

    /// 파티션 자체가 만료되었는지 확인
    /// `partition_created_at`: 파티션 생성 Unix timestamp (초)
    pub fn is_partition_expired(&self, partition_created_at: u64) -> bool {
        match &self.policy {
            TtlPolicy::Partition { .. } => {
                partition_created_at < self.policy.expiry_boundary_secs()
            }
            TtlPolicy::Row { .. } => false,  // Row 정책은 파티션 단위 삭제 없음
        }
    }

    /// 행의 timestamp가 만료되었는지 확인
    /// `row_ts_raw`: 행의 timestamp 컬럼 원시값
    pub fn is_row_expired(&self, row_ts_raw: i64) -> bool {
        match &self.policy {
            TtlPolicy::Row { ts_unit, .. } => {
                // 원시값을 초로 정규화
                let ts_secs = if *ts_unit > 1 {
                    (row_ts_raw as u64) / ts_unit
                } else {
                    row_ts_raw as u64
                };
                ts_secs < self.policy.expiry_boundary_secs()
            }
            TtlPolicy::Partition { .. } => false,
        }
    }

    /// 행 배치 필터링 — 만료된 행 제거
    /// `timestamps`: 각 행의 timestamp 컬럼 값 (raw)
    /// 반환: 유효한 행의 인덱스 목록
    pub fn filter_rows(&self, timestamps: &[i64]) -> Vec<usize> {
        timestamps.iter()
            .enumerate()
            .filter_map(|(i, &ts)| {
                if self.is_row_expired(ts) {
                    debug!(ts = ts, row = i, "TTL 만료 행 제거");
                    None
                } else {
                    Some(i)
                }
            })
            .collect()
    }

    pub fn policy(&self) -> &TtlPolicy {
        &self.policy
    }
}

// ─── Compaction TTL 통합 ──────────────────────────────────────────────────────

/// Compaction 실행 시 TTL 처리 결과
#[derive(Debug, Default)]
pub struct TtlCompactionResult {
    pub partitions_dropped:   u32,
    pub rows_deleted:         u64,
    pub partitions_scanned:   u32,
    pub rows_scanned:         u64,
}

impl TtlCompactionResult {
    pub fn merge(&mut self, other: TtlCompactionResult) {
        self.partitions_dropped += other.partitions_dropped;
        self.rows_deleted       += other.rows_deleted;
        self.partitions_scanned += other.partitions_scanned;
        self.rows_scanned       += other.rows_scanned;
    }

    pub fn log_summary(&self, cube_name: &str) {
        info!(
            cube     = %cube_name,
            partitions_dropped = self.partitions_dropped,
            rows_deleted       = self.rows_deleted,
            partitions_scanned = self.partitions_scanned,
            rows_scanned       = self.rows_scanned,
            "TTL compaction summary"
        );
    }
}

/// Compaction 도중 TTL을 적용하는 핵심 로직
///
/// 실제 Compaction 파이프라인에서 호출:
/// - 파티션 레벨: `process_partition()` → 전체 파티션 폴더 삭제 여부 결정
/// - 행 레벨: `filter_rows()` → SSTable merge 시 만료 행 제외
pub struct TtlCompactor {
    filter: TtlFilter,
}

impl TtlCompactor {
    pub fn new(policy: TtlPolicy) -> Self {
        Self { filter: TtlFilter::new(policy) }
    }

    /// 파티션 처리 — 만료 여부 반환
    /// `partition_name`: "p_2024_q1" 형태
    /// `created_at`: 파티션 생성 Unix timestamp (초)
    pub fn should_drop_partition(&self, partition_name: &str, created_at: u64) -> bool {
        if self.filter.is_partition_expired(created_at) {
            info!(partition = %partition_name, "TTL: 파티션 만료 → 삭제 예정");
            true
        } else {
            false
        }
    }

    /// 행 레벨 필터링 — 유효한 행 인덱스 반환
    pub fn filter_rows(&self, timestamps: &[i64]) -> (Vec<usize>, u64) {
        let before = timestamps.len() as u64;
        let valid = self.filter.filter_rows(timestamps);
        let deleted = before - valid.len() as u64;
        (valid, deleted)
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_ttl_expiry() {
        let policy = TtlPolicy::Partition { retain_seconds: 86400 }; // 1일
        let filter = TtlFilter::new(policy);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // 2일 전 생성 → 만료
        let old_ts = now - 2 * 86400;
        assert!(filter.is_partition_expired(old_ts));

        // 1시간 전 생성 → 유효
        let new_ts = now - 3600;
        assert!(!filter.is_partition_expired(new_ts));
    }

    #[test]
    fn test_row_ttl_seconds() {
        let policy = TtlPolicy::Row {
            timestamp_col:  "event_time".to_string(),
            retain_seconds: 86400,
            ts_unit:        1,
        };
        let filter = TtlFilter::new(policy);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        assert!(filter.is_row_expired(now - 2 * 86400), "2일 전 행 → 만료");
        assert!(!filter.is_row_expired(now - 3600),     "1시간 전 행 → 유효");
    }

    #[test]
    fn test_row_ttl_milliseconds() {
        let policy = TtlPolicy::Row {
            timestamp_col:  "event_time".to_string(),
            retain_seconds: 86400,
            ts_unit:        1000,  // 밀리초
        };
        let filter = TtlFilter::new(policy);

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        assert!(filter.is_row_expired(now_ms - 2 * 86400 * 1000), "2일 전(ms) → 만료");
        assert!(!filter.is_row_expired(now_ms - 3600 * 1000),     "1시간 전(ms) → 유효");
    }

    #[test]
    fn test_filter_rows_batch() {
        let policy = TtlPolicy::Row {
            timestamp_col:  "ts".to_string(),
            retain_seconds: 86400,
            ts_unit:        1,
        };
        let filter = TtlFilter::new(policy);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let timestamps = vec![
            now - 3 * 86400,  // 만료 (row 0)
            now - 3600,       // 유효 (row 1)
            now - 5 * 86400,  // 만료 (row 2)
            now - 1800,       // 유효 (row 3)
        ];

        let valid = filter.filter_rows(&timestamps);
        assert_eq!(valid, vec![1, 3]);
    }

    #[test]
    fn test_compactor_should_drop_partition() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let compactor = TtlCompactor::new(TtlPolicy::Partition { retain_seconds: 86400 });

        assert!(compactor.should_drop_partition("p_old", now - 2 * 86400));
        assert!(!compactor.should_drop_partition("p_new", now - 3600));
    }

    #[test]
    fn test_compaction_result_merge() {
        let mut r1 = TtlCompactionResult { partitions_dropped: 1, rows_deleted: 100, partitions_scanned: 5, rows_scanned: 500 };
        let r2     = TtlCompactionResult { partitions_dropped: 2, rows_deleted: 200, partitions_scanned: 3, rows_scanned: 300 };
        r1.merge(r2);
        assert_eq!(r1.partitions_dropped, 3);
        assert_eq!(r1.rows_deleted, 300);
    }
}
