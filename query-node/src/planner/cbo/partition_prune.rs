// T131: Partition Pruning (FR-037)
// WHERE predicate와 PartitionStats.partition_key_min/max 비교로 범위 밖 Partition 조기 제거

use std::cmp::Ordering;

use uuid::Uuid;

use shared::types::{PartitionStats, Value};

// ─── Predicate 표현 ──────────────────────────────────────────────────────────

/// Partition Pruning을 위한 범위 predicate
#[derive(Debug, Clone)]
pub enum PartitionPredicate {
    /// col >= value
    Ge { column: String, value: Value },
    /// col > value
    Gt { column: String, value: Value },
    /// col <= value
    Le { column: String, value: Value },
    /// col < value
    Lt { column: String, value: Value },
    /// col = value
    Eq { column: String, value: Value },
    /// value_min <= col < value_max (BETWEEN 단축형)
    Between { column: String, min: Value, max: Value },
}

impl PartitionPredicate {
    pub fn column(&self) -> &str {
        match self {
            Self::Ge { column, .. }
            | Self::Gt { column, .. }
            | Self::Le { column, .. }
            | Self::Lt { column, .. }
            | Self::Eq { column, .. }
            | Self::Between { column, .. } => column,
        }
    }
}

// ─── PartitionPruner ────────────────────────────────────────────────────────

pub struct PartitionPruner;

impl PartitionPruner {
    /// 주어진 predicates를 기반으로 파티션 목록에서 제거 대상을 필터링한다.
    ///
    /// # 반환
    /// 살아남은 파티션 ID 목록 (스캔해야 할 파티션만 포함)
    pub fn prune(
        partitions: &[(Uuid, PartitionStats)],
        predicates: &[PartitionPredicate],
    ) -> Vec<Uuid> {
        partitions
            .iter()
            .filter(|(_, stats)| Self::should_include(stats, predicates))
            .map(|(pid, _)| *pid)
            .collect()
    }

    /// 단일 파티션이 predicate 집합에서 살아남아야 하는지 판단
    fn should_include(stats: &PartitionStats, predicates: &[PartitionPredicate]) -> bool {
        // predicate가 없으면 모든 파티션 포함
        if predicates.is_empty() {
            return true;
        }
        // 모든 predicate에 대해 "스킵 불가"면 포함
        // 하나라도 "확실히 범위 밖"이면 제거
        for pred in predicates {
            if Self::can_skip(stats, pred) {
                return false;
            }
        }
        true
    }

    /// 파티션이 이 predicate에 의해 확실히 범위 밖인지 판단
    /// true → 이 파티션은 스킵 가능 (포함 불필요)
    fn can_skip(stats: &PartitionStats, pred: &PartitionPredicate) -> bool {
        let (p_min, p_max) = match (&stats.partition_key_min, &stats.partition_key_max) {
            (Some(mn), Some(mx)) => (mn, mx),
            _ => return false, // 통계 없으면 스킵 불가 (보수적 판단)
        };

        match pred {
            // col >= value: 파티션의 max가 value보다 작으면 스킵
            PartitionPredicate::Ge { value, .. }
            | PartitionPredicate::Gt { value, .. } => {
                matches!(p_max.partial_cmp(value), Some(Ordering::Less))
            }

            // col <= value: 파티션의 min이 value보다 크면 스킵 (min > value)
            PartitionPredicate::Le { value, .. } => {
                matches!(p_min.partial_cmp(value), Some(Ordering::Greater))
            }

            // col < value: 파티션의 min이 value 이상이면 스킵 (min >= value)
            PartitionPredicate::Lt { value, .. } => {
                matches!(
                    p_min.partial_cmp(value),
                    Some(Ordering::Greater) | Some(Ordering::Equal)
                )
            }

            // col = value: 파티션의 [min, max] 범위 밖이면 스킵
            PartitionPredicate::Eq { value, .. } => {
                let below_min = matches!(p_min.partial_cmp(value), Some(Ordering::Greater));
                let above_max = matches!(p_max.partial_cmp(value), Some(Ordering::Less));
                below_min || above_max
            }

            // value_min <= col < value_max
            PartitionPredicate::Between { min, max, .. } => {
                // 파티션 최댓값 < 쿼리 최솟값: 파티션 전체가 범위 앞
                let partition_before_range =
                    matches!(p_max.partial_cmp(min), Some(Ordering::Less));
                // 파티션 최솟값 >= 쿼리 최댓값: 파티션 전체가 범위 뒤
                let partition_after_range = matches!(
                    p_min.partial_cmp(max),
                    Some(Ordering::Greater) | Some(Ordering::Equal)
                );
                partition_before_range || partition_after_range
            }
        }
    }
}

// ─── 단위 테스트 ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_partition(min: i64, max: i64) -> PartitionStats {
        PartitionStats {
            row_count:         1_000_000,
            size_bytes:        1024 * 1024 * 100,
            partition_key_min: Some(Value::Int64(min)),
            partition_key_max: Some(Value::Int64(max)),
        }
    }

    #[test]
    fn test_prune_ge_removes_partitions_before_range() {
        let p1_id = Uuid::new_v4();
        let p2_id = Uuid::new_v4();
        let p3_id = Uuid::new_v4();

        let partitions = vec![
            (p1_id, make_partition(1000, 1999)),  // 완전히 범위 앞 → 제거
            (p2_id, make_partition(2000, 2999)),  // 경계 포함 → 유지
            (p3_id, make_partition(3000, 3999)),  // 범위 안 → 유지
        ];

        let predicates = vec![
            PartitionPredicate::Ge {
                column: "event_time".to_string(),
                value:  Value::Int64(2000),
            },
        ];

        let result = PartitionPruner::prune(&partitions, &predicates);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&p2_id));
        assert!(result.contains(&p3_id));
        assert!(!result.contains(&p1_id));
    }

    #[test]
    fn test_prune_lt_removes_partitions_after_range() {
        let p1_id = Uuid::new_v4();
        let p2_id = Uuid::new_v4();
        let p3_id = Uuid::new_v4();

        let partitions = vec![
            (p1_id, make_partition(1000, 1999)),
            (p2_id, make_partition(2000, 2999)),
            (p3_id, make_partition(3000, 3999)),  // 완전히 범위 밖 → 제거
        ];

        let predicates = vec![
            PartitionPredicate::Lt {
                column: "event_time".to_string(),
                value:  Value::Int64(3000),
            },
        ];

        let result = PartitionPruner::prune(&partitions, &predicates);
        assert_eq!(result.len(), 2);
        assert!(!result.contains(&p3_id));
    }

    #[test]
    fn test_prune_between_keeps_overlapping_partitions() {
        let p1_id = Uuid::new_v4(); // 1000~1999: 범위 앞 → 제거
        let p2_id = Uuid::new_v4(); // 2000~2999: 범위 겹침 → 유지
        let p3_id = Uuid::new_v4(); // 3000~3999: 범위 안 → 유지
        let p4_id = Uuid::new_v4(); // 4000~4999: 범위 뒤 → 제거

        let partitions = vec![
            (p1_id, make_partition(1000, 1999)),
            (p2_id, make_partition(2000, 2999)),
            (p3_id, make_partition(3000, 3999)),
            (p4_id, make_partition(4000, 4999)),
        ];

        let predicates = vec![
            PartitionPredicate::Between {
                column: "event_time".to_string(),
                min:    Value::Int64(2500),
                max:    Value::Int64(3500),
            },
        ];

        let result = PartitionPruner::prune(&partitions, &predicates);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&p2_id));
        assert!(result.contains(&p3_id));
    }

    #[test]
    fn test_prune_eq_exact_match() {
        let p1_id = Uuid::new_v4(); // 1000~1999
        let p2_id = Uuid::new_v4(); // 2000~2999: value=2500 포함 → 유지
        let p3_id = Uuid::new_v4(); // 3000~3999

        let partitions = vec![
            (p1_id, make_partition(1000, 1999)),
            (p2_id, make_partition(2000, 2999)),
            (p3_id, make_partition(3000, 3999)),
        ];

        let predicates = vec![
            PartitionPredicate::Eq {
                column: "event_time".to_string(),
                value:  Value::Int64(2500),
            },
        ];

        let result = PartitionPruner::prune(&partitions, &predicates);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&p2_id));
    }

    #[test]
    fn test_prune_no_predicates_returns_all() {
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        let partitions: Vec<_> = ids.iter()
            .map(|&id| (id, make_partition(0, 999)))
            .collect();
        let result = PartitionPruner::prune(&partitions, &[]);
        assert_eq!(result.len(), 3);
    }
}
