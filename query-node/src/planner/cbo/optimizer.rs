// T057: CBO 최적화 — 파티션 Pruning, Join 순서 최적화, 집계 전략 선택

use anyhow::Result;

use super::stats::StatisticsManager;
use crate::planner::logical::{
    LogicalPlan, ScanNode, FilterNode, JoinNode, AggregateNode, JoinType,
    Predicate, Expr, BinaryOp, ScalarValue, PartitionFilter,
};

// ─── 최적화 규칙 ──────────────────────────────────────────────────────────────

pub struct CboOptimizer {
    stats: StatisticsManager,
}

impl CboOptimizer {
    pub fn new(stats: StatisticsManager) -> Self {
        Self { stats }
    }

    /// 논리 계획을 최적화하여 반환
    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let plan = self.push_down_predicates(plan)?;
        let plan = self.partition_pruning(plan)?;
        let plan = self.optimize_join_order(plan)?;
        Ok(plan)
    }

    // ─── Predicate Pushdown ───────────────────────────────────────────────────

    fn push_down_predicates(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(FilterNode { input, predicate }) => {
                match *input {
                    LogicalPlan::Scan(mut scan) => {
                        // Filter를 Scan의 predicates로 내려보내기
                        scan.predicates.push(predicate);
                        Ok(LogicalPlan::Scan(scan))
                    }
                    other => {
                        let optimized = self.push_down_predicates(other)?;
                        Ok(LogicalPlan::Filter(FilterNode {
                            input:     Box::new(optimized),
                            predicate,
                        }))
                    }
                }
            }
            LogicalPlan::Limit { input, n } => {
                Ok(LogicalPlan::Limit {
                    input: Box::new(self.push_down_predicates(*input)?),
                    n,
                })
            }
            other => Ok(other),
        }
    }

    // ─── Partition Pruning ────────────────────────────────────────────────────

    fn partition_pruning(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Scan(mut scan) => {
                // predicates에서 파티션 컬럼 범위 조건 추출
                use shared::types::PartitionVariant;
                let pk_col_opt: Option<String> = match &scan.cube_schema.partition_key.variant {
                    PartitionVariant::Range { column, .. } => Some(column.clone()),
                    PartitionVariant::List  { column, .. } => Some(column.clone()),
                };
                if let Some(pk_col) = pk_col_opt.as_deref() {
                    let mut lo: Option<ScalarValue> = None;
                    let mut hi: Option<ScalarValue> = None;

                    for pred in &scan.predicates {
                        if let Some((plo, phi)) = self.extract_range(&pred.expr, pk_col) {
                            lo = plo.or(lo);
                            hi = phi.or(hi);
                        }
                    }

                    if lo.is_some() || hi.is_some() {
                        scan.partition_filter = Some(PartitionFilter {
                            column: pk_col.to_string(),
                            lo,
                            hi,
                        });
                    }
                }
                Ok(LogicalPlan::Scan(scan))
            }
            LogicalPlan::Filter(FilterNode { input, predicate }) => {
                Ok(LogicalPlan::Filter(FilterNode {
                    input:     Box::new(self.partition_pruning(*input)?),
                    predicate,
                }))
            }
            LogicalPlan::Limit { input, n } => {
                Ok(LogicalPlan::Limit {
                    input: Box::new(self.partition_pruning(*input)?),
                    n,
                })
            }
            other => Ok(other),
        }
    }

    fn extract_range(&self, expr: &Expr, col: &str) -> Option<(Option<ScalarValue>, Option<ScalarValue>)> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match (left.as_ref(), op, right.as_ref()) {
                    (Expr::Column(c), BinaryOp::Gt | BinaryOp::Ge, Expr::Literal(v))
                        if c.column == col =>
                        Some((Some(v.clone()), None)),
                    (Expr::Column(c), BinaryOp::Lt | BinaryOp::Le, Expr::Literal(v))
                        if c.column == col =>
                        Some((None, Some(v.clone()))),
                    (Expr::Column(c), BinaryOp::Eq, Expr::Literal(v))
                        if c.column == col =>
                        Some((Some(v.clone()), Some(v.clone()))),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    // ─── Join 순서 최적화 ─────────────────────────────────────────────────────

    fn optimize_join_order(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Join(join) => {
                let left_rows  = self.estimate_rows(&join.left);
                let right_rows = self.estimate_rows(&join.right);

                // Build side를 더 작은 쪽으로 (hash join: 작은 쪽 = build)
                if right_rows < left_rows {
                    Ok(LogicalPlan::Join(JoinNode {
                        left:      join.right,
                        right:     join.left,
                        join_type: join.join_type,
                        condition: join.condition,
                    }))
                } else {
                    Ok(LogicalPlan::Join(join))
                }
            }
            other => Ok(other),
        }
    }

    fn estimate_rows(&self, plan: &LogicalPlan) -> u64 {
        match plan {
            LogicalPlan::Scan(scan) => {
                let agg = self.stats.get_aggregate(&scan.cube_name);
                agg.row_count
            }
            LogicalPlan::Filter(f) => {
                // 기본 selectivity 0.1 적용
                self.estimate_rows(&f.input) / 10
            }
            LogicalPlan::Limit { n, .. } => *n as u64,
            _ => 1000, // 추정 불가 시 기본값
        }
    }

    // ─── 집계 전략 선택 ───────────────────────────────────────────────────────

    /// NDV 기반 집계 전략 선택
    /// 반환: true = Hash Aggregate, false = Sort Aggregate
    pub fn choose_agg_strategy(&self, cube: &str, group_col: &str) -> bool {
        let agg = self.stats.get_aggregate(cube);
        if let Some(cs) = agg.get_column(group_col) {
            // NDV가 row_count의 10% 미만이면 Hash Agg 선호
            cs.ndv < agg.row_count / 10
        } else {
            true // 기본: Hash Aggregate
        }
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::stats::TableStats;
    use crate::planner::logical::*;
    use shared::types::CubeSchema;

    fn empty_schema(name: &str) -> CubeSchema {
        use shared::types::{PartitionKey, PartitionVariant, PartitionGranularity, Distribution};
        CubeSchema::new(
            name,
            "default",
            Vec::new(),
            PartitionKey {
                variant: PartitionVariant::Range {
                    column:      "event_time".to_string(),
                    granularity: Some(PartitionGranularity::Month),
                },
                auto_partition: true,
            },
            Vec::new(),
            Distribution { column: "device_id".to_string(), bucket_count: 64 },
        )
    }

    #[test]
    fn test_predicate_pushdown() {
        let stats = StatisticsManager::new();
        let opt   = CboOptimizer::new(stats);

        let scan = LogicalPlan::Scan(ScanNode {
            cube_name:      "events".into(),
            cube_schema:    empty_schema("events"),
            projected_cols: None,
            predicates:     Vec::new(),
            partition_filter: None,
        });

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: Predicate { expr: Expr::Wildcard },
        });

        let result = opt.push_down_predicates(filter).unwrap();
        // Filter가 Scan에 흡수되어야
        assert!(matches!(result, LogicalPlan::Scan(_)));
    }
}
