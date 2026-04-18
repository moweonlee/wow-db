// T055: AST → LogicalPlan 변환

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use shared::types::{CubeSchema, ColumnRef, DataType as WowDataType};

use crate::sql_parser::{WowDbStatement, WowDbCustom};

// ─── Logical Plan 노드 ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// 테이블 전체 스캔
    Scan(ScanNode),
    /// 행 필터
    Filter(FilterNode),
    /// 컬럼 투영
    Project(ProjectNode),
    /// 집계
    Aggregate(AggregateNode),
    /// Join
    Join(JoinNode),
    /// Limit
    Limit { input: Box<LogicalPlan>, n: usize },
    /// Sort
    Sort(SortNode),
    /// Funnel 분석
    FunnelAnalysis(FunnelNode),
    /// Cohort 분석
    CohortAnalysis(CohortNode),
    /// Path 분석
    PathAnalysis(PathNode),
    /// 단일 리터럴 배치
    Values(Vec<Vec<ScalarValue>>),
}

impl LogicalPlan {
    pub fn schema(&self) -> Vec<ColumnRef> {
        match self {
            LogicalPlan::Scan(n) => n.projected_cols.clone()
                .unwrap_or_else(|| {
                    n.cube_schema.columns.iter()
                        .map(|c| ColumnRef::new(&c.name))
                        .collect()
                }),
            LogicalPlan::Project(n) => n.exprs.iter().filter_map(|e| e.as_col()).collect(),
            _ => Vec::new(),
        }
    }
}

// ─── 노드 타입 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ScanNode {
    pub cube_name:      String,
    pub cube_schema:    CubeSchema,
    pub projected_cols: Option<Vec<ColumnRef>>,
    pub predicates:     Vec<Predicate>,
    pub partition_filter: Option<PartitionFilter>,
}

#[derive(Debug, Clone)]
pub struct FilterNode {
    pub input:     Box<LogicalPlan>,
    pub predicate: Predicate,
}

#[derive(Debug, Clone)]
pub struct ProjectNode {
    pub input: Box<LogicalPlan>,
    pub exprs: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct AggregateNode {
    pub input:      Box<LogicalPlan>,
    pub group_by:   Vec<Expr>,
    pub agg_exprs:  Vec<AggExpr>,
}

#[derive(Debug, Clone)]
pub struct JoinNode {
    pub left:       Box<LogicalPlan>,
    pub right:      Box<LogicalPlan>,
    pub join_type:  JoinType,
    pub condition:  Box<Predicate>,
}

#[derive(Debug, Clone)]
pub struct SortNode {
    pub input:     Box<LogicalPlan>,
    pub sort_keys: Vec<SortKeyExpr>,
}

#[derive(Debug, Clone)]
pub struct FunnelNode {
    pub input:      Box<LogicalPlan>,
    pub user_key:   String,
    pub steps:      Vec<String>,
    pub window_sec: i64,
}

#[derive(Debug, Clone)]
pub struct CohortNode {
    pub input:        Box<LogicalPlan>,
    pub user_key:     String,
    pub entry_event:  String,
    pub return_event: String,
    pub period_unit:  String,
    pub max_periods:  usize,
}

#[derive(Debug, Clone)]
pub struct PathNode {
    pub input:       Box<LogicalPlan>,
    pub user_key:    String,
    pub max_depth:   usize,
    pub session_gap: i64,
    pub top_n:       usize,
}

// ─── 표현식 타입 ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Expr {
    Column(ColumnRef),
    Literal(ScalarValue),
    BinaryOp { left: Box<Expr>, op: BinaryOp, right: Box<Expr> },
    Alias(Box<Expr>, String),
    AggFunc(AggExpr),
    Wildcard,
}

impl Expr {
    pub fn as_col(&self) -> Option<ColumnRef> {
        match self {
            Expr::Column(c) => Some(c.clone()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryOp { Add, Sub, Mul, Div, Eq, Ne, Lt, Le, Gt, Ge, And, Or }

#[derive(Debug, Clone)]
pub struct AggExpr {
    pub func:   AggFunc,
    pub input:  Box<Expr>,
    pub alias:  String,
    pub distinct: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc { Count, Sum, Avg, Min, Max, CountDistinct }

#[derive(Debug, Clone)]
pub enum ScalarValue {
    Int64(Option<i64>),
    Float64(Option<f64>),
    Utf8(Option<String>),
    Boolean(Option<bool>),
    Null,
}

#[derive(Debug, Clone)]
pub struct Predicate {
    pub expr: Expr,
}

#[derive(Debug, Clone)]
pub struct PartitionFilter {
    pub column:    String,
    pub lo:        Option<ScalarValue>,
    pub hi:        Option<ScalarValue>,
}

#[derive(Debug, Clone)]
pub struct SortKeyExpr {
    pub expr:        Expr,
    pub descending:  bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType { Inner, Left, Right, Full, Cross }

// ─── Logical Planner ─────────────────────────────────────────────────────────

pub struct LogicalPlanner {
    cube_schemas: HashMap<String, CubeSchema>,
}

impl LogicalPlanner {
    pub fn new(schemas: HashMap<String, CubeSchema>) -> Self {
        Self { cube_schemas: schemas }
    }

    pub fn plan(&self, stmt: &WowDbStatement) -> Result<LogicalPlan> {
        match stmt {
            WowDbStatement::Standard(sql_stmt) => self.plan_standard(sql_stmt),
            WowDbStatement::Custom(_) => Err(anyhow!("DDL은 실행 계획 불필요")),
        }
    }

    fn plan_standard(&self, stmt: &sqlparser::ast::Statement) -> Result<LogicalPlan> {
        use sqlparser::ast::Statement;
        match stmt {
            Statement::Query(q) => self.plan_query(q),
            _ => Err(anyhow!("지원하지 않는 쿼리 타입: {:?}", stmt)),
        }
    }

    fn plan_query(&self, query: &sqlparser::ast::Query) -> Result<LogicalPlan> {
        use sqlparser::ast::{SetExpr, Select};

        match query.body.as_ref() {
            SetExpr::Select(select) => self.plan_select(select, query),
            _ => Err(anyhow!("UNION/INTERSECT/EXCEPT는 Phase D에서 지원")),
        }
    }

    fn plan_select(
        &self,
        select: &sqlparser::ast::Select,
        query:  &sqlparser::ast::Query,
    ) -> Result<LogicalPlan> {
        // FROM 절 처리
        let from = select.from.first()
            .ok_or_else(|| anyhow!("FROM 절 없음"))?;
        let table_name = self.extract_table_name(&from.relation)?;

        // Scan 노드 생성
        let cube_schema = self.cube_schemas.get(&table_name)
            .cloned()
            .unwrap_or_else(|| {
                use shared::types::{PartitionKey, PartitionVariant, PartitionGranularity, Distribution};
                CubeSchema::new(
                    table_name.clone(),
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
            });

        let mut plan: LogicalPlan = LogicalPlan::Scan(ScanNode {
            cube_name:      table_name,
            cube_schema,
            projected_cols: None,
            predicates:     Vec::new(),
            partition_filter: None,
        });

        // WHERE 절 처리
        if let Some(selection) = &select.selection {
            plan = LogicalPlan::Filter(FilterNode {
                input:     Box::new(plan),
                predicate: Predicate { expr: self.translate_expr(selection)? },
            });
        }

        // GROUP BY + 집계
        let has_group_by = match &select.group_by {
            sqlparser::ast::GroupByExpr::All(_) => false,
            sqlparser::ast::GroupByExpr::Expressions(es, _) => !es.is_empty(),
        };
        if has_group_by || self.has_aggregate(&select.projection) {
            let group_by = self.translate_group_by(&select.group_by)?;
            let agg_exprs = self.extract_agg_exprs(&select.projection)?;
            plan = LogicalPlan::Aggregate(AggregateNode {
                input:     Box::new(plan),
                group_by,
                agg_exprs,
            });
        }

        // LIMIT 처리
        if let Some(limit) = &query.limit {
            if let sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) = limit {
                if let Ok(n) = n.parse::<usize>() {
                    plan = LogicalPlan::Limit { input: Box::new(plan), n };
                }
            }
        }

        Ok(plan)
    }

    fn extract_table_name(&self, relation: &sqlparser::ast::TableFactor) -> Result<String> {
        use sqlparser::ast::TableFactor;
        match relation {
            TableFactor::Table { name, .. } => Ok(name.to_string()),
            _ => Err(anyhow!("서브쿼리/함수 FROM: Phase D에서 지원")),
        }
    }

    fn translate_expr(&self, expr: &sqlparser::ast::Expr) -> Result<Expr> {
        use sqlparser::ast::Expr as SqlExpr;
        match expr {
            SqlExpr::Identifier(ident) => Ok(Expr::Column(ColumnRef::new(&ident.value))),
            SqlExpr::Value(v) => Ok(Expr::Literal(self.translate_value(v)?)),
            SqlExpr::BinaryOp { left, op, right } => {
                let bop = self.translate_binop(op)?;
                Ok(Expr::BinaryOp {
                    left:  Box::new(self.translate_expr(left)?),
                    op:    bop,
                    right: Box::new(self.translate_expr(right)?),
                })
            }
            SqlExpr::Wildcard => Ok(Expr::Wildcard),
            _ => Err(anyhow!("미지원 표현식: {:?}", expr)),
        }
    }

    fn translate_value(&self, val: &sqlparser::ast::Value) -> Result<ScalarValue> {
        use sqlparser::ast::Value;
        match val {
            Value::Number(n, _)       => Ok(ScalarValue::Int64(n.parse().ok())),
            Value::SingleQuotedString(s) => Ok(ScalarValue::Utf8(Some(s.clone()))),
            Value::Boolean(b)         => Ok(ScalarValue::Boolean(Some(*b))),
            Value::Null               => Ok(ScalarValue::Null),
            _ => Ok(ScalarValue::Null),
        }
    }

    fn translate_binop(&self, op: &sqlparser::ast::BinaryOperator) -> Result<BinaryOp> {
        use sqlparser::ast::BinaryOperator as SqlOp;
        Ok(match op {
            SqlOp::Plus  => BinaryOp::Add,
            SqlOp::Minus => BinaryOp::Sub,
            SqlOp::Multiply => BinaryOp::Mul,
            SqlOp::Divide   => BinaryOp::Div,
            SqlOp::Eq  => BinaryOp::Eq,
            SqlOp::NotEq => BinaryOp::Ne,
            SqlOp::Lt  => BinaryOp::Lt,
            SqlOp::LtEq => BinaryOp::Le,
            SqlOp::Gt  => BinaryOp::Gt,
            SqlOp::GtEq => BinaryOp::Ge,
            SqlOp::And => BinaryOp::And,
            SqlOp::Or  => BinaryOp::Or,
            _ => return Err(anyhow!("미지원 연산자: {:?}", op)),
        })
    }

    fn translate_group_by(&self, exprs: &sqlparser::ast::GroupByExpr) -> Result<Vec<Expr>> {
        use sqlparser::ast::GroupByExpr;
        match exprs {
            GroupByExpr::All(_) => Ok(Vec::new()),
            GroupByExpr::Expressions(es, _) => {
                es.iter().map(|e| self.translate_expr(e)).collect()
            }
        }
    }

    fn extract_agg_exprs(&self, proj: &[sqlparser::ast::SelectItem]) -> Result<Vec<AggExpr>> {
        let mut result = Vec::new();
        for item in proj {
            use sqlparser::ast::SelectItem;
            if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
                if let sqlparser::ast::Expr::Function(f) = expr {
                    let func_name = f.name.to_string().to_uppercase();
                    let func = match func_name.as_str() {
                        "COUNT"  => AggFunc::Count,
                        "SUM"    => AggFunc::Sum,
                        "AVG"    => AggFunc::Avg,
                        "MIN"    => AggFunc::Min,
                        "MAX"    => AggFunc::Max,
                        _        => continue,
                    };
                    result.push(AggExpr {
                        func,
                        input:    Box::new(Expr::Wildcard),
                        alias:    func_name.to_lowercase(),
                        distinct: false,
                    });
                }
            }
        }
        Ok(result)
    }

    fn has_aggregate(&self, proj: &[sqlparser::ast::SelectItem]) -> bool {
        for item in proj {
            use sqlparser::ast::SelectItem;
            if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
                if let sqlparser::ast::Expr::Function(f) = expr {
                    let name = f.name.to_string().to_uppercase();
                    if matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                        return true;
                    }
                }
            }
        }
        false
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_parser::WowDbParser;

    fn plan_sql(sql: &str) -> Result<LogicalPlan> {
        let stmts = WowDbParser::parse(sql)?;
        let stmt = stmts.into_iter().next()
            .ok_or_else(|| anyhow!("빈 SQL"))?;
        let planner = LogicalPlanner::new(HashMap::new());
        planner.plan(&stmt)
    }

    #[test]
    fn test_simple_scan() {
        let plan = plan_sql("SELECT * FROM page_events").unwrap();
        assert!(matches!(plan, LogicalPlan::Scan(_)));
    }

    #[test]
    fn test_filter_plan() {
        let plan = plan_sql("SELECT * FROM events WHERE user_id = 123").unwrap();
        assert!(matches!(plan, LogicalPlan::Filter(_)));
    }

    #[test]
    fn test_limit_plan() {
        let plan = plan_sql("SELECT * FROM events LIMIT 10").unwrap();
        assert!(matches!(plan, LogicalPlan::Limit { n: 10, .. }));
    }
}
