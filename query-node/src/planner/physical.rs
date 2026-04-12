// T058: LogicalPlan → PhysicalPlan Fragment 생성, CN 할당, Exchange 노드 삽입

use std::collections::HashMap;

use anyhow::Result;
use uuid::Uuid;

use crate::planner::logical::{LogicalPlan, JoinType};

// ─── Physical Plan ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    pub root:      PhysicalNode,
    pub fragments: Vec<Fragment>,
}

#[derive(Debug, Clone)]
pub enum PhysicalNode {
    /// 스토리지 노드에서 직접 스캔
    TableScan {
        cube_name:  String,
        tablet_ids: Vec<String>,
        columns:    Vec<String>,
        predicate:  Option<String>, // 직렬화된 predicate
    },
    /// CN에서 실행되는 집계
    HashAggregate {
        input:    Box<PhysicalNode>,
        group_by: Vec<usize>,
        agg_ops:  Vec<String>,
    },
    /// CN 간 데이터 교환
    Exchange {
        input:   Box<PhysicalNode>,
        mode:    ExchangeMode,
    },
    /// CN에서 실행되는 해시 조인
    HashJoin {
        build:      Box<PhysicalNode>,
        probe:      Box<PhysicalNode>,
        join_type:  String,
        key_cols:   Vec<usize>,
    },
    /// 결과 제한
    Limit {
        input: Box<PhysicalNode>,
        n:     usize,
    },
    /// 정렬
    Sort {
        input:     Box<PhysicalNode>,
        sort_keys: Vec<(usize, bool)>, // (col_idx, descending)
    },
    /// Funnel 분석
    FunnelAnalysis {
        input:      Box<PhysicalNode>,
        user_key:   String,
        steps:      Vec<String>,
        window_sec: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeMode {
    HashShuffle { key_col: usize, n_partitions: usize },
    Broadcast,
    Gather,
    Passthrough,
}

// ─── Fragment (CN에 할당되는 실행 단위) ──────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Fragment {
    pub fragment_id: String,
    pub plan:        PhysicalNode,
    /// 이 Fragment를 실행할 CN 노드 ID (None = 임의 CN)
    pub assigned_cn: Option<String>,
    /// 이 Fragment가 의존하는 Fragment ID 목록
    pub depends_on:  Vec<String>,
}

// ─── Physical Planner ────────────────────────────────────────────────────────

pub struct PhysicalPlanner {
    /// 가용 CN 노드 목록
    compute_nodes: Vec<String>,
    /// CN별 부하 (단순 라운드로빈용)
    cn_idx:        usize,
}

impl PhysicalPlanner {
    pub fn new(compute_nodes: Vec<String>) -> Self {
        Self { compute_nodes, cn_idx: 0 }
    }

    pub fn plan(&mut self, logical: LogicalPlan) -> Result<PhysicalPlan> {
        let mut fragments = Vec::new();
        let root = self.translate_node(&logical, &mut fragments)?;

        Ok(PhysicalPlan { root, fragments })
    }

    fn translate_node(
        &mut self,
        plan:      &LogicalPlan,
        fragments: &mut Vec<Fragment>,
    ) -> Result<PhysicalNode> {
        match plan {
            LogicalPlan::Scan(scan) => {
                Ok(PhysicalNode::TableScan {
                    cube_name:  scan.cube_name.clone(),
                    tablet_ids: Vec::new(), // TabletManager에서 채움
                    columns:    scan.cube_schema.columns.iter().map(|c| c.name.clone()).collect(),
                    predicate:  None,
                })
            }

            LogicalPlan::Filter(f) => {
                // Filter는 Scan에 흡수 (CBO 최적화 후 여기 도달 시 처리)
                self.translate_node(&f.input, fragments)
            }

            LogicalPlan::Aggregate(agg) => {
                let input = self.translate_node(&agg.input, fragments)?;
                // CN 간 집계: Partial Agg → Shuffle → Final Agg
                let partial = PhysicalNode::HashAggregate {
                    input:    Box::new(input),
                    group_by: (0..agg.group_by.len()).collect(),
                    agg_ops:  agg.agg_exprs.iter().map(|a| format!("{:?}", a.func)).collect(),
                };

                if self.compute_nodes.len() > 1 {
                    // Exchange → Final Agg
                    let exchange = PhysicalNode::Exchange {
                        input: Box::new(partial),
                        mode: ExchangeMode::HashShuffle {
                            key_col:      0,
                            n_partitions: self.compute_nodes.len(),
                        },
                    };
                    Ok(PhysicalNode::HashAggregate {
                        input:    Box::new(exchange),
                        group_by: (0..agg.group_by.len()).collect(),
                        agg_ops:  agg.agg_exprs.iter().map(|a| format!("{:?}_final", a.func)).collect(),
                    })
                } else {
                    Ok(partial)
                }
            }

            LogicalPlan::Join(join) => {
                let build = self.translate_node(&join.left, fragments)?;
                let probe = self.translate_node(&join.right, fragments)?;
                Ok(PhysicalNode::HashJoin {
                    build:     Box::new(build),
                    probe:     Box::new(probe),
                    join_type: format!("{:?}", join.join_type),
                    key_cols:  vec![0],
                })
            }

            LogicalPlan::Limit { input, n } => {
                let input = self.translate_node(input, fragments)?;
                Ok(PhysicalNode::Limit { input: Box::new(input), n: *n })
            }

            LogicalPlan::Sort(sort) => {
                let input = self.translate_node(&sort.input, fragments)?;
                Ok(PhysicalNode::Sort {
                    input:     Box::new(input),
                    sort_keys: sort.sort_keys.iter().enumerate()
                        .map(|(i, sk)| (i, sk.descending))
                        .collect(),
                })
            }

            LogicalPlan::FunnelAnalysis(funnel) => {
                let input = self.translate_node(&funnel.input, fragments)?;
                Ok(PhysicalNode::FunnelAnalysis {
                    input:      Box::new(input),
                    user_key:   funnel.user_key.clone(),
                    steps:      funnel.steps.clone(),
                    window_sec: funnel.window_sec,
                })
            }

            _ => {
                // 미지원: Passthrough
                Ok(PhysicalNode::Limit { input: Box::new(PhysicalNode::TableScan {
                    cube_name:  "unknown".into(),
                    tablet_ids: Vec::new(),
                    columns:    Vec::new(),
                    predicate:  None,
                }), n: 0 })
            }
        }
    }

    /// 다음 CN 라운드로빈 선택
    fn next_cn(&mut self) -> Option<String> {
        if self.compute_nodes.is_empty() {
            return None;
        }
        let cn = self.compute_nodes[self.cn_idx % self.compute_nodes.len()].clone();
        self.cn_idx += 1;
        Some(cn)
    }

    /// PhysicalPlan을 Fragment로 분할
    pub fn build_fragments(&mut self, plan: PhysicalPlan) -> Vec<Fragment> {
        let fragment_id = Uuid::new_v4().to_string();
        let cn = self.next_cn();
        vec![Fragment {
            fragment_id,
            plan: plan.root,
            assigned_cn: cn,
            depends_on: Vec::new(),
        }]
    }
}
