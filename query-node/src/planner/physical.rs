// T058 + T130: LogicalPlan → PhysicalPlan Fragment 생성, CN 할당, Exchange 노드 삽입
// T130 추가: ShardScan 기반 Fragment 생성, Shard → CN 할당 전략 (FR-041)

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;

use shared::types::{LsmScanRange, ShardPredicate, ShardScanRequest};

use crate::meta::shard_map::ShardMap;
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

// ─── ShardScan (FR-041): CN이 SN에 요청하는 Shard 스캔 명세 ─────────────────

/// Fragment에 포함되는 단일 Shard 스캔 명세
/// CN은 이 정보를 바탕으로 SN gRPC ShardScanRequest를 구성한다
#[derive(Debug, Clone)]
pub struct ShardScanSpec {
    pub shard_id:    Uuid,
    pub sn_endpoint: SocketAddr,
    pub shard_dir:   PathBuf,
    /// Column Projection (빈 배열 = 전체 컬럼)
    pub columns:     Vec<String>,
    /// Pushdown 필터
    pub predicates:  Vec<ShardPredicate>,
    /// 스캔할 LSM 레벨 범위
    pub scan_range:  LsmScanRange,
    /// Point Lookup용 Bloom 프로브 키
    pub bloom_probe_keys: Vec<Vec<u8>>,
}

impl ShardScanSpec {
    /// ShardScanSpec → shared::ShardScanRequest 변환
    pub fn to_scan_request(&self) -> ShardScanRequest {
        ShardScanRequest {
            shard_id:         self.shard_id,
            shard_dir:        self.shard_dir.clone(),
            columns:          self.columns.clone(),
            predicates:       self.predicates.clone(),
            scan_range:       self.scan_range.clone(),
            bloom_probe_keys: self.bloom_probe_keys.clone(),
        }
    }
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
    /// T130: 이 Fragment가 스캔할 Shard 목록 (FR-041)
    pub shard_scans: Vec<ShardScanSpec>,
}

// ─── Physical Planner ────────────────────────────────────────────────────────

pub struct PhysicalPlanner {
    /// 가용 CN 노드 목록
    compute_nodes: Vec<String>,
    /// CN별 부하 (단순 라운드로빈용)
    cn_idx:        usize,
    /// CN 노드 ID → SN endpoint 공동 배치 여부 맵 (Data Locality용)
    /// key: CN node_id, value: 같은 호스트에 배치된 SN endpoint 목록
    cn_colocated_sns: HashMap<String, Vec<SocketAddr>>,
    /// CN별 현재 활성 fragment 수 (Load Balancing용)
    cn_load: HashMap<String, usize>,
}

impl PhysicalPlanner {
    pub fn new(compute_nodes: Vec<String>) -> Self {
        let cn_load = compute_nodes.iter().map(|c| (c.clone(), 0usize)).collect();
        Self {
            compute_nodes,
            cn_idx: 0,
            cn_colocated_sns: HashMap::new(),
            cn_load,
        }
    }

    /// Co-located 모드: CN node_id 와 같은 호스트의 SN endpoint 등록
    pub fn register_colocated_sn(&mut self, cn_node_id: String, sn_endpoint: SocketAddr) {
        self.cn_colocated_sns
            .entry(cn_node_id)
            .or_default()
            .push(sn_endpoint);
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

    /// Load Balancing: 가장 부하가 낮은 CN 선택
    fn least_loaded_cn(&self) -> Option<String> {
        self.compute_nodes.iter()
            .min_by_key(|cn| self.cn_load.get(*cn).copied().unwrap_or(0))
            .cloned()
    }

    /// Data Locality: sn_endpoint와 co-located된 CN 선택
    fn cn_colocated_with(&self, sn_endpoint: SocketAddr) -> Option<String> {
        for (cn_id, sns) in &self.cn_colocated_sns {
            if sns.contains(&sn_endpoint) {
                return Some(cn_id.clone());
            }
        }
        None
    }

    /// CN 부하 증가 기록
    fn increment_cn_load(&mut self, cn_id: &str) {
        *self.cn_load.entry(cn_id.to_string()).or_insert(0) += 1;
    }

    /// PhysicalPlan을 Fragment로 분할 (단순 버전 — shard_scans 없음)
    pub fn build_fragments(&mut self, plan: PhysicalPlan) -> Vec<Fragment> {
        let fragment_id = Uuid::new_v4().to_string();
        let cn = self.next_cn();
        if let Some(ref cn_id) = cn {
            self.increment_cn_load(cn_id);
        }
        vec![Fragment {
            fragment_id,
            plan: plan.root,
            assigned_cn: cn,
            depends_on: Vec::new(),
            shard_scans: Vec::new(),
        }]
    }

    /// ShardScan 기반 Fragment 생성 (T130, FR-041)
    ///
    /// 각 Shard에 대해:
    ///   1. Data Locality: co-located CN 우선
    ///   2. Load Balancing: 부하 낮은 CN
    ///   3. 같은 CN으로 여러 Shard를 합산 가능
    ///
    /// shard_ids: 스캔할 Shard ID 목록
    /// columns: 필요한 컬럼 (빈 배열 = 전체)
    /// predicates: Pushdown 필터
    pub async fn build_shard_fragments(
        &mut self,
        plan:       PhysicalPlan,
        shard_ids:  Vec<Uuid>,
        columns:    Vec<String>,
        predicates: Vec<ShardPredicate>,
        shard_map:  &ShardMap,
    ) -> Result<Vec<Fragment>> {
        if shard_ids.is_empty() {
            return Ok(self.build_fragments(plan));
        }

        // CN별로 Shard 묶기: CN node_id → Vec<ShardScanSpec>
        let mut cn_shards: HashMap<String, Vec<ShardScanSpec>> = HashMap::new();

        for shard_id in &shard_ids {
            let (sn_endpoint, shard_dir) = match shard_map.lookup(*shard_id).await {
                Ok(v)  => v,
                Err(e) => {
                    tracing::warn!(shard_id = %shard_id, err = %e, "Shard 매핑 조회 실패 — 스킵");
                    continue;
                }
            };

            // CN 선택: Data Locality → Load Balancing → RoundRobin
            let assigned_cn = self
                .cn_colocated_with(sn_endpoint)
                .or_else(|| self.least_loaded_cn())
                .or_else(|| self.next_cn());

            let cn_id = match assigned_cn {
                Some(id) => id,
                None => continue, // CN 없음
            };

            let spec = ShardScanSpec {
                shard_id:         *shard_id,
                sn_endpoint,
                shard_dir,
                columns:          columns.clone(),
                predicates:       predicates.clone(),
                scan_range:       LsmScanRange::all_levels(),
                bloom_probe_keys: Vec::new(),
            };

            cn_shards.entry(cn_id).or_default().push(spec);
        }

        // 각 CN에 하나의 Fragment 생성
        let mut fragments = Vec::new();
        for (cn_id, shards) in cn_shards {
            self.increment_cn_load(&cn_id);
            let fragment_id = Uuid::new_v4().to_string();
            fragments.push(Fragment {
                fragment_id,
                plan:        plan.root.clone(),
                assigned_cn: Some(cn_id),
                depends_on:  Vec::new(),
                shard_scans: shards,
            });
        }

        // shard_ids가 있었지만 모두 실패한 경우 fallback
        if fragments.is_empty() {
            return Ok(self.build_fragments(plan));
        }

        Ok(fragments)
    }
}
