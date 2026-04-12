// T019: 공통 타입 정의
// WOW-DB 전체에서 사용되는 핵심 도메인 타입

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

// ──── 기본 타입 ────────────────────────────────────────────

pub type Timestamp = DateTime<Utc>;
pub type PartitionId = Uuid;
pub type TabletId = Uuid;
pub type CubeId = Uuid;
pub type SmvId = Uuid;
pub type MvId = Uuid;

// ──── 데이터 타입 ──────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    DateTime,
    Date,
    Json,
    Binary,
    Nullable(Box<DataType>),
}

// ──── 값 표현 ──────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    DateTime(Timestamp),
    Bytes(Vec<u8>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use Value::*;
        match (self, other) {
            (Int64(a), Int64(b)) => a.partial_cmp(b),
            (Float64(a), Float64(b)) => a.partial_cmp(b),
            (String(a), String(b)) => a.partial_cmp(b),
            (DateTime(a), DateTime(b)) => a.partial_cmp(b),
            (Null, Null) => Some(std::cmp::Ordering::Equal),
            (Null, _) => Some(std::cmp::Ordering::Less),
            (_, Null) => Some(std::cmp::Ordering::Greater),
            _ => None,
        }
    }
}

// ──── Sort Key ─────────────────────────────────────────────

/// LSM-Tree Sort Key: 파티션 내 레코드 정렬 및 lookup 키
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SortKey {
    pub values: Vec<SortKeyComponent>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SortKeyComponent {
    pub column: String,
    pub encoded: Vec<u8>,  // 정렬 가능한 바이너리 인코딩
}

impl SortKey {
    pub fn new(values: Vec<SortKeyComponent>) -> Self {
        Self { values }
    }
}

// ──── 컬럼 참조 ────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

impl ColumnRef {
    pub fn new(column: impl Into<String>) -> Self {
        Self { table: None, column: column.into() }
    }

    pub fn with_table(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self { table: Some(table.into()), column: column.into() }
    }
}

// ──── 컬럼 인코딩/압축 ─────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum Encoding {
    #[default]
    Plain,
    Dictionary,
    Delta,
    BitPacking,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum Compression {
    #[default]
    None,
    Lz4,
    Zstd,
}

// ──── Data Skipping Index 타입 ─────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SkippingIndexType {
    MinMax,
    BloomFilter,
    Set,
    NgramBf { ngram_size: u8, false_positive_rate: f64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkippingIndex {
    pub name:        String,
    pub index_type:  SkippingIndexType,
    pub granularity: u32,  // Granule 단위 수 (기본 1)
}

// ──── Flat JSON 설정 ────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlatJsonConfig {
    /// 자동 컬럼화 트리거 임계값 (출현율, 기본 0.5 = 50%)
    pub promotion_threshold: f64,
    /// 자동 컬럼화 비활성화
    pub disabled: bool,
}

impl Default for FlatJsonConfig {
    fn default() -> Self {
        Self { promotion_threshold: 0.5, disabled: false }
    }
}

// ──── 컬럼 정의 ────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name:          String,
    pub data_type:     DataType,
    pub nullable:      bool,
    pub encoding:      Encoding,
    pub compression:   Compression,
    pub skipping_index: Option<SkippingIndex>,
    pub flat_json:     Option<FlatJsonConfig>,
    pub default_value: Option<Value>,
}

// ──── 파티션 전략 ──────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionGranularity {
    Day,
    Month,
    Year,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionVariant {
    Range {
        column:      String,
        granularity: Option<PartitionGranularity>,
    },
    List {
        column: String,
        values: Vec<Value>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionKey {
    pub variant:       PartitionVariant,
    pub auto_partition: bool,
}

// ──── 분산 전략 ────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Distribution {
    pub column:       String,
    pub bucket_count: u32,
}

// ──── 스토리지 백엔드 ──────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum StorageBackend {
    #[default]
    Native,
    S3 {
        bucket: String,
        prefix: String,
        region: String,
    },
    Hdfs {
        namenode:   String,
        base_path:  String,
        kerberos:   Option<KerberosConfig>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KerberosConfig {
    pub keytab:    String,
    pub principal: String,
    pub renew_interval_s: u64,
}

// ──── TTL / Tiering 정책 ────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlPolicy {
    pub ttl_days: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringPolicy {
    pub hot_ttl_days: u32,
    pub cold_backend: StorageBackend,
}

// ──── Cube 스키마 ───────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CubeSchema {
    pub cube_id:         CubeId,
    pub name:            String,
    pub database:        String,
    pub columns:         Vec<ColumnDef>,
    pub partition_key:   PartitionKey,
    pub sort_key:        Vec<ColumnRef>,
    pub distribution:    Distribution,
    pub colocate_group:  Option<String>,
    pub storage_backend: StorageBackend,
    pub ttl_policy:      Option<TtlPolicy>,
    pub tiering_policy:  Option<TieringPolicy>,
    pub created_at:      Timestamp,
    pub version:         u64,
}

impl CubeSchema {
    pub fn new(
        name: impl Into<String>,
        database: impl Into<String>,
        columns: Vec<ColumnDef>,
        partition_key: PartitionKey,
        sort_key: Vec<ColumnRef>,
        distribution: Distribution,
    ) -> Self {
        Self {
            cube_id: Uuid::new_v4(),
            name: name.into(),
            database: database.into(),
            columns,
            partition_key,
            sort_key,
            distribution,
            colocate_group: None,
            storage_backend: StorageBackend::Native,
            ttl_policy: None,
            tiering_policy: None,
            created_at: Utc::now(),
            version: 1,
        }
    }
}

// ──── 파티션 인스턴스 ──────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    pub partition_id: PartitionId,
    pub cube_id:      CubeId,
    pub range_start:  Value,
    pub range_end:    Value,
    pub tablets:      Vec<TabletRef>,
    pub row_count:    u64,
    pub size_bytes:   u64,
    pub created_at:   Timestamp,
    pub ttl_expires:  Option<Timestamp>,
    pub tier:         StorageTier,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum StorageTier {
    #[default]
    Hot,
    Cold,
}

// ──── Tablet 참조 ──────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabletRef {
    pub tablet_id: TabletId,
    pub bucket_id: u32,
}

// ──── CBO 컬럼 통계 ─────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnStats {
    pub cube_id:    CubeId,
    pub column:     String,
    pub row_count:  u64,
    pub null_count: u64,
    pub min_val:    Option<Value>,
    pub max_val:    Option<Value>,
    pub ndv:        Option<u64>,     // HyperLogLog 추정
    pub histogram:  Option<Histogram>,
    pub updated_at: Option<Timestamp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Histogram {
    pub buckets: Vec<HistogramBucket>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub lower: Value,
    pub upper: Value,
    pub count: u64,
}

// ──── 쿼리 프로파일 ────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryProfile {
    pub query_id:      Uuid,
    pub sql_text:      String,
    pub submitted_at:  Timestamp,
    pub finished_at:   Option<Timestamp>,
    pub total_ms:      u64,
    pub rows_scanned:  u64,
    pub rows_returned: u64,
    pub status:        QueryStatus,
    pub error_msg:     Option<String>,
    pub stages:        Vec<StageMetric>,
    pub node_metrics:  Vec<NodeMetric>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryStatus {
    Running,
    Success,
    Error,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageMetric {
    pub stage_name:  String,
    pub duration_ms: u64,
    pub rows_in:     u64,
    pub rows_out:    u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetric {
    pub node_id:     String,
    pub node_type:   String,
    pub rows_processed: u64,
    pub duration_ms: u64,
}

// ──── Session Materialized View ────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMaterializedView {
    pub smv_id:           SmvId,
    pub name:             String,
    pub source_cube_id:   CubeId,
    pub user_key_column:  ColumnRef,
    pub session_timeout_s: u64,
    pub refresh_mode:     RefreshMode,
    pub state:            SmvState,
    pub last_refreshed:   Option<Timestamp>,
    pub version:          u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SmvState {
    Building,
    Ready,
    Refreshing,
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RefreshMode {
    OnInsert,
    Scheduled { interval_s: u64 },
    Manual,
}

// ──── Resource Group ───────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceGroup {
    pub name:              String,
    pub max_cpu_cores:     Option<f64>,
    pub max_memory_mb:     Option<u64>,
    pub max_concurrency:   Option<u32>,
    pub query_timeout_ms:  Option<u64>,
    pub assigned_to:       Vec<String>,  // user 또는 role 이름
}

// ──── 기타 유틸 ────────────────────────────────────────────

/// 노드 정보
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id:   String,
    pub node_type: NodeType,
    pub address:   String,
    pub port_grpc: u16,
    pub state:     NodeState,
    pub version:   String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeType {
    QueryNode,
    ComputeNode,
    StorageNode,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeState {
    Online,
    Degraded,
    Offline,
}

/// Granule 크기 (기본 8,192행)
pub const DEFAULT_GRANULE_SIZE: u32 = 8192;

/// Query Profiler Circular Buffer 크기
pub const PROFILER_BUFFER_SIZE: usize = 1000;

/// MemTable flush 임계값 (기본 64MB)
pub const DEFAULT_MEMTABLE_SIZE_BYTES: usize = 64 * 1024 * 1024;
