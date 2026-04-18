// T020: 공통 에러 타입 (thiserror 기반)

use thiserror::Error;

#[derive(Debug, Error)]
pub enum WowDbError {
    // ── 파싱 오류 ──────────────────────────────────────────
    #[error("SQL parse error: {0}")]
    ParseError(String),

    #[error("Schema validation error: {0}")]
    SchemaError(String),

    // ── 스토리지 오류 ──────────────────────────────────────
    #[error("Storage I/O error: {0}")]
    StorageIo(#[from] std::io::Error),

    #[error("Compaction error: {0}")]
    CompactionError(String),

    #[error("WAL error: {0}")]
    WalError(String),

    #[error("SSTable error: {0}")]
    SstableError(String),

    // ── 네트워크 / gRPC 오류 ───────────────────────────────
    #[error("gRPC transport error: {0}")]
    GrpcTransport(#[from] tonic::transport::Error),

    #[error("gRPC status error: {0}")]
    GrpcStatus(#[from] tonic::Status),

    // ── 메타데이터 / Raft 오류 ─────────────────────────────
    #[error("Metadata not found: {0}")]
    NotFound(String),

    #[error("Metadata conflict: {0}")]
    Conflict(String),

    #[error("Raft error: {0}")]
    RaftError(String),

    // ── 쿼리 실행 오류 ─────────────────────────────────────
    #[error("Query execution error: {0}")]
    QueryError(String),

    #[error("Query cancelled: {query_id}")]
    QueryCancelled { query_id: String },

    #[error("Query timeout: {query_id} exceeded {timeout_ms}ms")]
    QueryTimeout { query_id: String, timeout_ms: u64 },

    // ── 수집 오류 ──────────────────────────────────────────
    #[error("Ingestion error: {0}")]
    IngestionError(String),

    #[error("Schema mismatch: column '{column}' not found in cube '{cube}'")]
    SchemaMismatch { column: String, cube: String },

    // ── 트랜잭션 오류 ──────────────────────────────────────
    #[error("Transaction error: {0}")]
    TransactionError(String),

    #[error("Transaction not found: {tx_id}")]
    TransactionNotFound { tx_id: String },

    // ── 직렬화 오류 ────────────────────────────────────────
    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Arrow error: {0}")]
    ArrowError(String),

    // ── 인증/권한 오류 ─────────────────────────────────────
    #[error("Authentication failed: {0}")]
    AuthError(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    // ── 리소스 제한 ────────────────────────────────────────
    #[error("Resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),

    // ── 기타 ───────────────────────────────────────────────
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, WowDbError>;

// gRPC Status 변환
impl From<WowDbError> for tonic::Status {
    fn from(e: WowDbError) -> tonic::Status {
        match &e {
            WowDbError::NotFound(_) => tonic::Status::not_found(e.to_string()),
            WowDbError::Conflict(_) => tonic::Status::already_exists(e.to_string()),
            WowDbError::AuthError(_) => tonic::Status::unauthenticated(e.to_string()),
            WowDbError::PermissionDenied(_) => tonic::Status::permission_denied(e.to_string()),
            WowDbError::ParseError(_) | WowDbError::SchemaError(_) => {
                tonic::Status::invalid_argument(e.to_string())
            }
            WowDbError::QueryTimeout { .. } => tonic::Status::deadline_exceeded(e.to_string()),
            WowDbError::ResourceLimitExceeded(_) => {
                tonic::Status::resource_exhausted(e.to_string())
            }
            _ => tonic::Status::internal(e.to_string()),
        }
    }
}
