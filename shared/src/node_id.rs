// T022: NodeId, ClusterId 타입 및 유틸

use uuid::Uuid;
use serde::{Deserialize, Serialize};

/// 클러스터 내 노드 고유 식별자
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn generate() -> Self {
        Self(format!("node-{}", Uuid::new_v4()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// WOW-DB 클러스터 고유 식별자
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterId(pub Uuid);

impl ClusterId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }
}

impl Default for ClusterId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ClusterId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 쿼리 고유 식별자 생성
pub fn new_query_id() -> String {
    Uuid::new_v4().to_string()
}

/// Fragment 고유 식별자 생성
pub fn new_fragment_id() -> String {
    Uuid::new_v4().to_string()
}

/// 트랜잭션 고유 식별자 생성
pub fn new_tx_id() -> String {
    format!("tx-{}", Uuid::new_v4())
}
