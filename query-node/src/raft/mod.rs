// T030: openraft 통합 — QN Raft 클러스터 초기화 스켈레톤
// StateMachine, LogStorage, Network 구현 뼈대
// 완전 구현: Phase E (영속화 + 실제 네트워크 전송)

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ─── Raft 노드 식별 ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default, PartialOrd, Ord)]
pub struct RaftNodeId(pub u64);

impl std::fmt::Display for RaftNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "QN-{}", self.0)
    }
}

// ─── Raft 커맨드 (상태 머신 입력) ────────────────────────────────────────────

/// Raft에 커밋되는 명령 단위
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftCommand {
    /// Cube 스키마 생성/수정
    UpsertCube {
        cube_id:     String,
        schema_json: String,
    },
    /// Cube 삭제
    DropCube { cube_id: String },
    /// Tablet 위치 정보 등록
    RegisterTablet {
        tablet_id: String,
        node_id:   u64,
        partition: String,
    },
    /// 컬럼 통계 갱신
    UpdateStats {
        cube_id:    String,
        stats_json: String,
    },
    /// 세션 토큰 등록
    SessionSet { key: String, value: String },
    /// 세션 토큰 삭제
    SessionDel { key: String },
    /// 범용 KV 쓰기
    UpsertKv { key: String, value: String },
    /// 범용 KV 삭제
    DeleteKv { key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftResponse {
    pub ok: bool,
}

// ─── 상태 머신 ────────────────────────────────────────────────────────────────

/// 메타데이터 KV 상태 머신 (JSON 스냅샷 영속화)
#[derive(Debug, Default)]
pub struct MetaStateMachine {
    /// KV 저장소: "cube:{id}" → schema_json, "tablet:{id}" → info_json 등
    pub kv: BTreeMap<String, String>,
}

impl MetaStateMachine {
    /// 파일에서 KV 스냅샷을 로드 (QN 재시작 복구용)
    fn load_from_file(path: &Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(json) => match serde_json::from_str::<BTreeMap<String, String>>(&json) {
                Ok(kv) => {
                    info!(path = %path.display(), entries = kv.len(), "Raft: snapshot loaded");
                    MetaStateMachine { kv }
                }
                Err(e) => {
                    warn!(path = %path.display(), err = %e, "Raft: corrupt snapshot, starting fresh");
                    MetaStateMachine::default()
                }
            },
            Err(_) => MetaStateMachine::default(),
        }
    }
}

impl MetaStateMachine {
    pub fn apply(&mut self, cmd: &RaftCommand) {
        match cmd {
            RaftCommand::UpsertCube { cube_id, schema_json } => {
                self.kv.insert(format!("cube:{cube_id}"), schema_json.clone());
            }
            RaftCommand::DropCube { cube_id } => {
                self.kv.remove(&format!("cube:{cube_id}"));
            }
            RaftCommand::RegisterTablet { tablet_id, node_id, partition } => {
                let v = serde_json::json!({
                    "node_id":   node_id,
                    "partition": partition
                })
                .to_string();
                self.kv.insert(format!("tablet:{tablet_id}"), v);
            }
            RaftCommand::UpdateStats { cube_id, stats_json } => {
                self.kv.insert(format!("stats:{cube_id}"), stats_json.clone());
            }
            RaftCommand::SessionSet { key, value } => {
                self.kv.insert(format!("session:{key}"), value.clone());
            }
            RaftCommand::SessionDel { key } => {
                self.kv.remove(&format!("session:{key}"));
            }
            RaftCommand::UpsertKv { key, value } => {
                self.kv.insert(key.clone(), value.clone());
            }
            RaftCommand::DeleteKv { key } => {
                self.kv.remove(key);
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.kv.get(key)
    }
}

// ─── Raft 클러스터 매니저 ─────────────────────────────────────────────────────

/// QN Raft 클러스터 초기화 및 메타데이터 인터페이스
///
/// Phase B 구현:
/// - openraft::Raft<WowDbTypeConfig> 인스턴스 래핑
/// - 실제 로그 직렬화/역직렬화
/// - gRPC 기반 QN 간 Raft RPC 전송
pub struct RaftManager {
    pub node_id: RaftNodeId,
    /// 공유 상태 머신 (읽기/쓰기 잠금)
    pub sm:      Arc<RwLock<MetaStateMachine>>,
    /// JSON 스냅샷 파일 경로 (None이면 메모리 전용)
    snapshot_path: Option<PathBuf>,
}

impl RaftManager {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id:       RaftNodeId(node_id),
            sm:            Arc::new(RwLock::new(MetaStateMachine::default())),
            snapshot_path: None,
        }
    }

    /// 디스크 영속화를 활성화한 RaftManager 생성
    /// data_dir: QN 노드별 고유 디렉터리 (재시작 후 복구에 사용)
    pub fn new_with_persistence(node_id: u64, data_dir: &Path) -> Self {
        let snapshot_path = data_dir.join("raft_snapshot.json");
        let sm = MetaStateMachine::load_from_file(&snapshot_path);
        let _ = std::fs::create_dir_all(data_dir); // ensure dir exists
        Self {
            node_id:       RaftNodeId(node_id),
            sm:            Arc::new(RwLock::new(sm)),
            snapshot_path: Some(snapshot_path),
        }
    }

    /// Raft 클러스터 초기화
    ///
    /// `peers`: [(node_id, "host:raft_port"), ...]
    pub async fn start(&self, peers: Vec<(u64, String)>) -> Result<()> {
        info!(
            node_id = %self.node_id,
            peers   = peers.len(),
            "Raft 클러스터 초기화 (Phase B 구현 예정)"
        );
        // TODO (Phase B):
        // 1. openraft::Config 구성
        // 2. MemLogStore + MemStateMachine + NetworkFactory 구성
        // 3. openraft::Raft::new() 호출
        // 4. 단독 노드: raft.initialize(members).await
        // 5. 복수 노드: 피어 gRPC 연결 후 클러스터 합류
        Ok(())
    }

    /// 커맨드를 Raft 로그에 기록하고 상태 머신에 적용 (Phase B 전: 직접 적용)
    pub async fn write(&self, cmd: RaftCommand) -> Result<RaftResponse> {
        let snapshot = {
            let mut sm = self.sm.write().await;
            sm.apply(&cmd);
            self.snapshot_path.as_ref().map(|_| sm.kv.clone())
        };

        // JSON 스냅샷을 원자적 파일로 영속화 (tmp → rename)
        if let (Some(kv), Some(path)) = (snapshot, &self.snapshot_path) {
            match serde_json::to_string(&kv) {
                Ok(json) => {
                    let tmp = path.with_extension("json.tmp");
                    if let Err(e) = tokio::fs::write(&tmp, &json).await
                        .and_then(|_| std::fs::rename(&tmp, path).map_err(Into::into))
                    {
                        warn!(path = %path.display(), err = %e, "Raft: snapshot persist failed");
                    }
                }
                Err(e) => warn!(err = %e, "Raft: snapshot serialize failed"),
            }
        }

        debug!(cmd = ?cmd, "Raft write");
        Ok(RaftResponse { ok: true })
    }

    /// KV 읽기 (로컬 상태 머신 조회)
    pub async fn read(&self, key: &str) -> Option<String> {
        self.sm.read().await.get(key).cloned()
    }

    /// 상태 머신 직접 접근 (메타 관리자 전용)
    pub fn state_machine(&self) -> Arc<RwLock<MetaStateMachine>> {
        self.sm.clone()
    }

    /// 로컬(단일 노드) RaftManager 생성 — 테스트 전용 (메모리 전용, 영속화 없음)
    pub fn new_local() -> Self {
        Self::new(1)
    }

    /// prefix로 시작하는 모든 KV 항목 반환
    pub async fn scan_prefix(&self, prefix: &str) -> Vec<(String, String)> {
        self.sm.read().await
            .kv
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_write_and_read() {
        let mgr = RaftManager::new(1);

        mgr.write(RaftCommand::UpsertCube {
            cube_id:     "events".to_string(),
            schema_json: r#"{"name":"events"}"#.to_string(),
        })
        .await
        .unwrap();

        let val = mgr.read("cube:events").await;
        assert_eq!(val.as_deref(), Some(r#"{"name":"events"}"#));
    }

    #[tokio::test]
    async fn test_drop_cube() {
        let mgr = RaftManager::new(1);
        mgr.write(RaftCommand::UpsertCube {
            cube_id:     "tmp".to_string(),
            schema_json: "{}".to_string(),
        })
        .await
        .unwrap();
        mgr.write(RaftCommand::DropCube { cube_id: "tmp".to_string() })
            .await
            .unwrap();
        assert!(mgr.read("cube:tmp").await.is_none());
    }
}
