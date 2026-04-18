// T031: CubeSchema CRUD via Raft KV
// create / get / list / drop — RaftManager를 통한 메타데이터 영속화

use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use anyhow::{bail, Result};
use shared::types::CubeSchema;
use tracing::info;

use crate::meta::bt_registry::{BtEntry, BtRegistry, BtState};
use crate::raft::{RaftCommand, RaftManager};

// ─── Cube 관리자 ──────────────────────────────────────────────────────────────

pub struct CubeManager {
    raft: Arc<RaftManager>,
    /// Registry of Behavioral Tables (Session MVs) associated with event cubes
    bt_registry: RwLock<BtRegistry>,
}

impl CubeManager {
    pub fn new(raft: Arc<RaftManager>) -> Self {
        Self {
            raft,
            bt_registry: RwLock::new(BtRegistry::new()),
        }
    }

    // ── BtRegistry 접근자 ─────────────────────────────────────────────────────

    /// Read-only access to the BtRegistry.
    pub fn bt_registry(&self) -> RwLockReadGuard<BtRegistry> {
        self.bt_registry.read().expect("BtRegistry RwLock poisoned")
    }

    /// Mutable access to the BtRegistry.
    pub fn bt_registry_mut(&self) -> RwLockWriteGuard<BtRegistry> {
        self.bt_registry.write().expect("BtRegistry RwLock poisoned")
    }

    /// Register a Session MV (Behavioral Table) in the BtRegistry.
    ///
    /// Called when `CREATE SESSION MATERIALIZED VIEW` is executed.
    /// The BT is initially registered in `Building` state; callers should update
    /// to `Active` once materialization completes.
    pub fn register_behavioral_table(
        &self,
        smv_name: &str,
        source_cube: &str,
        user_key: &str,
        session_timeout_sec: u64,
    ) {
        let entry = BtEntry {
            bt_name: smv_name.to_string(),
            event_table: source_cube.to_string(),
            user_key: user_key.to_string(),
            session_timeout_sec,
            state: BtState::Building,
            last_refresh: None,
        };
        self.bt_registry_mut().register(entry);
        info!(
            smv = %smv_name,
            source = %source_cube,
            user_key = %user_key,
            "BT registered in BtRegistry (state=Building)"
        );
    }

    // ── 생성 ─────────────────────────────────────────────────────────────────

    pub async fn create(&self, schema: &CubeSchema) -> Result<()> {
        let cube_id = schema.cube_id.to_string();

        // 중복 확인
        if self.raft.read(&format!("cube:{cube_id}")).await.is_some() {
            bail!("Cube '{}' already exists", schema.name);
        }

        let schema_json = serde_json::to_string(schema)?;
        self.raft
            .write(RaftCommand::UpsertCube { cube_id: cube_id.clone(), schema_json })
            .await?;

        info!(cube_id = %cube_id, name = %schema.name, "Cube created");
        Ok(())
    }

    // ── 조회 ─────────────────────────────────────────────────────────────────

    pub async fn get(&self, cube_id: &str) -> Result<Option<CubeSchema>> {
        match self.raft.read(&format!("cube:{cube_id}")).await {
            None      => Ok(None),
            Some(json) => {
                let schema: CubeSchema = serde_json::from_str(&json)?;
                Ok(Some(schema))
            }
        }
    }

    /// 이름으로 Cube 조회
    pub async fn get_by_name(&self, name: &str) -> Result<Option<CubeSchema>> {
        // 현재는 전체 목록 스캔 (Phase E에서 인덱스 추가 예정)
        let all = self.list().await?;
        Ok(all.into_iter().find(|s| s.name == name))
    }

    // ── 목록 ─────────────────────────────────────────────────────────────────

    pub async fn list(&self) -> Result<Vec<CubeSchema>> {
        let sm = self.raft.sm.read().await;
        let mut cubes = Vec::new();
        for (k, v) in &sm.kv {
            if k.starts_with("cube:") {
                match serde_json::from_str::<CubeSchema>(v) {
                    Ok(schema) => cubes.push(schema),
                    Err(e)     => tracing::warn!(key = %k, err = %e, "Cube 역직렬화 실패"),
                }
            }
        }
        cubes.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(cubes)
    }

    // ── 수정 ─────────────────────────────────────────────────────────────────

    pub async fn update(&self, schema: &CubeSchema) -> Result<()> {
        let cube_id = schema.cube_id.to_string();
        if self.raft.read(&format!("cube:{cube_id}")).await.is_none() {
            bail!("Cube '{}' not found", schema.name);
        }
        let schema_json = serde_json::to_string(schema)?;
        self.raft
            .write(RaftCommand::UpsertCube { cube_id: cube_id.clone(), schema_json })
            .await?;
        info!(cube_id = %cube_id, name = %schema.name, "Cube updated");
        Ok(())
    }

    // ── 삭제 ─────────────────────────────────────────────────────────────────

    pub async fn drop_cube(&self, cube_id: &str) -> Result<()> {
        if self.raft.read(&format!("cube:{cube_id}")).await.is_none() {
            bail!("Cube '{}' not found", cube_id);
        }
        self.raft
            .write(RaftCommand::DropCube { cube_id: cube_id.to_string() })
            .await?;
        info!(cube_id = %cube_id, "Cube dropped");
        Ok(())
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use shared::types::{
        ColumnDef, ColumnRef, DataType, Distribution, Encoding, Compression,
        PartitionGranularity, PartitionKey, PartitionVariant, StorageBackend,
    };
    use uuid::Uuid;

    fn make_schema(name: &str) -> CubeSchema {
        use chrono::Utc;
        CubeSchema {
            cube_id:         Uuid::new_v4(),
            name:            name.to_string(),
            database:        "default".to_string(),
            columns:         vec![ColumnDef {
                name:           "event_time".to_string(),
                data_type:      DataType::DateTime,
                nullable:       false,
                encoding:       Encoding::Delta,
                compression:    Compression::Lz4,
                skipping_index: None,
                flat_json:      None,
                default_value:  None,
            }],
            partition_key:   PartitionKey {
                variant:        PartitionVariant::Range {
                    column:      "event_time".to_string(),
                    granularity: Some(PartitionGranularity::Month),
                },
                auto_partition: true,
            },
            sort_key:        vec![ColumnRef::new("event_time")],
            distribution:    Distribution { column: "user_id".to_string(), bucket_count: 32 },
            colocate_group:  None,
            storage_backend: StorageBackend::Native,
            ttl_policy:      None,
            tiering_policy:  None,
            created_at:      Utc::now(),
            version:         1,
        }
    }

    #[tokio::test]
    async fn test_create_and_get() {
        let raft = Arc::new(RaftManager::new(1));
        let mgr  = CubeManager::new(raft);
        let s    = make_schema("page_events");
        let id   = s.cube_id.to_string();

        mgr.create(&s).await.unwrap();
        let got = mgr.get(&id).await.unwrap();
        assert!(got.is_some());
        assert_eq!(got.unwrap().name, "page_events");
    }

    #[tokio::test]
    async fn test_duplicate_create() {
        let raft = Arc::new(RaftManager::new(1));
        let mgr  = CubeManager::new(raft);
        let s    = make_schema("dup_cube");
        #[allow(unused_variables)]

        mgr.create(&s).await.unwrap();
        assert!(mgr.create(&s).await.is_err());
    }

    #[tokio::test]
    async fn test_list() {
        let raft = Arc::new(RaftManager::new(1));
        let mgr  = CubeManager::new(raft);

        for name in ["cube_a", "cube_b", "cube_c"] {
            mgr.create(&make_schema(name)).await.unwrap();
        }
        let list = mgr.list().await.unwrap();
        assert_eq!(list.len(), 3);
    }

    #[tokio::test]
    async fn test_drop() {
        let raft = Arc::new(RaftManager::new(1));
        let mgr  = CubeManager::new(raft);
        let s    = make_schema("drop_me");
        let id   = s.cube_id.to_string();

        mgr.create(&s).await.unwrap();
        mgr.drop_cube(&id).await.unwrap();
        assert!(mgr.get(&id).await.unwrap().is_none());
    }
}
