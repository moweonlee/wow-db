// T183: ShardService gRPC stub (CreateShard / CopyShard)
// Full implementation in Phase D when cluster.proto is wired up.

use anyhow::Result;
use tracing::info;
use uuid::Uuid;

/// Request to prepare a new shard directory on this SN.
#[derive(Debug, Clone)]
pub struct CreateShardRequest {
    pub shard_id:  Uuid,
    pub table_id:  Uuid,
    pub data_dir:  String,
}

/// Request to copy shard data from a remote SN to this SN.
#[derive(Debug, Clone)]
pub struct CopyShardRequest {
    pub shard_id:      Uuid,
    pub source_addr:   String, // gRPC addr of source SN
    pub dest_data_dir: String,
}

/// ShardService: handles shard lifecycle operations on this Storage Node.
pub struct ShardService {
    /// Root data directory for this SN
    pub data_root: String,
}

impl ShardService {
    pub fn new(data_root: impl Into<String>) -> Self {
        Self { data_root: data_root.into() }
    }

    /// Phase 1 of 4-phase migration: prepare shard directory.
    pub async fn create_shard(&self, req: &CreateShardRequest) -> Result<String> {
        let shard_dir = format!(
            "{}/tables/{}/shards/{}",
            self.data_root, req.table_id, req.shard_id
        );
        // TODO (Phase D): tokio::fs::create_dir_all(&shard_dir).await?;
        // TODO (Phase D): Write shard metadata (tablet manifest)
        info!(
            shard_id  = %req.shard_id,
            table_id  = %req.table_id,
            shard_dir = %shard_dir,
            "CreateShard: stub — directory would be created here"
        );
        Ok(shard_dir)
    }

    /// Phase 2 of 4-phase migration: copy shard data from source SN.
    pub async fn copy_shard(&self, req: &CopyShardRequest) -> Result<()> {
        // TODO (Phase D): Connect to source SN gRPC, stream shard files
        info!(
            shard_id    = %req.shard_id,
            source_addr = %req.source_addr,
            dest_dir    = %req.dest_data_dir,
            "CopyShard: stub — data transfer would happen here"
        );
        Ok(())
    }

    /// Drop a shard after migration commit (cleanup).
    pub async fn drop_shard(&self, shard_id: Uuid, table_id: Uuid) -> Result<()> {
        let shard_dir = format!(
            "{}/tables/{}/shards/{}",
            self.data_root, table_id, shard_id
        );
        // TODO (Phase D): tokio::fs::remove_dir_all(&shard_dir).await?;
        info!(
            shard_id  = %shard_id,
            shard_dir = %shard_dir,
            "DropShard: stub — directory would be removed here"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_shard_returns_expected_path() {
        let svc      = ShardService::new("/data");
        let shard_id = Uuid::new_v4();
        let table_id = Uuid::new_v4();
        let req      = CreateShardRequest {
            shard_id,
            table_id,
            data_dir: "/data".to_string(),
        };
        let path = svc.create_shard(&req).await.unwrap();
        assert!(path.contains(&shard_id.to_string()));
        assert!(path.contains(&table_id.to_string()));
    }

    #[tokio::test]
    async fn copy_shard_stub_succeeds() {
        let svc      = ShardService::new("/data");
        let shard_id = Uuid::new_v4();
        let req      = CopyShardRequest {
            shard_id,
            source_addr:   "10.0.0.1:9060".to_string(),
            dest_data_dir: "/data/shards".to_string(),
        };
        svc.copy_shard(&req).await.unwrap();
    }

    #[tokio::test]
    async fn drop_shard_stub_succeeds() {
        let svc = ShardService::new("/data");
        svc.drop_shard(Uuid::new_v4(), Uuid::new_v4()).await.unwrap();
    }
}
