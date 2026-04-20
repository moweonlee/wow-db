// T071: DROP CUBE — Tablet 삭제 코디네이션, Raft 메타 정리

use std::sync::Arc;

use anyhow::{anyhow, Result};
use tracing::{info, warn};

use crate::meta::cube::CubeManager;
use crate::meta::tablet::TabletManager;
use crate::raft::{RaftCommand, RaftManager};

pub struct DropCubeHandler {
    cube_mgr:   Arc<CubeManager>,
    tablet_mgr: Arc<TabletManager>,
    raft:       Arc<RaftManager>,
}

impl DropCubeHandler {
    pub fn new(
        cube_mgr:   Arc<CubeManager>,
        tablet_mgr: Arc<TabletManager>,
        raft:       Arc<RaftManager>,
    ) -> Self {
        Self { cube_mgr, tablet_mgr, raft }
    }

    /// DROP CUBE 실행
    /// 순서: 1) 스키마 존재 확인  2) Tablet 목록 수집  3) Tablet 삭제  4) Raft 메타 정리
    pub async fn execute(&self, cube_name: &str, if_exists: bool) -> Result<()> {
        // 1. 스키마 조회
        let schema = match self.cube_mgr.get_by_name(cube_name).await? {
            Some(s) => s,
            None => {
                if if_exists {
                    info!(cube = %cube_name, "DROP CUBE IF EXISTS: cube not found, skipping");
                    return Ok(());
                }
                return Err(anyhow!("Cube '{}' not found", cube_name));
            }
        };

        let cube_id = schema.cube_id.to_string();
        info!(cube = %cube_name, cube_id = %cube_id, "Dropping cube");

        // 2. Tablet 목록 수집
        let tablets = self.tablet_mgr.list_for_cube(&cube_id).await
            .unwrap_or_default();

        // 3. 모든 SN에 HTTP POST /api/v1/tablet/{cube_name}/truncate 호출
        //    SN 물리 데이터(WAL + SSTable) 삭제 트리거
        {
            let sn_addrs: Vec<String> = std::env::var("STORAGE_NODES")
                .unwrap_or_default()
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .map(|grpc_addr| {
                    // gRPC 포트(9060~9069) → HTTP 포트(8040~8049) 변환
                    let addr = grpc_addr.trim();
                    if let Some(colon) = addr.rfind(':') {
                        let port_str = &addr[colon + 1..];
                        if let Ok(grpc_port) = port_str.parse::<u16>() {
                            let http_port = grpc_port.saturating_sub(1020);
                            return format!("{}:{}", &addr[..colon], http_port);
                        }
                    }
                    addr.to_string()
                })
                .collect();

            let client = reqwest::Client::new();
            for sn_http in &sn_addrs {
                let url = format!("http://{}/api/v1/tablet/{}/truncate", sn_http, cube_name);
                match client.post(&url).send().await {
                    Ok(resp) => info!(
                        cube = %cube_name,
                        sn   = %sn_http,
                        status = %resp.status(),
                        "Tablet truncated on SN"
                    ),
                    Err(e) => warn!(
                        cube = %cube_name,
                        sn   = %sn_http,
                        err  = %e,
                        "Failed to truncate tablet on SN (continuing)"
                    ),
                }
            }
            let _ = tablets; // suppress unused warning
        }

        // 4. Raft 메타 정리
        self.raft
            .write(RaftCommand::DropCube { cube_id: cube_id.clone() })
            .await?;

        info!(
            cube       = %cube_name,
            tablet_cnt = tablets.len(),
            "Cube dropped"
        );
        Ok(())
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use shared::types::{CubeSchema, PartitionKey, PartitionVariant, PartitionGranularity, Distribution};

    fn make_schema(name: &str) -> CubeSchema {
        CubeSchema::new(
            name,
            "default",
            Vec::new(),
            PartitionKey {
                variant: PartitionVariant::Range {
                    column: "event_time".into(),
                    granularity: Some(PartitionGranularity::Month),
                },
                auto_partition: true,
            },
            Vec::new(),
            Distribution { column: "device_id".into(), bucket_count: 16 },
        )
    }

    #[tokio::test]
    async fn test_drop_cube_not_found_if_exists() {
        let raft       = Arc::new(crate::raft::RaftManager::new_local());
        let cube_mgr   = Arc::new(CubeManager::new(raft.clone()));
        let tablet_mgr = Arc::new(TabletManager::new(raft.clone()));
        let handler    = DropCubeHandler::new(cube_mgr, tablet_mgr, raft);

        // IF EXISTS: 없어도 오류 없음
        handler.execute("nonexistent", true).await.unwrap();
    }

    #[tokio::test]
    async fn test_drop_cube_not_found_without_if_exists() {
        let raft       = Arc::new(crate::raft::RaftManager::new_local());
        let cube_mgr   = Arc::new(CubeManager::new(raft.clone()));
        let tablet_mgr = Arc::new(TabletManager::new(raft.clone()));
        let handler    = DropCubeHandler::new(cube_mgr, tablet_mgr, raft);

        // IF EXISTS 없음: 오류
        assert!(handler.execute("nonexistent", false).await.is_err());
    }

    #[tokio::test]
    async fn test_drop_existing_cube() {
        let raft       = Arc::new(crate::raft::RaftManager::new_local());
        let cube_mgr   = Arc::new(CubeManager::new(raft.clone()));
        let tablet_mgr = Arc::new(TabletManager::new(raft.clone()));
        let handler    = DropCubeHandler::new(cube_mgr.clone(), tablet_mgr, raft);

        let schema = make_schema("test_cube");
        cube_mgr.create(&schema).await.unwrap();
        assert!(cube_mgr.get_by_name("test_cube").await.unwrap().is_some());

        handler.execute("test_cube", false).await.unwrap();
        assert!(cube_mgr.get_by_name("test_cube").await.unwrap().is_none());
    }
}
