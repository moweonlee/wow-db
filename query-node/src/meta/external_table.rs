// T098: External Table — S3, HDFS, Iceberg/Hive Metastore 가상 테이블 메타데이터 등록

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::raft::{RaftCommand, RaftManager};

// ─── External Table 소스 ─────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExternalSource {
    /// S3 / MinIO 오브젝트 스토리지
    S3 {
        bucket:   String,
        prefix:   String,
        region:   Option<String>,
        endpoint: Option<String>,
        format:   FileFormat,
    },
    /// HDFS (Kerberos 인증)
    Hdfs {
        namenode:   String,
        path:       String,
        format:     FileFormat,
    },
    /// Iceberg 테이블
    Iceberg {
        catalog_uri:  String,
        database:     String,
        table:        String,
        format:       FileFormat,
    },
    /// Hive Metastore
    HiveMetastore {
        metastore_uri: String,
        database:      String,
        table:         String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FileFormat {
    Parquet,
    Orc,
    Json,
    Csv { delimiter: char, has_header: bool },
    Avro,
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileFormat::Parquet             => write!(f, "PARQUET"),
            FileFormat::Orc                 => write!(f, "ORC"),
            FileFormat::Json                => write!(f, "JSON"),
            FileFormat::Csv { .. }          => write!(f, "CSV"),
            FileFormat::Avro                => write!(f, "AVRO"),
        }
    }
}

// ─── External Table 정의 ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalColumnDef {
    pub name:      String,
    pub data_type: String,
    pub nullable:  bool,
    pub comment:   Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalTableDef {
    /// 테이블 이름 (WOW-DB 내부 식별자)
    pub name:         String,
    /// 소속 데이터베이스
    pub database:     String,
    /// 외부 소스 정보
    pub source:       ExternalSource,
    /// 컬럼 정의 (None이면 소스에서 스키마 자동 추론)
    pub columns:      Option<Vec<ExternalColumnDef>>,
    /// 파티션 컬럼 (선택적)
    pub partition_by: Vec<String>,
    /// 생성 시각 (Unix epoch ms)
    pub created_at:   u64,
    /// 마지막 스키마 새로고침 시각
    pub refreshed_at: Option<u64>,
    /// 설명
    pub comment:      Option<String>,
}

// ─── External Table Manager ───────────────────────────────────────────────────

const EXT_TABLE_PREFIX: &str = "external_table/";

pub struct ExternalTableManager {
    raft: Arc<RaftManager>,
}

impl ExternalTableManager {
    pub fn new(raft: Arc<RaftManager>) -> Self {
        Self { raft }
    }

    fn raft_key(database: &str, name: &str) -> String {
        format!("{}{}/{}", EXT_TABLE_PREFIX, database, name)
    }

    /// External Table 등록 (CREATE EXTERNAL TABLE)
    pub async fn create(
        &self,
        table_def: ExternalTableDef,
    ) -> Result<()> {
        let key = Self::raft_key(&table_def.database, &table_def.name);

        // 중복 확인
        if self.raft.read(&key).await.is_some() {
            return Err(anyhow!(
                "External table '{}.{}' already exists",
                table_def.database, table_def.name
            ));
        }

        let json = serde_json::to_string(&table_def)?;
        self.raft.write(RaftCommand::UpsertKv {
            key:   key.clone(),
            value: json,
        }).await?;

        info!(
            database = %table_def.database,
            name     = %table_def.name,
            "External table created"
        );
        Ok(())
    }

    /// External Table 삭제 (DROP EXTERNAL TABLE)
    pub async fn drop(
        &self,
        database: &str,
        name:     &str,
        if_exists: bool,
    ) -> Result<()> {
        let key = Self::raft_key(database, name);
        if self.raft.read(&key).await.is_none() {
            if if_exists {
                return Ok(());
            }
            return Err(anyhow!("External table '{}.{}' not found", database, name));
        }
        self.raft.write(RaftCommand::DeleteKv { key }).await?;
        info!(database = %database, name = %name, "External table dropped");
        Ok(())
    }

    /// External Table 메타데이터 조회
    pub async fn get(
        &self,
        database: &str,
        name:     &str,
    ) -> Result<Option<ExternalTableDef>> {
        let key = Self::raft_key(database, name);
        match self.raft.read(&key).await {
            Some(json) => Ok(Some(serde_json::from_str(&json)?)),
            None       => Ok(None),
        }
    }

    /// 데이터베이스 내 External Table 목록
    pub async fn list(&self, database: &str) -> Result<Vec<ExternalTableDef>> {
        let prefix = format!("{}{}/", EXT_TABLE_PREFIX, database);
        let pairs  = self.raft.scan_prefix(&prefix).await;
        let mut tables = vec![];
        for (_, json) in pairs {
            if let Ok(t) = serde_json::from_str::<ExternalTableDef>(&json) {
                tables.push(t);
            }
        }
        Ok(tables)
    }

    /// 스키마 새로고침 (외부 소스에서 컬럼 목록 재조회)
    pub async fn refresh_schema(
        &self,
        database: &str,
        name:     &str,
    ) -> Result<()> {
        let key = Self::raft_key(database, name);
        let json = self.raft.read(&key).await
            .ok_or_else(|| anyhow!("External table '{}.{}' not found", database, name))?;
        let mut table: ExternalTableDef = serde_json::from_str(&json)?;

        // TODO (Phase D): 실제 소스에서 스키마 조회
        // match &table.source {
        //   ExternalSource::S3 { .. } => infer_parquet_schema(...),
        //   ExternalSource::Iceberg { .. } => iceberg_catalog.get_schema(...),
        //   ...
        // }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        table.refreshed_at = Some(now_ms);

        self.raft.write(RaftCommand::UpsertKv {
            key,
            value: serde_json::to_string(&table)?,
        }).await?;

        info!(database = %database, name = %name, "External table schema refreshed");
        Ok(())
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RaftManager;

    fn make_mgr() -> ExternalTableManager {
        ExternalTableManager::new(Arc::new(RaftManager::new_local()))
    }

    fn make_s3_table(name: &str) -> ExternalTableDef {
        ExternalTableDef {
            name:         name.to_string(),
            database:     "analytics".to_string(),
            source:       ExternalSource::S3 {
                bucket:   "my-data".to_string(),
                prefix:   "events/".to_string(),
                region:   Some("ap-northeast-2".to_string()),
                endpoint: None,
                format:   FileFormat::Parquet,
            },
            columns:      Some(vec![
                ExternalColumnDef {
                    name:      "event_time".to_string(),
                    data_type: "DATETIME".to_string(),
                    nullable:  false,
                    comment:   None,
                },
                ExternalColumnDef {
                    name:      "user_id".to_string(),
                    data_type: "VARCHAR".to_string(),
                    nullable:  false,
                    comment:   None,
                },
            ]),
            partition_by: vec!["event_time".to_string()],
            created_at:   0,
            refreshed_at: None,
            comment:      Some("S3 이벤트 외부 테이블".to_string()),
        }
    }

    #[tokio::test]
    async fn test_create_and_get() {
        let mgr   = make_mgr();
        let table = make_s3_table("ext_events");
        mgr.create(table.clone()).await.unwrap();

        let fetched = mgr.get("analytics", "ext_events").await.unwrap().unwrap();
        assert_eq!(fetched.name, "ext_events");
        assert!(matches!(fetched.source, ExternalSource::S3 { .. }));
    }

    #[tokio::test]
    async fn test_duplicate_create_fails() {
        let mgr = make_mgr();
        mgr.create(make_s3_table("dup")).await.unwrap();
        assert!(mgr.create(make_s3_table("dup")).await.is_err());
    }

    #[tokio::test]
    async fn test_drop() {
        let mgr = make_mgr();
        mgr.create(make_s3_table("to_drop")).await.unwrap();
        mgr.drop("analytics", "to_drop", false).await.unwrap();
        assert!(mgr.get("analytics", "to_drop").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_drop_if_exists() {
        let mgr = make_mgr();
        // 존재하지 않는 테이블, if_exists=true → 오류 없음
        assert!(mgr.drop("analytics", "nonexistent", true).await.is_ok());
        // if_exists=false → 오류
        assert!(mgr.drop("analytics", "nonexistent", false).await.is_err());
    }

    #[tokio::test]
    async fn test_list() {
        let mgr = make_mgr();
        mgr.create(make_s3_table("t1")).await.unwrap();
        mgr.create(make_s3_table("t2")).await.unwrap();
        let tables = mgr.list("analytics").await.unwrap();
        assert_eq!(tables.len(), 2);
    }

    #[tokio::test]
    async fn test_refresh_schema() {
        let mgr = make_mgr();
        mgr.create(make_s3_table("refresh_test")).await.unwrap();
        mgr.refresh_schema("analytics", "refresh_test").await.unwrap();

        let t = mgr.get("analytics", "refresh_test").await.unwrap().unwrap();
        assert!(t.refreshed_at.is_some());
    }
}
