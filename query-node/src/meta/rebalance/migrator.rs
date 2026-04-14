// T181: ShardMigrator — 단일 shard 4단계 이전 실행
//
// 4-phase migration protocol:
//   Phase 1: PREPARE  — SN에 새 shard 디렉토리 준비 지시
//   Phase 2: COPY     — 소스 SN에서 대상 SN으로 shard 데이터 복사
//   Phase 3: SYNC     — WAL 델타 동기화 + 대상 SN을 Follower로 승격
//   Phase 4: COMMIT   — Raft KV에서 shard 소유권 변경 + 소스 정리

use anyhow::Result;
use tracing::{info, warn};
use uuid::Uuid;

/// Migration 상태
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationState {
    Pending,
    Preparing,
    Copying,
    Syncing,
    Committing,
    Done,
    Failed(String),
}

impl std::fmt::Display for MigrationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationState::Pending     => write!(f, "PENDING"),
            MigrationState::Preparing   => write!(f, "PREPARING"),
            MigrationState::Copying     => write!(f, "COPYING"),
            MigrationState::Syncing     => write!(f, "SYNCING"),
            MigrationState::Committing  => write!(f, "COMMITTING"),
            MigrationState::Done        => write!(f, "DONE"),
            MigrationState::Failed(msg) => write!(f, "FAILED: {}", msg),
        }
    }
}

/// Single-shard migration executor.
pub struct ShardMigrator {
    pub shard_id: Uuid,
    pub from_sn:  String,
    pub to_sn:    String,
    pub state:    MigrationState,
}

impl ShardMigrator {
    pub fn new(shard_id: Uuid, from_sn: impl Into<String>, to_sn: impl Into<String>) -> Self {
        Self {
            shard_id,
            from_sn: from_sn.into(),
            to_sn:   to_sn.into(),
            state:   MigrationState::Pending,
        }
    }

    /// Phase 1: 대상 SN에 shard 디렉토리 생성 요청
    pub async fn prepare(&mut self) -> Result<()> {
        self.state = MigrationState::Preparing;
        info!(
            shard_id = %self.shard_id,
            to_sn    = %self.to_sn,
            "Migration Phase 1: PREPARE"
        );
        // TODO: gRPC CreateShard(shard_id, to_sn) call
        // For now: stub OK
        Ok(())
    }

    /// Phase 2: 소스 → 대상 데이터 복사
    pub async fn copy_data(&mut self) -> Result<()> {
        self.state = MigrationState::Copying;
        info!(
            shard_id = %self.shard_id,
            from_sn  = %self.from_sn,
            to_sn    = %self.to_sn,
            "Migration Phase 2: COPY"
        );
        // TODO: gRPC CopyShard(shard_id, from_sn_addr, to_sn_addr) call
        Ok(())
    }

    /// Phase 3: WAL 델타 동기화 + Follower 승격
    pub async fn sync(&mut self) -> Result<()> {
        self.state = MigrationState::Syncing;
        info!(
            shard_id = %self.shard_id,
            "Migration Phase 3: SYNC"
        );
        // TODO: WAL delta sync + promote to Follower in Raft group
        Ok(())
    }

    /// Phase 4: Raft KV 소유권 변경 + 소스 shard 정리
    pub async fn commit(&mut self) -> Result<()> {
        self.state = MigrationState::Committing;
        info!(
            shard_id = %self.shard_id,
            "Migration Phase 4: COMMIT"
        );
        // TODO: Update ShardMap in Raft KV, then drop source shard
        self.state = MigrationState::Done;
        info!(shard_id = %self.shard_id, "Migration DONE");
        Ok(())
    }

    /// Full migration: 4단계 순차 실행 (중간 실패 시 상태 기록)
    pub async fn run(&mut self) -> Result<()> {
        if let Err(e) = self.prepare().await {
            let msg = e.to_string();
            warn!(shard_id = %self.shard_id, err = %msg, "Migration failed at PREPARE");
            self.state = MigrationState::Failed(msg);
            return Err(e);
        }
        if let Err(e) = self.copy_data().await {
            let msg = e.to_string();
            self.state = MigrationState::Failed(msg);
            return Err(e);
        }
        if let Err(e) = self.sync().await {
            let msg = e.to_string();
            self.state = MigrationState::Failed(msg);
            return Err(e);
        }
        if let Err(e) = self.commit().await {
            let msg = e.to_string();
            self.state = MigrationState::Failed(msg);
            return Err(e);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn migration_lifecycle_reaches_done() {
        let mut m = ShardMigrator::new(Uuid::new_v4(), "sn-1", "sn-2");
        assert_eq!(m.state, MigrationState::Pending);
        m.run().await.unwrap();
        assert_eq!(m.state, MigrationState::Done);
    }

    #[tokio::test]
    async fn state_display() {
        assert_eq!(MigrationState::Pending.to_string(),   "PENDING");
        assert_eq!(MigrationState::Done.to_string(),      "DONE");
        assert_eq!(MigrationState::Failed("oops".into()).to_string(), "FAILED: oops");
    }
}
