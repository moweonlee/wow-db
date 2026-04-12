// T065: SN 측 2PC — Prepare 상태 유지, Commit 적용, Rollback 취소

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use tokio::sync::RwLock;
use tracing::{info, warn};

// ─── SN Transaction 상태 ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnTxState {
    /// 데이터 수신 중
    Active,
    /// Prepare 완료 (QN Commit 대기 중)
    Prepared,
    /// Commit 완료
    Committed,
    /// Rollback 완료
    RolledBack,
}

/// SN이 보관하는 트랜잭션 정보
#[derive(Debug, Clone)]
pub struct SnTransaction {
    pub tx_id:       u64,
    pub state:       SnTxState,
    /// 이 트랜잭션에 속한 Tablet ID 목록
    pub tablet_ids:  Vec<String>,
    /// Prepare 시점에 WAL에 기록된 LSN
    pub prepare_lsn: Option<u64>,
    pub started_at:  Instant,
}

impl SnTransaction {
    pub fn new(tx_id: u64) -> Self {
        Self {
            tx_id,
            state:       SnTxState::Active,
            tablet_ids:  Vec::new(),
            prepare_lsn: None,
            started_at:  Instant::now(),
        }
    }
}

// ─── SN Transaction Manager ──────────────────────────────────────────────────

pub struct SnTransactionManager {
    transactions: Arc<RwLock<HashMap<u64, SnTransaction>>>,
    timeout:      Duration,
}

impl SnTransactionManager {
    pub fn new() -> Self {
        Self::with_timeout(Duration::from_secs(120))
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            timeout,
        }
    }

    /// 트랜잭션 등록 (WriteRows 최초 수신 시)
    pub async fn register(&self, tx_id: u64) -> Result<()> {
        let mut txns = self.transactions.write().await;
        txns.entry(tx_id).or_insert_with(|| SnTransaction::new(tx_id));
        Ok(())
    }

    /// Tablet 연결
    pub async fn add_tablet(&self, tx_id: u64, tablet_id: &str) -> Result<()> {
        let mut txns = self.transactions.write().await;
        let tx = txns.get_mut(&tx_id)
            .ok_or_else(|| anyhow!("TX {} not found", tx_id))?;
        if !tx.tablet_ids.contains(&tablet_id.to_string()) {
            tx.tablet_ids.push(tablet_id.to_string());
        }
        Ok(())
    }

    /// Prepare Phase:
    /// - WAL에 PREPARE 마커 기록
    /// - 상태를 Prepared로 전환
    pub async fn prepare(&self, tx_id: u64, lsn: u64) -> Result<()> {
        let mut txns = self.transactions.write().await;
        let tx = txns.get_mut(&tx_id)
            .ok_or_else(|| anyhow!("TX {} not found", tx_id))?;

        if tx.state != SnTxState::Active {
            return Err(anyhow!("TX {} already in state {:?}", tx_id, tx.state));
        }

        if tx.started_at.elapsed() > self.timeout {
            tx.state = SnTxState::RolledBack;
            return Err(anyhow!("TX {} timed out during prepare", tx_id));
        }

        tx.state       = SnTxState::Prepared;
        tx.prepare_lsn = Some(lsn);
        info!(tx_id, lsn, "SN TX prepared");
        Ok(())
    }

    /// Commit Phase:
    /// - Prepared 상태인 TX를 Committed로 전환
    /// - MemTable 데이터가 이 시점에 가시화
    pub async fn commit(&self, tx_id: u64) -> Result<Vec<String>> {
        let mut txns = self.transactions.write().await;
        let tx = txns.get_mut(&tx_id)
            .ok_or_else(|| anyhow!("TX {} not found", tx_id))?;

        match tx.state {
            SnTxState::Prepared => {}
            SnTxState::Committed => {
                // idempotent commit
                return Ok(tx.tablet_ids.clone());
            }
            _ => return Err(anyhow!("Cannot commit TX {} in state {:?}", tx_id, tx.state)),
        }

        let tablet_ids = tx.tablet_ids.clone();
        tx.state = SnTxState::Committed;
        info!(tx_id, "SN TX committed");
        Ok(tablet_ids)
    }

    /// Rollback:
    /// - 해당 TX의 MemTable 항목 삭제 마커 삽입
    pub async fn rollback(&self, tx_id: u64) -> Result<Vec<String>> {
        let mut txns = self.transactions.write().await;
        let tx = txns.get_mut(&tx_id)
            .ok_or_else(|| anyhow!("TX {} not found", tx_id))?;

        if tx.state == SnTxState::Committed {
            return Err(anyhow!("Cannot rollback already committed TX {}", tx_id));
        }

        let tablet_ids = tx.tablet_ids.clone();
        tx.state = SnTxState::RolledBack;
        warn!(tx_id, "SN TX rolled back");
        Ok(tablet_ids)
    }

    /// 타임아웃 TX 정리
    pub async fn cleanup_timed_out(&self) -> Vec<u64> {
        let mut txns = self.transactions.write().await;
        let mut aborted = Vec::new();

        for tx in txns.values_mut() {
            if matches!(tx.state, SnTxState::Active | SnTxState::Prepared)
                && tx.started_at.elapsed() > self.timeout
            {
                tx.state = SnTxState::RolledBack;
                aborted.push(tx.tx_id);
                warn!(tx_id = tx.tx_id, "SN TX timed out and rolled back");
            }
        }

        // 완료된 항목 제거
        txns.retain(|_, tx| {
            matches!(tx.state, SnTxState::Active | SnTxState::Prepared)
        });

        aborted
    }

    pub async fn get_state(&self, tx_id: u64) -> Option<SnTxState> {
        self.transactions.read().await
            .get(&tx_id)
            .map(|tx| tx.state.clone())
    }

    pub async fn active_count(&self) -> usize {
        self.transactions.read().await
            .values()
            .filter(|tx| matches!(tx.state, SnTxState::Active | SnTxState::Prepared))
            .count()
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_full_2pc_lifecycle() {
        let mgr = SnTransactionManager::new();

        mgr.register(100).await.unwrap();
        mgr.add_tablet(100, "tablet-001").await.unwrap();
        mgr.add_tablet(100, "tablet-002").await.unwrap();

        assert_eq!(mgr.get_state(100).await, Some(SnTxState::Active));

        mgr.prepare(100, 42).await.unwrap();
        assert_eq!(mgr.get_state(100).await, Some(SnTxState::Prepared));

        let tablets = mgr.commit(100).await.unwrap();
        assert_eq!(tablets.len(), 2);
        assert_eq!(mgr.get_state(100).await, Some(SnTxState::Committed));
    }

    #[tokio::test]
    async fn test_rollback() {
        let mgr = SnTransactionManager::new();
        mgr.register(200).await.unwrap();
        mgr.add_tablet(200, "tablet-003").await.unwrap();
        mgr.prepare(200, 10).await.unwrap();

        let tablets = mgr.rollback(200).await.unwrap();
        assert_eq!(tablets.len(), 1);
        assert_eq!(mgr.get_state(200).await, Some(SnTxState::RolledBack));
    }

    #[tokio::test]
    async fn test_timeout_cleanup() {
        let mgr = SnTransactionManager::with_timeout(Duration::from_millis(1));
        mgr.register(300).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let aborted = mgr.cleanup_timed_out().await;
        assert!(aborted.contains(&300));
    }

    #[tokio::test]
    async fn test_idempotent_commit() {
        let mgr = SnTransactionManager::new();
        mgr.register(400).await.unwrap();
        mgr.prepare(400, 1).await.unwrap();
        let t1 = mgr.commit(400).await.unwrap();
        let t2 = mgr.commit(400).await.unwrap();
        assert_eq!(t1, t2);
    }
}
