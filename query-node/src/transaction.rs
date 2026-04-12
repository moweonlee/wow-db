// T062: 2PC Transaction Manager — TxID 발급, Prepare/Commit/Rollback 코디네이션

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use tokio::sync::RwLock;
use tracing::{info, warn};
use uuid::Uuid;

// ─── Transaction 상태 ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxState {
    /// 트랜잭션 시작 — 참가자 등록 중
    Active,
    /// Prepare 완료 — 모든 참가자 Prepare ACK 수신
    Prepared,
    /// Commit 완료
    Committed,
    /// Rollback 완료
    RolledBack,
    /// 타임아웃으로 중단
    Aborted,
}

/// 2PC 참가자 (Data Node 또는 Compute Node)
#[derive(Debug, Clone)]
pub struct TxParticipant {
    pub node_id:  String,
    pub endpoint: String,
    pub prepared: bool,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub tx_id:       String,
    pub state:       TxState,
    pub participants: Vec<TxParticipant>,
    pub started_at:  Instant,
    /// 쓰기 대상 Cube 이름
    pub cube_name:   String,
}

impl Transaction {
    pub fn new(cube_name: String) -> Self {
        Self {
            tx_id:        Uuid::new_v4().to_string(),
            state:        TxState::Active,
            participants: Vec::new(),
            started_at:   Instant::now(),
            cube_name,
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub fn is_timed_out(&self, timeout: Duration) -> bool {
        self.elapsed() > timeout
    }
}

// ─── Transaction Manager ──────────────────────────────────────────────────────

pub struct TransactionManager {
    transactions: Arc<RwLock<HashMap<String, Transaction>>>,
    /// 트랜잭션 타임아웃 (기본 60초)
    timeout:      Duration,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self::with_timeout(Duration::from_secs(60))
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            timeout,
        }
    }

    /// 새 트랜잭션 시작 — TxID 반환
    pub async fn begin(&self, cube_name: &str) -> Result<String> {
        let tx = Transaction::new(cube_name.to_string());
        let tx_id = tx.tx_id.clone();
        self.transactions.write().await.insert(tx_id.clone(), tx);
        info!(tx_id = %tx_id, cube = %cube_name, "Transaction started");
        Ok(tx_id)
    }

    /// 참가자 등록
    pub async fn register_participant(
        &self,
        tx_id:    &str,
        node_id:  &str,
        endpoint: &str,
    ) -> Result<()> {
        let mut txns = self.transactions.write().await;
        let tx = txns.get_mut(tx_id)
            .ok_or_else(|| anyhow!("Transaction '{}' not found", tx_id))?;

        if tx.state != TxState::Active {
            return Err(anyhow!("Transaction '{}' is not active", tx_id));
        }

        tx.participants.push(TxParticipant {
            node_id:  node_id.to_string(),
            endpoint: endpoint.to_string(),
            prepared: false,
        });
        Ok(())
    }

    /// Prepare ACK 수신
    pub async fn receive_prepare_ack(&self, tx_id: &str, node_id: &str) -> Result<bool> {
        let mut txns = self.transactions.write().await;
        let tx = txns.get_mut(tx_id)
            .ok_or_else(|| anyhow!("Transaction '{}' not found", tx_id))?;

        for p in &mut tx.participants {
            if p.node_id == node_id {
                p.prepared = true;
                break;
            }
        }

        // 모든 참가자 Prepare 완료 여부 확인
        let all_prepared = tx.participants.iter().all(|p| p.prepared);
        if all_prepared && tx.state == TxState::Active {
            tx.state = TxState::Prepared;
            info!(tx_id = %tx_id, "All participants prepared");
        }
        Ok(all_prepared)
    }

    /// 2PC Commit Phase
    /// 반환: 커밋 대상 참가자 목록 (coordinator가 gRPC Commit 호출 담당)
    pub async fn prepare_commit(&self, tx_id: &str) -> Result<Vec<TxParticipant>> {
        let mut txns = self.transactions.write().await;
        let tx = txns.get_mut(tx_id)
            .ok_or_else(|| anyhow!("Transaction '{}' not found", tx_id))?;

        match tx.state {
            TxState::Active | TxState::Prepared => {}
            _ => return Err(anyhow!("Cannot commit tx '{}' in state {:?}", tx_id, tx.state)),
        }

        if tx.is_timed_out(self.timeout) {
            tx.state = TxState::Aborted;
            return Err(anyhow!("Transaction '{}' timed out", tx_id));
        }

        Ok(tx.participants.clone())
    }

    /// Commit 완료 기록
    pub async fn mark_committed(&self, tx_id: &str) -> Result<()> {
        let mut txns = self.transactions.write().await;
        let tx = txns.get_mut(tx_id)
            .ok_or_else(|| anyhow!("Transaction '{}' not found", tx_id))?;
        tx.state = TxState::Committed;
        info!(tx_id = %tx_id, "Transaction committed");
        Ok(())
    }

    /// Rollback
    pub async fn rollback(&self, tx_id: &str) -> Result<Vec<TxParticipant>> {
        let mut txns = self.transactions.write().await;
        let tx = txns.get_mut(tx_id)
            .ok_or_else(|| anyhow!("Transaction '{}' not found", tx_id))?;

        let participants = tx.participants.clone();
        tx.state = TxState::RolledBack;
        warn!(tx_id = %tx_id, "Transaction rolled back");
        Ok(participants)
    }

    /// 상태 조회
    pub async fn get_state(&self, tx_id: &str) -> Option<TxState> {
        self.transactions.read().await
            .get(tx_id)
            .map(|tx| tx.state.clone())
    }

    /// 타임아웃된 트랜잭션 정리 (백그라운드 태스크)
    pub async fn cleanup_timed_out(&self) -> usize {
        let mut txns = self.transactions.write().await;
        let mut aborted = 0;
        for tx in txns.values_mut() {
            if matches!(tx.state, TxState::Active | TxState::Prepared)
                && tx.is_timed_out(self.timeout)
            {
                tx.state = TxState::Aborted;
                warn!(tx_id = %tx.tx_id, "Transaction aborted due to timeout");
                aborted += 1;
            }
        }
        // 완료된 트랜잭션 제거 (커밋/롤백/중단 후 5분 보관 정책 → 단순화: 즉시 제거)
        txns.retain(|_, tx| {
            matches!(tx.state, TxState::Active | TxState::Prepared)
        });
        aborted
    }

    pub async fn active_count(&self) -> usize {
        self.transactions.read().await
            .values()
            .filter(|tx| matches!(tx.state, TxState::Active | TxState::Prepared))
            .count()
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_begin_and_commit() {
        let mgr = TransactionManager::new();
        let tx_id = mgr.begin("events").await.unwrap();
        assert!(!tx_id.is_empty());

        mgr.register_participant(&tx_id, "dn-1", "dn-1:9060").await.unwrap();
        mgr.receive_prepare_ack(&tx_id, "dn-1").await.unwrap();

        let participants = mgr.prepare_commit(&tx_id).await.unwrap();
        assert_eq!(participants.len(), 1);

        mgr.mark_committed(&tx_id).await.unwrap();
        assert_eq!(mgr.get_state(&tx_id).await, Some(TxState::Committed));
    }

    #[tokio::test]
    async fn test_rollback() {
        let mgr = TransactionManager::new();
        let tx_id = mgr.begin("events").await.unwrap();
        mgr.register_participant(&tx_id, "dn-1", "dn-1:9060").await.unwrap();

        let participants = mgr.rollback(&tx_id).await.unwrap();
        assert_eq!(participants.len(), 1);
        assert_eq!(mgr.get_state(&tx_id).await, Some(TxState::RolledBack));
    }

    #[tokio::test]
    async fn test_timeout_cleanup() {
        let mgr = TransactionManager::with_timeout(Duration::from_millis(1));
        let _tx_id = mgr.begin("events").await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let aborted = mgr.cleanup_timed_out().await;
        assert_eq!(aborted, 1);
    }

    #[tokio::test]
    async fn test_all_prepared_transition() {
        let mgr = TransactionManager::new();
        let tx_id = mgr.begin("cube").await.unwrap();
        mgr.register_participant(&tx_id, "dn-1", ":9060").await.unwrap();
        mgr.register_participant(&tx_id, "dn-2", ":9061").await.unwrap();

        let all = mgr.receive_prepare_ack(&tx_id, "dn-1").await.unwrap();
        assert!(!all);
        assert_eq!(mgr.get_state(&tx_id).await, Some(TxState::Active));

        let all = mgr.receive_prepare_ack(&tx_id, "dn-2").await.unwrap();
        assert!(all);
        assert_eq!(mgr.get_state(&tx_id).await, Some(TxState::Prepared));
    }
}
