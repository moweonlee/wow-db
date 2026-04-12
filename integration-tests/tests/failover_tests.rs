// T104: 장애 복구 통합 테스트

#[cfg(feature = "integration")]
mod failover {
    #[tokio::test]
    async fn test_sn_single_node_failure() {
        // SN 1개 중단 → 쿼리 계속 성공 확인
        todo!("Implement after Phase B Storage Node HA")
    }

    #[tokio::test]
    async fn test_qn_leader_failover() {
        // QN Leader 중단 → Raft Follower가 Leader 선출 → 쿼리 재개
        todo!("Implement after Phase D QN Raft")
    }
}
