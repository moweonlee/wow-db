// T100: DDL 통합 테스트 (integration feature 필요)
// 실행: docker compose up -d --wait && cargo test -p integration-tests --features integration

#[cfg(feature = "integration")]
mod ddl {
    use integration_tests::TestConfig;

    #[tokio::test]
    async fn test_create_cube_basic() {
        // TODO: CREATE CUBE 실행 → 성공 확인
        // let config = TestConfig::default();
        // let pool = mysql_async::Pool::new(config.qn_mysql_url.as_str());
        // ...
        todo!("Implement after Phase D QN DDL support")
    }

    #[tokio::test]
    async fn test_create_session_mv() {
        todo!("Implement after Phase C SMV support")
    }

    #[tokio::test]
    async fn test_alter_cube_add_column() {
        todo!("Implement after Phase D ALTER CUBE support")
    }

    #[tokio::test]
    async fn test_drop_cube() {
        todo!("Implement after Phase D DROP CUBE support")
    }
}
