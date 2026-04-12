// T103: MySQL 클라이언트 호환성 통합 테스트

#[cfg(feature = "integration")]
mod compat {
    use integration_tests::TestConfig;

    #[tokio::test]
    async fn test_mysql_async_connect() {
        todo!("Implement after Phase D MySQL Protocol")
    }

    #[tokio::test]
    async fn test_show_tables() {
        todo!("Implement after Phase D SHOW TABLES support")
    }

    #[tokio::test]
    async fn test_describe_table() {
        todo!("Implement after Phase D DESCRIBE support")
    }
}
