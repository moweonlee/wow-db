// WOW-DB Integration Test Helpers
// Docker Compose 클러스터와 통신하는 테스트 유틸리티

pub mod cluster_management;

/// 기본 QN MySQL 주소
pub const QN_MYSQL_ADDR: &str = "mysql://admin@127.0.0.1:9030/default";
/// 기본 QN Web UI 주소
pub const QN_WEB_ADDR: &str = "http://127.0.0.1:8080";
/// MinIO S3 주소
pub const MINIO_ADDR: &str = "http://127.0.0.1:9000";
/// Redpanda Kafka 주소
pub const KAFKA_BROKERS: &str = "127.0.0.1:9092";

/// 통합 테스트 설정
pub struct TestConfig {
    pub qn_mysql_url: String,
    pub qn_web_url: String,
    pub kafka_brokers: String,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            qn_mysql_url: std::env::var("TEST_QN_MYSQL_URL")
                .unwrap_or_else(|_| QN_MYSQL_ADDR.to_string()),
            qn_web_url: std::env::var("TEST_QN_WEB_URL")
                .unwrap_or_else(|_| QN_WEB_ADDR.to_string()),
            kafka_brokers: std::env::var("TEST_KAFKA_BROKERS")
                .unwrap_or_else(|_| KAFKA_BROKERS.to_string()),
        }
    }
}
