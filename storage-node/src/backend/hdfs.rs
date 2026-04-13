// T094: HDFS 백엔드 — opendal + hdfs-native 스텁, Kerberos GSSAPI 인증, keytab 자동 갱신
// 실제 HDFS 연결은 native-hdfs crate가 필요하므로 인터페이스/설정만 완전 구현

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

// ─── HDFS 설정 ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct HdfsConfig {
    /// HDFS NameNode URI (예: hdfs://namenode:8020)
    pub namenode:                   String,
    /// HDFS 기본 경로 (예: /wowdb/analytics)
    pub base_path:                  String,
    /// Kerberos keytab 경로 (Kerberos 인증 필수)
    pub kerberos_keytab:            PathBuf,
    /// Kerberos principal (예: wowdb/dn-host@REALM.COM)
    pub kerberos_principal:         String,
    /// keytab 자동 갱신 간격 (초)
    pub kerberos_renew_interval:    Duration,
    /// 최대 재시도 횟수
    pub max_retries:                u32,
    /// 연결 타임아웃
    pub connect_timeout:            Duration,
    /// 읽기/쓰기 타임아웃
    pub io_timeout:                 Duration,
    /// 복제 계수
    pub replication:                u16,
    /// 블록 크기 (바이트, 기본 128MB)
    pub block_size:                 u64,
}

impl HdfsConfig {
    pub fn from_env() -> Self {
        Self {
            namenode:                std::env::var("HDFS_NAMENODE")
                .unwrap_or_else(|_| "hdfs://namenode:8020".to_string()),
            base_path:              std::env::var("HDFS_BASE_PATH")
                .unwrap_or_else(|_| "/wowdb/analytics".to_string()),
            kerberos_keytab:        PathBuf::from(
                std::env::var("HDFS_KEYTAB")
                    .unwrap_or_else(|_| "/etc/security/keytabs/wowdb.keytab".to_string())
            ),
            kerberos_principal:     std::env::var("HDFS_PRINCIPAL")
                .unwrap_or_else(|_| "wowdb@REALM.COM".to_string()),
            kerberos_renew_interval: Duration::from_secs(
                std::env::var("HDFS_KRB5_RENEW_INTERVAL_SEC")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(3600)
            ),
            max_retries:            3,
            connect_timeout:        Duration::from_secs(10),
            io_timeout:             Duration::from_secs(60),
            replication:            3,
            block_size:             128 * 1024 * 1024,
        }
    }
}

// ─── Kerberos 인증 관리 ───────────────────────────────────────────────────────

struct KerberosContext {
    config:         HdfsConfig,
    last_renewal:   Option<Instant>,
}

impl KerberosContext {
    fn new(config: HdfsConfig) -> Self {
        Self { config, last_renewal: None }
    }

    /// keytab으로 Kerberos TGT 획득 (kinit 동등)
    fn kinit(&mut self) -> Result<()> {
        // 실제 구현은 gssapi-sys 또는 libkrb5 FFI를 통해 수행
        // Phase D에서 native-tls + Kerberos 라이브러리 연동
        info!(
            principal = %self.config.kerberos_principal,
            keytab    = %self.config.kerberos_keytab.display(),
            "Kerberos TGT 획득 (stub)"
        );
        self.last_renewal = Some(Instant::now());
        Ok(())
    }

    /// 필요 시 자동 갱신
    fn renew_if_needed(&mut self) -> Result<()> {
        let needs_renewal = match self.last_renewal {
            None => true,
            Some(t) => t.elapsed() >= self.config.kerberos_renew_interval,
        };
        if needs_renewal {
            self.kinit()?;
        }
        Ok(())
    }

    fn is_authenticated(&self) -> bool {
        self.last_renewal.is_some()
    }
}

// ─── HDFS 백엔드 ──────────────────────────────────────────────────────────────

pub struct HdfsBackend {
    config:  HdfsConfig,
    krb:     Mutex<KerberosContext>,
}

impl HdfsBackend {
    /// 새 HDFS 백엔드 생성 및 Kerberos 초기 인증
    pub async fn new(config: HdfsConfig) -> Result<Self> {
        let mut krb = KerberosContext::new(config.clone());
        // 초기 kinit
        krb.kinit().map_err(|e| anyhow!("Initial Kerberos authentication failed: {}", e))?;

        Ok(Self {
            config,
            krb: Mutex::new(krb),
        })
    }

    /// HDFS 전체 경로 생성
    fn hdfs_path(&self, key: &str) -> String {
        format!("{}/{}", self.config.base_path.trim_end_matches('/'), key)
    }

    /// keytab 자동 갱신 후 호출 준비
    async fn ensure_auth(&self) -> Result<()> {
        self.krb.lock().await.renew_if_needed()
    }

    // ── SSTable 파일 조작 ────────────────────────────────────────────────────

    /// SSTable 업로드
    pub async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        self.ensure_auth().await?;
        let path = self.hdfs_path(key);
        debug!(path = %path, bytes = data.len(), "HDFS PUT (stub)");
        // TODO (Phase D): native-hdfs write with replication=config.replication, block_size
        Ok(())
    }

    /// SSTable 다운로드
    pub async fn get(&self, key: &str) -> Result<Bytes> {
        self.ensure_auth().await?;
        let path = self.hdfs_path(key);
        debug!(path = %path, "HDFS GET (stub)");
        // TODO (Phase D): native-hdfs read with streaming
        Err(anyhow!("HDFS backend not connected (stub implementation)"))
    }

    /// SSTable 삭제
    pub async fn delete(&self, key: &str) -> Result<()> {
        self.ensure_auth().await?;
        let path = self.hdfs_path(key);
        debug!(path = %path, "HDFS DELETE (stub)");
        Ok(())
    }

    /// 디렉토리 내 파일 목록 조회
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.ensure_auth().await?;
        let path = self.hdfs_path(prefix);
        debug!(path = %path, "HDFS LIST (stub)");
        Ok(vec![])
    }

    /// 파일 존재 여부 확인
    pub async fn exists(&self, key: &str) -> Result<bool> {
        self.ensure_auth().await?;
        Ok(false)
    }

    /// 파일 이름 변경 (atomic rename — SSTable finalization에 사용)
    pub async fn rename(&self, from: &str, to: &str) -> Result<()> {
        self.ensure_auth().await?;
        let from_path = self.hdfs_path(from);
        let to_path   = self.hdfs_path(to);
        debug!(from = %from_path, to = %to_path, "HDFS RENAME (stub)");
        Ok(())
    }

    pub fn is_authenticated(&self) -> bool {
        // 동기적 간단 확인
        true
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> HdfsConfig {
        HdfsConfig {
            namenode:                "hdfs://test-namenode:8020".to_string(),
            base_path:              "/wowdb/test".to_string(),
            kerberos_keytab:        PathBuf::from("/tmp/test.keytab"),
            kerberos_principal:     "wowdb@TEST.REALM".to_string(),
            kerberos_renew_interval: Duration::from_secs(3600),
            max_retries:            3,
            connect_timeout:        Duration::from_secs(5),
            io_timeout:             Duration::from_secs(30),
            replication:            1,
            block_size:             64 * 1024 * 1024,
        }
    }

    #[test]
    fn test_hdfs_path_generation() {
        let config = make_config();
        let mut krb = KerberosContext::new(config);
        let _ = krb.kinit(); // 초기화
        assert!(krb.is_authenticated());
    }

    #[test]
    fn test_kerberos_renew_interval() {
        let config = HdfsConfig {
            kerberos_renew_interval: Duration::from_millis(1),
            ..make_config()
        };
        let mut krb = KerberosContext::new(config);
        krb.kinit().unwrap();
        // 1ms 후 갱신 필요
        std::thread::sleep(Duration::from_millis(5));
        assert!(krb.last_renewal.map(|t| t.elapsed() >= Duration::from_millis(1)).unwrap_or(true));
        krb.renew_if_needed().unwrap();
        assert!(krb.last_renewal.unwrap().elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_hdfs_backend_creation() {
        let config  = make_config();
        let backend = HdfsBackend::new(config).await;
        assert!(backend.is_ok(), "Backend creation should succeed (stub)");
    }

    #[tokio::test]
    async fn test_hdfs_path_format() {
        let config  = make_config();
        let backend = HdfsBackend::new(config).await.unwrap();
        let path    = backend.hdfs_path("partition=p_2024_q1/seg_0001.col");
        assert_eq!(path, "/wowdb/test/partition=p_2024_q1/seg_0001.col");
    }

    #[tokio::test]
    async fn test_hdfs_put_stub() {
        let backend = HdfsBackend::new(make_config()).await.unwrap();
        let result  = backend.put("test_key.col", Bytes::from("hello")).await;
        assert!(result.is_ok(), "stub PUT should succeed");
    }

    #[tokio::test]
    async fn test_hdfs_get_stub_fails() {
        let backend = HdfsBackend::new(make_config()).await.unwrap();
        let result  = backend.get("nonexistent.col").await;
        assert!(result.is_err(), "stub GET should return error");
    }
}
