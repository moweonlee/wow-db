// T121: Query Node DiskMonitor — Raft WAL/스냅샷 디스크 모니터링 (FR-036)
// QN은 ClusterGuard를 직접 호출 (gRPC 없음)
// Raft WAL 디렉토리 + 스냅샷 디렉토리 감시
// 동일 임계값: disk_full_threshold=0.95, disk_recovery_threshold=0.85

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::meta::cluster_guard::{ClusterGuard, ReadOnlyReason};

// ─── QN DiskMonitor 설정 ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct QnDiskMonitorConfig {
    /// 폴링 주기 (기본: 5초)
    pub poll_interval:           Duration,
    /// Read-Only 진입 임계값 (기본: 95%)
    pub disk_full_threshold:     f64,
    /// 복구 임계값 (기본: 85%)
    pub disk_recovery_threshold: f64,
    /// 이 노드 식별자
    pub node_id:                 String,
    /// 감시할 경로 목록 (Raft WAL + 스냅샷 디렉토리)
    pub watch_paths:             Vec<PathBuf>,
}

impl Default for QnDiskMonitorConfig {
    fn default() -> Self {
        Self {
            poll_interval:           Duration::from_secs(5),
            disk_full_threshold:     0.95,
            disk_recovery_threshold: 0.85,
            node_id:                 "qn-1".to_string(),
            watch_paths: vec![
                PathBuf::from("/data/raft/wal"),
                PathBuf::from("/data/raft/snapshots"),
            ],
        }
    }
}

// ─── DiskUsageProvider 트레이트 ───────────────────────────────────────────────

/// 디스크 사용량 조회 추상화 (테스트 주입용)
pub trait DiskUsageProvider: Send + Sync + 'static {
    fn usage_ratio(&self, path: &std::path::Path) -> Result<f64>;
}

/// 실제 시스템 디스크 사용량 조회
pub struct SystemDiskUsageProvider;

impl DiskUsageProvider for SystemDiskUsageProvider {
    fn usage_ratio(&self, path: &std::path::Path) -> Result<f64> {
        native_disk_usage(path)
    }
}

#[cfg(windows)]
fn native_disk_usage(path: &std::path::Path) -> Result<f64> {
    use std::os::windows::ffi::OsStrExt;

    #[link(name = "kernel32")]
    extern "system" {
        fn GetDiskFreeSpaceExW(
            lpDirectoryName:              *const u16,
            lpFreeBytesAvailableToCaller: *mut u64,
            lpTotalNumberOfBytes:         *mut u64,
            lpTotalNumberOfFreeBytes:     *mut u64,
        ) -> i32;
    }

    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut caller_free = 0u64;
    let mut total       = 0u64;
    let mut total_free  = 0u64;

    let ok = unsafe {
        GetDiskFreeSpaceExW(wide.as_ptr(), &mut caller_free, &mut total, &mut total_free)
    };

    if ok == 0 {
        return Err(anyhow::anyhow!("GetDiskFreeSpaceExW 실패: path={:?}", path));
    }
    if total == 0 { return Ok(0.0); }

    let used = total.saturating_sub(caller_free);
    Ok(used as f64 / total as f64)
}

#[cfg(unix)]
fn native_disk_usage(path: &std::path::Path) -> Result<f64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| anyhow::anyhow!("CString 변환 실패: {}", e))?;

    let stat = unsafe {
        let mut st: libc::statvfs = std::mem::zeroed();
        let ret = libc::statvfs(c_path.as_ptr(), &mut st);
        if ret != 0 {
            return Err(anyhow::anyhow!("statvfs 실패: path={:?}", path));
        }
        st
    };

    if stat.f_blocks == 0 { return Ok(0.0); }
    let total = stat.f_blocks as u64 * stat.f_frsize as u64;
    let free  = stat.f_bavail as u64 * stat.f_frsize as u64;
    let used  = total.saturating_sub(free);
    Ok(used as f64 / total as f64)
}

#[cfg(not(any(windows, unix)))]
fn native_disk_usage(_path: &std::path::Path) -> Result<f64> {
    Ok(0.0)
}

// ─── 경로 상태 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathState {
    Normal,
    ReadOnly,
}

// ─── QnDiskMonitor ───────────────────────────────────────────────────────────

/// QN 전용 디스크 모니터
/// ClusterGuard를 직접 호출하여 Read-Only 상태 관리
pub struct QnDiskMonitor<P: DiskUsageProvider = SystemDiskUsageProvider> {
    config:        QnDiskMonitorConfig,
    provider:      Arc<P>,
    cluster_guard: Arc<ClusterGuard>,
    states:        Arc<Mutex<Vec<(PathBuf, PathState)>>>,
}

impl QnDiskMonitor<SystemDiskUsageProvider> {
    pub fn new(config: QnDiskMonitorConfig, cluster_guard: Arc<ClusterGuard>) -> Self {
        Self::with_provider(config, SystemDiskUsageProvider, cluster_guard)
    }
}

impl<P: DiskUsageProvider> QnDiskMonitor<P> {
    pub fn with_provider(
        config:        QnDiskMonitorConfig,
        provider:      P,
        cluster_guard: Arc<ClusterGuard>,
    ) -> Self {
        let states = config.watch_paths.iter()
            .map(|p| (p.clone(), PathState::Normal))
            .collect();
        Self {
            config,
            provider:      Arc::new(provider),
            cluster_guard,
            states:        Arc::new(Mutex::new(states)),
        }
    }

    /// 백그라운드 폴링 루프 (Arc<Self> 로 감싸 spawn)
    pub async fn run(self: Arc<Self>) {
        let mut interval = tokio::time::interval(self.config.poll_interval);
        loop {
            interval.tick().await;
            if let Err(e) = self.poll_once().await {
                warn!(err = %e, "QnDiskMonitor 폴링 오류");
            }
        }
    }

    /// 단일 폴링 사이클
    pub async fn poll_once(&self) -> Result<()> {
        let paths: Vec<PathBuf> = self.config.watch_paths.clone();

        for path in &paths {
            let usage = match self.provider.usage_ratio(path) {
                Ok(u)  => u,
                Err(e) => {
                    warn!(path = ?path, err = %e, "QN 디스크 사용량 조회 실패 — 스킵");
                    continue;
                }
            };

            let mut states = self.states.lock().await;
            if let Some((_, state)) = states.iter_mut().find(|(p, _)| p == path) {
                match state {
                    PathState::Normal if usage > self.config.disk_full_threshold => {
                        *state = PathState::ReadOnly;
                        let reason = ReadOnlyReason::DiskFull {
                            node_id: self.config.node_id.clone(),
                            path:    path.to_string_lossy().to_string(),
                            usage,
                        };
                        drop(states);
                        info!(
                            node_id = %self.config.node_id,
                            path    = ?path,
                            usage,
                            "QN 디스크 임계값 초과 — ClusterGuard Read-Only 진입"
                        );
                        self.cluster_guard.add_reason(reason).await?;
                    }
                    PathState::ReadOnly if usage <= self.config.disk_recovery_threshold => {
                        *state = PathState::Normal;
                        let reason = ReadOnlyReason::DiskFull {
                            node_id: self.config.node_id.clone(),
                            path:    path.to_string_lossy().to_string(),
                            usage,
                        };
                        drop(states);
                        info!(
                            node_id = %self.config.node_id,
                            path    = ?path,
                            usage,
                            "QN 디스크 복구 — ClusterGuard Read-Only 해제"
                        );
                        self.cluster_guard.remove_reason(&reason).await?;
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }
}

// ─── 단위 테스트 ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RaftManager;

    struct MockDiskUsageProvider {
        usage: std::sync::Mutex<f64>,
    }

    impl MockDiskUsageProvider {
        fn new(u: f64) -> Self { Self { usage: std::sync::Mutex::new(u) } }
        fn set(&self, u: f64) { *self.usage.lock().unwrap() = u; }
    }

    impl DiskUsageProvider for MockDiskUsageProvider {
        fn usage_ratio(&self, _: &std::path::Path) -> Result<f64> {
            Ok(*self.usage.lock().unwrap())
        }
    }

    fn make_monitor(usage: f64) -> (QnDiskMonitor<MockDiskUsageProvider>, Arc<MockDiskUsageProvider>, Arc<ClusterGuard>) {
        let raft    = Arc::new(RaftManager::new_local());
        let guard   = Arc::new(ClusterGuard::new(raft));
        let prov    = Arc::new(MockDiskUsageProvider::new(usage));
        let config  = QnDiskMonitorConfig {
            watch_paths: vec![PathBuf::from("/data/raft/wal")],
            ..Default::default()
        };
        let states = config.watch_paths.iter()
            .map(|p| (p.clone(), PathState::Normal))
            .collect();
        let monitor = QnDiskMonitor {
            config,
            provider:      prov.clone(),
            cluster_guard: guard.clone(),
            states:        Arc::new(Mutex::new(states)),
        };
        (monitor, prov, guard)
    }

    #[tokio::test]
    async fn test_high_usage_enters_read_only() {
        let (monitor, _, guard) = make_monitor(0.97);
        monitor.poll_once().await.unwrap();
        assert!(guard.check_write_allowed().await.is_err(), "97% 시 Read-Only 진입");
    }

    #[tokio::test]
    async fn test_normal_usage_no_state_change() {
        let (monitor, _, guard) = make_monitor(0.80);
        monitor.poll_once().await.unwrap();
        assert!(guard.check_write_allowed().await.is_ok(), "80% 시 정상 유지");
    }

    #[tokio::test]
    async fn test_recovery_exits_read_only() {
        let (monitor, prov, guard) = make_monitor(0.97);
        monitor.poll_once().await.unwrap();
        assert!(guard.check_write_allowed().await.is_err());

        prov.set(0.80);
        monitor.poll_once().await.unwrap();
        assert!(guard.check_write_allowed().await.is_ok(), "80%로 복구 후 정상 복귀");
    }
}
