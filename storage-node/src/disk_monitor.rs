// T120: DiskMonitor — 디스크 용량 감시 (FR-036)
// 5초 주기 폴링, disk_full_threshold=0.95 초과 시 QN에 ReportDiskFull 전송
// disk_recovery_threshold=0.85 이하 시 ReportDiskRecovered 전송
// DiskProbe 트레이트로 플랫폼별 구현 추상화 (테스트 주입 가능)

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::{info, warn};

// ─── 설정 ─────────────────────────────────────────────────────────────────────

/// DiskMonitor 설정
#[derive(Debug, Clone)]
pub struct DiskMonitorConfig {
    /// 폴링 주기 (기본: 5초)
    pub poll_interval:           Duration,
    /// Read-Only 진입 임계값 (기본: 95%)
    pub disk_full_threshold:     f64,
    /// 복구 임계값 (기본: 85%)
    pub disk_recovery_threshold: f64,
    /// 노드 식별자
    pub node_id:                 String,
    /// 감시할 경로 목록 (각 경로별 독립 감시)
    pub watch_paths:             Vec<PathBuf>,
}

impl Default for DiskMonitorConfig {
    fn default() -> Self {
        Self {
            poll_interval:           Duration::from_secs(5),
            disk_full_threshold:     0.95,
            disk_recovery_threshold: 0.85,
            node_id:                 "sn-1".to_string(),
            watch_paths:             vec![PathBuf::from("/data")],
        }
    }
}

// ─── DiskProbe 트레이트 ───────────────────────────────────────────────────────

/// 디스크 사용량 조회 추상화
/// - 실제 환경: SystemDiskProbe (statvfs/GetDiskFreeSpaceEx)
/// - 테스트: MockDiskProbe
#[async_trait]
pub trait DiskProbe: Send + Sync + 'static {
    /// 경로의 디스크 사용률 반환 (0.0~1.0)
    async fn usage_ratio(&self, path: &std::path::Path) -> Result<f64>;
}

// ─── SystemDiskProbe ──────────────────────────────────────────────────────────

/// 실제 시스템 디스크 사용량 조회 (플랫폼별 네이티브 API)
pub struct SystemDiskProbe;

#[async_trait]
impl DiskProbe for SystemDiskProbe {
    async fn usage_ratio(&self, path: &std::path::Path) -> Result<f64> {
        system_disk_usage(path)
    }
}

/// 플랫폼별 디스크 사용량 조회
#[cfg(windows)]
fn system_disk_usage(path: &std::path::Path) -> Result<f64> {
    use std::os::windows::ffi::OsStrExt;

    // GetDiskFreeSpaceExW via kernel32 (no winapi crate needed)
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
        return Err(anyhow::anyhow!(
            "GetDiskFreeSpaceExW 실패: path={:?}",
            path
        ));
    }
    if total == 0 {
        return Ok(0.0);
    }

    let used  = total.saturating_sub(caller_free);
    Ok(used as f64 / total as f64)
}

#[cfg(unix)]
fn system_disk_usage(path: &std::path::Path) -> Result<f64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| anyhow::anyhow!("CString 변환 실패: {}", e))?;

    // libc::statvfs — 주요 Linux/macOS 플랫폼 지원
    let stat = unsafe {
        let mut st: libc::statvfs = std::mem::zeroed();
        let ret = libc::statvfs(c_path.as_ptr(), &mut st);
        if ret != 0 {
            return Err(anyhow::anyhow!("statvfs 실패: path={:?}", path));
        }
        st
    };

    if stat.f_blocks == 0 {
        return Ok(0.0);
    }
    let total = stat.f_blocks as u64 * stat.f_frsize as u64;
    let free  = stat.f_bavail as u64 * stat.f_frsize as u64;
    let used  = total.saturating_sub(free);
    Ok(used as f64 / total as f64)
}

#[cfg(not(any(windows, unix)))]
fn system_disk_usage(_path: &std::path::Path) -> Result<f64> {
    // 미지원 플랫폼: 항상 0% 반환 (안전 기본값)
    Ok(0.0)
}

// ─── QnCapacityReporter 트레이트 ──────────────────────────────────────────────

/// QN Leader에 디스크 용량 이벤트 보고 추상화
/// - 실제: GrpcQnCapacityReporter (MetaService.ReportDiskFull/Recovered gRPC)
/// - 테스트: MockQnCapacityReporter
#[async_trait]
pub trait QnCapacityReporter: Send + Sync + 'static {
    /// 디스크 임계값 초과 보고 (QN Leader에 Read-Only 진입 요청)
    async fn report_disk_full(
        &self,
        node_id: &str,
        path:    &str,
        usage:   f64,
    ) -> Result<()>;

    /// 디스크 복구 보고 (QN Leader에 Read-Only 해제 요청)
    async fn report_disk_recovered(
        &self,
        node_id: &str,
        path:    &str,
        usage:   f64,
    ) -> Result<()>;
}

/// gRPC QnCapacityReporter 스텁 (proto 재생성 후 구현 예정)
pub struct GrpcQnCapacityReporter {
    pub qn_endpoint: String,
}

#[async_trait]
impl QnCapacityReporter for GrpcQnCapacityReporter {
    async fn report_disk_full(&self, node_id: &str, path: &str, usage: f64) -> Result<()> {
        // TODO: MetaServiceClient::report_disk_full() gRPC 호출
        warn!(
            qn = %self.qn_endpoint,
            node_id,
            path,
            usage,
            "GrpcQnCapacityReporter::report_disk_full — proto 재생성 필요 (stub)"
        );
        Ok(())
    }

    async fn report_disk_recovered(&self, node_id: &str, path: &str, usage: f64) -> Result<()> {
        warn!(
            qn = %self.qn_endpoint,
            node_id,
            path,
            usage,
            "GrpcQnCapacityReporter::report_disk_recovered — proto 재생성 필요 (stub)"
        );
        Ok(())
    }
}

// ─── PathState ────────────────────────────────────────────────────────────────

/// 각 감시 경로의 상태
#[derive(Debug, Clone, PartialEq, Eq)]
enum PathState {
    /// 정상 (disk_full 보고 없음)
    Normal,
    /// Read-Only 보고됨 (disk_full 초과 상태)
    ReadOnly,
}

// ─── DiskMonitor ──────────────────────────────────────────────────────────────

/// 디스크 용량 감시 서비스
pub struct DiskMonitor<P: DiskProbe, R: QnCapacityReporter> {
    config:   DiskMonitorConfig,
    probe:    Arc<P>,
    reporter: Arc<R>,
    /// 경로별 현재 상태 추적 (중복 보고 방지)
    states:   Arc<Mutex<Vec<(PathBuf, PathState)>>>,
}

impl<P: DiskProbe, R: QnCapacityReporter> DiskMonitor<P, R> {
    pub fn new(config: DiskMonitorConfig, probe: P, reporter: R) -> Self {
        let states = config.watch_paths.iter()
            .map(|p| (p.clone(), PathState::Normal))
            .collect();
        Self {
            config,
            probe:    Arc::new(probe),
            reporter: Arc::new(reporter),
            states:   Arc::new(Mutex::new(states)),
        }
    }

    /// 백그라운드 폴링 루프 시작 (tokio::spawn으로 호출)
    pub async fn run(self: Arc<Self>) {
        let mut interval = tokio::time::interval(self.config.poll_interval);
        loop {
            interval.tick().await;
            if let Err(e) = self.poll_once().await {
                warn!(err = %e, "DiskMonitor 폴링 오류");
            }
        }
    }

    /// 단일 폴링 사이클 — 각 경로의 디스크 사용량 확인 후 임계값 비교
    pub async fn poll_once(&self) -> Result<()> {
        let paths: Vec<PathBuf> = self.config.watch_paths.clone();

        for path in &paths {
            let usage = match self.probe.usage_ratio(path).await {
                Ok(u)  => u,
                Err(e) => {
                    warn!(path = ?path, err = %e, "디스크 사용량 조회 실패 — 스킵");
                    continue;
                }
            };

            let mut states = self.states.lock().await;
            let state = states.iter_mut()
                .find(|(p, _)| p == path)
                .map(|(_, s)| s);

            if let Some(state) = state {
                match state {
                    PathState::Normal if usage > self.config.disk_full_threshold => {
                        *state = PathState::ReadOnly;
                        drop(states);
                        info!(
                            node_id = %self.config.node_id,
                            path    = ?path,
                            usage   = usage,
                            "디스크 임계값 초과 — QN에 DiskFull 보고"
                        );
                        self.reporter.report_disk_full(
                            &self.config.node_id,
                            &path.to_string_lossy(),
                            usage,
                        ).await?;
                    }
                    PathState::ReadOnly if usage <= self.config.disk_recovery_threshold => {
                        *state = PathState::Normal;
                        drop(states);
                        info!(
                            node_id = %self.config.node_id,
                            path    = ?path,
                            usage   = usage,
                            "디스크 복구 — QN에 DiskRecovered 보고"
                        );
                        self.reporter.report_disk_recovered(
                            &self.config.node_id,
                            &path.to_string_lossy(),
                            usage,
                        ).await?;
                    }
                    _ => {
                        // 상태 변화 없음
                    }
                }
            }
        }

        Ok(())
    }
}

/// DiskMonitor 팩토리 (기본 설정용)
pub fn create_disk_monitor(
    node_id:    String,
    watch_paths: Vec<PathBuf>,
    qn_endpoint: String,
) -> DiskMonitor<SystemDiskProbe, GrpcQnCapacityReporter> {
    let config = DiskMonitorConfig {
        node_id,
        watch_paths,
        ..Default::default()
    };
    DiskMonitor::new(
        config,
        SystemDiskProbe,
        GrpcQnCapacityReporter { qn_endpoint },
    )
}

// ─── 단위 테스트 ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    // ── MockDiskProbe ────────────────────────────────────────────────────────

    struct MockDiskProbe {
        usage: std::sync::Mutex<f64>,
    }

    impl MockDiskProbe {
        fn new(initial_usage: f64) -> Self {
            Self { usage: std::sync::Mutex::new(initial_usage) }
        }
        fn set_usage(&self, u: f64) {
            *self.usage.lock().unwrap() = u;
        }
    }

    #[async_trait]
    impl DiskProbe for MockDiskProbe {
        async fn usage_ratio(&self, _path: &std::path::Path) -> Result<f64> {
            Ok(*self.usage.lock().unwrap())
        }
    }

    // ── MockQnCapacityReporter ───────────────────────────────────────────────

    struct MockQnCapacityReporter {
        full_count:      Arc<AtomicU32>,
        recovered_count: Arc<AtomicU32>,
    }

    impl MockQnCapacityReporter {
        fn new() -> Self {
            Self {
                full_count:      Arc::new(AtomicU32::new(0)),
                recovered_count: Arc::new(AtomicU32::new(0)),
            }
        }
    }

    #[async_trait]
    impl QnCapacityReporter for MockQnCapacityReporter {
        async fn report_disk_full(&self, _: &str, _: &str, _: f64) -> Result<()> {
            self.full_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn report_disk_recovered(&self, _: &str, _: &str, _: f64) -> Result<()> {
            self.recovered_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn make_monitor(
        usage: f64,
    ) -> (
        DiskMonitor<MockDiskProbe, MockQnCapacityReporter>,
        Arc<MockDiskProbe>,
        Arc<MockQnCapacityReporter>,
    ) {
        let probe    = Arc::new(MockDiskProbe::new(usage));
        let reporter = Arc::new(MockQnCapacityReporter::new());
        let config   = DiskMonitorConfig {
            watch_paths: vec![PathBuf::from("/data")],
            ..Default::default()
        };
        // 직접 공유 Arc로 monitor 생성을 위한 별도 경로
        let monitor = DiskMonitor {
            config:   config.clone(),
            probe:    probe.clone(),
            reporter: reporter.clone(),
            states:   Arc::new(Mutex::new(vec![(PathBuf::from("/data"), PathState::Normal)])),
        };
        (monitor, probe, reporter)
    }

    #[tokio::test]
    async fn test_normal_usage_no_report() {
        let (monitor, _, reporter) = make_monitor(0.80);
        monitor.poll_once().await.unwrap();
        assert_eq!(reporter.full_count.load(Ordering::SeqCst), 0, "80% 사용량은 보고 없어야 함");
        assert_eq!(reporter.recovered_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_above_threshold_reports_full() {
        let (monitor, _, reporter) = make_monitor(0.97);
        monitor.poll_once().await.unwrap();
        assert_eq!(reporter.full_count.load(Ordering::SeqCst), 1, "97% 사용량은 disk_full 보고 1회");
    }

    #[tokio::test]
    async fn test_no_duplicate_full_report() {
        let (monitor, _, reporter) = make_monitor(0.97);
        // 두 번 폴링해도 최초 1회만 보고
        monitor.poll_once().await.unwrap();
        monitor.poll_once().await.unwrap();
        assert_eq!(reporter.full_count.load(Ordering::SeqCst), 1, "중복 full 보고 금지");
    }

    #[tokio::test]
    async fn test_recovery_below_threshold_reports_recovered() {
        let (monitor, probe, reporter) = make_monitor(0.97);
        // 먼저 full 상태로 전환
        monitor.poll_once().await.unwrap();
        assert_eq!(reporter.full_count.load(Ordering::SeqCst), 1);

        // 85% 이하로 복구
        probe.set_usage(0.80);
        monitor.poll_once().await.unwrap();
        assert_eq!(reporter.recovered_count.load(Ordering::SeqCst), 1, "복구 후 recovered 보고 1회");
    }

    #[tokio::test]
    async fn test_no_recovery_report_from_normal_state() {
        let (monitor, probe, reporter) = make_monitor(0.50);
        probe.set_usage(0.50);
        monitor.poll_once().await.unwrap();
        assert_eq!(reporter.recovered_count.load(Ordering::SeqCst), 0, "normal 상태에서 복구 보고 없어야 함");
    }
}
