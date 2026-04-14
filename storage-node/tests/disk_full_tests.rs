// T125: Read-Only 모드 통합 테스트 (FR-036)
// ① 95% 임계값 초과 시 Read-Only 상태 진입 검증
// ② 85% 이하 복구 시 정상 모드 자동 전환 검증
// ③ 복수 노드 DiskFull 시 모든 노드 복구 후 해제 검증
// ④ DiskMonitor poll_once 흐름 정합성 검증

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::Result;
use async_trait::async_trait;

use storage_node::disk_monitor::{
    DiskMonitor, DiskMonitorConfig, DiskProbe, QnCapacityReporter,
};

// ─── 테스트 인프라 ────────────────────────────────────────────────────────────

struct MockDiskProbe {
    usage: std::sync::Mutex<f64>,
}

impl MockDiskProbe {
    fn new(u: f64) -> Arc<Self> {
        Arc::new(Self { usage: std::sync::Mutex::new(u) })
    }
    fn set(&self, u: f64) {
        *self.usage.lock().unwrap() = u;
    }
}

#[async_trait]
impl DiskProbe for MockDiskProbe {
    async fn usage_ratio(&self, _path: &std::path::Path) -> Result<f64> {
        Ok(*self.usage.lock().unwrap())
    }
}

#[derive(Clone)]
struct MockReporter {
    full_calls:      Arc<AtomicU32>,
    recovered_calls: Arc<AtomicU32>,
    /// 가장 최근에 보고된 (node_id, path, usage) — 검증용
    last_full:       Arc<std::sync::Mutex<Option<(String, String, f64)>>>,
    last_recovered:  Arc<std::sync::Mutex<Option<(String, String, f64)>>>,
}

impl MockReporter {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            full_calls:      Arc::new(AtomicU32::new(0)),
            recovered_calls: Arc::new(AtomicU32::new(0)),
            last_full:       Arc::new(std::sync::Mutex::new(None)),
            last_recovered:  Arc::new(std::sync::Mutex::new(None)),
        })
    }
}

#[async_trait]
impl QnCapacityReporter for MockReporter {
    async fn report_disk_full(&self, node_id: &str, path: &str, usage: f64) -> Result<()> {
        self.full_calls.fetch_add(1, Ordering::SeqCst);
        *self.last_full.lock().unwrap() =
            Some((node_id.to_string(), path.to_string(), usage));
        Ok(())
    }

    async fn report_disk_recovered(&self, node_id: &str, path: &str, usage: f64) -> Result<()> {
        self.recovered_calls.fetch_add(1, Ordering::SeqCst);
        *self.last_recovered.lock().unwrap() =
            Some((node_id.to_string(), path.to_string(), usage));
        Ok(())
    }
}

// 재공유 가능한 Arc<MockDiskProbe>를 위한 래퍼
struct ArcProbeWrapper(Arc<MockDiskProbe>);

#[async_trait]
impl DiskProbe for ArcProbeWrapper {
    async fn usage_ratio(&self, path: &std::path::Path) -> Result<f64> {
        self.0.usage_ratio(path).await
    }
}

struct ArcReporterWrapper(Arc<MockReporter>);

#[async_trait]
impl QnCapacityReporter for ArcReporterWrapper {
    async fn report_disk_full(&self, node_id: &str, path: &str, usage: f64) -> Result<()> {
        self.0.report_disk_full(node_id, path, usage).await
    }
    async fn report_disk_recovered(&self, node_id: &str, path: &str, usage: f64) -> Result<()> {
        self.0.report_disk_recovered(node_id, path, usage).await
    }
}

fn make_monitor(
    node_id: &str,
    watch_paths: Vec<PathBuf>,
    probe: Arc<MockDiskProbe>,
    reporter: Arc<MockReporter>,
) -> DiskMonitor<ArcProbeWrapper, ArcReporterWrapper> {
    let config = DiskMonitorConfig {
        poll_interval:           std::time::Duration::from_secs(5),
        disk_full_threshold:     0.95,
        disk_recovery_threshold: 0.85,
        node_id:                 node_id.to_string(),
        watch_paths,
    };
    DiskMonitor::new(config, ArcProbeWrapper(probe), ArcReporterWrapper(reporter))
}

// ─── 테스트 1: 95% 초과 시 Read-Only 보고 ────────────────────────────────────

/// ① 95% 임계값 초과 시 report_disk_full 호출 검증
#[tokio::test]
async fn test_disk_full_at_95_percent_threshold() {
    let probe    = MockDiskProbe::new(0.96);  // 96% > 95% 임계값
    let reporter = MockReporter::new();
    let monitor  = make_monitor(
        "sn-01",
        vec![PathBuf::from("/data")],
        probe.clone(),
        reporter.clone(),
    );

    monitor.poll_once().await.unwrap();

    assert_eq!(
        reporter.full_calls.load(Ordering::SeqCst), 1,
        "96% 사용량 시 disk_full 보고 1회"
    );
    let last = reporter.last_full.lock().unwrap().clone().unwrap();
    assert_eq!(last.0, "sn-01", "보고된 node_id 일치");
    assert!((last.2 - 0.96).abs() < 1e-9, "보고된 usage 일치");
}

/// 95% 미만에서는 보고 없음
#[tokio::test]
async fn test_no_disk_full_below_threshold() {
    let probe    = MockDiskProbe::new(0.90);  // 90% < 95%
    let reporter = MockReporter::new();
    let monitor  = make_monitor(
        "sn-01",
        vec![PathBuf::from("/data")],
        probe.clone(),
        reporter.clone(),
    );

    monitor.poll_once().await.unwrap();

    assert_eq!(
        reporter.full_calls.load(Ordering::SeqCst), 0,
        "90% 사용량은 임계값 미만으로 보고 없어야 함"
    );
}

// ─── 테스트 2: 중복 보고 방지 ────────────────────────────────────────────────

/// disk_full 상태에서 반복 폴링해도 1회만 보고
#[tokio::test]
async fn test_no_duplicate_disk_full_reports() {
    let probe    = MockDiskProbe::new(0.97);
    let reporter = MockReporter::new();
    let monitor  = make_monitor(
        "sn-01",
        vec![PathBuf::from("/data")],
        probe.clone(),
        reporter.clone(),
    );

    // 3회 폴링
    monitor.poll_once().await.unwrap();
    monitor.poll_once().await.unwrap();
    monitor.poll_once().await.unwrap();

    assert_eq!(
        reporter.full_calls.load(Ordering::SeqCst), 1,
        "동일 Read-Only 상태 지속 시 중복 보고 없어야 함"
    );
}

// ─── 테스트 3: 85% 이하 복구 시 정상 모드 자동 전환 ─────────────────────────

/// ③ disk_full → 복구(80%) → report_disk_recovered 1회
#[tokio::test]
async fn test_recovery_below_85_percent() {
    let probe    = MockDiskProbe::new(0.97);
    let reporter = MockReporter::new();
    let monitor  = make_monitor(
        "sn-01",
        vec![PathBuf::from("/data")],
        probe.clone(),
        reporter.clone(),
    );

    // 임계값 초과 → Read-Only
    monitor.poll_once().await.unwrap();
    assert_eq!(reporter.full_calls.load(Ordering::SeqCst), 1);

    // 복구 (80% <= 85%)
    probe.set(0.80);
    monitor.poll_once().await.unwrap();
    assert_eq!(
        reporter.recovered_calls.load(Ordering::SeqCst), 1,
        "80%로 복구 시 report_disk_recovered 1회"
    );

    // 복구 후 정상 상태 — 추가 full 보고 없음
    monitor.poll_once().await.unwrap();
    assert_eq!(reporter.full_calls.load(Ordering::SeqCst), 1);
    assert_eq!(reporter.recovered_calls.load(Ordering::SeqCst), 1);
}

/// 85%~95% 구간은 히스테리시스 — full 상태에서 이 구간은 복구 보고 없음
#[tokio::test]
async fn test_hysteresis_between_85_and_95() {
    let probe    = MockDiskProbe::new(0.97);
    let reporter = MockReporter::new();
    let monitor  = make_monitor(
        "sn-01",
        vec![PathBuf::from("/data")],
        probe.clone(),
        reporter.clone(),
    );

    // full 상태 진입
    monitor.poll_once().await.unwrap();
    assert_eq!(reporter.full_calls.load(Ordering::SeqCst), 1);

    // 90%: 복구 임계값(85%) 초과 → 복구 보고 없음
    probe.set(0.90);
    monitor.poll_once().await.unwrap();
    assert_eq!(
        reporter.recovered_calls.load(Ordering::SeqCst), 0,
        "90%는 복구 임계값 85% 초과 — 복구 보고 없어야 함"
    );
}

// ─── 테스트 4: 복수 노드 DiskFull 시 각 노드 독립 처리 ──────────────────────

/// ④ 복수 경로 독립 모니터링
/// sn-01의 /data1 full + /data2 정상 → /data1만 보고
#[tokio::test]
async fn test_multi_path_independent_monitoring() {
    // 두 경로를 위한 probe (같은 probe를 사용하지만 실제 환경에서는 경로별)
    let probe1   = MockDiskProbe::new(0.97);  // /data1: 97% full
    let probe2   = MockDiskProbe::new(0.50);  // /data2: 50% 정상
    let reporter = MockReporter::new();

    // path1 monitor
    let mon1 = make_monitor(
        "sn-01",
        vec![PathBuf::from("/data1")],
        probe1.clone(),
        reporter.clone(),
    );
    // path2 monitor
    let mon2 = make_monitor(
        "sn-01",
        vec![PathBuf::from("/data2")],
        probe2.clone(),
        reporter.clone(),
    );

    mon1.poll_once().await.unwrap();
    mon2.poll_once().await.unwrap();

    // /data1만 full 보고
    assert_eq!(reporter.full_calls.load(Ordering::SeqCst), 1, "/data1만 full 보고");
    assert_eq!(reporter.recovered_calls.load(Ordering::SeqCst), 0);

    // 두 경로 모두 복구
    probe1.set(0.80);
    mon1.poll_once().await.unwrap();

    assert_eq!(reporter.recovered_calls.load(Ordering::SeqCst), 1, "/data1 복구 보고");
}

/// 복수 노드가 모두 full → 각각 독립적으로 보고
#[tokio::test]
async fn test_multi_node_both_full_then_recover() {
    let probe_n1 = MockDiskProbe::new(0.97);
    let probe_n2 = MockDiskProbe::new(0.96);
    let rep_n1   = MockReporter::new();
    let rep_n2   = MockReporter::new();

    let mon_n1 = make_monitor("sn-01", vec![PathBuf::from("/data")], probe_n1.clone(), rep_n1.clone());
    let mon_n2 = make_monitor("sn-02", vec![PathBuf::from("/data")], probe_n2.clone(), rep_n2.clone());

    // 두 노드 모두 full
    mon_n1.poll_once().await.unwrap();
    mon_n2.poll_once().await.unwrap();
    assert_eq!(rep_n1.full_calls.load(Ordering::SeqCst), 1, "sn-01 full 보고");
    assert_eq!(rep_n2.full_calls.load(Ordering::SeqCst), 1, "sn-02 full 보고");

    // sn-01만 먼저 복구 — sn-02는 여전히 full
    probe_n1.set(0.80);
    mon_n1.poll_once().await.unwrap();
    assert_eq!(rep_n1.recovered_calls.load(Ordering::SeqCst), 1, "sn-01 복구 보고");
    assert_eq!(rep_n2.recovered_calls.load(Ordering::SeqCst), 0, "sn-02 미복구");

    // sn-02도 복구
    probe_n2.set(0.70);
    mon_n2.poll_once().await.unwrap();
    assert_eq!(rep_n2.recovered_calls.load(Ordering::SeqCst), 1, "sn-02 복구 보고");
}
