// T091: Prometheus `/metrics` 엔드포인트
// 노드 상태, 쿼리 처리량, 수집 속도, 메모리/CPU 사용률 지표

use std::sync::LazyLock;

use prometheus::{
    Counter, Gauge, Histogram, HistogramOpts, IntCounter, IntGauge,
    Opts, Registry, TextEncoder, Encoder,
};

// ─── 메트릭 정의 ──────────────────────────────────────────────────────────────

pub struct WowDbMetrics {
    pub registry: Registry,

    // 쿼리 처리량
    pub queries_total:         IntCounter,
    pub queries_failed:        IntCounter,
    pub queries_in_flight:     IntGauge,
    pub query_duration_us:     Histogram,

    // 수집(Ingestion) 속도
    pub rows_ingested_total:   IntCounter,
    pub bytes_ingested_total:  IntCounter,
    pub kafka_lag:             IntGauge,

    // 스토리지
    pub tablets_total:         IntGauge,
    pub sstables_total:        IntGauge,
    pub compaction_running:    IntGauge,

    // 노드 상태
    pub raft_leader:           IntGauge,    // 1 = Leader
    pub raft_term:             IntGauge,
    pub compute_nodes_online:  IntGauge,
    pub data_nodes_online:     IntGauge,

    // 자원 사용
    pub memory_used_bytes:     Gauge,
    pub cpu_usage_pct:         Gauge,
}

impl WowDbMetrics {
    fn new() -> Self {
        let registry = Registry::new();

        macro_rules! reg {
            ($m:expr) => {{
                registry.register(Box::new($m.clone())).expect("metric register");
                $m
            }};
        }

        // ── 쿼리 ──────────────────────────────────────────────────────────────

        let queries_total = reg!(IntCounter::with_opts(
            Opts::new("wowdb_queries_total", "Total SQL queries received")
        ).unwrap());

        let queries_failed = reg!(IntCounter::with_opts(
            Opts::new("wowdb_queries_failed_total", "Total failed SQL queries")
        ).unwrap());

        let queries_in_flight = reg!(IntGauge::with_opts(
            Opts::new("wowdb_queries_in_flight", "Currently executing queries")
        ).unwrap());

        let query_duration_us = reg!(Histogram::with_opts(
            HistogramOpts::new("wowdb_query_duration_us", "Query execution time in microseconds")
                .buckets(vec![100.0, 500.0, 1_000.0, 5_000.0, 10_000.0, 50_000.0,
                              100_000.0, 500_000.0, 1_000_000.0, 5_000_000.0])
        ).unwrap());

        // ── 수집 ──────────────────────────────────────────────────────────────

        let rows_ingested_total = reg!(IntCounter::with_opts(
            Opts::new("wowdb_rows_ingested_total", "Total rows ingested")
        ).unwrap());

        let bytes_ingested_total = reg!(IntCounter::with_opts(
            Opts::new("wowdb_bytes_ingested_total", "Total bytes ingested")
        ).unwrap());

        let kafka_lag = reg!(IntGauge::with_opts(
            Opts::new("wowdb_kafka_consumer_lag", "Kafka consumer lag (messages behind)")
        ).unwrap());

        // ── 스토리지 ──────────────────────────────────────────────────────────

        let tablets_total = reg!(IntGauge::with_opts(
            Opts::new("wowdb_tablets_total", "Total tablets across all data nodes")
        ).unwrap());

        let sstables_total = reg!(IntGauge::with_opts(
            Opts::new("wowdb_sstables_total", "Total SSTable files")
        ).unwrap());

        let compaction_running = reg!(IntGauge::with_opts(
            Opts::new("wowdb_compaction_running", "Number of running compaction tasks")
        ).unwrap());

        // ── 노드 상태 ─────────────────────────────────────────────────────────

        let raft_leader = reg!(IntGauge::with_opts(
            Opts::new("wowdb_raft_is_leader", "1 if this QN is Raft leader")
        ).unwrap());

        let raft_term = reg!(IntGauge::with_opts(
            Opts::new("wowdb_raft_term", "Current Raft term number")
        ).unwrap());

        let compute_nodes_online = reg!(IntGauge::with_opts(
            Opts::new("wowdb_compute_nodes_online", "Online compute nodes count")
        ).unwrap());

        let data_nodes_online = reg!(IntGauge::with_opts(
            Opts::new("wowdb_data_nodes_online", "Online data nodes count")
        ).unwrap());

        // ── 자원 ──────────────────────────────────────────────────────────────

        let memory_used_bytes = reg!(Gauge::with_opts(
            Opts::new("wowdb_memory_used_bytes", "Process resident memory in bytes")
        ).unwrap());

        let cpu_usage_pct = reg!(Gauge::with_opts(
            Opts::new("wowdb_cpu_usage_pct", "Process CPU usage percent (0-100)")
        ).unwrap());

        Self {
            registry,
            queries_total,
            queries_failed,
            queries_in_flight,
            query_duration_us,
            rows_ingested_total,
            bytes_ingested_total,
            kafka_lag,
            tablets_total,
            sstables_total,
            compaction_running,
            raft_leader,
            raft_term,
            compute_nodes_online,
            data_nodes_online,
            memory_used_bytes,
            cpu_usage_pct,
        }
    }

    /// Prometheus 텍스트 포맷 직렬화
    pub fn render(&self) -> String {
        // 자원 사용률 갱신 (근사값)
        self.refresh_resource_metrics();

        let encoder = TextEncoder::new();
        let mf      = self.registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&mf, &mut buf).unwrap_or(());
        String::from_utf8(buf).unwrap_or_default()
    }

    /// 쿼리 시작 기록
    pub fn on_query_begin(&self) {
        self.queries_total.inc();
        self.queries_in_flight.inc();
    }

    /// 쿼리 완료 기록
    pub fn on_query_end(&self, elapsed_us: u64, failed: bool) {
        self.queries_in_flight.dec();
        self.query_duration_us.observe(elapsed_us as f64);
        if failed { self.queries_failed.inc(); }
    }

    /// 수집 행/바이트 기록
    pub fn on_ingest(&self, rows: u64, bytes: u64) {
        self.rows_ingested_total.inc_by(rows);
        self.bytes_ingested_total.inc_by(bytes);
    }

    fn refresh_resource_metrics(&self) {
        // 프로세스 메모리: /proc/self/status (Linux) 또는 근사값
        #[cfg(target_os = "linux")]
        {
            if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
                for line in status.lines() {
                    if line.starts_with("VmRSS:") {
                        let kb: f64 = line
                            .split_whitespace()
                            .nth(1)
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(0.0);
                        self.memory_used_bytes.set(kb * 1024.0);
                        break;
                    }
                }
            }
        }
    }
}

// ─── 글로벌 싱글턴 ────────────────────────────────────────────────────────────

pub static METRICS: LazyLock<WowDbMetrics> = LazyLock::new(WowDbMetrics::new);

/// Prometheus 텍스트 포맷 렌더링 — axum 핸들러에서 직접 사용
pub fn render_metrics() -> String { METRICS.render() }

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_render_not_empty() {
        let m = WowDbMetrics::new();
        m.on_query_begin();
        m.on_query_end(500, false);
        let text = m.render();
        assert!(text.contains("wowdb_queries_total"));
        assert!(text.contains("wowdb_query_duration_us"));
    }

    #[test]
    fn test_query_counters() {
        let m = WowDbMetrics::new();
        m.on_query_begin();
        m.on_query_begin();
        assert_eq!(m.queries_in_flight.get(), 2);
        m.on_query_end(100, false);
        assert_eq!(m.queries_in_flight.get(), 1);
        m.on_query_end(200, true);
        assert_eq!(m.queries_in_flight.get(), 0);
        assert_eq!(m.queries_failed.get(), 1);
    }

    #[test]
    fn test_ingest_counters() {
        let m = WowDbMetrics::new();
        m.on_ingest(1000, 65536);
        assert_eq!(m.rows_ingested_total.get(), 1000);
        assert_eq!(m.bytes_ingested_total.get(), 65536);
    }
}
