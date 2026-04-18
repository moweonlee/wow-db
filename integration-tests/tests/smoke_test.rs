// Smoke test: docker run mysql:8.0 mysql 로 QN에 직접 접속
//
// 실행 방법:
//   # 클러스터 기동 후
//   cargo test -p integration-tests --features integration --test smoke_test
//
// 전제조건:
//   - Docker Desktop 실행 중
//   - QN MySQL port 9030, Web UI 8080 에서 수신 중

#[cfg(not(feature = "integration"))]
#[test]
#[ignore = "requires running WOW-DB cluster"]
fn smoke_placeholder() {}

#[cfg(feature = "integration")]
mod smoke {
    use integration_tests::TestConfig;
    use std::process::Command;
    use std::time::Duration;

    // Docker MySQL 클라이언트로 SQL 실행 헬퍼
    fn mysql(sql: &str) -> std::process::Output {
        Command::new("docker")
            .args([
                "run", "--rm",
                "mysql:8.0",
                "mysql",
                "-h", "host.docker.internal",
                "-P", "9030",
                "-u", "admin",
                "--password=",
                "--connect-timeout=5",
                "-e", sql,
            ])
            .output()
            .expect("docker command failed")
    }

    fn mysql_val(sql: &str) -> String {
        let out = mysql(sql);
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter(|l| !l.is_empty() && !l.contains("Warning"))
            .last()
            .unwrap_or("")
            .trim()
            .to_string()
    }

    fn mysql_ok(sql: &str) -> bool {
        mysql(sql).status.success()
    }

    // ── 1. 클러스터 헬스체크 ──────────────────────────────────────────────────

    #[test]
    fn step1_cluster_health() {
        let config = TestConfig::default();
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        let resp = client
            .get(format!("{}/healthz", config.qn_web_url))
            .send()
            .expect("Health check request failed");

        assert_eq!(resp.status(), 200, "QN /healthz should return 200");
        let body: serde_json::Value = resp.json().unwrap();
        assert_eq!(body["status"], "ok");
    }

    // ── 2. MySQL 연결 (Docker) ────────────────────────────────────────────────

    #[test]
    fn step2_mysql_connect() {
        let out = mysql("SELECT 1");
        assert!(out.status.success(), "MySQL connection via Docker should succeed");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains('1'), "SELECT 1 should return 1");
    }

    // ── 3. CREATE CUBE ────────────────────────────────────────────────────────

    #[test]
    fn step3_create_cube() {
        mysql_ok("DROP CUBE IF EXISTS smoke_page_events");

        let ddl = "CREATE CUBE IF NOT EXISTS smoke_page_events \
            (event_time DATETIME NOT NULL, user_id VARCHAR(36) NOT NULL, \
             event_name VARCHAR(64) NOT NULL, properties JSON) \
            PARTITION BY RANGE(event_time) (PARTITION AUTO GRANULARITY DAY) \
            DISTRIBUTED BY HASH(user_id) BUCKETS 8 \
            ORDER BY (user_id, event_time)";

        assert!(mysql_ok(ddl), "CREATE CUBE should succeed");
    }

    // ── 4. INSERT 이벤트 데이터 ───────────────────────────────────────────────

    #[test]
    fn step4_insert_events() {
        // step3 의존 — CUBE 없으면 MEM_STORE에 저장됨
        let rows = [
            "('2024-06-01 10:00:00','user-001','page_view','{}')",
            "('2024-06-01 10:01:00','user-001','click','{}')",
            "('2024-06-01 10:02:00','user-001','purchase','{}')",
            "('2024-06-01 11:00:00','user-002','page_view','{}')",
            "('2024-06-01 11:05:00','user-002','click','{}')",
            "('2024-06-01 12:00:00','user-003','page_view','{}')",
        ];
        let sql = format!(
            "INSERT INTO smoke_page_events VALUES {}",
            rows.join(",")
        );
        assert!(mysql_ok(&sql), "INSERT should succeed");
    }

    // ── 5. SELECT COUNT ───────────────────────────────────────────────────────

    #[test]
    fn step5_select_count() {
        let val = mysql_val("SELECT COUNT(*) FROM smoke_page_events");
        let count: i64 = val.parse().unwrap_or(0);
        assert!(count >= 6, "Expected >= 6 rows, got {count}");
    }

    // ── 6. SHOW TABLES ────────────────────────────────────────────────────────

    #[test]
    fn step6_show_tables() {
        let out = mysql("SHOW TABLES");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            stdout.contains("smoke_page_events"),
            "smoke_page_events should appear in SHOW TABLES. got: {stdout}"
        );
    }

    // ── 7. FUNNEL 쿼리 ────────────────────────────────────────────────────────

    #[test]
    fn step7_funnel_query() {
        let sql = "SELECT FUNNEL_COUNT(user_id,event_time,'7d',\
            'event_name = ''page_view''',\
            'event_name = ''click''',\
            'event_name = ''purchase''')\
            FROM smoke_page_events\
            WHERE event_time >= '2024-06-01' AND event_time < '2024-06-02'";
        // 오류 없이 반환되면 OK (stub 응답 포함)
        let out = mysql(sql);
        assert!(
            out.status.success(),
            "FUNNEL_COUNT query should not error. stderr={}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // ── 8. Prometheus 메트릭 ──────────────────────────────────────────────────

    #[test]
    fn step8_prometheus_metrics() {
        let config = TestConfig::default();
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        let resp = client
            .get(format!("{}/metrics", config.qn_web_url))
            .send()
            .expect("Prometheus /metrics request failed");

        assert_eq!(resp.status(), 200);
        let text = resp.text().unwrap();
        assert!(text.contains("wowdb_queries_total"));
    }

    // ── 9. Cluster Overview API ───────────────────────────────────────────────

    #[test]
    fn step9_cluster_overview() {
        let config = TestConfig::default();
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        let resp = client
            .get(format!("{}/api/v1/cluster", config.qn_web_url))
            .send()
            .expect("Cluster overview request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().unwrap();
        assert!(body["query_nodes"].is_array());
        assert!(!body["query_nodes"].as_array().unwrap().is_empty());
    }

    // ── 정리 ─────────────────────────────────────────────────────────────────

    #[test]
    fn step_cleanup() {
        mysql_ok("DROP CUBE IF EXISTS smoke_page_events");
    }
}
