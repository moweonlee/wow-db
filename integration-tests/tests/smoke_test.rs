// T106: quickstart.md 기반 로컬 검증 — docker compose up → CREATE CUBE → INSERT → FUNNEL 쿼리 전체 플로우
//
// 실행 방법:
//   docker compose up -d --wait
//   cargo test -p integration-tests --test smoke_test -- --ignored

#[cfg(not(feature = "integration"))]
#[tokio::test]
#[ignore = "requires running WOW-DB cluster (docker compose up --wait)"]
async fn smoke_full_flow() {
    // 클러스터 없이 실행 시 즉시 skip
}

#[cfg(feature = "integration")]
mod smoke {
    use integration_tests::TestConfig;
    use std::time::Duration;

    // ─── 1. 클러스터 헬스체크 ────────────────────────────────────────────────

    #[tokio::test]
    async fn step1_cluster_health() {
        let config = TestConfig::default();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        let resp = client
            .get(format!("{}/healthz", config.qn_web_url))
            .send()
            .await
            .expect("Health check request failed");

        assert_eq!(resp.status(), 200, "QN /healthz should return 200");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "ok", "Health check should return status:ok");
    }

    // ─── 2. MySQL 연결 ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn step2_mysql_connect() {
        let config  = TestConfig::default();
        let pool    = mysql_async::Pool::new(config.qn_mysql_url.as_str());
        let _conn   = pool.get_conn().await
            .expect("MySQL connection to QN should succeed");
        pool.disconnect().await.unwrap();
    }

    // ─── 3. CREATE CUBE ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn step3_create_cube() {
        let config = TestConfig::default();
        let pool   = mysql_async::Pool::new(config.qn_mysql_url.as_str());
        let mut conn = pool.get_conn().await.unwrap();

        use mysql_async::prelude::Queryable;
        conn.query_drop("DROP CUBE IF EXISTS smoke_page_events").await.unwrap_or(());
        conn.query_drop(
            "CREATE CUBE IF NOT EXISTS smoke_page_events (
                event_time  DATETIME      NOT NULL,
                user_id     VARCHAR(36)   NOT NULL,
                event_name  VARCHAR(64)   NOT NULL,
                properties  JSON
            )
            PARTITION BY RANGE(event_time) (PARTITION AUTO GRANULARITY DAY)
            DISTRIBUTED BY HASH(user_id) BUCKETS 8
            ORDER BY (user_id, event_time)"
        ).await.expect("CREATE CUBE should succeed");

        pool.disconnect().await.unwrap();
    }

    // ─── 4. INSERT 이벤트 데이터 ─────────────────────────────────────────────

    #[tokio::test]
    async fn step4_insert_events() {
        let config = TestConfig::default();
        let pool   = mysql_async::Pool::new(config.qn_mysql_url.as_str());
        let mut conn = pool.get_conn().await.unwrap();

        use mysql_async::prelude::Queryable;

        // 3명 × 3 이벤트씩 삽입
        let events = vec![
            ("2024-06-01 10:00:00", "user-001", "page_view",  r#"{"page":"/home"}"#),
            ("2024-06-01 10:01:00", "user-001", "click",      r#"{"button":"signup"}"#),
            ("2024-06-01 10:02:00", "user-001", "purchase",   r#"{"amount":9900}"#),
            ("2024-06-01 11:00:00", "user-002", "page_view",  r#"{"page":"/home"}"#),
            ("2024-06-01 11:05:00", "user-002", "click",      r#"{"button":"login"}"#),
            ("2024-06-01 12:00:00", "user-003", "page_view",  r#"{"page":"/pricing"}"#),
        ];

        for (ts, uid, evt, props) in &events {
            conn.query_drop(format!(
                "INSERT INTO smoke_page_events VALUES ('{}', '{}', '{}', '{}')",
                ts, uid, evt, props
            )).await.expect("INSERT should succeed");
        }

        pool.disconnect().await.unwrap();
    }

    // ─── 5. SELECT COUNT 확인 ────────────────────────────────────────────────

    #[tokio::test]
    async fn step5_select_count() {
        let config = TestConfig::default();
        let pool   = mysql_async::Pool::new(config.qn_mysql_url.as_str());
        let mut conn = pool.get_conn().await.unwrap();

        use mysql_async::prelude::Queryable;

        let count: Option<i64> = conn
            .query_first("SELECT COUNT(*) FROM smoke_page_events")
            .await
            .expect("SELECT COUNT should succeed");

        assert_eq!(count, Some(6), "Should have 6 rows");
        pool.disconnect().await.unwrap();
    }

    // ─── 6. SHOW TABLES 확인 ─────────────────────────────────────────────────

    #[tokio::test]
    async fn step6_show_tables() {
        let config = TestConfig::default();
        let pool   = mysql_async::Pool::new(config.qn_mysql_url.as_str());
        let mut conn = pool.get_conn().await.unwrap();

        use mysql_async::prelude::Queryable;

        let tables: Vec<String> = conn
            .query("SHOW TABLES")
            .await
            .expect("SHOW TABLES should succeed");

        assert!(
            tables.iter().any(|t| t.contains("smoke_page_events")),
            "smoke_page_events should be in SHOW TABLES result"
        );
        pool.disconnect().await.unwrap();
    }

    // ─── 7. FUNNEL 쿼리 (스텁 검증) ─────────────────────────────────────────

    #[tokio::test]
    async fn step7_funnel_query() {
        // Phase D에서 완전 구현. 현재는 stub 응답 확인
        let config = TestConfig::default();
        let pool   = mysql_async::Pool::new(config.qn_mysql_url.as_str());
        let mut conn = pool.get_conn().await.unwrap();

        use mysql_async::prelude::Queryable;

        // Stub 응답 기대: 오류 없이 결과셋 반환
        let _rows: Vec<mysql_async::Row> = conn
            .query(
                "SELECT FUNNEL_COUNT(
                    user_id,
                    event_time,
                    '7d',
                    'event_name = ''page_view''',
                    'event_name = ''click''',
                    'event_name = ''purchase'''
                ) FROM smoke_page_events
                WHERE event_time >= '2024-06-01' AND event_time < '2024-06-02'"
            )
            .await
            .expect("FUNNEL_COUNT query should not return a protocol error");

        pool.disconnect().await.unwrap();
    }

    // ─── 8. Prometheus 메트릭 확인 ──────────────────────────────────────────

    #[tokio::test]
    async fn step8_prometheus_metrics() {
        let config = TestConfig::default();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        let resp = client
            .get(format!("{}/metrics", config.qn_web_url))
            .send()
            .await
            .expect("Prometheus /metrics request failed");

        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        assert!(text.contains("wowdb_queries_total"), "/metrics should contain WOW-DB metrics");
    }

    // ─── 9. Cluster Overview API ────────────────────────────────────────────

    #[tokio::test]
    async fn step9_cluster_overview() {
        let config = TestConfig::default();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        let resp = client
            .get(format!("{}/api/v1/cluster", config.qn_web_url))
            .send()
            .await
            .expect("Cluster overview request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert!(body["query_nodes"].is_array());
        assert!(!body["query_nodes"].as_array().unwrap().is_empty());
    }
}
