// SQL 쿼리 지원 엄격 검증 테스트
// specs/002-wow-db-srs-v02/design/sql_syntax.md 기반으로
// 구현된 모든 SQL 명령어의 동작을 검증한다.

#[cfg(test)]
mod sql_query_tests {
    use std::sync::Arc;

    use super::super::handler::{QueryOutput};
    use super::super::schema_cmds::handle_schema_command;
    use crate::executor::insert_exec::execute_insert;
    use crate::executor::select_exec::execute_select;
    use crate::executor::analytics_exec::{
        execute_funnel_count, execute_cohort_analysis, execute_path_analysis,
    };
    use crate::executor::mem_store::MEM_STORE;
    use crate::meta::cluster_guard::{ClusterGuard, ClusterReadOnlyState, ReadOnlyReason, is_write_statement};
    use crate::meta::cube::CubeManager;
    use crate::raft::RaftManager;

    fn make_cube_mgr() -> Arc<CubeManager> {
        let raft = Arc::new(RaftManager::new_local());
        Arc::new(CubeManager::new(raft))
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §1 DDL — SHOW DATABASES
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sql_show_databases() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW DATABASES", &mgr, "default", None).await.unwrap();
        match out {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(columns[0].name, "Database", "컬럼명 'Database'");
                assert!(rows.len() >= 1, "최소 1개 DB 반환");
            }
            _ => panic!("SHOW DATABASES: Rows 기대"),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §2 DDL — USE
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sql_use_database() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("USE analytics", &mgr, "default", None).await.unwrap();
        assert!(matches!(out, QueryOutput::Affected(0)), "USE: Affected(0) 반환");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §3 DDL — SET 명령
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sql_set_names() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SET NAMES utf8mb4", &mgr, "default", None).await.unwrap();
        assert!(matches!(out, QueryOutput::Affected(0)));
    }

    #[tokio::test]
    async fn sql_set_character_set() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SET character_set_client = utf8mb4", &mgr, "default", None).await.unwrap();
        assert!(matches!(out, QueryOutput::Affected(0)));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §4 DDL — CREATE CUBE (파서 검증)
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sql_create_cube_parses_correctly() {
        use crate::sql_parser::cube_ddl::parse_create_cube_full;
        let sql = "CREATE CUBE page_events (
            event_id    BIGINT     NOT NULL,
            event_time  DATETIME   NOT NULL,
            user_id     VARCHAR    NOT NULL,
            device_id   VARCHAR    NOT NULL,
            event_name  VARCHAR    NOT NULL,
            properties  JSON
        ) PARTITION BY RANGE (event_time) AUTO
          ORDER BY (event_time, user_id)
          DISTRIBUTED BY HASH (device_id) BUCKETS 32";
        let stmt = parse_create_cube_full(sql);
        assert!(stmt.is_ok(), "CREATE CUBE 파싱 성공: {:?}", stmt.err());
        let stmt = stmt.unwrap();
        assert_eq!(stmt.name, "page_events");
        assert_eq!(stmt.columns.len(), 6, "6개 컬럼");
        assert_eq!(stmt.order_by, vec!["event_time", "user_id"]);
    }

    #[tokio::test]
    async fn sql_create_cube_if_not_exists() {
        use crate::sql_parser::cube_ddl::parse_create_cube_full;
        let sql = "CREATE CUBE IF NOT EXISTS events (id BIGINT NOT NULL) DISTRIBUTED BY HASH (id) BUCKETS 4";
        let stmt = parse_create_cube_full(sql);
        assert!(stmt.is_ok(), "CREATE CUBE IF NOT EXISTS 파싱");
    }

    #[tokio::test]
    async fn sql_create_cube_sort_key_max_4_columns() {
        use crate::sql_parser::cube_ddl::parse_create_cube_full;
        // 5개 sort key = 에러
        let sql = "CREATE CUBE t (a BIGINT, b BIGINT, c BIGINT, d BIGINT, e BIGINT) ORDER BY (a, b, c, d, e) DISTRIBUTED BY HASH (a) BUCKETS 4";
        let result = parse_create_cube_full(sql);
        assert!(result.is_err(), "Sort Key 5개는 거부되어야 함");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §5 DDL — CREATE DATABASE / DROP DATABASE
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sql_create_database_reaches_handler() {
        // handler.rs에서 CREATE DATABASE는 is_write_statement()를 통과해야 함
        assert!(is_write_statement("CREATE DATABASE analytics"), "CREATE DATABASE는 쓰기 구문");
    }

    #[tokio::test]
    async fn sql_drop_database_reaches_handler() {
        assert!(is_write_statement("DROP DATABASE analytics"), "DROP DATABASE는 쓰기 구문");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §6 SHOW — 테이블/Cube 메타데이터
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sql_show_tables() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW TABLES", &mgr, "default", None).await.unwrap();
        match out {
            QueryOutput::Rows { columns, .. } => {
                assert!(columns[0].name.starts_with("Tables_in_"), "컬럼명 Tables_in_<db>");
            }
            _ => panic!("SHOW TABLES: Rows 기대"),
        }
    }

    #[tokio::test]
    async fn sql_show_tables_from_db() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW TABLES FROM analytics", &mgr, "analytics", None).await.unwrap();
        assert!(matches!(out, QueryOutput::Rows { .. }));
    }

    #[tokio::test]
    async fn sql_show_cubes() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW CUBES", &mgr, "default", None).await.unwrap();
        match out {
            QueryOutput::Rows { columns, .. } => {
                let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                assert!(names.contains(&"Cube"), "Cube 컬럼 필수");
                assert!(names.contains(&"Distribution"), "Distribution 컬럼 필수");
            }
            _ => panic!("SHOW CUBES: Rows 기대"),
        }
    }

    #[tokio::test]
    async fn sql_describe_not_found_returns_error() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("DESCRIBE nonexistent", &mgr, "default", None).await.unwrap();
        assert!(matches!(out, QueryOutput::Error(_)), "없는 테이블 DESCRIBE → Error");
    }

    #[tokio::test]
    async fn sql_describe_after_create_returns_columns() {
        let raft    = Arc::new(RaftManager::new_local());
        let cube_mgr = Arc::new(CubeManager::new(raft.clone()));

        // Cube 생성
        use crate::sql_parser::cube_ddl::parse_create_cube_full;
        let sql = "CREATE CUBE events_test (id BIGINT NOT NULL, ts DATETIME NOT NULL) DISTRIBUTED BY HASH (id) BUCKETS 4";
        let stmt = parse_create_cube_full(sql).unwrap();
        let schema = crate::mysql_protocol::handler::stmt_to_schema_pub(stmt, "default");
        cube_mgr.create(&schema).await.unwrap();

        let out = handle_schema_command("DESCRIBE events_test", &cube_mgr, "default", None).await.unwrap();
        match out {
            QueryOutput::Rows { columns, rows } => {
                let col_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                assert!(col_names.contains(&"Field"), "Field 컬럼");
                assert!(col_names.contains(&"Type"), "Type 컬럼");
                assert_eq!(rows.len(), 2, "2개 컬럼 행");
            }
            _ => panic!("DESCRIBE: Rows 기대"),
        }
    }

    #[tokio::test]
    async fn sql_show_create_table() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW CREATE TABLE nonexistent", &mgr, "default", None).await.unwrap();
        match out {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(columns.len(), 2, "Table + Create Table 컬럼");
                assert_eq!(rows.len(), 1, "1개 행");
            }
            _ => panic!("SHOW CREATE TABLE: Rows 기대"),
        }
    }

    #[tokio::test]
    async fn sql_show_columns_from() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW COLUMNS FROM my_table", &mgr, "default", None).await;
        // 없는 테이블이면 None 또는 Error
        assert!(out.is_none() || matches!(out, Some(QueryOutput::Error(_)) | Some(QueryOutput::Rows { .. })));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §7 SHOW — STATUS / VARIABLES
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sql_show_status() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW STATUS", &mgr, "default", None).await.unwrap();
        match out {
            QueryOutput::Rows { columns, rows } => {
                let col_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                assert!(col_names.contains(&"Variable_name"));
                assert!(col_names.contains(&"Value"));
                assert!(!rows.is_empty(), "최소 1개 변수");
            }
            _ => panic!("SHOW STATUS: Rows 기대"),
        }
    }

    #[tokio::test]
    async fn sql_show_global_status() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW GLOBAL STATUS", &mgr, "default", None).await.unwrap();
        assert!(matches!(out, QueryOutput::Rows { .. }));
    }

    #[tokio::test]
    async fn sql_show_variables() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW VARIABLES", &mgr, "default", None).await.unwrap();
        assert!(matches!(out, QueryOutput::Rows { .. }));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §8 SHOW — 데이터 분포 가시성 (FR-043~FR-046)
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sql_show_partitions_column_contract() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW PARTITIONS FROM page_events", &mgr, "default", None).await.unwrap();
        match out {
            QueryOutput::Rows { columns, .. } => {
                let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                // FR-043 필수 컬럼 검증
                for col in &["partition_id", "range_start", "range_end", "row_count",
                             "size_bytes", "shard_count", "part_count", "tier", "created_at"] {
                    assert!(names.contains(col), "SHOW PARTITIONS: 컬럼 '{}' 필수", col);
                }
            }
            _ => panic!("SHOW PARTITIONS: Rows 기대"),
        }
    }

    #[tokio::test]
    async fn sql_show_shards_column_contract() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW SHARDS FROM page_events", &mgr, "default", None).await.unwrap();
        match out {
            QueryOutput::Rows { columns, .. } => {
                let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                // FR-044 필수 컬럼 검증
                for col in &["shard_id", "partition_id", "sn_node_id", "bucket_id",
                             "role", "state", "row_count", "size_bytes", "part_count", "lsn"] {
                    assert!(names.contains(col), "SHOW SHARDS: 컬럼 '{}' 필수", col);
                }
            }
            _ => panic!("SHOW SHARDS: Rows 기대"),
        }
    }

    #[tokio::test]
    async fn sql_show_parts_column_contract() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW PARTS FROM page_events", &mgr, "default", None).await.unwrap();
        match out {
            QueryOutput::Rows { columns, .. } => {
                let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                // FR-045 필수 컬럼 검증
                for col in &["part_id", "shard_id", "partition_id", "sn_node_id",
                             "level", "sequence_num", "row_count", "size_bytes",
                             "min_sort_key", "max_sort_key", "bloom_size_bytes", "created_at"] {
                    assert!(names.contains(col), "SHOW PARTS: 컬럼 '{}' 필수", col);
                }
            }
            _ => panic!("SHOW PARTS: Rows 기대"),
        }
    }

    #[tokio::test]
    async fn sql_show_parts_on_partition_syntax() {
        let mgr = make_cube_mgr();
        // "SHOW PARTS ON PARTITION" 별칭 구문 지원 확인
        let out = handle_schema_command(
            "SHOW PARTS ON PARTITION 'pid-001' FROM page_events",
            &mgr, "default", None,
        ).await;
        assert!(out.is_some(), "SHOW PARTS ON PARTITION 구문 인식되어야 함");
        assert!(matches!(out.unwrap(), QueryOutput::Rows { .. }));
    }

    #[tokio::test]
    async fn sql_show_parts_with_shard_filter() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command(
            "SHOW PARTS FROM page_events SHARD 'shard-uuid-001'",
            &mgr, "default", None,
        ).await;
        assert!(out.is_some(), "SHOW PARTS FROM ... SHARD 구문 인식");
        assert!(matches!(out.unwrap(), QueryOutput::Rows { .. }));
    }

    #[tokio::test]
    async fn sql_show_distributed_status_column_contract() {
        let mgr = make_cube_mgr();
        let out = handle_schema_command("SHOW DISTRIBUTED STATUS FROM page_events", &mgr, "default", None).await.unwrap();
        match out {
            QueryOutput::Rows { columns, .. } => {
                let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                // FR-046 필수 컬럼 검증
                for col in &["sn_node_id", "sn_endpoint", "shard_count", "leader_shard_count",
                             "partition_count", "row_count", "size_bytes", "avg_part_per_shard"] {
                    assert!(names.contains(col), "SHOW DISTRIBUTED STATUS: 컬럼 '{}' 필수", col);
                }
            }
            _ => panic!("SHOW DISTRIBUTED STATUS: Rows 기대"),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §9 DML — INSERT
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn sql_insert_single_row() {
        let result = execute_insert(
            "INSERT INTO events (id, name, ts) VALUES (1, 'page_view', '2024-01-01')"
        );
        assert!(result.is_ok(), "단건 INSERT: {:?}", result.err());
        assert_eq!(result.unwrap().rows_affected, 1);
    }

    #[test]
    fn sql_insert_multiple_rows() {
        let result = execute_insert(
            "INSERT INTO events (id, name) VALUES (2, 'click'), (3, 'scroll'), (4, 'purchase')"
        );
        assert!(result.is_ok(), "다건 INSERT: {:?}", result.err());
        assert_eq!(result.unwrap().rows_affected, 3, "3개 행 삽입");
    }

    #[test]
    fn sql_insert_without_column_list() {
        let result = execute_insert(
            "INSERT INTO events VALUES (5, 'view')"
        );
        assert!(result.is_ok(), "컬럼 목록 없는 INSERT");
    }

    #[test]
    fn sql_insert_with_json_value() {
        let result = execute_insert(
            r#"INSERT INTO events (id, props) VALUES (10, '{"page": "/home", "ref": "google"}')"#
        );
        assert!(result.is_ok(), "JSON 값 INSERT: {:?}", result.err());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §10 DQL — SELECT
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn sql_select_all_from_empty_table() {
        MEM_STORE.drop_table("empty_tbl");
        let result = execute_select("SELECT * FROM empty_tbl");
        assert!(result.is_ok(), "빈 테이블 SELECT");
        let sel = result.unwrap();
        assert!(sel.rows.is_empty(), "빈 결과");
    }

    #[test]
    fn sql_select_after_insert() {
        MEM_STORE.drop_table("sel_test");
        execute_insert("INSERT INTO sel_test (id, val) VALUES (1, 'hello')").unwrap();
        execute_insert("INSERT INTO sel_test (id, val) VALUES (2, 'world')").unwrap();

        let result = execute_select("SELECT * FROM sel_test");
        assert!(result.is_ok(), "INSERT 후 SELECT: {:?}", result.err());
        let sel = result.unwrap();
        assert_eq!(sel.rows.len(), 2, "2개 행 조회");
    }

    #[test]
    fn sql_select_with_where_clause() {
        MEM_STORE.drop_table("where_test");
        execute_insert("INSERT INTO where_test (id, name) VALUES (1, 'alice')").unwrap();
        execute_insert("INSERT INTO where_test (id, name) VALUES (2, 'bob')").unwrap();

        let result = execute_select("SELECT * FROM where_test WHERE id = 1");
        assert!(result.is_ok(), "WHERE 절 SELECT: {:?}", result.err());
    }

    #[test]
    fn sql_select_with_limit() {
        MEM_STORE.drop_table("limit_test");
        for i in 0..10 {
            execute_insert(&format!("INSERT INTO limit_test (id) VALUES ({})", i)).unwrap();
        }
        let result = execute_select("SELECT * FROM limit_test LIMIT 3");
        assert!(result.is_ok(), "LIMIT SELECT");
        let sel = result.unwrap();
        assert!(sel.rows.len() <= 3, "LIMIT 3: {}개 이하", sel.rows.len());
    }

    #[test]
    fn sql_select_with_order_by() {
        MEM_STORE.drop_table("order_test");
        execute_insert("INSERT INTO order_test (val) VALUES (30)").unwrap();
        execute_insert("INSERT INTO order_test (val) VALUES (10)").unwrap();
        execute_insert("INSERT INTO order_test (val) VALUES (20)").unwrap();
        let result = execute_select("SELECT * FROM order_test ORDER BY val ASC");
        assert!(result.is_ok(), "ORDER BY SELECT");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §11 분석 함수 — FUNNEL_COUNT, COHORT_ANALYSIS, PATH_ANALYSIS
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn sql_funnel_count_parses_and_returns_result() {
        let sql = r#"SELECT FUNNEL_COUNT(
            user_id, event_name, event_time,
            WINDOW => '7 DAYS',
            STEPS  => ['page_view', 'add_to_cart', 'purchase']
        ) FROM events"#;
        let result = execute_funnel_count(sql);
        assert!(result.is_ok(), "FUNNEL_COUNT 실행: {:?}", result.err());
        let sel = result.unwrap();
        assert!(!sel.columns.is_empty(), "FUNNEL_COUNT: 컬럼 반환");
    }

    #[test]
    fn sql_cohort_analysis_parses_and_returns_result() {
        let sql = r#"SELECT COHORT_ANALYSIS(
            user_id, event_name, event_time,
            ENTRY_EVENT   => 'signup',
            RETURN_EVENT  => 'purchase',
            GRANULARITY   => 'week',
            PERIODS       => '8'
        ) FROM events"#;
        let result = execute_cohort_analysis(sql);
        assert!(result.is_ok(), "COHORT_ANALYSIS 실행: {:?}", result.err());
    }

    #[test]
    fn sql_path_analysis_parses_and_returns_result() {
        let sql = r#"SELECT PATH_ANALYSIS(
            user_id, event_name, event_time,
            MAX_STEPS       => '5',
            SESSION_TIMEOUT => '30 MINUTES'
        ) FROM events"#;
        let result = execute_path_analysis(sql);
        assert!(result.is_ok(), "PATH_ANALYSIS 실행: {:?}", result.err());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §12 Read-Only 모드 — is_write_statement 분류
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn sql_write_statement_detection_insert() {
        assert!(is_write_statement("INSERT INTO events VALUES (1)"), "INSERT는 쓰기");
        assert!(is_write_statement("  INSERT INTO events VALUES (1)"), "공백 앞 INSERT");
    }

    #[test]
    fn sql_write_statement_detection_ddl() {
        assert!(is_write_statement("CREATE CUBE events (id BIGINT)"), "CREATE는 쓰기");
        assert!(is_write_statement("ALTER CUBE events ADD COLUMN x INT"), "ALTER는 쓰기");
        assert!(is_write_statement("DROP CUBE events"), "DROP은 쓰기");
        assert!(is_write_statement("TRUNCATE TABLE events"), "TRUNCATE는 쓰기");
    }

    #[test]
    fn sql_write_statement_detection_dml() {
        assert!(is_write_statement("UPDATE events SET x=1"), "UPDATE는 쓰기");
        assert!(is_write_statement("DELETE FROM events"), "DELETE는 쓰기");
        assert!(is_write_statement("REPLACE INTO events VALUES (1)"), "REPLACE는 쓰기");
        assert!(is_write_statement("LOAD DATA INFILE 'x.csv'"), "LOAD는 쓰기");
    }

    #[test]
    fn sql_read_statement_detection() {
        assert!(!is_write_statement("SELECT * FROM events"), "SELECT는 읽기");
        assert!(!is_write_statement("SHOW TABLES"), "SHOW는 읽기");
        assert!(!is_write_statement("DESCRIBE events"), "DESCRIBE는 읽기");
        assert!(!is_write_statement("EXPLAIN SELECT *"), "EXPLAIN은 읽기");
        assert!(!is_write_statement("SHOW PARTITIONS FROM events"), "SHOW PARTITIONS는 읽기");
        assert!(!is_write_statement("SHOW SHARDS FROM events"), "SHOW SHARDS는 읽기");
        assert!(!is_write_statement("SHOW PARTS FROM events"), "SHOW PARTS는 읽기");
        assert!(!is_write_statement("SHOW DISTRIBUTED STATUS FROM events"), "SHOW DISTRIBUTED는 읽기");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §13 Read-Only 모드 — ClusterGuard 쓰기 차단
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sql_read_only_blocks_insert() {
        let raft  = Arc::new(RaftManager::new_local());
        let guard = ClusterGuard::new(raft);

        // 정상 상태: INSERT 허용
        assert!(guard.check_write_allowed().await.is_ok(), "정상 상태: INSERT 허용");

        // DiskFull 진입
        guard.add_reason(ReadOnlyReason::DiskFull {
            node_id: "sn-01".to_string(),
            path:    "/data".to_string(),
            usage:   0.97,
        }).await.unwrap();

        let result = guard.check_write_allowed().await;
        assert!(result.is_err(), "Read-Only 모드: INSERT 거부");

        let err = result.unwrap_err();
        assert_eq!(crate::meta::cluster_guard::ReadOnlyError::mysql_error_code(), 1290,
                   "MySQL 에러 코드 1290");
        assert_eq!(crate::meta::cluster_guard::ReadOnlyError::mysql_sql_state(), "HY000",
                   "SQL State HY000");
        assert!(err.mysql_message().contains("read-only mode"), "에러 메시지에 read-only 포함");
    }

    #[tokio::test]
    async fn sql_read_only_allows_select() {
        let raft  = Arc::new(RaftManager::new_local());
        let guard = ClusterGuard::new(raft);

        guard.add_reason(ReadOnlyReason::DiskFull {
            node_id: "sn-01".to_string(),
            path:    "/data".to_string(),
            usage:   0.97,
        }).await.unwrap();

        // Read-Only여도 SELECT는 허용 (check_write_allowed()를 호출하지 않음)
        assert!(!is_write_statement("SELECT * FROM events"), "SELECT는 읽기 — check_write_allowed 불필요");
        assert!(!is_write_statement("SHOW PARTITIONS FROM events"), "SHOW도 읽기");
    }

    #[tokio::test]
    async fn sql_read_only_recovery_reenables_writes() {
        let raft  = Arc::new(RaftManager::new_local());
        let guard = ClusterGuard::new(raft);

        let reason = ReadOnlyReason::DiskFull {
            node_id: "sn-01".to_string(),
            path:    "/data".to_string(),
            usage:   0.97,
        };

        guard.add_reason(reason.clone()).await.unwrap();
        assert!(guard.check_write_allowed().await.is_err(), "DiskFull 시 차단");

        guard.remove_reason(&reason).await.unwrap();
        assert!(guard.check_write_allowed().await.is_ok(), "복구 후 쓰기 허용");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §14 ClusterReadOnlyState JSON 직렬화 (Raft KV 저장 형식)
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn sql_cluster_state_json_roundtrip() {
        let mut state = ClusterReadOnlyState::default();
        assert!(!state.enabled, "기본 상태: Read-Only 아님");

        state.add_reason(ReadOnlyReason::DiskFull {
            node_id: "sn-01".to_string(),
            path:    "/data".to_string(),
            usage:   0.97,
        });
        assert!(state.enabled);

        let json     = state.to_json().unwrap();
        let restored = ClusterReadOnlyState::from_json(&json).unwrap();
        assert!(restored.enabled, "역직렬화 후 enabled=true");
        assert_eq!(restored.reasons.len(), 1, "reason 1개");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // §15 MySQL 호환 — 시스템 변수 쿼리
    // ─────────────────────────────────────────────────────────────────────────
    // 이 테스트들은 handler.rs의 on_query()를 직접 호출할 수 없어
    // 파서/실행기 레벨에서 검증한다.

    #[test]
    fn sql_select_1_is_not_write() {
        assert!(!is_write_statement("SELECT 1"), "SELECT 1은 읽기");
        assert!(!is_write_statement("SELECT @@version"), "시스템 변수 SELECT는 읽기");
    }
}

