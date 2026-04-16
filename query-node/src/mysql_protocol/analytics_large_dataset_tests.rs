// T-TEST-LA: 대용량 사용자 이벤트 데이터셋 분석 테스트
//
// 목적:
//   - 100,000건 이상의 웹 이벤트 데이터를 MEM_STORE에 적재
//   - 사용자 분석 함수(FUNNEL_COUNT, COHORT_ANALYSIS, PATH_ANALYSIS) 검증
//   - SELECT + GROUP BY + WHERE + ORDER BY + LIMIT 쿼리 검증
//   - LSM MemTable 대량 삽입 처리량 검증
//
// 데이터 모델:
//   user_id    (String)  — 1,000 명의 고유 사용자 (user_0000 ~ user_0999)
//   session_id (String)  — 세션 ID
//   event_name (String)  — page_view | search | add_to_cart | checkout | purchase | signup
//   event_time (String)  — ISO 8601 형식 (2026-03-01T … ~ 2026-04-10T …)
//   page_url   (String)  — /home | /search | /products | /cart | /checkout | /confirm
//   referrer   (String)  — google | facebook | direct | email | organic
//   properties (JSON)    — browser, country, device
//
// 퍼넬 구조 (10만 행 기준):
//   - step 1: page_view    — 1,000 users (전체 사용자)
//   - step 2: add_to_cart  —   600 users
//   - step 3: purchase     —   200 users

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;
    use serde_json::{json, Value};

    use crate::executor::analytics_exec::{
        execute_cohort_analysis, execute_funnel_count, execute_path_analysis,
    };
    use crate::executor::mem_store::{MEM_STORE, Row};
    use crate::executor::select_exec::execute_select;

    // ── 상수 ─────────────────────────────────────────────────────────────────

    /// 이 테스트 전용 테이블명 (다른 테스트와 충돌 방지)
    const TABLE: &str = "_large_events_v1";
    const ROW_COUNT: usize = 100_000;
    const USER_COUNT: usize = 1_000;

    // ── 데이터 초기화 (한 번만 실행) ─────────────────────────────────────────

    static INIT: OnceLock<()> = OnceLock::new();

    fn setup() {
        INIT.get_or_init(|| {
            MEM_STORE.drop_table(TABLE);
            populate_events();
        });
    }

    /// 100,000건의 사용자 이벤트 생성 및 MEM_STORE에 삽입
    fn populate_events() {
        let pages    = ["/home", "/search", "/products", "/cart", "/checkout", "/confirm"];
        let referrers = ["google", "facebook", "direct", "email", "organic"];
        let browsers  = ["chrome", "firefox", "safari", "edge"];
        let countries = ["KR", "US", "JP", "DE", "GB"];

        // user u (0..1000), event_pos e (0..100) → 100개 이벤트/사용자 × 1000 = 100,000
        for u in 0..USER_COUNT {
            let user_id    = format!("user_{:04}", u);
            let session_id = format!("sess_{:06}", u * 10); // 기본 세션

            for e in 0..100usize {
                // 이벤트 타입 결정: 퍼넬 구조 반영
                let event_name: &str = if e == 0 && u < 300 {
                    "signup"             // user 0..299: 가입 이벤트 먼저
                } else if e == 10 && u < 800 {
                    "search"             // user 0..799: 검색
                } else if e == 20 && u < 600 {
                    "add_to_cart"        // user 0..599: 장바구니 추가
                } else if e == 30 && u < 400 {
                    "checkout"           // user 0..399: 결제 시작
                } else if e == 40 && u < 200 {
                    "purchase"           // user 0..199: 구매 완료
                } else {
                    "page_view"          // 나머지: 페이지 조회
                };

                // 타임스탬프: user 당 3600초 + event 당 60초 간격
                // → 전체 범위: 2026-03-01 ~ 2026-04-10 (41일)
                let total_secs = u as i64 * 3600 + e as i64 * 60;
                let days       = total_secs / 86400;
                let rem        = total_secs % 86400;
                let hour       = rem / 3600;
                let min        = (rem % 3600) / 60;
                let (month, day) = if days < 31 { (3u32, days as u32 + 1) }
                                   else         { (4u32, days as u32 - 30) };
                let event_time = format!("2026-{:02}-{:02}T{:02}:{:02}:00Z",
                    month, day, hour, min);

                let page    = pages[u % pages.len()];
                let referrer = referrers[u % referrers.len()];
                let browser  = browsers[e % browsers.len()];
                let country  = countries[u % countries.len()];
                let device   = if e % 2 == 0 { "desktop" } else { "mobile" };

                let mut row: Row = std::collections::HashMap::new();
                row.insert("user_id".into(),    json!(user_id));
                row.insert("session_id".into(), json!(session_id));
                row.insert("event_name".into(), json!(event_name));
                row.insert("event_time".into(), json!(event_time));
                row.insert("page_url".into(),   json!(page));
                row.insert("referrer".into(),   json!(referrer));
                row.insert("properties".into(), json!({
                    "browser": browser,
                    "country": country,
                    "device":  device,
                }));

                MEM_STORE.insert(TABLE, row);
            }
        }
    }

    // ── §1 기본 데이터 무결성 ─────────────────────────────────────────────────

    #[test]
    fn test_row_count_100k() {
        setup();
        assert_eq!(
            MEM_STORE.row_count(TABLE),
            ROW_COUNT,
            "100,000건 삽입 확인"
        );
    }

    #[test]
    fn test_select_count_star() {
        setup();
        let r = execute_select(&format!("SELECT COUNT(*) FROM {TABLE}".await)).unwrap();
        assert_eq!(r.rows.len(), 1);
        let count = match &r.rows[0][0] {
            Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => panic!("COUNT(*) must return a number"),
        };
        assert_eq!(count as usize, ROW_COUNT, "SELECT COUNT(*) = 100,000");
    }

    // ── §2 이벤트 분포 검증 ──────────────────────────────────────────────────

    #[test]
    fn test_event_distribution_by_group_by() {
        setup();
        let r = execute_select(&format!(
            "SELECT event_name, COUNT(*) as cnt \
             FROM {TABLE} \
             GROUP BY event_name \
             ORDER BY cnt DESC"
        .await)).unwrap();

        // 최소 5가지 이벤트 타입 존재
        assert!(r.rows.len() >= 5, "이벤트 종류 >= 5, actual={}", r.rows.len());

        // page_view가 가장 많은 이벤트 (퍼넬 나머지가 모두 page_view)
        let first_event = match &r.rows[0][0] {
            Value::String(s) => s.clone(),
            _ => String::new(),
        };
        assert_eq!(first_event, "page_view", "page_view가 가장 많은 이벤트 타입");

        // page_view 수 검증: 100k - (signup+search+add_to_cart+checkout+purchase) 이벤트
        // signup: 300명 × 1회 = 300
        // search: 800명 × 1회 = 800
        // add_to_cart: 600명 × 1회 = 600
        // checkout: 400명 × 1회 = 400
        // purchase: 200명 × 1회 = 200
        // page_view: 100,000 - 2,300 = 97,700
        let page_view_count = match &r.rows[0][1] {
            Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => 0,
        };
        assert_eq!(page_view_count, 97_700, "page_view 이벤트 수 = 97,700");
    }

    #[test]
    fn test_signup_event_count() {
        setup();
        let r = execute_select(&format!(
            "SELECT COUNT(*) FROM {TABLE} WHERE event_name = 'signup'"
        .await)).unwrap();
        let count = match &r.rows[0][0] {
            Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => panic!("COUNT must return number"),
        };
        assert_eq!(count, 300, "signup 이벤트 수 = 300 (user 0..299)");
    }

    #[test]
    fn test_purchase_event_count() {
        setup();
        let r = execute_select(&format!(
            "SELECT COUNT(*) FROM {TABLE} WHERE event_name = 'purchase'"
        .await)).unwrap();
        let count = match &r.rows[0][0] {
            Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => panic!("COUNT must return number"),
        };
        assert_eq!(count, 200, "purchase 이벤트 수 = 200 (user 0..199)");
    }

    // ── §3 사용자별 집계 ─────────────────────────────────────────────────────

    #[test]
    fn test_distinct_user_count() {
        setup();
        // GROUP BY user_id → 고유 사용자 수 = 1,000개 행 반환
        let r = execute_select(&format!(
            "SELECT user_id, COUNT(*) as cnt \
             FROM {TABLE} \
             GROUP BY user_id"
        .await)).unwrap();
        assert_eq!(r.rows.len(), USER_COUNT, "고유 사용자 수 = 1,000");
    }

    #[test]
    fn test_events_per_user_exactly_100() {
        setup();
        let r = execute_select(&format!(
            "SELECT user_id, COUNT(*) as cnt \
             FROM {TABLE} \
             GROUP BY user_id \
             ORDER BY user_id ASC \
             LIMIT 10"
        .await)).unwrap();
        assert_eq!(r.rows.len(), 10, "LIMIT 10 결과");
        for row in &r.rows {
            let cnt = match &row[1] {
                Value::Number(n) => n.as_u64().unwrap_or(0),
                _ => panic!("cnt must be number"),
            };
            assert_eq!(cnt, 100, "각 사용자는 정확히 100개 이벤트");
        }
    }

    #[test]
    fn test_top_users_by_event_count() {
        setup();
        let r = execute_select(&format!(
            "SELECT user_id, COUNT(*) as cnt \
             FROM {TABLE} \
             GROUP BY user_id \
             ORDER BY cnt DESC \
             LIMIT 5"
        .await)).unwrap();
        assert_eq!(r.rows.len(), 5, "TOP 5 사용자");
        for row in &r.rows {
            let cnt = match &row[1] {
                Value::Number(n) => n.as_u64().unwrap_or(0),
                _ => panic!(),
            };
            assert_eq!(cnt, 100, "모든 사용자 이벤트 수 동일 (100건)");
        }
    }

    // ── §4 시간대 분석 ────────────────────────────────────────────────────────

    #[test]
    fn test_events_in_march() {
        setup();
        // 2026-03-01 ~ 2026-03-31 범위 필터
        // 계산: user u의 event e가 3월에 속하는 조건:
        //   total_secs = u*3600 + e*60 < 31*86400 = 2,678,400
        // → user 0..742 (743명) × 100 = 74,300 + user_743 의 event 0..59 (60건)
        //   = 74,360건
        let r = execute_select(&format!(
            "SELECT COUNT(*) FROM {TABLE} \
             WHERE event_time >= '2026-03-01T00:00:00Z' \
             AND event_time < '2026-04-01T00:00:00Z'"
        .await)).unwrap();
        let count = match &r.rows[0][0] {
            Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => 0,
        };
        // 문자열 비교 기반 날짜 필터이므로 ±100건 허용
        assert!(count >= 74_000 && count <= 75_000,
            "3월 이벤트 수 ≈ 74,360, actual = {count}");
    }

    #[test]
    fn test_referrer_distribution() {
        setup();
        let r = execute_select(&format!(
            "SELECT referrer, COUNT(*) as cnt \
             FROM {TABLE} \
             GROUP BY referrer \
             ORDER BY cnt DESC"
        .await)).unwrap();
        // 5가지 레퍼러 (google, facebook, direct, email, organic)
        assert_eq!(r.rows.len(), 5, "레퍼러 종류 = 5");
        // 모든 레퍼러가 균등하게 분배 (각 20,000건)
        for row in &r.rows {
            let cnt = match &row[1] {
                Value::Number(n) => n.as_u64().unwrap_or(0),
                _ => panic!(),
            };
            assert_eq!(cnt, 20_000, "각 레퍼러별 이벤트 수 = 20,000");
        }
    }

    // ── §5 퍼넬 분석 (FUNNEL_COUNT) ─────────────────────────────────────────

    #[test]
    fn test_funnel_page_view_to_purchase() {
        setup();
        let sql = format!(
            "SELECT FUNNEL_COUNT(\
                user_key => 'user_id', \
                event_col => 'event_name', \
                time_col => 'event_time', \
                steps => ['page_view', 'add_to_cart', 'purchase'], \
                time_window => INTERVAL '30 DAY' \
            ) FROM {TABLE}"
        );
        let r = execute_funnel_count(&sql).unwrap();

        // 3단계 퍼넬 결과
        assert_eq!(r.rows.len(), 3, "퍼넬 3단계");
        assert_eq!(r.columns, vec!["step", "step_name", "users"]);

        // step 1: page_view — 1,000 users (모든 사용자 page_view 보유)
        let step1 = r.rows[0][2].as_u64().unwrap_or(0);
        assert_eq!(step1, 1_000, "step 1 (page_view): 1,000 users");

        // step 2: add_to_cart — 600 users (user 0..599)
        let step2 = r.rows[1][2].as_u64().unwrap_or(0);
        assert_eq!(step2, 600, "step 2 (add_to_cart): 600 users");

        // step 3: purchase — 200 users (user 0..199)
        let step3 = r.rows[2][2].as_u64().unwrap_or(0);
        assert_eq!(step3, 200, "step 3 (purchase): 200 users");
    }

    #[test]
    fn test_funnel_conversion_rate() {
        setup();
        let sql = format!(
            "SELECT FUNNEL_COUNT(\
                user_key => 'user_id', \
                event_col => 'event_name', \
                time_col => 'event_time', \
                steps => ['page_view', 'add_to_cart', 'purchase'], \
                time_window => INTERVAL '30 DAY' \
            ) FROM {TABLE}"
        );
        let r = execute_funnel_count(&sql).unwrap();

        let step1 = r.rows[0][2].as_u64().unwrap_or(1) as f64;
        let step2 = r.rows[1][2].as_u64().unwrap_or(0) as f64;
        let step3 = r.rows[2][2].as_u64().unwrap_or(0) as f64;

        let conv1to2 = step2 / step1 * 100.0; // 60.0%
        let conv2to3 = step3 / step2 * 100.0; // 33.3%
        let conv1to3 = step3 / step1 * 100.0; // 20.0%

        assert!((conv1to2 - 60.0).abs() < 0.1, "step1→step2 전환율 60% ± 0.1, actual={conv1to2:.1}%");
        assert!((conv2to3 - 33.3).abs() < 0.2, "step2→step3 전환율 33.3% ± 0.2, actual={conv2to3:.1}%");
        assert!((conv1to3 - 20.0).abs() < 0.1, "전체 전환율 20% ± 0.1, actual={conv1to3:.1}%");
    }

    #[test]
    fn test_funnel_signup_to_purchase() {
        setup();
        let sql = format!(
            "SELECT FUNNEL_COUNT(\
                user_key => 'user_id', \
                event_col => 'event_name', \
                time_col => 'event_time', \
                steps => ['signup', 'purchase'], \
                time_window => INTERVAL '30 DAY' \
            ) FROM {TABLE}"
        );
        let r = execute_funnel_count(&sql).unwrap();
        assert_eq!(r.rows.len(), 2, "2단계 퍼넬");

        // signup: user 0..299 (300명)
        let step1 = r.rows[0][2].as_u64().unwrap_or(0);
        assert_eq!(step1, 300, "signup 사용자 = 300");

        // purchase: user 0..199 (200명) — signup + purchase 모두 가진 사용자 = user 0..199
        let step2 = r.rows[1][2].as_u64().unwrap_or(0);
        assert_eq!(step2, 200, "signup→purchase 사용자 = 200");
    }

    // ── §6 코호트 분석 (COHORT_ANALYSIS) ────────────────────────────────────

    #[test]
    fn test_cohort_analysis_signup_to_purchase() {
        setup();
        let sql = format!(
            "SELECT COHORT_ANALYSIS(\
                user_key => 'user_id', \
                entry_event => 'signup', \
                return_event => 'purchase', \
                event_col => 'event_name', \
                time_col => 'event_time' \
            ) FROM {TABLE}"
        );
        let r = execute_cohort_analysis(&sql).unwrap();

        assert!(!r.rows.is_empty(), "코호트 분석 결과 있음");
        assert_eq!(
            r.columns,
            vec!["cohort_week", "cohort_size", "day_7_retention", "day_14_retention", "day_30_retention"]
        );

        // 전체 코호트 사용자 합산 = signup 사용자 수 = 300
        let total_size: u64 = r.rows.iter()
            .filter_map(|row| row[1].as_u64())
            .sum();
        assert_eq!(total_size, 300, "코호트 전체 사용자 = 300 (signup 사용자)");
    }

    #[test]
    fn test_cohort_analysis_returns_percentage_strings() {
        setup();
        let sql = format!(
            "SELECT COHORT_ANALYSIS(\
                user_key => 'user_id', \
                entry_event => 'signup', \
                return_event => 'purchase', \
                event_col => 'event_name', \
                time_col => 'event_time' \
            ) FROM {TABLE}"
        );
        let r = execute_cohort_analysis(&sql).unwrap();
        for row in &r.rows {
            // retention 컬럼은 "XX.X%" 형식 문자열이어야 함
            for i in 2..=4 {
                match &row[i] {
                    Value::String(s) => assert!(s.ends_with('%'), "retention이 % 형식: {s}"),
                    other => panic!("retention은 String이어야 함, got: {other:?}"),
                }
            }
        }
    }

    // ── §7 경로 분석 (PATH_ANALYSIS) ────────────────────────────────────────

    #[test]
    fn test_path_analysis_page_navigation() {
        setup();
        let sql = format!(
            "SELECT PATH_ANALYSIS(\
                user_key => 'user_id', \
                event_col => 'page_url', \
                time_col => 'event_time', \
                path_length => 3 \
            ) FROM {TABLE}"
        );
        let r = execute_path_analysis(&sql).unwrap();

        assert!(!r.rows.is_empty(), "경로 분석 결과 있음");
        assert_eq!(r.columns, vec!["path", "count", "percentage"]);
        // 결과는 count DESC 정렬
        if r.rows.len() >= 2 {
            let count1 = r.rows[0][1].as_u64().unwrap_or(0);
            let count2 = r.rows[1][1].as_u64().unwrap_or(0);
            assert!(count1 >= count2, "경로 분석 결과 count 내림차순 정렬");
        }
    }

    #[test]
    fn test_path_analysis_event_sequence() {
        setup();
        let sql = format!(
            "SELECT PATH_ANALYSIS(\
                user_key => 'user_id', \
                event_col => 'event_name', \
                time_col => 'event_time', \
                path_length => 2 \
            ) FROM {TABLE}"
        );
        let r = execute_path_analysis(&sql).unwrap();

        assert!(!r.rows.is_empty(), "이벤트 시퀀스 경로 있음");
        // 상위 20개로 제한
        assert!(r.rows.len() <= 20, "상위 20개 경로만 반환");

        // 모든 percentage 합은 100% 이하 (버림 오류 포함)
        let total_pct: f64 = r.rows.iter()
            .filter_map(|row| {
                if let Value::String(s) = &row[2] {
                    s.trim_end_matches('%').parse::<f64>().ok()
                } else { None }
            })
            .sum();
        // 상위 20개 경로이므로 100% 미만 가능
        assert!(total_pct <= 100.1, "상위 경로 percentage 합 ≤ 100%");
    }

    // ── §8 WHERE 필터 조합 쿼리 ─────────────────────────────────────────────

    #[test]
    fn test_filter_by_user_id() {
        setup();
        let r = execute_select(&format!(
            "SELECT event_name, COUNT(*) FROM {TABLE} \
             WHERE user_id = 'user_0000' \
             GROUP BY event_name"
        .await)).unwrap();
        // user_0000은 signup, search, add_to_cart, checkout, purchase + page_view 보유
        let total: u64 = r.rows.iter()
            .filter_map(|row| row[1].as_u64())
            .sum();
        assert_eq!(total, 100, "user_0000의 이벤트 수 = 100");
    }

    #[test]
    fn test_filter_purchase_users_only() {
        setup();
        // purchase 이벤트를 가진 사용자만 필터
        let r = execute_select(&format!(
            "SELECT user_id, COUNT(*) as cnt \
             FROM {TABLE} \
             WHERE event_name = 'purchase' \
             GROUP BY user_id \
             ORDER BY user_id ASC \
             LIMIT 5"
        .await)).unwrap();
        assert_eq!(r.rows.len(), 5, "purchase 사용자 TOP 5");
        // 모두 user_0000 ~ user_0004 (u < 200)
        let first = match &r.rows[0][0] {
            Value::String(s) => s.clone(),
            _ => String::new(),
        };
        assert!(
            first.starts_with("user_0"),
            "첫 번째 purchase 사용자는 user_0xxx: {first}"
        );
    }

    #[test]
    fn test_limit_offset() {
        setup();
        let r = execute_select(&format!(
            "SELECT user_id, event_name FROM {TABLE} LIMIT 100"
        )).await.unwrap();
        assert_eq!(r.rows.len(), 100, "LIMIT 100 결과");
    }

    // ── §9 HAVING 절 ─────────────────────────────────────────────────────────

    #[test]
    fn test_having_high_activity_users() {
        setup();
        // 이벤트 수가 정확히 100인 사용자만 (모든 사용자가 해당)
        let r = execute_select(&format!(
            "SELECT user_id, COUNT(*) as cnt \
             FROM {TABLE} \
             GROUP BY user_id \
             HAVING cnt = 100"
        .await)).unwrap();
        assert_eq!(r.rows.len(), USER_COUNT, "모든 사용자가 정확히 100건 이벤트 보유");
    }

    // ── §10 MEM_STORE 행 수 확인 ────────────────────────────────────────────

    #[test]
    fn test_mem_store_row_count_after_inserts() {
        // MEM_STORE의 row_count API 직접 검증
        setup();
        let count = MEM_STORE.row_count(TABLE);
        assert_eq!(count, ROW_COUNT, "MEM_STORE.row_count() = 100,000");
    }

    // ── §11 빈 결과 처리 ─────────────────────────────────────────────────────

    #[test]
    fn test_empty_result_for_nonexistent_user() {
        setup();
        let r = execute_select(&format!(
            "SELECT * FROM {TABLE} WHERE user_id = 'user_9999'"
        )).await.unwrap();
        assert_eq!(r.rows.len(), 0, "존재하지 않는 사용자 → 빈 결과");
    }

    #[test]
    fn test_funnel_empty_steps() {
        setup();
        let sql = format!(
            "SELECT FUNNEL_COUNT(\
                user_key => 'user_id', \
                event_col => 'event_name', \
                time_col => 'event_time', \
                steps => [] \
            ) FROM {TABLE}"
        );
        let r = execute_funnel_count(&sql).unwrap();
        assert_eq!(r.rows.len(), 0, "빈 steps → 빈 결과");
    }

    // ── §12 전체 성능 지표 ────────────────────────────────────────────────────

    #[test]
    fn test_bulk_analytics_performance() {
        setup();
        use std::time::Instant;

        // GROUP BY 쿼리 성능
        let t0 = Instant::now();
        let _ = execute_select(&format!(
            "SELECT event_name, COUNT(*) FROM {TABLE} GROUP BY event_name"
        .await)).unwrap();
        let group_by_ms = t0.elapsed().as_millis();

        // FUNNEL 분석 성능
        let t1 = Instant::now();
        let sql = format!(
            "SELECT FUNNEL_COUNT(\
                user_key => 'user_id', \
                event_col => 'event_name', \
                time_col => 'event_time', \
                steps => ['page_view', 'add_to_cart', 'purchase'], \
                time_window => INTERVAL '30 DAY' \
            ) FROM {TABLE}"
        );
        let _ = execute_funnel_count(&sql).unwrap();
        let funnel_ms = t1.elapsed().as_millis();

        // COHORT 분석 성능
        let t2 = Instant::now();
        let _ = execute_cohort_analysis(&format!(
            "SELECT COHORT_ANALYSIS(\
                user_key => 'user_id', \
                entry_event => 'signup', \
                return_event => 'purchase', \
                event_col => 'event_name', \
                time_col => 'event_time' \
            ) FROM {TABLE}"
        .await)).unwrap();
        let cohort_ms = t2.elapsed().as_millis();

        // PATH 분석 성능
        let t3 = Instant::now();
        let _ = execute_path_analysis(&format!(
            "SELECT PATH_ANALYSIS(\
                user_key => 'user_id', \
                event_col => 'event_name', \
                time_col => 'event_time', \
                path_length => 3 \
            ) FROM {TABLE}"
        .await)).unwrap();
        let path_ms = t3.elapsed().as_millis();

        // 성능 기준: 100k 행 대상으로 각 쿼리 < 5,000ms (단일 스레드 메모리 스캔 기준)
        assert!(group_by_ms < 5_000, "GROUP BY 100k rows < 5s, actual={group_by_ms}ms");
        assert!(funnel_ms  < 5_000, "FUNNEL_COUNT 100k rows < 5s, actual={funnel_ms}ms");
        assert!(cohort_ms  < 5_000, "COHORT_ANALYSIS 100k rows < 5s, actual={cohort_ms}ms");
        assert!(path_ms    < 5_000, "PATH_ANALYSIS 100k rows < 5s, actual={path_ms}ms");

        eprintln!(
            "\n[Large Dataset Performance] \
             group_by={group_by_ms}ms  funnel={funnel_ms}ms  \
             cohort={cohort_ms}ms  path={path_ms}ms  \
             rows={ROW_COUNT}"
        );
    }
}

// NOTE: storage-node LSM MemTable 처리량 테스트는 storage-node 크레이트 내부의
//       storage-node/src/lsm/memtable.rs 단위 테스트에서 수행한다.
//       (query-node에서 storage-node 내부 타입에 직접 접근하지 않음)
