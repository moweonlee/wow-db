// WOW-DB 웹 분석 전용 함수 구현
// FUNNEL_COUNT, COHORT_ANALYSIS, PATH_ANALYSIS

use std::collections::HashMap;
use serde_json::Value;
use super::mem_store::{MEM_STORE, Row};
use super::select_exec::SelectResult;

// ── 공통 파서 유틸 ────────────────────────────────────────────────────────────

/// 큰따옴표/작은따옴표 제거
fn unquote(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'')) ||
       (s.starts_with('"') && s.ends_with('"')) {
        s[1..s.len()-1].to_string()
    } else {
        s.to_string()
    }
}

/// key => value 쌍 파싱
fn parse_named_args(args_str: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let mut remaining = args_str.trim().to_string();

    while !remaining.is_empty() {
        let arrow_pos = match remaining.find("=>") {
            Some(p) => p,
            None => break,
        };
        let key = remaining[..arrow_pos].trim().to_string();
        let after_arrow = remaining[arrow_pos+2..].trim().to_string();

        // 배열 [ ... ] 처리
        if after_arrow.starts_with('[') {
            if let Some(close) = after_arrow.find(']') {
                let value = after_arrow[..close+1].to_string();
                map.insert(key, value);
                remaining = after_arrow[close+1..].trim_start_matches(',').trim().to_string();
                continue;
            }
        }

        // INTERVAL '...' 처리
        let upper = after_arrow.to_uppercase();
        if upper.starts_with("INTERVAL") {
            let rest = after_arrow[8..].trim().to_string();
            let end = rest.find(|c: char| c == ',' || c == ')').unwrap_or(rest.len());
            let value = format!("INTERVAL {}", &rest[..end]);
            map.insert(key, value.trim().to_string());
            remaining = rest[end..].trim_start_matches(',').trim().to_string();
            continue;
        }

        // 일반 값 (콤마까지)
        let mut depth = 0i32;
        let mut end_pos = after_arrow.len();
        for (i, c) in after_arrow.char_indices() {
            match c {
                '\'' => depth = 1 - depth,
                ',' if depth == 0 => { end_pos = i; break; }
                _ => {}
            }
        }
        let value = after_arrow[..end_pos].trim().trim_end_matches(')').trim().to_string();
        map.insert(key, value);
        remaining = after_arrow[end_pos..].trim_start_matches(',').trim().to_string();
    }
    map
}

/// 배열 문자열 ['a','b'] → ["a","b"] 파싱
fn parse_string_array(s: &str) -> Vec<String> {
    let s = s.trim();
    if !s.starts_with('[') { return vec![]; }
    let inner = &s[1..s.len()-1];
    inner.split(',')
        .map(|item| unquote(item.trim()))
        .filter(|s| !s.is_empty())
        .collect()
}

/// INTERVAL '7 DAY' → 초 단위
fn parse_interval_secs(s: &str) -> i64 {
    let s = s.trim().to_uppercase();
    let s = s.trim_start_matches("INTERVAL").trim();
    let s = s.trim_matches('\'');
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() < 2 { return 86400 * 7; }
    let n: i64 = parts[0].parse().unwrap_or(7);
    match parts[1] {
        "SECOND" => n,
        "MINUTE" => n * 60,
        "HOUR"   => n * 3600,
        "DAY"    => n * 86400,
        "WEEK"   => n * 86400 * 7,
        "MONTH"  => n * 86400 * 30,
        "YEAR"   => n * 86400 * 365,
        _        => n * 86400,
    }
}

/// datetime 문자열 → Unix timestamp (초, 근사)
fn parse_datetime(s: &str) -> Option<i64> {
    let s = s.trim().trim_matches('\'').replace('T', " ");
    let parts: Vec<&str> = s.split(' ').collect();
    if parts.is_empty() { return None; }
    let dp: Vec<i64> = parts[0].split('-').filter_map(|x| x.parse().ok()).collect();
    if dp.len() < 3 { return None; }
    let (y, m, d) = (dp[0], dp[1], dp[2]);
    // 월별 일 수 누적 (윤년 무시한 간이 계산)
    let days_per_month = [0i64,31,28,31,30,31,30,31,31,30,31,30,31];
    let month_days: i64 = (1..m).map(|i| days_per_month[i as usize]).sum();
    let mut ts = (y - 1970) * 365 * 86400 + month_days * 86400 + (d - 1) * 86400;
    if parts.len() > 1 {
        let tp: Vec<i64> = parts[1].split(':').filter_map(|x| x.parse().ok()).collect();
        if tp.len() >= 3 { ts += tp[0]*3600 + tp[1]*60 + tp[2]; }
    }
    Some(ts)
}

fn row_str(row: &Row, col: &str) -> String {
    row.get(col).map(|v| match v {
        Value::String(s) => s.clone(),
        other => other.to_string().trim_matches('"').to_string(),
    }).unwrap_or_default()
}

/// SQL에서 괄호 범위 찾기 (args_start 위치에서 시작)
fn find_closing_paren(sql: &str, open_pos: usize) -> usize {
    let mut depth = 1i32;
    for (i, c) in sql[open_pos..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 { return open_pos + i; }
            }
            _ => {}
        }
    }
    sql.len()
}

/// FROM 다음 테이블명 추출
fn extract_table_from_sql(sql_after_func: &str) -> String {
    let lower = sql_after_func.to_lowercase();
    let from_pos = lower.find("from ").unwrap_or(0);
    lower[from_pos+5..].split_whitespace().next().unwrap_or("").to_string()
}

// ── FUNNEL_COUNT ──────────────────────────────────────────────────────────────

pub fn execute_funnel_count(sql: &str) -> Result<SelectResult, String> {
    let lower = sql.trim().to_lowercase();
    let fc_start = lower.find("funnel_count(").ok_or("FUNNEL_COUNT not found")?;
    let args_start = fc_start + "funnel_count(".len();
    let args_end = find_closing_paren(sql, args_start);

    let args_str = &sql[args_start..args_end];
    let args = parse_named_args(args_str);

    let user_key  = args.get("user_key").map(|s| unquote(s)).unwrap_or_else(|| "user_id".to_string());
    let event_col = args.get("event_col").map(|s| unquote(s)).unwrap_or_else(|| "event_name".to_string());
    let time_col  = args.get("time_col").map(|s| unquote(s)).unwrap_or_else(|| "event_time".to_string());
    let steps_raw = args.get("steps").cloned().unwrap_or_else(|| "[]".to_string());
    let steps = parse_string_array(&steps_raw);
    let window_raw = args.get("time_window").cloned().unwrap_or_else(|| "INTERVAL '7 DAY'".to_string());
    let window_secs = parse_interval_secs(&window_raw);

    let table_name = extract_table_from_sql(&sql[args_end+1..]);
    let all_rows = MEM_STORE.scan(&table_name);

    if steps.is_empty() {
        return Ok(SelectResult {
            columns: vec!["step".to_string(), "step_name".to_string(), "users".to_string()],
            rows: vec![],
        });
    }

    // 사용자별 이벤트 그룹화 (시간순)
    let mut user_events: HashMap<String, Vec<(i64, String)>> = HashMap::new();
    for row in &all_rows {
        let uid = row_str(row, &user_key);
        let event = row_str(row, &event_col);
        let ts = parse_datetime(&row_str(row, &time_col)).unwrap_or(0);
        if !uid.is_empty() {
            user_events.entry(uid).or_default().push((ts, event));
        }
    }
    for events in user_events.values_mut() {
        events.sort_by_key(|(ts, _)| *ts);
    }

    let mut step_counts = vec![0u64; steps.len()];

    for events in user_events.values() {
        let mut step_idx = 0usize;
        let mut first_step_ts: Option<i64> = None;
        let mut max_step = 0usize;

        for (ts, event) in events {
            if step_idx < steps.len() && event == &steps[step_idx] {
                if step_idx == 0 {
                    first_step_ts = Some(*ts);
                    step_idx = 1;
                    max_step = 1;
                } else if let Some(start_ts) = first_step_ts {
                    if ts - start_ts <= window_secs {
                        step_idx += 1;
                        max_step = step_idx;
                    } else {
                        // 윈도우 초과 — 새 퍼넬 시작
                        step_idx = 1;
                        first_step_ts = Some(*ts);
                    }
                }
            }
        }

        for i in 0..max_step {
            step_counts[i] += 1;
        }
    }

    let result_rows: Vec<Vec<Value>> = steps.iter().enumerate().map(|(i, name)| vec![
        Value::Number(serde_json::Number::from((i+1) as u64)),
        Value::String(name.clone()),
        Value::Number(serde_json::Number::from(step_counts[i])),
    ]).collect();

    Ok(SelectResult {
        columns: vec!["step".to_string(), "step_name".to_string(), "users".to_string()],
        rows: result_rows,
    })
}

// ── COHORT_ANALYSIS ───────────────────────────────────────────────────────────

pub fn execute_cohort_analysis(sql: &str) -> Result<SelectResult, String> {
    let lower = sql.trim().to_lowercase();
    let ca_start = lower.find("cohort_analysis(").ok_or("COHORT_ANALYSIS not found")?;
    let args_start = ca_start + "cohort_analysis(".len();
    let args_end = find_closing_paren(sql, args_start);

    let args_str = &sql[args_start..args_end];
    let args = parse_named_args(args_str);

    let user_key     = args.get("user_key").map(|s| unquote(s)).unwrap_or_else(|| "user_id".to_string());
    let entry_event  = args.get("entry_event").map(|s| unquote(s)).unwrap_or_else(|| "signup".to_string());
    let return_event = args.get("return_event").map(|s| unquote(s)).unwrap_or_else(|| "purchase".to_string());
    let event_col    = args.get("event_col").map(|s| unquote(s)).unwrap_or_else(|| "event_name".to_string());
    let time_col     = args.get("time_col").map(|s| unquote(s)).unwrap_or_else(|| "event_time".to_string());

    let table_name = extract_table_from_sql(&sql[args_end+1..]);
    let all_rows = MEM_STORE.scan(&table_name);

    let mut user_events: HashMap<String, Vec<(i64, String)>> = HashMap::new();
    for row in &all_rows {
        let uid = row_str(row, &user_key);
        let event = row_str(row, &event_col);
        let ts = parse_datetime(&row_str(row, &time_col)).unwrap_or(0);
        if !uid.is_empty() {
            user_events.entry(uid).or_default().push((ts, event));
        }
    }
    for events in user_events.values_mut() {
        events.sort_by_key(|(ts, _)| *ts);
    }

    // cohort_week → (size, d7_users, d14_users, d30_users)
    let mut cohort_data: HashMap<i64, (u64, u64, u64, u64)> = HashMap::new();

    for events in user_events.values() {
        let entry_ts = events.iter()
            .find(|(_, e)| e == &entry_event)
            .map(|(ts, _)| *ts);

        if let Some(entry_ts) = entry_ts {
            let cohort_week = entry_ts / (86400 * 7);
            let entry = cohort_data.entry(cohort_week).or_insert((0, 0, 0, 0));
            entry.0 += 1;

            for (ts, event) in events {
                if event == &return_event && *ts > entry_ts {
                    let days = (*ts - entry_ts) / 86400;
                    if days <= 7  { entry.1 += 1; }
                    if days <= 14 { entry.2 += 1; }
                    if days <= 30 { entry.3 += 1; break; }
                }
            }
        }
    }

    let mut result_rows: Vec<Vec<Value>> = cohort_data.iter()
        .map(|(week, (size, d7, d14, d30))| vec![
            Value::String(format!("Week {week}")),
            Value::Number(serde_json::Number::from(*size)),
            Value::String(format!("{:.1}%", if *size > 0 { *d7 as f64 / *size as f64 * 100.0 } else { 0.0 })),
            Value::String(format!("{:.1}%", if *size > 0 { *d14 as f64 / *size as f64 * 100.0 } else { 0.0 })),
            Value::String(format!("{:.1}%", if *size > 0 { *d30 as f64 / *size as f64 * 100.0 } else { 0.0 })),
        ])
        .collect();

    result_rows.sort_by(|a, b| a[0].to_string().cmp(&b[0].to_string()));

    Ok(SelectResult {
        columns: vec![
            "cohort_week".to_string(), "cohort_size".to_string(),
            "day_7_retention".to_string(), "day_14_retention".to_string(), "day_30_retention".to_string(),
        ],
        rows: result_rows,
    })
}

// ── PATH_ANALYSIS ─────────────────────────────────────────────────────────────

pub fn execute_path_analysis(sql: &str) -> Result<SelectResult, String> {
    let lower = sql.trim().to_lowercase();
    let pa_start = lower.find("path_analysis(").ok_or("PATH_ANALYSIS not found")?;
    let args_start = pa_start + "path_analysis(".len();
    let args_end = find_closing_paren(sql, args_start);

    let args_str = &sql[args_start..args_end];
    let args = parse_named_args(args_str);

    let user_key  = args.get("user_key").map(|s| unquote(s)).unwrap_or_else(|| "user_id".to_string());
    let event_col = args.get("event_col").map(|s| unquote(s)).unwrap_or_else(|| "page_url".to_string());
    let time_col  = args.get("time_col").map(|s| unquote(s)).unwrap_or_else(|| "event_time".to_string());
    let path_len: usize = args.get("path_length")
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(3);

    let table_name = extract_table_from_sql(&sql[args_end+1..]);
    let all_rows = MEM_STORE.scan(&table_name);

    let mut user_events: HashMap<String, Vec<(i64, String)>> = HashMap::new();
    for row in &all_rows {
        let uid = row_str(row, &user_key);
        let event = row_str(row, &event_col);
        let ts = parse_datetime(&row_str(row, &time_col)).unwrap_or(0);
        if !uid.is_empty() {
            user_events.entry(uid).or_default().push((ts, event));
        }
    }
    for events in user_events.values_mut() {
        events.sort_by_key(|(ts, _)| *ts);
    }

    let total_users = user_events.len().max(1) as f64;
    let mut path_counts: HashMap<String, u64> = HashMap::new();

    for events in user_events.values() {
        let pages: Vec<&str> = events.iter().map(|(_, e)| e.as_str()).collect();
        if pages.len() < path_len { continue; }
        for window in pages.windows(path_len) {
            let path = window.join(" => ");
            *path_counts.entry(path).or_insert(0) += 1;
        }
    }

    let mut result_rows: Vec<Vec<Value>> = path_counts.iter()
        .map(|(path, count)| vec![
            Value::String(path.clone()),
            Value::Number(serde_json::Number::from(*count)),
            Value::String(format!("{:.1}%", *count as f64 / total_users * 100.0)),
        ])
        .collect();

    result_rows.sort_by(|a, b| {
        let ca = if let Value::Number(n) = &a[1] { n.as_u64().unwrap_or(0) } else { 0 };
        let cb = if let Value::Number(n) = &b[1] { n.as_u64().unwrap_or(0) } else { 0 };
        cb.cmp(&ca)
    });

    result_rows.truncate(20);

    Ok(SelectResult {
        columns: vec!["path".to_string(), "count".to_string(), "percentage".to_string()],
        rows: result_rows,
    })
}
