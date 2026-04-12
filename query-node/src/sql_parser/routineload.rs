// T061: ROUTINE LOAD 문법 완전 파싱
// CREATE/PAUSE/RESUME/STOP ROUTINE LOAD 상세 파싱 유틸리티

use anyhow::{anyhow, Result};

use super::{CreateRoutineLoadStmt, WowDbCustom};

/// CREATE ROUTINE LOAD 상세 파서
///
/// 문법:
/// ```sql
/// CREATE ROUTINE LOAD <job_name> ON <cube_name>
///   FROM KAFKA
///   (
///     "kafka_broker_list" = "broker1:9092,broker2:9092",
///     "kafka_topic" = "my_topic",
///     "format" = "json"
///   )
///   PROPERTIES
///   (
///     "max_batch_rows" = "50000",
///     "max_batch_interval" = "10"
///   );
/// ```
pub fn parse_create_routine_load(sql: &str) -> Result<CreateRoutineLoadStmt> {
    let upper = sql.to_uppercase();

    // 1. Job name (after ROUTINE LOAD)
    let job_name = extract_after_keyword(sql, "ROUTINE LOAD")
        .ok_or_else(|| anyhow!("Expected job name after ROUTINE LOAD"))?;

    // 2. Cube name (after ON)
    let cube_name = extract_after_keyword(sql, " ON ")
        .ok_or_else(|| anyhow!("Expected cube name after ON"))?;

    // 3. FROM type (only KAFKA supported)
    if !upper.contains("FROM KAFKA") {
        return Err(anyhow!("Only FROM KAFKA is supported in ROUTINE LOAD"));
    }

    // 4. Parse key=value pairs from parenthesized blocks
    let kvs = parse_kv_blocks(sql)?;

    let brokers_str = kvs.get("kafka_broker_list")
        .or_else(|| kvs.get("kafka_brokers"))
        .cloned()
        .unwrap_or_default();
    let brokers: Vec<String> = brokers_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let topic = kvs.get("kafka_topic")
        .cloned()
        .unwrap_or_default();

    let format = kvs.get("format")
        .cloned()
        .unwrap_or_else(|| "JSON".to_string())
        .to_uppercase();

    // Remaining kvs become properties
    let properties: Vec<(String, String)> = kvs
        .into_iter()
        .filter(|(k, _)| {
            k != "kafka_broker_list"
                && k != "kafka_brokers"
                && k != "kafka_topic"
                && k != "format"
        })
        .collect();

    Ok(CreateRoutineLoadStmt {
        job_name,
        cube_name,
        kafka_topic: topic,
        brokers,
        format,
        properties,
    })
}

/// PAUSE/RESUME/STOP ROUTINE LOAD 파서
pub fn parse_routine_load_control(sql: &str) -> Result<WowDbCustom> {
    let upper = sql.trim().to_uppercase();

    let (verb, sql_verb) = if upper.starts_with("PAUSE") {
        ("PAUSE", "PAUSE")
    } else if upper.starts_with("RESUME") {
        ("RESUME", "RESUME")
    } else if upper.starts_with("STOP") {
        ("STOP", "STOP")
    } else {
        return Err(anyhow!("Unknown ROUTINE LOAD control verb in: {}", sql));
    };

    let job_name = extract_after_keyword(sql, "LOAD")
        .ok_or_else(|| anyhow!("Expected job name after ROUTINE LOAD in {} statement", verb))?;

    Ok(match sql_verb {
        "PAUSE"  => WowDbCustom::PauseRoutineLoad  { job_name },
        "RESUME" => WowDbCustom::ResumeRoutineLoad { job_name },
        _        => WowDbCustom::StopRoutineLoad   { job_name },
    })
}

// ─── 내부 헬퍼 ────────────────────────────────────────────────────────────────

/// keyword 다음의 첫 번째 토큰 추출 (대소문자 무시)
fn extract_after_keyword(sql: &str, keyword: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let kw_upper = keyword.to_uppercase();
    let pos = upper.find(&kw_upper)?;
    let rest = &sql[pos + keyword.len()..];
    let token = rest
        .split_whitespace()
        .next()?
        .trim_end_matches(';')
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
        .to_string();
    if token.is_empty() { None } else { Some(token) }
}

/// 괄호 내 "key" = "value" 쌍 파싱 (여러 블록 합산)
fn parse_kv_blocks(sql: &str) -> Result<std::collections::HashMap<String, String>> {
    let mut result = std::collections::HashMap::new();

    // 각 괄호 블록 추출
    let mut depth = 0i32;
    let mut block_start = None;
    let chars: Vec<char> = sql.chars().collect();

    for (i, &c) in chars.iter().enumerate() {
        match c {
            '(' => {
                if depth == 0 { block_start = Some(i + 1); }
                depth += 1;
            }
            ')' => {
                depth -= 1;
                if depth == 0 {
                    if let Some(start) = block_start {
                        let block: String = chars[start..i].iter().collect();
                        parse_kv_in_block(&block, &mut result)?;
                        block_start = None;
                    }
                }
            }
            _ => {}
        }
    }

    Ok(result)
}

fn parse_kv_in_block(
    block: &str,
    result: &mut std::collections::HashMap<String, String>,
) -> Result<()> {
    // 따옴표를 고려한 쉼표 분리 — "key" = "val,with,commas" 지원
    let items = split_kv_items(block);

    for item in items {
        let item = item.trim();
        if item.is_empty() { continue; }

        // = 를 찾되, 따옴표 안의 = 는 무시
        if let Some(eq_pos) = find_unquoted_eq(item) {
            let key = item[..eq_pos]
                .trim()
                .trim_matches('"')
                .trim_matches('\'')
                .trim()
                .to_lowercase();
            let val = item[eq_pos + 1..]
                .trim()
                .trim_matches('"')
                .trim_matches('\'')
                .trim()
                .to_string();
            if !key.is_empty() {
                result.insert(key, val);
            }
        }
    }
    Ok(())
}

/// 따옴표 밖의 쉼표로 분리
fn split_kv_items(s: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut quote_char = ' ';

    for c in s.chars() {
        match c {
            '"' | '\'' if !in_quote => { in_quote = true; quote_char = c; current.push(c); }
            c if in_quote && c == quote_char => { in_quote = false; current.push(c); }
            ',' if !in_quote => { items.push(current.trim().to_string()); current.clear(); }
            _ => current.push(c),
        }
    }
    if !current.trim().is_empty() { items.push(current.trim().to_string()); }
    items
}

/// 따옴표 밖의 첫 번째 = 위치 탐색
fn find_unquoted_eq(s: &str) -> Option<usize> {
    let mut in_quote = false;
    let mut quote_char = ' ';
    for (i, c) in s.char_indices() {
        match c {
            '"' | '\'' if !in_quote => { in_quote = true; quote_char = c; }
            c if in_quote && c == quote_char => { in_quote = false; }
            '=' if !in_quote => return Some(i),
            _ => {}
        }
    }
    None
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_routine_load_full() {
        let sql = r#"
            CREATE ROUTINE LOAD my_job ON page_events
            FROM KAFKA
            (
                "kafka_broker_list" = "broker1:9092,broker2:9092",
                "kafka_topic" = "page_events_topic",
                "format" = "JSON"
            )
            PROPERTIES
            (
                "max_batch_rows" = "50000",
                "max_batch_interval" = "10"
            );
        "#;

        let stmt = parse_create_routine_load(sql).unwrap();
        assert_eq!(stmt.job_name, "my_job");
        assert_eq!(stmt.cube_name, "page_events");
        assert_eq!(stmt.kafka_topic, "page_events_topic");
        assert_eq!(stmt.brokers.len(), 2);
        assert_eq!(stmt.format, "JSON");
        // Properties should contain max_batch_rows
        assert!(stmt.properties.iter().any(|(k, _)| k == "max_batch_rows"));
    }

    #[test]
    fn test_parse_pause_routine_load() {
        let custom = parse_routine_load_control("PAUSE ROUTINE LOAD my_job").unwrap();
        assert!(matches!(custom, WowDbCustom::PauseRoutineLoad { job_name } if job_name == "my_job"));
    }

    #[test]
    fn test_parse_stop_routine_load() {
        let custom = parse_routine_load_control("STOP ROUTINE LOAD job_x;").unwrap();
        assert!(matches!(custom, WowDbCustom::StopRoutineLoad { job_name } if job_name == "job_x"));
    }

    #[test]
    fn test_extract_after_keyword() {
        let s = "CREATE ROUTINE LOAD my_job ON cube_a FROM KAFKA";
        assert_eq!(extract_after_keyword(s, "ROUTINE LOAD"), Some("my_job".into()));
        assert_eq!(extract_after_keyword(s, " ON "), Some("cube_a".into()));
    }
}
