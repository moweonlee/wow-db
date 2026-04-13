// INSERT INTO 실행기
// INSERT INTO tbl (c1,c2) VALUES (v1,v2), (v3,v4)
// INSERT INTO tbl VALUES ('{"json":"val"}')  ← JSON 단일 컬럼

use serde_json::{json, Value};
use std::collections::HashMap;

use super::mem_store::{MEM_STORE, Row};

#[derive(Debug)]
pub struct InsertResult {
    pub rows_affected: u64,
}

/// SQL 문자열로부터 INSERT 실행
pub fn execute_insert(sql: &str) -> Result<InsertResult, String> {
    let sql = sql.trim().trim_end_matches(';');
    let upper = sql.to_uppercase();

    // INSERT INTO <table> [(col,...)] VALUES (val,...)[,(val,...)]
    let after_into = upper.find("INTO").ok_or("INSERT: INTO keyword not found")?;
    let rest = sql[after_into + 4..].trim();

    // 테이블명 추출
    let (table_name, rest) = split_table_name(rest);
    let rest = rest.trim();

    // 컬럼 목록 (선택 사항)
    let (columns, values_str) = if rest.starts_with('(') && !rest.to_uppercase().starts_with("(SELECT") {
        // VALUES 키워드 앞까지가 컬럼 목록
        let upper_rest = rest.to_uppercase();
        let values_pos = upper_rest.find("VALUES").ok_or("INSERT: VALUES keyword not found")?;
        let col_part = &rest[..values_pos];
        let val_part = &rest[values_pos + 6..];
        let cols = parse_identifier_list(col_part)?;
        (Some(cols), val_part.trim())
    } else {
        // VALUES 바로 시작
        let upper_rest = rest.to_uppercase();
        let values_pos = upper_rest.find("VALUES").ok_or("INSERT: VALUES keyword not found")?;
        (None, rest[values_pos + 6..].trim())
    };

    // VALUES (...), (...) 파싱
    let value_tuples = parse_value_tuples(values_str)?;

    let mut count = 0u64;
    for tuple in value_tuples {
        let row = if let Some(ref cols) = columns {
            if cols.len() != tuple.len() {
                return Err(format!(
                    "Column count {} doesn't match value count {}",
                    cols.len(), tuple.len()
                ));
            }
            cols.iter().zip(tuple.iter())
                .map(|(col, val)| (col.clone(), val.clone()))
                .collect::<Row>()
        } else if tuple.len() == 1 {
            // 단일 값 → JSON 파싱 시도, 아니면 _value 키로 저장
            match &tuple[0] {
                Value::Object(map) => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                other => {
                    let mut row = Row::new();
                    row.insert("_value".to_string(), other.clone());
                    row
                }
            }
        } else {
            return Err("No column list provided for multi-value INSERT".to_string());
        };
        MEM_STORE.insert(&table_name, row);
        count += 1;
    }

    Ok(InsertResult { rows_affected: count })
}

// ── 파싱 헬퍼 ──────────────────────────────────────────────────────────────────

fn split_table_name(s: &str) -> (String, &str) {
    let s = s.trim_start_matches('`');
    let end = s.find(|c: char| c.is_whitespace() || c == '(').unwrap_or(s.len());
    let name = s[..end].trim_end_matches('`').to_string();
    (name, &s[end..])
}

fn parse_identifier_list(s: &str) -> Result<Vec<String>, String> {
    // "(col1, col2, col3)" → ["col1", "col2", "col3"]
    let inner = s.trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim_end_matches(',');
    Ok(inner.split(',')
        .map(|c| c.trim().trim_matches('`').to_string())
        .filter(|c| !c.is_empty())
        .collect())
}

/// VALUES 절에서 튜플 목록 파싱
/// "(1,'a','{"k":"v"}'),(2,'b','{}')  →  [[1,"a",{"k":"v"}],[2,"b",{}]]
fn parse_value_tuples(s: &str) -> Result<Vec<Vec<Value>>, String> {
    let mut tuples = Vec::new();
    let mut pos = 0;
    let bytes = s.as_bytes();

    while pos < s.len() {
        // 공백·콤마 스킵
        while pos < s.len() && (bytes[pos] == b' ' || bytes[pos] == b'\t' || bytes[pos] == b'\n' || bytes[pos] == b',') {
            pos += 1;
        }
        if pos >= s.len() { break; }
        if bytes[pos] != b'(' {
            return Err(format!("Expected '(' at pos {pos}, got '{}'", bytes[pos] as char));
        }
        let (tuple, next) = parse_tuple(&s[pos..])?;
        tuples.push(tuple);
        pos += next;
    }
    Ok(tuples)
}

/// 단일 튜플 파싱 "(val1, val2, ...)" → (values, consumed_bytes)
fn parse_tuple(s: &str) -> Result<(Vec<Value>, usize), String> {
    let mut values = Vec::new();
    let bytes = s.as_bytes();
    let mut pos = 1; // '(' 이후

    loop {
        // 앞 공백 스킵
        while pos < s.len() && (bytes[pos] == b' ' || bytes[pos] == b'\t') { pos += 1; }
        if pos >= s.len() { return Err("Unterminated tuple".into()); }

        if bytes[pos] == b')' { return Ok((values, pos + 1)); }
        if bytes[pos] == b',' { pos += 1; continue; }

        let (val, consumed) = parse_value(&s[pos..])?;
        values.push(val);
        pos += consumed;
    }
}

/// 단일 값 파싱 → (Value, consumed_bytes)
fn parse_value(s: &str) -> Result<(Value, usize), String> {
    let s = s.trim_start();
    let skipped = {
        let orig_len = s.len();
        let trimmed_len = s.len();
        orig_len - trimmed_len
    };
    let _ = skipped;

    if s.is_empty() { return Err("Empty value".into()); }

    let bytes = s.as_bytes();

    match bytes[0] {
        // NULL
        b'N' | b'n' if s.to_uppercase().starts_with("NULL") => {
            Ok((Value::Null, 4))
        }
        // 문자열: 싱글쿼트
        b'\'' => {
            let mut i = 1;
            let mut buf = String::new();
            while i < s.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < s.len() && bytes[i + 1] == b'\'' {
                        // escaped ''
                        buf.push('\'');
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else if bytes[i] == b'\\' && i + 1 < s.len() {
                    match bytes[i + 1] {
                        b'n'  => { buf.push('\n'); i += 2; }
                        b't'  => { buf.push('\t'); i += 2; }
                        b'\\' => { buf.push('\\'); i += 2; }
                        b'\'' => { buf.push('\''); i += 2; }
                        b'"'  => { buf.push('"');  i += 2; }
                        other => { buf.push(other as char); i += 2; }
                    }
                } else {
                    buf.push(bytes[i] as char);
                    i += 1;
                }
            }
            // JSON 파싱 시도
            let v = serde_json::from_str::<Value>(&buf).unwrap_or(Value::String(buf));
            Ok((v, i))
        }
        // 숫자
        b'0'..=b'9' | b'-' | b'+' => {
            let end = s.find(|c: char| c == ',' || c == ')' || c.is_whitespace())
                .unwrap_or(s.len());
            let num_str = &s[..end];
            let v = if num_str.contains('.') {
                num_str.parse::<f64>().map(|n| json!(n)).unwrap_or(Value::String(num_str.to_string()))
            } else {
                num_str.parse::<i64>().map(|n| json!(n)).unwrap_or(Value::String(num_str.to_string()))
            };
            Ok((v, end))
        }
        // TRUE / FALSE
        b'T' | b't' if s.to_uppercase().starts_with("TRUE") => Ok((Value::Bool(true), 4)),
        b'F' | b'f' if s.to_uppercase().starts_with("FALSE") => Ok((Value::Bool(false), 5)),
        // NOW() → 현재 시간 문자열
        b'N' if s.to_uppercase().starts_with("NOW()") => {
            let now = chrono::Utc::now().to_rfc3339();
            Ok((Value::String(now), 5))
        }
        other => Err(format!("Unexpected value character '{}' in: {}", other as char, &s[..s.len().min(40)])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_basic() {
        let sql = "INSERT INTO test_tbl (id, name, score) VALUES (1, 'alice', 99.5), (2, 'bob', 87)";
        let r = execute_insert(sql).unwrap();
        assert_eq!(r.rows_affected, 2);
        let rows = MEM_STORE.scan("test_tbl");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_insert_json() {
        let sql = r#"INSERT INTO json_tbl (id, props) VALUES (1, '{"page":"home","ref":"google"}')"#;
        let r = execute_insert(sql).unwrap();
        assert_eq!(r.rows_affected, 1);
    }
}
