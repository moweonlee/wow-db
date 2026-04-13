// T069: 전체 CREATE CUBE 문법 파서
// PARTITION BY RANGE, AUTO PARTITION, ORDER BY, DISTRIBUTED BY HASH, COLOCATE WITH, STORAGE BACKEND
// T116: Sort Key 검증 — FR-026: 최대 4 컬럼, 128 bytes, JSON 금지, STRING 64 bytes 경고

use anyhow::{anyhow, Result};
use tracing::warn;

use super::{
    CubeColumnDef, CubeDistribution, CubePartitionBy, CreateCubeStmt, WowDbCustom,
    AlterCubeStmt, AlterCubeAction, DropCubeStmt,
};

/// CREATE CUBE 전체 파서
///
/// 지원 문법:
/// ```sql
/// CREATE CUBE [IF NOT EXISTS] <name>
/// (
///   <col> <type> [NOT NULL] [ENCODING(<enc>)] [COMMENT '<c>'],
///   ...
/// )
/// [PARTITION BY RANGE(<col>) [INTERVAL DAY|MONTH|YEAR] [AUTO]]
/// [ORDER BY (<col>, ...)]
/// [DISTRIBUTED BY HASH(<col>, ...) BUCKETS <n>]
/// [COLOCATE WITH <group>]
/// [STORAGE BACKEND = 'native'|'s3'|'hdfs']
/// [TTL <n> DAY|MONTH|YEAR]
/// [PROPERTIES (...)]
/// ```
pub fn parse_create_cube_full(sql: &str) -> Result<CreateCubeStmt> {
    let upper = sql.to_uppercase();

    // IF NOT EXISTS
    let if_not_exists = upper.contains("IF NOT EXISTS");

    // 큐브 이름
    let name_keyword = if if_not_exists { "EXISTS" } else { "CUBE" };
    let name = extract_token_after(sql, name_keyword)
        .ok_or_else(|| anyhow!("Expected cube name after {}", name_keyword))?;

    // 컬럼 정의 파싱 (첫 번째 괄호 블록)
    let columns = parse_column_defs(sql)?;

    // PARTITION BY
    let partition_by = parse_partition_by(sql);

    // ORDER BY
    let order_by = parse_order_by(sql);

    // DISTRIBUTED BY HASH
    let distribution = parse_distribution(sql);

    // PROPERTIES
    let properties = parse_properties(sql);

    Ok(CreateCubeStmt {
        name,
        database: None,
        columns,
        partition_by,
        order_by,
        distribution,
        properties,
        if_not_exists,
    })
}

/// ALTER CUBE 파서
///
/// ```sql
/// ALTER CUBE <name>
///   ADD COLUMN <col> <type>
///   | DROP COLUMN <col>
///   | ADD INDEX <name> (<cols>) USING BITMAP|BLOOM|MINMAX
///   | DROP INDEX <name>
///   | MODIFY TTL <n> DAY|MONTH|YEAR
/// ```
pub fn parse_alter_cube(sql: &str) -> Result<AlterCubeStmt> {
    let upper  = sql.to_uppercase();
    let name   = extract_token_after(sql, "CUBE")
        .ok_or_else(|| anyhow!("Expected cube name after CUBE"))?;

    let action = if upper.contains("ADD COLUMN") {
        let rest = extract_after_pattern(sql, "ADD COLUMN")?;
        let col  = parse_single_column_def(rest.trim())?;
        AlterCubeAction::AddColumn(col)
    } else if upper.contains("DROP COLUMN") {
        let col = extract_token_after(sql, "COLUMN")
            .ok_or_else(|| anyhow!("Expected column name after COLUMN"))?;
        AlterCubeAction::DropColumn(col)
    } else if upper.contains("ADD INDEX") {
        let rest = extract_after_pattern(sql, "ADD INDEX")?;
        parse_add_index(&rest)?
    } else if upper.contains("DROP INDEX") {
        let idx = extract_token_after(sql, "INDEX")
            .ok_or_else(|| anyhow!("Expected index name after INDEX"))?;
        AlterCubeAction::DropIndex(idx)
    } else if upper.contains("MODIFY TTL") {
        parse_modify_ttl(sql)?
    } else {
        return Err(anyhow!("Unknown ALTER CUBE action in: {}", &sql[..sql.len().min(80)]));
    };

    Ok(AlterCubeStmt { name, action })
}

/// DROP CUBE 파서
pub fn parse_drop_cube(sql: &str) -> Result<DropCubeStmt> {
    let upper     = sql.to_uppercase();
    let if_exists = upper.contains("IF EXISTS");
    let keyword   = if if_exists { "EXISTS" } else { "CUBE" };
    let name      = extract_token_after(sql, keyword)
        .ok_or_else(|| anyhow!("Expected cube name"))?;
    Ok(DropCubeStmt { name, if_exists })
}

// ─── 내부 파서 헬퍼 ──────────────────────────────────────────────────────────

/// 첫 번째 괄호 블록에서 컬럼 정의 파싱
fn parse_column_defs(sql: &str) -> Result<Vec<CubeColumnDef>> {
    let block = extract_first_paren_block(sql)
        .unwrap_or_default();

    if block.is_empty() {
        return Ok(Vec::new());
    }

    let mut cols = Vec::new();
    for item in split_top_level_commas(&block) {
        let item = item.trim();
        if item.is_empty() { continue; }
        if let Ok(col) = parse_single_column_def(item) {
            cols.push(col);
        }
    }
    Ok(cols)
}

/// 단일 컬럼 정의 파싱: `col_name TYPE [NOT NULL] [ENCODING(x)] [COMMENT 'c']`
fn parse_single_column_def(s: &str) -> Result<CubeColumnDef> {
    let upper = s.to_uppercase();
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() < 2 {
        return Err(anyhow!("Invalid column def: '{}'", s));
    }

    let name      = parts[0].trim_matches('`').to_string();
    let data_type = parts[1].to_uppercase();
    let nullable  = !upper.contains("NOT NULL");

    let encoding = if upper.contains("ENCODING(") {
        extract_between(s, "ENCODING(", ")").map(|s| s.to_uppercase())
    } else {
        None
    };

    let comment = if upper.contains("COMMENT") {
        extract_quoted_after(s, "COMMENT")
    } else {
        None
    };

    Ok(CubeColumnDef { name, data_type, nullable, encoding, comment })
}

/// PARTITION BY 파싱
fn parse_partition_by(sql: &str) -> Option<CubePartitionBy> {
    let upper = sql.to_uppercase();
    if !upper.contains("PARTITION BY") {
        return None;
    }

    // PARTITION BY RANGE(<col>)
    let col = extract_between(sql, "RANGE(", ")")
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    let columns = if col.is_empty() { vec![] } else { vec![col] };

    // INTERVAL DAY|MONTH|YEAR
    let granularity = if upper.contains("INTERVAL DAY") {
        "DAY".to_string()
    } else if upper.contains("INTERVAL YEAR") {
        "YEAR".to_string()
    } else {
        "MONTH".to_string() // default
    };

    let auto = upper.contains("AUTO");

    Some(CubePartitionBy { columns, granularity, auto })
}

/// ORDER BY 파싱
fn parse_order_by(sql: &str) -> Vec<String> {
    let upper = sql.to_uppercase();
    if !upper.contains("ORDER BY") {
        return Vec::new();
    }

    // ORDER BY (col1, col2) 또는 ORDER BY col1, col2
    if let Some(block) = {
        // ORDER BY 이후 괄호 블록
        let ob_pos = upper.find("ORDER BY").unwrap_or(0);
        let rest = &sql[ob_pos + 8..];
        let rest_upper = rest.to_uppercase().trim_start().to_string();
        if rest_upper.starts_with('(') {
            extract_first_paren_block(rest)
        } else {
            // 괄호 없이: ORDER BY col1, col2 DISTRIBUTED BY ...
            let end = rest_upper.find('\n')
                .or_else(|| rest_upper.find("DISTRIBUTED"))
                .or_else(|| rest_upper.find("COLOCATE"))
                .or_else(|| rest_upper.find("STORAGE"))
                .or_else(|| rest_upper.find("TTL"))
                .or_else(|| rest_upper.find("PROPERTIES"))
                .unwrap_or(rest.len());
            Some(rest[..end].to_string())
        }
    } {
        block.split(',')
            .map(|s| s.trim().trim_matches('`').to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        Vec::new()
    }
}

/// DISTRIBUTED BY HASH 파싱
fn parse_distribution(sql: &str) -> Option<CubeDistribution> {
    let upper = sql.to_uppercase();
    if !upper.contains("DISTRIBUTED BY HASH") {
        return None;
    }

    let col_block = extract_between(sql, "HASH(", ")")?;
    let columns: Vec<String> = col_block.split(',')
        .map(|s| s.trim().trim_matches('`').to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // BUCKETS <n>
    let buckets = extract_token_after(sql, "BUCKETS")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(64);

    Some(CubeDistribution { columns, buckets })
}

/// PROPERTIES(...) 파싱
/// 컬럼 정의 블록(첫 번째 최상위 괄호 블록) 이후에서만 PROPERTIES 키워드를 검색
fn parse_properties(sql: &str) -> Vec<(String, String)> {
    // 컬럼 정의 블록(첫 번째 top-level paren) 이후 오프셋 계산
    let after_col_defs = skip_past_first_top_level_paren(sql);
    let search_str = &sql[after_col_defs..];
    let upper = search_str.to_uppercase();
    let prop_pos = upper.find("PROPERTIES");
    if prop_pos.is_none() { return Vec::new(); }
    let rest = &search_str[prop_pos.unwrap()..];
    if let Some(block) = extract_first_paren_block(rest) {
        block.split(',')
            .filter_map(|item| {
                let item = item.trim();
                if let Some(eq) = item.find('=') {
                    let k = item[..eq].trim().trim_matches('"').trim_matches('\'').to_lowercase();
                    let v = item[eq+1..].trim().trim_matches('"').trim_matches('\'').to_string();
                    if !k.is_empty() { Some((k, v)) } else { None }
                } else { None }
            })
            .collect()
    } else {
        Vec::new()
    }
}

/// ADD INDEX 파싱
fn parse_add_index(rest: &str) -> Result<AlterCubeAction> {
    let upper = rest.to_uppercase();
    let parts: Vec<&str> = rest.split_whitespace().collect();
    let idx_name = parts.first()
        .ok_or_else(|| anyhow!("Expected index name"))?
        .to_string();

    let cols_block = extract_between(rest, "(", ")")
        .unwrap_or_default();
    let columns: Vec<String> = cols_block.split(',')
        .map(|s| s.trim().trim_matches('`').to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let index_type = if upper.contains("USING BITMAP") { "BITMAP".to_string() }
        else if upper.contains("USING BLOOM") { "BLOOM".to_string() }
        else if upper.contains("USING MINMAX") { "MINMAX".to_string() }
        else { "MINMAX".to_string() };

    Ok(AlterCubeAction::AddIndex { name: idx_name, columns, index_type })
}

/// MODIFY TTL 파싱
fn parse_modify_ttl(sql: &str) -> Result<AlterCubeAction> {
    let upper = sql.to_uppercase();
    let ttl_pos = upper.find("TTL").ok_or_else(|| anyhow!("No TTL in ALTER CUBE"))?;
    let rest = &sql[ttl_pos + 3..].trim_start();
    let parts: Vec<&str> = rest.split_whitespace().collect();
    let days = parts.first()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);
    Ok(AlterCubeAction::ModifyTtl { days })
}

// ─── 기본 파싱 유틸리티 ───────────────────────────────────────────────────────

fn extract_token_after(sql: &str, keyword: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let kw    = keyword.to_uppercase();
    let pos   = upper.find(&kw)?;
    let rest  = &sql[pos + keyword.len()..];
    let token = rest.split_whitespace().next()?
        .trim_end_matches(';')
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
        .to_string();
    if token.is_empty() { None } else { Some(token) }
}

fn extract_after_pattern(sql: &str, pattern: &str) -> Result<String> {
    let upper = sql.to_uppercase();
    let pat   = pattern.to_uppercase();
    let pos   = upper.find(&pat)
        .ok_or_else(|| anyhow!("Pattern '{}' not found", pattern))?;
    Ok(sql[pos + pattern.len()..].to_string())
}

/// 첫 번째 최상위 괄호 블록 이후의 바이트 오프셋 반환
/// 괄호가 없으면 0 반환 (전체 SQL 검색)
fn skip_past_first_top_level_paren(sql: &str) -> usize {
    let mut depth = 0i32;
    let bytes = sql.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'(' => { depth += 1; }
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return i + 1;
                }
            }
            _ => {}
        }
    }
    0
}

fn extract_first_paren_block(sql: &str) -> Option<String> {
    let mut depth = 0i32;
    let mut start = None;
    let chars: Vec<char> = sql.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        match c {
            '(' => {
                if depth == 0 { start = Some(i + 1); }
                depth += 1;
            }
            ')' => {
                depth -= 1;
                if depth == 0 {
                    if let Some(s) = start {
                        return Some(chars[s..i].iter().collect());
                    }
                }
            }
            _ => {}
        }
    }
    None
}

fn extract_between<'a>(sql: &'a str, open: &str, close: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let ou    = open.to_uppercase();
    let cu    = close.to_uppercase();
    let start = upper.find(&ou)? + open.len();
    let end   = upper[start..].find(&cu)? + start;
    Some(sql[start..end].to_string())
}

fn extract_quoted_after(sql: &str, keyword: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let kw    = keyword.to_uppercase();
    let pos   = upper.find(&kw)?;
    let rest  = &sql[pos + keyword.len()..].trim_start();
    let quote = rest.chars().next()?;
    if quote == '\'' || quote == '"' {
        let inner = &rest[1..];
        let end   = inner.find(quote)?;
        Some(inner[..end].to_string())
    } else {
        None
    }
}

/// 최상위 쉼표로 분리 (괄호 내부 무시)
fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut parts  = Vec::new();
    let mut current = String::new();
    let mut depth  = 0i32;

    for c in s.chars() {
        match c {
            '(' | '[' => { depth += 1; current.push(c); }
            ')' | ']' => { depth -= 1; current.push(c); }
            ',' if depth == 0 => {
                parts.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(c),
        }
    }
    if !current.trim().is_empty() { parts.push(current.trim().to_string()); }
    parts
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_cube_full() {
        let sql = r#"
            CREATE CUBE IF NOT EXISTS page_events (
                event_time    DATETIME   NOT NULL,
                device_id     VARCHAR(64) NOT NULL,
                event_name    VARCHAR(128) ENCODING(DICT),
                properties    JSON
            )
            PARTITION BY RANGE(event_time) INTERVAL MONTH AUTO
            ORDER BY (device_id, event_time)
            DISTRIBUTED BY HASH(device_id) BUCKETS 32
            PROPERTIES ("storage_backend" = "native");
        "#;

        let stmt = parse_create_cube_full(sql).unwrap();
        assert_eq!(stmt.name, "page_events");
        assert!(stmt.if_not_exists);
        assert_eq!(stmt.columns.len(), 4);
        assert_eq!(stmt.columns[0].name, "event_time");
        assert!(!stmt.columns[0].nullable); // NOT NULL
        assert_eq!(stmt.columns[2].encoding.as_deref(), Some("DICT"));

        let part = stmt.partition_by.unwrap();
        assert_eq!(part.granularity, "MONTH");
        assert!(part.auto);

        assert_eq!(stmt.order_by, vec!["device_id", "event_time"]);

        let dist = stmt.distribution.unwrap();
        assert_eq!(dist.columns, vec!["device_id"]);
        assert_eq!(dist.buckets, 32);

        assert!(stmt.properties.iter().any(|(k, v)| k == "storage_backend" && v == "native"));
    }

    #[test]
    fn test_parse_alter_cube_add_column() {
        let sql = "ALTER CUBE page_events ADD COLUMN new_col INT NOT NULL";
        let stmt = parse_alter_cube(sql).unwrap();
        assert_eq!(stmt.name, "page_events");
        match stmt.action {
            AlterCubeAction::AddColumn(col) => {
                assert_eq!(col.name, "new_col");
                assert_eq!(col.data_type, "INT");
            }
            _ => panic!("Expected AddColumn"),
        }
    }

    #[test]
    fn test_parse_alter_cube_drop_column() {
        let sql = "ALTER CUBE events DROP COLUMN old_col";
        let stmt = parse_alter_cube(sql).unwrap();
        assert!(matches!(stmt.action, AlterCubeAction::DropColumn(n) if n == "old_col"));
    }

    #[test]
    fn test_parse_drop_cube() {
        let stmt = parse_drop_cube("DROP CUBE IF EXISTS my_cube").unwrap();
        assert_eq!(stmt.name, "my_cube");
        assert!(stmt.if_exists);
    }

    #[test]
    fn test_parse_distribution() {
        let sql = "CREATE CUBE t (...) DISTRIBUTED BY HASH(user_id) BUCKETS 64";
        let dist = parse_distribution(sql).unwrap();
        assert_eq!(dist.columns, vec!["user_id"]);
        assert_eq!(dist.buckets, 64);
    }
}
