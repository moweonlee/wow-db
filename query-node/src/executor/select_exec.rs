// SELECT 실행기 (인-메모리 스캔 + GROUP BY + ORDER BY + LIMIT)
// sqlparser 0.49 API 기준

use std::collections::BTreeMap;
use serde_json::Value;

use super::mem_store::{MEM_STORE, Row};

// ── 결과 타입 ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct SelectResult {
    pub columns: Vec<String>,
    pub rows:    Vec<Vec<Value>>,
}

// ── 공개 진입점 ─────────────────────────────────────────────────────────────

/// SQL SELECT 실행 (async — QN→SN 전체 스캔 또는 CN→SN 단일, 또는 MEM_STORE 폴백)
///
/// 우선순위:
///   1. SN 다중 노드 → scan_all_rows (모든 SN 스캔 후 QN에서 GROUP BY 머지)
///   2. SN 단일 노드 + CN 설정 → QN→CN→SN 파이프라인
///   3. MEM_STORE 폴백
pub async fn execute_select(sql: &str) -> Result<SelectResult, String> {
    let all_tables = quick_extract_all_tables(sql);

    // ── 우선순위 1: SN 다중 노드 — 전체 scan_all_rows → QN 집계 ─────────────
    // 샤딩된 데이터는 여러 SN에 분산되어 있으므로 모든 SN을 스캔하여 머지한 후
    // QN 에서 GROUP BY / ORDER BY / LIMIT 을 처리한다.
    // 멀티-테이블 FROM(j1, j2)도 지원: FROM의 모든 테이블을 SN에서 스캔한다.
    if !all_tables.is_empty() {
        let sn_count = {
            let pool = crate::storage_client::STORAGE.lock().await;
            pool.sn_count()
        };

        if sn_count > 0 {
            let mut any_sn_hit = false;
            for table in &all_tables {
                let sn_rows = {
                    let mut pool = crate::storage_client::STORAGE.lock().await;
                    pool.scan_all_rows(table).await
                };

                if let Some(rows) = sn_rows {
                    any_sn_hit = true;
                    if !rows.is_empty() {
                        // SN에 실제 데이터가 있으면 MEM_STORE를 SN 데이터로 교체
                        MEM_STORE.drop_table(table);
                        for row in &rows {
                            MEM_STORE.insert(table, row.clone());
                        }
                        tracing::debug!(table, total = rows.len(), "select: scan_all_rows merged");
                    }
                    // SN이 빈 결과를 반환해도 MEM_STORE 데이터는 유지 (INSERT 이중-기록 보장)
                }
            }
            if any_sn_hit {
                return execute_select_sync(sql);
            }
        }
    }

    // ── 우선순위 2: CN 경유 (SN 미연결 시 폴백) ──────────────────────────────
    let table_name = all_tables.into_iter().next();
    if let Some(ref table) = table_name {
        let cn_has_compute = crate::cn_client::CN.lock().await.has_compute();
        if cn_has_compute {
            let sn_endpoint = std::env::var("STORAGE_NODES")
                .unwrap_or_default()
                .split(',')
                .next()
                .unwrap_or("127.0.0.1:9060")
                .trim()
                .to_string();

            let cn_rows = {
                let mut cn = crate::cn_client::CN.lock().await;
                cn.execute_select(sql, table, &sn_endpoint).await
            };

            if let Some(result) = cn_rows {
                MEM_STORE.drop_table(table);
                for row in &result.rows {
                    let map: std::collections::HashMap<String, serde_json::Value> =
                        result.columns.iter().zip(row.iter())
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                    MEM_STORE.insert(table, map);
                }
                return execute_select_sync(sql);
            }
        }
    }

    // ── 우선순위 3: MEM_STORE 폴백 ───────────────────────────────────────────
    execute_select_sync(sql)
}

/// Extract ALL table names from FROM clause (handles comma-separated and multiline SQL).
/// Skips aliases. Returns empty vec if FROM not found.
fn quick_extract_all_tables(sql: &str) -> Vec<String> {
    let lower = sql.to_lowercase();

    // Find FROM keyword (handle leading space or newline)
    let from_start = [" from ", "\nfrom ", "\tfrom "]
        .iter()
        .find_map(|pat| lower.find(pat).map(|p| p + pat.len()));
    let after_from = match from_start {
        Some(p) => lower[p..].trim_start(),
        None    => return vec![],
    };

    // End of FROM clause — first keyword that starts a new clause
    let end_pos = [" where ", "\nwhere ", " group ", "\ngroup ",
                   " order ", "\norder ", " having ", "\nhaving ",
                   " limit ", "\nlimit ", " join ", "\njoin "]
        .iter()
        .filter_map(|kw| after_from.find(kw))
        .min()
        .unwrap_or(after_from.len());

    let from_clause = &after_from[..end_pos];

    // Each comma-separated segment: first word = table name (skip alias)
    from_clause.split(',')
        .filter_map(|seg| {
            let name: String = seg.trim()
                .split(|c: char| c.is_whitespace())
                .next()
                .unwrap_or("")
                .trim_matches('`')
                .to_string();
            if name.is_empty() || name.starts_with('(') { None } else { Some(name) }
        })
        .collect()
}

/// 동기 SELECT 실행 (MEM_STORE 기반)
fn execute_select_sync(sql: &str) -> Result<SelectResult, String> {
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;
    use sqlparser::ast::{Statement, SetExpr, TableFactor};

    let dialect = MySqlDialect {};
    let stmts = Parser::parse_sql(&dialect, sql)
        .map_err(|e| format!("Parse error: {e}"))?;

    let stmt = stmts.into_iter().next().ok_or("Empty query")?;

    let query = match stmt {
        Statement::Query(q) => q,
        _ => return Err("Not a SELECT statement".into()),
    };

    let select = match *query.body {
        SetExpr::Select(s) => s,
        _ => return Err("Unsupported query body".into()),
    };

    // FROM 절 — 테이블명 (alias 없이 실제 테이블명)
    let table_name = select.from.first()
        .and_then(|t| match &t.relation {
            TableFactor::Table { name, .. } => {
                // 마지막 파트 (schema.table → table)
                Some(name.0.last().map(|i| i.value.clone()).unwrap_or_default())
            }
            _ => None,
        })
        .unwrap_or_default();

    // FROM 없는 경우 (SELECT 1, SELECT NOW(), SELECT 'hello', ...)
    if select.from.is_empty() {
        let select_items: Vec<_> = select.projection.iter().collect();
        // Bare column identifier without FROM → error (e.g. SELECT $$, SELECT foo)
        // MySQL returns "Unknown column 'X' in 'field list'" for these.
        // MySQL 8 CLI probes with `select $$` and expects an error; returning a
        // result set causes it to skip mysql_store_result(), breaking subsequent queries.
        for item in &select_items {
            if let sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(id)) = item {
                if !id.value.starts_with('@') {
                    return Err(format!("Unknown column '{}' in 'field list'", id.value));
                }
            }
        }
        let empty_row: Row = std::collections::HashMap::new();
        let col_names: Vec<String> = select_items.iter().map(|i| select_item_header(i)).collect();
        let out_row: Vec<Value> = select_items.iter().map(|item| match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) |
            sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => eval_expr(expr, &empty_row).into_value(),
            _ => Value::Null,
        }).collect();
        return Ok(SelectResult { columns: col_names, rows: vec![out_row] });
    }

    // JOIN 처리: FROM 절의 테이블 및 JOIN 목록 수집
    let from_item = &select.from[0];
    let left_alias = match &from_item.relation {
        TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
        _ => None,
    };

    // 전체 행 스캔 (기본 테이블)
    let raw_base_rows: Vec<Row> = MEM_STORE.scan(&table_name);

    // 기본 테이블 alias가 있으면 alias.col 키도 추가
    let base_rows: Vec<Row> = if let Some(ref alias) = left_alias {
        raw_base_rows.into_iter().map(|mut row| {
            let keys: Vec<_> = row.keys().cloned().collect();
            for k in keys {
                let v = row[&k].clone();
                row.insert(format!("{}.{}", alias, k), v);
            }
            row
        }).collect()
    } else {
        raw_base_rows
    };

    // Extract equality conditions from WHERE for hash join (col1 = col2 pairs)
    let eq_conds: Vec<(String, String)> = select.selection.as_ref()
        .map(|e| extract_eq_conditions(e))
        .unwrap_or_default();

    // Split WHERE into AND-conjuncts for predicate pushdown
    let conjuncts: Vec<sqlparser::ast::Expr> = select.selection.as_ref()
        .map(|e| split_and_conjuncts(e))
        .unwrap_or_default();

    // Collect all columns referenced in the query for join pipeline column projection
    let needed_cols = collect_query_needed_cols(
        &select.projection,
        select.selection.as_ref(),
        &select.group_by,
        query.order_by.as_ref(),
        select.having.as_ref(),
    );

    // Macro-like helper: apply pushdown then project (inline closure)
    macro_rules! prune {
        ($r:expr) => {{
            let r = apply_partial_where($r, &conjuncts);
            if let Some(ref nc) = needed_cols { project_to_needed_cols(r, nc) } else { r }
        }};
    }

    // Apply pushdown + projection to base table before any joins
    let base_rows = prune!(base_rows);

    let mut rows: Vec<Row> = if from_item.joins.is_empty() {
        base_rows
    } else {
        let joined = apply_joins(base_rows, &from_item.joins, left_alias.as_deref())?;
        prune!(joined)
    };

    // Handle comma-separated FROM tables (FROM a, b, c → implicit hash/cross joins)
    for extra_from in &select.from[1..] {
        let extra_table = match &extra_from.relation {
            TableFactor::Table { name, .. } =>
                name.0.last().map(|i| i.value.clone()).unwrap_or_default(),
            _ => continue,
        };
        let extra_alias = match &extra_from.relation {
            TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
            _ => None,
        };
        let right_rows = MEM_STORE.scan(&extra_table);

        eprintln!("[join] table={extra_table} left_rows={} right_rows={}", rows.len(), right_rows.len());

        let known_cols: std::collections::HashSet<String> = rows.first()
            .map(|r| r.keys().cloned().collect())
            .unwrap_or_default();
        let right_cols: std::collections::HashSet<String> = right_rows.first()
            .map(|r| r.keys().cloned().collect())
            .unwrap_or_default();

        eprintln!("[join] eq_conds={eq_conds:?}");
        eprintln!("[join] known_cols sample: {:?}", known_cols.iter().take(5).collect::<Vec<_>>());
        eprintln!("[join] right_cols sample: {:?}", right_cols.iter().take(5).collect::<Vec<_>>());

        // Use hash join when an equality condition links known col to new table col
        let join_key = eq_conds.iter().find_map(|(a, b)| {
            if known_cols.contains(a) && right_cols.contains(b) {
                Some((a.clone(), b.clone()))
            } else if known_cols.contains(b) && right_cols.contains(a) {
                Some((b.clone(), a.clone()))
            } else {
                None
            }
        });
        eprintln!("[join] join_key={join_key:?}");

        rows = if let Some((lc, rc)) = join_key {
            hash_join_tables(rows, right_rows, &lc, &rc, extra_alias.as_deref())
        } else {
            cross_join_tables(rows, &right_rows, extra_alias.as_deref())
        };

        // Predicate pushdown + column projection after each join
        rows = prune!(rows);

        if !extra_from.joins.is_empty() {
            rows = apply_joins(rows, &extra_from.joins, extra_alias.as_deref())?;
            rows = prune!(rows);
        }
    }

    // WHERE 필터
    if let Some(ref where_expr) = select.selection {
        rows = rows.into_iter()
            .filter(|row| eval_expr(where_expr, row).as_bool())
            .collect();
    }

    // GROUP BY 컬럼 목록
    let group_by_cols: Vec<String> = match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) =>
            exprs.iter().map(expr_to_col_name).collect(),
        _ => vec![],
    };

    let select_items: Vec<_> = select.projection.iter().collect();

    let mut result = if group_by_cols.is_empty() {
        project_rows(&select_items, &rows, select.distinct.is_some())?
    } else {
        execute_group_by(&select_items, &rows, &group_by_cols)?
    };

    // HAVING — GROUP BY 결과 행에 HAVING 조건 적용
    if let Some(having_expr) = &select.having {
        let col_names = result.columns.clone();
        result.rows = result.rows.into_iter().filter(|row| {
            // 결과 행을 Row(HashMap)로 변환해 eval_expr 재사용
            let mut fake_row: Row = std::collections::HashMap::new();
            for (col, val) in col_names.iter().zip(row.iter()) {
                // alias 없는 집계 함수명도 컬럼명으로 등록
                fake_row.insert(col.clone(), val.clone());
                // COUNT(*) → count(*) 소문자도 등록
                fake_row.insert(col.to_lowercase(), val.clone());
            }
            eval_expr(having_expr, &fake_row).as_bool()
        }).collect();
    }

    // ORDER BY
    if let Some(order_by) = &query.order_by {
        apply_order_by(&mut result, &order_by.exprs);
    }

    // LIMIT
    if let Some(limit_expr) = &query.limit {
        if let Ok(n) = eval_literal_as_usize(limit_expr) {
            result.rows.truncate(n);
        }
    }

    Ok(result)
}

// ── JOIN 처리 ─────────────────────────────────────────────────────────────────

/// 기본 테이블 rows에 JOIN 목록을 순서대로 적용
fn apply_joins(
    left_rows: Vec<Row>,
    joins: &[sqlparser::ast::Join],
    _left_alias: Option<&str>,
) -> Result<Vec<Row>, String> {
    use sqlparser::ast::{Join, JoinOperator, JoinConstraint, TableFactor};

    let mut result = left_rows;

    for join in joins {
        // 오른쪽 테이블명 추출
        let right_table = match &join.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err("Unsupported JOIN table type".into()),
        };
        let right_alias = match &join.relation {
            TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
            _ => None,
        };
        let right_rows = MEM_STORE.scan(&right_table);

        // ON 조건 추출
        let on_expr = match &join.join_operator {
            JoinOperator::Inner(JoinConstraint::On(expr))      => Some(expr.clone()),
            JoinOperator::LeftOuter(JoinConstraint::On(expr))  => Some(expr.clone()),
            JoinOperator::RightOuter(JoinConstraint::On(expr)) => Some(expr.clone()),
            JoinOperator::CrossJoin => None,
            _ => None,
        };
        let is_left = matches!(&join.join_operator, JoinOperator::LeftOuter(_));

        // 두 테이블 중 컬럼명이 겹칠 경우 alias.col 형식으로 추가 등록
        let mut new_result: Vec<Row> = Vec::new();
        for left_row in &result {
            let mut matched = false;
            for right_row in &right_rows {
                // 두 행 병합: right alias가 있으면 alias.col 키로도 등록
                let mut merged = left_row.clone();
                for (k, v) in right_row.iter() {
                    // 기본 키
                    merged.entry(k.clone()).or_insert_with(|| v.clone());
                    // alias.col 형식도 등록
                    if let Some(alias) = &right_alias {
                        merged.insert(format!("{}.{}", alias, k), v.clone());
                    }
                }
                // ON 조건 평가
                let include = on_expr.as_ref().map(|e| eval_expr(e, &merged).as_bool()).unwrap_or(true);
                if include {
                    new_result.push(merged);
                    matched = true;
                }
            }
            // LEFT JOIN: 매칭 행이 없으면 left_row만으로 NULL 행 추가
            if is_left && !matched {
                new_result.push(left_row.clone());
            }
        }
        result = new_result;
    }
    Ok(result)
}

// ── 멀티-테이블 FROM 헬퍼 ────────────────────────────────────────────────────

/// Extract all col=col equality pairs from a WHERE expression (for hash join planning)
fn extract_eq_conditions(expr: &sqlparser::ast::Expr) -> Vec<(String, String)> {
    use sqlparser::ast::{Expr, BinaryOperator};
    match expr {
        Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
            match (simple_col_name(left), simple_col_name(right)) {
                (Some(l), Some(r)) => vec![(l, r)],
                _ => vec![],
            }
        }
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            let mut v = extract_eq_conditions(left);
            v.extend(extract_eq_conditions(right));
            v
        }
        Expr::Nested(inner) => extract_eq_conditions(inner),
        _ => vec![],
    }
}

fn simple_col_name(expr: &sqlparser::ast::Expr) -> Option<String> {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(i) => Some(i.value.trim_matches('`').to_string()),
        Expr::CompoundIdentifier(parts) =>
            parts.last().map(|i| i.value.trim_matches('`').to_string()),
        _ => None,
    }
}

/// Split a WHERE expression into AND-conjuncts for predicate pushdown.
fn split_and_conjuncts(expr: &sqlparser::ast::Expr) -> Vec<sqlparser::ast::Expr> {
    use sqlparser::ast::{Expr, BinaryOperator};
    match expr {
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            let mut v = split_and_conjuncts(left);
            v.extend(split_and_conjuncts(right));
            v
        }
        Expr::Nested(inner) => split_and_conjuncts(inner),
        _ => vec![expr.clone()],
    }
}

/// Collect all column identifiers referenced in an expression (lowercase).
fn collect_expr_cols(expr: &sqlparser::ast::Expr) -> std::collections::HashSet<String> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
    let mut cols = std::collections::HashSet::new();
    fn walk(e: &Expr, out: &mut std::collections::HashSet<String>) {
        match e {
            Expr::Identifier(i) => { out.insert(i.value.trim_matches('`').to_lowercase()); }
            Expr::CompoundIdentifier(parts) => {
                if let Some(last) = parts.last() {
                    out.insert(last.value.trim_matches('`').to_lowercase());
                }
            }
            Expr::BinaryOp { left, right, .. } => { walk(left, out); walk(right, out); }
            Expr::UnaryOp { expr, .. } => walk(expr, out),
            Expr::Between { expr, low, high, .. } => { walk(expr, out); walk(low, out); walk(high, out); }
            Expr::InList { expr, list, .. } => { walk(expr, out); list.iter().for_each(|e| walk(e, out)); }
            Expr::Like { expr, pattern, .. } => { walk(expr, out); walk(pattern, out); }
            Expr::Function(f) => {
                if let FunctionArguments::List(list) = &f.args {
                    for arg in &list.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg { walk(e, out); }
                    }
                }
            }
            Expr::Nested(inner) => walk(inner, out),
            Expr::Case { operand, conditions, results, else_result } => {
                if let Some(op) = operand { walk(op, out); }
                conditions.iter().for_each(|e| walk(e, out));
                results.iter().for_each(|e| walk(e, out));
                if let Some(er) = else_result { walk(er, out); }
            }
            _ => {}
        }
    }
    walk(expr, &mut cols);
    cols
}

/// Apply WHERE conjuncts that are fully evaluable with the current row set.
/// Conjuncts referencing columns not yet available are skipped (pass-through).
fn apply_partial_where(rows: Vec<Row>, conjuncts: &[sqlparser::ast::Expr]) -> Vec<Row> {
    if rows.is_empty() || conjuncts.is_empty() {
        return rows;
    }
    let available: std::collections::HashSet<String> = rows.first()
        .map(|r| r.keys().map(|k| k.to_lowercase()).collect())
        .unwrap_or_default();

    // Only apply conjuncts where every referenced column is present
    let applicable: Vec<&sqlparser::ast::Expr> = conjuncts.iter()
        .filter(|c| {
            let needed = collect_expr_cols(c);
            !needed.is_empty() && needed.iter().all(|col| {
                available.contains(col.as_str()) ||
                available.iter().any(|k| k.ends_with(&format!(".{col}")))
            })
        })
        .collect();

    if applicable.is_empty() {
        return rows;
    }

    let before = rows.len();
    let result: Vec<Row> = rows.into_iter()
        .filter(|row| applicable.iter().all(|c| eval_expr(c, row).as_bool()))
        .collect();
    if before != result.len() {
        eprintln!("[pushdown] {} conjunct(s): {} → {} rows", applicable.len(), before, result.len());
    }
    result
}

/// Collect all column names needed by the entire query for join pipeline projection.
/// Returns None if a wildcard is present (must keep all columns).
fn collect_query_needed_cols(
    projection: &[sqlparser::ast::SelectItem],
    where_expr: Option<&sqlparser::ast::Expr>,
    group_by: &sqlparser::ast::GroupByExpr,
    order_by: Option<&sqlparser::ast::OrderBy>,
    having: Option<&sqlparser::ast::Expr>,
) -> Option<std::collections::HashSet<String>> {
    use sqlparser::ast::{SelectItem, GroupByExpr};
    let mut cols = std::collections::HashSet::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return None,
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                cols.extend(collect_expr_cols(e));
            }
        }
    }
    if let Some(e) = where_expr { cols.extend(collect_expr_cols(e)); }
    if let GroupByExpr::Expressions(exprs, _) = group_by {
        for e in exprs { cols.extend(collect_expr_cols(e)); }
    }
    if let Some(ob) = order_by {
        for oe in &ob.exprs { cols.extend(collect_expr_cols(&oe.expr)); }
    }
    if let Some(e) = having { cols.extend(collect_expr_cols(e)); }
    Some(cols)
}

/// Project rows to only the columns needed by the query, reducing per-row memory.
/// Precomputes keep-set from first row to amortize the cost across all rows.
fn project_to_needed_cols(rows: Vec<Row>, needed: &std::collections::HashSet<String>) -> Vec<Row> {
    if needed.is_empty() || rows.is_empty() { return rows; }
    let keep: Vec<String> = rows.first()
        .map(|r| r.keys().filter(|k| {
            let kl = k.to_lowercase();
            needed.contains(&kl) || needed.iter().any(|n| kl.ends_with(&format!(".{n}")))
        }).cloned().collect())
        .unwrap_or_default();
    if keep.is_empty() { return rows; }
    let keep_set: std::collections::HashSet<&str> = keep.iter().map(|s| s.as_str()).collect();
    rows.into_iter().map(|mut row| { row.retain(|k, _| keep_set.contains(k.as_str())); row }).collect()
}

/// Hash join: build index on right[right_col], probe with left[left_col]. O(n+m).
fn hash_join_tables(
    left: Vec<Row>,
    right: Vec<Row>,
    left_col: &str,
    right_col: &str,
    right_alias: Option<&str>,
) -> Vec<Row> {
    tracing::debug!(
        left_rows = left.len(), right_rows = right.len(),
        left_col, right_col, "hash_join_tables: start"
    );
    eprintln!("[hash_join] left={} rows on {left_col}, right={} rows on {right_col}",
        left.len(), right.len());

    let mut right_index: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    for (i, row) in right.iter().enumerate() {
        if let Some(v) = row.get(right_col) {
            let k = json_to_sort_str(v);
            if i < 3 { eprintln!("[hash_join] right index[{i}]: {right_col}={v} → key={k:?}"); }
            right_index.entry(k).or_default().push(i);
        } else {
            if i < 3 { eprintln!("[hash_join] right row[{i}] missing col {right_col}: {:?}", row.keys().collect::<Vec<_>>()); }
        }
    }
    eprintln!("[hash_join] right_index size={}", right_index.len());

    let mut result = Vec::new();
    for (li, left_row) in left.iter().enumerate() {
        let key = left_row.get(left_col).map(|v| {
            let k = json_to_sort_str(v);
            if li < 3 { eprintln!("[hash_join] probe left[{li}]: {left_col}={v} → key={k:?} hit={}", right_index.contains_key(&k)); }
            k
        }).unwrap_or_else(|| {
            if li < 3 { eprintln!("[hash_join] left row[{li}] missing col {left_col}: {:?}", left_row.keys().collect::<Vec<_>>()); }
            String::new()
        });
        if let Some(idxs) = right_index.get(&key) {
            for &i in idxs {
                let right_row = &right[i];
                let mut merged = left_row.clone();
                for (k, v) in right_row.iter() {
                    merged.entry(k.clone()).or_insert_with(|| v.clone());
                    if let Some(alias) = right_alias {
                        merged.insert(format!("{}.{}", alias, k), v.clone());
                    }
                }
                result.push(merged);
            }
        }
    }
    eprintln!("[hash_join] result={} rows", result.len());
    result
}

/// Cross join fallback (used when no equality join key is found)
fn cross_join_tables(left: Vec<Row>, right: &[Row], right_alias: Option<&str>) -> Vec<Row> {
    let mut result = Vec::with_capacity(left.len() * right.len());
    for left_row in &left {
        for right_row in right {
            let mut merged = left_row.clone();
            for (k, v) in right_row.iter() {
                merged.entry(k.clone()).or_insert_with(|| v.clone());
                if let Some(alias) = right_alias {
                    merged.insert(format!("{}.{}", alias, k), v.clone());
                }
            }
            result.push(merged);
        }
    }
    result
}

// ── GROUP BY 집계 ─────────────────────────────────────────────────────────────

fn execute_group_by(
    select_items: &[&sqlparser::ast::SelectItem],
    rows: &[Row],
    group_keys: &[String],
) -> Result<SelectResult, String> {
    // 그룹화: BTreeMap 으로 정렬된 순서 보장
    let mut groups: BTreeMap<Vec<String>, Vec<usize>> = BTreeMap::new();
    for (i, row) in rows.iter().enumerate() {
        let key: Vec<String> = group_keys.iter()
            .map(|k| row.get(k).map(json_to_sort_str).unwrap_or_default())
            .collect();
        groups.entry(key).or_default().push(i);
    }

    // 컬럼 헤더
    let col_names: Vec<String> = select_items.iter().map(|i| select_item_header(i)).collect();
    let mut result_rows: Vec<Vec<Value>> = Vec::new();

    for (key_vals, row_indices) in &groups {
        let group_rows: Vec<&Row> = row_indices.iter().map(|&i| &rows[i]).collect();
        let mut out_row: Vec<Value> = Vec::new();

        for item in select_items {
            match item {
                sqlparser::ast::SelectItem::UnnamedExpr(expr) |
                sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                    if is_aggregate(expr) {
                        out_row.push(eval_aggregate(expr, &group_rows));
                    } else {
                        let col = expr_to_col_name(expr);
                        let pos = group_keys.iter().position(|k| k == &col);
                        if let Some(i) = pos {
                            let s = key_vals.get(i).cloned().unwrap_or_default();
                            // 원본 Value 복원 시도
                            let v = group_rows.first()
                                .and_then(|r| r.get(&col))
                                .cloned()
                                .unwrap_or(Value::String(s));
                            out_row.push(v);
                        } else {
                            let v = group_rows.first()
                                .and_then(|r| r.get(&col))
                                .cloned()
                                .unwrap_or(Value::Null);
                            out_row.push(v);
                        }
                    }
                }
                sqlparser::ast::SelectItem::Wildcard(_) => {
                    for (i, _k) in group_keys.iter().enumerate() {
                        out_row.push(Value::String(key_vals.get(i).cloned().unwrap_or_default()));
                    }
                }
                _ => out_row.push(Value::Null),
            }
        }
        result_rows.push(out_row);
    }

    Ok(SelectResult { columns: col_names, rows: result_rows })
}

// ── 단순 프로젝션 ─────────────────────────────────────────────────────────────

fn project_rows(
    items: &[&sqlparser::ast::SelectItem],
    rows: &[Row],
    distinct: bool,
) -> Result<SelectResult, String> {
    let has_wildcard = items.iter().any(|i| matches!(i, sqlparser::ast::SelectItem::Wildcard(_)));

    // 컬럼 헤더 (와일드카드는 첫 행에서 결정)
    let col_names: Vec<String> = if has_wildcard {
        if let Some(first) = rows.first() {
            let mut keys: Vec<String> = first.keys().cloned().collect();
            keys.sort();
            keys
        } else {
            vec!["*".to_string()]
        }
    } else {
        items.iter().map(|i| select_item_header(i)).collect()
    };

    let mut result: Vec<Vec<Value>> = Vec::new();
    for row in rows {
        let mut out: Vec<Value> = Vec::new();
        if has_wildcard {
            let mut keys: Vec<&String> = row.keys().collect();
            keys.sort();
            for k in keys { out.push(row[k].clone()); }
        } else {
            for item in items {
                match item {
                    sqlparser::ast::SelectItem::UnnamedExpr(expr) |
                    sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                        // SELECT COUNT(*) without GROUP BY = aggregate over all rows
                        if is_aggregate(expr) {
                            // will be handled below
                            out.push(Value::Null);
                        } else {
                            out.push(eval_expr(expr, row).into_value());
                        }
                    }
                    _ => out.push(Value::Null),
                }
            }
        }
        result.push(out);
    }

    // 집계 함수만 있는 경우 (GROUP BY 없이 COUNT(*), SUM, 복합 집계식 등)
    let any_agg = !has_wildcard && items.iter().any(|i| match i {
        sqlparser::ast::SelectItem::UnnamedExpr(e) |
        sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => contains_aggregate(e),
        _ => false,
    });
    if any_agg {
        let group_rows: Vec<&Row> = rows.iter().collect();
        let agg_row: Vec<Value> = items.iter().map(|item| match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) |
            sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => eval_aggregate_expr(expr, &group_rows),
            _ => Value::Null,
        }).collect();
        return Ok(SelectResult { columns: col_names, rows: vec![agg_row] });
    }

    // DISTINCT 중복 제거
    let final_rows = if distinct {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        result.into_iter().filter(|row| {
            let key = row.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("\x00");
            seen.insert(key)
        }).collect()
    } else {
        result
    };

    Ok(SelectResult { columns: col_names, rows: final_rows })
}

// ── 표현식 평가 ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum EvalResult {
    Val(Value),
    Bool(bool),
    Null,
}

impl EvalResult {
    fn as_bool(&self) -> bool {
        match self {
            EvalResult::Bool(b) => *b,
            EvalResult::Val(Value::Bool(b)) => *b,
            EvalResult::Null | EvalResult::Val(Value::Null) => false,
            EvalResult::Val(_) => true,
        }
    }
    fn into_value(self) -> Value {
        match self {
            EvalResult::Val(v) => v,
            EvalResult::Bool(b) => Value::Bool(b),
            EvalResult::Null => Value::Null,
        }
    }
    fn as_f64(&self) -> Option<f64> {
        match self {
            EvalResult::Val(Value::Number(n)) => n.as_f64(),
            EvalResult::Val(Value::String(s)) => s.parse().ok(),
            _ => None,
        }
    }
}

fn eval_expr(expr: &sqlparser::ast::Expr, row: &Row) -> EvalResult {
    use sqlparser::ast::Expr as E;
    use sqlparser::ast::BinaryOperator as BOp;

    match expr {
        E::Identifier(ident) => {
            let col = ident.value.trim_matches('`').to_string();
            match row.get(&col) {
                Some(v) => EvalResult::Val(v.clone()),
                None    => EvalResult::Null,
            }
        }
        E::CompoundIdentifier(parts) => {
            let col = parts.last().map(|i| i.value.trim_matches('`').to_string()).unwrap_or_default();
            // alias.col 형식: "alias.col" 키로 먼저 찾고, 없으면 col로 찾기
            let full_key = parts.iter().map(|i| i.value.trim_matches('`')).collect::<Vec<_>>().join(".");
            match row.get(&full_key).or_else(|| row.get(&col)) {
                Some(v) => EvalResult::Val(v.clone()),
                None    => EvalResult::Null,
            }
        }
        E::Value(v) => EvalResult::Val(sql_value_to_json(v)),
        E::UnaryOp { op, expr } => {
            use sqlparser::ast::UnaryOperator;
            let inner = eval_expr(expr, row);
            match op {
                UnaryOperator::Not   => EvalResult::Bool(!inner.as_bool()),
                UnaryOperator::Minus => inner.as_f64().map(|n| EvalResult::Val(serde_json::json!(-n))).unwrap_or(EvalResult::Null),
                _ => inner,
            }
        }
        E::BinaryOp { left, op, right } => {
            let l = eval_expr(left, row);
            let r = eval_expr(right, row);
            match op {
                BOp::Eq      => EvalResult::Bool(json_eq(&l.clone().into_value(), &r.clone().into_value())),
                BOp::NotEq   => EvalResult::Bool(!json_eq(&l.clone().into_value(), &r.clone().into_value())),
                BOp::Lt      => EvalResult::Bool(cmp_vals(&l, &r) == std::cmp::Ordering::Less),
                BOp::LtEq    => EvalResult::Bool(matches!(cmp_vals(&l, &r), std::cmp::Ordering::Less | std::cmp::Ordering::Equal)),
                BOp::Gt      => EvalResult::Bool(cmp_vals(&l, &r) == std::cmp::Ordering::Greater),
                BOp::GtEq    => EvalResult::Bool(matches!(cmp_vals(&l, &r), std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)),
                BOp::And     => EvalResult::Bool(l.as_bool() && r.as_bool()),
                BOp::Or      => EvalResult::Bool(l.as_bool() || r.as_bool()),
                BOp::Plus    => numeric_op(&l, &r, |a, b| a + b),
                BOp::Minus   => numeric_op(&l, &r, |a, b| a - b),
                BOp::Multiply => numeric_op(&l, &r, |a, b| a * b),
                BOp::Divide  => numeric_op(&l, &r, |a, b| if b == 0.0 { 0.0 } else { a / b }),
                BOp::StringConcat => {
                    let ls = json_to_str(&l.into_value());
                    let rs = json_to_str(&r.into_value());
                    EvalResult::Val(Value::String(ls + &rs))
                }
                _ => EvalResult::Null,
            }
        }
        E::Like { expr, pattern, negated, .. } => {
            let s = json_to_str(&eval_expr(expr, row).into_value());
            let p = json_to_str(&eval_expr(pattern, row).into_value());
            let matched = like_match(&s, &p);
            EvalResult::Bool(if *negated { !matched } else { matched })
        }
        E::IsNull(e)    => {
            let v = eval_expr(e, row);
            EvalResult::Bool(matches!(v, EvalResult::Null | EvalResult::Val(Value::Null)))
        }
        E::IsNotNull(e) => {
            let v = eval_expr(e, row);
            EvalResult::Bool(!matches!(v, EvalResult::Null | EvalResult::Val(Value::Null)))
        }
        E::InList { expr, list, negated } => {
            let v = eval_expr(expr, row).into_value();
            let found = list.iter().any(|item| json_eq(&v, &eval_expr(item, row).into_value()));
            EvalResult::Bool(if *negated { !found } else { found })
        }
        E::Between { expr, low, high, negated } => {
            let v  = eval_expr(expr, row);
            let lo = eval_expr(low, row);
            let hi = eval_expr(high, row);
            let in_range = !matches!(cmp_vals(&v, &lo), std::cmp::Ordering::Less)
                        && !matches!(cmp_vals(&v, &hi), std::cmp::Ordering::Greater);
            EvalResult::Bool(if *negated { !in_range } else { in_range })
        }
        E::Nested(inner) => eval_expr(inner, row),
        E::Function(f)   => eval_function(f, row),
        E::Case { operand, conditions, results, else_result } => {
            if let Some(op) = operand {
                let op_val = eval_expr(op, row).into_value();
                for (cond, res) in conditions.iter().zip(results.iter()) {
                    if json_eq(&eval_expr(cond, row).into_value(), &op_val) {
                        return eval_expr(res, row);
                    }
                }
            } else {
                for (cond, res) in conditions.iter().zip(results.iter()) {
                    if eval_expr(cond, row).as_bool() {
                        return eval_expr(res, row);
                    }
                }
            }
            else_result.as_ref().map(|e| eval_expr(e, row)).unwrap_or(EvalResult::Null)
        }
        _ => EvalResult::Null,
    }
}

// ── 함수 평가 ─────────────────────────────────────────────────────────────────

/// FunctionArguments::List 에서 첫 번째 Expr 인수 추출
fn first_arg_expr(args: &sqlparser::ast::FunctionArguments) -> Option<&sqlparser::ast::Expr> {
    match args {
        sqlparser::ast::FunctionArguments::List(list) => {
            list.args.iter().find_map(|a| match a {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => Some(e),
                _ => None,
            })
        }
        _ => None,
    }
}

fn eval_function(f: &sqlparser::ast::Function, row: &Row) -> EvalResult {
    let name = f.name.to_string().to_uppercase();
    match name.as_str() {
        "UPPER" => first_arg_expr(&f.args).map(|e| {
            EvalResult::Val(Value::String(json_to_str(&eval_expr(e, row).into_value()).to_uppercase()))
        }).unwrap_or(EvalResult::Null),
        "LOWER" => first_arg_expr(&f.args).map(|e| {
            EvalResult::Val(Value::String(json_to_str(&eval_expr(e, row).into_value()).to_lowercase()))
        }).unwrap_or(EvalResult::Null),
        "LENGTH" | "CHAR_LENGTH" => first_arg_expr(&f.args).map(|e| {
            EvalResult::Val(serde_json::json!(json_to_str(&eval_expr(e, row).into_value()).len()))
        }).unwrap_or(EvalResult::Null),
        "NOW" | "CURRENT_TIMESTAMP" | "SYSDATE" => {
            // MySQL 호환 datetime 포맷: 'YYYY-MM-DD HH:MM:SS'
            let now = chrono::Utc::now();
            EvalResult::Val(Value::String(now.format("%Y-%m-%d %H:%M:%S").to_string()))
        }
        "COALESCE" => {
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                for arg in &list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) = arg {
                        let v = eval_expr(e, row);
                        if !matches!(&v, EvalResult::Null | EvalResult::Val(Value::Null)) {
                            return v;
                        }
                    }
                }
            }
            EvalResult::Null
        }
        "IF" => {
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                let args: Vec<&sqlparser::ast::Expr> = list.args.iter().filter_map(|a| match a {
                    sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
                }).collect();
                if args.len() >= 3 {
                    if eval_expr(args[0], row).as_bool() {
                        return eval_expr(args[1], row);
                    } else {
                        return eval_expr(args[2], row);
                    }
                }
            }
            EvalResult::Null
        }
        "CONCAT" => {
            let mut s = String::new();
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                for arg in &list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) = arg {
                        s.push_str(&json_to_str(&eval_expr(e, row).into_value()));
                    }
                }
            }
            EvalResult::Val(Value::String(s))
        }
        "ROUND" => {
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                let exprs: Vec<&sqlparser::ast::Expr> = list.args.iter().filter_map(|a| match a {
                    sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
                }).collect();
                if let Some(n) = exprs.first().and_then(|e| eval_expr(e, row).as_f64()) {
                    let decimals = exprs.get(1).and_then(|e| eval_expr(e, row).as_f64()).unwrap_or(0.0) as i32;
                    let factor = 10f64.powi(decimals);
                    let result = (n * factor).round() / factor;
                    // 소수점 없는 경우 정수 반환
                    if decimals <= 0 && result.fract() == 0.0 {
                        return EvalResult::Val(serde_json::json!(result as i64));
                    }
                    return EvalResult::Val(serde_json::json!(result));
                }
            }
            EvalResult::Null
        }
        "DATE_FORMAT" => {
            // DATE_FORMAT(date, format) — 간단한 포맷 지원
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                let exprs: Vec<&sqlparser::ast::Expr> = list.args.iter().filter_map(|a| match a {
                    sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
                }).collect();
                if exprs.len() >= 2 {
                    let date_str = json_to_str(&eval_expr(exprs[0], row).into_value());
                    let fmt_str = json_to_str(&eval_expr(exprs[1], row).into_value());
                    // 날짜 앞 10자만 추출 (YYYY-MM-DD)
                    let date_part = if date_str.len() >= 10 { &date_str[..10] } else { &date_str };
                    let parts: Vec<&str> = date_part.split('-').collect();
                    let (y, m, d) = (
                        parts.get(0).copied().unwrap_or(""),
                        parts.get(1).copied().unwrap_or(""),
                        parts.get(2).copied().unwrap_or(""),
                    );
                    let result = fmt_str
                        .replace("%Y", y).replace("%m", m).replace("%d", d)
                        .replace("%y", if y.len() >= 4 { &y[2..] } else { y });
                    return EvalResult::Val(Value::String(result));
                }
            }
            EvalResult::Null
        }
        _ => EvalResult::Null,
    }
}

// ── 집계 함수 ─────────────────────────────────────────────────────────────────

fn is_aggregate(expr: &sqlparser::ast::Expr) -> bool {
    match expr {
        sqlparser::ast::Expr::Function(f) => {
            let name = f.name.to_string().to_uppercase();
            matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
        }
        _ => false,
    }
}

/// True if expr contains any aggregate function anywhere (e.g. 100 * SUM(...) / SUM(...))
fn contains_aggregate(expr: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Function(_) => is_aggregate(expr),
        Expr::BinaryOp { left, right, .. } => contains_aggregate(left) || contains_aggregate(right),
        Expr::UnaryOp { expr, .. } => contains_aggregate(expr),
        Expr::Nested(inner) => contains_aggregate(inner),
        _ => false,
    }
}

/// Evaluate an expression that may contain embedded aggregate functions.
/// Used for no-GROUP-BY aggregates like `100.0 * SUM(a) / SUM(b)`.
fn eval_aggregate_expr(expr: &sqlparser::ast::Expr, group_rows: &[&Row]) -> Value {
    use sqlparser::ast::{Expr, BinaryOperator};
    match expr {
        // Direct aggregate → existing eval_aggregate
        Expr::Function(_) if is_aggregate(expr) => eval_aggregate(expr, group_rows),
        // Arithmetic with embedded aggregates
        Expr::BinaryOp { left, op, right } => {
            let lv = EvalResult::Val(eval_aggregate_expr(left, group_rows));
            let rv = EvalResult::Val(eval_aggregate_expr(right, group_rows));
            match op {
                BinaryOperator::Plus     => numeric_op(&lv, &rv, |a, b| a + b),
                BinaryOperator::Minus    => numeric_op(&lv, &rv, |a, b| a - b),
                BinaryOperator::Multiply => numeric_op(&lv, &rv, |a, b| a * b),
                BinaryOperator::Divide   => numeric_op(&lv, &rv, |a, b| if b == 0.0 { 0.0 } else { a / b }),
                _ => EvalResult::Null,
            }.into_value()
        }
        Expr::Nested(inner) => eval_aggregate_expr(inner, group_rows),
        Expr::Value(v) => sql_value_to_json(v),
        // Scalar expr: evaluate on first row
        other => group_rows.first()
            .map(|r| eval_expr(other, r).into_value())
            .unwrap_or(Value::Null),
    }
}

fn eval_aggregate(expr: &sqlparser::ast::Expr, group_rows: &[&Row]) -> Value {
    let func = match expr { sqlparser::ast::Expr::Function(f) => f, _ => return Value::Null };
    let name = func.name.to_string().to_uppercase();

    let is_wildcard = matches!(&func.args, sqlparser::ast::FunctionArguments::List(list)
        if list.args.iter().any(|a| matches!(a, sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard))));

    let col_expr = first_arg_expr(&func.args);

    // COUNT(DISTINCT col) 여부 확인
    let is_distinct = matches!(&func.args, sqlparser::ast::FunctionArguments::List(list)
        if list.duplicate_treatment == Some(sqlparser::ast::DuplicateTreatment::Distinct));

    match name.as_str() {
        "COUNT" => {
            if is_wildcard || col_expr.is_none() {
                serde_json::json!(group_rows.len() as i64)
            } else if is_distinct {
                // COUNT(DISTINCT col): 유니크 비-NULL 값 카운트
                let mut seen = std::collections::HashSet::new();
                let cnt = group_rows.iter().filter(|r| {
                    let v = eval_expr(col_expr.unwrap(), r).into_value();
                    if matches!(&v, Value::Null) { return false; }
                    let key = json_to_sort_str(&v);
                    seen.insert(key)
                }).count();
                serde_json::json!(cnt as i64)
            } else {
                let cnt = group_rows.iter().filter(|r| {
                    let v = eval_expr(col_expr.unwrap(), r);
                    !matches!(v, EvalResult::Null | EvalResult::Val(Value::Null))
                }).count();
                serde_json::json!(cnt as i64)
            }
        }
        "SUM" => col_expr.map(|e| {
            // as_f64() handles both Value::Number and Value::String("3.14") column values
            let vals: Vec<f64> = group_rows.iter().filter_map(|r| eval_expr(e, r).as_f64()).collect();
            if vals.is_empty() { Value::Null }
            else { serde_json::json!(vals.iter().sum::<f64>()) }
        }).unwrap_or(Value::Null),
        "AVG" => col_expr.map(|e| {
            let vals: Vec<f64> = group_rows.iter().filter_map(|r| eval_expr(e, r).as_f64()).collect();
            if vals.is_empty() { Value::Null }
            else { serde_json::json!(vals.iter().sum::<f64>() / vals.len() as f64) }
        }).unwrap_or(Value::Null),
        "MIN" => col_expr.map(|e|
            group_rows.iter().filter_map(|r| eval_expr(e, r).as_f64()).reduce(f64::min)
                .map(|v| serde_json::json!(v)).unwrap_or(Value::Null)
        ).unwrap_or(Value::Null),
        "MAX" => col_expr.map(|e|
            group_rows.iter().filter_map(|r| eval_expr(e, r).as_f64()).reduce(f64::max)
                .map(|v| serde_json::json!(v)).unwrap_or(Value::Null)
        ).unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

// ── ORDER BY ─────────────────────────────────────────────────────────────────

fn apply_order_by(result: &mut SelectResult, order_by: &[sqlparser::ast::OrderByExpr]) {
    // 여러 ORDER BY 키를 순서대로 처리 (primary, secondary, ...)
    // stable_sort를 반복해 multi-key sort 구현
    for ob in order_by.iter().rev() {
        let asc = ob.asc.unwrap_or(true);
        // 컬럼 인덱스 결정: 숫자 리터럴(위치) 또는 컬럼명/별칭
        let idx_opt = match &ob.expr {
            sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
                n.parse::<usize>().ok().and_then(|p| if p > 0 { Some(p-1) } else { None })
            }
            other => {
                let col_name = expr_to_col_name(other);
                result.columns.iter().position(|c| c.to_lowercase() == col_name.to_lowercase())
                    .or_else(|| {
                        // alias 없이 집계식 그대로도 시도
                        result.columns.iter().position(|c| c == &col_name)
                    })
            }
        };
        if let Some(idx) = idx_opt {
            result.rows.sort_by(|a, b| {
                let ord = cmp_json(
                    a.get(idx).unwrap_or(&Value::Null),
                    b.get(idx).unwrap_or(&Value::Null),
                );
                if asc { ord } else { ord.reverse() }
            });
        }
    }
}

// ── 유틸리티 ──────────────────────────────────────────────────────────────────

pub fn expr_to_col_name(expr: &sqlparser::ast::Expr) -> String {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(i) => i.value.trim_matches('`').to_string(),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.trim_matches('`').to_string()).unwrap_or_default(),
        Expr::Function(f) => {
            let args_str = match &f.args {
                sqlparser::ast::FunctionArguments::List(list) =>
                    list.args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(","),
                _ => String::new(),
            };
            format!("{}({})", f.name, args_str)
        }
        _ => expr.to_string(),
    }
}

pub fn select_item_header(item: &sqlparser::ast::SelectItem) -> String {
    match item {
        sqlparser::ast::SelectItem::UnnamedExpr(e)             => expr_to_col_name(e),
        sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        sqlparser::ast::SelectItem::Wildcard(_)                => "*".to_string(),
        sqlparser::ast::SelectItem::QualifiedWildcard(name, _) => format!("{}.*", name),
    }
}

pub fn sql_value_to_json(v: &sqlparser::ast::Value) -> Value {
    use sqlparser::ast::Value::*;
    match v {
        Number(s, _) => {
            // 정수 우선 파싱, 실패 시 f64
            if let Ok(n) = s.parse::<i64>() {
                serde_json::json!(n)
            } else if let Ok(n) = s.parse::<f64>() {
                serde_json::json!(n)
            } else {
                Value::String(s.clone())
            }
        }
        SingleQuotedString(s) | DoubleQuotedString(s) =>
            serde_json::from_str::<Value>(s).unwrap_or(Value::String(s.clone())),
        Boolean(b) => Value::Bool(*b),
        Null        => Value::Null,
        _           => Value::String(v.to_string()),
    }
}

fn json_to_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null      => String::new(),
        other            => other.to_string(),
    }
}

fn json_to_sort_str(v: &Value) -> String {
    match v {
        Value::Number(n) => format!("{:020.6}", n.as_f64().unwrap_or(0.0)),
        Value::String(s) => {
            // Normalize numeric strings to same key format as numbers.
            // Needed when SN returns integers as JSON strings but MEM_STORE has serde_json Numbers.
            if let Ok(n) = s.parse::<f64>() {
                format!("{:020.6}", n)
            } else {
                s.clone()
            }
        }
        Value::Bool(b) => b.to_string(),
        Value::Null    => String::new(),
        other          => other.to_string(),
    }
}

fn json_eq(a: &Value, b: &Value) -> bool {
    // 숫자 비교는 f64 로
    match (a, b) {
        (Value::Number(na), Value::Number(nb)) => na.as_f64() == nb.as_f64(),
        (Value::String(sa), Value::Number(nb)) => sa.parse::<f64>().ok() == nb.as_f64(),
        (Value::Number(na), Value::String(sb)) => na.as_f64() == sb.parse::<f64>().ok(),
        _ => a == b || a.to_string() == b.to_string(),
    }
}

fn cmp_vals(a: &EvalResult, b: &EvalResult) -> std::cmp::Ordering {
    cmp_json(&a.clone().into_value(), &b.clone().into_value())
}

fn cmp_json(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Number(na), Value::Number(nb)) =>
            na.as_f64().unwrap_or(0.0).partial_cmp(&nb.as_f64().unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(sa), Value::String(sb)) => sa.cmp(sb),
        (Value::Bool(ba),   Value::Bool(bb))   => ba.cmp(bb),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _)  => std::cmp::Ordering::Less,
        (_,  Value::Null) => std::cmp::Ordering::Greater,
        _ => a.to_string().cmp(&b.to_string()),
    }
}

fn numeric_op(a: &EvalResult, b: &EvalResult, op: impl Fn(f64, f64) -> f64) -> EvalResult {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => {
            let result = op(x, y);
            // 정수 피연산자 + 정수 결과이면 정수로 반환
            let a_is_int = matches!(a, EvalResult::Val(Value::Number(n)) if n.is_i64() || n.is_u64());
            let b_is_int = matches!(b, EvalResult::Val(Value::Number(n)) if n.is_i64() || n.is_u64());
            if a_is_int && b_is_int && result.fract() == 0.0 && result.abs() < 9007199254740992.0 {
                EvalResult::Val(serde_json::json!(result as i64))
            } else {
                EvalResult::Val(serde_json::json!(result))
            }
        }
        _ => EvalResult::Null,
    }
}

fn like_match(s: &str, pattern: &str) -> bool {
    let re_str = "(?i)^".to_string()
        + &regex::escape(pattern)
            .replace('%', ".*")
            .replace('_', ".")
        + "$";
    regex::Regex::new(&re_str).map(|re| re.is_match(s)).unwrap_or(false)
}

fn eval_literal_as_usize(expr: &sqlparser::ast::Expr) -> Result<usize, ()> {
    match expr {
        sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(s, _)) => s.parse().map_err(|_| ()),
        _ => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::insert_exec::execute_insert;

    #[tokio::test]
    async fn test_select_with_where() {
        execute_insert("INSERT INTO sel_w_test (id, val) VALUES (1, 10), (2, 20), (3, 30)", None, None).await.unwrap();
        let r = execute_select("SELECT id, val FROM sel_w_test WHERE val > 15").await.unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_group_by_count() {
        execute_insert("INSERT INTO grp_c_test (cat, val) VALUES ('a', 1), ('a', 2), ('b', 3)", None, None).await.unwrap();
        let r = execute_select("SELECT cat, COUNT(*) FROM grp_c_test GROUP BY cat").await.unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_select_star() {
        execute_insert("INSERT INTO star_test (a, b) VALUES (1, 2), (3, 4)", None, None).await.unwrap();
        let r = execute_select("SELECT * FROM star_test").await.unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.columns.len(), 2);
    }

    #[tokio::test]
    async fn test_count_all() {
        execute_insert("INSERT INTO cnt_test (x) VALUES (1), (2), (3), (4), (5)", None, None).await.unwrap();
        let r = execute_select("SELECT COUNT(*) FROM cnt_test").await.unwrap();
        assert_eq!(r.rows[0][0], serde_json::json!(5i64));
    }

    #[tokio::test]
    async fn test_order_by_limit() {
        execute_insert("INSERT INTO ord_test (n) VALUES (3), (1), (2)", None, None).await.unwrap();
        let r = execute_select("SELECT n FROM ord_test ORDER BY n ASC LIMIT 2").await.unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], serde_json::json!(1));
    }
}
