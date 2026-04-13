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

pub fn execute_select(sql: &str) -> Result<SelectResult, String> {
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

    // FROM 절 — 테이블명
    let table_name = select.from.first()
        .and_then(|t| match &t.relation {
            TableFactor::Table { name, .. } => Some(name.to_string()),
            _ => None,
        })
        .unwrap_or_default();

    // FROM 없는 경우 (SELECT 1, SELECT NOW(), SELECT 'hello', ...)
    if select.from.is_empty() {
        let select_items: Vec<_> = select.projection.iter().collect();
        let empty_row: Row = std::collections::HashMap::new();
        let col_names: Vec<String> = select_items.iter().map(|i| select_item_header(i)).collect();
        let out_row: Vec<Value> = select_items.iter().map(|item| match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) |
            sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => eval_expr(expr, &empty_row).into_value(),
            _ => Value::Null,
        }).collect();
        return Ok(SelectResult { columns: col_names, rows: vec![out_row] });
    }

    // 전체 행 스캔
    let mut rows: Vec<Row> = MEM_STORE.scan(&table_name);

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
        project_rows(&select_items, &rows)?
    } else {
        execute_group_by(&select_items, &rows, &group_by_cols)?
    };

    // HAVING
    // (간단 구현: GROUP BY 결과 행에 대해 HAVING 조건 적용)

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

    // 집계 함수만 있는 경우 (GROUP BY 없이 COUNT(*) 등)
    let all_agg = !has_wildcard && items.iter().all(|i| match i {
        sqlparser::ast::SelectItem::UnnamedExpr(e) |
        sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => is_aggregate(e),
        _ => false,
    });
    if all_agg {
        let group_rows: Vec<&Row> = rows.iter().collect();
        let agg_row: Vec<Value> = items.iter().map(|item| match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) |
            sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => eval_aggregate(expr, &group_rows),
            _ => Value::Null,
        }).collect();
        return Ok(SelectResult { columns: col_names, rows: vec![agg_row] });
    }

    Ok(SelectResult { columns: col_names, rows: result })
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
            match row.get(&col) {
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
        "NOW" | "CURRENT_TIMESTAMP" => {
            EvalResult::Val(Value::String(chrono::Utc::now().to_rfc3339()))
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
            if let Some(e) = first_arg_expr(&f.args) {
                if let Some(n) = eval_expr(e, row).as_f64() {
                    return EvalResult::Val(serde_json::json!((n * 100.0).round() / 100.0));
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

fn eval_aggregate(expr: &sqlparser::ast::Expr, group_rows: &[&Row]) -> Value {
    let func = match expr { sqlparser::ast::Expr::Function(f) => f, _ => return Value::Null };
    let name = func.name.to_string().to_uppercase();

    let is_wildcard = matches!(&func.args, sqlparser::ast::FunctionArguments::List(list)
        if list.args.iter().any(|a| matches!(a, sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard))));

    let col_expr = first_arg_expr(&func.args);

    match name.as_str() {
        "COUNT" => {
            if is_wildcard || col_expr.is_none() {
                serde_json::json!(group_rows.len() as i64)
            } else {
                let cnt = group_rows.iter().filter(|r| {
                    let v = eval_expr(col_expr.unwrap(), r);
                    !matches!(v, EvalResult::Null | EvalResult::Val(Value::Null))
                }).count();
                serde_json::json!(cnt as i64)
            }
        }
        "SUM" => col_expr.map(|e| {
            let s: f64 = group_rows.iter().filter_map(|r| eval_expr(e, r).as_f64()).sum();
            serde_json::json!(s)
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
    for ob in order_by.iter().rev() {
        let col_name = expr_to_col_name(&ob.expr);
        let asc = ob.asc.unwrap_or(true);
        if let Some(idx) = result.columns.iter().position(|c| c == &col_name) {
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
        Number(s, _) => s.parse::<f64>().map(|n| serde_json::json!(n)).unwrap_or(Value::String(s.clone())),
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
        Value::String(s) => s.clone(),
        Value::Number(n) => format!("{:020.6}", n.as_f64().unwrap_or(0.0)),
        Value::Bool(b)   => b.to_string(),
        Value::Null      => String::new(),
        other            => other.to_string(),
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
        (Some(x), Some(y)) => EvalResult::Val(serde_json::json!(op(x, y))),
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

    #[test]
    fn test_select_with_where() {
        execute_insert("INSERT INTO sel_w_test (id, val) VALUES (1, 10), (2, 20), (3, 30)").unwrap();
        let r = execute_select("SELECT id, val FROM sel_w_test WHERE val > 15").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    fn test_group_by_count() {
        execute_insert("INSERT INTO grp_c_test (cat, val) VALUES ('a', 1), ('a', 2), ('b', 3)").unwrap();
        let r = execute_select("SELECT cat, COUNT(*) FROM grp_c_test GROUP BY cat").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    fn test_select_star() {
        execute_insert("INSERT INTO star_test (a, b) VALUES (1, 2), (3, 4)").unwrap();
        let r = execute_select("SELECT * FROM star_test").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.columns.len(), 2);
    }

    #[test]
    fn test_count_all() {
        execute_insert("INSERT INTO cnt_test (x) VALUES (1), (2), (3), (4), (5)").unwrap();
        let r = execute_select("SELECT COUNT(*) FROM cnt_test").unwrap();
        assert_eq!(r.rows[0][0], serde_json::json!(5i64));
    }

    #[test]
    fn test_order_by_limit() {
        execute_insert("INSERT INTO ord_test (n) VALUES (3), (1), (2)").unwrap();
        let r = execute_select("SELECT n FROM ord_test ORDER BY n ASC LIMIT 2").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], serde_json::json!(1.0));
    }
}
