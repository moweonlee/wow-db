// T087: MySQL 결과셋 직렬화 — ColumnDef 패킷, Row 패킷, EOF 패킷 표준 포맷

use opensrv_mysql::{Column, ColumnFlags, ColumnType};
use shared::types::{DataType, Value};

// ─── 컬럼 빌더 ───────────────────────────────────────────────────────────────

/// WOW-DB DataType → opensrv_mysql Column 변환
pub fn build_columns(name: &str, col_type: ColumnType) -> Column {
    Column {
        table: String::new(),
        column: name.to_string(),
        coltype: col_type,
        colflags: ColumnFlags::empty(),
    }
}

/// shared::types::DataType → MySQL ColumnType 매핑
pub fn data_type_to_mysql(dt: &DataType) -> ColumnType {
    match dt {
        DataType::Boolean => ColumnType::MYSQL_TYPE_TINY,
        DataType::Int8    => ColumnType::MYSQL_TYPE_TINY,
        DataType::Int16   => ColumnType::MYSQL_TYPE_SHORT,
        DataType::Int32   => ColumnType::MYSQL_TYPE_LONG,
        DataType::Int64   => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::Float32 => ColumnType::MYSQL_TYPE_FLOAT,
        DataType::Float64 => ColumnType::MYSQL_TYPE_DOUBLE,
        DataType::String  => ColumnType::MYSQL_TYPE_VAR_STRING,
        DataType::DateTime | DataType::Date => ColumnType::MYSQL_TYPE_DATETIME,
        DataType::Json    => ColumnType::MYSQL_TYPE_JSON,
        DataType::Binary  => ColumnType::MYSQL_TYPE_BLOB,
        DataType::Nullable(inner) => data_type_to_mysql(inner),
    }
}

// ─── 행 인코딩 ────────────────────────────────────────────────────────────────

/// WOW-DB Value → MySQL text protocol 바이트 (옵션으로 래핑)
pub fn encode_row(row: &[Value]) -> Vec<Option<Vec<u8>>> {
    row.iter().map(encode_value).collect()
}

fn encode_value(val: &Value) -> Option<Vec<u8>> {
    match val {
        Value::Null       => None,
        Value::Boolean(b) => Some(if *b { b"1".to_vec() } else { b"0".to_vec() }),
        Value::Int64(n)   => Some(n.to_string().into_bytes()),
        Value::Float64(f) => Some(format!("{}", f).into_bytes()),
        Value::String(s)  => Some(s.as_bytes().to_vec()),
        Value::DateTime(dt) => Some(dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string().into_bytes()),
        Value::Bytes(b)   => Some(b.clone()),
    }
}

// ─── 결과셋 빌더 ─────────────────────────────────────────────────────────────

/// 컬럼 정의 + 데이터 행을 MySQL Text Protocol Row로 변환하는 유틸
pub struct ResultSetBuilder {
    pub columns: Vec<Column>,
    pub rows:    Vec<Vec<Option<Vec<u8>>>>,
}

impl ResultSetBuilder {
    pub fn new() -> Self {
        Self { columns: Vec::new(), rows: Vec::new() }
    }

    pub fn add_column(&mut self, name: &str, col_type: ColumnType) -> &mut Self {
        self.columns.push(build_columns(name, col_type));
        self
    }

    pub fn add_string_row(&mut self, vals: impl IntoIterator<Item = Option<String>>) -> &mut Self {
        let row: Vec<Option<Vec<u8>>> = vals.into_iter()
            .map(|v| v.map(|s| s.into_bytes()))
            .collect();
        self.rows.push(row);
        self
    }

    pub fn add_value_row(&mut self, vals: &[Value]) -> &mut Self {
        self.rows.push(encode_row(vals));
        self
    }
}

impl Default for ResultSetBuilder {
    fn default() -> Self { Self::new() }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use shared::types::DataType;

    #[test]
    fn test_data_type_to_mysql() {
        assert_eq!(data_type_to_mysql(&DataType::Int64),  ColumnType::MYSQL_TYPE_LONGLONG);
        assert_eq!(data_type_to_mysql(&DataType::String), ColumnType::MYSQL_TYPE_VAR_STRING);
        assert_eq!(data_type_to_mysql(&DataType::Json),   ColumnType::MYSQL_TYPE_JSON);
        assert_eq!(data_type_to_mysql(&DataType::Binary), ColumnType::MYSQL_TYPE_BLOB);
    }

    #[test]
    fn test_encode_value() {
        assert_eq!(encode_value(&Value::Null), None);
        assert_eq!(encode_value(&Value::Int64(42)), Some(b"42".to_vec()));
        assert_eq!(encode_value(&Value::Boolean(true)), Some(b"1".to_vec()));
        assert_eq!(encode_value(&Value::String("hello".to_string())), Some(b"hello".to_vec()));
    }

    #[test]
    fn test_result_set_builder() {
        let mut builder = ResultSetBuilder::new();
        builder.add_column("name", ColumnType::MYSQL_TYPE_VAR_STRING);
        builder.add_column("age",  ColumnType::MYSQL_TYPE_LONGLONG);

        builder.add_string_row([Some("Alice".to_string()), Some("30".to_string())]);
        builder.add_string_row([Some("Bob".to_string()), None]);

        assert_eq!(builder.columns.len(), 2);
        assert_eq!(builder.rows.len(), 2);
        assert_eq!(builder.rows[1][1], None);
    }

    #[test]
    fn test_encode_row() {
        let row = vec![
            Value::Int64(100),
            Value::String("test".to_string()),
            Value::Null,
        ];
        let encoded = encode_row(&row);
        assert_eq!(encoded[0], Some(b"100".to_vec()));
        assert_eq!(encoded[1], Some(b"test".to_vec()));
        assert_eq!(encoded[2], None);
    }
}
