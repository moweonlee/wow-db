// T063: Row-to-Columnar 변환 — Kafka/INSERT 배치 → Arrow2 RecordBatch, 파티션 라우팅

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use arrow2::{
    array::{Array, Int64Array, MutableArray, MutablePrimitiveArray, MutableUtf8Array, Utf8Array},
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
};
use serde_json::Value;

// ─── 컬럼 스키마 정의 ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Int64,
    Float64,
    Utf8,
    Boolean,
}

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name:     String,
    pub col_type: ColumnType,
    pub nullable: bool,
}

/// Row-to-Columnar 변환기
pub struct RowToColumnar {
    schema: Vec<ColumnSchema>,
}

impl RowToColumnar {
    pub fn new(schema: Vec<ColumnSchema>) -> Self {
        Self { schema }
    }

    /// JSON Row 배치를 Arrow2 Chunk로 변환
    pub fn convert(
        &self,
        rows: &[HashMap<String, Value>],
    ) -> Result<Chunk<Box<dyn Array>>> {
        if rows.is_empty() {
            return Ok(Chunk::new(Vec::new()));
        }

        let mut arrays: Vec<Box<dyn Array>> = Vec::new();

        for col_schema in &self.schema {
            let arr = self.build_column(rows, col_schema)?;
            arrays.push(arr);
        }

        Ok(Chunk::new(arrays))
    }

    fn build_column(
        &self,
        rows: &[HashMap<String, Value>],
        schema: &ColumnSchema,
    ) -> Result<Box<dyn Array>> {
        match schema.col_type {
            ColumnType::Int64 => {
                let mut arr: MutablePrimitiveArray<i64> = MutablePrimitiveArray::new();
                for row in rows {
                    match row.get(&schema.name) {
                        Some(Value::Number(n)) => {
                            if let Some(i) = n.as_i64() {
                                arr.push(Some(i));
                            } else if let Some(f) = n.as_f64() {
                                arr.push(Some(f as i64));
                            } else {
                                if schema.nullable { arr.push(None); }
                                else { return Err(anyhow!("Non-nullable column '{}' has null", schema.name)); }
                            }
                        }
                        Some(Value::String(s)) => {
                            match s.parse::<i64>() {
                                Ok(i)  => arr.push(Some(i)),
                                Err(_) => {
                                    if schema.nullable { arr.push(None); }
                                    else { return Err(anyhow!("Cannot parse '{}' as i64 for column '{}'", s, schema.name)); }
                                }
                            }
                        }
                        None | Some(Value::Null) => {
                            if schema.nullable { arr.push(None); }
                            else { return Err(anyhow!("Non-nullable column '{}' is missing", schema.name)); }
                        }
                        other => {
                            return Err(anyhow!("Unexpected value {:?} for i64 column '{}'", other, schema.name));
                        }
                    }
                }
                Ok(arr.as_box())
            }

            ColumnType::Float64 => {
                let mut arr: MutablePrimitiveArray<f64> = MutablePrimitiveArray::new();
                for row in rows {
                    match row.get(&schema.name) {
                        Some(Value::Number(n)) => {
                            arr.push(n.as_f64());
                        }
                        None | Some(Value::Null) => {
                            if schema.nullable { arr.push(None); }
                            else { return Err(anyhow!("Non-nullable column '{}' is missing", schema.name)); }
                        }
                        other => {
                            return Err(anyhow!("Unexpected value {:?} for f64 column '{}'", other, schema.name));
                        }
                    }
                }
                Ok(arr.as_box())
            }

            ColumnType::Utf8 => {
                let mut arr: MutableUtf8Array<i32> = MutableUtf8Array::new();
                for row in rows {
                    match row.get(&schema.name) {
                        Some(Value::String(s)) => arr.push(Some(s.as_str())),
                        Some(Value::Number(n)) => arr.push(Some(&n.to_string())),
                        Some(Value::Bool(b))   => arr.push(Some(if *b { "true" } else { "false" })),
                        None | Some(Value::Null) => {
                            if schema.nullable { arr.push::<&str>(None); }
                            else { return Err(anyhow!("Non-nullable column '{}' is missing", schema.name)); }
                        }
                        other => arr.push(Some(&format!("{:?}", other))),
                    }
                }
                Ok(arr.as_box())
            }

            ColumnType::Boolean => {
                use arrow2::array::{MutableBooleanArray, MutableArray};
                let mut arr = MutableBooleanArray::new();
                for row in rows {
                    match row.get(&schema.name) {
                        Some(Value::Bool(b))   => arr.push(Some(*b)),
                        Some(Value::Number(n)) => arr.push(Some(n.as_i64().unwrap_or(0) != 0)),
                        None | Some(Value::Null) => {
                            if schema.nullable { arr.push(None); }
                            else { return Err(anyhow!("Non-nullable column '{}' is missing", schema.name)); }
                        }
                        _ => {
                            if schema.nullable { arr.push(None); }
                            else { return Err(anyhow!("Cannot convert to bool for column '{}'", schema.name)); }
                        }
                    }
                }
                Ok(arr.as_box())
            }
        }
    }

    /// Arrow2 Schema 반환
    pub fn arrow_schema(&self) -> Schema {
        let fields: Vec<Field> = self.schema.iter().map(|s| {
            let dt = match s.col_type {
                ColumnType::Int64   => DataType::Int64,
                ColumnType::Float64 => DataType::Float64,
                ColumnType::Utf8    => DataType::Utf8,
                ColumnType::Boolean => DataType::Boolean,
            };
            Field::new(&s.name, dt, s.nullable)
        }).collect();
        Schema::from(fields)
    }
}

// ─── 파티션 라우터 ────────────────────────────────────────────────────────────

/// 파티션 컬럼 기준으로 행을 파티션 버킷으로 분류
pub struct PartitionRouter {
    partition_col: String,
    bucket_count:  u32,
}

impl PartitionRouter {
    pub fn new(partition_col: String, bucket_count: u32) -> Self {
        Self { partition_col, bucket_count }
    }

    /// 각 행의 파티션 버킷 ID 계산
    /// 반환: (bucket_id → row_indices) 맵
    pub fn route(
        &self,
        rows: &[HashMap<String, Value>],
    ) -> HashMap<u32, Vec<usize>> {
        let mut buckets: HashMap<u32, Vec<usize>> = HashMap::new();

        for (idx, row) in rows.iter().enumerate() {
            let bucket = self.compute_bucket(row);
            buckets.entry(bucket).or_default().push(idx);
        }

        buckets
    }

    fn compute_bucket(&self, row: &HashMap<String, Value>) -> u32 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let val = row.get(&self.partition_col)
            .cloned()
            .unwrap_or(Value::Null);

        let mut hasher = DefaultHasher::new();
        match &val {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() { i.hash(&mut hasher); }
                else if let Some(f) = n.as_f64() {
                    (f.to_bits() as i64).hash(&mut hasher);
                }
            }
            Value::String(s) => s.hash(&mut hasher),
            Value::Bool(b)   => b.hash(&mut hasher),
            _                => 0u64.hash(&mut hasher),
        }

        (hasher.finish() % self.bucket_count as u64) as u32
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_rows() -> Vec<HashMap<String, Value>> {
        vec![
            [("user_id".into(), Value::Number(1.into())),
             ("event".into(), Value::String("click".into()))].into_iter().collect(),
            [("user_id".into(), Value::Number(2.into())),
             ("event".into(), Value::String("view".into()))].into_iter().collect(),
            [("user_id".into(), Value::Number(3.into())),
             ("event".into(), Value::Null)].into_iter().collect(),
        ]
    }

    #[test]
    fn test_int64_column() {
        let schema = vec![
            ColumnSchema { name: "user_id".into(), col_type: ColumnType::Int64, nullable: false },
        ];
        let conv = RowToColumnar::new(schema);
        let rows = make_rows();
        let chunk = conv.convert(&rows).unwrap();
        assert_eq!(chunk.arrays().len(), 1);
        let arr = chunk.arrays()[0].as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value(1), 2);
        assert_eq!(arr.value(2), 3);
    }

    #[test]
    fn test_nullable_utf8_column() {
        let schema = vec![
            ColumnSchema { name: "event".into(), col_type: ColumnType::Utf8, nullable: true },
        ];
        let conv = RowToColumnar::new(schema);
        let rows = make_rows();
        let chunk = conv.convert(&rows).unwrap();
        assert_eq!(chunk.arrays().len(), 1);
        let arr = chunk.arrays()[0].as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.value(0), "click");
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_partition_router() {
        let rows = make_rows();
        let router = PartitionRouter::new("user_id".into(), 4);
        let buckets = router.route(&rows);
        // 모든 행이 어딘가의 버킷에 배정되어야
        let total: usize = buckets.values().map(|v| v.len()).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn test_empty_rows() {
        let schema = vec![
            ColumnSchema { name: "col".into(), col_type: ColumnType::Int64, nullable: true },
        ];
        let conv = RowToColumnar::new(schema);
        let chunk = conv.convert(&[]).unwrap();
        // empty rows → empty chunk
        assert_eq!(chunk.len(), 0);
    }
}
