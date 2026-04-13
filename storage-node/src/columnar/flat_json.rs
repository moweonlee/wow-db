// T073: Flat JSON 자동 컬럼 추출 — Compaction 시 key 출현율 분석, 임계값 이상 key → 독립 .col 파일, _flat_meta.json 갱신

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, info};

// ─── 추출 대상 열 타입 ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InferredType {
    Int64,
    Float64,
    Bool,
    String,
    Null,  // 항상 null인 경우
}

impl InferredType {
    /// JSON 값에서 타입 추론
    pub fn from_value(v: &Value) -> Self {
        match v {
            Value::Null    => InferredType::Null,
            Value::Bool(_) => InferredType::Bool,
            Value::Number(n) => {
                if n.is_i64() || n.is_u64() { InferredType::Int64 }
                else { InferredType::Float64 }
            }
            Value::String(_) => InferredType::String,
            Value::Array(_) | Value::Object(_) => InferredType::String, // nested → String
        }
    }

    /// 두 타입의 상위 타입 (Int64 < Float64 < String)
    pub fn coerce(a: InferredType, b: InferredType) -> InferredType {
        use InferredType::*;
        match (a, b) {
            (Null, x) | (x, Null)   => x,
            (Int64, Int64)           => Int64,
            (Float64, Float64)       => Float64,
            (Int64, Float64) | (Float64, Int64) => Float64,
            (Bool, Bool)             => Bool,
            _                        => String,
        }
    }
}

// ─── Flat Meta ────────────────────────────────────────────────────────────────

/// `_flat_meta.json` 파일 내용 — 추출된 key 목록과 통계
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FlatMeta {
    /// key → (출현 횟수, 추론 타입)
    pub extracted: HashMap<String, ExtractedKeyMeta>,
    /// 전체 분석된 행 수
    pub total_rows: u64,
    /// 추출 임계값 (0.0 ~ 1.0)
    pub threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedKeyMeta {
    pub count:        u64,
    pub occurrence:   f64,  // count / total_rows
    pub inferred_type: InferredType,
}

// ─── Flat JSON 분석기 ─────────────────────────────────────────────────────────

/// JSON 컬럼 행들을 분석하여 key 출현율 및 타입을 계산
pub struct FlatJsonAnalyzer {
    /// key → (출현 횟수, 누적 타입)
    key_stats: HashMap<String, (u64, InferredType)>,
    total_rows: u64,
    /// 추출 임계값 (기본 0.5: 전체 행의 50% 이상에 등장하는 key를 추출)
    pub threshold: f64,
}

impl FlatJsonAnalyzer {
    pub fn new(threshold: f64) -> Self {
        Self {
            key_stats:  HashMap::new(),
            total_rows: 0,
            threshold,
        }
    }

    /// 단일 JSON 행 추가 분석
    pub fn add_row(&mut self, json_bytes: &[u8]) {
        self.total_rows += 1;
        let Ok(value) = serde_json::from_slice::<Value>(json_bytes) else { return };
        let Some(obj) = value.as_object() else { return };

        for (k, v) in obj {
            let inferred = InferredType::from_value(v);
            let entry = self.key_stats.entry(k.clone()).or_insert((0, InferredType::Null));
            entry.0 += 1;
            entry.1 = InferredType::coerce(entry.1.clone(), inferred);
        }
    }

    /// 일괄 분석: JSON 행 슬라이스
    pub fn add_rows(&mut self, rows: &[Vec<u8>]) {
        for row in rows {
            self.add_row(row);
        }
    }

    /// 임계값 이상 출현하는 key 목록 반환
    pub fn extractable_keys(&self) -> Vec<(String, InferredType)> {
        if self.total_rows == 0 { return Vec::new(); }

        self.key_stats.iter()
            .filter_map(|(k, (count, ty))| {
                let occ = *count as f64 / self.total_rows as f64;
                if occ >= self.threshold {
                    Some((k.clone(), ty.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// FlatMeta 생성
    pub fn build_meta(&self) -> FlatMeta {
        let extracted = self.key_stats.iter()
            .filter_map(|(k, (count, ty))| {
                let occ = if self.total_rows > 0 { *count as f64 / self.total_rows as f64 } else { 0.0 };
                if occ >= self.threshold {
                    Some((k.clone(), ExtractedKeyMeta {
                        count:         *count,
                        occurrence:    occ,
                        inferred_type: ty.clone(),
                    }))
                } else {
                    None
                }
            })
            .collect();

        FlatMeta {
            extracted,
            total_rows: self.total_rows,
            threshold:  self.threshold,
        }
    }
}

// ─── Flat JSON 추출기 ─────────────────────────────────────────────────────────

/// JSON 컬럼 데이터에서 분석 결과를 바탕으로 독립 컬럼 파일 생성
pub struct FlatJsonExtractor {
    partition_dir: PathBuf,
    json_col_name: String,
}

impl FlatJsonExtractor {
    pub fn new(partition_dir: PathBuf, json_col_name: impl Into<String>) -> Self {
        Self { partition_dir, json_col_name: json_col_name.into() }
    }

    fn flat_dir(&self) -> PathBuf {
        self.partition_dir
            .join(&self.json_col_name)
            .join("_flat")
    }

    fn meta_path(&self) -> PathBuf {
        self.flat_dir().join("_flat_meta.json")
    }

    /// Compaction 완료 후 추출 실행:
    /// 1. 분석기로 출현율 계산
    /// 2. 임계값 이상 key → `_flat/<key>/seg_NNNN.col` 파일 생성
    /// 3. `_flat_meta.json` 갱신
    pub async fn extract_and_write(
        &self,
        rows: &[Vec<u8>],  // JSON 원본 행들
        seg_id: u32,
        threshold: f64,
    ) -> Result<FlatMeta> {
        let mut analyzer = FlatJsonAnalyzer::new(threshold);
        analyzer.add_rows(rows);

        let keys = analyzer.extractable_keys();
        if keys.is_empty() {
            debug!(
                partition = %self.partition_dir.display(),
                col = %self.json_col_name,
                "No keys meet flat JSON threshold"
            );
            return Ok(analyzer.build_meta());
        }

        let flat_dir = self.flat_dir();
        tokio::fs::create_dir_all(&flat_dir).await?;

        let seg_name = format!("seg_{:04}.col", seg_id);

        for (key, inferred_type) in &keys {
            let col_dir = flat_dir.join(key);
            tokio::fs::create_dir_all(&col_dir).await?;

            // 해당 key의 값을 JSON 행에서 추출 → 직렬화
            let col_data = Self::serialize_key_column(rows, key, inferred_type);
            tokio::fs::write(col_dir.join(&seg_name), &col_data).await?;

            debug!(
                key = %key,
                seg = %seg_name,
                rows = rows.len(),
                "Flat JSON column extracted"
            );
        }

        // meta 갱신
        let meta = analyzer.build_meta();
        let meta_json = serde_json::to_string_pretty(&meta)?;
        tokio::fs::write(self.meta_path(), meta_json).await?;

        info!(
            partition = %self.partition_dir.display(),
            col = %self.json_col_name,
            keys = keys.len(),
            "Flat JSON extraction complete"
        );

        Ok(meta)
    }

    /// key의 값을 모든 행에서 추출하여 바이트 직렬화 (simple newline-delimited format)
    fn serialize_key_column(rows: &[Vec<u8>], key: &str, _ty: &InferredType) -> Vec<u8> {
        let mut buf = Vec::new();
        for row in rows {
            let val = serde_json::from_slice::<Value>(row).ok()
                .and_then(|v| v.get(key).cloned())
                .unwrap_or(Value::Null);

            // null은 빈 줄, 나머지는 JSON 인코딩 (추후 타입별 인코딩으로 교체)
            let encoded = match &val {
                Value::Null => b"null\n".to_vec(),
                other => {
                    let mut s = other.to_string().into_bytes();
                    s.push(b'\n');
                    s
                }
            };
            buf.extend_from_slice(&encoded);
        }
        buf
    }

    /// 기존 `_flat_meta.json` 로드 (없으면 기본값)
    pub async fn load_meta(&self) -> FlatMeta {
        let path = self.meta_path();
        match tokio::fs::read_to_string(&path).await {
            Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
            Err(_) => FlatMeta::default(),
        }
    }

    /// 특정 key의 컬럼 파일 경로 목록 반환
    pub fn col_paths_for_key(&self, key: &str) -> impl Iterator<Item = PathBuf> + '_ {
        let key = key.to_string();
        // 실제 구현에서는 glob으로 seg_*.col 열거
        let dir = self.flat_dir().join(key);
        std::iter::once(dir)  // Phase D에서 glob 확장
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn rows(data: &[&str]) -> Vec<Vec<u8>> {
        data.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn test_analyzer_basic() {
        let mut a = FlatJsonAnalyzer::new(0.5);
        a.add_rows(&rows(&[
            r#"{"product_id":"p1","price":100}"#,
            r#"{"product_id":"p2","price":200}"#,
            r#"{"product_id":"p3"}"#,
        ]));

        let keys = a.extractable_keys();
        let key_names: Vec<_> = keys.iter().map(|(k, _)| k.as_str()).collect();
        assert!(key_names.contains(&"product_id"), "product_id 100% → 추출");
        // price는 2/3 ≈ 0.67 ≥ 0.5 → 추출
        assert!(key_names.contains(&"price"), "price 67% → 추출");
    }

    #[test]
    fn test_analyzer_threshold() {
        let mut a = FlatJsonAnalyzer::new(0.8);
        a.add_rows(&rows(&[
            r#"{"a":1,"b":1}"#,
            r#"{"a":2,"b":2}"#,
            r#"{"a":3}"#,
            r#"{"a":4}"#,
        ]));

        let keys = a.extractable_keys();
        let key_names: Vec<_> = keys.iter().map(|(k, _)| k.as_str()).collect();
        assert!(key_names.contains(&"a"), "a 100% → 추출");
        // b는 2/4 = 0.5 < 0.8 → 제외
        assert!(!key_names.contains(&"b"), "b 50% < 80% → 제외");
    }

    #[test]
    fn test_inferred_type_coerce() {
        assert_eq!(InferredType::coerce(InferredType::Int64, InferredType::Float64), InferredType::Float64);
        assert_eq!(InferredType::coerce(InferredType::Null, InferredType::Int64), InferredType::Int64);
        assert_eq!(InferredType::coerce(InferredType::Int64, InferredType::String), InferredType::String);
    }

    #[tokio::test]
    async fn test_extractor_writes_files() {
        let dir = tempdir().unwrap();
        let extractor = FlatJsonExtractor::new(dir.path().to_path_buf(), "properties");

        let json_rows = rows(&[
            r#"{"product_id":"p1","price":100,"rare":"x"}"#,
            r#"{"product_id":"p2","price":200}"#,
            r#"{"product_id":"p3","price":300}"#,
        ]);

        let meta = extractor.extract_and_write(&json_rows, 1, 0.6).await.unwrap();

        // product_id(100%), price(100%) → 추출
        assert!(meta.extracted.contains_key("product_id"));
        assert!(meta.extracted.contains_key("price"));
        // rare(33%) < 60% → 미추출
        assert!(!meta.extracted.contains_key("rare"));

        // 파일 존재 확인
        let prod_file = dir.path().join("properties/_flat/product_id/seg_0001.col");
        assert!(prod_file.exists(), "product_id col 파일 생성됨");

        let meta_file = dir.path().join("properties/_flat/_flat_meta.json");
        assert!(meta_file.exists(), "_flat_meta.json 생성됨");
    }

    #[test]
    fn test_build_meta() {
        let mut a = FlatJsonAnalyzer::new(0.5);
        a.add_rows(&rows(&[
            r#"{"x":1}"#,
            r#"{"x":2,"y":"hello"}"#,
        ]));

        let meta = a.build_meta();
        assert_eq!(meta.total_rows, 2);
        assert_eq!(meta.threshold, 0.5);
        assert!(meta.extracted.contains_key("x")); // 100%
        // y = 50% ≥ 0.5 → 포함
        assert!(meta.extracted.contains_key("y"));
    }
}
