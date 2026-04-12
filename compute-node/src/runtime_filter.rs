// T049: Runtime Filter — Build Side Bloom/InList/MinMax Filter 생성 → Probe Side 전파

use std::collections::HashSet;

use bytes::Bytes;

// ─── Runtime Filter 종류 ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum RuntimeFilter {
    /// Bloom Filter (고기수 컬럼용)
    Bloom {
        column:     String,
        /// 직렬화된 bloom filter 바이트
        bloom_data: Bytes,
    },
    /// In-list Filter (저기수 컬럼용)
    InList {
        column: String,
        values: Vec<i64>,
    },
    /// MinMax Filter
    MinMax {
        column: String,
        min:    i64,
        max:    i64,
    },
}

impl RuntimeFilter {
    /// i64 값이 필터를 통과하는지 확인
    pub fn test_i64(&self, val: i64) -> bool {
        match self {
            RuntimeFilter::InList { values, .. } => values.contains(&val),
            RuntimeFilter::MinMax { min, max, .. } => val >= *min && val <= *max,
            RuntimeFilter::Bloom { .. } => true, // bloom은 bytes 기반, i64는 pass-through
        }
    }
}

// ─── Build Side 생성기 ────────────────────────────────────────────────────────

pub struct RuntimeFilterBuilder {
    column:  String,
    values:  Vec<i64>,
    min:     Option<i64>,
    max:     Option<i64>,
    max_inlist: usize,
}

impl RuntimeFilterBuilder {
    pub fn new(column: impl Into<String>, max_inlist: usize) -> Self {
        Self {
            column: column.into(),
            values: Vec::new(),
            min: None,
            max: None,
            max_inlist,
        }
    }

    pub fn add_value(&mut self, val: i64) {
        self.values.push(val);
        self.min = Some(self.min.map_or(val, |m| m.min(val)));
        self.max = Some(self.max.map_or(val, |m| m.max(val)));
    }

    /// 적절한 필터 종류 선택하여 빌드
    pub fn build(mut self) -> Option<RuntimeFilter> {
        if self.values.is_empty() {
            return None;
        }

        self.values.sort();
        self.values.dedup();
        let ndv = self.values.len();

        if ndv <= self.max_inlist {
            // 저기수: InList
            Some(RuntimeFilter::InList {
                column: self.column,
                values: self.values,
            })
        } else {
            // 고기수: MinMax (Bloom은 Phase C에서 bloomfilter 직렬화 포함)
            Some(RuntimeFilter::MinMax {
                column: self.column,
                min:    self.min.unwrap_or(i64::MIN),
                max:    self.max.unwrap_or(i64::MAX),
            })
        }
    }
}

// ─── Filter 적용 (Probe Side) ─────────────────────────────────────────────────

/// i64 배열에 RuntimeFilter 적용 → 통과 인덱스 목록
pub fn apply_runtime_filter_i64(values: &[Option<i64>], filter: &RuntimeFilter) -> Vec<usize> {
    values.iter().enumerate()
        .filter_map(|(i, v)| {
            v.map(|val| if filter.test_i64(val) { Some(i) } else { None })
                .flatten()
        })
        .collect()
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inlist_filter() {
        let mut builder = RuntimeFilterBuilder::new("user_id", 1000);
        for v in [1i64, 2, 3, 4, 5] { builder.add_value(v); }
        let filter = builder.build().unwrap();

        assert!(filter.test_i64(3));
        assert!(!filter.test_i64(99));
    }

    #[test]
    fn test_minmax_filter() {
        let filter = RuntimeFilter::MinMax { column: "ts".into(), min: 100, max: 200 };
        assert!(filter.test_i64(150));
        assert!(!filter.test_i64(50));
        assert!(!filter.test_i64(250));
    }

    #[test]
    fn test_builder_selects_minmax_for_high_ndv() {
        let mut builder = RuntimeFilterBuilder::new("col", 10);
        for v in 0i64..100 { builder.add_value(v); }
        let filter = builder.build().unwrap();
        assert!(matches!(filter, RuntimeFilter::MinMax { .. }));
    }

    #[test]
    fn test_apply_filter() {
        let filter = RuntimeFilter::MinMax { column: "v".into(), min: 5, max: 10 };
        let vals = vec![Some(3), Some(7), None, Some(10), Some(11)];
        let idx = apply_runtime_filter_i64(&vals, &filter);
        assert_eq!(idx, vec![1, 3]);
    }
}
