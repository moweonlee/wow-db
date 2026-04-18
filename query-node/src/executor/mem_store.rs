// 인-메모리 행 저장소 — INSERT/SELECT 테스트용
// 실제 LSM 스토리지 연동 전 Query Node 내 임시 저장소

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde_json::Value;

pub type Row = HashMap<String, Value>;

#[derive(Debug, Default)]
struct TableData {
    rows: Vec<Row>,
}

#[derive(Debug, Default)]
pub struct InMemoryStore {
    tables: RwLock<HashMap<String, TableData>>,
}

impl InMemoryStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// 행 삽입
    pub fn insert(&self, table: &str, row: Row) {
        let mut tables = self.tables.write().unwrap();
        tables.entry(table.to_string()).or_default().rows.push(row);
    }

    /// 전체 스캔 (복사본 반환)
    pub fn scan(&self, table: &str) -> Vec<Row> {
        let tables = self.tables.read().unwrap();
        tables.get(table).map(|t| t.rows.clone()).unwrap_or_default()
    }

    /// 행 수
    pub fn row_count(&self, table: &str) -> usize {
        let tables = self.tables.read().unwrap();
        tables.get(table).map(|t| t.rows.len()).unwrap_or(0)
    }

    /// 테이블 삭제 (DROP CUBE 연동)
    pub fn drop_table(&self, table: &str) {
        let mut tables = self.tables.write().unwrap();
        tables.remove(table);
    }

    /// 테이블 존재 여부
    pub fn table_exists(&self, table: &str) -> bool {
        let tables = self.tables.read().unwrap();
        tables.contains_key(table)
    }
}

// ── 전역 싱글톤 ────────────────────────────────────────────────────────────────

use std::sync::LazyLock;
pub static MEM_STORE: LazyLock<Arc<InMemoryStore>> = LazyLock::new(InMemoryStore::new);
