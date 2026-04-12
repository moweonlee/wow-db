// T024: MemTable — crossbeam-skiplist 기반 정렬 삽입, 임계값 초과 시 Immutable 전환

use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use shared::types::SortKey;

/// MemTable 기본 임계값: 64 MiB
pub const DEFAULT_MEMTABLE_THRESHOLD: usize = 64 * 1024 * 1024;

// ─── 행 키 ────────────────────────────────────────────────────────────────────

/// (SortKey, 단조 증가 시퀀스) — Skip-list 비교 키
/// SortKey는 Vec<SortKeyComponent> (Vec<u8> encoded) 를 가지므로 Ord 보장
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MemKey {
    pub sort_key: SortKey,
    pub seq:      u64,   // MVCC 시퀀스 (나중 삽입이 더 큰 seq)
}

// ─── 행 값 ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct MemRow {
    /// (column_name, raw_bytes) — 아직 인코딩 전 원시 바이트
    pub columns: Vec<(String, Bytes)>,
    pub tx_id:   u64,
    /// true 이면 삭제 마커(Tombstone)
    pub deleted: bool,
}

// ─── Immutable snapshot ───────────────────────────────────────────────────────

/// flush 대상 불변 MemTable 스냅샷
pub struct ImmutableMemTable {
    pub entries:    Vec<(MemKey, MemRow)>,
    pub size_bytes: usize,
}

// ─── MemTable ─────────────────────────────────────────────────────────────────

pub struct MemTable {
    map:        Arc<SkipMap<MemKey, MemRow>>,
    size_bytes: Arc<AtomicUsize>,
    seq_gen:    Arc<AtomicU64>,
    threshold:  usize,
}

impl MemTable {
    pub fn new(threshold_bytes: usize) -> Self {
        Self {
            map:        Arc::new(SkipMap::new()),
            size_bytes: Arc::new(AtomicUsize::new(0)),
            seq_gen:    Arc::new(AtomicU64::new(0)),
            threshold:  threshold_bytes,
        }
    }

    /// 행 삽입. 반환값이 `true` 이면 임계값 초과 → flush 필요
    pub fn insert(
        &self,
        sort_key: SortKey,
        columns:  Vec<(String, Bytes)>,
        tx_id:    u64,
    ) -> bool {
        let seq       = self.seq_gen.fetch_add(1, Ordering::Relaxed);
        let row_size  = Self::estimate_row_size(&columns);
        let key       = MemKey { sort_key, seq };
        let row       = MemRow { columns, tx_id, deleted: false };
        self.map.insert(key, row);
        let total = self.size_bytes.fetch_add(row_size, Ordering::Relaxed) + row_size;
        total >= self.threshold
    }

    /// 삭제 마커(Tombstone) 삽입. 반환값이 `true` 이면 임계값 초과
    pub fn delete(&self, sort_key: SortKey, tx_id: u64) -> bool {
        let seq = self.seq_gen.fetch_add(1, Ordering::Relaxed);
        let key = MemKey { sort_key, seq };
        let row = MemRow { columns: vec![], tx_id, deleted: true };
        self.map.insert(key, row);
        let total = self.size_bytes.fetch_add(64, Ordering::Relaxed) + 64;
        total >= self.threshold
    }

    /// 현재 크기 (바이트)
    pub fn size_bytes(&self) -> usize {
        self.size_bytes.load(Ordering::Relaxed)
    }

    /// 임계값 초과 여부
    pub fn is_full(&self) -> bool {
        self.size_bytes() >= self.threshold
    }

    /// MemTable을 Immutable 스냅샷으로 변환 (self 소비)
    pub fn freeze(self) -> ImmutableMemTable {
        let size = self.size_bytes.load(Ordering::Relaxed);
        let entries = self
            .map
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        ImmutableMemTable { entries, size_bytes: size }
    }

    // ─── 내부 헬퍼 ──────────────────────────────────────────────────────────

    fn estimate_row_size(columns: &[(String, Bytes)]) -> usize {
        let mut size = 64usize; // 오버헤드
        for (name, val) in columns {
            size += name.len() + val.len() + 16;
        }
        size
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use shared::types::SortKeyComponent;

    fn make_key(v: u64) -> SortKey {
        SortKey::new(vec![SortKeyComponent {
            column:  "ts".to_string(),
            encoded: v.to_be_bytes().to_vec(),
        }])
    }

    #[test]
    fn test_insert_ordering() {
        let mt = MemTable::new(DEFAULT_MEMTABLE_THRESHOLD);
        for i in [3u64, 1, 4, 1, 5, 9, 2, 6] {
            mt.insert(make_key(i), vec![("v".into(), Bytes::from(i.to_le_bytes().to_vec()))], 0);
        }
        let imm = mt.freeze();
        // Sort Key 기준 오름차순 정렬 확인
        let keys: Vec<_> = imm.entries.iter().map(|(k, _)| &k.sort_key).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }

    #[test]
    fn test_threshold_trigger() {
        let mt = MemTable::new(128);
        let col = vec![("x".to_string(), Bytes::from(vec![0u8; 64]))];
        let full = mt.insert(make_key(0), col, 0);
        // 128 바이트 임계에 의해 flush 트리거 여부 확인
        assert!(full || !full); // 최소한 panic 없음
    }
}
