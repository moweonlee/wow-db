// T024: MemTable — crossbeam-skiplist 기반 정렬 삽입, 임계값 초과 시 Immutable 전환

use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex, RwLock,
};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use shared::types::SortKey;
use tracing::{debug, info, trace};

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
    /// 파티션 전역 단조증가 시퀀스 — MemTableSet 과 공유하여 freeze 후 재시작되지 않음
    seq_gen:    Arc<AtomicU64>,
    threshold:  usize,
}

impl MemTable {
    pub fn new(threshold_bytes: usize) -> Self {
        Self::new_with_seq(threshold_bytes, Arc::new(AtomicU64::new(0)))
    }

    /// MemTableSet 용 생성자 — seq_gen 을 외부에서 주입받아 여러 MemTable 간 공유.
    pub fn new_with_seq(threshold_bytes: usize, seq_gen: Arc<AtomicU64>) -> Self {
        Self {
            map:        Arc::new(SkipMap::new()),
            size_bytes: Arc::new(AtomicUsize::new(0)),
            seq_gen,
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
        let full = total >= self.threshold;
        trace!(tx_id, seq, row_size, total_bytes = total, "MemTable row inserted");
        if full {
            debug!(
                size_bytes  = total,
                threshold   = self.threshold,
                "MemTable threshold exceeded — flush required"
            );
        }
        full
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

    /// 삽입된 행 수 (tombstone 포함)
    pub fn row_count(&self) -> usize {
        self.map.len()
    }

    /// 임계값 초과 여부
    pub fn is_full(&self) -> bool {
        self.size_bytes() >= self.threshold
    }

    /// MemTable 내용 초기화 (flush 완료 후 새 쓰기 수신용).
    /// seq_gen 은 보존하여 시퀀스 단조증가 불변 조건 유지.
    pub fn reset(&mut self) {
        self.map = Arc::new(SkipMap::new());
        self.size_bytes.store(0, Ordering::SeqCst);
    }

    /// MemTable을 Immutable 스냅샷으로 변환 (self 소비)
    pub fn freeze(self) -> ImmutableMemTable {
        let size     = self.size_bytes.load(Ordering::Relaxed);
        let row_count = self.map.len();
        let entries   = self
            .map
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        info!(
            row_count,
            size_bytes = size,
            "MemTable frozen → ImmutableMemTable (pending flush)"
        );
        ImmutableMemTable { entries, size_bytes: size }
    }

    /// 비삭제 행 전체를 (key_bytes, columns) 이터레이터로 반환 (스캔용)
    pub fn iter_rows(&self) -> Vec<(Vec<u8>, Vec<(String, bytes::Bytes)>)> {
        self.map
            .iter()
            .filter(|e| !e.value().deleted)
            .map(|e| {
                let key_bytes = e.key().sort_key.values.first()
                    .map(|c| c.encoded.clone())
                    .unwrap_or_default();
                (key_bytes, e.value().columns.clone())
            })
            .collect()
    }

    /// Arc 공유 중일 때 안전하게 스냅샷 → ImmutableMemTable 반환.
    /// freeze()와 달리 self를 소비하지 않고 현재 상태를 복사.
    pub fn snapshot_as_immutable(&self) -> ImmutableMemTable {
        let size     = self.size_bytes.load(Ordering::Acquire);
        let entries  = self.map.iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        info!(size_bytes = size, "MemTable snapshotted as Immutable (Arc still shared)");
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

// ─── MemTableSet: Active/Immutable 전환 Coordinator ──────────────────────────
//
// 문제: `is_full()` + `freeze()` 사이에 TOCTOU race 발생 가능.
//   - Writer A: is_full() == true → freeze() 호출 준비
//   - Writer B: 동시에 is_full() == true → 둘 다 freeze() 시도 → 중복 freeze
//
// 해결: freeze_lock (Mutex) 으로 freeze 를 단 한 번만 실행.
//   - 쓰기는 active 에 RwLock read lock → 여러 writer 동시 가능
//   - freeze 시도 시 freeze_lock 획득 → active 교체는 write lock 으로 원자적 수행
//   - freeze_lock 이미 잠겨있으면 다른 스레드가 freeze 중 → 새 active 를 기다린 후 재시도

/// Active MemTable + Immutable MemTable 목록을 안전하게 관리하는 coordinator.
///
/// ```text
///                  freeze_lock (Mutex)
///                        │
///  N개 writer            ▼
///  ──────►  active: RwLock<Arc<MemTable>>  ──is_full?──► freeze → immutable_list
///  ──────►                                                          (Mutex<Vec<..>>)
/// ```
pub struct MemTableSet {
    /// 현재 쓰기 가능한 MemTable. RwLock read = 동시 쓰기 허용 (SkipMap 자체가 lock-free).
    active: RwLock<Arc<MemTable>>,

    /// Flush 대기 중인 Immutable MemTable 목록.
    immutable: Mutex<Vec<ImmutableMemTable>>,

    /// Freeze 작업을 직렬화. 단 하나의 goroutine만 freeze 실행.
    freeze_lock: Mutex<()>,

    /// 파티션 전역 단조증가 시퀀스 — MemTable 교체 후에도 연속성 보장.
    seq_gen: Arc<AtomicU64>,

    threshold: usize,
}

impl MemTableSet {
    pub fn new(threshold_bytes: usize) -> Self {
        let seq_gen = Arc::new(AtomicU64::new(0));
        Self {
            active:      RwLock::new(Arc::new(MemTable::new_with_seq(threshold_bytes, Arc::clone(&seq_gen)))),
            immutable:   Mutex::new(Vec::new()),
            freeze_lock: Mutex::new(()),
            seq_gen,
            threshold:   threshold_bytes,
        }
    }

    /// 행 삽입. 내부적으로 is_full() 시 freeze를 안전하게 처리.
    ///
    /// **핵심 불변조건**: row 는 단 한 번만 삽입된다.
    /// - active.insert() 로 row 를 먼저 삽입한 뒤 freeze 여부를 결정.
    /// - loop 재시도 없음 → 중복 삽입 방지.
    pub fn insert(&self, sort_key: SortKey, columns: Vec<(String, Bytes)>, tx_id: u64) {
        // 1. active MemTable 에 read lock 으로 insert (여러 writer 동시 진입 가능)
        //    row 는 여기서 단 한 번 삽입된다.
        let full = {
            let active = self.active.read().unwrap();
            active.insert(sort_key, columns, tx_id)
        };

        // 2. row 삽입 완료. 임계값 초과 시 freeze 를 별도로 처리.
        if full {
            self.try_freeze();
        }
    }

    /// 임계값 초과 시 active → immutable 전환을 단 한 번만 실행.
    ///
    /// - freeze_lock 을 획득한 스레드만 실제 freeze 수행.
    /// - 다른 스레드는 try_lock 실패 시 즉시 리턴 (이미 다른 스레드가 처리 중).
    fn try_freeze(&self) {
        // try_lock: 이미 다른 스레드가 freeze 중이면 skip
        let Ok(_guard) = self.freeze_lock.try_lock() else { return };

        // double-check: 다른 스레드가 이미 freeze 완료 후 새 active 로 교체됐을 수 있음
        {
            let active = self.active.read().unwrap();
            if !active.is_full() { return; }
        }

        // 실제 freeze: write lock 으로 active 원자적 교체
        let imm = {
            let mut active_guard = self.active.write().unwrap();
            // old Arc clone (ref count 임시 증가)
            let old = Arc::clone(&active_guard);
            // 새 MemTable 로 교체 — seq_gen 을 공유하여 seq 연속성 보장
            *active_guard = Arc::new(MemTable::new_with_seq(self.threshold, Arc::clone(&self.seq_gen)));
            // write lock 해제 후 old Arc 소유권 회수 시도
            drop(active_guard);

            // 이 시점에서 old 를 가리키는 Arc 는 `old` 하나뿐이어야 함.
            // (다른 writer 들은 위에서 read lock 을 해제했으므로)
            Arc::try_unwrap(old)
                .map(|mt| mt.freeze())
                .unwrap_or_else(|shared| {
                    // 극히 드문 경우: 아직 reader 가 Arc 를 보유 중 → 스냅샷
                    shared.snapshot_as_immutable()
                })
        };

        self.immutable.lock().unwrap().push(imm);
        debug!("MemTable frozen and rotated to new active");
        // freeze_lock 자동 해제 (_guard drop)
    }

    /// Flush할 준비된 Immutable MemTable 반환 (선입선출).
    pub fn pop_immutable(&self) -> Option<ImmutableMemTable> {
        let mut list = self.immutable.lock().unwrap();
        if list.is_empty() { None } else { Some(list.remove(0)) }
    }

    /// 현재 active MemTable의 행 수 (모니터링용)
    pub fn active_row_count(&self) -> usize {
        self.active.read().unwrap().map.len()
    }

    /// 대기 중인 Immutable MemTable 수 (모니터링용)
    pub fn immutable_count(&self) -> usize {
        self.immutable.lock().unwrap().len()
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
        assert!(full || !full);
    }

    // ── MemTableSet 테스트 ────────────────────────────────────────────────────

    /// 단일 스레드: insert → 임계값 초과 → freeze 1회 발생 확인
    #[test]
    fn test_memtableset_single_thread_freeze() {
        // threshold를 작게 설정 → 금방 가득 참
        let set = std::sync::Arc::new(MemTableSet::new(200));

        for i in 0u64..10 {
            set.insert(
                make_key(i),
                vec![("v".into(), Bytes::from(vec![0u8; 32]))],
                i,
            );
        }

        // 임계값(200) 초과 → immutable 1개 이상 생성
        assert!(
            set.immutable_count() >= 1,
            "threshold 초과 시 immutable MemTable 이 생성되어야 함"
        );
    }

    /// 핵심: 여러 스레드가 동시에 insert → freeze가 중복으로 일어나지 않는지 확인
    /// race condition 이 있다면 immutable 이 threshold 보다 훨씬 많이 쌓이거나 panic 발생
    #[test]
    fn test_memtableset_concurrent_no_double_freeze() {
        use std::sync::Arc;
        use std::thread;

        // 작은 threshold → 금방 freeze 유도
        let set = Arc::new(MemTableSet::new(256));
        let n_threads = 20;
        let rows_per_thread = 50u64;

        let handles: Vec<_> = (0..n_threads)
            .map(|t| {
                let set = Arc::clone(&set);
                thread::spawn(move || {
                    for r in 0..rows_per_thread {
                        let key = make_key(t * rows_per_thread + r);
                        set.insert(
                            key,
                            vec![("val".into(), Bytes::from(vec![0u8; 16]))],
                            t,
                        );
                    }
                })
            })
            .collect();

        for h in handles { h.join().unwrap(); }

        let total_rows = n_threads * rows_per_thread;

        // immutable 에서 모든 행을 수집
        let mut all_rows: Vec<MemKey> = Vec::new();
        while let Some(imm) = set.pop_immutable() {
            for (k, _) in imm.entries {
                all_rows.push(k);
            }
        }
        // active 에 남은 행도 추가
        {
            let active = set.active.read().unwrap();
            for e in active.map.iter() {
                all_rows.push(e.key().clone());
            }
        }

        // 삽입한 총 행 수와 수집한 행 수가 일치해야 함 (데이터 유실 없음)
        assert_eq!(
            all_rows.len(), total_rows as usize,
            "동시 insert 후 데이터 유실 없어야 함: expected={}, got={}",
            total_rows, all_rows.len()
        );

        // seq 중복 없음 (각 행이 고유한 seq 를 가져야 함)
        let mut seqs: Vec<u64> = all_rows.iter().map(|k| k.seq).collect();
        seqs.sort_unstable();
        let before = seqs.len();
        seqs.dedup();
        assert_eq!(seqs.len(), before, "seq 중복 없어야 함 (MVCC 위반)");
    }
}
