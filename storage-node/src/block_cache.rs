// T039: LRU 블록 캐시 — SSTable 컬럼 블록 단위, 크기 설정 가능

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

// ─── 캐시 키 ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockKey {
    pub partition: String,
    pub column:    String,
    pub segment:   u64,
}

impl BlockKey {
    pub fn new(partition: &str, column: &str, segment: u64) -> Self {
        Self {
            partition: partition.to_string(),
            column:    column.to_string(),
            segment,
        }
    }
}

// ─── LRU 노드 ─────────────────────────────────────────────────────────────────

struct Node {
    key:   BlockKey,
    value: Bytes,
    prev:  Option<BlockKey>,
    next:  Option<BlockKey>,
}

// ─── LRU 캐시 내부 ───────────────────────────────────────────────────────────

struct LruInner {
    map:        HashMap<BlockKey, Node>,
    head:       Option<BlockKey>,   // MRU end
    tail:       Option<BlockKey>,   // LRU end
    used_bytes: usize,
    max_bytes:  usize,
}

impl LruInner {
    fn new(max_bytes: usize) -> Self {
        Self { map: HashMap::new(), head: None, tail: None, used_bytes: 0, max_bytes }
    }

    fn get(&mut self, key: &BlockKey) -> Option<Bytes> {
        if self.map.contains_key(key) {
            self.move_to_head(key.clone());
            self.map.get(key).map(|n| n.value.clone())
        } else {
            None
        }
    }

    fn insert(&mut self, key: BlockKey, value: Bytes) {
        let size = value.len();

        // 이미 존재하면 업데이트 후 head로 이동
        if self.map.contains_key(&key) {
            let old_size = self.map[&key].value.len();
            self.used_bytes -= old_size;
            self.map.get_mut(&key).unwrap().value = value;
            self.used_bytes += size;
            self.move_to_head(key);
            return;
        }

        // 공간 확보
        while self.used_bytes + size > self.max_bytes && self.tail.is_some() {
            self.evict_tail();
        }

        // 삽입
        let node = Node { key: key.clone(), value, prev: None, next: self.head.clone() };
        if let Some(ref h) = self.head.clone() {
            if let Some(head_node) = self.map.get_mut(h) {
                head_node.prev = Some(key.clone());
            }
        } else {
            self.tail = Some(key.clone());
        }
        self.head = Some(key.clone());
        self.used_bytes += size;
        self.map.insert(key, node);
    }

    fn move_to_head(&mut self, key: BlockKey) {
        if self.head.as_ref() == Some(&key) {
            return;
        }
        // 노드 연결 끊기
        let (prev, next) = {
            let node = &self.map[&key];
            (node.prev.clone(), node.next.clone())
        };
        if let Some(ref p) = prev {
            if let Some(pn) = self.map.get_mut(p) { pn.next = next.clone(); }
        }
        if let Some(ref n) = next {
            if let Some(nn) = self.map.get_mut(n) { nn.prev = prev.clone(); }
        } else {
            self.tail = prev.clone();
        }
        // head에 붙이기
        let old_head = self.head.clone();
        if let Some(ref h) = old_head {
            if let Some(hn) = self.map.get_mut(h) { hn.prev = Some(key.clone()); }
        }
        let node = self.map.get_mut(&key).unwrap();
        node.prev = None;
        node.next = old_head;
        self.head = Some(key);
    }

    fn evict_tail(&mut self) {
        if let Some(tail_key) = self.tail.clone() {
            let node = self.map.remove(&tail_key).unwrap();
            self.used_bytes -= node.value.len();
            self.tail = node.prev.clone();
            if let Some(ref t) = self.tail {
                if let Some(tn) = self.map.get_mut(t) { tn.next = None; }
            } else {
                self.head = None;
            }
        }
    }

    fn used_bytes(&self) -> usize { self.used_bytes }
    fn max_bytes(&self)  -> usize { self.max_bytes }
    fn entry_count(&self) -> usize { self.map.len() }
}

// ─── 공개 BlockCache ─────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct BlockCache {
    inner: Arc<Mutex<LruInner>>,
}

impl BlockCache {
    pub fn new(max_bytes: usize) -> Self {
        Self { inner: Arc::new(Mutex::new(LruInner::new(max_bytes))) }
    }

    pub fn get(&self, key: &BlockKey) -> Option<Bytes> {
        self.inner.lock().unwrap().get(key)
    }

    pub fn insert(&self, key: BlockKey, value: Bytes) {
        self.inner.lock().unwrap().insert(key, value);
    }

    pub fn used_bytes(&self)  -> usize { self.inner.lock().unwrap().used_bytes() }
    pub fn max_bytes(&self)   -> usize { self.inner.lock().unwrap().max_bytes() }
    pub fn entry_count(&self) -> usize { self.inner.lock().unwrap().entry_count() }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn key(segment: u64) -> BlockKey {
        BlockKey::new("p_2024_q1", "event_name", segment)
    }

    fn block(size: usize) -> Bytes {
        Bytes::from(vec![0u8; size])
    }

    #[test]
    fn test_insert_and_get() {
        let cache = BlockCache::new(1024 * 1024); // 1 MiB
        cache.insert(key(1), block(100));
        cache.insert(key(2), block(200));

        assert!(cache.get(&key(1)).is_some());
        assert!(cache.get(&key(2)).is_some());
        assert!(cache.get(&key(3)).is_none());
    }

    #[test]
    fn test_eviction_on_overflow() {
        // 500 bytes 캐시
        let cache = BlockCache::new(500);
        for i in 0u64..10 {
            cache.insert(key(i), block(100));
        }
        // 최대 5개만 유지 (500/100=5)
        assert!(cache.entry_count() <= 5);
        assert!(cache.used_bytes() <= 500);
    }

    #[test]
    fn test_lru_order() {
        let cache = BlockCache::new(300);
        cache.insert(key(1), block(100));
        cache.insert(key(2), block(100));
        cache.insert(key(3), block(100));

        // key(1) 접근 → MRU로 이동
        cache.get(&key(1));

        // 새 블록 삽입 → LRU인 key(2)가 제거돼야
        cache.insert(key(4), block(100));
        assert!(cache.get(&key(1)).is_some(), "key(1) should survive (recently used)");
        assert!(cache.get(&key(2)).is_none(), "key(2) should be evicted (LRU)");
    }
}
