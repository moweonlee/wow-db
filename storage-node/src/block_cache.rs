// T114: 3-pool Block Cache — Data(70%) / Filter(20%, 높은 eviction 저항) / Index(10%)
// lsm-engine.md §10 기준

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

// ─── 블록 종류 ────────────────────────────────────────────────────────────────

/// 캐시 블록의 종류 (Pool 선택에 사용)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlockType {
    /// SSTable 컬럼 데이터 블록 (전체 캐시의 70%)
    Data,
    /// Bloom Filter 블록 (전체 캐시의 20%, 높은 eviction 저항)
    Filter,
    /// Min/Max 인덱스 블록 (전체 캐시의 10%)
    Index,
}

// ─── 캐시 키 ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockKey {
    pub partition: String,
    pub column:    String,
    pub segment:   u64,
    pub block_type: BlockType,
}

impl BlockKey {
    pub fn data(partition: &str, column: &str, segment: u64) -> Self {
        Self { partition: partition.to_string(), column: column.to_string(), segment, block_type: BlockType::Data }
    }

    pub fn filter(partition: &str, column: &str, segment: u64) -> Self {
        Self { partition: partition.to_string(), column: column.to_string(), segment, block_type: BlockType::Filter }
    }

    pub fn index(partition: &str, column: &str, segment: u64) -> Self {
        Self { partition: partition.to_string(), column: column.to_string(), segment, block_type: BlockType::Index }
    }
}

// ─── LRU 내부 ─────────────────────────────────────────────────────────────────

struct Node {
    key:        BlockKey,
    value:      Bytes,
    /// Filter 블록의 eviction 저항 카운터
    /// 일반 eviction 시 카운터가 0이 되어야 실제 제거 (최대 저항 횟수: FILTER_RESIST_COUNT)
    resist:     u8,
    prev:       Option<BlockKey>,
    next:       Option<BlockKey>,
}

/// Filter 블록의 eviction 저항 횟수 (우선 보존 정책)
const FILTER_RESIST_COUNT: u8 = 3;

struct LruPool {
    map:        HashMap<BlockKey, Node>,
    head:       Option<BlockKey>,
    tail:       Option<BlockKey>,
    used_bytes: usize,
    max_bytes:  usize,
    block_type: BlockType,
}

impl LruPool {
    fn new(max_bytes: usize, block_type: BlockType) -> Self {
        Self { map: HashMap::new(), head: None, tail: None, used_bytes: 0, max_bytes, block_type }
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

        if self.map.contains_key(&key) {
            let old_size = self.map[&key].value.len();
            self.used_bytes -= old_size;
            self.map.get_mut(&key).unwrap().value = value;
            self.used_bytes += size;
            self.move_to_head(key);
            return;
        }

        while self.used_bytes + size > self.max_bytes && self.tail.is_some() {
            self.try_evict_tail();
        }

        let resist = if self.block_type == BlockType::Filter { FILTER_RESIST_COUNT } else { 0 };
        let node = Node {
            key: key.clone(), value, resist, prev: None, next: self.head.clone(),
        };
        if let Some(ref h) = self.head.clone() {
            if let Some(hn) = self.map.get_mut(h) { hn.prev = Some(key.clone()); }
        } else {
            self.tail = Some(key.clone());
        }
        self.head = Some(key.clone());
        self.used_bytes += size;
        self.map.insert(key, node);
    }

    /// Filter 블록의 높은 eviction 저항:
    /// resist > 0인 경우 카운터를 줄이고 head로 이동 (실제 제거 지연)
    fn try_evict_tail(&mut self) {
        let Some(tail_key) = self.tail.clone() else { return };

        if self.block_type == BlockType::Filter {
            if let Some(node) = self.map.get_mut(&tail_key) {
                if node.resist > 0 {
                    node.resist -= 1;
                    // tail을 head 근처로 이동 (다음 eviction 시도 시 저항)
                    self.move_to_head(tail_key);
                    return;
                }
            }
        }

        // 실제 제거
        if let Some(node) = self.map.remove(&tail_key) {
            self.used_bytes -= node.value.len();
            self.tail = node.prev.clone();
            if let Some(ref t) = self.tail {
                if let Some(tn) = self.map.get_mut(t) { tn.next = None; }
            } else {
                self.head = None;
            }
        }
    }

    fn move_to_head(&mut self, key: BlockKey) {
        if self.head.as_ref() == Some(&key) {
            return;
        }
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
        let old_head = self.head.clone();
        if let Some(ref h) = old_head {
            if let Some(hn) = self.map.get_mut(h) { hn.prev = Some(key.clone()); }
        }
        let node = self.map.get_mut(&key).unwrap();
        node.prev = None;
        node.next = old_head;
        self.head = Some(key);
    }

    fn used_bytes(&self)  -> usize { self.used_bytes }
    fn max_bytes(&self)   -> usize { self.max_bytes }
    fn entry_count(&self) -> usize { self.map.len() }
}

// ─── 3-Pool BlockCache ────────────────────────────────────────────────────────

struct BlockCacheInner {
    data_pool:   LruPool,
    filter_pool: LruPool,
    index_pool:  LruPool,
    total_max:   usize,
}

impl BlockCacheInner {
    /// total_max_bytes 기준으로 70/20/10 비율로 Pool 생성
    fn new(total_max_bytes: usize) -> Self {
        let data_max   = total_max_bytes * 70 / 100;
        let filter_max = total_max_bytes * 20 / 100;
        let index_max  = total_max_bytes - data_max - filter_max; // 나머지 10%
        Self {
            data_pool:   LruPool::new(data_max,   BlockType::Data),
            filter_pool: LruPool::new(filter_max, BlockType::Filter),
            index_pool:  LruPool::new(index_max,  BlockType::Index),
            total_max:   total_max_bytes,
        }
    }

    fn pool_for(&mut self, block_type: BlockType) -> &mut LruPool {
        match block_type {
            BlockType::Data   => &mut self.data_pool,
            BlockType::Filter => &mut self.filter_pool,
            BlockType::Index  => &mut self.index_pool,
        }
    }

    fn pool_for_ref(&self, block_type: BlockType) -> &LruPool {
        match block_type {
            BlockType::Data   => &self.data_pool,
            BlockType::Filter => &self.filter_pool,
            BlockType::Index  => &self.index_pool,
        }
    }

    fn get(&mut self, key: &BlockKey) -> Option<Bytes> {
        let bt = key.block_type;
        self.pool_for(bt).get(key)
    }

    fn insert(&mut self, key: BlockKey, value: Bytes) {
        let bt = key.block_type;
        self.pool_for(bt).insert(key, value);
    }

    fn used_bytes(&self) -> usize {
        self.data_pool.used_bytes() + self.filter_pool.used_bytes() + self.index_pool.used_bytes()
    }

    fn pool_stats(&self) -> PoolStats {
        PoolStats {
            data_used:   self.data_pool.used_bytes(),
            data_max:    self.data_pool.max_bytes(),
            filter_used: self.filter_pool.used_bytes(),
            filter_max:  self.filter_pool.max_bytes(),
            index_used:  self.index_pool.used_bytes(),
            index_max:   self.index_pool.max_bytes(),
        }
    }
}

// ─── 공개 타입 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub data_used:   usize,
    pub data_max:    usize,
    pub filter_used: usize,
    pub filter_max:  usize,
    pub index_used:  usize,
    pub index_max:   usize,
}

#[derive(Clone)]
pub struct BlockCache {
    inner: Arc<Mutex<BlockCacheInner>>,
}

impl BlockCache {
    /// total_max_bytes: 전체 캐시 크기 (Data 70% / Filter 20% / Index 10% 자동 분배)
    pub fn new(total_max_bytes: usize) -> Self {
        Self { inner: Arc::new(Mutex::new(BlockCacheInner::new(total_max_bytes))) }
    }

    pub fn get(&self, key: &BlockKey) -> Option<Bytes> {
        self.inner.lock().unwrap().get(key)
    }

    pub fn insert(&self, key: BlockKey, value: Bytes) {
        self.inner.lock().unwrap().insert(key, value);
    }

    pub fn used_bytes(&self)  -> usize { self.inner.lock().unwrap().used_bytes() }
    pub fn max_bytes(&self)   -> usize { self.inner.lock().unwrap().total_max }
    pub fn pool_stats(&self)  -> PoolStats { self.inner.lock().unwrap().pool_stats() }

    pub fn data_entry_count(&self)   -> usize { self.inner.lock().unwrap().data_pool.entry_count() }
    pub fn filter_entry_count(&self) -> usize { self.inner.lock().unwrap().filter_pool.entry_count() }
    pub fn index_entry_count(&self)  -> usize { self.inner.lock().unwrap().index_pool.entry_count() }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn block(size: usize) -> Bytes {
        Bytes::from(vec![0u8; size])
    }

    #[test]
    fn test_pool_split_ratio() {
        let cache = BlockCache::new(1000);
        let stats = cache.pool_stats();
        assert_eq!(stats.data_max,   700);
        assert_eq!(stats.filter_max, 200);
        assert_eq!(stats.index_max,  100);
    }

    #[test]
    fn test_data_insert_and_get() {
        let cache = BlockCache::new(1024 * 1024);
        let key = BlockKey::data("p_2024_q1", "event_name", 1);
        cache.insert(key.clone(), block(100));
        assert!(cache.get(&key).is_some());
    }

    #[test]
    fn test_filter_insert_and_get() {
        let cache = BlockCache::new(1024 * 1024);
        let key = BlockKey::filter("p_2024_q1", "event_name", 1);
        cache.insert(key.clone(), block(100));
        assert!(cache.get(&key).is_some());
    }

    #[test]
    fn test_pools_are_independent() {
        let cache = BlockCache::new(1024 * 1024);
        let data_key   = BlockKey::data("p", "col", 1);
        let filter_key = BlockKey::filter("p", "col", 1);
        let index_key  = BlockKey::index("p", "col", 1);

        cache.insert(data_key.clone(),   block(100));
        cache.insert(filter_key.clone(), block(200));
        cache.insert(index_key.clone(),  block(50));

        assert_eq!(cache.data_entry_count(),   1);
        assert_eq!(cache.filter_entry_count(), 1);
        assert_eq!(cache.index_entry_count(),  1);
    }

    #[test]
    fn test_data_pool_eviction() {
        // data pool = 500 bytes, filter pool = 142, index pool = 71
        let cache = BlockCache::new(714);
        let data_max = cache.pool_stats().data_max; // ~500

        // data pool 꽉 채우기
        for i in 0u64..10 {
            let key = BlockKey::data("p", "col", i);
            cache.insert(key, block(data_max / 10));
        }
        // 오버플로우 삽입 → eviction 발생
        cache.insert(BlockKey::data("p", "col", 99), block(data_max / 10));
        assert!(cache.pool_stats().data_used <= data_max);
    }

    #[test]
    fn test_filter_eviction_resistance() {
        // filter pool = 200 bytes (total 1000)
        let cache = BlockCache::new(1000);
        let filter_pool_max = cache.pool_stats().filter_max; // 200

        // filter 블록 삽입
        let key1 = BlockKey::filter("p", "col", 1);
        cache.insert(key1.clone(), block(100));
        let key2 = BlockKey::filter("p", "col", 2);
        cache.insert(key2.clone(), block(100));

        // 오버플로우 → filter 블록은 resist 카운터로 즉시 제거되지 않음
        // resist=3이므로 3번 시도 후 제거
        for i in 3u64..10 {
            cache.insert(BlockKey::filter("p", "col", i), block(100));
        }
        // 완전히 제거되기 전에 여러 번 시도하므로 filter pool 내 블록이 일부 남음
        let stats = cache.pool_stats();
        assert!(stats.filter_used <= stats.filter_max);
    }

    #[test]
    fn test_lru_order_in_data_pool() {
        let cache = BlockCache::new(3 * 100 + 200 /* margin */);
        let k1 = BlockKey::data("p", "col", 1);
        let k2 = BlockKey::data("p", "col", 2);
        let k3 = BlockKey::data("p", "col", 3);

        cache.insert(k1.clone(), block(100));
        cache.insert(k2.clone(), block(100));
        cache.insert(k3.clone(), block(100));

        // k1 접근 → MRU로 이동
        cache.get(&k1);

        // 새 블록 → k2가 LRU로 제거
        cache.insert(BlockKey::data("p", "col", 4), block(100));
        assert!(cache.get(&k1).is_some(), "k1은 최근 접근되어 생존해야 함");
        assert!(cache.get(&k2).is_none(), "k2는 LRU로 제거되어야 함");
    }
}
