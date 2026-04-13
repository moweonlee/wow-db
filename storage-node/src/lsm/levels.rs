// T107: LevelState — L0~L6 레벨 구조, CompactionConfig, compaction_score, L1+ 비중첩 불변 조건

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ─── SSTable 참조 (레벨 내 단위) ─────────────────────────────────────────────

/// 레벨 내 하나의 SSTable 파일에 대한 참조
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SstRef {
    pub id:             Uuid,
    /// SSTable 파티션 내 단조 증가 시퀀스 번호.
    /// 동일 Sort Key가 여러 SSTable에 존재할 때 이 값이 큰 쪽이 최신 버전.
    pub sequence_num:   u64,
    /// Compaction 세대 번호 (0 = MemTable flush, N = N번째 compact 출력)
    pub generation:     u64,
    /// 이 SSTable을 만들 때 입력이 된 SSTable ID 목록 (계보 추적)
    pub compacted_from: Vec<Uuid>,
    pub level:          u32,
    pub path:           PathBuf,
    pub size_bytes:     u64,
    pub row_count:      u64,
    pub min_sort_key:   Vec<u8>,
    pub max_sort_key:   Vec<u8>,
}

impl SstRef {
    /// key가 이 SSTable의 key range 내에 있는지 (byte-comparable 비교)
    pub fn key_in_range(&self, key: &[u8]) -> bool {
        key >= self.min_sort_key.as_slice() && key <= self.max_sort_key.as_slice()
    }

    /// 두 SSTable의 key range가 겹치는지
    pub fn overlaps_with(&self, other: &SstRef) -> bool {
        self.min_sort_key <= other.max_sort_key && other.min_sort_key <= self.max_sort_key
    }
}

// ─── 레벨별 상태 ──────────────────────────────────────────────────────────────

/// 한 파티션의 전체 레벨 상태
#[derive(Debug, Default)]
pub struct PartitionLevels {
    /// levels[0] = L0, levels[1] = L1, …, levels[6] = L6
    pub levels: Vec<Vec<SstRef>>,
    config: CompactionConfig,
}

impl PartitionLevels {
    pub fn new(config: CompactionConfig) -> Self {
        let n = config.max_levels as usize + 1; // L0..=L6
        Self { levels: vec![Vec::new(); n], config }
    }

    // ─── 등록 ────────────────────────────────────────────────────────────

    pub fn add(&mut self, sst: SstRef) {
        let lvl = sst.level as usize;
        self.levels[lvl].push(sst);
        // L1+ 정렬 유지 (min_sort_key 기준)
        if lvl > 0 {
            self.levels[lvl].sort_by(|a, b| a.min_sort_key.cmp(&b.min_sort_key));
        }
    }

    pub fn remove(&mut self, id: Uuid) {
        for level in self.levels.iter_mut() {
            level.retain(|s| s.id != id);
        }
    }

    // ─── 통계 ─────────────────────────────────────────────────────────────

    pub fn level_size_bytes(&self, lvl: usize) -> u64 {
        self.levels.get(lvl).map_or(0, |v| v.iter().map(|s| s.size_bytes).sum())
    }

    pub fn l0_count(&self) -> usize {
        self.levels.first().map_or(0, |v| v.len())
    }

    /// 레벨 k의 compaction 점수 (1.0 이상이면 compaction 필요)
    /// - L0: 현재 파일 수 / l0_file_compaction_trigger
    /// - L1+: 현재 크기 / target_level_size_bytes(k)
    pub fn compaction_score(&self, lvl: usize) -> f64 {
        if lvl == 0 {
            self.l0_count() as f64 / self.config.l0_file_compaction_trigger as f64
        } else {
            let target = self.config.target_level_size_bytes(lvl);
            self.level_size_bytes(lvl) as f64 / target as f64
        }
    }

    /// 가장 점수가 높은 레벨 (compaction 우선순위) 반환
    pub fn highest_priority_level(&self) -> Option<usize> {
        (0..self.levels.len())
            .filter(|&lvl| !self.levels[lvl].is_empty())
            .max_by(|&a, &b| {
                self.compaction_score(a)
                    .partial_cmp(&self.compaction_score(b))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .filter(|&lvl| self.compaction_score(lvl) >= 1.0)
    }

    // ─── 불변 조건 검증 ────────────────────────────────────────────────────

    /// L1+ 레벨에서 key range 중첩 여부 검사 (런타임 불변 조건 확인)
    /// 중첩이 발견되면 중첩된 SSTable 쌍의 ID를 반환.
    pub fn verify_non_overlapping(&self) -> Result<(), NonOverlapViolation> {
        for lvl in 1..self.levels.len() {
            let ssts = &self.levels[lvl];
            for i in 0..ssts.len() {
                for j in (i + 1)..ssts.len() {
                    if ssts[i].overlaps_with(&ssts[j]) {
                        return Err(NonOverlapViolation {
                            level: lvl as u32,
                            sst_a: ssts[i].id,
                            sst_b: ssts[j].id,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// L0 쓰기 제어 상태 반환
    pub fn write_control(&self) -> WriteControl {
        let l0 = self.l0_count();
        let cfg = &self.config;
        if l0 >= cfg.l0_stop_write_trigger {
            WriteControl::Stop
        } else if l0 >= cfg.l0_slowdown_write_trigger {
            WriteControl::Slowdown
        } else {
            WriteControl::Normal
        }
    }
}

// ─── 비중첩 위반 에러 ─────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct NonOverlapViolation {
    pub level: u32,
    pub sst_a: Uuid,
    pub sst_b: Uuid,
}

impl std::fmt::Display for NonOverlapViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "L{} 비중첩 불변 조건 위반: {} ↔ {}",
            self.level, self.sst_a, self.sst_b
        )
    }
}

impl std::error::Error for NonOverlapViolation {}

// ─── 쓰기 제어 상태 ──────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq)]
pub enum WriteControl {
    Normal,
    /// L0 파일이 slowdown 임계값 도달 — 쓰기 속도 제한
    Slowdown,
    /// L0 파일이 stop 임계값 도달 — 쓰기 완전 차단
    Stop,
}

// ─── Compaction 설정 ──────────────────────────────────────────────────────────

/// Leveled Compaction 설정
/// `lsm-engine.md §6` 기준
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// L0 파일 수가 이 값 이상이면 compaction 트리거 (기본 4)
    pub l0_file_compaction_trigger: usize,
    /// L0 파일 수가 이 값 이상이면 쓰기 슬로우다운 (기본 8)
    pub l0_slowdown_write_trigger: usize,
    /// L0 파일 수가 이 값 이상이면 쓰기 완전 차단 (기본 12)
    pub l0_stop_write_trigger: usize,
    /// L1 목표 크기 (기본 256 MiB)
    pub l1_max_bytes: u64,
    /// 레벨별 크기 배수 (기본 10 — L2=2.56GB, L3=25.6GB, ...)
    pub level_size_multiplier: u64,
    /// 최대 레벨 번호 (기본 6, 즉 L0..=L6)
    pub max_levels: u32,
    /// SSTable 목표 파일 크기 (기본 64 MiB)
    pub target_file_size_bytes: u64,
    /// Compaction 백그라운드 체크 주기 (ms)
    pub check_interval_ms: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            l0_file_compaction_trigger: 4,
            l0_slowdown_write_trigger:  8,
            l0_stop_write_trigger:      12,
            l1_max_bytes:               256 * 1024 * 1024,  // 256 MiB
            level_size_multiplier:      10,
            max_levels:                 6,
            target_file_size_bytes:     64 * 1024 * 1024,   // 64 MiB
            check_interval_ms:          1_000,
        }
    }
}

impl CompactionConfig {
    /// 레벨 k의 목표 크기 (bytes)
    /// L1 = l1_max_bytes, Lk = L1 × multiplier^(k-1)
    pub fn target_level_size_bytes(&self, lvl: usize) -> u64 {
        if lvl == 0 {
            return 0; // L0는 파일 수 기준
        }
        let exp = (lvl - 1) as u32;
        self.l1_max_bytes.saturating_mul(self.level_size_multiplier.saturating_pow(exp))
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sst(id: Uuid, seq: u64, min: &[u8], max: &[u8]) -> SstRef {
        SstRef {
            id,
            sequence_num: seq,
            generation: 0,
            compacted_from: vec![],
            level: 1,
            path: PathBuf::from(format!("seg-{seq}.col")),
            size_bytes: 1024,
            row_count: 10,
            min_sort_key: min.to_vec(),
            max_sort_key: max.to_vec(),
        }
    }

    #[test]
    fn test_no_overlap_ok() {
        let mut levels = PartitionLevels::new(CompactionConfig::default());
        levels.add(sst(Uuid::new_v4(), 1, b"a", b"c"));
        levels.add(sst(Uuid::new_v4(), 2, b"d", b"f"));
        assert!(levels.verify_non_overlapping().is_ok());
    }

    #[test]
    fn test_overlap_detected() {
        let mut levels = PartitionLevels::new(CompactionConfig::default());
        levels.add(sst(Uuid::new_v4(), 1, b"a", b"e"));
        levels.add(sst(Uuid::new_v4(), 2, b"c", b"g")); // overlaps with first
        assert!(levels.verify_non_overlapping().is_err());
    }

    #[test]
    fn test_compaction_score_l0() {
        let mut levels = PartitionLevels::new(CompactionConfig::default());
        // 4개 = trigger → score = 1.0
        for i in 0..4 {
            let mut s = sst(Uuid::new_v4(), i, b"a", b"z");
            s.level = 0;
            levels.levels[0].push(s);
        }
        assert!((levels.compaction_score(0) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_target_level_size() {
        let cfg = CompactionConfig::default();
        assert_eq!(cfg.target_level_size_bytes(1), 256 * 1024 * 1024);
        assert_eq!(cfg.target_level_size_bytes(2), 256 * 1024 * 1024 * 10);
        assert_eq!(cfg.target_level_size_bytes(3), 256 * 1024 * 1024 * 100);
    }

    #[test]
    fn test_write_control() {
        let cfg = CompactionConfig { l0_slowdown_write_trigger: 8, l0_stop_write_trigger: 12, ..Default::default() };
        let mut levels = PartitionLevels::new(cfg);
        assert_eq!(levels.write_control(), WriteControl::Normal);

        for i in 0..8 {
            let mut s = sst(Uuid::new_v4(), i as u64, b"a", b"z");
            s.level = 0;
            levels.levels[0].push(s);
        }
        assert_eq!(levels.write_control(), WriteControl::Slowdown);

        for i in 8..12 {
            let mut s = sst(Uuid::new_v4(), i as u64, b"a", b"z");
            s.level = 0;
            levels.levels[0].push(s);
        }
        assert_eq!(levels.write_control(), WriteControl::Stop);
    }
}
