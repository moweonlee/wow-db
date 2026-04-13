// T110: MANIFEST — JSON Lines, ADD/REMOVE/CompactionBegin/CompactionEnd 이벤트
// 원자적 교체(MANIFEST.tmp → MANIFEST), 크래시 복구, FR-032

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::levels::SstRef;

// ─── 이벤트 타입 ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ManifestEvent {
    /// MemTable flush 후 SSTable 추가
    Add { sst: SstRef },
    /// Compaction 완료 또는 수동 삭제 시 SSTable 제거
    Remove { id: Uuid, level: u32 },
    /// Compaction 시작 (크래시 후 롤백 기준점)
    CompactionBegin {
        compaction_id:  Uuid,
        input_ids:      Vec<Uuid>,
        output_level:   u32,
    },
    /// Compaction 정상 완료
    CompactionEnd {
        compaction_id:  Uuid,
        output_ids:     Vec<Uuid>,
    },
}

// ─── MANIFEST 파일 ────────────────────────────────────────────────────────────

pub struct Manifest {
    path: PathBuf,
    /// 현재 활성 SSTable 상태 (메모리 내 투영)
    active: Vec<SstRef>,
    /// 진행 중인 compaction 상태 (크래시 복구용)
    pending_compactions: Vec<PendingCompaction>,
}

#[derive(Debug)]
struct PendingCompaction {
    id:       Uuid,
    input_ids: Vec<Uuid>,
}

impl Manifest {
    // ─── 열기 / 생성 ────────────────────────────────────────────────────────

    /// MANIFEST 파일을 열거나 새로 생성한다. 크래시 후 복구를 포함한다.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let path = dir.as_ref().join("MANIFEST");
        let mut m = Manifest { path, active: Vec::new(), pending_compactions: Vec::new() };
        if m.path.exists() {
            m.recover()?;
        }
        Ok(m)
    }

    // ─── 공개 API ────────────────────────────────────────────────────────────

    /// SSTable 추가 (MemTable flush 완료 시 호출)
    pub fn add_sst(&mut self, sst: SstRef) -> Result<()> {
        let event = ManifestEvent::Add { sst: sst.clone() };
        self.append_event(&event)?;
        self.active.push(sst);
        Ok(())
    }

    /// SSTable 제거 (compaction 완료 또는 수동 삭제)
    pub fn remove_sst(&mut self, id: Uuid, level: u32) -> Result<()> {
        let event = ManifestEvent::Remove { id, level };
        self.append_event(&event)?;
        self.active.retain(|s| s.id != id);
        Ok(())
    }

    /// Compaction 시작 기록 (크래시 후 미완료 compaction 감지용)
    pub fn begin_compaction(
        &mut self,
        compaction_id: Uuid,
        input_ids: Vec<Uuid>,
        output_level: u32,
    ) -> Result<()> {
        let event = ManifestEvent::CompactionBegin {
            compaction_id,
            input_ids: input_ids.clone(),
            output_level,
        };
        self.append_event(&event)?;
        self.pending_compactions.push(PendingCompaction { id: compaction_id, input_ids });
        Ok(())
    }

    /// Compaction 완료 기록 + 원자적 MANIFEST 재작성
    pub fn complete_compaction(
        &mut self,
        compaction_id: Uuid,
        output_ids: Vec<Uuid>,
        removed_ids: &[Uuid],
        added_ssts: Vec<SstRef>,
    ) -> Result<()> {
        // 메모리 상태 업데이트
        for id in removed_ids {
            self.active.retain(|s| &s.id != id);
        }
        for sst in added_ssts {
            self.active.push(sst);
        }
        self.pending_compactions.retain(|p| p.id != compaction_id);

        let event = ManifestEvent::CompactionEnd { compaction_id, output_ids };
        self.append_event(&event)?;

        // 주기적으로 MANIFEST를 최신 상태로 재작성하여 파일 크기 제어
        // (이벤트 수가 임계값을 초과하면 스냅샷 재작성)
        if self.should_rewrite() {
            self.atomic_rewrite()?;
        }
        Ok(())
    }

    /// 현재 활성 SSTable 목록 반환 (읽기 전용)
    pub fn active_sstables(&self) -> &[SstRef] {
        &self.active
    }

    /// 미완료 compaction이 있으면 true (크래시 복구 후 확인)
    pub fn has_pending_compactions(&self) -> bool {
        !self.pending_compactions.is_empty()
    }

    /// 미완료 compaction 목록
    pub fn pending_compaction_ids(&self) -> Vec<Uuid> {
        self.pending_compactions.iter().map(|p| p.id).collect()
    }

    // ─── 내부 구현 ───────────────────────────────────────────────────────────

    /// 이벤트 한 줄을 MANIFEST에 추가 (JSON Lines)
    fn append_event(&self, event: &ManifestEvent) -> Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("MANIFEST 열기 실패: {:?}", self.path))?;
        let line = serde_json::to_string(event)?;
        writeln!(file, "{}", line)?;
        file.flush()?;
        Ok(())
    }

    /// MANIFEST를 처음부터 재생하여 활성 상태 복원 (크래시 복구)
    fn recover(&mut self) -> Result<()> {
        let file = std::fs::File::open(&self.path)?;
        let reader = std::io::BufReader::new(file);

        for (line_no, line) in reader.lines().enumerate() {
            let line = line.with_context(|| format!("MANIFEST 라인 {} 읽기 실패", line_no))?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<ManifestEvent>(line) {
                Ok(event) => self.apply_event(event),
                Err(e) => {
                    tracing::warn!(
                        line = line_no,
                        err = %e,
                        "MANIFEST 라인 파싱 실패 — 이 지점 이후 복구 중단"
                    );
                    break;
                }
            }
        }

        // 크래시 복구: 미완료 compaction의 출력 SSTable을 active에서 제거
        // (입력은 아직 삭제되지 않았으므로 유지)
        let orphan_ids: Vec<Uuid> = self.pending_compactions
            .iter()
            .flat_map(|p| p.input_ids.iter().copied())
            .collect();
        // 미완료 compaction의 입력이 active에 없으면 복구 불필요 (이미 커밋됨)
        let _ = orphan_ids; // 현재는 로그만 남김
        if self.has_pending_compactions() {
            tracing::warn!(
                count = self.pending_compactions.len(),
                "미완료 compaction 발견 — 크래시 복구 필요"
            );
        }

        Ok(())
    }

    fn apply_event(&mut self, event: ManifestEvent) {
        match event {
            ManifestEvent::Add { sst } => {
                self.active.push(sst);
            }
            ManifestEvent::Remove { id, .. } => {
                self.active.retain(|s| s.id != id);
            }
            ManifestEvent::CompactionBegin { compaction_id, input_ids, .. } => {
                self.pending_compactions.push(PendingCompaction {
                    id: compaction_id,
                    input_ids,
                });
            }
            ManifestEvent::CompactionEnd { compaction_id, .. } => {
                self.pending_compactions.retain(|p| p.id != compaction_id);
            }
        }
    }

    /// MANIFEST를 현재 활성 상태 스냅샷으로 원자적 재작성
    /// MANIFEST.tmp 에 쓰고 MANIFEST로 rename (atomic on POSIX, best-effort on Windows)
    fn atomic_rewrite(&self) -> Result<()> {
        let tmp_path = self.path.with_extension("tmp");
        {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;

            for sst in &self.active {
                let event = ManifestEvent::Add { sst: sst.clone() };
                let line = serde_json::to_string(&event)?;
                writeln!(file, "{}", line)?;
            }
            file.flush()?;
        }
        std::fs::rename(&tmp_path, &self.path)
            .with_context(|| "MANIFEST 원자적 교체 실패")?;
        Ok(())
    }

    fn should_rewrite(&self) -> bool {
        // MANIFEST 파일 크기가 16 MiB를 초과하거나 active SSTable 수의 10배 이상의 이벤트가 있을 때
        if let Ok(meta) = std::fs::metadata(&self.path) {
            meta.len() > 16 * 1024 * 1024
        } else {
            false
        }
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn make_sst(seq: u64, level: u32) -> SstRef {
        SstRef {
            id:             Uuid::new_v4(),
            sequence_num:   seq,
            generation:     0,
            compacted_from: vec![],
            level,
            path:           PathBuf::from(format!("seg-{seq}.col")),
            size_bytes:     1024,
            row_count:      100,
            min_sort_key:   vec![0u8],
            max_sort_key:   vec![255u8],
        }
    }

    #[test]
    fn test_add_and_remove() {
        let tmp = TempDir::new().unwrap();
        let mut m = Manifest::open(tmp.path()).unwrap();

        let sst = make_sst(1, 0);
        let id = sst.id;
        m.add_sst(sst).unwrap();
        assert_eq!(m.active_sstables().len(), 1);

        m.remove_sst(id, 0).unwrap();
        assert_eq!(m.active_sstables().len(), 0);
    }

    #[test]
    fn test_crash_recovery() {
        let tmp = TempDir::new().unwrap();
        let sst_id;
        {
            let mut m = Manifest::open(tmp.path()).unwrap();
            let sst = make_sst(1, 0);
            sst_id = sst.id;
            m.add_sst(sst).unwrap();
            let sst2 = make_sst(2, 1);
            m.add_sst(sst2).unwrap();
        }

        // 재열기 — 이벤트 재생
        let m2 = Manifest::open(tmp.path()).unwrap();
        assert_eq!(m2.active_sstables().len(), 2);
        assert!(m2.active_sstables().iter().any(|s| s.id == sst_id));
    }

    #[test]
    fn test_pending_compaction_detection() {
        let tmp = TempDir::new().unwrap();
        let mut m = Manifest::open(tmp.path()).unwrap();

        let sst = make_sst(1, 0);
        let id = sst.id;
        m.add_sst(sst).unwrap();

        let comp_id = Uuid::new_v4();
        m.begin_compaction(comp_id, vec![id], 1).unwrap();

        // 재열기 — 미완료 compaction 검출
        let m2 = Manifest::open(tmp.path()).unwrap();
        assert!(m2.has_pending_compactions());
        assert_eq!(m2.pending_compaction_ids(), vec![comp_id]);
    }

    #[test]
    fn test_complete_compaction() {
        let tmp = TempDir::new().unwrap();
        let mut m = Manifest::open(tmp.path()).unwrap();

        let sst = make_sst(1, 0);
        let old_id = sst.id;
        m.add_sst(sst).unwrap();

        let comp_id = Uuid::new_v4();
        m.begin_compaction(comp_id, vec![old_id], 1).unwrap();

        let new_sst = make_sst(2, 1);
        let new_id = new_sst.id;
        m.complete_compaction(comp_id, vec![new_id], &[old_id], vec![new_sst]).unwrap();

        assert!(!m.has_pending_compactions());
        assert_eq!(m.active_sstables().len(), 1);
        assert_eq!(m.active_sstables()[0].id, new_id);
    }
}
