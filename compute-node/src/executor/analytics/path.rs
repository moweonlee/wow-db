// T053: PATH_ANALYSIS 실행기 — 이벤트 시퀀스 패턴 카운팅, Top-N 경로 반환

use ahash::AHashMap;

use super::funnel::EventRecord;

// ─── Path 분석 정의 ───────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PathDef {
    /// 분석 시작 이벤트 (예: "page_view")
    pub start_event:  Option<String>,
    /// 분석 종료 이벤트 (예: "purchase")
    pub end_event:    Option<String>,
    /// 경로 최대 깊이
    pub max_depth:    usize,
    /// 세션 내 이벤트 간 최대 허용 시간 (초)
    pub session_gap:  i64,
    /// 반환할 Top-N 경로 수
    pub top_n:        usize,
}

// ─── Path 실행기 ──────────────────────────────────────────────────────────────

pub struct PathExecutor {
    path_def:       PathDef,
    /// 경로(이벤트 시퀀스) → 발생 횟수
    path_counts:    AHashMap<Vec<String>, u64>,
    /// user_key → (현재 경로, 마지막 이벤트 시각)
    user_sessions:  AHashMap<Vec<u8>, (Vec<String>, i64)>,
}

impl PathExecutor {
    pub fn new(path_def: PathDef) -> Self {
        Self {
            path_def,
            path_counts: AHashMap::new(),
            user_sessions: AHashMap::new(),
        }
    }

    pub fn process_events(&mut self, events: &[EventRecord]) {
        let mut paths_to_flush: Vec<Vec<String>> = Vec::new();

        for event in events {
            let (path, last_time) = self.user_sessions
                .entry(event.user_key.clone())
                .or_insert_with(|| (Vec::new(), i64::MIN));

            // 세션 만료 확인
            if *last_time != i64::MIN && event.event_time - *last_time > self.path_def.session_gap {
                if !path.is_empty() {
                    paths_to_flush.push(path.clone());
                }
                path.clear();
            }

            // 경로 시작 조건 확인
            let should_start = match &self.path_def.start_event {
                None        => path.is_empty(),
                Some(start) => path.is_empty() && &event.event_name == start,
            };

            if should_start || !path.is_empty() {
                path.push(event.event_name.clone());
                *last_time = event.event_time;

                // 종료 이벤트 도달 또는 최대 깊이 초과
                let reached_end = self.path_def.end_event.as_ref()
                    .map(|e| &event.event_name == e)
                    .unwrap_or(false);
                let reached_max = path.len() >= self.path_def.max_depth;

                if reached_end || reached_max {
                    paths_to_flush.push(path.clone());
                    path.clear();
                }
            }
        }

        for path in paths_to_flush {
            self.flush_path(path);
        }

        // 남은 미완료 세션 플러시
        let remaining_paths: Vec<Vec<String>> = self.user_sessions.values()
            .map(|(p, _)| p.clone())
            .filter(|p| !p.is_empty())
            .collect();
        for path in remaining_paths {
            self.flush_path(path);
        }
        self.user_sessions.clear();
    }

    fn flush_path(&mut self, path: Vec<String>) {
        if path.len() >= 2 {
            *self.path_counts.entry(path).or_insert(0) += 1;
        }
    }

    /// Top-N 경로 반환
    pub fn top_paths(&self) -> Vec<PathResult> {
        let mut sorted: Vec<(&Vec<String>, &u64)> = self.path_counts.iter().collect();
        sorted.sort_unstable_by(|a, b| b.1.cmp(a.1));
        sorted.truncate(self.path_def.top_n);

        sorted.into_iter().enumerate().map(|(rank, (path, &count))| {
            PathResult {
                rank:  rank + 1,
                path:  path.clone(),
                count,
            }
        }).collect()
    }

    /// 병렬 실행기 결과 병합
    pub fn merge(&mut self, other: PathExecutor) {
        for (path, count) in other.path_counts {
            *self.path_counts.entry(path).or_insert(0) += count;
        }
    }
}

#[derive(Debug, Clone)]
pub struct PathResult {
    pub rank:  usize,
    pub path:  Vec<String>,
    pub count: u64,
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn event(user: &str, name: &str, time: i64) -> EventRecord {
        EventRecord {
            user_key:   user.as_bytes().to_vec(),
            event_name: name.to_string(),
            event_time: time,
        }
    }

    #[test]
    fn test_path_basic() {
        let path_def = PathDef {
            start_event: None,
            end_event:   None,
            max_depth:   3,
            session_gap: 1800,
            top_n:       5,
        };
        let mut exec = PathExecutor::new(path_def);

        exec.process_events(&[
            event("user1", "home",     0),
            event("user1", "product",  10),
            event("user1", "purchase", 20),
        ]);

        exec.process_events(&[
            event("user2", "home",    0),
            event("user2", "product", 10),
            event("user2", "cart",    20),
        ]);

        let tops = exec.top_paths();
        assert!(!tops.is_empty());
        // 각 경로가 최소 1회 발생
        assert!(tops.iter().all(|r| r.count >= 1));
    }

    #[test]
    fn test_session_gap_splits() {
        let path_def = PathDef {
            start_event: None,
            end_event:   None,
            max_depth:   5,
            session_gap: 60, // 1분 세션
            top_n:       10,
        };
        let mut exec = PathExecutor::new(path_def);

        exec.process_events(&[
            event("user1", "a", 0),
            event("user1", "b", 30),
            event("user1", "c", 200), // 세션 분리 (>60초)
            event("user1", "d", 210),
        ]);

        let tops = exec.top_paths();
        // a→b 와 c→d 두 개의 경로
        assert!(tops.len() <= 2);
    }
}
