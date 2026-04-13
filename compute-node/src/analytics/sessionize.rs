// T081: SMV 구체화 실행기 — User Key 기준 이벤트 그룹화, Timeout 기반 세션 경계 결정, session_id UUID 생성

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ─── 이벤트 행 (입력) ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct EventRow {
    /// 사용자 식별 컬럼 값
    pub user_key:   String,
    /// 이벤트 발생 시각 (Unix epoch 초)
    pub event_time: i64,
    /// 이벤트 이름
    pub event_name: String,
    /// 기타 컬럼 (key → 직렬화된 값)
    pub extra:      HashMap<String, String>,
}

// ─── 세션 행 (출력) ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRow {
    /// 세션 고유 ID
    pub session_id:     String,
    /// 사용자 식별 값
    pub user_key:       String,
    /// 세션 시작 시각 (Unix epoch 초)
    pub session_start:  i64,
    /// 세션 종료 시각 (마지막 이벤트 시각)
    pub session_end:    i64,
    /// 세션 내 이벤트 수
    pub event_count:    u32,
    /// 세션 내 이벤트 이름 목록 (순서 유지)
    pub event_sequence: Vec<String>,
    /// 세션 지속 시간 (초)
    pub duration_secs:  i64,
}

impl SessionRow {
    fn new(user_key: String, first_event_time: i64, first_event_name: String) -> Self {
        Self {
            session_id:     Uuid::new_v4().to_string(),
            user_key,
            session_start:  first_event_time,
            session_end:    first_event_time,
            event_count:    1,
            event_sequence: vec![first_event_name],
            duration_secs:  0,
        }
    }

    fn add_event(&mut self, event_time: i64, event_name: String) {
        self.session_end = event_time;
        self.event_count += 1;
        self.event_sequence.push(event_name);
        self.duration_secs = self.session_end - self.session_start;
    }
}

// ─── Sessionizer ─────────────────────────────────────────────────────────────

/// User Key 기준으로 이벤트를 세션으로 변환하는 실행기
pub struct Sessionizer {
    /// 세션 타임아웃 (초): 연속 이벤트 간 허용 최대 비활성 시간
    session_timeout_secs: i64,
}

impl Sessionizer {
    pub fn new(session_timeout_secs: u64) -> Self {
        Self { session_timeout_secs: session_timeout_secs as i64 }
    }

    /// 이벤트 배치를 세션 목록으로 변환
    ///
    /// 처리 순서:
    /// 1. User Key별 이벤트 그룹화
    /// 2. 각 그룹을 event_time 기준으로 정렬
    /// 3. 연속 이벤트 간 간격이 timeout 초과하면 새 세션 시작
    pub fn sessionize(&self, events: Vec<EventRow>) -> Vec<SessionRow> {
        // User Key별 그룹화 + event_time 정렬
        let mut by_user: HashMap<String, Vec<EventRow>> = HashMap::new();
        for ev in events {
            by_user.entry(ev.user_key.clone()).or_default().push(ev);
        }

        let mut sessions = Vec::new();

        for (user_key, mut user_events) in by_user {
            user_events.sort_by_key(|e| e.event_time);
            let user_sessions = self.build_sessions(user_key, user_events);
            sessions.extend(user_sessions);
        }

        sessions
    }

    /// 단일 사용자의 이벤트 → 세션 목록
    fn build_sessions(&self, user_key: String, events: Vec<EventRow>) -> Vec<SessionRow> {
        if events.is_empty() { return Vec::new(); }

        let mut sessions = Vec::new();
        let first = &events[0];
        let mut current = SessionRow::new(
            user_key.clone(),
            first.event_time,
            first.event_name.clone(),
        );

        for ev in events.iter().skip(1) {
            let gap = ev.event_time - current.session_end;
            if gap > self.session_timeout_secs {
                // 새 세션 시작
                sessions.push(current);
                current = SessionRow::new(
                    user_key.clone(),
                    ev.event_time,
                    ev.event_name.clone(),
                );
            } else {
                current.add_event(ev.event_time, ev.event_name.clone());
            }
        }
        sessions.push(current);
        sessions
    }
}

// ─── 통계 ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct SessionizeStats {
    pub input_events: u64,
    pub output_sessions: u64,
    pub unique_users: u64,
    pub avg_events_per_session: f64,
}

impl SessionizeStats {
    pub fn compute(input: &[EventRow], output: &[SessionRow]) -> Self {
        let unique_users = {
            let mut users = std::collections::HashSet::new();
            for ev in input { users.insert(&ev.user_key); }
            users.len() as u64
        };
        let total_events: u64 = output.iter().map(|s| s.event_count as u64).sum();
        let n = output.len() as u64;
        Self {
            input_events:         input.len() as u64,
            output_sessions:      n,
            unique_users,
            avg_events_per_session: if n > 0 { total_events as f64 / n as f64 } else { 0.0 },
        }
    }
}

// ─── 단위 테스트 ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn event(user: &str, ts: i64, name: &str) -> EventRow {
        EventRow {
            user_key:   user.to_string(),
            event_time: ts,
            event_name: name.to_string(),
            extra:      HashMap::new(),
        }
    }

    #[test]
    fn test_single_session() {
        let sessionizer = Sessionizer::new(1800); // 30분

        let events = vec![
            event("user1", 0,    "page_view"),
            event("user1", 60,   "click"),
            event("user1", 120,  "purchase"),
        ];

        let sessions = sessionizer.sessionize(events);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].event_count, 3);
        assert_eq!(sessions[0].session_start, 0);
        assert_eq!(sessions[0].session_end, 120);
        assert_eq!(sessions[0].duration_secs, 120);
        assert_eq!(sessions[0].event_sequence, vec!["page_view", "click", "purchase"]);
    }

    #[test]
    fn test_session_split_on_timeout() {
        let sessionizer = Sessionizer::new(1800); // 30분 = 1800초

        let events = vec![
            event("user1", 0,    "page_view"),
            event("user1", 900,  "click"),      // 15분 후 (동일 세션)
            event("user1", 3600, "purchase"),   // 30분+ 후 (새 세션)
        ];

        let sessions = sessionizer.sessionize(events);
        assert_eq!(sessions.len(), 2);
        // 첫 세션: page_view, click
        // 두 번째 세션: purchase
        let with_view = sessions.iter().find(|s| s.event_sequence.contains(&"page_view".to_string())).unwrap();
        assert_eq!(with_view.event_count, 2);

        let with_purchase = sessions.iter().find(|s| s.event_sequence.contains(&"purchase".to_string())).unwrap();
        assert_eq!(with_purchase.event_count, 1);
    }

    #[test]
    fn test_multiple_users() {
        let sessionizer = Sessionizer::new(1800);

        let events = vec![
            event("user1", 0,   "view"),
            event("user2", 10,  "click"),
            event("user1", 60,  "click"),
            event("user2", 900, "view"),
        ];

        let sessions = sessionizer.sessionize(events);
        // 각 유저마다 1 세션 = 총 2 세션
        assert_eq!(sessions.len(), 2);

        let user1_sessions: Vec<_> = sessions.iter().filter(|s| s.user_key == "user1").collect();
        let user2_sessions: Vec<_> = sessions.iter().filter(|s| s.user_key == "user2").collect();

        assert_eq!(user1_sessions.len(), 1);
        assert_eq!(user1_sessions[0].event_count, 2);

        assert_eq!(user2_sessions.len(), 1);
        assert_eq!(user2_sessions[0].event_count, 2);
    }

    #[test]
    fn test_empty_input() {
        let sessionizer = Sessionizer::new(1800);
        let sessions = sessionizer.sessionize(vec![]);
        assert!(sessions.is_empty());
    }

    #[test]
    fn test_session_ids_unique() {
        let sessionizer = Sessionizer::new(60);

        // 2개 세션 생성 (간격 > 60초)
        let events = vec![
            event("u1", 0,    "ev"),
            event("u1", 3600, "ev"),
        ];

        let sessions = sessionizer.sessionize(events);
        assert_eq!(sessions.len(), 2);
        assert_ne!(sessions[0].session_id, sessions[1].session_id, "세션 ID는 고유해야 함");
    }

    #[test]
    fn test_stats() {
        let sessionizer = Sessionizer::new(1800);
        let events = vec![
            event("u1", 0, "a"),
            event("u1", 100, "b"),
            event("u2", 0, "c"),
        ];
        let sessions = sessionizer.sessionize(events.clone());
        let stats = SessionizeStats::compute(&events, &sessions);
        assert_eq!(stats.input_events, 3);
        assert_eq!(stats.output_sessions, 2);
        assert_eq!(stats.unique_users, 2);
    }
}
