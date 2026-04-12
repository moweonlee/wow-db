// T051: FUNNEL_COUNT 실행기 — 비트마스크 Step 달성 추적, Time Window 필터, 사용자별 집계

use std::collections::HashMap;

use ahash::AHashMap;
use anyhow::Result;

// ─── Funnel 정의 ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FunnelStep {
    /// 이벤트 이름 (예: "page_view", "add_to_cart", "purchase")
    pub event_name: String,
    /// 추가 필터 조건 (예: page_url = '/checkout')
    pub filter:     Option<String>,
}

#[derive(Debug, Clone)]
pub struct FunnelDef {
    pub steps:      Vec<FunnelStep>,
    /// 퍼널 완료 허용 시간 윈도우 (초)
    pub window_sec: i64,
}

// ─── 이벤트 레코드 ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct EventRecord {
    pub user_key:   Vec<u8>,   // 사용자 식별자
    pub event_name: String,
    pub event_time: i64,       // Unix timestamp (초)
}

// ─── Funnel 실행기 ────────────────────────────────────────────────────────────

/// 사용자별 퍼널 달성 상태
#[derive(Debug, Default)]
struct UserFunnelState {
    /// 비트마스크: step i 달성 여부
    achieved:   u64,
    /// 마지막으로 달성한 step의 시각
    last_times: Vec<Option<i64>>,
}

pub struct FunnelExecutor {
    funnel: FunnelDef,
    /// user_key → 퍼널 상태
    states: AHashMap<Vec<u8>, UserFunnelState>,
}

impl FunnelExecutor {
    pub fn new(funnel: FunnelDef) -> Self {
        Self { funnel, states: AHashMap::new() }
    }

    /// 이벤트 배치를 처리 (이미 user_key 기준으로 정렬되어 있어야 함)
    pub fn process_events(&mut self, events: &[EventRecord]) {
        let n_steps = self.funnel.steps.len();

        for event in events {
            let state = self.states.entry(event.user_key.clone()).or_insert_with(|| {
                UserFunnelState {
                    achieved:   0,
                    last_times: vec![None; n_steps],
                }
            });

            // 이벤트가 어느 step에 해당하는지 확인
            for (step_idx, step) in self.funnel.steps.iter().enumerate() {
                if event.event_name != step.event_name {
                    continue;
                }

                if step_idx == 0 {
                    // Step 0: 무조건 달성 (또는 이전 달성 시각 갱신)
                    state.achieved |= 1 << step_idx;
                    state.last_times[0] = Some(event.event_time);
                } else {
                    // Step N: 이전 step이 달성되어 있고, 시간 윈도우 내인지 확인
                    let prev_bit = 1u64 << (step_idx - 1);
                    if state.achieved & prev_bit != 0 {
                        let prev_time = state.last_times[step_idx - 1].unwrap_or(i64::MIN);
                        let elapsed = event.event_time - prev_time;
                        if elapsed >= 0 && elapsed <= self.funnel.window_sec {
                            state.achieved |= 1u64 << step_idx;
                            state.last_times[step_idx] = Some(event.event_time);
                        }
                    }
                }
            }
        }
    }

    /// 각 step별 달성 사용자 수 반환
    pub fn step_counts(&self) -> Vec<u64> {
        let n = self.funnel.steps.len();
        let mut counts = vec![0u64; n];
        for state in self.states.values() {
            for step in 0..n {
                if state.achieved & (1u64 << step) != 0 {
                    counts[step] += 1;
                }
            }
        }
        counts
    }

    /// 전환율 (각 step 달성자 / step 0 달성자)
    pub fn conversion_rates(&self) -> Vec<f64> {
        let counts = self.step_counts();
        let base = counts.first().copied().unwrap_or(0) as f64;
        if base == 0.0 {
            return vec![0.0; counts.len()];
        }
        counts.iter().map(|&c| c as f64 / base).collect()
    }

    /// 병렬 실행기 결과 병합
    pub fn merge(&mut self, other: FunnelExecutor) {
        for (user_key, other_state) in other.states {
            let state = self.states.entry(user_key).or_insert_with(|| {
                UserFunnelState {
                    achieved:   0,
                    last_times: vec![None; self.funnel.steps.len()],
                }
            });
            state.achieved |= other_state.achieved;
            for (i, t) in other_state.last_times.iter().enumerate() {
                if let Some(ot) = t {
                    state.last_times[i] = Some(match state.last_times[i] {
                        None => *ot,
                        Some(existing) => existing.min(*ot),
                    });
                }
            }
        }
    }
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
    fn test_funnel_basic() {
        let funnel = FunnelDef {
            steps: vec![
                FunnelStep { event_name: "view".into(),     filter: None },
                FunnelStep { event_name: "add_cart".into(), filter: None },
                FunnelStep { event_name: "purchase".into(), filter: None },
            ],
            window_sec: 3600,
        };
        let mut exec = FunnelExecutor::new(funnel);

        // user1: view → add_cart → purchase (완료)
        exec.process_events(&[
            event("user1", "view",     0),
            event("user1", "add_cart", 100),
            event("user1", "purchase", 200),
        ]);

        // user2: view → add_cart (step 2 미완료)
        exec.process_events(&[
            event("user2", "view",     0),
            event("user2", "add_cart", 100),
        ]);

        let counts = exec.step_counts();
        assert_eq!(counts[0], 2); // view: 2명
        assert_eq!(counts[1], 2); // add_cart: 2명
        assert_eq!(counts[2], 1); // purchase: 1명
    }

    #[test]
    fn test_funnel_window_exceeded() {
        let funnel = FunnelDef {
            steps: vec![
                FunnelStep { event_name: "step1".into(), filter: None },
                FunnelStep { event_name: "step2".into(), filter: None },
            ],
            window_sec: 60, // 1분 윈도우
        };
        let mut exec = FunnelExecutor::new(funnel);

        // step2가 윈도우 초과 (3600초 후)
        exec.process_events(&[
            event("user1", "step1", 0),
            event("user1", "step2", 3600),
        ]);

        let counts = exec.step_counts();
        assert_eq!(counts[0], 1);
        assert_eq!(counts[1], 0); // 윈도우 초과로 미달성
    }
}
