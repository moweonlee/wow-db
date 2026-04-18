// T052: COHORT_ANALYSIS 실행기 — Entry 이벤트 기준 코호트 그룹화, 기간별 retention 계산

use ahash::AHashMap;

// ─── Cohort 정의 ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CohortDef {
    /// 코호트 진입 이벤트 (예: "signup", "first_purchase")
    pub entry_event:   String,
    /// 재방문 판별 이벤트 (예: "login", "purchase")
    pub return_event:  String,
    /// 분석 기간 단위: "day", "week", "month"
    pub period_unit:   PeriodUnit,
    /// 분석할 최대 기간 수
    pub max_periods:   usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeriodUnit {
    Day,
    Week,
    Month,
}

impl PeriodUnit {
    pub fn period_of(&self, ts: i64) -> i64 {
        match self {
            PeriodUnit::Day   => ts / 86400,
            PeriodUnit::Week  => ts / (86400 * 7),
            PeriodUnit::Month => ts / (86400 * 30), // 근사치
        }
    }
}

// ─── Cohort 실행기 ────────────────────────────────────────────────────────────

use super::funnel::EventRecord;

pub struct CohortExecutor {
    cohort:        CohortDef,
    /// user_key → 진입 period
    entry_periods: AHashMap<Vec<u8>, i64>,
    /// (cohort_period, delta_period) → 재방문 사용자 집합
    retention:     AHashMap<(i64, usize), std::collections::HashSet<Vec<u8>>>,
}

impl CohortExecutor {
    pub fn new(cohort: CohortDef) -> Self {
        Self {
            cohort,
            entry_periods: AHashMap::new(),
            retention:     AHashMap::new(),
        }
    }

    pub fn process_events(&mut self, events: &[EventRecord]) {
        for event in events {
            if event.event_name == self.cohort.entry_event {
                // 진입 이벤트: 첫 번째 발생 시각을 cohort period로 등록
                let period = self.cohort.period_unit.period_of(event.event_time);
                self.entry_periods.entry(event.user_key.clone()).or_insert(period);
            }

            if event.event_name == self.cohort.return_event {
                if let Some(&entry_period) = self.entry_periods.get(&event.user_key) {
                    let current_period = self.cohort.period_unit.period_of(event.event_time);
                    let delta = (current_period - entry_period) as usize;
                    if delta <= self.cohort.max_periods {
                        self.retention
                            .entry((entry_period, delta))
                            .or_default()
                            .insert(event.user_key.clone());
                    }
                }
            }
        }
    }

    /// Retention 행렬 반환
    /// 반환: Vec<CohortRow> - 각 코호트의 기간별 재방문 사용자 수
    pub fn build_result(&self) -> Vec<CohortRow> {
        // 코호트 기간 목록 (오름차순)
        let mut cohort_periods: Vec<i64> = self.entry_periods.values().copied().collect();
        cohort_periods.sort();
        cohort_periods.dedup();

        // 코호트 크기 (진입 사용자 수)
        let mut cohort_sizes: AHashMap<i64, usize> = AHashMap::new();
        for &period in self.entry_periods.values() {
            *cohort_sizes.entry(period).or_insert(0) += 1;
        }

        cohort_periods.iter().map(|&cp| {
            let cohort_size = cohort_sizes.get(&cp).copied().unwrap_or(0);
            let mut periods = vec![0u64; self.cohort.max_periods + 1];

            for delta in 0..=self.cohort.max_periods {
                if let Some(users) = self.retention.get(&(cp, delta)) {
                    periods[delta] = users.len() as u64;
                }
            }

            CohortRow {
                cohort_period: cp,
                cohort_size:   cohort_size as u64,
                periods,
            }
        }).collect()
    }
}

#[derive(Debug, Clone)]
pub struct CohortRow {
    pub cohort_period: i64,
    pub cohort_size:   u64,
    /// periods[0] = 진입 기간, periods[1] = 1 기간 후, ...
    pub periods:       Vec<u64>,
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
    fn test_cohort_basic() {
        let cohort = CohortDef {
            entry_event:  "signup".into(),
            return_event: "login".into(),
            period_unit:  PeriodUnit::Day,
            max_periods:  7,
        };
        let mut exec = CohortExecutor::new(cohort);

        let day0 = 0i64;
        let day1 = 86400i64;
        let day7 = 86400 * 7;

        // user1: day0 가입, day1 로그인, day7 로그인
        exec.process_events(&[
            event("user1", "signup", day0),
            event("user1", "login",  day0),    // delta=0
            event("user1", "login",  day1),    // delta=1
            event("user1", "login",  day7),    // delta=7
        ]);

        // user2: day0 가입, day1 로그인
        exec.process_events(&[
            event("user2", "signup", day0),
            event("user2", "login",  day1),    // delta=1
        ]);

        let rows = exec.build_result();
        assert_eq!(rows.len(), 1); // 코호트 1개 (day0)
        let row = &rows[0];
        assert_eq!(row.cohort_size, 2);        // 가입 2명
        assert_eq!(row.periods[0], 1);         // day0 로그인: user1만
        assert_eq!(row.periods[1], 2);         // day1 로그인: user1 + user2
        assert_eq!(row.periods[7], 1);         // day7 로그인: user1만
    }
}
