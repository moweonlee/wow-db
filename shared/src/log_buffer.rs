// 노드 인메모리 로그 버퍼 — 대시보드 /logs 엔드포인트용
// 최근 N개(기본 100) 항목을 원형 버퍼로 보관, 스레드 안전

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

// ─── 타입 ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "DEBUG" => LogLevel::Debug,
            "WARN"  => LogLevel::Warn,
            "ERROR" => LogLevel::Error,
            _       => LogLevel::Info,
        }
    }

    pub fn severity(&self) -> u8 {
        match self {
            LogLevel::Debug => 0,
            LogLevel::Info  => 1,
            LogLevel::Warn  => 2,
            LogLevel::Error => 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp_ms: u64,
    pub level:        LogLevel,
    pub target:       String,
    pub message:      String,
    #[serde(default)]
    pub fields:       std::collections::HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogsResponse {
    pub node_id:       String,
    pub role:          String,
    pub entries:       Vec<LogEntry>,
    pub total_buffered: usize,
}

// ─── 버퍼 ────────────────────────────────────────────────────────────────────

pub struct LogBuffer {
    entries:  VecDeque<LogEntry>,
    capacity: usize,
}

impl LogBuffer {
    pub fn new(capacity: usize) -> Self {
        Self { entries: VecDeque::with_capacity(capacity), capacity }
    }

    pub fn push(&mut self, entry: LogEntry) {
        if self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    pub fn recent(&self, n: usize) -> Vec<LogEntry> {
        self.entries.iter().rev().take(n).cloned().collect()
    }

    pub fn recent_by_level(&self, min_level: &LogLevel, n: usize) -> Vec<LogEntry> {
        let sev = min_level.severity();
        self.entries.iter().rev()
            .filter(|e| e.level.severity() >= sev)
            .take(n)
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize { self.entries.len() }
}

// ─── 헬퍼 ────────────────────────────────────────────────────────────────────

/// 현재 Unix 밀리초 타임스탬프
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// LogEntry 간편 생성
pub fn make_entry(
    level:   LogLevel,
    target:  &str,
    message: &str,
    fields:  Vec<(&str, &str)>,
) -> LogEntry {
    LogEntry {
        timestamp_ms: now_ms(),
        level,
        target: target.to_string(),
        message: message.to_string(),
        fields: fields.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
    }
}
