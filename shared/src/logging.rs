// WOW-DB 구조화 로깅 초기화 유틸리티
//
// 각 노드(QN/CN/SN) main.rs에서 호출하여:
//   - 콘솔(stdout): 개발용 컬러 포맷
//   - 파일(rolling): 운영용 구조화 키=값 포맷, 일별 로테이션
// 두 싱크에 동시 기록한다.
//
// # 사용 예시 (main.rs)
// ```rust
// let _log_guard = shared::logging::init_logging(
//     "query-node",
//     "./logs",
//     "query_node=info,shared=info",
// );
// ```
// `_log_guard`를 main 함수 생존 기간 내내 유지해야 파일 버퍼가 flush된다.

use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{
    fmt,
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};

// ─── 공개 타입 ────────────────────────────────────────────────────────────────

/// 로그 가드 — drop 시 파일 버퍼 flush + 워커 스레드 종료
/// main() 에서 `let _log_guard = init_logging(...)` 형태로 보유
pub struct LogGuard {
    _guard: WorkerGuard,
}

// ─── 초기화 ──────────────────────────────────────────────────────────────────

/// WOW-DB 노드 로깅 초기화
///
/// # 인자
/// - `node_name`     : 노드 이름 (파일명 접두어). 예: `"query-node"`, `"storage-node"`
/// - `log_dir`       : 로그 파일 저장 디렉토리. 없으면 자동 생성.
/// - `default_filter`: `RUST_LOG` 미설정 시 기본 필터.
///                      예: `"query_node=info,shared=info"`
///
/// # 반환
/// `LogGuard` — 프로세스 종료 시까지 보유해야 함
pub fn init_logging(node_name: &str, log_dir: &str, default_filter: &str) -> LogGuard {
    // ── 로그 디렉토리 생성 ────────────────────────────────────────────────────
    if let Err(e) = std::fs::create_dir_all(log_dir) {
        eprintln!("WARNING: 로그 디렉토리 생성 실패 ({log_dir}): {e}");
    }

    // ── Rolling File Appender (일별 로테이션) ─────────────────────────────────
    let file_appender = RollingFileAppender::new(
        Rotation::DAILY,
        log_dir,
        format!("{}.log", node_name),
    );
    let (non_blocking_writer, guard) = tracing_appender::non_blocking(file_appender);

    // ── EnvFilter: RUST_LOG 우선, 없으면 default_filter ─────────────────────
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_filter));

    // ── 파일 레이어: 구조화 키=값, ANSI 없음, 스레드 ID 포함 ─────────────────
    let file_layer = fmt::layer()
        .with_writer(non_blocking_writer)
        .with_ansi(false)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(false)
        .with_line_number(false);

    // ── 콘솔 레이어: 컴팩트 포맷, 개발용 ────────────────────────────────────
    let console_layer = fmt::layer()
        .compact()
        .with_target(true);

    // ── 구독자 등록 ──────────────────────────────────────────────────────────
    tracing_subscriber::registry()
        .with(filter)
        .with(file_layer)
        .with(console_layer)
        .init();

    LogGuard { _guard: guard }
}

/// 테스트 환경용 간이 초기화 (파일 없음, 콘솔만)
/// `try_init()`으로 이미 초기화된 경우 무시
#[allow(dead_code)]
pub fn init_test_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("debug")),
        )
        .with_test_writer()
        .try_init();
}
