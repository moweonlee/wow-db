# Implementation Plan: Web Monitoring Dashboard

**Branch**: `002-wow-db-srs-v02` | **Date**: 2026-04-17 | **Spec**: [spec.md](./spec.md)  
**Input**: Feature specification from `specs/003-web-monitoring-dashboard/spec.md`

## Summary

QN 웹 서버(port 8080)에 HTML 기반 모니터링 대시보드를 추가한다.
기존 REST API(`/api/v1/cluster`, `/metrics`, `/api/cubes`)를 재사용하고,
현재 없는 3가지를 신규 구현한다:
(1) 실시간 HTML 대시보드 페이지 (server-side 렌더링 + JS 자동갱신),
(2) SN별 LSM Compaction 상태 집계 API (`/api/v1/lsm`),
(3) CN/SN 노드의 `/logs` 엔드포인트 (최근 로그 100개).

---

## Technical Context

**Language/Version**: Rust 1.87 stable  
**Primary Dependencies**: axum (already in use), tokio, serde_json, tracing  
**Storage**: 읽기 전용 — Raft KV (cube 목록), SN gRPC scan (LSM 상태 폴링)  
**Testing**: `cargo test --workspace`, Docker 기반 통합 테스트  
**Target Platform**: Linux server (Docker/K8s), Windows dev  
**Project Type**: web-service (embedded monitoring UI)  
**Performance Goals**: 대시보드 페이지 응답 < 500ms, 자동갱신 주기 5초  
**Constraints**: 별도 npm/webpack 빌드 없이 단일 Rust 바이너리에 포함  
**Scale/Scope**: 최대 20 노드 클러스터 기준, 100개 테이블

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**WOW-DB Constitution v1.1.0 — 7 Gates:**

- [x] **I. Dual-Layout**: 모니터링 대시보드는 Funnel/Cohort/Path 분석 쿼리를 실행하지 않는다.
      기존 테이블 목록을 읽기만 하므로 적용 면제(EXEMPT). ✅
- [x] **II. Behavioral Routing**: 해당 없음 — 대시보드는 쿼리 파이프라인을 통과하지 않는다. ✅
- [x] **III. LSM-Tree**: 대시보드는 LSM 상태를 읽기 전용으로 조회한다.
      WAL/Compaction 경로에 쓰기가 발생하지 않는다. ✅
- [x] **IV. SIMD**: 신규 집계/스캔 연산 없음 — 적용 면제. ✅
- [x] **V. K8s-Native**: QN 웹 서버에 새 라우트 추가는 Stateless 원칙 위반 없음.
      모든 클러스터 상태는 Raft KV 또는 외부 노드 폴링으로 읽는다.
      `/health`, `/metrics` 엔드포인트는 이미 존재함. ✅
- [x] **VI. MySQL Compat**: 대시보드는 HTTP UI이므로 MySQL 프로토콜에 영향 없음. ✅
- [x] **VII. Storage-Compute**: CN의 대시보드는 SN 파일시스템에 직접 접근하지 않는다.
      QN이 SN HTTP `/api/v1/lsm-status` 엔드포인트를 폴링하는 구조로 격리 유지. ✅

**Result: 모든 Constitution 게이트 통과.**

---

## Project Structure

### Documentation (this feature)

```text
specs/003-web-monitoring-dashboard/
├── plan.md              ← 이 파일
├── research.md          ← Phase 0 결과
├── data-model.md        ← Phase 1 결과
├── quickstart.md        ← Phase 1 결과
├── contracts/           ← Phase 1 결과
│   ├── qn-dashboard-api.md
│   └── node-logs-api.md
└── tasks.md             ← /speckit.tasks 생성 예정
```

### Source Code (변경 대상 파일)

```text
query-node/
├── src/
│   ├── startup.rs             # [NEW] QN 피어 자기등록 + 15초 heartbeat (StarRocks FE 패턴)
│   └── web_ui/
│       ├── server.rs          # 신규 라우트 등록 (/, /api/v1/lsm) + WebUiState.node_id 필드 추가
│       ├── dashboard.rs       # [NEW] HTML 대시보드 핸들러
│       ├── monitoring.rs      # /api/v1/lsm 엔드포인트 추가, cluster_overview QN ID 수정
│       └── mod.rs             # dashboard 모듈 공개

compute-node/
└── src/
    └── main.rs                # /logs 엔드포인트 추가 (HTTP port 10040)

storage-node/
└── src/
    ├── main.rs                # /logs, /api/v1/lsm-status 엔드포인트 추가
    └── lsm/
        └── compaction.rs      # CompactionStatus 직렬화 구조체 추가
```

**Structure Decision**: 단일 프로젝트 구조 유지. 기존 axum 라우터에 새 라우트/핸들러 추가.
프론트엔드는 Rust 핸들러 내 인라인 HTML 문자열로 구현 (빌드 도구 불필요).

---

## Complexity Tracking

*Constitution 위반 없음 — 이 섹션 해당 없음.*
