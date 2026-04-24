# Tasks: WOW-DB Release Readiness

**Input**: Design documents from `specs/004-release-readiness/`
**Prerequisites**: plan.md ✓, spec.md ✓, research.md ✓, data-model.md ✓, contracts/ ✓, quickstart.md ✓

**Organization**: Tasks grouped by user story. Each story is independently implementable and testable.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: User story label (US1–US4)

---

## Phase 1: Setup

**Purpose**: Scaffold directories, extend bench.py CLI, and verify cluster is running.

- [X] T001 Create output directories: `docs/`, `samples/tpch/queries/`, `samples/tpch/expected/`, `samples/web-analytics/queries/`, `scripts/test/`
- [X] T002 Extend `scripts/tpch/bench.py` CLI: add `--sf`, `--queries all`, `--output json|markdown|text`, `--report FILE`, `--runs N`, `--no-load` arguments per `specs/004-release-readiness/contracts/benchmark-output.md`
- [X] T003 [P] Add `tabulate` and `mysql-connector-python` to bench.py dependencies; document in `samples/README.md`
- [ ] T004 Verify Docker cluster is healthy: `docker compose -f docker/docker-compose.yml --profile full ps` — all 9 nodes healthy

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: SQL gap fixes that multiple test phases depend on. Must complete before TPC-H full-run and test phases.

**⚠️ CRITICAL**: Phase 3 (TPC-H full audit), Phase 4 (persistence/distribution tests), and Phase 5 (large data) all depend on these SQL fixes being in place.

- [X] T005 Implement LEFT OUTER JOIN in `query-node/src/executor/select_exec.rs`: add `JoinOperator::LeftOuter` case in `apply_joins()`, return NULL-padded right side when no match — fixes Q13
- [X] T006 Implement WITH (CTE) clause in `query-node/src/executor/select_exec.rs`: pre-evaluate CTE body, register result in a per-query temp `HashMap<String, Vec<Row>>`, reuse as virtual table in FROM — fixes Q15, Q18, Q20
- [X] T007 Implement NOT IN subquery in `query-node/src/executor/select_exec.rs` `eval_expr`: add `E::InSubquery { negated: true, .. }` case, execute inner SELECT, collect values into `HashSet`, invert membership test — fixes Q16, Q21
- [X] T008 Implement COUNT(DISTINCT col) in `query-node/src/executor/select_exec.rs` `eval_aggregate`: detect `DISTINCT` flag in function args, deduplicate via `HashSet` before counting — fixes Q16
- [X] T009 Implement EXTRACT(YEAR/MONTH/DAY FROM col) in `query-node/src/executor/select_exec.rs` `eval_expr`: add `E::Extract { field, expr }` case, parse stored date string or BIGINT ms to extract field — fixes Q07
- [X] T010 Verify scalar subquery in WHERE (Q17 pattern): `WHERE col > (SELECT AVG(col) FROM t)` — add `E::Subquery` case in `eval_expr` that executes inner SELECT and returns scalar `Value` — fixes Q17, Q02 partial
- [X] T011 Build and smoke-test all SQL fixes: `cargo build -p query-node` then run `SELECT 1 FROM (WITH t AS (SELECT 1) SELECT * FROM t) x` and `SELECT COUNT(DISTINCT session_id) FROM events`

**Checkpoint**: SQL foundation ready — all 5 P1 gaps fixed. TPC-H full audit and test harness can now proceed.

---

## Phase 3: User Story 1 — First-Time Developer Quick Start (P1) 🎯 MVP

**Goal**: A developer with Docker can start WOW-DB and run their first query in under 10 minutes.

**Independent Test**: Follow `docs/quickstart.md` from a clean Docker environment; all copy-paste SQL blocks execute without error.

- [X] T012 [US1] Write `docs/quickstart.md` from the draft at `specs/004-release-readiness/quickstart.md` — finalize with exact port numbers, Docker version requirements, and copy-pasteable commands verified against the live cluster
- [X] T013 [P] [US1] Write troubleshooting section in `docs/quickstart.md`: cover port conflict, Docker memory limit, health check timeout, access denied, UPDATE/DELETE not supported
- [ ] T014 [P] [US1] Test Quick Start end-to-end: connect via `docker exec -it wowdb-mysql-client mysql -h qn-1 -P 9030 -u root`, run all SQL blocks from `docs/quickstart.md`, confirm each returns expected output
- [X] T015 [US1] Verify DBeaver/TablePlus connection section in `docs/quickstart.md`: document host=127.0.0.1, port=9030, user=root, no password
- [ ] T016 [P] [US1] Verify monitoring dashboard section in `docs/quickstart.md`: open http://localhost:8080 and confirm all 9 nodes show as ONLINE; update screenshot description

---

## Phase 4: User Story 2 — Advanced Guide (P2)

**Goal**: An engineer can deploy, configure, tune, and operate a multi-node WOW-DB cluster using `docs/advanced.md` alone.

**Independent Test**: Follow the rolling-restart runbook from `docs/advanced.md` on the live cluster; verify cluster stays healthy throughout.

- [X] T017 [US2] Create `docs/advanced.md` skeleton with sections: Cluster Architecture, Configuration Reference, DDL Reference, Operations Runbook, Performance Tuning, Behavioral Routing, Monitoring, Security
- [X] T018 [P] [US2] Write Cluster Architecture section in `docs/advanced.md`: QN/CN/SN roles, port map table (MySQL 9030, Web 8080, Raft 9010, gRPC 9011, CN gRPC 9040, SN gRPC 9060, SN HTTP 8040), inter-node communication diagram
- [X] T019 [P] [US2] Write Configuration Reference section in `docs/advanced.md`: document all env vars (NODE_ID, MYSQL_PORT, WEB_PORT, RAFT_PORT, GRPC_PORT, COMPUTE_NODES, STORAGE_NODES, QN_HTTP_PEERS, DATA_DIR, LOG_DIR) and TOML config keys with defaults
- [X] T020 [P] [US2] Write DDL Reference section in `docs/advanced.md`: `CREATE CUBE`, `ALTER CUBE`, `DROP CUBE`, `SHOW CUBES`, `DESCRIBE`, `SHOW CREATE CUBE`, `CREATE SESSION MATERIALIZED VIEW` — full syntax with examples
- [X] T021 [US2] Write Operations Runbook section in `docs/advanced.md`: rolling restart procedure (restart each node one at a time, verify healthy before next), node addition, node removal, log rotation
- [X] T022 [P] [US2] Write Performance Tuning section in `docs/advanced.md`: LSM MemTable flush threshold, L0 compaction trigger, gRPC 256 MiB message limit, partition strategy (HASH buckets), recommended hardware ratios
- [X] T023 [P] [US2] Write Behavioral Routing section in `docs/advanced.md`: when routing fires (session_id / SESSION_START / SESSION_END column references), how to create and populate session MVs, how to bypass routing (avoid session_* column names in non-BT queries)
- [X] T024 [P] [US2] Write Security section in `docs/advanced.md`: document no authentication currently implemented, recommend network isolation (firewall port 9030 to trusted hosts only), TLS not yet supported — label as known limitations
- [ ] T025 [US2] Test rolling restart runbook from `docs/advanced.md`: restart qn-1, verify qn-2 and qn-3 continue serving; restart sn-1, verify SELECT COUNT still returns correct total

---

## Phase 5: User Story 3 — Performance Evaluation (P2)

**Goal**: An evaluator can reproduce benchmark results and verify WOW-DB's performance claims.

**Independent Test**: Run `python scripts/tpch/bench.py --sf 0.01 --queries all --output json --report /tmp/report.json`; JSON file is produced and contains entries for all 22 queries.

### 5A — TPC-H Full 22-Query Audit

- [X] T026 [US3] Create `samples/tpch/queries/Q01.sql` through `Q22.sql`: write each TPC-H query with minor adaptations for WOW-DB SQL (replace unsupported syntax with documented workarounds where possible; mark unsupported queries with `-- STATUS: FAIL - requires LEFT JOIN` comments)
- [ ] T027 [US3] Create `samples/tpch/expected/` JSON files for all 22 queries at SF=0.01: `Q01-expected.json` through `Q22-expected.json` with `expected_row_count` and `sample_first_row` per `specs/004-release-readiness/data-model.md` schema
- [X] T028 [US3] Extend `scripts/tpch/bench.py` to load all 22 SQL files from `samples/tpch/queries/`, run each, capture status/duration/row_count, emit JSON per `specs/004-release-readiness/contracts/benchmark-output.md`
- [ ] T029 [US3] Run full TPC-H audit at SF=0.01: `python scripts/tpch/bench.py --sf 0.01 --queries all --output json --report scripts/tpch/audit-sf001.json`; verify Q01/Q03/Q04/Q06/Q10/Q14 all PASS; verify T005–T010 fixes change status of Q13/Q15/Q16/Q17/Q18/Q20/Q21

### 5B — Persistence Tests

- [X] T030 [P] [US3] Create `scripts/test/persistence_test.py`: insert 1000 rows into a test cube, capture COUNT(*), run `docker compose restart sn-1 sn-2 sn-3`, wait for healthy, assert COUNT(*) == 1000; emit PASS/FAIL per contracts schema
- [X] T031 [P] [US3] Add hard-kill test to `scripts/test/persistence_test.py`: insert 1000 rows, `docker compose kill sn-1 && docker compose up -d sn-1`, wait for healthy, assert COUNT(*) == 1000
- [ ] T032 [US3] Run persistence tests and record results: `python scripts/test/persistence_test.py`; both scenarios must return PASS

### 5C — Distribution Tests

- [X] T033 [P] [US3] Create `scripts/test/distribution_test.py`: insert 60,000 rows into a test cube, poll `http://localhost:18040/api/v1/lsm-status`, `http://localhost:28040/api/v1/lsm-status`, `http://localhost:38040/api/v1/lsm-status`; assert each SN has between 25%–45% of total rows; emit PASS/FAIL
- [ ] T034 [US3] Run distribution test and record results: `python scripts/test/distribution_test.py`; record per-SN row counts and skew percentage

### 5D — Concurrent Read/Write Tests

- [X] T035 [P] [US3] Create `scripts/test/concurrent_test.py`: spawn writer thread (INSERT 1 row/ms for 10s = ~10,000 rows), spawn 5 reader threads (SELECT COUNT(*) every 100ms), join all threads, assert final COUNT(*) == rows_inserted and reader_errors == 0; emit PASS/FAIL
- [ ] T036 [US3] Run concurrent test and record results: `python scripts/test/concurrent_test.py`; must show 0 reader errors and correct final count

### 5E — Large Data Tests

- [ ] T037 [US3] Run SF=0.1 test: `python scripts/tpch/bench.py --sf 0.1 --queries Q01,Q03,Q06,Q10,Q14 --output json --report scripts/tpch/audit-sf01.json`; verify all 5 queries PASS with correct row counts and no OOM
- [ ] T038 [P] [US3] Create `scripts/test/large_data_test.py`: load SF=0.1, measure load time (seconds), measure SELECT COUNT(*), assert count matches expected rows; record memory usage via `docker stats --no-stream`
- [ ] T039 [US3] Attempt SF=1 stress test (best-effort): `python scripts/tpch/bench.py --sf 1 --queries Q01,Q06 --no-load` (load separately); document behavior, OOM conditions if any, in `docs/performance-report.md` known limitations

### 5F — Performance Report

- [X] T040 [US3] Write `docs/performance-report.md` per `specs/004-release-readiness/contracts/perf-report-schema.md`: populate hardware spec, all 9 required sections (TPC-H results, ingestion benchmark, concurrent query p50/p95/p99, restart persistence, distribution, startup time, known limitations, reproduction instructions)
- [ ] T041 [US3] Run ingestion benchmark: time bulk INSERT of 60,000 rows; compute rows/sec; add to `docs/performance-report.md` ingestion section
- [ ] T042 [P] [US3] Run concurrent query benchmark: run Q01 with 1, 5, 10 concurrent connections using Python thread pool; measure p50/p95/p99 latency; add to `docs/performance-report.md`
- [ ] T043 [P] [US3] Measure cluster startup time: `time docker compose -f docker/docker-compose.yml --profile full up -d` from stopped state; add to `docs/performance-report.md`
- [X] T044 [US3] Complete known limitations section in `docs/performance-report.md`: classify each failing TPC-H query and SQL gap as Critical/Major/Minor with workaround

---

## Phase 6: User Story 4 — Feature Showcase & Sample Datasets (P3)

**Goal**: A developer can load sample data and run working examples demonstrating WOW-DB's unique features (Cube DDL, Session MV, behavioral routing).

**Independent Test**: Run `python samples/web-analytics/generate.py`; then run all 5 queries in `samples/web-analytics/queries/`; all return non-empty results.

### 6A — TPC-H Sample Package

- [X] T045 [P] [US4] Write `samples/tpch/README.md`: describe TPC-H dataset (8 tables, SF options), explain each query's business question, show loader command, note Q PASS/FAIL status
- [ ] T046 [P] [US4] Verify TPC-H loader script `scripts/tpch/bench.py --sf 0.01` completes in under 5 minutes and all 8 tables are populated with correct row counts

### 6B — Web Analytics Sample

- [X] T047 [US4] Create `samples/web-analytics/schema.sql`: events + sessions Cube DDL with `CREATE SESSION MATERIALIZED VIEW sessions ON events KEY user_id TIMEOUT 30`
- [X] T048 [P] [US4] Create `samples/web-analytics/generate.py`: generate 10,000 synthetic events across 500 sessions with realistic distributions (70% mobile/desktop split, 5 countries, 10 page URLs, pageview/click/purchase event types); insert into running cluster
- [X] T049 [P] [US4] Create `samples/web-analytics/queries/01-session-summary.sql`: session count, avg duration, total events — query on `sessions` table
- [X] T050 [P] [US4] Create `samples/web-analytics/queries/02-top-pages.sql`: top 10 pages by pageview count from `events` — no behavioral routing trigger
- [X] T051 [P] [US4] Create `samples/web-analytics/queries/03-funnel-analysis.sql`: entry page → product page → checkout conversion using session entry/exit pages from `sessions`
- [X] T052 [P] [US4] Create `samples/web-analytics/queries/04-user-retention.sql`: returning users (session_count > 1) grouped by country from `sessions`
- [X] T053 [P] [US4] Create `samples/web-analytics/queries/05-behavioral-routing-demo.sql`: a comment-annotated query showing a `session_id` reference on `events` being transparently routed to `sessions` BT; explain with `-- ROUTING: behavioral router redirects this to 'sessions'` comment
- [X] T054 [US4] Write `samples/web-analytics/README.md`: describe the dataset (event types, scale), show load command, explain each query and its expected output, explain behavioral routing demo

### 6C — Samples Master README

- [X] T055 [US4] Write `samples/README.md`: overview of both datasets, quick-start loader commands, query complexity index (Basic / Intermediate / Advanced), link to `docs/quickstart.md`

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Final validation, teardown scripts, `CLAUDE.md` update, and release checklist.

- [X] T056 Create `samples/teardown.sql`: `DROP CUBE IF EXISTS` for all sample tables (lineitem, orders, partsupp, part, customer, supplier, nation, region, events, sessions, pageviews) — satisfies FR-026
- [ ] T057 [P] Run `cargo clippy --workspace -- -D warnings` and resolve all new warnings introduced by T005–T011 SQL fixes
- [ ] T058 [P] Run `cargo test --workspace` and confirm all existing tests pass after SQL fixes
- [ ] T059 Run full end-to-end validation: start fresh cluster → load TPC-H SF=0.01 → run all tests (persistence, distribution, concurrent) → run web analytics queries → verify `docs/performance-report.md` reflects current results
- [X] T060 Update `CLAUDE.md` with any new commands or patterns from this release (benchmark script usage, test script usage)

---

## Dependencies

```
T001–T004 (Setup)
    └── T005–T011 (SQL fixes) ← blocking for T029, T032, T034, T036, T037, T039
        ├── T026–T044 (US3: Performance Evaluation)
        ├── T012–T016 (US1: Quick Start) ← can start in parallel with SQL fixes
        ├── T017–T025 (US2: Advanced Guide) ← can start in parallel with SQL fixes
        └── T045–T055 (US4: Feature Showcase)
            └── T056–T060 (Polish)
```

**US1 and US2 are documentation-only** — they can run in parallel with T005–T011 SQL fixes, but Quick Start testing (T014, T016) requires a working cluster.

---

## Parallel Execution Examples

### Track A (Documentation) — no code dependencies
```
T012 quickstart.md writing
T013 troubleshooting section
T017 advanced.md skeleton
T018 architecture section      [P with T019, T020, T022, T023, T024]
T019 config reference          [P]
T020 DDL reference             [P]
```

### Track B (SQL fixes) — sequential within track
```
T005 LEFT JOIN → T006 CTE → T007 NOT IN → T008 COUNT DISTINCT → T009 EXTRACT → T010 scalar subquery → T011 build+smoke
```

### Track C (Test scripts) — parallel after T004
```
T030 persistence_test.py       [P with T033, T035, T038]
T033 distribution_test.py      [P]
T035 concurrent_test.py        [P]
T038 large_data_test.py        [P]
```

### Track D (Samples) — parallel after T002
```
T026–T027 TPC-H SQL files      [P with T047–T054]
T047 web analytics schema      
T048 generate.py               [P with T049–T053]
T049–T053 query files          [P with each other]
```

---

## Implementation Strategy

**MVP (User Story 1 only)**: T001–T004, T012–T016 — delivers working Quick Start guide. Estimated: 1 day.

**Release Gate**: T001–T044 — all P1+P2 stories complete, performance report written, all SQL P1 gaps fixed. Estimated: 5–7 days.

**Full Release**: T001–T060 — all stories including web analytics showcase and polish. Estimated: 8–10 days.

---

## Task Summary

| Phase | Tasks | Parallel [P] | Story |
|-------|-------|-------------|-------|
| 1 Setup | T001–T004 | T003 | — |
| 2 SQL Fixes | T005–T011 | — | — |
| 3 Quick Start (US1) | T012–T016 | T013, T014, T015, T016 | US1 |
| 4 Advanced Guide (US2) | T017–T025 | T018–T024 | US2 |
| 5A TPC-H Audit (US3) | T026–T029 | — | US3 |
| 5B Persistence (US3) | T030–T032 | T030, T031 | US3 |
| 5C Distribution (US3) | T033–T034 | T033 | US3 |
| 5D Concurrent (US3) | T035–T036 | T035 | US3 |
| 5E Large Data (US3) | T037–T039 | T038 | US3 |
| 5F Perf Report (US3) | T040–T044 | T042, T043 | US3 |
| 6A TPC-H Sample (US4) | T045–T046 | T045, T046 | US4 |
| 6B Web Analytics (US4) | T047–T055 | T048–T053 | US4 |
| 7 Polish | T056–T060 | T057, T058 | — |
| **Total** | **60 tasks** | **28 parallel** | |
