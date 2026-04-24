# Implementation Plan: WOW-DB Release Readiness

**Branch**: `004-release-readiness` | **Date**: 2026-04-24 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `specs/004-release-readiness/spec.md`

---

## Summary

Deliver the four artifacts required before WOW-DB public release: Easy Startup Guide, Advanced Guide, Sample Datasets & Queries, and a Performance Test Report. The plan is expanded per user direction to include full TPC-H 22-query coverage (not just 6), data distribution verification across nodes, restart persistence tests (WAL/MANIFEST durability), concurrent read-while-write tests, and large-volume data tests (SF=0.1, SF=1).

The work is split into three tracks running in parallel:
- **Track A** — Documentation (Quick Start + Advanced Guide)
- **Track B** — Sample datasets, all 22 TPC-H queries, SQL completeness audit
- **Track C** — Test harness: distribution, persistence, concurrency, large-data, performance report

---

## Technical Context

**Language/Version**: Rust 1.87 stable (DB engine), Python 3.10+ (benchmark/loader scripts), Bash/PowerShell (test harness)
**Primary Dependencies**: tokio, axum, tonic, opensrv-mysql, sqlparser (engine); mysql-connector-python, tabulate (scripts)
**Storage**: Native LSM-Tree (local NVMe/SSD); MinIO S3-compatible (optional cold tier)
**Testing**: `cargo test --workspace` (unit); `integration-tests/` crate (Rust integration); `scripts/tpch/bench.py` (TPC-H); custom Python test scripts (distribution, persistence, concurrency)
**Target Platform**: Docker Desktop (Windows/macOS dev), Docker Engine on Linux, Kubernetes 1.28+ (production)
**Project Type**: Distributed OLAP database — documentation + test deliverables for this feature
**Performance Goals**: All 6 current TPC-H queries < 60s total at SF=0.01; > 10,000 rows/sec bulk INSERT on 3-SN; cluster up in < 60s
**Constraints**: Docker Desktop 8 GB RAM minimum for full cluster (QN×3 + CN×3 + SN×3); SF=1 test requires 16 GB RAM
**Scale/Scope**: SF=0.01 (~60k lineitems) for CI; SF=0.1 (~600k) for perf gate; SF=1 (~6M) for stress test

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- [X] **I. Dual-Layout**: Sample datasets include `events` + `sessions` pair. Quick Start guide teaches both DDLs.
- [X] **II. Behavioral Routing**: Sample queries explicitly demonstrate transparent routing. Guide documents when routing fires and how to bypass.
- [X] **III. LSM-Tree**: Restart persistence tests verify WAL durability and MANIFEST-based recovery. Distribution tests verify partition-scoped data placement.
- [X] **IV. SIMD**: No new aggregation code in this feature. Benchmark measures throughput but does not modify SIMD paths.
- [X] **V. K8s-Native**: `/health`, `/healthz` endpoints verified in Quick Start. Guide documents rolling restart. Distribution tests use multi-node cluster.
- [X] **VI. MySQL Compat**: All sample queries are validated via `mysql` CLI and `mysql-connector-python`. Quick Start includes DBeaver connection example.
- [X] **VII. Storage-Compute**: Distribution tests verify data spread across SNs via gRPC scan, not direct SN file access.

**Result**: All 7 gates PASS. No complexity tracking required.

---

## Project Structure

### Documentation (this feature)

```text
specs/004-release-readiness/
├── plan.md              ← this file
├── research.md          ← Phase 0 (SQL completeness audit, TPC-H gap analysis)
├── data-model.md        ← Phase 1 (deliverable schemas)
├── quickstart.md        ← Phase 1 (Quick Start Guide draft)
├── contracts/
│   ├── benchmark-output.md    ← benchmark script output contract
│   └── perf-report-schema.md  ← performance report structure contract
└── tasks.md             ← Phase 2 (/speckit.tasks — not created here)
```

### Source Code Layout (repository root)

```text
docs/
├── quickstart.md           # Easy Startup Guide (FR-001–008)
├── advanced.md             # Advanced Guide (FR-009–018)
└── performance-report.md   # Performance Test Report (FR-028–036)

samples/
├── README.md               # Dataset overview + loader instructions
├── tpch/
│   ├── bench.py            # Extended: all 22 queries, SF param, structured output
│   ├── queries/            # Q01.sql – Q22.sql (individual query files)
│   └── expected/           # Q01-expected.json – Q22-expected.json (row counts + sample)
└── web-analytics/
    ├── schema.sql           # events + sessions DDL
    ├── generate.py          # Synthetic data generator (10k events, 500 sessions)
    └── queries/
        ├── 01-session-summary.sql
        ├── 02-top-pages.sql
        ├── 03-funnel-analysis.sql
        ├── 04-user-retention.sql
        └── 05-behavioral-routing-demo.sql

scripts/
├── tpch/
│   ├── bench.py            # (existing, to be extended)
│   └── run-benchmark.sh    # Shell wrapper: load + run + emit report
└── test/
    ├── persistence_test.py  # Write → restart → verify (WAL/MANIFEST durability)
    ├── distribution_test.py # Insert N rows → verify spread across SNs
    ├── concurrent_test.py   # Concurrent writer + reader threads, verify no data loss
    └── large_data_test.py   # SF=0.1 and SF=1 load + query correctness
```

---

## Phase 0: Research

> See [research.md](./research.md) for full findings.

### Key Research Questions Resolved

#### R1 — TPC-H 22-Query SQL Feature Coverage

Full audit of all 22 TPC-H queries against WOW-DB's current SQL executor:

| Query | Key SQL Features | WOW-DB Status | Blocker |
|-------|-----------------|--------------|---------|
| Q01 | GROUP BY, SUM, AVG, ORDER BY, date arithmetic | **PASS** | — |
| Q02 | Multi-table JOIN (5 tables), correlated subquery, MIN | **PARTIAL** | correlated subquery in SELECT list |
| Q03 | JOIN (3 tables), GROUP BY, ORDER BY, date filter | **PASS** | — |
| Q04 | EXISTS correlated subquery | **PASS** (fixed) | — |
| Q05 | JOIN (6 tables), GROUP BY, SUM | **PARTIAL** | 6-table join memory pressure |
| Q06 | date range filter, SUM, BETWEEN | **PASS** (cmp_json fixed) | — |
| Q07 | CASE WHEN, nested subquery, year extraction | **PARTIAL** | EXTRACT(YEAR FROM date) |
| Q08 | CASE WHEN SUM, 7-table JOIN | **FAIL** | 7-table join not tested |
| Q09 | LIKE pattern, arithmetic in GROUP BY | **PARTIAL** | LIKE with % not verified |
| Q10 | JOIN (4 tables), GROUP BY, ORDER BY | **PASS** | — |
| Q11 | GROUP BY HAVING subquery, SUM | **PARTIAL** | HAVING with subquery |
| Q12 | CASE WHEN COUNT, IN list | **PARTIAL** | IN list in SELECT position |
| Q13 | LEFT OUTER JOIN, GROUP BY, COUNT | **FAIL** | LEFT JOIN not implemented |
| Q14 | CASE WHEN SUM / SUM | **PASS** | — |
| Q15 | WITH (CTE), MAX, JOIN on view | **FAIL** | CTE not implemented |
| Q16 | NOT IN subquery, COUNT DISTINCT | **FAIL** | NOT IN, COUNT DISTINCT |
| Q17 | AVG subquery in WHERE, BETWEEN | **PARTIAL** | scalar subquery in WHERE |
| Q18 | IN with GROUP BY subquery, LIMIT | **FAIL** | CTE / IN-subquery |
| Q19 | Complex OR conditions, AND-OR mix | **PARTIAL** | complex AND-OR predicate |
| Q20 | IN-subquery, date filter | **FAIL** | CTE + IN-subquery chain |
| Q21 | EXISTS + NOT EXISTS, multi-JOIN | **FAIL** | NOT EXISTS |
| Q22 | SUBSTRING, CAST, NOT IN subquery | **FAIL** | SUBSTRING(), CAST() |

**Current passing**: Q01, Q03, Q04, Q06, Q10, Q14 (6/22 = 27%)
**Partial (runs but wrong results)**: Q02, Q05, Q07, Q09, Q11, Q12, Q17, Q19 (8/22 = 36%)
**Fail (parse error or 0 rows)**: Q08, Q13, Q15, Q16, Q18, Q20, Q21, Q22 (8/22 = 36%)

**Priority fixes for release**:
1. LEFT JOIN (blocks Q13, important for analytics) — HIGH
2. CTE / WITH clause (blocks Q15, Q18, Q20) — HIGH
3. NOT IN subquery (blocks Q16, Q21, Q22) — HIGH
4. COUNT DISTINCT (blocks Q16) — MEDIUM
5. EXTRACT(YEAR/MONTH FROM col) (blocks Q7) — MEDIUM
6. Scalar subquery in WHERE (blocks Q2, Q17) — MEDIUM
7. SUBSTRING(), CAST() (blocks Q22) — LOW for release

#### R2 — Data Distribution Verification Strategy

WOW-DB distributes data by `HASH() BUCKETS 4` across available SNs. With 3 SNs and 4 buckets, distribution is approximately 33%/33%/33% per SN. Verification approach:
- After INSERT of N rows, scan each SN's `/api/v1/lsm-status` HTTP endpoint
- Compare `total_rows` per SN — acceptable skew ≤ 40% of N/3
- For large datasets (SF=0.1+), also verify via `SELECT COUNT(*) FROM t` matches total inserted

#### R3 — Restart Persistence Test Pattern

WAL guarantees: write → WAL flush → MemTable → crash → replay WAL → MemTable restored. MANIFEST guarantees SSTable list survives restart. Test pattern:
1. INSERT batch of rows → verify COUNT(*) = N
2. `docker compose restart sn-1 sn-2 sn-3` (graceful restart)
3. `SELECT COUNT(*) FROM t` — must return N (WAL replay)
4. `docker compose kill sn-1 && docker compose up -d sn-1` (hard kill)
5. `SELECT COUNT(*) FROM t` — must return N (WAL CRC32 replay)

#### R4 — Concurrent Read/Write Pattern

WOW-DB does not yet implement MVCC. During an active INSERT stream, SELECT scans the MemTable snapshot at scan-start time. Known behavior: rows inserted after scan start may or may not appear (MemTable snapshot non-determinism). Test pattern:
- Writer thread: INSERT 1 row/ms for 10s (10,000 rows)
- Reader thread: SELECT COUNT(*) every 100ms
- Assertion: final COUNT(*) after writer completes = 10,000 (eventual consistency)
- Assertion: no reader returns an error (no crash/panic during concurrent access)
- Known limitation to document: intermediate COUNT(*) values during write are not guaranteed monotonically increasing

#### R5 — Large Data Volume Requirements

| Scale | lineitem rows | Approx size | Min RAM (Docker) | Load time (3-SN) |
|-------|-------------|-------------|-----------------|-----------------|
| SF=0.01 | ~60k | ~10 MB | 8 GB | < 30s |
| SF=0.1 | ~600k | ~100 MB | 8 GB | < 5 min |
| SF=1 | ~6M | ~1 GB | 16 GB | < 45 min (estimate) |

SF=1 is a stress test, not a CI gate. Performance report must label SF clearly.

---

## Phase 1: Design & Contracts

### Data Model (`data-model.md`)

> See [data-model.md](./data-model.md) for full schemas.

The "data" in this feature is the structure of deliverable artifacts:

**Performance Report**: Hierarchical — Hardware Spec → Test Run → Query Result rows
**Benchmark Output**: JSON per query run, aggregated into Markdown table
**Sample Dataset**: TPC-H 8 tables (region, nation, supplier, customer, part, partsupp, orders, lineitem) + web analytics 2 tables (events, sessions)

### Interface Contracts (`contracts/`)

> See [contracts/benchmark-output.md](./contracts/benchmark-output.md) and [contracts/perf-report-schema.md](./contracts/perf-report-schema.md).

**Benchmark script output** (`bench.py --output json`):
```json
{
  "run_id": "2026-04-24T10:00:00Z",
  "hardware": { "cpu": "...", "ram_gb": 32, "storage": "SSD" },
  "cluster": { "qn": 3, "cn": 3, "sn": 3 },
  "scale_factor": 0.01,
  "queries": [
    {
      "id": "Q01", "sql_file": "queries/Q01.sql",
      "status": "PASS",
      "duration_ms": 1243,
      "row_count": 4,
      "expected_row_count": 4,
      "error": null
    }
  ],
  "summary": { "total_ms": 18432, "pass": 6, "fail": 0, "partial": 0 }
}
```

**Test harness output** (all test scripts emit same format to stdout):
```
[PASS] persistence_test: WAL replay after graceful restart (1000 rows)
[PASS] persistence_test: WAL replay after hard kill (1000 rows)
[FAIL] distribution_test: sn-1=60%, sn-2=20%, sn-3=20% — skew 40% exceeds threshold
[PASS] concurrent_test: 10000 rows inserted, final count=10000, 0 reader errors
```

---

## Phases Summary

| Phase | Track | Key Deliverables |
|-------|-------|-----------------|
| **Setup** | All | Docker cluster running, bench.py extended to emit JSON, test scripts scaffolded |
| **A1** | Docs | `docs/quickstart.md` — single-node Quick Start |
| **A2** | Docs | `docs/advanced.md` — cluster guide, DDL reference, ops runbook |
| **B1** | Samples | TPC-H loader extended to SF param; Q01–Q22 SQL files with expected row counts |
| **B2** | Samples | Web analytics generator; 5 analytics query examples |
| **B3** | SQL audit | Run all 22 TPC-H queries, record PASS/PARTIAL/FAIL, file bugs for HIGH priority gaps |
| **B4** | SQL fixes | Implement: LEFT JOIN, CTE/WITH, NOT IN, COUNT DISTINCT, EXTRACT(), scalar subquery WHERE |
| **C1** | Tests | `persistence_test.py` — graceful + hard kill restart tests |
| **C2** | Tests | `distribution_test.py` — data spread across SNs |
| **C3** | Tests | `concurrent_test.py` — concurrent read+write, no crash assertion |
| **C4** | Tests | `large_data_test.py` — SF=0.1 load + query; SF=1 stress load |
| **C5** | Report | `docs/performance-report.md` — generated from benchmark JSON + manual hardware spec |
