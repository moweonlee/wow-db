# Research: WOW-DB Release Readiness

**Date**: 2026-04-24
**Feature**: 004-release-readiness

---

## Decision Log

### D1 — TPC-H Query Scope

**Decision**: Include all 22 TPC-H queries in the benchmark, classified as PASS / PARTIAL / FAIL. Only the 6 currently passing queries (Q01, Q03, Q04, Q06, Q10, Q14) count toward the release gate SC-002. The full 22-query audit is reported for transparency and drives the SQL completeness fix backlog.

**Rationale**: Shipping with only 6 queries passing would be misleading. Publishing the full matrix with honest PASS/FAIL is more credible and gives users a clear picture of current capability.

**Alternatives considered**:
- Report only passing queries → rejected (hides gaps, damages credibility)
- Block release on all 22 passing → rejected (CTE, NOT IN require significant executor work; not feasible for this release)

---

### D2 — SQL Gap Priority for This Release

**Decision**: Implement 5 SQL gaps before release: LEFT JOIN, CTE (WITH clause), NOT IN subquery, COUNT DISTINCT, EXTRACT(YEAR/MONTH FROM col). Defer SUBSTRING(), CAST(), and 7+-table join optimization.

**Rationale**:
- LEFT JOIN: Used in Q13, essential for analytics (outer join on nullable FK). Missing LEFT JOIN is a critical gap users will immediately notice.
- CTE: Blocks Q15, Q18, Q20. CTEs are standard SQL since SQL:1999; professional users expect them.
- NOT IN: Blocks Q16, Q21. Common pattern. NOT EXISTS was already fixed; NOT IN should follow.
- COUNT DISTINCT: Blocks Q16. Very common analytics pattern (unique users, unique sessions).
- EXTRACT(): Blocks Q7 date-based analysis. Year/month extraction is fundamental to time-series analytics.

**Deferred**:
- SUBSTRING(), CAST(): Only block Q22. Useful but not critical for analytics workloads.
- 7+-table join: Only blocks Q8. Uncommon in web analytics workloads.

**Alternatives considered**:
- Fix all 22 → rejected (scope too large, SUBSTRING/CAST are low analytics value)
- Fix none → rejected (LEFT JOIN and CTEs are blockers for real-world users)

---

### D3 — Restart Persistence Test Methodology

**Decision**: Test both graceful restart (`docker compose restart`) and hard kill (`docker compose kill`) scenarios. Success criterion: zero row loss in both cases.

**Rationale**: WAL guarantees durability only if CRC32 replay works correctly after both orderly shutdown and crash. Hard kill tests the crash recovery path specifically.

**Alternatives considered**:
- Test only graceful restart → rejected (crash recovery is the WAL's main value, must be validated)
- Test with `docker compose stop` (SIGTERM) → included as variant of graceful restart

---

### D4 — Data Distribution Verification Method

**Decision**: Verify distribution by polling each SN's `/api/v1/lsm-status` HTTP endpoint after INSERT and comparing `total_rows`. Acceptable skew: ≤ 40% deviation from N/SN_count.

**Rationale**: The HTTP endpoint is already implemented on all SNs and returns `total_rows`. This is non-intrusive and mirrors what the monitoring dashboard uses.

**Alternatives considered**:
- Use gRPC ScanTablet with EXPLAIN → requires deeper instrumentation, not ready
- Compare file sizes on SN volumes → fragile, format-dependent, violates Constitution VII

---

### D5 — Concurrent Read/Write Test Scope

**Decision**: Assert only (1) no crashes during concurrent access and (2) eventual consistency (final count = total inserted). Do NOT assert read monotonicity during write.

**Rationale**: WOW-DB does not implement MVCC. Reads during an active write may observe partial MemTable state. Asserting monotonic reads would require MVCC. The release note documents this as a known limitation.

**Alternatives considered**:
- Assert monotonic read counts → rejected (would require MVCC, out of scope)
- Skip concurrent test → rejected (crashes during concurrent access must be discovered before release)

---

### D6 — Performance Report Hardware Reference

**Decision**: Use the development machine (Windows 11, Docker Desktop, Intel/AMD CPU, 32 GB RAM) as the reference hardware. Report must include full hardware spec so readers can calibrate their expectations.

**Rationale**: No dedicated benchmark server is available. Docker Desktop on Windows is the target environment for most early adopters.

**Alternatives considered**:
- Use cloud VM (e.g., AWS c5.4xlarge) → preferred but not available for this release
- Report only relative timings (no hardware spec) → rejected (makes results uninterpretable)

---

### D7 — Large Data Test Scale Selection

**Decision**:
- SF=0.01 (~60k rows): CI gate, must pass in < 5 min
- SF=0.1 (~600k rows): Performance gate, must complete without OOM, all 6 passing queries correct
- SF=1 (~6M rows): Stress test, no time gate, documents behavior at scale

**Rationale**: SF=0.1 is 10× the CI scale and exercises memory pressure on the MemTable flush path. SF=1 is realistic for small-scale production and surfaces any O(n²) bugs.

**Alternatives considered**:
- SF=0.01 only → rejected (too small to surface memory or distribution issues)
- SF=10 → rejected (unrealistic on Docker Desktop, 16+ GB data)

---

## TPC-H Query Feature Matrix (Full)

| Query | Features Required | Status | Fix Priority |
|-------|-----------------|--------|-------------|
| Q01 | GROUP BY, SUM/AVG, date arithmetic, ORDER BY | PASS | — |
| Q02 | 5-table JOIN, correlated subquery in SELECT | PARTIAL | P3 |
| Q03 | 3-table JOIN, GROUP BY, date filter | PASS | — |
| Q04 | EXISTS correlated subquery | PASS | — |
| Q05 | 6-table JOIN, GROUP BY, SUM | PARTIAL | P3 |
| Q06 | BETWEEN date range, SUM | PASS | — |
| Q07 | CASE WHEN, EXTRACT(YEAR), nested subquery | PARTIAL | P2 |
| Q08 | 7-table JOIN, CASE WHEN ratio | FAIL | P3 |
| Q09 | LIKE '%..%', arithmetic expressions | PARTIAL | P2 |
| Q10 | 4-table JOIN, GROUP BY, ORDER BY LIMIT | PASS | — |
| Q11 | HAVING with scalar subquery | PARTIAL | P2 |
| Q12 | IN list, CASE WHEN COUNT | PARTIAL | P2 |
| Q13 | LEFT OUTER JOIN, COUNT, GROUP BY | FAIL | **P1** |
| Q14 | CASE WHEN SUM / SUM | PASS | — |
| Q15 | WITH (CTE), MAX, JOIN on CTE | FAIL | **P1** |
| Q16 | NOT IN subquery, COUNT DISTINCT | FAIL | **P1** |
| Q17 | Scalar subquery in WHERE, AVG | PARTIAL | P2 |
| Q18 | IN-subquery, GROUP BY, LIMIT | FAIL | **P1** |
| Q19 | Complex AND/OR predicates | PARTIAL | P2 |
| Q20 | CTE + IN-subquery chain | FAIL | **P1** |
| Q21 | EXISTS + NOT EXISTS, 4-table JOIN | FAIL | **P1** |
| Q22 | SUBSTRING, CAST, NOT IN | FAIL | P3 |

**P1 = implement before release | P2 = implement, lower priority | P3 = document as known limitation**

---

## SQL Feature Completeness — Beyond TPC-H

Additional SQL features commonly expected by users that need audit/documentation:

| Feature | Status | Notes |
|---------|--------|-------|
| LEFT/RIGHT OUTER JOIN | FAIL | P1 fix |
| FULL OUTER JOIN | FAIL | P3 — rare in analytics |
| WITH (CTE) | FAIL | P1 fix |
| Window functions (RANK, ROW_NUMBER, LAG) | FAIL | P2 — critical for analytics |
| NOT IN subquery | FAIL | P1 fix |
| COUNT DISTINCT | FAIL | P1 fix |
| EXTRACT(YEAR/MONTH FROM col) | PARTIAL | P2 fix |
| SUBSTRING(), TRIM(), UPPER(), LOWER() | PARTIAL | P3 |
| CAST(), CONVERT() | FAIL | P3 |
| UNION / UNION ALL | FAIL | P2 |
| INSERT … SELECT | FAIL | P2 |
| UPDATE / DELETE | FAIL | by design (append-only) — document |
| LIMIT with OFFSET | PARTIAL | needs verification |
| Nested subqueries (3+ levels) | PARTIAL | needs verification |
| JSON functions | FAIL | out of scope for release |

**Note**: UPDATE/DELETE are intentionally not supported (append-only LSM design). This must be prominently documented in the Quick Start guide.
