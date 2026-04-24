# Contract: Performance Report Schema

**Version**: 1.0.0
**Output file**: `docs/performance-report.md`
**Generated from**: benchmark JSON + manual hardware spec + test script outputs

---

## Required Sections (in order)

### 1. Header
- Title, date, WOW-DB version (commit hash), cluster configuration
- Hardware specification table

### 2. Test Environment
```markdown
| Component | Specification |
|-----------|--------------|
| CPU       | Intel Core i7-12700H (14 cores) |
| RAM       | 32 GB DDR5 |
| Storage   | 512 GB NVMe SSD |
| OS        | Windows 11 Pro 23H2 |
| Docker    | Desktop 28.0.4 |
| Cluster   | QN×3, CN×3, SN×3 |
```

### 3. TPC-H Benchmark Results

Must include one table per scale factor tested (SF=0.01 minimum).

```markdown
### SF=0.01 (~60,000 lineitem rows)

| Query | Status | Duration (ms) | Rows Actual | Rows Expected | Notes |
|-------|--------|--------------|-------------|---------------|-------|
| Q01   | ✅ PASS | 1,243        | 4           | 4             | |
| Q13   | ❌ FAIL | —            | 0           | 90            | LEFT JOIN not implemented |
```

### 4. Data Ingestion Benchmark

```markdown
| Rows Inserted | SN Count | Duration (s) | Rows/sec |
|--------------|----------|-------------|---------|
| 60,000       | 3        | 5.2         | 11,538  |
| 600,000      | 3        | 52.1        | 11,516  |
```

### 5. Concurrent Query Benchmark

```markdown
| Concurrency | p50 (ms) | p95 (ms) | p99 (ms) | Errors |
|-------------|---------|---------|---------|--------|
| 1           | 210     | 380     | 520     | 0      |
| 5           | 240     | 450     | 620     | 0      |
| 10          | 310     | 580     | 820     | 0      |
```

### 6. Restart Persistence Test

```markdown
| Test Scenario | Rows Written | Rows After Restart | Result |
|--------------|-------------|-------------------|--------|
| Graceful restart (SIGTERM) | 1,000 | 1,000 | ✅ PASS |
| Hard kill (SIGKILL) | 1,000 | 1,000 | ✅ PASS |
```

### 7. Data Distribution Test

```markdown
| SN | Rows | % of Total | Skew from Expected |
|----|------|-----------|-------------------|
| sn-1 | 20,234 | 33.7% | +0.7% |
| sn-2 | 19,891 | 33.1% | +0.1% |
| sn-3 | 19,875 | 33.1% | +0.1% |
```

### 8. Cluster Startup Time

```markdown
| Scenario | Time to All-Healthy (s) |
|----------|------------------------|
| Cold start (images present) | 42s |
| Warm restart | 18s |
```

### 9. Known Limitations

```markdown
| # | Severity | Description | Workaround |
|---|----------|-------------|-----------|
| 1 | Major | LEFT JOIN not implemented (Q13 fails) | Use INNER JOIN where possible |
| 2 | Major | CTE (WITH clause) not implemented | Rewrite as subquery |
| 3 | Minor | UPDATE/DELETE not supported (by design) | Append new rows; use ts filter |
```

### 10. Reproduction Instructions

```bash
# Reproduce benchmark results
git clone https://github.com/.../wow-db && cd wow-db
docker compose -f docker/docker-compose.yml --profile full up -d
python scripts/tpch/bench.py --sf 0.01 --queries all --output json --report my-report.json
```

## Invariants

- All sections 1–9 MUST be present in the final report
- Every FAIL status MUST have a note explaining the root cause
- Reproduction instructions MUST be copy-pasteable and produce the same results on equivalent hardware
