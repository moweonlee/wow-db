# Contract: Benchmark Script Output

**Version**: 1.0.0
**Consumer**: Performance report generator, CI pipeline
**Producer**: `scripts/tpch/bench.py`

---

## JSON Output Schema

Emitted when `--output json` or `--report FILE` is passed.

```json
{
  "run_id": "<ISO8601 timestamp>",
  "wow_db_version": "<git commit hash>",
  "hardware": {
    "cpu_model": "string",
    "cpu_cores": 8,
    "ram_gb": 32,
    "storage_type": "SSD",
    "os": "string",
    "docker_version": "string"
  },
  "cluster": { "qn": 3, "cn": 3, "sn": 3 },
  "scale_factor": 0.01,
  "data_state": "cold_cache",
  "load_duration_ms": 12345,
  "queries": [
    {
      "id": "Q01",
      "sql_file": "queries/Q01.sql",
      "status": "PASS",
      "duration_ms": 1243,
      "row_count_actual": 4,
      "row_count_expected": 4,
      "error": null,
      "notes": null
    }
  ],
  "summary": {
    "total_query_ms": 18432,
    "pass": 6,
    "partial": 0,
    "fail": 0,
    "skip": 16
  }
}
```

## Markdown Output (--output markdown)

```markdown
## TPC-H Benchmark Results — SF=0.01 — 2026-04-24

| Query | Status | Duration (ms) | Rows Actual | Rows Expected |
|-------|--------|--------------|-------------|---------------|
| Q01   | ✅ PASS | 1243         | 4           | 4             |
| Q13   | ❌ FAIL | —            | 0           | 90            |
```

## Status Values

| Status | Meaning |
|--------|---------|
| PASS | Executed without error, row count matches expected (±1%) |
| PARTIAL | Executed without error, row count does NOT match expected |
| FAIL | Parse error, runtime error, or timeout (> 30s per query) |
| SKIP | Not attempted in this run (excluded from --queries list) |

## Invariants

- `run_id` is unique per invocation
- `queries` array always contains an entry for every query in the `--queries` list
- `summary.pass + summary.partial + summary.fail + summary.skip = 22` (when `--queries all`)
- `duration_ms` is null when status is FAIL (no valid timing)
