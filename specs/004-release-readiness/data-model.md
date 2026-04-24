# Data Model: WOW-DB Release Readiness

**Feature**: 004-release-readiness
**Date**: 2026-04-24

---

## Deliverable Artifact Schemas

### 1. Performance Report Structure

```
PerformanceReport
├── header
│   ├── generated_at: timestamp
│   ├── wow_db_version: string (git commit hash)
│   └── run_id: string (UUID)
├── hardware
│   ├── cpu_model: string
│   ├── cpu_cores: int
│   ├── ram_gb: int
│   ├── storage_type: enum [SSD, HDD, NVMe]
│   ├── os: string
│   └── docker_version: string
├── cluster
│   ├── qn_count: int
│   ├── cn_count: int
│   └── sn_count: int
├── benchmark_runs[]
│   ├── run_type: enum [tpch, persistence, distribution, concurrent, large_data]
│   ├── scale_factor: float (TPC-H only)
│   ├── data_state: enum [cold_cache, warm_cache]
│   └── query_results[]
│       ├── query_id: string (Q01–Q22, or test name)
│       ├── status: enum [PASS, PARTIAL, FAIL, SKIP]
│       ├── duration_ms: int
│       ├── row_count_actual: int
│       ├── row_count_expected: int
│       ├── error_message: string | null
│       └── notes: string | null
└── known_limitations[]
    ├── severity: enum [Critical, Major, Minor]
    ├── description: string
    └── workaround: string | null
```

### 2. TPC-H Expected Results (per query)

```
TpchExpected
├── query_id: string (Q01–Q22)
├── scale_factor: float
├── expected_row_count: int
├── sample_first_row: object  (for visual verification)
└── validation_columns: string[]  (columns to compare for correctness)
```

### 3. Sample Dataset Schemas

#### TPC-H Tables (8 tables — unchanged from spec)
Standard TPC-H 2.18 schema. Primary keys and foreign keys preserved as column conventions (WOW-DB does not enforce FK constraints).

#### Web Analytics Tables (2 tables — Dual-Layout pair)

**events** (Event Table — raw stream):
```
events
├── event_id: VARCHAR(36) NOT NULL      -- UUID
├── session_id: VARCHAR(36) NOT NULL    -- groups events into sessions
├── visitor_id: VARCHAR(36) NOT NULL    -- anonymous device identifier
├── user_id: VARCHAR(64)                -- logged-in user (nullable)
├── event_type: VARCHAR(32) NOT NULL    -- pageview | click | scroll | purchase
├── page_url: VARCHAR(512) NOT NULL
├── referrer_url: VARCHAR(512)
├── device_type: VARCHAR(16)            -- desktop | mobile | tablet
├── browser: VARCHAR(32)
├── os: VARCHAR(32)
├── country: VARCHAR(2)                 -- ISO 3166
└── ts: BIGINT NOT NULL                 -- Unix ms
```

**sessions** (Session MV — Behavioral Table):
```
sessions
├── session_id: VARCHAR(36) NOT NULL
├── visitor_id: VARCHAR(36) NOT NULL
├── user_id: VARCHAR(64)
├── session_start: BIGINT NOT NULL
├── session_end: BIGINT NOT NULL
├── duration_ms: BIGINT NOT NULL
├── event_count: INT NOT NULL
├── pageview_count: INT NOT NULL
├── entry_page: VARCHAR(512) NOT NULL
├── exit_page: VARCHAR(512) NOT NULL
├── device_type: VARCHAR(16)
├── browser: VARCHAR(32)
├── os: VARCHAR(32)
└── country: VARCHAR(2)
```

### 4. Benchmark Script CLI Contract

```
bench.py
  --host HOST         QN host (default: 127.0.0.1)
  --port PORT         MySQL port (default: 9030)
  --db DB             Database name (default: tpch)
  --sf SF             Scale factor: 0.01 | 0.1 | 1 (default: 0.01)
  --queries Q         Comma-separated query IDs, or 'all' (default: Q01,Q03,Q05,Q06,Q10,Q14)
  --output FORMAT     Output format: text | json | markdown (default: text)
  --no-load           Skip data loading, run queries only
  --report FILE       Write JSON report to FILE
  --runs N            Number of runs per query for p50/p95/p99 (default: 1)
```

### 5. Test Script Output Schema (all test scripts)

```
TestResult
├── test_name: string
├── status: enum [PASS, FAIL, SKIP]
├── duration_ms: int
├── actual: any          -- what was observed
├── expected: any        -- what was required
└── message: string      -- human-readable description
```
