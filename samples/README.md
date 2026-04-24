# WOW-DB Sample Datasets

Two sample datasets are included to help you explore WOW-DB's features.

## Available Datasets

| Dataset | Tables | Rows | Use Case |
|---------|--------|------|----------|
| [TPC-H](tpch/) | 8 (lineitem, orders, customer...) | ~60K (SF=0.01) | Analytical query benchmarking |
| [Web Analytics](web-analytics/) | 2 (events, sessions) | ~10K events + 500 sessions | Dual-layout and behavioral routing demo |

## Quick Start

### TPC-H (Benchmarking)

```bash
# Load SF=0.01 (~60K rows) and run 6 core queries
python scripts/tpch/bench.py --sf 0.01

# Run all 22 TPC-H queries with JSON report
python scripts/tpch/bench.py --sf 0.01 --queries all --output json --report out.json
```

### Web Analytics (Feature Demo)

```bash
# Load schema and 10K events
mysql -h 127.0.0.1 -P 9030 -u root < samples/web-analytics/schema.sql
python samples/web-analytics/generate.py

# Run session summary query
mysql -h 127.0.0.1 -P 9030 -u root web_analytics < samples/web-analytics/queries/01-session-summary.sql
```

## Query Complexity Index

| Level | Examples |
|-------|---------|
| **Basic** | SELECT + GROUP BY + ORDER BY (Q01, Q06, 01-session-summary) |
| **Intermediate** | Multi-table JOIN, CASE (Q03, Q05, Q10, Q12, Q14, 03-funnel) |
| **Advanced** | EXISTS, CTE, LEFT JOIN, subqueries (Q04, Q13, Q15, Q16, Q17, Q18, 05-routing) |

## Clean Up

```sql
-- Remove all sample tables
SOURCE samples/teardown.sql
```

See [docs/quickstart.md](../docs/quickstart.md) for the full getting-started guide.
