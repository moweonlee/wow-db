# WOW-DB TPC-H Sample Dataset

TPC-H is an industry-standard OLAP benchmark with 8 tables and 22 analytical queries covering a simulated supply chain business.

## Dataset Scale Factors

| SF | lineitem rows | Load time | Use case |
|----|--------------|-----------|---------|
| 0.01 | ~60,000 | ~5s | Correctness testing |
| 0.1 | ~600,000 | ~52s | Performance testing |
| 1 | ~6,000,000 | ~10min | Stress testing |

## TPC-H Tables

| Table | SF=0.01 rows | Description |
|-------|-------------|-------------|
| lineitem | ~60,000 | Order line items (fact table) |
| orders | ~15,000 | Customer orders |
| customer | 1,500 | Customers |
| part | 2,000 | Parts catalog |
| partsupp | 8,000 | Part-supplier relationships |
| supplier | 100 | Suppliers |
| nation | 25 | Nations |
| region | 5 | Regions |

## Load the Dataset

```bash
# SF=0.01 (default — correctness check)
python scripts/tpch/bench.py --sf 0.01

# SF=0.1 (performance test, selected queries)
python scripts/tpch/bench.py --sf 0.1 --queries Q01,Q03,Q06,Q10,Q14

# All 22 queries with JSON report
python scripts/tpch/bench.py --sf 0.01 --queries all --output json --report my-report.json

# Skip data load (tables already populated)
python scripts/tpch/bench.py --no-load --queries all
```

## Query Status (SF=0.01)

| Query | Business Question | Status |
|-------|------------------|--------|
| Q01 | Pricing summary by return flag | ✅ PASS |
| Q02 | Minimum cost supplier | ⚠️ PARTIAL |
| Q03 | Shipping priority | ✅ PASS |
| Q04 | Order priority checking (EXISTS) | ✅ PASS |
| Q05 | Local supplier volume | ✅ PASS |
| Q06 | Forecasting revenue change | ✅ PASS |
| Q07 | Volume shipping between nations | ⚠️ PARTIAL |
| Q08 | National market share | ⚠️ PARTIAL |
| Q09 | Product type profit measure | ⚠️ PARTIAL |
| Q10 | Returned item reporting | ✅ PASS |
| Q11 | Important stock identification | ⚠️ PARTIAL |
| Q12 | Shipping modes priority | ✅ PASS |
| Q13 | Customer distribution (LEFT JOIN) | ✅ PASS |
| Q14 | Promotion effect | ✅ PASS |
| Q15 | Top supplier (CTE) | ✅ PASS |
| Q16 | Parts/supplier (NOT IN + COUNT DISTINCT) | ✅ PASS |
| Q17 | Small-quantity order revenue (scalar subquery) | ✅ PASS |
| Q18 | Large volume customer (IN subquery) | ✅ PASS |
| Q19 | Discounted revenue | ✅ PASS |
| Q20 | Potential part promotion (nested IN) | ✅ PASS |
| Q21 | Suppliers keeping orders waiting | ⚠️ PARTIAL |
| Q22 | Global sales opportunity | ⚠️ PARTIAL |

**14 PASS, 8 PARTIAL, 0 FAIL** at SF=0.01

## Clean Up

```sql
SOURCE samples/teardown.sql
```
