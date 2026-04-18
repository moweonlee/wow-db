# WOW-DB

**MySQL-compatible OLAP database purpose-built for web analytics at scale.**

> Process 20 billion+ user events. Query sessions, funnels, cohorts, and paths — all with standard MySQL syntax.  
> **The engine automatically selects the optimal physical layout. You always query the base table.**

---

## The #1 Differentiator: Transparent Dual-Layout Query Routing

Most analytics systems force you to choose between two worlds:

- **Event-level tables**: fast for raw counts and time-series aggregations
- **Session-level materialized views**: fast for funnel, cohort, and path analysis

Choosing wrong costs 4–10× in query latency. Choosing right requires understanding internal storage layout — something analysts shouldn't have to know.

**WOW-DB eliminates this choice entirely.**

WOW-DB maintains two physical data layouts simultaneously and **automatically routes every query to the optimal layout** based on what the query is actually asking for:

```
You always write:                      WOW-DB automatically executes against:
──────────────────────────────────────────────────────────────────────────────
SELECT FUNNEL_COUNT(...)               → page_events_sessions  (Session MV)
FROM page_events WHERE ...               4–10× faster: pre-computed session
                                         boundaries, event_sequence arrays

SELECT COHORT_ANALYSIS(...)            → page_events_sessions  (Session MV)
FROM page_events WHERE ...               No per-user regrouping needed

SELECT PATH_ANALYSIS(...)              → page_events_sessions  (Session MV)
FROM page_events WHERE ...               event_sequence already ordered

SELECT COUNT(*) FROM page_events       → page_events           (Event Table)
WHERE DATE(event_time) = YESTERDAY       Simple event rollup, no session needed

SELECT event_name, COUNT(*)            → page_events           (Event Table)
FROM page_events GROUP BY event_name     Aggregation over event types
```

The routing decision lives entirely inside the query optimizer — no hints, no table aliasing, no manual rewrites. The analyst writes `FROM page_events` always, and the engine figures out the rest.

### How It Works

WOW-DB runs a **SMV Rewrite Pass** immediately after logical planning:

```
SQL → AST → LogicalPlan
                 ↓
        [SMV Rewrite Pass]          ← scans for FUNNEL_COUNT / COHORT_ANALYSIS /
                 │                     PATH_ANALYSIS / session_* column refs
          Pattern A (session)
                 │ YES
                 ↓
          CBO cost comparison:
          C(event_table) vs C(session_mv)
                 │
          C_smv < C_event × 0.7?
                 │ YES
                 ↓
          Rewrite FROM page_events → page_events_sessions
          Map columns: event_time → session_start, etc.
                 ↓
          Physical Plan → Execute on Session MV
```

Fallback is automatic and silent: if the Session MV is stale, being rebuilt, or doesn't cover the query's time range, the engine falls back to the Event Table with no error returned.

### What This Means for Web Analytics

Web analytics has exactly two dominant workloads. WOW-DB handles both without you specifying which one you're doing:

| Workload | What you write | What runs | Speedup |
|---|---|---|---|
| **Session behavior** (funnel, cohort, path) | `FROM page_events` | Session MV (pre-computed boundaries) | 4–10× |
| **Event rollup** (counts, time-series, raw events) | `FROM page_events` | Event Table (columnar scan) | Baseline |

No other analytics database does this automatically. ClickHouse, StarRocks, and BigQuery require you to build, maintain, and explicitly reference materialized views yourself.

---

## Why WOW-DB Exists

Modern web services emit hundreds of billions of user events. Existing OLAP systems (ClickHouse, StarRocks, BigQuery) are general-purpose: they store the data efficiently but force analysts to manually implement the abstractions that web analytics actually needs.

With a general OLAP database, building a funnel analysis looks like:

```sql
-- ClickHouse / StarRocks — 30+ lines of window functions and CTEs
WITH ordered AS (
  SELECT user_id, event_name, event_time,
         row_number() OVER (PARTITION BY user_id ORDER BY event_time) AS rn
  FROM events
  WHERE event_time >= today() - 7
),
step1 AS (SELECT DISTINCT user_id FROM ordered WHERE event_name = 'page_view'),
step2 AS (
  SELECT o.user_id FROM ordered o
  JOIN step1 s ON o.user_id = s.user_id
  WHERE o.event_name = 'add_to_cart'
    AND o.event_time BETWEEN ... AND ...
),
...
SELECT count(*) FROM step1, count(*) FROM step2, ...
```

With WOW-DB, this is:

```sql
SELECT FUNNEL_COUNT(
    user_key  => user_id,
    timestamp => event_time,
    window    => INTERVAL 7 DAY,
    steps     => [
        event_name = 'page_view',
        event_name = 'add_to_cart',
        event_name = 'purchase'
    ]
) AS funnel
FROM page_events
WHERE event_time >= CURRENT_DATE - 7;
```

And the engine automatically routes this to the Session MV where session boundaries are pre-computed — **you get both simplicity and performance.**

---

## What Makes WOW-DB Different

| Capability | ClickHouse | StarRocks | WOW-DB |
|---|---|---|---|
| **Transparent dual-layout routing** | ❌ Manual MV reference | ❌ Manual MV reference | ✅ **Auto: engine selects Event vs Session MV** |
| **MySQL wire protocol** | Partial | Yes | Yes (full 8.0 compat) |
| **Event → Behavioral Table automation** | Manual | Manual | `CREATE SESSION MATERIALIZED VIEW` → Behavioral Table |
| **Built-in funnel functions** | No | No | `FUNNEL_COUNT()` |
| **Built-in cohort functions** | No | No | `COHORT_ANALYSIS()` |
| **Built-in path functions** | No | No | `PATH_ANALYSIS()` |
| **Storage-Compute separation** | No | Partial | Yes (native + S3/HDFS) |
| **Cluster-wide read-only guard** | No | No | `ClusterGuard` (disk-full protection) |
| **Flat JSON auto-extraction** | No | Partial | Yes (Compaction-time auto-columnize) |
| **Auto-partitioning on INSERT** | Partial | Yes | Yes (DAY / MONTH / YEAR) |
| **Stateless query nodes** | No | No | Yes (Raft metadata, K8s-friendly) |
| **Dynamic node add/remove** | Manual restart | Manual | `ALTER CLUSTER JOIN/DRAIN/DISMISS` |
| **Auto Shard rebalance** | No | Partial | Yes (background, zero-downtime) |
| **Graceful drain (data-safe)** | No | No | Yes (DRAIN → DISMISS, data migrates first) |
| **Colocate groups** | No | Yes | Yes |
| **Storage tiering** | Partial | No | Yes (Hot NVMe → Cold S3) |
| **Implementation language** | C++ | C++/Java | Rust (SIMD-first) |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         CLIENT LAYER                                │
│                                                                     │
│   MySQL Workbench     Web SQL Editor     Spark Connector     Kafka  │
│   mysql -h host       Browser UI         Spark 3.x           JSON  │
│      :9030              :8080              :8040              topic │
└──────────┬──────────────────┬────────────────┬─────────────────────┘
           │  MySQL Protocol  │  HTTP/WS       │ Stream / gRPC
           ▼                  ▼                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    QUERY NODE CLUSTER (Raft)                        │
│                                                                     │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐            │
│  │  QN Leader   │◄──│  QN Follow   │◄──│  QN Follow   │  (3,5,7…) │
│  │              │──►│              │──►│              │            │
│  │ SQL Parser   │   │ Raft Replica │   │ Raft Replica │            │
│  │ CBO / Stats  │   └──────────────┘   └──────────────┘            │
│  │ Logical Plan │                                                   │
│  │ Phys. Plan   │  All QN pods hold identical Raft-replicated       │
│  │ Table Manager │  metadata → any pod handles any request           │
│  │ Session Mgr  │  (K8s LoadBalancer friendly)                      │
│  │ ClusterGuard │                                                   │
│  └──────┬───────┘                                                   │
└─────────┼───────────────────────────────────────────────────────────┘
          │  Physical Plan Fragments (gRPC :9040)
          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    COMPUTE NODE CLUSTER                             │
│                                                                     │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐            │
│  │  Compute  1  │   │  Compute  2  │   │  Compute  N  │            │
│  │              │   │              │   │              │            │
│  │ SIMD Executor│   │ Hash Join    │   │ Vectorized   │            │
│  │ AVX2/AVX-512 │   │ Sort / Merge │   │ Aggregation  │            │
│  │ Funnel Exec  │   │ Data Shuffle │   │ Cohort Exec  │            │
│  │ Path Exec    │   │ Runtime Flt  │   │ Pipeline Sch │            │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘            │
└─────────┼──────────────────┼──────────────────┼────────────────────┘
          │  Column chunks (gRPC :9060)          │
          ▼                  ▼                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    STORAGE NODE CLUSTER                             │
│                                                                     │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐            │
│  │  Storage  1  │   │  Storage  2  │   │  Storage  N  │            │
│  │              │   │              │   │              │            │
│  │ LSM-Tree     │   │ LSM-Tree     │   │ LSM-Tree     │            │
│  │ MemTable+WAL │   │ Compaction   │   │ Bloom Filter │            │
│  │ Columnar SST │   │ Block Cache  │   │ Skipping Idx │            │
│  │ Partition Mgr│   │ Flat JSON    │   │ TTL / Tier   │            │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘            │
└─────────┼──────────────────┼──────────────────┼────────────────────┘
          │                  │                   │
    ┌─────▼──────┐    ┌──────▼─────┐    ┌────────▼────┐
    │ NVMe SSD   │    │  AWS S3 /  │    │    HDFS     │
    │ (Native    │    │  MinIO /   │    │ (Kerberos   │
    │  LSM)      │    │  Ceph RGW) │    │  required)  │
    └────────────┘    └────────────┘    └─────────────┘
         HOT                              COLD (Tiered)
```

### Data Flow: a single SELECT query

```
Client
  │  "SELECT FUNNEL_COUNT(...) FROM page_events WHERE ..."
  ▼
Query Node
  ├─ Parse SQL → AST
  ├─ CBO: fetch column stats (min/max/NDV/histogram)
  ├─ Partition pruning: skip partitions outside WHERE range
  ├─ Build Physical Plan with Funnel fragment
  └─ Fan out fragments to Compute Nodes
        │
        ▼
Compute Node × N  (parallel)
  ├─ Request only needed columns from Storage Nodes
  ├─ SIMD scan: AVX2 filter on event_name bitmap
  ├─ Per-user step bitmask accumulation + time-window check
  └─ Partial funnel counts → Query Node
        │
        ▼
Query Node
  ├─ Merge partial results
  ├─ Record in Query Profiler (circular buffer, 1000 entries)
  └─ Return MySQL result set
```

---

## Storage Layout

WOW-DB stores each column in its own file per partition, enabling analytical queries to read only the columns they touch:

```
storage-node/data/
└── page_events/
    ├── partition=2026_04_12/          ← AUTO PARTITION BY DAY
    │   ├── event_time/
    │   │   ├── seg_0001.col           ← Delta-encoded timestamps (LZ4)
    │   │   └── seg_0001.min_max       ← CBO statistics
    │   ├── event_name/
    │   │   ├── seg_0001.col           ← Dictionary-encoded strings
    │   │   ├── seg_0001.dict          ← String→int dictionary
    │   │   └── seg_0001.bloom         ← Bloom filter (point lookup)
    │   ├── user_id/
    │   │   └── seg_0001.col           ← BitPacked integers
    │   └── properties/
    │       ├── seg_0001.col           ← JSON blob (ZSTD)
    │       └── _flat/                 ← Auto-extracted hot keys
    │           ├── price/seg_0001.col ← Compaction promoted to column
    │           └── _flat_meta.json    ← Key list, occurrence rates
    └── partition=2026_04_13/
        └── ...
```

LSM-Tree merge is strictly partition-scoped — cross-partition merges never occur, so old partitions remain immutable and safe to tier to S3.

---

## Key Features

### Automatic Sessionization → Behavioral Table

Define it once; WOW-DB maintains the Behavioral Table automatically:

```sql
CREATE SESSION MATERIALIZED VIEW page_events_sessions
FROM page_events
USER KEY (user_id)
SESSION TIMEOUT 30 MINUTE;
```

The Behavioral Table receives every INSERT into `page_events` and continuously computes session boundaries, session IDs, and per-session event sequences. No ETL pipeline required.

Once created, **all Funnel/Cohort/Path queries on `page_events` automatically route to this Behavioral Table** — you never need to reference it explicitly. If you haven't created one yet, WOW-DB tells you how and estimates the speedup.

### Web Analytics Functions

```sql
-- Funnel: conversion at each step within a time window
SELECT FUNNEL_COUNT(
    user_key => user_id, timestamp => event_time,
    window   => INTERVAL 24 HOUR,
    steps    => [event_name='page_view', event_name='add_to_cart', event_name='purchase']
) FROM page_events WHERE event_time >= '2026-04-01';

-- Cohort: weekly retention after signup
SELECT COHORT_ANALYSIS(
    user_key    => user_id,   timestamp => event_time,
    entry_event => 'signup',  return_event => 'purchase',
    granularity => 'week',    periods => 8
) FROM page_events;

-- Path: top N user journeys
SELECT PATH_ANALYSIS(
    user_key        => user_id, timestamp => event_time,
    max_steps       => 5,
    session_timeout => INTERVAL 30 MINUTE
) FROM page_events;
```

### Data Distribution Visibility

No black-box distributions. Operators can inspect exactly how data is spread across Storage Nodes at any granularity:

```sql
-- Partition distribution (time/key ranges, row counts, sizes)
SHOW PARTITIONS FROM page_events ORDER BY size_bytes DESC LIMIT 10;

-- Shard distribution (which Storage Node holds what)
SHOW SHARDS FROM page_events WHERE sn_node_id = 'sn-01';

-- LSM Part (SSTable) level — detect Compaction lag
SHOW PARTS FROM page_events WHERE level = 0;  -- many L0 = compaction lag

-- Node-level summary
SHOW DISTRIBUTED STATUS FROM page_events ORDER BY size_bytes DESC;
```

### Dynamic Cluster Management

WOW-DB clusters scale horizontally without downtime. Every node type — Query Node, Compute Node, Storage Node — can be added or removed while queries run.

**All nodes register with QN.** When a new node starts, it reads the QN address from the `QN_PEERS` environment variable (or Kubernetes ConfigMap `qn.peers`) and self-registers:

```
New SN Pod starts
  └─ reads QN_PEERS from ConfigMap
  └─ sends RegisterNode gRPC to QN
  └─ QN commits JOIN to Raft KV
  └─ QN starts background Shard Rebalance automatically
```

**Node Lifecycle**:

```
ACTIVE ──── ALTER CLUSTER DRAIN ────► READONLY ──► DRAINING ──► [DISMISSED]
              INSERT routing removed   data migration in background
              within 100ms
```

| State | INSERT routing | SELECT serving | New shards |
|-------|---------------|----------------|------------|
| ACTIVE | ✅ | ✅ | ✅ |
| READONLY | ❌ | ✅ | ❌ |
| DRAINING | ❌ | ✅ | ❌ |
| DISMISSED | — | — | — |

**Cluster management SQL commands**:

```sql
-- Add a new Storage Node (auto-triggers rebalance)
ALTER CLUSTER JOIN SN '10.0.0.7:9060' AS 'sn-4';

-- Gracefully remove a node (data migrates to other SNs in background)
ALTER CLUSTER DRAIN 'sn-4';
-- → monitor progress:
SHOW CLUSTER REBALANCE;
-- → remove after drain completes:
ALTER CLUSTER DISMISS 'sn-4';

-- Force-remove a node with data (deletes shard metadata — data loss warning)
ALTER CLUSTER DISMISS 'sn-4' FORCE;

-- Inspect cluster topology
SHOW CLUSTER NODES;
SHOW CLUSTER STATUS;
```

**DRAIN safety**: A node with data cannot be dismissed without `FORCE`. WOW-DB rejects the command:
```
ERROR 3001 (WW000): Node 'sn-4' has 8 shards with data.
Run 'ALTER CLUSTER DRAIN sn-4' first, or use FORCE to permanently delete all data.
```

**Kubernetes / Helm scale-out**:

```yaml
# helm/wowdb/values.yaml — just change replicas
storageNode:
  replicas: 5   # was 3 — new SNs self-register and trigger rebalance

computeNode:
  replicas: 4   # CN Deployment + HPA auto-scales on CPU
  autoscaling:
    enabled: true
    maxReplicas: 16

queryNode:
  replicas: 5   # must stay odd (Raft quorum)
```

QN is a `StatefulSet` (stable Raft IDs), CN is a `Deployment` (HPA-friendly), SN is a `StatefulSet + PVC` (data survives rescheduling).

### Cluster Protection

WOW-DB automatically enters cluster-wide read-only mode when any Storage Node approaches disk full (95% threshold, 85% recovery hysteresis). All INSERT/DDL statements are rejected with MySQL error 1290 until space is freed. The state is persisted in Raft KV so every Query Node enforces it consistently.

### Flat JSON Auto-Extraction

The `properties JSON` column is stored compressed. During Compaction, WOW-DB analyses key occurrence rates. Keys appearing in ≥ threshold% of rows are automatically promoted to dedicated column files — gaining SIMD scans, Bloom filters, and CBO statistics — without any schema migration.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Primary language | Rust 1.87 stable |
| SIMD | AVX2 / AVX-512 via `std::arch` |
| Consensus | Custom Raft (query-node) |
| Serialization | Protobuf (tonic + prost) |
| Columnar exchange | Apache Arrow IPC |
| Object storage | AWS SDK for Rust (S3 / MinIO) |
| Streaming ingest | rdkafka (librdkafka C bindings) |
| Inter-node RPC | gRPC / tonic |
| MySQL protocol | opensrv-mysql |
| Async runtime | tokio |

---

## Getting Started

### Prerequisites

| Tool | Version |
|---|---|
| Rust | 1.87+ (`rustup update stable`) |
| Docker | 25.0+ |
| Docker Compose | v2.24+ |
| protoc | 3.21+ |

### Run a full cluster locally

```bash
git clone <repo> wow-db && cd wow-db
git checkout 002-wow-db-srs-v02

# Start QN×3, CN×2, SN×3 + MinIO + Redpanda
docker compose up --build

# Connect with any MySQL client
mysql -h 127.0.0.1 -P 9030 -u admin -p''
```

### First query

```sql
CREATE TABLE IF NOT EXISTS page_events (
    event_time  DATETIME    NOT NULL,
    user_id     VARCHAR(64) NOT NULL,
    event_name  VARCHAR(128) NOT NULL,
    properties  JSON
)
AUTO PARTITION BY DAY
ORDER BY (event_time, user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 8;

INSERT INTO page_events VALUES
('2026-04-12 10:00:00', 'u001', 'page_view',   '{"ref":"google"}'),
('2026-04-12 10:01:00', 'u001', 'add_to_cart', '{"item":"A100"}'),
('2026-04-12 10:03:00', 'u001', 'purchase',    '{"amount":49.99}');

SELECT FUNNEL_COUNT(
    user_key  => user_id,
    timestamp => event_time,
    window    => INTERVAL 24 HOUR,
    steps     => [
        event_name = 'page_view',
        event_name = 'add_to_cart',
        event_name = 'purchase'
    ]
) AS funnel
FROM page_events
WHERE event_time >= '2026-04-12';
```

### Run tests

```bash
cargo test --workspace          # all unit tests
cargo test -p query-node        # query node only
cargo test -p storage-node      # storage node only
```

---

## Port Reference

| Node | Port | Purpose |
|---|---|---|
| Query Node | 9030 | MySQL wire protocol |
| Query Node | 8080 | Web SQL Editor (HTTP/WebSocket) |
| Query Node | 9010 | Raft consensus (QN↔QN) |
| Query Node | 9011 | Internal gRPC (QN↔CN/SN, ClusterService node registration) |
| Compute Node | 9040 | Fragment plan receiver (gRPC) |
| Storage Node | 9060 | Storage API (gRPC) |
| Storage Node | 8040 | HTTP Stream Load (Spark) |

---

## Project Layout

```
query-node/         MySQL protocol, SQL parser, CBO, Raft, Web UI
compute-node/       SIMD executor, hash join, analytics functions
storage-node/       LSM-Tree, columnar files, S3/HDFS backend
shared/             Common types, Arrow IPC codec, error handling
proto/              Protobuf definitions (tonic + prost)
integration-tests/  End-to-end cluster tests
docker/             Docker Compose, Dockerfiles, config templates
specs/              Software Requirements Specification
```

---

## Design Principles

- **Transparent dual-layout routing**: the engine maintains event-level and session-level layouts simultaneously and automatically routes each query to the optimal one — analysts always query the base event table
- **Write-first**: LSM-Tree accepts high-throughput event streams without sacrificing read performance after Compaction
- **Column-oriented**: only touched columns are read from disk — multi-billion-row aggregations scan megabytes, not gigabytes
- **SIMD everywhere**: scan, filter, hash-join, aggregation all use AVX2 or wider; no scalar fallback in hot paths
- **Storage-Compute separation**: Storage Nodes scale independently from Compute Nodes; S3/HDFS backends allow infinite cold storage at low cost
- **Stateless Query Nodes**: all state lives in Raft KV, so Kubernetes can route any request to any QN pod
- **Dynamic cluster management**: nodes join and leave while queries run; `ALTER CLUSTER JOIN/DRAIN/DISMISS` is the single interface for topology changes; Shard rebalancing happens automatically in the background
- **Graceful drain**: a Storage Node with data can never be force-removed without explicit `FORCE` — by default, draining migrates all Shards first, so no data is lost
- **Schema-on-write**: Table schema is fixed at creation; the optimizer knows all types and cardinalities ahead of time

---

## Comparison with Reference Systems

WOW-DB draws inspiration from several proven systems:

| System | What we borrowed | What we improved |
|---|---|---|
| **StarRocks** | FE/BE separation, Tablet distribution, Routine Load | Stateless QN (Raft-replicated meta), native web analytics functions |
| **ClickHouse** | MergeTree columnar layout, real-time ingest, SIMD patterns | Storage-Compute separation, sessionization MV, MySQL compat |
| **Snowflake** | Storage-Compute decoupling, multi-cloud object storage | On-premise NVMe mode, open protocol, Kafka-native ingest |
| **RocksDB** | LSM-Tree concepts | Full Rust re-implementation, partition-scoped merge, columnar layout |
| **MySQL 8.0** | Wire protocol, DDL/DML grammar | OLAP execution engine, horizontal scale, event-native types |

---

*WOW-DB is under active development on branch `002-wow-db-srs-v02`.*
