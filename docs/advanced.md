# WOW-DB Advanced Guide

This guide covers cluster architecture, configuration, DDL reference, operations, and performance tuning for production deployments.

---

## 1. Cluster Architecture

WOW-DB uses a three-tier shared-nothing architecture:

```
MySQL Client
      │  MySQL Wire Protocol (port 9030)
      ▼
 Query Node (QN) × 3       — SQL parser, planner, Raft consensus, Web UI
      │  gRPC (port 9040)
      ▼
Compute Node (CN) × 3      — SIMD execution, hash join, aggregation
      │  gRPC (port 9060)
      ▼
Storage Node (SN) × 3      — LSM-Tree, columnar SSTs, WAL
```

### Port Map

| Node | Service | Default Port |
|------|---------|-------------|
| QN | MySQL Wire Protocol | 9030 |
| QN | Web/monitoring HTTP | 8080 |
| QN | Raft consensus | 9010 |
| QN | Internal gRPC | 9011 |
| CN | Compute gRPC | 9040 |
| SN | Storage gRPC | 9060 |
| SN | Status/metrics HTTP | 8040 |

For multi-QN deployments, ports are offset by 10000 per additional node:
- QN-1: MySQL=9030, Web=8080
- QN-2: MySQL=19030, Web=18080
- QN-3: MySQL=29030, Web=28080

### Data Distribution

Data is partitioned by hash of the first column across all SNs. Each INSERT is routed to a SN based on the row's partition key. All SNs are queried on SELECT and results are merged at QN.

---

## 2. Configuration Reference

### Environment Variables

All nodes are configured via environment variables (Docker Compose) or TOML files (native).

#### Query Node (QN)

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | `qn-1` | Unique node identifier |
| `MYSQL_PORT` | `9030` | MySQL Wire Protocol listen port |
| `WEB_PORT` | `8080` | Web UI and metrics HTTP port |
| `RAFT_PORT` | `9010` | Raft consensus port |
| `GRPC_PORT` | `9011` | Internal QN gRPC port |
| `COMPUTE_NODES` | — | Comma-separated CN endpoints: `cn-1:9040,cn-2:9040` |
| `STORAGE_NODES` | — | Comma-separated SN endpoints: `sn-1:9060,sn-2:9060` |
| `QN_HTTP_PEERS` | — | Comma-separated QN HTTP peers for Raft: `qn-1:8080,qn-2:8080` |
| `LOG_DIR` | `/var/log/wow-db` | Log file directory |
| `DATA_DIR` | `/data` | Raft state directory |

#### Compute Node (CN)

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | `cn-1` | Unique node identifier |
| `GRPC_PORT` | `9040` | Compute gRPC listen port |
| `LOG_DIR` | `/var/log/wow-db` | Log file directory |

#### Storage Node (SN)

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | `sn-1` | Unique node identifier |
| `GRPC_PORT` | `9060` | Storage gRPC listen port |
| `HTTP_PORT` | `8040` | Status and metrics HTTP port |
| `DATA_DIR` | `/data` | LSM-Tree data directory |
| `LOG_DIR` | `/var/log/wow-db` | Log file directory |

### TOML Configuration (Native)

For native (non-Docker) deployments, config lives in `dev/configs/`:
- `query-node-local.toml` — QN configuration
- `compute-node-local.toml` — CN configuration
- `storage-node-local.toml` — SN configuration

---

## 3. DDL Reference

### CREATE CUBE

WOW-DB tables are called **Cubes** (event-optimized columnar tables).

```sql
CREATE CUBE [IF NOT EXISTS] table_name (
    column_name data_type [NOT NULL],
    ...
);
```

**Supported data types**: `VARCHAR(n)`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `DATE`

**Example:**
```sql
CREATE CUBE IF NOT EXISTS events (
    event_id    VARCHAR(36)  NOT NULL,
    session_id  VARCHAR(36)  NOT NULL,
    user_id     VARCHAR(64),
    event_type  VARCHAR(32)  NOT NULL,
    page_url    VARCHAR(512) NOT NULL,
    ts          BIGINT       NOT NULL
);
```

### DROP CUBE

```sql
DROP CUBE [IF EXISTS] table_name;
```

### SHOW CUBES

```sql
SHOW CUBES;
```

### DESCRIBE

```sql
DESCRIBE table_name;
-- or
DESC table_name;
```

### SHOW CREATE CUBE

```sql
SHOW CREATE CUBE table_name;
```

### CREATE SESSION MATERIALIZED VIEW

Session MVs pre-aggregate raw event streams into session-level summaries.

```sql
CREATE SESSION MATERIALIZED VIEW view_name
ON source_table
KEY user_identifier_column
TIMEOUT session_timeout_minutes;
```

- `ON source_table` — the raw event Cube
- `KEY column` — column used to group events into sessions (e.g., `user_id`, `visitor_id`)
- `TIMEOUT N` — session gap timeout in minutes (events > N minutes apart start a new session)

**Example:**
```sql
CREATE SESSION MATERIALIZED VIEW sessions ON events KEY user_id TIMEOUT 30;
```

### ALTER CUBE

```sql
ALTER CUBE table_name ADD COLUMN column_name data_type;
```

---

## 4. Operations Runbook

### Rolling Restart

Restart each node one at a time. Verify it reaches `healthy` status before restarting the next.

```bash
# Restart QN-1, verify, then QN-2, then QN-3
docker compose -f docker/docker-compose.yml restart qn-1
docker compose -f docker/docker-compose.yml ps qn-1  # wait for "healthy"

docker compose -f docker/docker-compose.yml restart qn-2
docker compose -f docker/docker-compose.yml ps qn-2

docker compose -f docker/docker-compose.yml restart qn-3
docker compose -f docker/docker-compose.yml ps qn-3

# Restart SNs (data nodes — restart one at a time)
docker compose -f docker/docker-compose.yml restart sn-1
docker compose -f docker/docker-compose.yml ps sn-1

docker compose -f docker/docker-compose.yml restart sn-2
docker compose -f docker/docker-compose.yml ps sn-2

docker compose -f docker/docker-compose.yml restart sn-3
docker compose -f docker/docker-compose.yml ps sn-3
```

During SN restart, queries continue to be served by remaining SNs. Row counts may temporarily be incomplete until the restarted SN rejoins.

### Check Cluster Health

```bash
# All nodes status
docker compose -f docker/docker-compose.yml --profile full ps

# SN LSM status
curl http://localhost:18040/api/v1/lsm-status
curl http://localhost:28040/api/v1/lsm-status
curl http://localhost:38040/api/v1/lsm-status
```

### Log Rotation

Logs are written to `LOG_DIR` (default `/var/log/wow-db`). Mount a host directory to collect logs:
```yaml
volumes:
  - ./logs:/var/log/wow-db
```

### Node Addition

1. Add the new node to `docker-compose.yml` following the existing node template
2. Add the new node's endpoint to `COMPUTE_NODES` / `STORAGE_NODES` / `QN_HTTP_PEERS` on all QNs
3. Restart QNs (rolling) to pick up new config

---

## 5. Performance Tuning

### LSM-Tree Parameters

The storage node uses an LSM-Tree with configurable flush and compaction thresholds:

| Parameter | Default | Effect |
|-----------|---------|--------|
| MemTable flush threshold | 4 MB | Larger → fewer L0 files, more memory |
| L0 file compaction trigger | 4 files | Smaller → more frequent compaction, lower read amplification |
| Block size | 64 KB | Larger → better sequential scan throughput |

### gRPC Message Limit

QN↔CN and CN↔SN gRPC calls are capped at **256 MiB** per message. For queries returning very large result sets, apply `LIMIT` or increase batch sizes.

### Partition Strategy

Data is distributed via hash of the first column. Choose a high-cardinality column as the first column in your Cube DDL to maximize even distribution:
```sql
CREATE CUBE IF NOT EXISTS events (
    event_id VARCHAR(36) NOT NULL,  -- high cardinality → good partition key
    ...
);
```

### Recommended Hardware Ratios

| Role | CPU | RAM | Disk |
|------|-----|-----|------|
| QN | 4 cores | 4 GB | 10 GB (Raft log) |
| CN | 8 cores | 8 GB | — |
| SN | 4 cores | 8 GB | 100 GB NVMe SSD |

---

## 6. Behavioral Routing

WOW-DB transparently rewrites queries that reference Session MV columns to read from the pre-aggregated behavioral table (BT).

### When Routing Fires

A query is routed to the Session MV BT if:
- It references any column named `session_id`, `SESSION_START`, `SESSION_END`, or any column starting with `SESSION_`
- The source table has an associated Session MV defined

### Example

```sql
-- Source table: events (raw)
-- Session MV: sessions (pre-aggregated)

-- This query is transparently routed to `sessions`:
SELECT session_id, COUNT(*) AS event_count
FROM events
GROUP BY session_id;

-- Internally rewritten to:
SELECT session_id, COUNT(*) AS event_count
FROM sessions
GROUP BY session_id;
```

### Bypassing Routing

To query the raw event table without routing, avoid `session_id` column references or use column aliases:
```sql
-- No routing: references only non-session columns
SELECT COUNT(*) FROM events WHERE page_url = '/home';
```

### Creating and Populating Session MVs

1. Define the MV: `CREATE SESSION MATERIALIZED VIEW sessions ON events KEY user_id TIMEOUT 30`
2. Create the backing Cube with session schema
3. Populate with pre-aggregated data via INSERT

---

## 7. Monitoring

### Web Dashboard

Each QN exposes a monitoring dashboard:
- **QN-1**: http://localhost:8080
- **QN-2**: http://localhost:18080
- **QN-3**: http://localhost:28080

The dashboard shows:
- All cluster nodes with ONLINE/OFFLINE status
- Per-node row counts
- Recent query log
- LSM-Tree compaction status per SN

### Metrics Endpoint

```bash
curl http://localhost:8080/metrics  # Prometheus-format metrics
```

---

## 8. Security

**Current status**: WOW-DB does not implement authentication or TLS in this release.

**Known limitations:**

| # | Severity | Description | Workaround |
|---|----------|-------------|-----------|
| 1 | Major | No authentication — any client can connect | Restrict port 9030 at the firewall to trusted hosts only |
| 2 | Major | No TLS — all traffic is plaintext | Deploy within a private network or VPN |
| 3 | Minor | Root user has full access | Deploy behind a MySQL proxy for multi-tenant use |

**Recommended network isolation:**
```bash
# Allow only specific IP ranges to reach MySQL port
iptables -A INPUT -p tcp --dport 9030 -s 10.0.0.0/8 -j ACCEPT
iptables -A INPUT -p tcp --dport 9030 -j DROP
```
