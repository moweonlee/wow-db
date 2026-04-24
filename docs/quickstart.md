# WOW-DB Quick Start Guide

> **Time to first query**: ~10 minutes | **Prerequisites**: Docker Desktop 4.x+

WOW-DB is a MySQL-compatible OLAP database for web analytics workloads. This guide starts a single-node cluster and runs your first queries.

---

## 1. Requirements

| Item | Minimum | Recommended |
|------|---------|-------------|
| RAM | 4 GB (single-node) | 8 GB (full cluster) |
| CPU | 2 cores | 4+ cores |
| Disk | 2 GB free | 10 GB |
| Docker | Desktop 4.x / Engine 24+ | latest |

---

## 2. Start the Cluster

```bash
# Clone the repository
git clone https://github.com/your-org/wow-db && cd wow-db

# Start full cluster: QN×3, CN×3, SN×3
docker compose -f docker/docker-compose.yml --profile full up -d

# Wait for all nodes to become healthy (~30-45 seconds)
docker compose -f docker/docker-compose.yml ps
```

Expected output (all STATUS should show `healthy`):
```
NAME                STATUS
wowdb-qn-1         Up 42 seconds (healthy)
wowdb-qn-2         Up 42 seconds (healthy)
wowdb-qn-3         Up 42 seconds (healthy)
wowdb-cn-1         Up 38 seconds (healthy)
wowdb-cn-2         Up 38 seconds (healthy)
wowdb-cn-3         Up 38 seconds (healthy)
wowdb-sn-1         Up 35 seconds (healthy)
wowdb-sn-2         Up 35 seconds (healthy)
wowdb-sn-3         Up 35 seconds (healthy)
```

---

## 3. Connect

**Using mysql CLI (from the included client container):**
```bash
docker exec -it wowdb-mysql-client mysql -h qn-1 -P 9030 -u root
```

**Using mysql CLI from your host machine:**
```bash
mysql -h 127.0.0.1 -P 9030 -u root
```

**Using DBeaver / TablePlus / any MySQL-compatible GUI:**
- Host: `127.0.0.1`
- Port: `9030`
- User: `root`
- Password: *(leave blank)*
- Database: *(leave blank for now)*

---

## 4. Your First Queries

Copy and paste the following into your MySQL client:

```sql
-- Create a database
CREATE DATABASE IF NOT EXISTS demo;
USE demo;

-- Create an Event Cube (WOW-DB's core table type)
CREATE CUBE IF NOT EXISTS pageviews (
    event_id   VARCHAR(36)  NOT NULL,
    user_id    VARCHAR(64),
    page_url   VARCHAR(256) NOT NULL,
    country    VARCHAR(2),
    ts         BIGINT       NOT NULL
);

-- Insert sample data
INSERT INTO pageviews VALUES
  ('e1', 'alice', '/home',    'KR', 1714000000000),
  ('e2', 'alice', '/product', 'KR', 1714000010000),
  ('e3', 'bob',   '/home',    'US', 1714000020000),
  ('e4', 'bob',   '/pricing', 'US', 1714000030000),
  ('e5', NULL,    '/home',    'KR', 1714000040000);

-- Count total events
SELECT COUNT(*) FROM pageviews;
-- Expected: 5

-- Top pages by views
SELECT page_url, COUNT(*) AS views
FROM pageviews
GROUP BY page_url
ORDER BY views DESC;

-- Events by country
SELECT country, COUNT(*) AS events
FROM pageviews
GROUP BY country;
```

---

## 5. Try the Session Materialized View (WOW-DB Unique Feature)

```sql
-- Create a sessions pre-aggregation (Session MV)
-- KEY = the user identifier column
-- TIMEOUT = session inactivity window in minutes
CREATE SESSION MATERIALIZED VIEW sessions ON pageviews KEY user_id TIMEOUT 30;

-- Create the sessions storage cube
CREATE CUBE IF NOT EXISTS sessions (
    session_id   VARCHAR(36) NOT NULL,
    user_id      VARCHAR(64),
    page_count   INT         NOT NULL,
    entry_page   VARCHAR(256) NOT NULL,
    duration_ms  BIGINT      NOT NULL,
    country      VARCHAR(2)
);

-- Insert pre-aggregated session data
INSERT INTO sessions VALUES
  ('s1', 'alice', 2, '/home',    10000, 'KR'),
  ('s2', 'bob',   2, '/home',    10000, 'US'),
  ('s3', NULL,    1, '/home',    0,     'KR');

-- Query sessions — served from the pre-aggregated view
SELECT user_id, COUNT(*) AS session_count, MIN(duration_ms) AS min_dur
FROM sessions
GROUP BY user_id;
```

---

## 6. Monitoring Dashboard

Open the web monitoring dashboard in your browser:
- **QN-1**: http://localhost:8080
- **QN-2**: http://localhost:18080
- **QN-3**: http://localhost:28080

You should see all 9 nodes (3 QN + 3 CN + 3 SN) listed as ONLINE.

---

## 7. Load the Sample Dataset

Run the included TPC-H sample to load ~60,000 rows and run 6 analytical queries:

```bash
cd scripts/tpch
python bench.py --host 127.0.0.1 --port 9030 --sf 0.01
```

---

## 8. Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| Port 9030 already in use | Another process using port 9030 | Change `9030:9030` to `19030:9030` in docker-compose.yml |
| Container exits immediately | Insufficient Docker memory | Increase Docker Desktop memory to 8 GB in Settings |
| Health check timeout | Slow first build | Wait 2 minutes, then `docker compose ps` again |
| `Access denied for user 'root'` | Wrong port (connected to wrong QN) | Confirm port 9030 is qn-1 |
| `ERROR 1235: unsupported SQL` | UPDATE/DELETE not supported | WOW-DB is append-only; use INSERT for new data |
| Nodes not showing ONLINE in dashboard | Cluster still starting | Wait 45 seconds for health checks to pass |
| `Connection refused` on port 9030 | qn-1 not started | Run `docker compose --profile full up -d qn-1` |

---

## 9. Stop and Clean Up

```bash
# Stop without deleting data
docker compose -f docker/docker-compose.yml down

# Stop and delete all data volumes
docker compose -f docker/docker-compose.yml down -v
```

---

## Next Steps

- **Advanced Guide** → `docs/advanced.md` — cluster configuration, DDL reference, tuning
- **Sample Datasets** → `samples/README.md` — TPC-H and web analytics examples
- **Performance Report** → `docs/performance-report.md` — benchmark results
