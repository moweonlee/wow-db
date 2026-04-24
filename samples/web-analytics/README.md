# WOW-DB Web Analytics Sample Dataset

This sample demonstrates WOW-DB's dual-layout architecture with 10,000 synthetic web events across 500 sessions.

## Dataset

| Table | Rows | Description |
|-------|------|-------------|
| events | ~10,000 | Raw event stream (pageview, click, scroll, purchase) |
| sessions | 500 | Pre-aggregated session summaries |

**Distributions**: 5 countries (KR 40%, US 25%, JP 15%, DE 10%, GB 10%), 3 device types (desktop 45%, mobile 45%, tablet 10%), 4 browsers, 10 page URLs.

## Load the Dataset

```bash
# Start the cluster first
docker compose -f docker/docker-compose.yml --profile full up -d

# Load schema
mysql -h 127.0.0.1 -P 9030 -u root < samples/web-analytics/schema.sql

# Generate and load data
python samples/web-analytics/generate.py
```

## Queries

| File | Description | Complexity |
|------|-------------|------------|
| 01-session-summary.sql | Session counts and avg duration by country | Basic |
| 02-top-pages.sql | Top 10 pages by pageview count | Basic |
| 03-funnel-analysis.sql | Entry-to-exit page funnel analysis | Intermediate |
| 04-user-retention.sql | Returning users by country | Intermediate |
| 05-behavioral-routing-demo.sql | Behavioral routing demonstration | Advanced |

## Run Queries

```bash
mysql -h 127.0.0.1 -P 9030 -u root web_analytics < samples/web-analytics/queries/01-session-summary.sql
```

## Behavioral Routing

Query 05 demonstrates WOW-DB's unique behavioral routing feature:
- Query references `session_id` column on the `events` table
- WOW-DB detects this and transparently redirects to the `sessions` pre-aggregated table
- Result is served from the fast session BT instead of scanning all raw events

See `docs/advanced.md` → Behavioral Routing for details.
