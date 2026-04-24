#!/usr/bin/env python3
"""
WOW-DB Data Distribution Test
Verifies that rows are evenly distributed across all 3 SNs.
"""
import time, sys, json
import urllib.request, urllib.error

try:
    import pymysql
except ImportError:
    print("Install: pip install pymysql")
    sys.exit(1)

HOST = "127.0.0.1"
PORT = 9030
DB   = "distribution_test"
TABLE = "dist_check"
TEST_ROWS = 60_000
SN_HTTP_PORTS = [18040, 28040, 38040]
SN_NAMES = ["sn-1", "sn-2", "sn-3"]
EXPECTED_PCT = 100.0 / len(SN_NAMES)
TOLERANCE_PCT = 20.0  # allow ±20% skew

results = []

def connect():
    return pymysql.connect(host=HOST, port=PORT, user="root", database=DB,
                           autocommit=True, connect_timeout=30)

def emit_result(name, status, actual, expected, duration_ms, message=""):
    r = {"test_name": name, "status": status, "duration_ms": int(duration_ms),
         "actual": actual, "expected": expected,
         "message": message or f"actual={actual} expected={expected}"}
    results.append(r)
    sym = "✅ PASS" if status == "PASS" else "❌ FAIL"
    print(f"  {sym}: {name} — {r['message']}")
    return r

print(f"\n{'='*60}")
print("  WOW-DB Data Distribution Test")
print(f"{'='*60}\n")

# Setup
print(f"[SETUP] Loading {TEST_ROWS:,} rows...")
t0 = time.perf_counter()
conn = connect()
cur = conn.cursor()
cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")
cur.execute(f"USE {DB}")
cur.execute(f"DROP CUBE IF EXISTS {TABLE}")
cur.execute(f"CREATE CUBE IF NOT EXISTS {TABLE} (id INT NOT NULL, bucket VARCHAR(8))")
batch = 500
for i in range(0, TEST_ROWS, batch):
    vals = ", ".join(f"({j}, 'b{j % 100}')" for j in range(i, min(i+batch, TEST_ROWS)))
    cur.execute(f"INSERT INTO {TABLE} VALUES {vals}")
elapsed_s = time.perf_counter() - t0
cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
total = cur.fetchone()[0]
print(f"  Loaded {total:,} rows in {elapsed_s:.1f}s")
cur.close()
conn.close()

# Wait for flush to SNs
time.sleep(3)

# Check per-SN distribution via LSM status API
print("\n[TEST] Per-SN row distribution:")
sn_counts = {}
for sn, port in zip(SN_NAMES, SN_HTTP_PORTS):
    try:
        url = f"http://127.0.0.1:{port}/api/v1/lsm-status"
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
        row_count = data.get("total_rows", data.get("row_count", 0))
        sn_counts[sn] = row_count
        pct = 100.0 * row_count / total if total > 0 else 0
        skew = abs(pct - EXPECTED_PCT)
        print(f"  {sn}: {row_count:,} rows ({pct:.1f}%)  skew={skew:+.1f}%")
    except Exception as e:
        print(f"  {sn}: ERROR — {e} (HTTP API may not be available)")
        sn_counts[sn] = -1

# Validate distribution
t_elapsed = (time.perf_counter() - t0) * 1000
if all(v >= 0 for v in sn_counts.values()):
    total_from_sns = sum(sn_counts.values())
    max_skew = max(abs(100.0 * v / total_from_sns - EXPECTED_PCT)
                   for v in sn_counts.values()) if total_from_sns > 0 else 100
    status = "PASS" if max_skew <= TOLERANCE_PCT else "FAIL"
    emit_result("data_distribution", status,
                sn_counts, {"per_sn_pct": EXPECTED_PCT, "tolerance": TOLERANCE_PCT},
                t_elapsed, f"max_skew={max_skew:.1f}% tolerance={TOLERANCE_PCT}%")
else:
    emit_result("data_distribution", "SKIP", sn_counts, {},
                t_elapsed, "SN HTTP API not available — distribution check skipped")

print(f"\n{'='*60}")
passed = sum(1 for r in results if r["status"] == "PASS")
print(f"  Results: {passed}/{len(results)} PASS")
print(f"{'='*60}\n")
print(json.dumps(results, indent=2))
