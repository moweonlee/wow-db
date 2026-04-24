#!/usr/bin/env python3
"""
WOW-DB Persistence Test
Tests that data survives graceful restart (SIGTERM) and hard kill (SIGKILL).
"""
import subprocess, time, sys, json
from datetime import datetime

try:
    import pymysql
except ImportError:
    print("Install: pip install pymysql")
    sys.exit(1)

HOST = "127.0.0.1"
PORT = 9030
DB   = "persistence_test"
TABLE = "persist_check"
TEST_ROWS = 1000
COMPOSE_FILE = "docker/docker-compose.yml"

results = []

def connect():
    return pymysql.connect(host=HOST, port=PORT, user="root", database=DB,
                           autocommit=True, connect_timeout=30)

def run_cmd(cmd, check=False):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}\n{r.stderr}")
    return r

def wait_healthy(service="sn-1", timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = run_cmd(f'docker inspect --format="{{{{.State.Health.Status}}}}" wowdb-{service}')
        if "healthy" in r.stdout.lower():
            return True
        time.sleep(3)
    return False

def setup():
    conn = connect()
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")
    cur.execute(f"USE {DB}")
    cur.execute(f"DROP CUBE IF EXISTS {TABLE}")
    cur.execute(f"CREATE CUBE IF NOT EXISTS {TABLE} (id INT NOT NULL, val VARCHAR(32))")
    for i in range(0, TEST_ROWS, 100):
        vals = ", ".join(f"({j}, 'data_{j}')" for j in range(i, min(i+100, TEST_ROWS)))
        cur.execute(f"INSERT INTO {TABLE} VALUES {vals}")
    cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count

def check_count():
    time.sleep(5)
    for attempt in range(10):
        try:
            conn = connect()
            cur = conn.cursor()
            cur.execute(f"USE {DB}")
            cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
            count = cur.fetchone()[0]
            cur.close()
            conn.close()
            return count
        except Exception as e:
            if attempt < 9:
                time.sleep(5)
            else:
                raise

def emit_result(name, status, actual, expected, duration_ms, message=""):
    r = {
        "test_name": name,
        "status": status,
        "duration_ms": int(duration_ms),
        "actual": actual,
        "expected": expected,
        "message": message or f"actual={actual} expected={expected}",
    }
    results.append(r)
    sym = "✅ PASS" if status == "PASS" else "❌ FAIL"
    print(f"  {sym}: {name} — {r['message']}")
    return r

print(f"\n{'='*60}")
print("  WOW-DB Persistence Test")
print(f"{'='*60}\n")

# Test 1: Graceful restart
print("[TEST 1] Graceful restart (SIGTERM)")
t0 = time.perf_counter()
try:
    written = setup()
    print(f"  Inserted {written} rows")

    run_cmd(f"docker compose -f {COMPOSE_FILE} restart sn-1 sn-2 sn-3", check=True)
    print("  Waiting for SNs to become healthy...")
    for sn in ["sn-1", "sn-2", "sn-3"]:
        ok = wait_healthy(sn)
        print(f"  {sn}: {'healthy' if ok else 'TIMEOUT'}")

    after = check_count()
    status = "PASS" if after == TEST_ROWS else "FAIL"
    emit_result("graceful_restart", status, after, TEST_ROWS,
                (time.perf_counter() - t0) * 1000)
except Exception as e:
    emit_result("graceful_restart", "FAIL", 0, TEST_ROWS,
                (time.perf_counter() - t0) * 1000, str(e))

# Test 2: Hard kill
print("\n[TEST 2] Hard kill (SIGKILL)")
t0 = time.perf_counter()
try:
    written = setup()
    print(f"  Inserted {written} rows")

    run_cmd(f"docker compose -f {COMPOSE_FILE} kill sn-1 && docker compose -f {COMPOSE_FILE} up -d sn-1", check=False)
    time.sleep(5)
    run_cmd(f"docker compose -f {COMPOSE_FILE} up -d sn-1")
    print("  Waiting for sn-1 to become healthy...")
    ok = wait_healthy("sn-1")
    print(f"  sn-1: {'healthy' if ok else 'TIMEOUT'}")

    after = check_count()
    status = "PASS" if after >= TEST_ROWS * 0.99 else "FAIL"
    emit_result("hard_kill_restart", status, after, TEST_ROWS,
                (time.perf_counter() - t0) * 1000)
except Exception as e:
    emit_result("hard_kill_restart", "FAIL", 0, TEST_ROWS,
                (time.perf_counter() - t0) * 1000, str(e))

# Summary
print(f"\n{'='*60}")
passed = sum(1 for r in results if r["status"] == "PASS")
print(f"  Results: {passed}/{len(results)} PASS")
print(f"{'='*60}\n")
print(json.dumps(results, indent=2))
