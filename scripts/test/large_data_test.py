#!/usr/bin/env python3
"""
WOW-DB Large Data Test
Loads SF=0.1 TPC-H data and verifies row counts and performance.
"""
import subprocess, time, sys, json, os

try:
    import pymysql
except ImportError:
    print("Install: pip install pymysql")
    sys.exit(1)

HOST = "127.0.0.1"
PORT = 9030
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BENCH_PY = os.path.join(os.path.dirname(SCRIPT_DIR), "tpch", "bench.py")

results = []

def emit_result(name, status, actual, expected, duration_ms, message=""):
    r = {"test_name": name, "status": status, "duration_ms": int(duration_ms),
         "actual": actual, "expected": expected,
         "message": message or f"actual={actual} expected={expected}"}
    results.append(r)
    sym = "✅ PASS" if status == "PASS" else "❌ FAIL"
    print(f"  {sym}: {name} — {r['message']}")

print(f"\n{'='*60}")
print("  WOW-DB Large Data Test (SF=0.1)")
print(f"{'='*60}\n")

# Expected row counts at SF=0.1
SF01_EXPECTED = {
    "lineitem": 600_000,
    "orders":   150_000,
    "customer":  15_000,
    "part":      20_000,
    "partsupp":  80_000,
    "supplier":   1_000,
    "nation":        25,
    "region":         5,
}

# Run bench.py with SF=0.1
print("[TEST] Loading SF=0.1 and running Q01,Q03,Q06,Q10,Q14...")
t0 = time.perf_counter()
report_file = "/tmp/large_data_report.json"

r = subprocess.run(
    [sys.executable, BENCH_PY,
     "--host", HOST, "--port", str(PORT),
     "--sf", "0.1",
     "--queries", "Q01,Q03,Q06,Q10,Q14",
     "--output", "json", "--report", report_file],
    capture_output=True, text=True, timeout=600
)
elapsed_ms = (time.perf_counter() - t0) * 1000
print(r.stdout[-2000:] if len(r.stdout) > 2000 else r.stdout)
if r.stderr:
    print("[STDERR]", r.stderr[-500:])

# Parse report
load_ok = False
if os.path.exists(report_file):
    with open(report_file) as f:
        report = json.load(f)
    pass_count = report.get("summary", {}).get("pass", 0)
    fail_count = report.get("summary", {}).get("fail", 0)
    load_ms = report.get("load_duration_ms", 0)
    status = "PASS" if pass_count >= 5 and fail_count == 0 else "FAIL"
    emit_result("sf01_queries", status,
                {"pass": pass_count, "fail": fail_count},
                {"pass": 5, "fail": 0},
                elapsed_ms,
                f"pass={pass_count} fail={fail_count} load={load_ms/1000:.1f}s")
    load_ok = True
else:
    emit_result("sf01_queries", "FAIL", {}, {}, elapsed_ms, f"bench.py failed: {r.returncode}")

# Check docker stats for memory
print("\n[TEST] Docker memory usage...")
mem_r = subprocess.run(
    ["docker", "stats", "--no-stream", "--format",
     "{{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}"],
    capture_output=True, text=True, timeout=30
)
if mem_r.returncode == 0:
    print(mem_r.stdout)
    oom = any("OOM" in line for line in mem_r.stdout.splitlines())
    emit_result("memory_usage", "PASS" if not oom else "FAIL",
                mem_r.stdout, "no OOM", 0,
                "OOM detected" if oom else "No OOM")

# Verify lineitem row count at SF=0.1
print("\n[TEST] Verifying row counts...")
t0 = time.perf_counter()
conn = pymysql.connect(host=HOST, port=PORT, user="root", database="tpch",
                       autocommit=True, connect_timeout=30)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM lineitem")
actual_lineitem = cur.fetchone()[0]
cur.close()
conn.close()
expected_lineitem = SF01_EXPECTED["lineitem"]
status = "PASS" if abs(actual_lineitem - expected_lineitem) < expected_lineitem * 0.05 else "FAIL"
emit_result("lineitem_row_count", status,
            actual_lineitem, expected_lineitem,
            (time.perf_counter() - t0) * 1000,
            f"actual={actual_lineitem:,} expected≈{expected_lineitem:,}")

# Summary
print(f"\n{'='*60}")
passed = sum(1 for r in results if r["status"] == "PASS")
print(f"  Results: {passed}/{len(results)} PASS")
print(f"{'='*60}\n")
print(json.dumps(results, indent=2))
