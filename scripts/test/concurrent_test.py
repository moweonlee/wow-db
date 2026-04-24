#!/usr/bin/env python3
"""
WOW-DB Concurrent Read/Write Test
Spawns writer + reader threads simultaneously and verifies no read errors occur.
"""
import threading, time, sys, json

try:
    import pymysql
except ImportError:
    print("Install: pip install pymysql")
    sys.exit(1)

HOST = "127.0.0.1"
PORT = 9030
DB   = "concurrent_test"
TABLE = "conc_check"
WRITE_DURATION_S = 10
READ_INTERVAL_MS = 100
NUM_READERS = 5

results = []
write_count = 0
read_errors = 0
read_counts = []
writer_error = None
lock = threading.Lock()

def make_conn():
    return pymysql.connect(host=HOST, port=PORT, user="root", database=DB,
                           autocommit=True, connect_timeout=10)

def writer_thread():
    global write_count, writer_error
    try:
        conn = make_conn()
        cur = conn.cursor()
        seq = 0
        deadline = time.time() + WRITE_DURATION_S
        while time.time() < deadline:
            cur.execute(f"INSERT INTO {TABLE} VALUES ({seq}, 'val_{seq}')")
            with lock:
                write_count += 1
            seq += 1
            time.sleep(0.001)
        cur.close()
        conn.close()
    except Exception as e:
        writer_error = str(e)

def reader_thread(reader_id):
    global read_errors
    try:
        conn = make_conn()
        cur = conn.cursor()
        deadline = time.time() + WRITE_DURATION_S + 2
        while time.time() < deadline:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
                cnt = cur.fetchone()[0]
                with lock:
                    read_counts.append(cnt)
            except Exception as e:
                with lock:
                    read_errors += 1
            time.sleep(READ_INTERVAL_MS / 1000.0)
        cur.close()
        conn.close()
    except Exception as e:
        with lock:
            read_errors += 1

print(f"\n{'='*60}")
print("  WOW-DB Concurrent Read/Write Test")
print(f"  Writers: 1  Readers: {NUM_READERS}  Duration: {WRITE_DURATION_S}s")
print(f"{'='*60}\n")

# Setup
conn = make_conn()
cur = conn.cursor()
cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")
cur.execute(f"USE {DB}")
cur.execute(f"DROP CUBE IF EXISTS {TABLE}")
cur.execute(f"CREATE CUBE IF NOT EXISTS {TABLE} (id INT NOT NULL, val VARCHAR(32))")
cur.close()
conn.close()
print("[SETUP] Table ready")

t0 = time.perf_counter()

# Start threads
threads = []
w = threading.Thread(target=writer_thread)
threads.append(w)
for i in range(NUM_READERS):
    r = threading.Thread(target=reader_thread, args=(i,))
    threads.append(r)

for t in threads:
    t.start()
for t in threads:
    t.join()

elapsed_ms = (time.perf_counter() - t0) * 1000

# Verify final count
time.sleep(2)
conn = make_conn()
cur = conn.cursor()
cur.execute(f"USE {DB}")
cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
final_count = cur.fetchone()[0]
cur.close()
conn.close()

print(f"\n[RESULTS]")
print(f"  Rows written: {write_count}")
print(f"  Final count:  {final_count}")
print(f"  Read errors:  {read_errors}")
print(f"  Read samples: {len(read_counts)}")
if read_counts:
    print(f"  Min reads during write: {min(read_counts)}")
    print(f"  Max reads during write: {max(read_counts)}")

if writer_error:
    print(f"  Writer error: {writer_error}")

count_match = final_count >= write_count * 0.99
no_errors = read_errors == 0
status = "PASS" if count_match and no_errors else "FAIL"

r = {
    "test_name": "concurrent_read_write",
    "status": status,
    "duration_ms": int(elapsed_ms),
    "actual": {"write_count": write_count, "final_count": final_count, "read_errors": read_errors},
    "expected": {"final_count_min": write_count, "read_errors": 0},
    "message": f"writes={write_count} final={final_count} errors={read_errors}",
}
results.append(r)

print(f"\n{'='*60}")
sym = "✅ PASS" if status == "PASS" else "❌ FAIL"
print(f"  {sym}: concurrent_read_write")
print(f"{'='*60}\n")
print(json.dumps(results, indent=2))
