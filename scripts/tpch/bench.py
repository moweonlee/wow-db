#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
"""
WOW-DB TPC-H Benchmark (SF=0.01 → correctness, SF=0.1 → performance)

Usage:
  python bench.py                    # SF=0.01 quick test
  python bench.py --sf 0.1           # SF=0.1 perf test
  python bench.py --no-load          # skip data load (tables already loaded)
  python bench.py --query 1          # run single query
  python bench.py --port 19030       # QN MySQL port
"""

import pymysql
import random
import time
import sys
import argparse
import math
from datetime import date, timedelta
from collections import defaultdict

# ─── CLI Args ────────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser(description="WOW-DB TPC-H Benchmark")
parser.add_argument("--host",    default="127.0.0.1")
parser.add_argument("--port",    type=int, default=19030)
parser.add_argument("--user",    default="root")
parser.add_argument("--db",      default="tpch")
parser.add_argument("--sf",      type=float, default=0.01, help="Scale factor (0.01 | 0.1 | 1)")
parser.add_argument("--no-load", action="store_true", help="Skip table creation & data load")
parser.add_argument("--query",   type=int, default=0, help="Run single query number (0=all)")
parser.add_argument("--timeout", type=int, default=120, help="Query timeout seconds")
args = parser.parse_args()

SF = args.sf

# Row counts per SF=1 (TPC-H spec)
N_REGION    = 5
N_NATION    = 25
N_SUPPLIER  = int(10_000 * SF)
N_CUSTOMER  = int(150_000 * SF)
N_PART      = int(200_000 * SF)
N_PARTSUPP  = N_PART * 4
N_ORDERS    = int(1_500_000 * SF)
N_LINEITEM  = int(6_000_000 * SF)

print(f"\n{'='*60}")
print(f"  WOW-DB TPC-H Benchmark  |  SF={SF}")
print(f"  Target: {args.host}:{args.port}")
print(f"  Row counts: lineitem~{N_LINEITEM:,}  orders~{N_ORDERS:,}  customer~{N_CUSTOMER:,}")
print(f"{'='*60}\n")

# ─── DB Connection ────────────────────────────────────────────────────────────

def connect():
    return pymysql.connect(
        host=args.host, port=args.port, user=args.user,
        database="", charset="utf8mb4",
        autocommit=True,
        read_timeout=args.timeout, write_timeout=args.timeout,
        connect_timeout=10,
    )

def run_sql(cur, sql, label=""):
    t0 = time.perf_counter()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed = (time.perf_counter() - t0) * 1000
        return rows, elapsed, None
    except Exception as e:
        elapsed = (time.perf_counter() - t0) * 1000
        return None, elapsed, str(e)

# ─── Random Data Helpers ──────────────────────────────────────────────────────

random.seed(42)

REGIONS = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
NATIONS = [
    ("ALGERIA",0),("ARGENTINA",1),("BRAZIL",1),("CANADA",1),("EGYPT",4),
    ("ETHIOPIA",0),("FRANCE",3),("GERMANY",3),("INDIA",2),("INDONESIA",2),
    ("IRAN",4),("IRAQ",4),("JAPAN",2),("JORDAN",4),("KENYA",0),
    ("MOROCCO",0),("MOZAMBIQUE",0),("PERU",1),("CHINA",2),("ROMANIA",3),
    ("SAUDI ARABIA",4),("VIETNAM",2),("RUSSIA",3),("UNITED KINGDOM",3),("UNITED STATES",1),
]
SEGMENTS = ["AUTOMOBILE","BUILDING","FURNITURE","MACHINERY","HOUSEHOLD"]
PRIORITIES = ["1-URGENT","2-HIGH","3-MEDIUM","4-NOT SPECIFIED","5-LOW"]
SHIPMODES = ["AIR","FOB","MAIL","RAIL","REG AIR","SHIP","TRUCK"]
SHIPINSTRUCT = ["DELIVER IN PERSON","COLLECT COD","NONE","TAKE BACK RETURN"]
RETURNFLAGS = ["N","R","A"]
LINESTATUS = ["O","F"]
BRANDS = [f"Brand#{i}{j}" for i in range(1,6) for j in range(1,6)]
CONTAINERS = ["SM BOX","SM CASE","SM PKG","SM PACK","MED BAG","MED BOX","MED PKG","MED PACK","LG BOX","LG CASE","LG PKG","LG PACK","JUMBO BAG","JUMBO BOX","JUMBO PKG","JUMBO PACK","WRAP BOX","WRAP CASE","WRAP PKG","WRAP PACK"]
MFGRS = [f"Manufacturer#{i}" for i in range(1,6)]
P_TYPES_PREFIX = ["STANDARD","SMALL","MEDIUM","LARGE","ECONOMY","PROMO"]
P_TYPES_SUFFIX = ["ANODIZED STEEL","ANODIZED BRASS","ANODIZED NICKEL","ANODIZED ALUMINUM","ANODIZED COPPER","POLISHED STEEL","POLISHED BRASS","POLISHED NICKEL","POLISHED ALUMINUM","POLISHED COPPER","BRUSHED STEEL","BRUSHED BRASS","BRUSHED NICKEL","BRUSHED ALUMINUM","BRUSHED COPPER","PLATED STEEL","PLATED BRASS","PLATED NICKEL","PLATED ALUMINUM","PLATED COPPER","BURNISHED STEEL","BURNISHED BRASS","BURNISHED NICKEL","BURNISHED ALUMINUM","BURNISHED COPPER"]

BASE_DATE = date(1992, 1, 1)
END_DATE  = date(1998, 12, 31)
DATE_RANGE = (END_DATE - BASE_DATE).days

def rand_date(start_offset=0, end_offset=DATE_RANGE):
    return BASE_DATE + timedelta(days=random.randint(start_offset, end_offset))

def rand_str(length):
    return ''.join(random.choices("abcdefghijklmnopqrstuvwxyz ", k=length))[:length]

def fmt_date(d):
    return d.strftime("%Y-%m-%d")

def esc(s):
    return str(s).replace("'", "''")

# ─── Schema DDL ──────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS region (
    r_regionkey  INT NOT NULL,
    r_name       VARCHAR(25) NOT NULL,
    r_comment    VARCHAR(152)
);

CREATE TABLE IF NOT EXISTS nation (
    n_nationkey  INT NOT NULL,
    n_name       VARCHAR(25) NOT NULL,
    n_regionkey  INT NOT NULL,
    n_comment    VARCHAR(152)
);

CREATE TABLE IF NOT EXISTS supplier (
    s_suppkey    INT NOT NULL,
    s_name       VARCHAR(25) NOT NULL,
    s_address    VARCHAR(40) NOT NULL,
    s_nationkey  INT NOT NULL,
    s_phone      VARCHAR(15) NOT NULL,
    s_acctbal    DOUBLE NOT NULL,
    s_comment    VARCHAR(101)
);

CREATE TABLE IF NOT EXISTS customer (
    c_custkey    INT NOT NULL,
    c_name       VARCHAR(25) NOT NULL,
    c_address    VARCHAR(40) NOT NULL,
    c_nationkey  INT NOT NULL,
    c_phone      VARCHAR(15) NOT NULL,
    c_acctbal    DOUBLE NOT NULL,
    c_mktsegment VARCHAR(10) NOT NULL,
    c_comment    VARCHAR(117)
);

CREATE TABLE IF NOT EXISTS part (
    p_partkey     INT NOT NULL,
    p_name        VARCHAR(55) NOT NULL,
    p_mfgr        VARCHAR(25) NOT NULL,
    p_brand       VARCHAR(10) NOT NULL,
    p_type        VARCHAR(25) NOT NULL,
    p_size        INT NOT NULL,
    p_container   VARCHAR(10) NOT NULL,
    p_retailprice DOUBLE NOT NULL,
    p_comment     VARCHAR(23)
);

CREATE TABLE IF NOT EXISTS partsupp (
    ps_partkey    INT NOT NULL,
    ps_suppkey    INT NOT NULL,
    ps_availqty   INT NOT NULL,
    ps_supplycost DOUBLE NOT NULL,
    ps_comment    VARCHAR(199)
);

CREATE TABLE IF NOT EXISTS orders (
    o_orderkey      INT NOT NULL,
    o_custkey       INT NOT NULL,
    o_orderstatus   VARCHAR(1) NOT NULL,
    o_totalprice    DOUBLE NOT NULL,
    o_orderdate     VARCHAR(10) NOT NULL,
    o_orderpriority VARCHAR(15) NOT NULL,
    o_clerk         VARCHAR(15) NOT NULL,
    o_shippriority  INT NOT NULL,
    o_comment       VARCHAR(79)
);

CREATE TABLE IF NOT EXISTS lineitem (
    l_orderkey      INT NOT NULL,
    l_partkey       INT NOT NULL,
    l_suppkey       INT NOT NULL,
    l_linenumber    INT NOT NULL,
    l_quantity      DOUBLE NOT NULL,
    l_extendedprice DOUBLE NOT NULL,
    l_discount      DOUBLE NOT NULL,
    l_tax           DOUBLE NOT NULL,
    l_returnflag    VARCHAR(1) NOT NULL,
    l_linestatus    VARCHAR(1) NOT NULL,
    l_shipdate      VARCHAR(10) NOT NULL,
    l_commitdate    VARCHAR(10) NOT NULL,
    l_receiptdate   VARCHAR(10) NOT NULL,
    l_shipinstruct  VARCHAR(25) NOT NULL,
    l_shipmode      VARCHAR(10) NOT NULL,
    l_comment       VARCHAR(44)
);
"""

DROP_SQL = [
    "DROP TABLE IF EXISTS lineitem",
    "DROP TABLE IF EXISTS orders",
    "DROP TABLE IF EXISTS partsupp",
    "DROP TABLE IF EXISTS part",
    "DROP TABLE IF EXISTS customer",
    "DROP TABLE IF EXISTS supplier",
    "DROP TABLE IF EXISTS nation",
    "DROP TABLE IF EXISTS region",
]

# ─── Data Generators ─────────────────────────────────────────────────────────

def gen_region():
    return [(i, REGIONS[i], rand_str(40)) for i in range(N_REGION)]

def gen_nation():
    return [(i, NATIONS[i][0], NATIONS[i][1], rand_str(80)) for i in range(N_NATION)]

def gen_supplier(n_sup, n_nat):
    rows = []
    for i in range(1, n_sup+1):
        rows.append((i, f"Supplier#{i:09d}", rand_str(25), random.randint(0,n_nat-1),
                     f"{random.randint(10,99)}-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
                     round(random.uniform(-999.99, 9999.99), 2), rand_str(63)))
    return rows

def gen_customer(n_cust, n_nat):
    rows = []
    for i in range(1, n_cust+1):
        rows.append((i, f"Customer#{i:09d}", rand_str(25), random.randint(0,n_nat-1),
                     f"{random.randint(10,99)}-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
                     round(random.uniform(-999.99, 9999.99), 2),
                     random.choice(SEGMENTS), rand_str(73)))
    return rows

def gen_part(n_part):
    rows = []
    for i in range(1, n_part+1):
        price = round(90000 + (i/10) + (i % 1000) * 0.01, 2)
        rows.append((i, rand_str(20), random.choice(MFGRS), random.choice(BRANDS),
                     f"{random.choice(P_TYPES_PREFIX)} {random.choice(P_TYPES_SUFFIX)}",
                     random.randint(1,50), random.choice(CONTAINERS),
                     price, rand_str(14)))
    return rows

def gen_partsupp(n_part, n_sup):
    rows = []
    for pk in range(1, n_part+1):
        for s in range(4):
            sk = ((pk + (s * (n_sup // 4 + 1))) % n_sup) + 1
            rows.append((pk, sk, random.randint(1,9999),
                         round(random.uniform(1.0, 1000.0), 2), rand_str(124)))
    return rows

def gen_orders(n_ord, n_cust):
    rows = []
    for i in range(1, n_ord+1):
        odate = rand_date(0, DATE_RANGE - 151)
        total = round(random.uniform(100.0, 500000.0), 2)
        status = "F" if odate < date(1995, 6, 17) else "O"
        rows.append((i, random.randint(1, n_cust), status, total, fmt_date(odate),
                     random.choice(PRIORITIES),
                     f"Clerk#{random.randint(1,1000):09d}", 0, rand_str(44)))
    return rows

def gen_lineitem(n_ord, n_part, n_sup):
    rows = []
    for ok in range(1, n_ord+1):
        n_lines = random.randint(1, 7)
        odate = BASE_DATE + timedelta(days=random.randint(0, DATE_RANGE - 151))
        for ln in range(1, n_lines+1):
            pk = random.randint(1, n_part)
            sk = ((pk + (random.randint(0,3) * (n_sup//4+1))) % n_sup) + 1
            qty = random.randint(1, 50)
            price = round(90000 + (pk/10) + (pk % 1000) * 0.01, 2)
            disc  = round(random.choice([0,0.01,0.02,0.03,0.04,0.05,0.06,0.07,0.08,0.09,0.10]), 2)
            tax   = round(random.choice([0,0.02,0.04,0.06,0.08]), 2)
            sdate = odate + timedelta(days=random.randint(1,121))
            cdate = odate + timedelta(days=random.randint(30,90))
            rdate = sdate + timedelta(days=random.randint(1,30))
            rf = "N" if sdate > date(1995,6,17) else random.choice(["R","A"])
            ls = "O" if sdate > date(1995,6,17) else "F"
            rows.append((ok, pk, sk, ln, float(qty), round(price*qty, 2), disc, tax,
                         rf, ls, fmt_date(sdate), fmt_date(cdate), fmt_date(rdate),
                         random.choice(SHIPINSTRUCT), random.choice(SHIPMODES), rand_str(27)))
    return rows

# ─── Bulk Insert ──────────────────────────────────────────────────────────────

def bulk_insert(cur, table, cols, rows, batch=200):
    if not rows:
        return
    col_str = ", ".join(cols)
    total = 0
    for i in range(0, len(rows), batch):
        chunk = rows[i:i+batch]
        vals = []
        for r in chunk:
            v = ", ".join(f"'{esc(x)}'" if isinstance(x,str) else str(x) for x in r)
            vals.append(f"({v})")
        sql = f"INSERT INTO {table} ({col_str}) VALUES {', '.join(vals)}"
        cur.execute(sql)
        total += len(chunk)
    return total

# ─── TPC-H Queries ───────────────────────────────────────────────────────────

QUERIES = {
    1: ("Pricing Summary Report",
        """SELECT
            l_returnflag, l_linestatus,
            SUM(l_quantity) AS sum_qty,
            SUM(l_extendedprice) AS sum_base_price,
            SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
            SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
            COUNT(*) AS count_order
        FROM lineitem
        WHERE l_shipdate <= '1998-09-02'
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus"""),

    6: ("Forecasting Revenue Change",
        """SELECT SUM(l_extendedprice * l_discount) AS revenue
        FROM lineitem
        WHERE l_shipdate >= '1994-01-01'
          AND l_shipdate < '1995-01-01'
          AND l_discount >= 0.05
          AND l_discount <= 0.07
          AND l_quantity < 24"""),

    3: ("Shipping Priority",
        """SELECT
            l_orderkey,
            SUM(l_extendedprice * (1 - l_discount)) AS revenue,
            o_orderdate, o_shippriority
        FROM customer, orders, lineitem
        WHERE c_mktsegment = 'BUILDING'
          AND c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND o_orderdate < '1995-03-15'
          AND l_shipdate > '1995-03-15'
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue DESC
        LIMIT 10"""),

    5: ("Local Supplier Volume",
        """SELECT
            n_name,
            SUM(l_extendedprice * (1 - l_discount)) AS revenue
        FROM customer, orders, lineitem, supplier, nation, region
        WHERE c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND l_suppkey = s_suppkey
          AND c_nationkey = s_nationkey
          AND s_nationkey = n_nationkey
          AND n_regionkey = r_regionkey
          AND r_name = 'ASIA'
          AND o_orderdate >= '1994-01-01'
          AND o_orderdate < '1995-01-01'
        GROUP BY n_name
        ORDER BY revenue DESC"""),

    10: ("Returned Item Reporting",
        """SELECT
            c_custkey, c_name,
            SUM(l_extendedprice * (1 - l_discount)) AS revenue,
            c_acctbal, n_name, c_address, c_phone, c_comment
        FROM customer, orders, lineitem, nation
        WHERE c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND o_orderdate >= '1993-10-01'
          AND o_orderdate < '1994-01-01'
          AND l_returnflag = 'R'
          AND c_nationkey = n_nationkey
        GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
        ORDER BY revenue DESC
        LIMIT 20"""),

    14: ("Promotion Effect",
        """SELECT
            100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
                / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
        FROM lineitem, part
        WHERE l_partkey = p_partkey
          AND l_shipdate >= '1995-09-01'
          AND l_shipdate < '1995-10-01'"""),
}

# ─── Main ─────────────────────────────────────────────────────────────────────

results = {}

try:
    conn = connect()
    cur = conn.cursor()
    print(f"[OK] Connected to WOW-DB at {args.host}:{args.port}\n")
except Exception as e:
    print(f"[FAIL] Cannot connect: {e}")
    sys.exit(1)

# ── Setup database ────────────────────────────────────────────────────────────
if not args.no_load:
    print("[SETUP] Creating database...")
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {args.db}")
    except Exception as e:
        print(f"  WARN: CREATE DATABASE: {e}")
    try:
        cur.execute(f"USE {args.db}")
    except Exception as e:
        print(f"  WARN: USE {args.db}: {e}")

    print("[SETUP] Dropping existing tables...")
    for sql in DROP_SQL:
        try:
            cur.execute(sql)
        except Exception as e:
            print(f"  WARN: {sql}: {e}")

    print("[SETUP] Creating TPC-H schema...")
    for stmt in SCHEMA_SQL.strip().split(";"):
        stmt = stmt.strip()
        if not stmt:
            continue
        try:
            cur.execute(stmt)
            print(f"  OK: {stmt.split()[2]}")
        except Exception as e:
            print(f"  ERR: {e}  SQL: {stmt[:60]}")

    print()

    # ── Data generation & load ────────────────────────────────────────────────
    tables_data = [
        ("region",   ["r_regionkey","r_name","r_comment"],             gen_region()),
        ("nation",   ["n_nationkey","n_name","n_regionkey","n_comment"], gen_nation()),
        ("supplier", ["s_suppkey","s_name","s_address","s_nationkey","s_phone","s_acctbal","s_comment"],
                     gen_supplier(N_SUPPLIER, N_NATION)),
        ("customer", ["c_custkey","c_name","c_address","c_nationkey","c_phone","c_acctbal","c_mktsegment","c_comment"],
                     gen_customer(N_CUSTOMER, N_NATION)),
        ("part",     ["p_partkey","p_name","p_mfgr","p_brand","p_type","p_size","p_container","p_retailprice","p_comment"],
                     gen_part(N_PART)),
        ("partsupp", ["ps_partkey","ps_suppkey","ps_availqty","ps_supplycost","ps_comment"],
                     gen_partsupp(N_PART, N_SUPPLIER)),
        ("orders",   ["o_orderkey","o_custkey","o_orderstatus","o_totalprice","o_orderdate",
                      "o_orderpriority","o_clerk","o_shippriority","o_comment"],
                     gen_orders(N_ORDERS, N_CUSTOMER)),
        ("lineitem", ["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice",
                      "l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate",
                      "l_receiptdate","l_shipinstruct","l_shipmode","l_comment"],
                     gen_lineitem(N_ORDERS, N_PART, N_SUPPLIER)),
    ]

    for tbl, cols, rows in tables_data:
        t0 = time.perf_counter()
        print(f"[LOAD] {tbl:10s}  {len(rows):>7,} rows ... ", end="", flush=True)
        try:
            bulk_insert(cur, tbl, cols, rows)
            elapsed = time.perf_counter() - t0
            rate = len(rows) / elapsed if elapsed > 0 else 0
            print(f"done  {elapsed:.1f}s  ({rate:,.0f} rows/s)")
        except Exception as e:
            print(f"FAIL: {e}")
else:
    print("[SKIP] Data load skipped (--no-load)\n")
    try:
        cur.execute(f"USE {args.db}")
    except Exception as e:
        print(f"  WARN: USE {args.db}: {e}")

# ── Run Queries ───────────────────────────────────────────────────────────────

print(f"\n{'='*60}")
print(f"  TPC-H Query Execution")
print(f"{'='*60}\n")

query_ids = [args.query] if args.query else sorted(QUERIES.keys())

for qid in query_ids:
    if qid not in QUERIES:
        print(f"Q{qid}: unknown query")
        continue
    name, sql = QUERIES[qid]
    print(f"[Q{qid:02d}] {name}")
    t0 = time.perf_counter()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed_ms = (time.perf_counter() - t0) * 1000
        results[qid] = {"status": "OK", "rows": len(rows), "ms": elapsed_ms}
        print(f"       {elapsed_ms:8.1f} ms   {len(rows)} rows returned")
        # Print first 3 rows of result
        if rows:
            cols = [d[0] for d in cur.description] if cur.description else []
            for row in rows[:3]:
                vals = "  ".join(f"{c}={v}" for c,v in zip(cols, row))
                print(f"         {vals}")
            if len(rows) > 3:
                print(f"         ... ({len(rows)-3} more rows)")
    except Exception as e:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        results[qid] = {"status": "ERR", "rows": 0, "ms": elapsed_ms, "error": str(e)}
        print(f"       {elapsed_ms:8.1f} ms   ERROR: {e}")
    print()

# ── Summary ───────────────────────────────────────────────────────────────────

print(f"\n{'='*60}")
print(f"  TPC-H Summary  (SF={SF})")
print(f"{'='*60}")
print(f"  {'Q':>3}  {'Query Name':<35}  {'ms':>8}  {'Rows':>6}  Status")
print(f"  {'-'*3}  {'-'*35}  {'-'*8}  {'-'*6}  {'-'*6}")
for qid in query_ids:
    if qid not in results:
        continue
    r = results[qid]
    name, _ = QUERIES.get(qid, ("", ""))
    status_sym = "✓" if r["status"] == "OK" else "✗"
    print(f"  Q{qid:02d}  {name:<35}  {r['ms']:>8.1f}  {r['rows']:>6}  {status_sym} {r['status']}")
ok_count  = sum(1 for r in results.values() if r["status"] == "OK")
err_count = len(results) - ok_count
print(f"\n  Pass: {ok_count}  Fail: {err_count}  Total: {len(results)}")
if ok_count > 0:
    total_ms = sum(r["ms"] for r in results.values() if r["status"] == "OK")
    print(f"  Total query time: {total_ms:.1f} ms")
print(f"{'='*60}\n")

cur.close()
conn.close()
