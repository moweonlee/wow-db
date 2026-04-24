#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
"""
WOW-DB TPC-H Benchmark

Usage:
  python bench.py --sf 0.01 --queries all --output json --report out.json
  python bench.py --sf 0.1 --queries Q01,Q03,Q06 --output markdown
  python bench.py --no-load --queries Q01 --runs 3
"""

import pymysql
import random
import time
import sys
import argparse
import math
import json
import os
import platform
import subprocess
from datetime import date, timedelta, datetime
from collections import defaultdict

# ─── CLI Args ────────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser(description="WOW-DB TPC-H Benchmark")
parser.add_argument("--host",    default="127.0.0.1")
parser.add_argument("--port",    type=int, default=9030)
parser.add_argument("--user",    default="root")
parser.add_argument("--db",      default="tpch")
parser.add_argument("--sf",      type=float, default=0.01, help="Scale factor (0.01 | 0.1 | 1)")
parser.add_argument("--no-load", action="store_true", help="Skip table creation & data load")
parser.add_argument("--query",   type=int, default=0, help="Run single query number (0=all; legacy)")
parser.add_argument("--queries", default="Q01,Q03,Q05,Q06,Q10,Q14",
                    help="Comma-separated query IDs (e.g. Q01,Q03) or 'all' for all 22")
parser.add_argument("--output",  default="text", choices=["text","json","markdown"],
                    help="Output format")
parser.add_argument("--report",  default=None,
                    help="Write JSON report to FILE")
parser.add_argument("--runs",    type=int, default=1,
                    help="Number of runs per query (for p50/p95/p99 stats)")
parser.add_argument("--timeout", type=int, default=30, help="Per-query timeout seconds")
args = parser.parse_args()

SF = args.sf
RUN_ID = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# Resolve query list
ALL_QUERY_IDS = [f"Q{i:02d}" for i in range(1, 23)]
if args.query:
    query_ids_to_run = [f"Q{args.query:02d}"]
elif args.queries.lower() == "all":
    query_ids_to_run = ALL_QUERY_IDS
else:
    query_ids_to_run = [q.strip().upper() for q in args.queries.split(",")]

# Row counts per SF=1 (TPC-H spec)
N_REGION    = 5
N_NATION    = 25
N_SUPPLIER  = int(10_000 * SF)
N_CUSTOMER  = int(150_000 * SF)
N_PART      = int(200_000 * SF)
N_PARTSUPP  = N_PART * 4
N_ORDERS    = int(1_500_000 * SF)
N_LINEITEM  = int(6_000_000 * SF)

def get_wow_db_version():
    try:
        result = subprocess.run(["git","rev-parse","--short","HEAD"],
                                capture_output=True, text=True, cwd=os.path.dirname(__file__))
        return result.stdout.strip() if result.returncode == 0 else "unknown"
    except Exception:
        return "unknown"

def get_hardware_info():
    import multiprocessing
    try:
        ram_gb = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') // (1024**3)
    except Exception:
        ram_gb = 0
    try:
        import psutil
        ram_gb = psutil.virtual_memory().total // (1024**3)
    except Exception:
        pass
    docker_ver = "unknown"
    try:
        r = subprocess.run(["docker","version","--format","{{.Client.Version}}"],
                           capture_output=True, text=True)
        docker_ver = r.stdout.strip() if r.returncode == 0 else "unknown"
    except Exception:
        pass
    return {
        "cpu_model": platform.processor() or platform.machine(),
        "cpu_cores": multiprocessing.cpu_count(),
        "ram_gb": ram_gb,
        "storage_type": "SSD",
        "os": platform.platform(),
        "docker_version": docker_ver,
    }

WOW_DB_VERSION = get_wow_db_version()

print(f"\n{'='*60}")
print(f"  WOW-DB TPC-H Benchmark  |  SF={SF}")
print(f"  Target: {args.host}:{args.port}  Queries: {args.queries}")
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
CREATE CUBE IF NOT EXISTS region (
    r_regionkey  INT NOT NULL,
    r_name       VARCHAR(25) NOT NULL,
    r_comment    VARCHAR(152)
);

CREATE CUBE IF NOT EXISTS nation (
    n_nationkey  INT NOT NULL,
    n_name       VARCHAR(25) NOT NULL,
    n_regionkey  INT NOT NULL,
    n_comment    VARCHAR(152)
);

CREATE CUBE IF NOT EXISTS supplier (
    s_suppkey    INT NOT NULL,
    s_name       VARCHAR(25) NOT NULL,
    s_address    VARCHAR(40) NOT NULL,
    s_nationkey  INT NOT NULL,
    s_phone      VARCHAR(15) NOT NULL,
    s_acctbal    DOUBLE NOT NULL,
    s_comment    VARCHAR(101)
);

CREATE CUBE IF NOT EXISTS customer (
    c_custkey    INT NOT NULL,
    c_name       VARCHAR(25) NOT NULL,
    c_address    VARCHAR(40) NOT NULL,
    c_nationkey  INT NOT NULL,
    c_phone      VARCHAR(15) NOT NULL,
    c_acctbal    DOUBLE NOT NULL,
    c_mktsegment VARCHAR(10) NOT NULL,
    c_comment    VARCHAR(117)
);

CREATE CUBE IF NOT EXISTS part (
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

CREATE CUBE IF NOT EXISTS partsupp (
    ps_partkey    INT NOT NULL,
    ps_suppkey    INT NOT NULL,
    ps_availqty   INT NOT NULL,
    ps_supplycost DOUBLE NOT NULL,
    ps_comment    VARCHAR(199)
);

CREATE CUBE IF NOT EXISTS orders (
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

CREATE CUBE IF NOT EXISTS lineitem (
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
    "DROP CUBE IF EXISTS lineitem",
    "DROP CUBE IF EXISTS orders",
    "DROP CUBE IF EXISTS partsupp",
    "DROP CUBE IF EXISTS part",
    "DROP CUBE IF EXISTS customer",
    "DROP CUBE IF EXISTS supplier",
    "DROP CUBE IF EXISTS nation",
    "DROP CUBE IF EXISTS region",
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

# ── Expected row counts at SF=0.01 (used for PASS/PARTIAL classification) ─────

EXPECTED_ROWS = {
    "Q01": 4,   "Q02": 5,   "Q03": 10,  "Q04": 5,   "Q05": 5,
    "Q06": 1,   "Q07": 4,   "Q08": 2,   "Q09": 175, "Q10": 20,
    "Q11": 50,  "Q12": 2,   "Q13": 42,  "Q14": 1,   "Q15": 1,
    "Q16": 18,  "Q17": 1,   "Q18": 10,  "Q19": 1,   "Q20": 1,
    "Q21": 10,  "Q22": 7,
}

# ── Load SQL files from samples/tpch/queries/ if present ─────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT   = os.path.dirname(os.path.dirname(SCRIPT_DIR))
SQL_DIR     = os.path.join(REPO_ROOT, "samples", "tpch", "queries")

def load_query_sql(qid):
    """Load SQL for a query ID. Falls back to QUERIES dict if file not found."""
    sql_file = os.path.join(SQL_DIR, f"{qid}.sql")
    if os.path.exists(sql_file):
        with open(sql_file, encoding="utf-8") as f:
            content = f.read()
        # Strip SQL comments at top to get the actual SQL
        lines = [l for l in content.splitlines() if not l.strip().startswith("--")]
        return "\n".join(lines).strip(), sql_file
    # Fall back to inline QUERIES dict (legacy)
    qnum = int(qid[1:])
    if qnum in QUERIES:
        name, sql = QUERIES[qnum]
        return sql, None
    return None, None

# ── Run Queries ───────────────────────────────────────────────────────────────

print(f"\n{'='*60}")
print(f"  TPC-H Query Execution  (SF={SF}, runs={args.runs})")
print(f"{'='*60}\n")

load_start = time.perf_counter()
query_results = []   # list of result dicts per contract schema

for qid in query_ids_to_run:
    sql, sql_file = load_query_sql(qid)
    qnum = int(qid[1:])
    name = QUERIES.get(qnum, ("Unknown",))[0] if qnum in QUERIES else qid

    if sql is None:
        print(f"[{qid}] SKIP — no SQL file or inline definition")
        query_results.append({
            "id": qid, "sql_file": sql_file, "status": "SKIP",
            "duration_ms": None, "row_count_actual": None,
            "row_count_expected": EXPECTED_ROWS.get(qid),
            "error": None, "notes": "No SQL defined"
        })
        continue

    print(f"[{qid}] {name}")
    run_times = []
    status = "FAIL"
    actual_rows = 0
    error_msg = None

    for run_idx in range(args.runs):
        # Reconnect if connection was lost by a previous query
        try:
            conn.ping(reconnect=True)
            cur = conn.cursor()
            cur.execute(f"USE {args.db}")
        except Exception:
            try:
                conn = connect()
                cur = conn.cursor()
                cur.execute(f"USE {args.db}")
            except Exception as e2:
                error_msg = f"Reconnect failed: {e2}"
                status = "FAIL"
                break

        t0 = time.perf_counter()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            elapsed_ms = (time.perf_counter() - t0) * 1000
            run_times.append(elapsed_ms)
            actual_rows = len(rows)
            expected = EXPECTED_ROWS.get(qid)
            if expected is None:
                status = "PASS"
            elif abs(actual_rows - expected) <= max(1, int(expected * 0.01)):
                status = "PASS"
            else:
                status = "PARTIAL"
            if run_idx == 0:
                cols = [d[0] for d in cur.description] if cur.description else []
                for row in rows[:2]:
                    vals = "  ".join(f"{c}={v}" for c, v in zip(cols, row))
                    print(f"         {vals}")
                if len(rows) > 2:
                    print(f"         ... ({len(rows)-2} more)")
            print(f"       run {run_idx+1}: {elapsed_ms:.1f} ms  {actual_rows} rows  {status}")
        except Exception as e:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            run_times.append(elapsed_ms)
            error_msg = str(e)
            status = "FAIL"
            print(f"       run {run_idx+1}: {elapsed_ms:.1f} ms  ERROR: {e}")
            break  # don't retry on error

    median_ms = sorted(run_times)[len(run_times)//2] if run_times else None
    sym = {"PASS": "✅", "PARTIAL": "⚠️", "FAIL": "❌", "SKIP": "⏭"}.get(status, "?")
    print(f"       {sym} {status}  median={median_ms:.1f}ms  rows={actual_rows}\n")

    query_results.append({
        "id": qid,
        "sql_file": sql_file or f"inline:{qid}",
        "status": status,
        "duration_ms": median_ms,
        "row_count_actual": actual_rows,
        "row_count_expected": EXPECTED_ROWS.get(qid),
        "error": error_msg,
        "notes": None,
    })

load_end = time.perf_counter()
total_load_ms = (load_end - load_start) * 1000

# ── Build summary ─────────────────────────────────────────────────────────────

counts = {"PASS": 0, "PARTIAL": 0, "FAIL": 0, "SKIP": 0}
for r in query_results:
    counts[r["status"]] = counts.get(r["status"], 0) + 1

total_query_ms = sum(r["duration_ms"] for r in query_results if r["duration_ms"])

# ── Text summary (always printed) ─────────────────────────────────────────────

print(f"\n{'='*60}")
print(f"  TPC-H Summary  SF={SF}  WOW-DB {WOW_DB_VERSION}")
print(f"{'='*60}")
print(f"  {'Query':<6}  {'Status':<8}  {'ms':>8}  {'Actual':>7}  {'Expected':>8}")
print(f"  {'-'*6}  {'-'*8}  {'-'*8}  {'-'*7}  {'-'*8}")
for r in query_results:
    sym = {"PASS": "✅", "PARTIAL": "⚠️", "FAIL": "❌", "SKIP": "⏭"}.get(r["status"], "?")
    ms_str = f"{r['duration_ms']:.1f}" if r["duration_ms"] else "—"
    exp_str = str(r["row_count_expected"]) if r["row_count_expected"] is not None else "—"
    print(f"  {r['id']:<6}  {sym} {r['status']:<6}  {ms_str:>8}  {r['row_count_actual']:>7}  {exp_str:>8}")
print(f"\n  PASS={counts['PASS']}  PARTIAL={counts['PARTIAL']}  FAIL={counts['FAIL']}  SKIP={counts['SKIP']}")
print(f"  Total query time: {total_query_ms:.1f} ms")
print(f"{'='*60}\n")

# ── JSON output ───────────────────────────────────────────────────────────────

if args.output == "json" or args.report:
    hw = get_hardware_info()
    report = {
        "run_id": RUN_ID,
        "wow_db_version": WOW_DB_VERSION,
        "hardware": hw,
        "cluster": {"qn": 3, "cn": 3, "sn": 3},
        "scale_factor": SF,
        "data_state": "cold_cache",
        "load_duration_ms": int(total_load_ms),
        "queries": query_results,
        "summary": {
            "total_query_ms": int(total_query_ms),
            "pass": counts["PASS"],
            "partial": counts["PARTIAL"],
            "fail": counts["FAIL"],
            "skip": counts["SKIP"],
        },
    }
    if args.output == "json":
        print(json.dumps(report, indent=2))
    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"[REPORT] Written to {args.report}")

# ── Markdown output ───────────────────────────────────────────────────────────

elif args.output == "markdown":
    print(f"## TPC-H Benchmark Results — SF={SF} — {RUN_ID[:10]}\n")
    print(f"| Query | Status | Duration (ms) | Rows Actual | Rows Expected |")
    print(f"|-------|--------|--------------|-------------|---------------|")
    for r in query_results:
        sym = {"PASS": "✅ PASS", "PARTIAL": "⚠️ PARTIAL", "FAIL": "❌ FAIL", "SKIP": "⏭ SKIP"}.get(r["status"], r["status"])
        ms = f"{r['duration_ms']:.0f}" if r["duration_ms"] else "—"
        exp = str(r["row_count_expected"]) if r["row_count_expected"] is not None else "—"
        print(f"| {r['id']} | {sym} | {ms} | {r['row_count_actual']} | {exp} |")

cur.close()
conn.close()
