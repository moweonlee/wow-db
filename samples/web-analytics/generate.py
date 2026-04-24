#!/usr/bin/env python3
"""
WOW-DB Web Analytics Sample Data Generator
Generates 10,000 synthetic events across 500 sessions.
"""
import random, time, sys, uuid
from datetime import datetime, timedelta

try:
    import pymysql
except ImportError:
    print("Install: pip install pymysql")
    sys.exit(1)

HOST = "127.0.0.1"
PORT = 9030
DB   = "web_analytics"

NUM_SESSIONS  = 500
EVENTS_TARGET = 10_000

COUNTRIES    = ["KR", "US", "JP", "DE", "GB"]
COUNTRY_WEIGHTS = [0.4, 0.25, 0.15, 0.1, 0.1]
DEVICES      = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [0.45, 0.45, 0.10]
BROWSERS     = ["Chrome", "Safari", "Firefox", "Edge"]
OS_LIST      = ["Windows", "macOS", "iOS", "Android", "Linux"]
PAGES        = ["/home", "/product", "/pricing", "/about", "/blog",
                "/checkout", "/login", "/signup", "/docs", "/contact"]
EVENT_TYPES  = ["pageview", "click", "scroll", "purchase"]
ET_WEIGHTS   = [0.70, 0.20, 0.07, 0.03]

BASE_TS = int(datetime(2026, 1, 1).timestamp() * 1000)

def gen_id():
    return str(uuid.uuid4())

random.seed(42)

sessions = []
for _ in range(NUM_SESSIONS):
    session_start = BASE_TS + random.randint(0, 30 * 24 * 3600 * 1000)
    event_count   = random.randint(1, 30)
    device        = random.choices(DEVICES, weights=DEVICE_WEIGHTS)[0]
    country       = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
    browser       = random.choice(BROWSERS)
    os            = random.choice(OS_LIST)
    visitor_id    = gen_id()
    user_id       = gen_id() if random.random() > 0.3 else None

    session_events = []
    ts = session_start
    entry_page = random.choice(PAGES)
    exit_page  = entry_page
    for i in range(event_count):
        page = random.choice(PAGES)
        if i == 0:
            entry_page = page
        exit_page = page
        et = random.choices(EVENT_TYPES, weights=ET_WEIGHTS)[0]
        session_events.append({
            "event_id":    gen_id(),
            "session_id":  gen_id(),
            "visitor_id":  visitor_id,
            "user_id":     user_id,
            "event_type":  et,
            "page_url":    page,
            "referrer_url": random.choice(PAGES) if random.random() > 0.5 else None,
            "device_type": device,
            "browser":     browser,
            "os":          os,
            "country":     country,
            "ts":          ts,
        })
        ts += random.randint(1000, 120_000)

    session_end = ts
    duration_ms = session_end - session_start
    pv_count    = sum(1 for e in session_events if e["event_type"] == "pageview")
    session_id  = session_events[0]["session_id"] if session_events else gen_id()

    sessions.append({
        "session_id":     session_id,
        "visitor_id":     visitor_id,
        "user_id":        user_id,
        "session_start":  session_start,
        "session_end":    session_end,
        "duration_ms":    duration_ms,
        "event_count":    event_count,
        "pageview_count": pv_count,
        "entry_page":     entry_page,
        "exit_page":      exit_page,
        "device_type":    device,
        "browser":        browser,
        "os":             os,
        "country":        country,
        "events":         session_events,
    })

# Flatten all events
all_events = [e for s in sessions for e in s["events"]]
print(f"Generated {len(sessions)} sessions, {len(all_events)} events")

# Connect and load
conn = pymysql.connect(host=HOST, port=PORT, user="root", database=DB,
                       autocommit=True, connect_timeout=30)
cur = conn.cursor()

def esc(v):
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"

# Load events
print("Loading events...")
batch = 200
t0 = time.perf_counter()
for i in range(0, len(all_events), batch):
    chunk = all_events[i:i+batch]
    vals = []
    for e in chunk:
        vals.append(f"({esc(e['event_id'])},{esc(e['session_id'])},{esc(e['visitor_id'])},"
                    f"{esc(e['user_id'])},{esc(e['event_type'])},{esc(e['page_url'])},"
                    f"{esc(e['referrer_url'])},{esc(e['device_type'])},{esc(e['browser'])},"
                    f"{esc(e['os'])},{esc(e['country'])},{e['ts']})")
    cur.execute(f"INSERT INTO events VALUES {','.join(vals)}")
print(f"  {len(all_events)} events in {time.perf_counter()-t0:.1f}s")

# Load sessions
print("Loading sessions...")
t0 = time.perf_counter()
for i in range(0, len(sessions), batch):
    chunk = sessions[i:i+batch]
    vals = []
    for s in chunk:
        vals.append(f"({esc(s['session_id'])},{esc(s['visitor_id'])},{esc(s['user_id'])},"
                    f"{s['session_start']},{s['session_end']},{s['duration_ms']},"
                    f"{s['event_count']},{s['pageview_count']},"
                    f"{esc(s['entry_page'])},{esc(s['exit_page'])},"
                    f"{esc(s['device_type'])},{esc(s['browser'])},{esc(s['os'])},{esc(s['country'])})")
    cur.execute(f"INSERT INTO sessions VALUES {','.join(vals)}")
print(f"  {len(sessions)} sessions in {time.perf_counter()-t0:.1f}s")

cur.execute("SELECT COUNT(*) FROM events")
print(f"\nFinal: {cur.fetchone()[0]} events")
cur.execute("SELECT COUNT(*) FROM sessions")
print(f"       {cur.fetchone()[0]} sessions")

cur.close()
conn.close()
print("\nDone! Run queries from samples/web-analytics/queries/")
