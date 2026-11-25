#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# SRH Hochschule Heidelberg — Data Engineering 1
# Data Processing Lab — One-shot setup for Ubuntu 22.04
#
# Concepts covered:
#   • Batch Processing (Pandas ETL pipeline)
#   • Stream Processing (windowing simulation)
#   • ETL pattern (extract → stage → transform → load)
#   • ELT pattern (extract → load raw → transform in SQL)
#
# Usage (run once on the Ubuntu VM):
#   bash data_processing_setup.sh
#
# Then follow the printed instructions.
# ─────────────────────────────────────────────────────────────
set -e

G='\033[0;32m'; O='\033[0;33m'; R='\033[0;31m'; B='\033[1;34m'; N='\033[0m'
LAB="$HOME/data-processing-lab"

banner() { echo -e "\n${B}▸ $1${N}"; }
ok()     { echo -e "${G}  ✓ $1${N}"; }
warn()   { echo -e "${O}  ! $1${N}"; }

echo -e "${O}"
echo "  ╔══════════════════════════════════════════════════╗"
echo "  ║   SRH — Data Processing Lab  Setup Script       ║"
echo "  ║   Batch · Stream · ETL · ELT                    ║"
echo "  ╚══════════════════════════════════════════════════╝"
echo -e "${N}"

# ── 1. System packages ────────────────────────────────────────
banner "Installing system packages (requires sudo)"
sudo apt-get update -qq
sudo apt-get install -y -qq python3 python3-pip python3-venv sqlite3
ok "System packages ready"

# ── 2. Project folder ─────────────────────────────────────────
banner "Creating project folder: $LAB"
mkdir -p "$LAB"
cd "$LAB"
ok "Folder: $LAB"

# ── 3. Virtual environment ────────────────────────────────────
banner "Creating Python virtual environment"
python3 -m venv venv
source venv/bin/activate
pip install -q --upgrade pip
ok "venv created and activated"

# ── 4. Python libraries ───────────────────────────────────────
banner "Installing Python libraries"
pip install -q pandas petl
ok "pandas, petl installed"

# ── 5. Generate sample data ───────────────────────────────────
banner "Generating sample dataset (bookings + countries)"
python3 << 'PYEOF'
import csv, random
from datetime import datetime, timedelta

random.seed(42)   # reproducible data

# ── bookings.csv (100 rows, 7 days of data) ───────────────────
countries  = ["DE","US","FR","GB","IT","ES","NL","PL","SE","CH"]
platforms  = ["web","mobile","app"]
start_date = datetime(2024, 1, 1)

with open("bookings.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["id","email","country_code","platform","user_id","listing_id","created_at"])
    for i in range(1, 101):
        days  = random.randint(0, 6)
        hours = random.randint(0, 23)
        mins  = random.randint(0, 59)
        ts    = (start_date + timedelta(days=days, hours=hours, minutes=mins)).strftime("%Y-%m-%d %H:%M:%S")
        w.writerow([i, f"user{i}@example.com", random.choice(countries),
                    random.choice(platforms), random.randint(1, 50),
                    random.randint(1, 200), ts])

# ── countries_code.csv (10 rows) ──────────────────────────────
with open("countries_code.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["country_code", "country_name"])
    for code, name in [
        ("DE","Germany"), ("US","United States"), ("FR","France"),
        ("GB","United Kingdom"), ("IT","Italy"), ("ES","Spain"),
        ("NL","Netherlands"), ("PL","Poland"), ("SE","Sweden"), ("CH","Switzerland")
    ]:
        w.writerow([code, name])

# ── stream_events.csv (60 events, 60-second simulation) ───────
with open("stream_events.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["timestamp_s","user_id","event_type","revenue"])
    for t in range(60):
        event_type = random.choices(
            ["page_view","click","purchase"],
            weights=[60, 30, 10]
        )[0]
        revenue = random.randint(50, 500) if event_type == "purchase" else 0
        w.writerow([t, random.randint(1, 20), event_type, revenue])

print("  bookings.csv        → 100 rows")
print("  countries_code.csv  → 10 rows")
print("  stream_events.csv   → 60 events")
PYEOF
ok "Sample data ready"

# ── 6. Write lab scripts ──────────────────────────────────────
banner "Writing lab Python scripts"

# ─── check_setup.py ───────────────────────────────────────────
cat > check_setup.py << 'PYEOF'
"""Phase 1 — Verify the lab environment."""
import sys
print(f"Python: {sys.version}\n")
missing = []
for lib in ["pandas", "petl", "sqlite3"]:
    try:
        __import__(lib)
        print(f"  {lib:<20} OK")
    except ImportError:
        print(f"  {lib:<20} MISSING")
        missing.append(lib)

import os
for fname in ["bookings.csv", "countries_code.csv", "stream_events.csv"]:
    status = "OK" if os.path.isfile(fname) else "MISSING"
    print(f"  {fname:<20} {status}")
    if status == "MISSING":
        missing.append(fname)

if missing:
    print(f"\n[!] Issues with: {', '.join(missing)}")
    print("    Re-run:  bash ~/data-processing-lab/data_processing_setup.sh")
else:
    print("\nAll checks passed — ready for the lab!")
PYEOF

# ─── phase2_batch_etl.py ──────────────────────────────────────
cat > phase2_batch_etl.py << 'PYEOF'
"""
Phase 2 — BATCH PROCESSING with Pandas ETL
─────────────────────────────────────────────
Scenario: Aggregate daily booking totals by country and platform.
Pattern:  Extract (CSV) → Transform (join + group) → Load (CSV + SQLite)
Lecture:  Slides 5, 20, 24 — Batch Data Flow & The ETL Process
"""
import pandas as pd
import sqlite3

# ── EXTRACT ───────────────────────────────────────────────────
print("=" * 55)
print("PHASE 2 — Batch ETL with Pandas")
print("=" * 55)

print("\n[1/3] EXTRACT — reading source CSV files...")
bookings  = pd.read_csv("bookings.csv")
countries = pd.read_csv("countries_code.csv")
print(f"  bookings.csv:       {len(bookings)} rows, columns: {list(bookings.columns)}")
print(f"  countries_code.csv: {len(countries)} rows, columns: {list(countries.columns)}")
print(f"\n  Sample booking row:\n  {bookings.iloc[0].to_dict()}")

# ── TRANSFORM (staging — outside the warehouse) ───────────────
print("\n[2/3] TRANSFORM — joining tables, aggregating...")
print("  (This happens on a 'staging server', NOT inside the warehouse)")

merged = bookings.merge(countries, on="country_code")
merged["date_d"] = pd.to_datetime(merged["created_at"]).dt.date

bookings_daily = (
    merged
    .groupby(["date_d", "country_name", "platform"])
    .size()
    .reset_index(name="total")
    .sort_values(["date_d", "total"], ascending=[True, False])
)

print(f"\n  Result: {len(bookings_daily)} rows in bookings_daily")
print("\n  Top 10 rows of transformed table:")
print(bookings_daily.head(10).to_string(index=False))

# ── LOAD (into 'data warehouse') ──────────────────────────────
print("\n[3/3] LOAD — writing to warehouse (SQLite)...")
print("  (Only clean, transformed data enters the warehouse)")
bookings_daily.to_csv("bookings_daily.csv", index=False)
conn = sqlite3.connect("warehouse.db")
bookings_daily.to_sql("bookings_daily", conn, if_exists="replace", index=False)
conn.close()

print(f"  bookings_daily.csv  → saved ({len(bookings_daily)} rows)")
print(f"  warehouse.db        → table 'bookings_daily' loaded")
print("\n  ETL complete: raw data stayed in CSV, only aggregated data entered the DW.")
print("=" * 55)
PYEOF

# ─── phase3_stream_windows.py ─────────────────────────────────
cat > phase3_stream_windows.py << 'PYEOF'
"""
Phase 3 — STREAM PROCESSING: Windowing Simulation
──────────────────────────────────────────────────
Scenario: A live stream of website events arrives second-by-second.
          Apply different window types to aggregate in real-time.
Lecture:  Slides 9–13 — Tumbling, Hopping, Sliding, Session Windows
"""
import csv, time
from collections import defaultdict

# ── Load "stream" (60 events over 60 simulated seconds) ───────
print("=" * 55)
print("PHASE 3 — Stream Processing: Window Types")
print("=" * 55)

events = []
with open("stream_events.csv") as f:
    for row in csv.DictReader(f):
        events.append({
            "t":        int(row["timestamp_s"]),
            "user_id":  int(row["user_id"]),
            "type":     row["event_type"],
            "revenue":  int(row["revenue"])
        })

print(f"\nStream contains {len(events)} events (simulated 0–59 seconds)")
print("Replaying stream (fast-forward)...")
time.sleep(0.5)
for e in events[:5]:
    print(f"  t={e['t']:2d}s  user={e['user_id']:2d}  {e['type']:<10}  revenue=€{e['revenue']}")
    time.sleep(0.07)
print(f"  ... ({len(events) - 5} more events)")

# ── TUMBLING WINDOW (10s, non-overlapping) ────────────────────
print("\n" + "─" * 55)
print("TUMBLING WINDOW  (size=10s, non-overlapping)")
print("Each event belongs to exactly ONE window.")
print("─" * 55)
tumbling = defaultdict(list)
for e in events:
    wid = e["t"] // 10          # integer division → window bucket
    tumbling[wid].append(e)

for wid in sorted(tumbling.keys()):
    ws      = tumbling[wid]
    start   = wid * 10
    end     = start + 10
    count   = len(ws)
    buys    = sum(1 for x in ws if x["type"] == "purchase")
    revenue = sum(x["revenue"] for x in ws)
    print(f"  Window [{start:2d}s–{end:2d}s):  {count:2d} events  |  {buys} purchases  |  €{revenue} revenue")

# ── HOPPING WINDOW (10s size, 5s hop, overlapping) ────────────
print("\n" + "─" * 55)
print("HOPPING WINDOW   (size=10s, hop=5s, overlapping)")
print("Events can belong to MORE THAN ONE window.")
print("─" * 55)
size, hop = 10, 5
max_t = max(e["t"] for e in events)
for start in range(0, max_t - size + 2, hop):
    end  = start + size
    ws   = [e for e in events if start <= e["t"] < end]
    buys = sum(1 for x in ws if x["type"] == "purchase")
    rev  = sum(x["revenue"] for x in ws)
    print(f"  Window [{start:2d}s–{end:2d}s):  {len(ws):2d} events  |  {buys} purchases  |  €{rev} revenue")

# ── SESSION WINDOW (5s inactivity gap) ────────────────────────
print("\n" + "─" * 55)
print("SESSION WINDOW   (5s inactivity gap)")
print("Window closes when no event arrives for 5+ seconds.")
print("─" * 55)
GAP = 5
sessions = []
session_start = events[0]["t"]
prev_t        = events[0]["t"]
session_events= [events[0]]

for e in events[1:]:
    if e["t"] - prev_t > GAP:
        sessions.append((session_start, prev_t, session_events))
        session_start  = e["t"]
        session_events = []
    session_events.append(e)
    prev_t = e["t"]
sessions.append((session_start, prev_t, session_events))

for i, (s, end, ws) in enumerate(sessions, 1):
    buys = sum(1 for x in ws if x["type"] == "purchase")
    rev  = sum(x["revenue"] for x in ws)
    print(f"  Session {i:2d} [{s:2d}s–{end:2d}s]:  {len(ws):2d} events  |  {buys} purchases  |  €{rev} revenue")

print("\n  Key insight: same data, different window → different aggregation results!")
print("=" * 55)
PYEOF

# ─── phase4_elt.py ────────────────────────────────────────────
cat > phase4_elt.py << 'PYEOF'
"""
Phase 4 — ELT PATTERN: Transform Inside the Warehouse
────────────────────────────────────────────────────────
Scenario: Same bookings pipeline, but ELT approach:
          raw data loaded first → transform with SQL inside the DW.
Contrast: Phase 2 used ETL (transform on staging server, then load).
Lecture:  Slides 21–23 — The ELT Process & ETL vs ELT
"""
import pandas as pd
import sqlite3

print("=" * 55)
print("PHASE 4 — ELT Pipeline (SQL transformations in-warehouse)")
print("=" * 55)

# ── EXTRACT & LOAD RAW (no transformation yet) ────────────────
print("\n[1/3] EXTRACT + LOAD RAW — no transformation on staging...")
print("  (ELT key difference: raw data goes straight into the DW)")

conn = sqlite3.connect("warehouse.db")

bookings  = pd.read_csv("bookings.csv")
countries = pd.read_csv("countries_code.csv")

bookings.to_sql("raw_bookings",  conn, if_exists="replace", index=False)
countries.to_sql("raw_countries", conn, if_exists="replace", index=False)

print(f"  raw_bookings  → {len(bookings)} rows loaded (untransformed!)")
print(f"  raw_countries → {len(countries)} rows loaded")

# ── TRANSFORM INSIDE THE WAREHOUSE (SQL) ──────────────────────
print("\n[2/3] TRANSFORM inside DW using SQL...")
print("  (The 'analyst' writes SQL — no staging server or ETL code needed)")

sql = """
CREATE TABLE IF NOT EXISTS elt_bookings_daily AS
SELECT
    DATE(b.created_at)  AS date_d,
    c.country_name,
    b.platform,
    COUNT(*)            AS total
FROM  raw_bookings  b
JOIN  raw_countries c ON b.country_code = c.country_code
GROUP BY DATE(b.created_at), c.country_name, b.platform
ORDER BY date_d, total DESC
"""
conn.execute("DROP TABLE IF EXISTS elt_bookings_daily")
conn.execute(sql)
conn.commit()

result = pd.read_sql("SELECT * FROM elt_bookings_daily", conn)
print(f"\n  Result: {len(result)} rows in elt_bookings_daily")
print("\n  Top 10 rows:")
print(result.head(10).to_string(index=False))

# ── COMPARE ETL vs ELT RESULTS ────────────────────────────────
print("\n[3/3] VERIFY — ETL vs ELT produce identical output...")
etl_result = pd.read_sql("SELECT * FROM bookings_daily ORDER BY date_d, total DESC", conn)
etl_sums   = etl_result["total"].sum()
elt_sums   = result["total"].sum()
match      = "✓ MATCH" if etl_sums == elt_sums else "✗ MISMATCH"
print(f"  ETL total bookings: {etl_sums}")
print(f"  ELT total bookings: {elt_sums}")
print(f"  Result comparison:  {match}")

conn.close()
print("\n  Tables in warehouse.db: raw_bookings, raw_countries,")
print("  bookings_daily (ETL), elt_bookings_daily (ELT) — same data, different path!")
print("=" * 55)

print("""
  ┌──────────────┬────────────────────────────────────────────┐
  │              │ ETL (Phase 2)       ELT (Phase 4)         │
  ├──────────────┼────────────────────────────────────────────┤
  │ Transform    │ Python/Pandas       SQL inside SQLite      │
  │ Where?       │ Staging (outside)   Inside the warehouse   │
  │ DW contains  │ Clean data only     Raw + transformed data │
  │ Who writes?  │ Data Engineer       Analyst (SQL)          │
  │ GDPR risk    │ Lower (PII masked)  Higher (raw in DW)     │
  └──────────────┴────────────────────────────────────────────┘
""")
PYEOF

# ─── data_processing_lab.py (complete solution — full pipeline) ──
cat > data_processing_lab.py << 'PYEOF'
"""
SRH Data Engineering 1 — Data Processing Lab
Complete Pipeline: Batch ETL + Stream Windows + ELT Comparison
─────────────────────────────────────────────────────────────
Assignment: Build a complete data processing pipeline that:
  1. Reads raw booking data (batch ETL with Pandas)
  2. Simulates stream windowing (tumbling + hopping)
  3. Loads and transforms using ELT (SQL in SQLite)
  4. Saves all outputs to files
  5. Prints a final report
"""
import pandas as pd
import sqlite3
import csv
from collections import defaultdict

SEP = "=" * 60

# ═══════════════════════════════════════════════════════════════
# PART 1 — BATCH ETL (Pandas)
# ═══════════════════════════════════════════════════════════════
print(SEP)
print("PART 1: BATCH ETL PIPELINE (Pandas)")
print(SEP)

bookings  = pd.read_csv("bookings.csv")
countries = pd.read_csv("countries_code.csv")

merged = bookings.merge(countries, on="country_code")
merged["date_d"] = pd.to_datetime(merged["created_at"]).dt.date

bookings_daily = (
    merged
    .groupby(["date_d", "country_name", "platform"])
    .size()
    .reset_index(name="total")
    .sort_values(["date_d", "total"], ascending=[True, False])
)

bookings_daily.to_csv("bookings_daily.csv", index=False)
print(f"  Extracted  : {len(bookings)} bookings, {len(countries)} countries")
print(f"  Transformed: {len(bookings_daily)} aggregated rows")
print(f"  Loaded     : bookings_daily.csv (ETL output)")

# Top country summary
top_countries = (
    merged.groupby("country_name").size()
    .reset_index(name="bookings")
    .sort_values("bookings", ascending=False)
    .head(5)
)
print("\n  Top 5 countries by bookings:")
print(top_countries.to_string(index=False))

# ═══════════════════════════════════════════════════════════════
# PART 2 — STREAM PROCESSING (Tumbling Windows)
# ═══════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("PART 2: STREAM PROCESSING — TUMBLING WINDOWS")
print(SEP)

events = []
with open("stream_events.csv") as f:
    for row in csv.DictReader(f):
        events.append({"t": int(row["timestamp_s"]), "type": row["event_type"],
                        "revenue": int(row["revenue"])})

tumbling = defaultdict(list)
for e in events:
    tumbling[e["t"] // 10].append(e)

print(f"  Processed {len(events)} stream events in tumbling windows (size=10s):\n")
print(f"  {'Window':<20} {'Events':>7} {'Purchases':>10} {'Revenue':>10}")
print(f"  {'-'*20} {'-'*7} {'-'*10} {'-'*10}")
for wid in sorted(tumbling.keys()):
    ws    = tumbling[wid]
    start = wid * 10
    buys  = sum(1 for x in ws if x["type"] == "purchase")
    rev   = sum(x["revenue"] for x in ws)
    print(f"  [{start:2d}s – {start+10:2d}s)       {len(ws):>7} {buys:>10} {rev:>9}€")

# ═══════════════════════════════════════════════════════════════
# PART 3 — ELT (SQLite — transform inside the warehouse)
# ═══════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("PART 3: ELT PIPELINE (SQL transformations inside SQLite DW)")
print(SEP)

conn = sqlite3.connect("lab_warehouse.db")
bookings.to_sql("raw_bookings",   conn, if_exists="replace", index=False)
countries.to_sql("raw_countries", conn, if_exists="replace", index=False)
conn.execute("DROP TABLE IF EXISTS bookings_daily_elt")
conn.execute("""
    CREATE TABLE bookings_daily_elt AS
    SELECT DATE(b.created_at) AS date_d,
           c.country_name, b.platform, COUNT(*) AS total
    FROM  raw_bookings  b
    JOIN  raw_countries c ON b.country_code = c.country_code
    GROUP BY DATE(b.created_at), c.country_name, b.platform
""")
conn.commit()
elt_result = pd.read_sql("SELECT * FROM bookings_daily_elt", conn)
conn.close()

print(f"  Loaded     : {len(bookings)} raw rows → lab_warehouse.db")
print(f"  Transformed: {len(elt_result)} rows via SQL (no staging server!)")
print(f"  ETL total  : {bookings_daily['total'].sum()}")
print(f"  ELT total  : {elt_result['total'].sum()}")
print(f"  Match      : {'✓ Both approaches yield the same result' if bookings_daily['total'].sum() == elt_result['total'].sum() else '✗'}")

# ═══════════════════════════════════════════════════════════════
# FINAL REPORT
# ═══════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("LAB COMPLETE — Output files:")
print(SEP)
import os
for f in ["bookings_daily.csv", "lab_warehouse.db"]:
    size = os.path.getsize(f) if os.path.isfile(f) else 0
    print(f"  {f:<30} {size:>8} bytes")
print(f"\n  Run:  sqlite3 lab_warehouse.db '.tables'")
print(f"  Run:  sqlite3 lab_warehouse.db 'SELECT * FROM bookings_daily_elt LIMIT 5;'")
PYEOF

ok "Lab scripts written (5 files)"

# ── 7. Verify installation ────────────────────────────────────
banner "Verifying setup"
python3 check_setup.py

# ── 8. Write activate helper ──────────────────────────────────
cat > "$LAB/lab.sh" << SHEOF
#!/usr/bin/env bash
# Run any lab script with the venv automatically active
# Usage: bash lab.sh <script.py>
source "$LAB/venv/bin/activate"
python3 "\$1"
SHEOF
chmod +x "$LAB/lab.sh"
ok "Helper script created: lab.sh"

# ── Done ──────────────────────────────────────────────────────
echo ""
echo -e "${G}╔══════════════════════════════════════════════════════╗${N}"
echo -e "${G}║  Setup complete! Follow the lab steps below:        ║${N}"
echo -e "${G}╠══════════════════════════════════════════════════════╣${N}"
echo -e "${G}║  STEP 1 — Activate the Python environment (once):   ║${N}"
echo -e "${G}║    source ~/data-processing-lab/venv/bin/activate    ║${N}"
echo -e "${G}║                                                       ║${N}"
echo -e "${G}║  STEP 2 — Go to the lab folder:                     ║${N}"
echo -e "${G}║    cd ~/data-processing-lab                          ║${N}"
echo -e "${G}║                                                       ║${N}"
echo -e "${G}║  STEP 3 — Run the phases in order:                  ║${N}"
echo -e "${G}║    python3 check_setup.py         # verify setup     ║${N}"
echo -e "${G}║    python3 phase2_batch_etl.py    # Batch ETL        ║${N}"
echo -e "${G}║    python3 phase3_stream_windows.py # Stream windows ║${N}"
echo -e "${G}║    python3 phase4_elt.py          # ELT pipeline     ║${N}"
echo -e "${G}║                                                       ║${N}"
echo -e "${G}║  STEP 4 — Run the complete lab deliverable:         ║${N}"
echo -e "${G}║    python3 data_processing_lab.py                    ║${N}"
echo -e "${G}║                                                       ║${N}"
echo -e "${G}║  BONUS — Inspect the SQLite warehouse:              ║${N}"
echo -e "${G}║    sqlite3 lab_warehouse.db '.tables'                ║${N}"
echo -e "${G}║    sqlite3 lab_warehouse.db \                        ║${N}"
echo -e "${G}║      'SELECT * FROM bookings_daily_elt LIMIT 10;'    ║${N}"
echo -e "${G}╚══════════════════════════════════════════════════════╝${N}"
echo ""
echo -e "${B}  Concepts demonstrated:${N}"
echo -e "  • Batch Processing   → phase2_batch_etl.py      (Pandas ETL pipeline)"
echo -e "  • Stream Windows     → phase3_stream_windows.py (Tumbling/Hopping/Session)"
echo -e "  • ELT vs ETL         → phase4_elt.py            (SQL transform in SQLite)"
echo -e "  • Full pipeline      → data_processing_lab.py   (all combined)"
echo ""
