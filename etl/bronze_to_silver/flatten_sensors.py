"""
flatten_sensors.py -- Bronze to Silver: sensor_events (optimized)
================================================================
Parallel processing, resume-capable via watermark.
Only scans recent Bronze folders instead of full rglob.

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import csv
import io
import json
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from psycopg2 import extras as _pg_extras
from sqlalchemy import create_engine, text

load_dotenv()

BRONZE_ROOT = Path(os.getenv("BRONZE_ROOT", r"storage\bronze"))
DB_URL      = os.getenv("DB_URL")
APARTMENTS  = ["jimmy", "jeremie"]
WORKERS     = 8
BATCH_SIZE  = 2000
LOG_EVERY   = 1     # log after every batch — keeps the user reassured during long runs

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("flatten_sensors")

R="\033[0m"; B="\033[1m"; D="\033[2m"; GR="\033[32m"; RE="\033[31m"

# -- WATERMARK -----------------------------------------------------------------

WATERMARK_DDL = """
    CREATE TABLE IF NOT EXISTS silver.etl_watermark (
        filename     VARCHAR(200) PRIMARY KEY,
        processed_at TIMESTAMPTZ DEFAULT NOW()
    );
"""

def load_watermark(engine):
    with engine.begin() as conn:
        conn.execute(text(WATERMARK_DDL))
        rows = conn.execute(text("SELECT filename FROM silver.etl_watermark")).fetchall()
    return {r[0] for r in rows}


def watermark_count(engine):
    """Fast count without loading all filenames."""
    with engine.begin() as conn:
        row = conn.execute(text("SELECT COUNT(*) FROM silver.etl_watermark")).fetchone()
    return row[0]


def mark_done(engine, filenames):
    """Bulk-mark filenames as processed. Uses psycopg2.execute_values for
    ~10-30x speedup over SQLAlchemy executemany."""
    if not filenames:
        return
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        _pg_extras.execute_values(
            cur,
            "INSERT INTO silver.etl_watermark (filename) VALUES %s ON CONFLICT DO NOTHING",
            [(f,) for f in filenames],
            page_size=5000,
        )
        raw.commit()
    finally:
        raw.close()

# -- SENSOR PARSING ------------------------------------------------------------

ROOM_MAP = {
    "Bhroom": "Bathroom", "Bdroom": "Bedroom", "Livingroom": "Living Room",
    "Office": "Office", "Kitchen": "Kitchen", "Laundry": "Laundry",
    "Outdoor": "Outdoor", "House": "House",
}

def norm_room(r):
    return ROOM_MAP.get(r, r)

BOUNDS = {
    "temperature_c": (-20, 60), "humidity_pct": (0, 100), "co2_ppm": (300, 5000),
    "noise_db": (0, 140), "pressure_hpa": (870, 1085), "power": (0, 10000), "battery": (0, 100),
}

def is_outlier(field, value):
    if field not in BOUNDS or value is None:
        return False
    lo, hi = BOUNDS[field]
    return not (lo <= value <= hi)


def parse_timestamp(raw):
    try:
        return datetime.strptime(raw, "%d.%m.%Y %H:%M").replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


def make_row(apt, room, stype, field, value, unit, ts):
    v = float(value) if value is not None else None
    return {
        "apartment": apt, "room": norm_room(room), "sensor_type": stype,
        "field": field, "value": v, "unit": unit, "timestamp": ts,
        "is_outlier": is_outlier(field, v),
    }


def flatten(apt, payload, ts):
    rows = []
    for room, d in payload.get("plugs", {}).items():
        for f, u in [("power","W"),("total","Wh"),("temperature","C")]:
            if d.get(f) is not None:
                rows.append(make_row(apt, room, "plug", f, d[f], u, ts))

    for room, sensors in payload.get("doorsWindows", {}).items():
        if not isinstance(sensors, list):
            sensors = [sensors]
        for s in sensors:
            stype = s.get("type","door").lower()
            rows.append(make_row(apt, room, stype, "open", 1.0 if str(s.get("switch","off")).lower()=="on" else 0.0, "bool", ts))
            if s.get("battery") is not None:
                rows.append(make_row(apt, room, stype, "battery", s["battery"], "%", ts))

    for room, d in payload.get("motions", {}).items():
        for f, u in [("motion","bool"),("light","lux"),("temperature","C")]:
            if d.get(f) is not None:
                v = 1.0 if d[f] is True else (0.0 if d[f] is False else d[f])
                rows.append(make_row(apt, room, "motion", f, v, u, ts))

    inner = payload.get("meteos", {}).get("meteo", payload.get("meteos", {}))
    for room, d in inner.items():
        for src, field, unit in [
            ("Temperature","temperature_c","C"),("CO2","co2_ppm","ppm"),
            ("Humidity","humidity_pct","%"),("Noise","noise_db","dB"),
            ("Pressure","pressure_hpa","hPa"),("AbsolutePressure","abs_pressure_hpa","hPa"),
            ("battery_percent","battery","%"),
        ]:
            if d.get(src) is not None:
                rows.append(make_row(apt, room, "meteo", field, d[src], unit, ts))

    for room, d in payload.get("humidities", {}).items():
        for f, u in [("temperature","C"),("humidity","%")]:
            if d.get(f) is not None:
                rows.append(make_row(apt, room, "humidity", f, d[f], u, ts))
        if d.get("devicePower") is not None:
            rows.append(make_row(apt, room, "humidity", "battery", d["devicePower"], "%", ts))

    for loc, d in payload.get("consumptions", {}).items():
        for f, u in [("total_power","W"),("power1","W"),("power2","W"),("power3","W"),
                     ("current1","A"),("current2","A"),("current3","A"),
                     ("voltage1","V"),("voltage2","V"),("voltage3","V")]:
            if d.get(f) is not None:
                rows.append(make_row(apt, loc, "consumption", f, d[f], u, ts))

    return rows

# -- BATCH PROCESSING ----------------------------------------------------------

def process_batch(args):
    paths_and_apt, db_url = args
    all_rows = []
    processed = []
    errors = 0
    for path_str, apt in paths_and_apt:
        try:
            with open(Path(path_str), encoding="utf-8") as f:
                payload = json.load(f)
            ts = parse_timestamp(payload.get("datetime", ""))
            all_rows.extend(flatten(apt, payload, ts))
            processed.append(Path(path_str).name)
        except Exception:
            errors += 1
    return {"rows": all_rows, "processed": processed, "errors": errors}


_TEMP_DDL = """
    CREATE TEMP TABLE _tmp_sensor_events (
        apartment   text,
        room        text,
        sensor_type text,
        field       text,
        value       double precision,
        unit        text,
        "timestamp" timestamptz,
        is_outlier  boolean
    ) ON COMMIT DROP
"""

# Upsert from temp table. DISTINCT ON dedupes within the batch so PostgreSQL
# doesn't complain about "command cannot affect row a second time" when the
# same (apartment, room, sensor_type, field, timestamp) appears twice.
_UPSERT_FROM_TMP = """
    INSERT INTO silver.sensor_events
        (apartment, room, sensor_type, field, value, unit, timestamp, is_outlier)
    SELECT DISTINCT ON (apartment, room, sensor_type, field, timestamp)
           apartment, room, sensor_type, field, value, unit, timestamp, is_outlier
    FROM _tmp_sensor_events
    ORDER BY apartment, room, sensor_type, field, timestamp
    ON CONFLICT (apartment, room, sensor_type, field, timestamp)
    DO UPDATE SET value      = EXCLUDED.value,
                  unit       = EXCLUDED.unit,
                  is_outlier = EXCLUDED.is_outlier
"""

def upsert(engine, rows):
    """Bulk-upsert sensor events using PostgreSQL COPY into a TEMP TABLE,
    then a single INSERT ... SELECT ... ON CONFLICT to merge into silver.

    COPY is the fastest bulk-load mechanism in Postgres — bytes stream
    directly into the temp table, no statement parsing per row, no per-row
    network round trips. Combined with a single set-based upsert, this is
    typically 5-10x faster than execute_values and 50-150x faster than the
    original per-row INSERT.
    """
    if not rows:
        return
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute(_TEMP_DDL)

        # Stream rows as CSV into the temp table. NULL '' makes empty cells
        # be NULL (so missing value/unit doesn't blow up doubles).
        buf = io.StringIO()
        w = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
        for r in rows:
            ts = r["timestamp"]
            if hasattr(ts, "isoformat"):
                ts = ts.isoformat()
            w.writerow([
                r["apartment"],
                r["room"],
                r["sensor_type"],
                r["field"],
                r["value"] if r["value"] is not None else "",
                r["unit"]  if r["unit"]  is not None else "",
                ts,
                "true" if r["is_outlier"] else "false",
            ])
        buf.seek(0)
        cur.copy_expert(
            "COPY _tmp_sensor_events "
            "(apartment, room, sensor_type, field, value, unit, \"timestamp\", is_outlier) "
            "FROM STDIN WITH (FORMAT CSV, NULL '')",
            buf,
        )

        cur.execute(_UPSERT_FROM_TMP)
        raw.commit()
    finally:
        raw.close()


# -- FIND NEW FILES (FAST) -----------------------------------------------------

def count_bronze_files(apt):
    """Count JSON files in a Bronze apartment folder."""
    apt_root = BRONZE_ROOT / apt
    if not apt_root.exists():
        return 0
    return sum(1 for _ in apt_root.rglob("*.json"))


def find_new_files_fast(engine, watermark_set):
    """
    For each apartment, walk Bronze folders from newest to oldest.
    Stop scanning an apartment once we hit a streak of files that
    are all in the watermark (meaning everything older is too).
    """
    STOP_AFTER = 50  # consecutive watermarked files before we stop
    all_tasks = []

    for apt in APARTMENTS:
        apt_root = BRONZE_ROOT / apt
        if not apt_root.exists():
            log.warning(f"Bronze folder not found: {apt_root}")
            continue

        # Get all year/month/day/hour folders sorted descending (newest first)
        hour_folders = sorted(apt_root.glob("*/*/*/*"), reverse=True)
        consecutive_existing = 0
        apt_new = 0

        for folder in hour_folders:
            if not folder.is_dir():
                continue

            files = sorted(folder.glob("*.json"), reverse=True)
            for f in files:
                if f.name in watermark_set:
                    consecutive_existing += 1
                    if consecutive_existing >= STOP_AFTER:
                        break
                else:
                    consecutive_existing = 0
                    all_tasks.append((str(f), apt))
                    apt_new += 1

            if consecutive_existing >= STOP_AFTER:
                break

        if apt_new > 0:
            log.info(f"[{apt}] {apt_new:,} new files to process")
        else:
            log.info(f"[{apt}] up to date")

    return all_tasks


# -- MAIN ----------------------------------------------------------------------

def run():
    if not DB_URL:
        raise EnvironmentError("DB_URL not set in .env")
    engine = create_engine(DB_URL, pool_size=WORKERS, max_overflow=4)

    print(f"\n{B}flatten_sensors -- Bronze to Silver{R}")
    print(f"{D}Bronze  : {BRONZE_ROOT.resolve()}{R}")
    print(f"{D}DB      : {DB_URL.split('@')[-1]}{R}")
    print(f"{D}Workers : {WORKERS}{R}\n")

    # Load watermark and scan newest Bronze folders for new files
    t0 = time.monotonic()
    log.info("Loading watermark...")
    watermark = load_watermark(engine)
    log.info(f"Watermark: {len(watermark):,} files already processed")

    log.info("Scanning for new files (newest first)...")
    all_tasks = find_new_files_fast(engine, watermark)

    check_time = time.monotonic() - t0
    log.info(f"Found {len(all_tasks):,} files to process in {check_time:.1f}s")

    if not all_tasks:
        print(f"\n{GR}Nothing to do.{R}\n")
        return

    batches = [all_tasks[i:i+BATCH_SIZE] for i in range(0, len(all_tasks), BATCH_SIZE)]
    log.info(f"Batches: {len(batches)} x {BATCH_SIZE} files  ({WORKERS} parallel workers)")
    log.info(f"Starting... first progress line will appear after the first batch finishes (~10-30s).")

    total_files = total_rows = total_errors = 0
    t_start = time.monotonic()

    with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(process_batch, (batch, DB_URL)): i for i, batch in enumerate(batches)}
        for n, future in enumerate(as_completed(futures), 1):
            result = future.result()
            upsert(engine, result["rows"])
            mark_done(engine, result["processed"])
            total_files  += len(result["processed"])
            total_rows   += len(result["rows"])
            total_errors += result["errors"]

            if n % LOG_EVERY == 0 or n == len(batches):
                elapsed   = time.monotonic() - t_start
                rate      = total_files / elapsed if elapsed > 0 else 1
                remaining = (len(all_tasks) - total_files) / rate
                pct       = total_files / len(all_tasks) * 100
                # Render a simple text progress bar so users see motion
                bar_w = 24
                filled = int(bar_w * pct / 100)
                bar = "█" * filled + "░" * (bar_w - filled)
                print(f"  [{bar}] batch {n:>3}/{len(batches)}  "
                      f"{total_files:>7,} files  {total_rows:>10,} rows  "
                      f"{pct:5.1f}%  {D}~{remaining/60:.1f}min left{R}")

    elapsed = time.monotonic() - t_start
    total_time = time.monotonic() - t0

    print(f"\n{B}{'_'*48}{R}")
    print(f"{GR}{B}  Done in {total_time:.0f}s (check {check_time:.1f}s + process {elapsed:.1f}s){R}")
    print(f"  {GR}v{R} {total_files:,} files  {total_rows:,} rows")
    if total_errors:
        print(f"  {RE}x {total_errors} errors{R}")
    print()


if __name__ == "__main__":
    run()