"""
flatten_sensors.py — Bronze to Silver: sensor_events
Parallel processing, resume-capable via watermark.
Author: Group 14 · Data Cycle Project · HES-SO Valais 2026
"""

import json
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

BRONZE_ROOT = Path(os.getenv("BRONZE_ROOT", r"storage\bronze"))
DB_URL      = os.getenv("DB_URL")
APARTMENTS  = ["jimmy", "jeremie"]
WORKERS     = 8
BATCH_SIZE  = 2000
LOG_EVERY   = 10

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("flatten_sensors")

R="\033[0m"; B="\033[1m"; D="\033[2m"; GR="\033[32m"; RE="\033[31m"

WATERMARK_DDL = """
    CREATE TABLE IF NOT EXISTS silver.etl_watermark (
        filename     VARCHAR(200) PRIMARY KEY,
        processed_at TIMESTAMPTZ DEFAULT NOW()
    );
"""

def load_watermark(engine) -> set:
    with engine.begin() as conn:
        conn.execute(text(WATERMARK_DDL))
        rows = conn.execute(text("SELECT filename FROM silver.etl_watermark")).fetchall()
    return {r[0] for r in rows}

def mark_done(engine, filenames: list):
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO silver.etl_watermark (filename) VALUES (:f) ON CONFLICT DO NOTHING"),
            [{"f": f} for f in filenames]
        )

ROOM_MAP = {
    "Bhroom": "Bathroom", "Bdroom": "Bedroom", "Livingroom": "Living Room",
    "Office": "Office", "Kitchen": "Kitchen", "Laundry": "Laundry",
    "Outdoor": "Outdoor", "House": "House",
}

def norm_room(r): return ROOM_MAP.get(r, r)

BOUNDS = {
    "temperature_c": (-20, 60), "humidity_pct": (0, 100), "co2_ppm": (300, 5000),
    "noise_db": (0, 140), "pressure_hpa": (870, 1085), "power": (0, 10000), "battery": (0, 100),
}

def is_outlier(field, value):
    if field not in BOUNDS or value is None: return False
    lo, hi = BOUNDS[field]
    return not (lo <= value <= hi)

def parse_timestamp(raw):
    try: return datetime.strptime(raw, "%d.%m.%Y %H:%M").replace(tzinfo=timezone.utc)
    except: return datetime.now(tz=timezone.utc)

def make_row(apt, room, stype, field, value, unit, ts):
    v = float(value) if value is not None else None
    return {"apartment": apt, "room": norm_room(room), "sensor_type": stype,
            "field": field, "value": v, "unit": unit, "timestamp": ts, "is_outlier": is_outlier(field, v)}

def flatten(apt, payload, ts):
    rows = []
    for room, d in payload.get("plugs", {}).items():
        for f, u in [("power","W"),("total","Wh"),("temperature","°C")]:
            if d.get(f) is not None: rows.append(make_row(apt, room, "plug", f, d[f], u, ts))

    for room, sensors in payload.get("doorsWindows", {}).items():
        if not isinstance(sensors, list): sensors = [sensors]
        for s in sensors:
            stype = s.get("type","door").lower()
            rows.append(make_row(apt, room, stype, "open", 1.0 if str(s.get("switch","off")).lower()=="on" else 0.0, "bool", ts))
            if s.get("battery") is not None: rows.append(make_row(apt, room, stype, "battery", s["battery"], "%", ts))

    for room, d in payload.get("motions", {}).items():
        for f, u in [("motion","bool"),("light","lux"),("temperature","°C")]:
            if d.get(f) is not None:
                v = 1.0 if d[f] is True else (0.0 if d[f] is False else d[f])
                rows.append(make_row(apt, room, "motion", f, v, u, ts))

    inner = payload.get("meteos", {}).get("meteo", payload.get("meteos", {}))
    for room, d in inner.items():
        for src, field, unit in [
            ("Temperature","temperature_c","°C"),("CO2","co2_ppm","ppm"),
            ("Humidity","humidity_pct","%"),("Noise","noise_db","dB"),
            ("Pressure","pressure_hpa","hPa"),("AbsolutePressure","abs_pressure_hpa","hPa"),
            ("battery_percent","battery","%"),
        ]:
            if d.get(src) is not None: rows.append(make_row(apt, room, "meteo", field, d[src], unit, ts))

    for room, d in payload.get("humidities", {}).items():
        for f, u in [("temperature","°C"),("humidity","%")]:
            if d.get(f) is not None: rows.append(make_row(apt, room, "humidity", f, d[f], u, ts))
        if d.get("devicePower") is not None: rows.append(make_row(apt, room, "humidity", "battery", d["devicePower"], "%", ts))

    for loc, d in payload.get("consumptions", {}).items():
        for f, u in [("total_power","W"),("power1","W"),("power2","W"),("power3","W"),
                     ("current1","A"),("current2","A"),("current3","A"),
                     ("voltage1","V"),("voltage2","V"),("voltage3","V")]:
            if d.get(f) is not None: rows.append(make_row(apt, loc, "consumption", f, d[f], u, ts))

    return rows

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

UPSERT_SQL = text("""
    INSERT INTO silver.sensor_events (apartment, room, sensor_type, field, value, unit, timestamp, is_outlier)
    VALUES (:apartment, :room, :sensor_type, :field, :value, :unit, :timestamp, :is_outlier)
    ON CONFLICT (apartment, room, sensor_type, field, timestamp)
    DO UPDATE SET value=EXCLUDED.value, unit=EXCLUDED.unit, is_outlier=EXCLUDED.is_outlier
""")

def upsert(engine, rows):
    if rows:
        with engine.begin() as conn:
            conn.execute(UPSERT_SQL, rows)

def run():
    if not DB_URL: raise EnvironmentError("DB_URL not set in .env")
    engine = create_engine(DB_URL, pool_size=WORKERS, max_overflow=4)

    print(f"\n{B}flatten_sensors — Bronze to Silver{R}")
    print(f"{D}Bronze  : {BRONZE_ROOT.resolve()}{R}")
    print(f"{D}DB      : {DB_URL.split('@')[-1]}{R}")
    print(f"{D}Workers : {WORKERS}{R}\n")

    watermark = load_watermark(engine)
    log.info(f"Watermark: {len(watermark):,} files already processed")

    all_tasks = []
    for apt in APARTMENTS:
        apt_root = BRONZE_ROOT / apt
        if not apt_root.exists():
            log.warning(f"Bronze folder not found: {apt_root}")
            continue
        files = sorted(apt_root.rglob("*.json"))
        log.info(f"[{apt}] {len(files):,} files in Bronze")
        for p in files:
            if p.name not in watermark:
                all_tasks.append((str(p), apt))

    log.info(f"Files to process: {len(all_tasks):,}")
    if not all_tasks:
        print(f"{GR}Nothing to do.{R}\n")
        return

    batches = [all_tasks[i:i+BATCH_SIZE] for i in range(0, len(all_tasks), BATCH_SIZE)]
    log.info(f"Batches: {len(batches)} x {BATCH_SIZE} files")

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
                print(f"  {GR}checkmark{R} {total_files:>7,} files  {total_rows:>10,} rows  {pct:.1f}%  {D}~{remaining/60:.1f}min remaining{R}")

    elapsed = time.monotonic() - t_start
    print(f"\n{B}{'─'*48}{R}")
    print(f"{GR}{B}  Done in {elapsed/60:.1f}min{R}")
    print(f"  {GR}checkmark{R} {total_files:,} files  {total_rows:,} rows")
    if total_errors: print(f"  {RE}x {total_errors} errors{R}")
    print()

if __name__ == "__main__":
    run()