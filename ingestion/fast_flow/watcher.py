"""
fast_flow.py — Fast flow: SMB → Bronze
=======================================
Watches Z:\\ for new JSON files (one per minute per apartment).
On each new file:
  1. Identifies the apartment from the filename
  2. Copies it to Bronze with YYYY/MM/DD/HH/ structure (source untouched)
  3. Parses and flattens the JSON
  4. Pretty-prints a summary to the terminal

No database required yet — Bronze only, dry run for validation.

Usage:
  python ingestion/fast_flow.py

Author: Group 14 · Data Cycle Project · HES-SO Valais 2026
"""

import json
import logging
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────

SMB_PATH    = Path(os.getenv("SMB_PATH",    r"Z:\\"))
BRONZE_ROOT = Path(os.getenv("BRONZE_ROOT", r"storage\bronze"))

# ── LOGGING ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fast_flow")

# ── ANSI COLORS ───────────────────────────────────────────────────────────────

R  = "\033[0m"
B  = "\033[1m"
D  = "\033[2m"
GR = "\033[32m"
BL = "\033[34m"
YE = "\033[33m"
CY = "\033[36m"
RE = "\033[31m"
WH = "\033[97m"

APT_COLOR    = {"jimmy": BL, "jeremie": GR}
SENSOR_COLOR = {
    "plug": YE, "door": CY, "window": CY,
    "motion": WH, "meteo": GR, "humidity": BL, "consumption": RE,
}

# ── APARTMENT IDENTIFICATION ──────────────────────────────────────────────────

APARTMENT_MAP = {
    "jimmyloup":     "jimmy",
    "jeremievianin": "jeremie",
}

def identify_apartment(filename: str) -> str | None:
    lower = filename.lower()
    for key, name in APARTMENT_MAP.items():
        if key in lower:
            return name
    return None

# ── BRONZE PATH ───────────────────────────────────────────────────────────────

def bronze_path(apartment: str, ts: datetime, filename: str) -> Path:
    """
    bronze/<apartment>/YYYY/MM/DD/HH/<filename>
    """
    folder = (
        BRONZE_ROOT
        / apartment
        / ts.strftime("%Y")
        / ts.strftime("%m")
        / ts.strftime("%d")
        / ts.strftime("%H")
    )
    folder.mkdir(parents=True, exist_ok=True)
    return folder / filename

# ── TIMESTAMP PARSING ─────────────────────────────────────────────────────────

def parse_timestamp(raw: str) -> datetime:
    try:
        return datetime.strptime(raw, "%d.%m.%Y %H:%M").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return datetime.now(tz=timezone.utc)

# ── ROOM NORMALISATION ────────────────────────────────────────────────────────

ROOM_MAP = {
    "Bhroom": "Bathroom", "Bdroom": "Bedroom",
    "Livingroom": "Living Room", "Office": "Office",
    "Kitchen": "Kitchen", "Laundry": "Laundry",
    "Outdoor": "Outdoor", "House": "House",
}

def norm_room(r: str) -> str:
    return ROOM_MAP.get(r, r)

# ── OUTLIER DETECTION ─────────────────────────────────────────────────────────

BOUNDS = {
    "temperature_c": (-20, 60), "humidity_pct": (0, 100),
    "co2_ppm": (300, 5000),     "noise_db": (0, 140),
    "pressure_hpa": (870, 1085),"power": (0, 10000),
    "battery": (0, 100),
}

def is_outlier(field: str, value) -> bool:
    if field not in BOUNDS or value is None:
        return False
    lo, hi = BOUNDS[field]
    return not (lo <= value <= hi)

# ── FLATTENERS ────────────────────────────────────────────────────────────────

def make_row(apt, room, stype, field, value, unit, ts):
    v = float(value) if value is not None else None
    return {
        "apartment": apt, "room": norm_room(room),
        "sensor_type": stype, "field": field,
        "value": v, "unit": unit, "timestamp": ts,
        "is_outlier": is_outlier(field, v),
    }

def flatten(apt: str, payload: dict, ts: datetime) -> list[dict]:
    rows = []

    for room, d in payload.get("plugs", {}).items():
        for f, u in [("power","W"),("total","Wh"),("temperature","°C")]:
            if d.get(f) is not None:
                rows.append(make_row(apt, room, "plug", f, d[f], u, ts))

    for room, sensors in payload.get("doorsWindows", {}).items():
        if not isinstance(sensors, list): sensors = [sensors]
        for s in sensors:
            stype = s.get("type","door").lower()
            rows.append(make_row(apt, room, stype, "open",
                1.0 if str(s.get("switch","off")).lower()=="on" else 0.0, "bool", ts))
            if s.get("battery") is not None:
                rows.append(make_row(apt, room, stype, "battery", s["battery"], "%", ts))

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
            ("Pressure","pressure_hpa","hPa"),("battery_percent","battery","%"),
        ]:
            if d.get(src) is not None:
                rows.append(make_row(apt, room, "meteo", field, d[src], unit, ts))

    for room, d in payload.get("humidities", {}).items():
        for f, u in [("temperature","°C"),("humidity","%")]:
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

# ── PRETTY PRINT ──────────────────────────────────────────────────────────────

def print_summary(apt: str, filename: str, dst: Path, ts: datetime, rows: list[dict]):
    ac = APT_COLOR.get(apt, WH)
    outliers = [r for r in rows if r["is_outlier"]]

    print(f"\n{ac}{B}{'─'*64}{R}")
    print(f"{ac}{B}  {apt.upper()}{R}  {D}{filename}{R}")
    print(f"{D}  {ts.strftime('%Y-%m-%d %H:%M UTC')}  ·  {len(rows)} readings{R}")
    print(f"{ac}{B}{'─'*64}{R}")

    by_type: dict[str, list] = {}
    for r in rows:
        by_type.setdefault(r["sensor_type"], []).append(r)

    for stype, type_rows in by_type.items():
        sc = SENSOR_COLOR.get(stype, WH)
        print(f"\n  {sc}{B}{stype.upper()}{R}")

        by_room: dict[str, list] = {}
        for r in type_rows:
            by_room.setdefault(r["room"], []).append(r)

        for room, rrows in by_room.items():
            parts = []
            for r in rrows:
                if r["value"] is None:
                    continue
                flag = f"  {RE}⚠{R}" if r["is_outlier"] else ""
                parts.append(
                    f"{D}{r['field']}:{R} "
                    f"{RE if r['is_outlier'] else WH}"
                    f"{r['value']:.1f}{r['unit']}{R}{flag}"
                )
            if parts:
                print(f"    {D}{room:<16}{R} {'  '.join(parts)}")

    print(f"\n  {GR}✓ Bronze → {D}{dst}{R}")
    if outliers:
        print(f"  {RE}{B}⚠  {len(outliers)} outlier(s){R}")
    print()

# ── EVENT HANDLER ─────────────────────────────────────────────────────────────

class ApartmentHandler(FileSystemEventHandler):

    def on_created(self, event):
        if event.is_directory:
            return

        path = Path(event.src_path)
        if path.suffix.lower() != ".json":
            return

        time.sleep(1)  # let SMB finish writing

        apt = identify_apartment(path.name)
        if not apt:
            log.warning(f"Unknown apartment: {path.name}")
            return

        try:
            with open(path, encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as e:
            log.error(f"Read failed — {path.name}: {e}")
            return

        ts  = parse_timestamp(payload.get("datetime", ""))
        dst = bronze_path(apt, ts, path.name)

        try:
            shutil.copy2(str(path), str(dst))
        except Exception as e:
            log.error(f"Bronze copy failed: {e}")
            return

        rows = flatten(apt, payload, ts)
        print_summary(apt, path.name, dst, ts, rows)

# ── MAIN ──────────────────────────────────────────────────────────────────────

def run():
    if not SMB_PATH.exists():
        raise FileNotFoundError(
            f"SMB path not found: {SMB_PATH}\n"
            "Is Z: mounted? Check File Explorer."
        )

    BRONZE_ROOT.mkdir(parents=True, exist_ok=True)

    print(f"\n{B}fast_flow — SMB → Bronze{R}")
    print(f"{D}Source  : {SMB_PATH}{R}")
    print(f"{D}Bronze  : {BRONZE_ROOT.resolve()}{R}")
    print(f"{D}Polling every 10s — Ctrl+C to stop{R}\n")

    handler  = ApartmentHandler()
    observer = PollingObserver(timeout=10)
    observer.schedule(handler, path=str(SMB_PATH), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n{D}Stopping...{R}")
        observer.stop()

    observer.join()
    print(f"{D}Done.{R}\n")


if __name__ == "__main__":
    run()