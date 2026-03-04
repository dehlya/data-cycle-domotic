"""
bulk_to_bronze.py — Historical bulk load: SMB -> Bronze
=======================================================
Parallel copy — 16 threads to saturate SMB bandwidth.
Resume-capable — skips files already in Bronze.
Source files NEVER touched.

Usage: python ingestion/fast_flow/bulk_to_bronze.py
"""

import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

SMB_PATH    = Path(os.getenv("SMB_PATH",    r"Z:\\"))
BRONZE_ROOT = Path(os.getenv("BRONZE_ROOT", r"storage\bronze"))
WORKERS     = 16
LOG_EVERY   = 2000

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bulk_to_bronze")

R="\033[0m"; B="\033[1m"; D="\033[2m"; GR="\033[32m"; RE="\033[31m"; YE="\033[33m"

APARTMENT_MAP = {"jimmyloup": "jimmy", "jeremievianin": "jeremie"}

def identify_apartment(filename):
    lower = filename.lower()
    for key, name in APARTMENT_MAP.items():
        if key in lower: return name
    return None

def parse_filename_timestamp(filename):
    try:
        date_part = filename.split("_")[0].strip()
        return datetime.strptime(date_part, "%d.%m.%Y %H%M").replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)

def bronze_path(apartment, ts, filename):
    folder = BRONZE_ROOT / apartment / ts.strftime("%Y") / ts.strftime("%m") / ts.strftime("%d") / ts.strftime("%H")
    folder.mkdir(parents=True, exist_ok=True)
    return folder / filename

def copy_file(path):
    apt = identify_apartment(path.name)
    if not apt: return "unknown"
    ts  = parse_filename_timestamp(path.name)
    dst = bronze_path(apt, ts, path.name)
    if dst.exists(): return "skipped"
    try:
        shutil.copy2(str(path), str(dst))
        return "copied"
    except Exception as e:
        log.error(f"Copy failed — {path.name}: {e}")
        return "error"

def run():
    if not SMB_PATH.exists():
        raise FileNotFoundError(f"SMB path not found: {SMB_PATH}\nIs Z: mounted?")

    BRONZE_ROOT.mkdir(parents=True, exist_ok=True)

    print(f"\n{B}bulk_to_bronze — SMB -> Bronze{R}")
    print(f"{D}Source  : {SMB_PATH}{R}")
    print(f"{D}Bronze  : {BRONZE_ROOT.resolve()}{R}")
    print(f"{D}Workers : {WORKERS} parallel threads{R}\n")

    log.info("Scanning SMB share...")
    t0 = time.monotonic()
    all_files = sorted(SMB_PATH.glob("*.json"))
    log.info(f"Found {len(all_files):,} files in {time.monotonic()-t0:.1f}s")

    if not all_files:
        log.warning("No JSON files found.")
        return

    copied = skipped = errors = unknown = done = 0
    t_start = time.monotonic()

    print(f"{D}Starting parallel copy ({WORKERS} threads)...{R}\n")

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(copy_file, p): p for p in all_files}
        for future in as_completed(futures):
            result = future.result()
            done += 1
            if result == "copied":   copied  += 1
            elif result == "skipped": skipped += 1
            elif result == "error":   errors  += 1
            elif result == "unknown": unknown += 1

            if done % LOG_EVERY == 0:
                elapsed   = time.monotonic() - t_start
                rate      = done / elapsed
                remaining = (len(all_files) - done) / rate if rate > 0 else 0
                pct       = done / len(all_files) * 100
                print(
                    f"  {GR}✓{R} {copied:>7,} copied  "
                    f"{D}{skipped:>7,} skipped  "
                    f"{errors:>4} errors  "
                    f"{pct:.1f}%  ~{remaining/60:.1f}min remaining{R}"
                )

    elapsed = time.monotonic() - t_start
    print(f"\n{B}{'─'*48}{R}")
    print(f"{GR}{B}  Done in {elapsed/60:.1f}min{R}")
    print(f"  {GR}✓ {copied:,} copied{R}  {D}{skipped:,} skipped{R}")
    if errors:  print(f"  {RE}✗ {errors} errors{R}")
    if unknown: print(f"  {YE}? {unknown} unknown{R}")
    print(f"\n{D}Bronze: {BRONZE_ROOT.resolve()}{R}\n")

if __name__ == "__main__":
    run()