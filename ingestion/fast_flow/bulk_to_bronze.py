"""
bulk_to_bronze.py -- SMB -> Bronze (optimized)
===============================================
Copies new JSON files from SMB share to Bronze storage.

Fast mode (default): finds newest Bronze file, then predicts
the next filenames minute by minute. Checks with .exists()
instead of scanning 246k files. Stops when predicted files
don't exist (= caught up).

Full mode (--full): scans everything for first run or gaps.

Resume-capable -- skips files already in Bronze.
Source files NEVER touched.

Usage:
  python ingestion/fast_flow/bulk_to_bronze.py         # fast prediction
  python ingestion/fast_flow/bulk_to_bronze.py --full   # full scan

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import logging
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

SMB_PATH    = Path(os.getenv("SMB_PATH",    r"Z:\\"))
BRONZE_ROOT = Path(os.getenv("BRONZE_ROOT", r"storage\bronze"))
WORKERS     = 16

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bulk_to_bronze")

R="\033[0m"; B="\033[1m"; D="\033[2m"; GR="\033[32m"; RE="\033[31m"; YE="\033[33m"

APARTMENT_MAP = {"jimmyloup": "jimmy", "jeremievianin": "jeremie"}
APARTMENTS_SMB = ["JimmyLoup", "JeremieVianin"]
MAX_EMPTY_MINUTES = 10  # stop after this many consecutive minutes with no files


def identify_apartment(filename):
    lower = filename.lower()
    for key, name in APARTMENT_MAP.items():
        if key in lower:
            return name
    return None


def parse_filename_timestamp(filename):
    try:
        date_part = filename.split("_")[0].strip()
        return datetime.strptime(date_part, "%d.%m.%Y %H%M").replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


def bronze_dest(apartment, ts, filename):
    folder = BRONZE_ROOT / apartment / ts.strftime("%Y") / ts.strftime("%m") / ts.strftime("%d") / ts.strftime("%H")
    folder.mkdir(parents=True, exist_ok=True)
    return folder / filename


def copy_file(src, dst):
    try:
        shutil.copy2(str(src), str(dst))
        return "copied"
    except Exception as e:
        log.error(f"Copy failed -- {src.name}: {e}")
        return "error"


def get_newest_bronze_filename():
    """Find the newest filename in Bronze by checking newest folders."""
    newest = None
    for apt in ["jimmy", "jeremie"]:
        apt_path = BRONZE_ROOT / apt
        if not apt_path.exists():
            continue
        hour_folders = sorted(apt_path.glob("*/*/*/*"), reverse=True)
        for folder in hour_folders:
            if not folder.is_dir():
                continue
            files = sorted(folder.glob("*.json"), reverse=True)
            if files:
                name = files[0].name
                if newest is None or name > newest:
                    newest = name
                break
    return newest


def find_new_files_predict(after_filename):
    """
    Predict filenames from after_filename forward, minute by minute.
    Check .exists() on SMB for each. Stop after MAX_EMPTY_MINUTES
    consecutive minutes with no files found.
    """
    start_dt = parse_filename_timestamp(after_filename)
    if start_dt is None:
        return []

    new_files = []
    empty_streak = 0
    current_dt = start_dt + timedelta(minutes=1)
    minutes_checked = 0

    while empty_streak < MAX_EMPTY_MINUTES:
        found_this_minute = False

        for apt_smb in APARTMENTS_SMB:
            filename = f"{current_dt.strftime('%d.%m.%Y %H%M')}_{apt_smb}_received.json"
            smb_file = SMB_PATH / filename

            if smb_file.exists():
                apt_local = identify_apartment(filename)
                if apt_local:
                    dst = bronze_dest(apt_local, current_dt, filename)
                    if not dst.exists():
                        new_files.append((smb_file, dst))
                found_this_minute = True

        if found_this_minute:
            empty_streak = 0
        else:
            empty_streak += 1

        current_dt += timedelta(minutes=1)
        minutes_checked += 1

    log.info(f"Predicted {minutes_checked} minutes, found {len(new_files)} new files (stopped after {MAX_EMPTY_MINUTES} empty minutes)")
    return new_files


def find_new_files_full():
    """Full scan: os.scandir + newest-first check for first run or gaps."""
    log.info("Full scan: scanning SMB share...")
    t0 = time.monotonic()

    all_files = []
    for entry in os.scandir(SMB_PATH):
        if entry.is_file(follow_symlinks=False) and entry.name.endswith(".json"):
            all_files.append(entry.name)

    all_files.sort()
    scan_time = time.monotonic() - t0
    log.info(f"Found {len(all_files):,} files in {scan_time:.1f}s")

    if not all_files:
        return []

    log.info("Checking newest first...")
    new_files = []
    consecutive_existing = 0

    for name in reversed(all_files):
        apt = identify_apartment(name)
        if not apt:
            continue
        ts  = parse_filename_timestamp(name)
        dst = BRONZE_ROOT / apt / ts.strftime("%Y") / ts.strftime("%m") / ts.strftime("%d") / ts.strftime("%H") / name
        src = SMB_PATH / name

        if dst.exists():
            consecutive_existing += 1
            if consecutive_existing >= 50:
                break
        else:
            consecutive_existing = 0
            new_files.append((src, dst))

    log.info(f"{len(new_files):,} new files found")
    return new_files


def run():
    if not SMB_PATH.exists():
        raise FileNotFoundError(f"SMB path not found: {SMB_PATH}\nIs Z: mounted?")

    BRONZE_ROOT.mkdir(parents=True, exist_ok=True)
    full_mode = "--full" in sys.argv

    print(f"\n{B}bulk_to_bronze -- SMB -> Bronze{R}")
    print(f"{D}Source  : {SMB_PATH}{R}")
    print(f"{D}Bronze  : {BRONZE_ROOT.resolve()}{R}")
    print(f"{D}Workers : {WORKERS} parallel threads{R}")
    print(f"{D}Mode    : {'full scan' if full_mode else 'prediction'}{R}\n")

    t0 = time.monotonic()

    if full_mode:
        new_files = find_new_files_full()
    else:
        newest_bronze = get_newest_bronze_filename()
        if newest_bronze is None:
            log.info("No Bronze files found -- falling back to full scan")
            new_files = find_new_files_full()
        else:
            log.info(f"Newest in Bronze: {newest_bronze}")
            new_files = find_new_files_predict(newest_bronze)

    if not new_files:
        total = time.monotonic() - t0
        print(f"\n{GR}Nothing to copy -- Bronze is up to date ({total:.1f}s){R}\n")
        return

    # Copy new files
    copied = errors = 0
    t_copy = time.monotonic()

    print(f"{D}Copying {len(new_files):,} files ({WORKERS} threads)...{R}\n")

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(copy_file, src, dst): src for src, dst in new_files}
        for future in as_completed(futures):
            result = future.result()
            if result == "copied":
                copied += 1
            elif result == "error":
                errors += 1

    copy_time = time.monotonic() - t_copy
    total_time = time.monotonic() - t0

    print(f"\n{B}{'-'*48}{R}")
    print(f"{GR}{B}  Done in {total_time:.1f}s{R}")
    print(f"  {GR}v {copied:,} copied{R}")
    if errors:
        print(f"  {RE}x {errors} errors{R}")
    print(f"\n{D}Bronze: {BRONZE_ROOT.resolve()}{R}\n")


if __name__ == "__main__":
    run()