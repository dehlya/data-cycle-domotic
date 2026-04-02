"""
watcher.py -- Pipeline loop: SMB -> Bronze -> Silver + Weather
==============================================================
Fast flow: Predicts the next expected filenames based on the last known file.
           Checks with a single .exists() call -- milliseconds, no scanning.
Slow flow: Daily weather pipeline (sFTP download + Bronze -> Silver cleaning).
Nightly:   Full os.scandir at midnight to catch missed files.

Usage: python ingestion/fast_flow/watcher.py
       python ingestion/fast_flow/watcher.py --scan      (full SMB scan + pipeline, then exit)
       python ingestion/fast_flow/watcher.py --weather   (run weather once and exit)

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# -- CONFIG --------------------------------------------------------------------

SMB_PATH      = Path(os.getenv("SMB_PATH", r"Z:\\"))
BRONZE_ROOT   = Path(os.getenv("BRONZE_ROOT", r"storage\bronze"))
INTERVAL_SECS = 60
NIGHTLY_HOUR  = 0  # midnight
WEATHER_HOUR  = int(os.getenv("WEATHER_HOUR", "7"))   # hour to trigger weather
WEATHER_MIN   = int(os.getenv("WEATHER_MIN", "30"))    # minute to trigger weather

# -- ANSI COLORS ---------------------------------------------------------------

R  = "\033[0m"
B  = "\033[1m"
D  = "\033[2m"
GR = "\033[32m"
RE = "\033[31m"
YE = "\033[33m"
CY = "\033[36m"

# -- PIPELINE ------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

APARTMENTS_SMB = ["JimmyLoup", "JeremieVianin"]


def parse_filename_to_dt(filename):
    """Parse '31.08.2023 2144_JimmyLoup_received.json' -> datetime."""
    try:
        date_part = filename.split("_")[0].strip()
        return datetime.strptime(date_part, "%d.%m.%Y %H%M").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def dt_to_filename(dt, apartment):
    """datetime -> '31.08.2023 2145_JimmyLoup_received.json'"""
    return f"{dt.strftime('%d.%m.%Y %H%M')}_{apartment}_received.json"


def predict_next_files(last_filename):
    """
    Given the last known filename, predict the next expected files.
    Files come every minute, two apartments. Returns list of predicted names.
    """
    dt = parse_filename_to_dt(last_filename)
    if dt is None:
        return []

    next_dt = dt + timedelta(minutes=1)
    return [dt_to_filename(next_dt, apt) for apt in APARTMENTS_SMB]


def check_predicted(predicted_files):
    """Check if any predicted files exist on SMB. Returns list of found files."""
    found = []
    for name in predicted_files:
        if (SMB_PATH / name).exists():
            found.append(name)
    return found


def get_newest_bronze_filename():
    """Find the newest filename in Bronze by checking newest folders."""
    bronze = PROJECT_ROOT / BRONZE_ROOT
    if not bronze.exists():
        return None
    newest = None
    for apt in ["jimmy", "jeremie"]:
        apt_path = bronze / apt
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


def get_newest_smb_filename():
    """Full os.scandir pass -- used only for nightly safety check."""
    newest = None
    try:
        for entry in os.scandir(SMB_PATH):
            if entry.is_file(follow_symlinks=False) and entry.name.endswith(".json"):
                if newest is None or entry.name > newest:
                    newest = entry.name
    except Exception as e:
        print(f"  {RE}SMB scan error: {e}{R}")
    return newest


def run_pipeline():
    """Run all pipeline steps in sequence. Returns elapsed seconds."""
    t_start = time.monotonic()

    steps = [
        ("bulk_to_bronze",  PROJECT_ROOT / "ingestion" / "fast_flow" / "bulk_to_bronze.py",  "SMB -> Bronze"),
        ("flatten_sensors", PROJECT_ROOT / "etl" / "bronze_to_silver" / "flatten_sensors.py", "Bronze -> Silver"),
    ]

    for name, script, desc in steps:
        if not script.exists():
            print(f"  {RE}x {name} -- script not found: {script}{R}")
            continue

        print(f"  {YE}>{R} {name} -- {desc}")
        try:
            result = subprocess.run(
                [sys.executable, "-u", str(script)],
                cwd=str(PROJECT_ROOT),
                timeout=7200,
            )
            if result.returncode == 0:
                print(f"  {GR}v{R} {name} done\n")
            else:
                print(f"  {RE}x {name} exited with code {result.returncode}{R}\n")
        except subprocess.TimeoutExpired:
            print(f"  {RE}x {name} timed out (2h){R}\n")
        except Exception as e:
            print(f"  {RE}x {name} error: {e}{R}\n")

    elapsed = time.monotonic() - t_start
    return elapsed


def run_weather_pipeline():
    """Run the slow flow: weather download from sFTP then clean into Silver."""
    t_start = time.monotonic()

    steps = [
        ("weather_download", PROJECT_ROOT / "ingestion" / "slow_flow" / "weather_download.py", "sFTP -> Bronze"),
        ("clean_weather",    PROJECT_ROOT / "etl" / "bronze_to_silver" / "clean_weather.py",   "Bronze -> Silver"),
    ]

    for name, script, desc in steps:
        if not script.exists():
            print(f"  {RE}x {name} -- script not found: {script}{R}")
            continue

        print(f"  {YE}>{R} {name} -- {desc}")
        try:
            result = subprocess.run(
                [sys.executable, "-u", str(script)],
                cwd=str(PROJECT_ROOT),
                timeout=3600,
            )
            if result.returncode == 0:
                print(f"  {GR}v{R} {name} done\n")
            else:
                print(f"  {RE}x {name} exited with code {result.returncode}{R}\n")
        except subprocess.TimeoutExpired:
            print(f"  {RE}x {name} timed out (1h){R}\n")
        except Exception as e:
            print(f"  {RE}x {name} error: {e}{R}\n")

    elapsed = time.monotonic() - t_start
    return elapsed


# -- MAIN ----------------------------------------------------------------------

def run():
    # Manual triggers
    if "--weather" in sys.argv:
        print(f"\n{B}watcher -- Manual Weather Pipeline{R}\n")
        elapsed = run_weather_pipeline()
        print(f"\n{GR}{B}  Weather pipeline done in {elapsed:.0f}s{R}\n")
        return

    if "--scan" in sys.argv:
        print(f"\n{B}watcher -- Manual Full Scan{R}\n")
        newest_smb = get_newest_smb_filename()
        if newest_smb:
            print(f"  {GR}Newest on SMB: {newest_smb}{R}\n")
        elapsed = run_pipeline()
        print(f"\n{GR}{B}  Pipeline done in {elapsed:.0f}s{R}\n")
        return

    if not SMB_PATH.exists():
        raise FileNotFoundError(
            f"SMB path not found: {SMB_PATH}\n"
            "Is Z: mounted? Check File Explorer."
        )

    print(f"\n{B}watcher -- Pipeline Loop{R}")
    print(f"{D}SMB      : {SMB_PATH}{R}")
    print(f"{D}Bronze   : {(PROJECT_ROOT / BRONZE_ROOT).resolve()}{R}")
    print(f"{D}Interval : {INTERVAL_SECS}s{R}")
    print(f"{D}Fast flow: bulk_to_bronze -> flatten_sensors{R}")
    print(f"{D}Slow flow: weather_download -> clean_weather (daily at {WEATHER_HOUR:02d}:{WEATHER_MIN:02d}){R}")
    print(f"{D}Nightly  : full scan at {NIGHTLY_HOUR:02d}:00{R}")
    print(f"{D}Flags    : --scan (full scan + pipeline) | --weather (weather only){R}")
    print(f"{D}Ctrl+C to stop{R}\n")

    # Find starting point
    print(f"  {D}Finding newest Bronze file...{R}", end=" ")
    last_known = get_newest_bronze_filename()
    if last_known:
        print(f"{GR}{last_known}{R}")
    else:
        print(f"{YE}none -- first run will do full scan{R}")

    run_count = 0
    skip_count = 0
    pipeline_count = 0
    weather_count = 0
    total_time = 0
    last_run_str = "never"
    nightly_done_today = False
    weather_done_today = False

    try:
        while True:
            run_count += 1
            now = time.strftime("%H:%M:%S")
            current_hour = int(time.strftime("%H"))

            # Reset daily flags
            if current_hour == NIGHTLY_HOUR + 1:
                nightly_done_today = False
            if current_hour == WEATHER_HOUR + 1:
                weather_done_today = False

            # Nightly safety scan
            if current_hour == NIGHTLY_HOUR and not nightly_done_today:
                nightly_done_today = True
                print(f"\n  {YE}[{now}] NIGHTLY SCAN -- full os.scandir check{R}", end=" ", flush=True)
                t_check = time.monotonic()
                newest_smb = get_newest_smb_filename()
                check_time = time.monotonic() - t_check

                if newest_smb and (last_known is None or newest_smb > last_known):
                    print(f"{GR}found newer: {newest_smb} ({check_time:.0f}s) -- running pipeline{R}")
                    # Run pipeline
                    pipeline_count += 1
                    print(f"\n{CY}{B}{'=' * 56}{R}")
                    print(f"{CY}{B}  PIPELINE #{pipeline_count} (nightly) -- {now}{R}")
                    print(f"{CY}{B}{'=' * 56}{R}\n")

                    elapsed = run_pipeline()
                    total_time += elapsed
                    last_known = get_newest_bronze_filename()
                    last_run_str = f"{time.strftime('%H:%M:%S')} ({elapsed:.0f}s, nightly)"

                    print(f"{CY}{B}{'=' * 56}{R}")
                    print(f"{GR}{B}  PIPELINE #{pipeline_count} COMPLETE -- {elapsed:.0f}s{R}")
                    if last_known:
                        print(f"{D}  newest: {last_known}{R}")
                    print(f"{CY}{B}{'=' * 56}{R}")
                else:
                    print(f"{D}all caught up ({check_time:.0f}s){R}")

                # Continue to normal countdown
                for remaining in range(INTERVAL_SECS, 0, -1):
                    mins, secs = divmod(remaining, 60)
                    print(
                        f"\r  {D}[{time.strftime('%H:%M:%S')}] idle -- next in {mins:02d}:{secs:02d}"
                        f"  |  pipelines: {pipeline_count}  weather: {weather_count}  skipped: {skip_count}"
                        f"  |  last: {last_run_str}{R}   ",
                        end="", flush=True,
                    )
                    time.sleep(1)
                print()
                continue

            # Daily weather pipeline
            current_min = int(time.strftime("%M"))
            if current_hour == WEATHER_HOUR and current_min >= WEATHER_MIN and not weather_done_today:
                weather_done_today = True
                weather_count += 1
                print(f"\n{CY}{B}{'=' * 56}{R}")
                print(f"{CY}{B}  WEATHER #{weather_count} -- {now}{R}")
                print(f"{CY}{B}{'=' * 56}{R}\n")

                elapsed = run_weather_pipeline()

                print(f"{CY}{B}{'=' * 56}{R}")
                print(f"{GR}{B}  WEATHER #{weather_count} COMPLETE -- {elapsed:.0f}s{R}")
                print(f"{CY}{B}{'=' * 56}{R}")

            # Normal cycle: predict next files
            if last_known is None:
                # No baseline -- need full scan first time
                print(f"\n  {YE}[{now}] No baseline -- full scan{R}", end=" ", flush=True)
                t_check = time.monotonic()
                newest_smb = get_newest_smb_filename()
                check_time = time.monotonic() - t_check

                if newest_smb:
                    print(f"{GR}{newest_smb} ({check_time:.0f}s){R}")
                    pipeline_count += 1
                    print(f"\n{CY}{B}{'=' * 56}{R}")
                    print(f"{CY}{B}  PIPELINE #{pipeline_count} -- {now}{R}")
                    print(f"{CY}{B}{'=' * 56}{R}\n")

                    elapsed = run_pipeline()
                    total_time += elapsed
                    last_known = get_newest_bronze_filename()
                    last_run_str = f"{time.strftime('%H:%M:%S')} ({elapsed:.0f}s)"

                    print(f"{CY}{B}{'=' * 56}{R}")
                    print(f"{GR}{B}  PIPELINE #{pipeline_count} COMPLETE -- {elapsed:.0f}s{R}")
                    print(f"{CY}{B}{'=' * 56}{R}")
                else:
                    print(f"{D}no files on SMB{R}")
            else:
                # Predict and check
                predicted = predict_next_files(last_known)
                print(f"\n  {D}[{now}] Checking {predicted[0][:20]}...{R}", end=" ", flush=True)
                t_check = time.monotonic()
                found = check_predicted(predicted)
                check_time = time.monotonic() - t_check

                if found:
                    print(f"{GR}+{len(found)} new ({check_time:.2f}s){R}")

                    pipeline_count += 1
                    print(f"\n{CY}{B}{'=' * 56}{R}")
                    print(f"{CY}{B}  PIPELINE #{pipeline_count} -- {now}{R}")
                    print(f"{CY}{B}{'=' * 56}{R}\n")

                    elapsed = run_pipeline()
                    total_time += elapsed
                    last_known = get_newest_bronze_filename()
                    last_run_str = f"{time.strftime('%H:%M:%S')} ({elapsed:.0f}s)"

                    print(f"{CY}{B}{'=' * 56}{R}")
                    print(f"{GR}{B}  PIPELINE #{pipeline_count} COMPLETE -- {elapsed:.0f}s{R}")
                    print(f"{D}  checks: {run_count}  |  pipelines: {pipeline_count}  |  skipped: {skip_count}{R}")
                    if last_known:
                        print(f"{D}  newest: {last_known}{R}")
                    print(f"{CY}{B}{'=' * 56}{R}")
                else:
                    skip_count += 1
                    print(f"{D}not yet ({check_time:.2f}s){R}")

            # Countdown
            for remaining in range(INTERVAL_SECS, 0, -1):
                mins, secs = divmod(remaining, 60)
                print(
                    f"\r  {D}[{time.strftime('%H:%M:%S')}] idle -- next in {mins:02d}:{secs:02d}"
                    f"  |  pipelines: {pipeline_count}  weather: {weather_count}  skipped: {skip_count}"
                    f"  |  last: {last_run_str}{R}   ",
                    end="", flush=True,
                )
                time.sleep(1)
            print()

    except KeyboardInterrupt:
        print(f"\n\n{B}{'-' * 56}{R}")
        print(f"{D}Stopped after {run_count} checks ({pipeline_count} pipelines, {weather_count} weather, {skip_count} skipped){R}")
        print(f"{D}Total pipeline time: {total_time / 60:.1f}min{R}")
        print(f"{B}{'-' * 56}{R}\n")


if __name__ == "__main__":
    run()