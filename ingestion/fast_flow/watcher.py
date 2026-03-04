"""
watcher.py -- Pipeline loop: SMB -> Bronze -> Silver
===================================================
Runs bulk_to_bronze + flatten_sensors on a fixed interval.
Skips pipeline if newest SMB file already exists in Bronze.
Both scripts are idempotent -- safe to run repeatedly.

Usage: python ingestion/fast_flow/watcher.py

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import os
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# -- CONFIG --------------------------------------------------------------------

SMB_PATH      = Path(os.getenv("SMB_PATH", r"Z:\\"))
BRONZE_ROOT   = Path(os.getenv("BRONZE_ROOT", r"storage\bronze"))
INTERVAL_SECS = 60

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

APARTMENT_MAP = {"jimmyloup": "jimmy", "jeremievianin": "jeremie"}


def get_newest_smb_file():
    """Get the newest JSON filename on the SMB share (by name, not stat)."""
    try:
        files = sorted(SMB_PATH.glob("*.json"))
        return files[-1].name if files else None
    except Exception:
        return None


def file_exists_in_bronze(filename):
    """Check if a filename exists anywhere in the Bronze folder tree."""
    bronze = PROJECT_ROOT / BRONZE_ROOT
    if not bronze.exists():
        return False
    matches = list(bronze.rglob(filename))
    return len(matches) > 0


def check_new_data():
    """Compare newest SMB file vs Bronze. Returns (has_new, newest_filename)."""
    newest = get_newest_smb_file()
    if newest is None:
        return False, None
    
    already_in_bronze = file_exists_in_bronze(newest)
    return not already_in_bronze, newest


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


# -- MAIN ----------------------------------------------------------------------

def run():
    if not SMB_PATH.exists():
        raise FileNotFoundError(
            f"SMB path not found: {SMB_PATH}\n"
            "Is Z: mounted? Check File Explorer."
        )

    print(f"\n{B}watcher -- Pipeline Loop{R}")
    print(f"{D}SMB      : {SMB_PATH}{R}")
    print(f"{D}Bronze   : {(PROJECT_ROOT / BRONZE_ROOT).resolve()}{R}")
    print(f"{D}Interval : {INTERVAL_SECS}s{R}")
    print(f"{D}Pipeline : bulk_to_bronze -> flatten_sensors{R}")
    print(f"{D}Ctrl+C to stop{R}\n")

    run_count = 0
    skip_count = 0
    pipeline_count = 0
    total_time = 0
    last_run_str = "never"

    try:
        while True:
            run_count += 1
            now = time.strftime("%H:%M:%S")

            # Quick check: is there new data?
            print(f"\n  {D}[{now}] Checking for new files...{R}", end=" ")
            t_check = time.monotonic()
            has_new, newest = check_new_data()
            check_time = time.monotonic() - t_check

            if not has_new:
                skip_count += 1
                if newest:
                    print(f"{D}latest: {newest} (already in Bronze) -- {check_time:.1f}s{R}")
                else:
                    print(f"{D}no files on SMB{R}")

                # Countdown to next check
                for remaining in range(INTERVAL_SECS, 0, -1):
                    mins, secs = divmod(remaining, 60)
                    print(
                        f"\r  {D}[{time.strftime('%H:%M:%S')}] idle -- next check in {mins:02d}:{secs:02d}"
                        f"  |  runs: {pipeline_count}  skipped: {skip_count}"
                        f"  |  last: {last_run_str}{R}   ",
                        end="", flush=True,
                    )
                    time.sleep(1)
                print()
                continue

            # New data found -- run pipeline
            print(f"{GR}NEW: {newest} -- running pipeline{R}")
            pipeline_count += 1

            print(f"\n{CY}{B}{'=' * 56}{R}")
            print(f"{CY}{B}  PIPELINE #{pipeline_count} -- {now}{R}")
            print(f"{CY}{B}{'=' * 56}{R}\n")

            elapsed = run_pipeline()
            total_time += elapsed
            last_run_str = f"{time.strftime('%H:%M:%S')} ({elapsed:.0f}s)"

            print(f"{CY}{B}{'=' * 56}{R}")
            print(f"{GR}{B}  PIPELINE #{pipeline_count} COMPLETE -- {elapsed:.0f}s{R}")
            print(f"{D}  checks: {run_count}  |  pipelines: {pipeline_count}  |  skipped: {skip_count}  |  {total_time / 60:.1f}min{R}")
            print(f"{CY}{B}{'=' * 56}{R}")

            # Countdown to next check
            for remaining in range(INTERVAL_SECS, 0, -1):
                mins, secs = divmod(remaining, 60)
                print(
                    f"\r  {D}[{time.strftime('%H:%M:%S')}] idle -- next check in {mins:02d}:{secs:02d}"
                    f"  |  runs: {pipeline_count}  skipped: {skip_count}"
                    f"  |  last: {last_run_str}{R}   ",
                    end="", flush=True,
                )
                time.sleep(1)
            print()

    except KeyboardInterrupt:
        print(f"\n\n{B}{'-' * 56}{R}")
        print(f"{D}Stopped after {run_count} checks ({pipeline_count} pipelines, {skip_count} skipped){R}")
        print(f"{D}Total pipeline time: {total_time / 60:.1f}min{R}")
        print(f"{B}{'-' * 56}{R}\n")


if __name__ == "__main__":
    run()