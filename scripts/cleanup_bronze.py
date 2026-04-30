"""
cleanup_bronze.py -- Delete bronze files already imported to silver
====================================================================
Reads silver watermarks (silver.etl_watermark for sensor JSON,
silver.weather_watermark for weather CSV) and deletes the corresponding
bronze files when they are older than BRONZE_RETENTION_DAYS.

This makes bronze a bounded buffer instead of an immutable archive.
Trade-off: lose the ability to re-derive silver from bronze for files
older than the retention window. Source data (SMB, sFTP, MySQL) is
still available for full reprocessing if needed.

Usage:
    python scripts/cleanup_bronze.py            # delete eligible files
    python scripts/cleanup_bronze.py --dry-run  # report only, delete nothing

Config (.env):
    BRONZE_RETENTION_DAYS   default 30. Set to -1 to disable cleanup entirely.
                            Files are only deleted if their watermark
                            processed_at is older than this many days.

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DB_URL      = os.getenv("DB_URL")
BRONZE_ROOT = Path(os.getenv("BRONZE_ROOT", "storage/bronze"))
if not BRONZE_ROOT.is_absolute():
    BRONZE_ROOT = PROJECT_ROOT / BRONZE_ROOT
RETENTION_DAYS = int(os.getenv("BRONZE_RETENTION_DAYS", "30"))


# ── ANSI ──────────────────────────────────────────────────────────────────────
RESET="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"
GREEN="\033[32m"; RED="\033[31m"; YELLOW="\033[33m"; BLUE="\033[36m"

if not sys.stdout.isatty():
    RESET = BOLD = DIM = GREEN = RED = YELLOW = BLUE = ""


def header(msg):  print(f"\n{BOLD}{BLUE}== {msg} =={RESET}")
def ok(msg):      print(f"  {GREEN}\u2713{RESET} {msg}")
def warn(msg):    print(f"  {YELLOW}\u26a0{RESET} {msg}")
def fail(msg):    print(f"  {RED}\u2717{RESET} {msg}")


# ── HELPERS ───────────────────────────────────────────────────────────────────
def parse_sensor_filename_date(filename: str):
    """Sensor JSON file: '31.08.2023 2144_JimmyLoup_received.json' -> date."""
    try:
        date_part = filename.split("_")[0].strip()
        return datetime.strptime(date_part, "%d.%m.%Y %H%M").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def find_bronze_path(filename: str) -> Path | None:
    """Locate a bronze file by filename. Returns path or None."""
    if filename.endswith(".json"):
        # Sensor JSON: bronze/<apt>/YYYY/MM/DD/HH/filename
        dt = parse_sensor_filename_date(filename)
        if dt is None: return None
        apt_lower = filename.lower()
        apt = "jimmy" if "jimmyloup" in apt_lower else ("jeremie" if "jeremievianin" in apt_lower else None)
        if apt is None: return None
        candidate = (BRONZE_ROOT / apt
                     / dt.strftime("%Y") / dt.strftime("%m")
                     / dt.strftime("%d") / dt.strftime("%H") / filename)
        return candidate if candidate.exists() else None

    if filename.endswith(".csv"):
        # Weather CSV: bronze/weather/YYYY/MM/DD/Pred_YYYY-MM-DD.csv
        # Filename contains the date
        try:
            base = filename.replace("Pred_", "").replace(".csv", "")
            yy, mm, dd = base.split("-")
            candidate = BRONZE_ROOT / "weather" / yy / mm / dd / filename
            return candidate if candidate.exists() else None
        except Exception:
            return None
    return None


# ── CLEANUP ───────────────────────────────────────────────────────────────────
PROCESSED_LOG = BRONZE_ROOT.parent / "processed.log"


def append_to_processed_log(filenames: list[str]):
    """Append filenames to processed.log so the watcher knows not to re-copy
    them on its next scan. Idempotent — duplicates are filtered later by
    callers that read this file as a set."""
    if not filenames:
        return
    PROCESSED_LOG.parent.mkdir(parents=True, exist_ok=True)
    with PROCESSED_LOG.open("a", encoding="utf-8") as f:
        for name in filenames:
            f.write(f"{name}\n")


def cleanup_from_watermark(engine, table: str, dry_run: bool, cutoff_days: int):
    """Delete bronze files listed in `silver.<table>` (filename, processed_at)
    when processed_at is older than `cutoff_days`. Also records each deleted
    filename in processed.log so the next watcher scan doesn't re-copy it."""
    rows = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT filename, processed_at FROM silver.{table} "
                f"WHERE processed_at < NOW() - INTERVAL '{cutoff_days} days'"
            )).fetchall()
    except Exception as e:
        warn(f"silver.{table}: {str(e)[:80]}")
        return 0, 0, 0

    if not rows:
        ok(f"silver.{table}: nothing eligible (no files older than {cutoff_days} days)")
        return 0, 0, 0

    deleted = missing = errors = 0
    deleted_names: list[str] = []
    for filename, _processed_at in rows:
        path = find_bronze_path(filename)
        if path is None:
            # Already gone from bronze — still record in processed.log so
            # the watcher's nightly full-scan doesn't think it's "new".
            missing += 1
            deleted_names.append(filename)
            continue
        if dry_run:
            deleted += 1
            continue
        try:
            path.unlink()
            deleted += 1
            deleted_names.append(filename)
        except Exception as e:
            errors += 1
            warn(f"  could not delete {path.name}: {str(e)[:60]}")

    if not dry_run:
        append_to_processed_log(deleted_names)

    label = "would delete" if dry_run else "deleted"
    ok(f"silver.{table}: {label} {deleted}, missing {missing}, errors {errors}  (of {len(rows)} eligible)")
    if not dry_run and deleted_names:
        ok(f"  appended {len(deleted_names)} filenames to {PROCESSED_LOG.name}")
    return deleted, missing, errors


def remove_empty_dirs(root: Path, dry_run: bool):
    """Walk bronze and remove empty directories left after file deletion."""
    if not root.exists():
        return 0
    removed = 0
    # Walk bottom-up so we delete leaf folders first
    for p in sorted(root.rglob("*"), key=lambda x: -len(str(x))):
        if p.is_dir():
            try:
                if not any(p.iterdir()):
                    if dry_run:
                        removed += 1
                    else:
                        p.rmdir()
                        removed += 1
            except Exception:
                pass
    label = "would remove" if dry_run else "removed"
    ok(f"empty directories: {label} {removed}")
    return removed


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    dry_run = "--dry-run" in sys.argv
    print(f"\n{BOLD}{BLUE}cleanup_bronze{RESET}  "
          f"{DIM}({'DRY RUN' if dry_run else 'live'}){RESET}\n")

    if not DB_URL:
        sys.exit("DB_URL not set in .env")
    if RETENTION_DAYS < 0:
        print(f"{YELLOW}BRONZE_RETENTION_DAYS = {RETENTION_DAYS}{RESET}  -> cleanup disabled, exiting.\n")
        return

    print(f"  Bronze root         : {BRONZE_ROOT}")
    print(f"  Retention window    : {RETENTION_DAYS} days")
    print(f"  Source of truth     : silver.etl_watermark + silver.weather_watermark")

    engine = create_engine(DB_URL)
    total_deleted = total_missing = total_errors = 0

    header("Sensors (silver.etl_watermark)")
    d, m, e = cleanup_from_watermark(engine, "etl_watermark", dry_run, RETENTION_DAYS)
    total_deleted += d; total_missing += m; total_errors += e

    header("Weather (silver.weather_watermark)")
    d, m, e = cleanup_from_watermark(engine, "weather_watermark", dry_run, RETENTION_DAYS)
    total_deleted += d; total_missing += m; total_errors += e

    header("Empty folders")
    remove_empty_dirs(BRONZE_ROOT, dry_run)

    engine.dispose()

    print()
    print(f"{BOLD}Total{RESET}: {total_deleted} {'would be ' if dry_run else ''}deleted, "
          f"{total_missing} missing (already gone), {total_errors} errors")
    print()


if __name__ == "__main__":
    main()
