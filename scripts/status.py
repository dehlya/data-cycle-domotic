"""
status.py -- Pipeline health check
===================================
Quick local status report. Run anytime to see if the pipeline is healthy.

Usage:
    python scripts/status.py

Shows:
    - Last gold ETL run (timestamp from latest fact_environment_minute row)
    - Row counts in all key gold tables
    - Watcher process status (PID + running?)
    - Bronze folder size + newest file
    - Recent install.log lines

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DB_URL      = os.getenv("DB_URL", "")
BRONZE_ROOT = Path(os.getenv("BRONZE_ROOT", "storage/bronze"))
if not BRONZE_ROOT.is_absolute():
    BRONZE_ROOT = PROJECT_ROOT / BRONZE_ROOT


# ── ANSI ──────────────────────────────────────────────────────────────────────
RESET="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"
GREEN="\033[32m"; RED="\033[31m"; YELLOW="\033[33m"; BLUE="\033[36m"

if not sys.stdout.isatty():
    RESET = BOLD = DIM = GREEN = RED = YELLOW = BLUE = ""


def header(text):
    print(f"\n{BOLD}{BLUE}== {text} =={RESET}")


def row(label, value, status=""):
    color = GREEN if status == "ok" else (RED if status == "fail" else (YELLOW if status == "warn" else ""))
    end_color = RESET if color else ""
    print(f"  {label:30s} {color}{value}{end_color}")


# ── DB CHECKS ─────────────────────────────────────────────────────────────────
def db_status():
    header("Database")
    if not DB_URL:
        row("DB_URL", "NOT SET in .env", "fail")
        return

    p = urlparse(DB_URL)
    row("Host", f"{p.hostname}:{p.port or 5432}")
    row("Database", (p.path or "/").lstrip("/"))
    row("App user", unquote(p.username or ""))

    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            row("Connection", "OK", "ok")
            check_row_counts(conn)
            check_freshness(conn)
        engine.dispose()
    except Exception as e:
        row("Connection", f"FAILED: {str(e)[:80]}", "fail")


def check_row_counts(conn):
    header("Gold tables")
    tables = [
        "dim_apartment", "dim_room", "dim_device", "dim_date", "dim_datetime",
        "dim_tariff", "dim_weather_site",
        "fact_environment_minute", "fact_energy_minute", "fact_presence_minute",
        "fact_device_health_day", "fact_weather_hour",
    ]
    for t in tables:
        try:
            n = conn.execute(text(f"SELECT COUNT(*) FROM gold.{t}")).scalar()
            status = "ok" if n and n > 0 else "warn"
            row(f"gold.{t}", f"{n:>12,} rows", status)
        except Exception as e:
            row(f"gold.{t}", f"ERR: {str(e)[:50]}", "fail")


def check_freshness(conn):
    header("Data freshness (per source)")
    now = datetime.now(timezone.utc)

    sources = [
        ("Sensors (environment)",  "SELECT MAX(d.timestamp_utc) FROM gold.fact_environment_minute f JOIN gold.dim_datetime d ON d.datetime_key = f.datetime_key"),
        ("Sensors (energy)",       "SELECT MAX(d.timestamp_utc) FROM gold.fact_energy_minute      f JOIN gold.dim_datetime d ON d.datetime_key = f.datetime_key"),
        ("Sensors (presence)",     "SELECT MAX(d.timestamp_utc) FROM gold.fact_presence_minute    f JOIN gold.dim_datetime d ON d.datetime_key = f.datetime_key"),
        ("Weather forecasts",      "SELECT MAX(d.timestamp_utc) FROM gold.fact_weather_hour       f JOIN gold.dim_datetime d ON d.datetime_key = f.datetime_key"),
        ("Device health (daily)",  "SELECT MAX(dt.date) FROM gold.fact_device_health_day h JOIN gold.dim_date dt ON dt.date_key = h.date_key"),
    ]

    for label, sql in sources:
        try:
            latest = conn.execute(text(sql)).scalar()
            if not latest:
                row(label, "no data", "warn")
                continue
            # Normalise: dim_date returns a date, dim_datetime returns datetime
            if hasattr(latest, "tzinfo"):
                if latest.tzinfo is None:
                    latest = latest.replace(tzinfo=timezone.utc)
                age = now - latest
                fmt = latest.strftime('%Y-%m-%d %H:%M UTC')
            else:
                # date-only
                age = now.date() - latest
                fmt = latest.strftime('%Y-%m-%d')
            # Status thresholds
            seconds = age.total_seconds() if hasattr(age, "total_seconds") else age.days * 86400
            status = "ok" if seconds < 26 * 3600 else ("warn" if seconds < 7 * 86400 else "fail")
            row(label, f"{fmt}  ({age} ago)", status)
        except Exception as e:
            row(label, f"ERR: {str(e)[:50]}", "fail")


# ── WATCHER ───────────────────────────────────────────────────────────────────
def watcher_status():
    header("Watcher process")
    if os.name == "nt":
        try:
            res = subprocess.run(
                ["wmic", "process", "where", "name='python.exe'", "get", "processid,commandline"],
                capture_output=True, text=True, check=True
            )
            running = [l for l in res.stdout.splitlines() if "watcher.py" in l.lower()]
            if running:
                for line in running:
                    row("Running", line.strip()[:90], "ok")
            else:
                row("Running", "no watcher.py process found", "warn")
        except Exception as e:
            row("Running", f"could not check: {e}", "warn")
    else:
        try:
            res = subprocess.run(["pgrep", "-af", "watcher.py"], capture_output=True, text=True)
            if res.stdout.strip():
                for line in res.stdout.strip().splitlines():
                    row("Running", line[:90], "ok")
            else:
                row("Running", "not running", "warn")
        except Exception:
            row("Running", "could not check", "warn")


# ── BRONZE ────────────────────────────────────────────────────────────────────
def bronze_status():
    header("Bronze storage")
    if not BRONZE_ROOT.exists():
        row("Folder", f"{BRONZE_ROOT} (not found)", "warn")
        return

    row("Folder", str(BRONZE_ROOT))

    # Top-level breakdown only — avoid full rglob on 100K+ files.
    # Per subfolder: walk one level deep, count files only.
    for sub in sorted(BRONZE_ROOT.iterdir()):
        if not sub.is_dir():
            continue
        # Lazy approximation: count files with os.walk (faster than glob,
        # interruptible). Stops gracefully on huge trees.
        n = 0
        size = 0
        newest_mtime = 0.0
        try:
            for dirpath, _, filenames in os.walk(sub):
                for f in filenames:
                    n += 1
                    full = os.path.join(dirpath, f)
                    try:
                        st = os.stat(full)
                        size += st.st_size
                        if st.st_mtime > newest_mtime:
                            newest_mtime = st.st_mtime
                    except OSError:
                        pass
                    if n > 500_000:  # safety cap to keep status snappy
                        break
                if n > 500_000:
                    break
        except KeyboardInterrupt:
            row(f"  {sub.name}", "scan interrupted", "warn")
            continue

        size_mb = size / 1_000_000
        size_str = f"{size_mb / 1024:.1f} GB" if size_mb >= 1024 else f"{size_mb:.0f} MB"
        newest_str = ""
        if newest_mtime:
            newest_dt = datetime.fromtimestamp(newest_mtime, tz=timezone.utc)
            newest_str = f"  newest: {newest_dt.strftime('%Y-%m-%d %H:%M')}"
        cap = " (capped)" if n >= 500_000 else ""
        row(f"  {sub.name}", f"{n:,} files, {size_str}{cap}{newest_str}", "ok" if n > 0 else "warn")

    # Processed.log if present (from cleanup_bronze)
    processed_log = BRONZE_ROOT.parent / "processed.log"
    if processed_log.exists():
        try:
            n_processed = sum(1 for _ in processed_log.open(encoding="utf-8"))
            row("processed.log", f"{n_processed:,} entries (deleted from bronze, do-not-recopy list)", "ok")
        except Exception:
            pass


# ── LOGS ──────────────────────────────────────────────────────────────────────
def recent_logs():
    header("Recent install.log (last 10 lines)")
    log = PROJECT_ROOT / "install.log"
    if not log.exists():
        row("install.log", "not found", "warn")
        return
    lines = log.read_text(encoding="utf-8", errors="ignore").splitlines()[-10:]
    for line in lines:
        prefix = "  "
        if "FAIL" in line:   prefix = f"  {RED}"
        elif "WARN" in line: prefix = f"  {YELLOW}"
        elif "OK" in line:   prefix = f"  {GREEN}"
        suffix = RESET if prefix != "  " else ""
        print(f"{prefix}{line[:120]}{suffix}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{BOLD}{BLUE}Data Cycle Pipeline -- Status Report{RESET}")
    print(f"{DIM}Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    db_status()
    bronze_status()
    watcher_status()
    recent_logs()
    print()


if __name__ == "__main__":
    main()
