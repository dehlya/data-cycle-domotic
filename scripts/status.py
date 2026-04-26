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
    header("Data freshness")
    try:
        latest = conn.execute(text(
            "SELECT MAX(timestamp_utc) FROM gold.dim_datetime"
        )).scalar()
        if not latest:
            row("Latest data", "no data", "warn")
            return
        now = datetime.now(timezone.utc)
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        age = now - latest
        ok_status = "ok" if age.total_seconds() < 24 * 3600 else "warn"
        row("Latest data", f"{latest.strftime('%Y-%m-%d %H:%M UTC')}  ({age} ago)", ok_status)
    except Exception as e:
        row("Latest data", f"ERR: {str(e)[:50]}", "fail")


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

    files = list(BRONZE_ROOT.rglob("*.json"))
    n = len(files)
    if n == 0:
        row("Files", "0", "warn")
        return

    total = sum(f.stat().st_size for f in files)
    newest = max(files, key=lambda f: f.stat().st_mtime)
    newest_dt = datetime.fromtimestamp(newest.stat().st_mtime, tz=timezone.utc)
    row("Folder", str(BRONZE_ROOT))
    row("Files", f"{n:,}", "ok")
    row("Size", f"{total / 1_000_000:.1f} MB")
    row("Newest", f"{newest.name} ({newest_dt.strftime('%Y-%m-%d %H:%M UTC')})")


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
