"""
fast_silver_backfill.py — drop unique constraint, run flatten_sensors,
                          dedupe, re-add constraint. ~5-10x faster than
                          the ON CONFLICT path on big backfills.

Use ONLY for the initial backfill on a fresh install where flatten_sensors
hits the unique-index slowdown wall. The watermark + idempotency story
still works (every filename gets recorded), but rows in
silver.sensor_events may briefly contain duplicates between phases 2
and 3 — don't run anything that reads silver during that window.

Usage:
    python scripts/fast_silver_backfill.py            # all 4 phases
    python scripts/fast_silver_backfill.py --dropped   # skip phase 1 (already dropped)
    python scripts/fast_silver_backfill.py --dedupe-only  # only run phases 3-4

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import os
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    sys.exit("DB_URL not set in .env")

CONSTRAINT_NAME = "sensor_events_apartment_room_sensor_type_field_timestamp_key"
TABLE = "silver.sensor_events"
COLS = ("apartment", "room", "sensor_type", "field", "timestamp")


# ── ANSI ──────────────────────────────────────────────────────────────────────
RESET="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"
GREEN="\033[32m"; RED="\033[31m"; YELLOW="\033[33m"; BLUE="\033[36m"
if not sys.stdout.isatty():
    RESET = BOLD = DIM = GREEN = RED = YELLOW = BLUE = ""

def header(m): print(f"\n{BOLD}{BLUE}== {m} =={RESET}")
def ok(m):     print(f"  {GREEN}\u2713{RESET} {m}")
def warn(m):   print(f"  {YELLOW}\u26a0{RESET} {m}")


# ── PHASES ────────────────────────────────────────────────────────────────────
def find_constraint(conn) -> str | None:
    """Find the actual unique constraint name on (apartment, room, sensor_type,
    field, timestamp). Postgres auto-generates a name when ALTER TABLE …
    UNIQUE (…) is used without naming it; we don't know it ahead of time."""
    rows = conn.execute(text("""
        SELECT con.conname
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        WHERE nsp.nspname = 'silver'
          AND rel.relname = 'sensor_events'
          AND con.contype = 'u'
        LIMIT 1
    """)).fetchall()
    return rows[0][0] if rows else None


def phase_1_drop_constraint(engine):
    header("Phase 1 — drop unique constraint")
    with engine.begin() as conn:
        name = find_constraint(conn)
        if not name:
            warn("No unique constraint found on silver.sensor_events — "
                 "either already dropped or table not yet created")
            return None
        ok(f"Found constraint: {name}")
        conn.execute(text(f"ALTER TABLE {TABLE} DROP CONSTRAINT {name}"))
        ok(f"Dropped {name}")
        return name


def phase_2_flatten_sensors():
    header("Phase 2 — run flatten_sensors")
    print(f"  {DIM}Running flatten_sensors with no unique constraint = max speed{RESET}")
    print(f"  {DIM}(this can take 30-90 min for 400k files){RESET}\n")
    t0 = time.monotonic()
    result = subprocess.run(
        [sys.executable, "-m", "etl.bronze_to_silver.flatten_sensors"],
        cwd=str(PROJECT_ROOT),
    )
    elapsed = time.monotonic() - t0
    if result.returncode != 0:
        sys.exit(f"flatten_sensors exited with code {result.returncode}")
    ok(f"flatten_sensors completed in {elapsed/60:.1f} min")


def phase_3_dedupe(engine):
    header("Phase 3 — dedupe silver.sensor_events")
    print(f"  {DIM}DELETE WHERE id > MIN(id) per (apt,room,sensor_type,field,ts){RESET}")
    print(f"  {DIM}This may take a few minutes on a 20M+ row table{RESET}\n")
    t0 = time.monotonic()
    cols_clause = " AND ".join(f"s1.{c} = s2.{c}" for c in COLS)
    sql = f"""
        DELETE FROM {TABLE} s1
        USING {TABLE} s2
        WHERE s1.id > s2.id
          AND {cols_clause}
    """
    with engine.begin() as conn:
        # Get row count before
        n_before = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
        result = conn.execute(text(sql))
        n_deleted = result.rowcount
        n_after = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
    elapsed = time.monotonic() - t0
    ok(f"Before: {n_before:,} rows")
    ok(f"Deleted: {n_deleted:,} duplicate row(s) in {elapsed/60:.1f} min")
    ok(f"After: {n_after:,} rows")


def phase_4_recreate_constraint(engine, original_name: str | None):
    header("Phase 4 — verify no duplicates, then re-add unique constraint")
    cols_csv = ", ".join(COLS)
    name = original_name or CONSTRAINT_NAME

    # Pre-flight: count any remaining duplicates BEFORE attempting the
    # ALTER TABLE. If any survive, we abort cleanly rather than letting
    # Postgres throw "could not create unique index" mid-statement.
    print(f"  {DIM}Checking for residual duplicates...{RESET}")
    t_check = time.monotonic()
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM (
                SELECT 1
                FROM {TABLE}
                GROUP BY {cols_csv}
                HAVING COUNT(*) > 1
                LIMIT 5
            ) AS dup_keys
        """))
        n_dup_keys = result.scalar() or 0
    check_elapsed = time.monotonic() - t_check
    ok(f"Duplicate-key scan: {n_dup_keys} duplicate key(s) found "
       f"({check_elapsed:.1f}s)")

    if n_dup_keys > 0:
        # Show a sample of which keys are still duplicated, then abort
        with engine.connect() as conn:
            samples = conn.execute(text(f"""
                SELECT {cols_csv}, COUNT(*) AS n
                FROM {TABLE}
                GROUP BY {cols_csv}
                HAVING COUNT(*) > 1
                ORDER BY n DESC
                LIMIT 5
            """)).fetchall()
        warn("Duplicates remain — re-adding the constraint would fail.")
        warn("Sample of duplicate keys (count, then key):")
        for row in samples:
            warn(f"  ({row[-1]}×) {row[:-1]}")
        warn("")
        warn("Re-running phase 3 (dedupe) usually clears it. To do that "
             "without re-running flatten_sensors:")
        warn("  python scripts/fast_silver_backfill.py --dedupe-only")
        sys.exit(1)

    print(f"  {DIM}No duplicates — safe to re-add constraint{RESET}")
    t0 = time.monotonic()
    with engine.begin() as conn:
        conn.execute(text(
            f"ALTER TABLE {TABLE} ADD CONSTRAINT {name} UNIQUE ({cols_csv})"
        ))
    elapsed = time.monotonic() - t0
    ok(f"Constraint {name} re-added in {elapsed/60:.1f} min "
       f"(builds the unique index)")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    engine = create_engine(DB_URL)

    skip_drop  = "--dropped"     in sys.argv
    dedupe_only = "--dedupe-only" in sys.argv

    print(f"\n{BOLD}{BLUE}fast_silver_backfill — drop / load / dedupe / re-add{RESET}\n")
    warn("Don't run any silver-reading code while this is in flight.")
    warn("If you have the watcher running, stop it first:")
    warn("  Get-Process pythonw -EA SilentlyContinue | Stop-Process -Force\n")

    original_name: str | None = None

    if dedupe_only:
        phase_3_dedupe(engine)
        # Skip recreating constraint? Caller probably has it dropped
        # already. Print a hint.
        warn("--dedupe-only: skipping constraint recreate. Run phase 4 "
             "manually if needed.")
        return

    if not skip_drop:
        original_name = phase_1_drop_constraint(engine)
    else:
        ok("--dropped flag: assuming constraint already gone")

    phase_2_flatten_sensors()
    phase_3_dedupe(engine)
    phase_4_recreate_constraint(engine, original_name)

    print(f"\n{BOLD}{GREEN}\u2713 Done. silver.sensor_events is now fully loaded "
          f"and idempotent again.{RESET}\n")


if __name__ == "__main__":
    main()
