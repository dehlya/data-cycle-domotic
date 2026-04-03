"""
populate_gold.py -- Silver -> Gold ETL (coordinator)
=====================================================
Orchestrates Gold population by calling sub-modules.
Safe to re-run -- all inserts use ON CONFLICT DO UPDATE.

Usage:
  python populate_gold.py                  # all (dimensions + sensors + weather)
  python populate_gold.py --sensors        # dimensions + sensor facts only
  python populate_gold.py --weather        # dimensions + weather facts only
  python populate_gold.py --full           # (reserved for future incremental mode)
  python populate_gold.py --full --sensors # (reserved)

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import logging
import os
import sys
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

try:
    from etl.silver_to_gold import populate_dimensions, populate_sensors, populate_weather
except ImportError:
    import populate_dimensions, populate_sensors, populate_weather

load_dotenv()

DB_URL = os.getenv("DB_URL")
WEATHER_SITES = [s.strip() for s in os.getenv("WEATHER_SITES", "Sion").split(",")]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("populate_gold")

R="\033[0m"; B="\033[1m"; D="\033[2m"; GR="\033[32m"; RE="\033[31m"; YE="\033[33m"


def run():
    if not DB_URL:
        raise EnvironmentError("DB_URL not set in .env")

    # Parse flags
    full_reload = "--full" in sys.argv
    do_sensors = "--sensors" in sys.argv
    do_weather = "--weather" in sys.argv

    # If no specific flag, do both
    if not do_sensors and not do_weather:
        do_sensors = True
        do_weather = True

    engine = create_engine(DB_URL, pool_pre_ping=True)

    mode = "full" if full_reload else "incremental"
    scope = []
    if do_sensors: scope.append("sensors")
    if do_weather: scope.append("weather")

    print(f"\n{B}populate_gold -- Silver -> Gold{R}")
    print(f"{D}DB   : {DB_URL.split('@')[-1]}{R}")
    print(f"{D}Mode : {mode} ({', '.join(scope)}){R}")
    if do_weather:
        print(f"{D}Sites: {', '.join(WEATHER_SITES)}{R}")
    print()

    t0 = time.monotonic()

    # Bump work_mem for this session
    with engine.begin() as conn:
        conn.execute(text("SET work_mem = '256MB'"))

    # ── Dimensions (always run — fast, idempotent) ───────────────────────
    populate_dimensions.populate(engine, log, YE, R)

    # ── Sensor facts ─────────────────────────────────────────────────────
    if do_sensors:
        populate_sensors.populate(engine, log, YE, R, GR)
    else:
        print(f"\n  {D}-- skipping sensor facts (--weather only){R}")

    # ── Weather facts ────────────────────────────────────────────────────
    if do_weather:
        populate_weather.populate(engine, log, WEATHER_SITES, YE, R)
    else:
        print(f"\n  {D}-- skipping weather facts (--sensors only){R}")

    # ── Summary ──────────────────────────────────────────────────────────
    elapsed = time.monotonic() - t0
    print(f"\n{B}{'-'*52}{R}")
    print(f"{GR}{B}  Gold populated in {elapsed:.0f}s{R}\n")

    with engine.connect() as conn:
        tables = [
            'dim_datetime', 'dim_date', 'dim_apartment', 'dim_room',
            'dim_device', 'dim_tariff', 'dim_weather_site',
            'fact_energy_minute', 'fact_environment_minute',
            'fact_presence_minute', 'fact_device_health_day',
            'fact_weather_day', 'mv_energy_with_cost',
        ]
        for table in tables:
            try:
                count = conn.execute(text(f"SELECT COUNT(*) FROM gold.{table}")).scalar()
                print(f"  {GR}v{R} gold.{table}: {count:,} rows")
            except Exception:
                print(f"  {D}-{R} gold.{table}: skipped")

    print()
    engine.dispose()


if __name__ == "__main__":
    run()
