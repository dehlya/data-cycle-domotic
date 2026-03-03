"""
import_mysql_to_silver.py — Import static MySQL tables into Silver (PostgreSQL)
Reads from the school's MySQL source DB and writes dimension/reference tables
into the silver schema of your local Postgres.

Author: Group 14 · Data Cycle Project · HES-SO Valais 2026

Usage:
    python import_mysql_to_silver.py

Requires .env with:
    DB_URL=postgresql+psycopg2://user:pass@localhost:5432/domotic_dev
Or pass MYSQL_URL as env var (defaults to school DB).
"""

import logging
import time
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────────────
DB_URL    = os.getenv("DB_URL")  # local Postgres
MYSQL_URL = os.getenv("MYSQL_URL", "mysql+pymysql://student:widSN3Ey35fWVOxY@10.130.25.152:3306/Appartments")

# Tables to import: (mysql_table, silver_table, description)
TABLES = [
    # ── Core dimensions ──
    ("buildings",        "dim_buildings",        "Apartment metadata, location, building year"),
    ("buildingtype",     "dim_building_types",   "Maison / Appartement lookup"),
    ("rooms",            "dim_rooms",            "Room details, sensor counts, orientation, m²"),
    ("sensors",          "dim_sensors",          "Sensor IPs mapped to rooms"),
    ("devices",          "dim_devices",          "Appliances per room (fridge, washer, etc.)"),

    # ── Reference / analytics ──
    ("profilereference", "ref_energy_profiles",  "Reference energy consumption kWh/yr by type"),
    ("profile",          "ref_power_snapshots",  "Power consumption snapshots over time"),
    ("parameters",       "ref_parameters",       "Threshold configs per building"),
    ("parameterstype",   "ref_parameters_type",  "Parameter type lookup"),

    # ── Operational ──
    ("dierrors",         "log_sensor_errors",    "Sensor error logs — null values, failures"),
]

# Skipped tables (with reasons):
# - users              → GDPR (names, emails, passwords, phone numbers)
# - actions            → gamification, not relevant for analytics
# - achievements       → gamification
# - badges             → gamification
# - events             → app-generated alerts, not raw sensor data
# - eventsgeneric      → energy saving tips, not sensor data
# - eventsignore       → app config
# - categories         → only useful with events table
# - userrelationships  → app config

R = "\033[0m"; B = "\033[1m"; D = "\033[2m"; GR = "\033[32m"; RE = "\033[31m"; YL = "\033[33m"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("import_mysql")


def run():
    if not DB_URL:
        raise EnvironmentError("DB_URL not set in .env")

    pg_engine = create_engine(DB_URL)
    my_engine = create_engine(MYSQL_URL)

    print(f"\n{B}import_mysql_to_silver — MySQL → Silver{R}")
    print(f"{D}Source : {MYSQL_URL.split('@')[-1]}{R}")
    print(f"{D}Target : {DB_URL.split('@')[-1]}{R}\n")

    # Ensure silver schema exists
    with pg_engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))

    # Check available MySQL tables
    my_inspector = inspect(my_engine)
    available = set(my_inspector.get_table_names())
    log.info(f"MySQL tables found: {sorted(available)}")

    total_rows = 0
    imported = 0
    t_start = time.monotonic()

    for source_table, silver_table, notes in TABLES:
        if source_table not in available:
            print(f"  {YL}⚠ {source_table} not found in MySQL — skipping{R}")
            continue

        try:
            # Read all rows from MySQL
            with my_engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM `{source_table}`"))
                col_names = list(result.keys())
                rows = result.fetchall()

            count = len(rows)
            if count == 0:
                print(f"  {D}○ {source_table} → {silver_table} (empty){R}")
                continue

            # Drop + recreate in silver schema (TEXT columns for safe import)
            col_defs = ", ".join([f'"{c}" TEXT' for c in col_names])
            drop_create = f"""
                DROP TABLE IF EXISTS silver.{silver_table} CASCADE;
                CREATE TABLE silver.{silver_table} ({col_defs});
            """

            # Build insert
            placeholders = ", ".join([f":{c}" for c in col_names])
            cols_quoted = ", ".join([f'"{c}"' for c in col_names])
            insert_sql = text(
                f'INSERT INTO silver.{silver_table} ({cols_quoted}) VALUES ({placeholders})'
            )

            with pg_engine.begin() as conn:
                for stmt in drop_create.strip().split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        conn.execute(text(stmt))
                conn.execute(insert_sql, [dict(zip(col_names, row)) for row in rows])

            total_rows += count
            imported += 1
            print(f"  {GR}✓{R} {source_table} → silver.{silver_table}  {D}({count:,} rows) — {notes}{R}")

        except Exception as e:
            print(f"  {RE}✗ {source_table} — {e}{R}")

    elapsed = time.monotonic() - t_start

    print(f"\n{B}{'─' * 52}{R}")
    print(f"{GR}{B}  Done in {elapsed:.1f}s{R}")
    print(f"  {GR}✓{R} {imported} tables · {total_rows:,} rows imported into silver schema")
    print()


if __name__ == "__main__":
    run()