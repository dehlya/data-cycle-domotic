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
MYSQL_URL = os.getenv("MYSQL_URL", "mysql+pymysql://student:password@10.130.25.152:3306/Appartments")

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
# - users              -> GDPR (names, emails, passwords, phone numbers)
# - actions            -> gamification, not relevant for analytics
# - achievements       -> gamification
# - badges             -> gamification
# - events             -> app-generated alerts, not raw sensor data
# - eventsgeneric      -> energy saving tips, not sensor data
# - eventsignore       -> app config
# - categories         -> only useful with events table
# - userrelationships  -> app config

R = "\033[0m"; B = "\033[1m"; D = "\033[2m"; GR = "\033[32m"; RE = "\033[31m"; YL = "\033[33m"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("import_mysql")


def run():
    if not DB_URL:
        raise EnvironmentError("DB_URL not set in .env")

    pg_engine = create_engine(DB_URL)
    my_engine = create_engine(MYSQL_URL)

    print(f"\n{B}import_mysql_to_silver — MySQL -> Silver{R}")
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
                print(f"  {D}○ {source_table} -> {silver_table} (empty){R}")
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
            print(f"  {GR}✓{R} {source_table} -> silver.{silver_table}  {D}({count:,} rows) — {notes}{R}")

        except Exception as e:
            print(f"  {RE}✗ {source_table} — {e}{R}")

    elapsed = time.monotonic() - t_start

    print(f"\n{B}{'─' * 52}{R}")
    print(f"{GR}{B}  Raw MySQL import done in {elapsed:.1f}s{R}")
    print(f"  {GR}✓{R} {imported} tables · {total_rows:,} rows imported into silver schema")
    print()

    # Transform raw error log → cleaned errors with proper types + apartment mapping
    transform_di_errors(pg_engine)


def transform_di_errors(pg_engine):
    """Transform raw silver.log_sensor_errors (TEXT cols, fresh from MySQL) into
    silver.di_errors_clean with proper types, apartment_id mapping, and a
    severity heuristic so populate_sensors.py's fact_device_health_day join
    actually finds rows.

    Mapping logic:
      - error_id      ← idError (int)
      - timestamp     ← creationDate (timestamptz)
      - error_message ← errorMessage (text)
      - apartment     ← join silver.dim_buildings on idBuilding → houseName
                        → "jimmyloup" → "jimmy", "jeremievianin" → "jeremie"
      - sensor_id / room_id  ← left NULL (DIErrors doesn't carry them directly;
                                          could be parsed from `file` later)
      - severity      ← "high" if message contains "fatal|crash|down",
                        "medium" if "fail|error", else "low"
    """
    print(f"{B}clean_di_errors — silver.log_sensor_errors → silver.di_errors_clean{R}")
    t = time.monotonic()
    try:
        with pg_engine.begin() as conn:
            # Skip cleanly if upstream tables are missing (fresh install)
            row = conn.execute(text(
                "SELECT to_regclass('silver.log_sensor_errors') IS NOT NULL"
            )).scalar()
            if not row:
                print(f"  {YL}⚠ silver.log_sensor_errors not found — nothing to transform{R}\n")
                return

            # Truncate target so re-runs are idempotent
            conn.execute(text("TRUNCATE TABLE silver.di_errors_clean"))

            # Build apartment_map from dim_buildings if it exists, else fall back
            # to a hardcoded mapping. dim_buildings has houseName like
            # 'JimmyLoup' / 'JeremieVianin' — we lowercase and substring-match.
            has_buildings = conn.execute(text(
                "SELECT to_regclass('silver.dim_buildings') IS NOT NULL"
            )).scalar()

            apartment_join = ""
            apartment_select = "NULL::VARCHAR(20) AS apartment"
            if has_buildings:
                apartment_join = """
                    LEFT JOIN silver.dim_buildings b
                        ON b."idBuilding" = e."idBuilding"
                """
                apartment_select = """
                    CASE
                        WHEN LOWER(COALESCE(b."houseName", '')) LIKE '%jimmy%'   THEN 'jimmy'
                        WHEN LOWER(COALESCE(b."houseName", '')) LIKE '%jeremie%' THEN 'jeremie'
                        ELSE NULL
                    END::VARCHAR(20) AS apartment
                """

            insert_sql = f"""
                INSERT INTO silver.di_errors_clean
                    (error_id, sensor_id, room_id, apartment, timestamp, error_message, severity)
                SELECT
                    NULLIF(e."idError", '')::INTEGER         AS error_id,
                    NULL::VARCHAR(50)                         AS sensor_id,
                    NULL::INTEGER                             AS room_id,
                    {apartment_select},
                    NULLIF(e."creationDate", '')::TIMESTAMPTZ AS timestamp,
                    e."errorMessage"                          AS error_message,
                    CASE
                        WHEN LOWER(e."errorMessage") ~ 'fatal|crash|down|offline'   THEN 'high'
                        WHEN LOWER(e."errorMessage") ~ 'fail|error|exception|null'  THEN 'medium'
                        ELSE 'low'
                    END                                       AS severity
                FROM silver.log_sensor_errors e
                {apartment_join}
                WHERE e."creationDate" IS NOT NULL AND e."creationDate" <> ''
            """
            result = conn.execute(text(insert_sql))
            n = result.rowcount
            elapsed_t = time.monotonic() - t
            print(f"  {GR}✓{R} silver.di_errors_clean populated  "
                  f"{D}({n:,} rows · {elapsed_t:.1f}s){R}\n")
    except Exception as ex:
        print(f"  {RE}✗ transform_di_errors failed: {ex}{R}\n")


if __name__ == "__main__":
    run()