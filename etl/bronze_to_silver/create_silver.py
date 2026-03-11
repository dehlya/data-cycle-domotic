"""
create_silver.py — Create Silver schema tables
===============================================
Run once to create all Silver tables in domotic_dev and domotic_prod.
Safe to re-run — uses CREATE TABLE IF NOT EXISTS.

Usage: python etl/create_silver.py

Author: Group 14 · Data Cycle Project · HES-SO Valais 2026
"""

import os
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_URL       = os.getenv("DB_URL")        # postgresql://domotic:pass@localhost:5432/domotic_dev
DB_ADMIN_URL = os.getenv("DB_ADMIN_URL")  # postgresql://postgres:adminpass@localhost:5432/postgres

SILVER_TABLES = """
-- SCHEMA
CREATE SCHEMA IF NOT EXISTS silver;

-- SENSOR EVENTS
CREATE TABLE IF NOT EXISTS silver.sensor_events (
    id           BIGSERIAL PRIMARY KEY,
    apartment    VARCHAR(20)  NOT NULL,
    room         VARCHAR(50)  NOT NULL,
    sensor_type  VARCHAR(20)  NOT NULL,
    field        VARCHAR(50)  NOT NULL,
    value        FLOAT,
    unit         VARCHAR(10),
    timestamp    TIMESTAMPTZ  NOT NULL,
    is_outlier   BOOLEAN      DEFAULT FALSE,
    UNIQUE (apartment, room, sensor_type, field, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_sensor_events_timestamp   ON silver.sensor_events (timestamp);
CREATE INDEX IF NOT EXISTS idx_sensor_events_apartment   ON silver.sensor_events (apartment);
CREATE INDEX IF NOT EXISTS idx_sensor_events_sensor_type ON silver.sensor_events (sensor_type);

-- WEATHER CLEAN
CREATE TABLE IF NOT EXISTS silver.weather_clean (
    id               BIGSERIAL PRIMARY KEY,
    timestamp        TIMESTAMPTZ  NOT NULL,
    site             VARCHAR(50),
    temperature_c    FLOAT,
    humidity_pct     FLOAT,
    precipitation_mm FLOAT,
    radiation_wm2    FLOAT,
    UNIQUE (timestamp, site)
);
CREATE INDEX IF NOT EXISTS idx_weather_clean_timestamp ON silver.weather_clean (timestamp);

-- APARTMENT METADATA
-- TODO: populate from MySQL snapshot (blocked on schema confirmation)
CREATE TABLE IF NOT EXISTS silver.apartment_metadata (
    id            BIGSERIAL PRIMARY KEY,
    apartment     VARCHAR(20)  NOT NULL,
    building_id   INTEGER,
    building_name VARCHAR(100),
    room_id       INTEGER,
    room_name     VARCHAR(50),
    sensor_id     VARCHAR(50),
    sensor_type   VARCHAR(20),
    device_id     VARCHAR(50),
    is_active     BOOLEAN DEFAULT TRUE,
    UNIQUE (apartment, room_name, sensor_type)
);

-- DI ERRORS
-- TODO: populate from MySQL DIErrors table (blocked on schema confirmation)
CREATE TABLE IF NOT EXISTS silver.di_errors_clean (
    id            BIGSERIAL PRIMARY KEY,
    error_id      INTEGER,
    sensor_id     VARCHAR(50),
    room_id       INTEGER,
    apartment     VARCHAR(20),
    timestamp     TIMESTAMPTZ,
    error_message TEXT,
    severity      VARCHAR(10)  DEFAULT 'low'
);
CREATE INDEX IF NOT EXISTS idx_di_errors_timestamp ON silver.di_errors_clean (timestamp);
"""


def get_db_name(db_url: str) -> str:
    match = re.match(r"postgresql(?:\+\w+)?://[^/]+/(\w+)", db_url)
    if not match:
        raise ValueError(f"Could not parse database name from DB_URL: {db_url}")
    return match.group(1)


def get_db_user(db_url: str) -> str | None:
    match = re.match(r"postgresql(?:\+\w+)?://([^:@]+)", db_url)
    return match.group(1) if match else None


def get_admin_target_url(admin_url: str, db_name: str) -> str:
    """Replace the database at the end of the admin URL with db_name."""
    return re.sub(r"/\w+$", f"/{db_name}", admin_url)


def ensure_database(db_url: str, admin_url: str | None) -> str:
    """Ensure the target database exists and that the app user has full privileges.
    Returns a connection URL pointing to the target DB using admin credentials,
    so we can safely create schemas/tables regardless of the app user's permissions.
    """
    db_name = get_db_name(db_url)
    db_user = get_db_user(db_url)

    if not admin_url:
        # No admin URL — fall back to connecting directly, may fail if no privileges
        print("  Warning: DB_ADMIN_URL not set, attempting with DB_URL credentials")
        return db_url

    # Connect to the admin maintenance DB (e.g. postgres) to create the target DB
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        with admin_engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": db_name},
            ).fetchone()

            if not exists:
                conn.execute(text(f'CREATE DATABASE "{db_name}"'))
                print(f"  Database '{db_name}' created")
            else:
                print(f"  Database '{db_name}' already exists")

            # Always ensure app user has full privileges (idempotent)
            if db_user:
                conn.execute(text(
                    f'GRANT ALL PRIVILEGES ON DATABASE "{db_name}" TO "{db_user}"'
                ))
                print(f"  Privileges on '{db_name}' granted to '{db_user}'")
    finally:
        admin_engine.dispose()

    # Return an admin URL pointing at the target DB for schema/table creation
    return get_admin_target_url(admin_url, db_name)


def run():
    if not DB_URL:
        raise EnvironmentError("DB_URL not set in .env")

    print(f"\ncreate_silver -- target: {DB_URL.split('@')[-1]}")

    # 1. Ensure DB exists and privileges are set; get the URL to use for DDL
    ddl_url = ensure_database(DB_URL, admin_url=DB_ADMIN_URL)

    # 2. Create schema + tables (using admin URL so we're never blocked by privileges)
    engine = create_engine(ddl_url)
    try:
        with engine.begin() as conn:
            conn.execute(text(SILVER_TABLES))
    finally:
        engine.dispose()

    print("  Silver schema and tables created (or already exist)")
    print("    silver.sensor_events")
    print("    silver.weather_clean")
    print("    silver.apartment_metadata")
    print("    silver.di_errors_clean\n")


if __name__ == "__main__":
    run()