"""
create_gold.py -- Create Gold schema (OLAP star schema)
========================================================
Creates all dimension and fact tables in the gold schema.
Safe to re-run -- uses CREATE TABLE IF NOT EXISTS.

Usage: python etl/silver_to_gold/create_gold.py

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import os
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_URL       = os.getenv("DB_URL")
DB_ADMIN_URL = os.getenv("DB_ADMIN_URL")

GOLD_DDL = """
-- ============================================================================
-- GOLD SCHEMA -- OLAP Star Schema v2
-- ============================================================================

-- ── DIMENSION: datetime (grain = 1 minute) ──────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.dim_datetime (
    datetime_key    BIGINT PRIMARY KEY,         -- YYYYMMDDHHMM
    timestamp_utc   TIMESTAMPTZ NOT NULL,
    date_key        INTEGER NOT NULL,            -- YYYYMMDD
    hour            SMALLINT NOT NULL,
    minute          SMALLINT NOT NULL,
    day_of_week     VARCHAR(10) NOT NULL,        -- Monday, Tuesday, ...
    week            SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    year            SMALLINT NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_holiday      BOOLEAN DEFAULT FALSE,
    UNIQUE (timestamp_utc)
);

-- ── DIMENSION: date (grain = 1 day) ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key        INTEGER PRIMARY KEY,         -- YYYYMMDD
    date            DATE NOT NULL,
    day_of_week     VARCHAR(10) NOT NULL,
    week            SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    year            SMALLINT NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_holiday      BOOLEAN DEFAULT FALSE,
    UNIQUE (date)
);

-- ── DIMENSION: apartment ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.dim_apartment (
    apartment_key   SERIAL PRIMARY KEY,
    apartment_id    VARCHAR(20) NOT NULL,        -- 'jimmy', 'jeremie'
    name            VARCHAR(100),
    owner_user_id   VARCHAR(100),                -- for RLS in Power BI / SAC
    building_id     INTEGER,
    building_name   VARCHAR(100),
    floor           INTEGER,
    UNIQUE (apartment_id)
);

-- ── DIMENSION: room ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.dim_room (
    room_key        SERIAL PRIMARY KEY,
    room_id         INTEGER,                     -- from MySQL dim_rooms
    room_name       VARCHAR(50) NOT NULL,
    room_type       VARCHAR(50),
    apartment_key   INTEGER REFERENCES gold.dim_apartment(apartment_key),
    UNIQUE (room_name, apartment_key)
);

-- ── DIMENSION: device (sensor merged in) ────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.dim_device (
    device_key      SERIAL PRIMARY KEY,
    device_id       VARCHAR(50),
    room_key        INTEGER REFERENCES gold.dim_room(room_key),
    device_name     VARCHAR(100),
    device_type     VARCHAR(50),
    sensor_type     VARCHAR(20),                 -- plug, motion, meteo, door, window, humidity, consumption
    is_active       BOOLEAN DEFAULT TRUE,
    UNIQUE (device_id, sensor_type)
);

-- ── DIMENSION: tariff ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.dim_tariff (
    tariff_key      SERIAL PRIMARY KEY,
    provider        VARCHAR(50) NOT NULL,        -- 'OIKEN'
    year            SMALLINT NOT NULL,
    chf_per_kwh     NUMERIC(6,4) NOT NULL,
    UNIQUE (provider, year)
);

-- ── FACT: energy per minute ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.fact_energy_minute (
    datetime_key    BIGINT NOT NULL REFERENCES gold.dim_datetime(datetime_key),
    date_key        INTEGER NOT NULL REFERENCES gold.dim_date(date_key),
    device_key      INTEGER NOT NULL REFERENCES gold.dim_device(device_key),
    room_key        INTEGER NOT NULL REFERENCES gold.dim_room(room_key),
    apartment_key   INTEGER NOT NULL REFERENCES gold.dim_apartment(apartment_key),
    power_w         FLOAT,
    energy_kwh      FLOAT,
    is_valid        BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (datetime_key, device_key)
);

CREATE INDEX IF NOT EXISTS idx_fem_date ON gold.fact_energy_minute (date_key);
CREATE INDEX IF NOT EXISTS idx_fem_apt  ON gold.fact_energy_minute (apartment_key);
CREATE INDEX IF NOT EXISTS idx_fem_room ON gold.fact_energy_minute (room_key);

-- ── MATERIALIZED VIEW: energy with cost ─────────────────────────────────────
-- cost_chf lives here, not in fact_energy_minute
-- Refresh after each populate_gold run.

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_energy_with_cost AS
SELECT
    fem.*,
    t.chf_per_kwh,
    ROUND((fem.energy_kwh * t.chf_per_kwh)::NUMERIC, 4) AS cost_chf
FROM gold.fact_energy_minute fem
JOIN gold.dim_date d ON d.date_key = fem.date_key
LEFT JOIN gold.dim_tariff t ON t.year = d.year AND t.provider = 'OIKEN'
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_ewc_pk
    ON gold.mv_energy_with_cost (datetime_key, device_key);

-- ── FACT: environment per minute ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.fact_environment_minute (
    datetime_key    BIGINT NOT NULL REFERENCES gold.dim_datetime(datetime_key),
    date_key        INTEGER NOT NULL REFERENCES gold.dim_date(date_key),
    room_key        INTEGER NOT NULL REFERENCES gold.dim_room(room_key),
    apartment_key   INTEGER NOT NULL REFERENCES gold.dim_apartment(apartment_key),
    temperature_c   FLOAT,
    humidity_pct    FLOAT,
    co2_ppm         FLOAT,
    noise_db        FLOAT,
    pressure_hpa    FLOAT,
    window_open_flag BOOLEAN,
    door_open_flag  BOOLEAN,
    is_anomaly      BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (datetime_key, room_key)
);

CREATE INDEX IF NOT EXISTS idx_fenv_date ON gold.fact_environment_minute (date_key);
CREATE INDEX IF NOT EXISTS idx_fenv_apt  ON gold.fact_environment_minute (apartment_key);

-- ── FACT: presence per minute ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.fact_presence_minute (
    datetime_key    BIGINT NOT NULL REFERENCES gold.dim_datetime(datetime_key),
    date_key        INTEGER NOT NULL REFERENCES gold.dim_date(date_key),
    room_key        INTEGER NOT NULL REFERENCES gold.dim_room(room_key),
    apartment_key   INTEGER NOT NULL REFERENCES gold.dim_apartment(apartment_key),
    motion_count    INTEGER,
    door_open_flag  BOOLEAN,
    presence_flag   BOOLEAN,
    presence_prob   FLOAT,                       -- NULL until ML sprint
    PRIMARY KEY (datetime_key, room_key)
);

CREATE INDEX IF NOT EXISTS idx_fpres_date ON gold.fact_presence_minute (date_key);
CREATE INDEX IF NOT EXISTS idx_fpres_apt  ON gold.fact_presence_minute (apartment_key);

-- ── FACT: device health per day ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.fact_device_health_day (
    date_key         INTEGER NOT NULL REFERENCES gold.dim_date(date_key),
    device_key       INTEGER NOT NULL REFERENCES gold.dim_device(device_key),
    room_key         INTEGER NOT NULL REFERENCES gold.dim_room(room_key),
    apartment_key    INTEGER NOT NULL REFERENCES gold.dim_apartment(apartment_key),
    error_count      INTEGER DEFAULT 0,
    missing_readings INTEGER DEFAULT 0,
    uptime_pct       FLOAT,
    battery_min_pct  FLOAT,
    battery_avg_pct  FLOAT,
    PRIMARY KEY (date_key, device_key)
);

CREATE INDEX IF NOT EXISTS idx_fhealth_apt ON gold.fact_device_health_day (apartment_key);

-- ── FACT: weather per day (blocked on Sacha) ────────────────────────────────
-- Uncomment when silver.weather_clean is ready.

-- CREATE TABLE IF NOT EXISTS gold.dim_weather_site ( ... );
-- CREATE TABLE IF NOT EXISTS gold.fact_weather_day ( ... );

-- ── FACT: prediction (blocked on Johann ML sprint) ──────────────────────────
-- Uncomment when ML sprint is done.

-- CREATE TABLE IF NOT EXISTS gold.dim_model ( ... );
-- CREATE TABLE IF NOT EXISTS gold.dim_horizon ( ... );
-- CREATE TABLE IF NOT EXISTS gold.dim_target ( ... );
-- CREATE TABLE IF NOT EXISTS gold.fact_prediction ( ... );
"""


def get_db_name(db_url):
    match = re.match(r"postgresql(?:\+\w+)?://[^/]+/(\w+)", db_url)
    if not match:
        raise ValueError(f"Could not parse database name from DB_URL: {db_url}")
    return match.group(1)


def get_admin_target_url(admin_url, db_name):
    return re.sub(r"/\w+$", f"/{db_name}", admin_url)


def run():
    if not DB_URL:
        raise EnvironmentError("DB_URL not set in .env")

    print(f"\ncreate_gold -- target: {DB_URL.split('@')[-1]}")

    # ── Step 1: Create schema + grant privileges (admin connection) ──────────
    if DB_ADMIN_URL:
        db_name = get_db_name(DB_URL)
        admin_target = get_admin_target_url(DB_ADMIN_URL, db_name)
        admin_engine = create_engine(admin_target)
        try:
            with admin_engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
                db_user = re.match(r"postgresql(?:\+\w+)?://([^:@]+)", DB_URL).group(1)
                conn.execute(text(f'GRANT ALL ON SCHEMA gold TO "{db_user}"'))
                conn.execute(text(f'ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO "{db_user}"'))
                print(f"  v gold schema created, privileges granted to {db_user}")
        finally:
            admin_engine.dispose()
    else:
        print("  ! DB_ADMIN_URL not set — assuming schema exists")

    # ── Step 2: Create all tables + indexes + MV in one shot ─────────────────
    engine = create_engine(DB_URL)
    try:
        with engine.begin() as conn:
            conn.execute(text(GOLD_DDL))
            print(f"  v tables, indexes and materialized view created")
    finally:
        engine.dispose()

    # ── Step 3: Verify ───────────────────────────────────────────────────────
    print(f"\n  Verifying...")
    verify_engine = create_engine(DB_URL)
    try:
        with verify_engine.connect() as conn:
            tables = [
                'dim_datetime', 'dim_date', 'dim_apartment', 'dim_room',
                'dim_device', 'dim_tariff',
                'fact_energy_minute', 'fact_environment_minute',
                'fact_presence_minute', 'fact_device_health_day',
            ]
            for t in tables:
                try:
                    conn.execute(text(f"SELECT 1 FROM gold.{t} LIMIT 0"))
                    print(f"    v gold.{t}")
                except Exception:
                    print(f"    x gold.{t} -- NOT FOUND")

            # Check MV separately
            try:
                conn.execute(text("SELECT 1 FROM gold.mv_energy_with_cost LIMIT 0"))
                print(f"    v gold.mv_energy_with_cost")
            except Exception:
                print(f"    x gold.mv_energy_with_cost -- NOT FOUND")
    finally:
        verify_engine.dispose()

    print(f"\n  Done.\n")


if __name__ == "__main__":
    run()
