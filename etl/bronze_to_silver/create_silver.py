"""
create_silver.py — Create Silver schema tables
===============================================
Run once to create all Silver tables in domotic_dev and domotic_prod.
Safe to re-run — uses CREATE TABLE IF NOT EXISTS.

Usage: python etl/create_silver.py

Author: Group 14 · Data Cycle Project · HES-SO Valais 2026
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_URL = os.getenv("DB_URL")  # postgresql://domotic:pass@localhost:5432/domotic_dev

SILVER_TABLES = """
-- ── SENSOR EVENTS ─────────────────────────────────────────────────────────────
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
CREATE INDEX IF NOT EXISTS idx_sensor_events_timestamp  ON silver.sensor_events (timestamp);
CREATE INDEX IF NOT EXISTS idx_sensor_events_apartment  ON silver.sensor_events (apartment);
CREATE INDEX IF NOT EXISTS idx_sensor_events_sensor_type ON silver.sensor_events (sensor_type);

-- ── WEATHER CLEAN ─────────────────────────────────────────────────────────────
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

-- ── APARTMENT METADATA ────────────────────────────────────────────────────────
-- TODO: populate from MySQL snapshot (blocked on schema confirmation)
CREATE TABLE IF NOT EXISTS silver.apartment_metadata (
    id            BIGSERIAL PRIMARY KEY,
    apartment     VARCHAR(20)  NOT NULL,
    building_id   INTEGER,
    building_name VARCHAR(100),  -- masked for GDPR
    room_id       INTEGER,
    room_name     VARCHAR(50),
    sensor_id     VARCHAR(50),
    sensor_type   VARCHAR(20),
    device_id     VARCHAR(50),
    is_active     BOOLEAN DEFAULT TRUE,
    UNIQUE (apartment, room_name, sensor_type)
);

-- ── DI ERRORS ─────────────────────────────────────────────────────────────────
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

def run():
    if not DB_URL:
        raise EnvironmentError("DB_URL not set in .env")

    engine = create_engine(DB_URL)
    print(f"\ncreate_silver — connecting to {DB_URL.split('@')[-1]}")

    with engine.begin() as conn:
        conn.execute(text(SILVER_TABLES))

    print("✓ Silver tables created (or already exist)")
    print("  silver.sensor_events")
    print("  silver.weather_clean")
    print("  silver.apartment_metadata")
    print("  silver.di_errors_clean\n")

if __name__ == "__main__":
    run()