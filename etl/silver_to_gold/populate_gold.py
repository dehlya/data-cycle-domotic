"""
populate_gold.py -- Silver -> Gold ETL
=======================================
Populates Gold dimension and fact tables from Silver data.
Safe to re-run -- all inserts use ON CONFLICT DO UPDATE.

Speed optimisations:
  - work_mem bumped per session for large sorts/hashes
  - ANALYZE after each large fact insert
  - dim tables skip re-insert if already populated
  - mv_energy_with_cost refreshed CONCURRENTLY at the end

Usage: python etl/silver_to_gold/populate_gold.py

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import logging
import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_URL = os.getenv("DB_URL")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("populate_gold")

R="\033[0m"; B="\033[1m"; D="\033[2m"; GR="\033[32m"; RE="\033[31m"; YE="\033[33m"


def row_count(conn, table):
    return conn.execute(text(f"SELECT COUNT(*) FROM gold.{table}")).scalar()


def run():
    if not DB_URL:
        raise EnvironmentError("DB_URL not set in .env")

    engine = create_engine(DB_URL, pool_pre_ping=True)

    print(f"\n{B}populate_gold -- Silver -> Gold{R}")
    print(f"{D}DB : {DB_URL.split('@')[-1]}{R}\n")

    t0 = time.monotonic()

    # Bump work_mem for this session — helps large GROUP BY / hash joins
    with engine.begin() as conn:
        conn.execute(text("SET work_mem = '256MB'"))

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 1 — dim_date
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_date...")
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.dim_date
                (date_key, date, day_of_week, week, month, year, is_weekend)
            SELECT DISTINCT
                TO_CHAR(timestamp::date, 'YYYYMMDD')::INTEGER,
                timestamp::date,
                TO_CHAR(timestamp::date, 'FMDay'),
                EXTRACT(WEEK  FROM timestamp)::SMALLINT,
                EXTRACT(MONTH FROM timestamp)::SMALLINT,
                EXTRACT(YEAR  FROM timestamp)::SMALLINT,
                EXTRACT(ISODOW FROM timestamp) IN (6, 7)
            FROM silver.sensor_events
            ON CONFLICT (date) DO NOTHING
        """))
        log.info(f"dim_date: {result.rowcount} rows inserted")

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 2 — dim_datetime
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_datetime...")
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.dim_datetime
                (datetime_key, timestamp_utc, date_key, hour, minute,
                 day_of_week, week, month, year, is_weekend)
            SELECT DISTINCT
                TO_CHAR(date_trunc('minute', timestamp), 'YYYYMMDDHH24MI')::BIGINT,
                date_trunc('minute', timestamp),
                TO_CHAR(timestamp::date, 'YYYYMMDD')::INTEGER,
                EXTRACT(HOUR   FROM timestamp)::SMALLINT,
                EXTRACT(MINUTE FROM timestamp)::SMALLINT,
                TO_CHAR(timestamp::date, 'FMDay'),
                EXTRACT(WEEK  FROM timestamp)::SMALLINT,
                EXTRACT(MONTH FROM timestamp)::SMALLINT,
                EXTRACT(YEAR  FROM timestamp)::SMALLINT,
                EXTRACT(ISODOW FROM timestamp) IN (6, 7)
            FROM silver.sensor_events
            ON CONFLICT (timestamp_utc) DO NOTHING
        """))
        log.info(f"dim_datetime: {result.rowcount} rows inserted")

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 3 — dim_apartment
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_apartment...")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO gold.dim_apartment (apartment_id, name)
            SELECT DISTINCT apartment, apartment
            FROM silver.sensor_events
            ON CONFLICT (apartment_id) DO NOTHING
        """))
        log.info(f"dim_apartment: {row_count(conn, 'dim_apartment')} rows")

    # Enrich from dim_buildings (separate transaction — OK to fail)
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE gold.dim_apartment a
                SET building_name = b."houseName",
                    building_id   = b."id"::INTEGER
                FROM silver.dim_buildings b
                WHERE (a.apartment_id = 'jimmy'   AND LOWER(b."houseName") LIKE '%jimmy%')
                   OR (a.apartment_id = 'jeremie' AND LOWER(b."houseName") LIKE '%jeremie%')
            """))
            log.info("dim_apartment enriched from dim_buildings")
    except Exception as e:
        log.warning(f"Could not enrich dim_apartment from dim_buildings: {e}")

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 4 — dim_room
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_room...")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO gold.dim_room (room_name, apartment_key)
            SELECT DISTINCT
                se.room,
                a.apartment_key
            FROM silver.sensor_events se
            JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
            ON CONFLICT (room_name, apartment_key) DO NOTHING
        """))
        log.info(f"dim_room: {row_count(conn, 'dim_room')} rows")

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 5 — dim_device
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_device...")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO gold.dim_device (device_id, room_key, device_type, sensor_type)
            SELECT DISTINCT
                se.apartment || '_' || se.room || '_' || se.sensor_type,
                r.room_key,
                se.sensor_type,
                se.sensor_type
            FROM silver.sensor_events se
            JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
            JOIN gold.dim_room r
              ON r.room_name = se.room AND r.apartment_key = a.apartment_key
            ON CONFLICT (device_id, sensor_type) DO NOTHING
        """))
        log.info(f"dim_device: {row_count(conn, 'dim_device')} rows")

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 6 — dim_tariff  (OIKEN rates — update yearly if needed)
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_tariff...")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO gold.dim_tariff (provider, year, chf_per_kwh)
            VALUES
                ('OIKEN', 2023, 0.34),
                ('OIKEN', 2024, 0.34),
                ('OIKEN', 2025, 0.34)
            ON CONFLICT (provider, year) DO UPDATE SET
                chf_per_kwh = EXCLUDED.chf_per_kwh
        """))
        log.info(f"dim_tariff: {row_count(conn, 'dim_tariff')} rows")

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 7 — fact_energy_minute
    # ═══════════════════════════════════════════════════════════════════════
    print(f"\n  {YE}>{R} fact_energy_minute...")
    t1 = time.monotonic()
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.fact_energy_minute
                (datetime_key, date_key, device_key, room_key, apartment_key,
                 power_w, energy_kwh, is_valid)
            SELECT
                TO_CHAR(date_trunc('minute', se.timestamp), 'YYYYMMDDHH24MI')::BIGINT,
                TO_CHAR(se.timestamp::date, 'YYYYMMDD')::INTEGER,
                d.device_key,
                r.room_key,
                a.apartment_key,
                MAX(CASE WHEN se.field IN ('power', 'total_power') THEN se.value END),
                MAX(CASE WHEN se.field = 'total' THEN se.value / 1000.0 END),
                BOOL_AND(NOT se.is_outlier)
            FROM silver.sensor_events se
            JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
            JOIN gold.dim_room r
              ON r.room_name = se.room AND r.apartment_key = a.apartment_key
            JOIN gold.dim_device d
              ON d.device_id = se.apartment || '_' || se.room || '_' || se.sensor_type
             AND d.sensor_type = se.sensor_type
            WHERE se.sensor_type IN ('plug', 'consumption')
            GROUP BY
                date_trunc('minute', se.timestamp),
                se.timestamp::date,
                d.device_key, r.room_key, a.apartment_key
            ON CONFLICT (datetime_key, device_key) DO UPDATE SET
                power_w    = EXCLUDED.power_w,
                energy_kwh = EXCLUDED.energy_kwh,
                is_valid   = EXCLUDED.is_valid
        """))
        log.info(f"fact_energy_minute: {result.rowcount} rows ({time.monotonic()-t1:.1f}s)")
    with engine.begin() as conn:
        conn.execute(text("ANALYZE gold.fact_energy_minute"))

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 8 — fact_environment_minute
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} fact_environment_minute...")
    t1 = time.monotonic()
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.fact_environment_minute
                (datetime_key, date_key, room_key, apartment_key,
                 temperature_c, humidity_pct, co2_ppm, noise_db, pressure_hpa,
                 window_open_flag, door_open_flag, is_anomaly)
            SELECT
                TO_CHAR(date_trunc('minute', se.timestamp), 'YYYYMMDDHH24MI')::BIGINT,
                TO_CHAR(se.timestamp::date, 'YYYYMMDD')::INTEGER,
                r.room_key,
                a.apartment_key,
                MAX(CASE WHEN se.field = 'temperature_c' THEN se.value
                         WHEN se.field = 'temperature' AND se.sensor_type IN ('meteo','humidity','motion')
                              THEN se.value END),
                MAX(CASE WHEN se.field = 'humidity_pct' THEN se.value
                         WHEN se.field = 'humidity' AND se.sensor_type IN ('humidity','meteo')
                              THEN se.value END),
                MAX(CASE WHEN se.field = 'co2_ppm'  THEN se.value END),
                MAX(CASE WHEN se.field = 'noise_db' THEN se.value END),
                MAX(CASE WHEN se.field = 'pressure_hpa' THEN se.value END),
                BOOL_OR(CASE WHEN se.sensor_type = 'window' AND se.field = 'open'
                             THEN se.value = 1.0 END),
                BOOL_OR(CASE WHEN se.sensor_type = 'door'   AND se.field = 'open'
                             THEN se.value = 1.0 END),
                BOOL_OR(se.is_outlier)
            FROM silver.sensor_events se
            JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
            JOIN gold.dim_room r
              ON r.room_name = se.room AND r.apartment_key = a.apartment_key
            WHERE se.sensor_type IN ('meteo', 'humidity', 'door', 'window', 'motion')
              AND se.field NOT IN ('battery', 'motion', 'open', 'light')
            GROUP BY
                date_trunc('minute', se.timestamp),
                se.timestamp::date,
                r.room_key, a.apartment_key
            ON CONFLICT (datetime_key, room_key) DO UPDATE SET
                temperature_c    = EXCLUDED.temperature_c,
                humidity_pct     = EXCLUDED.humidity_pct,
                co2_ppm          = EXCLUDED.co2_ppm,
                noise_db         = EXCLUDED.noise_db,
                pressure_hpa     = EXCLUDED.pressure_hpa,
                window_open_flag = EXCLUDED.window_open_flag,
                door_open_flag   = EXCLUDED.door_open_flag,
                is_anomaly       = EXCLUDED.is_anomaly
        """))
        log.info(f"fact_environment_minute: {result.rowcount} rows ({time.monotonic()-t1:.1f}s)")
    with engine.begin() as conn:
        conn.execute(text("ANALYZE gold.fact_environment_minute"))

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 9 — fact_presence_minute
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} fact_presence_minute...")
    t1 = time.monotonic()
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.fact_presence_minute
                (datetime_key, date_key, room_key, apartment_key,
                 motion_count, door_open_flag, presence_flag, presence_prob)
            SELECT
                TO_CHAR(date_trunc('minute', se.timestamp), 'YYYYMMDDHH24MI')::BIGINT,
                TO_CHAR(se.timestamp::date, 'YYYYMMDD')::INTEGER,
                r.room_key,
                a.apartment_key,
                SUM(CASE WHEN se.sensor_type = 'motion' AND se.field = 'motion'
                          AND se.value = 1.0 THEN 1 ELSE 0 END)::INTEGER,
                BOOL_OR(CASE WHEN se.sensor_type = 'door' AND se.field = 'open'
                             THEN se.value = 1.0 END),
                BOOL_OR(
                    (se.sensor_type = 'motion' AND se.field = 'motion' AND se.value = 1.0) OR
                    (se.sensor_type = 'door'   AND se.field = 'open'   AND se.value = 1.0)
                ),
                NULL::FLOAT   -- ML sprint will fill this
            FROM silver.sensor_events se
            JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
            JOIN gold.dim_room r
              ON r.room_name = se.room AND r.apartment_key = a.apartment_key
            WHERE se.sensor_type IN ('motion', 'door')
              AND se.field IN ('motion', 'open')
            GROUP BY
                date_trunc('minute', se.timestamp),
                se.timestamp::date,
                r.room_key, a.apartment_key
            ON CONFLICT (datetime_key, room_key) DO UPDATE SET
                motion_count   = EXCLUDED.motion_count,
                door_open_flag = EXCLUDED.door_open_flag,
                presence_flag  = EXCLUDED.presence_flag
        """))
        log.info(f"fact_presence_minute: {result.rowcount} rows ({time.monotonic()-t1:.1f}s)")
    with engine.begin() as conn:
        conn.execute(text("ANALYZE gold.fact_presence_minute"))

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 10 — fact_device_health_day
    # Combines battery readings + error log + missing reading detection
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} fact_device_health_day...")
    t1 = time.monotonic()
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.fact_device_health_day
                (date_key, device_key, room_key, apartment_key,
                 error_count, missing_readings, uptime_pct,
                 battery_min_pct, battery_avg_pct)
            WITH

            -- Battery readings per device per day
            battery AS (
                SELECT
                    TO_CHAR(se.timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
                    d.device_key,
                    r.room_key,
                    a.apartment_key,
                    MIN(se.value) AS battery_min_pct,
                    AVG(se.value) AS battery_avg_pct
                FROM silver.sensor_events se
                JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
                JOIN gold.dim_room r
                  ON r.room_name = se.room AND r.apartment_key = a.apartment_key
                JOIN gold.dim_device d
                  ON d.device_id = se.apartment || '_' || se.room || '_' || se.sensor_type
                 AND d.sensor_type = se.sensor_type
                WHERE se.field = 'battery'
                GROUP BY se.timestamp::date, d.device_key, r.room_key, a.apartment_key
            ),

            -- Error counts per device per day from di_errors_clean
            errors AS (
                SELECT
                    TO_CHAR(e.timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
                    d.device_key,
                    COUNT(*) AS error_count
                FROM silver.di_errors_clean e
                JOIN gold.dim_device d ON d.device_id::TEXT = e.sensor_id::TEXT
                GROUP BY e.timestamp::date, d.device_key
            ),

            -- Expected vs actual readings per device per day (uptime proxy)
            -- Sensors report every minute = 1440 expected readings/day
            readings AS (
                SELECT
                    TO_CHAR(se.timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
                    d.device_key,
                    COUNT(DISTINCT date_trunc('minute', se.timestamp)) AS actual_readings,
                    1440 AS expected_readings
                FROM silver.sensor_events se
                JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
                JOIN gold.dim_room r
                  ON r.room_name = se.room AND r.apartment_key = a.apartment_key
                JOIN gold.dim_device d
                  ON d.device_id = se.apartment || '_' || se.room || '_' || se.sensor_type
                 AND d.sensor_type = se.sensor_type
                GROUP BY se.timestamp::date, d.device_key
            )

            SELECT
                b.date_key,
                b.device_key,
                b.room_key,
                b.apartment_key,
                COALESCE(e.error_count, 0)                                          AS error_count,
                GREATEST(r.expected_readings - r.actual_readings, 0)                AS missing_readings,
                ROUND((r.actual_readings::NUMERIC / r.expected_readings * 100), 2)  AS uptime_pct,
                b.battery_min_pct,
                b.battery_avg_pct
            FROM battery b
            LEFT JOIN errors e   USING (date_key, device_key)
            LEFT JOIN readings r USING (date_key, device_key)

            ON CONFLICT (date_key, device_key) DO UPDATE SET
                error_count      = EXCLUDED.error_count,
                missing_readings = EXCLUDED.missing_readings,
                uptime_pct       = EXCLUDED.uptime_pct,
                battery_min_pct  = EXCLUDED.battery_min_pct,
                battery_avg_pct  = EXCLUDED.battery_avg_pct
        """))
        log.info(f"fact_device_health_day: {result.rowcount} rows ({time.monotonic()-t1:.1f}s)")
    with engine.begin() as conn:
        conn.execute(text("ANALYZE gold.fact_device_health_day"))

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 11 — Refresh mv_energy_with_cost
    # ═══════════════════════════════════════════════════════════════════════
    print(f"\n  {YE}>{R} mv_energy_with_cost (refresh)...")
    t1 = time.monotonic()
    with engine.begin() as conn:
        # CONCURRENTLY requires a unique index — we created idx_mv_ewc_pk in create_gold.py
        conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_energy_with_cost"))
        log.info(f"mv_energy_with_cost refreshed ({time.monotonic()-t1:.1f}s)")

    # ═══════════════════════════════════════════════════════════════════════
    # DONE — summary
    # ═══════════════════════════════════════════════════════════════════════
    elapsed = time.monotonic() - t0
    print(f"\n{B}{'-'*52}{R}")
    print(f"{GR}{B}  Gold populated in {elapsed:.0f}s{R}\n")

    with engine.connect() as conn:
        tables = [
            'dim_datetime', 'dim_date', 'dim_apartment', 'dim_room',
            'dim_device', 'dim_tariff',
            'fact_energy_minute', 'fact_environment_minute',
            'fact_presence_minute', 'fact_device_health_day',
            'mv_energy_with_cost',
        ]
        for table in tables:
            count = conn.execute(text(f"SELECT COUNT(*) FROM gold.{table}")).scalar()
            print(f"  {GR}v{R} gold.{table}: {count:,} rows")

    print()
    engine.dispose()


if __name__ == "__main__":
    run()
