"""
populate_gold.py -- Silver -> Gold ETL
=======================================
Populates Gold dimension tables from Silver data, then builds
fact tables by aggregating sensor_events at minute grain.

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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("populate_gold")

R="\033[0m"; B="\033[1m"; D="\033[2m"; GR="\033[32m"; RE="\033[31m"; YE="\033[33m"


def run():
    if not DB_URL:
        raise EnvironmentError("DB_URL not set in .env")

    engine = create_engine(DB_URL)

    print(f"\n{B}populate_gold -- Silver -> Gold{R}")
    print(f"{D}DB : {DB_URL.split('@')[-1]}{R}\n")

    t0 = time.monotonic()

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 1: Populate dim_date
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_date...")
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.dim_date (date_key, date, day_of_week, week, month, year, is_weekend)
            SELECT DISTINCT
                TO_CHAR(timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
                timestamp::date AS date,
                TO_CHAR(timestamp::date, 'FMDay') AS day_of_week,
                EXTRACT(WEEK FROM timestamp)::SMALLINT AS week,
                EXTRACT(MONTH FROM timestamp)::SMALLINT AS month,
                EXTRACT(YEAR FROM timestamp)::SMALLINT AS year,
                EXTRACT(ISODOW FROM timestamp) IN (6, 7) AS is_weekend
            FROM silver.sensor_events
            ON CONFLICT (date) DO NOTHING
        """))
        log.info(f"dim_date: {result.rowcount} rows inserted")

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 2: Populate dim_datetime
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_datetime...")
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.dim_datetime (datetime_key, timestamp_utc, date_key, hour, minute,
                                           day_of_week, week, month, year, is_weekend)
            SELECT DISTINCT
                TO_CHAR(date_trunc('minute', timestamp), 'YYYYMMDDHH24MI')::BIGINT AS datetime_key,
                date_trunc('minute', timestamp) AS timestamp_utc,
                TO_CHAR(timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
                EXTRACT(HOUR FROM timestamp)::SMALLINT AS hour,
                EXTRACT(MINUTE FROM timestamp)::SMALLINT AS minute,
                TO_CHAR(timestamp::date, 'FMDay') AS day_of_week,
                EXTRACT(WEEK FROM timestamp)::SMALLINT AS week,
                EXTRACT(MONTH FROM timestamp)::SMALLINT AS month,
                EXTRACT(YEAR FROM timestamp)::SMALLINT AS year,
                EXTRACT(ISODOW FROM timestamp) IN (6, 7) AS is_weekend
            FROM silver.sensor_events
            ON CONFLICT (timestamp_utc) DO NOTHING
        """))
        log.info(f"dim_datetime: {result.rowcount} rows inserted")

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 3: Populate dim_apartment
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_apartment...")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO gold.dim_apartment (apartment_id, name)
            SELECT DISTINCT apartment, apartment
            FROM silver.sensor_events
            ON CONFLICT (apartment_id) DO NOTHING
        """))
        count = conn.execute(text("SELECT COUNT(*) FROM gold.dim_apartment")).scalar()
        log.info(f"dim_apartment: {count} rows")

    # Try to enrich from dim_buildings (separate transaction, OK to fail)
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE gold.dim_apartment a
                SET building_name = b."houseName",
                    building_id = b."id"::INTEGER
                FROM silver.dim_buildings b
                WHERE (a.apartment_id = 'jimmy' AND LOWER(b."houseName") LIKE '%jimmy%')
                   OR (a.apartment_id = 'jeremie' AND LOWER(b."houseName") LIKE '%jeremie%')
            """))
            log.info("dim_apartment enriched from dim_buildings")
    except Exception as e:
        log.warning(f"Could not enrich dim_apartment from dim_buildings: {e}")

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 4: Populate dim_room
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
        count = conn.execute(text("SELECT COUNT(*) FROM gold.dim_room")).scalar()
        log.info(f"dim_room: {count} rows")

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 5: Populate dim_device
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_device...")
    with engine.begin() as conn:
        # Create a synthetic device_id from apartment + room + sensor_type
        conn.execute(text("""
            INSERT INTO gold.dim_device (device_id, room_key, device_type, sensor_type)
            SELECT DISTINCT
                se.apartment || '_' || se.room || '_' || se.sensor_type AS device_id,
                r.room_key,
                se.sensor_type AS device_type,
                se.sensor_type
            FROM silver.sensor_events se
            JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
            JOIN gold.dim_room r ON r.room_name = se.room AND r.apartment_key = a.apartment_key
            ON CONFLICT (device_id, sensor_type) DO NOTHING
        """))
        count = conn.execute(text("SELECT COUNT(*) FROM gold.dim_device")).scalar()
        log.info(f"dim_device: {count} rows")

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 6: fact_energy_minute
    # ═══════════════════════════════════════════════════════════════════════
    print(f"\n  {YE}>{R} fact_energy_minute...")
    t1 = time.monotonic()
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.fact_energy_minute
                (datetime_key, date_key, device_key, room_key, apartment_key,
                 power_w, energy_wh, energy_kwh, counter_total, is_valid)
            SELECT
                TO_CHAR(date_trunc('minute', se.timestamp), 'YYYYMMDDHH24MI')::BIGINT AS datetime_key,
                TO_CHAR(se.timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
                d.device_key,
                r.room_key,
                a.apartment_key,
                MAX(CASE WHEN se.field = 'power' OR se.field = 'total_power' THEN se.value END) AS power_w,
                MAX(CASE WHEN se.field = 'total' THEN se.value END) AS energy_wh,
                MAX(CASE WHEN se.field = 'total' THEN se.value / 1000.0 END) AS energy_kwh,
                MAX(CASE WHEN se.field = 'total' THEN se.value END) AS counter_total,
                BOOL_AND(NOT se.is_outlier) AS is_valid
            FROM silver.sensor_events se
            JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
            JOIN gold.dim_room r ON r.room_name = se.room AND r.apartment_key = a.apartment_key
            JOIN gold.dim_device d ON d.device_id = se.apartment || '_' || se.room || '_' || se.sensor_type
                                  AND d.sensor_type = se.sensor_type
            WHERE se.sensor_type IN ('plug', 'consumption')
            GROUP BY date_trunc('minute', se.timestamp), se.timestamp::date,
                     d.device_key, r.room_key, a.apartment_key
            ON CONFLICT (datetime_key, device_key) DO UPDATE SET
                power_w = EXCLUDED.power_w,
                energy_wh = EXCLUDED.energy_wh,
                energy_kwh = EXCLUDED.energy_kwh,
                counter_total = EXCLUDED.counter_total,
                is_valid = EXCLUDED.is_valid
        """))
        log.info(f"fact_energy_minute: {result.rowcount} rows ({time.monotonic()-t1:.1f}s)")

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 7: fact_environment_minute
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
                TO_CHAR(date_trunc('minute', se.timestamp), 'YYYYMMDDHH24MI')::BIGINT AS datetime_key,
                TO_CHAR(se.timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
                r.room_key,
                a.apartment_key,
                MAX(CASE WHEN se.field = 'temperature_c' THEN se.value
                         WHEN se.field = 'temperature' AND se.sensor_type = 'meteo' THEN se.value
                         END) AS temperature_c,
                MAX(CASE WHEN se.field = 'humidity_pct' THEN se.value
                         WHEN se.field = 'humidity' AND se.sensor_type = 'humidity' THEN se.value
                         END) AS humidity_pct,
                MAX(CASE WHEN se.field = 'co2_ppm' THEN se.value END) AS co2_ppm,
                MAX(CASE WHEN se.field = 'noise_db' THEN se.value END) AS noise_db,
                MAX(CASE WHEN se.field = 'pressure_hpa' THEN se.value END) AS pressure_hpa,
                BOOL_OR(CASE WHEN se.sensor_type = 'window' AND se.field = 'open' THEN se.value = 1.0 END) AS window_open_flag,
                BOOL_OR(CASE WHEN se.sensor_type = 'door' AND se.field = 'open' THEN se.value = 1.0 END) AS door_open_flag,
                BOOL_OR(se.is_outlier) AS is_anomaly
            FROM silver.sensor_events se
            JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
            JOIN gold.dim_room r ON r.room_name = se.room AND r.apartment_key = a.apartment_key
            WHERE se.sensor_type IN ('meteo', 'humidity', 'door', 'window')
              AND se.field NOT IN ('battery')
            GROUP BY date_trunc('minute', se.timestamp), se.timestamp::date,
                     r.room_key, a.apartment_key
            ON CONFLICT (datetime_key, room_key) DO UPDATE SET
                temperature_c = EXCLUDED.temperature_c,
                humidity_pct = EXCLUDED.humidity_pct,
                co2_ppm = EXCLUDED.co2_ppm,
                noise_db = EXCLUDED.noise_db,
                pressure_hpa = EXCLUDED.pressure_hpa,
                window_open_flag = EXCLUDED.window_open_flag,
                door_open_flag = EXCLUDED.door_open_flag,
                is_anomaly = EXCLUDED.is_anomaly
        """))
        log.info(f"fact_environment_minute: {result.rowcount} rows ({time.monotonic()-t1:.1f}s)")

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 8: fact_presence_minute
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} fact_presence_minute...")
    t1 = time.monotonic()
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.fact_presence_minute
                (datetime_key, date_key, room_key, apartment_key,
                 motion_count, door_open_flag, presence_flag, presence_prob)
            SELECT
                TO_CHAR(date_trunc('minute', se.timestamp), 'YYYYMMDDHH24MI')::BIGINT AS datetime_key,
                TO_CHAR(se.timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
                r.room_key,
                a.apartment_key,
                SUM(CASE WHEN se.sensor_type = 'motion' AND se.field = 'motion' AND se.value = 1.0
                         THEN 1 ELSE 0 END)::INTEGER AS motion_count,
                BOOL_OR(CASE WHEN se.sensor_type = 'door' AND se.field = 'open'
                             THEN se.value = 1.0 END) AS door_open_flag,
                -- Simple presence logic: motion detected OR door opened = present
                BOOL_OR(
                    (se.sensor_type = 'motion' AND se.field = 'motion' AND se.value = 1.0) OR
                    (se.sensor_type = 'door' AND se.field = 'open' AND se.value = 1.0)
                ) AS presence_flag,
                NULL::FLOAT AS presence_prob  -- ML will fill this later
            FROM silver.sensor_events se
            JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
            JOIN gold.dim_room r ON r.room_name = se.room AND r.apartment_key = a.apartment_key
            WHERE se.sensor_type IN ('motion', 'door')
              AND se.field IN ('motion', 'open')
            GROUP BY date_trunc('minute', se.timestamp), se.timestamp::date,
                     r.room_key, a.apartment_key
            ON CONFLICT (datetime_key, room_key) DO UPDATE SET
                motion_count = EXCLUDED.motion_count,
                door_open_flag = EXCLUDED.door_open_flag,
                presence_flag = EXCLUDED.presence_flag
        """))
        log.info(f"fact_presence_minute: {result.rowcount} rows ({time.monotonic()-t1:.1f}s)")

    # ═══════════════════════════════════════════════════════════════════════
    # STEP 9: fact_device_health_day
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} fact_device_health_day...")
    t1 = time.monotonic()
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.fact_device_health_day
                (date_key, device_key, room_key, apartment_key,
                 battery_min_pct, battery_avg_pct)
            SELECT
                TO_CHAR(se.timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
                d.device_key,
                r.room_key,
                a.apartment_key,
                MIN(se.value) AS battery_min_pct,
                AVG(se.value) AS battery_avg_pct
            FROM silver.sensor_events se
            JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
            JOIN gold.dim_room r ON r.room_name = se.room AND r.apartment_key = a.apartment_key
            JOIN gold.dim_device d ON d.device_id = se.apartment || '_' || se.room || '_' || se.sensor_type
                                  AND d.sensor_type = se.sensor_type
            WHERE se.field = 'battery'
            GROUP BY se.timestamp::date, d.device_key, r.room_key, a.apartment_key
            ON CONFLICT (date_key, device_key) DO UPDATE SET
                battery_min_pct = EXCLUDED.battery_min_pct,
                battery_avg_pct = EXCLUDED.battery_avg_pct
        """))
        log.info(f"fact_device_health_day: {result.rowcount} rows ({time.monotonic()-t1:.1f}s)")

    # ═══════════════════════════════════════════════════════════════════════
    # DONE
    # ═══════════════════════════════════════════════════════════════════════
    elapsed = time.monotonic() - t0
    print(f"\n{B}{'-'*48}{R}")
    print(f"{GR}{B}  Gold populated in {elapsed:.0f}s{R}")

    with engine.connect() as conn:
        for table in ['dim_datetime','dim_date','dim_apartment','dim_room','dim_device',
                      'fact_energy_minute','fact_environment_minute','fact_presence_minute',
                      'fact_device_health_day']:
            count = conn.execute(text(f"SELECT COUNT(*) FROM gold.{table}")).scalar()
            print(f"  {GR}v{R} gold.{table}: {count:,} rows")

    print()


if __name__ == "__main__":
    run()