"""
populate_sensors.py -- Populate Gold sensor fact tables
=======================================================
Steps 7-10 + MV refresh: energy, environment, presence, device health.
Run with: python populate_gold.py --sensors

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import time
from sqlalchemy import text


def populate(engine, log, YE, R, GR):
    """Populate sensor fact tables from silver.sensor_events."""

    # ═══════════════════════════════════════════════════════════════════════
    # fact_energy_minute
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
                d.device_key, r.room_key, a.apartment_key,
                MAX(CASE WHEN se.field IN ('power', 'total_power') THEN se.value END),
                MAX(CASE WHEN se.field = 'total' THEN se.value / 1000.0 END),
                BOOL_AND(NOT se.is_outlier)
            FROM silver.sensor_events se
            JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
            JOIN gold.dim_room r ON r.room_name = se.room AND r.apartment_key = a.apartment_key
            JOIN gold.dim_device d ON d.device_id = se.apartment || '_' || se.room || '_' || se.sensor_type
                 AND d.sensor_type = se.sensor_type
            WHERE se.sensor_type IN ('plug', 'consumption')
            GROUP BY date_trunc('minute', se.timestamp), se.timestamp::date,
                     d.device_key, r.room_key, a.apartment_key
            ON CONFLICT (datetime_key, device_key) DO UPDATE SET
                power_w = EXCLUDED.power_w, energy_kwh = EXCLUDED.energy_kwh, is_valid = EXCLUDED.is_valid
        """))
        log.info(f"fact_energy_minute: {result.rowcount} rows ({time.monotonic()-t1:.1f}s)")
    with engine.begin() as conn:
        conn.execute(text("ANALYZE gold.fact_energy_minute"))

    # ═══════════════════════════════════════════════════════════════════════
    # fact_environment_minute
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
                r.room_key, a.apartment_key,
                MAX(CASE WHEN se.field = 'temperature_c' THEN se.value
                         WHEN se.field = 'temperature' AND se.sensor_type IN ('meteo','humidity','motion') THEN se.value END),
                MAX(CASE WHEN se.field = 'humidity_pct' THEN se.value
                         WHEN se.field = 'humidity' AND se.sensor_type IN ('humidity','meteo') THEN se.value END),
                MAX(CASE WHEN se.field = 'co2_ppm'  THEN se.value END),
                MAX(CASE WHEN se.field = 'noise_db' THEN se.value END),
                MAX(CASE WHEN se.field = 'pressure_hpa' THEN se.value END),
                BOOL_OR(CASE WHEN se.sensor_type = 'window' AND se.field = 'open' THEN se.value = 1.0 END),
                BOOL_OR(CASE WHEN se.sensor_type = 'door' AND se.field = 'open' THEN se.value = 1.0 END),
                BOOL_OR(se.is_outlier)
            FROM silver.sensor_events se
            JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
            JOIN gold.dim_room r ON r.room_name = se.room AND r.apartment_key = a.apartment_key
            WHERE se.sensor_type IN ('meteo', 'humidity', 'door', 'window')
              AND se.field IN ('temperature_c', 'temperature', 'humidity_pct', 'humidity',
                               'co2_ppm', 'noise_db', 'pressure_hpa', 'open')
            GROUP BY date_trunc('minute', se.timestamp), se.timestamp::date, r.room_key, a.apartment_key
            ON CONFLICT (datetime_key, room_key) DO UPDATE SET
                temperature_c = EXCLUDED.temperature_c, humidity_pct = EXCLUDED.humidity_pct,
                co2_ppm = EXCLUDED.co2_ppm, noise_db = EXCLUDED.noise_db, pressure_hpa = EXCLUDED.pressure_hpa,
                window_open_flag = EXCLUDED.window_open_flag, door_open_flag = EXCLUDED.door_open_flag,
                is_anomaly = EXCLUDED.is_anomaly
        """))
        log.info(f"fact_environment_minute: {result.rowcount} rows ({time.monotonic()-t1:.1f}s)")
    with engine.begin() as conn:
        conn.execute(text("ANALYZE gold.fact_environment_minute"))

    # ═══════════════════════════════════════════════════════════════════════
    # fact_presence_minute
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
                r.room_key, a.apartment_key,
                SUM(CASE WHEN se.sensor_type = 'motion' AND se.field = 'motion'
                          AND se.value = 1.0 THEN 1 ELSE 0 END)::INTEGER,
                BOOL_OR(CASE WHEN se.sensor_type = 'door' AND se.field = 'open' THEN se.value = 1.0 END),
                BOOL_OR(se.sensor_type = 'motion' AND se.field = 'motion' AND se.value = 1.0),
                NULL::FLOAT
            FROM silver.sensor_events se
            JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
            JOIN gold.dim_room r ON r.room_name = se.room AND r.apartment_key = a.apartment_key
            WHERE se.sensor_type IN ('motion', 'door') AND se.field IN ('motion', 'open')
            GROUP BY date_trunc('minute', se.timestamp), se.timestamp::date, r.room_key, a.apartment_key
            ON CONFLICT (datetime_key, room_key) DO UPDATE SET
                motion_count = EXCLUDED.motion_count, door_open_flag = EXCLUDED.door_open_flag,
                presence_flag = EXCLUDED.presence_flag
        """))
        log.info(f"fact_presence_minute: {result.rowcount} rows ({time.monotonic()-t1:.1f}s)")
    with engine.begin() as conn:
        conn.execute(text("ANALYZE gold.fact_presence_minute"))

    # ═══════════════════════════════════════════════════════════════════════
    # fact_device_health_day
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} fact_device_health_day...")
    t1 = time.monotonic()
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.fact_device_health_day
                (date_key, device_key, room_key, apartment_key,
                 error_count, missing_readings, uptime_pct, battery_min_pct, battery_avg_pct)
            WITH
            battery AS (
                SELECT TO_CHAR(se.timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
                    d.device_key, r.room_key, a.apartment_key,
                    MIN(se.value) AS battery_min_pct, AVG(se.value) AS battery_avg_pct
                FROM silver.sensor_events se
                JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
                JOIN gold.dim_room r ON r.room_name = se.room AND r.apartment_key = a.apartment_key
                JOIN gold.dim_device d ON d.device_id = se.apartment || '_' || se.room || '_' || se.sensor_type
                     AND d.sensor_type = se.sensor_type
                WHERE se.field = 'battery'
                GROUP BY se.timestamp::date, d.device_key, r.room_key, a.apartment_key
            ),
            errors AS (
                SELECT TO_CHAR(e.timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
                    d.device_key, COUNT(*) AS error_count
                FROM silver.di_errors_clean e
                JOIN gold.dim_device d ON d.device_id::TEXT = e.sensor_id::TEXT
                GROUP BY e.timestamp::date, d.device_key
            ),
            readings AS (
                SELECT TO_CHAR(se.timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
                    d.device_key,
                    COUNT(DISTINCT date_trunc('minute', se.timestamp)) AS actual_readings,
                    1440 AS expected_readings
                FROM silver.sensor_events se
                JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
                JOIN gold.dim_room r ON r.room_name = se.room AND r.apartment_key = a.apartment_key
                JOIN gold.dim_device d ON d.device_id = se.apartment || '_' || se.room || '_' || se.sensor_type
                     AND d.sensor_type = se.sensor_type
                GROUP BY se.timestamp::date, d.device_key
            )
            SELECT b.date_key, b.device_key, b.room_key, b.apartment_key,
                COALESCE(e.error_count, 0) AS error_count,
                GREATEST(r.expected_readings - r.actual_readings, 0) AS missing_readings,
                ROUND((r.actual_readings::NUMERIC / r.expected_readings * 100), 2) AS uptime_pct,
                b.battery_min_pct, b.battery_avg_pct
            FROM battery b
            LEFT JOIN errors e USING (date_key, device_key)
            LEFT JOIN readings r USING (date_key, device_key)
            ON CONFLICT (date_key, device_key) DO UPDATE SET
                error_count = EXCLUDED.error_count, missing_readings = EXCLUDED.missing_readings,
                uptime_pct = EXCLUDED.uptime_pct, battery_min_pct = EXCLUDED.battery_min_pct,
                battery_avg_pct = EXCLUDED.battery_avg_pct
        """))
        log.info(f"fact_device_health_day: {result.rowcount} rows ({time.monotonic()-t1:.1f}s)")
    with engine.begin() as conn:
        conn.execute(text("ANALYZE gold.fact_device_health_day"))

    # ═══════════════════════════════════════════════════════════════════════
    # Refresh mv_energy_with_cost
    # ═══════════════════════════════════════════════════════════════════════
    # CONCURRENTLY isn't allowed on the FIRST refresh of an empty MV (Postgres
    # rule). It also can't sit in the same transaction as a fallback (the
    # transaction gets poisoned). So each attempt gets its own transaction.
    print(f"\n  {YE}>{R} mv_energy_with_cost (refresh)...")
    t1 = time.monotonic()
    refreshed = False
    try:
        with engine.begin() as conn:
            conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_energy_with_cost"))
        refreshed = True
    except Exception as e:
        log.info(f"mv_energy_with_cost: CONCURRENTLY not possible yet ({str(e)[:60]}...), trying plain REFRESH")

    if not refreshed:
        try:
            with engine.begin() as conn:
                conn.execute(text("REFRESH MATERIALIZED VIEW gold.mv_energy_with_cost"))
            refreshed = True
        except Exception as e:
            log.warning(f"mv_energy_with_cost refresh failed: {e}")

    if refreshed:
        log.info(f"mv_energy_with_cost refreshed ({time.monotonic()-t1:.1f}s)")
