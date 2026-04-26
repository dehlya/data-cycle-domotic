"""
populate_dimensions.py -- Populate Gold dimension tables
========================================================
Steps 1-6: dim_date, dim_datetime, dim_apartment, dim_room, dim_device, dim_tariff.
Always runs regardless of --sensors or --weather flag.

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

from sqlalchemy import text


def row_count(conn, table):
    return conn.execute(text(f"SELECT COUNT(*) FROM gold.{table}")).scalar()


def populate(engine, log, YE, R):
    """Populate all shared dimension tables."""

    # ═══════════════════════════════════════════════════════════════════════
    # dim_date — from both sensor_events AND weather_forecasts
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_date...")
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.dim_date
                (date_key, date, day_of_week, week, month, year, is_weekend)
            SELECT DISTINCT
                TO_CHAR(d::date, 'YYYYMMDD')::INTEGER AS date_key,
                d::date AS date,
                TO_CHAR(d::date, 'FMDay') AS day_of_week,
                EXTRACT(WEEK  FROM d)::SMALLINT AS week,
                EXTRACT(MONTH FROM d)::SMALLINT AS month,
                EXTRACT(YEAR  FROM d)::SMALLINT AS year,
                EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend
            FROM (
                SELECT timestamp AS d FROM silver.sensor_events
                UNION
                SELECT timestamp AS d FROM silver.weather_forecasts
            ) all_dates
            ON CONFLICT (date) DO NOTHING
        """))
        log.info(f"dim_date: {result.rowcount} rows inserted")

    # ═══════════════════════════════════════════════════════════════════════
    # dim_datetime — from both sensor_events AND weather_forecasts
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_datetime...")
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.dim_datetime
                (datetime_key, timestamp_utc, date_key, hour, minute,
                 day_of_week, week, month, year, is_weekend)
            SELECT DISTINCT
                TO_CHAR(date_trunc('minute', d), 'YYYYMMDDHH24MI')::BIGINT AS datetime_key,
                date_trunc('minute', d) AS timestamp_utc,
                TO_CHAR(d::date, 'YYYYMMDD')::INTEGER AS date_key,
                EXTRACT(HOUR   FROM d)::SMALLINT AS hour,
                EXTRACT(MINUTE FROM d)::SMALLINT AS minute,
                TO_CHAR(d::date, 'FMDay') AS day_of_week,
                EXTRACT(WEEK  FROM d)::SMALLINT AS week,
                EXTRACT(MONTH FROM d)::SMALLINT AS month,
                EXTRACT(YEAR  FROM d)::SMALLINT AS year,
                EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend
            FROM (
                SELECT timestamp AS d FROM silver.sensor_events
                UNION
                SELECT timestamp AS d FROM silver.weather_forecasts
            ) all_timestamps
            ON CONFLICT (timestamp_utc) DO NOTHING
        """))
        log.info(f"dim_datetime: {result.rowcount} rows inserted")

    # ═══════════════════════════════════════════════════════════════════════
    # dim_apartment
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
                    building_id   = b."idBuilding"::INTEGER
                FROM silver.dim_buildings b
                WHERE (a.apartment_id = 'jimmy'   AND LOWER(b."houseName") LIKE '%jimmy%')
                   OR (a.apartment_id = 'jeremie' AND LOWER(b."houseName") LIKE '%jeremie%')
            """))
            log.info("dim_apartment enriched from dim_buildings")
    except Exception as e:
        log.warning(f"Could not enrich dim_apartment from dim_buildings: {e}")

    # ── Pseudonymise dim_apartment for BI display ─────────────────────────
    # GDPR: end users (Power BI viewers) shouldn't see resident first names
    # or building locations. Replace with generic codes. apartment_id is the
    # internal join key and stays untouched — only display fields are masked.
    # Set BI_PSEUDONYMISE=false in .env to opt out (deployer's choice).
    import os
    if os.getenv("BI_PSEUDONYMISE", "true").lower() != "false":
        print(f"  {YE}>{R} pseudonymising dim_apartment for BI display...")
        with engine.begin() as conn:
            conn.execute(text("""
                WITH ranked AS (
                    SELECT apartment_key,
                           ROW_NUMBER() OVER (ORDER BY apartment_key) AS rn
                    FROM gold.dim_apartment
                )
                UPDATE gold.dim_apartment a
                SET name          = 'Apartment ' || CHR(64 + r.rn::INTEGER),
                    building_name = CASE WHEN a.building_id IS NULL THEN NULL
                                         ELSE 'Building ' || a.building_id::TEXT END,
                    owner_user_id = NULL
                FROM ranked r
                WHERE a.apartment_key = r.apartment_key
            """))
            log.info("dim_apartment pseudonymised for BI (name -> 'Apartment A/B/...', building_name -> 'Building N', owner_user_id -> NULL)")
    else:
        log.info("dim_apartment pseudonymisation skipped (BI_PSEUDONYMISE=false)")

    # ═══════════════════════════════════════════════════════════════════════
    # dim_room
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_room...")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO gold.dim_room (room_name, apartment_key)
            SELECT DISTINCT se.room, a.apartment_key
            FROM silver.sensor_events se
            JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
            ON CONFLICT (room_name, apartment_key) DO NOTHING
        """))
        log.info(f"dim_room: {row_count(conn, 'dim_room')} rows")

    # ═══════════════════════════════════════════════════════════════════════
    # dim_device
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_device...")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO gold.dim_device (device_id, room_key, device_type, sensor_type)
            SELECT DISTINCT
                se.apartment || '_' || se.room || '_' || se.sensor_type,
                r.room_key, se.sensor_type, se.sensor_type
            FROM silver.sensor_events se
            JOIN gold.dim_apartment a ON a.apartment_id = se.apartment
            JOIN gold.dim_room r ON r.room_name = se.room AND r.apartment_key = a.apartment_key
            ON CONFLICT (device_id, sensor_type) DO NOTHING
        """))
        log.info(f"dim_device: {row_count(conn, 'dim_device')} rows")

    # ═══════════════════════════════════════════════════════════════════════
    # dim_tariff
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_tariff...")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO gold.dim_tariff (provider, year, chf_per_kwh)
            VALUES ('OIKEN', 2023, 0.34), ('OIKEN', 2024, 0.34), ('OIKEN', 2025, 0.34)
            ON CONFLICT (provider, year) DO UPDATE SET chf_per_kwh = EXCLUDED.chf_per_kwh
        """))
        log.info(f"dim_tariff: {row_count(conn, 'dim_tariff')} rows")
