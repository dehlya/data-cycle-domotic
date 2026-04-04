"""
populate_weather.py -- Populate Gold weather dimension + fact
=============================================================
Steps 11-12: dim_weather_site, fact_weather_hour.
Run with: python populate_gold.py --weather

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import time
from sqlalchemy import text


def row_count(conn, table):
    return conn.execute(text(f"SELECT COUNT(*) FROM gold.{table}")).scalar()


def populate(engine, log, weather_sites, YE, R):
    """Populate weather dimension and fact table."""

    # ═══════════════════════════════════════════════════════════════════════
    # dim_weather_site — one row per site (independent of apartments)
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_weather_site...")
    with engine.begin() as conn:
        for site in weather_sites:
            conn.execute(text("""
                INSERT INTO gold.dim_weather_site (site_name)
                VALUES (:site)
                ON CONFLICT (site_name) DO NOTHING
            """), {"site": site})
        log.info(f"dim_weather_site: {row_count(conn, 'dim_weather_site')} rows (sites: {weather_sites})")

    # ═══════════════════════════════════════════════════════════════════════
    # Link apartments to their weather site (dim_apartment.weather_site_key)
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} linking apartments to weather sites...")
    with engine.begin() as conn:
        for site in weather_sites:
            conn.execute(text("""
                UPDATE gold.dim_apartment
                SET weather_site_key = ws.site_key
                FROM gold.dim_weather_site ws
                WHERE ws.site_name = :site
                  AND weather_site_key IS NULL
            """), {"site": site})
        log.info("dim_apartment linked to weather sites")

    # ═══════════════════════════════════════════════════════════════════════
    # fact_weather_hour
    # Aggregates weather forecasts: average across model runs per hour
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} fact_weather_hour...")
    t1 = time.monotonic()
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.fact_weather_hour
                (datetime_key, date_key, site_key, prediction_date,
                 temperature_c, humidity_pct, precipitation_mm, radiation_wm2,
                 n_model_runs)
            SELECT
                TO_CHAR(date_trunc('minute', wf.timestamp), 'YYYYMMDDHH24MI')::BIGINT AS datetime_key,
                TO_CHAR(wf.timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
                ws.site_key,
                wf.prediction_date,
                -- Average across all model runs for this hour + prediction_date
                AVG(CASE WHEN wf.measurement = 'PRED_T_2M_ctrl' THEN wf.value END) AS temperature_c,
                AVG(CASE WHEN wf.measurement = 'PRED_RELHUM_2M_ctrl' THEN wf.value END) AS humidity_pct,
                AVG(CASE WHEN wf.measurement = 'PRED_TOT_PREC_ctrl' THEN wf.value END) AS precipitation_mm,
                AVG(CASE WHEN wf.measurement = 'PRED_GLOB_ctrl' THEN wf.value END) AS radiation_wm2,
                -- Data quality: how many distinct model runs contributed
                COUNT(DISTINCT wf.prediction) AS n_model_runs
            FROM silver.weather_forecasts wf
            JOIN gold.dim_weather_site ws ON ws.site_name = wf.site
            WHERE NOT wf.is_outlier
            GROUP BY date_trunc('minute', wf.timestamp), wf.timestamp::date, ws.site_key, wf.prediction_date
            ON CONFLICT (datetime_key, site_key, prediction_date) DO UPDATE SET
                temperature_c    = EXCLUDED.temperature_c,
                humidity_pct     = EXCLUDED.humidity_pct,
                precipitation_mm = EXCLUDED.precipitation_mm,
                radiation_wm2    = EXCLUDED.radiation_wm2,
                n_model_runs     = EXCLUDED.n_model_runs
        """))
        log.info(f"fact_weather_hour: {result.rowcount} rows ({time.monotonic()-t1:.1f}s)")
    with engine.begin() as conn:
        conn.execute(text("ANALYZE gold.fact_weather_hour"))
