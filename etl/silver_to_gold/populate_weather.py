"""
populate_weather.py -- Populate Gold weather dimension + fact
=============================================================
Steps 11-12: dim_weather_site, fact_weather_day.
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
    # dim_weather_site
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} dim_weather_site...")
    with engine.begin() as conn:
        # Insert configured weather sites, linked to each apartment
        for site in weather_sites:
            conn.execute(text("""
                INSERT INTO gold.dim_weather_site (site_name, apartment_key, is_primary)
                SELECT :site, apartment_key, true
                FROM gold.dim_apartment
                ON CONFLICT (site_name, apartment_key) DO NOTHING
            """), {"site": site})
        log.info(f"dim_weather_site: {row_count(conn, 'dim_weather_site')} rows (sites: {weather_sites})")

    # ═══════════════════════════════════════════════════════════════════════
    # fact_weather_day
    # Aggregates weather forecasts: average across model runs, then daily
    # ═══════════════════════════════════════════════════════════════════════
    print(f"  {YE}>{R} fact_weather_day...")
    t1 = time.monotonic()
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.fact_weather_day
                (date_key, site_key, prediction_date,
                 temperature_c_avg, temperature_c_min, temperature_c_max,
                 humidity_pct_avg, precipitation_mm_sum, radiation_wm2_avg,
                 n_model_runs)
            SELECT
                TO_CHAR(wf.timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
                ws.site_key,
                wf.prediction_date,
                -- Temperature: avg/min/max across all model runs and hours
                AVG(CASE WHEN wf.measurement = 'PRED_T_2M_ctrl' THEN wf.value END) AS temperature_c_avg,
                MIN(CASE WHEN wf.measurement = 'PRED_T_2M_ctrl' THEN wf.value END) AS temperature_c_min,
                MAX(CASE WHEN wf.measurement = 'PRED_T_2M_ctrl' THEN wf.value END) AS temperature_c_max,
                -- Humidity: daily average
                AVG(CASE WHEN wf.measurement = 'PRED_RELHUM_2M_ctrl' THEN wf.value END) AS humidity_pct_avg,
                -- Precipitation: daily sum (total across hours)
                SUM(CASE WHEN wf.measurement = 'PRED_TOT_PREC_ctrl' THEN wf.value END) AS precipitation_mm_sum,
                -- Radiation: daily average
                AVG(CASE WHEN wf.measurement = 'PRED_GLOB_ctrl' THEN wf.value END) AS radiation_wm2_avg,
                -- Data quality: how many distinct model runs contributed
                COUNT(DISTINCT wf.prediction) AS n_model_runs
            FROM silver.weather_forecasts wf
            JOIN gold.dim_weather_site ws ON ws.site_name = wf.site
            WHERE NOT wf.is_outlier
            GROUP BY wf.timestamp::date, ws.site_key, wf.prediction_date
            ON CONFLICT (date_key, site_key, prediction_date) DO UPDATE SET
                temperature_c_avg    = EXCLUDED.temperature_c_avg,
                temperature_c_min    = EXCLUDED.temperature_c_min,
                temperature_c_max    = EXCLUDED.temperature_c_max,
                humidity_pct_avg     = EXCLUDED.humidity_pct_avg,
                precipitation_mm_sum = EXCLUDED.precipitation_mm_sum,
                radiation_wm2_avg    = EXCLUDED.radiation_wm2_avg,
                n_model_runs         = EXCLUDED.n_model_runs
        """))
        log.info(f"fact_weather_day: {result.rowcount} rows ({time.monotonic()-t1:.1f}s)")
    with engine.begin() as conn:
        conn.execute(text("ANALYZE gold.fact_weather_day"))
