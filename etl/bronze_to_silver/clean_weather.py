
#Time,                      Value,      Prediction, Site,               Measurement,        Unit
#2023-01-05 00:00:00+00:00,-99999.0,    00,         Aadorf / Tänikon,   PRED_GLOB_ctrl,     Watt/m2


import os
import logging
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# ─── MACRO ───
load_dotenv()

BRONZE_ROOT = Path(os.getenv("BRONZE_ROOT", r"storage\bronze"))
DB_URL = os.getenv("DB_URL")


# ─── LOGGING ───

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("clean_weather")


# ─── DDL ───

WEATHER_CLEAN_DDL = """
CREATE TABLE IF NOT EXISTS silver.weather_clean (
    id               BIGSERIAL PRIMARY KEY,
    timestamp        TIMESTAMPTZ  NOT NULL,
    site             VARCHAR(50),
    temperature_c    FLOAT,
    humidity_pct     FLOAT,
    precipitation_mm FLOAT,
    radiation_wm2    FLOAT,
    is_outlier       BOOLEAN      DEFAULT FALSE,
    UNIQUE (timestamp, site)
);
CREATE INDEX IF NOT EXISTS idx_weather_clean_timestamp ON silver.weather_clean (timestamp);
"""

WATERMARK_DDL = """
CREATE TABLE IF NOT EXISTS silver.weather_watermark (
    filename TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ DEFAULT NOW()
)
"""


def init_db(engine):
    """Ensure that the necessary tables exist before starting."""
    with engine.begin() as conn:
        conn.execute(text(WATERMARK_DDL))
        conn.execute(text(WEATHER_CLEAN_DDL))

def load_watermark(engine):
    """Load processed filenames from watermark table."""
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT filename FROM silver.weather_watermark")
        ).fetchall()

    return {r[0] for r in rows}


def mark_done(engine, filename):
    """Insert filename into watermark table."""
    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO silver.weather_watermark (filename)
            VALUES (:f)
            ON CONFLICT DO NOTHING
            """),
            {"f": filename}
        )


# ─── MEASUREMENT MAPPING ───

MEASURE_MAP = {
    "PRED_T_2M_ctrl": "temperature_c",
    "PRED_RELHUM_2M_ctrl": "humidity_pct",
    "PRED_TOT_PREC_ctrl": "precipitation_mm",
    "PRED_GLOB_ctrl": "radiation_wm2",
}

BOUNDS = {
    "temperature_c": (-50, 60),
    "humidity_pct": (0, 100),
    "precipitation_mm": (0, 500),
    "radiation_wm2": (0, 1500),
}


# ─── CLEANING ───

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean raw weather data and pivot to wide format."""


    # timestamp
    df["timestamp"] = pd.to_datetime(df["Time"], errors="coerce", utc=True)

    # filter out old data (keep 2023+)
    df = df[df["timestamp"].dt.year >= 2023].copy()

    # site clean
    df["site"] = df["Site"].str.replace('"', "").str.strip()

    # Prediction
    df["Prediction"] = pd.to_numeric(df["Prediction"], errors="coerce")

    df = df.sort_values(by=["timestamp", "site", "Measurement", "Prediction"])
    df = df.drop_duplicates(subset=["timestamp", "site", "Measurement"], keep="first")

    # keep relevant measurements
    df = df[df["Measurement"].isin(MEASURE_MAP.keys())].copy()

    # rename measurement
    df["field"] = df["Measurement"].map(MEASURE_MAP)

    # numeric value
    df["value"] = pd.to_numeric(df["Value"], errors="coerce")

    # Sentinel value for missing data is -99999.0, convert to null
    df.loc[df["value"] == -99999.0, "value"] = None

    # pivot long → wide
    df = df.pivot_table(
        index=["timestamp", "site"],
        columns="field",
        values="value",
        aggfunc="first"
    ).reset_index()

    # ensure all columns exist
    for col in MEASURE_MAP.values():
        if col not in df.columns:
            df[col] = None

    # outlier filtering
    df["is_outlier"] = False
    for field, (lo, hi) in BOUNDS.items():
        mask = (df[field] < lo) | (df[field] > hi)
        df.loc[mask, "is_outlier"] = True

    return df



# ─── SQL UPSERT ───

UPSERT_SQL = """
INSERT INTO silver.weather_clean
(timestamp, site, temperature_c, humidity_pct, precipitation_mm, radiation_wm2, is_outlier)
VALUES
(:timestamp, :site, :temperature_c, :humidity_pct, :precipitation_mm, :radiation_wm2, :is_outlier)

ON CONFLICT (timestamp, site)
DO UPDATE SET
temperature_c = EXCLUDED.temperature_c,
humidity_pct = EXCLUDED.humidity_pct,
precipitation_mm = EXCLUDED.precipitation_mm,
radiation_wm2 = EXCLUDED.radiation_wm2,
is_outlier = EXCLUDED.is_outlier
"""


def upsert(engine, df):
    """Upsert cleaned data into silver.weather_clean table."""
    rows = df.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(text(UPSERT_SQL), rows)


def find_csv(watermark):
    """Find new CSV files in bronze directory that are not in watermark."""

    root = BRONZE_ROOT / "weather"

    all_files = list(root.rglob("*.csv"))

    new_files = [f for f in all_files if f.name not in watermark]
    
    return new_files



# ─── JOB ───

def run():

    if not DB_URL:
        raise EnvironmentError("DB_URL not set")

    engine = create_engine(DB_URL)

    log.info("Initializing database schema...")
    init_db(engine)

    log.info("Loading watermark...")
    watermark = load_watermark(engine)

    files = find_csv(watermark)
    log.info(f"{len(files)} new files to process")

    total_rows = 0

    for i, path in enumerate(files, 1):

        log.info(f"Processing {path.name}")
        try:
            df = pd.read_csv(path)
            df_clean = clean_dataframe(df)
            upsert(engine, df_clean)
            total_rows += len(df_clean)
            mark_done(engine, path.name)
        except Exception as e:
            log.warning(f"Failed to process {path.name}: {e}")
        


    engine.dispose()
    log.info(f"Done — {total_rows} rows inserted")


if __name__ == "__main__":
    run()