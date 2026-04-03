# Bronze -> Silver — parses raw weather CSV, standardizes and flattens into rows

#Time,                      Value,      Prediction, Site,               Measurement,        Unit
#2023-01-05 00:00:00+00:00,-99999.0,    00,         Aadorf / Tänikon,   PRED_GLOB_ctrl,     Watt/m2


import os
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# ─── MACRO ───
load_dotenv()

BRONZE_ROOT = Path(os.getenv("BRONZE_ROOT", r"storage\bronze"))
DB_URL = os.getenv("DB_URL")
WEATHER_MIN_YEAR = int(os.getenv("WEATHER_MIN_YEAR", "2023"))


# ─── LOGGING ───
LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("clean_weather")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

_sh = logging.StreamHandler()
_sh.setFormatter(_fmt)
log.addHandler(_sh)

_fh = logging.FileHandler(LOG_DIR / "clean_weather.log", encoding="utf-8")
_fh.setFormatter(_fmt)
log.addHandler(_fh)


# ─── DDL ───

WEATHER_CLEAN_DDL = """
CREATE TABLE IF NOT EXISTS silver.weather_forecasts (
    id               BIGSERIAL PRIMARY KEY,
    timestamp        TIMESTAMPTZ  NOT NULL,
    site             VARCHAR(100),
    prediction       SMALLINT,
    prediction_date  DATE,
    measurement      VARCHAR(50),
    value            FLOAT,
    unit             VARCHAR(20),
    is_outlier       BOOLEAN      DEFAULT FALSE,
    UNIQUE (timestamp, site, prediction, prediction_date, measurement)
);
CREATE INDEX IF NOT EXISTS idx_weather_forecasts_timestamp ON silver.weather_forecasts (timestamp);
CREATE INDEX IF NOT EXISTS idx_weather_forecasts_pred_date ON silver.weather_forecasts (prediction_date);
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

RELEVANT_MEASUREMENTS = {
    "PRED_T_2M_ctrl",
    "PRED_RELHUM_2M_ctrl",
    "PRED_TOT_PREC_ctrl",
    "PRED_GLOB_ctrl",
}

# Outlier bounds — generous on purpose. These are forecasts, not sensor readings.
# Extreme weather events must NOT be flagged as outliers.
# Swiss records: 158mm/1h (Binntal 2024), 41mm/10min (Lausanne 2018).
# 3h record estimated ~200-250mm. Bound at 500 to catch only model bugs.
# Ref: meteoswiss.admin.ch/climate/the-climate-of-switzerland/records-and-extremes.html
BOUNDS = {
    "PRED_T_2M_ctrl": (-50, 60),        # Swiss record: -42°C / 37°C + generous margin
    "PRED_RELHUM_2M_ctrl": (0, 100),    # physical limit
    "PRED_TOT_PREC_ctrl": (0, 500),     # 3h extreme ~200-250mm, 500 catches model bugs only
    "PRED_GLOB_ctrl": (0, 1500),        # solar constant ~1361 W/m², model may overshoot
}


# ─── CLEANING ───

REQUIRED_COLUMNS = {"Time", "Value", "Prediction", "Site", "Measurement", "Unit"}


def parse_prediction_date(filename):
    """Extract prediction date from filename: Pred_2023-01-01.csv -> 2023-01-01"""
    try:
        return datetime.strptime(filename[5:15], "%Y-%m-%d").date()
    except Exception:
        return None


def clean_dataframe(df: pd.DataFrame, prediction_date) -> pd.DataFrame:
    """Clean raw weather data. Keep flat — one row per reading. No pivot."""

    n_raw = len(df)

    # validate expected columns
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing required columns: {missing}")

    # timestamp
    df["timestamp"] = pd.to_datetime(df["Time"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])
    n_after_ts = len(df)

    # filter out old data
    df = df[df["timestamp"].dt.year >= WEATHER_MIN_YEAR].copy()
    n_after_year = len(df)

    # keep relevant fields only
    df = df[df["Measurement"].isin(RELEVANT_MEASUREMENTS)].copy()
    n_after_filter = len(df)

    # site clean
    df["site"] = df["Site"].str.replace('"', "").str.strip()

    # prediction number
    df["prediction"] = pd.to_numeric(df["Prediction"], errors="coerce").astype("Int16")

    # numeric value
    df["value"] = pd.to_numeric(df["Value"], errors="coerce")

    # Remove sentinel values (-99999.0)
    df.loc[df["value"] == -99999.0, "value"] = None
    df = df.dropna(subset=["value"])
    n_after_sentinel = len(df)

    # prediction date from filename
    df["prediction_date"] = prediction_date

    # measurement (keep original name — mapping done in Gold)
    df["measurement"] = df["Measurement"]

    # unit
    df["unit"] = df["Unit"].str.strip()

    # outlier flagging
    df["is_outlier"] = False
    for measurement, (lo, hi) in BOUNDS.items():
        mask = (df["measurement"] == measurement) & ((df["value"] < lo) | (df["value"] > hi))
        n_outliers = mask.sum()
        if n_outliers > 0:
            log.info(f"  {measurement}: {n_outliers} outlier(s) flagged")
        df.loc[mask, "is_outlier"] = True

    log.info(
        f"  rows: {n_raw} raw → {n_after_ts} valid ts → {n_after_year} after year filter "
        f"→ {n_after_filter} relevant measures → {n_after_sentinel} after sentinel removal"
    )

    # select final columns
    return df[["timestamp", "site", "prediction", "prediction_date", "measurement", "value", "unit", "is_outlier"]]


# ─── SQL UPSERT ───

UPSERT_SQL = """
INSERT INTO silver.weather_forecasts
(timestamp, site, prediction, prediction_date, measurement, value, unit, is_outlier)
VALUES
(:timestamp, :site, :prediction, :prediction_date, :measurement, :value, :unit, :is_outlier)

ON CONFLICT (timestamp, site, prediction, prediction_date, measurement)
DO UPDATE SET
value = EXCLUDED.value,
unit = EXCLUDED.unit,
is_outlier = EXCLUDED.is_outlier
"""


def upsert(engine, df):
    """Upsert cleaned data into silver.weather_forecasts table."""
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

        log.info(f"[{i}/{len(files)}] Processing {path.name}")

        # Extract prediction date from filename
        prediction_date = parse_prediction_date(path.name)
        if prediction_date is None:
            log.warning(f"  Skipping {path.name}: could not parse prediction date from filename")
            continue

        try:
            df = pd.read_csv(path)
            if df.empty:
                log.warning(f"  Skipping {path.name}: file is empty")
                mark_done(engine, path.name)
                continue
            df_clean = clean_dataframe(df, prediction_date)
            if df_clean.empty:
                log.warning(f"  Skipping {path.name}: no rows after cleaning")
                mark_done(engine, path.name)
                continue
            upsert(engine, df_clean)
            total_rows += len(df_clean)
            mark_done(engine, path.name)
        except Exception as e:
            log.error(f"  Failed to process {path.name}: {e}")



    engine.dispose()
    log.info(f"Done — {total_rows} rows inserted")


if __name__ == "__main__":
    run()
