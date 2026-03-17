
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


# ─── WATERMARK ───

WATERMARK_DDL = """
CREATE TABLE IF NOT EXISTS silver.weather_watermark (
    filename TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ DEFAULT NOW()
)
"""


def load_watermark(engine):

    with engine.begin() as conn:
        conn.execute(text(WATERMARK_DDL))
        rows = conn.execute(
            text("SELECT filename FROM silver.weather_watermark")
        ).fetchall()

    return {r[0] for r in rows}


def mark_done(engine, filenames):

    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO silver.weather_watermark (filename)
            VALUES (:f)
            ON CONFLICT DO NOTHING
            """),
            [{"f": f} for f in filenames]
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

    # timestamp
    df["timestamp"] = pd.to_datetime(df["Time"], errors="coerce")

    # site clean
    df["site"] = df["Site"].str.replace('"', "").str.strip()

    # keep relevant measurements
    df = df[df["Measurement"].isin(MEASURE_MAP.keys())]

    # rename measurement
    df["field"] = df["Measurement"].map(MEASURE_MAP)

    # numeric value
    df["value"] = pd.to_numeric(df["Value"], errors="coerce")

    # convert precipitation
    df.loc[df["Unit"] == "Kg/m2", "value"] = df["value"]

    # remove nulls
    df = df.dropna(subset=["timestamp", "value"])

    # pivot long → wide
    df = df.pivot_table(
        index=["timestamp", "site"],
        columns="field",
        values="value",
        aggfunc="mean"
    ).reset_index()

    # ensure all columns exist
    for col in MEASURE_MAP.values():
        if col not in df.columns:
            df[col] = None

    # outlier filtering
    for field, (lo, hi) in BOUNDS.items():
        df.loc[(df[field] < lo) | (df[field] > hi), field] = None

    return df



# ─── SQL UPSERT ───

UPSERT_SQL = text("""
INSERT INTO silver.weather_clean
(timestamp, site, temperature_c, humidity_pct, precipitation_mm, radiation_wm2)
VALUES
(:timestamp, :site, :temperature_c, :humidity_pct, :precipitation_mm, :radiation_wm2)

ON CONFLICT (timestamp, site)
DO UPDATE SET
temperature_c = EXCLUDED.temperature_c,
humidity_pct = EXCLUDED.humidity_pct,
precipitation_mm = EXCLUDED.precipitation_mm,
radiation_wm2 = EXCLUDED.radiation_wm2
""")


def upsert(engine, df):

    rows = df.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(UPSERT_SQL, rows)


def find_csv(watermark):

    root = BRONZE_ROOT / "weather"

    all_files = list(root.rglob("*.csv"))

    new_files = [f for f in all_files if str(f) not in watermark]
    
    return new_files



# ─── JOB ───

def run():

    if not DB_URL:
        raise EnvironmentError("DB_URL not set")

    engine = create_engine(DB_URL)

    log.info("Loading watermark...")

    watermark = load_watermark(engine)

    files = find_csv(watermark)

    log.info(f"{len(files)} new files to process")

    total_rows = 0

    processed_files = []

    for i, path in enumerate(files, 1):

        log.info(f"Processing {path.name}")

        df = pd.read_csv(path)

        df_clean = clean_dataframe(df)

        upsert(engine, df_clean)

        total_rows += len(df_clean)

        processed_files.append(str(path))
        
        if i % 10 == 0:
            log.info(f"{i}/{len(files)} files processed")


    mark_done(engine, processed_files)

    log.info(f"Done — {total_rows} rows inserted")


if __name__ == "__main__":
    run()