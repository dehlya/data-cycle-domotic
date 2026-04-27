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

    # outlier flagging (silent — outlier counts not interesting per-file)
    df["is_outlier"] = False
    for measurement, (lo, hi) in BOUNDS.items():
        mask = (df["measurement"] == measurement) & ((df["value"] < lo) | (df["value"] > hi))
        df.loc[mask, "is_outlier"] = True

    # select final columns
    return df[["timestamp", "site", "prediction", "prediction_date", "measurement", "value", "unit", "is_outlier"]]


# ─── BULK LOAD ───

def upsert(engine, df):
    """Bulk load via COPY to temp table + INSERT ON CONFLICT. Fastest method."""
    import io

    # 1. Write DataFrame to CSV buffer in memory
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, sep='\t')
    buf.seek(0)

    # 2. Get raw psycopg2 connection for COPY
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()

        # 3. Create temp table (no constraints = fast writes)
        cur.execute("DROP TABLE IF EXISTS _tmp_weather")
        cur.execute("""
            CREATE TEMP TABLE _tmp_weather (
                timestamp   TIMESTAMPTZ,
                site        VARCHAR(100),
                prediction  SMALLINT,
                prediction_date DATE,
                measurement VARCHAR(50),
                value       FLOAT,
                unit        VARCHAR(20),
                is_outlier  BOOLEAN
            )
        """)

        # 4. COPY from buffer — this is the fast part
        cur.copy_from(buf, '_tmp_weather', sep='\t', null='')

        # 5. Merge into target
        cur.execute("""
            INSERT INTO silver.weather_forecasts
                (timestamp, site, prediction, prediction_date, measurement, value, unit, is_outlier)
            SELECT timestamp, site, prediction, prediction_date, measurement, value, unit, is_outlier
            FROM _tmp_weather
            ON CONFLICT (timestamp, site, prediction, prediction_date, measurement)
            DO UPDATE SET
                value = EXCLUDED.value,
                unit = EXCLUDED.unit,
                is_outlier = EXCLUDED.is_outlier
        """)
        n = cur.rowcount

        cur.execute("DROP TABLE IF EXISTS _tmp_weather")
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()

    return n


def find_csv(watermark):
    """Find new CSV files in bronze directory that are not in watermark."""

    root = BRONZE_ROOT / "weather"

    all_files = list(root.rglob("*.csv"))

    new_files = [f for f in all_files if f.name not in watermark]

    return new_files



# ─── JOB ───

# Parallel file processor. Workers run independently — each opens its own
# connection, processes one CSV (read → clean → COPY → upsert), and returns
# row count. PostgreSQL handles concurrent INSERT FROM SELECT against the same
# table fine, so we get ~3-4x speedup vs the previous sequential loop.
WORKERS = int(os.getenv("CLEAN_WEATHER_WORKERS", "4"))


def _process_one_file(path_str: str) -> tuple[str, int, str | None]:
    """Worker entrypoint. Returns (filename, rows_inserted, error_or_None)."""
    path = Path(path_str)
    prediction_date = parse_prediction_date(path.name)
    if prediction_date is None:
        return (path.name, 0, "could not parse prediction date")

    try:
        df = pd.read_csv(path)
        if df.empty:
            return (path.name, 0, "empty file")
        df_clean = clean_dataframe(df, prediction_date)
        if df_clean.empty:
            return (path.name, 0, "no rows after cleaning")

        # Each worker gets its own engine
        engine = create_engine(DB_URL)
        try:
            n = upsert(engine, df_clean)
            mark_done(engine, path.name)
        finally:
            engine.dispose()
        return (path.name, n, None)
    except Exception as e:
        return (path.name, 0, str(e)[:120])


def run():
    if not DB_URL:
        raise EnvironmentError("DB_URL not set")

    engine = create_engine(DB_URL)
    log.info("Initializing database schema...")
    init_db(engine)
    log.info("Loading watermark...")
    watermark = load_watermark(engine)
    engine.dispose()

    files = find_csv(watermark)
    if not files:
        log.info("Nothing to do. weather is up to date.")
        return

    log.info(f"{len(files)} new files to process  ({WORKERS} parallel workers)")
    log.info("Starting... first progress line will appear after the first file finishes (~5-15s).")

    import time
    from concurrent.futures import ProcessPoolExecutor, as_completed
    t_start = time.monotonic()
    total_rows = 0
    done = 0
    errors = 0

    with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(_process_one_file, str(p)): p for p in files}
        for fut in as_completed(futures):
            done += 1
            name, n, err = fut.result()
            total_rows += n

            elapsed = time.monotonic() - t_start
            rate = done / elapsed if elapsed > 0 else 1
            eta = (len(files) - done) / rate
            pct = done / len(files) * 100
            bar_w = 24
            filled = int(bar_w * pct / 100)
            bar = "█" * filled + "░" * (bar_w - filled)

            if err:
                errors += 1
                log.warning(f"  [{bar}] {done:>3}/{len(files)}  {pct:5.1f}%  {name}  ✗ {err}")
            else:
                log.info(f"  [{bar}] {done:>3}/{len(files)}  {pct:5.1f}%  "
                         f"{name}  +{n:,} rows  ETA {eta/60:.1f}min")

    elapsed = time.monotonic() - t_start
    log.info(f"Done in {elapsed/60:.1f}min — {total_rows:,} rows total, "
             f"{done - errors} ok, {errors} failed")


if __name__ == "__main__":
    run()
