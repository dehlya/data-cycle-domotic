"""
ml/forecast.py — Energy and presence forecasting workflow.

Trains simple forecasting models on Gold-layer KPI tables and persists
the trained models to the ml/models/ directory.
"""

import os
import logging
import sqlite3
import pickle
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

GOLD_DB = os.environ.get("GOLD_DB", "data/gold/gold.db")
MODEL_DIR = os.environ.get("MODEL_DIR", "ml/models")


def _load_energy_series(conn: sqlite3.Connection, apt_id: str) -> list:
    rows = conn.execute(
        """
        SELECT hour, total_kwh
        FROM fact_energy_hourly
        WHERE apt_id = ?
        ORDER BY hour
        """,
        (apt_id,),
    ).fetchall()
    return rows


def _load_presence_series(conn: sqlite3.Connection, apt_id: str) -> list:
    rows = conn.execute(
        """
        SELECT date, presence_ratio
        FROM fact_presence_daily
        WHERE apt_id = ?
        ORDER BY date
        """,
        (apt_id,),
    ).fetchall()
    return rows


def train_energy_model(series: list, apt_id: str) -> dict:
    """Train a rolling-average baseline energy forecast model."""
    values = [row[1] for row in series if row[1] is not None]
    if not values:
        logger.warning("No energy data for %s — skipping.", apt_id)
        return {}
    window = min(24, len(values))
    rolling_mean = sum(values[-window:]) / window
    model = {"type": "rolling_mean", "window": window, "value": rolling_mean}
    logger.info("Energy model for %s: mean=%.4f kWh over last %d hours", apt_id, rolling_mean, window)
    return model


def train_presence_model(series: list, apt_id: str) -> dict:
    """Train a rolling-average baseline presence forecast model."""
    values = [row[1] for row in series if row[1] is not None]
    if not values:
        logger.warning("No presence data for %s — skipping.", apt_id)
        return {}
    window = min(7, len(values))
    rolling_mean = sum(values[-window:]) / window
    model = {"type": "rolling_mean", "window": window, "value": rolling_mean}
    logger.info("Presence model for %s: ratio=%.4f over last %d days", apt_id, rolling_mean, window)
    return model


def save_model(model: dict, name: str) -> None:
    os.makedirs(MODEL_DIR, exist_ok=True)
    path = os.path.join(MODEL_DIR, f"{name}.pkl")
    with open(path, "wb") as fh:
        pickle.dump(model, fh)
    logger.info("Model saved → %s", path)


def main() -> None:
    if not os.path.exists(GOLD_DB):
        logger.error("Gold DB not found at %s — run etl/gold/aggregate.py first.", GOLD_DB)
        return

    with sqlite3.connect(GOLD_DB) as conn:
        for apt_id in ["apt1", "apt2"]:
            energy_series = _load_energy_series(conn, apt_id)
            energy_model = train_energy_model(energy_series, apt_id)
            if energy_model:
                save_model(energy_model, f"energy_{apt_id}")

            presence_series = _load_presence_series(conn, apt_id)
            presence_model = train_presence_model(presence_series, apt_id)
            if presence_model:
                save_model(presence_model, f"presence_{apt_id}")

    logger.info("Forecasting workflow complete.")


if __name__ == "__main__":
    main()
