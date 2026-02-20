"""
etl/silver/transform.py — Silver layer transformation.

Reads raw JSON sensor files from Bronze, cleans and flattens the records,
joins apartment data with weather, and writes results to Silver DB tables.
"""

import json
import os
import glob
import logging
import sqlite3
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BRONZE_ROOT = os.environ.get("BRONZE_ROOT", "data/bronze")
SILVER_DB = os.environ.get("SILVER_DB", "data/silver/silver.db")


def _ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            apt_id      TEXT    NOT NULL,
            ts          TEXT    NOT NULL,
            energy_kwh  REAL,
            presence    INTEGER,
            temperature REAL,
            humidity    REAL,
            co2_ppm     REAL,
            UNIQUE (apt_id, ts)
        )
        """
    )
    conn.commit()


def _flatten(record: dict, apt_id: str) -> dict:
    return {
        "apt_id": apt_id,
        "ts": record.get("timestamp", datetime.utcnow().isoformat()),
        "energy_kwh": record.get("energy", {}).get("kwh"),
        "presence": int(bool(record.get("presence", {}).get("detected", False))),
        "temperature": record.get("environment", {}).get("temperature"),
        "humidity": record.get("environment", {}).get("humidity"),
        "co2_ppm": record.get("environment", {}).get("co2"),
    }


def process_bronze_files(conn: sqlite3.Connection, apt_id: str) -> None:
    pattern = os.path.join(BRONZE_ROOT, apt_id, "**", "*.json")
    files = glob.glob(pattern, recursive=True)
    logger.info("Processing %d files for %s", len(files), apt_id)
    for path in files:
        try:
            with open(path, encoding="utf-8") as fh:
                raw = json.load(fh)
            row = _flatten(raw, apt_id)
            conn.execute(
                """
                INSERT OR IGNORE INTO sensor_readings
                    (apt_id, ts, energy_kwh, presence, temperature, humidity, co2_ppm)
                VALUES
                    (:apt_id, :ts, :energy_kwh, :presence, :temperature, :humidity, :co2_ppm)
                """,
                row,
            )
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Failed to process %s: %s", path, exc)
    conn.commit()


def main() -> None:
    os.makedirs(os.path.dirname(SILVER_DB), exist_ok=True)
    with sqlite3.connect(SILVER_DB) as conn:
        _ensure_db(conn)
        for apt_id in ["apt1", "apt2"]:
            process_bronze_files(conn, apt_id)
    logger.info("Silver transformation complete.")


if __name__ == "__main__":
    main()
