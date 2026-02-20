"""
etl/gold/aggregate.py — Gold layer aggregation.

Reads cleaned Silver records and builds OLAP/DWH aggregations:
hourly and daily KPI fact tables for energy, presence, and environment.
"""

import os
import logging
import sqlite3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

SILVER_DB = os.environ.get("SILVER_DB", "data/silver/silver.db")
GOLD_DB = os.environ.get("GOLD_DB", "data/gold/gold.db")


def _ensure_gold_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS fact_energy_hourly (
            apt_id          TEXT NOT NULL,
            hour            TEXT NOT NULL,
            total_kwh       REAL,
            avg_kwh         REAL,
            PRIMARY KEY (apt_id, hour)
        );

        CREATE TABLE IF NOT EXISTS fact_presence_daily (
            apt_id          TEXT NOT NULL,
            date            TEXT NOT NULL,
            presence_ratio  REAL,
            PRIMARY KEY (apt_id, date)
        );

        CREATE TABLE IF NOT EXISTS fact_environment_daily (
            apt_id          TEXT NOT NULL,
            date            TEXT NOT NULL,
            avg_temperature REAL,
            avg_humidity    REAL,
            avg_co2_ppm     REAL,
            PRIMARY KEY (apt_id, date)
        );
        """
    )
    conn.commit()


def build_energy_hourly(silver: sqlite3.Connection, gold: sqlite3.Connection) -> None:
    rows = silver.execute(
        """
        SELECT
            apt_id,
            strftime('%Y-%m-%dT%H', ts) AS hour,
            SUM(energy_kwh)             AS total_kwh,
            AVG(energy_kwh)             AS avg_kwh
        FROM sensor_readings
        GROUP BY apt_id, hour
        """
    ).fetchall()
    gold.executemany(
        """
        INSERT OR REPLACE INTO fact_energy_hourly (apt_id, hour, total_kwh, avg_kwh)
        VALUES (?, ?, ?, ?)
        """,
        rows,
    )
    gold.commit()
    logger.info("fact_energy_hourly: %d rows upserted.", len(rows))


def build_presence_daily(silver: sqlite3.Connection, gold: sqlite3.Connection) -> None:
    rows = silver.execute(
        """
        SELECT
            apt_id,
            strftime('%Y-%m-%d', ts)    AS date,
            AVG(presence)               AS presence_ratio
        FROM sensor_readings
        GROUP BY apt_id, date
        """
    ).fetchall()
    gold.executemany(
        """
        INSERT OR REPLACE INTO fact_presence_daily (apt_id, date, presence_ratio)
        VALUES (?, ?, ?)
        """,
        rows,
    )
    gold.commit()
    logger.info("fact_presence_daily: %d rows upserted.", len(rows))


def build_environment_daily(silver: sqlite3.Connection, gold: sqlite3.Connection) -> None:
    rows = silver.execute(
        """
        SELECT
            apt_id,
            strftime('%Y-%m-%d', ts)    AS date,
            AVG(temperature)            AS avg_temperature,
            AVG(humidity)               AS avg_humidity,
            AVG(co2_ppm)               AS avg_co2_ppm
        FROM sensor_readings
        GROUP BY apt_id, date
        """
    ).fetchall()
    gold.executemany(
        """
        INSERT OR REPLACE INTO fact_environment_daily
            (apt_id, date, avg_temperature, avg_humidity, avg_co2_ppm)
        VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )
    gold.commit()
    logger.info("fact_environment_daily: %d rows upserted.", len(rows))


def main() -> None:
    os.makedirs(os.path.dirname(GOLD_DB), exist_ok=True)
    with sqlite3.connect(SILVER_DB) as silver, sqlite3.connect(GOLD_DB) as gold:
        _ensure_gold_tables(gold)
        build_energy_hourly(silver, gold)
        build_presence_daily(silver, gold)
        build_environment_daily(silver, gold)
    logger.info("Gold aggregation complete.")


if __name__ == "__main__":
    main()
