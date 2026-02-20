"""
recup.py — Sensor data acquisition script.

Polls JSON sensors from two smart apartments every minute via local network,
fetches MySQL DB (pidb) data, and downloads Weather CSV via sFTP.
Raw responses are written to timestamped Bronze-layer paths.
"""

import json
import os
import logging
from datetime import datetime

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

SENSOR_URLS = [
    os.environ.get("SENSOR_URL_APT1", "http://192.168.1.10/sensors"),
    os.environ.get("SENSOR_URL_APT2", "http://192.168.1.20/sensors"),
]

BRONZE_ROOT = os.environ.get("BRONZE_ROOT", "data/bronze")


def _timestamped_path(source_name: str) -> str:
    now = datetime.utcnow()
    date_part = now.strftime("%Y/%m/%d")
    filename = now.strftime("%H%M%S") + ".json"
    path = os.path.join(BRONZE_ROOT, source_name, date_part, filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path


def fetch_sensor(url: str, apt_id: str) -> None:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logger.error("HTTP error fetching %s: %s", url, exc)
        return
    except ValueError as exc:
        logger.error("Invalid JSON from %s: %s", url, exc)
        return

    dest = _timestamped_path(apt_id)
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    logger.info("Wrote %s → %s", apt_id, dest)


def poll_once() -> None:
    for idx, url in enumerate(SENSOR_URLS, start=1):
        fetch_sensor(url, f"apt{idx}")


def main() -> None:
    logger.info("Starting sensor acquisition poll.")
    poll_once()


if __name__ == "__main__":
    main()
