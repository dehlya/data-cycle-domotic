"""
etl/bronze/ingest.py — Bronze layer ingestion.

Copies raw sensor JSON files and weather CSV from their landing zones into
immutable, timestamped Bronze paths.  Files are never modified once written.
"""

import os
import shutil
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

LANDING_ROOT = os.environ.get("LANDING_ROOT", "data/landing")
BRONZE_ROOT = os.environ.get("BRONZE_ROOT", "data/bronze")


def _dest_path(source_name: str, filename: str) -> str:
    now = datetime.utcnow()
    date_part = now.strftime("%Y/%m/%d")
    dest_dir = os.path.join(BRONZE_ROOT, source_name, date_part)
    os.makedirs(dest_dir, exist_ok=True)
    return os.path.join(dest_dir, filename)


def archive_landing_files(source_name: str) -> None:
    landing_dir = os.path.join(LANDING_ROOT, source_name)
    if not os.path.isdir(landing_dir):
        logger.warning("Landing directory not found: %s", landing_dir)
        return

    for filename in os.listdir(landing_dir):
        src = os.path.join(landing_dir, filename)
        if not os.path.isfile(src):
            continue
        dest = _dest_path(source_name, filename)
        shutil.move(src, dest)
        logger.info("Moved %s → %s", src, dest)


def main() -> None:
    for source in ["apt1", "apt2", "weather"]:
        archive_landing_files(source)


if __name__ == "__main__":
    main()
