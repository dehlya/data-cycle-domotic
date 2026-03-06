# Daily sFTP download — pulls today's weather CSV from Meteo folder, writes to Bronze

import os
import logging
from datetime import datetime
import paramiko
from dotenv import load_dotenv
from pathlib import Path
from dateutil.relativedelta import relativedelta

# ─── MACRO ───
load_dotenv()

SFTP_HOST     = os.getenv("SFTP_HOST")
SFTP_PORT     = int(os.getenv("SFTP_PORT"))
SFTP_USER     = os.getenv("SFTP_USER")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")
SFTP_PATH     = os.getenv("SFTP_PATH")

LOCAL_BASE_PATH = Path.home() / "Desktop" / "Bronze" / "Weather"

# ─── LOGGING ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("weather_sftp")



# ─── JOB ───
def fetchTodaysFile():
    

    # Because start in 2023
    date_minus3 = datetime.today() - relativedelta(years=3)

    expected_filename = f"Pred_{date_minus3:%Y-%m-%d}.csv"
    remote_file_path = f"{SFTP_PATH}/{expected_filename}"

    log.info(f"Connecting to sFTP to find {expected_filename}...")

    try:
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        try:
            sftp.stat(remote_file_path)  # File exist?
        except FileNotFoundError:
            log.warning(f"Today's file not found: {expected_filename}")
            sftp.close()
            transport.close()
            return

        local_dir = Path(
            LOCAL_BASE_PATH,
            str(date_minus3.year),
            f"{date_minus3.month:02d}",
            f"{date_minus3.day:02d}"
        )
        local_dir.mkdir(parents=True, exist_ok=True)
        local_file_path = local_dir / expected_filename

        # Download
        log.info(f"Downloading {expected_filename} to {local_file_path}")
        sftp.get(remote_file_path, str(local_file_path))
        log.info("Download completed.")

        sftp.close()
        transport.close()

    except Exception as e:
        log.error(f"SFTP error: {e}")


if __name__ == "__main__":
    fetchTodaysFile()