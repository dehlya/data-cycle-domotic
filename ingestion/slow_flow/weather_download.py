import os
import logging
from datetime import datetime
import paramiko
import time
from dotenv import load_dotenv
from pathlib import Path

# ─── MACRO ───
load_dotenv()

SFTP_HOST     = os.getenv("SFTP_HOST")
SFTP_PORT     = int(os.getenv("SFTP_PORT", "22"))
SFTP_USER     = os.getenv("SFTP_USER")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")
SFTP_PATH     = os.getenv("SFTP_PATH")

BRONZE_ROOT   = Path(os.getenv("BRONZE_ROOT", r"storage\bronze"))

MAX_RETRIES   = int(os.getenv("SFTP_MAX_RETRIES", "3"))
RETRY_DELAY   = int(os.getenv("SFTP_RETRY_DELAY", "600"))

# ─── LOGGING ───
LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("weather_sftp")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

_sh = logging.StreamHandler()
_sh.setFormatter(_fmt)
log.addHandler(_sh)

_fh = logging.FileHandler(LOG_DIR / "weather_download.log", encoding="utf-8")
_fh.setFormatter(_fmt)
log.addHandler(_fh)


def bronze_path(filename):
    try:
        # Expected format: Pred_YYYY-MM-DD.csv
        dt = datetime.strptime(filename[5:15], "%Y-%m-%d")
    except Exception:
        log.warning(f"Skipping unexpected filename: {filename}")
        return None
    path = BRONZE_ROOT / "weather" / f"{dt:%Y}" / f"{dt:%m}" / f"{dt:%d}"
    path.mkdir(parents=True, exist_ok=True)
    return path / filename


def connect():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            t = paramiko.Transport((SFTP_HOST, SFTP_PORT))
            t.connect(username=SFTP_USER, password=SFTP_PASSWORD)
            return paramiko.SFTPClient.from_transport(t), t
        except Exception as e:
            if attempt == MAX_RETRIES:
                log.error(f"SFTP failed after {MAX_RETRIES} attempts: {e}")
                raise
            log.warning(f"SFTP error (attempt {attempt}/{MAX_RETRIES}) → retry in {RETRY_DELAY}s: {e}")
            time.sleep(RETRY_DELAY)



def run():

    # 1. Connect to SFTP
    log.info(f"Connecting to {SFTP_HOST}:{SFTP_PORT} (max {MAX_RETRIES} attempts)...")
    sftp, transport = connect()
    log.info("Connected to SFTP")

    try:
        # 2. List all files on the server
        remote_files = sftp.listdir(SFTP_PATH)
        csv_files = [f for f in remote_files if f.endswith('.csv')]
        log.info(f"Found {len(csv_files)} CSV files on SFTP ({len(remote_files)} total)")

        copied = 0
        skipped = 0
        failed = 0

        # 3. For each CSV file on the server
        for filename in csv_files:

            # 4. Create local path (Bronze)
            #    Ex: Pred_2023-08-18.csv -> bronze/weather/2023/08/18/Pred_2023-08-18.csv
            local = bronze_path(filename)
            if local is None:
                skipped += 1
                continue

            # 5. If it already exist -> skip
            if local.exists():
                skipped += 1
                continue

            # 6. Otherwise -> download
            try:
                sftp.get(f"{SFTP_PATH}/{filename}", str(local))
                log.info(f"  Downloaded {filename} → {local}")
                copied += 1
            except Exception as e:
                log.error(f"  Failed to download {filename}: {e}")
                failed += 1

        log.info(f"Done: {copied} downloaded, {skipped} skipped, {failed} failed")

    finally:
        sftp.close()
        transport.close()

if __name__ == "__main__":
    run()