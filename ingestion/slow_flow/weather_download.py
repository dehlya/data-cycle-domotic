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

RETRY_DELAY = 600

# ─── LOGGING ───
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("weather_sftp")


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
    for i in range(2):
        try:
            t = paramiko.Transport((SFTP_HOST, SFTP_PORT))
            t.connect(username=SFTP_USER, password=SFTP_PASSWORD)
            return paramiko.SFTPClient.from_transport(t), t
        except Exception as e:
            if i == 1: raise
            log.warning(f"SFTP error → retry in 10min: {e}")
            time.sleep(RETRY_DELAY)



def run():

    # 1. Connect to SFTP
    sftp, transport = connect()
    
    try:
        # 2. List all files on the server
        remote_files = sftp.listdir(SFTP_PATH)
        log.info(f"Found {len(remote_files)} files on SFTP")
        
        copied = 0
        skipped = 0
        
        # 3. For each CSV file on the server
        for filename in remote_files:
            if not filename.endswith('.csv'):
                continue
            
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
            sftp.get(f"{SFTP_PATH}/{filename}", str(local))
            log.info(f"Downloaded {filename}")
            copied += 1
        
        log.info(f"Done: {copied} copied, {skipped} skipped")
    
    finally:
        sftp.close()
        transport.close()

if __name__ == "__main__":
    run()