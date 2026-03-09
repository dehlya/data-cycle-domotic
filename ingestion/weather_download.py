import os
import logging
from datetime import datetime
import paramiko
import time
import json
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

BRONZE_ROOT   = Path(os.getenv("BRONZE_ROOT", r"storage\bronze"))
STATE         = Path("weather_missing_files.json")

MAX_RETRIES = 3
RETRY_DELAY = 600

# ─── LOGGING ───
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("weather_sftp")


def bronze_path(file):
    d = datetime.strptime(file[5:15], "%Y-%m-%d")
    p = BRONZE_ROOT / "weather" / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}"
    p.mkdir(parents=True, exist_ok=True)
    return p / file


def connect():
    for i in range(2):
        try:
            t = paramiko.Transport((SFTP_HOST, SFTP_PORT))
            t.connect(username=SFTP_USER, password=SFTP_PASSWORD)
            return paramiko.SFTPClient.from_transport(t), t
        except Exception as e:
            if i == 0:
                log.warning(f"SFTP error → retry in 10min: {e}")
                time.sleep(RETRY_DELAY)
            else:
                raise


def run():

    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    state.setdefault(f"Pred_{datetime.today():%Y-%m-%d}.csv", 0)

    try:
        sftp, t = connect()
    except:
        STATE.write_text(json.dumps(state, indent=2))
        return

    next_state = {}

    for file, retries in state.items():

        local = bronze_path(file)

        if local.exists():
            log.info(f"skip {file}")
            continue

        try:
            sftp.get(f"{SFTP_PATH}/{file}", str(local))
            log.info(f"downloaded {file}")

        except FileNotFoundError:
            retries += 1
            if retries < MAX_RETRIES:
                next_state[file] = retries
            else:
                log.error(f"abort {file}")

        except Exception as e:
            log.error(f"{file} error: {e}")
            retries += 1
            if retries < MAX_RETRIES:
                next_state[file] = retries

    STATE.write_text(json.dumps(next_state, indent=2))
    sftp.close()
    t.close()


if __name__ == "__main__":
    run()