"""
watcher.py — Watch SMB share, trigger pipeline on new files
============================================================
Watches Z:\\ for new JSON files. When a batch arrives:
  1. Waits for the batch to finish (debounce 60s)
  2. Runs bulk_to_bronze.py (copies new files to Bronze)
  3. Runs flatten_sensors.py (flattens new files to Silver)

No duplicate work — both scripts are resume-capable.

Usage: python ingestion/fast_flow/watcher.py

Author: Group 14 · Data Cycle Project · HES-SO Valais 2026
"""

import logging
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

from dotenv import load_dotenv
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────

SMB_PATH       = Path(os.getenv("SMB_PATH", r"Z:\\"))
DEBOUNCE_SECS  = 60   # wait this long after last file before triggering pipeline
POLL_INTERVAL  = 10   # how often to check SMB for changes (seconds)

# ── LOGGING ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("watcher")

# ── ANSI COLORS ───────────────────────────────────────────────────────────────

R  = "\033[0m"
B  = "\033[1m"
D  = "\033[2m"
GR = "\033[32m"
RE = "\033[31m"
YE = "\033[33m"
CY = "\033[36m"

# ── PIPELINE ──────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

PIPELINE_STEPS = [
    {
        "name": "bulk_to_bronze",
        "script": PROJECT_ROOT / "ingestion" / "fast_flow" / "bulk_to_bronze.py",
        "desc": "SMB → Bronze",
    },
    {
        "name": "flatten_sensors",
        "script": PROJECT_ROOT / "etl" / "bronze_to_silver" / "flatten_sensors.py",
        "desc": "Bronze → Silver",
    },
]


def run_pipeline():
    """Run all pipeline steps in sequence."""
    print(f"\n{CY}{B}{'═' * 56}{R}")
    print(f"{CY}{B}  PIPELINE TRIGGERED{R}")
    print(f"{CY}{B}{'═' * 56}{R}\n")

    t_start = time.monotonic()

    for step in PIPELINE_STEPS:
        script = step["script"]
        if not script.exists():
            print(f"  {RE}✗ {step['name']} — script not found: {script}{R}")
            continue

        print(f"  {YE}▶{R} {step['name']} — {step['desc']}")
        try:
            result = subprocess.run(
                [sys.executable, str(script)],
                cwd=str(PROJECT_ROOT),
                timeout=7200,  # 2h max per step
            )
            if result.returncode == 0:
                print(f"  {GR}✓{R} {step['name']} done\n")
            else:
                print(f"  {RE}✗ {step['name']} exited with code {result.returncode}{R}\n")
        except subprocess.TimeoutExpired:
            print(f"  {RE}✗ {step['name']} timed out (2h){R}\n")
        except Exception as e:
            print(f"  {RE}✗ {step['name']} error: {e}{R}\n")

    elapsed = time.monotonic() - t_start
    print(f"{CY}{B}{'═' * 56}{R}")
    print(f"{GR}{B}  PIPELINE COMPLETE — {elapsed / 60:.1f}min{R}")
    print(f"{CY}{B}{'═' * 56}{R}\n")


# ── DEBOUNCED HANDLER ─────────────────────────────────────────────────────────

class DebouncedHandler(FileSystemEventHandler):
    """
    Collects new file events and triggers the pipeline once
    no new files have arrived for DEBOUNCE_SECS seconds.
    """

    def __init__(self):
        super().__init__()
        self._timer = None
        self._lock = threading.Lock()
        self._new_files = 0
        self._pipeline_running = False

    def on_created(self, event):
        if event.is_directory:
            return

        path = Path(event.src_path)
        if path.suffix.lower() != ".json":
            return

        with self._lock:
            self._new_files += 1
            count = self._new_files

            if self._pipeline_running:
                log.info(f"Pipeline running — queuing {path.name}")
                return

            # Cancel previous timer and start a new one
            if self._timer is not None:
                self._timer.cancel()

            self._timer = threading.Timer(DEBOUNCE_SECS, self._trigger)
            self._timer.start()

        log.info(f"New file: {path.name}  ({count} in batch, waiting {DEBOUNCE_SECS}s...)")

    def _trigger(self):
        with self._lock:
            count = self._new_files
            self._new_files = 0
            self._pipeline_running = True

        print(f"\n  {GR}Batch complete — {count} new file(s) detected{R}")

        try:
            run_pipeline()
        finally:
            with self._lock:
                self._pipeline_running = False

                # If new files arrived while pipeline was running, trigger again
                if self._new_files > 0:
                    log.info(f"{self._new_files} files arrived during pipeline — retriggering...")
                    self._timer = threading.Timer(DEBOUNCE_SECS, self._trigger)
                    self._timer.start()


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run():
    if not SMB_PATH.exists():
        raise FileNotFoundError(
            f"SMB path not found: {SMB_PATH}\n"
            "Is Z: mounted? Check File Explorer."
        )

    print(f"\n{B}watcher — SMB → Pipeline{R}")
    print(f"{D}Watching : {SMB_PATH}{R}")
    print(f"{D}Debounce : {DEBOUNCE_SECS}s after last file{R}")
    print(f"{D}Polling  : every {POLL_INTERVAL}s{R}")
    print(f"{D}Pipeline : bulk_to_bronze → flatten_sensors{R}")
    print(f"{D}Ctrl+C to stop{R}\n")

    handler  = DebouncedHandler()
    observer = PollingObserver(timeout=POLL_INTERVAL)
    observer.schedule(handler, path=str(SMB_PATH), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n{D}Stopping watcher...{R}")
        observer.stop()

    observer.join()
    print(f"{D}Done.{R}\n")


if __name__ == "__main__":
    run()