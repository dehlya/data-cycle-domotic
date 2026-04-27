"""
admin.py -- DataCycle admin dashboard (Streamlit)
==================================================
Single pane of glass for non-technical users:
    - Pipeline freshness (when did each layer last update?)
    - Row counts per gold table
    - Quick action buttons (run gold, run KNIME, run weather, refresh PBI)
    - Live log tail
    - Configuration display (.env, masked)

Run with:
    streamlit run scripts/admin.py

Then open http://localhost:8501 in any browser.

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse, unquote

import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DB_URL      = os.getenv("DB_URL", "")
BRONZE_ROOT = Path(os.getenv("BRONZE_ROOT", "storage/bronze"))
if not BRONZE_ROOT.is_absolute():
    BRONZE_ROOT = PROJECT_ROOT / BRONZE_ROOT


# ── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DataCycle Admin",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
  .stMetric { background: #f8f9fa; padding: 10px; border-radius: 4px; }
  .small { font-size: 0.85em; color: #6c757d; }
  div[data-testid="stMetricValue"] { font-size: 1.3rem; }
</style>
""", unsafe_allow_html=True)


# ── HELPERS ───────────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    if not DB_URL:
        return None
    return create_engine(DB_URL, pool_pre_ping=True, pool_recycle=300)


def fmt_age(dt: datetime) -> str:
    """Human-readable age: '3 min ago', '2 hours ago'."""
    if dt is None:
        return "never"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    s = delta.total_seconds()
    if s < 60:
        return f"{int(s)}s ago"
    if s < 3600:
        return f"{int(s/60)} min ago"
    if s < 86400:
        return f"{int(s/3600)} hours ago"
    return f"{int(s/86400)} days ago"


def status_color(seconds_old: float, ok_thresh: int, warn_thresh: int) -> str:
    if seconds_old < ok_thresh:   return "🟢"
    if seconds_old < warn_thresh: return "🟡"
    return "🔴"


def run_in_background(cmd: list[str], label: str, log_path: Path) -> bool:
    """Spawn a subprocess and store its handle in session state. Returns True
    if started, False if another job is already running."""
    if st.session_state.get("running_job"):
        st.error(f"Another job is already running: {st.session_state['running_job']['label']}")
        return False
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log = open(log_path, "w", encoding="utf-8", buffering=1)
    proc = subprocess.Popen(
        cmd, cwd=str(PROJECT_ROOT), stdout=log, stderr=subprocess.STDOUT,
        text=True,
    )
    st.session_state["running_job"] = {
        "label": label, "pid": proc.pid, "log": str(log_path),
        "started": datetime.now(timezone.utc).isoformat(),
        "cmd": " ".join(cmd[1:]),
    }
    return True


def check_running_job():
    """Check if the tracked job is still running and clear state if not."""
    job = st.session_state.get("running_job")
    if not job:
        return None
    pid = job["pid"]
    if os.name == "nt":
        try:
            r = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"], capture_output=True, text=True)
            if str(pid) not in r.stdout:
                st.session_state["running_job"] = None
                return None
        except Exception:
            pass
    else:
        try:
            os.kill(pid, 0)
        except OSError:
            st.session_state["running_job"] = None
            return None
    return job


# ── HEADER ────────────────────────────────────────────────────────────────────
st.title("📊 DataCycle Admin")
st.caption("Pipeline status, quick actions, logs — all in one place. "
           f"Refresh: {datetime.now().strftime('%H:%M:%S')}")

if not DB_URL:
    st.error("DB_URL not set in .env — admin can't query the DB until that's fixed.")
    st.stop()


# ── DB CONNECTION ─────────────────────────────────────────────────────────────
engine = get_engine()
parsed = urlparse(DB_URL)
db_label = f"{unquote(parsed.username or '')}@{parsed.hostname}:{parsed.port or 5432}/{(parsed.path or '/').lstrip('/')}"

try:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    db_ok = True
except Exception as e:
    db_ok = False
    db_err = str(e)[:200]


# ── TOP STATUS ROW ────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns([2, 2, 3])
with col1:
    st.markdown("**Database**")
    if db_ok:
        st.markdown(f"🟢  `{db_label}`")
    else:
        st.markdown(f"🔴  `{db_label}`")
        st.caption(db_err)

with col2:
    st.markdown("**Watcher process**")
    watcher_running = False
    if os.name == "nt":
        try:
            r = subprocess.run(
                ["wmic", "process", "where", "name='python.exe' or name='pythonw.exe'",
                 "get", "commandline"],
                capture_output=True, text=True, timeout=5,
            )
            watcher_running = "watcher.py" in r.stdout.lower()
        except Exception:
            pass
    else:
        try:
            r = subprocess.run(["pgrep", "-f", "watcher.py"],
                               capture_output=True, text=True, timeout=5)
            watcher_running = bool(r.stdout.strip())
        except Exception:
            pass
    if watcher_running:
        st.markdown("🟢  Running")
    else:
        st.markdown("🟡  Not running")
        st.caption("Auto-start added to login? Or run `python ingestion/fast_flow/watcher.py` once.")

with col3:
    st.markdown("**Active job**")
    job = check_running_job()
    if job:
        st.markdown(f"🟡  Running: **{job['label']}**  (PID {job['pid']})")
        st.caption(f"started {fmt_age(datetime.fromisoformat(job['started']))}")
    else:
        st.markdown("⚪  None")

st.divider()


# ── DATA FRESHNESS ────────────────────────────────────────────────────────────
st.subheader("Data freshness")

if db_ok:
    sources = [
        ("Sensors — environment", "SELECT MAX(d.timestamp_utc) FROM gold.fact_environment_minute f JOIN gold.dim_datetime d ON d.datetime_key = f.datetime_key", 26*3600, 7*86400),
        ("Sensors — energy",      "SELECT MAX(d.timestamp_utc) FROM gold.fact_energy_minute f JOIN gold.dim_datetime d ON d.datetime_key = f.datetime_key", 26*3600, 7*86400),
        ("Sensors — presence",    "SELECT MAX(d.timestamp_utc) FROM gold.fact_presence_minute f JOIN gold.dim_datetime d ON d.datetime_key = f.datetime_key", 26*3600, 7*86400),
        ("Weather forecasts",     "SELECT MAX(d.timestamp_utc) FROM gold.fact_weather_hour f JOIN gold.dim_datetime d ON d.datetime_key = f.datetime_key", 36*3600, 14*86400),
        ("Predictions — motion",      "SELECT MAX(prediction_made_at) FROM gold.fact_prediction_motion", 48*3600, 14*86400),
        ("Predictions — consumption", "SELECT MAX(prediction_made_at) FROM gold.fact_prediction_consumption", 48*3600, 14*86400),
    ]

    cols = st.columns(3)
    for i, (label, sql, ok_th, warn_th) in enumerate(sources):
        with cols[i % 3]:
            try:
                with engine.connect() as conn:
                    latest = conn.execute(text(sql)).scalar()
                if latest is None:
                    st.metric(label, "no data", "—")
                else:
                    if hasattr(latest, "tzinfo") and latest.tzinfo is None:
                        latest = latest.replace(tzinfo=timezone.utc)
                    age_s = (datetime.now(timezone.utc) - latest).total_seconds() if hasattr(latest, "tzinfo") else 0
                    icon = status_color(age_s, ok_th, warn_th)
                    st.metric(f"{icon} {label}", fmt_age(latest),
                              latest.strftime('%Y-%m-%d %H:%M') if hasattr(latest, "strftime") else str(latest))
            except Exception as e:
                st.metric(label, "ERR", str(e)[:60])

st.divider()


# ── ROW COUNTS ────────────────────────────────────────────────────────────────
st.subheader("Gold tables")

if db_ok:
    tables = [
        "dim_apartment", "dim_room", "dim_device", "dim_date", "dim_datetime",
        "dim_tariff", "dim_weather_site",
        "fact_environment_minute", "fact_energy_minute", "fact_presence_minute",
        "fact_device_health_day", "fact_weather_hour",
        "fact_prediction_motion", "fact_prediction_consumption",
    ]
    rows_data = []
    for t in tables:
        try:
            with engine.connect() as conn:
                n = conn.execute(text(f"SELECT COUNT(*) FROM gold.{t}")).scalar()
            rows_data.append({"Table": f"gold.{t}", "Rows": f"{n:,}"})
        except Exception as e:
            rows_data.append({"Table": f"gold.{t}", "Rows": f"ERR: {str(e)[:40]}"})
    st.dataframe(rows_data, use_container_width=True, hide_index=True)

st.divider()


# ── QUICK ACTIONS ─────────────────────────────────────────────────────────────
st.subheader("Quick actions")
st.caption("Each button starts a job in the background. Watch its log below.")

action_cols = st.columns(4)
LOG_DIR = PROJECT_ROOT / "storage" / "admin_logs"

actions = [
    ("Run gold ETL (sensors)", [sys.executable, "-m", "etl.silver_to_gold.populate_gold", "--sensors"], "gold_sensors.log"),
    ("Run gold ETL (weather)", [sys.executable, "-m", "etl.silver_to_gold.populate_gold", "--weather"], "gold_weather.log"),
    ("Run weather backfill",   [sys.executable, str(PROJECT_ROOT / "ingestion/fast_flow/watcher.py"), "--weather"], "weather.log"),
    ("Run KNIME predictions",  [sys.executable, str(PROJECT_ROOT / "scripts/run_knime_predictions.py")], "knime.log"),
    ("Run silver backfill",    [sys.executable, str(PROJECT_ROOT / "ingestion/fast_flow/watcher.py"), "--scan"], "silver.log"),
    ("Cleanup old bronze",     [sys.executable, str(PROJECT_ROOT / "scripts/cleanup_bronze.py")], "cleanup.log"),
    ("Refresh Power BI",       ["powershell", "-NoProfile", "-Command",
                                "Start-Process -FilePath '" + str(PROJECT_ROOT / "bi/power_bi/DataCycleDomotic.pbix") + "'; "
                                "Start-Sleep 8; "
                                "(New-Object -ComObject WScript.Shell).SendKeys('^+{F5}')"], "pbi_refresh.log"),
    ("Open Power BI",          ["powershell", "-NoProfile", "-Command",
                                "Start-Process -FilePath '" + str(PROJECT_ROOT / "bi/power_bi/DataCycleDomotic.pbix") + "'"], "pbi_open.log"),
]

for i, (label, cmd, logname) in enumerate(actions):
    with action_cols[i % 4]:
        if st.button(label, use_container_width=True, key=f"btn_{i}"):
            log_path = LOG_DIR / logname
            if run_in_background(cmd, label, log_path):
                st.success(f"Started: {label}")
                st.rerun()

st.divider()


# ── LIVE LOG TAIL ─────────────────────────────────────────────────────────────
st.subheader("Logs")

LOG_DIR.mkdir(parents=True, exist_ok=True)
log_choices = sorted([p.name for p in LOG_DIR.glob("*.log")])
extra_logs = ["install.log"]
for el in extra_logs:
    p = PROJECT_ROOT / el
    if p.exists():
        log_choices.append(el)

if not log_choices:
    st.caption("No logs yet. Run an action to generate one.")
else:
    chosen = st.selectbox("Pick a log", log_choices)
    log_path = (PROJECT_ROOT / chosen) if (PROJECT_ROOT / chosen).exists() else (LOG_DIR / chosen)
    if log_path.exists():
        try:
            with open(log_path, encoding="utf-8", errors="replace") as f:
                content = f.read()
        except Exception as e:
            content = f"Could not read: {e}"
        # Tail to last ~300 lines
        lines = content.splitlines()[-300:]
        # Strip ANSI escape codes for cleaner display
        ansi = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        clean = "\n".join(ansi.sub("", line) for line in lines)
        st.text_area("tail (last 300 lines)", clean, height=320, key="logbox")
        st.caption(f"{log_path}  ·  {len(lines)} lines shown")

st.divider()


# ── CONFIG (masked) ───────────────────────────────────────────────────────────
with st.expander("Configuration (.env, masked)"):
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        st.warning(".env not found")
    else:
        try:
            content = env_file.read_text(encoding="utf-8")
        except Exception as e:
            content = f"# could not read: {e}"
        # Mask passwords and credentials
        masked_lines = []
        for line in content.splitlines():
            if "=" not in line or line.lstrip().startswith("#"):
                masked_lines.append(line)
                continue
            key, _, val = line.partition("=")
            if any(s in key.upper() for s in ("PASSWORD", "PASS", "SECRET", "KEY", "TOKEN")):
                masked_lines.append(f"{key}={'*' * 8}")
            elif "://" in val and "@" in val:
                # Redact password inside connection URL
                masked_lines.append(re.sub(r"(://[^:]+:)[^@]*(@)", r"\1********\2", line))
            else:
                masked_lines.append(line)
        st.code("\n".join(masked_lines), language="ini")


# ── FOOTER / AUTO-REFRESH ─────────────────────────────────────────────────────
st.caption("⚙️  This page auto-refreshes every 10s. Use buttons above to trigger pipelines.")
time.sleep(10)
st.rerun()
