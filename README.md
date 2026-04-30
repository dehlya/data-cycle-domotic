# Data Cycle Project — Apartment Domotics

> **Group 14 · HES-SO Valais 2026 · Use Case 02**
> Smart-apartment IoT data platform — energy, comfort, presence and predictions for two real apartments in Valais.

---

## What this is

End-to-end data pipeline for two smart apartments. Sensor JSON files land on
an SMB share every minute, weather forecasts arrive daily over sFTP, and an
apartment registry lives in a school-managed MySQL database. We turn all
three sources into clean star-schema tables in PostgreSQL, run KNIME ML
workflows for motion + consumption predictions, and surface everything in
Power BI with row-level security per tenant. A self-contained Python
installer deploys the whole thing onto a single Windows VM.

```
SMB share (sensor JSON) ─┐
MySQL  (apartment dims)  ─┼─►  Bronze ──► Silver ──► Gold ──► KNIME predictions
sFTP   (weather CSV)     ─┘   (raw files) (Postgres) (star)   (motion + consumption)
                                                       │
                                                       ├─► Power BI dashboards (RLS)
                                                       └─► Streamlit admin pane
```

Everything runs locally on one Windows VM. No cloud dependencies.

---

## Stack

| Layer | What we use |
|---|---|
| Ingestion | Python 3.10+ (threads + multiprocessing, paramiko, SMB) |
| Orchestration | Custom Python watcher loop — 60 s tick + daily weather + nightly catch-up |
| Bronze | Local file system, timestamped folders, gzip-compressed after silver |
| Silver / Gold | PostgreSQL 17 |
| ETL | pandas + SQLAlchemy + psycopg2 `COPY` upsert |
| ML | KNIME Analytics Platform **5.8** (pinned), batch mode invocation |
| BI | Power BI Desktop with row-level security |
| Admin pane | Streamlit (local, http://localhost:8501) |
| Deployment | Self-contained Python installer + interactive web wizard |

---

## Repo layout

```
ingestion/
  fast_flow/
    bulk_to_bronze.py       # SMB → Bronze, predictive ingestion
    watcher.py              # the orchestrator (60 s loop + daily + nightly)
  slow_flow/
    weather_download.py     # sFTP weather CSV → Bronze
etl/
  bronze_to_silver/         # flatten_sensors, clean_weather, import_mysql_to_silver
  silver_to_gold/           # create_gold, populate_dimensions, populate_sensors,
                            # populate_weather, run_gold orchestrator
ml/
  knime/                    # 2 .knwf workflows, pinned to KNIME 5.8
bi/
  power_bi/                 # DataCycleDomotic.pbix dashboards
  dax/                      # measure references
  exports/                  # static exports
scripts/
  admin.py / admin.bat      # Streamlit ops dashboard
  run_knime_predictions.py  # headless KNIME runner
  fast_silver_backfill.py   # drop-constraint backfill (first install only)
  cleanup_bronze.py         # retention enforcement
installer/
  install_template.py       # the one script that deploys everything
security/                   # credentials & GDPR notes
docs/v2/                    # all customer-facing documentation
storage/                    # local data — gitignored, populated at runtime
tests/                      # pytest suites
```

---

## Quickstart

**Recommended (guided install via the project website):**

1. Visit the project's `/install` page
2. Fill in your DB host, app password, sFTP credentials, SMB share path
3. Download `data-cycle-installer.py` and run it:
   ```powershell
   python data-cycle-installer.py
   ```
4. The installer clones this repo, creates a venv, installs deps, sets up the
   database, runs the bootstrap ETL, configures KNIME workflows for runtime
   credential injection, and (optionally) registers the watcher to auto-start
   on every login.
5. When it finishes, the Streamlit admin dashboard auto-launches at
   <http://localhost:8501>

**Timing:** ~4 hours on a fresh install (silver backfill is the long pole —
years of sensor history land in one go). On a re-run it's ~15 minutes —
watermarks + the `processed.log` skip-list make every step idempotent.

**Manual setup** (devs only):

```powershell
cp .env.example .env                                 # fill in credentials
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
.\.venv\Scripts\python -m etl.bronze_to_silver.create_silver
.\.venv\Scripts\python -m etl.silver_to_gold.create_gold
.\.venv\Scripts\python -m etl.silver_to_gold.populate_gold
.\.venv\Scripts\python ingestion\fast_flow\watcher.py
```

---

## Documentation

All customer-facing docs live in [`docs/v2/`](docs/v2/):

| File | Audience | Topic |
|---|---|---|
| [`docs/v2/README.md`](docs/v2/README.md) | All | Overview, table-of-contents |
| [`docs/v2/INSTALLATION.md`](docs/v2/INSTALLATION.md) | End user / IT | Step-by-step install, troubleshooting |
| [`docs/v2/USER_GUIDE.md`](docs/v2/USER_GUIDE.md) | End user | Dashboards, admin pane, day-to-day usage |
| [`docs/v2/TECHNICAL.md`](docs/v2/TECHNICAL.md) | Devs / report | Architecture, data model, performance, internals |
| [`docs/v2/OPERATIONS.md`](docs/v2/OPERATIONS.md) | Ops / maintainer | Runbook, monitoring, common ops |
| [`docs/v2/DECISIONS.md`](docs/v2/DECISIONS.md) | All | Architecture decision records (ADRs) |

The [`security/`](security/) folder covers the GDPR / anonymisation /
credential story specifically.

The [`ml/knime/`](ml/knime/) folder has its own README for the KNIME workflow
setup (PostgreSQL connector + Variable-to-Credentials pattern).

---

## Authors

Group 14 — HES-SO Valais Data Engineering, Spring 2026.
Dehlya Herbelin · Sacha · Johann
