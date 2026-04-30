# Technical Documentation

For developers, code reviewers, and the academic report.

## 1. Architecture overview

### Three external sources

| Source | Format | Volume | Path |
|---|---|---|---|
| **SMB share** (sensor data) | One JSON per minute per apartment | ~2 880 files/day for 2 apartments | `\\server\share\` → mounted as `Z:\` |
| **MySQL** (apartment metadata) | Tables: `apartment`, `room`, `device` | Static, refreshed weekly | School DB `pidb` |
| **sFTP** (weather forecasts) | One CSV per day per site, 24 prediction steps × 7 measurements × 1 site ≈ 150 k rows | 1 file/day | School sFTP server |

### Medallion layers (PostgreSQL 17)

```
storage\bronze\         (filesystem, raw immutable)
   └── jimmy\YYYY\MM\DD\HH\<filename>.json
   └── jeremie\YYYY\MM\DD\HH\<filename>.json
   └── weather\YYYY\MM\DD\Pred_YYYY-MM-DD.csv

silver.<table>          (PostgreSQL, normalized)
   ├── sensor_events     (apartment, room, sensor_type, field, value, unit, ts, is_outlier)
   ├── weather_forecasts (timestamp, site, prediction, prediction_date, measurement, value, ...)
   ├── apartments / rooms / devices  (mirrored from MySQL)
   ├── etl_watermark     (filename → processed_at, for sensor pipeline idempotency)
   └── weather_watermark (filename → processed_at, for weather pipeline idempotency)

gold.<table>            (PostgreSQL, star schema, OLAP-ready)
   ├── dim_apartment / dim_room / dim_device / dim_date / dim_datetime / dim_tariff / dim_weather_site
   ├── fact_environment_minute     (temperature, humidity, co2, noise, pressure, anomaly flag)
   ├── fact_energy_minute          (power_w, energy_kwh, is_valid)
   ├── fact_presence_minute        (motion, door, window flags)
   ├── fact_device_health_day      (battery, comm errors, last seen)
   ├── fact_weather_hour           (temp, humidity, precip, radiation, n_model_runs)
   ├── fact_prediction_motion      (KNIME-written, motion forecast 1h ahead)
   ├── fact_prediction_consumption (KNIME-written, consumption forecast 24h ahead)
   └── mv_energy_with_cost         (materialized view joining energy + tariff)
```

### Where each piece runs

```
                       ┌──────────────────────────────────────┐
                       │  watcher.py  (single long-running    │
                       │   Python process, the "scheduler")   │
                       └────────────┬─────────────────────────┘
                                    │ every 60 s    every 15 min      every day @ 06:30
                                    ▼               ▼                  ▼
SMB ──► bulk_to_bronze.py     flatten_sensors    populate_gold       weather_download
        (file copy)           (bronze→silver)    --sensors           clean_weather (b→s)
                                                                     populate_gold --weather
                                                                     run_knime_predictions
                                                                     cleanup_bronze

KNIME ◄─ deploys workflows from ml/knime/*.knwf into ~/knime-workspace/
         ◄── run_knime_predictions.py invokes knime.exe in batch mode

Power BI ◄─ patches host/port/db in .pbix at install (configure_bi_knime.py)
            User opens .pbix in Desktop → Refresh → live query against gold.*

Streamlit ◄─ scripts/admin.py serves http://localhost:8501
              same gold.* connection, plus subprocess buttons
```

## 2. Pipeline details

### 2.1 Bronze ingestion

#### Sensor JSONs (continuous)

`ingestion/fast_flow/bulk_to_bronze.py`:

- **Predictive mode** (default): looks at the newest filename already in
  bronze, predicts the next expected filename based on the timestamp +
  1-minute increment, checks `.exists()` on the SMB share. Stops after
  10 consecutive empty minutes. ~5 ms per check.
- **Full scan mode** (`--full` flag, used at install + nightly): does
  `os.scandir()` on the entire SMB folder, sorts results, copies anything
  not yet in bronze.
- **Skip list**: reads `storage\processed.log` (filenames already imported
  to silver and removed from bronze) so a full rescan doesn't re-copy them.
- **Storage layout**: `storage\bronze\<apt>\YYYY\MM\DD\HH\<filename>.json`,
  partition by hour to keep folders small.

#### Weather CSVs (daily)

`ingestion/slow_flow/weather_download.py`:

- Connects to sFTP via paramiko
- Lists remote `*.csv`, filters to ones not already present in bronze
- Sequential download (sFTP servers tend to dislike parallel sessions from
  the same client) — but with a progress bar
- Storage: `storage\bronze\weather\YYYY\MM\DD\Pred_YYYY-MM-DD.csv`

### 2.2 Bronze → Silver

#### Sensors

`etl/bronze_to_silver/flatten_sensors.py`:

- **Discovery**: full `rglob("*.json")` over each apartment's bronze tree,
  diff against `silver.etl_watermark` to find new files. (Earlier "stop
  after 50 watermarked files" optimization was removed — it was unsafe with
  parallel batches that complete out of order.)
- **Parallel parsing**: `ProcessPoolExecutor(max_workers=8)`, each worker
  takes a batch of 2 000 files, parses JSON, normalizes room names, applies
  outlier bounds (e.g. `temperature_c ∈ [-20, 60]`), returns rows.
- **Bulk upsert**: psycopg2 `copy_expert("COPY _tmp_sensor_events ...
  FROM STDIN ...")` into a temp table, then `INSERT INTO silver.sensor_events
  ... SELECT DISTINCT ON (...) FROM _tmp_sensor_events ON CONFLICT (...)
  DO UPDATE`. The DISTINCT ON dedupes within-batch (PostgreSQL forbids
  upserting the same key twice in one statement).
- **Watermark**: `INSERT INTO silver.etl_watermark VALUES %s ON CONFLICT
  DO NOTHING` via `psycopg2.execute_values`.
- **Aggressive bronze cleanup** (default on; disable with `KEEP_BRONZE=1`):
  after the upsert + watermark commit, delete the bronze JSONs and append
  filenames to `storage\processed.log`. Frees disk immediately rather than
  waiting for the daily retention pass.

Performance: ~30 k rows/second on the workers' main-thread upsert
bottleneck. ~220 k files × 60 events/file = ~13 M rows in 10-15 minutes.

#### Weather

`etl/bronze_to_silver/clean_weather.py`:

- **Parallel files**: `ProcessPoolExecutor(max_workers=4)`, each worker
  processes one CSV end-to-end (read with pandas, clean, COPY + upsert).
  Each worker has its own SQLAlchemy engine.
- **Cleaning**: drop rows with bad timestamps, filter to `WEATHER_MIN_YEAR`,
  drop sentinel `-99999.0` values, flag outliers via per-measurement bounds.
- **Bulk upsert**: same COPY + INSERT FROM SELECT pattern as sensors, on
  unique key `(timestamp, site, prediction, prediction_date, measurement)`.

Performance: 4× speedup vs sequential. ~297 files of 150 k rows in 15-20 min.

### 2.3 Silver → Gold

`etl/silver_to_gold/populate_gold.py` orchestrates three steps in order:

1. **`populate_dimensions`** — refresh `dim_apartment`, `dim_room`,
   `dim_device`, `dim_date`, `dim_datetime`, `dim_tariff`, `dim_weather_site`.
   Set-based SQL: `INSERT INTO gold.dim_X ... SELECT FROM silver.X ON
   CONFLICT DO NOTHING/UPDATE`. Anonymizes apartment metadata (owner_user_id
   → NULL, building_name → 'Building <id>').
2. **`populate_sensors`** — refresh `fact_environment_minute`,
   `fact_energy_minute`, `fact_presence_minute`, `fact_device_health_day`.
   Each is a single `INSERT INTO gold.fact_X ... SELECT FROM
   silver.sensor_events ... GROUP BY ... ON CONFLICT DO UPDATE`. Pivots
   the long-format `sensor_events` into wide-format minute facts.
3. **`populate_weather`** — refresh `fact_weather_hour` from
   `silver.weather_forecasts`, aggregating multiple model runs per hour
   (median value across runs).

Materialized views (`mv_energy_with_cost`) refreshed individually via
`REFRESH MATERIALIZED VIEW CONCURRENTLY` (with non-concurrent fallback
on first build of an empty MV).

### 2.4 Gold → ML (KNIME)

`scripts/run_knime_predictions.py` invokes `knime.exe` in batch mode:

```
knime.exe -consoleLog -nosplash -reset
  -application org.knime.product.KNIME_BATCH_APPLICATION
  -workflowDir=<workspace>/<workflow>
  -workflow.variable=db_user,<user>,String
  -workflow.variable=db_pwd,<password>,String
```

**Why `-workflow.variable`?** KNIME explicitly forbids overwriting password
fields via flow variables for security. So the workflows can't bind a
password flow variable directly to the PG Connector's password slot.

**The trick (Variable to Credentials):**

```
[String Configuration: db_user]  ─┐
[String Configuration: db_pwd]   ─┴─►  [Variable to Credentials]  ──►  [PG Connectors]
   (overridable via              (creates a real credential          (use credentials → "db")
    -workflow.variable)           object internally)
```

The two String Configuration nodes accept their values from
`-workflow.variable=...` (allowed for strings). The Variable to Credentials
node packs them into a credential object internally — KNIME's password
restriction never triggers because no `xpassword` field is being overwritten
from outside. The PG Connectors then read from the credential by name
(`db`), getting both user + password.

**Workflows shipped:**
- `Motion_Prediction_Server.knwf` — logistic regression, predicts motion
  probability 1 hour ahead per apartment / room
- `Consumption_Weather_Prediction_Server.knwf` — linear regression, predicts
  consumption 24 hours ahead, with weather as a feature

Both write to `gold.fact_prediction_*` via DB Writer nodes.

## 3. Data model details

### Apartment + room dimensions

```
gold.dim_apartment
  apartment_key   PK (surrogate)
  apartment_id    "jimmy" / "jeremie"  (natural key from sensor JSON filenames)
  building_name   anonymized to "Building 1" etc.
  occupant_name   first name only — see DECISIONS.md for GDPR rationale

gold.dim_room
  room_key        PK
  room_name       normalized (e.g. "Bdroom" → "Bedroom")
  apartment_key   FK
```

### Star schema for sensor facts

Every fact table has the same dim spine:

```
fact_X_minute (
    datetime_key    FK → dim_datetime    (1-minute grain)
    date_key        FK → dim_date
    room_key        FK → dim_room
    apartment_key   FK → dim_apartment
    device_key      FK → dim_device      (where applicable)
    <measure cols>
    is_valid / is_anomaly  flag
)
```

Unique constraint on `(datetime_key, room_key)` (or `device_key` for
energy) means upserts are idempotent.

### Time dimensions

- `dim_date` — one row per calendar day, with year / month / quarter /
  weekday columns. `date_key = YYYYMMDD::int`.
- `dim_datetime` — one row per minute, with `timestamp_utc`, hour, minute,
  is_business_hour. `datetime_key = YYYYMMDDHHMM::bigint`.

Generated for the entire range covered by the data (2023 onward).

### Predictions

```
gold.fact_prediction_motion
  prediction_made_at TIMESTAMPTZ   when KNIME ran the model
  target_at          TIMESTAMPTZ   what time the prediction is for (~1h ahead)
  apartment_key      FK
  room_key           FK
  motion_prob        FLOAT
  model_name         TEXT          ("logreg_v1", "logreg_v2", ...)

gold.fact_prediction_consumption
  prediction_made_at TIMESTAMPTZ
  target_at          TIMESTAMPTZ
  apartment_key      FK
  predicted_kwh      FLOAT
  model_name         TEXT
```

The `prediction_made_at` field lets you compare model versions or look at
prediction drift over time.

## 4. Configuration

Everything in `.env` (created by the installer from the wizard inputs):

```
SMB_PATH=Z:\
BRONZE_ROOT=storage\bronze
DB_URL=postgresql://domotic:<pwd>@localhost:5432/domotic_tests
MYSQL_URL=mysql+pymysql://...
SFTP_HOST=...
SFTP_USER=...
SFTP_PASSWORD=...
SFTP_PATH=/forecasts
WEATHER_MIN_YEAR=2023
WEATHER_SITES=Aadorf / Tänikon
PBI_SERVER=...
PBI_DATABASE=...

# Tunables (optional; defaults shown)
GOLD_INTERVAL_MIN=15
WEATHER_HOUR=6
WEATHER_MIN=30
KEEP_BRONZE=0           # 1 to disable aggressive bronze cleanup
CLEAN_WEATHER_WORKERS=4
BRONZE_RETENTION_DAYS=30  # used by cleanup_bronze.py for the retention pass
```

The PostgreSQL admin password is **never** written to `.env` — only used
during install for the one-time admin operations.

## 5. Security

| Concern | Mitigation |
|---|---|
| Postgres admin credentials on disk | Used only at install time, never written to `.env` |
| App user privileges | `domotic` has only DML / DDL on `silver` + `gold` schemas |
| Power BI dashboard data leakage | RLS by `apartment_key`; tenants can't see each other's data |
| KNIME passwords baked in workflows | Avoided — Variable to Credentials accepts password as String flow variable at runtime |
| GDPR — personal data | Only first names retained (Art. 4(1) considers them low-risk identifiers absent additional context). User IDs and building names are anonymized in gold. |
| GDPR — right to erasure | Supported: `DELETE FROM gold.dim_apartment WHERE apartment_id = '...'` cascades through `apartment_key` FKs |

See `DECISIONS.md` for the full GDPR analysis (ADR 005).

## 6. Performance numbers

(Measured on the project VM — 8 cores, 32GB RAM, local Postgres 17.)

| Phase | Throughput |
|---|---|
| `bulk_to_bronze` (SMB → bronze) | ~150 files/sec, 16 parallel threads |
| `flatten_sensors` (bronze → silver) | ~30 k rows/sec, COPY-based upsert |
| `clean_weather` (bronze → silver) | ~15 files/min, 4 parallel processes |
| `populate_gold` (silver → gold) | ~10 M rows in ~30 sec, set-based SQL |
| KNIME prediction workflow | 5 min per workflow |

A full **first-time install** with empty Postgres and ~220 k bronze files
takes **45-60 minutes** end to end on the project VM.

## 7. Idempotency guarantees

Every step is safe to re-run:

- `bulk_to_bronze`: skips existing files in bronze
- `flatten_sensors`: `silver.etl_watermark` skips already-processed files
- `clean_weather`: `silver.weather_watermark` does the same
- `populate_dimensions`: `INSERT ... ON CONFLICT DO NOTHING/UPDATE`
- `populate_sensors` / `populate_weather`: `INSERT ... ON CONFLICT DO UPDATE`
- KNIME workflows: `INSERT ... ON CONFLICT DO UPDATE` on prediction tables
- `cleanup_bronze`: only deletes files older than `BRONZE_RETENTION_DAYS`

So re-running the installer (or any individual step) never duplicates
data and never crashes from "already exists" errors.

## 8. Project layout

```
data-cycle-domotic/
├── ingestion/
│   ├── fast_flow/
│   │   ├── watcher.py             # main scheduler / event loop
│   │   └── bulk_to_bronze.py      # SMB → bronze
│   └── slow_flow/
│       └── weather_download.py    # sFTP → bronze
├── etl/
│   ├── bronze_to_silver/
│   │   ├── flatten_sensors.py     # JSON → silver.sensor_events (COPY upsert)
│   │   ├── clean_weather.py       # CSV → silver.weather_forecasts (parallel)
│   │   ├── import_mysql_to_silver.py
│   │   └── create_silver.py       # DDL for silver tables
│   └── silver_to_gold/
│       ├── create_gold.py         # DDL for gold star schema
│       ├── populate_gold.py       # orchestrator
│       ├── populate_dimensions.py
│       ├── populate_sensors.py
│       └── populate_weather.py
├── ml/
│   └── knime/
│       ├── Motion_Prediction_Server.knwf
│       └── Consumption_Weather_Prediction_Server.knwf
├── bi/
│   └── power_bi/
│       └── DataCycleDomotic.pbix  # Power BI report with RLS
├── scripts/
│   ├── admin.py                   # Streamlit admin pane
│   ├── admin.bat                  # one-click launcher
│   ├── status.py                  # CLI version of the admin pane
│   ├── configure_bi_knime.py      # patches host/port/db in .pbix and .knwf
│   ├── deploy_knime.py            # extracts .knwf into KNIME workspace
│   ├── run_knime_predictions.py   # invokes knime.exe batch
│   └── cleanup_bronze.py          # daily retention pass
├── installer/
│   └── install_template.py        # consumed by web wizard, generates data-cycle-installer.py
├── website/                       # git submodule, the install wizard
└── docs/v2/                       # this folder
```
