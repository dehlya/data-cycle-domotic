# Technical Documentation

For developers, code reviewers, and the academic report. Covers architecture,
data model, pipeline internals, performance, security and the optimisations
that matter.

## 1. Architecture overview

### Three external sources

| Source | Format | Volume | Path |
|---|---|---|---|
| **SMB share** (sensor data) | One JSON per minute per apartment, ~60 sensor events per file | ~2 880 files/day for 2 apartments | `\\server\share\` → mounted as `Z:\` on the VM |
| **MySQL** (apartment metadata) | Tables `buildings`, `buildingtype`, `rooms`, `sensors`, `devices`, `profile`, `profilereference`, `parameters`, `parameterstype`, `dierrors` | Static, refreshed at install (idempotent) | School DB `pidb` (a.k.a. `Appartments`) at `10.130.25.152:3306` |
| **sFTP** (weather forecasts) | One CSV per day per site, 24 prediction steps × 7 measurements ≈ 150 k rows | 1 file/day | School sFTP server, path `/Meteo2` |

Skipped MySQL tables (PII / out-of-scope): `users`, `events`, `actions`,
`achievements`, `badges`, `userrelationships`. See [DECISIONS.md](DECISIONS.md)
ADR-005 for the GDPR rationale.

### Medallion layers (PostgreSQL 17)

```
storage\bronze\         (filesystem, raw immutable, gzip-compressed after silver)
   ├── jimmy\YYYY\MM\DD\HH\<filename>.json[.gz]
   ├── jeremie\YYYY\MM\DD\HH\<filename>.json[.gz]
   └── weather\YYYY\MM\DD\Pred_YYYY-MM-DD.csv

silver.<table>          (PostgreSQL, normalized, idempotent)
   ├── sensor_events       (apartment, room, sensor_type, field, value, unit, timestamp, is_outlier)
   ├── weather_forecasts   (timestamp, site, prediction, prediction_date, measurement, value, ...)
   ├── apartment_metadata  (joined snapshot of MySQL dims)
   ├── di_errors_clean     (cleaned + transformed sensor errors from dierrors)
   ├── dim_buildings, dim_rooms, dim_devices, dim_sensors  (mirrored from MySQL)
   ├── etl_watermark       (filename → processed_at, sensor pipeline)
   └── weather_watermark   (filename → processed_at, weather pipeline)

gold.<table>            (PostgreSQL, star schema, BI-ready)
   ├── dim_apartment / dim_room / dim_device                (anonymised — see §5)
   ├── dim_date / dim_datetime / dim_tariff / dim_weather_site
   ├── fact_environment_minute     (temperature, humidity, co2, noise, pressure, window/door flags)
   ├── fact_energy_minute          (power_w, energy_kwh, is_valid)
   ├── fact_presence_minute        (motion_count, presence_flag, presence_prob)
   ├── fact_device_health_day      (uptime_pct, error_count, battery_min/avg)
   ├── fact_weather_hour           (temp, humidity, precip, radiation, n_model_runs)
   ├── fact_prediction_motion      (KNIME-written, motion forecast 1 h ahead)
   ├── fact_prediction_consumption (KNIME-written, consumption forecast 24 h ahead)
   └── mv_energy_with_cost         (materialised view: fact_energy_minute ⨝ dim_tariff)
```

### Where each piece runs

```
                       ┌─────────────────────────────────────────┐
                       │  watcher.py  (single long-running       │
                       │   Python process — the orchestrator)    │
                       └────────────┬────────────────────────────┘
                                    │ every 60 s    every 15 min     every day @ 07:30   nightly @ 00:00
                                    ▼               ▼                 ▼                    ▼
SMB ──► bulk_to_bronze.py     flatten_sensors    populate_gold       weather_download      bulk_to_bronze
        (file copy +          (bronze→silver,    --sensors           clean_weather (b→s)   --full (full SMB
         predictive scan)      COPY upsert,                          populate_gold         scan, catches any
                               compress bronze)                      --weather             missed minutes)
                                                                     run_knime_predictions
                                                                     cleanup_bronze

KNIME ◄─ workflows live in ml/knime/*.knwf, deployed to ~/knime-workspace/
         ◄── run_knime_predictions.py invokes knime.exe in batch mode (KNIME 5.8 pinned)

Power BI ◄─ admin pane wizard walks the user through one-time .pbix re-pointing
            User opens .pbix in Desktop → Refresh → live query against gold.*
            (RLS per apartment_key)

Streamlit ◄─ scripts/admin.py serves http://localhost:8501
              freshness dashboard, row counts, one-click ops, log tail,
              first-time PBI setup wizard
```

## 2. Pipeline details

### 2.1 Bronze ingestion

#### Sensor JSON files (continuous)

`ingestion/fast_flow/bulk_to_bronze.py`:

- **Predictive mode** (default, every 60 s): looks at the newest filename
  already in bronze, predicts the next expected filename based on the
  timestamp + 1-minute increment, checks `.exists()` on the SMB share. Stops
  after 10 consecutive empty minutes. ~5 ms per check vs minutes for a full
  scan — cheap enough to run every minute.
- **Full scan mode** (`--full`, used at install + nightly): does
  `os.scandir()` on the entire SMB folder, sorts results, copies anything not
  yet in bronze. Catches any minutes the predictive mode missed.
- **Skip list**: reads `storage\processed.log` (filenames already imported to
  silver) so a full rescan doesn't re-copy them.
- **Compressed bronze recognition**: globs `*.json*` so already-processed
  files (now `.json.gz` after compress-after-silver, see §2.2) are still seen
  as "present" and not re-copied.
- **Storage layout**: `storage\bronze\<apt>\YYYY\MM\DD\HH\<filename>.json`,
  partitioned by hour to keep folders small.

#### Weather CSVs (daily at 07:30)

`ingestion/slow_flow/weather_download.py`:

- Connects to sFTP via paramiko with strict-host-checking optional, 3 retries
  with 600 s back-off (sFTP servers occasionally drop connections).
- Lists remote `*.csv`, filters to ones not already present in bronze.
- **Sequential download** — sFTP servers tend to dislike parallel sessions
  from the same client. With a progress bar.
- Storage: `storage\bronze\weather\YYYY\MM\DD\Pred_YYYY-MM-DD.csv`.

### 2.2 Bronze → Silver

#### Sensors

`etl/bronze_to_silver/flatten_sensors.py`:

- **Discovery**: full `rglob("*.json*")` over each apartment's bronze tree
  (matches `.json` and `.json.gz`), diff against `silver.etl_watermark` to
  find new files. Uses canonical filename (strips trailing `.gz`) so the
  watermark stays stable across compression.
- **Parallel parsing**: `ProcessPoolExecutor(max_workers=8)`, each worker
  takes a batch of **5 000 files** (`BATCH_SIZE`), parses JSON (gzip-aware),
  normalises room names, applies outlier bounds (e.g. `temperature_c ∈
  [-20, 60]`), returns rows.
- **Bulk upsert** (the optimisation that matters): psycopg2
  `copy_expert("COPY tmp_sensor_events ... FROM STDIN ...")` into a TEMP
  TABLE, then a single
  ```sql
  INSERT INTO silver.sensor_events
  SELECT DISTINCT ON (apartment, room, sensor_type, field, timestamp) *
  FROM tmp_sensor_events
  ON CONFLICT (apartment, room, sensor_type, field, timestamp)
  DO UPDATE SET value = EXCLUDED.value, is_outlier = EXCLUDED.is_outlier
  ```
  COPY is the fastest bulk-load primitive in Postgres — typically **5–10×
  faster than `execute_values`** and **50–150× faster than per-row INSERT**.
  The `DISTINCT ON` is required because Postgres forbids upserting the same
  key twice in one statement (would happen with duplicate rows in a batch).
- **Watermark**: `INSERT INTO silver.etl_watermark VALUES %s ON CONFLICT
  DO NOTHING` via `psycopg2.execute_values` — committed in the same
  transaction as the upsert.
- **Compress-after-silver** (default ON; `KEEP_BRONZE=1` to disable;
  `DELETE_BRONZE=1` for hard-delete instead): after the upsert + watermark
  commit, gzip the bronze JSON in place (`<file>.json` → `<file>.json.gz`,
  ~10–15× smaller). Preserves the audit trail (you can replay silver from
  bronze at any time) while reclaiming disk. Replaces an earlier
  delete-after-silver policy that destroyed evidence on errors.
- **Skip log**: filenames also appended to `storage\processed.log` so a full
  SMB rescan on the next watcher tick doesn't re-copy them.

**First-install backfill (`scripts/fast_silver_backfill.py`):** on a fresh
install with hundreds of thousands of files, even the COPY trick is
bottlenecked by the unique-index update. The script runs four phases:

1. Drop the unique constraint on `(apartment, room, sensor_type, field, timestamp)`
2. Run `flatten_sensors` (5–10× faster without the index)
3. `DELETE FROM s1 USING s2 WHERE s1.id > s2.id AND <key cols match>` to dedupe
4. Pre-flight duplicate-key check, then re-add the unique constraint

The pre-flight check is what makes it safe — if any residual duplicates
remain, the script aborts cleanly with samples instead of letting the
ALTER TABLE throw mid-statement (which would leave the table in an
unconstrainted state).

**Performance**: ~30 k rows/sec on the worker upsert path. ~220 k files ×
60 events/file ≈ 13 M rows in 10–15 minutes once the index slowdown wall is
solved by tuning (see §6).

#### Weather

`etl/bronze_to_silver/clean_weather.py`:

- **Parallel files**: `ProcessPoolExecutor(max_workers=4)`, each worker
  processes one CSV end-to-end (read with pandas, clean, COPY + upsert).
  Each worker has its own SQLAlchemy engine.
- **Cleaning**: drop rows with bad timestamps, filter to `WEATHER_MIN_YEAR`,
  drop sentinel `-99999.0` values, flag outliers via per-measurement bounds.
- **Bulk upsert**: same COPY → TEMP TABLE → INSERT FROM SELECT pattern as
  sensors, on unique key `(timestamp, site, prediction, prediction_date,
  measurement)`.

**Performance**: 4× speedup vs sequential. ~300 files at 150 k rows each in
15–20 min on a fresh install; ~16 s for the daily incremental.

### 2.3 Silver → Gold

`etl/silver_to_gold/populate_gold.py` orchestrates a 9-step process:

1. **`populate_dimensions`** — refresh `dim_apartment`, `dim_room`,
   `dim_device`, `dim_date`, `dim_datetime`, `dim_tariff`, `dim_weather_site`.
   Set-based SQL: `INSERT INTO gold.dim_X ... SELECT FROM silver.X ON
   CONFLICT DO NOTHING/UPDATE`. Anonymises apartment metadata
   (`owner_user_id` → NULL, `building_name` → `Building <id>`; first names
   retained as RLS pseudonyms — see §5).
2. **`populate_sensors`** — refresh the four sensor fact tables in one pass.
   Each is a single `INSERT INTO gold.fact_X ... SELECT FROM
   silver.sensor_events ... GROUP BY ... ON CONFLICT DO UPDATE`. Pivots the
   long-format `sensor_events` into wide-format minute facts.
3. **`populate_weather`** — refresh `fact_weather_hour` from
   `silver.weather_forecasts`, aggregating multiple model runs per hour
   (median value across runs, `n_model_runs` count preserved).
4. **`populate_health`** — daily device-health rollup from
   `silver.di_errors_clean` joined against expected-readings count.
5. **Refresh** `mv_energy_with_cost` materialised view —
   `REFRESH MATERIALIZED VIEW CONCURRENTLY` (with non-concurrent fallback on
   first build of an empty MV).
6. KNIME prediction tables are written *by KNIME* directly, not by us — see
   §2.4.
7. `VACUUM ANALYZE` on the changed fact tables — keeps query plans accurate.
8. Print row counts (visible in admin pane and logs).
9. Update `gold.populate_log` with timestamp + duration.

**Session-level `work_mem` tuning**: each populate pass runs `SET work_mem
= '256MB'` first, so the large `GROUP BY` queries stay in RAM rather than
spilling to disk.

### 2.4 Gold → ML (KNIME)

`scripts/run_knime_predictions.py` invokes `knime.exe` in batch mode:

```
knime.exe -consoleLog -nosplash -reset
  -application org.knime.product.KNIME_BATCH_APPLICATION
  -workflowDir=<workspace>/<workflow>
  -workflow.variable=db_user,<user>,String
  -workflow.variable=db_pwd,<password>,String
```

**KNIME 5.8 version pin (important).** The `.knwf` files are pinned to
KNIME `5.8.0.v202510151000` in their `created_by` XML field. KNIME refuses
to load a workflow exported from a newer version, so re-exporting from
KNIME 5.9+ on a dev laptop will break headless runs on a 5.8 VM. Either
re-export from the same KNIME version that target machines have, or patch
the version stamp back in the `.knwf` zip.

**Why `-workflow.variable` for credentials?** KNIME explicitly forbids
overwriting `xpassword` fields via flow variables for security. So workflows
can't bind a password flow variable directly to the PG Connector's password
slot.

**The trick (Variable → Credentials):**

```
[String Configuration: db_user]  ─┐
[String Configuration: db_pwd]   ─┴─►  [Variable to Credentials]  ──►  [PG Connectors]
   (overridable via                 (creates a real credential       (use credentials → "db")
    -workflow.variable)              object internally)
```

The two String Configuration nodes accept their values from
`-workflow.variable=...` (allowed for strings). The Variable to Credentials
node packs them into a credential object — KNIME's password restriction
never triggers because no `xpassword` field is being overwritten from
outside. The PG Connectors then read from the credential by name (`db`),
getting both user + password.

**Workflows shipped:**

- `Motion_Prediction_Server.knwf` — logistic regression, predicts motion
  probability 1 hour ahead per (apartment, room, 15-min slot)
- `Consumption_Weather_Prediction_Server.knwf` — linear regression with
  weather features, predicts consumption 24 hours ahead

Both write back to `gold.fact_prediction_*` via DB Writer nodes. KNIME
defines the column types (we don't pre-create the tables). Each table is
shaped roughly:

```
fact_prediction_motion        fact_prediction_consumption
  predicted_occupied            predicted_power_w
  actual_occupied               actual_power_w
  apartment                     apartment
  room                          room
  timestamp_rounded (15-min)    timestamp_rounded
  model_name='logistic_regression' model_name='linear_regression'
  target='Presence'             target='Consumption'
```

## 3. Data model details

### Apartment + room dimensions

```
gold.dim_apartment
  apartment_key   PK (surrogate)
  apartment_id    "jimmy" / "jeremie"  (natural key from sensor JSON filenames)
  building_name   anonymised to "Building <building_id>"
  name            first name only — see DECISIONS.md (ADR-005) for GDPR rationale
  weather_site_key  FK → dim_weather_site (so we know which forecast site to use)

gold.dim_room
  room_key        PK
  room_name       normalised (e.g. "Bdroom" → "Bedroom")
  room_type       generic category (bathroom, bedroom, kitchen, …)
  apartment_key   FK
```

### Star schema for sensor facts

Every fact table has the same dim spine:

```
fact_X_minute (
    datetime_key    FK → dim_datetime    (1-minute grain, YYYYMMDDHHMM)
    date_key        FK → dim_date        (1-day grain,    YYYYMMDD)
    room_key        FK → dim_room
    apartment_key   FK → dim_apartment
    device_key      FK → dim_device      (where applicable, e.g. fact_energy_minute)
    <measure cols>
    is_valid / is_anomaly  flag
)
```

Unique constraint on `(datetime_key, room_key)` (or `device_key` for energy)
makes upserts idempotent.

### Time dimensions

- `dim_date` — one row per calendar day, with year / month / quarter /
  weekday / week / `is_weekend` / `is_holiday` columns. `date_key =
  YYYYMMDD::int`.
- `dim_datetime` — one row per minute, with `timestamp_utc`, hour, minute,
  day-of-week, business-hour flags. `datetime_key = YYYYMMDDHHMM::bigint`.

Both are pre-generated for the entire range covered by the data (2023
onward) so dim joins always succeed.

### Tariff dimension

```
gold.dim_tariff
  tariff_key      PK
  provider        text   (e.g. "OIKEN")
  year            int
  chf_per_kwh     numeric
```

Both apartments are on Oiken's network in Sion. Seeded with the published
2023–2025 rate of 0.34 CHF/kWh; `mv_energy_with_cost` joins on year so cost
calculations stay correct as tariffs change.

### Predictions

KNIME owns these tables — schema is defined by the workflow's DB Writer.
Quoted in §2.4 above. Both share the same shape: predicted vs actual
side-by-side at 15-minute grain, per (apartment, room), tagged with
`model_name` and `target` so multiple model versions can coexist.

## 4. Configuration

Everything lives in `.env` at the project root (created by the installer
from the wizard's form inputs):

```
SMB_PATH=Z:\
BRONZE_ROOT=storage\bronze
DB_URL=postgresql://domotic:<pwd>@localhost:5432/domotic
MYSQL_URL=mysql+pymysql://...
SFTP_HOST=...
SFTP_USER=...
SFTP_PASSWORD=...
SFTP_PATH=/Meteo2
WEATHER_MIN_YEAR=2023
WEATHER_SITES=Sion

# Tunables (optional; defaults shown)
GOLD_INTERVAL_MIN=15
WEATHER_HOUR=7
WEATHER_MIN=30
KEEP_BRONZE=0             # 1 = skip compress-after-silver, keep raw .json files
DELETE_BRONZE=0           # 1 = hard-delete after silver instead of compressing
BRONZE_RETENTION_DAYS=30  # cleanup_bronze.py retention pass; -1 = keep forever
CLEAN_WEATHER_WORKERS=4
```

Tuning knobs that don't live in `.env` (Python module constants):

| Constant | Default | Where | What |
|---|---|---|---|
| `BATCH_SIZE` | 5 000 | `flatten_sensors.py` | Files per batch — bigger = fewer round-trips |
| `WORKERS` | 8 | `flatten_sensors.py` | Parallel parse workers |
| `work_mem` (Postgres) | 256 MB | per-session, set in `populate_gold.py` | Large `GROUP BY` stays in RAM |
| `shared_buffers` (Postgres) | **4 GB** | `postgresql.conf` | Hot index in RAM — see §6 |

The PostgreSQL admin password is **never** written to `.env` — only used
once during install for the role + database creation.

## 5. Security & GDPR

| Concern | Mitigation |
|---|---|
| Postgres admin credentials on disk | Used only at install, never written to `.env` |
| App-user privileges | `domotic` has DML/DDL only on `silver` + `gold` schemas |
| Power BI dashboard data leakage | RLS on `dim_apartment.apartment_key` — tenants can't see each other's data |
| KNIME passwords baked in workflows | Avoided via Variable → Credentials chain (§2.4) |
| GDPR — personal data | First names retained as RLS pseudonyms (Art. 4(1) considers them low-risk identifiers absent additional context). User IDs stripped, building names masked. |
| GDPR — right to erasure | `DELETE FROM gold.dim_apartment WHERE apartment_id = '...'` cascades through `apartment_key` FKs |
| KNIME version drift | `.knwf` files pinned to 5.8 in `created_by`; CI / re-export from same version |

See [DECISIONS.md](DECISIONS.md) for the full GDPR analysis (ADR-005), the
compress-vs-delete trade-off (ADR-007), and the `security/` folder for the
detailed threat model.

## 6. Performance numbers

Measured on the project VM — 8 cores, 32 GB RAM, local Postgres 17.

| Phase | Throughput / time |
|---|---|
| `bulk_to_bronze` (SMB → bronze) | ~150 files/sec, 16 parallel threads |
| `flatten_sensors` (bronze → silver, with COPY upsert) | ~30 k rows/sec |
| `clean_weather` (bronze → silver, 4 workers) | ~15 files/min |
| `populate_gold` (silver → gold, all 9 steps) | ~10 M rows in ~30 s |
| KNIME prediction (motion) | ~7 min |
| KNIME prediction (consumption) | ~3 min |

### The unique-index slowdown wall

On a fresh install with ~220 k bronze files, the `flatten_sensors` ETA
climbs steadily as the unique index on `silver.sensor_events` grows beyond
RAM — every `INSERT ... ON CONFLICT` requires a B-tree lookup that thrashes
disk. ETAs of 4 + hours are common without tuning.

Two fixes solve it:

1. **`shared_buffers = 4 GB`** in `postgresql.conf` (default is 128 MB) —
   keeps the unique index hot in RAM. Single biggest win; brings the ETA
   from 4 h to ~6 min for the full backfill.
2. **`scripts/fast_silver_backfill.py`** — drops the constraint, loads
   freely, dedupes, re-adds the constraint with a pre-flight check. Used
   for the very first install only; subsequent re-runs use the COPY +
   ON CONFLICT path.

### End-to-end install timing

| Scenario | Duration |
|---|---|
| **First install** (empty Postgres, 220 k bronze files, all sources fresh) | **~4 hours** |
| **Re-install** (same machine, watermarks intact) | **~15 minutes** |

Re-runs are fast because the watermark + `processed.log` skip-list let every
step short-circuit on already-done work.

## 7. Idempotency guarantees

Every step is safe to re-run:

- `bulk_to_bronze`: skips existing files in bronze (recognises both `.json`
  and `.json.gz`); also reads `processed.log` to skip files already imported
- `flatten_sensors`: `silver.etl_watermark` skips already-processed files
- `clean_weather`: `silver.weather_watermark` does the same
- `populate_dimensions`: `INSERT ... ON CONFLICT DO NOTHING/UPDATE`
- `populate_sensors` / `populate_weather`: `INSERT ... ON CONFLICT DO UPDATE`
- KNIME workflows: `INSERT ... ON CONFLICT DO UPDATE` on prediction tables
- `cleanup_bronze`: only acts on files older than `BRONZE_RETENTION_DAYS`

Belt-and-suspenders for the sensor flow: the watermark is in Postgres
(transactional with the data write), and `processed.log` is a flat file
appended after compress. Even if Postgres is wiped and restored, the skip
list keeps the cost of re-copying SMB files at zero.

## 8. Project layout

```
data-cycle-domotic/
├── ingestion/
│   ├── fast_flow/
│   │   ├── watcher.py              # main scheduler / event loop
│   │   └── bulk_to_bronze.py       # SMB → bronze, predictive scan
│   └── slow_flow/
│       └── weather_download.py     # sFTP → bronze
├── etl/
│   ├── bronze_to_silver/
│   │   ├── flatten_sensors.py      # JSON → silver.sensor_events (COPY upsert + compress)
│   │   ├── clean_weather.py        # CSV → silver.weather_forecasts (parallel)
│   │   ├── import_mysql_to_silver.py
│   │   └── create_silver.py        # DDL for silver tables
│   └── silver_to_gold/
│       ├── create_gold.py          # DDL for gold star schema
│       ├── populate_gold.py        # 9-step orchestrator
│       ├── populate_dimensions.py  # incl. anonymisation
│       ├── populate_sensors.py
│       └── populate_weather.py
├── ml/
│   └── knime/
│       ├── Motion_Prediction_Server.knwf            (KNIME 5.8 pinned)
│       ├── Consumption_Weather_Prediction_Server.knwf (KNIME 5.8 pinned)
│       └── README.md               # workflow setup + Variable→Credentials trick
├── bi/
│   ├── power_bi/
│   │   └── DataCycleDomotic.pbix   # Power BI report with RLS
│   ├── dax/                        # measure references
│   └── exports/                    # static exports
├── scripts/
│   ├── admin.py                    # Streamlit admin pane (incl. PBI setup wizard)
│   ├── admin.bat                   # one-click launcher
│   ├── status.py                   # CLI version of the admin pane
│   ├── configure_bi_knime.py       # patches host/port/db in .pbix and .knwf at install
│   ├── deploy_knime.py             # extracts .knwf into KNIME workspace
│   ├── run_knime_predictions.py    # invokes knime.exe batch
│   ├── fast_silver_backfill.py     # drop-constraint backfill (first install)
│   └── cleanup_bronze.py           # daily retention pass
├── installer/
│   └── install_template.py         # consumed by web wizard, generates data-cycle-installer.py
├── security/                       # threat model, GDPR notes, ADR-005 deep-dive
├── tests/                          # pytest suites
├── docs/v2/                        # this folder — TECHNICAL, INSTALLATION, USER_GUIDE,
│                                   # OPERATIONS, DECISIONS, README
├── storage/                        # local data — gitignored, populated at runtime
├── requirements.txt
└── README.md
```

The website source (the install wizard frontend at the project's `/install`
page) lives in a separate repo (`DCP-Website`) and is no longer carried in
this repo's history.
