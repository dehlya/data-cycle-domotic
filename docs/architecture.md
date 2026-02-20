# Architecture

## Overview

End-to-end data pipeline for two smart apartments.

```
Sources          MySQL DB (pidb) · JSON sensors ×2 · Weather CSV (sFTP)
    ↓
Acquisition      recup.py — polls sensors every minute via local network
    ↓
Bronze           Raw immutable copies — file system, timestamped paths
    ↓
Silver           Cleaned, flattened, joined — DB tables
    ↓
Gold             OLAP/DWH — aggregated KPIs, fact tables
    ↓
ML               Energy & presence forecasting
    ↓
BI               Dashboards — energy, environment, presence
```

---

## Layer Descriptions

### Sources

| Source | Type | Description |
|--------|------|-------------|
| MySQL DB (`pidb`) | Relational DB | Historical sensor readings, device registry |
| JSON sensors ×2 | HTTP/JSON | Live readings from apartment 1 and apartment 2 |
| Weather CSV | sFTP | Hourly weather data — temperature, humidity, precipitation |

### Acquisition (`ingestion/`)

`recup.py` is a Python script that runs continuously (via cron, every minute).
It polls each apartment's JSON sensor endpoint, and stores the raw response
as a timestamped JSON file under `data/bronze/<apt_id>/YYYY/MM/DD/HHmmss.json`.

### Bronze (`etl/bronze/`)

Raw, immutable copies of all source data.  Files are never modified after
being written.  Timestamped paths allow replay and audit.

### Silver (`etl/silver/`)

Cleaned and normalised records stored in SQLite (`data/silver/silver.db`).
Key transformations:
- Flatten nested JSON sensor payloads
- Standardise timestamps to ISO-8601 UTC
- Cast numeric fields
- Join apartment readings (no weather join yet at this layer)

### Gold (`etl/gold/`)

Aggregated KPI tables for BI and ML consumption:

| Table | Grain | Key metrics |
|-------|-------|-------------|
| `fact_energy_hourly` | apt × hour | total_kwh, avg_kwh |
| `fact_presence_daily` | apt × day | presence_ratio |
| `fact_environment_daily` | apt × day | avg_temperature, avg_humidity, avg_co2_ppm |

### ML (`ml/`)

Forecasting models trained on Gold tables:
- **Energy forecast** — rolling-mean baseline per apartment
- **Presence forecast** — rolling-mean baseline per apartment

Trained models are persisted under `ml/models/` as pickle files.

### BI (`bi/`)

Dashboard definition files (JSON) consumed by a BI tool:
- `energy.json` — energy consumption panels
- `environment.json` — environmental quality panels
- `presence.json` — room presence panels

---

## Data Flow Diagram

```
Sensors (apt1, apt2)
    │  HTTP/JSON
    ▼
ingestion/recup.py  ──────────────── every 60 s (cron)
    │  writes raw JSON
    ▼
data/bronze/<apt>/<YYYY/MM/DD>/<HHmmss>.json
    │
    ▼
etl/bronze/ingest.py  (copy landing → bronze)
    │
    ▼
etl/silver/transform.py  (flatten + clean → silver.db)
    │
    ▼
etl/gold/aggregate.py  (KPIs → gold.db)
    │
    ├──► ml/forecast.py  (train & save models)
    │
    └──► bi/ dashboards  (energy, environment, presence)
```
