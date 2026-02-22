# Data Cycle Project
**Apartment Domotics & IoT Sensors**

---

## What this is

End-to-end data pipeline collecting and processing sensor data from two smart apartments — energy consumption, room presence, environmental quality — through to predictive analytics and BI dashboards.

---

## Architecture

```
Sources          MySQL DB (pidb) · JSON sensors ×2 · Weather CSV (sFTP)
    ↓
Acquisition      recup.py — async, polls sensors every minute
    ↓
Bronze           Raw immutable copies — file system, timestamped paths
    ↓
Silver           Cleaned, flattened, joined — PostgreSQL tables
    ↓
Gold             OLAP/DWH — aggregated KPIs, fact tables
    ↓
ML               Energy & presence forecasting — Python · KNIME
    ↓
BI               Power BI (energy, environment) · SAP SAC (presence)
```

---

## Stack

| Layer | Tool |
|---|---|
| Ingestion | Python 3.11 + asyncio |
| Orchestration | Apache Airflow |
| Bronze | File system |
| Silver / Gold | PostgreSQL |
| ETL | pandas + SQLAlchemy |
| ML | scikit-learn + statsmodels + KNIME |
| BI | Power BI · SAP Analytics Cloud |

---

## Repo structure

```
ingestion/      recup.py (async sensor polling) · weather_download.py (sFTP)
etl/            Bronze → Silver → Gold transformation scripts
ml/             models/ (Python) · knime/ (KNIME workflow exports)
bi/             powerbi/ (.pbix files) · sac/ (SAC exports)
storage/        local data — gitignored
security/       credential management guidelines
tests/          pytest test suites
docs/           architecture diagrams · technical decisions (ADRs)
.github/        workflows — notion-sync, future CI
```

---

## Setup

```bash
cp .env.example .env   # fill in credentials
pip install -r requirements.txt
```

See `security/README.md` for credential management guidelines.
See `docs/decisions.md` for architecture decision records.