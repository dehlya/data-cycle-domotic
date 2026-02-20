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

## Repo structure

```
ingestion/      recup.py and cron scripts — sensor data acquisition
etl/            Bronze → Silver → Gold transformation scripts
ml/             Notebooks and workflows
bi/             Dashboard files and exports
docs/           Architecture diagrams and technical documentation
```