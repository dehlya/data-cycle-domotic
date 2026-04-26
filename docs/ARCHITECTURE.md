# Architecture & Data Flow

Technical overview of the UC2 Apartments Domotic data pipeline.

---

## High-level architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  SOURCES                                                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │ JSON Sensors │  │ MySQL (pidb) │  │ Weather CSV  │           │
│  │ Jimmy/Jérémie│  │ 10.130.25.152│  │ sFTP Meteo2  │           │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘           │ 
└─────────┼──────────────────┼──────────────────┼─────────────────┘
          │                  │                  │
          ▼                  │                  ▼
┌─────────────────────┐      │      ┌─────────────────────┐
│  BRONZE             │      │      │  BRONZE             │
│  Raw JSON files     │      │      │  Raw weather CSV    │
│  /bronze/{apt}/     │      │      │  /bronze/weather/   │
│  YYYY/MM/DD/HH/     │      │      │  YYYY/MM/DD/        │
└─────────┬───────────┘      │      └─────────┬───────────┘
          │                  │                  │
          │flatten_sensors   │ import_mysql     │  clean_weather
          │                  │                  │  (Sacha)
          ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER — PostgreSQL (domotic_dev / domotic_prod)               │
│                                                                 │
│  sensor_events (15M rows)    dim_buildings    weather_forecasts │
│  etl_watermark               dim_rooms        (TBD)             │
│                              dim_sensors                        │
│                              dim_devices                        │
│                              dim_building_types                 │
│                              ref_energy_profiles                │
│                              ref_power_snapshots                │
│                              ref_parameters                     │
│                              ref_parameters_type                │
│                              log_sensor_errors                  │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              │  Silver -> Gold ETL (Sprint 3)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD — PostgreSQL                                              │
│                                                                 │
│  fact_energy          hourly kWh per room/apartment             │
│  fact_occupation      hourly presence per room                  │
│  fact_environment     hourly temp/humidity/CO2                  │
│  fact_sensor_health   error counts, uptime %                    │
│  dim_* (typed)        dimensions with proper data types         │
└────────────┬──────────────────────────┬─────────────────────────┘
             │                          │
             ▼                          ▼
┌────────────────────┐      ┌────────────────────────┐
│  ML (Sprint 5)     │      │  BI (Sprint 4)         │
│  Reads: Silver     │      │  Reads: Gold           │
│  Writes: Gold      │      │                        │
│                    │      │  Power BI              │
│  Energy forecast   │      │  ├─ Energy dashboard   │
│  Presence predict. │      │  └─ Environment dash.  │
│  Anomaly detection │      │                        │
│                    │      │  SAP SAC               │
│  Tools: Python,    │      │  └─ Room presence dash.│
│         KNIME      │      │                        │
└─────────┬──────────┘      └────────────────────────┘
          │
          │ predictions -> gold.fact_ml_predictions
          ▼
       BI reads enriched Gold
```

---

## Medallion architecture

The project follows the **medallion (Bronze/Silver/Gold)** pattern:

### Bronze — Raw storage

Immutable copies of all source data, stored exactly as received. Purpose: auditability, reprocessing, never lose data.

- JSON sensor files in timestamped folder structure
- Weather CSVs in daily folders
- MySQL snapshots (via import script)

Storage: File system (Windows NTFS on VM)

### Silver — Clean data

Cleaned, flattened, deduplicated, timestamp-aligned data at **full resolution**. This is where data scientists and ML models read from.

- Every individual sensor reading preserved (no aggregation)
- Outliers flagged but not removed
- Room names normalized
- All sources in one queryable PostgreSQL schema

Storage: PostgreSQL `silver` schema

### Gold — Business-ready

Aggregated, typed, modeled data optimized for BI tools and dashboards. This is what end users see.

- Hourly aggregations for energy, occupation, environment
- Proper data types (not TEXT)
- Star schema with fact and dimension tables
- ML predictions loaded as additional fact tables

Storage: PostgreSQL `gold` schema

---

## Data flow for ML

ML models read from **Silver** (not Gold) because Silver has full-resolution data that Gold aggregates away:

- Silver `sensor_events` has minute-by-minute readings -> ML can detect patterns, spikes, cycles
- Gold `fact_energy` has hourly kWh -> too coarse for a forecasting model

The flow:
1. ML reads Silver for training data
2. ML trains models (energy forecast, presence prediction)
3. ML writes predictions back to Gold as `fact_ml_predictions`
4. BI dashboards read Gold (both historical facts + ML predictions)

---

## Environments

| Environment | Database | Host | Usage |
|---|---|---|---|
| Dev | domotic_dev | localhost:5432 | Local development |
| Prod | domotic_prod | localhost:5432 (VM) | Production pipeline |

Each environment has its own `.env` file. The VM runs the prod environment with a separate user account.

---

## Stack decisions

| Decision | Choice | Rationale |
|---|---|---|
| ADR-001 | Python + asyncio for ingestion | I/O-bound task, async allows concurrent sensor polling within 60s |
| ADR-002 | PostgreSQL for Silver and Gold | Multi-user, Power BI connector, proper SQL for OLAP |
| ADR-003 | Custom watcher over Airflow | Single-VM deploy, zero infrastructure, trivially restartable |
| ADR-004 | File system for Bronze | Immutable, no overhead, easy to inspect |
| ADR-005 | Self-contained installer | One-command deploy, credentials never leave deployer's machine |
| ADR-006 | Two PostgreSQL roles (admin + app) | Least-privilege at runtime, admin secret never persisted |
| ADR-007 | Mask PII in gold dim_apartment | GDPR Art. 4(1): keep first-name pseudonym, mask the rest |
| ADR-008 | Watcher revisited at deploy time | Right size for < 10 apartments on a single VM |