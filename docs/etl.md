# ETL Pipeline Documentation

Technical documentation for all ETL scripts in the project.

---

## Overview

```
Sources                          Silver                          Gold
───────                          ──────                          ────
JSON sensors (Bronze)  ──→  sensor_events (15M rows)  ──→  fact_energy
MySQL DB (pidb)        ──→  dim_buildings, dim_rooms…  ──→  fact_occupation
Weather CSV (Bronze)   ──→  (Sacha — TBD)              ──→  fact_environment
```

All ETL scripts live in `etl/` and use SQLAlchemy + the `DB_URL` from `.env`.

---

## etl/bronze_to_silver/flatten_sensors.py

**What it does:** Reads raw JSON sensor files from Bronze, flattens the nested structure into individual sensor readings, and upserts them into `silver.sensor_events`.

**Source:** `storage/bronze/{jimmy,jeremie}/YYYY/MM/DD/HH/*.json`
**Target:** `silver.sensor_events`

### How it works

1. Loads a watermark table (`silver.etl_watermark`) to know which files have already been processed
2. Scans both apartment folders for all `.json` files
3. Splits the unprocessed files into batches of 2,000
4. Processes batches in parallel (8 workers) — each batch:
   - Reads the JSON file
   - Extracts the timestamp from the `datetime` field
   - Calls `flatten()` to produce one row per sensor reading
   - Returns all rows + list of processed filenames
5. Upserts rows into `silver.sensor_events` (ON CONFLICT updates existing rows)
6. Marks filenames as done in the watermark table

### Sensor types handled

| JSON key | Sensor type | Fields extracted |
|---|---|---|
| `plugs` | plug | power (W), total (Wh), temperature (°C) |
| `doorsWindows` | door/window | open (bool), battery (%) |
| `motions` | motion | motion (bool), light (lux), temperature (°C) |
| `meteos` | meteo | temperature_c, co2_ppm, humidity_pct, noise_db, pressure_hpa, battery |
| `humidities` | humidity | temperature (°C), humidity (%), battery (%) |
| `consumptions` | consumption | total_power, power1-3 (W), current1-3 (A), voltage1-3 (V) |

### Target table schema

```sql
silver.sensor_events (
    apartment    TEXT,        -- 'jimmy' or 'jeremie'
    room         TEXT,        -- normalized room name (e.g. 'Bathroom', 'Kitchen')
    sensor_type  TEXT,        -- 'plug', 'motion', 'meteo', etc.
    field        TEXT,        -- 'power', 'temperature_c', 'open', etc.
    value        FLOAT,
    unit         TEXT,        -- 'W', '°C', 'bool', 'ppm', etc.
    timestamp    TIMESTAMPTZ,
    is_outlier   BOOLEAN,     -- flagged but NOT removed
    UNIQUE (apartment, room, sensor_type, field, timestamp)
)
```

### Outlier detection

Values are flagged (not removed) based on physical bounds:

| Field | Valid range |
|---|---|
| temperature_c | -20 to 60 |
| humidity_pct | 0 to 100 |
| co2_ppm | 300 to 5,000 |
| noise_db | 0 to 140 |
| pressure_hpa | 870 to 1,085 |
| power | 0 to 10,000 |
| battery | 0 to 100 |

### Room name normalization

| Raw JSON | Silver |
|---|---|
| Bhroom | Bathroom |
| Bdroom | Bedroom |
| Livingroom | Living Room |
| Office | Office |
| Kitchen | Kitchen |
| Laundry | Laundry |
| Outdoor | Outdoor |
| House | House |

### Resume capability

The watermark system (`silver.etl_watermark`) tracks processed filenames. If the script is interrupted, rerunning it will skip already-processed files and continue from where it left off.

### Performance

- ~243,000 files across both apartments
- ~15M rows produced
- ~3.5 hours on first run (8 parallel workers, batches of 2,000)
- Subsequent runs: only new files are processed

### Known issues

- **Timestamp fallback:** If a JSON file has an unparseable `datetime` field, the script falls back to `datetime.now()`. This produces incorrect timestamps. Run `SELECT COUNT(*) FROM silver.sensor_events WHERE timestamp::date = CURRENT_DATE` to check for affected rows.
- **Bare except in process_batch:** Errors are counted but not logged. Consider adding `logging.exception()` for prod.
- **Hardcoded WORKERS=8:** Should match the VM's CPU cores.

---

## etl/bronze_to_silver/import_mysql_to_silver.py

**What it does:** Reads static reference tables from the school's MySQL database and imports them into the Silver schema as dimension and reference tables.

**Source:** MySQL `pidb` at `10.130.25.152:3306`
**Target:** `silver.dim_*`, `silver.ref_*`, `silver.log_*`

### Tables imported

| MySQL source | Silver target | Rows | Description |
|---|---|---|---|
| buildings | dim_buildings | 2 | Apartment metadata, location, building year |
| buildingtype | dim_building_types | 2 | Maison / Appartement lookup |
| rooms | dim_rooms | 11 | Room details, sensor counts, orientation, m² |
| sensors | dim_sensors | 16 | Sensor IPs mapped to rooms |
| devices | dim_devices | 9 | Appliances per room |
| profilereference | ref_energy_profiles | 4 | Reference energy consumption kWh/yr by type |
| profile | ref_power_snapshots | ~514 | Power consumption snapshots over time |
| parameters | ref_parameters | 18 | Threshold configs per building |
| parameterstype | ref_parameters_type | 9 | Parameter type lookup |
| dierrors | log_sensor_errors | varies | Sensor error logs |

### Tables intentionally skipped

| Table | Reason |
|---|---|
| users | GDPR — contains names, emails, passwords, phone numbers |
| actions | Gamification feature, not relevant for analytics |
| achievements | Gamification |
| badges | Gamification |
| events | App-generated alerts, not raw sensor data |
| eventsgeneric | Energy saving tips |
| eventsignore | App config |
| categories | Only useful with events table |
| userrelationships | App config |

### How it works

1. Connects to both MySQL (source) and PostgreSQL (target)
2. For each table in the import list:
   - Reads all rows from MySQL
   - Drops and recreates the target table in Silver (all TEXT columns)
   - Inserts all rows
3. Idempotent — safe to rerun anytime

### Notes

- All columns are imported as TEXT for safety. Type casting happens in Gold.
- The script is idempotent (DROP + CREATE on each run), so rerunning it refreshes the data from MySQL.
- Runs in seconds since these are small reference tables.

---

## etl/bronze_to_silver/clean_weather.py

> **Status:** Not yet implemented. Assigned to Sacha.
>
> Will handle: Weather CSV from sFTP → `silver.weather_forecasts`

---

## etl/silver_to_gold/

> **Status:** Sprint 3 — not yet implemented.
>
> Planned fact tables:
> - `gold.fact_energy` — hourly kWh per room/apartment
> - `gold.fact_occupation` — hourly presence per room
> - `gold.fact_environment` — hourly temp/humidity/CO2 per room
> - `gold.fact_sensor_reliability` — error counts, uptime %