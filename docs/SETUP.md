# Setup Guide

How to get the UC2 Apartments Domotic project running from scratch.

---

## Prerequisites

- **Python 3.11+**
- **PostgreSQL** (local or on VM)
- **Access to school network** (for MySQL source DB and sensor endpoints)
- **TablePlus** or any SQL client (optional, for inspecting data)

---

## 1. Clone the repo

```bash
git clone https://github.com/<your-org>/data-cycle-domotic.git
cd data-cycle-domotic
```

## 2. Install dependencies

```bash
pip install -r requirements.txt
```

Key packages: `sqlalchemy`, `psycopg2-binary`, `pymysql`, `python-dotenv`, `paramiko`

## 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your actual credentials:

```dotenv
# Bronze storage (SMB mount point for sensor JSON files)
SMB_PATH=Z:\
BRONZE_ROOT=storage\bronze

# PostgreSQL — Silver & Gold layers
DB_URL=postgresql://domotic:Domotic_password@localhost:5432/domotic_dev

# MySQL — school source DB (read-only)
MYSQL_URL=mysql+pymysql://student:<password>@10.130.25.152:3306/pidb
```

> **Note:** Never commit `.env` — it's in `.gitignore`. The `.env.example` has the structure without real credentials.

## 4. Set up PostgreSQL

Create the database and schemas:

```sql
CREATE DATABASE domotic_dev;
\c domotic_dev
CREATE SCHEMA silver;
CREATE SCHEMA gold;
```

## 5. Mount Bronze storage

The sensor JSON files are accessible via SMB share. Mount it to a local drive (e.g. `Z:\`) and set `SMB_PATH` in `.env`. The Bronze folder structure is:

```
storage/bronze/
├── jimmy/
│   └── YYYY/MM/DD/HH/*.json
└── jeremie/
    └── YYYY/MM/DD/HH/*.json
```

## 6. Run the ETL pipeline

### Step 1 — Flatten sensor JSON → Silver

```bash
python etl/bronze_to_silver/flatten_sensors.py
```

This processes all JSON files from both apartments into `silver.sensor_events` (~15M rows, ~3.5 hours on first run). It's resume-capable via watermark — safe to interrupt and restart.

### Step 2 — Import MySQL static data → Silver

```bash
python etl/bronze_to_silver/import_mysql_to_silver.py
```

Imports dimension and reference tables from the school MySQL DB into the Silver schema. Takes a few seconds.

### Step 3 — Weather CSV → Silver (Sacha)

```bash
python etl/bronze_to_silver/clean_weather.py
```

> Not yet implemented — assigned to Sacha.

### Step 4 — Silver → Gold aggregation

> Sprint 3 — not yet implemented.

---

## Environments

| Environment | DB | Usage |
|---|---|---|
| `domotic_dev` | localhost:5432 | Local development and testing |
| `domotic_prod` | localhost:5432 | Production on the VM (separate `.env`) |

Switch between them by changing `DB_URL` in `.env`. The prod VM has its own `.env` with prod credentials.

---

## Useful queries

Check Silver data after ETL:

```sql
-- Row count
SELECT COUNT(*) FROM silver.sensor_events;

-- Breakdown by apartment and sensor type
SELECT apartment, sensor_type, COUNT(*)
FROM silver.sensor_events
GROUP BY apartment, sensor_type
ORDER BY apartment, sensor_type;

-- Check for timestamp fallback issues (should be 0)
SELECT COUNT(*) FROM silver.sensor_events
WHERE timestamp::date = CURRENT_DATE;

-- Outlier distribution
SELECT field, COUNT(*) FILTER (WHERE is_outlier) AS outliers, COUNT(*) AS total
FROM silver.sensor_events
GROUP BY field
ORDER BY outliers DESC;
```

---

## Troubleshooting

**`ModuleNotFoundError: No module named 'pymysql'`**
→ `pip install pymysql`

**`Unknown database 'appartments'`**
→ The MySQL database name is `pidb`, not `Appartments`. Check your `MYSQL_URL`.

**Flatten script is slow**
→ First run processes ~243k files (~3.5h). Subsequent runs only process new files thanks to the watermark table.

**Can't see tables in TablePlus**
→ You're probably looking at the `public` schema. Switch to `silver` schema.