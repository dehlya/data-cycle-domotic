# KNIME ML Workflows

Machine learning models for the Data Cycle project (UC2).

## Workflows

| File | Issue | Description |
|------|-------|-------------|
| `energy_prediction.knwf` | #27 | Predict energy consumption per apartment (considers weather + historical patterns) |
| `presence_prediction.knwf` | #26 | Predict room occupancy from motion/door sensor patterns |

## Setup

1. Install [KNIME Analytics Platform](https://www.knime.com/downloads)
2. File → Import KNIME Workflow → select the `.knwf` file
3. Configure the **PostgreSQL Connector** node:
   - Host: your PostgreSQL host
   - Port: 5432
   - Database: `domotic_dev` (or `domotic_prod`)
   - Schema: `gold`
   - User/password: from your `.env`
4. Run the workflow

## Data Source

Both workflows read from the Gold star schema:

- **Energy prediction**: `gold.fact_energy_minute`, `gold.mv_energy_with_cost`, `gold.fact_weather_day`
- **Presence prediction**: `gold.fact_presence_minute`, `gold.fact_environment_minute`
- **Dimensions**: `gold.dim_datetime`, `gold.dim_date`, `gold.dim_apartment`, `gold.dim_room`

## Output

Predictions are written back to `gold.fact_prediction` (see `create_gold.py` — currently commented out, uncomment when ready).

## Important

- Use **workflow variables** for DB connection — do NOT hardcode credentials in the workflow
- Export as `.knwf` (File → Export KNIME Workflow) before committing
- Test with `domotic_dev` before running on `domotic_prod`
