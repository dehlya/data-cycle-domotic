# KNIME ML workflows

Machine learning workflows for the Data Cycle project. See full methodology in
the [Technical Documentation, section 09](../../website/src/app/technical/page.tsx)
or on the deployed website at `/technical#s09`.

## Workflows

| File | Use case | Selected model |
|---|---|---|
| `Motion_Prediction_Server.knwf` | Room occupancy (15-min grain) | logistic_regression |
| `Consumption_Weather_Prediction_Server.knwf` | Room energy consumption (15-min grain) | linear_regression |

Each workflow:
- Reads from `gold.fact_*` + `gold.dim_*` via the **PostgreSQL Connector** node
- Reuses the same preparation chain as its benchmark counterpart (so training
  and operational scoring stay consistent)
- Writes predictions back to `gold.fact_prediction` via **DB Writer**

## Setup

1. Install [KNIME Analytics Platform](https://www.knime.com/downloads)
2. **File → Import KNIME Workflow** → select the `.knwf` file
3. Configure the **PostgreSQL Connector** node:
   - Host: your Postgres host (e.g. `localhost`)
   - Port: `5432`
   - Database: matches `PG_DATABASE` from your `.env`
   - User / password: app user from your `.env` (`PG_USER` / `PG_PASSWORD`)
   - Schema: `gold`
4. Run the workflow

## Output table

`gold.fact_prediction` (one row per room × 15-min window):

| column | description |
|---|---|
| predicted_occupied / predicted_power_w | model output |
| actual_occupied / actual_power_w | observed value (for backtesting) |
| apartment, room, timestamp_rounded | identifying dimensions |
| model_name | constant per workflow |
| target | `Presence` or `Consumption` |

## Important

- Use **workflow variables** for DB credentials — never hard-code in the workflow
- Re-export with **File → Export KNIME Workflow** before committing changes
- Test against `domotic_dev` before running on production data
