# KNIME ML workflows

Two KNIME Analytics Platform workflows used to predict room-level occupancy
and energy consumption from gold facts. Full methodology is in
[`docs/v2/TECHNICAL.md`](../../docs/v2/TECHNICAL.md) (section "BI / ML layer").

## Workflows

| File | Use case | Grain | Selected model | Output table |
|---|---|---|---|---|
| `Motion_Prediction_Server.knwf` | Room occupancy probability | 15 min | logistic regression | `gold.fact_prediction_motion` |
| `Consumption_Weather_Prediction_Server.knwf` | Room energy consumption | 15 min | linear regression w/ weather features | `gold.fact_prediction_consumption` |

Each workflow:

- Reads from `gold.fact_*` and `gold.dim_*` via the **PostgreSQL Connector** node.
- Reuses the same preparation chain as its training counterpart so backtests
  and operational scoring stay consistent.
- Writes predictions back to its own gold fact table via **DB Writer**.

## Version pinning — important

The workflows are pinned to **KNIME 5.8** (`created_by="5.8.0.v202510151000"` in
the `.knwf` XML). KNIME refuses to load a workflow exported from a newer
version, so:

- **Don't open the .knwf in a newer KNIME** and re-export — it bumps the version
  string and breaks compatibility for everyone else.
- **If you have to**, repack the .knwf with the version string set back to 5.8.0.
- See [`scripts/run_knime_predictions.py`](../../scripts/run_knime_predictions.py)
  for the headless-run wrapper used by the watcher and the admin pane.

## Setup (one-time, per machine)

The installer does this automatically. Manual setup if you need it:

1. Install [KNIME Analytics Platform 5.8](https://www.knime.com/downloads).
2. **File → Import KNIME Workflow…** → select the `.knwf` file.
3. Open the imported workflow and configure the **PostgreSQL Connector** node:
   - **Authentication kind**: *Credentials*
   - **Credentials parameter name**: `db` (the workflow's `Variable to Credentials`
     node provides this — see "Credentials trick" below)
   - **Database name**: matches the database in your `.env` `DB_URL`
     (default `domotic`)
   - **Schema**: `gold`
4. Run the workflow once interactively to confirm. After that, batch-mode runs
   pick up credentials from CLI flow variables.

## Credentials trick (Variable → Credentials)

KNIME blocks flow-variable overrides on `xpassword` fields for security, so the
workflows use a small adapter chain to inject credentials at batch-run time:

```
String Configuration (db_user) ┐
String Configuration (db_pwd)  ├─►  Variable to Credentials  ──►  PG Connector ("db" credential)
```

The headless runner passes the values via:

```
knime.exe -workflow.variable=db_user,<USER>,String -workflow.variable=db_pwd,<PWD>,String
```

This way credentials live in `.env` (read by the Python wrapper at runtime) and
**never** end up baked into the `.knwf` on disk.

## Output tables

Both prediction tables share the same shape:

| Column | Type | Notes |
|---|---|---|
| `prediction_id` | bigint | Surrogate PK |
| `apartment_key` | int | FK → `gold.dim_apartment` |
| `room_key` | int | FK → `gold.dim_room` |
| `datetime_key` | bigint | FK → `gold.dim_datetime` (15-min grain) |
| `predicted_value` | numeric | `predicted_occupied` (motion, 0..1) or `predicted_power_w` (consumption) |
| `actual_value` | numeric | Observed value at the same key — for backtest comparison |
| `model_name` | text | e.g. `logistic_regression`, `linear_regression` |
| `target` | text | `Presence` or `Consumption` |
| `created_at` | timestamptz | When the row was written |

Power BI's Predictions dashboard joins these to the dim tables for the
"predicted vs actual" plots.

## Editing workflows

If you need to change a workflow:

1. Open it in **your VM's KNIME 5.8** (not a newer version on a dev laptop).
2. Make changes interactively, run end-to-end, validate.
3. **File → Export KNIME Workflow…** to overwrite the existing `.knwf`.
4. Commit + push.

Never hard-code DB credentials inside the workflow — always go through the
String Configuration → Variable to Credentials chain so headless runs keep
working.
