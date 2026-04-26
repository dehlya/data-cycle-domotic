# KNIME workflow — one-time setup

After cloning the repo (or running the installer), do this **once per
workflow** so batch mode works automatically. Subsequent deployments and
runs need no manual steps.

## Why

The `.knwf` files were originally exported with the PostgreSQL Connector
password encrypted with Sacha's KNIME master key. After deployment we
need any user's password to work, so we rewire the connectors to use a
**Workflow Credential** that batch mode can fill from the command line
via `-credential=db;<user>;<password>`.

> **Important — why not flow variables?** KNIME deliberately forbids
> overwriting password fields via flow variables (`It's not possible to
> overwrite passwords with flow variables`). The only batch-mode-safe
> mechanism for passwords is `-credential` targeting a Workflow
> Credential. Don't waste time on `-workflow.variable=db_pwd,...`.

## Steps (5 min per workflow)

For both `Motion_Prediction_Server.knwf` and
`Consumption_Weather_Prediction_Server.knwf`:

1. **Open the workflow in KNIME Analytics Platform.**

2. **Add a Workflow Credential named `db`**
   - In the **Explorer panel** (left sidebar, NOT the canvas), right-click
     the workflow → **Workflow Credentials...**
   - Click **Add**
     - **Name:** `db`
     - **Login:** leave empty (filled at runtime)
     - **Password:** leave empty (filled at runtime)
   - OK

3. **Bind every PostgreSQL Connector to that credential**

   For **each** PostgreSQL Connector node in the workflow (use
   `scripts/status.py` or expand all components to find them):

   1. Double-click the node → **Authentication** tab
   2. Switch to **"Use credentials"** mode (NOT "Username & password")
   3. From the dropdown, select **`db`**
   4. OK

   The Connection Settings tab still has host / port / database hardcoded —
   leave those as-is (they're patched per-deployer by `configure_bi_knime.py`).

4. **Remove any leftover credential setup** from earlier attempts:
   - Delete any **Credentials Configuration** node that's no longer wired
   - Delete any **String Configuration** node named `db_user` / `db_pwd`
   - Right-click workflow → **Workflow Variables...** → remove `db_user`
     and `db_pwd` if they exist
   - The PG Connectors should ONLY use the `db` Workflow Credential now

5. **Save** the workflow (Ctrl+S)

6. **File → Export KNIME Workflow** → save as `.knwf`, **overwrite** the
   file in `ml/knime/`

7. **Test:**
   ```bash
   python scripts/run_knime_predictions.py
   ```

   The runner injects:
   ```
   -credential=db;<your-user>;<your-password>
   ```
   and KNIME's batch executor fills the `db` Workflow Credential before
   any PG Connector executes.

## Verify

`scripts/run_knime_predictions.py` should report:
```
Motion_Prediction_Server completed in 30s
Consumption_Weather_Prediction_Server completed in 40s
Total: 2 ok 0 failed
```

And your DB should have new rows:
```sql
SELECT model_name, target, COUNT(*) FROM gold.fact_prediction_motion       GROUP BY 1,2;
SELECT model_name, target, COUNT(*) FROM gold.fact_prediction_consumption  GROUP BY 1,2;
```

## Common gotchas

| Symptom | Cause | Fix |
|---|---|---|
| `Attempt to overwrite the password with config key 'password' failed` | A PG Connector is still in "Username & password" mode with the password bound to a flow variable | Switch the node's Authentication tab to "Use credentials" → `db` |
| `Database host is not defined` (×N warnings) | Some PG Connector still has empty host or upstream connector failed | Check ALL connectors (use `scripts/status.py` or grep `host` field in settings.xml) |
| `Java exit code=4` immediately after launch | KNIME GUI is open with the same workspace | Close KNIME GUI before running batch |
| `Required credentials are missing` | The Workflow Credential `db` doesn't exist on the workflow | Re-do step 2 (right-click workflow → Workflow Credentials → Add `db`) |
| Workflow runs but writes 0 rows | Upstream filters too restrictive (e.g. no rows in time range) | Open in KNIME, run interactively to see where it stops |

## For future-Sacha (keeping this clean)

If you change the workflows further, please:
1. Keep using the Workflow Credential name `db` (the runner script
   passes `-credential=db;...`)
2. Keep every PG Connector in "Use credentials" mode bound to `db` —
   never re-introduce inline passwords or flow-variable password binding
3. Re-export the .knwf so the repo always has the latest credential-aware
   version
4. Test with `python scripts/run_knime_predictions.py` before pushing
