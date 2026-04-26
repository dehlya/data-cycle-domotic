# KNIME workflow — one-time setup

After cloning the repo (or running the installer), do this **once per
workflow** so batch mode works automatically. Subsequent deployments and
runs need no manual steps.

## Why

The `.knwf` files have the PostgreSQL Connector node with Sacha's password
encrypted into them. After deployment we need any user's password to work,
so we replace the inline password with a **Workflow Variable** that batch
mode can inject from the command line.

## Steps (5 min per workflow)

For both `Motion_Prediction_Server.knwf` and
`Consumption_Weather_Prediction_Server.knwf`:

1. **Open the workflow in KNIME Analytics Platform.**

2. **Add two Workflow Variables**
   - In the **Explorer panel** (left sidebar, not the canvas), right-click
     the workflow → **Workflow Variables...**
   - Click **Add** → name `db_user`, type **String**, value (leave empty)
   - Click **Add** → name `db_pwd`, type **String**, value (leave empty)
   - OK
   - *(If "Workflow Variables" is missing in your KNIME version, instead use
     a **Configuration Node** with two String Input nodes — same idea.)*

3. **Bind the PostgreSQL Connector fields to those variables**

   For **each** PostgreSQL Connector node in the workflow (use the diagnostic
   in `scripts/status.py` or just expand all components to find them):

   1. Double-click the node → **Authentication** tab
   2. Switch to **"Username & password"** mode (NOT "Use credentials")
   3. **Click the small "v" icon** at the right of the Username field →
      from the dropdown, pick **"db_user"**
   4. Same for the Password field → bind to **"db_pwd"**
   5. OK

   The Connection Settings tab still has host / port / database hardcoded —
   leave those as-is (they're patched per-deployer by `configure_bi_knime.py`).

4. **Remove any leftover credential setup** from earlier attempts:
   - Delete any **Credentials Configuration** node that's no longer wired
   - Right-click workflow → **Workflow Credentials** → remove any `db` entry
   - The PG Connector should ONLY rely on the two Workflow Variables now

5. **Save** the workflow (Ctrl+S)

6. **File → Export KNIME Workflow** → save as `.knwf`, **overwrite** the
   file in `ml/knime/`

7. **Test:**
   ```bash
   python scripts/run_knime_predictions.py
   ```

   The runner injects:
   ```
   -workflow.variable=db_user,<your-user>,String
   -workflow.variable=db_pwd,<your-password>,String
   ```
   and KNIME's batch executor passes those values to your bound fields.

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
| `Database host is not defined` (×N warnings) | Some PG Connector still has empty host or empty bound variable | Check ALL connectors (use `scripts/status.py` or grep `host` field in settings.xml) |
| `Java exit code=4` immediately after launch | KNIME GUI is open with the same workspace | Close KNIME GUI before running batch |
| `Username: 'X', Password: not provided` | Variable binding didn't take | Re-open Authentication tab, re-bind Username/Password to `db_user`/`db_pwd` |
| Workflow runs but writes 0 rows | Upstream filters too restrictive (e.g. no rows in time range) | Open in KNIME, run interactively to see where it stops |

## For future-Sacha (keeping this clean)

If you change the workflows further, please:
1. Keep using `db_user` and `db_pwd` as the variable names (the runner
   script depends on them)
2. Re-export the .knwf so the repo always has the latest credential-aware
   version
3. Test with `python scripts/run_knime_predictions.py` before pushing
