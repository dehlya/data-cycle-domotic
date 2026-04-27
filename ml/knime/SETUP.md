# KNIME workflow — one-time setup

After cloning the repo (or running the installer), do this **once per
workflow** so batch mode works automatically. Subsequent runs need no
manual steps.

## Why

KNIME explicitly forbids overwriting password fields via either flow
variables OR the `-credential` CLI flag when the target is a Credentials
Configuration node (the error is `Attempt to overwrite the password
with config key 'password' failed. It's not possible to overwrite
passwords with flow variables.`).

So the only path that works is: **bake the DB password into a
Credentials Configuration node** inside each workflow, saved with KNIME's
"weakly encrypted" cipher (which uses a fixed key, so the encrypted
value is portable across machines as long as everyone uses the same DB
password).

## Steps (5 min per workflow)

For both `Motion_Prediction_Server` and `Consumption_Weather_Prediction_Server`:

1. **Open the workflow in KNIME Analytics Platform**
   (KNIME Explorer → LOCAL → workflow name).

2. **Drop a Credentials Configuration node onto the root canvas**
   (Node Repository → search "Credentials Configuration" → drag onto
   canvas, OUTSIDE any component / metanode).

3. **Configure the node:**
   - **Parameter / Variable Name:** `db`
   - **Username:** your `.env` DB user (e.g. `domotic`)
   - **Password:** your `.env` DB password (the real one)
   - ✅ **Save password in configuration (weakly encrypted)** — REQUIRED
   - ☐ Prompt user name in component dialog (uncheck)
   - **Apply → OK**

4. **Right-click the node → Execute** → wait for the green status dot.

5. **Wire the red flow variable port** (top-right corner of the
   Credentials Configuration node) **to BOTH PostgreSQL Connectors'**
   red input ports (top-left of each).

6. **For each PostgreSQL Connector** (there are 2 per workflow:
   `(#1)` and `(#579)`):
   1. Double-click → **Connection Settings** tab
   2. Authentication section → **Credentials** radio
   3. Dropdown → pick **`db`**
   4. **Apply → OK**

7. **Ctrl+S** to save the workflow.
   The `*` next to the workflow name in the title bar should disappear.

8. **File → Export KNIME Workflow** → overwrite the file in
   `ml/knime/<workflow_name>.knwf`.

## Verify

After save, run from PowerShell:

```powershell
Get-ChildItem "$HOME\knime-workspace\<workflow>" -Directory | Where-Object { $_.Name -match "Credentials Configuration" }
Select-String -Path "$HOME\knime-workspace\<workflow>\PostgreSQL Connector*\settings.xml" -Pattern 'key="credentials"'
```

Expected:
- First command lists `Credentials Configuration (#XXX)`
- Both PG Connectors show `value="db"` (NOT `isnull="true"`)

## Test

```bash
python scripts/run_knime_predictions.py
```

Should report:
```
Motion_Prediction_Server completed in 30s
Consumption_Weather_Prediction_Server completed in 40s
Total: 2 ok 0 failed
```

And the DB should have new rows:
```sql
SELECT model_name, target, COUNT(*) FROM gold.fact_prediction_motion       GROUP BY 1,2;
SELECT model_name, target, COUNT(*) FROM gold.fact_prediction_consumption  GROUP BY 1,2;
```

## What if my DB password changes?

The encrypted password lives inside the `.knwf`. If `.env` `DB_URL`
changes, you must:

1. Open both workflows in KNIME GUI
2. Edit the Credentials Configuration node → update the password →
   Apply → OK → Ctrl+S → re-export `.knwf`
3. Commit the new `.knwf` files

Yes, this is mildly annoying. KNIME doesn't expose a CLI mechanism to
update passwords because of the security rule above.

## Common gotchas

| Symptom | Cause | Fix |
|---|---|---|
| `Attempt to overwrite the password with config key 'password' failed` | Old runner code is still passing `-credential=db;...` | Pull latest `scripts/run_knime_predictions.py` |
| `credentials="" isnull="true"` in PG Connector settings.xml | You picked `db` from dropdown but didn't Apply+OK before saving, OR Ctrl+S didn't fire | Re-open dialog, Apply → OK, then Ctrl+S, watch the title-bar `*` disappear |
| Credentials Configuration folder doesn't appear in workspace | Node was added but workflow not saved | Click in canvas, Ctrl+S |
| `Java exit code=4` immediately after launch | KNIME GUI is open with the same workspace | Close KNIME GUI before running batch (`Stop-Process -Name knime -Force`) |
| `Please enter a valid password` warnings | Password field was left empty in the Credentials Configuration node | Re-open node config, fill password, ✅ "Save password (weakly encrypted)", Apply |
| JVM crash in `libcef.dll` | Stale workspace state from interrupted GUI session | Close KNIME GUI, delete `~/knime-workspace/<workflow>/`, re-deploy with `python scripts/deploy_knime.py` |

## For future-Sacha (keeping this clean)

If you change the workflows further, please:
1. Keep using the Credentials Configuration node with parameter name `db`
2. Keep "Save password in configuration (weakly encrypted)" checked
3. Re-export the `.knwf` so the repo always has the latest credential-aware
   version
4. Test with `python scripts/run_knime_predictions.py` before pushing
