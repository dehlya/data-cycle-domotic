# KNIME workflow — one-time setup

After cloning the repo, do this **once per workflow** so batch mode works
automatically. Subsequent deployments and runs need no manual steps.

## Why

The `.knwf` files have the PostgreSQL Connector node with Sacha's password
encrypted into them. After deployment we need any user's password to work,
so we replace the inline password with a credential variable that batch
mode can inject from the command line.

## Steps (5 min per workflow)

1. **Open** `Motion_Prediction_Server.knwf` in KNIME Analytics Platform
2. From the Node Repository, drag in a **Credentials Configuration** node
   (search for "Credentials Configuration")
3. Place it at the very start of the workflow (before the PostgreSQL
   Connectors)
4. **Configure** the new node:
   - **Parameter/flow variable name:** `db`
   - **Username:** `domotic`
   - **Password:** leave empty (will be supplied at run time)
   - **Prompt user at runtime:** unchecked (we want batch-mode automation)
5. **Connect** the Credentials Configuration node's output (top-right port)
   to the **input port** of each `PostgreSQL Connector` node
   (most workflows have two — the data fetch one + the prediction write one)
6. **Open each `PostgreSQL Connector`** node → **Authentication** tab:
   - Toggle from "Username/Password" to **"Use credentials"**
   - Select credential: **`db`**
   - OK
7. **Save** the workflow (Ctrl+S)
8. **File → Export KNIME Workflow** → save as `Motion_Prediction_Server.knwf`,
   replacing the file in `ml/knime/`
9. Repeat steps 1–8 for `Consumption_Weather_Prediction_Server.knwf`
10. Commit + push the updated .knwf files

## Verify

After this, `scripts/run_knime_predictions.py` runs the workflows headlessly:

```bash
python scripts/run_knime_predictions.py
```

It reads the password from `.env` (`DB_URL` → `PG_PASSWORD`) and passes it
via KNIME's `-credential=db;<user>;<password>` CLI flag. The Credentials
Configuration node receives the values, propagates them to each PostgreSQL
Connector, and the workflow runs to completion.

## Common gotchas

- **Forgot to connect the Credentials Configuration node**: PostgreSQL
  Connector falls back to its own (empty) password, batch run fails.
- **Used a different credential name**: must match `db` exactly (case
  sensitive). If you prefer another name, also update `WORKFLOWS` /
  the `-credential=...` flag in `scripts/run_knime_predictions.py`.
- **"Use credentials" greyed out**: the PostgreSQL Connector needs the
  Credentials port enabled. Right-click the node → "Show Flow Variable
  Ports" if needed.
- **Workflow has additional nodes that hardcode credentials**: scan for
  any node with `xpassword` in its `settings.xml` and route it through
  the Credentials Configuration node too.

## Note for future-Sacha

If you change the workflows further, please rebuild and re-export so the
.knwf in this repo always reflects the latest credential-aware version.
