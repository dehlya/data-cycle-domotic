# KNIME workflow setup

The KNIME workflows ship pre-wired with a **Variable to Credentials**
architecture that lets the runner inject DB credentials from `.env` at
runtime without ever touching a password field directly. After install,
no GUI steps are needed.

## How it works

```
.env DB_URL  ──>  scripts/run_knime_predictions.py
                            │
                            │  -workflow.variable=db_user,<user>,String
                            │  -workflow.variable=db_pwd,<password>,String
                            ▼
   ┌──────────────────────────────────────────────┐
   │   String Configuration  "db_user"            │
   │   String Configuration  "db_pwd"             │
   │             ↓ (flow variables)               │
   │   Variable to Credentials  →  credential 'db'│
   │             ↓ (flow variable port)           │
   │   PostgreSQL Connector(s)  use 'db'          │
   └──────────────────────────────────────────────┘
```

KNIME blocks flow-variable overrides on `xpassword` fields for security.
Plain `String` flow variables are unrestricted, and the **Variable to
Credentials** node converts the two strings into a real credential object
*before* it reaches any password-typed field — so KNIME's rule is never
violated.

## What's already done in the shipped `.knwf` files

For both `Motion_Prediction_Server.knwf` and
`Consumption_Weather_Prediction_Server.knwf`:

1. Two **String Configuration** nodes at root canvas:
   - Parameter Name `db_user`, default `domotic`
   - Parameter Name `db_pwd`, default `dummy_default` (overridden at runtime)
2. One **Variable to Credentials** node, bound to `db_user` (username) and
   `db_pwd` (password), output credential name `db`.
3. Each PostgreSQL Connector has Authentication → Credentials → `db`,
   wired via the flow variable port from Variable to Credentials.
4. The `Flow Variables` tab on every PG Connector has `username`,
   `password`, `selectedType`, `credentials` all UNBOUND. (Bindings on
   that tab would trigger the password-overwrite restriction.)

## Running

```bash
python scripts/run_knime_predictions.py            # both workflows
python scripts/run_knime_predictions.py motion     # just motion
python scripts/run_knime_predictions.py consumption# just consumption
```

The runner reads `DB_URL` from `.env`, extracts the user + password,
passes them as `-workflow.variable=...,String`, and KNIME does the rest.

## What if I edit a workflow?

If you modify a `.knwf` in KNIME GUI and re-export it, **DO NOT delete**
the String Configuration / Variable to Credentials wiring or the runner
won't be able to inject credentials. Specifically keep:

- The two String Configuration nodes named `db_user` and `db_pwd`
  (parameter names must match those exact strings — that's what
  `-workflow.variable` targets).
- The Variable to Credentials node with output name `db`.
- The flow variable port wires from those nodes to every PostgreSQL
  Connector.
- The PG Connector Authentication mode set to **Credentials** with
  dropdown value `db`.
- The PG Connector **Flow Variables tab** completely empty (no bindings).

Verify after edit by running the runner against the deployed workspace.
If you see "Attempt to overwrite the password" → there's a password
flow-var binding somewhere that needs clearing.

## Common gotchas

| Symptom | Cause | Fix |
|---|---|---|
| `Attempt to overwrite the password with config key 'password' failed` | A PG Connector's Flow Variables tab has `password` bound to a flow variable | Open PG Connector → Flow Variables tab → clear `password` (and `username` and `credentials`) bindings → Apply → OK |
| `No credentials stored to name "C:\Users\...\knime-workspace"` | A PG Connector's `credentials` slot was bound to the built-in `knime.workspace` flow variable | Same fix as above — clear the binding on the Flow Variables tab |
| `The workflow variables are potentially unused: "db_user" "db_pwd"` | The workflow doesn't have String Configuration nodes with those exact parameter names | Re-add the String Configuration nodes with parameter names `db_user` and `db_pwd` |
| `Java exit code=4` immediately after launch | KNIME GUI is open with the same workspace | Close KNIME GUI before running batch (`Stop-Process -Name knime -Force`) |
| `Input table is empty` (and only that) | Source gold tables are empty / filter excluded all rows | Run the gold ETL first: `python -m etl.silver_to_gold.populate_gold` |
