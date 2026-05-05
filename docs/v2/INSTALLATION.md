# Installation Guide

The complete walkthrough for deploying DataCycle on a fresh Windows machine.
Every step the installer takes is documented below with the exact terminal
output you should see, so you know whether it's working.

**Time budget:** about **4 hours on a first install** (the silver backfill is
the long pole — years of historical sensor data land in one go) or **~15
minutes on a re-install** (watermarks let every step short-circuit on
already-done work).

> Treat the long first install like a build pipeline run, not a wizard.
> Start it, walk away, come back to a finished system. There are no manual
> steps in the middle — the installer handles all 10 phases automatically
> and only stops at the very end for optional yes/no prompts.

## What gets deployed

For context, here is the end-to-end pipeline the installer is about to set up:

![End-to-end pipeline diagram for UC2 — sources → bronze → silver → gold → BI/ML](../../_personal/diagrams/manual/architecture.png)

---

## Hardware & software requirements

### Machine

| Resource | Minimum | Recommended | Why |
|---|---|---|---|
| **RAM** | 16 GB | 32 GB | The unique-index check on `silver.sensor_events` keeps the index hot in `shared_buffers`; below 16 GB the index thrashes disk and the install ETA balloons from minutes to hours. |
| **Free disk** | 30 GB | 50 GB | Bronze ~2.5 GB/year (raw) or ~250 MB/year compressed, Silver ~3 GB, Gold ~1 GB, plus the venv (~500 MB), Postgres data dir, Power BI cache, and headroom for logs. |
| **CPU** | 4 cores | 8 cores | `flatten_sensors` runs 8 parallel workers; fewer cores still works, just slower. |
| **OS** | Windows 10 / 11 / Server 2019+ | Windows 11 / Server 2022 | Power BI Desktop is Windows-only; the rest of the stack is cross-platform. |

### Software you need installed first

| Tool | Why | Get it from |
|---|---|---|
| **Python 3.10+** | Pipeline + installer | <https://www.python.org/downloads/> |
| **Git** | Repo clone | <https://git-scm.com/downloads> |
| **PostgreSQL 17** | Silver + Gold storage | <https://www.postgresql.org/download/windows/> |
| **Power BI Desktop** | Dashboards (Windows-only) | <https://www.microsoft.com/en-us/download/details.aspx?id=58494> |
| **KNIME Analytics Platform 5.8** | ML predictions | <https://www.knime.com/downloads> |

> **Important — KNIME version:** the workflows are pinned to KNIME **5.8**.
> Installing a newer version (5.9+) on the target machine causes
> `Unsupported workflow version` errors. Pick 5.8 from the KNIME archive
> page if you need to roll back.

### Postgres tuning (one-time, before running the installer)

After installing Postgres, edit `postgresql.conf` (default location:
`C:\Program Files\PostgreSQL\17\data\postgresql.conf`) and set:

```ini
shared_buffers = 4GB        # default is 128 MB; this is the single biggest
                            # install-time win — keeps the unique index in RAM
```

Then restart the Postgres service: open the Services panel
(`services.msc`), find `postgresql-x64-17`, right-click → **Restart**.
Without this change, the silver backfill takes ~4 h instead of being
CPU-bound at ~6 minutes.

### Credentials you'll need

Have these ready before opening the install wizard:

- **Postgres admin** account (typically `postgres` / your admin password)
  — used **only at install** to create the app DB + user. Never written
  to disk after install.
- **App user credentials** to be created — e.g. `domotic` / `<your choice>`.
  This is the user the pipeline runs as.
- **SMB share credentials** for the sensor JSON files (UNC path + user/password).
- **sFTP credentials** for the weather forecasts (host, port, user, password).
- **MySQL credentials** for the school apartment registry (`pidb`).

The installer auto-detects Python, Git, Power BI, and KNIME on Windows.

---

## Step 1 — Generate `data-cycle-installer.py` from the wizard

Open the project's install wizard at `/install` in your browser.

![The install page header — “Get the project running”](img/01-install-page-header.png)

The page summarises what you're about to deploy and lists prerequisites
in plain English:

![Prerequisites overview — what you'll be wiring up + what to install first](img/02-prerequisites.png)

Scroll down and fill in the form. Most fields have sensible defaults; the
sections that need your input are credentials (Postgres / MySQL / sFTP)
and the SMB share configuration:

![SMB share form + Download installer button](img/03-install-form-smb.png)

| Field | Example | Notes |
|---|---|---|
| Postgres admin user / password | `postgres` / `<admin pwd>` | Used **only** during install. Never persisted. |
| App user / password | `domotic` / `<app pwd>` | What the pipeline scripts run as. Goes into `.env`. |
| App database name | `domotic` | Created fresh; renames are easy via the wizard. |
| Host / port | `localhost` / `5432` | Defaults to your local Postgres. |
| MySQL URL | `mysql+pymysql://student:pwd@10.130.25.152:3306/Appartments` | Provided by the school. |
| sFTP host / user / password / path | (school-provided) / `/Meteo2` | For weather CSV downloads. |
| SMB share / user / password / drive letter | `\\server\share` / `user` / `pwd` / `Z:` | Auto-mounted on Windows via `net use`. |
| Bronze root | `storage\bronze` | Where raw files land on disk. |

Click **Download installer**. Every value you typed is now baked into the
generated file. If you mis-typed something, the wizard has a *Restore
from previous installer* uploader — drop the .py file in to pre-fill the
form.

> **Tip:** the downloaded file contains your passwords in plaintext. Keep
> it on the install machine only; don't commit it to a shared repo.

---

## Step 2 — Run the installer

From a PowerShell or CMD window:

```powershell
python C:\path\to\data-cycle-installer.py
```

By default the installer creates `./data-cycle-domotic` next to where you
ran it. Pass a path to override:

```powershell
python data-cycle-installer.py D:\Projects\DataCycle
```

What happens next is a **10-phase pipeline**. Each phase prints its
progress to the console; the installer never asks for input during
phases 1–10 (only optional yes/no prompts at the very end).

### Phase 1 — Prerequisite checks (~5 s)

The installer probes for Python ≥ 3.10, git, PostgreSQL psql, Power BI
Desktop, and KNIME Analytics Platform. Missing tools produce a clear
warning and a download link, but the installer continues — Power BI and
KNIME are only needed for their respective steps and can be installed
after the fact (just re-run the installer once they're in place).

### Phase 2 — Clone repo (~30–60 s)

```
[2/10] Cloning repo...
  Repo : https://github.com/dehlya/data-cycle-domotic.git
  Branch: main
  ...
  ✓ Repo cloned to C:\DEV\users\Install\data-cycle-domotic
```

If the directory already exists, the installer does `git fetch` + `git
checkout -B main FETCH_HEAD` instead of cloning fresh — re-runs always
get the latest code.

### Phase 3 — Write `.env` (<1 s)

All connection strings + credentials + tunables you typed in the wizard
land in the project's `.env` file. The Postgres admin password is
deliberately **not** written here; only the app user's credentials are.

### Phase 4 — Python venv + dependencies (~2–3 min)

```
[4/10] Setting up Python environment
  Creating virtual environment in .venv...
  Checking for missing or outdated packages...
  ✓ Python dependencies up to date
```

The installer creates `.venv\` and runs `pip install -r requirements.txt`.
On a re-install pip skips already-installed packages and only catches
new ones (e.g. `matplotlib` if you added it later).

### Phase 5 — Pre-flight connectivity checks (~5–10 s)

The installer mounts the SMB drive (`net use Z: \\server\share /USER:...`),
opens a quick Postgres connection, and verifies MySQL + sFTP credentials.
If any of these fail it bails early with a clear error — better to fail
in 5 seconds than 4 hours in.

### Phase 6 — Database + schemas (~10–30 s)

```
[6/10] Creating database + schemas
  Connecting as postgres@localhost...
  ✓ User 'domotic' created
  ✓ Database 'domotic' created
  ✓ Schema 'silver' created
  ✓ Schema 'gold' created
  ✓ Granted permissions to 'domotic' on silver + gold
```

Idempotent — uses `CREATE IF NOT EXISTS` everywhere. Re-running this
phase on an existing install is a no-op.

### Phase 7 — Bootstrap silver (the long step)

This is what makes the first install long. There are four sub-phases:

**(a) MySQL → silver** (~25 s):
The 10 master tables (`buildings`, `rooms`, `sensors`, `devices`,
`profile`, `dierrors`, etc.) are imported into silver as dim/ref tables.

**(b) SMB → bronze → silver backfill** prompt:

```
Run full SMB → bronze → silver backfill now? (20-60 min,
idempotent — safe to re-run) [Y/n]:
```

This is the only interactive question during the install. Answer **Y**
on a first install, or **N** if you want to skip it (e.g. for a quick
test on an empty system).

![Installer mid-Phase-7 — SMB backfill prompt](img/04-installer-smb-backfill.png)

The installer first runs `bulk_to_bronze` (SMB → bronze, file copy)
followed by `flatten_sensors` (bronze → silver, the heavy ETL).

**(c) `flatten_sensors`** — the long pole:

```
flatten_sensors -- Bronze to Silver
Bronze : C:\DEV\users\Install\data-cycle-domotic\storage\bronze
DB     : localhost:5432/domotic
Workers: 8

12:41:06 [INFO] Loading watermark...
12:41:06 [INFO] Watermark: 0 files already processed
12:41:06 [INFO] Scanning for new files (newest first)...
12:41:50 [INFO] [jimmy] 191,309 new files to process
12:42:41 [INFO] [jeremie] 209,486 new files to process
12:42:41 [INFO] Found 400,795 files to process in 94.8s
12:42:41 [INFO] Batches: 201 x 2,000 files (8 parallel workers)

  [████░░░░░░░░░░] batch 1/201   2,000 files  108,000 rows  0.5%  ~125 min left
  [████░░░░░░░░░░] batch 2/201   4,000 files  220,620 rows  1.0%  ~92 min left
  ...
```

![flatten_sensors — progress bars across 8 parallel workers](img/05-installer-flatten-progress.png)

The first ETA is conservative; it tightens as workers warm up and the
COPY-into-TEMP-TABLE upsert path gets going. With `shared_buffers=4GB`
tuned (see prerequisites), the full 400k-file backfill finishes in
**~6 minutes**. Without that tuning, expect ~4 hours.

**(d) Weather sFTP download + clean** (~30 s):
sequential download (sFTP servers dislike parallel sessions) of any new
`Pred_*.csv` files, followed by `clean_weather` to push them into
`silver.weather_forecasts`.

### Phase 8 — Initial gold ETL (~30–60 s)

Once silver is full, the installer runs `populate_gold` to materialise
the star schema:

```
[8/10] Running initial gold ETL

populate_gold -- Silver -> Gold
DB   : localhost:5432/domotic
Mode : incremental (sensors, weather)
Sites: Sion

  > dim_date... 305 rows inserted
  > dim_datetime... 199,599 rows inserted
  > dim_apartment... 2 rows
    dim_apartment enriched from dim_buildings
  > masking dim_apartment PII (building_name, owner_user_id)...
    dim_apartment PII masked (name kept as RLS pseudonym, building_name + owner_user_id sanitised)
  > dim_room... 22 rows
  > dim_device... 46 rows
  > dim_tariff... 3 rows
  > fact_energy_minute... 1,891,059 rows (214.8s)
  > fact_environment_minute... 2,431,411 rows (254.4s)
  > fact_presence_minute... 2,097,142 rows (164.5s)
  > fact_device_health_day... 1,899 rows (65.9s)
  > mv_energy_with_cost (refresh)... refreshed (13.4s)
  > dim_weather_site... 1 rows (sites: ['Sion'])
  > linking apartments to weather sites...
  > fact_weather_hour... 21,527 rows (228.0s)
```

![Phase 8 — populate_gold materialising the star schema](img/06-installer-gold-running.png)

Note the **PII masking line** — the installer enforces GDPR Art. 4(1)
at the silver → gold boundary (see [DECISIONS.md](DECISIONS.md) ADR-005).
First names are kept as RLS pseudonyms; building names and user IDs are
sanitised.

When done you get a summary of every gold table and its row count:

```
Gold populated in 964s

  ✓ gold.dim_datetime: 199,599 rows
  ✓ gold.dim_date: 305 rows
  ✓ gold.dim_apartment: 2 rows
  ✓ gold.dim_room: 18 rows
  ✓ gold.dim_device: 42 rows
  ✓ gold.dim_tariff: 3 rows
  ✓ gold.dim_weather_site: 1 rows
  ✓ gold.fact_energy_minute: 1,082,931 rows
  ✓ gold.fact_environment_minute: 1,269,369 rows
  ✓ gold.fact_presence_minute: 1,113,364 rows
  ✓ gold.fact_device_health_day: 1,899 rows
  ✓ gold.fact_weather_hour: 21,527 rows
  ✓ gold.mv_energy_with_cost: 1,082,931 rows

  ✓ Initial gold ETL complete
```

![Phase 8 finished — gold summary with row counts](img/07-installer-gold-summary.png)

If any of these are 0, check the silver row counts (the watcher / admin
pane shows them) and look at `logs/populate_gold.log` for errors.

### Phase 9 — Verify + auto-config BI/KNIME (~30–60 s)

```
[9/10] Verifying + configuring BI/KNIME
  ✓ gold.dim_apartment — 2 rows
  ✓ gold.dim_room — 18 rows
  ✓ gold.dim_device — 42 rows
  ✓ gold.fact_environment_minute — 1,269,369 rows
  ✓ gold.fact_energy_minute — 1,082,931 rows
  ✓ gold.fact_presence_minute — 1,113,364 rows
  ✓ All core tables populated — your install is ready to use.

configure_bi_knime  (live)
  Target DB: domotic@localhost:5432/domotic

KNIME workflows
  ✓ Consumption_Weather_Prediction_Server.knwf :: PostgreSQL Connector → host, port, database_name
  ✓ Motion_Prediction_Server.knwf :: PostgreSQL Connector → host, port, database_name

Power BI
  ⚠ .pbix has no Connections file; skipping
Done.

  ✓ BI + KNIME configured
  ✓ KNIME detected: C:\Program Files\KNIME\knime.exe
  ✓ Deployed Consumption_Weather_Prediction_Server to KNIME workspace
  ⚠ Motion_Prediction_Server already in knime-workspace, skipping
  ✓ 1 workflow(s) deployed. Open KNIME — they will appear in the KNIME Explorer.
```

![Phase 9 — KNIME workflow patched, deployed; PBI skipped](img/08-installer-bi-knime.png)

The installer:
- Patches the `.knwf` workflow files so their PostgreSQL Connector nodes
  point at your local DB (host / port / database_name)
- Deploys the workflows to your KNIME workspace
- Tries to patch the `.pbix` data source — but Power BI stores the
  connection in a binary blob the installer can't safely modify. That's
  the `.pbix has no Connections file; skipping` warning. **You re-point
  it once after install, guided by the admin pane wizard** (see Step 4).
- Pip-installs `matplotlib` + `pandas` into Power BI's Python interpreter
  so its Python visuals render correctly.

### Phase 10 — Auto-start watcher on login (optional)

```
[10/10] Auto-start watcher on login (optional)

  The watcher runs continuously: every minute it pulls new sensor
  files, every 15 min it refreshes gold facts, and once a day
  (07:30) it downloads weather + runs ML predictions.
  Auto-starting it on login means you literally never touch it.

Auto-start the watcher every time you log in? [Y/n]:
```

Answering **Y** drops a shortcut into `shell:startup` so the watcher
launches as a hidden background process every time you sign in.

### Final post-install prompts

After the 10 phases, the installer offers four optional one-shot actions:

![Final post-install prompts](img/09-installer-final-prompts.png)

```
Run KNIME prediction workflows now? [y/N]:    # default No (~10 min)
Open the Power BI dashboard now?     [Y/n]:    # default Yes
Start the watcher now?                [Y/n]:   # default Yes (also registered for autostart if you said Y in phase 10)
Launch the admin dashboard now?       [Y/n]:   # default Yes — opens http://localhost:8501 in your browser
```

---

## Step 3 — Verify everything is healthy

The Streamlit admin dashboard at <http://localhost:8501> is the easiest
way to verify the install:

![Admin pane — Data freshness](img/11-admin-freshness.png)

What to look for on first launch:

- 🟢 **Database** indicator (top-left of the status row)
- 🟢 **Watcher process** (if you accepted autostart or started it
  manually)
- **Data freshness** cards — each gold table updated < 1 day ago
- **Gold tables** section — every fact table shows a row count > 0

If any indicator is red or any count is 0, the same dashboard has
one-click action buttons (e.g. **Run gold ETL (sensors)**, **Run KNIME
predictions**) to re-trigger that pipeline. The action runs in a
subprocess and logs to `storage\admin_logs\` for review.

You can also verify from the command line:

```powershell
cd C:\path\to\data-cycle-domotic
.venv\Scripts\python.exe scripts\status.py
```

Same checks in CLI form — useful for headless automation.

---

## Step 4 — Re-point the Power BI dashboard (one-time)

The first time the dashboard opens, Power BI is still pointing at the
developer's database (because of the binary-blob limitation in Phase 9).
The admin pane has a guided **Power BI First-Time Setup** wizard at the
top of the page that walks you through the 30-second re-point:

![Admin pane — Power BI first-time setup wizard](img/10-admin-pbi-wizard.png)

Your `localhost / domotic / domotic` values are pre-filled with one-click
copy buttons. Click **📂 Open Power BI Dashboard**, then in Power BI:

1. Click **Transform Data → Data source settings**
2. Select the existing PostgreSQL source, click **Change Source…**
3. Paste the **Server** and **Database** values from the wizard
4. Click **OK**, then **Close**, then **Apply changes**

When Power BI prompts for credentials:

1. **Authentication kind**: choose **Database** (not Windows)
2. **Username**: paste from the wizard
3. **Password**: the Postgres password you set during installation
4. Click **Connect**

Once done, click **✓ I've configured the connection — don't show again**
in the wizard. The panel collapses to a small green banner; it'll only
come back if you switch DBs later.

After re-pointing, the dashboard is live. Useful tricks:

- Press **F11** in Power BI for fullscreen presentation mode
- **Modeling → View as → Other user → Jimmy** (or `Jeremie`, or
  unselect for admin) to preview the row-level security as a tenant
- The watcher refreshes gold every 15 minutes, so hit **Home → Refresh**
  in Power BI to pull the latest

---

## Common install issues

| Symptom | Fix |
|---|---|
| `psycopg2.OperationalError: password authentication failed for user "postgres"` | Wrong admin password in the form. Re-run wizard, fix it, re-run installer. |
| `Cannot connect to MySQL` | School VPN required; check VPN is connected. |
| `SMB path not found: Z:\` | Mount failed — installer prints the `net use` command it tried; run it manually with the right credentials. |
| Silver step says "0 new files" but bronze has data | Old bug — pull latest, re-run. The watermark scanner now does a full scan each time. |
| Silver step ETA climbs above 1 hour and keeps growing | `shared_buffers` not tuned. Stop the installer, edit `postgresql.conf` to set `shared_buffers = 4GB`, restart the Postgres service, re-run. |
| KNIME predictions fail with "Attempt to overwrite the password" | Old `.knwf` shipped before the Variable-to-Credentials swap. Pull latest, re-run. |
| KNIME predictions fail with `Unsupported workflow version: 5.9.x` | The `.knwf` was exported from a newer KNIME than your VM has. Pull latest (`.knwf` files are pinned to 5.8) or install KNIME 5.8 specifically. |
| Admin dashboard fails with "DB_URL not set" | `.env` empty or missing. Re-run installer (idempotent — won't redo finished work). |
| Power BI Python visual: `ModuleNotFoundError: No module named 'matplotlib'` | Power BI's Python interpreter is missing `matplotlib`/`pandas`. The installer auto-installs them on a fresh run; if it failed (e.g. PBI was installed *after* the installer ran), do it manually: `& "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe" -m pip install matplotlib pandas` (adjust `Python311` to whatever version PBI's *Options → Python scripting* shows as the detected home). Then refresh the visual. |
| Power BI dashboard opens but tables are empty / "Cannot connect" | The `.pbix` data source still points at the developer's DB — see Step 4 above; the admin pane's setup wizard walks you through the re-point in 30 seconds. |
| Power BI Python visual: `QuerySystemError` on a single visual while others work | One DAX measure references a column that no longer exists in the gold schema. Click the visual → **Filters → Filters on this visual** pane → look for a red exclamation icon on a field; remove + re-add it. |

---

## Re-running the installer

The installer is **fully idempotent**. Re-running it:

- Skips clone if already cloned (does `git pull` to update)
- Skips deps if venv intact (re-runs `pip install` to catch up on new deps)
- Skips DB + schemas if already created (`CREATE IF NOT EXISTS`)
- Skips bronze files already in silver (watermark)
- Skips silver files already in gold (set-based merge)

So if anything fails partway, just re-run. Total time on a re-run is
typically under 15 minutes.

---

## Uninstall / clean reset

```powershell
# Stop running pipelines
Get-Process python,pythonw,knime -ErrorAction SilentlyContinue | Stop-Process -Force

# Remove autostart
Remove-Item "$env:APPDATA\Microsoft\Windows\Start Menu\Programs\Startup\DataCycle Watcher.lnk" -ErrorAction SilentlyContinue

# Drop DB + user (use admin password)
$env:PGPASSWORD = "<admin pwd>"
psql -U postgres -h localhost -c "DROP DATABASE IF EXISTS domotic"
psql -U postgres -h localhost -c "DROP USER IF EXISTS domotic"

# Remove install dir + KNIME workspace
Remove-Item C:\path\to\data-cycle-domotic -Recurse -Force
Remove-Item $HOME\knime-workspace -Recurse -Force
```

That's a full clean slate. The Postgres tuning (`shared_buffers`) will
remain in `postgresql.conf` — that's fine, it doesn't hurt anything else.
