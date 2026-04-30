# Installation Guide

The "for dummies" walkthrough. If you can fill in a web form and run one
Python command, you can install DataCycle on a fresh Windows machine.

**Time budget:** about **4 hours on a first install** (the silver backfill is
the long pole — years of historical sensor data land in one go) or **~15
minutes on a re-install** (watermarks let every step short-circuit on
already-done work).

> Treat the long first install like a build pipeline run, not a wizard.
> Start it, walk away, come back to a finished system. There are no manual
> steps in the middle.

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
> Installing a newer (e.g. 5.9) version on the target machine will cause
> `Unsupported workflow version` errors. Pick 5.8 from the KNIME archive page
> if needed.

### Postgres tuning (one-time)

After installing Postgres, edit `postgresql.conf` (default location:
`C:\Program Files\PostgreSQL\17\data\postgresql.conf`) and set:

```ini
shared_buffers = 4GB        # default is 128MB; this is the single biggest
                            # install-time win — keeps the unique index in RAM
```

Then restart the Postgres service (Services panel → `postgresql-x64-17` →
Restart). Without this change, the silver backfill will take ~4 h instead
of being CPU-bound.

### Credentials you'll need

- **Postgres admin** account (typically `postgres` / your admin password)
  — used **only at install** to create the app DB + user. Never written
  to disk.
- **SMB share credentials** for the sensor JSON files
- **sFTP credentials** for the weather forecasts
- **MySQL credentials** for the school apartment registry (`pidb`)

The installer auto-detects Python, Git, Power BI, and KNIME on Windows.

---

## Step 1 — Generate `data-cycle-installer.py`

Open the project's install wizard at `/install` in your browser.

![Install page header — “Get the project running”](img/01-install-page-header.png)

The page summarises what you're about to deploy and the prerequisites:

![Prerequisites overview](img/02-prerequisites.png)

Fill in the form with the values you collected above. Most fields have
sensible defaults; the SMB and credentials sections are the parts that need
your input:

![SMB share form + Download button](img/03-install-form-smb.png)

| Field | Example |
|---|---|
| Postgres admin user / password | `postgres` / `<your admin pwd>` |
| App user to create (used by all pipeline scripts) | `domotic` / `<your app pwd>` |
| App database name | `domotic` |
| Host / port | `localhost` / `5432` |
| MySQL URL | `mysql+pymysql://user:pass@host/Appartments` |
| sFTP host / user / password | (provided by school) |
| SMB share / user / password / drive letter | `\\server\share` / `user` / `pwd` / `Z:` |
| Bronze root | `storage\bronze` |

Click **Download installer** → downloads `data-cycle-installer.py` to your
machine. Every value you typed is now baked into that file. Restarts of the
install (e.g. after fixing a typo) are just re-running it — values are
preserved.

> **Tip:** if you mis-typed something, the wizard has a *Restore from
> previous installer* uploader. Drop the .py file in, the form pre-fills.

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

### What it does (10 steps)

| Step | Time | What |
|---|---|---|
| 1. Prerequisites | 5 s | Check Python ≥ 3.10, git, Power BI, KNIME |
| 2. Clone repo | 30–60 s | `git clone --branch main` (or `git pull` if already there) |
| 3. Write `.env` | < 1 s | All connection strings + paths |
| 4. Python venv + deps | 2–3 min | `python -m venv .venv` + `pip install -r requirements.txt` |
| 5. Pre-flight | 5–10 s | Mounts SMB drive (Windows `net use`); validates Postgres / MySQL / sFTP creds |
| 6. DB + schemas | 10–30 s | Creates `domotic` user + `domotic` DB + `silver` / `gold` schemas |
| 7. Bootstrap silver | **2–4 h** *(first install)* / **~5 min** *(re-run)* | MySQL dim import + (optional) full SMB → bronze backfill + bronze → silver flatten + weather download + weather clean |
| 8. Initial gold ETL | 30–60 s | `populate_dimensions` + `populate_sensors` + `populate_weather` |
| 9. Verify + auto-config BI/KNIME | 30–60 s | Row-count checks; pip-install matplotlib + pandas into Power BI's Python so its visuals work; deploy `.knwf` to KNIME workspace |
| 10. Auto-start (optional) | < 1 s | Register watcher in `shell:startup` so it runs on login |

The bootstrap-silver step is what makes the first install long. You'll see
its progress in real time:

![Installer terminal — Gold ETL refreshing](img/04-installer-gold-etl.png)

End: a banner saying **Installation complete!** plus interactive prompts
for the optional final steps:

![Installer terminal — KNIME prompt](img/05-installer-knime-prompt.png)

- Run KNIME predictions now? (default: No, takes ~10 min)
- Open Power BI now? (default: Yes)
- Start the watcher now? (default: Yes — but it's also registered for autostart)
- **Launch the admin dashboard now?** (default: Yes, opens in browser at
  <http://localhost:8501>)

---

## Step 3 — Verify

### From the admin dashboard

The Streamlit page at <http://localhost:8501> shows everything you need:

![Admin pane — Data freshness](img/07-admin-freshness.png)

What to look for on first launch:
- 🟢 **Database** indicator (top-left of the status row)
- 🟢 **Watcher process** (if you accepted autostart)
- **Data freshness** cards: each gold table updated < 1 day ago
- **Gold tables** section: every fact table has rows

If anything is red or empty, the same dashboard has one-click action
buttons (e.g. **Run gold ETL (sensors)**, **Run KNIME predictions**) to
re-trigger that pipeline.

### From PowerShell (alternative)

```powershell
cd C:\path\to\data-cycle-domotic
.venv\Scripts\python.exe scripts\status.py
```

Same checks in CLI form — useful for headless automation.

---

## Step 4 — Open Power BI (one-time setup)

The first time the dashboard opens, Power BI is still pointing at the
developer's database. Power BI stores its data-source connection inside a
binary blob the installer can't safely patch — but the admin pane has a
guided **First-Time Setup** wizard at the top of the page:

![Admin pane — Power BI first-time setup wizard](img/06-admin-pbi-wizard.png)

Your `localhost / domotic / domotic` values are pre-filled with one-click
copy buttons. Click **Open Power BI Dashboard**, then in Power BI:

1. **Transform Data → Data source settings**
2. Click **Change Source…**
3. Paste the **Server** and **Database** values from the wizard
4. Click **OK**, then **Close**, then **Apply changes**
5. When prompted for credentials: choose **Database** authentication, paste
   the **Username** from the wizard and the **Postgres password you set
   during install**, click **Connect**

Once done, click **✓ I've configured the connection — don't show again** in
the wizard. The panel collapses to a small green banner; it'll only come
back if you switch DBs later.

After re-pointing:
- Press **F11** in Power BI for fullscreen presentation
- **Modeling → View as → Other user → Jimmy** (or `Jeremie`, or unselect
  for admin) to preview RLS as a tenant

---

## Common install issues

| Symptom | Fix |
|---|---|
| `psycopg2.OperationalError: password authentication failed for user "postgres"` | Wrong admin password in the form. Re-run wizard, fix it, re-run installer. |
| `Cannot connect to MySQL` | School VPN required; check VPN is connected. |
| `SMB path not found: Z:\` | Mount failed — installer prints the `net use` command it tried; run it manually with the right credentials. |
| Silver step says "0 new files" but bronze has data | Old bug — pull latest, re-run. The watermark scanner now does a full scan each time. |
| Silver step ETA climbs above 1 hour and keeps growing | `shared_buffers` not tuned. Stop the installer, edit `postgresql.conf` to set `shared_buffers = 4GB`, restart Postgres service, re-run. |
| KNIME predictions fail with "Attempt to overwrite the password" | Old `.knwf` shipped before the Variable-to-Credentials swap. Pull latest, re-run. |
| KNIME predictions fail with `Unsupported workflow version: 5.9.x` | The `.knwf` was exported from a newer KNIME than your VM has. Pull latest (`.knwf` files are pinned to 5.8) or install KNIME 5.8 specifically. |
| Admin dashboard fails with "DB_URL not set" | `.env` empty or missing. Re-run installer (idempotent — won't redo finished work). |
| Power BI Python visual: `ModuleNotFoundError: No module named 'matplotlib'` | Power BI's Python interpreter is missing `matplotlib`/`pandas`. The installer auto-installs them on a fresh run; if it failed (e.g. PBI was installed *after* the installer ran), do it manually: `& "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe" -m pip install matplotlib pandas` (adjust `Python311` to whatever version PBI's *Options → Python scripting* shows as the detected home). Then refresh the visual. |
| Power BI dashboard opens but tables are empty / "Cannot connect" | The `.pbix` data source still points at the developer's DB — see [Step 4](#step-4--open-power-bi-one-time-setup) above; the admin pane's setup wizard walks you through the re-point in 30 seconds. |

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
