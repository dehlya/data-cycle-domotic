# Installation Guide

This is the "for dummies" walkthrough. If you can fill in a web form and run
one Python command, you can install DataCycle on a fresh Windows machine in
about 45-60 minutes.

## Before you start

You need:

| Tool | Why | Get it from |
|---|---|---|
| **Python 3.11+** | Pipeline + installer | <https://www.python.org/downloads/> |
| **Git** | Repo clone | <https://git-scm.com/downloads> |
| **PostgreSQL 14+** | Silver + Gold storage | <https://www.postgresql.org/download/windows/> |
| **Power BI Desktop** | Dashboards | <https://www.microsoft.com/en-us/download/details.aspx?id=58494> (Windows-only) |
| **KNIME Analytics Platform 5.x** | ML predictions | <https://www.knime.com/downloads> |

You also need:
- A **Postgres admin account** (typically `postgres` user) — used only at install
  time to create the app DB and app user. Never written to disk.
- **SMB share credentials** for the sensor JSON files
- **sFTP credentials** for the weather forecasts
- **MySQL credentials** for the school apartment dim table

The installer auto-detects Python, Git, Power BI, and KNIME.

## Step 1 — Generate `data-cycle-installer.py`

Open the project's install wizard at `/install` in your browser. Fill in:

| Field | Example |
|---|---|
| Postgres admin user / password | `postgres` / `<your admin pwd>` |
| App user to create (used by all pipeline scripts) | `domotic` / `<your app pwd>` |
| App database name | `domotic_tests` |
| Host / port | `localhost` / `5432` |
| MySQL URL | `mysql+pymysql://user:pass@host/pidb` |
| sFTP host / user / password | (provided by school) |
| SMB share / user / password / drive letter | `\\server\share` / `user` / `pwd` / `Z:` |
| Bronze root | `storage\bronze` |

Click **Generate installer** → downloads `data-cycle-installer.py` to your
machine. Every value you typed is now baked into that file. Restart of the
install (e.g., after fixing a typo) is just re-running it — values are
preserved.

> **Tip:** if you mis-typed something, the wizard has a *Restore from
> previous installer* uploader. Drop the .py file in, the form pre-fills.

## Step 2 — Run the installer

From a PowerShell or CMD window:

```powershell
python C:\path\to\data-cycle-installer.py
```

By default the installer creates `./data-cycle-domotic` next to where you ran
it. Pass a path to override:

```powershell
python data-cycle-installer.py D:\Projects\DataCycle
```

### What it does (10 steps)

| Step | Time | What |
|---|---|---|
| 1. Prerequisites | 5 s | Check Python ≥ 3.11, git, Power BI, KNIME |
| 2. Clone repo | 30-60 s | `git clone --branch release-final` (or `git pull` if already there) |
| 3. Write `.env` | < 1 s | All connection strings + paths |
| 4. Python venv + deps | 2-3 min | `python -m venv .venv` + `pip install -r requirements.txt` |
| 5. Pre-flight | 5-10 s | Mounts SMB drive (Windows `net use`); validates Postgres / MySQL / sFTP creds |
| 6. DB + schemas | 10-30 s | Creates `domotic` user + `domotic_tests` DB + `silver` / `gold` schemas |
| 7. Bootstrap silver | 25-35 min | MySQL dim import + (optional) full SMB → bronze backfill + bronze → silver flatten + weather download + weather clean |
| 8. Initial gold ETL | 30-60 s | `populate_dimensions` + `populate_sensors` + `populate_weather` |
| 9. Verify + auto-config BI/KNIME | 30-60 s | Row-count checks + patch `.knwf` and `.pbix` host/port/db to user's local Postgres + deploy `.knwf` to KNIME workspace |
| 10. Auto-start (optional) | < 1 s | Register watcher in `shell:startup` so it runs on login |

End: a banner saying **Installation complete!** plus prompts to:
- Open Power BI now? (default: No)
- Run KNIME predictions now? (default: No, takes 5-10 min)
- Start the watcher now? (default: No — but it's also registered for autostart)
- **Launch the admin dashboard now?** (default: Yes, opens in browser at
  <http://localhost:8501>)

## Step 3 — Verify

Two quick checks:

### From the admin dashboard

The Streamlit page at <http://localhost:8501> should show:

- 🟢 Database
- 🟢 Watcher process (if you accepted autostart or started it manually)
- Data freshness cards: each gold table updated < 1 day ago
- Gold tables row counts: every fact table > 0 rows

If anything is red or empty, click the matching action button (e.g.
**Run gold ETL (sensors)**) to refill.

### From PowerShell

```powershell
cd C:\path\to\data-cycle-domotic
.venv\Scripts\python.exe scripts\status.py
```

Same checks in CLI form.

## Step 4 — Open Power BI

```powershell
start C:\path\to\data-cycle-domotic\bi\power_bi\DataCycleDomotic.pbix
```

In Power BI Desktop:
1. Click **Refresh** (Home tab) — pulls latest data from your local Postgres
2. Press **F11** to enter fullscreen presentation mode
3. To preview as a tenant: **Modeling → View as → Other user → Jimmy** (or
   `Jeremie`, or unselect for admin)

That's it.

## Common install issues

| Symptom | Fix |
|---|---|
| `psycopg2.OperationalError: password authentication failed for user "postgres"` | Wrong admin password in the form. Re-run wizard, fix it, re-run installer. |
| `Cannot connect to MySQL` | School VPN required; check VPN is connected. |
| `SMB path not found: Z:\` | Mount failed — installer prints the `net use` command it tried; run it manually with right credentials. |
| Silver step says "0 new files" but bronze has data | Old bug — pull latest, re-run. The watermark scanner now does a full scan each time. |
| KNIME predictions fail with "Attempt to overwrite the password" | Old `.knwf` shipped before the Variable-to-Credentials swap. Pull latest, re-run `configure_bi_knime.py` then `deploy_knime.py`. |
| Admin dashboard fails with "DB_URL not set" | `.env` empty or missing. Re-run installer (idempotent — won't redo finished work). |

## Re-running the installer

The installer is **fully idempotent**. Re-running it:
- Skips clone if already cloned (does `git pull` to update)
- Skips deps if venv intact
- Skips DB + schemas if already created (`CREATE IF NOT EXISTS`)
- Skips bronze files already in silver (watermark)
- Skips silver files already in gold (set-based merge)

So if anything fails partway, just re-run.

## Uninstall / clean reset

```powershell
# Stop running pipelines
Get-Process python,pythonw,knime -ErrorAction SilentlyContinue | Stop-Process -Force

# Remove autostart
Remove-Item "$env:APPDATA\Microsoft\Windows\Start Menu\Programs\Startup\DataCycle Watcher.lnk" -ErrorAction SilentlyContinue

# Drop DB + user (use admin password)
$env:PGPASSWORD = "<admin pwd>"
psql -U postgres -h localhost -c "DROP DATABASE IF EXISTS domotic_tests"
psql -U postgres -h localhost -c "DROP USER IF EXISTS domotic"

# Remove install dir + KNIME workspace
Remove-Item C:\path\to\data-cycle-domotic -Recurse -Force
Remove-Item $HOME\knime-workspace -Recurse -Force
```

That's a full clean slate.
