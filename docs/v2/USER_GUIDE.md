# User Guide

Two surfaces are exposed to humans:

1. **Power BI dashboards** — for tenants and decision-makers (the data product)
2. **Streamlit admin pane** — for operators (the cockpit)

This guide covers both.

---

## 1. Power BI dashboards

### Where it lives

```
<install-dir>\bi\power_bi\DataCycleDomotic.pbix
```

Open with Power BI Desktop (Microsoft's free app, comes with the install
prereqs).

### What's in it

Three pages:

- **Energy** — apartment-level consumption, per-room breakdown, KWh trend,
  cost projections (using `dim_tariff`)
- **Environment** — temperature / humidity / CO₂ / noise / pressure trends,
  outlier highlighting, room comparison
- **Presence** — motion + door + window minute-grain heatmaps, daily
  occupancy patterns
- **Weather** (read-only context) — local forecast, used in the consumption
  prediction model

### How tenants see only their data — Row Level Security

The `.pbix` ships with three RLS roles defined:

| Role | Filter applied | Sees |
|---|---|---|
| `Jimmy` | `dim_apartment[apartment_key] = <Jimmy's key>` | Only Jimmy's data |
| `Jeremie` | `dim_apartment[apartment_key] = <Jeremie's key>` | Only Jeremie's data |
| `Admin` | (no filter) | Everything |

To preview a tenant view in Power BI Desktop:

1. **Modeling tab → View as → Other user → check Jimmy → OK**
2. The dashboard re-renders filtered to Jimmy's apartment
3. **Modeling → View as → None** to return to admin view

> RLS is enforced **at the data model layer**: every query Power BI sends to
> Postgres includes the role's WHERE clause. Switching off RLS requires
> editing the model — it's not just visual hiding.

### Refreshing the data

Power BI uses **Import mode**: it loads a snapshot of the gold tables into the
.pbix at refresh time. To pull fresh data:

1. **Home tab → Refresh** (or Ctrl+R)
2. Wait 30-60 seconds while it queries `gold.*`
3. Visuals update with the new snapshot

For a "live" experience without manual refresh, see *Auto-refresh* below.

### Fullscreen presentation mode

Press **F11** to hide all toolbars and ribbons. The dashboard fills the
screen — looks like a real product. Press F11 again to exit.

### Auto-refresh from the admin pane

The Streamlit admin pane has a **Refresh Power BI** button that:
1. Opens the .pbix in Power BI Desktop
2. Sends Ctrl+Shift+F5 (refresh all)

So you can trigger a refresh without leaving the admin tab.

### Adding a new apartment

When a new apartment is registered (new row in `gold.dim_apartment`), the
visuals update on the next refresh — but **RLS roles are model-level and must
be added manually**. One-time, takes 30 seconds:

1. **Modeling tab → Manage roles → Create**
2. Name the role (e.g. `Apartment3`)
3. Apply DAX filter: `[apartment_key] = 3`
4. Save and re-export the .pbix

There's no API to do this programmatically — it's a Microsoft tooling
limitation, not a project limitation.

### Limitation: Power BI Service unavailable

The HES-SO Microsoft 365 tenant restricts free Power BI Service signup, so a
view-only browser deployment (the typical "production" way) isn't available
for this project. The Power BI Desktop file is the deliverable artifact.

For a production rollout outside the academic context, the .pbix would be
published to Power BI Service for browser-based view-only access — but that
requires Pro licenses per user.

---

## 2. Streamlit admin pane

### Launching it

Three ways:

| Method | How |
|---|---|
| Auto-launch at end of install | Installer asks "Launch the admin dashboard now?" — answer Yes |
| Double-click | `<install-dir>\scripts\admin.bat` |
| CLI | `streamlit run scripts/admin.py` from the install dir |

Opens at <http://localhost:8501>.

### What's on it

```
┌─ DataCycle Admin ──────────────────────────────────────┐
│                                                        │
│  Database     🟢  domotic@100.96.81.114:5432/domotic   │
│  Watcher      🟢  Running                              │
│  Active job   ⚪  None                                  │
│                                                        │
│  Data freshness                                        │
│   🟢 Sensors environment   3 min ago    2026-04-27 14:11│
│   🟢 Sensors energy        3 min ago    2026-04-27 14:11│
│   🟡 Predictions motion    2 hours ago  2026-04-27 12:00│
│   ...                                                  │
│                                                        │
│  Gold tables                                           │
│   gold.dim_apartment            2 rows                 │
│   gold.dim_room                14 rows                 │
│   gold.fact_environment_minute  3,847,221 rows         │
│   ...                                                  │
│                                                        │
│  Quick actions                                         │
│   [Run gold ETL (sensors)]  [Run gold ETL (weather)]   │
│   [Run weather backfill]    [Run KNIME predictions]    │
│   [Run silver backfill]     [Cleanup old bronze]       │
│   [Refresh Power BI]        [Open Power BI]            │
│                                                        │
│  Logs                                                  │
│   [Pick a log: gold_sensors.log ▾]                     │
│   ┌──────────────────────────────────────────────┐    │
│   │ 14:23 ✓ populate_dimensions: 14 rows         │    │
│   │ 14:23 ✓ fact_environment_minute: 1234 rows   │    │
│   └──────────────────────────────────────────────┘    │
│                                                        │
│  Configuration (.env, masked)  [+]                     │
└────────────────────────────────────────────────────────┘
```

### What each section does

**Top status row**
- Database: 🟢 if connected, 🔴 with the error message if not
- Watcher: 🟢 if `watcher.py` is running anywhere on the machine
- Active job: shows when a button-triggered subprocess is mid-run

**Data freshness** — six tiles, color-coded:
- 🟢 fresh (within 26 hours for sensor minute facts; 36 hours for weather; 48 hours for predictions)
- 🟡 stale but recoverable (within 7 / 14 days)
- 🔴 missing or error

**Gold tables** — exact row count of every dim and fact table.

**Quick actions** — every button starts a job in the background and writes
its output to a log file under `storage/admin_logs/`. The dashboard prevents
launching a second job while one is running (top-right "Active job" indicator).

**Logs** — pick any log from the dropdown, see the last 300 lines (ANSI
colors stripped, plain text). Refreshes every 10 seconds with the rest of
the page.

**Configuration** — your `.env` displayed with passwords masked. Useful for
verifying what the pipeline thinks it should be connecting to without
exposing credentials.

### Auto-refresh

The whole dashboard re-renders every 10 seconds (you can see the timestamp
in the header). No manual refresh needed.

### Stopping the dashboard

Ctrl+C in the terminal where streamlit was launched.

---

## 3. The watcher (background service)

The watcher is a long-running Python process that handles all scheduling:

| Cadence | Action |
|---|---|
| Every 60s | Check SMB for new sensor JSON files → bronze → silver |
| Every 15 min | `populate_gold --sensors` (refresh all sensor fact tables) |
| Daily 06:30 | Weather download + clean + `populate_gold --weather` + KNIME predictions + `cleanup_bronze` |
| Daily 00:00 | Full SMB rescan (catches files missed by the per-minute predictor) |

Configurable via `.env`:
- `GOLD_INTERVAL_MIN` (default 15)
- `WEATHER_HOUR` / `WEATHER_MIN` (default 06:30)

### Starting it

If you accepted autostart at install: it's already running. Look at
*Watcher process* in the admin pane.

Otherwise, manually:
```powershell
cd <install-dir>
.venv\Scripts\python.exe ingestion\fast_flow\watcher.py
```

Leave the terminal open — Ctrl+C stops it.

### Re-enabling autostart

```powershell
# From the install dir, in PowerShell:
$pyw = "<install-dir>\.venv\Scripts\pythonw.exe"
$watcher = "<install-dir>\ingestion\fast_flow\watcher.py"
$startup = "$env:APPDATA\Microsoft\Windows\Start Menu\Programs\Startup\DataCycle Watcher.lnk"
$s = (New-Object -ComObject WScript.Shell).CreateShortcut($startup)
$s.TargetPath = $pyw
$s.Arguments  = "`"$watcher`""
$s.WorkingDirectory = "<install-dir>"
$s.Save()
```

### Disabling autostart

```powershell
Remove-Item "$env:APPDATA\Microsoft\Windows\Start Menu\Programs\Startup\DataCycle Watcher.lnk"
```
