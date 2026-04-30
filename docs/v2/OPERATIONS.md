# Operations Runbook

For whoever maintains this after install — Sacha, future-Dehlya, or a stranger.

## Daily check (30 seconds)

Open <http://localhost:8501> (the admin pane). Healthy state:

- 🟢 Database
- 🟢 Watcher process
- All 6 freshness tiles green
- All gold facts > 0 rows

If anything is yellow / red → see *Troubleshooting* below.

## Weekly check (5 minutes)

| What | Where to look | What's normal |
|---|---|---|
| Bronze disk usage | `storage\bronze\` size on disk | Should hover at ~1-2 GB if aggressive cleanup is on. If growing without bound, cleanup is broken |
| Watcher uptime | Task Manager → look for `pythonw.exe` running `watcher.py` | Should have been up since last login (autostart) or since you last started it manually |
| Logs in `storage\admin_logs\` | `Get-ChildItem storage\admin_logs\` | A handful of `.log` files; oldest can be deleted |
| Failed prediction runs | `gold.fact_prediction_motion` `MAX(prediction_made_at)` | Should be within 24 h |

## Monthly maintenance

Nothing strictly required — the pipeline is self-maintaining. But once a
month it's worth:

1. **Check Postgres disk usage**:
   ```sql
   SELECT pg_size_pretty(pg_database_size('domotic_tests'));
   SELECT schemaname, relname, pg_size_pretty(pg_relation_size(relid))
   FROM pg_catalog.pg_statio_user_tables ORDER BY pg_relation_size(relid) DESC LIMIT 20;
   ```

2. **Vacuum / analyze** (Postgres usually does this automatically but
   forcing once a month doesn't hurt):
   ```sql
   VACUUM ANALYZE silver.sensor_events;
   VACUUM ANALYZE gold.fact_environment_minute;
   ```

3. **Check for stale predictions** — if KNIME workflow files were edited,
   re-run once: from the admin pane click **Run KNIME predictions**.

4. **Review `install.log`** for any warnings logged at install time that
   might indicate misconfiguration.

## Adding a new apartment

The pipeline auto-handles new apartments at the data layer; only the
Power BI RLS roles need a manual touch.

### Step-by-step

1. **Add the apartment to MySQL** (school admin will do this) — a new row
   in the `apartment` table.

2. **Wait for the next gold ETL pass** (or click **Run gold ETL (sensors)**
   in the admin pane). This pulls the new row into `gold.dim_apartment`.

3. **Add the corresponding RLS role in Power BI**:
   - Open `bi/power_bi/DataCycleDomotic.pbix` in Power BI Desktop
   - **Modeling → Manage roles → Create**
   - Name: e.g. `Apartment3`
   - Filter: `[apartment_key] = 3` (use the actual key — query
     `gold.dim_apartment` to find it)
   - **Save** → File → Save
   - Re-export the `.pbix` to the install dir if you want it to ship with
     the install package next time

4. **Verify**: Modeling → View as → Apartment3 → confirm only the new
   apartment's data shows.

## Adding a new room or device

100% automatic. The first sensor JSON file from a new room/device that
lands in bronze creates the corresponding `dim_room` / `dim_device` row on
the next gold pass. No manual action.

## Rotating the DB password

The app user (`domotic`) password lives in `.env` only.

```sql
ALTER USER domotic PASSWORD '<new pwd>';
```

Then update `.env`:
```
DB_URL=postgresql://domotic:<new pwd>@localhost:5432/domotic_tests
```

Restart the watcher (and any running streamlit) so they pick up the new env.

The KNIME workflows pick up the new password automatically because
`run_knime_predictions.py` reads it from `.env` and passes it as a
`-workflow.variable=db_pwd,...` flag at runtime.

The Power BI .pbix has the password baked in — re-run
`scripts\configure_bi_knime.py` to patch the new password in.

## Backup strategy

There's no built-in backup. For a school project this is acceptable; for
production you'd want:

| What | How | Frequency |
|---|---|---|
| Postgres `silver` + `gold` schemas | `pg_dump domotic_tests > backup.sql` | Daily |
| `.env` | Manual copy | After every change |
| `.knwf` workflows | Already in git | After every edit |
| `.pbix` | Already in git | After every edit |
| `bronze` | Disposable — re-fetchable from SMB | Skip |

A simple PowerShell scheduled task with `pg_dump` covers it.

## Troubleshooting

### 🔴 Database red on the admin pane

```
Connection refused / could not connect to server
```

- PostgreSQL service stopped → `Start-Service postgresql-x64-17`
- Wrong host in `.env` → check `DB_URL` matches what `pg_isready -h <host>` says
- Firewall blocking 5432 → `New-NetFirewallRule ...` if remote, or just use `localhost`

```
password authentication failed for user "domotic"
```

- Password in `.env` doesn't match what's in `pg_authid` → reset with
  `ALTER USER domotic PASSWORD '...'` and update `.env` to match

### 🟡 Watcher not running

```powershell
# Start it manually
cd <install-dir>
.venv\Scripts\python.exe ingestion\fast_flow\watcher.py
```

If autostart shortcut got deleted, see USER_GUIDE.md *Re-enabling autostart*.

### 🟡 Sensor data freshness > 1 hour

Means the watcher loop is stuck or SMB is unreachable.

```powershell
# Test SMB reachability
Get-Item Z:\
ls Z:\ | Select -First 5

# If SMB is fine, check watcher log
Get-Content storage\admin_logs\silver.log -Tail 50
```

Common causes:
- Z: drive unmounted (Windows VM rebooted) — re-run installer (idempotent),
  or manually `net use Z: \\server\share /USER:user pwd`
- SMB credentials expired
- Watcher crashed — restart it

### 🔴 KNIME predictions fail

Check `storage/admin_logs/knime.log`. Common errors:

| Error | Fix |
|---|---|
| `Java exit code=4` immediately | KNIME GUI was open with the same workspace → close it: `Get-Process knime \| Stop-Process -Force` |
| `Attempt to overwrite the password` | Old `.knwf` shipped before the Variable-to-Credentials swap → pull latest, run `configure_bi_knime.py` then `deploy_knime.py` |
| `Database host is not defined` | `configure_bi_knime.py` wasn't run after install → `python scripts\configure_bi_knime.py` |
| `Input table is empty` | Source gold data not populated — run the gold ETL first |

### 🟡 Bronze disk usage growing

Aggressive cleanup is supposed to delete bronze JSONs after they land in
silver. If disk keeps growing:

```powershell
# Check that aggressive cleanup is enabled
Select-String -Path .env -Pattern "KEEP_BRONZE"  # should be absent or =0

# Check that processed.log is being appended
Get-Item storage\processed.log | Format-List Length, LastWriteTime
```

If aggressive cleanup is disabled or broken, the daily `cleanup_bronze.py`
retention pass still kicks in (default: delete files > 30 days old). To
trigger it manually:

```powershell
.venv\Scripts\python.exe scripts\cleanup_bronze.py
```

### 🔴 Gold tables empty after install

```powershell
# Re-run gold ETL only
.venv\Scripts\python.exe -m etl.silver_to_gold.populate_gold

# Or via the admin pane: click "Run gold ETL (sensors)" then "Run gold ETL (weather)"
```

If silver is also empty, then bronze→silver didn't run. Check
`storage/admin_logs/silver.log` and re-run `python ingestion\fast_flow\watcher.py --scan`.

### 🟡 Power BI says "Cannot connect"

The .pbix has Postgres credentials baked in at install time. If they go
stale (password rotated, host changed):

```powershell
# Re-patch from current .env values
.venv\Scripts\python.exe scripts\configure_bi_knime.py

# Then in Power BI: Home → Refresh
```

If still failing: the user account Power BI is logged in as may not have
local DB driver permissions. Check Data source settings in Power BI.

## Logs

| Log file | What's in it |
|---|---|
| `install.log` | Everything the installer did, written at install time |
| `logs/clean_weather.log` | Weather pipeline, persisted by clean_weather's logging handler |
| `storage/admin_logs/*.log` | Output from admin-pane buttons (one log per action) |
| `storage/processed.log` | Filenames imported to silver and removed from bronze (skip-list for SMB rescans) |

The watcher itself writes to stdout — if you launch it from a terminal
(not autostart), you see output live. Autostart-launched watchers write to
no log; if you need visibility there, switch to a manual launch in a
terminal you can see.

## Disaster recovery

### "I lost the install dir"

The repo is in git. The `.env` is the only non-recoverable file (it has
your specific creds). Re-run the installer:

```powershell
python data-cycle-installer.py    # if you still have the .py from your wizard download
```

It re-clones, re-creates venv, etc. The Postgres data survives independently.

### "I lost everything including the DB"

Painful but recoverable:
1. Re-run installer → re-creates DB + schemas
2. Watcher's first scan rebuilds bronze from SMB
3. flatten_sensors rebuilds silver from bronze
4. populate_gold rebuilds gold
5. KNIME predictions rebuild themselves on the next daily batch

Total time: same as a fresh install. Maybe slower if SMB has a lot of
historical files (~30 min for ~200 k files).

### "I lost the SMB historical data"

Then you can't backfill. Anything not yet in bronze/silver is gone. This
is why a real production deployment would replicate bronze offsite — but
for a school project, accept the constraint.
