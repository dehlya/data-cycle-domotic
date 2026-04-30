# Architecture Decision Records

Numbered, dated, named. Each ADR documents a decision that has alternatives.

---

## ADR 001 — Custom Python watcher instead of Airflow

**Date**: 2026-03

**Context**: We need to schedule:
- A continuous bronze→silver loop (every minute)
- A periodic gold refresh (every 15 min)
- A daily ML batch (weather + KNIME + cleanup)

The textbook answer for a data engineering course is Apache Airflow. We
considered it.

**Decision**: A single Python process (`watcher.py`) handles all scheduling.
No Airflow.

**Rationale**:
- Airflow is heavy (web server, scheduler, executor, metadata DB) for a
  pipeline that has ~6 jobs total
- The "for dummies" install goal means every dependency we add is a
  potential install-time failure
- A single 350-line Python script with three time conditions is easier to
  read, debug, and explain in a report
- Restart on reboot is solved with a Windows Startup folder shortcut, no
  systemd / service management needed

**Trade-offs accepted**:
- No web UI for DAGs (replaced by the Streamlit admin pane, which is more
  fit-for-purpose anyway)
- No retry / SLA primitives (subprocess return codes + warning logs cover
  the simple cases we need)
- No DAG-level dependency expression (the watcher's three time triggers
  call functions in fixed order, which is enough for our flow)

---

## ADR 002 — Medallion architecture (bronze · silver · gold)

**Date**: 2026-02

**Context**: Three sources of varying shapes (JSON / CSV / MySQL rows) need
to be unified for ML and BI consumption.

**Decision**: Three layers:
- **Bronze** — raw, immutable, on filesystem partitioned by date
- **Silver** — cleaned, normalized, in PostgreSQL
- **Gold** — star schema for OLAP / ML / BI, in PostgreSQL

**Rationale**:
- Standard pattern for IoT / sensor pipelines
- Bronze gives us a re-runnable source-of-truth (any silver table can be
  rebuilt from bronze)
- Silver isolates parsing/cleaning from analytic concerns
- Gold's star schema is what Power BI and KNIME expect

**Trade-offs accepted**:
- Storage cost (3 copies of the data) — mitigated by aggressive bronze
  cleanup (ADR 003) and Postgres compression
- Two ETL hops instead of one — but each hop is small and idempotent

---

## ADR 003 — Aggressive bronze cleanup (delete after silver insert)

**Date**: 2026-04

**Context**: 2 apartments × 1 file/min × 365 days = 1 M JSON files/year/apt.
Bronze grows unbounded if untouched. ~10 KB per file → ~20 GB/year just for
sensors.

**Decision**: After every successful silver insert + watermark commit,
delete the bronze JSONs from disk (default behavior; opt-out with
`KEEP_BRONZE=1`). Filenames are appended to `storage/processed.log` so
future SMB rescans don't re-copy them.

**Rationale**:
- The original SMB share is the durable copy; bronze was always meant as a
  staging area
- Silver fully reconstructs the relevant data (we don't keep the raw JSON
  for any analytic purpose)
- Disk space matters on a school-allocated VM

**Trade-offs accepted**:
- If silver gets corrupted / dropped, full re-fetch from SMB is the only
  recovery (slower but possible)
- Less obvious "raw audit trail" — but `silver.etl_watermark` records
  every filename that landed, so we have provenance for free

A retention-based fallback (`scripts/cleanup_bronze.py`, 30-day default)
exists for users who set `KEEP_BRONZE=1` but still want bounded disk usage.

---

## ADR 004 — KNIME credential injection via "Variable to Credentials"

**Date**: 2026-04

**Context**: KNIME workflows need DB credentials at runtime, but those
credentials live in `.env` per user. Hardcoding is unacceptable.

We tried four KNIME mechanisms in order:

| Mechanism | Result |
|---|---|
| Inline password in PG Connector | KNIME-master-key encrypted, not portable |
| Workflow Credentials + `-credential=db;user;pwd` | Worked for the credential entry, but PG Connectors couldn't find it consistently |
| `-workflow.variable=db_pwd,...,String` directly into the password field | KNIME error: *"Attempt to overwrite the password with config key 'password' failed. It's not possible to overwrite passwords with flow variables."* |
| `-option=NODE,PARAM,VALUE,credentials` | KNIME error: *"Unknown option type for db: credentials"* (only primitive types accepted) |

**Decision**: Use a **Variable to Credentials** node bridge:
1. Two String Configuration nodes (`db_user`, `db_pwd`) at workflow root
2. A Variable to Credentials node that takes those two strings, outputs a
   credential object named `db`
3. PG Connectors bind to credential `db` via Authentication → Credentials
4. At runtime, `run_knime_predictions.py` passes
   `-workflow.variable=db_user,...,String -workflow.variable=db_pwd,...,String`

**Why this works**: KNIME blocks flow-variable overrides on `xpassword`
fields specifically. Plain String flow variables are unrestricted, and the
Variable to Credentials node builds the credential object internally
before any password field is touched. KNIME's protection rule never
triggers.

**Trade-offs accepted**:
- Workflows need a one-time GUI setup (documented in
  `ml/knime/SETUP.md`) that creates the three nodes + wiring
- Re-exporting the .knwf must preserve the wiring; SETUP.md flags this

---

## ADR 005 — GDPR: keep first names, anonymize everything else

**Date**: 2026-04

**Context**: The dim_apartment table has `owner_user_id`, `building_name`,
`occupant_name`, etc. Some of these are personal data under GDPR Art. 4(1).

**Decision**: In the `gold.dim_apartment` view exposed to BI / ML:
- `occupant_name`: keep first names (e.g. "Jimmy", "Jeremie")
- `owner_user_id`: NULL
- `building_name`: replaced with anonymous label "Building <id>"
- All sensor + energy + presence data: keep, with apartment_key as the
  only re-identifier

Power BI enforces RLS so a tenant viewing the dashboard sees only their own
data, never another tenant's.

**Rationale**:
- A first name alone is generally not considered personal data under GDPR
  Art. 4(1) absent additional context (lots of people share first names)
- Stripping first names entirely would make the dashboard sterile and
  harder to demo
- Building name + user IDs ARE direct identifiers; those are masked
- For a real production deployment (>2 tenants, real people), first names
  would also be stripped or pseudonymized

**Trade-offs accepted**:
- A determined attacker with side knowledge could re-identify "Jimmy" as a
  specific person — but Art. 4(1)'s "reasonably likely" identification
  test is borderline-met by this scenario
- Documented as a known limitation; production rollout would need a
  GDPR review with the school's DPO

---

## ADR 006 — Two-role Postgres install (admin + app)

**Date**: 2026-03

**Context**: Should we run everything as the Postgres superuser, or split?

**Decision**: Two roles:
- **Admin** (typically `postgres`) — used ONLY at install time, in memory,
  to create the app user / app DB / schemas. Password never written to
  `.env` or any file.
- **App** (`domotic`) — has DML / DDL on `silver` and `gold` schemas only.
  No CREATE DATABASE, no superuser. Password lives in `.env`.

**Rationale**:
- Principle of least privilege — runtime should not have admin rights
- Limits blast radius if `.env` leaks
- Standard production pattern; good to model in an academic project

**Trade-offs accepted**:
- Slightly more complex install (one extra prompt for the admin password)
- Adding a new schema requires either (a) the admin's password again, or
  (b) granting `domotic` CREATE on the database — we chose (a) because it's
  rare

---

## ADR 007 — COPY into temp table for silver upserts

**Date**: 2026-04

**Context**: Original `flatten_sensors` used `INSERT ... VALUES (:a, :b, ...)
ON CONFLICT DO UPDATE` per row via SQLAlchemy. A 220k-file backfill took
~3 hours, almost all of which was DB write time.

**Decision**: Use `psycopg2.copy_expert` to stream rows into a `TEMP TABLE
ON COMMIT DROP`, then a single `INSERT INTO silver.sensor_events SELECT
DISTINCT ON (...) ... FROM tmp_table ON CONFLICT DO UPDATE`.

**Rationale**:
- COPY skips per-statement parsing — bytes go directly into the temp table
- One `INSERT ... SELECT ... ON CONFLICT` is a single set-based operation,
  minimal overhead vs millions of single-row INSERTs
- DISTINCT ON dedupes within-batch (PostgreSQL forbids upserting same
  key twice in one statement)

**Result**: 3 hours → ~10-15 minutes. ~50-150x speedup measured.

**Trade-offs accepted**:
- COPY format requires careful CSV escaping (handled by Python's `csv`
  module with `QUOTE_MINIMAL` + `NULL ''`)
- Per-batch temp table creation has a small fixed overhead, but amortized
  over 2 000 files per batch

---

## ADR 008 — Streamlit admin pane in addition to status.py CLI

**Date**: 2026-04

**Context**: Operators need to:
- Check pipeline freshness
- Trigger ad-hoc ETL runs
- Read logs
- See config without exposing passwords

A CLI tool (`status.py`) covers it for technical users. Non-technical users
prefer a GUI.

**Decision**: Build `scripts/admin.py` (Streamlit) for the GUI, **keep**
`status.py` (CLI) for terminal users. Both read the same data.

**Rationale**:
- Streamlit is one `pip install` away — no separate web server, no
  React build, no auth setup
- Auto-refresh every 10 seconds gives near-live status without polling logic
- Buttons → subprocess.Popen with output redirected to log files — no
  in-process state to manage
- File-based log tail is dead-simple and aligns with how existing scripts
  already write logs

**Trade-offs accepted**:
- Streamlit adds ~50MB to venv
- No auth on the admin pane — but it binds to localhost only by default,
  so reaching it requires already being on the VM

---

## ADR 009 — Power BI Desktop only (no Service)

**Date**: 2026-04

**Context**: The natural deployment for view-only Power BI is Power BI
Service (cloud). HES-SO's Microsoft 365 tenant restricts free Power BI
signup, so individual students can't publish to Service.

**Decision**: Ship the `.pbix` as a local Desktop file. Document F11
fullscreen as the "view mode" approximation.

**Considered alternatives**:
- Power BI Report Server Developer Edition (self-hosted) — rejected as
  disproportionate infrastructure for a school project
- Microsoft 365 Developer Program tenant — possible, but adds external
  dependency outside the "local-only" goal
- Switch to Streamlit dashboards — viable but the school requirement is
  explicitly Power BI

**Rationale**:
- The school requires Power BI specifically
- The "local-only" deployment requirement conflicts with Service's cloud
  nature anyway
- Power BI Desktop in F11 mode is acceptable for an academic demo
- RLS still enforces per-apartment data isolation at the model layer,
  regardless of UI

**Trade-offs accepted**:
- End users see editing chrome until they press F11
- No automatic refresh — users hit Refresh manually or use the admin
  pane's "Refresh Power BI" button (sends Ctrl+Shift+F5 via WScript)
- Documented as a known limitation in the report

---

## ADR 010 — Self-contained installer + web wizard

**Date**: 2026-04

**Context**: The "for dummies" install goal means a non-technical user
should run **one command** to deploy everything. Cloning a repo, creating
a venv, configuring `.env`, etc. is too much for that audience.

**Decision**: Two-step install:
1. **Web wizard** (`/install` page) — user fills in a form
2. **Single Python file** generated client-side with all values baked in;
   user downloads it and runs `python data-cycle-installer.py`

The wizard's JavaScript renders `installer/install_template.py` (server-side
copy at `website/public/install_template.py`) by replacing `{{PLACEHOLDER}}`
tokens with the form values. The output is a valid standalone installer
the user runs locally.

**Rationale**:
- Zero credentials transit through any backend (form values never leave
  the browser)
- The `.py` file is auditable: user can read it before running, see what
  it'll do
- Re-installs are easy: keep the file, run again
- Installer is fully idempotent so re-runs are safe

**Trade-offs accepted**:
- The user must trust the wizard / install_template — but the file is
  open source and inspectable before run
- The template lives in two places (installer/ and website/public/) and
  must be kept in sync — handled by a `cp` on every commit, with a CI
  check planned
