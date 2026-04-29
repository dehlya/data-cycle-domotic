// Generate the project's technical documentation as a polished .docx file.
// Combines docs/v2/TECHNICAL.md + DECISIONS.md content into one Word doc
// with cover page, TOC, headings, tables, and ADR sections.
//
// Run with: node tools/generate_technical_docx.js
//
// Output: docs/v2/out/DataCycle_Technical_Documentation.docx

const fs = require("fs");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, PageOrientation, LevelFormat,
  ExternalHyperlink, TabStopType, TabStopPosition,
  TableOfContents, HeadingLevel, BorderStyle, WidthType, ShadingType,
  VerticalAlign, PageNumber, PageBreak, ShadingType: _,
} = require("docx");

// ── Style helpers ───────────────────────────────────────────────────────────
const ACCENT = "2E5BFF";          // blue-ish
const TEAL   = "0F766E";
const ORANGE = "C2410C";
const GREY_BG = "F3F4F6";
const GREY_LINE = "D1D5DB";
const TEXT_DIM  = "6B7280";

const border = (color = GREY_LINE) => ({ style: BorderStyle.SINGLE, size: 1, color });
const allBorders = (color = GREY_LINE) => ({
  top: border(color), bottom: border(color), left: border(color), right: border(color),
});
const noBorder = { style: BorderStyle.NONE, size: 0, color: "FFFFFF" };
const noBorders = { top: noBorder, bottom: noBorder, left: noBorder, right: noBorder };

const t = (text, opts = {}) => new TextRun({ text, ...opts });
const para = (children, opts = {}) =>
  new Paragraph({ children: Array.isArray(children) ? children : [children], ...opts });
const h1 = (text) => para([t(text, { bold: true, size: 36, color: "111827" })], {
  heading: HeadingLevel.HEADING_1, spacing: { before: 480, after: 240 },
});
const h2 = (text) => para([t(text, { bold: true, size: 28, color: ACCENT })], {
  heading: HeadingLevel.HEADING_2, spacing: { before: 360, after: 180 },
});
const h3 = (text) => para([t(text, { bold: true, size: 24, color: "111827" })], {
  heading: HeadingLevel.HEADING_3, spacing: { before: 280, after: 140 },
});
const p = (text, opts = {}) => para([t(text, { size: 22 })], { spacing: { after: 140 }, ...opts });
const code = (text) => para([t(text, { font: "Cascadia Code", size: 20, color: "0F172A" })], {
  shading: { fill: GREY_BG, type: ShadingType.CLEAR },
  spacing: { before: 100, after: 100 },
  indent: { left: 200, right: 200 },
});
const codeBlock = (lines) => lines.map(line => code(line));
const bullet = (text) => new Paragraph({
  children: [t(text, { size: 22 })],
  numbering: { reference: "bullets", level: 0 },
  spacing: { after: 80 },
});
const numbered = (text) => new Paragraph({
  children: [t(text, { size: 22 })],
  numbering: { reference: "numbers", level: 0 },
  spacing: { after: 80 },
});

// Table builder
const cell = (text, opts = {}) => new TableCell({
  borders: allBorders(opts.border || GREY_LINE),
  width: { size: opts.width || 2000, type: WidthType.DXA },
  shading: opts.fill ? { fill: opts.fill, type: ShadingType.CLEAR } : undefined,
  margins: { top: 100, bottom: 100, left: 140, right: 140 },
  children: [new Paragraph({ children: Array.isArray(text)
    ? text.map(t_ => typeof t_ === "string" ? t(t_, { size: 20, ...opts.textOpts }) : t_)
    : [t(text, { size: 20, bold: opts.bold, ...opts.textOpts })] })],
});
const row = (cells) => new TableRow({ children: cells });
const tbl = (headers, rows, columnWidths) => {
  const totalWidth = columnWidths.reduce((a, b) => a + b, 0);
  return new Table({
    width: { size: totalWidth, type: WidthType.DXA },
    columnWidths,
    rows: [
      row(headers.map((h, i) => cell(h, { bold: true, fill: GREY_BG, width: columnWidths[i] }))),
      ...rows.map(r => row(r.map((c, i) => cell(c, { width: columnWidths[i] })))),
    ],
  });
};

// Callout (styled paragraph)
const callout = (label, body, color = ACCENT) => new Table({
  width: { size: 9360, type: WidthType.DXA },
  columnWidths: [9360],
  rows: [row([new TableCell({
    borders: { top: noBorder, right: noBorder, bottom: noBorder,
      left: { style: BorderStyle.SINGLE, size: 24, color } },
    width: { size: 9360, type: WidthType.DXA },
    margins: { top: 120, bottom: 120, left: 200, right: 200 },
    shading: { fill: GREY_BG, type: ShadingType.CLEAR },
    children: [
      para([t(label, { bold: true, size: 20, color, allCaps: true })], { spacing: { after: 80 } }),
      para([t(body, { size: 22 })]),
    ],
  })])],
});

// ── Build the document ─────────────────────────────────────────────────────
const sections = [];

// ============= COVER PAGE =============
sections.push({
  properties: {
    page: {
      size: { width: 12240, height: 15840 },
      margin: { top: 2880, right: 1440, bottom: 1440, left: 1440 },
    },
    type: "nextPage",
  },
  children: [
    para([t("DataCycle Domotic", { size: 72, bold: true, color: "111827" })],
      { alignment: AlignmentType.CENTER, spacing: { after: 200 } }),
    para([t("Smart-Apartment IoT Data Platform", { size: 36, color: TEXT_DIM })],
      { alignment: AlignmentType.CENTER, spacing: { after: 1200 } }),

    para([t("Technical Documentation", { size: 48, bold: true, color: ACCENT })],
      { alignment: AlignmentType.CENTER, spacing: { after: 600 } }),

    para([t("End-to-end medallion ETL pipeline, ML predictions, BI dashboards,", { size: 24 })],
      { alignment: AlignmentType.CENTER, spacing: { after: 80 } }),
    para([t("and a self-contained installer for non-technical users.", { size: 24 })],
      { alignment: AlignmentType.CENTER, spacing: { after: 1600 } }),

    new Table({
      width: { size: 6000, type: WidthType.DXA },
      columnWidths: [2400, 3600],
      alignment: AlignmentType.CENTER,
      rows: [
        row([
          cell("Project", { bold: true, width: 2400, fill: GREY_BG }),
          cell("DataCycle Domotic — Group 14", { width: 3600 }),
        ]),
        row([
          cell("Institution", { bold: true, width: 2400, fill: GREY_BG }),
          cell("HES-SO Valais — Data Engineering", { width: 3600 }),
        ]),
        row([
          cell("Academic year", { bold: true, width: 2400, fill: GREY_BG }),
          cell("2025-2026 (Spring semester)", { width: 3600 }),
        ]),
        row([
          cell("Document version", { bold: true, width: 2400, fill: GREY_BG }),
          cell("1.0", { width: 3600 }),
        ]),
        row([
          cell("Document date", { bold: true, width: 2400, fill: GREY_BG }),
          cell("April 2026", { width: 3600 }),
        ]),
        row([
          cell("Status", { bold: true, width: 2400, fill: GREY_BG }),
          cell("Final", { width: 3600 }),
        ]),
      ],
    }),

    para([new PageBreak()]),
  ],
});

// ============= MAIN DOC =============
const main = [];

// (AI Tools Usage section is appended at the end of the document, see below.)

main.push(h1("Executive summary"));
main.push(p("DataCycle Domotic is an end-to-end data platform that ingests sensor readings from two smart apartments (~2 880 JSON files per day), MySQL apartment metadata, and daily weather forecasts via sFTP, then transforms them through a three-layer medallion architecture (bronze, silver, gold) on PostgreSQL 17. Two KNIME workflows produce ML-based motion and consumption forecasts. Power BI dashboards expose per-apartment views via Row-Level Security, and a Streamlit admin pane provides operators with one-click pipeline control."));
main.push(p("The system runs entirely on a single Windows VM, with no cloud dependencies. Deployment is automated through a web wizard that generates a single self-contained Python installer the user runs once. After install, a continuous Python watcher schedules every layer (bronze→silver every minute, gold every 15 minutes, ML batch + cleanup daily at 06:30) without external orchestration tools."));
main.push(p("This document covers the architecture, data model, pipeline internals, configuration surface, security posture, and the ten architecture decisions made along the way (ADRs)."));

// ============= TABLE OF CONTENTS =============
main.push(para([new PageBreak()]));
main.push(h1("Contents"));
main.push(new TableOfContents("Contents", { hyperlink: true, headingStyleRange: "1-3" }));

main.push(para([new PageBreak()]));

// ============= 1. ARCHITECTURE OVERVIEW =============
main.push(h1("1 · Architecture overview"));

main.push(h2("1.1 Three external sources"));
main.push(tbl(
  ["Source", "Format", "Volume", "Path"],
  [
    ["SMB share (sensor data)", "One JSON per minute per apartment", "~2 880 files/day for 2 apartments", "\\\\server\\share\\ → mounted as Z:\\"],
    ["MySQL (apartment metadata)", "Tables: apartment, room, device", "Static, refreshed weekly", "School DB pidb"],
    ["sFTP (weather forecasts)", "One CSV per day per site, ~150 k rows each", "1 file/day", "School sFTP server"],
  ],
  [2200, 2300, 2300, 2560],
));

main.push(h2("1.2 Medallion layers (PostgreSQL 17)"));
main.push(p("Three storage tiers, each with a distinct purpose:"));
main.push(bullet("Bronze — raw, immutable JSON / CSV files on the filesystem, partitioned by year/month/day/hour. Acts as the source of truth before any cleanup is applied."));
main.push(bullet("Silver — cleaned, normalised PostgreSQL tables. Sensor events are pivoted into a long-format silver.sensor_events table; weather rows go to silver.weather_forecasts."));
main.push(bullet("Gold — analytical star schema with conformed dimensions (dim_apartment, dim_room, dim_device, dim_date, dim_datetime, dim_tariff, dim_weather_site) and pre-aggregated fact tables (fact_environment_minute, fact_energy_minute, fact_presence_minute, fact_device_health_day, fact_weather_hour, fact_prediction_*)."));

main.push(callout("Naming convention",
  "All gold tables follow the dim_<noun> / fact_<grain> pattern. Surrogate keys are integer columns ending in _key; natural keys keep their source-system name (apartment_id, room_name, device_id).",
  TEAL));

main.push(h2("1.3 Where each component runs"));
main.push(p("A single Python process (watcher.py) drives the whole pipeline as a long-running scheduler:"));
main.push(...codeBlock([
  "                       ┌──────────────────────────────────────┐",
  "                       │  watcher.py  — single long-running   │",
  "                       │   Python process, the scheduler      │",
  "                       └────────────┬─────────────────────────┘",
  "                                    │ every 60 s",
  "                                    │      every 15 min",
  "                                    │              every day @ 06:30",
  "                                    ▼",
  "    SMB ──► bulk_to_bronze.py    flatten_sensors    populate_gold",
  "            (file copy)          (bronze→silver)    --sensors",
  "                                                        ",
  "                                                       weather_download",
  "                                                       clean_weather",
  "                                                       populate_gold --weather",
  "                                                       run_knime_predictions",
  "                                                       cleanup_bronze",
]));

main.push(para([new PageBreak()]));

// ============= 2. PIPELINE DETAILS =============
main.push(h1("2 · Pipeline details"));

main.push(h2("2.1 Bronze ingestion"));

main.push(h3("Sensor JSONs (continuous)"));
main.push(p("ingestion/fast_flow/bulk_to_bronze.py runs every 60 seconds inside the watcher loop. Two operating modes:"));
main.push(bullet("Predictive mode (default): looks at the newest filename already in bronze, predicts the next expected filename based on timestamp + 1-minute increment, checks .exists() on the SMB share. Stops after 10 consecutive empty minutes. Roughly 5 ms per check."));
main.push(bullet("Full scan mode (--full flag, used at install + nightly): does os.scandir() on the entire SMB folder, sorts results, copies anything not yet in bronze."));
main.push(bullet("Skip list: reads storage\\processed.log (filenames already imported to silver and removed from bronze) so a full rescan does not re-copy them."));
main.push(bullet("Storage layout: storage\\bronze\\<apt>\\YYYY\\MM\\DD\\HH\\<filename>.json — partitioned by hour to keep folders small."));

main.push(h3("Weather CSVs (daily)"));
main.push(p("ingestion/slow_flow/weather_download.py runs once per day at 06:30 UTC inside the watcher's daily ML batch:"));
main.push(bullet("Connects to sFTP via paramiko."));
main.push(bullet("Lists remote *.csv, filters to ones not already present in bronze."));
main.push(bullet("Sequential download (sFTP servers tend to dislike parallel sessions from the same client) — but with a progress bar."));
main.push(bullet("Storage: storage\\bronze\\weather\\YYYY\\MM\\DD\\Pred_YYYY-MM-DD.csv."));

main.push(h2("2.2 Bronze → Silver"));

main.push(h3("Sensors"));
main.push(p("etl/bronze_to_silver/flatten_sensors.py — the heaviest hop, parallelised:"));
main.push(bullet("Discovery: full rglob over each apartment's bronze tree, diff against silver.etl_watermark to find new files."));
main.push(bullet("Parallel parsing: ProcessPoolExecutor(max_workers=8), each worker takes a batch of 2 000 files, parses JSON, normalises room names, applies outlier bounds (e.g., temperature_c ∈ [-20, 60]), returns rows."));
main.push(bullet("Bulk upsert: psycopg2.copy_expert into a TEMP TABLE _tmp_sensor_events, then INSERT INTO silver.sensor_events SELECT DISTINCT ON ... FROM _tmp_sensor_events ON CONFLICT (...) DO UPDATE. The DISTINCT ON dedupes within-batch."));
main.push(bullet("Watermark: INSERT INTO silver.etl_watermark VALUES %s ON CONFLICT DO NOTHING via psycopg2.execute_values."));
main.push(bullet("Aggressive bronze cleanup (default on; disable with KEEP_BRONZE=1): after the upsert + watermark commit, deletes the bronze JSONs and appends filenames to storage\\processed.log."));
main.push(callout("Performance",
  "~30 k rows/second on the worker's main-thread upsert. ~220 k files × 60 events/file = ~13 M rows in 10–15 minutes on the project VM.",
  TEAL));

main.push(h3("Weather"));
main.push(p("etl/bronze_to_silver/clean_weather.py — also parallelised, file-level:"));
main.push(bullet("Parallel files: ProcessPoolExecutor(max_workers=4), each worker processes one CSV end-to-end (read, clean, COPY + upsert). Each worker has its own SQLAlchemy engine."));
main.push(bullet("Cleaning: drop rows with bad timestamps, filter to WEATHER_MIN_YEAR, drop sentinel -99 999.0 values, flag outliers via per-measurement bounds."));
main.push(bullet("Bulk upsert: same COPY + INSERT FROM SELECT pattern as sensors, on unique key (timestamp, site, prediction, prediction_date, measurement)."));
main.push(bullet("Aggressive bronze cleanup: same as sensors — CSV deleted after silver insert, filename appended to processed.log."));

main.push(h2("2.3 Silver → Gold"));
main.push(p("etl/silver_to_gold/populate_gold.py orchestrates three steps in order:"));
main.push(numbered("populate_dimensions — refreshes dim_apartment, dim_room, dim_device, dim_date, dim_datetime, dim_tariff, dim_weather_site. Set-based SQL: INSERT INTO gold.dim_X ... SELECT FROM silver.X ON CONFLICT DO NOTHING/UPDATE. Anonymises apartment metadata (owner_user_id → NULL, building_name → 'Building <id>')."));
main.push(numbered("populate_sensors — refreshes the four minute-grain fact tables (fact_environment_minute, fact_energy_minute, fact_presence_minute, fact_device_health_day). Each is a single INSERT INTO gold.fact_X ... SELECT FROM silver.sensor_events ... GROUP BY ... ON CONFLICT DO UPDATE that pivots the long-format sensor_events into wide-format minute facts."));
main.push(numbered("populate_weather — refreshes fact_weather_hour from silver.weather_forecasts, aggregating multiple model runs per hour (median value across runs)."));
main.push(p("Materialised views (mv_energy_with_cost) are refreshed individually via REFRESH MATERIALIZED VIEW CONCURRENTLY (with non-concurrent fallback on first build of an empty MV)."));

main.push(h2("2.4 Gold → ML (KNIME)"));
main.push(p("scripts/run_knime_predictions.py invokes knime.exe in batch mode:"));
main.push(...codeBlock([
  "knime.exe -consoleLog -nosplash -reset",
  "  -application org.knime.product.KNIME_BATCH_APPLICATION",
  "  -workflowDir=<workspace>/<workflow>",
  "  -workflow.variable=db_user,<user>,String",
  "  -workflow.variable=db_pwd,<password>,String",
]));
main.push(p("Why -workflow.variable? KNIME explicitly forbids overwriting password fields via flow variables for security. So the workflows cannot bind a password flow variable directly to the PG Connector's password slot."));
main.push(callout("The trick (Variable to Credentials)",
  "Two String Configuration nodes accept their values from -workflow.variable=...,String (allowed for plain strings). A Variable to Credentials node packs them into a credential object internally. The PG Connectors then read from the credential by name (db), getting both user and password — KNIME's password-overwrite rule never triggers because no xpassword field is being overwritten from outside.",
  ORANGE));

main.push(p("Workflows shipped:"));
main.push(bullet("Motion_Prediction_Server.knwf — logistic regression, predicts motion probability 1 hour ahead per apartment / room. Latest run: 13 173 prediction rows."));
main.push(bullet("Consumption_Weather_Prediction_Server.knwf — linear regression, predicts consumption 24 hours ahead with weather as a feature. Latest run: 66 186 prediction rows."));

main.push(para([new PageBreak()]));

// ============= 3. DATA MODEL =============
main.push(h1("3 · Data model details"));

main.push(h2("3.1 Apartment + room dimensions"));
main.push(...codeBlock([
  "gold.dim_apartment",
  "  apartment_key   PK (surrogate)",
  "  apartment_id    \"jimmy\" / \"jeremie\"  (natural key from JSON filenames)",
  "  building_name   anonymized to \"Building 1\" etc.",
  "  occupant_name   first name only (see ADR 005)",
  "",
  "gold.dim_room",
  "  room_key        PK",
  "  room_name       normalized (e.g. \"Bdroom\" → \"Bedroom\")",
  "  apartment_key   FK",
]));

main.push(h2("3.2 Star schema for sensor facts"));
main.push(p("Every fact table shares the same dim spine:"));
main.push(...codeBlock([
  "fact_X_minute (",
  "    datetime_key    FK → dim_datetime    (1-minute grain)",
  "    date_key        FK → dim_date",
  "    room_key        FK → dim_room",
  "    apartment_key   FK → dim_apartment",
  "    device_key      FK → dim_device      (where applicable)",
  "    <measure cols>",
  "    is_valid / is_anomaly  flag",
  ")",
]));
main.push(p("Unique constraint on (datetime_key, room_key) (or device_key for energy) means upserts are idempotent."));

main.push(h2("3.3 Time dimensions"));
main.push(bullet("dim_date — one row per calendar day, with year / month / quarter / weekday columns. date_key = YYYYMMDD::int."));
main.push(bullet("dim_datetime — one row per minute, with timestamp_utc, hour, minute, is_business_hour. datetime_key = YYYYMMDDHHMM::bigint."));
main.push(p("Both dimensions are pre-generated for the entire range covered by the data (2023 onward)."));

main.push(h2("3.4 Predictions"));
main.push(...codeBlock([
  "gold.fact_prediction_motion",
  "  prediction_made_at TIMESTAMPTZ   when KNIME ran the model",
  "  target_at          TIMESTAMPTZ   what time the prediction is for (~1h ahead)",
  "  apartment_key      FK",
  "  room_key           FK",
  "  motion_prob        FLOAT",
  "  model_name         TEXT          (\"logistic_regression\")",
  "",
  "gold.fact_prediction_consumption",
  "  prediction_made_at TIMESTAMPTZ",
  "  target_at          TIMESTAMPTZ",
  "  apartment_key      FK",
  "  predicted_kwh      FLOAT",
  "  model_name         TEXT          (\"linear_regression\")",
]));
main.push(p("The prediction_made_at field lets you compare model versions or look at prediction drift over time."));

main.push(para([new PageBreak()]));

// ============= 4. CONFIGURATION =============
main.push(h1("4 · Configuration"));
main.push(p("All runtime configuration lives in .env, generated by the install wizard:"));
main.push(...codeBlock([
  "SMB_PATH=Z:\\",
  "BRONZE_ROOT=storage\\bronze",
  "DB_URL=postgresql://domotic:<pwd>@localhost:5432/<db_name>",
  "MYSQL_URL=mysql+pymysql://...",
  "SFTP_HOST=...",
  "SFTP_USER=...",
  "SFTP_PASSWORD=...",
  "SFTP_PATH=/forecasts",
  "WEATHER_MIN_YEAR=2023",
  "WEATHER_SITES=Aadorf / Tänikon",
  "PBI_SERVER=...",
  "PBI_DATABASE=...",
  "",
  "# Tunables (optional; defaults shown)",
  "GOLD_INTERVAL_MIN=15",
  "WEATHER_HOUR=6",
  "WEATHER_MIN=30",
  "KEEP_BRONZE=0           # 1 to disable aggressive bronze cleanup",
  "CLEAN_WEATHER_WORKERS=4",
  "BRONZE_RETENTION_DAYS=30",
  "KNIME_FREE_MEMORY=0     # 1 to auto-close memory hogs before KNIME runs",
]));
main.push(callout("Admin password",
  "The PostgreSQL admin password is never written to .env — it is used only at install time, in memory, to create the app DB and app user.",
  ACCENT));

// ============= 5. SECURITY =============
main.push(h1("5 · Security"));
main.push(tbl(
  ["Concern", "Mitigation"],
  [
    ["Postgres admin credentials on disk", "Used only at install time; never written to .env"],
    ["App user privileges", "domotic has only DML / DDL on silver and gold schemas; no superuser, no CREATE DATABASE"],
    ["Power BI data leakage", "RLS by apartment_key — tenants cannot see each other's data"],
    ["KNIME passwords baked in workflows", "Avoided — Variable to Credentials accepts password as String flow variable at runtime"],
    ["GDPR — personal data", "Only first names retained (Art. 4(1) considers them low-risk identifiers absent additional context). User IDs and building names anonymised in gold."],
    ["GDPR — right to erasure", "Supported: DELETE FROM gold.dim_apartment WHERE apartment_id = '...' cascades through apartment_key FKs"],
  ],
  [3000, 6360],
));
main.push(p("See ADR 005 (chapter 7) for the full GDPR analysis."));

// ============= 6. PERFORMANCE =============
main.push(h1("6 · Performance"));
main.push(p("Numbers measured on the project VM (8 cores, 32 GB RAM, local Postgres 17):"));
main.push(tbl(
  ["Phase", "Throughput / wall time"],
  [
    ["bulk_to_bronze (SMB → bronze)", "~150 files/sec, 16 parallel threads"],
    ["flatten_sensors (bronze → silver)", "~30 k rows/sec via COPY-based upsert"],
    ["clean_weather (bronze → silver)", "~15 files/min, 4 parallel processes"],
    ["populate_gold (silver → gold)", "~10 M rows in ~30 sec, set-based SQL"],
    ["KNIME Consumption_Weather_Prediction_Server", "~125 sec end-to-end, 66 186 rows produced"],
    ["KNIME Motion_Prediction_Server", "~770 sec end-to-end, 13 173 rows produced"],
    ["Full first-time install (empty Postgres → ML predictions in DB)", "45–60 minutes"],
  ],
  [4500, 4860],
));

main.push(h2("6.1 COPY into temp table — the silver upsert hot-path"));
main.push(p("The original flatten_sensors used INSERT ... VALUES (:a, :b, ...) ON CONFLICT DO UPDATE per row via SQLAlchemy. A 220 k-file backfill took ~3 hours, almost all of which was DB write time."));
main.push(p("Switched to: psycopg2.copy_expert streams rows into a TEMP TABLE ON COMMIT DROP, then a single INSERT INTO silver.sensor_events SELECT DISTINCT ON (...) ... FROM tmp_table ON CONFLICT DO UPDATE finishes the merge."));
main.push(bullet("COPY skips per-statement parsing — bytes go directly into the temp table."));
main.push(bullet("One INSERT ... SELECT ... ON CONFLICT is a single set-based operation, minimal overhead."));
main.push(bullet("DISTINCT ON dedupes within-batch (PostgreSQL forbids upserting the same key twice in one statement)."));
main.push(callout("Result", "3-hour backfill → 10–15 minutes (~50–150x speedup measured on the project VM).", TEAL));

main.push(h2("6.2 Idempotency guarantees"));
main.push(p("Every step is safe to re-run:"));
main.push(bullet("bulk_to_bronze: skips existing files in bronze."));
main.push(bullet("flatten_sensors: silver.etl_watermark skips already-processed files."));
main.push(bullet("clean_weather: silver.weather_watermark + processed.log do the same."));
main.push(bullet("populate_dimensions: INSERT ... ON CONFLICT DO NOTHING/UPDATE."));
main.push(bullet("populate_sensors / populate_weather: INSERT ... ON CONFLICT DO UPDATE."));
main.push(bullet("KNIME workflows: INSERT ... ON CONFLICT DO UPDATE on prediction tables."));
main.push(bullet("cleanup_bronze: only deletes files older than BRONZE_RETENTION_DAYS."));

main.push(para([new PageBreak()]));

// ============= 7. ADRs =============
main.push(h1("7 · Architecture decision records"));
main.push(p("Eight decisions documented along the way. Only choices where viable alternatives existed are recorded as ADRs — the medallion architecture itself was prescribed by the course's reference architecture (PDF chapter \"Architecture — Medaillon architecture\"), so it is described in chapter 1, not as an ADR. Likewise, the COPY-based silver upsert pattern is a performance optimisation captured in chapter 6.1, not a high-level architectural choice."));
main.push(p("Each ADR captures the context, the chosen path, the rationale, and the trade-offs accepted."));

const adrs = [
  {
    id: "ADR 001", title: "Custom Python watcher instead of Airflow",
    date: "2026-03",
    context: "We need to schedule a continuous bronze→silver loop (every minute), a periodic gold refresh (every 15 min), and a daily ML batch (weather + KNIME + cleanup). The textbook answer is Apache Airflow.",
    decision: "A single Python process (watcher.py) handles all scheduling. No Airflow.",
    rationale: "Airflow is heavy (web server, scheduler, executor, metadata DB) for a pipeline that has six jobs total. The 'for dummies' install goal means every dependency we add is a potential install-time failure. A 350-line script with three time conditions is easier to read, debug, and explain.",
    tradeoffs: "No web UI for DAGs (replaced by the Streamlit admin pane); no retry/SLA primitives (subprocess return codes + warning logs cover what we need); no DAG-level dependency expression (the watcher's three time triggers call functions in fixed order).",
  },
  {
    id: "ADR 002", title: "Aggressive bronze cleanup (delete after silver insert)",
    date: "2026-04",
    context: "The course's reference architecture (PDF chapter \"Architecture — Medaillon architecture\") describes the bronze layer as \"Stored incrementally, with all history.\" In our deployment, 2 apartments × 1 file/min × 365 days = 1 M JSON files/year/apt; ~10 KB per file → ~20 GB/year just for sensors. Bronze grows unbounded if we follow the reference exactly.",
    decision: "Deviate from the reference: after every successful silver insert + watermark commit, delete the bronze JSONs from disk (default on; opt-out with KEEP_BRONZE=1). Filenames are appended to storage/processed.log so future SMB rescans do not re-copy them.",
    rationale: "The original SMB share is the durable copy; bronze on the VM was always a staging area. Silver fully reconstructs the relevant data. The course-allocated VM has limited disk; bounded bronze is more important than the immutable-archive property.",
    tradeoffs: "If silver gets corrupted or dropped, full re-fetch from SMB is the only recovery (slower but possible). Less obvious raw audit trail — partly compensated by silver.etl_watermark which records every filename that landed.",
  },
  {
    id: "ADR 003", title: "KNIME credential injection via Variable to Credentials",
    date: "2026-04",
    context: "KNIME workflows need DB credentials at runtime, but those credentials live in .env per user. Hardcoding is unacceptable. Four KNIME mechanisms were tried unsuccessfully (inline password, Workflow Credentials + -credential, -workflow.variable directly into password, -option=NODE,credentials).",
    decision: "Use a Variable to Credentials node bridge: two String Configuration nodes (db_user, db_pwd) at workflow root → Variable to Credentials node → PG Connectors bind to credential 'db'. At runtime the Python wrapper passes -workflow.variable=db_user,...,String + -workflow.variable=db_pwd,...,String.",
    rationale: "KNIME blocks flow-variable overrides on xpassword fields specifically. Plain String flow variables are unrestricted, and the Variable to Credentials node builds the credential object internally before any password field is touched. KNIME's protection rule never triggers.",
    tradeoffs: "Workflows need a one-time GUI setup (documented in ml/knime/SETUP.md) that creates the three nodes + wiring. Re-exporting the .knwf must preserve the wiring.",
  },
  {
    id: "ADR 004", title: "GDPR — keep first names, anonymise everything else",
    date: "2026-04",
    context: "The dim_apartment table has owner_user_id, building_name, occupant_name, etc. Some are personal data under GDPR Art. 4(1).",
    decision: "In the gold.dim_apartment view exposed to BI / ML: occupant_name keeps first names (e.g. 'Jimmy', 'Jeremie'); owner_user_id is set to NULL; building_name is replaced with 'Building <id>'; sensor data keeps apartment_key as the only re-identifier. Power BI enforces RLS.",
    rationale: "A first name alone is generally not considered personal data under GDPR Art. 4(1) absent additional context. Stripping first names entirely would make the dashboard sterile. Building name + user IDs are direct identifiers; those are masked.",
    tradeoffs: "A determined attacker with side knowledge could re-identify 'Jimmy' as a specific person. Documented as a known limitation; production rollout would need a GDPR review with the school's DPO.",
  },
  {
    id: "ADR 005", title: "Two-role Postgres install (admin + app)",
    date: "2026-03",
    context: "Should we run everything as the Postgres superuser, or split?",
    decision: "Two roles — Admin (typically postgres) used ONLY at install time, in memory; App (domotic) has DML/DDL on silver and gold schemas only.",
    rationale: "Principle of least privilege — runtime should not have admin rights. Limits blast radius if .env leaks.",
    tradeoffs: "Slightly more complex install (one extra prompt for the admin password). Adding a new schema requires either the admin's password again, or granting domotic CREATE on the database.",
  },
  {
    id: "ADR 006", title: "Streamlit admin pane in addition to status.py CLI",
    date: "2026-04",
    context: "Operators need to check pipeline freshness, trigger ad-hoc ETL runs, read logs, see config. A CLI tool covers it for technical users; non-technical users prefer a GUI.",
    decision: "Build scripts/admin.py (Streamlit) for the GUI. Keep status.py (CLI) for terminal users. Both read the same data.",
    rationale: "Streamlit is one pip install away — no separate web server, no React build, no auth setup. Auto-refresh every 10 s gives near-live status. Buttons → subprocess.Popen with output redirected to log files — no in-process state to manage.",
    tradeoffs: "Streamlit adds ~50 MB to venv. No auth on the admin pane — but it binds to localhost only by default, so reaching it requires already being on the VM.",
  },
  {
    id: "ADR 007", title: "Power BI Desktop only (no Service)",
    date: "2026-04",
    context: "The natural deployment for view-only Power BI is Power BI Service (cloud). The HES-SO Microsoft 365 tenant restricts free Power BI signup, so individual students cannot publish to Service.",
    decision: "Ship the .pbix as a local Desktop file. Document F11 fullscreen as the 'view mode' approximation.",
    rationale: "The school requires Power BI specifically. The 'local-only' deployment requirement conflicts with Service's cloud nature anyway. Desktop in F11 is acceptable for an academic demo. RLS still enforces per-apartment data isolation at the model layer regardless of UI.",
    tradeoffs: "End users see editing chrome until they press F11. No automatic refresh — users hit Refresh manually or use the admin pane's 'Refresh Power BI' button (sends Ctrl+Shift+F5).",
  },
  {
    id: "ADR 008", title: "Self-contained installer + web wizard",
    date: "2026-04",
    context: "The 'for dummies' install goal means a non-technical user should run one command to deploy everything.",
    decision: "Two-step install: Web wizard (/install page) where the user fills in a form + Single Python file generated client-side with all values baked in; user downloads it and runs python data-cycle-installer.py. The wizard's JavaScript renders installer/install_template.py by replacing {{PLACEHOLDER}} tokens. Re-runs are idempotent.",
    rationale: "Zero credentials transit through any backend (form values never leave the browser). The .py file is auditable: user can read it before running. Re-installs are easy: keep the file, run again.",
    tradeoffs: "The user must trust the wizard / install_template — but the file is open source. The template lives in two places (installer/ and website/public/) and must be kept in sync.",
  },
];

for (const adr of adrs) {
  main.push(h2(`${adr.id} · ${adr.title}`));
  main.push(para([t("Date  ", { bold: true, size: 20, color: TEXT_DIM }),
                  t(adr.date, { size: 20 })], { spacing: { after: 80 } }));
  main.push(h3("Context"));
  main.push(p(adr.context));
  main.push(h3("Decision"));
  main.push(p(adr.decision));
  main.push(h3("Rationale"));
  main.push(p(adr.rationale));
  main.push(h3("Trade-offs accepted"));
  main.push(p(adr.tradeoffs));
}

main.push(para([new PageBreak()]));

// ============= 8. TECHNOLOGY STACK =============
main.push(h1("8 · Technology stack"));
main.push(tbl(
  ["Layer", "Tool", "Version", "Why"],
  [
    ["Language",          "Python",            "3.11",          "Single language across ingestion, ETL, ML wrappers, admin pane — easy hire/handoff"],
    ["Database",          "PostgreSQL",        "17",            "Strong COPY performance, JSON support for raw bronze, materialised views, free"],
    ["Async I/O",         "asyncio + aiohttp", "3.9",           "Used by recup.py for concurrent sensor polling"],
    ["MySQL client",      "aiomysql",          "0.2",           "Reads the school's pidb apartment metadata"],
    ["Postgres driver",   "psycopg2-binary",   "2.9",           "Wins over asyncpg here for COPY support"],
    ["ORM / DDL helper",  "SQLAlchemy",        "2.0",           "Used for schema bootstrap; bulk inserts go through psycopg2 directly"],
    ["DataFrames",        "pandas",            "2.1",           "Weather CSV cleaning"],
    ["sFTP",              "paramiko",          "3.4",           "Weather forecast download"],
    ["ML — classical",    "scikit-learn",      "1.3",           "Baseline models / Python-side experiments"],
    ["ML — workflow",     "KNIME Analytics Platform", "5.x batch", "Course requirement; PG Connectors built-in"],
    ["BI",                "Power BI Desktop",  "latest",        "Course requirement; RLS at model layer"],
    ["Admin UI",          "Streamlit",         "1.32",          "One-pip GUI for non-technical operators"],
    ["Config",            "python-dotenv",     "1.0",           "All runtime config in .env"],
    ["Test",              "pytest",            "7.4",           "Smoke tests for parsing + idempotency"],
  ],
  [1800, 2400, 1500, 3660],
));

main.push(para([new PageBreak()]));

// ============= 9. DATABASE SCHEMAS DETAIL =============
main.push(h1("9 · Database schemas — detailed"));

main.push(h2("9.1 Silver schema"));
main.push(tbl(
  ["Table", "Source", "Purpose"],
  [
    ["silver.sensor_events", "JSON files (apartments)", "Long-format events. One row per (apartment, room, sensor_type, field, timestamp)."],
    ["silver.weather_forecasts", "CSV files (sFTP)", "All forecast rows, one row per (timestamp, site, prediction-step, measurement)."],
    ["silver.dim_buildings", "MySQL buildings", "Apartment metadata mirrored from school DB (anonymised in gold)."],
    ["silver.dim_rooms / dim_devices / dim_sensors", "MySQL rooms / devices / sensors", "Physical asset inventory."],
    ["silver.log_sensor_errors", "MySQL DIErrors (raw)", "Untyped MySQL dump."],
    ["silver.di_errors_clean", "transform from log_sensor_errors", "Typed + apartment-mapped + severity heuristic. Joined into gold.fact_device_health_day."],
    ["silver.etl_watermark", "self", "Filenames already imported (sensor pipeline idempotency)."],
    ["silver.weather_watermark", "self", "Filenames already imported (weather pipeline idempotency)."],
  ],
  [3200, 2400, 3760],
));

main.push(h2("9.2 Gold star schema"));
main.push(p("Conformed dimensions:"));
main.push(bullet("dim_apartment — apartment_key (PK), apartment_id, building_name (anon), occupant_name (first name only)"));
main.push(bullet("dim_room — room_key, room_name, apartment_key (FK)"));
main.push(bullet("dim_device — device_key, device_id, sensor_type, room_key, apartment_key"));
main.push(bullet("dim_date — date_key (YYYYMMDD), year, month, quarter, weekday"));
main.push(bullet("dim_datetime — datetime_key (YYYYMMDDHHMM), timestamp_utc, hour, minute, is_business_hour"));
main.push(bullet("dim_tariff — electricity rate per hour-of-day (used by mv_energy_with_cost)"));
main.push(bullet("dim_weather_site — weather_site_key, site_name (e.g. \"Aadorf / Tänikon\")"));

main.push(p("Fact tables:"));
main.push(bullet("fact_environment_minute — temperature, humidity, CO₂, noise, pressure, anomaly flag"));
main.push(bullet("fact_energy_minute — power_w, energy_kwh, is_valid"));
main.push(bullet("fact_presence_minute — motion_flag, door_open_flag, window_open_flag"));
main.push(bullet("fact_device_health_day — error_count, missing_readings, uptime_pct, battery_min/avg"));
main.push(bullet("fact_weather_hour — temperature, humidity, precipitation, radiation, n_model_runs"));
main.push(bullet("fact_prediction_motion — KNIME-written, motion probability per (apartment, room, target hour)"));
main.push(bullet("fact_prediction_consumption — KNIME-written, predicted kWh per apartment per future hour"));

main.push(h2("9.3 Materialised views"));
main.push(bullet("mv_energy_with_cost — joins fact_energy_minute with dim_tariff to surface CHF cost per minute. Refreshed by populate_gold."));

main.push(para([new PageBreak()]));

// ============= 10. SCRIPTS REFERENCE =============
main.push(h1("10 · Scripts reference"));
main.push(p("Every script entry-point in the repo, with what it does and how to call it:"));

main.push(h2("10.1 Ingestion"));
main.push(tbl(
  ["Script", "What it does", "Run"],
  [
    ["ingestion/fast_flow/watcher.py", "Long-running scheduler — bronze→silver every minute, gold every 15 min, daily ML batch at 06:30", "python ingestion/fast_flow/watcher.py [--scan|--weather]"],
    ["ingestion/fast_flow/bulk_to_bronze.py", "SMB share → local bronze. Predictive (default) or full-scan mode.", "python ingestion/fast_flow/bulk_to_bronze.py [--full]"],
    ["ingestion/slow_flow/weather_download.py", "sFTP → bronze. Sequential download with progress bar.", "python ingestion/slow_flow/weather_download.py"],
  ],
  [3500, 4200, 1660],
));

main.push(h2("10.2 ETL"));
main.push(tbl(
  ["Script", "What it does", "Run"],
  [
    ["etl/bronze_to_silver/create_silver.py", "DDL — creates silver schema + tables", "python -m etl.bronze_to_silver.create_silver"],
    ["etl/bronze_to_silver/flatten_sensors.py", "JSON → silver.sensor_events (parallel + COPY upsert)", "python -m etl.bronze_to_silver.flatten_sensors"],
    ["etl/bronze_to_silver/clean_weather.py", "CSV → silver.weather_forecasts (4 parallel workers)", "python -m etl.bronze_to_silver.clean_weather"],
    ["etl/bronze_to_silver/import_mysql_to_silver.py", "MySQL dim tables → silver + DIErrors transform", "python -m etl.bronze_to_silver.import_mysql_to_silver"],
    ["etl/silver_to_gold/create_gold.py", "DDL — creates gold star schema", "python -m etl.silver_to_gold.create_gold"],
    ["etl/silver_to_gold/populate_gold.py", "Orchestrator: dimensions + sensors + weather", "python -m etl.silver_to_gold.populate_gold [--sensors|--weather]"],
  ],
  [3500, 4200, 1660],
));

main.push(h2("10.3 ML / BI / Admin"));
main.push(tbl(
  ["Script", "What it does", "Run"],
  [
    ["scripts/run_knime_predictions.py", "Invokes knime.exe in batch with credential injection", "python scripts/run_knime_predictions.py [motion|consumption]"],
    ["scripts/configure_bi_knime.py", "Patches host/port/db in .pbix and .knwf at install", "python scripts/configure_bi_knime.py"],
    ["scripts/deploy_knime.py", "Extracts .knwf into ~/knime-workspace", "python scripts/deploy_knime.py"],
    ["scripts/cleanup_bronze.py", "Daily retention pass (delete files older than BRONZE_RETENTION_DAYS)", "python scripts/cleanup_bronze.py"],
    ["scripts/admin.py", "Streamlit admin pane", "streamlit run scripts/admin.py (or admin.bat)"],
    ["scripts/status.py", "CLI version of admin pane", "python scripts/status.py"],
  ],
  [3500, 4200, 1660],
));

main.push(para([new PageBreak()]));

// ============= 11. DEPLOYMENT =============
main.push(h1("11 · Deployment"));

main.push(h2("11.1 Web wizard"));
main.push(p("The website's /install page presents a form. The user fills:"));
main.push(bullet("Postgres admin credentials (used only at install time, never written)"));
main.push(bullet("App user / DB / host / port"));
main.push(bullet("MySQL connection string"));
main.push(bullet("sFTP host / user / password"));
main.push(bullet("SMB share / drive letter / credentials"));
main.push(bullet("Bronze root path"));
main.push(p("On submit, JavaScript renders installer/install_template.py with all values baked in, prompts a download (data-cycle-installer.py). No credentials touch any backend."));

main.push(h2("11.2 The installer (10 steps)"));
main.push(tbl(
  ["#", "Step", "Time"],
  [
    ["1", "Prerequisites — check Python ≥ 3.11, git, Power BI, KNIME", "5 s"],
    ["2", "Clone repo (or git pull if existing)", "30-60 s"],
    ["3", "Write .env", "<1 s"],
    ["4", "Python venv + pip install requirements", "2-3 min"],
    ["5", "Pre-flight — mount SMB, validate Postgres / MySQL / sFTP", "5-10 s"],
    ["6", "Create app DB + user + silver/gold schemas", "10-30 s"],
    ["7", "Bootstrap silver — MySQL dims + (opt) SMB backfill + weather", "25-35 min"],
    ["8", "Initial gold ETL", "30-60 s"],
    ["9", "Verify + auto-config BI/KNIME (.pbix / .knwf host patching)", "30-60 s"],
    ["10", "Optional autostart watcher in shell:startup", "<1 s"],
  ],
  [400, 7200, 1760],
));

main.push(p("Total typical install on the project VM: 45-60 minutes. Idempotent — re-running picks up where it stopped."));

main.push(h2("11.3 Post-install"));
main.push(bullet("Admin dashboard auto-launches in browser at http://localhost:8501"));
main.push(bullet("Watcher registered in shell:startup → runs on every login"));
main.push(bullet("Power BI .pbix has host/port/db pre-patched to local Postgres"));
main.push(bullet("KNIME workflows extracted into ~/knime-workspace/, ready for batch invocation"));

main.push(para([new PageBreak()]));

// ============= 12. SERVICES & PROCESSES =============
main.push(h1("12 · Services & processes"));
main.push(p("Long-running and scheduled processes on the VM after install:"));
main.push(tbl(
  ["Process", "Schedule", "Notes"],
  [
    ["postgresql-x64-17", "Always running (Windows service)", "Listens on 5432; created by Postgres install, not by us"],
    ["watcher.py (pythonw.exe)", "Auto-start on user login (shell:startup .lnk)", "Single Python process; embeds the scheduler"],
    ["knime.exe (batch)", "Spawned by watcher daily at 06:30", "Two prediction workflows; ~5-15 min each"],
    ["streamlit run admin.py", "On-demand (admin.bat double-click or installer prompt)", "Localhost:8501; hand-launched"],
    ["bulk_to_bronze + flatten_sensors + clean_weather", "Subprocesses spawned by watcher", "Inherit watcher's process; finish in seconds (continuous) or minutes (backfill)"],
  ],
  [3000, 3000, 3360],
));

main.push(p("Stopping the watcher: Stop-Process -Name pythonw -Force, or kill via Task Manager. Re-enable autostart by restoring the shortcut in shell:startup."));

main.push(para([new PageBreak()]));

// ============= 13. MONITORING & LOGS =============
main.push(h1("13 · Monitoring & logs"));
main.push(tbl(
  ["File", "Producer", "Contents"],
  [
    ["install.log", "data-cycle-installer.py", "Every step of the install with [HH:MM:SS] timestamps"],
    ["logs/clean_weather.log", "clean_weather.py", "Persistent log handler; per-file processing details"],
    ["storage/admin_logs/*.log", "Streamlit admin pane buttons", "Output of every action triggered from the dashboard"],
    ["storage/processed.log", "flatten_sensors / clean_weather", "Filenames imported to silver and removed from bronze (skip-list)"],
    ["watcher stdout (terminal)", "watcher.py", "Live progress; not persisted by default"],
  ],
  [3500, 2500, 3360],
));
main.push(p("All logs are plain text — the admin pane's log viewer tails the last 300 lines with ANSI colour codes stripped. For long-term log retention, redirect watcher's output to a file (e.g. python watcher.py >> logs/watcher.log 2>&1)."));

main.push(callout("Health check at a glance",
  "Open the admin pane at http://localhost:8501. Six freshness tiles (sensors environment / energy / presence, weather forecasts, motion predictions, consumption predictions) — all green = healthy.",
  TEAL));

main.push(para([new PageBreak()]));

// ============= 14. TROUBLESHOOTING =============
main.push(h1("14 · Troubleshooting"));
main.push(tbl(
  ["Symptom", "Likely cause", "Fix"],
  [
    ["Database red on admin pane", "Postgres service stopped, wrong .env DB_URL, or password mismatch", "Start-Service postgresql-x64-17 / verify pg_isready / ALTER USER ... PASSWORD"],
    ["Sensor data >1 h stale", "Watcher not running, or SMB drive (Z:) unmounted", "Check Get-Process pythonw / Test-Path Z: / re-mount via net use"],
    ["KNIME exit code 4", "GUI open with same workspace OR JVM module-access flags overridden", "Close KNIME GUI, do NOT pass -vmargs from CLI (use knime.ini for heap)"],
    ["KNIME 'Attempt to overwrite password'", ".knwf was edited and lost the Variable to Credentials wiring", "Re-do the SETUP.md steps, re-export .knwf, re-deploy"],
    ["Bronze disk growing unbounded", "KEEP_BRONZE=1 set in .env or aggressive cleanup disabled", "Unset KEEP_BRONZE; or run cleanup_bronze.py for retention pass"],
    ["Gold tables empty after install", "Silver bootstrap was skipped or interrupted", "python -m etl.silver_to_gold.populate_gold from install dir"],
    ["Power BI 'Cannot connect'", ".pbix has stale host/port/password baked in", "Re-run scripts/configure_bi_knime.py and refresh .pbix"],
    ["JVM OOM on Motion workflow", "Default Xmx too high for VM, or page file too small", "Edit knime.ini -Xmx line; increase Windows virtual memory"],
  ],
  [2800, 2800, 3760],
));

main.push(para([new PageBreak()]));

// ============= 15. MAINTENANCE =============
main.push(h1("15 · Maintenance"));
main.push(p("The pipeline is largely self-maintaining; this is a maintainer's runbook for the once-a-month-if-ever tasks."));

main.push(h2("15.1 Daily check (30 seconds)"));
main.push(p("Open the admin dashboard. Healthy state = green database + green watcher + 6 green freshness tiles + every gold fact >0 rows."));

main.push(h2("15.2 Weekly check (5 minutes)"));
main.push(bullet("Bronze disk usage — should hover around 1-2 GB if aggressive cleanup is on; growing unbounded means cleanup is broken"));
main.push(bullet("Watcher uptime — Task Manager → pythonw.exe with watcher.py in the command line"));
main.push(bullet("storage/admin_logs/ — review any non-empty error logs"));
main.push(bullet("gold.fact_prediction_motion MAX(prediction_made_at) — should be within 24 hours"));

main.push(h2("15.3 Monthly maintenance (optional)"));
main.push(bullet("Postgres VACUUM ANALYZE silver.sensor_events / gold.fact_environment_minute — Postgres autovacuum usually keeps up; force once a month doesn't hurt"));
main.push(bullet("Disk-usage queries: SELECT pg_size_pretty(pg_database_size('domotic_tests'))"));
main.push(bullet("Re-run KNIME predictions if the workflow .knwf has been edited"));

main.push(h2("15.4 Adding a new apartment"));
main.push(numbered("School DB admin adds a row to MySQL apartment table"));
main.push(numbered("Wait for next gold ETL pass (or click \"Run gold ETL (sensors)\" in admin pane) — pulls the new row into gold.dim_apartment"));
main.push(numbered("Open .pbix in Power BI Desktop → Modeling → Manage roles → Create. Name e.g. \"Apartment3\", filter [apartment_key] = 3, save. RLS roles must be added manually — Power BI tooling has no programmatic API"));
main.push(numbered("Verify: Modeling → View as → Apartment3 → confirm only the new apartment's data shows"));

main.push(h2("15.5 Rotating the DB password"));
main.push(p("Change Postgres password:"));
main.push(...codeBlock([
  "ALTER USER domotic PASSWORD '<new pwd>';",
]));
main.push(p("Update .env DB_URL with the new password. Restart the watcher and any running streamlit. KNIME picks up the new password automatically (read from .env at runtime). Re-run scripts/configure_bi_knime.py to patch the .pbix."));

main.push(para([new PageBreak()]));

// ============= 16. GDPR & ETHICS =============
main.push(h1("16 · GDPR & ethics"));

main.push(h2("16.1 Personal data inventory"));
main.push(tbl(
  ["Data point", "Source", "GDPR status", "Treatment"],
  [
    ["First name (occupant)",       "MySQL.buildings.firstName",  "Not personal data alone (Art. 4(1))",                        "Kept for usability"],
    ["Last name",                   "MySQL.buildings.lastName",   "Personal data",                                              "Anonymised — not exposed in gold"],
    ["Email / phone",               "MySQL.buildings",             "Personal data",                                              "Not imported"],
    ["Building name",               "MySQL.buildings.houseName",   "Quasi-identifier",                                           "Replaced with \"Building <id>\" in gold"],
    ["Apartment key (surrogate)",   "Generated",                   "Pseudonym (Art. 4(5))",                                      "Kept; only re-identifier in fact tables"],
    ["GPS coordinates",             "MySQL.buildings.lat/lng",     "Personal data (precise location)",                           "Not imported"],
    ["Postal address",              "MySQL.buildings.address/npa", "Personal data",                                              "Not imported"],
    ["Sensor readings",             "JSON files",                  "Personal data when linkable to identified person",          "Aggregated to room-minute facts; tenant sees only own via RLS"],
  ],
  [2400, 2400, 2400, 2160],
));

main.push(h2("16.2 Lawful basis"));
main.push(p("For the academic deployment, processing is justified under Art. 6(1)(a) (consent — the school provides the data to the project as part of registered student work) and Art. 6(1)(f) (legitimate interest — analysing one's own apartment's data)."));

main.push(h2("16.3 Data subject rights"));
main.push(bullet("Right of access (Art. 15) — RLS gives each tenant access to their own data only via Power BI"));
main.push(bullet("Right to erasure (Art. 17) — DELETE FROM gold.dim_apartment WHERE apartment_id = '...' cascades through apartment_key FKs in all fact tables"));
main.push(bullet("Right to data portability (Art. 20) — silver tables can be exported as CSV/parquet on request"));

main.push(h2("16.4 Risk assessment"));
main.push(callout("Residual risk",
  "A determined attacker with side knowledge (e.g. who knows that Jimmy lives on the second floor of building X) could re-identify a tenant from the dashboards. For an academic demo with two consenting subjects, this is acceptable. A production rollout to >10 tenants would require pseudonymising first names too and a formal DPIA.",
  ORANGE));

main.push(para([new PageBreak()]));

// ============= 17. SCALABILITY =============
main.push(h1("17 · Scalability"));

main.push(h2("17.1 Current limits (single-VM deployment)"));
main.push(tbl(
  ["Component", "Threshold", "Symptom", "Mitigation"],
  [
    ["Single PostgreSQL node", "~50-100 apartments", "INSERT throughput drops, query plans degrade", "Native partitioning by date_key + tune work_mem / shared_buffers"],
    ["Single bronze disk", "Months of accumulation on one VM disk", "Disk fills, slow scans", "cleanup_bronze.py keeps it bounded; long-term move to S3-compatible blob"],
    ["Watcher (single process)", "~500 files/min", "Falls behind on the 1-min loop", "Watcher per apartment cluster, or move to Kafka/Redis Streams"],
    ["Power BI Direct Query", "Many concurrent users", "Slow refreshes, dashboard timeouts", "Switch to Import + scheduled refresh; consider Power BI Premium / read replica"],
    ["MySQL source DB", "Polling many tables", "Read load on school DB", "CDC (Debezium) or batch sync at off-peak"],
  ],
  [2800, 1800, 2800, 1960],
));

main.push(h2("17.2 Bronze lifecycle"));
main.push(p("On a single-VM deployment without object storage, bronze accumulates roughly 5 GB per apartment per year. cleanup_bronze.py is the pragmatic mitigation:"));
main.push(numbered("Reads silver watermarks (silver.etl_watermark, silver.weather_watermark) — same tables the ETL uses to skip already-processed files."));
main.push(numbered("For each filename in the watermarks where processed_at is older than BRONZE_RETENTION_DAYS (default 30, configurable in .env), deletes the bronze file."));
main.push(numbered("Cleans up empty folders left behind."));
main.push(numbered("Set BRONZE_RETENTION_DAYS=-1 to disable cleanup entirely (keep bronze forever)."));
main.push(callout("Trade-off",
  "Bronze becomes a bounded buffer (last N days) instead of an immutable archive. You lose the ability to re-derive silver from bronze for files older than the retention window. The original source data (SMB share, sFTP server, MySQL) is still available for full reprocessing.",
  ORANGE));

main.push(h2("17.3 Recommended evolution path"));
main.push(numbered("0-1 year — current single-VM setup, scales to 5-10 apartments easily"));
main.push(numbered("1-3 years — split watcher per apartment cluster, move bronze to blob storage (e.g. MinIO on-prem)"));
main.push(numbered("3+ years — Kafka or similar between sources and silver, partitioned PG tables, read replica for BI"));

main.push(para([new PageBreak()]));

// ============= 18. PROJECT LAYOUT =============
main.push(h1("18 · Project layout"));
main.push(p("Top-level directories of the data-cycle-domotic repository:"));
main.push(...codeBlock([
  "data-cycle-domotic/",
  "├── ingestion/",
  "│   ├── fast_flow/",
  "│   │   ├── watcher.py             # main scheduler / event loop",
  "│   │   └── bulk_to_bronze.py      # SMB → bronze",
  "│   └── slow_flow/",
  "│       └── weather_download.py    # sFTP → bronze",
  "├── etl/",
  "│   ├── bronze_to_silver/",
  "│   │   ├── flatten_sensors.py     # JSON → silver.sensor_events (COPY upsert)",
  "│   │   ├── clean_weather.py       # CSV → silver.weather_forecasts (parallel)",
  "│   │   ├── import_mysql_to_silver.py",
  "│   │   └── create_silver.py       # DDL for silver tables",
  "│   └── silver_to_gold/",
  "│       ├── create_gold.py         # DDL for gold star schema",
  "│       ├── populate_gold.py       # orchestrator",
  "│       ├── populate_dimensions.py",
  "│       ├── populate_sensors.py",
  "│       └── populate_weather.py",
  "├── ml/",
  "│   └── knime/",
  "│       ├── Motion_Prediction_Server.knwf",
  "│       └── Consumption_Weather_Prediction_Server.knwf",
  "├── bi/",
  "│   └── power_bi/",
  "│       └── DataCycleDomotic.pbix  # Power BI report with RLS",
  "├── scripts/",
  "│   ├── admin.py                   # Streamlit admin pane",
  "│   ├── admin.bat                  # one-click launcher",
  "│   ├── status.py                  # CLI version",
  "│   ├── configure_bi_knime.py      # patches host/port/db at install time",
  "│   ├── deploy_knime.py            # extracts .knwf into KNIME workspace",
  "│   ├── run_knime_predictions.py   # invokes knime.exe batch",
  "│   └── cleanup_bronze.py          # daily retention pass",
  "├── installer/",
  "│   └── install_template.py        # generates data-cycle-installer.py via wizard",
  "├── website/                       # git submodule, the install wizard",
  "└── docs/",
]));

main.push(para([new PageBreak()]));

// ============= 19. AI TOOLS USAGE =============
main.push(h1("19 · AI Tools Usage"));
main.push(p("Generative AI was used as a drafting and pair-programming aid during the project. No AI tool is considered an author. All architecture decisions, KNIME workflow design, Power BI dashboards (including Row-Level Security), VM deployment, and end-to-end testing were performed by the author."));

main.push(h2("19.1 Tools Used"));
main.push(tbl(
  ["AI Tool", "Provider"],
  [
    ["Claude Sonnet 4.6 (via Claude Code CLI)", "Anthropic"],
  ],
  [4680, 4680],
));

main.push(h2("19.2 Scope of Assistance"));
main.push(tbl(
  ["Area", "How AI Was Used"],
  [
    ["Documentation",      "Drafted sections from the author's notes; edited by the author"],
    ["Python boilerplate", "Initial drafts for ETL scripts, watcher loop, admin pane scaffolding"],
    ["Debugging",          "Suggested fixes during the KNIME credential-injection investigation"],
    ["Language",           "Grammar and consistency passes"],
  ],
  [3000, 6360],
));

main.push(h2("19.3 Manual Verification"));
main.push(p("Every script was executed on the project VM and verified against expected outputs. The 79 359 ML prediction rows in gold.fact_prediction_* (66 186 consumption + 13 173 motion), the working install wizard, and the live KNIME workflows are direct evidence of the author's testing and integration work."));

main.push(h2("19.4 Accountability Statement"));
main.push(p("The author retains full responsibility for the content of this report and for the design, implementation, and validation of the DataCycle Domotic platform. AI tools were used exclusively as support; all AI-assisted outputs were reviewed, corrected, and edited where necessary."));

main.push(para([new PageBreak()]));
main.push(p("— end of document —", { alignment: AlignmentType.CENTER }));

// ============= ADD MAIN TO SECTIONS =============
sections.push({
  properties: {
    page: {
      size: { width: 12240, height: 15840 },
      margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 },
    },
    type: "nextPage",
  },
  headers: {
    default: new Header({
      children: [para([
        t("DataCycle Domotic · Technical Documentation · v1.0", { size: 18, color: TEXT_DIM }),
      ], { alignment: AlignmentType.RIGHT })],
    }),
  },
  footers: {
    default: new Footer({
      children: [para([
        t("Group 14 — HES-SO Valais · Spring 2026", { size: 18, color: TEXT_DIM }),
        t("\t", { size: 18 }),
        t("Page ", { size: 18, color: TEXT_DIM }),
        new TextRun({ size: 18, color: TEXT_DIM, children: [PageNumber.CURRENT] }),
      ], {
        tabStops: [{ type: TabStopType.RIGHT, position: TabStopPosition.MAX }],
      })],
    }),
  },
  children: main,
});

// ============= BUILD THE DOC =============
const doc = new Document({
  creator: "Group 14",
  title: "DataCycle Domotic — Technical Documentation",
  description: "End-to-end medallion ETL pipeline for smart-apartment IoT data.",
  styles: {
    default: { document: { run: { font: "Calibri", size: 22 } } },
    paragraphStyles: [
      {
        id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 36, bold: true, color: "111827", font: "Calibri" },
        paragraph: { spacing: { before: 480, after: 240 }, outlineLevel: 0 },
      },
      {
        id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 28, bold: true, color: ACCENT, font: "Calibri" },
        paragraph: { spacing: { before: 360, after: 180 }, outlineLevel: 1 },
      },
      {
        id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 24, bold: true, color: "111827", font: "Calibri" },
        paragraph: { spacing: { before: 280, after: 140 }, outlineLevel: 2 },
      },
    ],
  },
  numbering: {
    config: [
      {
        reference: "bullets",
        levels: [{
          level: 0, format: LevelFormat.BULLET, text: "•",
          alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 720, hanging: 360 } } },
        }],
      },
      {
        reference: "numbers",
        levels: [{
          level: 0, format: LevelFormat.DECIMAL, text: "%1.",
          alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 720, hanging: 360 } } },
        }],
      },
    ],
  },
  sections,
});

// ============= WRITE =============
const primary = "docs/v2/out/DataCycle_Technical_Documentation.docx";
Packer.toBuffer(doc).then(buf => {
  let target = primary;
  try {
    fs.writeFileSync(primary, buf);
  } catch (e) {
    if (e.code === "EBUSY" || e.code === "EPERM") {
      // File open in Word — fall back to a timestamped sibling so we don't crash
      const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 16);
      target = primary.replace(".docx", `_${stamp}.docx`);
      fs.writeFileSync(target, buf);
      console.warn(`⚠ Primary file was locked; wrote ${target} instead. Close Word and re-run to overwrite the original.`);
    } else {
      throw e;
    }
  }
  console.log(`✓ Wrote ${target} (${(buf.length / 1024).toFixed(0)} KB)`);
});
