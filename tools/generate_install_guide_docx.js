// Generate the Installation Guide as a polished .docx file with screenshots
// embedded from docs/v2/out/*.png. Audience: a non-technical end user / IT
// person who has just downloaded data-cycle-installer.py from the wizard.
//
// Run with: node tools/generate_install_guide_docx.js

const fs = require("fs");
const path = require("path");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell, ImageRun,
  Header, Footer, AlignmentType, PageOrientation, LevelFormat,
  TabStopType, TabStopPosition,
  HeadingLevel, BorderStyle, WidthType, ShadingType,
  PageNumber, PageBreak,
} = require("docx");

const ACCENT  = "2E5BFF";
const TEAL    = "0F766E";
const ORANGE  = "C2410C";
const GREY_BG = "F3F4F6";
const GREY_LINE = "D1D5DB";
const TEXT_DIM  = "6B7280";

const border = (color = GREY_LINE) => ({ style: BorderStyle.SINGLE, size: 1, color });
const allBorders = (color = GREY_LINE) => ({
  top: border(color), bottom: border(color), left: border(color), right: border(color),
});
const noBorder = { style: BorderStyle.NONE, size: 0, color: "FFFFFF" };

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

const cell = (text, opts = {}) => new TableCell({
  borders: allBorders(opts.border || GREY_LINE),
  width: { size: opts.width || 2000, type: WidthType.DXA },
  shading: opts.fill ? { fill: opts.fill, type: ShadingType.CLEAR } : undefined,
  margins: { top: 100, bottom: 100, left: 140, right: 140 },
  children: [new Paragraph({ children: [t(text, { size: 20, bold: opts.bold })] })],
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

// ── Screenshots ────────────────────────────────────────────────────────────
const SCREENSHOTS_DIR = "docs/v2/out";
const screenshotFiles = fs.existsSync(SCREENSHOTS_DIR)
  ? fs.readdirSync(SCREENSHOTS_DIR)
      .filter(f => f.toLowerCase().endsWith(".png"))
      .sort((a, b) => {
        // Sort by mtime so they appear in the order the user took them
        const sa = fs.statSync(path.join(SCREENSHOTS_DIR, a)).mtimeMs;
        const sb = fs.statSync(path.join(SCREENSHOTS_DIR, b)).mtimeMs;
        return sa - sb;
      })
  : [];

const figure = (filename, caption) => {
  const data = fs.readFileSync(path.join(SCREENSHOTS_DIR, filename));
  // Calibrate display size: aim for 6 inch wide max, preserve aspect from a
  // sensible default. docx-js needs explicit width/height.
  const widthDXA  = 5760;  // 4 inches in EMU-ish units (sized for portrait page)
  const heightDXA = 3240;  // ~2.25 inch — rough 16:9 for a typical screenshot
  return [
    new Paragraph({
      alignment: AlignmentType.CENTER,
      spacing: { before: 200, after: 80 },
      children: [new ImageRun({
        type: "png",
        data,
        transformation: { width: 480, height: 270 },
        altText: { title: caption || filename, description: caption || filename, name: filename },
      })],
    }),
    para([t(caption || filename, { italics: true, size: 18, color: TEXT_DIM })], {
      alignment: AlignmentType.CENTER,
      spacing: { after: 240 },
    }),
  ];
};

// ── Document body ──────────────────────────────────────────────────────────
const sections = [];

// COVER
sections.push({
  properties: {
    page: { size: { width: 12240, height: 15840 },
            margin: { top: 2880, right: 1440, bottom: 1440, left: 1440 } },
    type: "nextPage",
  },
  children: [
    para([t("DataCycle Domotic", { size: 72, bold: true, color: "111827" })],
      { alignment: AlignmentType.CENTER, spacing: { after: 200 } }),
    para([t("Smart-Apartment IoT Data Platform", { size: 36, color: TEXT_DIM })],
      { alignment: AlignmentType.CENTER, spacing: { after: 1200 } }),
    para([t("Installation Guide", { size: 48, bold: true, color: ACCENT })],
      { alignment: AlignmentType.CENTER, spacing: { after: 600 } }),
    para([t("Step-by-step instructions to deploy DataCycle on a fresh Windows VM,", { size: 24 })],
      { alignment: AlignmentType.CENTER, spacing: { after: 80 } }),
    para([t("from web wizard to running pipeline.", { size: 24 })],
      { alignment: AlignmentType.CENTER, spacing: { after: 1600 } }),
    new Table({
      width: { size: 6000, type: WidthType.DXA },
      columnWidths: [2400, 3600],
      alignment: AlignmentType.CENTER,
      rows: [
        row([cell("Audience", { bold: true, width: 2400, fill: GREY_BG }),
             cell("End user / IT installer (no Python required)", { width: 3600 })]),
        row([cell("Estimated time", { bold: true, width: 2400, fill: GREY_BG }),
             cell("45-60 minutes", { width: 3600 })]),
        row([cell("Document version", { bold: true, width: 2400, fill: GREY_BG }),
             cell("1.0", { width: 3600 })]),
        row([cell("Document date", { bold: true, width: 2400, fill: GREY_BG }),
             cell("April 2026", { width: 3600 })]),
      ],
    }),
    para([new PageBreak()]),
  ],
});

const main = [];

main.push(h1("Before you start"));
main.push(p("DataCycle is a self-contained data platform that runs entirely on a single Windows VM. The installer is a single Python file — fill out a form on the project's web wizard, download a .py, run it, done."));

main.push(h2("Prerequisites"));
main.push(p("These need to be installed on the target machine before you run the installer:"));
main.push(tbl(
  ["Tool", "Why", "Where"],
  [
    ["Python 3.11 or newer", "Pipeline + installer", "https://www.python.org/downloads/"],
    ["Git", "Repo cloning during install", "https://git-scm.com/downloads"],
    ["PostgreSQL 14 or newer", "Silver + Gold storage", "https://www.postgresql.org/download/windows/"],
    ["Power BI Desktop", "Dashboards (Windows-only)", "https://www.microsoft.com/en-us/download/details.aspx?id=58494"],
    ["KNIME Analytics Platform 5.x", "ML predictions", "https://www.knime.com/downloads"],
  ],
  [2400, 3000, 3960],
));
main.push(p("The installer auto-detects all five and warns if anything is missing — but won't proceed until at least Python and Git are installed."));

main.push(h2("What you'll need to type into the wizard"));
main.push(bullet("Postgres ADMIN credentials (typically the postgres user) — used only at install time, never written to disk"));
main.push(bullet("App user name + password (defaults to domotic) — created during install, used by all pipeline scripts"));
main.push(bullet("App database name (e.g. domotic_prod) — created during install"));
main.push(bullet("Postgres host + port (typically localhost:5432)"));
main.push(bullet("MySQL connection details (provided by school: user / password / host / database)"));
main.push(bullet("sFTP credentials for the weather forecasts (provided by school)"));
main.push(bullet("SMB share UNC path (e.g. \\\\\\\\server\\\\share), credentials, and a free drive letter (Z:)"));
main.push(bullet("Bronze storage root (default: storage\\bronze relative to install folder)"));

main.push(callout("Privacy note",
  "Form values never touch any backend. The wizard runs entirely in your browser; on submit, JavaScript renders the installer template locally and triggers a download. The Postgres admin password is baked into the .py file you download — if you re-share that file, the admin password travels with it.",
  ACCENT));

main.push(para([new PageBreak()]));

main.push(h1("Step 1 — Generate the installer"));
main.push(numbered("Open the project's install wizard URL in your browser (e.g. https://datacycledomotic.vercel.app/install)"));
main.push(numbered("Fill in every field. Tip: tooltips on each field explain valid formats"));
main.push(numbered("Click \"Generate installer\" — your browser downloads data-cycle-installer.py"));
main.push(numbered("Move the file to a stable location, e.g. C:\\DEV\\users\\Install\\"));
main.push(p("If you've installed before, the wizard offers a \"Restore from previous installer\" upload. Drop your old .py file in to pre-fill the form."));

main.push(para([new PageBreak()]));

main.push(h1("Step 2 — Run the installer"));
main.push(p("Open PowerShell, cd to the folder containing data-cycle-installer.py, and run:"));
main.push(...codeBlock([
  "cd C:\\DEV\\users\\Install",
  "python data-cycle-installer.py",
]));
main.push(p("By default the installer creates a sub-folder data-cycle-domotic/ next to the .py. Pass a path to override:"));
main.push(...codeBlock([
  "python data-cycle-installer.py D:\\Projects\\DataCycle",
]));

main.push(h2("What it does (10 steps)"));
main.push(tbl(
  ["#", "Step", "Time"],
  [
    ["1", "Prerequisites — verify Python, Git, Power BI, KNIME presence", "5 s"],
    ["2", "Clone repo (or git pull if the dir already exists)", "30-60 s"],
    ["3", "Write .env with all your wizard inputs", "<1 s"],
    ["4", "Create Python venv + install requirements.txt", "2-3 min"],
    ["5", "Mount SMB drive + validate Postgres / MySQL / sFTP credentials", "5-10 s"],
    ["6", "Create Postgres app user, app database, silver/gold schemas", "10-30 s"],
    ["7", "Bootstrap silver — MySQL dim import + (opt) full SMB backfill + weather", "25-35 min"],
    ["8", "Run initial gold ETL", "30-60 s"],
    ["9", "Verify row counts + auto-config Power BI / KNIME with your DB credentials", "30-60 s"],
    ["10", "Optional autostart watcher in Windows Startup folder", "<1 s"],
  ],
  [400, 7200, 1760],
));

main.push(callout("Idempotent",
  "Re-running the installer is always safe. It skips clone if already cloned, skips dependency install if venv intact, skips DB creation if already created, and the bronze→silver step uses watermarks to skip already-processed files. So if anything fails halfway, just re-run.",
  TEAL));

main.push(para([new PageBreak()]));

main.push(h1("Step 3 — Verify everything works"));

main.push(h2("3.1 The auto-launched admin dashboard"));
main.push(p("At the end of the install, you're prompted: \"Launch the admin dashboard now? [Y/n]\". Hit Y. The Streamlit dashboard opens in your browser at http://localhost:8501."));
main.push(p("Healthy state:"));
main.push(bullet("🟢 Database — connection green"));
main.push(bullet("🟢 Watcher process — Running"));
main.push(bullet("Six freshness tiles all green (Sensors environment, energy, presence; Weather; Predictions motion + consumption)"));
main.push(bullet("Every gold table has > 0 rows"));
main.push(p("If anything is yellow / red, the dashboard's \"Quick actions\" buttons let you trigger the matching ETL step (e.g. \"Run gold ETL (sensors)\")."));

main.push(h2("3.2 Power BI"));
main.push(p("Open the dashboard:"));
main.push(...codeBlock([
  "start <install-dir>\\bi\\power_bi\\DataCycleDomotic.pbix",
]));
main.push(numbered("Click Refresh (Home tab) — pulls latest data from your local Postgres"));
main.push(numbered("Press F11 to enter fullscreen presentation mode"));
main.push(numbered("To preview tenant-only views: Modeling tab → View as → Other user → Jimmy or Jeremie"));

main.push(h2("3.3 KNIME predictions (optional, takes 5-15 minutes)"));
main.push(p("If you accepted the install's offer to run KNIME predictions, they'll already be in gold.fact_prediction_motion and gold.fact_prediction_consumption. Otherwise:"));
main.push(...codeBlock([
  "cd <install-dir>",
  ".venv\\Scripts\\python.exe scripts\\run_knime_predictions.py",
]));
main.push(p("Or click \"Run KNIME predictions\" in the admin pane."));

main.push(para([new PageBreak()]));

// ============= APPENDIX: SCREENSHOTS =============
if (screenshotFiles.length) {
  main.push(h1("Appendix · Screenshots"));
  main.push(p("Captured during the reference install on the project VM. Use these as a visual cross-reference while running the installer on a new machine."));
  for (let i = 0; i < screenshotFiles.length; i++) {
    const f = screenshotFiles[i];
    const caption = `Figure ${i + 1} — ${f.replace(/^\{/, "").replace(/\}\.png$/i, "").substring(0, 16)}…`;
    main.push(...figure(f, caption));
  }
}

main.push(para([new PageBreak()]));

main.push(h1("Common install issues"));
main.push(tbl(
  ["Symptom", "Fix"],
  [
    ["psycopg2.OperationalError: password authentication failed for user 'postgres'", "Wrong admin password in the wizard form. Re-run wizard, fix it, re-run installer."],
    ["Cannot connect to MySQL", "School VPN required — connect to VPN first."],
    ["SMB path not found: Z:\\", "Mount failed — installer prints the net use command it tried; run it manually with the correct credentials, then re-run installer."],
    ["Silver step says \"0 new files\" but bronze has data", "Old bug — pull latest, re-run. The watermark scanner now does a full scan each time."],
    ["KNIME predictions fail with \"Attempt to overwrite the password\"", "Old .knwf shipped before the Variable-to-Credentials swap. Pull latest, re-run configure_bi_knime.py then deploy_knime.py."],
    ["Admin dashboard fails with \"DB_URL not set\"", ".env empty or missing. Re-run installer (idempotent — won't redo finished work)."],
    ["Java exit code=4 on KNIME", "KNIME GUI is open with the same workspace. Close it (Stop-Process -Name knime -Force) and retry."],
  ],
  [4500, 4860],
));

main.push(para([new PageBreak()]));

main.push(h1("Uninstall / clean reset"));
main.push(p("To completely remove DataCycle from a machine:"));
main.push(...codeBlock([
  "# Stop running pipelines",
  "Get-Process knime,java,javaw,python,pythonw -EA SilentlyContinue | Stop-Process -Force",
  "",
  "# Remove the watcher autostart shortcut",
  "Remove-Item \"$env:APPDATA\\Microsoft\\Windows\\Start Menu\\Programs\\Startup\\DataCycle Watcher.lnk\" -EA SilentlyContinue",
  "",
  "# Drop DBs + user (use admin password)",
  "$env:PGPASSWORD = '<admin pwd>'",
  "psql -U postgres -h localhost -c \"DROP DATABASE IF EXISTS <your_db_name>;\"",
  "psql -U postgres -h localhost -c \"DROP USER IF EXISTS domotic;\"",
  "Remove-Item Env:\\PGPASSWORD",
  "",
  "# Remove the install dir + KNIME workspace",
  "Remove-Item C:\\path\\to\\data-cycle-domotic -Recurse -Force",
  "Remove-Item $HOME\\knime-workspace -Recurse -Force",
]));

main.push(para([new PageBreak()]));

// ── AI Tools Usage (slim version, at the end) ──
main.push(h1("AI Tools Usage"));
main.push(p("Generative AI (Anthropic Claude Sonnet 4.6, via the Claude Code CLI) was used as a drafting aid for parts of this guide, for the installer template's Python code, and for the wording of inline code comments and log messages emitted by the pipeline. No AI tool is considered an author. The installer flow, all credentials handling, and every screenshot in the appendix come from real installs performed by the authors on the project VM."));
main.push(p("The authors retain full responsibility for the installer behavior and for the accuracy of this guide. All AI-assisted outputs were reviewed, corrected, and tested manually."));

main.push(para([new PageBreak()]));
main.push(p("— end of installation guide —", { alignment: AlignmentType.CENTER }));

sections.push({
  properties: {
    page: { size: { width: 12240, height: 15840 },
            margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 } },
    type: "nextPage",
  },
  headers: {
    default: new Header({
      children: [para([t("DataCycle Domotic · Installation Guide · v1.0", { size: 18, color: TEXT_DIM })],
        { alignment: AlignmentType.RIGHT })],
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

const doc = new Document({
  creator: "Group 14",
  title: "DataCycle Domotic — Installation Guide",
  description: "Step-by-step Installation Guide for the DataCycle Domotic platform.",
  styles: {
    default: { document: { run: { font: "Calibri", size: 22 } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 36, bold: true, color: "111827", font: "Calibri" },
        paragraph: { spacing: { before: 480, after: 240 }, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 28, bold: true, color: ACCENT, font: "Calibri" },
        paragraph: { spacing: { before: 360, after: 180 }, outlineLevel: 1 } },
      { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 24, bold: true, color: "111827", font: "Calibri" },
        paragraph: { spacing: { before: 280, after: 140 }, outlineLevel: 2 } },
    ],
  },
  numbering: {
    config: [
      { reference: "bullets", levels: [{ level: 0, format: LevelFormat.BULLET, text: "•",
          alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 720, hanging: 360 } } } }] },
      { reference: "numbers", levels: [{ level: 0, format: LevelFormat.DECIMAL, text: "%1.",
          alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 720, hanging: 360 } } } }] },
    ],
  },
  sections,
});

const primary = "docs/v2/out/DataCycle_Installation_Guide.docx";
Packer.toBuffer(doc).then(buf => {
  let target = primary;
  try {
    fs.writeFileSync(primary, buf);
  } catch (e) {
    if (e.code === "EBUSY" || e.code === "EPERM") {
      const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 16);
      target = primary.replace(".docx", `_${stamp}.docx`);
      fs.writeFileSync(target, buf);
      console.warn(`⚠ Primary file was locked; wrote ${target} instead.`);
    } else {
      throw e;
    }
  }
  console.log(`✓ Wrote ${target} (${(buf.length / 1024).toFixed(0)} KB) — ${screenshotFiles.length} screenshots embedded`);
});
