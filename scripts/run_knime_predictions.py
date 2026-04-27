"""
run_knime_predictions.py -- Run KNIME prediction workflows in batch mode
=========================================================================
Headlessly executes the deployed KNIME workflows so the predictions get
written to gold.fact_prediction without anyone clicking through the GUI.

Usage:
    python scripts/run_knime_predictions.py            # run all
    python scripts/run_knime_predictions.py motion     # only motion
    python scripts/run_knime_predictions.py consumption# only consumption

Requirements:
    - KNIME Analytics Platform installed (auto-detected at common paths)
    - Workflows deployed to ~/knime-workspace (done by the installer's
      configure_bi_knime step)
    - DB credentials in .env

Notes on the password:
    KNIME forbids overwriting password fields via flow variables for
    security reasons ("It's not possible to overwrite passwords with flow
    variables"). So we use **Workflow Credentials** instead:
      1. Each workflow has a Workflow Credential named 'db' (created once
         in KNIME GUI: right-click workflow -> Workflow Credentials -> Add
         'db' with empty user/password). See ml/knime/SETUP.md.
      2. Each PostgreSQL Connector is set to "Use credentials" -> 'db'.
      3. We inject username + password at runtime via:
            -credential=db;<user>;<password>
         which IS allowed by KNIME for Workflow Credentials.

This script is safe to schedule daily (Windows Task Scheduler / cron) to
keep predictions fresh.

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import os
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
DB_URL = os.getenv("DB_URL", "")

KNIME_WORKSPACE = Path(os.path.expanduser("~/knime-workspace"))
WORKFLOWS = {
    "motion":      "Motion_Prediction_Server",
    "consumption": "Consumption_Weather_Prediction_Server",
}


# ── ANSI ──────────────────────────────────────────────────────────────────────
RESET="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"
GREEN="\033[32m"; RED="\033[31m"; YELLOW="\033[33m"; BLUE="\033[36m"

if not sys.stdout.isatty():
    RESET = BOLD = DIM = GREEN = RED = YELLOW = BLUE = ""


def header(msg):  print(f"\n{BOLD}{BLUE}== {msg} =={RESET}")
def ok(msg):      print(f"  {GREEN}\u2713{RESET} {msg}")
def warn(msg):    print(f"  {YELLOW}\u26a0{RESET} {msg}")
def fail(msg):    print(f"  {RED}\u2717{RESET} {msg}")


# ── KNIME LOCATION ────────────────────────────────────────────────────────────
def find_knime():
    """Locate the KNIME executable."""
    candidates = []
    if os.name == "nt":
        candidates += [
            Path(r"C:\Program Files\KNIME\knime.exe"),
            Path(r"C:\Program Files (x86)\KNIME\knime.exe"),
            Path(os.path.expanduser(r"~\AppData\Local\KNIME\knime.exe")),
        ]
    elif sys.platform == "darwin":
        candidates += [Path("/Applications/KNIME.app/Contents/MacOS/Knime")]
    else:
        candidates += [
            Path("/opt/knime/knime"),
            Path(os.path.expanduser("~/knime/knime")),
            Path("/usr/local/bin/knime"),
        ]
    for c in candidates:
        if c.exists():
            return c
    on_path = shutil.which("knime")
    return Path(on_path) if on_path else None


def parse_db_credentials():
    """Pull username + password out of DB_URL for the -credential flag."""
    if not DB_URL:
        return None, None
    p = urlparse(DB_URL)
    return unquote(p.username or ""), unquote(p.password or "")


# ── CREDENTIAL NODE DISCOVERY ─────────────────────────────────────────────────
def _find_model_param_name(settings_xml: Path) -> str | None:
    """
    Extract the user-set Parameter/Variable Name from a Credentials
    Configuration node's settings.xml.

    The XML has multiple <entry key="parameter-name" .../> elements at
    different config nesting levels. The one we want lives directly under
    <config key="model"> (NOT inside nested configs like credentialsValue).
    Parse the XML and pick that exact one.
    """
    try:
        tree = ET.parse(settings_xml)
    except Exception:
        return None
    root = tree.getroot()
    # Strip namespace for simple matching
    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"
    # Find <config key="model"> child of root
    for cfg in root.findall(f"{ns}config"):
        if cfg.attrib.get("key") == "model":
            # Look ONLY at direct children (don't recurse into nested configs).
            # KNIME 5.x uses 'parameterName' (camelCase); older versions used
            # 'parameter-name' (kebab). Accept both.
            for entry in cfg.findall(f"{ns}entry"):
                if entry.attrib.get("key") in ("parameterName", "parameter-name"):
                    val = entry.attrib.get("value")
                    if val:
                        return val
            break
    return None


def find_credential_config_nodes(workflow_dir: Path) -> list[tuple[str, str]]:
    """
    Find every Credentials Configuration node inside a workflow folder.
    Returns list of (node_id, parameter_name) tuples.

    Reads the model-level parameter-name from each node's settings.xml.
    """
    found = []
    for d in workflow_dir.rglob("Credentials Configuration*"):
        if not d.is_dir():
            continue
        m = re.search(r"\(#(\d+)\)", d.name)
        if not m:
            continue
        node_id = m.group(1)
        settings = d / "settings.xml"
        if not settings.exists():
            continue
        param_name = _find_model_param_name(settings) or "credentials"
        found.append((node_id, param_name))
    return found


# ── BATCH RUNNER ──────────────────────────────────────────────────────────────
def run_workflow(knime_exe: Path, workflow_dir: Path, db_user: str, db_password: str) -> bool:
    """Run one workflow in batch mode. Returns True on exit code 0."""
    if not workflow_dir.exists():
        fail(f"Workflow not found: {workflow_dir}")
        warn(f"   Run the installer (or scripts/configure_bi_knime.py) to deploy it first.")
        return False

    print(f"  Running {workflow_dir.name} ...")
    print(f"  {DIM}(this can take 5-30 min depending on data volume){RESET}")

    cmd = [
        str(knime_exe),
        "-consoleLog",
        "-nosplash",
        "-reset",
        "-application", "org.knime.product.KNIME_BATCH_APPLICATION",
        "-workflowDir=" + str(workflow_dir),
    ]
    # NOTE: KNIME has no CLI mechanism for injecting credentials into
    # Credentials Configuration nodes. We tried -credential (Workflow
    # Credentials only), -workflow.variable (KNIME blocks password
    # overrides), and -option (no 'credentials' type — only primitives).
    # The only working path is to bake the encrypted password into the
    # node's settings.xml using KNIME's own encryption code:
    #
    #     python scripts/bake_knime_password.py
    #
    # That script reads the password from .env and patches every
    # Credentials Configuration node in both ~/knime-workspace/ and
    # ml/knime/*.knwf. Run it once after install (or any time .env's
    # DB password changes). See ml/knime/SETUP.md.

    t0 = datetime.now()
    res = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = (datetime.now() - t0).total_seconds()

    if res.returncode == 0:
        ok(f"  {workflow_dir.name} completed in {elapsed:.0f}s")
        return True

    fail(f"  {workflow_dir.name} failed (exit {res.returncode}, {elapsed:.0f}s)")
    # Print last few lines of output to help debug
    out_lines = (res.stdout + "\n" + res.stderr).strip().splitlines()
    for line in out_lines[-15:]:
        print(f"    {DIM}{line[:140]}{RESET}")
    return False


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    header("Run KNIME Predictions")

    knime_exe = find_knime()
    if not knime_exe:
        fail("KNIME Analytics Platform not detected.")
        warn("  Install from https://www.knime.com/downloads")
        sys.exit(1)
    ok(f"KNIME: {knime_exe}")

    if not KNIME_WORKSPACE.exists():
        fail(f"KNIME workspace not found: {KNIME_WORKSPACE}")
        warn("  Run the installer (or scripts/configure_bi_knime.py) to deploy workflows.")
        sys.exit(1)
    ok(f"Workspace: {KNIME_WORKSPACE}")

    db_user, db_password = parse_db_credentials()
    if db_user:
        ok(f"DB credentials loaded for {db_user}")
    else:
        warn("DB_URL not in .env -- workflows will use embedded credentials only")

    # Pick which workflows to run
    args = [a for a in sys.argv[1:] if a in WORKFLOWS]
    selected = args if args else list(WORKFLOWS.keys())

    header(f"Running {len(selected)} workflow(s)")
    successes = failures = 0
    for key in selected:
        wf_dir = KNIME_WORKSPACE / WORKFLOWS[key]
        if run_workflow(knime_exe, wf_dir, db_user, db_password):
            successes += 1
        else:
            failures += 1

    print()
    print(f"{BOLD}Total:{RESET} {GREEN}{successes} ok{RESET}  {RED}{failures} failed{RESET}\n")
    sys.exit(0 if failures == 0 else 1)


if __name__ == "__main__":
    main()
