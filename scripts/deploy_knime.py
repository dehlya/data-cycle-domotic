"""
deploy_knime.py -- Deploy ml/knime/*.knwf into ~/knime-workspace
=================================================================
Run this if KNIME wasn't installed when the main installer ran (so
the deploy step was skipped) — or any time you want to re-push the
.knwf workflows into your KNIME workspace.

Usage:
    python scripts/deploy_knime.py

What it does:
    - Locates ~/knime-workspace (creates it if missing)
    - Extracts every .knwf file in ml/knime/ as a folder inside the workspace
    - Workflows then appear in KNIME Explorer next time you open KNIME

If a workflow folder already exists in the workspace it is skipped
(remove it first if you want a fresh copy).

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import os
import sys
import zipfile
from pathlib import Path

PROJECT_ROOT    = Path(__file__).resolve().parent.parent
KNIME_WORKFLOWS = PROJECT_ROOT / "ml" / "knime"
KNIME_WORKSPACE = Path(os.path.expanduser("~/knime-workspace"))

GREEN="\033[32m" if sys.stdout.isatty() else ""
YELLOW="\033[33m" if sys.stdout.isatty() else ""
RED="\033[31m" if sys.stdout.isatty() else ""
RESET="\033[0m" if sys.stdout.isatty() else ""

def main():
    if not KNIME_WORKFLOWS.exists():
        sys.exit(f"{RED}Workflows folder not found: {KNIME_WORKFLOWS}{RESET}")
    knwfs = list(KNIME_WORKFLOWS.glob("*.knwf"))
    if not knwfs:
        sys.exit(f"{YELLOW}No .knwf files in {KNIME_WORKFLOWS}{RESET}")

    KNIME_WORKSPACE.mkdir(parents=True, exist_ok=True)
    print(f"Target workspace: {KNIME_WORKSPACE}\n")

    deployed = skipped = errors = 0
    for knwf in knwfs:
        target = KNIME_WORKSPACE / knwf.stem
        if target.exists():
            print(f"  {YELLOW}-{RESET} {knwf.stem}: already in workspace, skipping (delete the folder to re-deploy)")
            skipped += 1
            continue
        try:
            with zipfile.ZipFile(knwf) as zf:
                zf.extractall(KNIME_WORKSPACE)
            print(f"  {GREEN}\u2713{RESET} {knwf.stem}: deployed")
            deployed += 1
        except Exception as e:
            print(f"  {RED}\u2717{RESET} {knwf.stem}: {e}")
            errors += 1

    print()
    print(f"{GREEN}{deployed} deployed{RESET}, {YELLOW}{skipped} skipped{RESET}, {RED}{errors} errors{RESET}")
    if deployed > 0:
        print(f"\nNext: open KNIME. The workflows should appear in the KNIME Explorer panel on the left.")
        print(f"If KNIME opens with a different workspace, switch via File -> Switch Workspace -> {KNIME_WORKSPACE}")


if __name__ == "__main__":
    main()
