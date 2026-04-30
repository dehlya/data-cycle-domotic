"""
configure_bi_knime.py -- Patch KNIME workflows + Power BI report
=================================================================
Reads .env (DB_URL, PBI_*) and rewrites the embedded DB connection
strings inside the .knwf and .pbix files so they point at the user's
local Postgres instead of the developer's.

Usage:
    python scripts/configure_bi_knime.py
    python scripts/configure_bi_knime.py --dry-run     # show what would change

KNIME (.knwf files in ml/knime/):
  - Patches every PostgreSQL Connector node: host, port, database, username.
  - Blanks out the encrypted password so KNIME prompts on first run
    (we can't generate a valid encrypted xpassword without KNIME's key).

Power BI (.pbix in bi/power_bi/):
  - Best-effort patch of the Connections file. Power BI may still ask
    for credentials and require you to "Apply changes" the first time
    (especially because the M code in DataMashup also references the
    server). If the auto-patch isn't enough, follow the manual steps
    printed by the installer.

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import os
import re
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import unquote, urlparse

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
KNIME_DIR    = PROJECT_ROOT / "ml" / "knime"
PBIX         = PROJECT_ROOT / "bi" / "power_bi" / "DataCycleDomotic.pbix"


# ── ANSI ──────────────────────────────────────────────────────────────────────
RESET="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"
GREEN="\033[32m"; RED="\033[31m"; YELLOW="\033[33m"; BLUE="\033[36m"

if not sys.stdout.isatty():
    RESET = BOLD = DIM = GREEN = RED = YELLOW = BLUE = ""


def ok(msg):   print(f"  {GREEN}\u2713{RESET} {msg}")
def warn(msg): print(f"  {YELLOW}\u26a0{RESET} {msg}")
def fail(msg): print(f"  {RED}\u2717{RESET} {msg}")


# ── ENV ───────────────────────────────────────────────────────────────────────
def load_db_settings():
    """Pull the app DB connection settings out of .env."""
    load_dotenv(PROJECT_ROOT / ".env")
    db_url = os.getenv("DB_URL")
    if not db_url:
        sys.exit("DB_URL not set in .env")
    p = urlparse(db_url)
    return {
        "host":     p.hostname or "localhost",
        "port":     str(p.port or 5432),
        "database": (p.path or "/").lstrip("/") or "postgres",
        "user":     unquote(p.username or ""),
        "password": unquote(p.password or ""),
    }


# ── KNIME PATCHING ────────────────────────────────────────────────────────────
def patch_xml_field(content: str, key: str, new_value: str, is_int=False) -> tuple[str, bool]:
    """Replace <entry key="KEY" type="..." value="OLD"/> with new_value."""
    if is_int:
        pattern = re.compile(rf'(<entry key="{re.escape(key)}" type="xint" value=")[^"]*(")')
    else:
        pattern = re.compile(rf'(<entry key="{re.escape(key)}" type="xstring" value=")[^"]*(")')
    new_content, n = pattern.subn(rf'\g<1>{re.escape(new_value).replace(chr(92), "")}\g<2>', content, count=1)
    return new_content, n > 0


def blank_password(content: str) -> tuple[str, bool]:
    """Replace <entry key="password" type="xpassword" value="..."/> with empty."""
    pattern = re.compile(r'(<entry key="password" type="xpassword" value=")[^"]*(")')
    new_content, n = pattern.subn(r'\g<1>\g<2>', content, count=1)
    return new_content, n > 0


def patch_settings_xml(content: str, db: dict) -> tuple[str, list[str]]:
    """Patch one PostgreSQL Connector settings.xml. Return (new_content, list of changed fields)."""
    changes = []
    content, ok_h = patch_xml_field(content, "host",          db["host"])
    if ok_h: changes.append("host")
    content, ok_p = patch_xml_field(content, "port",          db["port"], is_int=True)
    if ok_p: changes.append("port")
    content, ok_d = patch_xml_field(content, "database_name", db["database"])
    if ok_d: changes.append("database_name")
    content, ok_u = patch_xml_field(content, "username",      db["user"])
    if ok_u: changes.append("username")
    content, ok_pw = blank_password(content)
    if ok_pw: changes.append("password (cleared)")
    return content, changes


def patch_knwf(knwf_path: Path, db: dict, dry_run=False) -> int:
    """Patch all PostgreSQL Connector settings.xml inside one .knwf. Return count of nodes patched."""
    patched_count = 0
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        # Extract
        with zipfile.ZipFile(knwf_path, "r") as zf:
            zf.extractall(tmp)

        # Walk and patch
        for settings in tmp.rglob("settings.xml"):
            # Only PostgreSQL Connector node folders
            if "PostgreSQL Connector" not in str(settings.parent):
                continue
            content = settings.read_text(encoding="utf-8")
            new_content, changes = patch_settings_xml(content, db)
            if changes:
                if not dry_run:
                    settings.write_text(new_content, encoding="utf-8")
                rel = settings.relative_to(tmp)
                ok(f"  {knwf_path.name} :: {rel.parent.name} -> {', '.join(changes)}")
                patched_count += 1

        if patched_count and not dry_run:
            # Re-zip in place
            backup = knwf_path.with_suffix(".knwf.bak")
            if not backup.exists():
                shutil.copy2(knwf_path, backup)
            tmp_zip = knwf_path.with_suffix(".knwf.tmp")
            with zipfile.ZipFile(tmp_zip, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in tmp.rglob("*"):
                    if f.is_file():
                        zf.write(f, f.relative_to(tmp))
            tmp_zip.replace(knwf_path)

    return patched_count


# ── POWER BI PATCHING (best-effort) ───────────────────────────────────────────
def patch_pbix(pbix_path: Path, db: dict, dry_run=False) -> bool:
    """
    Try to patch the Connections file inside .pbix. Power BI may still need
    you to refresh credentials manually because the M code in DataMashup is
    a binary container we don't touch here.
    """
    if not pbix_path.exists():
        warn(f"{pbix_path} not found, skipping")
        return False

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        with zipfile.ZipFile(pbix_path, "r") as zf:
            zf.extractall(tmp)

        connections_file = tmp / "Connections"
        if not connections_file.exists():
            warn(".pbix has no Connections file; skipping")
            return False

        # Connections is UTF-16 LE JSON in newer .pbix files
        raw = connections_file.read_bytes()
        try:
            text = raw.decode("utf-16-le")
            encoding = "utf-16-le"
        except UnicodeDecodeError:
            text = raw.decode("utf-8")
            encoding = "utf-8"

        # Replace any "Server=...;Database=...;..." style connection strings
        # (Power BI stores them with various property orderings, regex-based)
        original = text
        text = re.sub(
            r'Server=([^;]+);Database=([^;]+)',
            f'Server={db["host"]}:{db["port"]};Database={db["database"]}',
            text,
        )
        # Some variants use "Host=" instead of "Server=" — cover that too
        text = re.sub(
            r'Host=([^;]+);Port=(\d+);Database=([^;]+)',
            f'Host={db["host"]};Port={db["port"]};Database={db["database"]}',
            text,
        )

        if text == original:
            warn("Connections file didn't match expected pattern -- no auto-patch possible")
            warn("You'll need to update the data source in Power BI manually (see install logs)")
            return False

        if dry_run:
            ok(f"Would patch Connections in {pbix_path.name}")
            return True

        connections_file.write_bytes(text.encode(encoding))

        # Re-zip in place
        backup = pbix_path.with_suffix(".pbix.bak")
        if not backup.exists():
            shutil.copy2(pbix_path, backup)
        tmp_zip = pbix_path.with_suffix(".pbix.tmp")
        with zipfile.ZipFile(tmp_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in tmp.rglob("*"):
                if f.is_file():
                    zf.write(f, f.relative_to(tmp))
        tmp_zip.replace(pbix_path)
        ok(f"Patched Connections in {pbix_path.name}")
        warn("First open in Power BI: may still ask for credentials. Enter app user "
             f"'{db['user']}' / your password.")
        return True


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    dry_run = "--dry-run" in sys.argv
    print(f"\n{BOLD}{BLUE}configure_bi_knime{RESET}  "
          f"{DIM}({'DRY RUN' if dry_run else 'live'}){RESET}\n")

    db = load_db_settings()
    print(f"  Target DB: {BOLD}{db['user']}@{db['host']}:{db['port']}/{db['database']}{RESET}\n")

    # KNIME
    print(f"{BOLD}KNIME workflows{RESET}")
    if not KNIME_DIR.exists():
        warn(f"{KNIME_DIR} not found, skipping")
    else:
        knwfs = list(KNIME_DIR.glob("*.knwf"))
        if not knwfs:
            warn("No .knwf files found")
        for knwf in knwfs:
            patched = patch_knwf(knwf, db, dry_run=dry_run)
            if patched == 0:
                warn(f"{knwf.name}: no PostgreSQL Connector nodes found to patch")

    # Power BI
    print(f"\n{BOLD}Power BI{RESET}")
    patch_pbix(PBIX, db, dry_run=dry_run)

    print(f"\n{GREEN}Done.{RESET}\n")


if __name__ == "__main__":
    main()
