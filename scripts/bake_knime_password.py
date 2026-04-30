"""
bake_knime_password.py -- Bake .env DB password into KNIME workflows
=====================================================================
KNIME refuses to overwrite password fields via flow variables or the
-credential CLI flag at runtime ("It's not possible to overwrite
passwords with flow variables"). The only way to get the password
into a Credentials Configuration node is to write the encrypted blob
directly into its settings.xml.

This script does that automatically by calling KNIME's own
`org.knime.core.util.KnimeEncryption.encrypt()` Java method (the same
code KNIME GUI uses), guaranteeing the output blob is one KNIME will
accept.

Flow:
  1. Read DB password from .env DB_URL.
  2. Locate KNIME install (bundled JRE + org.knime.core_*.jar).
  3. Compile a tiny Java helper inline (KnimeEncrypt.java).
  4. Run it to get the encrypted blob.
  5. Patch every Credentials Configuration settings.xml in
     ~/knime-workspace/<workflow>/  AND  ml/knime/*.knwf.

Usage:
    python scripts/bake_knime_password.py                # bake from .env
    python scripts/bake_knime_password.py --dry-run      # show only

Author: Group 14 - Data Cycle Project - HES-SO Valais 2026
"""

import os
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import unquote, urlparse

from dotenv import load_dotenv


PROJECT_ROOT     = Path(__file__).resolve().parent.parent
KNIME_DIR        = PROJECT_ROOT / "ml" / "knime"
KNIME_WORKSPACE  = Path(os.path.expanduser("~/knime-workspace"))


# ── ANSI ──────────────────────────────────────────────────────────────────────
RESET="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"
GREEN="\033[32m"; RED="\033[31m"; YELLOW="\033[33m"; BLUE="\033[36m"
if not sys.stdout.isatty():
    RESET = BOLD = DIM = GREEN = RED = YELLOW = BLUE = ""

def header(m): print(f"\n{BOLD}{BLUE}== {m} =={RESET}")
def ok(m):     print(f"  {GREEN}\u2713{RESET} {m}")
def warn(m):   print(f"  {YELLOW}\u26a0{RESET} {m}")
def fail(m):   print(f"  {RED}\u2717{RESET} {m}")


# ── DISCOVERY ─────────────────────────────────────────────────────────────────
def find_knime_root() -> Path:
    """Locate the KNIME installation directory."""
    candidates = []
    if os.name == "nt":
        candidates += [
            Path(r"C:\Program Files\KNIME"),
            Path(r"C:\Program Files (x86)\KNIME"),
            Path(os.path.expanduser(r"~\AppData\Local\KNIME")),
        ]
    elif sys.platform == "darwin":
        candidates += [Path("/Applications/KNIME.app/Contents/Eclipse")]
    else:
        candidates += [Path("/opt/knime"), Path(os.path.expanduser("~/knime"))]
    for c in candidates:
        if c.exists() and (c / "plugins").exists():
            return c
    sys.exit(f"{RED}Could not find KNIME installation. Searched: {candidates}{RESET}")


def find_java_tool(knime_root: Path, tool: str) -> Path:
    """Find java.exe or javac.exe in KNIME's bundled JRE."""
    exe = f"{tool}.exe" if os.name == "nt" else tool
    matches = list(knime_root.glob(f"plugins/org.knime.binary.jre.*/jre/bin/{exe}"))
    if matches:
        return matches[0]
    # Fallback: PATH
    found = shutil.which(tool)
    if found:
        return Path(found)
    return None


def find_knime_core_jar(knime_root: Path) -> Path:
    """Find org.knime.core_*.jar (contains KnimeEncryption)."""
    matches = list((knime_root / "plugins").glob("org.knime.core_*.jar"))
    if not matches:
        sys.exit(f"{RED}Could not find org.knime.core_*.jar in {knime_root / 'plugins'}{RESET}")
    # Prefer the largest (likely the main artefact)
    return max(matches, key=lambda p: p.stat().st_size)


# ── ENCRYPT ───────────────────────────────────────────────────────────────────
JAVA_HELPER_SOURCE = r"""
public class KnimeEncrypt {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: KnimeEncrypt <password>");
            System.exit(2);
        }
        String result = org.knime.core.util.KnimeEncryption.encrypt(args[0].toCharArray());
        System.out.print(result);
    }
}
""".strip()


def encrypt_with_knime(password: str, knime_root: Path) -> str:
    """Compile and run a tiny Java helper to encrypt the password via KNIME's own code."""
    java  = find_java_tool(knime_root, "java")
    javac = find_java_tool(knime_root, "javac")
    if not java:
        sys.exit(f"{RED}Could not find java executable.{RESET}")
    if not javac:
        sys.exit(
            f"{RED}Could not find javac. KNIME's bundled JRE doesn't include the compiler.{RESET}\n"
            f"Install a JDK and ensure 'javac' is on PATH, or set JAVA_HOME."
        )
    core_jar = find_knime_core_jar(knime_root)
    ok(f"java:     {java}")
    ok(f"javac:    {javac}")
    ok(f"core jar: {core_jar.name}")

    sep = ";" if os.name == "nt" else ":"
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        (tmp / "KnimeEncrypt.java").write_text(JAVA_HELPER_SOURCE, encoding="utf-8")
        # Compile
        compile_cmd = [str(javac), "-cp", str(core_jar), "KnimeEncrypt.java"]
        r = subprocess.run(compile_cmd, cwd=tmp, capture_output=True, text=True)
        if r.returncode != 0:
            sys.exit(f"{RED}javac failed:{RESET}\n{r.stderr}")
        # Run
        cp = f"{tmp}{sep}{core_jar}"
        run_cmd = [str(java), "-cp", cp, "KnimeEncrypt", password]
        r = subprocess.run(run_cmd, capture_output=True, text=True)
        if r.returncode != 0:
            sys.exit(f"{RED}java run failed:{RESET}\n{r.stderr}")
        encrypted = r.stdout.strip()
        if not encrypted:
            sys.exit(f"{RED}Encryption returned empty string.{RESET}")
        return encrypted


# ── PATCH XML ─────────────────────────────────────────────────────────────────
def patch_credentials_settings(settings_path: Path, username: str, encrypted_pw: str, dry_run: bool) -> bool:
    """Update <username> and <passwordEncrypted> in a Credentials Configuration settings.xml."""
    content = settings_path.read_text(encoding="utf-8")

    # The Credentials Configuration node stores its config under a nested
    # <config key="credentialsValue"> — that's where username + encrypted
    # password live. Patch them.
    new = content
    new = re.sub(
        r'(<entry key="username" type="xstring" value=")[^"]*(")',
        rf'\g<1>{re.escape(username).replace(chr(92), "")}\g<2>',
        new, count=2,  # username appears twice (default + credentialsValue), patch both
    )
    new = re.sub(
        r'(<entry key="passwordEncrypted" type="xpassword" value=")[^"]*(")',
        rf'\g<1>{encrypted_pw}\g<2>',
        new, count=1,
    )
    # Also patch the legacy `password` xpassword key if present
    new = re.sub(
        r'(<entry key="password" type="xpassword" value=")[^"]*(")',
        rf'\g<1>{encrypted_pw}\g<2>',
        new, count=1,
    )

    if new == content:
        return False
    if not dry_run:
        settings_path.write_text(new, encoding="utf-8")
    return True


def patch_workspace(workspace: Path, username: str, encrypted_pw: str, dry_run: bool) -> int:
    """Patch all Credentials Configuration settings.xml files in the workspace."""
    count = 0
    for cfg in workspace.rglob("settings.xml"):
        if "Credentials Configuration" not in str(cfg.parent):
            continue
        if patch_credentials_settings(cfg, username, encrypted_pw, dry_run):
            ok(f"Patched: {cfg.relative_to(workspace.parent)}")
            count += 1
        else:
            warn(f"No change: {cfg.relative_to(workspace.parent)}")
    return count


def patch_knwf(knwf_path: Path, username: str, encrypted_pw: str, dry_run: bool) -> int:
    """Patch every Credentials Configuration settings.xml inside one .knwf."""
    count = 0
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        with zipfile.ZipFile(knwf_path, "r") as zf:
            zf.extractall(tmp)
        for cfg in tmp.rglob("settings.xml"):
            if "Credentials Configuration" not in str(cfg.parent):
                continue
            if patch_credentials_settings(cfg, username, encrypted_pw, dry_run):
                ok(f"Patched: {knwf_path.name} :: {cfg.parent.name}")
                count += 1
        if count and not dry_run:
            backup = knwf_path.with_suffix(".knwf.bak")
            if not backup.exists():
                shutil.copy2(knwf_path, backup)
            tmp_zip = knwf_path.with_suffix(".knwf.tmp")
            with zipfile.ZipFile(tmp_zip, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in tmp.rglob("*"):
                    if f.is_file():
                        zf.write(f, f.relative_to(tmp))
            tmp_zip.replace(knwf_path)
    return count


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    dry_run = "--dry-run" in sys.argv

    header("Bake KNIME password")
    print(f"  {DIM}({'DRY RUN' if dry_run else 'live'}){RESET}\n")

    # Load .env
    load_dotenv(PROJECT_ROOT / ".env")
    db_url = os.getenv("DB_URL")
    if not db_url:
        sys.exit(f"{RED}DB_URL not in .env{RESET}")
    p = urlparse(db_url)
    db_user = unquote(p.username or "")
    db_pwd  = unquote(p.password or "")
    if not db_user or not db_pwd:
        sys.exit(f"{RED}Could not extract user+password from DB_URL{RESET}")
    ok(f"DB user from .env: {db_user}")

    # Locate KNIME
    header("Locating KNIME")
    knime_root = find_knime_root()
    ok(f"KNIME root: {knime_root}")

    # Encrypt password
    header("Encrypting password via KNIME's own KnimeEncryption")
    encrypted = encrypt_with_knime(db_pwd, knime_root)
    ok(f"Encrypted blob: {encrypted[:20]}... ({len(encrypted)} chars)")

    # Patch workspace
    header("Patching ~/knime-workspace")
    if KNIME_WORKSPACE.exists():
        for wf in KNIME_WORKSPACE.iterdir():
            if wf.is_dir() and (wf / "workflow.knime").exists():
                n = patch_workspace(wf, db_user, encrypted, dry_run)
                if n == 0:
                    warn(f"{wf.name}: no Credentials Configuration nodes found")
    else:
        warn(f"Workspace not found: {KNIME_WORKSPACE}")

    # Patch source .knwf
    header("Patching ml/knime/*.knwf")
    if KNIME_DIR.exists():
        for knwf in KNIME_DIR.glob("*.knwf"):
            n = patch_knwf(knwf, db_user, encrypted, dry_run)
            if n == 0:
                warn(f"{knwf.name}: no Credentials Configuration nodes found")
    else:
        warn(f"{KNIME_DIR} not found")

    print(f"\n{GREEN}{BOLD}Done.{RESET}\n")
    if not dry_run:
        print(f"Next: {DIM}python scripts/run_knime_predictions.py{RESET}\n")


if __name__ == "__main__":
    main()
