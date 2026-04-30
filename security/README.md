# Security & data privacy

## Threat model in one paragraph

DataCycle runs entirely on a single Windows VM behind the school's network.
Source data (sensor JSON over SMB, weather over sFTP, apartment metadata over
MySQL) reaches the VM through the school's existing trust boundary; we don't
re-implement transport security at the application layer. Inside the VM,
PostgreSQL holds all silver + gold data, and Power BI Desktop is the only
client. Our security work focuses on **(a)** keeping credentials out of source
control, **(b)** GDPR-compliant handling of identifiable data at the
silver-to-gold boundary, and **(c)** row-level access control in Power BI so a
tenant can only see their own apartment.

## Credentials

All secrets are loaded from a `.env` file at the project root:

- `.env.example` is committed (template, no real values)
- `.env` is in `.gitignore` and **never** committed
- The installer writes `.env` based on form input; subsequent runs reuse it

Single connection string per system:

| Variable | What |
|---|---|
| `DB_URL` | PostgreSQL connection (app user, not admin) |
| `MYSQL_URL` | School MySQL master registry |
| `SFTP_HOST` / `SFTP_USER` / `SFTP_PASSWORD` / `SFTP_PATH` | Weather sFTP |
| `SMB_PATH` | UNC path to the sensor share (mounted at install time) |

KNIME workflows use a `Variable → Credentials` adapter chain so DB credentials
are passed in at run time via CLI flow variables — they're never written into
the `.knwf` files on disk. See [`ml/knime/README.md`](../ml/knime/README.md)
for the pattern.

## GDPR & anonymisation

We apply [GDPR Art. 4(1)](https://gdpr-info.eu/art-4-gdpr/) at the
**silver-to-gold boundary**. Identifiable fields are dropped or masked before
they reach the gold star schema that Power BI consumes. The full rationale is
in [`docs/v2/DECISIONS.md`](../docs/v2/DECISIONS.md) — ADR-005 covers the
first-name pseudonym trade-off; ADR-007 covers the masking policy.

| Field (silver) | Gold | Reason |
|---|---|---|
| `users.*` table | **Not imported at all** | Defence in depth — full user accounts are sensitive |
| `dim_apartment.owner_user_id` | NULL'd | Would link a tenant to their account |
| `dim_apartment.building_name` | Replaced with `"Building <building_id>"` | Address leakage |
| `dim_apartment.first_name` | **Kept as a pseudonym** | Used for RLS lookup; first name alone at 2-tenant scale isn't re-identifiable per Art. 4(1). For >2 tenants in production we'd hash these too. |
| `dierrors.*` (error logs) | Cleaned, no message body retained | Logs sometimes contain incidental PII |
| Sensor readings (energy, environment, presence) | Pass through | Not personal data at the granularity we collect |

Implementation lives in `etl/silver_to_gold/populate_dimensions.py`.

## Row-level security in Power BI

The `.pbix` enforces RLS on `gold.dim_apartment.apartment_key`:

- A tenant signed in as e.g. `jeremie` only sees rows where `apartment_key`
  matches their apartment.
- The "admin" role sees both apartments and a comparison page.
- Roles are defined in *Modeling → Manage roles* inside Power BI Desktop and
  enforced when the `.pbix` is published or when *View as roles* is used
  during testing.

The Power BI connection itself uses Postgres credentials (the app user from
`DB_URL`, not the admin user). Postgres is configured with
`pg_hba.conf` set to require password auth for non-local hosts; on localhost
we allow trust auth so the watcher and admin pane can connect without a
password roundtrip.

## Postgres roles

Two roles, principle of least privilege:

| Role | Used by | Permissions |
|---|---|---|
| Admin (default `postgres`) | Installer only — once, at setup | `CREATEDB`, `CREATEROLE`, ownership of new objects |
| App user (default `domotic`) | Watcher, ETL, Power BI, KNIME | `CONNECT` + `USAGE` on `silver`, `gold`; `SELECT/INSERT/UPDATE/DELETE` on its own tables |

The installer creates the app user once and rotates `DB_URL` to point at it
— after install, the admin password is no longer needed by the running
system.

## Data at rest

- **Bronze files** live under `storage/bronze/` on the VM's local disk. Folder
  permissions inherit Windows ACLs from the VM's user account. After silver
  ingestion the files are gzip-compressed in place (10–15× smaller, audit
  trail preserved); nothing in bronze is encrypted at the application level.
- **PostgreSQL** uses default Windows file permissions on the data directory.
  No transparent data encryption — we rely on the VM's BitLocker / disk
  encryption (out of scope for this project).
- **`.env`** sits at the project root; on a hardened deploy this would be
  moved to a permission-locked location (`%PROGRAMDATA%\DataCycle\.env` with
  ACLs restricted to the service account).

## Data in transit

- **MySQL → silver**: school-managed, school's responsibility.
- **sFTP weather download**: SSH transport (paramiko), strict host-key
  checking can be enabled but is off by default for first-run convenience.
- **SMB share**: mounted with the credentials the user supplied to the
  installer; the actual transport security is whatever Windows negotiates
  (SMB 3.x with encryption when both ends support it).
- **PostgreSQL connections**: localhost-only by default; SSL is available on
  the connection string but disabled out of the box because everything runs
  on one machine.

## What we deliberately did NOT build

These are documented as future work in `docs/v2/DECISIONS.md`:

- Encryption-at-rest for bronze + Postgres (rely on disk-level encryption instead)
- Centralised secret management (e.g. HashiCorp Vault) — `.env` is enough at
  2-tenant scale
- Full IAM / SSO for Power BI access — RLS by apartment is the access mechanism
- Audit log of who-saw-what in the BI dashboards
- Penetration testing / formal threat modelling

## If you find a security issue

This is a school project — no formal disclosure programme. Open a GitHub
issue and tag it `security`, or contact the team directly through the
HES-SO channel.
