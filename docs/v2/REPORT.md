# DataCycle Domotic — Final Project Report

**HES-SO Valais · Data Engineering · Spring 2026**
**Group 14 · Use Case 02 — Apartment Domotics**
**Authors:** Dehlya Herbelin · Sacha Germanier · Johann

---

## Executive summary

DataCycle Domotic is an end-to-end data platform that ingests sensor readings
from two real Swiss apartments — one minute per file per apartment, every
minute, for the full year of historical data — and turns them into clean
analytical tables that drive Power BI dashboards (energy, comfort, presence)
and KNIME machine-learning predictions (motion forecasting at one-hour grain,
energy consumption forecasting at 24-hour grain).

The system runs entirely on a single Windows VM with no cloud dependencies.
Three external sources (an SMB share for sensor JSON, a school MySQL database
for apartment metadata, an sFTP server for daily weather forecasts) feed a
classical medallion ETL on PostgreSQL 17: bronze (raw immutable files,
gzip-compressed after silver insertion to preserve the audit trail while
shrinking disk usage 10–15×), silver (cleaned and validated, ~26 million
rows), and gold (a star schema with seven dimensions, five fact tables, one
materialised view for cost calculations).

Deployment is fully automated. A web-based wizard generates a single
self-contained Python installer, baking the customer's credentials into one
file. Running it executes ten phases — clone, virtual environment, .env,
pre-flight checks, schema creation, MySQL bootstrap, SMB backfill, gold ETL,
Power BI / KNIME auto-configuration, optional watcher autostart — and ends
with a Streamlit admin pane open at `http://localhost:8501`. First install
takes about four hours on a fresh machine (the silver backfill is the long
pole); re-installs are about fifteen minutes thanks to watermarks and a
processed-files skip-list.

The system has been tested end-to-end on the project VM and is ready for
the defence demonstration on 8 May 2026.

---

## 1. Project overview

### 1.1 Context and problem

The HES-SO Valais Smart Living Lab provides a stream of IoT sensor data from
two instrumented apartments. The data covers four physical phenomena —
electrical energy (per device, per minute), environmental quality
(temperature, humidity, CO₂, noise, atmospheric pressure, window/door open
flags), occupant presence (motion sensors), and device health (battery
level, error counts, last-seen timestamp). Daily weather forecasts from the
Swiss MeteoSwiss network round out the picture.

The challenge is the classical data-engineering one — turning raw
high-frequency sensor JSON into clean, query-able analytical tables that
non-technical users can consume through a dashboard, while preserving an
auditable trail of the original measurements and respecting GDPR.

### 1.2 What we built

A complete medallion-architecture pipeline plus the operational tooling
needed to run it as a single-VM deployment:

- **Three ingestion paths** — predictive SMB scan for sensor JSON files,
  paramiko sFTP for weather, SQLAlchemy for the school MySQL master
  registry — feeding a filesystem bronze layer.
- **Bronze → silver ETL** for sensors and weather, both using a COPY-into-
  TEMP-TABLE upsert pattern that is 50–150× faster than per-row INSERT and
  produces idempotent results via a watermark table.
- **Silver → gold ETL** in nine steps, materialising a star schema with
  conformed dimensions, applying GDPR anonymisation at this boundary, and
  refreshing a `mv_energy_with_cost` materialised view that joins energy
  facts with the Oiken tariff dimension.
- **Two KNIME machine-learning workflows** — logistic regression for room
  occupancy probability one hour ahead, linear regression with weather
  features for room energy consumption 24 hours ahead — running in batch
  mode and writing predictions back into the gold layer.
- **Power BI dashboards** with row-level security per apartment, plus a
  Streamlit admin pane for non-technical operators.
- **A self-contained Python installer** that deploys the whole stack on
  a fresh Windows VM in one command.

### 1.3 Scope and out-of-scope

In scope and delivered:

- Two apartments, one operational year of historical data
- Energy, environment, presence, device-health, weather facts
- Motion and consumption ML predictions
- Power BI dashboards (Energy, Environment, Presence, Device Health,
  Predictions, Overview, Compare)
- Customer-deployable installer + admin operations pane

Deferred / out of scope (documented in
[`DECISIONS.md`](DECISIONS.md) and discussed below in §6):

- Centralised secrets management (HashiCorp Vault or similar)
- Encryption at rest at the application layer (we rely on disk-level
  encryption and OS file permissions)
- Alerting / on-call monitoring beyond the admin pane's freshness view
- Multi-tenant SSO (we use Power BI's role-based RLS instead)
- Internationalisation of the dashboards
- A separate SAP Analytics Cloud track (abandoned in favour of Power BI
  focus)

---

## 2. Documentation map

This report is the high-level overview. For the deep technical detail, code
walkthroughs, and operational runbook, the project ships five companion
documents alongside the source code, all in `docs/v2/`:

| Document | What's in it | Audience |
|---|---|---|
| [`TECHNICAL.md`](TECHNICAL.md) | Architecture, schema, pipeline internals, performance numbers, every script explained, project layout. ~520 lines. | Developers, code reviewers, the academic report. |
| [`INSTALLATION.md`](INSTALLATION.md) | Hardware + software requirements, the wizard walkthrough, terminal output for each of the ten install phases (with screenshots), verification steps, troubleshooting table. ~520 lines. | The IT specialist installing the system on the customer's machine. |
| [`USER_GUIDE.md`](USER_GUIDE.md) | How to use the Power BI dashboards (each tab explained), the Streamlit admin pane, FAQs about the data, refresh cadence. | The end user — caretaker, building manager, tenant-with-RLS. |
| [`OPERATIONS.md`](OPERATIONS.md) | Day-to-day runbook: where logs are, how to restart the watcher, retention policies, common failures and recoveries, scheduled tasks. | The future maintainer (could be a different person from the installer). |
| [`DECISIONS.md`](DECISIONS.md) | Eight Architecture Decision Records (ADRs) — every non-obvious choice we made, what alternatives we rejected, and why. Includes ADR-005 (first-name pseudonym GDPR rationale) and ADR-007 (compress vs. delete trade-off). | Anyone reviewing the architecture; defence jury. |

Plus two domain-specific READMEs: [`security/README.md`](../security/README.md) for
the threat model and credential handling, and
[`ml/knime/README.md`](../ml/knime/README.md) for the KNIME workflow setup
including the Variable-to-Credentials trick used for runtime credential
injection.

The project's `README.md` at the root points to all of the above.

---

## 3. Architecture in brief

```
SMB share (sensor JSON) ─┐
MySQL  (apartment dims)  ─┼─►  Bronze ──► Silver ──► Gold ──► KNIME predictions
sFTP   (weather CSV)     ─┘   (raw files) (Postgres) (star)   (motion + consumption)
                                                       │
                                                       ├─► Power BI dashboards (RLS)
                                                       └─► Streamlit admin pane
```

A single Python process — `ingestion/fast_flow/watcher.py` — orchestrates
the whole pipeline. Three concurrent loops:

- **Fast flow** every 60 seconds: predictive SMB scan + bronze copy + silver
  flatten. Predictive ingestion uses a 5 ms `.exists()` check on the
  expected next filename rather than a full SMB scan, which makes the
  per-minute tick essentially free.
- **Gold refresh** every 15 minutes: incremental `populate_gold --sensors`.
- **Daily batch** at 07:30: weather download + silver clean + gold weather
  refresh + KNIME predictions + bronze cleanup. A nightly full-SMB scan at
  midnight catches any minutes the predictive mode missed.

Idempotency is built in at every step. Every silver insert is paired with a
watermark row in the same transaction; every bronze file appearing in the
watermark also appears in `processed.log` (a flat file the next SMB scan
reads as a skip-list). Both layers can be rebuilt from scratch by replaying
bronze, and individual phases are safe to re-run any number of times.

Full architectural detail is in [`TECHNICAL.md`](TECHNICAL.md). Decision
rationale (PostgreSQL over MongoDB, custom watcher over Airflow, file
system for bronze, etc.) is in [`DECISIONS.md`](DECISIONS.md).

---

## 4. GDPR & data privacy assessment

This section provides the written assessment requested by issue #32 of the
project backlog: *"how the solution we have developed is compliant with
General Data Privacy Rules (GDPR) and ethical principles, and if there are
potential issues."*

### 4.1 Regulatory framing

DataCycle Domotic is deployed in Switzerland on data from Swiss
apartments. The applicable regulatory framework is the **Swiss Federal Act
on Data Protection (revised FADP, in force since 1 September 2023)** for
the local jurisdiction, combined with **EU GDPR (Regulation 2016/679)** for
any cross-border processing involving EU data subjects. The two regimes are
broadly aligned in their core principles since the FADP revision; we apply
the stricter of the two for each requirement.

The system is a **data processor** in GDPR terminology: the school's Smart
Living Lab is the controller (it determines purpose and means), and
DataCycle is one technical component in the lab's data pipeline. For the
purposes of this assessment we treat DataCycle as if it were a controller
— we apply the more demanding controller-side obligations to be safe.

### 4.2 Data inventory

We classify every data element the pipeline touches into three categories
according to GDPR Article 4(1) ("personal data" = "any information relating
to an identified or identifiable natural person").

#### 4.2.1 Personal data — handled with care

- **First names** of occupants (`jimmy`, `jeremie`). At two-tenant scale,
  alone, with no surnames, addresses, contact details or device IDs that
  link back to an individual, a first name is generally not re-identifiable
  per Art. 4(1) — but in combination with sensor data showing presence
  patterns it could become so. We treat first names as **pseudonyms** and
  document this carefully. (See ADR-005.)

- **Apartment occupancy patterns** (presence by room and minute, motion
  events, door-window state). These reveal lifestyle patterns —
  daily routine, sleep schedule, frequency of guests, periods of absence.
  Even without a name attached, this is special-category-adjacent
  information that demands access controls.

- **Energy consumption per device per minute**. Disaggregated electrical
  load profiles can reveal lifestyle (microwave use → meals; TV →
  evening pattern) and even appliance brands. We treat this with the same
  controls as occupancy patterns.

#### 4.2.2 Data masked or removed at the silver-to-gold boundary

The single most important privacy control in the pipeline is what we call
the **silver-to-gold boundary**. Silver tables faithfully mirror the source
data including identifiers; gold tables, which are what Power BI and KNIME
consume, are sanitised. Implementation lives in
`etl/silver_to_gold/populate_dimensions.py`.

| Field (silver) | What happens at gold | Rationale |
|---|---|---|
| MySQL `users` table | **Never imported into silver in the first place** | Defence in depth — full user accounts are sensitive |
| `dim_apartment.owner_user_id` | NULLed | Would link a tenant to their school account |
| `dim_apartment.building_name` | Replaced with `"Building <building_id>"` | Address leakage prevention |
| `dim_apartment.first_name` | **Kept as a pseudonym** for RLS lookup | Without it the dashboards cannot route a tenant view; ADR-005 documents the trade-off |
| `dierrors.error_message` body | Not retained in `silver.di_errors_clean` | Logs sometimes contain incidental PII |
| Sensor readings (kWh, °C, motion) | Pass through | Not personal data at our granularity |

#### 4.2.3 Data we deliberately did not collect

Several MySQL tables exist in the source but are **never imported into
silver**:

`users`, `events`, `actions`, `achievements`, `badges`, `userrelationships`.

These are either out of scope (gamification features unused by us) or
contain identifying data we have no analytical use for.

### 4.3 Compliance with the seven GDPR principles (Art. 5)

#### 4.3.1 Lawfulness, fairness and transparency
The system processes data under contractual and legitimate-interest bases
established by the Smart Living Lab between itself and the apartment
occupants. As implementers, we are bound by the lab's existing consent
arrangements. Our role is to be transparent about *what* we process and
*how* — this report and the documentation map of §2 are part of that
transparency obligation.

#### 4.3.2 Purpose limitation
Each data element has a documented purpose:

- Energy data → consumption monitoring + ML forecasting
- Environmental data → comfort dashboards + anomaly detection
- Presence data → occupancy analytics + ML forecasting
- Weather data → cross-correlation with consumption (feature for ML)
- Apartment metadata → row-level-security routing in Power BI

We do not repurpose the data for marketing, profiling, or any use beyond
the project scope.

#### 4.3.3 Data minimisation
The pipeline imports only the MySQL tables needed for the analytics
(see §4.2.3 for what is *not* imported). Bronze keeps the raw payload, but
silver drops every column not used downstream. Gold is further reduced —
fact tables are pivots over only the four measurements that drive
the dashboards.

#### 4.3.4 Accuracy
Sensor readings are monotonically appended; we never modify historical data.
Outlier flags (`is_outlier`) mark physically-implausible values for
downstream filtering rather than silently dropping them — preserving
accuracy while enabling clean visualisations.

#### 4.3.5 Storage limitation
Bronze raw files are subject to a 30-day retention by default
(`BRONZE_RETENTION_DAYS=30`); after that, files are removed by
`scripts/cleanup_bronze.py`. Earlier than retention, after silver successfully
ingests them, files are gzip-compressed in place (10–15× shrink) but
kept readable so the audit trail survives a silver rebuild
(see ADR-007).

Silver and gold tables have no retention limit by default — they're the
analytical data the customer paid us to produce. The customer can apply
a retention policy via standard PostgreSQL `DELETE` operations; the
schema is designed to support this without breaking referential integrity
because date keys are integers, not enforced foreign keys to a fixed
range.

#### 4.3.6 Integrity and confidentiality
- All credentials are loaded from `.env` (gitignored).
- The Postgres app user (`domotic`) has DML/DDL privileges only on the
  `silver` and `gold` schemas, not on system catalogues.
- The Postgres admin password is used only at install time and never
  written to disk.
- KNIME workflows use the Variable→Credentials pattern to inject
  passwords at run time rather than baking them into `.knwf` files.
- Power BI enforces row-level security: tenant Jimmy sees only
  `apartment_key = jimmy`'s rows.

#### 4.3.7 Accountability
This document, the ADRs in [`DECISIONS.md`](DECISIONS.md), the inline
explanations in [`security/README.md`](../security/README.md), and the
git history together constitute our accountability record. Any reviewer
can trace why a decision was made and when.

### 4.4 Data subject rights

The pipeline supports the canonical GDPR data subject rights as follows:

| Right | How it's supported |
|---|---|
| **Right to be informed** (Arts. 13, 14) | This report + the user guide — the lab passes the relevant sections to occupants. |
| **Right of access** (Art. 15) | A SQL query against `silver.sensor_events` filtered by apartment yields the full record for a subject. The lab can extract and provide on request. |
| **Right to rectification** (Art. 16) | Sensor readings are observations and not user-stated facts, so rectification rarely applies. If incorrect occupancy metadata needs fixing, it's a single MySQL `UPDATE` on the source followed by a re-import. |
| **Right to erasure** (Art. 17) | `DELETE FROM gold.dim_apartment WHERE apartment_id = '...'` cascades through `apartment_key` foreign keys in the gold star schema. The bronze layer can be cleaned with a `find ... -delete`. Implementation is straightforward because we deliberately kept the natural-key chain transparent. |
| **Right to data portability** (Art. 20) | Silver and gold tables can be exported as CSV with one `COPY` command per table. |
| **Right to object** (Art. 21) | The lab handles consent withdrawal; once withdrawn, our delete-cascade workflow removes the data. |
| **Rights related to automated decision-making** (Art. 22) | The KNIME predictions are advisory — they do not gate access to anything or trigger automated decisions affecting the data subject. The dashboards display predicted values to operators for context; no consequence flows automatically. |

### 4.5 Risk assessment

We consider three classes of residual risk:

1. **Re-identification of pseudonymised first names.** At two-tenant
   scale, "jimmy" and "jeremie" are de-facto identifiable to anyone who
   knows who lives in the building. We accept this for the project
   scope. **For a production deployment beyond two tenants, we would
   replace first names with HMAC-hashed pseudonyms** (the lookup table
   stays in a separate, more tightly controlled storage tier). This
   mitigation is documented as deferred work, not as a current control.

2. **Inference of lifestyle from energy and presence data.** Even
   without identifiers, an attacker with access to the gold layer can
   infer occupancy patterns. Mitigation: row-level security in Power BI
   restricts each role to one apartment; the gold layer itself is on
   the local VM, not exposed to any external network.

3. **Compromise of the local VM.** If an attacker gets shell access to
   the VM, they read the Postgres data directory directly. Mitigation:
   the VM is on the school's internal network behind firewall rules;
   transparent disk encryption (BitLocker on the deployment VM) is
   recommended at deploy time but is the customer's responsibility.

Risks we explicitly do *not* address (out of scope, see §6):
formal pen-testing, intrusion detection, audit-log tampering protection,
formal DPIA against a registered DPO.

---

## 5. Ethical considerations beyond GDPR

GDPR is a legal floor; ethics is the ceiling we aspire to. Three points
worth raising:

**Power asymmetry between tenant and operator.** The dashboards we built
are for the building manager, not directly for the tenant. The tenant
generates the data but doesn't control the lens through which it's
analysed. This is a structural asymmetry we cannot solve at the technical
layer — only the organisational arrangement (who has dashboard access,
how findings are communicated) can address it. Our role is to make the
data minimisation and access controls (RLS) good enough that this
asymmetry is bounded.

**Algorithmic fairness for predictions.** The motion and consumption
forecasts are trained on historical patterns of the same two apartments
they predict. There is no risk of discriminatory generalisation across
groups (only two subjects, both consenting participants in the lab).
However, if the system were ever generalised across many apartments, the
training data would carry the energy signatures of socioeconomic groups
and could entrench biased forecasts. Documented as a future-work concern.

**Informed-by-default vs informed-on-request.** The lab's consent
arrangement is opaque to us. We recommend that any future deployment
include an explicit "what we collect, what we do with it, how to opt
out" notice given to occupants on day one — not buried in a contract
clause.

---

## 6. Scalability assessment

This addresses the scalability requirement of issue #31.

### 6.1 Current scale

Two apartments, one year of historical data:

- ~4 800 sensor JSON files per day across both apartments
- ~26 million rows in `silver.sensor_events`
- ~6.5 million rows across the gold fact tables
- ~2.5 GB of bronze on disk per year (raw); ~250 MB compressed
- A first install runs in ~4 hours, re-runs in ~15 minutes

### 6.2 100-apartment scenario (50× current)

| Layer | Estimated size at 100 apt | Bottleneck |
|---|---|---|
| Bronze | ~125 GB/year raw, ~12 GB compressed | Disk; trivially solved with a NAS or S3-compatible object store |
| Silver `sensor_events` | ~1.3 billion rows | The unique-index lookup during upserts becomes the bottleneck. Mitigation: partition by month, add BRIN index on `timestamp`, OR migrate to TimescaleDB (drop-in compatible). |
| Gold facts | ~325 million rows | Fact tables are still queryable on commodity hardware; partitioning by date_key eliminates full-table scans. |
| KNIME predictions | Current per-room cost × 50 rooms × 2 horizons | The two ML models scale linearly with apartments; runtime would be ~6h for the daily batch unless parallelised. Mitigation: split workflows by apartment, run multiple KNIME instances in parallel. |
| Watcher | One process per N apartments | Easy horizontal scale: spawn watchers for groups of apartments, share Postgres. |

### 6.3 Migration path

The architecture decisions were made with this growth in mind. Specifically:

- PostgreSQL → TimescaleDB is a single `CREATE EXTENSION` away — no
  schema changes needed, the `silver.sensor_events` and gold fact tables
  become hypertables.
- Bronze on filesystem can swap to S3 by changing `BRONZE_ROOT` to an
  `s3a://` URI and updating the watcher's open/copy paths.
- Custom watcher to Airflow is a re-implementation of three loops as
  three DAGs — each ETL script is already CLI-callable.

We made these design choices deliberately: small scale today, no painful
rewrite tomorrow. Documented in `DECISIONS.md` ADR-002 (Custom watcher
over Airflow) and ADR-001 (Postgres for silver+gold).

---

## 7. AI usage declaration

In line with HES-SO's policy on the use of generative AI in academic
projects, we explicitly disclose how AI tools were used in the development
of DataCycle Domotic.

### 7.1 Tools

The team used **Anthropic Claude (Sonnet 4.5)** as a coding and writing
assistant throughout the project, primarily as an interactive pair-
programming partner inside the development environment.

### 7.2 Scope

AI assistance was used for:

- **Code generation** — boilerplate (logging setup, argparse scaffolding,
  test fixtures), initial drafts of ETL scripts that the team then edited
  and integrated, suggestions for pandas / SQLAlchemy / DAX idioms.
- **Debugging** — explaining error tracebacks, suggesting causes, narrowing
  down the search space (e.g. the Power BI Python visual matplotlib issue,
  the KNIME version mismatch, the silver-index slowdown wall).
- **Documentation drafting** — initial drafts of `TECHNICAL.md`,
  `INSTALLATION.md`, `OPERATIONS.md`, `DECISIONS.md`, and this report,
  which the team then reviewed, corrected, and rewrote.
- **Architecture discussions** — sounding-board for trade-offs (compress
  vs. delete, watcher loops vs. Airflow, how to handle the .pbix data-
  source binary blob).

AI assistance was **not** used for:

- Final architectural decisions (every ADR was decided by the human team
  in weekly Friday review meetings — AI was consulted but not the
  decider).
- The core data model (the silver→gold boundary, the GDPR anonymisation
  strategy, the medallion structure are human-authored).
- Code review of teammates' work (humans reviewed humans).
- Hands-on operations on the customer's actual data — the install
  testing, the .pbix dashboard authoring, the KNIME workflow editing —
  was done by team members directly.

### 7.3 Verification

Every AI-suggested code change was subject to the same review process as
human-written code:

- Local execution and validation against real data
- Pull requests with at least one human reviewer
- Unit tests where applicable
- Integration tests via re-running the full installer end-to-end

When AI-generated code or text turned out to be incorrect (which happened
several times — e.g., AI-suggested DAX measures that referenced columns
not in our schema, AI-drafted documentation containing fabricated row
counts), the team caught and corrected it in review. The git history shows
the iterative correction process explicitly.

### 7.4 Accountability

The team takes full responsibility for all code, design choices, and
documentation in this project, regardless of whether the first draft
came from a human or from AI. Where the AI's contribution materially
shaped a decision, this is reflected in the relevant ADR or in
inline code comments. The team is accountable for the system's behaviour,
its security, its compliance, and its correctness — not the AI tools used
along the way.

---

## 8. Limitations and future work

The project ships a working end-to-end pipeline, but explicitly defers
several engineering concerns. Documented here so the customer (and the
defence jury) know what's missing and why.

| Item | Why deferred | Indicative effort to address |
|---|---|---|
| Centralised secrets management (Vault, AWS Secrets Manager) | At two-tenant scale `.env` is sufficient. Production at >10 apartments would change the calculus. | 1–2 weeks |
| Application-layer encryption at rest | We rely on disk-level encryption (BitLocker on the deployment VM). Adequate for the school's threat model. | 2–3 weeks |
| Monitoring + alerting (Prometheus / Grafana, PagerDuty) | The Streamlit admin pane provides visibility; alerting is overkill for a non-SLA system. | 1 week |
| Identity & access management (SSO, full IAM) | Power BI's RLS is the access mechanism. Multi-tenant SSO is irrelevant at our scale. | 2 weeks |
| Internationalisation of dashboards | All occupants speak French/English; not a current need. | 1 week |
| Formal DPIA against a registered DPO | This report serves the assessment requirement of the academic project. A real production deployment would warrant a formal DPIA. | 1–2 days work for a DPO |
| Multi-language documentation | English suffices for the academic deliverables and the technical audience. | 1 week per additional language |
| Automated test coverage beyond idempotency | The ETL is idempotent by design — re-running and verifying outputs is itself a smoke test — but a proper pytest suite with Postgres docker fixtures would be valuable. | 1–2 weeks |
| SAP Analytics Cloud track | Abandoned in favour of Power BI focus (see Sprint 7 retrospective). | 2–3 weeks if revived |

The project is intentionally scoped to deliver a working two-tenant
medallion pipeline with documentation, deployment, and a monitoring pane
— not a production-hardened SaaS. The decisions about what to leave
out are explicit and justified.

---

## 9. Conclusion

DataCycle Domotic delivers an end-to-end, customer-deployable smart-
apartment data pipeline that meets the project's functional requirements
and exceeds them in operational quality (idempotent re-runs, an admin
pane for non-technical operators, a self-contained installer with clear
error messages, comprehensive documentation across five companion
documents).

The technical work is grounded in deliberate design choices captured in
eight ADRs and validated against a real year of data on a real VM. The
GDPR posture is documented above, with explicit residual risks
acknowledged and a clear path to production hardening for a larger scale.
AI tools were used as productivity aids but did not determine
architectural choices, which were made and reviewed by the human team in
weekly Friday meetings throughout the spring 2026 semester.

The complete deliverables — source code, three customer-facing documents
(installation guide, user guide, technical documentation), this report,
two KNIME ML workflows, one Power BI dashboard, the Streamlit admin
pane, and the self-contained installer — are submitted together as the
group's final project for HES-SO Valais Data Engineering, Spring 2026.

---

*Last updated: 1 May 2026 · Group 14 · HES-SO Valais 2026.*
