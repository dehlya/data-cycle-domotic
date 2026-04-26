# Architecture Decisions

> Authoritative ADR list lives in the technical doc on the website
> (`/technical#s21`). This file is a short summary.

## ADR-001 — Python 3.11 for ingestion
**Decision:** Python 3.11 with `ThreadPoolExecutor` for parallel file copy and
`ProcessPoolExecutor` for CPU-bound JSON parsing.
**Reason:** I/O-bound workload, team familiarity, rich ecosystem (pandas,
SQLAlchemy, paramiko).

## ADR-002 — PostgreSQL for Silver and Gold
**Decision:** PostgreSQL 15+ with separate `silver` and `gold` schemas in the
same database.
**Reason:** Multi-user access, native Power BI connector, proper SQL for OLAP
queries, free.

## ADR-003 — Custom watcher over Airflow
**Decision:** A lightweight Python watcher (60s loop + daily weather
subprocess) instead of Apache Airflow.
**Reason:** Single-VM deployment makes Airflow overkill. Watcher has zero
infrastructure overhead, is trivially restartable, and avoids dependency
conflicts (Airflow pins old SQLAlchemy versions).

## ADR-004 — File system for Bronze
**Decision:** Local file system with `YYYY/MM/DD/HH/` timestamped folders.
**Reason:** Immutable raw storage, no DB overhead, easy to inspect and replay.

## ADR-005 — Self-contained installer
**Decision:** A client-side install wizard that generates a self-contained
Python installer with the deployer's credentials baked in.
**Reason:** Brings the deploy story from ~10 manual steps to one command.
Credentials never leave the deployer's machine.

## ADR-006 — Two PostgreSQL roles created at install
**Decision:** Admin credentials are used only at install time (DB + user
creation, ownership transfers); the .env contains only the app user.
**Reason:** Pipeline runs as a least-privilege user during normal operation.
Admin secret never persists.

## ADR-007 — Mask PII in gold dim_apartment, keep first-name pseudonym
**Decision:** Always mask `owner_user_id` and `building_name` in gold; keep
`name` (a common first name like 'jimmy') as-is.
**Reason:** Under GDPR Art. 4(1) a common first name in isolation is not
personal data. Power BI RLS depends on a stable column. The truly identifying
fields are removed.

## ADR-008 — Watcher (revisited at deploy time)
**Decision:** Reaffirm ADR-003 even at the deploy stage. Watcher remains the
sole orchestrator.
**Reason:** Adding Airflow now would mean another DB, another web UI, more
config, more failure surface. Status is observable via `install.log`,
`scripts/status.py`, and direct DB queries. Right size for &lt; 10 apartments
on a single VM.
