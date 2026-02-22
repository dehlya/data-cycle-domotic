# Architecture Decisions

## Python + asyncio for ingestion
**Decision:** Python 3.11 with asyncio for recup.py
**Reason:** I/O-bound task, asyncio allows concurrent sensor requests within the 60s window.

## PostgreSQL for Silver and Gold
**Decision:** PostgreSQL on the VM
**Reason:** Multi-user access, Power BI connector, proper SQL for OLAP queries.

## Apache Airflow for orchestration
**Decision:** Airflow on the VM
**Reason:** Full control over DAGs, visual monitoring, retry logic, logging.

## File system for Bronze
**Decision:** Timestamped folder structure on disk
**Reason:** Immutable raw storage, no DB overhead, easy to inspect and replay.
