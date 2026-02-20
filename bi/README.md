# BI Dashboards

This directory contains dashboard configuration files and exports for the three
main reporting areas.

## Dashboards

| Dashboard | Description |
|-----------|-------------|
| `energy.json` | Energy consumption KPIs — hourly and daily totals by apartment |
| `environment.json` | Environmental quality — temperature, humidity, CO₂ trends |
| `presence.json` | Room presence ratios — occupancy patterns by apartment and day |

## Data Sources

All dashboards read from the Gold-layer SQLite database (`data/gold/gold.db`).

| Table | Used by |
|-------|---------|
| `fact_energy_hourly` | energy dashboard |
| `fact_environment_daily` | environment dashboard |
| `fact_presence_daily` | presence dashboard |
