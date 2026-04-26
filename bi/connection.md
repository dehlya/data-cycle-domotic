# Power BI - PostgreSQL Connection Guide

## Prerequisites
- Power BI Desktop (latest version)
- Npgsql PostgreSQL connector (install from https://github.com/npgsql/npgsql/releases)
- Network access to the VM (via Tailscale or school network)

## Environment Variables

All connection details are stored in `.env` (see `.env.example`):

```env
PBI_SERVER=localhost:5432          # Tailscale IP or localhost
PBI_DATABASE=domotic_dev
PBI_USER=your_pg_user
PBI_PASSWORD=your_pg_password
```

## Connection Steps

1. Open Power BI Desktop
2. **Get Data** > **PostgreSQL database**
3. Fill in (from your `.env`):
   - **Server**: `PBI_SERVER`
   - **Database**: `PBI_DATABASE`
4. Credentials:
   - **User**: `PBI_USER`
   - **Password**: `PBI_PASSWORD`
5. In the Navigator, expand **gold** schema
6. Select all tables:
   - `dim_date`, `dim_datetime`, `dim_apartment`, `dim_room`, `dim_device`, `dim_tariff`, `dim_weather_site`
   - `fact_energy_minute`, `fact_environment_minute`, `fact_presence_minute`, `fact_device_health_day`
   - `fact_weather_hour`
   - `mv_energy_with_cost`

## Data Model Relationships

Power BI should auto-detect most relationships from the FK constraints.
Verify these exist (Modeling > Manage Relationships):

| From                          | To                              | Cardinality |
|-------------------------------|----------------------------------|-------------|
| fact_energy_minute.datetime_key | dim_datetime.datetime_key      | Many:1      |
| fact_energy_minute.date_key     | dim_date.date_key              | Many:1      |
| fact_energy_minute.device_key   | dim_device.device_key          | Many:1      |
| fact_energy_minute.room_key     | dim_room.room_key              | Many:1      |
| fact_energy_minute.apartment_key| dim_apartment.apartment_key    | Many:1      |
| fact_environment_minute.datetime_key | dim_datetime.datetime_key | Many:1      |
| fact_environment_minute.room_key     | dim_room.room_key         | Many:1      |
| fact_presence_minute.datetime_key    | dim_datetime.datetime_key | Many:1      |
| fact_presence_minute.room_key        | dim_room.room_key         | Many:1      |
| fact_weather_hour.datetime_key       | dim_datetime.datetime_key | Many:1      |
| fact_weather_hour.site_key           | dim_weather_site.site_key | Many:1      |
| fact_device_health_day.date_key      | dim_date.date_key         | Many:1      |
| fact_device_health_day.device_key    | dim_device.device_key     | Many:1      |
| dim_room.apartment_key              | dim_apartment.apartment_key| Many:1      |
| dim_device.room_key                 | dim_room.room_key          | Many:1      |
| dim_apartment.weather_site_key      | dim_weather_site.site_key  | Many:1      |
| mv_energy_with_cost.date_key        | dim_date.date_key          | Many:1      |

## Recommended Import Mode

- **Import** for all dimension tables (small, fast)
- **Import** for fact tables (needed for DAX calculations)
- Set scheduled refresh if publishing to Power BI Service

## Notes
- The `mv_energy_with_cost` view pre-joins energy with tariff data — use it for cost dashboards
- Filter by `dim_apartment.apartment_id` to compare jimmy vs jeremie
- Use `dim_date` hierarchy: Year > Month > Week > Day for drill-down
