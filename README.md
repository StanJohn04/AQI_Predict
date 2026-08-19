# Automated Environmental Air Quality Data Pipeline

An automated data engineering pipeline that collects, stores, and processes **hourly** air
quality and weather data for three locations around Savannah, Georgia. A scheduled Python job
fetches from the Google Air Quality and Open-Meteo APIs, transforms the responses, and loads them
into a local PostgreSQL database — building a time series intended for analysis and AQI
forecasting.

## Key Features

* **Hourly resolution.** Observations are stored per hour in UTC; daily figures are derived by a
  SQL view rather than stored, so the within-day signal that AQI forecasting depends on is never
  averaged away at ingest.
* **Automated ETL** — runs unattended via Windows Task Scheduler, re-fetching a trailing 3-day
  window so a missed run heals itself.
* **Multi-source API integration** — the Google Air Quality API (two AQI indexes + six
  pollutants) combined with Open-Meteo (temperature, precipitation, wind, humidity, pressure).
* **Idempotent loading** — `ON CONFLICT (location_id, observed_at) DO UPDATE` with `COALESCE`,
  so any date can be re-run without creating duplicates or blanking data already held.
* **Loud failures** — every failure path logs to `logs/` and returns a non-zero exit code that
  the batch wrapper propagates to Task Scheduler.
* **Tested transforms** — the JSON parsing is pure and covered by pytest against real captured
  API responses.

## Tech Stack

| | |
|---|---|
| Language | Python 3.12 (Anaconda) |
| Libraries | Pandas, SQLAlchemy, Requests, python-dotenv, pytest |
| Database | PostgreSQL 17 |
| Automation | Windows Task Scheduler + batch script |
| Analysis | Jupyter, Matplotlib, Seaborn |

## System Architecture

Everything runs on one machine: the same Windows PC hosts the PostgreSQL server and executes the
ETL job.

```text
[ Google Air Quality API ]      [ Open-Meteo API ]
            |                           |
            +------------+--------------+
                         |  1. Fetch raw JSON
                         v
              [ Python ETL pipeline ]
                         |  2. Transform to one row per location per HOUR (UTC)
                         v
            [ PostgreSQL: hourly_readings ]
                         |  3. Aggregate by true local date
                         v
            [ view: daily_readings_daily ]
```

### Repository layout

```text
scripts/
  common.py           Config, LOCATIONS, logging, HTTP (timeout/retry/error body), DB engine
  transform.py        Pure JSON -> row dicts. No I/O. The tested module.
  sources.py          Network calls to Google Air Quality and Open-Meteo
  load.py             The single upsert statement and location lookup
  fetch.py            CLI entry point; owns exit codes
  load_raw.py         Loads the rescued captures in Data/raw into the database
  schema_hourly.sql   Additive migration — safe to re-run
  reset_database.sql  DESTRUCTIVE from-scratch reset — drops both tables
  create_etl_role.sql One-time creation of the least-privilege aqi_etl role
  run_etl.bat         Task Scheduler entry point
tests/
  test_transform.py   Transform tests
  fixtures/           Real API responses used as fixtures
notebooks/
  01_data_exploration.ipynb
Docs/
  Docs.pdf            Full project documentation
```

## Database Schema

**`locations`** — one row per collection site, unique on `(city, country)`, with a `timezone`
column used to derive local days.

**`hourly_readings`** — the time series, unique on `(location_id, observed_at)`:

| Column | Description |
|---|---|
| `observed_at` | Hour of observation, **UTC** |
| `uaqi` | Google universal AQI index |
| `usa_epa` | US EPA AQI index — generally the more useful one here |
| `pm10`, `pm25`, `o3`, `no2`, `co`, `so2` | Pollutant concentrations |
| `temperature_celsius`, `precipitation_mm`, `wind_speed_kmh` | Weather |
| `relative_humidity_pct`, `surface_pressure_hpa` | Weather |
| `weather_source` | `archive` (reanalysis) or `forecast` (recent tail) |

**`daily_readings_daily`** (view) — daily aggregation by true local date, with `hours_present`
and `hours_with_aqi` so coverage is visible rather than assumed. Temperature is a mean,
precipitation a sum, wind a maximum.

**`daily_readings`** (legacy table) — frozen, no longer written to. See below.

> ### Data semantics — read before modelling
>
> The dataset has a **regime boundary at 2026-03-17**.
>
> | Period | State |
> |---|---|
> | 2025-06-08 .. 2026-03-16 | `daily_readings`. Internally inconsistent: rows from the old daily job are point-in-time AQI readings with *forecast* weather, while backfilled rows are 24-hour means with *observed* weather — in the same columns. It cannot be un-mixed, so the table is frozen rather than migrated. |
> | 2026-03-17 .. 2026-07-20 | **Permanently absent.** A credential failure stopped collection and the outage went unnoticed because failures were silent. Google's history API reaches back only 30 days, so this window is unrecoverable. |
> | 2026-07-21 onward | `hourly_readings`. Consistent hourly observations. |

## Getting Started

### 1. Clone

```bash
git clone https://github.com/StanJohn04/AQI_Predict.git
```

### 2. Create the environment

```bash
conda create --name AQI_Predict python=3.12
```

```bash
conda activate AQI_Predict
```

```bash
pip install -r requirements.txt
```

### 3. Set up PostgreSQL

Create the database, then create the dedicated ETL role. The role script prompts for the
password interactively, so it never lands in a file or your shell history:

```bash
psql -U postgres -f scripts/create_etl_role.sql
```

Then build the schema. `schema_hourly.sql` is **additive and safe to re-run**:

```bash
psql -U postgres -d aqi_db -f scripts/schema_hourly.sql
```

> `scripts/reset_database.sql` drops both tables. It is a from-scratch reset, not a migration —
> never run it against a populated database.

### 4. Configure credentials

Create a `.env` file in the project root (it is gitignored):

```text
GOOGLE_API_KEY=your_key
DB_HOST=localhost
DB_PORT=5432
DB_NAME=aqi_db
DB_USER=aqi_etl
DB_PASSWORD=your_password
```

The Google API key needs the **Air Quality API** enabled on a billing-enabled Google Cloud
project.

Use the dedicated `aqi_etl` role rather than a personal one. The pipeline previously shared a
personal role with other projects, and an unrelated password change took it offline for five
months.

### 5. Populate and schedule

Backfill the recoverable history, then let the scheduled job take over:

```bash
python scripts/fetch.py --days 30
```

Point a Windows Task Scheduler task at `scripts\run_etl.bat` to run once a day. It derives
the repo root from its own location, so no editing is needed. If Anaconda is not at
`%USERPROFILE%\anaconda3`, set `CONDA_ROOT` in the environment to point at your install.

## Operations

```bash
python scripts/fetch.py                              # trailing 3-day catch-up (the scheduled run)
```

```bash
python scripts/fetch.py --date 2026-08-18            # one specific day
```

```bash
python scripts/fetch.py --start 2026-08-01 --end 2026-08-10   # a range
```

```bash
python scripts/fetch.py --days 30                    # the full recoverable window
```

Every run is idempotent, so re-running any date is harmless.

### Diagnosing a failed run

Failures are visible in three places, in increasing detail:

1. **Task Scheduler** — "last run result" is now accurate; non-zero means something failed.
2. **`logs/run_etl-<date>.log`** — everything the scheduled run printed.
3. **`logs/aqi_etl-<date>.log`** — the pipeline's own log, including HTTP response bodies, which
   is where Google reports the real cause of a 4xx (key expired, quota, billing, API disabled).

### API limits worth knowing

* Google's history endpoint reaches back **exactly 30 days**. Gaps must be repaired promptly;
  past that they are permanent.
* A history window may not end **less than ~2 hours in the past**, so today's data always
  arrives truncated. The trailing catch-up window fills it in on subsequent runs.
* Google intermittently returns HTTP 200 with every pollutant concentration null. `sources.py`
  detects the low coverage and re-requests; without that, roughly 45% of pollutant values are
  silently lost.

## Tests

```bash
pytest tests/ -v
```

The tests cover `scripts/transform.py` using real captured API responses as fixtures — that
module is deliberately pure, and it is where every subtle bug in this project's history has been.

## Full Documentation

For architecture details, implementation notes, and planned enhancements, see the
[Project Documentation PDF](Docs/Docs.pdf). *(The PDF still describes the pre-2026-08-19 daily
architecture and needs regenerating.)*
