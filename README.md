# Automated Environmental Air Quality Data Pipeline

An automated data engineering pipeline that collects, stores, and processes daily air quality
and weather data for three locations around Savannah, Georgia. A scheduled Python job fetches
from the Google Air Quality and Open-Meteo APIs, transforms the responses, and loads them into a
local PostgreSQL database — building a time series intended for analysis and AQI forecasting.

## Key Features

* **Automated daily ETL** — runs unattended via Windows Task Scheduler.
* **Multi-source API integration** — combines the Google Air Quality API (AQI + six pollutants)
  with the Open-Meteo API (temperature, precipitation, wind).
* **Idempotent loading** — `ON CONFLICT (location_id, reading_date) DO UPDATE` means a date can
  be re-run or re-fetched without creating duplicates.
* **Historical backfill and patching** — dedicated scripts fill the initial ~30 days and repair
  individual missing dates, averaging hourly readings into daily summaries.

## Tech Stack

| | |
|---|---|
| Language | Python 3.12 (Anaconda) |
| Libraries | Pandas, SQLAlchemy, Requests, python-dotenv |
| Database | PostgreSQL |
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
              [ Python ETL script ]
                         |  2. Transform to one row per location per day
                         v
            [ PostgreSQL: aqi_db ]
```

### Repository layout

```text
scripts/
  etl.py                  Daily job — current conditions for each location
  historical_backfill.py  One-time backfill of the last N days (Google caps history at ~30)
  historical_patch.py     Re-fetch specific dates listed in the script
  database_setup.sql      Schema reset — DROPS both tables, then recreates them
  run_etl.bat             Task Scheduler entry point (activates conda, runs etl.py)
notebooks/
  01_data_exploration.ipynb
Docs/
  Docs.pdf                Full project documentation
```

## Database Schema

**`locations`** — one row per collection site, unique on `(city, country)`.

**`daily_readings`** — the time series, unique on `(location_id, reading_date)`:

| Column | Description |
|---|---|
| `aqi` | Universal AQI (Google `uaqi` index) |
| `pm10`, `pm25`, `o3`, `no2`, `co`, `so2` | Pollutant concentrations |
| `temperature_celsius` | Daily mean |
| `precipitation_mm` | Daily sum |
| `wind_speed_kmh` | Daily maximum |

> **Note on data semantics.** The daily job stores a *point-in-time* AQI reading and the day's
> *forecast* weather; the historical scripts store a *24-hour mean* AQI and *observed* archive
> weather. Rows written by the two paths are not directly comparable even though they share the
> same columns. Account for this before training a model on the table.

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

`requirements.txt` is a full `pip freeze` of the original conda environment and contains local
build paths, so it will not install on another machine. Install the direct dependencies instead:

```bash
pip install requests pandas sqlalchemy psycopg2-binary python-dotenv matplotlib seaborn jupyterlab
```

### 3. Set up PostgreSQL

Create a database and a user for the project, then build the schema. `database_setup.sql` drops
`daily_readings` and `locations` first — only run it on an empty or disposable database.

```bash
psql -U your_user -d your_db_name -f scripts/database_setup.sql
```

### 4. Configure credentials

Create a `.env` file in the project root (it is gitignored):

```text
GOOGLE_API_KEY=your_key
DB_HOST=localhost
DB_PORT=5432
DB_NAME=aqi_db
DB_USER=your_user
DB_PASSWORD=your_password
```

The Google API key needs the **Air Quality API** enabled on a billing-enabled Google Cloud
project.

### 5. Populate and schedule

Backfill history first, then let the daily job take over:

```bash
python scripts/historical_backfill.py
```

Point a Windows Task Scheduler task at `scripts\run_etl.bat` to run once a day. The batch file
contains absolute paths to Anaconda and to this checkout — edit them for your machine.

## Operations

Run the daily job manually at any time; it is idempotent for the current date:

```bash
python scripts/etl.py
```

To fill a gap, add the dates to the `dates_to_process` list at the bottom of
`historical_patch.py` and run it. Google's history endpoint only reaches back about 30 days, so
gaps must be repaired promptly.

**Known limitation:** the scripts report errors to stdout and always exit 0, and `run_etl.bat`
does not capture output. A failed run therefore looks identical to a successful one in Task
Scheduler history. To see what a scheduled run actually did, run it from a terminal and read the
output, or redirect the batch file's output to a log file.

## Full Documentation

For architecture details, implementation notes, and planned enhancements, see the
[Project Documentation PDF](Docs/Docs.pdf).
