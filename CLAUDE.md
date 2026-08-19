# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-machine ETL pipeline that pulls daily air-quality and weather data for three cities
near Savannah, GA into a local PostgreSQL database, so the resulting time series can later be
used for AQI forecasting. There is no application, service, or API — just scripts run on a
schedule plus a notebook for exploration.

## Commands

All Python runs from the `AQI_Predict` conda environment (Python 3.12):

```bash
conda activate AQI_Predict
```

| Task | Command |
|---|---|
| Daily pull (today, all locations) | `python scripts/etl.py` |
| Scheduled entry point (used by Task Scheduler) | `scripts\run_etl.bat` |
| Backfill last N days | `python scripts/historical_backfill.py` |
| Patch specific missing dates | `python scripts/historical_patch.py` |
| Create/reset schema (**drops all data**) | `psql -U <user> -d <db> -f scripts/database_setup.sql` |
| Exploration notebook | `jupyter lab notebooks/01_data_exploration.ipynb` |

No tests, linter, or CI exist in this repo.

Both historical scripts take their date range from **hardcoded constants in `__main__`**, not
CLI arguments — `DAYS_OF_HISTORY` in `historical_backfill.py`, the `dates_to_process` list in
`historical_patch.py`. Changing what they fetch means editing the source.

## Configuration

Credentials come from `.env` in the repo root (gitignored), loaded via `python-dotenv`:
`GOOGLE_API_KEY`, `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`.

`run_etl.bat` hardcodes absolute paths to Anaconda and to this checkout. It only works on the
machine it was written for.

## Architecture

### The three scripts are forks of each other

`etl.py`, `historical_backfill.py`, and `historical_patch.py` each carry their **own copy** of
`LOCATIONS`, `get_db_engine()`, `load_data()`, and the full `INSERT ... ON CONFLICT` statement.
Nothing is shared. A schema or location change must be applied in all three files or they will
silently diverge. `historical_patch.py` is a near-verbatim fork of `historical_backfill.py`
with a different weather-fetch strategy and hardcoded dates.

### The same columns hold two different quantities

This is the most important thing to know before doing any analysis or modeling on this data:

- **`etl.py`** calls Google's `currentConditions:lookup` and stores that **single instantaneous
  reading** as the day's value, paired with Open-Meteo's `forecast` endpoint — i.e. the day's
  *predicted* weather.
- **`historical_*.py`** call `history:lookup`, load the 24 hourly records into a DataFrame, and
  store the **daily mean**, paired with Open-Meteo's `archive` endpoint — i.e. *observed*
  weather.

So backfilled rows and daily rows in `daily_readings` are not comparable, even though they sit
in the same columns. Reprocessing a date with the historical script overwrites the snapshot with
a mean (the `ON CONFLICT DO UPDATE` makes the load idempotent per `(location_id, reading_date)`).

### External API shapes

- **Google Air Quality.** `currentConditions:lookup` returns `indexes` and `pollutants` at the
  top level; `history:lookup` nests them per hour under **`hoursInfo`** (not `hours` — that was
  a real bug, see the comment in `historical_backfill.py`). AQI is read from the entry whose
  `code == 'uaqi'`; the `LOCAL_AQI` extra computation also returns a `usa_epa` index that is
  currently ignored. History lookback is capped at ~30 days.
- **Open-Meteo.** The `archive` endpoint lags several days behind real time. `historical_patch.py`
  branches on `days_ago < 3` to use `forecast` with `past_days` for recent dates, and reads
  `[-1]` from the daily arrays because `past_days` returns a range, not a single day. The
  backfill script reads `[0]` because its archive query returns exactly one day. Getting this
  index wrong silently attributes the wrong day's weather to a row.

### Day boundaries are inconsistent

`reading_date` is the host machine's local date. The historical scripts build the Google query
window as `datetime.combine(date, time.min).isoformat() + "Z"` — local midnight labelled as UTC,
so the 24-hour window is actually offset by the local UTC offset. Open-Meteo is queried with
`timezone: auto`, which aggregates by true local day. The AQI mean and the weather values on a
given row therefore cover different spans.

### Failure model — everything is silent

No script raises or exits nonzero. Every failure path (`get_db_engine`, both fetch functions,
`transform_data`) prints a message and returns `None`; `__main__` skips the row and keeps going,
then exits 0. `requests` calls have no timeout and no retry. `run_etl.bat` does not redirect
output to a log and does not propagate Python's exit code, so **Windows Task Scheduler records
every run as successful regardless of what happened.** When diagnosing a missed pull, the
scheduler history and the database are both useless as evidence — run the script by hand and
read stdout, or add logging first.

Note also that `raise_for_status()` errors are printed without the response body, which is where
Google puts the actual reason for a 4xx (key expired, quota, billing, API not enabled).

## Schema

Two tables — `locations` (city/country unique) and `daily_readings` (one row per
`location_id` + `reading_date`, with `aqi`, six pollutants, and three weather columns).

`scripts/database_setup.sql` begins with `DROP TABLE IF EXISTS` on both tables. It is a reset
script, not a migration — never run it against the populated database. Its column list also
disagrees with `Docs/Docs.md` (`aqi INTEGER` vs `aqi REAL`); verify against the live database
before trusting either, since the historical scripts write a float mean into `aqi`.

## Repo conventions

- `Docs/Docs.md` is **gitignored** (`docs.md` pattern); only `Docs/Docs.pdf` is tracked. Edits
  to the markdown will not show up in `git status`.
- `.gitignore` contains `data/`, which on case-insensitive Windows also excludes the `Data/`
  directory.
- `requirements.txt` is a raw `pip freeze` from a conda environment — 86 of its 110 entries are
  `@ file:///C:/...` local build paths and will not install anywhere else. The real direct
  dependencies are `requests`, `pandas`, `sqlalchemy`, `psycopg2`, `python-dotenv`, plus
  `matplotlib`/`seaborn`/`jupyter` for the notebook.
- Commit messages follow Conventional Commits (`feat:`, `fix:`, `chore:`).
