# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-machine ETL pipeline that pulls hourly air-quality and weather data for three cities
near Savannah, GA into a local PostgreSQL database, so the resulting time series can be used for
AQI forecasting. There is no application, service, or API — just scripts run on a schedule plus a
notebook for exploration.

## Commands

All Python runs from the `AQI_Predict` conda environment (Python 3.12):

```bash
conda activate AQI_Predict
```

| Task | Command |
|---|---|
| Daily pull (trailing 3-day catch-up) | `python scripts/fetch.py` |
| Scheduled entry point (Task Scheduler) | `scripts\run_etl.bat` |
| One specific day | `python scripts/fetch.py --date 2026-08-18` |
| A date range | `python scripts/fetch.py --start 2026-08-01 --end 2026-08-10` |
| Last N days | `python scripts/fetch.py --days 30` |
| Load rescued raw captures | `python scripts/load_raw.py` |
| Apply schema (**additive, safe**) | `psql -U postgres -d aqi_db -f scripts/schema_hourly.sql` |
| Create the ETL role (one-time) | `psql -U postgres -f scripts/create_etl_role.sql` |
| Tests | `pytest tests/ -v` |
| Exploration notebook | `jupyter lab notebooks/01_data_exploration.ipynb` |

There is no linter or CI. `pytest` covers `scripts/transform.py` only — deliberately, because
that is where every subtle bug in this project's history has lived.

## Configuration

Credentials come from `.env` in the repo root (gitignored), loaded via `python-dotenv`:
`GOOGLE_API_KEY`, `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`.

The pipeline authenticates as a dedicated `aqi_etl` role with only SELECT/INSERT/UPDATE on the
two tables. **Do not point it back at a personal superuser role.** It used to use the shared
`stant` role, and on 2026-03-17 an unrelated project's `ALTER ROLE stant WITH PASSWORD ...`
invalidated the credential and took the pipeline down for five months.

`run_etl.bat` derives its paths: `%~dp0..` gives the repo root from the script's own location,
and Anaconda is looked up at `%USERPROFILE%\anaconda3` unless `CONDA_ROOT` is set in
the environment. It names no user and no machine, and exits 1 if conda is not found where
expected.

## Architecture

```
scripts/
  common.py      config, LOCATIONS, logging, HTTP (timeout+retry+error body), DB engine
  transform.py   pure JSON -> row dicts. No I/O. This is the tested module.
  sources.py     network calls to Google Air Quality and Open-Meteo
  load.py        the single upsert statement and location lookup
  fetch.py       CLI entry point; owns exit codes
  load_raw.py    loads the rescued captures in Data/raw into the database
```

Each module has one job and nothing is duplicated. A schema or location change is a one-file
edit. (Before 2026-08-19 the three entry scripts each carried their own copy of `LOCATIONS`,
`get_db_engine`, `load_data` and the full INSERT — a change had to be made in three places and
nothing detected a missed one.)

### Data model: hourly is the source of truth

`hourly_readings` stores one row per `(location_id, observed_at)` with `observed_at` in **UTC**.
Daily figures are derived by the `daily_readings_daily` view, which groups by true local date
using `locations.timezone`. Nothing stores a daily aggregate.

The view mirrors the original daily semantics on purpose: mean temperature, **summed**
precipitation, **max** wind.

Both AQI indexes are kept: `uaqi` (Google's universal index) and `usa_epa`. The latter is
generally the more useful one for a US location and was previously discarded.

### `daily_readings` is legacy — do not write to it

It holds 2025-06-08 .. 2026-03-16 and is **not internally consistent**: rows written by the old
daily job are instantaneous snapshots paired with *forecast* weather, while backfilled rows are
24-hour means paired with *observed* weather, in the same columns. This cannot be un-mixed, so
the table is frozen rather than migrated.

**Any model must treat 2026-03-17 as a regime boundary.** Timeline:

| Period | State |
|---|---|
| 2025-06-08 .. 2026-03-16 | `daily_readings`, mixed snapshot/mean semantics |
| 2026-03-17 .. 2026-07-20 | **permanently absent** — outage, past Google's 30-day horizon |
| 2026-07-21 onward | `hourly_readings`, consistent hourly observations |

### External API shapes

- **Google Air Quality.** `history:lookup` nests hourly records under **`hoursInfo`** (not
  `hours` — that was a real bug once). Records come back **newest-first**, so nothing may depend
  on position; `transform.py` keys everything by absolute timestamp instead. AQI is read from
  the entries whose `code` is `uaqi` and `usa_epa`.
  - **History reaches back exactly 30 days.** Day 30 returns HTTP 400. Anything older is gone.
  - **A window may not end less than ~2 hours in the past** — otherwise HTTP 400. This is why
    today's data always arrives truncated.
  - **It intermittently returns 200 with every `concentration` null.** Not an error status; an
    identical re-request returns the data. `sources.fetch_google_day` retries on low
    concentration coverage for exactly this reason. Without that retry roughly 45% of pollutant
    values are silently lost.
  - `concentration` arrives as JSON `null`, not as a missing key.
  - The exclusive end-of-window hour comes back with pollutant codes but null values; the
    upsert's `COALESCE` stops it blanking good data from the adjacent day.
- **Open-Meteo.** Queried with `timezone: auto`, so `hourly.time` is naive **local** time;
  `utc_offset_seconds` converts it exactly. The `archive` endpoint lags several days behind real
  time, so `sources.fetch_weather` uses `forecast` with `past_days` for anything recent and
  tags the row with `weather_source` accordingly.

### Failure model — everything is loud

Every failure path logs and propagates. `fetch.py` returns non-zero if *any* location-day fails;
`run_etl.bat` redirects stdout and stderr to `logs/run_etl-<date>.log` and exits with Python's
code, so **Task Scheduler's "last run result" is now trustworthy.** All `requests` calls have a
30-second timeout and bounded retries, and HTTP errors log the response body — which is where
Google puts the actual reason for a 4xx (key expired, quota, billing, API not enabled).

The daily run re-fetches a trailing 3-day window, so a missed run self-heals on the next one.
The upsert is idempotent per `(location_id, observed_at)`, making any re-run harmless.

## Repo conventions

- `Docs/Docs.md` is **gitignored** (`docs.md` pattern); only `Docs/Docs.pdf` is tracked. Edits to
  the markdown will not show up in `git status`. **It still describes the pre-2026-08-19
  architecture and needs regenerating.**
- `.gitignore` contains `data/`, which on case-insensitive Windows also excludes `Data/` — this
  is deliberate: `Data/raw/` holds ~12 MB of captured API responses.
- `Data/raw/` is the rescued 2026-07-21..2026-08-19 window, kept verbatim. It is the only copy
  of that data outside the database and doubles as the source for `tests/fixtures/`.
- `scripts/reset_database.sql` (formerly `database_setup.sql`) **drops both tables**. It is a
  from-scratch reset, not a migration. Use `schema_hourly.sql`, which is additive and re-runnable.
- Commit messages follow Conventional Commits (`feat:`, `fix:`, `chore:`, `test:`, `docs:`).
