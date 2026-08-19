# AQI_Predict Hourly Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this
> plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace three forked scripts and a snapshot/mean-mixing daily table with one unified,
loudly-failing pipeline that stores hourly observations and derives daily figures as a view.

**Architecture:** A new `hourly_readings` table keyed on `(location_id, observed_at)` in UTC
becomes the single source of truth; daily figures come from a `daily_readings_daily` view that
groups by true local date. One CLI (`scripts/fetch.py`) handles today, a date, or a range, and
always re-fetches a trailing window so a missed run self-heals. Pure transform functions live in
`scripts/transform.py` and are covered by pytest using the rescued API responses as fixtures.

**Tech Stack:** Python 3.12 (conda env `AQI_Predict`), PostgreSQL 17, SQLAlchemy 2.x, psycopg2,
requests, python-dotenv, pytest.

## Global Constraints

- All Python runs from the `AQI_Predict` conda env (`conda activate AQI_Predict`);
  the commands below assume it is active. Do not hardcode an interpreter path.
- Credentials come only from `.env` (gitignored). Never hardcode or log a password or API key.
- Google `history:lookup` reaches back **exactly 30 days** and rejects any window ending less
  than **~2 hours** in the past. Both limits are load-bearing; encode them, don't rediscover them.
- Every failure path must log and cause a non-zero exit. No silent `return None` + `exit 0`.
- Every `requests` call passes `timeout=30` and logs `response.text` on a non-2xx status.
- Existing `daily_readings` is **legacy**: never dropped, never written to again, kept for the
  2025-06-08 .. 2026-03-16 history it holds.
- Migrations are additive. `database_setup.sql`'s `DROP TABLE` statements must not run again.
- Commit messages use Conventional Commits (`feat:`, `fix:`, `chore:`, `test:`, `docs:`).

---

## File Structure

| File | Responsibility |
|---|---|
| `scripts/schema_hourly.sql` | Create (additive) `hourly_readings`, `locations.timezone`, daily view |
| `scripts/common.py` | Config, `LOCATIONS`, logging setup, HTTP helper, DB engine |
| `scripts/transform.py` | Pure functions: raw API JSON to hourly row dicts. No I/O |
| `scripts/sources.py` | Network fetch for Google AQI + Open-Meteo. Thin, no parsing logic |
| `scripts/load.py` | Upsert hourly rows; location lookup/create |
| `scripts/fetch.py` | CLI entry point tying the above together; owns exit codes |
| `scripts/load_raw.py` | Load the rescued `Data/raw/**` captures into the DB |
| `scripts/run_etl.bat` | Scheduler entry: activates env, redirects to log, propagates exit code |
| `tests/test_transform.py` | pytest over `transform.py` using rescued responses as fixtures |
| `tests/fixtures/` | Small committed subset of real API responses |

Retired at the end: `etl.py`, `historical_backfill.py`, `historical_patch.py`.

---

### Task 1: Additive schema — hourly table, location timezone, daily view

**Files:**
- Create: `scripts/schema_hourly.sql`
- Test: manual verification query (no pytest; this is DDL)

**Interfaces:**
- Produces: table `hourly_readings(location_id, observed_at, uaqi, usa_epa, pm25, pm10, o3, no2,
  co, so2, temperature_celsius, precipitation_mm, wind_speed_kmh, relative_humidity_pct,
  surface_pressure_hpa, weather_source, ingested_at)`; column `locations.timezone`;
  view `daily_readings_daily`.

- [ ] **Step 1: Write the migration**

```sql
-- scripts/schema_hourly.sql
-- ADDITIVE migration. Contains no DROP statements and is safe to re-run.
-- Legacy table daily_readings is intentionally left untouched.

ALTER TABLE locations
    ADD COLUMN IF NOT EXISTS timezone TEXT NOT NULL DEFAULT 'America/New_York';

CREATE TABLE IF NOT EXISTS hourly_readings (
    id                    BIGSERIAL PRIMARY KEY,
    location_id           INTEGER NOT NULL REFERENCES locations(id),
    -- Always UTC. The local day is derived in the view via locations.timezone,
    -- which is what fixes the old local-midnight-labelled-as-Z bug (F5).
    observed_at           TIMESTAMPTZ NOT NULL,
    uaqi                  INTEGER,
    usa_epa               INTEGER,
    pm25                  REAL,
    pm10                  REAL,
    o3                    REAL,
    no2                   REAL,
    co                    REAL,
    so2                   REAL,
    temperature_celsius   REAL,
    precipitation_mm      REAL,
    wind_speed_kmh        REAL,
    relative_humidity_pct REAL,
    surface_pressure_hpa  REAL,
    -- 'archive' (reanalysis) or 'forecast' (recent tail); they differ slightly.
    weather_source        TEXT,
    ingested_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (location_id, observed_at)
);

CREATE INDEX IF NOT EXISTS hourly_readings_observed_at_idx
    ON hourly_readings (observed_at);

-- Daily figures are DERIVED, never stored. Aggregations deliberately mirror the
-- old Open-Meteo daily semantics: mean temperature, summed precipitation,
-- max wind -- so the view is comparable with the legacy daily_readings rows.
CREATE OR REPLACE VIEW daily_readings_daily AS
SELECT
    h.location_id,
    (h.observed_at AT TIME ZONE l.timezone)::date       AS reading_date,
    count(*)                                            AS hours_present,
    count(h.uaqi)                                       AS hours_with_aqi,
    avg(h.uaqi)::real                                   AS uaqi_mean,
    max(h.uaqi)                                         AS uaqi_max,
    avg(h.usa_epa)::real                                AS usa_epa_mean,
    max(h.usa_epa)                                      AS usa_epa_max,
    avg(h.pm25)::real                                   AS pm25,
    avg(h.pm10)::real                                   AS pm10,
    avg(h.o3)::real                                     AS o3,
    avg(h.no2)::real                                    AS no2,
    avg(h.co)::real                                     AS co,
    avg(h.so2)::real                                    AS so2,
    avg(h.temperature_celsius)::real                    AS temperature_celsius,
    sum(h.precipitation_mm)::real                       AS precipitation_mm,
    max(h.wind_speed_kmh)::real                         AS wind_speed_kmh,
    avg(h.relative_humidity_pct)::real                  AS relative_humidity_pct,
    avg(h.surface_pressure_hpa)::real                   AS surface_pressure_hpa
FROM hourly_readings h
JOIN locations l ON l.id = h.location_id
GROUP BY h.location_id, (h.observed_at AT TIME ZONE l.timezone)::date;

COMMENT ON TABLE hourly_readings IS
    'Hourly observations, UTC. Single source of truth; daily figures come from daily_readings_daily.';
COMMENT ON VIEW daily_readings_daily IS
    'Daily aggregation of hourly_readings by true local date. hours_with_aqi gives coverage.';
```

- [ ] **Step 2: Apply it**

```bash
psql -U postgres -d aqi_db -f scripts/schema_hourly.sql
```

Expected: `ALTER TABLE`, `CREATE TABLE`, `CREATE INDEX`, `CREATE VIEW`, `COMMENT`, no errors.

- [ ] **Step 3: Verify the objects exist and the view is queryable**

```bash
psql -U postgres -d aqi_db -c "\d hourly_readings" -c "select count(*) from daily_readings_daily;"
```

Expected: column list as above; count returns `0` (no rows loaded yet).

- [ ] **Step 4: Commit**

```bash
git add scripts/schema_hourly.sql
git commit -m "feat: add hourly_readings table and derived daily view"
```

---

### Task 2: Shared foundation — config, logging, HTTP, engine

**Files:**
- Create: `scripts/common.py`

**Interfaces:**
- Produces: `LOCATIONS: list[dict]` (keys `city`, `country`, `latitude`, `longitude`, `timezone`);
  `setup_logging(name: str) -> logging.Logger`; `get_engine() -> sqlalchemy.Engine` (raises on
  failure); `request_json(method: str, url: str, **kw) -> dict` (raises `FetchError` after
  retries); `class FetchError(Exception)`; `REPO_ROOT: Path`;
  `GOOGLE_HISTORY_MAX_AGE_DAYS = 30`; `GOOGLE_HISTORY_MIN_LAG_HOURS = 2`.

- [ ] **Step 1: Write `scripts/common.py`**

```python
"""Shared configuration, logging, HTTP and database plumbing for the AQI pipeline.

Everything that used to be copy-pasted across etl.py / historical_backfill.py /
historical_patch.py lives here exactly once.
"""
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

LOG_DIR = REPO_ROOT / "logs"

# Google's history endpoint reaches back exactly 30 days and refuses any window
# ending less than ~2 hours ago. Both were measured against the live API on
# 2026-08-19; see Docs/code-review-2026-08-04.md.
GOOGLE_HISTORY_MAX_AGE_DAYS = 30
GOOGLE_HISTORY_MIN_LAG_HOURS = 2

HTTP_TIMEOUT = 30
HTTP_RETRIES = 3

LOCATIONS = [
    {"city": "Savannah",       "country": "USA", "latitude": 32.0809, "longitude": -81.0912,
     "timezone": "America/New_York"},
    {"city": "Port Wentworth", "country": "USA", "latitude": 32.17,   "longitude": -81.17,
     "timezone": "America/New_York"},
    {"city": "Pooler",         "country": "USA", "latitude": 32.11,   "longitude": -81.25,
     "timezone": "America/New_York"},
]


class FetchError(Exception):
    """A network or API failure that the caller must not silently swallow."""


def setup_logging(name: str) -> logging.Logger:
    """Log to both stdout and logs/<name>-YYYY-MM-DD.log."""
    LOG_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s")

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    logger.addHandler(stream)

    fileh = logging.FileHandler(LOG_DIR / f"{name}-{date.today().isoformat()}.log",
                                encoding="utf-8")
    fileh.setFormatter(fmt)
    logger.addHandler(fileh)
    return logger


def get_engine() -> Engine:
    """Build the engine and prove the connection works. Raises on any failure."""
    missing = [k for k in ("DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME")
               if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required .env variables: {', '.join(missing)}")

    # URL.create quotes the password properly, so special characters can never
    # corrupt the connection string the way the old f-string allowed.
    url = URL.create(
        "postgresql+psycopg2",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        database=os.getenv("DB_NAME"),
    )
    engine = create_engine(url, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return engine


def request_json(method: str, url: str, **kwargs) -> dict:
    """HTTP with a timeout, bounded retries, and the response body on error.

    The old code printed only the exception, which for a 4xx omits the body --
    exactly where Google puts the real reason (F2).
    """
    kwargs.setdefault("timeout", HTTP_TIMEOUT)
    last = ""
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            resp = requests.request(method, url, **kwargs)
            if resp.status_code == 200:
                return resp.json()
            last = f"HTTP {resp.status_code}: {resp.text[:500]}"
            # 4xx other than rate-limiting will not improve on retry.
            if 400 <= resp.status_code < 500 and resp.status_code != 429:
                raise FetchError(f"{url} -> {last}")
        except requests.exceptions.RequestException as exc:
            last = f"request error: {exc}"
        if attempt < HTTP_RETRIES:
            time.sleep(2 * attempt)
    raise FetchError(f"{url} failed after {HTTP_RETRIES} attempts -> {last}")
```

- [ ] **Step 2: Verify it imports and connects**

```bash
python -c "import sys; sys.path.insert(0,'scripts'); import common; print(len(common.LOCATIONS)); print(common.get_engine())"
```

Expected: `3` then an `Engine(postgresql+psycopg2://...)` line, no traceback.

- [ ] **Step 3: Commit**

```bash
git add scripts/common.py
git commit -m "feat: add shared config, logging, HTTP and engine helpers"
```

---

### Task 3: Pure transforms, test-first

**Files:**
- Create: `scripts/transform.py`, `tests/test_transform.py`, `tests/fixtures/`

**Interfaces:**
- Consumes: nothing from earlier tasks (deliberately dependency-free so it stays unit-testable).
- Produces:
  - `google_hours(payload: dict) -> list[dict]` — each dict has `observed_at` (aware UTC
    `datetime`) plus `uaqi`, `usa_epa`, `pm25`, `pm10`, `o3`, `no2`, `co`, `so2` (any may be `None`).
  - `openmeteo_hours(payload: dict) -> dict[datetime, dict]` — maps aware UTC `datetime` to
    `temperature_celsius`, `precipitation_mm`, `wind_speed_kmh`, `relative_humidity_pct`,
    `surface_pressure_hpa`.
  - `merge_hours(aqi_rows: list[dict], weather: dict, weather_source: str) -> list[dict]` —
    rows ready for upsert, each carrying `weather_source`.

- [ ] **Step 1: Build fixtures from the rescued captures**

```bash
mkdir -p tests/fixtures
cp "Data/raw/google_aqi/Savannah/2026-08-01.json" tests/fixtures/google_day.json
cp "Data/raw/google_aqi/Savannah/2026-08-13.json" tests/fixtures/google_day_with_gaps.json
python -c "
import json
d=json.load(open('Data/raw/open_meteo/Savannah.archive.json',encoding='utf-8'))
h=d['hourly']; keep=slice(0,48)
d['hourly']={k:(v[keep] if isinstance(v,list) else v) for k,v in h.items()}
d.pop('daily',None); d.pop('daily_units',None)
json.dump(d,open('tests/fixtures/openmeteo_archive.json','w',encoding='utf-8'),indent=1)
"
```

- [ ] **Step 2: Write the failing tests**

```python
# tests/test_transform.py
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import transform  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"


def load(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_google_hours_extracts_both_indexes():
    rows = transform.google_hours(load("google_day.json"))
    assert len(rows) >= 24
    first = rows[0]
    assert first["observed_at"].tzinfo is not None
    assert first["observed_at"].utcoffset().total_seconds() == 0
    assert isinstance(first["uaqi"], int)
    # usa_epa was previously discarded even though it is the more useful US index (F12).
    assert isinstance(first["usa_epa"], int)
    assert first["pm25"] is not None


def test_google_hours_keeps_empty_hours_as_nulls():
    """Hours the API returns as bare timestamps must survive as NULL rows, not vanish."""
    rows = transform.google_hours(load("google_day_with_gaps.json"))
    empty = [r for r in rows if r["uaqi"] is None]
    assert empty, "fixture should contain at least one empty hour"
    assert all(r["observed_at"] is not None for r in empty)


def test_google_hours_on_empty_payload():
    assert transform.google_hours({"hoursInfo": []}) == []
    assert transform.google_hours({}) == []


def test_openmeteo_hours_converts_local_time_to_utc():
    data = load("openmeteo_archive.json")
    hours = transform.openmeteo_hours(data)
    # Fixture is America/New_York (utc_offset_seconds -14400); local 00:00 is 04:00Z.
    assert datetime(2026, 7, 21, 4, 0, tzinfo=timezone.utc) in hours
    sample = hours[datetime(2026, 7, 21, 4, 0, tzinfo=timezone.utc)]
    assert sample["temperature_celsius"] is not None
    assert "wind_speed_kmh" in sample


def test_merge_hours_joins_on_timestamp_and_tags_source():
    aqi = transform.google_hours(load("google_day.json"))
    weather = transform.openmeteo_hours(load("openmeteo_archive.json"))
    merged = transform.merge_hours(aqi, weather, "archive")
    assert len(merged) == len(aqi)
    assert all(r["weather_source"] == "archive" for r in merged)


def test_merge_hours_tolerates_missing_weather():
    aqi = transform.google_hours(load("google_day.json"))
    merged = transform.merge_hours(aqi, {}, "archive")
    assert len(merged) == len(aqi)
    assert all(r["temperature_celsius"] is None for r in merged)
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
python -m pytest tests/test_transform.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'transform'`.

- [ ] **Step 4: Write `scripts/transform.py`**

```python
"""Pure JSON -> row-dict transforms. No network, no database, no logging.

These functions are where every historically subtle bug has lived (the
hours/hoursInfo mixup, the [0] vs [-1] index divergence, the UTC window
offset), which is exactly why they are isolated and unit-tested.
"""
from datetime import datetime, timedelta, timezone

POLLUTANTS = ("pm25", "pm10", "o3", "no2", "co", "so2")


def _parse_utc(stamp: str) -> datetime:
    """Google returns RFC-3339 with a trailing Z."""
    return datetime.fromisoformat(stamp.replace("Z", "+00:00")).astimezone(timezone.utc)


def google_hours(payload: dict) -> list[dict]:
    """One row per hour from a history:lookup response.

    Hours the API returns as a bare timestamp with no indexes/pollutants are
    kept with NULL measurements: 'we asked and there was nothing' is a fact
    worth recording, and it keeps the daily view's coverage count honest.
    """
    rows = []
    for hour in payload.get("hoursInfo", []):
        stamp = hour.get("dateTime")
        if not stamp:
            continue
        indexes = {i.get("code"): i.get("aqi") for i in hour.get("indexes", []) or []}
        conc = {p.get("code"): (p.get("concentration") or {}).get("value")
                for p in hour.get("pollutants", []) or []}
        row = {
            "observed_at": _parse_utc(stamp),
            "uaqi": indexes.get("uaqi"),
            "usa_epa": indexes.get("usa_epa"),
        }
        row.update({p: conc.get(p) for p in POLLUTANTS})
        rows.append(row)
    return rows


def openmeteo_hours(payload: dict) -> dict:
    """Map aware-UTC hour -> weather values.

    Open-Meteo is queried with timezone=auto, so `hourly.time` is naive LOCAL
    time; utc_offset_seconds converts it exactly. Keying by absolute time is
    what removes the old [0] vs [-1] positional-index hazard entirely.
    """
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    offset = timedelta(seconds=payload.get("utc_offset_seconds", 0))

    def col(name):
        return hourly.get(name) or [None] * len(times)

    temp, precip = col("temperature_2m"), col("precipitation")
    wind, rh = col("wind_speed_10m"), col("relative_humidity_2m")
    pressure = col("surface_pressure")

    out = {}
    for i, local in enumerate(times):
        key = (datetime.fromisoformat(local) - offset).replace(tzinfo=timezone.utc)
        out[key] = {
            "temperature_celsius": temp[i],
            "precipitation_mm": precip[i],
            "wind_speed_kmh": wind[i],
            "relative_humidity_pct": rh[i],
            "surface_pressure_hpa": pressure[i],
        }
    return out


WEATHER_FIELDS = ("temperature_celsius", "precipitation_mm", "wind_speed_kmh",
                  "relative_humidity_pct", "surface_pressure_hpa")


def merge_hours(aqi_rows: list[dict], weather: dict, weather_source: str) -> list[dict]:
    """Attach weather to each AQI hour by absolute timestamp."""
    merged = []
    for row in aqi_rows:
        w = weather.get(row["observed_at"], {})
        out = dict(row)
        out.update({f: w.get(f) for f in WEATHER_FIELDS})
        out["weather_source"] = weather_source
        merged.append(out)
    return merged
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
python -m pytest tests/test_transform.py -v
```

Expected: 6 passed.

- [ ] **Step 6: Commit**

```bash
git add scripts/transform.py tests/
git commit -m "test: add pure hourly transforms with fixtures from real API responses"
```

---

### Task 4: Fetching and loading

**Files:**
- Create: `scripts/sources.py`, `scripts/load.py`

**Interfaces:**
- Consumes: `common.request_json`, `common.FetchError`, `common.GOOGLE_HISTORY_MIN_LAG_HOURS`.
- Produces:
  - `sources.fetch_google_day(loc: dict, day: date) -> dict` (raw payload, pages followed)
  - `sources.fetch_weather(loc: dict, start: date, end: date) -> tuple[dict, str]`
    (payload, `"archive"` or `"forecast"`)
  - `load.ensure_location(engine, loc: dict) -> int`
  - `load.upsert_hours(engine, location_id: int, rows: list[dict]) -> int`

- [ ] **Step 1: Write `scripts/sources.py`**

```python
"""Network access. Thin wrappers -- all parsing lives in transform.py."""
import os
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from common import GOOGLE_HISTORY_MIN_LAG_HOURS, FetchError, request_json

GOOGLE_HISTORY_URL = "https://airquality.googleapis.com/v1/history:lookup"
OPENMETEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPENMETEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

EXTRA_COMPUTATIONS = [
    "HEALTH_RECOMMENDATIONS", "POLLUTANT_ADDITIONAL_INFO",
    "DOMINANT_POLLUTANT_CONCENTRATION", "POLLUTANT_CONCENTRATION", "LOCAL_AQI",
]
HOURLY_VARS = ("temperature_2m,precipitation,wind_speed_10m,"
               "relative_humidity_2m,surface_pressure")


def utc_window(loc: dict, day: date) -> tuple[datetime, datetime]:
    """The true local day expressed in UTC, clamped to the API's lag requirement."""
    tz = ZoneInfo(loc["timezone"])
    start = datetime.combine(day, time.min, tzinfo=tz).astimezone(timezone.utc)
    latest = (datetime.now(timezone.utc)
              - timedelta(hours=GOOGLE_HISTORY_MIN_LAG_HOURS)).replace(
        minute=0, second=0, microsecond=0)
    return start, min(start + timedelta(days=1), latest)


def fetch_google_day(loc: dict, day: date) -> dict:
    key = os.getenv("GOOGLE_API_KEY")
    if not key:
        raise FetchError("GOOGLE_API_KEY is not set in .env")
    start, end = utc_window(loc, day)
    if end <= start:
        raise FetchError(f"{day} is not yet available (needs a "
                         f"{GOOGLE_HISTORY_MIN_LAG_HOURS}h lag)")

    hours, token, pages = [], None, 0
    while True:
        payload = {
            "pageSize": 24,
            "location": {"latitude": loc["latitude"], "longitude": loc["longitude"]},
            "period": {"startTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                       "endTime": end.strftime("%Y-%m-%dT%H:%M:%SZ")},
            "extraComputations": EXTRA_COMPUTATIONS,
        }
        if token:
            payload["pageToken"] = token
        data = request_json("POST", GOOGLE_HISTORY_URL, params={"key": key}, json=payload)
        hours.extend(data.get("hoursInfo", []))
        token, pages = data.get("nextPageToken"), pages + 1
        if not token or pages > 5:
            break
    return {"hoursInfo": hours}


def fetch_weather(loc: dict, start: date, end: date) -> tuple[dict, str]:
    """Archive where available, forecast+past_days for the recent tail.

    The archive endpoint lags several days behind real time; asking it for
    yesterday returns empty columns rather than an error.
    """
    common_params = {"latitude": loc["latitude"], "longitude": loc["longitude"],
                     "hourly": HOURLY_VARS, "timezone": "auto"}
    if (date.today() - end).days >= 6:
        return request_json("GET", OPENMETEO_ARCHIVE_URL, params={
            **common_params, "start_date": start.isoformat(),
            "end_date": end.isoformat()}), "archive"
    past = min((date.today() - start).days + 1, 92)
    return request_json("GET", OPENMETEO_FORECAST_URL, params={
        **common_params, "past_days": past, "forecast_days": 1}), "forecast"
```

- [ ] **Step 2: Write `scripts/load.py`**

```python
"""Database writes. One upsert statement, defined exactly once."""
from sqlalchemy import text

UPSERT = text("""
    INSERT INTO hourly_readings (
        location_id, observed_at, uaqi, usa_epa, pm25, pm10, o3, no2, co, so2,
        temperature_celsius, precipitation_mm, wind_speed_kmh,
        relative_humidity_pct, surface_pressure_hpa, weather_source
    ) VALUES (
        :location_id, :observed_at, :uaqi, :usa_epa, :pm25, :pm10, :o3, :no2, :co, :so2,
        :temperature_celsius, :precipitation_mm, :wind_speed_kmh,
        :relative_humidity_pct, :surface_pressure_hpa, :weather_source
    )
    ON CONFLICT (location_id, observed_at) DO UPDATE SET
        uaqi = COALESCE(EXCLUDED.uaqi, hourly_readings.uaqi),
        usa_epa = COALESCE(EXCLUDED.usa_epa, hourly_readings.usa_epa),
        pm25 = COALESCE(EXCLUDED.pm25, hourly_readings.pm25),
        pm10 = COALESCE(EXCLUDED.pm10, hourly_readings.pm10),
        o3 = COALESCE(EXCLUDED.o3, hourly_readings.o3),
        no2 = COALESCE(EXCLUDED.no2, hourly_readings.no2),
        co = COALESCE(EXCLUDED.co, hourly_readings.co),
        so2 = COALESCE(EXCLUDED.so2, hourly_readings.so2),
        temperature_celsius = COALESCE(EXCLUDED.temperature_celsius,
                                       hourly_readings.temperature_celsius),
        precipitation_mm = COALESCE(EXCLUDED.precipitation_mm,
                                    hourly_readings.precipitation_mm),
        wind_speed_kmh = COALESCE(EXCLUDED.wind_speed_kmh, hourly_readings.wind_speed_kmh),
        relative_humidity_pct = COALESCE(EXCLUDED.relative_humidity_pct,
                                         hourly_readings.relative_humidity_pct),
        surface_pressure_hpa = COALESCE(EXCLUDED.surface_pressure_hpa,
                                        hourly_readings.surface_pressure_hpa),
        weather_source = EXCLUDED.weather_source,
        ingested_at = now();
""")

SELECT_LOCATION = text(
    "SELECT id FROM locations WHERE city = :city AND country = :country")
INSERT_LOCATION = text("""
    INSERT INTO locations (city, country, latitude, longitude, timezone)
    VALUES (:city, :country, :latitude, :longitude, :timezone)
    RETURNING id;
""")


def ensure_location(engine, loc: dict) -> int:
    with engine.begin() as conn:
        found = conn.execute(SELECT_LOCATION, loc).scalar_one_or_none()
        if found:
            return found
        return conn.execute(INSERT_LOCATION, loc).scalar_one()


def upsert_hours(engine, location_id: int, rows: list[dict]) -> int:
    """COALESCE means a later partial fetch never blanks a value we already hold."""
    if not rows:
        return 0
    payload = [{**r, "location_id": location_id} for r in rows]
    with engine.begin() as conn:
        conn.execute(UPSERT, payload)
    return len(payload)
```

- [ ] **Step 3: Commit**

```bash
git add scripts/sources.py scripts/load.py
git commit -m "feat: add hourly fetch and upsert modules"
```

---

### Task 5: The CLI entry point

**Files:**
- Create: `scripts/fetch.py`

**Interfaces:**
- Consumes: everything from Tasks 2-4.
- Produces: a CLI. Exit `0` only when every requested location-day succeeded; `1` otherwise.

- [ ] **Step 1: Write `scripts/fetch.py`**

```python
"""Unified entry point. Replaces etl.py, historical_backfill.py and historical_patch.py.

    python scripts/fetch.py                      # trailing catch-up window (default 3 days)
    python scripts/fetch.py --date 2026-08-18
    python scripts/fetch.py --start 2026-07-21 --end 2026-08-19
    python scripts/fetch.py --days 30            # last 30 days

Exit status is 0 only if every location-day loaded. Anything else exits 1 so
Task Scheduler's "last run result" is finally meaningful.
"""
import argparse
import sys
from datetime import date, timedelta

import load as loader
import sources
import transform
from common import (GOOGLE_HISTORY_MAX_AGE_DAYS, LOCATIONS, FetchError,
                    get_engine, setup_logging)

DEFAULT_CATCHUP_DAYS = 3

log = setup_logging("aqi_etl")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Fetch hourly air-quality and weather data.")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--date", type=date.fromisoformat, help="single YYYY-MM-DD")
    g.add_argument("--days", type=int, help="the last N days ending today")
    p.add_argument("--start", type=date.fromisoformat)
    p.add_argument("--end", type=date.fromisoformat)
    return p.parse_args(argv)


def resolve_days(args) -> list[date]:
    today = date.today()
    if args.date:
        days = [args.date]
    elif args.start or args.end:
        start = args.start or today
        end = args.end or today
        days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    else:
        n = args.days or DEFAULT_CATCHUP_DAYS
        days = [today - timedelta(days=i) for i in range(n - 1, -1, -1)]

    oldest = today - timedelta(days=GOOGLE_HISTORY_MAX_AGE_DAYS - 1)
    usable = [d for d in days if d >= oldest]
    for d in sorted(set(days) - set(usable)):
        log.error("%s is older than the %d-day Google history limit -- unrecoverable",
                  d, GOOGLE_HISTORY_MAX_AGE_DAYS)
    return usable


def main(argv=None) -> int:
    args = parse_args(argv)
    days = resolve_days(args)
    if not days:
        log.error("No fetchable dates requested.")
        return 1

    try:
        engine = get_engine()
    except Exception as exc:
        log.error("Database connection failed: %s", exc)
        return 1
    log.info("Connected. Fetching %d day(s): %s .. %s", len(days), days[0], days[-1])

    failures = 0
    for loc in LOCATIONS:
        try:
            location_id = loader.ensure_location(engine, loc)
            weather_raw, source = sources.fetch_weather(loc, days[0], days[-1])
            weather = transform.openmeteo_hours(weather_raw)
        except Exception as exc:
            log.error("%s: setup failed: %s", loc["city"], exc)
            failures += len(days)
            continue

        for day in days:
            try:
                raw = sources.fetch_google_day(loc, day)
                rows = transform.merge_hours(transform.google_hours(raw), weather, source)
                n = loader.upsert_hours(engine, location_id, rows)
                measured = sum(1 for r in rows if r["uaqi"] is not None)
                log.info("%-15s %s  %2d hours (%d with AQI, weather=%s)",
                         loc["city"], day, n, measured, source)
                if n == 0:
                    log.warning("%s %s returned no hours", loc["city"], day)
                    failures += 1
            except FetchError as exc:
                log.error("%s %s: %s", loc["city"], day, exc)
                failures += 1
            except Exception as exc:
                log.exception("%s %s: unexpected failure: %s", loc["city"], day, exc)
                failures += 1

    if failures:
        log.error("Finished with %d failure(s).", failures)
        return 1
    log.info("Finished cleanly.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Verify argument handling without touching the network**

```bash
python -c "
import sys; sys.path.insert(0,'scripts')
import fetch
print(fetch.resolve_days(fetch.parse_args(['--date','2026-08-18'])))
print(len(fetch.resolve_days(fetch.parse_args(['--days','3']))))
print(fetch.resolve_days(fetch.parse_args(['--date','2020-01-01'])))
"
```

Expected: `[datetime.date(2026, 8, 18)]`, then `3`, then an ERROR log line about the 30-day
limit followed by `[]`.

- [ ] **Step 3: Run it for real against one recent day**

```bash
python scripts/fetch.py --date 2026-08-18
```

Expected: three `INFO` lines (one per city) showing ~24 hours each, `Finished cleanly.`, exit 0.

- [ ] **Step 4: Confirm the exit code is real**

```bash
python scripts/fetch.py --date 2020-01-01; echo "exit=$?"
```

Expected: `exit=1`.

- [ ] **Step 5: Commit**

```bash
git add scripts/fetch.py
git commit -m "feat: add unified fetch CLI with real exit codes and logging"
```

---

### Task 6: Load the rescued captures

**Files:**
- Create: `scripts/load_raw.py`

**Interfaces:**
- Consumes: `common.get_engine`, `common.LOCATIONS`, `common.REPO_ROOT`, `common.setup_logging`,
  `transform.google_hours`, `transform.openmeteo_hours`, `transform.merge_hours`,
  `load.ensure_location`, `load.upsert_hours`.

- [ ] **Step 1: Write `scripts/load_raw.py`**

```python
"""Load the rescued raw API captures under Data/raw into hourly_readings.

Data/raw holds the 2026-07-21..2026-08-19 window that was pulled off Google
before it aged out of the 30-day history limit. Re-runnable: the upsert is
idempotent per (location_id, observed_at).
"""
import json
import sys

import load as loader
import transform
from common import LOCATIONS, REPO_ROOT, get_engine, setup_logging

log = setup_logging("load_raw")
RAW = REPO_ROOT / "Data" / "raw"


def weather_for(slug: str) -> dict:
    """Forecast first, then archive on top: reanalysis wins where both exist."""
    merged = {}
    for kind in ("forecast", "archive"):
        path = RAW / "open_meteo" / f"{slug}.{kind}.json"
        if path.exists():
            merged.update(transform.openmeteo_hours(
                json.loads(path.read_text(encoding="utf-8"))))
    return merged


def main() -> int:
    engine = get_engine()
    total = 0
    for loc in LOCATIONS:
        slug = loc["city"].replace(" ", "_")
        location_id = loader.ensure_location(engine, loc)
        weather = weather_for(slug)

        for f in sorted((RAW / "google_aqi" / slug).glob("*.json")):
            payload = json.loads(f.read_text(encoding="utf-8"))
            rows = transform.merge_hours(
                transform.google_hours(payload), weather, "archive")
            n = loader.upsert_hours(engine, location_id, rows)
            total += n
            log.info("%-15s %s  %2d hours", loc["city"], f.stem, n)
    log.info("Loaded %d hourly rows.", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Run it**

```bash
python scripts/load_raw.py
```

Expected: 90 lines, then `Loaded 2211 hourly rows.`

- [ ] **Step 3: Verify against the source data**

```bash
psql -U postgres -d aqi_db -c "select count(*) from hourly_readings;" -c "select reading_date, hours_present, hours_with_aqi, round(uaqi_mean::numeric,1) from daily_readings_daily where location_id=1 order by reading_date limit 5;"
```

Expected: fewer rows than the 2211 upserted, because the day-file windows overlap by one hour
and the unique constraint collapses them; the view shows one row per date with `hours_present`
of 24 for full days.

- [ ] **Step 4: Commit**

```bash
git add scripts/load_raw.py
git commit -m "feat: load rescued raw captures into hourly_readings"
```

---

### Task 7: Make the scheduled run honest

**Files:**
- Modify: `scripts/run_etl.bat` (full rewrite)

- [ ] **Step 1: Rewrite the batch file**

```bat
@echo OFF
SETLOCAL
:: Scheduled entry point. Unlike the previous version this redirects all output
:: to a dated log AND propagates Python's exit code, so Task Scheduler's
:: "last run result" finally distinguishes a real run from a total failure.

:: Paths are DERIVED, never hardcoded: %~dp0 is this file's own directory,
:: so the repo root is its parent. Nothing names a user or a machine.
FOR %%I IN ("%~dp0..") DO SET "REPO=%%~fI"
SET "LOGDIR=%REPO%\logs"
IF NOT EXIST "%LOGDIR%" MKDIR "%LOGDIR%"

FOR /F %%d IN ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') DO SET "TODAY=%%d"
SET "LOGFILE=%LOGDIR%\run_etl-%TODAY%.log"

:: Set CONDA_ROOT in the environment to point at a non-default install.
IF NOT DEFINED CONDA_ROOT SET "CONDA_ROOT=%USERPROFILE%\anaconda3"
IF NOT EXIST "%CONDA_ROOT%\Scripts\activate.bat" (
    ECHO [%DATE% %TIME%] ERROR: conda activate.bat not found under "%CONDA_ROOT%" >> "%LOGFILE%"
    EXIT /B 1
)

CALL "%CONDA_ROOT%\Scripts\activate.bat"
CALL conda activate AQI_Predict

python "%REPO%\scripts\fetch.py" >> "%LOGFILE%" 2>&1
SET "RC=%ERRORLEVEL%"
ECHO [%DATE% %TIME%] fetch.py exited with %RC% >> "%LOGFILE%"
EXIT /B %RC%
```

- [ ] **Step 2: Verify the exit code propagates**

```bash
cmd //c "scripts\run_etl.bat" ; echo "exit=$?"
```

Expected: `exit=0`, and `logs/run_etl-<today>.log` contains the run output.

- [ ] **Step 3: Commit**

```bash
git add scripts/run_etl.bat
git commit -m "fix: log output and propagate exit code from scheduled ETL run"
```

---

### Task 8: Retire the forks and clean house

**Files:**
- Delete: `scripts/etl.py`, `scripts/historical_backfill.py`, `scripts/historical_patch.py`
- Modify: `requirements.txt`
- Rename: `scripts/database_setup.sql` to `scripts/reset_database.sql`

- [ ] **Step 1: Replace `requirements.txt` with the real direct dependencies**

```
# Direct dependencies only. The previous file was a raw `pip freeze` from conda
# in which 86 of 110 entries were local `@ file:///C:/...` build paths that
# could not install anywhere else (F13).
pandas>=2.0
psycopg2-binary>=2.9
python-dotenv>=1.0
requests>=2.31
SQLAlchemy>=2.0

# Notebook / exploration
jupyterlab>=4.0
matplotlib>=3.8
seaborn>=0.13

# Tests
pytest>=8.0
```

- [ ] **Step 2: Split the destructive SQL**

```bash
git mv scripts/database_setup.sql scripts/reset_database.sql
```

Then prepend this warning to `scripts/reset_database.sql`:

```sql
-- ############################################################################
-- DESTRUCTIVE. This DROPs daily_readings and locations and every row in them,
-- including readings that can no longer be re-fetched (Google's history API
-- reaches back only 30 days). This is a from-scratch reset, NOT a migration.
-- For the current schema use scripts/schema_hourly.sql, which is additive.
-- ############################################################################
```

- [ ] **Step 3: Delete the retired forks**

```bash
git rm scripts/etl.py scripts/historical_backfill.py scripts/historical_patch.py
```

- [ ] **Step 4: Verify nothing still references them**

```bash
grep -rn "etl\.py\|historical_backfill\|historical_patch\|database_setup" --include=*.py --include=*.bat --include=*.md . | grep -v "Docs/code-review" | grep -v "Docs/plans"
```

Expected: only `run_etl.bat` (which references `fetch.py`, not the retired scripts) and nothing
else. README and CLAUDE.md are updated in Task 9.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: retire forked scripts, fix requirements, split destructive SQL"
```

---

### Task 9: Update the documentation to match reality

**Files:**
- Modify: `CLAUDE.md`, `README.md`

- [ ] **Step 1: Rewrite the stale sections**

`CLAUDE.md` currently documents the three-way fork, the snapshot-vs-mean column collision, and
the silent failure model as *current behaviour*. All three are now false. Replace those sections
with: the module layout from this plan, the hourly-plus-view data model, the loud failure model,
and the two measured Google API limits (30-day lookback, 2-hour lag).

`README.md` must document the new commands:

```bash
python scripts/fetch.py                    # trailing 3-day catch-up (what the scheduler runs)
python scripts/fetch.py --date 2026-08-18  # one specific day
python scripts/fetch.py --days 30          # the full recoverable window
python scripts/load_raw.py                 # load rescued captures from Data/raw
pytest tests/ -v                           # run the transform tests
```

Both files must state the regime boundary: `daily_readings` is legacy and mixes snapshots with
means; `hourly_readings` starts 2026-07-21; 2026-03-17..2026-07-20 is permanently absent.

- [ ] **Step 2: Verify the documented commands actually work**

```bash
python -m pytest tests/ -v && python scripts/fetch.py --days 2
```

Expected: tests pass, fetch exits 0.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md README.md
git commit -m "docs: update for the unified hourly pipeline"
```

---

## Deferred / explicitly out of scope

- **Migrating legacy `daily_readings` into `hourly_readings`.** Impossible: those rows are a
  mix of instantaneous snapshots and 24-hour means and cannot be un-averaged. They stay as a
  labelled legacy table and any model must treat 2026-03-17 as a regime boundary.
- **The 2026-03-17 .. 2026-07-20 gap (~126 days).** Permanently unrecoverable; past Google's
  30-day horizon. Nothing to do but record it.
- **Alerting beyond exit codes.** Task Scheduler's "last run result" is sufficient for a
  personal project now that it reports the truth.
