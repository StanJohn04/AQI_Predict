# Code Review & Next Steps — AQI_Predict

**Date:** 2026-08-04
**Reviewed at commit:** `4fcdcdd` (main, clean)
**Scope:** full repo — `scripts/`, `notebooks/`, schema, docs, packaging
**Status:** review complete; bug diagnosis not started

---

## 0. TL;DR for the next session

The daily pull is failing at the **database connection**, not at the APIs. Postgres is up and
answering, but it rejects the credentials in `.env`:

```
FATAL: password authentication failed for user "stant"
```

Reproduced by loading `.env` through the same `python-dotenv` → SQLAlchemy path `etl.py` uses.
`etl.py` catches this, prints, and **exits 0**, so Task Scheduler has been logging success the
whole time. Root cause of the rejection is still unknown — that is the first job tomorrow. See
§4 for the diagnosis plan.

---

## 1. What this project is

A single-machine ETL pipeline. A scheduled Python job pulls air-quality data (Google Air Quality
API) and weather data (Open-Meteo) for three locations near Savannah, GA, transforms them into
one row per location per day, and loads them into a local PostgreSQL database. Three scripts
(`etl.py`, `historical_backfill.py`, `historical_patch.py`), one schema file, one exploration
notebook. No service, no application layer.

The design is sound and the pipeline did work. The problems found are in **error handling**,
**code duplication**, and **data semantics** — not in the overall approach.

---

## 2. Findings

### 2.1 Critical

**F1 — Every failure path is silent, and the process always exits 0.**
`get_db_engine()`, both fetch functions, and `transform_data()` all follow the same pattern:
catch the exception, `print` a message, `return None`. `__main__` then skips that item and keeps
going. Nothing raises, nothing sets an exit code.

Compounding it, `scripts/run_etl.bat` neither redirects output to a log nor propagates Python's
exit code back to the caller. The net effect: **a total failure and a clean run are
indistinguishable from outside the process.** Task Scheduler history is worthless as evidence.
This is the reason the outage went unnoticed for an extended period rather than being caught the
first morning — it is arguably a more serious defect than whatever broke the credentials.

*Impact:* silent data loss. Google's history endpoint only reaches back ~30 days, so any gap
older than that is **permanently unrecoverable**.

**F2 — API error bodies are discarded.**
`except requests.exceptions.RequestException as e: print(...)` prints the exception, which for a
4xx contains only the status line and URL. Google puts the actual reason (key expired, quota
exhausted, billing disabled, API not enabled, referrer restriction) in the **response body**,
which is thrown away. When the failure eventually is an API failure, there will be nothing to
diagnose from.

**F3 — No request timeouts.**
`requests.get` / `requests.post` have no default timeout. A hung socket blocks the scheduled task
indefinitely, with no upper bound and no alert. On a daily schedule this can silently stack
processes.

### 2.2 Data integrity

**F4 — The same columns hold two different quantities.**
This is the most consequential finding for the modeling phase.

| | `etl.py` (daily) | `historical_*.py` (backfill/patch) |
|---|---|---|
| AQI source | `currentConditions:lookup` | `history:lookup` |
| AQI value stored | single **instantaneous** reading | **24-hour mean** of hourly readings |
| Weather source | Open-Meteo `forecast` | Open-Meteo `archive` |
| Weather value stored | the day's **prediction** | the day's **observation** |

Both write to the same columns in `daily_readings`. Rows are therefore not comparable across the
boundary between backfilled and daily-collected dates, and a model trained across it will fit an
artificial discontinuity. Re-running a historical script over a date silently converts that row
from snapshot to mean.

**F5 — Day boundaries do not line up.**
The historical scripts build the Google query window as
`datetime.combine(date, time.min).isoformat() + "Z"` — local midnight **labelled as UTC**. For
Savannah (UTC−4/−5) the 24-hour window is shifted by that offset, so the "daily mean" actually
spans roughly 20:00 the previous day → 20:00 local. Open-Meteo is queried with `timezone: auto`,
which aggregates by true local day. The AQI figure and the weather figures on a single row cover
different spans of time.

**F6 — Checked-in schema disagrees with the docs.**
`scripts/database_setup.sql:21` declares `aqi INTEGER`; `Docs/Docs.md` documents `aqi REAL`. The
historical scripts compute a float mean and write it to that column. At least one of the two is
wrong about the live table. Confirm against the live database once it is reachable.

**F7 — `database_setup.sql` is a destructive reset, not a migration.**
It opens with `DROP TABLE IF EXISTS daily_readings; DROP TABLE IF EXISTS locations;`. The
previous README instructed running it as a setup step with no warning attached. Running it
against the populated database destroys every collected reading — including the ones that can no
longer be re-fetched (see F1).

### 2.3 Maintainability

**F8 — Three-way duplication.**
`etl.py`, `historical_backfill.py`, and `historical_patch.py` each carry their **own** copy of
`LOCATIONS`, `get_db_engine()`, `load_data()`, and the full `INSERT ... ON CONFLICT` statement.
Nothing is shared. A schema change or a new location requires three synchronized edits, and
nothing detects a missed one. `historical_patch.py` is a near-verbatim fork of
`historical_backfill.py` differing only in weather-fetch strategy and hardcoded dates.

This is the highest-leverage refactor available: a single `scripts/common.py` would remove
roughly 80% of the codebase.

**F9 — Date ranges are hardcoded in `__main__`.**
`DAYS_OF_HISTORY = 28` in the backfill; a literal `[datetime(2025, 6, 6), datetime(2025, 6, 7)]`
list in the patch script. Filling a gap means editing source and committing it. A `--start` /
`--end` argument on one unified historical script would retire `historical_patch.py` entirely.

**F10 — Subtle index divergence between the two historical scripts.**
`historical_backfill.py` reads `[0]` from the Open-Meteo daily arrays; `historical_patch.py`
reads `[-1]`. Both are correct *for their own query shape* — the patch script uses `past_days`,
which returns a range rather than a single day. If the two are ever merged carelessly this
silently attributes the wrong day's weather to a row, with no error.

**F11 — No tests, no linting, no CI.**
`transform_data` and `transform_historical_data` are pure functions over JSON dicts — they are
trivially testable with a couple of saved API responses as fixtures, and they are exactly where
the subtle bugs live (F4, F5, F10, and the historical `hours` → `hoursInfo` bug already fixed
once).

**F12 — Minor.** Unused `pandas` and `json` imports in `etl.py`; unused `json` in
`historical_patch.py`. `AQI` is read only from the `uaqi` index — the `LOCAL_AQI` extra
computation also returns `usa_epa`, which is discarded and is arguably the more useful index for
a US location.

### 2.4 Packaging and repo hygiene

**F13 — `requirements.txt` is not installable.**
It is a raw `pip freeze` from a conda environment: 86 of its 110 lines are
`@ file:///C:/b/.../work` local build paths. `pip install -r requirements.txt` fails on any other
machine. Real direct dependencies: `requests`, `pandas`, `sqlalchemy`, `psycopg2`,
`python-dotenv`, plus `matplotlib` / `seaborn` / `jupyterlab` for the notebook.

**F14 — `Docs/Docs.md` is gitignored.**
The `docs.md` pattern in `.gitignore` matches it (case-insensitively, on Windows). Only
`Docs/Docs.pdf` is tracked, so edits to the markdown source never appear in `git status` and the
PDF is the only version-controlled copy. Deliberate per commit `5bd0b07`, but worth being
conscious of.

**F15 — `.gitignore` contains `data/`**, which on case-insensitive Windows also excludes the
`Data/` directory in this repo.

**F16 — `run_etl.bat` hardcodes absolute paths** to Anaconda and to this checkout. It works only
on the machine it was written for. Relevant to the Raspberry Pi migration noted in `Docs.md` §5.2.

---

## 3. What was done this session

- Full read of every script, the schema, the notebook, and `Docs/Docs.md`.
- Confirmed the failure reproduces at the DB connection step (see §0).
- Wrote `CLAUDE.md` — orientation for future Claude Code sessions: commands, the fork structure,
  the API response shapes, the failure model, and the repo traps above.
- Rewrote `README.md` — corrected the broken install instructions (F13), documented
  `historical_patch.py`, added the schema table, flagged the destructive SQL (F7), added the
  data-semantics warning (F4) and an Operations section covering the silent-failure limitation.

No code was changed.

---

## 4. Next steps

### Step 1 — Diagnose the auth failure (do this first, change nothing yet)

Do not reset the Postgres password as a first move. Establish *what changed* first, because "the
password was rotated" and "`pg_hba.conf` changed" and "`.env` was edited" have different fixes
and different implications for whether it recurs.

Questions to answer, roughly in order:

1. Does `psql` with the same user/password from the command line fail the same way? (Isolates
   the app path from the credential itself.)
2. Does the `.env` password still match what the Postgres role expects — was the role's password
   changed, or was `.env` edited? `.env` is untracked, so git will not tell you; check its file
   mtime against the date the pulls stopped.
3. What does `pg_hba.conf` say for `host all all 127.0.0.1/32` — `scram-sha-256`, `md5`, or
   `trust`? A Postgres upgrade or a `password_encryption` change can invalidate a stored md5
   hash and produce exactly this error without anyone touching the password.
4. Check the Postgres server log around the first failing date — it records the auth method that
   was attempted, which distinguishes the above cases.

**Bound the damage while diagnosing:** find the last `reading_date` in `daily_readings` once
connected. Anything missing beyond ~30 days back is unrecoverable from Google's history
endpoint; anything inside the window should be patched promptly, before it ages out. This is
time-sensitive and worth doing before any refactoring.

### Step 2 — Make failure loud (do this even if the fix in Step 1 is trivial)

The credential problem is the proximate cause of the outage. **F1 is the reason it lasted.** Fix
it in the same session or it will happen again with a different trigger.

Minimum viable version:

- `sys.exit(1)` on DB connection failure and on any location failing to load.
- Replace `print` with `logging`, writing to a dated file under a `logs/` directory.
- In `run_etl.bat`: redirect stdout and stderr to that log, and `exit /b %ERRORLEVEL%` so Task
  Scheduler records the real result.
- Log the response body on HTTP errors (F2).
- Add `timeout=30` to every `requests` call (F3).

Optional but cheap: a non-zero exit makes Task Scheduler's "last run result" meaningful, which is
enough of an alert for a personal project. A push notification on failure is a nice-to-have.

### Step 3 — Backfill the gap

Once connected and logging, patch the missing dates with the historical script. Note this writes
*daily means* into the gap while surrounding daily rows are *snapshots* (F4) — decide before
running whether that is acceptable or whether the whole table should be normalized to one
convention.

### Step 4 — Consolidate the three scripts

`scripts/common.py` holding `LOCATIONS`, `get_db_engine()`, `load_data()`, the INSERT statement,
and the pollutant-extraction helpers. Then one `fetch.py` with `--date` / `--start` / `--end`
arguments replacing all three entry points. This is the change that makes every subsequent fix a
one-file edit instead of a three-file edit.

Worth pairing with a few tests over saved API-response fixtures (F11) — the transform functions
are pure, and they are where the real bugs have been.

### Step 5 — Decide the data-semantics question (F4/F5)

This is a design decision, not a bug fix, and should be made deliberately before the modeling
phase rather than discovered during it. Options:

- **A — Normalize to daily means.** Switch the daily job to run the *next* morning against
  `history:lookup` for the previous day, so every row is a 24-hour mean of observed data. Most
  consistent; costs a day of latency; makes the daily and historical paths the same code.
- **B — Keep both, label them.** Add a `source` / `aggregation` column so rows are
  self-describing and models can filter or adjust. Cheapest; preserves existing data as-is.
- **C — Store hourly.** Add an `hourly_readings` table and derive daily summaries as a view.
  Most flexible and the best foundation for forecasting; largest change.

Option A or C is the better long-term answer; B is the honest cheap fix if the goal is to get to
modeling quickly. Also fix the UTC/local window offset (F5) as part of whichever is chosen.

### Step 6 — Housekeeping (low priority, do opportunistically)

- Replace `requirements.txt` with the ~8 real direct dependencies (F13).
- Reconcile `database_setup.sql` with the live schema and add a comment warning about the DROPs,
  or split it into `schema.sql` + a separate `reset.sql` (F6, F7).
- Remove unused imports (F12).

---

## 5. Suggested priority

| Priority | Item | Why |
|---|---|---|
| 1 | Diagnose auth failure (§4.1) | Nothing else matters while the pipeline is down |
| 2 | Assess gap, patch what is inside the 30-day window | Time-sensitive; data ages out permanently |
| 3 | Loud failures + logging (F1, F2, F3) | The actual root cause of the *outage duration* |
| 4 | Consolidate to `common.py` (F8, F9) | Makes everything after this cheaper |
| 5 | Decide data semantics (F4, F5) | Blocks the modeling phase; decide before collecting more |
| 6 | Tests over transform functions (F11) | Where the subtle bugs live |
| 7 | Packaging and hygiene (F13, F6, F12) | Nice to have |
