"""Unified entry point. Replaces etl.py, historical_backfill.py and historical_patch.py.

    python scripts/fetch.py                      # trailing catch-up window (default 3 days)
    python scripts/fetch.py --date 2026-08-18
    python scripts/fetch.py --start 2026-07-21 --end 2026-08-19
    python scripts/fetch.py --days 30            # last 30 days

The trailing window means a missed run self-heals on the next one, and the
upsert makes re-running any date harmless.

Exit status is 0 only if every location-day loaded. Anything else exits 1, so
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
    g.add_argument("--date", type=date.fromisoformat, help="single day, YYYY-MM-DD")
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
