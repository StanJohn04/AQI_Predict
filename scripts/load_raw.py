"""Load the rescued raw API captures under Data/raw into hourly_readings.

Data/raw holds the 2026-07-21..2026-08-19 window pulled off Google before it
aged out of the 30-day history limit, after a repair pass that recovered the
pollutant concentrations Google had intermittently returned as null.
Re-runnable: the upsert is idempotent per (location_id, observed_at).
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
    log.info("Upserted %d hourly rows.", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
