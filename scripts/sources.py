"""Network access. Thin wrappers -- all parsing lives in transform.py."""
import os
import time
from datetime import date, datetime, time as dtime, timedelta, timezone
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

# Google intermittently answers with the AQI indexes present but every
# pollutant concentration null. It is not an error status and an identical
# re-request returns the data, so a bare fetch silently loses ~45% of the
# pollutant columns. Measured 2026-08-19 while repairing the rescue capture.
CONCENTRATION_ATTEMPTS = 3


def utc_window(loc: dict, day: date) -> tuple[datetime, datetime]:
    """The true local day expressed in UTC, clamped to the API's lag requirement."""
    tz = ZoneInfo(loc["timezone"])
    start = datetime.combine(day, dtime.min, tzinfo=tz).astimezone(timezone.utc)
    latest = (datetime.now(timezone.utc)
              - timedelta(hours=GOOGLE_HISTORY_MIN_LAG_HOURS)).replace(
        minute=0, second=0, microsecond=0)
    return start, min(start + timedelta(days=1), latest)


def _concentration_coverage(hours: list[dict]) -> int:
    return sum(1 for h in hours
               if any((p.get("concentration") or {}).get("value") is not None
                      for p in (h.get("pollutants") or [])))


def _fetch_once(loc: dict, start: datetime, end: datetime, key: str) -> list[dict]:
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
    return hours


def fetch_google_day(loc: dict, day: date) -> dict:
    """All hours of one local day, retrying while concentrations come back empty."""
    key = os.getenv("GOOGLE_API_KEY")
    if not key:
        raise FetchError("GOOGLE_API_KEY is not set in .env")
    start, end = utc_window(loc, day)
    if end <= start:
        raise FetchError(f"{day} is not yet available (needs a "
                         f"{GOOGLE_HISTORY_MIN_LAG_HOURS}h lag)")

    best, best_cov = [], -1
    for attempt in range(1, CONCENTRATION_ATTEMPTS + 1):
        hours = _fetch_once(loc, start, end, key)
        cov = _concentration_coverage(hours)
        if cov > best_cov:
            best, best_cov = hours, cov
        # Allow a small shortfall: some hours are genuinely empty upstream.
        if best_cov >= len(best) - 2:
            break
        if attempt < CONCENTRATION_ATTEMPTS:
            time.sleep(2 * attempt)
    return {"hoursInfo": best}


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
