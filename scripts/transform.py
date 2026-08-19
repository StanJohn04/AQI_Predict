"""Pure JSON -> row-dict transforms. No network, no database, no logging.

These functions are where every historically subtle bug has lived (the
hours/hoursInfo mixup, the [0] vs [-1] positional-index divergence, the UTC
window offset), which is exactly why they are isolated and unit-tested.
"""
from datetime import datetime, timedelta, timezone

POLLUTANTS = ("pm25", "pm10", "o3", "no2", "co", "so2")

WEATHER_FIELDS = ("temperature_celsius", "precipitation_mm", "wind_speed_kmh",
                  "relative_humidity_pct", "surface_pressure_hpa")


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
