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
    assert all(r["observed_at"].utcoffset().total_seconds() == 0 for r in rows)

    measured = [r for r in rows if r["uaqi"] is not None]
    assert len(measured) >= 20, "a full day should be almost entirely populated"
    assert all(isinstance(r["uaqi"], int) for r in measured)
    # usa_epa was previously discarded even though it is the more useful US index (F12).
    assert all(isinstance(r["usa_epa"], int) for r in measured)
    assert sum(1 for r in rows if r["pm25"] is not None) >= 20


def test_google_hours_does_not_assume_response_ordering():
    """The API returns hours newest-first, and the exclusive end-of-window hour
    comes back with pollutant codes but null concentrations. Nothing downstream
    may depend on position -- rows are keyed by absolute timestamp precisely so
    that the old [0] vs [-1] class of bug cannot recur."""
    rows = transform.google_hours(load("google_day.json"))
    stamps = [r["observed_at"] for r in rows]
    assert stamps != sorted(stamps), "fixture is expected to be newest-first"
    assert len(set(stamps)) == len(stamps), "timestamps must be unique keys"


def test_google_hours_handles_null_concentration():
    """`concentration` arrives as JSON null, not as a missing key."""
    payload = {"hoursInfo": [{"dateTime": "2026-08-02T04:00:00Z",
                              "pollutants": [{"code": "pm25", "concentration": None}]}]}
    assert transform.google_hours(payload)[0]["pm25"] is None


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
    hours = transform.openmeteo_hours(load("openmeteo_archive.json"))
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
