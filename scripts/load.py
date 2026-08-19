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
