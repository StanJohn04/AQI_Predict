-- ADDITIVE migration. Contains no DROP statements and is safe to re-run.
-- Legacy table daily_readings is intentionally left untouched: it holds the
-- 2025-06-08..2026-03-16 history and mixes instantaneous snapshots with daily
-- means, which cannot be un-mixed. Nothing writes to it any more.

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
-- old Open-Meteo daily semantics -- mean temperature, summed precipitation,
-- max wind -- so the view stays comparable with the legacy daily_readings rows.
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

-- Grants live with the migration that creates the objects, so a new table can
-- never be invisible to the ETL role. Guarded so this file still runs on a
-- database where create_etl_role.sql has not been applied yet.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'aqi_etl') THEN
        GRANT SELECT, INSERT, UPDATE ON hourly_readings TO aqi_etl;
        GRANT USAGE, SELECT ON SEQUENCE hourly_readings_id_seq TO aqi_etl;
        GRANT SELECT ON daily_readings_daily TO aqi_etl;
    END IF;
END
$$;
