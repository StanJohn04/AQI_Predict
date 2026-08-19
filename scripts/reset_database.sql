-- ############################################################################
-- DESTRUCTIVE. This DROPs daily_readings and locations and every row in them,
-- including readings that can no longer be re-fetched (Google's history API
-- reaches back only 30 days). This is a from-scratch reset, NOT a migration.
--
-- For the current schema use scripts/schema_hourly.sql, which is additive and
-- safe to re-run against the populated database.
-- ############################################################################

DROP TABLE IF EXISTS daily_readings;
DROP TABLE IF EXISTS locations;

-- Create the locations table
-- This table stores the geographic information for each city we collect data for.
CREATE TABLE locations (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    UNIQUE(city, country)
);

-- Create the daily_readings table
-- This table stores the core time-series data, linking air quality and weather.
CREATE TABLE daily_readings (
    id SERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL REFERENCES locations(id),
    reading_date DATE NOT NULL,
    aqi INTEGER,
    pm25 REAL,
    o3 REAL,
    no2 REAL,
    temperature_celsius REAL,
    precipitation_mm REAL,
    wind_speed_kmh REAL,
    pm10 REAL, 
    co REAL,   
    so2 REAL,  
    UNIQUE(location_id, reading_date)
);

COMMENT ON TABLE locations IS 'Stores unique geographic locations for which data is collected.';
COMMENT ON TABLE daily_readings IS 'Stores daily air quality and weather readings for each location.';
COMMENT ON COLUMN daily_readings.location_id IS 'Foreign key linking to the locations table.';