-- Drop tables if they exist to start with a clean slate.
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
    pm10 REAL, -- Added
    co REAL,   -- Added
    so2 REAL,  -- Added
    UNIQUE(location_id, reading_date)
);

-- Add comments to explain the schema (optional but good practice)
COMMENT ON TABLE locations IS 'Stores unique geographic locations for which data is collected.';
COMMENT ON TABLE daily_readings IS 'Stores daily air quality and weather readings for each location.';
COMMENT ON COLUMN daily_readings.location_id IS 'Foreign key linking to the locations table.';