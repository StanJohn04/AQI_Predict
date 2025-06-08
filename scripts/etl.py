import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
import json

# --- CONFIGURATION ---
load_dotenv()

LOCATIONS = [
    { "city": "Savannah", "country": "USA", "latitude": 32.0809, "longitude": -81.0912 },
    { "city": "Port Wentworth", "country": "USA", "latitude": 32.17, "longitude": -81.17 },
    { "city": "Pooler", "country": "USA", "latitude": 32.11, "longitude": -81.25 }
]

def get_db_engine():
    try:
        db_user, db_password = os.getenv("DB_USER"), os.getenv("DB_PASSWORD")
        db_host, db_port, db_name = os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME")
        engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
        with engine.connect() as connection:
            print("Database engine created and connection successful.")
        return engine
    except Exception as e:
        print(f"Error creating database engine: {e}")
        return None


def fetch_air_quality_data(latitude, longitude):
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("Error: GOOGLE_API_KEY not found.")
        return None
    url = "https://airquality.googleapis.com/v1/currentConditions:lookup"
    params = {"key": api_key}
    # This is the user-discovered payload that gets all the data
    json_payload = {
        "location": { "latitude": latitude, "longitude": longitude },
        "extraComputations": [
            "HEALTH_RECOMMENDATIONS",
            "POLLUTANT_ADDITIONAL_INFO",
            "DOMINANT_POLLUTANT_CONCENTRATION",
            "POLLUTANT_CONCENTRATION",
            "LOCAL_AQI" # Including LOCAL_AQI might be what unlocked all pollutants
        ]
    }
    try:
        response = requests.post(url, params=params, json=json_payload)
        response.raise_for_status()
        print(f"Successfully fetched air quality data for lat:{latitude}, lon:{longitude}.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching air quality data: {e}")
        return None

def fetch_weather_data(latitude, longitude):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude, "longitude": longitude,
        "daily": "temperature_2m_mean,precipitation_sum,wind_speed_10m_max",
        "timezone": "auto", "forecast_days": 1
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        print(f"Successfully fetched weather forecast data for lat:{latitude}, lon:{longitude}.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

def transform_data(aqi_data, weather_data):
    """
    FINAL: Transforms raw API data, extracting all available pollutants.
    """
    print("Transforming data...")
    if not aqi_data or not weather_data:
        return None
    try:
        daily_weather = weather_data.get('daily', {})
        temp = daily_weather.get('temperature_2m_mean', [None])[0]
        precip = daily_weather.get('precipitation_sum', [None])[0]
        wind = daily_weather.get('wind_speed_10m_max', [None])[0]

        aqi_index = next((item for item in aqi_data.get('indexes', []) if item.get('code') == 'uaqi'), {})
        aqi = aqi_index.get('aqi')
        
        pollutants = {p.get('code'): p.get('concentration', {}).get('value') for p in aqi_data.get('pollutants', [])}
        
        # --- FINAL: Extract all pollutants ---
        pm10 = pollutants.get('pm10')
        pm25 = pollutants.get('pm25')
        o3 = pollutants.get('o3')
        no2 = pollutants.get('no2')
        co = pollutants.get('co')
        so2 = pollutants.get('so2')

        return {
            "reading_date": datetime.now().date(), "aqi": aqi, "pm10": pm10, "pm25": pm25, "o3": o3,
            "no2": no2, "co": co, "so2": so2, "temperature_celsius": temp,
            "precipitation_mm": precip, "wind_speed_kmh": wind
        }
    except (KeyError, IndexError, TypeError) as e:
        print(f"Error during data transformation: {e}")
        return None


def load_data(engine, location_info, reading_data):
    """
    FINAL: Loads transformed data, including all new pollutant columns.
    """
    print(f"Loading data for {location_info['city']}...")
    if not reading_data:
        return

    with engine.connect() as connection:
        # This logic correctly finds or creates the location and gets the ID
        existing_id_sql = text("SELECT id FROM locations WHERE city = :city AND country = :country")
        existing_id = connection.execute(existing_id_sql, location_info).scalar_one_or_none()
        
        if existing_id:
            location_id = existing_id
        else:
            insert_location_sql = text("""
                INSERT INTO locations (city, country, latitude, longitude)
                VALUES (:city, :country, :latitude, :longitude)
                RETURNING id;
            """)
            location_id = connection.execute(insert_location_sql, location_info).scalar_one()

        reading_data['location_id'] = location_id
        
        # FINAL: The INSERT statement now includes all columns
        insert_sql = text("""
            INSERT INTO daily_readings (
                location_id, reading_date, aqi, pm10, pm25, o3, no2, co, so2,
                temperature_celsius, precipitation_mm, wind_speed_kmh
            ) VALUES (
                :location_id, :reading_date, :aqi, :pm10, :pm25, :o3, :no2, :co, :so2,
                :temperature_celsius, :precipitation_mm, :wind_speed_kmh
            )
            ON CONFLICT (location_id, reading_date) DO UPDATE SET
                aqi = EXCLUDED.aqi, pm10 = EXCLUDED.pm10, pm25 = EXCLUDED.pm25,
                o3 = EXCLUDED.o3, no2 = EXCLUDED.no2, co = EXCLUDED.co, so2 = EXCLUDED.so2,
                temperature_celsius = EXCLUDED.temperature_celsius,
                precipitation_mm = EXCLUDED.precipitation_mm,
                wind_speed_kmh = EXCLUDED.wind_speed_kmh;
        """)
        
        connection.execute(insert_sql, reading_data)
        connection.commit()
        print(f"Successfully loaded data for {location_info['city']} for date {reading_data['reading_date']}.")


if __name__ == "__main__":
    print("Starting ETL process...")
    engine = get_db_engine()
    if not engine:
        print("Halting ETL process due to database connection failure.")
    else:
        for location in LOCATIONS:
            print(f"\n--- Processing data for {location['city']} ---")
            aqi_data = fetch_air_quality_data(location['latitude'], location['longitude'])
            weather_data = fetch_weather_data(location['latitude'], location['longitude'])
            transformed_reading = transform_data(aqi_data, weather_data)
            if transformed_reading:
                load_data(engine, location, transformed_reading)
    print("\nETL process finished.")