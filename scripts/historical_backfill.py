import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timedelta, time
import time as time_sleep # To avoid conflicts with datetime.time

# --- CONFIGURATION ---
load_dotenv()

LOCATIONS = [
    { "city": "Savannah", "country": "USA", "latitude": 32.0809, "longitude": -81.0912 },
    { "city": "Port Wentworth", "country": "USA", "latitude": 32.17, "longitude": -81.17 },
    { "city": "Pooler", "country": "USA", "latitude": 32.11, "longitude": -81.25 }
]

DAYS_OF_HISTORY = 28 # Set to desired number of days for the full backfill (30 day limit from google api)

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

def fetch_historical_aqi_data(latitude, longitude, date_to_fetch):
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key: return None
    url = "https://airquality.googleapis.com/v1/history:lookup"
    start_time = datetime.combine(date_to_fetch, time.min).isoformat() + "Z"
    end_time = datetime.combine(date_to_fetch, time.max).isoformat() + "Z"
    json_payload = {
        "pageSize": 24, "location": { "latitude": latitude, "longitude": longitude },
        "period": { "startTime": start_time, "endTime": end_time },
        "extraComputations": [ "HEALTH_RECOMMENDATIONS", "POLLUTANT_ADDITIONAL_INFO", "DOMINANT_POLLUTANT_CONCENTRATION", "POLLUTANT_CONCENTRATION", "LOCAL_AQI" ]
    }
    try:
        response = requests.post(url, params={"key": api_key}, json=json_payload)
        response.raise_for_status()
        print(f"Successfully fetched historical AQI data for {date_to_fetch.strftime('%Y-%m-%d')}.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching historical AQI data for {date_to_fetch.strftime('%Y-%m-%d')}: {e}")
        return None

def fetch_weather_data(latitude, longitude, date_str):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude, "longitude": longitude, "start_date": date_str, "end_date": date_str,
        "daily": "temperature_2m_mean,precipitation_sum,wind_speed_10m_max", "timezone": "auto"
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        print(f"Successfully fetched weather data for {date_str}.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data for {date_str}: {e}")
        return None

def transform_historical_data(date_to_fetch, aqi_history_data, weather_data):
    print(f"Transforming data for {date_to_fetch.strftime('%Y-%m-%d')}...")
    if not aqi_history_data or not weather_data: return None
    try:
        daily_weather = weather_data.get('daily', {})
        temp = daily_weather.get('temperature_2m_mean', [None])[0]
        precip = daily_weather.get('precipitation_sum', [None])[0]
        wind = daily_weather.get('wind_speed_10m_max', [None])[0]

        # --- THE FIX: Use 'hoursInfo' instead of 'hours' ---
        hourly_readings = aqi_history_data.get('hoursInfo', [])
        
        if not hourly_readings: return None
        df = pd.DataFrame(hourly_readings)
        def get_aqi(indexes):
            if not isinstance(indexes, list): return None
            return next((idx.get('aqi') for idx in indexes if idx.get('code') == 'uaqi'), None)
        df['aqi'] = df['indexes'].apply(get_aqi)
        def get_pollutant_value(pollutants, code):
            if not isinstance(pollutants, list): return None
            p_dict = {p.get('code'): p.get('concentration', {}).get('value') for p in pollutants}
            return p_dict.get(code)
        for p_code in ['pm10', 'pm25', 'o3', 'no2', 'co', 'so2']:
            df[p_code] = df['pollutants'].apply(lambda x: get_pollutant_value(x, p_code))
        daily_averages = df[['aqi', 'pm10', 'pm25', 'o3', 'no2', 'co', 'so2']].mean().to_dict()
        return {
            "reading_date": date_to_fetch.date(), "aqi": daily_averages.get('aqi'), "pm10": daily_averages.get('pm10'),
            "pm25": daily_averages.get('pm25'), "o3": daily_averages.get('o3'), "no2": daily_averages.get('no2'),
            "co": daily_averages.get('co'), "so2": daily_averages.get('so2'), "temperature_celsius": temp,
            "precipitation_mm": precip, "wind_speed_kmh": wind
        }
    except Exception as e:
        print(f"Error during data transformation: {e}")
        return None

def load_data(engine, location_info, reading_data):
    print(f"Loading data for {location_info['city']} for date {reading_data['reading_date']}...")
    if not reading_data: return
    with engine.connect() as connection:
        existing_id_sql = text("SELECT id FROM locations WHERE city = :city AND country = :country")
        existing_id = connection.execute(existing_id_sql, location_info).scalar_one_or_none()
        if existing_id: location_id = existing_id
        else:
            insert_location_sql = text("INSERT INTO locations (city, country, latitude, longitude) VALUES (:city, :country, :latitude, :longitude) RETURNING id;")
            location_id = connection.execute(insert_location_sql, location_info).scalar_one()
        reading_data['location_id'] = location_id
        insert_sql = text("""
            INSERT INTO daily_readings (location_id, reading_date, aqi, pm10, pm25, o3, no2, co, so2, temperature_celsius, precipitation_mm, wind_speed_kmh)
            VALUES (:location_id, :reading_date, :aqi, :pm10, :pm25, :o3, :no2, :co, :so2, :temperature_celsius, :precipitation_mm, :wind_speed_kmh)
            ON CONFLICT (location_id, reading_date) DO UPDATE SET
                aqi = EXCLUDED.aqi, pm10 = EXCLUDED.pm10, pm25 = EXCLUDED.pm25, o3 = EXCLUDED.o3, no2 = EXCLUDED.no2,
                co = EXCLUDED.co, so2 = EXCLUDED.so2, temperature_celsius = EXCLUDED.temperature_celsius,
                precipitation_mm = EXCLUDED.precipitation_mm, wind_speed_kmh = EXCLUDED.wind_speed_kmh;
        """)
        connection.execute(insert_sql, reading_data)
        connection.commit()
        print(f"Successfully loaded data.")


if __name__ == "__main__":
    print("--- Starting Historical Data Backfill Process ---")
    engine = get_db_engine()
    if not engine:
        print("Halting process due to database connection failure.")
    else:
        today = datetime.now()
        for i in range(DAYS_OF_HISTORY, 0, -1):
            date_to_fetch = today - timedelta(days=i)
            date_str = date_to_fetch.strftime('%Y-%m-%d')
            print(f"\n{'='*20} Processing Date: {date_str} {'='*20}")
            for location in LOCATIONS:
                print(f"\n--- Fetching for {location['city']} ---")
                aqi_data = fetch_historical_aqi_data(location['latitude'], location['longitude'], date_to_fetch)
                weather_data = fetch_weather_data(location['latitude'], location['longitude'], date_str)
                transformed_reading = transform_historical_data(date_to_fetch, aqi_data, weather_data)
                if transformed_reading:
                    load_data(engine, location, transformed_reading)
                time_sleep.sleep(1) 
    print("\n--- Historical Data Backfill Process Finished ---")
