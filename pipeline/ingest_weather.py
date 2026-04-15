import os
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# Coordinates for Surat, Gujarat (factory location)
LATITUDE  = 21.1702
LONGITUDE = 72.8311
LOCATION  = "Surat, Gujarat, India"

def fetch_weather():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":        LATITUDE,
        "longitude":       LONGITUDE,
        "current":         "temperature_2m,relative_humidity_2m",
        "timezone":        "Asia/Kolkata",
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    current     = data["current"]
    recorded_at = datetime.now(timezone.utc)

    row = {
        "recorded_at":   recorded_at,
        "location":      LOCATION,
        "ambient_temp_c": current["temperature_2m"],
        "humidity_pct":   current["relative_humidity_2m"],
    }

    return row

def ingest_weather():
    row = fetch_weather()
    df  = pd.DataFrame([row])

    with engine.begin() as conn:
        df.to_sql("raw_weather_data", conn, if_exists="append", index=False)

    print(f"[ingest_weather] Weather written at {row['recorded_at']} — "
          f"Temp: {row['ambient_temp_c']}°C, Humidity: {row['humidity_pct']}%")
    return df

if __name__ == "__main__":
    ingest_weather()