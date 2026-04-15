import os
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

def load_raw_data():
    with engine.connect() as conn:
        sensors = pd.read_sql("SELECT * FROM raw_sensor_data ORDER BY recorded_at DESC LIMIT 500", conn)
        weather = pd.read_sql("SELECT * FROM raw_weather_data ORDER BY recorded_at DESC LIMIT 500", conn)
    return sensors, weather

def join_weather(sensors, weather):
    # Round timestamps to nearest minute for joining
    sensors["join_key"] = pd.to_datetime(sensors["recorded_at"]).dt.floor("min")
    weather["join_key"] = pd.to_datetime(weather["recorded_at"]).dt.floor("min")

    # Keep only the weather columns we need
    weather_slim = weather[["join_key", "ambient_temp_c", "humidity_pct"]]

    # Left join — every sensor row gets the nearest weather reading
    merged = pd.merge_asof(
        sensors.sort_values("join_key"),
        weather_slim.sort_values("join_key"),
        on="join_key",
        direction="nearest"
    )
    return merged

def engineer_features(df):
    df = df.copy()
    df["recorded_at"] = pd.to_datetime(df["recorded_at"], utc=True)

    # Peak hour flag — factory peak shift is 8am to 8pm IST (UTC+5:30)
    hour_ist = (df["recorded_at"].dt.hour + 5) % 24
    df["is_peak_hour"] = hour_ist.between(8, 20)

    # Rolling 24h average energy per machine
    df = df.sort_values(["machine_id", "recorded_at"])
    df["rolling_24h_avg_kwh"] = (
        df.groupby("machine_id")["energy_kwh"]
        .transform(lambda x: x.rolling(window=24, min_periods=1).mean())
        .round(3)
    )

    # Efficiency ratio — production units per kWh (avoid divide by zero)
    df["efficiency_ratio"] = (
        df["production_units"] / df["energy_kwh"].replace(0, float("nan"))
    ).round(3)

    return df

def write_to_fact(df):
    columns = [
        "recorded_at", "machine_id", "energy_kwh", "temperature_c",
        "production_units", "machine_status", "ambient_temp_c",
        "humidity_pct", "rolling_24h_avg_kwh", "is_peak_hour",
        "efficiency_ratio"
    ]
    fact_df = df[columns].copy()
    fact_df["is_anomaly"] = False
    fact_df["transformed_at"] = datetime.now(timezone.utc)

    with engine.begin() as conn:
        # Clear and rewrite — keeps fact table fresh on every run
        conn.execute(text("DELETE FROM fact_energy_hourly"))
        fact_df.to_sql("fact_energy_hourly", conn, if_exists="append", index=False)

    print(f"[transform] {len(fact_df)} rows written to fact_energy_hourly")
    return fact_df

def run_transform():
    sensors, weather = load_raw_data()
    merged = join_weather(sensors, weather)
    featured = engineer_features(merged)
    fact_df = write_to_fact(featured)
    return fact_df

if __name__ == "__main__":
    run_transform()