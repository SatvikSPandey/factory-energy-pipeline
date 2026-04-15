import os
from dotenv import load_dotenv
from prefect import flow, task
from ingest_sensors import ingest_sensors
from ingest_weather import ingest_weather
from transform import run_transform
from detect_anomalies import run_detection

load_dotenv()

@task(name="Ingest Sensor Data", retries=3, retry_delay_seconds=10)
def task_ingest_sensors():
    df = ingest_sensors()
    return len(df)

@task(name="Ingest Weather Data", retries=3, retry_delay_seconds=10)
def task_ingest_weather():
    df = ingest_weather()
    return len(df)

@task(name="Transform Data", retries=2, retry_delay_seconds=15)
def task_transform():
    df = run_transform()
    return len(df)

@task(name="Detect Anomalies", retries=2, retry_delay_seconds=15)
def task_detect_anomalies():
    df = run_detection()
    anomaly_count = df["is_anomaly"].sum()
    return int(anomaly_count)

@flow(name="Factory Energy Pipeline", log_prints=True)
def factory_energy_pipeline():
    print("Starting Factory Energy Pipeline...")

    # Run ingestion tasks
    sensor_count  = task_ingest_sensors()
    weather_count = task_ingest_weather()

    print(f"Ingested {sensor_count} sensor rows and {weather_count} weather rows")

    # Transform raw data into fact table
    fact_count = task_transform()
    print(f"Transformed {fact_count} rows into fact_energy_hourly")

    # Run anomaly detection
    anomaly_count = task_detect_anomalies()
    print(f"Pipeline complete — {anomaly_count} anomalies flagged")

if __name__ == "__main__":
    factory_energy_pipeline()