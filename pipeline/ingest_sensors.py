import os
import random
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

MACHINES = ["MCH-001", "MCH-002", "MCH-003", "MCH-004", "MCH-005"]

MACHINE_PROFILES = {
    "MCH-001": {"base_kwh": 40.0, "base_units": 120, "capacity": 45.0},
    "MCH-002": {"base_kwh": 10.0, "base_units": 500, "capacity": 12.0},
    "MCH-003": {"base_kwh": 27.0, "base_units": 80,  "capacity": 30.0},
    "MCH-004": {"base_kwh": 22.0, "base_units": 60,  "capacity": 25.0},
    "MCH-005": {"base_kwh": 16.0, "base_units": 200, "capacity": 18.0},
}

def generate_status():
    roll = random.random()
    if roll < 0.75:
        return "running"
    elif roll < 0.90:
        return "idle"
    else:
        return "fault"

def generate_sensor_reading(machine_id, recorded_at):
    profile = MACHINE_PROFILES[machine_id]
    status = generate_status()

    if status == "running":
        energy = round(random.gauss(profile["base_kwh"], profile["base_kwh"] * 0.1), 3)
        units  = int(random.gauss(profile["base_units"], profile["base_units"] * 0.1))
    elif status == "idle":
        energy = round(random.uniform(0.5, 2.0), 3)
        units  = 0
    else:  # fault
        energy = round(random.uniform(0.1, 0.5), 3)
        units  = 0

    # Occasional spike anomaly for ML to detect later
    if random.random() < 0.03:
        energy = round(profile["capacity"] * random.uniform(1.2, 1.8), 3)

    temp = round(random.gauss(65.0, 8.0), 2)

    return {
        "machine_id":       machine_id,
        "recorded_at":      recorded_at,
        "energy_kwh":       max(0.0, energy),
        "temperature_c":    temp,
        "production_units": max(0, units),
        "machine_status":   status,
    }

def ingest_sensors():
    recorded_at = datetime.now(timezone.utc)
    rows = [generate_sensor_reading(m, recorded_at) for m in MACHINES]
    df = pd.DataFrame(rows)

    with engine.begin() as conn:
        df.to_sql("raw_sensor_data", conn, if_exists="append", index=False)

    print(f"[ingest_sensors] {len(df)} rows written at {recorded_at}")
    return df

if __name__ == "__main__":
    ingest_sensors()