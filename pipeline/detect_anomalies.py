import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sklearn.ensemble import IsolationForest

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

def load_fact_data():
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM fact_energy_hourly", conn)
    return df

def detect_anomalies(df):
    # Features the model uses to judge what is normal vs anomalous
    features = ["energy_kwh", "temperature_c", "rolling_24h_avg_kwh", "efficiency_ratio"]

    # Drop rows where any feature is null
    model_df = df[features].copy()
    model_df = model_df.fillna(0)

    # Isolation Forest — contamination=0.05 means we expect ~5% anomalies
    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,
        random_state=42
    )

    predictions = model.fit_predict(model_df)

    # IsolationForest returns -1 for anomaly, 1 for normal
    # We convert to True/False
    df = df.copy()
    df["is_anomaly"] = predictions == -1

    anomaly_count = df["is_anomaly"].sum()
    print(f"[detect_anomalies] {anomaly_count} anomalies detected out of {len(df)} rows")

    return df

def write_anomalies(df):
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("""
                    UPDATE fact_energy_hourly
                    SET is_anomaly = :is_anomaly
                    WHERE id = :id
                """),
                {"is_anomaly": bool(row["is_anomaly"]), "id": int(row["id"])}
            )
    print(f"[detect_anomalies] Anomaly flags written back to fact_energy_hourly")

def run_detection():
    df = load_fact_data()
    df = detect_anomalies(df)
    write_anomalies(df)
    return df

if __name__ == "__main__":
    run_detection()