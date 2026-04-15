import os
import sys
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Add project root to path so pipeline modules can be found
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.ingest_sensors import ingest_sensors
from pipeline.ingest_weather import ingest_weather
from pipeline.transform import run_transform
from pipeline.detect_anomalies import run_detection

load_dotenv()

# Works locally via .env and on Streamlit Cloud via st.secrets
DATABASE_URL = st.secrets.get("DATABASE_URL", None) or os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

st.set_page_config(
    page_title="Factory Energy Dashboard",
    page_icon="⚡",
    layout="wide"
)

st.title("⚡ Factory Energy Operations Dashboard")
st.caption("Real-time energy consumption analytics — Powered by a live data pipeline")

# ── Run Pipeline Button ──────────────────────────────────────────────────────
if st.button("🔄 Run Pipeline Now"):
    with st.spinner("Running pipeline..."):
        ingest_sensors()
        ingest_weather()
        run_transform()
        run_detection()
    st.success("Pipeline complete! Data refreshed.")
    st.rerun()

# ── Load Data ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_data():
    with engine.connect() as conn:
        fact = pd.read_sql("""
            SELECT f.*, d.machine_name, d.machine_type, d.factory_zone, d.rated_capacity_kwh
            FROM fact_energy_hourly f
            LEFT JOIN dim_machines d ON f.machine_id = d.machine_id
            ORDER BY f.recorded_at DESC
        """, conn)
        weather = pd.read_sql("""
            SELECT * FROM raw_weather_data ORDER BY recorded_at DESC LIMIT 50
        """, conn)
    return fact, weather

fact_df, weather_df = load_data()

if fact_df.empty:
    st.warning("No data yet. Click 'Run Pipeline Now' to generate data.")
    st.stop()

# ── KPI Cards ────────────────────────────────────────────────────────────────
st.subheader("📊 Key Metrics")
col1, col2, col3, col4 = st.columns(4)

total_kwh      = fact_df["energy_kwh"].sum().round(2)
anomaly_count  = fact_df["is_anomaly"].sum()
fault_machines = fact_df[fact_df["machine_status"] == "fault"]["machine_id"].nunique()
avg_efficiency = fact_df["efficiency_ratio"].mean().round(2)

col1.metric("Total Energy (kWh)", f"{total_kwh}")
col2.metric("Anomalies Detected", f"{int(anomaly_count)}")
col3.metric("Machines in Fault", f"{int(fault_machines)}")
col4.metric("Avg Efficiency (units/kWh)", f"{avg_efficiency}")

st.divider()

# ── Energy Consumption by Machine ────────────────────────────────────────────
st.subheader("🔋 Energy Consumption by Machine")

fig1 = px.line(
    fact_df.sort_values("recorded_at"),
    x="recorded_at",
    y="energy_kwh",
    color="machine_id",
    title="Energy Consumption Over Time",
    labels={"recorded_at": "Time", "energy_kwh": "Energy (kWh)", "machine_id": "Machine"},
    template="plotly_dark"
)

# Overlay anomaly markers in red
anomalies = fact_df[fact_df["is_anomaly"] == True]
if not anomalies.empty:
    fig1.add_trace(go.Scatter(
        x=anomalies["recorded_at"],
        y=anomalies["energy_kwh"],
        mode="markers",
        marker=dict(color="red", size=12, symbol="x"),
        name="Anomaly"
    ))

st.plotly_chart(fig1, use_container_width=True)

st.divider()

# ── Machine Status Distribution ───────────────────────────────────────────────
st.subheader("🏭 Machine Status Distribution")
col_a, col_b = st.columns(2)

status_counts = fact_df["machine_status"].value_counts().reset_index()
status_counts.columns = ["Status", "Count"]

fig2 = px.pie(
    status_counts,
    names="Status",
    values="Count",
    title="Machine Status Breakdown",
    color_discrete_map={"running": "green", "idle": "yellow", "fault": "red"},
    template="plotly_dark"
)
col_a.plotly_chart(fig2, use_container_width=True)

# ── Efficiency by Machine ─────────────────────────────────────────────────────
eff_df = fact_df.groupby("machine_id")["efficiency_ratio"].mean().reset_index()
eff_df.columns = ["Machine", "Avg Efficiency"]

fig3 = px.bar(
    eff_df,
    x="Machine",
    y="Avg Efficiency",
    title="Average Efficiency by Machine (units/kWh)",
    template="plotly_dark",
    color="Avg Efficiency",
    color_continuous_scale="greens"
)
col_b.plotly_chart(fig3, use_container_width=True)

st.divider()

# ── Weather vs Energy Correlation ─────────────────────────────────────────────
st.subheader("🌡️ Weather vs Energy Correlation")

if "ambient_temp_c" in fact_df.columns and fact_df["ambient_temp_c"].notna().any():
    fig4 = px.scatter(
        fact_df,
        x="ambient_temp_c",
        y="energy_kwh",
        color="machine_id",
        title="Ambient Temperature vs Energy Consumption",
        labels={"ambient_temp_c": "Ambient Temp (°C)", "energy_kwh": "Energy (kWh)"},
        template="plotly_dark"
    )
    st.plotly_chart(fig4, use_container_width=True)
else:
    st.info("Weather correlation will appear after more pipeline runs.")

st.divider()

# ── Raw Data Table ────────────────────────────────────────────────────────────
st.subheader("📋 Latest Raw Readings")
display_cols = ["recorded_at", "machine_id", "machine_name", "energy_kwh",
                "machine_status", "production_units", "is_anomaly", "ambient_temp_c"]
st.dataframe(
    fact_df[display_cols].head(20),
    use_container_width=True
)