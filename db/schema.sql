-- ============================================================
-- Factory Energy Pipeline — Database Schema
-- Run this script in Supabase SQL Editor to set up all tables
-- ============================================================

-- Raw sensor data (exactly as ingested, never modified)
CREATE TABLE IF NOT EXISTS raw_sensor_data (
    id BIGSERIAL PRIMARY KEY,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    machine_id VARCHAR(20) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    energy_kwh FLOAT,
    temperature_c FLOAT,
    production_units INTEGER,
    machine_status VARCHAR(20)
);

-- Raw weather data (exactly as ingested, never modified)
CREATE TABLE IF NOT EXISTS raw_weather_data (
    id BIGSERIAL PRIMARY KEY,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    recorded_at TIMESTAMPTZ NOT NULL,
    location VARCHAR(50),
    ambient_temp_c FLOAT,
    humidity_pct FLOAT
);

-- Machine reference data (dimension table)
CREATE TABLE IF NOT EXISTS dim_machines (
    machine_id VARCHAR(20) PRIMARY KEY,
    machine_name VARCHAR(100),
    machine_type VARCHAR(50),
    factory_zone VARCHAR(20),
    rated_capacity_kwh FLOAT
);

-- Transformed, joined, analytics-ready fact table
CREATE TABLE IF NOT EXISTS fact_energy_hourly (
    id BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL,
    machine_id VARCHAR(20),
    energy_kwh FLOAT,
    temperature_c FLOAT,
    production_units INTEGER,
    machine_status VARCHAR(20),
    ambient_temp_c FLOAT,
    humidity_pct FLOAT,
    rolling_24h_avg_kwh FLOAT,
    is_peak_hour BOOLEAN,
    efficiency_ratio FLOAT,
    is_anomaly BOOLEAN DEFAULT FALSE,
    transformed_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed machine reference data
INSERT INTO dim_machines (machine_id, machine_name, machine_type, factory_zone, rated_capacity_kwh)
VALUES
    ('MCH-001', 'Press Line Alpha',     'hydraulic_press', 'Zone-A', 45.0),
    ('MCH-002', 'Conveyor Belt Beta',   'conveyor',        'Zone-A', 12.0),
    ('MCH-003', 'CNC Mill Gamma',       'cnc_mill',        'Zone-B', 30.0),
    ('MCH-004', 'Welding Station Delta','welding',         'Zone-B', 25.0),
    ('MCH-005', 'Assembly Robot Epsilon','robotic_arm',    'Zone-C', 18.0)
ON CONFLICT (machine_id) DO NOTHING;