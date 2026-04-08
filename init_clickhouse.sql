-- ClickHouse Real-time Tables
-- Simplified architecture: Single table for real-time queries
-- Medallion layers (Bronze/Silver/Gold) remain in Iceberg for ML workloads

CREATE DATABASE IF NOT EXISTS lakehouse;

-- =============================================================================
-- REAL-TIME DATA TABLES
-- =============================================================================

-- Taxi trips - real-time storage from Kafka
CREATE TABLE IF NOT EXISTS lakehouse.taxi_prod
(
    vendorid Int32,
    tpep_pickup_datetime DateTime,
    tpep_dropoff_datetime DateTime,
    passenger_count Int32,
    trip_distance Float32,
    ratecodeid Int32,
    store_and_fwd_flag String,
    pulocationid Int32,
    dolocationid Int32,
    payment_type Int32,
    fare_amount Float32,
    extra Float32,
    mta_tax Float32,
    tip_amount Float32,
    tolls_amount Float32,
    improvement_surcharge Float32,
    total_amount Float32,
    congestion_surcharge Float32,
    airport_fee Float32,
    cbd_congestion_fee Float32,
    _source_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(tpep_pickup_datetime)
ORDER BY (tpep_pickup_datetime, pulocationid)
TTL tpep_pickup_datetime + INTERVAL 365 DAY;

-- Weather data - real-time storage from Kafka
CREATE TABLE IF NOT EXISTS lakehouse.nyc_weather
(
    time DateTime,
    temperature Float32,
    precipitation Float32,
    humidity Float32,
    windspeed Float32,
    _source_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY time
TTL time + INTERVAL 365 DAY;

-- =============================================================================
-- LOOKUP TABLES
-- =============================================================================

-- Taxi zones lookup (265 NYC taxi zones)
CREATE TABLE IF NOT EXISTS lakehouse.taxi_zones
(
    LocationID Int32,
    Borough String,
    Zone String,
    service_zone String
)
ENGINE = MergeTree()
ORDER BY LocationID;