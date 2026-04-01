-- ClickHouse Iceberg Native Tables
-- These tables READ from MinIO Iceberg storage (written by Spark CDC)
-- No CDC sync needed - ClickHouse reads Iceberg metadata and fetches Parquet files directly

-- Create database
CREATE DATABASE IF NOT EXISTS lakehouse;

-- Taxi trips (matches Spark output: lakehouse.taxi_prod)
CREATE TABLE IF NOT EXISTS lakehouse.taxi_prod
(
    vendorid Nullable(Int32),
    tpep_pickup_datetime Nullable(DateTime),
    tpep_dropoff_datetime Nullable(DateTime),
    passenger_count Nullable(Int32),
    trip_distance Nullable(Float32),
    ratecodeid Nullable(Int32),
    store_and_fwd_flag Nullable(String),
    pulocationid Nullable(Int32),
    dolocationid Nullable(Int32),
    payment_type Nullable(Int32),
    fare_amount Nullable(Float32),
    extra Nullable(Float32),
    mta_tax Nullable(Float32),
    tip_amount Nullable(Float32),
    tolls_amount Nullable(Float32),
    improvement_surcharge Nullable(Float32),
    total_amount Nullable(Float32),
    congestion_surcharge Nullable(Float32),
    airport_fee Nullable(Float32),
    cbd_congestion_fee Nullable(Float32)
)
ENGINE = Iceberg(
    'http://minio:9000/datalake/warehouse/taxi_prod/',
    'admin',
    'admin12345'
);

-- Weather data (matches Spark output: lakehouse.nyc_weather)
CREATE TABLE IF NOT EXISTS lakehouse.nyc_weather
(
    time Nullable(DateTime),
    temperature Nullable(Float32),
    precipitation Nullable(Float32),
    humidity Nullable(Float32),
    windspeed Nullable(Float32)
)
ENGINE = Iceberg(
    'http://minio:9000/datalake/warehouse/nyc_weather/',
    'admin',
    'admin12345'
);

-- Taxi zones lookup (matches Spark output: lakehouse.taxi_zones)
CREATE TABLE IF NOT EXISTS lakehouse.taxi_zones
(
    LocationID Nullable(Int32),
    Borough Nullable(String),
    Zone Nullable(String),
    service_zone Nullable(String)
)
ENGINE = Iceberg(
    'http://minio:9000/datalake/warehouse/taxi_zones/',
    'admin',
    'admin12345'
);
