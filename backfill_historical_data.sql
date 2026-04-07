-- Backfill Historical Data from Kafka
-- This script recreates Kafka engine tables to consume from offset 0

-- =============================================================================
-- STOP EXISTING INGESTION
-- =============================================================================

-- Drop materialized views first (they depend on source tables)
DROP VIEW IF EXISTS lakehouse.taxi_kafka_parser;
DROP VIEW IF EXISTS lakehouse.weather_kafka_parser;

-- Drop Kafka engine tables
DROP TABLE IF EXISTS lakehouse.taxi_kafka_source;
DROP TABLE IF EXISTS lakehouse.weather_kafka_source;

-- =============================================================================
-- CLEAR EXISTING DATA (to avoid duplicates)
-- =============================================================================

-- Clear existing real-time data to avoid duplicates
TRUNCATE TABLE lakehouse.taxi_prod;
TRUNCATE TABLE lakehouse.silver_taxi_trips;
TRUNCATE TABLE lakehouse.nyc_weather;

-- =============================================================================
-- RECREATE KAFKA ENGINE TABLES (WITH EARLIEST OFFSET)
-- =============================================================================

-- Taxi trips Kafka source - consume from BEGINNING
CREATE TABLE lakehouse.taxi_kafka_source
(
    data String
)
ENGINE = Kafka()
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'postgres.public.yellow_taxi_trips',
    kafka_group_name = 'clickhouse_taxi_historical_consumer',
    kafka_format = 'JSONAsString',
    kafka_auto_offset_reset = 'earliest',  -- Start from offset 0
    kafka_max_poll_records = 10000,         -- Batch size for backfill
    kafka_poll_timeout_ms = 1000;

-- Weather Kafka source - consume from BEGINNING
CREATE TABLE lakehouse.weather_kafka_source
(
    data String
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'postgres.public.nyc_weather_hourly',
    kafka_group_name = 'clickhouse_weather_historical_consumer',
    kafka_format = 'JSONAsString',
    kafka_auto_offset_reset = 'earliest',
    kafka_max_poll_records = 10000,
    kafka_poll_timeout_ms = 1000;

-- =============================================================================
-- RECREATE MATERIALIZED VIEWS
-- =============================================================================

-- Parse Debezium JSON and insert to taxi_prod
CREATE MATERIALIZED VIEW lakehouse.taxi_kafka_parser TO lakehouse.taxi_prod AS
SELECT
    JSONExtractInt(data, 'after', 'vendorid') as vendorid,
    toDateTime(JSONExtractUInt(data, 'after', 'tpep_pickup_datetime') / 1000000) as tpep_pickup_datetime,
    toDateTime(JSONExtractUInt(data, 'after', 'tpep_dropoff_datetime') / 1000000) as tpep_dropoff_datetime,
    JSONExtractInt(data, 'after', 'passenger_count') as passenger_count,
    JSONExtractFloat(data, 'after', 'trip_distance') as trip_distance,
    JSONExtractInt(data, 'after', 'ratecodeid') as ratecodeid,
    JSONExtractString(data, 'after', 'store_and_fwd_flag') as store_and_fwd_flag,
    JSONExtractInt(data, 'after', 'pulocationid') as pulocationid,
    JSONExtractInt(data, 'after', 'dolocationid') as dolocationid,
    JSONExtractInt(data, 'after', 'payment_type') as payment_type,
    JSONExtractFloat(data, 'after', 'fare_amount') as fare_amount,
    JSONExtractFloat(data, 'after', 'extra') as extra,
    JSONExtractFloat(data, 'after', 'mta_tax') as mta_tax,
    JSONExtractFloat(data, 'after', 'tip_amount') as tip_amount,
    JSONExtractFloat(data, 'after', 'tolls_amount') as tolls_amount,
    JSONExtractFloat(data, 'after', 'improvement_surcharge') as improvement_surcharge,
    JSONExtractFloat(data, 'after', 'total_amount') as total_amount,
    JSONExtractFloat(data, 'after', 'congestion_surcharge') as congestion_surcharge,
    JSONExtractFloat(data, 'after', 'airport_fee') as airport_fee,
    JSONExtractFloat(data, 'after', 'cbd_congestion_fee') as cbd_congestion_fee,
    now() as _source_time
FROM lakehouse.taxi_kafka_source
WHERE JSONExtractString(data, 'op') IN ('c', 'r', 'u');  -- create, read (snapshot), update

-- Parse Weather Debezium JSON and insert to nyc_weather
CREATE MATERIALIZED VIEW lakehouse.weather_kafka_parser TO lakehouse.nyc_weather AS
SELECT
    toDateTime(JSONExtractUInt(data, 'after', 'time') / 1000000) as time,
    JSONExtractFloat(data, 'after', 'temperature') as temperature,
    JSONExtractFloat(data, 'after', 'precipitation') as precipitation,
    JSONExtractFloat(data, 'after', 'humidity') as humidity,
    JSONExtractFloat(data, 'after', 'windspeed') as windspeed,
    now() as _source_time
FROM lakehouse.weather_kafka_source
WHERE JSONExtractString(data, 'op') IN ('c', 'r', 'u');

-- =============================================================================
-- POPULATE SILVER LAYER
-- =============================================================================

-- Populate silver_taxi_trips from taxi_prod (cleaned data)
INSERT INTO lakehouse.silver_taxi_trips
SELECT 
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    now() as _processing_time
FROM lakehouse.taxi_prod
WHERE fare_amount > 0 
  AND trip_distance > 0 
  AND tpep_pickup_datetime < now();

-- =============================================================================
-- VERIFY DATA
-- =============================================================================

SELECT 'taxi_prod' as table_name, COUNT(*) as records FROM lakehouse.taxi_prod
UNION ALL
SELECT 'silver_taxi_trips' as table_name, COUNT(*) as records FROM lakehouse.silver_taxi_trips
UNION ALL
SELECT 'nyc_weather' as table_name, COUNT(*) as records FROM lakehouse.nyc_weather;

SELECT 
    'Date Range Check' as check_name,
    MIN(tpep_pickup_datetime) as min_date,
    MAX(tpep_pickup_datetime) as max_date,
    COUNT(*) as total_records
FROM lakehouse.taxi_prod;