-- ClickHouse Kafka Ingestion Setup
-- Run this AFTER the base tables are created

-- Create Kafka source table (single String column for raw JSON)
CREATE TABLE IF NOT EXISTS lakehouse.taxi_kafka_source
(
    data String
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'postgres.public.yellow_taxi_trips',
    kafka_group_name = 'clickhouse_taxi_consumer',
    kafka_format = 'JSONAsString',
    kafka_max_block_size = 1048576;

-- Create Kafka source table for weather
CREATE TABLE IF NOT EXISTS lakehouse.weather_kafka_source
(
    data String
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'postgres.public.nyc_weather_hourly',
    kafka_group_name = 'clickhouse_weather_consumer',
    kafka_format = 'JSONAsString',
    kafka_max_block_size = 1048576;

-- Materialized view to parse and insert taxi trips
CREATE MATERIALIZED VIEW IF NOT EXISTS lakehouse.taxi_kafka_parser TO lakehouse.taxi_prod AS
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
    JSONExtractFloat(data, 'after', 'cbd_congestion_fee') as cbd_congestion_fee
FROM lakehouse.taxi_kafka_source
WHERE JSONExtractString(data, 'op') IN ('c', 'r', 'u');  -- create, read, update

-- Materialized view to parse and insert weather data
CREATE MATERIALIZED VIEW IF NOT EXISTS lakehouse.weather_kafka_parser TO lakehouse.nyc_weather AS
SELECT
    toDateTime(JSONExtractUInt(data, 'after', 'time') / 1000000) as time,
    JSONExtractFloat(data, 'after', 'temperature') as temperature,
    JSONExtractFloat(data, 'after', 'precipitation') as precipitation,
    JSONExtractFloat(data, 'after', 'humidity') as humidity,
    JSONExtractFloat(data, 'after', 'windspeed') as windspeed
FROM lakehouse.weather_kafka_source
WHERE JSONExtractString(data, 'op') IN ('c', 'r', 'u');