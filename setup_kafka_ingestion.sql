-- ClickHouse Kafka Ingestion Setup

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
-- _source_time is used by ReplacingMergeTree for deduplication
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
    JSONExtractFloat(data, 'after', 'cbd_congestion_fee') as cbd_congestion_fee,
    now() as _source_time
FROM lakehouse.taxi_kafka_source
WHERE JSONExtractString(data, 'op') IN ('c', 'r', 'u');

-- Materialized view to parse and insert weather data
-- _source_time is used by ReplacingMergeTree for deduplication
CREATE MATERIALIZED VIEW IF NOT EXISTS lakehouse.weather_kafka_parser TO lakehouse.nyc_weather AS
SELECT
    toDateTime(JSONExtractUInt(data, 'after', 'time') / 1000000) as time,
    JSONExtractFloat(data, 'after', 'temperature') as temperature,
    JSONExtractFloat(data, 'after', 'precipitation') as precipitation,
    JSONExtractFloat(data, 'after', 'humidity') as humidity,
    JSONExtractFloat(data, 'after', 'windspeed') as windspeed,
    now() as _source_time
FROM lakehouse.weather_kafka_source
WHERE JSONExtractString(data, 'op') IN ('c', 'r', 'u');


-- Create the Target Table (ReplacingMergeTree handles updates to zones)
CREATE TABLE IF NOT EXISTS lakehouse.taxi_zones (
    LocationID Int32,
    Borough String,
    Zone String,
    service_zone String
) ENGINE = ReplacingMergeTree() ORDER BY LocationID;

-- Create the Kafka Source Engine
CREATE TABLE IF NOT EXISTS lakehouse.zones_kafka_source (data String)
ENGINE = Kafka() 
SETTINGS 
    kafka_broker_list = 'kafka:9092', 
    kafka_topic_list = 'postgres.public.taxi_zones', 
    kafka_group_name = 'clickhouse_zones_consumer', 
    kafka_format = 'JSONAsString';

-- Create the Materialized View Parser
CREATE MATERIALIZED VIEW IF NOT EXISTS lakehouse.zones_kafka_parser TO lakehouse.taxi_zones AS
SELECT
    JSONExtractInt(data, 'after', 'LocationID') as LocationID,
    JSONExtractString(data, 'after', 'Borough') as Borough,
    JSONExtractString(data, 'after', 'Zone') as Zone,
    JSONExtractString(data, 'after', 'service_zone') as service_zone
FROM lakehouse.zones_kafka_source
WHERE JSONExtractString(data, 'op') IN ('c', 'r', 'u');