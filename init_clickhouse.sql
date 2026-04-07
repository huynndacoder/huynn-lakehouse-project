-- ClickHouse Real-time Tables (Kafka Engine)
-- Reads directly from Kafka for real-time ingestion

CREATE DATABASE IF NOT EXISTS lakehouse;

-- =============================================================================
-- BRONZE LAYER (MergeTree - Final Data Storage)
-- =============================================================================

-- Taxi trips - final storage
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

-- Weather data - final storage
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
-- SILVER LAYER (Cleaned Data)
-- =============================================================================

CREATE TABLE IF NOT EXISTS lakehouse.silver_taxi_trips
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
    _processing_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(tpep_pickup_datetime)
ORDER BY (tpep_pickup_datetime, pulocationid)
TTL tpep_pickup_datetime + INTERVAL 90 DAY;

-- =============================================================================
-- LOOKUP TABLES
-- =============================================================================

-- Taxi zones lookup (static data - manually populated)
CREATE TABLE IF NOT EXISTS lakehouse.taxi_zones
(
    LocationID Int32,
    Borough String,
    Zone String,
    service_zone String
)
ENGINE = MergeTree()
ORDER BY LocationID;

-- =============================================================================
-- GOLD LAYER (Aggregated Analytics - as Views for real-time)
-- =============================================================================

-- Gold: Zone performance summary view
CREATE VIEW IF NOT EXISTS lakehouse.gold_zone_performance AS
SELECT
    COALESCE(t.pulocationid, 0) as zone_id,
    IF(z.Zone = '' OR z.Zone IS NULL, 'Zone ' || toString(COALESCE(t.pulocationid, 0)), z.Zone) as zone_name,
    IF(z.Borough = '' OR z.Borough IS NULL, 'Unknown', z.Borough) as borough,
    COUNT(*) as total_trips,
    SUM(t.total_amount) as total_revenue,
    AVG(t.fare_amount) as avg_fare,
    AVG(t.trip_distance) as avg_distance
FROM lakehouse.taxi_prod t
LEFT JOIN lakehouse.taxi_zones z ON t.pulocationid = z.LocationID
GROUP BY t.pulocationid, z.Zone, z.Borough;

-- Gold: Hourly metrics view
CREATE VIEW IF NOT EXISTS lakehouse.gold_hourly_metrics AS
SELECT
    toDate(tpep_pickup_datetime) as metric_date,
    toHour(tpep_pickup_datetime) as metric_hour,
    pulocationid,
    dolocationid,
    IF(z.Borough = '' OR z.Borough IS NULL, 'Unknown', z.Borough) as borough,
    COUNT(*) as trip_count,
    SUM(total_amount) as total_revenue,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    AVG(passenger_count) as avg_passengers,
    SUM(tip_amount) as tip_sum,
    SUM(tolls_amount) as tolls_sum,
    toHour(tpep_pickup_datetime) as peak_hour
FROM lakehouse.taxi_prod t
LEFT JOIN lakehouse.taxi_zones z ON t.pulocationid = z.LocationID
GROUP BY metric_date, metric_hour, pulocationid, dolocationid, z.Borough;

-- Gold: Borough summary view
CREATE VIEW IF NOT EXISTS lakehouse.gold_borough_summary AS
SELECT
    IF(z.Borough = '' OR z.Borough IS NULL, 'Unknown', z.Borough) as borough,
    toDate(tpep_pickup_datetime) as metric_date,
    COUNT(*) as total_trips,
    SUM(total_amount) as total_revenue,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance,
    COUNT(*) as trip_count_pickup,
    COUNT(*) as trip_count_dropoff
FROM lakehouse.taxi_prod t
LEFT JOIN lakehouse.taxi_zones z ON t.pulocationid = z.LocationID
GROUP BY z.Borough, metric_date;