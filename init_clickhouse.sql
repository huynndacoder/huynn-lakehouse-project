CREATE TABLE IF NOT EXISTS yellow_taxi_trips
(
    VendorID Int32,
    tpep_pickup_datetime DateTime,
    tpep_dropoff_datetime DateTime,
    passenger_count String,
    trip_distance Float32,
    RatecodeID String,
    store_and_fwd_flag String,
    PULocationID Int32,
    DOLocationID Int32,
    payment_type String,
    fare_amount Float32,
    extra Float32,
    mta_tax Float32,
    tip_amount Float32,
    tolls_amount Float32,
    improvement_surcharge Float32,
    total_amount Float32,
    congestion_surcharge Float32,
    Airport_fee Float32,
    cbd_congestion_fee Float32
)
ENGINE = MergeTree
ORDER BY tpep_pickup_datetime;

CREATE TABLE IF NOT EXISTS nyc_weather_hourly
(
    time DateTime,
    temperature Float32,
    precipitation Float32,
    humidity Float32,
    windspeed Float32
)
ENGINE = MergeTree
ORDER BY time;