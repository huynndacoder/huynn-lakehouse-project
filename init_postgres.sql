CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count TEXT,
    trip_distance REAL,
    RatecodeID TEXT,
    store_and_fwd_flag TEXT,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type TEXT,
    fare_amount REAL,
    extra REAL,
    mta_tax REAL,
    tip_amount REAL,
    tolls_amount REAL,
    improvement_surcharge REAL,
    total_amount REAL,
    congestion_surcharge REAL,
    Airport_fee REAL,
    cbd_congestion_fee REAL
);

CREATE TABLE IF NOT EXISTS nyc_weather_hourly (
    time TIMESTAMP,
    temperature REAL,
    precipitation REAL,
    humidity REAL,
    windspeed REAL
);