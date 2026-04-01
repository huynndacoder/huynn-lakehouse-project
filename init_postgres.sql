CREATE TABLE IF NOT EXISTS ingestion_log (
    file_name TEXT PRIMARY KEY,
    ingested_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
    id BIGSERIAL PRIMARY KEY,

    vendorid INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance REAL,
    ratecodeid INTEGER,
    store_and_fwd_flag TEXT,
    pulocationid INTEGER,
    dolocationid INTEGER,
    payment_type INTEGER,
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

CREATE INDEX IF NOT EXISTS idx_taxi_pickup_datetime ON yellow_taxi_trips(tpep_pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_taxi_pulocationid ON yellow_taxi_trips(pulocationid);
CREATE INDEX IF NOT EXISTS idx_taxi_dolocationid ON yellow_taxi_trips(dolocationid);
CREATE INDEX IF NOT EXISTS idx_taxi_vendorid ON yellow_taxi_trips(vendorid);
CREATE INDEX IF NOT EXISTS idx_taxi_pickup_location_time ON yellow_taxi_trips(pulocationid, tpep_pickup_datetime);

CREATE TABLE IF NOT EXISTS nyc_weather_hourly (
    id SERIAL PRIMARY KEY,
    time TIMESTAMP,
    temperature REAL,
    precipitation REAL,
    humidity REAL,
    windspeed REAL
);

CREATE INDEX IF NOT EXISTS idx_weather_time ON nyc_weather_hourly(time);
