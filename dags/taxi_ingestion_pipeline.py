"""
Airflow DAG for ingesting NYC Yellow Taxi parquet data into PostgreSQL.
Automatically fetches historical weather data for parquet date ranges.
"""

from airflow.decorators import dag, task
from datetime import datetime, timezone, timedelta
import duckdb
import psycopg2
import psycopg2.extras
import glob
import requests
import logging

TIMEZONE_OFFSET = timezone(timedelta(hours=7))
OPEN_METEO_ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"
NYC_LATITUDE = 40.7128
NYC_LONGITUDE = -74.0060
REQUEST_TIMEOUT = 300  # 5 minutes for large date ranges

logger = logging.getLogger(__name__)


@dag(
    dag_id="taxi_ingestion_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    description="Ingests NYC Yellow Taxi parquet files and fetches corresponding weather data",
)
def taxi_pipeline():

    @task
    def get_parquet_date_ranges():
        """Scan parquet files and extract date ranges for weather fetching."""
        files = glob.glob("/opt/airflow/data/yellow_tripdata_*.parquet")

        if not files:
            logger.warning("No parquet files found in /opt/airflow/data")
            return []

        logger.info(f"Found {len(files)} parquet file(s)")

        conn = psycopg2.connect(
            host="postgres", user="admin", password="admin", dbname="weather"
        )
        cur = conn.cursor()

        unprocessed_files = []

        for file in files:
            # Check if file already processed
            cur.execute("SELECT 1 FROM ingestion_log WHERE file_name = %s", (file,))
            if cur.fetchone():
                logger.info(f"Skipping {file} (already processed)")
                continue

            # Extract date range from parquet file
            try:
                duck_conn = duckdb.connect()
                result = duck_conn.execute(f"""
                    SELECT 
                        MIN(CAST(tpep_pickup_datetime AS DATE)) as start_date,
                        MAX(CAST(tpep_pickup_datetime AS DATE)) as end_date,
                        COUNT(*) as total_rows
                    FROM read_parquet('{file}')
                """).fetchone()

                duck_conn.close()

                if result and result[0] and result[1]:
                    unprocessed_files.append(
                        {
                            "file": file,
                            "start_date": result[0],
                            "end_date": result[1],
                            "total_rows": result[2],
                        }
                    )
                    logger.info(
                        f"File {file}: {result[0]} to {result[1]}, {result[2]:,} rows"
                    )

            except Exception as e:
                logger.error(f"Error reading {file}: {e}")
                continue

        cur.close()
        conn.close()

        return unprocessed_files

    @task
    def fetch_weather_for_ranges(parquet_ranges):
        """Fetch historical weather data from Open-Meteo Archive API."""
        if not parquet_ranges:
            logger.info("No unprocessed parquet files, skipping weather fetch")
            return []

        conn = psycopg2.connect(
            host="postgres", user="admin", password="admin", dbname="weather"
        )
        cur = conn.cursor()

        # Get existing weather coverage
        cur.execute("""
            SELECT MIN(DATE(time)), MAX(DATE(time))
            FROM nyc_weather_hourly
        """)
        weather_min, weather_max = cur.fetchone()

        weather_ranges_to_fetch = []

        for parquet in parquet_ranges:
            start = parquet["start_date"]
            end = parquet["end_date"]

            # Don't fetch future weather (only historical data)
            yesterday = (datetime.now() - timedelta(days=1)).date()
            if end > yesterday:
                end = yesterday

            # Check if we need to fetch weather for this range
            need_fetch = False
            fetch_start = None
            fetch_end = None

            if weather_min is None:
                # No weather data at all
                need_fetch = True
                fetch_start = start
                fetch_end = end
            elif start < weather_min:
                # Gap before existing weather data
                need_fetch = True
                fetch_start = start
                fetch_end = min(weather_min - timedelta(days=1), end)
            elif end > weather_max:
                # Gap after existing weather data
                need_fetch = True
                fetch_start = max(weather_max + timedelta(days=1), start)
                fetch_end = end

            if need_fetch and fetch_start and fetch_end:
                weather_ranges_to_fetch.append(
                    {
                        "parquet_file": parquet["file"],
                        "start": fetch_start,
                        "end": fetch_end,
                    }
                )
                logger.info(
                    f"Need weather for {parquet['file']}: {fetch_start} to {fetch_end}"
                )
            else:
                logger.info(f"Weather already covered for {parquet['file']}")

        # Fetch and insert weather data
        total_weather_records = 0

        for weather_range in weather_ranges_to_fetch:
            try:
                logger.info(
                    f"Fetching weather: {weather_range['start']} to {weather_range['end']}"
                )

                # Build API URL
                url = (
                    f"{OPEN_METEO_ARCHIVE_API}"
                    f"?latitude={NYC_LATITUDE}"
                    f"&longitude={NYC_LONGITUDE}"
                    f"&start_date={weather_range['start']}"
                    f"&end_date={weather_range['end']}"
                    f"&hourly=temperature_2m,precipitation,relative_humidity_2m,windspeed_10m"
                    f"&timezone=America%2FNew_York"
                )

                # Fetch weather data
                response = requests.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                data = response.json()

                if "hourly" not in data:
                    logger.error(
                        f"Invalid API response for {weather_range['start']} to {weather_range['end']}"
                    )
                    continue

                hourly = data["hourly"]
                times = hourly["time"]
                temperatures = hourly["temperature_2m"]
                precipitation = hourly["precipitation"]
                humidity = hourly["relative_humidity_2m"]
                windspeed = hourly["windspeed_10m"]

                # Insert into PostgreSQL
                insert_query = """
                    INSERT INTO nyc_weather_hourly 
                    (time, temperature, precipitation, humidity, windspeed)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (time) DO UPDATE SET
                        temperature = EXCLUDED.temperature,
                        precipitation = EXCLUDED.precipitation,
                        humidity = EXCLUDED.humidity,
                        windspeed = EXCLUDED.windspeed
                """

                rows = list(
                    zip(times, temperatures, precipitation, humidity, windspeed)
                )
                psycopg2.extras.execute_batch(cur, insert_query, rows, page_size=100)
                conn.commit()

                total_weather_records += len(rows)
                logger.info(
                    f"Inserted {len(rows)} weather records for {weather_range['start']} to {weather_range['end']}"
                )

            except requests.Timeout:
                logger.error(
                    f"Timeout fetching weather for {weather_range['start']} to {weather_range['end']}"
                )
                # Continue with other ranges
            except requests.RequestException as e:
                logger.error(
                    f"API error for {weather_range['start']} to {weather_range['end']}: {e}"
                )
            except Exception as e:
                logger.error(f"Unexpected error fetching weather: {e}")
                conn.rollback()

        cur.close()
        conn.close()

        logger.info(f"Total weather records inserted: {total_weather_records}")

        return parquet_ranges

    @task
    def ingest_taxi_data(parquet_ranges):
        """Load parquet files into PostgreSQL."""
        if not parquet_ranges:
            logger.info("No parquet files to process")
            return

        conn = psycopg2.connect(
            host="postgres", user="admin", password="admin", dbname="weather"
        )
        cur = conn.cursor()
        duck_conn = duckdb.connect()

        insert_query = """
            INSERT INTO public.yellow_taxi_trips (
                vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, 
                passenger_count, trip_distance, ratecodeid, 
                store_and_fwd_flag, pulocationid, dolocationid, 
                payment_type, fare_amount, extra, mta_tax, 
                tip_amount, tolls_amount, improvement_surcharge, 
                total_amount, congestion_surcharge, airport_fee
            ) VALUES %s
        """

        total_files = 0
        total_rows = 0

        for parquet in parquet_ranges:
            file = parquet["file"]

            # Check if already processed
            cur.execute("SELECT 1 FROM ingestion_log WHERE file_name = %s", (file,))
            if cur.fetchone():
                logger.info(f"Skipping {file} (already processed)")
                continue

            logger.info(f"Processing {file}...")

            # Stream data from parquet
            res = duck_conn.execute(f"""
                SELECT 
                    VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, 
                    passenger_count, trip_distance, RatecodeID, 
                    store_and_fwd_flag, PULocationID, DOLocationID, 
                    payment_type, fare_amount, extra, mta_tax, 
                    tip_amount, tolls_amount, improvement_surcharge, 
                    total_amount, congestion_surcharge, Airport_fee
                FROM read_parquet('{file}')
            """)

            file_rows = 0
            while True:
                chunk = res.fetchmany(10000)
                if not chunk:
                    break

                psycopg2.extras.execute_values(cur, insert_query, chunk)
                file_rows += len(chunk)
                conn.commit()

                if file_rows % 100000 == 0:
                    logger.info(f"Progress: {file_rows:,} rows from {file}")

            # Mark file as processed
            cur.execute("INSERT INTO ingestion_log (file_name) VALUES (%s)", (file,))
            conn.commit()

            logger.info(f"Inserted {file_rows:,} rows from {file}")
            total_files += 1
            total_rows += file_rows

        cur.close()
        conn.close()
        duck_conn.close()

        logger.info(f"Completed: {total_files} files, {total_rows:,} total rows")
        logger.info(
            f"Finished at {datetime.now(TIMEZONE_OFFSET).strftime('%Y-%m-%d %H:%M:%S (GMT+7)')}"
        )

    # Define task flow: Scan parquet → Fetch weather → Load taxi
    parquet_ranges = get_parquet_date_ranges()
    weather_ready = fetch_weather_for_ranges(parquet_ranges)
    ingest_taxi_data(weather_ready)


taxi_pipeline()
