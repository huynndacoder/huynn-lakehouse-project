"""
Airflow DAG for ingesting NYC Yellow Taxi parquet data into PostgreSQL.
Implements incremental ingestion with deduplication via ingestion_log table.
"""

from airflow.decorators import dag, task
from datetime import datetime, timezone, timedelta
import duckdb
import psycopg2
import psycopg2.extras
import glob

TIMEZONE_OFFSET = timezone(timedelta(hours=7))


@dag(
    dag_id="taxi_parquet_to_postgres",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    description="Ingests NYC Yellow Taxi parquet files into PostgreSQL",
)
def taxi_pipeline():

    @task
    def ingest_taxi():
        files = glob.glob("/opt/airflow/data/yellow_tripdata_*.parquet")

        print(f"FILES FOUND: {files}")
        if not files:
            raise ValueError("No parquet files found in /opt/airflow/data")

        conn = psycopg2.connect(
            host="postgres", user="admin", password="admin", dbname="weather"
        )
        cur = conn.cursor()
        con = duckdb.connect()

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

        for file in files:
            cur.execute("SELECT 1 FROM ingestion_log WHERE file_name = %s", (file,))
            if cur.fetchone():
                print(f"Skipping {file} (already processed)")
                continue

            print(f"Starting ingestion for {file}...")

            res = con.execute(f"""
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
                    print(f"Progress: {file_rows} rows from {file}...")

            cur.execute("INSERT INTO ingestion_log (file_name) VALUES (%s)", (file,))
            conn.commit()

            print(f"Inserted {file_rows} rows from {file}")
            total_files += 1
            total_rows += file_rows

        cur.close()
        conn.close()

        print(f"Completed: {total_files} files, {total_rows} total rows")
        print(
            f"Finished at {datetime.now(TIMEZONE_OFFSET).strftime('%Y-%m-%d %H:%M:%S (GMT+7)')}"
        )

    ingest_taxi()


taxi_pipeline()
