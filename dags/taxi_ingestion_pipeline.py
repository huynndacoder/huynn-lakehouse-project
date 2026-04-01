from airflow.decorators import dag, task
from datetime import datetime
import duckdb
import psycopg2
import psycopg2.extras
import glob

@dag(
    dag_id="taxi_parquet_to_postgres",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
)
def taxi_pipeline():

    @task
    def ingest_taxi():
        files = glob.glob("/opt/airflow/data/yellow_tripdata_2025-*.parquet")
        
        print("FILES FOUND:", files)
        if not files:
            raise ValueError("No parquet files found in /opt/airflow/data")

        conn = psycopg2.connect(
            host="postgres", user="admin",
            password="admin", dbname="weather"
        )
        cur = conn.cursor()
        con = duckdb.connect()

        # Pre-compile the insert query
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

        for file in files:
            cur.execute("SELECT 1 FROM ingestion_log WHERE file_name = %s", (file,))
            if cur.fetchone():
                print(f"Skipping {file}")
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

            total_inserted = 0
            while True:
                chunk = res.fetchmany(10000)
                if not chunk:
                    break 
                
                psycopg2.extras.execute_values(cur, insert_query, chunk)
                total_inserted += len(chunk)
                
                conn.commit()

                if total_inserted % 100000 == 0:
                    print(f"Progress: Inserted {total_inserted} rows...")

            # 3. Log the file as completed
            cur.execute("INSERT INTO ingestion_log (file_name) VALUES (%s)", (file,))
            print(f"✅ Successfully inserted ALL {total_inserted} rows from {file}")

        cur.close()
        conn.close()

    ingest_taxi()

taxi_pipeline()