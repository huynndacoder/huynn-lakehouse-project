import psycopg2
import psycopg2.extras
import random
import time
import signal
import sys
from datetime import datetime, timedelta

POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "weather",
    "user": "admin",
    "password": "admin",
}

BATCH_SIZE = 100
INTERVAL_SECONDS = 30
MAX_RETRIES = 5
RETRY_DELAY = 10

NYC_ZONES = list(range(1, 266))
PAYMENT_TYPES = [1, 2, 3, 4, 5]

running = True


def signal_handler(signum, frame):
    global running
    print("\nShutdown signal received, finishing current batch...")
    running = False


def generate_trip():
    pickup = datetime.now()
    trip_duration = random.randint(300, 3600)
    dropoff = pickup + timedelta(seconds=trip_duration)

    passenger_count = random.randint(1, 6)
    trip_distance = round(random.uniform(0.5, 30.0), 2)
    fare_amount = round(2.5 + trip_distance * 2.5 + random.uniform(0, 5), 2)

    return (
        random.choice([1, 2]),
        pickup,
        dropoff,
        passenger_count,
        trip_distance,
        random.randint(1, 9),
        random.choice(["N", "Y"]),
        random.choice(NYC_ZONES),
        random.choice(NYC_ZONES),
        random.choice(PAYMENT_TYPES),
        fare_amount,
        round(random.uniform(0, 2.5), 2),
        round(random.uniform(0, 0.5), 2),
        round(random.uniform(0, fare_amount * 0.3), 2),
        0,
        round(random.uniform(0, 1), 2),
        fare_amount + random.uniform(0, 5),
        round(random.uniform(0, 2.5), 2),
        round(random.uniform(0, 1.25), 2),
        0,
    )


def get_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)


def insert_trips_batch(conn, trips):
    cursor = conn.cursor()
    try:
        insert_query = """
            INSERT INTO public.yellow_taxi_trips (
                vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
                passenger_count, trip_distance, ratecodeid, store_and_fwd_flag,
                pulocationid, dolocationid, payment_type, fare_amount, extra,
                mta_tax, tip_amount, tolls_amount, improvement_surcharge,
                total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee
            ) VALUES %s
        """
        psycopg2.extras.execute_values(cursor, insert_query, trips)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()


def main():
    global running
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    print(
        f"Taxi Simulator started - Batch size: {BATCH_SIZE}, Interval: {INTERVAL_SECONDS}s"
    )

    conn = None
    retries = 0

    while True:
        if not running:
            break
        try:
            if conn is None or conn.closed:
                conn = get_connection()
                retries = 0
                print("Connected to PostgreSQL")

            trips = [generate_trip() for _ in range(BATCH_SIZE)]
            insert_trips_batch(conn, trips)

            print(
                f"Inserted {BATCH_SIZE} trips at {datetime.now().strftime('%H:%M:%S')}"
            )

            time.sleep(INTERVAL_SECONDS)

        except psycopg2.OperationalError as e:
            print(f"Database connection error: {e}")
            if conn:
                conn.close()
                conn = None
            retries += 1
            if retries >= MAX_RETRIES:
                print(f"Max retries ({MAX_RETRIES}) reached, exiting...")
                sys.exit(1)
            print(f"Retrying in {RETRY_DELAY}s (attempt {retries}/{MAX_RETRIES})...")
            time.sleep(RETRY_DELAY)

        except KeyboardInterrupt:
            print("\nKeyboard interrupt received, shutting down...")
            running = False

        except Exception as e:
            print(f"Unexpected error: {e}")
            time.sleep(RETRY_DELAY)

    if conn and not conn.closed:
        conn.close()
        print("Database connection closed.")

    print("Taxi Simulator stopped.")


if __name__ == "__main__":
    main()
