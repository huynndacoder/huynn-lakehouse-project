import requests
import psycopg2
import time
import signal
import sys
from datetime import datetime, timedelta, timezone

TIMEZONE_OFFSET = timezone(timedelta(hours=7))  # GMT+7

OPEN_METEO_API = "https://api.open-meteo.com/v1/forecast"
POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "weather",
    "user": "admin",
    "password": "admin",
}
INTERVAL_SECONDS = 3600
MAX_RETRIES = 5
RETRY_DELAY = 60
REQUEST_TIMEOUT = 30

running = True


def signal_handler(signum, frame):
    global running
    print("\nShutdown signal received, finishing current iteration...")
    running = False


def fetch_weather():
    params = {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "hourly": "temperature_2m,precipitation,relative_humidity_2m,windspeed_10m",
        "forecast_days": 7,
        "timezone": "Asia/Bangkok",  # GMT+7 
    }

    response = requests.get(OPEN_METEO_API, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()

    if "hourly" not in data:
        raise ValueError("Invalid API response: missing 'hourly' key")

    hourly = data["hourly"]
    current_hour_idx = 0

    return (
        hourly["time"][current_hour_idx],
        hourly["temperature_2m"][current_hour_idx],
        hourly["precipitation"][current_hour_idx],
        hourly["relative_humidity_2m"][current_hour_idx],
        hourly["windspeed_10m"][current_hour_idx],
    )


def insert_weather(conn, weather):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO public.nyc_weather_hourly 
            (time, temperature, precipitation, humidity, windspeed)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (time) DO UPDATE SET
                temperature = EXCLUDED.temperature,
                precipitation = EXCLUDED.precipitation,
                humidity = EXCLUDED.humidity,
                windspeed = EXCLUDED.windspeed
        """,
            weather,
        )
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()


def get_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)


def main():
    global running
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    print(f"Weather Simulator started - Fetching every {INTERVAL_SECONDS}s")

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

            weather = fetch_weather()
            insert_weather(conn, weather)

            print(
                f"Weather updated: {weather[1]}C, {weather[2]}mm rain at {weather[0]} (GMT+7: {datetime.now(TIMEZONE_OFFSET).strftime('%H:%M:%S')})"
            )

            time.sleep(INTERVAL_SECONDS)

        except requests.exceptions.Timeout:
            print(f"Request timeout after {REQUEST_TIMEOUT}s, retrying...")
            time.sleep(RETRY_DELAY)

        except requests.exceptions.RequestException as e:
            print(f"HTTP request error: {e}")
            time.sleep(RETRY_DELAY)

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

    print("Weather Simulator stopped.")


if __name__ == "__main__":
    main()
