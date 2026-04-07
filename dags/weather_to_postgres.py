"""
Airflow DAG for ingesting historical weather data from Open-Meteo API
into PostgreSQL for correlation analysis with taxi trip data.
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import psycopg2
import psycopg2.extras


@dag(
    dag_id="weather_to_postgres",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    description="Ingests historical weather data from Open-Meteo API",
)
def weather_pipeline():

    @task
    def ingest_weather():
        # Calculate date range (past 30 days to yesterday)
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        url = (
            f"https://archive-api.open-meteo.com/v1/archive"
            f"?latitude=40.7128"
            f"&longitude=-74.0060"
            f"&start_date={start_date}"
            f"&end_date={end_date}"
            f"&hourly=temperature_2m,precipitation,relative_humidity_2m,windspeed_10m"
            f"&timezone=America%2FNew_York"
        )

        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()

        if "hourly" not in data:
            raise ValueError("Invalid API response: missing 'hourly' data")

        hourly = data["hourly"]
        times = hourly["time"]
        temperatures = hourly["temperature_2m"]
        precipitation = hourly["precipitation"]
        humidity = hourly["relative_humidity_2m"]
        windspeed = hourly["windspeed_10m"]

        conn = psycopg2.connect(
            host="postgres", database="weather", user="admin", password="admin"
        )
        cur = conn.cursor()

        insert_query = """
            INSERT INTO nyc_weather_hourly (time, temperature, precipitation, humidity, windspeed)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (time) DO UPDATE SET
                temperature = EXCLUDED.temperature,
                precipitation = EXCLUDED.precipitation,
                humidity = EXCLUDED.humidity,
                windspeed = EXCLUDED.windspeed
        """

        rows = list(zip(times, temperatures, precipitation, humidity, windspeed))
        psycopg2.extras.execute_batch(cur, insert_query, rows)
        conn.commit()

        cur.close()
        conn.close()

        print(f"Weather data ingested successfully: {len(rows)} records")

    ingest_weather()


weather_pipeline()
