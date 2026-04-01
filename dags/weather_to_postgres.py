from airflow.decorators import dag, task
from datetime import datetime
import duckdb


@dag(
    dag_id="weather_to_postgres",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
def weather_pipeline():

    @task
    def ingest_weather():
        con = duckdb.connect()

        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute("SET force_download=true")
        con.execute("INSTALL json")
        con.execute("LOAD json")
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")

        con.execute("""
        ATTACH 'dbname=weather user=admin password=admin host=postgres'
        AS postgres_db (TYPE POSTGRES)
        """)

        url = (
            "https://archive-api.open-meteo.com/v1/archive"
            "?latitude=40.7128"
            "&longitude=-74.0060"
            "&start_date=2025-01-01"
            "&end_date=2026-12-31"
            "&hourly=temperature_2m,precipitation,relative_humidity_2m,windspeed_10m"
            "&timezone=America/New_York"
        )

        con.execute(f"""
        INSERT INTO postgres_db.public.nyc_weather_hourly
        SELECT
            strptime(unnest(hourly.time), '%Y-%m-%dT%H:%M')::TIMESTAMP AS time,
            unnest(hourly.temperature_2m)       AS temperature,
            unnest(hourly.precipitation)        AS precipitation,
            unnest(hourly.relative_humidity_2m) AS humidity,
            unnest(hourly.windspeed_10m)        AS windspeed
        FROM read_json_auto('{url}')
        """)

    ingest_weather()


weather_pipeline()
