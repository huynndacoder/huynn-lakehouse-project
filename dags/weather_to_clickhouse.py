from airflow.decorators import dag, task
from datetime import datetime
import requests
import pandas as pd
import clickhouse_connect


@dag(
    dag_id="weather_to_clickhouse",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False
)
def weather_pipeline():

    @task
    def extract_weather():

        url = "https://archive-api.open-meteo.com/v1/archive"

        params = {
            "latitude": 40.7128,
            "longitude": -74.0060,
            "start_date": "2025-01-01",
            "end_date": "2025-03-31",
            "hourly": "temperature_2m,precipitation,relative_humidity_2m,windspeed_10m",
            "timezone": "America/New_York"
        }

        r = requests.get(url, params=params)
        return r.json()["hourly"]


    @task
    def load_clickhouse(hourly_data):

        df = pd.DataFrame(hourly_data)
        df["time"] = pd.to_datetime(df["time"])

        client = clickhouse_connect.get_client(
            host="clickhouse",
            port=8123,
            username="airflow",
            password="airflow",
            database="weather"
        )

        rows = list(df.itertuples(index=False, name=None))

        client.insert(
            "nyc_weather_hourly",
            rows,
            column_names=[
                "time",
                "temperature_2m",
                "precipitation",
                "relative_humidity_2m",
                "windspeed_10m"
            ]
        )


    load_clickhouse(extract_weather())


weather_pipeline()