import requests
import pandas as pd

url = "https://archive-api.open-meteo.com/v1/archive"

params = {
    "latitude": 40.7128,
    "longitude": -74.0060,
    "start_date": "2025-01-01",
    "end_date": "2025-03-31",
    "hourly": "temperature_2m,precipitation,relative_humidity_2m,windspeed_10m",
    "timezone": "America/New_York"
}

response = requests.get(url, params=params)
data = response.json()

df = pd.DataFrame(data["hourly"])

print(df.head())

df["time"] = pd.to_datetime(df["time"])
df.to_csv("nyc_weather_q1_2025.csv", index=False)