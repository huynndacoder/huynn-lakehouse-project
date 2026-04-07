#!/bin/bash
# backfill_weather.sh - Fetch and insert historical weather data from Open-Meteo API
# Usage: ./scripts/backfill_weather.sh

set -e

echo "=========================================="
echo "Weather Data Backfill Script"
echo "=========================================="
echo ""
echo "This script fetches historical weather data from Open-Meteo API"
echo "and inserts it into PostgreSQL for historical analysis."
echo ""

# Calculate dynamic date range
START_DATE="2024-12-31"
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d 2>/dev/null || date -v-1d +%Y-%m-%d)

echo "Date range: $START_DATE to $YESTERDAY"
echo ""

# Run Python script inside analytics-api container
docker exec analytics-api python3 << 'PYEOF'
import requests
import psycopg2
import psycopg2.extras
import sys
from datetime import datetime, timedelta

# Calculate date range: from Dec 31, 2024 to yesterday
start_date = "2024-12-31"
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

url = (
    f"https://archive-api.open-meteo.com/v1/archive"
    f"?latitude=40.7128"
    f"&longitude=-74.0060"
    f"&start_date={start_date}"
    f"&end_date={yesterday}"
    f"&hourly=temperature_2m,precipitation,relative_humidity_2m,windspeed_10m"
    f"&timezone=America%2FNew_York"
)

print(f"Fetching weather data from {start_date} to {yesterday}...")
print(f"URL: {url}")
print("")

try:
    response = requests.get(url, timeout=300)
    response.raise_for_status()
    data = response.json()
except requests.exceptions.Timeout:
    print("ERROR: API request timed out after 300 seconds")
    sys.exit(1)
except requests.exceptions.RequestException as e:
    print(f"ERROR: API request failed: {e}")
    sys.exit(1)

if 'hourly' not in data:
    print(f"ERROR: Invalid API response: {data}")
    sys.exit(1)

hourly = data['hourly']
print(f"Received {len(hourly['time'])} hourly records")

# Check for missing data
if len(hourly['time']) == 0:
    print("ERROR: No hourly data received")
    sys.exit(1)

# Connect to PostgreSQL
print("Connecting to PostgreSQL...")
try:
    conn = psycopg2.connect(
        host='postgres',
        port=5432,
        database='weather',
        user='admin',
        password='admin'
    )
    cur = conn.cursor()
except Exception as e:
    print(f"ERROR: Failed to connect to PostgreSQL: {e}")
    sys.exit(1)

# Prepare insert query with upsert
insert_query = """
    INSERT INTO nyc_weather_hourly (time, temperature, precipitation, humidity, windspeed)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (time) DO UPDATE SET
        temperature = EXCLUDED.temperature,
        precipitation = EXCLUDED.precipitation,
        humidity = EXCLUDED.humidity,
        windspeed = EXCLUDED.windspeed
"""

# Prepare rows
rows = list(zip(
    hourly['time'],
    hourly['temperature_2m'],
    hourly['precipitation'],
    hourly['relative_humidity_2m'],
    hourly['windspeed_10m']
))

print(f"Inserting/updating {len(rows)} weather records...")

try:
    psycopg2.extras.execute_batch(cur, insert_query, rows, page_size=100)
    conn.commit()
    print(f"✓ Successfully processed {len(rows)} weather records")
except Exception as e:
    conn.rollback()
    print(f"ERROR: Failed to insert data: {e}")
    sys.exit(1)
finally:
    cur.close()
    conn.close()

# Verify data
try:
    conn = psycopg2.connect(
        host='postgres',
        port=5432,
        database='weather',
        user='admin',
        password='admin'
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*), MIN(time), MAX(time) FROM nyc_weather_hourly")
    count, min_time, max_time = cur.fetchone()
    print(f"✓ Total weather records: {count}")
    print(f"✓ Date range: {min_time} to {max_time}")
    cur.close()
    conn.close()
except Exception as e:
    print(f"WARNING: Could not verify data: {e}")

print("")
print("Weather data backfill complete!")
PYEOF

echo ""
echo "=========================================="
echo "Weather backfill complete!"
echo "=========================================="
echo ""
echo "Verify with: ./huynn.sh weather:count"