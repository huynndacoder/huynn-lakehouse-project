# NYC Taxi Real-Time Analytics Pipeline

A production-ready, dual-path analytics pipeline for real-time NYC Taxi data with Change Data Capture (CDC), supporting both instantaneous real-time queries and comprehensive historical analysis.

## Architecture Overview

### Dual-Path Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        REAL-TIME CDC PIPELINE                               │
│                                                                             │
│  ┌───────────────┐    ┌───────────────┐                                    │
│  │ Taxi Simulator │    │Weather Simul.│                                     │
│  │   (every 30s)  │    │  (every hour) │                                     │
│  └───────┬───────┘    └───────┬───────┘                                    │
│          │                    │                                             │
│          ▼                    ▼                                             │
│  ┌─────────────────────────────────────────────────────────────────┐       │
│  │                    PostgreSQL (Source DB)                       │       │
│  │         Tables: yellow_taxi_trips, nyc_weather_hourly          │       │
│  │                    Historical: 3.5M+ trips                       │       │
│  └─────────────────────────────┬───────────────────────────────────┘       │
│                                │                                            │
│                                │ Debezium CDC                               │
│                                ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────┐       │
│  │                      Apache Kafka                                │       │
│  │        Topics: postgres.public.yellow_taxi_trips, etc.          │       │
│  └─────────────────────────────┬───────────────────────────────────┘       │
│                                │                                            │
│                    ┌───────────┴───────────┐                                │
│                    │                       │                                │
│                    ▼                       ▼                                │
│  ┌──────────────────────────┐    ┌──────────────────────────┐             │
│  │  ClickHouse Kafka Engine │    │  Spark Structured Stream │             │
│  │    (Real-time Path)      │    │   (Batch/Iceberg Path)   │             │
│  └───────────┬──────────────┘    └───────────┬──────────────┘             │
│              │                               │                              │
│              ▼                               ▼                              │
│  ┌──────────────────────────┐    ┌──────────────────────────┐             │
│  │   ClickHouse MergeTree   │    │   MinIO Iceberg Tables   │             │
│  │   Real-time queries      │    │   Long-term storage      │             │
│  │   < 1s latency           │    │   Batch analytics        │             │
│  └───────────┬──────────────┘    └───────────┬──────────────┘             │
│              │                               │                              │
│              └───────────┬───────────────────┘                              │
│                          │                                                  │
│                          ▼                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐       │
│  │                    FastAPI (REST API)                            │       │
│  │            Real-time mode: ClickHouse queries                    │       │
│  │            Historical mode: PostgreSQL direct queries            │       │
│  │            Connection pooling: PostgreSQL (2-20), ClickHouse (20)│       │
│  └─────────────────────────────┬───────────────────────────────────┘       │
│                                │                                            │
│                                ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────┐       │
│  │                  Streamlit Dashboard                              │       │
│  │          Toggle between Real-Time and Historical modes           │       │
│  └─────────────────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Architecture Decisions

**Real-time Path (ClickHouse):**
- PostgreSQL → Debezium → Kafka → ClickHouse Kafka Engine → MergeTree tables
- Direct Kafka ingestion using ClickHouse Kafka engine tables
- Materialized views parse Debezium JSON format
- Sub-second query latencies for operational dashboards
- Stores recent data (last 24-48 hours typically)

**Historical Path (PostgreSQL):**
- API directly queries PostgreSQL for historical analysis
- 3.5M+ trips from January 2025 onwards
- Complex aggregations with zone/borough joins
- 1-3 second query times for large aggregations

**Batch Path (Iceberg - ML Workloads):**
- Spark writes to Iceberg usingmedallion architecture (Bronze/Silver/Gold)
- Bronze: Raw CDC data from Kafka
- Silver: Quality-checked data (fare >= 0, distance >= 0, timestamps valid)
- Gold: Pre-computed aggregations for ML feature engineering
- Independent of real-time ClickHouse path

## Dashboard Modes

| Mode | Data Source | Use Case | Date Range | Latency | Pool Size |
|------|-------------|----------|------------|---------|-----------|
| **Real-Time** | ClickHouse `taxi_prod` | Live dashboards, monitoring | Last 24h | <1s | 20 connections |
| **Historical** | PostgreSQL `yellow_taxi_trips` | Trend analysis, reporting | Jan 2025+ | 1-3s | 2-20 connections |

## Features

- **Dual-Path Architecture**: Real-time ClickHouse for dashboards, PostgreSQL for historical analysis
- **Real-time CDC**: PostgreSQL changes captured via Debezium and streamed to Kafka
- **Kafka Engine Ingestion**: ClickHouse directly consumes Kafka topics via engine tables
- **Materialized Views**: Automatic JSON parsing of Debezium format
- **Connection Pooling**: PostgreSQL (min=2, max=20), ClickHouse (size=20, overflow=10)
- **Redis Caching**: Query result caching with TTL=300s
- **Zone Analytics**: 265 NYC taxi zones with borough filtering
- **Weather Integration**: Real-time weather correlation with trip data
- **Dashboard Toggle**: Switch between real-time and historical modes

## Prerequisites

### System Requirements

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| RAM | 16 GB | 32 GB |
| CPU | 4 cores | 8 cores |
| Disk | 50 GB | 100 GB SSD |
| Docker | 20.10+ | Latest |
| Docker Compose | 2.0+ | Latest |

### Port Requirements

| Port | Service | Description |
|------|---------|-------------|
| 5432 | PostgreSQL | OLTP database |
| 6379 | Redis | Connection pooling & cache |
| 7077 | Spark Master | Spark communication |
| 8001 | Analytics API | REST API |
| 8081 | Airflow | Workflow UI |
| 8083 | Debezium | CDC connector API |
| 8090 | Spark UI | Spark Master Web UI |
| 8123 | ClickHouse | HTTP interface |
| 8505 | Dashboard | Streamlit UI |
| 9000 | MinIO | S3-compatible storage |
| 9001 | MinIO Console | Storage web UI |
| 9092 | Kafka | Message broker |

## Quick Start

### 1. Start All Services

```bash
# Start all services
docker compose up -d

# Wait for services to be healthy (2-3 minutes)
docker compose ps

```

**What happens automatically:**
- ✅ PostgreSQL schema created (`init_postgres.sql` runs automatically)
- ✅ ClickHouse schema created (`init_clickhouse.sql` runs automatically)
- ✅ MinIO bucket created by init container
- ✅ Tables created: `yellow_taxi_trips`, `nyc_weather_hourly`, `taxi_zones`, etc.

### 2. Setup ClickHouse Kafka Engine (CRITICAL!)

```bash
# Create Kafka engine tables and materialized views
docker exec clickhouse clickhouse-client --multiquery < setup_kafka_ingestion.sql

# This creates:
# - taxi_kafka_source: Consumes from Kafka topic
# - weather_kafka_source: Consumes weather data
# - taxi_kafka_parser: Materialized view to parse Debezium JSON
# - weather_kafka_parser: Materialized view for weather data
```

### 3. Load Zone Metadata

```bash
# Load NYC taxi zone lookup data (265 zones)
# This enables zone names like "JFK Airport" instead of "Zone 132"

# PostgreSQL (for historical queries)
cat data/taxi_zone_lookup.csv | docker exec -i postgres psql -U admin -d weather -c "\COPY taxi_zones FROM STDIN WITH (FORMAT csv, HEADER true)"

# ClickHouse (for real-time queries)
cat data/taxi_zone_lookup.csv | docker exec -i clickhouse clickhouse-client --query "INSERT INTO lakehouse.taxi_zones FORMAT CSVWithNames"

# Verify
docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) FROM taxi_zones"
# Should show: 265
docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM lakehouse.taxi_zones"
# Should show: 265
```

### 4. Initialize Pipeline

```bash
# Run pipeline initialization (registers Debezium, verifies Kafka topics)
./scripts/fix_pipeline.sh

# This will:
# - Wait for all services to be healthy
# - Register Debezium connector (postgres-taxi-connector)
# - Wait for Kafka topics to be created
# - Unpause Airflow DAGs
# - Show pipeline summary
```

### 5. Start Spark Streaming Jobs

```bash
# Start CDC streaming jobs
./start_spark_jobs.sh

# This will start:
# - TaxiMedallionCDC (writes to Iceberg)
# - WeatherMedallionCDC (writes to Iceberg)
# - GoldAggregations (Silver → Gold)
```

### 6. Verify Pipeline

```bash
# Check PostgreSQL (historical data)
docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) FROM yellow_taxi_trips"
# Expected: 0 (no historical data loaded yet) or 3.5M+ if loaded

# Check ClickHouse (real-time data)
docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM lakehouse.taxi_prod"
# Expected: Growing (depends on simulator activity)

# Check Kafka consumers
docker exec clickhouse clickhouse-client --query "SELECT table, num_messages_read FROM system.kafka_consumers"
# Expected: Messages being read, LAG should be 0 or growing

# Check Debezium connector
curl http://localhost:8083/connectors/postgres-taxi-connector/status | jq '.connector.state'
# Expected: "RUNNING"

# Check API health
curl http://localhost:8001/health
# Expected: {"success": true, "message": "API is operational", ...}
```

### 7. Load Historical Data (Optional)

```bash
# Download NYC taxi parquet files
mkdir -p data
cd data

# Download January 2025 data
curl -L -O "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"

# Copy to Airflow container
cd ..
docker cp data/yellow_tripdata_2025-01.parquet airflow:/opt/airflow/data/

# Unpause and trigger DAG
docker exec airflow airflow dags unpause taxi_parquet_to_postgres
docker exec airflow airflow dags trigger taxi_parquet_to_postgres
```

### 8. Access Dashboards

- **Streamlit Dashboard**: http://localhost:8505
- **API Documentation**: http://localhost:8001/docs
- **Spark UI**: http://localhost:8090
- **Airflow UI**: http://localhost:8081 (admin/Airflow password is auto-generated. Retrieve with: docker exec airflow cat /opt/airflow/standalone_admin_password.txt)
- **MinIO Console**: http://localhost:9001 (admin/admin12345)

## What's Automatic vs Manual

### Automatic (on `docker compose up -d`)

| Service | Auto-Initialization | Description |
|---------|-------------------|-------------|
| PostgreSQL | ✅ `init_postgres.sql` | Creates tables, indexes, schema |
| ClickHouse | ✅ `init_clickhouse.sql` | Creates real-time tables (taxi_prod, nyc_weather, zones) |
| MinIO | ✅ Init container | Creates `datalake` bucket |
| Taxi Simulator | ✅ Starts automatically | Generates 100 trips every 30s |
| Weather Simulator | ✅ Starts automatically | Fetches weather hourly |

### Manual Steps

| Step | Command | Purpose |
|------|---------|---------|
| 1. Kafka Engine | `docker exec clickhouse clickhouse-client --multiquery < setup_kafka_ingestion.sql` | Create Kafka consumers |
| 2. Zone Data | `cat data/taxi_zone_lookup.csv \| docker exec -i ...` | Load zone metadata |
| 3. Debezium | `./scripts/fix_pipeline.sh` | Register CDC connector |
| 4. Spark Jobs | `./start_spark_jobs.sh` | Start streaming jobs |
| 5. Historical Data | `docker exec airflow airflow dags trigger ...` | Load parquet files |

## Helper Script (huynn.sh)

The `huynn.sh` script provides comprehensive pipeline management:

### Stack Management

```bash
./huynn.sh up              # Start all services
./huynn.sh down            # Stop all services
./huynn.sh ps              # Show service status
./huynn.sh logs <service>  # View logs (e.g., logs spark-master)
./huynn.sh restart <svc>   # Restart specific service
./huynn.sh clean           # Clean up Docker resources
```

### Pipeline Operations

```bash
./huynn.sh pipeline:init      # Initialize CDC pipeline
./huynn.sh pipeline:status    # Check pipeline health
./huynn.sh pipeline:verify    # Verify data pipeline
```

### Debezium & CDC

```bash
./huynn.sh debezium:register  # Register CDC connector
./huynn.sh debezium:status    # Check connector status
./huynn.sh debezium:restart   # Restart connector
./huynn.sh debezium:delete    # Delete connector
```

### Kafka

```bash
./huynn.sh kafka:topics             # List all topics
./huynn.sh kafka:count <topic>      # Count messages in topic
./huynn.sh kafka:consume <topic>    # Consume from beginning
./huynn.sh kafka:consume-live <topic> # Live consumption
```

### Spark

```bash
./huynn.sh spark:jobs     # Start all streaming jobs
./huynn.sh spark:apps     # List running applications
./huynn.sh spark:logs     # View Spark master logs
./huynn.sh spark:submit <job.py>  # Submit custom job
```

### Databases

```bash
./huynn.sh postgres:count    # Count taxi trips
./huynn.sh postgres:range    # Show date range
./huynn.sh postgres:weather  # Count weather records
./huynn.sh clickhouse:count  # Count real-time records
./huynn.sh clickhouse:tables # List ClickHouse tables
```

### Weather

```bash
./huynn.sh weather:backfill  # Backfill historical weather
./huynn.sh weather:count     # Count and show date range
```

### API Testing

```bash
./huynn.sh api:health              # Health check
./huynn.sh api:stats               # Real-time stats
./huynn.sh api:stats-historical    # Historical stats (default: Jan 2025)
./huynn.sh api:stats-historical 2025-01-01 2025-01-07  # Custom range
./huynn.sh api:zones               # Zone analytics
./huynn.sh api:weather             # Weather impact data
```

## Data Flow

### Real-Time Flow

1. **Taxi Simulator** → PostgreSQL (every 30s, 100 trips/batch)
2. **Weather Simulator** → PostgreSQL (hourly from Open-Meteo)
3. **Debezium CDC** → Kafka topics (postgres.public.yellow_taxi_trips)
4. **ClickHouse Kafka Engine** → Consumes Kafka directly
5. **Materialized Views** → Parse Debezium JSON → MergeTree tables
6. **API** → Dashboard (sub-second query)

### Batch Flow

1. **Same CDC** → Kafka topics
2. **Spark Streaming** → Writes to Iceberg (MinIO)
3. **Medallion Architecture** → Bronze/Silver/Gold layers for ML feature engineering
4. **Long-term storage** for ML and batch analytics

**Note:** ClickHouse uses a simplified single-table architecture for real-time queries (no medallion needed). Iceberg maintains Bronze/Silver/Gold for batch ML workloads.

### Historical Flow

1. **Airflow DAG** → Loads parquet files to PostgreSQL
2. **API** → Direct PostgreSQL queries (no CDC involved)
3. **Complex joins** with zone_metadata for analytics

## API Endpoints

### Dashboard Stats

```bash
# Real-time (last 24 hours)
curl "http://localhost:8001/api/v1/dashboard/stats?mode=realtime&hours_back=24" \
  -H "X-API-Key: huynz-super-secret-key-2026"

# Historical (date range)
curl "http://localhost:8001/api/v1/dashboard/stats?mode=historical&start_date=2025-01-01&end_date=2025-01-07" \
  -H "X-API-Key: huynz-super-secret-key-2026"
```

### Zone Analytics

```bash
# Real-time zones
curl "http://localhost:8001/api/v1/analytics/zones?mode=realtime&hours_back=24&limit=10" \
  -H "X-API-Key: huynz-super-secret-key-2026"

# Historical zones
curl "http://localhost:8001/api/v1/analytics/zones?mode=historical&start_date=2025-01-01&end_date=2025-01-02" \
  -H "X-API-Key: huynz-super-secret-key-2026"
```

### Time Series

```bash
curl "http://localhost:8001/api/v1/analytics/time-series?mode=historical&metric=trip_count&interval=hour&start_date=2025-01-01&end_date=2025-01-02" \
  -H "X-API-Key: huynz-super-secret-key-2026"
```

### Weather Impact

```bash
curl "http://localhost:8001/api/v1/analytics/weather-impact?mode=historical&start_date=2025-01-01&end_date=2025-01-02" \
  -H "X-API-Key: huynz-super-secret-key-2026"
```

### Health Check

```bash
# Basic health
curl http://localhost:8001/health

# Pool metrics
curl http://localhost:8001/health/pool -H "X-API-Key: huynz-super-secret-key-2026"
```

## Connection Pooling

### PostgreSQL Pool (PostgresHistoricalService)

- **Type**: `psycopg2.pool.ThreadedConnectionPool`
- **Min Connections**: 2
- **Max Connections**: 20
- **Purpose**: Historical queries, complex analytics
- **Features**:
  - Thread-safe connection sharing
  - Automatic connection lifecycle management
  - Fallback to temporary connection if pool exhausted

### ClickHouse Pool (DatabaseService)

- **Type**: SQLAlchemy QueuePool
- **Pool Size**: 20
- **Max Overflow**: 10
- **Pool Timeout**: 30 seconds
- **Pool Recycle**: 3600 seconds (1 hour)
- **Purpose**: Real-time queries, dashboard stats
- **Features**:
  - Connection pooling with overflow
  - Pre-ping for connection validation
  - LIFO queue for connection reuse

### Redis Cache

- **Type**: Redis connection
- **TTL**: 300 seconds (5 minutes)
- **Purpose**: Query result caching
- **Features**:
  - Decorator-based caching
  - Graceful degradation if Redis unavailable
  - JSON serialization

## Troubleshooting

### Issue 1: Kafka Fails to Start (NodeExists Error)

```bash
# This is a Zookeeper znode conflict. Fix:
docker compose restart zookeeper kafka schema-registry debezium

# Wait 30 seconds and verify
./huynn.sh kafka:topics
```

### Issue 2: Debezium Connector Not Running

```bash
# Check connector status
./huynn.sh debezium:status

# If not found, register it
./huynn.sh debezium:register

# If failed, restart it
./huynn.sh debezium:restart
```

### Issue 3: No CDC Topics in Kafka

```bash
# Verify Debezium is running
./huynn.sh debezium:status

# Should show: "RUNNING"
# If not, see issue #2

# Wait 10-15 seconds after connector starts
./huynn.sh kafka:topics | grep postgres
```

### Issue 4: ClickHouse Empty but Kafka Has Messages

```bash
# Check if Kafka engine tables exist
docker exec clickhouse clickhouse-client --query "SHOW TABLES FROM lakehouse LIKE '%kafka%'"

# Should show: taxi_kafka_source, weather_kafka_source

# If missing, run setup script
docker exec clickhouse clickhouse-client --multiquery < setup_kafka_ingestion.sql

# Check consumer status
docker exec clickhouse clickhouse-client --query "SELECT table, num_messages_read FROM system.kafka_consumers"

# LAG should be 0 or growing
# If num_messages_read is 0, Kafka engine tables may not be created
```

### Issue 5: Spark Jobs Not Streaming

```bash
# Check Spark UI: http://localhost:8090
# Should show "Running Applications: 1"

# If not, start jobs manually
./huynn.sh spark:jobs

# Check logs
./huynn.sh spark:logs
```

### Issue 6: Zone Names Showing as Numbers

```bash
# Load zone lookup data into PostgreSQL
cat data/taxi_zone_lookup.csv | docker exec -i postgres psql -U admin -d weather -c "\COPY taxi_zones FROM STDIN WITH (FORMAT csv, HEADER true)"

# Load into ClickHouse
cat data/taxi_zone_lookup.csv | docker exec -i clickhouse clickhouse-client --query "INSERT INTO lakehouse.taxi_zones FORMAT CSVWithNames"

# Verify
docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM lakehouse.taxi_zones"
# Should show: 265
```

### Issue 7: Weather Simulator Connection Errors

```bash
# Check if PostgreSQL is healthy
docker compose ps postgres

# Weather simulator uses restart: unless-stopped
# It will retry automatically. Check logs:
./huynn.sh logs weather-simulator
```

### Issue 8: API Returns Empty Data for Real-time Mode

```bash
# ClickHouse might be empty if pipeline just started
# Check ClickHouse data count
docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM lakehouse.taxi_prod"

# Check Kafka consumer LAG
docker exec clickhouse clickhouse-client --query "SELECT table, assignments.current_offset FROM system.kafka_consumers"

# If LAG = 0, all messages consumed but ClickHouse might be filtering by datetime
# Check datetime filter in query (e.g., WHERE tpep_pickup_datetime >= now() - INTERVAL 24 HOUR)

# Check if simulator is running
docker logs taxi-simulator --tail 20
```

### Issue 9: Dashboard Shows "Out of range float values are not JSON compliant"

```bash
# This was fixed in database.py with _clean_for_json() function
# NaN/Infinity values are replaced with None before JSON serialization

# If still seeing this error:
# 1. Restart API container
docker compose restart analytics-api

# 2. Check if database.py has the fix
grep "_clean_for_json" serving/database.py
```

### Issue 10: Pipeline initialization fails

```bash
# Check all services are healthy
docker compose ps

# If services are unhealthy, restart:
docker compose restart

# Run initialization again
docker exec clickhouse clickhouse-client --multiquery < setup_kafka_ingestion.sql
./scripts/fix_pipeline.sh
```

### Port Conflicts

```bash
# Check what's using a port
sudo lsof -i :8081

# Kill the process
sudo kill <PID>

# Restart from scratch
docker compose down -v
docker compose up -d

# Setup pipeline (follow steps 2-5 above)
docker exec clickhouse clickhouse-client --multiquery < setup_kafka_ingestion.sql
# Load zone data (see Issue 6)
./scripts/fix_pipeline.sh
./start_spark_jobs.sh
```

# Project Structure
```
Huynz/
├── docker-compose.yml           # Container orchestration
├── README.md                    # This file
├── huynn.sh                     # Helper CLI script
├── start_spark_jobs.sh          # Spark job starter
├── init_minio.sh                # MinIO bucket setup
├── debezium.json                # Debezium connector config
├── requirements.txt             # Python dependencies
│
├── init_postgres.sql            # PostgreSQL schema (auto-loaded)
├── init_clickhouse.sql          # ClickHouse schema (auto-loaded)
├── setup_kafka_ingestion.sql    # Kafka engine tables (MANUAL)
├── backfill_historical_data.sql # Historical data backfill
│
├── spark/                       # Spark custom image
│   └── Dockerfile
│
├── spark_jobs/                  # Spark streaming jobs
│   ├── taxi_cdc.py              # Taxi CDC → Iceberg
│   ├── weather_cdc.py           # Weather CDC → Iceberg
│   ├── gold_aggregations.py     # Silver → Gold
│   ├── load_zones.py            # Zone metadata loader
│   ├── taxi_simulator.py        # Real-time trip generator
│   └── weather_simulator.py     # Weather data fetcher
│
├── serving/                     # API & Dashboard
│   ├── api.py                   # FastAPI endpoints
│   ├── cache.py                 # Redis caching layer
│   ├── client.py                # API client example
│   ├── config.py                # Configuration
│   ├── dashboard.py             # Streamlit dashboard
│   ├── database.py              # Database query services
│   ├── models.py                # Pydantic models
│   ├── requirements.txt         # Python dependencies
│   └── Dockerfile               # API/Dashboard image
│
├── dags/                        # Airflow DAGs
│   ├── spark_cdc_medallion_pipeline.py
│   ├── taxi_ingestion_pipeline.py
│   └── weather_to_postgres.py
│
├── scripts/                     # Utility scripts
│   ├── fix_pipeline.sh          # Pipeline initialization
│   └── backfill_weather.sh      # Historical weather backfill
│
└── data/                        # Data directory (gitignored)
    └── taxi_zone_lookup.csv     # NYC taxi zone metadata
```

## Performance Notes

### Resource Allocation

- **Spark Resources**: 4 cores, 2GB per executor
- **Streaming Triggers (Iceberg medallion layers)**: 
  - Bronze/Silver: 30 seconds
  - Gold: 300 seconds
- **Connection Pools**:
  - PostgreSQL: min=2, max=20
  - ClickHouse: size=20, overflow=10

### Query Latency

| Query Type | Database | Typical Latency |
|------------|-----------|------------------|
| Real-time stats | ClickHouse | <1s |
| Historical stats | PostgreSQL | 1-3s |
| Zone analytics | ClickHouse | <1s |
| Zone analytics | PostgreSQL | 2-5s |
| Time series | ClickHouse | <1s |
| Time series | PostgreSQL | 1-3s |
| Weather impact | ClickHouse | <1s |
| Weather impact | PostgreSQL | 2-4s |

### Timezone

All timestamps in GMT+7 (Asia/Bangkok)

### Data Freshness

| Data Type | Source | Update Frequency |
|-----------|--------|------------------|
| Taxi Trips | Simulator | Every 30 seconds (100 trips) |
| Weather | Open-Meteo API | Hourly |
| ClickHouse Real-time | Kafka Engine | Sub-second (continuous) |
| Iceberg Batch | Spark CDC | Every 30 seconds |
| Historical Data | Airflow DAG | Manual trigger |

## Architecture Decisions

### Why Dual Path?

**Real-time Path (ClickHouse):**
- Instant dashboard updates (<1s latency)
- High concurrency (20+ connections)
- Real-time monitoring and alerting
- Efficient aggregation queries
- Kafka engine for direct ingestion

**Historical Path (PostgreSQL):**
- Full historical dataset (millions of rows)
- Complex joins with zone metadata
- Ad-hoc analytical queries
- No overhead on real-time pipeline
- Proven relational query optimizer

**Batch Path (Iceberg/Spark - Optional):**
- Long-term data archival
- Complex ETL transformations
- Machine learning feature engineering
- Independent of real-time queries
- Point-in-time queries and time travel

### Why Kafka Engine Instead of Spark → ClickHouse?

1. **Latency**: Direct Kafka ingestion has sub-second latency vs 30+ seconds through Spark
2. **Simplicity**: No need for Spark cluster to write to ClickHouse
3. **Reliability**: Kafka engine is battle-tested for exactly-once semantics
4. **Resource Efficiency**: ClickHouse handles ingestion without external dependencies
5. **Dual Consumer**: Both ClickHouse and Spark can consume from same Kafka topic

### Why Not ClickHouse for Historical?

1. **Query Complexity**: Historical queries need complex zone joins
2. **Data Volume**: PostgreSQL is optimized for OLTP-style queries
3. **Cost**: Storing months of data in ClickHouse is expensive
4. **Flexibility**: PostgreSQL offers more query flexibility for ad-hoc analysis

## Monitoring

### Real-time Pipeline

```bash
# Check ClickHouse ingestion rate
docker exec clickhouse clickhouse-client --query "
SELECT 
  table,
  num_messages_read,
  last_poll_time
FROM system.kafka_consumers"

# Check Kafka consumer lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group clickhouse_taxi_consumer

# Check ClickHouse record count
docker exec clickhouse clickhouse-client --query "
SELECT 
  COUNT(*) as total,
  MIN(tpep_pickup_datetime) as earliest,
  MAX(tpep_pickup_datetime) as latest
FROM lakehouse.taxi_prod"
```

### Historical Pipeline

```bash
# Check PostgreSQL record count
docker exec postgres psql -U admin -d weather -c "
SELECT 
  COUNT(*) as total,
  MIN(DATE(tpep_pickup_datetime)) as earliest,
  MAX(DATE(tpep_pickup_datetime)) as latest
FROM yellow_taxi_trips"

# Check PostgreSQL connection pool
curl http://localhost:8001/health/pool -H "X-API-Key: huynz-super-secret-key-2026" | jq '.postgresql_pool'
```

### Pipeline Health

```bash
# Run comprehensive health check
./huynn.sh pipeline:status

# Check Debezium connector
curl http://localhost:8083/connectors/postgres-taxi-connector/status | jq '.connector.state'
# Expected: "RUNNING"

# Check Spark jobs
docker exec spark-master ps aux | grep spark_jobs | wc -l
# Expected: 3 (TaxiMedallionCDC, WeatherMedallionCDC, GoldAggregations)
```

## Data Examples

### Dashboard Stats Response

```json
{
  "success": true,
  "message": "Stats retrieved",
  "data": {
    "total_trips": 18000,
    "total_revenue": 808978.97,
    "avg_fare": 42.93,
    "active_zones": 265,
    "top_zones": [
      {
        "zone_name": "JFK Airport",
        "trips": 11176,
        "revenue": 802846.60
      },
      {
        "zone_name": "Midtown Center",
        "trips": 7606,
        "revenue": 174591.77
      }
    ],
    "temperature": 5.9,
    "humidity": 78.0,
    "precipitation": 0.0
  }
}
```

### Zone Analytics Response

```json
{
  "success": true,
  "message": "Zone analytics retrieved",
  "data": [
    {
      "zone_id": 132,
      "zone_name": "JFK Airport",
      "borough": "Queens",
      "pickups": 11176,
      "revenue": 802846.60,
      "avg_fare": 56.16,
      "distance": 15.08,
      "dropoffs": 10555
    }
  ]
}
```

## License

MIT