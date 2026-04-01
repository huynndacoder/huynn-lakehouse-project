# NYC Taxi Real-Time Analytics Pipeline

A complete CDC (Change Data Capture) pipeline for real-time NYC Taxi data analytics using PostgreSQL, Debezium, Kafka, Spark Structured Streaming, Iceberg, and ClickHouse.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│  HISTORICAL DATA (One-time via Airflow)                                              │
│                                                                                       │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐      ┌────────────┐  │
│  │  NYC Taxi    │      │              │      │              │      │            │  │
│  │  Parquet    │─────▶│   Airflow    │─────▶│  PostgreSQL  │      │            │  │
│  │  Files      │      │   (DAGs)     │      │   (OLTP)    │      │            │  │
│  └──────────────┘      └──────────────┘      └──────┬───────┘      │            │  │
│                                                       │               │            │  │
│  ┌──────────────┐      ┌──────────────┐            │               │            │  │
│  │  Open-Meteo │─────▶│   Airflow    │───────────┘               │            │  │
│  │  Archive    │      │   (DAGs)    │───────────────────────────┘            │  │
│  └──────────────┘      └──────────────┘                                     │            │  │
│                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                           │
                                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│  REAL-TIME CDC PIPELINE                                                               │
│                                                                                       │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐           │
│  │            │     │             │     │             │     │             │           │
│  │ PostgreSQL │────▶│  Debezium   │────▶│    Kafka    │────▶│    Spark    │           │
│  │  (OLTP)    │ CDC │   (CDC)     │     │             │     │  Streaming  │           │
│  │            │     │             │     │             │     │             │           │
│  └─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘           │
│                                                                       │                 │
│                                                                       ▼                 │
│                                                               ┌─────────────┐         │
│                                                               │             │         │
│                                                               │   MinIO     │         │
│                                                               │  (Iceberg)  │         │
│                                                               │             │         │
│                                                               └──────┬──────┘         │
│                                                                      │                 │
│                                                                      ▼                 │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐         │
│  │            │     │             │     │             │     │             │         │
│  │ Dashboard  │◀────│    API      │◀────│ ClickHouse  │◀────│   MinIO     │         │
│  │(Streamlit)│     │ (FastAPI)   │     │  (Iceberg)  │     │             │         │
│  │            │     │             │     │             │     │             │         │
│  └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘         │
│                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## Features

- **Real-time CDC**: PostgreSQL changes captured via Debezium and streamed to Kafka
- **Micro-batch Processing**: Spark Structured Streaming with configurable batches
- **Iceberg Data Lake**: Transactional data lake on MinIO with ACID guarantees
- **ClickHouse Analytics**: Fast OLAP queries via Iceberg table federation
- **Weather Integration**: Real-time + historical weather data from Open-Meteo
- **Airflow Orchestration**: DAG-based workflow for historical data ingestion

## Prerequisites

### System Requirements

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| RAM | 8 GB | 16 GB |
| CPU | 4 cores | 8 cores |
| Disk | 20 GB | 50 GB |
| Docker | 20.10+ | Latest |
| Docker Compose | 2.0+ | Latest |

### Port Requirements

Ensure these ports are available:

| Port | Service | Description |
|------|---------|-------------|
| 5432 | PostgreSQL | Database |
| 6379 | Redis | Caching |
| 7077 | Spark Master | Spark communication |
| 8001 | Analytics API | REST API |
| 8081 | Airflow | Workflow UI |
| 8083 | Debezium | CDC connector API |
| 8090 | Spark UI | Spark Master Web UI |
| 8123 | ClickHouse | HTTP interface |
| 8505 | Dashboard | Streamlit UI |
| 9000 | MinIO | S3-compatible storage |
| 9001 | MinIO Console | Storage web UI |

## Quick Start

### 1. Clone the Repository

```bash
git clone <repo-url>
cd Huynz
```

### 2. Start Infrastructure

```bash
docker compose up -d
```

Wait for all services to be healthy (about 30-60 seconds).

### 3. Download Historical Taxi Data

```bash
# Create data directory
mkdir -p data

# Download NYC Yellow Taxi data from official source
curl -L -o data/yellow_tripdata_2025-01.parquet \
  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"

# Download more months as needed (2025-01 to 2025-12)
for month in 02 03 04 05 06 07 08 09 10 11 12; do
  curl -L -o data/yellow_tripdata_2025-$month.parquet \
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-$month.parquet"
done
```

### 4. Copy Data to Airflow

```bash
docker cp data/*.parquet airflow:/opt/airflow/data/
```

### 5. Run Airflow DAGs (Historical Data)

Access Airflow at http://localhost:8081 (admin/admin)

**Run `taxi_parquet_to_postgres` DAG:**
1. Click on the DAG name
2. Click "Trigger DAG" button
3. Monitor progress in "Grid" view

**Run `weather_to_postgres` DAG:**
1. Click on the DAG name
2. Click "Trigger DAG" button
3. This fetches 2025-2026 historical weather data

Or via CLI:
```bash
docker exec airflow airflow dags trigger taxi_parquet_to_postgres
docker exec airflow airflow dags trigger weather_to_postgres
```

### 6. Register Debezium Connector

```bash
# Wait ~30 seconds for Kafka to be ready, then:
./huynn.sh debezium:register-auto
```

Verify:
```bash
./huynn.sh debezium:status
```

### 7. Start Real-Time Simulators

The simulators auto-start with Docker Compose. To verify:
```bash
docker logs taxi-simulator
docker logs weather-simulator
```

Or restart them:
```bash
docker compose restart taxi-simulator weather-simulator
```

### 8. Start Spark CDC Jobs

**Terminal 1 - Taxi CDC:**
```bash
docker exec -it spark-worker bash
cd /spark_jobs
python3 taxi_cdc.py
```

**Terminal 2 - Weather CDC:**
```bash
docker exec -it spark-worker bash
cd /spark_jobs
python3 weather_cdc.py
```

### 9. Verify Pipeline

```bash
# Check PostgreSQL has historical data
docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) FROM yellow_taxi_trips;"

# Check Kafka messages
./huynn.sh kafka:count postgres.public.yellow_taxi_trips

# Check ClickHouse
docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM lakehouse.taxi_prod;"
```

## Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Dashboard | http://localhost:8505 | - |
| API Docs | http://localhost:8001/docs | API Key: `huynz-super-secret-key-2026` |
| Airflow | http://localhost:8081 | admin/admin |
| Spark UI | http://localhost:8090 | - |
| MinIO Console | http://localhost:9001 | admin/admin12345 |

## Data Flow

### Historical Data (One-time via Airflow)
1. **Download parquet files** → NYC Taxi data from cloudfront
2. **Airflow `taxi_parquet_to_postgres`** → Ingest historical taxi trips
3. **Airflow `weather_to_postgres`** → Fetch 2025-2026 weather archive
4. **Debezium** → Capture historical data as CDC events to Kafka

### Real-Time Data (Continuous)
1. **Taxi Simulator** → Generates taxi trips every 30 seconds
2. **Weather Simulator** → Fetches weather every hour
3. **PostgreSQL** → Stores data
4. **Debezium** → Captures changes via logical replication
5. **Kafka** → Streams CDC events
6. **Spark Streaming** → Transforms and writes to Iceberg
7. **MinIO** → Stores Iceberg data files
8. **ClickHouse** → Queries Iceberg tables
9. **API + Dashboard** → Serves analytics

## Airflow DAGs

### taxi_parquet_to_postgres

Ingests historical NYC Yellow Taxi data from parquet files into PostgreSQL.

**Requirements:**
- Parquet files in `/opt/airflow/data/`
- Filename pattern: `yellow_tripdata_YYYY-*.parquet`

**Usage:**
```bash
# Download data first
curl -L -o data/yellow_tripdata_2025-01.parquet \
  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"

# Copy to Airflow
docker cp data/*.parquet airflow:/opt/airflow/data/

# Trigger via Airflow UI or CLI
docker exec airflow airflow dags trigger taxi_parquet_to_postgres

# Check status
docker exec airflow airflow dags list-runs -d taxi_parquet_to_postgres
```

### weather_to_postgres

Fetches historical weather data from Open-Meteo archive (2025-2026) directly into PostgreSQL using DuckDB.

**Features:**
- Downloads 2 years of hourly weather data
- Uses NYC coordinates (40.7128, -74.0060)
- No manual data download required

**Usage:**
```bash
# Trigger via Airflow UI or CLI
docker exec airflow airflow dags trigger weather_to_postgres
```

## Helper Commands

### Docker Management
```bash
./huynn.sh up              # Start all services
./huynn.sh down            # Stop all services
./huynn.sh ps              # Show service status
./huynn.sh logs <service>  # View logs (e.g., ./huynn.sh logs kafka)
```

### Debezium
```bash
./huynn.sh debezium:register-auto  # Register connector (auto-retry)
./huynn.sh debezium:status        # Check connector status
./huynn.sh debezium:list          # List all connectors
```

### Kafka
```bash
./huynn.sh kafka:topics              # List topics
./huynn.sh kafka:count <topic>       # Count messages
./huynn.sh kafka:consume <topic>     # Consume from topic
```

### Spark
```bash
./huynn.sh spark:sh      # Shell into spark-worker
./huynn.sh spark:logs    # View CDC logs
```

### PostgreSQL
```bash
./huynn.sh postgres:sh    # Shell into postgres
```

### Airflow
```bash
# List DAGs
docker exec airflow airflow dags list

# Trigger DAG
docker exec airflow airflow dags trigger <dag_id>

# Check DAG runs
docker exec airflow airflow dags list-runs -d <dag_id>

# Check specific task
docker exec airflow airflow tasks states-for-dag-run <dag_id> <run_id>
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CH_HOST` | clickhouse | ClickHouse host |
| `CH_PORT` | 8123 | ClickHouse HTTP port |
| `API_KEY` | huynz-super-secret-key-2026 | API authentication key |

### Key Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Container orchestration |
| `debezium.json` | CDC connector configuration |
| `init_postgres.sql` | PostgreSQL schema + indexes |
| `init_clickhouse.sql` | ClickHouse Iceberg tables |
| `spark_jobs/taxi_cdc.py` | Main CDC processing job |
| `serving/dashboard.py` | Streamlit analytics dashboard |
| `serving/api.py` | FastAPI analytics endpoints |
| `dags/taxi_ingestion_pipeline.py` | Historical taxi data ingestion |
| `dags/weather_to_postgres.py` | Historical weather data ingestion |

## Troubleshooting

### Services Won't Start

1. Check if ports are in use:
```bash
docker compose ps  # Look for "unhealthy" or "exited"
```

2. Check logs for errors:
```bash
docker compose logs <service-name>
```

3. Restart:
```bash
docker compose down && docker compose up -d
```

### Airflow DAGs Not Showing

1. Check Airflow is running:
```bash
docker compose ps airflow
```

2. Refresh DAGs:
```bash
docker exec airflow airflow dags list
```

### CDC Not Working

1. Verify PostgreSQL WAL level:
```bash
docker exec postgres psql -U admin -d weather -c "SHOW wal_level;"
```
Should return `logical`.

2. Check Debezium connector:
```bash
./huynn.sh debezium:status
```

3. Verify Kafka has messages:
```bash
./huynn.sh kafka:count postgres.public.yellow_taxi_trips
```

### Spark Jobs Failing

1. Check Spark master:
```bash
curl -s http://localhost:8090
```

2. Verify pyspark:
```bash
docker exec spark-worker python3 -c "import pyspark; print(pyspark.__version__)"
```

3. Check logs:
```bash
docker exec spark-worker tail -50 /tmp/taxi_cdc.log
```

### Database Issues

1. Test PostgreSQL:
```bash
docker exec postgres psql -U admin -d weather -c "SELECT 1;"
```

2. Test ClickHouse:
```bash
docker exec clickhouse clickhouse-client --query "SELECT 1"
```

## Common Issues

### "No parquet files found" in Airflow
```bash
# Ensure files are in the right location
docker exec airflow ls /opt/airflow/data/

# Re-copy if needed
docker cp data/*.parquet airflow:/opt/airflow/data/
```

### "No space left on device"
```bash
docker system prune -af --volumes
docker builder prune -af
```

### "Port already in use"
```bash
sudo lsof -i :8081  # Replace with affected port
sudo kill <PID>
```

### "Debezium connector not found"
```bash
./huynn.sh debezium:register-auto
```

## Project Structure

```
Huynz/
├── docker-compose.yml           # Container orchestration
├── README.md                   # This file
├── requirements.txt            # Python dependencies
├── debezium.json               # Debezium connector config
├── huynn.sh                   # Helper CLI script
├── register-debezium.sh        # Auto-registration script
│
├── init_postgres.sql          # PostgreSQL schema + indexes
├── init_clickhouse.sql        # ClickHouse Iceberg tables
│
├── data/                      # Historical taxi data (download separately)
│
├── spark/                     # Spark custom image
│   └── Dockerfile
│
├── spark_jobs/                 # Spark streaming jobs
│   ├── taxi_cdc.py            # Taxi CDC → Iceberg
│   ├── weather_cdc.py         # Weather CDC → Iceberg
│   ├── taxi_simulator.py      # Real-time taxi generator
│   └── weather_simulator.py   # Real-time weather fetcher
│
├── serving/                   # API & Dashboard
│   ├── api.py                 # FastAPI endpoints
│   ├── dashboard.py           # Streamlit dashboard
│   ├── database.py             # ClickHouse queries
│   ├── config.py               # Configuration
│   └── Dockerfile              # API/Dashboard image
│
└── dags/                     # Airflow DAGs
    ├── taxi_ingestion_pipeline.py   # Historical taxi data ingestion
    └── weather_to_postgres.py      # Historical weather data ingestion
```

## Performance Notes

- **Spark Parallelism**: 4 cores, 2 executors
- **Micro-batch Interval**: 30 seconds (configurable in taxi_cdc.py)
- **Kafka Partitions**: Single partition (adjust for higher throughput)
- **PostgreSQL Indexes**: On datetime, location columns

## License

MIT
