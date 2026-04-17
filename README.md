# NYC Taxi Real-Time Analytics Pipeline

A production-ready, tri-path analytics pipeline for real-time NYC Taxi data with Change Data Capture (CDC), supporting instantaneous real-time queries via ClickHouse, historical analytics via Doris/Iceberg, and PostgreSQL fallback.
 
## Architecture Overview

### Tri-Path Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           REAL-TIME CDC PIPELINE                                 │
│                                                                                  │
│  ┌───────────────┐         ┌───────────────┐                                     │ 
│  │ Taxi Simulator│         │Weather Simul. │                                     │
│  │   (every 30s) │         │  (every hour) │                                     │
│  └───────┬───────┘         └───────┬───────┘                                     │
│          │                         │                                             │
│          ▼                         ▼                                             │
│  ┌───────────────────────────────────────────────────────────────────┐           │
│  │                      PostgreSQL (Source DB)                       │           │
│  │   Tables: yellow_taxi_trips, nyc_weather_hourly, taxi_zones       │           │
│  │   + MinIO: Iceberg metadata (JSON files)                          │           │
│  └─────────────────────────────┬─────────────────────────────────────┘           │
│                                │                                                 │
│                  ┌─────────────┴─────────────┐                                   │
│                  │                           │                                   │
│                  ▼                           ▼                                   │
│  ┌──────────────────────────┐   ┌───────────────────────────────────────┐        │
│  │     Debezium CDC         │   │          Spark Structured Stream      │        │
│  │   (Change Data Capture)  │   │    ┌──────────────────────────┐       │        │
│  └─────────────┬────────────┘   │    │ Hadoop Catalog (MinIO)   │       │        │
│                │                │    │  - iceberg_tables        │       │        │
│                ▼                │    │  - table metadata        │       │        │
│  ┌──────────────────────────┐   │    └──────────────────────────┘       │        │
│  │      Apache Kafka        │   └─────────────────┬─────────────────────┘        │
│  │  Topics: CDC events      │                     │                              │
│  └──────┬──────────┬────────┘                     ▼                              │
│         │          │                ┌──────────────────────────┐                 │
│         │          │                │    MinIO Iceberg Tables  │                 │
│         │          │                │    ┌────────────────┐    │                 │
│         │          │                │    │ taxi_zones     │    │                 │
│         │          │                │    │ silver_taxi_trips   │                 │
│         │          │                │    │ nyc_weather    |    │                 │
│         │          │                │    │ gold_*(aggregations)│                 │
│         │          │                │    └────────────────┘    │                 │
│         │          │                └───────────┬──────────────┘                 │
│         │          │                            │                                │
│         ▼          ▼                            ▼                                │
│  ┌──────────────────────────┐   ┌──────────────────────────┐                     │
│  │  ClickHouse Kafka Engine │   │     Apache Doris 2.1     │                     │
│  │    (Real-time Path)      │   │   (SQL Query Engine)     │                     │
│  │   < 1s latency           │   │   ├─ Hadoop Catalog      │                     │
│  └───────────┬──────────────┘   │   ├─ Iceberg Connector   │                     │
│              │                  │   └─ MySQL Protocol      │                     │
│              │                  └───────────┬──────────────┘                     │
│              └──────────────┬───────────────┘                                    │
│                             │                                                    │
│                             ▼                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐         │
│  │                        FastAPI (REST API)                           │         │
│  │        ├─ Real-time mode: ClickHouse queries (sub-second)           │         │
│  │        ├─ Historical mode: Doris → Iceberg Gold (50-200ms)          │         │
│  │        └─ Fallback: PostgreSQL direct (1-5s)                        │         │
│  │        Connection pooling: PostgreSQL (2-20), ClickHouse (20)       │         │
│  └─────────────────────────────┬───────────────────────────────────────┘         │
│                                │                                                 │
│                                ▼                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐         │
│  │                      Streamlit Dashboard                            │         │
│  │          Toggle between Real-Time and Historical modes              │         │
│  └─────────────────────────────────────────────────────────────────────┘         │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Key Engineering Decisions

**CDC Distribution Layer (Kafka):**
- Debezium captures PostgreSQL changes → Kafka topics
- **Single topic consumed by multiple independent consumers:**
  - ClickHouse Kafka Engine (real-time dashboards)
  - Spark Structured Streaming (Iceberg processing)
- Fan-out architecture: one producer, multiple consumers
- Each consumer maintains independent offset tracking
- Fault tolerance: if one consumer fails, others continue unaffected

**Real-time Consumer (ClickHouse):**
- Kafka → ClickHouse Kafka Engine → MergeTree tables
- Consumer group: `clickhouse_taxi_consumer`
- Direct Kafka ingestion using materialized views
- Automatic JSON parsing of Debezium format
- Sub-second query latencies for operational dashboards
- **Data retention:** 365-day TTL (configurable)

**Batch Consumer (Spark - Iceberg Medallion Architecture):**
- Kafka → Spark Structured Streaming → Iceberg (MinIO)
- **Bronze:** Raw CDC data (`taxi_prod`, `taxi_prod_deletes`)
- **Silver:** Quality-checked data (`silver_taxi_trips`, `nyc_weather`)
- **Gold:** Pre-computed aggregations (`gold_hourly_metrics`, `gold_zone_performance`, `gold_borough_summary`)
- Hadoop Catalog (MinIO) stores Iceberg table metadata

**Historical Query Path (Doris → Iceberg → PostgreSQL Fallback):**
- **Primary:** Doris queries Iceberg Gold tables via Hadoop catalog
- **Fallback:** PostgreSQL direct queries when Iceberg tables unavailable
- Hadoop catalog stores metadata alongside data in MinIO (no extra infrastructure)

## Quick Start

### 0. Download Historical Data 

```bash
# Download parquet files
mkdir -p data && cd data
curl -L -O "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
cd ..
```

### 1. Start All Services

```bash
docker compose up -d
```

### 2. Initialising all necessary steps (Seeding taxi_zones, Register Debezium, Link Kafka to ClickHouse, Create Iceberg Catalog, Triggering Headless Airflow Ingestion)

```bash
./huynn.sh pipeline:init

```

### 3. Start Spark Streaming Jobs

```bash
# Optional: Clear ghost checkpoints if recovering from a hard crash (For Example: RAM Overflow -> Hard reset)
./huynn.sh spark:clean

# And Submit Spark Jobs
./huynn.sh spark:jobs
```

This starts:
- **load_zones** → Loads taxi zones into Iceberg
- **TaxiMedallionCDC** → Kafka → Iceberg silver_taxi_trips 
- **WeatherMedallionCDC** → Kafka → Iceberg nyc_weather 
- **GoldAggregations** → Silver → Gold (hourly_metrics, zone_performance, borough_summary)

### 5. Verify Pipeline

```bash
./huynn.sh pipeline:verify
./huynn.sh pipeline:status
```
**Note:** The DAG automatically fetches historical weather data from Open-Meteo Archive API.

### 6. Access Dashboards

- **Streamlit Dashboard**: http://localhost:8505
- **API Documentation**: http://localhost:8001/docs
- **Spark UI**: http://localhost:8090
- **MinIO Console**: http://localhost:9001 (admin/admin12345)
- 

**Note:** I'm using Headless Airflow in the pipeline init script to make startup much faster, so there will be no Airflow UI


## Helper Script (huynn.sh)

```bash
# Quick start everything
./huynn.sh quickstart

# Stack management
./huynn.sh up              # Start all services
./huynn.sh down            # Stop all services
./huynn.sh down-v           # Stop and remove all volumes (full reset)
./huynn.sh ps               # Show service status
./huynn.sh logs <service>   # View logs

# Pipeline operations
./huynn.sh pipeline:init     # Initialize CDC pipeline (Debezium + zones + Kafka)
./huynn.sh pipeline:status   # Check pipeline health
./huynn.sh pipeline:verify   # Verify data across all layers

# Debezium & CDC
./huynn.sh debezium:register  # Register CDC connector
./huynn.sh debezium:status   # Check connector status

# Spark
./huynn.sh spark:jobs        # Start all streaming jobs (load_zones + CDC + Gold)
./huynn.sh spark:clean        #Clean all the checkpoints to avoid zombie process issue

# Doris / Iceberg
./huynn.sh doris:switch       # Switch to Iceberg catalog and list tables
./huynn.sh doris:count <tbl> # Count rows in Iceberg table
./huynn.sh doris:sh           # Open Doris SQL shell (mysql client)

# Databases
./huynn.sh postgres:count    # Count taxi trips
./huynn.sh postgres:zones     # Count taxi zones
./huynn.sh postgres:iceberg   # Show Iceberg catalog tables
./huynn.sh clickhouse:count   # Count real-time records

# API
./huynn.sh api:health                    # Health check
./huynn.sh api:stats                     # Real-time stats
./huynn.sh api:stats-historical          # Historical stats (default: Jan 2025)
./huynn.sh api:weather-historical        # Weather impact (historical)
./huynn.sh api:test                      # Test both modes

# Full reset
./huynn.sh down-v && ./huynn.sh quickstart
```

## Troubleshooting

### Spark Out of Memory (OOM) or Zombie Jobs

```bash
# 1. Restart Spark containers to kill zombie JVMs
docker compose restart spark-master spark-worker

# 2. Wipe existing checkpoint locks
./huynn.sh spark:clean

# 3. Resubmit topology
./huynn.sh spark:jobs
```

### Dashboard Shows "Viewing backup data from PostgreSQL"
This indicates the built-in observability loop successfully caught a failure in Apache Doris.

    1/ Check the API logs for Doris syntax or connection errors: docker compose logs analytics-api

    2/ Verify Doris is running and can read the Hadoop catalog: ./huynn.sh doris:switch

### Empty Real-Time Dashboard (ClickHouse)

```bash
# Force recreation of the Kafka ingestion engine
./huynn.sh clickhouse:kafka-ingestion
```

### Spark Jobs Not Streaming

```bash
./huynn.sh spark:jobs   # Start CDC jobs
./huynn.sh spark:apps   # Check running apps
./huynn.sh spark:logs    # View logs
```

### Iceberg Tables Empty or Missing

```bash
# Check via Doris (Hadoop catalog)
./huynn.sh doris:switch

# Or directly in MinIO:
docker exec minio mc ls local/datalake/warehouse/lakehouse/

# If tables have empty namespace, fix:
docker exec postgres psql -U admin -d weather -c "UPDATE iceberg_tables SET table_namespace = 'lakehouse' WHERE table_namespace = '';"
```

### Full Reset

```bash
# Stop everything and remove all data
./huynn.sh down-v

# Or manually:
docker compose down -v

# Then start fresh:
./huynn.sh quickstart
```

## Project Structure

```
Huynz/
├── docker-compose.yml           # Container orchestration
├── README.md                    # This file
├── huynn.sh                     # Helper CLI script
├── start_spark_jobs.sh          # Spark job starter (load_zones + CDC + Gold Aggregations)
├── debezium.json                # Debezium connector config
├── requirements.txt             # Airflow + serving Python dependencies
│
├── init_postgres.sql            # PostgreSQL schema (auto-loaded): includes taxi_zones
├── init_clickhouse.sql          # ClickHouse schema (auto-loaded)
├── setup_kafka_ingestion.sql    # ClickHouse Kafka engine tables (run via pipeline-init)
├── dummy_aws_credentials        # I keep this to make sure Iceberg/Doris Catalog is stable
│
│
├── spark/                       # Spark custom image
│   └── Dockerfile
│
├── spark_jobs/                  # Spark streaming jobs
│   ├── taxi_cdc.py              # Taxi CDC → Iceberg 
│   ├── weather_cdc.py           # Weather CDC → Iceberg
│   ├── gold_aggregations.py     # Silver → Gold 
│   ├── load_zones.py            # Zone metadata loader → Iceberg
│   ├── taxi_simulator.py        # Real-time trip generator
│   └── weather_simulator.py     # Weather data fetcher (Open-Meteo)
│
├── serving/                     # API & Dashboard
│   ├── api.py                   # FastAPI endpoints (realtime + historical + Doris)
│   ├── database.py              # ClickHouse + PostgreSQL services
│   ├── doris_service.py         # Doris → Iceberg Gold queries
│   ├── dashboard.py             # Streamlit dashboard
│   ├── models.py                # Pydantic models
│   ├── config.py                # Configuration
│   ├── client.py                # API client example
│   ├── requirements.txt         # Python dependencies
│   └── Dockerfile               # API/Dashboard image
│
├── dags/                        # Airflow DAGs
│   └── taxi_ingestion_pipeline.py       # Parquet taxi files + weather ingestion
│
├── scripts/
│   └── init_doris.sql          # Optional: Doris Iceberg catalog initialization
│
└── data/                        # Data directory (gitignored)
    └── taxi_zone_lookup.csv     # NYC taxi zone metadata (265 zones)
```

## Iceberg Catalog Configuration

All Spark jobs and Doris use the **Hadoop catalog** with metadata stored as JSON files in MinIO:

```properties
# Doris (auto-configured in docker-compose.yml)
# Or manually via scripts/init_doris.sql:
CREATE CATALOG iceberg_hadoop PROPERTIES (
    'type' = 'iceberg',
    'iceberg.catalog.type' = 'hadoop',
    'warehouse' = 's3a://datalake/warehouse/',
    's3.endpoint' = 'http://minio:9000',
    's3.access_key' = 'admin',
    's3.secret_key' = 'admin12345',
    's3.region' = 'us-east-1',
    'use_path_style' = 'true',
    'core.site.fs.s3a.endpoint' = 'http://minio:9000',
    'core.site.fs.s3a.access.key' = 'admin',
    'core.site.fs.s3a.secret.key' = 'admin12345',
    'core.site.fs.s3a.path.style.access' = 'true',
    'core.site.fs.s3a.impl' = 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'core.site.fs.s3a.connection.ssl.enabled' = 'false',
    'core.site.fs.s3a.signature.version' = 'S3V4'
);
```

```python
# Spark jobs (common config)
.config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.lakehouse.type", "hadoop")
.config("spark.sql.catalog.lakehouse.warehouse", "s3a://datalake/warehouse")
```

**Why Hadoop Catalog:**
- No PostgreSQL dependency for Iceberg metadata (simplified stack)
- Metadata stored as JSON files alongside data in MinIO
- Both Spark and Doris share the same warehouse directory
- Easy to inspect: `ls minio_data/datalake/warehouse/lakehouse/`

## Performance Notes

| Query Type | Database | Typical Latency |
|------------|-----------|------------------|
| Real-time stats | ClickHouse | <1s |
| Historical stats | Doris → Iceberg Gold | 50-200ms |
| Historical stats | PostgreSQL (fallback) | 1-5s |
| Weather impact | PostgreSQL + Iceberg | 2-4s |

## License

MIT